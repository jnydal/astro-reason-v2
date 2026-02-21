# embeddings/jobs.py
import json
import os
import time
from datetime import datetime

import numpy as np
import psycopg2, psycopg2.extras
from psycopg2 import sql
from pgvector.psycopg2 import register_vector
from sentence_transformers import SentenceTransformer
from confluent_kafka import Consumer

try:
    from app.core.provenance import log_event
except ModuleNotFoundError:
    # app resolves to top-level app/ (backfill) not src/app/ (provenance); use no-op
    def log_event(*args, **kwargs) -> None:
        pass

EMBED_MODEL = os.getenv("EMBEDDINGS_MODEL", "BAAI/bge-large-en-v1.5")
DSN = os.getenv("DATABASE_URL", "").replace("postgresql+psycopg://", "postgresql://")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "embeddings-worker")
KAFKA_TOPIC = os.getenv("KAFKA_EMBEDDINGS_TOPIC", "embeddings")
EMBED_CHUNK_SIZE = max(1, int(os.getenv("EMBEDDINGS_CHUNK_SIZE", "32")))


def _update_job_status(job_id: str, status: str, result: str | None = None, exc_info: str | None = None) -> None:
    if not DSN:
        return
    conn = psycopg2.connect(DSN)
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE job_status
           SET status = %s,
               started_at = COALESCE(started_at, %s),
               ended_at = CASE WHEN %s IN ('FINISHED', 'FAILED') THEN %s ELSE ended_at END,
               result = %s,
               exc_info = %s,
               updated_at = NOW()
         WHERE id = %s
        """,
        (
            status,
            int(time.time() * 1000),
            status,
            int(time.time() * 1000),
            result,
            exc_info,
            job_id,
        ),
    )
    conn.commit()
    cur.close()
    conn.close()

def _chunked(items, chunk_size: int):
    for i in range(0, len(items), chunk_size):
        yield items[i:i + chunk_size]


def _normalize_person_ids(person_ids) -> list:
    """Accept person_ids as list (fetch-bio, backfill) or comma-separated string (ingest)."""
    if not person_ids:
        return []
    if isinstance(person_ids, str):
        return [x.strip() for x in person_ids.split(",") if x.strip()]
    return list(person_ids)


def embed_person_bios(payload: dict, heartbeat=None):
    """Kafka job: Embed bios for given person_ids and upsert into embeddings table."""
    started = time.monotonic()
    person_ids = _normalize_person_ids(payload.get("person_ids"))
    model_name = payload.get("model", EMBED_MODEL)
    source = payload.get("source", "astrodb-upload")

    if not person_ids:
        print("⚠️ No person_ids provided to embed_person_bios.")
        conn = psycopg2.connect(DSN)
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        log_event(
            cur,
            stage="embeddings",
            status="error",
            count=0,
            duration_ms=int((time.monotonic() - started) * 1000),
            error="no_person_ids",
        )
        conn.commit()
        cur.close()
        conn.close()
        return {"status": "no_ids"}

    print(f"Embedding {len(person_ids)} bios using {model_name}...")

    conn = psycopg2.connect(DSN)
    register_vector(conn)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # Fetch texts that need embeddings (missing or text_hash changed)
    # Use tuple + IN for reliable UUID matching (psycopg2 ANY/array can be flaky with uuid[])
    # Deduplicate by person_id: prefer wiki (rev_id>0) over ingest stub (rev_id=0), then longer text
    if not person_ids:
        rows = []
    else:
        cur.execute(
            """
            SELECT DISTINCT ON (bt.person_id)
                   bt.person_id, bt.text, bt.text_hash, e.text_hash AS existing_hash
            FROM bio_text bt
            LEFT JOIN embeddings e
              ON e.person_id = bt.person_id AND e.model_name = %s
            WHERE bt.person_id IN %s
              AND bt.text IS NOT NULL AND LENGTH(TRIM(bt.text)) > 0
            ORDER BY bt.person_id,
                     COALESCE(bt.rev_id, 0) DESC,
                     COALESCE(bt.char_count, LENGTH(bt.text)) DESC NULLS LAST
            """,
            (model_name, tuple(person_ids)),
        )
        rows = cur.fetchall()

    # Filter only new or changed bios
    todo = [r for r in rows if not r["existing_hash"] or r["existing_hash"] != r["text_hash"]]

    # Diagnostic: log when we get far fewer rows than person_ids (possible query/format issue)
    distinct_in_rows = len(set(r["person_id"] for r in rows)) if rows else 0
    if len(person_ids) > 0 and len(rows) == 0:
        print(f"⚠️ Job had {len(person_ids)} person_ids but query returned 0 rows from bio_text. Check DB connectivity and schema.")
    elif len(person_ids) > 0 and distinct_in_rows < len(person_ids) * 0.5:
        print(f"⚠️ Job had {len(person_ids)} person_ids but only {distinct_in_rows} distinct persons in bio_text. Possible missing data.")

    if not todo:
        print(f"No new or changed bios to embed. (person_ids={len(person_ids)}, rows={len(rows)}, already_embedded={len(rows) - len(todo)})")
        log_event(
            cur,
            stage="embeddings",
            status="ok",
            count=0,
            duration_ms=int((time.monotonic() - started) * 1000),
            meta={"model": model_name, "source": source},
        )
        conn.commit()
        cur.close()
        conn.close()
        return {"status": "noop", "count": 0}

    model = SentenceTransformer(model_name)
    processed = 0

    for chunk in _chunked(todo, EMBED_CHUNK_SIZE):
        if heartbeat:
            heartbeat()

        texts = [r["text"] for r in chunk]
        pids = [r["person_id"] for r in chunk]

        embeddings = model.encode(
            texts, batch_size=8, show_progress_bar=False, normalize_embeddings=True
        )
        embeddings = np.array(embeddings, dtype=np.float32)

        for pid, vec, row in zip(pids, embeddings, chunk):
            dim = int(len(vec))
            if dim not in (384, 768, 1024, 1536):
                print(f"⚠️ Unsupported embedding dimension {dim} for person_id={pid}. Skipping.")
                continue
            table_name = f"embeddings_{dim}"
            cur.execute(
                sql.SQL("""
                    INSERT INTO {table} (person_id, model_name, dim, vector, text_hash, meta, source, updated_at)
                    VALUES (%s, %s, %s, %s, %s, jsonb_build_object('provider','sentence-transformers'), %s, NOW())
                    ON CONFLICT (person_id, model_name) DO UPDATE
                      SET dim = EXCLUDED.dim,
                          vector = EXCLUDED.vector,
                          text_hash = EXCLUDED.text_hash,
                          meta = EXCLUDED.meta,
                          source = EXCLUDED.source,
                          updated_at = NOW()
                """).format(table=sql.Identifier(table_name)),
                (pid, model_name, dim, vec, row["text_hash"], source),
            )

        conn.commit()
        processed += len(chunk)

    # Provenance logging
    log_event(
        cur,
        stage="embeddings",
        status="ok",
        count=processed,
        duration_ms=int((time.monotonic() - started) * 1000),
        meta={
            "model": model_name,
            "source": source,
            "timestamp": datetime.utcnow().isoformat(),
        },
    )
    conn.commit()
    cur.close(); conn.close()

    print(f"✅ Embedded {processed} bios.")
    return {"status": "ok", "count": processed}


def _consume_loop():
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": KAFKA_GROUP_ID,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([KAFKA_TOPIC])

    print(f"Embeddings worker listening on Kafka topic '{KAFKA_TOPIC}'...")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"Kafka error: {msg.error()}")
                continue

            job_id = None
            try:
                payload = json.loads(msg.value().decode("utf-8"))
                job_id = payload.get("id")
                kwargs = payload.get("kwargs") or {}
                if job_id:
                    _update_job_status(job_id, "STARTED")
                result = embed_person_bios(kwargs, heartbeat=lambda: consumer.poll(0))
                if job_id:
                    _update_job_status(job_id, "FINISHED", result=json.dumps(result))
            except Exception as exc:
                if job_id:
                    _update_job_status(job_id, "FAILED", exc_info=str(exc))
                print(f"Embeddings job failed: {exc}")
    finally:
        consumer.close()


if __name__ == "__main__":
    _consume_loop()
