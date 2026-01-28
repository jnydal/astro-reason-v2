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

from app.core.provenance import log_event  # optional helper

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


def embed_person_bios(payload: dict, heartbeat=None):
    """Kafka job: Embed bios for given person_ids and upsert into embeddings table."""
    started = time.monotonic()
    person_ids = payload.get("person_ids") or []
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
    cur.execute("""
        SELECT bt.person_id, bt.text, bt.text_hash, e.text_hash AS existing_hash
        FROM bio_text bt
        LEFT JOIN embeddings e
          ON e.person_id = bt.person_id AND e.model_name = %s
        WHERE bt.person_id = ANY(%s::uuid[])
    """, (model_name, person_ids))
    rows = cur.fetchall()

    # Filter only new or changed bios
    todo = [r for r in rows if not r["existing_hash"] or r["existing_hash"] != r["text_hash"]]
    if not todo:
        print("No new or changed bios to embed.")
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
