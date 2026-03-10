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
EMBEDDINGS_SKIP_FAILED = os.getenv("EMBEDDINGS_SKIP_FAILED", "true").lower() in ("true", "1", "yes")


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


def _record_failed_embedding(cur, person_id, model_name: str, reason: str, details: str | None = None) -> None:
    """Insert/upsert into failed_embeddings. Truncate details to avoid huge rows."""
    details_trunc = (details or "")[:500] if details else None
    cur.execute(
        """
        INSERT INTO failed_embeddings (person_id, model_name, failure_reason, details, attempted_at)
        VALUES (%s, %s, %s, %s, NOW())
        ON CONFLICT (person_id, model_name) DO UPDATE
          SET failure_reason = EXCLUDED.failure_reason,
              details = EXCLUDED.details,
              attempted_at = NOW()
        """,
        (person_id, model_name, reason, details_trunc),
    )


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
    # Deduplicate by person_id: prefer MOST RECENTLY UPDATED/RETRIEVED row (fetch-bio updates stubs
    # in place; old rev_id ordering wrongly preferred stale wiki rows over updated stubs).
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
                     COALESCE(bt.retrieved_at, bt.updated_at) DESC NULLS LAST,
                     COALESCE(bt.char_count, LENGTH(bt.text)) DESC NULLS LAST
            """,
            (model_name, tuple(person_ids)),
        )
        rows = cur.fetchall()

    # Filter only new or changed bios
    todo = [r for r in rows if not r["existing_hash"] or r["existing_hash"] != r["text_hash"]]

    # Optionally exclude persons already in failed_embeddings (avoid retry loops)
    if EMBEDDINGS_SKIP_FAILED and todo:
        cur.execute(
            """
            SELECT person_id FROM failed_embeddings
            WHERE model_name = %s AND person_id IN %s
            """,
            (model_name, tuple(r["person_id"] for r in todo)),
        )
        failed_ids = {row["person_id"] for row in cur.fetchall()}
        if failed_ids:
            todo = [r for r in todo if r["person_id"] not in failed_ids]

    skipped = [r for r in rows if r not in todo]

    # Diagnostic: log when we get far fewer rows than person_ids (possible query/format issue)
    distinct_in_rows = len(set(r["person_id"] for r in rows)) if rows else 0
    if len(person_ids) > 0 and len(rows) == 0:
        print(f"⚠️ Job had {len(person_ids)} person_ids but query returned 0 rows from bio_text. Check DB connectivity and schema.")
    elif len(person_ids) > 0 and distinct_in_rows < len(person_ids) * 0.5:
        print(f"⚠️ Job had {len(person_ids)} person_ids but only {distinct_in_rows} distinct persons in bio_text. Possible missing data.")

    if not todo:
        sample = [str(s["person_id"])[:8] for s in skipped[:5]] if skipped else []
        print(
            f"No new or changed bios to embed. person_ids={len(person_ids)}, rows={len(rows)}, "
            f"skipped_as_unchanged={len(skipped)} (sample: {sample})"
        )
        log_event(
            cur,
            stage="embeddings",
            status="ok",
            count=0,
            duration_ms=int((time.monotonic() - started) * 1000),
            meta={
                "model": model_name,
                "source": source,
                "skipped_unchanged": len(skipped),
                "sample_person_ids": [str(s["person_id"])[:8] for s in skipped[:5]],
            },
        )
        conn.commit()
        cur.close()
        conn.close()
        return {"status": "noop", "count": 0, "skipped_unchanged": len(skipped)}

    model = SentenceTransformer(model_name)
    processed = 0
    failed_count = 0
    failed_sample: list[str] = []

    def _insert_embedding(pid, vec, row, table_name: str) -> None:
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
            (pid, model_name, int(len(vec)), vec, row["text_hash"], source),
        )

    def _process_one(row) -> bool:
        """Process one person. Returns True if successful, False if failed (recorded in failed_embeddings)."""
        nonlocal failed_count, failed_sample
        pid = row["person_id"]
        text = (row["text"] or "").strip()
        if not text:
            _record_failed_embedding(cur, pid, model_name, "empty_text", "text empty or invalid")
            failed_count += 1
            if len(failed_sample) < 5:
                failed_sample.append(str(pid)[:8])
            print(f"Skipped person_id={pid} reason=empty_text")
            return False
        try:
            embeddings = model.encode(
                [text], batch_size=1, show_progress_bar=False, normalize_embeddings=True
            )
            vec = np.array(embeddings[0], dtype=np.float32)
        except Exception as e:
            _record_failed_embedding(cur, pid, model_name, "encode_error", str(e))
            failed_count += 1
            if len(failed_sample) < 5:
                failed_sample.append(str(pid)[:8])
            print(f"Skipped person_id={pid} reason=encode_error details={str(e)[:100]}")
            return False
        dim = int(len(vec))
        if dim not in (384, 768, 1024, 1536):
            _record_failed_embedding(cur, pid, model_name, "unsupported_dim", f"dim={dim}")
            failed_count += 1
            if len(failed_sample) < 5:
                failed_sample.append(str(pid)[:8])
            print(f"Skipped person_id={pid} reason=unsupported_dim dim={dim}")
            return False
        try:
            _insert_embedding(pid, vec, row, f"embeddings_{dim}")
        except Exception as e:
            _record_failed_embedding(cur, pid, model_name, "db_error", str(e))
            failed_count += 1
            if len(failed_sample) < 5:
                failed_sample.append(str(pid)[:8])
            print(f"Skipped person_id={pid} reason=db_error details={str(e)[:100]}")
            return False
        return True

    for chunk in _chunked(todo, EMBED_CHUNK_SIZE):
        if heartbeat:
            heartbeat()

        texts = [r["text"] for r in chunk]
        pids = [r["person_id"] for r in chunk]

        try:
            embeddings = model.encode(
                texts, batch_size=8, show_progress_bar=False, normalize_embeddings=True
            )
            embeddings = np.array(embeddings, dtype=np.float32)

            for pid, vec, row in zip(pids, embeddings, chunk):
                dim = int(len(vec))
                if dim not in (384, 768, 1024, 1536):
                    _record_failed_embedding(cur, pid, model_name, "unsupported_dim", f"dim={dim}")
                    failed_count += 1
                    if len(failed_sample) < 5:
                        failed_sample.append(str(pid)[:8])
                    print(f"Skipped person_id={pid} reason=unsupported_dim dim={dim}")
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
                processed += 1
            conn.commit()
        except Exception as batch_err:
            print(f"Falling back to per-person processing for chunk ({len(chunk)} persons) after batch error: {batch_err}")
            for row in chunk:
                if heartbeat:
                    heartbeat()
                if _process_one(row):
                    processed += 1
            conn.commit()

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
            "failed_count": failed_count,
            "failed_sample": failed_sample[:5],
        },
    )
    conn.commit()
    cur.close()
    conn.close()

    msg = f"Embedded {processed} bios."
    if failed_count:
        msg += f" Skipped {failed_count} (recorded in failed_embeddings, sample: {failed_sample})"
    print(f"✅ {msg}")
    return {
        "status": "ok",
        "count": processed,
        "failed_count": failed_count,
        "failed_sample": failed_sample[:5],
    }


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
