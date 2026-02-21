"""
One-off script: enqueue embedding jobs for people who have bio_text but no embeddings.

Use when "Embeddings Computed" has stagnated and there are people with biographies
that never got embedding jobs (e.g. jobs lost, fetch_bio didn't enqueue, etc.).

Run via Docker (recommended):
  docker compose run --rm embeddings python -m app.enqueue_embeddings_backfill

Or locally from worker_embeddings directory with PYTHONPATH set:
  cd service/worker_embeddings && PYTHONPATH=. python -m app.enqueue_embeddings_backfill
"""
from __future__ import annotations

import json
import os
import time
import uuid

import psycopg2
from confluent_kafka import Producer


def _normalize_dsn(dsn: str) -> str:
    if dsn.startswith("postgresql+psycopg://"):
        return "postgresql://" + dsn[len("postgresql+psycopg://") :]
    if dsn.startswith("postgresql+psycopg2://"):
        return "postgresql://" + dsn[len("postgresql+psycopg2://") :]
    return dsn


def _get_dsn() -> str:
    raw = os.getenv("PG_DSN") or os.getenv("DATABASE_URL") or ""
    return _normalize_dsn(raw)


def _require_qid() -> bool:
    v = os.getenv("EMBEDDINGS_REQUIRE_QID", "false").lower()
    return v in ("true", "1", "yes")


def _person_ids_bio_no_embeddings(conn, require_qid: bool = False) -> list[str]:
    """Person IDs that have bio_text with text but no embeddings in any dimension table."""
    cur = conn.cursor()
    qid_clause = (
        " AND EXISTS (SELECT 1 FROM entity_link el WHERE el.person_id = bt.person_id)"
        if require_qid
        else ""
    )
    cur.execute(
        f"""
        SELECT DISTINCT bt.person_id::text
        FROM bio_text bt
        WHERE bt.text IS NOT NULL AND LENGTH(TRIM(bt.text)) > 0
          AND NOT EXISTS (
            SELECT 1 FROM embeddings_384 e WHERE e.person_id = bt.person_id
            UNION
            SELECT 1 FROM embeddings_768 e WHERE e.person_id = bt.person_id
            UNION
            SELECT 1 FROM embeddings_1024 e WHERE e.person_id = bt.person_id
            UNION
            SELECT 1 FROM embeddings_1536 e WHERE e.person_id = bt.person_id
          ){qid_clause}
        ORDER BY 1
        """
    )
    rows = cur.fetchall()
    cur.close()
    return [r[0] for r in rows]


def main(batch_size: int = 500) -> None:
    dsn = _get_dsn()
    if not dsn:
        raise SystemExit("PG_DSN or DATABASE_URL required")

    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = os.getenv("KAFKA_EMBEDDINGS_TOPIC", "embeddings")
    model = os.getenv("EMBEDDINGS_MODEL", "BAAI/bge-large-en-v1.5")

    require_qid = _require_qid()
    conn = psycopg2.connect(dsn)
    try:
        person_ids = _person_ids_bio_no_embeddings(conn, require_qid=require_qid)
    finally:
        conn.close()

    if not person_ids:
        print("No person_ids with bio_text and missing embeddings. Nothing to enqueue.")
        return

    scope = "bio_text + entity_link without embeddings" if require_qid else "bio_text without embeddings"
    print(
        f"Enqueueing embedding jobs for {len(person_ids)} people ({scope}) to topic '{topic}'..."
    )

    producer = Producer({"bootstrap.servers": kafka_bootstrap, "client.id": "enqueue-embeddings-backfill"})
    now_ms = int(time.time() * 1000)
    jobs_enqueued = 0
    for i in range(0, len(person_ids), batch_size):
        batch = person_ids[i : i + batch_size]
        job_id = str(uuid.uuid4())
        job = {
            "id": job_id,
            "function": "embeddings.embed_person_bios",
            "args": [],
            "kwargs": {
                "person_ids": batch,
                "model": model,
                "source": "enqueue-embeddings-backfill",
            },
            "status": "QUEUED",
            "enqueuedAt": now_ms,
            "startedAt": None,
            "endedAt": None,
            "result": None,
            "excInfo": None,
        }
        producer.produce(topic, key=job_id.encode("utf-8"), value=json.dumps(job))
        jobs_enqueued += 1
        if jobs_enqueued % 5 == 0 or i + batch_size >= len(person_ids):
            producer.flush()
            print(f"  enqueued {min(i + batch_size, len(person_ids))}/{len(person_ids)}")
    producer.flush()
    print(f"Done. Enqueued {len(person_ids)} person_ids in {jobs_enqueued} job(s). Ensure embeddings worker is running.")


if __name__ == "__main__":
    main()
