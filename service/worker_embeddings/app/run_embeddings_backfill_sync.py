"""
Sync backfill: process embeddings directly, bypassing Kafka.

Use when the Kafka-based backfill repeatedly fails to produce embeddings
(e.g. jobs enqueued but worker noops, or messages never consumed correctly).

This script processes all people with bio_text but no embeddings in-process,
writing directly to the database. No Kafka involved.

Run via Docker (recommended):
  docker compose run --rm embeddings python -m app.run_embeddings_backfill_sync

Or locally:
  cd service/worker_embeddings && PYTHONPATH=src:. python -m app.run_embeddings_backfill_sync
"""
from __future__ import annotations

import os
import time

import psycopg2

from jobs import embed_person_bios

MAX_RETRIES = 3
RETRY_DELAYS = (5, 10, 20)  # seconds


def _normalize_dsn(dsn: str) -> str:
    if dsn.startswith("postgresql+psycopg://"):
        return "postgresql://" + dsn[len("postgresql+psycopg://") :]
    if dsn.startswith("postgresql+psycopg2://"):
        return "postgresql://" + dsn[len("postgresql+psycopg2://") :]
    return dsn


def _get_dsn() -> str:
    raw = os.getenv("PG_DSN") or os.getenv("DATABASE_URL") or ""
    return _normalize_dsn(raw)


def _person_ids_bio_no_embeddings(conn) -> list[str]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT DISTINCT bt.person_id::text
        FROM bio_text bt
        WHERE bt.text IS NOT NULL AND LENGTH(TRIM(bt.text)) > 0
          AND NOT EXISTS (
            SELECT 1 FROM embeddings_384 e WHERE e.person_id = bt.person_id
            UNION SELECT 1 FROM embeddings_768 e WHERE e.person_id = bt.person_id
            UNION SELECT 1 FROM embeddings_1024 e WHERE e.person_id = bt.person_id
            UNION SELECT 1 FROM embeddings_1536 e WHERE e.person_id = bt.person_id
          )
        ORDER BY 1
        """
    )
    rows = cur.fetchall()
    cur.close()
    return [r[0] for r in rows]


def main(batch_size: int = 200) -> None:
    dsn = _get_dsn()
    if not dsn:
        raise SystemExit("PG_DSN or DATABASE_URL required")

    model = os.getenv("EMBEDDINGS_MODEL", "BAAI/bge-large-en-v1.5")

    conn = psycopg2.connect(dsn)
    try:
        person_ids = _person_ids_bio_no_embeddings(conn)
    finally:
        conn.close()

    if not person_ids:
        print("No person_ids with bio_text and missing embeddings. Nothing to do.")
        return

    print(
        "Resumable: only people without embeddings are processed. "
        "If interrupted or failed, re-run the same command; progress is preserved."
    )
    print(
        f"Sync backfill: processing {len(person_ids)} people in batches of {batch_size} (no Kafka)."
    )

    total_embedded = 0
    for i in range(0, len(person_ids), batch_size):
        batch = person_ids[i : i + batch_size]
        batch_num = i // batch_size + 1
        for attempt in range(MAX_RETRIES):
            try:
                result = embed_person_bios(
                    {
                        "person_ids": batch,
                        "model": model,
                        "source": "run_embeddings_backfill_sync",
                    },
                    heartbeat=None,
                )
                count = result.get("count", 0)
                total_embedded += count
                status = result.get("status", "?")
                print(f"  batch {batch_num}: {count} embedded (status={status})")
                break
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as exc:
                if attempt < MAX_RETRIES - 1:
                    delay = RETRY_DELAYS[attempt]
                    print(
                        f"  batch {batch_num}: DB connection error, retrying in {delay}s "
                        f"(attempt {attempt + 1}/{MAX_RETRIES}): {exc}"
                    )
                    time.sleep(delay)
                else:
                    raise

    print(f"Done. Total embedded: {total_embedded}")


if __name__ == "__main__":
    main()
