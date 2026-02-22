"""
Re-embed people whose bio_text has changed (text_hash mismatch with stored embeddings).

Use when the embeddings worker was picking the wrong bio_text row (e.g. stale wiki over
updated stub). This finds people where the canonical bio_text row's hash differs from
embeddings and re-embeds them.

Run via Docker:
  docker compose run --rm embeddings python -m app.reembed_stale_sync

Or locally:
  cd service/worker_embeddings && PYTHONPATH=src:. python -m app.reembed_stale_sync
"""
from __future__ import annotations

import os
import time

import psycopg2
import psycopg2.extras

from jobs import embed_person_bios

MAX_RETRIES = 3
RETRY_DELAYS = (5, 10, 20)


def _normalize_dsn(dsn: str) -> str:
    if dsn.startswith("postgresql+psycopg://"):
        return "postgresql://" + dsn[len("postgresql+psycopg://") :]
    if dsn.startswith("postgresql+psycopg2://"):
        return "postgresql://" + dsn[len("postgresql+psycopg2://") :]
    return dsn


def _get_dsn() -> str:
    raw = os.getenv("PG_DSN") or os.getenv("DATABASE_URL") or ""
    return _normalize_dsn(raw)


def _person_ids_stale_embeddings(conn, model_name: str) -> list[str]:
    """People who have embeddings but bio_text (best row) hash differs."""
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(
        """
        WITH best_bio AS (
          SELECT DISTINCT ON (bt.person_id)
                 bt.person_id, bt.text_hash
          FROM bio_text bt
          WHERE bt.text IS NOT NULL AND LENGTH(TRIM(bt.text)) > 0
          ORDER BY bt.person_id,
                   COALESCE(bt.retrieved_at, bt.updated_at) DESC NULLS LAST,
                   COALESCE(bt.char_count, LENGTH(bt.text)) DESC NULLS LAST
        ),
        with_emb AS (
          SELECT person_id, text_hash AS emb_hash
          FROM embeddings_384 WHERE model_name = %s
          UNION ALL
          SELECT person_id, text_hash FROM embeddings_768 WHERE model_name = %s
          UNION ALL
          SELECT person_id, text_hash FROM embeddings_1024 WHERE model_name = %s
          UNION ALL
          SELECT person_id, text_hash FROM embeddings_1536 WHERE model_name = %s
        )
        SELECT bb.person_id::text
        FROM best_bio bb
        JOIN with_emb e ON e.person_id = bb.person_id
        WHERE bb.text_hash IS DISTINCT FROM e.emb_hash
        ORDER BY bb.person_id
        """,
        (model_name,) * 4,
    )
    rows = cur.fetchall()
    cur.close()
    return [r["person_id"] for r in rows]


def main(batch_size: int = 200) -> None:
    dsn = _get_dsn()
    if not dsn:
        raise SystemExit("PG_DSN or DATABASE_URL required")

    model = os.getenv("EMBEDDINGS_MODEL", "BAAI/bge-large-en-v1.5")

    conn = psycopg2.connect(dsn)
    try:
        person_ids = _person_ids_stale_embeddings(conn, model)
    finally:
        conn.close()

    if not person_ids:
        print("No person_ids with stale embeddings (hash mismatch). Nothing to do.")
        return

    print(
        f"Re-embedding {len(person_ids)} people with stale embeddings "
        f"(bio_text hash != embeddings hash) in batches of {batch_size}."
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
                        "source": "reembed_stale_sync",
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
                        f"  batch {batch_num}: DB error, retrying in {delay}s "
                        f"(attempt {attempt + 1}/{MAX_RETRIES}): {exc}"
                    )
                    time.sleep(delay)
                else:
                    raise

    print(f"Done. Total re-embedded: {total_embedded}")


if __name__ == "__main__":
    main()
