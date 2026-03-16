"""
One-off script: enqueue astro.interpret jobs for all people who have embeddings
and astro_features but are missing astrological readings (same pipeline-ready
set as "embeddings computed" that are ready for interpretation).

Run from repo root (with .env or same env as astro-interpreter):
  python -m service.worker_astro_interpret.app.enqueue_interpret

Or via Docker:
  docker compose run --rm astro-interpreter python -m service.worker_astro_interpret.app.enqueue_interpret
"""
from __future__ import annotations

import json
import os
import time
import uuid

import psycopg2
import psycopg2.extras
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


def _person_ids_embeddings_astro_missing_readings(conn) -> list[str]:
    """Person IDs that have embeddings and astro_features but no astro_interpretations."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT DISTINCT e.person_id::text
        FROM (
            SELECT person_id FROM embeddings_384
            UNION
            SELECT person_id FROM embeddings_768
            UNION
            SELECT person_id FROM embeddings_1024
            UNION
            SELECT person_id FROM embeddings_1536
        ) e
        INNER JOIN astro_features af ON af.person_id = e.person_id
        LEFT JOIN astro_interpretations ai ON ai.person_id = e.person_id
        WHERE ai.person_id IS NULL
        ORDER BY 1
        """
    )
    rows = cur.fetchall()
    cur.close()
    return [r[0] for r in rows]


def _insert_job_status(cur, job: dict) -> None:
    """Insert a QUEUED row so the job is visible via job_status (architecture invariant)."""
    cur.execute(
        """
        INSERT INTO job_status
            (id, function, status, args_json, kwargs_json, enqueued_at)
        VALUES
            (%s, %s, %s, %s, %s, %s)
        """,
        (
            job["id"],
            job["function"],
            job["status"],
            psycopg2.extras.Json(job.get("args", [])),
            psycopg2.extras.Json(job.get("kwargs", {})),
            job["enqueuedAt"],
        ),
    )


def main() -> None:
    dsn = _get_dsn()
    if not dsn:
        raise SystemExit("PG_DSN or DATABASE_URL required")

    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = os.getenv("KAFKA_ASTRO_TOPIC", "astro")

    conn = psycopg2.connect(dsn)
    try:
        person_ids = _person_ids_embeddings_astro_missing_readings(conn)
        if not person_ids:
            print("No person_ids with embeddings and astro_features missing readings. Nothing to enqueue.")
            return

        print(f"Enqueueing astro.interpret for {len(person_ids)} people (embeddings+astro, missing readings) to topic '{topic}'...")

        producer = Producer({"bootstrap.servers": kafka_bootstrap, "client.id": "enqueue-interpret"})
        now_ms = int(time.time() * 1000)
        for i, person_id in enumerate(person_ids):
            job_id = str(uuid.uuid4())
            job = {
                "id": job_id,
                "function": "astro.interpret",
                "args": [person_id],
                "kwargs": {},
                "status": "QUEUED",
                "enqueuedAt": now_ms,
                "startedAt": None,
                "endedAt": None,
                "result": None,
                "excInfo": None,
            }
            cur = conn.cursor()
            _insert_job_status(cur, job)
            conn.commit()
            cur.close()
            producer.produce(topic, key=job_id.encode("utf-8"), value=json.dumps(job))
            if (i + 1) % 200 == 0:
                producer.flush()
                print(f"  produced {i + 1}/{len(person_ids)}")
        producer.flush()
        print(f"Done. Enqueued {len(person_ids)} astro.interpret jobs. Run astro-interpreter worker to process them.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
