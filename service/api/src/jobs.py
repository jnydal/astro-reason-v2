import json
import os
import time
import uuid

import psycopg2
import psycopg2.extras
from confluent_kafka import Producer

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
DEFAULT_TOPIC = os.getenv("KAFKA_DEFAULT_TOPIC", "default")

_producer = Producer(
    {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "client.id": "api-producer",
    }
)

def _dsn() -> str:
    return os.getenv("PG_DSN") or os.getenv("DATABASE_URL") or ""

def _insert_job(job: dict) -> None:
    dsn = _dsn()
    if not dsn:
        raise RuntimeError("PG_DSN or DATABASE_URL must be set")
    conn = psycopg2.connect(dsn)
    cur = conn.cursor()
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
    conn.commit()
    cur.close()
    conn.close()

def enqueue_parse_adb_xml(object_uri: str, source_label: str = "upload") -> dict:
    job_id = str(uuid.uuid4())
    job = {
        "id": job_id,
        "function": "worker.ingest.parse_adb_xml",
        "args": [object_uri],
        "kwargs": {"source": source_label},
        "status": "QUEUED",
        "enqueuedAt": int(time.time() * 1000),
        "startedAt": None,
        "endedAt": None,
        "result": None,
        "excInfo": None,
    }

    _insert_job(job)
    _producer.produce(DEFAULT_TOPIC, key=job_id, value=json.dumps(job))
    _producer.flush()
    return job

def fetch_job_status(job_id: str) -> dict | None:
    dsn = _dsn()
    if not dsn:
        raise RuntimeError("PG_DSN or DATABASE_URL must be set")
    conn = psycopg2.connect(dsn)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(
        """
        SELECT id, status, enqueued_at, started_at, ended_at, exc_info, result
        FROM job_status
        WHERE id = %s
        """,
        (job_id,),
    )
    row = cur.fetchone()
    cur.close()
    conn.close()
    return row
