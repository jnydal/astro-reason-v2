# worker/ingest.py
import os, hashlib, tempfile
import boto3
from botocore.config import Config
import psycopg2, psycopg2.extras
from psycopg2.extras import execute_values
from redis import Redis
from rq import Queue

from app.utils.adb_parser_stream import iter_people

UPSERT_BATCH   = 500
ENQUEUE_BATCH  = 200
EMBED_MODEL    = os.getenv("EMBEDDINGS_MODEL", "BAAI/bge-large-en-v1.5")
REDIS_URL      = os.getenv("REDIS_URL", "redis://redis:6379/0")

def _s3():
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minio"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minio123"),
        region_name=os.getenv("MINIO_REGION", "us-east-1"),
        config=Config(connect_timeout=2, read_timeout=30, retries={"max_attempts": 3}),
    )

def _sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def _tz_to_minutes(tz: str) -> int | None:
    if not tz:
        return None
    sign = -1 if ('-' in tz or '−' in tz) else 1
    z = tz.replace('+', '').replace('-', '').replace('−', '')
    h, m = z.split(':') if ':' in z else (z, '0')
    return sign * (int(h) * 60 + int(m))

def parse_adb_xml(object_uri: str, meta: dict):
    # --- Download XML from MinIO to a temp file ---
    assert object_uri.startswith("s3://"), "Expected s3://bucket/key"
    _, rest = object_uri.split("s3://", 1)
    bucket, key = rest.split("/", 1)

    s3 = _s3()
    data = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    with tempfile.NamedTemporaryFile(suffix=".xml", delete=False) as tf:
        tf.write(data)
        xml_path = tf.name

    source = meta.get("source", "astrodb-upload")
    dsn = os.getenv("DATABASE_URL", "").replace("postgresql+psycopg://", "postgresql://")
    if not dsn:
        raise RuntimeError("DATABASE_URL env is required")

    touched_pids = set()
    records_seen = 0

    conn = psycopg2.connect(dsn)
    cur  = conn.cursor()

    birth_batch = []   # (person_id, date, time, tz_mins, place, lat, lon, rating)
    bio_batch   = []   # (person_id, text, text_hash, source)

    def flush_batches():
        if birth_batch:
            execute_values(cur, """
                INSERT INTO birth (person_id, date, time, tz_offset_minutes, place_name, lat, lon, data_quality)
                VALUES %s
                ON CONFLICT (person_id) DO UPDATE
                  SET date=EXCLUDED.date,
                      time=EXCLUDED.time,
                      tz_offset_minutes=EXCLUDED.tz_offset_minutes,
                      place_name=EXCLUDED.place_name,
                      lat=EXCLUDED.lat,
                      lon=EXCLUDED.lon,
                      data_quality=EXCLUDED.data_quality
            """, birth_batch, page_size=UPSERT_BATCH)
            birth_batch.clear()

        if bio_batch:
            execute_values(cur, """
                INSERT INTO bio_text (person_id, text, text_hash, source, updated_at)
                VALUES %s
                ON CONFLICT (person_id) DO UPDATE
                  SET text=EXCLUDED.text,
                      text_hash=EXCLUDED.text_hash,
                      source=EXCLUDED.source,
                      updated_at=NOW()
            """, bio_batch, page_size=UPSERT_BATCH)
            bio_batch.clear()

    # --- Stream parse people from the XML and upsert ---
    for rec in iter_people(xml_path):
        records_seen += 1

        # person_raw upsert (RETURNING person_id)
        cur.execute("""
            INSERT INTO person_raw (adb_id, full_name, adb_xml_path)
            VALUES (%s, %s, %s)
            ON CONFLICT (adb_id) DO UPDATE SET full_name = EXCLUDED.full_name
            RETURNING person_id
        """, (rec["adb_id"], rec["full_name"], xml_path))
        person_id = cur.fetchone()[0]
        touched_pids.add(person_id)

        # birth
        birth_batch.append((
            person_id,
            rec.get("date"),
            rec.get("time"),
            _tz_to_minutes(rec.get("tz")),
            rec.get("place"),
            rec.get("lat"),
            rec.get("lon"),
            rec.get("rating"),
        ))

        # bio_text (optional)
        bio_text = rec.get("bio_text")
        if bio_text:
            bio_batch.append((person_id, bio_text, _sha256(bio_text), source))

        # periodic flush
        if (len(birth_batch) + len(bio_batch)) >= UPSERT_BATCH:
            flush_batches()
            conn.commit()  # incremental commit for large files

    # final flush & commit before enqueue
    flush_batches()
    conn.commit()
    cur.close(); conn.close()

    # --- Enqueue embedding jobs on the "embeddings" queue ---
    jobs_enqueued = 0
    if touched_pids:
        redis = Redis.from_url(REDIS_URL)
        q = Queue("embeddings", connection=redis)  # <— embeddings queue
        pids = list(touched_pids)
        for i in range(0, len(pids), ENQUEUE_BATCH):
            batch = pids[i:i + ENQUEUE_BATCH]
            q.enqueue(
                "embeddings.jobs.embed_person_bios",  # dotted path in embeddings worker
                {"person_ids": batch, "model": EMBED_MODEL, "source": source},
                job_timeout=1800,
                retry_strategy={"max": 3, "interval": 30},
            )
            jobs_enqueued += 1

    return {
        "bytes": len(data),
        "records_seen": records_seen,
        "people_upserted": len(touched_pids),
        "jobs_enqueued": jobs_enqueued,
        "object_uri": object_uri,
        "source": source,
    }
