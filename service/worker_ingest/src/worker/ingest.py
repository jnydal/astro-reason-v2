# worker/ingest.py (core loop)
import hashlib, os, tempfile
import psycopg2, psycopg2.extras
from redis import Redis
from rq import Queue
from app.utils.adb_parser_stream import iter_people

UPSERT_BATCH   = 500
ENQUEUE_BATCH  = 200
EMBED_MODEL    = os.getenv("EMBEDDINGS_MODEL", "BAAI/bge-large-en-v1.5")
REDIS_URL      = os.getenv("REDIS_URL", "redis://redis:6379/0")

def _sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def _tz_to_minutes(tz: str) -> int | None:
    if not tz: return None
    sign = -1 if ('-' in tz or '−' in tz) else 1
    z = tz.replace('+','').replace('-','').replace('−','')
    h, m = z.split(':') if ':' in z else (z, '0')
    return sign*(int(h)*60 + int(m))

def parse_adb_xml(object_uri: str, meta: dict):
    # ... download to xml_path (as you already do) ...
    source = meta.get("source", "astrodb-upload")
    dsn = os.getenv("DATABASE_URL").replace("postgresql+psycopg://","postgresql://")

    touched_pids = set()
    records_seen = 0

    conn = psycopg2.connect(dsn)
    cur = conn.cursor()

    person_batch = []      # for person_raw
    birth_batch  = []      # for birth
    bio_batch    = []      # for bio_text

    def flush_batches():
        # person_raw
        if person_batch:
            psycopg2.extras.execute_values(cur, """
                INSERT INTO person_raw (adb_id, full_name, adb_xml_path)
                VALUES %s
                ON CONFLICT (adb_id) DO UPDATE SET full_name = EXCLUDED.full_name
            """, person_batch, page_size=UPSERT_BATCH)
            person_batch.clear()
        # birth
        if birth_batch:
            psycopg2.extras.execute_values(cur, """
                INSERT INTO birth (person_id, date, time, tz_offset_minutes, place_name, lat, lon, data_quality)
                VALUES %s
                ON CONFLICT (person_id) DO UPDATE
                  SET date=EXCLUDED.date, time=EXCLUDED.time, tz_offset_minutes=EXCLUDED.tz_offset_minutes,
                      place_name=EXCLUDED.place_name, lat=EXCLUDED.lat, lon=EXCLUDED.lon, data_quality=EXCLUDED.data_quality
            """, birth_batch, page_size=UPSERT_BATCH)
            birth_batch.clear()
        # bio_text
        if bio_batch:
            psycopg2.extras.execute_values(cur, """
                INSERT INTO bio_text (person_id, text, text_hash, source, updated_at)
                VALUES %s
                ON CONFLICT (person_id) DO UPDATE
                  SET text=EXCLUDED.text, text_hash=EXCLUDED.text_hash, source=EXCLUDED.source, updated_at=NOW()
            """, bio_batch, page_size=UPSERT_BATCH)
            bio_batch.clear()

    # stream parse
    for rec in iter_people(xml_path):
        records_seen += 1

        # get or create person_id by adb_id (returning id)
        cur.execute("""
            INSERT INTO person_raw (adb_id, full_name, adb_xml_path)
            VALUES (%s,%s,%s)
            ON CONFLICT (adb_id) DO UPDATE SET full_name = EXCLUDED.full_name
            RETURNING person_id
        """, (rec["adb_id"], rec["full_name"], xml_path))
        person_id = cur.fetchone()[0]
        touched_pids.add(person_id)

        tz_mins = _tz_to_minutes(rec["tz"])
        birth_batch.append((person_id, rec["date"], rec["time"], tz_mins, rec["place"], rec["lat"], rec["lon"], rec["rating"]))

        if rec["bio_text"]:
            bio_hash = _sha256(rec["bio_text"])
            bio_batch.append((person_id, rec["bio_text"], bio_hash, source))

        if (len(birth_batch) + len(bio_batch)) >= UPSERT_BATCH:
            flush_batches()
            conn.commit()  # commit incrementally for big files

    flush_batches()
    conn.commit()      # ✅ durable before enqueue
    cur.close(); conn.close()

    # enqueue traits jobs
    if touched_pids:
        redis = Redis.from_url(REDIS_URL)
        q = Queue("traits", connection=redis)
        pids = list(touched_pids)
        for i in range(0, len(pids), ENQUEUE_BATCH):
            q.enqueue(
                "traits.embed.embed_person_bios",
                {"person_ids": pids[i:i+ENQUEUE_BATCH], "model": EMBED_MODEL, "source": source},
                job_timeout=1800,
                retry_strategy={"max": 3, "interval": 30},
            )

    return {
        "bytes": os.path.getsize(xml_path),
        "records_seen": records_seen,
        "people_upserted": len(touched_pids),
        "jobs_enqueued": (len(touched_pids) + ENQUEUE_BATCH - 1) // ENQUEUE_BATCH,
        "object_uri": object_uri,
        "source": source,
    }
