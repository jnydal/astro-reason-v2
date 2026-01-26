"""Fetch and store Wikipedia biographies, then trigger downstream jobs.

This script:
- Finds people with QIDs but no biography text
- Fetches and cleans Wikipedia wikitext
- Updates the bio_text table
- Enqueues downstream jobs via Kafka:
  - Traits scoring on the `traits` topic (Kotlin worker)
  - Semantic embeddings on the `embeddings` topic (Python worker)
"""

import json
import os
import time
import uuid
import random

import psycopg2
import psycopg2.extras
import requests
import re
from confluent_kafka import Producer

class RateLimiter:
    def __init__(self, min_interval_sec: float, jitter_sec: float = 0.0):
        self._min_interval = max(min_interval_sec, 0.0)
        self._jitter = max(jitter_sec, 0.0)
        self._next_time = 0.0

    def wait(self) -> None:
        now = time.monotonic()
        if now < self._next_time:
            time.sleep(self._next_time - now)
        if self._jitter:
            time.sleep(random.uniform(0, self._jitter))
        self._next_time = time.monotonic() + self._min_interval


def _wiki_session() -> requests.Session:
    user_agent = os.getenv(
        "WIKI_USER_AGENT",
        "astro-reason/0.1 (contact: you@example.com)",
    )
    session = requests.Session()
    session.headers.update({"User-Agent": user_agent})
    return session


def sitelink(session: requests.Session, limiter: RateLimiter, qid, lang="en"):
    limiter.wait()
    r = session.get(
        f"https://www.wikidata.org/wiki/Special:EntityData/{qid}.json",
        timeout=20,
    )
    r.raise_for_status()
    j = r.json()
    ent = j["entities"][qid]
    key = f"{lang}wiki"
    return ent.get("sitelinks", {}).get(key, {}).get("title")

def fetch_latest_wikitext(session: requests.Session, limiter: RateLimiter, lang, title):
    limiter.wait()
    r = session.get(
        f"https://{lang}.wikipedia.org/w/rest.php/v1/page/{title}",
        timeout=20,
    )
    r.raise_for_status()
    j = r.json()
    return j.get("latest", {}).get("id"), j.get("source")

def clean_wikitext(wt):
    wt = re.sub(r"==.*?==", "\n", wt)
    wt = re.sub(r"\{\{.*?\}\}", "", wt, flags=re.S)
    wt = re.sub(r"<ref.*?</ref>", "", wt, flags=re.S)
    wt = re.sub(r"\[\[(?:[^|\]]+\|)?([^\]]+)\]\]", r"\1", wt)
    paras = [p.strip() for p in wt.split("\n") if len(p.strip())>120]
    return "\n\n".join(paras[:20])

def _get_producer():
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    return Producer({"bootstrap.servers": bootstrap, "client.id": "fetch-bio"})


def _insert_job_status(cur, job: dict) -> None:
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


def _enqueue_traits_job(producer: Producer, cur, person_id):
    """Enqueue a traits scoring job in the JSON format used by the Kotlin JobQueue."""
    job_id = str(uuid.uuid4())
    now_ms = int(time.time() * 1000)

    job = {
        "id": job_id,
        "function": "traits.score_person",
        "args": [str(person_id)],
        "kwargs": {},
        "status": "QUEUED",
        "enqueuedAt": now_ms,
        "startedAt": None,
        "endedAt": None,
        "result": None,
        "excInfo": None,
    }

    _insert_job_status(cur, job)
    producer.produce(os.getenv("KAFKA_TRAITS_TOPIC", "traits"), key=job_id, value=json.dumps(job))


def run(dsn, lang="en", limit=500):
    started = time.monotonic()
    conn = psycopg2.connect(dsn)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # Kafka producer for downstream jobs
    producer = _get_producer()
    session = _wiki_session()
    wikidata_limiter = RateLimiter(
        float(os.getenv("WIKIDATA_MIN_INTERVAL_SEC", "1.0")),
        float(os.getenv("WIKIDATA_JITTER_SEC", "0.2")),
    )
    wikipedia_limiter = RateLimiter(
        float(os.getenv("WIKIPEDIA_MIN_INTERVAL_SEC", "1.0")),
        float(os.getenv("WIKIPEDIA_JITTER_SEC", "0.2")),
    )

    cur.execute(
        "SELECT person_id, qid FROM bio_text "
        "WHERE text IS NULL AND qid IS NOT NULL "
        "LIMIT %s",
        (limit,),
    )
    rows = cur.fetchall()

    wrote = 0
    enriched_ids = []

    for r in rows:
        person_id = r["person_id"]
        title = sitelink(session, wikidata_limiter, r["qid"], lang)
        if not title:
            continue

        rev, wt = fetch_latest_wikitext(session, wikipedia_limiter, lang, title)
        if not wt:
            continue

        text = clean_wikitext(wt)
        if not text:
            continue

        cur.execute(
            """
          UPDATE bio_text
             SET wiki_lang=%s,
                 wiki_page=%s,
                 wiki_rev_id=%s,
                 license=%s,
                 text=%s,
                 meta = COALESCE(meta,'{}'::jsonb) || jsonb_build_object('source','wikitext')
           WHERE person_id=%s
        """,
            (lang, title, rev, "CC BY-SA 4.0 (Wikipedia)", text, person_id),
        )

        wrote += 1
        enriched_ids.append(person_id)

        # Enqueue traits scoring for this person
        _enqueue_traits_job(producer, cur, person_id)

    conn.commit()

    # Batch enqueue embeddings job(s) for all enriched bios
    if enriched_ids:
        job_id = str(uuid.uuid4())
        now_ms = int(time.time() * 1000)
        job = {
            "id": job_id,
            "function": "embeddings.embed_person_bios",
            "args": [],
            "kwargs": {
                "person_ids": enriched_ids,
                "model": os.getenv("EMBEDDINGS_MODEL"),
                "source": f"fetch_bio:{lang}",
            },
            "status": "QUEUED",
            "enqueuedAt": now_ms,
            "startedAt": None,
            "endedAt": None,
            "result": None,
            "excInfo": None,
        }
        _insert_job_status(cur, job)
        producer.produce(
            os.getenv("KAFKA_EMBEDDINGS_TOPIC", "embeddings"),
            key=job_id,
            value=json.dumps(job),
        )

    cur.execute(
        "INSERT INTO provenance_event (stage, detail) VALUES (%s, %s)",
        (
            "fetch_bio",
            psycopg2.extras.Json(
                {
                    "status": "ok",
                    "count": wrote,
                    "duration_ms": int((time.monotonic() - started) * 1000),
                    "meta": {"lang": lang},
                }
            ),
        ),
    )

    producer.flush()
    conn.commit()
    cur.close()
    conn.close()
    return wrote


if __name__ == "__main__":
    run(os.environ["PG_DSN"], os.environ.get("WIKI_LANG", "en"))
