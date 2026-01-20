"""Fetch and store Wikipedia biographies, then trigger downstream jobs.

This script:
- Finds people with QIDs but no biography text
- Fetches and cleans Wikipedia wikitext
- Updates the bio_text table
- Enqueues downstream jobs:
  - Traits scoring on the `traits` queue (Kotlin worker)
  - Semantic embeddings on the `embeddings` queue (Python RQ worker)
"""

import json
import os
import time
import uuid

import psycopg2
import psycopg2.extras
import requests
import re
from redis import Redis
from rq import Queue

def sitelink(qid, lang="en"):
    j = requests.get(f"https://www.wikidata.org/wiki/Special:EntityData/{qid}.json", timeout=20).json()
    ent = j["entities"][qid]; key=f"{lang}wiki"
    return ent.get("sitelinks",{}).get(key,{}).get("title")

def fetch_latest_wikitext(lang, title):
    r = requests.get(f"https://{lang}.wikipedia.org/w/rest.php/v1/page/{title}", timeout=20); r.raise_for_status()
    j = r.json()
    return j.get("latest",{}).get("id"), j.get("source")

def clean_wikitext(wt):
    wt = re.sub(r"==.*?==", "\n", wt)
    wt = re.sub(r"\{\{.*?\}\}", "", wt, flags=re.S)
    wt = re.sub(r"<ref.*?</ref>", "", wt, flags=re.S)
    wt = re.sub(r"\[\[(?:[^|\]]+\|)?([^\]]+)\]\]", r"\1", wt)
    paras = [p.strip() for p in wt.split("\n") if len(p.strip())>120]
    return "\n\n".join(paras[:20])

def _get_redis():
    """Create a Redis client from REDIS_URL or default."""
    url = os.getenv("REDIS_URL", "redis://redis:6379/0")
    return Redis.from_url(url)


def _enqueue_traits_job(redis: Redis, person_id):
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

    job_json = json.dumps(job)
    # Match Kotlin JobQueue storage layout
    pipe = redis.pipeline()
    pipe.lpush("rq:queue:traits", job_json)
    pipe.setex(f"rq:job:{job_id}", 86400, job_json)  # 1 day TTL, aligns with defaults
    pipe.execute()


def run(dsn, lang="en", limit=500):
    conn = psycopg2.connect(dsn)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # Redis / RQ setup for downstream jobs
    redis = _get_redis()
    emb_queue = Queue("embeddings", connection=redis)

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
        title = sitelink(r["qid"], lang)
        if not title:
            continue

        rev, wt = fetch_latest_wikitext(lang, title)
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
        _enqueue_traits_job(redis, person_id)

    conn.commit()

    # Batch enqueue embeddings job(s) for all enriched bios
    if enriched_ids:
        emb_queue.enqueue(
            "embeddings.jobs.embed_person_bios",
            {
                "person_ids": enriched_ids,
                "model": os.getenv("EMBEDDINGS_MODEL"),
                "source": f"fetch_bio:{lang}",
            },
            job_timeout=1800,
        )

    cur.execute(
        """INSERT INTO provenance_event (event_type,status,payload)
           VALUES ('fetch_bio','ok', jsonb_build_object('written',$1,'lang',$2))""",
        (wrote, lang),
    )

    conn.commit()
    cur.close()
    conn.close()
    return wrote


if __name__ == "__main__":
    run(os.environ["PG_DSN"], os.environ.get("WIKI_LANG", "en"))
