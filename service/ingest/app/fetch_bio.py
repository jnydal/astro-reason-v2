"""Fetch and store Wikipedia biographies, then trigger downstream jobs.

This script:
- Finds people with QIDs but no biography text
- Fetches and cleans Wikipedia wikitext
- Updates the bio_text table
- Enqueues downstream jobs via Kafka:
  - Semantic embeddings on the `embeddings` topic (Python worker)
"""

import json
import os
import time
import uuid
import random
import hashlib

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


def _get_with_retry(
    session: requests.Session,
    url: str,
    max_retries: int = 3,
) -> requests.Response:
    """GET with bounded retries on 429 (rate limit). Respects Retry-After header."""
    last_exc = None
    for attempt in range(max_retries):
        r = session.get(url, timeout=20)
        if r.status_code != 429:
            r.raise_for_status()
            return r
        last_exc = requests.exceptions.HTTPError(
            f"429 Too Many Requests: {r.text[:200]}", response=r
        )
        # Respect Retry-After if present; else exponential backoff (60s, 120s, 240s)
        retry_sec = 60
        retry_after = r.headers.get("Retry-After")
        if retry_after:
            try:
                retry_sec = int(retry_after)
            except ValueError:
                pass
        else:
            retry_sec = 60 * (2**attempt)
        if attempt < max_retries - 1:
            time.sleep(retry_sec)
    raise last_exc  # type: ignore[misc]


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
    r = _get_with_retry(
        session, f"https://www.wikidata.org/wiki/Special:EntityData/{qid}.json"
    )
    j = r.json()
    ent = j["entities"][qid]
    key = f"{lang}wiki"
    return ent.get("sitelinks", {}).get(key, {}).get("title")

def fetch_latest_wikitext(session: requests.Session, limiter: RateLimiter, lang, title):
    limiter.wait()
    r = _get_with_retry(
        session, f"https://{lang}.wikipedia.org/w/rest.php/v1/page/{title}"
    )
    j = r.json()
    page_id = j.get("id")
    latest = j.get("latest", {}) or {}
    rev_id = latest.get("id")
    return page_id, rev_id, j.get("source")

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


def _normalize_dsn(dsn: str) -> str:
    # psycopg2 expects postgresql://, not SQLAlchemy-style postgresql+psycopg://
    if dsn.startswith("postgresql+psycopg://"):
        return "postgresql://" + dsn[len("postgresql+psycopg://"):]
    if dsn.startswith("postgresql+psycopg2://"):
        return "postgresql://" + dsn[len("postgresql+psycopg2://"):]
    return dsn


def run(dsn, lang="en", limit=500):
    started = time.monotonic()
    conn = psycopg2.connect(_normalize_dsn(dsn))
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

    # Use limit only when explicitly set and > 0; otherwise process all eligible people.
    # Prioritize pending wiki enrichment (no fetch_bio in source) so new bios get embedded.
    base_query = """
        SELECT
            pr.id AS person_id,
            COALESCE(el.qid, bt.qid) AS qid,
            bt.text AS text,
            bt.source AS source,
            bt.text_hash AS existing_text_hash
        FROM person_raw pr
        LEFT JOIN entity_link el
          ON el.person_id = pr.id
        LEFT JOIN LATERAL (
            SELECT qid, text, source, text_hash
            FROM bio_text
            WHERE person_id = pr.id
            ORDER BY retrieved_at DESC NULLS LAST
            LIMIT 1
        ) bt ON TRUE
        WHERE COALESCE(el.qid, bt.qid) IS NOT NULL
        ORDER BY (COALESCE(bt.source, '') LIKE '%%fetch_bio%%') ASC NULLS FIRST, pr.id ASC
    """
    if limit and int(limit) > 0:
        cur.execute(base_query + " LIMIT %s", (int(limit),))
    else:
        cur.execute(base_query)
    rows = cur.fetchall()

    wrote = 0
    enriched_ids = []

    for r in rows:
        person_id = r["person_id"]
        title = sitelink(session, wikidata_limiter, r["qid"], lang)
        if not title:
            continue

        page_id, rev, wt = fetch_latest_wikitext(session, wikipedia_limiter, lang, title)
        if not wt:
            continue

        text = clean_wikitext(wt)
        if not text:
            continue
        existing_text = (r.get("text") or "").strip()
        combined_text = text
        if existing_text:
            # Append fetched bio only if it's not already present.
            combined_text = existing_text if text in existing_text else f"{existing_text}\n\n{text}"

        text_hash = hashlib.sha256(combined_text.encode("utf-8")).hexdigest()
        page_url = f"https://{lang}.wikipedia.org/wiki/{title}"
        existing_source = (r.get("source") or "").strip()
        if existing_source and f"fetch_bio:{lang}" not in existing_source:
            combined_source = f"{existing_source},fetch_bio:{lang}"
        else:
            combined_source = f"fetch_bio:{lang}"

        # Update stub row in place; do NOT change rev_id (avoids PK violation when
        # a row with wiki rev_id already exists from a previous fetch).
        cur.execute(
            """
          UPDATE bio_text
             SET lang=%s,
                 wiki_pageid=%s,
                 url=%s,
                 license=%s,
                 text=%s,
                 text_sha256=%s,
                 text_hash=%s,
                 retrieved_at=NOW(),
                 char_count=%s,
                 source=%s,
                 updated_at=NOW()
           WHERE person_id=%s
             AND (rev_id = 0 OR rev_id IS NULL)
        """,
            (
                lang,
                page_id,
                page_url,
                "CC BY-SA 4.0 (Wikipedia)",
                combined_text,
                text_hash,
                text_hash,
                len(combined_text),
                combined_source,
                person_id,
            ),
        )
        if cur.rowcount == 0:
            cur.execute(
                """
              INSERT INTO bio_text
                  (person_id, qid, lang, wiki_pageid, rev_id, url, license, text, text_sha256, text_hash,
                   retrieved_at, char_count, source, updated_at)
              VALUES
                  (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s, %s, NOW())
              ON CONFLICT (person_id, rev_id) DO UPDATE
                SET lang=EXCLUDED.lang,
                    wiki_pageid=EXCLUDED.wiki_pageid,
                    url=EXCLUDED.url,
                    license=EXCLUDED.license,
                    text=EXCLUDED.text,
                    text_sha256=EXCLUDED.text_sha256,
                    text_hash=EXCLUDED.text_hash,
                    retrieved_at=NOW(),
                    char_count=EXCLUDED.char_count,
                    source=EXCLUDED.source,
                    updated_at=NOW()
            """,
                (
                    person_id,
                    r["qid"],
                    lang,
                    page_id,
                    rev or 0,
                    page_url,
                    "CC BY-SA 4.0 (Wikipedia)",
                    combined_text,
                    text_hash,
                    text_hash,
                    len(combined_text),
                    combined_source,
                ),
            )

        wrote += 1
        # Only enqueue for embeddings when text actually changed; avoids redundant
        # jobs where embeddings worker would noop (already_embedded).
        existing_hash = (r.get("existing_text_hash") or "").strip()
        if text_hash != existing_hash:
            enriched_ids.append(person_id)

    conn.commit()

    # Enqueue everyone we enriched. The embeddings worker is idempotent: it will
    # noop when text_hash is unchanged and overwrite when bio_text changed.
    # This supports backfills: embed stubs first, then fetch-bio enriches, then
    # re-embed with higher-quality Wikipedia text.
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
    return wrote, len(enriched_ids)


if __name__ == "__main__":
    run(os.environ["PG_DSN"], os.environ.get("WIKI_LANG", "en"))
