import time
import os
import random
import re
import requests
import psycopg
from psycopg import rows as psycopg_rows
from psycopg.types.json import Json


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

def _normalize_name(name: str) -> str:
    cleaned = (name or "").strip()
    if "," not in cleaned:
        return cleaned
    last, first = cleaned.split(",", 1)
    last = last.strip()
    first = first.strip()
    if not first:
        return cleaned
    return f"{first} {last}"

def search_qid(session: requests.Session, limiter: RateLimiter, name):
    limiter.wait()
    r = session.get(
        "https://www.wikidata.org/w/api.php",
        params={
            "action": "wbsearchentities",
            "language": "en",
            "format": "json",
            "type": "item",
            "search": name,
        },
        timeout=20,
    )
    r.raise_for_status()
    return r.json().get("search", [])

def _extract_wikidata_date(time_value: str):
    match = re.search(r"[+-]\d{4}-\d{2}-\d{2}", time_value or "")
    if not match:
        return None
    return match.group(0).lstrip("+")


def dob_matches(session: requests.Session, limiter: RateLimiter, qid, dob_iso):
    try:
        limiter.wait()
        r = session.get(
            f"https://www.wikidata.org/wiki/Special:EntityData/{qid}.json",
            timeout=20,
        )
        r.raise_for_status()
        j = r.json()
        time = j["entities"][qid]["claims"]["P569"][0]["mainsnak"]["datavalue"]["value"]["time"]
        return dob_iso and dob_iso == _extract_wikidata_date(time)
    except Exception:
        return False

def run(dsn):
    started = time.monotonic()
    conn = psycopg.connect(dsn)
    cur = conn.cursor(row_factory=psycopg_rows.dict_row)
    session = _wiki_session()
    wikidata_limiter = RateLimiter(
        float(os.getenv("WIKIDATA_MIN_INTERVAL_SEC", "1.0")),
        float(os.getenv("WIKIDATA_JITTER_SEC", "0.2")),
    )
    cur.execute("""
      SELECT pr.person_id, pr.name, to_char(b.date,'YYYY-MM-DD') AS dob
      FROM person_raw pr
      JOIN birth b ON b.person_id=pr.person_id
      LEFT JOIN bio_text bt ON bt.person_id=pr.person_id
      WHERE bt.person_id IS NULL OR bt.qid IS NULL OR bt.qid = ''
      LIMIT 500
    """)
    rows = cur.fetchall()
    hits = 0
    for r in rows:
        name = r["name"]
        cands = search_qid(session, wikidata_limiter, _normalize_name(name))
        if not cands:
            cands = search_qid(session, wikidata_limiter, name)
        qid = None
        for c in cands[:10]:
            if dob_matches(session, wikidata_limiter, c["id"], r["dob"]):
                qid = c["id"]; break
        if not qid and cands: qid = cands[0]["id"]
        if not qid: continue

        cur.execute("""
          INSERT INTO bio_text (person_id, rev_id, qid, meta) VALUES (%s,0,%s,'{}'::jsonb)
          ON CONFLICT (person_id, rev_id) DO UPDATE SET qid=EXCLUDED.qid
        """, (r["person_id"], qid))
        hits += 1

    conn.commit()
    cur.execute(
        "INSERT INTO provenance_event (stage, detail) VALUES (%s, %s)",
        (
            "resolve_qid",
            Json(
                {
                    "status": "ok",
                    "count": hits,
                    "duration_ms": int((time.monotonic() - started) * 1000),
                }
            ),
        ),
    )
    conn.commit(); cur.close(); conn.close()

if __name__ == "__main__":
    import os; run(os.environ["PG_DSN"])
