# app/workers/resolve_qid.py
import time
import requests, psycopg2, psycopg2.extras

def search_qid(name):
    r = requests.get("https://www.wikidata.org/w/api.php", params={
        "action":"wbsearchentities","language":"en","format":"json","type":"item","search":name
    }, timeout=20); r.raise_for_status(); return r.json().get("search",[])

def dob_matches(qid, dob_iso):
    try:
        j = requests.get(f"https://www.wikidata.org/wiki/Special:EntityData/{qid}.json", timeout=20).json()
        time = j["entities"][qid]["claims"]["P569"][0]["mainsnak"]["datavalue"]["value"]["time"]
        return dob_iso and dob_iso in time
    except Exception: return False

def run(dsn):
    started = time.monotonic()
    conn = psycopg2.connect(dsn)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute("""
      SELECT pr.person_id, pr.full_name, to_char(b.date,'YYYY-MM-DD') AS dob
      FROM person_raw pr
      JOIN birth b ON b.person_id=pr.person_id
      LEFT JOIN bio_text bt ON bt.person_id=pr.person_id
      WHERE bt.person_id IS NULL
      LIMIT 500
    """)
    rows = cur.fetchall()
    hits = 0
    for r in rows:
        cands = search_qid(r["full_name"])
        qid = None
        for c in cands[:10]:
            if dob_matches(c["id"], r["dob"]):
                qid = c["id"]; break
        if not qid and cands: qid = cands[0]["id"]
        if not qid: continue

        cur.execute("""
          INSERT INTO bio_text (person_id, qid, meta) VALUES (%s,%s,'{}'::jsonb)
          ON CONFLICT (person_id) DO UPDATE SET qid=EXCLUDED.qid
        """, (r["person_id"], qid))
        hits += 1

    conn.commit()
    cur.execute(
        "INSERT INTO provenance_event (stage, detail) VALUES (%s, %s)",
        (
            "resolve_qid",
            psycopg2.extras.Json(
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
