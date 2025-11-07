# app/workers/fetch_bio.py
import requests, re, psycopg2, psycopg2.extras

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

def run(dsn, lang="en"):
    conn = psycopg2.connect(dsn); cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute("SELECT person_id, qid FROM bio_text WHERE text IS NULL AND qid IS NOT NULL LIMIT 500")
    rows = cur.fetchall()
    wrote = 0
    for r in rows:
        title = sitelink(r["qid"], lang); if not title: continue
        rev, wt = fetch_latest_wikitext(lang, title); if not wt: continue
        text = clean_wikitext(wt)
        if not text: continue
        cur.execute("""
          UPDATE bio_text
             SET wiki_lang=%s, wiki_page=%s, wiki_rev_id=%s, license=%s, text=%s,
                 meta = COALESCE(meta,'{}'::jsonb) || jsonb_build_object('source','wikitext')
           WHERE person_id=%s
        """, (lang, title, rev, "CC BY-SA 4.0 (Wikipedia)", text, r["person_id"]))
        wrote += 1

    conn.commit()
    cur.execute("""INSERT INTO provenance_event (event_type,status,payload)
                   VALUES ('fetch_bio','ok', jsonb_build_object('written',$1,'lang',$2))""",
                (wrote, lang))
    conn.commit(); cur.close(); conn.close()

if __name__ == "__main__":
    import os; run(os.environ["PG_DSN"], os.environ.get("WIKI_LANG","en"))
