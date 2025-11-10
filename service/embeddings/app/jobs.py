# embeddings/jobs.py
import os
import numpy as np
from datetime import datetime
import psycopg2, psycopg2.extras
from sentence_transformers import SentenceTransformer
from app.core.provenance import log_event  # optional helper

EMBED_MODEL = os.getenv("EMBEDDINGS_MODEL", "BAAI/bge-large-en-v1.5")
DSN = os.getenv("DATABASE_URL", "").replace("postgresql+psycopg://", "postgresql://")

def embed_person_bios(payload: dict):
    """RQ job: Embed bios for given person_ids and upsert into embeddings table."""
    person_ids = payload.get("person_ids") or []
    model_name = payload.get("model", EMBED_MODEL)
    source = payload.get("source", "astrodb-upload")

    if not person_ids:
        print("⚠️ No person_ids provided to embed_person_bios.")
        return {"status": "no_ids"}

    print(f"Embedding {len(person_ids)} bios using {model_name}...")

    conn = psycopg2.connect(DSN)
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # Fetch texts that need embeddings (missing or text_hash changed)
    cur.execute("""
        SELECT bt.person_id, bt.text, bt.text_hash, e.text_hash AS existing_hash
        FROM bio_text bt
        LEFT JOIN embeddings e
          ON e.person_id = bt.person_id AND e.model_name = %s
        WHERE bt.person_id = ANY(%s)
    """, (model_name, person_ids))
    rows = cur.fetchall()

    # Filter only new or changed bios
    todo = [r for r in rows if not r["existing_hash"] or r["existing_hash"] != r["text_hash"]]
    if not todo:
        print("No new or changed bios to embed.")
        cur.close(); conn.close()
        return {"status": "noop", "count": 0}

    texts = [r["text"] for r in todo]
    pids  = [r["person_id"] for r in todo]

    # Encode embeddings
    model = SentenceTransformer(model_name)
    embeddings = model.encode(
        texts, batch_size=8, show_progress_bar=False, normalize_embeddings=True
    )
    embeddings = np.array(embeddings, dtype=np.float32)

    # Upsert into embeddings table
    for pid, vec, row in zip(pids, embeddings, todo):
        cur.execute("""
            INSERT INTO embeddings (person_id, model_name, dim, vector, text_hash, meta, source, updated_at)
            VALUES (%s, %s, %s, %s, %s, jsonb_build_object('provider','sentence-transformers'), %s, NOW())
            ON CONFLICT (person_id, model_name) DO UPDATE
              SET dim = EXCLUDED.dim,
                  vector = EXCLUDED.vector,
                  text_hash = EXCLUDED.text_hash,
                  meta = EXCLUDED.meta,
                  source = EXCLUDED.source,
                  updated_at = NOW()
        """, (pid, model_name, len(vec), vec.tolist(), row["text_hash"], source))

    conn.commit()

    # Optional provenance logging
    log_event(cur, event_type="embed_bio", status="ok", payload={
        "model": model_name,
        "count": len(todo),
        "timestamp": datetime.utcnow().isoformat()
    })
    conn.commit()
    cur.close(); conn.close()

    print(f"✅ Embedded {len(todo)} bios.")
    return {"status": "ok", "count": len(todo)}
