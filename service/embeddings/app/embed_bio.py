import numpy as np
from datetime import datetime
from app.core.db import pg_conn, pg_cursor
from app.core.settings import settings
from app.core.provenance import log_event  # optional helper
import psycopg2.extras

# You can swap this for any embedding backend you like:
# - sentence-transformers (BAAI/bge-large-en-v1.5)
# - Ollama embeddings endpoint
# - OpenAI API if you ever use cloud
from sentence_transformers import SentenceTransformer


def embed_texts(texts: list[str], model_name: str) -> np.ndarray:
    """Batch encode texts into vectors."""
    model = SentenceTransformer(model_name)
    embeddings = model.encode(texts, batch_size=8, show_progress_bar=True, normalize_embeddings=True)
    return np.array(embeddings, dtype=np.float32)


def run(limit: int = 100):
    model_name = settings.EMBEDDINGS_MODEL
    with pg_conn() as conn, pg_cursor(conn) as cur:
        # 1. Find bios missing embeddings
        cur.execute("""
            SELECT bt.person_id, bt.text
            FROM bio_text bt
            LEFT JOIN embeddings e ON e.person_id = bt.person_id
            WHERE bt.text IS NOT NULL AND e.person_id IS NULL
            LIMIT %s
        """, (limit,))
        rows = cur.fetchall()

        if not rows:
            print("No unembedded bios found.")
            return

        texts = [r["text"] for r in rows]
        person_ids = [r["person_id"] for r in rows]

        print(f"Embedding {len(rows)} bios using model {model_name}...")

        # 2. Generate embeddings
        vectors = embed_texts(texts, model_name)

        # 3. Store results
        for pid, vec in zip(person_ids, vectors):
            cur.execute("""
                INSERT INTO embeddings (person_id, model_name, dim, vector, meta)
                VALUES (%s, %s, %s, %s, jsonb_build_object('provider','sentence-transformers'))
                ON CONFLICT (person_id) DO UPDATE
                  SET model_name = EXCLUDED.model_name,
                      dim = EXCLUDED.dim,
                      vector = EXCLUDED.vector,
                      meta = EXCLUDED.meta
            """, (pid, model_name, len(vec), vec.tolist()))

        conn.commit()

        # 4. Log provenance
        log_event(cur, event_type="embed_bio", status="ok", payload={
            "model": model_name,
            "count": len(rows),
            "timestamp": datetime.utcnow().isoformat()
        })
        conn.commit()

    print("✅ Embeddings written to DB.")


if __name__ == "__main__":
    run()
