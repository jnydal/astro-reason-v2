-- Astrological readings produced by LLM from chart data (planets, houses, aspects, elements).
-- Used for semantic comparison with biography embeddings.

CREATE TABLE IF NOT EXISTS astro_interpretations (
    person_id UUID PRIMARY KEY
        REFERENCES person_raw(id)
        ON DELETE CASCADE,

    interpretation_text TEXT NOT NULL,
    model_name TEXT NOT NULL,
    prompt_hash TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_astro_interpretations_model
    ON astro_interpretations (model_name);
