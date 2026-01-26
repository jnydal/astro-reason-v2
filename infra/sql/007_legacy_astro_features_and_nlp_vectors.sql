-- Backfill missing schema for existing dev databases.
-- 1) Create nlp_vectors if it doesn't exist (defined in 000_init.sql).
-- 2) Upgrade legacy astro_features schema if it only has a "features" column.

-- 1) nlp_vectors (safe if it already exists)
CREATE TABLE IF NOT EXISTS nlp_vectors (
  id BIGSERIAL PRIMARY KEY,
  person_id UUID NOT NULL REFERENCES person_raw(id) ON DELETE CASCADE,
  vectors JSONB NOT NULL,        -- {"sound":6, "visual":4, ...}
  dominant TEXT[] NOT NULL,      -- ["sound","visual"]
  confidence DOUBLE PRECISION NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
  model_name TEXT NOT NULL,      -- e.g. "qwen2.5:7b-instruct-q4_K_M"
  provider TEXT NOT NULL,        -- "ollama"
  temperature DOUBLE PRECISION NOT NULL,
  prompt_hash TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_nlp_vectors_person ON nlp_vectors(person_id);

-- 2) astro_features upgrade path for legacy schema
DO $$
BEGIN
  -- Only run if legacy "features" column exists.
  IF EXISTS (
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'astro_features'
      AND column_name = 'features'
  ) THEN
    -- Add columns from 001_astro_features.sql if missing.
    ALTER TABLE astro_features
      ADD COLUMN IF NOT EXISTS system TEXT,
      ADD COLUMN IF NOT EXISTS jd_utc DOUBLE PRECISION,
      ADD COLUMN IF NOT EXISTS unknown_time BOOLEAN NOT NULL DEFAULT FALSE,
      ADD COLUMN IF NOT EXISTS longs JSONB,
      ADD COLUMN IF NOT EXISTS houses JSONB,
      ADD COLUMN IF NOT EXISTS aspects JSONB,
      ADD COLUMN IF NOT EXISTS elem_ratios JSONB,
      ADD COLUMN IF NOT EXISTS modality_ratios JSONB,
      ADD COLUMN IF NOT EXISTS feature_vec JSONB;

    -- Drop legacy column last.
    ALTER TABLE astro_features
      DROP COLUMN IF EXISTS features;
  END IF;
END $$;

-- Generated columns and indexes (safe if already present).
ALTER TABLE astro_features
  ADD COLUMN IF NOT EXISTS elem_fire  DOUBLE PRECISION GENERATED ALWAYS AS ((elem_ratios->>'fire')::DOUBLE PRECISION) STORED,
  ADD COLUMN IF NOT EXISTS elem_earth DOUBLE PRECISION GENERATED ALWAYS AS ((elem_ratios->>'earth')::DOUBLE PRECISION) STORED,
  ADD COLUMN IF NOT EXISTS elem_air   DOUBLE PRECISION GENERATED ALWAYS AS ((elem_ratios->>'air')::DOUBLE PRECISION) STORED,
  ADD COLUMN IF NOT EXISTS elem_water DOUBLE PRECISION GENERATED ALWAYS AS ((elem_ratios->>'water')::DOUBLE PRECISION) STORED;

CREATE INDEX IF NOT EXISTS idx_astro_features_system
  ON astro_features (system);

CREATE INDEX IF NOT EXISTS idx_astro_features_elem
  ON astro_features USING GIN (elem_ratios);

CREATE INDEX IF NOT EXISTS idx_astro_features_mod
  ON astro_features USING GIN (modality_ratios);

CREATE INDEX IF NOT EXISTS idx_astro_features_vec
  ON astro_features USING GIN (feature_vec);

CREATE INDEX IF NOT EXISTS idx_astro_features_elem_values
  ON astro_features (elem_fire, elem_earth, elem_air, elem_water);
