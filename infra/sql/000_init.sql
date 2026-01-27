CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE person_raw (
  id UUID PRIMARY KEY,
  xml_id TEXT,
  name TEXT NOT NULL,
  biography_stub TEXT,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE birth (
  person_id UUID PRIMARY KEY REFERENCES person_raw(id) ON DELETE CASCADE,
  date DATE,
  time TIME,
  tz_offset_minutes INT,
  tz TEXT,
  lat DOUBLE PRECISION,
  lon DOUBLE PRECISION
);

CREATE TABLE entity_link (
  person_id UUID PRIMARY KEY REFERENCES person_raw(id) ON DELETE CASCADE,
  qid TEXT,
  method TEXT,
  score DOUBLE PRECISION,
  candidates_json JSONB,
  decided_at TIMESTAMPTZ
);

CREATE TABLE bio_text (
  person_id UUID REFERENCES person_raw(id) ON DELETE CASCADE,
  qid TEXT,
  text TEXT,
  text_sha256 TEXT,
  lang TEXT,
  wiki_pageid BIGINT,
  rev_id BIGINT,
  url TEXT,
  license TEXT,
  retrieved_at TIMESTAMPTZ,
  char_count INT,
  text_uri TEXT,
  PRIMARY KEY(person_id, rev_id)
);

CREATE TABLE nlp_traits (
  person_id UUID PRIMARY KEY REFERENCES person_raw(id) ON DELETE CASCADE,
  model TEXT,
  version TEXT,
  scores_json JSONB,
  rationale_json JSONB,
  prompt_hash TEXT,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE provenance_event (
  id BIGSERIAL PRIMARY KEY,
  person_id UUID REFERENCES person_raw(id) ON DELETE CASCADE,
  stage TEXT,              -- xml_parse, resolve_qid, fetch_bio, etc.
  detail JSONB,
  created_at TIMESTAMPTZ DEFAULT now()
);

-- Burlan vectors scoring results
CREATE TABLE nlp_vectors (
  id BIGSERIAL PRIMARY KEY,
  person_id UUID NOT NULL REFERENCES person_raw(id) ON DELETE CASCADE,
  vectors JSONB NOT NULL,        -- {"sound":6, "visual":4, ...}
  dominant TEXT NOT NULL,        -- comma-separated list
  confidence DOUBLE PRECISION NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
  model_name TEXT NOT NULL,      -- e.g. "qwen2.5:7b-instruct-q4_K_M"
  provider TEXT NOT NULL,        -- "ollama"
  temperature DOUBLE PRECISION NOT NULL,
  prompt_hash TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_nlp_vectors_person ON nlp_vectors(person_id);
