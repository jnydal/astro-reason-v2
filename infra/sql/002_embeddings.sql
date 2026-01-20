CREATE EXTENSION IF NOT EXISTS vector;

-- 384
CREATE TABLE IF NOT EXISTS embeddings_384 (
  person_id UUID NOT NULL REFERENCES person_raw(id) ON DELETE CASCADE,
  model_name TEXT NOT NULL,
  dim INT NOT NULL CHECK (dim = 384),
  vector vector(384) NOT NULL,
  text_hash TEXT,
  meta JSONB,
  source TEXT,
  updated_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (person_id, model_name)
);

-- 768
CREATE TABLE IF NOT EXISTS embeddings_768 (
  person_id UUID NOT NULL REFERENCES person_raw(id) ON DELETE CASCADE,
  model_name TEXT NOT NULL,
  dim INT NOT NULL CHECK (dim = 768),
  vector vector(768) NOT NULL,
  text_hash TEXT,
  meta JSONB,
  source TEXT,
  updated_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (person_id, model_name)
);

-- 1024
CREATE TABLE IF NOT EXISTS embeddings_1024 (
  person_id UUID NOT NULL REFERENCES person_raw(id) ON DELETE CASCADE,
  model_name TEXT NOT NULL,
  dim INT NOT NULL CHECK (dim = 1024),
  vector vector(1024) NOT NULL,
  text_hash TEXT,
  meta JSONB,
  source TEXT,
  updated_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (person_id, model_name)
);

-- 1536 (optional)
CREATE TABLE IF NOT EXISTS embeddings_1536 (
  person_id UUID NOT NULL REFERENCES person_raw(id) ON DELETE CASCADE,
  model_name TEXT NOT NULL,
  dim INT NOT NULL CHECK (dim = 1536),
  vector vector(1536) NOT NULL,
  text_hash TEXT,
  meta JSONB,
  source TEXT,
  updated_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (person_id, model_name)
);

-- Convenience view
CREATE OR REPLACE VIEW embeddings AS
SELECT * FROM embeddings_384
UNION ALL
SELECT * FROM embeddings_768
UNION ALL
SELECT * FROM embeddings_1024
UNION ALL
SELECT * FROM embeddings_1536;

CREATE OR REPLACE FUNCTION embeddings_insert_router()
RETURNS trigger AS $$
BEGIN
  IF NEW.dim = 384 THEN
    INSERT INTO embeddings_384 VALUES (NEW.*);
  ELSIF NEW.dim = 768 THEN
    INSERT INTO embeddings_768 VALUES (NEW.*);
  ELSIF NEW.dim = 1024 THEN
    INSERT INTO embeddings_1024 VALUES (NEW.*);
  ELSIF NEW.dim = 1536 THEN
    INSERT INTO embeddings_1536 VALUES (NEW.*);
  ELSE
    RAISE EXCEPTION 'Unsupported embedding dimension: %', NEW.dim;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS embeddings_insert ON embeddings;
CREATE TRIGGER embeddings_insert
INSTEAD OF INSERT ON embeddings
FOR EACH ROW EXECUTE FUNCTION embeddings_insert_router();
