-- 384
CREATE TABLE IF NOT EXISTS embeddings_384 (
  person_id UUID NOT NULL REFERENCES person_raw(id) ON DELETE CASCADE,
  model_name TEXT NOT NULL,
  dim INT NOT NULL CHECK (dim = 384),
  vector vector(384) NOT NULL,
  created_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (person_id, model_name)
);

-- 768
CREATE TABLE IF NOT EXISTS embeddings_768 (
  person_id UUID NOT NULL REFERENCES person_raw(id) ON DELETE CASCADE,
  model_name TEXT NOT NULL,
  dim INT NOT NULL CHECK (dim = 768),
  vector vector(768) NOT NULL,
  created_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (person_id, model_name)
);

-- 1024
CREATE TABLE IF NOT EXISTS embeddings_1024 (
  person_id UUID NOT NULL REFERENCES person_raw(id) ON DELETE CASCADE,
  model_name TEXT NOT NULL,
  dim INT NOT NULL CHECK (dim = 1024),
  vector vector(1024) NOT NULL,
  created_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (person_id, model_name)
);

-- optional: 1536 if you need it
CREATE TABLE IF NOT EXISTS embeddings_1536 ( ... vector(1536) ... );

-- Convenience view
CREATE OR REPLACE VIEW embeddings AS
SELECT * FROM embeddings_384
UNION ALL
SELECT * FROM embeddings_768
UNION ALL
SELECT * FROM embeddings_1024
UNION ALL
SELECT * FROM embeddings_1536;
