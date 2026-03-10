-- failed_embeddings: persons whose embedding attempt failed (encode error, DB error, etc.).
-- Embeddings worker records these and optionally excludes them from retries.
-- Mirrors failed_qid_lookup pattern.

CREATE TABLE IF NOT EXISTS failed_embeddings (
  person_id UUID NOT NULL REFERENCES person_raw(id) ON DELETE CASCADE,
  model_name TEXT NOT NULL,
  failure_reason TEXT NOT NULL,   -- 'encode_error', 'db_error', 'unsupported_dim', 'empty_text'
  details TEXT,                   -- short error message / truncation
  attempted_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (person_id, model_name)
);

CREATE INDEX IF NOT EXISTS idx_failed_embeddings_reason ON failed_embeddings(failure_reason);

-- Extend pipeline_counts view with pending_embeddings_failed
DROP VIEW IF EXISTS pipeline_counts;
CREATE VIEW pipeline_counts AS
SELECT
  (SELECT COUNT(*) FROM person_raw) AS people_total,
  (SELECT COUNT(DISTINCT person_id) FROM (
      SELECT person_id FROM embeddings_384
      UNION
      SELECT person_id FROM embeddings_768
      UNION
      SELECT person_id FROM embeddings_1024
      UNION
      SELECT person_id FROM embeddings_1536
  ) emb_all) AS embeddings_computed,
  (SELECT COUNT(DISTINCT person_id) FROM (
      SELECT person_id FROM embeddings_384 WHERE source LIKE '%fetch_bio%'
      UNION
      SELECT person_id FROM embeddings_768 WHERE source LIKE '%fetch_bio%'
      UNION
      SELECT person_id FROM embeddings_1024 WHERE source LIKE '%fetch_bio%'
      UNION
      SELECT person_id FROM embeddings_1536 WHERE source LIKE '%fetch_bio%'
  ) emb_wiki) AS embeddings_wiki_computed,
  (SELECT COUNT(*) FROM astro_features) AS astro_features_computed,
  (SELECT COUNT(*) FROM astro_interpretations) AS astro_readings_created,
  (SELECT COUNT(*) FROM job_status
     WHERE function = 'stats.correlation'
       AND status = 'FINISHED') AS correlations_computed,
  (SELECT COUNT(*) FROM person_raw pr
   INNER JOIN birth b ON b.person_id = pr.id
   LEFT JOIN entity_link el ON el.person_id = pr.id
   WHERE el.person_id IS NULL) AS pending_qid_resolution,
  (SELECT COUNT(*) FROM failed_qid_lookup) AS pending_qid_failed,
  (SELECT COUNT(*) FROM entity_link el
   WHERE NOT EXISTS (
     SELECT 1 FROM bio_text bt
     WHERE bt.person_id = el.person_id
       AND bt.source LIKE '%fetch_bio%'
   )) AS pending_wiki_enrichment,
  (SELECT COUNT(*) FROM failed_embeddings) AS pending_embeddings_failed;
