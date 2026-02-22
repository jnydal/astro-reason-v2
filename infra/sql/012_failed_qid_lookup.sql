-- failed_qid_lookup: persons whose first QID resolution attempt failed.
-- Resolver excludes these from retry; future LLM post-processing job will handle them.

CREATE TABLE IF NOT EXISTS failed_qid_lookup (
  person_id UUID PRIMARY KEY REFERENCES person_raw(id) ON DELETE CASCADE,
  failure_reason TEXT NOT NULL,   -- 'api_error' | 'no_candidates' | 'no_dob_match'
  details_json JSONB,             -- optional: candidates, error snippet for LLM
  attempted_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_failed_qid_lookup_reason ON failed_qid_lookup(failure_reason);

-- Recreate pipeline_counts view with pending_qid_failed (column order change requires DROP)
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
   )) AS pending_wiki_enrichment;
