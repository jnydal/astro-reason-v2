-- Add astrological readings count to pipeline_counts (after astro_interpretations exists)

CREATE OR REPLACE VIEW pipeline_counts AS
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
       AND status = 'FINISHED') AS correlations_computed;
