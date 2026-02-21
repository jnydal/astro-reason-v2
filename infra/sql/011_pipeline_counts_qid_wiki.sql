-- Add pending QID resolution and pending wiki enrichment counts to pipeline_counts

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
       AND status = 'FINISHED') AS correlations_computed,
  (SELECT COUNT(*) FROM person_raw pr
   INNER JOIN birth b ON b.person_id = pr.id
   LEFT JOIN entity_link el ON el.person_id = pr.id
   WHERE el.person_id IS NULL) AS pending_qid_resolution,
  (SELECT COUNT(*) FROM entity_link el
   WHERE NOT EXISTS (
     SELECT 1 FROM bio_text bt
     WHERE bt.person_id = el.person_id
       AND bt.source LIKE '%fetch_bio%'
   )) AS pending_wiki_enrichment;
