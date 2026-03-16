-- Add pending_qid_resolver_can_try: count of people the resolver will actually try
-- (have birth, no QID, not in failed_qid_lookup). The rest of "pending QID" have failed once and are not retried.
-- For existing DBs: run manually, e.g. docker compose exec -T db psql -U postgres -d astro_reason -f - < infra/sql/016_pipeline_counts_resolver_can_try.sql

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
  (SELECT COUNT(*) FROM person_raw pr
   INNER JOIN birth b ON b.person_id = pr.id
   LEFT JOIN entity_link el ON el.person_id = pr.id
   LEFT JOIN failed_qid_lookup f ON f.person_id = pr.id
   WHERE el.person_id IS NULL AND f.person_id IS NULL) AS pending_qid_resolver_can_try,
  (SELECT COUNT(*) FROM failed_qid_lookup) AS pending_qid_failed,
  (SELECT COUNT(*) FROM entity_link el
   WHERE NOT EXISTS (
     SELECT 1 FROM bio_text bt
     WHERE bt.person_id = el.person_id
       AND bt.source LIKE '%fetch_bio%'
   )) AS pending_wiki_enrichment,
  (SELECT COUNT(*) FROM failed_embeddings) AS pending_embeddings_failed;
