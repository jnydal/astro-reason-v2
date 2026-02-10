-- Observability views for Grafana dashboards

CREATE OR REPLACE VIEW pipeline_counts AS
SELECT
  (SELECT COUNT(*) FROM person_raw) AS people_total,
  (SELECT COUNT(*) FROM nlp_vectors) AS traits_scored,
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
  (SELECT COUNT(*) FROM job_status
     WHERE function = 'stats.correlation'
       AND status = 'FINISHED') AS correlations_computed;

CREATE OR REPLACE VIEW pipeline_stage_daily AS
SELECT
  date_trunc('day', created_at) AS day,
  stage,
  COALESCE(detail->>'status', 'unknown') AS status,
  COUNT(*) AS events
FROM provenance_event
GROUP BY 1, 2, 3;

CREATE OR REPLACE VIEW pipeline_latest_event AS
SELECT
  person_id,
  MAX(created_at) AS last_event_at
FROM provenance_event
WHERE person_id IS NOT NULL
GROUP BY person_id;

CREATE OR REPLACE VIEW pipeline_stuck AS
SELECT
  pr.id AS person_id,
  ple.last_event_at,
  ROUND(EXTRACT(EPOCH FROM (NOW() - ple.last_event_at)) / 3600.0, 2) AS hours_since_last_event,
  (nv.person_id IS NULL) AS missing_traits,
  (emb.person_id IS NULL) AS missing_embeddings,
  (af.person_id IS NULL) AS missing_astro
FROM person_raw pr
LEFT JOIN pipeline_latest_event ple ON ple.person_id = pr.id
LEFT JOIN (SELECT DISTINCT person_id FROM nlp_vectors) nv ON nv.person_id = pr.id
LEFT JOIN (
    SELECT person_id FROM embeddings_384
    UNION
    SELECT person_id FROM embeddings_768
    UNION
    SELECT person_id FROM embeddings_1024
    UNION
    SELECT person_id FROM embeddings_1536
) emb ON emb.person_id = pr.id
LEFT JOIN astro_features af ON af.person_id = pr.id
WHERE ple.last_event_at IS NOT NULL
  AND ple.last_event_at < NOW() - INTERVAL '24 hours'
  AND (nv.person_id IS NULL OR emb.person_id IS NULL OR af.person_id IS NULL);

CREATE OR REPLACE VIEW service_error_events AS
SELECT
  created_at,
  stage,
  CASE
    WHEN stage IN ('ingest', 'ingest_job') THEN 'ingest'
    WHEN stage IN ('fetch_bio') THEN 'fetch_bio'
    WHEN stage IN ('resolve_qid') THEN 'resolver'
    WHEN stage IN ('traits') THEN 'traits'
    WHEN stage IN ('embeddings') THEN 'embeddings'
    WHEN stage IN ('astro') THEN 'astro'
    ELSE stage
  END AS service,
  COALESCE(detail->>'status', 'unknown') AS status,
  detail->>'error' AS error,
  detail->'meta' AS meta,
  detail AS detail
FROM provenance_event
WHERE COALESCE(detail->>'status', 'unknown') IN ('error', 'failed', 'fail');

CREATE OR REPLACE VIEW service_error_daily AS
SELECT
  date_trunc('day', created_at) AS day,
  service,
  COUNT(*) AS errors
FROM service_error_events
GROUP BY 1, 2;
