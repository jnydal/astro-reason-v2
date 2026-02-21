-- One-off: remove embeddings for people who have no entity_link (QID).
-- Use when you want to purge embeddings from XML-only bios and keep only
-- wiki-enriched people (e.g. before running correlation with embeddingsScope=qid_only
-- on a smaller, curated set).
--
-- Run manually: docker compose exec db psql -U postgres -d astro_reason -f - < infra/sql/010_cleanup_embeddings_without_qid.sql

DO $$
DECLARE
  r RECORD;
  deleted INT;
  total_deleted INT := 0;
BEGIN
  FOR r IN (SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name LIKE 'embeddings_%'
            AND table_name ~ '^embeddings_\d+$')
  LOOP
    EXECUTE format(
      'WITH deleted AS (
         DELETE FROM %I
         WHERE person_id NOT IN (SELECT person_id FROM entity_link)
         RETURNING person_id
       )
       SELECT COUNT(*)::int FROM deleted',
      r.table_name
    ) INTO deleted;
    total_deleted := total_deleted + deleted;
    RAISE NOTICE '%: deleted % rows', r.table_name, deleted;
  END LOOP;
  RAISE NOTICE 'Total rows deleted: %', total_deleted;
END;
$$;
