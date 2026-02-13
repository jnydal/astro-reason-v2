-- Remove legacy traits/Burlan tables (no longer used after traits worker removal).
-- Safe to run: IF EXISTS prevents errors if already dropped.

DROP TABLE IF EXISTS nlp_vectors;
DROP TABLE IF EXISTS nlp_traits;
