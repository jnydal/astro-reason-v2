Recommended Next Steps (Priority Order)

1. Implement Statistical Analysis Service (CORE PURPOSE)
This is the final step. Create a new service or add endpoints to:

Features needed:
- Correlation analysis between `nlp_vectors` (8 Burlan vectors) and `astro_features`
- Statistical tests (Pearson, Spearman, p-values)
- Visualization data export (JSON/CSV)
- Feature importance analysis
- Clustering based on embeddings + astro features

Implementation approach:
- New Kotlin service `service/stats` or add endpoints to API
- Use Kotlin statistics libraries (e.g., `org.apache.commons:commons-math3`)
- Or call Python via HTTP (scipy, pandas, scikit-learn)

2. Embeddings DB coherence + migration handling (CORE PIPELINE) (improve)
- Ensure embeddings schema matches worker writes (text_hash, meta, source, updated_at)
- Confirm `pgvector` extension is available and migrations are actually applied
- Insert routing: worker now writes directly to `embeddings_384/768/1024/1536` tables
  - Avoids `ON CONFLICT` upserts against the `embeddings` view

3. Complete Python → Kotlin migration (PIPELINE CLEANUP)
- Migrate remaining Python pipeline scripts to Kotlin workers/services
- Keep the Wikidata/Wikipedia fetcher in Python (`service/ingest/app/fetch_bio.py`)
Details:
- Keep Python only for external HTTP + parsing against Wikidata/Wikipedia
- Migrate Python workers to Kotlin where possible:
  - Embeddings worker (`service/worker_embeddings/src/jobs.py`)
  - Ingest worker (`service/worker_ingest/src/jobs.py`)
- Migrate Python API jobs/utilities to Kotlin equivalents:
  - API job hooks (`service/api/src/jobs.py`)
  - Shared storage/helpers (`service/api/src/storage.py`, `service/api/src/schemas.py`)
- Update `docker-compose.yml` and service Dockerfiles to drop Python services after migration
- Ensure new Kotlin workers publish the same queue payloads + provenance events

  