Missing Implementation (Priority Order)

1. Embeddings DB coherence + migration handling (CORE PIPELINE) (improve)
- Ensure embeddings schema matches worker writes (text_hash, meta, source, updated_at)
- Confirm `pgvector` extension is available and migrations are actually applied
- Insert routing: worker now writes directly to `embeddings_384/768/1024/1536` tables
  - Avoids `ON CONFLICT` upserts against the `embeddings` view

2. Complete Python → Kotlin migration (PIPELINE CLEANUP)
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

  