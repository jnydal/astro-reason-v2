# Deprecated: Python worker_ingest

**Status:** Deprecated. Do not use in production.

This Python implementation of the ingest worker is **deprecated** and **violates the current architecture invariants**.

## Why Deprecated

- **Replacement**: The Kotlin `worker-ingest` (`service/worker-ingest`) is the active implementation. It enqueues `astro.compute_features` to the `astro` topic and embedding jobs to the `embeddings` topic for persons with inline XML bio text.
- **Not in docker-compose**: The Python worker_ingest is not included in the default `docker-compose.yml`. The Kotlin worker-ingest is used instead.

## If You Must Use This

The Kotlin worker-ingest now enqueues embeddings for XML bio. This Python implementation is superseded; prefer the Kotlin worker.

## Removal

This code may be removed in a future release. Prefer the Kotlin worker-ingest for all ingest operations.
