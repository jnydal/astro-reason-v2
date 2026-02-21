# Deprecated: Python worker_ingest

**Status:** Deprecated. Do not use in production.

This Python implementation of the ingest worker is **deprecated** and **violates the current architecture invariants**.

## Why Deprecated

- **Architecture violation**: This worker enqueues embedding jobs directly after parsing XML. The canonical pipeline requires embeddings to be enqueued **only from fetch-bio** (after Wikipedia enrichment), so that embeddings are computed from full biographies, not XML stubs.
- **Replacement**: The Kotlin `worker-ingest` (`service/worker-ingest`) is the active implementation. It enqueues `astro.compute_features` to the `astro` topic and does **not** enqueue embeddings.
- **Not in docker-compose**: The Python worker_ingest is not included in the default `docker-compose.yml`. The Kotlin worker-ingest is used instead.

## If You Must Use This

If you intentionally need ingest-to-embeddings behavior (e.g., for XML-only datasets without wiki enrichment), understand that:

1. You will bypass the Resolver → fetch-bio → embeddings flow.
2. Embeddings will be computed from XML bio stubs, which may be shorter or lower quality than Wikipedia bios.
3. This conflicts with the invariants in `.cursor/rules/03-architecture-invariants.mdc`.

## Removal

This code may be removed in a future release. Prefer the Kotlin worker-ingest for all ingest operations.
