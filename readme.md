# Astro-Reason

Astro-Reason is a research-oriented pipeline for evaluating whether statistically meaningful correlations exist between birth chart configurations and semantic embeddings derived from biographical text.

The system computes semantic embeddings from biographies and compares those vectors against encoded astrological features. An LLM interprets birth charts into short astrological readings stored for semantic comparison with biographies.

---

## Abstract

Astrology claims that birth circumstances influence personality. Modern psychology typically claims personality is shaped entirely by environment and development. Astro-Reason provides a data-driven bridge between these perspectives.

The system:

1. Ingests and enriches biographical datasets (AstroDatabank C-sample + Wikipedia).
2. Performs NLP analysis to derive **semantic embeddings** from biography text.
3. Encodes birth charts into **numeric astro features** and **LLM-generated astrological readings**.
4. Applies statistical correlation and modeling to evaluate alignment (embeddings ↔ astro features); readings are stored for future semantic comparison.

The purpose is not advocacy for astrology, but scientific measurement of potential structure that may link symbolic birth data to observable psychological traits.

---

## Technical Overview

### Data Flow

```
AstroDatabank XML
        ↓ Parsing
 person_raw + birth
        ↓ Wikipedia enrichment
      bio_text
        ↓ NLP: embeddings              Astrological encoding (ephemeris)
  embeddings (semantic)                astro_features (numeric)
        ↓                                      ↓
        │                              Astro interpreter (LLM)
        │                              astro_interpretations (readings)
        ↓                                      ↓
  stats correlation (async) ←──────────────────┘
Stats + correlation visualization
```

### Detailed flow

Input & Ingest:
AstroDatabank XML is uploaded → API enqueues a job on the `default` Kafka topic → Kotlin ingest worker (`worker-ingest`) reads from `default`, parses XML, and writes to PostgreSQL (`person_raw`, `birth`, initial `bio_text` stubs).

Wikidata / Wikipedia enrichment:
Resolver service (no queue; DB polling) takes name + date of birth → resolves a Wikidata QID → stores it → calls the `fetch-bio` Python service → Wikipedia biography text is written into `bio_text`. After each successful update, `fetch-bio` enqueues downstream jobs.

Embeddings (semantic) – topic: `embeddings`:
The ingest worker enqueues embedding jobs for persons with inline XML bio text. The `fetch-bio` service also enqueues for wiki-enriched bios. The Python embeddings worker pulls from `embeddings` → computes sentence-transformer embeddings → stores vectors in `embeddings_*` tables. Use `enqueue_embeddings_backfill` to catch any gaps (e.g. jobs lost). Use `embeddingsScope=qid_only` on the correlation endpoint to restrict analysis to wiki-enriched people.

Astrological encoding – topic: `astro`:
The astro worker consumes `"astro.compute_features"` jobs from Kafka, computes ephemeris‑based features (Swiss Ephemeris with Skyfield fallback) and stores structured astro features + a flat numeric feature vector in `astro_features`. After each successful write, it enqueues `"astro.interpret"` jobs on the same topic. The astro interpreter worker (Python, consumer group `astro-interpreter`) consumes those, calls the LLM to produce a short astrological reading from the chart data, and stores the result in `astro_interpretations`.

Storage & analysis:
PostgreSQL is the source of truth for people, births, bios, embeddings, astro features, and astro interpretations. Kafka is used as the job queue (`default` → ingest, `embeddings` from ingest and fetch-bio, `astro` for features and interpretations, `stats` for correlation jobs). Embeddings are computed for all people with bio_text (XML or Wikipedia). Correlation uses embeddings ↔ astro feature_vec; use `embeddingsScope=qid_only` to restrict to wiki-enriched people. Interpretations are stored for future semantic comparison with biography embeddings. For astro-features mode, the stats worker also computes correlations on **birth-year–detrended** embeddings (linear regression residuals per dimension) so you can compare `featureImportance` (original) with `featureImportanceDetrended` and distinguish real astrological signals from spurious cohort effects (e.g. slow-moving outer planets acting as generation proxies).


### System Components

| Component | Description |
|----------|-------------|
| API (Kotlin/Ktor) | Uploads, triggers jobs, serves results |
| Worker | Ingest, astro computation, astro interpreter |
| **Ollama** (local LLM) | Astrological reading generation from chart data |
| Embeddings service | Semantic vector creation (BGE models) |
| Stats worker (`stats-worker`) | Async embeddings ↔ astro correlation jobs (original + birth-year detrended) |
| PostgreSQL + pgvector | Data and vector storage |
| Kafka | Job queue |
| MinIO | Raw biography text object storage |
| Grafana (optional) | Metrics |

---

## Database Schema (Core Tables)

- `person_raw` — identity & XML reference
- `birth` — date/time/location data
- `entity_link` — Wikidata QID per person (Resolver)
- `bio_text` — enriched biography metadata
- `embeddings_*` — semantic text embeddings (pgvector)
- `astro_features` — numeric planetary/house/aspect features
- `astro_interpretations` — LLM-generated astrological readings per person
- `provenance_event` — process audit tracking

---

## Installation & Local Setup

### Requirements
- Docker + Docker Compose
- ~12 GB disk space
- CPU-only support (GPU optional for faster inference)

### Start services

```bash
docker compose up -d --build
```

### Kafka topics (required)

The pipeline expects the `default`, `embeddings`, `astro`, and `stats` topics to exist. Create them once:

```bash
docker compose exec -T kafka rpk topic create default
docker compose exec -T kafka rpk topic create embeddings
docker compose exec -T kafka rpk topic create astro
docker compose exec -T kafka rpk topic create stats
```

Verify:

```bash
docker compose ps
```

### Load the Ollama LLM

```bash
docker exec -it <local-llm-container-name> ollama pull qwen2.5:7b-instruct-q4_K_M
```

Note: Ollama models are stored in the `ollama` Docker volume. You only need to
pull the model once unless you remove volumes (for example, `docker compose down -v`
or deleting the `ollama` volume). Ollama is used by the astro interpreter worker to generate astrological readings from chart data.

Test:

```bash
curl http://localhost:8001/api/tags
```

---

## Typical Usage Flow

1. Upload AstroDatabank dataset via frontend or API
2. Trigger enrichment (Wikipedia fetch)
3. Run batch processing:
   - Embeddings (ingest enqueues for XML bio; fetch-bio enqueues for wiki-enriched)
   - Astro encoding (ingest enqueues); astro interpreter runs after features are written

Each processing step logs a provenance record for reproducibility.

---

## Observability

Grafana dashboards are provisioned automatically from `grafana/` when you run
`docker compose up`. The default Grafana login is `admin` / `admin` and the
PostgreSQL datasource points at the local `db` container.

Dashboards:
- `Pipeline Observability` provides counts for people, embeddings, astro
  features, astrological readings created, and correlations computed, plus
  throughput and stuck-job views.

Alerts:
- `Pipeline stuck` triggers when any record has not progressed within the 24h SLA.
- `Pipeline errors` triggers when error events appear in the last 15 minutes.

Runbook:
- If `Pipeline stuck` fires, inspect `pipeline_stuck` to see which stages are missing.
- If `Pipeline errors` fires, review recent `provenance_event` rows with
  `detail->>'status' = 'error'` and check worker logs.
- Adjust SLA/thresholds by editing `infra/sql/003_observability.sql` and
  `grafana/provisioning/alerting/alerts.yml`. The `pipeline_counts` view is
  extended in `infra/sql/009_pipeline_counts_readings.sql` (astro readings count).

---

## License

For research use only. External datasets must follow their respective licenses.

---

## Citation

If using Astro-Reason for academic research, cite:
- Model name and version
- Prompt hash
- Processing date
- Dataset source attribution
