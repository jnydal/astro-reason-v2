# Astro-Reason Runbook

This document contains operational procedures, troubleshooting, and common tasks.

## Quick Start

### Starting the System

```bash
# Start all services
docker compose up -d --build

# Check service status
docker compose ps

# View logs
docker compose logs -f api
docker compose logs -f worker-ingest
```

### Verify Services

```bash
# API health
curl http://localhost:8000/healthz

# Fetch-Bio health
curl http://localhost:8002/healthz

# Ollama (if running)
curl http://localhost:8001/api/tags
```

## Data Pipeline Workflow

### Step 1: Ingest Data

```bash
# Upload XML file
curl -X POST http://localhost:8000/ingest/astrodatabank \
  -F "xml=@your-file.xml"

# Get job ID from response
JOB_ID="..."

# Monitor job
watch -n 2 "curl -s http://localhost:8000/jobs/$JOB_ID | jq .status"
```

**What Happens**:
1. API stores XML in MinIO
2. Enqueues job in Kafka (`default` topic)
3. Worker-ingest processes XML
4. Inserts into `person_raw`, `birth`, `bio_text`
5. Enqueues `astro.compute_features` jobs to `astro` topic
6. Enqueues embedding jobs to `embeddings` topic for persons with inline XML bio text (fetch-bio also enqueues for wiki-enriched bios)

### Step 2: Resolve & Enrich

The Resolver service runs automatically:
- Finds people without QIDs
- Resolves to Wikidata
- Calls fetch-bio API (when no ingest job is active)
- Fetches Wikipedia biographies

**Fetch-bio gating**: Resolver skips the fetch-bio call when (1) any ingest job is QUEUED or STARTED, or (2) embeddings-worker lag on the embeddings topic exceeds `EMBEDDINGS_LAG_THRESHOLD` (default 100). This serializes embeddings production and waits for the embeddings worker to drain before adding more. QID resolution continues every cycle; only fetch-bio is deferred. Log messages: `Skipping fetch-bio: ingest job(s) active` or `Skipping fetch-bio: embeddings topic lag above threshold`.

**Monitor**:
```bash
docker compose logs -f resolver
```

**Rate limiting (recommended)**:
- Default throttles are 1 request/sec to Wikidata and 1 request/sec to Wikipedia
- Add jitter to avoid synchronized bursts
- Set a clear User-Agent with contact info

Example `.env`:
```bash
WIKI_USER_AGENT=astro-reason/0.1 (contact: you@example.com)
WIKIDATA_MIN_INTERVAL_SEC=1.0
WIKIDATA_JITTER_SEC=0.2
WIKIPEDIA_MIN_INTERVAL_SEC=1.0
WIKIPEDIA_JITTER_SEC=0.2
# Skip fetch-bio when embeddings-worker lag exceeds this (0 = disabled)
EMBEDDINGS_LAG_THRESHOLD=100
```

Safe schedule suggestion:
- Run `fetch-bio` in small batches (e.g., `limit=200`)
- Trigger every 10–15 minutes for sustained ingestion

### Step 3: Generate Embeddings

Embeddings worker processes jobs automatically:
- Reads from `embeddings` topic
- Generates semantic vectors
- Stores in `embeddings_*` tables

Embeddings are computed for all people with `bio_text` (XML stubs or Wikipedia). Use the correlation endpoint's `embeddingsScope` parameter to choose which embeddings to include in correlation (see Step 6).

**Monitor**:
```bash
docker compose logs -f embeddings
```

### Step 4: Compute Astro Features

Astro service runs automatically when ingest enqueues `astro.compute_features` jobs:
- Consumes from `astro` topic (group: astro-worker)
- Computes astrological features from birth data
- Stores in `astro_features` table
- Enqueues `astro.interpret` jobs on the same topic

**Monitor**:
```bash
docker compose logs -f astro
```

### Step 5: Astro Interpreter

Astro interpreter worker runs automatically when astro service produces `astro.interpret` jobs:
- Consumes from `astro` topic (group: astro-interpreter)
- Loads chart data from `astro_features`
- Calls Ollama to generate a short astrological reading
- Stores in `astro_interpretations` table

**Monitor**:
```bash
docker compose logs -f astro-interpreter
```

**Check interpretations**:
```sql
SELECT person_id, LEFT(interpretation_text, 200) AS reading_preview, model_name, created_at
FROM astro_interpretations
ORDER BY created_at DESC
LIMIT 10;
```

**Backfill readings (one-off)** — when you already have people with embeddings and astro features but no `astro.interpret` jobs were enqueued (e.g. DB populated without going through ingest), enqueue interpret jobs for all such people so the interpreter can fill `astro_interpretations` and you can run correlation:

```bash
# Rebuild so the image includes the script (if you added it recently)
docker compose build astro-interpreter

docker compose run --rm astro-interpreter python -m service.worker_astro_interpret.app.enqueue_interpret
```

This enqueues one `astro.interpret` job per person who has both embeddings and astro_features (same count as "Embeddings Computed" in pipeline observability). Ensure the astro-interpreter worker is running to process the queue.

### Step 6: Run Correlation Jobs (async)

The stats worker processes correlation jobs:
- Reads from `stats` topic
- Computes embeddings ↔ astro correlations (default mode: astro features)
- For **features** mode: also computes correlations on **birth-year–detrended** embeddings (linear regression residuals per dimension) so you can separate real astro signals from cohort effects (e.g. outer planets as generation proxies)
- Stores summary in `job_status` and full JSON in MinIO/S3

**Enqueue**:
```bash
# All embeddings (default); mode: features or interpretations
curl "http://localhost:8000/stats/correlation"
curl "http://localhost:8000/stats/correlation?mode=interpretations"

# Restrict to wiki-enriched people only (embeddings from persons with QID)
curl "http://localhost:8000/stats/correlation?embeddingsScope=qid_only"
curl "http://localhost:8000/stats/correlation?mode=interpretations&embeddingsScope=qid_only"

# Optional: limit, minSamples
curl "http://localhost:8000/stats/correlation?limit=5000&minSamples=5"
```

**Check status / results**:
```bash
curl http://localhost:8000/stats/correlation/<jobId>
```

The result includes:
- **`featureImportance`** — mean absolute Pearson correlation of each astro feature with **original** embedding dimensions
- **`featureImportanceDetrended`** — same, but using **detrended** embeddings (residuals after regressing each dimension on birth year). Only present when mode is `features` and all loaded rows have a birth date. Compare the two lists: features that drop sharply in the detrended list may be driven by birth-year/cohort rather than astrological structure.

**Monitor**:
```bash
docker compose logs -f stats-worker
```

**Service name**: The correlation worker is the Compose service **`stats-worker`** (not `stats`). The Kafka topic is `stats`. If you run `docker compose restart stats`, nothing happens because no service has that name — use `stats-worker` for logs, restart, and rebuild.

After a code change to the stats worker or the API (e.g. detrending logic, new result fields), rebuild and restart **both** so new jobs use the new code and the API returns the new result shape:
```bash
docker compose build --no-cache api stats-worker
docker compose up -d api stats-worker
```
(The API decodes and serves the job result; if only the worker is updated, responses will still lack new fields like `featureImportanceDetrended` and `correlationResultVersion`.)

## Database Queries

### Check Pipeline Progress

```sql
-- People with complete data
SELECT 
  COUNT(*) as total_people,
  COUNT(b.id) as with_birth,
  COUNT(bt.person_id) as with_bio,
  COUNT(e.person_id) as with_embeddings,
  COUNT(af.person_id) as with_astro,
  COUNT(ai.person_id) as with_interpretation
FROM person_raw pr
LEFT JOIN birth b ON b.person_id = pr.id
LEFT JOIN bio_text bt ON bt.person_id = pr.id AND bt.text IS NOT NULL
LEFT JOIN embeddings_768 e ON e.person_id = pr.id
LEFT JOIN astro_features af ON af.person_id = pr.id
LEFT JOIN astro_interpretations ai ON ai.person_id = pr.id;
```

### Find People Ready for Processing

```sql
-- People with astro features but no interpretation yet
SELECT pr.id, pr.name
FROM person_raw pr
JOIN astro_features af ON af.person_id = pr.id
LEFT JOIN astro_interpretations ai ON ai.person_id = pr.id
WHERE ai.person_id IS NULL
LIMIT 10;
```

### Check Job Queue Status

```bash
# Kafka topic list and describe (Redpanda uses rpk)
docker compose exec kafka rpk topic list
docker compose exec kafka rpk topic describe default
docker compose exec kafka rpk topic describe embeddings
docker compose exec kafka rpk topic describe astro
docker compose exec kafka rpk topic describe stats
```

## Troubleshooting

### Service Won't Start

```bash
# Check logs
docker compose logs service-name

# Check dependencies
docker compose ps

# Verify database is ready
docker compose exec db pg_isready -U postgres
```

### Jobs Not Processing

```bash
# Check Kafka connection
docker compose exec kafka rpk topic list

# Check worker logs
docker compose logs -f worker-ingest
```

### Database Connection Issues

```bash
# Test connection
docker compose exec api java -jar app.jar
# Or check environment variables
docker compose exec api env | grep PG_DSN
```

### LLM Not Responding

```bash
# Check Ollama
curl http://localhost:8001/api/tags

# Pull model if needed
docker compose exec local-llm ollama pull qwen2.5:7b-instruct-q4_K_M
```

## Development Workflow

### Building Services

```bash
# Build all Kotlin services
./gradlew build

# Build specific service
./gradlew :service:api:build

# Run tests
./gradlew test
```

### Local Development

```bash
# Run API locally (requires DB/Kafka running)
cd service/api
./gradlew run

# Run worker locally
cd service/worker-ingest
./gradlew run
```

### Debugging

```bash
# Attach debugger to Kotlin service
# Add to docker-compose.yml:
#   environment:
#     JAVA_OPTS: "-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005"

# View service logs in real-time
docker compose logs -f --tail=100 service-name

# Execute commands in container
docker compose exec service-name /bin/sh
```

## API Client Examples

### Python Client

```python
import requests

# Upload XML
with open("data.xml", "rb") as f:
    response = requests.post(
        "http://localhost:8000/ingest/astrodatabank",
        files={"xml": f}
    )
    job = response.json()
    print(f"Job ID: {job['jobId']}")

# Check status
job_id = job['jobId']
status = requests.get(f"http://localhost:8000/jobs/{job_id}").json()
print(f"Status: {status['status']}")
```

### Kotlin Client

```kotlin
import io.ktor.client.*
import io.ktor.client.engine.cio.*
import io.ktor.client.request.*
import io.ktor.http.*

val client = HttpClient(CIO)

// Upload XML
val response = client.post("http://localhost:8000/ingest/astrodatabank") {
    setBody(MultiPartFormDataContent(
        formData {
            append("xml", File("data.xml").readBytes(), Headers.build {
                append(HttpHeaders.ContentType, "application/xml")
            })
        }
    ))
}
```

## Monitoring & Health Checks

### Service Health

All services expose health endpoints:
- API: `GET http://localhost:8000/healthz`
- Fetch-Bio: `GET http://localhost:8002/healthz`

### Database Health

```sql
-- Check table sizes
SELECT 
  schemaname,
  tablename,
  pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
```

### Queue Monitoring

```bash
# Watch topic status (sample)
watch -n 5 'docker compose exec kafka rpk topic describe default'
```

## Common Tasks

### Reset Pipeline for New Dataset

```sql
-- Clear all data (CAUTION: destructive)
TRUNCATE person_raw CASCADE;
```

### Reprocess Failed Jobs

Jobs are stored in PostgreSQL (`job_status`). Check failed job IDs and re-enqueue to Kafka if needed.

### Restart Workers

```bash
# Astro and astro-interpreter run continuously (Python). Resolver is Kotlin.
docker compose restart astro astro-interpreter resolver
```

### Pipeline "Stopped" at N People (Embeddings Not Growing)

Embeddings: **ingest** enqueues for persons with inline XML bio; **Resolver** → **fetch_bio** enqueues for wiki-enriched bios; **embeddings worker** consumes both.

The Resolver processes **RESOLVE_LIMIT** people per minute (default **50** in docker-compose). For a full AstroDatabank dataset (~30k), that’s 10+ hours at default.

**Diagnose**:

```bash
./scripts/diagnose-pipeline.ps1 -UseDocker
```

Or run SQL:

```sql
SELECT (SELECT COUNT(*) FROM person_raw) AS total,
       (SELECT COUNT(*) FROM entity_link) AS with_qid,
       (SELECT COUNT(DISTINCT person_id) FROM embeddings_384) AS embeddings;
```

**Check for backfillable gap** (people with bio_text but no embeddings):

```sql
SELECT COUNT(DISTINCT bt.person_id) AS bio_without_embeddings
FROM bio_text bt
WHERE bt.text IS NOT NULL AND LENGTH(TRIM(bt.text)) > 0
  AND NOT EXISTS (
    SELECT 1 FROM embeddings_384 e WHERE e.person_id = bt.person_id
    UNION SELECT 1 FROM embeddings_768 e WHERE e.person_id = bt.person_id
    UNION SELECT 1 FROM embeddings_1024 e WHERE e.person_id = bt.person_id
    UNION SELECT 1 FROM embeddings_1536 e WHERE e.person_id = bt.person_id
  );
```

If this returns a positive number, run one of:

- **Sync backfill** (recommended when Kafka backfill has failed repeatedly): processes directly, no Kafka. **Resumable**: skips already-embedded people; safe to re-run after failures or Ctrl+C.
  ```bash
  docker compose run --rm embeddings python -m app.run_embeddings_backfill_sync
  ```
- **Kafka backfill**: enqueues jobs; worker must be running to process
  ```bash
  docker compose run --rm embeddings python -m app.enqueue_embeddings_backfill
  docker compose logs -f embeddings   # ensure worker is running
  ```

**Speed up Resolver** (when upstream QID resolution is the bottleneck):

1. Set `RESOLVE_LIMIT=200` (or higher) in `.env`
2. Restart resolver: `docker compose restart resolver`
3. Keep resolver running; it processes a batch every 60 seconds

**Note**: Embeddings are computed for all people with bio_text (XML stubs or Wikipedia). Use the correlation endpoint's `embeddingsScope=qid_only` to restrict correlation analysis to wiki-enriched people.

**Lag 0 but embeddings count still low** (jobs consumed but total embeddings far below expected):

- **Ingest and fetch-bio now skip people who already have embeddings** — no duplicate jobs. One backfill run should process all remaining people.
- **Sync backfill** (bypasses Kafka, resumable): `docker compose run --rm embeddings python -m app.run_embeddings_backfill_sync` — use if Kafka backfill still fails.

## XML / ADB Data: Domain Rules

Functional and domain rules for what data we ingest from the AstroDatabank XML and how we treat it. When changing ingest, resolver, or data handling logic, keep this section in sync.

### Ingest Rules

- **Required**: `adb_id` and `full_name` must be non-blank. Entries without both are not ingested.
- **Supported formats**: `<person>` and `<adb_entry>`. Parser extracts `public_data/name`, `public_data/bdata/sbdate`, `sbtime`, `place`, `text_data/shortbiography`, etc.
- **Birth data**: Date, time, place, lat/lon stored in `birth`. Entries with `date` are enqueued for astro feature computation.
- **Bio text**: From `shortbiography`; stored in `bio_text` with source from upload metadata. Optional at ingest.

### Entry Types in XML

The ADB export mixes **people** (e.g. "Cardano, Girolamo") and **non-person entries** (events, accidents, disasters, roles). We ingest all entries that have `adb_id` + `full_name`, but the **resolver** skips non-person entries when attempting QID resolution.

### Resolver Skip Logic (Non-Person Entries)

Entries whose names match these patterns are not attempted for Wikidata resolution—they won't yield a person with matching birth data and Wikipedia bio:

| Pattern | Examples |
|---------|----------|
| Names starting with a digit | "1943 Frankford Junction derailment", "2015 Paris attacks survivor" |
| `accident:` | "Accident: 1955 Le Mans disaster" |
| `derailment`, `earthquake`, `explosion`, `disaster` | Event and disaster entries |
| `academic:`, `vocation :`, `role :` | Role/category entries |
| `nature: ` | "Nature: Loma Prieta Earthquake:California 1989" |
| `helicopter crash`, `train derailment`, `bus crash`, `plane crash`, `gas explosion`, `shopping center strike` | Accident/event subtypes |
| `victim` | "Abuse victim 44646" |

**Full skip-pattern list** (case-insensitive substring match): `accident:`, `derailment`, `academic:`, `earthquake`, `attacks survivor`, `explosion`, `missile strike`, `disaster`, `victim`, `vocation :`, `role :`, `nature: `, `nature:`, `helicopter crash`, `train derailment`, `train crash`, `bus crash`, `plane crash`, `gas explosion`, `shopping center strike`.

### Resolution Acceptance

- Only accept a Wikidata match when the entity has **P569 (birth date)** matching our birth date. No fallback to "first candidate" without DOB match.

## QA Test Catalog

Project-specific test targets and edge cases. See `.cursor/rules/04-qa-checklist.mdc` for general QA process steps.

### Unit Test Targets

- `parsePgVector`: bracketed `[1,2,3]`, parenthesized `(1.25,-2.5,3)`, empty `[]`
- Astro feature computation: deterministic output for fixed birth record (use monkeypatch for backends)
- Feature vector shape: `lon_{planet}_sin`, `lon_{planet}_cos`, `elem_ratios`, `modality_ratios`

### Integration Test Targets

- End-to-end: upload XML → ingest → embeddings → astro → interpretation
- Job status transitions: QUEUED → STARTED → FINISHED / FAILED
- Correlation: `featureImportance` and `featureImportanceDetrended` when mode=features

### Edge Cases

| Area | Edge case | Expected behavior |
|------|-----------|-------------------|
| Correlation | `embeddingsScope=qid_only` | Only embeddings from persons with entity_link (QID) included |
| Astro | Missing ephemeris | Swiss Ephemeris → Skyfield fallback can yield different results |
| Embeddings | Empty/blank bio_text | Skip embedding; no crash |
| Embeddings | Unsupported dimension | Skip with warning (only 384, 768, 1024, 1536) |
| Embeddings | Backfill gap | People with `bio_text` but no embeddings; use `enqueue_embeddings_backfill` |
| Resolver | Skip filters | Non-person entries skipped; only DOB-matched resolutions accepted. See RUNBOOK "XML / ADB Data: Domain Rules". |

### Correlation Mode

- `mode=features`: astro features ↔ embeddings; includes birth-year detrending
- `mode=interpretations`: interpretation embeddings ↔ astro interpretations
- `embeddingsScope=all` (default): include all embeddings
- `embeddingsScope=qid_only`: include only embeddings from persons with entity_link (wiki-enriched)
- Both analyses use same sample (people with known birth date)
