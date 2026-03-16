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
# API health (host port 8003; container listens on 8000)
curl http://localhost:8003/healthz

# Fetch-Bio health
curl http://localhost:8002/healthz

# Ollama (if running)
curl http://localhost:8001/api/tags
```

## Data Pipeline Workflow

### Step 1: Ingest Data

```bash
# Upload XML file (API on host port 8003)
curl -X POST http://localhost:8003/ingest/astrodatabank \
  -F "xml=@your-file.xml"

# Get job ID from response
JOB_ID="..."

# Monitor job
watch -n 2 "curl -s http://localhost:8003/jobs/$JOB_ID | jq .status"
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
# fetch-bio default: 2.0s for Wikidata to reduce 429 risk
WIKIDATA_MIN_INTERVAL_SEC=2.0
WIKIDATA_JITTER_SEC=0.5
WIKIPEDIA_MIN_INTERVAL_SEC=1.0
WIKIPEDIA_JITTER_SEC=0.2
# Skip fetch-bio when embeddings-worker lag exceeds this (0 = disabled)
EMBEDDINGS_LAG_THRESHOLD=100
# Resolver HTTP timeout for fetch-bio API (ms). Default 900000 (15 min) for ~3s/person with rate limits.
# Increase if batches still time out (e.g. 429 retries); decrease if you use FETCH_BIO_LIMIT for smaller batches.
FETCH_BIO_TIMEOUT_MS=900000
# Optional: smaller batch size for fetch-bio than RESOLVE_LIMIT (e.g. 50) to keep each call under timeout
# FETCH_BIO_LIMIT=50
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
curl "http://localhost:8003/stats/correlation"
curl "http://localhost:8003/stats/correlation?mode=interpretations"

# Restrict to persons with QID only (entity_link)
curl "http://localhost:8003/stats/correlation?embeddingsScope=qid_only"
curl "http://localhost:8003/stats/correlation?mode=interpretations&embeddingsScope=qid_only"

# Optional: limit, minSamples
curl "http://localhost:8003/stats/correlation?limit=5000&minSamples=5"
```

**Check status / results**:
```bash
curl http://localhost:8003/stats/correlation/<jobId>
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

### Pipeline Observability Dashboard

The Grafana "Pipeline Observability" dashboard (see README) shows:
- **Embeddings (Wiki Bio)** — count of embeddings based on Wikipedia bio text (`source` contains `fetch_bio`). Requires `infra/sql/013_pipeline_counts_wiki_embeddings.sql` for existing DBs.
- **Resolver can try** — people the resolver will attempt (no QID, not in `failed_qid_lookup`). Requires `infra/sql/016_pipeline_counts_resolver_can_try.sql` for existing DBs.
- **Pending QID Resolution** — people with birth data but no Wikidata QID (resolver backlog)
- **Pending QID Failed** — people whose first QID resolution failed; awaiting LLM post-processing (see "Failed QID Lookup" below)
- **Pending Wiki Enrichment** — people with QIDs but not yet fetched from Wikipedia (fetch-bio backlog)
- **Job Status** — Jobs Queued, In Progress, Failed (7d); Recent Jobs table; Errors by Service (daily timeseries); Recent Errors table

**Core pipeline diagnostic** (QID resolution, wiki enrichment, re-embedded wiki-based progress):

```bash
./scripts/diagnose-core-pipeline.sh -UseDocker
```

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

### Failed QID Lookup

The resolver records persons whose first QID resolution attempt fails (API error, no candidates, or no DOB match) in `failed_qid_lookup`. These are excluded from resolver retries; a future LLM post-processing job will handle them.

**Migration**: For existing databases, run `infra/sql/012_failed_qid_lookup.sql` manually. Fresh installs apply it automatically via `docker-entrypoint-initdb.d`.

**Inspect failed lookups**:

```sql
-- Count by failure reason
SELECT failure_reason, COUNT(*) FROM failed_qid_lookup GROUP BY failure_reason;

-- Sample rows with person context
SELECT f.person_id, pr.name, b.date, f.failure_reason, f.details_json, f.attempted_at
FROM failed_qid_lookup f
JOIN person_raw pr ON pr.id = f.person_id
LEFT JOIN birth b ON b.person_id = f.person_id
ORDER BY f.attempted_at DESC
LIMIT 20;
```

For `no_dob_match`, `details_json` contains `candidates` (Wikidata search results) for LLM context. The future LLM job will query this table, join `person_raw` and `birth`, and use LLM to suggest QIDs or alternate search strategies.

### Failed Embeddings

The embeddings worker records persons whose embedding attempt failed (encode error, DB error, unsupported dimension, empty text) in `failed_embeddings`. These are optionally excluded from retries via `EMBEDDINGS_SKIP_FAILED=true` (default). The worker uses chunk-then-fallback: on batch failure, it processes each person individually and records failures without blocking the pipeline.

**Migration**: For existing databases, run `infra/sql/015_failed_embeddings.sql` manually. Fresh installs apply it automatically via `docker-entrypoint-initdb.d`.

**Inspect failed embeddings**:

```sql
-- Count by failure reason
SELECT failure_reason, COUNT(*) FROM failed_embeddings GROUP BY failure_reason;

-- Sample rows
SELECT fe.person_id, pr.name, fe.model_name, fe.failure_reason, fe.details, fe.attempted_at
FROM failed_embeddings fe
JOIN person_raw pr ON pr.id = fe.person_id
ORDER BY fe.attempted_at DESC
LIMIT 20;
```

**Retry after fix** (e.g. after fixing a bad bio text or DB constraint): delete the failed row so the next job will retry:
```sql
DELETE FROM failed_embeddings WHERE person_id = '<uuid>' AND model_name = 'BAAI/bge-large-en-v1.5';
-- Or clear all for a full retry:
-- DELETE FROM failed_embeddings;
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

### Stats-Worker: Consumer Poll Timeout / Rebalance

If you see:
```text
consumer poll timeout has expired ... max.poll.interval.ms
Kafka commit failed (consumer likely rebalanced)
```

**Cause**: Correlation jobs with large row counts (e.g. 80K+) can run longer than Kafka's `max.poll.interval.ms`. The worker blocks on processing and does not call `poll()`, so the broker kicks it out of the consumer group.

**Fix**: Increase `KAFKA_MAX_POLL_INTERVAL_MS` for stats-worker. Default is 4 hours (14_400_000 ms). For very large datasets, raise it further in `.env`:

```bash
# 4 hours (default in docker-compose); use 6–8h for 100K+ rows
KAFKA_MAX_POLL_INTERVAL_MS=21600000
```

Then restart: `docker compose up -d stats-worker`.

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

### Wikidata 429 Rate Limit (Too Many Requests)

If you see `fetch_bio API failed: 500 ... 429 Client Error: Your bot is making too many requests`:

**Cause**: Wikidata rate-limits unauthenticated clients. The resolver and fetch-bio both call Wikidata; combined traffic can exceed limits.

**Mitigations**:

1. **Automatic retries**: fetch-bio retries 429s up to 3 times with exponential backoff (60s, 120s, 240s) and honors `Retry-After` when present.

2. **Slower rate limits**: Increase intervals in `.env`:
   ```bash
   WIKIDATA_MIN_INTERVAL_SEC=3.0   # fetch-bio default is 2.0
   RESOLVE_LIMIT=25                # fewer people per batch
   ```

3. **For high-volume use**: Contact bot-traffic@wikimedia.org to request higher limits or run on Wikimedia Toolforge.

### Grafana / Dashboard Numbers Not Moving (Pipeline Stuck)

If **Pipeline Observability** counts (pending QID, pending wiki, embeddings wiki, etc.) stay the same for days, the resolver or fetch-bio is almost certainly blocked. Check in this order:

1. **Stuck ingest job (most common)**  
   Resolver skips fetch-bio whenever any ingest job is QUEUED or STARTED. If a job was enqueued but never processed (e.g. message lost, worker down), it blocks forever.

   ```sql
   SELECT id, function, status, enqueued_at FROM job_status
   WHERE function = 'worker.ingest.parse_adb_xml' AND status IN ('QUEUED','STARTED');
   ```

   If you see rows here, either wait for the ingest worker to process them or mark them failed so the gate opens:

   ```sql
   UPDATE job_status SET status = 'FAILED', result = 'Manually failed to unblock resolver', ended_at = NOW()
   WHERE function = 'worker.ingest.parse_adb_xml' AND status IN ('QUEUED','STARTED');
   ```

2. **Embeddings lag above threshold**  
   Resolver skips fetch-bio when `embeddings-worker` lag on topic `embeddings` exceeds `EMBEDDINGS_LAG_THRESHOLD` (default 100).

   ```bash
   docker compose exec kafka rpk group describe embeddings-worker
   ```

   If LAG is > 100, ensure the **embeddings** service is running and consuming. Once it drains, fetch-bio will run again. If the consumer group had no committed offsets (e.g. new deployment), the resolver used to treat that as infinite lag and block forever; that is now fixed so fetch-bio is allowed when the group has no offsets.

3. **Resolver not resolving (Resolved 0 QIDs)**  
   If logs show `Resolved 0 QIDs` every cycle but there are pending people, set `RESOLVER_DEBUG=1` and restart the resolver; check the diagnostic output for Wikidata search/DOB match issues. See "Pending QID / Wiki Enrichment Stagnation" in this runbook.

4. **Quick diagnostic**  
   Run the core pipeline diagnostic and (on Windows) stagnation script:

   ```bash
   ./scripts/diagnose-core-pipeline.sh -UseDocker
   ./scripts/diagnose-pipeline-stagnation.ps1 -UseDocker
   ```

### Fetch-bio Request Timeout (HttpRequestTimeoutException)

If you see `HttpRequestTimeoutException: Request timeout has expired [url=.../fetch-bio, request_timeout=300000 ms]`:

**Cause**: The fetch-bio service processes each person with rate limiting (~2s Wikidata + ~1s Wikipedia per person). For 200 people that's ~10+ minutes. The resolver's HTTP client default timeout is now 15 minutes (900000 ms); if you had an older config or hit 429 retries, it can still exceed the limit.

**Mitigations**:

1. **Increase timeout**: Set `FETCH_BIO_TIMEOUT_MS=1200000` (20 min) in `.env` and restart the resolver.
2. **Use smaller batches**: Set `FETCH_BIO_LIMIT=50` so each fetch-bio call processes fewer people and completes within the timeout.

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

# Upload XML (API on host port 8003)
with open("data.xml", "rb") as f:
    response = requests.post(
        "http://localhost:8003/ingest/astrodatabank",
        files={"xml": f}
    )
    job = response.json()
    print(f"Job ID: {job['jobId']}")

# Check status
job_id = job['jobId']
status = requests.get(f"http://localhost:8003/jobs/{job_id}").json()
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
val response = client.post("http://localhost:8003/ingest/astrodatabank") {
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
- API: `GET http://localhost:8003/healthz` (host port)
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

**Note**: Embeddings are computed for all people with bio_text (XML stubs or Wikipedia). Use the correlation endpoint's `embeddingsScope=qid_only` (proxy) or `embeddingsScope=wiki_only` (strict) to restrict correlation analysis to wiki-enriched people.

### Pending QID / Wiki Enrichment Stagnation

If **pending QID resolution** and **pending wiki enrichment** stay flat for hours:

1. **Check Resolver logs** for `Resolved X QIDs`:
   ```bash
   docker compose logs resolver --tail=50 | grep -E "Resolved|Skipping|Fetched"
   ```
   - If you see `Resolved 0 QIDs` every cycle, QID resolution is failing. A past bug: Wikidata entity JSON has claims with mixed datavalue types (e.g. P373 string, P214 string); full deserialization failed when it hit a non-object value. The fix parses raw JSON and extracts only P569 (birth date). Ensure latest Resolver: `docker compose build resolver && docker compose up -d resolver`.

2. **Wiki enrichment gating**: Resolver skips fetch-bio when (1) any ingest job is QUEUED/STARTED, or (2) embeddings lag > 100. Check ingest: `SELECT status FROM job_status WHERE function = 'worker.ingest.parse_adb_xml' AND status IN ('QUEUED','STARTED');` (should be empty). Check lag: `docker compose exec kafka rpk group describe embeddings-worker` (LAG should be 0).

3. **QID resolution debugging**: When `Resolved 0 QIDs` every cycle, set `RESOLVER_DEBUG=1` in resolver env and restart. The resolver will run a **diagnostic pass** after each failed cycle. Common causes:
   - **Wikidata search returns 0** — names with titles (Conte, Dr., etc.) often fail. The resolver now tries a "strip titles" variant (e.g. "Conte Luigi Cadorna" → "Luigi Cadorna"). If still 0, check network/User-Agent/Wikidata API access.
   - **dobMatches fails** — diagnostic shows step-by-step trace (P569 present, timeValue, extracted date, match result) for first 5 candidates. Run `./scripts/diagnose-pipeline-stagnation.ps1 -UseDocker` for full diagnostics.

**Embeddings worker idle despite "Fetched X Wikipedia bios"** (wrote > 0 but embeddings never invoked):

- Fetch-bio **only enqueues when bio text changed** (`text_hash != existing_hash`). Re-fetches of unchanged Wikipedia content write to `bio_text` but produce 0 embedding jobs. Check resolver/fetch-bio logs: the message now includes `enqueued N for embeddings` or `(no embedding jobs: text unchanged)`.
- **Query prioritization**: fetch-bio now processes people **without** `fetch_bio` in their bio source first (pending wiki enrichment), so new bios are more likely to enqueue. If you still see wrote > 0 and enqueued = 0, the pool may already be wiki-enriched — run `enqueue_embeddings_backfill` or `run_embeddings_backfill_sync` to catch any gaps.

**Lag 0 but embeddings count still low** (jobs consumed but total embeddings far below expected):

- **Ingest and fetch-bio now skip people who already have embeddings** — no duplicate jobs. One backfill run should process all remaining people.
- **Sync backfill** (bypasses Kafka, resumable): `docker compose run --rm embeddings python -m app.run_embeddings_backfill_sync` — use if Kafka backfill still fails.

**Embeddings worker reports "No new or changed bios" when bio_text has changed**:

- The embeddings worker picks the *most recently updated/retrieved* bio_text row per person (by `retrieved_at`, `updated_at`), not by `rev_id`. Fetch-bio updates stubs (rev_id=0) in place; a previous bug preferred stale wiki rows (rev_id>0) over updated stubs. If you see frequent noops, check `docker compose logs embeddings` — it now logs `skipped_unchanged` count and sample person_ids.

**Fix already-affected people (stale embeddings)** — if embeddings were produced from the wrong row before the fix, run the stale re-embed backfill to update them:

```bash
docker compose run --rm embeddings python -m app.reembed_stale_sync
```

### Why Resolution Can Be "Halted" (Resolved 0 QIDs)

The resolver only considers people who: have birth data, **no** `entity_link` row, and **are not** in `failed_qid_lookup`. Anyone whose **first** resolution attempt failed (no candidates, no DOB match, or API error) is recorded in `failed_qid_lookup` and **never retried** by the resolver. So if everyone left without a QID has already been tried once, "Resolver can try" is 0 and resolution appears halted — by design.

**Check:** Run `./scripts/diagnose-core-pipeline.sh -UseDocker` and look at "Resolver can try" vs "Failed once". If Resolver can try = 0 and Pending (no QID) > 0, that's why: everyone without a QID is in `failed_qid_lookup`. A future LLM post-processing job is intended to handle those; until then they stay unresolved.

### Why Resolved QIDs Don't Always Yield Bios

Fetch-bio only writes a bio when the QID has an **en.wikipedia** sitelink in Wikidata and the page returns usable wikitext. Many QIDs have no en.wikipedia page (non-English focus, minor figures, stubs, or redirects). For those, fetch-bio skips the person (no write, no embedding job). So "Pending wiki enrichment" can stay high even when fetch-bio is being called every cycle — the resolver is not stuck; many resolved QIDs simply don't have a usable en.wikipedia bio.

**Check:** Resolver logs show "Fetched N Wikipedia bios" each run. If N is 0 or small while "Pending wiki" is large, most of those QIDs have no en.wikipedia sitelink. The diagnostic script notes this when pending wiki > 0.

### No-Wiki-Sitelink Cache (skip QIDs with no en sitelink)

Fetch-bio records QIDs that have **no** en.wikipedia (or other lang) sitelink in the `no_wiki_sitelink` table and excludes them from future batches. That avoids re-checking the same QIDs every cycle and saves Wikidata rate limit.

**Migration**: For existing databases, run `infra/sql/017_no_wiki_sitelink.sql` manually. Fresh installs apply it via `docker-entrypoint-initdb.d`.

**Re-check later** (e.g. once a year, in case Wikipedia added pages):

```sql
DELETE FROM no_wiki_sitelink WHERE lang = 'en';
```

Then the next fetch-bio runs will try those QIDs again.

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
- **Place disambiguation**: When multiple candidates match DOB and we have `birth.place_name`, prefer the candidate whose P19 (place of birth) fuzzy-matches our place. Resolution continues regardless—we always pick a candidate when DOB matches; place only affects which one and what we record.
- **`place_match_confidence`** (in `entity_link`): 1 = place fuzzy match succeeded; 0 = we had `place_name` and checked, but no match; null = not applicable (no `place_name` or only one DOB match).

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
| Correlation | `embeddingsScope=qid_only` | Persons with entity_link (QID) — proxy for wiki-enriched |
| Correlation | `embeddingsScope=wiki_only` | Embeddings whose source contains `fetch_bio` — strictly wiki-enriched |
| Astro | Missing ephemeris | Swiss Ephemeris → Skyfield fallback can yield different results |
| Embeddings | Empty/blank bio_text | Skip embedding; no crash |
| Embeddings | Unsupported dimension | Skip with warning (only 384, 768, 1024, 1536) |
| Embeddings | Backfill gap | People with `bio_text` but no embeddings; use `enqueue_embeddings_backfill` |
| Resolver | Skip filters | Non-person entries skipped; only DOB-matched resolutions accepted. See RUNBOOK "XML / ADB Data: Domain Rules". |
| Resolver | Place disambiguation | When 2+ candidates match DOB and `place_name` is set, prefer place-matching candidate; record `place_match_confidence` (1/0/null). |

### Correlation Mode

- `mode=features`: astro features ↔ embeddings; includes birth-year detrending
- `mode=interpretations`: interpretation embeddings ↔ astro interpretations
- `embeddingsScope=all` (default): include all embeddings
- `embeddingsScope=qid_only`: include only persons with `entity_link` (resolved QID). Proxy for wiki-enriched; may include XML-only embeddings during pipeline lag or when fetch_bio fails.
- `embeddingsScope=wiki_only`: include only embeddings whose `source` contains `fetch_bio` — strictly wiki-enriched bios.
- Both analyses use same sample (people with known birth date)
