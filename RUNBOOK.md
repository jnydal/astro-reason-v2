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
5. Enqueues embedding jobs

### Step 2: Resolve & Enrich

The Resolver service runs automatically:
- Finds people without QIDs
- Resolves to Wikidata
- Calls fetch-bio API
- Fetches Wikipedia biographies

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
```

Safe schedule suggestion:
- Run `fetch-bio` in small batches (e.g., `limit=200`)
- Trigger every 10–15 minutes for sustained ingestion

### Step 3: Generate Embeddings

Embeddings worker processes jobs automatically:
- Reads from `embeddings` topic
- Generates semantic vectors
- Stores in `embeddings_*` tables

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

### Step 6: Run Correlation Jobs (async)

The stats worker processes correlation jobs:
- Reads from `stats` topic
- Computes embeddings ↔ astro correlations
- Stores summary in `job_status` and full JSON in MinIO/S3

**Enqueue**:
```bash
curl http://localhost:8000/stats/correlation
```

**Check status / results**:
```bash
curl http://localhost:8000/stats/correlation/<jobId>
```

**Monitor**:
```bash
docker compose logs -f stats-worker
```

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
