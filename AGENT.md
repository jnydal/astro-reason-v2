# Astro-Reason Agent Guide

This document describes how to interact with the Astro-Reason system, including API usage, service interactions, and operational procedures.

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

## Service Interaction Patterns

### 1. API Service (Port 8000)

**Upload XML File**:
```bash
curl -X POST http://localhost:8000/ingest/astrodatabank \
  -F "xml=@testdata/c_sample.xml"
```

**Response**:
```json
{
  "jobId": "uuid-here",
  "objectUri": "s3://astro-raw/hash-timestamp.xml"
}
```

**Check Job Status**:
```bash
curl http://localhost:8000/jobs/{jobId}
```

**Response**:
```json
{
  "id": "uuid-here",
  "status": "finished",
  "enqueuedAt": "1234567890",
  "startedAt": "1234567891",
  "endedAt": "1234567892",
  "result": "Success",
  "excInfo": null
}
```

### 2. Fetch-Bio Service (Port 8002)

**Trigger Biography Fetching**:
```bash
curl -X POST http://localhost:8002/fetch-bio \
  -H "Content-Type: application/json" \
  -d '{"lang": "en", "limit": 500}'
```

**Response**:
```json
{
  "status": "ok",
  "written": 42,
  "message": "Fetched 42 biographies"
}
```

**Note**: This is typically called automatically by the Resolver service.

### 3. Worker Services

Workers run continuously and process jobs from Redis queues:

- **worker-ingest**: Processes `default` queue
- **embeddings**: Processes `embeddings` queue (Python)
- **traits**: Processes `traits` queue
- **resolver**: Polls database directly
- **astro**: Polls database directly

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
2. Enqueues job in Redis (`default` queue)
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

### Step 3: Generate Embeddings

Embeddings worker processes jobs automatically:
- Reads from `embeddings` queue
- Generates semantic vectors
- Stores in `embeddings_*` tables

**Monitor**:
```bash
docker compose logs -f embeddings
```

### Step 4: Score Traits

**Current State**: Traits jobs are enqueued by the fetch-bio service after biographies are written.

**Manual Trigger** (optional):
```bash
# Enqueue a traits job directly via Redis
redis-cli LPUSH "rq:queue:traits" '{"function":"traits.score_person","args":["person-uuid"]}'
```

### Step 5: Compute Astro Features

Astro service runs automatically:
- Finds people without `astro_features`
- Computes astrological features
- Stores in `astro_features` table

**Monitor**:
```bash
docker compose logs -f astro
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
  COUNT(nv.person_id) as with_traits,
  COUNT(af.person_id) as with_astro
FROM person_raw pr
LEFT JOIN birth b ON b.person_id = pr.id
LEFT JOIN bio_text bt ON bt.person_id = pr.id AND bt.text IS NOT NULL
LEFT JOIN embeddings_768 e ON e.person_id = pr.id
LEFT JOIN nlp_vectors nv ON nv.person_id = pr.id
LEFT JOIN astro_features af ON af.person_id = pr.id;
```

### Find People Ready for Processing

```sql
-- People with bios but no traits
SELECT pr.id, pr.name, bt.text
FROM person_raw pr
JOIN bio_text bt ON bt.person_id = pr.id
LEFT JOIN nlp_vectors nv ON nv.person_id = pr.id
WHERE bt.text IS NOT NULL
  AND nv.person_id IS NULL
LIMIT 10;
```

### Check Job Queue Status

```bash
# Redis queue lengths
redis-cli LLEN "rq:queue:default"
redis-cli LLEN "rq:queue:embeddings"
redis-cli LLEN "rq:queue:traits"
```

## Service Configuration

### Environment Variables

Create `.env` file:

```bash
# Database
DATABASE_URL=postgresql://postgres:postgres@db:5432/astro_reason
PG_DSN=postgresql://postgres:postgres@db:5432/astro_reason

# Redis
REDIS_URL=redis://redis:6379/0

# MinIO
MINIO_ENDPOINT=http://minio:9000
MINIO_ACCESS_KEY=minio
MINIO_SECRET_KEY=minio123
MINIO_BUCKET_RAW=astro-raw

# LLM
OLLAMA_URL=http://local-llm:11434
LLM_MODEL=qwen2.5:7b-instruct-q4_K_M

# Embeddings
EMBEDDINGS_MODEL=BAAI/bge-large-en-v1.5

# Astro
SWEPH_EPHE_PATH=/opt/ephe

# Fetch-Bio (for resolver)
FETCH_BIO_URL=http://fetch-bio:8002
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
# Check Redis connection
docker compose exec redis redis-cli ping

# Check queue status
docker compose exec redis redis-cli LLEN "rq:queue:default"

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
# Run API locally (requires DB/Redis running)
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
# Watch queue lengths
watch -n 1 'redis-cli LLEN "rq:queue:default"; redis-cli LLEN "rq:queue:embeddings"'
```

## Production Considerations

### Security
- Add authentication to API endpoints
- Use secrets management for credentials
- Enable TLS for external-facing services
- Restrict network access

### Performance
- Scale workers horizontally
- Use connection pooling (already configured)
- Monitor database query performance
- Cache frequently accessed data

### Reliability
- Implement retry logic for external APIs
- Add circuit breakers for service calls
- Set up monitoring and alerting
- Regular database backups

## Common Tasks

### Reset Pipeline for New Dataset

```sql
-- Clear all data (CAUTION: destructive)
TRUNCATE person_raw CASCADE;
```

### Reprocess Failed Jobs

Jobs are stored in Redis with TTL. Check Redis for failed job IDs and re-enqueue if needed.

### Manual Trigger Services

```bash
# Trigger astro computation
docker compose exec astro java -jar app.jar

# Trigger resolver
docker compose exec resolver java -jar app.jar
```

## Integration Points

### Adding New Services

1. Create service directory in `service/`
2. Add `build.gradle.kts` with dependencies
3. Create Dockerfile
4. Add to `docker-compose.yml`
5. Update `settings.gradle.kts`

### Extending API

Add new routes in `service/api/src/main/kotlin/com/astroreason/api/Application.kt`:

```kotlin
get("/new-endpoint") {
    call.respond(mapOf("status" to "ok"))
}
```

### Adding Database Tables

1. Define table in `service/core/src/main/kotlin/com/astroreason/core/schema/Tables.kt`
2. Create migration SQL in `infra/sql/`
3. Update Exposed table definitions
