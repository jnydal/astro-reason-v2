# Astro-Reason Agent Guide

This document describes how to interact with the Astro-Reason system at a high level.
Operational commands, troubleshooting, and step-by-step procedures live in `RUNBOOK.md`.

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

Workers run continuously and process jobs from Kafka topics:

- **worker-ingest**: Processes `default` topic
- **embeddings**: Processes `embeddings` topic (Python)
- **traits**: Processes `traits` topic
- **resolver**: Polls database directly
- **astro**: Polls database directly

## Service Configuration

### Environment Variables

Create `.env` file:

```bash
# Database
DATABASE_URL=postgresql://postgres:postgres@db:5432/astro_reason
PG_DSN=postgresql://postgres:postgres@db:5432/astro_reason

# Kafka
KAFKA_BOOTSTRAP_SERVERS=kafka:9092

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
