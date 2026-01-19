# Python to Kotlin Migration

This document describes the migration of the Astro-Reason system from Python to Kotlin.

## Overview

All Python services have been migrated to Kotlin while maintaining the microservices architecture. The system now uses:

- **Ktor** for the API service
- **Exposed ORM** for database access
- **Kotlin Coroutines** for async operations
- **Kotlinx Serialization** for JSON handling
- **AWS SDK for Kotlin** for S3/MinIO operations
- **Custom Redis job queue** implementation

## Architecture

The system maintains 7 services:

1. **api** - Ktor REST API (port 8000)
2. **worker-ingest** - XML parsing and database insertion
3. **worker-embeddings** - Stays Python (called via HTTP API)
4. **astro** - Astrological feature computation
5. **traits** - LLM scoring worker
6. **resolver** - Wikidata QID resolution
7. **core** - Shared database and configuration utilities

## Project Structure

```
.
├── build.gradle.kts              # Root build configuration
├── settings.gradle.kts           # Multi-project settings
├── service/
│   ├── core/                     # Shared core library
│   │   ├── build.gradle.kts
│   │   └── src/main/kotlin/com/astroreason/core/
│   │       ├── Database.kt       # Database connection management
│   │       ├── Settings.kt       # Configuration
│   │       ├── Provenance.kt     # Provenance logging
│   │       ├── schema/Tables.kt  # Exposed table definitions
│   │       └── queue/JobQueue.kt # Redis job queue
│   ├── api/                      # Ktor API service
│   ├── worker-ingest/            # XML ingestion worker
│   ├── astro/                    # Astrological calculations
│   ├── traits/                   # LLM trait scoring
│   └── resolver/                # Wikidata resolver
└── docker-compose.yml            # Updated for Kotlin services
```

## Key Changes

### Database Access
- **Before**: `psycopg2` with raw SQL
- **After**: Exposed ORM with type-safe queries

### API Framework
- **Before**: FastAPI with Pydantic
- **After**: Ktor with Kotlinx Serialization

### Job Queue
- **Before**: Python RQ (Redis Queue)
- **After**: Custom Kotlin implementation using Redis and coroutines

### Storage
- **Before**: `boto3` (Python AWS SDK)
- **After**: AWS SDK for Kotlin

### Astrological Calculations
- **Before**: `pyswisseph` (Python Swiss Ephemeris)
- **After**: Fallback implementation (Swiss Ephemeris JNI integration pending)

## Building

### Prerequisites
- JDK 17 or higher
- Gradle 8.5+
- Docker and Docker Compose

### Build Commands

```bash
# Build all services
./gradlew build

# Build specific service
./gradlew :service:api:build

# Run tests
./gradlew test
```

## Running

### Development

```bash
# Start all services
docker compose up -d --build

# View logs
docker compose logs -f api
docker compose logs -f worker-ingest
```

### Environment Variables

Key environment variables (set in `.env` file):

```bash
# Database
DATABASE_URL=postgresql://postgres:postgres@db:5432/astro_reason
PG_DSN=postgresql://postgres:postgres@db:5432/astro_reason

# Redis
REDIS_URL=redis://redis:6379/0

# MinIO/S3
MINIO_ENDPOINT=http://minio:9000
MINIO_ACCESS_KEY=minio
MINIO_SECRET_KEY=minio123
MINIO_BUCKET_RAW=astro-raw

# LLM
OLLAMA_URL=http://local-llm:11434
LLM_MODEL=qwen2.5:7b-instruct-q4_K_M

# Astro
SWEPH_EPHE_PATH=/opt/ephe
```

## API Endpoints

- `GET /healthz` - Health check
- `GET /version` - Version info
- `POST /ingest/astrodatabank` - Upload XML file
- `GET /jobs/{jobId}` - Get job status

## Testing

```bash
# Run all tests
./gradlew test

# Run specific service tests
./gradlew :service:astro:test
```

## Known Limitations

1. **Swiss Ephemeris JNI**: The astro service currently uses a fallback implementation. Full Swiss Ephemeris support requires JNI bindings to be set up separately.

2. **Embeddings Service**: Remains in Python and is called via HTTP API. This is intentional as sentence-transformers is Python-specific.

3. **Job Queue**: Uses a simplified Redis-based implementation. For production, consider using a more robust queue system.

## Migration Notes

- All Python files in `service/` have been migrated except `worker_embeddings/`
- Database schema remains unchanged
- Docker Compose configuration updated for Kotlin services
- Environment variable names mostly unchanged for compatibility

## Next Steps

1. Set up Swiss Ephemeris JNI bindings for accurate astrological calculations
2. Add comprehensive integration tests
3. Set up CI/CD pipeline for Kotlin services
4. Performance testing and optimization
5. Add monitoring and observability (metrics, tracing)
