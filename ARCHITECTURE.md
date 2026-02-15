# Astro-Reason Architecture

## Overview

Astro-Reason is a microservices-based research pipeline that evaluates correlations between astrological birth chart configurations and semantic embeddings derived from biographical text. Correlation is embeddings ↔ astro features; LLM-generated astrological readings are stored for future semantic comparison with biographies.

## System Architecture

```
┌─────────────┐
│   Client    │
└──────┬──────┘
       │ HTTP
       ↓
┌─────────────────────────────────────────────────────────────┐
│                    API Service (Kotlin)                      │
│  - Upload XML files                                          │
│  - Job status polling                                        │
│  - Health checks                                             │
└──────┬──────────────────────────────────────────────────────┘
       │
       ├─→ Kafka Topic (default)
       │         ↓
       │   ┌─────────────────────┐
       │   │ Worker-Ingest      │
       │   │ (Kotlin)           │
       │   └─────────────────────┘
       │
       └─→ MinIO/S3 (Object Storage)
                 │
                 └─→ Stores uploaded XML files

┌─────────────────────────────────────────────────────────────┐
│              Data Processing Pipeline                        │
└─────────────────────────────────────────────────────────────┘

1. Ingest Worker
   ├─→ Parses XML → PostgreSQL (person_raw, birth, bio_text)
   └─→ No embeddings enqueued here (waits for wiki enrichment)

2. Embeddings Worker (Python)
   ├─→ Reads from Kafka (embeddings topic)
   ├─→ Generates semantic vectors (sentence-transformers)
   └─→ Writes → PostgreSQL (embeddings_* tables)

3. Resolver Service (Kotlin)
   ├─→ Resolves Wikidata QIDs
   └─→ Calls → Fetch-Bio API (HTTP)

4. Fetch-Bio Service (Python - Containerized)
   ├─→ Fetches Wikipedia biographies
   └─→ Updates → PostgreSQL (bio_text.text)
       └─→ Enqueues → Kafka (embeddings topic)

5. Astro Service (Python)
   ├─→ Reads from Kafka (astro topic, group: astro-worker)
   ├─→ Computes astrological features from birth data
   ├─→ Writes → PostgreSQL (astro_features)
   └─→ Produces → Kafka (astro topic, astro.interpret jobs)

6. Astro Interpreter Worker (Python)
   ├─→ Reads from Kafka (astro topic, group: astro-interpreter)
   ├─→ Loads astro_features from PostgreSQL
   ├─→ Calls → Ollama LLM (astrological reading)
   └─→ Writes → PostgreSQL (astro_interpretations)

7. Stats Worker (Kotlin)
   ├─→ Reads from Kafka (stats topic)
   ├─→ Computes embeddings ↔ astro correlations (original + birth-year detrended)
   ├─→ Stores summary in job_status (featureImportance + featureImportanceDetrended)
   └─→ Writes full results to MinIO/S3
```

## Technology Stack

### Kotlin Services
- **Framework**: Ktor 2.3.7
- **Database**: Exposed ORM 0.49.0
- **Connection Pooling**: HikariCP 5.1.0
- **Serialization**: Kotlinx Serialization 1.6.2
- **HTTP Client**: Ktor Client
- **AWS SDK**: AWS SDK for Kotlin (S3)
- **Kafka**: Spring Kafka

### Python Services
- **Framework**: FastAPI (fetch-bio), batch worker (astro), Kafka consumer (embeddings)
- **ML**: sentence-transformers (embeddings)
- **Database**: psycopg2

### Infrastructure
- **Database**: PostgreSQL 16 with pgvector extension
- **Queue**: Kafka (KRaft, single-node)
- **Object Storage**: MinIO
- **LLM**: Ollama (local LLM runtime)
- **Monitoring**: Grafana (optional)

## Service Details

### 1. API Service (`service/api`)

**Technology**: Kotlin + Ktor  
**Port**: 8000  
**Responsibilities**:
- REST API endpoints
- File upload handling
- Job queue management
- S3/MinIO integration

**Endpoints**:
- `GET /healthz` - Health check
- `GET /version` - Version information
- `POST /ingest/astrodatabank` - Upload XML file
- `GET /jobs/{jobId}` - Get job status
- `GET /stats/correlation` - Enqueue correlation job (default mode: astro features; optional `?mode=interpretations`)
- `GET /stats/correlation/{jobId}` - Fetch correlation result/status. Result includes `featureImportance` (original) and, for features mode, `featureImportanceDetrended` (after birth-year detrending)

**Dependencies**: Database, Kafka, MinIO

### 2. Worker-Ingest (`service/worker-ingest`)

**Technology**: Kotlin  
**Queue**: `default`  
**Responsibilities**:
- Parse AstroDatabank XML files
- Extract person, birth, and biography data
- Batch insert into PostgreSQL
- Enqueue embedding jobs (after wiki enrichment, via fetch-bio)

**Key Components**:
- `XmlParser.kt` - Streaming XML parser (StAX)
- `IngestJob.kt` - Main processing logic
- `IngestWorker.kt` - Queue worker loop

**Dependencies**: Database, Kafka, MinIO

### 3. Embeddings Worker (`service/worker_embeddings`)

**Technology**: Python + Kafka  
**Queue**: `embeddings`  
**Responsibilities**:
- Generate semantic embeddings for biographies
- Store vectors in pgvector-enabled PostgreSQL
- Batch processing for efficiency

**Model**: BAAI/bge-large-en-v1.5 (configurable)

**Dependencies**: Database, Kafka

### 4. Resolver Service (`service/resolver`)

**Technology**: Kotlin  
**Mode**: Continuous polling  
**Responsibilities**:
- Resolve people to Wikidata QIDs
- Match by name and date of birth
- Trigger fetch-bio service via HTTP

**Workflow**:
1. Query people without QIDs
2. Search Wikidata API
3. Match by date of birth
4. Store QID in `bio_text`
5. Call fetch-bio API

**Dependencies**: Database, Fetch-Bio Service

### 5. Fetch-Bio Service (`service/ingest`)

**Technology**: Python + FastAPI  
**Port**: 8002  
**Responsibilities**:
- Fetch Wikipedia biographies for people with QIDs
- Clean and process wikitext
- Store biography text in database

**Endpoints**:
- `GET /healthz` - Health check
- `POST /fetch-bio` - Trigger biography fetching

**Dependencies**: Database

### 6. Astro Service (`service/astro`)

**Technology**: Python  
**Mode**: Kafka worker (topic: `astro`, group: `astro-worker`)  
**Responsibilities**:
- Compute astrological features from birth data
- Calculate planetary positions, aspects, elements
- Generate numeric feature vectors
- After each write, enqueue `astro.interpret` jobs on the same topic

**Backends**:
- Swiss Ephemeris (preferred)
- Skyfield fallback (if ephemeris data missing)

**Dependencies**: Database, Kafka

### 7. Astro Interpreter Worker (`service/worker_astro_interpret`)

**Technology**: Python  
**Queue**: `astro` (consumer group: `astro-interpreter`)  
**Responsibilities**:
- Consume `astro.interpret` jobs (produced by astro service)
- Load chart data (longs, houses, aspects, elements) from `astro_features`
- Call Ollama LLM to produce a short astrological reading
- Store result in `astro_interpretations`

**Dependencies**: Database, Kafka, Ollama

### 8. Stats Worker (`service/api` + `CorrelationWorker`)

**Technology**: Kotlin  
**Compose service name**: `stats-worker` (use this for `logs`, `restart`, `build`; the Kafka topic is `stats`).  
**Queue**: `stats`  
**Responsibilities**:
- Compute embeddings ↔ astro correlations asynchronously
- **Birth-year detrending** (astro-features mode only): for each embedding dimension, fit linear regression on birth year and use residuals so correlations are not confounded by cohort/generation. Enables comparison of:
  - **Real astrological signals** — planetary positions correlating with biography content independent of when someone was born
  - **Spurious cohort effects** — slow-moving outer planets (e.g. Pluto, Neptune, Uranus) acting as proxies for generation/era, which naturally correlate with biography style and content
- Output two feature-importance lists: `featureImportance` (original embeddings) and `featureImportanceDetrended` (residuals after removing birth-year trend). Only people with a known birth date (join to `birth` table) are included so both analyses use the same sample.
- Store summary in `job_status.result`
- Persist full correlation JSON to MinIO/S3 and return signed URLs

**Dependencies**: Database, Kafka, MinIO

## Data Flow

### Complete Pipeline

```
1. Upload XML
   Client → API → MinIO
   
2. Parse & Ingest
   API → Kafka Topic → Worker-Ingest → PostgreSQL
   (Embeddings are enqueued after wiki enrichment)
   
3. Resolve QIDs
   Resolver → Wikidata API → PostgreSQL
   Resolver → Fetch-Bio API (HTTP)
   
4. Fetch Biographies
   Fetch-Bio → Wikipedia API → PostgreSQL
   Fetch-Bio → Kafka Topic (embeddings)
   
5. Generate Embeddings
   Kafka Topic → Embeddings Worker → PostgreSQL
   
6. Compute Astro Features
   Kafka Topic (astro) → Astro Worker → PostgreSQL (astro_features)
   Astro Worker → Kafka Topic (astro.interpret)
   
7. Astro Interpreter
   Kafka Topic (astro) → Astro Interpreter Worker → Ollama → PostgreSQL (astro_interpretations)
```

### Database Schema

**Core Tables**:
- `person_raw` - Identity and XML reference
- `birth` - Date, time, location data
- `bio_text` - Biography text and metadata
- `embeddings_*` - Semantic text embeddings (pgvector)
- `astro_features` - Numeric astrological features
- `astro_interpretations` - LLM-generated astrological readings per person
- `provenance_event` - Process audit tracking

## Communication Patterns

### Synchronous (HTTP)
- Client ↔ API
- Resolver ↔ Fetch-Bio API
- Traits Worker ↔ Ollama

### Asynchronous (Kafka Topics)
- API → Worker-Ingest
- Worker-Ingest → Embeddings Worker
- Fetch-Bio → Traits Worker

### Database (PostgreSQL)
- All services read/write to shared database
- Connection pooling via HikariCP
- Exposed ORM for type-safe queries

## Deployment

### Containerization
- All services containerized with Docker
- Multi-stage builds for Kotlin services
- Python services use slim base images

### Orchestration
- Docker Compose for local development
- Health checks for all services
- Dependency management (depends_on)

### Environment Variables
- `PG_DSN` / `DATABASE_URL` - PostgreSQL connection
- `KAFKA_BOOTSTRAP_SERVERS` - Kafka connection
- `MINIO_*` - Object storage configuration
- `OLLAMA_URL` - LLM service URL
- `OLLAMA_TIMEOUT` - Read timeout in seconds for Ollama API (default 300)
- `LLM_MODEL` - Model name for astro interpreter (astrological reading generation)

## Scalability Considerations

### Horizontal Scaling
- **API**: Stateless, can scale horizontally
- **Workers**: Multiple instances can process different jobs
- **Database**: Single instance (can be replicated)

### Queue Management
- Kafka-based job queues
- Multiple workers can consume from same topic using consumer groups
- Job status tracking in PostgreSQL (`job_status`)

### Resource Requirements
- **CPU**: Moderate (LLM inference is CPU-bound)
- **Memory**: ~12GB for full stack
- **Storage**: Depends on dataset size
- **Network**: External API calls (Wikidata, Wikipedia)

## Security Considerations

- Database credentials via environment variables
- MinIO access keys for object storage
- No authentication on API (add for production)
- Network isolation via Docker networks
- Health checks for service monitoring

## Monitoring & Observability

- Health check endpoints on all services
- Grafana for metrics (optional)
- Provenance events for audit trail
- Job status tracking in PostgreSQL
- Structured logging (SLF4J/Logback)

## Future Enhancements

1. **Swiss Ephemeris JNI**: Full implementation for accurate astro calculations
4. **Metrics Export**: Prometheus integration
5. **Distributed Tracing**: OpenTelemetry support
6. **Statistical Analysis**: Correlation computation service
