# Astro-Reason Architecture

## Overview

Astro-Reason is a microservices-based research pipeline that evaluates correlations between astrological birth chart configurations and personality traits derived from biographical text. The system uses NLP to extract personality structure from biographies and compares those traits against encoded astrological features.

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

5. Traits Worker (Kotlin)
   ├─→ Reads from Kafka (traits topic)
   ├─→ Calls → Ollama LLM (HTTP)
   └─→ Writes → PostgreSQL (nlp_vectors)

6. Astro Service (Kotlin)
   ├─→ Processes birth data
   ├─→ Computes astrological features
   └─→ Writes → PostgreSQL (astro_features)
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
- **Framework**: FastAPI (fetch-bio), Kafka consumer (embeddings)
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

**Dependencies**: Database, Kafka, MinIO

### 2. Worker-Ingest (`service/worker-ingest`)

**Technology**: Kotlin  
**Queue**: `default`  
**Responsibilities**:
- Parse AstroDatabank XML files
- Extract person, birth, and biography data
- Batch insert into PostgreSQL
- Enqueue embedding jobs

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

### 6. Traits Worker (`service/traits`)

**Technology**: Kotlin  
**Queue**: `traits`  
**Responsibilities**:
- Score biographies using Yuri Burlan's 8-vector psychology
- Call Ollama LLM for structured JSON output
- Store personality vectors in database

**Vectors Scored**:
- sound, visual, oral, anal, urethral, skin, muscular, olfactory

**Dependencies**: Database, Kafka, Ollama

### 7. Astro Service (`service/astro`)

**Technology**: Kotlin  
**Mode**: Batch processing  
**Responsibilities**:
- Compute astrological features from birth data
- Calculate planetary positions, aspects, elements
- Generate numeric feature vectors

**Backends**:
- Swiss Ephemeris (JNI - pending full implementation)
- Fallback implementation (currently active)

**Dependencies**: Database

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
   
6. Score Traits
   [Manual trigger or polling] → Traits Worker → Ollama → PostgreSQL
   
7. Compute Astro Features
   Astro Service → PostgreSQL
```

### Database Schema

**Core Tables**:
- `person_raw` - Identity and XML reference
- `birth` - Date, time, location data
- `bio_text` - Biography text and metadata
- `nlp_vectors` - Burlan 8-vector personality scores
- `embeddings_*` - Semantic text embeddings (pgvector)
- `astro_features` - Numeric astrological features
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
- `LLM_MODEL` - Model name for traits scoring

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
