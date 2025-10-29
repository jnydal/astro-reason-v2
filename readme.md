# Astro-Reason

Astro-Reason is a research-oriented pipeline for evaluating whether statistically meaningful correlations exist between birth chart configurations and personality traits derived from biographical text.

The system uses NLP to extract personality structure from biographies (via Yuri Burlan’s System-Vector Psychology) and compares those numerical traits against encoded astrological features. Semantic embeddings are also produced to support similarity analysis, clustering, and data quality control.

---

## Abstract

Astrology claims that birth circumstances influence personality. Modern psychology typically claims personality is shaped entirely by environment and development. Astro-Reason provides a data-driven bridge between these perspectives.

The system:

1. Ingests and enriches biographical datasets (AstroDatabank C-sample + Wikipedia).
2. Performs NLP analysis to derive:
   - **Yuri Burlan 8-vector personality profile** using a local LLM
   - **Semantic embeddings** to capture broader personality “vibe”
3. Encodes birth charts into **numeric astro features**
4. Applies statistical correlation and modeling to evaluate alignment

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
   ┌─────────────┬─────────────┐
   ↓ NLP: LLM scoring              ↓ NLP: embeddings
 nlp_vectors (8D Burlan)        embeddings (semantic)
                                  ↓
                           similarity + QC
        ↓
Astrological encoding (ephemeris)
 astro_features (numeric)
        ↓
Stats + correlation visualization
```

### System Components

| Component | Description |
|----------|-------------|
| API (FastAPI) | Uploads, triggers jobs, serves results |
| Worker | NLP + astro computation jobs |
| **Ollama** (local LLM) | Burlan vector scoring with controlled JSON output |
| Embeddings service | Semantic vector creation (BGE models) |
| PostgreSQL + pgvector | Data and vector storage |
| Redis | Job queue |
| MinIO | Raw biography text object storage |
| Grafana (optional) | Metrics |

---

## Personality Model

The personality engine scores biographies on Yuri Burlan’s **8 vectors**:

- sound
- visual
- oral
- anal
- urethral
- skin
- muscular
- olfactory

Output example:

```json
{
  "vectors": { "sound": 6, "visual": 4, "oral": 4, "anal": 3, "urethral": 4, "skin": 4, "muscular": 3, "olfactory": 4 },
  "dominant": ["sound"],
  "rationale": { "...": "..." },
  "confidence": 0.62,
  "provider": "ollama",
  "model_name": "qwen2.5:7b-instruct-q4_K_M",
  "prompt_hash": "sha256..."
}
```

These structured, interpretable vectors allow direct comparison against astrological archetypal features.

---

## Database Schema (Core Tables)

- `person_raw` — identity & XML reference
- `birth` — date/time/location data
- `bio_text` — enriched biography metadata
- `nlp_vectors` — Burlan vector scoring results
- `embeddings` — semantic text embeddings
- `astro_features` — numeric planetary/house/aspect features
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

Verify:

```bash
docker compose ps
```

### Load the Ollama LLM

```bash
docker exec -it <local-llm-container-name> ollama pull qwen2.5:7b-instruct-q4_K_M
```

Test:

```bash
curl http://localhost:8001/api/tags
```

### Worker test (LLM scoring)

```bash
docker compose run --rm worker python -m app.worker_main
```

Expected: valid JSON Burlan scoring.

---

## Typical Usage Flow

1. Upload AstroDatabank dataset via frontend or API
2. Trigger enrichment (Wikipedia fetch)
3. Run batch processing:
   - Embeddings
   - Burlan vectors
   - Astro encoding

Each processing step logs a provenance record for reproducibility.

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
