# Astro‑Reason Pipeline: Scientific Dataflow and Methodology (NLP & Computational Social Science)

## 1. Abstract
Astro‑Reason is a multi‑stage pipeline that transforms biographical and temporal‑spatial records into structured textual, semantic, and astrological features. This document details the scientific workflow, including data ingestion, entity resolution, enrichment, embedding generation, trait inference, and deterministic astronomical feature computation. We emphasize reproducibility, provenance, and methodological transparency to support research‑grade analysis.

---

## 2. Scientific Goals
The pipeline enables:
- Population‑level analyses of biographical text and inferred traits  
- Semantic similarity and clustering in large corpora of biographies  
- Exploratory correlations between astrological features and NLP‑derived traits  
- Auditability and reproducibility for scientific usage  

---

## 3. Data Sources and Inputs

### 3.1 Primary Inputs
Structured biographical records (e.g., XML) containing:
- Name  
- Birth date, time, and location  
- Optional biography stub or text  

### 3.2 Enrichment Inputs
- Wikidata QIDs (entity resolution)  
- Wikipedia biographies (language‑specific, e.g., English)  

### 3.3 Licensing & Ethics
- Wikipedia text is licensed under **CC BY‑SA 4.0**.  
- Usage must comply with attribution requirements.  
- Sensitive data should be excluded or anonymized as required by law or ethics guidelines.  

---

## 4. Pipeline Overview

1. **Ingest**: parse structured records into canonical storage  
2. **Entity Resolution**: link internal entities to Wikidata identifiers  
3. **Enrichment**: fetch Wikipedia text for resolved entities  
4. **Embeddings**: compute semantic vectors from biography text  
5. **Trait Scoring**: infer interpretable trait vectors using an LLM  
6. **Astrological Features**: compute deterministic astronomical feature vectors  
7. **Provenance & Observability**: log stage outputs and counts  

---

## 5. Data Model (Research‑Facing)

### 5.1 Core Tables
- `person_raw`: canonical identity and source identifiers  
- `birth`: date/time/timezone/location  
- `bio_text`: text, hash, source metadata  
- `entity_link`: canonical entity resolution info  
- `embeddings_*`: dense vector representations  
- `nlp_vectors`: interpretable trait outputs  
- `astro_features`: deterministic astrological features  

---

## 6. Methods by Stage

### 6.1 Ingestion (Parsing & Canonicalization)
**Input:** structured records  
**Output:** normalized `person_raw`, `birth`, `bio_text` rows  
**Scientific concern:** reproducibility of parsing rules and normalization logic.  

---

### 6.2 Entity Resolution (Wikidata Matching)
**Input:** name + birth date  
**Approach:**  
- Query Wikidata search API  
- Validate candidate with DOB matching when possible  
- Persist chosen QID with provenance metadata  

**Scientific concern:**  
- False positives/negatives in entity matching  
- Coverage bias toward public figures  

**Recommendation:**  
- Evaluate resolution accuracy on a gold‑standard subset  
- Report precision/recall for entity resolution  

---

### 6.3 Enrichment (Wikipedia Retrieval)
**Input:** QID  
**Output:** `bio_text` from Wikipedia wikitext (cleaned)  

**Scientific concern:**  
- Text source bias (Wikipedia skew)  
- Language bias (English‑first unless extended)  

---

### 6.4 Embedding Computation (NLP Representation)
**Input:** biography text  
**Method:**  
- Sentence‑transformer embeddings using a fixed model (e.g., `BAAI/bge-large-en-v1.5`)  
- Store vectors by dimensionality (e.g., 1024)  

**Scientific concern:**  
- Embeddings inherit model‑specific biases  
- Representations change across model versions  

**Recommendation:**  
- Version‑pin embeddings model and record in provenance  
- Use text hashes to track changes  

---

### 6.5 Trait Inference (LLM‑based)
**Input:** biography text  
**Method:**  
- LLM prompts produce structured trait vectors (`nlp_vectors`)  
- Store model name, provider, confidence, prompt hash  

**Scientific concern:**  
- Construct validity: traits are model‑generated, not clinically validated  
- Sensitivity to prompt design  

**Recommendation:**  
- Pre‑register trait definitions and prompt templates  
- Validate output consistency on a human‑annotated benchmark  

---

### 6.6 Astrological Feature Computation
**Input:** birth date/time/location  
**Method:**  
- Deterministic astronomical calculations  
- Output: planetary longitudes, houses, aspects, derived ratios  

**Scientific concern:**  
- Accuracy depends on time and location completeness  
- Must record whether birth time is unknown  

---

## 7. Provenance and Reproducibility

Every stage produces provenance events capturing:
- Stage ID  
- Status  
- Counts  
- Duration  
- Model versions / parameters  
- Timestamps  

**Reproducibility Checklist:**  
- [ ] Dataset ID and version  
- [ ] Parser version & config  
- [ ] Resolver parameters and matching logic  
- [ ] External data source versions  
- [ ] Embedding model/version  
- [ ] LLM model/version and prompt hash  
- [ ] Astro ephemeris source/version  
- [ ] Commit hash of pipeline code  

---

## 8. Validation & Evaluation Plan

### 8.1 Entity Resolution
- Gold standard QID mapping on a stratified sample  
- Report precision/recall  

### 8.2 Embeddings
- Compare semantic similarity with external benchmarks (e.g., STS)  
- Qualitative checks on clustered outputs  

### 8.3 Traits
- Human‑annotated trait scores for a subset  
- Agreement statistics (e.g., Spearman correlation, Cohen’s kappa)  

### 8.4 Astrological Features
- Compare with known ephemeris outputs for validation  

---

## 9. Bias & Limitations
- Coverage bias: Wikipedia and Wikidata overrepresent notable individuals  
- Language bias: English‑first enrichment reduces representativeness  
- Demographic bias: underrepresentation of certain regions/groups  
- Model bias: embeddings/LLM traits inherit training data biases  

---

## 10. Ethical Considerations
- Trait inference is speculative and should not be used for clinical or high‑stakes decisions  
- Ensure responsible disclosure of limitations when presenting results  
- Respect data licensing and privacy constraints  

---

## 11. Monitoring & Observability (Operational Science Metrics)
- Total people ingested  
- Resolved QIDs  
- Enriched biographies  
- Embeddings computed  
- Traits scored  
- Astro features computed  

These metrics serve as **population coverage indicators**, not quality metrics.

---

## 12. Conclusion
Astro‑Reason provides an auditable, reproducible pipeline that converts structured biographical records into semantic and trait representations. Its scientific utility depends on explicit documentation of model choices, provenance, and bias mitigation. With appropriate validation, the pipeline can support computational social science and NLP investigations at scale.

---

## 13. References (Suggested)
- Sentence‑Transformers model documentation  
- Wikidata API documentation  
- Wikipedia content licensing  
- LLM model documentation  
- Ephemeris data sources  