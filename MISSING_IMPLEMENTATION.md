Recommended Next Steps (Priority Order)

1. Pipeline Monitoring & Observability (NICE TO HAVE)
- Add metrics for pipeline completion rates
- Dashboard showing: people processed, traits scored, embeddings computed, correlations computed
- Alerting for stuck jobs or failures

2. Implement Statistical Analysis Service (CORE PURPOSE)
This is the final step. Create a new service or add endpoints to:

Features needed:
- Correlation analysis between `nlp_vectors` (8 Burlan vectors) and `astro_features`
- Statistical tests (Pearson, Spearman, p-values)
- Visualization data export (JSON/CSV)
- Feature importance analysis
- Clustering based on embeddings + astro features

Implementation approach:
- New Kotlin service `service/stats` or add endpoints to API
- Use Kotlin statistics libraries (e.g., `org.apache.commons:commons-math3`)
- Or call Python via HTTP (scipy, pandas, scikit-learn)

3. Embeddings DB coherence + migration handling (CORE PIPELINE)
- Ensure embeddings schema matches worker writes (text_hash, meta, source, updated_at)
- Confirm `pgvector` extension is available and migrations are actually applied
- Validate insert routing for `embeddings` view with `ON CONFLICT` behavior
  - If upserts against the view fail, write to dimensioned tables directly
