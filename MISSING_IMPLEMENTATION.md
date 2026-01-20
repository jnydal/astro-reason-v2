Recommended Next Steps (Priority Order)

1. Complete Swiss Ephemeris Integration (MEDIUM PRIORITY) **(DONE – initial JVM integration)**
Replace the fallback with accurate calculations:
- Wire in Swiss Ephemeris Java/JNI wrapper and ephemeris data path
- Update `AstroFeatures.kt` to use the Swiss Ephemeris backend when `ASTRO_BACKEND=swisseph`
- Keep fallback backend as a safe default and add spot‑checks against known ephemeris data

2. Pipeline Monitoring & Observability (NICE TO HAVE)
- Add metrics for pipeline completion rates
- Dashboard showing: people processed, traits scored, embeddings computed, correlations computed
- Alerting for stuck jobs or failures

3. Implement Statistical Analysis Service (CORE PURPOSE)
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
