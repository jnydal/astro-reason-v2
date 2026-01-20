Recommended Next Steps (Priority Order)

1. Traits & Embeddings Pipeline After Wiki Enrichment (**DONE**)
The system now triggers both traits scoring and semantic embeddings from the wiki enrichment step:
- After `fetch-bio` writes full Wikipedia biographies into `bio_text`, it:
  - Enqueues a traits job for each enriched person on the `traits` queue (`"traits.score_person"`), using the same JSON job format as the Kotlin `JobQueue`.
  - Enqueues a batched embeddings job on the `embeddings` RQ queue (`"embeddings.jobs.embed_person_bios"`) with all enriched `person_ids`.
- The ingest worker no longer enqueues embeddings directly; embeddings are now clearly downstream of “bio is ready”.

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

3. Complete Swiss Ephemeris Integration (MEDIUM PRIORITY)
Replace the fallback with accurate calculations:
- Set up JNI bindings for Swiss Ephemeris
- Update `AstroFeatures.kt` to use the JNI backend
- Test accuracy against known ephemeris data

4. Pipeline Monitoring & Observability (NICE TO HAVE)
- Add metrics for pipeline completion rates
- Dashboard showing: people processed, traits scored, embeddings computed, correlations computed
- Alerting for stuck jobs or failures