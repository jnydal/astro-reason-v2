Recommended Next Steps (Priority Order)

1. Wire Up Traits Pipeline (HIGH PRIORITY)
The traits worker is ready but idle. After fetch-bio writes biographies, it should enqueue traits jobs.
Option A: Modify fetch-bio to enqueue traits jobs (Recommended)
After successfully fetching a biography, enqueue a traits job for that person
Requires Redis access in the Python fetch-bio service
Option B: Make traits worker poll database (Simpler)
Change TraitWorker.kt to poll for people with bios but no traits (like resolver/astro do)
No queue changes needed
Option C: Add API endpoint to trigger traits (Manual)
Add /api/traits/trigger endpoint that enqueues jobs for people ready for scoring
2. Implement Statistical Analysis Service (CORE PURPOSE)
This is the final step. Create a new service or add endpoints to:
Features needed:
Correlation analysis between nlp_vectors (8 Burlan vectors) and astro_features
Statistical tests (Pearson, Spearman, p-values)
Visualization data export (JSON/CSV)
Feature importance analysis
Clustering based on embeddings + astro features
Implementation approach:
New Kotlin service service/stats or add endpoints to API
Use Kotlin statistics libraries (e.g., org.apache.commons:commons-math3)
Or call Python via HTTP (scipy, pandas, scikit-learn)
3. Complete Swiss Ephemeris Integration (MEDIUM PRIORITY)
Replace the fallback with accurate calculations:
Set up JNI bindings for Swiss Ephemeris
Update AstroFeatures.kt to use JNI backend
Test accuracy against known ephemeris data
4. Pipeline Monitoring & Observability (NICE TO HAVE)
Add metrics for pipeline completion rates
Dashboard showing: people processed, traits scored, correlations computed
Alerting for stuck jobs or failures