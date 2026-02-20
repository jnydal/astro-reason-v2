High: hardcoded default credentials — docker-compose.yml includes defaults like postgres, minio123, admin. Fine for local dev, risky if reused in production.
Gaps and coherence risks
Stats/correlation service: Implemented as an async Kafka worker with DB summary + S3 full results; monitor throughput and storage costs as dataset scales. Birth-year detrending is implemented for astro-features mode (linear regression residuals per embedding dimension) so results include both `featureImportance` (original) and `featureImportanceDetrended` for comparing real astro signals vs cohort effects.



Not fully coherent yet. The astro features pipeline works, but there are integration gaps that make it inconsistent with the rest of the system.
Key issues:
Processing model is inconsistent: Astro uses DB polling loops while other services use queues and job status tracking.
Backend differences: Python uses Swiss Ephemeris → Skyfield fallback; this can yield different results if ephemeris data is missing.
Config/logging inconsistencies: Environment var naming and logging/provenance behavior should be standardized.
Net: setup is partially correct but not coherent with the rest of the system. The main risk is operational inconsistency (no job tracking) and silent backend shifts.
If you want, I can:
Standardize the feature computation inputs/outputs and document feature vector keys,
Add job status tracking or move astro processing onto the same worker/queue pattern,
Add a consistency test that verifies deterministic outputs for a fixed birth record.