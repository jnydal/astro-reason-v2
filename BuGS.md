Key blockers
1) Fetch‑bio not writing any bios
Eligible rows exist (702 rows match the “qid + not fetched yet” criteria), but fetch‑bio still writes 0.
This is the main reason traits and embeddings never start.
2) Missing Kafka topic: embeddings
The embeddings worker logs: UNKNOWN_TOPIC_OR_PART and cannot consume.
3) Astro service not running
It exited right away; no astro features will ever compute.
Recommended next steps
A) Create the missing Kafka topic
docker compose exec -T kafka rpk topic create embeddings
B) Investigate why fetch-bio returns 0
The API responds {"status":"ok","written":0,"message":"Fetched 0 biographies"}.
Yet the DB has hundreds of eligible rows.
If you want, I can dig into this and fix it (likely add logging + adjust fetch-bio selection or handling).
C) Restart astro worker if it should be long‑running
docker compose up -d astro
If you want me to proceed with deeper debugging + fixes (fetch‑bio + astro), say the word and I’ll continue.