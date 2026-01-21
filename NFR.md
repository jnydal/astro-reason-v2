# Non-Functional Requirements: Observability

This document describes non-functional requirements for the astro-reason system.

## Goals

- Provide end-to-end visibility into data ingest, feature generation, and scoring.
- Enable rapid detection, diagnosis, and recovery from failures.
- Support performance tuning and capacity planning.

## Reliability & Resilience

- **Idempotency:** All pipeline stages should safely reprocess the same input without corrupting state.
- **Retries:** External calls (Wikidata, Wikipedia, Ollama) use bounded retries with backoff.
- **Failure handling:** Jobs surface clear failure reason and can be re-enqueued safely.
- **Data integrity:** Writes must be transactional where possible; partial writes should be detectable.
- **Queue safety:** Workers must tolerate duplicate or out-of-order jobs.

## Performance & Scalability

- **Throughput:** Target steady-state processing with queue backlogs draining within 24 hours.
- **Batching:** Use batched inserts and vector writes to reduce DB overhead.
- **Horizontal scaling:** Workers should scale by adding instances per queue.
- **Resource caps:** Services should run within container memory/CPU limits and degrade gracefully.

## Security

- **AuthN/Z:** API endpoints should require authentication in production.
- **Secrets:** Credentials must be managed via environment variables or secret store.
- **Network:** Internal services isolated from public access; only API exposed.
- **Data access:** Database roles should follow least privilege.

## Data Governance

- **Provenance:** Pipeline actions recorded in `provenance_event` where applicable.
- **Retention:** Raw XML and biography text retention rules should be defined and enforced.
- **PII:** If personal data is present, redact or minimize in logs and exports.

## Availability & Recovery

- **Backups:** PostgreSQL backups at least daily with restore verification.
- **RPO/RTO:** Define recovery point and time objectives per environment.
- **Graceful degradation:** When external APIs fail, pipeline should pause and resume without data loss.

## Compliance & External Dependencies

- **Terms of use:** Respect Wikidata/Wikipedia usage guidelines and rate limits.
- **Attribution:** Ensure required attribution is preserved in downstream usage.

## Observability Signals

### Metrics

- **Pipeline throughput:** items processed per stage per time unit.
- **Latency:** end-to-end and per-stage processing time (p50/p95/p99).
- **Queue health:** backlog depth, age of oldest item, retry counts.
- **Error rate:** failures by stage, reason, and dependency.
- **Resource usage:** CPU, memory, disk, and network per service.

### Logs

- Structured logs with consistent fields: `service`, `stage`, `job_id`, `trace_id`,
  `qid` (when applicable), `status`, and `duration_ms`.
- Errors include stack traces and root-cause context.
- Sensitive data must be redacted or excluded.

### Traces

- Correlate requests/jobs across services using a shared `trace_id`.
- Capture spans for queue enqueue/dequeue, database calls, and external APIs.

## Dashboards

- **Pipeline overview:** stage throughput, latency, error rate, queue depth.
- **Service health:** resource usage and error spikes by service.
- **Dependency health:** database and external API response times.

## Alerts

- **Critical:** sustained pipeline outage, queue backlog growth without recovery,
  database connectivity errors, and repeated job failures.
- **Warning:** elevated latency, error rate above baseline, or resource saturation.
- Alerts must include actionable context (service, stage, time window, and links
  to dashboards/logs).

## Data Retention

- Metrics retained for at least 30 days.
- Logs retained for at least 14 days.
- Traces retained for at least 7 days.

## Responsibilities

- Service owners ensure instrumentation in new code paths.
- Operations owns dashboard and alert maintenance.

