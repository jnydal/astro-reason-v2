# Non-Functional Requirements: Observability

This document describes observability expectations for the astro-reason system.

## Goals

- Provide end-to-end visibility into data ingest, feature generation, and scoring.
- Enable rapid detection, diagnosis, and recovery from failures.
- Support performance tuning and capacity planning.

## Signals

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

