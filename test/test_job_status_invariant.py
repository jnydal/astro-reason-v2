"""
Integration test: pipeline enqueues must create a job_status row (architecture invariant).

Ensures that when we enqueue via the normal path (insert then Kafka), the returned job_id
has a row in job_status so status lookups work. Guards against new enqueue paths that
bypass job_status.
"""
from __future__ import annotations

import os
import sys

import pytest

# Ensure API src is on path (service.api.src may not be a package)
_TEST_ROOT = os.path.dirname(os.path.abspath(__file__))
_API_SRC = os.path.join(_TEST_ROOT, "..", "service", "api", "src")
if _API_SRC not in sys.path:
    sys.path.insert(0, _API_SRC)

# Only run when DB and Kafka are available (e.g. docker-compose.test.yml)
pytest.importorskip("psycopg2")
pytest.importorskip("confluent_kafka")


@pytest.mark.integration
def test_enqueue_parse_adb_xml_creates_job_status_row():
    """Enqueue via API jobs path; assert job_id has a row in job_status with status QUEUED."""
    if not os.getenv("PG_DSN") and not os.getenv("DATABASE_URL"):
        pytest.skip("PG_DSN or DATABASE_URL required for integration test")
    if not os.getenv("KAFKA_BOOTSTRAP_SERVERS"):
        pytest.skip("KAFKA_BOOTSTRAP_SERVERS required for integration test")

    import jobs as api_jobs

    object_uri = "s3://test-bucket/invariant-test/fake.xml"
    job = api_jobs.enqueue_parse_adb_xml(object_uri, source_label="test-invariant")
    job_id = job["id"]
    assert job_id

    row = api_jobs.fetch_job_status(job_id)
    assert row is not None, "Enqueued job must have a row in job_status (architecture invariant)"
    assert row["status"] == "QUEUED", "Newly enqueued job should be QUEUED"
