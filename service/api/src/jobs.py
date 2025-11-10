import os
from redis import Redis
from rq import Queue

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
redis = Redis.from_url(REDIS_URL, socket_timeout=2, socket_connect_timeout=2)
q = Queue("default", connection=redis)

def enqueue_parse_adb_xml(object_uri: str, source_label: str = "upload"):
    """
    Enqueue the downstream ingest job that your worker will implement.
    Dotted path should match your worker code entry (see worker example below).
    """
    return q.enqueue(
        "worker.ingest.parse_adb_xml",
        object_uri,
        {"source": source_label},
        job_timeout=1800,  # 30 min
        failure_ttl=86400,
        result_ttl=86400,
        retry_strategy={"max": 3, "interval": 30},
    )
