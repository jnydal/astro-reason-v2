from typing import Any, Dict, Optional

import psycopg2.extras


def log_event(
    cur,
    stage: str,
    status: str = "ok",
    count: Optional[int] = None,
    duration_ms: Optional[int] = None,
    error: Optional[str] = None,
    meta: Optional[Dict[str, Any]] = None,
    person_id: Optional[str] = None,
) -> None:
    detail: Dict[str, Any] = {"status": status}
    if count is not None:
        detail["count"] = count
    if duration_ms is not None:
        detail["duration_ms"] = duration_ms
    if error is not None:
        detail["error"] = error
    if meta:
        detail["meta"] = meta

    cur.execute(
        "INSERT INTO provenance_event (person_id, stage, detail) VALUES (%s, %s, %s)",
        (person_id, stage, psycopg2.extras.Json(detail)),
    )
