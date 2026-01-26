from service.worker_embeddings.src.app.core import provenance


class _FakeCursor:
    def __init__(self) -> None:
        self.calls = []

    def execute(self, query, params):
        self.calls.append((query, params))


def test_log_event_builds_detail_payload():
    cur = _FakeCursor()

    provenance.log_event(
        cur,
        stage="embed",
        status="ok",
        count=3,
        duration_ms=12,
        error=None,
        meta={"model": "bge"},
        person_id="p1",
    )

    assert len(cur.calls) == 1
    query, params = cur.calls[0]
    assert "INSERT INTO provenance_event" in query
    assert params[0] == "p1"
    assert params[1] == "embed"
    # psycopg2 Json adapter exposes payload via .adapted in tests
    payload = getattr(params[2], "adapted", getattr(params[2], "obj", None))
    assert payload["status"] == "ok"
    assert payload["count"] == 3
    assert payload["duration_ms"] == 12
    assert payload["meta"] == {"model": "bge"}
