from __future__ import annotations

from decimal import Decimal
from uuid import uuid4

from psycopg2.extras import Json

from trr_backend.repositories import cast_screentime


def test_json_wrap_sanitizes_decimal_and_uuid_values():
    payload = {
        "screen_time_seconds": Decimal("2.0"),
        "person_id": uuid4(),
        "nested": [{"confidence": Decimal("0.95")}],
    }

    wrapped = cast_screentime._json(payload)

    assert isinstance(wrapped, Json)
    adapted = wrapped.adapted
    assert adapted["screen_time_seconds"] == 2.0
    assert isinstance(adapted["person_id"], str)
    assert adapted["nested"][0]["confidence"] == 0.95


def test_replace_cast_screentime_evidence_dedupes_duplicate_evidence_keys(monkeypatch):
    captured = {}

    class _DummyCursor:
        def execute(self, sql, params):
            captured["delete_sql"] = sql
            captured["delete_params"] = params

    class _CursorContext:
        def __enter__(self):
            return _DummyCursor()

        def __exit__(self, exc_type, exc, tb):
            return False

    class _ConnContext:
        def __enter__(self):
            return object()

        def __exit__(self, exc_type, exc, tb):
            return False

    def _fake_execute_values_returning(sql, rows, *, conn=None):
        captured["insert_sql"] = sql
        captured["rows"] = rows
        return [{"evidence_key": row[2]} for row in rows]

    monkeypatch.setattr(cast_screentime.pg, "db_connection", lambda: _ConnContext())
    monkeypatch.setattr(cast_screentime.pg, "db_cursor", lambda conn=None: _CursorContext())
    monkeypatch.setattr(cast_screentime.pg, "execute_values_returning", _fake_execute_values_returning)

    result = cast_screentime.replace_cast_screentime_evidence(
        "run-1",
        [
            {
                "segment_key": "segment-a",
                "evidence_key": "still-1",
                "evidence_type": "still",
                "timestamp_ms": 100,
                "object_key": "a.jpg",
                "content_type": "image/jpeg",
            },
            {
                "segment_key": "segment-b",
                "evidence_key": "still-1",
                "evidence_type": "still",
                "timestamp_ms": 125,
                "object_key": "b.jpg",
                "content_type": "image/jpeg",
            },
        ],
    )

    assert captured["delete_params"] == ["run-1"]
    assert len(captured["rows"]) == 1
    assert captured["rows"][0][2] == "still-1"
    assert captured["rows"][0][4] == 125
    assert captured["rows"][0][5] == "b.jpg"
    assert result == [{"evidence_key": "still-1"}]
