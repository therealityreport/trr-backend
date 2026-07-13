from __future__ import annotations

from decimal import Decimal
from uuid import uuid4

import pytest
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


def test_get_subtitle_summary_returns_aggregate_counts_and_filters_skipped(monkeypatch):
    calls = []
    monkeypatch.setattr(
        cast_screentime.pg,
        "fetch_one",
        lambda sql, params: (
            {
                "video_asset_id": "asset-1",
                "status": "complete",
                "attempts": 1,
            }
            if "FROM ml.analysis_media_assets" in sql
            else {
                "discovered_track_count": 2,
                "eligible_track_count": 1,
                "completed_track_count": 1,
                "failed_track_count": 0,
                "primary_track_id": "track-1",
            }
        ),
    )

    def _fetch_all(sql, params):
        calls.append(sql)
        return [{"id": "track-1", "selection_status": "eligible_english"}]

    monkeypatch.setattr(cast_screentime.pg, "fetch_all", _fetch_all)

    result = cast_screentime.get_subtitle_summary("asset-1")

    assert result["status"] == "complete"
    assert result["discovered_track_count"] == 2
    assert result["primary_track_id"] == "track-1"
    assert "selection_status = 'eligible_english'" in calls[0]


def test_queue_subtitle_extraction_does_not_redispatch_active_job(monkeypatch):
    class _ConnectionContext:
        def __enter__(self):
            return object()

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(cast_screentime.pg, "db_connection", lambda **_kwargs: _ConnectionContext())
    monkeypatch.setattr(
        cast_screentime.pg,
        "fetch_one",
        lambda *_args, **_kwargs: {"video_asset_id": "asset-1", "status": "running"},
    )
    monkeypatch.setattr(
        cast_screentime.pg,
        "execute_returning",
        lambda *_args, **_kwargs: pytest.fail("active job must not be updated"),
    )

    result = cast_screentime.queue_subtitle_extraction("asset-1")

    assert result == {
        "video_asset_id": "asset-1",
        "status": "running",
        "already_active": True,
        "should_dispatch": False,
        "force": False,
    }


def test_queue_subtitle_extraction_force_requeues_complete_asset(monkeypatch):
    class _ConnectionContext:
        def __enter__(self):
            return object()

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(cast_screentime.pg, "db_connection", lambda **_kwargs: _ConnectionContext())
    monkeypatch.setattr(
        cast_screentime.pg,
        "fetch_one",
        lambda *_args, **_kwargs: {"video_asset_id": "asset-1", "status": "complete"},
    )
    monkeypatch.setattr(
        cast_screentime.pg,
        "execute_returning",
        lambda *_args, **_kwargs: [{"video_asset_id": "asset-1", "status": "queued"}],
    )

    result = cast_screentime.queue_subtitle_extraction("asset-1", force=True)

    assert result["status"] == "queued"
    assert result["should_dispatch"] is True
    assert result["force"] is True


def test_queue_subtitle_extraction_force_does_not_overlap_running_asset(monkeypatch):
    class _ConnectionContext:
        def __enter__(self):
            return object()

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(cast_screentime.pg, "db_connection", lambda **_kwargs: _ConnectionContext())
    monkeypatch.setattr(
        cast_screentime.pg,
        "fetch_one",
        lambda *_args, **_kwargs: {
            "video_asset_id": "asset-1",
            "status": "running",
            "is_stale": False,
        },
    )
    monkeypatch.setattr(
        cast_screentime.pg,
        "execute_returning",
        lambda *_args, **_kwargs: pytest.fail("a live worker must not be requeued"),
    )

    result = cast_screentime.queue_subtitle_extraction("asset-1", force=True)

    assert result["status"] == "running"
    assert result["already_active"] is True
    assert result["should_dispatch"] is False
    assert result["force"] is True


def test_queue_subtitle_extraction_force_recovers_only_stale_running_asset(monkeypatch):
    class _ConnectionContext:
        def __enter__(self):
            return object()

        def __exit__(self, exc_type, exc, tb):
            return False

    captured = {}
    monkeypatch.setattr(cast_screentime.pg, "db_connection", lambda **_kwargs: _ConnectionContext())
    monkeypatch.setattr(
        cast_screentime.pg,
        "fetch_one",
        lambda *_args, **_kwargs: {
            "video_asset_id": "asset-1",
            "status": "running",
            "is_stale": True,
        },
    )

    def _execute_returning(sql, params, **_kwargs):
        captured["sql"] = sql
        return [{"video_asset_id": "asset-1", "status": "queued"}]

    monkeypatch.setattr(cast_screentime.pg, "execute_returning", _execute_returning)

    result = cast_screentime.queue_subtitle_extraction("asset-1", force=True)

    assert result["status"] == "queued"
    assert result["already_active"] is False
    assert result["should_dispatch"] is True
    assert "subtitle_extraction_started_at = NULL" in captured["sql"]


def test_reconcile_stale_subtitle_extractions_bounds_interval(monkeypatch):
    captured = {}

    def _execute_returning(sql, params):
        captured["sql"] = sql
        captured["params"] = params
        return [{"id": "asset-1", "subtitle_extraction_status": "failed"}]

    monkeypatch.setattr(cast_screentime.pg, "execute_returning", _execute_returning)

    result = cast_screentime.reconcile_stale_subtitle_extractions(stale_after_seconds=1)

    assert result[0]["subtitle_extraction_status"] == "failed"
    assert captured["params"] == [60]
    assert "subtitle_worker_stale" in captured["sql"]
    assert "subtitle_extraction_status IN ('queued', 'running')" in captured["sql"]


def test_update_subtitle_extraction_status_rejects_untrusted_column_name():
    with pytest.raises(ValueError, match="Unsupported subtitle extraction timestamp"):
        cast_screentime.update_subtitle_extraction_status("asset-1", "failed", dropped_table_at="now")


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


def test_replace_cast_screentime_excluded_sections_writes_review_state(monkeypatch):
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
        return [{"review_kind": "excluded_section", "review_key": row[2]} for row in rows]

    monkeypatch.setattr(cast_screentime.pg, "db_connection", lambda: _ConnContext())
    monkeypatch.setattr(cast_screentime.pg, "db_cursor", lambda conn=None: _CursorContext())
    monkeypatch.setattr(cast_screentime.pg, "execute_values_returning", _fake_execute_values_returning)

    result = cast_screentime.replace_cast_screentime_excluded_sections(
        "run-1",
        [
            {
                "section_key": "cold-open",
                "section_type": "intro",
                "start_ms": 0,
                "end_ms": 12000,
                "duration_ms": 12000,
                "detection_source": "manual",
                "confidence_score": 1.0,
                "metadata": {"reason": "credits"},
            }
        ],
    )

    assert "DELETE FROM ml.screentime_review_state" in captured["delete_sql"]
    assert captured["delete_params"] == ["run-1"]
    assert "INSERT INTO ml.screentime_review_state" in captured["insert_sql"]
    assert captured["rows"][0][1] == "excluded_section"
    assert captured["rows"][0][2] == "cold-open"
    assert captured["rows"][0][10] == "run"
    assert captured["rows"][0][11] == "run-1"
    assert result == [{"review_kind": "excluded_section", "review_key": "cold-open"}]


def test_upsert_suggestion_decision_maps_review_columns_back_to_legacy_shape(monkeypatch):
    monkeypatch.setattr(
        cast_screentime.pg,
        "execute_returning",
        lambda _sql, _params: [
            {
                "review_key": "suggestion-1",
                "payload_json": {"person_id": "person-1"},
                "decision": "accepted",
            }
        ],
    )

    result = cast_screentime.upsert_suggestion_decision(
        {
            "show_id": "show-1",
            "owner_scope": "show",
            "owner_entity_id": "show-1",
            "suggestion_key": "suggestion-1",
            "person_id": "person-1",
            "decision": "accepted",
            "suggestion_payload": {"person_id": "person-1"},
            "decided_by": "admin-1",
        }
    )

    assert result["suggestion_key"] == "suggestion-1"
    assert result["suggestion_payload"] == {"person_id": "person-1"}
    assert result["decision"] == "accepted"


def test_upsert_unknown_review_state_maps_review_columns_back_to_legacy_shape(monkeypatch):
    monkeypatch.setattr(
        cast_screentime.pg,
        "execute_returning",
        lambda _sql, _params: [
            {
                "review_key": "queue-1",
                "payload_json": {"candidate_person_id": "person-2"},
                "decision": "defer",
            }
        ],
    )

    result = cast_screentime.upsert_unknown_review_state(
        {
            "show_id": "show-1",
            "owner_scope": "show",
            "owner_entity_id": "show-1",
            "queue_key": "queue-1",
            "queue_group": "episode",
            "candidate_person_id": "person-2",
            "decision": "defer",
            "queue_payload": {"candidate_person_id": "person-2"},
            "decided_by": "admin-1",
        }
    )

    assert result["queue_key"] == "queue-1"
    assert result["queue_payload"] == {"candidate_person_id": "person-2"}
    assert result["decision"] == "defer"
