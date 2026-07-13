from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import pytest

import trr_backend.socials.control_plane.run_lifecycle as run_lifecycle
import trr_backend.socials.social_season_analytics_impl as social_impl
from trr_backend.db import pg

_RUN_ID = "11111111-1111-1111-1111-111111111111"
_STAGE = "instagram_comments_scrapling"


@contextmanager
def _ctx(value: object) -> Iterator[object]:
    yield value


@contextmanager
def _ctx_recording_exit(value: object, events: list[str]) -> Iterator[object]:
    events.append("transaction_started")
    try:
        yield value
    except Exception:
        events.append("transaction_rolled_back")
        raise
    else:
        events.append("transaction_committed")


def _finish_counter_capture(
    monkeypatch: pytest.MonkeyPatch,
    *,
    prior_status: str,
    new_status: str,
    prior_items_found: int = 2,
    new_items_found: int = 5,
) -> dict[str, Any]:
    conn = object()
    cur = object()
    captured: dict[str, Any] = {}

    monkeypatch.setattr(run_lifecycle.legacy, "_run_counter_columns_ready", lambda: True)
    monkeypatch.setattr(run_lifecycle.legacy.pg, "db_connection", lambda: _ctx(conn))
    monkeypatch.setattr(run_lifecycle.legacy.pg, "db_cursor", lambda *, conn=None: _ctx(cur))

    def _fake_fetch_one_with_cursor(cursor: object, sql: str, params: list[object]) -> dict[str, Any]:
        assert cursor is cur
        assert "from social.scrape_runs" in sql
        assert params == [_RUN_ID]
        return {
            "total_jobs": 10,
            "completed_jobs": 3,
            "failed_jobs": 1,
            "active_jobs": 4,
            "items_found_total": 20,
            "stage_counts": {
                _STAGE: {
                    "total": 6,
                    "completed": 1,
                    "failed": 0,
                    "active": 4,
                },
            },
        }

    def _fake_persist_run_counters_and_summary(**kwargs: Any) -> dict[str, Any]:
        captured.update(kwargs)
        return {}

    monkeypatch.setattr(run_lifecycle.legacy.pg, "fetch_one_with_cursor", _fake_fetch_one_with_cursor)
    monkeypatch.setattr(run_lifecycle, "_persist_run_counters_and_summary", _fake_persist_run_counters_and_summary)

    run_lifecycle._increment_run_counters_on_job_finish(
        run_id=_RUN_ID,
        stage=_STAGE,
        prior_status=prior_status,
        new_status=new_status,
        prior_items_found=prior_items_found,
        new_items_found=new_items_found,
    )

    assert captured["conn"] is conn
    assert captured["run_id"] == _RUN_ID
    return captured


@pytest.mark.parametrize(
    ("prior_status", "new_status", "completed_delta", "failed_delta"),
    [
        ("running", "completed", 1, 0),
        ("running", "failed", 0, 1),
        ("retrying", "completed", 1, 0),
        ("retrying", "failed", 0, 1),
    ],
)
def test_increment_run_counters_on_job_finish_terminal_transitions(
    monkeypatch: pytest.MonkeyPatch,
    prior_status: str,
    new_status: str,
    completed_delta: int,
    failed_delta: int,
) -> None:
    captured = _finish_counter_capture(
        monkeypatch,
        prior_status=prior_status,
        new_status=new_status,
    )

    assert captured["total_jobs"] == 10
    assert captured["completed_jobs"] == 3 + completed_delta
    assert captured["failed_jobs"] == 1 + failed_delta
    assert captured["active_jobs"] == 3
    assert captured["items_found_total"] == 23
    assert captured["stage_counts"][_STAGE] == {
        "total": 6,
        "completed": 1 + completed_delta,
        "failed": failed_delta,
        "active": 3,
    }


def test_finish_job_preserves_primary_update_when_counter_sync_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    counter_calls: list[dict[str, Any]] = []
    events: list[str] = []
    conn = object()

    class _Cursor:
        def execute(self, sql: str) -> None:
            events.append(sql)

    cur = _Cursor()

    monkeypatch.setattr(
        social_impl.pg,
        "db_connection",
        lambda **_kwargs: _ctx_recording_exit(conn, events),
    )
    monkeypatch.setattr(social_impl.pg, "db_cursor", lambda *, conn=None, **_kwargs: _ctx(cur))

    def _fake_finish_fetch(cursor: object, _sql: str, params: list[object]) -> dict[str, Any]:
        assert cursor is cur
        assert params[0] == "job-1"
        events.append("job_update_returned")
        return {
            "id": "job-1",
            "run_id": _RUN_ID,
            "prior_status": "running",
            "prior_items_found": 2,
            "stage": _STAGE,
        }

    monkeypatch.setattr(social_impl.pg, "fetch_one_with_cursor", _fake_finish_fetch)

    def _unavailable_counter_sync(**kwargs: Any) -> None:
        counter_calls.append(kwargs)
        events.append("counter_failed")
        raise pg.DatabaseServiceUnavailableError("counter sync unavailable")

    monkeypatch.setattr(social_impl, "_increment_run_counters_on_job_finish", _unavailable_counter_sync)
    monkeypatch.setattr(social_impl, "_clear_worker_heartbeat_for_job", lambda **_kwargs: None)
    monkeypatch.setattr(social_impl, "_update_shared_account_partition_status_for_job", lambda **_kwargs: None)
    monkeypatch.setattr(social_impl, "_finalize_run_status", lambda *_args, **_kwargs: {"status": "deferred"})
    monkeypatch.setattr(social_impl, "_invalidate_queue_status_cache", lambda: None)

    social_impl._finish_job("job-1", status="failed", items_found=5)

    assert counter_calls == [
        {
            "run_id": _RUN_ID,
            "stage": _STAGE,
            "prior_status": "running",
            "new_status": "failed",
            "prior_items_found": 2,
            "new_items_found": 5,
            "conn": conn,
        },
    ]
    assert events == [
        "transaction_started",
        "job_update_returned",
        "SAVEPOINT finish_job_counter_sync",
        "counter_failed",
        "ROLLBACK TO SAVEPOINT finish_job_counter_sync",
        "RELEASE SAVEPOINT finish_job_counter_sync",
        "transaction_committed",
    ]
