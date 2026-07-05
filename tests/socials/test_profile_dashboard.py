"""Tests for social account profile dashboard composition."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any

import pytest

from trr_backend.socials import profile_dashboard
from trr_backend.socials.pipelines.account_catalog import launch as catalog_launch
from trr_backend.socials.pipelines.account_catalog import progress as catalog_progress


def _summary(catalog_recent_runs: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    return {
        "platform": "instagram",
        "account_handle": "thetraitorsus",
        "summary_detail": "lite",
        "catalog_recent_runs": catalog_recent_runs or [],
        "operational_alerts": [{"code": "needs_review"}],
    }


def test_active_catalog_run_fetches_progress(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    monkeypatch.setattr(
        profile_dashboard.analytics_repo,
        "get_social_account_profile_summary",
        lambda **kwargs: _summary([{"run_id": "run-active", "status": "running"}]),
    )

    def fake_progress(*args: Any, **kwargs: Any) -> dict[str, Any]:
        calls.append((args, kwargs))
        return {"run_id": args[2], "status": "running"}

    monkeypatch.setattr(profile_dashboard.analytics_repo, "get_social_account_catalog_run_progress", fake_progress)

    payload = profile_dashboard.build_social_account_profile_dashboard(
        platform="instagram",
        account_handle="thetraitorsus",
        detail="lite",
        run_id=None,
        recent_log_limit=12,
    )

    assert payload["data"]["catalog_run_progress"] == {"run_id": "run-active", "status": "running"}
    assert payload["operational_alerts"] == [{"code": "needs_review"}]
    assert calls == [(("instagram", "thetraitorsus", "run-active"), {"recent_log_limit": 12, "fast": True})]


def test_terminal_catalog_run_does_not_fetch_progress(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        profile_dashboard.analytics_repo,
        "get_social_account_profile_summary",
        lambda **kwargs: _summary([{"run_id": "run-done", "status": "completed"}]),
    )

    def fail_progress(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("progress should not be fetched for terminal runs")

    monkeypatch.setattr(profile_dashboard.analytics_repo, "get_social_account_catalog_run_progress", fail_progress)

    payload = profile_dashboard.build_social_account_profile_dashboard(
        platform="instagram",
        account_handle="thetraitorsus",
        detail="lite",
        run_id=None,
        recent_log_limit=12,
    )

    assert payload["data"]["catalog_run_progress"] is None


def test_catalog_stage_graph_all_cancelled_is_not_completed() -> None:
    status = catalog_progress._stage_status_from_payload(
        stage_name="comments",
        stages_payload={"comments": {"jobs_total": 2, "jobs_cancelled": 2}},
        job_rows=[],
    )

    assert status == "cancelled"


def test_catalog_progress_returns_failed_zero_job_launch_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    run_row = {
        "run_id": "run-failed-launch",
        "status": "failed",
        "config": {
            "launch_state": "failed",
            "launch_error_message": "task resolution failed",
            "launch_failed_at": "2026-07-02T12:00:00+00:00",
        },
        "created_at": None,
        "summary": {},
    }

    @contextmanager
    def fake_read_connection(**_kwargs):
        yield object()

    monkeypatch.setattr(catalog_progress.pg, "db_read_connection", fake_read_connection)
    monkeypatch.setattr(catalog_progress._core, "_load_social_account_catalog_run_row", lambda **_kwargs: run_row)
    monkeypatch.setattr(catalog_progress._core, "_load_social_account_catalog_jobs", lambda **_kwargs: [])

    def fail_recovery(**_kwargs):
        raise AssertionError("failed zero-job launch must not be treated as recoverable run_not_found")

    monkeypatch.setattr(catalog_progress._core, "recover_pending_social_account_catalog_launch", fail_recovery)

    payload = catalog_progress.get_social_account_catalog_run_progress(
        "instagram",
        "thetraitorsus",
        "run-failed-launch",
        fast=True,
    )

    assert payload["run_status"] == "failed"
    assert payload["launch_state"] == "failed"
    assert payload["launch_error_message"] == "task resolution failed"


def test_deferred_comments_skip_reason_beats_no_commentable_targets() -> None:
    result = catalog_launch.derive_comments_skip_reason(
        {
            "effective_selected_tasks": ["comments"],
            "target_readiness": {
                "can_start_comments": False,
                "commentable_target_count": 0,
            },
            "deferred_comments_followup": {
                "state": "pending",
                "platform": "instagram",
                "account_handle": "thetraitorsus",
            },
        }
    )

    assert result["reason"] == "comments_deferred_until_catalog_complete"


def test_explicit_run_id_overrides_inferred_active_run(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    monkeypatch.setattr(
        profile_dashboard.analytics_repo,
        "get_social_account_profile_summary",
        lambda **kwargs: _summary([{"run_id": "run-active", "status": "running"}]),
    )

    def fake_progress(*args: Any, **kwargs: Any) -> dict[str, Any]:
        calls.append((args, kwargs))
        return {"run_id": args[2]}

    monkeypatch.setattr(profile_dashboard.analytics_repo, "get_social_account_catalog_run_progress", fake_progress)

    payload = profile_dashboard.build_social_account_profile_dashboard(
        platform="instagram",
        account_handle="thetraitorsus",
        detail="lite",
        run_id="run-explicit",
        recent_log_limit=25,
    )

    assert payload["data"]["catalog_run_progress"] == {"run_id": "run-explicit"}
    assert calls == [(("instagram", "thetraitorsus", "run-explicit"), {"recent_log_limit": 25, "fast": True})]


def test_progress_call_receives_account_identity_run_id_and_log_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    summary_kwargs: dict[str, Any] = {}
    progress_calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    def fake_summary(*, platform: str, account_handle: str, detail: str) -> dict[str, Any]:
        summary_kwargs.update(
            {
                "platform": platform,
                "account_handle": account_handle,
                "detail": detail,
            }
        )
        return _summary([{"id": "run-from-id", "run_status": "in_progress"}])

    def fake_progress(*args: Any, **kwargs: Any) -> dict[str, Any]:
        progress_calls.append((args, kwargs))
        return {"logs": []}

    monkeypatch.setattr(profile_dashboard.analytics_repo, "get_social_account_profile_summary", fake_summary)
    monkeypatch.setattr(profile_dashboard.analytics_repo, "get_social_account_catalog_run_progress", fake_progress)

    profile_dashboard.build_social_account_profile_dashboard(
        platform="tiktok",
        account_handle="bravotv",
        detail="full",
        run_id=None,
        recent_log_limit=100,
    )

    assert summary_kwargs == {
        "platform": "tiktok",
        "account_handle": "bravotv",
        "detail": "full",
    }
    assert progress_calls == [(("tiktok", "bravotv", "run-from-id"), {"recent_log_limit": 100, "fast": True})]


def test_summary_exception_is_not_swallowed(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail_summary(**kwargs: Any) -> dict[str, Any]:
        raise RuntimeError("summary failed")

    monkeypatch.setattr(profile_dashboard.analytics_repo, "get_social_account_profile_summary", fail_summary)

    with pytest.raises(RuntimeError, match="summary failed"):
        profile_dashboard.build_social_account_profile_dashboard(
            platform="instagram",
            account_handle="thetraitorsus",
            detail="lite",
            run_id=None,
            recent_log_limit=12,
        )


def test_progress_exception_is_not_swallowed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        profile_dashboard.analytics_repo,
        "get_social_account_profile_summary",
        lambda **kwargs: _summary([{"run_id": "run-active", "status": "running"}]),
    )

    def fail_progress(*args: Any, **kwargs: Any) -> dict[str, Any]:
        raise RuntimeError("progress failed")

    monkeypatch.setattr(profile_dashboard.analytics_repo, "get_social_account_catalog_run_progress", fail_progress)

    with pytest.raises(RuntimeError, match="progress failed"):
        profile_dashboard.build_social_account_profile_dashboard(
            platform="instagram",
            account_handle="thetraitorsus",
            detail="lite",
            run_id=None,
            recent_log_limit=12,
        )
