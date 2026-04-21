"""Tests for start_tiktok_posts_scrapling_scrape — TikTok mirror of Task 5."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from trr_backend.repositories import social_season_analytics as repo


def test_start_tiktok_posts_rejects_empty_account() -> None:
    # _normalize_social_account_profile_handle raises ValueError on empty input
    # before the function's SocialIngestValidationError guard runs.
    # SocialIngestValidationError subclasses ValueError, so ValueError is the
    # tightest correct assertion that excludes unrelated bugs.
    with pytest.raises(ValueError, match="handle"):
        repo.start_tiktok_posts_scrapling_scrape(
            account_handle="",
            initiated_by="test",
        )


def _build_lock_mocks(monkeypatch, *, locked: bool = True) -> None:
    """Set up the pg.* mocks needed for the start helper to run.

    Set locked=False to test the lock-contention branch.
    """
    lock_conn_mock = MagicMock()
    cur_mock = MagicMock()
    cur_mock.__enter__ = lambda self: self
    cur_mock.__exit__ = lambda *a: None
    lock_conn_mock.__enter__ = lambda self: lock_conn_mock
    lock_conn_mock.__exit__ = lambda *a: None
    monkeypatch.setattr(repo.pg, "db_connection", lambda *a, **k: lock_conn_mock)
    monkeypatch.setattr(repo.pg, "db_cursor", lambda *a, **k: cur_mock)
    monkeypatch.setattr(
        repo.pg,
        "fetch_one_with_cursor",
        lambda cur, q, params=None: {"locked": locked} if "advisory_lock" in q else {"unlocked": True},
    )


def test_start_tiktok_posts_sets_required_worker_lane(monkeypatch) -> None:
    """The created job must carry required_worker_lane in its config so the
    lane-enforcement filter kicks in when queue is enabled."""
    created_job_config: dict = {}

    def fake_create_run(*args, **kwargs):
        return "fake-run-id"

    def fake_create_job(*args, **kwargs):
        nonlocal created_job_config
        created_job_config = dict(kwargs.get("config") or {})
        return "fake-job-id"

    monkeypatch.setattr(repo, "_create_run", fake_create_run)
    monkeypatch.setattr(repo, "_create_job", fake_create_job)
    monkeypatch.setattr(repo, "is_queue_enabled", lambda: False)
    monkeypatch.setattr(repo, "_assert_social_account_profile_exists", lambda *a, **k: None)
    monkeypatch.setattr(repo, "get_active_social_account_posts_scrapling_run", lambda *a, **k: None)
    monkeypatch.setattr(repo, "dispatch_due_social_jobs", lambda **k: None)
    _build_lock_mocks(monkeypatch, locked=True)

    result = repo.start_tiktok_posts_scrapling_scrape(
        account_handle="someone",
        max_pages=2,
        initiated_by="test",
    )
    assert result["required_worker_lane"] == repo.TIKTOK_POSTS_SCRAPLING_WORKER_LANE
    assert result["platform"] == "tiktok"
    assert result["account_handle"] == "someone"
    assert created_job_config.get("required_worker_lane") == repo.TIKTOK_POSTS_SCRAPLING_WORKER_LANE
    assert created_job_config.get("stage") == repo.TIKTOK_POSTS_SCRAPLING_STAGE
    assert created_job_config.get("max_pages") == 2


def test_start_tiktok_posts_asserts_worker_available_when_queue_enabled(monkeypatch) -> None:
    """When queue is enabled, the helper must call assert_worker_available_when_queue_enabled
    with the TikTok posts lane."""
    calls: list[dict] = []

    def fake_assert(**kwargs):
        calls.append(kwargs)
        return {"healthy": True}

    monkeypatch.setattr(repo, "is_queue_enabled", lambda: True)
    monkeypatch.setattr(repo, "assert_worker_available_when_queue_enabled", fake_assert)
    monkeypatch.setattr(repo, "_create_run", lambda *a, **k: "run")
    monkeypatch.setattr(repo, "_create_job", lambda *a, **k: "job")
    monkeypatch.setattr(repo, "_assert_social_account_profile_exists", lambda *a, **k: None)
    monkeypatch.setattr(repo, "get_active_social_account_posts_scrapling_run", lambda *a, **k: None)
    monkeypatch.setattr(repo, "dispatch_due_social_jobs", lambda **k: None)
    _build_lock_mocks(monkeypatch, locked=True)

    repo.start_tiktok_posts_scrapling_scrape(
        account_handle="someone",
        initiated_by="test",
    )
    assert len(calls) == 1
    assert calls[0]["required_worker_lane"] == repo.TIKTOK_POSTS_SCRAPLING_WORKER_LANE
    assert calls[0]["platform"] == "tiktok"


def test_start_tiktok_posts_raises_conflict_when_lock_already_held(monkeypatch) -> None:
    """If pg_try_advisory_lock returns False, the helper must raise
    SocialIngestConflictError without creating a run or job."""
    create_run_called = False
    create_job_called = False

    def fake_create_run(*args, **kwargs):
        nonlocal create_run_called
        create_run_called = True
        return "should-not-happen"

    def fake_create_job(*args, **kwargs):
        nonlocal create_job_called
        create_job_called = True
        return "should-not-happen"

    monkeypatch.setattr(repo, "_create_run", fake_create_run)
    monkeypatch.setattr(repo, "_create_job", fake_create_job)
    monkeypatch.setattr(repo, "is_queue_enabled", lambda: False)
    monkeypatch.setattr(repo, "_assert_social_account_profile_exists", lambda *a, **k: None)
    monkeypatch.setattr(
        repo,
        "get_active_social_account_posts_scrapling_run",
        lambda *a, **k: {"run_id": "existing-tiktok-run", "status": "running"},
    )
    monkeypatch.setattr(repo, "dispatch_due_social_jobs", lambda **k: None)
    _build_lock_mocks(monkeypatch, locked=False)

    with pytest.raises(repo.SocialIngestConflictError) as exc_info:
        repo.start_tiktok_posts_scrapling_scrape(
            account_handle="someone",
            initiated_by="test",
        )

    # Conflict error attribute is `.code` (not `.error_code`)
    assert getattr(exc_info.value, "code", None) == "SOCIAL_POSTS_SCRAPLING_RUN_ALREADY_ACTIVE"
    assert getattr(exc_info.value, "detail", {}).get("run_id") == "existing-tiktok-run"
    assert create_run_called is False
    assert create_job_called is False


def test_start_tiktok_posts_marks_run_failed_when_job_create_errors(monkeypatch) -> None:
    set_run_status_calls: list[tuple[str, str]] = []

    monkeypatch.setattr(repo, "_create_run", lambda *a, **k: "failed-tiktok-run-id")
    monkeypatch.setattr(repo, "_create_job", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("job create failed")))
    monkeypatch.setattr(repo, "_set_run_status", lambda run_id, status: set_run_status_calls.append((run_id, status)))
    monkeypatch.setattr(repo, "is_queue_enabled", lambda: False)
    monkeypatch.setattr(repo, "_assert_social_account_profile_exists", lambda *a, **k: None)
    monkeypatch.setattr(repo, "get_active_social_account_posts_scrapling_run", lambda *a, **k: None)
    monkeypatch.setattr(repo, "dispatch_due_social_jobs", lambda **k: None)
    _build_lock_mocks(monkeypatch, locked=True)

    with pytest.raises(RuntimeError, match="job create failed"):
        repo.start_tiktok_posts_scrapling_scrape(account_handle="someone", initiated_by="test")

    assert set_run_status_calls == [("failed-tiktok-run-id", "failed")]
