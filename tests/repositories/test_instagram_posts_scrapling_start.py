"""Tests for start_instagram_posts_scrapling_scrape — creates real
scrape_runs + scrape_jobs rows with worker-lane enforcement baked in."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from trr_backend.repositories import social_season_analytics as repo


def test_start_instagram_posts_rejects_empty_account() -> None:
    # An empty handle is rejected before SocialIngestValidationError is reached:
    # _normalize_social_account_profile_handle raises a plain ValueError first.
    # SocialIngestValidationError is also a ValueError, so ValueError is the
    # tightest assertion we can make without touching production code.
    with pytest.raises(ValueError, match="handle"):
        repo.start_instagram_posts_scrapling_scrape(
            account_handle="",
            initiated_by="test",
        )


def test_start_instagram_posts_sets_required_worker_lane(monkeypatch) -> None:
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
    # Short-circuit the advisory lock so we don't need a real DB
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
        lambda cur, q, params=None: {"locked": True} if "advisory_lock" in q else {"unlocked": True},
    )
    monkeypatch.setattr(repo, "get_active_social_account_posts_scrapling_run", lambda *a, **k: None)
    monkeypatch.setattr(repo, "dispatch_due_social_jobs", lambda **k: None)

    result = repo.start_instagram_posts_scrapling_scrape(
        account_handle="bravotv",
        max_pages=1,
        fast_mode=True,
        initiated_by="test",
    )
    assert result["required_worker_lane"] == repo.INSTAGRAM_POSTS_SCRAPLING_WORKER_LANE
    assert result["platform"] == "instagram"
    assert result["account_handle"] == "bravotv"
    assert created_job_config.get("required_worker_lane") == repo.INSTAGRAM_POSTS_SCRAPLING_WORKER_LANE
    assert created_job_config.get("stage") == repo.INSTAGRAM_POSTS_SCRAPLING_STAGE
    assert created_job_config.get("max_pages") == 1
    assert created_job_config.get("fast_mode") is True


def test_start_instagram_posts_asserts_worker_available_when_queue_enabled(monkeypatch) -> None:
    """When queue is enabled, the helper must call assert_worker_available_when_queue_enabled
    with the posts lane — otherwise jobs enqueue without a worker to run them."""
    calls: list[dict] = []

    def fake_assert(**kwargs):
        calls.append(kwargs)
        return {"healthy": True}

    monkeypatch.setattr(repo, "is_queue_enabled", lambda: True)
    monkeypatch.setattr(repo, "assert_worker_available_when_queue_enabled", fake_assert)
    monkeypatch.setattr(repo, "_create_run", lambda *a, **k: "run")
    monkeypatch.setattr(repo, "_create_job", lambda *a, **k: "job")
    monkeypatch.setattr(repo, "_assert_social_account_profile_exists", lambda *a, **k: None)
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
        lambda cur, q, params=None: {"locked": True} if "advisory_lock" in q else {"unlocked": True},
    )
    monkeypatch.setattr(repo, "get_active_social_account_posts_scrapling_run", lambda *a, **k: None)
    monkeypatch.setattr(repo, "dispatch_due_social_jobs", lambda **k: None)

    repo.start_instagram_posts_scrapling_scrape(
        account_handle="bravotv",
        initiated_by="test",
    )
    assert len(calls) == 1
    assert calls[0]["required_worker_lane"] == repo.INSTAGRAM_POSTS_SCRAPLING_WORKER_LANE
    assert calls[0]["platform"] == "instagram"


def test_start_instagram_posts_raises_conflict_when_lock_already_held(monkeypatch) -> None:
    """If pg_try_advisory_lock returns False (another scrape is in flight for
    this account), the helper must raise SocialIngestConflictError without
    creating a run or job."""
    create_run_called = False
    create_job_called = False

    def fake_create_run(*args, **kwargs):
        nonlocal create_run_called
        create_run_called = True
        return "should-not-happen-run-id"

    def fake_create_job(*args, **kwargs):
        nonlocal create_job_called
        create_job_called = True
        return "should-not-happen-job-id"

    monkeypatch.setattr(repo, "_create_run", fake_create_run)
    monkeypatch.setattr(repo, "_create_job", fake_create_job)
    monkeypatch.setattr(repo, "is_queue_enabled", lambda: False)
    monkeypatch.setattr(repo, "_assert_social_account_profile_exists", lambda *a, **k: None)
    monkeypatch.setattr(
        repo,
        "get_active_social_account_posts_scrapling_run",
        lambda *a, **k: {"run_id": "existing", "status": "running"},
    )
    monkeypatch.setattr(repo, "dispatch_due_social_jobs", lambda **k: None)

    lock_conn_mock = MagicMock()
    cur_mock = MagicMock()
    cur_mock.__enter__ = lambda self: self
    cur_mock.__exit__ = lambda *a: None
    lock_conn_mock.__enter__ = lambda self: lock_conn_mock
    lock_conn_mock.__exit__ = lambda *a: None
    monkeypatch.setattr(repo.pg, "db_connection", lambda *a, **k: lock_conn_mock)
    monkeypatch.setattr(repo.pg, "db_cursor", lambda *a, **k: cur_mock)

    # CRITICAL: pg_try_advisory_lock returns {"locked": False} → contention
    monkeypatch.setattr(
        repo.pg,
        "fetch_one_with_cursor",
        lambda cur, q, params=None: {"locked": False} if "advisory_lock" in q else {"unlocked": True},
    )

    with pytest.raises(repo.SocialIngestConflictError) as exc_info:
        repo.start_instagram_posts_scrapling_scrape(
            account_handle="bravotv",
            initiated_by="test",
        )

    # Error code must be the documented one (exposed as .code on this exception class)
    assert getattr(exc_info.value, "code", None) == "SOCIAL_POSTS_SCRAPLING_RUN_ALREADY_ACTIVE"
    # Detail should carry the active-run info we mocked
    assert getattr(exc_info.value, "detail", {}).get("run_id") == "existing"
    # CRITICAL: no run/job rows should have been created
    assert create_run_called is False
    assert create_job_called is False
