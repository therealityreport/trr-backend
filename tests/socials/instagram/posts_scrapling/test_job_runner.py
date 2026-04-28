from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest


def test_job_runner_rejects_missing_account():
    from trr_backend.socials.instagram.posts_scrapling.job_runner import (
        PostsScraplingRuntimeError,
        run_instagram_posts_scrapling_job,
    )

    job = {"id": "job-1", "run_id": "run-1", "config": {"account": ""}}
    with pytest.raises(PostsScraplingRuntimeError, match="missing an account"):
        run_instagram_posts_scrapling_job(job)


def test_job_runner_emits_post_skip_truthfulness_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.posts_scrapling.job_runner import run_instagram_posts_scrapling_job
    from trr_backend.socials.instagram.posts_scrapling.persistence import PersistedInstagramPosts

    captured_finish: dict[str, object] = {}

    class _FakeFetcher:
        runtime_metadata = {"request_count": 1}

        def __init__(self, **_kwargs) -> None:
            pass

        async def warmup(self, _account_handle: str) -> None:
            return None

        async def fetch_posts_page(self, _account_handle: str, cursor=None):  # noqa: ANN001
            del cursor
            return SimpleNamespace(
                auth_failed=False,
                fetch_failed=False,
                retryable=False,
                fetch_reason=None,
                posts=[{"shortcode": "abc123"}],
                has_next_page=False,
                end_cursor=None,
            )

        async def aclose(self) -> None:
            return None

    monkeypatch.setattr(
        "trr_backend.socials.instagram.posts_scrapling.job_runner.resolve_posts_scrapling_session",
        lambda **_kwargs: SimpleNamespace(
            cookies={},
            browser_account_id="thetraitorsus",
            auth_session=SimpleNamespace(cookies={}, metadata={"source": "test"}),
        ),
    )
    monkeypatch.setattr(
        "trr_backend.socials.instagram.posts_scrapling.job_runner.select_posts_proxy",
        lambda: None,
    )
    monkeypatch.setattr(
        "trr_backend.socials.instagram.posts_scrapling.job_runner.InstagramPostsScraplingFetcher",
        _FakeFetcher,
    )
    monkeypatch.setattr(
        "trr_backend.socials.instagram.posts_scrapling.job_runner.persist_instagram_posts",
        lambda **_kwargs: PersistedInstagramPosts(
            posts_upserted=0,
            posts_skipped=1,
            posts_skipped_by_reason={"canonical_upsert_returned_none": 1},
        ),
    )
    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})
    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(repo, "_emit_job_progress", lambda **_kwargs: True)
    monkeypatch.setattr(repo, "_iso", lambda _value: "2026-04-21T00:00:00+00:00")
    monkeypatch.setattr(repo, "_now_utc", lambda: datetime(2026, 4, 28, tzinfo=UTC))
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *_args, **_kwargs: {})

    def _fake_finish_job(job_id, *, status, items_found, error_message=None, metadata=None, **_kwargs):  # noqa: ANN001
        captured_finish["job_id"] = job_id
        captured_finish["status"] = status
        captured_finish["items_found"] = items_found
        captured_finish["error_message"] = error_message
        captured_finish["metadata"] = metadata

    monkeypatch.setattr(repo, "_finish_job", _fake_finish_job)
    monkeypatch.setattr(
        "trr_backend.socials.instagram.posts_scrapling.job_runner.pg.fetch_one",
        lambda *_args, **_kwargs: {},
    )

    job = {
        "id": "job-1",
        "run_id": "run-1",
        "config": {"account": "thetraitorsus", "stage": "posts_scrapling"},
    }
    run_instagram_posts_scrapling_job(job, worker_id="worker-1")

    metadata = dict(captured_finish["metadata"] or {})
    assert metadata["persist_counters"] == {
        "posts_upserted": 0,
        "posts_skipped": 1,
        "posts_skipped_by_reason": {"canonical_upsert_returned_none": 1},
    }
    assert metadata["posts_scrapling_persist_diagnostics"] == {
        "posts_upserted": 0,
        "posts_skipped": 1,
        "posts_skipped_by_reason": {"canonical_upsert_returned_none": 1},
    }


def test_job_runner_preserves_warmup_error_runtime_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.posts_scrapling import job_runner as jr

    finish_calls: list[dict[str, Any]] = []
    fetch_calls: list[str] = []

    class _WarmupError(RuntimeError):
        def __init__(self) -> None:
            super().__init__("posts warmup did not bridge cookies")
            self.error_code = "instagram_posts_warmup_no_cookies"
            self.retryable = True

    class _FakeFetcher:
        @property
        def runtime_metadata(self) -> dict[str, Any]:
            return {
                "transport": "httpx_after_browser_warmup",
                "request_count": 1,
                "warmup_cookie_count": 0,
                "warmup_cookie_names": [],
            }

        async def warmup(self, _account_handle: str) -> None:
            raise _WarmupError()

        async def fetch_posts_page(self, account_handle: str, *, cursor: str | None = None) -> SimpleNamespace:
            del cursor
            fetch_calls.append(account_handle)
            return SimpleNamespace(posts=[], fetch_failed=False, auth_failed=False)

        async def aclose(self) -> None:
            return None

    monkeypatch.setattr(jr, "InstagramPostsWarmupError", _WarmupError)
    monkeypatch.setattr(
        jr,
        "resolve_posts_scrapling_session",
        lambda **_kwargs: SimpleNamespace(
            cookies={},
            browser_account_id="thetraitorsus",
            auth_session=SimpleNamespace(cookies={}, metadata={"source": "test"}),
        ),
    )
    monkeypatch.setattr(jr, "select_posts_proxy", lambda: None)
    monkeypatch.setattr(jr, "InstagramPostsScraplingFetcher", lambda **_kwargs: _FakeFetcher())
    monkeypatch.setattr(repo, "_finish_job", lambda job_id, **kwargs: finish_calls.append({"job_id": job_id, **kwargs}))
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})
    monkeypatch.setattr(repo, "_retry_backoff_seconds", lambda _attempt: 30)
    monkeypatch.setattr(repo, "_now_utc", lambda: datetime(2026, 4, 28, tzinfo=UTC))
    monkeypatch.setattr(jr.pg, "fetch_one", lambda *_args, **_kwargs: {"id": "job-1", "status": "retrying"})

    job = {
        "id": "job-1",
        "run_id": "run-1",
        "config": {"account": "thetraitorsus", "stage": "posts_scrapling"},
        "attempt_count": 1,
        "max_attempts": 2,
    }
    payload = jr.run_instagram_posts_scrapling_job(job, worker_id="worker-1")

    assert payload["status"] == "retrying"
    assert fetch_calls == []
    finish_kwargs = finish_calls[-1]
    assert finish_kwargs["status"] == "retrying"
    assert finish_kwargs["last_error_code"] == "instagram_posts_warmup_no_cookies"
    metadata = finish_kwargs["metadata"]
    assert metadata["runtime_metadata"]["warmup_cookie_count"] == 0
    assert metadata["fetcher_runtime"]["warmup_cookie_count"] == 0


def test_job_runner_cancels_before_fetching_next_posts_page(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.posts_scrapling import job_runner as jr

    finish_calls: list[dict[str, Any]] = []
    fetch_calls: list[str] = []

    class _FakeFetcher:
        runtime_metadata = {"transport": "test", "request_count": 1}

        async def warmup(self, _account_handle: str) -> None:
            return None

        async def fetch_posts_page(self, account_handle: str, *, cursor: str | None = None) -> SimpleNamespace:
            del cursor
            fetch_calls.append(account_handle)
            return SimpleNamespace(posts=[], fetch_failed=False, auth_failed=False)

        async def aclose(self) -> None:
            return None

    def fake_fetch_one(sql: str, _params: list[object] | None = None, **_kwargs: Any) -> dict[str, Any]:
        normalized = " ".join(sql.split()).lower()
        if "select status from social.scrape_jobs" in normalized:
            return {"status": "cancelled"}
        if "select status from social.scrape_runs" in normalized:
            return {"status": "running"}
        return {"id": "job-1", "status": "cancelled", "items_found": 0}

    monkeypatch.setattr(
        jr,
        "resolve_posts_scrapling_session",
        lambda **_kwargs: SimpleNamespace(
            cookies={},
            browser_account_id="thetraitorsus",
            auth_session=SimpleNamespace(cookies={}, metadata={"source": "test"}),
        ),
    )
    monkeypatch.setattr(jr, "select_posts_proxy", lambda: None)
    monkeypatch.setattr(jr, "InstagramPostsScraplingFetcher", lambda **_kwargs: _FakeFetcher())
    monkeypatch.setattr(jr.pg, "fetch_one", fake_fetch_one)
    monkeypatch.setattr(repo, "_finish_job", lambda job_id, **kwargs: finish_calls.append({"job_id": job_id, **kwargs}))
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})
    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(repo, "_emit_job_progress", lambda **_kwargs: True)
    monkeypatch.setattr(repo, "_iso", lambda _value: "2026-04-28T00:00:00+00:00")
    monkeypatch.setattr(repo, "_now_utc", lambda: None)

    payload = jr.run_instagram_posts_scrapling_job(
        {"id": "job-1", "run_id": "run-1", "config": {"account": "thetraitorsus"}},
        worker_id="worker-1",
    )

    assert payload["status"] == "cancelled"
    assert fetch_calls == []
    finish_kwargs = finish_calls[-1]
    assert finish_kwargs["status"] == "cancelled"
    assert finish_kwargs["items_found"] == 0
    assert finish_kwargs["metadata"]["cancel_scope"] == "job"


def test_job_runner_returns_degraded_completed_summary_when_final_read_saturates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.db import pg
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.posts_scrapling import job_runner as jr

    class _FakeFetcher:
        runtime_metadata = {"transport": "test", "request_count": 2}

        async def warmup(self, _account_handle: str) -> None:
            return None

        async def fetch_posts_page(self, _account_handle: str, *, cursor: str | None = None) -> SimpleNamespace:
            del cursor
            return SimpleNamespace(
                auth_failed=False,
                fetch_failed=False,
                retryable=False,
                fetch_reason=None,
                posts=[],
                has_next_page=False,
                end_cursor=None,
            )

        async def aclose(self) -> None:
            return None

    def fake_fetch_one(sql: str, _params: list[object] | None = None, **_kwargs: Any) -> dict[str, Any] | None:
        normalized = " ".join(sql.split()).lower()
        if "select status from social.scrape_jobs" in normalized:
            return {"status": "running"}
        if "select status from social.scrape_runs" in normalized:
            return {"status": "running"}
        raise pg.DatabaseServiceUnavailableError("pool exhausted", reason="session_pool_capacity")

    monkeypatch.setattr(
        jr,
        "resolve_posts_scrapling_session",
        lambda **_kwargs: SimpleNamespace(
            cookies={},
            browser_account_id="thetraitorsus",
            auth_session=SimpleNamespace(cookies={}, metadata={"source": "test"}),
        ),
    )
    monkeypatch.setattr(jr, "select_posts_proxy", lambda: None)
    monkeypatch.setattr(jr, "InstagramPostsScraplingFetcher", lambda **_kwargs: _FakeFetcher())
    monkeypatch.setattr(jr.pg, "fetch_one", fake_fetch_one)
    monkeypatch.setattr(repo, "_finish_job", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})
    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(repo, "_emit_job_progress", lambda **_kwargs: True)
    monkeypatch.setattr(repo, "_iso", lambda _value: "2026-04-28T00:00:00+00:00")
    monkeypatch.setattr(repo, "_now_utc", lambda: None)

    payload = jr.run_instagram_posts_scrapling_job(
        {"id": "job-1", "run_id": "run-1", "config": {"account": "thetraitorsus"}},
        worker_id="worker-1",
    )

    assert payload["status"] == "completed"
    assert payload["items_found"] == 0
    assert payload["metadata"] == {
        "degraded_summary": True,
        "database_service_unavailable": True,
    }
