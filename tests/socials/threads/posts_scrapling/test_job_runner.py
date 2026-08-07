from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any, cast

import pytest

from trr_backend.socials.threads.posts_scrapling.persistence import PersistedThreadsPosts

_ROLLOUT_ENV = "SOCIAL_THREADS_POSTS_SCRAPLING_ENABLED"


class _FakeLifecycle:
    def __init__(self) -> None:
        self.heartbeat_calls: list[dict[str, Any]] = []
        self.progress_calls: list[dict[str, Any]] = []
        self.finish_calls: list[dict[str, Any]] = []
        self.finalize_calls: list[str] = []
        self.finalize_error: Exception | None = None

    def new_job_progress_state(self) -> dict[str, Any]:
        return {}

    def touch_job_heartbeat(self, *args: Any, **kwargs: Any) -> bool:
        self.heartbeat_calls.append({"args": args, "kwargs": kwargs})
        return True

    def emit_job_progress(self, **kwargs: Any) -> bool:
        self.progress_calls.append(kwargs)
        return True

    def format_time(self, _value: datetime | None) -> str:
        return "2026-05-05T00:00:00+00:00"

    def now_utc(self) -> datetime:
        return datetime(2026, 5, 5, tzinfo=UTC)

    def retry_backoff_seconds(self, _attempt: int) -> int:
        return 30

    def finish_job(self, job_id: str, **kwargs: Any) -> None:
        self.finish_calls.append({"job_id": job_id, **kwargs})

    def finalize_run_status(self, run_id: str) -> dict[str, Any]:
        self.finalize_calls.append(run_id)
        if self.finalize_error is not None:
            raise self.finalize_error
        return {}


@pytest.fixture
def fake_lifecycle(monkeypatch: pytest.MonkeyPatch) -> _FakeLifecycle:
    from trr_backend.socials.threads.posts_scrapling import job_runner as jr

    fake = _FakeLifecycle()
    monkeypatch.setattr(jr, "lifecycle", fake)
    return fake


@pytest.fixture(autouse=True)
def default_threads_posts_scrapling_rollout_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(_ROLLOUT_ENV, raising=False)


def _thread_post(post_id: str = "th-1") -> Any:
    return SimpleNamespace(
        post_id=post_id,
        username="bravotv",
        text="hello",
        media_urls=[],
        thumbnail_url=None,
        likes=1,
        replies=0,
        reposts=0,
        quotes=0,
        views=10,
        posted_at=None,
        url=f"https://www.threads.com/@bravotv/post/{post_id}",
        to_dict=lambda: {"post_id": post_id},
    )


def _install_common_fakes(
    monkeypatch: pytest.MonkeyPatch,
    jr,
    *,
    fetcher: Any,
    persist_result: PersistedThreadsPosts | None = None,
) -> None:
    monkeypatch.setattr(
        jr,
        "resolve_threads_posts_session",
        lambda: SimpleNamespace(
            raw_cookies={"sessionid": "raw-session-secret", "csrftoken": "raw-csrf-secret"},
            cookies=[{"name": "sessionid", "value": "raw-session-secret", "domain": ".threads.com", "path": "/"}],
            cookie_source="canonical",
        ),
    )
    monkeypatch.setattr(jr, "select_threads_posts_proxy", lambda: None)
    monkeypatch.setattr(jr, "ThreadsPostsScraplingFetcher", lambda **_kwargs: fetcher)
    monkeypatch.setattr(jr.auth_cooldown, "get_active_cooldown", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(jr.auth_cooldown, "clear_cooldown", lambda *_args, **_kwargs: True)
    if persist_result is not None:
        monkeypatch.setattr(jr, "persist_threads_posts", lambda **_kwargs: persist_result)


def _running_status_or_final_row(query: str, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
    normalized = " ".join(str(query).split()).lower()
    if normalized.startswith("select status from social.scrape_jobs"):
        return {"status": "running"}
    if normalized.startswith("select status from social.scrape_runs"):
        return {"status": "running"}
    return {"id": "job-1", "status": "completed"}


def test_threads_job_runner_defaults_scrapling_rollout_enabled(
    monkeypatch: pytest.MonkeyPatch,
    fake_lifecycle: _FakeLifecycle,
) -> None:
    from trr_backend.socials.threads.posts_scrapling import job_runner as jr

    class _FakeFetcher:
        runtime_metadata = {"transport": "test", "request_count": 1, "complete": True}

        async def warmup(self, _account_handle: str) -> None:
            return None

        async def fetch_posts(self, _account_handle: str, *, max_pages: int | None = None) -> Any:
            del max_pages
            return jr.ThreadsPostsFetchResult(
                posts=[_thread_post()],
                fetch_failed=False,
                auth_failed=False,
                retryable=False,
                fetch_reason=None,
            )

        async def aclose(self) -> None:
            return None

    _install_common_fakes(
        monkeypatch,
        jr,
        fetcher=_FakeFetcher(),
        persist_result=PersistedThreadsPosts(posts_upserted=1, posts_skipped=0),
    )
    monkeypatch.setattr(jr.pg, "fetch_one", _running_status_or_final_row)

    jr.run_threads_posts_scrapling_job(
        {"id": "job-1", "run_id": "run-1", "config": {"account": "bravotv"}},
        worker_id="worker-1",
    )

    metadata = fake_lifecycle.finish_calls[-1]["metadata"]
    assert fake_lifecycle.finish_calls[-1]["status"] == "completed"
    assert metadata["threads_posts_scrapling_enabled"] is True
    assert metadata["rollout_flags"]["threads_posts_scrapling"] == {
        "env_var": _ROLLOUT_ENV,
        "enabled": True,
        "default_enabled": True,
        "configured_value": None,
    }


def test_threads_job_runner_blocks_on_active_auth_cooldown(
    monkeypatch: pytest.MonkeyPatch,
    fake_lifecycle: _FakeLifecycle,
) -> None:
    from trr_backend.socials.threads.posts_scrapling import job_runner as jr

    class _Cooldown:
        def to_metadata(self) -> dict[str, Any]:
            return {
                "platform": "threads",
                "account_handle": "bravotv",
                "last_error_code": "threads_login_prompt",
                "cooldown_until": "2026-05-05T00:05:00+00:00",
            }

    monkeypatch.setattr(jr.auth_cooldown, "get_active_cooldown", lambda *_args, **_kwargs: _Cooldown())
    monkeypatch.setattr(jr, "resolve_threads_posts_session", lambda: pytest.fail("session should not resolve"))
    monkeypatch.setattr(jr.pg, "fetch_one", _running_status_or_final_row)

    jr.run_threads_posts_scrapling_job(
        {"id": "job-1", "run_id": "run-1", "config": {"account": "bravotv"}},
        worker_id="worker-1",
    )

    finish = fake_lifecycle.finish_calls[-1]
    assert finish["status"] == "failed"
    assert finish["last_error_code"] == "threads_login_prompt"
    assert finish["metadata"]["runtime_metadata"]["error"]["auth_cooldown_active"] is True


def test_threads_job_runner_records_auth_cooldown_on_auth_failed_fetch(
    monkeypatch: pytest.MonkeyPatch,
    fake_lifecycle: _FakeLifecycle,
) -> None:
    from trr_backend.socials.threads.posts_scrapling import job_runner as jr

    class _FakeFetcher:
        runtime_metadata = {"transport": "test", "request_count": 1, "complete": False}

        async def warmup(self, _account_handle: str) -> None:
            return None

        async def fetch_posts(self, _account_handle: str, *, max_pages: int | None = None) -> Any:
            del max_pages
            return jr.ThreadsPostsFetchResult(
                posts=[],
                fetch_failed=True,
                auth_failed=True,
                retryable=True,
                fetch_reason="threads_redirect_to_login",
            )

        async def aclose(self) -> None:
            return None

    class _Cooldown:
        def to_metadata(self) -> dict[str, Any]:
            return {"platform": "threads", "account_handle": "bravotv", "last_error_code": "threads_redirect_to_login"}

    record_calls: list[tuple[str, str, str]] = []
    _install_common_fakes(monkeypatch, jr, fetcher=_FakeFetcher())
    monkeypatch.setattr(
        jr.auth_cooldown,
        "record_auth_block",
        lambda platform, account, code: record_calls.append((platform, account, code)) or _Cooldown(),
    )
    monkeypatch.setattr(jr.pg, "fetch_one", _running_status_or_final_row)

    jr.run_threads_posts_scrapling_job(
        {"id": "job-1", "run_id": "run-1", "attempt_count": 1, "max_attempts": 2, "config": {"account": "bravotv"}},
        worker_id="worker-1",
    )

    assert record_calls == [("threads", "bravotv", "threads_redirect_to_login")]
    finish = fake_lifecycle.finish_calls[-1]
    assert finish["status"] == "retrying"
    assert finish["last_error_code"] == "threads_redirect_to_login"
    assert finish["metadata"]["runtime_metadata"]["error"]["auth_cooldown_recorded"] is True


def test_threads_job_runner_fails_safely_when_scrapling_rollout_disabled(
    monkeypatch: pytest.MonkeyPatch,
    fake_lifecycle: _FakeLifecycle,
) -> None:
    from trr_backend.socials.threads.posts_scrapling import job_runner as jr

    monkeypatch.setenv(_ROLLOUT_ENV, "false")
    monkeypatch.setattr(
        jr,
        "resolve_threads_posts_session",
        lambda: pytest.fail("session should not resolve when Threads posts Scrapling is disabled"),
    )
    monkeypatch.setattr(
        jr,
        "ThreadsPostsScraplingFetcher",
        lambda **_kwargs: pytest.fail("Scrapling fetcher should not run when disabled"),
    )
    monkeypatch.setattr(
        jr.pg,
        "fetch_one",
        lambda *_args, **_kwargs: {
            "id": "job-1",
            "run_id": "run-1",
            "platform": "threads",
            "job_type": "posts",
            "status": "failed",
            "items_found": 0,
            "error_message": "Threads posts Scrapling job is disabled by SOCIAL_THREADS_POSTS_SCRAPLING_ENABLED.",
            "metadata": fake_lifecycle.finish_calls[-1]["metadata"],
        },
    )

    result = jr.run_threads_posts_scrapling_job(
        {"id": "job-1", "run_id": "run-1", "config": {"account": "bravotv"}},
        worker_id="worker-1",
    )

    finish = fake_lifecycle.finish_calls[-1]
    metadata = finish["metadata"]
    assert result["status"] == "failed"
    assert finish["status"] == "failed"
    assert finish["last_error_code"] == "threads_posts_scrapling_disabled"
    assert finish["last_error_class"] == "ThreadsPostsScraplingRuntimeError"
    assert metadata["threads_posts_scrapling_enabled"] is False
    assert metadata["rollout_flags"]["threads_posts_scrapling"] == {
        "env_var": _ROLLOUT_ENV,
        "enabled": False,
        "default_enabled": True,
        "configured_value": "false",
    }
    assert metadata["stop_reason"] == "threads_posts_scrapling_disabled"
    assert metadata["runtime_metadata"]["error"]["disabled_reason"] == "disabled_by_env"
    assert metadata["runtime_metadata"]["error"]["threads_posts_scrapling_enabled"] is False


def test_threads_job_runner_uses_final_fetcher_metadata_not_warmup_snapshot(
    monkeypatch: pytest.MonkeyPatch,
    fake_lifecycle: _FakeLifecycle,
) -> None:
    from trr_backend.socials.threads.posts_scrapling import job_runner as jr

    class _FakeFetcher:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            del args, kwargs
            self._request_count = 0

        @property
        def runtime_metadata(self) -> dict[str, Any]:
            return {
                "request_count": self._request_count,
                "transport": "httpx_after_browser_warmup",
                "fallback_chain": ["scrapling_warmup", "graphql_profile_posts"],
                "stop_reason": "complete",
                "retryable": False,
                "complete": True,
            }

        async def warmup(self, username: str) -> None:
            del username
            self._request_count = 1

        async def fetch_posts(self, username: str, *, max_pages: int | None = None) -> Any:
            del username, max_pages
            self._request_count = 4
            return jr.ThreadsPostsFetchResult(
                posts=[
                    cast(
                        Any,
                        SimpleNamespace(
                            post_id="th-1",
                            username="bravotv",
                            text="hello",
                            media_urls=[],
                            thumbnail_url=None,
                            likes=1,
                            replies=0,
                            reposts=0,
                            quotes=0,
                            views=10,
                            posted_at=None,
                            url="https://www.threads.com/@bravotv/post/abc",
                            to_dict=lambda: {"post_id": "th-1"},
                        ),
                    )
                ],
                fetch_failed=False,
                auth_failed=False,
                retryable=False,
                fetch_reason=None,
            )

        async def aclose(self) -> None:
            return None

    _install_common_fakes(
        monkeypatch,
        jr,
        fetcher=_FakeFetcher(),
        persist_result=PersistedThreadsPosts(posts_upserted=1, posts_skipped=0),
    )
    monkeypatch.setattr(jr.pg, "fetch_one", lambda *_args, **_kwargs: {"status": "completed"})

    job = {
        "id": "00000000-0000-0000-0000-000000000101",
        "run_id": "00000000-0000-0000-0000-000000000201",
        "platform": "threads",
        "config": {"stage": "threads_posts_scrapling", "account": "bravotv"},
        "metadata": {},
        "attempt_count": 1,
        "max_attempts": 1,
    }

    jr.run_threads_posts_scrapling_job(job, worker_id="test-worker")

    metadata = fake_lifecycle.finish_calls[-1]["metadata"]
    assert metadata["fetch_counters"]["request_count"] == 4
    assert metadata["fetcher_runtime"]["request_count"] == 4
    assert metadata["source_runtime"]["request_count"] == 4


def test_threads_job_runner_marks_cancelled_job(
    monkeypatch: pytest.MonkeyPatch,
    fake_lifecycle: _FakeLifecycle,
) -> None:
    from trr_backend.socials.threads.posts_scrapling import job_runner as jr

    class _FakeFetcher:
        runtime_metadata = {"transport": "test", "request_count": 1, "warmup_cookie_count": 0}

        async def warmup(self, _account_handle: str) -> None:
            return None

        async def fetch_posts(self, _account_handle: str, *, max_pages: int | None = None) -> Any:
            del max_pages
            raise AssertionError("fetch_posts should not run after cancellation")

        async def aclose(self) -> None:
            return None

    def _cancelled_status_or_final_row(query: str, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
        normalized = " ".join(str(query).split()).lower()
        if normalized.startswith("select status from social.scrape_jobs"):
            return {"status": "cancelled"}
        if normalized.startswith("select status from social.scrape_runs"):
            return {"status": "running"}
        return {"id": "job-1", "status": "cancelled"}

    _install_common_fakes(monkeypatch, jr, fetcher=_FakeFetcher())
    monkeypatch.setattr(jr.pg, "fetch_one", _cancelled_status_or_final_row)

    result = jr.run_threads_posts_scrapling_job(
        {"id": "job-1", "run_id": "run-1", "job_type": "posts", "config": {"account": "bravotv"}},
        worker_id="worker-1",
    )

    finish = fake_lifecycle.finish_calls[-1]
    metadata = finish["metadata"]
    assert result["status"] == "cancelled"
    assert finish["status"] == "cancelled"
    assert finish["last_error_code"] == "threads_posts_scrapling_cancelled"
    assert metadata["cancelled"] is True
    assert metadata["cancel_scope"] == "job"
    assert metadata["job_status_at_cancel"] == "cancelled"
    assert metadata["run_status_at_cancel"] == "running"
    assert metadata["activity"]["phase"] == "cancelled"
    assert "raw-session-secret" not in repr(metadata)
    assert "raw-csrf-secret" not in repr(metadata)


def test_threads_job_runner_retries_transient_threads_500_warmup_error(
    monkeypatch: pytest.MonkeyPatch,
    fake_lifecycle: _FakeLifecycle,
) -> None:
    from trr_backend.socials.threads.posts_scrapling import job_runner as jr

    class _FailingFetcher:
        runtime_metadata = {"transport": "warmup", "request_count": 4}

        async def warmup(self, _account_handle: str) -> None:
            raise RuntimeError(
                "HTTPSConnectionPool(host='www.threads.com', port=443): Max retries exceeded "
                "with url: /@thetraitorsus (Caused by ResponseError('too many 500 error responses'))"
            )

        async def fetch_posts(self, _account_handle: str, *, max_pages: int | None = None) -> Any:
            del max_pages
            raise AssertionError("fetch_posts should not run after warmup fails")

        async def aclose(self) -> None:
            return None

    _install_common_fakes(monkeypatch, jr, fetcher=_FailingFetcher())
    monkeypatch.setattr(jr.pg, "fetch_one", _running_status_or_final_row)

    jr.run_threads_posts_scrapling_job(
        {
            "id": "job-1",
            "run_id": "run-1",
            "attempt_count": 1,
            "max_attempts": 2,
            "config": {"account": "thetraitorsus"},
        },
        worker_id="worker-1",
    )

    finish = fake_lifecycle.finish_calls[-1]
    metadata = finish["metadata"]
    assert finish["status"] == "retrying"
    assert finish["last_error_code"] == "threads_posts_transient_transport_error"
    assert metadata["retryable"] is True
    assert metadata["error_class"] == "RuntimeError"


def test_threads_job_runner_marks_cancelled_run(
    monkeypatch: pytest.MonkeyPatch,
    fake_lifecycle: _FakeLifecycle,
) -> None:
    from trr_backend.socials.threads.posts_scrapling import job_runner as jr

    class _FakeFetcher:
        runtime_metadata = {"transport": "test", "request_count": 1}

        async def warmup(self, _account_handle: str) -> None:
            return None

        async def fetch_posts(self, _account_handle: str, *, max_pages: int | None = None) -> Any:
            del max_pages
            raise AssertionError("fetch_posts should not run after run cancellation")

        async def aclose(self) -> None:
            return None

    def _cancelled_run_status_or_final_row(query: str, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
        normalized = " ".join(str(query).split()).lower()
        if normalized.startswith("select status from social.scrape_jobs"):
            return {"status": "running"}
        if normalized.startswith("select status from social.scrape_runs"):
            return {"status": "cancelled"}
        return {"id": "job-1", "status": "cancelled"}

    _install_common_fakes(monkeypatch, jr, fetcher=_FakeFetcher())
    monkeypatch.setattr(jr.pg, "fetch_one", _cancelled_run_status_or_final_row)

    jr.run_threads_posts_scrapling_job(
        {"id": "job-1", "run_id": "run-1", "config": {"account": "bravotv"}},
        worker_id="worker-1",
    )

    finish = fake_lifecycle.finish_calls[-1]
    metadata = finish["metadata"]
    assert finish["status"] == "cancelled"
    assert finish["last_error_code"] == "threads_posts_scrapling_cancelled"
    assert metadata["cancel_scope"] == "run"
    assert metadata["job_status_at_cancel"] == "running"
    assert metadata["run_status_at_cancel"] == "cancelled"


def test_threads_job_runner_checks_cancellation_after_fetch(
    monkeypatch: pytest.MonkeyPatch,
    fake_lifecycle: _FakeLifecycle,
) -> None:
    from trr_backend.socials.threads.posts_scrapling import job_runner as jr

    class _FakeFetcher:
        runtime_metadata = {"transport": "test", "request_count": 2}

        async def warmup(self, _account_handle: str) -> None:
            return None

        async def fetch_posts(self, _account_handle: str, *, max_pages: int | None = None) -> Any:
            del max_pages
            return jr.ThreadsPostsFetchResult(
                posts=[_thread_post()],
                fetch_failed=False,
                auth_failed=False,
                retryable=False,
                fetch_reason=None,
            )

        async def aclose(self) -> None:
            return None

    checks = 0

    def _cancel_after_fetch(query: str, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
        nonlocal checks
        normalized = " ".join(str(query).split()).lower()
        if normalized.startswith("select status from social.scrape_jobs"):
            checks += 1
            return {"status": "cancelled" if checks >= 3 else "running"}
        if normalized.startswith("select status from social.scrape_runs"):
            return {"status": "running"}
        return {"id": "job-1", "status": "cancelled"}

    persist_calls: list[dict[str, Any]] = []
    _install_common_fakes(monkeypatch, jr, fetcher=_FakeFetcher())
    monkeypatch.setattr(jr, "persist_threads_posts", lambda **kwargs: persist_calls.append(kwargs))
    monkeypatch.setattr(jr.pg, "fetch_one", _cancel_after_fetch)

    jr.run_threads_posts_scrapling_job(
        {"id": "job-1", "run_id": "run-1", "config": {"account": "bravotv"}},
        worker_id="worker-1",
    )

    assert persist_calls == []
    finish = fake_lifecycle.finish_calls[-1]
    assert finish["status"] == "cancelled"
    assert finish["metadata"]["stage_counters"]["posts"] == 0


def test_threads_job_runner_returns_degraded_summary_when_final_read_saturated(
    monkeypatch: pytest.MonkeyPatch,
    fake_lifecycle: _FakeLifecycle,
) -> None:
    from trr_backend.socials.threads.posts_scrapling import job_runner as jr

    class _FakeFetcher:
        runtime_metadata = {"transport": "test", "request_count": 2, "complete": True}

        async def warmup(self, _account_handle: str) -> None:
            return None

        async def fetch_posts(self, _account_handle: str, *, max_pages: int | None = None) -> Any:
            del max_pages
            return jr.ThreadsPostsFetchResult(
                posts=[_thread_post()],
                fetch_failed=False,
                auth_failed=False,
                retryable=False,
                fetch_reason=None,
            )

        async def aclose(self) -> None:
            return None

    def _status_or_db_error(query: str, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
        normalized = " ".join(str(query).split()).lower()
        if normalized.startswith("select status from social.scrape_jobs"):
            return {"status": "running"}
        if normalized.startswith("select status from social.scrape_runs"):
            return {"status": "running"}
        raise jr.pg.DatabaseServiceUnavailableError("db saturated", reason="session_pool_capacity")

    _install_common_fakes(
        monkeypatch,
        jr,
        fetcher=_FakeFetcher(),
        persist_result=PersistedThreadsPosts(posts_upserted=1, posts_skipped=0),
    )
    monkeypatch.setattr(jr.pg, "fetch_one", _status_or_db_error)

    result = jr.run_threads_posts_scrapling_job(
        {"id": "job-1", "run_id": "run-1", "job_type": "posts", "config": {"account": "bravotv"}},
        worker_id="worker-1",
    )

    assert result["id"] == "job-1"
    assert result["status"] == "completed"
    assert result["items_found"] == 1
    assert result["metadata"]["degraded_summary"] is True
    assert result["metadata"]["database_service_unavailable"] is True
    assert result["metadata"]["stage_counters"] == {"posts": 1, "pages": 1}
    assert result["metadata"]["persist_counters"]["posts_upserted"] == 1
    assert "raw-session-secret" not in repr(result["metadata"])
    assert "raw-csrf-secret" not in repr(result["metadata"])


def test_threads_job_runner_defers_run_finalization_when_db_saturated(
    monkeypatch: pytest.MonkeyPatch,
    fake_lifecycle: _FakeLifecycle,
) -> None:
    from trr_backend.socials.threads.posts_scrapling import job_runner as jr

    class _FakeFetcher:
        runtime_metadata = {"transport": "test", "request_count": 2}

        async def warmup(self, _account_handle: str) -> None:
            return None

        async def fetch_posts(self, _account_handle: str, *, max_pages: int | None = None) -> Any:
            del max_pages
            return jr.ThreadsPostsFetchResult(
                posts=[_thread_post()],
                fetch_failed=False,
                auth_failed=False,
                retryable=False,
                fetch_reason=None,
            )

        async def aclose(self) -> None:
            return None

    _install_common_fakes(
        monkeypatch,
        jr,
        fetcher=_FakeFetcher(),
        persist_result=PersistedThreadsPosts(posts_upserted=1, posts_skipped=0),
    )
    fake_lifecycle.finalize_error = jr.pg.DatabaseServiceUnavailableError("db saturated")
    monkeypatch.setattr(jr.pg, "fetch_one", _running_status_or_final_row)

    result = jr.run_threads_posts_scrapling_job(
        {"id": "job-1", "run_id": "run-1", "config": {"account": "bravotv"}},
        worker_id="worker-1",
    )

    assert fake_lifecycle.finalize_calls == ["run-1"]
    assert result["status"] == "completed"


def test_threads_job_runner_records_terminal_runtime_metadata(
    monkeypatch: pytest.MonkeyPatch,
    fake_lifecycle: _FakeLifecycle,
) -> None:
    from trr_backend.socials.threads.posts_scrapling import job_runner as jr

    class _FakeFetcher:
        runtime_metadata = {
            "transport": "graphql_profile_posts",
            "fallback_chain": ["scrapling_warmup", "graphql_profile_posts"],
            "stop_reason": "complete",
            "request_count": 3,
            "selected_proxy_fingerprint": "proxy.example:7000:decodo",
            "raw_cookies": {"sessionid": "raw-session-secret"},
        }

        async def warmup(self, _account_handle: str) -> None:
            return None

        async def fetch_posts(self, _account_handle: str, *, max_pages: int | None = None) -> Any:
            del max_pages
            return jr.ThreadsPostsFetchResult(
                posts=[_thread_post()],
                fetch_failed=False,
                auth_failed=False,
                retryable=False,
                fetch_reason=None,
            )

        async def aclose(self) -> None:
            return None

    _install_common_fakes(
        monkeypatch,
        jr,
        fetcher=_FakeFetcher(),
        persist_result=PersistedThreadsPosts(
            posts_upserted=1,
            posts_skipped=2,
            posts_skipped_by_reason={"missing_post_id": 1, "upsert_failed": 1},
        ),
    )
    monkeypatch.setattr(jr.pg, "fetch_one", _running_status_or_final_row)

    jr.run_threads_posts_scrapling_job(
        {"id": "job-1", "run_id": "run-1", "config": {"account": "bravotv", "fast_mode": True}},
        worker_id="worker-1",
    )

    finish = fake_lifecycle.finish_calls[-1]
    metadata = finish["metadata"]
    assert finish["status"] == "completed"
    assert metadata["progress"]["scraped_posts"] == 1
    assert metadata["stage_counters"] == {"posts": 1, "pages": 1}
    assert metadata["persist_counters"] == {
        "posts_upserted": 1,
        "materialized_posts_upserted": 1,
        "catalog_posts_upserted": 0,
        "posts_skipped": 2,
        "posts_skipped_by_reason": {"missing_post_id": 1, "upsert_failed": 1},
        "required_shared_persistence_failures": 0,
    }
    assert metadata["threads_posts_scrapling_persist_diagnostics"] == metadata["persist_counters"]
    assert metadata["fetcher_state"]["transport"] == "graphql_profile_posts"
    assert metadata["fetcher_state"]["selected_proxy_fingerprint"] == "proxy.example:7000:decodo"
    assert metadata["persistence_state"]["state"] == "completed"
    assert metadata["stop_reason"] == "completed"
    assert metadata["activity"]["phase"] == "threads_posts_scrapling_end"
    assert metadata["runtime_metadata"]["listing_progress"]["posts_seen"] == 1
    assert "raw-session-secret" not in repr(metadata)


def test_threads_job_runner_retries_after_persisting_incomplete_fetch(
    monkeypatch: pytest.MonkeyPatch,
    fake_lifecycle: _FakeLifecycle,
) -> None:
    from trr_backend.socials.threads.posts_scrapling import job_runner as jr

    class _FakeFetcher:
        runtime_metadata = {
            "transport": "graphql_profile_posts",
            "request_count": 4,
            "pages_fetched": 3,
            "complete": False,
            "retryable": True,
            "stop_reason": "page_fetch_failed",
        }

        async def warmup(self, _account_handle: str) -> None:
            return None

        async def fetch_posts(self, _account_handle: str, *, max_pages: int | None = None) -> Any:
            del max_pages
            return jr.ThreadsPostsFetchResult(
                posts=[_thread_post()],
                fetch_failed=False,
                auth_failed=False,
                retryable=True,
                fetch_reason="threads_graphql_page_fetch_failed",
            )

        async def aclose(self) -> None:
            return None

    _install_common_fakes(
        monkeypatch,
        jr,
        fetcher=_FakeFetcher(),
        persist_result=PersistedThreadsPosts(posts_upserted=1, posts_skipped=0),
    )
    monkeypatch.setattr(jr.pg, "fetch_one", _running_status_or_final_row)

    jr.run_threads_posts_scrapling_job(
        {
            "id": "job-1",
            "run_id": "run-1",
            "attempt_count": 1,
            "max_attempts": 2,
            "config": {"account": "bravotv"},
        },
        worker_id="worker-1",
    )

    finish = fake_lifecycle.finish_calls[-1]
    assert finish["status"] == "retrying"
    assert finish["items_found"] == 1
    assert finish["last_error_code"] == "threads_graphql_page_fetch_failed"
    assert finish["metadata"]["stage_counters"] == {"posts": 1, "pages": 3}


def test_threads_job_runner_honors_legacy_retryable_metadata(
    monkeypatch: pytest.MonkeyPatch,
    fake_lifecycle: _FakeLifecycle,
) -> None:
    from trr_backend.socials.threads.posts_scrapling import job_runner as jr

    class _FakeFetcher:
        runtime_metadata = {"transport": "graphql_profile_posts", "request_count": 1, "complete": False}

        async def warmup(self, _account_handle: str) -> None:
            return None

        async def fetch_posts(self, _account_handle: str, *, max_pages: int | None = None) -> Any:
            del max_pages
            return jr.ThreadsPostsFetchResult(
                posts=[],
                fetch_failed=True,
                retryable=True,
                fetch_reason="threads_graphql_page_fetch_failed",
            )

        async def aclose(self) -> None:
            return None

    class _FakeLegacyScraper:
        runtime_metadata = {
            "request_count": 2,
            "complete": False,
            "retryable": True,
            "stop_reason": "legacy_page_fetch_failed",
        }
        last_retrieval_meta = {"pages_scanned": 2}

        def scrape(self, _config: Any) -> list[Any]:
            return [_thread_post()]

    _install_common_fakes(
        monkeypatch,
        jr,
        fetcher=_FakeFetcher(),
        persist_result=PersistedThreadsPosts(posts_upserted=1, posts_skipped=0),
    )
    monkeypatch.setattr(
        "trr_backend.socials.threads.scraper.ThreadsScraper",
        lambda **_kwargs: _FakeLegacyScraper(),
    )
    monkeypatch.delattr("trr_backend.socials.threads.ThreadsScraper")
    monkeypatch.setattr(jr.pg, "fetch_one", _running_status_or_final_row)

    jr.run_threads_posts_scrapling_job(
        {
            "id": "job-1",
            "run_id": "run-1",
            "attempt_count": 1,
            "max_attempts": 2,
            "config": {"account": "bravotv"},
        },
        worker_id="worker-1",
    )

    finish = fake_lifecycle.finish_calls[-1]
    assert finish["status"] == "retrying"
    error_metadata = finish["metadata"]["runtime_metadata"]["error"]
    assert error_metadata["retryable"] is True
    assert error_metadata["fetch_incomplete"] is True


def test_threads_job_runner_retries_incomplete_shared_catalog_persistence(
    monkeypatch: pytest.MonkeyPatch,
    fake_lifecycle: _FakeLifecycle,
) -> None:
    from trr_backend.socials.threads.posts_scrapling import job_runner as jr

    class _FakeFetcher:
        runtime_metadata = {"transport": "test", "request_count": 1, "complete": True, "pages_fetched": 1}

        async def warmup(self, _account_handle: str) -> None:
            return None

        async def fetch_posts(self, _account_handle: str, *, max_pages: int | None = None) -> Any:
            del max_pages
            return jr.ThreadsPostsFetchResult(posts=[_thread_post()])

        async def aclose(self) -> None:
            return None

    _install_common_fakes(
        monkeypatch,
        jr,
        fetcher=_FakeFetcher(),
        persist_result=PersistedThreadsPosts(
            posts_upserted=1,
            catalog_posts_upserted=1,
            required_shared_persistence_failures=1,
            posts_skipped=1,
            posts_skipped_by_reason={"upsert_failed": 1},
        ),
    )
    monkeypatch.setattr(jr.pg, "fetch_one", _running_status_or_final_row)

    jr.run_threads_posts_scrapling_job(
        {
            "id": "job-1",
            "run_id": "run-1",
            "attempt_count": 1,
            "max_attempts": 2,
            "config": {"account": "bravotv", "pipeline_ingest_mode": "shared_account_catalog_backfill"},
        },
        worker_id="worker-1",
    )

    finish = fake_lifecycle.finish_calls[-1]
    assert finish["status"] == "retrying"
    assert finish["last_error_code"] == "threads_shared_catalog_persistence_incomplete"
    assert finish["metadata"]["persist_counters"]["posts_upserted"] == 1
    assert finish["metadata"]["persist_counters"]["required_shared_persistence_failures"] == 1
