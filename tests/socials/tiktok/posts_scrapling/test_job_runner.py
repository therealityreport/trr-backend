from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest

from trr_backend.socials.tiktok.posts_scrapling.persistence import PersistedTikTokPosts


class _FakeLifecycle:
    def __init__(self) -> None:
        self.heartbeat_calls: list[dict[str, Any]] = []
        self.progress_calls: list[dict[str, Any]] = []
        self.finish_calls: list[dict[str, Any]] = []
        self.finalize_calls: list[str] = []
        self.finalize_error: Exception | None = None

    def new_job_progress_state(self) -> dict[str, Any]:
        return {}

    def touch_job_heartbeat(self, *args, **kwargs) -> bool:
        self.heartbeat_calls.append({"args": args, "kwargs": kwargs})
        return True

    def emit_job_progress(self, **kwargs) -> bool:
        self.progress_calls.append(kwargs)
        return True

    def format_time(self, _value) -> str:
        return "2026-05-05T00:00:00+00:00"

    def now_utc(self) -> datetime:
        return datetime(2026, 5, 5, tzinfo=UTC)

    def retry_backoff_seconds(self, _attempt: int) -> int:
        return 30

    def finish_job(self, job_id: str, **kwargs) -> None:
        self.finish_calls.append({"job_id": job_id, **kwargs})

    def finalize_run_status(self, run_id: str) -> dict[str, Any]:
        self.finalize_calls.append(run_id)
        if self.finalize_error is not None:
            raise self.finalize_error
        return {}


@pytest.fixture
def fake_lifecycle(monkeypatch: pytest.MonkeyPatch) -> _FakeLifecycle:
    from trr_backend.socials.tiktok.posts_scrapling import job_runner as jr

    fake = _FakeLifecycle()
    monkeypatch.setattr(jr, "lifecycle", fake)
    return fake


def _install_common_fakes(
    monkeypatch: pytest.MonkeyPatch,
    jr,
    *,
    fetcher,
    persist_result: PersistedTikTokPosts | None = None,
) -> None:
    monkeypatch.setattr(
        jr,
        "resolve_tiktok_posts_session",
        lambda: SimpleNamespace(cookies={}, raw_cookies={"sessionid": "raw-session-secret"}),
    )
    monkeypatch.setattr(jr, "select_tiktok_posts_proxy", lambda: None)
    monkeypatch.setattr(jr, "TikTokPostsScraplingFetcher", lambda **_kwargs: fetcher)
    if persist_result is not None:
        monkeypatch.setattr(jr, "persist_tiktok_posts", lambda **_kwargs: persist_result)


def _running_status_or_final_row(query: str, *_args, **_kwargs) -> dict[str, Any]:
    normalized = " ".join(str(query).split()).lower()
    if normalized.startswith("select status from social.scrape_jobs"):
        return {"status": "running"}
    if normalized.startswith("select status from social.scrape_runs"):
        return {"status": "running"}
    return {"id": "job-1", "status": "completed"}


def test_tiktok_job_runner_rejects_missing_account():
    from trr_backend.socials.tiktok.posts_scrapling.job_runner import (
        TikTokPostsScraplingRuntimeError,
        run_tiktok_posts_scrapling_job,
    )

    job = {"id": "job-1", "run_id": "run-1", "config": {"account": ""}}
    with pytest.raises(TikTokPostsScraplingRuntimeError, match="missing an account"):
        run_tiktok_posts_scrapling_job(job)


def test_canonical_fallback_timeout_is_unbounded_unless_configured(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.socials.tiktok.posts_scrapling import job_runner as jr

    monkeypatch.delenv("SOCIAL_TIKTOK_POSTS_CANONICAL_FALLBACK_TIMEOUT_SECONDS", raising=False)
    assert jr._resolve_canonical_fallback_timeout_seconds() == 0.0  # noqa: SLF001

    monkeypatch.setenv("SOCIAL_TIKTOK_POSTS_CANONICAL_FALLBACK_TIMEOUT_SECONDS", "1200")
    assert jr._resolve_canonical_fallback_timeout_seconds() == 1200.0  # noqa: SLF001


def test_canonical_fallback_uses_ytdlp_without_api_fallthrough(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.socials.tiktok.posts_scrapling import job_runner as jr

    class _FakeScraper:
        def __init__(self, *, cookies: dict[str, Any]) -> None:
            assert cookies == {"sessionid": "raw-session-secret"}
            self.last_retrieval_meta = {}

        def scrape(self, _config: Any) -> list[Any]:
            raise AssertionError("canonical fallback must not call scrape()")

        def _scrape_via_ytdlp(self, config: Any) -> list[Any]:
            assert config.username == "bravotv"
            self.last_retrieval_meta = {
                "retrieval_mode": "ytdlp",
                "fallback_chain": ["yt_dlp"],
                "ytdlp_posts_found": 1,
            }
            return [SimpleNamespace(video_id="tiktok-1")]

    from trr_backend.socials.tiktok import scraper as scraper_module

    monkeypatch.setattr(scraper_module, "TikTokScraper", _FakeScraper)

    posts, metadata = jr._scrape_canonical_tiktok_fallback_posts(  # noqa: SLF001
        account_handle="bravotv",
        cookies={"sessionid": "raw-session-secret"},
        max_pages=None,
        trigger_reason="non_json_response",
    )

    assert len(posts) == 1
    assert metadata["retrieval_mode"] == "ytdlp"
    assert metadata["fallback_chain"] == ["yt_dlp"]
    assert metadata["used"] is True
    assert metadata["trigger_reason"] == "non_json_response"


def test_tiktok_job_runner_records_progress_and_completion_metadata(
    monkeypatch: pytest.MonkeyPatch,
    fake_lifecycle: _FakeLifecycle,
) -> None:
    from trr_backend.socials.tiktok.posts_scrapling import job_runner as jr

    class _FakeFetcher:
        runtime_metadata = {
            "transport": "test",
            "request_count": 2,
            "warmup_cookie_names": ["sessionid"],
            "warmup_cookie_count": 1,
        }

        async def warmup(self, _account_handle: str) -> None:
            return None

        async def resolve_sec_uid(self, _account_handle: str) -> str:
            return "SEC_UID"

        async def fetch_posts_page(self, *, sec_uid: str, cursor: str | None = None) -> SimpleNamespace:
            assert sec_uid == "SEC_UID"
            assert cursor is None
            return SimpleNamespace(
                auth_failed=False,
                fetch_failed=False,
                retryable=False,
                fetch_reason=None,
                posts=[{"id": "tiktok-1"}],
                has_more=False,
                cursor=None,
            )

        async def aclose(self) -> None:
            return None

    _install_common_fakes(
        monkeypatch,
        jr,
        fetcher=_FakeFetcher(),
        persist_result=PersistedTikTokPosts(posts_upserted=1, posts_skipped=0),
    )
    monkeypatch.setattr(jr.pg, "fetch_one", _running_status_or_final_row)

    jr.run_tiktok_posts_scrapling_job(
        {"id": "job-1", "run_id": "run-1", "config": {"account": "bravotv"}},
        worker_id="worker-1",
    )

    progress = fake_lifecycle.progress_calls[-1]
    assert progress["stage"] == "tiktok_posts_scrapling"
    assert progress["activity"]["phase"] == "tiktok_posts_scrapling_running"
    assert progress["activity"]["pages_fetched"] == 1
    assert progress["activity"]["listing_progress"]["posts_seen"] == 1
    assert progress["scraped_posts"] == 1
    assert progress["posts_upserted"] == 1

    finish = fake_lifecycle.finish_calls[-1]
    metadata = finish["metadata"]
    assert finish["status"] == "completed"
    assert finish["items_found"] == 1
    assert metadata["stage_counters"] == {"posts": 1, "pages": 1}
    assert metadata["persist_counters"] == {
        "posts_upserted": 1,
        "catalog_posts_upserted": 0,
        "posts_skipped": 0,
        "posts_skipped_by_reason": {},
    }
    assert metadata["listing_progress"]["stop_reason"] == "completed"
    assert metadata["stop_reason"] == "completed"
    assert metadata["activity"] == {
        "phase": "tiktok_posts_scrapling_end",
        "last_progress_at": "2026-05-05T00:00:00+00:00",
    }
    assert {"stage_counters", "persist_counters", "listing_progress", "runtime_metadata", "fetcher_runtime"}.issubset(
        metadata
    )
    assert "raw-session-secret" not in repr(metadata)


def test_tiktok_job_runner_records_retryable_error_metadata(
    monkeypatch: pytest.MonkeyPatch,
    fake_lifecycle: _FakeLifecycle,
) -> None:
    from trr_backend.socials.tiktok.posts_scrapling import job_runner as jr

    class _FakeFetcher:
        runtime_metadata = {"transport": "test", "request_count": 1}

        async def warmup(self, _account_handle: str) -> None:
            return None

        async def resolve_sec_uid(self, _account_handle: str) -> str:
            return "SEC_UID"

        async def fetch_posts_page(self, *, sec_uid: str, cursor: str | None = None) -> SimpleNamespace:
            del sec_uid, cursor
            return SimpleNamespace(
                auth_failed=False,
                fetch_failed=True,
                retryable=True,
                fetch_reason="http_429",
                posts=[],
                has_more=False,
                cursor=None,
            )

        async def aclose(self) -> None:
            return None

    _install_common_fakes(monkeypatch, jr, fetcher=_FakeFetcher())
    monkeypatch.setattr(jr.pg, "fetch_one", _running_status_or_final_row)

    jr.run_tiktok_posts_scrapling_job(
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
    metadata = finish["metadata"]
    assert finish["status"] == "retrying"
    assert finish["last_error_code"] == "http_429"
    assert finish["next_available_at"] == datetime(2026, 5, 5, 0, 0, 30, tzinfo=UTC)
    assert metadata["error_code"] == "http_429"
    assert metadata["runtime_metadata"]["fetch_reason"] == "http_429"
    assert metadata["persist_counters"] == {
        "posts_upserted": 0,
        "catalog_posts_upserted": 0,
        "posts_skipped": 0,
        "posts_skipped_by_reason": {},
    }
    assert metadata["listing_progress"]["partial"] is True
    assert metadata["stop_reason"] == "http_429"
    assert metadata["activity"] == {
        "phase": "failed",
        "last_progress_at": "2026-05-05T00:00:00+00:00",
    }


def test_tiktok_job_runner_shared_mode_passes_catalog_mode_to_persistence(
    monkeypatch: pytest.MonkeyPatch,
    fake_lifecycle: _FakeLifecycle,
) -> None:
    from trr_backend.socials.tiktok.posts_scrapling import job_runner as jr

    class _FakeFetcher:
        runtime_metadata = {"transport": "test", "request_count": 2}

        async def warmup(self, _account_handle: str) -> None:
            return None

        async def resolve_sec_uid(self, _account_handle: str) -> str:
            return "SEC_UID"

        async def fetch_posts_page(self, *, sec_uid: str, cursor: str | None = None) -> SimpleNamespace:
            del sec_uid, cursor
            return SimpleNamespace(
                auth_failed=False,
                fetch_failed=False,
                retryable=False,
                fetch_reason=None,
                posts=[{"id": "tiktok-1"}],
                has_more=False,
                cursor=None,
            )

        async def aclose(self) -> None:
            return None

    persist_calls: list[dict[str, Any]] = []

    def _fake_persist(**kwargs: Any) -> PersistedTikTokPosts:
        persist_calls.append(kwargs)
        return PersistedTikTokPosts(posts_upserted=1, catalog_posts_upserted=1, posts_skipped=0)

    _install_common_fakes(monkeypatch, jr, fetcher=_FakeFetcher())
    monkeypatch.setattr(jr, "persist_tiktok_posts", _fake_persist)
    monkeypatch.setattr(jr.pg, "fetch_one", _running_status_or_final_row)

    jr.run_tiktok_posts_scrapling_job(
        {
            "id": "job-1",
            "run_id": "run-1",
            "config": {
                "stage": "tiktok_posts_scrapling",
                "account": "bravotv",
                "pipeline_ingest_mode": "shared_account_catalog_backfill",
            },
        },
        worker_id="worker-1",
    )

    assert persist_calls[0]["pipeline_ingest_mode"] == "shared_account_catalog_backfill"
    assert persist_calls[0]["run_id"] == "run-1"
    finish = fake_lifecycle.finish_calls[-1]
    metadata = finish["metadata"]
    assert finish["status"] == "completed"
    assert metadata["stage"] == "tiktok_posts_scrapling"
    assert metadata["pipeline_ingest_mode"] == "shared_account_catalog_backfill"
    assert metadata["persist_counters"]["posts_upserted"] == 1
    assert metadata["persist_counters"]["catalog_posts_upserted"] == 1


def test_tiktok_job_runner_recovers_non_json_posts_with_canonical_fallback(
    monkeypatch: pytest.MonkeyPatch,
    fake_lifecycle: _FakeLifecycle,
) -> None:
    from trr_backend.socials.tiktok.posts_scrapling import job_runner as jr

    class _FakeFetcher:
        runtime_metadata = {
            "transport": "test",
            "request_count": 3,
            "sec_uid_source": "warmup_html",
        }

        async def warmup(self, _account_handle: str) -> None:
            return None

        async def resolve_sec_uid(self, _account_handle: str) -> str:
            return "SEC_UID"

        async def fetch_posts_page(self, *, sec_uid: str, cursor: str | None = None) -> SimpleNamespace:
            del sec_uid, cursor
            return SimpleNamespace(
                auth_failed=False,
                fetch_failed=True,
                retryable=False,
                fetch_reason="non_json_response",
                posts=[],
                has_more=False,
                cursor=None,
            )

        async def aclose(self) -> None:
            return None

    fallback_posts = [SimpleNamespace(video_id="fallback-video-1")]
    fallback_calls: list[dict[str, Any]] = []

    def _fake_fallback(**kwargs):
        fallback_calls.append(kwargs)
        return fallback_posts, {
            "used": True,
            "retrieval_mode": "ytdlp",
            "trigger_reason": kwargs["trigger_reason"],
            "posts_found": len(fallback_posts),
        }

    _install_common_fakes(monkeypatch, jr, fetcher=_FakeFetcher())
    monkeypatch.setattr(jr, "_scrape_canonical_tiktok_fallback_posts", _fake_fallback)
    monkeypatch.setattr(
        jr,
        "persist_tiktok_post_dtos",
        lambda **_kwargs: PersistedTikTokPosts(posts_upserted=1, posts_skipped=0),
    )
    monkeypatch.setattr(jr.pg, "fetch_one", _running_status_or_final_row)

    jr.run_tiktok_posts_scrapling_job(
        {"id": "job-1", "run_id": "run-1", "config": {"account": "bravotv", "max_pages": 1}},
        worker_id="worker-1",
    )

    assert fallback_calls
    assert fallback_calls[0]["account_handle"] == "bravotv"
    assert fallback_calls[0]["trigger_reason"] == "non_json_response"

    progress = fake_lifecycle.progress_calls[-1]
    assert progress["activity"]["phase"] == "tiktok_posts_canonical_fallback_completed"
    assert progress["activity"]["canonical_fallback"] == {
        "trigger_reason": "non_json_response",
        "posts_found": 1,
    }

    finish = fake_lifecycle.finish_calls[-1]
    metadata = finish["metadata"]
    assert finish["status"] == "completed"
    assert finish["items_found"] == 1
    assert metadata["stage_counters"] == {"posts": 1, "pages": 1}
    assert metadata["persist_counters"]["posts_upserted"] == 1
    assert metadata["listing_progress"]["partial"] is False
    assert metadata["runtime_metadata"]["fetcher_runtime"]["canonical_fallback"]["used"] is True
    assert metadata["runtime_metadata"]["fetcher_runtime"]["canonical_fallback"]["trigger_reason"] == (
        "non_json_response"
    )


def test_tiktok_job_runner_recovers_warmup_http_failure_with_canonical_fallback(
    monkeypatch: pytest.MonkeyPatch,
    fake_lifecycle: _FakeLifecycle,
) -> None:
    from trr_backend.socials.tiktok.posts_scrapling import job_runner as jr

    class _WarmupFailureFetcher:
        runtime_metadata = {
            "transport": "test",
            "request_count": 1,
        }

        async def warmup(self, _account_handle: str) -> None:
            raise RuntimeError("Page.goto: net::ERR_HTTP_RESPONSE_CODE_FAILURE")

        async def resolve_sec_uid(self, _account_handle: str) -> str:
            raise AssertionError("secUid API should not run after warmup fallback")

        async def fetch_posts_page(self, *, sec_uid: str, cursor: str | None = None) -> SimpleNamespace:
            raise AssertionError("post API should not run after warmup fallback")

        async def aclose(self) -> None:
            return None

    fallback_posts = [SimpleNamespace(video_id="fallback-video-1"), SimpleNamespace(video_id="fallback-video-2")]
    fallback_calls: list[dict[str, Any]] = []

    def _fake_fallback(**kwargs):
        fallback_calls.append(kwargs)
        return fallback_posts, {
            "used": True,
            "retrieval_mode": "ytdlp",
            "trigger_reason": kwargs["trigger_reason"],
            "posts_found": len(fallback_posts),
        }

    _install_common_fakes(monkeypatch, jr, fetcher=_WarmupFailureFetcher())
    monkeypatch.setattr(jr, "_scrape_canonical_tiktok_fallback_posts", _fake_fallback)
    monkeypatch.setattr(
        jr,
        "persist_tiktok_post_dtos",
        lambda **_kwargs: PersistedTikTokPosts(posts_upserted=2, posts_skipped=0),
    )
    monkeypatch.setattr(jr.pg, "fetch_one", _running_status_or_final_row)

    jr.run_tiktok_posts_scrapling_job(
        {"id": "job-1", "run_id": "run-1", "config": {"account": "bravotv", "max_pages": 1}},
        worker_id="worker-1",
    )

    assert fallback_calls
    assert fallback_calls[0]["trigger_reason"] == "warmup_http_response_code_failure"

    finish = fake_lifecycle.finish_calls[-1]
    metadata = finish["metadata"]
    assert finish["status"] == "completed"
    assert finish["items_found"] == 2
    assert metadata["stage_counters"] == {"posts": 2, "pages": 1}
    assert metadata["runtime_metadata"]["fetcher_runtime"]["canonical_fallback"]["used"] is True
    assert metadata["runtime_metadata"]["fetcher_runtime"]["canonical_fallback"]["trigger_reason"] == (
        "warmup_http_response_code_failure"
    )


def test_tiktok_job_runner_times_out_stalled_fetch_with_retry(
    monkeypatch: pytest.MonkeyPatch,
    fake_lifecycle: _FakeLifecycle,
) -> None:
    import asyncio

    from trr_backend.socials.tiktok.posts_scrapling import job_runner as jr

    class _SlowFetcher:
        runtime_metadata = {"transport": "test", "request_count": 3}

        async def warmup(self, _account_handle: str) -> None:
            return None

        async def resolve_sec_uid(self, _account_handle: str) -> str:
            return "SEC_UID"

        async def fetch_posts_page(self, *, sec_uid: str, cursor: str | None = None) -> SimpleNamespace:
            del sec_uid, cursor
            await asyncio.sleep(10)
            raise AssertionError("fetch should be cancelled by the operation timeout")

        async def aclose(self) -> None:
            return None

    _install_common_fakes(monkeypatch, jr, fetcher=_SlowFetcher())
    monkeypatch.setattr(jr.pg, "fetch_one", _running_status_or_final_row)
    monkeypatch.setattr(jr, "_resolve_operation_timeout_seconds", lambda: 0.01)
    monkeypatch.setattr(jr, "_resolve_operation_heartbeat_interval_seconds", lambda: 0.001)

    jr.run_tiktok_posts_scrapling_job(
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
    metadata = finish["metadata"]
    assert finish["status"] == "retrying"
    assert finish["last_error_code"] == "tiktok_posts_fetch_posts_page_timeout"
    assert metadata["runtime_metadata"]["phase"] == "fetch_posts_page"
    assert fake_lifecycle.heartbeat_calls


def test_tiktok_job_runner_records_cancelled_job_metadata(
    monkeypatch: pytest.MonkeyPatch,
    fake_lifecycle: _FakeLifecycle,
) -> None:
    from trr_backend.socials.tiktok.posts_scrapling import job_runner as jr

    class _FakeFetcher:
        runtime_metadata = {"transport": "test", "request_count": 1, "warmup_cookie_count": 0}

        async def warmup(self, _account_handle: str) -> None:
            return None

        async def resolve_sec_uid(self, _account_handle: str) -> str:
            raise AssertionError("resolve_sec_uid should not run after cancellation")

        async def fetch_posts_page(self, *, sec_uid: str, cursor: str | None = None) -> SimpleNamespace:
            raise AssertionError("fetch_posts_page should not run after cancellation")

        async def aclose(self) -> None:
            return None

    def _cancelled_status_or_final_row(query: str, *_args, **_kwargs) -> dict[str, Any]:
        normalized = " ".join(str(query).split()).lower()
        if normalized.startswith("select status from social.scrape_jobs"):
            return {"status": "cancelled"}
        if normalized.startswith("select status from social.scrape_runs"):
            return {"status": "running"}
        return {"id": "job-1", "status": "cancelled"}

    _install_common_fakes(monkeypatch, jr, fetcher=_FakeFetcher())
    monkeypatch.setattr(jr.pg, "fetch_one", _cancelled_status_or_final_row)

    result = jr.run_tiktok_posts_scrapling_job(
        {"id": "job-1", "run_id": "run-1", "config": {"account": "bravotv"}},
        worker_id="worker-1",
    )

    finish = fake_lifecycle.finish_calls[-1]
    metadata = finish["metadata"]
    assert result["status"] == "cancelled"
    assert finish["status"] == "cancelled"
    assert finish["last_error_code"] == "tiktok_posts_scrapling_cancelled"
    assert metadata["cancelled"] is True
    assert metadata["cancel_scope"] == "job"
    assert metadata["job_status_at_cancel"] == "cancelled"
    assert metadata["run_status_at_cancel"] == "running"
    assert metadata["runtime_metadata"]["transport"] == "test"
    assert metadata["activity"]["phase"] == "cancelled"


def test_tiktok_job_runner_returns_degraded_summary_when_final_db_read_fails(
    monkeypatch: pytest.MonkeyPatch,
    fake_lifecycle: _FakeLifecycle,
) -> None:
    from trr_backend.socials.tiktok.posts_scrapling import job_runner as jr

    class _FakeFetcher:
        runtime_metadata = {"transport": "test", "request_count": 2}

        async def warmup(self, _account_handle: str) -> None:
            return None

        async def resolve_sec_uid(self, _account_handle: str) -> str:
            return "SEC_UID"

        async def fetch_posts_page(self, *, sec_uid: str, cursor: str | None = None) -> SimpleNamespace:
            del sec_uid, cursor
            return SimpleNamespace(
                auth_failed=False,
                fetch_failed=False,
                retryable=False,
                fetch_reason=None,
                posts=[{"id": "tiktok-1"}],
                has_more=False,
                cursor=None,
            )

        async def aclose(self) -> None:
            return None

    def _status_or_db_error(query: str, *_args, **_kwargs) -> dict[str, Any]:
        normalized = " ".join(str(query).split()).lower()
        if normalized.startswith("select status from social.scrape_jobs"):
            return {"status": "running"}
        if normalized.startswith("select status from social.scrape_runs"):
            return {"status": "running"}
        raise jr.pg.DatabaseServiceUnavailableError("db saturated")

    _install_common_fakes(
        monkeypatch,
        jr,
        fetcher=_FakeFetcher(),
        persist_result=PersistedTikTokPosts(posts_upserted=1, posts_skipped=0),
    )
    fake_lifecycle.finalize_error = jr.pg.DatabaseServiceUnavailableError("db saturated")
    monkeypatch.setattr(jr.pg, "fetch_one", _status_or_db_error)

    result = jr.run_tiktok_posts_scrapling_job(
        {"id": "job-1", "run_id": "run-1", "config": {"account": "bravotv"}},
        worker_id="worker-1",
    )

    assert fake_lifecycle.finalize_calls == ["run-1"]
    assert result["id"] == "job-1"
    assert result["status"] == "completed"
    assert result["items_found"] == 1
    assert result["metadata"]["degraded_summary"] is True
    assert result["metadata"]["database_service_unavailable"] is True
    assert result["metadata"]["stage_counters"] == {"posts": 1, "pages": 1}
    assert result["metadata"]["persist_counters"]["posts_upserted"] == 1
