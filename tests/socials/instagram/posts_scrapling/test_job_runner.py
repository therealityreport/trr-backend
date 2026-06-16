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


def test_select_posts_proxy_distributes_explicit_urls_by_session_key(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.socials.instagram.posts_scrapling import proxy

    monkeypatch.setenv(
        "SOCIAL_INSTAGRAM_POSTS_PROXY_URLS",
        "http://user:pass@proxy-a.example:7000,http://user:pass@proxy-b.example:7001",
    )
    monkeypatch.setattr(proxy, "_build_proxy_rotator", lambda _value: object())

    first = proxy.select_posts_proxy(session_key="thetraitorsus:details:0")
    second = proxy.select_posts_proxy(session_key="thetraitorsus:details:1")

    assert first is not None
    assert second is not None
    assert first.session_mode == "explicit_sharded"
    assert second.session_mode == "explicit_sharded"
    assert first.fingerprint in {"proxy-a.example:7000:explicit", "proxy-b.example:7001:explicit"}
    assert second.fingerprint in {"proxy-a.example:7000:explicit", "proxy-b.example:7001:explicit"}
    assert "user" not in first.fingerprint
    assert "pass" not in first.fingerprint


def test_job_runner_rollout_flags_default_to_public_first(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.socials.instagram.posts_scrapling import job_runner as jr

    monkeypatch.delenv("SOCIAL_INSTAGRAM_SCRAPE_MODE", raising=False)
    monkeypatch.delenv("SOCIAL_INSTAGRAM_POSTS_ANONYMOUS_ENABLED", raising=False)
    monkeypatch.delenv("SOCIAL_INSTAGRAM_POSTS_ROTATE_ON_BLOCK_MAX_RETRIES", raising=False)
    monkeypatch.delenv("SOCIAL_INSTAGRAM_POSTS_ANONYMOUS_ROTATE_ON_BLOCK_MAX_RETRIES", raising=False)

    assert jr._instagram_scrape_mode({}) == "public_first"
    assert jr._posts_public_first_enabled({}) is True
    assert jr._posts_anonymous_enabled({}) is False
    assert jr._posts_anonymous_enabled({"instagram_scrape_mode": "anonymous"}) is True
    assert jr._rotate_on_block_max_retries(anonymous=False) == 0
    assert jr._rotate_on_block_max_retries(anonymous=True) == 2


def test_anonymous_job_skips_auth_session_and_cooldown(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.posts_scrapling import job_runner as jr

    fetcher_kwargs: dict[str, Any] = {}
    finish_calls: list[dict[str, Any]] = []

    class _FakeFetcher:
        def __init__(self, **kwargs: Any) -> None:
            fetcher_kwargs.update(kwargs)
            self.runtime_metadata = {
                "auth_state": kwargs.get("auth_state"),
                "request_count": 1,
            }

        async def warmup(self, _account_handle: str) -> None:
            return None

        async def fetch_posts_page(self, _account_handle: str, cursor=None):  # noqa: ANN001
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

    monkeypatch.setattr(jr, "resolve_posts_scrapling_session", lambda **_kwargs: pytest.fail("auth session resolved"))
    monkeypatch.setattr(jr, "_raise_if_auth_cooldown_active", lambda **_kwargs: pytest.fail("cooldown read"))
    monkeypatch.setattr(jr.auth_cooldown, "clear_cooldown", lambda *_args, **_kwargs: pytest.fail("cooldown clear"))
    monkeypatch.setattr(jr, "InstagramPostsScraplingFetcher", _FakeFetcher)
    monkeypatch.setattr(jr, "select_posts_proxy", lambda **_kwargs: None)
    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})
    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(repo, "_emit_job_progress", lambda **_kwargs: True)
    monkeypatch.setattr(repo, "_iso", lambda _value: "2026-04-21T00:00:00+00:00")
    monkeypatch.setattr(repo, "_now_utc", lambda: datetime(2026, 4, 28, tzinfo=UTC))
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(repo, "_finish_job", lambda job_id, **kwargs: finish_calls.append({"job_id": job_id, **kwargs}))
    monkeypatch.setattr(repo, "latest_instagram_profile_pagination_state", lambda **_kwargs: {})
    monkeypatch.setattr(repo, "persist_instagram_profile_pagination_state", lambda **_kwargs: {})
    monkeypatch.setattr(jr.pg, "fetch_one", lambda *_args, **_kwargs: {})

    jr.run_instagram_posts_scrapling_job(
        {
            "id": "job-1",
            "run_id": "run-1",
            "config": {
                "account": "bravotv",
                "stage": "posts_scrapling",
                "instagram_scrape_mode": "anonymous",
                "anonymous_enabled": True,
            },
        },
        worker_id="worker-1",
    )

    assert fetcher_kwargs["auth_state"] == "anonymous"
    assert fetcher_kwargs["cookies"] == []
    assert fetcher_kwargs["raw_cookies"] == {}
    assert finish_calls[-1]["status"] == "completed"


def test_public_first_job_uses_no_auth_no_proxy_and_persists_cursors(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.posts_scrapling import job_runner as jr
    from trr_backend.socials.instagram.posts_scrapling.persistence import PersistedInstagramPosts

    scraper_kwargs: dict[str, Any] = {}
    fetch_calls: list[dict[str, Any]] = []
    persist_calls: list[dict[str, Any]] = []
    pagination_calls: list[dict[str, Any]] = []
    finish_calls: list[dict[str, Any]] = []

    class _PublicScraper:
        def __init__(self, **kwargs: Any) -> None:
            scraper_kwargs.update(kwargs)
            self._request_count = 0
            self.last_retrieval_meta: dict[str, Any] = {}

        def fetch_posts_graphql(self, username: str, **kwargs: Any) -> dict[str, Any]:
            fetch_calls.append({"username": username, **kwargs})
            self._request_count += 1
            cursor = kwargs.get("cursor")
            self.last_retrieval_meta = {
                "retrieval_transport": "requests_enriched",
                "graphql_cursor": cursor,
                "doc_id_used": "doc-public",
                "profile_posts_doc_ids_attempted": ["doc-public"],
            }
            shortcode = "ABC123" if cursor is None else "DEF456"
            return {
                "data": {
                    "xdt_api__v1__feed__user_timeline_graphql_connection": {
                        "edges": [{"node": {"code": shortcode, "pk": shortcode.lower(), "id": shortcode.lower()}}],
                        "page_info": {
                            "has_next_page": cursor is None,
                            "end_cursor": "cursor-1" if cursor is None else None,
                        },
                    }
                }
            }

    monkeypatch.setattr("trr_backend.socials.instagram.scraper.InstagramScraper", _PublicScraper)
    monkeypatch.setattr(jr, "resolve_posts_scrapling_session", lambda **_kwargs: pytest.fail("auth session resolved"))
    monkeypatch.setattr(jr, "_raise_if_auth_cooldown_active", lambda **_kwargs: pytest.fail("cooldown read"))
    monkeypatch.setattr(jr, "select_posts_proxy", lambda **_kwargs: pytest.fail("proxy selected"))
    monkeypatch.setattr(jr, "InstagramPostsScraplingFetcher", lambda **_kwargs: pytest.fail("scrapling fetcher used"))
    monkeypatch.setattr(
        jr,
        "persist_instagram_posts",
        lambda **kwargs: (
            persist_calls.append(dict(kwargs))
            or PersistedInstagramPosts(posts_upserted=len(kwargs["post_nodes"]), posts_skipped=0)
        ),
    )
    monkeypatch.setattr(repo, "latest_instagram_profile_pagination_state", lambda **_kwargs: {})
    monkeypatch.setattr(
        repo,
        "persist_instagram_profile_pagination_state",
        lambda **kwargs: pagination_calls.append(dict(kwargs)) or dict(kwargs),
    )
    monkeypatch.setattr(
        jr,
        "lifecycle",
        SimpleNamespace(
            new_job_progress_state=lambda: {},
            touch_job_heartbeat=lambda *_args, **_kwargs: None,
            emit_job_progress=lambda **_kwargs: True,
            finish_job=lambda job_id, **kwargs: finish_calls.append({"job_id": job_id, **kwargs}),
            finalize_run_status=lambda *_args, **_kwargs: {},
            now_utc=lambda: datetime(2026, 4, 28, tzinfo=UTC),
            format_time=lambda value: value.isoformat(),
        ),
    )
    monkeypatch.setattr(
        jr.pg,
        "fetch_one",
        lambda *_args, **_kwargs: {"status": "running"},
    )

    jr.run_instagram_posts_scrapling_job(
        {
            "id": "job-public",
            "run_id": "run-public",
            "config": {
                "account": "bravotv",
                "stage": "posts_scrapling",
                "instagram_scrape_mode": "public_first",
                "max_pages": 2,
            },
        },
        worker_id="worker-public",
    )

    assert scraper_kwargs["cookies"] == {}
    assert scraper_kwargs["attach_auth_session"] is False
    assert fetch_calls[0]["allow_browser_fallback"] is False
    assert fetch_calls[0]["allow_recovery"] is False
    assert [call["cursor"] for call in fetch_calls] == [None, "cursor-1"]
    assert len(persist_calls) == 2
    assert pagination_calls[0]["proxy_fingerprint"] == "none"
    assert pagination_calls[0]["proxy_session_key"] is None
    assert pagination_calls[0]["metadata"]["auth_state"] == "public"
    assert pagination_calls[0]["metadata"]["proxy_state"] == "none"
    assert pagination_calls[0]["end_cursor"] == "cursor-1"
    assert finish_calls[-1]["status"] == "completed"
    assert finish_calls[-1]["metadata"]["instagram_scrape_mode"] == "public_first"
    assert finish_calls[-1]["metadata"]["auth_state"] == "public"
    assert finish_calls[-1]["metadata"]["proxy_state"] == "none"


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
        lambda **_kwargs: None,
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
        "config": {
            "account": "thetraitorsus",
            "stage": "posts_scrapling",
            "instagram_scrape_mode": "authenticated",
        },
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
    monkeypatch.setattr(jr, "select_posts_proxy", lambda **_kwargs: None)
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
        "config": {
            "account": "thetraitorsus",
            "stage": "posts_scrapling",
            "instagram_scrape_mode": "authenticated",
        },
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
    monkeypatch.setattr(jr, "select_posts_proxy", lambda **_kwargs: None)
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
        {
            "id": "job-1",
            "run_id": "run-1",
            "config": {"account": "thetraitorsus", "instagram_scrape_mode": "authenticated"},
        },
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
    monkeypatch.setattr(jr, "select_posts_proxy", lambda **_kwargs: None)
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
        {
            "id": "job-1",
            "run_id": "run-1",
            "config": {"account": "thetraitorsus", "instagram_scrape_mode": "authenticated"},
        },
        worker_id="worker-1",
    )

    assert payload["status"] == "completed"
    assert payload["items_found"] == 0
    assert payload["metadata"] == {
        "degraded_summary": True,
        "database_service_unavailable": True,
    }


def test_job_runner_persists_page_checkpoint_and_resumes_latest_cursor(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.posts_scrapling import job_runner as jr
    from trr_backend.socials.instagram.posts_scrapling.persistence import PersistedInstagramPosts

    fetch_cursors: list[str | None] = []
    checkpoint_calls: list[dict[str, Any]] = []

    class _FakeFetcher:
        runtime_metadata = {
            "transport": "test",
            "request_count": 1,
            "doc_id_used": "doc-good",
            "doc_ids_attempted": ["doc-good"],
            "proxy_fingerprint": "proxy-a",
        }

        async def warmup(self, _account_handle: str) -> None:
            return None

        async def fetch_posts_page(self, _account_handle: str, *, cursor: str | None = None) -> SimpleNamespace:
            fetch_cursors.append(cursor)
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
        jr,
        "resolve_posts_scrapling_session",
        lambda **_kwargs: SimpleNamespace(
            cookies={},
            browser_account_id="thetraitorsus",
            auth_session=SimpleNamespace(cookies={}, metadata={"source": "test"}),
        ),
    )
    monkeypatch.setattr(jr, "select_posts_proxy", lambda **_kwargs: SimpleNamespace(fingerprint="proxy-a"))
    monkeypatch.setattr(jr, "InstagramPostsScraplingFetcher", lambda **_kwargs: _FakeFetcher())
    monkeypatch.setattr(
        jr,
        "persist_instagram_posts",
        lambda **_kwargs: PersistedInstagramPosts(posts_upserted=1, posts_skipped=0, posts_skipped_by_reason={}),
    )
    monkeypatch.setattr(
        repo,
        "latest_instagram_profile_pagination_state",
        lambda **_kwargs: {"end_cursor": "resume-cursor"},
    )

    def _capture_checkpoint(**kwargs: Any) -> dict[str, Any]:
        checkpoint_calls.append(kwargs)
        return {
            "end_cursor": kwargs.get("end_cursor"),
            "stop_reason": kwargs.get("stop_reason"),
            "posts_seen": kwargs.get("posts_seen"),
            "posts_upserted": kwargs.get("posts_upserted"),
        }

    monkeypatch.setattr(repo, "persist_instagram_profile_pagination_state", _capture_checkpoint)
    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})
    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(repo, "_emit_job_progress", lambda **_kwargs: True)
    monkeypatch.setattr(repo, "_finish_job", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(repo, "_iso", lambda _value: "2026-05-03T00:00:00+00:00")
    monkeypatch.setattr(repo, "_now_utc", lambda: datetime(2026, 5, 3, tzinfo=UTC))
    monkeypatch.setattr(repo, "instagram_posts_acceleration_flags", lambda: {"flags": {}})
    monkeypatch.setattr(jr.pg, "fetch_one", lambda *_args, **_kwargs: {"id": "job-1", "status": "completed"})

    jr.run_instagram_posts_scrapling_job(
        {
            "id": "job-1",
            "run_id": "11111111-1111-4111-8111-111111111111",
            "config": {"account": "thetraitorsus", "instagram_scrape_mode": "authenticated"},
        },
        worker_id="worker-1",
    )

    assert fetch_cursors == ["resume-cursor"]
    assert checkpoint_calls[-1]["cursor_in"] == "resume-cursor"
    assert checkpoint_calls[-1]["posts_seen"] == 1
    assert checkpoint_calls[-1]["posts_upserted"] == 1
    assert checkpoint_calls[-1]["doc_id_used"] == "doc-good"
    assert checkpoint_calls[-1]["stop_reason"] == "completed"
    assert checkpoint_calls[-1]["completed"] is True


def test_job_runner_retries_when_page_checkpoint_persist_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.posts_scrapling import job_runner as jr
    from trr_backend.socials.instagram.posts_scrapling.persistence import PersistedInstagramPosts

    finish_calls: list[dict[str, Any]] = []

    class _FakeFetcher:
        runtime_metadata = {"transport": "test", "request_count": 1, "doc_id_used": "doc-good"}

        async def warmup(self, _account_handle: str) -> None:
            return None

        async def fetch_posts_page(self, _account_handle: str, *, cursor: str | None = None) -> SimpleNamespace:
            del cursor
            return SimpleNamespace(
                auth_failed=False,
                fetch_failed=False,
                retryable=False,
                fetch_reason=None,
                posts=[{"shortcode": "abc123"}],
                has_next_page=True,
                end_cursor="cursor-1",
            )

        async def aclose(self) -> None:
            return None

    def _fake_fetch_one(sql: str, _params: list[object] | None = None, **_kwargs: Any) -> dict[str, Any]:
        normalized = " ".join(sql.split()).lower()
        if "select status from social.scrape_jobs" in normalized:
            return {"status": "running"}
        if "select status from social.scrape_runs" in normalized:
            return {"status": "running"}
        return {"id": "job-1", "status": "retrying", "metadata": finish_calls[-1]["metadata"] if finish_calls else {}}

    monkeypatch.setattr(
        jr,
        "resolve_posts_scrapling_session",
        lambda **_kwargs: SimpleNamespace(
            cookies={},
            browser_account_id="thetraitorsus",
            auth_session=SimpleNamespace(cookies={}, metadata={"source": "test"}),
        ),
    )
    monkeypatch.setattr(jr, "select_posts_proxy", lambda **_kwargs: None)
    monkeypatch.setattr(jr, "posts_proxy_feature_flags", lambda: {"page_proxy_rotation_enabled": False})
    monkeypatch.setattr(jr, "InstagramPostsScraplingFetcher", lambda **_kwargs: _FakeFetcher())
    monkeypatch.setattr(
        jr,
        "persist_instagram_posts",
        lambda **_kwargs: PersistedInstagramPosts(posts_upserted=1, posts_skipped=0, posts_skipped_by_reason={}),
    )
    monkeypatch.setattr(repo, "latest_instagram_profile_pagination_state", lambda **_kwargs: {})
    monkeypatch.setattr(
        repo,
        "persist_instagram_profile_pagination_state",
        lambda **_kwargs: {"skipped": True, "reason": "pagination_state_persist_failed"},
    )
    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})
    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(repo, "_emit_job_progress", lambda **_kwargs: True)
    monkeypatch.setattr(repo, "_finish_job", lambda job_id, **kwargs: finish_calls.append({"job_id": job_id, **kwargs}))
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(repo, "_retry_backoff_seconds", lambda _attempt: 30)
    monkeypatch.setattr(repo, "_now_utc", lambda: datetime(2026, 5, 3, tzinfo=UTC))
    monkeypatch.setattr(repo, "instagram_posts_acceleration_flags", lambda: {"flags": {}})
    monkeypatch.setattr(jr.pg, "fetch_one", _fake_fetch_one)

    jr.run_instagram_posts_scrapling_job(
        {
            "id": "job-1",
            "run_id": "11111111-1111-4111-8111-111111111111",
            "attempt_count": 1,
            "max_attempts": 2,
            "config": {"account": "thetraitorsus", "instagram_scrape_mode": "authenticated"},
        },
        worker_id="worker-1",
    )

    assert finish_calls[-1]["status"] == "retrying"
    assert finish_calls[-1]["last_error_code"] == "pagination_state_persist_failed"
    metadata = finish_calls[-1]["metadata"]
    assert metadata["listing_progress"]["stop_reason"] == "pagination_state_persist_failed"
    assert metadata["runtime_metadata"]["pagination_checkpoint"]["direction"] == "forward"


def test_job_runner_rotates_page_proxy_when_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.posts_scrapling import job_runner as jr
    from trr_backend.socials.instagram.posts_scrapling.persistence import PersistedInstagramPosts

    proxy_selections: list[dict[str, Any]] = []
    proxy_switches: list[dict[str, Any]] = []

    class _FakeFetcher:
        def __init__(self, **_kwargs) -> None:
            self.runtime_metadata = {
                "transport": "test",
                "request_count": 0,
                "profile_posts_doc_ids": {"used": "doc-good", "attempted": ["doc-good"]},
                "selected_proxy_fingerprint": "initial-proxy",
            }

        async def warmup(self, _account_handle: str) -> None:
            return None

        async def set_api_proxy_config(self, proxy_config: Any, *, reason: str) -> None:
            proxy_switches.append({"fingerprint": proxy_config.fingerprint, "reason": reason})
            self.runtime_metadata["selected_proxy_fingerprint"] = proxy_config.fingerprint

        async def fetch_posts_page(self, _account_handle: str, *, cursor: str | None = None) -> SimpleNamespace:
            if cursor is None:
                return SimpleNamespace(
                    auth_failed=False,
                    fetch_failed=False,
                    retryable=False,
                    fetch_reason=None,
                    posts=[{"shortcode": "abc123"}],
                    has_next_page=True,
                    end_cursor="cursor-1",
                )
            return SimpleNamespace(
                auth_failed=False,
                fetch_failed=False,
                retryable=False,
                fetch_reason=None,
                posts=[{"shortcode": "def456"}],
                has_next_page=False,
                end_cursor=None,
            )

        async def aclose(self) -> None:
            return None

    def _select_posts_proxy(**kwargs: Any) -> SimpleNamespace:
        proxy_selections.append(kwargs)
        page_index = kwargs.get("page_index")
        fingerprint = "proxy-b:8080:explicit" if page_index == 1 else "proxy-a:8080:explicit"
        return SimpleNamespace(
            api_proxy_url=f"http://{fingerprint}",
            fingerprint=fingerprint,
            session_mode="explicit_page_rotation" if page_index is not None else "explicit_sharded",
            rotation_index=page_index,
        )

    monkeypatch.setattr(
        jr,
        "resolve_posts_scrapling_session",
        lambda **_kwargs: SimpleNamespace(
            cookies={},
            browser_account_id="thetraitorsus",
            auth_session=SimpleNamespace(cookies={}, metadata={"source": "test"}),
        ),
    )
    monkeypatch.setattr(jr, "select_posts_proxy", _select_posts_proxy)
    monkeypatch.setattr(jr, "posts_proxy_feature_flags", lambda: {"page_proxy_rotation_enabled": True})
    monkeypatch.setattr(jr, "InstagramPostsScraplingFetcher", lambda **_kwargs: _FakeFetcher())
    monkeypatch.setattr(
        jr,
        "persist_instagram_posts",
        lambda **_kwargs: PersistedInstagramPosts(posts_upserted=1, posts_skipped=0, posts_skipped_by_reason={}),
    )
    monkeypatch.setattr(repo, "latest_instagram_profile_pagination_state", lambda **_kwargs: {})
    monkeypatch.setattr(
        repo,
        "persist_instagram_profile_pagination_state",
        lambda **kwargs: {"end_cursor": kwargs.get("end_cursor"), "stop_reason": kwargs.get("stop_reason")},
    )
    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})
    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(repo, "_emit_job_progress", lambda **_kwargs: True)
    monkeypatch.setattr(repo, "_finish_job", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(repo, "_iso", lambda _value: "2026-05-03T00:00:00+00:00")
    monkeypatch.setattr(repo, "_now_utc", lambda: datetime(2026, 5, 3, tzinfo=UTC))
    monkeypatch.setattr(repo, "instagram_posts_acceleration_flags", lambda: {"flags": {}})
    monkeypatch.setattr(jr.pg, "fetch_one", lambda *_args, **_kwargs: {"id": "job-1", "status": "completed"})

    jr.run_instagram_posts_scrapling_job(
        {
            "id": "job-1",
            "run_id": "11111111-1111-4111-8111-111111111111",
            "config": {"account": "thetraitorsus", "instagram_scrape_mode": "authenticated"},
        },
        worker_id="worker-1",
    )

    assert [selection.get("page_index") for selection in proxy_selections] == [None, 0, 1]
    assert [switch["fingerprint"] for switch in proxy_switches] == [
        "proxy-a:8080:explicit",
        "proxy-b:8080:explicit",
    ]


def test_job_runner_starts_reverse_walker_after_bidirectional_probe_passes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.posts_scrapling import job_runner as jr
    from trr_backend.socials.instagram.posts_scrapling.persistence import PersistedInstagramPosts

    finish_calls: list[dict[str, Any]] = []
    checkpoint_calls: list[dict[str, Any]] = []
    fetcher_instances: list[Any] = []

    class _FakeFetcher:
        def __init__(self, **_kwargs: Any) -> None:
            self.kind = "forward" if not fetcher_instances else "reverse"
            self.runtime_metadata = {"transport": "test", "request_count": 0, "selected_proxy_fingerprint": self.kind}
            fetcher_instances.append(self)

        async def warmup(self, _account_handle: str) -> None:
            return None

        def warmup_snapshot(self) -> dict[str, Any]:
            return {"raw_cookies": {"sessionid": "x"}, "page_tokens": {"lsd": "token"}}

        async def apply_warmup_snapshot(self, _snapshot: dict[str, Any]) -> None:
            self.runtime_metadata["warmup_snapshot_reused"] = True

        async def probe_bidirectional_walk(
            self,
            _account_handle: str,
            *,
            forward_posts: list[dict[str, Any]],
        ) -> dict[str, Any]:
            assert forward_posts == [{"id": "new-1", "code": "NEW"}]
            self.runtime_metadata["bidirectional_probe"] = {"passed": True, "reason": "reverse_probe_passed"}
            return {"passed": True, "reason": "reverse_probe_passed"}

        async def fetch_posts_page(
            self,
            _account_handle: str,
            *,
            cursor: str | None = None,
            direction: str = "forward",
        ) -> SimpleNamespace:
            self.runtime_metadata["request_count"] += 1
            if self.kind == "reverse":
                assert direction == "reverse"
                assert cursor is None
                return SimpleNamespace(
                    auth_failed=False,
                    fetch_failed=False,
                    retryable=False,
                    fetch_reason=None,
                    posts=[{"id": "old-1", "code": "OLD"}],
                    has_next_page=False,
                    end_cursor=None,
                )
            assert direction == "forward"
            return SimpleNamespace(
                auth_failed=False,
                fetch_failed=False,
                retryable=False,
                fetch_reason=None,
                posts=[{"id": "new-1", "code": "NEW"}],
                has_next_page=False,
                end_cursor=None,
            )

        async def aclose(self) -> None:
            return None

    monkeypatch.setenv("SOCIAL_INSTAGRAM_POSTS_BIDIRECTIONAL_WALK_ENABLED", "1")
    monkeypatch.setattr(
        jr,
        "resolve_posts_scrapling_session",
        lambda **_kwargs: SimpleNamespace(
            cookies={},
            browser_account_id="thetraitorsus",
            auth_session=SimpleNamespace(cookies={}, metadata={"source": "test"}),
        ),
    )
    monkeypatch.setattr(jr, "select_posts_proxy", lambda **_kwargs: None)
    monkeypatch.setattr(jr, "posts_proxy_feature_flags", lambda: {"page_proxy_rotation_enabled": False})
    monkeypatch.setattr(jr, "InstagramPostsScraplingFetcher", _FakeFetcher)
    monkeypatch.setattr(
        jr,
        "persist_instagram_posts",
        lambda **kwargs: PersistedInstagramPosts(
            posts_upserted=len(kwargs.get("post_nodes") or []),
            posts_skipped=0,
            posts_skipped_by_reason={},
        ),
    )
    monkeypatch.setattr(repo, "latest_instagram_profile_pagination_state", lambda **_kwargs: {})
    monkeypatch.setattr(
        repo,
        "persist_instagram_profile_pagination_state",
        lambda **kwargs: checkpoint_calls.append(kwargs) or {"end_cursor": kwargs.get("end_cursor")},
    )
    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})
    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(repo, "_emit_job_progress", lambda **_kwargs: True)
    monkeypatch.setattr(repo, "_finish_job", lambda job_id, **kwargs: finish_calls.append({"job_id": job_id, **kwargs}))
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(repo, "_iso", lambda _value: "2026-05-03T00:00:00+00:00")
    monkeypatch.setattr(repo, "_now_utc", lambda: datetime(2026, 5, 3, tzinfo=UTC))
    monkeypatch.setattr(repo, "instagram_posts_acceleration_flags", lambda: {"flags": {}})
    monkeypatch.setattr(jr.pg, "fetch_one", lambda *_args, **_kwargs: {"id": "job-1", "status": "completed"})

    jr.run_instagram_posts_scrapling_job(
        {
            "id": "job-1",
            "run_id": "11111111-1111-4111-8111-111111111111",
            "config": {"account": "thetraitorsus", "instagram_scrape_mode": "authenticated"},
        },
        worker_id="worker-1",
    )

    assert [call["direction"] for call in checkpoint_calls] == ["forward", "reverse"]
    assert finish_calls[-1]["items_found"] == 2
    assert finish_calls[-1]["metadata"]["bidirectional_listing"]["reverse_started"] is True
    assert finish_calls[-1]["metadata"]["bidirectional_listing"]["reverse_posts_seen"] == 1


def test_job_runner_timeout_guard_records_partial_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.posts_scrapling import job_runner as jr

    checkpoint_calls: list[dict[str, Any]] = []
    finish_calls: list[dict[str, Any]] = []

    class _FakeFetcher:
        runtime_metadata = {"transport": "test", "request_count": 0, "doc_id_used": "doc-good"}

        async def warmup(self, _account_handle: str) -> None:
            return None

        async def fetch_posts_page(self, _account_handle: str, *, cursor: str | None = None) -> SimpleNamespace:
            raise AssertionError("timeout guard should fire before fetching")

        async def aclose(self) -> None:
            return None

    monkeypatch.setattr(jr, "_posts_pagination_timeout_guard_seconds", lambda _config: 0.0)
    monkeypatch.setattr(
        jr,
        "resolve_posts_scrapling_session",
        lambda **_kwargs: SimpleNamespace(
            cookies={},
            browser_account_id="thetraitorsus",
            auth_session=SimpleNamespace(cookies={}, metadata={"source": "test"}),
        ),
    )
    monkeypatch.setattr(jr, "select_posts_proxy", lambda **_kwargs: None)
    monkeypatch.setattr(jr, "InstagramPostsScraplingFetcher", lambda **_kwargs: _FakeFetcher())
    monkeypatch.setattr(repo, "latest_instagram_profile_pagination_state", lambda **_kwargs: {})

    def _capture_checkpoint(**kwargs: Any) -> dict[str, Any]:
        checkpoint_calls.append(kwargs)
        return {"end_cursor": kwargs.get("end_cursor"), "stop_reason": kwargs.get("stop_reason")}

    monkeypatch.setattr(repo, "persist_instagram_profile_pagination_state", _capture_checkpoint)
    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})
    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(repo, "_finish_job", lambda job_id, **kwargs: finish_calls.append({"job_id": job_id, **kwargs}))
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(repo, "_retry_backoff_seconds", lambda _attempt: 30)
    monkeypatch.setattr(repo, "_now_utc", lambda: datetime(2026, 5, 3, tzinfo=UTC))
    monkeypatch.setattr(repo, "instagram_posts_acceleration_flags", lambda: {"flags": {}})
    monkeypatch.setattr(jr.pg, "fetch_one", lambda *_args, **_kwargs: {"id": "job-1", "status": "retrying"})

    jr.run_instagram_posts_scrapling_job(
        {
            "id": "job-1",
            "run_id": "11111111-1111-4111-8111-111111111111",
            "attempt_count": 1,
            "max_attempts": 2,
            "config": {
                "account": "thetraitorsus",
                "instagram_scrape_mode": "authenticated",
                "pagination_timeout_guard_seconds": 1,
            },
        },
        worker_id="worker-1",
    )

    assert checkpoint_calls[-1]["stop_reason"] == "timeout_guard"
    assert checkpoint_calls[-1]["partial"] is True
    assert finish_calls[-1]["status"] == "retrying"
    assert finish_calls[-1]["last_error_code"] == "timeout_guard"
    assert finish_calls[-1]["metadata"]["listing_progress"]["stop_reason"] == "timeout_guard"


def test_job_runner_cursor_expired_records_restart_required(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.posts_scrapling import job_runner as jr

    checkpoint_calls: list[dict[str, Any]] = []
    finish_calls: list[dict[str, Any]] = []

    class _FakeFetcher:
        runtime_metadata = {"transport": "test", "request_count": 1, "doc_id_used": "doc-good"}

        async def warmup(self, _account_handle: str) -> None:
            return None

        async def fetch_posts_page(self, _account_handle: str, *, cursor: str | None = None) -> SimpleNamespace:
            assert cursor == "expired-cursor"
            return SimpleNamespace(
                auth_failed=False,
                fetch_failed=True,
                retryable=False,
                fetch_reason="cursor_expired",
                posts=[],
                has_next_page=False,
                end_cursor=None,
            )

        async def aclose(self) -> None:
            return None

    monkeypatch.setattr(
        jr,
        "resolve_posts_scrapling_session",
        lambda **_kwargs: SimpleNamespace(
            cookies={},
            browser_account_id="thetraitorsus",
            auth_session=SimpleNamespace(cookies={}, metadata={"source": "test"}),
        ),
    )
    monkeypatch.setattr(jr, "select_posts_proxy", lambda **_kwargs: None)
    monkeypatch.setattr(jr, "InstagramPostsScraplingFetcher", lambda **_kwargs: _FakeFetcher())
    monkeypatch.setattr(
        repo,
        "latest_instagram_profile_pagination_state",
        lambda **_kwargs: {"end_cursor": "expired-cursor"},
    )
    monkeypatch.setattr(
        repo,
        "persist_instagram_profile_pagination_state",
        lambda **kwargs: checkpoint_calls.append(kwargs) or {"end_cursor": kwargs.get("end_cursor")},
    )
    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})
    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(repo, "_finish_job", lambda job_id, **kwargs: finish_calls.append({"job_id": job_id, **kwargs}))
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(repo, "_now_utc", lambda: datetime(2026, 5, 3, tzinfo=UTC))
    monkeypatch.setattr(repo, "instagram_posts_acceleration_flags", lambda: {"flags": {}})
    monkeypatch.setattr(jr.pg, "fetch_one", lambda *_args, **_kwargs: {"id": "job-1", "status": "failed"})

    jr.run_instagram_posts_scrapling_job(
        {
            "id": "job-1",
            "run_id": "11111111-1111-4111-8111-111111111111",
            "attempt_count": 1,
            "max_attempts": 1,
            "config": {"account": "thetraitorsus", "instagram_scrape_mode": "authenticated"},
        },
        worker_id="worker-1",
    )

    assert checkpoint_calls[-1]["stop_reason"] == "cursor_expired_restart_required"
    assert checkpoint_calls[-1]["partial"] is True
    assert finish_calls[-1]["status"] == "failed"
    assert finish_calls[-1]["last_error_code"] == "cursor_expired_restart_required"
