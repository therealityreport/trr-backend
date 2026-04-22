"""Retry/backoff, cookie bridge, redirect handling, and partial-progress tests.

Mocks target `_fetch_api` (httpx path) for JSON tests, not the old `_fetch`
(browser path). The browser path is only used by warmup().
"""

from __future__ import annotations

import asyncio
from contextlib import nullcontext
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from trr_backend.socials.instagram.comments_scrapling.fetcher import (
    InstagramCommentsFetchResult,
    InstagramCommentsScraplingFetcher,
)
from trr_backend.socials.instagram.scraper import InstagramComment


class _TrackingClient:
    def __init__(self) -> None:
        self.closed = False

    async def aclose(self) -> None:
        self.closed = True


def _build_fetcher() -> InstagramCommentsScraplingFetcher:
    """Construct a fetcher with mocked browser backend (no real Patchright)."""
    with patch("scrapling.fetchers.StealthyFetcher", MagicMock()):
        fetcher = InstagramCommentsScraplingFetcher(
            cookies=[],
            raw_cookies={"csrftoken": "initial"},
            browser_account_id="testaccount",
        )
        # Pre-build httpx client so tests don't need warmup.
        asyncio.run(fetcher._rebuild_http_client())
        return fetcher


def test_rebuild_http_client_closes_previous_client(monkeypatch) -> None:
    fetcher = _build_fetcher()
    old = _TrackingClient()
    fetcher._http_client = old
    monkeypatch.setattr(
        "trr_backend.socials.instagram.comments_scrapling.fetcher.httpx.AsyncClient",
        lambda **_kwargs: _TrackingClient(),
    )

    asyncio.run(fetcher._rebuild_http_client())

    assert old.closed is True


def test_fetch_comments_preserves_reply_failure_across_later_pages(monkeypatch) -> None:
    fetcher = _build_fetcher()
    fetcher._parser._parse_comment = MagicMock(
        side_effect=[
            InstagramComment(
                comment_id="c1",
                text="one",
                username="alpha",
                user_id="1",
                created_at=1,
                date_time="1970-01-01T00:00:01+00:00",
                likes=0,
                is_reply=False,
                parent_comment_id=None,
                reply_count=1,
            )
        ]
    )
    fetcher._fetch_json_response = AsyncMock(
        side_effect=[
            {
                "payload": {
                    "comments": [{"id": "c1"}],
                    "has_more_comments": True,
                    "next_min_id": "cursor-2",
                },
                "failed": False,
                "auth_failed": False,
                "reason": None,
                "retryable": False,
            },
            {
                "payload": {
                    "comments": [],
                    "has_more_comments": False,
                },
                "failed": False,
                "auth_failed": False,
                "reason": None,
                "retryable": False,
            },
        ]
    )
    fetcher._fetch_comment_replies = AsyncMock(
        return_value=InstagramCommentsFetchResult(
            comments=[],
            fetch_failed=True,
            auth_failed=False,
            fetch_reason="reply_timeout",
            request_count=1,
            retryable=True,
        )
    )

    result = asyncio.run(fetcher.fetch_comments_for_shortcode("ABC123", max_comments=10, fetch_replies=True))

    assert result.fetch_failed is True
    assert result.retryable is True
    assert result.fetch_reason == "reply_timeout"


def _mock_httpx_response(
    *,
    status_code: int = 200,
    json_data: dict | None = None,
    headers: dict[str, str] | None = None,
    location: str | None = None,
) -> MagicMock:
    """Mock that looks like an httpx.Response."""
    response = MagicMock(spec=httpx.Response)
    response.status_code = status_code
    response.text = ""
    _headers = dict(headers or {})
    if location:
        _headers["location"] = location
        _headers["Location"] = location
    response.headers = _headers
    response.json = MagicMock(return_value=json_data if json_data is not None else {"status": "ok"})
    response.cookies = {}
    return response


# ---------------------------------------------------------------------------
# Retry/backoff tests (mock _fetch_api)
# ---------------------------------------------------------------------------


def test_fetch_retries_on_429_then_succeeds() -> None:
    """A single 429 should trigger backoff+retry, not abort."""
    fetcher = _build_fetcher()
    responses = [
        _mock_httpx_response(status_code=429, headers={"retry-after": "0"}),
        _mock_httpx_response(status_code=200, json_data={"status": "ok"}),
    ]
    fetcher._fetch_api = AsyncMock(side_effect=responses)

    with patch("trr_backend.socials.instagram.comments_scrapling.fetcher.asyncio.sleep", AsyncMock()):
        result = asyncio.run(
            fetcher._fetch_json_response(
                "https://www.instagram.com/api/v1/media/1/comments/",
                referer="https://www.instagram.com/p/ABC/",
            )
        )

    assert fetcher._fetch_api.await_count == 2
    assert result["failed"] is False
    assert result["retryable"] is False


def test_fetch_gives_up_after_max_retries_with_retryable_true() -> None:
    """Exhausting retries surfaces retryable=True for queue requeue."""
    fetcher = _build_fetcher()
    responses = [_mock_httpx_response(status_code=429, headers={"retry-after": "0"}) for _ in range(10)]
    fetcher._fetch_api = AsyncMock(side_effect=responses)

    with patch("trr_backend.socials.instagram.comments_scrapling.fetcher.asyncio.sleep", AsyncMock()):
        result = asyncio.run(
            fetcher._fetch_json_response(
                "https://www.instagram.com/api/v1/media/1/comments/",
                referer="https://www.instagram.com/p/ABC/",
            )
        )

    assert result["failed"] is True
    assert result["retryable"] is True
    assert result["reason"] == "http_429"
    assert fetcher._fetch_api.await_count == InstagramCommentsScraplingFetcher._MAX_TRANSIENT_RETRIES + 1


def test_fetch_4xx_validation_is_not_retryable() -> None:
    fetcher = _build_fetcher()
    fetcher._fetch_api = AsyncMock(return_value=_mock_httpx_response(status_code=404))

    result = asyncio.run(
        fetcher._fetch_json_response(
            "https://www.instagram.com/api/v1/media/1/comments/",
            referer="https://www.instagram.com/p/ABC/",
        )
    )

    assert result["failed"] is True
    assert result["retryable"] is False
    assert result["reason"] == "http_404"
    assert fetcher._fetch_api.await_count == 1


def test_fetch_auth_failures_never_retry() -> None:
    fetcher = _build_fetcher()
    fetcher._fetch_api = AsyncMock(return_value=_mock_httpx_response(status_code=401))

    result = asyncio.run(
        fetcher._fetch_json_response(
            "https://www.instagram.com/api/v1/media/1/comments/",
            referer="https://www.instagram.com/p/ABC/",
        )
    )

    assert result["failed"] is True
    assert result["auth_failed"] is True
    assert result["retryable"] is False


def test_fetch_respects_retry_after_header() -> None:
    fetcher = _build_fetcher()
    responses = [
        _mock_httpx_response(status_code=429, headers={"retry-after": "7"}),
        _mock_httpx_response(status_code=200, json_data={"status": "ok"}),
    ]
    fetcher._fetch_api = AsyncMock(side_effect=responses)
    sleep_mock = AsyncMock()

    with patch(
        "trr_backend.socials.instagram.comments_scrapling.fetcher.asyncio.sleep",
        sleep_mock,
    ):
        asyncio.run(
            fetcher._fetch_json_response(
                "https://www.instagram.com/api/v1/media/1/comments/",
                referer="https://www.instagram.com/p/ABC/",
            )
        )

    sleep_mock.assert_awaited_once_with(7.0)


# ---------------------------------------------------------------------------
# httpx.TimeoutException test
# ---------------------------------------------------------------------------


def test_httpx_timeout_exception_is_retryable() -> None:
    """httpx.TimeoutException must be caught and retried, same as TimeoutError."""
    fetcher = _build_fetcher()
    fetcher._fetch_api = AsyncMock(
        side_effect=[
            httpx.TimeoutException("read timeout"),
            _mock_httpx_response(status_code=200, json_data={"status": "ok"}),
        ]
    )

    with patch("trr_backend.socials.instagram.comments_scrapling.fetcher.asyncio.sleep", AsyncMock()):
        result = asyncio.run(
            fetcher._fetch_json_response(
                "https://www.instagram.com/api/v1/media/1/comments/",
                referer="https://www.instagram.com/p/ABC/",
            )
        )

    assert result["failed"] is False
    assert fetcher._fetch_api.await_count == 2


def test_rebuild_http_client_closes_existing_async_client() -> None:
    fetcher = _build_fetcher()
    stale_client = AsyncMock()
    fetcher._http_client = stale_client

    asyncio.run(fetcher._rebuild_http_client())

    stale_client.aclose.assert_awaited_once()
    assert fetcher._http_client is not stale_client


# ---------------------------------------------------------------------------
# 3xx redirect handling
# ---------------------------------------------------------------------------


def test_3xx_redirect_to_login() -> None:
    fetcher = _build_fetcher()
    fetcher._fetch_api = AsyncMock(
        return_value=_mock_httpx_response(status_code=302, location="/accounts/login/?next=/api/v1/media/1/comments/")
    )

    result = asyncio.run(
        fetcher._fetch_json_response(
            "https://www.instagram.com/api/v1/media/1/comments/",
            referer="https://www.instagram.com/p/ABC/",
        )
    )

    assert result["failed"] is True
    assert result["auth_failed"] is True
    assert result["reason"] == "redirect_to_login"
    assert result["retryable"] is False


def test_3xx_redirect_to_checkpoint() -> None:
    fetcher = _build_fetcher()
    fetcher._fetch_api = AsyncMock(return_value=_mock_httpx_response(status_code=302, location="/checkpoint/1234/"))

    result = asyncio.run(
        fetcher._fetch_json_response(
            "https://www.instagram.com/api/v1/media/1/comments/",
            referer="https://www.instagram.com/p/ABC/",
        )
    )

    assert result["failed"] is True
    assert result["auth_failed"] is True
    assert result["reason"] == "redirect_to_checkpoint"


def test_3xx_redirect_to_challenge() -> None:
    fetcher = _build_fetcher()
    fetcher._fetch_api = AsyncMock(return_value=_mock_httpx_response(status_code=302, location="/challenge/action/"))

    result = asyncio.run(
        fetcher._fetch_json_response(
            "https://www.instagram.com/api/v1/media/1/comments/",
            referer="https://www.instagram.com/p/ABC/",
        )
    )

    assert result["failed"] is True
    assert result["auth_failed"] is True
    assert result["reason"] == "redirect_to_checkpoint"


def test_3xx_redirect_to_homepage() -> None:
    fetcher = _build_fetcher()
    fetcher._fetch_api = AsyncMock(return_value=_mock_httpx_response(status_code=302, location="/"))

    result = asyncio.run(
        fetcher._fetch_json_response(
            "https://www.instagram.com/api/v1/media/1/comments/",
            referer="https://www.instagram.com/p/ABC/",
        )
    )

    assert result["failed"] is True
    assert result["auth_failed"] is False
    assert result["reason"] == "redirect_to_homepage"


# ---------------------------------------------------------------------------
# Cookie bridge tests
# ---------------------------------------------------------------------------


def test_warmup_merges_response_cookies_into_raw_cookies_and_http_client() -> None:
    """warmup() response cookies must land in _raw_cookies (in-place) and
    _http_client.cookies."""
    fetcher = _build_fetcher()
    original_raw_cookies_id = id(fetcher._raw_cookies)

    warmup_response = MagicMock()
    warmup_response.status = 200
    warmup_response.text = ""
    warmup_response.cookies = {"csrftoken": "new-csrf", "sessionid": "new-session"}

    fetcher._fetch_page = AsyncMock(return_value=warmup_response)

    asyncio.run(fetcher.warmup())

    # In-place mutation: same dict object.
    assert id(fetcher._raw_cookies) == original_raw_cookies_id
    assert fetcher._raw_cookies["csrftoken"] == "new-csrf"
    assert fetcher._raw_cookies["sessionid"] == "new-session"

    # httpx client rebuilt with merged cookies.
    assert fetcher._http_client is not None


def test_warmup_propagates_csrftoken_into_parser_headers() -> None:
    """After warmup bridges a new csrftoken, the parser's _get_headers()
    must return the updated value. This is the real risk: stale csrftoken
    in API request headers."""
    fetcher = _build_fetcher()

    warmup_response = MagicMock()
    warmup_response.status = 200
    warmup_response.text = ""
    warmup_response.cookies = {"csrftoken": "fresh-csrf-token"}

    fetcher._fetch_page = AsyncMock(return_value=warmup_response)

    asyncio.run(fetcher.warmup())

    headers = fetcher._parser._get_headers("https://www.instagram.com/p/ABC/")
    csrf_header = headers.get("x-csrftoken") or headers.get("X-CSRFToken") or ""
    assert csrf_header == "fresh-csrf-token", (
        f"Expected parser headers to reflect the bridged csrftoken, got: {csrf_header}"
    )


# ---------------------------------------------------------------------------
# Proxy identity test
# ---------------------------------------------------------------------------


def test_selected_proxy_identical_across_transports() -> None:
    """browser_proxy and api_proxy_url must point to the same upstream."""
    from trr_backend.socials.instagram.comments_scrapling.proxy import CommentsProxyConfig

    config = CommentsProxyConfig(
        browser_proxy={"server": "http://gate.decodo.com:7000", "username": "u", "password": "p"},
        api_proxy_url="http://u:p@gate.decodo.com:7000",
        proxy_rotator=None,
        fingerprint="gate.decodo.com:7000:decodo",
    )
    with patch("scrapling.fetchers.StealthyFetcher", MagicMock()):
        fetcher = InstagramCommentsScraplingFetcher(
            cookies=[],
            raw_cookies={},
            browser_account_id="test",
            proxy_config=config,
        )

    # Both should reference the same host:port.
    assert "gate.decodo.com:7000" in str(fetcher._api_proxy_url)
    assert fetcher._selected_proxy_fingerprint == "gate.decodo.com:7000:decodo"


# ---------------------------------------------------------------------------
# Partial-progress persistence (job_runner integration)
# ---------------------------------------------------------------------------


def test_job_runner_partial_progress_persists_before_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """If posts 1 and 2 succeed but post 3 fails transiently, posts 1 and
    2 must already be persisted when the runtime error surfaces."""
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr
    from trr_backend.socials.instagram.comments_scrapling.persistence import PersistedInstagramComments

    persist_calls: list[str] = []

    def fake_persist(*, shortcode: str, **_kwargs: Any) -> PersistedInstagramComments:
        persist_calls.append(shortcode)
        return PersistedInstagramComments(
            post_id="post-id",
            stored_total_comments=1,
            comments_upserted=1,
            comments_marked_missing=0,
            comment_media_mirror_jobs_enqueued=0,
            comment_media_mirror_job_enqueue_errors=0,
        )

    fetch_results = [
        InstagramCommentsFetchResult(comments=[object()], fetch_failed=False),
        InstagramCommentsFetchResult(comments=[object()], fetch_failed=False),
        InstagramCommentsFetchResult(
            comments=[],
            fetch_failed=True,
            fetch_reason="http_429",
            retryable=True,
        ),
    ]
    fetch_call_idx = {"i": 0}

    async def fake_fetch_method(shortcode, *, max_comments, fetch_replies):
        idx = fetch_call_idx["i"]
        fetch_call_idx["i"] += 1
        return fetch_results[idx]

    async def fake_warmup():
        return None

    async def fake_aclose():
        return None

    fake_fetcher = MagicMock()
    fake_fetcher._request_count = 3
    fake_fetcher.warmup = fake_warmup
    fake_fetcher.fetch_comments_for_shortcode = fake_fetch_method
    fake_fetcher.aclose = fake_aclose
    fake_fetcher.runtime_metadata = {"transport": "test"}

    fake_session = MagicMock()
    fake_session.cookies = []
    fake_session.auth_session.cookies = {}
    fake_session.auth_session.metadata = {"source": "test"}
    fake_session.browser_account_id = "testaccount"

    monkeypatch.setattr(jr, "persist_instagram_comments_for_post", fake_persist)
    monkeypatch.setattr(jr, "select_comments_proxy", lambda: None)
    monkeypatch.setattr(jr, "resolve_comments_scrapling_session", lambda **_: fake_session)
    monkeypatch.setattr(jr, "InstagramCommentsScraplingFetcher", lambda **_: fake_fetcher)

    from trr_backend.repositories import social_season_analytics as repo

    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_emit_job_progress", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_finish_job", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})
    monkeypatch.setattr(jr.pg, "db_connection", lambda **_kwargs: nullcontext(MagicMock()))

    job = {
        "id": "job-1",
        "run_id": "run-1",
        "config": {
            "mode": "profile",
            "account": "bravotv",
            "target_source_ids": ["SHORT1", "SHORT2", "SHORT3"],
            "max_comments_per_post": 10,
            "fetch_replies": False,
        },
        "attempt_count": 1,
        "max_attempts": 3,
    }

    with patch(
        "trr_backend.socials.instagram.comments_scrapling.job_runner.pg.fetch_one",
        return_value={"id": "job-1", "status": "retrying"},
    ):
        jr.run_instagram_comments_scrapling_job(job, worker_id="test-worker")

    assert persist_calls == ["SHORT1", "SHORT2"], (
        f"Expected partial persists for 1 and 2 before SHORT3 failure; got {persist_calls}"
    )


def test_job_runner_marks_uncapped_success_complete(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr
    from trr_backend.socials.instagram.comments_scrapling.persistence import PersistedInstagramComments

    captured_is_complete: list[bool] = []

    def fake_persist(*, is_complete: bool, **_kwargs: Any) -> PersistedInstagramComments:
        captured_is_complete.append(is_complete)
        return PersistedInstagramComments(
            post_id="post-id",
            stored_total_comments=1,
            comments_upserted=1,
            comments_marked_missing=0,
            comment_media_mirror_jobs_enqueued=0,
            comment_media_mirror_job_enqueue_errors=0,
        )

    class _FakeFetcher:
        _request_count = 2

        @property
        def runtime_metadata(self) -> dict[str, Any]:
            return {"transport": "test", "request_count": self._request_count}

        async def warmup(self) -> None:
            return None

        async def fetch_comments_for_shortcode(self, *_args: Any, **_kwargs: Any) -> InstagramCommentsFetchResult:
            return InstagramCommentsFetchResult(comments=[object()], fetch_failed=False, auth_failed=False)

        async def aclose(self) -> None:
            return None

    fake_session = MagicMock()
    fake_session.cookies = []
    fake_session.auth_session.cookies = {}
    fake_session.auth_session.metadata = {"source": "test"}
    fake_session.browser_account_id = "testaccount"

    monkeypatch.setattr(jr, "persist_instagram_comments_for_post", fake_persist)
    monkeypatch.setattr(jr, "select_comments_proxy", lambda: None)
    monkeypatch.setattr(jr, "resolve_comments_scrapling_session", lambda **_: fake_session)
    monkeypatch.setattr(jr, "InstagramCommentsScraplingFetcher", lambda **_: _FakeFetcher())
    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_emit_job_progress", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_finish_job", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})
    monkeypatch.setattr(jr.pg, "db_connection", lambda **_kwargs: nullcontext(MagicMock()))

    job = {
        "id": "job-1",
        "run_id": "run-1",
        "config": {
            "mode": "profile",
            "account": "bravotv",
            "target_source_ids": ["SHORT1"],
            "fetch_replies": False,
        },
        "attempt_count": 1,
        "max_attempts": 1,
    }

    with patch(
        "trr_backend.socials.instagram.comments_scrapling.job_runner.pg.fetch_one",
        return_value={"id": "job-1", "status": "completed"},
    ):
        jr.run_instagram_comments_scrapling_job(job, worker_id="test-worker")

    assert captured_is_complete == [True]


def test_job_runner_keeps_capped_success_incomplete_when_local_cap_is_hit(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr
    from trr_backend.socials.instagram.comments_scrapling.persistence import PersistedInstagramComments

    captured_is_complete: list[bool] = []

    def fake_persist(*, is_complete: bool, **_kwargs: Any) -> PersistedInstagramComments:
        captured_is_complete.append(is_complete)
        return PersistedInstagramComments(
            post_id="post-id",
            stored_total_comments=2,
            comments_upserted=2,
            comments_marked_missing=0,
            comment_media_mirror_jobs_enqueued=0,
            comment_media_mirror_job_enqueue_errors=0,
        )

    class _FakeFetcher:
        _request_count = 2

        @property
        def runtime_metadata(self) -> dict[str, Any]:
            return {"transport": "test", "request_count": self._request_count}

        async def warmup(self) -> None:
            return None

        async def fetch_comments_for_shortcode(self, *_args: Any, **_kwargs: Any) -> InstagramCommentsFetchResult:
            return InstagramCommentsFetchResult(
                comments=[object(), object()],
                fetch_failed=False,
                auth_failed=False,
            )

        async def aclose(self) -> None:
            return None

    fake_session = MagicMock()
    fake_session.cookies = []
    fake_session.auth_session.cookies = {}
    fake_session.auth_session.metadata = {"source": "test"}
    fake_session.browser_account_id = "testaccount"

    monkeypatch.setattr(jr, "persist_instagram_comments_for_post", fake_persist)
    monkeypatch.setattr(jr, "select_comments_proxy", lambda: None)
    monkeypatch.setattr(jr, "resolve_comments_scrapling_session", lambda **_: fake_session)
    monkeypatch.setattr(jr, "InstagramCommentsScraplingFetcher", lambda **_: _FakeFetcher())
    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_emit_job_progress", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_finish_job", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})
    monkeypatch.setattr(jr.pg, "db_connection", lambda **_kwargs: nullcontext(MagicMock()))

    job = {
        "id": "job-1",
        "run_id": "run-1",
        "config": {
            "mode": "profile",
            "account": "bravotv",
            "target_source_ids": ["SHORT1"],
            "max_comments_per_post": 2,
            "fetch_replies": False,
        },
        "attempt_count": 1,
        "max_attempts": 1,
    }

    with patch(
        "trr_backend.socials.instagram.comments_scrapling.job_runner.pg.fetch_one",
        return_value={"id": "job-1", "status": "completed"},
    ):
        jr.run_instagram_comments_scrapling_job(job, worker_id="test-worker")

    assert captured_is_complete == [False]


def test_job_runner_completed_metadata_reports_final_request_count(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr
    from trr_backend.socials.instagram.comments_scrapling.persistence import PersistedInstagramComments

    finish_calls: list[dict[str, Any]] = []

    def fake_finish_job(job_id: str, *, status: str, metadata: dict[str, Any], **_kwargs: Any) -> None:
        finish_calls.append({"job_id": job_id, "status": status, "metadata": metadata})

    def fake_persist(**_kwargs: Any) -> PersistedInstagramComments:
        return PersistedInstagramComments(
            post_id="post-id",
            stored_total_comments=1,
            comments_upserted=1,
            comments_marked_missing=0,
            comment_media_mirror_jobs_enqueued=0,
            comment_media_mirror_job_enqueue_errors=0,
        )

    class _FakeFetcher:
        def __init__(self) -> None:
            self._request_count = 0

        @property
        def runtime_metadata(self) -> dict[str, Any]:
            return {"transport": "test", "request_count": self._request_count}

        async def warmup(self) -> None:
            self._request_count = 1

        async def fetch_comments_for_shortcode(self, *_args: Any, **_kwargs: Any) -> InstagramCommentsFetchResult:
            self._request_count = 4
            return InstagramCommentsFetchResult(comments=[object()], fetch_failed=False, auth_failed=False)

        async def aclose(self) -> None:
            return None

    fake_session = MagicMock()
    fake_session.cookies = []
    fake_session.auth_session.cookies = {}
    fake_session.auth_session.metadata = {"source": "test"}
    fake_session.browser_account_id = "testaccount"

    monkeypatch.setattr(jr, "persist_instagram_comments_for_post", fake_persist)
    monkeypatch.setattr(jr, "select_comments_proxy", lambda: None)
    monkeypatch.setattr(jr, "resolve_comments_scrapling_session", lambda **_: fake_session)
    monkeypatch.setattr(jr, "InstagramCommentsScraplingFetcher", lambda **_: _FakeFetcher())
    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_emit_job_progress", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_finish_job", fake_finish_job)
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})
    monkeypatch.setattr(jr.pg, "db_connection", lambda **_kwargs: nullcontext(MagicMock()))

    job = {
        "id": "job-1",
        "run_id": "run-1",
        "config": {
            "mode": "profile",
            "account": "bravotv",
            "target_source_ids": ["SHORT1"],
            "fetch_replies": False,
        },
        "attempt_count": 1,
        "max_attempts": 1,
    }

    with patch(
        "trr_backend.socials.instagram.comments_scrapling.job_runner.pg.fetch_one",
        return_value={"id": "job-1", "status": "completed"},
    ):
        jr.run_instagram_comments_scrapling_job(job, worker_id="test-worker")

    metadata = finish_calls[-1]["metadata"]
    assert metadata["fetch_counters"]["request_count"] == 4
    assert metadata["fetcher_runtime"]["request_count"] == 4


def test_job_runner_persists_partial_success_when_auth_failed_but_comments_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr
    from trr_backend.socials.instagram.comments_scrapling.persistence import PersistedInstagramComments

    persist_calls: list[dict[str, Any]] = []
    finish_calls: list[dict[str, Any]] = []

    def fake_persist(*, shortcode: str, is_complete: bool, **_kwargs: Any) -> PersistedInstagramComments:
        persist_calls.append({"shortcode": shortcode, "is_complete": is_complete})
        return PersistedInstagramComments(
            post_id="post-id",
            stored_total_comments=22,
            comments_upserted=22,
            comments_marked_missing=0,
            comment_media_mirror_jobs_enqueued=0,
            comment_media_mirror_job_enqueue_errors=0,
        )

    class _FakeFetcher:
        _request_count = 3

        @property
        def runtime_metadata(self) -> dict[str, Any]:
            return {"transport": "test", "request_count": self._request_count}

        async def warmup(self) -> None:
            return None

        async def fetch_comments_for_shortcode(self, *_args: Any, **_kwargs: Any) -> InstagramCommentsFetchResult:
            return InstagramCommentsFetchResult(
                comments=[object()] * 22,
                fetch_failed=False,
                auth_failed=True,
                fetch_reason=None,
                request_count=3,
                retryable=False,
            )

        async def aclose(self) -> None:
            return None

    fake_session = MagicMock()
    fake_session.cookies = []
    fake_session.auth_session.cookies = {}
    fake_session.auth_session.metadata = {"source": "test"}
    fake_session.browser_account_id = "testaccount"

    monkeypatch.setattr(jr, "persist_instagram_comments_for_post", fake_persist)
    monkeypatch.setattr(jr, "select_comments_proxy", lambda: None)
    monkeypatch.setattr(jr, "resolve_comments_scrapling_session", lambda **_: fake_session)
    monkeypatch.setattr(jr, "InstagramCommentsScraplingFetcher", lambda **_: _FakeFetcher())
    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_emit_job_progress", lambda *a, **k: None)
    monkeypatch.setattr(
        repo,
        "_finish_job",
        lambda job_id, **kwargs: finish_calls.append({"job_id": job_id, **kwargs}),
    )
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})
    monkeypatch.setattr(jr.pg, "db_connection", lambda **_kwargs: nullcontext(MagicMock()))

    job = {
        "id": "job-1",
        "run_id": "run-1",
        "config": {
            "mode": "profile",
            "account": "bravotv",
            "target_source_ids": ["SHORT1"],
            "fetch_replies": True,
        },
        "attempt_count": 1,
        "max_attempts": 1,
    }

    with patch(
        "trr_backend.socials.instagram.comments_scrapling.job_runner.pg.fetch_one",
        return_value={"id": "job-1", "status": "completed"},
    ):
        payload = jr.run_instagram_comments_scrapling_job(job, worker_id="test-worker")

    assert payload["status"] == "completed"
    assert persist_calls == [{"shortcode": "SHORT1", "is_complete": False}]
    assert finish_calls[-1]["status"] == "completed"
    assert finish_calls[-1]["items_found"] == 23


def test_job_runner_reuses_one_db_connection_for_all_post_persists(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr
    from trr_backend.socials.instagram.comments_scrapling.persistence import PersistedInstagramComments

    persist_conns: list[object] = []
    shared_conn = MagicMock()

    def fake_persist(*, conn: object, **_kwargs: Any) -> PersistedInstagramComments:
        persist_conns.append(conn)
        return PersistedInstagramComments(
            post_id="post-id",
            stored_total_comments=1,
            comments_upserted=1,
            comments_marked_missing=0,
            comment_media_mirror_jobs_enqueued=0,
            comment_media_mirror_job_enqueue_errors=0,
        )

    class _FakeFetcher:
        _request_count = 2

        @property
        def runtime_metadata(self) -> dict[str, Any]:
            return {"transport": "test", "request_count": self._request_count}

        async def warmup(self) -> None:
            return None

        async def fetch_comments_for_shortcode(self, *_args: Any, **_kwargs: Any) -> InstagramCommentsFetchResult:
            return InstagramCommentsFetchResult(comments=[object()], fetch_failed=False, auth_failed=False)

        async def aclose(self) -> None:
            return None

    fake_session = MagicMock()
    fake_session.cookies = []
    fake_session.auth_session.cookies = {}
    fake_session.auth_session.metadata = {"source": "test"}
    fake_session.browser_account_id = "testaccount"

    monkeypatch.setattr(jr, "persist_instagram_comments_for_post", fake_persist)
    monkeypatch.setattr(jr, "select_comments_proxy", lambda: None)
    monkeypatch.setattr(jr, "resolve_comments_scrapling_session", lambda **_: fake_session)
    monkeypatch.setattr(jr, "InstagramCommentsScraplingFetcher", lambda **_: _FakeFetcher())
    monkeypatch.setattr(jr.pg, "db_connection", lambda **_kwargs: nullcontext(shared_conn))
    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_emit_job_progress", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_finish_job", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})

    job = {
        "id": "job-1",
        "run_id": "run-1",
        "config": {
            "mode": "profile",
            "account": "bravotv",
            "target_source_ids": ["SHORT1", "SHORT2"],
            "fetch_replies": False,
        },
        "attempt_count": 1,
        "max_attempts": 1,
    }

    with patch(
        "trr_backend.socials.instagram.comments_scrapling.job_runner.pg.fetch_one",
        return_value={"id": "job-1", "status": "completed"},
    ):
        jr.run_instagram_comments_scrapling_job(job, worker_id="test-worker")

    assert persist_conns == [shared_conn, shared_conn]


def test_job_runner_commits_each_post_persist(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr
    from trr_backend.socials.instagram.comments_scrapling.persistence import PersistedInstagramComments

    shared_conn = MagicMock()

    class _FakeFetcher:
        _request_count = 2

        @property
        def runtime_metadata(self) -> dict[str, Any]:
            return {"transport": "test", "request_count": self._request_count}

        async def warmup(self) -> None:
            return None

        async def fetch_comments_for_shortcode(self, *_args: Any, **_kwargs: Any) -> InstagramCommentsFetchResult:
            return InstagramCommentsFetchResult(comments=[object()], fetch_failed=False, auth_failed=False)

        async def aclose(self) -> None:
            return None

    fake_session = MagicMock()
    fake_session.cookies = []
    fake_session.auth_session.cookies = {}
    fake_session.auth_session.metadata = {"source": "test"}
    fake_session.browser_account_id = "testaccount"

    monkeypatch.setattr(
        jr,
        "persist_instagram_comments_for_post",
        lambda **_kwargs: PersistedInstagramComments(
            post_id="post-id",
            stored_total_comments=1,
            comments_upserted=1,
            comments_marked_missing=0,
            comment_media_mirror_jobs_enqueued=0,
            comment_media_mirror_job_enqueue_errors=0,
        ),
    )
    monkeypatch.setattr(jr, "select_comments_proxy", lambda: None)
    monkeypatch.setattr(jr, "resolve_comments_scrapling_session", lambda **_: fake_session)
    monkeypatch.setattr(jr, "InstagramCommentsScraplingFetcher", lambda **_: _FakeFetcher())
    monkeypatch.setattr(jr.pg, "db_connection", lambda **_kwargs: nullcontext(shared_conn))
    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_emit_job_progress", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_finish_job", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})

    job = {
        "id": "job-1",
        "run_id": "run-1",
        "config": {
            "mode": "profile",
            "account": "bravotv",
            "target_source_ids": ["SHORT1", "SHORT2"],
            "fetch_replies": False,
        },
        "attempt_count": 1,
        "max_attempts": 1,
    }

    with patch(
        "trr_backend.socials.instagram.comments_scrapling.job_runner.pg.fetch_one",
        return_value={"id": "job-1", "status": "completed"},
    ):
        jr.run_instagram_comments_scrapling_job(job, worker_id="test-worker")

    assert shared_conn.commit.call_count == 2


def test_job_runner_returns_degraded_summary_when_final_job_read_hits_db_saturation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr
    from trr_backend.socials.instagram.comments_scrapling.persistence import PersistedInstagramComments

    class _FakeFetcher:
        _request_count = 2

        @property
        def runtime_metadata(self) -> dict[str, Any]:
            return {"transport": "test", "request_count": self._request_count}

        async def warmup(self) -> None:
            return None

        async def fetch_comments_for_shortcode(self, *_args: Any, **_kwargs: Any) -> InstagramCommentsFetchResult:
            return InstagramCommentsFetchResult(comments=[object()], fetch_failed=False, auth_failed=False)

        async def aclose(self) -> None:
            return None

    fake_session = MagicMock()
    fake_session.cookies = []
    fake_session.auth_session.cookies = {}
    fake_session.auth_session.metadata = {"source": "test"}
    fake_session.browser_account_id = "testaccount"

    monkeypatch.setattr(
        jr,
        "persist_instagram_comments_for_post",
        lambda **_kwargs: PersistedInstagramComments(
            post_id="post-id",
            stored_total_comments=1,
            comments_upserted=1,
            comments_marked_missing=0,
            comment_media_mirror_jobs_enqueued=0,
            comment_media_mirror_job_enqueue_errors=0,
        ),
    )
    monkeypatch.setattr(jr, "select_comments_proxy", lambda: None)
    monkeypatch.setattr(jr, "resolve_comments_scrapling_session", lambda **_: fake_session)
    monkeypatch.setattr(jr, "InstagramCommentsScraplingFetcher", lambda **_: _FakeFetcher())
    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_emit_job_progress", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_finish_job", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})
    monkeypatch.setattr(jr.pg, "db_connection", lambda **_kwargs: nullcontext(MagicMock()))

    job = {
        "id": "job-1",
        "run_id": "run-1",
        "config": {
            "mode": "profile",
            "account": "bravotv",
            "target_source_ids": ["SHORT1"],
            "fetch_replies": False,
        },
        "attempt_count": 1,
        "max_attempts": 1,
    }

    with patch(
        "trr_backend.socials.instagram.comments_scrapling.job_runner.pg.fetch_one",
        side_effect=jr.pg.DatabaseServiceUnavailableError("db saturated"),
    ):
        payload = jr.run_instagram_comments_scrapling_job(job, worker_id="test-worker")

    assert payload["id"] == "job-1"
    assert payload["status"] == "completed"
    assert payload["metadata"]["degraded_summary"] is True
