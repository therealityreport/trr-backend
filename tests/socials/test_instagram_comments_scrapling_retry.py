"""P1-5 + P1-6 tests — retry/backoff behavior and partial-progress persistence.

These exercise the comments Scrapling lane's reliability contract:

- Transient HTTP failures (429 / 5xx) trigger exponential backoff inside
  `_fetch_json_response`, bounded by `_MAX_TRANSIENT_RETRIES`.
- If backoff budget is exhausted, the fetch returns `retryable=True` so the
  queue requeues rather than terminally failing.
- Partial progress is preserved: if post N fails after posts 1..N-1 succeed,
  the earlier persists are already in the database when the error surfaces.

Uses the repo's convention of calling `asyncio.run(...)` inside sync test
functions (pytest-asyncio is not part of the toolchain here).
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from trr_backend.socials.instagram.comments_scrapling.fetcher import (
    InstagramCommentsFetchResult,
    InstagramCommentsScraplingFetcher,
)


def _build_fetcher() -> InstagramCommentsScraplingFetcher:
    # StealthyFetcher is imported inside __init__ (lazy) — patch at the
    # source module in scrapling so the fetcher constructs without booting
    # a real browser. The mocked fetcher is only referenced by `_fetch`
    # which we replace per-test anyway.
    with patch("scrapling.fetchers.StealthyFetcher", MagicMock()):
        return InstagramCommentsScraplingFetcher(
            cookies=[],
            raw_cookies={},
            browser_account_id="testaccount",
        )


def _mock_response(*, status: int = 200, json_data: dict | None = None, headers: dict | None = None) -> MagicMock:
    response = MagicMock()
    response.status = status
    response.headers = headers or {}
    response.text = lambda: ""
    response.json = lambda: json_data if json_data is not None else {"status": "ok"}
    return response


def test_fetch_retries_on_429_then_succeeds() -> None:
    """P1-5: A single 429 should trigger backoff+retry, not abort the loop."""
    fetcher = _build_fetcher()
    responses = [
        _mock_response(status=429, headers={"retry-after": "0"}),
        _mock_response(status=200, json_data={"status": "ok", "has_more_comments": False}),
    ]
    fetcher._fetch = AsyncMock(side_effect=responses)

    with patch("trr_backend.socials.instagram.comments_scrapling.fetcher.asyncio.sleep", AsyncMock()):
        result = asyncio.run(
            fetcher._fetch_json_response(
                "https://www.instagram.com/api/v1/media/1/comments/",
                referer="https://www.instagram.com/p/ABC/",
            )
        )

    assert fetcher._fetch.await_count == 2, "Expected one retry after 429"
    assert result["failed"] is False
    assert result["retryable"] is False


def test_fetch_gives_up_after_max_retries_with_retryable_true() -> None:
    """P1-5: Exhausting retries should surface retryable=True so the queue requeues."""
    fetcher = _build_fetcher()
    responses = [_mock_response(status=429, headers={"retry-after": "0"}) for _ in range(10)]
    fetcher._fetch = AsyncMock(side_effect=responses)

    with patch("trr_backend.socials.instagram.comments_scrapling.fetcher.asyncio.sleep", AsyncMock()):
        result = asyncio.run(
            fetcher._fetch_json_response(
                "https://www.instagram.com/api/v1/media/1/comments/",
                referer="https://www.instagram.com/p/ABC/",
            )
        )

    assert result["failed"] is True
    assert result["retryable"] is True, "Transient exhaustion must surface as retryable"
    assert result["auth_failed"] is False
    assert result["reason"] == "http_429"
    # MAX_TRANSIENT_RETRIES=3 → we attempt up to 4 total (initial + 3 retries).
    assert fetcher._fetch.await_count == InstagramCommentsScraplingFetcher._MAX_TRANSIENT_RETRIES + 1


def test_fetch_4xx_validation_is_not_retryable() -> None:
    """P1-5: 400/404 must be terminal — retrying doesn't help and burns proxy quota."""
    fetcher = _build_fetcher()
    fetcher._fetch = AsyncMock(return_value=_mock_response(status=404))

    result = asyncio.run(
        fetcher._fetch_json_response(
            "https://www.instagram.com/api/v1/media/1/comments/",
            referer="https://www.instagram.com/p/ABC/",
        )
    )

    assert result["failed"] is True
    assert result["retryable"] is False
    assert result["reason"] == "http_404"
    assert fetcher._fetch.await_count == 1, "Non-transient 4xx must not retry"


def test_fetch_auth_failures_never_retry() -> None:
    """P1-5: 401/403 set auth_failed=True and retryable=False.
    Retrying with stale cookies just burns budget."""
    fetcher = _build_fetcher()
    fetcher._fetch = AsyncMock(return_value=_mock_response(status=401))

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
    """P1-5: When Instagram sends Retry-After, use it instead of exponential backoff."""
    fetcher = _build_fetcher()
    responses = [
        _mock_response(status=429, headers={"retry-after": "7"}),
        _mock_response(status=200, json_data={"status": "ok"}),
    ]
    fetcher._fetch = AsyncMock(side_effect=responses)
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


def test_job_runner_partial_progress_persists_before_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """P1-6: If posts 1 and 2 succeed but post 3 fails transiently, posts 1 and
    2 must already be persisted when the runtime error surfaces. The queue
    will requeue; earlier work is preserved."""
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

    # First two shortcodes return comments; third returns a transient failure
    # with retryable=True and no comments.
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

    fake_fetcher = MagicMock()
    fake_fetcher._request_count = 3
    fake_fetcher.warmup = fake_warmup
    fake_fetcher.fetch_comments_for_shortcode = fake_fetch_method

    fake_session = MagicMock()
    fake_session.cookies = []
    fake_session.auth_session.cookies = {}
    fake_session.auth_session.metadata = {"source": "test"}
    fake_session.browser_account_id = "testaccount"

    monkeypatch.setattr(jr, "persist_instagram_comments_for_post", fake_persist)
    monkeypatch.setattr(jr, "build_proxy_rotator_from_env", lambda: None)
    monkeypatch.setattr(jr, "resolve_comments_scrapling_session", lambda **_: fake_session)
    monkeypatch.setattr(jr, "InstagramCommentsScraplingFetcher", lambda **_: fake_fetcher)

    # Stub out the heavy queue-state writes; we only care about persist ordering.
    from trr_backend.repositories import social_season_analytics as repo

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
        f"Expected partial persists for posts 1 and 2 before SHORT3 failure; got {persist_calls}"
    )
