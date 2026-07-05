"""Retry/backoff, cookie bridge, redirect handling, and partial-progress tests."""

from __future__ import annotations

import asyncio
import html
import json
import time
from contextlib import contextmanager, nullcontext
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from trr_backend.socials.instagram.comments_scrapling.counts import flattened_comment_count, missing_reply_count
from trr_backend.socials.instagram.comments_scrapling.fetcher import (
    BROWSER_SESSION_INVALIDATED_REASON,
    InstagramCommentsFetchResult,
    InstagramCommentsScraplingFetcher,
    InstagramCommentsWarmupError,
    _extract_graphql_connection_comments,
    _extract_rendered_permalink_comments,
    _pace_global_api_request,
    _post_comments_graphql_doc_ids,
    _record_global_api_cooldown,
    _resolve_optional_positive_int_env,
    _try_advisory_lock_pace,
)
from trr_backend.socials.instagram.constants import resolve_comment_sort_order
from trr_backend.socials.instagram.scraper import InstagramComment

_FIXTURE_DIR = Path(__file__).parents[1] / "fixtures" / "instagram" / "scrapling"


@pytest.fixture(autouse=True)
def _default_legacy_retry_tests_to_authenticated_mode(
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> None:
    monkeypatch.setenv("SOCIAL_INSTAGRAM_SCRAPE_MODE", "authenticated")
    if str(request.node.name or "").startswith("test_completion_residual_gap_"):
        return
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr

    monkeypatch.setattr(jr, "_completion_residual_gap_targets_from_health", lambda **_kwargs: [])


def _fixture_json(name: str) -> dict:
    return json.loads((_FIXTURE_DIR / name).read_text(encoding="utf-8"))


def test_polaris_post_comments_container_query_fixture_parses_first_page() -> None:
    payload = _fixture_json("comments_polaris_container_first_page.json")

    comments, metadata = _extract_graphql_connection_comments(
        payload,
        shortcode="DXKD0wtAHRz",
        post_url="https://www.instagram.com/p/DXKD0wtAHRz/",
    )

    assert len(comments) == 15
    assert flattened_comment_count(comments) == 16
    assert metadata["has_next_page"] is True
    assert metadata["end_cursor"] == '{"cached_comments_cursor":"cursor-15","bifilter_token":"token-15"}'
    assert metadata["top_level_count"] == 15
    assert metadata["flattened_count"] == 16
    assert "26297736713236852" in _post_comments_graphql_doc_ids()

    first = comments[0]
    assert first.comment_id == "17870000000000001"
    assert first.child_comment_count == 1
    assert first.likes == 7
    assert first.is_covered is True
    assert first.is_edited is True
    assert first.parent_comment_id is None
    assert first.owner_fbid_v2 == "fbid-1001"
    assert first.owner_is_unpublished is False
    assert first.restriction_status == "limited"
    assert first.has_translation is True
    assert first.giphy_media_info == {
        "id": "giphy-1",
        "images": {"original": {"url": "https://media.example.invalid/giphy-1.gif"}},
    }
    assert first.replies[0].parent_comment_id == first.comment_id
    assert first.replies[0].owner_fbid_v2 == "fbid-1099"


class _TrackingClient:
    def __init__(self) -> None:
        self.closed = False

    async def aclose(self) -> None:
        self.closed = True


class _SlowSharedClient:
    def __init__(self) -> None:
        self.closed = False
        self.closed_during_request = False
        self.request_started = asyncio.Event()
        self.allow_response = asyncio.Event()

    async def get(self, url: str, **_kwargs: Any) -> httpx.Response:
        self.request_started.set()
        await self.allow_response.wait()
        self.closed_during_request = self.closed
        return httpx.Response(200, json={"status": "ok"}, request=httpx.Request("GET", url))

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


def test_comment_pagination_page_cap_zero_means_uncapped(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SOCIAL_INSTAGRAM_COMMENT_PAGINATION_MAX_PAGES", raising=False)
    assert _resolve_optional_positive_int_env("SOCIAL_INSTAGRAM_COMMENT_PAGINATION_MAX_PAGES", 0) is None

    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENT_PAGINATION_MAX_PAGES", "0")
    assert _resolve_optional_positive_int_env("SOCIAL_INSTAGRAM_COMMENT_PAGINATION_MAX_PAGES", 250) is None

    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENT_PAGINATION_MAX_PAGES", "1")
    assert _resolve_optional_positive_int_env("SOCIAL_INSTAGRAM_COMMENT_PAGINATION_MAX_PAGES", 0) == 1


def test_shared_http_client_rebuild_waits_for_inflight_request(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENT_GLOBAL_THROTTLE", "0")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENT_DELAY_SEC", "0")

    async def scenario() -> None:
        with patch("scrapling.fetchers.StealthyFetcher", MagicMock()):
            fetcher = InstagramCommentsScraplingFetcher(
                cookies=[],
                raw_cookies={"csrftoken": "initial"},
                browser_account_id="testaccount",
            )
        slow_client = _SlowSharedClient()
        fetcher._http_client = slow_client  # type: ignore[assignment]

        request_task = asyncio.create_task(
            fetcher._fetch_api("https://www.instagram.com/api/v1/test/", referer="https://www.instagram.com/p/ABC/")
        )
        await slow_client.request_started.wait()

        rebuild_task = asyncio.create_task(fetcher._rebuild_http_client())
        await asyncio.sleep(0)
        assert rebuild_task.done() is False

        slow_client.allow_response.set()
        response = await request_task
        await rebuild_task

        assert response.status_code == 200
        assert slow_client.closed_during_request is False
        assert slow_client.closed is True
        await fetcher.aclose()

    asyncio.run(scenario())


def test_api_pacing_serializes_concurrent_callers() -> None:
    async def scenario() -> None:
        with patch("scrapling.fetchers.StealthyFetcher", MagicMock()):
            fetcher = InstagramCommentsScraplingFetcher(
                cookies=[],
                raw_cookies={"csrftoken": "initial"},
                browser_account_id="testaccount",
            )

        active_calls = 0
        max_active_calls = 0

        async def fake_unlocked_pace(*, deadline: float | None = None) -> bool:
            nonlocal active_calls, max_active_calls
            active_calls += 1
            max_active_calls = max(max_active_calls, active_calls)
            await asyncio.sleep(0.01)
            active_calls -= 1
            return True

        fetcher._pace_api_requests_unlocked = fake_unlocked_pace  # type: ignore[method-assign]

        results = await asyncio.gather(
            fetcher._pace_api_requests(deadline=None),
            fetcher._pace_api_requests(deadline=None),
        )

        assert results == [True, True]
        assert max_active_calls == 1
        await fetcher.aclose()

    asyncio.run(scenario())


def test_comments_endpoint_probe_accepts_json_response() -> None:
    fetcher = _build_fetcher()
    fetcher._fetch_api = AsyncMock(return_value=httpx.Response(200, json={"comments": [], "status": "ok"}))

    result = asyncio.run(fetcher.validate_comments_endpoint("DXpWUKECX3t", mode="comments_endpoint"))

    assert result["status"] == "valid"
    assert result["mode"] == "comments_endpoint"
    assert fetcher.runtime_metadata["comments_auth_validation"]["status"] == "valid"


def test_comments_endpoint_probe_blocks_login_redirect_when_browser_fallback_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_BROWSER_API_FALLBACK", "false")
    fetcher = _build_fetcher()
    fetcher._fetch_api = AsyncMock(
        return_value=httpx.Response(
            302,
            headers={"location": "https://www.instagram.com/accounts/login/?next=/api/v1/media/1/comments/"},
        )
    )

    result = asyncio.run(fetcher.validate_comments_endpoint("DXpWUKECX3t", mode="comments_endpoint"))

    assert result["status"] == "auth_blocked"
    assert result["reason"] == "redirect_to_login"
    assert result["retryable"] is False


def test_comments_endpoint_probe_uses_browser_fallback_after_login_redirect() -> None:
    fetcher = _build_fetcher()
    fetcher._fetch_api = AsyncMock(
        return_value=httpx.Response(
            302,
            headers={"location": "https://www.instagram.com/accounts/login/?next=/api/v1/media/1/comments/"},
        )
    )
    fetcher._fetch_api_with_browser = AsyncMock(
        return_value=_mock_httpx_response(status_code=200, json_data={"status": "ok", "comments": []})
    )

    result = asyncio.run(fetcher.validate_comments_endpoint("DXpWUKECX3t", mode="comments_endpoint"))

    assert result["status"] == "valid"
    assert fetcher.runtime_metadata["retry_reason_counts"]["browser_api_fallback_after_auth_redirect"] == 1
    fetcher._fetch_api_with_browser.assert_awaited_once()


def test_comments_endpoint_probe_blocks_html_challenge(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_BROWSER_API_FALLBACK", "false")
    fetcher = _build_fetcher()
    fetcher._fetch_api = AsyncMock(return_value=httpx.Response(200, text="<html>checkpoint required</html>"))

    result = asyncio.run(fetcher.validate_comments_endpoint("DXpWUKECX3t", mode="comments_endpoint"))

    assert result["status"] == "auth_blocked"
    assert result["reason"] == "html_challenge_or_auth_required"


@pytest.mark.parametrize(
    "body_text",
    [
        "Your browser session has been invalidated. Please log back in.",
        "For your security, we've logged you out. Please log in again.",
    ],
)
def test_comments_endpoint_probe_classifies_browser_session_invalidation(
    monkeypatch: pytest.MonkeyPatch,
    body_text: str,
) -> None:
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_BROWSER_API_FALLBACK", "true")
    fetcher = _build_fetcher()
    fetcher._fetch_api = AsyncMock(
        return_value=_mock_httpx_response(
            status_code=200,
            text=f"<html><body>{body_text}</body></html>",
        )
    )
    fetcher._fetch_api_with_browser = AsyncMock()

    result = asyncio.run(fetcher.validate_comments_endpoint("DXpWUKECX3t", mode="comments_endpoint"))

    assert result["status"] == "auth_blocked"
    assert result["reason"] == BROWSER_SESSION_INVALIDATED_REASON
    assert result["session_invalidated"] is True
    fetcher._fetch_api_with_browser.assert_not_awaited()
    samples = fetcher.runtime_metadata["challenge_responses"]["samples"]
    assert samples[-1]["session_invalidated"] is True
    assert samples[-1]["text_markers"]["session_invalidated"] is True


def test_comments_endpoint_probe_blocks_checkpoint_json() -> None:
    fetcher = _build_fetcher()
    fetcher._fetch_api = AsyncMock(
        return_value=httpx.Response(200, json={"status": "fail", "message": "checkpoint_required"})
    )

    result = asyncio.run(fetcher.validate_comments_endpoint("DXpWUKECX3t", mode="comments_endpoint"))

    assert result["status"] == "auth_blocked"
    assert result["reason"] == "fail"


def test_comments_endpoint_probe_uses_browser_fallback_after_homepage_redirect() -> None:
    fetcher = _build_fetcher()
    fetcher._fetch_api = AsyncMock(
        return_value=_mock_httpx_response(status_code=302, location="https://www.instagram.com/")
    )
    fetcher._recover_homepage_redirect = AsyncMock(return_value=False)
    fetcher._fetch_api_with_browser = AsyncMock(
        return_value=_mock_httpx_response(status_code=200, json_data={"status": "ok", "comments": []})
    )

    result = asyncio.run(fetcher.validate_comments_endpoint("DXpWUKECX3t", mode="comments_endpoint"))

    assert result["status"] == "valid"
    fetcher._fetch_api.assert_awaited_once()
    fetcher._recover_homepage_redirect.assert_awaited_once()
    fetcher._fetch_api_with_browser.assert_awaited_once()
    assert fetcher.runtime_metadata["retry_reason_counts"]["browser_api_fallback"] == 1


def test_comments_endpoint_probe_treats_timeout_as_transport_block() -> None:
    fetcher = _build_fetcher()
    fetcher._fetch_api = AsyncMock(side_effect=httpx.TimeoutException("timed out"))

    result = asyncio.run(fetcher.validate_comments_endpoint("DXpWUKECX3t", mode="comments_endpoint"))

    assert result["status"] == "transport_blocked"
    assert result["retryable"] is True


def _comment(
    comment_id: str,
    *,
    reply_count: int = 0,
    replies: list[InstagramComment] | None = None,
    is_reply: bool = False,
    parent_comment_id: str | None = None,
) -> InstagramComment:
    return InstagramComment(
        comment_id=comment_id,
        text=comment_id,
        username=f"user_{comment_id}",
        user_id=comment_id,
        created_at=1,
        date_time="1970-01-01T00:00:01+00:00",
        likes=0,
        is_reply=is_reply,
        parent_comment_id=parent_comment_id,
        reply_count=reply_count,
        replies=list(replies or []),
    )


def test_extract_rendered_hidden_comment_from_post_dom() -> None:
    html = """
    <div>
      <a href="/thriller_book_junkie/">profile</a>
      <a href="/p/DXpWUKECX3t/c/18109809979882642/" role="link">
        <time datetime="2026-04-27T21:16:21.000Z">3d</time>
      </a>
      <span dir="auto">
        Rob was the best traitor ever.
        I don't think anyone will ever be able to play this game better than he did
      </span>
      <span dir="auto">2 likes</span>
      <span dir="auto">Reply</span>
    </div>
    """

    comments = _extract_rendered_permalink_comments(
        html,
        shortcode="DXpWUKECX3t",
        post_url="https://www.instagram.com/p/DXpWUKECX3t/",
    )

    assert len(comments) == 1
    assert comments[0].comment_id == "18109809979882642"
    assert comments[0].username == "thriller_book_junkie"
    assert comments[0].text == (
        "Rob was the best traitor ever. I don't think anyone will ever be able to play this game better than he did"
    )
    assert comments[0].likes == 2
    assert comments[0].created_at == int(datetime(2026, 4, 27, 21, 16, 21, tzinfo=UTC).timestamp())
    assert comments[0].is_hidden_by_instagram is True
    assert comments[0].source_snapshot_type == "rendered_hidden_comments"


def test_extract_rendered_hidden_comment_ignores_dynamic_owner_not_literal_handle() -> None:
    html = """
    <div>
      <a href="/thetraitorsus/">commenter profile</a>
      <a href="/bravotv/">post owner hover card</a>
      <a href="/p/DXpWUKECX3t/c/18109809979882643/" role="link">
        <time datetime="2026-04-27T21:16:21.000Z">3d</time>
      </a>
      <span dir="auto">Hidden comment body</span>
    </div>
    """

    comments = _extract_rendered_permalink_comments(
        html,
        shortcode="DXpWUKECX3t",
        post_url="https://www.instagram.com/p/DXpWUKECX3t/",
        ignored_usernames=["bravotv"],
    )

    assert len(comments) == 1
    assert comments[0].username == "thetraitorsus"


def test_extract_rendered_hidden_media_only_comment_from_post_dom() -> None:
    html = """
    <div>
      <a href="/viewer_account/">commenter profile</a>
      <a href="/p/DXpWUKECX3t/c/18109809979882644/" role="link">
        <time datetime="2026-04-27T21:16:21.000Z">3d</time>
      </a>
      <img alt="Comment sticker" src="https://scontent.example/comment-sticker.webp" />
      <span dir="auto">Reply</span>
    </div>
    """

    comments = _extract_rendered_permalink_comments(
        html,
        shortcode="DXpWUKECX3t",
        post_url="https://www.instagram.com/p/DXpWUKECX3t/",
    )

    assert len(comments) == 1
    assert comments[0].comment_id == "18109809979882644"
    assert comments[0].username == "viewer_account"
    assert comments[0].text == ""
    assert comments[0].media_urls == ["https://scontent.example/comment-sticker.webp"]
    assert comments[0].is_hidden_by_instagram is True
    assert comments[0].source_snapshot_type == "rendered_hidden_comments"


def test_fetch_comments_reveals_hidden_comments_when_api_is_short(monkeypatch) -> None:
    fetcher = _build_fetcher()
    visible_comment = InstagramComment(
        comment_id="visible",
        text="visible",
        username="alpha",
        user_id="1",
        created_at=1,
        date_time="1970-01-01 00:00:01",
        likes=0,
        is_reply=False,
        parent_comment_id=None,
        reply_count=0,
    )
    hidden_comment = InstagramComment(
        comment_id="18109809979882642",
        text="Rob was the best traitor ever.",
        username="thriller_book_junkie",
        user_id="",
        created_at=1,
        date_time="1970-01-01 00:00:01",
        likes=2,
        is_reply=False,
        parent_comment_id=None,
        reply_count=0,
    )
    fetcher._parser._parse_comment = MagicMock(return_value=visible_comment)
    fetcher._fetch_json_response = AsyncMock(
        return_value={
            "payload": {"comments": [{"id": "visible"}], "has_more_comments": False},
            "failed": False,
            "auth_failed": False,
            "reason": None,
            "retryable": False,
        }
    )
    fetcher._fetch_rendered_comments_after_revealing_hidden = AsyncMock(return_value=[hidden_comment])

    result = asyncio.run(
        fetcher.fetch_comments_for_shortcode(
            "DXpWUKECX3t",
            max_comments=0,
            fetch_replies=False,
            expected_comment_count=2,
        )
    )

    assert [comment.comment_id for comment in result.comments] == ["visible", "18109809979882642"]
    assert result.fetch_failed is False
    assert result.reported_comment_count == 2
    assert fetcher._fetch_rendered_comments_after_revealing_hidden.await_count == 1
    assert fetcher.runtime_metadata["hidden_comments"]["merged_comments"] == 1


def test_fetch_comments_skips_hidden_reveal_when_api_matches_reported_count(monkeypatch) -> None:
    fetcher = _build_fetcher()
    fetcher._parser._parse_comment = MagicMock(
        return_value=InstagramComment(
            comment_id="visible",
            text="visible",
            username="alpha",
            user_id="1",
            created_at=1,
            date_time="1970-01-01 00:00:01",
            likes=0,
            is_reply=False,
            parent_comment_id=None,
            reply_count=0,
        )
    )
    fetcher._fetch_json_response = AsyncMock(
        return_value={
            "payload": {"comments": [{"id": "visible"}], "has_more_comments": False},
            "failed": False,
            "auth_failed": False,
            "reason": None,
            "retryable": False,
        }
    )
    fetcher._fetch_rendered_comments_after_revealing_hidden = AsyncMock(return_value=[])

    result = asyncio.run(
        fetcher.fetch_comments_for_shortcode(
            "DXpWUKECX3t",
            max_comments=0,
            fetch_replies=False,
            expected_comment_count=1,
        )
    )

    assert len(result.comments) == 1
    fetcher._fetch_rendered_comments_after_revealing_hidden.assert_not_awaited()


def test_fetch_comments_reconciles_tiny_unavailable_hidden_gap_after_reveal(monkeypatch) -> None:
    fetcher = _build_fetcher()
    fetcher._parser._parse_comment = MagicMock(
        return_value=InstagramComment(
            comment_id="visible",
            text="visible",
            username="alpha",
            user_id="1",
            created_at=1,
            date_time="1970-01-01 00:00:01",
            likes=0,
            is_reply=False,
            parent_comment_id=None,
            reply_count=0,
        )
    )
    fetcher._fetch_json_response = AsyncMock(
        return_value={
            "payload": {"comments": [{"id": "visible"}], "has_more_comments": False},
            "failed": False,
            "auth_failed": False,
            "reason": None,
            "retryable": False,
        }
    )
    fetcher._fetch_rendered_comments_after_revealing_hidden = AsyncMock(return_value=[])

    result = asyncio.run(
        fetcher.fetch_comments_for_shortcode(
            "DXpWUKECX3t",
            max_comments=0,
            fetch_replies=False,
            expected_comment_count=3,
        )
    )

    # SA-1 (comment-completeness): the hidden-unavailable gap tolerance is now 0
    # and the baked `target<=3 and gap<=2` freebie was removed. With expected=3 and
    # only 1 visible comment (gap=2), the post is no longer blessed
    # "unavailable_reconciled" — it stays incomplete/retryable below target. The
    # reveal is still attempted, then the post is re-driven instead of reconciled.
    assert len(result.comments) == 1
    assert result.fetch_failed is True
    assert result.retryable is True
    assert result.fetch_reason == "hidden_comments_unresolved"
    fetcher._fetch_rendered_comments_after_revealing_hidden.assert_awaited_once()


def test_reply_only_classifies_missing_replies_when_reply_api_is_exhausted() -> None:
    fetcher = _build_fetcher()
    parent = _comment(
        "parent",
        reply_count=3,
        replies=[_comment("reply-1", is_reply=True, parent_comment_id="parent")],
    )
    fetcher._fetch_comment_replies = AsyncMock(
        return_value=InstagramCommentsFetchResult(
            comments=[],
            fetch_failed=False,
            auth_failed=False,
            fetch_reason=None,
            retryable=False,
        )
    )

    # SA-1 (comment-completeness): coverage_terminal_missing_classified is now
    # gated behind attempt_count >= 3 no-progress passes. Below that the post stays
    # reply_tail_incomplete/retryable; only on the exhausted attempt is the missing
    # reply terminally classified. Drive attempt_count=3 to assert the gated
    # terminal-missing classification this test is about.
    result = asyncio.run(
        fetcher._fetch_persisted_reply_tails(
            shortcode="DXpWUKECX3t",
            media_id="123",
            post_url="https://www.instagram.com/p/DXpWUKECX3t/",
            max_comments=0,
            fetch_replies=True,
            expected_comment_count=4,
            persisted_top_level_comments=[parent],
            persisted_replies_by_parent_id={},
            reply_resume_cursors_by_parent={},
            reply_resume_cursor_params_by_parent={},
            deadline=time.monotonic() + 30,
            reply_tail_deadline=time.monotonic() + 30,
            attempt_count=3,
        )
    )

    assert result.fetch_failed is False
    assert result.retryable is False
    assert result.fetch_reason == "coverage_terminal_missing_classified"
    assert result.diagnostic_metadata["missing_reason_counts"]["instagram_not_served_after_all_lanes"] == 2
    assert result.comments[0].reply_count == 1
    assert missing_reply_count(result.comments) == 0


def test_fetch_comments_uses_flattened_count_for_expected_total(monkeypatch) -> None:
    fetcher = _build_fetcher()
    top_level_comments = [_comment(f"c{i}", reply_count=1) for i in range(50)]
    fetcher._parser._parse_comment = MagicMock(side_effect=top_level_comments)
    fetcher._fetch_json_response = AsyncMock(
        return_value={
            "payload": {
                "comments": [{"id": comment.comment_id} for comment in top_level_comments],
                "has_more_comments": False,
            },
            "failed": False,
            "auth_failed": False,
            "reason": None,
            "retryable": False,
        }
    )

    async def fake_replies(**kwargs):
        parent_id = kwargs["comment_id"]
        return InstagramCommentsFetchResult(
            comments=[_comment(f"{parent_id}-r1", is_reply=True, parent_comment_id=parent_id)],
            fetch_failed=False,
            auth_failed=False,
            retryable=False,
        )

    fetcher._fetch_comment_replies = AsyncMock(side_effect=fake_replies)
    fetcher._fetch_rendered_comments_after_revealing_hidden = AsyncMock(return_value=[])

    result = asyncio.run(
        fetcher.fetch_comments_for_shortcode(
            "ABC123",
            max_comments=0,
            fetch_replies=True,
            expected_comment_count=100,
        )
    )

    assert flattened_comment_count(result.comments) == 100
    assert result.fetch_failed is False
    assert result.fetch_reason is None
    fetcher._fetch_rendered_comments_after_revealing_hidden.assert_not_awaited()


def test_flattened_count_ignores_idless_comments_that_cannot_be_persisted() -> None:
    comments = [
        _comment("c1"),
        _comment(""),
        _comment("c2", replies=[_comment("", is_reply=True), _comment("r1", is_reply=True)]),
        _comment("c1"),
    ]

    assert flattened_comment_count(comments) == 3


def test_comments_scrape_is_complete_accepts_reconciled_hidden_unavailable_gap() -> None:
    from trr_backend.socials.instagram.comments_scrapling.job_runner import _comments_scrape_is_complete

    result = InstagramCommentsFetchResult(
        comments=[_comment("c1")],
        fetch_failed=False,
        auth_failed=False,
        fetch_reason="hidden_comments_unavailable_reconciled",
        reported_comment_count=3,
        retryable=False,
    )

    assert _comments_scrape_is_complete(result=result, max_comments_per_post=0) is True


def test_parentless_fetched_reply_prevents_complete_status() -> None:
    from trr_backend.socials.instagram.comments_scrapling.job_runner import _comments_scrape_is_complete

    result = InstagramCommentsFetchResult(
        comments=[_comment("r1", is_reply=True, parent_comment_id="missing-parent")],
        fetch_failed=False,
        auth_failed=False,
        fetch_reason=None,
        reported_comment_count=1,
        retryable=False,
    )

    assert _comments_scrape_is_complete(result=result, max_comments_per_post=0) is False


def test_fetch_comments_reports_parentless_reply_attach_failed() -> None:
    fetcher = _build_fetcher()
    fetcher._parser._parse_comment = MagicMock(
        return_value=_comment("r1", is_reply=True, parent_comment_id="missing-parent")
    )
    fetcher._fetch_json_response = AsyncMock(
        return_value={
            "payload": {
                "comments": [{"id": "r1"}],
                "has_more_comments": False,
            },
            "failed": False,
            "auth_failed": False,
            "reason": None,
            "retryable": False,
        }
    )
    fetcher._fetch_rendered_comments_after_revealing_hidden = AsyncMock(return_value=[])

    result = asyncio.run(
        fetcher.fetch_comments_for_shortcode(
            "ABC123",
            max_comments=0,
            fetch_replies=False,
            expected_comment_count=1,
        )
    )

    assert result.fetch_failed is True
    assert result.retryable is True
    assert result.fetch_reason == "parentless_reply_attach_failed"
    assert result.diagnostic_metadata["parentless_reply_ids"] == ["r1"]


def test_fetch_comments_fetches_tail_when_preview_replies_are_short(monkeypatch) -> None:
    fetcher = _build_fetcher()
    preview_replies = [_comment(f"r{i}", is_reply=True, parent_comment_id="c1") for i in range(20)]
    fetcher._parser._parse_comment = MagicMock(return_value=_comment("c1", reply_count=50, replies=preview_replies))
    fetcher._fetch_json_response = AsyncMock(
        return_value={
            "payload": {
                "comments": [{"id": "c1"}],
                "has_more_comments": False,
            },
            "failed": False,
            "auth_failed": False,
            "reason": None,
            "retryable": False,
        }
    )
    tail_replies = [_comment(f"r{i}", is_reply=True, parent_comment_id="c1") for i in range(20, 50)]
    fetcher._fetch_comment_replies = AsyncMock(
        return_value=InstagramCommentsFetchResult(
            comments=tail_replies,
            fetch_failed=False,
            auth_failed=False,
            retryable=False,
        )
    )
    fetcher._fetch_rendered_comments_after_revealing_hidden = AsyncMock(return_value=[])

    result = asyncio.run(
        fetcher.fetch_comments_for_shortcode(
            "ABC123",
            max_comments=0,
            fetch_replies=True,
            expected_comment_count=51,
        )
    )

    assert flattened_comment_count(result.comments) == 51
    assert missing_reply_count(result.comments) == 0
    assert fetcher._fetch_comment_replies.await_count == 1
    fetcher._fetch_rendered_comments_after_revealing_hidden.assert_not_awaited()


def test_fetch_comments_skips_hidden_reveal_when_reply_tail_gap_explains_shortfall(monkeypatch) -> None:
    fetcher = _build_fetcher()
    preview_replies = [_comment(f"r{i}", is_reply=True, parent_comment_id="c1") for i in range(20)]
    fetcher._parser._parse_comment = MagicMock(return_value=_comment("c1", reply_count=50, replies=preview_replies))
    fetcher._fetch_json_response = AsyncMock(
        return_value={
            "payload": {
                "comments": [{"id": "c1"}],
                "has_more_comments": False,
            },
            "failed": False,
            "auth_failed": False,
            "reason": None,
            "retryable": False,
        }
    )
    fetcher._fetch_comment_replies = AsyncMock(
        return_value=InstagramCommentsFetchResult(
            comments=[],
            fetch_failed=False,
            auth_failed=False,
            retryable=False,
        )
    )
    fetcher._fetch_rendered_comments_after_revealing_hidden = AsyncMock(return_value=[])

    result = asyncio.run(
        fetcher.fetch_comments_for_shortcode(
            "ABC123",
            max_comments=0,
            fetch_replies=True,
            expected_comment_count=51,
        )
    )

    assert flattened_comment_count(result.comments) == 21
    assert missing_reply_count(result.comments) == 30
    assert result.fetch_failed is True
    assert result.retryable is True
    assert result.fetch_reason == "reply_tail_incomplete"
    fetcher._fetch_rendered_comments_after_revealing_hidden.assert_not_awaited()


def test_fetch_comments_reply_only_retries_when_saved_parent_reply_tail_is_still_short() -> None:
    fetcher = _build_fetcher()
    parent = _comment("c1", reply_count=3)
    existing_reply = _comment("r1", is_reply=True, parent_comment_id="c1")
    fetched_reply = _comment("r2", is_reply=True, parent_comment_id="c1")
    fetcher._fetch_json_response = AsyncMock()
    fetcher._fetch_comment_replies = AsyncMock(
        return_value=InstagramCommentsFetchResult(comments=[fetched_reply], fetch_failed=False, auth_failed=False)
    )

    result = asyncio.run(
        fetcher.fetch_comments_for_shortcode(
            "ABC123",
            max_comments=0,
            fetch_replies=True,
            expected_comment_count=10,
            persisted_replies_by_parent={"c1": [existing_reply]},
            persisted_top_level_comments=[parent],
            reply_only=True,
        )
    )

    fetcher._fetch_json_response.assert_not_awaited()
    fetcher._fetch_comment_replies.assert_awaited_once()
    assert [reply.comment_id for reply in result.comments[0].replies] == ["r1", "r2"]
    assert result.fetch_failed is True
    assert result.retryable is True
    assert result.fetch_reason == "reply_tail_incomplete"


def test_fetch_comments_checkpoints_remaining_replies_after_reply_tail_budget(monkeypatch) -> None:
    clock = {"now": 100.0}
    monkeypatch.setenv("SOCIAL_INSTAGRAM_REPLY_TAIL_TOTAL_MAX_SECONDS_PER_POST", "1")
    fetcher = _build_fetcher()
    monkeypatch.setattr(
        "trr_backend.socials.instagram.comments_scrapling.fetcher.time.monotonic",
        lambda: clock["now"],
    )
    fetcher._parser._parse_comment = MagicMock(
        side_effect=[
            _comment("c1", reply_count=1),
            _comment("c2", reply_count=1),
        ]
    )
    fetcher._fetch_json_response = AsyncMock(
        return_value={
            "payload": {
                "comments": [{"id": "c1"}, {"id": "c2"}],
                "has_more_comments": False,
            },
            "failed": False,
            "auth_failed": False,
            "reason": None,
            "retryable": False,
        }
    )

    async def fake_replies(**kwargs):
        parent_id = kwargs["comment_id"]
        clock["now"] = 102.0
        return InstagramCommentsFetchResult(
            comments=[_comment(f"{parent_id}-r1", is_reply=True, parent_comment_id=parent_id)],
            fetch_failed=False,
            auth_failed=False,
            retryable=False,
        )

    fetcher._fetch_comment_replies = AsyncMock(side_effect=fake_replies)
    fetcher._fetch_rendered_comments_after_revealing_hidden = AsyncMock(return_value=[])

    result = asyncio.run(
        fetcher.fetch_comments_for_shortcode(
            "ABC123",
            max_comments=0,
            fetch_replies=True,
            expected_comment_count=4,
        )
    )

    assert flattened_comment_count(result.comments) == 3
    assert missing_reply_count(result.comments) == 1
    assert result.fetch_failed is True
    assert result.retryable is True
    assert result.fetch_reason == "reply_tail_budget_exhausted"
    assert fetcher._fetch_comment_replies.await_count == 1
    assert result.reply_checkpoints[-1]["stop_reason"] == "reply_tail_budget_exhausted"
    fetcher._fetch_rendered_comments_after_revealing_hidden.assert_not_awaited()


def test_reply_tail_budget_gap_below_target_is_not_reconcilable() -> None:
    # SA-2 (comment-completeness): the reconcilable reported-count gap tolerance is
    # now 0 (was 1 absolute / ratio). A reply_tail_budget_exhausted stop that is
    # even a single comment below the reported target is no longer blessed
    # "complete-enough" — the post stays incomplete/retryable so the remaining
    # comment is re-driven. (The reply-topology-gap guard still independently
    # blocks reconciliation regardless of the count gap.)
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr

    result = InstagramCommentsFetchResult(
        comments=[_comment("c1")],
        fetch_failed=True,
        auth_failed=False,
        fetch_reason="reply_tail_budget_exhausted",
        reported_comment_count=100,
        retryable=True,
    )

    # A 1-comment gap (99 stored vs 100 reported) is no longer tolerable.
    assert not jr._persisted_comment_coverage_gap_is_reconcilable(
        result=result,
        stored_total_comments=99,
        max_comments_per_post=0,
    )
    # And a persisted reply-topology gap still independently blocks reconciliation.
    assert not jr._persisted_comment_coverage_gap_is_reconcilable(
        result=result,
        stored_total_comments=99,
        stored_reply_gap_total=1,
        max_comments_per_post=0,
    )


def test_reply_only_auth_blocked_gap_can_reconcile_stored_coverage(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr

    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_RECONCILABLE_GAP_MAX", "1")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_RECONCILABLE_GAP_RATIO", "0.25")

    result = InstagramCommentsFetchResult(
        comments=[_comment("c1")],
        fetch_failed=True,
        auth_failed=True,
        fetch_reason="html_challenge_or_auth_required",
        reported_comment_count=100,
        retryable=False,
    )

    assert jr._reply_only_auth_blocked_coverage_gap_is_reconcilable(
        result=result,
        stored_total_comments=80,
        max_comments_per_post=0,
        reply_only=True,
    )
    assert not jr._reply_only_auth_blocked_coverage_gap_is_reconcilable(
        result=result,
        stored_total_comments=74,
        max_comments_per_post=0,
        reply_only=True,
    )


def test_reply_only_auth_blocked_gap_reconciliation_requires_reply_only_strategy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr

    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_RECONCILABLE_GAP_RATIO", "0.25")

    result = InstagramCommentsFetchResult(
        comments=[_comment("c1")],
        fetch_failed=True,
        auth_failed=True,
        fetch_reason="html_challenge_or_auth_required",
        reported_comment_count=100,
        retryable=False,
        diagnostic_metadata={"strategy_decision": {"reply_only": False}},
    )

    assert not jr._reply_only_auth_blocked_coverage_gap_is_reconcilable(
        result=result,
        stored_total_comments=90,
        max_comments_per_post=0,
    )


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


def test_rebuild_http_client_forces_safe_accept_encoding(monkeypatch) -> None:
    fetcher = _build_fetcher()
    captured: dict[str, Any] = {}

    def _client_factory(**kwargs: Any) -> _TrackingClient:
        captured.update(kwargs)
        return _TrackingClient()

    monkeypatch.setattr(
        "trr_backend.socials.instagram.comments_scrapling.fetcher.httpx.AsyncClient",
        _client_factory,
    )

    asyncio.run(fetcher._rebuild_http_client())

    assert captured["headers"]["accept-encoding"] == "gzip, deflate"


def test_fetch_api_forces_safe_accept_encoding() -> None:
    fetcher = _build_fetcher()
    captured: dict[str, Any] = {}

    class _Client:
        async def get(self, url: str, *, params: dict[str, Any] | None = None, headers: dict[str, str] | None = None):
            captured["url"] = url
            captured["params"] = params
            captured["headers"] = headers
            return httpx.Response(200, json={"status": "ok"}, request=httpx.Request("GET", url))

        async def aclose(self) -> None:
            return None

    fetcher._http_client = _Client()  # type: ignore[assignment]
    fetcher._parser.get_headers = MagicMock(
        return_value={
            "accept-encoding": "gzip, deflate, br, zstd",
            "x-ig-app-id": "936619743392459",
        }
    )

    asyncio.run(
        fetcher._fetch_api(
            "https://www.instagram.com/api/v1/media/123/comments/",
            referer="https://www.instagram.com/p/test/",
            params={"max_id": None, "can_support_threading": "true"},
        )
    )

    assert captured["headers"]["accept-encoding"] == "gzip, deflate"
    assert "zstd" not in captured["headers"]["accept-encoding"]
    assert captured["headers"]["x-ig-app-id"] == "936619743392459"
    assert captured["params"] == {"can_support_threading": "true"}


def test_fetch_api_with_browser_strips_accept_encoding() -> None:
    fetcher = _build_fetcher()
    fetcher._parser.get_headers = MagicMock(
        return_value={
            "accept-encoding": "gzip, deflate, br, zstd",
            "x-ig-app-id": "936619743392459",
        }
    )
    fetcher._fetcher.async_fetch = AsyncMock(
        return_value=httpx.Response(
            200,
            json={"status": "ok"},
            request=httpx.Request("GET", "https://www.instagram.com/api/v1/media/123/comments/"),
        )
    )
    fetcher._rebuild_http_client = AsyncMock()

    asyncio.run(
        fetcher._fetch_api_with_browser(
            "https://www.instagram.com/api/v1/media/123/comments/",
            referer="https://www.instagram.com/p/test/",
        )
    )

    extra_headers = fetcher._fetcher.async_fetch.call_args.kwargs["extra_headers"]
    assert "accept-encoding" not in {key.lower(): value for key, value in extra_headers.items()}
    assert extra_headers["x-ig-app-id"] == "936619743392459"


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


def test_fetch_comments_continues_reply_requests_after_retryable_parent_failure(monkeypatch) -> None:
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
            ),
            InstagramComment(
                comment_id="c2",
                text="two",
                username="beta",
                user_id="2",
                created_at=2,
                date_time="1970-01-01T00:00:02+00:00",
                likes=0,
                is_reply=False,
                parent_comment_id=None,
                reply_count=1,
            ),
        ]
    )
    fetcher._fetch_json_response = AsyncMock(
        return_value={
            "payload": {
                "comments": [{"id": "c1"}, {"id": "c2"}],
                "has_more_comments": False,
            },
            "failed": False,
            "auth_failed": False,
            "reason": None,
            "retryable": False,
        }
    )
    fetcher._fetch_comment_replies = AsyncMock(
        return_value=InstagramCommentsFetchResult(
            comments=[],
            fetch_failed=True,
            auth_failed=False,
            fetch_reason="http_429",
            request_count=1,
            retryable=True,
        )
    )

    result = asyncio.run(fetcher.fetch_comments_for_shortcode("ABC123", max_comments=10, fetch_replies=True))

    assert result.fetch_failed is True
    assert result.retryable is True
    assert result.fetch_reason == "http_429"
    assert len(result.comments) == 2
    assert fetcher._fetch_comment_replies.await_count == 2
    assert "reply_fetch_circuit_open" not in fetcher.runtime_metadata["retry_reason_counts"]


def test_fetch_comments_dedupes_top_level_parents_before_reply_fetch(monkeypatch) -> None:
    fetcher = _build_fetcher()
    duplicate_parent = InstagramComment(
        comment_id="c1",
        text="one",
        username="alpha",
        user_id="1",
        created_at=1,
        date_time="1970-01-01T00:00:01+00:00",
        likes=0,
        is_reply=False,
        parent_comment_id=None,
        reply_count=2,
    )
    fetcher._parser.parse_comment = MagicMock(return_value=duplicate_parent)
    fetcher._fetch_json_response = AsyncMock(
        side_effect=[
            {
                "payload": {
                    "comments": [{"id": "c1"}],
                    "has_more_comments": True,
                    "next_min_id": "cursor-1",
                },
                "failed": False,
                "auth_failed": False,
                "reason": None,
                "retryable": False,
            },
            {
                "payload": {
                    "comments": [{"id": "c1"}],
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
            comments=[
                _comment("r1", is_reply=True, parent_comment_id="c1"),
                _comment("r2", is_reply=True, parent_comment_id="c1"),
            ],
            fetch_failed=False,
            auth_failed=False,
            fetch_reason=None,
            request_count=1,
            retryable=False,
        )
    )

    result = asyncio.run(fetcher.fetch_comments_for_shortcode("ABC123", max_comments=10, fetch_replies=True))

    assert result.fetch_failed is False
    assert flattened_comment_count(result.comments) == 3
    assert [comment.comment_id for comment in result.comments] == ["c1"]
    assert [reply.comment_id for reply in result.comments[0].replies] == ["r1", "r2"]
    assert fetcher._fetch_comment_replies.await_count == 1


def test_fetch_comments_repeated_cursor_without_expected_count_is_retryable(monkeypatch) -> None:
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
                reply_count=0,
            ),
            InstagramComment(
                comment_id="c2",
                text="two",
                username="beta",
                user_id="2",
                created_at=2,
                date_time="1970-01-01T00:00:02+00:00",
                likes=0,
                is_reply=False,
                parent_comment_id=None,
                reply_count=0,
            ),
        ]
    )
    fetcher._fetch_json_response = AsyncMock(
        side_effect=[
            {
                "payload": {
                    "comments": [{"id": "c1"}],
                    "has_more_comments": True,
                    "next_min_id": "cursor-1",
                },
                "failed": False,
                "auth_failed": False,
                "reason": None,
                "retryable": False,
            },
            {
                "payload": {
                    "comments": [{"id": "c2"}],
                    "has_more_comments": True,
                    "next_min_id": "cursor-1",
                },
                "failed": False,
                "auth_failed": False,
                "reason": None,
                "retryable": False,
            },
        ]
    )

    result = asyncio.run(fetcher.fetch_comments_for_shortcode("ABC123", max_comments=10, fetch_replies=True))

    assert len(result.comments) == 2
    assert result.fetch_failed is True
    assert result.retryable is True
    assert result.fetch_reason == "pagination_repeated_cursor"


def test_fetch_comments_marks_repeated_cursor_retryable_when_expected_gap_remains(monkeypatch) -> None:
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
                reply_count=0,
            ),
            InstagramComment(
                comment_id="c2",
                text="two",
                username="beta",
                user_id="2",
                created_at=2,
                date_time="1970-01-01T00:00:02+00:00",
                likes=0,
                is_reply=False,
                parent_comment_id=None,
                reply_count=0,
            ),
        ]
    )
    fetcher._fetch_json_response = AsyncMock(
        side_effect=[
            {
                "payload": {
                    "comments": [{"id": "c1"}],
                    "has_more_comments": True,
                    "next_min_id": "cursor-1",
                },
                "failed": False,
                "auth_failed": False,
                "reason": None,
                "retryable": False,
            },
            {
                "payload": {
                    "comments": [{"id": "c2"}],
                    "has_more_comments": True,
                    "next_min_id": "cursor-1",
                },
                "failed": False,
                "auth_failed": False,
                "reason": None,
                "retryable": False,
            },
        ]
    )
    fetcher._fetch_rendered_comments_after_revealing_hidden = AsyncMock(return_value=[])

    result = asyncio.run(
        fetcher.fetch_comments_for_shortcode(
            "ABC123",
            max_comments=10,
            fetch_replies=True,
            expected_comment_count=3,
        )
    )

    assert len(result.comments) == 2
    assert result.fetch_failed is True
    assert result.retryable is True
    assert result.fetch_reason == "pagination_repeated_cursor"
    fetcher._fetch_rendered_comments_after_revealing_hidden.assert_awaited_once()


def test_fetch_comments_repeated_cursor_reveals_hidden_comments_before_retrying(monkeypatch) -> None:
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
                reply_count=0,
            ),
            InstagramComment(
                comment_id="c2",
                text="two",
                username="beta",
                user_id="2",
                created_at=2,
                date_time="1970-01-01T00:00:02+00:00",
                likes=0,
                is_reply=False,
                parent_comment_id=None,
                reply_count=0,
            ),
        ]
    )
    fetcher._fetch_json_response = AsyncMock(
        side_effect=[
            {
                "payload": {
                    "comments": [{"id": "c1"}],
                    "has_more_comments": True,
                    "next_min_id": "cursor-1",
                },
                "failed": False,
                "auth_failed": False,
                "reason": None,
                "retryable": False,
            },
            {
                "payload": {
                    "comments": [{"id": "c2"}],
                    "has_more_comments": True,
                    "next_min_id": "cursor-1",
                },
                "failed": False,
                "auth_failed": False,
                "reason": None,
                "retryable": False,
            },
        ]
    )
    fetcher._fetch_rendered_comments_after_revealing_hidden = AsyncMock(
        return_value=[
            InstagramComment(
                comment_id="c3",
                text="hidden",
                username="gamma",
                user_id="3",
                created_at=3,
                date_time="1970-01-01T00:00:03+00:00",
                likes=0,
                is_reply=False,
                parent_comment_id=None,
                reply_count=0,
            )
        ]
    )

    result = asyncio.run(
        fetcher.fetch_comments_for_shortcode(
            "ABC123",
            max_comments=10,
            fetch_replies=True,
            expected_comment_count=3,
        )
    )

    assert len(result.comments) == 3
    assert result.fetch_failed is False
    assert result.retryable is False
    assert result.fetch_reason == "hidden_comments_recovered"
    assert fetcher.runtime_metadata["hidden_comments"]["merged_comments"] == 1
    fetcher._fetch_rendered_comments_after_revealing_hidden.assert_awaited_once()


def test_fetch_comments_repeated_cursor_is_complete_when_expected_count_met(monkeypatch) -> None:
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
                reply_count=0,
            ),
            InstagramComment(
                comment_id="c2",
                text="two",
                username="beta",
                user_id="2",
                created_at=2,
                date_time="1970-01-01T00:00:02+00:00",
                likes=0,
                is_reply=False,
                parent_comment_id=None,
                reply_count=0,
            ),
        ]
    )
    fetcher._fetch_json_response = AsyncMock(
        side_effect=[
            {
                "payload": {
                    "comments": [{"id": "c1"}],
                    "has_more_comments": True,
                    "next_min_id": "cursor-1",
                },
                "failed": False,
                "auth_failed": False,
                "reason": None,
                "retryable": False,
            },
            {
                "payload": {
                    "comments": [{"id": "c2"}],
                    "has_more_comments": True,
                    "next_min_id": "cursor-1",
                },
                "failed": False,
                "auth_failed": False,
                "reason": None,
                "retryable": False,
            },
        ]
    )
    fetcher._fetch_rendered_comments_after_revealing_hidden = AsyncMock(return_value=[])

    result = asyncio.run(
        fetcher.fetch_comments_for_shortcode(
            "ABC123",
            max_comments=10,
            fetch_replies=True,
            expected_comment_count=2,
        )
    )

    assert flattened_comment_count(result.comments) == 2
    assert result.fetch_failed is False
    assert result.retryable is False
    # SA-1 (comment-completeness): coverage_target_met now requires genuine cursor
    # exhaustion (api_top_level_complete), not target alone. Here pagination ended
    # on a repeated cursor (genuine end-of-pagination) with the target met, so the
    # post is still terminal/complete but surfaces the real terminal reason
    # (pagination_repeated_cursor) instead of being rewritten to coverage_target_met.
    assert result.fetch_reason == "pagination_repeated_cursor"
    fetcher._fetch_rendered_comments_after_revealing_hidden.assert_not_awaited()


def test_fetch_comments_ignores_page_cap_env_and_stops_on_repeated_cursor(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENT_PAGINATION_MAX_PAGES", "1")
    fetcher = _build_fetcher()
    fetcher._parser._parse_comment = MagicMock(return_value=_comment("c1"))
    fetcher._fetch_json_response = AsyncMock(
        return_value={
            "payload": {
                "comments": [{"id": "c1"}],
                "has_more_comments": True,
                "next_min_id": "cursor-2",
            },
            "failed": False,
            "auth_failed": False,
            "reason": None,
            "retryable": False,
        }
    )
    fetcher._fetch_rendered_comments_after_revealing_hidden = AsyncMock(return_value=[])

    result = asyncio.run(
        fetcher.fetch_comments_for_shortcode(
            "ABC123",
            max_comments=0,
            fetch_replies=False,
            expected_comment_count=10,
        )
    )

    assert result.fetch_failed is True
    assert result.retryable is True
    assert result.fetch_reason == "pagination_repeated_cursor"
    assert result.top_level_checkpoint is not None
    assert result.top_level_checkpoint["target_shortcode"] == "ABC123"
    assert result.top_level_checkpoint["last_top_level_cursor"] == "cursor-2"
    assert result.top_level_checkpoint["pages_seen"] == 2
    assert fetcher.runtime_metadata["top_level_checkpoint_metadata"]["items"][-1]["last_top_level_cursor"] == "cursor-2"


def test_fetch_comments_deadline_inside_page_records_response_next_cursor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENT_PAGINATION_MAX_SECONDS", "1")
    fetcher = _build_fetcher()
    clock_state = {"expired": False}
    monkeypatch.setattr(
        "trr_backend.socials.instagram.comments_scrapling.fetcher.time.monotonic",
        lambda: 101.1 if clock_state["expired"] else 100.0,
    )
    fetcher._parser._parse_comment = MagicMock(return_value=_comment("c1"))

    async def _fetch_page(*_args, **_kwargs):
        clock_state["expired"] = True
        return {
            "payload": {
                "comments": [{"id": "c1"}],
                "has_more_comments": True,
                "next_min_id": "cursor-after-returned-page",
            },
            "failed": False,
            "auth_failed": False,
            "reason": None,
            "retryable": False,
        }

    fetcher._fetch_json_response = AsyncMock(side_effect=_fetch_page)
    fetcher._fetch_rendered_comments_after_revealing_hidden = AsyncMock(return_value=[])

    result = asyncio.run(
        fetcher.fetch_comments_for_shortcode(
            "ABC123",
            max_comments=0,
            fetch_replies=False,
            expected_comment_count=10,
            top_level_cursor="request-cursor",
            top_level_cursor_param="min_id",
        )
    )

    assert result.fetch_failed is True
    assert result.retryable is True
    assert result.fetch_reason == "pagination_deadline_exceeded"
    assert result.top_level_checkpoint is not None
    assert result.top_level_checkpoint["last_top_level_cursor"] == "request-cursor"
    assert result.top_level_checkpoint["next_top_level_cursor"] == "cursor-after-returned-page"
    assert result.top_level_checkpoint["last_top_level_cursor_param"] == "min_id"
    assert result.top_level_checkpoint["next_top_level_cursor_param"] == "min_id"
    fetcher._parser._parse_comment.assert_not_called()


def test_fetch_comments_resumes_from_top_level_cursor(monkeypatch: pytest.MonkeyPatch) -> None:
    fetcher = _build_fetcher()
    fetcher._parser._parse_comment = MagicMock(return_value=_comment("c2"))
    fetcher._fetch_json_response = AsyncMock(
        return_value={
            "payload": {
                "comments": [{"id": "c2"}],
                "has_more_comments": False,
            },
            "failed": False,
            "auth_failed": False,
            "reason": None,
            "retryable": False,
        }
    )

    result = asyncio.run(
        fetcher.fetch_comments_for_shortcode(
            "ABC123",
            max_comments=0,
            fetch_replies=False,
            expected_comment_count=1,
            top_level_cursor="cursor-2",
        )
    )

    assert result.fetch_failed is False
    assert [comment.comment_id for comment in result.comments] == ["c2"]
    assert fetcher._fetch_json_response.await_args.kwargs["params"]["min_id"] == "cursor-2"
    assert fetcher._fetch_json_response.await_args.kwargs["params"]["sort_order"] == "recent"


def test_fetch_comments_can_disable_explicit_sort_order(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_SORT_ORDER", "none")
    fetcher = _build_fetcher()
    fetcher._parser._parse_comment = MagicMock(return_value=_comment("c1"))
    fetcher._fetch_json_response = AsyncMock(
        return_value={
            "payload": {
                "comments": [{"id": "c1"}],
                "has_more_comments": False,
            },
            "failed": False,
            "auth_failed": False,
            "reason": None,
            "retryable": False,
        }
    )

    result = asyncio.run(
        fetcher.fetch_comments_for_shortcode(
            "ABC123",
            max_comments=0,
            fetch_replies=False,
            expected_comment_count=1,
        )
    )

    assert result.fetch_failed is False
    assert "sort_order" not in fetcher._fetch_json_response.await_args.kwargs["params"]


def test_comment_sort_order_resolver_defaults_to_recent_for_coverage(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SOCIAL_INSTAGRAM_COMMENTS_SORT_ORDER", raising=False)
    assert resolve_comment_sort_order(None) == "recent"
    assert resolve_comment_sort_order("popular") == "popular"
    assert resolve_comment_sort_order("bogus") == "recent"
    assert resolve_comment_sort_order("none") is None


def test_fetch_comments_uses_max_id_for_tail_comment_pagination() -> None:
    fetcher = _build_fetcher()
    fetcher._parser._parse_comment = MagicMock(side_effect=[_comment("c1"), _comment("c2")])
    fetcher._fetch_json_response = AsyncMock(
        side_effect=[
            {
                "payload": {
                    "comments": [{"id": "c1"}],
                    "has_more_comments": True,
                    "has_more_headload_comments": True,
                    "next_max_id": "tail-cursor",
                    "next_min_id": "head-cursor",
                },
                "failed": False,
                "auth_failed": False,
                "reason": None,
                "retryable": False,
            },
            {
                "payload": {
                    "comments": [{"id": "c2"}],
                    "has_more_comments": False,
                    "has_more_headload_comments": False,
                },
                "failed": False,
                "auth_failed": False,
                "reason": None,
                "retryable": False,
            },
        ]
    )

    result = asyncio.run(
        fetcher.fetch_comments_for_shortcode(
            "ABC123",
            max_comments=0,
            fetch_replies=False,
            expected_comment_count=2,
        )
    )

    assert result.fetch_failed is False
    assert [comment.comment_id for comment in result.comments] == ["c1", "c2"]
    assert fetcher._fetch_json_response.await_args_list[1].kwargs["params"]["sort_order"] == "recent"
    assert fetcher._fetch_json_response.await_args_list[1].kwargs["params"]["max_id"] == "tail-cursor"
    assert "min_id" not in fetcher._fetch_json_response.await_args_list[1].kwargs["params"]


def test_fetch_comment_replies_resumes_with_checkpoint_cursor_param() -> None:
    fetcher = _build_fetcher()
    fetcher._parser._parse_comment = MagicMock(return_value=_comment("reply-1", is_reply=True))
    fetcher._fetch_json_response = AsyncMock(
        return_value={
            "payload": {
                "child_comments": [{"id": "reply-1"}],
                "has_more_tail_child_comments": False,
            },
            "failed": False,
            "auth_failed": False,
            "reason": None,
            "retryable": False,
        }
    )

    result = asyncio.run(
        fetcher._fetch_comment_replies(  # noqa: SLF001
            media_id="123",
            comment_id="parent-1",
            shortcode="ABC123",
            post_url="https://www.instagram.com/p/ABC123/",
            expected_reply_count=1,
            resume_cursor="tail-cursor",
            resume_cursor_param="max_id",
        )
    )

    assert result.fetch_failed is False
    assert fetcher._fetch_json_response.await_args.kwargs["params"] == {"max_id": "tail-cursor"}


def test_fetch_comment_replies_uses_rendered_parent_permalink_after_auth_challenge() -> None:
    fetcher = _build_fetcher()
    parent = _comment("parent-1", reply_count=1)
    rendered_reply = _comment("reply-1", is_reply=True, parent_comment_id="parent-1")
    parent.replies = [rendered_reply]
    fetcher._fetch_json_response = AsyncMock(
        return_value={
            "payload": None,
            "failed": True,
            "auth_failed": True,
            "reason": "html_challenge_or_auth_required",
            "retryable": False,
        }
    )
    fetcher._fetch_rendered_coauthor_comments_for_status_only = AsyncMock(return_value=[parent])

    result = asyncio.run(
        fetcher._fetch_comment_replies(  # noqa: SLF001
            media_id="123",
            comment_id="parent-1",
            shortcode="ABC123",
            post_url="https://www.instagram.com/p/ABC123/",
            expected_reply_count=1,
            existing_replies=[],
        )
    )

    assert result.fetch_failed is False
    assert result.auth_failed is False
    assert result.fetch_reason == "rendered_reply_fallback_recovered"
    assert [reply.comment_id for reply in result.comments] == ["reply-1"]
    fetcher._fetch_rendered_coauthor_comments_for_status_only.assert_awaited_once()
    assert fetcher._fetch_rendered_coauthor_comments_for_status_only.await_args.args[1] == (
        "https://www.instagram.com/p/ABC123/c/parent-1/"
    )
    assert (
        fetcher._fetch_rendered_coauthor_comments_for_status_only.await_args.kwargs["source_snapshot_type"]
        == "rendered_reply_fallback_comments"
    )


def test_fetch_comments_skips_reply_tail_when_persisted_replies_satisfy_count() -> None:
    fetcher = _build_fetcher()
    fetcher._parser._parse_comment = MagicMock(return_value=_comment("c1", reply_count=2))
    fetcher._fetch_json_response = AsyncMock(
        return_value={
            "payload": {
                "comments": [{"id": "c1"}],
                "has_more_comments": False,
            },
            "failed": False,
            "auth_failed": False,
            "reason": None,
            "retryable": False,
        }
    )
    fetcher._fetch_comment_replies = AsyncMock()

    result = asyncio.run(
        fetcher.fetch_comments_for_shortcode(
            "ABC123",
            max_comments=0,
            fetch_replies=True,
            expected_comment_count=3,
            persisted_replies_by_parent={
                "c1": [
                    _comment("r1", is_reply=True, parent_comment_id="c1"),
                    _comment("r2", is_reply=True, parent_comment_id="c1"),
                ]
            },
        )
    )

    assert flattened_comment_count(result.comments) == 3
    assert [reply.comment_id for reply in result.comments[0].replies] == ["r1", "r2"]
    fetcher._fetch_comment_replies.assert_not_awaited()


def test_fetch_comments_reply_only_uses_persisted_top_level_parents() -> None:
    fetcher = _build_fetcher()
    parent = _comment("c1", reply_count=2)
    existing_reply = _comment("r1", is_reply=True, parent_comment_id="c1")
    fetched_reply = _comment("r2", is_reply=True, parent_comment_id="c1")
    fetcher._fetch_json_response = AsyncMock()
    fetcher._fetch_comment_replies = AsyncMock(
        return_value=InstagramCommentsFetchResult(comments=[fetched_reply], fetch_failed=False, auth_failed=False)
    )

    result = asyncio.run(
        fetcher.fetch_comments_for_shortcode(
            "ABC123",
            max_comments=0,
            fetch_replies=True,
            expected_comment_count=10,
            persisted_replies_by_parent={"c1": [existing_reply]},
            persisted_top_level_comments=[parent],
            reply_only=True,
        )
    )

    fetcher._fetch_json_response.assert_not_awaited()
    fetcher._fetch_comment_replies.assert_awaited_once()
    assert [reply.comment_id for reply in result.comments[0].replies] == ["r1", "r2"]
    assert result.fetch_failed is False
    assert result.retryable is False
    assert result.fetch_reason == "reply_tail_coverage_complete"


def test_fetch_comments_resumes_top_level_cursor_with_recorded_param() -> None:
    fetcher = _build_fetcher()
    fetcher._parser._parse_comment = MagicMock(return_value=_comment("c2"))
    fetcher._fetch_json_response = AsyncMock(
        return_value={
            "payload": {
                "comments": [{"id": "c2"}],
                "has_more_comments": False,
            },
            "failed": False,
            "auth_failed": False,
            "reason": None,
            "retryable": False,
        }
    )

    result = asyncio.run(
        fetcher.fetch_comments_for_shortcode(
            "ABC123",
            max_comments=0,
            fetch_replies=False,
            expected_comment_count=1,
            top_level_cursor="cursor-2",
            top_level_cursor_param="max_id",
        )
    )

    assert result.fetch_failed is False
    assert fetcher._fetch_json_response.await_args.kwargs["params"]["max_id"] == "cursor-2"
    assert "min_id" not in fetcher._fetch_json_response.await_args.kwargs["params"]


def test_fetch_comment_replies_ignores_page_cap_env_and_records_repeated_cursor(monkeypatch) -> None:
    monkeypatch.setenv("SOCIAL_INSTAGRAM_REPLY_PAGINATION_MAX_PAGES", "1")
    fetcher = _build_fetcher()
    fetcher._parser._parse_comment = MagicMock(
        return_value=InstagramComment(
            comment_id="r1",
            text="reply",
            username="alpha",
            user_id="1",
            created_at=1,
            date_time="1970-01-01T00:00:01+00:00",
            likes=0,
            is_reply=True,
            parent_comment_id="c1",
            reply_count=0,
        )
    )
    fetcher._fetch_json_response = AsyncMock(
        return_value={
            "payload": {
                "child_comments": [{"id": "r1"}],
                "has_more_tail_child_comments": True,
                "next_min_child_cursor": "reply-cursor-2",
            },
            "failed": False,
            "auth_failed": False,
            "reason": None,
            "retryable": False,
        }
    )

    result = asyncio.run(
        fetcher._fetch_comment_replies(
            media_id="media-1",
            comment_id="c1",
            shortcode="ABC123",
            post_url="https://www.instagram.com/p/ABC123/",
        )
    )

    assert len(result.comments) == 1
    assert result.fetch_failed is True
    assert result.retryable is True
    assert result.fetch_reason == "pagination_repeated_cursor"
    assert result.reply_checkpoints == [
        {
            "platform": "instagram",
            "target_shortcode": "ABC123",
            "source_id": "ABC123",
            "media_id": "media-1",
            "parent_comment_id": "c1",
            "stop_reason": "pagination_repeated_cursor",
            "attempt_count": 0,
            "last_error_code": "pagination_repeated_cursor",
            "last_reply_cursor": "reply-cursor-2",
            "next_reply_cursor": "reply-cursor-2",
            "last_reply_cursor_param": "min_id",
            "next_reply_cursor_param": "min_id",
            "saved_reply_count_observed": 1,
            "pages_seen": 2,
            "retryable": True,
            "updated_at": result.reply_checkpoints[0]["updated_at"],
        }
    ]
    checkpoint_metadata = fetcher.runtime_metadata["reply_checkpoint_metadata"]
    assert checkpoint_metadata["total_count"] == 1
    assert checkpoint_metadata["dropped_count"] == 0
    assert checkpoint_metadata["truncated"] is False
    assert checkpoint_metadata["items"] == result.reply_checkpoints


def test_fetch_comment_replies_respects_parent_post_deadline(monkeypatch) -> None:
    fetcher = _build_fetcher()
    fetcher._fetch_json_response = AsyncMock()

    result = asyncio.run(
        fetcher._fetch_comment_replies(
            media_id="media-1",
            comment_id="parent-1",
            shortcode="ABC123",
            post_url="https://www.instagram.com/p/ABC123/",
            expected_reply_count=10,
            deadline=time.monotonic() - 0.01,
        )
    )

    assert result.fetch_failed is True
    assert result.retryable is True
    assert result.fetch_reason == "pagination_deadline_exceeded"
    assert result.reply_checkpoints[0]["stop_reason"] == "pagination_deadline_exceeded"
    fetcher._fetch_json_response.assert_not_awaited()


def test_fetch_comment_replies_resumes_from_checkpoint_cursor() -> None:
    fetcher = _build_fetcher()
    fetcher._parser._parse_comment = MagicMock(return_value=_comment("r2", is_reply=True, parent_comment_id="c1"))
    fetcher._fetch_json_response = AsyncMock(
        return_value={
            "payload": {
                "child_comments": [{"id": "r2"}],
                "has_more_tail_child_comments": False,
            },
            "failed": False,
            "auth_failed": False,
            "reason": None,
            "retryable": False,
        }
    )

    result = asyncio.run(
        fetcher._fetch_comment_replies(
            media_id="media-1",
            comment_id="c1",
            shortcode="ABC123",
            post_url="https://www.instagram.com/p/ABC123/",
            expected_reply_count=2,
            resume_cursor="reply-cursor-2",
        )
    )

    assert [comment.comment_id for comment in result.comments] == ["r2"]
    assert result.fetch_failed is False
    assert fetcher._fetch_json_response.await_args.kwargs["params"] == {"min_id": "reply-cursor-2"}


def test_fetch_comment_replies_follows_head_more_with_min_cursor() -> None:
    fetcher = _build_fetcher()
    fetcher._parser._parse_comment = MagicMock(
        side_effect=[
            _comment("r1", is_reply=True, parent_comment_id="c1"),
            _comment("r2", is_reply=True, parent_comment_id="c1"),
        ]
    )
    fetcher._fetch_json_response = AsyncMock(
        side_effect=[
            {
                "payload": {
                    "child_comments": [{"id": "r1"}],
                    "has_more_head_child_comments": True,
                    "has_more_tail_child_comments": False,
                    "next_min_child_cursor": "reply-cursor-2",
                    "next_max_child_cursor": None,
                },
                "failed": False,
                "auth_failed": False,
                "reason": None,
                "retryable": False,
            },
            {
                "payload": {
                    "child_comments": [{"id": "r2"}],
                    "has_more_head_child_comments": False,
                    "has_more_tail_child_comments": False,
                },
                "failed": False,
                "auth_failed": False,
                "reason": None,
                "retryable": False,
            },
        ]
    )

    result = asyncio.run(
        fetcher._fetch_comment_replies(
            media_id="media-1",
            comment_id="c1",
            shortcode="ABC123",
            post_url="https://www.instagram.com/p/ABC123/",
            expected_reply_count=2,
        )
    )

    assert [comment.comment_id for comment in result.comments] == ["r1", "r2"]
    assert result.fetch_failed is False
    assert result.retryable is False
    assert result.reply_checkpoints == []
    assert fetcher._fetch_json_response.await_args_list[0].kwargs["params"] is None
    assert fetcher._fetch_json_response.await_args_list[1].kwargs["params"] == {"min_id": "reply-cursor-2"}


def test_fetch_comment_replies_counts_existing_preview_replies_before_retrying(monkeypatch) -> None:
    monkeypatch.setenv("SOCIAL_INSTAGRAM_REPLY_PAGINATION_MAX_PAGES", "1")
    fetcher = _build_fetcher()
    fetcher._parser._parse_comment = MagicMock(return_value=_comment("r2", is_reply=True, parent_comment_id="c1"))
    fetcher._fetch_json_response = AsyncMock(
        return_value={
            "payload": {
                "child_comments": [{"id": "r2"}],
                "has_more_tail_child_comments": True,
                "next_min_child_cursor": "reply-cursor-2",
            },
            "failed": False,
            "auth_failed": False,
            "reason": None,
            "retryable": False,
        }
    )

    result = asyncio.run(
        fetcher._fetch_comment_replies(
            media_id="media-1",
            comment_id="c1",
            shortcode="ABC123",
            post_url="https://www.instagram.com/p/ABC123/",
            expected_reply_count=2,
            existing_replies=[_comment("r1", is_reply=True, parent_comment_id="c1")],
        )
    )

    assert len(result.comments) == 1
    assert result.fetch_failed is False
    assert result.retryable is False
    assert result.fetch_reason == "pagination_repeated_cursor"
    assert result.reply_checkpoints == []


def test_reply_checkpoint_metadata_is_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOCIAL_INSTAGRAM_REPLY_CHECKPOINT_MAX_ITEMS", "2")
    fetcher = _build_fetcher()
    fetcher._fetch_json_response = AsyncMock(
        return_value={
            "payload": None,
            "failed": True,
            "auth_failed": False,
            "reason": "http_429",
            "retryable": True,
            "attempt_count": 4,
        }
    )

    for comment_id in ("c1", "c2", "c3"):
        result = asyncio.run(
            fetcher._fetch_comment_replies(
                media_id="media-1",
                comment_id=comment_id,
                shortcode="ABC123",
                post_url="https://www.instagram.com/p/ABC123/",
                expected_reply_count=5,
            )
        )
        assert result.fetch_failed is True
        assert result.retryable is True
        assert result.reply_checkpoints[0]["parent_comment_id"] == comment_id
        assert result.reply_checkpoints[0]["expected_reply_count"] == 5
        assert result.reply_checkpoints[0]["attempt_count"] == 4

    checkpoint_metadata = fetcher.runtime_metadata["reply_checkpoint_metadata"]
    assert checkpoint_metadata["total_count"] == 3
    assert checkpoint_metadata["max_items"] == 2
    assert checkpoint_metadata["dropped_count"] == 1
    assert checkpoint_metadata["truncated"] is True
    assert [item["parent_comment_id"] for item in checkpoint_metadata["items"]] == ["c2", "c3"]


def _mock_httpx_response(
    *,
    status_code: int = 200,
    json_data: dict | None = None,
    headers: dict[str, str] | None = None,
    location: str | None = None,
    text: str | None = None,
) -> MagicMock:
    """Mock that looks like an httpx.Response."""
    response = MagicMock(spec=httpx.Response)
    response.status_code = status_code
    response.text = text or ""
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


def test_fetch_rebuilds_http_client_after_429_before_retry() -> None:
    """Decodo can rotate on a new proxy connection; don't pin all 429
    retries to one stale httpx client.
    """
    fetcher = _build_fetcher()
    responses = [
        _mock_httpx_response(status_code=429, headers={"retry-after": "0"}),
        _mock_httpx_response(status_code=200, json_data={"status": "ok"}),
    ]
    fetcher._fetch_api = AsyncMock(side_effect=responses)
    fetcher._rebuild_http_client = AsyncMock()

    with patch("trr_backend.socials.instagram.comments_scrapling.fetcher.asyncio.sleep", AsyncMock()):
        result = asyncio.run(
            fetcher._fetch_json_response(
                "https://www.instagram.com/api/v1/media/1/comments/",
                referer="https://www.instagram.com/p/ABC/",
            )
        )

    assert result["failed"] is False
    fetcher._rebuild_http_client.assert_awaited_once()


def test_fetch_records_shared_cooldown_after_429() -> None:
    """A 429 should slow sibling local workers, not only the current coroutine."""
    fetcher = _build_fetcher()
    responses = [
        _mock_httpx_response(status_code=429),
        _mock_httpx_response(status_code=200, json_data={"status": "ok"}),
    ]
    fetcher._fetch_api = AsyncMock(side_effect=responses)
    fetcher._rebuild_http_client = AsyncMock()

    with patch("trr_backend.socials.instagram.comments_scrapling.fetcher.asyncio.sleep", AsyncMock()):
        with patch(
            "trr_backend.socials.instagram.comments_scrapling.fetcher._record_global_api_cooldown"
        ) as cooldown_mock:
            result = asyncio.run(
                fetcher._fetch_json_response(
                    "https://www.instagram.com/api/v1/media/1/comments/",
                    referer="https://www.instagram.com/p/ABC/",
                )
            )

    assert result["failed"] is False
    cooldown_mock.assert_called_once_with(
        key=fetcher._global_rate_limit_key,
        delay_seconds=pytest.approx(15.0, abs=0.01),
    )


def test_fetch_429_shared_cooldown_uses_env_floor_and_multiplier(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENT_429_COOLDOWN_MIN_SEC", "3")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENT_429_COOLDOWN_MULTIPLIER", "4")
    fetcher = _build_fetcher()
    responses = [
        _mock_httpx_response(status_code=429, headers={"retry-after": "2"}),
        _mock_httpx_response(status_code=200, json_data={"status": "ok"}),
    ]
    fetcher._fetch_api = AsyncMock(side_effect=responses)
    fetcher._rebuild_http_client = AsyncMock()

    with patch("trr_backend.socials.instagram.comments_scrapling.fetcher.asyncio.sleep", AsyncMock()):
        with patch(
            "trr_backend.socials.instagram.comments_scrapling.fetcher._record_global_api_cooldown"
        ) as cooldown_mock:
            result = asyncio.run(
                fetcher._fetch_json_response(
                    "https://www.instagram.com/api/v1/media/1/comments/",
                    referer="https://www.instagram.com/p/ABC/",
                )
            )

    assert result["failed"] is False
    cooldown_mock.assert_called_once_with(
        key=fetcher._global_rate_limit_key,
        delay_seconds=pytest.approx(8.0, abs=0.01),
    )


def test_fetch_429_backoff_respects_deadline(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_BROWSER_API_FALLBACK_ON_429", "false")
    fetcher = _build_fetcher()
    fetcher._fetch_api = AsyncMock(return_value=_mock_httpx_response(status_code=429, headers={"retry-after": "5"}))
    fetcher._rebuild_http_client = AsyncMock()

    result = asyncio.run(
        fetcher._fetch_json_response(
            "https://www.instagram.com/api/v1/media/1/comments/",
            referer="https://www.instagram.com/p/ABC/",
            deadline=time.monotonic() + 0.01,
        )
    )

    assert result["failed"] is True
    assert result["retryable"] is True
    assert result["reason"] == "pagination_deadline_exceeded"
    fetcher._fetch_api.assert_awaited_once()


def test_fetch_uses_browser_api_fallback_after_repeated_429(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_BROWSER_API_FALLBACK_ON_429_ATTEMPT", "1")
    fetcher = _build_fetcher()
    fetcher._fetch_api = AsyncMock(return_value=_mock_httpx_response(status_code=429))
    fetcher._fetch_api_with_browser = AsyncMock(
        return_value=_mock_httpx_response(status_code=200, json_data={"status": "ok"})
    )
    fetcher._rebuild_http_client = AsyncMock()

    with patch("trr_backend.socials.instagram.comments_scrapling.fetcher.asyncio.sleep", AsyncMock()):
        result = asyncio.run(
            fetcher._fetch_json_response(
                "https://www.instagram.com/api/v1/media/1/comments/",
                referer="https://www.instagram.com/p/ABC/",
            )
        )

    assert result["failed"] is False
    assert result["payload"] == {"status": "ok"}
    fetcher._fetch_api.assert_awaited_once()
    fetcher._fetch_api_with_browser.assert_awaited_once()
    assert fetcher.runtime_metadata["retry_reason_counts"]["browser_api_fallback_after_429"] == 1


def test_fetch_api_with_browser_returns_browser_response() -> None:
    fetcher = _build_fetcher()
    response = _mock_httpx_response(
        status_code=200,
        json_data={"status": "ok"},
        headers={"set-cookie": "csrftoken=fresh-csrf-token"},
    )
    response.cookies = {"csrftoken": "fresh-csrf-token", "sessionid": "fresh-session"}
    fetcher._fetcher.async_fetch = AsyncMock(return_value=response)
    fetcher._rebuild_http_client = AsyncMock()

    result = asyncio.run(
        fetcher._fetch_api_with_browser(
            "https://www.instagram.com/api/v1/media/1/comments/",
            referer="https://www.instagram.com/p/ABC/",
            params={"can_support_threading": "true", "permalink_enabled": "false"},
        )
    )

    assert result is response
    fetcher._fetcher.async_fetch.assert_awaited_once()
    request_url = fetcher._fetcher.async_fetch.await_args.args[0]
    assert request_url == (
        "https://www.instagram.com/api/v1/media/1/comments/?can_support_threading=true&permalink_enabled=false"
    )
    assert fetcher._raw_cookies["sessionid"] == "fresh-session"
    fetcher._rebuild_http_client.assert_awaited_once()


def test_fetch_api_with_browser_evaluates_fetch_after_navigation_failure() -> None:
    fetcher = _build_fetcher()
    payload = {
        "status": 200,
        "statusText": "OK",
        "url": "https://www.instagram.com/api/v1/media/1/comments/",
        "headers": {"content-type": "application/json"},
        "body": json.dumps({"status": "ok", "comments": []}),
    }
    container_response = _mock_httpx_response(
        status_code=200,
        text=(f'<html><body><pre id="trr-browser-api-result">{html.escape(json.dumps(payload))}</pre></body></html>'),
    )
    container_response.cookies = {"csrftoken": "fresh-csrf-token"}
    fetcher._fetcher.async_fetch = AsyncMock(
        side_effect=[
            RuntimeError("Page.goto: net::ERR_HTTP_RESPONSE_CODE_FAILURE"),
            container_response,
        ]
    )
    fetcher._rebuild_http_client = AsyncMock()

    result = asyncio.run(
        fetcher._fetch_api_with_browser(
            "https://www.instagram.com/api/v1/media/1/comments/",
            referer="https://www.instagram.com/p/ABC/",
            params={"can_support_threading": "true"},
        )
    )

    assert isinstance(result, httpx.Response)
    assert result.status_code == 200
    assert result.json() == {"status": "ok", "comments": []}
    assert fetcher._fetcher.async_fetch.await_count == 2
    assert fetcher._fetcher.async_fetch.await_args_list[1].args[0] == "https://www.instagram.com/p/ABC/"
    assert fetcher.runtime_metadata["retry_reason_counts"]["browser_api_evaluate_fetch_after_navigation_failure"] == 1
    assert fetcher._raw_cookies["csrftoken"] == "fresh-csrf-token"
    fetcher._rebuild_http_client.assert_awaited_once()


def test_html_challenge_uses_browser_api_fallback() -> None:
    fetcher = _build_fetcher()
    fetcher._fetch_api = AsyncMock(
        return_value=_mock_httpx_response(
            status_code=200,
            text="<html><title>Instagram</title><body>challenge</body></html>",
        )
    )
    fetcher._fetch_api_with_browser = AsyncMock(
        return_value=_mock_httpx_response(status_code=200, json_data={"status": "ok"})
    )

    result = asyncio.run(
        fetcher._fetch_json_response(
            "https://www.instagram.com/api/v1/media/1/comments/",
            referer="https://www.instagram.com/p/ABC/",
            params={"can_support_threading": "true", "permalink_enabled": "false"},
        )
    )

    assert result["failed"] is False
    assert result["payload"] == {"status": "ok"}
    fetcher._fetch_api.assert_awaited_once()
    fetcher._fetch_api_with_browser.assert_awaited_once_with(
        "https://www.instagram.com/api/v1/media/1/comments/",
        referer="https://www.instagram.com/p/ABC/",
        params={"can_support_threading": "true", "permalink_enabled": "false"},
        deadline=None,
    )
    assert fetcher.runtime_metadata["retry_reason_counts"]["browser_api_fallback_after_html_challenge"] == 1


def test_html_challenge_records_safe_browser_and_httpx_fingerprints() -> None:
    fetcher = _build_fetcher()
    challenge_html = """
    <html>
      <head><title>Login - Instagram</title></head>
      <body>
        <form action="/challenge/action/" method="post">
          <input type="password" name="password" />
        </form>
        checkpoint challenge lsd jazoest
      </body>
    </html>
    """
    fetcher._fetch_api = AsyncMock(
        return_value=_mock_httpx_response(
            status_code=200,
            text=challenge_html,
            headers={
                "content-type": "text/html; charset=utf-8",
                "set-cookie": "ig_did=secret-cookie-value; Path=/, csrftoken=another-secret; Path=/",
            },
        )
    )
    fetcher._fetch_api_with_browser = AsyncMock(
        return_value=_mock_httpx_response(
            status_code=200,
            text=challenge_html,
            headers={"content-type": "text/html; charset=utf-8"},
        )
    )

    result = asyncio.run(
        fetcher._fetch_json_response(
            "https://www.instagram.com/api/v1/media/1/comments/",
            referer="https://www.instagram.com/p/ABC/",
            params={"can_support_threading": "true", "permalink_enabled": "false"},
        )
    )

    assert result["failed"] is True
    assert result["reason"] == "html_challenge_or_auth_required"
    challenge_metadata = fetcher.runtime_metadata["challenge_responses"]
    assert challenge_metadata["total_count"] == 2
    samples = challenge_metadata["samples"]
    assert {sample["transport"] for sample in samples} == {"httpx", "browser_api"}
    assert all(sample["html_title"] == "Login - Instagram" for sample in samples)
    assert all(sample["html_form_actions"] == ["/challenge/action/"] for sample in samples)
    assert all(sample["text_markers"]["checkpoint"] is True for sample in samples)
    assert all(sample["text_markers"]["password_field"] is True for sample in samples)
    assert all("text_sha256_16" in sample for sample in samples)
    assert "secret-cookie-value" not in str(samples)
    assert "another-secret" not in str(samples)
    assert "can_support_threading" in result["diagnostic_metadata"]["request_path"]
    assert "permalink_enabled" in result["diagnostic_metadata"]["request_path"]


def test_json_checkpoint_records_safe_payload_fingerprint() -> None:
    fetcher = _build_fetcher()
    payload = {
        "status": "fail",
        "message": "checkpoint_required",
        "challenge": {"url": "/challenge/"},
    }
    fetcher._fetch_api = AsyncMock(return_value=_mock_httpx_response(status_code=200, json_data=payload))

    result = asyncio.run(
        fetcher._fetch_json_response(
            "https://www.instagram.com/api/v1/media/1/comments/",
            referer="https://www.instagram.com/p/ABC/",
            params={"can_support_threading": "true"},
        )
    )

    assert result["failed"] is True
    assert result["auth_failed"] is True
    assert result["reason"] == "fail"
    challenge_metadata = fetcher.runtime_metadata["challenge_responses"]
    assert challenge_metadata["total_count"] == 1
    sample = challenge_metadata["samples"][0]
    assert sample["transport"] == "httpx"
    assert sample["payload_status"] == "fail"
    assert sample["payload_message"] == "checkpoint_required"
    assert sample["payload_keys"] == ["challenge", "message", "status"]
    assert "challenge" not in sample


def test_coauthor_auth_failure_uses_rendered_post_fallback() -> None:
    fetcher = _build_fetcher()
    rendered_comment = _comment("rendered-comment")
    fetcher._fetch_json_response = AsyncMock(
        return_value={
            "failed": True,
            "auth_failed": True,
            "reason": "html_challenge_or_auth_required",
            "retryable": False,
            "payload": None,
            "attempt_count": 2,
        }
    )
    fetcher._fetch_public_relay_coauthor_comments_for_status_only = AsyncMock(
        return_value=([], {"reason": "graphql_relay_empty"})
    )
    fetcher._fetch_rendered_coauthor_comments_for_status_only = AsyncMock(return_value=[rendered_comment])

    result = asyncio.run(
        fetcher.fetch_comments_for_shortcode(
            "DXpWUKECX3t",
            max_comments=1,
            fetch_replies=False,
            expected_comment_count=129,
            target_metadata={
                "source_id": "DXpWUKECX3t",
                "profile_account": "thetraitorsus",
                "owner_username": "peacock",
                "collaborators": ["thetraitorsus"],
                "is_collaborator_post": True,
            },
        )
    )

    assert result.fetch_failed is False
    assert result.auth_failed is False
    assert result.retryable is False
    assert result.fetch_reason == "coauthor_auth_rendered_fallback_recovered"
    assert result.comments == [rendered_comment]
    fetcher._fetch_rendered_coauthor_comments_for_status_only.assert_awaited_once()
    rendered_lane = fetcher.runtime_metadata["lane_diagnostics"]["rendered"]
    assert rendered_lane["last_reason"] == "coauthor_auth_rendered_fallback_recovered"
    assert rendered_lane["last_metadata"]["merged_comments"] == 1


def test_auth_failure_uses_public_relay_fallback_before_rendered_for_non_collaborator() -> None:
    fetcher = _build_fetcher()
    relay_comment = _comment("relay-comment")
    fetcher._fetch_json_response = AsyncMock(
        return_value={
            "failed": True,
            "auth_failed": True,
            "reason": "html_challenge_or_auth_required",
            "retryable": False,
            "payload": None,
            "attempt_count": 2,
        }
    )
    fetcher._fetch_public_relay_coauthor_comments_for_status_only = AsyncMock(
        return_value=([relay_comment], {"reason": "child_comments_target_reached"})
    )
    fetcher._fetch_rendered_coauthor_comments_for_status_only = AsyncMock(return_value=[])

    result = asyncio.run(
        fetcher.fetch_comments_for_shortcode(
            "DYAk3FDFQR3",
            max_comments=1,
            fetch_replies=False,
            expected_comment_count=19,
            target_metadata={
                "source_id": "DYAk3FDFQR3",
                "profile_account": "thetraitorsus",
                "owner_username": "thetraitorsus",
                "collaborators": [],
                "is_collaborator_post": False,
            },
        )
    )

    assert result.fetch_failed is False
    assert result.auth_failed is False
    assert result.retryable is False
    assert result.fetch_reason == "auth_relay_fallback_recovered"
    assert result.comments == [relay_comment]
    fetcher._fetch_public_relay_coauthor_comments_for_status_only.assert_awaited_once()
    fetcher._fetch_rendered_coauthor_comments_for_status_only.assert_not_awaited()
    relay_lane = fetcher.runtime_metadata["lane_diagnostics"]["relay"]
    assert relay_lane["last_reason"] == "auth_relay_fallback_recovered"
    assert relay_lane["last_metadata"]["merged_comments"] == 1
    assert relay_lane["last_metadata"]["relay_fallback"]["reason"] == "child_comments_target_reached"


def test_browser_session_invalidation_diagnostic_skips_relay_and_rendered_fallbacks() -> None:
    fetcher = _build_fetcher()
    fetcher._fetch_json_response = AsyncMock(
        return_value={
            "failed": True,
            "auth_failed": True,
            "reason": "html_challenge_or_auth_required",
            "retryable": False,
            "payload": None,
            "attempt_count": 1,
            "diagnostic_metadata": {
                "reason": "html_challenge_or_auth_required",
                "session_invalidated": True,
                "text_markers": {"session_invalidated": True},
            },
        }
    )
    fetcher._fetch_public_relay_coauthor_comments_for_status_only = AsyncMock(return_value=([], {}))
    fetcher._fetch_rendered_coauthor_comments_for_status_only = AsyncMock(return_value=[_comment("rendered")])

    result = asyncio.run(
        fetcher.fetch_comments_for_shortcode(
            "DYAk3FDFQR3",
            max_comments=1,
            fetch_replies=False,
            expected_comment_count=1,
            target_metadata={"is_collaborator_post": False},
        )
    )

    assert result.comments == []
    assert result.fetch_failed is True
    assert result.auth_failed is True
    assert result.retryable is False
    assert result.fetch_reason == BROWSER_SESSION_INVALIDATED_REASON
    assert result.diagnostic_metadata["session_invalidated"] is True
    fetcher._fetch_public_relay_coauthor_comments_for_status_only.assert_not_awaited()
    fetcher._fetch_rendered_coauthor_comments_for_status_only.assert_not_awaited()


def test_partial_auth_failure_uses_public_relay_fallback_for_remaining_gap() -> None:
    fetcher = _build_fetcher()
    fetcher._parser._parse_comment = MagicMock(return_value=_comment("api-1"))
    fetcher._fetch_json_response = AsyncMock(
        side_effect=[
            {
                "payload": {
                    "comments": [{"id": "api-1"}],
                    "has_more_comments": True,
                    "next_min_id": "cursor-1",
                },
                "failed": False,
                "auth_failed": False,
                "reason": None,
                "retryable": False,
            },
            {
                "payload": None,
                "failed": True,
                "auth_failed": True,
                "reason": "html_challenge_or_auth_required",
                "retryable": False,
            },
        ]
    )
    fetcher._fetch_public_relay_coauthor_comments_for_status_only = AsyncMock(
        return_value=(
            [_comment("api-1"), _comment("relay-2")],
            {"reason": "graphql_relay_target_reached"},
        )
    )
    fetcher._fetch_rendered_coauthor_comments_for_status_only = AsyncMock(return_value=[])

    result = asyncio.run(
        fetcher.fetch_comments_for_shortcode(
            "DQ4lvpcj-gu",
            max_comments=0,
            fetch_replies=False,
            expected_comment_count=2,
            target_metadata={
                "source_id": "DQ4lvpcj-gu",
                "profile_account": "thetraitorsus",
                "owner_username": "thetraitorsus",
                "collaborators": [],
                "is_collaborator_post": False,
            },
        )
    )

    assert [comment.comment_id for comment in result.comments] == ["api-1", "relay-2"]
    assert result.fetch_failed is False
    assert result.auth_failed is False
    assert result.retryable is False
    assert result.fetch_reason == "auth_relay_fallback_recovered"
    fetcher._fetch_public_relay_coauthor_comments_for_status_only.assert_awaited_once()
    fetcher._fetch_rendered_coauthor_comments_for_status_only.assert_not_awaited()
    relay_lane = fetcher.runtime_metadata["lane_diagnostics"]["relay"]
    assert relay_lane["last_reason"] == "auth_relay_fallback_recovered"
    assert relay_lane["last_metadata"]["merged_comments"] == 1
    assert relay_lane["last_metadata"]["api_fetch_reason"] == "html_challenge_or_auth_required"


def test_partial_relay_auth_recovery_uses_rendered_fallback_for_remaining_gap() -> None:
    fetcher = _build_fetcher()
    fetcher._parser._parse_comment = MagicMock(return_value=_comment("api-1"))
    fetcher._fetch_json_response = AsyncMock(
        side_effect=[
            {
                "payload": {
                    "comments": [{"id": "api-1"}],
                    "has_more_comments": True,
                    "next_min_id": "cursor-1",
                },
                "failed": False,
                "auth_failed": False,
                "reason": None,
                "retryable": False,
            },
            {
                "payload": None,
                "failed": True,
                "auth_failed": True,
                "reason": "html_challenge_or_auth_required",
                "retryable": False,
            },
        ]
    )
    fetcher._fetch_public_relay_coauthor_comments_for_status_only = AsyncMock(
        return_value=(
            [_comment("api-1"), _comment("relay-2")],
            {"reason": "graphql_relay_partial"},
        )
    )
    fetcher._fetch_rendered_coauthor_comments_for_status_only = AsyncMock(
        return_value=[_comment("api-1"), _comment("relay-2"), _comment("rendered-3")]
    )

    result = asyncio.run(
        fetcher.fetch_comments_for_shortcode(
            "DQ4lvpcj-gu",
            max_comments=0,
            fetch_replies=False,
            expected_comment_count=3,
            target_metadata={
                "source_id": "DQ4lvpcj-gu",
                "profile_account": "thetraitorsus",
                "owner_username": "thetraitorsus",
                "collaborators": [],
                "is_collaborator_post": False,
            },
        )
    )

    assert [comment.comment_id for comment in result.comments] == ["api-1", "relay-2", "rendered-3"]
    assert result.fetch_failed is False
    assert result.auth_failed is False
    assert result.retryable is False
    assert result.fetch_reason == "auth_rendered_fallback_recovered"
    fetcher._fetch_public_relay_coauthor_comments_for_status_only.assert_awaited_once()
    fetcher._fetch_rendered_coauthor_comments_for_status_only.assert_awaited_once()
    relay_lane = fetcher.runtime_metadata["lane_diagnostics"]["relay"]
    assert relay_lane["last_reason"] == "auth_relay_fallback_recovered"
    rendered_lane = fetcher.runtime_metadata["lane_diagnostics"]["rendered"]
    assert rendered_lane["last_reason"] == "auth_rendered_fallback_recovered"
    assert rendered_lane["last_metadata"]["merged_comments"] == 1


def test_auth_failure_uses_rendered_post_fallback_for_non_collaborator() -> None:
    fetcher = _build_fetcher()
    rendered_comment = _comment("rendered-comment")
    fetcher._fetch_json_response = AsyncMock(
        return_value={
            "failed": True,
            "auth_failed": True,
            "reason": "html_challenge_or_auth_required",
            "retryable": False,
            "payload": None,
            "attempt_count": 2,
        }
    )
    fetcher._fetch_public_relay_coauthor_comments_for_status_only = AsyncMock(
        return_value=([], {"reason": "graphql_relay_empty"})
    )
    fetcher._fetch_rendered_coauthor_comments_for_status_only = AsyncMock(return_value=[rendered_comment])

    result = asyncio.run(
        fetcher.fetch_comments_for_shortcode(
            "DVWyrYtga5L",
            max_comments=1,
            fetch_replies=False,
            expected_comment_count=1,
            target_metadata={
                "source_id": "DVWyrYtga5L",
                "profile_account": "thetraitorsus",
                "owner_username": "thetraitorsus",
                "collaborators": [],
                "is_collaborator_post": False,
            },
        )
    )

    assert result.fetch_failed is False
    assert result.auth_failed is False
    assert result.retryable is False
    assert result.fetch_reason == "auth_rendered_fallback_recovered"
    assert result.comments == [rendered_comment]
    fetcher._fetch_rendered_coauthor_comments_for_status_only.assert_awaited_once()
    assert (
        fetcher._fetch_rendered_coauthor_comments_for_status_only.await_args.kwargs["source_snapshot_type"]
        == "rendered_auth_fallback_comments"
    )
    rendered_lane = fetcher.runtime_metadata["lane_diagnostics"]["rendered"]
    assert rendered_lane["last_reason"] == "auth_rendered_fallback_recovered"
    assert rendered_lane["last_metadata"]["merged_comments"] == 1


def test_single_session_load_all_merges_rendered_hydration_after_api_gap(monkeypatch: pytest.MonkeyPatch) -> None:
    fetcher = _build_fetcher()
    fetcher._should_reveal_hidden_comments = MagicMock(return_value=False)
    fetcher._parser.parse_comment = MagicMock(return_value=_comment("api-1"))
    rendered_duplicate = _comment("rendered-synthetic")
    rendered_duplicate.username = "user_api-1"
    rendered_duplicate.text = "api-1"
    fetcher._fetch_json_response = AsyncMock(
        return_value={
            "payload": {"comments": [{"id": "api-1"}], "has_more_comments": False},
            "failed": False,
            "auth_failed": False,
            "reason": None,
            "retryable": False,
        }
    )
    fetcher._fetch_rendered_single_session_load_all = AsyncMock(
        return_value=(
            [_comment("api-1"), rendered_duplicate, _comment("rendered-2")],
            {"reason": "rendered_comments_found", "rendered_rows_seen": 2},
        )
    )

    result = asyncio.run(
        fetcher.fetch_comments_for_shortcode(
            "ABC123",
            max_comments=10,
            fetch_replies=False,
            expected_comment_count=2,
            load_strategy="single_session_load_all",
        )
    )

    assert [comment.comment_id for comment in result.comments] == ["api-1", "rendered-2"]
    assert result.fetch_failed is False
    assert result.retryable is False
    assert result.fetch_reason == "single_session_rendered_hydration_recovered"
    assert result.diagnostic_metadata["strategy_decision"]["selected_strategy"] == "single_session_load_all"
    assert result.diagnostic_metadata["fallback_trigger"] == "api_complete_expected_gap"
    assert result.diagnostic_metadata["lane_order"] == ["instagram_comments_endpoint_cursor", "rendered_hydration"]
    assert result.diagnostic_metadata["api_pages_loaded"] == 1
    assert result.diagnostic_metadata["rendered_load_attempts"] == 1
    assert result.diagnostic_metadata["rendered_rows_seen"] == 3
    assert result.diagnostic_metadata["rendered_merged_comments"] == 1
    assert result.diagnostic_metadata["merged_comments"] == 2


def test_single_session_load_all_memory_guardrail_stops_retryable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_SINGLE_SESSION_MAX_IN_MEMORY_ROWS", "1")
    fetcher = _build_fetcher()
    fetcher._should_reveal_hidden_comments = MagicMock(return_value=False)
    fetcher._parser.parse_comment = MagicMock(side_effect=[_comment("api-1"), _comment("api-2")])
    fetcher._fetch_json_response = AsyncMock(
        return_value={
            "payload": {
                "comments": [{"id": "api-1"}, {"id": "api-2"}],
                "has_more_comments": False,
            },
            "failed": False,
            "auth_failed": False,
            "reason": None,
            "retryable": False,
        }
    )
    fetcher._fetch_rendered_single_session_load_all = AsyncMock(return_value=([], {}))

    result = asyncio.run(
        fetcher.fetch_comments_for_shortcode(
            "ABC123",
            max_comments=10,
            fetch_replies=False,
            expected_comment_count=2,
            load_strategy="single_session_load_all",
        )
    )

    # Bug #10b: the guardrail trips at exactly max_in_memory_rows (inclusive cap),
    # so only the first row is retained before the loop stops.
    assert [comment.comment_id for comment in result.comments] == ["api-1"]
    assert result.fetch_failed is True
    assert result.retryable is True
    assert result.fetch_reason == "memory_guardrail_reached"
    assert result.diagnostic_metadata["memory_guardrail"] == {
        "max_in_memory_rows": 1,
        "current_rows": 1,
        "reached": True,
        "stop_reason": "memory_guardrail_reached",
    }
    assert result.diagnostic_metadata["rendered_load_attempts"] == 0
    fetcher._fetch_rendered_single_session_load_all.assert_not_awaited()


def test_single_session_load_all_challenge_stop_skips_rendered_hydration() -> None:
    fetcher = _build_fetcher()
    fetcher._fetch_json_response = AsyncMock(
        return_value={
            "payload": None,
            "failed": True,
            "auth_failed": True,
            "reason": "html_challenge_or_auth_required",
            "retryable": False,
        }
    )
    fetcher._fetch_rendered_single_session_load_all = AsyncMock(return_value=([_comment("rendered-1")], {}))

    result = asyncio.run(
        fetcher.fetch_comments_for_shortcode(
            "ABC123",
            max_comments=10,
            fetch_replies=False,
            expected_comment_count=2,
            load_strategy="single_session_load_all",
        )
    )

    assert result.comments == []
    assert result.fetch_failed is True
    assert result.auth_failed is True
    assert result.fetch_reason == "html_challenge_or_auth_required"
    assert result.diagnostic_metadata["challenge_stop"] is True
    assert result.diagnostic_metadata["rendered_load_attempts"] == 0
    fetcher._fetch_rendered_single_session_load_all.assert_not_awaited()


@pytest.mark.parametrize(
    "error_message",
    [
        "[SSL: WRONG_VERSION_NUMBER] wrong version number (_ssl.c:2590)",
        "[SSL] record layer failure (_ssl.c:2590)",
        "SSL connection has been closed unexpectedly\n",
    ],
)
def test_fetch_retries_raw_ssl_oserror_then_succeeds(error_message: str) -> None:
    """TLS/proxy errors can escape httpx as raw OSError; keep them in the
    bounded request retry path instead of failing the shard immediately."""
    fetcher = _build_fetcher()
    fetcher._fetch_api = AsyncMock(
        side_effect=[
            OSError(error_message),
            _mock_httpx_response(status_code=200, json_data={"status": "ok"}),
        ]
    )
    fetcher._rebuild_http_client = AsyncMock()

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
    assert fetcher.runtime_metadata["retry_reason_counts"]["transport_error"] == 1
    fetcher._rebuild_http_client.assert_awaited_once()


def test_fetch_gives_up_after_max_retries_with_retryable_true(monkeypatch: pytest.MonkeyPatch) -> None:
    """Exhausting retries surfaces retryable=True for queue requeue."""
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_BROWSER_API_FALLBACK_ON_429", "false")
    fetcher = _build_fetcher()
    # SA-1 (comment-completeness): the transient retry cap was raised 5 -> 10, so
    # exhausting it now takes _MAX_TRANSIENT_RETRIES + 1 attempts. Provide exactly
    # that many 429 responses so the side_effect feed matches the new budget.
    responses = [
        _mock_httpx_response(status_code=429, headers={"retry-after": "0"})
        for _ in range(InstagramCommentsScraplingFetcher._MAX_TRANSIENT_RETRIES + 1)
    ]
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
    assert fetcher.runtime_metadata["retry_reason_counts"]["http_429"] == (
        InstagramCommentsScraplingFetcher._MAX_TRANSIENT_RETRIES + 1
    )
    assert fetcher._fetch_api.await_count == InstagramCommentsScraplingFetcher._MAX_TRANSIENT_RETRIES + 1


def test_fetch_retry_count_can_be_raised_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENT_TRANSIENT_RETRIES", "7")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_BROWSER_API_FALLBACK_ON_429", "false")
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
    assert fetcher.runtime_metadata["max_transient_retries"] == 7
    assert fetcher._fetch_api.await_count == 8


def test_reply_fetch_retry_count_can_stay_lower_than_top_level(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENT_TRANSIENT_RETRIES", "9")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENT_REPLY_TRANSIENT_RETRIES", "2")
    fetcher = _build_fetcher()
    responses = [_mock_httpx_response(status_code=429, headers={"retry-after": "0"}) for _ in range(10)]
    fetcher._fetch_api = AsyncMock(side_effect=responses)

    with patch("trr_backend.socials.instagram.comments_scrapling.fetcher.asyncio.sleep", AsyncMock()):
        result = asyncio.run(
            fetcher._fetch_json_response(
                "https://www.instagram.com/api/v1/media/1/comments/2/child_comments/",
                referer="https://www.instagram.com/p/ABC/",
                max_retries=fetcher._reply_max_transient_retries,
            )
        )

    assert result["failed"] is True
    assert result["retryable"] is True
    assert fetcher.runtime_metadata["max_transient_retries"] == 9
    assert fetcher.runtime_metadata["reply_max_transient_retries"] == 2
    assert fetcher._fetch_api.await_count == 3


def test_reply_fetch_retry_count_defaults_to_top_level_budget(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENT_TRANSIENT_RETRIES", "9")
    monkeypatch.delenv("SOCIAL_INSTAGRAM_COMMENT_REPLY_TRANSIENT_RETRIES", raising=False)

    fetcher = _build_fetcher()

    assert fetcher.runtime_metadata["max_transient_retries"] == 9
    assert fetcher.runtime_metadata["reply_max_transient_retries"] == 9


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
    assert fetcher.runtime_metadata["retry_reason_counts"]["transport_timeout"] == 1


def test_httpx_transport_error_exhausts_retries() -> None:
    fetcher = _build_fetcher()
    fetcher._fetch_api = AsyncMock(side_effect=httpx.ConnectError("connection reset"))

    with patch("trr_backend.socials.instagram.comments_scrapling.fetcher.asyncio.sleep", AsyncMock()):
        result = asyncio.run(
            fetcher._fetch_json_response(
                "https://www.instagram.com/api/v1/media/1/comments/",
                referer="https://www.instagram.com/p/ABC/",
            )
        )

    assert result["failed"] is True
    assert result["retryable"] is True
    assert result["reason"] == "transport_error"
    assert result["diagnostic_metadata"]["exception_class"] == "ConnectError"
    assert result["diagnostic_metadata"]["exception_message"] == "connection reset"
    assert result["diagnostic_metadata"]["request_path"] == "/api/v1/media/1/comments/"
    assert result["diagnostic_metadata"]["referer_path"] == "/p/ABC/"
    assert fetcher.runtime_metadata["retry_reason_counts"]["transport_error"] == (
        InstagramCommentsScraplingFetcher._MAX_TRANSIENT_RETRIES + 1
    )
    transport_failures = fetcher.runtime_metadata["transport_failures"]
    assert transport_failures["total_count"] == InstagramCommentsScraplingFetcher._MAX_TRANSIENT_RETRIES + 1
    assert transport_failures["samples"][-1]["exception_class"] == "ConnectError"


def test_rebuild_http_client_closes_existing_async_client() -> None:
    fetcher = _build_fetcher()
    stale_client = AsyncMock()
    fetcher._http_client = stale_client

    asyncio.run(fetcher._rebuild_http_client())

    stale_client.aclose.assert_awaited_once()
    assert fetcher._http_client is not stale_client


def test_sync_response_cookies_updates_parser_headers() -> None:
    fetcher = _build_fetcher()
    response = _mock_httpx_response(status_code=200, json_data={"status": "ok"})
    response.cookies = {"csrftoken": "fresh-csrf-token", "sessionid": "fresh-session"}

    fetcher._sync_response_cookies(response)

    headers = fetcher._parser._get_headers("https://www.instagram.com/p/ABC/")
    csrf_header = headers.get("x-csrftoken") or headers.get("X-CSRFToken") or ""
    assert csrf_header == "fresh-csrf-token"
    assert fetcher._raw_cookies["sessionid"] == "fresh-session"


def test_pace_api_requests_honors_comment_delay() -> None:
    fetcher = _build_fetcher()
    fetcher._api_delay_seconds = 0.25
    fetcher._last_api_request_started_at = 10.0
    sleep_mock = AsyncMock()

    with patch("trr_backend.socials.instagram.comments_scrapling.fetcher.time.monotonic", return_value=10.10):
        with patch("trr_backend.socials.instagram.comments_scrapling.fetcher.asyncio.sleep", sleep_mock):
            asyncio.run(fetcher._pace_api_requests())

    sleep_mock.assert_awaited_once_with(pytest.approx(0.15, abs=0.01))


def test_global_api_pacing_ignores_legacy_wall_clock_lockfile(tmp_path: Path) -> None:
    lock_dir = tmp_path / "trr-instagram-comments-rate"
    lock_dir.mkdir()
    lock_path = lock_dir / "legacy.lock"
    lock_path.write_text("1700000000.0", encoding="utf-8")
    sleep_mock = MagicMock()

    with patch(
        "trr_backend.socials.instagram.comments_scrapling.fetcher.tempfile.gettempdir",
        return_value=str(tmp_path),
    ):
        with patch("trr_backend.socials.instagram.comments_scrapling.fetcher.time.monotonic", return_value=100.0):
            with patch("trr_backend.socials.instagram.comments_scrapling.fetcher.time.sleep", sleep_mock):
                _pace_global_api_request(key="legacy", delay_seconds=1.0)

    sleep_mock.assert_not_called()
    assert float(lock_path.read_text(encoding="utf-8")) == 100.0


def test_global_api_pacing_honors_shared_429_cooldown(tmp_path: Path) -> None:
    sleep_mock = MagicMock()

    with patch(
        "trr_backend.socials.instagram.comments_scrapling.fetcher.tempfile.gettempdir",
        return_value=str(tmp_path),
    ):
        with patch(
            "trr_backend.socials.instagram.comments_scrapling.fetcher.time.monotonic",
            side_effect=[100.0, 100.0, 100.0],
        ):
            _record_global_api_cooldown(key="shared", delay_seconds=5.0)
            with patch("trr_backend.socials.instagram.comments_scrapling.fetcher.time.sleep", sleep_mock):
                _pace_global_api_request(key="shared", delay_seconds=0.0)

    sleep_mock.assert_called_once_with(pytest.approx(5.0, abs=0.01))


def test_global_api_pacing_stops_when_cooldown_exceeds_deadline(tmp_path: Path) -> None:
    sleep_mock = MagicMock()

    with patch(
        "trr_backend.socials.instagram.comments_scrapling.fetcher.tempfile.gettempdir",
        return_value=str(tmp_path),
    ):
        _record_global_api_cooldown(key="deadline", delay_seconds=5.0)
        deadline = time.monotonic() + 0.01
        with patch("trr_backend.socials.instagram.comments_scrapling.fetcher.time.sleep", sleep_mock):
            paced = _pace_global_api_request(key="deadline", delay_seconds=0.0, deadline=deadline)

    assert paced is False
    sleep_mock.assert_called_once()
    assert sleep_mock.call_args.args[0] == pytest.approx(0.01, abs=0.01)


def test_advisory_api_pacing_stops_when_cooldown_exceeds_deadline(tmp_path: Path) -> None:
    sleep_mock = MagicMock()

    with patch(
        "trr_backend.socials.instagram.comments_scrapling.fetcher.tempfile.gettempdir",
        return_value=str(tmp_path),
    ):
        _record_global_api_cooldown(key="advisory-deadline", delay_seconds=5.0)
        deadline = time.monotonic() + 0.01
        with patch("trr_backend.socials.instagram.comments_scrapling.fetcher.time.sleep", sleep_mock):
            result = _try_advisory_lock_pace(key="advisory-deadline", delay_seconds=0.0, deadline=deadline)

    assert result["paced"] is False
    assert result["cooldown_blocked"] is True
    assert result["error"] is None
    sleep_mock.assert_called_once()
    assert sleep_mock.call_args.args[0] == pytest.approx(0.01, abs=0.01)


def test_advisory_api_pacing_uses_social_control_pool(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.db import pg

    calls: list[tuple[str, str]] = []

    class FakeCursor:
        def execute(self, _sql: str, _params: Any = None) -> None:
            return None

        def fetchone(self) -> list[float]:
            # Reservation upsert returns remaining-seconds; 0 ⇒ no wait.
            return [0.0]

    @contextmanager
    def fake_db_connection(*, label: str, pool_name: str = "default"):
        calls.append((label, pool_name))
        yield object()

    @contextmanager
    def fake_db_cursor(*, conn: Any = None, label: str = "write-cursor"):
        del conn, label
        yield FakeCursor()

    monkeypatch.setattr(pg, "db_connection", fake_db_connection)
    monkeypatch.setattr(pg, "db_cursor", fake_db_cursor)

    result = _try_advisory_lock_pace(key="advisory-pool", delay_seconds=0.0, deadline=None)

    assert result["acquired"] is True
    assert result["paced"] is True
    assert result["error"] is None
    assert calls == [("instagram-comments-rate-limit-pace", "social_control")]


# ---------------------------------------------------------------------------
# 3xx redirect handling
# ---------------------------------------------------------------------------


def test_3xx_redirect_to_login(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_BROWSER_API_FALLBACK", "0")
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


def test_3xx_redirect_to_login_uses_browser_api_fallback() -> None:
    fetcher = _build_fetcher()
    fetcher._fetch_api = AsyncMock(
        return_value=_mock_httpx_response(status_code=302, location="/accounts/login/?next=/api/v1/media/1/comments/")
    )
    fetcher._fetch_api_with_browser = AsyncMock(
        return_value=_mock_httpx_response(status_code=200, json_data=_fixture_json("comments_success.json"))
    )

    result = asyncio.run(
        fetcher._fetch_json_response(
            "https://www.instagram.com/api/v1/media/1/comments/",
            referer="https://www.instagram.com/p/ABC/",
            params={"can_support_threading": "true", "permalink_enabled": "false"},
        )
    )

    assert result["failed"] is False
    assert result["payload"]["status"] == "ok"
    fetcher._fetch_api_with_browser.assert_awaited_once_with(
        "https://www.instagram.com/api/v1/media/1/comments/",
        referer="https://www.instagram.com/p/ABC/",
        params={"can_support_threading": "true", "permalink_enabled": "false"},
        deadline=None,
    )
    assert fetcher.runtime_metadata["retry_reason_counts"]["browser_api_fallback_after_auth_redirect"] == 1


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


def test_3xx_redirect_to_homepage_rewarms_permalink_and_retries_once() -> None:
    fetcher = _build_fetcher()
    fetcher._fetch_api = AsyncMock(
        side_effect=[
            _mock_httpx_response(status_code=302, location="/"),
            _mock_httpx_response(status_code=200, json_data=_fixture_json("comments_success.json")),
        ]
    )
    fetcher._fetch_page = AsyncMock(return_value=_mock_httpx_response(status_code=200))
    fetcher._rebuild_http_client = AsyncMock()

    result = asyncio.run(
        fetcher._fetch_json_response(
            "https://www.instagram.com/api/v1/media/1/comments/",
            referer="https://www.instagram.com/p/DXXAP-Ekb59/",
        )
    )

    assert fetcher._fetch_api.await_count == 2
    fetcher._fetch_page.assert_awaited_once_with(
        "https://www.instagram.com/p/DXXAP-Ekb59/",
        referer="https://www.instagram.com/p/DXXAP-Ekb59/",
    )
    assert result["failed"] is False
    assert fetcher.runtime_metadata["retry_reason_counts"]["homepage_redirect_recovery"] == 1


def test_3xx_redirect_to_homepage_marks_auth_failed_after_recovery_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_BROWSER_API_FALLBACK", "0")
    fetcher = _build_fetcher()
    fetcher._fetch_api = AsyncMock(
        side_effect=[
            _mock_httpx_response(status_code=302, location="/"),
            _mock_httpx_response(status_code=302, location="/"),
        ]
    )
    fetcher._fetch_page = AsyncMock(return_value=_mock_httpx_response(status_code=200))
    fetcher._rebuild_http_client = AsyncMock()

    result = asyncio.run(
        fetcher._fetch_json_response(
            "https://www.instagram.com/api/v1/media/1/comments/",
            referer="https://www.instagram.com/p/DXXAP-Ekb59/",
        )
    )

    assert result["failed"] is True
    assert result["auth_failed"] is True
    assert result["reason"] == "redirect_to_homepage"


def test_3xx_redirect_to_homepage_uses_browser_api_fallback_after_recovery_retry() -> None:
    fetcher = _build_fetcher()
    fetcher._fetch_api = AsyncMock(
        side_effect=[
            _mock_httpx_response(status_code=302, location="/"),
            _mock_httpx_response(status_code=302, location="/"),
        ]
    )
    fetcher._fetch_page = AsyncMock(return_value=_mock_httpx_response(status_code=200))
    fetcher._rebuild_http_client = AsyncMock()
    fetcher._fetch_api_with_browser = AsyncMock(
        return_value=_mock_httpx_response(status_code=200, json_data=_fixture_json("comments_success.json"))
    )

    result = asyncio.run(
        fetcher._fetch_json_response(
            "https://www.instagram.com/api/v1/media/1/comments/",
            referer="https://www.instagram.com/p/DXpWUKECX3t/",
            params={"can_support_threading": "true", "permalink_enabled": "false"},
        )
    )

    assert result["failed"] is False
    assert result["payload"]["status"] == "ok"
    fetcher._fetch_api_with_browser.assert_awaited_once_with(
        "https://www.instagram.com/api/v1/media/1/comments/",
        referer="https://www.instagram.com/p/DXpWUKECX3t/",
        params={"can_support_threading": "true", "permalink_enabled": "false"},
        deadline=None,
    )
    assert fetcher.runtime_metadata["retry_reason_counts"]["browser_api_fallback"] == 1


def test_3xx_redirect_to_homepage_from_browser_api_marks_session_invalidated() -> None:
    fetcher = _build_fetcher()
    fetcher._fetch_api = AsyncMock(
        side_effect=[
            _mock_httpx_response(status_code=302, location="/"),
            _mock_httpx_response(status_code=302, location="/"),
        ]
    )
    fetcher._fetch_page = AsyncMock(return_value=_mock_httpx_response(status_code=200))
    fetcher._rebuild_http_client = AsyncMock()
    fetcher._fetch_api_with_browser = AsyncMock(return_value=_mock_httpx_response(status_code=302, location="/"))

    result = asyncio.run(
        fetcher._fetch_json_response(
            "https://www.instagram.com/api/v1/media/1/comments/",
            referer="https://www.instagram.com/p/DXpWUKECX3t/",
        )
    )

    assert result["failed"] is True
    assert result["auth_failed"] is True
    assert result["reason"] == BROWSER_SESSION_INVALIDATED_REASON
    assert result["diagnostic_metadata"]["transport"] == "browser_api"
    assert result["diagnostic_metadata"]["session_invalidated"] is True


def test_3xx_redirect_to_homepage_can_disable_browser_api_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_BROWSER_API_FALLBACK", "0")
    fetcher = _build_fetcher()
    fetcher._fetch_api = AsyncMock(
        side_effect=[
            _mock_httpx_response(status_code=302, location="/"),
            _mock_httpx_response(status_code=302, location="/"),
        ]
    )
    fetcher._fetch_page = AsyncMock(return_value=_mock_httpx_response(status_code=200))
    fetcher._rebuild_http_client = AsyncMock()
    fetcher._fetch_api_with_browser = AsyncMock()

    result = asyncio.run(
        fetcher._fetch_json_response(
            "https://www.instagram.com/api/v1/media/1/comments/",
            referer="https://www.instagram.com/p/DXpWUKECX3t/",
        )
    )

    assert result["failed"] is True
    assert result["auth_failed"] is True
    assert result["reason"] == "redirect_to_homepage"
    fetcher._fetch_api_with_browser.assert_not_awaited()


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

    fetcher._fetch_page.assert_awaited_once_with(
        "https://www.instagram.com/testaccount/",
        referer="https://www.instagram.com/testaccount/",
    )
    headers = fetcher._parser._get_headers("https://www.instagram.com/p/ABC/")
    csrf_header = headers.get("x-csrftoken") or headers.get("X-CSRFToken") or ""
    assert csrf_header == "fresh-csrf-token", (
        f"Expected parser headers to reflect the bridged csrftoken, got: {csrf_header}"
    )


def test_warmup_allows_existing_session_without_new_cookie_delta() -> None:
    fetcher = _build_fetcher()
    fetcher._raw_cookies["sessionid"] = "existing-session"

    warmup_response = MagicMock()
    warmup_response.status = 200
    warmup_response.text = ""
    warmup_response.cookies = {}

    fetcher._fetch_page = AsyncMock(return_value=warmup_response)
    fetcher._rebuild_http_client = AsyncMock()

    asyncio.run(fetcher.warmup())

    assert fetcher.runtime_metadata["warmup_cookie_count"] == 0
    fetcher._rebuild_http_client.assert_awaited_once()


def test_warmup_raises_when_no_cookies_are_bridged() -> None:
    fetcher = _build_fetcher()

    warmup_response = MagicMock()
    warmup_response.status = 200
    warmup_response.text = ""
    warmup_response.cookies = {}

    fetcher._fetch_page = AsyncMock(return_value=warmup_response)
    fetcher._rebuild_http_client = AsyncMock()

    with pytest.raises(InstagramCommentsWarmupError) as exc_info:
        asyncio.run(fetcher.warmup())

    assert exc_info.value.error_code == "instagram_comments_warmup_no_cookies"
    assert exc_info.value.retryable is True
    assert fetcher.runtime_metadata["warmup_cookie_count"] == 0
    fetcher._rebuild_http_client.assert_not_awaited()


def test_warmup_transport_ssl_error_is_retryable() -> None:
    fetcher = _build_fetcher()
    fetcher._fetch_page = AsyncMock(side_effect=OSError("[SSL: WRONG_VERSION_NUMBER] wrong version number"))
    fetcher._rebuild_http_client = AsyncMock()

    with pytest.raises(InstagramCommentsWarmupError) as exc_info:
        asyncio.run(fetcher.warmup())

    assert exc_info.value.error_code == "instagram_comments_warmup_transport_error"
    assert exc_info.value.retryable is True
    assert fetcher.runtime_metadata["retry_reason_counts"]["warmup_transport_error"] == 1
    fetcher._rebuild_http_client.assert_not_awaited()


def test_warmup_transport_http_response_code_failure_is_retryable_when_homepage_fallback_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_WARMUP_HOMEPAGE_FALLBACK", "0")
    fetcher = _build_fetcher()
    fetcher._fetch_page = AsyncMock(
        side_effect=RuntimeError("Page.goto: net::ERR_HTTP_RESPONSE_CODE_FAILURE at https://www.instagram.com/")
    )
    fetcher._rebuild_http_client = AsyncMock()

    with pytest.raises(InstagramCommentsWarmupError) as exc_info:
        asyncio.run(fetcher.warmup())

    assert exc_info.value.error_code == "instagram_comments_warmup_transport_error"
    assert exc_info.value.retryable is True
    assert fetcher.runtime_metadata["retry_reason_counts"]["warmup_transport_error"] == 1
    fetcher._rebuild_http_client.assert_not_awaited()


def test_warmup_transport_timed_out_is_retryable_when_homepage_fallback_disabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_WARMUP_HOMEPAGE_FALLBACK", "0")
    fetcher = _build_fetcher()
    fetcher._fetch_page = AsyncMock(
        side_effect=RuntimeError(
            "Page.goto: net::ERR_TIMED_OUT at https://www.instagram.com/testaccount/\n"
            "Call log:\n"
            '  - navigating to "https://www.instagram.com/testaccount/", waiting until "load"\n'
        )
    )
    fetcher._rebuild_http_client = AsyncMock()

    with pytest.raises(InstagramCommentsWarmupError) as exc_info:
        asyncio.run(fetcher.warmup())

    assert exc_info.value.error_code == "instagram_comments_warmup_transport_error"
    assert exc_info.value.retryable is True
    assert fetcher.runtime_metadata["retry_reason_counts"]["warmup_transport_error"] == 1
    fetcher._rebuild_http_client.assert_not_awaited()


def test_warmup_response_code_failure_falls_back_to_homepage() -> None:
    fetcher = _build_fetcher()

    warmup_response = MagicMock()
    warmup_response.status = 200
    warmup_response.text = ""
    warmup_response.cookies = {"csrftoken": "fresh-csrf-token", "sessionid": "fresh-session"}

    fetcher._fetch_page = AsyncMock(
        side_effect=[
            RuntimeError("Page.goto: net::ERR_HTTP_RESPONSE_CODE_FAILURE at https://www.instagram.com/testaccount/"),
            warmup_response,
        ]
    )
    fetcher._rebuild_http_client = AsyncMock()

    asyncio.run(fetcher.warmup())

    assert fetcher._fetch_page.await_count == 2
    assert fetcher._fetch_page.await_args_list[0].args[0] == "https://www.instagram.com/testaccount/"
    assert fetcher._fetch_page.await_args_list[1].args[0] == "https://www.instagram.com/"
    assert fetcher._raw_cookies["sessionid"] == "fresh-session"
    assert fetcher.runtime_metadata["retry_reason_counts"]["warmup_homepage_fallback"] == 1
    fetcher._rebuild_http_client.assert_awaited_once()


def test_warmup_transport_failure_can_continue_with_existing_session_cookie() -> None:
    fetcher = _build_fetcher()
    fetcher._raw_cookies["sessionid"] = "existing-session"
    fetcher._fetch_page = AsyncMock(
        side_effect=[
            RuntimeError("Page.goto: net::ERR_HTTP_RESPONSE_CODE_FAILURE at https://www.instagram.com/testaccount/"),
            RuntimeError("Page.goto: net::ERR_HTTP_RESPONSE_CODE_FAILURE at https://www.instagram.com/"),
        ]
    )
    fetcher._rebuild_http_client = AsyncMock()

    asyncio.run(fetcher.warmup())

    assert fetcher._fetch_page.await_count == 2
    assert fetcher.runtime_metadata["retry_reason_counts"]["warmup_homepage_fallback"] == 1
    assert fetcher.runtime_metadata["retry_reason_counts"]["warmup_cookie_only_after_transport_error"] == 1
    assert "warmup_transport_error" not in fetcher.runtime_metadata["retry_reason_counts"]
    fetcher._rebuild_http_client.assert_awaited_once()


def test_warmup_auth_challenge_can_continue_with_existing_session_cookie() -> None:
    fetcher = _build_fetcher()
    fetcher._raw_cookies["sessionid"] = "existing-session"

    warmup_response = MagicMock()
    warmup_response.status = 200
    warmup_response.url = "https://www.instagram.com/"
    warmup_response.headers = {"content-type": "text/html"}
    warmup_response.text = "<html><title>Instagram</title><body>/challenge/ Continue</body></html>"
    warmup_response.cookies = {}

    fetcher._fetch_page = AsyncMock(return_value=warmup_response)
    fetcher._rebuild_http_client = AsyncMock()

    asyncio.run(fetcher.warmup())

    metadata = fetcher.runtime_metadata
    assert metadata["retry_reason_counts"]["warmup_cookie_only_after_auth_challenge"] == 1
    assert metadata["challenge_responses"]["total_count"] == 1
    assert metadata["challenge_responses"]["samples"][0]["reason"] == "warmup_auth_challenge"
    fetcher._rebuild_http_client.assert_awaited_once()


def test_warmup_auth_challenge_still_fails_without_existing_session_cookie() -> None:
    fetcher = _build_fetcher()

    warmup_response = MagicMock()
    warmup_response.status = 200
    warmup_response.url = "https://www.instagram.com/"
    warmup_response.headers = {"content-type": "text/html"}
    warmup_response.text = "<html><body>/checkpoint/ Login</body></html>"
    warmup_response.cookies = {}

    fetcher._fetch_page = AsyncMock(return_value=warmup_response)
    fetcher._rebuild_http_client = AsyncMock()

    with pytest.raises(InstagramCommentsWarmupError) as exc_info:
        asyncio.run(fetcher.warmup())

    assert exc_info.value.error_code == "instagram_comments_warmup_auth_failed"
    assert fetcher.runtime_metadata["challenge_responses"]["total_count"] == 1
    fetcher._rebuild_http_client.assert_not_awaited()


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


def test_select_comments_proxy_decodo_sticky_session(monkeypatch) -> None:
    monkeypatch.delenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_URLS", raising=False)
    monkeypatch.delenv("DECODO_PROXY_URL", raising=False)
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_PROVIDER", "decodo")
    monkeypatch.setenv("DECODO_USERNAME", "user1")
    monkeypatch.setenv("DECODO_PASSWORD", "p@ss!")
    monkeypatch.setenv("DECODO_GATEWAY", "gate.decodo.com:7000")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_FORCE_ROTATING_PROXY", "false")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_USE_STICKY_PROXY", "true")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_SESSION_TTL_SECONDS", "600")

    from trr_backend.socials.instagram.comments_scrapling.proxy import select_comments_proxy

    result = select_comments_proxy()
    assert result is not None
    assert result.session_mode == "sticky"
    assert "-session-" in result.browser_proxy["username"]
    assert "-sessionduration-10" in result.browser_proxy["username"]
    assert "sessionduration-10" in result.api_proxy_url


def test_job_runner_uses_shard_proxy_session_only_when_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr
    from trr_backend.socials.instagram.comments_scrapling.persistence import PersistedInstagramComments

    selected_session_keys: list[str | None] = []

    class _FakeFetcher:
        @property
        def runtime_metadata(self) -> dict[str, Any]:
            return {"transport": "test", "request_count": 1}

        async def warmup(self) -> None:
            return None

        async def fetch_comments_for_shortcode(self, *_args: Any, **_kwargs: Any) -> InstagramCommentsFetchResult:
            return InstagramCommentsFetchResult(comments=[object()], fetch_failed=False, auth_failed=False)

        async def aclose(self) -> None:
            return None

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
    monkeypatch.setattr(
        jr,
        "select_comments_proxy",
        lambda *, session_key=None: selected_session_keys.append(session_key),
    )
    monkeypatch.setattr(jr, "resolve_comments_scrapling_session", lambda **_: _fake_comments_session())
    monkeypatch.setattr(jr, "InstagramCommentsScraplingFetcher", lambda **_: _FakeFetcher())
    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_emit_job_progress", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_finish_job", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})
    monkeypatch.setattr(jr.pg, "db_connection", lambda **_kwargs: nullcontext(MagicMock()))

    base_job = {
        "id": "job-1",
        "run_id": "run-1",
        "config": {
            "mode": "profile",
            "account": "bravotv",
            "target_source_ids": ["SHORT1"],
            "comments_shard_index": 2,
            "comments_shard_count": 4,
            "comments_shard_target_count": 1,
            "fetch_replies": False,
        },
        "attempt_count": 1,
        "max_attempts": 1,
    }

    with patch(
        "trr_backend.socials.instagram.comments_scrapling.job_runner.pg.fetch_one",
        return_value={
            "id": "job-1",
            "status": "running",
            "worker_id": "test-worker",
            "claimed_at": datetime(2026, 5, 1, tzinfo=UTC),
        },
    ):
        jr.run_instagram_comments_scrapling_job(base_job, worker_id="test-worker")
        disabled_string_job = {
            **base_job,
            "id": "job-2",
            "config": {**base_job["config"], "comments_proxy_shard_sessions": "false"},
        }
        jr.run_instagram_comments_scrapling_job(disabled_string_job, worker_id="test-worker")
        shard_session_job = {
            **base_job,
            "id": "job-3",
            "config": {**base_job["config"], "comments_proxy_shard_sessions": True},
        }
        jr.run_instagram_comments_scrapling_job(shard_session_job, worker_id="test-worker")

    assert selected_session_keys == ["testaccount", "testaccount", "bravotv:comments:2"]


# ---------------------------------------------------------------------------
# Partial-progress persistence (job_runner integration)
# ---------------------------------------------------------------------------


def _fake_comments_session() -> MagicMock:
    fake_session = MagicMock()
    fake_session.cookies = []
    fake_session.auth_session.cookies = {}
    fake_session.auth_session.metadata = {"source": "test"}
    fake_session.auth_session.source = "browser_session"
    fake_session.auth_session.browser_account_id = "testaccount"
    fake_session.auth_session.session_account_id = "testaccount"
    fake_session.auth_session.validation_category = "validated"
    fake_session.auth_session.validation_reason = None
    fake_session.auth_session.validated = True
    fake_session.auth_session.stale_ok = False
    fake_session.auth_session.resolver_version = 2
    fake_session.browser_account_id = "testaccount"
    return fake_session


def _active_comments_job_fetch_one(final_status: str = "completed"):
    def _fake_fetch_one(sql: str, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
        normalized = " ".join(str(sql or "").split()).lower()
        if "select status, worker_id, claimed_at" in normalized:
            return {
                "status": "running",
                "worker_id": "test-worker",
                "claimed_at": datetime(2026, 5, 1, tzinfo=UTC),
            }
        if "select status from social.scrape_jobs" in normalized:
            return {"status": "running"}
        if "select status from social.scrape_runs" in normalized:
            return {"status": "running"}
        return {"id": "job-1", "status": final_status}

    return _fake_fetch_one


def test_incomplete_retry_stall_stops_repeated_zero_comment_hidden_gap() -> None:
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr

    job = {
        "items_found": 53,
        "metadata": {
            "incomplete_target_source_ids": ["SHORT1"],
            "incomplete_fetch_reasons": {"SHORT1": "hidden_comments_unresolved"},
            "retry_rebalance": {"remaining_target_source_ids": ["SHORT1"]},
        },
    }

    # SA-2 (comment-completeness): the stall give-up threshold is now 8 (was 2),
    # so the post must reach attempt_count >= 8 with no cumulative progress before
    # it can be declared stalled.
    stalled = jr._incomplete_retry_has_stalled(
        job=job,
        attempt_count=8,
        retryable_incomplete_targets=["SHORT1"],
        retry_fetch_reasons={"SHORT1": "hidden_comments_unresolved"},
        comments_fetched=53,
        zero_comment_incomplete_targets=["SHORT1"],
    )

    assert stalled is not None
    assert stalled["target_source_ids"] == ["SHORT1"]
    assert stalled["zero_comment_target_source_ids"] == ["SHORT1"]


def test_incomplete_retry_stall_does_not_stop_unseen_zero_comment_hidden_gap() -> None:
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr

    job = {
        "items_found": 53,
        "metadata": {
            "retry_rebalance": {"remaining_target_source_ids": ["SHORT1"]},
        },
    }

    stalled = jr._incomplete_retry_has_stalled(
        job=job,
        attempt_count=3,
        retryable_incomplete_targets=["SHORT1"],
        retry_fetch_reasons={"SHORT1": "hidden_comments_unresolved"},
        comments_fetched=53,
        zero_comment_incomplete_targets=["SHORT1"],
    )

    assert stalled is None


def test_incomplete_retry_stall_stops_repeated_partial_hidden_gap() -> None:
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr

    job = {
        "items_found": 53,
        "metadata": {
            "incomplete_target_source_ids": ["SHORT1"],
            "incomplete_fetch_reasons": {"SHORT1": "hidden_comments_unresolved"},
            "retry_rebalance": {"remaining_target_source_ids": ["SHORT1"]},
        },
    }

    # SA-2 (comment-completeness): stall give-up threshold is now 8 (was 2).
    stalled = jr._incomplete_retry_has_stalled(
        job=job,
        attempt_count=8,
        retryable_incomplete_targets=["SHORT1"],
        retry_fetch_reasons={"SHORT1": "hidden_comments_unresolved"},
        comments_fetched=53,
        zero_comment_incomplete_targets=[],
    )

    assert stalled is not None
    assert stalled["target_source_ids"] == ["SHORT1"]
    assert stalled["prior_items_found"] == 53


def test_incomplete_retry_stall_does_not_stop_repeated_reply_tail_gap() -> None:
    # SA-2 (comment-completeness): an exhausted reply-tail budget is now a
    # retryable condition, not a genuinely-unrecoverable one. It was removed from
    # _INCOMPLETE_RETRY_STALL_REASONS, so a repeated reply_tail_budget_exhausted
    # gap must NEVER count toward the stall give-up — the post keeps retrying so
    # the remaining replies can be filled.
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr

    job = {
        "items_found": 355,
        "metadata": {
            "incomplete_target_source_ids": ["SHORT1"],
            "incomplete_fetch_reasons": {"SHORT1": "reply_tail_budget_exhausted"},
            "retry_rebalance": {"remaining_target_source_ids": ["SHORT1"]},
        },
    }

    stalled = jr._incomplete_retry_has_stalled(
        job=job,
        attempt_count=8,
        retryable_incomplete_targets=["SHORT1"],
        retry_fetch_reasons={"SHORT1": "reply_tail_budget_exhausted"},
        comments_fetched=355,
    )

    assert stalled is None


def test_incomplete_retry_stall_stops_repeated_coauthor_recovered_gap() -> None:
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr

    job = {
        "items_found": 844,
        "metadata": {
            "incomplete_target_source_ids": ["SHORT1"],
            "incomplete_fetch_reasons": {"SHORT1": "coauthor_auth_relay_fallback_recovered"},
            "retry_rebalance": {"remaining_target_source_ids": ["SHORT1"]},
        },
    }

    # SA-2 (comment-completeness): stall give-up threshold is now 8 (was 2).
    stalled = jr._incomplete_retry_has_stalled(
        job=job,
        attempt_count=8,
        retryable_incomplete_targets=["SHORT1"],
        retry_fetch_reasons={"SHORT1": "coauthor_auth_relay_fallback_recovered"},
        comments_fetched=844,
    )

    assert stalled is not None
    assert stalled["target_source_ids"] == ["SHORT1"]
    assert stalled["fetch_reasons"] == {"SHORT1": "coauthor_auth_relay_fallback_recovered"}


def test_incomplete_retry_stall_uses_default_threshold_of_eight() -> None:
    # SA-2 (comment-completeness): the default stall give-up threshold is now 8
    # (was 2). A genuinely-unrecoverable reason that recurs with no cumulative
    # progress only stalls once attempt_count reaches the default of 8.
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr

    job = {
        "items_found": 76,
        "metadata": {
            "incomplete_target_source_ids": ["SHORT1", "SHORT2"],
            "incomplete_fetch_reasons": {
                "SHORT1": "hidden_comments_unresolved",
                "SHORT2": "hidden_comments_unresolved",
            },
            "retry_rebalance": {"remaining_target_source_ids": ["SHORT1", "SHORT2"]},
        },
    }

    # Below the new default threshold the post must keep retrying.
    assert (
        jr._incomplete_retry_has_stalled(
            job=job,
            attempt_count=2,
            retryable_incomplete_targets=["SHORT1", "SHORT2"],
            retry_fetch_reasons={
                "SHORT1": "hidden_comments_unresolved",
                "SHORT2": "hidden_comments_unresolved",
            },
            comments_fetched=76,
        )
        is None
    )

    stalled = jr._incomplete_retry_has_stalled(
        job=job,
        attempt_count=8,
        retryable_incomplete_targets=["SHORT1", "SHORT2"],
        retry_fetch_reasons={
            "SHORT1": "hidden_comments_unresolved",
            "SHORT2": "hidden_comments_unresolved",
        },
        comments_fetched=76,
    )

    assert stalled is not None
    assert stalled["stall_attempts"] == 8
    assert stalled["target_source_ids"] == ["SHORT1", "SHORT2"]


def test_terminal_missing_classified_targets_are_not_retried() -> None:
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr

    targets = jr._retryable_incomplete_target_source_ids(
        incomplete_target_source_ids=["TERMINAL1", "APPROVAL1", "RETRY1", "TERMINAL1"],
        incomplete_fetch_reasons={
            "TERMINAL1": jr._TERMINAL_MISSING_CLASSIFIED_REASON,
            "APPROVAL1": jr.APPROVAL_BLOCKED_MISSING_CLASSIFICATION_REASON,
            "RETRY1": "reply_tail_budget_exhausted",
        },
        auth_failed_target_source_ids=["AUTH1"],
    )

    assert targets == ["RETRY1", "AUTH1"]


def test_retryable_incomplete_targets_drop_currently_complete_rows() -> None:
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr

    class _FakeRepo:
        @staticmethod
        def _instagram_filter_incomplete_comment_targets(account_handle: str, targets: list[str]) -> list[str]:
            assert account_handle == "bravotv"
            assert targets == ["SHORT1", "SHORT2"]
            return ["SHORT2"]

    targets, reasons, skipped = jr._filter_retryable_incomplete_targets_against_current_db(
        account_handle="bravotv",
        retryable_incomplete_targets=["SHORT1", "SHORT2", "SHORT1"],
        retry_fetch_reasons={
            "SHORT1": "coauthor_auth_relay_fallback_recovered",
            "SHORT2": "reply_tail_incomplete",
        },
        repo=_FakeRepo(),
    )

    assert targets == ["SHORT2"]
    assert reasons == {"SHORT2": "reply_tail_incomplete"}
    assert skipped == ["SHORT1"]


def test_retryable_incomplete_targets_keep_retry_path_when_db_saturated() -> None:
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr

    class _FakeRepo:
        @staticmethod
        def _instagram_filter_incomplete_comment_targets(_account_handle: str, _targets: list[str]) -> list[str]:
            raise jr.pg.DatabaseServiceUnavailableError("db saturated")

    targets, reasons, skipped = jr._filter_retryable_incomplete_targets_against_current_db(
        account_handle="bravotv",
        retryable_incomplete_targets=["SHORT1", "SHORT2"],
        retry_fetch_reasons={
            "SHORT1": "coauthor_auth_relay_fallback_recovered",
            "SHORT2": "reply_tail_incomplete",
        },
        repo=_FakeRepo(),
    )

    assert targets == ["SHORT1", "SHORT2"]
    assert reasons == {
        "SHORT1": "coauthor_auth_relay_fallback_recovered",
        "SHORT2": "reply_tail_incomplete",
    }
    assert skipped == []


def test_completion_residual_gap_targets_from_health_returns_canary_gaps(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr

    canaries = ["DTgXh94kXyo", "DT_3qLDjo5T", "DYiDH6pN-1Z", "DVbFVXCDgeu"]
    captured: dict[str, Any] = {}

    def _fake_fetch_all(sql: str, params: list[Any]) -> list[dict[str, Any]]:
        captured["sql"] = " ".join(sql.split()).lower()
        captured["params"] = list(params)
        return [
            {
                "shortcode": shortcode,
                "instagram_reported_comments": 100,
                "facebook_reported_comments": 0,
                "saved_comment_count": 55,
                "saved_parent_comments": 50,
                "saved_child_replies": 5,
                "covered_comment_count": 0,
                "parent_capture_gap": 50,
                "parent_capture_rate_pct": 50.0,
                "last_comment_scraped_at": None,
            }
            for shortcode in canaries
        ]

    monkeypatch.setattr(jr.pg, "fetch_all", _fake_fetch_all)

    residual = jr._completion_residual_gap_targets_from_health(target_source_ids=canaries)

    assert [row["shortcode"] for row in residual] == canaries
    assert [row["parent_capture_gap"] for row in residual] == [50, 50, 50, 50]
    assert "from social.comment_capture_health" in captured["sql"]
    assert captured["params"] == [canaries, 1, 4]


def test_completion_residual_gap_health_check_saturation_retries_targets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr

    canaries = ["DTgXh94kXyo", "DT_3qLDjo5T", "DYiDH6pN-1Z", "DVbFVXCDgeu"]

    def _raise_saturated(*_args: Any, **_kwargs: Any) -> list[dict[str, Any]]:
        raise jr.pg.DatabaseServiceUnavailableError("saturated")

    monkeypatch.setattr(jr.pg, "fetch_all", _raise_saturated)

    with pytest.raises(jr.CommentsScraplingRuntimeError) as exc_info:
        jr._completion_residual_gap_targets_from_health(target_source_ids=canaries)

    exc = exc_info.value
    assert exc.error_code == "instagram_comments_health_gap_check_unavailable"
    assert exc.retryable is True
    assert exc.runtime_metadata["retry_target_source_ids"] == canaries
    assert exc.runtime_metadata["completion_status"] == "comment_capture_health_check_unavailable"


def test_job_runner_completion_health_gap_requeues_canary_shortcodes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr
    from trr_backend.socials.instagram.comments_scrapling.persistence import PersistedInstagramComments

    canaries = ["DTgXh94kXyo", "DT_3qLDjo5T", "DYiDH6pN-1Z", "DVbFVXCDgeu"]
    fetch_calls: list[str] = []
    finish_calls: list[dict[str, Any]] = []
    config_updates: list[dict[str, Any]] = []

    def fake_persist(*, shortcode: str, **_kwargs: Any) -> PersistedInstagramComments:
        return PersistedInstagramComments(
            post_id=f"post-{shortcode}",
            stored_total_comments=1,
            comments_upserted=1,
            comments_marked_missing=0,
            comment_media_mirror_jobs_enqueued=0,
            comment_media_mirror_job_enqueue_errors=0,
        )

    class _FakeFetcher:
        @property
        def runtime_metadata(self) -> dict[str, Any]:
            return {"transport": "test", "request_count": 1}

        async def warmup(self) -> None:
            return None

        async def fetch_comments_for_shortcode(self, shortcode: str, **_kwargs: Any) -> InstagramCommentsFetchResult:
            fetch_calls.append(shortcode)
            return InstagramCommentsFetchResult(comments=[object()], fetch_failed=False, auth_failed=False)

        async def aclose(self) -> None:
            return None

    fake_session = MagicMock()
    fake_session.cookies = []
    fake_session.auth_session.cookies = {}
    fake_session.auth_session.metadata = {"source": "test"}
    fake_session.browser_account_id = "testaccount"

    monkeypatch.setattr(jr, "persist_instagram_comments_for_post", fake_persist)
    monkeypatch.setattr(jr, "select_comments_proxy", lambda *, session_key=None: None)
    monkeypatch.setattr(jr, "resolve_comments_scrapling_session", lambda **_: fake_session)
    monkeypatch.setattr(jr, "InstagramCommentsScraplingFetcher", lambda **_: _FakeFetcher())
    monkeypatch.setattr(jr, "_load_expected_comment_counts", lambda **_kwargs: {})
    monkeypatch.setattr(jr, "_load_comment_target_metadata", lambda **_kwargs: {})
    monkeypatch.setattr(
        jr,
        "_completion_residual_gap_targets_from_health",
        lambda **_kwargs: [
            {
                "shortcode": shortcode,
                "instagram_reported_comments": 100,
                "saved_parent_comments": 50,
                "parent_capture_gap": 50,
            }
            for shortcode in canaries
        ],
    )
    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_emit_job_progress", lambda **_kwargs: None)
    monkeypatch.setattr(repo, "_finish_job", lambda job_id, **kwargs: finish_calls.append({"job_id": job_id, **kwargs}))
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})
    monkeypatch.setattr(repo, "_update_job_config", lambda job_id, **kwargs: config_updates.append(dict(kwargs)))
    monkeypatch.setattr(jr.pg, "db_connection", lambda **_kwargs: nullcontext(MagicMock()))

    job = {
        "id": "job-1",
        "run_id": "run-1",
        "config": {
            "mode": "profile",
            "account": "bravotv",
            "target_source_ids": list(canaries),
            "max_comments_per_post": 10,
            "fetch_replies": False,
        },
        "attempt_count": 1,
        "max_attempts": 3,
    }

    with patch(
        "trr_backend.socials.instagram.comments_scrapling.job_runner.pg.fetch_one",
        side_effect=_active_comments_job_fetch_one("retrying"),
    ):
        jr.run_instagram_comments_scrapling_job(job, worker_id="test-worker")

    assert fetch_calls == canaries
    assert finish_calls[-1]["status"] == "retrying"
    assert finish_calls[-1]["last_error_code"] == "instagram_comments_health_gap_incomplete"
    assert finish_calls[-1]["metadata"]["runtime_metadata"]["incomplete_target_source_ids"] == canaries
    assert finish_calls[-1]["metadata"]["runtime_metadata"]["completion_status"] == "comment_capture_health_incomplete"
    assert config_updates[-1]["config_updates"]["target_source_ids"] == canaries
    assert config_updates[-1]["config_updates"]["comments_retry_incomplete"] is True


def test_auto_auth_fallback_target_selection_is_flagged_and_gap_gated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr

    targets = jr._select_auto_auth_fallback_targets(
        config={},
        retryable_incomplete_targets=["SMALL", "BIG"],
        expected_comment_counts_by_shortcode={"SMALL": 25, "BIG": 6274},
    )

    assert targets == []

    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_AUTO_AUTH_FALLBACK", "1")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_AUTO_AUTH_FALLBACK_MIN_GAP", "100")

    targets = jr._select_auto_auth_fallback_targets(
        config={"auth_fallback_escalated_source_ids": ["DONE"]},
        retryable_incomplete_targets=["SMALL", "BIG", "DONE"],
        expected_comment_counts_by_shortcode={"SMALL": 25, "BIG": 6274, "DONE": 8000},
    )

    assert targets == ["BIG"]


def test_auto_auth_fallback_enqueue_payload_coercion_is_defined() -> None:
    """Regression: the public-comments auto-auth fallback completion branch
    referenced an undefined ``_metadata_dict``, raising NameError and failing
    every public comments shard that finished with incomplete targets (live run
    812dbbea: 6/6 shards, last_error_class=NameError). Guard the helper and the
    exact call-site expression so this cannot regress silently again."""

    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr

    # The helper must exist and coerce arbitrary values to a plain dict.
    assert jr._metadata_dict({"performed": True}) == {"performed": True}
    assert jr._metadata_dict(None) == {}
    assert jr._metadata_dict("not-a-dict") == {}

    # Reproduce the crashed call site for the escalated branch (must not raise).
    auto_fallback_result: dict | None = {
        "enqueue": {"performed": True},
        "created_target_job_count": 2,
    }
    enqueue_payload = jr._metadata_dict((auto_fallback_result or {}).get("enqueue"))
    assert bool(enqueue_payload.get("performed")) is True

    # And the no-result branch must degrade to an empty dict, not NameError.
    auto_fallback_result = None
    enqueue_payload = jr._metadata_dict((auto_fallback_result or {}).get("enqueue"))
    assert enqueue_payload == {}
    assert not enqueue_payload.get("performed")


def test_incomplete_retry_stall_stops_repeated_subset_of_prior_retry_targets() -> None:
    # SA-2 (comment-completeness): reply_tail_incomplete is now retryable (dropped
    # from the stall set), so this "subset of prior targets" regression uses a
    # still-unrecoverable reason (hidden_comments_unresolved) and the new default
    # give-up threshold of 8.
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr

    job = {
        "items_found": 355,
        "metadata": {
            "incomplete_target_source_ids": ["SHORT1", "SHORT2"],
            "incomplete_fetch_reasons": {
                "SHORT1": "hidden_comments_unresolved",
                "SHORT2": "hidden_comments_unresolved",
            },
            "retry_rebalance": {"remaining_target_source_ids": ["SHORT1", "SHORT2"]},
        },
    }

    stalled = jr._incomplete_retry_has_stalled(
        job=job,
        attempt_count=8,
        retryable_incomplete_targets=["SHORT1"],
        retry_fetch_reasons={"SHORT1": "hidden_comments_unresolved"},
        comments_fetched=355,
    )

    assert stalled is not None
    assert stalled["target_source_ids"] == ["SHORT1"]


def test_incomplete_retry_stall_does_not_stop_subset_without_prior_reason() -> None:
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr

    job = {
        "items_found": 355,
        "metadata": {
            "retry_rebalance": {"remaining_target_source_ids": ["SHORT1", "SHORT2"]},
        },
    }

    stalled = jr._incomplete_retry_has_stalled(
        job=job,
        attempt_count=3,
        retryable_incomplete_targets=["SHORT1"],
        retry_fetch_reasons={"SHORT1": "reply_tail_incomplete"},
        comments_fetched=355,
    )

    assert stalled is None


def test_incomplete_retry_stall_does_not_stop_target_without_current_reason() -> None:
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr

    job = {
        "items_found": 355,
        "metadata": {
            "incomplete_target_source_ids": ["SHORT1", "SHORT2"],
            "incomplete_fetch_reasons": {
                "SHORT1": "reply_tail_incomplete",
                "SHORT2": "reply_tail_incomplete",
            },
            "retry_rebalance": {"remaining_target_source_ids": ["SHORT1", "SHORT2"]},
        },
    }

    stalled = jr._incomplete_retry_has_stalled(
        job=job,
        attempt_count=3,
        retryable_incomplete_targets=["SHORT1", "SHORT2"],
        retry_fetch_reasons={"SHORT1": "reply_tail_incomplete"},
        comments_fetched=355,
    )

    assert stalled is None


def test_incomplete_retry_stall_does_not_stop_repeated_pagination_deadline_gap() -> None:
    # SA-2 (comment-completeness): a self-imposed clock-cut
    # (pagination_deadline_exceeded) is a retryable condition, not a
    # genuinely-unrecoverable one. It was removed from the stall reason set, so a
    # repeated deadline gap must NEVER count toward the give-up — the post is
    # re-driven with backoff. (Deadlines are also unbounded by default now.)
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr

    job = {
        "items_found": 80,
        "metadata": {
            "incomplete_target_source_ids": ["SHORT1"],
            "incomplete_fetch_reasons": {"SHORT1": "pagination_deadline_exceeded"},
            "retry_rebalance": {"remaining_target_source_ids": ["SHORT1"]},
        },
    }

    stalled = jr._incomplete_retry_has_stalled(
        job=job,
        attempt_count=8,
        retryable_incomplete_targets=["SHORT1"],
        retry_fetch_reasons={"SHORT1": "pagination_deadline_exceeded"},
        comments_fetched=80,
    )

    assert stalled is None


def test_incomplete_retry_stall_does_not_stop_repeated_transport_error_gap() -> None:
    # SA-2 (comment-completeness): a transport error/timeout is a retryable
    # condition, not a genuinely-unrecoverable one. It was removed from the stall
    # reason set, so a repeated transport gap must NEVER count toward the give-up,
    # even at the new default give-up threshold.
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr

    job = {
        "items_found": 80,
        "metadata": {
            "incomplete_target_source_ids": ["SHORT1"],
            "incomplete_fetch_reasons": {"SHORT1": "transport_error"},
            "retry_rebalance": {"remaining_target_source_ids": ["SHORT1"]},
        },
    }

    stalled = jr._incomplete_retry_has_stalled(
        job=job,
        attempt_count=8,
        retryable_incomplete_targets=["SHORT1"],
        retry_fetch_reasons={"SHORT1": "transport_error"},
        comments_fetched=80,
        zero_comment_incomplete_targets=["SHORT1"],
    )

    assert stalled is None


def test_incomplete_retry_stall_stops_repeated_persisted_reply_topology_gap() -> None:
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr

    job = {
        "items_found": 355,
        "metadata": {
            "incomplete_target_source_ids": ["SHORT1"],
            "incomplete_fetch_reasons": {"SHORT1": "persisted_reply_topology_gap"},
            "retry_rebalance": {"remaining_target_source_ids": ["SHORT1"]},
        },
    }

    # SA-2 (comment-completeness): stall give-up threshold is now 8 (was 2).
    stalled = jr._incomplete_retry_has_stalled(
        job=job,
        attempt_count=8,
        retryable_incomplete_targets=["SHORT1"],
        retry_fetch_reasons={"SHORT1": "persisted_reply_topology_gap"},
        comments_fetched=355,
    )

    assert stalled is not None
    assert stalled["target_source_ids"] == ["SHORT1"]


def test_incomplete_retry_stall_stops_repeated_html_challenge_gap() -> None:
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr

    job = {
        "items_found": 15,
        "metadata": {
            "incomplete_target_source_ids": ["SHORT1"],
            "incomplete_fetch_reasons": {"SHORT1": "html_challenge_or_auth_required"},
            "retry_rebalance": {"remaining_target_source_ids": ["SHORT1"]},
        },
    }

    # SA-2 (comment-completeness): stall give-up threshold is now 8 (was 2).
    stalled = jr._incomplete_retry_has_stalled(
        job=job,
        attempt_count=8,
        retryable_incomplete_targets=["SHORT1"],
        retry_fetch_reasons={"SHORT1": "html_challenge_or_auth_required"},
        comments_fetched=15,
    )

    assert stalled is not None
    assert stalled["target_source_ids"] == ["SHORT1"]


def test_incomplete_retry_stall_does_not_stop_transient_reasons() -> None:
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr

    job = {
        "items_found": 2,
        "metadata": {
            "incomplete_target_source_ids": ["SHORT1"],
            "retry_rebalance": {"remaining_target_source_ids": ["SHORT1"]},
        },
    }

    stalled = jr._incomplete_retry_has_stalled(
        job=job,
        attempt_count=3,
        retryable_incomplete_targets=["SHORT1"],
        retry_fetch_reasons={"SHORT1": "http_429"},
        comments_fetched=2,
    )

    assert stalled is None


def test_job_runner_aborts_queued_sibling_shards_after_run_level_auth_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr

    finish_calls: list[dict[str, Any]] = []

    class _FakeRepo:
        @staticmethod
        def _finish_job(job_id: str, **kwargs: Any) -> None:
            finish_calls.append({"job_id": job_id, **kwargs})

    monkeypatch.setattr(
        jr.pg,
        "fetch_all",
        lambda *_args, **_kwargs: [
            {"id": "sibling-1", "items_found": 0},
            {"id": "sibling-2", "items_found": 3},
        ],
    )
    # Blast-radius threshold: 1 other shard already failed run-fatally + this one = 2,
    # meeting the default SOCIAL_INSTAGRAM_COMMENTS_SIBLING_ABORT_MIN_FAILED=2, so the
    # cascade fires.
    monkeypatch.setattr(jr.pg, "fetch_one", lambda *_args, **_kwargs: {"failed_count": 1})

    aborted = jr._abort_queued_sibling_shards_after_run_fatal_error(
        repo=_FakeRepo,
        run_id="11111111-1111-1111-1111-111111111111",
        failed_job_id="22222222-2222-2222-2222-222222222222",
        stage="comments_scrapling",
        account_handle="thetraitorsus",
        mode="profile",
        source_scope="bravo",
        error_code="instagram_comments_auth_failed",
        error_class="CommentsScraplingRuntimeError",
        error_message="Instagram auth failed while fetching comments for DUvvcUSFmI0.",
    )

    assert aborted == 2
    assert [call["job_id"] for call in finish_calls] == ["sibling-1", "sibling-2"]
    assert {call["status"] for call in finish_calls} == {"failed"}
    assert {call["last_error_code"] for call in finish_calls} == {"instagram_comments_auth_failed"}
    assert finish_calls[0]["metadata"]["aborted_by_sibling_job_id"] == "22222222-2222-2222-2222-222222222222"
    assert finish_calls[0]["metadata"]["account"] == "thetraitorsus"


def test_job_runner_defers_sibling_abort_below_blast_radius_threshold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr

    finish_calls: list[dict[str, Any]] = []

    class _FakeRepo:
        @staticmethod
        def _finish_job(job_id: str, **kwargs: Any) -> None:
            finish_calls.append({"job_id": job_id, **kwargs})

    # No other shard has failed run-fatally yet, so this is the FIRST failure
    # (effective failed = 1 < default threshold 2) -> the abort is deferred and the
    # queued siblings keep running to tolerate a transient auth/proxy blip.
    monkeypatch.setattr(jr.pg, "fetch_one", lambda *_args, **_kwargs: {"failed_count": 0})

    def _unexpected_fetch_all(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("sibling query must not run when the abort is deferred")

    monkeypatch.setattr(jr.pg, "fetch_all", _unexpected_fetch_all)

    aborted = jr._abort_queued_sibling_shards_after_run_fatal_error(
        repo=_FakeRepo,
        run_id="11111111-1111-1111-1111-111111111111",
        failed_job_id="22222222-2222-2222-2222-222222222222",
        stage="comments_scrapling",
        account_handle="thetraitorsus",
        mode="profile",
        source_scope="bravo",
        error_code="instagram_comments_auth_failed",
        error_class="CommentsScraplingRuntimeError",
        error_message="Instagram auth failed while fetching comments.",
    )

    assert aborted == 0
    assert finish_calls == []


def test_job_runner_tracks_isolated_post_auth_failure_for_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr
    from trr_backend.socials.instagram.comments_scrapling.persistence import PersistedInstagramComments

    finish_calls: list[dict[str, Any]] = []
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
        InstagramCommentsFetchResult(
            comments=[],
            fetch_failed=True,
            auth_failed=True,
            fetch_reason="html_challenge_or_auth_required",
        ),
        InstagramCommentsFetchResult(comments=[object()], fetch_failed=False),
    ]
    fetch_call_idx = {"i": 0}

    class _FakeFetcher:
        @property
        def runtime_metadata(self) -> dict[str, Any]:
            return {"transport": "test", "request_count": fetch_call_idx["i"]}

        async def warmup(self) -> None:
            return None

        async def fetch_comments_for_shortcode(self, *_args: Any, **_kwargs: Any) -> InstagramCommentsFetchResult:
            idx = fetch_call_idx["i"]
            fetch_call_idx["i"] += 1
            return fetch_results[idx]

        async def aclose(self) -> None:
            return None

    monkeypatch.setattr(jr, "persist_instagram_comments_for_post", fake_persist)
    monkeypatch.setattr(jr, "select_comments_proxy", lambda *, session_key=None: None)
    monkeypatch.setattr(jr, "resolve_comments_scrapling_session", lambda **_: _fake_comments_session())
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
            "target_source_ids": ["AUTHFAIL", "OKPOST"],
            "max_comments_per_post": 10,
            "fetch_replies": False,
        },
        "attempt_count": 1,
        "max_attempts": 2,
    }

    with (
        patch(
            "trr_backend.socials.instagram.comments_scrapling.job_runner.pg.fetch_one",
            return_value={
                "id": "job-1",
                "status": "running",
                "worker_id": "test-worker",
                "claimed_at": datetime(2026, 5, 1, tzinfo=UTC),
            },
        ),
        patch("trr_backend.socials.instagram.comments_scrapling.job_runner.pg.fetch_all", return_value=[]),
    ):
        jr.run_instagram_comments_scrapling_job(job, worker_id="test-worker")

    assert finish_calls[-1]["status"] == "retrying"
    assert finish_calls[-1]["last_error_code"] == "instagram_comments_incomplete_retryable"
    assert persist_calls == ["OKPOST"]
    metadata = finish_calls[-1]["metadata"]
    assert [sample["shortcode"] for sample in metadata["post_latency"]["samples"]] == ["AUTHFAIL", "OKPOST"]
    assert metadata["post_latency"]["samples"][0]["completion_reason"] == "post_auth_failed_skipped"
    assert metadata["post_auth_failures"]["target_source_ids"] == ["AUTHFAIL"]
    assert metadata["post_auth_failures"]["fetch_reasons"] == {"AUTHFAIL": "html_challenge_or_auth_required"}
    assert metadata["auth_failed_target_source_ids"] == ["AUTHFAIL"]
    assert metadata["runtime_metadata"]["incomplete_target_source_ids"] == ["AUTHFAIL"]
    assert metadata["retry_rebalance"]["remaining_target_source_ids"] == ["AUTHFAIL"]


def test_job_runner_fails_immediately_on_target_browser_session_invalidation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr

    finish_calls: list[dict[str, Any]] = []
    fetch_calls: list[str] = []

    class _FakeFetcher:
        @property
        def runtime_metadata(self) -> dict[str, Any]:
            return {"transport": "test", "request_count": len(fetch_calls)}

        async def warmup(self) -> None:
            return None

        async def fetch_comments_for_shortcode(self, shortcode: str, **_kwargs: Any) -> InstagramCommentsFetchResult:
            fetch_calls.append(shortcode)
            return InstagramCommentsFetchResult(
                comments=[],
                fetch_failed=True,
                auth_failed=True,
                fetch_reason=BROWSER_SESSION_INVALIDATED_REASON,
            )

        async def aclose(self) -> None:
            return None

    monkeypatch.setattr(jr, "select_comments_proxy", lambda *, session_key=None: None)
    monkeypatch.setattr(jr, "resolve_comments_scrapling_session", lambda **_: _fake_comments_session())
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

    job = {
        "id": "job-1",
        "run_id": "run-1",
        "config": {
            "mode": "profile",
            "account": "bravotv",
            "target_source_ids": ["INVALIDATED", "SHOULDNOTRUN"],
            "max_comments_per_post": 10,
            "fetch_replies": False,
        },
        "attempt_count": 1,
        "max_attempts": 2,
    }

    with (
        patch(
            "trr_backend.socials.instagram.comments_scrapling.job_runner.pg.fetch_one",
            return_value={
                "id": "job-1",
                "status": "running",
                "worker_id": "test-worker",
                "claimed_at": datetime(2026, 5, 1, tzinfo=UTC),
            },
        ),
        patch("trr_backend.socials.instagram.comments_scrapling.job_runner.pg.fetch_all", return_value=[]),
    ):
        jr.run_instagram_comments_scrapling_job(job, worker_id="test-worker")

    assert fetch_calls == ["INVALIDATED"]
    assert finish_calls[-1]["status"] == "failed"
    assert finish_calls[-1]["last_error_code"] == "instagram_comments_browser_session_invalidated"
    metadata = finish_calls[-1]["metadata"]
    assert metadata["error_code"] == "instagram_comments_browser_session_invalidated"
    assert metadata["runtime_metadata"]["fetch_reason"] == BROWSER_SESSION_INVALIDATED_REASON


def test_job_runner_lease_check_rejects_requeued_job(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr

    monkeypatch.setattr(
        "trr_backend.socials.instagram.comments_scrapling.job_runner.pg.fetch_one",
        lambda *_, **__: {"status": "retrying", "worker_id": None, "claimed_at": None},
    )

    with pytest.raises(jr.ScraplingJobLeaseLostError) as exc_info:
        jr._raise_if_job_lease_lost(job_id="job-1", worker_id="worker-1")

    assert exc_info.value.job_status == "retrying"
    assert exc_info.value.job_worker_id is None


def test_job_runner_lease_check_accepts_child_worker_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr

    monkeypatch.setattr(
        "trr_backend.socials.instagram.comments_scrapling.job_runner.pg.fetch_one",
        lambda *_, **__: {
            "status": "running",
            "worker_id": "worker-parent",
            "claimed_at": datetime(2026, 5, 1, tzinfo=UTC),
        },
    )

    jr._raise_if_job_lease_lost(job_id="job-1", worker_id="worker-parent:p1")


def test_job_runner_stops_post_auth_failures_at_circuit_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr

    finish_calls: list[dict[str, Any]] = []
    fetch_calls: list[str] = []

    class _FakeFetcher:
        @property
        def runtime_metadata(self) -> dict[str, Any]:
            return {"transport": "test", "request_count": len(fetch_calls)}

        async def warmup(self) -> None:
            return None

        async def fetch_comments_for_shortcode(self, shortcode: str, **_kwargs: Any) -> InstagramCommentsFetchResult:
            fetch_calls.append(shortcode)
            return InstagramCommentsFetchResult(
                comments=[],
                fetch_failed=True,
                auth_failed=True,
                fetch_reason="html_challenge_or_auth_required",
            )

        async def aclose(self) -> None:
            return None

    monkeypatch.setattr(jr, "select_comments_proxy", lambda *, session_key=None: None)
    monkeypatch.setattr(jr, "resolve_comments_scrapling_session", lambda **_: _fake_comments_session())
    monkeypatch.setattr(jr, "InstagramCommentsScraplingFetcher", lambda **_: _FakeFetcher())
    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_emit_job_progress", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_finish_job", lambda job_id, **kwargs: finish_calls.append({"job_id": job_id, **kwargs}))
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})
    monkeypatch.setattr(repo, "_update_job_config", lambda *a, **k: None)

    job = {
        "id": "job-1",
        "run_id": "run-1",
        "config": {
            "mode": "profile",
            "account": "bravotv",
            "target_source_ids": ["AUTH1", "AUTH2", "AUTH3", "AUTH4"],
            "post_auth_failure_circuit_limit": 3,
            "max_comments_per_post": 10,
            "fetch_replies": False,
        },
        "attempt_count": 1,
        "max_attempts": 2,
    }

    with (
        patch(
            "trr_backend.socials.instagram.comments_scrapling.job_runner.pg.fetch_one",
            return_value={
                "id": "job-1",
                "status": "running",
                "worker_id": "test-worker",
                "claimed_at": datetime(2026, 5, 1, tzinfo=UTC),
            },
        ),
        patch("trr_backend.socials.instagram.comments_scrapling.job_runner.pg.fetch_all", return_value=[]),
    ):
        jr.run_instagram_comments_scrapling_job(job, worker_id="test-worker")

    assert finish_calls[-1]["status"] == "failed"
    assert fetch_calls == ["AUTH1", "AUTH2", "AUTH3"]
    finish_kwargs = finish_calls[-1]
    assert finish_kwargs["last_error_code"] == "instagram_comments_auth_failed"
    metadata = finish_kwargs["metadata"]
    assert metadata["post_auth_failures"]["target_source_ids"] == ["AUTH1", "AUTH2", "AUTH3"]
    assert metadata["post_auth_failures"]["circuit_limit"] == 3
    assert metadata["auth_failed_target_source_ids"] == ["AUTH1", "AUTH2", "AUTH3"]
    assert metadata["error_code"] == "instagram_comments_auth_failed"
    assert metadata["retryable"] is False
    assert metadata["runtime_metadata"]["post_auth_failure_circuit_open"] is True
    assert metadata["runtime_metadata"]["post_auth_failure_circuit_limit"] == 3
    assert metadata["runtime_metadata"]["consecutive_post_auth_failures"] == 3
    assert metadata["runtime_metadata"]["auth_failed_target_source_ids"] == ["AUTH1", "AUTH2", "AUTH3"]
    assert metadata["runtime_metadata"]["auth_failed_fetch_reasons"] == {
        "AUTH1": "html_challenge_or_auth_required",
        "AUTH2": "html_challenge_or_auth_required",
        "AUTH3": "html_challenge_or_auth_required",
    }
    assert metadata["runtime_metadata"]["incomplete_target_source_ids"] == [
        "AUTH1",
        "AUTH2",
        "AUTH3",
        "AUTH4",
    ]
    assert metadata["runtime_metadata"]["remaining_target_source_ids"] == ["AUTH4"]
    assert metadata["retry_rebalance"] == {
        "remaining_target_source_ids": ["AUTH1", "AUTH2", "AUTH3", "AUTH4"],
        "eligible": True,
    }


def test_comments_job_runner_stops_before_targets_when_warmup_has_no_cookies(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr

    finish_calls: list[dict[str, Any]] = []
    fetch_calls: list[str] = []
    persist_calls: list[str] = []

    class _FakeFetcher:
        @property
        def runtime_metadata(self) -> dict[str, Any]:
            return {
                "transport": "httpx_after_browser_warmup",
                "request_count": 1,
                "warmup_cookie_count": 0,
                "warmup_cookie_names": [],
            }

        async def warmup(self) -> None:
            raise InstagramCommentsWarmupError(
                "Instagram comments warmup did not bridge any cookies.",
                error_code="instagram_comments_warmup_no_cookies",
                retryable=True,
            )

        async def fetch_comments_for_shortcode(self, shortcode: str, **_kwargs: Any) -> InstagramCommentsFetchResult:
            fetch_calls.append(shortcode)
            return InstagramCommentsFetchResult()

        async def aclose(self) -> None:
            return None

    def fake_persist(*, shortcode: str, **_kwargs: Any) -> object:
        persist_calls.append(shortcode)
        return object()

    monkeypatch.setattr(jr, "persist_instagram_comments_for_post", fake_persist)
    monkeypatch.setattr(jr, "select_comments_proxy", lambda *, session_key=None: None)
    monkeypatch.setattr(jr, "resolve_comments_scrapling_session", lambda **_: _fake_comments_session())
    monkeypatch.setattr(jr, "InstagramCommentsScraplingFetcher", lambda **_: _FakeFetcher())
    monkeypatch.setattr(repo, "_finish_job", lambda job_id, **kwargs: finish_calls.append({"job_id": job_id, **kwargs}))
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})
    monkeypatch.setattr(jr.pg, "db_connection", lambda **_kwargs: nullcontext(MagicMock()))

    job = {
        "id": "job-1",
        "run_id": "run-1",
        "config": {
            "mode": "profile",
            "account": "bravotv",
            "target_source_ids": ["SHORT1", "SHORT2"],
            "max_comments_per_post": 10,
            "fetch_replies": False,
        },
        "attempt_count": 1,
        "max_attempts": 2,
    }

    with patch(
        "trr_backend.socials.instagram.comments_scrapling.job_runner.pg.fetch_one",
        side_effect=_active_comments_job_fetch_one("retrying"),
    ):
        payload = jr.run_instagram_comments_scrapling_job(job, worker_id="test-worker")

    assert payload["status"] == "retrying"
    assert fetch_calls == []
    assert persist_calls == []
    finish_kwargs = finish_calls[-1]
    assert finish_kwargs["status"] == "retrying"
    assert finish_kwargs["last_error_code"] == "instagram_comments_warmup_no_cookies"
    metadata = finish_kwargs["metadata"]
    assert metadata["error_code"] == "instagram_comments_warmup_no_cookies"
    assert metadata["auth_context"]["session_source"] == "browser_session"
    assert metadata["auth_context"]["session_account_id"] == "testaccount"
    assert metadata["auth_context"]["validation_category"] == "validated"
    assert metadata["runtime_metadata"]["warmup_cookie_count"] == 0
    assert metadata["fetcher_runtime"]["warmup_cookie_count"] == 0


def test_comments_job_runner_continues_after_endpoint_probe_auth_blocked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr
    from trr_backend.socials.instagram.comments_scrapling.persistence import PersistedInstagramComments

    finish_calls: list[dict[str, Any]] = []
    fetch_calls: list[str] = []
    probe = {
        "mode": "comments_endpoint",
        "shortcode": "SHORT1",
        "status": "auth_blocked",
        "result": "auth_blocked",
        "reason": "redirect_to_login",
        "proxy_fingerprint": "none",
        "transport": "httpx_after_browser_warmup",
    }

    class _FakeFetcher:
        @property
        def runtime_metadata(self) -> dict[str, Any]:
            return {
                "transport": "httpx_after_browser_warmup",
                "request_count": 1,
                "comments_auth_validation": dict(probe),
            }

        async def warmup(self) -> None:
            return None

        async def validate_comments_endpoint(self, shortcode: str, *, mode: str) -> dict[str, Any]:
            probe["shortcode"] = shortcode
            probe["mode"] = mode
            return dict(probe)

        async def fetch_comments_for_shortcode(self, shortcode: str, **_kwargs: Any) -> InstagramCommentsFetchResult:
            fetch_calls.append(shortcode)
            return InstagramCommentsFetchResult(
                comments=[_comment(f"comment-{shortcode}")],
                fetch_failed=False,
                auth_failed=False,
            )

        async def aclose(self) -> None:
            return None

    def fake_persist(*, shortcode: str, **_kwargs: Any) -> PersistedInstagramComments:
        return PersistedInstagramComments(
            post_id=f"post-{shortcode}",
            stored_total_comments=1,
            comments_upserted=1,
            comments_marked_missing=0,
            comment_media_mirror_jobs_enqueued=0,
            comment_media_mirror_job_enqueue_errors=0,
            comments_inserted=1,
            comments_refreshed=0,
            comments_changed=1,
        )

    monkeypatch.setattr(jr, "persist_instagram_comments_for_post", fake_persist)
    monkeypatch.setattr(jr, "select_comments_proxy", lambda *, session_key=None: None)
    monkeypatch.setattr(jr, "resolve_comments_scrapling_session", lambda **_: _fake_comments_session())
    monkeypatch.setattr(jr, "InstagramCommentsScraplingFetcher", lambda **_: _FakeFetcher())
    monkeypatch.setattr(jr, "_load_expected_comment_counts", lambda **_kwargs: {})
    monkeypatch.setattr(jr, "_load_comment_target_metadata", lambda **_kwargs: {})
    monkeypatch.setattr(repo, "_finish_job", lambda job_id, **kwargs: finish_calls.append({"job_id": job_id, **kwargs}))
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})
    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(repo, "_emit_job_progress", lambda **_kwargs: True)
    monkeypatch.setattr(jr.pg, "db_connection", lambda **_kwargs: nullcontext(MagicMock()))

    job = {
        "id": "job-1",
        "run_id": "run-1",
        "config": {
            "mode": "profile",
            "account": "bravotv",
            "target_source_ids": ["SHORT1", "SHORT2"],
            "max_comments_per_post": 10,
            "fetch_replies": False,
        },
        "attempt_count": 1,
        "max_attempts": 2,
    }

    with patch(
        "trr_backend.socials.instagram.comments_scrapling.job_runner.pg.fetch_one",
        side_effect=_active_comments_job_fetch_one("completed"),
    ):
        payload = jr.run_instagram_comments_scrapling_job(job, worker_id="test-worker")

    assert payload["status"] == "completed"
    assert fetch_calls == ["SHORT1", "SHORT2"]
    finish_kwargs = finish_calls[-1]
    assert finish_kwargs["status"] == "completed"
    metadata = finish_kwargs["metadata"]
    assert metadata["fetcher_runtime"]["comments_auth_validation"]["reason"] == "redirect_to_login"
    assert metadata["comments_endpoint_probe"]["advisory_continue"] is True
    assert metadata["comments_endpoint_probe"]["advisory_reason"] == "redirect_to_login"
    assert metadata["persist_counters"]["comments_inserted"] == 2


def test_comments_job_runner_skips_duplicate_public_replay_after_auth_block(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr

    finish_calls: list[dict[str, Any]] = []
    captured_config_updates: list[dict[str, Any]] = []
    probe = {
        "mode": "comments_endpoint",
        "shortcode": "SHORT1",
        "status": "auth_blocked",
        "result": "auth_blocked",
        "reason": "redirect_to_login",
        "transport": "httpx_after_browser_warmup",
    }

    class _FakeFetcher:
        @property
        def runtime_metadata(self) -> dict[str, Any]:
            return {
                "transport": "httpx_after_browser_warmup",
                "request_count": 1,
                "comments_auth_validation": dict(probe),
            }

        async def warmup(self) -> None:
            return None

        async def validate_comments_endpoint(self, shortcode: str, *, mode: str) -> dict[str, Any]:
            probe["shortcode"] = shortcode
            probe["mode"] = mode
            return dict(probe)

        async def fetch_comments_for_shortcode(self, shortcode: str, **_kwargs: Any) -> InstagramCommentsFetchResult:
            raise AssertionError(f"duplicate public replay was not skipped for {shortcode}")

        async def aclose(self) -> None:
            return None

    monkeypatch.setattr(jr, "select_comments_proxy", lambda *, session_key=None: None)
    monkeypatch.setattr(jr, "resolve_comments_scrapling_session", lambda **_: _fake_comments_session())
    monkeypatch.setattr(jr, "InstagramCommentsScraplingFetcher", lambda **_: _FakeFetcher())
    monkeypatch.setattr(jr, "_load_expected_comment_counts", lambda **_kwargs: {"SHORT1": 171})
    monkeypatch.setattr(jr, "_load_comment_target_metadata", lambda **_kwargs: {})
    monkeypatch.setattr(jr, "_load_instagram_comments_audit_cursor_resume_metadata", lambda **_kwargs: {})
    monkeypatch.setattr(
        jr,
        "_load_public_replay_guard_rows",
        lambda **_kwargs: {
            "SHORT1": {
                "saved_comment_count": 124,
                "prior_public_fetched_comment_count": 71,
                "materialized_post_count": 1,
                "prior_public_audit_at": "2026-06-22T12:00:00+00:00",
            }
        },
    )

    def fake_update_job_config(_job_id: str, *, config_updates: dict[str, Any]) -> None:
        captured_config_updates.append(dict(config_updates))

    monkeypatch.setattr(repo, "_instagram_filter_incomplete_comment_targets", lambda _account, targets: list(targets))
    monkeypatch.setattr(repo, "_update_job_config", fake_update_job_config)
    monkeypatch.setattr(repo, "_finish_job", lambda job_id, **kwargs: finish_calls.append({"job_id": job_id, **kwargs}))
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})
    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(repo, "_emit_job_progress", lambda **_kwargs: True)

    job = {
        "id": "job-1",
        "run_id": "run-1",
        "config": {
            "mode": "profile",
            "account": "bravotv",
            "target_source_ids": ["SHORT1"],
            "max_comments_per_post": 0,
            "fetch_replies": False,
        },
        "attempt_count": 1,
        "max_attempts": 3,
    }

    with patch(
        "trr_backend.socials.instagram.comments_scrapling.job_runner.pg.fetch_one",
        side_effect=_active_comments_job_fetch_one("retrying"),
    ):
        payload = jr.run_instagram_comments_scrapling_job(job, worker_id="test-worker")

    assert payload["status"] == "retrying"
    assert captured_config_updates[-1]["target_source_ids"] == ["SHORT1"]
    finish_kwargs = finish_calls[-1]
    assert finish_kwargs["status"] == "retrying"
    metadata = finish_kwargs["metadata"]
    assert metadata["fetch_counters"]["comments_fetched"] == 0
    assert metadata["public_replay_guard"]["skipped_target_source_ids"] == ["SHORT1"]
    assert metadata["auth_failed_fetch_reasons"] == {
        "SHORT1": jr._PUBLIC_REPLAY_GUARD_FETCH_REASON,
    }
    assert metadata["post_latency"]["samples"][0]["completion_reason"] == (
        "auth_blocked_existing_public_coverage_skipped"
    )
    assert metadata["post_latency"]["samples"][0]["public_replay_guard"]["saved_comment_count"] == 124


def test_comments_job_runner_blocks_browser_session_invalidation_before_rendered_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr

    finish_calls: list[dict[str, Any]] = []
    fetch_calls: list[str] = []
    probe = {
        "mode": "comments_endpoint",
        "shortcode": "SHORT1",
        "status": "auth_blocked",
        "result": "auth_blocked",
        "reason": BROWSER_SESSION_INVALIDATED_REASON,
        "proxy_fingerprint": "none",
        "transport": "httpx_after_browser_warmup",
    }

    class _FakeFetcher:
        @property
        def runtime_metadata(self) -> dict[str, Any]:
            return {
                "transport": "httpx_after_browser_warmup",
                "request_count": 1,
                "comments_auth_validation": dict(probe),
            }

        async def warmup(self) -> None:
            return None

        async def validate_comments_endpoint(self, shortcode: str, *, mode: str) -> dict[str, Any]:
            probe["shortcode"] = shortcode
            probe["mode"] = mode
            return dict(probe)

        async def fetch_comments_for_shortcode(self, shortcode: str, **_kwargs: Any) -> InstagramCommentsFetchResult:
            fetch_calls.append(shortcode)
            raise AssertionError("invalidated browser session must block before target fetch")

        async def aclose(self) -> None:
            return None

    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_AUTH_RENDERED_FALLBACK", "1")
    monkeypatch.setattr(jr, "select_comments_proxy", lambda *, session_key=None: None)
    monkeypatch.setattr(jr, "resolve_comments_scrapling_session", lambda **_: _fake_comments_session())
    monkeypatch.setattr(jr, "InstagramCommentsScraplingFetcher", lambda **_: _FakeFetcher())
    monkeypatch.setattr(jr, "_load_expected_comment_counts", lambda **_kwargs: {})
    monkeypatch.setattr(jr, "_load_comment_target_metadata", lambda **_kwargs: {})
    monkeypatch.setattr(repo, "_finish_job", lambda job_id, **kwargs: finish_calls.append({"job_id": job_id, **kwargs}))
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})
    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(repo, "_emit_job_progress", lambda **_kwargs: True)
    monkeypatch.setattr(jr.pg, "fetch_all", lambda *_args, **_kwargs: [])

    job = {
        "id": "job-1",
        "run_id": "run-1",
        "config": {
            "mode": "profile",
            "account": "bravotv",
            "target_source_ids": ["SHORT1", "SHORT2"],
            "max_comments_per_post": 10,
            "fetch_replies": False,
        },
        "attempt_count": 1,
        "max_attempts": 2,
    }

    with patch(
        "trr_backend.socials.instagram.comments_scrapling.job_runner.pg.fetch_one",
        side_effect=_active_comments_job_fetch_one("failed"),
    ):
        payload = jr.run_instagram_comments_scrapling_job(job, worker_id="test-worker")

    assert payload["status"] == "failed"
    assert fetch_calls == []
    finish_kwargs = finish_calls[-1]
    assert finish_kwargs["last_error_code"] == "instagram_comments_browser_session_invalidated"
    metadata = finish_kwargs["metadata"]
    assert metadata["error_code"] == "instagram_comments_browser_session_invalidated"
    assert metadata["runtime_metadata"]["comments_auth_validation"]["reason"] == BROWSER_SESSION_INVALIDATED_REASON


def test_comments_job_runner_continues_collaborator_fetch_after_endpoint_html_challenge(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr
    from trr_backend.socials.instagram.comments_scrapling.persistence import PersistedInstagramComments

    finish_calls: list[dict[str, Any]] = []
    fetch_calls: list[str] = []
    probe = {
        "mode": "comments_endpoint",
        "shortcode": "SHORT1",
        "status": "auth_blocked",
        "result": "auth_blocked",
        "reason": "html_challenge_or_auth_required",
        "proxy_fingerprint": "none",
        "transport": "httpx_after_browser_warmup",
    }
    target_metadata = {
        "source_id": "SHORT1",
        "profile_account": "thetraitorsus",
        "owner_username": "peacock",
        "collaborator_handles": ["thetraitorsus"],
        "is_collaborator_post": True,
    }

    class _FakeFetcher:
        @property
        def runtime_metadata(self) -> dict[str, Any]:
            return {
                "transport": "httpx_after_browser_warmup",
                "request_count": 1,
                "comments_auth_validation": dict(probe),
            }

        async def warmup(self) -> None:
            return None

        async def validate_comments_endpoint(self, shortcode: str, *, mode: str) -> dict[str, Any]:
            probe["shortcode"] = shortcode
            probe["mode"] = mode
            return dict(probe)

        async def fetch_comments_for_shortcode(self, shortcode: str, **kwargs: Any) -> InstagramCommentsFetchResult:
            fetch_calls.append(shortcode)
            assert kwargs["target_metadata"]["is_collaborator_post"] is True
            return InstagramCommentsFetchResult(
                comments=[_comment(f"comment-{shortcode}")],
                fetch_failed=False,
                auth_failed=False,
            )

        async def aclose(self) -> None:
            return None

    def fake_persist(*, shortcode: str, **_kwargs: Any) -> PersistedInstagramComments:
        return PersistedInstagramComments(
            post_id=f"post-{shortcode}",
            stored_total_comments=1,
            comments_upserted=1,
            comments_marked_missing=0,
            comment_media_mirror_jobs_enqueued=0,
            comment_media_mirror_job_enqueue_errors=0,
            comments_inserted=1,
            comments_refreshed=0,
            comments_changed=1,
        )

    monkeypatch.setattr(jr, "persist_instagram_comments_for_post", fake_persist)
    monkeypatch.setattr(jr, "select_comments_proxy", lambda *, session_key=None: None)
    monkeypatch.setattr(jr, "resolve_comments_scrapling_session", lambda **_: _fake_comments_session())
    monkeypatch.setattr(jr, "InstagramCommentsScraplingFetcher", lambda **_: _FakeFetcher())
    monkeypatch.setattr(jr, "_load_expected_comment_counts", lambda **_kwargs: {"SHORT1": 129})
    monkeypatch.setattr(jr, "_load_comment_target_metadata", lambda **_kwargs: {"SHORT1": dict(target_metadata)})
    monkeypatch.setattr(repo, "_finish_job", lambda job_id, **kwargs: finish_calls.append({"job_id": job_id, **kwargs}))
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})
    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(repo, "_emit_job_progress", lambda **_kwargs: True)
    monkeypatch.setattr(jr.pg, "db_connection", lambda **_kwargs: nullcontext(MagicMock()))

    job = {
        "id": "job-1",
        "run_id": "run-1",
        "config": {
            "mode": "profile",
            "account": "thetraitorsus",
            "target_source_ids": ["SHORT1"],
            "max_comments_per_post": 1,
            "fetch_replies": False,
        },
        "attempt_count": 1,
        "max_attempts": 3,
    }

    with patch(
        "trr_backend.socials.instagram.comments_scrapling.job_runner.pg.fetch_one",
        side_effect=_active_comments_job_fetch_one("completed"),
    ):
        payload = jr.run_instagram_comments_scrapling_job(job, worker_id="test-worker")

    assert payload["status"] == "completed"
    assert fetch_calls == ["SHORT1"]
    metadata = finish_calls[-1]["metadata"]
    assert metadata["comments_endpoint_probe"]["reason"] == "html_challenge_or_auth_required"
    assert metadata["comments_endpoint_probe"]["advisory_continue"] is True
    assert metadata["comments_endpoint_probe"]["advisory_reason"] == "html_challenge_or_auth_required"
    assert metadata["fetcher_runtime"]["comments_auth_validation"]["reason"] == "html_challenge_or_auth_required"


def test_comments_job_runner_continues_direct_fetch_after_endpoint_html_challenge(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr
    from trr_backend.socials.instagram.comments_scrapling.persistence import PersistedInstagramComments

    finish_calls: list[dict[str, Any]] = []
    fetch_calls: list[str] = []
    probe = {
        "mode": "comments_endpoint",
        "shortcode": "SHORT1",
        "status": "auth_blocked",
        "result": "auth_blocked",
        "reason": "html_challenge_or_auth_required",
        "proxy_fingerprint": "none",
        "transport": "httpx_after_browser_warmup",
    }
    target_metadata = {
        "source_id": "SHORT1",
        "profile_account": "thetraitorsus",
        "owner_username": "thetraitorsus",
        "collaborator_handles": [],
        "is_collaborator_post": False,
    }

    class _FakeFetcher:
        @property
        def runtime_metadata(self) -> dict[str, Any]:
            return {
                "transport": "httpx_after_browser_warmup",
                "request_count": 1,
                "comments_auth_validation": dict(probe),
            }

        async def warmup(self) -> None:
            return None

        async def validate_comments_endpoint(self, shortcode: str, *, mode: str) -> dict[str, Any]:
            probe["shortcode"] = shortcode
            probe["mode"] = mode
            return dict(probe)

        async def fetch_comments_for_shortcode(self, shortcode: str, **kwargs: Any) -> InstagramCommentsFetchResult:
            fetch_calls.append(shortcode)
            assert kwargs["target_metadata"]["is_collaborator_post"] is False
            return InstagramCommentsFetchResult(
                comments=[_comment(f"comment-{shortcode}")],
                fetch_failed=False,
                auth_failed=False,
            )

        async def aclose(self) -> None:
            return None

    def fake_persist(*, shortcode: str, **_kwargs: Any) -> PersistedInstagramComments:
        return PersistedInstagramComments(
            post_id=f"post-{shortcode}",
            stored_total_comments=1,
            comments_upserted=1,
            comments_marked_missing=0,
            comment_media_mirror_jobs_enqueued=0,
            comment_media_mirror_job_enqueue_errors=0,
            comments_inserted=1,
            comments_refreshed=0,
            comments_changed=1,
        )

    monkeypatch.setattr(jr, "persist_instagram_comments_for_post", fake_persist)
    monkeypatch.setattr(jr, "select_comments_proxy", lambda *, session_key=None: None)
    monkeypatch.setattr(jr, "resolve_comments_scrapling_session", lambda **_: _fake_comments_session())
    monkeypatch.setattr(jr, "InstagramCommentsScraplingFetcher", lambda **_: _FakeFetcher())
    monkeypatch.setattr(jr, "_load_expected_comment_counts", lambda **_kwargs: {"SHORT1": 1})
    monkeypatch.setattr(jr, "_load_comment_target_metadata", lambda **_kwargs: {"SHORT1": dict(target_metadata)})
    monkeypatch.setattr(repo, "_finish_job", lambda job_id, **kwargs: finish_calls.append({"job_id": job_id, **kwargs}))
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})
    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(repo, "_emit_job_progress", lambda **_kwargs: True)
    monkeypatch.setattr(jr.pg, "db_connection", lambda **_kwargs: nullcontext(MagicMock()))

    job = {
        "id": "job-1",
        "run_id": "run-1",
        "config": {
            "mode": "profile",
            "account": "thetraitorsus",
            "target_source_ids": ["SHORT1"],
            "max_comments_per_post": 1,
            "fetch_replies": False,
        },
        "attempt_count": 1,
        "max_attempts": 3,
    }

    with patch(
        "trr_backend.socials.instagram.comments_scrapling.job_runner.pg.fetch_one",
        side_effect=_active_comments_job_fetch_one("completed"),
    ):
        payload = jr.run_instagram_comments_scrapling_job(job, worker_id="test-worker")

    assert payload["status"] == "completed"
    assert fetch_calls == ["SHORT1"]
    metadata = finish_calls[-1]["metadata"]
    assert metadata["comments_endpoint_probe"]["reason"] == "html_challenge_or_auth_required"
    assert metadata["comments_endpoint_probe"]["advisory_continue"] is True
    assert metadata["comments_endpoint_probe"]["advisory_reason"] == "html_challenge_or_auth_required"
    assert metadata["fetcher_runtime"]["comments_auth_validation"]["reason"] == "html_challenge_or_auth_required"


def test_comments_job_runner_config_schema_only_skips_endpoint_probe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr
    from trr_backend.socials.instagram.comments_scrapling.persistence import PersistedInstagramComments

    finish_calls: list[dict[str, Any]] = []
    fetch_calls: list[str] = []

    def fake_persist(*, shortcode: str, **_kwargs: Any) -> PersistedInstagramComments:
        return PersistedInstagramComments(
            post_id=f"post-{shortcode}",
            stored_total_comments=1,
            comments_upserted=1,
            comments_marked_missing=0,
            comment_media_mirror_jobs_enqueued=0,
            comment_media_mirror_job_enqueue_errors=0,
            comments_inserted=1,
            comments_refreshed=0,
            comments_changed=1,
        )

    class _FakeFetcher:
        @property
        def runtime_metadata(self) -> dict[str, Any]:
            return {"transport": "test", "request_count": len(fetch_calls)}

        async def warmup(self) -> None:
            return None

        async def validate_comments_endpoint(self, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
            raise AssertionError("schema_only jobs should not run the comments endpoint preflight")

        async def fetch_comments_for_shortcode(self, shortcode: str, **_kwargs: Any) -> InstagramCommentsFetchResult:
            fetch_calls.append(shortcode)
            return InstagramCommentsFetchResult(comments=[object()], fetch_failed=False, auth_failed=False)

        async def aclose(self) -> None:
            return None

    fake_session = _fake_comments_session()
    fake_session.auth_session.metadata = {
        **dict(fake_session.auth_session.metadata or {}),
        "comments_auth_validation_mode": "comments_endpoint",
    }

    monkeypatch.setattr(jr, "persist_instagram_comments_for_post", fake_persist)
    monkeypatch.setattr(jr, "select_comments_proxy", lambda *, session_key=None: None)
    monkeypatch.setattr(jr, "resolve_comments_scrapling_session", lambda **_: fake_session)
    monkeypatch.setattr(jr, "InstagramCommentsScraplingFetcher", lambda **_: _FakeFetcher())
    monkeypatch.setattr(jr, "_load_expected_comment_counts", lambda **_kwargs: {})
    monkeypatch.setattr(jr, "_load_comment_target_metadata", lambda **_kwargs: {})
    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(repo, "_emit_job_progress", lambda **_kwargs: True)
    monkeypatch.setattr(repo, "_finish_job", lambda job_id, **kwargs: finish_calls.append({"job_id": job_id, **kwargs}))
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
            "comments_auth_validation_mode": "schema_only",
            "max_comments_per_post": 10,
            "fetch_replies": False,
        },
        "attempt_count": 1,
        "max_attempts": 1,
    }

    with patch(
        "trr_backend.socials.instagram.comments_scrapling.job_runner.pg.fetch_one",
        side_effect=_active_comments_job_fetch_one("completed"),
    ):
        payload = jr.run_instagram_comments_scrapling_job(job, worker_id="test-worker")

    assert payload["status"] == "completed"
    assert fetch_calls == ["SHORT1"]
    metadata = finish_calls[-1]["metadata"]
    assert metadata["auth_context"]["comments_auth_validation_mode"] == "schema_only"
    assert "comments_auth_validation" not in metadata["fetcher_runtime"]


def test_comments_job_runner_preserves_prior_retry_progress_when_endpoint_probe_is_advisory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr
    from trr_backend.socials.instagram.comments_scrapling.persistence import PersistedInstagramComments

    finish_calls: list[dict[str, Any]] = []
    fetch_calls: list[str] = []
    probe = {
        "mode": "comments_endpoint",
        "shortcode": "SHORT1",
        "status": "auth_blocked",
        "result": "auth_blocked",
        "reason": "redirect_to_login",
        "proxy_fingerprint": "none",
        "transport": "httpx_after_browser_warmup",
    }
    prior_counters = {
        "posts": 5,
        "comments": 120,
        "comments_upserted": 110,
        "comments_inserted": 3,
        "comments_refreshed": 107,
        "comments_changed": 25,
    }

    class _FakeFetcher:
        @property
        def runtime_metadata(self) -> dict[str, Any]:
            return {
                "transport": "httpx_after_browser_warmup",
                "request_count": 1,
                "comments_auth_validation": dict(probe),
            }

        async def warmup(self) -> None:
            return None

        async def validate_comments_endpoint(self, shortcode: str, *, mode: str) -> dict[str, Any]:
            probe["shortcode"] = shortcode
            probe["mode"] = mode
            return dict(probe)

        async def fetch_comments_for_shortcode(self, shortcode: str, **_kwargs: Any) -> InstagramCommentsFetchResult:
            fetch_calls.append(shortcode)
            return InstagramCommentsFetchResult(
                comments=[_comment(f"comment-{shortcode}")],
                fetch_failed=False,
                auth_failed=False,
            )

        async def aclose(self) -> None:
            return None

    def fake_persist(*, shortcode: str, **_kwargs: Any) -> PersistedInstagramComments:
        return PersistedInstagramComments(
            post_id=f"post-{shortcode}",
            stored_total_comments=1,
            comments_upserted=1,
            comments_marked_missing=0,
            comment_media_mirror_jobs_enqueued=0,
            comment_media_mirror_job_enqueue_errors=0,
            comments_inserted=1,
            comments_refreshed=0,
            comments_changed=1,
        )

    def fake_fetch_one(sql: str, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
        normalized = " ".join(str(sql or "").split()).lower()
        if "select metadata from social.scrape_jobs" in normalized:
            return {"metadata": {"cumulative_counters": dict(prior_counters)}}
        return _active_comments_job_fetch_one("completed")(sql)

    monkeypatch.setattr(jr, "persist_instagram_comments_for_post", fake_persist)
    monkeypatch.setattr(jr, "select_comments_proxy", lambda *, session_key=None: None)
    monkeypatch.setattr(jr, "resolve_comments_scrapling_session", lambda **_: _fake_comments_session())
    monkeypatch.setattr(jr, "InstagramCommentsScraplingFetcher", lambda **_: _FakeFetcher())
    monkeypatch.setattr(jr, "_load_expected_comment_counts", lambda **_kwargs: {})
    monkeypatch.setattr(jr, "_load_comment_target_metadata", lambda **_kwargs: {})
    monkeypatch.setattr(repo, "_finish_job", lambda job_id, **kwargs: finish_calls.append({"job_id": job_id, **kwargs}))
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})
    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(repo, "_emit_job_progress", lambda **_kwargs: True)
    monkeypatch.setattr(jr.pg, "db_connection", lambda **_kwargs: nullcontext(MagicMock()))

    job = {
        "id": "job-1",
        "run_id": "run-1",
        "config": {
            "mode": "profile",
            "account": "bravotv",
            "target_source_ids": ["SHORT1", "SHORT2"],
            "max_comments_per_post": 10,
            "fetch_replies": False,
        },
        "attempt_count": 2,
        "max_attempts": 2,
    }

    with patch("trr_backend.socials.instagram.comments_scrapling.job_runner.pg.fetch_one", side_effect=fake_fetch_one):
        payload = jr.run_instagram_comments_scrapling_job(job, worker_id="test-worker")

    assert payload["status"] == "completed"
    assert fetch_calls == ["SHORT1", "SHORT2"]
    finish_kwargs = finish_calls[-1]
    assert finish_kwargs["status"] == "completed"
    assert finish_kwargs["metadata"]["stage_counters"] == {"posts": 2, "comments": 2}
    assert finish_kwargs["metadata"]["comments_endpoint_probe"]["advisory_continue"] is True
    assert finish_kwargs["metadata"]["fetcher_runtime"]["comments_auth_validation"]["status"] == "auth_blocked"


def test_comments_job_runner_continues_after_endpoint_probe_transport_block(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr
    from trr_backend.socials.instagram.comments_scrapling.persistence import PersistedInstagramComments

    finish_calls: list[dict[str, Any]] = []
    fetch_calls: list[str] = []
    probe = {
        "mode": "comments_endpoint",
        "shortcode": "SHORT1",
        "status": "transport_blocked",
        "result": "transport_blocked",
        "reason": "transport_timeout",
        "retryable": True,
        "proxy_fingerprint": "none",
        "transport": "httpx_after_browser_warmup",
    }

    class _FakeFetcher:
        @property
        def runtime_metadata(self) -> dict[str, Any]:
            return {
                "transport": "httpx_after_browser_warmup",
                "request_count": 1,
                "comments_auth_validation": dict(probe),
            }

        async def warmup(self) -> None:
            return None

        async def validate_comments_endpoint(self, shortcode: str, *, mode: str) -> dict[str, Any]:
            probe["shortcode"] = shortcode
            probe["mode"] = mode
            return dict(probe)

        async def fetch_comments_for_shortcode(self, shortcode: str, **_kwargs: Any) -> InstagramCommentsFetchResult:
            fetch_calls.append(shortcode)
            return InstagramCommentsFetchResult(
                comments=[
                    _comment(
                        f"comment-{shortcode}",
                    )
                ],
                fetch_failed=False,
                auth_failed=False,
            )

        async def aclose(self) -> None:
            return None

    def fake_persist(*, shortcode: str, **_kwargs: Any) -> PersistedInstagramComments:
        return PersistedInstagramComments(
            post_id=f"post-{shortcode}",
            stored_total_comments=1,
            comments_upserted=1,
            comments_marked_missing=0,
            comment_media_mirror_jobs_enqueued=0,
            comment_media_mirror_job_enqueue_errors=0,
            comments_inserted=1,
            comments_refreshed=0,
            comments_changed=1,
        )

    monkeypatch.setattr(jr, "persist_instagram_comments_for_post", fake_persist)
    monkeypatch.setattr(jr, "select_comments_proxy", lambda *, session_key=None: None)
    monkeypatch.setattr(jr, "resolve_comments_scrapling_session", lambda **_: _fake_comments_session())
    monkeypatch.setattr(jr, "InstagramCommentsScraplingFetcher", lambda **_: _FakeFetcher())
    monkeypatch.setattr(jr, "_load_expected_comment_counts", lambda **_kwargs: {})
    monkeypatch.setattr(jr, "_load_comment_target_metadata", lambda **_kwargs: {})
    monkeypatch.setattr(repo, "_finish_job", lambda job_id, **kwargs: finish_calls.append({"job_id": job_id, **kwargs}))
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})
    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(repo, "_emit_job_progress", lambda **_kwargs: True)
    monkeypatch.setattr(repo, "_retry_backoff_seconds", lambda _attempt_count: 1)
    monkeypatch.setattr(jr.pg, "db_connection", lambda **_kwargs: nullcontext(MagicMock()))

    job = {
        "id": "job-1",
        "run_id": "run-1",
        "config": {
            "mode": "profile",
            "account": "bravotv",
            "target_source_ids": ["SHORT1", "SHORT2"],
            "max_comments_per_post": 10,
            "fetch_replies": False,
        },
        "attempt_count": 1,
        "max_attempts": 2,
    }

    with patch(
        "trr_backend.socials.instagram.comments_scrapling.job_runner.pg.fetch_one",
        side_effect=_active_comments_job_fetch_one("completed"),
    ):
        payload = jr.run_instagram_comments_scrapling_job(job, worker_id="test-worker")

    assert payload["status"] == "completed"
    assert fetch_calls == ["SHORT1", "SHORT2"]
    finish_kwargs = finish_calls[-1]
    assert finish_kwargs["status"] == "completed"
    assert finish_kwargs["items_found"] == 4
    metadata = finish_kwargs["metadata"]
    assert metadata["fetcher_runtime"]["comments_auth_validation"]["status"] == "transport_blocked"
    assert metadata["comments_endpoint_probe"]["advisory_continue"] is True
    assert metadata["persist_counters"]["comments_inserted"] == 2


def test_comments_job_runner_retries_raw_warmup_transport_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr

    finish_calls: list[dict[str, Any]] = []

    class _FakeFetcher:
        @property
        def runtime_metadata(self) -> dict[str, Any]:
            return {"transport": "test", "request_count": 1}

        async def warmup(self) -> None:
            raise OSError("[SSL: WRONG_VERSION_NUMBER] wrong version number (_ssl.c:2590)")

        async def fetch_comments_for_shortcode(self, *_args: Any, **_kwargs: Any) -> InstagramCommentsFetchResult:
            raise AssertionError("targets should not be fetched after warmup transport failure")

        async def aclose(self) -> None:
            return None

    monkeypatch.setattr(jr, "select_comments_proxy", lambda *, session_key=None: None)
    monkeypatch.setattr(jr, "resolve_comments_scrapling_session", lambda **_: _fake_comments_session())
    monkeypatch.setattr(jr, "InstagramCommentsScraplingFetcher", lambda **_: _FakeFetcher())
    monkeypatch.setattr(repo, "_finish_job", lambda job_id, **kwargs: finish_calls.append({"job_id": job_id, **kwargs}))
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})

    job = {
        "id": "job-1",
        "run_id": "run-1",
        "config": {
            "mode": "profile",
            "account": "bravotv",
            "target_source_ids": ["SHORT1", "SHORT2"],
            "max_comments_per_post": 10,
            "fetch_replies": False,
        },
        "attempt_count": 1,
        "max_attempts": 2,
    }

    with patch(
        "trr_backend.socials.instagram.comments_scrapling.job_runner.pg.fetch_one",
        side_effect=_active_comments_job_fetch_one("retrying"),
    ):
        payload = jr.run_instagram_comments_scrapling_job(job, worker_id="test-worker")

    assert payload["status"] == "retrying"
    finish_kwargs = finish_calls[-1]
    assert finish_kwargs["status"] == "retrying"
    assert finish_kwargs["last_error_code"] == "instagram_comments_transport_error"
    assert finish_kwargs["metadata"]["error_code"] == "instagram_comments_transport_error"


def test_comments_job_runner_treats_closed_ssl_connection_as_transport_error() -> None:
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr

    closed_resource_error = type("ClosedResourceError", (Exception,), {})

    assert jr._is_comments_transport_error(closed_resource_error())
    assert jr._is_comments_transport_error(Exception("SSL connection has been closed unexpectedly"))
    assert jr._is_comments_transport_error(Exception("Cannot send a request, as the client has been closed."))
    assert jr._is_comments_transport_error(Exception("[SSL] record layer failure (_ssl.c:2590)"))
    assert jr._is_comments_transport_error(
        Exception(
            "Page.goto: net::ERR_TIMED_OUT at https://www.instagram.com/thetraitorsus/\n"
            "Call log:\n"
            '  - navigating to "https://www.instagram.com/thetraitorsus/", waiting until "load"\n'
        )
    )
    assert jr._is_comments_transport_error(
        Exception(
            "Page.goto: net::ERR_CONNECTION_CLOSED at https://www.instagram.com/thetraitorsus/\n"
            "Call log:\n"
            '  - navigating to "https://www.instagram.com/thetraitorsus/", waiting until "load"\n'
        )
    )


def test_job_runner_partial_progress_persists_before_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """If posts 1 and 2 succeed but post 3 fails transiently, posts 1 and
    2 must already be persisted when the runtime error surfaces."""
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr
    from trr_backend.socials.instagram.comments_scrapling.persistence import PersistedInstagramComments

    finish_calls: list[dict[str, Any]] = []
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

    async def fake_fetch_method(shortcode, *, max_comments, fetch_replies, expected_comment_count=None, **_kwargs):
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
    monkeypatch.setattr(jr, "select_comments_proxy", lambda *, session_key=None: None)
    monkeypatch.setattr(jr, "resolve_comments_scrapling_session", lambda **_: fake_session)
    monkeypatch.setattr(jr, "InstagramCommentsScraplingFetcher", lambda **_: fake_fetcher)
    monkeypatch.setattr(jr, "_load_expected_comment_counts", lambda **_kwargs: {})
    monkeypatch.setattr(jr, "_load_comment_target_metadata", lambda **_kwargs: {})

    from trr_backend.repositories import social_season_analytics as repo

    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *a, **k: None)
    monkeypatch.setattr(
        repo,
        "_emit_job_progress",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(repo, "_finish_job", lambda job_id, **kwargs: finish_calls.append({"job_id": job_id, **kwargs}))
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
            "comments_shard_index": 1,
            "comments_shard_count": 3,
            "comments_shard_target_count": 3,
            "max_comments_per_post": 10,
            "fetch_replies": False,
        },
        "attempt_count": 1,
        "max_attempts": 3,
    }

    with patch(
        "trr_backend.socials.instagram.comments_scrapling.job_runner.pg.fetch_one",
        side_effect=_active_comments_job_fetch_one("retrying"),
    ):
        jr.run_instagram_comments_scrapling_job(job, worker_id="test-worker")

    assert persist_calls == ["SHORT1", "SHORT2"], (
        f"Expected partial persists for 1 and 2 before SHORT3 failure; got {persist_calls}"
    )
    metadata = finish_calls[-1]["metadata"]
    assert metadata["comments_shard_index"] == 1
    assert metadata["comments_shard_count"] == 3
    assert metadata["comments_shard_target_count"] == 3
    assert metadata["post_latency"]["sample_count"] == 3
    assert [sample["shortcode"] for sample in metadata["post_latency"]["samples"]] == ["SHORT1", "SHORT2", "SHORT3"]
    assert metadata["post_latency"]["samples"][2]["completion_reason"] == "post_fetch_failed_retryable_skipped"
    assert metadata["comment_completeness"]["complete_posts"] == 2
    assert metadata["retry_rebalance"] == {
        "remaining_target_source_ids": ["SHORT3"],
        "eligible": True,
    }


def test_job_runner_continues_after_first_retryable_post_fetch_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr
    from trr_backend.socials.instagram.comments_scrapling.persistence import PersistedInstagramComments

    finish_calls: list[dict[str, Any]] = []
    persist_calls: list[str] = []
    progress_activities: list[dict[str, Any] | None] = []

    def fake_persist(*, shortcode: str, **_kwargs: Any) -> PersistedInstagramComments:
        persist_calls.append(shortcode)
        return PersistedInstagramComments(
            post_id=f"post-{shortcode}",
            stored_total_comments=1,
            comments_upserted=1,
            comments_marked_missing=0,
            comment_media_mirror_jobs_enqueued=0,
            comment_media_mirror_job_enqueue_errors=0,
        )

    fetch_results = [
        InstagramCommentsFetchResult(
            comments=[],
            fetch_failed=True,
            fetch_reason="hidden_comments_unresolved",
            retryable=True,
            reported_comment_count=39,
        ),
        InstagramCommentsFetchResult(comments=[object()], fetch_failed=False),
        InstagramCommentsFetchResult(comments=[object()], fetch_failed=False),
    ]
    fetch_call_idx = {"i": 0}

    async def fake_fetch_method(shortcode, *, max_comments, fetch_replies, expected_comment_count=None, **_kwargs):
        idx = fetch_call_idx["i"]
        fetch_call_idx["i"] += 1
        return fetch_results[idx]

    fake_fetcher = MagicMock()
    fake_fetcher._request_count = 3
    fake_fetcher.warmup = AsyncMock()
    fake_fetcher.fetch_comments_for_shortcode = fake_fetch_method
    fake_fetcher.aclose = AsyncMock()
    fake_fetcher.runtime_metadata = {"transport": "test"}

    fake_session = MagicMock()
    fake_session.cookies = []
    fake_session.auth_session.cookies = {}
    fake_session.auth_session.metadata = {"source": "test"}
    fake_session.browser_account_id = "testaccount"

    monkeypatch.setattr(jr, "persist_instagram_comments_for_post", fake_persist)
    monkeypatch.setattr(jr, "select_comments_proxy", lambda *, session_key=None: None)
    monkeypatch.setattr(jr, "resolve_comments_scrapling_session", lambda **_: fake_session)
    monkeypatch.setattr(jr, "InstagramCommentsScraplingFetcher", lambda **_: fake_fetcher)
    monkeypatch.setattr(jr, "_load_expected_comment_counts", lambda **_kwargs: {})
    monkeypatch.setattr(jr, "_load_comment_target_metadata", lambda **_kwargs: {})

    from trr_backend.repositories import social_season_analytics as repo

    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *a, **k: None)
    monkeypatch.setattr(
        repo,
        "_emit_job_progress",
        lambda **kwargs: progress_activities.append(kwargs.get("activity")),
    )
    monkeypatch.setattr(repo, "_finish_job", lambda job_id, **kwargs: finish_calls.append({"job_id": job_id, **kwargs}))
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
            "comments_shard_index": 1,
            "comments_shard_count": 3,
            "comments_shard_target_count": 3,
            "max_comments_per_post": 0,
            "fetch_replies": False,
            "post_fetch_failure_circuit_limit": 3,
        },
        "attempt_count": 1,
        "max_attempts": 3,
    }

    with patch(
        "trr_backend.socials.instagram.comments_scrapling.job_runner.pg.fetch_one",
        side_effect=_active_comments_job_fetch_one("retrying"),
    ):
        jr.run_instagram_comments_scrapling_job(job, worker_id="test-worker")

    assert persist_calls == ["SHORT2", "SHORT3"]
    skip_activity = progress_activities[1]
    assert skip_activity is not None
    assert skip_activity["posts_checked"] == 1
    assert skip_activity["incomplete_target_source_ids"] == ["SHORT1"]
    assert skip_activity["incomplete_fetch_reasons"] == {"SHORT1": "hidden_comments_unresolved"}
    finish_kwargs = finish_calls[-1]
    assert finish_kwargs["status"] == "retrying"
    metadata = finish_kwargs["metadata"]
    assert metadata["post_latency"]["sample_count"] == 3
    assert [sample["shortcode"] for sample in metadata["post_latency"]["samples"]] == ["SHORT1", "SHORT2", "SHORT3"]
    assert metadata["post_latency"]["samples"][0]["completion_reason"] == "post_fetch_failed_retryable_skipped"
    assert metadata["post_latency"]["samples"][0]["reported_comment_count"] == 39
    assert metadata["comment_completeness"]["complete_posts"] == 2
    assert metadata["comment_completeness"]["incomplete_posts"] == 1
    assert metadata["retry_rebalance"] == {
        "remaining_target_source_ids": ["SHORT1"],
        "eligible": True,
    }
    assert metadata["post_fetch_failures"]["target_source_ids"] == ["SHORT1"]
    assert metadata["post_fetch_failures"]["fetch_reasons"] == {"SHORT1": "hidden_comments_unresolved"}


def test_job_runner_surfaces_coauthor_status_only_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr
    from trr_backend.socials.instagram.comments_scrapling.persistence import PersistedInstagramComments

    finish_calls: list[dict[str, Any]] = []
    target_metadata = {
        "source_id": "DU_oEbbgZfJ",
        "profile_account": "thetraitorsus",
        "source_account": "thetraitorsus",
        "username": "peacock",
        "owner_username": "peacock",
        "collaborators": ["thetraitorsus"],
        "is_collaborator_post": True,
    }

    def fake_persist(*, shortcode: str, **_kwargs: Any) -> PersistedInstagramComments:
        return PersistedInstagramComments(
            post_id=f"post-{shortcode}",
            stored_total_comments=1,
            comments_upserted=1,
            comments_marked_missing=0,
            comment_media_mirror_jobs_enqueued=0,
            comment_media_mirror_job_enqueue_errors=0,
        )

    async def fake_fetch_method(shortcode: str, **kwargs: Any) -> InstagramCommentsFetchResult:
        if shortcode == "DU_oEbbgZfJ":
            assert kwargs["target_metadata"] == target_metadata
            return InstagramCommentsFetchResult(
                comments=[],
                fetch_failed=True,
                fetch_reason="comments_endpoint_status_only",
                retryable=True,
                reported_comment_count=149,
            )
        return InstagramCommentsFetchResult(comments=[object()], fetch_failed=False)

    fake_fetcher = MagicMock()
    fake_fetcher.warmup = AsyncMock()
    fake_fetcher.fetch_comments_for_shortcode = fake_fetch_method
    fake_fetcher.aclose = AsyncMock()
    fake_fetcher.runtime_metadata = {"transport": "test"}

    monkeypatch.setattr(jr, "persist_instagram_comments_for_post", fake_persist)
    monkeypatch.setattr(jr, "select_comments_proxy", lambda *, session_key=None: None)
    monkeypatch.setattr(jr, "resolve_comments_scrapling_session", lambda **_: _fake_comments_session())
    monkeypatch.setattr(jr, "InstagramCommentsScraplingFetcher", lambda **_: fake_fetcher)
    monkeypatch.setattr(
        jr,
        "_load_expected_comment_counts",
        lambda **_kwargs: {"DU_oEbbgZfJ": 149, "SHORT2": 1},
    )
    monkeypatch.setattr(
        jr,
        "_load_comment_target_metadata",
        lambda **_kwargs: {"DU_oEbbgZfJ": dict(target_metadata)},
    )
    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *a, **k: True)
    monkeypatch.setattr(repo, "_emit_job_progress", lambda **_kwargs: None)
    monkeypatch.setattr(repo, "_finish_job", lambda job_id, **kwargs: finish_calls.append({"job_id": job_id, **kwargs}))
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})
    monkeypatch.setattr(jr.pg, "db_connection", lambda **_kwargs: nullcontext(MagicMock()))

    job = {
        "id": "job-1",
        "run_id": "run-1",
        "config": {
            "mode": "profile",
            "account": "thetraitorsus",
            "target_source_ids": ["DU_oEbbgZfJ", "SHORT2"],
            "comments_shard_index": 1,
            "comments_shard_count": 1,
            "comments_shard_target_count": 2,
            "max_comments_per_post": 0,
            "fetch_replies": False,
        },
        "attempt_count": 1,
        "max_attempts": 3,
    }

    with patch(
        "trr_backend.socials.instagram.comments_scrapling.job_runner.pg.fetch_one",
        side_effect=_active_comments_job_fetch_one("retrying"),
    ):
        jr.run_instagram_comments_scrapling_job(job, worker_id="test-worker")

    metadata = finish_calls[-1]["metadata"]
    assert metadata["post_fetch_failures"]["fetch_reasons"] == {"DU_oEbbgZfJ": "comments_endpoint_status_only"}
    assert metadata["post_fetch_failures"]["coauthor_status_only_target_source_ids"] == ["DU_oEbbgZfJ"]
    assert metadata["coauthor_status_only_target_source_ids"] == ["DU_oEbbgZfJ"]
    assert metadata["post_fetch_failures"]["target_metadata"]["DU_oEbbgZfJ"]["target_metadata"] == target_metadata
    assert metadata["runtime_metadata"]["coauthor_status_only_target_source_ids"] == ["DU_oEbbgZfJ"]


def test_job_runner_retries_single_incomplete_target_in_large_shard(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr
    from trr_backend.socials.instagram.comments_scrapling.persistence import PersistedInstagramComments

    finish_calls: list[dict[str, Any]] = []
    captured_config_updates: list[dict[str, Any]] = []
    target_source_ids = [f"SHORT{i}" for i in range(1, 9)]

    def fake_persist(*, shortcode: str, **_kwargs: Any) -> PersistedInstagramComments:
        return PersistedInstagramComments(
            post_id=f"post-{shortcode}",
            stored_total_comments=1,
            comments_upserted=1,
            comments_marked_missing=0,
            comment_media_mirror_jobs_enqueued=0,
            comment_media_mirror_job_enqueue_errors=0,
        )

    async def fake_fetch_method(shortcode: str, **_kwargs: Any) -> InstagramCommentsFetchResult:
        if shortcode == "SHORT1":
            return InstagramCommentsFetchResult(
                comments=[],
                fetch_failed=True,
                fetch_reason="hidden_comments_unresolved",
                retryable=True,
                reported_comment_count=39,
            )
        return InstagramCommentsFetchResult(comments=[object()], fetch_failed=False)

    fake_fetcher = MagicMock()
    fake_fetcher.warmup = AsyncMock()
    fake_fetcher.fetch_comments_for_shortcode = fake_fetch_method
    fake_fetcher.aclose = AsyncMock()
    fake_fetcher.runtime_metadata = {"transport": "test"}

    monkeypatch.setattr(jr, "persist_instagram_comments_for_post", fake_persist)
    monkeypatch.setattr(jr, "select_comments_proxy", lambda *, session_key=None: None)
    monkeypatch.setattr(jr, "resolve_comments_scrapling_session", lambda **_: _fake_comments_session())
    monkeypatch.setattr(jr, "InstagramCommentsScraplingFetcher", lambda **_: fake_fetcher)
    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_emit_job_progress", lambda **_kwargs: None)
    monkeypatch.setattr(repo, "_finish_job", lambda job_id, **kwargs: finish_calls.append({"job_id": job_id, **kwargs}))
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})

    def fake_update_job_config(_job_id: str, *, config_updates: dict[str, Any]) -> None:
        captured_config_updates.append(dict(config_updates))

    monkeypatch.setattr(repo, "_update_job_config", fake_update_job_config)
    monkeypatch.setattr(jr.pg, "db_connection", lambda **_kwargs: nullcontext(MagicMock()))

    job = {
        "id": "job-1",
        "run_id": "run-1",
        "config": {
            "mode": "profile",
            "account": "bravotv",
            "target_source_ids": target_source_ids,
            "comments_shard_index": 1,
            "comments_shard_count": 1,
            "comments_shard_target_count": len(target_source_ids),
            "max_comments_per_post": 0,
            "fetch_replies": False,
        },
        "attempt_count": 1,
        "max_attempts": 3,
    }

    with patch(
        "trr_backend.socials.instagram.comments_scrapling.job_runner.pg.fetch_one",
        side_effect=_active_comments_job_fetch_one("retrying"),
    ):
        jr.run_instagram_comments_scrapling_job(job, worker_id="test-worker")

    assert finish_calls[-1]["status"] == "retrying"
    assert captured_config_updates[-1]["target_source_ids"] == ["SHORT1"]
    metadata = finish_calls[-1]["metadata"]
    assert metadata["comment_completeness"]["complete_posts"] == 7
    assert metadata["comment_completeness"]["incomplete_posts"] == 1
    assert metadata["retry_rebalance"] == {"remaining_target_source_ids": ["SHORT1"], "eligible": True}


def test_job_runner_passes_top_level_resume_cursor_from_prior_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr
    from trr_backend.socials.instagram.comments_scrapling.persistence import PersistedInstagramComments

    fetch_kwargs: list[dict[str, Any]] = []

    class _FakeFetcher:
        @property
        def runtime_metadata(self) -> dict[str, Any]:
            return {"transport": "test", "request_count": 1}

        async def warmup(self) -> None:
            return None

        async def fetch_comments_for_shortcode(self, _shortcode: str, **kwargs: Any) -> InstagramCommentsFetchResult:
            fetch_kwargs.append(dict(kwargs))
            return InstagramCommentsFetchResult(comments=[object()], fetch_failed=False, auth_failed=False)

        async def aclose(self) -> None:
            return None

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
    monkeypatch.setattr(jr, "select_comments_proxy", lambda *, session_key=None: None)
    monkeypatch.setattr(jr, "resolve_comments_scrapling_session", lambda **_: _fake_comments_session())
    monkeypatch.setattr(jr, "InstagramCommentsScraplingFetcher", lambda **_: _FakeFetcher())
    monkeypatch.setattr(jr, "_load_expected_comment_counts", lambda **_kwargs: {})
    monkeypatch.setattr(jr, "_load_comment_target_metadata", lambda **_kwargs: {})
    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *a, **k: True)
    monkeypatch.setattr(repo, "_emit_job_progress", lambda **_kwargs: None)
    monkeypatch.setattr(repo, "_finish_job", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})
    monkeypatch.setattr(jr.pg, "db_connection", lambda **_kwargs: nullcontext(MagicMock()))

    job = {
        "id": "job-1",
        "run_id": "run-1",
        "status": "retrying",
        "config": {
            "mode": "profile",
            "account": "bravotv",
            "target_source_ids": ["SHORT1"],
            "max_comments_per_post": 0,
            "fetch_replies": False,
        },
        "metadata": {
            "top_level_checkpoints": [
                {
                    "target_shortcode": "SHORT1",
                    "stop_reason": "pagination_deadline_exceeded",
                    "next_top_level_cursor": "cursor-2",
                    "next_top_level_cursor_param": "max_id",
                }
            ]
        },
        "attempt_count": 2,
        "max_attempts": 3,
    }

    with patch(
        "trr_backend.socials.instagram.comments_scrapling.job_runner.pg.fetch_one",
        side_effect=_active_comments_job_fetch_one("completed"),
    ):
        jr.run_instagram_comments_scrapling_job(job, worker_id="test-worker")

    assert fetch_kwargs == [
        {
            "max_comments": 0,
            "fetch_replies": False,
            "expected_comment_count": None,
            "load_strategy": "instagram_comments_endpoint_cursor",
            # The runner now always forwards the job's attempt_count so the
            # fetcher's terminal-missing classifier can gate on exhaustion.
            "attempt_count": 2,
            "top_level_cursor": "cursor-2",
            "top_level_cursor_param": "max_id",
        }
    ]


def test_job_runner_passes_coauthor_target_metadata_to_fetcher(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr
    from trr_backend.socials.instagram.comments_scrapling.persistence import PersistedInstagramComments

    fetch_kwargs: list[dict[str, Any]] = []
    target_metadata = {
        "source_id": "SHORT1",
        "profile_account": "thetraitorsus",
        "source_account": "thetraitorsus",
        "username": "peacock",
        "owner_username": "peacock",
        "collaborators": ["thetraitorsus"],
        "media_type": "carousel",
        "product_type": "carousel_container",
        "materialized_post_id": "post-id",
        "profile_match_mode": "profile_source_account",
        "is_collaborator_post": True,
    }

    class _FakeFetcher:
        @property
        def runtime_metadata(self) -> dict[str, Any]:
            return {"transport": "test", "request_count": 1}

        async def warmup(self) -> None:
            return None

        async def fetch_comments_for_shortcode(self, _shortcode: str, **kwargs: Any) -> InstagramCommentsFetchResult:
            fetch_kwargs.append(dict(kwargs))
            return InstagramCommentsFetchResult(comments=[object()], fetch_failed=False, auth_failed=False)

        async def aclose(self) -> None:
            return None

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
    monkeypatch.setattr(jr, "select_comments_proxy", lambda *, session_key=None: None)
    monkeypatch.setattr(jr, "resolve_comments_scrapling_session", lambda **_: _fake_comments_session())
    monkeypatch.setattr(jr, "InstagramCommentsScraplingFetcher", lambda **_: _FakeFetcher())
    monkeypatch.setattr(jr, "_load_expected_comment_counts", lambda **_kwargs: {"SHORT1": 149})
    monkeypatch.setattr(jr, "_load_comment_target_metadata", lambda **_kwargs: {"SHORT1": dict(target_metadata)})
    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *a, **k: True)
    monkeypatch.setattr(repo, "_emit_job_progress", lambda **_kwargs: None)
    monkeypatch.setattr(repo, "_finish_job", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})
    monkeypatch.setattr(jr.pg, "db_connection", lambda **_kwargs: nullcontext(MagicMock()))

    job = {
        "id": "job-1",
        "run_id": "run-1",
        "status": "queued",
        "config": {
            "mode": "profile",
            "account": "thetraitorsus",
            "target_source_ids": ["SHORT1"],
            "max_comments_per_post": 0,
            "fetch_replies": False,
        },
        "attempt_count": 1,
        "max_attempts": 1,
    }

    with patch(
        "trr_backend.socials.instagram.comments_scrapling.job_runner.pg.fetch_one",
        side_effect=_active_comments_job_fetch_one("completed"),
    ):
        jr.run_instagram_comments_scrapling_job(job, worker_id="test-worker")

    assert fetch_kwargs == [
        {
            "max_comments": 0,
            "fetch_replies": False,
            "expected_comment_count": 149,
            "load_strategy": "instagram_comments_endpoint_cursor",
            # The runner now always forwards the job's attempt_count.
            "attempt_count": 1,
            "target_metadata": target_metadata,
        }
    ]


def test_job_runner_threads_single_session_strategy_and_persists_once_per_shortcode(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr
    from trr_backend.socials.instagram.comments_scrapling.persistence import PersistedInstagramComments

    fetch_kwargs: list[dict[str, Any]] = []
    persist_calls: list[str] = []
    finish_calls: list[dict[str, Any]] = []

    class _FakeFetcher:
        @property
        def runtime_metadata(self) -> dict[str, Any]:
            return {
                "transport": "test",
                "request_count": len(fetch_kwargs),
                "comments_load_strategy": {
                    "last": {"strategy_decision": {"selected_strategy": "single_session_load_all"}}
                },
            }

        async def warmup(self) -> None:
            return None

        async def fetch_comments_for_shortcode(self, shortcode: str, **kwargs: Any) -> InstagramCommentsFetchResult:
            fetch_kwargs.append(dict(kwargs))
            return InstagramCommentsFetchResult(
                comments=[_comment(f"{shortcode}-comment")],
                fetch_failed=False,
                auth_failed=False,
                diagnostic_metadata={
                    "strategy_decision": {"selected_strategy": kwargs.get("load_strategy")},
                    "api_pages_loaded": 1,
                    "merged_comments": 1,
                },
            )

        async def aclose(self) -> None:
            return None

    def fake_persist(**kwargs: Any) -> PersistedInstagramComments:
        persist_calls.append(str(kwargs.get("shortcode") or ""))
        return PersistedInstagramComments(
            post_id=f"post-{len(persist_calls)}",
            stored_total_comments=1,
            comments_upserted=1,
            comments_marked_missing=0,
            comment_media_mirror_jobs_enqueued=0,
            comment_media_mirror_job_enqueue_errors=0,
        )

    monkeypatch.setattr(jr, "persist_instagram_comments_for_post", fake_persist)
    monkeypatch.setattr(jr, "select_comments_proxy", lambda *, session_key=None: None)
    monkeypatch.setattr(jr, "resolve_comments_scrapling_session", lambda **_: _fake_comments_session())
    monkeypatch.setattr(jr, "InstagramCommentsScraplingFetcher", lambda **_: _FakeFetcher())
    monkeypatch.setattr(jr, "_load_expected_comment_counts", lambda **_kwargs: {})
    monkeypatch.setattr(jr, "_load_comment_target_metadata", lambda **_kwargs: {})
    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *a, **k: True)
    monkeypatch.setattr(repo, "_emit_job_progress", lambda **_kwargs: None)
    monkeypatch.setattr(repo, "_finish_job", lambda *args, **kwargs: finish_calls.append(dict(kwargs)))
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})
    monkeypatch.setattr(jr.pg, "db_connection", lambda **_kwargs: nullcontext(MagicMock()))

    job = {
        "id": "job-1",
        "run_id": "run-1",
        "status": "queued",
        "config": {
            "mode": "profile",
            "account": "thetraitorsus",
            "target_source_ids": ["SHORT1", "SHORT2"],
            "max_comments_per_post": 0,
            "fetch_replies": False,
            "comments_load_strategy": "single_session_load_all",
            "comments_session_scope": "profile_single_worker",
        },
        "attempt_count": 1,
        "max_attempts": 1,
    }

    with patch(
        "trr_backend.socials.instagram.comments_scrapling.job_runner.pg.fetch_one",
        side_effect=_active_comments_job_fetch_one("completed"),
    ):
        jr.run_instagram_comments_scrapling_job(job, worker_id="test-worker")

    assert [kwargs["load_strategy"] for kwargs in fetch_kwargs] == [
        "single_session_load_all",
        "single_session_load_all",
    ]
    assert persist_calls == ["SHORT1", "SHORT2"]
    assert len(persist_calls) == len(fetch_kwargs)
    metadata = finish_calls[-1]["metadata"]
    assert metadata["comments_load_strategy"] == "single_session_load_all"
    assert metadata["comments_session_scope"] == "profile_single_worker"
    assert metadata["comments_strategy"]["selected"] == "single_session_load_all"
    assert metadata["comments_strategy"]["same_job_auth_retry_suppressed"] is True
    assert metadata["comments_strategy"]["saved_once_per_post"] == {
        "enabled": True,
        "count": 2,
        "target_source_ids": ["SHORT1", "SHORT2"],
    }


def test_job_runner_does_not_resume_terminal_repeated_top_level_cursor() -> None:
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr

    job = {
        "metadata": {
            "top_level_checkpoints": [
                {
                    "target_shortcode": "SHORT1",
                    "stop_reason": "pagination_repeated_cursor",
                    "last_top_level_cursor": "stuck-cursor",
                    "last_top_level_cursor_param": "max_id",
                    "next_top_level_cursor": None,
                }
            ]
        }
    }

    assert jr._top_level_resume_cursors_from_job(job) == {}
    assert jr._top_level_resume_cursor_params_from_job(job) == {}


def test_job_runner_passes_reply_resume_cursors_from_prior_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr
    from trr_backend.socials.instagram.comments_scrapling.persistence import PersistedInstagramComments

    fetch_kwargs: list[dict[str, Any]] = []

    class _FakeFetcher:
        @property
        def runtime_metadata(self) -> dict[str, Any]:
            return {"transport": "test", "request_count": 1}

        async def warmup(self) -> None:
            return None

        async def fetch_comments_for_shortcode(self, _shortcode: str, **kwargs: Any) -> InstagramCommentsFetchResult:
            fetch_kwargs.append(dict(kwargs))
            return InstagramCommentsFetchResult(comments=[object()], fetch_failed=False, auth_failed=False)

        async def aclose(self) -> None:
            return None

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
    monkeypatch.setattr(jr, "select_comments_proxy", lambda *, session_key=None: None)
    monkeypatch.setattr(jr, "resolve_comments_scrapling_session", lambda **_: _fake_comments_session())
    monkeypatch.setattr(jr, "InstagramCommentsScraplingFetcher", lambda **_: _FakeFetcher())
    monkeypatch.setattr(jr, "_load_expected_comment_counts", lambda **_kwargs: {})
    monkeypatch.setattr(jr, "_load_comment_target_metadata", lambda **_kwargs: {})
    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *a, **k: True)
    monkeypatch.setattr(repo, "_emit_job_progress", lambda **_kwargs: None)
    monkeypatch.setattr(repo, "_finish_job", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})
    monkeypatch.setattr(jr.pg, "db_connection", lambda **_kwargs: nullcontext(MagicMock()))

    job = {
        "id": "job-1",
        "run_id": "run-1",
        "status": "retrying",
        "config": {
            "mode": "profile",
            "account": "bravotv",
            "target_source_ids": ["SHORT1"],
            "max_comments_per_post": 0,
            "fetch_replies": True,
        },
        "metadata": {
            "fetcher_runtime": {
                "reply_checkpoint_metadata": {
                    "items": [
                        {
                            "target_shortcode": "SHORT1",
                            "parent_comment_id": "parent-1",
                            "stop_reason": "pagination_deadline_exceeded",
                            "next_reply_cursor": "reply-cursor-2",
                            "next_reply_cursor_param": "max_id",
                        }
                    ]
                }
            }
        },
        "attempt_count": 2,
        "max_attempts": 3,
    }

    with patch(
        "trr_backend.socials.instagram.comments_scrapling.job_runner.pg.fetch_one",
        side_effect=_active_comments_job_fetch_one("completed"),
    ):
        jr.run_instagram_comments_scrapling_job(job, worker_id="test-worker")

    assert fetch_kwargs == [
        {
            "max_comments": 0,
            "fetch_replies": True,
            "expected_comment_count": None,
            "load_strategy": "instagram_comments_endpoint_cursor",
            # The runner now always forwards the job's attempt_count.
            "attempt_count": 2,
            "reply_resume_cursors": {"parent-1": "reply-cursor-2"},
            "reply_resume_cursor_params": {"parent-1": "max_id"},
        }
    ]


def test_job_runner_passes_top_level_resume_cursor_from_audit_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr
    from trr_backend.socials.instagram.comments_scrapling.persistence import PersistedInstagramComments

    fetch_kwargs: list[dict[str, Any]] = []

    class _FakeFetcher:
        @property
        def runtime_metadata(self) -> dict[str, Any]:
            return {"transport": "test", "request_count": 1}

        async def warmup(self) -> None:
            return None

        async def fetch_comments_for_shortcode(self, _shortcode: str, **kwargs: Any) -> InstagramCommentsFetchResult:
            fetch_kwargs.append(dict(kwargs))
            return InstagramCommentsFetchResult(comments=[object()], fetch_failed=False, auth_failed=False)

        async def aclose(self) -> None:
            return None

    audit_checkpoint = {
        "target_shortcode": "SHORT1",
        "stop_reason": "pagination_deadline_exceeded",
        "next_top_level_cursor": "audit-cursor-2",
        "next_top_level_cursor_param": "max_id",
    }
    audit_metadata = {
        "audit_cursor_resume": {
            "source_count": 1,
            "source_target_source_ids": ["SHORT1"],
            "top_level_resume_count": 1,
            "reply_resume_count": 0,
        },
        "top_level_checkpoint_summary": jr._checkpoint_summary([audit_checkpoint]),
    }

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
    monkeypatch.setattr(jr, "select_comments_proxy", lambda *, session_key=None: None)
    monkeypatch.setattr(jr, "resolve_comments_scrapling_session", lambda **_: _fake_comments_session())
    monkeypatch.setattr(jr, "InstagramCommentsScraplingFetcher", lambda **_: _FakeFetcher())
    monkeypatch.setattr(jr, "_load_expected_comment_counts", lambda **_kwargs: {})
    monkeypatch.setattr(
        jr,
        "_load_comment_target_metadata",
        lambda **_kwargs: {
            "SHORT1": {
                "source_id": "SHORT1",
                "materialized_post_id": "00000000-0000-0000-0000-000000000001",
            }
        },
    )
    monkeypatch.setattr(jr, "_load_instagram_comments_audit_cursor_resume_metadata", lambda **_kwargs: audit_metadata)
    monkeypatch.setattr(jr, "_insert_instagram_post_comments_audit", lambda **_kwargs: None)
    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *a, **k: True)
    monkeypatch.setattr(repo, "_emit_job_progress", lambda **_kwargs: None)
    monkeypatch.setattr(repo, "_finish_job", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})
    monkeypatch.setattr(jr.pg, "db_connection", lambda **_kwargs: nullcontext(MagicMock()))

    job = {
        "id": "job-1",
        "run_id": "run-1",
        "status": "retrying",
        "config": {
            "mode": "profile",
            "account": "bravotv",
            "target_source_ids": ["SHORT1"],
            "max_comments_per_post": 0,
            "fetch_replies": False,
        },
        "metadata": {},
        "attempt_count": 2,
        "max_attempts": 3,
    }

    with patch(
        "trr_backend.socials.instagram.comments_scrapling.job_runner.pg.fetch_one",
        side_effect=_active_comments_job_fetch_one("completed"),
    ):
        jr.run_instagram_comments_scrapling_job(job, worker_id="test-worker")

    assert fetch_kwargs == [
        {
            "max_comments": 0,
            "fetch_replies": False,
            "expected_comment_count": None,
            "load_strategy": "instagram_comments_endpoint_cursor",
            # The runner now always forwards the job's attempt_count.
            "attempt_count": 2,
            "target_metadata": {
                "source_id": "SHORT1",
                "materialized_post_id": "00000000-0000-0000-0000-000000000001",
            },
            "top_level_cursor": "audit-cursor-2",
            "top_level_cursor_param": "max_id",
        }
    ]


def test_job_runner_passes_reply_resume_cursor_from_audit_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr
    from trr_backend.socials.instagram.comments_scrapling.persistence import PersistedInstagramComments

    fetch_kwargs: list[dict[str, Any]] = []

    class _FakeFetcher:
        @property
        def runtime_metadata(self) -> dict[str, Any]:
            return {"transport": "test", "request_count": 1}

        async def warmup(self) -> None:
            return None

        async def fetch_comments_for_shortcode(self, _shortcode: str, **kwargs: Any) -> InstagramCommentsFetchResult:
            fetch_kwargs.append(dict(kwargs))
            return InstagramCommentsFetchResult(comments=[object()], fetch_failed=False, auth_failed=False)

        async def aclose(self) -> None:
            return None

    audit_checkpoint = {
        "target_shortcode": "SHORT1",
        "parent_comment_id": "parent-1",
        "stop_reason": "pagination_deadline_exceeded",
        "next_reply_cursor": "audit-reply-cursor-2",
        "next_reply_cursor_param": "max_id",
    }
    audit_metadata = {
        "audit_cursor_resume": {
            "source_count": 1,
            "source_target_source_ids": ["SHORT1"],
            "top_level_resume_count": 0,
            "reply_resume_count": 1,
        },
        "reply_checkpoint_summary": jr._checkpoint_summary([audit_checkpoint]),
    }

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
    monkeypatch.setattr(jr, "select_comments_proxy", lambda *, session_key=None: None)
    monkeypatch.setattr(jr, "resolve_comments_scrapling_session", lambda **_: _fake_comments_session())
    monkeypatch.setattr(jr, "InstagramCommentsScraplingFetcher", lambda **_: _FakeFetcher())
    monkeypatch.setattr(jr, "_load_expected_comment_counts", lambda **_kwargs: {})
    monkeypatch.setattr(jr, "_load_comment_target_metadata", lambda **_kwargs: {})
    monkeypatch.setattr(jr, "_load_persisted_replies_by_parent", lambda **_kwargs: {})
    monkeypatch.setattr(jr, "_load_instagram_comments_audit_cursor_resume_metadata", lambda **_kwargs: audit_metadata)
    monkeypatch.setattr(jr, "_insert_instagram_post_comments_audit", lambda **_kwargs: None)
    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *a, **k: True)
    monkeypatch.setattr(repo, "_emit_job_progress", lambda **_kwargs: None)
    monkeypatch.setattr(repo, "_finish_job", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})
    monkeypatch.setattr(jr.pg, "db_connection", lambda **_kwargs: nullcontext(MagicMock()))

    job = {
        "id": "job-1",
        "run_id": "run-1",
        "status": "retrying",
        "config": {
            "mode": "profile",
            "account": "bravotv",
            "target_source_ids": ["SHORT1"],
            "max_comments_per_post": 0,
            "fetch_replies": True,
        },
        "metadata": {},
        "attempt_count": 2,
        "max_attempts": 3,
    }

    with patch(
        "trr_backend.socials.instagram.comments_scrapling.job_runner.pg.fetch_one",
        side_effect=_active_comments_job_fetch_one("completed"),
    ):
        jr.run_instagram_comments_scrapling_job(job, worker_id="test-worker")

    assert fetch_kwargs == [
        {
            "max_comments": 0,
            "fetch_replies": True,
            "expected_comment_count": None,
            "load_strategy": "instagram_comments_endpoint_cursor",
            # The runner now always forwards the job's attempt_count.
            "attempt_count": 2,
            "reply_resume_cursors": {"parent-1": "audit-reply-cursor-2"},
            "reply_resume_cursor_params": {"parent-1": "max_id"},
        }
    ]


def test_audit_cursor_resume_ignores_terminal_repeated_cursor() -> None:
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr

    metadata = jr._audit_cursor_resume_metadata_from_rows(
        [
            {
                "shortcode": "SHORT1",
                "cursor_stop_reason": "pagination_repeated_cursor",
                "cursor_payload": {
                    "top_level_checkpoint": {
                        "target_shortcode": "SHORT1",
                        "stop_reason": "pagination_repeated_cursor",
                        "last_top_level_cursor": "stuck-cursor",
                        "last_top_level_cursor_param": "max_id",
                    }
                },
            }
        ],
        existing_top_level_cursors={},
        existing_reply_cursors={},
    )

    assert metadata == {}


def test_audit_cursor_resume_preserves_existing_job_metadata_precedence() -> None:
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr

    metadata = jr._audit_cursor_resume_metadata_from_rows(
        [
            {
                "shortcode": "SHORT1",
                "cursor_stop_reason": "pagination_deadline_exceeded",
                "cursor_payload": {
                    "top_level_checkpoint": {
                        "target_shortcode": "SHORT1",
                        "stop_reason": "pagination_deadline_exceeded",
                        "next_top_level_cursor": "older-audit-cursor",
                        "next_top_level_cursor_param": "max_id",
                    },
                    "reply_checkpoint_summary": {
                        "items": [
                            {
                                "parent_comment_id": "parent-1",
                                "stop_reason": "pagination_deadline_exceeded",
                                "next_reply_cursor": "older-reply-cursor",
                                "next_reply_cursor_param": "max_id",
                            }
                        ]
                    },
                },
            }
        ],
        existing_top_level_cursors={"SHORT1": "current-job-cursor"},
        existing_reply_cursors={"parent-1": "current-job-reply-cursor"},
    )

    assert metadata == {}


def test_audit_cursor_resume_repairs_degenerate_checkpoint_from_payload_cursor() -> None:
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr

    # SA-2 (comment-completeness): pagination_deadline_exceeded is no longer a
    # retryable audit-cursor stop reason (deadlines are unbounded by default and
    # the reason was dropped from the retryable set), so the resume normalizer now
    # skips it. Use a still-retryable mid-pagination stop reason so the degenerate
    # cursor-repair path (the actual subject of this test) is still exercised.
    metadata = jr._audit_cursor_resume_metadata_from_rows(
        [
            {
                "shortcode": "SHORT1",
                "cursor_stop_reason": "pagination_page_cap_reached",
                "cursor_param": "min_id",
                "cursor_min_id": "duplicate-cursor",
                "cursor_payload": {
                    "chosen_cursor": "next-page-cursor",
                    "chosen_cursor_param": "min_id",
                    "top_level_checkpoint": {
                        "target_shortcode": "SHORT1",
                        "stop_reason": "pagination_page_cap_reached",
                        "last_top_level_cursor": "duplicate-cursor",
                        "next_top_level_cursor": "duplicate-cursor",
                        "last_top_level_cursor_param": "min_id",
                        "next_top_level_cursor_param": "min_id",
                    },
                },
            }
        ],
        existing_top_level_cursors={},
        existing_reply_cursors={},
    )

    item = metadata["top_level_checkpoint_summary"]["items"][0]
    assert item["next_top_level_cursor"] == "next-page-cursor"
    assert item["next_top_level_cursor_param"] == "min_id"
    assert item["last_top_level_cursor"] == "duplicate-cursor"
    assert item["cursor_repair_applied"] is True
    assert item["cursor_repair_reason"] == "degenerate_top_level_cursor_replayed"
    assert item["cursor_repair_source"] == "cursor_payload.chosen_cursor"
    assert item["cursor_repair"]["from_next_top_level_cursor"] == "duplicate-cursor"
    assert item["cursor_repair"]["to_next_top_level_cursor"] == "next-page-cursor"
    assert metadata["audit_cursor_resume"]["cursor_repair_count"] == 1
    assert metadata["audit_cursor_resume"]["cursor_repaired_target_source_ids"] == ["SHORT1"]


def test_job_resume_cursors_ignore_degenerate_top_level_checkpoint() -> None:
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr

    job = {
        "metadata": {
            "top_level_checkpoint_summary": {
                "items": [
                    {
                        "target_shortcode": "SHORT1",
                        "stop_reason": "pagination_deadline_exceeded",
                        "last_top_level_cursor": "duplicate-cursor",
                        "next_top_level_cursor": "duplicate-cursor",
                        "last_top_level_cursor_param": "min_id",
                        "next_top_level_cursor_param": "min_id",
                    }
                ]
            }
        }
    }

    assert jr._top_level_resume_cursors_from_job(job) == {}
    assert jr._top_level_resume_cursor_params_from_job(job) == {}


def test_job_runner_uses_reply_only_retry_for_persisted_missing_reply_parents(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr
    from trr_backend.socials.instagram.comments_scrapling.persistence import PersistedInstagramComments

    persisted_parent = _comment("parent-1", reply_count=3)
    fetch_kwargs: list[dict[str, Any]] = []

    class _FakeFetcher:
        @property
        def runtime_metadata(self) -> dict[str, Any]:
            return {"transport": "test", "request_count": 1}

        async def warmup(self) -> None:
            return None

        async def fetch_comments_for_shortcode(self, _shortcode: str, **kwargs: Any) -> InstagramCommentsFetchResult:
            fetch_kwargs.append(dict(kwargs))
            return InstagramCommentsFetchResult(
                comments=[persisted_parent],
                fetch_failed=True,
                fetch_reason="reply_tail_incomplete",
                retryable=True,
                auth_failed=False,
            )

        async def aclose(self) -> None:
            return None

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
    monkeypatch.setattr(jr, "select_comments_proxy", lambda *, session_key=None: None)
    monkeypatch.setattr(jr, "resolve_comments_scrapling_session", lambda **_: _fake_comments_session())
    monkeypatch.setattr(jr, "InstagramCommentsScraplingFetcher", lambda **_: _FakeFetcher())
    monkeypatch.setattr(jr, "_load_expected_comment_counts", lambda **_kwargs: {})
    monkeypatch.setattr(jr, "_load_comment_target_metadata", lambda **_kwargs: {})
    monkeypatch.setattr(jr, "_load_persisted_replies_by_parent", lambda **_kwargs: {})
    monkeypatch.setattr(
        jr,
        "_load_persisted_top_level_comments_for_reply_retry",
        lambda **_kwargs: [persisted_parent],
    )
    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *a, **k: True)
    monkeypatch.setattr(repo, "_emit_job_progress", lambda **_kwargs: None)
    monkeypatch.setattr(repo, "_finish_job", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})
    monkeypatch.setattr(jr.pg, "db_connection", lambda **_kwargs: nullcontext(MagicMock()))

    job = {
        "id": "job-1",
        "run_id": "run-1",
        "status": "queued",
        "config": {
            "mode": "profile",
            "account": "bravotv",
            "target_source_ids": ["SHORT1"],
            "max_comments_per_post": 0,
            "fetch_replies": True,
        },
        "metadata": {
            "incomplete_fetch_reasons": {
                "SHORT1": "http_429",
            }
        },
        "attempt_count": 1,
        "max_attempts": 3,
    }

    with patch(
        "trr_backend.socials.instagram.comments_scrapling.job_runner.pg.fetch_one",
        side_effect=_active_comments_job_fetch_one("retrying"),
    ):
        jr.run_instagram_comments_scrapling_job(job, worker_id="test-worker")

    assert fetch_kwargs == [
        {
            "max_comments": 0,
            "fetch_replies": True,
            "expected_comment_count": None,
            "load_strategy": "instagram_comments_endpoint_cursor",
            # The runner now always forwards the job's attempt_count.
            "attempt_count": 1,
            "persisted_top_level_comments": [persisted_parent],
            "reply_only": True,
        }
    ]


def test_job_runner_reports_actual_comments_posts_checked(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr
    from trr_backend.socials.instagram.comments_scrapling.persistence import PersistedInstagramComments

    progress_activities: list[dict[str, Any] | None] = []

    def fake_persist(*, shortcode: str, **_kwargs: Any) -> PersistedInstagramComments:
        return PersistedInstagramComments(
            post_id=f"post-{shortcode}",
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
    monkeypatch.setattr(jr, "select_comments_proxy", lambda *, session_key=None: None)
    monkeypatch.setattr(jr, "resolve_comments_scrapling_session", lambda **_: fake_session)
    monkeypatch.setattr(jr, "InstagramCommentsScraplingFetcher", lambda **_: _FakeFetcher())
    monkeypatch.setattr(jr, "_load_expected_comment_counts", lambda **_kwargs: {"SHORT1": 10})
    monkeypatch.setattr(jr, "_load_comment_target_metadata", lambda **_kwargs: {})
    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *a, **k: None)
    monkeypatch.setattr(
        repo,
        "_emit_job_progress",
        lambda **kwargs: progress_activities.append(kwargs.get("activity")),
    )
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
            "target_source_ids": ["SHORT1", "SHORT2"],
            "max_comments_per_post": 10,
            "fetch_replies": False,
        },
        "attempt_count": 1,
        "max_attempts": 1,
    }

    with patch(
        "trr_backend.socials.instagram.comments_scrapling.job_runner.pg.fetch_one",
        side_effect=_active_comments_job_fetch_one("completed"),
    ):
        jr.run_instagram_comments_scrapling_job(job, worker_id="test-worker")

    assert progress_activities[0] == {
        "phase": "comments_scrapling_start",
        "posts_checked": 0,
        "matched_posts": 0,
        "saved_posts": 0,
        "total_posts": 2,
        "comments_shard_index": 1,
        "comments_shard_count": 1,
        "comments_shard_target_count": 2,
        "comments_load_strategy": "instagram_comments_endpoint_cursor",
        "comments_session_scope": "instagram_comments_endpoint_cursor_worker",
        "comments_per_post_concurrency": 1,
        "instagram_scrape_mode": None,
        "auth_state": "authenticated",
        "proxy_state": "configured_by_environment",
        "fallback_policy": "automatic_enabled",
    }
    assert progress_activities[-1] == {
        "phase": "comments_scrapling_running",
        "posts_checked": 2,
        "matched_posts": 2,
        "saved_posts": 2,
        "total_posts": 2,
        "comments_shard_index": 1,
        "comments_shard_count": 1,
        "comments_shard_target_count": 2,
        "comments_load_strategy": "instagram_comments_endpoint_cursor",
        "comments_session_scope": "instagram_comments_endpoint_cursor_worker",
        "comments_per_post_concurrency": 1,
        "instagram_scrape_mode": None,
        "auth_state": "authenticated",
        "proxy_state": "configured_by_environment",
        "fallback_policy": "automatic_enabled",
    }


def test_job_runner_retry_skips_already_complete_targets(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr
    from trr_backend.socials.instagram.comments_scrapling.persistence import PersistedInstagramComments

    fetch_calls: list[str] = []
    persist_calls: list[str] = []
    finish_calls: list[dict[str, Any]] = []
    progress_activities: list[dict[str, Any] | None] = []

    def fake_persist(*, shortcode: str, **_kwargs: Any) -> PersistedInstagramComments:
        persist_calls.append(shortcode)
        return PersistedInstagramComments(
            post_id=f"post-{shortcode}",
            stored_total_comments=1,
            comments_upserted=1,
            comments_marked_missing=0,
            comment_media_mirror_jobs_enqueued=0,
            comment_media_mirror_job_enqueue_errors=0,
        )

    class _FakeFetcher:
        _request_count = 1

        @property
        def runtime_metadata(self) -> dict[str, Any]:
            return {"transport": "test", "request_count": self._request_count}

        async def warmup(self) -> None:
            return None

        async def fetch_comments_for_shortcode(self, shortcode: str, **_kwargs: Any) -> InstagramCommentsFetchResult:
            fetch_calls.append(shortcode)
            return InstagramCommentsFetchResult(comments=[object()], fetch_failed=False, auth_failed=False)

        async def aclose(self) -> None:
            return None

    fake_session = MagicMock()
    fake_session.cookies = []
    fake_session.auth_session.cookies = {}
    fake_session.auth_session.metadata = {"source": "test"}
    fake_session.browser_account_id = "testaccount"

    monkeypatch.setattr(repo, "_instagram_filter_incomplete_comment_targets", lambda *_args, **_kwargs: ["SHORT2"])
    monkeypatch.setattr(jr, "persist_instagram_comments_for_post", fake_persist)
    monkeypatch.setattr(jr, "select_comments_proxy", lambda *, session_key=None: None)
    monkeypatch.setattr(jr, "resolve_comments_scrapling_session", lambda **_: fake_session)
    monkeypatch.setattr(jr, "InstagramCommentsScraplingFetcher", lambda **_: _FakeFetcher())
    monkeypatch.setattr(jr, "_load_expected_comment_counts", lambda **_kwargs: {"SHORT1": 10})
    monkeypatch.setattr(jr, "_load_comment_target_metadata", lambda **_kwargs: {})
    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *a, **k: None)
    monkeypatch.setattr(
        repo,
        "_emit_job_progress",
        lambda **kwargs: progress_activities.append(kwargs.get("activity")),
    )
    monkeypatch.setattr(repo, "_finish_job", lambda job_id, **kwargs: finish_calls.append({"job_id": job_id, **kwargs}))
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})
    monkeypatch.setattr(jr.pg, "db_connection", lambda **_kwargs: nullcontext(MagicMock()))

    job = {
        "id": "job-1",
        "run_id": "run-1",
        "config": {
            "mode": "profile",
            "account": "bravotv",
            "target_source_ids": ["SHORT1", "SHORT2"],
            "comments_retry_rebalance": True,
            "max_comments_per_post": 10,
            "fetch_replies": False,
        },
        "attempt_count": 2,
        "max_attempts": 3,
    }

    with patch(
        "trr_backend.socials.instagram.comments_scrapling.job_runner.pg.fetch_one",
        side_effect=_active_comments_job_fetch_one("completed"),
    ):
        jr.run_instagram_comments_scrapling_job(job, worker_id="test-worker")

    assert fetch_calls == ["SHORT2"]
    assert persist_calls == ["SHORT2"]
    assert progress_activities[-1]["posts_checked"] == 2
    assert finish_calls[-1]["status"] == "completed"
    assert finish_calls[-1]["metadata"]["skipped_complete_target_source_ids"] == ["SHORT1"]
    assert finish_calls[-1]["metadata"]["post_latency"]["samples"][0]["completion_reason"] == "already_complete"


def test_job_runner_incomplete_fill_skips_targets_completed_by_prior_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr
    from trr_backend.socials.instagram.comments_scrapling.persistence import PersistedInstagramComments

    fetch_calls: list[str] = []
    finish_calls: list[dict[str, Any]] = []

    def fake_persist(*, shortcode: str, **_kwargs: Any) -> PersistedInstagramComments:
        return PersistedInstagramComments(
            post_id=f"post-{shortcode}",
            stored_total_comments=1,
            comments_upserted=1,
            comments_marked_missing=0,
            comment_media_mirror_jobs_enqueued=0,
            comment_media_mirror_job_enqueue_errors=0,
        )

    class _FakeFetcher:
        @property
        def runtime_metadata(self) -> dict[str, Any]:
            return {"transport": "test", "request_count": 1}

        async def warmup(self) -> None:
            return None

        async def fetch_comments_for_shortcode(self, shortcode: str, **_kwargs: Any) -> InstagramCommentsFetchResult:
            fetch_calls.append(shortcode)
            return InstagramCommentsFetchResult(comments=[object()], fetch_failed=False, auth_failed=False)

        async def aclose(self) -> None:
            return None

    fake_session = MagicMock()
    fake_session.cookies = []
    fake_session.auth_session.cookies = {}
    fake_session.auth_session.metadata = {"source": "test"}
    fake_session.browser_account_id = "testaccount"

    monkeypatch.setattr(repo, "_instagram_filter_incomplete_comment_targets", lambda *_args, **_kwargs: ["SHORT2"])
    monkeypatch.setattr(jr, "persist_instagram_comments_for_post", fake_persist)
    monkeypatch.setattr(jr, "select_comments_proxy", lambda *, session_key=None: None)
    monkeypatch.setattr(jr, "resolve_comments_scrapling_session", lambda **_: fake_session)
    monkeypatch.setattr(jr, "InstagramCommentsScraplingFetcher", lambda **_: _FakeFetcher())
    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_emit_job_progress", lambda **_kwargs: None)
    monkeypatch.setattr(repo, "_finish_job", lambda job_id, **kwargs: finish_calls.append({"job_id": job_id, **kwargs}))
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})
    monkeypatch.setattr(jr.pg, "db_connection", lambda **_kwargs: nullcontext(MagicMock()))

    job = {
        "id": "job-1",
        "run_id": "run-1",
        "config": {
            "mode": "profile",
            "account": "bravotv",
            "target_source_ids": ["SHORT1", "SHORT2"],
            "target_filter": "incomplete",
            "incomplete_fill": True,
            "max_comments_per_post": 10,
            "fetch_replies": False,
        },
        "attempt_count": 1,
        "max_attempts": 3,
    }

    with patch(
        "trr_backend.socials.instagram.comments_scrapling.job_runner.pg.fetch_one",
        side_effect=_active_comments_job_fetch_one("completed"),
    ):
        jr.run_instagram_comments_scrapling_job(job, worker_id="test-worker")

    assert fetch_calls == ["SHORT2"]
    assert finish_calls[-1]["status"] == "completed"
    assert finish_calls[-1]["metadata"]["skipped_complete_target_source_ids"] == ["SHORT1"]
    assert finish_calls[-1]["metadata"]["post_latency"]["samples"][0]["completion_reason"] == "already_complete"


def test_job_runner_retrying_job_prefilters_complete_targets_without_retry_flag(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr
    from trr_backend.socials.instagram.comments_scrapling.persistence import PersistedInstagramComments

    fetch_calls: list[str] = []
    finish_calls: list[dict[str, Any]] = []

    def fake_persist(*, shortcode: str, **_kwargs: Any) -> PersistedInstagramComments:
        return PersistedInstagramComments(
            post_id=f"post-{shortcode}",
            stored_total_comments=1,
            comments_upserted=1,
            comments_marked_missing=0,
            comment_media_mirror_jobs_enqueued=0,
            comment_media_mirror_job_enqueue_errors=0,
        )

    class _FakeFetcher:
        @property
        def runtime_metadata(self) -> dict[str, Any]:
            return {"transport": "test", "request_count": 1}

        async def warmup(self) -> None:
            return None

        async def fetch_comments_for_shortcode(self, shortcode: str, **_kwargs: Any) -> InstagramCommentsFetchResult:
            fetch_calls.append(shortcode)
            return InstagramCommentsFetchResult(comments=[object()], fetch_failed=False, auth_failed=False)

        async def aclose(self) -> None:
            return None

    fake_session = MagicMock()
    fake_session.cookies = []
    fake_session.auth_session.cookies = {}
    fake_session.auth_session.metadata = {"source": "test"}
    fake_session.browser_account_id = "testaccount"

    monkeypatch.setattr(repo, "_instagram_filter_incomplete_comment_targets", lambda *_args, **_kwargs: ["SHORT2"])
    monkeypatch.setattr(jr, "persist_instagram_comments_for_post", fake_persist)
    monkeypatch.setattr(jr, "select_comments_proxy", lambda *, session_key=None: None)
    monkeypatch.setattr(jr, "resolve_comments_scrapling_session", lambda **_: fake_session)
    monkeypatch.setattr(jr, "InstagramCommentsScraplingFetcher", lambda **_: _FakeFetcher())
    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_emit_job_progress", lambda **_kwargs: None)
    monkeypatch.setattr(repo, "_finish_job", lambda job_id, **kwargs: finish_calls.append({"job_id": job_id, **kwargs}))
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})
    monkeypatch.setattr(jr.pg, "db_connection", lambda **_kwargs: nullcontext(MagicMock()))

    job = {
        "id": "job-1",
        "run_id": "run-1",
        "status": "retrying",
        "config": {
            "mode": "profile",
            "account": "bravotv",
            "target_source_ids": ["SHORT1", "SHORT2"],
            "max_comments_per_post": 10,
            "fetch_replies": False,
        },
        "attempt_count": 2,
        "max_attempts": 3,
    }

    with patch(
        "trr_backend.socials.instagram.comments_scrapling.job_runner.pg.fetch_one",
        side_effect=_active_comments_job_fetch_one("completed"),
    ):
        jr.run_instagram_comments_scrapling_job(job, worker_id="test-worker")

    assert fetch_calls == ["SHORT2"]
    assert finish_calls[-1]["status"] == "completed"
    assert finish_calls[-1]["metadata"]["skipped_complete_target_source_ids"] == ["SHORT1"]
    assert finish_calls[-1]["metadata"]["post_latency"]["samples"][0]["completion_reason"] == "already_complete"


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
    monkeypatch.setattr(jr, "select_comments_proxy", lambda *, session_key=None: None)
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
        side_effect=_active_comments_job_fetch_one("completed"),
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
    monkeypatch.setattr(jr, "select_comments_proxy", lambda *, session_key=None: None)
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
        side_effect=_active_comments_job_fetch_one("completed"),
    ):
        jr.run_instagram_comments_scrapling_job(job, worker_id="test-worker")

    assert captured_is_complete == [False]


def test_job_runner_retries_only_retryable_incomplete_posts(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr
    from trr_backend.socials.instagram.comments_scrapling.persistence import PersistedInstagramComments

    finish_calls: list[dict[str, Any]] = []
    config_update_calls: list[dict[str, Any]] = []
    persist_calls: list[dict[str, Any]] = []

    def fake_persist(*, shortcode: str, is_complete: bool, **_kwargs: Any) -> PersistedInstagramComments:
        persist_calls.append({"shortcode": shortcode, "is_complete": is_complete})
        return PersistedInstagramComments(
            post_id=f"post-{shortcode}",
            stored_total_comments=1,
            comments_upserted=1,
            comments_marked_missing=0,
            comment_media_mirror_jobs_enqueued=0,
            comment_media_mirror_job_enqueue_errors=0,
        )

    class _FakeFetcher:
        _request_count = 4

        @property
        def runtime_metadata(self) -> dict[str, Any]:
            return {
                "transport": "test",
                "request_count": self._request_count,
                "reply_checkpoint_metadata": {
                    "items": [
                        {
                            "platform": "instagram",
                            "target_shortcode": "SHORT1",
                            "source_id": "SHORT1",
                            "parent_comment_id": "parent-1",
                            "stop_reason": "http_429",
                            "attempt_count": 3,
                            "last_error_code": "http_429",
                            "saved_reply_count_observed": 2,
                            "expected_reply_count": 5,
                            "retryable": True,
                            "updated_at": "2026-04-30T12:00:00+00:00",
                        }
                    ],
                    "total_count": 1,
                    "max_items": 25,
                    "dropped_count": 0,
                    "truncated": False,
                },
            }

        async def warmup(self) -> None:
            return None

        async def fetch_comments_for_shortcode(self, shortcode: str, **_kwargs: Any) -> InstagramCommentsFetchResult:
            if shortcode == "SHORT1":
                return InstagramCommentsFetchResult(
                    comments=[object()],
                    fetch_failed=True,
                    auth_failed=False,
                    fetch_reason="http_429",
                    request_count=3,
                    retryable=True,
                )
            return InstagramCommentsFetchResult(
                comments=[object()],
                fetch_failed=False,
                auth_failed=False,
                request_count=4,
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
    monkeypatch.setattr(jr, "select_comments_proxy", lambda *, session_key=None: None)
    monkeypatch.setattr(jr, "resolve_comments_scrapling_session", lambda **_: fake_session)
    monkeypatch.setattr(jr, "InstagramCommentsScraplingFetcher", lambda **_: _FakeFetcher())
    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_emit_job_progress", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})
    monkeypatch.setattr(repo, "_retry_backoff_seconds", lambda *_args, **_kwargs: 0)

    def fake_update_job_config(_job_id: str, *, config_updates: dict[str, Any]) -> dict[str, Any]:
        config_update_calls.append(dict(config_updates))
        return {}

    monkeypatch.setattr(repo, "_update_job_config", fake_update_job_config)
    monkeypatch.setattr(
        repo,
        "_finish_job",
        lambda job_id, **kwargs: finish_calls.append({"job_id": job_id, **kwargs}),
    )
    monkeypatch.setattr(jr.pg, "db_connection", lambda **_kwargs: nullcontext(MagicMock()))

    job = {
        "id": "job-1",
        "run_id": "run-1",
        "config": {
            "mode": "profile",
            "account": "bravotv",
            "target_source_ids": ["SHORT1", "SHORT2"],
            "fetch_replies": True,
            "comments_shard_index": 1,
            "comments_shard_count": 2,
            "comments_shard_target_count": 2,
        },
        "attempt_count": 1,
        "max_attempts": 3,
    }

    with patch(
        "trr_backend.socials.instagram.comments_scrapling.job_runner.pg.fetch_one",
        side_effect=_active_comments_job_fetch_one("retrying"),
    ):
        jr.run_instagram_comments_scrapling_job(job, worker_id="test-worker")

    assert persist_calls == [
        {"shortcode": "SHORT1", "is_complete": False},
        {"shortcode": "SHORT2", "is_complete": True},
    ]
    assert config_update_calls == [
        {
            "target_source_ids": ["SHORT1"],
            "comments_shard_target_count": 1,
            "comments_retry_incomplete": True,
            "comments_retry_incomplete_source_job_id": "job-1",
        }
    ]
    assert finish_calls[-1]["status"] == "retrying"
    assert finish_calls[-1]["last_error_code"] == "instagram_comments_incomplete_retryable"
    assert finish_calls[-1]["metadata"]["retry_rebalance"] == {
        "remaining_target_source_ids": ["SHORT1"],
        "eligible": True,
    }
    assert finish_calls[-1]["metadata"]["reply_checkpoint_summary"] == {
        "total_count": 1,
        "retained_count": 1,
        "dropped_count": 0,
        "truncated": False,
        "stop_reasons": {"http_429": 1},
        "latest": {
            "platform": "instagram",
            "target_shortcode": "SHORT1",
            "source_id": "SHORT1",
            "parent_comment_id": "parent-1",
            "stop_reason": "http_429",
            "attempt_count": 3,
            "last_error_code": "http_429",
            "saved_reply_count_observed": 2,
            "expected_reply_count": 5,
            "retryable": True,
            "updated_at": "2026-04-30T12:00:00+00:00",
        },
    }


def test_job_runner_accepts_retryable_fetch_when_stored_comment_coverage_is_complete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr
    from trr_backend.socials.instagram.comments_scrapling.persistence import PersistedInstagramComments

    finish_calls: list[dict[str, Any]] = []
    config_update_calls: list[dict[str, Any]] = []
    persist_calls: list[dict[str, Any]] = []

    def fake_persist(*, shortcode: str, is_complete: bool, **_kwargs: Any) -> PersistedInstagramComments:
        persist_calls.append({"shortcode": shortcode, "is_complete": is_complete})
        return PersistedInstagramComments(
            post_id=f"post-{shortcode}",
            stored_total_comments=10,
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
            return InstagramCommentsFetchResult(
                comments=[
                    InstagramComment(
                        comment_id="c1",
                        text="visible",
                        username="alpha",
                        user_id="1",
                        created_at=1,
                        date_time="1970-01-01T00:00:01+00:00",
                        likes=0,
                        is_reply=False,
                        parent_comment_id=None,
                        reply_count=0,
                    )
                ],
                fetch_failed=True,
                auth_failed=False,
                fetch_reason="hidden_comments_unresolved",
                reported_comment_count=10,
                request_count=2,
                retryable=True,
            )

        async def aclose(self) -> None:
            return None

    fake_session = MagicMock()
    fake_session.cookies = []
    fake_session.auth_session.cookies = {}
    fake_session.auth_session.metadata = {"source": "test"}
    fake_session.browser_account_id = "testaccount"

    monkeypatch.setattr(jr, "persist_instagram_comments_for_post", fake_persist)
    monkeypatch.setattr(jr, "select_comments_proxy", lambda *, session_key=None: None)
    monkeypatch.setattr(jr, "resolve_comments_scrapling_session", lambda **_: fake_session)
    monkeypatch.setattr(jr, "InstagramCommentsScraplingFetcher", lambda **_: _FakeFetcher())
    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_emit_job_progress", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})
    monkeypatch.setattr(repo, "_update_job_config", lambda *a, **k: config_update_calls.append(dict(k)))
    monkeypatch.setattr(
        repo,
        "_finish_job",
        lambda job_id, **kwargs: finish_calls.append({"job_id": job_id, **kwargs}),
    )
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
        "max_attempts": 3,
    }

    with patch(
        "trr_backend.socials.instagram.comments_scrapling.job_runner.pg.fetch_one",
        side_effect=_active_comments_job_fetch_one("completed"),
    ):
        jr.run_instagram_comments_scrapling_job(job, worker_id="test-worker")

    assert persist_calls == [{"shortcode": "SHORT1", "is_complete": False}]
    assert config_update_calls == []
    assert finish_calls[-1]["status"] == "completed"
    metadata = finish_calls[-1]["metadata"]
    assert metadata["incomplete_target_source_ids"] == []
    assert metadata["post_latency"]["samples"][0]["completion_reason"] == "stored_comment_coverage_complete"
    assert metadata["post_latency"]["samples"][0]["stored_total_comments"] == 10


def test_job_runner_marks_persisted_reply_topology_gap_retryable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr
    from trr_backend.socials.instagram.comments_scrapling.persistence import PersistedInstagramComments

    finish_calls: list[dict[str, Any]] = []
    persist_calls: list[dict[str, Any]] = []
    reconcile_calls: list[dict[str, Any]] = []

    def fake_persist(*, shortcode: str, is_complete: bool, **_kwargs: Any) -> PersistedInstagramComments:
        persist_calls.append({"shortcode": shortcode, "is_complete": is_complete})
        return PersistedInstagramComments(
            post_id=f"post-{shortcode}",
            stored_total_comments=9,
            comments_upserted=1,
            comments_marked_missing=0,
            comment_media_mirror_jobs_enqueued=0,
            comment_media_mirror_job_enqueue_errors=0,
            stored_parent_comments=1,
            stored_child_replies=8,
            expected_child_replies=9,
            stored_reply_gap_total=1,
            stored_reply_gap_parent_count=1,
            stored_reply_gap_samples=[
                {
                    "comment_id": "c1",
                    "expected_reply_count": 9,
                    "saved_reply_count": 8,
                    "missing_reply_count": 1,
                }
            ],
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
                comments=[
                    InstagramComment(
                        comment_id="c1",
                        text="visible",
                        username="alpha",
                        user_id="1",
                        created_at=1,
                        date_time="1970-01-01T00:00:01+00:00",
                        likes=0,
                        is_reply=False,
                        parent_comment_id=None,
                        reply_count=0,
                    )
                ],
                fetch_failed=True,
                auth_failed=False,
                fetch_reason="reply_tail_incomplete",
                reported_comment_count=10,
                request_count=2,
                retryable=True,
            )

        async def aclose(self) -> None:
            return None

    fake_session = MagicMock()
    fake_session.cookies = []
    fake_session.auth_session.cookies = {}
    fake_session.auth_session.metadata = {"source": "test"}
    fake_session.browser_account_id = "testaccount"

    monkeypatch.setattr(jr, "persist_instagram_comments_for_post", fake_persist)
    monkeypatch.setattr(jr, "select_comments_proxy", lambda *, session_key=None: None)
    monkeypatch.setattr(jr, "resolve_comments_scrapling_session", lambda **_: fake_session)
    monkeypatch.setattr(jr, "InstagramCommentsScraplingFetcher", lambda **_: _FakeFetcher())
    monkeypatch.setattr(jr, "_load_expected_comment_counts", lambda **_kwargs: {"SHORT1": 10})
    monkeypatch.setattr(jr, "_load_comment_target_metadata", lambda **_kwargs: {})
    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_emit_job_progress", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})
    monkeypatch.setattr(
        repo,
        "_reconcile_post_comment_count",
        lambda **kwargs: reconcile_calls.append(kwargs) or 9,
    )
    monkeypatch.setattr(
        repo,
        "_finish_job",
        lambda job_id, **kwargs: finish_calls.append({"job_id": job_id, **kwargs}),
    )
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
        "max_attempts": 3,
    }

    with patch(
        "trr_backend.socials.instagram.comments_scrapling.job_runner.pg.fetch_one",
        side_effect=_active_comments_job_fetch_one("completed"),
    ):
        jr.run_instagram_comments_scrapling_job(job, worker_id="test-worker")

    assert persist_calls == [{"shortcode": "SHORT1", "is_complete": False}]
    assert reconcile_calls == []
    assert finish_calls[-1]["status"] == "retrying"
    metadata = finish_calls[-1]["metadata"]
    assert metadata["incomplete_target_source_ids"] == ["SHORT1"]
    assert metadata["incomplete_fetch_reasons"] == {"SHORT1": "reply_tail_incomplete"}
    sample = metadata["post_latency"]["samples"][0]
    assert sample["completion_reason"] == "persisted_reply_topology_gap"
    assert sample["operator_status"] == "incomplete_retryable"
    assert sample["stored_total_comments"] == 9
    assert sample["stored_reply_gap_total"] == 1
    assert sample["stored_reply_gap_parent_count"] == 1
    assert sample["stored_reply_gap_samples"][0]["comment_id"] == "c1"
    failure = metadata["post_fetch_failures"]["target_metadata"]["SHORT1"]
    assert failure["fetch_reason"] == "reply_tail_incomplete"
    assert failure["persisted_reply_topology"]["stored_reply_gap_total"] == 1


def test_job_runner_reconciles_reply_only_auth_blocked_gap_despite_reply_topology_gap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr
    from trr_backend.socials.instagram.comments_scrapling.persistence import PersistedInstagramComments

    finish_calls: list[dict[str, Any]] = []
    reconcile_calls: list[dict[str, Any]] = []
    fetch_kwargs_seen: list[dict[str, Any]] = []

    def fake_persist(*, is_complete: bool, **_kwargs: Any) -> PersistedInstagramComments:
        assert is_complete is False
        return PersistedInstagramComments(
            post_id="post-SHORT1",
            stored_total_comments=1207,
            comments_upserted=53,
            comments_marked_missing=0,
            comment_media_mirror_jobs_enqueued=0,
            comment_media_mirror_job_enqueue_errors=0,
            comments_refreshed=53,
            stored_parent_comments=943,
            stored_child_replies=264,
            expected_child_replies=309,
            stored_reply_gap_total=45,
            stored_reply_gap_parent_count=32,
            stored_reply_gap_samples=[{"comment_id": "p1", "missing_reply_count": 3}],
        )

    class _FakeFetcher:
        _request_count = 4

        @property
        def runtime_metadata(self) -> dict[str, Any]:
            return {"transport": "test", "request_count": self._request_count}

        async def warmup(self) -> None:
            return None

        async def fetch_comments_for_shortcode(self, *_args: Any, **kwargs: Any) -> InstagramCommentsFetchResult:
            fetch_kwargs_seen.append(dict(kwargs))
            return InstagramCommentsFetchResult(
                comments=[_comment("p1", replies=[_comment("p1-r1", is_reply=True, parent_comment_id="p1")])],
                fetch_failed=True,
                auth_failed=True,
                fetch_reason="html_challenge_or_auth_required",
                reported_comment_count=1419,
                request_count=4,
                retryable=True,
            )

        async def aclose(self) -> None:
            return None

    fake_session = MagicMock()
    fake_session.cookies = []
    fake_session.auth_session.cookies = {}
    fake_session.auth_session.metadata = {"source": "test"}
    fake_session.browser_account_id = "testaccount"

    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_RECONCILABLE_GAP_RATIO", "0.25")
    monkeypatch.setattr(jr, "persist_instagram_comments_for_post", fake_persist)
    monkeypatch.setattr(jr, "select_comments_proxy", lambda *, session_key=None: None)
    monkeypatch.setattr(jr, "resolve_comments_scrapling_session", lambda **_: fake_session)
    monkeypatch.setattr(jr, "InstagramCommentsScraplingFetcher", lambda **_: _FakeFetcher())
    monkeypatch.setattr(jr, "_load_persisted_top_level_comments_for_reply_retry", lambda **_: [_comment("p1")])
    monkeypatch.setattr(jr, "_load_expected_comment_counts", lambda **_kwargs: {"SHORT1": 1419})
    monkeypatch.setattr(jr, "_load_comment_target_metadata", lambda **_kwargs: {})
    monkeypatch.setattr(jr, "_classify_unavailable_instagram_comment_gap", lambda **_kwargs: 212)
    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_emit_job_progress", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})
    monkeypatch.setattr(
        repo,
        "_reconcile_post_comment_count",
        lambda **kwargs: reconcile_calls.append(kwargs) or 1207,
    )
    monkeypatch.setattr(
        repo,
        "_finish_job",
        lambda job_id, **kwargs: finish_calls.append({"job_id": job_id, **kwargs}),
    )
    monkeypatch.setattr(jr.pg, "db_connection", lambda **_kwargs: nullcontext(MagicMock()))

    job = {
        "id": "job-1",
        "run_id": "run-1",
        "metadata": {"incomplete_fetch_reasons": {"SHORT1": "html_challenge_or_auth_required"}},
        "config": {
            "mode": "profile",
            "account": "thetraitorsus",
            "target_source_ids": ["SHORT1"],
            "target_filter": "incomplete",
            "fetch_replies": True,
        },
        "attempt_count": 1,
        "max_attempts": 12,
    }

    with patch(
        "trr_backend.socials.instagram.comments_scrapling.job_runner.pg.fetch_one",
        side_effect=_active_comments_job_fetch_one("completed"),
    ):
        jr.run_instagram_comments_scrapling_job(job, worker_id="test-worker")

    assert fetch_kwargs_seen[0]["reply_only"] is True
    assert reconcile_calls[0]["post_db_id"] == "post-SHORT1"
    assert finish_calls[-1]["status"] == "completed"
    metadata = finish_calls[-1]["metadata"]
    assert metadata["incomplete_target_source_ids"] == []
    sample = metadata["post_latency"]["samples"][0]
    assert sample["completion_reason"] == "stored_comment_coverage_auth_blocked_gap_reconciled"
    assert sample["operator_status"] == "complete"
    assert sample["stored_reply_gap_total"] == 45


def test_job_runner_keeps_high_coverage_terminal_pagination_gap_incomplete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr
    from trr_backend.socials.instagram.comments_scrapling.persistence import PersistedInstagramComments

    finish_calls: list[dict[str, Any]] = []
    persist_calls: list[dict[str, Any]] = []
    reconcile_calls: list[dict[str, Any]] = []

    def fake_persist(*, shortcode: str, is_complete: bool, **_kwargs: Any) -> PersistedInstagramComments:
        persist_calls.append({"shortcode": shortcode, "is_complete": is_complete})
        return PersistedInstagramComments(
            post_id=f"post-{shortcode}",
            stored_total_comments=910,
            comments_upserted=10,
            comments_marked_missing=0,
            comment_media_mirror_jobs_enqueued=0,
            comment_media_mirror_job_enqueue_errors=0,
        )

    class _FakeFetcher:
        _request_count = 8

        @property
        def runtime_metadata(self) -> dict[str, Any]:
            return {"transport": "test", "request_count": self._request_count}

        async def warmup(self) -> None:
            return None

        async def fetch_comments_for_shortcode(self, *_args: Any, **_kwargs: Any) -> InstagramCommentsFetchResult:
            return InstagramCommentsFetchResult(
                comments=[
                    InstagramComment(
                        comment_id="c1",
                        text="visible",
                        username="alpha",
                        user_id="1",
                        created_at=1,
                        date_time="1970-01-01T00:00:01+00:00",
                        likes=0,
                        is_reply=False,
                        parent_comment_id=None,
                        reply_count=0,
                    )
                ],
                fetch_failed=True,
                auth_failed=False,
                fetch_reason="pagination_repeated_cursor",
                reported_comment_count=1000,
                request_count=8,
                retryable=True,
            )

        async def aclose(self) -> None:
            return None

    fake_session = MagicMock()
    fake_session.cookies = []
    fake_session.auth_session.cookies = {}
    fake_session.auth_session.metadata = {"source": "test"}
    fake_session.browser_account_id = "testaccount"

    monkeypatch.setattr(jr, "persist_instagram_comments_for_post", fake_persist)
    monkeypatch.setattr(jr, "select_comments_proxy", lambda *, session_key=None: None)
    monkeypatch.setattr(jr, "resolve_comments_scrapling_session", lambda **_: fake_session)
    monkeypatch.setattr(jr, "InstagramCommentsScraplingFetcher", lambda **_: _FakeFetcher())
    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_emit_job_progress", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})
    monkeypatch.setattr(
        repo,
        "_reconcile_post_comment_count",
        lambda **kwargs: reconcile_calls.append(kwargs) or 910,
    )
    monkeypatch.setattr(
        repo,
        "_finish_job",
        lambda job_id, **kwargs: finish_calls.append({"job_id": job_id, **kwargs}),
    )
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
        "max_attempts": 3,
    }

    with patch(
        "trr_backend.socials.instagram.comments_scrapling.job_runner.pg.fetch_one",
        side_effect=_active_comments_job_fetch_one("completed"),
    ):
        jr.run_instagram_comments_scrapling_job(job, worker_id="test-worker")

    # SA-2 (comment-completeness): the terminal coverage-gap tolerance is now 0, so
    # a 1000-reported / 910-stored pagination_repeated_cursor stop (gap=90) is no
    # longer blessed terminal-complete. The post stays incomplete/retryable and is
    # NOT reconciled.
    assert persist_calls == [{"shortcode": "SHORT1", "is_complete": False}]
    assert reconcile_calls == []
    assert finish_calls[-1]["status"] == "retrying"
    metadata = finish_calls[-1]["metadata"]
    assert metadata["incomplete_target_source_ids"] == ["SHORT1"]
    assert metadata["incomplete_fetch_reasons"] == {"SHORT1": "pagination_repeated_cursor"}
    sample = metadata["post_latency"]["samples"][0]
    assert sample["completion_reason"] == "incomplete_fetch"
    assert sample["operator_status"] == "incomplete_retryable"
    assert sample["stored_total_comments"] == 910


def test_job_runner_keeps_one_comment_relay_recovery_gap_incomplete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr
    from trr_backend.socials.instagram.comments_scrapling.persistence import PersistedInstagramComments

    finish_calls: list[dict[str, Any]] = []
    reconcile_calls: list[dict[str, Any]] = []

    def fake_persist(*, is_complete: bool, **_kwargs: Any) -> PersistedInstagramComments:
        assert is_complete is False
        return PersistedInstagramComments(
            post_id="post-SHORT1",
            stored_total_comments=32,
            comments_upserted=32,
            comments_marked_missing=0,
            comment_media_mirror_jobs_enqueued=0,
            comment_media_mirror_job_enqueue_errors=0,
        )

    class _FakeFetcher:
        _request_count = 6

        @property
        def runtime_metadata(self) -> dict[str, Any]:
            return {
                "transport": "test",
                "request_count": self._request_count,
                "lane_diagnostics": {
                    "relay": {
                        "last_reason": "coauthor_auth_relay_fallback_recovered",
                        "last_count": 32,
                    }
                },
            }

        async def warmup(self) -> None:
            return None

        async def fetch_comments_for_shortcode(self, *_args: Any, **_kwargs: Any) -> InstagramCommentsFetchResult:
            return InstagramCommentsFetchResult(
                comments=[
                    InstagramComment(
                        comment_id=f"c{i}",
                        text="visible",
                        username=f"alpha{i}",
                        user_id=str(i),
                        created_at=i,
                        date_time="1970-01-01T00:00:01+00:00",
                        likes=0,
                        is_reply=False,
                        parent_comment_id=None,
                        reply_count=0,
                    )
                    for i in range(32)
                ],
                fetch_failed=True,
                auth_failed=False,
                fetch_reason="coauthor_auth_relay_fallback_recovered",
                reported_comment_count=33,
                request_count=6,
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
    monkeypatch.setattr(jr, "select_comments_proxy", lambda *, session_key=None: None)
    monkeypatch.setattr(jr, "resolve_comments_scrapling_session", lambda **_: fake_session)
    monkeypatch.setattr(jr, "InstagramCommentsScraplingFetcher", lambda **_: _FakeFetcher())
    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_emit_job_progress", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})
    monkeypatch.setattr(
        repo,
        "_reconcile_post_comment_count",
        lambda **kwargs: reconcile_calls.append(kwargs) or 32,
    )
    monkeypatch.setattr(
        repo,
        "_finish_job",
        lambda job_id, **kwargs: finish_calls.append({"job_id": job_id, **kwargs}),
    )
    monkeypatch.setattr(jr.pg, "db_connection", lambda **_kwargs: nullcontext(MagicMock()))

    job = {
        "id": "job-1",
        "run_id": "run-1",
        "config": {
            "mode": "profile",
            "account": "thetraitorsus",
            "target_source_ids": ["SHORT1"],
            "fetch_replies": True,
        },
        "attempt_count": 1,
        "max_attempts": 3,
    }

    with patch(
        "trr_backend.socials.instagram.comments_scrapling.job_runner.pg.fetch_one",
        side_effect=_active_comments_job_fetch_one("completed"),
    ):
        jr.run_instagram_comments_scrapling_job(job, worker_id="test-worker")

    # SA-2 (comment-completeness): the reconcilable reported-count gap tolerance is
    # now 0, so a 33-reported / 32-stored relay-recovery stop (gap=1) is no longer
    # reconciled-complete. The post stays incomplete/retryable.
    assert reconcile_calls == []
    assert finish_calls[-1]["status"] == "retrying"
    metadata = finish_calls[-1]["metadata"]
    assert metadata["incomplete_target_source_ids"] == ["SHORT1"]
    assert metadata["incomplete_fetch_reasons"] == {"SHORT1": "coauthor_auth_relay_fallback_recovered"}
    sample = metadata["post_latency"]["samples"][0]
    assert sample["completion_reason"] == "incomplete_fetch"
    assert sample["operator_status"] == "incomplete_retryable"
    assert sample["stored_total_comments"] == 32


def test_job_runner_keeps_high_coverage_transient_gap_incomplete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr
    from trr_backend.socials.instagram.comments_scrapling.persistence import PersistedInstagramComments

    finish_calls: list[dict[str, Any]] = []
    reconcile_calls: list[dict[str, Any]] = []

    def fake_persist(*, is_complete: bool, **_kwargs: Any) -> PersistedInstagramComments:
        assert is_complete is False
        return PersistedInstagramComments(
            post_id="post-SHORT1",
            stored_total_comments=847,
            comments_upserted=12,
            comments_marked_missing=0,
            comment_media_mirror_jobs_enqueued=0,
            comment_media_mirror_job_enqueue_errors=0,
        )

    class _FakeFetcher:
        _request_count = 14

        @property
        def runtime_metadata(self) -> dict[str, Any]:
            return {
                "transport": "test",
                "request_count": self._request_count,
                "retry_reason_counts": {"http_429": 14},
            }

        async def warmup(self) -> None:
            return None

        async def fetch_comments_for_shortcode(self, *_args: Any, **_kwargs: Any) -> InstagramCommentsFetchResult:
            return InstagramCommentsFetchResult(
                comments=[
                    InstagramComment(
                        comment_id="c1",
                        text="visible",
                        username="alpha",
                        user_id="1",
                        created_at=1,
                        date_time="1970-01-01T00:00:01+00:00",
                        likes=0,
                        is_reply=False,
                        parent_comment_id=None,
                        reply_count=0,
                    )
                ],
                fetch_failed=True,
                auth_failed=False,
                fetch_reason="http_429",
                reported_comment_count=876,
                request_count=14,
                retryable=True,
            )

        async def aclose(self) -> None:
            return None

    fake_session = MagicMock()
    fake_session.cookies = []
    fake_session.auth_session.cookies = {}
    fake_session.auth_session.metadata = {"source": "test"}
    fake_session.browser_account_id = "testaccount"

    monkeypatch.setattr(jr, "persist_instagram_comments_for_post", fake_persist)
    monkeypatch.setattr(jr, "select_comments_proxy", lambda *, session_key=None: None)
    monkeypatch.setattr(jr, "resolve_comments_scrapling_session", lambda **_: fake_session)
    monkeypatch.setattr(jr, "InstagramCommentsScraplingFetcher", lambda **_: _FakeFetcher())
    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_emit_job_progress", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})
    monkeypatch.setattr(
        repo,
        "_reconcile_post_comment_count",
        lambda **kwargs: reconcile_calls.append(kwargs) or 847,
    )
    monkeypatch.setattr(
        repo,
        "_finish_job",
        lambda job_id, **kwargs: finish_calls.append({"job_id": job_id, **kwargs}),
    )
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
        "max_attempts": 3,
    }

    with patch(
        "trr_backend.socials.instagram.comments_scrapling.job_runner.pg.fetch_one",
        side_effect=_active_comments_job_fetch_one("completed"),
    ):
        jr.run_instagram_comments_scrapling_job(job, worker_id="test-worker")

    # SA-2 (comment-completeness): http_429 was removed from the terminal
    # coverage-gap reason set (only pagination_repeated_cursor may bless a gap as
    # terminal now). A rate-limited stop that is 876-reported / 847-stored is never
    # treated as "complete-enough" — the post stays incomplete/retryable so the
    # rate-limit is re-driven with backoff. This is the biggest single coverage
    # leak the plan closes.
    assert reconcile_calls == []
    assert finish_calls[-1]["status"] == "retrying"
    metadata = finish_calls[-1]["metadata"]
    assert metadata["incomplete_target_source_ids"] == ["SHORT1"]
    assert metadata["incomplete_fetch_reasons"] == {"SHORT1": "http_429"}
    sample = metadata["post_latency"]["samples"][0]
    assert sample["completion_reason"] == "incomplete_fetch"
    assert sample["operator_status"] == "incomplete_retryable"
    assert sample["stored_total_comments"] == 847


def test_job_runner_retries_only_remaining_posts_after_hard_retryable_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr
    from trr_backend.socials.instagram.comments_scrapling.persistence import PersistedInstagramComments

    finish_calls: list[dict[str, Any]] = []
    config_update_calls: list[dict[str, Any]] = []
    persist_calls: list[str] = []

    def fake_persist(*, shortcode: str, **_kwargs: Any) -> PersistedInstagramComments:
        persist_calls.append(shortcode)
        return PersistedInstagramComments(
            post_id=f"post-{shortcode}",
            stored_total_comments=1,
            comments_upserted=1,
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

        async def fetch_comments_for_shortcode(self, shortcode: str, **_kwargs: Any) -> InstagramCommentsFetchResult:
            if shortcode == "SHORT2":
                return InstagramCommentsFetchResult(
                    comments=[],
                    fetch_failed=True,
                    auth_failed=False,
                    fetch_reason="http_429",
                    request_count=3,
                    retryable=True,
                )
            return InstagramCommentsFetchResult(
                comments=[object()],
                fetch_failed=False,
                auth_failed=False,
                request_count=2,
                retryable=False,
            )

        async def aclose(self) -> None:
            return None

    fake_session = MagicMock()
    fake_session.cookies = []
    fake_session.auth_session.cookies = {}
    fake_session.auth_session.metadata = {"source": "test"}
    fake_session.browser_account_id = "testaccount"

    def fake_update_job_config(_job_id: str, *, config_updates: dict[str, Any]) -> dict[str, Any]:
        config_update_calls.append(dict(config_updates))
        return {}

    monkeypatch.setattr(jr, "persist_instagram_comments_for_post", fake_persist)
    monkeypatch.setattr(jr, "select_comments_proxy", lambda *, session_key=None: None)
    monkeypatch.setattr(jr, "resolve_comments_scrapling_session", lambda **_: fake_session)
    monkeypatch.setattr(jr, "InstagramCommentsScraplingFetcher", lambda **_: _FakeFetcher())
    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_emit_job_progress", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})
    monkeypatch.setattr(repo, "_retry_backoff_seconds", lambda *_args, **_kwargs: 0)
    monkeypatch.setattr(repo, "_update_job_config", fake_update_job_config)
    monkeypatch.setattr(
        repo,
        "_finish_job",
        lambda job_id, **kwargs: finish_calls.append({"job_id": job_id, **kwargs}),
    )
    monkeypatch.setattr(jr.pg, "db_connection", lambda **_kwargs: nullcontext(MagicMock()))

    job = {
        "id": "job-1",
        "run_id": "run-1",
        "config": {
            "mode": "profile",
            "account": "bravotv",
            "target_source_ids": ["SHORT1", "SHORT2", "SHORT3"],
            "fetch_replies": True,
            "comments_shard_index": 1,
            "comments_shard_count": 2,
            "comments_shard_target_count": 3,
        },
        "attempt_count": 1,
        "max_attempts": 3,
    }

    with patch(
        "trr_backend.socials.instagram.comments_scrapling.job_runner.pg.fetch_one",
        side_effect=_active_comments_job_fetch_one("retrying"),
    ):
        jr.run_instagram_comments_scrapling_job(job, worker_id="test-worker")

    assert persist_calls == ["SHORT1", "SHORT3"]
    assert config_update_calls == [
        {
            "target_source_ids": ["SHORT2"],
            "comments_shard_target_count": 1,
            "comments_retry_incomplete": True,
            "comments_retry_incomplete_source_job_id": "job-1",
        }
    ]
    assert finish_calls[-1]["status"] == "retrying"
    assert finish_calls[-1]["last_error_code"] == "instagram_comments_incomplete_retryable"
    assert finish_calls[-1]["metadata"]["retry_rebalance"] == {
        "remaining_target_source_ids": ["SHORT2"],
        "eligible": True,
    }


def test_job_runner_completes_one_pass_incomplete_fill_with_unresolved_targets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr
    from trr_backend.socials.instagram.comments_scrapling.persistence import PersistedInstagramComments

    finish_calls: list[dict[str, Any]] = []
    config_update_calls: list[dict[str, Any]] = []

    def fake_persist(*, shortcode: str, is_complete: bool, **_kwargs: Any) -> PersistedInstagramComments:
        assert shortcode == "SHORT1"
        assert is_complete is False
        return PersistedInstagramComments(
            post_id="post-short1",
            stored_total_comments=1,
            comments_upserted=1,
            comments_marked_missing=0,
            comment_media_mirror_jobs_enqueued=0,
            comment_media_mirror_job_enqueue_errors=0,
        )

    class _FakeFetcher:
        @property
        def runtime_metadata(self) -> dict[str, Any]:
            return {"transport": "test", "request_count": 1}

        async def warmup(self) -> None:
            return None

        async def fetch_comments_for_shortcode(self, *_args: Any, **_kwargs: Any) -> InstagramCommentsFetchResult:
            return InstagramCommentsFetchResult(
                comments=[object()],
                fetch_failed=True,
                auth_failed=False,
                fetch_reason="reply_tail_incomplete",
                retryable=True,
            )

        async def aclose(self) -> None:
            return None

    fake_session = _fake_comments_session()
    monkeypatch.setattr(jr, "persist_instagram_comments_for_post", fake_persist)
    monkeypatch.setattr(jr, "select_comments_proxy", lambda *, session_key=None: None)
    monkeypatch.setattr(jr, "resolve_comments_scrapling_session", lambda **_: fake_session)
    monkeypatch.setattr(jr, "InstagramCommentsScraplingFetcher", lambda **_: _FakeFetcher())
    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_emit_job_progress", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_finalize_run_status", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_new_job_progress_state", lambda: {})
    monkeypatch.setattr(
        repo,
        "_update_job_config",
        lambda _job_id, config_updates: config_update_calls.append(config_updates),
    )
    monkeypatch.setattr(repo, "_finish_job", lambda job_id, **kwargs: finish_calls.append({"job_id": job_id, **kwargs}))
    monkeypatch.setattr(jr.pg, "db_connection", lambda **_kwargs: nullcontext(MagicMock()))

    job = {
        "id": "job-1",
        "run_id": "run-1",
        "config": {
            "mode": "profile",
            "account": "bravotv",
            "target_source_ids": ["SHORT1"],
            "target_filter": "incomplete",
            "incomplete_fill": True,
            "fetch_replies": True,
        },
        "attempt_count": 1,
        "max_attempts": 1,
    }

    with patch(
        "trr_backend.socials.instagram.comments_scrapling.job_runner.pg.fetch_one",
        side_effect=_active_comments_job_fetch_one("completed"),
    ):
        payload = jr.run_instagram_comments_scrapling_job(job, worker_id="test-worker")

    assert payload["status"] == "completed"
    assert config_update_calls == []
    finish_kwargs = finish_calls[-1]
    assert finish_kwargs["status"] == "completed"
    metadata = finish_kwargs["metadata"]
    assert metadata["incomplete_target_source_ids"] == ["SHORT1"]
    assert metadata["incomplete_fetch_reasons"] == {"SHORT1": "reply_tail_incomplete"}
    assert metadata["incomplete_retry_stalled"] == {
        "stalled": False,
        "retry_exhausted": True,
        "attempt_count": 1,
        "max_attempts": 1,
        "target_source_ids": ["SHORT1"],
        "fetch_reasons": {"SHORT1": "reply_tail_incomplete"},
        "current_comments_fetched": 1,
        "completion_status": "attempted_incomplete_fill",
    }


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
    monkeypatch.setattr(jr, "select_comments_proxy", lambda *, session_key=None: None)
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
            "comments_shard_index": 2,
            "comments_shard_count": 4,
            "comments_shard_target_count": 1,
            "fetch_replies": False,
        },
        "attempt_count": 1,
        "max_attempts": 1,
    }

    with patch(
        "trr_backend.socials.instagram.comments_scrapling.job_runner.pg.fetch_one",
        side_effect=_active_comments_job_fetch_one("completed"),
    ):
        jr.run_instagram_comments_scrapling_job(job, worker_id="test-worker")

    metadata = finish_calls[-1]["metadata"]
    assert metadata["fetch_counters"]["request_count"] == 4
    assert metadata["fetcher_runtime"]["request_count"] == 4
    assert metadata["comments_shard_index"] == 2
    assert metadata["comments_shard_count"] == 4
    assert metadata["comments_shard_target_count"] == 1
    assert metadata["post_latency"]["sample_count"] == 1
    assert metadata["post_latency"]["samples"][0]["shortcode"] == "SHORT1"
    assert metadata["post_latency"]["slowest"][0]["shortcode"] == "SHORT1"
    assert metadata["comment_completeness"] == {
        "complete_posts": 1,
        "incomplete_posts": 0,
        "completion_reasons": {"pagination_exhausted": 1},
    }
    assert metadata["incomplete_target_source_ids"] == []
    assert metadata["incomplete_fetch_reasons"] == {}
    assert metadata["auth_failed_target_source_ids"] == []
    assert metadata["auth_failed_fetch_reasons"] == {}
    assert metadata["timing"]["job_runner_started_at"] is not None
    assert metadata["timing"]["warmup_completed_at"] is not None
    assert metadata["timing"]["first_post_persisted_at"] is not None


def test_job_runner_reports_parent_child_fetch_and_write_metrics(monkeypatch: pytest.MonkeyPatch) -> None:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr
    from trr_backend.socials.instagram.comments_scrapling.persistence import PersistedInstagramComments

    finish_calls: list[dict[str, Any]] = []
    parent = _comment("c1", replies=[_comment("r1", is_reply=True, parent_comment_id="c1")], reply_count=1)

    def fake_persist(**_kwargs: Any) -> PersistedInstagramComments:
        return PersistedInstagramComments(
            post_id="post-id",
            stored_total_comments=2,
            comments_upserted=3,
            comments_marked_missing=0,
            comment_media_mirror_jobs_enqueued=0,
            comment_media_mirror_job_enqueue_errors=0,
            comments_inserted=2,
            comments_refreshed=1,
            comments_changed=3,
        )

    class _FakeFetcher:
        _request_count = 5

        @property
        def runtime_metadata(self) -> dict[str, Any]:
            return {
                "transport": "test",
                "request_count": self._request_count,
                "lane_diagnostics": {
                    "parent": {"attempted": True},
                    "child": {"attempted": True},
                    "relay": {"attempted": True},
                    "rendered": {"attempted": True},
                    "retry": {"attempted": True},
                },
            }

        async def warmup(self) -> None:
            return None

        async def fetch_comments_for_shortcode(self, *_args: Any, **_kwargs: Any) -> InstagramCommentsFetchResult:
            return InstagramCommentsFetchResult(
                comments=[parent],
                fetch_failed=False,
                auth_failed=False,
                reported_comment_count=2,
            )

        async def aclose(self) -> None:
            return None

    fake_session = MagicMock()
    fake_session.cookies = []
    fake_session.auth_session.cookies = {}
    fake_session.auth_session.metadata = {"source": "test"}
    fake_session.browser_account_id = "testaccount"

    monkeypatch.setattr(jr, "persist_instagram_comments_for_post", fake_persist)
    monkeypatch.setattr(jr, "select_comments_proxy", lambda *, session_key=None: None)
    monkeypatch.setattr(jr, "resolve_comments_scrapling_session", lambda **_: fake_session)
    monkeypatch.setattr(jr, "InstagramCommentsScraplingFetcher", lambda **_: _FakeFetcher())
    monkeypatch.setattr(repo, "_touch_job_heartbeat", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_emit_job_progress", lambda *a, **k: None)
    monkeypatch.setattr(repo, "_finish_job", lambda job_id, **kwargs: finish_calls.append({"job_id": job_id, **kwargs}))
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
        side_effect=_active_comments_job_fetch_one("completed"),
    ):
        jr.run_instagram_comments_scrapling_job(job, worker_id="test-worker")

    metadata = finish_calls[-1]["metadata"]
    assert metadata["fetch_counters"]["comments_fetched"] == 2
    assert metadata["fetch_counters"]["parent_comments_fetched"] == 1
    assert metadata["fetch_counters"]["child_replies_fetched"] == 1
    assert metadata["persist_counters"]["db_rows_written"] == 3
    assert metadata["persist_counters"]["new_instagram_comments_saved"] == 2
    assert metadata["persist_counters"]["existing_comment_rows_seen"] == 1
    assert metadata["persist_counters"]["existing_comment_rows_updated"] == 1
    assert metadata["post_latency"]["samples"][0]["parent_comments_fetched"] == 1
    assert metadata["post_latency"]["samples"][0]["child_replies_fetched"] == 1


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
                comments=[object() for _ in range(22)],
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
    monkeypatch.setattr(jr, "select_comments_proxy", lambda *, session_key=None: None)
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
        side_effect=_active_comments_job_fetch_one("completed"),
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
    monkeypatch.setattr(jr, "select_comments_proxy", lambda *, session_key=None: None)
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
        side_effect=_active_comments_job_fetch_one("completed"),
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
    monkeypatch.setattr(jr, "select_comments_proxy", lambda *, session_key=None: None)
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
        side_effect=_active_comments_job_fetch_one("completed"),
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
    monkeypatch.setattr(jr, "select_comments_proxy", lambda *, session_key=None: None)
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


def test_fetch_comments_swaps_cursor_direction_when_min_id_repeats(monkeypatch) -> None:
    """Phase A5 follow-up: when IG returns the same next_min_id twice but
    also ships a next_max_id, the fetcher swaps direction and continues
    paginating instead of declaring repeated_cursor terminal.
    """
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
                reply_count=0,
            ),
            InstagramComment(
                comment_id="c2",
                text="two",
                username="beta",
                user_id="2",
                created_at=2,
                date_time="1970-01-01T00:00:02+00:00",
                likes=0,
                is_reply=False,
                parent_comment_id=None,
                reply_count=0,
            ),
            InstagramComment(
                comment_id="c3",
                text="three",
                username="gamma",
                user_id="3",
                created_at=3,
                date_time="1970-01-01T00:00:03+00:00",
                likes=0,
                is_reply=False,
                parent_comment_id=None,
                reply_count=0,
            ),
        ]
    )
    fetcher._fetch_json_response = AsyncMock(
        side_effect=[
            # Page 1: primary follows next_max_id but next_min_id is also exposed.
            {
                "payload": {
                    "comments": [{"id": "c1"}],
                    "has_more_comments": True,
                    "next_max_id": "max-2",
                    "next_min_id": "min-9",
                },
                "failed": False,
                "auth_failed": False,
                "reason": None,
                "retryable": False,
            },
            # Page 2: max_id repeats (max-2 again) -> fetcher should swap to min_id.
            {
                "payload": {
                    "comments": [{"id": "c2"}],
                    "has_more_comments": True,
                    "next_max_id": "max-2",
                    "next_min_id": "min-9",
                },
                "failed": False,
                "auth_failed": False,
                "reason": None,
                "retryable": False,
            },
            # Page 3 (after swap): served with min_id=min-9; loop terminates cleanly.
            {
                "payload": {
                    "comments": [{"id": "c3"}],
                    "has_more_comments": False,
                    "has_more_headload_comments": False,
                },
                "failed": False,
                "auth_failed": False,
                "reason": None,
                "retryable": False,
            },
        ]
    )

    result = asyncio.run(
        fetcher.fetch_comments_for_shortcode(
            "ABC123",
            max_comments=10,
            fetch_replies=False,
        )
    )

    # All three comments persisted.
    assert [c.comment_id for c in result.comments] == ["c1", "c2", "c3"]
    # Loop terminated cleanly — pagination ran to completion via the alt cursor.
    assert result.fetch_failed is False
    assert result.fetch_reason is None
    # Direction swap surfaced in runtime metadata.
    assert fetcher.runtime_metadata["cursor_direction_swaps"]["top_level"] == 1
    assert fetcher.runtime_metadata["retry_reason_counts"].get("pagination_repeated_cursor_swap_direction") == 1


def test_zstd_decoding_error_is_retryable_transport_error() -> None:
    """Regression (2026-06-11): zstd bodies mangled by the API proxy raise
    httpx.DecodingError("zstd decompressor error: Unknown frame descriptor").
    The job-level classifier must treat it as a retryable transport failure;
    when it did not, whole comment shards failed terminally at attempt 1."""
    from trr_backend.socials.instagram.comments_scrapling.job_runner import _is_comments_transport_error

    exc = httpx.DecodingError("zstd decompressor error: Unknown frame descriptor")
    assert _is_comments_transport_error(exc) is True
