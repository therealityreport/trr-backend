"""Retry/backoff, cookie bridge, redirect handling, and partial-progress tests."""

from __future__ import annotations

import asyncio
import json
import time
from contextlib import nullcontext
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from trr_backend.socials.instagram.comments_scrapling.counts import flattened_comment_count, missing_reply_count
from trr_backend.socials.instagram.comments_scrapling.fetcher import (
    InstagramCommentsFetchResult,
    InstagramCommentsScraplingFetcher,
    InstagramCommentsWarmupError,
    _extract_rendered_permalink_comments,
    _pace_global_api_request,
    _record_global_api_cooldown,
)
from trr_backend.socials.instagram.constants import resolve_comment_sort_order
from trr_backend.socials.instagram.scraper import InstagramComment

_FIXTURE_DIR = Path(__file__).parents[1] / "fixtures" / "instagram" / "scrapling"


def _fixture_json(name: str) -> dict:
    return json.loads((_FIXTURE_DIR / name).read_text(encoding="utf-8"))


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

    assert len(result.comments) == 1
    assert result.fetch_failed is False
    assert result.retryable is False
    assert result.fetch_reason == "hidden_comments_unavailable_reconciled"
    fetcher._fetch_rendered_comments_after_revealing_hidden.assert_awaited_once()


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


def test_fetch_comments_fetches_tail_when_preview_replies_are_short(monkeypatch) -> None:
    fetcher = _build_fetcher()
    preview_replies = [_comment(f"r{i}", is_reply=True, parent_comment_id="c1") for i in range(20)]
    fetcher._parser._parse_comment = MagicMock(
        return_value=_comment("c1", reply_count=50, replies=preview_replies)
    )
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
    fetcher._parser._parse_comment = MagicMock(
        return_value=_comment("c1", reply_count=50, replies=preview_replies)
    )
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


def test_reply_tail_budget_gap_can_reconcile_stale_reported_count() -> None:
    from trr_backend.socials.instagram.comments_scrapling import job_runner as jr

    result = InstagramCommentsFetchResult(
        comments=[_comment("c1")],
        fetch_failed=True,
        auth_failed=False,
        fetch_reason="reply_tail_budget_exhausted",
        reported_comment_count=100,
        retryable=True,
    )

    assert jr._persisted_comment_coverage_gap_is_reconcilable(
        result=result,
        stored_total_comments=99,
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
    assert result.fetch_reason == "coverage_target_met"
    fetcher._fetch_rendered_comments_after_revealing_hidden.assert_not_awaited()


def test_fetch_comments_records_top_level_resume_checkpoint_at_page_cap(monkeypatch: pytest.MonkeyPatch) -> None:
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
    assert result.fetch_reason == "pagination_page_cap_reached"
    assert result.top_level_checkpoint is not None
    assert result.top_level_checkpoint["target_shortcode"] == "ABC123"
    assert result.top_level_checkpoint["next_top_level_cursor"] == "cursor-2"
    assert result.top_level_checkpoint["pages_seen"] == 1
    assert fetcher.runtime_metadata["top_level_checkpoint_metadata"]["items"][-1][
        "next_top_level_cursor"
    ] == "cursor-2"


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


def test_fetch_comment_replies_marks_page_cap_retryable(monkeypatch) -> None:
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
    assert result.fetch_reason == "pagination_page_cap_reached"
    assert result.reply_checkpoints == [
        {
            "platform": "instagram",
            "target_shortcode": "ABC123",
            "source_id": "ABC123",
            "media_id": "media-1",
            "parent_comment_id": "c1",
            "stop_reason": "pagination_page_cap_reached",
            "attempt_count": 0,
            "last_error_code": "pagination_page_cap_reached",
            "next_reply_cursor": "reply-cursor-2",
            "next_reply_cursor_param": "min_id",
            "saved_reply_count_observed": 1,
            "pages_seen": 1,
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
    fetcher._parser._parse_comment = MagicMock(
        return_value=_comment("r2", is_reply=True, parent_comment_id="c1")
    )
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
    assert result.fetch_reason == "pagination_page_cap_reached"
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
    assert fetcher.runtime_metadata["retry_reason_counts"]["transport_error"] == (
        InstagramCommentsScraplingFetcher._MAX_TRANSIENT_RETRIES + 1
    )


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


def test_warmup_transport_http_response_code_failure_is_retryable() -> None:
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
    monkeypatch.setenv("DECODO_USERNAME", "user1")
    monkeypatch.setenv("DECODO_PASSWORD", "p@ss!")
    monkeypatch.setenv("DECODO_GATEWAY", "gate.decodo.com:7000")
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
            "target_source_ids": ["AUTHFAIL", "OKPOST"],
            "max_comments_per_post": 10,
            "fetch_replies": False,
        },
        # Phase 1.3 / audit: with max_attempts == attempt_count, the job_runner
        # used to short-circuit can_retry to False on the first transient
        # failure. _job_attempt_state now enforces max_attempts >= attempt_count
        # + 1, so this row reaches "retrying" instead of "failed" — which is
        # the correct behavior the test name (..._for_retry) already implied.
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
        jr.run_instagram_comments_scrapling_job(job, worker_id="test-worker")

    assert finish_calls[-1]["status"] == "retrying"
    assert finish_calls[-1]["last_error_code"] == "instagram_comments_incomplete_retryable"
    assert persist_calls == ["OKPOST"]
    metadata = finish_calls[-1]["metadata"]
    assert [sample["shortcode"] for sample in metadata["post_latency"]["samples"]] == ["AUTHFAIL", "OKPOST"]
    assert metadata["post_latency"]["samples"][0]["completion_reason"] == "post_auth_failed_skipped"
    assert metadata["post_auth_failures"]["target_source_ids"] == ["AUTHFAIL"]
    assert metadata["post_auth_failures"]["fetch_reasons"] == {
        "AUTHFAIL": "html_challenge_or_auth_required"
    }
    assert metadata["auth_failed_target_source_ids"] == ["AUTHFAIL"]
    assert metadata["runtime_metadata"]["incomplete_target_source_ids"] == ["AUTHFAIL"]
    assert metadata["retry_rebalance"]["remaining_target_source_ids"] == ["AUTHFAIL"]


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


def test_job_runner_fails_after_post_auth_failure_circuit(monkeypatch: pytest.MonkeyPatch) -> None:
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
        jr.run_instagram_comments_scrapling_job(job, worker_id="test-worker")

    assert finish_calls[-1]["status"] == "failed"
    assert fetch_calls == ["AUTH1", "AUTH2", "AUTH3"]
    finish_kwargs = finish_calls[-1]
    assert finish_kwargs["last_error_code"] == "instagram_comments_auth_failed"
    metadata = finish_kwargs["metadata"]
    assert metadata["post_auth_failures"]["target_source_ids"] == ["AUTH1", "AUTH2", "AUTH3"]
    assert metadata["post_auth_failures"]["circuit_limit"] == 3


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

    assert jr._is_comments_transport_error(Exception("SSL connection has been closed unexpectedly"))
    assert jr._is_comments_transport_error(Exception("[SSL] record layer failure (_ssl.c:2590)"))


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

    async def fake_fetch_method(shortcode, *, max_comments, fetch_replies, expected_comment_count=None):
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

    async def fake_fetch_method(shortcode, *, max_comments, fetch_replies, expected_comment_count=None):
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
            "top_level_cursor": "cursor-2",
            "top_level_cursor_param": "max_id",
        }
    ]


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
            "reply_resume_cursors": {"parent-1": "reply-cursor-2"},
            "reply_resume_cursor_params": {"parent-1": "max_id"},
        }
    ]


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


def test_job_runner_reconciles_tiny_stored_reply_gap_without_retrying(
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
    assert reconcile_calls[0]["post_db_id"] == "post-SHORT1"
    assert finish_calls[-1]["status"] == "completed"
    metadata = finish_calls[-1]["metadata"]
    assert metadata["incomplete_target_source_ids"] == []
    assert metadata["post_latency"]["samples"][0]["completion_reason"] == "stored_comment_coverage_reconciled_gap"
    assert metadata["post_latency"]["samples"][0]["stored_total_comments"] == 9


def test_job_runner_reconciles_high_coverage_terminal_pagination_gap(
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

    assert persist_calls == [{"shortcode": "SHORT1", "is_complete": False}]
    assert reconcile_calls[0]["post_db_id"] == "post-SHORT1"
    assert finish_calls[-1]["status"] == "completed"
    metadata = finish_calls[-1]["metadata"]
    assert metadata["incomplete_target_source_ids"] == []
    assert (
        metadata["post_latency"]["samples"][0]["completion_reason"]
        == "stored_comment_coverage_terminal_gap_reconciled"
    )
    assert metadata["post_latency"]["samples"][0]["stored_total_comments"] == 910


def test_job_runner_reconciles_high_coverage_terminal_transient_gap(
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

    assert reconcile_calls[0]["post_db_id"] == "post-SHORT1"
    assert finish_calls[-1]["status"] == "completed"
    metadata = finish_calls[-1]["metadata"]
    assert metadata["incomplete_target_source_ids"] == []
    assert (
        metadata["post_latency"]["samples"][0]["completion_reason"]
        == "stored_comment_coverage_terminal_gap_reconciled"
    )
    assert metadata["post_latency"]["samples"][0]["stored_total_comments"] == 847


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
