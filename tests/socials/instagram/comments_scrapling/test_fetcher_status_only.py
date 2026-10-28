"""Status-only Instagram comments endpoint classification tests."""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from trr_backend.socials.instagram.comments_scrapling.counts import (
    child_reply_count,
    flattened_comment_count,
    parent_comment_count,
)
from trr_backend.socials.instagram.comments_scrapling.fetcher import (
    InstagramCommentsScraplingFetcher,
    _extract_graphql_child_connection_comments,
    _extract_graphql_connection_comments,
    _extract_graphql_preview_comments,
    _extract_rendered_dom_snapshot_comments,
    _extract_rendered_permalink_comments,
    _public_comments_coverage_metadata,
    _public_relay_block_reason_from_response,
    _target_metadata_indicates_coauthor,
)
from trr_backend.socials.instagram.scraper import InstagramComment


def _build_fetcher() -> InstagramCommentsScraplingFetcher:
    with patch("scrapling.fetchers.StealthyFetcher", MagicMock()):
        fetcher = InstagramCommentsScraplingFetcher(
            cookies=[],
            raw_cookies={"csrftoken": "initial"},
            browser_account_id="testaccount",
        )
        asyncio.run(fetcher._rebuild_http_client())
        return fetcher


def _comment(
    comment_id: str,
    *,
    username: str = "viewer_account",
    text: str = "rendered comment",
    replies: list[InstagramComment] | None = None,
    reply_count: int = 0,
    is_reply: bool = False,
    parent_comment_id: str | None = None,
) -> InstagramComment:
    return InstagramComment(
        comment_id=comment_id,
        text=text,
        username=username,
        user_id="1",
        created_at=1,
        date_time="1970-01-01 00:00:01",
        likes=0,
        is_reply=is_reply,
        parent_comment_id=parent_comment_id,
        reply_count=reply_count,
        reply_depth=1 if is_reply else 0,
        replies=list(replies or []),
    )


class _AsyncClientContext:
    def __init__(self, client):
        self.client = client

    async def __aenter__(self):
        return self.client

    async def __aexit__(self, exc_type, exc, traceback):
        return False


def test_public_comments_coverage_stays_partial_on_exhausted_relay_with_display_gap() -> None:
    metadata = _public_comments_coverage_metadata(
        advertised_count=27_000,
        recovered_count=1_044,
        terminal_reason="pagination_complete",
        max_comments=0,
        missing_replies=0,
    )

    assert metadata["classification"] == "public_partial"
    assert metadata["target_gap_count"] == 25_956
    assert metadata["fallback_blocked_reason"] == "public_comments_partial_public_recovery_pending"


@pytest.mark.parametrize(
    "advertised,recovered,limit,reason,missing,classification,limited,exhausted",
    [
        (27_000, 100, 100, "graphql_relay_target_reached", 0, "public_partial", True, False),
        (27_000, 100, 100, "pagination_complete", 0, "public_partial", True, True),
        (None, 100, 100, "graphql_relay_target_reached", 0, "public_partial", True, False),
        (100, 100, 100, "graphql_relay_target_reached", 0, "public_partial", True, False),
        (100, 100, 100, "pagination_complete", 0, "public_complete", False, True),
        (100, 100, 0, "graphql_relay_target_reached", 0, "public_complete", False, False),
        (None, 5, 100, "pagination_complete", 0, "public_complete", False, True),
        (100, 100, 0, "pagination_complete", 2, "public_partial", False, True),
    ],
)
def test_public_comments_coverage_distinguishes_request_limit_from_completion(
    advertised, recovered, limit, reason, missing, classification, limited, exhausted
) -> None:
    metadata = _public_comments_coverage_metadata(
        advertised_count=advertised,
        recovered_count=recovered,
        terminal_reason=reason,
        max_comments=limit,
        missing_replies=missing,
    )

    assert metadata["classification"] == classification
    assert metadata["request_limit_reached"] is (limit > 0 and recovered >= limit)
    assert metadata["limited_by_request"] is limited
    assert metadata["source_exhausted"] is exhausted
    if limited or classification == "public_complete":
        assert metadata["fallback_blocked_reason"] is None
    else:
        assert metadata["fallback_blocked_reason"] == "public_comments_partial_public_recovery_pending"


def test_public_relay_nested_graphql_rate_limit_code_is_blocking() -> None:
    response = httpx.Response(
        200,
        request=httpx.Request("POST", "https://www.instagram.com/graphql/query/"),
    )
    payload = {
        "data": {
            "comments": {
                "errors": [
                    {
                        "message": "Please wait a few minutes before you try again.",
                        "extensions": {"code": 1675004},
                    }
                ]
            }
        }
    }

    assert _public_relay_block_reason_from_response(response, payload=payload) == "http_429"


def test_status_only_payload_with_expected_comments_is_not_hidden_comments() -> None:
    fetcher = _build_fetcher()
    fetcher._fetch_json_response = AsyncMock(
        return_value={
            "payload": {"status": "ok"},
            "failed": False,
            "auth_failed": False,
            "reason": None,
            "retryable": False,
        }
    )
    fetcher._fetch_rendered_comments_after_revealing_hidden = AsyncMock(return_value=[])
    fetcher._fetch_graphql_coauthor_comments_for_status_only = AsyncMock(return_value=([], {}))
    fetcher._fetch_rendered_coauthor_comments_for_status_only = AsyncMock(return_value=[])

    result = asyncio.run(
        fetcher.fetch_comments_for_shortcode(
            "ABC123",
            max_comments=0,
            fetch_replies=False,
            expected_comment_count=5,
        )
    )

    assert result.comments == []
    assert result.fetch_failed is True
    assert result.retryable is True
    assert result.fetch_reason == "comments_endpoint_status_only"
    assert result.diagnostic_metadata["payload_keys"] == ["status"]
    assert result.diagnostic_metadata["expected_comment_count"] == 5
    assert result.diagnostic_metadata["fallback_attempted"] is False
    assert result.top_level_checkpoint is not None
    assert result.top_level_checkpoint["stop_reason"] == "comments_endpoint_status_only"
    assert result.top_level_checkpoint["diagnostic_metadata"]["payload_keys"] == ["status"]
    fetcher._fetch_rendered_comments_after_revealing_hidden.assert_not_awaited()
    fetcher._fetch_graphql_coauthor_comments_for_status_only.assert_not_awaited()
    fetcher._fetch_rendered_coauthor_comments_for_status_only.assert_not_awaited()


def test_coauthor_status_only_payload_attempts_bounded_rendered_fallback() -> None:
    fetcher = _build_fetcher()
    fetcher._fetch_json_response = AsyncMock(
        return_value={
            "payload": {"status": "ok"},
            "failed": False,
            "auth_failed": False,
            "reason": None,
            "retryable": False,
        }
    )
    fetcher._fetch_rendered_comments_after_revealing_hidden = AsyncMock(return_value=[])
    fetcher._fetch_graphql_coauthor_comments_for_status_only = AsyncMock(
        return_value=([], {"reason": "graphql_preview_empty"})
    )
    fetcher._fetch_rendered_coauthor_comments_for_status_only = AsyncMock(return_value=[])

    result = asyncio.run(
        fetcher.fetch_comments_for_shortcode(
            "ABC123",
            max_comments=0,
            fetch_replies=False,
            expected_comment_count=5,
            target_metadata={
                "source_account": "thetraitorsus",
                "owner_username": "peacock",
                "collaborator_handles": ["thetraitorsus"],
                "media_type": "video",
            },
        )
    )

    assert result.fetch_failed is True
    assert result.retryable is True
    assert result.fetch_reason == "coauthor_comments_endpoint_empty"
    assert result.diagnostic_metadata["is_coauthor_context"] is True
    assert result.diagnostic_metadata["owner_context"]["owner_username"] == "peacock"
    assert result.diagnostic_metadata["owner_context"]["source_account"] == "thetraitorsus"
    assert result.diagnostic_metadata["fallback_attempted"] is True
    assert result.diagnostic_metadata["fallback_result_counts"] == {
        "graphql_preview_comments": 0,
        "graphql_merged_comments": 0,
        "rendered_comments": 0,
        "rendered_merged_comments": 0,
        "merged_comments": 0,
        "comments_before_fallback": 0,
        "comments_after_fallback": 0,
    }
    fetcher._fetch_graphql_coauthor_comments_for_status_only.assert_awaited_once()
    fetcher._fetch_rendered_comments_after_revealing_hidden.assert_not_awaited()
    fetcher._fetch_rendered_coauthor_comments_for_status_only.assert_awaited_once()


def test_coauthor_status_only_rendered_fallback_can_recover_comments() -> None:
    fetcher = _build_fetcher()
    fetcher._fetch_json_response = AsyncMock(
        return_value={
            "payload": {"status": "ok"},
            "failed": False,
            "auth_failed": False,
            "reason": None,
            "retryable": False,
        }
    )
    fetcher._fetch_rendered_comments_after_revealing_hidden = AsyncMock(return_value=[])
    fetcher._fetch_graphql_coauthor_comments_for_status_only = AsyncMock(return_value=([], {}))
    fetcher._fetch_rendered_coauthor_comments_for_status_only = AsyncMock(return_value=[_comment("rendered-1")])

    result = asyncio.run(
        fetcher.fetch_comments_for_shortcode(
            "ABC123",
            max_comments=0,
            fetch_replies=False,
            expected_comment_count=1,
            target_metadata={
                "source_account": "thetraitorsus",
                "owner_username": "peacock",
                "collaborator_handles": ["thetraitorsus"],
            },
        )
    )

    assert [comment.comment_id for comment in result.comments] == ["rendered-1"]
    assert result.fetch_failed is False
    assert result.retryable is False
    assert result.fetch_reason == "coauthor_comments_fallback_recovered"
    assert result.diagnostic_metadata["fallback_result_counts"] == {
        "graphql_preview_comments": 0,
        "graphql_merged_comments": 0,
        "rendered_comments": 1,
        "rendered_merged_comments": 1,
        "merged_comments": 1,
        "comments_before_fallback": 0,
        "comments_after_fallback": 1,
    }
    fetcher._fetch_graphql_coauthor_comments_for_status_only.assert_awaited_once()
    fetcher._fetch_rendered_comments_after_revealing_hidden.assert_not_awaited()


def test_coauthor_status_only_rendered_partial_classifies_terminal_missing_comments() -> None:
    fetcher = _build_fetcher()
    parent = _comment(
        "rendered-1",
        replies=[_comment("rendered-reply-1", is_reply=True, parent_comment_id="rendered-1")],
        reply_count=1,
    )
    fetcher._fetch_json_response = AsyncMock(
        return_value={
            "payload": {"status": "ok"},
            "failed": False,
            "auth_failed": False,
            "reason": None,
            "retryable": False,
        }
    )
    fetcher._fetch_rendered_comments_after_revealing_hidden = AsyncMock(return_value=[])
    fetcher._fetch_graphql_coauthor_comments_for_status_only = AsyncMock(return_value=([], {}))
    fetcher._fetch_rendered_coauthor_comments_for_status_only = AsyncMock(return_value=[parent])

    result = asyncio.run(
        fetcher.fetch_comments_for_shortcode(
            "ABC123",
            max_comments=0,
            fetch_replies=False,
            expected_comment_count=3,
            target_metadata={
                "source_account": "thetraitorsus",
                "owner_username": "peacock",
                "collaborator_handles": ["thetraitorsus"],
            },
        )
    )

    assert flattened_comment_count(result.comments) == 2
    assert result.fetch_failed is False
    assert result.retryable is False
    assert result.fetch_reason == "coverage_terminal_missing_classified"
    assert result.diagnostic_metadata["missing_reason_counts"] == {"instagram_not_served_after_all_lanes": 1}
    assert result.diagnostic_metadata["formula_label"] == (
        "1 parent comments + 1 child replies + 0 Facebook comments + 1 missing comments = 3 reported comments"
    )


def test_coauthor_status_only_graphql_preview_can_recover_comments() -> None:
    fetcher = _build_fetcher()
    fetcher._fetch_json_response = AsyncMock(
        return_value={
            "payload": {"status": "ok"},
            "failed": False,
            "auth_failed": False,
            "reason": None,
            "retryable": False,
        }
    )
    fetcher._fetch_rendered_comments_after_revealing_hidden = AsyncMock(return_value=[])
    fetcher._fetch_graphql_coauthor_comments_for_status_only = AsyncMock(
        return_value=([_comment("graphql-1")], {"doc_id": "doc"})
    )
    fetcher._fetch_rendered_coauthor_comments_for_status_only = AsyncMock(return_value=[])

    result = asyncio.run(
        fetcher.fetch_comments_for_shortcode(
            "ABC123",
            max_comments=0,
            fetch_replies=False,
            expected_comment_count=1,
            target_metadata={
                "source_account": "thetraitorsus",
                "owner_username": "peacock",
                "collaborator_handles": ["thetraitorsus"],
            },
        )
    )

    assert [comment.comment_id for comment in result.comments] == ["graphql-1"]
    assert result.fetch_failed is False
    assert result.retryable is False
    assert result.fetch_reason == "coauthor_comments_fallback_recovered"
    assert result.diagnostic_metadata["fallback_result_counts"] == {
        "graphql_preview_comments": 1,
        "graphql_merged_comments": 1,
        "rendered_comments": 0,
        "rendered_merged_comments": 0,
        "merged_comments": 1,
        "comments_before_fallback": 0,
        "comments_after_fallback": 1,
    }
    fetcher._fetch_rendered_coauthor_comments_for_status_only.assert_not_awaited()


def test_coauthor_status_only_graphql_preview_stays_retryable_when_partial() -> None:
    fetcher = _build_fetcher()
    fetcher._fetch_json_response = AsyncMock(
        return_value={
            "payload": {"status": "ok"},
            "failed": False,
            "auth_failed": False,
            "reason": None,
            "retryable": False,
        }
    )
    fetcher._fetch_rendered_comments_after_revealing_hidden = AsyncMock(return_value=[])
    fetcher._fetch_graphql_coauthor_comments_for_status_only = AsyncMock(
        return_value=([_comment("graphql-1")], {"doc_id": "doc", "has_next_page": True})
    )
    fetcher._fetch_rendered_coauthor_comments_for_status_only = AsyncMock(return_value=[])

    result = asyncio.run(
        fetcher.fetch_comments_for_shortcode(
            "ABC123",
            max_comments=0,
            fetch_replies=False,
            expected_comment_count=5,
            target_metadata={
                "source_account": "thetraitorsus",
                "owner_username": "peacock",
                "collaborator_handles": ["thetraitorsus"],
            },
        )
    )

    assert [comment.comment_id for comment in result.comments] == ["graphql-1"]
    assert result.fetch_failed is True
    assert result.retryable is True
    assert result.fetch_reason == "coauthor_comments_endpoint_empty"
    assert result.diagnostic_metadata["fallback_result_counts"]["comments_after_fallback"] == 1
    fetcher._fetch_rendered_coauthor_comments_for_status_only.assert_awaited_once()


def test_is_collaborator_post_boolean_alone_triggers_coauthor_classification() -> None:
    fetcher = _build_fetcher()
    fetcher._fetch_json_response = AsyncMock(
        return_value={
            "payload": {"status": "ok"},
            "failed": False,
            "auth_failed": False,
            "reason": None,
            "retryable": False,
        }
    )
    fetcher._fetch_rendered_comments_after_revealing_hidden = AsyncMock(return_value=[])
    fetcher._fetch_graphql_coauthor_comments_for_status_only = AsyncMock(
        return_value=([], {"reason": "graphql_preview_empty"})
    )
    fetcher._fetch_rendered_coauthor_comments_for_status_only = AsyncMock(return_value=[])

    result = asyncio.run(
        fetcher.fetch_comments_for_shortcode(
            "ABC123",
            max_comments=0,
            fetch_replies=False,
            expected_comment_count=5,
            target_metadata={
                "source_account": "peacock",
                "owner_username": "peacock",
                "is_collaborator_post": True,
            },
        )
    )

    assert result.fetch_reason == "coauthor_comments_endpoint_empty"
    assert result.diagnostic_metadata["is_coauthor_context"] is True
    fetcher._fetch_graphql_coauthor_comments_for_status_only.assert_awaited_once()
    fetcher._fetch_rendered_coauthor_comments_for_status_only.assert_awaited_once()
    fetcher._fetch_rendered_comments_after_revealing_hidden.assert_not_awaited()


def test_has_collaborators_boolean_triggers_coauthor_classification_for_authored_post() -> None:
    fetcher = _build_fetcher()
    fetcher._fetch_json_response = AsyncMock(
        return_value={
            "payload": {"status": "ok"},
            "failed": False,
            "auth_failed": False,
            "reason": None,
            "retryable": False,
        }
    )
    fetcher._fetch_rendered_comments_after_revealing_hidden = AsyncMock(return_value=[])
    fetcher._fetch_graphql_coauthor_comments_for_status_only = AsyncMock(
        return_value=([], {"reason": "graphql_preview_empty"})
    )
    fetcher._fetch_rendered_coauthor_comments_for_status_only = AsyncMock(return_value=[])

    result = asyncio.run(
        fetcher.fetch_comments_for_shortcode(
            "DTRdpWtjbz5",
            max_comments=0,
            fetch_replies=False,
            expected_comment_count=5,
            target_metadata={
                "source_account": "thetraitorsus",
                "owner_username": "thetraitorsus",
                "collaborator_handles": ["johnnygweir", "nbcsports"],
                "is_collaborator_post": False,
                "has_collaborators": True,
            },
        )
    )

    assert result.fetch_reason == "coauthor_comments_endpoint_empty"
    assert result.diagnostic_metadata["is_coauthor_context"] is True
    assert _target_metadata_indicates_coauthor(
        {
            "source_account": "thetraitorsus",
            "owner_username": "thetraitorsus",
            "collaborator_handles": ["johnnygweir", "nbcsports"],
            "has_collaborators": True,
        }
    )
    fetcher._fetch_graphql_coauthor_comments_for_status_only.assert_awaited_once()
    fetcher._fetch_rendered_coauthor_comments_for_status_only.assert_awaited_once()
    fetcher._fetch_rendered_comments_after_revealing_hidden.assert_not_awaited()


def test_extract_graphql_preview_comments_from_post_action_payload() -> None:
    payload = {
        "data": {
            "xdt_shortcode_media": {
                "edge_media_to_parent_comment": {
                    "count": 149,
                    "page_info": {"has_next_page": True, "end_cursor": "cursor"},
                    "edges": [
                        {
                            "node": {
                                "id": "18452344879099188",
                                "text": "Eric is the best!",
                                "created_at": 1772766459,
                                "owner": {
                                    "id": "38867211",
                                    "username": "jennjin3",
                                    "is_verified": False,
                                    "profile_pic_url": "https://cdn.example/avatar.jpg",
                                },
                                "edge_liked_by": {"count": 2},
                                "edge_threaded_comments": {
                                    "count": 1,
                                    "page_info": {"has_next_page": False, "end_cursor": None},
                                    "edges": [
                                        {
                                            "node": {
                                                "id": "17900000000000001",
                                                "text": "Reply body",
                                                "created_at": 1772766460,
                                                "owner": {"id": "42", "username": "reply_user"},
                                            }
                                        }
                                    ],
                                },
                            }
                        }
                    ],
                }
            }
        }
    }

    comments, metadata = _extract_graphql_preview_comments(
        payload,
        shortcode="DU_oEbbgZfJ",
        post_url="https://www.instagram.com/p/DU_oEbbgZfJ/",
    )

    assert len(comments) == 1
    assert comments[0].comment_id == "18452344879099188"
    assert comments[0].username == "jennjin3"
    assert comments[0].likes == 2
    assert comments[0].reply_count == 1
    assert comments[0].source_snapshot_type == "graphql_coauthor_preview_comments"
    assert comments[0].replies[0].comment_id == "17900000000000001"
    assert comments[0].replies[0].is_reply is True
    assert comments[0].replies[0].parent_comment_id == "18452344879099188"
    assert metadata["reported_comment_count"] == 149
    assert metadata["top_level_preview_count"] == 1
    assert metadata["flattened_preview_count"] == 2
    assert metadata["has_next_page"] is True


def test_rendered_permalink_comments_accept_absolute_profile_links() -> None:
    html = """
    <div>
      <a href="https://www.instagram.com/feelingjonesy/">feelingjonesy</a>
      <a href="/p/DU_oEbbgZfJ/c/18243599467305250/">
        <time datetime="2026-02-20T21:08:36.000Z">10w</time>
      </a>
      <span dir="auto">I feel like the vibes are actually not in fact up</span>
      <span dir="auto">184 likes</span>
    </div>
    """

    comments = _extract_rendered_permalink_comments(
        html,
        shortcode="DU_oEbbgZfJ",
        post_url="https://www.instagram.com/p/DU_oEbbgZfJ/",
        ignored_usernames=["peacock", "thetraitorsus"],
        source_snapshot_type="rendered_coauthor_comments",
        is_hidden_by_instagram=False,
    )

    assert len(comments) == 1
    assert comments[0].comment_id == "18243599467305250"
    assert comments[0].username == "feelingjonesy"
    assert comments[0].text == "I feel like the vibes are actually not in fact up"
    assert comments[0].source_snapshot_type == "rendered_coauthor_comments"


def test_rendered_dom_snapshot_comments_attach_indented_replies() -> None:
    payload = {
        "rows": [
            {
                "username": "bracketology.tv",
                "rowText": "bracketology.tv 10w Um, I actually think that vibes are pretty bad..? 4 likes Reply",
                "left": 100,
            },
            {
                "username": "champagne.roast",
                "rowText": "champagne.roast 10w @bracketology.tv 😂😂😂 Reply",
                "left": 144,
            },
            {
                "username": "jennjin3",
                "rowText": "jennjin3 10w Eric is the best! Reply",
                "left": 100,
            },
        ]
    }
    html = (
        f'<html><script id="trr-rendered-comments-json" type="application/json">{json.dumps(payload)}</script></html>'
    )

    comments = _extract_rendered_dom_snapshot_comments(
        html,
        shortcode="DU_oEbbgZfJ",
        post_url="https://www.instagram.com/p/DU_oEbbgZfJ/",
        ignored_usernames=["peacock", "thetraitorsus"],
    )

    assert len(comments) == 2
    assert comments[0].comment_id.startswith("rendered_")
    assert comments[0].username == "bracketology.tv"
    assert comments[0].text == "Um, I actually think that vibes are pretty bad..?"
    assert comments[0].likes == 4
    assert comments[0].reply_count == 1
    assert comments[0].replies[0].username == "champagne.roast"
    assert comments[0].replies[0].is_reply is True
    assert comments[0].replies[0].parent_comment_id == comments[0].comment_id
    assert comments[1].username == "jennjin3"


def test_rendered_dom_snapshot_comments_preserve_permalink_fallback_ids() -> None:
    payload = {
        "rows": [
            {
                "username": "msjeepgirl",
                "rowText": "msjeepgirl 11w Love Johnny and Tara! 2 likes Reply",
                "commentId": "17900000000000001",
                "commentHref": "/p/DT1ht84DYB4/c/17900000000000001/",
                "left": 100,
            }
        ]
    }
    html = (
        f'<html><script id="trr-rendered-comments-json" type="application/json">{json.dumps(payload)}</script></html>'
    )

    comments = _extract_rendered_dom_snapshot_comments(
        html,
        shortcode="DT1ht84DYB4",
        post_url="https://www.instagram.com/p/DT1ht84DYB4/",
        ignored_usernames=["thetraitorsus", "nbcolympics"],
    )

    assert len(comments) == 1
    assert comments[0].comment_id == "17900000000000001"
    assert comments[0].comment_url == "https://www.instagram.com/p/DT1ht84DYB4/c/17900000000000001/"
    assert comments[0].username == "msjeepgirl"
    assert comments[0].text == "Love Johnny and Tara!"
    assert comments[0].likes == 2


def test_extract_graphql_relay_parent_comments_preserves_child_count() -> None:
    payload = {
        "data": {
            "xdt_api__v1__media__media_id__comments__connection": {
                "edges": [
                    {
                        "node": {
                            "pk": "18346102966234863",
                            "text": "Um, I actually think that vibes are pretty bad..?",
                            "created_at": 1772766459,
                            "child_comment_count": 1,
                            "comment_like_count": 4,
                            "user": {"pk": "100", "username": "bracketology.tv"},
                        }
                    }
                ],
                "page_info": {"has_next_page": False, "end_cursor": None},
            }
        }
    }

    comments, metadata = _extract_graphql_connection_comments(
        payload,
        shortcode="DU_oEbbgZfJ",
        post_url="https://www.instagram.com/p/DU_oEbbgZfJ/",
    )

    assert len(comments) == 1
    assert comments[0].comment_id == "18346102966234863"
    assert comments[0].username == "bracketology.tv"
    assert comments[0].reply_count == 1
    assert comments[0].likes == 4
    assert comments[0].phase == "parent"
    assert metadata["top_level_count"] == 1
    assert metadata["flattened_count"] == 1


def test_extract_graphql_child_connection_comments_marks_replies() -> None:
    payload = {
        "data": {
            "xdt_api__v1__media__media_id__comments__parent_comment_id__child_comments__connection": {
                "edges": [
                    {
                        "node": {
                            "pk": "18055682729437066",
                            "text": "@bracketology.tv \U0001f602\U0001f602\U0001f602",
                            "created_at": 1772766460,
                            "parent_comment_id": "18346102966234863",
                            "comment_like_count": 1,
                            "user": {"pk": "101", "username": "champagne.roast"},
                        }
                    }
                ],
                "page_info": {"has_next_page": False, "end_cursor": None},
            }
        }
    }

    comments, metadata = _extract_graphql_child_connection_comments(
        payload,
        shortcode="DU_oEbbgZfJ",
        post_url="https://www.instagram.com/p/DU_oEbbgZfJ/",
        parent_comment_id="18346102966234863",
    )

    assert len(comments) == 1
    assert comments[0].comment_id == "18055682729437066"
    assert comments[0].username == "champagne.roast"
    assert comments[0].is_reply is True
    assert comments[0].parent_comment_id == "18346102966234863"
    assert comments[0].reply_depth == 1
    assert comments[0].source_snapshot_type == "graphql_coauthor_relay_comments"
    assert comments[0].phase == "child"
    assert metadata["reply_count"] == 1


def _du_oebbgzfj_comments() -> list[InstagramComment]:
    parents = [
        _comment(
            "18346102966234863" if index == 0 else f"du-parent-{index:03d}",
            username="bracketology.tv" if index == 0 else f"parent_user_{index}",
            text="Um, I actually think that vibes are pretty bad..?" if index == 0 else "Parent comment",
            reply_count=0,
        )
        for index in range(104)
    ]
    replies = [
        _comment(
            "18055682729437066",
            username="champagne.roast",
            text="@bracketology.tv exactly",
            is_reply=True,
            parent_comment_id="18346102966234863",
        )
    ]
    replies.extend(
        _comment(
            f"du-child-{index:03d}",
            username=f"child_user_{index}",
            is_reply=True,
            parent_comment_id=parents[index % len(parents)].comment_id,
        )
        for index in range(1, 40)
    )
    for index, reply in enumerate(replies):
        parent = parents[index % len(parents)]
        reply.parent_comment_id = parent.comment_id
        parent.replies.append(reply)
    return parents


def test_coauthor_status_only_du_shape_classifies_terminal_missing_comments() -> None:
    fetcher = _build_fetcher()
    du_comments = _du_oebbgzfj_comments()
    fetcher._fetch_json_response = AsyncMock(
        return_value={
            "payload": {"status": "ok"},
            "failed": False,
            "auth_failed": False,
            "reason": None,
            "retryable": False,
        }
    )
    fetcher._fetch_graphql_coauthor_comments_for_status_only = AsyncMock(
        return_value=(
            du_comments,
            {
                "fallback_source": "public_relay_comments",
                "relay_comments": {
                    "reason": "pagination_complete",
                    "child_comments": {
                        "attempted": True,
                        "reason": "completed",
                        "parent_attempts": 104,
                        "merged_replies": 40,
                    },
                },
            },
        )
    )
    fetcher._fetch_rendered_coauthor_comments_for_status_only = AsyncMock(return_value=[])
    fetcher._fetch_rendered_comments_after_revealing_hidden = AsyncMock(return_value=[])

    result = asyncio.run(
        fetcher.fetch_comments_for_shortcode(
            "DU_oEbbgZfJ",
            max_comments=0,
            fetch_replies=True,
            expected_comment_count=149,
            target_metadata={
                "source_account": "thetraitorsus",
                "owner_username": "peacock",
                "collaborator_handles": ["thetraitorsus"],
            },
        )
    )

    assert parent_comment_count(result.comments) == 104
    assert child_reply_count(result.comments) == 40
    assert flattened_comment_count(result.comments) == 144
    assert result.fetch_failed is False
    assert result.retryable is False
    assert result.fetch_reason == "coverage_terminal_missing_classified"
    assert result.diagnostic_metadata["missing_reason_counts"] == {"instagram_not_served_after_all_lanes": 5}
    assert result.diagnostic_metadata["formula_label"] == (
        "104 parent comments + 40 child replies + 0 Facebook comments + 5 missing comments = 149 reported comments"
    )
    assert result.comments[0].replies[0].username == "champagne.roast"


def test_public_relay_child_hydration_probes_zero_count_parent_when_enabled(monkeypatch) -> None:
    # T3: the default skips reply-less parents (limit 0). A positive probe limit
    # re-enables probing them, preserving the hidden-reply capability as opt-in.
    monkeypatch.delenv("SOCIAL_INSTAGRAM_COMMENTS_COAUTHOR_GRAPHQL_CHILD_MAX_PAGES", raising=False)
    monkeypatch.setenv("SOCIAL_INSTAGRAM_PUBLIC_COMMENTS_ZERO_REPLY_PROBE_LIMIT", "5")
    fetcher = _build_fetcher()
    fetcher._pace_api_requests = AsyncMock(return_value=True)
    parent = _comment("18346102966234863", username="bracketology.tv", reply_count=0)
    payload = {
        "data": {
            "xdt_api__v1__media__media_id__comments__parent_comment_id__child_comments__connection": {
                "edges": [
                    {
                        "node": {
                            "pk": "18055682729437066",
                            "text": "@bracketology.tv exactly",
                            "created_at": 1772766460,
                            "parent_comment_id": "18346102966234863",
                            "comment_like_count": 1,
                            "user": {"pk": "101", "username": "champagne.roast"},
                        }
                    }
                ],
                "page_info": {"has_next_page": False, "end_cursor": None},
            }
        }
    }
    public_client = MagicMock()
    public_client.post = AsyncMock(return_value=httpx.Response(200, json=payload))

    metadata = asyncio.run(
        fetcher._fetch_public_relay_child_comments_for_status_only(
            public_client=public_client,
            shortcode="DU_oEbbgZfJ",
            post_url="https://www.instagram.com/p/DU_oEbbgZfJ/",
            media_id="123",
            comments=[parent],
            graphql_headers={},
            common_body={},
            target_count=1,
            max_comments=0,
        )
    )

    assert metadata["attempted"] is True
    assert metadata["max_pages"] is None
    assert metadata["page_cap_disabled"] is True
    assert metadata["parent_attempts"] == 1
    assert metadata["merged_replies"] == 1
    assert parent.reply_count == 1
    assert parent.replies[0].username == "champagne.roast"


def test_public_relay_child_hydration_honors_zero_count_parent_cap(monkeypatch) -> None:
    # T3: with a probe limit of 1, only the first reply-less parent is probed and
    # the rest are skipped — saving child requests on the global ~4 req/s budget.
    monkeypatch.setenv("SOCIAL_INSTAGRAM_PUBLIC_COMMENTS_ZERO_REPLY_PROBE_LIMIT", "1")
    fetcher = _build_fetcher()
    fetcher._pace_api_requests = AsyncMock(return_value=True)
    parents = [
        _comment("parent-1", username="alpha", reply_count=0),
        _comment("parent-2", username="beta", reply_count=0),
    ]
    payload = {
        "data": {
            "xdt_api__v1__media__media_id__comments__parent_comment_id__child_comments__connection": {
                "edges": [],
                "page_info": {"has_next_page": False, "end_cursor": None},
            }
        }
    }
    public_client = MagicMock()
    public_client.post = AsyncMock(return_value=httpx.Response(200, json=payload))

    metadata = asyncio.run(
        fetcher._fetch_public_relay_child_comments_for_status_only(
            public_client=public_client,
            shortcode="DVPmpFkgfzS",
            post_url="https://www.instagram.com/p/DVPmpFkgfzS/",
            media_id="123",
            comments=parents,
            graphql_headers={},
            common_body={},
            target_count=2,
            max_comments=0,
        )
    )

    assert metadata["zero_count_probe_limit"] == 1
    assert metadata["zero_count_parent_probes"] == 1
    assert metadata["parent_attempts"] == 1
    assert metadata["parents_skipped_without_reply_gap"] == 1
    assert public_client.post.await_count == 1


def test_public_relay_child_hydration_skips_zero_count_parents_by_default(monkeypatch) -> None:
    # T3 default: reply-less parents issue NO child GraphQL request (the dominant
    # per-post cost on the global ~4 req/s budget). One env var re-enables probing.
    monkeypatch.delenv("SOCIAL_INSTAGRAM_PUBLIC_COMMENTS_ZERO_REPLY_PROBE_LIMIT", raising=False)
    fetcher = _build_fetcher()
    fetcher._pace_api_requests = AsyncMock(return_value=True)
    parents = [
        _comment("parent-1", username="alpha", reply_count=0),
        _comment("parent-2", username="beta", reply_count=0),
    ]
    public_client = MagicMock()
    public_client.post = AsyncMock(return_value=httpx.Response(200, json={"data": {}}))

    metadata = asyncio.run(
        fetcher._fetch_public_relay_child_comments_for_status_only(
            public_client=public_client,
            shortcode="DVPmpFkgfzS",
            post_url="https://www.instagram.com/p/DVPmpFkgfzS/",
            media_id="123",
            comments=parents,
            graphql_headers={},
            common_body={},
            target_count=2,
            max_comments=0,
        )
    )

    assert metadata["zero_count_probe_limit"] == 0
    assert metadata["zero_count_parent_probes"] == 0
    assert metadata["parent_attempts"] == 0
    assert metadata["parents_skipped_without_reply_gap"] == 2
    assert public_client.post.await_count == 0


def test_public_relay_child_hydration_does_not_downgrade_after_rate_limit(monkeypatch) -> None:
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_COAUTHOR_GRAPHQL_CHILD_PAGE_SIZE", "50")
    fetcher = _build_fetcher()
    fetcher._pace_api_requests = AsyncMock(return_value=True)
    parent = _comment("parent-1", username="alpha", reply_count=1)
    public_client = MagicMock()
    public_client.post = AsyncMock(
        return_value=httpx.Response(
            429,
            text="too many requests",
            request=httpx.Request("POST", "https://www.instagram.com/graphql/query/"),
        )
    )

    with patch(
        "trr_backend.socials.instagram.comments_scrapling.fetcher._post_child_comments_graphql_doc_attempts",
        return_value=[("ChildCommentsQuery", "doc-1")],
    ):
        metadata = asyncio.run(
            fetcher._fetch_public_relay_child_comments_for_status_only(
                public_client=public_client,
                shortcode="ABC123",
                post_url="https://www.instagram.com/p/ABC123/",
                media_id="123",
                comments=[parent],
                graphql_headers={},
                common_body={},
                target_count=2,
                max_comments=0,
            )
        )

    assert metadata["reason"] == "http_429"
    assert metadata["block_reason"] == "http_429"
    assert public_client.post.await_count == 1
    assert "graphql_child_relay_page_size_downgrade" not in fetcher.runtime_metadata["retry_reason_counts"]


def test_coauthor_relay_prefers_authenticated_session_for_parent_comments() -> None:
    fetcher = _build_fetcher()
    fetcher._raw_cookies.update(
        {
            "csrftoken": "csrf-token",
            "ds_user_id": "123456",
            "sessionid": "session-token",
        }
    )
    fetcher._pace_api_requests = AsyncMock(return_value=True)
    fetcher._fetch_public_relay_child_comments_for_status_only = AsyncMock(
        return_value={"attempted": False, "reason": "test_child_lane_skipped"}
    )
    html = '"LSD",[],{"token":"lsd-token"}'
    payload = {
        "data": {
            "xdt_api__v1__media__media_id__comments__connection": {
                "edges": [
                    {
                        "node": {
                            "pk": "18346102966234863",
                            "text": "Um, I actually think that vibes are pretty bad..?",
                            "created_at": 1772766459,
                            "child_comment_count": 1,
                            "comment_like_count": 4,
                            "user": {"pk": "100", "username": "bracketology.tv"},
                        }
                    }
                ],
                "page_info": {"has_next_page": False, "end_cursor": None},
            }
        }
    }
    fake_client = MagicMock()
    fake_client.get = AsyncMock(
        return_value=httpx.Response(
            200,
            text=html,
            request=httpx.Request("GET", "https://www.instagram.com/p/DU_oEbbgZfJ/"),
        )
    )
    fake_client.post = AsyncMock(
        return_value=httpx.Response(
            200,
            json=payload,
            request=httpx.Request("POST", "https://www.instagram.com/graphql/query/"),
        )
    )
    fetcher._http_client = fake_client

    comments, metadata = asyncio.run(
        fetcher._fetch_public_relay_coauthor_comments_for_status_only(
            "DU_oEbbgZfJ",
            "https://www.instagram.com/p/DU_oEbbgZfJ/",
            media_id="123",
            expected_comment_count=1,
            max_comments=0,
        )
    )

    assert [comment.comment_id for comment in comments] == ["18346102966234863"]
    assert metadata["auth_mode"] == "authenticated"
    assert metadata["fallback_source"] == "authenticated_relay_comments"
    assert metadata["mode_attempts"][0]["auth_mode"] == "authenticated"
    json.dumps(metadata)
    fake_client.get.assert_awaited_once()
    fake_client.post.assert_awaited_once()
    posted_data = fake_client.post.await_args.kwargs["data"]
    posted_variables = json.loads(posted_data["variables"])
    assert posted_data["av"] == "123456"
    assert posted_data["__user"] == "123456"
    assert posted_variables["__relay_internal__pv__PolarisIsLoggedInrelayprovider"] is True
    assert posted_variables["media_id"] == "123"
    fetcher._fetch_public_relay_child_comments_for_status_only.assert_awaited_once()
    child_await_args = fetcher._fetch_public_relay_child_comments_for_status_only.await_args
    assert child_await_args is not None
    assert child_await_args.kwargs["relay_is_logged_in"] is True


def test_public_relay_parent_pagination_defaults_to_uncapped(monkeypatch) -> None:
    monkeypatch.delenv("SOCIAL_INSTAGRAM_COMMENTS_COAUTHOR_GRAPHQL_MAX_PAGES", raising=False)
    fetcher = _build_fetcher()
    fetcher._raw_cookies.update(
        {
            "csrftoken": "csrf-token",
            "ds_user_id": "123456",
            "sessionid": "session-token",
        }
    )
    fetcher._pace_api_requests = AsyncMock(return_value=True)
    fetcher._fetch_public_relay_child_comments_for_status_only = AsyncMock(
        return_value={"attempted": False, "reason": "test_child_lane_skipped"}
    )
    html = '"LSD",[],{"token":"lsd-token"}'

    def page_payload(comment_id: str, *, cursor: str | None, has_next: bool) -> dict[str, object]:
        return {
            "data": {
                "xdt_api__v1__media__media_id__comments__connection": {
                    "edges": [
                        {
                            "node": {
                                "pk": comment_id,
                                "text": f"comment {comment_id}",
                                "created_at": 1772766459,
                                "child_comment_count": 0,
                                "comment_like_count": 0,
                                "user": {"pk": f"user-{comment_id}", "username": f"user{comment_id}"},
                            }
                        }
                    ],
                    "page_info": {"has_next_page": has_next, "end_cursor": cursor},
                }
            }
        }

    fake_client = MagicMock()
    fake_client.get = AsyncMock(
        return_value=httpx.Response(
            200,
            text=html,
            request=httpx.Request("GET", "https://www.instagram.com/p/DU_oEbbgZfJ/"),
        )
    )
    fake_client.post = AsyncMock(
        side_effect=[
            httpx.Response(
                200,
                json=page_payload("1", cursor="cursor-1", has_next=True),
                request=httpx.Request("POST", "https://www.instagram.com/graphql/query/"),
            ),
            httpx.Response(
                200,
                json=page_payload("2", cursor="cursor-2", has_next=True),
                request=httpx.Request("POST", "https://www.instagram.com/graphql/query/"),
            ),
            httpx.Response(
                200,
                json=page_payload("3", cursor=None, has_next=False),
                request=httpx.Request("POST", "https://www.instagram.com/graphql/query/"),
            ),
        ]
    )
    fetcher._http_client = fake_client

    comments, metadata = asyncio.run(
        fetcher._fetch_public_relay_coauthor_comments_for_status_only(
            "DU_oEbbgZfJ",
            "https://www.instagram.com/p/DU_oEbbgZfJ/",
            media_id="123",
            expected_comment_count=3,
            max_comments=0,
        )
    )

    assert [comment.comment_id for comment in comments] == ["1", "2", "3"]
    assert metadata["max_pages"] is None
    assert metadata["page_cap_disabled"] is True
    assert metadata["attempts"][0]["pages"] == 3
    assert fake_client.post.await_count == 3


def test_public_relay_parent_pagination_advances_new_cursor_after_zero_append(monkeypatch) -> None:
    monkeypatch.delenv("SOCIAL_INSTAGRAM_PUBLIC_COMMENTS_ZERO_APPEND_ADVANCE_LIMIT", raising=False)
    fetcher = _build_fetcher()
    fetcher._pace_api_requests = AsyncMock(return_value=True)
    fetcher._fetch_public_relay_child_comments_for_status_only = AsyncMock(
        return_value={"attempted": False, "reason": "test_child_lane_skipped"}
    )
    html = '"LSD",[],{"token":"lsd-token"}'

    def page_payload(comment_id: str, *, cursor: str | None, has_next: bool) -> dict[str, object]:
        return {
            "data": {
                "xdt_api__v1__media__media_id__comments__connection": {
                    "edges": [
                        {
                            "node": {
                                "pk": comment_id,
                                "text": f"comment {comment_id}",
                                "created_at": 1772766459,
                                "child_comment_count": 0,
                                "comment_like_count": 0,
                                "user": {"pk": f"user-{comment_id}", "username": f"user{comment_id}"},
                            }
                        }
                    ],
                    "page_info": {"has_next_page": has_next, "end_cursor": cursor},
                }
            }
        }

    fake_client = MagicMock()
    fake_client.get = AsyncMock(
        return_value=httpx.Response(
            200,
            text=html,
            request=httpx.Request("GET", "https://www.instagram.com/p/DU_oEbbgZfJ/"),
        )
    )
    fake_client.post = AsyncMock(
        side_effect=[
            httpx.Response(
                200,
                json=page_payload("1", cursor="cursor-1", has_next=True),
                request=httpx.Request("POST", "https://www.instagram.com/graphql/query/"),
            ),
            httpx.Response(
                200,
                json=page_payload("1", cursor="cursor-2", has_next=True),
                request=httpx.Request("POST", "https://www.instagram.com/graphql/query/"),
            ),
            httpx.Response(
                200,
                json=page_payload("2", cursor=None, has_next=False),
                request=httpx.Request("POST", "https://www.instagram.com/graphql/query/"),
            ),
        ]
    )

    with patch(
        "trr_backend.socials.instagram.comments_scrapling.fetcher.build_comments_async_client",
        return_value=_AsyncClientContext(fake_client),
    ):
        comments, metadata = asyncio.run(
            fetcher._fetch_public_relay_coauthor_comments_for_status_only(
                "DU_oEbbgZfJ",
                "https://www.instagram.com/p/DU_oEbbgZfJ/",
                media_id="123",
                expected_comment_count=2,
                max_comments=0,
                allow_authenticated=False,
            )
        )

    assert [comment.comment_id for comment in comments] == ["1", "2"]
    assert metadata["reason"] == "child_comments_target_reached"
    assert metadata["pages"][1]["merged_comments"] == 0
    assert metadata["pages"][1]["zero_append_advance"] == 1
    assert metadata["attempts"][0]["zero_append_advances"] == 1
    assert fake_client.post.await_count == 3


def test_public_relay_parent_pagination_retries_non_json_before_stalling(monkeypatch) -> None:
    monkeypatch.delenv("SOCIAL_INSTAGRAM_PUBLIC_COMMENTS_DECODE_RETRY_LIMIT", raising=False)
    fetcher = _build_fetcher()
    fetcher._pace_api_requests = AsyncMock(return_value=True)
    fetcher._fetch_public_relay_child_comments_for_status_only = AsyncMock(
        return_value={"attempted": False, "reason": "test_child_lane_skipped"}
    )
    html = '"LSD",[],{"token":"lsd-token"}'
    payload = {
        "data": {
            "xdt_api__v1__media__media_id__comments__connection": {
                "edges": [
                    {
                        "node": {
                            "pk": "1",
                            "text": "comment 1",
                            "created_at": 1772766459,
                            "child_comment_count": 0,
                            "comment_like_count": 0,
                            "user": {"pk": "user-1", "username": "user1"},
                        }
                    }
                ],
                "page_info": {"has_next_page": False, "end_cursor": None},
            }
        }
    }
    fake_client = MagicMock()
    fake_client.get = AsyncMock(
        return_value=httpx.Response(
            200,
            text=html,
            request=httpx.Request("GET", "https://www.instagram.com/p/DU_oEbbgZfJ/"),
        )
    )
    fake_client.post = AsyncMock(
        side_effect=[
            httpx.Response(
                200,
                text="for (;;);not json",
                request=httpx.Request("POST", "https://www.instagram.com/graphql/query/"),
            ),
            httpx.Response(
                200,
                json=payload,
                request=httpx.Request("POST", "https://www.instagram.com/graphql/query/"),
            ),
        ]
    )

    with (
        patch(
            "trr_backend.socials.instagram.comments_scrapling.fetcher.build_comments_async_client",
            return_value=_AsyncClientContext(fake_client),
        ),
        patch(
            "trr_backend.socials.instagram.comments_scrapling.fetcher._sleep_before_deadline",
            AsyncMock(return_value=True),
        ),
    ):
        comments, metadata = asyncio.run(
            fetcher._fetch_public_relay_coauthor_comments_for_status_only(
                "DU_oEbbgZfJ",
                "https://www.instagram.com/p/DU_oEbbgZfJ/",
                media_id="123",
                expected_comment_count=1,
                max_comments=0,
                allow_authenticated=False,
            )
        )

    assert [comment.comment_id for comment in comments] == ["1"]
    assert metadata["reason"] == "child_comments_target_reached"
    assert metadata["attempts"][0]["decode_retries"] == 1
    assert metadata["attempts"][0]["last_decode_retry_reason"] == "graphql_relay_non_json_response"
    assert fake_client.post.await_count == 2


def test_public_relay_parent_pagination_does_not_retry_true_block_signal() -> None:
    fetcher = _build_fetcher()
    fetcher._pace_api_requests = AsyncMock(return_value=True)
    fetcher._fetch_public_relay_child_comments_for_status_only = AsyncMock(
        return_value={"attempted": False, "reason": "test_child_lane_skipped"}
    )
    html = '"LSD",[],{"token":"lsd-token"}'
    fake_client = MagicMock()
    fake_client.get = AsyncMock(
        return_value=httpx.Response(
            200,
            text=html,
            request=httpx.Request("GET", "https://www.instagram.com/p/DU_oEbbgZfJ/"),
        )
    )
    fake_client.post = AsyncMock(
        return_value=httpx.Response(
            429,
            text="too many requests",
            request=httpx.Request("POST", "https://www.instagram.com/graphql/query/"),
        )
    )

    with patch(
        "trr_backend.socials.instagram.comments_scrapling.fetcher.build_comments_async_client",
        return_value=_AsyncClientContext(fake_client),
    ):
        comments, metadata = asyncio.run(
            fetcher._fetch_public_relay_coauthor_comments_for_status_only(
                "DU_oEbbgZfJ",
                "https://www.instagram.com/p/DU_oEbbgZfJ/",
                media_id="123",
                expected_comment_count=1,
                max_comments=0,
                allow_authenticated=False,
            )
        )

    assert comments == []
    assert metadata["reason"] == "http_429"
    assert metadata["attempts"][0]["block_reason"] == "http_429"
    assert fake_client.post.await_count == 1


def test_single_session_load_all_memory_guardrail_marks_retryable(monkeypatch) -> None:
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_SINGLE_SESSION_MAX_IN_MEMORY_ROWS", "1")
    fetcher = _build_fetcher()
    fetcher._fetch_json_response = AsyncMock(
        return_value={
            "payload": {
                "comments": [{"id": "1"}, {"id": "2"}],
                "has_more_comments": False,
            },
            "failed": False,
            "auth_failed": False,
            "reason": None,
            "retryable": False,
        }
    )
    fetcher._parser.parse_comment = MagicMock(
        side_effect=[_comment("api-1", text="first"), _comment("api-2", text="second")]
    )
    fetcher._fetch_rendered_single_session_load_all = AsyncMock(return_value=([], {}))

    result = asyncio.run(
        fetcher.fetch_comments_for_shortcode(
            "ABC123",
            max_comments=0,
            fetch_replies=False,
            expected_comment_count=3,
            load_strategy="single_session_load_all",
        )
    )

    assert result.fetch_failed is True
    assert result.retryable is True
    assert result.fetch_reason == "memory_guardrail_reached"
    # Bug #10b: the guardrail now trips at exactly max_in_memory_rows (inclusive
    # cap) rather than max+1, so only the first row is retained before stopping.
    assert [comment.comment_id for comment in result.comments] == ["api-1"]
    assert result.diagnostic_metadata["strategy_decision"]["selected_strategy"] == "single_session_load_all"
    assert result.diagnostic_metadata["api_pages_loaded"] == 1
    assert result.diagnostic_metadata["api_rows_seen"] == 1
    assert result.diagnostic_metadata["memory_guardrail"] == {
        "max_in_memory_rows": 1,
        "current_rows": 1,
        "reached": True,
        "stop_reason": "memory_guardrail_reached",
    }
    fetcher._fetch_rendered_single_session_load_all.assert_not_awaited()


def test_single_session_load_all_uses_rendered_hydration_only_after_api_gap(monkeypatch) -> None:
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_SINGLE_SESSION_MAX_IN_MEMORY_ROWS", "10")
    fetcher = _build_fetcher()
    fetcher._fetch_json_response = AsyncMock(
        return_value={
            "payload": {
                "comments": [{"id": "1"}],
                "has_more_comments": False,
            },
            "failed": False,
            "auth_failed": False,
            "reason": None,
            "retryable": False,
        }
    )
    fetcher._parser.parse_comment = MagicMock(return_value=_comment("api-1", text="api"))
    fetcher._fetch_rendered_comments_after_revealing_hidden = AsyncMock(return_value=[])
    fetcher._fetch_rendered_single_session_load_all = AsyncMock(
        return_value=(
            [_comment("api-1", text="duplicate"), _comment("rendered-1", text="rendered")],
            {"reason": "rendered_comments_found", "rendered_rows_seen": 2},
        )
    )

    result = asyncio.run(
        fetcher.fetch_comments_for_shortcode(
            "ABC123",
            max_comments=0,
            fetch_replies=False,
            expected_comment_count=2,
            load_strategy="single_session_load_all",
        )
    )

    assert [comment.comment_id for comment in result.comments] == ["api-1", "rendered-1"]
    assert result.fetch_failed is False
    assert result.retryable is False
    assert result.fetch_reason == "single_session_rendered_hydration_recovered"
    assert result.diagnostic_metadata["fallback_trigger"] == "api_complete_expected_gap"
    assert result.diagnostic_metadata["lane_order"] == ["instagram_comments_endpoint_cursor", "rendered_hydration"]
    assert result.diagnostic_metadata["api_rows_seen"] == 1
    assert result.diagnostic_metadata["rendered_load_attempts"] == 1
    assert result.diagnostic_metadata["rendered_rows_seen"] == 2
    assert result.diagnostic_metadata["rendered_merged_comments"] == 1
    assert result.diagnostic_metadata["merged_comments"] == 2
    fetcher._fetch_rendered_single_session_load_all.assert_awaited_once()


def test_public_only_runtime_metadata_does_not_claim_unused_browser(monkeypatch):
    from trr_backend.socials.instagram.comments_scrapling import fetcher as module

    monkeypatch.setattr(
        module, "build_stealthy_fetcher", lambda: (_ for _ in ()).throw(AssertionError("browser built"))
    )
    fetcher = InstagramCommentsScraplingFetcher(cookies=[], raw_cookies={}, browser_account_id=None, public_only=True)
    metadata = fetcher.runtime_metadata
    assert metadata["scrapling_fetcher_class"] is None
    assert metadata["browser_used"] is False and metadata["http_client"] is None
    fetcher._parser.session.close()


def test_relay_runtime_metadata_reports_actual_http_client(monkeypatch):
    from trr_backend.socials.instagram.comments_scrapling import fetcher as module

    class _CurlCffiAsyncClient:
        cookies = httpx.Cookies()

        def __init__(self, **_kwargs):
            self.requests = []

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def get(self, url, **_kwargs):
            self.requests.append(url)
            return httpx.Response(403, request=httpx.Request("GET", url))

    fetcher = _build_fetcher()
    monkeypatch.setattr(module, "build_comments_async_client", _CurlCffiAsyncClient)
    monkeypatch.setattr(fetcher, "_pace_api_requests", AsyncMock(return_value=True))
    try:
        comments, metadata = asyncio.run(
            fetcher._fetch_public_relay_coauthor_comments_for_status_only(
                "AAAABC",
                "https://www.instagram.com/p/AAAABC/",
                allow_authenticated=False,
                use_proxy=False,
                max_comments=5,
                fetch_replies=False,
            )
        )
        assert comments == [] and metadata["reason"] == "graphql_relay_post_page_unavailable"
        assert fetcher.runtime_metadata["http_client"] == "curl_cffi"
        assert fetcher.runtime_metadata["transport"] == "curl_cffi"
        assert fetcher.runtime_metadata["browser_used"] is False
        assert fetcher.runtime_metadata["scrapling_fetcher_class"] is None
    finally:
        asyncio.run(fetcher.aclose())
        fetcher._parser.session.close()


@pytest.mark.parametrize("missing_replies", [False, True])
@pytest.mark.parametrize("terminal_reason", ["graphql_relay_target_reached", "http_429"])
def test_public_relay_request_limit_is_partial_without_automatic_retry(missing_replies, terminal_reason):
    fetcher = InstagramCommentsScraplingFetcher(cookies=[], raw_cookies={}, browser_account_id=None, public_only=True)
    comments = [_comment(str(index), reply_count=2 if index == 0 and missing_replies else 0) for index in range(5)]
    fetcher._fetch_public_relay_coauthor_comments_for_status_only = AsyncMock(
        return_value=(comments, {"reason": terminal_reason, "pages": [{"has_next_page": True}]})
    )
    try:
        result = asyncio.run(
            fetcher.fetch_comments_for_shortcode(
                "AAAABC", max_comments=5, fetch_replies=False, expected_comment_count=50, load_strategy="public_relay"
            )
        )
        metadata = result.diagnostic_metadata["public_comments"]
        assert metadata["classification"] == "public_partial"
        assert metadata["request_limit_reached"] is True and metadata["source_exhausted"] is False
        assert result.fetch_failed is (terminal_reason == "http_429")
        assert result.retryable is False
        assert result.fetch_reason == terminal_reason
        assert metadata["missing_reply_count"] == (2 if missing_replies else 0)
    finally:
        asyncio.run(fetcher.aclose())
        fetcher._parser.session.close()


def _relay_budget_node(identity, *, reply_count=0, previews=()):
    node = {
        "pk": identity,
        "text": f"comment {identity}",
        "created_at": 1772766459,
        "child_comment_count": reply_count,
        "user": {"pk": f"user-{identity}", "username": f"user{identity}"},
    }
    if previews:
        node["edge_threaded_comments"] = {
            "count": reply_count,
            "edges": [{"node": _relay_budget_node(child)} for child in previews],
            "page_info": {"has_next_page": True, "end_cursor": "inline-next"},
        }
    return node


def _relay_budget_page(nodes, *, child=False, more=True, cursor="next"):
    key = (
        "xdt_api__v1__media__media_id__comments__parent_comment_id__child_comments__connection"
        if child
        else "xdt_api__v1__media__media_id__comments__connection"
    )
    return httpx.Response(
        200,
        json={
            "data": {
                key: {
                    "edges": [{"node": node} for node in nodes],
                    "page_info": {"has_next_page": more, "end_cursor": cursor if more else None},
                }
            }
        },
    )


def _run_public_budget_sample(monkeypatch, *, cap, fetch_replies, parent_pages, child_pages):
    from trr_backend.socials.instagram.comments_scrapling import fetcher as module

    calls = []
    parent_pages, child_pages = list(parent_pages), list(child_pages)
    client = MagicMock()
    client.cookies = httpx.Cookies()
    client.get = AsyncMock(
        return_value=httpx.Response(
            200,
            text='"LSD",[],{"token":"fixture"}',
            request=httpx.Request("GET", "https://www.instagram.com/p/AAAABC/"),
        )
    )

    async def post(_url, *, data, **_kwargs):
        variables = json.loads(data["variables"])
        child = "parent_comment_id" in variables
        calls.append(("child" if child else "parent", variables))
        pages = child_pages if child else parent_pages
        assert pages, "unexpected source request after the sample budget or block"
        response = pages.pop(0)
        response.request = httpx.Request("POST", _url)
        return response

    client.post = AsyncMock(side_effect=post)
    monkeypatch.setattr(module, "build_comments_async_client", lambda **_kw: _AsyncClientContext(client))
    monkeypatch.setattr(module, "_post_comments_graphql_doc_ids", lambda *_: ["parent-doc"])
    monkeypatch.setattr(module, "_post_child_comments_graphql_doc_attempts", lambda: [("child-query", "child-doc")])
    fetcher = _build_fetcher()
    fetcher._pace_api_requests = AsyncMock(return_value=True)
    try:
        result = asyncio.run(
            fetcher.fetch_comments_for_shortcode(
                "AAAABC",
                max_comments=cap,
                fetch_replies=fetch_replies,
                expected_comment_count=1000,
                load_strategy="public_relay",
            )
        )
        return result, calls
    finally:
        asyncio.run(fetcher.aclose())
        fetcher._parser.session.close()


@pytest.mark.parametrize("cap", [1, 2, 100])
@pytest.mark.parametrize("fetch_replies", [False, True])
def test_public_sample_reserves_reply_room_within_total_cap(monkeypatch, cap, fetch_replies):
    parents = [_relay_budget_node(str(index), reply_count=1000 if index == cap - 1 else 0) for index in range(cap)]
    result, calls = _run_public_budget_sample(
        monkeypatch,
        cap=cap,
        fetch_replies=fetch_replies,
        parent_pages=[_relay_budget_page(parents)],
        child_pages=[_relay_budget_page([_relay_budget_node(f"reply-{index}") for index in range(50)], child=True)],
    )
    assert flattened_comment_count(result.comments) == cap
    expected_replies = 1 if fetch_replies and cap > 1 else 0
    assert child_reply_count(result.comments) == expected_replies
    assert [kind for kind, _ in calls] == (["parent", "child"] if expected_replies else ["parent"])
    if expected_replies:
        assert calls[-1][1]["first"] == 1
        assert result.diagnostic_metadata["relay_comments"]["pages"][0]["reply_slots_reserved"] == 1
    assert not result.fetch_failed and not result.retryable


def test_public_sample_hydrates_between_parent_pages_and_counts_inline_previews(monkeypatch):
    result, calls = _run_public_budget_sample(
        monkeypatch,
        cap=5,
        fetch_replies=True,
        parent_pages=[
            _relay_budget_page([_relay_budget_node("p1", reply_count=3, previews=["preview"])]),
            _relay_budget_page([_relay_budget_node("p2")]),
        ],
        child_pages=[_relay_budget_page([_relay_budget_node("r1"), _relay_budget_node("r2")], child=True, more=False)],
    )
    assert [kind for kind, _ in calls] == ["parent", "child", "parent"]
    assert flattened_comment_count(result.comments) == 5
    assert parent_comment_count(result.comments) == 2 and child_reply_count(result.comments) == 3


def test_public_sample_without_reply_gaps_continues_parent_pages(monkeypatch):
    result, calls = _run_public_budget_sample(
        monkeypatch,
        cap=3,
        fetch_replies=True,
        parent_pages=[
            _relay_budget_page([_relay_budget_node("p1")]),
            _relay_budget_page([_relay_budget_node("p2"), _relay_budget_node("p3")]),
        ],
        child_pages=[],
    )
    assert [kind for kind, _ in calls] == ["parent", "parent"]
    assert flattened_comment_count(result.comments) == 3 and child_reply_count(result.comments) == 0


def test_public_sample_inline_reply_tree_cannot_overflow_total_cap(monkeypatch):
    result, calls = _run_public_budget_sample(
        monkeypatch,
        cap=2,
        fetch_replies=False,
        parent_pages=[
            _relay_budget_page(
                [
                    _relay_budget_node("p1", reply_count=4, previews=["r1", "r2", "r3", "r4"]),
                    _relay_budget_node("p2"),
                ]
            )
        ],
        child_pages=[],
    )
    assert [kind for kind, _ in calls] == ["parent"]
    assert flattened_comment_count(result.comments) == 2


def test_public_child_budget_deduplicates_existing_ids_before_count_and_stops_pages(monkeypatch):
    result, calls = _run_public_budget_sample(
        monkeypatch,
        cap=5,
        fetch_replies=True,
        parent_pages=[_relay_budget_page([_relay_budget_node("p1", reply_count=100, previews=["preview"])])],
        child_pages=[
            _relay_budget_page(
                [_relay_budget_node("preview"), _relay_budget_node("r1"), _relay_budget_node("r1")], child=True
            ),
            _relay_budget_page(
                [
                    _relay_budget_node("r1"),
                    _relay_budget_node("r2"),
                    _relay_budget_node("r3"),
                    _relay_budget_node("overflow"),
                ],
                child=True,
            ),
        ],
    )
    assert [kind for kind, _ in calls] == ["parent", "child", "child"]
    assert [variables["first"] for kind, variables in calls if kind == "child"] == [3, 2]
    assert flattened_comment_count(result.comments) == 5
    assert [reply.comment_id for reply in result.comments[0].replies] == ["preview", "r1", "r2", "r3"]


def test_public_child_block_after_partial_replies_is_not_target_success(monkeypatch):
    result, calls = _run_public_budget_sample(
        monkeypatch,
        cap=5,
        fetch_replies=True,
        parent_pages=[_relay_budget_page([_relay_budget_node("p1", reply_count=10)])],
        child_pages=[
            _relay_budget_page([_relay_budget_node("r1")], child=True),
            httpx.Response(429, text="rate limited"),
        ],
    )
    assert [kind for kind, _ in calls] == ["parent", "child", "child"]
    assert result.fetch_failed and not result.retryable
    assert result.fetch_reason == "http_429" and result.public_block_signal != "none"
    assert result.diagnostic_metadata["public_comments"]["classification"] == "public_partial"
    assert child_reply_count(result.comments) == 1


def test_public_sample_reserves_reply_slot_when_gap_parent_arrives_on_later_page(monkeypatch):
    result, calls = _run_public_budget_sample(
        monkeypatch,
        cap=3,
        fetch_replies=True,
        parent_pages=[
            _relay_budget_page([_relay_budget_node("p1"), _relay_budget_node("p2")]),
            _relay_budget_page([_relay_budget_node("p3", reply_count=10)]),
        ],
        child_pages=[_relay_budget_page([_relay_budget_node("r1")], child=True)],
    )
    assert [kind for kind, _ in calls] == ["parent", "parent", "child"]
    assert [parent.comment_id for parent in result.comments] == ["p1", "p3"]
    assert [reply.comment_id for reply in result.comments[-1].replies] == ["r1"]
    assert flattened_comment_count(result.comments) == 3
    assert parent_comment_count(result.comments) == 2 and child_reply_count(result.comments) == 1
    assert result.diagnostic_metadata["relay_comments"]["pages"][-1]["reply_slots_reserved"] == 1


def test_public_relay_child_hydration_probes_unknown_without_zero_probe_allowance(monkeypatch) -> None:
    monkeypatch.delenv("SOCIAL_INSTAGRAM_PUBLIC_COMMENTS_ZERO_REPLY_PROBE_LIMIT", raising=False)
    fetcher = _build_fetcher()
    fetcher._pace_api_requests = AsyncMock(return_value=True)
    parent = _comment("unknown-parent")
    parent.reply_count = parent.child_comment_count = None
    public_client = MagicMock()
    public_client.post = AsyncMock(
        return_value=httpx.Response(
            200,
            json={
                "data": {
                    "xdt_api__v1__media__media_id__comments__parent_comment_id__child_comments__connection": {
                        "edges": [],
                        "page_info": {"has_next_page": False, "end_cursor": None},
                    }
                },
            },
        )
    )
    metadata = asyncio.run(
        fetcher._fetch_public_relay_child_comments_for_status_only(
            public_client=public_client,
            shortcode="fixture",
            post_url="https://www.instagram.com/p/fixture/",
            media_id="123",
            comments=[parent],
            graphql_headers={},
            common_body={},
            target_count=None,
            max_comments=0,
        )
    )
    assert public_client.post.await_count == 1
    assert metadata["zero_count_parent_probes"] == 0
    assert parent.reply_count is None and parent.child_comment_count is None


def _run_default_child_contract(monkeypatch, *, strict, resume=None, rejected=False):
    for key in ("INSTAGRAM_POST_CHILD_COMMENTS_GRAPHQL_DOC_IDS", "INSTAGRAM_POST_CHILD_COMMENTS_GRAPHQL_FRIENDLY_NAME"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_COAUTHOR_GRAPHQL_CHILD_PAGE_SIZE", "12")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_PUBLIC_COMMENTS_ZERO_REPLY_PROBE_LIMIT", "0")
    fetcher = _build_fetcher()
    fetcher._pace_api_requests = AsyncMock(return_value=True)
    parent = _comment("18034559608366960")
    parent.reply_count = parent.child_comment_count = None
    client = MagicMock()
    client.cookies = httpx.Cookies()
    payloads = []
    fetcher._source_payload_observer = lambda kind, payload: payloads.append((kind, payload))
    node = _relay_budget_node("17928451544257455")
    node["parent_comment_id"] = parent.comment_id
    response = (
        httpx.Response(
            200,
            json={
                "errors": [
                    {"code": 1675004, "message": "Rate limit exceeded", "severity": "CRITICAL"},
                ]
            },
        )
        if rejected
        else _relay_budget_page([node], child=True, more=False)
    )
    response.request = httpx.Request("POST", "https://www.instagram.com/graphql/query/")
    client.post = AsyncMock(return_value=response)
    try:
        metadata = asyncio.run(
            fetcher._fetch_public_relay_child_comments_for_status_only(
                public_client=client,
                shortcode="CfhVcINpHiY",
                post_url="https://www.instagram.com/p/CfhVcINpHiY/",
                media_id="2873672329179986072",
                comments=[parent],
                graphql_headers={},
                common_body={},
                target_count=None,
                max_comments=0,
                relay_is_logged_in=False,
                strict_native_exhaustion=strict,
                durable_resume=resume,
            )
        )
        return metadata, parent, client.post.await_args_list, payloads
    finally:
        asyncio.run(fetcher.aclose())
        fetcher._parser.session.close()


@pytest.mark.parametrize("strict", [False, True])
def test_default_child_contract_prefers_proven_secondary_and_merges_unknown_count_reply(monkeypatch, strict):
    metadata, parent, calls, payloads = _run_default_child_contract(monkeypatch, strict=strict)
    assert len(calls) == 1
    request = calls[0]
    assert request.args == ("https://www.instagram.com/graphql/query/",)
    assert request.kwargs["data"]["doc_id"] == "36239935742272683"
    assert request.kwargs["data"]["fb_api_req_friendly_name"] == "PolarisPostCommentsChildrenPaginationtQuery"
    assert request.kwargs["headers"]["x-fb-friendly-name"] == "PolarisPostCommentsChildrenPaginationtQuery"
    assert json.loads(request.kwargs["data"]["variables"]) == {
        "__relay_internal__pv__PolarisIsLoggedInrelayprovider": False,
        "first": 12,
        "is_chronological": False,
        "media_id": "2873672329179986072",
        "parent_comment_id": "18034559608366960",
    }
    assert metadata["merged_replies"] == 1 and metadata["pages"][0]["has_next_page"] is False
    assert parent.reply_count is None and parent.child_comment_count is None
    assert [reply.comment_id for reply in parent.replies] == ["17928451544257455"]
    assert parent.replies[0].parent_comment_id == parent.comment_id
    if strict:
        assert metadata["native_complete"] is True
        assert metadata["completed_parent_ids"] == [parent.comment_id]
        assert payloads[0][1]["cursor_payload"]["relay_doc_id"] == "36239935742272683"


def test_pinned_child_resume_keeps_primary_contract_and_exact_cursor(monkeypatch):
    resume = {
        "18034559608366960": {
            "pending": True,
            "relay_doc_id": "34884685271179117",
            "relay_friendly_name": "PolarisPostChildCommentsQuery",
            "page_size": 17,
            "after": "pinned-child-tail",
            "seen_cursors": ["pinned-child-tail"],
        }
    }
    metadata, parent, calls, payloads = _run_default_child_contract(monkeypatch, strict=True, resume=resume)
    assert len(calls) == 1
    request = calls[0].kwargs
    assert request["data"]["doc_id"] == "34884685271179117"
    assert request["data"]["fb_api_req_friendly_name"] == "PolarisPostChildCommentsQuery"
    assert request["headers"]["x-fb-friendly-name"] == "PolarisPostChildCommentsQuery"
    variables = json.loads(request["data"]["variables"])
    assert variables["after"] == "pinned-child-tail" and variables["first"] == 17
    assert variables["parent_comment_id"] == parent.comment_id
    assert metadata["native_complete"] is True
    assert payloads[0][1]["cursor_payload"]["relay_doc_id"] == "34884685271179117"


@pytest.mark.parametrize("strict", [False, True])
def test_default_child_native_rejection_tries_one_alternate_without_identity_block(monkeypatch, strict):
    metadata, parent, calls, _ = _run_default_child_contract(monkeypatch, strict=strict, rejected=True)
    assert len(calls) == 2
    assert [call.kwargs["data"]["doc_id"] for call in calls] == ["36239935742272683", "34884685271179117"]
    assert metadata["reason"] == "reply_access_rejected"
    assert "block_reason" not in metadata
    assert [lane["native_code"] for lane in metadata["rejected_lanes"]] == ["1675004", "1675004"]
    assert parent.replies == []
    if strict:
        assert metadata.get("native_complete") is not True


def test_pinned_rejected_primary_falls_through_once_to_proven_alternate_root(monkeypatch):
    fetcher = _build_fetcher()
    fetcher._pace_api_requests = AsyncMock(return_value=True)
    parent = _comment("parent")
    parent.reply_count = parent.child_comment_count = None
    client = MagicMock()
    client.cookies = httpx.Cookies()
    request = httpx.Request("POST", "https://www.instagram.com/graphql/query/")
    client.post = AsyncMock(
        side_effect=[
            httpx.Response(
                200, json={"errors": [{"code": 1675004, "message": "Rate limit exceeded"}]}, request=request
            ),
            _relay_budget_page([_relay_budget_node("reply")], child=True, more=False),
        ]
    )
    retained = []
    fetcher._source_payload_observer = lambda *args: retained.append(args)
    try:
        metadata = asyncio.run(
            fetcher._fetch_public_relay_child_comments_for_status_only(
                public_client=client,
                shortcode="fixture",
                post_url="https://www.instagram.com/p/fixture/",
                media_id="123",
                comments=[parent],
                graphql_headers={},
                common_body={},
                target_count=None,
                max_comments=0,
                strict_native_exhaustion=True,
                durable_resume={
                    "parent": {
                        "pending": True,
                        "relay_doc_id": "34884685271179117",
                        "relay_friendly_name": "PolarisPostChildCommentsQuery",
                        "page_size": 12,
                        "after": "old-tail",
                        "seen_cursors": ["old-tail"],
                    }
                },
            )
        )
        assert [call.kwargs["data"]["doc_id"] for call in client.post.await_args_list] == [
            "34884685271179117",
            "36239935742272683",
        ]
        variables = [json.loads(call.kwargs["data"]["variables"]) for call in client.post.await_args_list]
        assert variables[0]["after"] == "old-tail" and "after" not in variables[1]
        assert metadata["native_complete"] is True and metadata["completed_parent_ids"] == ["parent"]
        assert metadata["rejected_lanes"] == [
            {"parent_id": "parent", "doc_id": "34884685271179117", "native_code": "1675004"}
        ]
        assert [item.comment_id for item in parent.replies] == ["reply"]
    finally:
        asyncio.run(fetcher.aclose())
        fetcher._parser.session.close()
