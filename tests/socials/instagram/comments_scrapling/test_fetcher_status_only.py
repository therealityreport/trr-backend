"""Status-only Instagram comments endpoint classification tests."""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import httpx

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


def test_public_relay_child_hydration_probes_zero_count_parent() -> None:
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
    assert metadata["parent_attempts"] == 1
    assert metadata["merged_replies"] == 1
    assert parent.reply_count == 1
    assert parent.replies[0].username == "champagne.roast"


def test_public_relay_child_hydration_caps_zero_count_parent_probes(monkeypatch) -> None:
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_COAUTHOR_GRAPHQL_CHILD_ZERO_COUNT_PROBE_LIMIT", "1")
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
    public_client.post.assert_awaited_once()


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
    assert fetcher._fetch_public_relay_child_comments_for_status_only.await_args.kwargs["relay_is_logged_in"] is True


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
    assert [comment.comment_id for comment in result.comments] == ["api-1", "api-2"]
    assert result.diagnostic_metadata["strategy_decision"]["selected_strategy"] == "single_session_load_all"
    assert result.diagnostic_metadata["api_pages_loaded"] == 1
    assert result.diagnostic_metadata["api_rows_seen"] == 2
    assert result.diagnostic_metadata["memory_guardrail"] == {
        "max_in_memory_rows": 1,
        "current_rows": 2,
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
    assert result.diagnostic_metadata["lane_order"] == ["cursor_api", "rendered_hydration"]
    assert result.diagnostic_metadata["api_rows_seen"] == 1
    assert result.diagnostic_metadata["rendered_load_attempts"] == 1
    assert result.diagnostic_metadata["rendered_rows_seen"] == 2
    assert result.diagnostic_metadata["rendered_merged_comments"] == 1
    assert result.diagnostic_metadata["merged_comments"] == 2
    fetcher._fetch_rendered_single_session_load_all.assert_awaited_once()
