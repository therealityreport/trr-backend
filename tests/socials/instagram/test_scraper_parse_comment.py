"""Phase 1.2 + Phase 2 regression tests for ``InstagramScraper._parse_comment``.

Locks down:
- Phase 1.2: nested preview replies no longer overwrite ``reply_count``;
  observed nested-reply count lives on its own field so the fetcher's reply
  guard (``reply_count > reply_count_observed``) keeps firing.
- Phase 2: the parser builds ``comment_url``, ``created_at_iso``, and reads
  the Apify-source owner-metadata fields (``fbid_v2``, ``is_mentionable``,
  ``is_private``, ``latest_reel_media``, ``profile_pic_id``).
"""

from __future__ import annotations

import json
from pathlib import Path

from trr_backend.socials.instagram.scraper import InstagramScraper

_FIXTURE_DIR = Path(__file__).parents[2] / "fixtures" / "instagram" / "scrapling"


def _make_scraper() -> InstagramScraper:
    return InstagramScraper(cookies={"sessionid": "test"}, browser_account_id="bravotv")


def _fixture_json(name: str) -> dict:
    return json.loads((_FIXTURE_DIR / name).read_text(encoding="utf-8"))


def test_parse_comment_preserves_reported_reply_count_when_inline_previews_present():
    """Phase 1.2 / audit: previously the parser overwrote
    ``comment.reply_count`` with ``len(comment.replies)`` when IG didn't ship
    a count. That hid tail-reply gaps from the fetcher's reply guard.
    """
    scraper = _make_scraper()
    payload = {
        "pk": "comment-1",
        "text": "parent",
        "user": {"username": "alice", "pk": "u1"},
        "child_comment_count": 50,  # IG-reported
        "replies": [
            {"pk": "reply-a", "text": "first", "user": {"username": "bob", "pk": "u2"}},
            {"pk": "reply-b", "text": "second", "user": {"username": "carol", "pk": "u3"}},
        ],
    }

    comment = scraper._parse_comment(payload, shortcode="ABC123", post_url="https://www.instagram.com/p/ABC123/")

    # IG-reported reply_count is preserved for the fetcher's reply guard.
    assert comment.reply_count == 50
    # Observed nested replies are recorded separately.
    assert comment.reply_count_observed == 2
    assert len(comment.replies) == 2


def test_parse_comment_records_observed_count_when_no_reported_count():
    """When IG omits ``child_comment_count``, ``reply_count`` stays 0 (so the
    fetcher will pull the full reply tail) and the observed count is captured
    separately for the persistence layer.
    """
    scraper = _make_scraper()
    payload = {
        "pk": "comment-1",
        "text": "parent",
        "user": {"username": "alice", "pk": "u1"},
        # No child_comment_count, repliesCount, reply_count, replies_count.
        "replies": [
            {"pk": "reply-a", "text": "first", "user": {"username": "bob", "pk": "u2"}},
        ],
    }

    comment = scraper._parse_comment(payload, shortcode="ABC123", post_url="https://www.instagram.com/p/ABC123/")

    assert comment.reply_count == 0  # was wrongly forced to 1 by the old code
    assert comment.reply_count_observed == 1


def test_parse_comment_builds_comment_url_and_iso_timestamp():
    """Phase 2: ``comment_url`` and ``created_at_iso`` populate from shortcode
    + comment_id and an integer epoch timestamp respectively.
    """
    scraper = _make_scraper()
    payload = {
        "pk": "1234567890",
        "text": "hello",
        "user": {"username": "alice", "pk": "u1"},
        "created_at": 1_705_320_600,  # 2024-01-15T10:30:00Z
    }

    comment = scraper._parse_comment(payload, shortcode="ABC123", post_url="https://www.instagram.com/p/ABC123/")

    assert comment.comment_url == "https://www.instagram.com/p/ABC123/c/1234567890/"
    assert comment.created_at_iso is not None
    assert comment.created_at_iso.endswith("Z")
    assert "T" in comment.created_at_iso
    # Existing legacy fields stay populated for downstream consumers.
    assert comment.created_at == 1_705_320_600


def test_parse_comment_extracts_apify_source_owner_metadata():
    """Phase 2: the parser reads ``fbid_v2``, ``is_mentionable``,
    ``is_private``, ``latest_reel_media``, ``profile_pic_id`` from the owner
    or user payload variants IG ships.
    """
    scraper = _make_scraper()
    payload = {
        "pk": "comment-1",
        "text": "parent",
        "user": {
            "username": "alice",
            "pk": "u1",
            "fbid_v2": "fbid_987",
            "is_mentionable": True,
            "is_private": False,
            "latest_reel_media": 1_705_320_600,
            "profile_pic_id": "pic_abc",
        },
    }

    comment = scraper._parse_comment(payload, shortcode="ABC123", post_url="https://www.instagram.com/p/ABC123/")

    assert comment.owner_fbid_v2 == "fbid_987"
    assert comment.owner_is_mentionable is True
    assert comment.owner_is_private is False
    assert comment.owner_latest_reel_media == 1_705_320_600
    assert comment.owner_profile_pic_id == "pic_abc"


def test_parse_comment_owner_metadata_falls_back_to_camel_case():
    """IG sometimes ships these fields with camelCase keys; the fallback must
    cover both shapes."""
    scraper = _make_scraper()
    payload = {
        "pk": "comment-1",
        "text": "parent",
        "owner": {
            "username": "alice",
            "id": "u1",
            "fbidV2": "fbid_camel",
            "isMentionable": False,
            "isPrivate": True,
            "latestReelMedia": 999,
            "profilePicId": "pic_camel",
        },
    }

    comment = scraper._parse_comment(payload, shortcode="ABC123", post_url="https://www.instagram.com/p/ABC123/")

    assert comment.owner_fbid_v2 == "fbid_camel"
    assert comment.owner_is_mentionable is False
    assert comment.owner_is_private is True
    assert comment.owner_latest_reel_media == 999
    assert comment.owner_profile_pic_id == "pic_camel"


def test_instagram_comment_direct_construction_keeps_metadata_defaults():
    from trr_backend.socials.instagram.scraper import InstagramComment

    comment = InstagramComment(
        comment_id="comment-1",
        text="hello",
        username="alice",
        user_id="u1",
        created_at=0,
        date_time="",
        likes=0,
        is_reply=False,
        parent_comment_id=None,
        reply_count=0,
    )

    assert comment.is_covered is False
    assert comment.is_ranked is False
    assert comment.phase is None
    assert comment.status == "Active"
    assert comment.meta_ai_comment_type == "NONE"
    assert comment.child_comment_count == 0
    assert comment.cursor_payload == {}


def test_parse_ranked_fixture_extracts_phase_metadata_and_cursor_context():
    scraper = _make_scraper()
    payload = _fixture_json("comments_ranked.json")
    row = payload["comments"][0]

    comment = scraper.parse_comment(
        row,
        shortcode="ABC123",
        post_url="https://www.instagram.com/p/ABC123/",
        phase="ranked",
        cursor_param="cached_comments_cursor",
        cursor_min_id=payload["cached_comments_cursor"],
        cursor_payload={
            "cached_comments_cursor": payload["cached_comments_cursor"],
            "tao_cursor": payload["tao_cursor"],
        },
        comment_filter_param=payload["comment_filter_param"],
    )

    assert comment.phase == "ranked"
    assert comment.is_ranked is True
    assert comment.comment_index == 0
    assert comment.is_covered is False
    assert comment.did_report_as_spam is False
    assert comment.status == "Active"
    assert comment.is_edited is True
    assert comment.is_pinned is True
    assert comment.meta_ai_comment_type == "NONE"
    assert comment.child_comment_count == 1
    assert comment.reply_count == 1
    assert comment.liked_by_media_coauthors is True
    assert comment.cursor_param == "cached_comments_cursor"
    assert comment.cursor_min_id == "ranked-cursor-redacted"
    assert comment.cursor_payload["tao_cursor"] == "tao-cursor-redacted"
    assert comment.comment_filter_param == "ranked"
    assert comment.replies[0].phase == "child"


def test_parse_headload_fixture_marks_headload_without_ranked_default():
    scraper = _make_scraper()
    payload = _fixture_json("comments_headload.json")

    comment = scraper.parse_comment(
        payload["comments"][0],
        shortcode="ABC123",
        post_url="https://www.instagram.com/p/ABC123/",
        phase="headload",
        cursor_param="min_id",
        cursor_min_id=payload["next_min_id"],
        cursor_payload={"next_min_id": payload["next_min_id"]},
        comment_filter_param=payload["comment_filter_param"],
    )

    assert comment.phase == "headload"
    assert comment.is_ranked is False
    assert comment.cursor_param == "min_id"
    assert comment.cursor_payload == {"next_min_id": "headload-min-cursor-redacted"}
    assert comment.comment_filter_param == "headload"


def test_parse_covered_offensive_fixture_allows_absent_visible_text():
    scraper = _make_scraper()
    payload = _fixture_json("comments_covered_offensive.json")

    comment = scraper.parse_comment(
        payload["comments"][0],
        shortcode="ABC123",
        post_url="https://www.instagram.com/p/ABC123/",
        phase="headload",
        comment_filter_param=payload["comment_filter_param"],
    )

    assert comment.comment_id == "covered-parent-1"
    assert comment.text == ""
    assert comment.is_covered is True
    assert comment.did_report_as_spam is True
    assert comment.status == "Covered"
    assert comment.comment_filter_param == "offensive"
