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

from trr_backend.socials.instagram.scraper import InstagramScraper


def _make_scraper() -> InstagramScraper:
    return InstagramScraper(cookies={"sessionid": "test"}, browser_account_id="bravotv")


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
