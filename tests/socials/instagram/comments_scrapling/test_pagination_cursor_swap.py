"""Phase A5 follow-up regression tests for repeated-cursor recovery.

When IG returns ``next_min_id`` and ``next_max_id`` in the same payload but
the loop's currently-followed cursor (e.g. ``min_id``) repeats from a prior
page, the fetcher must:

1. Swap to the alt cursor direction (``max_id``) and continue paginating
   instead of immediately declaring the post incomplete.
2. Only declare ``pagination_repeated_cursor`` after BOTH cursor directions
   have been exhausted on this post.
3. Mark the stop ``retryable=False`` once both directions are exhausted so
   the job-level retry loop doesn't endlessly re-spin on the same IG state.

These tests poke the helper functions directly (`_extract_top_level_page`
and `_extract_reply_page`) — exercising the live fetcher would require
extensive Patchright/httpx scaffolding.
"""

from __future__ import annotations

import json
from pathlib import Path

from trr_backend.socials.instagram.comments_scrapling import fetcher as comments_fetcher

_FIXTURE_DIR = Path(__file__).parents[3] / "fixtures" / "instagram" / "scrapling"


def _fixture_json(name: str) -> dict:
    return json.loads((_FIXTURE_DIR / name).read_text(encoding="utf-8"))


def _make_top_level_payload(
    *,
    has_more_comments: bool,
    has_more_headload_comments: bool,
    next_min_id: str | None = None,
    next_max_id: str | None = None,
) -> tuple[dict[str, object], dict[str, object]]:
    payload: dict[str, object] = {
        "comments": [],
        "has_more_comments": has_more_comments,
        "has_more_headload_comments": has_more_headload_comments,
    }
    if next_min_id is not None:
        payload["next_min_id"] = next_min_id
    if next_max_id is not None:
        payload["next_max_id"] = next_max_id
    return payload, {}


def test_extract_top_level_page_returns_alt_cursor_when_both_directions_present():
    """Phase A5 follow-up: when IG returns BOTH next_min_id and next_max_id,
    the alt-cursor info must be exposed so the caller can swap on repeat."""
    payload, response = _make_top_level_payload(
        has_more_comments=True,
        has_more_headload_comments=False,
        next_min_id="42",
        next_max_id="99",
    )
    rows, has_more, primary_cursor, primary_param, alt_cursor, alt_param = (
        comments_fetcher._extract_top_level_page(payload, response)
    )
    assert rows == []
    assert has_more is True
    # has_more_comments + next_max => primary is max_id; alt = min_id.
    assert primary_cursor == "99"
    assert primary_param == "max_id"
    assert alt_cursor == "42"
    assert alt_param == "min_id"


def test_extract_top_level_page_no_alt_when_only_one_cursor_present():
    """Edge: when IG only returns one direction, alt must be None."""
    payload, response = _make_top_level_payload(
        has_more_comments=False,
        has_more_headload_comments=True,
        next_min_id="42",
        next_max_id=None,
    )
    rows, has_more, primary_cursor, primary_param, alt_cursor, alt_param = (
        comments_fetcher._extract_top_level_page(payload, response)
    )
    assert has_more is True
    assert primary_cursor == "42"
    assert primary_param == "min_id"
    assert alt_cursor is None
    assert alt_param is None


def test_extract_top_level_page_handles_no_more_pages():
    """Edge: when IG signals no more pages, both primary and alt are None."""
    payload, response = _make_top_level_payload(
        has_more_comments=False,
        has_more_headload_comments=False,
    )
    rows, has_more, primary_cursor, primary_param, alt_cursor, alt_param = (
        comments_fetcher._extract_top_level_page(payload, response)
    )
    assert has_more is False
    assert primary_cursor is None
    assert primary_param is None
    assert alt_cursor is None
    assert alt_param is None


def test_extract_top_level_page_envelope_captures_explicit_cursor_shapes():
    payload = _fixture_json("comments_cursor_envelope.json")

    envelope = comments_fetcher._extract_top_level_page_envelope(payload, {})

    assert envelope.has_more is True
    assert envelope.primary_cursor == "cached-comments-cursor-redacted"
    assert envelope.primary_cursor_param == "cached_comments_cursor"
    assert envelope.phase_signal == "ranked"
    assert envelope.cursor_payload["next_min_id"] == "min-cursor-redacted"
    assert envelope.cursor_payload["next_max_id"] == "max-cursor-redacted"
    assert envelope.cursor_payload["cached_comments_cursor"] == "cached-comments-cursor-redacted"
    assert envelope.cursor_payload["bifilter_token"] == "bifilter-token-redacted"
    assert envelope.cursor_payload["tao_cursor"] == "tao-cursor-redacted"
    assert set(envelope.cursor_shape_names) == {
        "next_min_id",
        "next_max_id",
        "cached_comments_cursor",
        "bifilter_token",
        "tao_cursor",
    }


def test_extract_top_level_page_headload_prefers_next_min_cursor():
    payload = _fixture_json("comments_headload.json")

    envelope = comments_fetcher._extract_top_level_page_envelope(payload, {})

    assert envelope.has_more is True
    assert envelope.primary_cursor == "headload-min-cursor-redacted"
    assert envelope.primary_cursor_param == "min_id"
    assert envelope.phase_signal == "headload"
    assert envelope.comment_filter_param == "headload"


def test_extract_top_level_page_bifilter_only_has_no_phase_signal():
    payload = {
        "status": "ok",
        "comments": [],
        "bifilter_token": "bifilter-token-redacted",
    }

    envelope = comments_fetcher._extract_top_level_page_envelope(payload, {})

    assert envelope.has_more is True
    assert envelope.primary_cursor == "bifilter-token-redacted"
    assert envelope.primary_cursor_param == "bifilter_token"
    assert envelope.phase_signal is None


def test_fb_crosspost_fixture_converts_to_collision_safe_comment_row():
    payload = _fixture_json("comments_fb_crosspost.json")
    rows = comments_fetcher._extract_fb_crosspost_comment_rows(payload)

    assert len(rows) == 1
    comment = comments_fetcher._fb_crosspost_comment_to_instagram_comment(
        rows[0],
        shortcode="ABC123",
        post_url="https://www.instagram.com/p/ABC123/",
        cursor_payload={"source": "fixture"},
        comment_filter_param=None,
    )

    assert comment is not None
    assert comment.comment_id == "fb:fb-comment-raw-id-redacted"
    assert comment.phase == "fb_crosspost"
    assert comment.source_snapshot_type == "instagram_fb_crosspost_comments"
    assert comment.username == "Facebook Fixture Author"
    assert comment.cursor_payload["raw_fb_comment_id"] == "fb-comment-raw-id-redacted"
    assert comments_fetcher._instagram_comment_phase_counts([comment]) == {"fb_crosspost": 1}


def test_fb_crosspost_has_more_flag_is_detected_as_incomplete_lane():
    payload = {
        "status": "ok",
        "comments": [],
        "fb_comments": [],
        "has_more_headload_fb_comments": True,
    }

    assert comments_fetcher._payload_has_more_fb_crosspost_comments(payload, {}) is True


def _make_reply_payload(
    *,
    has_more_tail: bool,
    has_more_head: bool,
    next_min: str | None = None,
    next_max: str | None = None,
) -> tuple[dict[str, object], dict[str, object]]:
    payload: dict[str, object] = {
        "child_comments": [],
        "has_more_tail_child_comments": has_more_tail,
        "has_more_head_child_comments": has_more_head,
    }
    if next_min is not None:
        payload["next_min_child_cursor"] = next_min
    if next_max is not None:
        payload["next_max_child_cursor"] = next_max
    return payload, {}


def test_extract_reply_page_returns_alt_cursor_when_both_directions_present():
    """Phase A5 follow-up: replies should also expose alt cursor info."""
    payload, response = _make_reply_payload(
        has_more_tail=True,
        has_more_head=False,
        next_min="m1",
        next_max="m9",
    )
    rows, primary_cursor, primary_param, alt_cursor, alt_param = (
        comments_fetcher._extract_reply_page(payload, response)
    )
    assert rows == []
    assert primary_cursor == "m1"
    assert primary_param == "min_id"
    assert alt_cursor == "m9"
    assert alt_param == "max_id"


def test_extract_reply_page_no_alt_when_single_direction():
    """Edge: only next_max present -> primary is max_id, alt None."""
    payload, response = _make_reply_payload(
        has_more_tail=False,
        has_more_head=True,
        next_min=None,
        next_max="m9",
    )
    rows, primary_cursor, primary_param, alt_cursor, alt_param = (
        comments_fetcher._extract_reply_page(payload, response)
    )
    assert primary_cursor == "m9"
    assert primary_param == "max_id"
    assert alt_cursor is None
    assert alt_param is None
