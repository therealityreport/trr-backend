from __future__ import annotations

from trr_backend.socials.crawlee_runtime.request_keys import build_request_key


def test_build_request_key_uses_fixed_segment_order() -> None:
    key = build_request_key(
        platform="instagram",
        target="bravotv",
        post_id="abc123",
        cursor="c1",
        reply_cursor="r1",
        mode="posts_and_comments",
    )
    assert key == "instagram|bravotv|abc123|c1|r1|posts_and_comments"


def test_build_request_key_normalizes_empty_and_special_chars() -> None:
    key = build_request_key(
        platform="Twitter/X",
        target=" @Bravo TV ",
        post_id=None,
        cursor="cursor with spaces",
        reply_cursor="",
        mode="comments only",
    )
    assert key == "twitter_x|bravo_tv|_|cursor_with_spaces|_|comments_only"
