"""Regression tests for comments Scrapling queue retry state."""

from __future__ import annotations

import json
from typing import Any

from trr_backend.socials.instagram.comments_scrapling import job_runner as jr
from trr_backend.socials.instagram.comments_scrapling.fetcher import InstagramCommentsFetchResult
from trr_backend.socials.instagram.scraper import InstagramComment


def test_job_attempt_state_floors_attempt_and_max_attempts_to_at_least_one():
    attempt_count, max_attempts = jr._job_attempt_state({})
    assert attempt_count == 1
    assert max_attempts == 12


def test_job_attempt_state_respects_explicit_exhausted_max_attempts():
    attempt_count, max_attempts = jr._job_attempt_state({"attempt_count": 1, "max_attempts": 1})
    assert attempt_count == 1
    assert max_attempts == 1


def test_job_attempt_state_preserves_explicit_max_when_above_threshold():
    attempt_count, max_attempts = jr._job_attempt_state({"attempt_count": 2, "max_attempts": 12})
    assert attempt_count == 2
    assert max_attempts == 12


def test_job_attempt_state_caps_attempt_count_at_one_when_invalid():
    attempt_count, max_attempts = jr._job_attempt_state({"attempt_count": "garbage", "max_attempts": None})
    assert attempt_count == 1
    assert max_attempts == 12


def test_comment_capture_metadata_prefers_fetch_result_diagnostics():
    result = InstagramCommentsFetchResult(
        diagnostic_metadata={
            "phase_counts": {"ranked": 2, "headload": 1},
            "cursor_diagnostics": {
                "cursor_param_counts": {"cached_comments_cursor": 1},
                "cursor_shape_counts": {"tao_cursor": 1},
            },
        },
        top_level_checkpoint={
            "target_shortcode": "ABC123",
            "stop_reason": "pagination_exhausted",
            "last_top_level_cursor_param": "cached_comments_cursor",
            "pages_seen": 3,
        },
    )

    metadata = jr._comment_capture_metadata_from_fetch_result(result)

    assert metadata["phase_counts"] == {"ranked": 2, "headload": 1}
    assert metadata["cursor_param_counts"]["cached_comments_cursor"] == 2
    assert metadata["cursor_shape_counts"] == {"tao_cursor": 1}
    assert metadata["sample"]["target_shortcode"] == "ABC123"


def test_comment_capture_metadata_derives_phase_counts_from_comment_tree():
    reply = InstagramComment(
        comment_id="reply-1",
        text="reply",
        username="viewer_reply",
        user_id="reply-user",
        created_at=1,
        date_time="2026-05-01 00:00:00",
        likes=0,
        is_reply=True,
        parent_comment_id="comment-1",
        reply_count=0,
    )
    parent = InstagramComment(
        comment_id="comment-1",
        text="parent",
        username="viewer",
        user_id="user-1",
        created_at=1,
        date_time="2026-05-01 00:00:00",
        likes=0,
        is_reply=False,
        parent_comment_id=None,
        reply_count=1,
        replies=[reply],
    )
    parent.phase = "ranked"  # type: ignore[attr-defined]
    parent.is_ranked = True  # type: ignore[attr-defined]

    metadata = jr._comment_capture_metadata_from_fetch_result(InstagramCommentsFetchResult(comments=[parent]))

    assert metadata["phase_counts"] == {"ranked": 1, "child": 1}


def test_post_comments_audit_writer_keeps_fb_crossposts_in_separate_bucket(monkeypatch):
    captured: dict[str, Any] = {}

    class _Cursor:
        def __enter__(self):
            return object()

        def __exit__(self, *_args: Any) -> None:
            return None

    def _fake_fetch_one_with_cursor(_cur: object, sql: str, params: list[Any]) -> dict[str, Any]:
        captured["sql"] = sql
        captured["params"] = params
        return {"id": "audit-1"}

    monkeypatch.setattr(jr, "_post_comments_audit_table_available", lambda _conn: True)
    monkeypatch.setattr(jr.pg, "db_cursor", lambda **_kwargs: _Cursor())
    monkeypatch.setattr(jr.pg, "fetch_one_with_cursor", _fake_fetch_one_with_cursor)

    parent = InstagramComment(
        comment_id="ig-1",
        text="parent",
        username="viewer",
        user_id="user-1",
        created_at=1,
        date_time="2026-05-01 00:00:00",
        likes=0,
        is_reply=False,
        parent_comment_id=None,
        reply_count=0,
        phase="ranked",
    )
    fb_crosspost = InstagramComment(
        comment_id="fb:1",
        text="fb",
        username="facebook",
        user_id="fb-user",
        created_at=1,
        date_time="2026-05-01 00:00:00",
        likes=0,
        is_reply=False,
        parent_comment_id=None,
        reply_count=0,
        phase="fb_crosspost",
        status="Deleted",
    )

    jr._insert_instagram_post_comments_audit(
        conn=object(),
        run_id="00000000-0000-0000-0000-000000000001",
        job_id="00000000-0000-0000-0000-000000000002",
        post_id="00000000-0000-0000-0000-000000000003",
        shortcode="ABC123",
        account_handle="bravotv",
        result=InstagramCommentsFetchResult(
            comments=[parent, fb_crosspost],
            reported_comment_count=10,
            fetch_reason="fb_crosspost_pagination_incomplete",
        ),
        capture_metadata={"phase_counts": {"ranked": 1, "fb_crosspost": 1}},
        fetched_parent_count=2,
        fetched_child_count=0,
        fetched_total_count=2,
        target_metadata={"fb_comment_count": 1},
    )

    assert "insert into social.instagram_post_comments_audit" in captured["sql"]
    params = captured["params"]
    assert params[6] == 1
    assert params[7] == 1
    assert params[8] == 1
    assert params[10] == 1
    assert params[12] == 1
    assert json.loads(params[14]) == {"fb_crosspost": 1, "ranked": 1}
    assert json.loads(params[18]) == {"Active": 1, "Deleted": 1}
