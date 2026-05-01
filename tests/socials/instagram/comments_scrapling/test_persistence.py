from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest

from trr_backend.socials.instagram.comments_scrapling import persistence
from trr_backend.socials.instagram.scraper import InstagramComment


def _comment(
    comment_id: str,
    *,
    is_reply: bool = False,
    parent_comment_id: str | None = None,
    media_urls: list[str] | None = None,
    hosted_media_urls: list[str] | None = None,
    replies: list[InstagramComment] | None = None,
) -> InstagramComment:
    return InstagramComment(
        comment_id=comment_id,
        text=f"text {comment_id}",
        username=f"user_{comment_id}",
        user_id=f"user-id-{comment_id}",
        created_at=1_775_000_000,
        date_time="2026-04-01 10:00:00",
        likes=1,
        is_reply=is_reply,
        parent_comment_id=parent_comment_id,
        reply_count=len(replies or []),
        reply_depth=1 if is_reply else 0,
        media_urls=list(media_urls or []),
        hosted_media_urls=list(hosted_media_urls or []),
        replies=list(replies or []),
    )


def test_no_season_persistence_preserves_media_and_reply_metadata() -> None:
    captured_batches: list[list[dict[str, Any]]] = []
    now = datetime(2026, 5, 1, tzinfo=UTC)

    def _flatten(comment: InstagramComment, parent_external_id: str | None = None):
        result = [(comment, parent_external_id)]
        external_id = str(comment.comment_id or "").strip()
        for reply in comment.replies:
            result.extend(_flatten(reply, external_id or parent_external_id))
        return result

    def _fake_upsert_many(
        _table: str,
        batch: list[dict[str, Any]],
        *,
        conflict_col: list[str],
        conn: object | None = None,
    ) -> list[dict[str, Any]]:
        captured_batches.append([dict(item) for item in batch])
        return [
            {"id": f"row-{item['comment_id']}", "comment_id": item["comment_id"], "post_id": item["post_id"]}
            for item in batch
        ]

    fake_repo = SimpleNamespace(
        _column_exists=lambda _schema, _table, column: column
        in {
            "media_urls",
            "hosted_media_urls",
            "media_mirror_status",
            "media_mirror_error",
            "parent_comment_external_id",
            "reply_depth",
            "source_snapshot_type",
        },
        _comment_lifecycle_supported=lambda table: table == "instagram_comments",
        _flatten_instagram_comment_tree=_flatten,
        _new_comment_persist_stats=lambda: {},
        _now_utc=lambda: now,
        _parse_instagram_time=lambda value: datetime.fromtimestamp(int(value), tz=UTC),
        _apply_instagram_comment_queryable_columns=lambda payload, _comment, **kwargs: payload.update(
            {
                "parent_comment_external_id": kwargs["parent_external_id"],
                "reply_depth": kwargs["reply_depth"],
                "source_snapshot_type": kwargs.get("source_snapshot_type", "full_comments_scrape"),
            }
        ),
        _pg_upsert_many=_fake_upsert_many,
    )
    reply = _comment(
        "reply-1",
        is_reply=True,
        parent_comment_id="comment-1",
        media_urls=["https://images.example/reply-sticker.gif"],
        hosted_media_urls=["https://cdn.example/reply-sticker.gif"],
    )
    comment = _comment(
        "comment-1",
        media_urls=["https://images.example/comment-gift.gif"],
        hosted_media_urls=["https://cdn.example/comment-gift.gif"],
        replies=[reply],
    )
    stats: dict[str, int] = {}

    written = persistence._persist_without_season_context(
        repo=fake_repo,
        post_id="post-1",
        account_handle="bravotv",
        comments=[comment],
        run_id="run-1",
        job_id="job-1",
        observed_comment_ids=set(),
        persist_stats=stats,
        enable_media_followups=True,
        conn=object(),
    )

    assert written == 2
    assert captured_batches[0][0]["media_urls"] == ["https://images.example/comment-gift.gif"]
    assert captured_batches[0][0]["hosted_media_urls"] == ["https://cdn.example/comment-gift.gif"]
    assert captured_batches[0][0]["media_mirror_status"] == "deferred"
    assert captured_batches[0][0]["media_mirror_error"] == "season_context_missing"
    assert captured_batches[0][0]["source_snapshot_type"] == "full_comments_scrape"
    assert captured_batches[1][0]["media_urls"] == ["https://images.example/reply-sticker.gif"]
    assert captured_batches[1][0]["hosted_media_urls"] == ["https://cdn.example/reply-sticker.gif"]
    assert captured_batches[1][0]["media_mirror_status"] == "deferred"
    assert captured_batches[1][0]["media_mirror_error"] == "season_context_missing"
    assert captured_batches[1][0]["parent_comment_id"] == "row-comment-1"
    assert captured_batches[1][0]["parent_comment_external_id"] == "comment-1"
    assert captured_batches[1][0]["reply_depth"] == 1
    assert captured_batches[1][0]["source_snapshot_type"] == "full_comments_scrape"


def test_partial_persistence_does_not_mark_missing_or_reconcile(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_repo = SimpleNamespace(
        _new_comment_persist_stats=lambda: {},
        _mark_missing_comments_for_anchor=lambda **_kwargs: pytest.fail("partial scrape marked comments missing"),
        _reconcile_post_comment_count=lambda **_kwargs: pytest.fail("partial scrape reconciled comment count"),
        _count_stored_comments=lambda _post_ids, _platform, **_kwargs: {"post-1": 4},
    )

    monkeypatch.setattr(persistence, "_load_repo_helpers", lambda: fake_repo)
    monkeypatch.setattr(
        persistence,
        "_materialize_instagram_post_for_comments",
        lambda **_kwargs: {"id": "post-1", "season_id": None},
    )
    monkeypatch.setattr(persistence, "_persist_without_season_context", lambda **_kwargs: 0)

    result = persistence.persist_instagram_comments_for_post(
        account_handle="bravotv",
        shortcode="ABC123",
        comments=[],
        run_id="run-1",
        job_id="job-1",
        is_complete="false",  # type: ignore[arg-type]
        conn=object(),
    )

    assert result.comments_marked_missing == 0
    assert result.stored_total_comments == 4
