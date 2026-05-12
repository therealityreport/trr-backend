from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest

from trr_backend.socials import social_season_analytics_impl as social_repo
from trr_backend.socials.instagram import persistence as instagram_persistence
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
        include_inserted_flag: bool = False,
    ) -> list[dict[str, Any]]:
        captured_batches.append([dict(item) for item in batch])
        return [
            {
                "id": f"row-{item['comment_id']}",
                "comment_id": item["comment_id"],
                "post_id": item["post_id"],
                "__trr_inserted": include_inserted_flag,
            }
            for item in batch
        ]

    fake_repo = SimpleNamespace(
        _column_exists=lambda _schema, _table, column: (
            column
            in {
                "media_urls",
                "hosted_media_urls",
                "media_mirror_status",
                "media_mirror_error",
                "parent_comment_external_id",
                "reply_depth",
                "source_snapshot_type",
            }
        ),
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
    assert stats["comments_upserted"] == 2
    assert stats["comments_inserted"] == 2
    assert stats["comments_refreshed"] == 0
    assert stats["comments_changed"] == 2
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


def test_load_persisted_reply_topology_counts_saved_reply_gaps(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = object()
    queries: list[str] = []
    fake_repo = SimpleNamespace(
        _column_exists=lambda _schema, _table, column, *, conn=None: (
            column in {"parent_comment_external_id", "child_comment_count"}
        )
    )

    def fake_fetch_one(sql: str, params: list[str], *, conn: object | None = None) -> dict[str, Any]:
        assert conn is not None
        assert params == ["post-1", "post-1"]
        queries.append(sql)
        return {
            "stored_parent_comments": 2,
            "stored_child_replies": 3,
            "expected_child_replies": 5,
            "stored_reply_gap_total": 2,
            "stored_reply_gap_parent_count": 1,
        }

    def fake_fetch_all(sql: str, params: list[str], *, conn: object | None = None) -> list[dict[str, Any]]:
        assert conn is not None
        assert params == ["post-1", "post-1"]
        queries.append(sql)
        return [
            {
                "comment_id": "parent-1",
                "expected_reply_count": 5,
                "saved_reply_count": 3,
                "missing_reply_count": 2,
            }
        ]

    monkeypatch.setattr(persistence.pg, "fetch_one", fake_fetch_one)
    monkeypatch.setattr(persistence.pg, "fetch_all", fake_fetch_all)

    topology = persistence._load_persisted_instagram_reply_topology(
        repo=fake_repo,
        post_id="post-1",
        conn=conn,
    )

    assert topology["stored_parent_comments"] == 2
    assert topology["stored_child_replies"] == 3
    assert topology["expected_child_replies"] == 5
    assert topology["stored_reply_gap_total"] == 2
    assert topology["stored_reply_gap_parent_count"] == 1
    assert topology["stored_reply_gap_samples"] == [
        {
            "comment_id": "parent-1",
            "expected_reply_count": 5,
            "saved_reply_count": 3,
            "missing_reply_count": 2,
        }
    ]
    assert any("parent.child_comment_count" in query for query in queries)
    assert any("reply.parent_comment_external_id" in query for query in queries)


def test_new_or_changed_comment_count_ignores_bookkeeping_fields() -> None:
    now = datetime(2026, 5, 1, tzinfo=UTC)
    later = datetime(2026, 5, 2, tzinfo=UTC)
    payloads = [
        {
            "post_id": "post-1",
            "comment_id": "same",
            "text": "same text",
            "likes": 1,
            "scraped_at": later,
            "last_seen_at": later,
            "last_seen_run_id": "run-new",
            "job_id": "job-new",
        },
        {
            "post_id": "post-1",
            "comment_id": "changed",
            "text": "updated text",
            "likes": 1,
            "scraped_at": later,
        },
        {
            "post_id": "post-1",
            "comment_id": "new",
            "text": "new text",
            "likes": 1,
        },
    ]
    baseline = {
        ("post-1", "same"): {
            "post_id": "post-1",
            "comment_id": "same",
            "text": "same text",
            "likes": 1,
            "scraped_at": now,
            "last_seen_at": now,
            "last_seen_run_id": "run-old",
            "job_id": "job-old",
        },
        ("post-1", "changed"): {
            "post_id": "post-1",
            "comment_id": "changed",
            "text": "old text",
            "likes": 1,
            "scraped_at": now,
        },
    }

    assert instagram_persistence._count_new_or_changed_instagram_comment_payloads(payloads, baseline) == 2


def test_no_season_persistence_writes_comment_api_metadata_when_enabled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_batches: list[list[dict[str, Any]]] = []
    now = datetime(2026, 5, 1, tzinfo=UTC)
    metadata_columns = {
        "parent_comment_external_id",
        "reply_depth",
        "source_snapshot_type",
        "is_covered",
        "is_ranked",
        "comment_index",
        "phase",
        "did_report_as_spam",
        "status",
        "is_edited",
        "is_pinned",
        "meta_ai_comment_type",
        "child_comment_count",
        "liked_by_media_coauthors",
        "cursor_min_id",
        "cursor_param",
        "cursor_payload",
        "comment_filter_param",
    }

    monkeypatch.delenv("SOCIAL_INSTAGRAM_COMMENT_METADATA_WRITES_ENABLED", raising=False)
    social_repo._instagram_comment_metadata_column_cache.clear()
    monkeypatch.setattr(
        social_repo,
        "_column_exists",
        lambda _schema, _table, column, **_kwargs: column in metadata_columns,
    )

    def _fake_upsert_many(
        _table: str,
        batch: list[dict[str, Any]],
        *,
        conflict_col: list[str],
        conn: object | None = None,
        include_inserted_flag: bool = False,
    ) -> list[dict[str, Any]]:
        captured_batches.append([dict(item) for item in batch])
        return [
            {
                "id": f"row-{item['comment_id']}",
                "comment_id": item["comment_id"],
                "post_id": item["post_id"],
                "__trr_inserted": include_inserted_flag,
            }
            for item in batch
        ]

    fake_repo = SimpleNamespace(
        _column_exists=lambda _schema, _table, column: column in metadata_columns,
        _comment_lifecycle_supported=lambda table: table == "instagram_comments",
        _flatten_instagram_comment_tree=social_repo._flatten_instagram_comment_tree,
        _now_utc=lambda: now,
        _parse_instagram_time=lambda value: datetime.fromtimestamp(int(value), tz=UTC),
        _apply_instagram_comment_queryable_columns=social_repo._apply_instagram_comment_queryable_columns,
        _instagram_comment_raw_data_for_write=social_repo._instagram_comment_raw_data_for_write,
        _dedupe_instagram_comment_payloads_for_upsert=social_repo._dedupe_instagram_comment_payloads_for_upsert,
        _preserve_existing_ranked_instagram_comment_values=social_repo._preserve_existing_ranked_instagram_comment_values,
        _load_instagram_comment_write_baseline=lambda _payloads, **_kwargs: {},
        _count_new_or_changed_instagram_comment_payloads=lambda payloads, _baseline: len(payloads),
        _pg_upsert_many=_fake_upsert_many,
    )
    reply = _comment("reply-1", is_reply=True, parent_comment_id="comment-1")
    comment = _comment("comment-1", replies=[reply])
    comment.is_ranked = True  # type: ignore[attr-defined]
    comment.phase = "ranked"  # type: ignore[attr-defined]
    comment.comment_index = 3  # type: ignore[attr-defined]
    comment.is_covered = True  # type: ignore[attr-defined]
    comment.cursor_min_id = "cursor-1"  # type: ignore[attr-defined]
    comment.cursor_param = "cached_comments_cursor"  # type: ignore[attr-defined]
    comment.cursor_payload = {"cached_comments_cursor": "cursor-1"}  # type: ignore[attr-defined]
    comment.comment_filter_param = "preview"  # type: ignore[attr-defined]

    written = persistence._persist_without_season_context(
        repo=fake_repo,
        post_id="post-1",
        account_handle="bravotv",
        comments=[comment],
        run_id="run-1",
        job_id="job-1",
        observed_comment_ids=set(),
        persist_stats={},
        enable_media_followups=True,
        conn=object(),
    )

    assert written == 2
    parent_payload = captured_batches[0][0]
    assert parent_payload["is_ranked"] is True
    assert parent_payload["phase"] == "ranked"
    assert parent_payload["comment_index"] == 3
    assert parent_payload["is_covered"] is True
    assert parent_payload["cursor_min_id"] == "cursor-1"
    assert parent_payload["cursor_param"] == "cached_comments_cursor"
    assert parent_payload["cursor_payload"] == {"cached_comments_cursor": "cursor-1"}
    assert parent_payload["comment_filter_param"] == "preview"
    assert captured_batches[1][0]["phase"] == "child"


def test_no_season_persistence_metadata_flag_disabled_keeps_legacy_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_batches: list[list[dict[str, Any]]] = []
    now = datetime(2026, 5, 1, tzinfo=UTC)
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENT_METADATA_WRITES_ENABLED", "0")
    social_repo._instagram_comment_metadata_column_cache.clear()

    def _fake_upsert_many(
        _table: str,
        batch: list[dict[str, Any]],
        *,
        conflict_col: list[str],
        conn: object | None = None,
        include_inserted_flag: bool = False,
    ) -> list[dict[str, Any]]:
        captured_batches.append([dict(item) for item in batch])
        return [
            {
                "id": f"row-{item['comment_id']}",
                "comment_id": item["comment_id"],
                "post_id": item["post_id"],
                "__trr_inserted": include_inserted_flag,
            }
            for item in batch
        ]

    fake_repo = SimpleNamespace(
        _column_exists=lambda _schema, _table, _column: True,
        _comment_lifecycle_supported=lambda table: table == "instagram_comments",
        _flatten_instagram_comment_tree=social_repo._flatten_instagram_comment_tree,
        _now_utc=lambda: now,
        _parse_instagram_time=lambda value: datetime.fromtimestamp(int(value), tz=UTC),
        _apply_instagram_comment_queryable_columns=social_repo._apply_instagram_comment_queryable_columns,
        _instagram_comment_raw_data_for_write=social_repo._instagram_comment_raw_data_for_write,
        _dedupe_instagram_comment_payloads_for_upsert=social_repo._dedupe_instagram_comment_payloads_for_upsert,
        _preserve_existing_ranked_instagram_comment_values=social_repo._preserve_existing_ranked_instagram_comment_values,
        _load_instagram_comment_write_baseline=lambda _payloads, **_kwargs: {},
        _count_new_or_changed_instagram_comment_payloads=lambda payloads, _baseline: len(payloads),
        _pg_upsert_many=_fake_upsert_many,
    )
    monkeypatch.setattr(social_repo, "_column_exists", lambda _schema, _table, _column, **_kwargs: True)
    comment = _comment("comment-1")
    comment.is_ranked = True  # type: ignore[attr-defined]
    comment.phase = "ranked"  # type: ignore[attr-defined]
    comment.comment_index = 7  # type: ignore[attr-defined]

    written = persistence._persist_without_season_context(
        repo=fake_repo,
        post_id="post-1",
        account_handle="bravotv",
        comments=[comment],
        run_id="run-1",
        job_id="job-1",
        observed_comment_ids=set(),
        persist_stats={},
        enable_media_followups=True,
        conn=object(),
    )

    assert written == 1
    payload = captured_batches[0][0]
    assert payload["parent_comment_external_id"] is None
    assert payload["reply_depth"] == 0
    assert payload["source_snapshot_type"] == "full_comments_scrape"
    assert "is_ranked" not in payload
    assert "phase" not in payload
    assert "comment_index" not in payload
    assert "is_ranked" not in payload["raw_data"]
    assert "phase" not in payload["raw_data"]
    assert "comment_index" not in payload["raw_data"]
    assert "cursor_payload" not in payload["raw_data"]


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
