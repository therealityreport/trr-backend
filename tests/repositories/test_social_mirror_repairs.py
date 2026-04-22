from __future__ import annotations

from contextlib import nullcontext
from datetime import UTC, date, datetime
from types import SimpleNamespace

import pytest

from trr_backend.repositories import social_season_analytics as social_repo
from trr_backend.socials.control_plane import COMMENT_MEDIA_MIRROR_STAGE, IngestOptions, SeasonContext


def _season_context() -> SeasonContext:
    return SeasonContext(
        season_id="season-1",
        show_id="show-1",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 1, 1),
    )


def _ingest_options() -> IngestOptions:
    return IngestOptions(
        platforms={"youtube"},
        source_scope="bravo",
        sync_strategy="incremental",
        max_posts_per_target=1,
        max_comments_per_post=1,
        max_replies_per_post=0,
        fetch_replies=False,
        ingest_mode="posts_and_comments",
        date_start=None,
        date_end=None,
    )


def test_normalize_hosted_tagged_profile_pics_accepts_string_and_object_values() -> None:
    payload = social_repo._normalize_hosted_tagged_profile_pics(  # noqa: SLF001
        {
            "andy": "https://cdn.test/a.jpg",
            "cohen": {
                "hosted_url": "https://cdn.test/c.jpg",
                "sha256": "abc",
                "mirrored_at": "2026-04-21T00:00:00+00:00",
            },
        }
    )

    assert payload["andy"]["hosted_url"] == "https://cdn.test/a.jpg"
    assert payload["cohen"]["sha256"] == "abc"


def test_instagram_post_avatar_urls_reads_object_shaped_tagged_entries() -> None:
    urls = social_repo._instagram_post_avatar_urls(  # noqa: SLF001
        {
            "hosted_owner_profile_pic_url": "https://cdn.test/owner.jpg",
            "hosted_tagged_profile_pics": {
                "friend": {
                    "hosted_url": "https://cdn.test/friend.jpg",
                    "sha256": "abc123",
                    "mirrored_at": "2026-04-21T00:00:00+00:00",
                }
            },
        }
    )

    assert urls == {
        "https://cdn.test/owner.jpg",
        "https://cdn.test/friend.jpg",
    }


def test_get_post_comments_instagram_filters_missing_comments_and_includes_media_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}
    mirror_ts = datetime(2026, 2, 24, 16, 30, tzinfo=UTC)
    monkeypatch.setattr(
        social_repo.pg,
        "fetch_one",
        lambda _sql, _params: {
            "id": "ig-db-1",
            "source_id": "abc123",
            "author": "bravotv",
            "text": "hello #rhoslc",
            "likes": 5,
            "comments_count": 1,
            "views": 10,
            "media_type": "video",
            "source_media_urls": ["https://instagram.example/reel.mp4"],
            "hosted_media_urls": ["https://cdn.example/reel.mp4"],
            "source_thumbnail_url": "https://instagram.example/thumb.jpg",
            "hosted_thumbnail_url": "https://cdn.example/thumb.jpg",
            "thumbnail_url": "https://cdn.example/thumb.jpg",
            "post_format": "reel",
            "profile_tags": [],
            "collaborators": [],
            "tagged_users_detail": [],
            "collaborators_detail": [],
            "child_posts_data": [],
            "hashtags": ["RHOSLC"],
            "mentions": [],
            "duration_seconds": 21,
            "raw_data": {},
            "media_mirror_attempt_count": 3,
            "media_mirror_last_attempt_at": mirror_ts,
            "media_mirror_last_job_id": "job-123",
            "ts": datetime(2026, 2, 23, 12, 0, tzinfo=UTC),
        },
    )

    def _fake_fetch_all(sql: str, params: list[object]) -> list[dict[str, object]]:
        captured["sql"] = sql
        captured["params"] = params
        return [
            {
                "id": "ig-comment-1",
                "comment_id": "comment-1",
                "parent_comment_id": None,
                "author": "viewer",
                "text": "comment",
                "likes": 2,
                "is_reply": False,
                "reply_count": 0,
                "media_urls": ["https://instagram.example/comment.jpg"],
                "hosted_media_urls": ["https://cdn.example/comment.jpg"],
                "media_mirror_status": "mirrored",
                "created_at": datetime(2026, 2, 23, 13, 0, tzinfo=UTC),
            }
        ]

    monkeypatch.setattr(social_repo.pg, "fetch_all", _fake_fetch_all)

    payload = social_repo.get_post_comments("season-1", platform="instagram", source_id="abc123")

    assert "coalesce(c.is_missing, false) = false" in str(captured["sql"]).lower()
    assert payload["comments"][0]["media_urls"] == ["https://instagram.example/comment.jpg"]
    assert payload["comments"][0]["hosted_media_urls"] == ["https://cdn.example/comment.jpg"]
    assert payload["comments"][0]["media_mirror_status"] == "mirrored"


def test_tiktok_comment_media_needs_mirror_flags_legacy_host(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OBJECT_STORAGE_PUBLIC_BASE_URL", "https://cdn.test")
    social_repo._expected_cdn_host.cache_clear()  # noqa: SLF001
    try:
        assert (
            social_repo._tiktok_comment_needs_media_mirror(  # noqa: SLF001
                {
                    "media_urls": ["https://src.test/comment.jpg"],
                    "hosted_media_urls": ["https://legacy.example/comment.jpg"],
                    "media_mirror_status": "mirrored",
                }
            )
            is True
        )
    finally:
        social_repo._expected_cdn_host.cache_clear()  # noqa: SLF001


def test_run_tiktok_comment_media_mirror_stage_repairs_and_resolves_week_index(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    updates: list[dict[str, object]] = []
    mirrored_inputs: list[dict[str, object]] = []

    monkeypatch.setattr(social_repo, "_tiktok_comments_has_column", lambda _column: True)
    monkeypatch.setattr(
        social_repo.pg,
        "fetch_one",
        lambda _sql, _params: {
            "id": "tt-comment-db-1",
            "comment_id": "tt-comment-1",
            "post_id": "tt-post-db-1",
            "media_urls": ["https://src.test/comment.jpg"],
            "hosted_media_urls": [],
            "media_mirror_status": "failed",
            "media_mirror_error": "old",
            "video_id": "tt-video-1",
            "posted_at": datetime(2026, 2, 20, tzinfo=UTC),
        },
    )
    monkeypatch.setattr(
        social_repo,
        "_update_tiktok_comment_media_mirror_fields",
        lambda **kwargs: updates.append(dict(kwargs)),
    )
    monkeypatch.setattr(social_repo, "_resolve_week_windows", lambda *_args, **_kwargs: (["window"], None))
    monkeypatch.setattr(
        social_repo,
        "_week_for_timestamp",
        lambda *_args, **_kwargs: SimpleNamespace(week_index=2),
    )

    def _fake_mirror_result(_context, *, platform: str, post, week_index: int | None, **_kwargs):  # noqa: ANN001
        mirrored_inputs.append(
            {
                "platform": platform,
                "week_index": week_index,
                "media_urls": list(post.media_urls or []),
            }
        )
        return {
            "hosted_media_urls": ["https://cdn.test/comment.jpg"],
            "status": "mirrored",
            "error": None,
            "retryable_error": False,
            "mirrored_count": 1,
            "source_count": 1,
        }

    monkeypatch.setattr(social_repo, "_mirror_platform_media_to_s3_result", _fake_mirror_result)

    posts, mirrored, metadata = social_repo._run_tiktok_comment_media_mirror_stage(  # noqa: SLF001
        context=_season_context(),
        job_id="job-tt-comment-1",
        config={"comment_id": "tt-comment-1", "_attempt_count": 2},
    )

    assert posts == 1
    assert mirrored == 1
    assert mirrored_inputs == [
        {
            "platform": "tiktok",
            "week_index": 2,
            "media_urls": ["https://src.test/comment.jpg"],
        }
    ]
    assert updates[0]["media_mirror_status"] == "pending"
    assert updates[-1]["media_mirror_status"] == "mirrored"
    assert metadata["comment_media_mirror"]["week_index"] == 2


def test_run_tiktok_comment_media_mirror_stage_skips_when_already_hosted(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(social_repo, "_tiktok_comments_has_column", lambda _column: True)
    monkeypatch.setattr(social_repo, "_hosted_urls_need_cdn_host_repair", lambda **_kwargs: False)
    monkeypatch.setattr(social_repo, "_hosted_media_urls_need_content_repair", lambda **_kwargs: False)
    monkeypatch.setattr(
        social_repo.pg,
        "fetch_one",
        lambda _sql, _params: {
            "id": "tt-comment-db-2",
            "comment_id": "tt-comment-2",
            "post_id": "tt-post-db-2",
            "media_urls": ["https://src.test/comment.jpg"],
            "hosted_media_urls": ["https://cdn.test/comment.jpg"],
            "media_mirror_status": "mirrored",
            "media_mirror_error": "",
            "video_id": "tt-video-2",
            "posted_at": datetime(2026, 2, 20, tzinfo=UTC),
        },
    )
    monkeypatch.setattr(
        social_repo,
        "_mirror_platform_media_to_s3_result",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("mirror should be skipped")),
    )

    posts, mirrored, metadata = social_repo._run_tiktok_comment_media_mirror_stage(  # noqa: SLF001
        context=_season_context(),
        job_id="job-tt-comment-2",
        config={"comment_id": "tt-comment-2", "_attempt_count": 1, "week_index": 1},
    )

    assert posts == 1
    assert mirrored == 0
    assert metadata["comment_media_mirror"]["status"] == "up_to_date"


def test_run_platform_media_mirror_stage_threads_uses_persisted_raw_data_when_config_payload_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_data_seen: list[dict[str, object]] = []
    mirrored_inputs: list[dict[str, object]] = []

    monkeypatch.setattr(
        social_repo.pg,
        "fetch_one",
        lambda _sql, _params: {
            "id": "threads-post-db-1",
            "source_id": "threads-post-1",
            "thumbnail_url": "",
            "media_urls": [],
            "asset_manifest": {},
            "post_username": "bravotv",
            "raw_data": {"media": "persisted"},
            "posted_at": datetime(2026, 2, 20, tzinfo=UTC),
            "hosted_thumbnail_url": "",
            "hosted_media_urls": [],
            "media_mirror_status": "failed",
            "media_mirror_error": "old",
            "user_avatar_url": "",
            "hosted_user_avatar_url": "",
            "owner_profile_pic_url": "",
            "hosted_owner_profile_pic_url": "",
            "hosted_tagged_profile_pics": {},
        },
    )
    monkeypatch.setattr(
        social_repo,
        "_update_platform_post_media_mirror_fields",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        social_repo,
        "_recover_platform_post_source_avatar",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        social_repo,
        "_update_platform_post_media_asset_meta",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        social_repo,
        "_platform_posts_has_column",
        lambda _platform, column: column in {"media_urls", "asset_manifest", "raw_data"},
    )

    def _fake_resolve_threads_media(raw_data: dict[str, object], validate_urls: bool = False):  # noqa: FBT002
        del validate_urls
        raw_data_seen.append(dict(raw_data))
        return SimpleNamespace(
            media_urls=["https://images.test/threads-video.mp4"],
            thumbnail_url="https://images.test/threads-thumb.jpg",
            source="threads_graphql_post_data",
            attempts=[{"source": "threads_graphql_post_data", "success": True}],
            media_asset_meta={},
        )

    monkeypatch.setattr("trr_backend.socials.threads.resolve_threads_media", _fake_resolve_threads_media)

    def _fake_mirror_result(_context, *, platform: str, post, week_index: int | None, **_kwargs):  # noqa: ANN001
        mirrored_inputs.append(
            {
                "platform": platform,
                "week_index": week_index,
                "thumbnail_url": post.thumbnail_url,
                "media_urls": list(post.media_urls or []),
            }
        )
        return {
            "hosted_thumbnail_url": "https://cdn.test/thumb.jpg",
            "hosted_media_urls": ["https://cdn.test/video.mp4"],
            "status": "mirrored",
            "error": None,
            "retryable_error": False,
            "mirrored_count": 2,
            "source_count": 2,
        }

    monkeypatch.setattr(social_repo, "_mirror_platform_media_to_s3_result", _fake_mirror_result)

    posts, mirrored, metadata = social_repo._run_platform_media_mirror_stage(  # noqa: SLF001
        context=_season_context(),
        platform="threads",
        job_id="job-threads-1",
        config={"post_id": "threads-post-db-1", "_attempt_count": 1, "week_index": 1},
    )

    assert posts == 1
    assert mirrored == 2
    assert raw_data_seen == [{"media": "persisted"}]
    assert mirrored_inputs[0]["media_urls"] == ["https://images.test/threads-video.mp4"]
    assert metadata["mirror"]["selected_source"] == "threads_graphql_post_data"


def test_run_generic_comment_media_mirror_stage_threads_repairs_from_persisted_raw_data(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    updates: list[dict[str, object]] = []

    monkeypatch.setattr(social_repo, "_column_exists", lambda _schema, _table, _column: True)
    monkeypatch.setattr(
        social_repo.pg,
        "fetch_one",
        lambda _sql, _params: {
            "id": "threads-comment-db-1",
            "comment_id": "threads-comment-1",
            "post_id": "threads-post-db-1",
            "media_urls": [],
            "hosted_media_urls": [],
            "media_mirror_status": "failed",
            "media_mirror_error": "old",
            "raw_data": {"media": "persisted"},
            "post_created_at": datetime(2026, 2, 20, tzinfo=UTC),
        },
    )
    monkeypatch.setattr(
        social_repo,
        "_update_platform_comment_media_mirror_fields",
        lambda **kwargs: updates.append(dict(kwargs)),
    )
    monkeypatch.setattr(social_repo, "_resolve_week_windows", lambda *_args, **_kwargs: (["window"], None))
    monkeypatch.setattr(
        social_repo,
        "_week_for_timestamp",
        lambda *_args, **_kwargs: SimpleNamespace(week_index=4),
    )
    monkeypatch.setattr(
        "trr_backend.socials.threads.resolve_threads_media",
        lambda raw_data, validate_urls=False: SimpleNamespace(  # noqa: ARG005, FBT002
            media_urls=["https://images.test/threads-comment.jpg"],
            thumbnail_url=None,
            source="threads_graphql_comment_data",
            attempts=[{"source": "threads_graphql_comment_data", "success": True}],
            media_asset_meta={},
        ),
    )
    monkeypatch.setattr(
        social_repo,
        "_mirror_platform_media_to_s3_result",
        lambda *_args, **_kwargs: {
            "hosted_media_urls": ["https://cdn.test/threads-comment.jpg"],
            "status": "mirrored",
            "error": None,
            "retryable_error": False,
            "mirrored_count": 1,
            "source_count": 1,
        },
    )

    posts, mirrored, metadata = social_repo._run_generic_comment_media_mirror_stage(  # noqa: SLF001
        context=_season_context(),
        platform="threads",
        job_id="job-threads-comment-1",
        config={"comment_id": "threads-comment-1", "_attempt_count": 2},
    )

    assert posts == 1
    assert mirrored == 1
    assert updates[0]["media_mirror_status"] == "pending"
    assert updates[-1]["media_mirror_status"] == "mirrored"
    assert metadata["comment_media_mirror"]["week_index"] == 4


def test_run_generic_comment_media_mirror_stage_threads_marks_missing_raw_data_non_repairable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    updates: list[dict[str, object]] = []

    monkeypatch.setattr(social_repo, "_column_exists", lambda _schema, _table, _column: True)
    monkeypatch.setattr(
        social_repo.pg,
        "fetch_one",
        lambda _sql, _params: {
            "id": "threads-comment-db-2",
            "comment_id": "threads-comment-2",
            "post_id": "threads-post-db-2",
            "media_urls": [],
            "hosted_media_urls": [],
            "media_mirror_status": "failed",
            "media_mirror_error": "old",
            "raw_data": {},
            "post_created_at": datetime(2026, 2, 20, tzinfo=UTC),
        },
    )
    monkeypatch.setattr(
        social_repo,
        "_update_platform_comment_media_mirror_fields",
        lambda **kwargs: updates.append(dict(kwargs)),
    )

    posts, mirrored, metadata = social_repo._run_generic_comment_media_mirror_stage(  # noqa: SLF001
        context=_season_context(),
        platform="threads",
        job_id="job-threads-comment-2",
        config={"comment_id": "threads-comment-2", "_attempt_count": 1},
    )

    assert posts == 1
    assert mirrored == 0
    assert updates[0]["media_mirror_error"] == "threads_comment_media_non_repairable"
    assert metadata["comment_media_mirror"]["error"] == "threads_comment_media_non_repairable"


def test_platform_post_source_urls_youtube_does_not_use_watch_url_as_thumbnail() -> None:
    thumbnail_url, media_urls = social_repo._platform_post_source_urls(
        "youtube",
        {
            "thumbnail_url": "",
            "media_urls": ["https://www.youtube.com/watch?v=vid123"],
        },
    )

    assert thumbnail_url == ""
    assert media_urls == ["https://www.youtube.com/watch?v=vid123"]


def test_run_platform_stage_retires_youtube_comment_media_lane() -> None:
    with pytest.raises(ValueError, match="youtube_comment_media_mirror_obsolete"):
        social_repo._run_platform_stage(  # noqa: SLF001
            context=_season_context(),
            run_id=None,
            platform="youtube",
            stage=COMMENT_MEDIA_MIRROR_STAGE,
            account="bravotv",
            hashtags=[],
            keywords=[],
            opts=_ingest_options(),
            job_id="job-youtube-comment-media",
            config={},
        )


def test_run_platform_media_mirror_stage_instagram_selects_asset_manifest(monkeypatch: pytest.MonkeyPatch) -> None:
    captured_sql: list[str] = []
    captured_asset_meta_updates: list[dict[str, object]] = []

    def _fake_fetch_one(sql: str, _params: list[object]) -> dict[str, object]:
        captured_sql.append(sql)
        return {
            "id": "ig-post-db-1",
            "source_id": "abc123",
            "thumbnail_url": "https://src.test/thumb.jpg",
            "media_urls": ["https://src.test/media.jpg"],
            "asset_manifest": {},
            "post_username": "bravotv",
            "raw_data": {},
            "posted_at": datetime(2026, 2, 20, tzinfo=UTC),
            "hosted_thumbnail_url": "https://cdn.test/thumb.jpg",
            "hosted_media_urls": ["https://cdn.test/media.jpg"],
            "media_mirror_status": "mirrored",
            "media_mirror_error": "",
            "user_avatar_url": "",
            "hosted_user_avatar_url": "",
            "owner_profile_pic_url": "",
            "hosted_owner_profile_pic_url": "",
            "hosted_tagged_profile_pics": {},
        }

    monkeypatch.setattr(social_repo.pg, "fetch_one", _fake_fetch_one)
    monkeypatch.setattr(
        social_repo,
        "_platform_posts_has_column",
        lambda _platform, column: column in {"media_urls", "asset_manifest"},
    )
    monkeypatch.setattr(
        social_repo,
        "_update_platform_post_media_asset_meta",
        lambda **kwargs: captured_asset_meta_updates.append(dict(kwargs)),
    )
    monkeypatch.setattr(
        social_repo,
        "_mirror_platform_media_to_s3_result",
        lambda *_args, **_kwargs: {
            "status": "mirrored",
            "error": None,
            "hosted_thumbnail_url": "https://cdn.test/thumb.jpg",
            "hosted_media_urls": ["https://cdn.test/media.jpg"],
            "retryable_error": False,
        },
    )

    social_repo._run_platform_media_mirror_stage(  # noqa: SLF001
        context=_season_context(),
        platform="instagram",
        job_id="job-instagram-asset-manifest",
        config={"post_id": "ig-post-db-1", "_attempt_count": 1, "week_index": 1},
    )

    assert "asset_manifest" in captured_sql[0].lower()
    assert captured_asset_meta_updates


def test_get_post_comments_facebook_filters_missing_comments(monkeypatch: pytest.MonkeyPatch) -> None:
    captured_sql: list[str] = []

    monkeypatch.setattr(
        social_repo.pg,
        "fetch_one",
        lambda _sql, _params: {
            "id": "fb-post-db-1",
            "source_id": "fb-1",
            "author": "bravotv",
            "text": "hello #bravo",
            "post_type": "feed",
            "likes": 5,
            "comments_count": 1,
            "shares": 2,
            "views": 11,
            "hashtags": ["BRAVO"],
            "mentions": [],
            "thumbnail_url": "https://cdn.test/thumb.jpg",
            "source_media_urls": [],
            "hosted_media_urls": [],
            "source_thumbnail_url": "https://src.test/thumb.jpg",
            "hosted_thumbnail_url": "https://cdn.test/thumb.jpg",
            "raw_data": {},
            "ts": datetime(2026, 2, 23, 12, 0, tzinfo=UTC),
        },
    )

    def _fake_fetch_all(sql: str, _params: list[object]) -> list[dict[str, object]]:
        captured_sql.append(sql)
        return []

    monkeypatch.setattr(social_repo.pg, "fetch_all", _fake_fetch_all)

    social_repo.get_post_comments("season-1", platform="facebook", source_id="fb-1")

    assert "coalesce(c.is_missing, false) = false" in captured_sql[0].lower()


def test_week_summary_fast_facebook_filters_missing_comments(monkeypatch: pytest.MonkeyPatch) -> None:
    seen_sql: list[str] = []

    def _fake_fetch_one(sql: str, _params: list[object]) -> dict[str, int]:
        seen_sql.append(sql)
        if "saved_comments_total" in sql:
            return {"saved_comments_total": 1}
        return {"total_posts": 1, "expected_comments_total": 2, "total_engagement": 10}

    monkeypatch.setattr(social_repo.pg, "fetch_one", _fake_fetch_one)
    monkeypatch.setattr(social_repo, "_comment_lifecycle_supported", lambda table: table == "facebook_comments")

    payload = social_repo._week_summary_fast_facebook(  # noqa: SLF001
        season_id="season-1",
        start_dt=datetime(2026, 2, 1, tzinfo=UTC),
        end_dt=datetime(2026, 2, 28, tzinfo=UTC),
        account_handles=set(),
    )

    assert payload["totals"]["saved_comments_total"] == 1
    assert any("coalesce(c.is_missing, false) = false" in sql.lower() for sql in seen_sql)


def test_refresh_facebook_post_detail_sync_preserves_existing_metrics(monkeypatch: pytest.MonkeyPatch) -> None:
    captured_metrics: dict[str, int] = {}

    class _FakeFacebookScraper:
        def __init__(self, *args, **kwargs) -> None:
            del args, kwargs

        def scrape_post(self, _url: str, delay_seconds: float, fetch_comment_list: bool):  # noqa: FBT001
            del delay_seconds, fetch_comment_list
            return (
                SimpleNamespace(
                    post_id="fb-1",
                    username="bravotv",
                    likes=0,
                    comments=0,
                    shares=1,
                    views=2,
                ),
                [],
            )

    monkeypatch.setattr("trr_backend.socials.facebook.FacebookScraper", _FakeFacebookScraper)
    monkeypatch.setattr(social_repo, "_load_facebook_cookies", lambda: {})
    monkeypatch.setattr(social_repo.pg, "db_connection", lambda: nullcontext(None))
    monkeypatch.setattr(
        social_repo,
        "_upsert_facebook_post",
        lambda _context, *, job_id, account, post, conn=None: (
            captured_metrics.update(
                {
                    "likes": int(post.likes),
                    "comments": int(post.comments),
                    "shares": int(post.shares),
                    "views": int(post.views),
                }
            ),
            {"id": "fb-db-1"},
        )[1],
    )

    payload = social_repo._refresh_facebook_post_detail_sync(  # noqa: SLF001
        _season_context(),
        source_id="fb-1",
        account="bravotv",
        row_json={
            "likes": 12,
            "comments_count": 8,
            "shares": 4,
            "views": 22,
            "raw_data": {},
        },
        detail_job_id="job-fb-detail",
    )

    assert payload["status"] == "success"
    assert captured_metrics == {"likes": 12, "comments": 8, "shares": 4, "views": 22}
