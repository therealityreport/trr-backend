from __future__ import annotations


def test_adapt_tiktok_item_to_post_dto():
    from trr_backend.socials.tiktok.posts_scrapling.persistence import _tiktok_item_to_post_dto

    item = {
        "id": "7300000000000000000",
        "desc": "Test post #fyp",
        "createTime": 1700000000,
        "author": {"uniqueId": "testuser", "nickname": "Test User", "avatarThumb": "https://p.tiktok.com/av.jpg"},
        "stats": {"diggCount": 100, "commentCount": 50, "shareCount": 25, "playCount": 10000, "collectCount": 5},
        "music": {"title": "Original Sound", "authorName": "testuser"},
        "video": {"duration": 30, "cover": "https://p.tiktok.com/cover.jpg"},
    }
    dto = _tiktok_item_to_post_dto(item, account_handle="testuser")
    assert dto.video_id == "7300000000000000000"
    assert dto.likes == 100
    assert dto.comments == 50
    assert dto.views == 10000
    assert dto.saves == 5
    assert dto.username == "testuser"
    assert dto.duration == 30
    assert dto.music_title == "Original Sound"
    assert dto.create_time == 1700000000
    assert hasattr(dto, "to_dict")


def test_adapt_tiktok_item_missing_fields_graceful():
    from trr_backend.socials.tiktok.posts_scrapling.persistence import _tiktok_item_to_post_dto

    item = {"id": "7400000000000000000", "createTime": 1700000000}
    dto = _tiktok_item_to_post_dto(item, account_handle="fallback_user")
    assert dto.video_id == "7400000000000000000"
    assert dto.username == "fallback_user"  # fell back to account_handle
    assert dto.likes == 0
    assert dto.description == ""


def test_adapt_tiktok_item_extracts_hashtags_from_challenges():
    from trr_backend.socials.tiktok.posts_scrapling.persistence import _tiktok_item_to_post_dto

    item = {
        "id": "7500000000000000000",
        "createTime": 1700000000,
        "desc": "Hello world",
        "challenges": [
            {"title": "fyp"},
            {"title": "foryou"},
            {"title": ""},  # should be filtered
        ],
    }
    dto = _tiktok_item_to_post_dto(item, account_handle="u")
    assert "fyp" in dto.hashtags
    assert "foryou" in dto.hashtags
    assert "" not in dto.hashtags


def test_adapt_tiktok_item_aligns_with_canonical_parser_alternate_shape():
    from trr_backend.socials.tiktok.posts_scrapling.persistence import _tiktok_item_to_post_dto
    from trr_backend.socials.tiktok.scraper import TikTokScrapeConfig, TikTokScraper

    item = {
        "aweme_id": "7600000000000000000",
        "text": "Alt shape #RHOSLC from @bravotv",
        "createTimeISO": "2026-05-01T12:34:56Z",
        "webVideoUrl": "https://www.tiktok.com/@canonical_user/video/7600000000000000000",
        "statsV2": {
            "shareCount": "1.2K",
            "collectCount": "345",
            "playCount": "4.5M",
        },
        "commentCount": "98",
        "diggCount": "765",
        "authorMeta": {
            "name": "canonical_user",
            "nickName": "Canonical User",
            "avatarLarger": "https://p.tiktokcdn.com/avatar-large.jpg",
        },
        "musicMeta": {
            "musicName": "Canonical Sound",
            "musicAuthor": "Sound Author",
        },
        "videoMeta": {
            "duration": 42,
            "playUrl": "https://v16.tiktokcdn.com/video.mp4",
            "coverUrl": "https://p.tiktokcdn.com/cover.jpg",
        },
    }

    dto = _tiktok_item_to_post_dto(item, account_handle="fallback_user")
    canonical = TikTokScraper(cookies={})._parse_post_item(  # noqa: SLF001
        item,
        TikTokScrapeConfig(username="fallback_user"),
    )

    assert dto.video_id == canonical.video_id == "7600000000000000000"
    assert dto.description == canonical.description == "Alt shape #RHOSLC from @bravotv"
    assert dto.url == canonical.url == "https://www.tiktok.com/@canonical_user/video/7600000000000000000"
    assert dto.username == canonical.username == "canonical_user"
    assert dto.author_nickname == canonical.author_nickname == "Canonical User"
    assert dto.hashtags == canonical.hashtags == ["RHOSLC"]
    assert dto.mentions == canonical.mentions == ["bravotv"]
    assert dto.likes == canonical.likes == 765
    assert dto.comments == canonical.comments == 98
    assert dto.shares == canonical.shares == 1200
    assert dto.saves == canonical.saves == 345
    assert dto.views == canonical.views == 4_500_000
    assert dto.duration == canonical.duration == 42
    assert dto.music_title == canonical.music_title == "Canonical Sound"
    assert dto.music_author == canonical.music_author == "Sound Author"
    assert dto.user_avatar_url == canonical.user_avatar_url == "https://p.tiktokcdn.com/avatar-large.jpg"
    assert dto.media_urls == canonical.media_urls == ["https://v16.tiktokcdn.com/video.mp4"]
    assert dto.thumbnail_url == canonical.thumbnail_url == "https://p.tiktokcdn.com/cover.jpg"
    assert dto.to_dict() == item


def test_persist_tiktok_posts_accepts_aweme_id_without_id(monkeypatch):
    from trr_backend.db import pg
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.tiktok.posts_scrapling.persistence import persist_tiktok_posts

    class _ConnectionContext:
        def __enter__(self):
            return object()

        def __exit__(self, *_args):
            return None

    persisted_video_ids: list[str] = []

    def _fake_upsert(_context, *, post, **_kwargs):
        persisted_video_ids.append(post.video_id)

    monkeypatch.setattr(pg, "db_connection", lambda: _ConnectionContext())
    monkeypatch.setattr(repo, "_upsert_tiktok_post", _fake_upsert)

    result = persist_tiktok_posts(
        account_handle="testuser",
        post_items=[
            {
                "aweme_id": "7700000000000000000",
                "createTime": 1700000000,
                "text": "aweme id post",
            }
        ],
        run_id="run-1",
        job_id="job-1",
    )

    assert persisted_video_ids == ["7700000000000000000"]
    assert result.posts_upserted == 1
    assert result.posts_skipped == 0


def test_persist_tiktok_post_dtos_accepts_canonical_posts(monkeypatch):
    from trr_backend.db import pg
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.tiktok.posts_scrapling.persistence import persist_tiktok_post_dtos
    from trr_backend.socials.tiktok.scraper import TikTokPost

    class _ConnectionContext:
        def __enter__(self):
            return object()

        def __exit__(self, *_args):
            return None

    persisted_video_ids: list[str] = []

    def _fake_upsert(_context, *, post, **_kwargs):
        persisted_video_ids.append(post.video_id)

    monkeypatch.setattr(pg, "db_connection", lambda: _ConnectionContext())
    monkeypatch.setattr(repo, "_upsert_tiktok_post", _fake_upsert)

    result = persist_tiktok_post_dtos(
        account_handle="testuser",
        posts=[
            TikTokPost(
                video_id="7900000000000000000",
                date_time="2026-05-01 00:00:00",
                create_time=1777593600,
                description="canonical fallback post",
                hashtags=[],
                mentions=[],
                likes=1,
                comments=2,
                shares=3,
                saves=4,
                views=5,
                url="https://www.tiktok.com/@testuser/video/7900000000000000000",
                username="testuser",
                author_nickname="Test User",
                duration=10,
                music_title="",
                music_author="",
            )
        ],
        run_id="run-1",
        job_id="job-1",
    )

    assert persisted_video_ids == ["7900000000000000000"]
    assert result.posts_upserted == 1
    assert result.posts_skipped == 0


def test_adapt_tiktok_item_accepts_video_id_without_id():
    from trr_backend.socials.tiktok.posts_scrapling.persistence import _tiktok_item_to_post_dto

    dto = _tiktok_item_to_post_dto(
        {
            "videoId": "7800000000000000000",
            "text": "videoId fallback",
            "createTime": 1700000000,
        },
        account_handle="testuser",
    )

    assert dto.video_id == "7800000000000000000"
    assert dto.url == "https://www.tiktok.com/@testuser/video/7800000000000000000"


def test_persist_tiktok_posts_tracks_skipped_reasons(monkeypatch):
    from trr_backend.db import pg
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.tiktok.posts_scrapling.persistence import persist_tiktok_posts

    class _ConnectionContext:
        def __enter__(self):
            return object()

        def __exit__(self, *_args):
            return None

    def _fake_upsert(_context, *, post, **_kwargs):
        if post.video_id == "bad":
            raise RuntimeError("upsert failed")

    monkeypatch.setattr(pg, "db_connection", lambda: _ConnectionContext())
    monkeypatch.setattr(repo, "_upsert_tiktok_post", _fake_upsert)

    result = persist_tiktok_posts(
        account_handle="testuser",
        post_items=[
            "not-a-dict",
            {},
            {"id": "bad"},
            {"id": "good"},
        ],
        run_id="run-1",
        job_id="job-1",
    )

    assert result.posts_upserted == 1
    assert result.posts_skipped == 3
    assert result.posts_skipped_by_reason == {
        "invalid_item": 1,
        "missing_video_id": 1,
        "upsert_failed": 1,
    }
