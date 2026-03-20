from __future__ import annotations

import json

import pytest

import scripts.socials.repair_social_hosted_urls as mod


class _FakeCursor:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self._rows = rows
        self.statements: list[tuple[str, tuple[object, ...] | None]] = []

    def execute(self, sql: str, params: tuple[object, ...] | None = None) -> None:
        self.statements.append((sql, params))

    def fetchall(self) -> list[dict[str, object]]:
        return self._rows


def test_rewrite_to_cdn_replaces_legacy_cloudfront_host_and_preserves_path() -> None:
    rewritten, changed = mod._rewrite_to_cdn(
        "https://d1fmdyqfafwim3.cloudfront.net/social/youtube/a/media-01.mp4",
        cdn_base_url="https://pub.example.r2.dev",
    )
    assert changed is True
    assert rewritten == "https://pub.example.r2.dev/social/youtube/a/media-01.mp4"


def test_rewrite_media_asset_meta_updates_nested_hosted_urls() -> None:
    raw_data, rewrite_count = mod._rewrite_media_asset_meta(
        {
            "media_asset_meta": {
                "hosted_assets": [{"url": "https://legacy.example/social/youtube/a/video.mp4"}],
                "thumbnail_hosted": {"url": "https://legacy.example/social/youtube/a/thumb.jpg"},
            }
        },
        cdn_base_url="https://pub.example.r2.dev",
    )
    assert rewrite_count == 2
    meta = raw_data["media_asset_meta"]
    assert meta["hosted_assets"][0]["url"] == "https://pub.example.r2.dev/social/youtube/a/video.mp4"
    assert meta["thumbnail_hosted"]["url"] == "https://pub.example.r2.dev/social/youtube/a/thumb.jpg"


def test_repair_platform_dry_run_reports_rewrites_without_updates(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mod.social_repo, "_platform_posts_has_column", lambda *_args, **_kwargs: False)
    cur = _FakeCursor(
        rows=[
            {
                "id": "00000000-0000-0000-0000-000000000111",
                "hosted_thumbnail_url": "https://legacy.example/social/facebook/a/thumb.jpg",
                "hosted_media_urls": ["https://legacy.example/social/facebook/a/media-01.mp4"],
                "hosted_user_avatar_url": "https://legacy.example/social/facebook/profile-pics/a/avatar.jpg",
                "hosted_owner_profile_pic_url": "",
                "hosted_tagged_profile_pics": {},
                "raw_data": {
                    "media_asset_meta": {
                        "hosted_assets": [{"url": "https://legacy.example/social/facebook/a/media-01.mp4"}],
                        "thumbnail_hosted": {"url": "https://legacy.example/social/facebook/a/thumb.jpg"},
                    }
                },
            }
        ]
    )

    stats = mod._repair_platform(
        cur,
        platform="facebook",
        table="facebook_posts",
        cdn_base_url="https://pub.example.r2.dev",
        season_ids=[],
        show_ids=[],
        season_numbers=[],
        limit_per_platform=100,
        dry_run=True,
    )

    assert stats.scanned_rows == 1
    assert stats.rows_needing_repair == 1
    assert stats.rows_updated == 0
    assert stats.thumbnail_urls_rewritten == 1
    assert stats.media_urls_rewritten == 1
    assert stats.avatar_urls_rewritten == 1
    assert stats.media_asset_meta_urls_rewritten == 2
    assert len(cur.statements) == 1


def test_repair_platform_apply_updates_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        mod.social_repo,
        "_platform_posts_has_column",
        lambda _platform, column: column in {"hosted_user_avatar_url", "hosted_tagged_profile_pics"},
    )
    cur = _FakeCursor(
        rows=[
            {
                "id": "00000000-0000-0000-0000-000000000222",
                "hosted_thumbnail_url": "https://legacy.example/social/instagram/a/thumb.jpg",
                "hosted_media_urls": [
                    "https://legacy.example/social/instagram/a/media-01.jpg",
                    "https://pub.example.r2.dev/social/instagram/a/media-02.jpg",
                ],
                "hosted_user_avatar_url": "",
                "hosted_owner_profile_pic_url": "",
                "hosted_tagged_profile_pics": {
                    "bravo": "https://legacy.example/social/instagram/profile-pics/bravo/a.jpg"
                },
                "raw_data": {},
            }
        ]
    )

    stats = mod._repair_platform(
        cur,
        platform="instagram",
        table="instagram_posts",
        cdn_base_url="https://pub.example.r2.dev",
        season_ids=[],
        show_ids=[],
        season_numbers=[],
        limit_per_platform=100,
        dry_run=False,
    )

    assert stats.scanned_rows == 1
    assert stats.rows_needing_repair == 1
    assert stats.rows_updated == 1
    assert stats.thumbnail_urls_rewritten == 1
    assert stats.media_urls_rewritten == 1
    assert stats.avatar_urls_rewritten == 1
    assert len(cur.statements) == 2

    update_sql, update_params = cur.statements[1]
    assert "update social.instagram_posts" in " ".join(update_sql.lower().split())
    assert isinstance(update_params, tuple)
    assert update_params[0] == "https://pub.example.r2.dev/social/instagram/a/thumb.jpg"
    media_urls = json.loads(str(update_params[1]))
    assert media_urls == [
        "https://pub.example.r2.dev/social/instagram/a/media-01.jpg",
        "https://pub.example.r2.dev/social/instagram/a/media-02.jpg",
    ]
    tagged_profile_pics = json.loads(str(update_params[4]))
    assert tagged_profile_pics == {"bravo": "https://pub.example.r2.dev/social/instagram/profile-pics/bravo/a.jpg"}


def test_parse_platforms_rejects_invalid_platforms() -> None:
    with pytest.raises(RuntimeError, match="Unsupported platforms"):
        mod._parse_platforms("facebook,invalid")
