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


def test_rewrite_to_cdn_replaces_s3_host_and_preserves_path() -> None:
    rewritten, changed = mod._rewrite_to_cdn(
        "https://trr-backend.s3.amazonaws.com/social/youtube/a/media-01.mp4",
        cdn_base_url="https://d1fmdyqfafwim3.cloudfront.net",
    )
    assert changed is True
    assert rewritten == "https://d1fmdyqfafwim3.cloudfront.net/social/youtube/a/media-01.mp4"


def test_rewrite_to_cdn_leaves_non_s3_host_unchanged() -> None:
    original = "https://d1fmdyqfafwim3.cloudfront.net/social/facebook/a/thumb.jpg"
    rewritten, changed = mod._rewrite_to_cdn(original, cdn_base_url="https://d1fmdyqfafwim3.cloudfront.net")
    assert changed is False
    assert rewritten == original


def test_repair_platform_dry_run_reports_rewrites_without_updates() -> None:
    cur = _FakeCursor(
        rows=[
            {
                "id": "00000000-0000-0000-0000-000000000111",
                "hosted_thumbnail_url": "https://trr-backend.s3.amazonaws.com/social/facebook/a/thumb.jpg",
                "hosted_media_urls": [
                    "https://trr-backend.s3.amazonaws.com/social/facebook/a/media-01.mp4",
                ],
            }
        ]
    )

    stats = mod._repair_platform(
        cur,
        table="facebook_posts",
        cdn_base_url="https://d1fmdyqfafwim3.cloudfront.net",
        season_id="",
        limit_per_platform=100,
        dry_run=True,
    )

    assert stats.scanned_rows == 1
    assert stats.rows_needing_repair == 1
    assert stats.rows_updated == 0
    assert stats.thumbnail_urls_rewritten == 1
    assert stats.media_urls_rewritten == 1
    assert len(cur.statements) == 1  # select only


def test_repair_platform_apply_updates_rows() -> None:
    cur = _FakeCursor(
        rows=[
            {
                "id": "00000000-0000-0000-0000-000000000222",
                "hosted_thumbnail_url": "https://trr-backend.s3.amazonaws.com/social/youtube/a/thumb.jpg",
                "hosted_media_urls": [
                    "https://trr-backend.s3.amazonaws.com/social/youtube/a/media-01.mp4",
                    "https://d1fmdyqfafwim3.cloudfront.net/social/youtube/a/media-02.mp4",
                ],
            }
        ]
    )

    stats = mod._repair_platform(
        cur,
        table="youtube_videos",
        cdn_base_url="https://d1fmdyqfafwim3.cloudfront.net",
        season_id="",
        limit_per_platform=100,
        dry_run=False,
    )

    assert stats.scanned_rows == 1
    assert stats.rows_needing_repair == 1
    assert stats.rows_updated == 1
    assert stats.thumbnail_urls_rewritten == 1
    assert stats.media_urls_rewritten == 1
    assert len(cur.statements) == 2  # select + update

    update_sql, update_params = cur.statements[1]
    assert "update social.youtube_videos" in " ".join(update_sql.lower().split())
    assert isinstance(update_params, tuple)
    assert update_params[0] == "https://d1fmdyqfafwim3.cloudfront.net/social/youtube/a/thumb.jpg"
    media_urls = json.loads(str(update_params[1]))
    assert media_urls == [
        "https://d1fmdyqfafwim3.cloudfront.net/social/youtube/a/media-01.mp4",
        "https://d1fmdyqfafwim3.cloudfront.net/social/youtube/a/media-02.mp4",
    ]


def test_parse_platforms_rejects_invalid_platforms() -> None:
    with pytest.raises(RuntimeError, match="Unsupported platforms"):
        mod._parse_platforms("facebook,instagram")
