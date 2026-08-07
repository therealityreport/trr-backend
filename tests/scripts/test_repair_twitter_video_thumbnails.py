from __future__ import annotations

from typing import Any, cast

import scripts.socials.repair_twitter_video_thumbnails as mod


class _FakeCursor:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self._rows = rows
        self.statements: list[tuple[str, tuple[object, ...] | None]] = []

    def execute(self, sql: str, params: tuple[object, ...] | None = None) -> None:
        self.statements.append((sql, params))

    def fetchall(self) -> list[dict[str, object]]:
        return self._rows


def test_is_video_like_url_detects_twitter_video_host_and_extensions() -> None:
    assert mod._is_video_like_url("https://video.twimg.com/ext_tw_video/12345/pu/vid/1280x720/clip") is True
    assert mod._is_video_like_url("https://cdn.test/social/twitter/x/thumbnail.mp4") is True
    assert mod._is_video_like_url("https://cdn.test/social/twitter/x/media-02.jpg") is False


def test_repair_rows_dry_run_prefers_hosted_media_non_video_url() -> None:
    cur = _FakeCursor(
        rows=[
            {
                "id": "00000000-0000-0000-0000-000000000111",
                "tweet_id": "tweet-1",
                "hosted_thumbnail_url": "https://cdn.test/social/twitter/x/thumbnail.mp4",
                "hosted_media_urls": [
                    "https://cdn.test/social/twitter/x/media-01.mp4",
                    "https://cdn.test/social/twitter/x/media-02.jpg",
                ],
                "media_urls": [
                    "https://video.twimg.com/ext_tw_video/1.mp4",
                    "https://pbs.twimg.com/ext_tw_video_thumb/1.jpg",
                ],
            }
        ]
    )

    stats = mod._repair_rows(cast(Any, cur), season_id="", limit=100, dry_run=True)

    assert stats.scanned == 1
    assert stats.eligible == 1
    assert stats.updated == 0
    assert stats.skipped == 1
    assert stats.unresolved == 0
    assert len(cur.statements) == 1  # select only


def test_repair_rows_apply_falls_back_to_source_media_non_video_url() -> None:
    cur = _FakeCursor(
        rows=[
            {
                "id": "00000000-0000-0000-0000-000000000222",
                "tweet_id": "tweet-2",
                "hosted_thumbnail_url": "https://cdn.test/social/twitter/x/thumbnail.mp4",
                "hosted_media_urls": [
                    "https://cdn.test/social/twitter/x/media-01.mp4",
                ],
                "media_urls": [
                    "https://video.twimg.com/ext_tw_video/2.mp4",
                    "https://pbs.twimg.com/ext_tw_video_thumb/2.jpg",
                ],
            }
        ]
    )

    stats = mod._repair_rows(cast(Any, cur), season_id="", limit=100, dry_run=False)

    assert stats.scanned == 1
    assert stats.eligible == 1
    assert stats.updated == 1
    assert stats.skipped == 0
    assert stats.unresolved == 0
    assert len(cur.statements) == 2  # select + update

    update_sql, update_params = cur.statements[1]
    assert "update social.twitter_tweets" in " ".join(update_sql.lower().split())
    assert update_params == (
        "https://pbs.twimg.com/ext_tw_video_thumb/2.jpg",
        "00000000-0000-0000-0000-000000000222",
    )


def test_repair_rows_tracks_unresolved_when_no_non_video_candidate_exists() -> None:
    cur = _FakeCursor(
        rows=[
            {
                "id": "00000000-0000-0000-0000-000000000333",
                "tweet_id": "tweet-3",
                "hosted_thumbnail_url": "https://cdn.test/social/twitter/x/thumbnail.mp4",
                "hosted_media_urls": [
                    "https://cdn.test/social/twitter/x/media-01.mp4",
                ],
                "media_urls": [
                    "https://video.twimg.com/ext_tw_video/3.mp4",
                ],
            }
        ]
    )

    stats = mod._repair_rows(cast(Any, cur), season_id="", limit=100, dry_run=False)

    assert stats.scanned == 1
    assert stats.eligible == 0
    assert stats.updated == 0
    assert stats.skipped == 0
    assert stats.unresolved == 1
    assert len(cur.statements) == 1  # select only
