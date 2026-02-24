from __future__ import annotations

from scripts.socials.backfill_instagram_metadata_and_media import _mirror_is_missing


def test_mirror_is_missing_thumbnail_only_complete_without_status() -> None:
    row = {
        "thumbnail_url": "https://src/thumb.jpg",
        "media_urls": [],
        "hosted_thumbnail_url": "https://cdn/thumb.jpg",
        "hosted_media_urls": [],
        "media_mirror_status": "",
    }
    assert _mirror_is_missing(row) is False


def test_mirror_is_missing_pending_status_returns_true() -> None:
    row = {
        "thumbnail_url": "https://src/thumb.jpg",
        "media_urls": [],
        "hosted_thumbnail_url": "https://cdn/thumb.jpg",
        "hosted_media_urls": [],
        "media_mirror_status": "pending",
    }
    assert _mirror_is_missing(row) is True
