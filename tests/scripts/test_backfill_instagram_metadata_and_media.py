from __future__ import annotations

from types import SimpleNamespace

import pytest

import scripts.socials.backfill_instagram_metadata_and_media as mod
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


def test_main_fails_fast_when_s3_preflight_fails(monkeypatch) -> None:
    monkeypatch.setattr(mod, "load_env", lambda: None)
    monkeypatch.setattr(
        mod,
        "_parse_args",
        lambda: SimpleNamespace(
            weeks=8,
            limit=None,
            source_scope="bravo",
            metadata_stale_hours=168,
            dry_run=False,
        ),
    )

    def _fail_preflight() -> None:
        raise RuntimeError("Missing required environment variable: OBJECT_STORAGE_BUCKET")

    monkeypatch.setattr(mod.social_repo, "ensure_media_mirror_s3_ready", _fail_preflight)

    with pytest.raises(SystemExit, match="Instagram media mirror S3 preflight failed"):
        mod.main()
