from __future__ import annotations

import json
from contextlib import nullcontext
from types import SimpleNamespace

import pytest

import scripts.socials.backfill_social_media_mirror_jobs as mod


def test_main_fails_fast_when_s3_preflight_fails(monkeypatch) -> None:
    monkeypatch.setattr(mod, "load_env", lambda: None)
    monkeypatch.setattr(
        mod,
        "_parse_args",
        lambda: SimpleNamespace(
            weeks=8,
            platforms="instagram,tiktok,youtube,twitter",
            source_scope="bravo",
            limit_per_platform=5000,
            failed_only=False,
            hosted_html_only=False,
        ),
    )

    def _fail_preflight() -> None:
        raise RuntimeError("Missing required environment variable: AWS_S3_BUCKET")

    monkeypatch.setattr(mod.social_repo, "ensure_media_mirror_s3_ready", _fail_preflight)

    with pytest.raises(SystemExit, match="Social media mirror S3 preflight failed"):
        mod.main()


def test_main_hosted_html_only_filters_non_html_rows(monkeypatch, capsys) -> None:
    monkeypatch.setattr(mod, "load_env", lambda: None)
    monkeypatch.setattr(
        mod,
        "_parse_args",
        lambda: SimpleNamespace(
            weeks=8,
            platforms="tiktok",
            source_scope="bravo",
            limit_per_platform=5000,
            failed_only=False,
            hosted_html_only=True,
        ),
    )
    monkeypatch.setattr(mod.social_repo, "ensure_media_mirror_s3_ready", lambda: None)
    monkeypatch.setattr(
        mod,
        "_load_rows",
        lambda **_kwargs: [
            {
                "id": "tt-1",
                "season_id": "season-1",
                "video_id": "video-1",
                "account": "bravotv",
                "posted_at": None,
                "thumbnail_url": "https://img.test/thumb.jpg",
                "media_urls": ["https://video.test/media-01.mp4"],
                "hosted_thumbnail_url": "https://cdn.test/thumb.jpg",
                "hosted_media_urls": ["https://cdn.test/social/tiktok/abc/media-01.html"],
                "media_mirror_status": "mirrored",
            },
            {
                "id": "tt-2",
                "season_id": "season-1",
                "video_id": "video-2",
                "account": "bravotv",
                "posted_at": None,
                "thumbnail_url": "https://img.test/thumb.jpg",
                "media_urls": ["https://video.test/media-02.mp4"],
                "hosted_thumbnail_url": "https://cdn.test/thumb.jpg",
                "hosted_media_urls": ["https://cdn.test/social/tiktok/abc/media-02.mp4"],
                "media_mirror_status": "mirrored",
            },
        ],
    )
    monkeypatch.setattr(mod.social_repo, "_platform_post_needs_media_mirror", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(mod.social_repo, "get_season_context", lambda _season_id: object())
    monkeypatch.setattr(mod.social_repo, "_resolve_week_windows", lambda *_args, **_kwargs: ([], None))
    monkeypatch.setattr(mod.social_repo, "_coerce_dt", lambda _value: None)
    monkeypatch.setattr(mod.social_repo, "_week_for_timestamp", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(mod.pg, "db_connection", lambda: nullcontext("conn"))

    enqueued_ids: list[str] = []

    def _fake_enqueue(*_args, **kwargs) -> str:
        post_row = kwargs.get("post_row") or {}
        enqueued_ids.append(str(post_row.get("id")))
        return f"job-{post_row.get('id')}"

    monkeypatch.setattr(mod.social_repo, "_enqueue_platform_media_mirror_job", _fake_enqueue)

    assert mod.main() == 0

    output = capsys.readouterr().out.strip()
    payload = json.loads(output)
    assert payload["hosted_html_only"] is True
    assert payload["totals"] == {"scanned": 2, "queued": 1, "skipped": 1, "failed": 0}
    assert payload["by_platform"]["tiktok"] == {"scanned": 2, "queued": 1, "skipped": 1, "failed": 0}
    assert enqueued_ids == ["tt-1"]
