from __future__ import annotations

import json
from contextlib import nullcontext
from types import SimpleNamespace

import pytest

import scripts.socials.backfill_social_media_mirror_jobs as mod


def _base_args(**overrides):
    values = {
        "weeks": 8,
        "all_history": False,
        "platforms": "instagram,tiktok,youtube,twitter",
        "source_scope": "bravo",
        "limit_per_platform": 5000,
        "season_id": [],
        "show_id": [],
        "season_number": [],
        "post_id": [],
        "source_id": [],
        "failed_only": False,
        "hosted_html_only": False,
        "normalize_only": False,
        "mirror_only": False,
        "repair_all": False,
        "repair_reasons": "",
        "dry_run": False,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_main_fails_fast_when_s3_preflight_fails(monkeypatch) -> None:
    monkeypatch.setattr(mod, "load_env", lambda: None)
    monkeypatch.setattr(mod, "_parse_args", lambda: _base_args())

    def _fail_preflight() -> None:
        raise RuntimeError("Missing required environment variable: OBJECT_STORAGE_BUCKET")

    monkeypatch.setattr(mod.social_repo, "ensure_media_mirror_s3_ready", _fail_preflight)

    with pytest.raises(SystemExit, match="Social media mirror object-storage preflight failed"):
        mod.main()


def test_main_hosted_html_only_filters_non_html_rows(monkeypatch, capsys) -> None:
    monkeypatch.setattr(mod, "load_env", lambda: None)
    monkeypatch.setenv("OBJECT_STORAGE_PUBLIC_BASE_URL", "https://cdn.test")
    mod.social_repo._expected_cdn_host.cache_clear()  # noqa: SLF001
    monkeypatch.setattr(mod, "_parse_args", lambda: _base_args(platforms="tiktok", hosted_html_only=True))
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
    assert payload["totals"] == {"scanned": 2, "eligible": 1, "queued": 1, "skipped": 1, "failed": 0}
    assert payload["by_platform"]["tiktok"] == {
        "scanned": 2,
        "eligible": 1,
        "queued": 1,
        "skipped": 1,
        "failed": 0,
        "repair_reasons": {
            "hosted_content": 1,
            "non_video_hosted_media": 1,
            "missing_source_avatar": 1,
        },
    }
    assert enqueued_ids == ["tt-1"]


def test_main_enqueues_shared_context_when_row_has_no_season(monkeypatch, capsys) -> None:
    monkeypatch.setattr(mod, "load_env", lambda: None)
    monkeypatch.setattr(mod, "_parse_args", lambda: _base_args(platforms="instagram"))
    monkeypatch.setattr(mod.social_repo, "ensure_media_mirror_s3_ready", lambda: None)
    monkeypatch.setattr(
        mod,
        "_load_rows",
        lambda **_kwargs: [
            {
                "id": "ig-1",
                "season_id": "",
                "show_id": "",
                "source_id": "DZNIQIgEoNd",
                "account": "bravotv",
                "posted_at": None,
                "thumbnail_url": "",
                "media_urls": [],
                "hosted_thumbnail_url": "",
                "hosted_media_urls": [],
                "media_mirror_status": "",
            }
        ],
    )
    monkeypatch.setattr(mod, "_row_repair_reasons", lambda *_args, **_kwargs: ["missing_hosted_thumbnail"])
    monkeypatch.setattr(mod.social_repo, "_platform_post_needs_media_mirror", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        mod.social_repo,
        "get_season_context",
        lambda *_args, **_kwargs: pytest.fail("season context should not be required"),
    )
    monkeypatch.setattr(
        mod.social_repo,
        "_resolve_week_windows",
        lambda *_args, **_kwargs: pytest.fail("season windows should not be resolved without a season"),
    )
    monkeypatch.setattr(mod.social_repo, "_coerce_dt", lambda _value: None)
    monkeypatch.setattr(mod.pg, "db_connection", lambda: nullcontext("conn"))

    enqueued: list[dict[str, object]] = []

    def _fake_enqueue(context, **kwargs) -> str:
        enqueued.append({"context": context, **kwargs})
        return "job-shared"

    monkeypatch.setattr(mod.social_repo, "_enqueue_platform_media_mirror_job", _fake_enqueue)

    assert mod.main() == 0

    payload = json.loads(capsys.readouterr().out.strip())
    assert payload["source_scope"] == "bravo"
    assert payload["effective_source_scope"] == "network"
    assert payload["totals"] == {"scanned": 1, "eligible": 1, "queued": 1, "skipped": 0, "failed": 0}
    assert enqueued[0]["context"] is None
    assert enqueued[0]["source_scope"] == "network"
    assert enqueued[0]["week_index"] is None


def test_row_repair_reasons_detects_historical_cleanup_cases(monkeypatch) -> None:
    monkeypatch.setenv("OBJECT_STORAGE_PUBLIC_BASE_URL", "https://pub.example.r2.dev")
    mod.social_repo._expected_cdn_host.cache_clear()  # noqa: SLF001
    try:
        row = {
            "id": "tw-1",
            "tweet_id": "tweet-1",
            "thumbnail_url": "https://video.twimg.com/ext_tw_video/1.mp4",
            "media_urls": [
                "https://video.twimg.com/ext_tw_video/1.mp4",
                "https://pbs.twimg.com/ext_tw_video_thumb/1.jpg",
            ],
            "hosted_thumbnail_url": "https://legacy-cdn.example/social/twitter/x/thumbnail.mp4",
            "hosted_media_urls": ["https://pub.example.r2.dev/social/twitter/x/media-02.jpg"],
            "media_mirror_status": "mirrored",
        }

        assert mod._row_repair_reasons("twitter", row) == [
            "legacy_hosted_url",
            "twitter_video_thumbnail",
            "missing_hosted_media",
            "missing_display_variants",
            "missing_source_avatar",
        ]
    finally:
        mod.social_repo._expected_cdn_host.cache_clear()  # noqa: SLF001


def test_row_repair_reasons_marks_instagram_reresolve_as_mirror_retry() -> None:
    row = {
        "id": "ig-1",
        "shortcode": "C123456",
        "thumbnail_url": "",
        "media_urls": [],
        "hosted_thumbnail_url": "",
        "hosted_media_urls": [],
        "media_mirror_status": "mirrored",
    }

    assert mod._row_repair_reasons("instagram", row) == ["mirror_retry", "missing_source_avatar"]


def test_main_all_history_dry_run_and_reason_filters(monkeypatch, capsys) -> None:
    monkeypatch.setattr(mod, "load_env", lambda: None)
    monkeypatch.setattr(
        mod,
        "_parse_args",
        lambda: _base_args(
            all_history=True,
            platforms="twitter",
            season_id=["season-1"],
            show_id=["show-1"],
            season_number=["6"],
            post_id=["post-1"],
            source_id=["tweet-1"],
            repair_reasons="twitter_video_thumbnail",
            dry_run=True,
        ),
    )
    preflight_called = False

    def _fail_if_called() -> None:
        nonlocal preflight_called
        preflight_called = True
        raise AssertionError("preflight should not run during dry-run")

    monkeypatch.setattr(mod.social_repo, "ensure_media_mirror_s3_ready", _fail_if_called)

    load_calls: list[dict[str, object]] = []

    def _fake_load_rows(**kwargs):
        load_calls.append(kwargs)
        return [
            {
                "id": "post-1",
                "season_id": "season-1",
                "source_id": "tweet-1",
                "account": "bravotv",
                "posted_at": None,
                "thumbnail_url": "https://video.twimg.com/ext_tw_video/1.mp4",
                "media_urls": [
                    "https://video.twimg.com/ext_tw_video/1.mp4",
                    "https://pbs.twimg.com/ext_tw_video_thumb/1.jpg",
                ],
                "hosted_thumbnail_url": "https://pub.example.r2.dev/social/twitter/x/thumbnail.mp4",
                "hosted_media_urls": ["https://pub.example.r2.dev/social/twitter/x/media-02.jpg"],
                "media_mirror_status": "mirrored",
            },
            {
                "id": "post-2",
                "season_id": "season-1",
                "source_id": "tweet-2",
                "account": "bravotv",
                "posted_at": None,
                "thumbnail_url": "https://img.test/thumb.jpg",
                "media_urls": ["https://img.test/thumb.jpg"],
                "hosted_thumbnail_url": "",
                "hosted_media_urls": [],
                "media_mirror_status": "mirrored",
            },
        ]

    monkeypatch.setattr(mod, "_load_rows", _fake_load_rows)
    monkeypatch.setattr(mod.social_repo, "_platform_post_needs_media_mirror", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        mod,
        "_row_repair_reasons",
        lambda _platform, row: (
            ["twitter_video_thumbnail", "missing_hosted_media"]
            if str(row.get("id")) == "post-1"
            else ["missing_hosted_thumbnail"]
        ),
    )
    monkeypatch.setattr(mod.pg, "db_connection", lambda: nullcontext("conn"))
    monkeypatch.setattr(
        mod.social_repo,
        "_enqueue_platform_media_mirror_job",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("dry-run must not enqueue")),
    )

    assert mod.main() == 0

    assert preflight_called is False
    assert load_calls == [
        {
            "platform": "twitter",
            "cutoff": None,
            "limit": 5000,
            "season_ids": ["season-1"],
            "show_ids": ["show-1"],
            "season_numbers": [6],
            "post_ids": ["post-1"],
            "source_ids": ["tweet-1"],
        }
    ]
    payload = json.loads(capsys.readouterr().out.strip())
    assert payload["all_history"] is True
    assert payload["cutoff"] is None
    assert payload["season_ids"] == ["season-1"]
    assert payload["show_ids"] == ["show-1"]
    assert payload["season_numbers"] == [6]
    assert payload["post_ids"] == ["post-1"]
    assert payload["source_ids"] == ["tweet-1"]
    assert payload["repair_reasons"] == ["twitter_video_thumbnail"]
    assert payload["dry_run"] is True
    assert payload["totals"] == {"scanned": 2, "eligible": 1, "queued": 0, "skipped": 1, "failed": 0}
    assert payload["repair_reasons_matched"] == {
        "missing_hosted_media": 1,
        "twitter_video_thumbnail": 1,
    }


def test_parse_repair_reasons_accepts_legacy_host_alias() -> None:
    assert mod._parse_repair_reasons("legacy_host,missing_hosted_media") == {
        "legacy_hosted_url",
        "missing_hosted_media",
    }


def test_row_matches_mode_distinguishes_normalize_only_rows() -> None:
    assert mod._row_matches_mode(["legacy_hosted_url"], normalize_only=True, mirror_only=False) is True
    assert (
        mod._row_matches_mode(
            ["legacy_hosted_url", "missing_hosted_media"],
            normalize_only=True,
            mirror_only=False,
        )
        is False
    )
    assert (
        mod._row_matches_mode(
            ["legacy_hosted_url", "missing_hosted_avatar"],
            normalize_only=False,
            mirror_only=True,
        )
        is True
    )
