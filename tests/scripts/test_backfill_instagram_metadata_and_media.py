from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

import scripts.socials.backfill_instagram_metadata_and_media as mod
from scripts.socials.backfill_instagram_metadata_and_media import _mirror_is_missing


class _FakeConn:
    def __init__(self) -> None:
        self.commit_calls = 0

    def commit(self) -> None:
        self.commit_calls += 1


class _ConnContext:
    def __init__(self, conn: _FakeConn | None = None) -> None:
        self._conn = conn or _FakeConn()

    def __enter__(self) -> _FakeConn:
        return self._conn

    def __exit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
        del exc_type, exc, tb
        return False


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


def test_metadata_is_missing_or_stale_respects_retry_eligibility(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mod.social_repo, "_instagram_metadata_retry_eligible", lambda **_kwargs: True)

    row = {
        "metadata_scraped_at": datetime(2026, 3, 1, tzinfo=UTC),
        "metadata_source": "api_permalink",
        "post_format": "reel",
        "metadata_error": "checkpoint_required",
        "metadata_last_attempted_at": datetime(2026, 3, 1, tzinfo=UTC),
        "metadata_consecutive_failures": 2,
    }

    assert mod._metadata_is_missing_or_stale(row, stale_before=datetime(2026, 2, 1, tzinfo=UTC)) is True


def test_main_upserts_before_enqueuing_mirror_job(monkeypatch: pytest.MonkeyPatch) -> None:
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
    monkeypatch.setattr(mod.social_repo, "ensure_media_mirror_s3_ready", lambda: None)
    monkeypatch.setattr(
        mod,
        "_load_candidate_rows",
        lambda **_: [
            {
                "id": "db-row-1",
                "shortcode": "ABC123",
                "media_id": "mid-1",
                "username": "creator",
                "caption": "",
                "media_type": "video",
                "media_urls": ["https://src/video.mp4"],
                "thumbnail_url": "https://src/thumb.jpg",
                "likes": 0,
                "comments_count": 0,
                "views": 0,
                "posted_at": datetime(2026, 3, 1, tzinfo=UTC),
                "raw_data": {},
                "source_account": "creator",
                "show_id": "show-1",
                "season_id": "season-1",
                "post_format": "reel",
                "profile_tags": [],
                "collaborators": [],
                "hashtags": [],
                "mentions": [],
                "duration_seconds": None,
                "metadata_source": "api_permalink",
                "metadata_scraped_at": datetime(2026, 3, 1, tzinfo=UTC),
                "metadata_error": None,
                "metadata_last_attempted_at": None,
                "metadata_last_failed_at": None,
                "metadata_consecutive_failures": 0,
                "hosted_thumbnail_url": "",
                "hosted_media_urls": [],
                "media_mirror_status": "",
                "media_mirror_error": None,
            }
        ],
    )
    monkeypatch.setattr(mod.social_repo, "get_season_context", lambda _season_id: SimpleNamespace(season_id="season-1"))
    monkeypatch.setattr(mod.social_repo, "_resolve_week_windows", lambda *args, **kwargs: ([], None))
    monkeypatch.setattr(mod.social_repo, "_load_instagram_cookies", lambda: {"sessionid": "cookie"})
    monkeypatch.setattr(mod.social_repo, "_enrich_instagram_post_from_permalink", lambda **_kwargs: None)
    monkeypatch.setattr(mod.pg, "db_connection", lambda **_kwargs: _ConnContext())

    persisted_row = {"id": "persisted-row-1", "shortcode": "ABC123"}
    upsert_calls: list[str] = []
    enqueue_rows: list[dict[str, object]] = []

    def _fake_upsert(*args, **kwargs):
        upsert_calls.append("upsert")
        return persisted_row

    def _fake_enqueue(*args, **kwargs):
        enqueue_rows.append(dict(kwargs["post_row"]))
        return "mirror-job-1"

    monkeypatch.setattr(mod.social_repo, "_upsert_instagram_post", _fake_upsert)
    monkeypatch.setattr(mod.social_repo, "_enqueue_platform_media_mirror_job", _fake_enqueue)

    assert mod.main() == 0
    assert upsert_calls == ["upsert"]
    assert enqueue_rows == [persisted_row]


def test_main_preserves_existing_reel_classification_on_weak_refresh(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
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
    monkeypatch.setattr(mod.social_repo, "ensure_media_mirror_s3_ready", lambda: None)
    monkeypatch.setattr(
        mod,
        "_load_candidate_rows",
        lambda **_: [
            {
                "id": "db-row-1",
                "shortcode": "ABC123",
                "media_id": "mid-1",
                "username": "creator",
                "caption": "",
                "media_type": "video",
                "media_urls": [],
                "thumbnail_url": "",
                "likes": 0,
                "comments_count": 0,
                "views": 0,
                "posted_at": datetime(2026, 3, 1, tzinfo=UTC),
                "raw_data": {},
                "source_account": "creator",
                "show_id": "show-1",
                "season_id": "season-1",
                "post_format": "reel",
                "profile_tags": [],
                "collaborators": [],
                "hashtags": [],
                "mentions": [],
                "duration_seconds": None,
                "metadata_source": "",
                "metadata_scraped_at": None,
                "metadata_error": None,
                "metadata_last_attempted_at": None,
                "metadata_last_failed_at": None,
                "metadata_consecutive_failures": 0,
                "hosted_thumbnail_url": "",
                "hosted_media_urls": [],
                "media_mirror_status": "mirrored",
                "media_mirror_error": None,
            }
        ],
    )
    monkeypatch.setattr(mod.social_repo, "get_season_context", lambda _season_id: SimpleNamespace(season_id="season-1"))
    monkeypatch.setattr(mod.social_repo, "_resolve_week_windows", lambda *args, **kwargs: ([], None))
    monkeypatch.setattr(mod.social_repo, "_load_instagram_cookies", lambda: {"sessionid": "cookie"})
    monkeypatch.setattr(mod.pg, "db_connection", lambda **_kwargs: _ConnContext())

    def _fake_enrich(*, post, **_kwargs) -> None:
        post.post_format = "post"
        post.metadata_source = "open_graph"

    captured_post_formats: list[str] = []

    def _fake_upsert(*args, **kwargs):
        captured_post_formats.append(kwargs["post"].post_format)
        return {"id": "persisted-row-1"}

    monkeypatch.setattr(mod.social_repo, "_enrich_instagram_post_from_permalink", _fake_enrich)
    monkeypatch.setattr(mod.social_repo, "_upsert_instagram_post", _fake_upsert)

    assert mod.main() == 0
    assert captured_post_formats == ["reel"]


def test_main_uses_shared_connection_and_commit_batches(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mod, "load_env", lambda: None)
    monkeypatch.setattr(
        mod,
        "_parse_args",
        lambda: SimpleNamespace(
            weeks=8,
            limit=None,
            source_scope="bravo",
            metadata_stale_hours=168,
            commit_batch_size=2,
            dry_run=False,
        ),
    )
    monkeypatch.setattr(mod.social_repo, "ensure_media_mirror_s3_ready", lambda: None)

    def _row(index: int) -> dict[str, object]:
        return {
            "id": f"db-row-{index}",
            "shortcode": f"ABC{index}",
            "media_id": f"mid-{index}",
            "username": "creator",
            "caption": "",
            "media_type": "video",
            "media_urls": [f"https://src/video-{index}.mp4"],
            "thumbnail_url": f"https://src/thumb-{index}.jpg",
            "likes": 0,
            "comments_count": 0,
            "views": 0,
            "posted_at": datetime(2026, 3, 1, tzinfo=UTC),
            "raw_data": {},
            "source_account": "creator",
            "show_id": "show-1",
            "season_id": "season-1",
            "post_format": "reel",
            "profile_tags": [],
            "collaborators": [],
            "hashtags": [],
            "mentions": [],
            "duration_seconds": None,
            "metadata_source": "api_permalink",
            "metadata_scraped_at": datetime(2026, 3, 1, tzinfo=UTC),
            "metadata_error": None,
            "metadata_last_attempted_at": None,
            "metadata_last_failed_at": None,
            "metadata_consecutive_failures": 0,
            "hosted_thumbnail_url": "",
            "hosted_media_urls": [],
            "media_mirror_status": "",
            "media_mirror_error": None,
        }

    monkeypatch.setattr(mod, "_load_candidate_rows", lambda **_: [_row(1), _row(2), _row(3)])
    monkeypatch.setattr(mod.social_repo, "get_season_context", lambda _season_id: SimpleNamespace(season_id="season-1"))
    monkeypatch.setattr(mod.social_repo, "_resolve_week_windows", lambda *args, **kwargs: ([], None))
    monkeypatch.setattr(mod.social_repo, "_load_instagram_cookies", lambda: {"sessionid": "cookie"})
    monkeypatch.setattr(mod.social_repo, "_enrich_instagram_post_from_permalink", lambda **_kwargs: None)

    conn = _FakeConn()
    monkeypatch.setattr(mod.pg, "db_connection", lambda **_kwargs: _ConnContext(conn))

    upsert_conns: list[object] = []
    enqueue_conns: list[object] = []

    def _fake_upsert(*args, **kwargs):
        upsert_conns.append(kwargs.get("conn"))
        post = kwargs["post"]
        return {"id": f"persisted-{post.shortcode}", "shortcode": post.shortcode}

    def _fake_enqueue(*args, **kwargs):
        enqueue_conns.append(kwargs.get("conn"))
        return f"mirror-job-{len(enqueue_conns)}"

    monkeypatch.setattr(mod.social_repo, "_upsert_instagram_post", _fake_upsert)
    monkeypatch.setattr(mod.social_repo, "_enqueue_platform_media_mirror_job", _fake_enqueue)

    assert mod.main() == 0
    assert upsert_conns == [conn, conn, conn]
    assert enqueue_conns == [conn, conn, conn]
    assert conn.commit_calls == 2
