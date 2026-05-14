from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

import scripts.socials.backfill_instagram_profile_avatars as mod


def _base_args(**overrides):
    values = {
        "weeks": 8,
        "all_history": False,
        "season_id": [],
        "show_id": [],
        "post_id": [],
        "source_id": [],
        "account": [],
        "limit": 1000,
        "source_scope": "bravo",
        "dry_run": True,
        "apply": False,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _base_row(**overrides):
    row = {
        "id": "post-1",
        "shortcode": "ABC123",
        "media_id": "123",
        "username": "bravotv",
        "caption": "Caption",
        "media_type": "post",
        "media_urls": [],
        "thumbnail_url": None,
        "likes": 1,
        "comments_count": 2,
        "views": 0,
        "posted_at": None,
        "raw_data": {},
        "source_account": "bravotv",
        "show_id": "show-1",
        "season_id": "season-1",
        "post_format": "post",
        "profile_tags": [],
        "collaborators": [],
        "hashtags": [],
        "mentions": [],
        "duration_seconds": None,
        "metadata_source": None,
        "metadata_scraped_at": None,
        "metadata_error": None,
        "owner_profile_pic_url": "https://images.test/source-avatar.jpg",
        "tagged_users_detail": [],
        "collaborators_detail": [],
        "hosted_owner_profile_pic_url": "",
        "hosted_tagged_profile_pics": {},
        "profile_pic_mirror_status": "",
        "profile_pic_mirror_error": None,
    }
    row.update(overrides)
    return row


def test_needs_avatar_backfill_detects_missing_owner_or_tagged_targets() -> None:
    assert mod._needs_avatar_backfill(_base_row()) is True
    assert (
        mod._needs_avatar_backfill(
            _base_row(
                hosted_owner_profile_pic_url="https://cdn.test/avatar.jpg",
                profile_pic_mirror_status="mirrored",
            )
        )
        is False
    )
    assert (
        mod._needs_avatar_backfill(
            _base_row(
                hosted_owner_profile_pic_url="https://cdn.test/avatar.jpg",
                profile_pic_mirror_status="mirrored",
                mentions=["@andycohen"],
                hosted_tagged_profile_pics={},
            )
        )
        is True
    )


def test_media_profile_pic_mirror_skips_comment_avatar_scan_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SOCIAL_INSTAGRAM_MEDIA_MIRROR_COMMENT_AVATARS", raising=False)
    monkeypatch.setattr(mod.social_repo, "_column_exists", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        mod.social_repo.pg,
        "fetch_all",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("comment avatar scan should be opt-in for media mirror jobs")
        ),
    )

    row = _base_row(owner_profile_pic_url=None, raw_data={}, mentions=[])
    post = SimpleNamespace(owner_detail=None, tagged_users_detail=[], collaborators_detail=[], mentions=[])

    result = mod.social_repo._mirror_instagram_profile_pics_for_post(row, post=post)  # noqa: SLF001

    assert result["profile_pic_mirror_status"] == "skipped"


def test_main_dry_run_skips_preflight_and_reports_counts(monkeypatch, capsys) -> None:
    monkeypatch.setattr(mod, "load_env", lambda: None)
    monkeypatch.setattr(mod, "_parse_args", lambda _argv: _base_args())
    monkeypatch.setattr(mod, "_load_candidate_rows", lambda **_kwargs: [_base_row()])
    monkeypatch.setattr(mod, "InstagramScraper", lambda cookies: object())
    monkeypatch.setattr(mod.social_repo, "_load_instagram_cookies", lambda: {})
    monkeypatch.setattr(mod, "_populate_avatar_details_from_instagram", lambda **_kwargs: True)
    monkeypatch.setattr(
        mod.social_repo,
        "_mirror_instagram_profile_pics_for_post",
        lambda *_args, **_kwargs: {
            "hosted_owner_profile_pic_url": "https://cdn.test/avatar.jpg",
            "hosted_tagged_profile_pics": {},
            "profile_pic_mirror_status": "mirrored",
            "profile_pic_mirror_error": None,
        },
    )

    preflight_called = False

    def _fail_if_called() -> None:
        nonlocal preflight_called
        preflight_called = True
        raise AssertionError("preflight should not run during dry-run")

    monkeypatch.setattr(mod.social_repo, "ensure_media_mirror_s3_ready", _fail_if_called)
    monkeypatch.setattr(
        mod.social_repo,
        "_upsert_instagram_post",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("dry-run must not persist")),
    )

    assert mod.main([]) == 0

    payload = json.loads(capsys.readouterr().out.strip())
    assert preflight_called is False
    assert payload["dry_run"] is True
    assert payload["totals"] == {
        "scanned": 1,
        "eligible": 1,
        "enriched": 1,
        "mirrored": 1,
        "partial": 0,
        "failed": 0,
        "skipped": 0,
    }


def test_main_apply_persists_backfilled_avatar_fields(monkeypatch, capsys) -> None:
    monkeypatch.setattr(mod, "load_env", lambda: None)
    monkeypatch.setattr(mod, "_parse_args", lambda _argv: _base_args(dry_run=False, apply=True))
    monkeypatch.setattr(mod, "_load_candidate_rows", lambda **_kwargs: [_base_row()])
    monkeypatch.setattr(mod, "InstagramScraper", lambda cookies: object())
    monkeypatch.setattr(mod.social_repo, "_load_instagram_cookies", lambda: {})
    monkeypatch.setattr(mod, "_populate_avatar_details_from_instagram", lambda **_kwargs: False)
    monkeypatch.setattr(mod.social_repo, "ensure_media_mirror_s3_ready", lambda: None)
    monkeypatch.setattr(
        mod.social_repo,
        "_mirror_instagram_profile_pics_for_post",
        lambda *_args, **_kwargs: {
            "hosted_owner_profile_pic_url": "https://cdn.test/avatar.jpg",
            "hosted_tagged_profile_pics": {
                "andycohen": {
                    "hosted_url": "https://cdn.test/andy.jpg",
                    "sha256": None,
                    "mirrored_at": "2026-04-21T00:00:00+00:00",
                }
            },
            "profile_pic_mirror_status": "mirrored",
            "profile_pic_mirror_error": None,
        },
    )
    monkeypatch.setattr(
        mod.social_repo,
        "get_season_context",
        lambda _season_id: SimpleNamespace(show_id="show-1"),
    )

    upsert_calls: list[dict[str, object]] = []

    def _fake_upsert(context, *, job_id, account, post, conn=None):
        del context, conn
        upsert_calls.append(
            {
                "job_id": job_id,
                "account": account,
                "hosted_owner_profile_pic_url": post.hosted_owner_profile_pic_url,
                "hosted_tagged_profile_pics": post.hosted_tagged_profile_pics,
                "profile_pic_mirror_status": post.profile_pic_mirror_status,
            }
        )
        return {"id": "post-1"}

    monkeypatch.setattr(mod.social_repo, "_upsert_instagram_post", _fake_upsert)

    assert mod.main([]) == 0

    payload = json.loads(capsys.readouterr().out.strip())
    assert payload["dry_run"] is False
    assert payload["totals"]["mirrored"] == 1
    assert upsert_calls == [
        {
            "job_id": None,
            "account": "bravotv",
            "hosted_owner_profile_pic_url": "https://cdn.test/avatar.jpg",
            "hosted_tagged_profile_pics": {
                "andycohen": {
                    "hosted_url": "https://cdn.test/andy.jpg",
                    "sha256": None,
                    "mirrored_at": "2026-04-21T00:00:00+00:00",
                }
            },
            "profile_pic_mirror_status": "mirrored",
        }
    ]


def test_main_apply_fails_fast_when_s3_preflight_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mod, "load_env", lambda: None)
    monkeypatch.setattr(mod, "_parse_args", lambda _argv: _base_args(dry_run=False, apply=True))

    def _fail_preflight() -> None:
        raise RuntimeError("Missing required environment variable: OBJECT_STORAGE_BUCKET")

    monkeypatch.setattr(mod.social_repo, "ensure_media_mirror_s3_ready", _fail_preflight)

    with pytest.raises(SystemExit, match="Instagram profile avatar mirror S3 preflight failed"):
        mod.main([])


def test_stale_unsupported_avatar_registry_entries_are_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    fixed_now = datetime(2026, 4, 22, 12, 0, tzinfo=UTC)
    monkeypatch.setenv("SOCIAL_AVATAR_SKIP_TTL_HOURS", "1")
    monkeypatch.setattr(mod.social_repo, "_now_utc", lambda: fixed_now)
    monkeypatch.setattr(mod.social_repo, "_avatar_registry_ready", lambda **_kwargs: True)
    monkeypatch.setattr(
        mod.social_repo.pg,
        "fetch_all",
        lambda *_args, **_kwargs: [
            {
                "id": "registry-1",
                "platform": "instagram",
                "account_handle": "andycohen",
                "source_url": None,
                "content_hash": None,
                "hosted_url": None,
                "status": "unsupported",
                "failure_reason": "profile_pic_not_available",
                "last_checked_at": fixed_now - timedelta(hours=2),
            }
        ],
    )

    fetch_calls: list[str] = []

    class _FakeScraper:
        def fetch_profile_info(self, handle: str, delay: float = 0.0) -> dict[str, object]:
            del delay
            fetch_calls.append(handle)
            return {}

        def _extract_profile_avatar_from_profile_payload(self, payload):  # noqa: ANN001
            del payload
            return None

    monkeypatch.setattr(
        mod.social_repo,
        "_build_instagram_scraper_with_auth_fallback",
        lambda **_kwargs: _FakeScraper(),
    )
    monkeypatch.setattr(mod.social_repo, "_upsert_avatar_registry_entry", lambda **_kwargs: None)

    row = _base_row(
        id="",
        owner_profile_pic_url=None,
        mentions=["@andycohen"],
    )
    post = mod._AvatarBackfillPost(row)

    mod.social_repo._mirror_instagram_profile_pics_for_post(row, post=post)  # noqa: SLF001

    assert fetch_calls == ["andycohen"]


def test_oversized_avatar_download_is_marked_non_retryable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOCIAL_AVATAR_MIRROR_MAX_BYTES", "1024")
    monkeypatch.setattr(mod.social_repo, "_avatar_registry_lookup", lambda **_kwargs: None)

    upsert_calls: list[dict[str, object]] = []
    monkeypatch.setattr(mod.social_repo, "_upsert_avatar_registry_entry", lambda **kwargs: upsert_calls.append(kwargs))

    class _FakeStreamResponse:
        headers = {"Content-Type": "image/jpeg"}

        def __enter__(self) -> _FakeStreamResponse:
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
            del exc_type, exc, tb
            return False

        def raise_for_status(self) -> None:
            return None

        def iter_content(self, chunk_size: int = 0):
            del chunk_size
            yield b"x" * 2048

    class _FakeS3Client:
        def upload_fileobj(self, fileobj, bucket: str, key: str, ExtraArgs=None) -> None:  # noqa: N803
            del fileobj, bucket, key, ExtraArgs
            raise AssertionError("oversized avatar should not upload")

    monkeypatch.setattr(mod.social_repo.requests, "get", lambda *args, **kwargs: _FakeStreamResponse())
    monkeypatch.setattr("trr_backend.media.s3_mirror.get_s3_client", lambda: _FakeS3Client())
    monkeypatch.setattr("trr_backend.media.s3_mirror.get_s3_bucket", lambda: "bucket")
    monkeypatch.setattr("trr_backend.media.s3_mirror.build_hosted_url", lambda key: f"https://cdn.test/{key}")
    monkeypatch.setattr(
        "trr_backend.media.s3_mirror.build_instagram_profile_pic_s3_key",
        lambda username, sha256, ext: f"avatars/{username}/{sha256}{ext}",
    )
    monkeypatch.setattr("trr_backend.media.s3_mirror.guess_ext_from_content_type", lambda _content_type: ".jpg")

    row = _base_row(id="")
    post = mod._AvatarBackfillPost(row)
    post.owner_detail = SimpleNamespace(username="bravotv", profile_pic_url="https://images.test/source-avatar.jpg")

    result = mod.social_repo._mirror_instagram_profile_pics_for_post(row, post=post)  # noqa: SLF001

    assert result["profile_pic_mirror_status"] == "failed"
    assert "asset_too_large" in str(result["profile_pic_mirror_error"] or "")
    assert upsert_calls == [
        {
            "platform": "instagram",
            "account_handle": "bravotv",
            "source_url": "https://images.test/source-avatar.jpg",
            "status": "unsupported",
            "failure_reason": "asset_too_large",
        }
    ]


def test_invalid_avatar_url_is_marked_unsupported(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mod.social_repo, "_avatar_registry_lookup", lambda **_kwargs: None)

    upsert_calls: list[dict[str, object]] = []
    monkeypatch.setattr(mod.social_repo, "_upsert_avatar_registry_entry", lambda **kwargs: upsert_calls.append(kwargs))
    monkeypatch.setattr(
        mod.social_repo.requests,
        "get",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("invalid avatar URL should not request")),
    )

    row = _base_row(id="")
    post = mod._AvatarBackfillPost(row)
    post.owner_detail = SimpleNamespace(username="bravotv", profile_pic_url="not-a-url")

    result = mod.social_repo._mirror_instagram_profile_pics_for_post(row, post=post)  # noqa: SLF001

    assert result["profile_pic_mirror_status"] == "failed"
    assert "invalid_source_url" in str(result["profile_pic_mirror_error"] or "")
    assert upsert_calls == [
        {
            "platform": "instagram",
            "account_handle": "bravotv",
            "source_url": "not-a-url",
            "status": "unsupported",
            "failure_reason": "invalid_source_url",
        }
    ]


def test_cached_unsupported_invalid_avatar_url_is_skipped(monkeypatch: pytest.MonkeyPatch) -> None:
    fixed_now = datetime(2026, 4, 22, 12, 0, tzinfo=UTC)
    monkeypatch.setenv("SOCIAL_AVATAR_SKIP_TTL_HOURS", "1")
    monkeypatch.setattr(mod.social_repo, "_now_utc", lambda: fixed_now)
    monkeypatch.setattr(
        mod.social_repo,
        "_avatar_registry_lookup",
        lambda **_kwargs: {
            "status": "unsupported",
            "failure_reason": "invalid_source_url",
            "last_checked_at": fixed_now,
        },
    )

    upsert_calls: list[dict[str, object]] = []
    monkeypatch.setattr(mod.social_repo, "_upsert_avatar_registry_entry", lambda **kwargs: upsert_calls.append(kwargs))
    monkeypatch.setattr(
        mod.social_repo.requests,
        "get",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("cached invalid avatar URL should not request")),
    )

    row = _base_row(id="")
    post = mod._AvatarBackfillPost(row)
    post.owner_detail = SimpleNamespace(username="bravotv", profile_pic_url="not-a-url")

    result = mod.social_repo._mirror_instagram_profile_pics_for_post(row, post=post)  # noqa: SLF001

    assert result["profile_pic_mirror_status"] == "mirrored"
    assert result["profile_pic_mirror_error"] is None
    assert upsert_calls == []


def test_tagged_avatar_writer_preserves_sha256_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mod.social_repo, "_avatar_registry_lookup", lambda **_kwargs: None)
    monkeypatch.setattr(mod.social_repo, "_avatar_registry_lookup_any", lambda **_kwargs: None)
    monkeypatch.setattr(mod.social_repo, "_upsert_avatar_registry_entry", lambda **_kwargs: None)

    class _FakeStreamResponse:
        headers = {"Content-Type": "image/jpeg"}

        def __enter__(self) -> _FakeStreamResponse:
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:  # noqa: ANN001
            del exc_type, exc, tb
            return False

        def raise_for_status(self) -> None:
            return None

        def iter_content(self, chunk_size: int = 0):
            del chunk_size
            yield b"friend-avatar-bytes"

    class _FakeS3Client:
        def upload_fileobj(self, fileobj, bucket: str, key: str, ExtraArgs=None) -> None:  # noqa: N803
            del fileobj, bucket, key, ExtraArgs
            return None

    monkeypatch.setattr(mod.social_repo.requests, "get", lambda *args, **kwargs: _FakeStreamResponse())
    monkeypatch.setattr("trr_backend.media.s3_mirror.get_s3_client", lambda: _FakeS3Client())
    monkeypatch.setattr("trr_backend.media.s3_mirror.get_s3_bucket", lambda: "bucket")
    monkeypatch.setattr("trr_backend.media.s3_mirror.build_hosted_url", lambda key: f"https://cdn.test/{key}")
    monkeypatch.setattr(
        "trr_backend.media.s3_mirror.build_instagram_profile_pic_s3_key",
        lambda username, sha256, ext: f"avatars/{username}/{sha256}{ext}",
    )
    monkeypatch.setattr("trr_backend.media.s3_mirror.guess_ext_from_content_type", lambda _content_type: ".jpg")
    monkeypatch.setattr(mod.social_repo, "_now_utc", lambda: datetime(2026, 4, 22, 12, 0, tzinfo=UTC))

    row = _base_row(id="")
    post = mod._AvatarBackfillPost(row)
    post.tagged_users_detail = [SimpleNamespace(username="friend", profile_pic_url="https://images.test/friend.jpg")]

    result = mod.social_repo._mirror_instagram_profile_pics_for_post(row, post=post)  # noqa: SLF001

    expected_sha = hashlib.sha256(b"friend-avatar-bytes").hexdigest()
    assert result["hosted_tagged_profile_pics"] == {
        "friend": {
            "hosted_url": f"https://cdn.test/avatars/friend/{expected_sha}.jpg",
            "sha256": expected_sha,
            "mirrored_at": "2026-04-22T12:00:00+00:00",
        }
    }
