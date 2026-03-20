from __future__ import annotations

import json
from types import SimpleNamespace

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
            "hosted_tagged_profile_pics": {"andycohen": "https://cdn.test/andy.jpg"},
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
            "hosted_tagged_profile_pics": {"andycohen": "https://cdn.test/andy.jpg"},
            "profile_pic_mirror_status": "mirrored",
        }
    ]
