from __future__ import annotations

from typing import Any

from fastapi.testclient import TestClient

from api.auth import require_internal_admin
from api.main import app


def test_comments_scrape_body_dry_run_uses_preview_without_enqueue(monkeypatch) -> None:
    captured_preview_kwargs: dict[str, Any] = {}

    def _preview_social_account_comments_scrape(**kwargs: Any) -> dict[str, Any]:
        captured_preview_kwargs.update(kwargs)
        return {
            "dry_run": True,
            "platform": kwargs["platform"],
            "account_handle": kwargs["account_handle"],
            "mode": kwargs["mode"],
            "target_filter": kwargs["target_filter"],
            "target_source_ids_count": 12,
            "comments_shard_count": 2,
            "comments_load_strategy": kwargs["comments_load_strategy"],
            "comments_session_scope": "public_relay",
            "date_start": kwargs["date_start"],
            "date_end": kwargs["date_end"],
            "sample_target_source_ids": ["DTgXh94kXyo", "DYiDH6pN-1Z"],
        }

    def _start_social_account_comments_scrape(**_kwargs: Any) -> dict[str, Any]:
        raise AssertionError("dry-run comments scrape must not enqueue a run")

    monkeypatch.setattr(
        "trr_backend.socials.pipelines.comments.instagram.preview_social_account_comments_scrape",
        _preview_social_account_comments_scrape,
    )
    monkeypatch.setattr(
        "trr_backend.socials.pipelines.comments.instagram.start_social_account_comments_scrape",
        _start_social_account_comments_scrape,
    )
    app.dependency_overrides[require_internal_admin] = lambda: {
        "email": "admin@example.test",
        "role": "admin",
    }
    try:
        response = TestClient(app).post(
            "/api/v1/admin/socials/profiles/instagram/bravotv/comments/scrape",
            json={
                "mode": "profile",
                "source_scope": "network",
                "refresh_policy": "stale_or_missing",
                "target_filter": "incomplete",
                "max_comments_per_post": 0,
                "comments_load_strategy": "public_relay",
                "date_start": "2026-01-01",
                "date_end": "2027-01-01",
                "dry_run": True,
            },
        )
    finally:
        app.dependency_overrides.pop(require_internal_admin, None)

    assert response.status_code == 200
    assert response.json() == {
        "dry_run": True,
        "platform": "instagram",
        "account_handle": "bravotv",
        "mode": "profile",
        "target_filter": "incomplete",
        "target_source_ids_count": 12,
        "comments_shard_count": 2,
        "comments_load_strategy": "public_relay",
        "comments_session_scope": "public_relay",
        "date_start": "2026-01-01",
        "date_end": "2027-01-01",
        "sample_target_source_ids": ["DTgXh94kXyo", "DYiDH6pN-1Z"],
    }
    assert captured_preview_kwargs == {
        "platform": "instagram",
        "account_handle": "bravotv",
        "mode": "profile",
        "source_id": None,
        "max_posts": None,
        "refresh_policy": "stale_or_missing",
        "target_filter": "incomplete",
        "comments_load_strategy": "public_relay",
        "date_start": "2026-01-01",
        "date_end": "2027-01-01",
    }
