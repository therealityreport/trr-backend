"""Tests for admin show Bravo import endpoints."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from uuid import uuid4

import jwt
import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from api.main import app
from api.routers.admin_show_bravo import _dedupe_items, _merge_external_ids_fill_missing


def _make_admin_token(secret: str, subject: str = "admin-1") -> str:
    now = datetime.now(tz=UTC)
    payload = {
        "sub": subject,
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=5)).timestamp()),
        "role": "service_role",
    }
    return jwt.encode(payload, secret, algorithm="HS256")


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


def test_preview_bravo_import_returns_expected_shape(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")

    show_id = str(uuid4())
    mock_db = MagicMock()

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch("api.routers.admin_show_bravo._show_exists", return_value=True):
            with patch("api.routers.admin_show_bravo._assert_show_sync_ready_for_bravo"):
                with patch(
                    "api.routers.admin_show_bravo.parse_bravo_show_bundle",
                    return_value={
                        "show": {"title": "The Valley", "description": "desc"},
                        "people": [{"canonical_url": "https://www.bravotv.com/people/janet-caperna"}],
                        "videos": [{"title": "The Valley Persian Style", "clip_url": "https://www.bravotv.com/v/1"}],
                        "news": [{"headline": "A headline", "article_url": "https://www.bravotv.com/n/1"}],
                        "image_candidates": [{"url": "https://www.bravotv.com/i/1.jpg"}],
                        "discovered_person_urls": ["https://www.bravotv.com/people/janet-caperna"],
                    },
                ):
                    response = client.post(
                        f"/api/v1/admin/shows/{show_id}/import-bravo/preview",
                        headers={"Authorization": f"Bearer {token}"},
                        json={"show_url": "https://www.bravotv.com/the-valley"},
                    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["show"]["title"] == "The Valley"
    assert len(payload["people"]) == 1
    assert len(payload["videos"]) == 1
    assert len(payload["news"]) == 1
    assert len(payload["image_candidates"]) == 1


def test_commit_bravo_import_returns_snapshot_metadata(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")

    show_id = str(uuid4())
    person_id = str(uuid4())
    mock_db = MagicMock()

    bundle = {
        "show": {
            "canonical_url": "https://www.bravotv.com/the-valley",
            "title": "The Valley",
            "description": "Bravo description",
            "airs_text": "Tuesdays 9/8c",
        },
        "videos": [{"title": "The Valley Persian Style", "clip_url": "https://www.bravotv.com/v/1"}],
        "news": [{"headline": "A headline", "article_url": "https://www.bravotv.com/n/1"}],
        "people": [
            {
                "canonical_url": "https://www.bravotv.com/people/janet-caperna",
                "name": "Janet Caperna",
                "bio": "Bio",
                "hero_image_url": "https://www.bravotv.com/p/1.jpg",
                "social_links": {"instagram": "janetcaperna"},
                "videos": [],
                "news": [],
            }
        ],
        "image_candidates": [],
        "discovered_person_urls": ["https://www.bravotv.com/people/janet-caperna"],
        "raw": {},
    }

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch("api.routers.admin_show_bravo._show_exists", return_value=True):
            with patch("api.routers.admin_show_bravo._assert_show_sync_ready_for_bravo"):
                with patch("api.routers.admin_show_bravo.parse_bravo_show_bundle", return_value=bundle):
                    with patch(
                        "api.routers.admin_show_bravo._build_show_cast_index",
                        return_value=[{"person_id": person_id, "person_name": "Janet Caperna"}],
                    ):
                        with patch(
                            "api.routers.admin_show_bravo._upsert_show_snapshot",
                            return_value={"show_id": show_id, "source_id": "bravo", "variant": "default"},
                        ):
                            with patch("api.routers.admin_show_bravo._persist_show_description"):
                                with patch(
                                    "api.routers.admin_show_bravo._upsert_person_snapshot",
                                    return_value={"person_id": person_id, "source_id": "bravo", "variant": "default"},
                                ):
                                    with patch("api.routers.admin_show_bravo._persist_person_profile"):
                                        with patch(
                                            "api.routers.admin_show_bravo._import_bravo_person_image",
                                            return_value={"imported": 1, "skipped": 0, "errors": []},
                                        ):
                                            response = client.post(
                                                f"/api/v1/admin/shows/{show_id}/import-bravo/commit",
                                                headers={"Authorization": f"Bearer {token}"},
                                                json={"show_url": "https://www.bravotv.com/the-valley"},
                                            )

    assert response.status_code == 200
    payload = response.json()
    assert payload["show_snapshot"]["source_id"] == "bravo"
    assert len(payload["person_snapshots"]) == 1
    assert payload["counts"]["people_updated"] == 1


def test_commit_bravo_import_persists_season_overview_for_season_scoped_sync(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")

    show_id = str(uuid4())
    season_id = str(uuid4())
    person_id = str(uuid4())
    mock_db = MagicMock()

    bundle = {
        "show": {
            "canonical_url": "https://www.bravotv.com/the-valley",
            "title": "The Valley",
            "description": "Season-specific Bravo copy",
            "airs_text": "Tuesdays 9/8c",
        },
        "videos": [],
        "news": [],
        "people": [
            {
                "canonical_url": "https://www.bravotv.com/people/janet-caperna",
                "name": "Janet Caperna",
                "bio": "Bio",
                "hero_image_url": None,
                "social_links": {},
                "videos": [],
                "news": [],
            }
        ],
        "image_candidates": [],
        "discovered_person_urls": ["https://www.bravotv.com/people/janet-caperna"],
        "raw": {},
    }

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch("api.routers.admin_show_bravo._show_exists", return_value=True):
            with patch("api.routers.admin_show_bravo._assert_show_sync_ready_for_bravo"):
                with patch("api.routers.admin_show_bravo.parse_bravo_show_bundle", return_value=bundle):
                    with patch(
                        "api.routers.admin_show_bravo._build_show_cast_index",
                        return_value=[{"person_id": person_id, "person_name": "Janet Caperna"}],
                    ):
                        with patch(
                            "api.routers.admin_show_bravo._resolve_season_id",
                            return_value=season_id,
                        ):
                            with patch(
                                "api.routers.admin_show_bravo._upsert_show_snapshot",
                                return_value={"show_id": show_id, "source_id": "bravo", "variant": "default"},
                            ):
                                with patch(
                                    "api.routers.admin_show_bravo._upsert_person_snapshot",
                                    return_value={"person_id": person_id, "source_id": "bravo", "variant": "default"},
                                ):
                                    with patch("api.routers.admin_show_bravo._persist_person_profile"):
                                        with patch(
                                            "api.routers.admin_show_bravo._persist_show_description"
                                        ) as persist_show_description_mock:
                                            with patch(
                                                "api.routers.admin_show_bravo._persist_season_overview"
                                            ) as persist_season_overview_mock:
                                                response = client.post(
                                                    f"/api/v1/admin/shows/{show_id}/import-bravo/commit",
                                                    headers={"Authorization": f"Bearer {token}"},
                                                    json={
                                                        "show_url": "https://www.bravotv.com/the-valley",
                                                        "season_number": 1,
                                                    },
                                                )

    assert response.status_code == 200
    persist_season_overview_mock.assert_called_once()
    persist_show_description_mock.assert_not_called()


def test_commit_bravo_import_uses_selected_show_image_kinds(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")

    show_id = str(uuid4())
    person_id = str(uuid4())
    mock_db = MagicMock()

    bundle = {
        "show": {
            "canonical_url": "https://www.bravotv.com/the-valley",
            "title": "The Valley",
            "description": "Bravo description",
            "airs_text": "Tuesdays 9/8c",
        },
        "videos": [],
        "news": [],
        "people": [
            {
                "canonical_url": "https://www.bravotv.com/people/janet-caperna",
                "name": "Janet Caperna",
                "bio": "Bio",
                "hero_image_url": "https://www.bravotv.com/p/1.jpg",
                "social_links": {},
                "videos": [],
                "news": [],
            }
        ],
        "image_candidates": [],
        "discovered_person_urls": ["https://www.bravotv.com/people/janet-caperna"],
        "raw": {},
    }

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch("api.routers.admin_show_bravo._show_exists", return_value=True):
            with patch("api.routers.admin_show_bravo._assert_show_sync_ready_for_bravo"):
                with patch("api.routers.admin_show_bravo.parse_bravo_show_bundle", return_value=bundle):
                    with patch(
                        "api.routers.admin_show_bravo._build_show_cast_index",
                        return_value=[{"person_id": person_id, "person_name": "Janet Caperna"}],
                    ):
                        with patch(
                            "api.routers.admin_show_bravo._upsert_show_snapshot",
                            return_value={"show_id": show_id, "source_id": "bravo", "variant": "default"},
                        ):
                            with patch("api.routers.admin_show_bravo._persist_show_description"):
                                with patch(
                                    "api.routers.admin_show_bravo._upsert_person_snapshot",
                                    return_value={"person_id": person_id, "source_id": "bravo", "variant": "default"},
                                ):
                                    with patch("api.routers.admin_show_bravo._persist_person_profile"):
                                        with patch(
                                            "api.routers.admin_show_bravo._import_bravo_person_image",
                                            return_value={"imported": 0, "skipped": 0, "errors": []},
                                        ):
                                            with patch(
                                                "api.routers.admin_scrape.import_images",
                                                return_value=SimpleNamespace(
                                                    imported=2,
                                                    skipped_duplicates=0,
                                                    errors=[],
                                                ),
                                            ) as import_images_mock:
                                                response = client.post(
                                                    f"/api/v1/admin/shows/{show_id}/import-bravo/commit",
                                                    headers={"Authorization": f"Bearer {token}"},
                                                    json={
                                                        "show_url": "https://www.bravotv.com/the-valley",
                                                        "selected_show_images": [
                                                            {
                                                                "url": "https://www.bravotv.com/i/logo.png",
                                                                "kind": "logo",
                                                            },
                                                            {
                                                                "url": "https://www.bravotv.com/i/poster.jpg",
                                                                "kind": "poster",
                                                            },
                                                        ],
                                                    },
                                                )

    assert response.status_code == 200
    assert import_images_mock.called
    import_request = import_images_mock.call_args.args[0]
    kinds = [image.kind for image in import_request.images]
    assert kinds == ["logo", "poster"]


def test_preview_bravo_import_filters_videos_by_season(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")

    show_id = str(uuid4())
    mock_db = MagicMock()

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch("api.routers.admin_show_bravo._show_exists", return_value=True):
            with patch("api.routers.admin_show_bravo._assert_show_sync_ready_for_bravo"):
                with patch(
                    "api.routers.admin_show_bravo.parse_bravo_show_bundle",
                    return_value={
                        "show": {"title": "Summer House"},
                        "people": [],
                        "videos": [
                            {"title": "S10 clip", "clip_url": "https://www.bravotv.com/v/10", "season_number": 10},
                            {"title": "S9 clip", "clip_url": "https://www.bravotv.com/v/9", "season_number": 9},
                        ],
                        "news": [],
                        "image_candidates": [],
                        "discovered_person_urls": [],
                    },
                ):
                    response = client.post(
                        f"/api/v1/admin/shows/{show_id}/import-bravo/preview",
                        headers={"Authorization": f"Bearer {token}"},
                        json={"show_url": "https://www.bravotv.com/summer-house", "season_number": 10},
                    )

    assert response.status_code == 200
    payload = response.json()
    assert len(payload["videos"]) == 1
    assert payload["videos"][0]["season_number"] == 10


def test_preview_bravo_import_requires_synced_seasons_episodes_and_cast(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")
    show_id = str(uuid4())
    mock_db = MagicMock()

    with patch("trr_backend.db.admin.create_supabase_admin_client", return_value=mock_db):
        with patch("api.routers.admin_show_bravo._show_exists", return_value=True):
            with patch(
                "api.routers.admin_show_bravo._assert_show_sync_ready_for_bravo",
                side_effect=HTTPException(
                    status_code=409,
                    detail="Sync seasons, episodes, and cast before Bravo import (missing: episodes, cast).",
                ),
            ):
                response = client.post(
                    f"/api/v1/admin/shows/{show_id}/import-bravo/preview",
                    headers={"Authorization": f"Bearer {token}"},
                    json={"show_url": "https://www.bravotv.com/summer-house"},
                )

    assert response.status_code == 409
    assert "Sync seasons, episodes, and cast before Bravo import" in response.json().get("detail", "")


def test_dedupe_items_merges_person_tags_for_duplicate_article() -> None:
    items = [
        {
            "article_url": "https://www.bravotv.com/the-daily-dish/story",
            "headline": "Story",
            "person_tags": [{"person_id": "p1", "person_name": "Person One", "person_url": None}],
        },
        {
            "article_url": "https://www.bravotv.com/the-daily-dish/story",
            "headline": "Story",
            "person_tags": [{"person_id": "p2", "person_name": "Person Two", "person_url": None}],
        },
    ]

    merged = _dedupe_items(items, "article_url", merge_person_tags=True)

    assert len(merged) == 1
    merged_tags = merged[0]["person_tags"]
    assert isinstance(merged_tags, list)
    assert {tag.get("person_id") for tag in merged_tags} == {"p1", "p2"}


def test_external_ids_merge_fill_missing_only() -> None:
    existing = {
        "instagram": "already-set",
        "twitter": "",
    }
    incoming = {
        "instagram": "should-not-overwrite",
        "twitter": "fresh-handle",
        "tiktok": "new-account",
    }

    merged = _merge_external_ids_fill_missing(existing, incoming)

    assert merged["instagram"] == "already-set"
    assert merged["instagram_id"] == "already-set"
    assert merged["twitter"] == "fresh-handle"
    assert merged["twitter_id"] == "fresh-handle"
    assert merged["tiktok"] == "new-account"
    assert merged["tiktok_id"] == "new-account"
    assert merged["tiktok_url"] == "https://www.tiktok.com/@new-account"


def test_external_ids_merge_normalizes_social_urls_to_ids_and_urls() -> None:
    merged = _merge_external_ids_fill_missing(
        existing={},
        incoming={
            "instagram": "https://www.instagram.com/janetcaperna/",
            "twitter": "https://x.com/janetcaperna",
            "youtube": "https://www.youtube.com/@janetcaperna",
        },
    )

    assert merged["instagram"] == "janetcaperna"
    assert merged["instagram_id"] == "janetcaperna"
    assert merged["instagram_url"] == "https://www.instagram.com/janetcaperna"
    assert merged["twitter"] == "janetcaperna"
    assert merged["twitter_id"] == "janetcaperna"
    assert merged["twitter_url"] == "https://x.com/janetcaperna"
    assert merged["youtube"] == "@janetcaperna"
    assert merged["youtube_id"] == "@janetcaperna"
    assert merged["youtube_url"] == "https://www.youtube.com/@janetcaperna"


def test_external_ids_merge_skips_generic_youtube_placeholders() -> None:
    merged = _merge_external_ids_fill_missing(
        existing={},
        incoming={
            "youtube": "https://www.youtube.com/user/",
        },
    )

    assert "youtube" not in merged
    assert "youtube_id" not in merged
    assert "youtube_url" not in merged
