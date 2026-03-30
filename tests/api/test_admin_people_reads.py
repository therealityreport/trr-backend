from __future__ import annotations

import math

import pytest
from fastapi.testclient import TestClient

from api.auth import require_internal_admin
from api.main import app
from api.routers import admin_people_reads as router_module


@pytest.fixture(autouse=True)
def override_admin():
    app.dependency_overrides[require_internal_admin] = lambda: {
        "id": "service_role:test",
        "role": "service_role",
    }
    yield
    app.dependency_overrides.pop(require_internal_admin, None)


@pytest.fixture(autouse=True)
def clear_cache():
    router_module.invalidate_person_read_cache()
    yield
    router_module.invalidate_person_read_cache()


def test_resolve_person_slug_returns_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        router_module.people_repo,
        "resolve_person_slug",
        lambda slug, show_input: (
            {
                "person_id": "person-1",
                "slug": slug,
                "canonical_slug": "brandi-glanville--abc12345",
            },
            "show-1",
            2,
        ),
    )

    client = TestClient(app)
    response = client.get(
        "/api/v1/admin/people/resolve-slug",
        params={"slug": "brandi-glanville", "show_slug": "rhobh"},
    )

    assert response.status_code == 200
    assert response.json() == {
        "resolved": {
            "person_id": "person-1",
            "slug": "brandi-glanville",
            "canonical_slug": "brandi-glanville--abc12345",
        },
        "show_id": "show-1",
    }


def test_get_person_detail_returns_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        router_module.people_repo,
        "get_person_detail",
        lambda person_id: (
            {
                "id": person_id,
                "full_name": "Brandi Glanville",
                "known_for": "RHOBH",
                "external_ids": {"imdb": "nm123"},
                "birthday": "1972-11-16",
                "gender": "female",
                "biography": "bio",
                "place_of_birth": "Salinas, California",
                "homepage": "https://example.com",
                "profile_image_url": "https://cdn.example.com/profile.jpg",
                "alternative_names": ["Brandi"],
            },
            1,
        ),
    )

    client = TestClient(app)
    response = client.get("/api/v1/admin/people/person-1")

    assert response.status_code == 200
    assert response.json() == {
        "person": {
            "id": "person-1",
            "full_name": "Brandi Glanville",
            "known_for": "RHOBH",
            "external_ids": {"imdb": "nm123"},
            "birthday": "1972-11-16",
            "gender": "female",
            "biography": "bio",
            "place_of_birth": "Salinas, California",
            "homepage": "https://example.com",
            "profile_image_url": "https://cdn.example.com/profile.jpg",
            "alternative_names": ["Brandi"],
        }
    }


def test_get_cover_photo_returns_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        router_module.people_repo,
        "get_person_cover_photo",
        lambda person_id: (
            {
                "person_id": person_id,
                "photo_id": "photo-1",
                "photo_url": "https://cdn.example.com/photo.jpg",
            },
            1,
        ),
    )

    client = TestClient(app)
    response = client.get("/api/v1/admin/people/person-1/cover-photo")

    assert response.status_code == 200
    assert response.json() == {
        "coverPhoto": {
            "person_id": "person-1",
            "photo_id": "photo-1",
            "photo_url": "https://cdn.example.com/photo.jpg",
        }
    }


def test_get_gallery_returns_narrow_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        router_module.people_repo,
        "get_person_gallery_page",
        lambda *args, **kwargs: (
            {
                "photos": [
                    {
                        "id": "photo-1",
                        "person_id": "person-1",
                        "source": "imdb",
                        "url": "https://example.com/source.jpg",
                        "hosted_url": "https://cdn.example.com/photo.jpg",
                        "hosted_content_type": "image/jpeg",
                        "caption": "caption",
                        "width": 800,
                        "height": 600,
                        "thumbnail_focus_x": 0.4,
                        "thumbnail_focus_y": 0.6,
                        "thumbnail_zoom": 1.2,
                        "thumbnail_crop_mode": "face",
                        "people_count": 1,
                        "people_count_source": "manual",
                        "face_boxes": [],
                        "face_crops": [],
                        "bucket_type": "gallery",
                        "bucket_key": "bucket-1",
                        "bucket_label": "Gallery",
                        "resolved_show_id": "show-1",
                        "resolved_show_name": "Bravo Show",
                        "media_asset_id": "asset-1",
                        "origin": "media_links",
                        "source_page_url": "https://example.com/page",
                    }
                ],
                "pagination": {
                    "limit": 120,
                    "offset": 0,
                    "count": 1,
                    "next_offset": 1,
                    "has_more": False,
                },
            },
            2,
        ),
    )

    client = TestClient(app)
    response = client.get("/api/v1/admin/people/person-1/gallery", params={"limit": 120, "offset": 0})

    assert response.status_code == 200
    payload = response.json()
    assert payload["pagination"] == {
        "limit": 120,
        "offset": 0,
        "count": 1,
        "next_offset": 1,
        "has_more": False,
    }
    assert payload["photos"][0] == {
        "id": "photo-1",
        "person_id": "person-1",
        "source": "imdb",
        "url": "https://example.com/source.jpg",
        "hosted_url": "https://cdn.example.com/photo.jpg",
        "hosted_content_type": "image/jpeg",
        "caption": "caption",
        "width": 800,
        "height": 600,
        "thumbnail_focus_x": 0.4,
        "thumbnail_focus_y": 0.6,
        "thumbnail_zoom": 1.2,
        "thumbnail_crop_mode": "face",
        "people_count": 1,
        "people_count_source": "manual",
        "face_boxes": [],
        "face_crops": [],
        "bucket_type": "gallery",
        "bucket_key": "bucket-1",
        "bucket_label": "Gallery",
        "resolved_show_id": "show-1",
        "resolved_show_name": "Bravo Show",
        "media_asset_id": "asset-1",
        "origin": "media_links",
        "source_page_url": "https://example.com/page",
    }


def test_get_gallery_sanitizes_non_finite_thumbnail_crop_values(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, list[object]]] = []

    def fake_fetch_all(query: str, params: list[object]):
        calls.append((query, params))
        if len(calls) == 1:
            return [
                {
                    "id": "photo-1",
                    "person_id": "person-1",
                    "source": "imdb",
                    "url": "https://example.com/source.jpg",
                    "hosted_url": "https://cdn.example.com/photo.jpg",
                    "hosted_content_type": "image/jpeg",
                    "caption": "caption",
                    "width": 800,
                    "height": 600,
                    "source_page_url": None,
                    "thumbnail_crop": {"focus_x": math.nan, "focus_y": math.inf, "zoom": 1.2, "mode": "face"},
                    "metadata_people_count": None,
                    "metadata_people_count_source": None,
                    "face_boxes": [],
                    "face_crops": [],
                    "bucket_type": None,
                    "bucket_key": None,
                    "bucket_label": None,
                    "resolved_show_id": None,
                    "resolved_show_name": None,
                    "gallery_status": None,
                    "people_count": None,
                    "people_count_source": None,
                }
            ]
        return []

    monkeypatch.setattr(router_module.people_repo.pg, "fetch_all", fake_fetch_all)

    client = TestClient(app)
    response = client.get("/api/v1/admin/people/person-1/gallery")

    assert response.status_code == 200
    payload = response.json()
    assert payload["photos"][0]["thumbnail_focus_x"] is None
    assert payload["photos"][0]["thumbnail_focus_y"] is None
    assert payload["photos"][0]["thumbnail_zoom"] == 1.2


def test_invalidate_person_cache_clears_backend_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"count": 0}

    def fake_detail(person_id: str):
        calls["count"] += 1
        return (
            {
                "id": person_id,
                "full_name": "Brandi Glanville",
                "known_for": None,
                "external_ids": {},
                "birthday": None,
                "gender": None,
                "biography": None,
                "place_of_birth": None,
                "homepage": None,
                "profile_image_url": None,
                "alternative_names": [],
            },
            1,
        )

    monkeypatch.setattr(router_module.people_repo, "get_person_detail", fake_detail)

    client = TestClient(app)
    client.get("/api/v1/admin/people/person-1")
    client.post("/api/v1/admin/people/person-1/cache/invalidate")
    client.get("/api/v1/admin/people/person-1")

    assert calls["count"] == 2
