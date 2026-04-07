from __future__ import annotations

from datetime import UTC, datetime, timedelta

import jwt
import pytest
from fastapi.testclient import TestClient

from api.auth import require_internal_admin
from api.main import app
from trr_backend.repositories import face_references
from trr_backend.services import face_reference_embeddings


@pytest.fixture(autouse=True)
def override_admin():
    app.dependency_overrides[require_internal_admin] = lambda: {
        "id": "service_role:test",
        "role": "service_role",
        "email": "admin@example.com",
    }
    yield
    app.dependency_overrides.pop(require_internal_admin, None)


@pytest.fixture
def client():
    return TestClient(app)


def _make_admin_token(secret: str, subject: str = "admin-1") -> str:
    now = datetime.now(tz=UTC)
    payload = {
        "sub": subject,
        "iat": int(now.timestamp()),
        "nbf": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=5)).timestamp()),
        "role": "service_role",
    }
    return jwt.encode(payload, secret, algorithm="HS256")


def test_list_face_references_by_person(client, monkeypatch):
    monkeypatch.setattr(
        face_references,
        "list_face_reference_images",
        lambda **_kwargs: [
            {
                "id": "ref-1",
                "person_id": "person-1",
                "media_link_id": "link-1",
                "media_asset_id": "asset-1",
                "legacy_screenalytics_face_bank_image_id": None,
                "is_active": True,
                "approved": False,
                "review_status": "pending_review",
                "review_notes": {"source": "seed-toggle"},
                "reviewed_at": None,
                "reviewed_by": None,
                "duplicate_of_reference_image_id": None,
                "embedding_status": "pending",
                "source_url": "https://example.com/source.jpg",
                "hosted_url": "https://cdn.example.com/source.jpg",
                "hosted_sha256": "sha",
                "metadata": {"source": "core.media_links.facebank_seed"},
                "last_enqueued_at": None,
                "deactivated_at": None,
                "created_at": "2026-04-03T00:00:00+00:00",
                "updated_at": "2026-04-03T00:00:00+00:00",
            }
        ],
    )

    response = client.get("/api/v1/admin/face-references/people/11111111-1111-1111-1111-111111111111")

    assert response.status_code == 200
    payload = response.json()
    assert payload["person_id"] == "11111111-1111-1111-1111-111111111111"
    assert payload["items"][0]["review_status"] == "pending_review"


def test_review_face_reference_updates_status(client, monkeypatch):
    monkeypatch.setattr(
        face_references,
        "set_face_reference_review_status",
        lambda **_kwargs: {
            "id": "ref-1",
            "person_id": "person-1",
            "media_link_id": "link-1",
            "media_asset_id": "asset-1",
            "legacy_screenalytics_face_bank_image_id": None,
            "is_active": True,
            "approved": True,
            "review_status": "approved",
            "review_notes": {"decision": "good"},
            "reviewed_at": "2026-04-03T00:00:00+00:00",
            "reviewed_by": "admin@example.com",
            "duplicate_of_reference_image_id": None,
            "embedding_status": "pending",
            "source_url": None,
            "hosted_url": "https://cdn.example.com/source.jpg",
            "hosted_sha256": "sha",
            "metadata": {},
            "last_enqueued_at": None,
            "deactivated_at": None,
            "created_at": "2026-04-03T00:00:00+00:00",
            "updated_at": "2026-04-03T00:00:00+00:00",
        },
    )

    response = client.post(
        "/api/v1/admin/face-references/11111111-1111-1111-1111-111111111111/review",
        json={"review_status": "approved", "review_notes": {"decision": "good"}},
    )

    assert response.status_code == 200
    assert response.json()["review_status"] == "approved"
    assert response.json()["approved"] is True


def test_search_face_references_uses_ready_embedding_from_reference(client, monkeypatch):
    monkeypatch.setattr(
        face_references,
        "resolve_face_reference_image",
        lambda **_kwargs: {"id": "ref-1", "hosted_url": "https://cdn.example.com/source.jpg", "metadata": {}},
    )
    monkeypatch.setattr(
        face_references,
        "get_ready_face_reference_embedding",
        lambda **_kwargs: {"embedding": [1.0, 0.0, 0.0]},
    )
    monkeypatch.setattr(
        face_reference_embeddings,
        "search_reference_matches",
        lambda **_kwargs: {
            "contract_key": face_reference_embeddings.FACE_REFERENCE_EMBEDDING_CONTRACT_KEY,
            "matches": [],
        },
    )

    response = client.post(
        "/api/v1/admin/face-references/search",
        json={"reference_image_id": "11111111-1111-1111-1111-111111111111", "limit": 3},
    )

    assert response.status_code == 200
    assert response.json()["contract_key"] == face_reference_embeddings.FACE_REFERENCE_EMBEDDING_CONTRACT_KEY


def test_verify_face_reference_pair_uses_resolved_sources(client, monkeypatch):
    monkeypatch.setattr(
        face_references,
        "resolve_face_reference_image",
        lambda **kwargs: {
            "id": kwargs.get("reference_image_id"),
            "hosted_url": f"https://cdn.example.com/{kwargs.get('reference_image_id')}.jpg",
            "source_url": None,
        },
    )
    monkeypatch.setattr(
        face_reference_embeddings,
        "verify_reference_pair",
        lambda **_kwargs: {"verified": True, "distance": 0.1},
    )

    response = client.post(
        "/api/v1/admin/face-references/verify",
        json={
            "left_reference_image_id": "11111111-1111-1111-1111-111111111111",
            "right_reference_image_id": "22222222-2222-2222-2222-222222222222",
        },
    )

    assert response.status_code == 200
    assert response.json()["verified"] is True


def test_reembed_face_reference_uses_register_reference_image(client, monkeypatch):
    monkeypatch.setattr(
        face_references,
        "resolve_face_reference_image",
        lambda **_kwargs: {"id": "ref-1", "hosted_url": "https://cdn.example.com/source.jpg", "source_url": None},
    )
    monkeypatch.setattr(
        face_reference_embeddings,
        "register_reference_image",
        lambda **_kwargs: {"id": "emb-1", "embedding_status": "ready"},
    )

    response = client.post("/api/v1/admin/face-references/11111111-1111-1111-1111-111111111111/reembed", json={})

    assert response.status_code == 200
    assert response.json()["embedding_status"] == "ready"
