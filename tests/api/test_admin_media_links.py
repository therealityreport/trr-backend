from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from api.auth import require_internal_admin
from api.main import app
from api.routers import admin_media_links as router_module


@pytest.fixture(autouse=True)
def override_admin():
    app.dependency_overrides[require_internal_admin] = lambda: {
        "id": "service_role:test",
        "role": "service_role",
    }
    yield
    app.dependency_overrides.pop(require_internal_admin, None)


def test_sync_media_link_tags_returns_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, dict[str, object]]] = []

    def fake_sync(link_id: str, payload: dict[str, object]) -> dict[str, object]:
        calls.append((link_id, payload))
        return {
            "people_names": ["Person One"],
            "people_ids": ["person-1"],
            "people_count": 2,
            "people_count_source": "manual",
            "face_boxes": [
                {
                    "index": 1,
                    "kind": "face",
                    "x": 0.1,
                    "y": 0.2,
                    "width": 0.3,
                    "height": 0.4,
                    "confidence": None,
                    "person_name": "Person One",
                }
            ],
        }

    monkeypatch.setattr(router_module.media_link_tags_repo, "sync_media_link_tags", fake_sync)

    response = TestClient(app).put(
        "/api/v1/admin/media-links/link-1/tags",
        json={
            "people": [{"id": "person-1", "name": "Person One"}],
            "people_count": 2,
            "face_boxes": [{"x": 0.1, "y": 0.2, "width": 0.3, "height": 0.4}],
        },
    )

    assert response.status_code == 200
    assert calls == [
        (
            "link-1",
            {
                "people": [{"id": "person-1", "name": "Person One"}],
                "people_count": 2,
                "face_boxes": [{"x": 0.1, "y": 0.2, "width": 0.3, "height": 0.4}],
            },
        )
    ]
    assert response.json() == {
        "people_names": ["Person One"],
        "people_ids": ["person-1"],
        "people_count": 2,
        "people_count_source": "manual",
        "face_boxes": [
            {
                "index": 1,
                "kind": "face",
                "x": 0.1,
                "y": 0.2,
                "width": 0.3,
                "height": 0.4,
                "confidence": None,
                "person_name": "Person One",
            }
        ],
    }


def test_sync_media_link_tags_maps_missing_link(monkeypatch: pytest.MonkeyPatch) -> None:
    def raise_missing(*_args: object, **_kwargs: object) -> dict[str, object]:
        raise router_module.media_link_tags_repo.MediaLinkTagsNotFoundError("Media link not found")

    monkeypatch.setattr(router_module.media_link_tags_repo, "sync_media_link_tags", raise_missing)

    response = TestClient(app).put(
        "/api/v1/admin/media-links/missing-link/tags",
        json={"people": []},
    )

    assert response.status_code == 404
    assert response.json() == {"detail": "Media link not found"}
