from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from api.auth import require_internal_admin
from api.main import app
from api.routers import admin_recent_people as router_module


@pytest.fixture(autouse=True)
def override_admin():
    app.dependency_overrides[require_internal_admin] = lambda: {
        "id": "service_role:test",
        "role": "service_role",
        "email": None,
    }
    yield
    app.dependency_overrides.pop(require_internal_admin, None)


def test_recent_people_get_and_post_preserve_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        router_module.recent_people_repo,
        "list_recent_people",
        lambda firebase_uid, limit=None: (
            [
                {
                    "person_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                    "full_name": "Bravo Star",
                    "known_for": "Reality TV",
                    "photo_url": "https://cdn.example.com/person.jpg",
                    "show_context": "show-1",
                    "view_count": 4,
                    "first_viewed_at": "2026-03-25T00:00:00Z",
                    "last_viewed_at": "2026-03-26T00:00:00Z",
                }
            ],
            1,
        ),
    )
    monkeypatch.setattr(
        router_module.recent_people_repo,
        "record_recent_person_view",
        lambda **kwargs: ({"ok": True}, 2),
    )

    client = TestClient(app)
    listed = client.get("/api/v1/admin/recent-people", headers={"X-TRR-Admin-User-Uid": "firebase:admin-1"})
    recorded = client.post(
        "/api/v1/admin/recent-people",
        headers={"X-TRR-Admin-User-Uid": "firebase:admin-1"},
        json={
            "personId": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "showId": "show-1",
        },
    )

    assert listed.status_code == 200
    assert listed.json()["pagination"] == {"limit": 20, "count": 1}
    assert listed.json()["people"][0]["full_name"] == "Bravo Star"
    assert recorded.status_code == 200
    assert recorded.json() == {"ok": True}


def test_recent_people_post_rejects_invalid_person_id() -> None:
    client = TestClient(app)
    response = client.post("/api/v1/admin/recent-people", json={"personId": "nope"})

    assert response.status_code == 400
    assert response.json() == {"detail": "personId must be a valid UUID"}
