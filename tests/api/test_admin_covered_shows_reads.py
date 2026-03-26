from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from api.auth import require_internal_admin
from api.main import app
from api.routers import admin_covered_shows as router_module


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
    router_module.invalidate_covered_shows_cache()
    yield
    router_module.invalidate_covered_shows_cache()


def test_list_covered_shows_returns_contract_and_caches(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"count": 0}

    def fake_list():
        calls["count"] += 1
        return (
            [
                {
                    "id": "covered-1",
                    "trr_show_id": "show-1",
                    "show_name": "Bravo Show",
                    "canonical_slug": "bravo-show",
                    "alternative_names": ["Bravo"],
                    "show_total_episodes": 12,
                    "poster_url": "https://cdn.example.com/poster.jpg",
                }
            ],
            1,
        )

    monkeypatch.setattr(router_module.covered_shows_repo, "list_covered_shows", fake_list)

    client = TestClient(app)
    first = client.get("/api/v1/admin/covered-shows")
    second = client.get("/api/v1/admin/covered-shows")

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json() == {
        "shows": [
            {
                "id": "covered-1",
                "trr_show_id": "show-1",
                "show_name": "Bravo Show",
                "canonical_slug": "bravo-show",
                "alternative_names": ["Bravo"],
                "show_total_episodes": 12,
                "poster_url": "https://cdn.example.com/poster.jpg",
            }
        ]
    }
    assert calls["count"] == 1


def test_get_covered_show_returns_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        router_module.covered_shows_repo,
        "get_covered_show",
        lambda show_id: (
            {
                "id": "covered-1",
                "trr_show_id": show_id,
                "show_name": "Bravo Show",
                "canonical_slug": "bravo-show",
                "alternative_names": ["Bravo"],
                "show_total_episodes": 12,
                "poster_url": "https://cdn.example.com/poster.jpg",
            },
            1,
        ),
    )

    client = TestClient(app)
    response = client.get("/api/v1/admin/covered-shows/show-1")

    assert response.status_code == 200
    assert response.json() == {
        "show": {
            "id": "covered-1",
            "trr_show_id": "show-1",
            "show_name": "Bravo Show",
            "canonical_slug": "bravo-show",
            "alternative_names": ["Bravo"],
            "show_total_episodes": 12,
            "poster_url": "https://cdn.example.com/poster.jpg",
        }
    }


def test_invalidate_covered_shows_cache_clears_backend_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"count": 0}

    def fake_list():
        calls["count"] += 1
        return ([], 1)

    monkeypatch.setattr(router_module.covered_shows_repo, "list_covered_shows", fake_list)

    client = TestClient(app)
    client.get("/api/v1/admin/covered-shows")
    client.post("/api/v1/admin/covered-shows/cache/invalidate")
    client.get("/api/v1/admin/covered-shows")

    assert calls["count"] == 2


def test_create_and_delete_covered_show_preserve_contract_and_invalidate_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"list": 0}

    monkeypatch.setattr(
        router_module.covered_shows_repo,
        "add_covered_show",
        lambda **kwargs: (
            {
                "id": "covered-1",
                "trr_show_id": kwargs["show_id"],
                "show_name": kwargs["show_name"],
                "canonical_slug": "bravo-show",
                "alternative_names": ["Bravo"],
                "show_total_episodes": 12,
                "poster_url": "https://cdn.example.com/poster.jpg",
            },
            2,
        ),
    )
    monkeypatch.setattr(router_module.covered_shows_repo, "remove_covered_show", lambda show_id: (True, 1))

    def fake_list():
        calls["list"] += 1
        return ([], 1)

    monkeypatch.setattr(router_module.covered_shows_repo, "list_covered_shows", fake_list)

    client = TestClient(app)
    client.get("/api/v1/admin/covered-shows")
    created = client.post(
        "/api/v1/admin/covered-shows",
        headers={"X-TRR-Admin-User-Uid": "firebase:admin-1"},
        json={"trr_show_id": "show-1", "show_name": "Bravo Show"},
    )
    client.get("/api/v1/admin/covered-shows")
    deleted = client.delete("/api/v1/admin/covered-shows/show-1")
    client.get("/api/v1/admin/covered-shows")

    assert created.status_code == 200
    assert created.json()["show"]["trr_show_id"] == "show-1"
    assert deleted.status_code == 200
    assert deleted.json() == {"success": True}
    assert calls["list"] == 3


def test_create_and_delete_covered_show_preserve_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        router_module.covered_shows_repo,
        "add_covered_show",
        lambda **kwargs: (
            {
                "id": "covered-1",
                "trr_show_id": kwargs["show_id"],
                "show_name": kwargs["show_name"],
                "canonical_slug": "bravo-show",
                "alternative_names": ["Bravo"],
                "show_total_episodes": 12,
                "poster_url": "https://cdn.example.com/poster.jpg",
            },
            2,
        ),
    )
    monkeypatch.setattr(router_module.covered_shows_repo, "remove_covered_show", lambda show_id: (True, 1))

    client = TestClient(app)
    created = client.post(
        "/api/v1/admin/covered-shows",
        headers={"X-TRR-Admin-User-Uid": "firebase:admin-1"},
        json={"trr_show_id": "show-1", "show_name": "Bravo Show"},
    )
    deleted = client.delete("/api/v1/admin/covered-shows/show-1")

    assert created.status_code == 200
    assert created.json()["show"]["trr_show_id"] == "show-1"
    assert deleted.status_code == 200
    assert deleted.json() == {"success": True}
