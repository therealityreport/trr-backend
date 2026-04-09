from __future__ import annotations

from uuid import uuid4

import psycopg2
import pytest
from fastapi.testclient import TestClient

from api.main import app
from api.routers import screenalytics as screenalytics_router


@pytest.fixture(autouse=True)
def set_service_token(monkeypatch):
    monkeypatch.setenv("SCREENALYTICS_SERVICE_TOKEN", "test-token")
    monkeypatch.setenv("TRR_SCREENALYTICS_ALLOW_SERVICE_TOKEN_FALLBACK", "1")
    yield


@pytest.fixture(autouse=True)
def mock_db_reads(monkeypatch):
    monkeypatch.setattr(screenalytics_router.pg, "fetch_all", lambda *args, **kwargs: [])
    yield


def _auth_headers():
    return {"Authorization": "Bearer test-token"}


def test_episode_cast_requires_token():
    client = TestClient(app)
    response = client.get(f"/api/v1/screenalytics/episodes/{uuid4()}/cast")
    assert response.status_code == 401


def test_episode_cast_rejects_wrong_token():
    client = TestClient(app)
    response = client.get(
        f"/api/v1/screenalytics/episodes/{uuid4()}/cast",
        headers={"Authorization": "Bearer wrong"},
    )
    assert response.status_code == 401


def test_episode_cast_ok():
    client = TestClient(app)
    response = client.get(
        f"/api/v1/screenalytics/episodes/{uuid4()}/cast",
        headers=_auth_headers(),
    )
    assert response.status_code == 200
    assert isinstance(response.json(), list)


def test_season_cast_ok():
    client = TestClient(app)
    response = client.get(
        f"/api/v1/screenalytics/seasons/{uuid4()}/cast",
        headers=_auth_headers(),
    )
    assert response.status_code == 200
    assert isinstance(response.json(), list)


def test_episode_cast_returns_empty_when_view_missing(monkeypatch):
    def raise_undefined_table(*_args, **_kwargs):
        raise psycopg2.errors.UndefinedTable("relation does not exist")

    monkeypatch.setattr(screenalytics_router.pg, "fetch_all", raise_undefined_table)
    client = TestClient(app)

    response = client.get(
        f"/api/v1/screenalytics/episodes/{uuid4()}/cast",
        headers=_auth_headers(),
    )

    assert response.status_code == 200
    assert response.json() == []


def test_season_cast_returns_empty_when_view_missing(monkeypatch):
    def raise_undefined_table(*_args, **_kwargs):
        raise psycopg2.errors.UndefinedTable("relation does not exist")

    monkeypatch.setattr(screenalytics_router.pg, "fetch_all", raise_undefined_table)
    client = TestClient(app)

    response = client.get(
        f"/api/v1/screenalytics/seasons/{uuid4()}/cast",
        headers=_auth_headers(),
    )

    assert response.status_code == 200
    assert response.json() == []


def test_person_photos_ok():
    client = TestClient(app)
    response = client.get(
        f"/api/v1/screenalytics/people/{uuid4()}/photos",
        headers=_auth_headers(),
    )
    assert response.status_code == 200
    assert isinstance(response.json(), list)


def test_person_photos_seed_only_true_adds_facebank_filter(monkeypatch):
    captured: dict[str, object] = {}

    def fake_fetch_all(sql: str, params: list[object]) -> list[dict]:
        captured["sql"] = sql
        captured["params"] = params
        return []

    monkeypatch.setattr(screenalytics_router.pg, "fetch_all", fake_fetch_all)
    person_id = uuid4()
    client = TestClient(app)

    response = client.get(
        f"/api/v1/screenalytics/people/{person_id}/photos?seed_only=true&limit=7&offset=2",
        headers=_auth_headers(),
    )

    assert response.status_code == 200
    assert "AND facebank_seed = true" in str(captured["sql"])
    assert captured["params"] == [str(person_id), 7, 2]


def test_person_photos_seed_only_false_omits_facebank_filter(monkeypatch):
    captured: dict[str, object] = {}

    def fake_fetch_all(sql: str, params: list[object]) -> list[dict]:
        captured["sql"] = sql
        captured["params"] = params
        return []

    monkeypatch.setattr(screenalytics_router.pg, "fetch_all", fake_fetch_all)
    person_id = uuid4()
    client = TestClient(app)

    response = client.get(
        f"/api/v1/screenalytics/people/{person_id}/photos?seed_only=false&limit=9&offset=1",
        headers=_auth_headers(),
    )

    assert response.status_code == 200
    assert "AND facebank_seed = true" not in str(captured["sql"])
    assert captured["params"] == [str(person_id), 9, 1]


def test_person_photos_facebank_initial_profile_uses_ranked_selection(monkeypatch):
    person_id = uuid4()
    captured: dict[str, object] = {}

    def fake_select_initial_facebank_photos(**kwargs):
        captured.update(kwargs)
        return [
            {
                "served_url": "https://cdn.example.com/seed.jpg",
                "media_asset_id": str(uuid4()),
                "selection_reasons": ["seeded", "solo"],
                "selection_bucket": 3,
                "is_primary": True,
                "width": 800,
                "height": 1000,
                "kind": "gallery",
            }
        ]

    monkeypatch.setattr(screenalytics_router, "_select_initial_facebank_photos", fake_select_initial_facebank_photos)
    client = TestClient(app)

    response = client.get(
        f"/api/v1/screenalytics/people/{person_id}/photos"
        "?selection_profile=facebank_initial&seed_only=true&limit=5&show_id="
        f"{uuid4()}&show_name=Demo%20Show",
        headers=_auth_headers(),
    )

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    assert body[0]["selection_reasons"] == ["seeded", "solo"]
    assert captured["person_id"] == person_id
    assert captured["limit"] == 5
    assert captured["seed_only"] is True


def test_person_photos_facebank_initial_profile_ignores_nonzero_offset(monkeypatch):
    monkeypatch.setattr(
        screenalytics_router,
        "_select_initial_facebank_photos",
        lambda **_kwargs: [{"served_url": "https://cdn.example.com/should-not-run.jpg"}],
    )
    client = TestClient(app)

    response = client.get(
        f"/api/v1/screenalytics/people/{uuid4()}/photos?selection_profile=facebank_initial&offset=1",
        headers=_auth_headers(),
    )

    assert response.status_code == 200
    assert response.json() == []
