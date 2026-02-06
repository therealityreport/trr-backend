from __future__ import annotations

from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from api.main import app
from api.routers import screenalytics as screenalytics_router


@pytest.fixture(autouse=True)
def set_service_token(monkeypatch):
    monkeypatch.setenv("SCREENALYTICS_SERVICE_TOKEN", "test-token")
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
