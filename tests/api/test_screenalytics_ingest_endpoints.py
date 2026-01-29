from __future__ import annotations

from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from api.main import app


@pytest.fixture(autouse=True)
def set_service_token(monkeypatch):
    monkeypatch.setenv("SCREENALYTICS_SERVICE_TOKEN", "test-token")
    yield


def _auth_headers():
    return {"Authorization": "Bearer test-token"}


def test_episode_cast_requires_token():
    client = TestClient (app)
    response = client.get(f"/api/v1/screenalytics/episodes/{uuid4()}/cast")
    assert response.status_code == 401


def test_episode_cast_rejects_wrong_token():
    client = TestClient (app)
    response = client.get(
        f"/api/v1/screenalytics/episodes/{uuid4()}/cast",
        headers={"Authorization": "Bearer wrong"},
    )
    assert response.status_code == 401


def test_episode_cast_ok():
    client = TestClient (app)
    response = client.get(
        f"/api/v1/screenalytics/episodes/{uuid4()}/cast",
        headers=_auth_headers(),
    )
    assert response.status_code == 200
    assert isinstance(response.json(), list)


def test_season_cast_ok():
    client = TestClient (app)
    response = client.get(
        f"/api/v1/screenalytics/seasons/{uuid4()}/cast",
        headers=_auth_headers(),
    )
    assert response.status_code == 200
    assert isinstance(response.json(), list)


def test_person_photos_ok():
    client = TestClient (app)
    response = client.get(
        f"/api/v1/screenalytics/people/{uuid4()}/photos",
        headers=_auth_headers(),
    )
    assert response.status_code == 200
    assert isinstance(response.json(), list)
