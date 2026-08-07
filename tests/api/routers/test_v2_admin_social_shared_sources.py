from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.auth import require_internal_admin
from api.routers.v2.admin_social_shared_sources import router
from trr_backend.db.pg import DatabaseServiceUnavailableError
from trr_backend.socials.control_plane import shared_source_config


def _source() -> dict[str, object]:
    return {
        "id": "source-1",
        "platform": "instagram",
        "source_scope": "network",
        "account_handle": "bravotv",
        "is_active": True,
        "scrape_priority": 10,
        "metadata": {"display_name": "Bravo TV"},
        "last_scrape_status": None,
        "last_scrape_run_id": None,
        "last_scrape_job_id": None,
        "last_scrape_at": None,
        "last_classified_at": None,
        "updated_by": None,
        "created_at": None,
        "updated_at": None,
        "is_default": False,
        "profile_kind": "network_streaming",
        "network_name": "Bravo TV",
        "assignment_mode": "multi_show_match",
        "assignment_rules": {"use_hashtags": True},
    }


def _build_app(*, authenticated: bool = True) -> FastAPI:
    app = FastAPI()
    app.include_router(router, prefix="/api/v2")
    if authenticated:
        app.dependency_overrides[require_internal_admin] = lambda: {
            "id": "trr-app-internal-admin",
            "admin_uid": "admin-1",
            "admin_email": "admin@example.com",
            "role": "internal_admin",
        }
    return app


@pytest.fixture
def client() -> Iterator[TestClient]:
    with TestClient(_build_app()) as test_client:
        yield test_client


def test_routes_require_internal_admin() -> None:
    response = TestClient(_build_app(authenticated=False)).get("/api/v2/admin/socials/shared-account-sources")
    assert response.status_code == 401


def test_get_forwards_normalized_query(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def get_sources(**kwargs):
        captured.update(kwargs)
        return {"source_scope": "network", "sources": [_source()], "using_defaults": False}

    monkeypatch.setattr(shared_source_config, "get_shared_account_sources", get_sources)
    response = client.get(
        "/api/v2/admin/socials/shared-account-sources"
        "?source_scope=network&include_inactive=false&platforms=instagram,tiktok"
    )

    assert response.status_code == 200
    assert response.json()["sources"][0]["account_handle"] == "bravotv"
    assert captured == {
        "source_scope": "network",
        "include_inactive": False,
        "platforms": ["instagram", "tiktok"],
    }


def test_get_rejects_invalid_boolean_and_platform(client: TestClient) -> None:
    invalid_boolean = client.get("/api/v2/admin/socials/shared-account-sources?include_inactive=maybe")
    invalid_platforms = client.get("/api/v2/admin/socials/shared-account-sources?platforms=")

    assert invalid_boolean.status_code == 400
    assert invalid_boolean.json()["detail"]["code"] == "INVALID_INCLUDE_INACTIVE"
    assert invalid_platforms.status_code == 400
    assert invalid_platforms.json()["detail"]["code"] == "INVALID_PLATFORM_FILTER"


def test_put_is_strict_and_forwards_verified_actor(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    def put_sources(**kwargs):
        captured.update(kwargs)
        return {"source_scope": "network", "sources": [_source()], "using_defaults": False}

    monkeypatch.setattr(shared_source_config, "put_shared_account_sources", put_sources)
    response = client.put(
        "/api/v2/admin/socials/shared-account-sources",
        json={
            "source_scope": "network",
            "sources": [
                {
                    "platform": "instagram",
                    "account_handle": "@BravoTV",
                    "is_active": True,
                    "scrape_priority": 10,
                    "metadata": {},
                }
            ],
        },
    )
    invalid = client.put(
        "/api/v2/admin/socials/shared-account-sources",
        json={"source_scope": "network", "sources": [], "extra": True},
    )

    assert response.status_code == 200
    assert captured["updated_by"] == "admin@example.com"
    assert captured["sources"][0]["account_handle"] == "@BravoTV"
    assert invalid.status_code == 400
    assert invalid.json()["detail"]["code"] == "INVALID_SHARED_ACCOUNT_SOURCES_REQUEST"


def test_database_outage_is_typed_503(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        shared_source_config,
        "get_shared_account_sources",
        lambda **_kwargs: (_ for _ in ()).throw(DatabaseServiceUnavailableError("pool unavailable")),
    )

    response = client.get("/api/v2/admin/socials/shared-account-sources")

    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "DATABASE_SERVICE_UNAVAILABLE"
