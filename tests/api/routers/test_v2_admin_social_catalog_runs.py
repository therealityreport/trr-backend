from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.auth import require_internal_admin
from api.routers.v2 import admin_social_catalog_runs
from trr_backend.db.pg import DatabaseServiceUnavailableError


@pytest.fixture
def fake_profile_reads(monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    calls: list[dict[str, Any]] = []

    def get_catalog_recent_runs(**kwargs: Any) -> dict[str, Any]:
        calls.append(kwargs)
        return {
            "platform": kwargs["platform"],
            "handle": kwargs["account_handle"],
            "catalog_recent_runs": [
                {
                    "job_id": "job-1",
                    "run_id": "run-1",
                    "status": "running",
                    "created_at": "2026-08-04T12:00:00.000Z",
                    "started_at": None,
                    "completed_at": None,
                    "error_message": None,
                    "catalog_action": "backfill",
                    "catalog_action_scope": "full_history",
                    "date_start": None,
                    "date_end": None,
                    "launch_group_id": "launch-1",
                    "launch_state": "pending",
                    "selected_tasks": ["post_details", "comments"],
                    "effective_selected_tasks": ["post_details", "comments"],
                    "comments_run_id": None,
                    "attached_followups": {
                        "comments": {
                            "run_id": "comments-1",
                            "status": "pending",
                            "state": "pending",
                            "source": "deferred_after_catalog",
                            "error_message": None,
                            "failed_at": None,
                            "retryable": None,
                        }
                    },
                }
            ],
        }

    fake = SimpleNamespace(calls=calls, get_catalog_recent_runs=get_catalog_recent_runs)
    monkeypatch.setattr(admin_social_catalog_runs, "profile_reads", fake)
    return fake


@pytest.fixture
def app() -> FastAPI:
    test_app = FastAPI()
    test_app.include_router(admin_social_catalog_runs.router, prefix="/api/v2")
    test_app.dependency_overrides[require_internal_admin] = lambda: {
        "id": "trr-app-internal-admin",
        "admin_uid": "signed-admin-uid",
        "role": "internal_admin",
    }
    return test_app


@pytest.fixture
def client(app: FastAPI) -> TestClient:
    return TestClient(app)


def test_v2_recent_catalog_runs_normalizes_the_profile_and_clamps_limit(
    client: TestClient,
    fake_profile_reads: SimpleNamespace,
) -> None:
    response = client.get("/api/v2/admin/social/profiles/Instagram/%40BravoTV/catalog/runs/recent?limit=99")

    assert response.status_code == 200
    assert response.json() == {
        "platform": "instagram",
        "handle": "bravotv",
        "catalog_recent_runs": [
            {
                "job_id": "job-1",
                "run_id": "run-1",
                "status": "running",
                "created_at": "2026-08-04T12:00:00.000Z",
                "catalog_action": "backfill",
                "catalog_action_scope": "full_history",
                "launch_group_id": "launch-1",
                "launch_state": "pending",
                "selected_tasks": ["post_details", "comments"],
                "effective_selected_tasks": ["post_details", "comments"],
                "attached_followups": {
                    "comments": {
                        "run_id": "comments-1",
                        "status": "pending",
                        "state": "pending",
                        "source": "deferred_after_catalog",
                    }
                },
            }
        ],
    }
    assert fake_profile_reads.calls == [{"platform": "instagram", "account_handle": "bravotv", "limit": 25}]


@pytest.mark.parametrize(
    ("path", "expected_code"),
    [
        ("/api/v2/admin/social/profiles/tiktok/bravotv/catalog/runs/recent", "UNSUPPORTED_CATALOG_PROFILE"),
        ("/api/v2/admin/social/profiles/instagram/%40%40/catalog/runs/recent", "INVALID_CATALOG_HANDLE"),
        (
            "/api/v2/admin/social/profiles/instagram/bravotv/catalog/runs/recent?limit=bad",
            "INVALID_CATALOG_RECENT_RUNS_LIMIT",
        ),
    ],
)
def test_v2_recent_catalog_runs_invalid_requests_use_typed_400(
    client: TestClient,
    fake_profile_reads: SimpleNamespace,
    path: str,
    expected_code: str,
) -> None:
    response = client.get(path, headers={"x-request-id": "catalog-runs-invalid"})

    assert response.status_code == 400
    assert response.json()["detail"]["code"] == expected_code
    assert response.json()["detail"]["request_id"] == "catalog-runs-invalid"
    assert "422" not in response.text
    assert fake_profile_reads.calls == []


def test_v2_recent_catalog_runs_database_unavailability_uses_safe_problem(
    client: TestClient,
    fake_profile_reads: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unavailable(**_kwargs: Any) -> dict[str, Any]:
        raise DatabaseServiceUnavailableError("secret database topology", reason="pool_capacity")

    monkeypatch.setattr(fake_profile_reads, "get_catalog_recent_runs", unavailable)
    response = client.get("/api/v2/admin/social/profiles/instagram/bravotv/catalog/runs/recent")

    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "DATABASE_SERVICE_UNAVAILABLE"
    assert "secret database topology" not in response.text


def test_v2_recent_catalog_runs_openapi_is_explicit_and_admin_protected(app: FastAPI) -> None:
    schema = app.openapi()
    path = "/api/v2/admin/social/profiles/{platform}/{handle}/catalog/runs/recent"
    operation = schema["paths"][path]["get"]

    assert operation["operationId"] == "listAdminSocialCatalogRecentRunsV2"
    assert operation["security"] == [{"InternalAdminBearer": []}]
    assert "422" not in operation["responses"]
    assert {"400", "500", "503"}.issubset(operation["responses"])
