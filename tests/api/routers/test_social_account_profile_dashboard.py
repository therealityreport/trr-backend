"""Tests for the account dashboard admin endpoint."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import jwt
import pytest
from fastapi.exceptions import ResponseValidationError
from fastapi.testclient import TestClient

from api.main import app


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


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


def _dashboard_payload() -> dict:
    return {
        "data": {
            "summary": {
                "platform": "instagram",
                "account_handle": "thetraitorsus",
                "summary_detail": "lite",
                "catalog_recent_runs": [],
            },
            "catalog_run_progress": None,
        },
        "freshness": {
            "status": "fresh",
            "source": "live",
            "generated_at": "2026-04-26T12:03:00+00:00",
            "age_seconds": 0,
        },
        "operational_alerts": [],
    }


def test_get_social_account_profile_dashboard(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with patch(
        "api.routers.socials.build_social_account_profile_dashboard",
        return_value=_dashboard_payload(),
    ) as mocked:
        response = client.get(
            "/api/v1/admin/socials/profiles/instagram/thetraitorsus/dashboard?detail=lite&recent_log_limit=12",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json() == _dashboard_payload()
    assert mocked.call_args.kwargs["platform"] == "instagram"
    assert mocked.call_args.kwargs["account_handle"] == "thetraitorsus"
    assert mocked.call_args.kwargs["detail"] == "lite"
    assert mocked.call_args.kwargs["recent_log_limit"] == 12


def test_recent_log_limit_is_capped_to_100(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with patch(
        "api.routers.socials.build_social_account_profile_dashboard",
        return_value=_dashboard_payload(),
    ) as mocked:
        response = client.get(
            "/api/v1/admin/socials/profiles/instagram/thetraitorsus/dashboard?recent_log_limit=500",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert mocked.call_args.kwargs["recent_log_limit"] == 100


def test_recent_log_limit_is_floored_to_1(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with patch(
        "api.routers.socials.build_social_account_profile_dashboard",
        return_value=_dashboard_payload(),
    ) as mocked:
        response = client.get(
            "/api/v1/admin/socials/profiles/instagram/thetraitorsus/dashboard?recent_log_limit=0",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert mocked.call_args.kwargs["recent_log_limit"] == 1


def test_invalid_recent_log_limit_returns_validation_error(
    client: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    response = client.get(
        "/api/v1/admin/socials/profiles/instagram/thetraitorsus/dashboard?recent_log_limit=not-an-int",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 422


def test_dashboard_response_schema_is_in_openapi() -> None:
    schema = app.openapi()
    operation = schema["paths"]["/api/v1/admin/socials/profiles/{platform}/{account_handle}/dashboard"]["get"]
    response_schema = operation["responses"]["200"]["content"]["application/json"]["schema"]

    assert response_schema["$ref"] == "#/components/schemas/SocialAccountDashboardPayload"
    assert "SocialAccountDashboardPayload" in schema["components"]["schemas"]


def test_invalid_service_output_fails_loudly(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    client = TestClient(app)
    invalid_payload = {
        "data": {"catalog_run_progress": None},
        "freshness": {"status": "fresh", "source": "live", "age_seconds": 0},
        "operational_alerts": [],
    }

    with patch("api.routers.socials.build_social_account_profile_dashboard", return_value=invalid_payload):
        with pytest.raises(ResponseValidationError):
            client.get(
                "/api/v1/admin/socials/profiles/instagram/thetraitorsus/dashboard",
                headers={"Authorization": f"Bearer {token}"},
            )
