"""Tests for admin brands shows/franchises endpoints."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import jwt
import pytest
from fastapi.testclient import TestClient

from api.main import app


def _make_admin_token(secret: str, subject: str = "admin-1") -> str:
    now = datetime.now(tz=UTC)
    payload = {
        "sub": subject,
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=5)).timestamp()),
        "role": "service_role",
    }
    return jwt.encode(payload, secret, algorithm="HS256")


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


def test_admin_brands_endpoints_require_authentication(client: TestClient) -> None:
    endpoints = [
        ("GET", "/api/v1/admin/brands/shows-franchises"),
        ("GET", "/api/v1/admin/brands/franchise-rules"),
        ("PUT", "/api/v1/admin/brands/franchise-rules/real-housewives"),
        ("POST", "/api/v1/admin/brands/franchise-rules/real-housewives/apply"),
    ]

    for method, path in endpoints:
        response = client.request(method, path, json={})
        assert response.status_code == 401


def test_get_shows_franchises_success(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")
    expected = {
        "rows": [{"show_name": "The Traitors", "franchise_key": "traitors"}],
        "count": 1,
        "groups": [{"franchise_key": "traitors", "count": 1}],
    }

    with patch("trr_backend.repositories.brands_franchises.list_shows_franchises", return_value=expected) as mocked:
        response = client.get(
            "/api/v1/admin/brands/shows-franchises?q=traitors&limit=5",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json() == expected
    assert mocked.call_args.kwargs == {"q": "traitors", "limit": 5}


def test_get_franchise_rules_success(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")
    expected = {
        "rules": [{"key": "real-housewives", "name": "Real Housewives"}],
        "suggested_franchises": ["real-housewives"],
    }

    with patch("trr_backend.repositories.brands_franchises.list_franchise_rules", return_value=expected):
        response = client.get(
            "/api/v1/admin/brands/franchise-rules",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json() == expected


def test_put_franchise_rule_success(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")
    payload = {
        "name": "Real Housewives",
        "primary_url": "https://real-housewives.fandom.com/wiki/Real_Housewives_Wiki",
        "source_rank": 10,
    }
    expected = {"rule": {"key": "real-housewives", **payload}}

    with patch("trr_backend.repositories.brands_franchises.update_franchise_rule", return_value=expected) as mocked:
        response = client.put(
            "/api/v1/admin/brands/franchise-rules/real-housewives",
            headers={"Authorization": f"Bearer {token}"},
            json=payload,
        )

    assert response.status_code == 200
    assert response.json() == expected
    assert mocked.call_args.kwargs["franchise_key"] == "real-housewives"
    assert mocked.call_args.kwargs["payload"] == payload
    assert mocked.call_args.kwargs["actor"] == "service_role:unknown"


def test_post_apply_franchise_rule_uses_safe_defaults(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")
    expected = {"franchise_key": "real-housewives", "dry_run": True, "missing_only": True}

    with patch("trr_backend.repositories.brands_franchises.apply_franchise_rule", return_value=expected) as mocked:
        response = client.post(
            "/api/v1/admin/brands/franchise-rules/real-housewives/apply",
            headers={"Authorization": f"Bearer {token}"},
            json={},
        )

    assert response.status_code == 200
    assert response.json() == expected
    assert mocked.call_args.kwargs["franchise_key"] == "real-housewives"
    assert mocked.call_args.kwargs["missing_only"] is True
    assert mocked.call_args.kwargs["dry_run"] is True
    assert mocked.call_args.kwargs["actor"] == "service_role:unknown"


def test_put_franchise_rule_maps_keyerror_to_404(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")

    with patch(
        "trr_backend.repositories.brands_franchises.update_franchise_rule",
        side_effect=KeyError("Unknown franchise key"),
    ):
        response = client.put(
            "/api/v1/admin/brands/franchise-rules/unknown-key",
            headers={"Authorization": f"Bearer {token}"},
            json={"primary_url": "https://example.com/wiki"},
        )

    assert response.status_code == 404
    assert response.json()["detail"] == "Unknown franchise key"


def test_put_franchise_rule_maps_valueerror_to_400(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")

    with patch(
        "trr_backend.repositories.brands_franchises.update_franchise_rule",
        side_effect=ValueError("primary_url is required"),
    ):
        response = client.put(
            "/api/v1/admin/brands/franchise-rules/real-housewives",
            headers={"Authorization": f"Bearer {token}"},
            json={"primary_url": ""},
        )

    assert response.status_code == 400
    assert response.json()["detail"] == "primary_url is required"


def test_get_franchise_rules_maps_readiness_runtimeerror_to_503(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")

    with patch(
        "trr_backend.repositories.brands_franchises.list_franchise_rules",
        side_effect=RuntimeError("Brands franchise rules table is unavailable. Run backend migrations."),
    ):
        response = client.get(
            "/api/v1/admin/brands/franchise-rules",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 503
    assert "unavailable" in response.json()["detail"].lower()


def test_get_shows_franchises_maps_unhandled_error_to_500(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret")
    token = _make_admin_token("test-secret")

    with patch("trr_backend.repositories.brands_franchises.list_shows_franchises", side_effect=RuntimeError("boom")):
        response = client.get(
            "/api/v1/admin/brands/shows-franchises",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 500
    assert response.json()["detail"] == "boom"
