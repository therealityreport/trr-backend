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
        ("GET", "/api/v1/admin/brands/families"),
        ("GET", "/api/v1/admin/brands/families/suggestions"),
        ("GET", "/api/v1/admin/brands/families/by-entity?entity_type=network&entity_key=bravo"),
        ("GET", "/api/v1/admin/brands/families/f1/links"),
        ("GET", "/api/v1/admin/brands/families/f1/wikipedia-show-urls"),
        ("GET", "/api/v1/admin/brands/logos?target_type=publication"),
        ("GET", "/api/v1/admin/brands/logo-targets?target_type=network"),
        ("PUT", "/api/v1/admin/brands/franchise-rules/real-housewives"),
        ("POST", "/api/v1/admin/brands/franchise-rules/real-housewives/apply"),
        ("POST", "/api/v1/admin/brands/families"),
        ("PATCH", "/api/v1/admin/brands/families/f1"),
        ("POST", "/api/v1/admin/brands/families/f1/members"),
        ("DELETE", "/api/v1/admin/brands/families/f1/members/m1"),
        ("POST", "/api/v1/admin/brands/families/f1/links"),
        ("PATCH", "/api/v1/admin/brands/families/f1/links/r1"),
        ("POST", "/api/v1/admin/brands/families/f1/links/apply"),
        ("POST", "/api/v1/admin/brands/families/f1/wikipedia-import"),
    ]

    for method, path in endpoints:
        response = client.request(method, path, json={})
        assert response.status_code == 401


def test_get_shows_franchises_success(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
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
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
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
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
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
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
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
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

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
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

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
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

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
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with patch("trr_backend.repositories.brands_franchises.list_shows_franchises", side_effect=RuntimeError("boom")):
        response = client.get(
            "/api/v1/admin/brands/shows-franchises",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 500
    assert response.json()["detail"] == "boom"


def test_get_brand_logos_success(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "rows": [
            {
                "id": "logo-1",
                "target_type": "publication",
                "target_key": "deadline.com",
                "target_label": "deadline.com",
            }
        ],
        "count": 1,
    }

    with patch("api.routers.admin_brands._list_brand_logos", return_value=expected) as mocked:
        response = client.get(
            "/api/v1/admin/brands/logos?target_type=publication&q=deadline&limit=5&offset=0",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json() == expected
    assert mocked.call_args.kwargs == {
        "target_type": "publication",
        "q": "deadline",
        "limit": 5,
        "offset": 0,
    }


def test_get_brand_logo_targets_success(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")
    expected = {
        "rows": [{"target_type": "network", "target_key": "1", "target_label": "Bravo"}],
        "count": 1,
    }

    with patch("api.routers.admin_brands._list_logo_targets", return_value=expected) as mocked:
        response = client.get(
            "/api/v1/admin/brands/logo-targets?target_type=network&q=bra&limit=10",
            headers={"Authorization": f"Bearer {token}"},
        )

    assert response.status_code == 200
    assert response.json() == expected
    assert mocked.call_args.kwargs == {
        "target_type": "network",
        "q": "bra",
        "limit": 10,
    }


def test_brand_family_endpoints_success(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    token = _make_admin_token("test-secret-32-bytes-minimum-abcdef")

    with patch("trr_backend.repositories.brand_families.list_families", return_value={"rows": [], "count": 0}):
        response = client.get(
            "/api/v1/admin/brands/families",
            headers={"Authorization": f"Bearer {token}"},
        )
    assert response.status_code == 200
    assert response.json() == {"rows": [], "count": 0}

    with patch(
        "trr_backend.repositories.brand_families.create_family",
        return_value={"id": "f1", "family_key": "nbcu", "display_name": "NBCU Family"},
    ):
        response = client.post(
            "/api/v1/admin/brands/families",
            headers={"Authorization": f"Bearer {token}"},
            json={"display_name": "NBCU Family"},
        )
    assert response.status_code == 200
    assert response.json()["id"] == "f1"

    with patch(
        "trr_backend.repositories.brand_families.get_family_by_entity",
        return_value={"id": "f1", "display_name": "NBCU Family"},
    ), patch(
        "trr_backend.repositories.brand_families.list_family_suggestions",
        return_value={"rows": []},
    ), patch(
        "trr_backend.repositories.brand_families.list_family_links",
        return_value={"rows": []},
    ), patch(
        "trr_backend.repositories.brand_families.list_family_wikipedia_show_links",
        return_value={"rows": []},
    ):
        response = client.get(
            "/api/v1/admin/brands/families/by-entity?entity_type=network&entity_key=bravo",
            headers={"Authorization": f"Bearer {token}"},
        )
    assert response.status_code == 200
    assert response.json()["family"]["id"] == "f1"
