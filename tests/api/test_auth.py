"""Auth tests for verified JWT behavior (no Supabase SDK)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import jwt
from fastapi import APIRouter, Depends, FastAPI
from fastapi.testclient import TestClient

import api.auth as api_auth
from api.auth import (
    CurrentUser,
    get_current_user,
    require_admin,
    require_cast_screentime_admin,
    require_internal_admin,
)


def _make_token(
    secret: str,
    subject: str,
    expires_delta: timedelta,
    *,
    issuer: str | None = None,
    audience: str | None = None,
    extra_claims: dict[str, object] | None = None,
) -> str:
    now = datetime.now(tz=UTC)
    payload = {
        "sub": subject,
        "iat": int(now.timestamp()),
        "nbf": int(now.timestamp()),
        "exp": int((now + expires_delta).timestamp()),
    }
    if issuer:
        payload["iss"] = issuer
    if audience:
        payload["aud"] = audience
    if extra_claims:
        payload.update(extra_claims)
    return jwt.encode(payload, secret, algorithm="HS256")


def _build_app() -> FastAPI:
    app = FastAPI()
    router = APIRouter()

    @router.get("/auth/required")
    async def auth_required(user: CurrentUser):
        return {"user_id": user["id"]}

    @router.get("/auth/optional")
    async def auth_optional(user=Depends(get_current_user)):  # noqa: B008
        return {"user_id": user["id"] if user else None}

    @router.get("/auth/internal-admin")
    async def auth_internal_admin(user=Depends(require_internal_admin)):  # noqa: B008
        return {"user_id": user["id"], "role": user["role"]}

    @router.get("/auth/admin")
    async def auth_admin(user=Depends(require_admin)):  # noqa: B008
        return {"user_id": user["id"], "role": user["role"]}

    @router.get("/auth/cast-screentime-admin")
    async def auth_cast_screentime_admin(user=Depends(require_cast_screentime_admin)):  # noqa: B008
        return {"user_id": user["id"], "role": user["role"]}

    app.include_router(router)
    return app


def test_require_user_missing_token_returns_401(monkeypatch):
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    app = _build_app()
    client = TestClient(app)

    response = client.get("/auth/required")
    assert response.status_code == 401


def test_require_user_invalid_token_returns_401(monkeypatch):
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    app = _build_app()
    client = TestClient(app)

    response = client.get("/auth/required", headers={"Authorization": "Bearer not-a-jwt"})
    assert response.status_code == 401


def test_require_user_expired_token_returns_401(monkeypatch):
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    app = _build_app()
    client = TestClient(app)

    token = _make_token("test-secret-32-bytes-minimum-abcdef", "user-1", timedelta(seconds=-30))
    response = client.get("/auth/required", headers={"Authorization": f"Bearer {token}"})
    assert response.status_code == 401


def test_require_user_valid_token_returns_user(monkeypatch):
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    monkeypatch.setenv("SUPABASE_PROJECT_REF", "project123")
    app = _build_app()
    client = TestClient(app)

    token = _make_token(
        "test-secret-32-bytes-minimum-abcdef",
        "user-123",
        timedelta(minutes=5),
        issuer="https://project123.supabase.co/auth/v1",
        extra_claims={"ref": "project123"},
    )
    response = client.get("/auth/required", headers={"Authorization": f"Bearer {token}"})
    assert response.status_code == 200
    assert response.json()["user_id"] == "user-123"


def test_require_user_rejects_wrong_project_ref(monkeypatch):
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    monkeypatch.setenv("SUPABASE_PROJECT_REF", "project123")
    app = _build_app()
    client = TestClient(app)

    token = _make_token(
        "test-secret-32-bytes-minimum-abcdef",
        "user-123",
        timedelta(minutes=5),
        issuer="https://project123.supabase.co/auth/v1",
        extra_claims={"ref": "wrong-project"},
    )
    response = client.get("/auth/required", headers={"Authorization": f"Bearer {token}"})
    assert response.status_code == 401


def test_require_internal_admin_accepts_signed_internal_token(monkeypatch):
    monkeypatch.setenv("TRR_INTERNAL_ADMIN_SHARED_SECRET", "internal-secret-32-bytes-minimum")
    monkeypatch.setenv("TRR_INTERNAL_ADMIN_JWT_ISSUER", "trr-app-internal")
    monkeypatch.setenv("TRR_INTERNAL_ADMIN_JWT_AUDIENCE", "trr-backend-internal-admin")
    app = _build_app()
    client = TestClient(app)

    token = _make_token(
        "internal-secret-32-bytes-minimum",
        "internal-admin-proxy",
        timedelta(minutes=5),
        issuer="trr-app-internal",
        audience="trr-backend-internal-admin",
        extra_claims={"scope": "internal_admin"},
    )
    response = client.get("/auth/internal-admin", headers={"Authorization": f"Bearer {token}"})
    assert response.status_code == 200
    assert response.json() == {"user_id": "internal-admin-proxy", "role": "internal_admin"}


def test_require_internal_admin_accepts_matching_shared_secret_header_when_fallback_enabled(monkeypatch):
    monkeypatch.setenv("TRR_INTERNAL_ADMIN_SHARED_SECRET", "internal-secret-32-bytes-minimum")
    monkeypatch.setenv("TRR_INTERNAL_ADMIN_ALLOW_RAW_SECRET_FALLBACK", "1")
    app = _build_app()
    client = TestClient(app)

    response = client.get(
        "/auth/internal-admin",
        headers={"X-TRR-Internal-Admin-Secret": "internal-secret-32-bytes-minimum"},
    )

    assert response.status_code == 200
    assert response.json() == {"user_id": "internal-admin:shared-secret", "role": "internal_admin"}


def test_require_internal_admin_rejects_matching_shared_secret_header_by_default(monkeypatch):
    monkeypatch.setenv("TRR_INTERNAL_ADMIN_SHARED_SECRET", "internal-secret-32-bytes-minimum")
    app = _build_app()
    client = TestClient(app)

    response = client.get(
        "/auth/internal-admin",
        headers={"X-TRR-Internal-Admin-Secret": "internal-secret-32-bytes-minimum"},
    )

    assert response.status_code == 401
    assert response.json()["detail"] == "Authentication required. Please provide a valid access token."


def test_require_internal_admin_rejects_matching_shared_secret_header_when_fallback_disabled(monkeypatch):
    monkeypatch.setenv("TRR_INTERNAL_ADMIN_SHARED_SECRET", "internal-secret-32-bytes-minimum")
    monkeypatch.setenv("TRR_INTERNAL_ADMIN_ALLOW_RAW_SECRET_FALLBACK", "0")
    app = _build_app()
    client = TestClient(app)

    response = client.get(
        "/auth/internal-admin",
        headers={"X-TRR-Internal-Admin-Secret": "internal-secret-32-bytes-minimum"},
    )

    assert response.status_code == 401
    assert response.json()["detail"] == "Authentication required. Please provide a valid access token."


def test_raw_secret_fallback_requires_strict_truthy_flag(monkeypatch):
    monkeypatch.setenv("TRR_INTERNAL_ADMIN_SHARED_SECRET", "internal-secret-32-bytes-minimum")
    monkeypatch.setenv("TRR_INTERNAL_ADMIN_ALLOW_RAW_SECRET_FALLBACK", "enabled")
    request = api_auth.Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/auth/internal-admin",
            "headers": [(b"x-trr-internal-admin-secret", b"internal-secret-32-bytes-minimum")],
        }
    )

    assert api_auth._raw_internal_admin_fallback_matches(request) is False


def test_require_internal_admin_rejects_non_allowlisted_user_token(monkeypatch):
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    monkeypatch.setenv("SUPABASE_PROJECT_REF", "project123")
    monkeypatch.setenv("TRR_INTERNAL_ADMIN_SHARED_SECRET", "internal-secret-32-bytes-minimum")
    monkeypatch.setenv("ADMIN_EMAIL_ALLOWLIST", "admin@thereality.report")
    app = _build_app()
    client = TestClient(app)

    token = _make_token(
        "test-secret-32-bytes-minimum-abcdef",
        "user-123",
        timedelta(minutes=5),
        issuer="https://project123.supabase.co/auth/v1",
        extra_claims={"ref": "project123", "email": "viewer@thereality.report", "role": "authenticated"},
    )

    response = client.get("/auth/internal-admin", headers={"Authorization": f"Bearer {token}"})
    assert response.status_code == 403
    assert response.json() == {"detail": "Allowlist admin access required"}


def test_require_internal_admin_accepts_local_loopback_proxy_marker(monkeypatch):
    monkeypatch.setenv("TRR_INTERNAL_ADMIN_SHARED_SECRET", "backend-secret-32-bytes-minimum")
    app = _build_app()
    client = TestClient(app, client=("127.0.0.1", 50000))

    response = client.get(
        "/auth/internal-admin",
        headers={
            "Authorization": "Bearer app-local-token-signed-with-a-different-secret",
            "Host": "127.0.0.1:8000",
            "x-trr-local-admin-proxy": "1",
            "x-trr-admin-uid": "local-admin",
            "x-trr-admin-email": "admin@thereality.report",
        },
    )

    assert response.status_code == 200
    assert response.json() == {"user_id": "internal-admin:local-admin", "role": "internal_admin"}


def test_require_internal_admin_rejects_remote_spoofed_local_proxy_marker(monkeypatch):
    monkeypatch.setenv("TRR_INTERNAL_ADMIN_SHARED_SECRET", "backend-secret-32-bytes-minimum")
    app = _build_app()
    client = TestClient(app, client=("203.0.113.10", 50000))

    response = client.get(
        "/auth/internal-admin",
        headers={
            "Authorization": "Bearer app-local-token-signed-with-a-different-secret",
            "Host": "127.0.0.1:8000",
            "x-trr-local-admin-proxy": "1",
            "x-trr-admin-uid": "local-admin",
        },
    )

    assert response.status_code == 403
    assert response.json() == {"detail": "Allowlist admin access required"}


def test_require_admin_rejects_service_role_by_default(monkeypatch):
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    monkeypatch.setenv("SUPABASE_PROJECT_REF", "project123")
    app = _build_app()
    client = TestClient(app)

    token = _make_token(
        "test-secret-32-bytes-minimum-abcdef",
        "service-role-subject",
        timedelta(minutes=5),
        issuer="https://project123.supabase.co/auth/v1",
        extra_claims={"ref": "project123", "role": "service_role"},
    )
    response = client.get("/auth/admin", headers={"Authorization": f"Bearer {token}"})
    assert response.status_code == 403
    assert response.json() == {"detail": "Admin access required"}


def test_require_admin_accepts_service_role_when_enabled(monkeypatch):
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    monkeypatch.setenv("SUPABASE_PROJECT_REF", "project123")
    monkeypatch.setenv("TRR_ADMIN_ALLOW_SERVICE_ROLE", "1")
    app = _build_app()
    client = TestClient(app)

    token = _make_token(
        "test-secret-32-bytes-minimum-abcdef",
        "service-role-subject",
        timedelta(minutes=5),
        issuer="https://project123.supabase.co/auth/v1",
        extra_claims={"ref": "project123", "role": "service_role"},
    )
    response = client.get("/auth/admin", headers={"Authorization": f"Bearer {token}"})
    assert response.status_code == 200
    assert response.json() == {"user_id": "service_role:project123", "role": "service_role"}


def test_service_role_flag_requires_strict_truthy_value(monkeypatch):
    monkeypatch.setenv("TRR_ADMIN_ALLOW_SERVICE_ROLE", "enabled")

    assert api_auth._service_role_allowed("TRR_ADMIN_ALLOW_SERVICE_ROLE") is False


def test_require_internal_admin_rejects_service_role_by_default(monkeypatch):
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    app = _build_app()
    client = TestClient(app)

    token = _make_token(
        "test-secret-32-bytes-minimum-abcdef",
        "service-role-subject",
        timedelta(minutes=5),
        extra_claims={"role": "service_role"},
    )
    response = client.get("/auth/internal-admin", headers={"Authorization": f"Bearer {token}"})
    assert response.status_code == 403
    assert response.json() == {"detail": "Allowlist admin access required"}


def test_require_internal_admin_accepts_service_role_when_enabled(monkeypatch):
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    monkeypatch.setenv("TRR_INTERNAL_ADMIN_ALLOW_SERVICE_ROLE", "1")
    app = _build_app()
    client = TestClient(app)

    token = _make_token(
        "test-secret-32-bytes-minimum-abcdef",
        "service-role-subject",
        timedelta(minutes=5),
        extra_claims={"role": "service_role"},
    )
    response = client.get("/auth/internal-admin", headers={"Authorization": f"Bearer {token}"})
    assert response.status_code == 200
    assert response.json() == {"user_id": "service_role:unknown", "role": "service_role"}


def test_require_internal_admin_accepts_service_role_with_legacy_supabase_issuer_when_enabled(monkeypatch):
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    monkeypatch.setenv("SUPABASE_PROJECT_REF", "project123")
    monkeypatch.setenv("TRR_INTERNAL_ADMIN_ALLOW_SERVICE_ROLE", "1")
    app = _build_app()
    client = TestClient(app)

    token = _make_token(
        "test-secret-32-bytes-minimum-abcdef",
        "service-role-subject",
        timedelta(minutes=5),
        issuer="supabase",
        extra_claims={"ref": "project123", "role": "service_role"},
    )
    response = client.get("/auth/internal-admin", headers={"Authorization": f"Bearer {token}"})
    assert response.status_code == 200
    assert response.json() == {"user_id": "service_role:project123", "role": "service_role"}


def test_require_cast_screentime_admin_accepts_service_role_with_internal_header(monkeypatch):
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    monkeypatch.setenv("TRR_INTERNAL_ADMIN_SHARED_SECRET", "internal-secret-32-bytes-minimum")
    monkeypatch.setenv("SUPABASE_PROJECT_REF", "project123")
    app = _build_app()
    client = TestClient(app)

    token = _make_token(
        "test-secret-32-bytes-minimum-abcdef",
        "service-role-subject",
        timedelta(minutes=5),
        issuer="https://project123.supabase.co/auth/v1",
        extra_claims={"ref": "project123", "role": "service_role"},
    )
    response = client.get(
        "/auth/cast-screentime-admin",
        headers={
            "Authorization": f"Bearer {token}",
            "X-TRR-Internal-Admin-Secret": "internal-secret-32-bytes-minimum",
        },
    )

    assert response.status_code == 200
    assert response.json() == {"user_id": "service_role:project123", "role": "service_role"}
