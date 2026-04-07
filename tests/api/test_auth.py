"""Auth tests for verified JWT behavior (no Supabase SDK)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import jwt
from fastapi import APIRouter, Depends, FastAPI
from fastapi.testclient import TestClient

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


def test_require_internal_admin_accepts_matching_shared_secret_header(monkeypatch):
    monkeypatch.setenv("TRR_INTERNAL_ADMIN_SHARED_SECRET", "internal-secret-32-bytes-minimum")
    app = _build_app()
    client = TestClient(app)

    response = client.get(
        "/auth/internal-admin",
        headers={"X-TRR-Internal-Admin-Secret": "internal-secret-32-bytes-minimum"},
    )

    assert response.status_code == 200
    assert response.json() == {"user_id": "internal-admin:shared-secret", "role": "internal_admin"}


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


def test_require_admin_accepts_service_role(monkeypatch):
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
    assert response.status_code == 200
    assert response.json() == {"user_id": "service_role:project123", "role": "service_role"}


def test_require_internal_admin_accepts_service_role(monkeypatch):
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
    assert response.status_code == 200
    assert response.json() == {"user_id": "service_role:unknown", "role": "service_role"}


def test_require_internal_admin_accepts_service_role_with_legacy_supabase_issuer(monkeypatch):
    monkeypatch.setenv("SUPABASE_JWT_SECRET", "test-secret-32-bytes-minimum-abcdef")
    monkeypatch.setenv("SUPABASE_PROJECT_REF", "project123")
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
