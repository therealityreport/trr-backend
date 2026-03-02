"""Auth tests for verified JWT behavior (no Supabase SDK)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import jwt
from fastapi import APIRouter, Depends, FastAPI
from fastapi.testclient import TestClient

from api.auth import CurrentUser, get_current_user


def _make_token(secret: str, subject: str, expires_delta: timedelta) -> str:
    now = datetime.now(tz=UTC)
    payload = {
        "sub": subject,
        "iat": int(now.timestamp()),
        "exp": int((now + expires_delta).timestamp()),
    }
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
    app = _build_app()
    client = TestClient(app)

    token = _make_token("test-secret-32-bytes-minimum-abcdef", "user-123", timedelta(minutes=5))
    response = client.get("/auth/required", headers={"Authorization": f"Bearer {token}"})
    assert response.status_code == 200
    assert response.json()["user_id"] == "user-123"
