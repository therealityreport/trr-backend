"""Screenalytics service-to-service auth."""

from __future__ import annotations

import hmac
import os

from fastapi import HTTPException, Request

from api.auth import get_bearer_token
from trr_backend.security.internal_admin import InvalidTokenError, verify_internal_admin_token


def get_screenalytics_service_token() -> str | None:
    token = os.getenv("SCREENALYTICS_SERVICE_TOKEN", "").strip()
    return token or None


def _internal_admin_secret_configured() -> bool:
    return bool((os.getenv("TRR_INTERNAL_ADMIN_SHARED_SECRET") or "").strip())


async def require_screenalytics_service_token(request: Request) -> None:
    token = get_bearer_token(request)
    if not token:
        raise HTTPException(
            status_code=401,
            detail="Authorization required",
            headers={"WWW-Authenticate": "Bearer"},
        )

    expected = get_screenalytics_service_token()
    if expected and hmac.compare_digest(token, expected):
        return None

    if expected and not _internal_admin_secret_configured():
        raise HTTPException(
            status_code=401,
            detail="Invalid service token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    try:
        verify_internal_admin_token(token)
        return None
    except RuntimeError as exc:
        raise HTTPException(
            status_code=500,
            detail="Authentication service unavailable",
            headers={"x-error-code": "AUTH_SERVICE_UNAVAILABLE"},
        ) from exc
    except InvalidTokenError:
        pass

    if expected:
        raise HTTPException(
            status_code=401,
            detail="Invalid service token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    raise HTTPException(
        status_code=401,
        detail="Valid internal admin token required",
        headers={"WWW-Authenticate": "Bearer"},
    )
