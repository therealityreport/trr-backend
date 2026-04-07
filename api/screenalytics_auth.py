"""Screenalytics service-to-service auth."""

from __future__ import annotations

import hmac
import logging
import os

from fastapi import HTTPException, Request

from api.auth import get_bearer_token
from trr_backend.security.internal_admin import InvalidTokenError, verify_internal_admin_token

logger = logging.getLogger(__name__)


def get_screenalytics_service_token() -> str | None:
    token = os.getenv("SCREENALYTICS_SERVICE_TOKEN", "").strip()
    return token or None


def _internal_admin_secret_configured() -> bool:
    return bool((os.getenv("TRR_INTERNAL_ADMIN_SHARED_SECRET") or "").strip())


def _env_flag(name: str, default: bool = False) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw not in {"0", "false", "no", "off"}


async def require_screenalytics_service_token(request: Request) -> None:
    token = get_bearer_token(request)
    if not token:
        raise HTTPException(
            status_code=401,
            detail="Authorization required",
            headers={"WWW-Authenticate": "Bearer"},
        )

    expected = get_screenalytics_service_token()
    allow_service_token_fallback = _env_flag("TRR_SCREENALYTICS_ALLOW_SERVICE_TOKEN_FALLBACK", True)

    if expected and allow_service_token_fallback and hmac.compare_digest(token, expected):
        logger.info("screenalytics auth fallback accepted via service token")
        return None

    if expected and allow_service_token_fallback and not _internal_admin_secret_configured():
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

    if expected and allow_service_token_fallback:
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
