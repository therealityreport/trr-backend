"""Auth shim for retained internal screentime worker endpoints."""

from __future__ import annotations

import logging

from fastapi import HTTPException, Request

from api.auth import get_bearer_token
from trr_backend.security.internal_admin import InvalidTokenError, verify_internal_admin_token

logger = logging.getLogger(__name__)


async def require_screenalytics_service_token(request: Request) -> None:
    token = get_bearer_token(request)
    if not token:
        raise HTTPException(
            status_code=401,
            detail="Authorization required",
            headers={"WWW-Authenticate": "Bearer"},
        )

    try:
        verify_internal_admin_token(token)
        logger.debug("retained screentime worker request accepted via internal admin token")
        return None
    except RuntimeError as exc:
        raise HTTPException(
            status_code=500,
            detail="Authentication service unavailable",
            headers={"x-error-code": "AUTH_SERVICE_UNAVAILABLE"},
        ) from exc
    except InvalidTokenError:
        logger.info("retained screentime worker request rejected: invalid internal admin token")

    raise HTTPException(
        status_code=401,
        detail="Valid internal admin token required for retained screentime worker endpoints",
        headers={"WWW-Authenticate": "Bearer"},
    )
