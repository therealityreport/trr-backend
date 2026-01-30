"""
Authentication utilities for FastAPI.

Extracts user information from verified Supabase JWT tokens.
All user-scoped writes must enforce ownership using the JWT subject.
"""

from __future__ import annotations

import logging
import os
from typing import Annotated, Any

from fastapi import Depends, HTTPException, Request

from trr_backend.security.jwt import InvalidTokenError, verify_jwt_token

logger = logging.getLogger(__name__)


def get_bearer_token(request: Request) -> str | None:
    """
    Extract Bearer token from Authorization header.

    Returns None if no token is present.
    """
    auth_header = request.headers.get("Authorization")
    if not auth_header:
        return None

    parts = auth_header.split()
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return None

    return parts[1]


async def get_current_user(request: Request) -> dict | None:
    """
    Get the current user from the JWT token payload.

    The token is verified (signature + exp validated).
    """
    token = get_bearer_token(request)
    if not token:
        return None

    try:
        payload = verify_jwt_token(token)
    except InvalidTokenError:
        return None
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    user_id = payload.get("sub") or payload.get("user_id") or payload.get("id")
    if not user_id:
        return None

    return {
        "id": str(user_id),
        "email": payload.get("email"),
        "role": payload.get("role"),
        "token": token,
    }


async def require_user(request: Request) -> dict:
    """
    Dependency that requires a valid authenticated user.

    Raises 401 if no token or invalid token.
    Returns user dict with 'id', 'email', 'token', etc. if valid.
    """
    user = await get_current_user(request)
    if not user:
        raise HTTPException(
            status_code=401,
            detail="Authentication required. Please provide a valid access token.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return user


def get_user_db_session(user: dict) -> Any:
    """Return a DB session for user-scoped operations."""
    from trr_backend.db.session import get_db_session

    return get_db_session()


# Type alias for dependency injection
CurrentUser = Annotated[dict, Depends(require_user)]
OptionalUser = Annotated[dict | None, Depends(get_current_user)]


def _admin_email_allowlist() -> set[str]:
    raw = os.getenv("ADMIN_EMAIL_ALLOWLIST", "").strip()
    if not raw:
        return set()
    return {email.strip().lower() for email in raw.split(",") if email.strip()}


async def require_admin(user: CurrentUser) -> dict:
    role = (user.get("role") or "").lower()
    if role in ("service_role", "admin"):
        return user
    allowlist = _admin_email_allowlist()
    email = (user.get("email") or "").lower()
    if allowlist and email in allowlist:
        return user
    raise HTTPException(status_code=403, detail="Admin access required")


AdminUser = Annotated[dict, Depends(require_admin)]
