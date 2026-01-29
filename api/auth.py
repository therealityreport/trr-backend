"""
Authentication utilities for FastAPI.

Extracts user information from Supabase JWT tokens.
All writes must use the user-scoped client to enforce RLS.
"""

from __future__ import annotations

import base64
import json
import logging
from typing import Annotated, Any

from fastapi import Depends, HTTPException, Request

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

    Note: This performs a lightweight decode without signature verification.
    Use service boundaries or upstream auth for full verification.
    """
    token = get_bearer_token(request)
    if not token:
        return None

    payload = _decode_jwt_payload(token)
    if not payload:
        return None

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


def _decode_jwt_payload(token: str) -> dict[str, Any] | None:
    parts = token.split(".")
    if len(parts) != 3:
        return None
    payload = parts[1]
    padding = "=" * (-len(payload) % 4)
    try:
        decoded = base64.urlsafe_b64decode(payload + padding)
        return json.loads(decoded.decode("utf-8"))
    except (ValueError, json.JSONDecodeError):
        return None


def get_user_supabase_client(user: dict) -> Any:
    """
    Placeholder for user-scoped DB access.

    Supabase Python SDK is intentionally not used in this repo.
    """
    raise RuntimeError(
        "Supabase Python SDK is disabled. Use the direct DB layer or PostgREST."
    )


# Type alias for dependency injection
CurrentUser = Annotated[dict, Depends(require_user)]
OptionalUser = Annotated[dict | None, Depends(get_current_user)]
