"""
Authentication utilities for FastAPI.

Extracts user information from verified Supabase JWT tokens.
All user-scoped writes must enforce ownership using the JWT subject.
"""

from __future__ import annotations

import hmac
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
    Supports both user JWTs (with sub) and service role JWTs (with role=service_role).
    """
    token = get_bearer_token(request)
    if not token:
        return None

    try:
        payload = verify_jwt_token(token)
    except InvalidTokenError as e:
        logger.debug(f"JWT verification failed: {e}")
        return None
    except RuntimeError as exc:
        logger.exception("JWT verification runtime failure")
        raise HTTPException(
            status_code=500,
            detail="Authentication service unavailable",
            headers={"x-error-code": "AUTH_SERVICE_UNAVAILABLE"},
        ) from exc

    role = payload.get("role")

    # Service role tokens don't have a user ID - use the project ref as identifier
    if role == "service_role":
        return {
            "id": f"service_role:{payload.get('ref', 'unknown')}",
            "email": None,
            "role": "service_role",
            "token": token,
        }

    user_id = payload.get("sub") or payload.get("user_id") or payload.get("id")
    if not user_id:
        return None

    return {
        "id": str(user_id),
        "email": payload.get("email"),
        "role": role,
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


async def require_cast_screentime_admin(request: Request, user: CurrentUser) -> dict:
    """
    Require an allowlisted/admin user, or a trusted internal service-role caller.

    Cast screentime app proxies intentionally authenticate the human admin in TRR-APP
    and then call backend routes with a service-role token plus the internal shared secret.
    """
    role = (user.get("role") or "").lower()
    if role == "admin":
        return user

    allowlist = _admin_email_allowlist()
    email = (user.get("email") or "").lower()
    if allowlist and email in allowlist:
        return user

    shared_secret = (os.getenv("TRR_INTERNAL_ADMIN_SHARED_SECRET") or "").strip()
    supplied_secret = (request.headers.get("X-TRR-Internal-Admin-Secret") or "").strip()
    if (
        role == "service_role"
        and shared_secret
        and supplied_secret
        and hmac.compare_digest(supplied_secret, shared_secret)
    ):
        return user

    raise HTTPException(status_code=403, detail="Allowlist admin access required")


CastScreentimeAdminUser = Annotated[dict, Depends(require_cast_screentime_admin)]


async def require_allowlist_admin(user: CurrentUser) -> dict:
    allowlist = _admin_email_allowlist()
    email = (user.get("email") or "").lower()
    if allowlist and email in allowlist:
        return user
    raise HTTPException(status_code=403, detail="Allowlist admin access required")


AllowlistAdminUser = Annotated[dict, Depends(require_allowlist_admin)]


async def require_facebank_seed_admin(request: Request, user: CurrentUser) -> dict:
    """Require allowlist admin, or internal service role with shared secret."""
    allowlist = _admin_email_allowlist()
    email = (user.get("email") or "").lower()
    if allowlist and email in allowlist:
        return user

    role = (user.get("role") or "").lower()
    shared_secret = (os.getenv("TRR_INTERNAL_ADMIN_SHARED_SECRET") or "").strip()
    supplied_secret = (request.headers.get("X-TRR-Internal-Admin-Secret") or "").strip()
    if (
        role == "service_role"
        and shared_secret
        and supplied_secret
        and hmac.compare_digest(supplied_secret, shared_secret)
    ):
        return user

    raise HTTPException(status_code=403, detail="Allowlist admin access required")


FacebankSeedAdminUser = Annotated[dict, Depends(require_facebank_seed_admin)]
