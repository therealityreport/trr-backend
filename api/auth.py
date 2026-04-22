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

from trr_backend.security.internal_admin import verify_internal_admin_token
from trr_backend.security.jwt import InvalidTokenError, verify_jwt_token

logger = logging.getLogger(__name__)


def _env_flag_strict(name: str, default: bool = False) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


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


def _build_internal_admin_identity(payload: dict[str, Any], token: str) -> dict[str, Any]:
    return {
        "id": str(payload.get("sub") or "internal-admin"),
        "email": None,
        "role": "internal_admin",
        "token": token,
        "issuer": payload.get("iss"),
        "scope": payload.get("scope"),
        "admin_uid": payload.get("admin_uid"),
        "admin_email": payload.get("admin_email"),
        "verified_at": payload.get("verified_at"),
    }


def _internal_admin_secret_matches(request: Request) -> bool:
    secret = (os.getenv("TRR_INTERNAL_ADMIN_SHARED_SECRET") or "").strip()
    header_secret = (request.headers.get("X-TRR-Internal-Admin-Secret") or "").strip()
    return bool(secret and header_secret and hmac.compare_digest(header_secret, secret))


def _raw_internal_admin_fallback_matches(request: Request) -> bool:
    if not _env_flag_strict("TRR_INTERNAL_ADMIN_ALLOW_RAW_SECRET_FALLBACK", False):
        return False
    logger.warning("[auth] raw-secret-fallback engaged; dev-only flag TRR_INTERNAL_ADMIN_ALLOW_RAW_SECRET_FALLBACK is enabled")
    return _internal_admin_secret_matches(request)


def _service_role_allowed(env_name: str) -> bool:
    if not _env_flag_strict(env_name, False):
        return False
    logger.warning("[auth] service-role bypass engaged via %s; dev-only flag is enabled", env_name)
    return True


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
        logger.debug("JWT verification failed: %s", e)
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
    if role in ("admin", "internal_admin"):
        return user
    if role == "service_role" and _service_role_allowed("TRR_ADMIN_ALLOW_SERVICE_ROLE"):
        return user
    allowlist = _admin_email_allowlist()
    email = (user.get("email") or "").lower()
    if allowlist and email in allowlist:
        return user
    raise HTTPException(status_code=403, detail="Admin access required")


AdminUser = Annotated[dict, Depends(require_admin)]


def _allowlist_match(user: dict | None) -> dict | None:
    if not user:
        return None
    role = (user.get("role") or "").lower()
    if role == "admin":
        return user
    allowlist = _admin_email_allowlist()
    email = (user.get("email") or "").lower()
    if allowlist and email in allowlist:
        return user
    return None


async def require_internal_admin(request: Request) -> dict:
    """
    Require an allowlisted/admin user, or a trusted internal service JWT caller.

    App-side admin proxy routes authenticate the human admin in TRR-APP and then
    call backend routes with a signed internal admin JWT.
    """
    try:
        current_user = await get_current_user(request)
        matched_user = _allowlist_match(current_user)
    except HTTPException as exc:
        if exc.status_code == 500 and exc.headers.get("x-error-code") == "AUTH_SERVICE_UNAVAILABLE":
            current_user = None
            matched_user = None
        else:
            raise
    if matched_user:
        return matched_user

    role = (current_user or {}).get("role") if isinstance(current_user, dict) else None
    if role == "service_role" and _service_role_allowed("TRR_INTERNAL_ADMIN_ALLOW_SERVICE_ROLE"):
        return current_user

    if _raw_internal_admin_fallback_matches(request):
        logger.info("internal admin auth fallback accepted via raw shared secret header")
        return {
            "id": "internal-admin:shared-secret",
            "email": None,
            "role": "internal_admin",
            "token": get_bearer_token(request),
            "issuer": "shared-secret",
            "scope": "internal_admin",
        }

    token = get_bearer_token(request)
    internal_admin_secret = (os.getenv("TRR_INTERNAL_ADMIN_SHARED_SECRET") or "").strip()
    attempted_internal_admin_verification = False
    if token and internal_admin_secret:
        attempted_internal_admin_verification = True
        try:
            return _build_internal_admin_identity(verify_internal_admin_token(token), token)
        except InvalidTokenError:
            pass
        except RuntimeError as exc:
            logger.exception("Internal admin verification runtime failure")
            raise HTTPException(
                status_code=500,
                detail="Authentication service unavailable",
                headers={"x-error-code": "AUTH_SERVICE_UNAVAILABLE"},
            ) from exc

    if current_user:
        raise HTTPException(status_code=403, detail="Allowlist admin access required")
    if attempted_internal_admin_verification:
        raise HTTPException(status_code=403, detail="Allowlist admin access required")

    raise HTTPException(
        status_code=401,
        detail="Authentication required. Please provide a valid access token.",
        headers={"WWW-Authenticate": "Bearer"},
    )


InternalAdminUser = Annotated[dict, Depends(require_internal_admin)]


async def require_cast_screentime_admin(request: Request) -> dict:
    """
    Require an allowlisted/admin user, or a trusted internal service caller.

    Cast screentime app proxies intentionally authenticate the human admin in TRR-APP
    and then call backend routes with a signed internal admin JWT.
    """
    try:
        current_user = await get_current_user(request)
    except HTTPException as exc:
        if exc.status_code == 500 and exc.headers.get("x-error-code") == "AUTH_SERVICE_UNAVAILABLE":
            current_user = None
        else:
            raise

    allowlisted_user = _allowlist_match(current_user)
    if allowlisted_user:
        return allowlisted_user

    role = (current_user or {}).get("role") if isinstance(current_user, dict) else None
    if role == "service_role":
        if _internal_admin_secret_matches(request):
            return current_user
        raise HTTPException(status_code=403, detail="Allowlist admin access required")

    return await require_internal_admin(request)


CastScreentimeAdminUser = Annotated[dict, Depends(require_cast_screentime_admin)]


async def require_allowlist_admin(user: CurrentUser) -> dict:
    allowlist = _admin_email_allowlist()
    email = (user.get("email") or "").lower()
    if allowlist and email in allowlist:
        return user
    raise HTTPException(status_code=403, detail="Allowlist admin access required")


AllowlistAdminUser = Annotated[dict, Depends(require_allowlist_admin)]


async def require_facebank_seed_admin(request: Request) -> dict:
    """Require allowlist admin, or a signed internal admin caller with secret header."""
    try:
        current_user = await get_current_user(request)
    except HTTPException as exc:
        if exc.status_code == 500 and exc.headers.get("x-error-code") == "AUTH_SERVICE_UNAVAILABLE":
            current_user = None
        else:
            raise

    allowlisted_user = _allowlist_match(current_user)
    if allowlisted_user:
        return allowlisted_user

    role = (current_user or {}).get("role") if isinstance(current_user, dict) else None
    if role == "service_role":
        if _internal_admin_secret_matches(request):
            return current_user
        raise HTTPException(status_code=403, detail="Allowlist admin access required")

    return await require_internal_admin(request)


FacebankSeedAdminUser = Annotated[dict, Depends(require_facebank_seed_admin)]
