"""Internal admin JWT helpers for trusted service-to-service calls."""

from __future__ import annotations

import os
from typing import Any

import jwt
from jwt import InvalidTokenError

INTERNAL_ADMIN_AUDIENCE = "trr-backend-internal-admin"
DEFAULT_INTERNAL_ADMIN_ISSUER = "trr-app-internal"
INTERNAL_ADMIN_SCOPE = "internal_admin"


def get_internal_admin_issuer() -> str:
    issuer = (os.getenv("TRR_INTERNAL_ADMIN_JWT_ISSUER") or DEFAULT_INTERNAL_ADMIN_ISSUER).strip()
    if not issuer:
        raise RuntimeError("TRR_INTERNAL_ADMIN_JWT_ISSUER resolved to an empty value")
    return issuer


def get_internal_admin_audience() -> str:
    audience = (os.getenv("TRR_INTERNAL_ADMIN_JWT_AUDIENCE") or INTERNAL_ADMIN_AUDIENCE).strip()
    if not audience:
        raise RuntimeError("TRR_INTERNAL_ADMIN_JWT_AUDIENCE resolved to an empty value")
    return audience


def _get_internal_admin_secret() -> str:
    secret = (os.getenv("TRR_INTERNAL_ADMIN_SHARED_SECRET") or "").strip()
    if not secret:
        raise RuntimeError("TRR_INTERNAL_ADMIN_SHARED_SECRET environment variable is not set")
    return secret


def verify_internal_admin_token(token: str) -> dict[str, Any]:
    payload = jwt.decode(
        token,
        _get_internal_admin_secret(),
        algorithms=["HS256"],
        audience=get_internal_admin_audience(),
        issuer=get_internal_admin_issuer(),
        options={
            "verify_iat": True,
            "verify_nbf": True,
            "require": ["exp", "iat", "nbf", "iss", "aud", "sub"],
        },
    )
    if str(payload.get("scope") or "").strip() != INTERNAL_ADMIN_SCOPE:
        raise InvalidTokenError("internal admin token missing required scope")
    return payload


__all__ = [
    "DEFAULT_INTERNAL_ADMIN_ISSUER",
    "INTERNAL_ADMIN_AUDIENCE",
    "INTERNAL_ADMIN_SCOPE",
    "InvalidTokenError",
    "get_internal_admin_audience",
    "get_internal_admin_issuer",
    "verify_internal_admin_token",
]
