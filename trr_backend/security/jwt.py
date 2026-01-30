"""JWT verification helpers (no Supabase SDK)."""

from __future__ import annotations

import os
from typing import Any

import jwt
from jwt import InvalidTokenError


def _get_jwt_secret() -> str:
    secret = (os.getenv("SUPABASE_JWT_SECRET") or "").strip()
    if not secret:
        raise RuntimeError("SUPABASE_JWT_SECRET environment variable is not set")
    return secret


def verify_jwt_token(token: str) -> dict[str, Any]:
    secret = _get_jwt_secret()
    options = {
        "verify_aud": False,
        "verify_iat": True,
        "verify_nbf": True,
        "require": ["exp"],
    }
    # Audience is intentionally not enforced because Supabase tokens in this
    # project do not set a stable aud claim across environments.
    return jwt.decode(token, secret, algorithms=["HS256"], options=options)


__all__ = ["InvalidTokenError", "verify_jwt_token"]
