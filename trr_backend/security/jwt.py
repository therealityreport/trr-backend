"""JWT verification helpers (no Supabase SDK)."""

from __future__ import annotations

import os
from typing import Any
from urllib.parse import urlparse

import jwt
from jwt import InvalidTokenError


def _get_jwt_secret() -> str:
    secret = (os.getenv("SUPABASE_JWT_SECRET") or "").strip()
    if not secret:
        raise RuntimeError("SUPABASE_JWT_SECRET environment variable is not set")
    return secret


def _candidate_supabase_project_ref() -> str | None:
    explicit = (
        os.getenv("SUPABASE_PROJECT_REF")
        or os.getenv("TRR_SUPABASE_PROJECT_REF")
        or os.getenv("TRR_CORE_SUPABASE_PROJECT_REF")
        or ""
    ).strip()
    if explicit:
        return explicit

    for url_env in ("TRR_CORE_SUPABASE_URL", "SUPABASE_URL"):
        raw = (os.getenv(url_env) or "").strip()
        if not raw:
            continue
        try:
            host = (urlparse(raw).hostname or "").strip().lower()
        except Exception:
            continue
        if host.endswith(".supabase.co"):
            return host.removesuffix(".supabase.co")

    for db_env in ("TRR_DB_URL", "TRR_DB_FALLBACK_URL"):
        raw = (os.getenv(db_env) or "").strip()
        if not raw:
            continue
        try:
            parsed = urlparse(raw)
        except Exception:
            continue
        username = parsed.username or ""
        if username.startswith("postgres."):
            return username.removeprefix("postgres.")
        host = (parsed.hostname or "").strip().lower()
        if host.startswith("db.") and host.endswith(".supabase.co"):
            return host.removeprefix("db.").removesuffix(".supabase.co")
    return None


def expected_supabase_project_ref() -> str | None:
    return _candidate_supabase_project_ref()


def expected_supabase_issuer() -> str | None:
    explicit = (os.getenv("SUPABASE_JWT_ISSUER") or "").strip()
    if explicit:
        return explicit
    project_ref = expected_supabase_project_ref()
    if not project_ref:
        return None
    return f"https://{project_ref}.supabase.co/auth/v1"


def verify_jwt_token(token: str) -> dict[str, Any]:
    secret = _get_jwt_secret()
    expected_issuer = expected_supabase_issuer()
    expected_project_ref = expected_supabase_project_ref()
    options = {
        "verify_aud": False,
        "verify_iss": False,
        "verify_iat": True,
        "verify_nbf": True,
        "require": ["exp"],
    }
    # Audience is intentionally not enforced because Supabase tokens in this
    # project do not set a stable aud claim across environments.
    payload = jwt.decode(
        token,
        secret,
        algorithms=["HS256"],
        options=options,
    )
    token_issuer = str(payload.get("iss") or "").strip()
    if expected_issuer and token_issuer and token_issuer != expected_issuer:
        raise InvalidTokenError("Supabase JWT issuer does not match configured backend project")
    token_project_ref = str(payload.get("ref") or "").strip()
    if expected_project_ref and token_project_ref and token_project_ref != expected_project_ref:
        raise InvalidTokenError("Supabase JWT project ref does not match configured backend project")
    return payload


__all__ = [
    "InvalidTokenError",
    "expected_supabase_issuer",
    "expected_supabase_project_ref",
    "verify_jwt_token",
]
