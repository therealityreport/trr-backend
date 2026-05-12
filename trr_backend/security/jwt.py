"""JWT verification helpers (no Supabase SDK)."""

from __future__ import annotations

import os
from typing import Any
from urllib.parse import urlparse

import jwt
from jwt import DecodeError, ExpiredSignatureError, InvalidSignatureError, InvalidTokenError


def _get_jwt_secret() -> str:
    secret = (os.getenv("SUPABASE_JWT_SECRET") or "").strip()
    if not secret:
        raise RuntimeError("SUPABASE_JWT_SECRET environment variable is not set")
    return secret


def _project_ref_from_supabase_url(raw: str) -> str | None:
    if not raw:
        return None
    try:
        host = (urlparse(raw).hostname or "").strip().lower()
    except Exception:
        return None
    if host.endswith(".supabase.co"):
        return host.removesuffix(".supabase.co")
    return None


def _project_ref_from_db_url(raw: str) -> str | None:
    if not raw:
        return None
    try:
        parsed = urlparse(raw)
    except Exception:
        return None
    username = parsed.username or ""
    if username.startswith("postgres."):
        return username.removeprefix("postgres.")
    host = (parsed.hostname or "").strip().lower()
    if host.startswith("db.") and host.endswith(".supabase.co"):
        return host.removeprefix("db.").removesuffix(".supabase.co")
    return None


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
        project_ref = _project_ref_from_supabase_url(raw)
        if project_ref:
            return project_ref

    for db_env in ("TRR_DB_DIRECT_URL", "TRR_DB_SESSION_URL", "TRR_DB_URL", "TRR_DB_FALLBACK_URL"):
        raw = (os.getenv(db_env) or "").strip()
        if not raw:
            continue
        project_ref = _project_ref_from_db_url(raw)
        if project_ref:
            return project_ref
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


def describe_supabase_jwt_context() -> list[str]:
    warnings: list[str] = []
    project_ref_candidates: dict[str, str] = {}

    explicit_project_ref = (
        os.getenv("SUPABASE_PROJECT_REF")
        or os.getenv("TRR_SUPABASE_PROJECT_REF")
        or os.getenv("TRR_CORE_SUPABASE_PROJECT_REF")
        or ""
    ).strip()
    if explicit_project_ref:
        project_ref_candidates["SUPABASE_PROJECT_REF"] = explicit_project_ref

    for url_env in ("TRR_CORE_SUPABASE_URL", "SUPABASE_URL"):
        raw = (os.getenv(url_env) or "").strip()
        project_ref = _project_ref_from_supabase_url(raw)
        if project_ref:
            project_ref_candidates[url_env] = project_ref

    for db_env in ("TRR_DB_DIRECT_URL", "TRR_DB_SESSION_URL", "TRR_DB_URL", "TRR_DB_FALLBACK_URL"):
        raw = (os.getenv(db_env) or "").strip()
        project_ref = _project_ref_from_db_url(raw)
        if project_ref:
            project_ref_candidates[db_env] = project_ref

    unique_project_refs = {value for value in project_ref_candidates.values() if value}
    if not unique_project_refs:
        warnings.append(
            "Unable to derive a Supabase project ref from SUPABASE_PROJECT_REF, "
            "TRR_CORE_SUPABASE_URL, SUPABASE_URL, TRR_DB_DIRECT_URL, TRR_DB_SESSION_URL, TRR_DB_URL, "
            "or TRR_DB_FALLBACK_URL.",
        )
    elif len(unique_project_refs) > 1:
        rendered = ", ".join(f"{source}={ref}" for source, ref in sorted(project_ref_candidates.items()))
        warnings.append(
            "Conflicting Supabase project ref candidates were found; issuer checks will use the explicit "
            f"or first derived value. Candidates: {rendered}",
        )

    explicit_issuer = (os.getenv("SUPABASE_JWT_ISSUER") or "").strip()
    if explicit_issuer == "supabase":
        warnings.append(
            "SUPABASE_JWT_ISSUER=supabase is legacy compatibility for service_role JWTs only; "
            "user JWT issuer checks still use the derived Supabase project issuer.",
        )

    return warnings


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
    try:
        payload = jwt.decode(
            token,
            secret,
            algorithms=["HS256"],
            options=options,
        )
    except ExpiredSignatureError as exc:
        raise InvalidTokenError("Supabase JWT has expired") from exc
    except InvalidSignatureError as exc:
        raise InvalidTokenError("Supabase JWT signature does not match the configured secret") from exc
    except DecodeError as exc:
        raise InvalidTokenError("Supabase JWT is malformed") from exc
    role = str(payload.get("role") or "").strip().lower()
    token_issuer = str(payload.get("iss") or "").strip()
    if expected_issuer and token_issuer and token_issuer != expected_issuer:
        if not (role == "service_role" and token_issuer == "supabase"):
            raise InvalidTokenError(
                "Supabase JWT issuer does not match the configured backend project issuer",
            )
    token_project_ref = str(payload.get("ref") or "").strip()
    if expected_project_ref and token_project_ref and token_project_ref != expected_project_ref:
        raise InvalidTokenError(
            "Supabase JWT project ref does not match the configured backend project ref",
        )
    return payload


__all__ = [
    "InvalidTokenError",
    "describe_supabase_jwt_context",
    "expected_supabase_issuer",
    "expected_supabase_project_ref",
    "verify_jwt_token",
]
