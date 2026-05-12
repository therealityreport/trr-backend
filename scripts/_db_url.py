from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass
from urllib.parse import SplitResult, urlsplit, urlunsplit


@dataclass(frozen=True)
class ResolvedDbUrl:
    value: str
    source: str
    deprecated: bool = False


def _read_env(name: str) -> str:
    return (os.getenv(name) or "").strip()


def _resolve_local_supabase_status_db_url() -> str | None:
    try:
        result = subprocess.run(
            ["supabase", "status", "--output", "env"],
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError:
        return None

    for line in (result.stdout or "").splitlines():
        if not line.startswith("DB_URL="):
            continue
        value = line.split("=", 1)[1].strip().strip('"').strip("'")
        if value:
            return value
    return None


def resolve_db_url(
    *,
    allow_database_url: bool = False,
    allow_deprecated_supabase_db_url: bool = True,
    allow_local_supabase_status: bool = True,
) -> ResolvedDbUrl:
    ordered_sources: list[tuple[str, str, bool]] = [
        ("TRR_DB_DIRECT_URL", _read_env("TRR_DB_DIRECT_URL"), False),
        ("TRR_DB_SESSION_URL", _read_env("TRR_DB_SESSION_URL"), False),
        ("TRR_DB_URL", _read_env("TRR_DB_URL"), False),
        ("TRR_DB_FALLBACK_URL", _read_env("TRR_DB_FALLBACK_URL"), False),
    ]
    if allow_database_url:
        ordered_sources.append(("DATABASE_URL", _read_env("DATABASE_URL"), True))
    if allow_deprecated_supabase_db_url:
        ordered_sources.append(("SUPABASE_DB_URL", _read_env("SUPABASE_DB_URL"), True))

    for source, value, deprecated in ordered_sources:
        if value:
            return ResolvedDbUrl(value=value, source=source, deprecated=deprecated)

    if allow_local_supabase_status:
        local_value = _resolve_local_supabase_status_db_url()
        if local_value:
            return ResolvedDbUrl(value=local_value, source="supabase status (local)", deprecated=False)

    raise RuntimeError(
        "No database URL configured. Set TRR_DB_DIRECT_URL, TRR_DB_SESSION_URL, TRR_DB_URL, "
        "optionally TRR_DB_FALLBACK_URL, "
        "and use DATABASE_URL only for tooling-specific flows."
    )


def _project_ref_from_username(username: str | None) -> str | None:
    normalized = (username or "").strip()
    if not normalized.startswith("postgres."):
        return None
    project_ref = normalized.partition(".")[2].strip()
    return project_ref or None


def _replace_netloc(
    parsed: SplitResult,
    *,
    hostname: str,
    port: int | None,
    username: str | None = None,
) -> str:
    effective_username = parsed.username or "" if username is None else username
    password = parsed.password or ""
    auth = effective_username
    if password:
        auth = f"{auth}:{password}"
    host = hostname
    if port is not None:
        host = f"{host}:{port}"
    netloc = f"{auth}@{host}" if auth else host
    return urlunsplit((parsed.scheme, netloc, parsed.path, parsed.query, parsed.fragment))


def to_direct_db_url(url: str) -> str:
    """Convert a Supabase pooler URL to the direct `db.<ref>.supabase.co:5432` URL.

    Tooling uses direct connections for concurrent DDL and observer queries.
    Runtime code must not call this helper.
    """

    parsed = urlsplit(url)
    host = (parsed.hostname or "").strip().lower()
    if not host:
        raise RuntimeError("Database URL has no hostname.")
    if host in {"localhost", "127.0.0.1"} or host.endswith(".supabase.co"):
        return url
    if not host.endswith("pooler.supabase.com"):
        raise RuntimeError(f"Cannot derive direct Supabase host from non-pooler URL host={host!r}.")

    project_ref = _project_ref_from_username(parsed.username)
    if not project_ref:
        raise RuntimeError(
            "Cannot derive direct Supabase host because the URL username does not include a project ref "
            "(expected `postgres.<project-ref>`)."
        )
    return _replace_netloc(
        parsed,
        hostname=f"db.{project_ref}.supabase.co",
        port=5432,
        username="postgres",
    )


def resolve_direct_db_url(
    *,
    allow_database_url: bool = False,
    allow_deprecated_supabase_db_url: bool = True,
    allow_local_supabase_status: bool = True,
) -> ResolvedDbUrl:
    """Resolve the current tooling DB URL and normalize it to a direct connection."""

    resolved = resolve_db_url(
        allow_database_url=allow_database_url,
        allow_deprecated_supabase_db_url=allow_deprecated_supabase_db_url,
        allow_local_supabase_status=allow_local_supabase_status,
    )
    return ResolvedDbUrl(
        value=to_direct_db_url(resolved.value),
        source=resolved.source,
        deprecated=resolved.deprecated,
    )
