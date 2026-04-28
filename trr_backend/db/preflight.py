"""
Database preflight checks for TRR Backend.

Use these helpers to fail fast with clear errors when migrations or scripts
target the wrong database (e.g., missing core schema).
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from trr_backend.db.session import DbSession


class DatabasePreflightError(RuntimeError):
    """Raised when a database preflight check fails."""

    pass


def _is_missing_schema_error(message: str) -> bool:
    """Check if error indicates schema does not exist."""
    msg = (message or "").casefold()
    return (
        "3f000" in msg  # invalid_schema_name
        or "schema" in msg
        and "does not exist" in msg
        or "pgrst106" in msg  # postgrest invalid schema
        or ("invalid schema" in msg and "core" in msg)
    )


def _is_missing_table_error(message: str) -> bool:
    """Check if error indicates table does not exist."""
    msg = (message or "").casefold()
    return (
        "42p01" in msg  # undefined_table
        or "pgrst205" in msg  # postgrest relation not found
        or ("relation" in msg and "does not exist" in msg)
        or ("could not find" in msg and "relation" in msg)
    )


def assert_core_schema_exists(db: DbSession) -> None:
    """
    Verify that the `core` schema exists in the connected database.

    Raises DatabasePreflightError with actionable guidance if the schema is missing.
    This prevents accidental migrations against the wrong database.
    """
    try:
        response = db.schema("core").table("shows").select("id").limit(1).execute()
    except Exception as exc:
        msg = str(exc)
        if _is_missing_schema_error(msg):
            raise DatabasePreflightError(
                "Database preflight failed: schema `core` does not exist.\n"
                "This likely means you're connected to the wrong database.\n"
                "Check TRR_DB_URL / TRR_DB_FALLBACK_URL environment variables.\n"
                "Runtime paths use direct Postgres access; do not troubleshoot this through PostgREST exposed schemas."
            ) from exc
        if _is_missing_table_error(msg):
            raise DatabasePreflightError(
                "Database preflight failed: table `core.shows` does not exist.\n"
                "Run migrations with `supabase db push` before running scripts."
            ) from exc
        raise DatabasePreflightError(f"Database preflight failed: {exc}") from exc

    error = getattr(response, "error", None)
    if not error:
        return

    parts = [
        str(getattr(error, "code", "") or ""),
        str(getattr(error, "message", "") or ""),
        str(getattr(error, "details", "") or ""),
        str(error),
    ]
    combined = " ".join([p for p in parts if p]).strip()
    if _is_missing_schema_error(combined):
        raise DatabasePreflightError(
            "Database preflight failed: schema `core` does not exist.\n"
            "This likely means you're connected to the wrong database.\n"
            "Check TRR_DB_URL / TRR_DB_FALLBACK_URL environment variables.\n"
            "Runtime paths use direct Postgres access; do not troubleshoot this through PostgREST exposed schemas."
        )
    if _is_missing_table_error(combined):
        raise DatabasePreflightError(
            "Database preflight failed: table `core.shows` does not exist.\n"
            "Run migrations with `supabase db push` before running scripts."
        )
    raise DatabasePreflightError(f"Database preflight failed: {combined}")


def assert_core_schema_exists_sql(connection) -> None:  # noqa: ANN001
    """Verify that the connected database exposes the core schema via direct SQL."""

    try:
        with connection.cursor() as cursor:
            cursor.execute("select exists (select 1 from information_schema.schemata where schema_name = 'core')")
            row = cursor.fetchone()
    except Exception as exc:  # noqa: BLE001
        raise DatabasePreflightError(f"Database preflight failed: {exc}") from exc

    schema_exists = bool(row[0]) if row else False
    if not schema_exists:
        raise DatabasePreflightError(
            "Database preflight failed: schema `core` does not exist.\n"
            "This likely means you're connected to the wrong database.\n"
            "Check TRR_DB_URL / TRR_DB_FALLBACK_URL environment variables.\n"
            "Runtime paths use direct Postgres access; do not troubleshoot this through PostgREST exposed schemas."
        )


def assert_migration_safe(*, require_core_schema: bool = True) -> None:
    """
    Standalone check for migration tooling that may still use raw Postgres URLs.

    Runtime code should use TRR_DB_DIRECT_URL, TRR_DB_SESSION_URL, TRR_DB_URL,
    and optional TRR_DB_FALLBACK_URL in precedence order.
    Tooling may still accept DATABASE_URL for isolated migration or psql flows.

    Args:
        require_core_schema: If True, warn if migrating against a DB that likely
                             doesn't have the core schema (heuristic based on URL).
    """
    db_url = (
        os.getenv("TRR_DB_DIRECT_URL")
        or os.getenv("TRR_DB_SESSION_URL")
        or os.getenv("TRR_DB_URL")
        or os.getenv("TRR_DB_FALLBACK_URL")
        or os.getenv("DATABASE_URL")
        or ""
    ).strip()
    if not db_url:
        raise DatabasePreflightError(
            "No database URL configured.\n"
            "Set TRR_DB_DIRECT_URL for local direct use, TRR_DB_SESSION_URL or TRR_DB_URL for runtime use, "
            "optionally TRR_DB_FALLBACK_URL for an explicit fallback, "
            "or DATABASE_URL only for tool-specific migration flows."
        )

    if require_core_schema and db_url:
        # Heuristic: local postgres without supabase likely doesn't have core schema
        if "localhost" in db_url or "127.0.0.1" in db_url:
            if "supabase" not in db_url.lower():
                print(
                    "WARNING: runtime DB URL points to localhost. "
                    "Ensure `core` schema exists before running migrations."
                )
