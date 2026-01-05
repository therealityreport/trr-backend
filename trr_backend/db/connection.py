"""
Unified database connection resolution for TRR Backend.

This module provides a single source of truth for resolving database URLs,
with support for local Supabase development and remote production environments.
"""
from __future__ import annotations

import os
import subprocess
import sys
from functools import lru_cache


class DatabaseConnectionError(RuntimeError):
    """Raised when database connection cannot be established."""

    pass


def _parse_supabase_status_env(output: str) -> dict[str, str]:
    """Parse output from `supabase status --output env`."""
    env_vars: dict[str, str] = {}
    for line in output.strip().splitlines():
        line = line.strip()
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and value:
            env_vars[key] = value
    return env_vars


def _get_local_supabase_db_url() -> str | None:
    """
    Try to get DB_URL from local Supabase instance via `supabase status`.

    Returns None if Supabase CLI is not available or not running.
    """
    try:
        result = subprocess.run(
            ["supabase", "status", "--output", "env"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode != 0:
            return None
        env_vars = _parse_supabase_status_env(result.stdout)
        return env_vars.get("DB_URL")
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        return None


@lru_cache(maxsize=1)
def resolve_database_url(*, allow_local_fallback: bool = True) -> str:
    """
    Resolve the database URL using a prioritized lookup.

    Priority order:
    1. SUPABASE_DB_URL - Explicit Supabase database URL (recommended for remote)
    2. DATABASE_URL - Standard Postgres connection string
    3. TRR_DB_URL - Legacy alias
    4. (Local only) `supabase status --output env` DB_URL - Local Supabase instance

    Args:
        allow_local_fallback: If True, try to resolve from local Supabase instance
                              when env vars are not set. Set to False for production.

    Returns:
        Database connection URL string.

    Raises:
        DatabaseConnectionError: If no valid database URL can be resolved.
    """
    # Priority 1: Explicit Supabase DB URL
    url = (os.getenv("SUPABASE_DB_URL") or "").strip()
    if url:
        return url

    # Priority 2: Standard DATABASE_URL
    url = (os.getenv("DATABASE_URL") or "").strip()
    if url:
        return url

    # Priority 3: Legacy TRR_DB_URL
    url = (os.getenv("TRR_DB_URL") or "").strip()
    if url:
        return url

    # Priority 4: Local Supabase fallback
    if allow_local_fallback:
        url = _get_local_supabase_db_url()
        if url:
            return url

    raise DatabaseConnectionError(
        "No database URL configured.\n\n"
        "For remote/production:\n"
        "  Set SUPABASE_DB_URL to your Supabase direct connection string.\n"
        "  Example: postgresql://postgres.<project>:<password>@<host>:5432/postgres\n\n"
        "For local development:\n"
        "  Start local Supabase: supabase start\n"
        "  Or set DATABASE_URL to your local Postgres connection string.\n\n"
        "Available environment variables (checked in order):\n"
        "  - SUPABASE_DB_URL (recommended for production)\n"
        "  - DATABASE_URL\n"
        "  - TRR_DB_URL\n"
    )


def get_psql_command(database_url: str | None = None) -> list[str]:
    """
    Get the psql command with connection string.

    Args:
        database_url: Optional URL override. If not provided, uses resolve_database_url().

    Returns:
        List of command arguments suitable for subprocess.run().
    """
    url = database_url or resolve_database_url()
    return ["psql", url]


def print_connection_info(database_url: str | None = None) -> None:
    """Print information about the resolved database connection."""
    try:
        url = database_url or resolve_database_url()
    except DatabaseConnectionError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return

    # Mask password for display
    masked = url
    if "@" in url and ":" in url.split("@")[0]:
        parts = url.split("@")
        user_pass = parts[0].rsplit(":", 1)
        if len(user_pass) == 2:
            masked = f"{user_pass[0]}:****@{'@'.join(parts[1:])}"

    source = "unknown"
    if os.getenv("SUPABASE_DB_URL"):
        source = "SUPABASE_DB_URL"
    elif os.getenv("DATABASE_URL"):
        source = "DATABASE_URL"
    elif os.getenv("TRR_DB_URL"):
        source = "TRR_DB_URL"
    else:
        source = "supabase status (local)"

    print(f"Database URL resolved from: {source}")
    print(f"Connection: {masked}")


def is_supabase_url(url: str) -> bool:
    """
    Check if a URL appears to be a Supabase database URL.

    This is a heuristic check based on common Supabase URL patterns.
    """
    url_lower = url.lower()
    return (
        "supabase" in url_lower
        or ".supabase.co" in url_lower
        or "pooler.supabase.com" in url_lower
        or ":54322" in url  # Local Supabase default port
    )


def ensure_ready_for_ingestion(
    database_url: str | None = None,
    *,
    reload_schema_cache: bool = True,
) -> None:
    """
    Verify the database is ready for ingestion and optionally reload PostgREST schema cache.

    This is the recommended pre-flight check before running import jobs.
    It verifies:
    1. Database URL is resolvable
    2. Core schema exists
    3. (Optional) PostgREST schema cache is refreshed

    Args:
        database_url: Optional URL override. If not provided, uses resolve_database_url().
        reload_schema_cache: If True, triggers PostgREST schema cache reload.

    Raises:
        DatabaseConnectionError: If verification fails.
    """
    from trr_backend.db.postgrest_cache import (
        PostgrestCacheError,
        reload_postgrest_schema,
        verify_core_schema_exists,
    )

    url = database_url or resolve_database_url()

    try:
        verify_core_schema_exists(url)
    except PostgrestCacheError as e:
        raise DatabaseConnectionError(str(e)) from e

    if reload_schema_cache:
        try:
            reload_postgrest_schema(url)
        except PostgrestCacheError:
            pass  # Best effort - continue anyway


def validate_supabase_connection(database_url: str | None = None) -> bool:
    """
    Validate that the database URL points to a Supabase instance with core schema.

    Returns True if validation passes, raises DatabaseConnectionError otherwise.
    """
    url = database_url or resolve_database_url()

    # Warn if URL doesn't look like Supabase
    if not is_supabase_url(url):
        print(
            "WARNING: Database URL does not appear to be a Supabase instance.\n"
            "         Ensure `core` schema exists before running migrations.",
            file=sys.stderr,
        )

    # Actually test the connection and schema
    try:
        result = subprocess.run(
            ["psql", url, "-c", "SELECT 1 FROM core.shows LIMIT 1;"],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode != 0:
            stderr = result.stderr or ""
            if "does not exist" in stderr.lower():
                raise DatabaseConnectionError(
                    "Database connection failed: schema `core` does not exist.\n\n"
                    "This usually means you're connected to the wrong database.\n"
                    f"Current URL source: {url[:50]}...\n\n"
                    "Check your environment variables:\n"
                    "  - SUPABASE_DB_URL (should point to your Supabase project)\n"
                    "  - DATABASE_URL\n"
                )
            raise DatabaseConnectionError(
                f"Database connection failed:\n{stderr}"
            )
        return True
    except FileNotFoundError:
        raise DatabaseConnectionError(
            "psql command not found. Install PostgreSQL client tools."
        )
    except subprocess.TimeoutExpired:
        raise DatabaseConnectionError(
            "Database connection timed out. Check network and credentials."
        )
