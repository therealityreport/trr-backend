"""
Unified database connection resolution for TRR Backend.

This module provides a single source of truth for resolving database URLs,
with support for local Supabase development and remote production environments.
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
from functools import lru_cache
from urllib.parse import urlsplit

logger = logging.getLogger(__name__)

CANONICAL_DB_ENV = "TRR_DB_URL"
FALLBACK_DB_ENV = "TRR_DB_FALLBACK_URL"
DIRECT_DB_ENV = "TRR_DB_DIRECT_URL"
SESSION_DB_ENV = "TRR_DB_SESSION_URL"
TRANSACTION_DB_ENV = "TRR_DB_TRANSACTION_URL"
RUNTIME_LANE_ENV = "TRR_DB_RUNTIME_LANE"
TRANSACTION_FLIGHT_TEST_ENV = "TRR_DB_TRANSACTION_FLIGHT_TEST"


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


def classify_database_url(url: str) -> str:
    """Return a coarse, non-secret class for the database target."""
    parsed = urlsplit(url)
    host = (parsed.hostname or "").strip().lower()
    if not host:
        return "unknown"
    if host in {"localhost", "127.0.0.1"}:
        return "local"
    if host.endswith("pooler.supabase.com"):
        return "pooler"
    if host.endswith(".supabase.co"):
        return "direct"
    return "other"


def classify_connection_class(url: str) -> str:
    """Return the connection class for policy logging."""
    parsed = urlsplit(url)
    host = (parsed.hostname or "").strip().lower()
    port = parsed.port
    if not host:
        return "unknown"
    if host in {"localhost", "127.0.0.1"}:
        return "local"
    if host.endswith("pooler.supabase.com"):
        if port == 5432:
            return "session"
        if port == 6543:
            return "transaction"
        return "pooler"
    if host.endswith(".supabase.co"):
        return "direct"
    return "other"


def _env_truthy(name: str) -> bool:
    return (os.getenv(name) or "").strip().lower() in {"1", "true", "yes", "on"}


def transaction_flight_test_enabled() -> bool:
    """Return True only when transaction-mode use is explicitly flight-tested."""
    return _env_truthy(TRANSACTION_FLIGHT_TEST_ENV)


def resolve_runtime_connection_lane() -> str:
    lane = (os.getenv(RUNTIME_LANE_ENV) or "").strip().lower()
    return "transaction" if lane == "transaction" else "session"


def describe_database_url_target(url: str, *, source: str) -> dict[str, str | int | None]:
    parsed = urlsplit(url)
    return {
        "source": source,
        "host_class": classify_database_url(url),
        "connection_class": classify_connection_class(url),
        "host": (parsed.hostname or "").strip().lower() or None,
        "port": parsed.port,
        "database": parsed.path.lstrip("/") or None,
    }


@lru_cache(maxsize=1)
def resolve_database_url_candidate_details() -> tuple[dict[str, str | int | None], ...]:
    """
    Resolve candidate database URLs in priority order.

    Priority order:
    1. TRR_DB_DIRECT_URL
    2. TRR_DB_TRANSACTION_URL only when TRR_DB_RUNTIME_LANE=transaction and
       TRR_DB_TRANSACTION_FLIGHT_TEST=1
    3. TRR_DB_SESSION_URL
    4. TRR_DB_URL
    5. TRR_DB_FALLBACK_URL (optional operator-provided fallback)
    """

    def _append_candidate(
        ordered: list[dict[str, str | int | None]],
        seen: set[str],
        value: str | None,
        *,
        source: str,
    ) -> None:
        url = (value or "").strip()
        if not url or url in seen:
            return
        ordered.append({"url": url, **describe_database_url_target(url, source=source)})
        seen.add(url)

    candidates: list[dict[str, str | int | None]] = []
    seen: set[str] = set()

    _append_candidate(candidates, seen, os.getenv(DIRECT_DB_ENV), source=DIRECT_DB_ENV)
    if resolve_runtime_connection_lane() == "transaction" and transaction_flight_test_enabled():
        _append_candidate(candidates, seen, os.getenv(TRANSACTION_DB_ENV), source=TRANSACTION_DB_ENV)
    _append_candidate(candidates, seen, os.getenv(SESSION_DB_ENV), source=SESSION_DB_ENV)
    _append_candidate(candidates, seen, os.getenv(CANONICAL_DB_ENV), source=CANONICAL_DB_ENV)
    _append_candidate(candidates, seen, os.getenv(FALLBACK_DB_ENV), source=FALLBACK_DB_ENV)

    return tuple(candidates)


@lru_cache(maxsize=1)
def resolve_database_url_candidates() -> tuple[str, ...]:
    """Resolve candidate database URLs in priority order."""
    return tuple(str(candidate["url"]) for candidate in resolve_database_url_candidate_details())


def log_database_resolution_summary() -> None:
    """Log the configured database target order without exposing credentials."""
    candidates = resolve_database_url_candidate_details()
    if not candidates:
        logger.warning("[db-resolution] no database URL candidates available")
        return
    winner = candidates[0]
    logger.info(
        "[db-resolution] winner_source=%s host_class=%s connection_class=%s host=%s port=%s database=%s",
        winner["source"],
        winner["host_class"],
        winner["connection_class"],
        winner["host"],
        winner["port"],
        winner["database"],
    )
    winner_source = str(winner["source"])
    winner_connection_class = str(winner["connection_class"])
    if winner_connection_class in {"direct", "transaction"}:
        logger.warning(
            "[db-resolution] non_default_connection_class connection_class=%s "
            "source=%s; default runtime lane is session via pooler.supabase.com:5432",
            winner_connection_class,
            winner_source,
        )
    for index, candidate in enumerate(candidates):
        logger.info(
            "[db-resolution] candidate_index=%s source=%s host_class=%s "
            "connection_class=%s host=%s port=%s database=%s",
            index,
            candidate["source"],
            candidate["host_class"],
            candidate["connection_class"],
            candidate["host"],
            candidate["port"],
            candidate["database"],
        )


@lru_cache(maxsize=1)
def resolve_database_url() -> str:
    """
    Resolve the database URL using a prioritized lookup.

    Priority order:
    1. TRR_DB_DIRECT_URL - explicit local direct database lane
    2. TRR_DB_TRANSACTION_URL - only during an explicit transaction-mode flight test
    3. TRR_DB_SESSION_URL - preferred explicit session-mode URL
    4. TRR_DB_URL - compatibility/canonical runtime database URL
    5. TRR_DB_FALLBACK_URL - Optional operator-provided fallback

    Returns:
        Database connection URL string.

    Raises:
        DatabaseConnectionError: If no valid database URL can be resolved.
    """
    candidates = resolve_database_url_candidates()
    if candidates:
        return candidates[0]

    raise DatabaseConnectionError(
        "No database URL configured.\n\n"
        "For remote/production:\n"
        "  Set TRR_DB_SESSION_URL or TRR_DB_URL to your Supabase session-pooler connection string.\n"
        "  Optionally set TRR_DB_FALLBACK_URL for controlled failover.\n"
        "  Transaction-mode flight tests require TRR_DB_TRANSACTION_URL, "
        "TRR_DB_RUNTIME_LANE=transaction, and TRR_DB_TRANSACTION_FLIGHT_TEST=1.\n"
        "  Example: postgresql://postgres.<project>:<password>@<host>:5432/postgres\n\n"
        "For local development:\n"
        "  Set TRR_DB_DIRECT_URL to your direct Postgres connection string.\n\n"
        "Available environment variables (checked in order):\n"
        "  - TRR_DB_DIRECT_URL (explicit local direct DB lane)\n"
        "  - TRR_DB_TRANSACTION_URL (explicit flight-test env only)\n"
        "  - TRR_DB_SESSION_URL (preferred session runtime env)\n"
        "  - TRR_DB_URL (compatibility/canonical runtime env)\n"
        "  - TRR_DB_FALLBACK_URL (optional runtime fallback)\n"
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
    details = resolve_database_url_candidate_details()
    source = str(details[0]["source"]) if details else "unknown"
    target = describe_database_url_target(url, source=source)

    print(f"Database URL resolved from: {source}")
    print(
        "Target: "
        f"host_class={target['host_class']} "
        f"connection_class={target['connection_class']} "
        f"host={target['host']} "
        f"port={target['port']} "
        f"database={target['database']}"
    )
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
                    "  - TRR_DB_URL (should point to your Supabase session-pooler runtime URL)\n"
                    "  - TRR_DB_FALLBACK_URL (optional runtime fallback)\n"
                )
            raise DatabaseConnectionError(f"Database connection failed:\n{stderr}")
        return True
    except FileNotFoundError:
        raise DatabaseConnectionError("psql command not found. Install PostgreSQL client tools.") from None
    except subprocess.TimeoutExpired:
        raise DatabaseConnectionError("Database connection timed out. Check network and credentials.") from None
