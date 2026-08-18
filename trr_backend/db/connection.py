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
from collections.abc import Mapping
from functools import lru_cache
from urllib.parse import parse_qsl, urlsplit, urlunsplit

import psycopg2

logger = logging.getLogger(__name__)

CANONICAL_DB_ENV = "TRR_DB_URL"
FALLBACK_DB_ENV = "TRR_DB_FALLBACK_URL"
DIRECT_DB_ENV = "TRR_DB_DIRECT_URL"
SESSION_DB_ENV = "TRR_DB_SESSION_URL"
TRANSACTION_DB_ENV = "TRR_DB_TRANSACTION_URL"
RUNTIME_LANE_ENV = "TRR_DB_RUNTIME_LANE"
TRANSACTION_FLIGHT_TEST_ENV = "TRR_DB_TRANSACTION_FLIGHT_TEST"
PREVIEW_READ_ONLY_ENV = "TRR_PREVIEW_READ_ONLY"


class DatabaseConnectionError(RuntimeError):
    """Raised when database connection cannot be established."""

    pass


class PreviewReadOnlyError(RuntimeError):
    """Raised when an isolated preview connection is not server-confirmed read-only."""

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


def derive_supavisor_transaction_url(session_url: str) -> str:
    """Derive the matching Supavisor transaction-pool URL from a session URL."""
    parsed = urlsplit(str(session_url or "").strip())
    try:
        port = parsed.port
    except ValueError as error:
        raise ValueError("Supavisor session URL must include a valid port") from error
    if (
        parsed.scheme not in {"postgres", "postgresql"}
        or not (parsed.hostname or "").strip().lower().endswith("pooler.supabase.com")
        or port != 5432
    ):
        raise ValueError("Supavisor session URL must use pooler.supabase.com:5432")
    host_and_auth, separator, _port = parsed.netloc.rpartition(":")
    if not separator or not host_and_auth:
        raise ValueError("Supavisor session URL must include port 5432")
    return urlunsplit(parsed._replace(netloc=f"{host_and_auth}:6543"))


def _env_truthy(name: str) -> bool:
    return (os.getenv(name) or "").strip().lower() in {"1", "true", "yes", "on"}


def preview_read_only_enabled() -> bool:
    """Return whether this process is the explicitly opt-in read-only preview."""
    return _env_truthy(PREVIEW_READ_ONLY_ENV)


def connection_uri_options(url: str) -> list[str]:
    """Return existing libpq ``options`` values from a Postgres connection URI."""
    return [
        value.strip()
        for key, value in parse_qsl(urlsplit(url).query, keep_blank_values=True)
        if key.lower() == "options" and value.strip()
    ]


def preview_read_only_connect_kwargs(url: str) -> dict[str, str]:
    """Return the explicit libpq option required by direct preview connections."""
    if not preview_read_only_enabled():
        return {}
    options = connection_uri_options(url)
    # Keep this last so an accidental URI-level `...=off` cannot undo the
    # explicit isolated-preview safety guard.
    options.append("-c default_transaction_read_only=on")
    return {"options": " ".join(options)}


def assert_preview_connection_read_only(conn: object, *, label: str) -> None:
    """Actively enforce and verify the read-only preview setting on one connection."""
    if not preview_read_only_enabled():
        return

    try:
        previous_autocommit = conn.autocommit  # type: ignore[union-attr]
    except Exception as error:  # noqa: BLE001 - re-raised without DSN details.
        raise PreviewReadOnlyError(f"Preview database {label} could not prepare transaction_read_only=on") from error

    try:
        # Supavisor can discard libpq startup options. Run SET and SHOW as
        # autocommitted statements so the setting is session-visible before the
        # next caller starts its own transaction, then restore the prior mode.
        if not previous_autocommit:
            conn.autocommit = True  # type: ignore[union-attr]
        with conn.cursor() as cur:  # type: ignore[union-attr]
            cur.execute("SET default_transaction_read_only = on")
            cur.execute("SHOW transaction_read_only")
            row = cur.fetchone()
    except Exception as error:  # noqa: BLE001 - re-raised without DSN details.
        raise PreviewReadOnlyError(f"Preview database {label} could not enforce transaction_read_only=on") from error
    finally:
        try:
            # Restoring autocommit after the two statements leaves an otherwise
            # healthy connection idle for the caller or pool.
            if not getattr(conn, "closed", False) and conn.autocommit != previous_autocommit:  # type: ignore[union-attr]
                conn.autocommit = previous_autocommit  # type: ignore[union-attr]
        except Exception as error:  # noqa: BLE001 - cleanup must fail closed.
            raise PreviewReadOnlyError(
                f"Preview database {label} could not restore autocommit after transaction_read_only=on"
            ) from error

    if isinstance(row, Mapping):
        value = row.get("transaction_read_only")
    elif isinstance(row, (list, tuple)):
        value = row[0] if row else None
    else:
        value = row
    if str(value or "").strip().lower() != "on":
        raise PreviewReadOnlyError(f"Preview database {label} requires transaction_read_only=on")


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

    Local/direct resolution is unchanged. In the explicit transaction runtime
    lane, only transaction-class candidates are returned; a missing explicit
    transaction URL is derived from a validated Supavisor session URL.
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
    transaction_lane = resolve_runtime_connection_lane() == "transaction" and transaction_flight_test_enabled()
    if transaction_lane:
        explicit_transaction_url = (os.getenv(TRANSACTION_DB_ENV) or "").strip()
        if explicit_transaction_url:
            _append_candidate(
                candidates,
                seen,
                explicit_transaction_url,
                source=TRANSACTION_DB_ENV,
            )
        for env_name in (SESSION_DB_ENV, CANONICAL_DB_ENV, FALLBACK_DB_ENV):
            source_url = (os.getenv(env_name) or "").strip()
            if not source_url:
                continue
            if classify_connection_class(source_url) == "transaction":
                _append_candidate(candidates, seen, source_url, source=env_name)
                continue
            if classify_connection_class(source_url) != "session":
                continue
            _append_candidate(
                candidates,
                seen,
                derive_supavisor_transaction_url(source_url),
                source=f"{env_name}:derived_transaction",
            )
    else:
        _append_candidate(candidates, seen, os.getenv(SESSION_DB_ENV), source=SESSION_DB_ENV)
        _append_candidate(candidates, seen, os.getenv(CANONICAL_DB_ENV), source=CANONICAL_DB_ENV)
        _append_candidate(candidates, seen, os.getenv(FALLBACK_DB_ENV), source=FALLBACK_DB_ENV)

    return tuple(candidates)


def resolve_session_database_url_candidate_details() -> tuple[dict[str, str | int | None], ...]:
    """Resolve candidates suitable for session-scoped locks and pacing."""
    candidates: list[dict[str, str | int | None]] = []
    seen: set[str] = set()

    def _append(value: str | None, *, source: str, allow_direct: bool = False) -> None:
        url = str(value or "").strip()
        if not url or url in seen:
            return
        connection_class = classify_connection_class(url)
        if connection_class != "session" and not (allow_direct and connection_class in {"direct", "local"}):
            return
        candidates.append({"url": url, **describe_database_url_target(url, source=source)})
        seen.add(url)

    _append(os.getenv(DIRECT_DB_ENV), source=DIRECT_DB_ENV, allow_direct=True)
    _append(os.getenv(SESSION_DB_ENV), source=SESSION_DB_ENV)
    _append(os.getenv(CANONICAL_DB_ENV), source=CANONICAL_DB_ENV)
    _append(os.getenv(FALLBACK_DB_ENV), source=FALLBACK_DB_ENV)
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
    if winner_connection_class == "direct":
        logger.warning(
            "[db-resolution] non_default_connection_class connection_class=%s "
            "source=%s; direct connections are reserved for local development",
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

    Local uses the explicit direct lane. Deployed transaction mode uses an
    explicit or safely derived transaction-pool URL. Session-mode resolution
    remains available for compatibility and dedicated session-control pools.

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
        "  Transaction runtime requires TRR_DB_RUNTIME_LANE=transaction and "
        "TRR_DB_TRANSACTION_FLIGHT_TEST=1. Set TRR_DB_TRANSACTION_URL or provide "
        "a valid Supavisor session URL that can be safely derived.\n"
        "  Example: postgresql://postgres.<project>:<password>@<host>:5432/postgres\n\n"
        "For local development:\n"
        "  Set TRR_DB_DIRECT_URL to your direct Postgres connection string.\n\n"
        "Available environment variables (checked in order):\n"
        "  - TRR_DB_DIRECT_URL (explicit local direct DB lane)\n"
        "  - TRR_DB_TRANSACTION_URL (optional explicit transaction runtime URL)\n"
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
    url = database_url or resolve_database_url()

    try:
        conn = psycopg2.connect(url, **preview_read_only_connect_kwargs(url))
        try:
            assert_preview_connection_read_only(conn, label="ingestion readiness")
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM pg_namespace WHERE nspname = 'core';")
                core_schema = cur.fetchone()
        finally:
            conn.close()
    except (psycopg2.Error, PreviewReadOnlyError) as error:
        raise DatabaseConnectionError(f"Failed to verify core schema: {error}") from error

    if not core_schema:
        raise DatabaseConnectionError(
            "Wrong database URL: `core` schema not found.\n\n"
            "Ensure TRR_DB_URL points to your runtime Supabase database."
        )

    if reload_schema_cache and not preview_read_only_enabled():
        try:
            conn = psycopg2.connect(url, **preview_read_only_connect_kwargs(url))
            try:
                conn.autocommit = True
                assert_preview_connection_read_only(conn, label="ingestion schema-cache reload")
                with conn.cursor() as cur:
                    cur.execute("SELECT pg_notify('pgrst', 'reload schema');")
            finally:
                conn.close()
        except psycopg2.Error:
            pass  # Best effort - continue anyway
    elif reload_schema_cache:
        logger.info("Skipping PostgREST schema cache reload in read-only preview")


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
