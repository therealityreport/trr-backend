"""
PostgREST Schema Cache Management.

This module provides utilities for reloading the PostgREST schema cache,
which is necessary after schema migrations to avoid PGRST204 errors.
"""
from __future__ import annotations

import time

import psycopg2

from trr_backend.db.connection import resolve_database_url


class PostgrestCacheError(RuntimeError):
    """Raised when PostgREST schema cache operations fail."""

    pass


def reload_postgrest_schema(database_url: str | None = None) -> None:
    """
    Trigger PostgREST to reload its schema cache.

    This sends a pg_notify signal that PostgREST listens to.
    Use this after migrations or when encountering PGRST204 errors.

    Args:
        database_url: Optional database URL. If not provided, uses resolve_database_url().

    Raises:
        PostgrestCacheError: If the schema reload signal cannot be sent.
    """
    url = database_url or resolve_database_url()

    try:
        conn = psycopg2.connect(url)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT pg_notify('pgrst', 'reload schema');")
        conn.close()
    except psycopg2.Error as e:
        raise PostgrestCacheError(f"Failed to reload PostgREST schema cache: {e}") from e


def is_pgrst204_error(error: Exception) -> bool:
    """
    Check if an exception is a PostgREST PGRST204 schema cache error.

    Args:
        error: The exception to check.

    Returns:
        True if the error indicates a stale PostgREST schema cache.
    """
    error_str = str(error).lower()
    error_code = getattr(error, "code", None)

    # Check for PGRST204 error code
    if error_code == "PGRST204":
        return True

    # Check for schema cache related messages
    schema_cache_indicators = [
        "pgrst204",
        "schema cache",
        "could not find the",
        "column of",
        "in the schema cache",
    ]

    return any(indicator in error_str for indicator in schema_cache_indicators)


def with_schema_cache_retry(
    func,
    *args,
    max_retries: int = 1,
    retry_delay: float = 0.5,
    database_url: str | None = None,
    **kwargs,
):
    """
    Execute a function with automatic retry on PGRST204 schema cache errors.

    On first PGRST204 error, triggers a schema cache reload and retries once.

    Args:
        func: The function to execute.
        *args: Positional arguments for the function.
        max_retries: Maximum number of retries (default: 1).
        retry_delay: Delay between retries in seconds (default: 0.5).
        database_url: Optional database URL for schema reload.
        **kwargs: Keyword arguments for the function.

    Returns:
        The result of the function call.

    Raises:
        The original exception if retries are exhausted or error is not PGRST204.
    """
    last_error = None

    for attempt in range(max_retries + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            last_error = e

            if not is_pgrst204_error(e):
                raise

            if attempt >= max_retries:
                # Add helpful hint to the error message
                hint = (
                    "\n\nPostgREST schema cache may still be stale after retry. "
                    "Wait 30-60s and try again, or run:\n"
                    "  psql \"$SUPABASE_DB_URL\" -f scripts/db/reload_postgrest_schema.sql"
                )
                raise type(e)(f"{e}{hint}") from e

            # Trigger schema reload and retry
            try:
                reload_postgrest_schema(database_url)
            except PostgrestCacheError:
                pass  # Best effort - continue with retry anyway

            time.sleep(retry_delay)

    # Should not reach here, but just in case
    if last_error:
        raise last_error


def verify_core_schema_exists(database_url: str | None = None) -> bool:
    """
    Verify that the `core` schema exists in the database.

    This is a lightweight check to ensure we're connected to the correct database
    before running migrations or import jobs.

    Args:
        database_url: Optional database URL. If not provided, uses resolve_database_url().

    Returns:
        True if the core schema exists.

    Raises:
        PostgrestCacheError: If the schema does not exist or connection fails.
    """
    url = database_url or resolve_database_url()

    try:
        conn = psycopg2.connect(url)
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_namespace WHERE nspname = 'core';")
            result = cur.fetchone()
        conn.close()

        if not result:
            raise PostgrestCacheError(
                "Wrong database URL: `core` schema not found.\n\n"
                "Ensure SUPABASE_DB_URL points to your Supabase project database."
            )

        return True

    except psycopg2.Error as e:
        raise PostgrestCacheError(f"Failed to verify core schema: {e}") from e
