"""Legacy Supabase helpers (SDK-free)."""

from __future__ import annotations

import os
from functools import lru_cache
from typing import Tuple

import httpx

from trr_backend.db.session import DbSession, get_db_session


# NOTE: Supabase Python SDK has been removed from this repo. These helpers
# provide backwards-compatible entry points for call sites that previously
# expected supabase-py client objects.


@lru_cache
def get_supabase_timeout_config() -> tuple[float, float, float]:
    """
    Parse timeout configuration from environment.

    Returns:
        (postgrest_timeout, storage_timeout, pool_timeout)
    """

    def parse_timeout(env_var: str, default: float) -> float:
        raw = os.getenv(env_var, "").strip()
        if not raw:
            return default
        try:
            value = float(raw)
            if value <= 0:
                return default
            return value
        except ValueError:
            return default

    postgrest = parse_timeout("SUPABASE_POSTGREST_TIMEOUT_SEC", 15.0)
    storage = parse_timeout("SUPABASE_STORAGE_TIMEOUT_SEC", 30.0)
    pool = parse_timeout("SUPABASE_HTTP_POOL_TIMEOUT_SEC", 5.0)

    return postgrest, storage, pool


@lru_cache
def get_supabase_http2_enabled() -> bool:
    """Check if HTTP/2 is enabled via environment variable."""
    value = os.getenv("SUPABASE_HTTP2_ENABLED", "").strip().lower()
    return value in ("1", "true", "yes", "on")


def create_supabase_admin_client(*, url: str | None = None, service_role_key: str | None = None) -> DbSession:
    return get_db_session()


def call_rpc_with_cache_reload_hint(
    db: DbSession,
    schema: str,
    function_name: str,
    params: dict,
) -> any:
    response = db.schema(schema).rpc(function_name, params).execute()
    if getattr(response, "error", None):
        raise RuntimeError(response.error.message)
    return response.data


def is_timeout_error(exc: BaseException) -> bool:
    """
    Check if an exception is a timeout error (or wraps one).

    Recursively checks through __cause__ and __context__ chains because
    httpx exceptions may be wrapped.
    """
    if isinstance(
        exc,
        (
            httpx.TimeoutException,
            httpx.ConnectTimeout,
            httpx.ReadTimeout,
            httpx.WriteTimeout,
            httpx.PoolTimeout,
        ),
    ):
        return True

    if exc.__cause__ is not None and is_timeout_error(exc.__cause__):
        return True
    if exc.__context__ is not None and is_timeout_error(exc.__context__):
        return True

    return False
