from __future__ import annotations

import os
from functools import lru_cache

import httpx
from supabase.lib.client_options import SyncClientOptions

from supabase import Client, create_client


@lru_cache
def get_supabase_url() -> str:
    url = (os.getenv("SUPABASE_URL") or "").strip()
    if not url:
        raise RuntimeError("SUPABASE_URL environment variable is not set")
    return url


@lru_cache
def get_supabase_service_key() -> str:
    key = (os.getenv("SUPABASE_SERVICE_ROLE_KEY") or "").strip()
    if not key:
        raise RuntimeError("SUPABASE_SERVICE_ROLE_KEY environment variable is not set")
    return key


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
                print(f"WARN: {env_var}={raw} invalid, using default={default}")
                return default
            return value
        except ValueError:
            print(f"WARN: {env_var}={raw} not a number, using default={default}")
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


@lru_cache
def get_supabase_pool_connections() -> int:
    """Get HTTP connection pool size from environment."""
    raw = os.getenv("SUPABASE_HTTP_POOL_CONNECTIONS", "").strip()
    if not raw:
        return 10
    try:
        value = int(raw)
        if value <= 0:
            print(f"WARN: SUPABASE_HTTP_POOL_CONNECTIONS={raw} invalid, using default=10")
            return 10
        return value
    except ValueError:
        print(f"WARN: SUPABASE_HTTP_POOL_CONNECTIONS={raw} not an integer, using default=10")
        return 10


def _get_major_minor(version: str) -> str | None:
    parts = version.split(".")
    if len(parts) < 2:
        return None
    return f"{parts[0]}.{parts[1]}"


def _get_supabase_postgrest_mismatch() -> tuple[str, str] | None:
    try:
        import postgrest

        import supabase as supabase_pkg
    except Exception:
        return None

    supabase_version = getattr(supabase_pkg, "__version__", None)
    postgrest_version = getattr(postgrest, "__version__", None)
    if not supabase_version or not postgrest_version:
        return None

    supabase_mm = _get_major_minor(supabase_version)
    postgrest_mm = _get_major_minor(postgrest_version)
    if not supabase_mm or not postgrest_mm:
        return None

    if supabase_mm != postgrest_mm:
        return supabase_version, postgrest_version

    return None


def is_timeout_error(exc: BaseException) -> bool:
    """
    Check if an exception is a timeout error (or wraps one).

    Recursively checks through __cause__ and __context__ chains because
    PostgREST/Supabase libraries sometimes wrap httpx exceptions.

    Args:
        exc: Exception to check

    Returns:
        True if the exception is a timeout error
    """
    # Check current exception
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

    # Check wrapped exceptions
    if exc.__cause__ is not None and is_timeout_error(exc.__cause__):
        return True
    if exc.__context__ is not None and is_timeout_error(exc.__context__):
        return True

    return False


def create_supabase_httpx_client() -> httpx.Client:
    """
    Create configured httpx.Client for Supabase operations.

    Features:
    - HTTP/1.1 by default (HTTP/2 causes hangs with PostgREST)
    - Granular timeouts (connect, read, write, pool)
    - Connection pooling
    """
    postgrest_timeout, storage_timeout, pool_timeout = get_supabase_timeout_config()
    http2_enabled = get_supabase_http2_enabled()
    pool_connections = get_supabase_pool_connections()

    # Use max timeout to avoid capping Storage operations below their configured timeout
    max_timeout = max(postgrest_timeout, storage_timeout)
    connect_timeout = max(3.0, postgrest_timeout / 3.0)

    timeout = httpx.Timeout(
        connect=connect_timeout,
        read=max_timeout,
        write=max_timeout,
        pool=pool_timeout,
    )

    limits = httpx.Limits(
        max_connections=pool_connections,
        max_keepalive_connections=pool_connections // 2,
    )

    return httpx.Client(
        http2=http2_enabled,
        timeout=timeout,
        limits=limits,
        follow_redirects=True,
    )


def create_supabase_admin_client(
    *,
    url: str | None = None,
    service_role_key: str | None = None,
    custom_httpx_client: httpx.Client | None = None,
) -> Client:
    """
    Create a Supabase client using the service role key (bypasses RLS).

    Configured with:
    - Hard PostgREST timeouts (default: 15s, configurable via SUPABASE_POSTGREST_TIMEOUT_SEC)
    - HTTP/1.1 by default (HTTP/2 causes hangs, enable via SUPABASE_HTTP2_ENABLED=1)
    - Storage timeouts (default: 30s, configurable via SUPABASE_STORAGE_TIMEOUT_SEC)
    - Connection pooling

    Intended for scripts and admin tasks (imports/backfills).

    IMPORTANT: When calling RPC functions, use call_rpc_with_cache_reload_hint() instead of
    direct db.schema().rpc() calls. This provides helpful error messages for PGRST202 errors
    (function not found in PostgREST schema cache) with instructions to reload the cache.

    See: docs/runbooks/postgrest_schema_cache.md

    Args:
        url: Supabase project URL (defaults to SUPABASE_URL env var)
        service_role_key: Service role key (defaults to SUPABASE_SERVICE_ROLE_KEY env var)
        custom_httpx_client: Optional pre-configured httpx client (for testing)

    Returns:
        Configured Supabase Client instance
    """
    resolved_url = url or get_supabase_url()
    resolved_key = service_role_key or get_supabase_service_key()
    mismatch = _get_supabase_postgrest_mismatch()
    if mismatch:
        supabase_version, postgrest_version = mismatch
        print(
            "WARN: Supabase/PostgREST version mismatch detected. "
            f"supabase={supabase_version} postgrest={postgrest_version}. "
            "Pin postgrest to match supabase major/minor."
        )

    # Create httpx client with timeout configuration
    httpx_client = custom_httpx_client or create_supabase_httpx_client()

    # Parse timeout configuration
    postgrest_timeout_sec, storage_timeout_sec, _ = get_supabase_timeout_config()

    # Create SyncClientOptions with timeout and httpx client
    # Note: Using SyncClientOptions (not ClientOptions) which supports httpx_client parameter
    options = SyncClientOptions(
        postgrest_client_timeout=postgrest_timeout_sec,
        storage_client_timeout=int(storage_timeout_sec),
        httpx_client=httpx_client,
    )

    return create_client(resolved_url, resolved_key, options=options)


def call_rpc_with_cache_reload_hint(
    db: Client,
    schema: str,
    function_name: str,
    params: dict,
) -> any:
    """
    Call an RPC function with helpful error message if schema cache is stale.

    Wraps db.schema().rpc() calls to catch PGRST202 errors (function not found)
    and provide instructions for reloading PostgREST schema cache.

    Args:
        db: Supabase client
        schema: Schema name (e.g., "core")
        function_name: RPC function name
        params: Function parameters

    Returns:
        Response data from RPC call

    Raises:
        RuntimeError: If RPC call fails, with helpful error message for PGRST202
    """
    try:
        response = db.schema(schema).rpc(function_name, params).execute()
        return response.data
    except Exception as exc:
        # Check for PGRST202 (function not found in schema cache)
        error_str = str(exc).lower()
        if "pgrst202" in error_str or "could not find the function" in error_str:
            msg = (
                f"PostgREST schema cache error calling {schema}.{function_name}(): {exc}\n\n"
                f"The function exists in PostgreSQL but PostgREST hasn't loaded it yet.\n\n"
                f"FIX: Reload PostgREST schema cache:\n"
                f'  psql "$SUPABASE_DB_URL" -c "NOTIFY pgrst, \'reload schema\';"\n\n'
                f"Or restart PostgREST:\n"
                f"  docker restart supabase_rest_$(basename $(pwd))\n\n"
                f"Or verify the function exists and is granted to service_role:\n"
                f'  psql "$SUPABASE_DB_URL" -c "\\df+ {schema}.{function_name}"\n'
                f'  psql "$SUPABASE_DB_URL" -c "\\l {schema}.{function_name}"'
            )
            raise RuntimeError(msg) from exc
        # Re-raise other errors with context
        raise RuntimeError(f"RPC call failed for {schema}.{function_name}(): {exc}") from exc
