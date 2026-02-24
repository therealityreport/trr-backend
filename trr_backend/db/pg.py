"""Lightweight Postgres helpers for direct SQL access."""

from __future__ import annotations

import os
from collections.abc import Callable, Iterable
from contextlib import contextmanager
from threading import Lock
from typing import TYPE_CHECKING, Any, TypeVar
from urllib.parse import urlparse

from psycopg2.extras import RealDictCursor, execute_values
from psycopg2.pool import ThreadedConnectionPool

from trr_backend.db.connection import resolve_database_url_candidates

if TYPE_CHECKING:
    from psycopg2.extensions import connection as connection_type
    from psycopg2.extensions import cursor as cursor_type

DEFAULT_POOL_MINCONN = 2
DEFAULT_POOL_MAXCONN = 24

_pool: ThreadedConnectionPool | None = None
_active_pool_dsn: str | None = None
_pool_lock = Lock()

T = TypeVar("T")


def _env_int(name: str, default: int, *, minimum: int = 1) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        parsed = int(raw)
    except ValueError:
        return default
    return max(minimum, parsed)


def _sslmode_for_url(url: str) -> str | None:
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if host in {"localhost", "127.0.0.1"}:
        return "disable"
    return None


def _error_message(error: Exception) -> str:
    return str(error).strip().lower()


def _is_transient_transport_error(error: Exception) -> bool:
    message = _error_message(error)
    if not message:
        return False
    markers = (
        "enotfound",
        "could not translate host name",
        "temporary failure in name resolution",
        "name or service not known",
        "nodename nor servname provided",
        "ssl syscall error: eof detected",
        "server closed the connection unexpectedly",
        "connection reset by peer",
        "connection refused",
        "connection timed out",
        "terminating connection due to administrator command",
    )
    return any(marker in message for marker in markers)


def _build_pool_for_url(url: str) -> ThreadedConnectionPool:
    minconn = _env_int("TRR_DB_POOL_MINCONN", DEFAULT_POOL_MINCONN)
    maxconn = _env_int("TRR_DB_POOL_MAXCONN", DEFAULT_POOL_MAXCONN)
    maxconn = max(minconn, maxconn)

    sslmode = _sslmode_for_url(url)
    connect_kwargs: dict[str, Any] = {"dsn": url}
    if sslmode:
        connect_kwargs["sslmode"] = sslmode

    return ThreadedConnectionPool(minconn=minconn, maxconn=maxconn, **connect_kwargs)


def _reset_pool_locked() -> None:
    global _pool, _active_pool_dsn
    if _pool is not None:
        _pool.closeall()
    _pool = None
    _active_pool_dsn = None


def _get_pool() -> ThreadedConnectionPool:
    global _pool, _active_pool_dsn
    if _pool is not None:
        return _pool

    with _pool_lock:
        if _pool is not None:
            return _pool

        init_errors: list[Exception] = []
        candidates = resolve_database_url_candidates()
        for index, candidate in enumerate(candidates):
            try:
                _pool = _build_pool_for_url(candidate)
                _active_pool_dsn = candidate
                return _pool
            except Exception as error:
                init_errors.append(error)
                has_more = index < len(candidates) - 1
                if has_more and _is_transient_transport_error(error):
                    continue
                raise

        if init_errors:
            raise init_errors[-1]
        raise RuntimeError("Database pool initialization failed: no database URL candidates available")


def reset_pool() -> None:
    """Reset the shared pool; used for transient transport recovery."""
    with _pool_lock:
        _reset_pool_locked()


def close_pool() -> None:
    """Close all pooled connections. Intended for tests/process shutdown."""
    reset_pool()


def current_pool_dsn() -> str | None:
    """Return the currently active pool DSN for diagnostics."""
    return _active_pool_dsn


def _should_retry_query(error: Exception, *, attempt: int) -> bool:
    return attempt == 0 and _is_transient_transport_error(error)


def _run_with_transient_retry(operation: Callable[[], T]) -> T:
    for attempt in range(2):
        try:
            return operation()
        except Exception as error:
            if not _should_retry_query(error, attempt=attempt):
                raise
            reset_pool()
    raise RuntimeError("unreachable")


def _get_connection_with_retry() -> tuple[ThreadedConnectionPool, connection_type]:
    for attempt in range(2):
        pool = _get_pool()
        try:
            conn = pool.getconn()
            return pool, conn
        except Exception as error:
            if not _should_retry_query(error, attempt=attempt):
                raise
            reset_pool()
    raise RuntimeError("unreachable")


@contextmanager
def db_connection():
    pool, conn = _get_connection_with_retry()
    try:
        yield conn
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        pool.putconn(conn)


@contextmanager
def db_cursor(*, conn: connection_type | None = None):
    """Yield a RealDict cursor, optionally reusing an existing connection."""
    if conn is not None:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            yield cur
        return

    with db_connection() as managed_conn:
        with managed_conn.cursor(cursor_factory=RealDictCursor) as cur:
            yield cur


def fetch_all_with_cursor(
    cur: cursor_type,
    query: str,
    params: Iterable[Any] | None = None,
) -> list[dict[str, Any]]:
    cur.execute(query, params or [])
    rows = cur.fetchall()
    return [dict(row) for row in rows]


def fetch_one_with_cursor(
    cur: cursor_type,
    query: str,
    params: Iterable[Any] | None = None,
) -> dict[str, Any] | None:
    cur.execute(query, params or [])
    row = cur.fetchone()
    return dict(row) if row else None


def fetch_all(query: str, params: Iterable[Any] | None = None) -> list[dict[str, Any]]:
    def _run() -> list[dict[str, Any]]:
        with db_cursor() as cur:
            return fetch_all_with_cursor(cur, query, params)

    return _run_with_transient_retry(_run)


def fetch_one(query: str, params: Iterable[Any] | None = None) -> dict[str, Any] | None:
    def _run() -> dict[str, Any] | None:
        with db_cursor() as cur:
            return fetch_one_with_cursor(cur, query, params)

    return _run_with_transient_retry(_run)


def execute_returning(
    query: str,
    params: Iterable[Any] | None = None,
) -> list[dict[str, Any]]:
    def _run() -> list[dict[str, Any]]:
        with db_cursor() as cur:
            cur.execute(query, params or [])
            rows = cur.fetchall()
            return [dict(row) for row in rows]

    return _run_with_transient_retry(_run)


def execute_values_returning(
    query: str,
    rows: list[tuple[Any, ...]],
    *,
    conn: connection_type | None = None,
) -> list[dict[str, Any]]:
    if not rows:
        return []
    if conn is not None:
        with db_cursor(conn=conn) as cur:
            execute_values(cur, query, rows)
            result = cur.fetchall()
            return [dict(row) for row in result]

    def _run() -> list[dict[str, Any]]:
        with db_cursor() as cur:
            execute_values(cur, query, rows)
            result = cur.fetchall()
            return [dict(row) for row in result]

    return _run_with_transient_retry(_run)


def execute_values_no_return(
    query: str,
    rows: list[tuple[Any, ...]],
    *,
    conn: connection_type | None = None,
) -> None:
    if not rows:
        return
    if conn is not None:
        with db_cursor(conn=conn) as cur:
            execute_values(cur, query, rows)
        return

    def _run() -> None:
        with db_cursor() as cur:
            execute_values(cur, query, rows)

    _run_with_transient_retry(_run)
