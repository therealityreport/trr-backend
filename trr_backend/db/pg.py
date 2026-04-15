"""Lightweight Postgres helpers for direct SQL access."""

from __future__ import annotations

import logging
import os
import time
from collections.abc import Callable, Iterable
from contextlib import contextmanager
from threading import Lock
from typing import TYPE_CHECKING, Any, TypeVar
from urllib.parse import urlparse

from psycopg2.extensions import (
    TRANSACTION_STATUS_ACTIVE,
    TRANSACTION_STATUS_IDLE,
    TRANSACTION_STATUS_INERROR,
    TRANSACTION_STATUS_INTRANS,
    TRANSACTION_STATUS_UNKNOWN,
)
from psycopg2.extras import RealDictCursor, execute_values
from psycopg2.pool import PoolError, ThreadedConnectionPool

from trr_backend.db.connection import (
    resolve_database_url_candidate_details,
)
from trr_backend.observability import (
    record_postgres_pool_acquire_duration,
    record_postgres_pool_exhausted,
    record_postgres_pool_state,
)

if TYPE_CHECKING:
    from psycopg2.extensions import connection as connection_type
    from psycopg2.extensions import cursor as cursor_type

DEFAULT_POOL_MINCONN = 2
DEFAULT_POOL_MAXCONN = 24
DEFAULT_SESSION_POOLER_MINCONN = 1
DEFAULT_SESSION_POOLER_MAXCONN = 2
DEFAULT_POOL_ACQUIRE_ATTEMPTS = 8
DEFAULT_POOL_ACQUIRE_SLEEP_MS = 50
DEFAULT_QUERY_TRANSIENT_ATTEMPTS = 3
DEFAULT_IDLE_IN_TX_TIMEOUT_MS = 60000
DEFAULT_CONNECT_TIMEOUT_SECONDS = 10
DEFAULT_STATEMENT_TIMEOUT_MS = 30000
DEFAULT_DB_APPLICATION_NAME = "trr-backend"
# Transaction-local search_path applied to every write checkout. Pinning prevents
# a prior caller's `SET search_path` from leaking through the pooled connection.
# Read checkouts run under autocommit and therefore cannot use SET LOCAL; reads
# in this codebase are schema-qualified, so the server default is acceptable.
DEFAULT_WRITE_SEARCH_PATH = "public, core, firebase_surveys"

_pool: ThreadedConnectionPool | None = None
_active_pool_dsn: str | None = None
_pool_lock = Lock()
_pool_creation_count = 0
_retired_pools: dict[int, ThreadedConnectionPool] = {}
_checkout_sequence = 0
_checkout_lock = Lock()
_checked_out_connections: dict[int, dict[str, Any]] = {}

T = TypeVar("T")
logger = logging.getLogger(__name__)


class DatabaseServiceUnavailableError(RuntimeError):
    """Raised when the Postgres runtime is unavailable or saturated."""

    def __init__(self, message: str, *, reason: str = "database_unavailable") -> None:
        super().__init__(message)
        self.reason = reason


def _env_int(name: str, default: int, *, minimum: int = 1) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        parsed = int(raw)
    except ValueError:
        return default
    return max(minimum, parsed)


def _env_has_value(name: str) -> bool:
    return bool((os.getenv(name) or "").strip())


def _is_local_or_dev_runtime() -> bool:
    runtime_markers = [
        os.getenv("APP_ENV"),
        os.getenv("ENV"),
        os.getenv("ENVIRONMENT"),
        os.getenv("TRR_ENV"),
        os.getenv("TRR_ENVIRONMENT"),
    ]
    normalized = {str(value or "").strip().lower() for value in runtime_markers if str(value or "").strip()}
    if normalized & {"prod", "production"}:
        return False
    if normalized & {"local", "dev", "development", "test"}:
        return True
    return bool(os.getenv("PYTEST_CURRENT_TEST"))


def _sslmode_for_url(url: str) -> str | None:
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if host in {"localhost", "127.0.0.1"}:
        return "disable"
    return None


def _is_supavisor_session_pooler_url(url: str) -> bool:
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    port = parsed.port
    return host.endswith("pooler.supabase.com") and port == 5432


def _error_message(error: Exception) -> str:
    return str(error).strip().lower()


def _is_pool_exhausted_error(error: Exception) -> bool:
    if isinstance(error, PoolError):
        return "pool exhausted" in _error_message(error)
    return "connection pool exhausted" in _error_message(error)


def _database_service_unavailable_reason(message: str) -> str:
    if "maxclientsinsessionmode" in message or "max clients reached - in session mode" in message:
        return "session_pool_capacity"
    if "connection pool exhausted" in message or "pool exhausted" in message:
        return "pool_capacity"
    if "no database url candidates available" in message:
        return "database_configuration"
    if "database pool initialization failed" in message:
        return "pool_initialization"
    return "database_unavailable"


def is_database_service_unavailable_error(error: Exception) -> bool:
    if isinstance(error, DatabaseServiceUnavailableError):
        return True
    message = _error_message(error)
    return _is_pool_exhausted_error(error) or "database pool initialization failed" in message


def database_service_unavailable_detail(error: Exception) -> dict[str, Any]:
    if isinstance(error, DatabaseServiceUnavailableError):
        reason = error.reason
    else:
        reason = _database_service_unavailable_reason(_error_message(error))

    if reason == "session_pool_capacity":
        safe_message = (
            "Database service unavailable: Supabase session-pool capacity is saturated. "
            "Reduce local DB concurrency or use the explicit local fallback lane."
        )
    elif reason == "database_configuration":
        safe_message = (
            "Database service unavailable: runtime DB configuration is incomplete. "
            "Set TRR_DB_URL and optional TRR_DB_FALLBACK_URL."
        )
    else:
        safe_message = "Database service unavailable. Check runtime DB connectivity and pool sizing."

    return {
        "code": "DATABASE_SERVICE_UNAVAILABLE",
        "reason": reason,
        "message": safe_message,
        "retryable": True,
        "retry_after_ms": 1000,
    }


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
        "cursor already closed",
        "connection already closed",
        "connection not open",
        "ssl connection has been closed unexpectedly",
        "ssl syscall error: eof detected",
        "server closed the connection unexpectedly",
        "connection reset by peer",
        "connection refused",
        "connection timed out",
        "maxclientsinsessionmode",
        "max clients reached - in session mode",
        "connection pool is closed",
        "pool is closed",
        "terminating connection due to administrator command",
    )
    return any(marker in message for marker in markers)


def _is_statement_timeout_error(error: Exception) -> bool:
    """Check if the error is a Postgres statement_timeout cancellation.

    Statement timeouts are NOT transient — retrying the same query will hit
    the same timeout. They must be logged distinctly and must NOT enter
    the transient retry classification.
    """
    message = _error_message(error)
    return "canceling statement due to statement timeout" in message


def _resolve_pool_sizing(url: str) -> dict[str, Any]:
    session_pooler = _is_supavisor_session_pooler_url(url)
    default_minconn = DEFAULT_SESSION_POOLER_MINCONN if session_pooler else DEFAULT_POOL_MINCONN
    default_maxconn = DEFAULT_SESSION_POOLER_MAXCONN if session_pooler else DEFAULT_POOL_MAXCONN
    minconn_overridden = _env_has_value("TRR_DB_POOL_MINCONN")
    maxconn_overridden = _env_has_value("TRR_DB_POOL_MAXCONN")
    minconn = _env_int("TRR_DB_POOL_MINCONN", default_minconn)
    maxconn = _env_int("TRR_DB_POOL_MAXCONN", default_maxconn)
    maxconn = max(minconn, maxconn)
    return {
        "session_pooler": session_pooler,
        "default_minconn": default_minconn,
        "default_maxconn": default_maxconn,
        "minconn": minconn,
        "maxconn": maxconn,
        "minconn_source": "env:TRR_DB_POOL_MINCONN" if minconn_overridden else "default",
        "maxconn_source": "env:TRR_DB_POOL_MAXCONN" if maxconn_overridden else "default",
        "using_tiny_session_defaults": (
            session_pooler
            and not minconn_overridden
            and not maxconn_overridden
            and minconn <= DEFAULT_SESSION_POOLER_MINCONN
            and maxconn <= DEFAULT_SESSION_POOLER_MAXCONN
        ),
    }


def _resolve_application_name() -> dict[str, str]:
    raw = (os.getenv("TRR_DB_APPLICATION_NAME") or "").strip()
    if raw:
        return {"application_name": raw, "application_name_source": "env:TRR_DB_APPLICATION_NAME"}
    return {"application_name": DEFAULT_DB_APPLICATION_NAME, "application_name_source": "default"}


def _build_pool_for_url(url: str) -> ThreadedConnectionPool:
    sizing = _resolve_pool_sizing(url)
    minconn = int(sizing["minconn"])
    maxconn = int(sizing["maxconn"])
    app_name = _resolve_application_name()

    sslmode = _sslmode_for_url(url)
    connect_kwargs: dict[str, Any] = {"dsn": url}
    connect_kwargs["application_name"] = app_name["application_name"]
    if sslmode:
        connect_kwargs["sslmode"] = sslmode
    # TCP-level connect timeout (seconds) — prevents 2-min OS TCP hangs
    connect_timeout = _env_int(
        "TRR_DB_CONNECT_TIMEOUT_SECONDS",
        DEFAULT_CONNECT_TIMEOUT_SECONDS,
        minimum=1,
    )
    connect_kwargs["connect_timeout"] = connect_timeout

    # Session-level Postgres options
    option_parts: list[str] = []

    idle_in_tx_timeout_ms = _env_int(
        "TRR_DB_IDLE_IN_TRANSACTION_TIMEOUT_MS",
        DEFAULT_IDLE_IN_TX_TIMEOUT_MS,
        minimum=1000,
    )
    if idle_in_tx_timeout_ms > 0:
        option_parts.append(f"idle_in_transaction_session_timeout={idle_in_tx_timeout_ms}")

    statement_timeout_ms = _env_int(
        "TRR_DB_STATEMENT_TIMEOUT_MS",
        DEFAULT_STATEMENT_TIMEOUT_MS,
        minimum=1000,
    )
    if statement_timeout_ms > 0:
        option_parts.append(f"statement_timeout={statement_timeout_ms}")

    if option_parts:
        connect_kwargs["options"] = " ".join(f"-c {part}" for part in option_parts)

    return ThreadedConnectionPool(minconn=minconn, maxconn=maxconn, **connect_kwargs)


def _pool_counts(pool: ThreadedConnectionPool | None) -> tuple[int | None, int | None]:
    if pool is None:
        return None, None
    try:
        available = len(getattr(pool, "_pool", []))
    except Exception:
        available = None
    try:
        in_use = len(getattr(pool, "_used", {}))
    except Exception:
        in_use = None
    return in_use, available


def _transaction_status_name(conn: connection_type) -> str:
    try:
        status = conn.get_transaction_status()
    except Exception:
        return "unknown"
    if status == TRANSACTION_STATUS_IDLE:
        return "idle"
    if status == TRANSACTION_STATUS_ACTIVE:
        return "active"
    if status == TRANSACTION_STATUS_INTRANS:
        return "in_transaction"
    if status == TRANSACTION_STATUS_INERROR:
        return "in_error"
    if status == TRANSACTION_STATUS_UNKNOWN:
        return "driver_unknown"
    return str(status)


def _is_connection_closed(conn: connection_type) -> bool:
    try:
        return bool(getattr(conn, "closed", False))
    except Exception:
        return True


def _log_checkout(
    *,
    pool: ThreadedConnectionPool,
    conn: connection_type,
    label: str,
    acquire_started_at: float,
) -> int:
    global _checkout_sequence
    with _checkout_lock:
        _checkout_sequence += 1
        checkout_id = _checkout_sequence
        _checked_out_connections[id(conn)] = {
            "checkout_id": checkout_id,
            "label": label,
            "started_at": time.perf_counter(),
            "pool_id": id(pool),
        }
    in_use, available = _pool_counts(pool)
    acquire_duration_seconds = max(0.0, time.perf_counter() - acquire_started_at)
    logger.info(
        "[db-pool] checkout id=%s label=%s acquire_ms=%.1f backend_pid=%s tx_status=%s in_use=%s available=%s",
        checkout_id,
        label,
        acquire_duration_seconds * 1000.0,
        getattr(conn, "get_backend_pid", lambda: None)(),
        _transaction_status_name(conn),
        in_use,
        available,
    )
    record_postgres_pool_state(in_use=in_use, available=available)
    record_postgres_pool_acquire_duration(label, acquire_duration_seconds)
    return checkout_id


def _log_return(
    *,
    pool: ThreadedConnectionPool,
    conn: connection_type,
    checkout_id: int | None,
    label: str,
) -> None:
    started_at: float | None = None
    with _checkout_lock:
        metadata = _checked_out_connections.pop(id(conn), None)
        if metadata:
            started_at = metadata.get("started_at")
    held_ms = (time.perf_counter() - started_at) * 1000.0 if started_at is not None else None
    in_use, available = _pool_counts(pool)
    logger.info(
        "[db-pool] return id=%s label=%s held_ms=%s backend_pid=%s tx_status=%s in_use=%s available=%s",
        checkout_id,
        label,
        f"{held_ms:.1f}" if held_ms is not None else "unknown",
        getattr(conn, "get_backend_pid", lambda: None)(),
        _transaction_status_name(conn),
        in_use,
        available,
    )
    record_postgres_pool_state(in_use=in_use, available=available)


def _ensure_connection_idle(conn: connection_type, *, label: str, phase: str) -> bool:
    if _is_connection_closed(conn):
        logger.warning(
            "[db-pool] discard_closed_connection label=%s phase=%s backend_pid=%s",
            label,
            phase,
            getattr(conn, "get_backend_pid", lambda: None)(),
        )
        return False
    tx_status = _transaction_status_name(conn)
    if tx_status in {"idle", "driver_unknown"}:
        return True
    try:
        conn.rollback()
        logger.warning(
            "[db-pool] rollback_dirty_connection label=%s phase=%s backend_pid=%s prior_tx_status=%s",
            label,
            phase,
            getattr(conn, "get_backend_pid", lambda: None)(),
            tx_status,
        )
    except Exception:
        logger.exception(
            "[db-pool] rollback_failed label=%s phase=%s backend_pid=%s prior_tx_status=%s",
            label,
            phase,
            getattr(conn, "get_backend_pid", lambda: None)(),
            tx_status,
        )
        return False
    return _transaction_status_name(conn) == "idle"


def _reset_pool_locked() -> None:
    global _pool, _active_pool_dsn
    if _pool is not None:
        _pool.closeall()
    _pool = None
    _active_pool_dsn = None


def _close_pool_quietly(pool: ThreadedConnectionPool) -> None:
    try:
        pool.closeall()
    except Exception:
        logger.exception("[db-pool] closeall_failed")


def _checked_out_count_for_pool(pool: ThreadedConnectionPool) -> int:
    pool_id = id(pool)
    with _checkout_lock:
        return sum(1 for metadata in _checked_out_connections.values() if metadata.get("pool_id") == pool_id)


def _retire_pool_locked(pool: ThreadedConnectionPool) -> None:
    global _pool, _active_pool_dsn

    if _pool is pool:
        _pool = None
        _active_pool_dsn = None

    checked_out = _checked_out_count_for_pool(pool)
    in_use, available = _pool_counts(pool)
    if checked_out > 0:
        _retired_pools[id(pool)] = pool
        logger.info(
            "[db-pool] retire_pool pending_checkouts=%s in_use=%s available=%s",
            checked_out,
            in_use,
            available,
        )
        return

    _retired_pools.pop(id(pool), None)
    _close_pool_quietly(pool)


def _finalize_retired_pool(pool: ThreadedConnectionPool) -> None:
    with _pool_lock:
        pool_id = id(pool)
        retired_pool = _retired_pools.get(pool_id)
        if retired_pool is None:
            return
        if _checked_out_count_for_pool(retired_pool) > 0:
            return
        _retired_pools.pop(pool_id, None)
        _close_pool_quietly(retired_pool)


def _get_pool() -> ThreadedConnectionPool:
    global _pool, _active_pool_dsn, _pool_creation_count
    if _pool is not None:
        return _pool

    with _pool_lock:
        if _pool is not None:
            return _pool

        init_errors: list[Exception] = []
        candidate_details = resolve_database_url_candidate_details()
        for index, candidate_detail in enumerate(candidate_details):
            candidate = str(candidate_detail["url"])
            sizing = _resolve_pool_sizing(candidate)
            app_name = _resolve_application_name()
            try:
                logger.info(
                    (
                        "[db-pool] init_attempt=%s source=%s host_class=%s connection_class=%s host=%s port=%s "
                        "application_name=%s application_name_source=%s minconn=%s maxconn=%s "
                        "minconn_source=%s maxconn_source=%s"
                    ),
                    index,
                    candidate_detail["source"],
                    candidate_detail["host_class"],
                    candidate_detail["connection_class"],
                    candidate_detail["host"],
                    candidate_detail["port"],
                    app_name["application_name"],
                    app_name["application_name_source"],
                    sizing["minconn"],
                    sizing["maxconn"],
                    sizing["minconn_source"],
                    sizing["maxconn_source"],
                )
                _pool = _build_pool_for_url(candidate)
                _pool_creation_count += 1
                _active_pool_dsn = candidate
                logger.info(
                    (
                        "[db-pool] init_selected source=%s host_class=%s connection_class=%s host=%s port=%s "
                        "application_name=%s application_name_source=%s minconn=%s maxconn=%s "
                        "minconn_source=%s maxconn_source=%s pool_creations=%s"
                    ),
                    candidate_detail["source"],
                    candidate_detail["host_class"],
                    candidate_detail["connection_class"],
                    candidate_detail["host"],
                    candidate_detail["port"],
                    app_name["application_name"],
                    app_name["application_name_source"],
                    sizing["minconn"],
                    sizing["maxconn"],
                    sizing["minconn_source"],
                    sizing["maxconn_source"],
                    _pool_creation_count,
                )
                if sizing["using_tiny_session_defaults"]:
                    logger.warning(
                        (
                            "[db-pool] session_pooler_tiny_defaults source=%s host=%s port=%s "
                            "minconn=%s maxconn=%s; set TRR_DB_POOL_MINCONN/TRR_DB_POOL_MAXCONN "
                            "for local high-concurrency social admin work"
                        ),
                        candidate_detail["source"],
                        candidate_detail["host"],
                        candidate_detail["port"],
                        sizing["minconn"],
                        sizing["maxconn"],
                    )
                elif sizing["session_pooler"] and (
                    int(sizing["minconn"]) > DEFAULT_SESSION_POOLER_MINCONN
                    or int(sizing["maxconn"]) > DEFAULT_SESSION_POOLER_MAXCONN
                ):
                    logger.warning(
                        (
                            "[db-pool] oversized_session_pool_override source=%s host=%s port=%s "
                            "minconn=%s maxconn=%s default_minconn=%s default_maxconn=%s"
                        ),
                        candidate_detail["source"],
                        candidate_detail["host"],
                        candidate_detail["port"],
                        sizing["minconn"],
                        sizing["maxconn"],
                        sizing["default_minconn"],
                        sizing["default_maxconn"],
                    )
                return _pool
            except Exception as error:
                init_errors.append(error)
                logger.warning(
                    "[db-pool] init_failed source=%s host_class=%s host=%s port=%s error=%s",
                    candidate_detail["source"],
                    candidate_detail["host_class"],
                    candidate_detail["host"],
                    candidate_detail["port"],
                    type(error).__name__,
                )
                has_more = index < len(candidate_details) - 1
                if has_more and _is_transient_transport_error(error):
                    continue
                raise DatabaseServiceUnavailableError(
                    f"Database pool initialization failed: {error}",
                    reason=_database_service_unavailable_reason(_error_message(error)),
                ) from error

        if init_errors:
            raise DatabaseServiceUnavailableError(
                f"Database pool initialization failed: {init_errors[-1]}",
                reason=_database_service_unavailable_reason(_error_message(init_errors[-1])),
            ) from init_errors[-1]
        raise DatabaseServiceUnavailableError(
            "Database pool initialization failed: no database URL candidates available",
            reason="database_configuration",
        )


def reset_pool(*, expected_pool: ThreadedConnectionPool | None = None) -> None:
    """Reset the shared pool; used for transient transport recovery."""
    with _pool_lock:
        current_pool = _pool
        if current_pool is None:
            return
        if expected_pool is not None and current_pool is not expected_pool:
            return
        _retire_pool_locked(current_pool)


def close_pool() -> None:
    """Close all pooled connections. Intended for tests/process shutdown."""
    global _pool, _active_pool_dsn
    with _pool_lock:
        active_pool = _pool
        retired_pools = list(_retired_pools.values())
        _retired_pools.clear()
        _pool = None
        _active_pool_dsn = None

    if active_pool is not None:
        _close_pool_quietly(active_pool)
    for retired_pool in retired_pools:
        if retired_pool is active_pool:
            continue
        _close_pool_quietly(retired_pool)


def current_pool_dsn() -> str | None:
    """Return the currently active pool DSN for diagnostics."""
    return _active_pool_dsn


def _should_retry_query(error: Exception, *, attempt: int) -> bool:
    max_attempts = _env_int(
        "TRR_DB_TRANSIENT_QUERY_ATTEMPTS",
        DEFAULT_QUERY_TRANSIENT_ATTEMPTS,
        minimum=1,
    )
    return attempt < (max_attempts - 1) and _is_transient_transport_error(error)


def _run_with_transient_retry(operation: Callable[[], T]) -> T:
    max_attempts = _env_int(
        "TRR_DB_TRANSIENT_QUERY_ATTEMPTS",
        DEFAULT_QUERY_TRANSIENT_ATTEMPTS,
        minimum=1,
    )
    last_error: Exception | None = None
    for attempt in range(max_attempts):
        try:
            return operation()
        except Exception as error:
            last_error = error
            if not _should_retry_query(error, attempt=attempt):
                raise
            reset_pool()
    if last_error is not None:
        raise last_error
    raise RuntimeError("unreachable")


def _get_connection_with_retry(*, label: str) -> tuple[ThreadedConnectionPool, connection_type, int]:
    acquire_attempts = _env_int("TRR_DB_POOL_ACQUIRE_ATTEMPTS", DEFAULT_POOL_ACQUIRE_ATTEMPTS, minimum=1)
    acquire_sleep_seconds = _env_int("TRR_DB_POOL_ACQUIRE_SLEEP_MS", DEFAULT_POOL_ACQUIRE_SLEEP_MS, minimum=1) / 1000.0
    last_error: Exception | None = None

    for attempt in range(2):
        pool = _get_pool()
        for acquire_attempt in range(acquire_attempts):
            acquire_started_at = time.perf_counter()
            logger.info(
                "[db-pool] acquire_start label=%s attempt=%s acquire_attempt=%s in_use=%s available=%s",
                label,
                attempt,
                acquire_attempt,
                *_pool_counts(pool),
            )
            try:
                conn = pool.getconn()
                if not _ensure_connection_idle(conn, label=label, phase="checkout"):
                    try:
                        pool.putconn(conn, close=True)
                    except Exception:
                        logger.exception("[db-pool] discard_failed label=%s phase=checkout", label)
                    raise RuntimeError("discarded dirty pooled connection during checkout")
                checkout_id = _log_checkout(
                    pool=pool,
                    conn=conn,
                    label=label,
                    acquire_started_at=acquire_started_at,
                )
                return pool, conn, checkout_id
            except Exception as error:
                last_error = error
                logger.warning(
                    "[db-pool] acquire_failed label=%s attempt=%s acquire_attempt=%s error=%s in_use=%s available=%s",
                    label,
                    attempt,
                    acquire_attempt,
                    type(error).__name__,
                    *_pool_counts(pool),
                )
                if _is_pool_exhausted_error(error):
                    record_postgres_pool_exhausted(
                        _database_service_unavailable_reason(_error_message(error)),
                    )
                if _is_pool_exhausted_error(error) and acquire_attempt < (acquire_attempts - 1):
                    time.sleep(acquire_sleep_seconds)
                    continue
                if _should_retry_query(error, attempt=attempt):
                    reset_pool(expected_pool=pool)
                    break
                raise
        else:
            if last_error is not None:
                raise last_error
    if last_error is not None:
        raise last_error
    raise RuntimeError("unreachable")


@contextmanager
def db_connection(*, label: str = "write"):
    pool, conn, checkout_id = _get_connection_with_retry(label=label)
    try:
        # Pin search_path for the duration of this transaction so pooled connections
        # cannot inherit a prior caller's SET search_path. psycopg2 starts the
        # transaction implicitly with this first statement.
        with conn.cursor() as _cur:
            _cur.execute(f"SET LOCAL search_path = {DEFAULT_WRITE_SEARCH_PATH}")
        yield conn
        conn.commit()
    except Exception as error:
        if _is_statement_timeout_error(error):
            logger.warning(
                "[db-pool] statement_timeout label=%s checkout_id=%s error=%s",
                label,
                checkout_id,
                error,
            )
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        should_close = _is_connection_closed(conn) or not _ensure_connection_idle(
            conn,
            label=label,
            phase="return",
        )
        if should_close:
            try:
                _log_return(pool=pool, conn=conn, checkout_id=checkout_id, label=label)
                pool.putconn(conn, close=True)
            except Exception:
                logger.exception("[db-pool] discard_failed label=%s phase=return", label)
        else:
            try:
                _log_return(pool=pool, conn=conn, checkout_id=checkout_id, label=label)
                pool.putconn(conn)
            except PoolError as error:
                if "pool is closed" not in _error_message(error):
                    raise
        _finalize_retired_pool(pool)


@contextmanager
def db_read_connection(*, label: str = "read"):
    pool, conn, checkout_id = _get_connection_with_retry(label=label)
    previous_autocommit = getattr(conn, "autocommit", False)
    autocommit_restore_failed = False
    try:
        if not previous_autocommit:
            conn.autocommit = True
        yield conn
    except Exception as error:
        if _is_statement_timeout_error(error):
            logger.warning(
                "[db-pool] statement_timeout label=%s checkout_id=%s error=%s",
                label,
                checkout_id,
                error,
            )
        raise
    finally:
        try:
            if not previous_autocommit and not _is_connection_closed(conn):
                conn.autocommit = previous_autocommit
        except Exception as error:
            autocommit_restore_failed = True
            if _is_connection_closed(conn) or _is_transient_transport_error(error):
                logger.warning(
                    "[db-pool] autocommit_restore_failed_closed_connection label=%s",
                    label,
                )
            else:
                logger.exception("[db-pool] autocommit_restore_failed label=%s", label)
        should_close = (
            autocommit_restore_failed
            or _is_connection_closed(conn)
            or not _ensure_connection_idle(conn, label=label, phase="read-return")
        )
        if should_close:
            try:
                _log_return(pool=pool, conn=conn, checkout_id=checkout_id, label=label)
                pool.putconn(conn, close=True)
            except Exception:
                logger.exception("[db-pool] discard_failed label=%s phase=read-return", label)
        else:
            try:
                _log_return(pool=pool, conn=conn, checkout_id=checkout_id, label=label)
                pool.putconn(conn)
            except PoolError as error:
                if "pool is closed" not in _error_message(error):
                    raise
        _finalize_retired_pool(pool)


@contextmanager
def db_cursor(*, conn: connection_type | None = None, label: str = "write-cursor"):
    """Yield a RealDict cursor, optionally reusing an existing connection."""
    if conn is not None:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            yield cur
        return

    with db_connection(label=label) as managed_conn:
        with managed_conn.cursor(cursor_factory=RealDictCursor) as cur:
            yield cur


@contextmanager
def db_read_cursor(*, label: str = "read-cursor"):
    with db_read_connection(label=label) as managed_conn:
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
        with db_read_cursor(label="fetch_all") as cur:
            return fetch_all_with_cursor(cur, query, params)

    return _run_with_transient_retry(_run)


def fetch_one(query: str, params: Iterable[Any] | None = None) -> dict[str, Any] | None:
    def _run() -> dict[str, Any] | None:
        with db_read_cursor(label="fetch_one") as cur:
            return fetch_one_with_cursor(cur, query, params)

    return _run_with_transient_retry(_run)


def execute(query: str, params: Iterable[Any] | None = None) -> None:
    def _run() -> None:
        with db_cursor() as cur:
            cur.execute(query, params or [])

    _run_with_transient_retry(_run)


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
