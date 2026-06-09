from __future__ import annotations

import json
import logging
import os
import socket
import sys
import traceback
from contextvars import ContextVar, Token
from datetime import UTC, datetime
from threading import Lock
from typing import Any
from urllib import request as urllib_request

_TRACE_ID: ContextVar[str] = ContextVar("trr_trace_id", default="")
_SERVICE_NAME = os.getenv("TRR_METRICS_SERVICE_NAME", "trr_backend_api")
_LOGGING_LOCK = Lock()
_STREAM_HANDLER_NAME = "trr-stream"
_BETTER_STACK_HANDLER_NAME = "trr-better-stack"
_SENTRY_LOCK = Lock()
_sentry_initialized = False

try:  # pragma: no cover - optional dependency in local/test envs
    from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest
except Exception:  # pragma: no cover - dependency may be absent
    CONTENT_TYPE_LATEST = "text/plain; version=0.0.4; charset=utf-8"
    Counter = None
    Gauge = None
    Histogram = None
    generate_latest = None

if Counter is not None:
    _REQUEST_TOTAL = Counter(
        "trr_api_http_requests_total",
        "Total HTTP requests handled by TRR backend API",
        ("service", "method", "route", "status"),
    )
    _REQUEST_LATENCY = Histogram(
        "trr_api_http_request_duration_seconds",
        "HTTP request latency for TRR backend API",
        ("service", "method", "route", "status"),
        # Buckets tuned to make p95/p99 visible in the 100ms-10s operating range.
        buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
    )
    _SUPPRESSED_PATH_CONVERSIONS = Counter(
        "trr_api_suppressed_path_conversions_total",
        "Count of formerly-suppressed failures now surfaced as explicit logs/guards",
        ("service", "component", "reason"),
    )
    _POSTGRES_POOL_IN_USE = Gauge(
        "trr_api_postgres_pool_in_use",
        "Number of Postgres connections currently checked out of the pool",
        ("service",),
    )
    _POSTGRES_POOL_AVAILABLE = Gauge(
        "trr_api_postgres_pool_available",
        "Number of Postgres connections currently idle and available in the pool",
        ("service",),
    )
    _POSTGRES_POOL_EXHAUSTED = Counter(
        "trr_api_postgres_pool_exhausted_total",
        "Count of pool-exhaustion events (checkout refused because the pool is full)",
        ("service", "reason"),
    )
    _POSTGRES_POOL_ACQUIRE_DURATION = Histogram(
        "trr_api_postgres_pool_acquire_duration_seconds",
        "Time spent waiting to acquire a pooled Postgres connection",
        ("service", "label"),
        # Buckets span instant-acquire (sub-ms) through multi-second queueing.
        buckets=(0.001, 0.005, 0.025, 0.1, 0.25, 1, 5, 15),
    )
    _SOCIAL_PROXY_BYTES = Counter(
        "trr_social_proxy_bytes_total",
        "Total response bytes downloaded through social proxies, attributed by destination host",
        ("service", "provider", "account", "host"),
    )
else:  # pragma: no cover - metrics disabled path
    _REQUEST_TOTAL = None
    _REQUEST_LATENCY = None
    _SUPPRESSED_PATH_CONVERSIONS = None
    _POSTGRES_POOL_IN_USE = None
    _POSTGRES_POOL_AVAILABLE = None
    _POSTGRES_POOL_EXHAUSTED = None
    _POSTGRES_POOL_ACQUIRE_DURATION = None
    _SOCIAL_PROXY_BYTES = None


def _env_bool(name: str, *, default: bool = False) -> bool:
    raw = str(os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def _env_float(name: str, *, default: float) -> float:
    raw = str(os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _resolve_log_level() -> int:
    raw = str(os.getenv("TRR_LOG_LEVEL") or "INFO").strip().upper()
    return getattr(logging, raw, logging.INFO)


def _resolve_runtime_name(service_name: str | None) -> str:
    explicit = str(service_name or "").strip()
    if explicit:
        return explicit
    configured = str(os.getenv("TRR_RUNTIME_SERVICE_NAME") or "").strip()
    if configured:
        return configured
    return _SERVICE_NAME


def _resolve_environment_name() -> str:
    """Resolve the deployment environment name from the standard TRR env var chain."""
    return str(
        os.getenv("TRR_ENV") or os.getenv("TRR_ENVIRONMENT") or os.getenv("APP_ENV") or os.getenv("ENV") or ""
    ).strip()


def _resolve_better_stack_source_token() -> str:
    for key in ("BETTER_STACK_SOURCE_TOKEN", "LOGTAIL_SOURCE_TOKEN"):
        value = str(os.getenv(key) or "").strip()
        if value:
            return value
    return ""


def _resolve_better_stack_endpoint() -> str:
    host = str(os.getenv("BETTER_STACK_INGESTING_HOST") or os.getenv("LOGTAIL_INGESTING_HOST") or "").strip()
    normalized = host or "in.logs.betterstack.com"
    if normalized.startswith(("http://", "https://")):
        return normalized.rstrip("/")
    return f"https://{normalized.rstrip('/')}"


def _build_better_stack_event(record: logging.LogRecord, *, service_name: str) -> dict[str, Any]:
    trace_id = get_trace_id()
    event: dict[str, Any] = {
        "dt": datetime.fromtimestamp(record.created, tz=UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "message": record.getMessage(),
        "level": record.levelname,
        "logger_name": record.name,
        "service": service_name,
        "host": socket.gethostname(),
        "module": record.module,
        "function": record.funcName,
        "path": record.pathname,
        "line": record.lineno,
        "process": record.process,
        "thread_name": record.threadName,
    }
    environment = _resolve_environment_name()
    if environment:
        event["environment"] = environment
    if trace_id:
        event["trace_id"] = trace_id
    if record.exc_info:
        event["exception"] = "".join(traceback.format_exception(*record.exc_info)).strip()
    return event


class BetterStackHTTPHandler(logging.Handler):
    """Send structured Python logs to Better Stack over the documented HTTP ingest API."""

    def __init__(
        self,
        *,
        source_token: str,
        endpoint: str,
        service_name: str,
        timeout_seconds: float,
        failure_cooldown_seconds: float,
    ) -> None:
        super().__init__()
        self.source_token = source_token
        self.endpoint = endpoint
        self.service_name = service_name
        self.timeout_seconds = max(0.1, timeout_seconds)
        self.failure_cooldown_seconds = max(1.0, failure_cooldown_seconds)
        self._muted_until = 0.0
        self._lock = Lock()
        self.set_name(_BETTER_STACK_HANDLER_NAME)

    def emit(self, record: logging.LogRecord) -> None:  # pragma: no cover - covered through helper/tests
        try:
            if record.name == "urllib3":
                return
            request = urllib_request.Request(
                self.endpoint,
                data=json.dumps(_build_better_stack_event(record, service_name=self.service_name), default=str).encode(
                    "utf-8"
                ),
                headers={
                    "Authorization": f"Bearer {self.source_token}",
                    "Content-Type": "application/json",
                },
                method="POST",
            )
            with self._lock:
                if self._muted_until > record.created:
                    return
                urllib_request.urlopen(request, timeout=self.timeout_seconds).read()
        except Exception as exc:  # noqa: BLE001
            self._mute(record.created, exc)

    def _mute(self, created: float, exc: Exception) -> None:
        with self._lock:
            self._muted_until = max(self._muted_until, created + self.failure_cooldown_seconds)
        try:
            sys.stderr.write(
                f"[observability] Better Stack log shipping muted for {int(self.failure_cooldown_seconds)}s: {exc}\n"
            )
        except Exception:  # pragma: no cover - stderr failure is non-fatal
            pass


def configure_sentry(*, service_name: str | None = None) -> None:
    """Initialize Sentry once per process, fully gated on ``SENTRY_DSN``.

    No-op when ``sentry_sdk`` is not installed, when ``SENTRY_DSN`` is empty, or when
    ``TRR_DISABLE_SENTRY`` is truthy. Mirrors the env-gated Better Stack pattern so behavior
    is unchanged unless a DSN is explicitly configured. Safe to call from both the FastAPI
    and Modal entrypoints.
    """
    global _sentry_initialized

    dsn = str(os.getenv("SENTRY_DSN") or "").strip()
    if not dsn or _env_bool("TRR_DISABLE_SENTRY", default=False):
        return

    try:  # pragma: no cover - optional dependency in local/test envs
        import sentry_sdk
    except ImportError:  # pragma: no cover - dependency may be absent
        sys.stderr.write("[observability] SENTRY_DSN is set but sentry-sdk is not installed; skipping Sentry init\n")
        return

    with _SENTRY_LOCK:
        if _sentry_initialized:
            return

        runtime_name = _resolve_runtime_name(service_name)
        integrations: list[Any] = []
        # Enable the FastAPI/Starlette + logging integrations when available; tolerate any
        # sentry-sdk version that ships a different integration surface.
        try:  # pragma: no cover - integration availability varies by sentry-sdk version
            from sentry_sdk.integrations.starlette import StarletteIntegration

            integrations.append(StarletteIntegration())
        except Exception:  # noqa: BLE001 - integration optional
            pass
        try:  # pragma: no cover - integration availability varies by sentry-sdk version
            from sentry_sdk.integrations.fastapi import FastApiIntegration

            integrations.append(FastApiIntegration())
        except Exception:  # noqa: BLE001 - integration optional
            pass
        try:  # pragma: no cover - integration availability varies by sentry-sdk version
            from sentry_sdk.integrations.logging import LoggingIntegration

            integrations.append(LoggingIntegration(level=logging.INFO, event_level=logging.ERROR))
        except Exception:  # noqa: BLE001 - integration optional
            pass

        init_kwargs: dict[str, Any] = {
            "dsn": dsn,
            "traces_sample_rate": _env_float("SENTRY_TRACES_SAMPLE_RATE", default=0.0),
        }
        environment = _resolve_environment_name()
        if environment:
            init_kwargs["environment"] = environment
        if integrations:
            init_kwargs["integrations"] = integrations

        try:
            sentry_sdk.init(**init_kwargs)
        except Exception as exc:  # noqa: BLE001 - never let observability setup break startup
            sys.stderr.write(f"[observability] Sentry init failed: {exc}\n")
            return

        try:  # pragma: no cover - tag is best-effort metadata
            sentry_sdk.set_tag("service", runtime_name)
        except Exception:  # noqa: BLE001 - tagging is non-fatal
            pass

        _sentry_initialized = True


def configure_runtime_observability(*, service_name: str | None = None) -> None:
    """Configure local stdout logging, optional Better Stack log shipping, and optional Sentry once per process."""
    level = _resolve_log_level()
    runtime_name = _resolve_runtime_name(service_name)
    root_logger = logging.getLogger()

    # Initialize Sentry first so any failure during the rest of setup is captured. Fully no-op
    # unless SENTRY_DSN is configured, so this does not change behavior in the default case.
    configure_sentry(service_name=service_name)

    with _LOGGING_LOCK:
        root_logger.setLevel(level)
        if not root_logger.handlers:
            stream_handler = logging.StreamHandler()
            stream_handler.set_name(_STREAM_HANDLER_NAME)
            stream_handler.setLevel(level)
            stream_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s"))
            root_logger.addHandler(stream_handler)

        source_token = _resolve_better_stack_source_token()
        if not source_token or _env_bool("TRR_DISABLE_BETTER_STACK", default=False):
            return
        if any(getattr(handler, "name", "") == _BETTER_STACK_HANDLER_NAME for handler in root_logger.handlers):
            return
        better_stack_handler = BetterStackHTTPHandler(
            source_token=source_token,
            endpoint=_resolve_better_stack_endpoint(),
            service_name=runtime_name,
            timeout_seconds=_env_float("BETTER_STACK_LOG_TIMEOUT_SECONDS", default=2.0),
            failure_cooldown_seconds=_env_float("BETTER_STACK_FAILURE_COOLDOWN_SECONDS", default=60.0),
        )
        better_stack_handler.setLevel(level)
        root_logger.addHandler(better_stack_handler)


def bind_trace_id(trace_id: str) -> Token[str]:
    return _TRACE_ID.set(trace_id)


def reset_trace_id(token: Token[str]) -> None:
    _TRACE_ID.reset(token)


def get_trace_id() -> str | None:
    value = _TRACE_ID.get()
    return value or None


def record_http_request(method: str, route: str, status_code: int, duration_seconds: float) -> None:
    if _REQUEST_TOTAL is None or _REQUEST_LATENCY is None:
        return
    status = str(int(status_code))
    _REQUEST_TOTAL.labels(_SERVICE_NAME, method.upper(), route, status).inc()
    _REQUEST_LATENCY.labels(_SERVICE_NAME, method.upper(), route, status).observe(max(0.0, float(duration_seconds)))


def record_proxy_bytes(provider: str, account: str, host: str, n: int) -> None:
    """Increment the social-proxy byte counter, labelled by provider/account/host.

    No-op when prometheus_client is absent or when ``n`` is non-positive. Mirrors the
    fail-open style of ``record_http_request``: never raise from the metering path.
    """
    if _SOCIAL_PROXY_BYTES is None:
        return
    try:
        count = int(n)
    except (TypeError, ValueError):
        return
    if count <= 0:
        return
    _SOCIAL_PROXY_BYTES.labels(
        _SERVICE_NAME,
        (provider or "unknown").strip() or "unknown",
        (account or "unknown").strip() or "unknown",
        (host or "unknown").strip() or "unknown",
    ).inc(count)


def inc_suppressed_path_conversion(component: str, reason: str) -> None:
    if _SUPPRESSED_PATH_CONVERSIONS is None:
        return
    _SUPPRESSED_PATH_CONVERSIONS.labels(
        _SERVICE_NAME,
        (component or "unknown").strip() or "unknown",
        (reason or "unspecified").strip() or "unspecified",
    ).inc()


def record_postgres_pool_state(*, in_use: int | None, available: int | None) -> None:
    """Emit current Postgres pool in-use/available counts as Prometheus gauges."""
    if _POSTGRES_POOL_IN_USE is None or _POSTGRES_POOL_AVAILABLE is None:
        return
    if in_use is not None:
        _POSTGRES_POOL_IN_USE.labels(_SERVICE_NAME).set(float(in_use))
    if available is not None:
        _POSTGRES_POOL_AVAILABLE.labels(_SERVICE_NAME).set(float(available))


def record_postgres_pool_exhausted(reason: str) -> None:
    """Increment the pool-exhaustion counter, labelled by classifier reason."""
    if _POSTGRES_POOL_EXHAUSTED is None:
        return
    _POSTGRES_POOL_EXHAUSTED.labels(
        _SERVICE_NAME,
        (reason or "unspecified").strip() or "unspecified",
    ).inc()


def record_postgres_pool_acquire_duration(label: str, duration_seconds: float) -> None:
    """Record a histogram observation for the time taken to acquire a pooled connection."""
    if _POSTGRES_POOL_ACQUIRE_DURATION is None:
        return
    _POSTGRES_POOL_ACQUIRE_DURATION.labels(
        _SERVICE_NAME,
        (label or "unknown").strip() or "unknown",
    ).observe(max(0.0, float(duration_seconds)))


def metrics_available() -> bool:
    return generate_latest is not None


def render_metrics() -> bytes:
    if generate_latest is None:
        return b""
    return generate_latest()
