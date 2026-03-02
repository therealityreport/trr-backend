from __future__ import annotations

import os
from contextvars import ContextVar, Token

_TRACE_ID: ContextVar[str] = ContextVar("trr_trace_id", default="")
_SERVICE_NAME = os.getenv("TRR_METRICS_SERVICE_NAME", "trr_backend_api")

try:  # pragma: no cover - optional dependency in local/test envs
    from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
except Exception:  # pragma: no cover - dependency may be absent
    CONTENT_TYPE_LATEST = "text/plain; version=0.0.4; charset=utf-8"
    Counter = None
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
else:  # pragma: no cover - metrics disabled path
    _REQUEST_TOTAL = None
    _REQUEST_LATENCY = None
    _SUPPRESSED_PATH_CONVERSIONS = None


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


def inc_suppressed_path_conversion(component: str, reason: str) -> None:
    if _SUPPRESSED_PATH_CONVERSIONS is None:
        return
    _SUPPRESSED_PATH_CONVERSIONS.labels(
        _SERVICE_NAME,
        (component or "unknown").strip() or "unknown",
        (reason or "unspecified").strip() or "unspecified",
    ).inc()


def metrics_available() -> bool:
    return generate_latest is not None


def render_metrics() -> bytes:
    if generate_latest is None:
        return b""
    return generate_latest()
