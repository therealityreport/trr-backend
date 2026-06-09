"""Pure ASGI middleware that enforces a wall-clock timeout on HTTP requests.

Uses asyncio.wait_for to cancel requests exceeding a configurable limit.
Health/metrics/liveness endpoints are exempt so monitoring probes never time out.
Known long-lived SSE routes opt out via explicit path patterns.
"""

from __future__ import annotations

import asyncio
import logging
import os
import re

from trr_backend.problem import (
    build_problem_detail,
    correlation_ids_from_scope,
    problem_asgi_headers,
    problem_json_bytes,
)

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT_SECONDS = 30.0
WEEK_DETAIL_TIMEOUT_SECONDS = 45.0

# Paths exempt from timeout enforcement.
# Health, metrics, and liveness probes must never be timed out.
# SSE/streaming routes are added from the route inventory.
EXEMPT_PATHS: frozenset[str] = frozenset(
    {
        "/",
        "/health",
        "/health/live",
        "/metrics",
    }
)

# Long-running screentime promotion requests can exceed the generic API timeout
# when they verify and promote large uploaded videos into canonical assets.
EXEMPT_PATH_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"^/api/v1/admin/bravotv/images/stream$"),
    re.compile(r"^/api/v1/admin/bravotv/images/shows/[^/]+/stream$"),
    re.compile(r"^/api/v1/admin/bravotv/images/people/[^/]+/stream$"),
    re.compile(r"^/api/v1/admin/operations/[^/]+/stream$"),
    re.compile(r"^/api/v1/admin/person/[^/]+/refresh-images/stream$"),
    re.compile(r"^/api/v1/admin/person/[^/]+/refresh-profile/stream$"),
    re.compile(r"^/api/v1/admin/person/[^/]+/reprocess-images/stream$"),
    re.compile(r"^/api/v1/admin/scrape/import/stream$"),
    re.compile(r"^/api/v1/admin/shows/[^/]+/assets/batch-jobs/stream$"),
    re.compile(r"^/api/v1/admin/shows/[^/]+/get-images/stream$"),
    re.compile(r"^/api/v1/admin/shows/[^/]+/import-bravo/preview/stream$"),
    re.compile(r"^/api/v1/admin/shows/[^/]+/links/discover/stream$"),
    re.compile(r"^/api/v1/admin/shows/[^/]+/refresh-photos/stream$"),
    re.compile(r"^/api/v1/admin/shows/[^/]+/refresh/stream$"),
    re.compile(r"^/api/v1/admin/shows/[^/]+/seasons/[^/]+/assets/batch-jobs/stream$"),
    re.compile(r"^/api/v1/admin/socials/live-status/stream$"),
    re.compile(r"^/api/v1/admin/socials/profiles/[^/]+/[^/]+/catalog/backfill$"),
    re.compile(r"^/api/v1/admin/socials/profiles/[^/]+/[^/]+/catalog/runs/[^/]+/manual-auth$"),
    re.compile(r"^/api/v1/admin/socials/profiles/[^/]+/[^/]+/catalog/runs/[^/]+/repair-auth$"),
    re.compile(r"^/api/v1/admin/socials/profiles/[^/]+/[^/]+/cookies/refresh$"),
    re.compile(r"^/api/v1/admin/socials/profiles/[^/]+/[^/]+/posts$"),
    re.compile(r"^/api/v1/admin/socials/profiles/[^/]+/[^/]+/summary$"),
    re.compile(r"^/api/v1/admin/socials/profiles/[^/]+/[^/]+/catalog/runs/[^/]+/progress$"),
    re.compile(r"^/api/v1/admin/socials/profiles/[^/]+/[^/]+/comments/scrape$"),
    re.compile(r"^/api/v1/admin/socials/seasons/[^/]+/sync-sessions/[^/]+/stream$"),
    re.compile(r"^/api/v1/admin/cast-screentime/upload-sessions/[^/]+/complete$"),
    re.compile(r"^/api/v1/admin/cast-screentime/video-assets/[^/]+/runs$"),
    re.compile(r"^/api/v1/admin/people/[^/]+/socialblade/refresh$"),
    re.compile(r"^/api/v1/admin/socials/profiles/[^/]+/[^/]+/socialblade/refresh$"),
)

EXTENDED_TIMEOUT_PATH_PATTERNS: tuple[tuple[re.Pattern[str], float], ...] = (
    # The app proxy waits 40s for week detail reads; keep a backend-side cap
    # slightly above that so aborted app requests cannot run indefinitely.
    (re.compile(r"^/api/v1/admin/socials/seasons/[^/]+/analytics/week/[^/]+$"), WEEK_DETAIL_TIMEOUT_SECONDS),
)


def _parse_timeout_from_env() -> float:
    raw = (os.getenv("TRR_REQUEST_TIMEOUT_SECONDS") or "").strip()
    if not raw:
        return DEFAULT_TIMEOUT_SECONDS
    try:
        value = float(raw)
        return value if value > 0 else DEFAULT_TIMEOUT_SECONDS
    except ValueError:
        return DEFAULT_TIMEOUT_SECONDS


def _is_exempt(path: str) -> bool:
    """Check if a path is exempt from timeout enforcement.

    Uses path-based matching because pure ASGI middleware runs before
    FastAPI routing, so route-level metadata is not yet available.
    Known long-lived stream routes are explicitly enumerated to avoid
    granting arbitrary `/stream` paths an unlimited runtime.
    """
    if path in EXEMPT_PATHS:
        return True
    return any(pattern.match(path) for pattern in EXEMPT_PATH_PATTERNS)


def _timeout_for_path(path: str, default_timeout_seconds: float) -> float:
    for pattern, timeout_seconds in EXTENDED_TIMEOUT_PATH_PATTERNS:
        if pattern.match(path):
            return max(default_timeout_seconds, timeout_seconds)
    return default_timeout_seconds


class RequestTimeoutMiddleware:
    """Pure ASGI middleware — wraps the inner app with asyncio.wait_for."""

    def __init__(self, app, *, timeout_seconds: float | None = None) -> None:
        self.app = app
        self.timeout_seconds = timeout_seconds if timeout_seconds is not None else _parse_timeout_from_env()

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path = scope.get("path", "")
        if _is_exempt(path):
            await self.app(scope, receive, send)
            return
        timeout_seconds = _timeout_for_path(path, self.timeout_seconds)

        response_started = False

        async def send_wrapper(message):
            nonlocal response_started
            if message["type"] == "http.response.start":
                response_started = True
            await send(message)

        try:
            await asyncio.wait_for(
                self.app(scope, receive, send_wrapper),
                timeout=timeout_seconds,
            )
        except asyncio.TimeoutError:  # noqa: UP041 — asyncio.TimeoutError != TimeoutError on Python <3.11
            method = scope.get("method", "?")
            logger.warning(
                "[request-timeout] path=%s method=%s timeout_seconds=%s",
                path,
                method,
                timeout_seconds,
            )
            # Only send 504 if the response hasn't already started
            if not response_started:
                trace_id, request_id = correlation_ids_from_scope(scope)
                problem = build_problem_detail(
                    code="REQUEST_TIMEOUT",
                    status=504,
                    message=f"Request timed out after {timeout_seconds}s",
                    retryable=True,
                    trace_id=trace_id,
                    request_id=request_id,
                    extra={
                        "retry_after_ms": 1000,
                        "timeout_seconds": timeout_seconds,
                    },
                )
                body = problem_json_bytes(problem)
                await send(
                    {
                        "type": "http.response.start",
                        "status": 504,
                        "headers": problem_asgi_headers(problem, content_length=len(body), scope=scope),
                    }
                )
                await send(
                    {
                        "type": "http.response.body",
                        "body": body,
                    }
                )
