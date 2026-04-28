"""Pure ASGI middleware that enforces a wall-clock timeout on HTTP requests.

Uses asyncio.wait_for to cancel requests exceeding a configurable limit.
Health/metrics/liveness endpoints are exempt so monitoring probes never time out.
Known long-lived SSE routes opt out via explicit path patterns.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT_SECONDS = 30.0

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


def _parse_timeout_from_env() -> float:
    raw = (os.getenv("TRR_REQUEST_TIMEOUT_SECONDS") or "").strip()
    if not raw:
        return DEFAULT_TIMEOUT_SECONDS
    try:
        value = float(raw)
        return value if value > 0 else DEFAULT_TIMEOUT_SECONDS
    except ValueError:
        return DEFAULT_TIMEOUT_SECONDS


def _extract_trace_headers(scope: dict) -> list[tuple[bytes, bytes]]:
    """Extract trace-related headers from the incoming request to forward in error responses."""
    trace_header_names = {b"x-request-id", b"x-trace-id", b"traceparent"}
    headers: list[tuple[bytes, bytes]] = []
    for name, value in scope.get("headers", []):
        if name.lower() in trace_header_names:
            headers.append((name, value))
    return headers


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

        response_started = False

        async def send_wrapper(message):
            nonlocal response_started
            if message["type"] == "http.response.start":
                response_started = True
            await send(message)

        try:
            await asyncio.wait_for(
                self.app(scope, receive, send_wrapper),
                timeout=self.timeout_seconds,
            )
        except asyncio.TimeoutError:  # noqa: UP041 — asyncio.TimeoutError != TimeoutError on Python <3.11
            method = scope.get("method", "?")
            logger.warning(
                "[request-timeout] path=%s method=%s timeout_seconds=%s",
                path,
                method,
                self.timeout_seconds,
            )
            # Only send 504 if the response hasn't already started
            if not response_started:
                body = json.dumps(
                    {
                        "detail": {
                            "code": "REQUEST_TIMEOUT",
                            "message": f"Request timed out after {self.timeout_seconds}s",
                            "retryable": True,
                        }
                    }
                ).encode()
                trace_headers = _extract_trace_headers(scope)
                await send(
                    {
                        "type": "http.response.start",
                        "status": 504,
                        "headers": [
                            [b"content-type", b"application/json"],
                            [b"content-length", str(len(body)).encode()],
                        ]
                        + trace_headers,
                    }
                )
                await send(
                    {
                        "type": "http.response.body",
                        "body": body,
                    }
                )
