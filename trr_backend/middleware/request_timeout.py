"""Pure ASGI middleware that enforces a wall-clock timeout on HTTP requests.

Uses asyncio.wait_for to cancel requests exceeding a configurable limit.
Health/metrics/liveness endpoints are exempt so monitoring probes never time out.
SSE and streaming routes can opt out via exempt_paths configuration.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os

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

# SSE/streaming route path prefixes that must be exempt.
# These are identified from the Phase 0 route inventory.
EXEMPT_STREAM_SUFFIXES: tuple[str, ...] = ("/stream",)


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
    """Check if a path is exempt from timeout enforcement."""
    if path in EXEMPT_PATHS:
        return True
    # SSE/streaming routes end with /stream
    return any(path.endswith(suffix) for suffix in EXEMPT_STREAM_SUFFIXES)


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
                await send(
                    {
                        "type": "http.response.start",
                        "status": 504,
                        "headers": [
                            [b"content-type", b"application/json"],
                            [b"content-length", str(len(body)).encode()],
                        ],
                    }
                )
                await send(
                    {
                        "type": "http.response.body",
                        "body": body,
                    }
                )
