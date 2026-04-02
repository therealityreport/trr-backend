"""Tests for request timeout middleware."""

from __future__ import annotations

import asyncio

from fastapi import FastAPI
from fastapi.testclient import TestClient

from trr_backend.middleware.request_timeout import RequestTimeoutMiddleware


def _make_app(timeout_seconds: float) -> FastAPI:
    app = FastAPI()
    app.add_middleware(RequestTimeoutMiddleware, timeout_seconds=timeout_seconds)

    @app.get("/fast")
    async def fast_endpoint():
        return {"ok": True}

    @app.get("/slow")
    async def slow_endpoint():
        await asyncio.sleep(10)
        return {"ok": True}

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    return app


class TestRequestTimeoutMiddleware:
    def test_fast_request_succeeds(self):
        app = _make_app(timeout_seconds=5.0)
        client = TestClient(app)
        response = client.get("/fast")
        assert response.status_code == 200

    def test_slow_request_times_out(self):
        app = _make_app(timeout_seconds=0.1)
        client = TestClient(app)
        response = client.get("/slow")
        assert response.status_code == 504
        body = response.json()
        assert body["detail"]["code"] == "REQUEST_TIMEOUT"
        assert body["detail"]["retryable"] is True

    def test_health_endpoint_exempt(self):
        app = _make_app(timeout_seconds=0.1)
        client = TestClient(app)
        response = client.get("/health")
        assert response.status_code == 200

    def test_default_timeout_from_env(self, monkeypatch):
        monkeypatch.setenv("TRR_REQUEST_TIMEOUT_SECONDS", "42")
        from trr_backend.middleware.request_timeout import _parse_timeout_from_env

        assert _parse_timeout_from_env() == 42.0

    def test_stream_endpoint_exempt(self):
        """SSE/stream routes ending in /stream bypass timeout."""
        app = _make_app(timeout_seconds=0.1)

        @app.get("/api/v1/admin/some-resource/stream")
        async def stream_endpoint():
            await asyncio.sleep(1)  # Would timeout if not exempt
            return {"streaming": True}

        client = TestClient(app)
        response = client.get("/api/v1/admin/some-resource/stream")
        assert response.status_code == 200

    def test_timeout_response_forwards_trace_headers(self):
        """504 response includes trace headers from the original request."""
        app = _make_app(timeout_seconds=0.1)

        @app.get("/traced")
        async def traced_endpoint():
            await asyncio.sleep(10)
            return {"ok": True}

        client = TestClient(app)
        response = client.get("/traced", headers={"X-Request-ID": "test-trace-123"})
        assert response.status_code == 504
        assert response.headers.get("x-request-id") == "test-trace-123"
