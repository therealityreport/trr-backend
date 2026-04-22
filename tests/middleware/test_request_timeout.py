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

    def test_known_stream_endpoint_exempt(self):
        """Known long-lived SSE routes bypass timeout enforcement."""
        app = _make_app(timeout_seconds=0.1)

        @app.get("/api/v1/admin/socials/live-status/stream")
        async def stream_endpoint():
            await asyncio.sleep(1)  # Would timeout if not exempt
            return {"streaming": True}

        client = TestClient(app)
        response = client.get("/api/v1/admin/socials/live-status/stream")
        assert response.status_code == 200

    def test_social_profile_posts_endpoint_exempt(self):
        """Shared profile posts reads use the proxy timeout tiers instead of the generic backend wall clock."""
        app = _make_app(timeout_seconds=0.1)

        @app.get("/api/v1/admin/socials/profiles/tiktok/bravotv/posts")
        async def posts_endpoint():
            await asyncio.sleep(1)  # Would timeout if not exempt
            return {"items": [], "pagination": {"page": 1, "page_size": 25, "total": 0, "total_pages": 1}}

        client = TestClient(app)
        response = client.get("/api/v1/admin/socials/profiles/tiktok/bravotv/posts?comments_only=true")
        assert response.status_code == 200

    def test_unknown_stream_endpoint_not_exempt(self):
        """Arbitrary /stream paths should not bypass the timeout."""
        app = _make_app(timeout_seconds=0.1)

        @app.get("/api/v1/admin/some-resource/stream")
        async def stream_endpoint():
            await asyncio.sleep(1)
            return {"streaming": True}

        client = TestClient(app)
        response = client.get("/api/v1/admin/some-resource/stream")
        assert response.status_code == 504

    def test_cast_screentime_upload_complete_endpoint_exempt(self):
        """Large screentime promotion requests must bypass the global request timeout."""
        app = _make_app(timeout_seconds=0.1)

        @app.post("/api/v1/admin/cast-screentime/upload-sessions/upload-123/complete")
        async def upload_complete_endpoint():
            await asyncio.sleep(1)  # Would timeout if not exempt
            return {"promoted": True}

        client = TestClient(app)
        response = client.post("/api/v1/admin/cast-screentime/upload-sessions/upload-123/complete")
        assert response.status_code == 200
        assert response.json() == {"promoted": True}

    def test_cast_screentime_run_create_endpoint_exempt(self):
        """Live run creation can exceed the generic timeout while snapshotting candidate cast."""
        app = _make_app(timeout_seconds=0.1)

        @app.post("/api/v1/admin/cast-screentime/video-assets/asset-123/runs")
        async def create_run_endpoint():
            await asyncio.sleep(1)  # Would timeout if not exempt
            return {"queued": True}

        client = TestClient(app)
        response = client.post("/api/v1/admin/cast-screentime/video-assets/asset-123/runs")
        assert response.status_code == 200
        assert response.json() == {"queued": True}

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
