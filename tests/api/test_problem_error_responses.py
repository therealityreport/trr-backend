from __future__ import annotations

import asyncio

from fastapi import FastAPI
from fastapi.testclient import TestClient

from trr_backend.middleware.request_timeout import RequestTimeoutMiddleware
from trr_backend.problem import build_problem_detail


def test_problem_detail_helper_includes_stable_safe_fields() -> None:
    problem = build_problem_detail(
        code="BACKEND_SATURATED",
        status=503,
        message="Backend capacity is temporarily saturated.",
        retryable=True,
        trace_id="trace-123",
        request_id="request-123",
        safe_detail={"reason": "pool_capacity"},
        extra={"retry_after_ms": 1000},
    )

    assert problem == {
        "code": "BACKEND_SATURATED",
        "status": 503,
        "message": "Backend capacity is temporarily saturated.",
        "trace_id": "trace-123",
        "request_id": "request-123",
        "retryable": True,
        "detail": {"reason": "pool_capacity"},
        "retry_after_ms": 1000,
    }


def test_timeout_problem_response_generates_trace_and_request_ids() -> None:
    app = FastAPI()
    app.add_middleware(RequestTimeoutMiddleware, timeout_seconds=0.01)

    @app.get("/slow")
    async def slow_endpoint() -> dict[str, bool]:
        await asyncio.sleep(1)
        return {"ok": True}

    response = TestClient(app).get("/slow")

    assert response.status_code == 504
    detail = response.json()["detail"]
    assert detail["code"] == "REQUEST_TIMEOUT"
    assert detail["status"] == 504
    assert detail["retryable"] is True
    assert detail["trace_id"]
    assert detail["request_id"]
    assert response.headers["x-trace-id"] == detail["trace_id"]
    assert response.headers["x-request-id"] == detail["request_id"]
