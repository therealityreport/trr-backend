"""Shared backend problem response helpers.

These helpers keep operator-facing errors stable without exposing internal
exception text. Route handlers can keep the existing FastAPI `detail` envelope
while standardizing the problem object inside it.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Mapping
from typing import Any

from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse

JsonMapping = Mapping[str, Any]


def _clean_string(value: object) -> str:
    return str(value or "").strip()


def _decode_header_value(value: bytes) -> str:
    try:
        return value.decode("latin-1").strip()
    except Exception:
        return ""


def _header_value_from_scope(scope: Mapping[str, Any], header_name: bytes) -> str:
    target = header_name.lower()
    for name, value in scope.get("headers", []):
        if bytes(name).lower() == target:
            return _decode_header_value(bytes(value))
    return ""


def _correlation_pair(
    *,
    trace_id: object = "",
    request_id: object = "",
    traceparent: object = "",
) -> tuple[str, str]:
    trace_value = _clean_string(trace_id)
    request_value = _clean_string(request_id)
    traceparent_value = _clean_string(traceparent)
    fallback = trace_value or request_value or traceparent_value or uuid.uuid4().hex
    return trace_value or fallback, request_value or fallback


def correlation_ids_from_scope(scope: Mapping[str, Any]) -> tuple[str, str]:
    """Resolve trace/request ids from an ASGI scope, generating one if absent."""
    return _correlation_pair(
        trace_id=_header_value_from_scope(scope, b"x-trace-id"),
        request_id=_header_value_from_scope(scope, b"x-request-id"),
        traceparent=_header_value_from_scope(scope, b"traceparent"),
    )


def correlation_ids_from_request(request: Request) -> tuple[str, str]:
    """Resolve trace/request ids from a FastAPI request."""
    state_trace_id = _clean_string(getattr(request.state, "trace_id", ""))
    return _correlation_pair(
        trace_id=state_trace_id or request.headers.get("x-trace-id", ""),
        request_id=request.headers.get("x-request-id", ""),
        traceparent=request.headers.get("traceparent", ""),
    )


def build_problem_detail(
    *,
    code: str,
    status: int,
    message: str,
    retryable: bool | None = None,
    trace_id: str = "",
    request_id: str = "",
    safe_detail: object | None = None,
    extra: JsonMapping | None = None,
) -> dict[str, Any]:
    """Build the stable problem object carried in the response `detail` field."""
    resolved_trace_id, resolved_request_id = _correlation_pair(
        trace_id=trace_id,
        request_id=request_id,
    )
    problem: dict[str, Any] = {
        "code": str(code),
        "status": int(status),
        "message": str(message),
        "trace_id": resolved_trace_id,
        "request_id": resolved_request_id,
    }
    if retryable is not None:
        problem["retryable"] = bool(retryable)
    if safe_detail is not None:
        problem["detail"] = safe_detail
    if extra:
        for key, value in extra.items():
            if key in problem or value is None:
                continue
            problem[str(key)] = value
    return problem


def problem_headers(problem: JsonMapping) -> dict[str, str]:
    """Return correlation headers for a problem response."""
    headers: dict[str, str] = {}
    trace_id = _clean_string(problem.get("trace_id"))
    request_id = _clean_string(problem.get("request_id"))
    if trace_id:
        headers["x-trace-id"] = trace_id
    if request_id:
        headers["x-request-id"] = request_id
    return headers


def problem_response(
    request: Request,
    *,
    code: str,
    status: int,
    message: str,
    retryable: bool | None = None,
    safe_detail: object | None = None,
    extra: JsonMapping | None = None,
    headers: Mapping[str, str] | None = None,
) -> JSONResponse:
    """Create a FastAPI JSONResponse with the shared problem shape."""
    trace_id, request_id = correlation_ids_from_request(request)
    problem = build_problem_detail(
        code=code,
        status=status,
        message=message,
        retryable=retryable,
        trace_id=trace_id,
        request_id=request_id,
        safe_detail=safe_detail,
        extra=extra,
    )
    response_headers = problem_headers(problem)
    if headers:
        response_headers.update(headers)
    return JSONResponse(status_code=status, content={"detail": problem}, headers=response_headers)


def problem_http_exception(
    request: Request,
    *,
    code: str,
    status: int,
    message: str,
    retryable: bool | None = None,
    safe_detail: object | None = None,
    extra: JsonMapping | None = None,
    headers: Mapping[str, str] | None = None,
) -> HTTPException:
    """Create an HTTPException carrying the shared problem shape."""
    trace_id, request_id = correlation_ids_from_request(request)
    problem = build_problem_detail(
        code=code,
        status=status,
        message=message,
        retryable=retryable,
        trace_id=trace_id,
        request_id=request_id,
        safe_detail=safe_detail,
        extra=extra,
    )
    exception_headers = problem_headers(problem)
    if headers:
        exception_headers.update(headers)
    return HTTPException(status_code=status, detail=problem, headers=exception_headers)


def problem_json_bytes(problem: JsonMapping) -> bytes:
    """Serialize a problem detail object using the existing FastAPI envelope."""
    return json.dumps({"detail": dict(problem)}, separators=(",", ":")).encode("utf-8")


def problem_asgi_headers(
    problem: JsonMapping,
    *,
    content_length: int,
    scope: Mapping[str, Any] | None = None,
) -> list[list[bytes]]:
    """Return ASGI response headers for a serialized problem response."""
    headers: list[list[bytes]] = [
        [b"content-type", b"application/json"],
        [b"content-length", str(content_length).encode("ascii")],
    ]
    for name, value in problem_headers(problem).items():
        headers.append([name.encode("ascii"), value.encode("latin-1")])
    if scope is not None:
        traceparent = _header_value_from_scope(scope, b"traceparent")
        if traceparent:
            headers.append([b"traceparent", traceparent.encode("latin-1")])
    return headers
