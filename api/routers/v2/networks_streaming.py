"""Strict API v2 networks/streaming admin summary endpoint."""

from __future__ import annotations

import json
import logging
import re
import time
from typing import Any

from fastapi import APIRouter, HTTPException, Request

from api.auth import InternalAdminUser
from api.schemas.v2.networks_streaming import (
    NetworkStreamingDetailNotFoundProblemResponseV2,
    NetworkStreamingDetailResponseV2,
    NetworkStreamingProblemResponseV2,
    NetworkStreamingSuggestionV2,
    NetworkStreamingSummaryResponseV2,
)
from trr_backend.db.pg import database_service_unavailable_detail, is_database_service_unavailable_error
from trr_backend.problem import problem_http_exception
from trr_backend.read_path_diagnostics import log_read_path
from trr_backend.services import networks_streaming_reads as networks_streaming_reads_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/networks-streaming", tags=["admin-networks-streaming-v2"])

_SUMMARY_ERROR_RESPONSES: dict[int | str, dict[str, Any]] = {
    500: {
        "model": NetworkStreamingProblemResponseV2,
        "description": "The networks/streaming summary request failed.",
    },
    503: {
        "model": NetworkStreamingProblemResponseV2,
        "description": "The networks/streaming store is unavailable.",
    },
}
_DETAIL_ERROR_RESPONSES: dict[int | str, dict[str, Any]] = {
    400: {
        "model": NetworkStreamingProblemResponseV2,
        "description": "The networks/streaming detail lookup is invalid.",
    },
    404: {
        "model": NetworkStreamingDetailNotFoundProblemResponseV2,
        "description": "The entity was not found; safe replacement suggestions are included.",
    },
    500: {
        "model": NetworkStreamingProblemResponseV2,
        "description": "The networks/streaming detail request failed.",
    },
    503: {
        "model": NetworkStreamingProblemResponseV2,
        "description": "The networks/streaming store is unavailable.",
    },
}
_MAX_LOOKUP_LENGTH = 240
_ENTITY_SLUG_RE = re.compile(r"^[a-z0-9]+(?:-+[a-z0-9]+)*$")
_DETAIL_QUERY_PARAMETERS = [
    {
        "name": "entity_type",
        "in": "query",
        "required": True,
        "description": "The entity registry to query.",
        "schema": {
            "type": "string",
            "enum": ["network", "streaming", "production"],
        },
    },
    {
        "name": "entity_key",
        "in": "query",
        "required": False,
        "description": "The case-insensitive entity key. Provide this or entity_slug.",
        "schema": {"type": "string", "minLength": 1, "maxLength": _MAX_LOOKUP_LENGTH},
    },
    {
        "name": "entity_slug",
        "in": "query",
        "required": False,
        "description": "The entity slug. Provide this or entity_key.",
        "schema": {
            "type": "string",
            "minLength": 1,
            "maxLength": _MAX_LOOKUP_LENGTH,
            "pattern": r"^[A-Za-z0-9]+(?:-+[A-Za-z0-9]+)*$",
        },
    },
]


def _database_problem(error: Exception, request: Request) -> HTTPException:
    detail = database_service_unavailable_detail(error)
    return problem_http_exception(
        request,
        code=str(detail.get("code") or "DATABASE_SERVICE_UNAVAILABLE"),
        status=503,
        message=str(detail.get("message") or "Database service unavailable."),
        retryable=bool(detail.get("retryable", True)),
        extra={
            "reason": detail.get("reason"),
            "retry_after_ms": detail.get("retry_after_ms"),
        },
    )


def _problem(
    request: Request,
    *,
    code: str,
    status: int,
    message: str,
    retryable: bool = False,
    extra: dict[str, Any] | None = None,
) -> HTTPException:
    return problem_http_exception(
        request,
        code=code,
        status=status,
        message=message,
        retryable=retryable,
        extra=extra,
    )


def _unexpected_problem(
    error: Exception,
    request: Request,
    *,
    operation: str,
) -> HTTPException:
    if is_database_service_unavailable_error(error):
        return _database_problem(error, request)
    logger.exception("[admin-networks-streaming-v2] %s failed", operation)
    return _problem(
        request,
        code=f"NETWORKS_STREAMING_{operation.upper()}_FAILED",
        status=500,
        message=f"The networks/streaming {operation} request could not be completed.",
    )


def _parse_detail_lookup(request: Request) -> tuple[str, str | None, str | None]:
    normalized_type = str(request.query_params.get("entity_type") or "").strip().casefold()
    if normalized_type not in {"network", "streaming", "production"}:
        raise _problem(
            request,
            code="INVALID_NETWORKS_STREAMING_ENTITY_TYPE",
            status=400,
            message="entity_type must be network, streaming, or production.",
        )

    entity_key = str(request.query_params.get("entity_key") or "").strip()
    entity_slug = str(request.query_params.get("entity_slug") or "").strip()
    if not entity_key and not entity_slug:
        raise _problem(
            request,
            code="NETWORKS_STREAMING_LOOKUP_REQUIRED",
            status=400,
            message="entity_key or entity_slug is required.",
        )
    if len(entity_key) > _MAX_LOOKUP_LENGTH:
        raise _problem(
            request,
            code="INVALID_NETWORKS_STREAMING_ENTITY_KEY",
            status=400,
            message=f"entity_key must be at most {_MAX_LOOKUP_LENGTH} characters.",
        )
    normalized_slug = entity_slug.casefold()
    if entity_slug and (len(entity_slug) > _MAX_LOOKUP_LENGTH or _ENTITY_SLUG_RE.fullmatch(normalized_slug) is None):
        raise _problem(
            request,
            code="INVALID_NETWORKS_STREAMING_ENTITY_SLUG",
            status=400,
            message="entity_slug must contain only letters, numbers, and hyphens.",
        )
    return normalized_type, entity_key or None, normalized_slug or None


@router.get(
    "/summary",
    response_model=NetworkStreamingSummaryResponseV2,
    operation_id="getAdminNetworksStreamingSummaryV2",
    summary="Get the networks and streaming coverage summary",
    responses=_SUMMARY_ERROR_RESPONSES,
)
def get_networks_streaming_summary(
    request: Request,
    _: InternalAdminUser,
) -> NetworkStreamingSummaryResponseV2:
    started_at = time.perf_counter()
    try:
        payload, query_count, cache_status = networks_streaming_reads_service.get_networks_streaming_summary()
        response = NetworkStreamingSummaryResponseV2.model_validate_json(
            json.dumps(payload),
            strict=True,
        )
    except Exception as error:
        raise _unexpected_problem(error, request, operation="summary") from error

    log_read_path(
        "admin-networks-streaming-v2.summary",
        latency_ms=(time.perf_counter() - started_at) * 1000.0,
        query_count=query_count,
        payload=payload,
        extra={"cache": cache_status},
    )
    return response


@router.get(
    "/detail",
    response_model=NetworkStreamingDetailResponseV2,
    operation_id="getAdminNetworksStreamingDetailV2",
    summary="Get full networks/streaming entity detail",
    responses=_DETAIL_ERROR_RESPONSES,
    openapi_extra={"parameters": _DETAIL_QUERY_PARAMETERS},
)
def get_networks_streaming_detail(
    request: Request,
    _: InternalAdminUser,
) -> NetworkStreamingDetailResponseV2:
    entity_type, entity_key, entity_slug = _parse_detail_lookup(request)
    started_at = time.perf_counter()
    try:
        payload, query_count, cache_status = networks_streaming_reads_service.get_networks_streaming_detail(
            entity_type=entity_type,
            entity_key=entity_key,
            entity_slug=entity_slug,
        )
        response = NetworkStreamingDetailResponseV2.model_validate_json(
            json.dumps(payload),
            strict=True,
        )
    except networks_streaming_reads_service.NetworksStreamingDetailNotFoundError as error:
        try:
            suggestions = [
                NetworkStreamingSuggestionV2.model_validate_json(
                    json.dumps(suggestion),
                    strict=True,
                ).model_dump(mode="json")
                for suggestion in error.suggestions
            ]
            not_found_problem = _problem(
                request,
                code="NETWORKS_STREAMING_ENTITY_NOT_FOUND",
                status=404,
                message="Networks/streaming entity not found.",
                extra={"suggestions": suggestions},
            )
            NetworkStreamingDetailNotFoundProblemResponseV2.model_validate_json(
                json.dumps({"detail": not_found_problem.detail}),
                strict=True,
            )
        except Exception as validation_error:
            raise _unexpected_problem(
                validation_error,
                request,
                operation="detail",
            ) from validation_error
        log_read_path(
            "admin-networks-streaming-v2.detail-not-found",
            latency_ms=(time.perf_counter() - started_at) * 1000.0,
            query_count=error.query_count,
            payload={"suggestions": suggestions},
            extra={"cache": "miss"},
        )
        raise not_found_problem from error
    except Exception as error:
        raise _unexpected_problem(error, request, operation="detail") from error

    log_read_path(
        "admin-networks-streaming-v2.detail",
        latency_ms=(time.perf_counter() - started_at) * 1000.0,
        query_count=query_count,
        payload=payload,
        extra={"cache": cache_status},
    )
    return response
