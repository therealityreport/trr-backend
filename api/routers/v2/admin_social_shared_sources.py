"""Strict API v2 admin endpoints for shared social account sources."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from pydantic import ValidationError

from api.auth import InternalAdminUser
from api.schemas.v2.admin_social_shared_sources import (
    PutSharedAccountSourcesRequestV2,
    SharedAccountSourcesProblemResponseV2,
    SharedAccountSourcesResponseV2,
)
from trr_backend.db.pg import database_service_unavailable_detail, is_database_service_unavailable_error
from trr_backend.problem import problem_http_exception
from trr_backend.socials.control_plane import shared_source_config

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/socials/shared-account-sources", tags=["admin-social-shared-sources-v2"])

_ERROR_RESPONSES: dict[int | str, dict[str, Any]] = {
    400: {"model": SharedAccountSourcesProblemResponseV2, "description": "The shared-source request is invalid."},
    500: {"model": SharedAccountSourcesProblemResponseV2, "description": "The shared-source request failed."},
    503: {"model": SharedAccountSourcesProblemResponseV2, "description": "The shared-source store is unavailable."},
}


def _problem(request: Request, *, code: str, status: int, message: str, retryable: bool = False) -> HTTPException:
    return problem_http_exception(request, code=code, status=status, message=message, retryable=retryable)


def _unexpected_problem(error: Exception, request: Request, *, operation: str) -> HTTPException:
    if isinstance(error, HTTPException):
        return error
    if is_database_service_unavailable_error(error):
        detail = database_service_unavailable_detail(error)
        return problem_http_exception(
            request,
            code=str(detail.get("code") or "DATABASE_SERVICE_UNAVAILABLE"),
            status=503,
            message=str(detail.get("message") or "Database service unavailable."),
            retryable=bool(detail.get("retryable", True)),
            extra={"reason": detail.get("reason"), "retry_after_ms": detail.get("retry_after_ms")},
        )
    logger.exception("[admin-social-shared-sources-v2] %s failed", operation)
    return _problem(
        request,
        code="SHARED_ACCOUNT_SOURCES_REQUEST_FAILED",
        status=500,
        message="The shared account source request could not be completed.",
    )


def _parse_include_inactive(request: Request) -> bool:
    raw = str(request.query_params.get("include_inactive", "true")).strip().lower()
    if raw in {"1", "true"}:
        return True
    if raw in {"0", "false"}:
        return False
    raise _problem(
        request,
        code="INVALID_INCLUDE_INACTIVE",
        status=400,
        message="include_inactive must be true or false.",
    )


def _parse_platforms(request: Request) -> list[str] | None:
    raw = request.query_params.get("platforms")
    if raw is None:
        return None
    platforms = [item.strip() for item in raw.split(",") if item.strip()]
    if not platforms:
        raise _problem(
            request,
            code="INVALID_PLATFORM_FILTER",
            status=400,
            message="platforms must contain at least one supported platform.",
        )
    return platforms


@router.get(
    "",
    response_model=SharedAccountSourcesResponseV2,
    operation_id="getAdminSharedAccountSourcesV2",
    responses=_ERROR_RESPONSES,
)
def get_shared_account_sources(request: Request, _: InternalAdminUser) -> SharedAccountSourcesResponseV2:
    try:
        payload = shared_source_config.get_shared_account_sources(
            source_scope=request.query_params.get("source_scope", "network"),
            include_inactive=_parse_include_inactive(request),
            platforms=_parse_platforms(request),
        )
        return SharedAccountSourcesResponseV2.model_validate(payload)
    except ValueError as error:
        raise _problem(
            request,
            code="INVALID_SHARED_ACCOUNT_SOURCE_QUERY",
            status=400,
            message=str(error),
        ) from error
    except Exception as error:
        raise _unexpected_problem(error, request, operation="get") from error


@router.put(
    "",
    response_model=SharedAccountSourcesResponseV2,
    operation_id="putAdminSharedAccountSourcesV2",
    responses=_ERROR_RESPONSES,
)
async def put_shared_account_sources(request: Request, user: InternalAdminUser) -> SharedAccountSourcesResponseV2:
    try:
        body = PutSharedAccountSourcesRequestV2.model_validate(await request.json())
    except (ValidationError, ValueError, TypeError) as error:
        raise _problem(
            request,
            code="INVALID_SHARED_ACCOUNT_SOURCES_REQUEST",
            status=400,
            message="source_scope and a strict sources array are required, with no extra fields.",
        ) from error
    try:
        payload = shared_source_config.put_shared_account_sources(
            source_scope=body.source_scope,
            sources=[source.model_dump() for source in body.sources],
            updated_by=(user or {}).get("admin_email") or (user or {}).get("email"),
        )
        return SharedAccountSourcesResponseV2.model_validate(payload)
    except ValueError as error:
        raise _problem(
            request,
            code="INVALID_SHARED_ACCOUNT_SOURCES_REQUEST",
            status=400,
            message=str(error),
        ) from error
    except Exception as error:
        raise _unexpected_problem(error, request, operation="put") from error
