"""Strict API v2 endpoint for recent social-account catalog runs."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, HTTPException, Request

from api.auth import InternalAdminUser
from api.schemas.v2.admin_social_catalog_runs import (
    AdminSocialCatalogRecentRunsResponseV2,
    AdminSocialCatalogRunsProblemResponseV2,
)
from trr_backend.db.pg import database_service_unavailable_detail, is_database_service_unavailable_error
from trr_backend.problem import problem_http_exception
from trr_backend.socials.api.handlers import profile_reads

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/social/profiles", tags=["admin-social-catalog-runs-v2"])

_DEFAULT_LIMIT = 10
_MAX_LIMIT = 25
_ERROR_RESPONSES: dict[int | str, dict[str, Any]] = {
    400: {"model": AdminSocialCatalogRunsProblemResponseV2, "description": "The catalog-runs request is invalid."},
    500: {"model": AdminSocialCatalogRunsProblemResponseV2, "description": "The catalog-runs request failed."},
    503: {"model": AdminSocialCatalogRunsProblemResponseV2, "description": "The catalog-runs store is unavailable."},
}
_LIMIT_QUERY_PARAMETER = {
    "name": "limit",
    "in": "query",
    "required": False,
    "schema": {"type": "integer", "minimum": 1, "maximum": _MAX_LIMIT, "default": _DEFAULT_LIMIT},
}


def _problem(
    request: Request,
    *,
    code: str,
    status: int,
    message: str,
    retryable: bool = False,
) -> HTTPException:
    return problem_http_exception(
        request,
        code=code,
        status=status,
        message=message,
        retryable=retryable,
    )


def _parse_profile(request: Request) -> tuple[str, str]:
    platform = str(request.path_params.get("platform") or "").strip().lower()
    handle = str(request.path_params.get("handle") or "").strip().lower().lstrip("@")
    if platform != "instagram":
        raise _problem(
            request,
            code="UNSUPPORTED_CATALOG_PROFILE",
            status=400,
            message="Only Instagram catalog profiles are supported.",
        )
    if not handle:
        raise _problem(
            request,
            code="INVALID_CATALOG_HANDLE",
            status=400,
            message="handle must be non-empty after normalization.",
        )
    return platform, handle


def _parse_limit(request: Request) -> int:
    raw_limit = request.query_params.get("limit")
    if raw_limit is None:
        return _DEFAULT_LIMIT
    try:
        limit = int(raw_limit.strip())
    except (AttributeError, TypeError, ValueError) as error:
        raise _problem(
            request,
            code="INVALID_CATALOG_RECENT_RUNS_LIMIT",
            status=400,
            message="limit must be an integer.",
        ) from error
    return min(max(limit, 1), _MAX_LIMIT)


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


def _unexpected_problem(error: Exception, request: Request) -> HTTPException:
    if is_database_service_unavailable_error(error):
        return _database_problem(error, request)
    logger.exception("[admin-social-catalog-runs-v2] list recent catalog runs failed")
    return _problem(
        request,
        code="CATALOG_RECENT_RUNS_REQUEST_FAILED",
        status=500,
        message="The catalog recent-runs request could not be completed.",
    )


@router.get(
    "/{platform}/{handle}/catalog/runs/recent",
    response_model=AdminSocialCatalogRecentRunsResponseV2,
    response_model_exclude_none=True,
    operation_id="listAdminSocialCatalogRecentRunsV2",
    summary="List recent catalog runs for an Instagram account",
    responses=_ERROR_RESPONSES,
    openapi_extra={"parameters": [_LIMIT_QUERY_PARAMETER]},
)
def list_recent_catalog_runs(request: Request, _: InternalAdminUser) -> AdminSocialCatalogRecentRunsResponseV2:
    platform, handle = _parse_profile(request)
    limit = _parse_limit(request)
    try:
        payload = profile_reads.get_catalog_recent_runs(
            platform=platform,
            account_handle=handle,
            limit=limit,
        )
        return AdminSocialCatalogRecentRunsResponseV2.model_validate(payload)
    except HTTPException:
        raise
    except Exception as error:
        raise _unexpected_problem(error, request) from error
