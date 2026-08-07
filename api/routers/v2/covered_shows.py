"""Strict API v2 covered-shows admin endpoints."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, cast
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request
from pydantic import ValidationError

from api.auth import InternalAdminUser
from api.schemas.v2.covered_shows import (
    CoveredShowDeleteResponseV2,
    CoveredShowListResponseV2,
    CoveredShowProblemResponseV2,
    CoveredShowResponseV2,
    CreateCoveredShowV2,
)
from trr_backend.db.pg import database_service_unavailable_detail, is_database_service_unavailable_error
from trr_backend.problem import problem_http_exception
from trr_backend.services import covered_shows as covered_shows_service

if TYPE_CHECKING:
    from api.schemas.v2.covered_shows import CoveredShowV2

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/covered-shows", tags=["admin-covered-shows-v2"])

_COMMON_ERROR_RESPONSES: dict[int | str, dict[str, Any]] = {
    400: {"model": CoveredShowProblemResponseV2, "description": "The covered-show request is invalid."},
    500: {"model": CoveredShowProblemResponseV2, "description": "The covered-show request failed."},
    503: {"model": CoveredShowProblemResponseV2, "description": "The covered-show store is unavailable."},
}
_DETAIL_ERROR_RESPONSES = {
    **_COMMON_ERROR_RESPONSES,
    404: {"model": CoveredShowProblemResponseV2, "description": "The show is not covered."},
}
_CREATE_REQUEST_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["trr_show_id", "show_name"],
    "properties": {
        "trr_show_id": {"type": "string", "format": "uuid"},
        "show_name": {"type": "string", "minLength": 1},
    },
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


def _unexpected_problem(error: Exception, request: Request, *, operation: str) -> HTTPException:
    if is_database_service_unavailable_error(error):
        return _database_problem(error, request)
    logger.exception("[admin-covered-shows-v2] %s failed", operation)
    return _problem(
        request,
        code="COVERED_SHOWS_REQUEST_FAILED",
        status=500,
        message="The covered-shows request could not be completed.",
    )


def _parse_show_id(raw_show_id: object, request: Request) -> str:
    try:
        return str(UUID(str(raw_show_id or "").strip()))
    except (TypeError, ValueError, AttributeError) as error:
        raise _problem(
            request,
            code="INVALID_COVERED_SHOW_ID",
            status=400,
            message="show_id must be a valid UUID.",
        ) from error


async def _parse_create_request(request: Request) -> CreateCoveredShowV2:
    try:
        payload = await request.json()
        return CreateCoveredShowV2.model_validate(payload)
    except (ValidationError, ValueError, TypeError) as error:
        raise _problem(
            request,
            code="INVALID_COVERED_SHOW_REQUEST",
            status=400,
            message="trr_show_id and show_name are required, and no extra fields are allowed.",
        ) from error


def _actor_uid(admin: dict[str, Any], request: Request) -> str:
    actor_uid = str(admin.get("admin_uid") or admin.get("id") or "").strip()
    if actor_uid:
        return actor_uid
    raise _problem(
        request,
        code="ADMIN_ACTOR_MISSING",
        status=403,
        message="The verified admin identity does not contain an actor UID.",
    )


@router.get(
    "",
    response_model=CoveredShowListResponseV2,
    operation_id="listAdminCoveredShowsV2",
    responses=_COMMON_ERROR_RESPONSES,
)
def list_covered_shows(request: Request, _: InternalAdminUser) -> CoveredShowListResponseV2:
    try:
        payload, _query_count, _cache_status = covered_shows_service.list_covered_shows()
        return CoveredShowListResponseV2.model_validate(payload)
    except Exception as error:
        raise _unexpected_problem(error, request, operation="list") from error


@router.post(
    "",
    status_code=201,
    response_model=CoveredShowResponseV2,
    operation_id="createAdminCoveredShowV2",
    responses=_COMMON_ERROR_RESPONSES,
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {"application/json": {"schema": _CREATE_REQUEST_SCHEMA}},
        }
    },
)
async def create_covered_show(request: Request, admin: InternalAdminUser) -> CoveredShowResponseV2:
    body = await _parse_create_request(request)
    try:
        show, _query_count = covered_shows_service.add_covered_show(
            show_id=str(body.trr_show_id),
            show_name=body.show_name,
            actor_uid=_actor_uid(admin, request),
        )
        return CoveredShowResponseV2(show=cast("CoveredShowV2", show))
    except HTTPException:
        raise
    except Exception as error:
        raise _unexpected_problem(error, request, operation="create") from error


@router.get(
    "/{show_id}",
    response_model=CoveredShowResponseV2,
    operation_id="getAdminCoveredShowV2",
    responses=_DETAIL_ERROR_RESPONSES,
)
def get_covered_show(request: Request, _: InternalAdminUser) -> CoveredShowResponseV2:
    show_id = _parse_show_id(request.path_params.get("show_id"), request)
    try:
        show, _query_count, _cache_status = covered_shows_service.get_covered_show(show_id)
    except Exception as error:
        raise _unexpected_problem(error, request, operation="detail") from error
    if show is None:
        raise _problem(
            request,
            code="COVERED_SHOW_NOT_FOUND",
            status=404,
            message="Show not found in covered shows list.",
        )
    try:
        return CoveredShowResponseV2(show=cast("CoveredShowV2", show))
    except Exception as error:
        raise _unexpected_problem(error, request, operation="detail-response") from error


@router.delete(
    "/{show_id}",
    response_model=CoveredShowDeleteResponseV2,
    operation_id="deleteAdminCoveredShowV2",
    responses=_DETAIL_ERROR_RESPONSES,
)
def delete_covered_show(request: Request, _: InternalAdminUser) -> CoveredShowDeleteResponseV2:
    show_id = _parse_show_id(request.path_params.get("show_id"), request)
    try:
        deleted, _query_count = covered_shows_service.remove_covered_show(show_id)
    except Exception as error:
        raise _unexpected_problem(error, request, operation="delete") from error
    if not deleted:
        raise _problem(
            request,
            code="COVERED_SHOW_NOT_FOUND",
            status=404,
            message="Show not found in covered shows list.",
        )
    return CoveredShowDeleteResponseV2(success=True)
