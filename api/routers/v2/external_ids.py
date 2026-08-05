"""Strict API v2 admin external-ID read endpoints."""

from __future__ import annotations

import logging
from typing import Any
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request
from pydantic import ValidationError

from api.auth import InternalAdminUser
from api.schemas.v2.external_ids import (
    MAX_EXTERNAL_ID_BATCH_SIZE,
    ExternalIdsProblemResponseV2,
    PersonExternalIdsBatchRequestV2,
    PersonExternalIdsBatchResponseV2,
    PersonExternalIdsResponseV2,
    ShowExternalIdsBatchRequestV2,
    ShowExternalIdsBatchResponseV2,
)
from trr_backend.db.pg import database_service_unavailable_detail, is_database_service_unavailable_error
from trr_backend.problem import problem_http_exception
from trr_backend.repositories import external_id_reads

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin", tags=["admin-external-ids-v2"])

_COMMON_ERROR_RESPONSES: dict[int | str, dict[str, Any]] = {
    400: {"model": ExternalIdsProblemResponseV2, "description": "The external-ID request is invalid."},
    500: {"model": ExternalIdsProblemResponseV2, "description": "The external-ID request failed."},
    503: {"model": ExternalIdsProblemResponseV2, "description": "The external-ID store is unavailable."},
}
_DETAIL_ERROR_RESPONSES = {
    **_COMMON_ERROR_RESPONSES,
    404: {"model": ExternalIdsProblemResponseV2, "description": "The person was not found."},
}
_PERSON_ID_PATH_PARAMETER = {
    "name": "person_id",
    "in": "path",
    "required": True,
    "schema": {"type": "string", "format": "uuid"},
}
_PERSON_DETAIL_QUERY_PARAMETER = {
    "name": "include_inactive",
    "in": "query",
    "required": False,
    "schema": {"type": "boolean", "default": False},
}
_PERSON_BATCH_REQUEST_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["person_ids"],
    "properties": {
        "person_ids": {
            "type": "array",
            "items": {"type": "string", "format": "uuid"},
            "minItems": 1,
            "maxItems": MAX_EXTERNAL_ID_BATCH_SIZE,
        },
        "include_inactive": {"type": "boolean", "default": False},
    },
}
_SHOW_BATCH_REQUEST_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["show_ids"],
    "properties": {
        "show_ids": {
            "type": "array",
            "items": {"type": "string", "format": "uuid"},
            "minItems": 1,
            "maxItems": MAX_EXTERNAL_ID_BATCH_SIZE,
        }
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
    logger.exception("[admin-external-ids-v2] %s failed", operation)
    return _problem(
        request,
        code="EXTERNAL_IDS_REQUEST_FAILED",
        status=500,
        message="The external-ID request could not be completed.",
    )


def _parse_uuid(raw_value: object, request: Request, *, field_name: str) -> str:
    try:
        return str(UUID(str(raw_value or "").strip()))
    except (TypeError, ValueError, AttributeError) as error:
        raise _problem(
            request,
            code=f"INVALID_{field_name.upper()}",
            status=400,
            message=f"{field_name} must be a valid UUID.",
        ) from error


def _parse_include_inactive(request: Request) -> bool:
    raw_value = request.query_params.get("include_inactive")
    if raw_value is None:
        return False
    normalized = raw_value.strip().lower()
    if normalized == "true":
        return True
    if normalized == "false":
        return False
    raise _problem(
        request,
        code="INVALID_INCLUDE_INACTIVE",
        status=400,
        message="include_inactive must be true or false.",
    )


async def _parse_person_batch_request(request: Request) -> PersonExternalIdsBatchRequestV2:
    try:
        return PersonExternalIdsBatchRequestV2.model_validate(await request.json())
    except (ValidationError, ValueError, TypeError) as error:
        raise _problem(
            request,
            code="INVALID_PERSON_EXTERNAL_IDS_BATCH_REQUEST",
            status=400,
            message=(
                f"person_ids must contain 1 to {MAX_EXTERNAL_ID_BATCH_SIZE} UUIDs, "
                "include_inactive must be a boolean, and no extra fields are allowed."
            ),
        ) from error


async def _parse_show_batch_request(request: Request) -> ShowExternalIdsBatchRequestV2:
    try:
        return ShowExternalIdsBatchRequestV2.model_validate(await request.json())
    except (ValidationError, ValueError, TypeError) as error:
        raise _problem(
            request,
            code="INVALID_SHOW_EXTERNAL_IDS_BATCH_REQUEST",
            status=400,
            message=(f"show_ids must contain 1 to {MAX_EXTERNAL_ID_BATCH_SIZE} UUIDs and no extra fields are allowed."),
        ) from error


@router.get(
    "/people/{person_id}/external-ids",
    response_model=PersonExternalIdsResponseV2,
    operation_id="getAdminPersonExternalIdsV2",
    summary="List a person's primary external IDs",
    responses=_DETAIL_ERROR_RESPONSES,
    openapi_extra={"parameters": [_PERSON_ID_PATH_PARAMETER, _PERSON_DETAIL_QUERY_PARAMETER]},
)
def get_person_external_ids(request: Request, _: InternalAdminUser) -> PersonExternalIdsResponseV2:
    person_id = _parse_uuid(request.path_params.get("person_id"), request, field_name="person_id")
    include_inactive = _parse_include_inactive(request)
    try:
        external_ids, _query_count = external_id_reads.get_person_external_ids(
            person_id,
            include_inactive=include_inactive,
        )
    except Exception as error:
        raise _unexpected_problem(error, request, operation="person-detail") from error
    if external_ids is None:
        raise _problem(
            request,
            code="PERSON_NOT_FOUND",
            status=404,
            message="Person not found",
        )
    try:
        return PersonExternalIdsResponseV2.model_validate({"person_id": person_id, "external_ids": external_ids})
    except Exception as error:
        raise _unexpected_problem(error, request, operation="person-detail-response") from error


@router.post(
    "/people/external-ids/batch",
    response_model=PersonExternalIdsBatchResponseV2,
    operation_id="listAdminPersonExternalIdsBatchV2",
    summary="List primary external IDs for a bounded person batch",
    responses=_COMMON_ERROR_RESPONSES,
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {"application/json": {"schema": _PERSON_BATCH_REQUEST_SCHEMA}},
        }
    },
)
async def list_person_external_ids_batch(
    request: Request,
    _: InternalAdminUser,
) -> PersonExternalIdsBatchResponseV2:
    body = await _parse_person_batch_request(request)
    try:
        people, _query_count = external_id_reads.list_person_external_ids_by_person_ids(
            [str(person_id) for person_id in body.person_ids],
            include_inactive=body.include_inactive,
        )
        return PersonExternalIdsBatchResponseV2.model_validate({"people": people})
    except Exception as error:
        raise _unexpected_problem(error, request, operation="person-batch") from error


@router.post(
    "/shows/external-ids/batch",
    response_model=ShowExternalIdsBatchResponseV2,
    operation_id="listAdminShowExternalIdsBatchV2",
    summary="List canonical external IDs for a bounded show batch",
    responses=_COMMON_ERROR_RESPONSES,
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {"application/json": {"schema": _SHOW_BATCH_REQUEST_SCHEMA}},
        }
    },
)
async def list_show_external_ids_batch(
    request: Request,
    _: InternalAdminUser,
) -> ShowExternalIdsBatchResponseV2:
    body = await _parse_show_batch_request(request)
    try:
        shows, _query_count = external_id_reads.list_show_external_ids_by_show_ids(
            [str(show_id) for show_id in body.show_ids]
        )
        return ShowExternalIdsBatchResponseV2.model_validate({"shows": shows})
    except Exception as error:
        raise _unexpected_problem(error, request, operation="show-batch") from error
