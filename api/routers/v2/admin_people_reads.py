"""Strict API v2 admin endpoints for core people reads."""

from __future__ import annotations

import logging
from typing import Any
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request

from api.auth import InternalAdminUser
from api.schemas.v2.admin_people_reads import (
    AdminPeopleListResponseV2,
    AdminPeopleReadProblemResponseV2,
    AdminPersonRelationshipsResponseV2,
    AdminPersonResponseV2,
)
from trr_backend.db.pg import database_service_unavailable_detail, is_database_service_unavailable_error
from trr_backend.problem import problem_http_exception
from trr_backend.services import core_people_reads as people_reads_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/people", tags=["admin-core-people-reads-v2"])

_DEFAULT_LIMIT = 20
_MAX_LIMIT = 500
_MAX_QUERY_LENGTH = 200
_COMMON_ERROR_RESPONSES: dict[int | str, dict[str, Any]] = {
    400: {"model": AdminPeopleReadProblemResponseV2, "description": "The people read request is invalid."},
    500: {"model": AdminPeopleReadProblemResponseV2, "description": "The people read request failed."},
    503: {"model": AdminPeopleReadProblemResponseV2, "description": "The people store is unavailable."},
}
_DETAIL_ERROR_RESPONSES: dict[int | str, dict[str, Any]] = {
    **_COMMON_ERROR_RESPONSES,
    404: {"model": AdminPeopleReadProblemResponseV2, "description": "The requested person was not found."},
}
_PAGINATION_QUERY_PARAMETERS = [
    {
        "name": "limit",
        "in": "query",
        "required": False,
        "schema": {"type": "integer", "minimum": 1, "maximum": _MAX_LIMIT, "default": _DEFAULT_LIMIT},
    },
    {
        "name": "offset",
        "in": "query",
        "required": False,
        "schema": {"type": "integer", "minimum": 0, "default": 0},
    },
]
_SEARCH_QUERY_PARAMETER = {
    "name": "q",
    "in": "query",
    "required": False,
    "schema": {"type": "string", "minLength": 1, "maxLength": _MAX_QUERY_LENGTH},
}
_PERSON_ID_PATH_PARAMETER = {
    "name": "person_id",
    "in": "path",
    "required": True,
    "schema": {"type": "string", "format": "uuid"},
}
_SHOW_ID_QUERY_PARAMETER = {
    "name": "show_id",
    "in": "query",
    "required": False,
    "schema": {"type": "string", "format": "uuid"},
}
_PERSON_SUMMARY_FIELDS = frozenset({"id", "full_name", "known_for", "external_ids", "created_at", "updated_at"})
_PERSON_DETAIL_FIELDS = frozenset(
    {
        *_PERSON_SUMMARY_FIELDS,
        "birthday",
        "gender",
        "biography",
        "place_of_birth",
        "homepage",
        "profile_image_url",
        "alternative_names",
    }
)


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
    if isinstance(error, HTTPException):
        return error
    if is_database_service_unavailable_error(error):
        return _database_problem(error, request)
    logger.exception("[admin-core-people-reads-v2] %s failed", operation)
    return _problem(
        request,
        code="PEOPLE_READ_REQUEST_FAILED",
        status=500,
        message="The people read request could not be completed.",
    )


def _parse_uuid(raw_value: object, request: Request, *, field_name: str, code: str) -> str:
    try:
        return str(UUID(str(raw_value or "").strip()))
    except (TypeError, ValueError, AttributeError) as error:
        raise _problem(
            request,
            code=code,
            status=400,
            message=f"{field_name} must be a valid UUID.",
        ) from error


def _parse_limit_offset(request: Request) -> tuple[int, int]:
    raw_limit = request.query_params.get("limit", str(_DEFAULT_LIMIT))
    raw_offset = request.query_params.get("offset", "0")
    try:
        limit = int(str(raw_limit).strip())
        offset = int(str(raw_offset).strip())
    except (TypeError, ValueError, AttributeError) as error:
        raise _problem(
            request,
            code="INVALID_PAGINATION",
            status=400,
            message=f"limit must be 1-{_MAX_LIMIT}; offset must be at least 0.",
        ) from error
    if limit < 1 or limit > _MAX_LIMIT or offset < 0:
        raise _problem(
            request,
            code="INVALID_PAGINATION",
            status=400,
            message=f"limit must be 1-{_MAX_LIMIT}; offset must be at least 0.",
        )
    return limit, offset


def _parse_query(request: Request) -> str | None:
    if "q" not in request.query_params:
        return None
    query = str(request.query_params.get("q") or "").strip()
    if not query or len(query) > _MAX_QUERY_LENGTH:
        raise _problem(
            request,
            code="INVALID_SEARCH_QUERY",
            status=400,
            message=f"q must be 1-{_MAX_QUERY_LENGTH} characters when provided.",
        )
    return query


def _parse_optional_show_id(request: Request) -> str | None:
    if "show_id" not in request.query_params:
        return None
    return _parse_uuid(
        request.query_params.get("show_id"),
        request,
        field_name="show_id",
        code="INVALID_SHOW_ID",
    )


def _pick_fields(row: dict[str, Any], fields: frozenset[str]) -> dict[str, Any]:
    return {key: value for key, value in row.items() if key in fields}


def _validate_payload(model: type[Any], payload: Any, request: Request, *, operation: str) -> Any:
    try:
        return model.model_validate(payload)
    except Exception as error:
        raise _unexpected_problem(error, request, operation=f"{operation}-response") from error


@router.get(
    "",
    response_model=AdminPeopleListResponseV2,
    operation_id="listAdminPeopleV2",
    summary="List or prefix-search core people",
    responses=_COMMON_ERROR_RESPONSES,
    openapi_extra={"parameters": [_SEARCH_QUERY_PARAMETER, *_PAGINATION_QUERY_PARAMETERS]},
)
def list_people(request: Request, _: InternalAdminUser) -> AdminPeopleListResponseV2:
    query = _parse_query(request)
    limit, offset = _parse_limit_offset(request)
    try:
        rows, _query_count = people_reads_service.search_people(query or "", limit=limit, offset=offset)
        payload = {
            "people": [_pick_fields(row, _PERSON_SUMMARY_FIELDS) for row in rows],
            "limit": limit,
            "offset": offset,
            "count": len(rows),
            "total_count": None,
            "has_more": len(rows) >= limit,
        }
        return _validate_payload(AdminPeopleListResponseV2, payload, request, operation="list-people")
    except Exception as error:
        raise _unexpected_problem(error, request, operation="list-people") from error


@router.get(
    "/{person_id}",
    response_model=AdminPersonResponseV2,
    operation_id="getAdminPersonV2",
    summary="Get a core person",
    responses=_DETAIL_ERROR_RESPONSES,
    openapi_extra={"parameters": [_PERSON_ID_PATH_PARAMETER]},
)
def get_person(request: Request, _: InternalAdminUser) -> AdminPersonResponseV2:
    person_id = _parse_uuid(
        request.path_params.get("person_id"),
        request,
        field_name="person_id",
        code="INVALID_PERSON_ID",
    )
    try:
        person, _query_count = people_reads_service.get_person_by_id(person_id)
        if person is None:
            raise _problem(
                request,
                code="PERSON_NOT_FOUND",
                status=404,
                message="Person not found.",
            )
        payload = {"person": _pick_fields(person, _PERSON_DETAIL_FIELDS)}
        return _validate_payload(AdminPersonResponseV2, payload, request, operation="get-person")
    except Exception as error:
        raise _unexpected_problem(error, request, operation="get-person") from error


@router.get(
    "/{person_id}/relationships",
    response_model=AdminPersonRelationshipsResponseV2,
    operation_id="getAdminPersonRelationshipsV2",
    summary="Get deduced family relationships for a core person",
    responses=_COMMON_ERROR_RESPONSES,
    openapi_extra={"parameters": [_PERSON_ID_PATH_PARAMETER, _SHOW_ID_QUERY_PARAMETER]},
)
def get_person_relationships(
    request: Request,
    _: InternalAdminUser,
) -> AdminPersonRelationshipsResponseV2:
    person_id = _parse_uuid(
        request.path_params.get("person_id"),
        request,
        field_name="person_id",
        code="INVALID_PERSON_ID",
    )
    show_id = _parse_optional_show_id(request)
    try:
        relationships, _query_count = people_reads_service.get_deduced_family_relationships_by_person_id(
            person_id,
            show_id=show_id,
        )
        payload = {
            "person_id": person_id,
            "show_id": show_id,
            "relationships": relationships,
        }
        return _validate_payload(
            AdminPersonRelationshipsResponseV2,
            payload,
            request,
            operation="get-person-relationships",
        )
    except Exception as error:
        raise _unexpected_problem(error, request, operation="get-person-relationships") from error
