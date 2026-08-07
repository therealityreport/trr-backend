"""Strict API v2 endpoints for authenticated typography administration."""

from __future__ import annotations

import logging
from typing import Any, cast

from fastapi import APIRouter, HTTPException, Request
from pydantic import ValidationError

from api.auth import InternalAdminUser
from api.schemas.v2.admin_typography import (
    AdminTypographyProblemResponseV2,
    AdminTypographyStateResponseV2,
    CreateTypographySetRequestV2,
    TypographyAssignmentResponseV2,
    TypographyDeleteResponseV2,
    TypographySetResponseV2,
    UpdateTypographySetRequestV2,
    UpsertTypographyAssignmentRequestV2,
)
from trr_backend.db.pg import database_service_unavailable_detail, is_database_service_unavailable_error
from trr_backend.problem import problem_http_exception
from trr_backend.repositories import admin_typography as typography_repo

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/site-typography", tags=["admin-site-typography-v2"])

_COMMON_ERROR_RESPONSES: dict[int | str, dict[str, Any]] = {
    400: {"model": AdminTypographyProblemResponseV2, "description": "The typography request is invalid."},
    500: {"model": AdminTypographyProblemResponseV2, "description": "The typography request failed."},
    503: {"model": AdminTypographyProblemResponseV2, "description": "The typography store is unavailable."},
}
_SET_ERROR_RESPONSES: dict[int | str, dict[str, Any]] = {
    **_COMMON_ERROR_RESPONSES,
    404: {"model": AdminTypographyProblemResponseV2, "description": "The typography set was not found."},
    409: {"model": AdminTypographyProblemResponseV2, "description": "The typography set is still assigned."},
}
_SET_ID_PARAMETER = {
    "name": "set_id",
    "in": "path",
    "required": True,
    "schema": {"type": "string", "minLength": 1},
}
_CREATE_SET_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["name", "area", "seed_source", "roles"],
    "properties": {
        "slug": {"type": ["string", "null"]},
        "name": {"type": "string", "minLength": 1},
        "area": {"type": "string", "enum": ["user-frontend", "surveys", "admin"]},
        "seed_source": {"type": "string", "minLength": 1},
        "roles": {"type": "object", "minProperties": 1},
    },
}
_UPDATE_SET_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "name": {"type": "string", "minLength": 1},
        "area": {"type": "string", "enum": ["user-frontend", "surveys", "admin"]},
        "seed_source": {"type": "string", "minLength": 1},
        "roles": {"type": "object", "minProperties": 1},
    },
}
_UPSERT_ASSIGNMENT_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["area", "set_id", "source_path"],
    "properties": {
        "area": {"type": "string", "enum": ["user-frontend", "surveys", "admin"]},
        "page_key": {"type": ["string", "null"]},
        "instance_key": {"type": ["string", "null"]},
        "set_id": {"type": "string", "minLength": 1},
        "source_path": {"type": "string", "minLength": 1},
        "notes": {"type": ["string", "null"]},
    },
}


def _problem(request: Request, *, code: str, status: int, message: str) -> HTTPException:
    return problem_http_exception(
        request,
        code=code,
        status=status,
        message=message,
        retryable=False,
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
    logger.exception("[admin-typography-v2] %s failed", operation)
    return _problem(
        request,
        code="TYPOGRAPHY_ADMIN_REQUEST_FAILED",
        status=500,
        message="The typography administration request could not be completed.",
    )


def _parse_set_id(request: Request) -> str:
    value = str(request.path_params.get("set_id") or "").strip()
    if value:
        return value
    raise _problem(
        request,
        code="INVALID_TYPOGRAPHY_SET_ID",
        status=400,
        message="set_id is required.",
    )


async def _parse_body(request: Request, model: type[Any], *, code: str, message: str) -> Any:
    try:
        return model.model_validate(await request.json())
    except (ValidationError, ValueError, TypeError) as error:
        raise _problem(request, code=code, status=400, message=message) from error


def _roles_payload(roles: dict[str, Any]) -> dict[str, Any]:
    return {key: value.model_dump(by_alias=True, exclude_none=True) for key, value in roles.items()}


@router.get(
    "",
    response_model=AdminTypographyStateResponseV2,
    operation_id="getAdminSiteTypographyV2",
    responses=_COMMON_ERROR_RESPONSES,
)
def get_typography_state(request: Request, _: InternalAdminUser) -> AdminTypographyStateResponseV2:
    try:
        state, _query_count = typography_repo.read_typography_state()
        return AdminTypographyStateResponseV2.model_validate(state)
    except Exception as error:
        raise _unexpected_problem(error, request, operation="get-state") from error


@router.post(
    "/sets",
    status_code=201,
    response_model=TypographySetResponseV2,
    operation_id="createAdminTypographySetV2",
    responses=_COMMON_ERROR_RESPONSES,
    openapi_extra={"requestBody": {"required": True, "content": {"application/json": {"schema": _CREATE_SET_SCHEMA}}}},
)
async def create_typography_set(request: Request, _: InternalAdminUser) -> TypographySetResponseV2:
    body = await _parse_body(
        request,
        CreateTypographySetRequestV2,
        code="INVALID_TYPOGRAPHY_SET_REQUEST",
        message="name, area, seed_source, and at least one complete role are required, with no extra fields.",
    )
    try:
        typography_set, _query_count = typography_repo.create_typography_set(
            slug=body.slug,
            name=body.name,
            area=body.area,
            seed_source=body.seed_source,
            roles=_roles_payload(body.roles),
        )
        return TypographySetResponseV2.model_validate({"set": typography_set})
    except Exception as error:
        raise _unexpected_problem(error, request, operation="create-set") from error


@router.put(
    "/sets/{set_id}",
    response_model=TypographySetResponseV2,
    operation_id="updateAdminTypographySetV2",
    responses=_SET_ERROR_RESPONSES,
    openapi_extra={
        "parameters": [_SET_ID_PARAMETER],
        "requestBody": {"required": True, "content": {"application/json": {"schema": _UPDATE_SET_SCHEMA}}},
    },
)
async def update_typography_set(request: Request, _: InternalAdminUser) -> TypographySetResponseV2:
    set_id = _parse_set_id(request)
    body = await _parse_body(
        request,
        UpdateTypographySetRequestV2,
        code="INVALID_TYPOGRAPHY_SET_UPDATE_REQUEST",
        message="Typography set updates must contain valid fields and no extra fields.",
    )
    try:
        updated, _query_count = typography_repo.update_typography_set(
            set_id,
            **({"name": body.name} if "name" in body.model_fields_set else {}),
            **({"area": body.area} if "area" in body.model_fields_set else {}),
            **({"seed_source": body.seed_source} if "seed_source" in body.model_fields_set else {}),
            **cast(
                "dict[str, Any]",
                {"roles": _roles_payload(body.roles)} if "roles" in body.model_fields_set else {},
            ),
        )
        if updated is None:
            raise _problem(
                request,
                code="TYPOGRAPHY_SET_NOT_FOUND",
                status=404,
                message="Typography set not found.",
            )
        return TypographySetResponseV2.model_validate({"set": updated})
    except Exception as error:
        raise _unexpected_problem(error, request, operation="update-set") from error


@router.delete(
    "/sets/{set_id}",
    response_model=TypographyDeleteResponseV2,
    operation_id="deleteAdminTypographySetV2",
    responses=_SET_ERROR_RESPONSES,
    openapi_extra={"parameters": [_SET_ID_PARAMETER]},
)
def delete_typography_set(request: Request, _: InternalAdminUser) -> TypographyDeleteResponseV2:
    set_id = _parse_set_id(request)
    try:
        outcome, _query_count = typography_repo.delete_typography_set(set_id)
        if outcome == "missing":
            raise _problem(
                request,
                code="TYPOGRAPHY_SET_NOT_FOUND",
                status=404,
                message="Typography set not found.",
            )
        if outcome == "in-use":
            raise _problem(
                request,
                code="TYPOGRAPHY_SET_IN_USE",
                status=409,
                message="Typography set is still assigned.",
            )
        return TypographyDeleteResponseV2(ok=True)
    except Exception as error:
        raise _unexpected_problem(error, request, operation="delete-set") from error


@router.put(
    "/assignments",
    response_model=TypographyAssignmentResponseV2,
    operation_id="upsertAdminTypographyAssignmentV2",
    responses=_COMMON_ERROR_RESPONSES,
    openapi_extra={
        "requestBody": {"required": True, "content": {"application/json": {"schema": _UPSERT_ASSIGNMENT_SCHEMA}}}
    },
)
async def upsert_typography_assignment(request: Request, _: InternalAdminUser) -> TypographyAssignmentResponseV2:
    body = await _parse_body(
        request,
        UpsertTypographyAssignmentRequestV2,
        code="INVALID_TYPOGRAPHY_ASSIGNMENT_REQUEST",
        message="area, set_id, source_path, and valid optional scope fields are required, with no extra fields.",
    )
    try:
        assignment, _query_count = typography_repo.upsert_typography_assignment(
            area=body.area,
            page_key=body.page_key,
            instance_key=body.instance_key,
            set_id=body.set_id,
            source_path=body.source_path,
            notes=body.notes,
        )
        return TypographyAssignmentResponseV2.model_validate({"assignment": assignment})
    except Exception as error:
        raise _unexpected_problem(error, request, operation="upsert-assignment") from error
