"""Strict API v2 admin endpoints for season cast survey roles."""

from __future__ import annotations

import logging
from typing import Any
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request
from pydantic import ValidationError

from api.auth import InternalAdminUser
from api.schemas.v2.season_cast_survey_roles import (
    DeleteSeasonCastSurveyRoleRequestV2,
    DeleteSeasonCastSurveyRoleResponseV2,
    ReplaceSeasonCastSurveyRolesRequestV2,
    SeasonCastSurveyRoleListResponseV2,
    SeasonCastSurveyRoleProblemResponseV2,
    UpsertSeasonCastSurveyRoleRequestV2,
    UpsertSeasonCastSurveyRoleResponseV2,
)
from trr_backend.db.pg import database_service_unavailable_detail, is_database_service_unavailable_error
from trr_backend.problem import problem_http_exception
from trr_backend.services import season_cast_survey_roles as roles_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/shows", tags=["admin-season-cast-survey-roles-v2"])

_ERROR_RESPONSES: dict[int | str, dict[str, Any]] = {
    400: {"model": SeasonCastSurveyRoleProblemResponseV2, "description": "The role request is invalid."},
    500: {"model": SeasonCastSurveyRoleProblemResponseV2, "description": "The role request failed."},
    503: {"model": SeasonCastSurveyRoleProblemResponseV2, "description": "The role store is unavailable."},
}
_SHOW_ID_PATH_PARAMETER = {
    "name": "show_id",
    "in": "path",
    "required": True,
    "schema": {"type": "string", "format": "uuid"},
}
_SEASON_NUMBER_PATH_PARAMETER = {
    "name": "season_number",
    "in": "path",
    "required": True,
    "schema": {"type": "integer"},
}
_ROLE_ENTRY_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["person_id", "role"],
    "properties": {
        "person_id": {"type": "string", "format": "uuid"},
        "role": {"type": "string", "enum": ["main", "friend_of"]},
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
    if is_database_service_unavailable_error(error):
        return _database_problem(error, request)
    logger.exception("[admin-season-cast-survey-roles-v2] %s failed", operation)
    return _problem(
        request,
        code="SEASON_CAST_SURVEY_ROLE_REQUEST_FAILED",
        status=500,
        message="The season cast survey-role request could not be completed.",
    )


def _parse_scope(request: Request) -> tuple[str, int]:
    try:
        show_id = str(UUID(str(request.path_params.get("show_id") or "").strip()))
    except (TypeError, ValueError, AttributeError) as error:
        raise _problem(
            request,
            code="INVALID_SHOW_ID",
            status=400,
            message="show_id must be a valid UUID.",
        ) from error
    raw_season_number = request.path_params.get("season_number")
    try:
        season_number = int(str(raw_season_number).strip())
    except (TypeError, ValueError, AttributeError) as error:
        raise _problem(
            request,
            code="INVALID_SEASON_NUMBER",
            status=400,
            message="season_number must be an integer.",
        ) from error
    return show_id, season_number


async def _parse_body(request: Request, model: type[Any], *, code: str, message: str) -> Any:
    try:
        return model.model_validate(await request.json())
    except (ValidationError, ValueError, TypeError) as error:
        raise _problem(request, code=code, status=400, message=message) from error


@router.get(
    "/{show_id}/seasons/{season_number}/cast-survey-roles",
    response_model=SeasonCastSurveyRoleListResponseV2,
    operation_id="listAdminSeasonCastSurveyRolesV2",
    responses=_ERROR_RESPONSES,
    openapi_extra={"parameters": [_SHOW_ID_PATH_PARAMETER, _SEASON_NUMBER_PATH_PARAMETER]},
)
def list_roles(request: Request, _: InternalAdminUser) -> SeasonCastSurveyRoleListResponseV2:
    show_id, season_number = _parse_scope(request)
    try:
        roles, _query_count = roles_service.list_roles(show_id=show_id, season_number=season_number)
        return SeasonCastSurveyRoleListResponseV2.model_validate({"roles": roles})
    except HTTPException:
        raise
    except Exception as error:
        raise _unexpected_problem(error, request, operation="list") from error


@router.post(
    "/{show_id}/seasons/{season_number}/cast-survey-roles",
    response_model=UpsertSeasonCastSurveyRoleResponseV2,
    operation_id="upsertAdminSeasonCastSurveyRoleV2",
    responses=_ERROR_RESPONSES,
    openapi_extra={
        "parameters": [_SHOW_ID_PATH_PARAMETER, _SEASON_NUMBER_PATH_PARAMETER],
        "requestBody": {
            "required": True,
            "content": {"application/json": {"schema": _ROLE_ENTRY_SCHEMA}},
        },
    },
)
async def upsert_role(
    request: Request,
    _: InternalAdminUser,
) -> UpsertSeasonCastSurveyRoleResponseV2:
    show_id, season_number = _parse_scope(request)
    body = await _parse_body(
        request,
        UpsertSeasonCastSurveyRoleRequestV2,
        code="INVALID_SEASON_CAST_SURVEY_ROLE_REQUEST",
        message="person_id and role are required, with no extra fields.",
    )
    try:
        role, _query_count = roles_service.upsert_role(
            show_id=show_id,
            season_number=season_number,
            person_id=str(body.person_id),
            role=body.role,
        )
        return UpsertSeasonCastSurveyRoleResponseV2.model_validate({"role": role})
    except HTTPException:
        raise
    except Exception as error:
        raise _unexpected_problem(error, request, operation="upsert") from error


@router.patch(
    "/{show_id}/seasons/{season_number}/cast-survey-roles",
    response_model=SeasonCastSurveyRoleListResponseV2,
    operation_id="replaceAdminSeasonCastSurveyRolesV2",
    responses=_ERROR_RESPONSES,
    openapi_extra={
        "parameters": [_SHOW_ID_PATH_PARAMETER, _SEASON_NUMBER_PATH_PARAMETER],
        "requestBody": {
            "required": True,
            "content": {
                "application/json": {
                    "schema": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["roles"],
                        "properties": {
                            "roles": {
                                "type": "array",
                                "maxItems": 500,
                                "items": _ROLE_ENTRY_SCHEMA,
                            }
                        },
                    }
                }
            },
        },
    },
)
async def replace_roles(
    request: Request,
    _: InternalAdminUser,
) -> SeasonCastSurveyRoleListResponseV2:
    show_id, season_number = _parse_scope(request)
    body = await _parse_body(
        request,
        ReplaceSeasonCastSurveyRolesRequestV2,
        code="INVALID_SEASON_CAST_SURVEY_ROLES_REQUEST",
        message="roles must be an array of at most 500 valid person-role entries.",
    )
    try:
        roles, _query_count = roles_service.replace_roles(
            show_id=show_id,
            season_number=season_number,
            roles=[(str(entry.person_id), entry.role) for entry in body.roles],
        )
        return SeasonCastSurveyRoleListResponseV2.model_validate({"roles": roles})
    except HTTPException:
        raise
    except Exception as error:
        raise _unexpected_problem(error, request, operation="replace") from error


@router.delete(
    "/{show_id}/seasons/{season_number}/cast-survey-roles",
    response_model=DeleteSeasonCastSurveyRoleResponseV2,
    operation_id="deleteAdminSeasonCastSurveyRoleV2",
    responses=_ERROR_RESPONSES,
    openapi_extra={
        "parameters": [_SHOW_ID_PATH_PARAMETER, _SEASON_NUMBER_PATH_PARAMETER],
        "requestBody": {
            "required": True,
            "content": {
                "application/json": {
                    "schema": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["person_id"],
                        "properties": {"person_id": {"type": "string", "format": "uuid"}},
                    }
                }
            },
        },
    },
)
async def delete_role(
    request: Request,
    _: InternalAdminUser,
) -> DeleteSeasonCastSurveyRoleResponseV2:
    show_id, season_number = _parse_scope(request)
    body = await _parse_body(
        request,
        DeleteSeasonCastSurveyRoleRequestV2,
        code="INVALID_SEASON_CAST_SURVEY_ROLE_DELETE_REQUEST",
        message="person_id is required, with no extra fields.",
    )
    try:
        removed, _query_count = roles_service.delete_role(
            show_id=show_id,
            season_number=season_number,
            person_id=str(body.person_id),
        )
        return DeleteSeasonCastSurveyRoleResponseV2(success=True, removed=removed)
    except HTTPException:
        raise
    except Exception as error:
        raise _unexpected_problem(error, request, operation="delete") from error
