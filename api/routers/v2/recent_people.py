"""Strict API v2 recent-people admin endpoints."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from pydantic import ValidationError

from api.auth import InternalAdminUser
from api.schemas.v2.recent_people import (
    RecentPeopleListResponseV2,
    RecentPeopleProblemResponseV2,
    RecordRecentPersonRequestV2,
    RecordRecentPersonResponseV2,
)
from trr_backend.db.pg import database_service_unavailable_detail, is_database_service_unavailable_error
from trr_backend.problem import problem_http_exception
from trr_backend.repositories import recent_people as recent_people_repo

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/recent-people", tags=["admin-recent-people-v2"])

_ERROR_RESPONSES: dict[int | str, dict[str, Any]] = {
    400: {"model": RecentPeopleProblemResponseV2, "description": "The recent-people request is invalid."},
    500: {"model": RecentPeopleProblemResponseV2, "description": "The recent-people request failed."},
    503: {"model": RecentPeopleProblemResponseV2, "description": "The recent-people store is unavailable."},
}
_LIST_QUERY_PARAMETER = {
    "name": "limit",
    "in": "query",
    "required": False,
    "schema": {"type": "integer", "minimum": 1, "maximum": 50, "default": 20},
}
_CREATE_REQUEST_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["personId"],
    "properties": {
        "personId": {"type": "string", "format": "uuid"},
        "showId": {"type": ["string", "null"]},
    },
}
_DEFAULT_LIMIT = 20


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
    logger.exception("[admin-recent-people-v2] %s failed", operation)
    return _problem(
        request,
        code="RECENT_PEOPLE_REQUEST_FAILED",
        status=500,
        message="The recent-people request could not be completed.",
    )


def _parse_limit(request: Request) -> int:
    raw_limit = request.query_params.get("limit")
    if raw_limit is None or raw_limit == "":
        return _DEFAULT_LIMIT
    try:
        limit = int(raw_limit)
    except (TypeError, ValueError) as error:
        raise _problem(
            request,
            code="INVALID_RECENT_PEOPLE_LIMIT",
            status=400,
            message="limit must be an integer between 1 and 50.",
        ) from error
    if limit < 1 or limit > 50:
        raise _problem(
            request,
            code="INVALID_RECENT_PEOPLE_LIMIT",
            status=400,
            message="limit must be an integer between 1 and 50.",
        )
    return limit


async def _parse_record_request(request: Request) -> RecordRecentPersonRequestV2:
    try:
        payload = await request.json()
        return RecordRecentPersonRequestV2.model_validate(payload)
    except (ValidationError, ValueError, TypeError) as error:
        raise _problem(
            request,
            code="INVALID_RECENT_PERSON_REQUEST",
            status=400,
            message="personId must be a valid UUID and showId must be a string or null.",
        ) from error


def _actor_uid(admin: dict[str, Any], request: Request) -> str:
    actor_uid = str(admin.get("admin_uid") or "").strip()
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
    response_model=RecentPeopleListResponseV2,
    operation_id="listAdminRecentPeopleV2",
    responses=_ERROR_RESPONSES,
    openapi_extra={"parameters": [_LIST_QUERY_PARAMETER]},
)
def list_recent_people(request: Request, admin: InternalAdminUser) -> RecentPeopleListResponseV2:
    limit = _parse_limit(request)
    try:
        people, _query_count = recent_people_repo.list_recent_people(_actor_uid(admin, request), limit=limit)
        return RecentPeopleListResponseV2.model_validate(
            {
                "people": people,
                "pagination": {
                    "limit": limit,
                    "count": len(people),
                },
            }
        )
    except HTTPException:
        raise
    except Exception as error:
        raise _unexpected_problem(error, request, operation="list") from error


@router.post(
    "",
    response_model=RecordRecentPersonResponseV2,
    operation_id="recordAdminRecentPersonV2",
    responses=_ERROR_RESPONSES,
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {"application/json": {"schema": _CREATE_REQUEST_SCHEMA}},
        }
    },
)
async def record_recent_person(request: Request, admin: InternalAdminUser) -> RecordRecentPersonResponseV2:
    body = await _parse_record_request(request)
    try:
        payload, _query_count = recent_people_repo.record_recent_person_view(
            firebase_uid=_actor_uid(admin, request),
            person_id=str(body.person_id),
            show_context=body.show_id,
            cap=_DEFAULT_LIMIT,
        )
        return RecordRecentPersonResponseV2.model_validate(payload)
    except HTTPException:
        raise
    except Exception as error:
        raise _unexpected_problem(error, request, operation="record") from error
