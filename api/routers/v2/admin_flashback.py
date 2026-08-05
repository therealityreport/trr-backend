"""Strict API v2 administration endpoints for Flashback quizzes and events."""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Any, Literal
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request, Response
from pydantic import BaseModel, ConfigDict, Field, StrictBool, StrictInt, ValidationError

from api.auth import InternalAdminUser
from trr_backend.db.pg import database_service_unavailable_detail, is_database_service_unavailable_error
from trr_backend.problem import problem_http_exception
from trr_backend.repositories import admin_flashback as flashback_repo

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/flashback", tags=["admin-flashback-v2"])


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class FlashbackQuizV2(_StrictModel):
    id: UUID
    title: str = Field(min_length=1)
    publish_date: date
    description: str | None = None
    is_published: bool
    created_at: datetime
    updated_at: datetime


class FlashbackEventV2(_StrictModel):
    id: UUID
    quiz_id: UUID
    description: str = Field(min_length=1)
    image_url: str | None = None
    year: int
    sort_order: int = Field(ge=1)
    point_value: int = Field(ge=2, le=5)


class FlashbackQuizListResponseV2(_StrictModel):
    quizzes: list[FlashbackQuizV2]


class FlashbackQuizResponseV2(_StrictModel):
    quiz: FlashbackQuizV2


class FlashbackEventListResponseV2(_StrictModel):
    events: list[FlashbackEventV2]


class FlashbackEventResponseV2(_StrictModel):
    event: FlashbackEventV2


class CreateFlashbackQuizRequestV2(_StrictModel):
    title: str = Field(min_length=1)
    publish_date: date
    description: str | None = None


class UpdateFlashbackQuizRequestV2(_StrictModel):
    is_published: StrictBool


class CreateFlashbackEventRequestV2(_StrictModel):
    description: str = Field(min_length=1)
    year: StrictInt
    image_url: str | None = None
    point_value: StrictInt = Field(ge=2, le=5)


class FlashbackProblemDetailV2(_StrictModel):
    code: str
    status: int
    message: str
    trace_id: str
    request_id: str
    retryable: bool | None = None
    detail: dict[str, Any] | None = None
    reason: str | None = None
    retry_after_ms: int | None = None


class FlashbackProblemResponseV2(_StrictModel):
    detail: FlashbackProblemDetailV2


class FlashbackDeleteResponseV2(_StrictModel):
    success: Literal[True]


_COMMON_ERROR_RESPONSES: dict[int | str, dict[str, Any]] = {
    400: {"model": FlashbackProblemResponseV2, "description": "The Flashback request is invalid."},
    500: {"model": FlashbackProblemResponseV2, "description": "The Flashback request failed."},
    503: {"model": FlashbackProblemResponseV2, "description": "The Flashback store is unavailable."},
}
_NOT_FOUND_ERROR_RESPONSES: dict[int | str, dict[str, Any]] = {
    **_COMMON_ERROR_RESPONSES,
    404: {"model": FlashbackProblemResponseV2, "description": "The Flashback resource was not found."},
}


def _uuid_path_parameter(name: str) -> dict[str, Any]:
    return {
        "name": name,
        "in": "path",
        "required": True,
        "schema": {"type": "string", "format": "uuid"},
    }


_CREATE_QUIZ_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["title", "publish_date"],
    "properties": {
        "title": {"type": "string", "minLength": 1},
        "publish_date": {"type": "string", "format": "date"},
        "description": {"type": ["string", "null"]},
    },
}
_UPDATE_QUIZ_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["is_published"],
    "properties": {"is_published": {"type": "boolean"}},
}
_CREATE_EVENT_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["description", "year", "point_value"],
    "properties": {
        "description": {"type": "string", "minLength": 1},
        "year": {"type": "integer"},
        "image_url": {"type": ["string", "null"]},
        "point_value": {"type": "integer", "minimum": 2, "maximum": 5},
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
    logger.exception("[admin-flashback-v2] %s failed", operation)
    return _problem(
        request,
        code="FLASHBACK_ADMIN_REQUEST_FAILED",
        status=500,
        message="The Flashback administration request could not be completed.",
    )


def _parse_uuid(request: Request, *, name: str) -> str:
    try:
        return str(UUID(str(request.path_params.get(name) or "").strip()))
    except (TypeError, ValueError, AttributeError) as error:
        raise _problem(
            request,
            code=f"INVALID_{name.upper()}",
            status=400,
            message=f"{name} must be a valid UUID.",
        ) from error


async def _parse_body(request: Request, model: type[Any], *, code: str, message: str) -> Any:
    try:
        return model.model_validate(await request.json())
    except (ValidationError, ValueError, TypeError) as error:
        raise _problem(request, code=code, status=400, message=message) from error


@router.get(
    "/quizzes",
    response_model=FlashbackQuizListResponseV2,
    operation_id="listAdminFlashbackQuizzesV2",
    responses=_COMMON_ERROR_RESPONSES,
)
def list_quizzes(request: Request, _: InternalAdminUser) -> FlashbackQuizListResponseV2:
    try:
        quizzes, _query_count = flashback_repo.list_quizzes()
        return FlashbackQuizListResponseV2.model_validate({"quizzes": quizzes})
    except Exception as error:
        raise _unexpected_problem(error, request, operation="list-quizzes") from error


@router.post(
    "/quizzes",
    response_model=FlashbackQuizResponseV2,
    status_code=201,
    operation_id="createAdminFlashbackQuizV2",
    responses=_COMMON_ERROR_RESPONSES,
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {"application/json": {"schema": _CREATE_QUIZ_SCHEMA}},
        }
    },
)
async def create_quiz(request: Request, _: InternalAdminUser) -> FlashbackQuizResponseV2:
    body = await _parse_body(
        request,
        CreateFlashbackQuizRequestV2,
        code="INVALID_FLASHBACK_QUIZ_REQUEST",
        message="title and publish_date are required, with no extra fields.",
    )
    try:
        quiz, _query_count = flashback_repo.create_quiz(
            title=body.title,
            publish_date=body.publish_date.isoformat(),
            description=body.description,
        )
        return FlashbackQuizResponseV2.model_validate({"quiz": quiz})
    except Exception as error:
        raise _unexpected_problem(error, request, operation="create-quiz") from error


@router.patch(
    "/quizzes/{quiz_id}",
    response_model=FlashbackQuizResponseV2,
    operation_id="updateAdminFlashbackQuizV2",
    responses=_NOT_FOUND_ERROR_RESPONSES,
    openapi_extra={
        "parameters": [_uuid_path_parameter("quiz_id")],
        "requestBody": {
            "required": True,
            "content": {"application/json": {"schema": _UPDATE_QUIZ_SCHEMA}},
        },
    },
)
async def update_quiz(request: Request, _: InternalAdminUser) -> FlashbackQuizResponseV2:
    quiz_id = _parse_uuid(request, name="quiz_id")
    body = await _parse_body(
        request,
        UpdateFlashbackQuizRequestV2,
        code="INVALID_FLASHBACK_QUIZ_UPDATE_REQUEST",
        message="is_published must be a boolean, with no extra fields.",
    )
    try:
        quiz, _query_count = flashback_repo.set_quiz_published(
            quiz_id=quiz_id,
            is_published=body.is_published,
        )
        if quiz is None:
            raise _problem(
                request,
                code="FLASHBACK_QUIZ_NOT_FOUND",
                status=404,
                message="Flashback quiz not found.",
            )
        return FlashbackQuizResponseV2.model_validate({"quiz": quiz})
    except Exception as error:
        raise _unexpected_problem(error, request, operation="update-quiz") from error


@router.get(
    "/quizzes/{quiz_id}/events",
    response_model=FlashbackEventListResponseV2,
    operation_id="listAdminFlashbackEventsV2",
    responses=_COMMON_ERROR_RESPONSES,
    openapi_extra={"parameters": [_uuid_path_parameter("quiz_id")]},
)
def list_events(request: Request, _: InternalAdminUser) -> FlashbackEventListResponseV2:
    quiz_id = _parse_uuid(request, name="quiz_id")
    try:
        events, _query_count = flashback_repo.list_events(quiz_id=quiz_id)
        return FlashbackEventListResponseV2.model_validate({"events": events})
    except Exception as error:
        raise _unexpected_problem(error, request, operation="list-events") from error


@router.post(
    "/quizzes/{quiz_id}/events",
    response_model=FlashbackEventResponseV2,
    status_code=201,
    operation_id="createAdminFlashbackEventV2",
    responses=_NOT_FOUND_ERROR_RESPONSES,
    openapi_extra={
        "parameters": [_uuid_path_parameter("quiz_id")],
        "requestBody": {
            "required": True,
            "content": {"application/json": {"schema": _CREATE_EVENT_SCHEMA}},
        },
    },
)
async def create_event(request: Request, _: InternalAdminUser) -> FlashbackEventResponseV2:
    quiz_id = _parse_uuid(request, name="quiz_id")
    body = await _parse_body(
        request,
        CreateFlashbackEventRequestV2,
        code="INVALID_FLASHBACK_EVENT_REQUEST",
        message="description, integer year, and point_value between 2 and 5 are required.",
    )
    try:
        event, _query_count = flashback_repo.create_event(
            quiz_id=quiz_id,
            description=body.description,
            year=body.year,
            image_url=body.image_url,
            point_value=body.point_value,
        )
        if event is None:
            raise _problem(
                request,
                code="FLASHBACK_QUIZ_NOT_FOUND",
                status=404,
                message="Flashback quiz not found.",
            )
        return FlashbackEventResponseV2.model_validate({"event": event})
    except Exception as error:
        raise _unexpected_problem(error, request, operation="create-event") from error


@router.delete(
    "/events/{event_id}",
    status_code=204,
    operation_id="deleteAdminFlashbackEventV2",
    responses=_NOT_FOUND_ERROR_RESPONSES,
    openapi_extra={"parameters": [_uuid_path_parameter("event_id")]},
)
def delete_event(request: Request, _: InternalAdminUser) -> Response:
    event_id = _parse_uuid(request, name="event_id")
    try:
        deleted, _query_count = flashback_repo.delete_event(event_id=event_id)
        if not deleted:
            raise _problem(
                request,
                code="FLASHBACK_EVENT_NOT_FOUND",
                status=404,
                message="Flashback event not found.",
            )
        return Response(status_code=204)
    except Exception as error:
        raise _unexpected_problem(error, request, operation="delete-event") from error
