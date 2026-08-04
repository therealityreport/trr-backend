"""Public, read-only API v2 endpoints for cast and credit projections."""

from __future__ import annotations

import logging
from typing import Any
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request

from api.schemas.v2.core_cast_credit_reads import (
    CastCreditReadProblemResponseV2,
    PersonCreditResponseV2,
    PersonEpisodeCreditResponseV2,
    SeasonCastResponseV2,
    ShowCastResponseV2,
)
from trr_backend.db.pg import database_service_unavailable_detail, is_database_service_unavailable_error
from trr_backend.problem import problem_http_exception

try:
    from trr_backend.services import core_cast_credit_reads as _core_cast_credit_reads_service
except ImportError:  # pragma: no cover - integration catches an incomplete worker handoff.
    _core_cast_credit_reads_service = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)

router = APIRouter(tags=["public-core-cast-credit-reads-v2"])

_MAX_LIMIT = 500
_DEFAULT_LIMIT = 50
_COMMON_ERROR_RESPONSES: dict[int | str, dict[str, Any]] = {
    400: {"model": CastCreditReadProblemResponseV2, "description": "The cast or credit request is invalid."},
    500: {"model": CastCreditReadProblemResponseV2, "description": "The cast or credit request failed."},
    503: {"model": CastCreditReadProblemResponseV2, "description": "The cast or credit store is unavailable."},
}
_PAGINATION_PARAMETERS = [
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
_SHOW_ID_PATH_PARAMETER = {
    "name": "show_id",
    "in": "path",
    "required": True,
    "schema": {"type": "string", "format": "uuid"},
}
_SEASON_ID_PATH_PARAMETER = {
    "name": "season_id",
    "in": "path",
    "required": True,
    "schema": {"type": "string", "format": "uuid"},
}
_PERSON_ID_PATH_PARAMETER = {
    "name": "person_id",
    "in": "path",
    "required": True,
    "schema": {"type": "string", "format": "uuid"},
}
_SHOW_VIEW_PARAMETER = {
    "name": "view",
    "in": "query",
    "required": False,
    "schema": {
        "type": "string",
        "enum": ["membership", "episode_evidence", "archive_only"],
        "default": "membership",
    },
}
_INCLUDE_PHOTOS_PARAMETER = {
    "name": "include_photos",
    "in": "query",
    "required": False,
    "schema": {"type": "boolean", "default": True},
}
_PHOTO_FALLBACK_PARAMETER = {
    "name": "photo_fallback",
    "in": "query",
    "required": False,
    "schema": {"type": "string", "enum": ["none", "bravo"], "default": "none"},
}
_SEASON_VIEW_PARAMETER = {
    "name": "view",
    "in": "query",
    "required": False,
    "schema": {"type": "string", "enum": ["membership", "episode_counts"], "default": "membership"},
}
_INCLUDE_ARCHIVE_ONLY_PARAMETER = {
    "name": "include_archive_only",
    "in": "query",
    "required": False,
    "schema": {"type": "boolean", "default": False},
}
_SHOW_ID_QUERY_PARAMETER = {
    "name": "show_id",
    "in": "query",
    "required": False,
    "schema": {"type": "string", "format": "uuid"},
}
_INCLUDE_ARCHIVE_FOOTAGE_PARAMETER = {
    "name": "include_archive_footage",
    "in": "query",
    "required": False,
    "schema": {"type": "boolean", "default": False},
}
_CAST_FIELDS = frozenset(
    {
        "id",
        "show_id",
        "person_id",
        "show_name",
        "cast_member_name",
        "role",
        "billing_order",
        "credit_category",
        "source_type",
        "full_name",
        "known_for",
        "photo_url",
        "thumbnail_focus_x",
        "thumbnail_focus_y",
        "thumbnail_zoom",
        "thumbnail_crop_mode",
        "total_episodes",
        "archive_episode_count",
        "created_at",
        "updated_at",
    }
)
_SEASON_MEMBERSHIP_FIELDS = frozenset(
    {
        "person_id",
        "person_name",
        "seasons_appeared",
        "total_episodes",
        "photo_url",
        "thumbnail_focus_x",
        "thumbnail_focus_y",
        "thumbnail_zoom",
        "thumbnail_crop_mode",
    }
)
_SEASON_EPISODE_COUNT_FIELDS = frozenset(
    {
        "person_id",
        "person_name",
        "episodes_in_season",
        "total_episodes",
        "photo_url",
        "thumbnail_focus_x",
        "thumbnail_focus_y",
        "thumbnail_zoom",
        "thumbnail_crop_mode",
        "archive_episodes_in_season",
    }
)
_PERSON_CREDIT_FIELDS = frozenset(
    {
        "id",
        "show_id",
        "person_id",
        "show_name",
        "role",
        "billing_order",
        "credit_category",
        "source_type",
        "external_imdb_id",
        "external_url",
        "metadata",
    }
)
_EPISODE_CREDIT_FIELDS = frozenset(
    {
        "show_id",
        "credit_id",
        "credit_category",
        "role",
        "billing_order",
        "source_type",
        "episode_id",
        "season_number",
        "episode_number",
        "episode_name",
        "appearance_type",
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
        extra={"reason": detail.get("reason"), "retry_after_ms": detail.get("retry_after_ms")},
    )


def _unexpected_problem(error: Exception, request: Request, *, operation: str) -> HTTPException:
    if isinstance(error, HTTPException):
        return error
    if is_database_service_unavailable_error(error):
        return _database_problem(error, request)
    logger.exception("[core-cast-credit-reads-v2] %s failed", operation)
    return _problem(
        request,
        code="CORE_CAST_CREDIT_READ_REQUEST_FAILED",
        status=500,
        message="The cast or credit read request could not be completed.",
    )


def _service_or_problem(request: Request) -> Any:
    if _core_cast_credit_reads_service is None:
        raise _problem(
            request,
            code="CORE_CAST_CREDIT_READS_SERVICE_UNAVAILABLE",
            status=503,
            message="The cast and credit read service is temporarily unavailable.",
            retryable=True,
        )
    return _core_cast_credit_reads_service


def _parse_uuid(raw_value: object, request: Request, *, field_name: str, code: str) -> str:
    try:
        return str(UUID(str(raw_value or "").strip()))
    except (TypeError, ValueError, AttributeError) as error:
        raise _problem(request, code=code, status=400, message=f"{field_name} must be a valid UUID.") from error


def _parse_limit_offset(request: Request) -> tuple[int, int]:
    try:
        limit = int(str(request.query_params.get("limit", _DEFAULT_LIMIT)).strip())
        offset = int(str(request.query_params.get("offset", 0)).strip())
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


def _parse_bool(request: Request, name: str, *, default: bool) -> bool:
    if name not in request.query_params:
        return default
    value = str(request.query_params.get(name) or "").strip().casefold()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    raise _problem(
        request,
        code="INVALID_BOOLEAN_QUERY",
        status=400,
        message=f"{name} must be true or false.",
    )


def _parse_choice(
    request: Request,
    name: str,
    *,
    allowed: frozenset[str],
    default: str,
    code: str,
) -> str:
    value = str(request.query_params.get(name, default) or "").strip().casefold()
    if value not in allowed:
        raise _problem(
            request,
            code=code,
            status=400,
            message=f"{name} must be one of: {', '.join(sorted(allowed))}.",
        )
    return value


def _validate_payload(model: type[Any], payload: Any, request: Request, *, operation: str) -> Any:
    try:
        return model.model_validate(payload)
    except Exception as error:
        raise _unexpected_problem(error, request, operation=f"{operation}-response") from error


def _page_payload(
    *,
    rows: list[dict[str, Any]],
    limit: int,
    offset: int,
    total_count: int | None = None,
) -> dict[str, Any]:
    return {
        "limit": limit,
        "offset": offset,
        "count": len(rows),
        "total_count": total_count,
        "has_more": offset + len(rows) < total_count if total_count is not None else len(rows) >= limit,
    }


@router.get(
    "/shows/{show_id}/cast",
    response_model=ShowCastResponseV2,
    operation_id="listPublicCoreShowCastV2",
    summary="List a public show cast projection",
    responses=_COMMON_ERROR_RESPONSES,
    openapi_extra={
        "security": [],
        "parameters": [
            _SHOW_ID_PATH_PARAMETER,
            _SHOW_VIEW_PARAMETER,
            _INCLUDE_PHOTOS_PARAMETER,
            _PHOTO_FALLBACK_PARAMETER,
            *_PAGINATION_PARAMETERS,
        ],
    },
)
def list_show_cast(request: Request) -> ShowCastResponseV2:
    show_id = _parse_uuid(request.path_params.get("show_id"), request, field_name="show_id", code="INVALID_SHOW_ID")
    view = _parse_choice(
        request,
        "view",
        allowed=frozenset({"membership", "episode_evidence", "archive_only"}),
        default="membership",
        code="INVALID_SHOW_CAST_VIEW",
    )
    include_photos = _parse_bool(request, "include_photos", default=True)
    photo_fallback = _parse_choice(
        request,
        "photo_fallback",
        allowed=frozenset({"none", "bravo"}),
        default="none",
        code="INVALID_PHOTO_FALLBACK",
    )
    limit, offset = _parse_limit_offset(request)
    try:
        rows, _query_count = _service_or_problem(request).get_show_cast(
            show_id,
            view=view,
            limit=limit,
            offset=offset,
            include_photos=include_photos,
            photo_fallback=photo_fallback,
        )
        payload = {
            "show_id": show_id,
            "view": view,
            "include_photos": include_photos,
            "photo_fallback": photo_fallback,
            "cast": [{key: value for key, value in row.items() if key in _CAST_FIELDS} for row in rows],
            **_page_payload(rows=rows, limit=limit, offset=offset),
        }
        return _validate_payload(ShowCastResponseV2, payload, request, operation="list-show-cast")
    except Exception as error:
        raise _unexpected_problem(error, request, operation="list-show-cast") from error


@router.get(
    "/seasons/{season_id}/cast",
    response_model=SeasonCastResponseV2,
    operation_id="listPublicCoreSeasonCastV2",
    summary="List a public season cast projection",
    responses=_COMMON_ERROR_RESPONSES,
    openapi_extra={
        "security": [],
        "parameters": [
            _SEASON_ID_PATH_PARAMETER,
            _SEASON_VIEW_PARAMETER,
            _INCLUDE_ARCHIVE_ONLY_PARAMETER,
            _PHOTO_FALLBACK_PARAMETER,
            *_PAGINATION_PARAMETERS,
        ],
    },
)
def list_season_cast(request: Request) -> SeasonCastResponseV2:
    season_id = _parse_uuid(
        request.path_params.get("season_id"),
        request,
        field_name="season_id",
        code="INVALID_SEASON_ID",
    )
    view = _parse_choice(
        request,
        "view",
        allowed=frozenset({"membership", "episode_counts"}),
        default="membership",
        code="INVALID_SEASON_CAST_VIEW",
    )
    include_archive_only = _parse_bool(request, "include_archive_only", default=False)
    photo_fallback = _parse_choice(
        request,
        "photo_fallback",
        allowed=frozenset({"none", "bravo"}),
        default="none",
        code="INVALID_PHOTO_FALLBACK",
    )
    limit, offset = _parse_limit_offset(request)
    try:
        rows, _query_count = _service_or_problem(request).get_season_cast(
            season_id,
            view=view,
            limit=limit,
            offset=offset,
            include_archive_only=include_archive_only,
            photo_fallback=photo_fallback,
        )
        fields = _SEASON_MEMBERSHIP_FIELDS if view == "membership" else _SEASON_EPISODE_COUNT_FIELDS
        payload = {
            "season_id": season_id,
            "view": view,
            "include_archive_only": include_archive_only,
            "photo_fallback": photo_fallback,
            "cast": [{key: value for key, value in row.items() if key in fields} for row in rows],
            **_page_payload(rows=rows, limit=limit, offset=offset),
        }
        return _validate_payload(SeasonCastResponseV2, payload, request, operation="list-season-cast")
    except Exception as error:
        raise _unexpected_problem(error, request, operation="list-season-cast") from error


@router.get(
    "/people/{person_id}/credits",
    response_model=PersonCreditResponseV2,
    operation_id="listPublicCorePersonCreditsV2",
    summary="List public show credits for a person",
    responses=_COMMON_ERROR_RESPONSES,
    openapi_extra={"security": [], "parameters": [_PERSON_ID_PATH_PARAMETER, *_PAGINATION_PARAMETERS]},
)
def list_person_credits(request: Request) -> PersonCreditResponseV2:
    person_id = _parse_uuid(
        request.path_params.get("person_id"),
        request,
        field_name="person_id",
        code="INVALID_PERSON_ID",
    )
    limit, offset = _parse_limit_offset(request)
    try:
        service_payload, _query_count = _service_or_problem(request).get_person_credits(
            person_id,
            limit=limit,
            offset=offset,
        )
        credits = service_payload.get("credits", [])
        total_count = int(service_payload.get("total_count") or 0)
        payload = {
            "person_id": person_id,
            "credits": [{key: value for key, value in row.items() if key in _PERSON_CREDIT_FIELDS} for row in credits],
            "curated_cast_show_ids": service_payload.get("curated_cast_show_ids", []),
            **_page_payload(rows=credits, limit=limit, offset=offset, total_count=total_count),
        }
        return _validate_payload(PersonCreditResponseV2, payload, request, operation="list-person-credits")
    except Exception as error:
        raise _unexpected_problem(error, request, operation="list-person-credits") from error


@router.get(
    "/people/{person_id}/episode-credits",
    response_model=PersonEpisodeCreditResponseV2,
    operation_id="listPublicCorePersonEpisodeCreditsV2",
    summary="List public episode-credit evidence for a person",
    responses=_COMMON_ERROR_RESPONSES,
    openapi_extra={
        "security": [],
        "parameters": [
            _PERSON_ID_PATH_PARAMETER,
            _SHOW_ID_QUERY_PARAMETER,
            _INCLUDE_ARCHIVE_FOOTAGE_PARAMETER,
            *_PAGINATION_PARAMETERS,
        ],
    },
)
def list_person_episode_credits(request: Request) -> PersonEpisodeCreditResponseV2:
    person_id = _parse_uuid(
        request.path_params.get("person_id"),
        request,
        field_name="person_id",
        code="INVALID_PERSON_ID",
    )
    show_id = None
    if "show_id" in request.query_params:
        show_id = _parse_uuid(
            request.query_params.get("show_id"),
            request,
            field_name="show_id",
            code="INVALID_SHOW_ID",
        )
    include_archive_footage = _parse_bool(request, "include_archive_footage", default=False)
    limit, offset = _parse_limit_offset(request)
    try:
        service_payload, _query_count = _service_or_problem(request).get_person_episode_credits(
            person_id,
            show_id=show_id,
            include_archive_footage=include_archive_footage,
            limit=limit,
            offset=offset,
        )
        rows = service_payload.get("episode_credits", [])
        total_count = int(service_payload.get("total_count") or 0)
        payload = {
            "person_id": person_id,
            "show_id": show_id,
            "include_archive_footage": include_archive_footage,
            "episode_credits": [
                {key: value for key, value in row.items() if key in _EPISODE_CREDIT_FIELDS} for row in rows
            ],
            **_page_payload(rows=rows, limit=limit, offset=offset, total_count=total_count),
        }
        return _validate_payload(
            PersonEpisodeCreditResponseV2,
            payload,
            request,
            operation="list-person-episode-credits",
        )
    except Exception as error:
        raise _unexpected_problem(error, request, operation="list-person-episode-credits") from error
