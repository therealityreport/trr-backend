"""Public, read-only identity resolver endpoints."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, HTTPException, Request

from api.schemas.v2.identities import (
    IdentityProblemResponse,
    PersonIdentityResponse,
    SeasonIdentityResponse,
    ShowIdentityResponse,
)
from trr_backend.db.pg import database_service_unavailable_detail, is_database_service_unavailable_error
from trr_backend.problem import problem_http_exception
from trr_backend.services import public_identity

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/identities", tags=["public-identities-v2"])

_ERROR_RESPONSES: dict[int | str, dict[str, Any]] = {
    400: {"model": IdentityProblemResponse, "description": "Invalid slug or conflicting show context."},
    404: {"model": IdentityProblemResponse, "description": "No public identity matched the request."},
    409: {"model": IdentityProblemResponse, "description": "A direct alias matched multiple identities."},
    500: {"model": IdentityProblemResponse, "description": "The resolver could not complete the request."},
    503: {"model": IdentityProblemResponse, "description": "The identity store is temporarily unavailable."},
}

_SHOW_SLUG_PATH_PARAMETER = {
    "name": "show_slug",
    "in": "path",
    "required": True,
    "schema": {"type": "string", "maxLength": 160, "example": "rhobh"},
}
_PERSON_SLUG_PATH_PARAMETER = {
    "name": "slug",
    "in": "path",
    "required": True,
    "schema": {"type": "string", "maxLength": 160, "example": "brandi-glanville"},
}


def _to_http_exception(error: Exception, request: Request) -> HTTPException:
    if isinstance(error, public_identity.IdentityResolutionError):
        return problem_http_exception(
            request,
            code=error.code,
            status=error.status,
            message=error.message,
            retryable=False,
            safe_detail=error.detail,
        )
    if is_database_service_unavailable_error(error):
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
    logger.exception("[public-identity-v2] unexpected resolver failure")
    return problem_http_exception(
        request,
        code="INTERNAL_SERVER_ERROR",
        status=500,
        message="The identity resolver could not complete the request.",
        retryable=False,
    )


@router.get(
    "/shows/{slug}",
    response_model=ShowIdentityResponse,
    operation_id="resolvePublicShowIdentityV2",
    summary="Resolve a public show identity",
    description=(
        "Resolves a canonical slug or direct alias. A canonical match wins over a colliding legacy alias; "
        "an unresolved alias collision returns HTTP 409. This endpoint is public and read-only."
    ),
    responses=_ERROR_RESPONSES,
    openapi_extra={
        "parameters": [
            {
                "name": "slug",
                "in": "path",
                "required": True,
                "schema": {
                    "type": "string",
                    "maxLength": 160,
                    "example": "real-housewives-of-beverly-hills",
                },
            }
        ]
    },
)
def resolve_show_identity(request: Request) -> ShowIdentityResponse:
    slug = str(request.path_params.get("slug") or "")
    try:
        return ShowIdentityResponse.model_validate(public_identity.resolve_show(slug))
    except Exception as error:
        raise _to_http_exception(error, request) from error


@router.get(
    "/shows/{show_slug}/seasons/{season_number}",
    response_model=SeasonIdentityResponse,
    operation_id="resolvePublicSeasonIdentityV2",
    summary="Resolve a public season identity",
    description=(
        "Resolves the show alias first, then identifies a season by its non-negative season number. "
        "The canonical season path is derived from the canonical show alias and season number."
    ),
    responses=_ERROR_RESPONSES,
    openapi_extra={
        "parameters": [
            _SHOW_SLUG_PATH_PARAMETER,
            {
                "name": "season_number",
                "in": "path",
                "required": True,
                "schema": {
                    "type": "integer",
                    "minimum": 0,
                    "maximum": 2_147_483_647,
                    "example": 14,
                },
            },
        ]
    },
)
def resolve_season_identity(request: Request) -> SeasonIdentityResponse:
    show_slug = str(request.path_params.get("show_slug") or "")
    season_number = str(request.path_params.get("season_number") or "")
    try:
        return SeasonIdentityResponse.model_validate(
            public_identity.resolve_season(show_slug=show_slug, season_number=season_number)
        )
    except Exception as error:
        raise _to_http_exception(error, request) from error


@router.get(
    "/people/{slug}",
    response_model=PersonIdentityResponse,
    operation_id="resolvePublicPersonIdentityV2",
    summary="Resolve a public person identity",
    description=(
        "Resolves a canonical slug or direct alias. Optional show_id or show_slug context narrows collisions "
        "to people linked to that show's cast. The endpoint rejects requests that provide both contexts."
    ),
    responses=_ERROR_RESPONSES,
    openapi_extra={
        "parameters": [
            _PERSON_SLUG_PATH_PARAMETER,
            {
                "name": "show_id",
                "in": "query",
                "required": False,
                "schema": {"type": "string", "format": "uuid"},
                "description": "Optional show UUID used to narrow person collisions.",
            },
            {
                "name": "show_slug",
                "in": "query",
                "required": False,
                "schema": {"type": "string", "maxLength": 160},
                "description": "Optional show slug or direct alias used to narrow person collisions.",
            },
        ]
    },
)
def resolve_person_identity(request: Request) -> PersonIdentityResponse:
    slug = str(request.path_params.get("slug") or "")
    show_id = request.query_params.get("show_id") if "show_id" in request.query_params else None
    show_slug = request.query_params.get("show_slug") if "show_slug" in request.query_params else None
    try:
        return PersonIdentityResponse.model_validate(
            public_identity.resolve_person(
                slug,
                show_id=show_id,
                show_slug=show_slug,
            )
        )
    except Exception as error:
        raise _to_http_exception(error, request) from error
