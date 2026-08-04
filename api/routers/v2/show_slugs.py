"""Strict API v2 admin exact show-slug endpoint."""

from __future__ import annotations

import logging
import re
from typing import Any

from fastapi import APIRouter, HTTPException, Request

from api.auth import InternalAdminUser
from api.schemas.v2.show_slugs import ExactShowSlugResponseV2, ShowSlugProblemResponseV2
from trr_backend.db.pg import database_service_unavailable_detail, is_database_service_unavailable_error
from trr_backend.problem import problem_http_exception
from trr_backend.repositories import show_slug_reads

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/shows", tags=["admin-show-slugs-v2"])

_MAX_SLUG_LENGTH = 160
_SLUG_RE = re.compile(r"^[a-z0-9]+(?:-+[a-z0-9]+)*$")
_ERROR_RESPONSES: dict[int | str, dict[str, Any]] = {
    400: {"model": ShowSlugProblemResponseV2, "description": "The show slug is invalid."},
    404: {"model": ShowSlugProblemResponseV2, "description": "The show was not found."},
    500: {"model": ShowSlugProblemResponseV2, "description": "The exact show-slug read failed."},
    503: {"model": ShowSlugProblemResponseV2, "description": "The show store is unavailable."},
}
_SLUG_PATH_PARAMETER = {
    "name": "slug",
    "in": "path",
    "required": True,
    "description": "A show slug. Matching is exact after lowercase normalization.",
    "schema": {
        "type": "string",
        "minLength": 1,
        "maxLength": _MAX_SLUG_LENGTH,
        "pattern": r"^[A-Za-z0-9]+(?:-+[A-Za-z0-9]+)*$",
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
    logger.exception("[admin-show-slugs-v2] %s failed", operation)
    return _problem(
        request,
        code="SHOW_SLUG_REQUEST_FAILED",
        status=500,
        message="The exact show-slug request could not be completed.",
    )


def _parse_slug(request: Request) -> str:
    normalized_slug = str(request.path_params.get("slug") or "").strip().lower()
    if not normalized_slug or len(normalized_slug) > _MAX_SLUG_LENGTH or _SLUG_RE.fullmatch(normalized_slug) is None:
        raise _problem(
            request,
            code="INVALID_SHOW_SLUG",
            status=400,
            message="slug must contain only letters, numbers, and hyphens.",
        )
    return normalized_slug


@router.get(
    "/exact-slug/{slug}",
    response_model=ExactShowSlugResponseV2,
    operation_id="getAdminShowByExactSlugV2",
    summary="Get a show by its exact stored slug",
    responses=_ERROR_RESPONSES,
    openapi_extra={"parameters": [_SLUG_PATH_PARAMETER]},
)
def get_show_by_exact_slug(
    request: Request,
    _: InternalAdminUser,
) -> ExactShowSlugResponseV2:
    normalized_slug = _parse_slug(request)
    try:
        show, _query_count = show_slug_reads.get_show_by_exact_slug(normalized_slug)
    except Exception as error:
        raise _unexpected_problem(error, request, operation="read") from error
    if show is None:
        raise _problem(
            request,
            code="SHOW_NOT_FOUND",
            status=404,
            message="Show not found",
        )
    try:
        response = ExactShowSlugResponseV2.model_validate({"show": show})
        if response.show.slug.lower() != normalized_slug:
            raise ValueError("repository returned a different show slug")
        return response
    except Exception as error:
        raise _unexpected_problem(error, request, operation="response") from error
