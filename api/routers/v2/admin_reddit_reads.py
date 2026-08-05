"""Strict API v2 admin endpoints for Reddit post and post-window reads."""

from __future__ import annotations

import logging
import re
from typing import Any
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request

from api.auth import InternalAdminUser
from api.schemas.v2.admin_reddit_reads import (
    AdminRedditPostDetailResponseV2,
    AdminRedditPostResolveResponseV2,
    AdminRedditPostWindowCountsResponseV2,
    AdminRedditPostWindowResponseV2,
    AdminRedditReadProblemResponseV2,
)
from trr_backend.db.pg import database_service_unavailable_detail, is_database_service_unavailable_error
from trr_backend.problem import problem_http_exception
from trr_backend.repositories import admin_reddit_reads as reddit_reads_repo

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/reddit", tags=["admin-reddit-reads-v2"])

_DEFAULT_PAGE = 1
_DEFAULT_PER_PAGE = 200
_MAX_PER_PAGE = 200
_MAX_COMMENTS_LIMIT = 500
_CANONICAL_CONTAINER_KEY_RE = re.compile(r"^(?:period-(?:preseason|postseason)|episode-[1-9]\d*)$")
_DETAIL_PART_RE = re.compile(r"^[a-z0-9-]+$")

_COMMON_ERROR_RESPONSES: dict[int | str, dict[str, Any]] = {
    400: {"model": AdminRedditReadProblemResponseV2, "description": "The Reddit read request is invalid."},
    500: {"model": AdminRedditReadProblemResponseV2, "description": "The Reddit read request failed."},
    503: {"model": AdminRedditReadProblemResponseV2, "description": "The Reddit store is unavailable."},
}
_DETAIL_ERROR_RESPONSES: dict[int | str, dict[str, Any]] = {
    **_COMMON_ERROR_RESPONSES,
    404: {"model": AdminRedditReadProblemResponseV2, "description": "The requested Reddit post was not found."},
}

_COMMUNITY_ID_QUERY_PARAMETER = {
    "name": "community_id",
    "in": "query",
    "required": True,
    "schema": {"type": "string", "format": "uuid"},
}
_SEASON_ID_QUERY_PARAMETER = {
    "name": "season_id",
    "in": "query",
    "required": True,
    "schema": {"type": "string", "format": "uuid"},
}
_POST_ID_PATH_PARAMETER = {
    "name": "post_id",
    "in": "path",
    "required": True,
    "schema": {"type": "string", "minLength": 1},
}
_COMMENTS_LIMIT_QUERY_PARAMETER = {
    "name": "comments_limit",
    "in": "query",
    "required": False,
    "schema": {"type": "integer", "minimum": 1, "maximum": _MAX_COMMENTS_LIMIT},
}
_WINDOW_KEY_QUERY_PARAMETER = {
    "name": "window_key",
    "in": "query",
    "required": True,
    "schema": {"type": "string", "minLength": 1},
}
_SLUG_QUERY_PARAMETER = {
    "name": "slug",
    "in": "query",
    "required": False,
    "schema": {"type": "string", "pattern": "^[a-z0-9-]+$"},
}
_AUTHOR_QUERY_PARAMETER = {
    "name": "author",
    "in": "query",
    "required": False,
    "schema": {"type": "string", "pattern": "^[a-z0-9-]+$"},
}
_RESOLVE_POST_ID_QUERY_PARAMETER = {
    "name": "post_id",
    "in": "query",
    "required": False,
    "schema": {"type": "string", "minLength": 1},
}
_CONTAINER_KEY_QUERY_PARAMETER = {
    "name": "container_key",
    "in": "query",
    "required": True,
    "schema": {"type": "string", "pattern": "^(?:period-(?:preseason|postseason)|episode-[1-9]\\d*)$"},
}
_PAGE_QUERY_PARAMETER = {
    "name": "page",
    "in": "query",
    "required": False,
    "schema": {"type": "integer", "minimum": 1, "default": _DEFAULT_PAGE},
}
_PER_PAGE_QUERY_PARAMETER = {
    "name": "per_page",
    "in": "query",
    "required": False,
    "schema": {"type": "integer", "minimum": 1, "maximum": _MAX_PER_PAGE, "default": _DEFAULT_PER_PAGE},
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
    if isinstance(error, HTTPException):
        return error
    if is_database_service_unavailable_error(error):
        return _database_problem(error, request)
    logger.exception("[admin-reddit-reads-v2] %s failed", operation)
    return _problem(
        request,
        code="REDDIT_READ_REQUEST_FAILED",
        status=500,
        message="The Reddit read request could not be completed.",
    )


def _parse_required_uuid(request: Request, *, field_name: str, code: str) -> str:
    try:
        return str(UUID(str(request.query_params.get(field_name) or "").strip()))
    except (TypeError, ValueError, AttributeError) as error:
        raise _problem(
            request,
            code=code,
            status=400,
            message=f"{field_name} must be a valid UUID.",
        ) from error


def _parse_post_id(raw_value: object, request: Request, *, code: str) -> str:
    post_id = str(raw_value or "").strip()
    if not post_id:
        raise _problem(request, code=code, status=400, message="post_id is required.")
    return post_id


def _parse_comments_limit(request: Request) -> int | None:
    if "comments_limit" not in request.query_params:
        return None
    try:
        comments_limit = int(str(request.query_params.get("comments_limit") or "").strip())
    except (TypeError, ValueError, AttributeError) as error:
        raise _problem(
            request,
            code="INVALID_COMMENTS_LIMIT",
            status=400,
            message=f"comments_limit must be 1-{_MAX_COMMENTS_LIMIT} when provided.",
        ) from error
    if comments_limit < 1 or comments_limit > _MAX_COMMENTS_LIMIT:
        raise _problem(
            request,
            code="INVALID_COMMENTS_LIMIT",
            status=400,
            message=f"comments_limit must be 1-{_MAX_COMMENTS_LIMIT} when provided.",
        )
    return comments_limit


def _parse_pagination(request: Request) -> tuple[int, int]:
    raw_page = request.query_params.get("page", str(_DEFAULT_PAGE))
    raw_per_page = request.query_params.get("per_page", str(_DEFAULT_PER_PAGE))
    try:
        page = int(str(raw_page).strip())
        per_page = int(str(raw_per_page).strip())
    except (TypeError, ValueError, AttributeError) as error:
        raise _problem(
            request,
            code="INVALID_PAGINATION",
            status=400,
            message=f"page must be at least 1; per_page must be 1-{_MAX_PER_PAGE}.",
        ) from error
    if page < 1 or per_page < 1 or per_page > _MAX_PER_PAGE:
        raise _problem(
            request,
            code="INVALID_PAGINATION",
            status=400,
            message=f"page must be at least 1; per_page must be 1-{_MAX_PER_PAGE}.",
        )
    return page, per_page


def _parse_canonical_container_key(raw_value: object, request: Request, *, field_name: str) -> str:
    container_key = str(raw_value or "").strip().lower()
    if not _CANONICAL_CONTAINER_KEY_RE.fullmatch(container_key):
        raise _problem(
            request,
            code="INVALID_WINDOW_KEY",
            status=400,
            message=f"{field_name} must be a canonical season window key.",
        )
    return container_key


def _resolve_container_key(raw_value: object, request: Request) -> str:
    window_key = str(raw_value or "").strip().lower()
    aliases = {
        "w0": "period-preseason",
        "period-preseason": "period-preseason",
        "w-postseason": "period-postseason",
        "period-postseason": "period-postseason",
    }
    if window_key in aliases:
        return aliases[window_key]
    episode_match = re.fullmatch(r"(?:e|w|episode-)([1-9]\d*)", window_key)
    if episode_match:
        return f"episode-{episode_match.group(1)}"
    raise _problem(
        request,
        code="INVALID_WINDOW_KEY",
        status=400,
        message="window_key must identify a season window.",
    )


def _parse_detail_part(request: Request, *, field_name: str) -> str | None:
    if field_name not in request.query_params:
        return None
    value = str(request.query_params.get(field_name) or "").strip().lower()
    if not _DETAIL_PART_RE.fullmatch(value):
        raise _problem(
            request,
            code="INVALID_POST_RESOLUTION",
            status=400,
            message=f"{field_name} must be a lowercase URL-safe detail part when provided.",
        )
    return value


def _validate_payload(model: type[Any], payload: Any, request: Request, *, operation: str) -> Any:
    try:
        return model.model_validate(payload)
    except Exception as error:
        raise _unexpected_problem(error, request, operation=f"{operation}-response") from error


@router.get(
    "/posts/resolve",
    response_model=AdminRedditPostResolveResponseV2,
    operation_id="resolveAdminRedditPostV2",
    summary="Resolve an admin Reddit post within a season window",
    responses=_DETAIL_ERROR_RESPONSES,
    openapi_extra={
        "parameters": [
            _COMMUNITY_ID_QUERY_PARAMETER,
            _SEASON_ID_QUERY_PARAMETER,
            _WINDOW_KEY_QUERY_PARAMETER,
            _SLUG_QUERY_PARAMETER,
            _AUTHOR_QUERY_PARAMETER,
            _RESOLVE_POST_ID_QUERY_PARAMETER,
        ]
    },
)
def resolve_post(
    request: Request,
    _: InternalAdminUser,
) -> AdminRedditPostResolveResponseV2:
    community_id = _parse_required_uuid(request, field_name="community_id", code="INVALID_COMMUNITY_ID")
    season_id = _parse_required_uuid(request, field_name="season_id", code="INVALID_SEASON_ID")
    container_key = _resolve_container_key(request.query_params.get("window_key"), request)
    title_slug = _parse_detail_part(request, field_name="slug")
    author_slug = _parse_detail_part(request, field_name="author")
    reddit_post_id = (
        _parse_post_id(request.query_params.get("post_id"), request, code="INVALID_POST_ID")
        if "post_id" in request.query_params
        else None
    )
    if reddit_post_id is None and (title_slug is None or author_slug is None):
        raise _problem(
            request,
            code="INVALID_POST_RESOLUTION",
            status=400,
            message="slug and author are required when post_id is omitted.",
        )
    try:
        payload, _query_count = reddit_reads_repo.resolve_reddit_post_detail_by_slug(
            community_id=community_id,
            season_id=season_id,
            container_key=container_key,
            title_slug=title_slug,
            author_slug=author_slug,
            reddit_post_id=reddit_post_id,
        )
        if payload is None:
            raise _problem(
                request,
                code="REDDIT_POST_NOT_FOUND",
                status=404,
                message="Post not found for community, season, and window.",
            )
        return _validate_payload(AdminRedditPostResolveResponseV2, payload, request, operation="resolve-post")
    except Exception as error:
        raise _unexpected_problem(error, request, operation="resolve-post") from error


@router.get(
    "/posts/{post_id}",
    response_model=AdminRedditPostDetailResponseV2,
    operation_id="getAdminRedditPostV2",
    summary="Get an admin Reddit post with its stored detail",
    responses=_DETAIL_ERROR_RESPONSES,
    openapi_extra={
        "parameters": [
            _POST_ID_PATH_PARAMETER,
            _COMMUNITY_ID_QUERY_PARAMETER,
            _SEASON_ID_QUERY_PARAMETER,
            _COMMENTS_LIMIT_QUERY_PARAMETER,
        ]
    },
)
def get_post(request: Request, _: InternalAdminUser) -> AdminRedditPostDetailResponseV2:
    community_id = _parse_required_uuid(request, field_name="community_id", code="INVALID_COMMUNITY_ID")
    season_id = _parse_required_uuid(request, field_name="season_id", code="INVALID_SEASON_ID")
    reddit_post_id = _parse_post_id(request.path_params.get("post_id"), request, code="INVALID_POST_ID")
    comments_limit = _parse_comments_limit(request)
    try:
        post, _query_count = reddit_reads_repo.get_reddit_post_details_by_community_and_season(
            community_id=community_id,
            season_id=season_id,
            reddit_post_id=reddit_post_id,
            comments_limit=comments_limit,
        )
        if post is None:
            raise _problem(
                request,
                code="REDDIT_POST_NOT_FOUND",
                status=404,
                message="Post not found for community and season.",
            )
        return _validate_payload(
            AdminRedditPostDetailResponseV2,
            {"post": post},
            request,
            operation="get-post",
        )
    except Exception as error:
        raise _unexpected_problem(error, request, operation="get-post") from error


@router.get(
    "/post-window-counts",
    response_model=AdminRedditPostWindowCountsResponseV2,
    operation_id="getAdminRedditPostWindowCountsV2",
    summary="Get admin Reddit post counts by season window",
    responses=_COMMON_ERROR_RESPONSES,
    openapi_extra={"parameters": [_COMMUNITY_ID_QUERY_PARAMETER, _SEASON_ID_QUERY_PARAMETER]},
)
def get_post_window_counts(request: Request, _: InternalAdminUser) -> AdminRedditPostWindowCountsResponseV2:
    community_id = _parse_required_uuid(request, field_name="community_id", code="INVALID_COMMUNITY_ID")
    season_id = _parse_required_uuid(request, field_name="season_id", code="INVALID_SEASON_ID")
    try:
        payload, _query_count = reddit_reads_repo.get_stored_post_counts_by_community_and_season(
            community_id,
            season_id,
        )
        return _validate_payload(
            AdminRedditPostWindowCountsResponseV2,
            payload,
            request,
            operation="get-post-window-counts",
        )
    except Exception as error:
        raise _unexpected_problem(error, request, operation="get-post-window-counts") from error


@router.get(
    "/post-windows",
    response_model=AdminRedditPostWindowResponseV2,
    operation_id="listAdminRedditPostWindowV2",
    summary="List stored admin Reddit posts for one season window",
    responses=_COMMON_ERROR_RESPONSES,
    openapi_extra={
        "parameters": [
            _COMMUNITY_ID_QUERY_PARAMETER,
            _SEASON_ID_QUERY_PARAMETER,
            _CONTAINER_KEY_QUERY_PARAMETER,
            _PAGE_QUERY_PARAMETER,
            _PER_PAGE_QUERY_PARAMETER,
        ]
    },
)
def list_post_window(request: Request, _: InternalAdminUser) -> AdminRedditPostWindowResponseV2:
    community_id = _parse_required_uuid(request, field_name="community_id", code="INVALID_COMMUNITY_ID")
    season_id = _parse_required_uuid(request, field_name="season_id", code="INVALID_SEASON_ID")
    container_key = _parse_canonical_container_key(
        request.query_params.get("container_key"),
        request,
        field_name="container_key",
    )
    page, per_page = _parse_pagination(request)
    try:
        payload, _query_count = reddit_reads_repo.get_stored_window_posts_by_community_and_season(
            community_id,
            season_id,
            container_key,
            page=page,
            per_page=per_page,
        )
        return _validate_payload(
            AdminRedditPostWindowResponseV2,
            payload,
            request,
            operation="list-post-window",
        )
    except Exception as error:
        raise _unexpected_problem(error, request, operation="list-post-window") from error
