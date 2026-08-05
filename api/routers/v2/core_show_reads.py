"""Public, read-only API v2 endpoints for core shows, seasons, and episodes."""

from __future__ import annotations

import logging
from typing import Any
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request

from api.schemas.v2.core_show_reads import (
    CoreEpisodeListResponseV2,
    CoreEpisodeResponseV2,
    CoreSeasonListResponseV2,
    CoreSeasonResponseV2,
    CoreShowListResponseV2,
    CoreShowReadProblemResponseV2,
    CoreShowResponseV2,
)
from trr_backend.db.pg import database_service_unavailable_detail, is_database_service_unavailable_error
from trr_backend.problem import problem_http_exception

try:
    from trr_backend.services import core_show_reads as _core_show_reads_service
except ImportError:  # pragma: no cover - exercised by integration if backend worker has not landed.
    _core_show_reads_service = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)

router = APIRouter(tags=["public-core-show-reads-v2"])

_MAX_LIMIT = 500
_DEFAULT_LIMIT = 50
_MAX_QUERY_LENGTH = 200
_COMMON_ERROR_RESPONSES: dict[int | str, dict[str, Any]] = {
    400: {"model": CoreShowReadProblemResponseV2, "description": "The core show read request is invalid."},
    500: {"model": CoreShowReadProblemResponseV2, "description": "The core show read request failed."},
    503: {"model": CoreShowReadProblemResponseV2, "description": "The core show read store is unavailable."},
}
_DETAIL_ERROR_RESPONSES: dict[int | str, dict[str, Any]] = {
    **_COMMON_ERROR_RESPONSES,
    404: {"model": CoreShowReadProblemResponseV2, "description": "The requested resource was not found."},
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
_INCLUDE_EPISODE_SIGNAL_PARAMETER = {
    "name": "include_episode_signal",
    "in": "query",
    "required": False,
    "schema": {"type": "boolean", "default": False},
}
_SHOW_FIELDS = frozenset(
    {
        "id",
        "name",
        "description",
        "premiere_date",
        "network",
        "streaming",
        "show_total_seasons",
        "show_total_episodes",
        "imdb_id",
        "tmdb_id",
        "imdb_series_id",
        "tmdb_series_id",
        "most_recent_episode",
        "slug",
        "canonical_slug",
        "alternative_names",
        "genres",
        "networks",
        "streaming_providers",
        "tags",
        "poster_url",
        "backdrop_url",
        "logo_url",
        "primary_poster_image_id",
        "primary_backdrop_image_id",
        "primary_logo_image_id",
        "tmdb_status",
        "tmdb_vote_average",
        "imdb_rating_value",
        "primary_tmdb_poster_path",
        "primary_tmdb_backdrop_path",
        "primary_tmdb_logo_path",
        "external_ids",
        "created_at",
        "updated_at",
    }
)
_SEASON_FIELDS = frozenset(
    {
        "id",
        "show_id",
        "show_name",
        "name",
        "season_number",
        "title",
        "overview",
        "air_date",
        "premiere_date",
        "tmdb_series_id",
        "imdb_series_id",
        "tmdb_season_id",
        "tmdb_season_object_id",
        "poster_path",
        "url_original_poster",
        "external_tvdb_id",
        "external_wikidata_id",
        "external_ids",
        "language",
        "fetched_at",
        "created_at",
        "updated_at",
        "episode_signal",
    }
)
_EPISODE_FIELDS = frozenset(
    {
        "id",
        "show_id",
        "season_id",
        "show_name",
        "show_slug",
        "title",
        "season_number",
        "episode_number",
        "air_date",
        "synopsis",
        "overview",
        "imdb_episode_id",
        "imdb_rating",
        "imdb_vote_count",
        "imdb_primary_image_url",
        "imdb_primary_image_caption",
        "imdb_primary_image_width",
        "imdb_primary_image_height",
        "tmdb_series_id",
        "tmdb_episode_id",
        "episode_type",
        "production_code",
        "runtime",
        "still_path",
        "url_original_still",
        "tmdb_vote_average",
        "tmdb_vote_count",
        "external_ids",
        "fetched_at",
        "created_at",
        "updated_at",
    }
)


def _problem(
    request: Request,
    *,
    code: str,
    status: int,
    message: str,
    retryable: bool = False,
    extra: dict[str, Any] | None = None,
) -> HTTPException:
    return problem_http_exception(
        request,
        code=code,
        status=status,
        message=message,
        retryable=retryable,
        extra=extra,
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


def _service_or_problem(request: Request) -> Any:
    if _core_show_reads_service is None:
        raise _problem(
            request,
            code="CORE_SHOW_READS_SERVICE_UNAVAILABLE",
            status=503,
            message="The core show read service is temporarily unavailable.",
            retryable=True,
        )
    return _core_show_reads_service


def _unexpected_problem(error: Exception, request: Request, *, operation: str) -> HTTPException:
    if isinstance(error, HTTPException):
        return error
    if is_database_service_unavailable_error(error):
        return _database_problem(error, request)
    logger.exception("[core-show-reads-v2] %s failed", operation)
    return _problem(
        request,
        code="CORE_SHOW_READ_REQUEST_FAILED",
        status=500,
        message="The core show read request could not be completed.",
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


def _parse_season_number(raw_value: object, request: Request) -> int:
    try:
        value = int(str(raw_value or "").strip())
    except (TypeError, ValueError, AttributeError) as error:
        raise _problem(
            request,
            code="INVALID_SEASON_NUMBER",
            status=400,
            message="season_number must be a non-negative integer.",
        ) from error
    if value < 0:
        raise _problem(
            request,
            code="INVALID_SEASON_NUMBER",
            status=400,
            message="season_number must be a non-negative integer.",
        )
    return value


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


def _parse_bool_query(request: Request, name: str, *, default: bool = False) -> bool:
    if name not in request.query_params:
        return default
    raw_value = str(request.query_params.get(name) or "").strip().casefold()
    if raw_value in {"1", "true", "yes", "on"}:
        return True
    if raw_value in {"0", "false", "no", "off"}:
        return False
    raise _problem(
        request,
        code="INVALID_BOOLEAN_QUERY",
        status=400,
        message=f"{name} must be true or false.",
    )


def _validate_payload(model: type[Any], payload: Any, request: Request, *, operation: str) -> Any:
    try:
        return model.model_validate(payload)
    except Exception as error:
        raise _unexpected_problem(error, request, operation=f"{operation}-response") from error


def _require_found(value: Any, request: Request, *, code: str, message: str) -> Any:
    if value is None:
        raise _problem(request, code=code, status=404, message=message)
    return value


def _pick_fields(row: dict[str, Any], fields: frozenset[str]) -> dict[str, Any]:
    return {key: value for key, value in row.items() if key in fields}


def _normalize_show(row: dict[str, Any]) -> dict[str, Any]:
    return _pick_fields(row, _SHOW_FIELDS)


def _normalize_season(row: dict[str, Any]) -> dict[str, Any]:
    normalized = _pick_fields(row, _SEASON_FIELDS)
    if normalized.get("episode_signal") is None and (
        "episode_airdate_count" in row or "has_scheduled_or_aired_episode" in row
    ):
        count = int(row.get("episode_airdate_count") or 0)
        normalized["episode_signal"] = {
            "episode_count": count,
            "first_air_date": None,
            "latest_air_date": None,
            "has_episode_data": bool(row.get("has_scheduled_or_aired_episode") or count > 0),
        }
    return normalized


def _normalize_episode(row: dict[str, Any]) -> dict[str, Any]:
    return _pick_fields(row, _EPISODE_FIELDS)


def _page_payload(
    *,
    key: str,
    rows: list[dict[str, Any]],
    limit: int,
    offset: int,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = {
        key: rows,
        "limit": limit,
        "offset": offset,
        "count": len(rows),
        "total_count": None,
        "has_more": len(rows) >= limit,
    }
    if extra:
        payload.update(extra)
    return payload


@router.get(
    "/shows",
    response_model=CoreShowListResponseV2,
    operation_id="listPublicCoreShowsV2",
    summary="List public core shows",
    description="Public, anonymous read endpoint for searchable core show records.",
    responses=_COMMON_ERROR_RESPONSES,
    openapi_extra={"parameters": [_SEARCH_QUERY_PARAMETER, *_PAGINATION_QUERY_PARAMETERS]},
)
def list_shows(request: Request) -> CoreShowListResponseV2:
    query = _parse_query(request)
    limit, offset = _parse_limit_offset(request)
    try:
        rows, _query_count = _service_or_problem(request).search_shows(query or "", limit=limit, offset=offset)
        payload = _page_payload(
            key="shows",
            rows=[_normalize_show(row) for row in rows],
            limit=limit,
            offset=offset,
        )
        return _validate_payload(CoreShowListResponseV2, payload, request, operation="list-shows")
    except Exception as error:
        raise _unexpected_problem(error, request, operation="list-shows") from error


@router.get(
    "/shows/{show_id}",
    response_model=CoreShowResponseV2,
    operation_id="getPublicCoreShowV2",
    summary="Get a public core show",
    responses=_DETAIL_ERROR_RESPONSES,
)
def get_show(request: Request) -> CoreShowResponseV2:
    show_id = _parse_uuid(
        request.path_params.get("show_id"),
        request,
        field_name="show_id",
        code="INVALID_SHOW_ID",
    )
    try:
        show, _query_count = _service_or_problem(request).get_show_by_id(show_id)
        payload = {
            "show": _normalize_show(_require_found(show, request, code="SHOW_NOT_FOUND", message="Show not found."))
        }
        return _validate_payload(CoreShowResponseV2, payload, request, operation="get-show")
    except Exception as error:
        raise _unexpected_problem(error, request, operation="get-show") from error


@router.get(
    "/shows/{show_id}/seasons",
    response_model=CoreSeasonListResponseV2,
    operation_id="listPublicCoreShowSeasonsV2",
    summary="List public core seasons for a show",
    responses=_DETAIL_ERROR_RESPONSES,
    openapi_extra={"parameters": [*_PAGINATION_QUERY_PARAMETERS, _INCLUDE_EPISODE_SIGNAL_PARAMETER]},
)
def list_show_seasons(request: Request) -> CoreSeasonListResponseV2:
    show_id = _parse_uuid(
        request.path_params.get("show_id"),
        request,
        field_name="show_id",
        code="INVALID_SHOW_ID",
    )
    limit, offset = _parse_limit_offset(request)
    include_episode_signal = _parse_bool_query(request, "include_episode_signal")
    try:
        rows, _query_count = _service_or_problem(request).get_seasons_by_show_id(
            show_id,
            limit=limit,
            offset=offset,
            include_episode_signal=include_episode_signal,
        )
        payload = _page_payload(
            key="seasons",
            rows=[_normalize_season(row) for row in rows],
            limit=limit,
            offset=offset,
            extra={"show_id": show_id, "include_episode_signal": include_episode_signal},
        )
        return _validate_payload(CoreSeasonListResponseV2, payload, request, operation="list-show-seasons")
    except Exception as error:
        raise _unexpected_problem(error, request, operation="list-show-seasons") from error


@router.get(
    "/seasons/{season_id}",
    response_model=CoreSeasonResponseV2,
    operation_id="getPublicCoreSeasonV2",
    summary="Get a public core season",
    responses=_DETAIL_ERROR_RESPONSES,
)
def get_season(request: Request) -> CoreSeasonResponseV2:
    season_id = _parse_uuid(
        request.path_params.get("season_id"),
        request,
        field_name="season_id",
        code="INVALID_SEASON_ID",
    )
    try:
        season, _query_count = _service_or_problem(request).get_season_by_id(season_id)
        payload = {
            "season": _normalize_season(
                _require_found(season, request, code="SEASON_NOT_FOUND", message="Season not found.")
            )
        }
        return _validate_payload(CoreSeasonResponseV2, payload, request, operation="get-season")
    except Exception as error:
        raise _unexpected_problem(error, request, operation="get-season") from error


@router.get(
    "/shows/{show_id}/seasons/{season_number}",
    response_model=CoreSeasonResponseV2,
    operation_id="getPublicCoreShowSeasonByNumberV2",
    summary="Get a public core season by show and season number",
    responses=_DETAIL_ERROR_RESPONSES,
)
def get_show_season(request: Request) -> CoreSeasonResponseV2:
    show_id = _parse_uuid(
        request.path_params.get("show_id"),
        request,
        field_name="show_id",
        code="INVALID_SHOW_ID",
    )
    season_number = _parse_season_number(request.path_params.get("season_number"), request)
    try:
        season, _query_count = _service_or_problem(request).get_season_by_show_and_number(show_id, season_number)
        payload = {
            "season": _normalize_season(
                _require_found(season, request, code="SEASON_NOT_FOUND", message="Season not found.")
            )
        }
        return _validate_payload(CoreSeasonResponseV2, payload, request, operation="get-show-season")
    except Exception as error:
        raise _unexpected_problem(error, request, operation="get-show-season") from error


@router.get(
    "/seasons/{season_id}/episodes",
    response_model=CoreEpisodeListResponseV2,
    operation_id="listPublicCoreSeasonEpisodesV2",
    summary="List public core episodes for a season",
    responses=_DETAIL_ERROR_RESPONSES,
    openapi_extra={"parameters": _PAGINATION_QUERY_PARAMETERS},
)
def list_season_episodes(request: Request) -> CoreEpisodeListResponseV2:
    season_id = _parse_uuid(
        request.path_params.get("season_id"),
        request,
        field_name="season_id",
        code="INVALID_SEASON_ID",
    )
    limit, offset = _parse_limit_offset(request)
    try:
        rows, _query_count = _service_or_problem(request).get_episodes_by_season_id(
            season_id,
            limit=limit,
            offset=offset,
        )
        payload = _page_payload(
            key="episodes",
            rows=[_normalize_episode(row) for row in rows],
            limit=limit,
            offset=offset,
        )
        return _validate_payload(CoreEpisodeListResponseV2, payload, request, operation="list-season-episodes")
    except Exception as error:
        raise _unexpected_problem(error, request, operation="list-season-episodes") from error


@router.get(
    "/shows/{show_id}/seasons/{season_number}/episodes",
    response_model=CoreEpisodeListResponseV2,
    operation_id="listPublicCoreShowSeasonEpisodesV2",
    summary="List public core episodes for a show season",
    responses=_DETAIL_ERROR_RESPONSES,
    openapi_extra={"parameters": _PAGINATION_QUERY_PARAMETERS},
)
def list_show_season_episodes(request: Request) -> CoreEpisodeListResponseV2:
    show_id = _parse_uuid(
        request.path_params.get("show_id"),
        request,
        field_name="show_id",
        code="INVALID_SHOW_ID",
    )
    season_number = _parse_season_number(request.path_params.get("season_number"), request)
    limit, offset = _parse_limit_offset(request)
    try:
        rows, _query_count = _service_or_problem(request).get_episodes_by_show_and_season(
            show_id,
            season_number,
            limit=limit,
            offset=offset,
        )
        payload = _page_payload(
            key="episodes",
            rows=[_normalize_episode(row) for row in rows],
            limit=limit,
            offset=offset,
        )
        return _validate_payload(CoreEpisodeListResponseV2, payload, request, operation="list-show-season-episodes")
    except Exception as error:
        raise _unexpected_problem(error, request, operation="list-show-season-episodes") from error


@router.get(
    "/episodes/{episode_id}",
    response_model=CoreEpisodeResponseV2,
    operation_id="getPublicCoreEpisodeV2",
    summary="Get a public core episode",
    responses=_DETAIL_ERROR_RESPONSES,
)
def get_episode(request: Request) -> CoreEpisodeResponseV2:
    episode_id = _parse_uuid(
        request.path_params.get("episode_id"),
        request,
        field_name="episode_id",
        code="INVALID_EPISODE_ID",
    )
    try:
        episode, _query_count = _service_or_problem(request).get_episode_by_id(episode_id)
        payload = {
            "episode": _normalize_episode(
                _require_found(episode, request, code="EPISODE_NOT_FOUND", message="Episode not found.")
            )
        }
        return _validate_payload(CoreEpisodeResponseV2, payload, request, operation="get-episode")
    except Exception as error:
        raise _unexpected_problem(error, request, operation="get-episode") from error


@router.get(
    "/episodes",
    response_model=CoreEpisodeListResponseV2,
    operation_id="listPublicCoreEpisodesV2",
    summary="List public core episodes",
    description="Public, anonymous read endpoint for searchable core episode records.",
    responses=_COMMON_ERROR_RESPONSES,
    openapi_extra={"parameters": [_SEARCH_QUERY_PARAMETER, *_PAGINATION_QUERY_PARAMETERS]},
)
def list_episodes(request: Request) -> CoreEpisodeListResponseV2:
    query = _parse_query(request)
    limit, offset = _parse_limit_offset(request)
    try:
        rows, _query_count = _service_or_problem(request).search_episodes(query or "", limit=limit, offset=offset)
        payload = _page_payload(
            key="episodes",
            rows=[_normalize_episode(row) for row in rows],
            limit=limit,
            offset=offset,
        )
        return _validate_payload(CoreEpisodeListResponseV2, payload, request, operation="list-episodes")
    except Exception as error:
        raise _unexpected_problem(error, request, operation="list-episodes") from error
