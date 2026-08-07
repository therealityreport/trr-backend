"""Strict API v2 admin media endpoints."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, TypeVar, cast
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, ValidationError

from api.auth import InternalAdminUser
from api.schemas.v2.admin_media import (
    AdminImageArchiveRequestV2,
    AdminImageReassignRequestV2,
    AdminImageResponseV2,
    AdminMediaLinksResponseV2,
    AdminMediaProblemResponseV2,
    AdminMediaSuccessResponseV2,
    AdminSeasonAssetsResponseV2,
    CreateAdminMediaLinkRequestV2,
    CreateAdminMediaLinkResponseV2,
    FeaturedImageValidationRequestV2,
    FeaturedImageValidationResponseV2,
    PatchAdminMediaLinkContextRequestV2,
    PatchAdminMediaLinkContextResponseV2,
)
from trr_backend.db.pg import database_service_unavailable_detail, is_database_service_unavailable_error
from trr_backend.problem import problem_http_exception
from trr_backend.repositories.admin_media import ImageType

if TYPE_CHECKING:
    from api.schemas.v2.admin_media import AdminMediaLinkV2
from trr_backend.services import admin_media as admin_media_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin", tags=["admin-media-v2"])

_DEFAULT_ASSET_LIMIT = 200
_MAX_ASSET_LIMIT = 500
_VALID_IMAGE_TYPES = frozenset({"cast", "episode", "season"})
_COMMON_ERROR_RESPONSES: dict[int | str, dict[str, Any]] = {
    400: {"model": AdminMediaProblemResponseV2, "description": "The admin-media request is invalid."},
    500: {"model": AdminMediaProblemResponseV2, "description": "The admin-media request failed."},
    503: {"model": AdminMediaProblemResponseV2, "description": "The admin-media store is unavailable."},
}
_NOT_FOUND_ERROR_RESPONSES: dict[int | str, dict[str, Any]] = {
    **_COMMON_ERROR_RESPONSES,
    404: {"model": AdminMediaProblemResponseV2, "description": "The requested admin-media record was not found."},
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
    "schema": {"type": "integer", "minimum": 0},
}
_IMAGE_TYPE_PATH_PARAMETER = {
    "name": "image_type",
    "in": "path",
    "required": True,
    "schema": {"type": "string", "enum": ["cast", "episode", "season"]},
}
_IMAGE_ID_PATH_PARAMETER = {
    "name": "image_id",
    "in": "path",
    "required": True,
    "schema": {"type": "string", "format": "uuid"},
}
_LINK_ID_PATH_PARAMETER = {
    "name": "link_id",
    "in": "path",
    "required": True,
    "schema": {"type": "string", "format": "uuid"},
}
_ASSET_QUERY_PARAMETERS = [
    {
        "name": "limit",
        "in": "query",
        "required": False,
        "schema": {"type": "integer", "minimum": 1, "maximum": _MAX_ASSET_LIMIT, "default": 200},
    },
    {
        "name": "offset",
        "in": "query",
        "required": False,
        "schema": {"type": "integer", "minimum": 0, "default": 0},
    },
    {
        "name": "sources",
        "in": "query",
        "required": False,
        "schema": {"type": "string", "description": "Comma-separated source names."},
    },
    {
        "name": "full",
        "in": "query",
        "required": False,
        "schema": {"type": "boolean", "default": False},
    },
]
_MEDIA_ASSET_ID_QUERY_PARAMETER = {
    "name": "media_asset_id",
    "in": "query",
    "required": True,
    "schema": {"type": "string", "format": "uuid"},
}
_FEATURED_IMAGE_REQUEST_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["image_id", "expected_kind"],
    "properties": {
        "image_id": {"type": "string", "format": "uuid"},
        "expected_kind": {"type": "string", "enum": ["poster", "backdrop"]},
    },
}
_ARCHIVE_REQUEST_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["archive"],
    "properties": {
        "archive": {"type": "boolean"},
        "reason": {"anyOf": [{"type": "string", "maxLength": 2000}, {"type": "null"}]},
    },
}
_REASSIGN_REQUEST_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["to_entity_id"],
    "properties": {
        "to_entity_id": {"type": "string", "format": "uuid"},
        "to_type": {
            "anyOf": [
                {"type": "string", "enum": ["cast", "episode", "season"]},
                {"type": "null"},
            ]
        },
        "mode": {"type": "string", "enum": ["preserve", "copy"], "default": "preserve"},
    },
}
_CREATE_MEDIA_LINK_REQUEST_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["media_asset_id", "entity_type", "entity_id"],
    "properties": {
        "media_asset_id": {"type": "string", "format": "uuid"},
        "entity_type": {"type": "string", "enum": ["person", "season", "show", "episode"]},
        "entity_id": {"type": "string", "format": "uuid"},
        "kind": {
            "anyOf": [
                {"type": "string", "minLength": 1, "maxLength": 200},
                {"type": "null"},
            ]
        },
        "context": {"anyOf": [{"type": "object"}, {"type": "null"}]},
    },
}
_THUMBNAIL_CROP_INPUT_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["x", "y", "zoom", "mode"],
    "properties": {
        "x": {"anyOf": [{"type": "number"}, {"type": "string"}]},
        "y": {"anyOf": [{"type": "number"}, {"type": "string"}]},
        "zoom": {"anyOf": [{"type": "number"}, {"type": "string"}]},
        "mode": {"type": "string", "enum": ["manual", "auto"]},
    },
}
_PATCH_MEDIA_LINK_CONTEXT_REQUEST_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "minProperties": 1,
    "properties": {
        "people_count": {
            "anyOf": [{"type": "number"}, {"type": "string"}, {"type": "null"}],
        },
        "people_count_source": {
            "anyOf": [{"type": "string", "enum": ["auto", "manual"]}, {"type": "null"}],
        },
        "thumbnail_crop": {"anyOf": [_THUMBNAIL_CROP_INPUT_SCHEMA, {"type": "null"}]},
    },
}

ModelT = TypeVar("ModelT", bound=BaseModel)


def _request_body(schema: dict[str, Any]) -> dict[str, Any]:
    return {
        "required": True,
        "content": {"application/json": {"schema": schema}},
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
    logger.exception("[admin-media-v2] %s failed", operation)
    return _problem(
        request,
        code="ADMIN_MEDIA_REQUEST_FAILED",
        status=500,
        message="The admin-media request could not be completed.",
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


def _parse_image_type(request: Request) -> ImageType:
    image_type = str(request.path_params.get("image_type") or "").strip()
    if image_type not in _VALID_IMAGE_TYPES:
        raise _problem(
            request,
            code="INVALID_IMAGE_TYPE",
            status=400,
            message="image_type must be cast, episode, or season.",
        )
    return cast(ImageType, image_type)


def _parse_season_number(request: Request) -> int:
    try:
        season_number = int(str(request.path_params.get("season_number") or "").strip())
    except (TypeError, ValueError, AttributeError) as error:
        raise _problem(
            request,
            code="INVALID_SEASON_NUMBER",
            status=400,
            message="season_number must be a non-negative integer.",
        ) from error
    if season_number < 0:
        raise _problem(
            request,
            code="INVALID_SEASON_NUMBER",
            status=400,
            message="season_number must be a non-negative integer.",
        )
    return season_number


def _parse_asset_pagination(request: Request) -> tuple[int, int]:
    try:
        limit = int(str(request.query_params.get("limit", _DEFAULT_ASSET_LIMIT)).strip())
        offset = int(str(request.query_params.get("offset", 0)).strip())
    except (TypeError, ValueError, AttributeError) as error:
        raise _problem(
            request,
            code="INVALID_PAGINATION",
            status=400,
            message=f"limit must be 1-{_MAX_ASSET_LIMIT}; offset must be at least 0.",
        ) from error
    if limit < 1 or limit > _MAX_ASSET_LIMIT or offset < 0:
        raise _problem(
            request,
            code="INVALID_PAGINATION",
            status=400,
            message=f"limit must be 1-{_MAX_ASSET_LIMIT}; offset must be at least 0.",
        )
    return limit, offset


def _parse_bool_query(request: Request, name: str, *, default: bool = False) -> bool:
    if name not in request.query_params:
        return default
    raw_value = str(request.query_params.get(name) or "").strip().lower()
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


def _parse_sources(request: Request) -> list[str] | None:
    raw_sources = str(request.query_params.get("sources") or "")
    sources = list(dict.fromkeys(source.strip().lower() for source in raw_sources.split(",") if source.strip()))
    return sources or None


async def _parse_request_model(
    request: Request,
    model: type[ModelT],
    *,
    code: str,
    message: str,
) -> ModelT:
    try:
        return model.model_validate(await request.json())
    except (ValidationError, ValueError, TypeError) as error:
        raise _problem(request, code=code, status=400, message=message) from error


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


async def _parse_context_patch(request: Request) -> dict[str, Any]:
    try:
        raw_body = await request.json()
        body = PatchAdminMediaLinkContextRequestV2.model_validate(raw_body)
    except (ValidationError, ValueError, TypeError) as error:
        raise _problem(
            request,
            code="INVALID_MEDIA_LINK_CONTEXT_REQUEST",
            status=400,
            message="Allowed keys are people_count, people_count_source, and thumbnail_crop.",
        ) from error
    if not isinstance(raw_body, dict) or not body.model_fields_set:
        raise _problem(
            request,
            code="INVALID_MEDIA_LINK_CONTEXT_REQUEST",
            status=400,
            message="At least one media-link context field is required.",
        )

    patch: dict[str, Any] = {}
    if "people_count" in body.model_fields_set:
        raw_people_count = raw_body.get("people_count")
        parsed_people_count = admin_media_service.parse_people_count(raw_people_count)
        if raw_people_count is not None and parsed_people_count is None:
            raise _problem(
                request,
                code="INVALID_MEDIA_LINK_CONTEXT_REQUEST",
                status=400,
                message="people_count must be a finite number, numeric string, or null.",
            )
        patch["people_count"] = parsed_people_count
    if "people_count_source" in body.model_fields_set:
        patch["people_count_source"] = body.people_count_source
    if "thumbnail_crop" in body.model_fields_set:
        raw_crop = raw_body.get("thumbnail_crop")
        parsed_crop = admin_media_service.parse_thumbnail_crop(raw_crop)
        if raw_crop is not None and parsed_crop is None:
            raise _problem(
                request,
                code="INVALID_MEDIA_LINK_CONTEXT_REQUEST",
                status=400,
                message="thumbnail_crop must contain valid x, y, zoom, and mode values, or be null.",
            )
        patch["thumbnail_crop"] = parsed_crop
    return patch


@router.get(
    "/shows/{show_id}/seasons/{season_number}/assets",
    response_model=AdminSeasonAssetsResponseV2,
    response_model_exclude_unset=True,
    operation_id="getAdminShowSeasonAssetsV2",
    summary="Get admin assets for a show season",
    responses=_COMMON_ERROR_RESPONSES,
    openapi_extra={"parameters": [_SHOW_ID_PATH_PARAMETER, _SEASON_NUMBER_PATH_PARAMETER, *_ASSET_QUERY_PARAMETERS]},
)
def get_show_season_assets(request: Request, _: InternalAdminUser) -> AdminSeasonAssetsResponseV2:
    show_id = _parse_uuid(
        request.path_params.get("show_id"),
        request,
        field_name="show_id",
        code="INVALID_SHOW_ID",
    )
    season_number = _parse_season_number(request)
    limit, offset = _parse_asset_pagination(request)
    full = _parse_bool_query(request, "full")
    try:
        payload, _query_count = admin_media_service.get_show_season_assets(
            show_id=show_id,
            season_number=season_number,
            limit=limit,
            offset=offset,
            sources=_parse_sources(request),
            full=full,
        )
        return AdminSeasonAssetsResponseV2.model_validate(payload)
    except Exception as error:
        raise _unexpected_problem(error, request, operation="get-show-season-assets") from error


@router.post(
    "/shows/{show_id}/featured-image-validation",
    response_model=FeaturedImageValidationResponseV2,
    operation_id="validateAdminShowFeaturedImageV2",
    summary="Validate a featured show image",
    responses=_COMMON_ERROR_RESPONSES,
    openapi_extra={
        "parameters": [_SHOW_ID_PATH_PARAMETER],
        "requestBody": _request_body(_FEATURED_IMAGE_REQUEST_SCHEMA),
    },
)
async def validate_show_featured_image(
    request: Request,
    _: InternalAdminUser,
) -> FeaturedImageValidationResponseV2:
    show_id = _parse_uuid(
        request.path_params.get("show_id"),
        request,
        field_name="show_id",
        code="INVALID_SHOW_ID",
    )
    body = await _parse_request_model(
        request,
        FeaturedImageValidationRequestV2,
        code="INVALID_FEATURED_IMAGE_VALIDATION_REQUEST",
        message="image_id must be a valid UUID and expected_kind must be poster or backdrop.",
    )
    try:
        valid, _query_count = admin_media_service.validate_show_featured_image(
            show_id=show_id,
            image_id=str(body.image_id),
            expected_kind=body.expected_kind,
        )
        return FeaturedImageValidationResponseV2(valid=valid)
    except Exception as error:
        raise _unexpected_problem(error, request, operation="validate-show-featured-image") from error


@router.get(
    "/images/{image_type}/{image_id}",
    response_model=AdminImageResponseV2,
    operation_id="getAdminImageV2",
    summary="Get an admin image",
    responses=_NOT_FOUND_ERROR_RESPONSES,
    openapi_extra={"parameters": [_IMAGE_TYPE_PATH_PARAMETER, _IMAGE_ID_PATH_PARAMETER]},
)
def get_image(request: Request, _: InternalAdminUser) -> AdminImageResponseV2:
    image_type = _parse_image_type(request)
    image_id = _parse_uuid(
        request.path_params.get("image_id"),
        request,
        field_name="image_id",
        code="INVALID_IMAGE_ID",
    )
    try:
        image, _query_count = admin_media_service.get_image(image_type, image_id)
        if image is None:
            raise _problem(request, code="IMAGE_NOT_FOUND", status=404, message="Image not found.")
        return AdminImageResponseV2(image=image)
    except Exception as error:
        raise _unexpected_problem(error, request, operation="get-image") from error


@router.delete(
    "/images/{image_type}/{image_id}",
    response_model=AdminMediaSuccessResponseV2,
    operation_id="deleteAdminImageV2",
    summary="Permanently delete an admin image",
    responses=_COMMON_ERROR_RESPONSES,
    openapi_extra={"parameters": [_IMAGE_TYPE_PATH_PARAMETER, _IMAGE_ID_PATH_PARAMETER]},
)
def delete_image(request: Request, admin: InternalAdminUser) -> AdminMediaSuccessResponseV2:
    image_type = _parse_image_type(request)
    image_id = _parse_uuid(
        request.path_params.get("image_id"),
        request,
        field_name="image_id",
        code="INVALID_IMAGE_ID",
    )
    try:
        admin_media_service.delete_image(
            image_type=image_type,
            image_id=image_id,
            actor_uid=_actor_uid(admin, request),
        )
        return AdminMediaSuccessResponseV2(success=True)
    except Exception as error:
        raise _unexpected_problem(error, request, operation="delete-image") from error


@router.put(
    "/images/{image_type}/{image_id}/archive",
    response_model=AdminMediaSuccessResponseV2,
    operation_id="putAdminImageArchiveV2",
    summary="Archive or unarchive an admin image",
    responses=_COMMON_ERROR_RESPONSES,
    openapi_extra={
        "parameters": [_IMAGE_TYPE_PATH_PARAMETER, _IMAGE_ID_PATH_PARAMETER],
        "requestBody": _request_body(_ARCHIVE_REQUEST_SCHEMA),
    },
)
async def put_image_archive(request: Request, admin: InternalAdminUser) -> AdminMediaSuccessResponseV2:
    image_type = _parse_image_type(request)
    image_id = _parse_uuid(
        request.path_params.get("image_id"),
        request,
        field_name="image_id",
        code="INVALID_IMAGE_ID",
    )
    body = await _parse_request_model(
        request,
        AdminImageArchiveRequestV2,
        code="INVALID_IMAGE_ARCHIVE_REQUEST",
        message="archive must be a boolean; reason must be a string or null, with no extra fields.",
    )
    try:
        admin_media_service.set_image_archive_state(
            image_type=image_type,
            image_id=image_id,
            archive=body.archive,
            reason=body.reason,
            actor_uid=_actor_uid(admin, request),
        )
        return AdminMediaSuccessResponseV2(success=True)
    except Exception as error:
        raise _unexpected_problem(error, request, operation="put-image-archive") from error


@router.put(
    "/images/{image_type}/{image_id}/reassign",
    response_model=AdminMediaSuccessResponseV2,
    operation_id="putAdminImageReassignV2",
    summary="Reassign an admin image",
    responses=_NOT_FOUND_ERROR_RESPONSES,
    openapi_extra={
        "parameters": [_IMAGE_TYPE_PATH_PARAMETER, _IMAGE_ID_PATH_PARAMETER],
        "requestBody": _request_body(_REASSIGN_REQUEST_SCHEMA),
    },
)
async def put_image_reassign(request: Request, admin: InternalAdminUser) -> AdminMediaSuccessResponseV2:
    image_type = _parse_image_type(request)
    image_id = _parse_uuid(
        request.path_params.get("image_id"),
        request,
        field_name="image_id",
        code="INVALID_IMAGE_ID",
    )
    body = await _parse_request_model(
        request,
        AdminImageReassignRequestV2,
        code="INVALID_IMAGE_REASSIGN_REQUEST",
        message="The image reassignment request is invalid.",
    )
    try:
        admin_media_service.reassign_image(
            image_type=image_type,
            image_id=image_id,
            to_type=body.to_type,
            to_entity_id=str(body.to_entity_id),
            mode=body.mode,
            actor_uid=_actor_uid(admin, request),
        )
        return AdminMediaSuccessResponseV2(success=True)
    except admin_media_service.SourceImageNotFoundError as error:
        raise _problem(request, code="SOURCE_IMAGE_NOT_FOUND", status=404, message="Source image not found.") from error
    except Exception as error:
        raise _unexpected_problem(error, request, operation="put-image-reassign") from error


@router.get(
    "/media-links",
    response_model=AdminMediaLinksResponseV2,
    operation_id="listAdminMediaLinksV2",
    summary="List links for a media asset",
    responses=_COMMON_ERROR_RESPONSES,
    openapi_extra={"parameters": [_MEDIA_ASSET_ID_QUERY_PARAMETER]},
)
def list_media_links(request: Request, _: InternalAdminUser) -> AdminMediaLinksResponseV2:
    media_asset_id = _parse_uuid(
        request.query_params.get("media_asset_id"),
        request,
        field_name="media_asset_id",
        code="INVALID_MEDIA_ASSET_ID",
    )
    try:
        links, _query_count = admin_media_service.get_media_links(media_asset_id)
        return AdminMediaLinksResponseV2(links=cast("list[AdminMediaLinkV2]", links))
    except Exception as error:
        raise _unexpected_problem(error, request, operation="list-media-links") from error


@router.post(
    "/media-links",
    response_model=CreateAdminMediaLinkResponseV2,
    operation_id="createAdminMediaLinkV2",
    summary="Create or merge an admin media link",
    responses=_NOT_FOUND_ERROR_RESPONSES,
    openapi_extra={"requestBody": _request_body(_CREATE_MEDIA_LINK_REQUEST_SCHEMA)},
)
async def create_media_link(request: Request, _: InternalAdminUser) -> CreateAdminMediaLinkResponseV2:
    body = await _parse_request_model(
        request,
        CreateAdminMediaLinkRequestV2,
        code="INVALID_MEDIA_LINK_REQUEST",
        message="The media-link request is invalid.",
    )
    try:
        payload, _query_count = admin_media_service.create_media_link(
            media_asset_id=str(body.media_asset_id),
            entity_type=body.entity_type,
            entity_id=str(body.entity_id),
            kind=body.kind or "gallery",
            context=body.context or {},
        )
        return CreateAdminMediaLinkResponseV2.model_validate(payload)
    except admin_media_service.MediaAssetNotFoundError as error:
        raise _problem(request, code="MEDIA_ASSET_NOT_FOUND", status=404, message="Media asset not found.") from error
    except Exception as error:
        raise _unexpected_problem(error, request, operation="create-media-link") from error


@router.patch(
    "/media-links/{link_id}/context",
    response_model=PatchAdminMediaLinkContextResponseV2,
    operation_id="patchAdminMediaLinkContextV2",
    summary="Patch safe context keys for an admin media link",
    responses=_NOT_FOUND_ERROR_RESPONSES,
    openapi_extra={
        "parameters": [_LINK_ID_PATH_PARAMETER],
        "requestBody": _request_body(_PATCH_MEDIA_LINK_CONTEXT_REQUEST_SCHEMA),
    },
)
async def patch_media_link_context(
    request: Request,
    _: InternalAdminUser,
) -> PatchAdminMediaLinkContextResponseV2:
    link_id = _parse_uuid(
        request.path_params.get("link_id"),
        request,
        field_name="link_id",
        code="INVALID_MEDIA_LINK_ID",
    )
    patch = await _parse_context_patch(request)
    try:
        payload, _query_count = admin_media_service.update_media_link_context(link_id, patch)
        if payload is None:
            raise _problem(request, code="MEDIA_LINK_NOT_FOUND", status=404, message="Media link not found.")
        return PatchAdminMediaLinkContextResponseV2.model_validate(payload)
    except Exception as error:
        raise _unexpected_problem(error, request, operation="patch-media-link-context") from error
