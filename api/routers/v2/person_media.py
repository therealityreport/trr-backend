"""Strict API v2 admin person-media endpoints."""

from __future__ import annotations

import logging
from typing import Any
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request
from pydantic import ValidationError

from api.auth import InternalAdminUser
from api.schemas.v2.person_media import (
    PersonCoverPhotoDeleteResponseV2,
    PersonCoverPhotoResponseV2,
    PersonMediaProblemResponseV2,
    PersonThumbnailCropWriteResultV2,
    PutPersonCoverPhotoRequestV2,
    PutPersonThumbnailCropRequestV2,
)
from trr_backend.db.pg import database_service_unavailable_detail, is_database_service_unavailable_error
from trr_backend.problem import problem_http_exception
from trr_backend.services import person_media_admin as person_media_service

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/people", tags=["admin-person-media-v2"])

_COMMON_ERROR_RESPONSES: dict[int | str, dict[str, Any]] = {
    400: {"model": PersonMediaProblemResponseV2, "description": "The person-media request is invalid."},
    500: {"model": PersonMediaProblemResponseV2, "description": "The person-media request failed."},
    503: {"model": PersonMediaProblemResponseV2, "description": "The person-media store is unavailable."},
}
_CROP_ERROR_RESPONSES = {
    **_COMMON_ERROR_RESPONSES,
    404: {"model": PersonMediaProblemResponseV2, "description": "The owned person photo was not found."},
}
_PERSON_ID_PATH_PARAMETER = {
    "name": "person_id",
    "in": "path",
    "required": True,
    "schema": {"type": "string", "format": "uuid"},
}
_COVER_PHOTO_REQUEST_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["photo_id", "photo_url"],
    "properties": {
        "photo_id": {"type": "string", "minLength": 1, "maxLength": 2048},
        "photo_url": {"type": "string", "format": "uri", "minLength": 1, "maxLength": 8192},
    },
}
_THUMBNAIL_CROP_REQUEST_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "required": ["origin", "photo_id", "link_id", "crop"],
    "properties": {
        "origin": {"type": "string", "enum": ["cast_photos", "media_links"]},
        "photo_id": {"type": "string", "format": "uuid"},
        "link_id": {"anyOf": [{"type": "string", "format": "uuid"}, {"type": "null"}]},
        "crop": {
            "anyOf": [
                {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["x", "y", "zoom", "mode"],
                    "properties": {
                        "x": {"type": "number", "minimum": 0, "maximum": 100},
                        "y": {"type": "number", "minimum": 0, "maximum": 100},
                        "zoom": {"type": "number", "minimum": 1, "maximum": 4},
                        "mode": {"type": "string", "enum": ["manual", "auto"]},
                    },
                },
                {"type": "null"},
            ]
        },
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
    logger.exception("[admin-person-media-v2] %s failed", operation)
    return _problem(
        request,
        code="PERSON_MEDIA_REQUEST_FAILED",
        status=500,
        message="The person-media request could not be completed.",
    )


def _parse_person_id(request: Request) -> str:
    try:
        return str(UUID(str(request.path_params.get("person_id") or "").strip()))
    except (TypeError, ValueError, AttributeError) as error:
        raise _problem(
            request,
            code="INVALID_PERSON_ID",
            status=400,
            message="person_id must be a valid UUID.",
        ) from error


async def _parse_cover_photo_request(request: Request) -> PutPersonCoverPhotoRequestV2:
    try:
        return PutPersonCoverPhotoRequestV2.model_validate(await request.json())
    except (ValidationError, ValueError, TypeError) as error:
        raise _problem(
            request,
            code="INVALID_PERSON_COVER_PHOTO_REQUEST",
            status=400,
            message="photo_id and a valid http(s) photo_url are required, with no extra fields.",
        ) from error


async def _parse_thumbnail_crop_request(request: Request) -> PutPersonThumbnailCropRequestV2:
    try:
        return PutPersonThumbnailCropRequestV2.model_validate(await request.json())
    except (ValidationError, ValueError, TypeError) as error:
        raise _problem(
            request,
            code="INVALID_PERSON_THUMBNAIL_CROP_REQUEST",
            status=400,
            message="The thumbnail-crop request is invalid.",
        ) from error


def _actor_uid(admin: dict[str, Any], request: Request) -> str:
    actor_uid = str(admin.get("admin_uid") or admin.get("id") or "").strip()
    if actor_uid:
        return actor_uid
    raise _problem(
        request,
        code="ADMIN_ACTOR_MISSING",
        status=403,
        message="The verified admin identity does not contain an actor UID.",
    )


@router.get(
    "/{person_id}/cover-photos",
    response_model=PersonCoverPhotoResponseV2,
    operation_id="getAdminPersonCoverPhotoV2",
    responses=_COMMON_ERROR_RESPONSES,
    openapi_extra={"parameters": [_PERSON_ID_PATH_PARAMETER]},
)
def get_person_cover_photo(request: Request, _: InternalAdminUser) -> PersonCoverPhotoResponseV2:
    person_id = _parse_person_id(request)
    try:
        cover_photo, _query_count = person_media_service.get_cover_photo(person_id)
        return PersonCoverPhotoResponseV2.model_validate({"coverPhoto": cover_photo})
    except HTTPException:
        raise
    except Exception as error:
        raise _unexpected_problem(error, request, operation="get-cover-photo") from error


@router.put(
    "/{person_id}/cover-photos",
    response_model=PersonCoverPhotoResponseV2,
    operation_id="putAdminPersonCoverPhotoV2",
    responses=_COMMON_ERROR_RESPONSES,
    openapi_extra={
        "parameters": [_PERSON_ID_PATH_PARAMETER],
        "requestBody": {
            "required": True,
            "content": {"application/json": {"schema": _COVER_PHOTO_REQUEST_SCHEMA}},
        },
    },
)
async def put_person_cover_photo(
    request: Request,
    admin: InternalAdminUser,
) -> PersonCoverPhotoResponseV2:
    person_id = _parse_person_id(request)
    body = await _parse_cover_photo_request(request)
    try:
        cover_photo, _query_count = person_media_service.set_cover_photo(
            person_id=person_id,
            photo_id=body.photo_id,
            photo_url=body.photo_url,
            actor_uid=_actor_uid(admin, request),
        )
        return PersonCoverPhotoResponseV2.model_validate({"coverPhoto": cover_photo})
    except HTTPException:
        raise
    except Exception as error:
        raise _unexpected_problem(error, request, operation="put-cover-photo") from error


@router.delete(
    "/{person_id}/cover-photos",
    response_model=PersonCoverPhotoDeleteResponseV2,
    operation_id="deleteAdminPersonCoverPhotoV2",
    responses=_COMMON_ERROR_RESPONSES,
    openapi_extra={"parameters": [_PERSON_ID_PATH_PARAMETER]},
)
def delete_person_cover_photo(
    request: Request,
    _: InternalAdminUser,
) -> PersonCoverPhotoDeleteResponseV2:
    person_id = _parse_person_id(request)
    try:
        removed, _query_count = person_media_service.remove_cover_photo(person_id)
        return PersonCoverPhotoDeleteResponseV2(success=True, removed=removed)
    except HTTPException:
        raise
    except Exception as error:
        raise _unexpected_problem(error, request, operation="delete-cover-photo") from error


@router.put(
    "/{person_id}/thumbnail-crops",
    response_model=PersonThumbnailCropWriteResultV2,
    operation_id="putAdminPersonThumbnailCropV2",
    responses=_CROP_ERROR_RESPONSES,
    openapi_extra={
        "parameters": [_PERSON_ID_PATH_PARAMETER],
        "requestBody": {
            "required": True,
            "content": {"application/json": {"schema": _THUMBNAIL_CROP_REQUEST_SCHEMA}},
        },
    },
)
async def put_person_thumbnail_crop(
    request: Request,
    _: InternalAdminUser,
) -> PersonThumbnailCropWriteResultV2:
    person_id = _parse_person_id(request)
    body = await _parse_thumbnail_crop_request(request)
    photo_id = str(body.link_id or body.photo_id)
    try:
        crop = body.crop.model_dump(mode="json") if body.crop is not None else None
        result, _query_count = person_media_service.update_thumbnail_crop(
            origin=body.origin,
            person_id=person_id,
            photo_id=photo_id,
            crop=crop,
        )
        if result is None:
            raise _problem(
                request,
                code="PERSON_THUMBNAIL_CROP_NOT_FOUND",
                status=404,
                message="Photo not found.",
            )
        return PersonThumbnailCropWriteResultV2.model_validate(result)
    except HTTPException:
        raise
    except Exception as error:
        raise _unexpected_problem(error, request, operation="put-thumbnail-crop") from error
