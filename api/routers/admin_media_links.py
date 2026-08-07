"""Admin endpoints for media-link operations."""

from __future__ import annotations

import logging
from typing import Any, Literal, cast

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, ConfigDict, Field

from api.auth import InternalAdminUser
from trr_backend.db.pg import (
    database_service_unavailable_detail,
    is_database_service_unavailable_error,
)
from trr_backend.problem import problem_http_exception
from trr_backend.repositories import media_link_tags as media_link_tags_repo

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/media-links", tags=["admin-media-links"])


class MediaLinkTagsRequest(BaseModel):
    model_config = ConfigDict(extra="allow")

    people: list[Any] | None = None
    people_count: Any | None = None
    people_count_source: Any | None = None
    face_boxes: Any | None = None


class MediaLinkTagsResponse(BaseModel):
    people_names: list[str] = Field(default_factory=list)
    people_ids: list[str] = Field(default_factory=list)
    people_count: int | None = None
    people_count_source: Literal["auto", "manual"] | None = None
    face_boxes: list[dict[str, Any]] | None = None


def _database_unavailable_exception(error: Exception, request: Request) -> HTTPException:
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


@router.put("/{link_id}/tags", response_model=MediaLinkTagsResponse)
def sync_media_link_tags(
    request: Request,
    link_id: str,
    payload: MediaLinkTagsRequest,
    _: InternalAdminUser = cast(InternalAdminUser, None),
) -> MediaLinkTagsResponse:
    try:
        result = media_link_tags_repo.sync_media_link_tags(
            link_id,
            payload.model_dump(mode="python", exclude_unset=True),
        )
    except media_link_tags_repo.MediaLinkTagsNotFoundError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error
    except Exception as error:
        if is_database_service_unavailable_error(error):
            raise _database_unavailable_exception(error, request) from error
        logger.exception("[admin-media-links] tag sync failed link_id=%s", link_id)
        raise HTTPException(status_code=500, detail="Failed to update media link tags.") from error

    logger.info(
        "[admin-media-links] route=tags link_id=%s people=%s",
        link_id,
        len(result.get("people_ids") or []),
    )
    return MediaLinkTagsResponse(**result)
