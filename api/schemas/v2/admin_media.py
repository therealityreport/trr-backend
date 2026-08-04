"""Strict API v2 contracts for admin images, season assets, and media links."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class AdminSeasonAssetV2(_StrictModel):
    id: str = Field(min_length=1)
    type: Literal["season", "episode", "cast", "show"]
    origin_table: Literal["show_images", "season_images", "episode_images", "cast_photos", "media_assets"] | None = None
    source: str | None
    source_url: str | None = None
    kind: str = Field(min_length=1)
    hosted_url: str = Field(min_length=1)
    original_url: str | None = None
    thumb_url: str | None = None
    display_url: str | None = None
    detail_url: str | None = None
    crop_display_url: str | None = None
    crop_detail_url: str | None = None
    width: int | None
    height: int | None
    caption: str | None
    episode_number: int | None = None
    person_name: str | None = None
    person_id: str | None = None
    season_number: int | None = None
    ingest_status: str | None = None
    created_at: str | None = None
    fetched_at: str | None = None
    context_section: str | None = None
    context_type: str | None = None
    metadata: dict[str, Any] | None = None
    hosted_content_type: str | None = None
    link_id: str | None = None
    media_asset_id: str | None = None
    people_count: int | None = Field(default=None, ge=0)
    people_count_source: Literal["auto", "manual"] | None = None
    thumbnail_focus_x: float | None = None
    thumbnail_focus_y: float | None = None
    thumbnail_zoom: float | None = None
    thumbnail_crop_mode: Literal["manual", "auto"] | None = None
    logo_black_url: str | None = None
    logo_white_url: str | None = None
    logo_link_is_primary: bool | None = None


class AdminAssetPaginationV2(_StrictModel):
    limit: int = Field(ge=1, le=5000)
    offset: int = Field(ge=0)
    count: int = Field(ge=0, le=5000)
    has_more: bool
    next_cursor: str | None
    cursor: str | None
    full: bool
    truncated: bool


class AdminSeasonAssetsResponseV2(_StrictModel):
    assets: list[AdminSeasonAssetV2]
    pagination: AdminAssetPaginationV2


class FeaturedImageValidationRequestV2(_StrictModel):
    image_id: UUID
    expected_kind: Literal["poster", "backdrop"]


class FeaturedImageValidationResponseV2(_StrictModel):
    valid: bool


class AdminImageResponseV2(_StrictModel):
    image: dict[str, Any]


class AdminMediaSuccessResponseV2(_StrictModel):
    success: Literal[True]


class AdminImageArchiveRequestV2(_StrictModel):
    archive: bool = Field(strict=True)
    reason: str | None = Field(default=None, max_length=2000)


class AdminImageReassignRequestV2(_StrictModel):
    to_entity_id: UUID
    to_type: Literal["cast", "episode", "season"] | None = None
    mode: Literal["preserve", "copy"] = "preserve"


class AdminMediaLinkV2(_StrictModel):
    id: UUID
    entity_type: str = Field(min_length=1)
    entity_id: UUID
    media_asset_id: UUID
    kind: str = Field(min_length=1)
    position: int | None
    context: dict[str, Any] | None
    created_at: datetime


class AdminMediaLinksResponseV2(_StrictModel):
    links: list[AdminMediaLinkV2]


class CreateAdminMediaLinkRequestV2(_StrictModel):
    media_asset_id: UUID
    entity_type: Literal["person", "season", "show", "episode"]
    entity_id: UUID
    kind: str | None = Field(default=None, min_length=1, max_length=200)
    context: dict[str, Any] | None = None


class CreateAdminMediaLinkResponseV2(_StrictModel):
    link: AdminMediaLinkV2
    already_exists: bool
    message: str


class AdminMediaLinkThumbnailCropInputV2(_StrictModel):
    x: int | float | str
    y: int | float | str
    zoom: int | float | str
    mode: Literal["manual", "auto"]


class PatchAdminMediaLinkContextRequestV2(_StrictModel):
    people_count: int | float | str | None = None
    people_count_source: Literal["auto", "manual"] | None = None
    thumbnail_crop: AdminMediaLinkThumbnailCropInputV2 | None = None


class AdminMediaLinkThumbnailCropV2(_StrictModel):
    x: float = Field(ge=0, le=100)
    y: float = Field(ge=0, le=100)
    zoom: float = Field(ge=1, le=4)
    mode: Literal["manual", "auto"]


class PatchAdminMediaLinkContextResponseV2(_StrictModel):
    link_id: UUID
    people_count: int | None = Field(default=None, ge=0)
    people_count_source: Literal["auto", "manual"] | None
    thumbnail_crop: AdminMediaLinkThumbnailCropV2 | None


class AdminMediaProblemDetailV2(_StrictModel):
    code: str
    status: int
    message: str
    trace_id: str
    request_id: str
    retryable: bool | None = None
    detail: dict[str, Any] | None = None
    reason: str | None = None
    retry_after_ms: int | None = None


class AdminMediaProblemResponseV2(_StrictModel):
    detail: AdminMediaProblemDetailV2
