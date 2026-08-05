"""Strict API v2 contracts for person cover photos and thumbnail crops."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal, Self
from urllib.parse import urlsplit
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class _StrictModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
        populate_by_name=True,
    )


class PersonCoverPhotoV2(_StrictModel):
    person_id: UUID
    photo_id: str = Field(min_length=1, max_length=2048)
    photo_url: str = Field(min_length=1, max_length=8192)
    created_at: datetime
    updated_at: datetime
    created_by_firebase_uid: str = Field(min_length=1, max_length=512)

    @field_validator("photo_url")
    @classmethod
    def require_http_url(cls, value: str) -> str:
        parsed = urlsplit(value)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("photo_url must be a valid http(s) URL")
        return value


class PutPersonCoverPhotoRequestV2(_StrictModel):
    photo_id: str = Field(min_length=1, max_length=2048)
    photo_url: str = Field(min_length=1, max_length=8192)

    @field_validator("photo_url")
    @classmethod
    def require_http_url(cls, value: str) -> str:
        parsed = urlsplit(value)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("photo_url must be a valid http(s) URL")
        return value


class PersonCoverPhotoResponseV2(_StrictModel):
    cover_photo: PersonCoverPhotoV2 | None = Field(alias="coverPhoto")


class PersonCoverPhotoDeleteResponseV2(_StrictModel):
    success: Literal[True]
    removed: bool


class PersonThumbnailCropV2(_StrictModel):
    x: float = Field(ge=0, le=100, strict=True)
    y: float = Field(ge=0, le=100, strict=True)
    zoom: float = Field(ge=1, le=4, strict=True)
    mode: Literal["manual", "auto"]


class PutPersonThumbnailCropRequestV2(_StrictModel):
    origin: Literal["cast_photos", "media_links"]
    photo_id: UUID
    link_id: UUID | None
    crop: PersonThumbnailCropV2 | None

    @model_validator(mode="after")
    def validate_origin_identity(self) -> Self:
        if self.origin == "cast_photos" and self.link_id is not None:
            raise ValueError("link_id is only valid for media_links")
        if self.origin == "media_links" and self.link_id != self.photo_id:
            raise ValueError("link_id must match photo_id for media_links")
        return self


class PersonThumbnailCropWriteResultV2(_StrictModel):
    origin: Literal["cast_photos", "media_links"]
    photo_id: UUID
    person_id: UUID
    link_id: UUID | None
    thumbnail_focus_x: float | None = Field(ge=0, le=100)
    thumbnail_focus_y: float | None = Field(ge=0, le=100)
    thumbnail_zoom: float | None = Field(ge=1, le=4)
    thumbnail_crop_mode: Literal["manual", "auto"] | None


class PersonMediaProblemDetailV2(_StrictModel):
    code: str
    status: int
    message: str
    trace_id: str
    request_id: str
    retryable: bool | None = None
    detail: dict[str, Any] | None = None
    reason: str | None = None
    retry_after_ms: int | None = None


class PersonMediaProblemResponseV2(_StrictModel):
    detail: PersonMediaProblemDetailV2
