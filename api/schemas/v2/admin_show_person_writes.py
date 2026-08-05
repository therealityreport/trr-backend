"""Strict API v2 contracts for admin show and person write seams."""

from __future__ import annotations

from datetime import date
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class AdminShowUpdateRequestV2(_StrictModel):
    name: str | None = None
    slug: str | None = None
    description: str | None = None
    premiere_date: date | None = None
    alternative_names: list[str] | None = None
    imdb_id: str | None = None
    tmdb_id: int | None = None
    external_ids: dict[str, Any] | None = None
    genres: list[str] | None = None
    networks: list[str] | None = None
    streaming_providers: list[str] | None = None
    tags: list[str] | None = None
    primary_poster_image_id: UUID | None = None
    primary_backdrop_image_id: UUID | None = None
    primary_logo_image_id: UUID | None = None


class AdminShowWriteResponseV2(_StrictModel):
    show: dict[str, Any]


class AdminPersonCanonicalProfileSourceOrderRequestV2(_StrictModel):
    source_order: list[str] = Field(min_length=1, max_length=4)


class AdminPersonCanonicalProfileSourceOrderResponseV2(_StrictModel):
    person: dict[str, Any]


class AdminEffectivePersonSocialHandlesRequestV2(_StrictModel):
    person_ids: list[UUID] = Field(min_length=1, max_length=500)


class AdminEffectivePersonSocialHandlesV2(_StrictModel):
    person_id: UUID
    facebook_handle: str | None = None
    instagram_handle: str | None = None
    tiktok_handle: str | None = None
    twitter_handle: str | None = None
    youtube_handle: str | None = None


class AdminEffectivePersonSocialHandlesResponseV2(_StrictModel):
    handles: list[AdminEffectivePersonSocialHandlesV2]


class AdminShowPersonWriteProblemDetailV2(_StrictModel):
    code: str
    status: int
    message: str
    trace_id: str
    request_id: str
    retryable: bool | None = None
    detail: dict[str, Any] | None = None
    reason: str | None = None
    retry_after_ms: int | None = None


class AdminShowPersonWriteProblemResponseV2(_StrictModel):
    detail: AdminShowPersonWriteProblemDetailV2
