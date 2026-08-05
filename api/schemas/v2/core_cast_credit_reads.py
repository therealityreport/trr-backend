"""Strict API v2 contracts for public cast and credit reads."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class CastMemberV2(_StrictModel):
    id: UUID
    show_id: UUID
    person_id: UUID
    show_name: str | None = None
    cast_member_name: str | None = None
    role: str | None = None
    billing_order: int | None = None
    credit_category: str
    source_type: str
    full_name: str | None = None
    known_for: str | None = None
    photo_url: str | None = None
    thumbnail_focus_x: float | None = None
    thumbnail_focus_y: float | None = None
    thumbnail_zoom: float | None = None
    thumbnail_crop_mode: Literal["manual", "auto"] | None = None
    total_episodes: int | None = Field(default=None, ge=0)
    archive_episode_count: int | None = Field(default=None, ge=0)
    created_at: datetime
    updated_at: datetime


class CastCreditPaginationV2(_StrictModel):
    limit: int = Field(ge=1, le=500)
    offset: int = Field(ge=0)
    count: int = Field(ge=0)
    total_count: int | None = Field(default=None, ge=0)
    has_more: bool


class ShowCastResponseV2(CastCreditPaginationV2):
    show_id: UUID
    view: Literal["membership", "episode_evidence", "archive_only"]
    include_photos: bool
    photo_fallback: Literal["none", "bravo"]
    cast: list[CastMemberV2]


class SeasonCastMemberV2(_StrictModel):
    person_id: UUID
    person_name: str
    seasons_appeared: list[int]
    total_episodes: int = Field(ge=0)
    photo_url: str | None = None
    thumbnail_focus_x: float | None = None
    thumbnail_focus_y: float | None = None
    thumbnail_zoom: float | None = None
    thumbnail_crop_mode: Literal["manual", "auto"] | None = None


class SeasonCastEpisodeCountV2(_StrictModel):
    person_id: UUID
    person_name: str | None = None
    episodes_in_season: int = Field(ge=0)
    total_episodes: int | None = Field(default=None, ge=0)
    photo_url: str | None = None
    thumbnail_focus_x: float | None = None
    thumbnail_focus_y: float | None = None
    thumbnail_zoom: float | None = None
    thumbnail_crop_mode: Literal["manual", "auto"] | None = None
    archive_episodes_in_season: int | None = Field(default=None, ge=0)


class SeasonCastResponseV2(CastCreditPaginationV2):
    season_id: UUID
    view: Literal["membership", "episode_counts"]
    include_archive_only: bool
    photo_fallback: Literal["none", "bravo"]
    cast: list[SeasonCastMemberV2 | SeasonCastEpisodeCountV2]


class PersonCreditV2(_StrictModel):
    # Local UUID credits and synthetic IMDb-only credits share this string field.
    id: str = Field(min_length=1)
    show_id: UUID | None = None
    person_id: UUID
    show_name: str | None = None
    role: str | None = None
    billing_order: int | None = None
    credit_category: str
    source_type: str | None = None
    external_imdb_id: str | None = None
    external_url: str | None = None
    metadata: dict[str, Any] | None = None


class PersonCreditResponseV2(CastCreditPaginationV2):
    person_id: UUID
    credits: list[PersonCreditV2]
    curated_cast_show_ids: list[UUID]


class PersonEpisodeCreditV2(_StrictModel):
    show_id: UUID
    credit_id: UUID
    credit_category: str
    role: str | None = None
    billing_order: int | None = None
    source_type: str | None = None
    episode_id: UUID
    season_number: int | None = Field(default=None, ge=0)
    episode_number: int | None = Field(default=None, ge=0)
    episode_name: str | None = None
    appearance_type: str | None = None


class PersonEpisodeCreditResponseV2(CastCreditPaginationV2):
    person_id: UUID
    show_id: UUID | None = None
    include_archive_footage: bool
    episode_credits: list[PersonEpisodeCreditV2]


class CastCreditReadProblemDetailV2(_StrictModel):
    code: str
    status: int
    message: str
    trace_id: str
    request_id: str
    retryable: bool | None = None
    detail: dict[str, Any] | None = None
    reason: str | None = None
    retry_after_ms: int | None = None


class CastCreditReadProblemResponseV2(_StrictModel):
    detail: CastCreditReadProblemDetailV2
