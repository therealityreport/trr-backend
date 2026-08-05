"""Strict API v2 contracts for public core show reads."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class CoreShowV2(_StrictModel):
    id: UUID
    name: str = Field(min_length=1)
    description: str | None = None
    premiere_date: date | None = None
    network: str | None = None
    streaming: str | None = None
    show_total_seasons: int | None = Field(default=None, ge=0)
    show_total_episodes: int | None = Field(default=None, ge=0)
    imdb_id: str | None = None
    tmdb_id: int | None = None
    imdb_series_id: str | None = None
    tmdb_series_id: int | None = None
    most_recent_episode: dict[str, Any] | str | None = None
    slug: str | None = None
    canonical_slug: str | None = None
    alternative_names: list[str] = Field(default_factory=list)
    genres: list[str] = Field(default_factory=list)
    networks: list[str] = Field(default_factory=list)
    streaming_providers: list[str] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)
    poster_url: str | None = None
    backdrop_url: str | None = None
    logo_url: str | None = None
    primary_poster_image_id: UUID | None = None
    primary_backdrop_image_id: UUID | None = None
    primary_logo_image_id: UUID | None = None
    tmdb_status: str | None = None
    tmdb_vote_average: float | None = None
    imdb_rating_value: float | None = None
    primary_tmdb_poster_path: str | None = None
    primary_tmdb_backdrop_path: str | None = None
    primary_tmdb_logo_path: str | None = None
    external_ids: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime | None = None
    updated_at: datetime | None = None


class CoreSeasonEpisodeSignalV2(_StrictModel):
    episode_count: int = Field(ge=0)
    first_air_date: date | None
    latest_air_date: date | None
    has_episode_data: bool


class CoreSeasonV2(_StrictModel):
    id: UUID
    show_id: UUID
    show_name: str | None = None
    name: str | None = None
    season_number: int = Field(ge=0)
    title: str | None = None
    overview: str | None = None
    air_date: date | None = None
    premiere_date: date | None = None
    tmdb_series_id: int | None = None
    imdb_series_id: str | None = None
    tmdb_season_id: int | None = None
    tmdb_season_object_id: str | None = None
    poster_path: str | None = None
    url_original_poster: str | None = None
    external_tvdb_id: int | None = None
    external_wikidata_id: str | None = None
    external_ids: dict[str, Any] = Field(default_factory=dict)
    language: str | None = None
    fetched_at: datetime | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    episode_signal: CoreSeasonEpisodeSignalV2 | None = None


class CoreEpisodeV2(_StrictModel):
    id: UUID
    show_id: UUID | None = None
    season_id: UUID | None = None
    show_name: str | None = None
    show_slug: str | None = None
    title: str | None = None
    season_number: int | None = Field(default=None, ge=0)
    episode_number: int | None = Field(default=None, ge=0)
    air_date: date | None = None
    synopsis: str | None = None
    overview: str | None = None
    imdb_episode_id: str | None = None
    imdb_rating: float | None = None
    imdb_vote_count: int | None = Field(default=None, ge=0)
    imdb_primary_image_url: str | None = None
    imdb_primary_image_caption: str | None = None
    imdb_primary_image_width: int | None = Field(default=None, ge=0)
    imdb_primary_image_height: int | None = Field(default=None, ge=0)
    tmdb_series_id: int | None = None
    tmdb_episode_id: int | None = None
    episode_type: str | None = None
    production_code: str | None = None
    runtime: int | None = Field(default=None, ge=0)
    still_path: str | None = None
    url_original_still: str | None = None
    tmdb_vote_average: float | None = None
    tmdb_vote_count: int | None = Field(default=None, ge=0)
    external_ids: dict[str, Any] = Field(default_factory=dict)
    fetched_at: datetime | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None


class CorePaginationV2(_StrictModel):
    limit: int = Field(ge=1)
    offset: int = Field(ge=0)
    count: int = Field(ge=0)
    total_count: int | None = Field(default=None, ge=0)
    has_more: bool


class CoreShowListResponseV2(CorePaginationV2):
    shows: list[CoreShowV2]


class CoreShowResponseV2(_StrictModel):
    show: CoreShowV2


class CoreSeasonListResponseV2(CorePaginationV2):
    show_id: UUID
    include_episode_signal: bool
    seasons: list[CoreSeasonV2]


class CoreSeasonResponseV2(_StrictModel):
    season: CoreSeasonV2


class CoreEpisodeListResponseV2(CorePaginationV2):
    episodes: list[CoreEpisodeV2]


class CoreEpisodeResponseV2(_StrictModel):
    episode: CoreEpisodeV2


class CoreShowReadProblemDetailV2(_StrictModel):
    code: str
    status: int
    message: str
    trace_id: str
    request_id: str
    retryable: bool | None = None
    detail: dict[str, Any] | None = None
    reason: str | None = None
    retry_after_ms: int | None = None


class CoreShowReadProblemResponseV2(_StrictModel):
    detail: CoreShowReadProblemDetailV2
