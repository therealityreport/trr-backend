from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from uuid import UUID


@dataclass(frozen=True)
class ShowRecord:
    """
    Canonical show record (maps to `core.shows`).

    Note: core.shows is the canonical surface; vendor payloads live in tmdb_meta/imdb_meta.
    """

    id: UUID
    name: str
    description: str | None = None
    premiere_date: str | None = None

    # ID columns (renamed from imdb_series_id/tmdb_series_id)
    imdb_id: str | None = None
    tmdb_id: int | None = None
    # Backward compatibility aliases
    imdb_series_id: str | None = None
    tmdb_series_id: int | None = None

    # Show metadata
    show_total_seasons: int | None = None
    show_total_episodes: int | None = None

    # New typed columns for most_recent_episode
    most_recent_episode: str | None = None  # Legacy text field
    most_recent_episode_season: int | None = None
    most_recent_episode_number: int | None = None
    most_recent_episode_title: str | None = None
    most_recent_episode_air_date: date | None = None
    most_recent_episode_imdb_id: str | None = None

    # Primary image FKs
    primary_poster_image_id: UUID | None = None
    primary_backdrop_image_id: UUID | None = None
    primary_logo_image_id: UUID | None = None

    # Resolution flags
    needs_imdb_resolution: bool | None = None
    needs_tmdb_resolution: bool | None = None

    # Array columns for attributes
    genres: list[str] | None = None
    keywords: list[str] | None = None
    tags: list[str] | None = None
    networks: list[str] | None = None
    streaming_providers: list[str] | None = None

    # List provenance (values: 'imdb', 'tmdb')
    listed_on: list[str] | None = None

    # External IDs (from TMDb)
    tvdb_id: int | None = None
    tvrage_id: int | None = None
    wikidata_id: str | None = None
    facebook_id: str | None = None
    instagram_id: str | None = None
    twitter_id: str | None = None


@dataclass(frozen=True)
class ShowUpsert:
    """Data for inserting/updating a show."""

    name: str

    # ID columns (use new names)
    imdb_id: str | None = None
    tmdb_id: int | None = None

    # Core metadata
    show_total_seasons: int | None = None
    show_total_episodes: int | None = None
    premiere_date: str | None = None  # YYYY-MM-DD when available
    description: str | None = None
    needs_imdb_resolution: bool | None = None

    # Most recent episode (typed)
    most_recent_episode: str | None = None  # Legacy text field (for backward compat)
    most_recent_episode_season: int | None = None
    most_recent_episode_number: int | None = None
    most_recent_episode_title: str | None = None
    most_recent_episode_air_date: str | None = None  # YYYY-MM-DD
    most_recent_episode_imdb_id: str | None = None

    # Resolution flags
    needs_tmdb_resolution: bool | None = None

    # Array columns for attributes
    genres: list[str] | None = None
    keywords: list[str] | None = None
    tags: list[str] | None = None
    networks: list[str] | None = None
    streaming_providers: list[str] | None = None

    # List provenance (values: 'imdb', 'tmdb')
    listed_on: list[str] | None = None

    # External IDs (from TMDb)
    tvdb_id: int | None = None
    tvrage_id: int | None = None
    wikidata_id: str | None = None
    facebook_id: str | None = None
    instagram_id: str | None = None
    twitter_id: str | None = None
