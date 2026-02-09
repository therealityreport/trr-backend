from __future__ import annotations

from dataclasses import dataclass
from typing import Any
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

    # Consolidated jsonb field keyed by source:
    # {"imdb": {"season": 1, "episode": 2, ...}, "tmdb": {...}}
    most_recent_episode: dict[str, Any] | None = None

    # Primary image FKs
    primary_poster_image_id: UUID | None = None
    primary_backdrop_image_id: UUID | None = None
    primary_logo_image_id: UUID | None = None

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

    # Consolidated jsonb field keyed by source:
    # {"imdb": {"season": 1, "episode": 2, ...}, "tmdb": {...}}
    most_recent_episode: dict[str, Any] | None = None

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
