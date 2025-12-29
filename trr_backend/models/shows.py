from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping
from uuid import UUID


@dataclass(frozen=True)
class ShowRecord:
    """
    Canonical show record (maps to `core.shows`).

    Note: external IDs and provider metadata live in `external_ids` (jsonb).
    """

    id: UUID
    name: str
    description: str | None = None
    premiere_date: str | None = None
    external_ids: Mapping[str, Any] = field(default_factory=dict)
    imdb_meta: Mapping[str, Any] = field(default_factory=dict)
    imdb_series_id: str | None = None
    tmdb_series_id: int | None = None

    @property
    def imdb_id(self) -> str | None:
        if isinstance(self.imdb_series_id, str) and self.imdb_series_id.strip():
            return self.imdb_series_id.strip()
        value = self.external_ids.get("imdb")
        return value if isinstance(value, str) and value else None

    @property
    def tmdb_id(self) -> int | None:
        if isinstance(self.tmdb_series_id, int):
            return self.tmdb_series_id
        value = self.external_ids.get("tmdb")
        if isinstance(value, int):
            return value
        if isinstance(value, str):
            try:
                return int(value)
            except ValueError:
                return None
        return None


@dataclass(frozen=True)
class ShowUpsert:
    name: str
    tmdb_series_id: int | None = None
    imdb_series_id: str | None = None
    network: str | None = None
    streaming: str | None = None
    show_total_seasons: int | None = None
    show_total_episodes: int | None = None
    most_recent_episode: str | None = None
    premiere_date: str | None = None  # YYYY-MM-DD when available
    description: str | None = None
    external_ids: dict[str, Any] = field(default_factory=dict)
    imdb_meta: dict[str, Any] = field(default_factory=dict)
