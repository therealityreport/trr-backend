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
    title: str
    description: str | None = None
    premiere_date: str | None = None
    external_ids: Mapping[str, Any] = field(default_factory=dict)

    @property
    def imdb_id(self) -> str | None:
        value = self.external_ids.get("imdb")
        return value if isinstance(value, str) and value else None

    @property
    def tmdb_id(self) -> int | None:
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
    title: str
    tmdb_id: int | None = None
    premiere_date: str | None = None  # YYYY-MM-DD when available
    description: str | None = None
    external_ids: dict[str, Any] = field(default_factory=dict)
