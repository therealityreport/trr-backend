from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping


@dataclass
class AggregatedCastMember:
    key: str
    min_idx: int
    cast_member_name: str | None
    person_id: str | None
    show_name: str | None
    show_id: str | None
    imdb_show_id: str | None
    tmdb_show_id: str | None
    seasons: list[Any] = field(default_factory=list)
    tmdb_season_ids: list[Any] = field(default_factory=list)
    imdb_episode_title_ids: list[Any] = field(default_factory=list)
    tmdb_episode_ids: list[Any] = field(default_factory=list)
    total_episodes: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "cast_member_name": self.cast_member_name,
            "person_id": self.person_id,
            "show_name": self.show_name,
            "show_id": self.show_id,
            "imdb_show_id": self.imdb_show_id,
            "tmdb_show_id": self.tmdb_show_id,
            "seasons": list(self.seasons),
            "tmdb_season_ids": list(self.tmdb_season_ids),
            "imdb_episode_title_ids": list(self.imdb_episode_title_ids),
            "tmdb_episode_ids": list(self.tmdb_episode_ids),
            "total_episodes": self.total_episodes,
        }


def _normalize_name(name: str | None) -> str:
    return (name or "").strip().casefold()


def _parse_idx(value: Any, fallback: int) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.isdigit():
            return int(stripped)
    return fallback


def _coerce_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return list(value)
    if isinstance(value, tuple):
        return list(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return []
        return [stripped]
    return [value]


def _merge_unique(existing: list[Any], additions: Iterable[Any]) -> list[Any]:
    seen = set()
    merged: list[Any] = []
    for item in existing:
        key = item if isinstance(item, (str, int, float, bool)) else repr(item)
        if key in seen:
            continue
        seen.add(key)
        merged.append(item)
    for item in additions:
        key = item if isinstance(item, (str, int, float, bool)) else repr(item)
        if key in seen:
            continue
        seen.add(key)
        merged.append(item)
    return merged


def aggregate_episode_appearances(
    rows: Iterable[Mapping[str, Any]],
    *,
    imdb_show_id: str,
) -> list[AggregatedCastMember]:
    grouped: dict[str, AggregatedCastMember] = {}

    for fallback_idx, row in enumerate(rows):
        if not isinstance(row, Mapping):
            continue
        if str(row.get("imdb_show_id") or "").strip() != imdb_show_id:
            continue

        person_id = str(row.get("person_id") or "").strip() or None
        name = str(row.get("cast_member_name") or "").strip() or None
        if not person_id and not name:
            continue
        key = f"person:{person_id}" if person_id else f"name:{_normalize_name(name)}"

        idx_value = _parse_idx(row.get("idx"), fallback_idx)

        entry = grouped.get(key)
        if entry is None:
            entry = AggregatedCastMember(
                key=key,
                min_idx=idx_value,
                cast_member_name=name,
                person_id=person_id,
                show_name=str(row.get("show_name") or "").strip() or None,
                show_id=str(row.get("show_id") or "").strip() or None,
                imdb_show_id=str(row.get("imdb_show_id") or "").strip() or None,
                tmdb_show_id=str(row.get("tmdb_show_id") or "").strip() or None,
            )
            grouped[key] = entry
        elif idx_value < entry.min_idx:
            entry.min_idx = idx_value
            entry.cast_member_name = name or entry.cast_member_name
            entry.person_id = person_id or entry.person_id
            entry.show_name = str(row.get("show_name") or "").strip() or entry.show_name
            entry.show_id = str(row.get("show_id") or "").strip() or entry.show_id
            entry.imdb_show_id = str(row.get("imdb_show_id") or "").strip() or entry.imdb_show_id
            entry.tmdb_show_id = str(row.get("tmdb_show_id") or "").strip() or entry.tmdb_show_id

        entry.seasons = _merge_unique(entry.seasons, _coerce_list(row.get("seasons")))
        entry.tmdb_season_ids = _merge_unique(entry.tmdb_season_ids, _coerce_list(row.get("tmdb_season_ids")))
        entry.imdb_episode_title_ids = _merge_unique(
            entry.imdb_episode_title_ids,
            _coerce_list(row.get("imdb_episode_title_ids")),
        )
        entry.tmdb_episode_ids = _merge_unique(entry.tmdb_episode_ids, _coerce_list(row.get("tmdb_episode_ids")))

        total_episodes = row.get("total_episodes")
        try:
            total_int = int(total_episodes) if total_episodes is not None else None
        except (TypeError, ValueError):
            total_int = None
        if total_int is not None:
            entry.total_episodes = max(entry.total_episodes or 0, total_int)

    return sorted(grouped.values(), key=lambda item: item.min_idx)
