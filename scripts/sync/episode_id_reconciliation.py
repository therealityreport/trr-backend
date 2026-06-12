from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import requests

from trr_backend.integrations.tmdb.client import TmdbClientError, fetch_tv_episode_external_ids

_IMDB_TITLE_ID_RE = re.compile(r"^tt\d+$", re.IGNORECASE)
_TITLE_TOKEN_RE = re.compile(r"[^a-z0-9]+")


@dataclass(frozen=True)
class EpisodeIdMatch:
    episode_id: str
    imdb_episode_id: str
    strategy: str


def _coerce_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    return None


def _valid_imdb_episode_id(value: object) -> str | None:
    text = str(value or "").strip()
    if not _IMDB_TITLE_ID_RE.match(text):
        return None
    return text


def _normalize_title(value: object) -> str:
    text = str(value or "").strip().casefold().replace("&", " and ")
    text = re.sub(r"\bpart\s+(\d+)\b", r"\1", text)
    return _TITLE_TOKEN_RE.sub(" ", text).strip()


def _air_year(value: object) -> int | None:
    text = str(value or "").strip()
    if len(text) < 4 or not text[:4].isdigit():
        return None
    return int(text[:4])


def _episode_imdb_id(row: Mapping[str, Any]) -> str | None:
    direct = _valid_imdb_episode_id(row.get("imdb_episode_id"))
    if direct:
        return direct
    external_ids = row.get("external_ids")
    if isinstance(external_ids, Mapping):
        return _valid_imdb_episode_id(external_ids.get("imdb"))
    return None


def merge_external_ids(existing: object, updates: Mapping[str, object]) -> dict[str, object] | None:
    merged = dict(existing) if isinstance(existing, Mapping) else {}
    changed = False
    for key, raw_value in updates.items():
        value = raw_value
        if isinstance(value, str):
            value = value.strip()
        if value in (None, ""):
            continue
        if merged.get(key) != value:
            merged[key] = value
            changed = True
    return merged if changed else None


def safe_match_episode_ref(
    local_episodes: Sequence[Mapping[str, Any]],
    *,
    imdb_episode_id: str,
    season_number: int | None = None,
    episode_number: int | None = None,
    title: str | None = None,
    air_date: str | None = None,
    year: int | None = None,
) -> EpisodeIdMatch | None:
    imdb_id = _valid_imdb_episode_id(imdb_episode_id)
    if not imdb_id:
        return None

    def compatible(row: Mapping[str, Any]) -> bool:
        local_season_number = _coerce_int(row.get("season_number"))
        if local_season_number == 0 and (season_number is None or season_number <= 0):
            return False
        existing_imdb_id = _episode_imdb_id(row)
        return existing_imdb_id in (None, imdb_id)

    if season_number is not None and episode_number is not None:
        numbered = [
            row
            for row in local_episodes
            if compatible(row)
            and _coerce_int(row.get("season_number")) == season_number
            and _coerce_int(row.get("episode_number")) == episode_number
        ]
        if len(numbered) == 1:
            return EpisodeIdMatch(
                episode_id=str(numbered[0].get("id") or ""),
                imdb_episode_id=imdb_id,
                strategy="season_episode",
            )

    title_key = _normalize_title(title)
    if not title_key:
        return None

    target_year = _air_year(air_date) or year
    title_matches = [
        row for row in local_episodes if compatible(row) and _normalize_title(row.get("title")) == title_key
    ]
    if target_year is not None:
        title_matches = [row for row in title_matches if _air_year(row.get("air_date")) == target_year]

    if len(title_matches) == 1:
        return EpisodeIdMatch(
            episode_id=str(title_matches[0].get("id") or ""),
            imdb_episode_id=imdb_id,
            strategy="title_year",
        )

    return None


def update_episode_imdb_id(db, *, episode: Mapping[str, Any], imdb_episode_id: str) -> bool:
    imdb_id = _valid_imdb_episode_id(imdb_episode_id)
    episode_id = str(episode.get("id") or "").strip()
    if not imdb_id or not episode_id:
        return False

    existing_imdb_id = _episode_imdb_id(episode)
    if existing_imdb_id and existing_imdb_id != imdb_id:
        return False

    merged_external_ids = merge_external_ids(episode.get("external_ids"), {"imdb": imdb_id})
    patch: dict[str, object] = {"imdb_episode_id": imdb_id}
    if merged_external_ids is not None:
        patch["external_ids"] = merged_external_ids

    response = db.schema("core").table("episodes").update(patch).eq("id", episode_id).execute()
    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Supabase error updating episode imdb id episode_id={episode_id}: {response.error}")
    return True


def reconcile_episode_imdb_ids_from_refs(
    db,
    *,
    local_episodes: Sequence[Mapping[str, Any]],
    refs: Sequence[Mapping[str, Any]],
) -> int:
    by_id = {str(row.get("id") or ""): row for row in local_episodes if str(row.get("id") or "")}
    updated = 0
    for ref in refs:
        match = safe_match_episode_ref(
            local_episodes,
            imdb_episode_id=str(ref.get("imdb_episode_id") or ref.get("title_id") or ""),
            season_number=_coerce_int(ref.get("season_number")),
            episode_number=_coerce_int(ref.get("episode_number")),
            title=str(ref.get("title") or "").strip() or None,
            air_date=str(ref.get("air_date") or "").strip() or None,
            year=_coerce_int(ref.get("year")),
        )
        if match is None:
            continue
        episode = by_id.get(match.episode_id)
        if episode is not None and update_episode_imdb_id(db, episode=episode, imdb_episode_id=match.imdb_episode_id):
            updated += 1
    return updated


def reconcile_episode_imdb_ids_from_tmdb(
    db,
    *,
    show_id: str,
    tmdb_series_id: int,
    episodes: Sequence[Mapping[str, Any]],
    api_key: str | None,
    session: requests.Session | None = None,
    verbose: bool = False,
) -> int:
    if not api_key:
        return 0

    session = session or requests.Session()
    updated = 0
    cache: dict[tuple[int, int, int], dict[str, Any]] = {}
    for episode in episodes:
        if _episode_imdb_id(episode):
            continue
        if not _coerce_int(episode.get("tmdb_episode_id")):
            continue
        season_number = _coerce_int(episode.get("season_number"))
        episode_number = _coerce_int(episode.get("episode_number"))
        if season_number is None or episode_number is None:
            continue
        if season_number == 0:
            continue

        key = (int(tmdb_series_id), int(season_number), int(episode_number))
        try:
            payload = cache.get(key)
            if payload is None:
                payload = fetch_tv_episode_external_ids(
                    int(tmdb_series_id),
                    int(season_number),
                    int(episode_number),
                    api_key=api_key,
                    session=session,
                )
                cache[key] = payload
        except TmdbClientError as exc:
            if verbose:
                print(
                    "Episode IMDb ID reconciliation: TMDb external_ids failed "
                    f"show_id={show_id} season={season_number} episode={episode_number} http={exc.status_code}"
                )
            continue
        imdb_id = _valid_imdb_episode_id(payload.get("imdb_id"))
        if not imdb_id:
            continue
        if update_episode_imdb_id(db, episode=episode, imdb_episode_id=imdb_id):
            updated += 1

    return updated
