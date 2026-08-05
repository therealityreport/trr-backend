"""Service layer for public core show reads."""

from __future__ import annotations

from typing import Any

from trr_backend.repositories import core_show_reads as repository


def search_shows(
    query: str,
    *,
    limit: int | None = None,
    offset: int | None = None,
) -> tuple[list[dict[str, Any]], int]:
    return repository.search_shows(query, limit=limit, offset=offset)


def get_show_by_id(show_id: str) -> tuple[dict[str, Any] | None, int]:
    return repository.get_show_by_id(show_id)


def get_seasons_by_show_id(
    show_id: str,
    *,
    limit: int | None = None,
    offset: int | None = None,
    include_episode_signal: bool = False,
) -> tuple[list[dict[str, Any]], int]:
    return repository.get_seasons_by_show_id(
        show_id,
        limit=limit,
        offset=offset,
        include_episode_signal=include_episode_signal,
    )


def get_season_by_id(season_id: str) -> tuple[dict[str, Any] | None, int]:
    return repository.get_season_by_id(season_id)


def get_season_by_show_and_number(show_id: str, season_number: int) -> tuple[dict[str, Any] | None, int]:
    return repository.get_season_by_show_and_number(show_id, season_number)


def get_episodes_by_season_id(
    season_id: str,
    *,
    limit: int | None = None,
    offset: int | None = None,
) -> tuple[list[dict[str, Any]], int]:
    return repository.get_episodes_by_season_id(season_id, limit=limit, offset=offset)


def get_episodes_by_show_and_season(
    show_id: str,
    season_number: int,
    *,
    limit: int | None = None,
    offset: int | None = None,
) -> tuple[list[dict[str, Any]], int]:
    return repository.get_episodes_by_show_and_season(
        show_id,
        season_number,
        limit=limit,
        offset=offset,
    )


def get_episode_by_id(episode_id: str) -> tuple[dict[str, Any] | None, int]:
    return repository.get_episode_by_id(episode_id)


def search_episodes(
    query: str,
    *,
    limit: int | None = None,
    offset: int | None = None,
) -> tuple[list[dict[str, Any]], int]:
    return repository.search_episodes(query, limit=limit, offset=offset)
