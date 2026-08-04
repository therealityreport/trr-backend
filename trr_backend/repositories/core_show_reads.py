"""Public core show read queries for backend API adapters."""

from __future__ import annotations

from typing import Any

from trr_backend.db import pg

DEFAULT_LIMIT = 20
MAX_LIMIT = 500

SHOW_SLUG_SQL = """
lower(
  trim(
    both '-' FROM regexp_replace(
      regexp_replace(COALESCE(s.name, ''), '&', ' and ', 'gi'),
      '[^a-z0-9]+',
      '-',
      'gi'
    )
  )
)
"""

SHOW_WITH_SLUG_CTE = f"""
WITH shows_with_slug AS (
  SELECT
    s.*,
    {SHOW_SLUG_SQL} AS computed_slug,
    COUNT(*) OVER (PARTITION BY {SHOW_SLUG_SQL}) AS slug_collision_count
  FROM core.shows AS s
)
"""

SHOW_SELECT_SQL = """
SELECT
  s.*,
  CASE
    WHEN s.slug_collision_count > 1
      THEN COALESCE(NULLIF(s.slug, ''), s.computed_slug) || '--' || lower(left(s.id::text, 8))
    ELSE COALESCE(NULLIF(s.slug, ''), s.computed_slug)
  END AS canonical_slug,
  poster.hosted_url AS poster_url,
  backdrop.hosted_url AS backdrop_url,
  logo.hosted_url AS logo_url
FROM shows_with_slug AS s
LEFT JOIN core.show_images AS poster ON poster.id = s.primary_poster_image_id
LEFT JOIN core.show_images AS backdrop ON backdrop.id = s.primary_backdrop_image_id
LEFT JOIN core.show_images AS logo ON logo.id = s.primary_logo_image_id
"""

EPISODE_SEARCH_SHOWS_CTE = f"""
WITH shows_with_slug AS (
  SELECT
    s.id,
    s.name,
    s.slug,
    {SHOW_SLUG_SQL} AS computed_slug,
    COUNT(*) OVER (PARTITION BY {SHOW_SLUG_SQL}) AS slug_collision_count
  FROM core.shows AS s
)
"""


class CoreShowReadRepositoryError(RuntimeError):
    """Raised when public core show reads cannot be completed."""


def normalize_pagination(limit: int | None = None, offset: int | None = None) -> tuple[int, int]:
    normalized_limit = min(max(limit if limit is not None else DEFAULT_LIMIT, 1), MAX_LIMIT)
    normalized_offset = max(offset if offset is not None else 0, 0)
    return normalized_limit, normalized_offset


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def normalize_show_row(row: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(row)
    for key in (
        "alternative_names",
        "overview_alternative_names",
        "genres",
        "networks",
        "overview_networks",
        "streaming_providers",
        "overview_streaming_providers",
        "overview_watch_availability",
        "watch_provider_regions",
        "watch_providers",
        "tags",
    ):
        normalized[key] = _list(normalized.get(key))
    return normalized


def search_shows(
    query: str,
    *,
    limit: int | None = None,
    offset: int | None = None,
) -> tuple[list[dict[str, Any]], int]:
    normalized_limit, normalized_offset = normalize_pagination(limit, offset)
    like = f"%{query}%"
    rows = pg.fetch_all(
        f"""
        {SHOW_WITH_SLUG_CTE}
        {SHOW_SELECT_SQL}
        WHERE s.name ILIKE %s
           OR EXISTS (
             SELECT 1
             FROM unnest(COALESCE(s.alternative_names, ARRAY[]::text[])) AS alt(name)
             WHERE alt.name ILIKE %s
           )
        ORDER BY s.name ASC
        LIMIT %s OFFSET %s
        """,
        [like, like, normalized_limit, normalized_offset],
    )
    return [normalize_show_row(row) for row in rows], 1


def get_show_by_id(show_id: str) -> tuple[dict[str, Any] | None, int]:
    row = pg.fetch_one(
        f"""
        {SHOW_WITH_SLUG_CTE}
        {SHOW_SELECT_SQL}
        WHERE s.id = %s::uuid
        LIMIT 1
        """,
        [show_id],
    )
    return (normalize_show_row(row) if row else None), 1


def get_seasons_by_show_id(
    show_id: str,
    *,
    limit: int | None = None,
    offset: int | None = None,
    include_episode_signal: bool = False,
) -> tuple[list[dict[str, Any]], int]:
    normalized_limit, normalized_offset = normalize_pagination(limit, offset)
    if include_episode_signal:
        rows = pg.fetch_all(
            """
            SELECT s.*,
                   COALESCE(ep.episode_airdate_count, 0)::int AS episode_airdate_count,
                   (COALESCE(ep.episode_airdate_count, 0) > 0) AS has_scheduled_or_aired_episode
              FROM core.seasons AS s
              LEFT JOIN LATERAL (
                SELECT COUNT(*)::int AS episode_airdate_count
                  FROM core.episodes AS e
                 WHERE e.season_id = s.id
                   AND e.air_date IS NOT NULL
              ) AS ep ON TRUE
             WHERE s.show_id = %s::uuid
             ORDER BY s.season_number DESC
             LIMIT %s OFFSET %s
            """,
            [show_id, normalized_limit, normalized_offset],
        )
        return rows, 1
    rows = pg.fetch_all(
        """
        SELECT *
          FROM core.seasons
         WHERE show_id = %s::uuid
         ORDER BY season_number DESC
         LIMIT %s OFFSET %s
        """,
        [show_id, normalized_limit, normalized_offset],
    )
    return rows, 1


def get_season_by_id(season_id: str) -> tuple[dict[str, Any] | None, int]:
    row = pg.fetch_one(
        """
        SELECT *
          FROM core.seasons
         WHERE id = %s::uuid
         LIMIT 1
        """,
        [season_id],
    )
    return row, 1


def get_season_by_show_and_number(show_id: str, season_number: int) -> tuple[dict[str, Any] | None, int]:
    row = pg.fetch_one(
        """
        SELECT *
          FROM core.seasons
         WHERE show_id = %s::uuid
           AND season_number = %s::int
         LIMIT 1
        """,
        [show_id, int(season_number)],
    )
    return row, 1


def get_episodes_by_season_id(
    season_id: str,
    *,
    limit: int | None = None,
    offset: int | None = None,
) -> tuple[list[dict[str, Any]], int]:
    normalized_limit, normalized_offset = normalize_pagination(limit, offset)
    rows = pg.fetch_all(
        """
        SELECT *
          FROM core.episodes
         WHERE season_id = %s::uuid
         ORDER BY episode_number ASC
         LIMIT %s OFFSET %s
        """,
        [season_id, normalized_limit, normalized_offset],
    )
    return rows, 1


def get_episodes_by_show_and_season(
    show_id: str,
    season_number: int,
    *,
    limit: int | None = None,
    offset: int | None = None,
) -> tuple[list[dict[str, Any]], int]:
    normalized_limit, normalized_offset = normalize_pagination(limit, offset)
    rows = pg.fetch_all(
        """
        SELECT *
          FROM core.episodes
         WHERE show_id = %s::uuid
           AND season_number = %s::int
         ORDER BY episode_number ASC
         LIMIT %s OFFSET %s
        """,
        [show_id, int(season_number), normalized_limit, normalized_offset],
    )
    return rows, 1


def get_episode_by_id(episode_id: str) -> tuple[dict[str, Any] | None, int]:
    row = pg.fetch_one(
        """
        SELECT *
          FROM core.episodes
         WHERE id = %s::uuid
         LIMIT 1
        """,
        [episode_id],
    )
    return row, 1


def search_episodes(
    query: str,
    *,
    limit: int | None = None,
    offset: int | None = None,
) -> tuple[list[dict[str, Any]], int]:
    normalized_limit, normalized_offset = normalize_pagination(limit, offset)
    like = f"%{query}%"
    like_prefix = f"{query}%"
    rows = pg.fetch_all(
        f"""
        {EPISODE_SEARCH_SHOWS_CTE}
        SELECT
          e.id,
          e.title,
          e.episode_number,
          e.season_number,
          e.air_date,
          e.show_id,
          sws.name AS show_name,
          CASE
            WHEN sws.slug_collision_count > 1
              THEN COALESCE(NULLIF(sws.slug, ''), sws.computed_slug) || '--' || lower(left(sws.id::text, 8))
            ELSE COALESCE(NULLIF(sws.slug, ''), sws.computed_slug)
          END AS show_slug
          FROM core.episodes AS e
          JOIN shows_with_slug AS sws ON sws.id = e.show_id
         WHERE COALESCE(e.title, '') ILIKE %s
            OR CONCAT('episode ', COALESCE(e.episode_number::text, '')) ILIKE %s
            OR CONCAT('s', COALESCE(e.season_number::text, ''), 'e', COALESCE(e.episode_number::text, '')) ILIKE %s
         ORDER BY
           CASE WHEN COALESCE(e.title, '') ILIKE %s THEN 0 ELSE 1 END,
           e.air_date DESC NULLS LAST,
           e.updated_at DESC NULLS LAST,
           e.id ASC
         LIMIT %s OFFSET %s
        """,
        [like, like, like, like_prefix, normalized_limit, normalized_offset],
    )
    return rows, 1
