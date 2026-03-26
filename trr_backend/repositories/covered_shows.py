from __future__ import annotations

from typing import Any

from trr_backend.db import pg

_COVERED_SHOWS_QUERY = """
WITH covered AS (
  SELECT
    cs.id::text AS id,
    cs.trr_show_id::text AS trr_show_id,
    cs.show_name,
    s.name AS core_show_name,
    s.slug,
    COALESCE(s.alternative_names, ARRAY[]::text[]) AS alternative_names,
    s.show_total_episodes,
    si.hosted_url AS poster_url,
    lower(
      trim(
        both '-' FROM regexp_replace(
          regexp_replace(COALESCE(s.name, ''), '&', ' and ', 'gi'),
          '[^a-z0-9]+',
          '-',
          'gi'
        )
      )
    ) AS computed_slug
  FROM admin.covered_shows AS cs
  LEFT JOIN core.shows AS s
    ON s.id = cs.trr_show_id
  LEFT JOIN core.show_images AS si
    ON si.id = s.primary_poster_image_id
),
ranked AS (
  SELECT
    id,
    trr_show_id,
    show_name,
    alternative_names,
    show_total_episodes,
    poster_url,
    CASE
      WHEN COALESCE(NULLIF(trim(slug), ''), NULLIF(computed_slug, '')) IS NULL
        THEN NULL
      WHEN COUNT(*) OVER (PARTITION BY computed_slug) > 1
        THEN COALESCE(NULLIF(trim(slug), ''), computed_slug) || '--' || lower(left(trr_show_id, 8))
      ELSE COALESCE(NULLIF(trim(slug), ''), computed_slug)
    END AS canonical_slug
  FROM covered
)
SELECT
  id,
  trr_show_id,
  show_name,
  canonical_slug,
  alternative_names,
  show_total_episodes,
  poster_url
FROM ranked
"""


def _listify_text_array(value: Any) -> list[str] | None:
    if not isinstance(value, list):
        return None
    items = [item for item in value if isinstance(item, str) and item.strip()]
    return items or None


def _map_covered_show_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": row["id"],
        "trr_show_id": row["trr_show_id"],
        "show_name": row["show_name"],
        "canonical_slug": row.get("canonical_slug"),
        "alternative_names": _listify_text_array(row.get("alternative_names")),
        "show_total_episodes": row.get("show_total_episodes"),
        "poster_url": row.get("poster_url"),
    }


def list_covered_shows() -> tuple[list[dict[str, Any]], int]:
    rows = pg.fetch_all(f"{_COVERED_SHOWS_QUERY}\nORDER BY show_name ASC")
    return [_map_covered_show_row(row) for row in rows], 1


def get_covered_show(show_id: str) -> tuple[dict[str, Any] | None, int]:
    row = pg.fetch_one(
        f"{_COVERED_SHOWS_QUERY}\nWHERE trr_show_id = %s\nLIMIT 1",
        [show_id],
    )
    if row is None:
        return None, 1
    return _map_covered_show_row(row), 1


def add_covered_show(*, show_id: str, show_name: str, actor_uid: str) -> tuple[dict[str, Any], int]:
    pg.execute(
        """
        insert into admin.covered_shows (
          trr_show_id,
          show_name,
          created_by_firebase_uid
        ) values (%s::uuid, %s, %s)
        on conflict (trr_show_id) do update
        set show_name = excluded.show_name
        """,
        [show_id, show_name, actor_uid],
    )
    show, read_query_count = get_covered_show(show_id)
    if show is None:
        raise RuntimeError("Failed to load covered show after add")
    return show, 1 + read_query_count


def remove_covered_show(show_id: str) -> tuple[bool, int]:
    rows = pg.execute_returning(
        """
        delete from admin.covered_shows
        where trr_show_id = %s::uuid
        returning id
        """,
        [show_id],
    )
    return bool(rows), 1
