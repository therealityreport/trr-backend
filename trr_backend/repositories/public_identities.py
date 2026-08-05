"""Database reads for stable public show, season, and person identities."""

from __future__ import annotations

from typing import Any

from trr_backend.db import pg

_SHOW_CANDIDATES_SQL = """
SELECT
  s.id::text AS show_id,
  s.name AS show_name,
  canonical.slug AS canonical_slug,
  bool_or(matched.is_canonical) AS matched_is_canonical
FROM core.show_slug_aliases AS matched
JOIN core.shows AS s
  ON s.id = matched.show_id
JOIN core.show_slug_aliases AS canonical
  ON canonical.show_id = s.id
 AND canonical.is_canonical = true
WHERE matched.slug = %s
GROUP BY s.id, s.name, canonical.slug
ORDER BY bool_or(matched.is_canonical) DESC, canonical.slug ASC, s.id ASC
"""

_SHOW_BY_ID_SQL = """
SELECT
  s.id::text AS show_id,
  s.name AS show_name,
  canonical.slug AS canonical_slug,
  true AS matched_is_canonical
FROM core.shows AS s
JOIN core.show_slug_aliases AS canonical
  ON canonical.show_id = s.id
 AND canonical.is_canonical = true
WHERE s.id = %s::uuid
LIMIT 1
"""

_SEASON_SQL = """
SELECT
  season.id::text AS season_id,
  season.show_id::text AS show_id,
  season.season_number,
  COALESCE(season.name, season.title) AS season_title
FROM core.seasons AS season
WHERE season.show_id = %s::uuid
  AND season.season_number = %s
LIMIT 1
"""

_PERSON_CANDIDATES_SQL = """
WITH context AS (
  SELECT %s::uuid AS show_id
)
SELECT
  person.id::text AS person_id,
  person.full_name,
  canonical.slug AS canonical_slug,
  bool_or(matched.is_canonical) AS matched_is_canonical
FROM core.person_slug_aliases AS matched
JOIN core.people AS person
  ON person.id = matched.person_id
JOIN core.person_slug_aliases AS canonical
  ON canonical.person_id = person.id
 AND canonical.is_canonical = true
CROSS JOIN context
WHERE matched.slug = %s
  AND (
    context.show_id IS NULL
    OR EXISTS (
      SELECT 1
      FROM core.v_show_cast AS show_cast
      WHERE show_cast.show_id = context.show_id
        AND show_cast.person_id = person.id
    )
  )
GROUP BY person.id, person.full_name, canonical.slug
ORDER BY bool_or(matched.is_canonical) DESC, canonical.slug ASC, person.id ASC
"""


def _to_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _map_show_candidate(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "show_id": str(row["show_id"]),
        "show_name": str(row["show_name"]),
        "canonical_slug": str(row["canonical_slug"]),
        "matched_is_canonical": _to_bool(row.get("matched_is_canonical")),
    }


def list_show_slug_candidates(slug: str) -> list[dict[str, Any]]:
    """Return every show matching a direct alias, in deterministic priority order."""
    return [_map_show_candidate(row) for row in pg.fetch_all(_SHOW_CANDIDATES_SQL, [slug])]


def get_show_identity_by_id(show_id: str) -> dict[str, Any] | None:
    """Return a show and its canonical alias by UUID."""
    row = pg.fetch_one(_SHOW_BY_ID_SQL, [show_id])
    return _map_show_candidate(row) if row is not None else None


def get_season_identity(*, show_id: str, season_number: int) -> dict[str, Any] | None:
    """Return the season identified by a show UUID and season number."""
    row = pg.fetch_one(_SEASON_SQL, [show_id, season_number])
    if row is None:
        return None
    return {
        "season_id": str(row["season_id"]),
        "show_id": str(row["show_id"]),
        "season_number": int(row["season_number"]),
        "season_title": str(row["season_title"]) if row.get("season_title") is not None else None,
    }


def list_person_slug_candidates(*, slug: str, show_id: str | None = None) -> list[dict[str, Any]]:
    """Return direct person-alias matches, optionally limited to one show's cast."""
    rows = pg.fetch_all(_PERSON_CANDIDATES_SQL, [show_id, slug])
    return [
        {
            "person_id": str(row["person_id"]),
            "full_name": str(row["full_name"]),
            "canonical_slug": str(row["canonical_slug"]),
            "matched_is_canonical": _to_bool(row.get("matched_is_canonical")),
        }
        for row in rows
    ]
