"""Admin v2 write primitives for shows and people.

These queries deliberately own the three remaining app-side SQL seams.  The
show mutation returns the same post-update row shape as the former app CTE in
one database operation, including canonical-slug collision handling and
featured-media URLs.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date
from typing import Any
from urllib.parse import parse_qs, urlparse

from psycopg2.extras import Json

from trr_backend.db import pg

_CANONICAL_PROFILE_SOURCES = ("imdb", "tmdb", "fandom", "manual")
_SHOW_PATCH_COLUMNS: dict[str, tuple[str, str]] = {
    "name": ("name", "text"),
    "slug": ("slug", "text"),
    "description": ("description", "text"),
    "premiere_date": ("premiere_date", "date"),
    "alternative_names": ("alternative_names", "text[]"),
    "imdb_id": ("imdb_id", "text"),
    "tmdb_id": ("tmdb_id", "int"),
    "external_ids": ("external_ids", "jsonb"),
    "genres": ("genres", "text[]"),
    "networks": ("networks", "text[]"),
    "streaming_providers": ("streaming_providers", "text[]"),
    "tags": ("tags", "text[]"),
    "primary_poster_image_id": ("primary_poster_image_id", "uuid"),
    "primary_backdrop_image_id": ("primary_backdrop_image_id", "uuid"),
    "primary_logo_image_id": ("primary_logo_image_id", "uuid"),
}
_SHOW_SLUG_SQL = """
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


def _adapt_show_patch_value(field_name: str, value: Any) -> Any:
    if field_name == "external_ids" and value is not None:
        return Json(value)
    if isinstance(value, date):
        return value.isoformat()
    return value


def update_show(show_id: str, patch: Mapping[str, Any]) -> tuple[dict[str, Any] | None, int]:
    """Patch a show and return its post-write detail row in one SQL statement."""

    updates: list[str] = []
    params: list[Any] = []
    for field_name, (column_name, postgres_type) in _SHOW_PATCH_COLUMNS.items():
        if field_name not in patch:
            continue
        params.append(_adapt_show_patch_value(field_name, patch[field_name]))
        updates.append(f"{column_name} = %s::{postgres_type}")

    if updates:
        params.append(show_id)
        target_cte = f"""
        updated_show AS (
          UPDATE core.shows AS updated
          SET {", ".join(updates)}
          WHERE updated.id = %s::uuid
          RETURNING updated.id
        )
        """
    else:
        params.append(show_id)
        target_cte = """
        updated_show AS (
          SELECT id
          FROM core.shows
          WHERE id = %s::uuid
        )
        """

    rows = pg.execute_returning(
        f"""
        WITH {target_cte},
        shows_with_slug AS (
          SELECT
            s.*,
            {_SHOW_SLUG_SQL} AS computed_slug,
            COUNT(*) OVER (PARTITION BY {_SHOW_SLUG_SQL}) AS slug_collision_count
          FROM core.shows AS s
        )
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
        JOIN updated_show AS updated ON updated.id = s.id
        LEFT JOIN core.show_images AS poster ON poster.id = s.primary_poster_image_id
        LEFT JOIN core.show_images AS backdrop ON backdrop.id = s.primary_backdrop_image_id
        LEFT JOIN core.show_images AS logo ON logo.id = s.primary_logo_image_id
        LIMIT 1
        """,
        params,
    )
    return (dict(rows[0]) if rows else None, 1)


def normalize_canonical_profile_source_order(source_order: Sequence[str]) -> list[str]:
    if len(source_order) != len(_CANONICAL_PROFILE_SOURCES):
        raise ValueError("source_order_must_include_all_sources")
    normalized = [str(value or "").strip().lower() for value in source_order]
    if len(set(normalized)) != len(_CANONICAL_PROFILE_SOURCES):
        raise ValueError("source_order_contains_duplicates")
    if set(normalized) != set(_CANONICAL_PROFILE_SOURCES):
        raise ValueError("source_order_contains_invalid_source")
    return normalized


def update_person_canonical_profile_source_order(
    person_id: str,
    source_order: Sequence[str],
) -> tuple[dict[str, Any] | None, int]:
    normalized = normalize_canonical_profile_source_order(source_order)
    rows = pg.execute_returning(
        """
        UPDATE core.people
        SET external_ids = jsonb_set(
          COALESCE(external_ids, '{}'::jsonb),
          '{canonical_profile_source_order}',
          to_jsonb(%s::text[]),
          true
        ),
        updated_at = now()
        WHERE id = %s::uuid
        RETURNING *
        """,
        [normalized, person_id],
    )
    return (dict(rows[0]) if rows else None, 1)


def _first_non_empty(*values: object) -> str | None:
    for value in values:
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _external_handle(external_ids: Mapping[str, Any] | None, keys: Sequence[str]) -> str | None:
    if not external_ids:
        return None
    return _first_non_empty(*(external_ids.get(key) for key in keys))


def _social_path_segments(value: str) -> tuple[list[str], dict[str, list[str]]] | None:
    parsed = urlparse(value)
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.netloc:
        return None
    segments = [segment.strip() for segment in parsed.path.strip("/").split("/") if segment.strip()]
    return segments, parse_qs(parsed.query)


def _normalize_social_handle(source: str, value: str | None) -> str | None:
    raw = _first_non_empty(value)
    if raw is None:
        return None
    parsed = _social_path_segments(raw)
    normalized = raw
    if parsed is not None:
        segments, query = parsed
        first = segments[0] if segments else raw
        if source == "facebook":
            if first.lower() == "profile.php":
                normalized = _first_non_empty(*(query.get("id") or [])) or raw
            elif first.lower() == "people" and len(segments) > 2:
                normalized = segments[2]
            elif first.lower() == "pg" and len(segments) > 1:
                normalized = segments[1]
            else:
                normalized = first
        elif source == "twitter":
            normalized = _first_non_empty(*(query.get("screen_name") or [])) or first
        elif source in {"instagram", "tiktok"}:
            normalized = first
        elif source == "youtube":
            if first.startswith("@"):
                normalized = first
            elif first.lower() == "channel" and len(segments) > 1:
                normalized = segments[1]
            elif first.lower() in {"user", "c"} and len(segments) > 1:
                normalized = f"{first.lower()}/{segments[1]}"
            else:
                normalized = segments[1] if len(segments) > 1 else first
    if source != "youtube":
        normalized = normalized.lstrip("@")
    normalized = normalized.strip()
    return normalized or None


def _effective_social_handles(person_id: str, row: Mapping[str, Any] | None) -> dict[str, Any]:
    external_ids = row.get("external_ids") if row and isinstance(row.get("external_ids"), Mapping) else None
    return {
        "person_id": person_id,
        "facebook_handle": _normalize_social_handle(
            "facebook", _external_handle(external_ids, ("facebook_id", "facebook", "facebook_handle"))
        ),
        "instagram_handle": _normalize_social_handle(
            "instagram",
            _first_non_empty(
                row.get("instagram_override") if row else None,
                _external_handle(external_ids, ("instagram_id", "instagram", "instagram_handle")),
            ),
        ),
        "tiktok_handle": _normalize_social_handle(
            "tiktok",
            _first_non_empty(
                row.get("tiktok_override") if row else None,
                _external_handle(external_ids, ("tiktok_id", "tiktok", "tiktok_handle")),
            ),
        ),
        "twitter_handle": _normalize_social_handle(
            "twitter",
            _first_non_empty(
                row.get("twitter_override") if row else None,
                _external_handle(external_ids, ("twitter_id", "twitter", "twitter_handle", "x_id", "x_handle", "x")),
            ),
        ),
        "youtube_handle": _normalize_social_handle(
            "youtube",
            _first_non_empty(
                row.get("youtube_override") if row else None,
                _external_handle(external_ids, ("youtube_id", "youtube", "youtube_handle")),
            ),
        ),
    }


def list_effective_person_social_handles(person_ids: Sequence[str]) -> tuple[list[dict[str, Any]], int]:
    unique_person_ids = list(dict.fromkeys(person_id.strip() for person_id in person_ids if person_id.strip()))
    if not unique_person_ids:
        return [], 0
    rows = pg.fetch_all(
        """
        SELECT
          p.id::text AS person_id,
          p.external_ids,
          po.instagram_handle AS instagram_override,
          po.tiktok_handle AS tiktok_override,
          po.twitter_handle AS twitter_override,
          po.youtube_handle AS youtube_override
        FROM core.people AS p
        LEFT JOIN core.people_overrides AS po ON po.person_id = p.id
        WHERE p.id = ANY(%s::uuid[])
        """,
        [unique_person_ids],
    )
    rows_by_person_id = {str(row.get("person_id")): row for row in rows}
    handles = [
        _effective_social_handles(person_id, rows_by_person_id.get(person_id)) for person_id in unique_person_ids
    ]
    return handles, 1
