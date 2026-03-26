from __future__ import annotations

import re
import unicodedata
from typing import Any

from trr_backend.db import pg

UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I)
PERSON_SLUG_SUFFIX_RE = re.compile(r"--([0-9a-f]{8})$", re.I)
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


def _slugify_token(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value or "")
    normalized = "".join(char for char in normalized if not unicodedata.combining(char))
    normalized = normalized.strip().lower().replace("&", " and ")
    normalized = re.sub(r"[^a-z0-9]+", "-", normalized)
    return normalized.strip("-")


def _dedupe_text_values(values: list[str | None]) -> list[str]:
    seen: set[str] = set()
    items: list[str] = []
    for value in values:
        normalized = (value or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        items.append(normalized)
    return items


def _build_candidate_person_full_names(slug: str) -> list[str]:
    tokens = [token.strip() for token in slug.split("-") if token.strip()]
    if not tokens:
        return []
    titled = [token[:1].upper() + token[1:] for token in tokens]
    ampersand = ["&" if token.lower() == "and" else token for token in titled]
    return _dedupe_text_values(
        [
            " ".join(titled),
            "-".join(titled),
            " ".join(ampersand),
            "-".join(ampersand),
        ]
    )


def _build_show_slug_candidates(raw_slug: str) -> list[str]:
    normalized = _slugify_token(raw_slug)
    if not normalized:
        return []
    values = [normalized]
    if normalized.startswith("the-"):
        without_article = normalized[4:].strip()
        if without_article:
            values.append(without_article)
    else:
        values.append(f"the-{normalized}")
    return list(dict.fromkeys(value for value in values if value))


def _pick_preferred_show_alias_slug(alternative_names: list[str] | None) -> str | None:
    if not isinstance(alternative_names, list) or not alternative_names:
        return None
    normalized = [_slugify_token(value) for value in alternative_names if isinstance(value, str)]
    normalized = [value for value in normalized if value]
    if not normalized:
        return None
    for value in normalized:
        if re.match(r"^rh[a-z0-9]{2,}$", value, re.I) and "-" not in value:
            return value
    return normalized[0]


def resolve_show_slug(slug: str) -> dict[str, Any] | None:
    raw_suffix_match = PERSON_SLUG_SUFFIX_RE.search(slug or "")
    requested_prefix = raw_suffix_match.group(1).lower() if raw_suffix_match else None
    raw_base = slug[: -len(raw_suffix_match.group(0))] if raw_suffix_match else slug
    for base_slug in _build_show_slug_candidates(raw_base):
        rows = pg.fetch_all(
            f"""
            WITH shows_with_slug AS (
              SELECT
                s.id::text AS id,
                s.name,
                COALESCE(s.alternative_names, ARRAY[]::text[]) AS alternative_names,
                {SHOW_SLUG_SQL} AS computed_slug,
                COALESCE(
                  NULLIF(
                    lower(
                      trim(
                        both '-' FROM regexp_replace(
                          regexp_replace(COALESCE(s.slug, ''), '&', ' and ', 'gi'),
                          '[^a-z0-9]+',
                          '-',
                          'gi'
                        )
                      )
                    ),
                    ''
                  ),
                  {SHOW_SLUG_SQL}
                ) AS effective_slug
              FROM core.shows AS s
            )
            SELECT
              s.id,
              s.name,
              s.alternative_names,
              s.effective_slug AS slug,
              CASE
                WHEN COUNT(*) OVER (PARTITION BY s.effective_slug) > 1
                  THEN s.effective_slug || '--' || lower(left(s.id, 8))
                ELSE s.effective_slug
              END AS canonical_slug
            FROM shows_with_slug AS s
            WHERE (
              s.effective_slug = %s
              OR s.computed_slug = %s
              OR EXISTS (
                SELECT 1
                FROM unnest(s.alternative_names) AS alt(name)
                WHERE lower(
                  trim(
                    both '-' FROM regexp_replace(
                      regexp_replace(COALESCE(alt.name, ''), '&', ' and ', 'gi'),
                      '[^a-z0-9]+',
                      '-',
                      'gi'
                    )
                  )
                ) = %s
              )
            )
            ORDER BY s.id ASC
            """,
            [base_slug, base_slug, base_slug],
        )
        if not rows:
            continue
        selected = rows[0]
        if requested_prefix:
            selected = next((row for row in rows if row["id"].lower().startswith(requested_prefix)), None)
            if not selected:
                continue
        preferred_slug = (
            _pick_preferred_show_alias_slug(selected.get("alternative_names"))
            or selected.get("slug")
            or base_slug
        )
        has_collision = len(rows) > 1
        return {
            "show_id": selected["id"],
            "slug": preferred_slug,
            "canonical_slug": f"{preferred_slug}--{selected['id'][:8].lower()}" if has_collision else preferred_slug,
            "show_name": selected["name"],
        }
    return None


def resolve_person_slug(slug: str, show_input: str | None = None) -> tuple[dict[str, Any] | None, str | None, int]:
    raw_slug = slug or ""
    raw_suffix_match = PERSON_SLUG_SUFFIX_RE.search(raw_slug)
    requested_prefix = raw_suffix_match.group(1).lower() if raw_suffix_match else None
    raw_base = raw_slug[: -len(raw_suffix_match.group(0))] if raw_suffix_match else raw_slug
    base_slug = _slugify_token(raw_base)
    if not base_slug:
        return None, None, 0

    query_count = 0
    resolved_show_id: str | None = None
    normalized_show_input = (show_input or "").strip()
    if normalized_show_input:
        if UUID_RE.match(normalized_show_input):
            resolved_show_id = normalized_show_input
        else:
            query_count += 1
            resolved_show = resolve_show_slug(normalized_show_input)
            resolved_show_id = str(resolved_show["show_id"]) if resolved_show else None

    candidate_full_names = _build_candidate_person_full_names(base_slug)
    if candidate_full_names:
        query_count += 1
        exact_rows = pg.fetch_all(
            """
            SELECT
              p.id::text AS id,
              p.full_name,
              CASE
                WHEN %s::uuid IS NOT NULL AND EXISTS (
                  SELECT 1
                  FROM core.show_cast AS sc
                  WHERE sc.person_id = p.id
                    AND sc.show_id = %s::uuid
                )
                  THEN true
                ELSE false
              END AS on_show
            FROM core.people AS p
            WHERE p.full_name = ANY(%s::text[])
            ORDER BY on_show DESC, p.id ASC
            """,
            [resolved_show_id, resolved_show_id, candidate_full_names],
        )
        if exact_rows:
            preferred_rows = (
                [row for row in exact_rows if row.get("on_show")]
                if resolved_show_id and any(row.get("on_show") for row in exact_rows)
                else exact_rows
            )
            selected = preferred_rows[0]
            if requested_prefix:
                selected = next((row for row in preferred_rows if row["id"].lower().startswith(requested_prefix)), None)
                if selected is None:
                    selected = next((row for row in exact_rows if row["id"].lower().startswith(requested_prefix)), None)
            if selected and selected.get("full_name"):
                has_collision = len(exact_rows) > 1
                return (
                    {
                        "person_id": selected["id"],
                        "slug": base_slug,
                        "canonical_slug": f"{base_slug}--{selected['id'][:8].lower()}" if has_collision else base_slug,
                    },
                    resolved_show_id,
                    query_count,
                )

    query_count += 1
    rows = pg.fetch_all(
        """
        SELECT
          p.id::text AS id,
          p.full_name,
          CASE
            WHEN %s::uuid IS NOT NULL AND EXISTS (
              SELECT 1
              FROM core.show_cast AS sc
              WHERE sc.person_id = p.id
                AND sc.show_id = %s::uuid
            )
              THEN true
            ELSE false
          END AS on_show
        FROM core.people AS p
        WHERE lower(
          trim(
            both '-' FROM regexp_replace(
              regexp_replace(COALESCE(p.full_name, ''), '&', ' and ', 'gi'),
              '[^a-z0-9]+',
              '-',
              'gi'
            )
          )
        ) = %s
        ORDER BY on_show DESC, p.id ASC
        """,
        [resolved_show_id, resolved_show_id, base_slug],
    )
    if not rows:
        return None, resolved_show_id, query_count

    preferred_rows = (
        [row for row in rows if row.get("on_show")]
        if resolved_show_id and any(row.get("on_show") for row in rows)
        else rows
    )
    selected = preferred_rows[0]
    if requested_prefix:
        selected = next((row for row in preferred_rows if row["id"].lower().startswith(requested_prefix)), None)
        if selected is None:
            selected = next((row for row in rows if row["id"].lower().startswith(requested_prefix)), None)
    if not selected or not selected.get("full_name"):
        return None, resolved_show_id, query_count
    has_collision = len(rows) > 1
    return (
        {
            "person_id": selected["id"],
            "slug": base_slug,
            "canonical_slug": f"{base_slug}--{selected['id'][:8].lower()}" if has_collision else base_slug,
        },
        resolved_show_id,
        query_count,
    )


def get_person_detail(person_id: str) -> tuple[dict[str, Any] | None, int]:
    row = pg.fetch_one(
        """
        SELECT
          p.id::text AS id,
          p.full_name,
          p.known_for,
          p.external_ids,
          p.birthday,
          p.gender,
          p.biography,
          p.place_of_birth,
          p.homepage,
          p.profile_image_url,
          COALESCE(ct.also_known_as, ARRAY[]::text[]) AS alternative_names
        FROM core.people AS p
        LEFT JOIN core.cast_tmdb AS ct
          ON ct.person_id = p.id
        WHERE p.id = %s::uuid
        LIMIT 1
        """,
        [person_id],
    )
    return row, 1


def get_person_cover_photo(person_id: str) -> tuple[dict[str, Any] | None, int]:
    row = pg.fetch_one(
        """
        SELECT
          person_id::text AS person_id,
          photo_id,
          photo_url
        FROM admin.person_cover_photos
        WHERE person_id = %s::uuid
        LIMIT 1
        """,
        [person_id],
    )
    return row, 1


def _parse_optional_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return int(value.strip())
        except ValueError:
            return None
    return None


def _to_list(value: Any) -> list[Any] | None:
    return value if isinstance(value, list) else None


def _thumbnail_crop_fields(value: Any) -> dict[str, Any]:
    crop = value if isinstance(value, dict) else {}
    return {
        "thumbnail_focus_x": crop.get("focus_x") if isinstance(crop.get("focus_x"), (int, float)) else None,
        "thumbnail_focus_y": crop.get("focus_y") if isinstance(crop.get("focus_y"), (int, float)) else None,
        "thumbnail_zoom": crop.get("zoom") if isinstance(crop.get("zoom"), (int, float)) else None,
        "thumbnail_crop_mode": crop.get("mode") if isinstance(crop.get("mode"), str) else None,
    }


def _is_broken(context: Any, metadata: Any) -> bool:
    for candidate in (context, metadata):
        if isinstance(candidate, dict):
            status = candidate.get("gallery_status")
            if isinstance(status, str) and status.strip().lower() == "broken_unreachable":
                return True
    return False


def _media_key(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        row.get("media_asset_id") or "",
        row.get("hosted_url") or "",
        row.get("url") or "",
        row.get("source") or "",
    )


def _map_cast_photo_row(row: dict[str, Any]) -> dict[str, Any]:
    crop = _thumbnail_crop_fields(row.get("thumbnail_crop"))
    people_count = row.get("people_count")
    people_count_source = row.get("people_count_source")
    return {
        "id": row["id"],
        "person_id": row["person_id"],
        "source": row.get("source"),
        "url": row.get("url"),
        "hosted_url": row.get("hosted_url"),
        "hosted_content_type": row.get("hosted_content_type"),
        "caption": row.get("caption"),
        "width": row.get("width"),
        "height": row.get("height"),
        "thumbnail_focus_x": crop["thumbnail_focus_x"],
        "thumbnail_focus_y": crop["thumbnail_focus_y"],
        "thumbnail_zoom": crop["thumbnail_zoom"],
        "thumbnail_crop_mode": crop["thumbnail_crop_mode"],
        "people_count": (
            people_count
            if isinstance(people_count, int)
            else _parse_optional_int(row.get("metadata_people_count"))
        ),
        "people_count_source": (
            people_count_source
            if isinstance(people_count_source, str)
            else row.get("metadata_people_count_source")
        ),
        "face_boxes": _to_list(row.get("face_boxes")),
        "face_crops": _to_list(row.get("face_crops")),
        "bucket_type": row.get("bucket_type") if isinstance(row.get("bucket_type"), str) else None,
        "bucket_key": row.get("bucket_key") if isinstance(row.get("bucket_key"), str) else None,
        "bucket_label": row.get("bucket_label") if isinstance(row.get("bucket_label"), str) else None,
        "resolved_show_id": row.get("resolved_show_id") if isinstance(row.get("resolved_show_id"), str) else None,
        "resolved_show_name": (
            row.get("resolved_show_name")
            if isinstance(row.get("resolved_show_name"), str)
            else None
        ),
        "media_asset_id": None,
        "origin": "cast_photos",
        "source_page_url": row.get("source_page_url"),
        "_broken": _is_broken({"gallery_status": row.get("gallery_status")}, None),
    }


def _map_media_link_row(row: dict[str, Any]) -> dict[str, Any] | None:
    crop = _thumbnail_crop_fields(row.get("thumbnail_crop"))
    context_people_count = _parse_optional_int(row.get("context_people_count"))
    metadata_people_count = _parse_optional_int(row.get("metadata_people_count"))
    return {
        "id": row["link_id"],
        "person_id": row["person_id"],
        "source": row.get("source"),
        "url": row.get("resolved_source_url"),
        "hosted_url": row.get("hosted_url"),
        "hosted_content_type": row.get("hosted_content_type"),
        "caption": row.get("caption"),
        "width": row.get("width"),
        "height": row.get("height"),
        "thumbnail_focus_x": crop["thumbnail_focus_x"],
        "thumbnail_focus_y": crop["thumbnail_focus_y"],
        "thumbnail_zoom": crop["thumbnail_zoom"],
        "thumbnail_crop_mode": crop["thumbnail_crop_mode"],
        "people_count": context_people_count if context_people_count is not None else metadata_people_count,
        "people_count_source": (
            row.get("context_people_count_source")
            if isinstance(row.get("context_people_count_source"), str)
            else row.get("metadata_people_count_source")
        ),
        "face_boxes": _to_list(row.get("face_boxes")),
        "face_crops": _to_list(row.get("face_crops")),
        "bucket_type": row.get("bucket_type") if isinstance(row.get("bucket_type"), str) else None,
        "bucket_key": row.get("bucket_key") if isinstance(row.get("bucket_key"), str) else None,
        "bucket_label": row.get("bucket_label") if isinstance(row.get("bucket_label"), str) else None,
        "resolved_show_id": row.get("resolved_show_id") if isinstance(row.get("resolved_show_id"), str) else None,
        "resolved_show_name": (
            row.get("resolved_show_name")
            if isinstance(row.get("resolved_show_name"), str)
            else None
        ),
        "media_asset_id": row.get("media_asset_id"),
        "origin": "media_links",
        "source_page_url": row.get("source_page_url"),
        "_broken": _is_broken({"gallery_status": row.get("gallery_status")}, None),
    }


def get_person_gallery_page(
    person_id: str,
    *,
    limit: int,
    offset: int,
    include_broken: bool,
    sources: list[str] | None,
) -> tuple[dict[str, Any], int]:
    normalized_sources = [source.strip().lower() for source in (sources or []) if source and source.strip()]
    requested_sources = normalized_sources or None
    cast_rows = pg.fetch_all(
        """
        SELECT
          cp.id,
          cp.person_id::text AS person_id,
          lower(cp.source) AS source,
          cp.url,
          cp.hosted_url,
          cp.hosted_content_type,
          cp.caption,
          cp.width,
          cp.height,
          cp.source_page_url,
          cp.metadata -> 'thumbnail_crop' AS thumbnail_crop,
          cp.metadata ->> 'people_count' AS metadata_people_count,
          cp.metadata ->> 'people_count_source' AS metadata_people_count_source,
          cp.metadata -> 'face_boxes' AS face_boxes,
          cp.metadata -> 'face_crops' AS face_crops,
          cp.metadata ->> 'bucket_type' AS bucket_type,
          cp.metadata ->> 'bucket_key' AS bucket_key,
          cp.metadata ->> 'bucket_label' AS bucket_label,
          cp.metadata ->> 'resolved_show_id' AS resolved_show_id,
          cp.metadata ->> 'resolved_show_name' AS resolved_show_name,
          cp.metadata ->> 'gallery_status' AS gallery_status,
          tags.people_count,
          tags.people_count_source
        FROM core.cast_photos AS cp
        LEFT JOIN admin.cast_photo_people_tags AS tags
          ON tags.cast_photo_id = cp.id
        WHERE cp.person_id = %s::uuid
          AND cp.hosted_url IS NOT NULL
          AND (%s::text[] IS NULL OR lower(cp.source) = ANY(%s::text[]))
        ORDER BY cp.gallery_index ASC NULLS LAST, lower(cp.source) ASC, cp.id ASC
        LIMIT %s::int
        OFFSET %s::int
        """,
        [person_id, requested_sources, requested_sources, limit + 1, offset],
    )
    media_rows = pg.fetch_all(
        """
        SELECT
          ml.id::text AS link_id,
          %s::text AS person_id,
          ml.media_asset_id::text AS media_asset_id,
          COALESCE(ml.context -> 'thumbnail_crop', ma.metadata -> 'thumbnail_crop') AS thumbnail_crop,
          ml.context ->> 'people_count' AS context_people_count,
          ml.context ->> 'people_count_source' AS context_people_count_source,
          COALESCE(ml.context -> 'face_boxes', ma.metadata -> 'face_boxes') AS face_boxes,
          COALESCE(ml.context -> 'face_crops', ma.metadata -> 'face_crops') AS face_crops,
          COALESCE(ml.context ->> 'bucket_type', ma.metadata ->> 'bucket_type') AS bucket_type,
          COALESCE(ml.context ->> 'bucket_key', ma.metadata ->> 'bucket_key') AS bucket_key,
          COALESCE(ml.context ->> 'bucket_label', ma.metadata ->> 'bucket_label') AS bucket_label,
          COALESCE(ml.context ->> 'resolved_show_id', ma.metadata ->> 'resolved_show_id') AS resolved_show_id,
          COALESCE(ml.context ->> 'resolved_show_name', ma.metadata ->> 'resolved_show_name') AS resolved_show_name,
          COALESCE(ml.context ->> 'source_page_url', ma.metadata ->> 'source_page_url') AS source_page_url,
          COALESCE(ml.context ->> 'gallery_status', ma.metadata ->> 'gallery_status') AS gallery_status,
          lower(ma.source) AS source,
          ma.source_url,
          COALESCE(ma.metadata ->> 'source_url', ma.source_url) AS resolved_source_url,
          ma.hosted_url,
          ma.hosted_content_type,
          ma.caption,
          ma.width,
          ma.height,
          ma.metadata ->> 'source_url' AS metadata_source_url,
          ma.metadata ->> 'people_count' AS metadata_people_count,
          ma.metadata ->> 'people_count_source' AS metadata_people_count_source
        FROM core.media_links AS ml
        JOIN core.media_assets AS ma
          ON ma.id = ml.media_asset_id
        WHERE ml.entity_type = 'person'
          AND ml.entity_id = %s::uuid
          AND ml.kind = 'gallery'
          AND ma.hosted_url IS NOT NULL
          AND (%s::text[] IS NULL OR lower(coalesce(ma.source, '')) = ANY(%s::text[]))
        ORDER BY ml.position ASC NULLS LAST, ml.id ASC
        LIMIT %s::int
        OFFSET %s::int
        """,
        [person_id, person_id, requested_sources, requested_sources, limit + 1, offset],
    )
    cast_photos = [_map_cast_photo_row(row) for row in cast_rows]
    media_photos = [row for row in (_map_media_link_row(item) for item in media_rows) if row is not None]
    merged: list[dict[str, Any]] = []
    index_by_key: dict[tuple[Any, ...], int] = {}
    for row in cast_photos:
        if not include_broken and row["_broken"]:
            continue
        key = _media_key(row)
        index_by_key[key] = len(merged)
        merged.append(row)
    for row in media_photos:
        if not include_broken and row["_broken"]:
            continue
        key = _media_key(row)
        existing_index = index_by_key.get(key)
        if existing_index is None:
            index_by_key[key] = len(merged)
            merged.append(row)
            continue
        merged[existing_index] = row

    has_more = len(merged) > limit
    page_rows = merged[:limit]
    photos = [
        {
            key: value
            for key, value in row.items()
            if not key.startswith("_")
        }
        for row in page_rows
    ]
    return (
        {
            "photos": photos,
            "pagination": {
                "limit": limit,
                "offset": offset,
                "count": len(photos),
                "next_offset": offset + len(photos),
                "has_more": has_more,
            },
        },
        2,
    )
