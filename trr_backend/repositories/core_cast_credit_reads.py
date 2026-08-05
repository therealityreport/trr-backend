"""Public cast and credit read queries for backend API adapters."""

from __future__ import annotations

import json
import logging
import math
import re
import unicodedata
from collections.abc import Mapping
from typing import Any, Literal
from urllib.parse import urlparse

from trr_backend.db import pg

DEFAULT_LIMIT = 20
MAX_LIMIT = 500

ShowCastView = Literal["membership", "episode_evidence", "archive_only"]

logger = logging.getLogger(__name__)

_EMPTY_THUMBNAIL = {
    "thumbnail_focus_x": None,
    "thumbnail_focus_y": None,
    "thumbnail_zoom": None,
    "thumbnail_crop_mode": None,
}
_FEATURED_IMAGE_LINK_KINDS = [
    "bravo_profile",
    "imdb",
    "tmdb",
    "wikipedia",
    "wikidata",
    "fandom",
]


def normalize_pagination(limit: int | None = None, offset: int | None = None) -> tuple[int, int]:
    normalized_limit = min(max(limit if limit is not None else DEFAULT_LIMIT, 1), MAX_LIMIT)
    normalized_offset = max(offset if offset is not None else 0, 0)
    return normalized_limit, normalized_offset


def _mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return dict(parsed) if isinstance(parsed, Mapping) else {}
    return {}


def _text(value: Any) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None


def _metadata_variant_url(metadata: Mapping[str, Any], signature: str, variant_key: str) -> str | None:
    variants = metadata.get("variants")
    signature_bucket = variants.get(signature) if isinstance(variants, Mapping) else None
    variant_bucket = signature_bucket.get(variant_key) if isinstance(signature_bucket, Mapping) else None
    if not isinstance(variant_bucket, Mapping):
        return None
    for format_key in ("webp", "jpg"):
        format_bucket = variant_bucket.get(format_key)
        if isinstance(format_bucket, Mapping):
            candidate = _text(format_bucket.get("url"))
            if candidate:
                return candidate
    return None


def _finite_number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def _thumbnail_fields(value: Any) -> dict[str, Any]:
    crop = _mapping(value)
    mode = crop.get("mode")
    x = _finite_number(crop.get("x"))
    y = _finite_number(crop.get("y"))
    zoom = _finite_number(crop.get("zoom"))
    if mode not in {"manual", "auto"} or x is None or y is None or zoom is None:
        return dict(_EMPTY_THUMBNAIL)
    return {
        "thumbnail_focus_x": min(max(x, 0.0), 100.0),
        "thumbnail_focus_y": min(max(y, 0.0), 100.0),
        "thumbnail_zoom": min(max(zoom, 1.0), 4.0),
        "thumbnail_crop_mode": mode,
    }


def _is_likely_image(content_type: Any, url: Any) -> bool:
    normalized_content_type = str(content_type or "").strip().lower()
    if normalized_content_type.startswith("image/"):
        return True
    normalized_url = str(url or "").strip().lower()
    if not normalized_url:
        return False
    return not normalized_url.endswith((".mp4", ".mov", ".m3u8", ".webm", ".mp3", ".pdf", ".html"))


def _source_mapped_url(value: Any, source: str) -> str | None:
    direct = _text(value)
    if direct:
        return direct
    return _text(_mapping(value).get(source))


def _normalize_bravo_profile_url(value: Any) -> str | None:
    candidate = _text(value)
    if not candidate:
        return None
    try:
        parsed = urlparse(candidate)
    except ValueError:
        return None
    if parsed.hostname not in {"bravotv.com", "www.bravotv.com"}:
        return None
    parts = [part.strip() for part in parsed.path.split("/") if part.strip()]
    try:
        people_index = [part.casefold() for part in parts].index("people")
    except ValueError:
        return None
    if people_index + 1 >= len(parts):
        return None
    slug = parts[people_index + 1].casefold().strip()
    return f"https://www.bravotv.com/people/{slug}" if slug else None


def _bravo_profile_url_from_name(value: Any) -> str | None:
    name = _text(value)
    if not name:
        return None
    normalized = unicodedata.normalize("NFKD", name.casefold())
    normalized = "".join(char for char in normalized if not unicodedata.combining(char))
    slug = re.sub(r"[^a-z0-9]+", "-", normalized).strip("-")
    return f"https://www.bravotv.com/people/{slug}" if slug else None


def get_bravo_photo_candidates(person_ids: list[str]) -> tuple[dict[str, dict[str, Any]], int]:
    """Return live-fallback inputs without performing network I/O."""

    ordered_person_ids = list(dict.fromkeys(person_ids))
    if not ordered_person_ids:
        return {}, 0

    profile_links: dict[str, str] = {}
    try:
        link_rows = pg.fetch_all(
            """
            SELECT entity_id::text AS person_id, url
              FROM core.entity_links
             WHERE entity_type = 'person'
               AND link_kind = 'bravo_profile'
               AND status <> 'rejected'
               AND entity_id = ANY(%s::uuid[])
             ORDER BY
               entity_id,
               CASE WHEN status = 'approved' THEN 0 WHEN status = 'pending' THEN 1 ELSE 2 END,
               COALESCE(confidence, 0) DESC,
               updated_at DESC
            """,
            [ordered_person_ids],
        )
        for row in link_rows:
            person_id = str(row.get("person_id") or "")
            if not person_id or person_id in profile_links:
                continue
            profile_url = _normalize_bravo_profile_url(row.get("url"))
            if profile_url:
                profile_links[person_id] = profile_url
    except Exception as error:  # noqa: BLE001 - optional fallback must soft-fail.
        logger.warning("core cast Bravo link lookup failed: %s", error)

    candidates: dict[str, dict[str, Any]] = {}
    try:
        people_rows = pg.fetch_all(
            """
            SELECT id::text AS id, full_name, profile_image_url, homepage
              FROM core.people
             WHERE id = ANY(%s::uuid[])
            """,
            [ordered_person_ids],
        )
        rows_by_id = {str(row.get("id")): row for row in people_rows if row.get("id")}
        for person_id in ordered_person_ids:
            row = rows_by_id.get(person_id)
            if row is None:
                continue
            image_url = _source_mapped_url(row.get("profile_image_url"), "bravo")
            if image_url and not _is_likely_image(None, image_url):
                image_url = None
            profile_url = (
                profile_links.get(person_id)
                or _normalize_bravo_profile_url(_source_mapped_url(row.get("homepage"), "bravo"))
                or _bravo_profile_url_from_name(row.get("full_name"))
            )
            if image_url or profile_url:
                candidates[person_id] = {"image_url": image_url, "profile_url": profile_url}
    except Exception as error:  # noqa: BLE001 - optional fallback must soft-fail.
        logger.warning("core cast Bravo person lookup failed: %s", error)

    return candidates, 2


def get_preferred_cast_photos(
    person_ids: list[str],
    *,
    season_number: int | None = None,
) -> tuple[dict[str, dict[str, Any]], int]:
    """Resolve local cast photos in the same precedence order as the app repository."""

    ordered_person_ids = list(dict.fromkeys(person_ids))
    photos: dict[str, dict[str, Any]] = {}
    query_count = 0
    if not ordered_person_ids:
        return photos, query_count

    query_count += 1
    try:
        media_rows = pg.fetch_all(
            """
            SELECT
              ml.entity_id::text AS person_id,
              ma.hosted_url,
              ma.hosted_content_type,
              ma.metadata,
              ml.context,
              ml.position,
              ml.created_at
              FROM core.media_links AS ml
              JOIN core.media_assets AS ma ON ma.id = ml.media_asset_id
             WHERE ml.entity_type = 'person'
               AND ml.entity_id = ANY(%s::uuid[])
               AND ml.kind = 'gallery'
               AND ma.hosted_url IS NOT NULL
             ORDER BY
               ml.entity_id,
               CASE
                 WHEN LOWER(COALESCE(ml.context->>'context_section', '')) = 'bravo_profile'
                   AND %s::int IS NOT NULL
                   AND COALESCE(ml.context->>'season_number', '') ~ '^[0-9]+$'
                   AND (ml.context->>'season_number')::int = %s::int THEN 0
                 WHEN LOWER(COALESCE(ml.context->>'context_section', '')) IN (
                   'official season announcement', 'official_season_announcement'
                 )
                   AND %s::int IS NOT NULL
                   AND COALESCE(ml.context->>'season_number', '') ~ '^[0-9]+$'
                   AND (ml.context->>'season_number')::int = %s::int THEN 1
                 WHEN LOWER(COALESCE(ml.context->>'context_section', '')) = 'bravo_profile' THEN 2
                 WHEN LOWER(COALESCE(ml.context->>'context_section', '')) IN (
                   'official season announcement', 'official_season_announcement'
                 ) THEN 3
                 WHEN COALESCE(ml.context->>'people_count', '') ~ '^[0-9]+$'
                   AND (ml.context->>'people_count')::int = 1 THEN 4
                 WHEN LOWER(COALESCE(ml.context->>'context_type', '')) IN ('profile_picture', 'profile') THEN 5
                 ELSE 6
               END,
               COALESCE(ml.position, 2147483647) ASC,
               ml.created_at DESC
            """,
            [ordered_person_ids, season_number, season_number, season_number, season_number],
        )
        for row in media_rows:
            person_id = str(row.get("person_id") or "")
            if not person_id or person_id in photos:
                continue
            metadata = _mapping(row.get("metadata"))
            candidate = (
                _metadata_variant_url(metadata, "base", "thumb")
                or _text(metadata.get("thumb_url"))
                or _metadata_variant_url(metadata, "base", "card")
                or _text(metadata.get("display_url"))
                or _text(row.get("hosted_url"))
            )
            if not candidate or not _is_likely_image(row.get("hosted_content_type"), candidate):
                continue
            context = _mapping(row.get("context"))
            crop = context.get("thumbnail_crop", metadata.get("thumbnail_crop"))
            photos[person_id] = {"url": candidate, **_thumbnail_fields(crop)}
    except Exception as error:  # noqa: BLE001 - optional source must soft-fail.
        logger.warning("core cast gallery photo lookup failed: %s", error)

    remaining = [person_id for person_id in ordered_person_ids if person_id not in photos]
    if not remaining:
        return photos, query_count

    query_count += 1
    try:
        cast_photo_rows = pg.fetch_all(
            """
            SELECT person_id, thumb_url, display_url, hosted_url, url,
                   context_section, season, gallery_index
              FROM core.v_cast_photos
             WHERE person_id = ANY(%s::uuid[])
             ORDER BY
               person_id,
               CASE
                 WHEN LOWER(COALESCE(context_section, '')) = 'bravo_profile'
                   AND %s::int IS NOT NULL AND season = %s::int THEN 0
                 WHEN LOWER(COALESCE(context_section, '')) IN (
                   'official season announcement', 'official_season_announcement'
                 ) AND %s::int IS NOT NULL AND season = %s::int THEN 1
                 WHEN LOWER(COALESCE(context_section, '')) = 'bravo_profile' THEN 2
                 WHEN LOWER(COALESCE(context_section, '')) IN (
                   'official season announcement', 'official_season_announcement'
                 ) THEN 3
                 ELSE 4
               END,
               gallery_index ASC NULLS LAST
            """,
            [remaining, season_number, season_number, season_number, season_number],
        )
        for row in cast_photo_rows:
            person_id = str(row.get("person_id") or "")
            if not person_id or person_id in photos:
                continue
            candidate = next(
                (
                    value
                    for value in (
                        _text(row.get("thumb_url")),
                        _text(row.get("display_url")),
                        _text(row.get("hosted_url")),
                        _text(row.get("url")),
                    )
                    if value
                ),
                None,
            )
            if candidate and _is_likely_image(None, candidate):
                photos[person_id] = {"url": candidate, **_EMPTY_THUMBNAIL}
    except Exception as error:  # noqa: BLE001 - optional source must soft-fail.
        logger.warning("core cast photo-view lookup failed: %s", error)

    unresolved = [person_id for person_id in ordered_person_ids if person_id not in photos]
    if not unresolved:
        return photos, query_count

    query_count += 1
    try:
        featured_rows = pg.fetch_all(
            """
            SELECT
              entity_id::text AS person_id,
              NULLIF(BTRIM(metadata->>'featured_image_url'), '') AS featured_image_url
              FROM core.entity_links
             WHERE entity_type = 'person'
               AND entity_id = ANY(%s::uuid[])
               AND status <> 'rejected'
               AND link_kind = ANY(%s::text[])
               AND NULLIF(BTRIM(metadata->>'featured_image_url'), '') IS NOT NULL
             ORDER BY
               entity_id,
               CASE WHEN status = 'approved' THEN 0 WHEN status = 'pending' THEN 1 ELSE 2 END,
               CASE
                 WHEN link_kind = 'bravo_profile' THEN 0
                 WHEN link_kind = 'imdb' THEN 1
                 WHEN link_kind = 'tmdb' THEN 2
                 WHEN link_kind = 'wikipedia' THEN 3
                 WHEN link_kind = 'wikidata' THEN 4
                 WHEN link_kind = 'fandom' THEN 5
                 ELSE 6
               END,
               COALESCE(confidence, 0) DESC,
               updated_at DESC
            """,
            [unresolved, _FEATURED_IMAGE_LINK_KINDS],
        )
        for row in featured_rows:
            person_id = str(row.get("person_id") or "")
            if not person_id or person_id in photos:
                continue
            candidate = _text(row.get("featured_image_url"))
            if candidate and _is_likely_image(None, candidate):
                photos[person_id] = {"url": candidate, **_EMPTY_THUMBNAIL}
    except Exception as error:  # noqa: BLE001 - optional source must soft-fail.
        logger.warning("core cast entity-link photo lookup failed: %s", error)

    return photos, query_count


def get_people_by_ids(person_ids: list[str]) -> tuple[dict[str, dict[str, Any]], int]:
    ordered_person_ids = list(dict.fromkeys(person_ids))
    if not ordered_person_ids:
        return {}, 0
    try:
        rows = pg.fetch_all(
            """
            SELECT id::text AS id, full_name, known_for
              FROM core.people
             WHERE id = ANY(%s::uuid[])
            """,
            [ordered_person_ids],
        )
    except Exception as error:  # noqa: BLE001 - current app treats this enrichment as optional.
        logger.warning("core cast people lookup failed: %s", error)
        return {}, 1
    return {
        str(row.get("id")): {
            "full_name": row.get("full_name"),
            "known_for": row.get("known_for"),
        }
        for row in rows
        if row.get("id")
    }, 1


def get_show_cast_episode_totals(
    show_id: str,
    person_ids: list[str],
) -> tuple[dict[str, dict[str, Any]], int]:
    ordered_person_ids = list(dict.fromkeys(person_ids))
    if not ordered_person_ids:
        return {}, 0
    try:
        rows = pg.fetch_all(
            """
            SELECT person_id::text AS person_id,
                   COUNT(
                     DISTINCT CASE
                       WHEN COALESCE(appearance_type, 'appears') <> 'archive_footage'
                       THEN episode_id
                     END
                   )::int AS total_episodes,
                   COUNT(
                     DISTINCT CASE
                       WHEN COALESCE(appearance_type, '') = 'archive_footage'
                       THEN episode_id
                     END
                   )::int AS archive_episodes,
                   MAX(person_name) AS person_name
              FROM core.v_episode_credits
             WHERE show_id = %s::uuid
               AND person_id = ANY(%s::uuid[])
             GROUP BY person_id
            """,
            [show_id, ordered_person_ids],
        )
    except Exception as error:  # noqa: BLE001 - stats are soft enrichment in the current app.
        logger.warning("core show cast episode totals lookup failed: %s", error)
        return {}, 1
    return {str(row["person_id"]): dict(row) for row in rows if row.get("person_id")}, 1


def get_show_cast_archive_totals(
    show_id: str,
    person_ids: list[str],
) -> tuple[dict[str, dict[str, Any]], int]:
    ordered_person_ids = list(dict.fromkeys(person_ids))
    if not ordered_person_ids:
        return {}, 0
    try:
        rows = pg.fetch_all(
            """
            SELECT person_id::text AS person_id,
                   COUNT(
                     DISTINCT CASE
                       WHEN COALESCE(appearance_type, '') = 'archive_footage'
                       THEN episode_id
                     END
                   )::int AS archive_episodes,
                   MAX(person_name) AS person_name
              FROM core.v_episode_credits
             WHERE show_id = %s::uuid
               AND person_id = ANY(%s::uuid[])
             GROUP BY person_id
            """,
            [show_id, ordered_person_ids],
        )
    except Exception as error:  # noqa: BLE001 - stats are soft enrichment in the current app.
        logger.warning("core show archive cast totals lookup failed: %s", error)
        return {}, 1
    return {str(row["person_id"]): dict(row) for row in rows if row.get("person_id")}, 1


def _relation_unavailable(error: Exception) -> bool:
    return getattr(error, "pgcode", None) in {"3F000", "42P01", "42501"} or getattr(
        error,
        "code",
        None,
    ) in {"3F000", "42P01", "42501"}


def list_season_episode_counts(
    show_id: str,
    season_id: str,
    season_number: int,
    *,
    limit: int | None = None,
    offset: int | None = None,
) -> tuple[list[dict[str, Any]], dict[str, dict[str, int]], int]:
    """Read season cast counts using the app's ordered relation fallback chain."""

    normalized_limit, normalized_offset = normalize_pagination(limit, offset)
    query_count = 0
    counts: list[dict[str, Any]] | None = None

    query_count += 1
    try:
        counts = pg.fetch_all(
            """
            SELECT person_id::text AS person_id, episodes_in_season
              FROM core.v_season_cast
             WHERE show_id = %s::uuid
               AND season_id = %s::uuid
             ORDER BY episodes_in_season DESC, person_id ASC
             LIMIT %s OFFSET %s
            """,
            [show_id, season_id, normalized_limit, normalized_offset],
        )
    except Exception as error:  # noqa: BLE001 - only documented relation failures fall through.
        if not _relation_unavailable(error):
            raise

    if counts is None:
        query_count += 1
        try:
            counts = pg.fetch_all(
                """
                SELECT vec.person_id::text AS person_id,
                       COUNT(DISTINCT vec.episode_id)::int AS episodes_in_season
                  FROM core.v_episode_cast AS vec
                  JOIN core.episodes AS e ON e.id = vec.episode_id
                 WHERE vec.show_id = %s::uuid
                   AND e.season_id = %s::uuid
                 GROUP BY vec.person_id
                 ORDER BY episodes_in_season DESC, vec.person_id ASC
                 LIMIT %s OFFSET %s
                """,
                [show_id, season_id, normalized_limit, normalized_offset],
            )
        except Exception as error:  # noqa: BLE001 - only documented relation failures fall through.
            if not _relation_unavailable(error):
                raise

    if counts is None:
        query_count += 1
        try:
            counts = pg.fetch_all(
                """
                SELECT person_id::text AS person_id,
                       COUNT(
                         DISTINCT CASE
                           WHEN COALESCE(appearance_type, 'appears') <> 'archive_footage'
                           THEN episode_id
                         END
                       )::int AS episodes_in_season
                  FROM core.v_episode_credits
                 WHERE show_id = %s::uuid
                   AND season_number = %s::int
                 GROUP BY person_id
                 ORDER BY episodes_in_season DESC, person_id ASC
                 LIMIT %s OFFSET %s
                """,
                [show_id, season_number, normalized_limit, normalized_offset],
            )
        except Exception as error:  # noqa: BLE001 - only documented relation failures fall through.
            if not _relation_unavailable(error):
                raise

    if counts is None:
        return [], {}, query_count

    evidence: dict[str, dict[str, int]] = {}
    query_count += 1
    try:
        evidence_rows = pg.fetch_all(
            """
            SELECT person_id::text AS person_id,
                   COUNT(
                     DISTINCT CASE
                       WHEN COALESCE(appearance_type, 'appears') <> 'archive_footage'
                       THEN episode_id
                     END
                   )::int AS regular_episodes_in_season,
                   COUNT(
                     DISTINCT CASE
                       WHEN COALESCE(appearance_type, '') = 'archive_footage'
                       THEN episode_id
                     END
                   )::int AS archive_episodes_in_season
              FROM core.v_episode_credits
             WHERE show_id = %s::uuid
               AND season_number = %s::int
             GROUP BY person_id
            """,
            [show_id, season_number],
        )
        for row in evidence_rows:
            person_id = str(row.get("person_id") or "")
            if not person_id:
                continue
            evidence[person_id] = {
                "regular_episodes_in_season": int(row.get("regular_episodes_in_season") or 0),
                "archive_episodes_in_season": int(row.get("archive_episodes_in_season") or 0),
            }
    except Exception as error:  # noqa: BLE001 - archive evidence is best-effort in the app.
        logger.warning("core season cast episode evidence lookup failed: %s", error)

    return counts, evidence, query_count


def get_season_context(season_id: str) -> tuple[dict[str, Any] | None, int]:
    row = pg.fetch_one(
        """
        SELECT id::text AS id, show_id::text AS show_id, season_number
          FROM core.seasons
         WHERE id = %s::uuid
         LIMIT 1
        """,
        [season_id],
    )
    return row, 1


def list_season_membership(
    show_id: str,
    season_number: int,
    *,
    limit: int | None = None,
    offset: int | None = None,
) -> tuple[list[dict[str, Any]], int]:
    normalized_limit, normalized_offset = normalize_pagination(limit, offset)
    rows = pg.fetch_all(
        """
        SELECT person_id::text AS person_id, person_name, seasons_appeared, total_episodes
          FROM core.v_person_show_seasons
         WHERE show_id = %s::uuid
           AND seasons_appeared @> ARRAY[%s]::int[]
         ORDER BY total_episodes DESC
         LIMIT %s OFFSET %s
        """,
        [show_id, season_number, normalized_limit, normalized_offset],
    )
    return rows, 1


def get_season_membership_totals(
    show_id: str,
    person_ids: list[str],
) -> tuple[dict[str, dict[str, Any]], int]:
    ordered_person_ids = list(dict.fromkeys(person_ids))
    if not ordered_person_ids:
        return {}, 0
    try:
        rows = pg.fetch_all(
            """
            SELECT person_id::text AS person_id, person_name, total_episodes
              FROM core.v_person_show_seasons
             WHERE show_id = %s::uuid
               AND person_id = ANY(%s::uuid[])
            """,
            [show_id, ordered_person_ids],
        )
    except Exception as error:  # noqa: BLE001 - names/totals are optional enrichment.
        logger.warning("core season membership totals lookup failed: %s", error)
        return {}, 1

    totals: dict[str, dict[str, Any]] = {}
    for row in rows:
        person_id = str(row.get("person_id") or "")
        if not person_id:
            continue
        existing = totals.get(person_id)
        row_total = int(row.get("total_episodes") or 0)
        if existing is None or row_total > int(existing.get("total_episodes") or 0):
            totals[person_id] = dict(row)
    return totals, 1


def list_local_person_credits(person_id: str) -> tuple[list[dict[str, Any]], int]:
    rows = pg.fetch_all(
        """
        SELECT
          c.id::text AS id,
          c.show_id::text AS show_id,
          c.person_id::text AS person_id,
          s.name AS show_name,
          c.role,
          c.billing_order,
          c.credit_category,
          c.source_type,
          c.metadata,
          COALESCE(s.imdb_id, sei.external_id) AS show_imdb_id
          FROM core.credits AS c
          LEFT JOIN core.shows AS s ON s.id = c.show_id
          LEFT JOIN LATERAL (
            SELECT external_id
              FROM core.show_external_ids
             WHERE show_id = c.show_id
               AND source_id = 'imdb'
             ORDER BY is_primary DESC, observed_at DESC NULLS LAST, id DESC
             LIMIT 1
          ) AS sei ON TRUE
         WHERE c.person_id = %s::uuid
         ORDER BY c.billing_order ASC NULLS LAST, s.name ASC NULLS LAST, c.id ASC
        """,
        [person_id],
    )
    credits: list[dict[str, Any]] = []
    for row in rows:
        imdb_id = _text(row.get("show_imdb_id"))
        credits.append(
            {
                "id": str(row.get("id") or ""),
                "show_id": str(row.get("show_id")) if row.get("show_id") is not None else None,
                "person_id": str(row.get("person_id") or person_id),
                "show_name": row.get("show_name"),
                "role": row.get("role"),
                "billing_order": row.get("billing_order"),
                "credit_category": row.get("credit_category"),
                "source_type": row.get("source_type"),
                "external_imdb_id": imdb_id,
                "external_url": f"https://www.imdb.com/title/{imdb_id}/" if imdb_id else None,
                "metadata": row.get("metadata"),
            }
        )
    return credits, 1


def get_person_imdb_id(person_id: str) -> tuple[str | None, int]:
    row = pg.fetch_one(
        """
        SELECT external_ids ->> 'imdb' AS imdb_person_id
          FROM core.people
         WHERE id = %s::uuid
         LIMIT 1
        """,
        [person_id],
    )
    return (_text((row or {}).get("imdb_person_id"))), 1


def map_imdb_titles(imdb_title_ids: list[str]) -> tuple[dict[str, dict[str, str]], int]:
    normalized_ids = list(
        dict.fromkeys(value.strip().casefold() for value in imdb_title_ids if isinstance(value, str) and value.strip())
    )
    if not normalized_ids:
        return {}, 0
    rows = pg.fetch_all(
        """
        SELECT DISTINCT ON (imdb_title_id)
          show_id::text AS show_id,
          show_name,
          imdb_title_id
          FROM (
            SELECT
              s.id AS show_id,
              s.name AS show_name,
              LOWER(s.imdb_id) AS imdb_title_id
              FROM core.shows AS s
             WHERE s.imdb_id = ANY(%s::text[])

            UNION ALL

            SELECT
              s.id AS show_id,
              s.name AS show_name,
              LOWER(sei.external_id) AS imdb_title_id
              FROM core.show_external_ids AS sei
              JOIN core.shows AS s ON s.id = sei.show_id
             WHERE sei.source_id = 'imdb'
               AND LOWER(sei.external_id) = ANY(%s::text[])
          ) AS mapped
         ORDER BY imdb_title_id, show_id
        """,
        [normalized_ids, normalized_ids],
    )
    return {
        str(row.get("imdb_title_id") or "").casefold(): {
            "show_id": str(row.get("show_id") or ""),
            "show_name": str(row.get("show_name") or ""),
        }
        for row in rows
        if row.get("imdb_title_id") and row.get("show_id")
    }, 1


def list_curated_cast_show_ids(person_id: str) -> tuple[list[str], int]:
    rows = pg.fetch_all(
        """
        SELECT DISTINCT sra.show_id::text AS show_id
          FROM core.show_cast_role_assignments AS sra
          JOIN core.show_role_catalog AS src ON src.id = sra.role_id
         WHERE sra.person_id = %s::uuid
           AND src.is_active = true
        """,
        [person_id],
    )
    return [str(row.get("show_id")) for row in rows if row.get("show_id")], 1


def list_person_episode_credits(
    person_id: str,
    *,
    show_id: str | None = None,
    include_archive_footage: bool = False,
) -> tuple[list[dict[str, Any]], int]:
    scope_filter = ""
    params: list[Any] = [person_id]
    if show_id is not None:
        scope_filter = "AND vec.show_id = %s::uuid"
        params.append(show_id)
    appearance_filter = (
        "" if include_archive_footage else "AND COALESCE(vec.appearance_type, 'appears') <> 'archive_footage'"
    )
    show_order = "vec.show_id ASC," if show_id is None else ""
    rows = pg.fetch_all(
        f"""
        SELECT
          vec.show_id::text AS show_id,
          vec.credit_id::text AS credit_id,
          vec.credit_category,
          vec.role,
          vec.billing_order,
          vec.source_type,
          vec.episode_id::text AS episode_id,
          vec.season_number,
          vec.episode_number,
          vec.episode_name,
          vec.appearance_type
          FROM core.v_episode_credits AS vec
         WHERE vec.person_id = %s::uuid
           {scope_filter}
           {appearance_filter}
         ORDER BY
           {show_order}
           vec.billing_order ASC NULLS LAST,
           vec.role ASC NULLS LAST,
           vec.season_number DESC NULLS LAST,
           vec.episode_number ASC NULLS LAST,
           vec.episode_name ASC NULLS LAST,
           vec.episode_id ASC
        """,
        params,
    )
    return rows, 1


def list_show_cast(
    show_id: str,
    *,
    view: ShowCastView,
    limit: int | None = None,
    offset: int | None = None,
) -> tuple[list[dict[str, Any]], int]:
    """Return the current app's show-cast row set for one explicit view."""

    normalized_limit, normalized_offset = normalize_pagination(limit, offset)
    if view == "membership":
        sql = """
        SELECT vsc.*
          FROM core.v_show_cast AS vsc
         WHERE vsc.show_id = %s::uuid
         ORDER BY billing_order ASC NULLS LAST
         LIMIT %s OFFSET %s
        """
    elif view == "episode_evidence":
        sql = """
        SELECT vsc.*,
               eligible.total_episodes AS eligible_total_episodes
          FROM core.v_show_cast AS vsc
          JOIN (
            SELECT person_id,
                   COUNT(DISTINCT episode_id)::int AS total_episodes
              FROM core.v_episode_credits
             WHERE show_id = %s::uuid
               AND COALESCE(appearance_type, 'appears') <> 'archive_footage'
             GROUP BY person_id
            HAVING COUNT(DISTINCT episode_id) > 0
          ) AS eligible ON eligible.person_id = vsc.person_id
         WHERE vsc.show_id = %s::uuid
         ORDER BY billing_order ASC NULLS LAST
         LIMIT %s OFFSET %s
        """
        rows = pg.fetch_all(
            sql,
            [show_id, show_id, normalized_limit, normalized_offset],
        )
        return rows, 1
    elif view == "archive_only":
        sql = """
        SELECT vsc.*
          FROM core.v_show_cast AS vsc
          JOIN (
            SELECT person_id,
                   COUNT(
                     DISTINCT CASE
                       WHEN COALESCE(appearance_type, 'appears') <> 'archive_footage'
                       THEN episode_id
                     END
                   )::int AS regular_episodes,
                   COUNT(
                     DISTINCT CASE
                       WHEN COALESCE(appearance_type, '') = 'archive_footage'
                       THEN episode_id
                     END
                   )::int AS archive_episodes
              FROM core.v_episode_credits
             WHERE show_id = %s::uuid
             GROUP BY person_id
          ) AS episode_counts ON episode_counts.person_id = vsc.person_id
         WHERE vsc.show_id = %s::uuid
           AND COALESCE(episode_counts.regular_episodes, 0) = 0
           AND COALESCE(episode_counts.archive_episodes, 0) > 0
         ORDER BY billing_order ASC NULLS LAST
         LIMIT %s OFFSET %s
        """
        rows = pg.fetch_all(
            sql,
            [show_id, show_id, normalized_limit, normalized_offset],
        )
        return rows, 1
    else:  # pragma: no cover - guarded by the public service/router contract.
        raise ValueError(f"Unsupported show cast view: {view}")

    rows = pg.fetch_all(sql, [show_id, normalized_limit, normalized_offset])
    return rows, 1
