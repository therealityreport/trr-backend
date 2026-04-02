from __future__ import annotations

import re
from datetime import UTC, datetime
from functools import lru_cache
from typing import Any

from trr_backend.db import pg

_SUMMARY_TOTALS_SQL = """
WITH added AS (
  SELECT DISTINCT btrim(cs.trr_show_id::text) AS show_id
  FROM admin.covered_shows cs
  WHERE btrim(cs.trr_show_id::text) <> ''
)
SELECT
  (SELECT COUNT(*)::int FROM core.shows) AS total_available_shows,
  (
    SELECT COUNT(DISTINCT s.id)::int
    FROM core.shows s
    JOIN added a ON a.show_id = s.id::text
  ) AS total_added_shows
"""

_SUMMARY_ROWS_SQL = """
WITH added AS (
  SELECT DISTINCT btrim(cs.trr_show_id::text) AS show_id
  FROM admin.covered_shows cs
  WHERE btrim(cs.trr_show_id::text) <> ''
),
network_source AS (
  SELECT
    s.id AS show_id,
    btrim(network_name) AS display_name,
    lower(btrim(network_name)) AS name_key
  FROM core.shows s
  CROSS JOIN LATERAL unnest(COALESCE(s.networks, ARRAY[]::text[])) AS network_name
  WHERE btrim(network_name) <> ''
),
network_grouped AS (
  SELECT
    ns.name_key,
    MIN(ns.display_name) AS name,
    COUNT(DISTINCT ns.show_id)::int AS available_show_count,
    COUNT(DISTINCT CASE WHEN a.show_id IS NOT NULL THEN ns.show_id END)::int AS added_show_count
  FROM network_source ns
  LEFT JOIN added a ON a.show_id = ns.show_id::text
  GROUP BY ns.name_key
),
network_rows AS (
  SELECT
    'network'::text AS type,
    ng.name,
    ng.available_show_count,
    ng.added_show_count,
    meta.hosted_logo_url,
    meta.hosted_logo_black_url,
    meta.hosted_logo_white_url,
    meta.wikidata_id,
    meta.wikipedia_url,
    meta.tmdb_entity_id,
    meta.homepage_url,
    comp.resolution_status,
    comp.resolution_reason,
    comp.last_attempt_at
  FROM network_grouped ng
  LEFT JOIN LATERAL (
    SELECT
      n.hosted_logo_url AS hosted_logo_url,
      n.hosted_logo_black_url AS hosted_logo_black_url,
      n.hosted_logo_white_url AS hosted_logo_white_url,
      n.wikidata_id AS wikidata_id,
      n.wikipedia_url AS wikipedia_url,
      n.id::text AS tmdb_entity_id,
      (n.tmdb_meta->>'homepage')::text AS homepage_url
    FROM core.networks n
    WHERE lower(btrim(n.name)) = ng.name_key
    ORDER BY n.id ASC
    LIMIT 1
  ) meta ON true
  LEFT JOIN LATERAL (
    SELECT
      c.resolution_status AS resolution_status,
      c.resolution_reason AS resolution_reason,
      c.last_attempt_at AS last_attempt_at
    FROM admin.network_streaming_completion c
    WHERE c.entity_type = 'network'
      AND c.entity_key = ng.name_key
    ORDER BY c.updated_at DESC
    LIMIT 1
  ) comp ON true
),
provider_primary AS (
  SELECT
    swp.show_id::uuid AS show_id,
    btrim(wp.provider_name) AS display_name,
    lower(btrim(wp.provider_name)) AS name_key
  FROM core.show_watch_providers swp
  JOIN core.watch_providers wp ON wp.provider_id = swp.provider_id
  WHERE btrim(COALESCE(wp.provider_name, '')) <> ''
),
provider_primary_grouped AS (
  SELECT
    pp.name_key,
    MIN(pp.display_name) AS name,
    COUNT(DISTINCT pp.show_id)::int AS available_show_count,
    COUNT(DISTINCT CASE WHEN a.show_id IS NOT NULL THEN pp.show_id END)::int AS added_show_count
  FROM provider_primary pp
  LEFT JOIN added a ON a.show_id = pp.show_id::text
  GROUP BY pp.name_key
),
provider_fallback AS (
  SELECT
    s.id::uuid AS show_id,
    btrim(provider_name) AS display_name,
    lower(btrim(provider_name)) AS name_key
  FROM core.shows s
  CROSS JOIN LATERAL unnest(COALESCE(s.streaming_providers, ARRAY[]::text[])) AS provider_name
  WHERE btrim(provider_name) <> ''
),
provider_fallback_grouped AS (
  SELECT
    pf.name_key,
    MIN(pf.display_name) AS name,
    COUNT(DISTINCT pf.show_id)::int AS available_show_count,
    COUNT(DISTINCT CASE WHEN a.show_id IS NOT NULL THEN pf.show_id END)::int AS added_show_count
  FROM provider_fallback pf
  LEFT JOIN added a ON a.show_id = pf.show_id::text
  LEFT JOIN provider_primary_grouped ppg ON ppg.name_key = pf.name_key
  WHERE ppg.name_key IS NULL
  GROUP BY pf.name_key
),
provider_grouped AS (
  SELECT * FROM provider_primary_grouped
  UNION ALL
  SELECT * FROM provider_fallback_grouped
),
provider_rows AS (
  SELECT
    'streaming'::text AS type,
    pg.name,
    pg.available_show_count,
    pg.added_show_count,
    meta.hosted_logo_url,
    meta.hosted_logo_black_url,
    meta.hosted_logo_white_url,
    meta.wikidata_id,
    meta.wikipedia_url,
    meta.tmdb_entity_id,
    meta.homepage_url,
    comp.resolution_status,
    comp.resolution_reason,
    comp.last_attempt_at
  FROM provider_grouped pg
  LEFT JOIN LATERAL (
    SELECT
      wp.hosted_logo_url AS hosted_logo_url,
      wp.hosted_logo_black_url AS hosted_logo_black_url,
      wp.hosted_logo_white_url AS hosted_logo_white_url,
      wp.wikidata_id AS wikidata_id,
      wp.wikipedia_url AS wikipedia_url,
      wp.provider_id::text AS tmdb_entity_id,
      (wp.tmdb_meta->>'homepage')::text AS homepage_url
    FROM core.watch_providers wp
    WHERE lower(btrim(wp.provider_name)) = pg.name_key
    ORDER BY wp.provider_id ASC
    LIMIT 1
  ) meta ON true
  LEFT JOIN LATERAL (
    SELECT
      c.resolution_status AS resolution_status,
      c.resolution_reason AS resolution_reason,
      c.last_attempt_at AS last_attempt_at
    FROM admin.network_streaming_completion c
    WHERE c.entity_type = 'streaming'
      AND c.entity_key = pg.name_key
    ORDER BY c.updated_at DESC
    LIMIT 1
  ) comp ON true
),
production_source AS (
  SELECT
    s.id AS show_id,
    btrim(pc.name) AS display_name,
    lower(btrim(pc.name)) AS name_key
  FROM core.shows s
  CROSS JOIN LATERAL unnest(COALESCE(s.tmdb_production_company_ids, ARRAY[]::int[])) AS pc_id
  JOIN core.production_companies pc ON pc.id = pc_id
  WHERE btrim(pc.name) <> ''
),
production_grouped AS (
  SELECT
    ps.name_key,
    MIN(ps.display_name) AS name,
    COUNT(DISTINCT ps.show_id)::int AS available_show_count,
    COUNT(DISTINCT CASE WHEN a.show_id IS NOT NULL THEN ps.show_id END)::int AS added_show_count
  FROM production_source ps
  LEFT JOIN added a ON a.show_id = ps.show_id::text
  GROUP BY ps.name_key
),
production_rows AS (
  SELECT
    'production'::text AS type,
    pcg.name,
    pcg.available_show_count,
    pcg.added_show_count,
    meta.hosted_logo_url,
    meta.hosted_logo_black_url,
    meta.hosted_logo_white_url,
    meta.wikidata_id,
    meta.wikipedia_url,
    meta.tmdb_entity_id,
    meta.homepage_url,
    comp.resolution_status,
    comp.resolution_reason,
    comp.last_attempt_at
  FROM production_grouped pcg
  LEFT JOIN LATERAL (
    SELECT
      pc.hosted_logo_url AS hosted_logo_url,
      pc.hosted_logo_black_url AS hosted_logo_black_url,
      pc.hosted_logo_white_url AS hosted_logo_white_url,
      pc.wikidata_id AS wikidata_id,
      pc.wikipedia_url AS wikipedia_url,
      pc.id::text AS tmdb_entity_id,
      (pc.tmdb_meta->>'homepage')::text AS homepage_url
    FROM core.production_companies pc
    WHERE lower(btrim(pc.name)) = pcg.name_key
    ORDER BY pc.id ASC
    LIMIT 1
  ) meta ON true
  LEFT JOIN LATERAL (
    SELECT
      c.resolution_status AS resolution_status,
      c.resolution_reason AS resolution_reason,
      c.last_attempt_at AS last_attempt_at
    FROM admin.network_streaming_completion c
    WHERE c.entity_type = 'production'
      AND c.entity_key = pcg.name_key
    ORDER BY c.updated_at DESC
    LIMIT 1
  ) comp ON true
)
SELECT *
FROM (
  SELECT * FROM network_rows
  UNION ALL
  SELECT * FROM provider_rows
  UNION ALL
  SELECT * FROM production_rows
) all_rows
ORDER BY type ASC, added_show_count DESC, available_show_count DESC, name ASC
"""


def _to_int(value: Any) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str) and value.strip():
        try:
            return int(value)
        except ValueError:
            return 0
    return 0


def _to_string_or_none(value: Any) -> str | None:
    if isinstance(value, str):
        trimmed = value.strip()
        return trimmed or None
    return None


def _normalize_resolution_status(value: Any) -> str | None:
    if value in {"resolved", "manual_required", "failed"}:
        return str(value)
    return None


def get_networks_streaming_summary() -> tuple[dict[str, Any], int]:
    totals_row = pg.fetch_one(_SUMMARY_TOTALS_SQL) or {}
    rows = pg.fetch_all(_SUMMARY_ROWS_SQL)

    payload_rows: list[dict[str, Any]] = []
    for row in rows:
        hosted_logo_url = _to_string_or_none(row.get("hosted_logo_url"))
        hosted_logo_black_url = _to_string_or_none(row.get("hosted_logo_black_url"))
        hosted_logo_white_url = _to_string_or_none(row.get("hosted_logo_white_url"))
        wikidata_id = _to_string_or_none(row.get("wikidata_id"))
        wikipedia_url = _to_string_or_none(row.get("wikipedia_url"))
        payload_rows.append(
            {
                "type": row["type"],
                "name": row["name"],
                "available_show_count": _to_int(row.get("available_show_count")),
                "added_show_count": _to_int(row.get("added_show_count")),
                "hosted_logo_url": hosted_logo_url,
                "hosted_logo_black_url": hosted_logo_black_url,
                "hosted_logo_white_url": hosted_logo_white_url,
                "wikidata_id": wikidata_id,
                "wikipedia_url": wikipedia_url,
                "tmdb_entity_id": _to_string_or_none(row.get("tmdb_entity_id")),
                "homepage_url": _to_string_or_none(row.get("homepage_url")),
                "resolution_status": _normalize_resolution_status(row.get("resolution_status")),
                "resolution_reason": _to_string_or_none(row.get("resolution_reason")),
                "last_attempt_at": _to_string_or_none(row.get("last_attempt_at")),
                "has_logo": hosted_logo_url is not None,
                "has_bw_variants": hosted_logo_black_url is not None and hosted_logo_white_url is not None,
                "has_links": wikidata_id is not None and wikipedia_url is not None,
            }
        )

    payload = {
        "totals": {
            "total_available_shows": _to_int(totals_row.get("total_available_shows")),
            "total_added_shows": _to_int(totals_row.get("total_added_shows")),
        },
        "rows": payload_rows,
        "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    }
    return payload, 2


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

_ENTITY_REGISTRY_CTE_SQL = """
WITH added AS (
  SELECT DISTINCT btrim(cs.trr_show_id::text) AS show_id
  FROM admin.covered_shows cs
  WHERE btrim(cs.trr_show_id::text) <> ''
),
network_source AS (
  SELECT
    s.id AS show_id,
    btrim(network_name) AS display_name,
    lower(btrim(network_name)) AS name_key
  FROM core.shows s
  CROSS JOIN LATERAL unnest(COALESCE(s.networks, ARRAY[]::text[])) AS network_name
  WHERE btrim(network_name) <> ''
),
network_grouped AS (
  SELECT
    ns.name_key,
    MIN(ns.display_name) AS name,
    COUNT(DISTINCT ns.show_id)::int AS available_show_count,
    COUNT(DISTINCT CASE WHEN a.show_id IS NOT NULL THEN ns.show_id END)::int AS added_show_count
  FROM network_source ns
  LEFT JOIN added a ON a.show_id = ns.show_id::text
  GROUP BY ns.name_key
),
provider_primary AS (
  SELECT
    swp.show_id::uuid AS show_id,
    btrim(wp.provider_name) AS display_name,
    lower(btrim(wp.provider_name)) AS name_key
  FROM core.show_watch_providers swp
  JOIN core.watch_providers wp ON wp.provider_id = swp.provider_id
  WHERE btrim(COALESCE(wp.provider_name, '')) <> ''
),
provider_primary_grouped AS (
  SELECT
    pp.name_key,
    MIN(pp.display_name) AS name,
    COUNT(DISTINCT pp.show_id)::int AS available_show_count,
    COUNT(DISTINCT CASE WHEN a.show_id IS NOT NULL THEN pp.show_id END)::int AS added_show_count
  FROM provider_primary pp
  LEFT JOIN added a ON a.show_id = pp.show_id::text
  GROUP BY pp.name_key
),
provider_fallback AS (
  SELECT
    s.id::uuid AS show_id,
    btrim(provider_name) AS display_name,
    lower(btrim(provider_name)) AS name_key
  FROM core.shows s
  CROSS JOIN LATERAL unnest(COALESCE(s.streaming_providers, ARRAY[]::text[])) AS provider_name
  WHERE btrim(provider_name) <> ''
),
provider_fallback_grouped AS (
  SELECT
    pf.name_key,
    MIN(pf.display_name) AS name,
    COUNT(DISTINCT pf.show_id)::int AS available_show_count,
    COUNT(DISTINCT CASE WHEN a.show_id IS NOT NULL THEN pf.show_id END)::int AS added_show_count
  FROM provider_fallback pf
  LEFT JOIN added a ON a.show_id = pf.show_id::text
  LEFT JOIN provider_primary_grouped ppg ON ppg.name_key = pf.name_key
  WHERE ppg.name_key IS NULL
  GROUP BY pf.name_key
),
provider_grouped AS (
  SELECT * FROM provider_primary_grouped
  UNION ALL
  SELECT * FROM provider_fallback_grouped
),
production_source AS (
  SELECT
    s.id AS show_id,
    btrim(pc.name) AS display_name,
    lower(btrim(pc.name)) AS name_key
  FROM core.shows s
  CROSS JOIN LATERAL unnest(COALESCE(s.tmdb_production_company_ids, ARRAY[]::int[])) AS pc_id
  JOIN core.production_companies pc ON pc.id = pc_id
  WHERE btrim(pc.name) <> ''
),
production_grouped AS (
  SELECT
    ps.name_key,
    MIN(ps.display_name) AS name,
    COUNT(DISTINCT ps.show_id)::int AS available_show_count,
    COUNT(DISTINCT CASE WHEN a.show_id IS NOT NULL THEN ps.show_id END)::int AS added_show_count
  FROM production_source ps
  LEFT JOIN added a ON a.show_id = ps.show_id::text
  GROUP BY ps.name_key
),
entity_registry AS (
  SELECT
    'network'::text AS type,
    ng.name_key,
    ng.name,
    ng.available_show_count,
    ng.added_show_count,
    regexp_replace(lower(ng.name), '[^a-z0-9]+', '-', 'g') AS entity_slug
  FROM network_grouped ng
  UNION ALL
  SELECT
    'streaming'::text AS type,
    pg.name_key,
    pg.name,
    pg.available_show_count,
    pg.added_show_count,
    regexp_replace(lower(pg.name), '[^a-z0-9]+', '-', 'g') AS entity_slug
  FROM provider_grouped pg
  UNION ALL
  SELECT
    'production'::text AS type,
    pcg.name_key,
    pcg.name,
    pcg.available_show_count,
    pcg.added_show_count,
    regexp_replace(lower(pcg.name), '[^a-z0-9]+', '-', 'g') AS entity_slug
  FROM production_grouped pcg
)
"""


def _normalize_entity_key(value: str | None) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip()).lower()


def _to_entity_slug(value: str | None) -> str:
    normalized = str(value or "").strip().lower().replace("&", " and ")
    normalized = re.sub(r"[^a-z0-9]+", "-", normalized)
    normalized = re.sub(r"-{2,}", "-", normalized).strip("-")
    return normalized


def _to_nullable_int(value: Any) -> int | None:
    if value is None:
        return None
    parsed = _to_int(value)
    return parsed if parsed or value in {0, "0"} else None


def _to_string_array(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item.strip() for item in value if isinstance(item, str) and item.strip()]


def _normalize_brand_identity(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").strip().lower())


def _logo_asset_matches_entity(display_name: str, row: dict[str, Any]) -> bool:
    brand_identity = _normalize_brand_identity(display_name)
    if not brand_identity:
        return True
    source_url = _to_string_or_none(row.get("source_url")) or ""
    source = _to_string_or_none(row.get("source")) or ""
    if source in {"tmdb", "override"}:
        return True
    haystack = _normalize_brand_identity(f"{source} {source_url}")
    if not haystack:
        return True
    if brand_identity in haystack:
        return True
    if brand_identity.endswith("tv") and brand_identity[:-2] and brand_identity[:-2] in haystack:
        return True
    return False


@lru_cache(maxsize=16)
def _table_exists(table_name: str) -> bool:
    row = pg.fetch_one("select to_regclass(%s) is not null as exists", [table_name]) or {}
    return bool(row.get("exists"))


def get_networks_streaming_detail(
    *,
    entity_type: str,
    entity_key: str | None = None,
    entity_slug: str | None = None,
) -> tuple[dict[str, Any] | None, int]:
    normalized_entity_key = _normalize_entity_key(entity_key)
    normalized_entity_slug = _to_entity_slug(entity_slug)
    entity_row = pg.fetch_one(
        f"""
        {_ENTITY_REGISTRY_CTE_SQL},
        target_entity AS (
          SELECT *
          FROM entity_registry er
          WHERE er.type = %s
            AND (
              (%s <> '' AND er.name_key = %s)
              OR (%s = '' AND %s <> '' AND er.entity_slug = %s)
            )
          ORDER BY er.added_show_count DESC, er.available_show_count DESC, er.name ASC
          LIMIT 1
        )
        SELECT
          t.type AS entity_type,
          t.name_key,
          t.name AS display_name,
          t.entity_slug,
          t.available_show_count,
          t.added_show_count,
          CASE
            WHEN t.type = 'network' THEN n.id::text
            WHEN t.type = 'streaming' THEN wp.provider_id::text
            WHEN t.type = 'production' THEN pc.id::text
          END AS core_entity_id,
          COALESCE(n.origin_country, pc.origin_country) AS core_origin_country,
          wp.display_priority AS core_display_priority,
          COALESCE(n.tmdb_logo_path, wp.tmdb_logo_path, pc.tmdb_logo_path) AS core_tmdb_logo_path,
          COALESCE(n.logo_path, wp.logo_path, pc.logo_path) AS core_logo_path,
          COALESCE(n.hosted_logo_key, wp.hosted_logo_key, pc.hosted_logo_key) AS core_hosted_logo_key,
          COALESCE(n.hosted_logo_url, wp.hosted_logo_url, pc.hosted_logo_url) AS core_hosted_logo_url,
          COALESCE(
            n.hosted_logo_black_url,
            wp.hosted_logo_black_url,
            pc.hosted_logo_black_url
          ) AS core_hosted_logo_black_url,
          COALESCE(
            n.hosted_logo_white_url,
            wp.hosted_logo_white_url,
            pc.hosted_logo_white_url
          ) AS core_hosted_logo_white_url,
          COALESCE(n.wikidata_id, wp.wikidata_id, pc.wikidata_id) AS core_wikidata_id,
          COALESCE(n.wikipedia_url, wp.wikipedia_url, pc.wikipedia_url) AS core_wikipedia_url,
          COALESCE(n.wikimedia_logo_file, wp.wikimedia_logo_file, pc.wikimedia_logo_file) AS core_wikimedia_logo_file,
          COALESCE(
            n.link_enriched_at::text,
            wp.link_enriched_at::text,
            pc.link_enriched_at::text
          ) AS core_link_enriched_at,
          COALESCE(
            n.link_enrichment_source,
            wp.link_enrichment_source,
            pc.link_enrichment_source
          ) AS core_link_enrichment_source,
          COALESCE(n.facebook_id, wp.facebook_id) AS core_facebook_id,
          COALESCE(n.instagram_id, wp.instagram_id) AS core_instagram_id,
          COALESCE(n.twitter_id, wp.twitter_id) AS core_twitter_id,
          COALESCE(n.tiktok_id, wp.tiktok_id) AS core_tiktok_id,
          ov.id::text AS override_id,
          ov.display_name_override,
          ov.wikidata_id_override,
          ov.wikipedia_url_override,
          ov.logo_source_urls_override,
          ov.source_priority_override,
          ov.aliases_override,
          ov.notes AS override_notes,
          ov.is_active AS override_is_active,
          ov.updated_by AS override_updated_by,
          ov.updated_at::text AS override_updated_at,
          comp.resolution_status AS completion_resolution_status,
          comp.resolution_reason AS completion_resolution_reason,
          comp.last_attempt_at::text AS completion_last_attempt_at
        FROM target_entity t
        LEFT JOIN LATERAL (
          SELECT *
          FROM core.networks n
          WHERE t.type = 'network'
            AND lower(btrim(n.name)) = t.name_key
          ORDER BY n.id ASC
          LIMIT 1
        ) n ON true
        LEFT JOIN LATERAL (
          SELECT *
          FROM core.watch_providers wp
          WHERE t.type = 'streaming'
            AND lower(btrim(wp.provider_name)) = t.name_key
          ORDER BY wp.provider_id ASC
          LIMIT 1
        ) wp ON true
        LEFT JOIN LATERAL (
          SELECT *
          FROM core.production_companies pc
          WHERE t.type = 'production'
            AND lower(btrim(pc.name)) = t.name_key
          ORDER BY pc.id ASC
          LIMIT 1
        ) pc ON true
        LEFT JOIN LATERAL (
          SELECT *
          FROM admin.network_streaming_overrides ov
          WHERE ov.entity_type = t.type
            AND ov.entity_key = t.name_key
            AND ov.is_active = true
          ORDER BY ov.updated_at DESC
          LIMIT 1
        ) ov ON true
        LEFT JOIN LATERAL (
          SELECT *
          FROM admin.network_streaming_completion c
          WHERE c.entity_type = t.type
            AND c.entity_key = t.name_key
          ORDER BY c.updated_at DESC
          LIMIT 1
        ) comp ON true
        """,
        [
            entity_type,
            normalized_entity_key,
            normalized_entity_key,
            normalized_entity_key,
            normalized_entity_slug,
            normalized_entity_slug,
        ],
    )
    if entity_row is None:
        return None, 1

    show_rows = pg.fetch_all(
        f"""
        WITH added AS (
          SELECT DISTINCT btrim(cs.trr_show_id::text) AS show_id
          FROM admin.covered_shows cs
          WHERE btrim(cs.trr_show_id::text) <> ''
        ),
        provider_primary_names AS (
          SELECT DISTINCT lower(btrim(wp.provider_name)) AS name_key
          FROM core.show_watch_providers swp
          JOIN core.watch_providers wp ON wp.provider_id = swp.provider_id
          WHERE btrim(COALESCE(wp.provider_name, '')) <> ''
        ),
        network_show_source AS (
          SELECT DISTINCT s.id AS show_id
          FROM core.shows s
          WHERE %s = 'network'
            AND EXISTS (
              SELECT 1
              FROM unnest(COALESCE(s.networks, ARRAY[]::text[])) AS network_name
              WHERE btrim(network_name) <> ''
                AND lower(btrim(network_name)) = %s
            )
        ),
        streaming_show_source AS (
          SELECT DISTINCT swp.show_id AS show_id
          FROM core.show_watch_providers swp
          JOIN core.watch_providers wp ON wp.provider_id = swp.provider_id
          WHERE %s = 'streaming'
            AND lower(btrim(wp.provider_name)) = %s
        ),
        streaming_fallback_show_source AS (
          SELECT DISTINCT s.id AS show_id
          FROM core.shows s
          LEFT JOIN provider_primary_names pp ON pp.name_key = %s
          WHERE %s = 'streaming'
            AND pp.name_key IS NULL
            AND EXISTS (
              SELECT 1
              FROM unnest(COALESCE(s.streaming_providers, ARRAY[]::text[])) AS provider_name
              WHERE btrim(provider_name) <> ''
                AND lower(btrim(provider_name)) = %s
            )
        ),
        production_show_source AS (
          SELECT DISTINCT s.id AS show_id
          FROM core.shows s
          WHERE %s = 'production'
            AND EXISTS (
              SELECT 1
              FROM unnest(COALESCE(s.tmdb_production_company_ids, ARRAY[]::int[])) AS pc_id
              JOIN core.production_companies pc ON pc.id = pc_id
              WHERE lower(btrim(pc.name)) = %s
            )
        ),
        entity_show_source AS (
          SELECT show_id FROM network_show_source
          UNION
          SELECT show_id FROM streaming_show_source
          UNION
          SELECT show_id FROM streaming_fallback_show_source
          UNION
          SELECT show_id FROM production_show_source
        ),
        shows_with_slug AS (
          SELECT
            s.id,
            s.name,
            s.slug,
            s.primary_poster_image_id,
            {_SHOW_SLUG_SQL} AS computed_slug,
            COUNT(*) OVER (PARTITION BY {_SHOW_SLUG_SQL}) AS slug_collision_count
          FROM core.shows AS s
        )
        SELECT DISTINCT
          es.show_id::text AS trr_show_id,
          COALESCE(NULLIF(btrim(cs.show_name), ''), NULLIF(btrim(s.name), ''), es.show_id::text) AS show_name,
          CASE
            WHEN s.slug_collision_count > 1
              THEN COALESCE(NULLIF(s.slug, ''), s.computed_slug) || '--' || lower(left(s.id::text, 8))
            ELSE COALESCE(NULLIF(s.slug, ''), s.computed_slug)
          END AS canonical_slug,
          si.hosted_url AS poster_url
        FROM entity_show_source es
        JOIN added a ON a.show_id = es.show_id::text
        LEFT JOIN admin.covered_shows cs ON cs.trr_show_id::text = es.show_id::text
        LEFT JOIN shows_with_slug s ON s.id = es.show_id
        LEFT JOIN core.show_images si ON si.id = s.primary_poster_image_id
        ORDER BY show_name ASC
        """,
        [
            entity_row["entity_type"],
            entity_row["name_key"],
            entity_row["entity_type"],
            entity_row["name_key"],
            entity_row["name_key"],
            entity_row["entity_type"],
            entity_row["name_key"],
            entity_row["entity_type"],
            entity_row["name_key"],
        ],
    )

    query_count = 2
    logo_assets: list[dict[str, Any]] = []
    if _table_exists("admin.network_streaming_logo_assets"):
        query_count += 2
        logo_asset_rows = pg.fetch_all(
            """
            SELECT
              la.id::text AS id,
              la.source,
              la.source_url,
              la.source_rank,
              la.hosted_logo_url,
              la.hosted_logo_content_type,
              la.base_logo_format,
              la.pixel_width,
              la.pixel_height,
              la.mirror_status,
              la.failure_reason,
              la.is_primary,
              la.updated_at::text AS updated_at
            FROM admin.network_streaming_logo_assets la
            WHERE la.entity_type = %s
              AND la.entity_key = %s
            ORDER BY
              la.is_primary DESC,
              CASE la.source
                WHEN 'override' THEN 1
                WHEN 'tmdb' THEN 2
                WHEN 'wikimedia' THEN 3
                WHEN 'official' THEN 4
                WHEN 'catalog' THEN 5
                WHEN 'imdb' THEN 6
                ELSE 99
              END ASC,
              la.source_rank ASC,
              la.updated_at DESC
            """,
            [entity_row["entity_type"], entity_row["name_key"]],
        )
        logo_assets = [
            {
                "id": _to_string_or_none(row.get("id")) or "",
                "source": _to_string_or_none(row.get("source")) or "catalog",
                "source_url": _to_string_or_none(row.get("source_url")) or "",
                "source_rank": _to_int(row.get("source_rank")),
                "hosted_logo_url": _to_string_or_none(row.get("hosted_logo_url")),
                "hosted_logo_content_type": _to_string_or_none(row.get("hosted_logo_content_type")),
                "base_logo_format": _to_string_or_none(row.get("base_logo_format")) or "unknown",
                "pixel_width": _to_nullable_int(row.get("pixel_width")),
                "pixel_height": _to_nullable_int(row.get("pixel_height")),
                "mirror_status": _to_string_or_none(row.get("mirror_status")) or "failed",
                "failure_reason": _to_string_or_none(row.get("failure_reason")),
                "is_primary": bool(row.get("is_primary")),
                "updated_at": _to_string_or_none(row.get("updated_at")),
            }
            for row in logo_asset_rows
            if _to_string_or_none(row.get("id"))
            and _logo_asset_matches_entity(
                str(entity_row.get("display_name") or ""),
                row,
            )
        ]
    else:
        query_count += 1

    payload = {
        "entity_type": entity_row["entity_type"],
        "entity_key": entity_row["name_key"],
        "entity_slug": _to_entity_slug(entity_row.get("entity_slug") or entity_row.get("display_name")),
        "display_name": entity_row["display_name"],
        "available_show_count": _to_int(entity_row.get("available_show_count")),
        "added_show_count": _to_int(entity_row.get("added_show_count")),
        "core": {
            "entity_id": _to_string_or_none(entity_row.get("core_entity_id")),
            "origin_country": _to_string_or_none(entity_row.get("core_origin_country")),
            "display_priority": _to_nullable_int(entity_row.get("core_display_priority")),
            "tmdb_logo_path": _to_string_or_none(entity_row.get("core_tmdb_logo_path")),
            "logo_path": _to_string_or_none(entity_row.get("core_logo_path")),
            "hosted_logo_key": _to_string_or_none(entity_row.get("core_hosted_logo_key")),
            "hosted_logo_url": _to_string_or_none(entity_row.get("core_hosted_logo_url")),
            "hosted_logo_black_url": _to_string_or_none(entity_row.get("core_hosted_logo_black_url")),
            "hosted_logo_white_url": _to_string_or_none(entity_row.get("core_hosted_logo_white_url")),
            "wikidata_id": _to_string_or_none(entity_row.get("core_wikidata_id")),
            "wikipedia_url": _to_string_or_none(entity_row.get("core_wikipedia_url")),
            "wikimedia_logo_file": _to_string_or_none(entity_row.get("core_wikimedia_logo_file")),
            "link_enriched_at": _to_string_or_none(entity_row.get("core_link_enriched_at")),
            "link_enrichment_source": _to_string_or_none(entity_row.get("core_link_enrichment_source")),
            "facebook_id": _to_string_or_none(entity_row.get("core_facebook_id")),
            "instagram_id": _to_string_or_none(entity_row.get("core_instagram_id")),
            "twitter_id": _to_string_or_none(entity_row.get("core_twitter_id")),
            "tiktok_id": _to_string_or_none(entity_row.get("core_tiktok_id")),
        },
        "override": {
            "id": _to_string_or_none(entity_row.get("override_id")),
            "display_name_override": _to_string_or_none(entity_row.get("display_name_override")),
            "wikidata_id_override": _to_string_or_none(entity_row.get("wikidata_id_override")),
            "wikipedia_url_override": _to_string_or_none(entity_row.get("wikipedia_url_override")),
            "logo_source_urls_override": _to_string_array(entity_row.get("logo_source_urls_override")),
            "source_priority_override": _to_string_array(entity_row.get("source_priority_override")),
            "aliases_override": _to_string_array(entity_row.get("aliases_override")),
            "notes": _to_string_or_none(entity_row.get("override_notes")),
            "is_active": bool(entity_row.get("override_is_active")),
            "updated_by": _to_string_or_none(entity_row.get("override_updated_by")),
            "updated_at": _to_string_or_none(entity_row.get("override_updated_at")),
        },
        "completion": {
            "resolution_status": _normalize_resolution_status(entity_row.get("completion_resolution_status")),
            "resolution_reason": _to_string_or_none(entity_row.get("completion_resolution_reason")),
            "last_attempt_at": _to_string_or_none(entity_row.get("completion_last_attempt_at")),
        },
        "logo_assets": logo_assets,
        "shows": [
            {
                "trr_show_id": row.get("trr_show_id"),
                "show_name": _to_string_or_none(row.get("show_name")) or row.get("trr_show_id"),
                "canonical_slug": _to_string_or_none(row.get("canonical_slug")),
                "poster_url": _to_string_or_none(row.get("poster_url")),
            }
            for row in show_rows
        ],
    }
    return payload, query_count


def get_networks_streaming_suggestions(
    *,
    entity_type: str,
    entity_key: str | None = None,
    entity_slug: str | None = None,
) -> tuple[list[dict[str, Any]], int]:
    normalized_entity_key = _normalize_entity_key(entity_key)
    normalized_entity_slug = _to_entity_slug(entity_slug)
    rows = pg.fetch_all(
        f"""
        {_ENTITY_REGISTRY_CTE_SQL},
        ranked AS (
          SELECT
            er.type AS entity_type,
            er.name,
            er.entity_slug,
            er.available_show_count,
            er.added_show_count,
            CASE
              WHEN %s <> '' AND er.entity_slug = %s THEN 120
              WHEN %s <> '' AND er.entity_slug LIKE ('%%' || %s || '%%') THEN 100
              WHEN %s <> '' AND %s LIKE ('%%' || er.entity_slug || '%%') THEN 90
              WHEN %s <> '' AND er.name_key = %s THEN 120
              WHEN %s <> '' AND er.name_key LIKE ('%%' || %s || '%%') THEN 95
              WHEN %s <> '' AND %s LIKE ('%%' || er.name_key || '%%') THEN 85
              ELSE 0
            END AS score
          FROM entity_registry er
          WHERE er.type = %s
        )
        SELECT
          entity_type,
          name,
          entity_slug,
          available_show_count,
          added_show_count
        FROM ranked
        ORDER BY score DESC, added_show_count DESC, available_show_count DESC, name ASC
        LIMIT 8
        """,
        [
            normalized_entity_slug,
            normalized_entity_slug,
            normalized_entity_slug,
            normalized_entity_slug,
            normalized_entity_slug,
            normalized_entity_slug,
            normalized_entity_key,
            normalized_entity_key,
            normalized_entity_key,
            normalized_entity_key,
            normalized_entity_key,
            normalized_entity_key,
            entity_type,
        ],
    )
    payload = [
        {
            "entity_type": row.get("entity_type"),
            "name": row.get("name"),
            "entity_slug": _to_entity_slug(row.get("entity_slug") or row.get("name")),
            "available_show_count": _to_int(row.get("available_show_count")),
            "added_show_count": _to_int(row.get("added_show_count")),
        }
        for row in rows
    ]
    return payload, 1
