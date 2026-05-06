from __future__ import annotations

import json
import logging
import math
import re
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from psycopg2.extras import RealDictCursor

from trr_backend.db import pg
from trr_backend.media.s3_mirror import mirror_media_asset_row

DEFAULT_LIMIT = 20
MAX_LIMIT = 500
ASSET_QUERY_LIMIT = 500
ASSET_FULL_FETCH_LIMIT = 5001
DEFAULT_SEARCH_LIMIT = 8
MAX_SEARCH_LIMIT = 20
DEFAULT_PEOPLE_HOME_LIMIT = 12
MAX_PEOPLE_HOME_LIMIT = 24
DEFAULT_CAST_MIN_EPISODES = 1
UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I)
SHOW_SLUG_SUFFIX_RE = re.compile(r"--([0-9a-f]{8})$", re.I)
logger = logging.getLogger(__name__)
SHOW_FALLBACK_WARNING = (
    "Episode-credit evidence is missing or stale. Showing approximate show-level cast until cast/credits sync succeeds."
)
SEASON_FALLBACK_WARNING = (
    "Season episode evidence is missing or stale. Showing approximate show-level cast until cast/credits sync succeeds."
)
_CURRENT_READ_CURSOR: ContextVar[Any | None] = ContextVar("admin_show_reads_current_read_cursor", default=None)
IMDB_CREW_CREDIT_CATEGORIES = (
    "Producers",
    "Editors",
    "Casting Director",
    "Casting Department",
    "Visual Effects",
    "Production Design",
    "Editorial Department",
    "Production Department",
)
IMDB_CREW_CREDIT_CATEGORY_ORDER = {value: index for index, value in enumerate(IMDB_CREW_CREDIT_CATEGORIES)}
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
PERSON_CARD_PHOTO_SQL = """
LEFT JOIN LATERAL (
  SELECT
    cp.thumb_url,
    cp.display_url,
    cp.hosted_url,
    cp.url
  FROM core.v_cast_photos AS cp
  WHERE cp.person_id = person_metrics.person_id
  ORDER BY
    CASE
      WHEN lower(COALESCE(cp.context_section, '')) = 'bravo_profile' THEN 0
      WHEN lower(COALESCE(cp.context_section, '')) IN (
        'official season announcement',
        'official_season_announcement'
      ) THEN 1
      ELSE 2
    END,
    cp.gallery_index ASC NULLS LAST
  LIMIT 1
) AS photo ON true
"""
PERSON_SHOW_CONTEXT_SQL = """
LEFT JOIN LATERAL (
  SELECT
    CASE
      WHEN sws.slug_collision_count > 1
        THEN COALESCE(NULLIF(sws.slug, ''), sws.computed_slug) || '--' || lower(left(sws.id::text, 8))
      ELSE COALESCE(NULLIF(sws.slug, ''), sws.computed_slug)
    END AS canonical_slug
  FROM core.v_person_show_seasons AS vpss
  JOIN shows_with_slug AS sws ON sws.id = vpss.show_id
  WHERE vpss.person_id = person_metrics.person_id
  ORDER BY
    COALESCE(vpss.total_episodes, 0) DESC,
    vpss.show_id ASC
  LIMIT 1
) AS show_context ON true
"""


def _slugify_token(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value or "")
    normalized = "".join(char for char in normalized if not unicodedata.combining(char))
    normalized = normalized.strip().lower().replace("&", " and ")
    normalized = re.sub(r"[^a-z0-9]+", "-", normalized)
    return normalized.strip("-")


def _normalize_pagination(limit: int | None, offset: int | None) -> tuple[int, int]:
    normalized_limit = min(max(limit or DEFAULT_LIMIT, 1), MAX_LIMIT)
    normalized_offset = max(offset or 0, 0)
    return normalized_limit, normalized_offset


def _normalize_search_limit(limit: int | None) -> int:
    return min(max(limit or DEFAULT_SEARCH_LIMIT, 1), MAX_SEARCH_LIMIT)


def _normalize_people_home_limit(limit: int | None) -> int:
    return min(max(limit or DEFAULT_PEOPLE_HOME_LIMIT, 1), MAX_PEOPLE_HOME_LIMIT)


def _normalize_string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str)]


def _dedupe_preserve_strings(values: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for raw in values:
        value = str(raw or "").strip()
        if not value:
            continue
        key = value.casefold()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(value)
    return deduped


def _build_show_overview_alternative_names(alternative_names: list[str] | None) -> list[str]:
    return _dedupe_preserve_strings(_normalize_string_list(alternative_names))


def _canonicalize_overview_network(value: str) -> str | None:
    normalized = str(value or "").strip().casefold()
    if not normalized:
        return None
    if normalized in {"bravo", "bravo tv"}:
        return "Bravo"
    return None


def _canonicalize_overview_streaming_provider(value: str) -> str:
    trimmed = str(value or "").strip()
    normalized = trimmed.casefold()
    if normalized in {"peacock", "peacock premium", "peacock premium plus"}:
        return "Peacock"
    if normalized == "hayu" or normalized.startswith("hayu "):
        return "Hayu"
    return trimmed


def _build_show_overview_brand_buckets(
    *,
    networks: list[str] | None,
    streaming_providers: list[str] | None,
    watch_providers: list[str] | None,
) -> tuple[list[str], list[str]]:
    overview_networks: list[str] = []
    overview_streaming_providers: list[str] = []

    for network in _normalize_string_list(networks):
        canonical_network = _canonicalize_overview_network(network)
        if canonical_network:
            overview_networks.append(canonical_network)
        else:
            overview_networks.append(network.strip())

    for provider in [*_normalize_string_list(streaming_providers), *_normalize_string_list(watch_providers)]:
        canonical_network = _canonicalize_overview_network(provider)
        if canonical_network:
            overview_networks.append(canonical_network)
            continue
        canonical_provider = _canonicalize_overview_streaming_provider(provider)
        if canonical_provider:
            overview_streaming_providers.append(canonical_provider)

    return (
        sorted(_dedupe_preserve_strings(overview_networks)),
        sorted(_dedupe_preserve_strings(overview_streaming_providers)),
    )


def _build_show_derived_external_links(row: dict[str, Any]) -> dict[str, Any]:
    justwatch_url = str(row.get("justwatch_url") or "").strip() or None
    return {
        "justwatch_url": justwatch_url,
    }


def _normalize_json_list_payload(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return []
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def _normalize_overview_watch_availability(value: Any) -> list[dict[str, Any]]:
    value = _normalize_json_list_payload(value)
    if not value:
        return []

    region_order = {"US": 0, "GB": 1, "CA": 2, "AU": 3}
    rows: list[dict[str, Any]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        region = str(item.get("region") or "").strip().upper()
        if region not in region_order:
            continue
        stream = sorted(_dedupe_preserve_strings(_normalize_string_list(item.get("stream"))))
        buy = sorted(_dedupe_preserve_strings(_normalize_string_list(item.get("buy"))))
        if not stream and not buy:
            continue
        rows.append(
            {
                "region": region,
                "stream": stream,
                "buy": buy,
            }
        )
    return sorted(rows, key=lambda row: region_order.get(str(row.get("region") or ""), 99))


def _normalize_watch_provider_regions(value: Any) -> list[dict[str, Any]]:
    value = _normalize_json_list_payload(value)
    if not value:
        return []

    region_order = {"US": 0, "GB": 1, "CA": 2, "AU": 3}
    rows: list[dict[str, Any]] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        region = str(item.get("region") or "").strip().upper()
        if not region:
            continue

        stream = sorted(_dedupe_preserve_strings(_normalize_string_list(item.get("stream"))))
        free = sorted(_dedupe_preserve_strings(_normalize_string_list(item.get("free"))))
        buy_rent = sorted(_dedupe_preserve_strings(_normalize_string_list(item.get("buy_rent"))))
        if not stream and not free and not buy_rent:
            continue

        rows.append(
            {
                "region": region,
                "stream": stream,
                "free": free,
                "buy_rent": buy_rent,
            }
        )

    return sorted(
        rows,
        key=lambda row: (
            region_order.get(str(row.get("region") or ""), 99),
            str(row.get("region") or ""),
        ),
    )


def _normalize_show_row(row: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(row)
    for key in ("alternative_names", "genres", "networks", "streaming_providers", "watch_providers", "tags"):
        normalized[key] = _normalize_string_list(normalized.get(key))
    for key in ("computed_slug", "slug_collision_count", "tmdb_meta", "imdb_meta"):
        normalized.pop(key, None)
    return normalized


def _augment_show_detail_row(row: dict[str, Any]) -> dict[str, Any]:
    normalized = _normalize_show_row(row)
    overview_networks, overview_streaming_providers = _build_show_overview_brand_buckets(
        networks=normalized.get("networks"),
        streaming_providers=normalized.get("streaming_providers"),
        watch_providers=normalized.get("watch_providers"),
    )
    normalized["overview_alternative_names"] = _build_show_overview_alternative_names(
        normalized.get("alternative_names")
    )
    normalized["overview_networks"] = overview_networks
    normalized["overview_streaming_providers"] = overview_streaming_providers
    normalized["overview_watch_availability"] = _normalize_overview_watch_availability(
        row.get("overview_watch_availability")
    )
    normalized["watch_provider_regions"] = _normalize_watch_provider_regions(row.get("watch_provider_regions"))
    normalized["derived_external_links"] = _build_show_derived_external_links(row)
    normalized.pop("justwatch_url", None)
    return normalized


def _normalize_json_safe_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        normalized = float(value)
        return normalized if math.isfinite(normalized) else None
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, datetime):
        return value.isoformat()
    isoformat = getattr(value, "isoformat", None)
    if callable(isoformat):
        serialized = isoformat()
        return serialized if isinstance(serialized, str) else str(serialized)
    return value


def _normalize_show_season_row(row: dict[str, Any]) -> dict[str, Any]:
    normalized = {key: _normalize_json_safe_value(value) for key, value in dict(row).items()}
    if "episode_airdate_count" in normalized:
        normalized["episode_airdate_count"] = _normalize_int(normalized.get("episode_airdate_count"))
    if "has_scheduled_or_aired_episode" in normalized:
        normalized["has_scheduled_or_aired_episode"] = _normalize_boolish(
            normalized.get("has_scheduled_or_aired_episode")
        )
    return normalized


def _normalize_boolish(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return False


def _normalize_int(value: Any, *, default: int = 0) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if stripped and re.match(r"^-?\d+$", stripped):
            return int(stripped)
    return default


def _normalize_asset_request(limit: int | None, offset: int | None, *, full: bool) -> tuple[int, int, int]:
    normalized_limit = max(limit or (ASSET_FULL_FETCH_LIMIT if full else ASSET_QUERY_LIMIT), 1)
    normalized_offset = max(offset or 0, 0)
    max_limit = ASSET_FULL_FETCH_LIMIT if full else ASSET_QUERY_LIMIT + 1
    normalized_limit = min(normalized_limit, max_limit)
    fetch_limit = ASSET_FULL_FETCH_LIMIT if full else min(normalized_offset + normalized_limit, max_limit)
    return normalized_limit, normalized_offset, fetch_limit


@contextmanager
def _shared_asset_read_cursor(label: str):
    with pg.db_read_connection(label=label) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            token = _CURRENT_READ_CURSOR.set(cur)
            try:
                yield
            finally:
                _CURRENT_READ_CURSOR.reset(token)


def _fetch_asset_source_rows(
    source_timings: dict[str, dict[str, Any]],
    label: str,
    query: str,
    params: list[Any] | None = None,
) -> list[dict[str, Any]]:
    started_at = time.perf_counter()
    rows = _fetch_all_rows(query, params)
    source_timings[label] = {
        "rows": len(rows),
        "duration_ms": round((time.perf_counter() - started_at) * 1000, 2),
    }
    return rows


def _log_asset_source_timings(
    route: str,
    *,
    show_id: str,
    season_number: int | None = None,
    timings: dict[str, dict[str, Any]],
) -> None:
    if not logger.isEnabledFor(logging.DEBUG) or not timings:
        return
    logger.debug(
        "admin_gallery_asset_source_timings route=%s show_id=%s season_number=%s source_timings=%s",
        route,
        show_id,
        season_number,
        timings,
    )


def _metadata_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _metadata_text(metadata: dict[str, Any] | None, *keys: str) -> str | None:
    payload = metadata or {}
    for key in keys:
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _read_metadata_original_url(metadata: dict[str, Any] | None) -> str | None:
    return _pick_url_candidate(
        _metadata_text(
            metadata,
            "original_source_file_url",
            "getty_original_image_url",
            "source_image_url",
            "original_url",
            "url_original",
        )
    )


def _read_metadata_source_url(metadata: dict[str, Any] | None) -> str | None:
    return _pick_url_candidate(
        _metadata_text(
            metadata,
            "source_url",
            "sourceUrl",
            "source_page_url",
            "sourcePageUrl",
            "url",
        )
    )


def _read_people_count(value: Any) -> int | None:
    if isinstance(value, int):
        return value if value >= 0 else None
    if isinstance(value, str) and value.strip().isdigit():
        parsed = int(value.strip())
        return parsed if parsed >= 0 else None
    return None


def _read_people_count_source(value: Any) -> str | None:
    normalized = str(value or "").strip().lower()
    return normalized if normalized in {"auto", "manual"} else None


def _thumbnail_crop_fields(value: Any) -> dict[str, Any]:
    crop = value if isinstance(value, dict) else {}
    mode = str(crop.get("mode") or "").strip().lower() or None
    return {
        "thumbnail_focus_x": float(crop["x"]) if isinstance(crop.get("x"), (int, float)) else None,
        "thumbnail_focus_y": float(crop["y"]) if isinstance(crop.get("y"), (int, float)) else None,
        "thumbnail_zoom": float(crop["zoom"]) if isinstance(crop.get("zoom"), (int, float)) else None,
        "thumbnail_crop_mode": mode if mode in {"manual", "auto"} else None,
    }


def _is_likely_image(content_type: str | None, url: str | None) -> bool:
    normalized_content_type = str(content_type or "").strip().lower()
    if normalized_content_type.startswith("image/"):
        return True
    normalized_url = str(url or "").strip().lower()
    if not normalized_url:
        return False
    return not any(normalized_url.endswith(ext) for ext in (".mp4", ".mov", ".m3u8", ".webm", ".mp3", ".pdf", ".html"))


def _variant_urls(
    metadata: dict[str, Any] | None,
    *,
    hosted_url: str | None,
    source_url: str | None,
    original_url: str | None,
) -> dict[str, Any]:
    payload = metadata or {}
    return {
        "thumb_url": _metadata_text(payload, "thumb_url", "thumbnail_url") or hosted_url,
        "display_url": _metadata_text(payload, "display_url") or hosted_url or source_url,
        "original_url": _metadata_text(payload, "original_url", "url_original") or original_url or source_url,
    }


def _pick_url_candidate(*values: Any) -> str | None:
    for value in values:
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _normalize_scrape_source(
    source: str | None,
    url: str | None,
    metadata: dict[str, Any] | None = None,
) -> str:
    raw_source = str(source or "")
    lower = raw_source.lower()
    if not lower.startswith("web_scrape") and not lower.startswith("webscrape"):
        return raw_source

    def _to_domain(value: str | None) -> str | None:
        if not value:
            return None
        trimmed = value.strip().lower()
        if not trimmed:
            return None
        try:
            hostname = re.sub(r"^www\.", "", re.sub(r":\d+$", "", re.sub(r"^.+://", "", trimmed).split("/")[0]))
            if "://" in trimmed:
                from urllib.parse import urlparse

                parsed = urlparse(trimmed)
                hostname = (parsed.hostname or "").lower()
                hostname = re.sub(r"^www\.", "", hostname)
            if hostname and "." in hostname and not re.search(r"\s", hostname):
                return hostname
        except Exception:
            return None
        return None

    candidate_url = _pick_url_candidate(
        url,
        _read_metadata_source_url(metadata),
        _metadata_text(metadata, "source_domain", "sourceDomain"),
    )
    normalized_domain = _to_domain(candidate_url)
    if normalized_domain:
        return normalized_domain

    if not candidate_url:
        cleaned = re.sub(r"^web[_-]?scrape[:]?", "", lower).removeprefix("www.")
        if cleaned and "." in cleaned:
            return cleaned

    return raw_source


def _metadata_variant_url(metadata: dict[str, Any] | None, signature: str, variant_key: str) -> str | None:
    payload = metadata or {}
    variants = payload.get("variants")
    if not isinstance(variants, dict):
        return None
    signature_bucket = variants.get(signature)
    if not isinstance(signature_bucket, dict):
        return None
    variant_bucket = signature_bucket.get(variant_key)
    if not isinstance(variant_bucket, dict):
        return None
    for format_key in ("webp", "jpg"):
        format_bucket = variant_bucket.get(format_key)
        if isinstance(format_bucket, dict):
            url = format_bucket.get("url")
            if isinstance(url, str) and url.strip():
                return url.strip()
    return None


def _resolve_season_asset_variant_urls(
    metadata: dict[str, Any] | None,
    *,
    hosted_url: str | None,
    source_url: str | None,
    original_url: str | None,
) -> dict[str, Any]:
    payload = metadata or {}
    fallback_hosted_url = _pick_url_candidate(hosted_url)
    fallback_original_url = _pick_url_candidate(original_url)
    fallback_source_url = _pick_url_candidate(source_url)
    metadata_original_url = _read_metadata_original_url(payload)
    metadata_source_url = _read_metadata_source_url(payload)
    fallback_url = _pick_url_candidate(
        fallback_hosted_url,
        fallback_original_url,
        fallback_source_url,
        metadata_original_url,
        metadata_source_url,
    )
    canonical_original_url = _pick_url_candidate(
        metadata_original_url,
        fallback_original_url,
        metadata_source_url,
        fallback_source_url,
        fallback_hosted_url,
    )
    direct_thumb = _pick_url_candidate(payload.get("thumb_url"), payload.get("thumbnail_url"))
    direct_display = _pick_url_candidate(payload.get("display_url"))
    direct_detail = _pick_url_candidate(payload.get("detail_url"))
    direct_crop_display = _pick_url_candidate(payload.get("crop_display_url"))
    direct_crop_detail = _pick_url_candidate(payload.get("crop_detail_url"))
    active_crop_signature = _pick_url_candidate(payload.get("active_crop_signature"))
    variant_thumb = _metadata_variant_url(payload, "base", "thumb") or direct_thumb
    variant_display = _metadata_variant_url(payload, "base", "card") or direct_display
    variant_detail = _metadata_variant_url(payload, "base", "detail") or direct_detail
    variant_crop_display = (
        _metadata_variant_url(payload, active_crop_signature, "crop_card") if active_crop_signature else None
    ) or direct_crop_display
    variant_crop_detail = (
        _metadata_variant_url(payload, active_crop_signature, "crop_detail") if active_crop_signature else None
    ) or direct_crop_detail
    return {
        "original_url": canonical_original_url or fallback_url,
        "thumb_url": variant_thumb or variant_display or fallback_url,
        "display_url": variant_display or fallback_url,
        "detail_url": variant_detail or fallback_url,
        "crop_display_url": variant_crop_display,
        "crop_detail_url": variant_crop_detail,
    }


def _resolve_logo_variant_urls(metadata: dict[str, Any] | None) -> dict[str, Any]:
    payload = metadata or {}
    return {
        "logo_black_url": _pick_url_candidate(_metadata_text(payload, "logo_black_url", "hosted_logo_black_url")),
        "logo_white_url": _pick_url_candidate(_metadata_text(payload, "logo_white_url", "hosted_logo_white_url")),
    }


def _to_date_only(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed = datetime.strptime(value[:10], "%Y-%m-%d")
        except ValueError:
            return None
    return parsed.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=UTC if parsed.tzinfo else None)


def _to_sql_date(value: datetime | None) -> str | None:
    return value.date().isoformat() if isinstance(value, datetime) else None


def _merged_metadata(metadata: Any, context: Any) -> dict[str, Any]:
    return {**_metadata_dict(metadata), **_metadata_dict(context)}


def _push_asset(
    assets: list[dict[str, Any]],
    hosted_url_seen: set[str],
    *,
    asset: dict[str, Any],
) -> None:
    hosted_url = str(asset.get("hosted_url") or "").strip()
    if not hosted_url:
        return
    if hosted_url in hosted_url_seen:
        return
    assets.append(asset)
    hosted_url_seen.add(hosted_url)


def _mirror_media_asset(asset_row: dict[str, Any]) -> tuple[str, str | None]:
    asset_id = str(asset_row.get("id") or "").strip()
    if not asset_id:
        return "", "Missing media asset id"
    try:
        pg.execute(
            """
            update core.media_assets
            set
              ingest_status = 'in_progress',
              updated_at = now()
            where id = %s::uuid
            """,
            [asset_id],
        )
        patch = mirror_media_asset_row(asset_row, force=False)
        completed_at = datetime.now(UTC).isoformat()
        if not patch:
            pg.execute(
                """
                update core.media_assets
                set
                  ingest_status = 'hosted',
                  ingest_completed_at = %s::timestamptz,
                  ingest_last_error = null,
                  ingest_failed_at = null,
                  updated_at = now()
                where id = %s::uuid
                """,
                [completed_at, asset_id],
            )
            return asset_id, None

        assignments: list[str] = []
        params: list[Any] = []
        patch_mapping = {
            "source": "source",
            "source_url": "source_url",
            "hosted_bucket": "hosted_bucket",
            "hosted_key": "hosted_key",
            "hosted_url": "hosted_url",
            "hosted_sha256": "hosted_sha256",
            "hosted_content_type": "hosted_content_type",
            "hosted_bytes": "hosted_bytes",
            "hosted_etag": "hosted_etag",
            "hosted_at": "hosted_at",
            "width": "width",
            "height": "height",
        }
        for column, key in patch_mapping.items():
            if key in patch:
                params.append(patch[key])
                assignments.append(f"{column} = %s")
        metadata = patch.get("metadata") if isinstance(patch.get("metadata"), dict) else None
        if metadata is not None:
            params.append(json.dumps(metadata))
            assignments.append("metadata = %s::jsonb")
        params.extend([completed_at, asset_id])
        pg.execute(
            f"""
            update core.media_assets
            set
              {", ".join(assignments)},
              ingest_status = 'hosted',
              ingest_completed_at = %s::timestamptz,
              ingest_last_error = null,
              ingest_failed_at = null,
              updated_at = now()
            where id = %s::uuid
            """,
            params,
        )
        return asset_id, None
    except Exception as exc:  # noqa: BLE001
        failure = str(exc)
        pg.execute(
            """
            update core.media_assets
            set
              ingest_status = 'failed',
              ingest_last_error = %s,
              ingest_failed_at = %s::timestamptz,
              updated_at = now()
            where id = %s::uuid
            """,
            [failure, datetime.now(UTC).isoformat(), asset_id],
        )
        return asset_id, failure


def get_show_assets(
    show_id: str,
    *,
    limit: int | None = None,
    offset: int | None = None,
    sources: list[str] | None = None,
    full: bool = False,
) -> tuple[list[dict[str, Any]], int]:
    with _shared_asset_read_cursor("admin_show_reads.get_show_assets"):
        return _get_show_assets_impl(
            show_id,
            limit=limit,
            offset=offset,
            sources=sources,
            full=full,
        )


def _get_show_assets_impl(
    show_id: str,
    *,
    limit: int | None = None,
    offset: int | None = None,
    sources: list[str] | None = None,
    full: bool = False,
) -> tuple[list[dict[str, Any]], int]:
    normalized_limit, normalized_offset, fetch_limit = _normalize_asset_request(limit, offset, full=full)
    normalized_sources = {
        str(source).strip().lower() for source in (sources or []) if isinstance(source, str) and source.strip()
    }
    source_fetch_limit = (
        ASSET_FULL_FETCH_LIMIT
        if full
        else (ASSET_QUERY_LIMIT + 1 if normalized_sources else fetch_limit)
    )
    assets: list[dict[str, Any]] = []
    hosted_url_seen: set[str] = set()
    source_timings: dict[str, dict[str, Any]] = {}
    query_count = 0

    media_link_rows = _fetch_asset_source_rows(
        source_timings,
        "show_media_links",
        """
        select
          ml.id::text as link_id,
          ml.kind as link_kind,
          ml.is_primary as link_is_primary,
          ml.context,
          ml.media_asset_id::text as media_asset_id,
          ma.id::text as asset_id,
          ma.source,
          ma.source_url,
          ma.hosted_url,
          ma.hosted_content_type,
          ma.width,
          ma.height,
          ma.caption,
          ma.metadata,
          ma.ingest_status,
          ma.fetched_at::text as fetched_at,
          ma.created_at::text as created_at
        from core.media_links as ml
        left join core.media_assets as ma
          on ma.id = ml.media_asset_id
        where ml.entity_type = 'show'
          and ml.entity_id = %s::uuid
        order by ma.created_at desc nulls last, ml.id asc
        limit %s::int
        """,
        [show_id, source_fetch_limit],
    )
    query_count += 1
    for row in media_link_rows:
        hosted_url = _pick_url_candidate(row.get("hosted_url"))
        if not hosted_url or not _is_likely_image(row.get("hosted_content_type"), hosted_url):
            continue
        context = row.get("context") if isinstance(row.get("context"), dict) else {}
        merged_metadata = _merged_metadata(row.get("metadata"), context)
        source_url = _pick_url_candidate(
            _read_metadata_source_url(merged_metadata),
            row.get("source_url"),
        )
        thumbnail_crop = (
            context.get("thumbnail_crop")
            if context.get("thumbnail_crop") is not None
            else merged_metadata.get("thumbnail_crop")
        )
        _push_asset(
            assets,
            hosted_url_seen,
            asset={
                "id": row.get("asset_id") or row.get("media_asset_id"),
                "type": "show",
                "origin_table": "media_assets",
                "source": _normalize_scrape_source(str(row.get("source") or "unknown"), source_url, merged_metadata),
                "source_url": source_url,
                "kind": row.get("link_kind") or "other",
                "hosted_url": hosted_url,
                **_resolve_season_asset_variant_urls(
                    merged_metadata,
                    hosted_url=hosted_url,
                    source_url=source_url,
                    original_url=_read_metadata_original_url(merged_metadata),
                ),
                **_resolve_logo_variant_urls(merged_metadata),
                "width": row.get("width"),
                "height": row.get("height"),
                "caption": row.get("caption"),
                "context_section": context.get("context_section")
                if isinstance(context.get("context_section"), str)
                else None,
                "context_type": context.get("context_type") if isinstance(context.get("context_type"), str) else None,
                "fetched_at": row.get("fetched_at"),
                "created_at": row.get("created_at"),
                "metadata": merged_metadata,
                "ingest_status": row.get("ingest_status"),
                "hosted_content_type": row.get("hosted_content_type"),
                "link_id": row.get("link_id"),
                "media_asset_id": row.get("media_asset_id"),
                "logo_link_is_primary": bool(row.get("link_is_primary"))
                if str(row.get("link_kind") or "").strip().lower() == "logo"
                else None,
                "people_count": _read_people_count(context.get("people_count")),
                "people_count_source": _read_people_count_source(context.get("people_count_source")),
                **_thumbnail_crop_fields(thumbnail_crop),
            },
        )

    show_image_rows = _fetch_asset_source_rows(
        source_timings,
        "show_images",
        """
        select
          id::text as id,
          source,
          kind,
          image_type,
          url,
          url_original,
          hosted_url,
          width,
          height,
          created_at::text as created_at,
          metadata
        from core.show_images
        where show_id = %s::uuid
          and hosted_url is not null
        order by created_at desc nulls last, id asc
        limit %s::int
        """,
        [show_id, source_fetch_limit],
    )
    query_count += 1
    for row in show_image_rows:
        hosted_url = _pick_url_candidate(row.get("hosted_url"))
        if not hosted_url:
            continue
        metadata = _metadata_dict(row.get("metadata"))
        source_url = _pick_url_candidate(_read_metadata_source_url(metadata), row.get("url"))
        _push_asset(
            assets,
            hosted_url_seen,
            asset={
                "id": row.get("id"),
                "type": "show",
                "origin_table": "show_images",
                "source": _normalize_scrape_source(row.get("source"), source_url, metadata),
                "source_url": source_url,
                "kind": row.get("image_type") or row.get("kind") or "poster",
                "hosted_url": hosted_url,
                **_resolve_season_asset_variant_urls(
                    metadata,
                    hosted_url=hosted_url,
                    source_url=source_url,
                    original_url=_pick_url_candidate(
                        row.get("url_original"),
                        _read_metadata_original_url(metadata),
                    ),
                ),
                **_resolve_logo_variant_urls(metadata),
                "width": row.get("width"),
                "height": row.get("height"),
                "caption": None,
                "created_at": row.get("created_at"),
                "ingest_status": None,
                "metadata": metadata or None,
                "link_id": None,
                "media_asset_id": None,
                "logo_link_is_primary": None,
                "people_count": None,
                "people_count_source": None,
                "thumbnail_focus_x": None,
                "thumbnail_focus_y": None,
                "thumbnail_zoom": None,
                "thumbnail_crop_mode": None,
            },
        )

    filtered_assets = (
        [asset for asset in assets if str(asset.get("source") or "").strip().lower() in normalized_sources]
        if normalized_sources
        else assets
    )
    _log_asset_source_timings("show-assets", show_id=show_id, timings=source_timings)
    return filtered_assets[normalized_offset : normalized_offset + normalized_limit], query_count


def get_show_season_assets(
    show_id: str,
    season_number: int,
    *,
    limit: int | None = None,
    offset: int | None = None,
    sources: list[str] | None = None,
    full: bool = False,
) -> tuple[list[dict[str, Any]], int]:
    with _shared_asset_read_cursor("admin_show_reads.get_show_season_assets"):
        return _get_show_season_assets_impl(
            show_id,
            season_number,
            limit=limit,
            offset=offset,
            sources=sources,
            full=full,
        )


def _get_show_season_assets_impl(
    show_id: str,
    season_number: int,
    *,
    limit: int | None = None,
    offset: int | None = None,
    sources: list[str] | None = None,
    full: bool = False,
) -> tuple[list[dict[str, Any]], int]:
    normalized_limit, normalized_offset, fetch_limit = _normalize_asset_request(limit, offset, full=full)
    normalized_sources = {
        str(source).strip().lower() for source in (sources or []) if isinstance(source, str) and source.strip()
    }
    source_fetch_limit = (
        ASSET_FULL_FETCH_LIMIT
        if full
        else (ASSET_QUERY_LIMIT + 1 if normalized_sources else fetch_limit)
    )
    assets: list[dict[str, Any]] = []
    hosted_url_seen: set[str] = set()
    source_timings: dict[str, dict[str, Any]] = {}
    query_count = 0
    season_id: str | None = None
    season_start_date: datetime | None = None
    season_end_date: datetime | None = None
    show_imdb_id: str | None = None
    show_name: str | None = None

    season_row = _fetch_one_row(
        """
        select
          s.id::text as id,
          s.premiere_date::text as premiere_date,
          s.air_date::text as air_date,
          min(e.air_date)::text as episode_start_date,
          max(e.air_date)::text as episode_end_date,
          sh.name,
          sh.external_ids
        from core.seasons as s
        join core.shows as sh
          on sh.id = s.show_id
        left join core.episodes as e
          on e.season_id = s.id
          and e.air_date is not null
        where s.show_id = %s::uuid
          and s.season_number = %s::int
        group by s.id, s.premiere_date, s.air_date, sh.name, sh.external_ids
        limit 1
        """,
        [show_id, season_number],
    )
    query_count += 1
    if season_row:
        season_id = str(season_row.get("id") or "").strip() or None
        initial_start = season_row.get("premiere_date") or season_row.get("air_date")
        season_start_date = _to_date_only(season_row.get("episode_start_date"))
        season_end_date = _to_date_only(season_row.get("episode_end_date"))
        if season_start_date is None:
            season_start_date = _to_date_only(initial_start)
            if season_start_date is not None:
                season_end_date = _to_date_only(datetime.now(UTC).isoformat())
        elif season_end_date is None:
            season_end_date = _to_date_only(datetime.now(UTC).isoformat())
        show_name = season_row.get("name") if isinstance(season_row.get("name"), str) else None
        external_ids = season_row.get("external_ids") if isinstance(season_row.get("external_ids"), dict) else {}
        show_imdb_id = _pick_url_candidate(external_ids.get("imdb_id"), external_ids.get("imdb"))

    if season_id:
        media_link_rows = _fetch_asset_source_rows(
            source_timings,
            "season_media_links",
            """
            select
              ml.id::text as link_id,
              ml.kind as link_kind,
              ml.context,
              ml.media_asset_id::text as media_asset_id,
              ma.id::text as asset_id,
              ma.source,
              ma.source_url,
              ma.hosted_url,
              ma.hosted_content_type,
              ma.width,
              ma.height,
              ma.caption,
              ma.metadata,
              ma.ingest_status,
              ma.fetched_at::text as fetched_at,
              ma.created_at::text as created_at
            from core.media_links as ml
            left join core.media_assets as ma
              on ma.id = ml.media_asset_id
            where ml.entity_type = 'season'
              and ml.entity_id = %s::uuid
            order by ma.created_at desc nulls last, ml.id asc
            limit %s::int
            """,
            [season_id, source_fetch_limit],
        )
        query_count += 1
        for row in media_link_rows:
            hosted_url = _pick_url_candidate(row.get("hosted_url"))
            if not hosted_url or not _is_likely_image(row.get("hosted_content_type"), hosted_url):
                continue
            merged_metadata = _merged_metadata(row.get("metadata"), row.get("context"))
            source_url = _pick_url_candidate(
                _read_metadata_source_url(merged_metadata),
                row.get("source_url"),
            )
            context = row.get("context") if isinstance(row.get("context"), dict) else {}
            context_section = (
                context.get("context_section") if isinstance(context.get("context_section"), str) else None
            )
            context_type = context.get("context_type") if isinstance(context.get("context_type"), str) else None
            thumbnail_crop = (
                context.get("thumbnail_crop")
                if context.get("thumbnail_crop") is not None
                else merged_metadata.get("thumbnail_crop")
            )
            _push_asset(
                assets,
                hosted_url_seen,
                asset={
                    "id": row.get("asset_id") or row.get("media_asset_id"),
                    "type": "season",
                    "origin_table": "media_assets",
                    "source": _normalize_scrape_source(
                        str(row.get("source") or "unknown"),
                        source_url,
                        merged_metadata,
                    ),
                    "source_url": source_url,
                    "kind": row.get("link_kind") or "other",
                    "hosted_url": hosted_url,
                    **_resolve_season_asset_variant_urls(
                        merged_metadata,
                        hosted_url=hosted_url,
                        source_url=source_url,
                        original_url=_read_metadata_original_url(merged_metadata),
                    ),
                    "width": row.get("width"),
                    "height": row.get("height"),
                    "caption": row.get("caption") or f"Season {season_number}",
                    "season_number": season_number,
                    "context_section": context_section,
                    "context_type": context_type,
                    "fetched_at": row.get("fetched_at"),
                    "created_at": row.get("created_at"),
                    "metadata": merged_metadata,
                    "ingest_status": row.get("ingest_status"),
                    "hosted_content_type": row.get("hosted_content_type"),
                    "link_id": row.get("link_id"),
                    "media_asset_id": row.get("media_asset_id"),
                    "people_count": _read_people_count(context.get("people_count")),
                    "people_count_source": _read_people_count_source(context.get("people_count_source")),
                    **_thumbnail_crop_fields(thumbnail_crop),
                },
            )

    season_image_rows = _fetch_asset_source_rows(
        source_timings,
        "season_images",
        """
        select
          id::text as id,
          source,
          kind,
          coalesce(metadata->>'image_type', kind) as inferred_image_type,
          url,
          url_original,
          hosted_url,
          width,
          height,
          created_at::text as created_at,
          metadata
        from core.season_images
        where show_id = %s::uuid
          and season_number = %s::int
          and hosted_url is not null
        order by created_at desc nulls last, id asc
        limit %s::int
        """,
        [show_id, season_number, source_fetch_limit],
    )
    query_count += 1
    for row in season_image_rows:
        hosted_url = _pick_url_candidate(row.get("hosted_url"))
        if not hosted_url:
            continue
        metadata = _metadata_dict(row.get("metadata"))
        source_url = _pick_url_candidate(_read_metadata_source_url(metadata), row.get("url"))
        _push_asset(
            assets,
            hosted_url_seen,
            asset={
                "id": row.get("id"),
                "type": "season",
                "origin_table": "season_images",
                "source": _normalize_scrape_source(row.get("source"), source_url, metadata),
                "source_url": source_url,
                "kind": row.get("inferred_image_type") or row.get("kind") or "poster",
                "hosted_url": hosted_url,
                **_resolve_season_asset_variant_urls(
                    metadata,
                    hosted_url=hosted_url,
                    source_url=source_url,
                    original_url=_pick_url_candidate(row.get("url_original"), _read_metadata_original_url(metadata)),
                ),
                "width": row.get("width"),
                "height": row.get("height"),
                "caption": f"Season {season_number}",
                "season_number": season_number,
                "created_at": row.get("created_at"),
                "ingest_status": None,
                "metadata": metadata or None,
                "link_id": None,
                "media_asset_id": None,
                "people_count": None,
                "people_count_source": None,
                "thumbnail_focus_x": None,
                "thumbnail_focus_y": None,
                "thumbnail_zoom": None,
                "thumbnail_crop_mode": None,
            },
        )

    episode_image_rows = _fetch_asset_source_rows(
        source_timings,
        "episode_images",
        """
        select
          id::text as id,
          source,
          kind,
          coalesce(metadata->>'image_type', kind) as inferred_image_type,
          url,
          url_original,
          hosted_url,
          width,
          height,
          episode_number,
          created_at::text as created_at,
          metadata
        from core.episode_images
        where show_id = %s::uuid
          and season_number = %s::int
          and hosted_url is not null
        order by episode_number asc
        limit %s::int
        """,
        [show_id, season_number, source_fetch_limit],
    )
    query_count += 1
    for row in episode_image_rows:
        hosted_url = _pick_url_candidate(row.get("hosted_url"))
        if not hosted_url:
            continue
        metadata = _metadata_dict(row.get("metadata"))
        source_url = _pick_url_candidate(_read_metadata_source_url(metadata), row.get("url"))
        _push_asset(
            assets,
            hosted_url_seen,
            asset={
                "id": row.get("id"),
                "type": "episode",
                "origin_table": "episode_images",
                "source": _normalize_scrape_source(row.get("source"), source_url, metadata),
                "source_url": source_url,
                "kind": row.get("inferred_image_type") or row.get("kind") or "still",
                "hosted_url": hosted_url,
                **_resolve_season_asset_variant_urls(
                    metadata,
                    hosted_url=hosted_url,
                    source_url=source_url,
                    original_url=_pick_url_candidate(row.get("url_original"), _read_metadata_original_url(metadata)),
                ),
                "width": row.get("width"),
                "height": row.get("height"),
                "caption": f"Episode {row.get('episode_number')}",
                "episode_number": row.get("episode_number"),
                "season_number": season_number,
                "created_at": row.get("created_at"),
                "ingest_status": None,
                "metadata": metadata or None,
                "link_id": None,
                "media_asset_id": None,
                "people_count": None,
                "people_count_source": None,
                "thumbnail_focus_x": None,
                "thumbnail_focus_y": None,
                "thumbnail_zoom": None,
                "thumbnail_crop_mode": None,
            },
        )

    season_cast_rows = _fetch_all_rows(
        """
        select distinct
          c.person_id::text as person_id,
          p.full_name as person_name
        from core.credits as c
        join core.people as p
          on p.id = c.person_id
        join core.credit_occurrences as co
          on co.credit_id = c.id
        join core.episodes as e
          on e.id = co.episode_id
        join core.seasons as s
          on s.id = e.season_id
        where c.show_id = %s::uuid
          and s.season_number = %s::int
        limit %s::int
        """,
        [show_id, season_number, source_fetch_limit],
    )
    query_count += 1
    if season_cast_rows:
        person_ids = [row["person_id"] for row in season_cast_rows if isinstance(row.get("person_id"), str)]
        if person_ids:
            person_name_by_id = {
                str(row.get("person_id")): row.get("person_name")
                for row in season_cast_rows
                if isinstance(row.get("person_id"), str)
            }
            cast_photo_rows = _fetch_asset_source_rows(
                source_timings,
                "cast_photos",
                """
                select
                  cp.id::text as id,
                  cp.person_id::text as person_id,
                  cp.source,
                  cp.url,
                  cp.hosted_url,
                  cp.hosted_content_type,
                  cp.width,
                  cp.height,
                  cp.caption,
                  cp.context_section,
                  cp.context_type,
                  cp.season,
                  cp.fetched_at::text as fetched_at,
                  cp.hosted_at::text as hosted_at,
                  cp.updated_at::text as updated_at,
                  cp.title_imdb_ids,
                  cp.title_names,
                  cp.metadata,
                  tags.people_count,
                  tags.people_count_source
                from core.cast_photos as cp
                left join admin.cast_photo_people_tags as tags
                  on tags.cast_photo_id = cp.id
                where cp.person_id = any(%s::uuid[])
                  and cp.hosted_url is not null
                  and (
                    cp.hosted_content_type is null
                    or cp.hosted_content_type ilike 'image/%%'
                  )
                  and (
                    coalesce(cardinality(cp.title_imdb_ids), 0) = 0
                    or %s::text is null
                    or %s::text = any(cp.title_imdb_ids)
                  )
                  and (
                    cp.season = %s::int
                    or (
                      %s::date is not null
                      and %s::date is not null
                      and coalesce(cp.fetched_at::date, cp.hosted_at::date, cp.updated_at::date)
                        between %s::date and %s::date
                    )
                  )
                order by coalesce(cp.fetched_at, cp.hosted_at, cp.updated_at) desc nulls last
                limit %s::int
                """,
                [
                    person_ids,
                    show_imdb_id,
                    show_imdb_id,
                    season_number,
                    _to_sql_date(season_start_date),
                    _to_sql_date(season_end_date),
                    _to_sql_date(season_start_date),
                    _to_sql_date(season_end_date),
                    source_fetch_limit,
                ],
            )
            query_count += 1
            normalized_show_name = show_name.lower() if isinstance(show_name, str) else None
            for row in cast_photo_rows:
                hosted_url = _pick_url_candidate(row.get("hosted_url"))
                if not hosted_url or not _is_likely_image(row.get("hosted_content_type"), hosted_url):
                    continue
                title_imdb_ids = row.get("title_imdb_ids") if isinstance(row.get("title_imdb_ids"), list) else None
                title_names = row.get("title_names") if isinstance(row.get("title_names"), list) else None
                if title_imdb_ids:
                    if show_imdb_id and show_imdb_id not in title_imdb_ids:
                        continue
                    if not show_imdb_id and normalized_show_name:
                        if not any(
                            isinstance(title, str) and normalized_show_name in title.lower()
                            for title in title_names or []
                        ):
                            continue
                metadata = _metadata_dict(row.get("metadata"))
                metadata_people_count = _read_people_count(metadata.get("people_count"))
                metadata_people_count_source = _read_people_count_source(metadata.get("people_count_source"))
                people_count = _read_people_count(row.get("people_count")) or metadata_people_count
                people_count_source = (
                    _read_people_count_source(row.get("people_count_source")) or metadata_people_count_source
                )
                _push_asset(
                    assets,
                    hosted_url_seen,
                    asset={
                        "id": row.get("id"),
                        "type": "cast",
                        "origin_table": "cast_photos",
                        "source": row.get("source"),
                        "source_url": row.get("url"),
                        "kind": "profile",
                        "hosted_url": hosted_url,
                        **_resolve_season_asset_variant_urls(
                            metadata,
                            hosted_url=hosted_url,
                            source_url=row.get("url"),
                            original_url=_read_metadata_original_url(metadata),
                        ),
                        "width": row.get("width"),
                        "height": row.get("height"),
                        "caption": row.get("caption"),
                        "person_id": row.get("person_id"),
                        "person_name": person_name_by_id.get(str(row.get("person_id") or "")) or None,
                        "season_number": row.get("season") or season_number,
                        "context_section": row.get("context_section"),
                        "context_type": row.get("context_type"),
                        "fetched_at": row.get("fetched_at"),
                        "created_at": None,
                        "metadata": metadata,
                        "ingest_status": None,
                        "hosted_content_type": row.get("hosted_content_type"),
                        "link_id": None,
                        "media_asset_id": None,
                        "people_count": people_count,
                        "people_count_source": people_count_source,
                        **_thumbnail_crop_fields(metadata.get("thumbnail_crop")),
                    },
                )

    type_priority = {"season": 0, "episode": 1, "cast": 2, "show": 3}
    assets.sort(
        key=lambda asset: (
            type_priority.get(str(asset.get("type")), 99),
            int(asset.get("episode_number") or 0) if asset.get("episode_number") is not None else 0,
        )
    )
    filtered_assets = (
        [asset for asset in assets if str(asset.get("source") or "").strip().lower() in normalized_sources]
        if normalized_sources
        else assets
    )
    _log_asset_source_timings(
        "season-assets",
        show_id=show_id,
        season_number=season_number,
        timings=source_timings,
    )
    return filtered_assets[normalized_offset : normalized_offset + normalized_limit], query_count


def get_unassigned_season_backdrops(season_id: str) -> tuple[dict[str, Any] | None, int]:
    query_count = 0
    season_row = _fetch_one_row(
        """
        select
          id::text as id,
          show_id::text as show_id,
          season_number
        from core.seasons
        where id = %s::uuid
        limit 1
        """,
        [season_id],
    )
    query_count += 1
    if not season_row:
        return None, query_count

    assigned_rows = _fetch_all_rows(
        """
        select distinct ml.media_asset_id::text as media_asset_id
        from core.media_links as ml
        join core.seasons as s
          on s.id = ml.entity_id
        where ml.entity_type = 'season'
          and ml.kind = 'backdrop'
          and s.show_id = %s::uuid
        """,
        [season_row["show_id"]],
    )
    query_count += 1
    assigned_ids = {
        str(row.get("media_asset_id")) for row in assigned_rows if isinstance(row.get("media_asset_id"), str)
    }
    backdrop_rows = _fetch_all_rows(
        """
        select
          ml.media_asset_id::text as media_asset_id,
          ml.context,
          ma.source,
          ma.source_url,
          ma.hosted_url,
          ma.width,
          ma.height,
          ma.caption,
          ma.fetched_at::text as fetched_at,
          ma.metadata
        from core.media_links as ml
        join core.media_assets as ma
          on ma.id = ml.media_asset_id
        where ml.entity_type = 'show'
          and ml.entity_id = %s::uuid
          and ml.kind = 'backdrop'
          and ma.source ilike 'tmdb'
        order by ma.created_at desc nulls last
        limit 500
        """,
        [season_row["show_id"]],
    )
    query_count += 1

    backdrops: list[dict[str, Any]] = []
    for row in backdrop_rows:
        media_asset_id = str(row.get("media_asset_id") or "").strip()
        if not media_asset_id or media_asset_id in assigned_ids:
            continue
        display_url = _pick_url_candidate(row.get("hosted_url"), row.get("source_url"))
        if not display_url:
            continue
        merged_metadata = _merged_metadata(row.get("metadata"), row.get("context"))
        backdrops.append(
            {
                "media_asset_id": media_asset_id,
                "hosted_url": row.get("hosted_url"),
                "source_url": row.get("source_url"),
                "display_url": display_url,
                "width": row.get("width"),
                "height": row.get("height"),
                "caption": row.get("caption"),
                "fetched_at": row.get("fetched_at"),
                "metadata": merged_metadata or None,
            }
        )

    return (
        {
            "season": {
                "id": season_row["id"],
                "show_id": season_row["show_id"],
                "season_number": season_row["season_number"],
            },
            "backdrops": backdrops,
        },
        query_count,
    )


def assign_season_backdrops(
    season_id: str,
    media_asset_ids: list[str],
) -> tuple[dict[str, Any] | None, int, str | None]:
    query_count = 0
    season_row = _fetch_one_row(
        """
        select
          id::text as id,
          show_id::text as show_id,
          season_number
        from core.seasons
        where id = %s::uuid
        limit 1
        """,
        [season_id],
    )
    query_count += 1
    if not season_row:
        return None, query_count, None

    existing_rows = _fetch_all_rows(
        """
        select media_asset_id::text as media_asset_id
        from core.media_links
        where entity_type = 'season'
          and entity_id = %s::uuid
          and kind = 'backdrop'
          and media_asset_id = any(%s::uuid[])
        """,
        [season_id, media_asset_ids],
    )
    query_count += 1
    already_assigned = {
        str(row.get("media_asset_id")) for row in existing_rows if isinstance(row.get("media_asset_id"), str)
    }
    requested = [asset_id for asset_id in media_asset_ids if isinstance(asset_id, str)]
    candidates = [asset_id for asset_id in requested if asset_id not in already_assigned]
    if not candidates:
        return (
            {
                "requested": len(requested),
                "assigned": 0,
                "skipped": len(requested),
                "mirrored_attempted": 0,
                "mirrored_failed": 0,
                "mirrored_failed_ids": [],
            },
            query_count,
            season_row["show_id"],
        )

    asset_rows = _fetch_all_rows(
        """
        select
          id::text as id,
          hosted_url,
          source,
          source_url,
          hosted_bucket,
          hosted_key,
          hosted_content_type,
          metadata,
          width,
          height
        from core.media_assets
        where id = any(%s::uuid[])
        """,
        [candidates],
    )
    query_count += 1
    assets_by_id = {str(row.get("id")): row for row in asset_rows if isinstance(row.get("id"), str)}
    to_mirror = [
        asset_id
        for asset_id in candidates
        if assets_by_id.get(asset_id) and not _pick_url_candidate(assets_by_id[asset_id].get("hosted_url"))
    ]
    mirror_failures: list[dict[str, str]] = []
    if to_mirror:
        max_workers = max(1, min(3, len(to_mirror)))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for asset_id, failure in executor.map(
                _mirror_media_asset,
                [assets_by_id[asset_id] for asset_id in to_mirror if asset_id in assets_by_id],
            ):
                if failure:
                    mirror_failures.append({"id": asset_id, "error": failure})

    post_mirror_rows = _fetch_all_rows(
        """
        select
          id::text as id,
          hosted_url
        from core.media_assets
        where id = any(%s::uuid[])
        """,
        [candidates],
    )
    query_count += 1
    hosted_by_id = {
        str(row.get("id")): _pick_url_candidate(row.get("hosted_url"))
        for row in post_mirror_rows
        if isinstance(row.get("id"), str)
    }
    assignable = [asset_id for asset_id in candidates if hosted_by_id.get(asset_id)]
    mirror_failed_ids = [asset_id for asset_id in candidates if not hosted_by_id.get(asset_id)]
    if not assignable:
        return (
            {
                "requested": len(requested),
                "assigned": 0,
                "skipped": len(already_assigned),
                "mirrored_attempted": len(to_mirror),
                "mirrored_failed": len(mirror_failed_ids),
                "mirrored_failed_ids": mirror_failed_ids,
            },
            query_count,
            season_row["show_id"],
        )

    context = json.dumps(
        {
            "show_id": season_row["show_id"],
            "season_number": season_row["season_number"],
            "assigned_from": "show_backdrops",
        }
    )
    rows = [("season", season_id, asset_id, "backdrop", None, context) for asset_id in assignable]
    pg.execute_values_no_return(
        """
        insert into core.media_links (
          entity_type,
          entity_id,
          media_asset_id,
          kind,
          position,
          context
        )
        values %s
        """,
        rows,
    )
    query_count += 1
    return (
        {
            "requested": len(requested),
            "assigned": len(assignable),
            "skipped": len(already_assigned),
            "mirrored_attempted": len(to_mirror),
            "mirrored_failed": len(mirror_failed_ids),
            "mirrored_failed_ids": mirror_failed_ids,
            "mirror_failures": mirror_failures,
        },
        query_count,
        season_row["show_id"],
    )


def _fetch_all_rows(
    query: str,
    params: list[Any] | tuple[Any, ...] | None = None,
    *,
    cur: Any | None = None,
) -> list[dict[str, Any]]:
    if cur is None:
        active_cur = _CURRENT_READ_CURSOR.get()
        if active_cur is not None:
            return pg.fetch_all_with_cursor(active_cur, query, params)
        return pg.fetch_all(query, params)
    return pg.fetch_all_with_cursor(cur, query, params)


def _fetch_one_row(
    query: str,
    params: list[Any] | tuple[Any, ...] | None = None,
    *,
    cur: Any | None = None,
) -> dict[str, Any] | None:
    if cur is None:
        active_cur = _CURRENT_READ_CURSOR.get()
        if active_cur is not None:
            return pg.fetch_one_with_cursor(active_cur, query, params)
        return pg.fetch_one(query, params)
    return pg.fetch_one_with_cursor(cur, query, params)


def _should_search_episodes(query: str) -> bool:
    normalized = (query or "").strip().lower()
    if not normalized:
        return False
    if len(normalized) >= 4:
        return True
    if re.search(r"\d", normalized):
        return True
    return bool(re.search(r"\bs\d+e\d+\b|\bepisode\b", normalized))


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


def _resolve_preferred_show_route_slug(
    *,
    alternative_names: list[str] | None,
    canonical_slug: str | None,
    slug: str | None,
    fallback: str | None,
) -> str:
    alias = _pick_preferred_show_alias_slug(alternative_names)
    if alias:
        return alias
    for value in (canonical_slug, slug, fallback):
        normalized = _slugify_token(value or "")
        if normalized:
            return normalized
    return "show"


def _build_person_route_slug(person_name: str | None, person_id: str) -> str:
    base = _slugify_token(person_name or "") or "person"
    normalized_person_id = (person_id or "").strip().lower()
    if not UUID_RE.match(normalized_person_id):
        return base
    return base


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


def search_global(query: str, limit: int | None = None) -> tuple[dict[str, Any], int]:
    normalized_limit = _normalize_search_limit(limit)
    query_count = 0
    with pg.db_read_connection(label="admin_show_reads.search_global") as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            shows = _fetch_all_rows(
                f"""
                WITH shows_with_slug AS (
                  SELECT
                    s.id,
                    s.name,
                    s.slug,
                    COALESCE(s.alternative_names, ARRAY[]::text[]) AS alternative_names,
                    {SHOW_SLUG_SQL} AS computed_slug,
                    COUNT(*) OVER (PARTITION BY {SHOW_SLUG_SQL}) AS slug_collision_count
                  FROM core.shows AS s
                )
                SELECT
                  s.id::text AS id,
                  s.name,
                  s.slug,
                  s.alternative_names,
                  CASE
                    WHEN s.slug_collision_count > 1
                      THEN COALESCE(NULLIF(s.slug, ''), s.computed_slug) || '--' || lower(left(s.id::text, 8))
                    ELSE COALESCE(NULLIF(s.slug, ''), s.computed_slug)
                  END AS canonical_slug
                FROM shows_with_slug AS s
                WHERE s.name ILIKE %s
                   OR EXISTS (
                     SELECT 1
                     FROM unnest(s.alternative_names) AS alt(name)
                     WHERE alt.name ILIKE %s
                   )
                ORDER BY s.name ASC
                LIMIT %s OFFSET 0
                """,
                [f"%{query}%", f"%{query}%", normalized_limit],
                cur=cur,
            )
            query_count += 1
            people = _fetch_all_rows(
                f"""
                WITH shows_with_slug AS (
                  SELECT
                    s.id,
                    s.slug,
                    {SHOW_SLUG_SQL} AS computed_slug,
                    COUNT(*) OVER (PARTITION BY {SHOW_SLUG_SQL}) AS slug_collision_count
                  FROM core.shows AS s
                ),
                person_hits AS (
                  SELECT
                    p.id,
                    p.full_name,
                    p.known_for,
                    CASE
                      WHEN p.full_name ILIKE %s THEN 0
                      WHEN p.full_name ILIKE %s THEN 1
                      ELSE 2
                    END AS match_rank
                  FROM core.people AS p
                  WHERE p.full_name ILIKE %s
                  ORDER BY match_rank ASC, p.full_name ASC
                  LIMIT %s OFFSET 0
                )
                SELECT
                  person_hits.id::text AS id,
                  person_hits.full_name,
                  person_hits.known_for,
                  (
                    SELECT
                      CASE
                        WHEN sws.slug_collision_count > 1
                          THEN COALESCE(NULLIF(sws.slug, ''), sws.computed_slug) || '--' || lower(left(sws.id::text, 8))
                        ELSE COALESCE(NULLIF(sws.slug, ''), sws.computed_slug)
                      END
                    FROM core.v_person_show_seasons AS vpss
                    JOIN shows_with_slug AS sws ON sws.id = vpss.show_id
                    WHERE vpss.person_id = person_hits.id
                    ORDER BY
                      COALESCE(vpss.total_episodes, 0) DESC,
                      vpss.show_id ASC
                    LIMIT 1
                  ) AS show_context
                FROM person_hits
                """,
                [query, f"{query}%", f"%{query}%", normalized_limit],
                cur=cur,
            )
            query_count += 1
            episodes: list[dict[str, Any]] = []
            if _should_search_episodes(query):
                episodes = _fetch_all_rows(
                    f"""
                    WITH shows_with_slug AS (
                      SELECT
                        s.id,
                        s.name,
                        s.slug,
                        {SHOW_SLUG_SQL} AS computed_slug,
                        COUNT(*) OVER (PARTITION BY {SHOW_SLUG_SQL}) AS slug_collision_count
                      FROM core.shows AS s
                    )
                    SELECT
                      e.id::text AS id,
                      e.title,
                      e.episode_number,
                      e.season_number,
                      e.air_date,
                      e.show_id::text AS show_id,
                      sws.name AS show_name,
                      CASE
                        WHEN sws.slug_collision_count > 1
                          THEN COALESCE(NULLIF(sws.slug, ''), sws.computed_slug) || '--' || lower(left(sws.id::text, 8))
                        ELSE COALESCE(NULLIF(sws.slug, ''), sws.computed_slug)
                      END AS show_slug
                    FROM core.episodes AS e
                    JOIN shows_with_slug AS sws ON sws.id = e.show_id
                    WHERE
                      COALESCE(e.title, '') ILIKE %s
                      OR CONCAT('episode ', COALESCE(e.episode_number::text, '')) ILIKE %s
                      OR CONCAT(
                        's',
                        COALESCE(e.season_number::text, ''),
                        'e',
                        COALESCE(e.episode_number::text, '')
                      ) ILIKE %s
                    ORDER BY
                      CASE WHEN COALESCE(e.title, '') ILIKE %s THEN 0 ELSE 1 END,
                      e.air_date DESC NULLS LAST,
                      e.id ASC
                    LIMIT %s OFFSET 0
                    """,
                    [f"{query}%", f"%{query}%", f"%{query}%", f"{query}%", normalized_limit],
                    cur=cur,
                )
                query_count += 1
    return (
        {
            "query": query,
            "pagination": {"per_type_limit": normalized_limit},
            "shows": [
                {
                    "id": row["id"],
                    "name": row["name"],
                    "slug": _resolve_preferred_show_route_slug(
                        alternative_names=row.get("alternative_names"),
                        canonical_slug=row.get("canonical_slug"),
                        slug=row.get("slug"),
                        fallback=row.get("name"),
                    ),
                }
                for row in shows
            ],
            "people": [
                {
                    "id": row["id"],
                    "full_name": row["full_name"],
                    "known_for": row.get("known_for"),
                    "show_context": row.get("show_context"),
                    "person_slug": _build_person_route_slug(row.get("full_name"), row["id"]),
                }
                for row in people
            ],
            "episodes": episodes,
        },
        query_count,
    )


def search_shows(query: str, limit: int | None = None, offset: int | None = None) -> tuple[list[dict[str, Any]], int]:
    normalized_limit, normalized_offset = _normalize_pagination(limit, offset)
    rows = pg.fetch_all(
        f"""
        WITH shows_with_slug AS (
          SELECT
            s.id,
            s.name,
            s.slug,
            COALESCE(s.alternative_names, ARRAY[]::text[]) AS alternative_names,
            s.show_total_seasons,
            s.show_total_episodes,
            COALESCE(s.networks, ARRAY[]::text[]) AS networks,
            s.primary_poster_image_id,
            {SHOW_SLUG_SQL} AS computed_slug,
            COUNT(*) OVER (PARTITION BY {SHOW_SLUG_SQL}) AS slug_collision_count
          FROM core.shows AS s
        )
        SELECT
          s.id::text AS id,
          s.name,
          s.slug,
          s.alternative_names,
          s.show_total_seasons,
          s.show_total_episodes,
          s.networks,
          CASE
            WHEN s.slug_collision_count > 1
              THEN COALESCE(NULLIF(s.slug, ''), s.computed_slug) || '--' || lower(left(s.id::text, 8))
            ELSE COALESCE(NULLIF(s.slug, ''), s.computed_slug)
          END AS canonical_slug,
          poster.hosted_url AS poster_url
        FROM shows_with_slug AS s
        LEFT JOIN core.show_images AS poster ON poster.id = s.primary_poster_image_id
        WHERE s.name ILIKE %s
           OR EXISTS (
             SELECT 1
             FROM unnest(s.alternative_names) AS alt(name)
             WHERE alt.name ILIKE %s
           )
        ORDER BY s.name ASC
        LIMIT %s OFFSET %s
        """,
        [f"%{query}%", f"%{query}%", normalized_limit, normalized_offset],
    )
    return ([_normalize_show_row(row) for row in rows], 1)


def resolve_show_slug(slug: str) -> tuple[dict[str, Any] | None, int]:
    raw_suffix_match = SHOW_SLUG_SUFFIX_RE.search(slug or "")
    requested_prefix = raw_suffix_match.group(1).lower() if raw_suffix_match else None
    raw_base = slug[: -len(raw_suffix_match.group(0))] if raw_suffix_match else slug
    query_count = 0
    for base_slug in _build_show_slug_candidates(raw_base):
        query_count += 1
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
            _pick_preferred_show_alias_slug(selected.get("alternative_names")) or selected.get("slug") or base_slug
        )
        has_collision = len(rows) > 1
        return (
            {
                "show_id": selected["id"],
                "slug": preferred_slug,
                "canonical_slug": (
                    f"{preferred_slug}--{selected['id'][:8].lower()}" if has_collision else preferred_slug
                ),
                "show_name": selected["name"],
            },
            query_count,
        )
    return None, query_count


def get_show_detail(show_id: str) -> tuple[dict[str, Any] | None, int]:
    row = _fetch_one_row(
        f"""
        WITH shows_with_slug AS (
          SELECT
            s.id,
            s.name,
            s.slug,
            COALESCE(s.alternative_names, ARRAY[]::text[]) AS alternative_names,
            s.imdb_id,
            s.tmdb_id,
            s.tvdb_id,
            s.tvrage_id,
            s.wikidata_id,
            COALESCE(s.external_ids, '{{}}'::jsonb) AS external_ids,
            s.show_total_seasons,
            s.show_total_episodes,
            s.description,
            s.premiere_date,
            COALESCE(s.genres, ARRAY[]::text[]) AS genres,
            COALESCE(s.networks, ARRAY[]::text[]) AS networks,
            COALESCE(s.streaming_providers, ARRAY[]::text[]) AS streaming_providers,
            COALESCE(watch.provider_names, ARRAY[]::text[]) AS watch_providers,
            watch.justwatch_url,
            watch.overview_watch_availability,
            watch.watch_provider_regions,
            COALESCE(s.tags, ARRAY[]::text[]) AS tags,
            s.primary_poster_image_id,
            s.primary_backdrop_image_id,
            s.primary_logo_image_id,
            s.tmdb_status,
            s.tmdb_vote_average,
            s.imdb_rating_value,
            s.created_at,
            s.updated_at,
            {SHOW_SLUG_SQL} AS computed_slug,
            COUNT(*) OVER (PARTITION BY {SHOW_SLUG_SQL}) AS slug_collision_count
          FROM core.shows AS s
          LEFT JOIN LATERAL (
            SELECT
              COALESCE(
                ARRAY_AGG(DISTINCT btrim(wp.provider_name) ORDER BY btrim(wp.provider_name))
                  FILTER (WHERE btrim(COALESCE(wp.provider_name, '')) <> ''),
                ARRAY[]::text[]
              ) AS provider_names,
              MIN(NULLIF(btrim(swp.link), '')) AS justwatch_url
              ,
              COALESCE(
                (
                  SELECT jsonb_agg(
                    jsonb_build_object(
                      'region',
                      grouped.region,
                      'stream',
                      grouped.stream,
                      'buy',
                      grouped.buy
                    )
                    ORDER BY grouped.sort_order
                  )
                  FROM (
                    SELECT
                      swp_group.region,
                      CASE swp_group.region
                        WHEN 'US' THEN 0
                        WHEN 'GB' THEN 1
                        WHEN 'CA' THEN 2
                        WHEN 'AU' THEN 3
                        ELSE 99
                      END AS sort_order,
                      COALESCE(
                        ARRAY_AGG(DISTINCT btrim(wp_group.provider_name) ORDER BY btrim(wp_group.provider_name))
                          FILTER (
                            WHERE swp_group.offer_type IN ('flatrate', 'ads')
                              AND btrim(COALESCE(wp_group.provider_name, '')) <> ''
                          ),
                        ARRAY[]::text[]
                      ) AS stream,
                      COALESCE(
                        ARRAY_AGG(DISTINCT btrim(wp_group.provider_name) ORDER BY btrim(wp_group.provider_name))
                          FILTER (
                            WHERE swp_group.offer_type IN ('buy', 'rent')
                              AND btrim(COALESCE(wp_group.provider_name, '')) <> ''
                          ),
                        ARRAY[]::text[]
                      ) AS buy
                    FROM core.show_watch_providers AS swp_group
                    JOIN core.watch_providers AS wp_group ON wp_group.provider_id = swp_group.provider_id
                    WHERE swp_group.show_id = s.id
                      AND swp_group.region IN ('US', 'GB', 'CA', 'AU')
                    GROUP BY swp_group.region
                  ) AS grouped
                ),
                '[]'::jsonb
              ) AS overview_watch_availability,
              COALESCE(
                (
                  SELECT jsonb_agg(
                    jsonb_build_object(
                      'region',
                      grouped.region,
                      'stream',
                      grouped.stream,
                      'free',
                      grouped.free,
                      'buy_rent',
                      grouped.buy_rent
                    )
                    ORDER BY grouped.sort_order, grouped.region
                  )
                  FROM (
                    SELECT
                      upper(btrim(swp_group.region)) AS region,
                      CASE upper(btrim(swp_group.region))
                        WHEN 'US' THEN 0
                        WHEN 'GB' THEN 1
                        WHEN 'CA' THEN 2
                        WHEN 'AU' THEN 3
                        ELSE 99
                      END AS sort_order,
                      COALESCE(
                        ARRAY_AGG(DISTINCT btrim(wp_group.provider_name) ORDER BY btrim(wp_group.provider_name))
                          FILTER (
                            WHERE swp_group.offer_type IN ('flatrate', 'ads')
                              AND btrim(COALESCE(wp_group.provider_name, '')) <> ''
                          ),
                        ARRAY[]::text[]
                      ) AS stream,
                      COALESCE(
                        ARRAY_AGG(DISTINCT btrim(wp_group.provider_name) ORDER BY btrim(wp_group.provider_name))
                          FILTER (
                            WHERE swp_group.offer_type = 'free'
                              AND btrim(COALESCE(wp_group.provider_name, '')) <> ''
                          ),
                        ARRAY[]::text[]
                      ) AS free,
                      COALESCE(
                        ARRAY_AGG(DISTINCT btrim(wp_group.provider_name) ORDER BY btrim(wp_group.provider_name))
                          FILTER (
                            WHERE swp_group.offer_type IN ('buy', 'rent')
                              AND btrim(COALESCE(wp_group.provider_name, '')) <> ''
                          ),
                        ARRAY[]::text[]
                      ) AS buy_rent
                    FROM core.show_watch_providers AS swp_group
                    JOIN core.watch_providers AS wp_group ON wp_group.provider_id = swp_group.provider_id
                    WHERE swp_group.show_id = s.id
                      AND btrim(COALESCE(swp_group.region, '')) <> ''
                    GROUP BY upper(btrim(swp_group.region))
                  ) AS grouped
                ),
                '[]'::jsonb
              ) AS watch_provider_regions
            FROM core.show_watch_providers AS swp
            JOIN core.watch_providers AS wp ON wp.provider_id = swp.provider_id
            WHERE swp.show_id = s.id
          ) AS watch ON TRUE
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
        LEFT JOIN core.show_images AS poster ON poster.id = s.primary_poster_image_id
        LEFT JOIN core.show_images AS backdrop ON backdrop.id = s.primary_backdrop_image_id
        LEFT JOIN core.show_images AS logo ON logo.id = s.primary_logo_image_id
        WHERE s.id = %s::uuid
        LIMIT 1
        """,
        [show_id],
    )
    return (_augment_show_detail_row(row) if row else None, 1)


def list_show_seasons(
    show_id: str,
    *,
    limit: int | None = None,
    offset: int | None = None,
    include_episode_signal: bool = False,
) -> tuple[list[dict[str, Any]], int]:
    normalized_limit, normalized_offset = _normalize_pagination(limit, offset)
    if include_episode_signal:
        rows = pg.fetch_all(
            """
            SELECT
                   s.show_name,
                   s.name,
                   s.season_number,
                   s.show_id::text AS show_id,
                   s.title,
                   s.overview,
                   s.air_date,
                   s.premiere_date,
                   s.tmdb_series_id,
                   s.imdb_series_id,
                   s.tmdb_season_id,
                   s.tmdb_season_object_id,
                   s.poster_path,
                   s.url_original_poster,
                   s.external_tvdb_id,
                   s.external_wikidata_id,
                   s.external_ids,
                   s.language,
                   s.fetched_at,
                   s.id::text AS id,
                   s.created_at,
                   s.updated_at,
                   COALESCE(ep.episode_count, 0)::int AS episode_count,
                   COALESCE(ep.episode_airdate_count, 0)::int AS episode_airdate_count,
                   ep.first_episode_air_date,
                   ep.last_episode_air_date,
                   (COALESCE(ep.episode_airdate_count, 0) > 0) AS has_scheduled_or_aired_episode,
                   sf.source_url AS fandom_source_url,
                   sf.page_title AS fandom_page_title
              FROM core.seasons AS s
              LEFT JOIN LATERAL (
                SELECT COUNT(*)::int AS episode_count,
                       COUNT(*) FILTER (WHERE e.air_date IS NOT NULL)::int AS episode_airdate_count,
                       MIN(e.air_date) AS first_episode_air_date,
                       MAX(e.air_date) AS last_episode_air_date
                  FROM core.episodes AS e
                 WHERE e.season_id = s.id
              ) AS ep ON TRUE
              LEFT JOIN LATERAL (
                SELECT sf.source_url, sf.page_title
                FROM core.season_fandom AS sf
                WHERE sf.season_id = s.id
                ORDER BY sf.scraped_at DESC NULLS LAST, sf.id DESC
                LIMIT 1
              ) AS sf ON TRUE
             WHERE s.show_id = %s::uuid
             ORDER BY s.season_number DESC
             LIMIT %s OFFSET %s
            """,
            [show_id, normalized_limit, normalized_offset],
        )
        return [_normalize_show_season_row(row) for row in rows], 1
    rows = pg.fetch_all(
        """
        SELECT
          s.show_name,
          s.name,
          s.season_number,
          s.show_id::text AS show_id,
          s.title,
          s.overview,
          s.air_date,
          s.premiere_date,
          s.tmdb_series_id,
          s.imdb_series_id,
          s.tmdb_season_id,
          s.tmdb_season_object_id,
          s.poster_path,
          s.url_original_poster,
          s.external_tvdb_id,
          s.external_wikidata_id,
          s.external_ids,
          s.language,
          s.fetched_at,
          s.id::text AS id,
          s.created_at,
          s.updated_at,
          sf.source_url AS fandom_source_url,
          sf.page_title AS fandom_page_title
          FROM core.seasons AS s
          LEFT JOIN LATERAL (
            SELECT sf.source_url, sf.page_title
            FROM core.season_fandom AS sf
            WHERE sf.season_id = s.id
            ORDER BY sf.scraped_at DESC NULLS LAST, sf.id DESC
            LIMIT 1
          ) AS sf ON TRUE
         WHERE show_id = %s::uuid
         ORDER BY s.season_number DESC
         LIMIT %s OFFSET %s
        """,
        [show_id, normalized_limit, normalized_offset],
    )
    return [_normalize_show_season_row(row) for row in rows], 1


def get_show_seasons(
    show_id: str,
    *,
    limit: int | None = None,
    offset: int | None = None,
    include_episode_signal: bool = False,
) -> tuple[list[dict[str, Any]], int]:
    return list_show_seasons(
        show_id,
        limit=limit,
        offset=offset,
        include_episode_signal=include_episode_signal,
    )


def get_season_episodes(
    season_id: str,
    *,
    limit: int | None = None,
    offset: int | None = None,
) -> tuple[list[dict[str, Any]], int]:
    normalized_limit, normalized_offset = _normalize_pagination(limit, offset)
    rows = pg.fetch_all(
        """
        SELECT
          e.id::text AS id,
          e.season_number,
          e.episode_number,
          e.title,
          e.synopsis,
          e.overview,
          e.air_date,
          e.runtime,
          e.imdb_rating,
          e.imdb_vote_count,
          e.tmdb_vote_average,
          e.tmdb_vote_count,
          e.url_original_still,
          e.tmdb_episode_id,
          e.imdb_episode_id,
          COALESCE(e.title, CONCAT('Episode ', e.episode_number::text)) AS name
        FROM core.episodes AS e
        WHERE e.season_id = %s::uuid
        ORDER BY e.episode_number ASC, e.id ASC
        LIMIT %s OFFSET %s
        """,
        [season_id, normalized_limit, normalized_offset],
    )
    return rows, 1


def _fetch_show_cast_base_rows(
    show_id: str,
    *,
    include_photos: bool = True,
    cur: Any | None = None,
) -> tuple[list[dict[str, Any]], int]:
    photo_select_sql = """
          COALESCE(primary_photo.photo_url, fallback_photo.photo_url) AS photo_url,
          COALESCE(primary_photo.thumbnail_focus_x, fallback_photo.thumbnail_focus_x) AS thumbnail_focus_x,
          COALESCE(primary_photo.thumbnail_focus_y, fallback_photo.thumbnail_focus_y) AS thumbnail_focus_y,
          COALESCE(primary_photo.thumbnail_zoom, fallback_photo.thumbnail_zoom) AS thumbnail_zoom,
          COALESCE(primary_photo.thumbnail_crop_mode, fallback_photo.thumbnail_crop_mode) AS thumbnail_crop_mode,
    """
    photo_join_sql = """
        LEFT JOIN LATERAL (
          SELECT
            ranked.photo_url,
            CASE
              WHEN ranked.crop ? 'x'
               AND COALESCE(ranked.crop->>'x', '') ~ '^-?[0-9]+(?:\\.[0-9]+)?$'
              THEN (ranked.crop->>'x')::double precision
              ELSE NULL
            END AS thumbnail_focus_x,
            CASE
              WHEN ranked.crop ? 'y'
               AND COALESCE(ranked.crop->>'y', '') ~ '^-?[0-9]+(?:\\.[0-9]+)?$'
              THEN (ranked.crop->>'y')::double precision
              ELSE NULL
            END AS thumbnail_focus_y,
            CASE
              WHEN ranked.crop ? 'zoom'
               AND COALESCE(ranked.crop->>'zoom', '') ~ '^-?[0-9]+(?:\\.[0-9]+)?$'
              THEN (ranked.crop->>'zoom')::double precision
              ELSE NULL
            END AS thumbnail_zoom,
            CASE
              WHEN COALESCE(ranked.crop->>'mode', '') IN ('manual', 'auto')
              THEN ranked.crop->>'mode'
              ELSE NULL
            END AS thumbnail_crop_mode
          FROM (
            SELECT
              COALESCE(
                NULLIF(ma.metadata->>'thumb_url', ''),
                NULLIF(ma.metadata->>'display_url', ''),
                ma.hosted_url
              ) AS photo_url,
              COALESCE(ml.context->'thumbnail_crop', ma.metadata->'thumbnail_crop', '{}'::jsonb) AS crop,
              LOWER(COALESCE(ml.context->>'context_section', '')) AS context_section,
              COALESCE(ml.position, 2147483647) AS position,
              ml.created_at
            FROM core.media_links AS ml
            JOIN core.media_assets AS ma
              ON ma.id = ml.media_asset_id
            WHERE ml.entity_type = 'person'
              AND ml.entity_id = vsc.person_id
              AND ml.kind = 'gallery'
              AND ma.hosted_url IS NOT NULL
          ) AS ranked
          WHERE ranked.photo_url IS NOT NULL
          ORDER BY
            CASE
              WHEN ranked.context_section = 'bravo_profile' THEN 0
              WHEN ranked.context_section IN ('official season announcement', 'official_season_announcement') THEN 1
              ELSE 2
            END,
            ranked.position ASC,
            ranked.created_at DESC
          LIMIT 1
        ) AS primary_photo ON TRUE
        LEFT JOIN LATERAL (
          SELECT
            COALESCE(cp.thumb_url, cp.display_url, cp.hosted_url, cp.url) AS photo_url,
            NULL::double precision AS thumbnail_focus_x,
            NULL::double precision AS thumbnail_focus_y,
            NULL::double precision AS thumbnail_zoom,
            NULL::text AS thumbnail_crop_mode
          FROM core.v_cast_photos AS cp
          WHERE cp.person_id = vsc.person_id
          ORDER BY
            CASE
              WHEN LOWER(COALESCE(cp.context_section, '')) = 'bravo_profile' THEN 0
              WHEN LOWER(COALESCE(cp.context_section, '')) IN (
                'official season announcement',
                'official_season_announcement'
              ) THEN 1
              ELSE 2
            END,
            cp.gallery_index ASC NULLS LAST
          LIMIT 1
        ) AS fallback_photo ON primary_photo.photo_url IS NULL
    """
    if not include_photos:
        photo_select_sql = """
          NULL::text AS photo_url,
          NULL::double precision AS thumbnail_focus_x,
          NULL::double precision AS thumbnail_focus_y,
          NULL::double precision AS thumbnail_zoom,
          NULL::text AS thumbnail_crop_mode,
    """
        photo_join_sql = ""

    rows = _fetch_all_rows(
        f"""
        WITH episode_counts AS (
          SELECT
            vec.person_id,
            COUNT(
              DISTINCT CASE
                WHEN COALESCE(vec.appearance_type, 'appears') <> 'archive_footage'
                THEN vec.episode_id
              END
            )::int AS regular_episodes,
            COUNT(
              DISTINCT CASE
                WHEN COALESCE(vec.appearance_type, '') = 'archive_footage'
                THEN vec.episode_id
              END
            )::int AS archive_episodes
          FROM core.v_episode_credits AS vec
          WHERE vec.show_id = %s::uuid
          GROUP BY vec.person_id
        ),
        self_credit_metadata AS (
          SELECT
            c.person_id,
            MAX(COALESCE(NULLIF(c.metadata->>'episode_count', '')::int, 0))::int AS metadata_episode_count
          FROM core.credits AS c
          WHERE c.show_id = %s::uuid
            AND c.credit_category = 'Self'
          GROUP BY c.person_id
        )
        SELECT
          vsc.id::text AS id,
          vsc.show_id::text AS show_id,
          vsc.show_name,
          vsc.person_id::text AS person_id,
          vsc.cast_member_name,
          vsc.role,
          vsc.billing_order,
          vsc.credit_category,
          vsc.source_type,
          vsc.created_at,
          vsc.updated_at,
          p.full_name,
          p.known_for,
          {photo_select_sql}
          cover.photo_url AS cover_photo_url,
          COALESCE(episode_counts.regular_episodes, 0)::int AS total_episodes,
          COALESCE(episode_counts.archive_episodes, 0)::int AS archive_episode_count,
          CASE
            WHEN COALESCE(episode_counts.regular_episodes, 0) > 0
              THEN COALESCE(episode_counts.regular_episodes, 0)::int
            ELSE COALESCE(self_credit_metadata.metadata_episode_count, 0)::int
          END AS effective_total_episodes
        FROM core.v_show_cast AS vsc
        LEFT JOIN episode_counts
          ON episode_counts.person_id = vsc.person_id
        LEFT JOIN self_credit_metadata
          ON self_credit_metadata.person_id = vsc.person_id
        LEFT JOIN core.people AS p
          ON p.id = vsc.person_id
        LEFT JOIN admin.person_cover_photos AS cover
          ON cover.person_id = vsc.person_id
        {photo_join_sql}
        WHERE vsc.show_id = %s::uuid
        ORDER BY vsc.billing_order ASC NULLS LAST, vsc.person_id ASC
        """,
        [show_id, show_id, show_id],
        cur=cur,
    )
    return rows, 1


def _build_show_cast_fallback_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        **row,
        "total_episodes": row.get("total_episodes"),
        "archive_episode_count": row.get("archive_episode_count"),
    }


def _read_show_cast_effective_total_episodes(row: dict[str, Any]) -> int:
    effective_total = _normalize_int(row.get("effective_total_episodes"))
    if effective_total > 0:
        return effective_total
    return _normalize_int(row.get("total_episodes"))


def _is_voice_only_show_cast_role(role: Any) -> bool:
    normalized = str(role or "").strip().casefold()
    if not normalized:
        return False
    return bool(re.search(r"\bvoice\b", normalized))


def _is_voice_only_show_cast_roles(roles: list[str]) -> bool:
    normalized_roles = [str(role).strip() for role in roles if str(role).strip()]
    if not normalized_roles:
        return False
    return all(_is_voice_only_show_cast_role(role) for role in normalized_roles)


def _filter_show_cast_rows_for_links(
    rows: list[dict[str, Any]],
    *,
    show_total_seasons: int | None,
) -> list[dict[str, Any]]:
    filtered: list[dict[str, Any]] = []
    normalized_total_seasons = max(_normalize_int(show_total_seasons), 0)

    for raw_row in rows:
        row = dict(raw_row)
        regular_episodes = _normalize_int(row.get("total_episodes"))
        archive_episode_count = _normalize_int(row.get("archive_episode_count"))
        effective_total_episodes = _read_show_cast_effective_total_episodes(row)

        if archive_episode_count > 0 and regular_episodes <= 0:
            continue
        if _is_voice_only_show_cast_role(row.get("role")):
            continue
        if effective_total_episodes <= 0:
            continue
        if normalized_total_seasons > 1 and effective_total_episodes <= 3:
            continue

        row["total_episodes"] = effective_total_episodes
        row["effective_total_episodes"] = effective_total_episodes
        filtered.append(row)

    return filtered


def _shape_show_cast_payload(
    rows: list[dict[str, Any]],
    *,
    limit: int,
    offset: int,
    min_episodes: int | None,
    has_explicit_min_episodes: bool,
    exclude_zero_episode_members: bool,
    require_image: bool,
    roster_mode: str,
    eligibility_mode: str = "default",
    links_eligibility_show_total_seasons: int | None = None,
) -> dict[str, Any]:
    membership_rows = [dict(row) for row in rows]
    normalized_eligibility_mode = "links" if eligibility_mode == "links" else "default"
    if normalized_eligibility_mode == "links":
        membership_rows = _filter_show_cast_rows_for_links(
            membership_rows,
            show_total_seasons=links_eligibility_show_total_seasons,
        )
    normalized_roster_mode = "imdb_show_membership" if roster_mode == "imdb_show_membership" else "episode_evidence"
    min_episodes_value = (
        0
        if normalized_roster_mode == "imdb_show_membership" and min_episodes is None
        else DEFAULT_CAST_MIN_EPISODES if min_episodes is None else max(min_episodes, 0)
    )

    cast_rows = (
        membership_rows
        if normalized_roster_mode == "imdb_show_membership"
        else [row for row in membership_rows if _normalize_int(row.get("total_episodes")) > 0]
    )
    archive_rows = [
        row
        for row in membership_rows
        if _normalize_int(row.get("total_episodes")) <= 0 and _normalize_int(row.get("archive_episode_count")) > 0
    ]
    if normalized_eligibility_mode == "links":
        archive_rows = []
    cast_source = normalized_roster_mode
    eligibility_warning: str | None = None

    cast_rows = [row for row in cast_rows if _normalize_int(row.get("total_episodes")) >= min_episodes_value]

    if exclude_zero_episode_members:
        cast_rows = [row for row in cast_rows if _normalize_int(row.get("total_episodes")) > 0]

    if (
        normalized_roster_mode == "episode_evidence"
        and not has_explicit_min_episodes
        and not cast_rows
        and membership_rows
    ):
        cast_rows = [_build_show_cast_fallback_row(row) for row in membership_rows]
        cast_source = "show_fallback"
        eligibility_warning = SHOW_FALLBACK_WARNING

    if require_image:
        cast_rows = [row for row in cast_rows if row.get("photo_url")]
        archive_rows = [row for row in archive_rows if row.get("photo_url")]

    paginated_cast = cast_rows[offset : offset + limit]
    paginated_archive = archive_rows[offset : offset + limit]
    return {
        "cast": paginated_cast,
        "archive_footage_cast": paginated_archive,
        "cast_source": cast_source,
        "eligibility_warning": eligibility_warning,
        "pagination": {
            "limit": limit,
            "offset": offset,
            "count": len(paginated_cast),
        },
    }


def get_show_cast(
    show_id: str,
    *,
    limit: int | None = None,
    offset: int | None = None,
    min_episodes: int | None = None,
    has_explicit_min_episodes: bool = False,
    exclude_zero_episode_members: bool = False,
    require_image: bool = False,
    roster_mode: str = "episode_evidence",
    photo_fallback: str = "none",
    include_photos: bool = True,
    eligibility_mode: str = "default",
) -> tuple[dict[str, Any], int]:
    del photo_fallback
    normalized_limit, normalized_offset = _normalize_pagination(limit, offset)
    rows, query_count = _fetch_show_cast_base_rows(show_id, include_photos=include_photos)
    links_eligibility_show_total_seasons: int | None = None
    if eligibility_mode == "links":
        show_row = pg.fetch_one(
            """
            SELECT show_total_seasons
            FROM core.shows
            WHERE id = %s::uuid
            """,
            [show_id],
        ) or {}
        links_eligibility_show_total_seasons = _normalize_int(show_row.get("show_total_seasons")) or None
        query_count += 1
    payload = _shape_show_cast_payload(
        rows,
        limit=normalized_limit,
        offset=normalized_offset,
        min_episodes=min_episodes,
        has_explicit_min_episodes=has_explicit_min_episodes,
        exclude_zero_episode_members=exclude_zero_episode_members,
        require_image=require_image,
        roster_mode=roster_mode,
        eligibility_mode=eligibility_mode,
        links_eligibility_show_total_seasons=links_eligibility_show_total_seasons,
    )
    return payload, query_count


def get_show_links_eligible_people(
    show_id: str,
    *,
    person_ids: set[str] | None = None,
) -> tuple[list[dict[str, Any]], int]:
    rows, query_count = _fetch_show_cast_base_rows(show_id, include_photos=False)
    show_row = pg.fetch_one(
        """
        SELECT show_total_seasons
        FROM core.shows
        WHERE id = %s::uuid
        """,
        [show_id],
    ) or {}
    query_count += 1
    filtered_rows = _filter_show_cast_rows_for_links(
        rows,
        show_total_seasons=_normalize_int(show_row.get("show_total_seasons")) or None,
    )
    if person_ids:
        normalized_person_ids = {str(value).strip() for value in person_ids if str(value).strip()}
        filtered_rows = [
            row for row in filtered_rows if str(row.get("person_id") or "").strip() in normalized_person_ids
        ]
    return filtered_rows, query_count


def get_show_credits(show_id: str) -> tuple[dict[str, Any], int]:
    cast_roster_rows = _fetch_all_rows(
        """
        SELECT
          c.show_id::text AS show_id,
          c.person_id::text AS person_id,
          COALESCE(po.full_name_override, c.person_name) AS person_name,
          c.total_episodes,
          c.archive_episodes,
          c.seasons_appeared,
          c.season_numbers,
          c.latest_season,
          c.roles,
          cp.display_url AS photo_url
        FROM core.v_show_cast_roles_enriched AS c
        LEFT JOIN core.people_overrides AS po
          ON po.person_id = c.person_id
        LEFT JOIN LATERAL (
          SELECT
            COALESCE(ph.hosted_url, ph.image_url, ph.url, ph.thumb_url) AS display_url
          FROM core.cast_photos AS ph
          WHERE ph.person_id = c.person_id
          ORDER BY ph.gallery_index ASC NULLS LAST
          LIMIT 1
        ) AS cp ON true
        WHERE c.show_id = %s::uuid
        ORDER BY
          c.total_episodes DESC NULLS LAST,
          c.latest_season DESC NULLS LAST,
          COALESCE(po.full_name_override, c.person_name) ASC NULLS LAST,
          c.person_id ASC
        """,
        [show_id],
    )
    role_rows = _fetch_all_rows(
        """
        SELECT
          sra.person_id::text AS person_id,
          array_remove(array_agg(DISTINCT rc.name), NULL) AS role_names
        FROM core.show_cast_role_assignments AS sra
        JOIN core.show_role_catalog AS rc
          ON rc.id = sra.role_id
        WHERE sra.show_id = %s::uuid
          AND rc.is_active = true
        GROUP BY sra.person_id
        """,
        [show_id],
    )
    self_credit_metadata_rows = _fetch_all_rows(
        """
        SELECT
          c.person_id::text AS person_id,
          c.metadata
        FROM core.credits AS c
        WHERE c.show_id = %s::uuid
          AND c.credit_category = 'Self'
        ORDER BY
          COALESCE(NULLIF(c.metadata->>'episode_count', '')::int, 0) DESC,
          c.billing_order ASC NULLS LAST,
          c.id ASC
        """,
        [show_id],
    )

    crew_rows = _fetch_all_rows(
        """
        SELECT
          c.id::text AS credit_id,
          c.show_id::text AS show_id,
          c.person_id::text AS person_id,
          COALESCE(po.full_name_override, p.full_name) AS person_name,
          c.credit_category,
          c.role,
          c.billing_order,
          c.source_type,
          c.metadata,
          c.updated_at,
          s.imdb_id
        FROM core.credits AS c
        JOIN core.people AS p
          ON p.id = c.person_id
        LEFT JOIN core.people_overrides AS po
          ON po.person_id = c.person_id
        JOIN core.shows AS s
          ON s.id = c.show_id
        WHERE c.show_id = %s::uuid
          AND c.credit_category = ANY(%s::text[])
        ORDER BY
          array_position(%s::text[], c.credit_category),
          COALESCE(NULLIF(c.metadata->>'display_order', '')::int, c.billing_order, 2147483647),
          COALESCE(po.full_name_override, p.full_name) ASC NULLS LAST,
          c.id ASC
        """,
        [show_id, list(IMDB_CREW_CREDIT_CATEGORIES), list(IMDB_CREW_CREDIT_CATEGORIES)],
    )

    role_map: dict[str, list[str]] = {}
    for row in role_rows:
        person_id = str(row.get("person_id") or "").strip()
        if not person_id:
            continue
        role_names = sorted(
            {
                str(value).strip()
                for value in (row.get("role_names") or [])
                if isinstance(value, str) and str(value).strip()
            }
        )
        if role_names:
            role_map[person_id] = role_names

    self_credit_metadata_by_person: dict[str, dict[str, Any]] = {}
    for row in self_credit_metadata_rows:
        person_id = str(row.get("person_id") or "").strip()
        metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
        if person_id and person_id not in self_credit_metadata_by_person:
            self_credit_metadata_by_person[person_id] = metadata

    cast_roster = []
    for row in cast_roster_rows:
        person_id = str(row.get("person_id") or "").strip()
        selected_roles = role_map.get(person_id, [])
        if not selected_roles:
            selected_roles = sorted(
                {
                    str(value).strip()
                    for value in (row.get("roles") or [])
                    if isinstance(value, str) and str(value).strip()
                }
            )
        fallback_metadata = self_credit_metadata_by_person.get(person_id, {})
        total_episodes = _normalize_int(row.get("total_episodes"))
        if not total_episodes:
            total_episodes = _read_people_count(fallback_metadata.get("episode_count"))
        total_episodes = total_episodes or 0
        archive_episodes = _normalize_int(row.get("archive_episodes"))
        if archive_episodes > 0 and total_episodes <= 0:
            continue
        if _is_voice_only_show_cast_roles(selected_roles):
            continue
        if total_episodes <= 0:
            continue
        cast_roster.append(
            {
                "show_id": str(row.get("show_id") or show_id),
                "person_id": person_id,
                "person_name": row.get("person_name"),
                "photo_url": row.get("photo_url"),
                "total_episodes": total_episodes,
                "archive_episodes": archive_episodes,
                "seasons_appeared": _normalize_int(row.get("seasons_appeared")),
                "season_numbers": [value for value in (row.get("season_numbers") or []) if isinstance(value, int)],
                "latest_season": _normalize_int(row.get("latest_season")) or None,
                "roles": selected_roles,
            }
        )

    crew_sections_by_category: dict[str, list[dict[str, Any]]] = {}
    crew_grouped_sections_by_category: dict[str, dict[str, dict[str, Any]]] = {}
    latest_updated_at: str | None = None
    source_page_url: str | None = None
    show_imdb_id: str | None = None
    for row in crew_rows:
        metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
        credit_category = str(row.get("credit_category") or "").strip()
        if not credit_category:
            continue
        updated_at_value = _normalize_json_safe_value(row.get("updated_at"))
        latest_updated_at = max(
            [value for value in [latest_updated_at, updated_at_value] if isinstance(value, str)],
            default=latest_updated_at,
        )
        source_page_url = source_page_url or _metadata_text(metadata, "source_page_url")
        show_imdb_id = show_imdb_id or (str(row.get("imdb_id") or "").strip() or None)
        row_payload = {
            "credit_id": str(row.get("credit_id") or ""),
            "person_id": str(row.get("person_id") or ""),
            "person_name": row.get("person_name"),
            "role": row.get("role"),
            "billing_order": _normalize_int(row.get("billing_order")) or None,
            "source_type": row.get("source_type"),
            "episode_count": _read_people_count(metadata.get("episode_count")),
            "episodes_label": _metadata_text(metadata, "episodes_label"),
            "years_label": _metadata_text(metadata, "years_label"),
            "imdb_name_id": _metadata_text(metadata, "imdb_name_id"),
            "display_order": _read_people_count(metadata.get("display_order")),
        }
        crew_sections_by_category.setdefault(credit_category, []).append(row_payload)
        grouped_rows = crew_grouped_sections_by_category.setdefault(credit_category, {})
        person_key = row_payload["person_id"] or row_payload["credit_id"]
        grouped_row = grouped_rows.setdefault(
            person_key,
            {
                "person_id": row_payload["person_id"],
                "person_name": row_payload["person_name"],
                "role_lines": [],
            },
        )
        grouped_row["role_lines"].append(
            {
                "credit_id": row_payload["credit_id"],
                "role": row_payload["role"],
                "billing_order": row_payload["billing_order"],
                "source_type": row_payload["source_type"],
                "episode_count": row_payload["episode_count"],
                "episodes_label": row_payload["episodes_label"],
                "years_label": row_payload["years_label"],
                "imdb_name_id": row_payload["imdb_name_id"],
                "display_order": row_payload["display_order"],
            }
        )

    crew_sections = [
        {
            "title": category,
            "rows": rows,
            "grouped_rows": list(crew_grouped_sections_by_category.get(category, {}).values()),
        }
        for category, rows in sorted(
            crew_sections_by_category.items(),
            key=lambda item: IMDB_CREW_CREDIT_CATEGORY_ORDER.get(item[0], 9999),
        )
    ]

    payload = {
        "cast_roster": cast_roster,
        "crew_sections": crew_sections,
        "source_metadata": {
            "source_page_url": source_page_url
            or (f"https://www.imdb.com/title/{show_imdb_id}/fullcredits/" if show_imdb_id else None),
            "show_imdb_id": show_imdb_id,
            "last_synced_at": latest_updated_at,
            "crew_categories": list(IMDB_CREW_CREDIT_CATEGORIES),
        },
    }
    return payload, 4


def _fetch_season_cast_base_rows(
    show_id: str,
    season_number: int,
    *,
    cur: Any | None = None,
) -> tuple[list[dict[str, Any]], int]:
    rows = _fetch_all_rows(
        """
        WITH season_counts AS (
          SELECT
            vec.person_id,
            COUNT(
              DISTINCT CASE
                WHEN COALESCE(vec.appearance_type, 'appears') <> 'archive_footage'
                THEN vec.episode_id
              END
            )::int AS regular_episodes,
            COUNT(
              DISTINCT CASE
                WHEN COALESCE(vec.appearance_type, '') = 'archive_footage'
                THEN vec.episode_id
              END
            )::int AS archive_episodes
          FROM core.v_episode_credits AS vec
          WHERE vec.show_id = %s::uuid
            AND vec.season_number = %s::int
          GROUP BY vec.person_id
        ),
        total_counts AS (
          SELECT
            vpss.person_id,
            MAX(vpss.total_episodes)::int AS total_episodes
          FROM core.v_person_show_seasons AS vpss
          WHERE vpss.show_id = %s::uuid
          GROUP BY vpss.person_id
        )
        SELECT
          vsc.person_id::text AS person_id,
          COALESCE(p.full_name, vsc.cast_member_name) AS person_name,
          COALESCE(season_counts.regular_episodes, 0)::int AS episodes_in_season,
          COALESCE(total_counts.total_episodes, 0)::int AS total_episodes,
          COALESCE(primary_photo.photo_url, fallback_photo.photo_url) AS photo_url,
          COALESCE(primary_photo.thumbnail_focus_x, fallback_photo.thumbnail_focus_x) AS thumbnail_focus_x,
          COALESCE(primary_photo.thumbnail_focus_y, fallback_photo.thumbnail_focus_y) AS thumbnail_focus_y,
          COALESCE(primary_photo.thumbnail_zoom, fallback_photo.thumbnail_zoom) AS thumbnail_zoom,
          COALESCE(primary_photo.thumbnail_crop_mode, fallback_photo.thumbnail_crop_mode) AS thumbnail_crop_mode,
          COALESCE(season_counts.archive_episodes, 0)::int AS archive_episodes_in_season,
          vsc.billing_order
        FROM core.v_show_cast AS vsc
        LEFT JOIN season_counts
          ON season_counts.person_id = vsc.person_id
        LEFT JOIN total_counts
          ON total_counts.person_id = vsc.person_id
        LEFT JOIN core.people AS p
          ON p.id = vsc.person_id
        LEFT JOIN LATERAL (
          SELECT
            ranked.photo_url,
            CASE
              WHEN ranked.crop ? 'x'
               AND COALESCE(ranked.crop->>'x', '') ~ '^-?[0-9]+(?:\\.[0-9]+)?$'
              THEN (ranked.crop->>'x')::double precision
              ELSE NULL
            END AS thumbnail_focus_x,
            CASE
              WHEN ranked.crop ? 'y'
               AND COALESCE(ranked.crop->>'y', '') ~ '^-?[0-9]+(?:\\.[0-9]+)?$'
              THEN (ranked.crop->>'y')::double precision
              ELSE NULL
            END AS thumbnail_focus_y,
            CASE
              WHEN ranked.crop ? 'zoom'
               AND COALESCE(ranked.crop->>'zoom', '') ~ '^-?[0-9]+(?:\\.[0-9]+)?$'
              THEN (ranked.crop->>'zoom')::double precision
              ELSE NULL
            END AS thumbnail_zoom,
            CASE
              WHEN COALESCE(ranked.crop->>'mode', '') IN ('manual', 'auto')
              THEN ranked.crop->>'mode'
              ELSE NULL
            END AS thumbnail_crop_mode
          FROM (
            SELECT
              COALESCE(
                NULLIF(ma.metadata->>'thumb_url', ''),
                NULLIF(ma.metadata->>'display_url', ''),
                ma.hosted_url
              ) AS photo_url,
              COALESCE(ml.context->'thumbnail_crop', ma.metadata->'thumbnail_crop', '{}'::jsonb) AS crop,
              LOWER(COALESCE(ml.context->>'context_section', '')) AS context_section,
              COALESCE(ml.position, 2147483647) AS position,
              ml.created_at
            FROM core.media_links AS ml
            JOIN core.media_assets AS ma
              ON ma.id = ml.media_asset_id
            WHERE ml.entity_type = 'person'
              AND ml.entity_id = vsc.person_id
              AND ml.kind = 'gallery'
              AND ma.hosted_url IS NOT NULL
          ) AS ranked
          WHERE ranked.photo_url IS NOT NULL
          ORDER BY
            CASE
              WHEN ranked.context_section = 'bravo_profile' THEN 0
              WHEN ranked.context_section IN ('official season announcement', 'official_season_announcement') THEN 1
              ELSE 2
            END,
            ranked.position ASC,
            ranked.created_at DESC
          LIMIT 1
        ) AS primary_photo ON TRUE
        LEFT JOIN LATERAL (
          SELECT
            COALESCE(cp.thumb_url, cp.display_url, cp.hosted_url, cp.url) AS photo_url,
            NULL::double precision AS thumbnail_focus_x,
            NULL::double precision AS thumbnail_focus_y,
            NULL::double precision AS thumbnail_zoom,
            NULL::text AS thumbnail_crop_mode
          FROM core.v_cast_photos AS cp
          WHERE cp.person_id = vsc.person_id
          ORDER BY
            CASE
              WHEN LOWER(COALESCE(cp.context_section, '')) = 'bravo_profile' AND cp.season = %s::int THEN 0
              WHEN LOWER(COALESCE(cp.context_section, '')) IN (
                'official season announcement',
                'official_season_announcement'
              ) AND cp.season = %s::int THEN 1
              WHEN LOWER(COALESCE(cp.context_section, '')) = 'bravo_profile' THEN 2
              WHEN LOWER(COALESCE(cp.context_section, '')) IN (
                'official season announcement',
                'official_season_announcement'
              ) THEN 3
              ELSE 4
            END,
            cp.gallery_index ASC NULLS LAST
          LIMIT 1
        ) AS fallback_photo ON primary_photo.photo_url IS NULL
        WHERE vsc.show_id = %s::uuid
        ORDER BY vsc.billing_order ASC NULLS LAST, vsc.person_id ASC
        """,
        [show_id, season_number, show_id, season_number, season_number, show_id],
        cur=cur,
    )
    return rows, 1


def _shape_season_cast_payload(
    rows: list[dict[str, Any]],
    *,
    limit: int,
    offset: int,
    include_archive_only: bool,
) -> dict[str, Any]:
    season_rows = [dict(row) for row in rows]
    if include_archive_only:
        cast_rows = [
            row
            for row in season_rows
            if _normalize_int(row.get("episodes_in_season")) > 0
            or _normalize_int(row.get("archive_episodes_in_season")) > 0
        ]
    else:
        cast_rows = [row for row in season_rows if _normalize_int(row.get("episodes_in_season")) > 0]

    cast_source = "season_evidence"
    eligibility_warning: str | None = None
    if not include_archive_only and not cast_rows and season_rows:
        cast_rows = [
            {
                "person_id": row.get("person_id"),
                "person_name": row.get("person_name"),
                "episodes_in_season": 0,
                "total_episodes": 0,
                "photo_url": row.get("photo_url"),
                "thumbnail_focus_x": row.get("thumbnail_focus_x"),
                "thumbnail_focus_y": row.get("thumbnail_focus_y"),
                "thumbnail_zoom": row.get("thumbnail_zoom"),
                "thumbnail_crop_mode": row.get("thumbnail_crop_mode"),
                "archive_episodes_in_season": 0,
            }
            for row in season_rows
        ]
        cast_source = "show_fallback"
        eligibility_warning = SEASON_FALLBACK_WARNING

    paginated_cast = cast_rows[offset : offset + limit]
    return {
        "cast": paginated_cast,
        "cast_source": cast_source,
        "eligibility_warning": eligibility_warning,
        "pagination": {
            "limit": limit,
            "offset": offset,
            "count": len(paginated_cast),
        },
        "include_archive_only": include_archive_only,
    }


def get_season_cast(
    show_id: str,
    season_number: int,
    *,
    limit: int | None = None,
    offset: int | None = None,
    include_archive_only: bool = False,
    photo_fallback: str = "none",
) -> tuple[dict[str, Any], int]:
    del photo_fallback
    normalized_limit, normalized_offset = _normalize_pagination(limit, offset)
    rows, query_count = _fetch_season_cast_base_rows(show_id, season_number)
    payload = _shape_season_cast_payload(
        rows,
        limit=normalized_limit,
        offset=normalized_offset,
        include_archive_only=include_archive_only,
    )
    return payload, query_count


def _recent_people_views(
    firebase_uid: str,
    limit: int,
    *,
    cur: Any | None = None,
) -> tuple[list[dict[str, Any]], int]:
    try:
        rows = _fetch_all_rows(
            """
            SELECT
              rv.person_id::text AS person_id,
              p.full_name,
              p.known_for,
              COALESCE(photo.thumb_url, photo.display_url, photo.hosted_url, photo.url) AS photo_url,
              rv.show_context,
              rv.view_count,
              rv.first_viewed_at,
              rv.last_viewed_at
            FROM admin.recent_people_views AS rv
            JOIN core.people AS p
              ON p.id = rv.person_id
            LEFT JOIN LATERAL (
              SELECT
                cp.thumb_url,
                cp.display_url,
                cp.hosted_url,
                cp.url
              FROM core.v_cast_photos AS cp
              WHERE cp.person_id = rv.person_id
              ORDER BY
                CASE
                  WHEN lower(COALESCE(cp.context_section, '')) = 'bravo_profile' THEN 0
                  WHEN lower(COALESCE(cp.context_section, '')) IN (
                    'official season announcement',
                    'official_season_announcement'
                  ) THEN 1
                  ELSE 2
                END,
                cp.gallery_index ASC NULLS LAST
              LIMIT 1
            ) AS photo ON true
            WHERE rv.firebase_uid = %s
            ORDER BY rv.last_viewed_at DESC, rv.person_id ASC
            LIMIT %s
            """,
            [firebase_uid, limit],
            cur=cur,
        )
    except Exception as error:
        message = str(error).lower()
        if "admin.recent_people_views" in message and "does not exist" in message:
            logger.warning("[admin-show-reads] recently_viewed_unavailable relation=admin.recent_people_views")
            return [], 0
        raise
    return rows, 1


def _people_most_shows(limit: int, *, cur: Any | None = None) -> tuple[list[dict[str, Any]], int]:
    rows = _fetch_all_rows(
        f"""
        WITH shows_with_slug AS (
          SELECT
            s.id,
            s.slug,
            {SHOW_SLUG_SQL} AS computed_slug,
            COUNT(*) OVER (PARTITION BY {SHOW_SLUG_SQL}) AS slug_collision_count
          FROM core.shows AS s
        ),
        person_metrics AS (
          SELECT
            vpss.person_id,
            MAX(vpss.person_name) AS person_name,
            COUNT(DISTINCT vpss.show_id)::int AS metric_value
          FROM core.v_person_show_seasons AS vpss
          GROUP BY vpss.person_id
        ),
        ranked_people AS (
          SELECT
            person_metrics.person_id,
            person_metrics.person_name,
            person_metrics.metric_value
          FROM person_metrics
          ORDER BY
            person_metrics.metric_value DESC,
            person_metrics.person_name ASC NULLS LAST
          LIMIT %s
        )
        SELECT
          person_metrics.person_id::text AS person_id,
          COALESCE(p.full_name, person_metrics.person_name) AS full_name,
          p.known_for,
          COALESCE(photo.thumb_url, photo.display_url, photo.hosted_url, photo.url) AS photo_url,
          person_metrics.metric_value,
          show_context.canonical_slug AS show_context,
          NULL::timestamptz AS latest_at
        FROM ranked_people AS person_metrics
        LEFT JOIN core.people AS p ON p.id = person_metrics.person_id
        {PERSON_CARD_PHOTO_SQL}
        {PERSON_SHOW_CONTEXT_SQL}
        ORDER BY
          person_metrics.metric_value DESC,
          COALESCE(p.full_name, person_metrics.person_name) ASC NULLS LAST
        LIMIT %s
        """,
        [limit, limit],
        cur=cur,
    )
    return rows, 1


def _people_top_episodes(limit: int, *, cur: Any | None = None) -> tuple[list[dict[str, Any]], int]:
    rows = _fetch_all_rows(
        f"""
        WITH shows_with_slug AS (
          SELECT
            s.id,
            s.slug,
            {SHOW_SLUG_SQL} AS computed_slug,
            COUNT(*) OVER (PARTITION BY {SHOW_SLUG_SQL}) AS slug_collision_count
          FROM core.shows AS s
        ),
        person_metrics AS (
          SELECT
            vec.person_id,
            MAX(vec.person_name) AS person_name,
            COUNT(DISTINCT vec.episode_id)::int AS metric_value
          FROM core.v_episode_credits AS vec
          WHERE COALESCE(vec.appearance_type, 'appears') <> 'archive_footage'
          GROUP BY vec.person_id
        ),
        ranked_people AS (
          SELECT
            person_metrics.person_id,
            person_metrics.person_name,
            person_metrics.metric_value
          FROM person_metrics
          ORDER BY
            person_metrics.metric_value DESC,
            person_metrics.person_name ASC NULLS LAST
          LIMIT %s
        )
        SELECT
          person_metrics.person_id::text AS person_id,
          COALESCE(p.full_name, person_metrics.person_name) AS full_name,
          p.known_for,
          COALESCE(photo.thumb_url, photo.display_url, photo.hosted_url, photo.url) AS photo_url,
          person_metrics.metric_value,
          show_context.canonical_slug AS show_context,
          NULL::timestamptz AS latest_at
        FROM ranked_people AS person_metrics
        LEFT JOIN core.people AS p ON p.id = person_metrics.person_id
        {PERSON_CARD_PHOTO_SQL}
        {PERSON_SHOW_CONTEXT_SQL}
        ORDER BY
          person_metrics.metric_value DESC,
          COALESCE(p.full_name, person_metrics.person_name) ASC NULLS LAST
        LIMIT %s
        """,
        [limit, limit],
        cur=cur,
    )
    return rows, 1


def _people_recently_added(limit: int, *, cur: Any | None = None) -> tuple[list[dict[str, Any]], int]:
    rows = _fetch_all_rows(
        f"""
        WITH shows_with_slug AS (
          SELECT
            s.id,
            s.slug,
            {SHOW_SLUG_SQL} AS computed_slug,
            COUNT(*) OVER (PARTITION BY {SHOW_SLUG_SQL}) AS slug_collision_count
          FROM core.shows AS s
        ),
        person_metrics AS (
          SELECT
            p.id AS person_id,
            p.full_name AS person_name,
            0::int AS metric_value,
            GREATEST(p.updated_at, p.created_at) AS latest_at
          FROM core.people AS p
        ),
        ranked_people AS (
          SELECT
            person_metrics.person_id,
            person_metrics.person_name,
            person_metrics.metric_value,
            person_metrics.latest_at
          FROM person_metrics
          ORDER BY
            person_metrics.latest_at DESC NULLS LAST,
            person_metrics.person_name ASC
          LIMIT %s
        )
        SELECT
          person_metrics.person_id::text AS person_id,
          p.full_name,
          p.known_for,
          COALESCE(photo.thumb_url, photo.display_url, photo.hosted_url, photo.url) AS photo_url,
          person_metrics.metric_value,
          show_context.canonical_slug AS show_context,
          person_metrics.latest_at
        FROM ranked_people AS person_metrics
        JOIN core.people AS p ON p.id = person_metrics.person_id
        {PERSON_CARD_PHOTO_SQL}
        {PERSON_SHOW_CONTEXT_SQL}
        ORDER BY
          person_metrics.latest_at DESC NULLS LAST,
          p.full_name ASC
        LIMIT %s
        """,
        [limit, limit],
        cur=cur,
    )
    return rows, 1


def _people_most_popular(limit: int, *, cur: Any | None = None) -> tuple[list[dict[str, Any]], int]:
    rows = _fetch_all_rows(
        f"""
        WITH shows_with_slug AS (
          SELECT
            s.id,
            s.slug,
            {SHOW_SLUG_SQL} AS computed_slug,
            COUNT(*) OVER (PARTITION BY {SHOW_SLUG_SQL}) AS slug_collision_count
          FROM core.shows AS s
        ),
        news_mentions AS (
          SELECT
            ssl.source_id,
            COALESCE(
              NULLIF(news_item->>'published_at', '')::timestamptz,
              ssl.updated_at
            ) AS published_at,
            NULLIF(trim(tag->>'person_id'), '') AS person_id_token,
            NULLIF(trim(tag->>'name'), '') AS person_name_token
          FROM core.show_source_latest AS ssl
          CROSS JOIN LATERAL jsonb_array_elements(
            CASE
              WHEN jsonb_typeof(ssl.payload->'normalized'->'news') = 'array'
                THEN ssl.payload->'normalized'->'news'
              ELSE '[]'::jsonb
            END
          ) AS news_item
          CROSS JOIN LATERAL jsonb_array_elements(
            CASE
              WHEN jsonb_typeof(news_item->'person_tags') = 'array'
                THEN news_item->'person_tags'
              ELSE '[]'::jsonb
            END
          ) AS tag
          WHERE
            ssl.source_id IN ('google_news', 'bravo')
            AND COALESCE(ssl.variant, 'default') = 'default'
        ),
        resolved_mentions AS (
          SELECT
            COALESCE(p_by_id.id, p_by_name.id) AS person_id,
            mentions.source_id,
            mentions.published_at
          FROM news_mentions AS mentions
          LEFT JOIN core.people AS p_by_id
            ON mentions.person_id_token ~* '^[0-9a-f-]{36}$'
           AND p_by_id.id = mentions.person_id_token::uuid
          LEFT JOIN core.people AS p_by_name
            ON p_by_id.id IS NULL
           AND mentions.person_name_token IS NOT NULL
           AND lower(p_by_name.full_name) = lower(mentions.person_name_token)
          WHERE COALESCE(p_by_id.id, p_by_name.id) IS NOT NULL
        ),
        person_metrics AS (
          SELECT
            resolved_mentions.person_id,
            COUNT(*)::int AS metric_value,
            MAX(resolved_mentions.published_at) AS latest_at
          FROM resolved_mentions
          GROUP BY resolved_mentions.person_id
        ),
        ranked_people AS (
          SELECT
            person_metrics.person_id,
            person_metrics.metric_value,
            person_metrics.latest_at
          FROM person_metrics
          ORDER BY
            person_metrics.metric_value DESC,
            person_metrics.latest_at DESC NULLS LAST,
            person_metrics.person_id ASC
          LIMIT %s
        )
        SELECT
          person_metrics.person_id::text AS person_id,
          p.full_name,
          p.known_for,
          COALESCE(photo.thumb_url, photo.display_url, photo.hosted_url, photo.url) AS photo_url,
          person_metrics.metric_value,
          show_context.canonical_slug AS show_context,
          person_metrics.latest_at
        FROM ranked_people AS person_metrics
        JOIN core.people AS p ON p.id = person_metrics.person_id
        {PERSON_CARD_PHOTO_SQL}
        {PERSON_SHOW_CONTEXT_SQL}
        ORDER BY
          person_metrics.metric_value DESC,
          person_metrics.latest_at DESC NULLS LAST,
          p.full_name ASC
        LIMIT %s
        """,
        [limit, limit],
        cur=cur,
    )
    return rows, 1


def _map_people_home_item(row: dict[str, Any], metric_label: str, metric_value: int | None = None) -> dict[str, Any]:
    full_name = (row.get("full_name") or "").strip() or "Unknown Person"
    return {
        "person_id": row["person_id"],
        "person_slug": _build_person_route_slug(full_name, row["person_id"]),
        "full_name": full_name,
        "known_for": row.get("known_for"),
        "photo_url": row.get("photo_url"),
        "show_context": row.get("show_context"),
        "metric_label": metric_label,
        "metric_value": int(metric_value if metric_value is not None else row.get("metric_value") or 0),
        "latest_at": row.get("latest_at"),
    }


def get_people_home(limit: int | None = None, *, firebase_uid: str | None = None) -> tuple[dict[str, Any], int]:
    normalized_limit = _normalize_people_home_limit(limit)
    query_count = 0

    def build_section(loader: Any, *, cur: Any) -> dict[str, Any]:
        nonlocal query_count
        try:
            rows, section_queries, metric_label, metric_value_key = loader(cur)
            query_count += section_queries
            items = [
                _map_people_home_item(
                    row,
                    metric_label,
                    int(row.get(metric_value_key) or 0) if metric_value_key else None,
                )
                for row in rows
            ]
            return {"items": items, "error": None}
        except Exception as error:  # pragma: no cover - route contract handles section failures
            return {"items": [], "error": str(error) or "Failed to load section"}

    with pg.db_read_connection(label="admin_show_reads.people_home") as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            sections = {
                "recentlyViewed": build_section(
                    lambda _cur: (
                        [],
                        0,
                        "Views",
                        "view_count",
                    )
                    if not firebase_uid
                    else (
                        lambda result: (
                            [{**row, "latest_at": row.get("last_viewed_at")} for row in result[0]],
                            result[1],
                            "Views",
                            "view_count",
                        )
                    )(_recent_people_views(firebase_uid, normalized_limit, cur=cur)),
                    cur=cur,
                ),
                "mostPopular": build_section(
                    lambda _cur: (lambda result: (result[0], result[1], "News Score", None))(
                        _people_most_popular(normalized_limit, cur=cur)
                    ),
                    cur=cur,
                ),
                "mostShows": build_section(
                    lambda _cur: (lambda result: (result[0], result[1], "Shows", None))(
                        _people_most_shows(normalized_limit, cur=cur)
                    ),
                    cur=cur,
                ),
                "topEpisodes": build_section(
                    lambda _cur: (lambda result: (result[0], result[1], "Episodes", None))(
                        _people_top_episodes(normalized_limit, cur=cur)
                    ),
                    cur=cur,
                ),
                "recentlyAdded": build_section(
                    lambda _cur: (lambda result: (result[0], result[1], "Recently Added", None))(
                        _people_recently_added(normalized_limit, cur=cur)
                    ),
                    cur=cur,
                ),
            }

    return (
        {
            "sections": sections,
            "pagination": {"limit": normalized_limit},
        },
        query_count,
    )


search_admin_global = search_global
search_admin_content = search_global
