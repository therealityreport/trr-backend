"""Admin endpoints for brands shows/franchise workflows."""

from __future__ import annotations

import base64
import hashlib
import io
import ipaddress
import json
import logging
import re
from collections.abc import Callable
from datetime import UTC, datetime
from functools import lru_cache
from typing import Any, Literal
from urllib.parse import urlparse

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from api.auth import AdminUser
from api.deps import SupabaseAdminClient, get_supabase_admin_client
from trr_backend.db import pg
from trr_backend.integrations.free_logo_sources import (
    FREE_LOGO_SOURCE_PROVIDERS,
    build_source_query_profile,
    collect_free_logo_candidates,
)
from trr_backend.media.s3_mirror import download_image
from trr_backend.repositories import brand_families, brands_franchises

router = APIRouter(prefix="/admin/brands", tags=["admin-brands"])
logger = logging.getLogger(__name__)


class UpdateFranchiseRuleRequest(BaseModel):
    name: str | None = None
    primary_url: str | None = None
    review_allpages_url: str | None = None
    match_terms: list[str] | None = None
    aliases: list[str] | None = None
    community_domains: list[str] | None = None
    include_allpages_scan: bool | None = None
    source_rank: int | None = Field(default=None, ge=0)
    network_terms: list[str] | None = None
    is_active: bool | None = None


class ApplyFranchiseRuleRequest(BaseModel):
    missing_only: bool = True
    dry_run: bool = True


class CreateBrandFamilyRequest(BaseModel):
    family_key: str | None = None
    display_name: str
    owner_wikidata_id: str | None = None
    owner_label: str | None = None
    is_active: bool = True
    notes: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class PatchBrandFamilyRequest(BaseModel):
    display_name: str | None = None
    owner_wikidata_id: str | None = None
    owner_label: str | None = None
    is_active: bool | None = None
    notes: str | None = None
    metadata: dict[str, Any] | None = None


class AddBrandFamilyMemberRequest(BaseModel):
    entity_type: Literal["network", "streaming"]
    entity_key: str
    entity_display_name: str | None = None
    source: Literal["manual", "suggested_owner", "system"] = "manual"
    confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    metadata: dict[str, Any] = Field(default_factory=dict)


class CreateBrandFamilyLinkRuleRequest(BaseModel):
    link_group: Literal["official", "social", "knowledge", "cast_announcements", "other"] = "knowledge"
    link_kind: str
    label: str | None = None
    url: str
    coverage_type: Literal[
        "family_all_shows",
        "family_network_shows",
        "family_streaming_shows",
        "franchise_rule",
        "show_wikidata_exact",
        "show_name_contains",
    ] = "family_all_shows"
    coverage_value: str | None = None
    source: Literal["manual", "wikipedia_import", "system"] = "manual"
    priority: int = Field(default=100, ge=0)
    auto_apply: bool = True
    is_active: bool = True
    metadata: dict[str, Any] = Field(default_factory=dict)


class PatchBrandFamilyLinkRuleRequest(BaseModel):
    link_group: Literal["official", "social", "knowledge", "cast_announcements", "other"] | None = None
    link_kind: str | None = None
    label: str | None = None
    url: str | None = None
    coverage_type: (
        Literal[
            "family_all_shows",
            "family_network_shows",
            "family_streaming_shows",
            "franchise_rule",
            "show_wikidata_exact",
            "show_name_contains",
        ]
        | None
    ) = None
    coverage_value: str | None = None
    source: Literal["manual", "wikipedia_import", "system"] | None = None
    priority: int | None = Field(default=None, ge=0)
    auto_apply: bool | None = None
    is_active: bool | None = None
    metadata: dict[str, Any] | None = None


class ApplyBrandFamilyLinksRequest(BaseModel):
    dry_run: bool = True
    rule_ids: list[str] | None = None


class ImportBrandFamilyWikipediaLinksRequest(BaseModel):
    entity_type: Literal["network", "streaming"] | None = None
    entity_key: str | None = None
    apply_matched: bool = False


BrandLogoTargetType = Literal[
    "show",
    "network",
    "streaming",
    "production",
    "franchise",
    "publication",
    "social",
    "other",
]
BrandLogoRole = Literal["wordmark", "icon"]

BrandLogoSyncScope = Literal["all", "page", "show"]
BrandLogoSyncPage = Literal["news", "other", "shows", "networks_streaming", "production_companies"]

_SOCIAL_LINK_KINDS = {"instagram", "tiktok", "twitter", "youtube", "facebook", "threads", "reddit"}
_LOGO_ROLE_ORDER = {"wordmark": 0, "icon": 1, "unknown": 2}
_PRIVATE_HOST_SUFFIXES = (
    ".internal",
    ".local",
    ".lan",
    ".home",
    ".localhost",
    ".intranet",
    ".localdomain",
)
_DIRECT_INVALID_HOSTS = {"localhost", "0.0.0.0"}
_DEFAULT_PAGE_TARGET_TYPES: dict[BrandLogoSyncPage, list[BrandLogoTargetType]] = {
    "news": ["publication", "social"],
    "other": ["other"],
    "shows": ["franchise", "publication", "social"],
    "networks_streaming": ["network", "streaming"],
    "production_companies": ["production"],
}
_ALL_TARGET_TYPES: tuple[BrandLogoTargetType, ...] = (
    "network",
    "streaming",
    "production",
    "franchise",
    "publication",
    "social",
    "other",
)
_DEFAULT_LOGO_OPTIONS_PAGE_SIZE = 24
_DEFAULT_DISCOVER_PAGE_SIZE = 20
_DISCOVERABLE_SOURCE_PROVIDERS = set(FREE_LOGO_SOURCE_PROVIDERS)
_SCHEMA_FALLBACK_WARNED_KEYS: set[str] = set()


class BrandLogoDiscoverCandidateRequest(BaseModel):
    source_url: str
    source_provider: str | None = None
    discovered_from: str | None = None


class BrandLogosOptionDiscoverRequest(BaseModel):
    target_type: BrandLogoTargetType
    target_key: str
    target_label: str | None = None
    logo_role: BrandLogoRole
    source_provider: str | None = None
    query_override: str | None = None
    query_overrides: list[str] | None = None
    offset: int = Field(default=0, ge=0)
    limit: int = Field(default=_DEFAULT_DISCOVER_PAGE_SIZE, ge=1, le=100)
    include_related: bool = True


class BrandLogosSourceQueryRequest(BaseModel):
    target_type: BrandLogoTargetType
    target_key: str
    target_label: str | None = None
    logo_role: BrandLogoRole
    source_provider: str
    query_value: str | None = None
    query_values: list[str] | None = None


class BrandLogosOptionSelectRequest(BaseModel):
    target_type: BrandLogoTargetType
    target_key: str
    target_label: str | None = None
    logo_role: BrandLogoRole
    asset_id: str | None = None
    candidate: BrandLogoDiscoverCandidateRequest | None = None


class BrandLogosSyncRequest(BaseModel):
    scope: BrandLogoSyncScope
    page: BrandLogoSyncPage | None = None
    show_id: str | None = None
    target_types: list[BrandLogoTargetType] = Field(default_factory=list)
    only_missing: bool = True
    force: bool = False
    limit: int = Field(default=150, ge=1, le=500)


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


@lru_cache(maxsize=1)
def _brand_logo_assets_variant_columns() -> tuple[bool, bool]:
    rows = pg.fetch_all(
        """
        select column_name
        from information_schema.columns
        where table_schema = 'admin'
          and table_name = 'brand_logo_assets'
          and column_name in ('hosted_logo_black_url', 'hosted_logo_white_url')
        """
    )
    names = {_normalize_text(row.get("column_name")) for row in rows if isinstance(row, dict)}
    return ("hosted_logo_black_url" in names, "hosted_logo_white_url" in names)


def _brand_logo_assets_variant_select_sql(*, table_alias: str = "") -> str:
    has_black, has_white = _brand_logo_assets_variant_columns()
    prefix = f"{table_alias}." if table_alias else ""
    black_expr = (
        f"{prefix}hosted_logo_black_url as hosted_logo_black_url"
        if has_black
        else "null::text as hosted_logo_black_url"
    )
    white_expr = (
        f"{prefix}hosted_logo_white_url as hosted_logo_white_url"
        if has_white
        else "null::text as hosted_logo_white_url"
    )
    return f"{black_expr},\n              {white_expr},"


@lru_cache(maxsize=1)
def _network_streaming_logo_assets_variant_columns() -> tuple[bool, bool]:
    rows = pg.fetch_all(
        """
        select column_name
        from information_schema.columns
        where table_schema = 'admin'
          and table_name = 'network_streaming_logo_assets'
          and column_name in ('hosted_logo_black_url', 'hosted_logo_white_url')
        """
    )
    names = {_normalize_text(row.get("column_name")) for row in rows if isinstance(row, dict)}
    return ("hosted_logo_black_url" in names, "hosted_logo_white_url" in names)


def _network_streaming_variant_select_sql(*, table_alias: str = "") -> str:
    has_black, has_white = _network_streaming_logo_assets_variant_columns()
    prefix = f"{table_alias}." if table_alias else ""
    black_expr = (
        f"{prefix}hosted_logo_black_url as hosted_logo_black_url"
        if has_black
        else "null::text as hosted_logo_black_url"
    )
    white_expr = (
        f"{prefix}hosted_logo_white_url as hosted_logo_white_url"
        if has_white
        else "null::text as hosted_logo_white_url"
    )
    return f"{black_expr},\n              {white_expr},"


def _null_logo_variant_select_sql() -> str:
    return "null::text as hosted_logo_black_url,\n              null::text as hosted_logo_white_url,"


def _fetch_all_with_logo_variant_fallback(
    *,
    query_builder: Callable[[str], str],
    params: list[Any],
    variant_sql: str,
    cache_clear: Callable[[], None],
    fallback_key: str,
    fallback_message: str,
) -> list[dict[str, Any]]:
    query = query_builder(variant_sql)
    try:
        rows = pg.fetch_all(query, params)
    except Exception as error:  # noqa: BLE001
        if not _is_missing_logo_variant_column_error(error):
            raise
        cache_clear()
        _log_schema_fallback_once(key=fallback_key, message=fallback_message, error=error)
        rows = pg.fetch_all(query_builder(_null_logo_variant_select_sql()), params)
    return [row for row in rows if isinstance(row, dict)]


def _fetch_one_with_logo_variant_fallback(
    *,
    query_builder: Callable[[str], str],
    params: list[Any],
    variant_sql: str,
    cache_clear: Callable[[], None],
    fallback_key: str,
    fallback_message: str,
) -> dict[str, Any] | None:
    query = query_builder(variant_sql)
    try:
        row = pg.fetch_one(query, params)
    except Exception as error:  # noqa: BLE001
        if not _is_missing_logo_variant_column_error(error):
            raise
        cache_clear()
        _log_schema_fallback_once(key=fallback_key, message=fallback_message, error=error)
        row = pg.fetch_one(query_builder(_null_logo_variant_select_sql()), params)
    return row if isinstance(row, dict) else None


def _is_missing_logo_variant_column_error(error: Exception) -> bool:
    message = _normalize_text(error).lower()
    if "does not exist" not in message:
        return False
    return "hosted_logo_black_url" in message or "hosted_logo_white_url" in message


def _log_schema_fallback_once(*, key: str, message: str, error: Exception | None = None) -> None:
    if key in _SCHEMA_FALLBACK_WARNED_KEYS:
        return
    _SCHEMA_FALLBACK_WARNED_KEYS.add(key)
    extra = {"fallback_key": key}
    if error is None:
        logger.warning(message, extra=extra)
        return
    logger.warning("%s: %s", message, error, extra=extra)


def _normalize_hostname_from_url(value: str) -> str:
    text = _normalize_text(value)
    if not text:
        return ""
    if text.lower().startswith("data:image/svg+xml"):
        return ""
    parsed = urlparse(text if "://" in text else f"https://{text}")
    host = _normalize_text(parsed.netloc or parsed.path).lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def _infer_source_provider(
    *,
    source_provider: str | None,
    source_url: str | None,
    discovered_from: str | None,
    source_domain: str | None,
) -> str:
    explicit = _normalize_text(source_provider)
    if explicit:
        return explicit
    host = (
        _normalize_hostname_from_url(_normalize_text(source_url))
        or _normalize_hostname_from_url(_normalize_text(discovered_from))
        or _normalize_hostname_from_url(_normalize_text(source_domain))
    )
    if not host:
        return "stored_existing"
    if "commons.wikimedia.org" in host:
        return "wikimedia_commons"
    if "logos.fandom.com" in host or "wikia.nocookie.net" in host:
        return "logos_fandom"
    if "1000logos.net" in host:
        return "logos1000"
    if "worldvectorlogo.com" in host:
        return "worldvectorlogo"
    if "seeklogo.com" in host:
        return "seeklogo"
    if "logowik.com" in host:
        return "logowik"
    if "logo.wine" in host:
        return "logo_wine"
    if "logosear.ch" in host:
        return "logosearch"
    if "simpleicons.org" in host or "cdn.simpleicons.org" in host:
        return "simple_icons"
    return host


def _is_valid_public_hostname(host: str) -> bool:
    normalized = _normalize_text(host).lower()
    if not normalized:
        return False
    if normalized in _DIRECT_INVALID_HOSTS:
        return False
    if any(normalized.endswith(suffix) for suffix in _PRIVATE_HOST_SUFFIXES):
        return False
    if "." not in normalized:
        return False
    try:
        ip = ipaddress.ip_address(normalized)
    except ValueError:
        return True
    return not (
        ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_multicast or ip.is_reserved or ip.is_unspecified
    )


def _infer_logo_role_from_row(row: dict[str, Any]) -> str:
    metadata = row.get("metadata")
    if isinstance(metadata, dict):
        role = _normalize_text(metadata.get("logo_role")).lower()
        if role in {"wordmark", "icon"}:
            return role
    return "wordmark" if bool(row.get("is_primary")) else "icon"


def _normalize_logo_role(value: Any) -> BrandLogoRole:
    normalized = _normalize_text(value).lower()
    return "icon" if normalized == "icon" else "wordmark"


def _normalize_logo_role_selection_state(row: dict[str, Any]) -> dict[str, Any]:
    metadata = row.get("metadata")
    role = _infer_logo_role_from_row(row)
    selected_from_metadata = False
    if isinstance(metadata, dict):
        selected_from_metadata = bool(metadata.get("selected_for_role"))
    selected = selected_from_metadata or (role == "wordmark" and bool(row.get("is_primary")))
    row["logo_role"] = role
    row["is_selected_for_role"] = selected
    row["option_kind"] = _normalize_text(row.get("option_kind")) or "stored"
    row["origin_target_type"] = _normalize_text(row.get("origin_target_type")) or _normalize_text(
        row.get("target_type")
    )
    row["source_provider"] = _infer_source_provider(
        source_provider=row.get("source_provider"),
        source_url=row.get("source_url"),
        discovered_from=row.get("discovered_from"),
        source_domain=row.get("source_domain"),
    )
    return row


def _apply_logo_selection_fallback(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for row in rows:
        normalized = _normalize_logo_role_selection_state(row)
        key = (
            _normalize_text(normalized.get("target_type")),
            _normalize_text(normalized.get("target_key")).casefold(),
            _normalize_logo_role(normalized.get("logo_role")),
        )
        grouped.setdefault(key, []).append(normalized)

    for grouped_rows in grouped.values():
        if any(bool(row.get("is_selected_for_role")) for row in grouped_rows):
            continue
        role = _normalize_logo_role(grouped_rows[0].get("logo_role"))
        if role == "wordmark":
            primary = next((row for row in grouped_rows if bool(row.get("is_primary"))), None)
            (primary or grouped_rows[0])["is_selected_for_role"] = True
            continue
        sorted_rows = sorted(
            grouped_rows,
            key=lambda row: (
                _normalize_text(row.get("updated_at")),
                _normalize_text(row.get("created_at")),
            ),
            reverse=True,
        )
        sorted_rows[0]["is_selected_for_role"] = True

    return rows


def _detect_logo_role(
    *,
    candidate_url: str,
    content_type: str | None,
    width: int | None,
    height: int | None,
) -> str:
    lowered = _normalize_text(candidate_url).lower()
    ct = _normalize_text(content_type).lower()
    cue_wordmark = bool(re.search(r"(wordmark|logotype|full-logo|horizontal)", lowered))
    cue_icon = bool(re.search(r"(favicon|icon|symbol|mark|avatar)", lowered))

    ratio: float | None = None
    if width and height and height > 0:
        ratio = width / height

    wordmark_score = 0
    icon_score = 0
    if ratio is not None:
        if ratio >= 1.7:
            wordmark_score += 3
        if 0.78 <= ratio <= 1.35:
            icon_score += 3
    if cue_wordmark:
        wordmark_score += 2
    if cue_icon:
        icon_score += 2
    if ".svg" in lowered or ct == "image/svg+xml":
        wordmark_score += 1
    if "favicon" in lowered:
        icon_score += 1
    if wordmark_score == 0 and icon_score == 0:
        return "wordmark"
    return "wordmark" if wordmark_score >= icon_score else "icon"


def _discover_format_priority(source_url: str) -> int:
    lowered = _normalize_text(source_url).lower()
    if ".svg" in lowered:
        return 0
    if ".png" in lowered:
        return 1
    if ".webp" in lowered:
        return 2
    if ".jpg" in lowered or ".jpeg" in lowered:
        return 3
    return 4


def _is_previewable_logo_url(source_url: str) -> bool:
    text = _normalize_text(source_url)
    if not text:
        return False
    if text.lower().startswith("data:image/svg+xml;base64,"):
        return True
    parsed = urlparse(text)
    if parsed.scheme not in {"http", "https"}:
        return False
    lowered = text.lower()
    path = _normalize_text(parsed.path).lower()
    if any(path.endswith(ext) for ext in (".svg", ".png", ".webp", ".jpg", ".jpeg", ".avif", ".gif", ".ico")):
        return True
    if "special:filepath/" in lowered:
        return True
    if _normalize_text(parsed.netloc).lower() == "cdn.simpleicons.org":
        return True
    return False


def _extract_image_dimensions(image_data: bytes) -> tuple[int | None, int | None]:
    try:
        from PIL import Image  # type: ignore
    except Exception:  # noqa: BLE001
        return None, None
    try:
        with Image.open(io.BytesIO(image_data)) as image:
            width = int(image.width) if image.width else None
            height = int(image.height) if image.height else None
            return width, height
    except Exception:  # noqa: BLE001
        return None, None


def _filter_target_rows_by_query(rows: list[dict[str, Any]], q: str) -> list[dict[str, Any]]:
    needle = _normalize_text(q).casefold()
    if not needle:
        return rows
    out: list[dict[str, Any]] = []
    for row in rows:
        haystack = " ".join(
            [
                _normalize_text(row.get("target_key")),
                _normalize_text(row.get("target_label")),
                _normalize_text(row.get("discovered_from")),
            ]
        ).casefold()
        if needle in haystack:
            out.append(row)
    return out


def _seed_logo_targets_from_entity_links(*, show_id: str | None = None) -> list[dict[str, Any]]:
    rows = pg.fetch_all(
        """
        select
          show_id::text as show_id,
          entity_type,
          entity_id::text as entity_id,
          season_number,
          lower(coalesce(link_kind, '')) as link_kind,
          coalesce(label, '') as label,
          coalesce(url, '') as url
        from core.entity_links
        where lower(coalesce(status, '')) = 'approved'
          and coalesce(url, '') <> ''
          and (%s = '' or show_id::text = %s)
        order by updated_at desc nulls last, created_at desc nulls last
        """,
        [_normalize_text(show_id), _normalize_text(show_id)],
    )

    buckets: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        source_url = _normalize_text(row.get("url"))
        if not source_url:
            continue
        host = _normalize_hostname_from_url(source_url)
        if not _is_valid_public_hostname(host):
            continue
        kind = _normalize_text(row.get("link_kind")).lower()
        target_type = "social" if kind in _SOCIAL_LINK_KINDS else "publication"
        key = (target_type, host)
        bucket = buckets.get(key)
        if bucket is None:
            bucket = {
                "target_type": target_type,
                "target_key": host,
                "target_label": host,
                "discovered_from": source_url,
                "discovered_from_urls": [],
                "show_ids": [],
                "source_link_kinds": [],
            }
            buckets[key] = bucket
        discovered_from_urls = bucket.setdefault("discovered_from_urls", [])
        if source_url not in discovered_from_urls and len(discovered_from_urls) < 20:
            discovered_from_urls.append(source_url)
        show_id_value = _normalize_text(row.get("show_id"))
        if show_id_value:
            show_ids = bucket.setdefault("show_ids", [])
            if show_id_value not in show_ids:
                show_ids.append(show_id_value)
        if kind:
            kinds = bucket.setdefault("source_link_kinds", [])
            if kind not in kinds:
                kinds.append(kind)
        if not bucket.get("discovered_from"):
            bucket["discovered_from"] = source_url

    out = list(buckets.values())
    out.sort(
        key=lambda item: (
            _normalize_text(item.get("target_label")).casefold(),
            _normalize_text(item.get("target_key")),
        )
    )
    return out


def _like_term(value: str) -> str:
    return f"%{value}%"


def _sort_logo_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        rows,
        key=lambda row: (
            _normalize_text(row.get("target_label")).casefold(),
            _normalize_text(row.get("target_key")).casefold(),
            _LOGO_ROLE_ORDER.get(_normalize_text(row.get("logo_role")).lower() or "unknown", 2),
            0 if bool(row.get("is_selected_for_role")) else 1,
            0 if bool(row.get("is_primary")) else 1,
            _normalize_text(row.get("updated_at")),
        ),
    )


def _find_related_network_streaming_assets_by_host(
    *,
    target_type: BrandLogoTargetType,
    target_host: str,
    logo_role: BrandLogoRole | None = None,
    limit: int = 60,
) -> list[dict[str, Any]]:
    normalized_host = _normalize_text(target_host).casefold()
    if not normalized_host or "." not in normalized_host:
        return []
    host_pattern = rf"^https?://(www\.)?{re.escape(normalized_host)}([/:?#]|$)"
    variant_select = _network_streaming_variant_select_sql()

    def query_builder(variant: str) -> str:
        return f"""
        select
          id::text as id,
          entity_type,
          entity_key,
          coalesce(display_name, entity_key) as target_label,
          source_url,
          hosted_logo_url,
          {variant}
          is_primary,
          mirror_status,
          failure_reason,
          created_at,
          updated_at
        from admin.network_streaming_logo_assets
        where entity_type in ('network', 'streaming', 'production')
          and mirror_status = 'mirrored'
          and source_url ~* %s
        order by is_primary desc, updated_at desc nulls last, created_at desc nulls last
        limit %s
        """

    try:
        rows = _fetch_all_with_logo_variant_fallback(
            query_builder=query_builder,
            params=[host_pattern, max(1, limit)],
            variant_sql=variant_select,
            cache_clear=_network_streaming_logo_assets_variant_columns.cache_clear,
            fallback_key="related-network-variant-columns-missing",
            fallback_message="Related network/streaming logo pairing using base hosted logos only",
        )
    except Exception as error:  # noqa: BLE001
        if _is_missing_logo_variant_column_error(error):
            _log_schema_fallback_once(
                key="related-network-variant-columns-fallback-failed",
                message="Related network/streaming logo pairing unavailable after fallback",
                error=error,
            )
            return []
        raise
    related_rows: list[dict[str, Any]] = []
    for row in rows:
        role: BrandLogoRole = "wordmark" if bool(row.get("is_primary")) else "icon"
        if logo_role and role != logo_role:
            continue
        related_rows.append(
            {
                "id": f"related:{_normalize_text(row.get('id'))}",
                "target_type": target_type,
                "target_key": normalized_host,
                "target_label": _normalize_text(row.get("target_label")) or normalized_host,
                "source_url": _normalize_text(row.get("source_url")) or None,
                "source_page_url": None,
                "source_domain": _normalize_hostname_from_url(_normalize_text(row.get("source_url"))),
                "hosted_logo_url": _normalize_text(row.get("hosted_logo_url")) or None,
                "hosted_logo_black_url": _normalize_text(row.get("hosted_logo_black_url")) or None,
                "hosted_logo_white_url": _normalize_text(row.get("hosted_logo_white_url")) or None,
                "is_primary": role == "wordmark",
                "mirror_status": _normalize_text(row.get("mirror_status")) or "mirrored",
                "failure_reason": _normalize_text(row.get("failure_reason")) or None,
                "metadata": {
                    "logo_role": role,
                    "selection_origin": "related_pair",
                    "origin_asset_id": _normalize_text(row.get("id")),
                    "origin_target_type": _normalize_text(row.get("entity_type")) or "network",
                    "origin_target_key": _normalize_text(row.get("entity_key")) or None,
                },
                "logo_role": role,
                "source_provider": "related_network_streaming",
                "discovered_from": _normalize_text(row.get("source_url")) or None,
                "created_at": row.get("created_at"),
                "updated_at": row.get("updated_at"),
                "option_kind": "related_linked",
                "origin_target_type": _normalize_text(row.get("entity_type")) or "network",
                "is_selected_for_role": False,
            }
        )
    return related_rows


def _filter_logo_rows(
    *,
    rows: list[dict[str, Any]],
    target_key: str | None,
    logo_role: BrandLogoRole | None,
    source_provider: str | None,
) -> list[dict[str, Any]]:
    normalized_target_key = _normalize_text(target_key).casefold()
    normalized_provider = _normalize_text(source_provider).casefold()
    out: list[dict[str, Any]] = []
    for row in rows:
        row_key = _normalize_text(row.get("target_key")).casefold()
        if normalized_target_key and row_key != normalized_target_key:
            continue
        role = _normalize_logo_role(row.get("logo_role"))
        if logo_role and role != logo_role:
            continue
        provider = _normalize_text(row.get("source_provider")).casefold()
        if normalized_provider and provider != normalized_provider:
            continue
        out.append(row)
    return out


def _list_brand_logos(
    *,
    target_type: BrandLogoTargetType,
    q: str,
    limit: int,
    offset: int,
    include_missing: bool = False,
    target_key: str | None = None,
    logo_role: BrandLogoRole | None = None,
    source_provider: str | None = None,
    include_related: bool = False,
    show_id: str | None = None,
) -> dict[str, Any]:
    needle = q.strip()
    like = _like_term(needle)
    rows: list[dict[str, Any]] = []

    if target_type in {"network", "streaming", "production"}:
        network_variant_select = _network_streaming_variant_select_sql()

        def query_builder(variant: str) -> str:
            return f"""
            select
              id::text as id,
              entity_type as target_type,
              entity_key as target_key,
              coalesce(display_name, entity_key) as target_label,
              source_url,
              null::text as source_page_url,
              null::text as source_domain,
              hosted_logo_url,
              {variant}
              is_primary,
              mirror_status,
              failure_reason,
              null::jsonb as metadata,
              case when is_primary then 'wordmark' else 'icon' end as logo_role,
              source as source_provider,
              source_url as discovered_from,
              'stored'::text as option_kind,
              entity_type as origin_target_type,
              created_at,
              updated_at
            from admin.network_streaming_logo_assets
            where entity_type = %s
              and (
                %s = ''
                or entity_key ilike %s
                or coalesce(display_name, '') ilike %s
                or coalesce(source_url, '') ilike %s
              )
            order by is_primary desc, updated_at desc nulls last, created_at desc nulls last
            """

        rows = _fetch_all_with_logo_variant_fallback(
            query_builder=query_builder,
            params=[target_type, needle, like, like, like],
            variant_sql=network_variant_select,
            cache_clear=_network_streaming_logo_assets_variant_columns.cache_clear,
            fallback_key="list-network-variant-columns-missing",
            fallback_message="Network/streaming logo list using base hosted logos only",
        )
    elif target_type in {"franchise", "publication", "social", "other"}:
        brand_variant_select = _brand_logo_assets_variant_select_sql()

        def query_builder(variant: str) -> str:
            return f"""
            select
              id::text as id,
              target_type,
              target_key,
              target_label,
              source_url,
              source_page_url,
              source_domain,
              hosted_logo_url,
              {variant}
              is_primary,
              mirror_status,
              failure_reason,
              metadata,
              coalesce(metadata->>'logo_role', case when is_primary then 'wordmark' else 'icon' end) as logo_role,
              coalesce(metadata->>'source_provider', source_domain, 'manual') as source_provider,
              coalesce(metadata->>'discovered_from', source_page_url, source_url) as discovered_from,
              'stored'::text as option_kind,
              target_type as origin_target_type,
              created_at,
              updated_at
            from admin.brand_logo_assets
            where target_type = %s
              and (
                %s = ''
                or target_key ilike %s
                or target_label ilike %s
                or coalesce(source_domain, '') ilike %s
                or coalesce(source_url, '') ilike %s
              )
            order by is_primary desc, updated_at desc nulls last, created_at desc nulls last
            """

        rows = _fetch_all_with_logo_variant_fallback(
            query_builder=query_builder,
            params=[target_type, needle, like, like, like, like],
            variant_sql=brand_variant_select,
            cache_clear=_brand_logo_assets_variant_columns.cache_clear,
            fallback_key="list-brand-variant-columns-missing",
            fallback_message="Brand logo list using base hosted logos only",
        )

        if include_missing:
            target_payload = _list_logo_targets(
                target_type=target_type,
                q=q,
                limit=max(2000, limit + offset + 500),
                show_id=show_id,
            )
            target_rows = target_payload.get("rows", []) if isinstance(target_payload, dict) else []
            existing_by_target_role: set[tuple[str, str, str]] = set()
            for row in rows:
                existing_by_target_role.add(
                    (
                        _normalize_text(row.get("target_type")),
                        _normalize_text(row.get("target_key")).casefold(),
                        _normalize_logo_role(row.get("logo_role")),
                    )
                )
            for target in target_rows:
                seeded_target_key = _normalize_text(target.get("target_key")).casefold()
                seeded_target_label = _normalize_text(target.get("target_label")) or seeded_target_key
                discovered_from = _normalize_text(target.get("discovered_from")) or None
                for role in ("wordmark", "icon"):
                    key = (target_type, seeded_target_key, role)
                    if key in existing_by_target_role:
                        continue
                    rows.append(
                        {
                            "id": f"missing:{target_type}:{seeded_target_key}:{role}",
                            "target_type": target_type,
                            "target_key": seeded_target_key,
                            "target_label": seeded_target_label,
                            "source_url": None,
                            "source_page_url": None,
                            "source_domain": None,
                            "hosted_logo_url": None,
                            "hosted_logo_black_url": None,
                            "hosted_logo_white_url": None,
                            "is_primary": role == "wordmark",
                            "mirror_status": "missing",
                            "failure_reason": None,
                            "metadata": {
                                "logo_role": role,
                                "discovered_from": discovered_from,
                            },
                            "logo_role": role,
                            "source_provider": None,
                            "discovered_from": discovered_from,
                            "created_at": None,
                            "updated_at": None,
                            "option_kind": "stored",
                            "origin_target_type": target_type,
                            "is_selected_for_role": False,
                        }
                    )

        if include_related and target_type in {"publication", "social"}:
            target_hosts: set[str] = set()
            if target_key:
                normalized_host = _normalize_text(target_key).casefold()
                if "." in normalized_host:
                    target_hosts.add(normalized_host)
            for row in rows:
                host = _normalize_text(row.get("target_key")).casefold()
                if host and "." in host:
                    target_hosts.add(host)

            dedupe_related: set[tuple[str, str, str]] = set()
            for row in rows:
                dedupe_related.add(
                    (
                        _normalize_text(row.get("target_key")).casefold(),
                        _normalize_logo_role(row.get("logo_role")),
                        _normalize_text(row.get("source_url")).casefold(),
                    )
                )

            for host in sorted(target_hosts):
                try:
                    related_rows = _find_related_network_streaming_assets_by_host(
                        target_type=target_type,
                        target_host=host,
                        logo_role=logo_role,
                        limit=120,
                    )
                except Exception as error:  # noqa: BLE001
                    if _is_missing_logo_variant_column_error(error):
                        _log_schema_fallback_once(
                            key="publication-social-related-pairing-disabled",
                            message=(
                                "Publication/social related logo pairing disabled due missing "
                                "network-streaming monochrome columns"
                            ),
                            error=error,
                        )
                        related_rows = []
                    else:
                        raise
                for related in related_rows:
                    dedupe_key = (
                        _normalize_text(related.get("target_key")).casefold(),
                        _normalize_logo_role(related.get("logo_role")),
                        _normalize_text(related.get("source_url")).casefold(),
                    )
                    if dedupe_key in dedupe_related:
                        continue
                    dedupe_related.add(dedupe_key)
                    rows.append(related)
    else:
        rows = pg.fetch_all(
            """
            select distinct on (ma.id, ml.entity_id)
              ma.id::text as id,
              'show'::text as target_type,
              ml.entity_id::text as target_key,
              coalesce(s.name, ml.entity_id::text) as target_label,
              ma.url as source_url,
              coalesce(ml.context->>'source_page_url', ma.metadata->>'source_page_url') as source_page_url,
              coalesce(ml.context->>'source_domain', ma.metadata->>'source_domain') as source_domain,
              ma.hosted_url as hosted_logo_url,
              coalesce(ma.metadata->>'logo_black_url', ma.metadata->>'hosted_logo_black_url') as hosted_logo_black_url,
              coalesce(ma.metadata->>'logo_white_url', ma.metadata->>'hosted_logo_white_url') as hosted_logo_white_url,
              case when lower(ml.kind) = 'logo' then true else false end as is_primary,
              'mirrored'::text as mirror_status,
              null::text as failure_reason,
              ma.metadata,
              case when lower(ml.kind) = 'logo' then 'wordmark' else 'icon' end as logo_role,
              coalesce(ml.context->>'source_provider', ma.metadata->>'source_provider') as source_provider,
              coalesce(ml.context->>'discovered_from', ma.metadata->>'discovered_from', ma.url) as discovered_from,
              'stored'::text as option_kind,
              'show'::text as origin_target_type,
              ma.created_at,
              ma.updated_at
            from core.media_links ml
            join core.media_assets ma on ma.id = ml.media_asset_id
            left join core.shows s on s.id = ml.entity_id
            where ml.entity_type = 'show'
              and lower(coalesce(ml.kind, '')) = 'logo'
              and (
                coalesce(ml.context->>'logo_target_type', ma.metadata->>'logo_target_type', '') = 'show'
                or upper(coalesce(ml.context->>'source_logo', ma.metadata->>'source_logo', '')) = 'SHOW'
              )
              and (
                %s = ''
                or coalesce(s.name, '') ilike %s
                or ml.entity_id::text ilike %s
                or coalesce(ma.url, '') ilike %s
              )
            order by ma.id, ml.entity_id, ma.updated_at desc nulls last, ma.created_at desc nulls last
            """,
            [needle, like, like, like],
        )

    rows = _apply_logo_selection_fallback(rows)
    filtered_rows = _filter_logo_rows(
        rows=rows,
        target_key=target_key,
        logo_role=logo_role,
        source_provider=source_provider,
    )
    sorted_rows = _sort_logo_rows(filtered_rows)
    return {"rows": sorted_rows[offset : offset + limit], "count": len(sorted_rows)}


def _list_logo_targets(
    *,
    target_type: BrandLogoTargetType,
    q: str,
    limit: int,
    show_id: str | None = None,
) -> dict[str, Any]:
    needle = q.strip()
    like = _like_term(needle)

    if target_type == "show":
        rows = pg.fetch_all(
            """
            select
              'show'::text as target_type,
              s.id::text as target_key,
              s.name as target_label
            from core.shows s
            where %s = '' or s.name ilike %s
            order by s.name asc
            limit %s
            """,
            [needle, like, limit],
        )
        return {"rows": rows, "count": len(rows)}

    if target_type == "network":
        rows = pg.fetch_all(
            """
            select
              'network'::text as target_type,
              n.id::text as target_key,
              n.name as target_label
            from core.networks n
            where %s = '' or n.name ilike %s
            order by n.name asc
            limit %s
            """,
            [needle, like, limit],
        )
        return {"rows": rows, "count": len(rows)}

    if target_type == "streaming":
        rows = pg.fetch_all(
            """
            select
              'streaming'::text as target_type,
              wp.provider_id::text as target_key,
              wp.provider_name as target_label
            from core.watch_providers wp
            where %s = '' or wp.provider_name ilike %s
            order by wp.provider_name asc
            limit %s
            """,
            [needle, like, limit],
        )
        return {"rows": rows, "count": len(rows)}

    if target_type == "production":
        rows = pg.fetch_all(
            """
            select
              'production'::text as target_type,
              pc.id::text as target_key,
              pc.name as target_label
            from core.production_companies pc
            where %s = '' or pc.name ilike %s
            order by pc.name asc
            limit %s
            """,
            [needle, like, limit],
        )
        return {"rows": rows, "count": len(rows)}

    if target_type == "franchise":
        payload = brands_franchises.list_franchise_rules()
        rules = payload.get("rules", []) if isinstance(payload, dict) else []
        rows = []
        for rule in rules:
            if not isinstance(rule, dict):
                continue
            key = str(rule.get("key") or "").strip()
            label = str(rule.get("name") or key).strip()
            if not key or not label:
                continue
            haystack = f"{key} {label}".lower()
            if needle and needle.lower() not in haystack:
                continue
            rows.append(
                {
                    "target_type": "franchise",
                    "target_key": key,
                    "target_label": label,
                }
            )
            if len(rows) >= limit:
                break
        return {"rows": rows, "count": len(rows)}

    if target_type in {"publication", "social"}:
        seeded_rows = _seed_logo_targets_from_entity_links(show_id=show_id)
        filtered = [
            row
            for row in _filter_target_rows_by_query(seeded_rows, q)
            if _normalize_text(row.get("target_type")) == target_type
        ]
        return {"rows": filtered[:limit], "count": len(filtered)}

    rows = pg.fetch_all(
        """
        select distinct on (target_key)
          target_type,
          target_key,
          target_label,
          coalesce(metadata->>'discovered_from', source_page_url, source_url) as discovered_from
        from admin.brand_logo_assets
        where target_type = %s
          and (
            %s = ''
            or target_key ilike %s
            or target_label ilike %s
          )
        order by target_key, is_primary desc, updated_at desc nulls last
        limit %s
        """,
        [target_type, needle, like, like, limit],
    )
    return {"rows": rows, "count": len(rows)}


def _resolve_sync_target_types(payload: BrandLogosSyncRequest) -> list[BrandLogoTargetType]:
    if payload.target_types:
        ordered: list[BrandLogoTargetType] = []
        seen: set[str] = set()
        for target_type in payload.target_types:
            key = _normalize_text(target_type)
            if key in seen:
                continue
            seen.add(key)
            ordered.append(target_type)
        return ordered
    if payload.scope == "page":
        if not payload.page:
            raise ValueError("page is required when scope=page")
        return list(_DEFAULT_PAGE_TARGET_TYPES[payload.page])
    if payload.scope == "show":
        return ["publication", "social"]
    return list(_ALL_TARGET_TYPES)


def _load_sync_targets(
    *,
    target_types: list[BrandLogoTargetType],
    scope: BrandLogoSyncScope,
    show_id: str | None,
    limit: int,
) -> list[dict[str, Any]]:
    resolved_show_id = _normalize_text(show_id) if scope == "show" else ""
    rows: list[dict[str, Any]] = []
    for target_type in target_types:
        payload = _list_logo_targets(
            target_type=target_type,
            q="",
            limit=max(limit * 3, 400),
            show_id=resolved_show_id or None,
        )
        current_rows = payload.get("rows", []) if isinstance(payload, dict) else []
        if not isinstance(current_rows, list):
            continue
        for row in current_rows:
            if not isinstance(row, dict):
                continue
            normalized = {
                "target_type": target_type,
                "target_key": _normalize_text(row.get("target_key")).casefold(),
                "target_label": _normalize_text(row.get("target_label")) or _normalize_text(row.get("target_key")),
                "discovered_from": _normalize_text(row.get("discovered_from")) or None,
                "discovered_from_urls": (
                    row.get("discovered_from_urls") if isinstance(row.get("discovered_from_urls"), list) else []
                ),
            }
            if not normalized["target_key"]:
                continue
            rows.append(normalized)
    deduped: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        key = (_normalize_text(row.get("target_type")), _normalize_text(row.get("target_key")))
        if key not in deduped:
            deduped[key] = row
            continue
        existing = deduped[key]
        existing_urls = (
            existing.get("discovered_from_urls") if isinstance(existing.get("discovered_from_urls"), list) else []
        )
        incoming_urls = row.get("discovered_from_urls") if isinstance(row.get("discovered_from_urls"), list) else []
        for url in incoming_urls:
            normalized_url = _normalize_text(url)
            if normalized_url and normalized_url not in existing_urls and len(existing_urls) < 20:
                existing_urls.append(normalized_url)
        if not existing.get("discovered_from") and row.get("discovered_from"):
            existing["discovered_from"] = row.get("discovered_from")
    ordered = sorted(
        deduped.values(),
        key=lambda item: (
            _normalize_text(item.get("target_label")).casefold(),
            _normalize_text(item.get("target_key")),
        ),
    )
    return ordered[:limit]


def _list_logo_options(
    *,
    target_type: BrandLogoTargetType,
    target_key: str,
    logo_role: BrandLogoRole,
    include_related: bool = True,
    source_provider: str | None = None,
    offset: int = 0,
    limit: int = _DEFAULT_LOGO_OPTIONS_PAGE_SIZE,
) -> dict[str, Any]:
    normalized_key = _normalize_text(target_key).casefold()
    payload = _list_brand_logos(
        target_type=target_type,
        q="",
        limit=max(2000, offset + limit + 200),
        offset=0,
        include_missing=False,
        target_key=normalized_key,
        logo_role=logo_role,
        source_provider=source_provider,
        include_related=include_related,
    )
    rows = payload.get("rows", []) if isinstance(payload, dict) else []
    if not isinstance(rows, list):
        rows = []
    return {
        "rows": rows[offset : offset + limit],
        "count": len(rows),
    }


def _source_provider_catalog(
    *,
    target_type: BrandLogoTargetType,
    include_related: bool,
) -> list[str]:
    providers = list(FREE_LOGO_SOURCE_PROVIDERS)
    if include_related and target_type in {"publication", "social"}:
        return ["related_network_streaming", *providers]
    return providers


def _is_missing_logo_source_query_table_error(error: Exception) -> bool:
    message = _normalize_text(error).casefold()
    return "brand_logo_source_queries" in message and "does not exist" in message


def _is_missing_logo_source_query_values_column_error(error: Exception) -> bool:
    message = _normalize_text(error).casefold()
    return "query_values" in message and "does not exist" in message


def _normalize_source_query_values(values: Any) -> list[str]:
    raw_values = values if isinstance(values, list) else []
    normalized_values: list[str] = []
    seen: set[str] = set()
    for value in raw_values:
        normalized = _normalize_text(value)
        if not normalized:
            continue
        normalized_key = normalized.casefold()
        if normalized_key in seen:
            continue
        seen.add(normalized_key)
        normalized_values.append(normalized)
    return normalized_values


def _coerce_logo_source_query_values(row: dict[str, Any]) -> list[str]:
    query_values = row.get("query_values")
    if isinstance(query_values, str):
        try:
            parsed = json.loads(query_values)
        except Exception:  # noqa: BLE001
            parsed = None
        if isinstance(parsed, list):
            normalized = _normalize_source_query_values(parsed)
            if normalized:
                return normalized
    elif isinstance(query_values, list):
        normalized = _normalize_source_query_values(query_values)
        if normalized:
            return normalized
    query_value = _normalize_text(row.get("query_value"))
    return [query_value] if query_value else []


def _load_logo_source_query_overrides(
    *,
    target_type: BrandLogoTargetType,
    target_key: str,
    logo_role: BrandLogoRole,
) -> dict[str, list[str]]:
    normalized_key = _normalize_text(target_key).casefold()
    try:
        rows = pg.fetch_all(
            """
            select source_provider, query_value, query_values
            from admin.brand_logo_source_queries
            where target_type = %s
              and target_key = %s
              and logo_role = %s
            """,
            [target_type, normalized_key, logo_role],
        )
    except Exception as error:  # noqa: BLE001
        if _is_missing_logo_source_query_values_column_error(error):
            rows = pg.fetch_all(
                """
                select source_provider, query_value
                from admin.brand_logo_source_queries
                where target_type = %s
                  and target_key = %s
                  and logo_role = %s
                """,
                [target_type, normalized_key, logo_role],
            )
        elif _is_missing_logo_source_query_table_error(error):
            return {}
        else:
            raise
    overrides: dict[str, list[str]] = {}
    for row in rows:
        provider = _normalize_text(row.get("source_provider")).casefold()
        query_values = _coerce_logo_source_query_values(row)
        if provider and query_values:
            overrides[provider] = query_values
    return overrides


def _upsert_logo_source_query_override(
    *,
    target_type: BrandLogoTargetType,
    target_key: str,
    logo_role: BrandLogoRole,
    source_provider: str,
    query_values: list[str],
) -> None:
    normalized_values = _normalize_source_query_values(query_values)
    if not normalized_values:
        raise ValueError("At least one query value is required")
    serialized_values = json.dumps(normalized_values)
    try:
        pg.fetch_one(
            """
            insert into admin.brand_logo_source_queries (
              target_type,
              target_key,
              logo_role,
              source_provider,
              query_value,
              query_values
            )
            values (%s, %s, %s, %s, %s, %s::jsonb)
            on conflict (target_type, target_key, logo_role, source_provider)
            do update set
              query_value = excluded.query_value,
              query_values = excluded.query_values,
              updated_at = timezone('utc', now())
            returning target_type
            """,
            [
                target_type,
                _normalize_text(target_key).casefold(),
                logo_role,
                _normalize_text(source_provider).casefold(),
                normalized_values[0],
                serialized_values,
            ],
        )
    except Exception as error:  # noqa: BLE001
        if _is_missing_logo_source_query_values_column_error(error) and len(normalized_values) == 1:
            pg.fetch_one(
                """
                insert into admin.brand_logo_source_queries (
                  target_type,
                  target_key,
                  logo_role,
                  source_provider,
                  query_value
                )
                values (%s, %s, %s, %s, %s)
                on conflict (target_type, target_key, logo_role, source_provider)
                do update set
                  query_value = excluded.query_value,
                  updated_at = timezone('utc', now())
                returning target_type
                """,
                [
                    target_type,
                    _normalize_text(target_key).casefold(),
                    logo_role,
                    _normalize_text(source_provider).casefold(),
                    normalized_values[0],
                ],
            )
            return
        if _is_missing_logo_source_query_table_error(error):
            raise
        raise


def _delete_logo_source_query_override(
    *,
    target_type: BrandLogoTargetType,
    target_key: str,
    logo_role: BrandLogoRole,
    source_provider: str,
) -> None:
    pg.fetch_one(
        """
        delete from admin.brand_logo_source_queries
        where target_type = %s
          and target_key = %s
          and logo_role = %s
          and source_provider = %s
        returning target_type
        """,
        [target_type, _normalize_text(target_key).casefold(), logo_role, _normalize_text(source_provider).casefold()],
    )


def _build_logo_source_summary(
    *,
    provider: str,
    total_count: int,
    target_type: BrandLogoTargetType,
    target_key: str,
    target_label: str,
    logo_role: BrandLogoRole,
    query_override: str | list[str] | None,
) -> dict[str, Any]:
    profile = build_source_query_profile(
        source_provider=provider,
        target_label=target_label,
        target_key=target_key,
        query_override=query_override,
    )
    return {
        "source_provider": provider,
        "total_count": total_count,
        "has_more": total_count > _DEFAULT_LOGO_OPTIONS_PAGE_SIZE or provider in _DISCOVERABLE_SOURCE_PROVIDERS,
        "editable": bool(profile.get("editable")),
        "refreshable": bool(profile.get("refreshable")),
        "query_kind": _normalize_text(profile.get("query_kind")) or "search_term",
        "default_query_value": _normalize_text(profile.get("default_query_value")) or None,
        "effective_query_value": _normalize_text(profile.get("effective_query_value")) or None,
        "query_values": _normalize_source_query_values(profile.get("query_values")),
        "query_links": list(profile.get("query_links") or []),
        "logo_role": logo_role,
    }


def _list_logo_option_sources(
    *,
    target_type: BrandLogoTargetType,
    target_key: str,
    target_label: str | None = None,
    logo_role: BrandLogoRole,
    include_related: bool = True,
) -> dict[str, Any]:
    try:
        payload = _list_logo_options(
            target_type=target_type,
            target_key=target_key,
            logo_role=logo_role,
            include_related=include_related,
            offset=0,
            limit=5000,
        )
    except Exception as error:  # noqa: BLE001
        if (
            include_related
            and target_type in {"publication", "social"}
            and _is_missing_logo_variant_column_error(error)
        ):
            _log_schema_fallback_once(
                key="sources-related-fallback",
                message="Falling back to base source counts without related pairing",
                error=error,
            )
            payload = _list_logo_options(
                target_type=target_type,
                target_key=target_key,
                logo_role=logo_role,
                include_related=False,
                offset=0,
                limit=5000,
            )
        else:
            raise
    rows = payload.get("rows", []) if isinstance(payload, dict) else []
    if not isinstance(rows, list):
        rows = []
    counts: dict[str, int] = {}
    for row in rows:
        provider = _normalize_text(row.get("source_provider")) or "unknown"
        counts[provider] = counts.get(provider, 0) + 1
    overrides = _load_logo_source_query_overrides(
        target_type=target_type,
        target_key=target_key,
        logo_role=logo_role,
    )
    catalog = _source_provider_catalog(target_type=target_type, include_related=include_related)
    ordered_providers = list(catalog)
    for provider in sorted(counts.keys(), key=lambda item: item.casefold()):
        if provider not in ordered_providers:
            ordered_providers.append(provider)
    resolved_label = _normalize_text(target_label) or _normalize_text(target_key)
    return {
        "target_type": target_type,
        "target_key": _normalize_text(target_key).casefold(),
        "logo_role": logo_role,
        "sources": [
            _build_logo_source_summary(
                provider=provider,
                total_count=counts.get(provider, 0),
                target_type=target_type,
                target_key=target_key,
                target_label=resolved_label,
                logo_role=logo_role,
                query_override=overrides.get(provider),
            )
            for provider in ordered_providers
        ],
    }


def _discover_logo_candidates_by_source(payload: BrandLogosOptionDiscoverRequest) -> dict[str, Any]:
    target_key = _normalize_text(payload.target_key).casefold()
    target_label = _normalize_text(payload.target_label) or target_key

    try:
        existing_payload = _list_brand_logos(
            target_type=payload.target_type,
            q="",
            limit=2000,
            offset=0,
            include_missing=True,
            target_key=target_key,
            logo_role=None,
            source_provider=None,
            include_related=payload.include_related,
        )
    except Exception as error:  # noqa: BLE001
        if (
            payload.include_related
            and payload.target_type in {"publication", "social"}
            and _is_missing_logo_variant_column_error(error)
        ):
            _log_schema_fallback_once(
                key="discover-related-fallback",
                message="Falling back to base discovery context without related pairing",
                error=error,
            )
            existing_payload = _list_brand_logos(
                target_type=payload.target_type,
                q="",
                limit=2000,
                offset=0,
                include_missing=True,
                target_key=target_key,
                logo_role=None,
                source_provider=None,
                include_related=False,
            )
        else:
            raise
    existing_rows = existing_payload.get("rows", []) if isinstance(existing_payload, dict) else []
    if not isinstance(existing_rows, list):
        existing_rows = []

    discovered_urls: list[str] = []
    existing_source_urls: set[str] = set()
    for row in existing_rows:
        for value in (
            row.get("discovered_from"),
            row.get("source_page_url"),
            row.get("source_url"),
        ):
            text = _normalize_text(value)
            if text and text not in discovered_urls:
                discovered_urls.append(text)
        source_url = _normalize_text(row.get("source_url"))
        if source_url:
            existing_source_urls.add(source_url.casefold())

    aliases = [target_label]
    if "." in target_key:
        aliases.append(target_key.split(".", 1)[0])
    limit_per_source = max(10, payload.offset + payload.limit + 10)
    candidates = collect_free_logo_candidates(
        target_label=target_label,
        target_key=target_key,
        discovered_from_urls=discovered_urls,
        aliases=aliases,
        source_provider=payload.source_provider,
        query_override=payload.query_overrides if payload.query_overrides else payload.query_override,
        limit_per_source=limit_per_source,
        timeout_seconds=15.0,
    )

    normalized_provider_filter = _normalize_text(payload.source_provider).casefold()
    out: list[dict[str, Any]] = []
    seen_sources: set[str] = set()
    for candidate in candidates:
        source_url = _normalize_text(candidate.url)
        if not source_url:
            continue
        if not _is_previewable_logo_url(source_url):
            continue
        source_url_key = source_url.casefold()
        if source_url_key in seen_sources or source_url_key in existing_source_urls:
            continue
        seen_sources.add(source_url_key)

        provider = _normalize_text(candidate.source_provider) or "unknown"
        if normalized_provider_filter and provider.casefold() != normalized_provider_filter:
            continue
        detected_role = _detect_logo_role(candidate_url=source_url, content_type=None, width=None, height=None)
        out.append(
            {
                "id": f"candidate:{hashlib.sha256(f'{provider}:{source_url}'.encode()).hexdigest()[:24]}",
                "target_type": payload.target_type,
                "target_key": target_key,
                "target_label": target_label,
                "source_url": source_url,
                "source_provider": provider,
                "discovered_from": _normalize_text(candidate.discovered_from) or None,
                "logo_role": payload.logo_role,
                "detected_logo_role": _normalize_logo_role(detected_role),
                "option_kind": "candidate",
                "origin_target_type": payload.target_type,
                "width": None,
                "height": None,
                "aspect_ratio": None,
            }
        )

    total = len(out)
    out.sort(
        key=lambda row: (
            _discover_format_priority(_normalize_text(row.get("source_url"))),
            _normalize_text(row.get("source_url")).casefold(),
        )
    )
    next_offset = payload.offset + payload.limit
    return {
        "target_type": payload.target_type,
        "target_key": target_key,
        "target_label": target_label,
        "logo_role": payload.logo_role,
        "candidates": out[payload.offset : payload.offset + payload.limit],
        "total_count": total,
        "next_offset": next_offset if next_offset < total else total,
        "has_more": next_offset < total,
    }


def _save_logo_source_query(payload: BrandLogosSourceQueryRequest) -> dict[str, Any]:
    provider = _normalize_text(payload.source_provider).casefold()
    allowed = set(_source_provider_catalog(target_type=payload.target_type, include_related=True))
    if provider not in allowed or provider == "related_network_streaming":
        raise ValueError("Source query editing is not supported for this provider")

    target_key = _normalize_text(payload.target_key).casefold()
    target_label = _normalize_text(payload.target_label) or target_key
    requested_query_values = payload.query_values if payload.query_values is not None else [payload.query_value]
    normalized_requested_values = _normalize_source_query_values(requested_query_values)
    if normalized_requested_values:
        profile = build_source_query_profile(
            source_provider=provider,
            target_label=target_label,
            target_key=target_key,
            query_override=normalized_requested_values,
        )
        effective_query_values = _normalize_source_query_values(profile.get("query_values"))
        if not effective_query_values:
            raise ValueError("Query value is invalid for this provider")
        _upsert_logo_source_query_override(
            target_type=payload.target_type,
            target_key=target_key,
            logo_role=payload.logo_role,
            source_provider=provider,
            query_values=effective_query_values,
        )
    else:
        _delete_logo_source_query_override(
            target_type=payload.target_type,
            target_key=target_key,
            logo_role=payload.logo_role,
            source_provider=provider,
        )

    overrides = _load_logo_source_query_overrides(
        target_type=payload.target_type,
        target_key=target_key,
        logo_role=payload.logo_role,
    )
    return {
        "target_type": payload.target_type,
        "target_key": target_key,
        "logo_role": payload.logo_role,
        "source": _build_logo_source_summary(
            provider=provider,
            total_count=0,
            target_type=payload.target_type,
            target_key=target_key,
            target_label=target_label,
            logo_role=payload.logo_role,
            query_override=overrides.get(provider),
        ),
    }


def _fetch_logo_option_row(
    *,
    target_type: BrandLogoTargetType,
    target_key: str,
    asset_id: str,
) -> dict[str, Any] | None:
    normalized_target_key = _normalize_text(target_key).casefold()
    if target_type in {"network", "streaming", "production"}:
        network_variant_select = _network_streaming_variant_select_sql()

        def query_builder(variant: str) -> str:
            return f"""
            select
              id::text as id,
              entity_type as target_type,
              entity_key as target_key,
              coalesce(display_name, entity_key) as target_label,
              source_url,
              null::text as source_page_url,
              null::text as source_domain,
              hosted_logo_url,
              {variant}
              is_primary,
              mirror_status,
              failure_reason,
              null::jsonb as metadata,
              case when is_primary then 'wordmark' else 'icon' end as logo_role,
              source as source_provider,
              source_url as discovered_from,
              'stored'::text as option_kind,
              entity_type as origin_target_type,
              created_at,
              updated_at
            from admin.network_streaming_logo_assets
            where id::text = %s
              and entity_type = %s
              and entity_key = %s
            limit 1
            """

        row = _fetch_one_with_logo_variant_fallback(
            query_builder=query_builder,
            params=[asset_id, target_type, normalized_target_key],
            variant_sql=network_variant_select,
            cache_clear=_network_streaming_logo_assets_variant_columns.cache_clear,
            fallback_key="fetch-network-option-variant-columns-missing",
            fallback_message="Fetching network logo option using base hosted logos only",
        )
    else:
        brand_variant_select = _brand_logo_assets_variant_select_sql()

        def query_builder(variant: str) -> str:
            return f"""
            select
              id::text as id,
              target_type,
              target_key,
              target_label,
              source_url,
              source_page_url,
              source_domain,
              hosted_logo_url,
              {variant}
              is_primary,
              mirror_status,
              failure_reason,
              metadata,
              coalesce(metadata->>'logo_role', case when is_primary then 'wordmark' else 'icon' end) as logo_role,
              coalesce(metadata->>'source_provider', source_domain, 'manual') as source_provider,
              coalesce(metadata->>'discovered_from', source_page_url, source_url) as discovered_from,
              'stored'::text as option_kind,
              target_type as origin_target_type,
              created_at,
              updated_at
            from admin.brand_logo_assets
            where id::text = %s
              and target_type = %s
              and target_key = %s
            limit 1
            """

        row = _fetch_one_with_logo_variant_fallback(
            query_builder=query_builder,
            params=[asset_id, target_type, normalized_target_key],
            variant_sql=brand_variant_select,
            cache_clear=_brand_logo_assets_variant_columns.cache_clear,
            fallback_key="fetch-brand-option-variant-columns-missing",
            fallback_message="Fetching brand logo option using base hosted logos only",
        )
    if row is None:
        return None
    return _normalize_logo_role_selection_state(row)


def _import_logo_option_candidate(
    *,
    db: SupabaseAdminClient,
    target_type: BrandLogoTargetType,
    target_key: str,
    target_label: str,
    logo_role: BrandLogoRole,
    source_url: str,
    source_provider: str | None,
    discovered_from: str | None,
    selection_origin: str,
) -> dict[str, Any]:
    from api.routers.admin_scrape import _import_non_show_logo_target  # noqa: PLC0415

    if _normalize_text(source_url).lower().startswith("data:image/svg+xml;base64,"):
        encoded = source_url.split(",", 1)[1] if "," in source_url else ""
        try:
            image_data = base64.b64decode(encoded, validate=True)
        except Exception as error:  # noqa: BLE001
            raise RuntimeError("Invalid inline SVG logo data") from error
        content_type = "image/svg+xml"
    else:
        image_data, content_type = download_image(
            source_url,
            source="manual_logo_import",
            referer=discovered_from,
        )
    metadata = {
        "logo_role": logo_role,
        "source_provider": _normalize_text(source_provider) or "unknown",
        "discovered_from": _normalize_text(discovered_from) or None,
        "discovery_timestamp": datetime.now(tz=UTC).isoformat(),
        "selection_origin": selection_origin,
        "selected_for_role": True,
        "selection_updated_at": datetime.now(tz=UTC).isoformat(),
    }
    status, _, created_asset_id = _import_non_show_logo_target(
        db=db,
        target_type=target_type,
        target_key=target_key,
        target_label=target_label,
        set_primary=(logo_role == "wordmark"),
        image_data=image_data,
        sha256=hashlib.sha256(image_data).hexdigest(),
        content_type=_normalize_text(content_type) or "application/octet-stream",
        source_url=source_url,
        source_page_url=_normalize_text(discovered_from),
        source_domain=_normalize_hostname_from_url(source_url),
        metadata=metadata,
    )
    if status != "imported":
        raise RuntimeError("Failed to import selected logo option")

    normalized_target_key = _normalize_text(target_key).casefold()
    if created_asset_id:
        row = _fetch_logo_option_row(
            target_type=target_type,
            target_key=normalized_target_key,
            asset_id=created_asset_id,
        )
        if row:
            return row
    if target_type in {"network", "streaming", "production"}:
        row = pg.fetch_one(
            """
            select id::text as id
            from admin.network_streaming_logo_assets
            where entity_type = %s
              and source_url = %s
            order by updated_at desc nulls last, created_at desc nulls last
            limit 1
            """,
            [target_type, source_url],
        )
    else:
        row = pg.fetch_one(
            """
            select id::text as id
            from admin.brand_logo_assets
            where target_type = %s
              and target_key = %s
              and source_url = %s
            order by updated_at desc nulls last, created_at desc nulls last
            limit 1
            """,
            [target_type, normalized_target_key, source_url],
        )
    asset_id = _normalize_text((row or {}).get("id"))
    if not asset_id:
        raise RuntimeError("Imported logo asset could not be resolved")
    resolved_row = _fetch_logo_option_row(
        target_type=target_type,
        target_key=normalized_target_key,
        asset_id=asset_id,
    )
    if not resolved_row:
        raise RuntimeError("Imported logo asset could not be loaded")
    return resolved_row


def _set_network_role_selection(
    *,
    db: SupabaseAdminClient,
    target_type: BrandLogoTargetType,
    target_key: str,
    asset_id: str,
    logo_role: BrandLogoRole,
) -> None:
    normalized_key = _normalize_text(target_key).casefold()
    now_iso = datetime.now(tz=UTC).isoformat()
    if logo_role == "wordmark":
        reset = (
            db.schema("admin")
            .table("network_streaming_logo_assets")
            .update({"is_primary": False, "updated_at": now_iso})
            .eq("entity_type", target_type)
            .eq("entity_key", normalized_key)
            .execute()
        )
        if hasattr(reset, "error") and reset.error:
            raise RuntimeError(f"Failed to reset network logo role selection: {reset.error}")
        select = (
            db.schema("admin")
            .table("network_streaming_logo_assets")
            .update({"is_primary": True, "updated_at": now_iso})
            .eq("entity_type", target_type)
            .eq("entity_key", normalized_key)
            .eq("id", asset_id)
            .execute()
        )
        if hasattr(select, "error") and select.error:
            raise RuntimeError(f"Failed to set selected network wordmark: {select.error}")
        return

    select = (
        db.schema("admin")
        .table("network_streaming_logo_assets")
        .update({"is_primary": False, "updated_at": now_iso})
        .eq("entity_type", target_type)
        .eq("entity_key", normalized_key)
        .eq("id", asset_id)
        .execute()
    )
    if hasattr(select, "error") and select.error:
        raise RuntimeError(f"Failed to set selected network icon: {select.error}")


def _set_brand_role_selection(
    *,
    db: SupabaseAdminClient,
    target_type: BrandLogoTargetType,
    target_key: str,
    asset_id: str,
    logo_role: BrandLogoRole,
    selection_origin: str,
) -> None:
    normalized_key = _normalize_text(target_key).casefold()
    rows_response = (
        db.schema("admin")
        .table("brand_logo_assets")
        .select("id,is_primary,metadata")
        .eq("target_type", target_type)
        .eq("target_key", normalized_key)
        .execute()
    )
    if hasattr(rows_response, "error") and rows_response.error:
        raise RuntimeError(f"Failed to load brand logo options for selection: {rows_response.error}")
    rows = rows_response.data or []
    now_iso = datetime.now(tz=UTC).isoformat()

    for row in rows:
        current_id = _normalize_text(row.get("id"))
        metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}
        role = _normalize_logo_role(metadata.get("logo_role") if isinstance(metadata, dict) else None)
        if role == "wordmark" and not isinstance(metadata, dict):
            role = "wordmark" if bool(row.get("is_primary")) else "icon"
        if role != logo_role:
            continue

        new_metadata = dict(metadata or {})
        new_metadata["logo_role"] = logo_role
        new_metadata["selection_updated_at"] = now_iso
        new_metadata["selection_origin"] = selection_origin
        new_metadata["selected_for_role"] = current_id == asset_id

        patch: dict[str, Any] = {
            "metadata": new_metadata,
            "updated_at": now_iso,
        }
        if logo_role == "wordmark":
            patch["is_primary"] = current_id == asset_id
        else:
            patch["is_primary"] = False

        update = db.schema("admin").table("brand_logo_assets").update(patch).eq("id", current_id).execute()
        if hasattr(update, "error") and update.error:
            raise RuntimeError(f"Failed to update brand logo selection: {update.error}")


def _selected_logo_role_summary(
    *,
    target_type: BrandLogoTargetType,
    target_key: str,
    include_related: bool,
) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for role in ("wordmark", "icon"):
        payload = _list_logo_options(
            target_type=target_type,
            target_key=target_key,
            logo_role=role,  # type: ignore[arg-type]
            include_related=include_related,
            offset=0,
            limit=500,
        )
        rows = payload.get("rows", []) if isinstance(payload, dict) else []
        if not isinstance(rows, list):
            rows = []
        selected_row = next((row for row in rows if bool(row.get("is_selected_for_role"))), None)
        summary[role] = {
            "selected_asset_id": _normalize_text((selected_row or {}).get("id")) or None,
            "selected_source_provider": _normalize_text((selected_row or {}).get("source_provider")) or None,
            "count": len(rows),
        }
    return summary


def _select_logo_option(
    *,
    payload: BrandLogosOptionSelectRequest,
    db: SupabaseAdminClient | None = None,
) -> dict[str, Any]:
    db = db or get_supabase_admin_client()
    normalized_target_key = _normalize_text(payload.target_key).casefold()
    target_label = _normalize_text(payload.target_label) or normalized_target_key
    asset_id = _normalize_text(payload.asset_id)

    if not asset_id and payload.candidate is None:
        raise ValueError("Either asset_id or candidate is required")

    selected_row: dict[str, Any] | None = None
    selection_origin = "manual_picker"
    if payload.candidate is not None:
        selected_row = _import_logo_option_candidate(
            db=db,
            target_type=payload.target_type,
            target_key=normalized_target_key,
            target_label=target_label,
            logo_role=payload.logo_role,
            source_url=_normalize_text(payload.candidate.source_url),
            source_provider=payload.candidate.source_provider,
            discovered_from=payload.candidate.discovered_from,
            selection_origin=selection_origin,
        )
        asset_id = _normalize_text(selected_row.get("id"))
    elif asset_id.startswith("related:"):
        if payload.target_type not in {"publication", "social"}:
            raise ValueError("Related logo options are supported only for publication/social targets")
        related_rows = _find_related_network_streaming_assets_by_host(
            target_type=payload.target_type,
            target_host=normalized_target_key,
            logo_role=payload.logo_role,
            limit=200,
        )
        selected_related = next((row for row in related_rows if _normalize_text(row.get("id")) == asset_id), None)
        if not selected_related:
            raise KeyError("Related logo option not found")
        selected_row = _import_logo_option_candidate(
            db=db,
            target_type=payload.target_type,
            target_key=normalized_target_key,
            target_label=target_label,
            logo_role=payload.logo_role,
            source_url=_normalize_text(selected_related.get("source_url")),
            source_provider=_normalize_text(selected_related.get("source_provider")) or "related_network_streaming",
            discovered_from=_normalize_text(selected_related.get("discovered_from")) or None,
            selection_origin="related_pair",
        )
        asset_id = _normalize_text(selected_row.get("id"))
        selection_origin = "related_pair"
    else:
        selected_row = _fetch_logo_option_row(
            target_type=payload.target_type,
            target_key=normalized_target_key,
            asset_id=asset_id,
        )
        if not selected_row:
            raise KeyError("Logo option not found")
        row_source_url = _normalize_text(selected_row.get("source_url"))
        if payload.target_type in {"network", "streaming", "production"}:
            _set_network_role_selection(
                db=db,
                target_type=payload.target_type,
                target_key=normalized_target_key,
                asset_id=asset_id,
                logo_role=payload.logo_role,
            )
        else:
            _set_brand_role_selection(
                db=db,
                target_type=payload.target_type,
                target_key=normalized_target_key,
                asset_id=asset_id,
                logo_role=payload.logo_role,
                selection_origin=selection_origin,
            )
        if row_source_url:
            selected_row = _fetch_logo_option_row(
                target_type=payload.target_type,
                target_key=normalized_target_key,
                asset_id=asset_id,
            )

    if not selected_row:
        raise RuntimeError("Failed to select logo option")
    selected_asset_id = _normalize_text(selected_row.get("id"))
    if payload.target_type in {"network", "streaming", "production"}:
        _set_network_role_selection(
            db=db,
            target_type=payload.target_type,
            target_key=normalized_target_key,
            asset_id=selected_asset_id,
            logo_role=payload.logo_role,
        )
    else:
        _set_brand_role_selection(
            db=db,
            target_type=payload.target_type,
            target_key=normalized_target_key,
            asset_id=selected_asset_id,
            logo_role=payload.logo_role,
            selection_origin=selection_origin,
        )

    refreshed_selected = _fetch_logo_option_row(
        target_type=payload.target_type,
        target_key=normalized_target_key,
        asset_id=selected_asset_id,
    )
    return {
        "selected": refreshed_selected or selected_row,
        "summary": _selected_logo_role_summary(
            target_type=payload.target_type,
            target_key=normalized_target_key,
            include_related=payload.target_type in {"publication", "social"},
        ),
    }


def _load_related_pair_candidates_for_sync(
    *,
    target_type: BrandLogoTargetType,
    target_key: str,
) -> list[dict[str, Any]]:
    if target_type not in {"publication", "social"}:
        return []
    return _find_related_network_streaming_assets_by_host(
        target_type=target_type,
        target_host=target_key,
        logo_role=None,
        limit=160,
    )


def _load_role_counts(
    *,
    target_type: BrandLogoTargetType,
    target_key: str,
    target_label: str,
) -> dict[str, int]:
    counts = {"wordmark": 0, "icon": 0}
    key = _normalize_text(target_key).casefold()
    if target_type in {"network", "streaming", "production"}:
        entity_key = _normalize_text(target_label).casefold()
        rows = pg.fetch_all(
            """
            select is_primary, hosted_logo_url
            from admin.network_streaming_logo_assets
            where entity_type = %s
              and entity_key = %s
              and mirror_status = 'mirrored'
            """,
            [target_type, entity_key],
        )
        for row in rows:
            if not bool(row.get("hosted_logo_url")):
                continue
            role = "wordmark" if bool(row.get("is_primary")) else "icon"
            counts[role] += 1
        return counts

    rows = pg.fetch_all(
        """
        select is_primary, hosted_logo_url, metadata
        from admin.brand_logo_assets
        where target_type = %s
          and target_key = %s
          and mirror_status = 'mirrored'
        """,
        [target_type, key],
    )
    for row in rows:
        if not bool(row.get("hosted_logo_url")):
            continue
        role = "wordmark"
        metadata = row.get("metadata")
        if isinstance(metadata, dict):
            raw_role = _normalize_text(metadata.get("logo_role")).lower()
            if raw_role in {"wordmark", "icon"}:
                role = raw_role
        elif not bool(row.get("is_primary")):
            role = "icon"
        counts[role] += 1
    return counts


def _load_existing_logo_role_flags(
    *,
    target_type: BrandLogoTargetType,
    target_key: str,
    target_label: str,
) -> dict[str, bool]:
    counts = _load_role_counts(target_type=target_type, target_key=target_key, target_label=target_label)
    return {
        "wordmark": counts["wordmark"] > 0,
        "icon": counts["icon"] > 0,
        "wordmark_count": counts["wordmark"],
        "icon_count": counts["icon"],
    }


def _sync_import_logo_source(
    *,
    db: SupabaseAdminClient,
    target_type: BrandLogoTargetType,
    target_key: str,
    target_label: str,
    role: BrandLogoRole,
    source_url: str,
    source_provider: str | None,
    discovered_from: str | None,
    selection_origin: str,
) -> tuple[bool, bool]:
    from api.routers.admin_scrape import _import_non_show_logo_target  # noqa: PLC0415

    try:
        image_data, content_type = download_image(
            source_url,
            source="manual_logo_import",
            referer=discovered_from,
        )
    except Exception:  # noqa: BLE001
        return False, False

    existed_before = _logo_asset_exists(
        target_type=target_type,
        target_key=target_key,
        target_label=target_label,
        source_url=source_url,
    )
    metadata = {
        "logo_role": role,
        "source_provider": _normalize_text(source_provider) or "unknown",
        "discovered_from": _normalize_text(discovered_from) or None,
        "discovery_timestamp": datetime.now(tz=UTC).isoformat(),
        "selection_origin": selection_origin,
    }
    try:
        status, _, _ = _import_non_show_logo_target(
            db=db,
            target_type=target_type,
            target_key=target_key,
            target_label=target_label,
            set_primary=(role == "wordmark"),
            image_data=image_data,
            sha256=hashlib.sha256(image_data).hexdigest(),
            content_type=_normalize_text(content_type) or "application/octet-stream",
            source_url=source_url,
            source_page_url=_normalize_text(discovered_from),
            source_domain=_normalize_hostname_from_url(source_url),
            metadata=metadata,
        )
    except Exception:  # noqa: BLE001
        return False, existed_before
    return status == "imported", existed_before


def _logo_asset_exists(
    *,
    target_type: BrandLogoTargetType,
    target_key: str,
    target_label: str,
    source_url: str,
) -> bool:
    if target_type in {"network", "streaming", "production"}:
        row = pg.fetch_one(
            """
            select id
            from admin.network_streaming_logo_assets
            where entity_type = %s
              and entity_key = %s
              and source_url = %s
            limit 1
            """,
            [target_type, _normalize_text(target_label).casefold(), source_url],
        )
        return bool(row)

    row = pg.fetch_one(
        """
        select id
        from admin.brand_logo_assets
        where target_type = %s
          and target_key = %s
          and source_url = %s
        limit 1
        """,
        [target_type, _normalize_text(target_key).casefold(), source_url],
    )
    return bool(row)


def _candidate_discovered_urls(row: dict[str, Any]) -> list[str]:
    urls: list[str] = []
    discovered_from = _normalize_text(row.get("discovered_from"))
    if discovered_from:
        urls.append(discovered_from)
    discovered_from_urls = row.get("discovered_from_urls")
    if isinstance(discovered_from_urls, list):
        for value in discovered_from_urls:
            url = _normalize_text(value)
            if url and url not in urls:
                urls.append(url)
    target_key = _normalize_text(row.get("target_key")).casefold()
    if target_key and "." in target_key:
        for fallback in (f"https://{target_key}", f"https://www.{target_key}"):
            if fallback not in urls:
                urls.append(fallback)
    return urls


def _merge_sync_source_query_overrides(
    *,
    wordmark_overrides: dict[str, list[str]],
    icon_overrides: dict[str, list[str]],
) -> dict[str, list[str]]:
    merged: dict[str, list[str]] = {}
    for overrides in (wordmark_overrides, icon_overrides):
        for provider, query_values in overrides.items():
            bucket = merged.setdefault(provider, [])
            seen = {value.casefold() for value in bucket}
            for query_value in query_values:
                normalized = _normalize_text(query_value)
                if not normalized:
                    continue
                key = normalized.casefold()
                if key in seen:
                    continue
                seen.add(key)
                bucket.append(normalized)

    return merged


def _collect_sync_logo_candidates(
    *,
    target_type: BrandLogoTargetType,
    target_key: str,
    target_label: str,
    discovered_from_urls: list[str],
    aliases: list[str],
    source_query_overrides: dict[str, list[str]],
    limit_per_source: int,
    timeout_seconds: float,
) -> list[Any]:
    candidates: list[Any] = []
    for provider, query_values in source_query_overrides.items():
        candidates.extend(
            collect_free_logo_candidates(
                target_label=target_label,
                target_key=target_key,
                discovered_from_urls=discovered_from_urls,
                aliases=aliases,
                source_provider=provider,
                query_override=query_values,
                limit_per_source=limit_per_source,
                timeout_seconds=timeout_seconds,
            )
        )
    candidates.extend(
        collect_free_logo_candidates(
            target_label=target_label,
            target_key=target_key,
            discovered_from_urls=discovered_from_urls,
            aliases=aliases,
            limit_per_source=limit_per_source,
            timeout_seconds=timeout_seconds,
        )
    )

    deduped: list[Any] = []
    seen_urls: set[str] = set()
    for candidate in candidates:
        source_url = _normalize_text(getattr(candidate, "url", None))
        if not source_url:
            continue
        key = source_url.casefold()
        if key in seen_urls:
            continue
        seen_urls.add(key)
        deduped.append(candidate)
    return deduped


def _sync_brand_logos(
    *,
    payload: BrandLogosSyncRequest,
    db: SupabaseAdminClient | None = None,
) -> dict[str, Any]:
    db = db or get_supabase_admin_client()
    if payload.scope == "page" and not payload.page:
        raise ValueError("page is required when scope=page")
    if payload.scope == "show" and not _normalize_text(payload.show_id):
        raise ValueError("show_id is required when scope=show")

    target_types = _resolve_sync_target_types(payload)
    targets = _load_sync_targets(
        target_types=target_types,
        scope=payload.scope,
        show_id=payload.show_id,
        limit=payload.limit,
    )

    metrics = {
        "targets_scanned": len(targets),
        "targets_with_wordmark": 0,
        "targets_with_icon": 0,
        "imports_created": 0,
        "imports_updated": 0,
        "skipped": 0,
        "failed": 0,
        "unresolved": 0,
        "related_pairs_created": 0,
        "options_imported_wordmark": 0,
        "options_imported_icon": 0,
    }
    role_cap = 5

    for target in targets:
        target_type = _normalize_text(target.get("target_type"))  # type: ignore[assignment]
        target_key = _normalize_text(target.get("target_key")).casefold()
        target_label = _normalize_text(target.get("target_label")) or target_key
        existing = _load_existing_logo_role_flags(
            target_type=target_type,  # type: ignore[arg-type]
            target_key=target_key,
            target_label=target_label,
        )
        existing_wordmark_count = int(existing.get("wordmark_count") or 0)
        existing_icon_count = int(existing.get("icon_count") or 0)
        wordmark_source_overrides = _load_logo_source_query_overrides(
            target_type=target_type,  # type: ignore[arg-type]
            target_key=target_key,
            logo_role="wordmark",
        )
        icon_source_overrides = _load_logo_source_query_overrides(
            target_type=target_type,  # type: ignore[arg-type]
            target_key=target_key,
            logo_role="icon",
        )
        desired_wordmark_count = (
            role_cap
            if (payload.force or not payload.only_missing or not existing["wordmark"])
            else existing_wordmark_count
        )
        desired_icon_count = (
            role_cap if (payload.force or not payload.only_missing or not existing["icon"]) else existing_icon_count
        )
        if payload.only_missing and not payload.force:
            if wordmark_source_overrides:
                desired_wordmark_count = min(role_cap, max(desired_wordmark_count, existing_wordmark_count + 1))
            if icon_source_overrides:
                desired_icon_count = min(role_cap, max(desired_icon_count, existing_icon_count + 1))
        need_wordmark = existing_wordmark_count < desired_wordmark_count
        need_icon = existing_icon_count < desired_icon_count

        if not need_wordmark and not need_icon:
            metrics["skipped"] += 1
            metrics["targets_with_wordmark"] += int(existing["wordmark"])
            metrics["targets_with_icon"] += int(existing["icon"])
            continue

        imported_counts = {"wordmark": 0, "icon": 0}
        candidate_failures = 0

        related_candidates = _load_related_pair_candidates_for_sync(
            target_type=target_type,  # type: ignore[arg-type]
            target_key=target_key,
        )
        for related in related_candidates:
            role = _normalize_logo_role(related.get("logo_role"))
            if role == "wordmark":
                if existing_wordmark_count + imported_counts["wordmark"] >= desired_wordmark_count:
                    continue
            else:
                if existing_icon_count + imported_counts["icon"] >= desired_icon_count:
                    continue
            source_url = _normalize_text(related.get("source_url"))
            if not source_url:
                continue
            imported, existed_before = _sync_import_logo_source(
                db=db,
                target_type=target_type,  # type: ignore[arg-type]
                target_key=target_key,
                target_label=target_label,
                role=role,
                source_url=source_url,
                source_provider="related_network_streaming",
                discovered_from=_normalize_text(related.get("discovered_from")) or source_url,
                selection_origin="related_pair",
            )
            if not imported:
                candidate_failures += 1
                continue
            imported_counts[role] += 1
            metrics[f"options_imported_{role}"] += 1
            if existed_before:
                metrics["imports_updated"] += 1
            else:
                metrics["imports_created"] += 1
                metrics["related_pairs_created"] += 1

            if (
                existing_wordmark_count + imported_counts["wordmark"] >= desired_wordmark_count
                and existing_icon_count + imported_counts["icon"] >= desired_icon_count
            ):
                break

        discovered_urls = _candidate_discovered_urls(target)
        aliases = [target_label]
        if target_key and "." in target_key:
            aliases.append(target_key.split(".", 1)[0])

        source_query_overrides = _merge_sync_source_query_overrides(
            wordmark_overrides=wordmark_source_overrides if need_wordmark else {},
            icon_overrides=icon_source_overrides if need_icon else {},
        )
        candidates = _collect_sync_logo_candidates(
            target_type=target_type,  # type: ignore[arg-type]
            target_label=target_label,
            target_key=target_key,
            discovered_from_urls=discovered_urls,
            aliases=aliases,
            source_query_overrides=source_query_overrides,
            limit_per_source=max(10, role_cap + payload.limit // 10),
            timeout_seconds=15.0,
        )

        for candidate in candidates[:120]:
            role = _normalize_logo_role(
                _detect_logo_role(candidate_url=candidate.url, content_type=None, width=None, height=None)
            )
            if role == "wordmark":
                if existing_wordmark_count + imported_counts["wordmark"] >= desired_wordmark_count:
                    continue
            else:
                if existing_icon_count + imported_counts["icon"] >= desired_icon_count:
                    continue
            source_url = _normalize_text(candidate.url)
            if not source_url:
                continue
            imported, existed_before = _sync_import_logo_source(
                db=db,
                target_type=target_type,  # type: ignore[arg-type]
                target_key=target_key,
                target_label=target_label,
                role=role,
                source_url=source_url,
                source_provider=_normalize_text(candidate.source_provider) or "unknown",
                discovered_from=_normalize_text(candidate.discovered_from) or source_url,
                selection_origin="sync",
            )
            if not imported:
                candidate_failures += 1
                continue
            imported_counts[role] += 1
            metrics[f"options_imported_{role}"] += 1
            if existed_before:
                metrics["imports_updated"] += 1
            else:
                metrics["imports_created"] += 1

            if (
                existing_wordmark_count + imported_counts["wordmark"] >= desired_wordmark_count
                and existing_icon_count + imported_counts["icon"] >= desired_icon_count
            ):
                break

        final_wordmark_count = existing_wordmark_count + imported_counts["wordmark"]
        final_icon_count = existing_icon_count + imported_counts["icon"]
        final_wordmark = final_wordmark_count > 0
        final_icon = final_icon_count > 0
        if final_wordmark:
            metrics["targets_with_wordmark"] += 1
        if final_icon:
            metrics["targets_with_icon"] += 1

        unresolved = final_wordmark_count < desired_wordmark_count or final_icon_count < desired_icon_count
        if unresolved:
            metrics["unresolved"] += 1
        elif imported_counts["wordmark"] == 0 and imported_counts["icon"] == 0:
            metrics["skipped"] += 1

        if candidate_failures > 0 and imported_counts["wordmark"] == 0 and imported_counts["icon"] == 0 and unresolved:
            metrics["failed"] += 1

    return {
        "scope": payload.scope,
        "page": payload.page,
        "show_id": payload.show_id,
        "target_types": target_types,
        **metrics,
    }


def _is_service_unavailable_error(error: RuntimeError) -> bool:
    message = str(error).strip().lower()
    return (
        "table is unavailable" in message
        or "run backend migrations" in message
        or "schema" in message
        and "missing" in message
        or "is not migrated" in message
        or "connection pool exhausted" in message
        or "database pool initialization failed" in message
    )


def _to_http_exception(error: Exception) -> HTTPException:
    if isinstance(error, KeyError):
        detail = str(error).strip().strip('"').strip("'") or "Not found"
        return HTTPException(status_code=404, detail=detail)
    if isinstance(error, ValueError):
        return HTTPException(status_code=400, detail=str(error))
    if isinstance(error, RuntimeError) and _is_service_unavailable_error(error):
        return HTTPException(status_code=503, detail=str(error))
    return HTTPException(status_code=500, detail=str(error) or "Internal server error")


@router.get("/shows-franchises")
def get_shows_franchises(
    q: str = Query(default=""),
    limit: int = Query(default=300, ge=1, le=1000),
    _: AdminUser = None,
) -> dict[str, Any]:
    try:
        return brands_franchises.list_shows_franchises(q=q, limit=limit)
    except Exception as error:  # noqa: BLE001
        raise _to_http_exception(error) from error


@router.get("/franchise-rules")
def get_franchise_rules(_: AdminUser = None) -> dict[str, Any]:
    try:
        return brands_franchises.list_franchise_rules()
    except Exception as error:  # noqa: BLE001
        raise _to_http_exception(error) from error


@router.put("/franchise-rules/{franchise_key}")
def put_franchise_rule(
    franchise_key: str,
    payload: UpdateFranchiseRuleRequest,
    user: AdminUser = None,
) -> dict[str, Any]:
    actor = str((user or {}).get("id") or "admin")
    try:
        return brands_franchises.update_franchise_rule(
            franchise_key=franchise_key,
            payload=payload.model_dump(exclude_none=True),
            actor=actor,
        )
    except Exception as error:  # noqa: BLE001
        raise _to_http_exception(error) from error


@router.post("/franchise-rules/{franchise_key}/apply")
def post_apply_franchise_rule(
    franchise_key: str,
    payload: ApplyFranchiseRuleRequest,
    user: AdminUser = None,
) -> dict[str, Any]:
    actor = str((user or {}).get("id") or "admin")
    try:
        return brands_franchises.apply_franchise_rule(
            franchise_key=franchise_key,
            missing_only=payload.missing_only,
            dry_run=payload.dry_run,
            actor=actor,
        )
    except Exception as error:  # noqa: BLE001
        raise _to_http_exception(error) from error


@router.get("/families")
def get_brand_families(
    active_only: bool = Query(default=True),
    _: AdminUser = None,
) -> dict[str, Any]:
    try:
        return brand_families.list_families(active_only=active_only)
    except Exception as error:  # noqa: BLE001
        raise _to_http_exception(error) from error


@router.post("/families")
def post_brand_family(
    payload: CreateBrandFamilyRequest,
    user: AdminUser = None,
) -> dict[str, Any]:
    actor = str((user or {}).get("id") or "admin")
    try:
        return brand_families.create_family(payload=payload.model_dump(), actor=actor)
    except Exception as error:  # noqa: BLE001
        raise _to_http_exception(error) from error


@router.patch("/families/{family_id}")
def patch_brand_family(
    family_id: str,
    payload: PatchBrandFamilyRequest,
    user: AdminUser = None,
) -> dict[str, Any]:
    actor = str((user or {}).get("id") or "admin")
    try:
        return brand_families.patch_family(
            family_id=family_id,
            payload=payload.model_dump(exclude_unset=True),
            actor=actor,
        )
    except Exception as error:  # noqa: BLE001
        raise _to_http_exception(error) from error


@router.post("/families/{family_id}/members")
def post_brand_family_member(
    family_id: str,
    payload: AddBrandFamilyMemberRequest,
    user: AdminUser = None,
) -> dict[str, Any]:
    actor = str((user or {}).get("id") or "admin")
    try:
        return brand_families.add_family_member(family_id=family_id, payload=payload.model_dump(), actor=actor)
    except Exception as error:  # noqa: BLE001
        raise _to_http_exception(error) from error


@router.delete("/families/{family_id}/members/{member_id}")
def delete_brand_family_member(
    family_id: str,
    member_id: str,
    _: AdminUser = None,
) -> dict[str, Any]:
    try:
        return brand_families.delete_family_member(family_id=family_id, member_id=member_id)
    except Exception as error:  # noqa: BLE001
        raise _to_http_exception(error) from error


@router.get("/families/suggestions")
def get_brand_family_suggestions(_: AdminUser = None) -> dict[str, Any]:
    try:
        return brand_families.list_family_suggestions()
    except Exception as error:  # noqa: BLE001
        raise _to_http_exception(error) from error


@router.get("/families/by-entity")
def get_brand_family_by_entity(
    entity_type: Literal["network", "streaming"] = Query(...),
    entity_key: str = Query(...),
    _: AdminUser = None,
) -> dict[str, Any]:
    try:
        family = brand_families.get_family_by_entity(entity_type=entity_type, entity_key=entity_key)
        suggestions = brand_families.list_family_suggestions()
        if not family:
            return {
                "family": None,
                "family_suggestions": suggestions.get("rows", []),
                "shared_links": [],
                "wikipedia_show_urls": [],
            }
        family_id = str(family.get("id") or "")
        links = brand_families.list_family_links(family_id=family_id, active_only=True)
        wiki_rows = brand_families.list_family_wikipedia_show_links(family_id=family_id, limit=500)
        return {
            "family": family,
            "family_suggestions": suggestions.get("rows", []),
            "shared_links": links.get("rows", []),
            "wikipedia_show_urls": wiki_rows.get("rows", []),
        }
    except Exception as error:  # noqa: BLE001
        raise _to_http_exception(error) from error


@router.get("/families/{family_id}/links")
def get_brand_family_links(
    family_id: str,
    active_only: bool = Query(default=False),
    _: AdminUser = None,
) -> dict[str, Any]:
    try:
        return brand_families.list_family_links(family_id=family_id, active_only=active_only)
    except Exception as error:  # noqa: BLE001
        raise _to_http_exception(error) from error


@router.post("/families/{family_id}/links")
def post_brand_family_link(
    family_id: str,
    payload: CreateBrandFamilyLinkRuleRequest,
    user: AdminUser = None,
) -> dict[str, Any]:
    actor = str((user or {}).get("id") or "admin")
    try:
        return brand_families.create_family_link_rule(family_id=family_id, payload=payload.model_dump(), actor=actor)
    except Exception as error:  # noqa: BLE001
        raise _to_http_exception(error) from error


@router.patch("/families/{family_id}/links/{rule_id}")
def patch_brand_family_link(
    family_id: str,
    rule_id: str,
    payload: PatchBrandFamilyLinkRuleRequest,
    user: AdminUser = None,
) -> dict[str, Any]:
    actor = str((user or {}).get("id") or "admin")
    try:
        return brand_families.patch_family_link_rule(
            family_id=family_id,
            rule_id=rule_id,
            payload=payload.model_dump(exclude_unset=True),
            actor=actor,
        )
    except Exception as error:  # noqa: BLE001
        raise _to_http_exception(error) from error


@router.post("/families/{family_id}/links/apply")
def post_brand_family_links_apply(
    family_id: str,
    payload: ApplyBrandFamilyLinksRequest,
    user: AdminUser = None,
) -> dict[str, Any]:
    actor = str((user or {}).get("id") or "admin")
    try:
        return brand_families.apply_family_links(
            family_id=family_id,
            dry_run=payload.dry_run,
            actor=actor,
            rule_ids=payload.rule_ids,
        )
    except Exception as error:  # noqa: BLE001
        raise _to_http_exception(error) from error


@router.post("/families/{family_id}/wikipedia-import")
def post_brand_family_wikipedia_import(
    family_id: str,
    payload: ImportBrandFamilyWikipediaLinksRequest,
    user: AdminUser = None,
) -> dict[str, Any]:
    actor = str((user or {}).get("id") or "admin")
    try:
        result = brand_families.import_family_wikipedia_show_links(
            family_id=family_id,
            actor=actor,
            entity_type=payload.entity_type,
            entity_key=payload.entity_key,
            apply_matched=payload.apply_matched,
            import_source="manual",
        )
        result["wikipedia_show_urls"] = brand_families.list_family_wikipedia_show_links(family_id=family_id, limit=500)
        return result
    except Exception as error:  # noqa: BLE001
        raise _to_http_exception(error) from error


@router.get("/families/{family_id}/wikipedia-show-urls")
def get_brand_family_wikipedia_show_urls(
    family_id: str,
    limit: int = Query(default=500, ge=1, le=5000),
    _: AdminUser = None,
) -> dict[str, Any]:
    try:
        return brand_families.list_family_wikipedia_show_links(family_id=family_id, limit=limit)
    except Exception as error:  # noqa: BLE001
        raise _to_http_exception(error) from error


@router.get("/logos")
def get_brand_logos(
    target_type: BrandLogoTargetType = Query(...),
    q: str = Query(default=""),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    include_missing: bool = Query(default=False),
    target_key: str | None = Query(default=None),
    logo_role: BrandLogoRole | None = Query(default=None),
    source_provider: str | None = Query(default=None),
    include_related: bool = Query(default=False),
    show_id: str | None = Query(default=None),
    _: AdminUser = None,
) -> dict[str, Any]:
    try:
        return _list_brand_logos(
            target_type=target_type,
            q=q,
            limit=limit,
            offset=offset,
            include_missing=include_missing,
            target_key=target_key,
            logo_role=logo_role,
            source_provider=source_provider,
            include_related=include_related,
            show_id=show_id,
        )
    except Exception as error:  # noqa: BLE001
        raise _to_http_exception(error) from error


@router.get("/logo-targets")
def get_brand_logo_targets(
    target_type: BrandLogoTargetType = Query(...),
    q: str = Query(default=""),
    limit: int = Query(default=50, ge=1, le=200),
    show_id: str | None = Query(default=None),
    _: AdminUser = None,
) -> dict[str, Any]:
    try:
        return _list_logo_targets(target_type=target_type, q=q, limit=limit, show_id=show_id)
    except Exception as error:  # noqa: BLE001
        raise _to_http_exception(error) from error


@router.get("/logos/options/sources")
def get_brand_logo_option_sources(
    target_type: BrandLogoTargetType = Query(...),
    target_key: str = Query(...),
    target_label: str | None = Query(default=None),
    logo_role: BrandLogoRole = Query(...),
    include_related: bool = Query(default=True),
    _: AdminUser = None,
) -> dict[str, Any]:
    try:
        return _list_logo_option_sources(
            target_type=target_type,
            target_key=target_key,
            target_label=target_label,
            logo_role=logo_role,
            include_related=include_related,
        )
    except Exception as error:  # noqa: BLE001
        raise _to_http_exception(error) from error


@router.post("/logos/options/discover")
def post_brand_logo_option_discover(
    payload: BrandLogosOptionDiscoverRequest,
    _: AdminUser = None,
) -> dict[str, Any]:
    try:
        return _discover_logo_candidates_by_source(payload)
    except Exception as error:  # noqa: BLE001
        raise _to_http_exception(error) from error


@router.post("/logos/options/source-query")
def post_brand_logo_option_source_query(
    payload: BrandLogosSourceQueryRequest,
    _: AdminUser = None,
) -> dict[str, Any]:
    try:
        return _save_logo_source_query(payload)
    except Exception as error:  # noqa: BLE001
        raise _to_http_exception(error) from error


@router.post("/logos/options/select")
def post_brand_logo_option_select(
    payload: BrandLogosOptionSelectRequest,
    _: AdminUser = None,
) -> dict[str, Any]:
    try:
        return _select_logo_option(payload=payload)
    except Exception as error:  # noqa: BLE001
        raise _to_http_exception(error) from error


@router.post("/logos/sync")
def post_brand_logos_sync(
    payload: BrandLogosSyncRequest,
    _: AdminUser = None,
) -> dict[str, Any]:
    try:
        return _sync_brand_logos(payload=payload)
    except Exception as error:  # noqa: BLE001
        raise _to_http_exception(error) from error
