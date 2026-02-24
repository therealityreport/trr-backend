#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any
from urllib.parse import quote, urlparse

import requests

from trr_backend.db.admin import create_supabase_admin_client
from trr_backend.integrations.brandfetch import (
    BrandfetchAuthError,
    BrandfetchNotFoundError,
    BrandfetchRequestError,
    fetch_brandfetch_logo_candidates,
    normalize_domain,
)
from trr_backend.integrations.imdb.companycredits import (
    ImdbCompanyCreditsError,
    fetch_imdb_company_credits,
)
from trr_backend.integrations.imdb.graphql_operations import fetch_hero_watch_box
from trr_backend.integrations.logopedia import (
    LogopediaNoFilesError,
    LogopediaRequestError,
    fetch_logopedia_logo_candidates,
)
from trr_backend.integrations.tmdb.client import (
    TmdbClientError,
    fetch_network_alternative_names,
    fetch_network_details,
    resolve_api_key,
    resolve_bearer_token,
)
from trr_backend.media.s3_mirror import (
    MonochromeLogoMirrorResult,
    get_s3_client,
    mirror_external_logo_row,
    mirror_logo_monochrome_variants_row,
    svg_rasterizer_available,
)
from trr_backend.utils.env import load_env

WIKIDATA_SEARCH_URL = "https://www.wikidata.org/w/api.php"
WIKIDATA_ENTITY_URL = "https://www.wikidata.org/wiki/Special:EntityData/{item_id}.json"
WIKIDATA_ITEM_RE = re.compile(r"^Q\d+$", re.IGNORECASE)
LOGO_CLAIM_IDS = ("P154", "P2910")
DEFAULT_SOURCE_PRIORITY = ["override", "tmdb", "wikimedia", "official", "catalog"]
LOGO_SOURCE_CAPS: dict[str, int] = {
    "override": 12,
    "tmdb": 5,
    "wikimedia": 5,
    "official": 8,
    "catalog": 12,
    "imdb": 8,
}
DISCOVERY_SOURCES = ("official", "catalog", "imdb")
ATTEMPT_TABLE_SOURCES = {"override", "tmdb", "wikimedia", "official", "catalog", "variant"}
LOGO_ASSET_TABLE_SOURCES = {"override", "tmdb", "wikimedia", "official", "catalog", "imdb"}
REQUEST_HEADERS = {
    "accept": "application/json",
    "user-agent": "TRR-Backend/1.0",
}
WIKIDATA_CONNECT_TIMEOUT_SECONDS = 5.0
WIKIDATA_READ_TIMEOUT_SECONDS = 20.0
WIKIDATA_RETRY_ATTEMPTS = 2
WIKIDATA_RETRY_BACKOFF_MS = 300


class FatalSyncError(RuntimeError):
    """Raised when sync should fail immediately instead of marking entity unresolved."""
STREAMING_SUFFIX_PATTERNS = (
    r"\s+amazon channel$",
    r"\s+apple tv channel$",
    r"\s+roku premium channel$",
    r"\s+channel$",
)
STREAMING_TIER_PATTERNS = (
    r"\s+premium plus$",
    r"\s+premium$",
    r"\s+basic with ads$",
    r"\s+standard with ads$",
    r"\s+free with ads$",
    r"\s+with ads$",
    r"\s+essential$",
    r"\s+plus$",
)
KNOWN_METADATA_ALIASES: dict[str, list[str]] = {
    "apple tv store": ["Apple TV"],
    "peacock premium": ["Peacock"],
    "peacock premium plus": ["Peacock"],
    "amazon prime video with ads": ["Prime Video", "Amazon Prime Video"],
    "amazon prime video free with ads": ["Prime Video", "Amazon Prime Video"],
    "fandango at home free": ["Fandango at Home", "Vudu"],
    "plex channel": ["Plex"],
    "spectrum on demand": ["Spectrum"],
    "netflix standard with ads": ["Netflix"],
    "netflix kids": ["Netflix"],
    "paramount plus premium": ["Paramount+"],
    "paramount plus essential": ["Paramount+"],
    "paramount plus basic with ads": ["Paramount+"],
    "paramount plus apple tv channel": ["Paramount+"],
    "paramount+ amazon channel": ["Paramount+"],
    "paramount+ mtv amazon channel": ["Paramount+"],
    "paramount+ originals amazon channel": ["Paramount+"],
    "paramount+ roku premium channel": ["Paramount+"],
    "amc+ amazon channel": ["AMC+"],
    "amc plus apple tv channel": ["AMC+"],
    "amc+ roku premium channel": ["AMC+"],
    "allblk amazon channel": ["ALLBLK"],
    "allblk apple tv channel": ["ALLBLK"],
    "hayu amazon channel": ["Hayu"],
    "outtv amazon channel": ["OUTtv"],
    "outtv apple tv channel": ["OUTtv"],
    "crave amazon channel": ["Crave"],
    "hbo max amazon channel": ["HBO Max"],
    "hbo max  amazon channel": ["HBO Max"],
    "itvx premium": ["ITVX"],
    "lionsgate play amazon channel": ["Lionsgate Play"],
    "lionsgate play apple tv channel": ["Lionsgate Play"],
    "stacktv amazon channel": ["StackTV"],
    "teletoon+ amazon channel": ["TELETOON+"],
    "mtv plus amazon channel": ["MTV+"],
    "mtv hits amazon channel": ["MTV Hits"],
    "tv2 skyshowtime": ["SkyShowtime", "TV 2 Play"],
    "universal+ amazon channel": ["Universal+"],
    "wow fiction amazon channel": ["WOW"],
    "xumo play": ["Xumo"],
}


@dataclass
class UnresolvedLogo:
    type: str
    id: str
    name: str
    reason: str


@dataclass
class AttemptRecord:
    source: str
    attempt_url: str | None
    outcome: str
    failure_reason: str | None
    duration_ms: int
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class InventoryEntity:
    entity_type: str
    entity_key: str
    display_name: str
    available_show_count: int
    added_show_count: int


@dataclass
class OverrideConfig:
    id: str
    entity_type: str
    entity_key: str
    display_name_override: str | None
    wikidata_id_override: str | None
    wikipedia_url_override: str | None
    aliases_override: list[str]
    source_priority_override: list[str]
    logo_source_urls_by_source: dict[str, list[str]]


@dataclass
class SyncRunContext:
    tmdb_api_key: str | None
    tmdb_bearer_token: str | None
    tmdb_network_ids_by_key: dict[str, set[int]] = field(default_factory=dict)
    tmdb_network_hints_by_id: dict[int, dict[str, Any]] = field(default_factory=dict)
    provider_imdb_ids_by_provider_id: dict[int, list[str]] = field(default_factory=dict)
    imdb_watch_box_cache: dict[str, dict[str, Any]] = field(default_factory=dict)
    production_imdb_hints_by_key: dict[str, dict[str, list[str]]] = field(default_factory=dict)
    svg_rasterizer_available: bool = False


@dataclass
class SyncSummary:
    run_id: str = ""
    run_status: str = "running"
    resume_cursor_entity_type: str | None = None
    resume_cursor_entity_key: str | None = None
    svg_rasterizer_available: bool = False
    processed: int = 0
    links_enriched: int = 0
    wikidata_linked: int = 0
    wikipedia_linked: int = 0
    logos_mirrored: int = 0
    variants_black_mirrored: int = 0
    variants_white_mirrored: int = 0
    logo_assets_discovered: int = 0
    logo_assets_mirrored: int = 0
    logo_assets_skipped: int = 0
    logo_assets_failed: int = 0
    show_logos_discovered: int = 0
    show_logos_imported: int = 0
    show_logos_skipped: int = 0
    show_logo_failures: int = 0
    failures: int = 0
    unresolved_logos: list[UnresolvedLogo] = field(default_factory=list)
    completion_total: int = 0
    completion_resolved: int = 0
    completion_unresolved: int = 0
    completion_percent: float = 0.0


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="sync_networks_streaming_links",
        description=(
            "Enrich used core.networks/core.watch_providers/core.production_companies rows with "
            "Wikidata + Wikipedia links, "
            "mirror missing logo assets from multiple sources, and generate black/white transparent variants."
        ),
    )
    parser.add_argument("--all", action="store_true", help="Accepted for CLI parity. Script processes all used rows.")
    parser.add_argument("--force", action="store_true", help="Re-enrich rows and force logo/variant re-mirror.")
    parser.add_argument("--dry-run", action="store_true", help="Resolve + print intended updates without writing.")
    parser.add_argument("--skip-s3", action="store_true", help="Skip logo and variant mirroring.")
    parser.add_argument("--unresolved-only", action="store_true", help="Process only unresolved completion rows.")
    parser.add_argument(
        "--refresh-external-sources",
        action="store_true",
        help="Refresh Brandfetch/Logopedia/IMDb discovery even when discovery lock exists.",
    )
    parser.add_argument("--batch-size", type=int, default=25, help="Persist run-progress metrics every N entities.")
    parser.add_argument(
        "--max-runtime-sec",
        type=int,
        default=840,
        help="Gracefully stop after this runtime budget and return resumable cursor.",
    )
    parser.add_argument("--resume-run-id", type=str, default=None, help="Resume an existing run id from stored cursor.")
    parser.add_argument(
        "--entity-type",
        type=str,
        choices=("network", "streaming", "production"),
        default=None,
        help="Optionally process only one entity type.",
    )
    parser.add_argument(
        "--entity-key",
        action="append",
        default=None,
        help="Optionally process only these normalized entity keys (repeatable).",
    )
    parser.add_argument(
        "--start-after",
        type=str,
        default=None,
        help="Start after explicit cursor in form entity_type:entity_key.",
    )
    parser.add_argument("--limit", type=int, default=None, help="Optional per-type processing cap.")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging.")
    return parser.parse_args(argv)


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _name_key(value: Any) -> str:
    return _normalize_text(value).casefold()


def _sanitize_name_variant(name: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^\w\s&+.-]", " ", name)).strip()


def _extract_json_list_strings(value: Any) -> list[str]:
    if isinstance(value, list):
        out: list[str] = []
        for item in value:
            if isinstance(item, str) and item.strip():
                out.append(item.strip())
        return out
    return []


def _to_int(value: Any) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str) and value.strip():
        try:
            return int(value)
        except ValueError:
            return 0
    return 0


def _iter_rows_paged(query, *, page_size: int = 1000):
    start = 0
    while True:
        response = query.range(start, start + page_size - 1).execute()
        if hasattr(response, "error") and response.error:
            raise RuntimeError(f"Supabase paging error: {response.error}")
        rows = response.data or []
        if not isinstance(rows, list) or not rows:
            break
        for row in rows:
            if isinstance(row, dict):
                yield row
        if len(rows) < page_size:
            break
        start += page_size


def _to_pg_text_array_literal(values: list[str]) -> str:
    escaped: list[str] = []
    for item in values:
        text = _normalize_text(item)
        if not text:
            continue
        value = text.replace("\\", "\\\\").replace('"', '\\"')
        escaped.append(f'"{value}"')
    return "{" + ",".join(escaped) + "}"


def _parse_iso_datetime(value: Any) -> datetime | None:
    raw = _normalize_text(value)
    if not raw:
        return None
    candidate = raw
    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _load_discovery_state(
    db,
    *,
    entity_type: str,
    entity_key: str,
) -> dict[str, dict[str, Any]]:
    if not hasattr(db, "schema"):
        return {}
    out: dict[str, dict[str, Any]] = {}
    query = (
        db.schema("admin")
        .table("network_streaming_discovery_state")
        .select("*")
        .eq("entity_type", entity_type)
        .eq("entity_key", entity_key)
    )
    for row in _iter_rows_paged(query):
        source = _normalize_text(row.get("source")).lower()
        if not source:
            continue
        out[source] = row
    return out


def _is_discovery_locked(row: dict[str, Any] | None) -> bool:
    if not isinstance(row, dict) or not row:
        return False
    lock_until = _parse_iso_datetime(row.get("lock_until"))
    if lock_until is None:
        return True
    return lock_until > datetime.now(UTC)


def _upsert_discovery_state(
    db,
    *,
    entity_type: str,
    entity_key: str,
    source: str,
    outcome: str,
    reason: str | None,
    cached_candidate_count: int,
    previous_row: dict[str, Any] | None = None,
) -> None:
    if not hasattr(db, "schema"):
        return
    source_name = _normalize_text(source).lower()
    if source_name not in {"official", "catalog", "imdb", "tmdb", "wikimedia", "override"}:
        return
    previous_attempt_count = _to_int((previous_row or {}).get("attempt_count"))
    payload = {
        "entity_type": entity_type,
        "entity_key": entity_key,
        "source": source_name,
        "last_outcome": outcome if outcome in {"success", "failed", "skipped"} else "failed",
        "last_reason": _normalize_text(reason) or None,
        "attempt_count": max(1, previous_attempt_count + 1),
        "last_attempt_at": _now_iso(),
        "lock_until": None,
        "cached_candidate_count": max(0, int(cached_candidate_count)),
        "updated_at": _now_iso(),
    }
    response = (
        db.schema("admin")
        .table("network_streaming_discovery_state")
        .upsert(payload, on_conflict="entity_type,entity_key,source")
        .execute()
    )
    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Supabase error upserting discovery state: {response.error}")


def _load_sync_run_row(db, *, run_id: str) -> dict[str, Any] | None:
    if not hasattr(db, "schema"):
        return None
    response = (
        db.schema("admin")
        .table("network_streaming_sync_runs")
        .select("*")
        .eq("run_id", run_id)
        .limit(1)
        .execute()
    )
    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Supabase error loading sync run row: {response.error}")
    rows = response.data or []
    if isinstance(rows, list) and rows:
        first = rows[0]
        if isinstance(first, dict):
            return first
    return None


def _load_sync_run_cursor(db, *, run_id: str) -> tuple[str, str] | None:
    row = _load_sync_run_row(db, run_id=run_id)
    if not row:
        return None
    entity_type = _normalize_text(row.get("cursor_entity_type"))
    entity_key = _name_key(row.get("cursor_entity_key"))
    if entity_type in {"network", "streaming", "production"} and entity_key:
        return entity_type, entity_key
    return None


def _upsert_sync_run_state(
    db,
    *,
    run_id: str,
    status: str,
    summary: SyncSummary,
    cursor: tuple[str, str] | None,
    started_at: str,
    finished_at: str | None = None,
    error_message: str | None = None,
) -> None:
    if not hasattr(db, "schema"):
        return
    cursor_entity_type = cursor[0] if cursor else None
    cursor_entity_key = cursor[1] if cursor else None
    payload = {
        "run_id": run_id,
        "status": status,
        "started_at": started_at,
        "finished_at": finished_at,
        "cursor_entity_type": cursor_entity_type,
        "cursor_entity_key": cursor_entity_key,
        "processed": int(summary.processed),
        "links_enriched": int(summary.links_enriched),
        "wikidata_linked": int(summary.wikidata_linked),
        "wikipedia_linked": int(summary.wikipedia_linked),
        "logos_mirrored": int(summary.logos_mirrored),
        "variants_black_mirrored": int(summary.variants_black_mirrored),
        "variants_white_mirrored": int(summary.variants_white_mirrored),
        "logo_assets_discovered": int(summary.logo_assets_discovered),
        "logo_assets_mirrored": int(summary.logo_assets_mirrored),
        "logo_assets_skipped": int(summary.logo_assets_skipped),
        "logo_assets_failed": int(summary.logo_assets_failed),
        "show_logos_discovered": int(summary.show_logos_discovered),
        "show_logos_imported": int(summary.show_logos_imported),
        "show_logos_skipped": int(summary.show_logos_skipped),
        "show_logo_failures": int(summary.show_logo_failures),
        "completion_total": int(summary.completion_total),
        "completion_resolved": int(summary.completion_resolved),
        "completion_unresolved": int(summary.completion_unresolved),
        "completion_percent": float(summary.completion_percent),
        "failures": int(summary.failures),
        "error_message": _normalize_text(error_message) or None,
    }
    response = (
        db.schema("admin")
        .table("network_streaming_sync_runs")
        .upsert(payload, on_conflict="run_id")
        .execute()
    )
    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Supabase error upserting sync run state: {response.error}")


def _parse_start_after(value: str | None) -> tuple[str, str] | None:
    text = _normalize_text(value)
    if not text:
        return None
    if ":" not in text:
        raise ValueError("start-after must be in form entity_type:entity_key")
    entity_type, entity_key = text.split(":", 1)
    type_name = _normalize_text(entity_type).lower()
    key = _name_key(entity_key)
    if type_name not in {"network", "streaming", "production"} or not key:
        raise ValueError("start-after must use entity_type in network|streaming|production and non-empty key")
    return type_name, key


def _apply_start_after_cursor(
    entities: list[InventoryEntity],
    *,
    start_after: tuple[str, str] | None,
) -> list[InventoryEntity]:
    if not start_after:
        return entities
    for idx, entity in enumerate(entities):
        if (entity.entity_type, entity.entity_key) == start_after:
            return entities[idx + 1 :]
    return entities


def _url_looks_like_svg(value: str | None) -> bool:
    text = _normalize_text(value).lower()
    if not text:
        return False
    return ".svg" in text or "format=svg" in text


def _candidates_contain_svg(candidates: dict[str, list[str]]) -> bool:
    for urls in candidates.values():
        for url in urls:
            if _url_looks_like_svg(url):
                return True
    return False


def _score_search_result(name: str, candidate: dict[str, Any]) -> int:
    score = 0
    target = name.casefold()
    label = _normalize_text(candidate.get("label")).casefold()
    description = _normalize_text(candidate.get("description")).casefold()

    if label == target:
        score += 100
    if target and target in label:
        score += 35
    if target and label and label in target:
        score += 20
    if "network" in description:
        score += 15
    if "television" in description:
        score += 10
    if "streaming" in description:
        score += 10
    return score


def _extract_commons_logo_file(entity: dict[str, Any]) -> str | None:
    claims = entity.get("claims")
    if not isinstance(claims, dict):
        return None

    for claim_id in LOGO_CLAIM_IDS:
        claim_rows = claims.get(claim_id)
        if not isinstance(claim_rows, list):
            continue
        for claim in claim_rows:
            if not isinstance(claim, dict):
                continue
            mainsnak = claim.get("mainsnak")
            if not isinstance(mainsnak, dict):
                continue
            datavalue = mainsnak.get("datavalue")
            if not isinstance(datavalue, dict):
                continue
            value = datavalue.get("value")
            if isinstance(value, str) and value.strip():
                return value.strip()
    return None


def _commons_file_urls(file_name: str | None) -> list[str]:
    text = _normalize_text(file_name)
    if not text:
        return []
    normalized = text.replace(" ", "_")
    encoded = quote(normalized, safe="()_-")
    return [
        f"https://commons.wikimedia.org/wiki/Special:FilePath/{encoded}?width=1024",
        f"https://commons.wikimedia.org/wiki/Special:FilePath/{encoded}",
    ]


def _fetch_wikidata_entity(item_id: str) -> dict[str, Any] | None:
    item_id = _normalize_text(item_id).upper()
    if not WIKIDATA_ITEM_RE.match(item_id):
        return None

    payload = _request_json_with_retry(
        WIKIDATA_ENTITY_URL.format(item_id=item_id),
        params=None,
    )
    entities = payload.get("entities")
    if not isinstance(entities, dict):
        return None
    entity = entities.get(item_id)
    return entity if isinstance(entity, dict) else None


def _search_wikidata_item(name: str) -> str | None:
    candidate = _normalize_text(name)
    if not candidate:
        return None

    payload = _request_json_with_retry(
        WIKIDATA_SEARCH_URL,
        params={
            "action": "wbsearchentities",
            "format": "json",
            "language": "en",
            "type": "item",
            "limit": 10,
            "search": candidate,
        },
    )
    rows = payload.get("search")
    if not isinstance(rows, list) or not rows:
        return None

    best = None
    best_score = -1
    for row in rows:
        if not isinstance(row, dict):
            continue
        item_id = _normalize_text(row.get("id")).upper()
        if not WIKIDATA_ITEM_RE.match(item_id):
            continue
        score = _score_search_result(candidate, row)
        if score > best_score:
            best_score = score
            best = item_id
    return best


def _request_json_with_retry(url: str, *, params: dict[str, Any] | None) -> dict[str, Any]:
    last_error: Exception | None = None
    for attempt in range(WIKIDATA_RETRY_ATTEMPTS):
        try:
            response = requests.get(
                url,
                headers=REQUEST_HEADERS,
                params=params,
                timeout=(WIKIDATA_CONNECT_TIMEOUT_SECONDS, WIKIDATA_READ_TIMEOUT_SECONDS),
            )
            if response.status_code in {429} or response.status_code >= 500:
                if attempt + 1 < WIKIDATA_RETRY_ATTEMPTS:
                    if WIKIDATA_RETRY_BACKOFF_MS > 0:
                        time.sleep(WIKIDATA_RETRY_BACKOFF_MS / 1000)
                    continue
                response.raise_for_status()
            response.raise_for_status()
            payload = response.json()
            if isinstance(payload, dict):
                return payload
            raise RuntimeError("wikidata_invalid_json")
        except (requests.ConnectTimeout, requests.ReadTimeout, requests.Timeout) as exc:
            last_error = exc
            if attempt + 1 < WIKIDATA_RETRY_ATTEMPTS:
                if WIKIDATA_RETRY_BACKOFF_MS > 0:
                    time.sleep(WIKIDATA_RETRY_BACKOFF_MS / 1000)
                continue
            raise
        except requests.RequestException as exc:
            last_error = exc
            if attempt + 1 < WIKIDATA_RETRY_ATTEMPTS:
                if WIKIDATA_RETRY_BACKOFF_MS > 0:
                    time.sleep(WIKIDATA_RETRY_BACKOFF_MS / 1000)
                continue
            raise
    if last_error is not None:
        raise last_error
    raise RuntimeError("wikidata_request_failed")


def _extract_enwiki_url(entity: dict[str, Any]) -> str | None:
    sitelinks = entity.get("sitelinks")
    if not isinstance(sitelinks, dict):
        return None

    def _wiki_url(site_key: str, title_value: Any) -> str | None:
        title = _normalize_text(title_value)
        if not title:
            return None
        if not site_key.endswith("wiki"):
            return None
        lang = site_key[:-4].replace("_", "-")
        if not lang or lang == "commons":
            return None
        return f"https://{lang}.wikipedia.org/wiki/{quote(title.replace(' ', '_'))}"

    enwiki = sitelinks.get("enwiki")
    if isinstance(enwiki, dict):
        preferred = _wiki_url("enwiki", enwiki.get("title"))
        if preferred:
            return preferred

    # Fallback to any available Wikipedia sitelink when enwiki does not exist.
    fallback_order = ("simplewiki", "dewiki", "frwiki", "eswiki", "itwiki")
    for site_key in fallback_order:
        row = sitelinks.get(site_key)
        if not isinstance(row, dict):
            continue
        url = _wiki_url(site_key, row.get("title"))
        if url:
            return url

    for site_key, row in sitelinks.items():
        if not isinstance(row, dict):
            continue
        url = _wiki_url(_normalize_text(site_key), row.get("title"))
        if url:
            return url
    return None


def _derive_metadata_aliases(entity_type: str, display_name: str) -> list[str]:
    aliases: list[str] = []
    seen: set[str] = set()

    def add(value: str) -> None:
        text = _normalize_text(re.sub(r"\s+", " ", value))
        key = text.casefold()
        if not text or key in seen:
            return
        seen.add(key)
        aliases.append(text)

    add(display_name)
    key = _name_key(display_name)
    for alias in KNOWN_METADATA_ALIASES.get(key, []):
        add(alias)

    if entity_type != "streaming":
        return aliases

    base = _normalize_text(display_name)
    add(base.replace("+", " plus "))
    add(base.replace("+", " "))
    add(base.replace("plus", "+"))

    work = base
    changed = True
    while changed:
        changed = False
        for pattern in (*STREAMING_SUFFIX_PATTERNS, *STREAMING_TIER_PATTERNS):
            stripped = re.sub(pattern, "", work, flags=re.IGNORECASE).strip()
            if stripped and stripped != work:
                add(stripped)
                work = stripped
                changed = True

    if "amazon channel" in _name_key(base):
        add(re.sub(r"\s+amazon channel$", "", base, flags=re.IGNORECASE).strip())
    if "apple tv channel" in _name_key(base):
        add(re.sub(r"\s+apple tv channel$", "", base, flags=re.IGNORECASE).strip())
    if "roku premium channel" in _name_key(base):
        add(re.sub(r"\s+roku premium channel$", "", base, flags=re.IGNORECASE).strip())

    # Partner-suffixed variants frequently resolve via root brand names.
    add(re.sub(r"\s+(mtv|originals|hits|fiction|one)$", "", work, flags=re.IGNORECASE).strip())
    add(re.sub(r"\s+tv$", "", work, flags=re.IGNORECASE).strip())
    add(re.sub(r"\s+play$", "", work, flags=re.IGNORECASE).strip())

    return aliases


def _expand_lookup_candidates(name: str, aliases: list[str]) -> list[str]:
    raw: list[str] = [name, *aliases]
    seen: set[str] = set()
    candidates: list[str] = []
    for entry in raw:
        text = _normalize_text(entry)
        if not text:
            continue
        variants = {
            text,
            text.replace("&", "and"),
            text.replace(" and ", " & "),
            _sanitize_name_variant(text),
        }
        for variant in variants:
            normalized = _normalize_text(variant)
            key = normalized.casefold()
            if not normalized or key in seen:
                continue
            seen.add(key)
            candidates.append(normalized)
    return candidates


def _resolve_entity_metadata(
    name: str,
    existing_wikidata_id: str | None,
    aliases: list[str] | None = None,
) -> dict[str, str | None]:
    aliases = aliases or []
    wikidata_id = _normalize_text(existing_wikidata_id).upper() or None
    entity = None

    if wikidata_id and WIKIDATA_ITEM_RE.match(wikidata_id):
        try:
            entity = _fetch_wikidata_entity(wikidata_id)
        except requests.RequestException:
            entity = None

    if entity is None:
        for candidate in _expand_lookup_candidates(name, aliases):
            found_id = _search_wikidata_item(candidate)
            if not found_id:
                continue
            wikidata_id = found_id
            entity = _fetch_wikidata_entity(found_id)
            break

    if not wikidata_id and entity is None:
        return {
            "wikidata_id": None,
            "wikipedia_url": None,
            "wikimedia_logo_file": None,
        }

    if not isinstance(entity, dict):
        return {
            "wikidata_id": wikidata_id,
            "wikipedia_url": None,
            "wikimedia_logo_file": None,
        }

    return {
        "wikidata_id": wikidata_id,
        "wikipedia_url": _extract_enwiki_url(entity),
        "wikimedia_logo_file": _extract_commons_logo_file(entity),
    }


def _collect_added_show_ids(db) -> set[str]:
    show_ids: set[str] = set()
    query = db.schema("admin").table("covered_shows").select("trr_show_id").order("trr_show_id")
    for row in _iter_rows_paged(query):
        value = _normalize_text(row.get("trr_show_id"))
        if value:
            show_ids.add(value)
    return show_ids


def _build_network_inventory(db, *, added_show_ids: set[str]) -> dict[str, InventoryEntity]:
    by_key: dict[str, dict[str, Any]] = {}
    query = db.schema("core").table("shows").select("id,networks").order("id")
    for row in _iter_rows_paged(query):
        show_id = _normalize_text(row.get("id"))
        is_added = bool(show_id and show_id in added_show_ids)
        values = row.get("networks")
        if not isinstance(values, list):
            continue
        for value in values:
            display = _normalize_text(value)
            key = _name_key(display)
            if not key:
                continue
            bucket = by_key.setdefault(
                key,
                {
                    "display_name": display,
                    "show_ids": set(),
                    "added_show_ids": set(),
                },
            )
            bucket["show_ids"].add(show_id)
            if is_added:
                bucket["added_show_ids"].add(show_id)

    inventory: dict[str, InventoryEntity] = {}
    for key, value in by_key.items():
        inventory[key] = InventoryEntity(
            entity_type="network",
            entity_key=key,
            display_name=str(value["display_name"]),
            available_show_count=len(value["show_ids"]),
            added_show_count=len(value["added_show_ids"]),
        )
    return inventory


def _load_provider_names_by_id(db) -> dict[int, str]:
    by_id: dict[int, str] = {}
    query = db.schema("core").table("watch_providers").select("provider_id,provider_name").order("provider_id")
    for row in _iter_rows_paged(query):
        provider_id = row.get("provider_id")
        name = _normalize_text(row.get("provider_name"))
        if isinstance(provider_id, int) and name:
            by_id[provider_id] = name
    return by_id


def _build_provider_primary_rows(db) -> dict[str, dict[str, Any]]:
    by_key: dict[str, dict[str, Any]] = {}
    provider_names_by_id = _load_provider_names_by_id(db)
    query = db.schema("core").table("show_watch_providers").select("show_id,provider_id").order("show_id")
    for row in _iter_rows_paged(query):
        show_id = _normalize_text(row.get("show_id"))
        provider_id = row.get("provider_id")
        if not isinstance(provider_id, int):
            continue
        display = _normalize_text(provider_names_by_id.get(provider_id))
        key = _name_key(display)
        if not key:
            continue
        bucket = by_key.setdefault(
            key,
            {
                "display_name": display,
                "show_ids": set(),
            },
        )
        bucket["show_ids"].add(show_id)
    return by_key


def _build_provider_inventory(db, *, added_show_ids: set[str]) -> dict[str, InventoryEntity]:
    primary = _build_provider_primary_rows(db)
    by_key: dict[str, dict[str, Any]] = {
        key: {
            "display_name": value["display_name"],
            "show_ids": set(value["show_ids"]),
            "added_show_ids": {show_id for show_id in value["show_ids"] if show_id in added_show_ids},
        }
        for key, value in primary.items()
    }

    query = db.schema("core").table("shows").select("id,streaming_providers").order("id")
    for row in _iter_rows_paged(query):
        show_id = _normalize_text(row.get("id"))
        is_added = bool(show_id and show_id in added_show_ids)
        values = row.get("streaming_providers")
        if not isinstance(values, list):
            continue
        for value in values:
            display = _normalize_text(value)
            key = _name_key(display)
            if not key or key in primary:
                continue
            bucket = by_key.setdefault(
                key,
                {
                    "display_name": display,
                    "show_ids": set(),
                    "added_show_ids": set(),
                },
            )
            bucket["show_ids"].add(show_id)
            if is_added:
                bucket["added_show_ids"].add(show_id)

    inventory: dict[str, InventoryEntity] = {}
    for key, value in by_key.items():
        inventory[key] = InventoryEntity(
            entity_type="streaming",
            entity_key=key,
            display_name=str(value["display_name"]),
            available_show_count=len(value["show_ids"]),
            added_show_count=len(value["added_show_ids"]),
        )
    return inventory


def _load_production_names_by_id(db) -> dict[int, str]:
    by_id: dict[int, str] = {}
    query = db.schema("core").table("production_companies").select("id,name").order("id")
    for row in _iter_rows_paged(query):
        production_id = row.get("id")
        name = _normalize_text(row.get("name"))
        if isinstance(production_id, int) and name:
            by_id[production_id] = name
    return by_id


def _extract_tmdb_meta_production_names(row: dict[str, Any]) -> list[str]:
    meta = row.get("tmdb_meta")
    if not isinstance(meta, dict):
        return []
    values = meta.get("production_companies")
    if not isinstance(values, list):
        return []

    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        if not isinstance(value, dict):
            continue
        name = _normalize_text(value.get("name"))
        key = _name_key(name)
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(name)
    return out


def _build_production_inventory(db, *, added_show_ids: set[str]) -> dict[str, InventoryEntity]:
    production_names_by_id = _load_production_names_by_id(db)
    by_key: dict[str, dict[str, Any]] = {}

    query = db.schema("core").table("shows").select("id,tmdb_production_company_ids,tmdb_meta").order("id")
    for row in _iter_rows_paged(query):
        show_id = _normalize_text(row.get("id"))
        if not show_id:
            continue
        is_added = show_id in added_show_ids

        keys_seen_for_show: set[str] = set()
        for production_id in row.get("tmdb_production_company_ids") or []:
            if not isinstance(production_id, int):
                continue
            display = _normalize_text(production_names_by_id.get(production_id))
            key = _name_key(display)
            if not key:
                continue
            keys_seen_for_show.add(key)
            bucket = by_key.setdefault(
                key,
                {
                    "display_name": display,
                    "show_ids": set(),
                    "added_show_ids": set(),
                },
            )
            bucket["show_ids"].add(show_id)
            if is_added:
                bucket["added_show_ids"].add(show_id)

        for display in _extract_tmdb_meta_production_names(row):
            key = _name_key(display)
            if not key or key in keys_seen_for_show:
                continue
            bucket = by_key.setdefault(
                key,
                {
                    "display_name": display,
                    "show_ids": set(),
                    "added_show_ids": set(),
                },
            )
            bucket["show_ids"].add(show_id)
            if is_added:
                bucket["added_show_ids"].add(show_id)

    inventory: dict[str, InventoryEntity] = {}
    for key, value in by_key.items():
        inventory[key] = InventoryEntity(
            entity_type="production",
            entity_key=key,
            display_name=str(value["display_name"]),
            available_show_count=len(value["show_ids"]),
            added_show_count=len(value["added_show_ids"]),
        )
    return inventory


def _load_used_inventory(
    db,
    *,
    include_types: set[str] | None = None,
) -> dict[tuple[str, str], InventoryEntity]:
    added_show_ids = _collect_added_show_ids(db)
    inventory: dict[tuple[str, str], InventoryEntity] = {}
    selected_types = include_types or {"network", "streaming", "production"}

    if "network" in selected_types:
        networks = _build_network_inventory(db, added_show_ids=added_show_ids)
        for key, entity in networks.items():
            inventory[("network", key)] = entity

    if "streaming" in selected_types:
        providers = _build_provider_inventory(db, added_show_ids=added_show_ids)
        for key, entity in providers.items():
            inventory[("streaming", key)] = entity

    if "production" in selected_types:
        productions = _build_production_inventory(db, added_show_ids=added_show_ids)
        for key, entity in productions.items():
            inventory[("production", key)] = entity

    return inventory


def _extract_domain_from_value(value: str | None) -> str | None:
    text = _normalize_text(value)
    if not text:
        return None
    parsed = urlparse(text if "://" in text else f"https://{text}")
    host = _normalize_text(parsed.netloc or parsed.path).lower()
    if host.startswith("www."):
        host = host[4:]
    if not host or "." not in host:
        return None
    return host


def _heuristic_domains_for_name(name: str) -> list[str]:
    slug = re.sub(r"[^a-z0-9]+", "", _name_key(name))
    if not slug:
        return []
    candidates = [f"{slug}.com", f"{slug}tv.com"]
    seen: set[str] = set()
    out: list[str] = []
    for value in candidates:
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _logopedia_title_slug(name: str) -> str:
    return quote(_normalize_text(name).replace(" ", "_"), safe="()_-")


def _collect_tmdb_network_ids_by_key(db) -> dict[str, set[int]]:
    by_key: dict[str, set[int]] = defaultdict(set)
    query = db.schema("core").table("shows").select("networks,tmdb_network_ids").order("id")
    for row in _iter_rows_paged(query):
        networks = row.get("networks")
        ids = [value for value in (row.get("tmdb_network_ids") or []) if isinstance(value, int)]
        if not isinstance(networks, list) or not ids:
            continue
        for value in networks:
            key = _name_key(value)
            if not key:
                continue
            by_key[key].update(ids)
    return dict(by_key)


def _load_provider_imdb_ids_by_provider_id(db, *, max_ids_per_provider: int = 3) -> dict[int, list[str]]:
    by_show_id: dict[str, str] = {}
    show_query = db.schema("core").table("shows").select("id,imdb_id").order("id")
    for row in _iter_rows_paged(show_query):
        show_id = _normalize_text(row.get("id"))
        imdb_id = _normalize_text(row.get("imdb_id"))
        if show_id and imdb_id.startswith("tt"):
            by_show_id[show_id] = imdb_id

    by_provider: dict[int, list[str]] = defaultdict(list)
    query = db.schema("core").table("show_watch_providers").select("show_id,provider_id").order("provider_id")
    for row in _iter_rows_paged(query):
        provider_id = row.get("provider_id")
        show_id = _normalize_text(row.get("show_id"))
        imdb_id = by_show_id.get(show_id)
        if not isinstance(provider_id, int) or not imdb_id:
            continue
        bucket = by_provider[provider_id]
        if imdb_id in bucket:
            continue
        if len(bucket) >= max_ids_per_provider:
            continue
        bucket.append(imdb_id)
    return dict(by_provider)


def _is_imdb_production_category(value: str | None) -> bool:
    label = _normalize_text(value).casefold()
    if not label:
        return True
    return "production" in label


def _load_production_imdb_hints_by_key(
    db,
    *,
    max_requests: int = 200,
    target_entity_keys: set[str] | None = None,
) -> dict[str, dict[str, list[str]]]:
    production_names_by_id = _load_production_names_by_id(db)
    hints: dict[str, dict[str, set[str]]] = {}
    fetched = 0

    query = db.schema("core").table("shows").select("id,imdb_id,tmdb_production_company_ids,tmdb_meta").order("id")
    for row in _iter_rows_paged(query):
        if fetched >= max_requests:
            break

        imdb_id = _normalize_text(row.get("imdb_id"))
        if not imdb_id.startswith("tt"):
            continue

        target_names_by_key: dict[str, set[str]] = {}
        for production_id in row.get("tmdb_production_company_ids") or []:
            if not isinstance(production_id, int):
                continue
            name = _normalize_text(production_names_by_id.get(production_id))
            key = _name_key(name)
            if not key:
                continue
            target_names_by_key.setdefault(key, set()).add(name)

        for name in _extract_tmdb_meta_production_names(row):
            key = _name_key(name)
            if not key:
                continue
            target_names_by_key.setdefault(key, set()).add(name)

        if target_entity_keys:
            target_names_by_key = {
                key: values
                for key, values in target_names_by_key.items()
                if key in target_entity_keys
            }

        if not target_names_by_key:
            continue

        try:
            imdb_result = fetch_imdb_company_credits(imdb_id)
            fetched += 1
        except ImdbCompanyCreditsError:
            continue

        companies = [
            company
            for company in imdb_result.companies
            if _is_imdb_production_category(company.category)
        ] or imdb_result.companies
        if not companies:
            continue

        matched_any = False
        for entity_key, names in target_names_by_key.items():
            compare_keys = {_name_key(name) for name in names if _normalize_text(name)}
            entity_hints = hints.setdefault(
                entity_key,
                {
                    "aliases": set(),
                    "company_urls": set(),
                    "source_urls": set(),
                },
            )
            matched_for_entity = False
            for company in companies:
                company_name = _normalize_text(company.name)
                if not company_name:
                    continue
                if any(_tmdb_name_match(compare_key, company_name) for compare_key in compare_keys):
                    matched_for_entity = True
                    matched_any = True
                    entity_hints["aliases"].add(company_name)
                    if company.company_url:
                        entity_hints["company_urls"].add(company.company_url)
                    entity_hints["source_urls"].add(imdb_result.source_url)

            if not matched_for_entity and len(compare_keys) == 1 and len(companies) == 1:
                company = companies[0]
                company_name = _normalize_text(company.name)
                if company_name:
                    matched_any = True
                    entity_hints["aliases"].add(company_name)
                    if company.company_url:
                        entity_hints["company_urls"].add(company.company_url)
                    entity_hints["source_urls"].add(imdb_result.source_url)

        if not matched_any and len(target_names_by_key) == 1 and len(companies) == 1:
            entity_key = next(iter(target_names_by_key.keys()))
            company = companies[0]
            company_name = _normalize_text(company.name)
            if company_name:
                entity_hints = hints.setdefault(
                    entity_key,
                    {
                        "aliases": set(),
                        "company_urls": set(),
                        "source_urls": set(),
                    },
                )
                entity_hints["aliases"].add(company_name)
                if company.company_url:
                    entity_hints["company_urls"].add(company.company_url)
                entity_hints["source_urls"].add(imdb_result.source_url)

    out: dict[str, dict[str, list[str]]] = {}
    for entity_key, values in hints.items():
        out[entity_key] = {
            "aliases": sorted(values.get("aliases") or set()),
            "company_urls": sorted(values.get("company_urls") or set()),
            "source_urls": sorted(values.get("source_urls") or set()),
        }
    return out


def _build_sync_context(
    db,
    *,
    include_network_hints: bool = True,
    include_streaming_hints: bool = True,
    include_production_hints: bool = True,
    production_entity_keys: set[str] | None = None,
) -> SyncRunContext:
    return SyncRunContext(
        tmdb_api_key=resolve_api_key(),
        tmdb_bearer_token=resolve_bearer_token(),
        tmdb_network_ids_by_key=_collect_tmdb_network_ids_by_key(db) if include_network_hints else {},
        provider_imdb_ids_by_provider_id=_load_provider_imdb_ids_by_provider_id(db) if include_streaming_hints else {},
        production_imdb_hints_by_key=_load_production_imdb_hints_by_key(
            db,
            target_entity_keys=production_entity_keys,
        )
        if include_production_hints
        else {},
        svg_rasterizer_available=svg_rasterizer_available(),
    )


def _load_dimension_lookup(db, *, table: str, id_field: str, name_field: str) -> dict[str, dict[str, Any]]:
    lookup: dict[str, dict[str, Any]] = {}
    query = db.schema("core").table(table).select("*").order(name_field)
    for row in _iter_rows_paged(query):
        name = _normalize_text(row.get(name_field))
        key = _name_key(name)
        if not key or key in lookup:
            continue
        lookup[key] = row
    return lookup


def _parse_logo_override_map(raw: Any) -> dict[str, list[str]]:
    source_map: dict[str, list[str]] = defaultdict(list)
    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, str) and item.strip():
                source_map["override"].append(item.strip())
                continue
            if not isinstance(item, dict):
                continue
            url = _normalize_text(item.get("url"))
            if not url:
                continue
            source = _normalize_text(item.get("source")).lower() or "override"
            source_map[source].append(url)
    elif isinstance(raw, dict):
        for source, value in raw.items():
            source_name = _normalize_text(source).lower() or "override"
            if isinstance(value, str) and value.strip():
                source_map[source_name].append(value.strip())
                continue
            if isinstance(value, list):
                for entry in value:
                    if isinstance(entry, str) and entry.strip():
                        source_map[source_name].append(entry.strip())
    return dict(source_map)


def _load_overrides(db) -> dict[tuple[str, str], OverrideConfig]:
    overrides: dict[tuple[str, str], OverrideConfig] = {}
    query = (
        db.schema("admin")
        .table("network_streaming_overrides")
        .select("*")
        .eq("is_active", True)
        .order("updated_at", desc=True)
    )
    for row in _iter_rows_paged(query):
        entity_type = _normalize_text(row.get("entity_type"))
        entity_key = _name_key(row.get("entity_key"))
        if entity_type not in {"network", "streaming", "production"} or not entity_key:
            continue
        key = (entity_type, entity_key)
        if key in overrides:
            continue
        overrides[key] = OverrideConfig(
            id=_normalize_text(row.get("id")),
            entity_type=entity_type,
            entity_key=entity_key,
            display_name_override=_normalize_text(row.get("display_name_override")) or None,
            wikidata_id_override=_normalize_text(row.get("wikidata_id_override")) or None,
            wikipedia_url_override=_normalize_text(row.get("wikipedia_url_override")) or None,
            aliases_override=_extract_json_list_strings(row.get("aliases_override")),
            source_priority_override=[
                _normalize_text(item).lower()
                for item in _extract_json_list_strings(row.get("source_priority_override"))
                if _normalize_text(item)
            ],
            logo_source_urls_by_source=_parse_logo_override_map(row.get("logo_source_urls_override")),
        )
    return overrides


def _load_unresolved_keys(db, *, used_keys: set[tuple[str, str]]) -> set[tuple[str, str]]:
    unresolved: set[tuple[str, str]] = set()
    seen: set[tuple[str, str]] = set()
    query = (
        db.schema("admin")
        .table("network_streaming_completion")
        .select("entity_type,entity_key,resolution_status")
        .order("updated_at", desc=True)
    )
    for row in _iter_rows_paged(query):
        entity_type = _normalize_text(row.get("entity_type"))
        entity_key = _name_key(row.get("entity_key"))
        pair = (entity_type, entity_key)
        if pair not in used_keys:
            continue
        seen.add(pair)
        if _normalize_text(row.get("resolution_status")) != "resolved":
            unresolved.add(pair)
    unresolved.update(used_keys - seen)
    return unresolved


def _reason_from_exception(exc: Exception) -> str:
    text = _normalize_text(str(exc)).lower()
    if text.startswith("brandfetch_"):
        return text
    if text.startswith("logopedia_"):
        return text
    if "brandfetch_auth_missing" in text:
        return "brandfetch_auth_missing"
    if "brandfetch_not_found" in text:
        return "brandfetch_not_found"
    if "logopedia_no_files" in text:
        return "logopedia_no_files"
    if "imdb_provider_not_found" in text:
        return "imdb_provider_not_found"
    if "logo_decode_failed" in text:
        return "logo_decode_failed"
    if "transparent_extraction_failed" in text:
        return "transparent_extraction_failed"
    if "s3" in text or "upload" in text or "bucket" in text:
        return "s3_upload_failed"
    return "download_failed"


def _update_core_row(db, *, table: str, id_field: str, entity_id: Any, patch: dict[str, Any]) -> None:
    response = db.schema("core").table(table).update(patch).eq(id_field, entity_id).execute()
    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Supabase error updating {table}: {response.error}")


def _load_existing_logo_assets_sha(
    db,
    *,
    entity_type: str,
    entity_key: str,
) -> set[str]:
    out: set[str] = set()
    query = (
        db.schema("admin")
        .table("network_streaming_logo_assets")
        .select("hosted_logo_sha256,mirror_status")
        .eq("entity_type", entity_type)
        .eq("entity_key", entity_key)
    )
    for row in _iter_rows_paged(query):
        if _normalize_text(row.get("mirror_status")) != "mirrored":
            continue
        sha = _normalize_text(row.get("hosted_logo_sha256"))
        if sha:
            out.add(sha)
    return out


def _load_existing_logo_asset_source_urls(
    db,
    *,
    entity_type: str,
    entity_key: str,
) -> dict[str, list[str]]:
    out: dict[str, list[str]] = defaultdict(list)
    query = (
        db.schema("admin")
        .table("network_streaming_logo_assets")
        .select("source,source_url")
        .eq("entity_type", entity_type)
        .eq("entity_key", entity_key)
        .order("source_rank")
    )
    for row in _iter_rows_paged(query):
        source = _normalize_text(row.get("source")).lower()
        source_url = _normalize_text(row.get("source_url"))
        if not source or not source_url:
            continue
        bucket = out.setdefault(source, [])
        if source_url in bucket:
            continue
        bucket.append(source_url)
    return dict(out)


def _load_existing_logo_asset_index(
    db,
    *,
    entity_type: str,
    entity_key: str,
) -> dict[tuple[str, str], dict[str, Any]]:
    out: dict[tuple[str, str], dict[str, Any]] = {}
    query = (
        db.schema("admin")
        .table("network_streaming_logo_assets")
        .select(
            "source,source_url,mirror_status,hosted_logo_key,hosted_logo_url,hosted_logo_sha256,"
            "hosted_logo_content_type,hosted_logo_bytes,hosted_logo_etag,base_logo_format,failure_reason"
        )
        .eq("entity_type", entity_type)
        .eq("entity_key", entity_key)
    )
    for row in _iter_rows_paged(query):
        source = _normalize_text(row.get("source")).lower()
        source_url = _normalize_text(row.get("source_url"))
        if not source or not source_url:
            continue
        out[(source, source_url)] = row
    return out


def _logo_patch_from_asset_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "hosted_logo_key": _normalize_text(row.get("hosted_logo_key")) or None,
        "hosted_logo_url": _normalize_text(row.get("hosted_logo_url")) or None,
        "hosted_logo_sha256": _normalize_text(row.get("hosted_logo_sha256")) or None,
        "hosted_logo_content_type": _normalize_text(row.get("hosted_logo_content_type")) or None,
        "hosted_logo_bytes": row.get("hosted_logo_bytes"),
        "hosted_logo_etag": _normalize_text(row.get("hosted_logo_etag")) or None,
    }


def _source_has_logopedia_urls(urls: list[str]) -> bool:
    for url in urls:
        host = _normalize_text(urlparse(_normalize_text(url)).netloc).lower()
        if host.endswith("logos.fandom.com") or host.endswith("fandom.com") or host.endswith("wikia.nocookie.net"):
            return True
    return False


def _upsert_logo_asset(
    db,
    *,
    row: dict[str, Any],
) -> None:
    payload = dict(row)
    payload["updated_at"] = _now_iso()
    payload.setdefault("created_at", payload["updated_at"])
    response = (
        db.schema("admin")
        .table("network_streaming_logo_assets")
        .upsert(payload, on_conflict="entity_type,entity_key,source,source_url")
        .execute()
    )
    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Supabase error upserting logo asset row: {response.error}")


def _mark_logo_asset_skipped(
    db,
    *,
    entity_type: str,
    entity_key: str,
    source: str,
    source_url: str,
    reason: str,
) -> None:
    response = (
        db.schema("admin")
        .table("network_streaming_logo_assets")
        .update(
            {
                "mirror_status": "skipped",
                "failure_reason": reason,
                "updated_at": _now_iso(),
            }
        )
        .eq("entity_type", entity_type)
        .eq("entity_key", entity_key)
        .eq("source", source)
        .eq("source_url", source_url)
        .execute()
    )
    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Supabase error marking logo asset skipped: {response.error}")


def _superseded_failed_logopedia_svg_urls(
    existing_asset_index: dict[tuple[str, str], dict[str, Any]],
) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    for (source, source_url), row in existing_asset_index.items():
        if source != "catalog":
            continue
        if _normalize_text(row.get("mirror_status")) != "failed":
            continue
        if _normalize_text(row.get("failure_reason")) != "logo_decode_failed":
            continue
        raster_url = _logopedia_svg_raster_variant(source_url)
        if not raster_url:
            continue
        raster_row = existing_asset_index.get((source, raster_url))
        if not raster_row:
            continue
        if _normalize_text(raster_row.get("mirror_status")) != "mirrored":
            continue
        if not _normalize_text(raster_row.get("hosted_logo_url")):
            continue
        out.append((source, source_url))
    return out


def _reset_logo_asset_primary_flags(
    db,
    *,
    entity_type: str,
    entity_key: str,
) -> None:
    response = (
        db.schema("admin")
        .table("network_streaming_logo_assets")
        .update({"is_primary": False, "updated_at": _now_iso()})
        .eq("entity_type", entity_type)
        .eq("entity_key", entity_key)
        .execute()
    )
    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Supabase error resetting logo asset primary flags: {response.error}")


def _mark_logo_asset_primary(
    db,
    *,
    entity_type: str,
    entity_key: str,
    source: str,
    source_url: str,
) -> None:
    response = (
        db.schema("admin")
        .table("network_streaming_logo_assets")
        .update({"is_primary": True, "updated_at": _now_iso()})
        .eq("entity_type", entity_type)
        .eq("entity_key", entity_key)
        .eq("source", source)
        .eq("source_url", source_url)
        .execute()
    )
    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Supabase error marking logo asset primary: {response.error}")


def _upsert_completion(db, row: dict[str, Any]) -> dict[str, Any]:
    payload = dict(row)
    source_priority = payload.get("source_priority")
    if isinstance(source_priority, list):
        payload["source_priority"] = _to_pg_text_array_literal(source_priority)
    response = (
        db.schema("admin")
        .table("network_streaming_completion")
        .upsert(payload, on_conflict="entity_type,entity_key")
        .execute()
    )
    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Supabase error upserting completion row: {response.error}")
    rows = response.data or []
    if isinstance(rows, list) and rows:
        return rows[0]
    return payload


def _insert_attempts(
    db,
    *,
    completion_id: str,
    run_id: str,
    entity_type: str,
    entity_key: str,
    attempts: list[AttemptRecord],
) -> None:
    if not attempts:
        return
    payload = [
        {
            "completion_id": completion_id,
            "run_id": run_id,
            "entity_type": entity_type,
            "entity_key": entity_key,
            "source": _attempt_source_name(attempt.source),
            "attempt_url": attempt.attempt_url,
            "outcome": attempt.outcome,
            "failure_reason": attempt.failure_reason,
            "duration_ms": attempt.duration_ms,
            "details": attempt.details,
        }
        for attempt in attempts
    ]
    response = db.schema("admin").table("network_streaming_completion_attempts").insert(payload).execute()
    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Supabase error inserting completion attempts: {response.error}")


def _source_priority(override: OverrideConfig | None) -> list[str]:
    if override and override.source_priority_override:
        seen: set[str] = set()
        out: list[str] = []
        for source in override.source_priority_override:
            name = _normalize_text(source).lower()
            if not name or name in seen:
                continue
            seen.add(name)
            out.append(name)
        if out:
            return out
    return list(DEFAULT_SOURCE_PRIORITY)


def _merge_source_urls(by_source: dict[str, list[str]], source: str, urls: list[str]) -> None:
    bucket = by_source.setdefault(source, [])
    for url in urls:
        text = _normalize_text(url)
        if not text or text in bucket:
            continue
        bucket.append(text)


def _logopedia_svg_raster_variant(url: str) -> str | None:
    text = _normalize_text(url)
    lower = text.lower()
    if "static.wikia.nocookie.net" not in lower:
        return None
    if ".svg/revision/latest" not in lower:
        return None
    if "/scale-to-width-down/" in lower:
        return None
    if "?" in text:
        base, query = text.split("?", 1)
        return f"{base}/scale-to-width-down/1024?{query}"
    return f"{text}/scale-to-width-down/1024"


def _expand_candidate_urls(source: str, urls: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    source_name = _normalize_text(source).lower()
    for value in urls:
        text = _normalize_text(value)
        if not text:
            continue
        if text not in seen:
            seen.add(text)
            out.append(text)
        if source_name == "catalog":
            raster = _logopedia_svg_raster_variant(text)
            if raster and raster not in seen:
                seen.add(raster)
                out.append(raster)
    return out


def _tmdb_name_match(name_key: str, candidate: str) -> bool:
    candidate_key = _name_key(candidate)
    if not candidate_key:
        return False
    if candidate_key == name_key:
        return True
    # handle punctuation/ampersand variants
    variant = _name_key(_sanitize_name_variant(candidate))
    return bool(variant and variant == name_key)


def _resolve_tmdb_network_hints(
    *,
    entity: InventoryEntity,
    core_row: dict[str, Any],
    context: SyncRunContext | None,
    aliases: list[str],
) -> dict[str, Any]:
    if entity.entity_type != "network" or context is None:
        return {"aliases": [], "official_urls": [], "tmdb_logo_urls": []}
    if not (context.tmdb_api_key or context.tmdb_bearer_token):
        return {"aliases": [], "official_urls": [], "tmdb_logo_urls": []}

    candidate_ids: set[int] = set()
    row_id = core_row.get("id")
    if isinstance(row_id, int):
        candidate_ids.add(row_id)
    candidate_ids.update(context.tmdb_network_ids_by_key.get(entity.entity_key, set()))

    aliases_out: list[str] = []
    official_urls: list[str] = []
    tmdb_logo_urls: list[str] = []
    seen_aliases: set[str] = set()
    seen_urls: set[str] = set()

    for network_id in sorted(candidate_ids):
        cached = context.tmdb_network_hints_by_id.get(network_id)
        if cached is None:
            try:
                details = fetch_network_details(
                    network_id,
                    api_key=context.tmdb_api_key,
                    bearer_token=context.tmdb_bearer_token,
                )
                alt = fetch_network_alternative_names(
                    network_id,
                    api_key=context.tmdb_api_key,
                    bearer_token=context.tmdb_bearer_token,
                )
                names: list[str] = []
                details_name = _normalize_text(details.get("name"))
                if details_name:
                    names.append(details_name)
                for alias_row in alt.get("results") or []:
                    if not isinstance(alias_row, dict):
                        continue
                    alias_name = _normalize_text(alias_row.get("name"))
                    if alias_name:
                        names.append(alias_name)
                homepage = _normalize_text(details.get("homepage"))
                logo_path = _normalize_text(details.get("logo_path"))
                logo_url = ""
                if logo_path:
                    if logo_path.startswith("http://") or logo_path.startswith("https://"):
                        logo_url = logo_path
                    else:
                        normalized_logo_path = logo_path if logo_path.startswith("/") else f"/{logo_path}"
                        logo_url = f"https://image.tmdb.org/t/p/original{normalized_logo_path}"
                cached = {
                    "names": names,
                    "homepage": homepage,
                    "logo_url": logo_url,
                }
            except (TmdbClientError, RuntimeError, requests.RequestException):
                cached = {"names": [], "homepage": "", "logo_url": ""}
            context.tmdb_network_hints_by_id[network_id] = cached

        names = [value for value in cached.get("names") or [] if isinstance(value, str)]
        matches = any(_tmdb_name_match(entity.entity_key, name) for name in names)
        if not matches and aliases:
            alias_keys = {_name_key(alias) for alias in aliases if _normalize_text(alias)}
            matches = any(_name_key(name) in alias_keys for name in names)
        if not matches:
            continue

        for name in names:
            clean = _normalize_text(name)
            if not clean:
                continue
            key = clean.casefold()
            if key in seen_aliases:
                continue
            seen_aliases.add(key)
            aliases_out.append(clean)

        homepage = _normalize_text(cached.get("homepage"))
        if homepage and homepage not in seen_urls:
            seen_urls.add(homepage)
            official_urls.append(homepage)
        logo_url = _normalize_text(cached.get("logo_url"))
        if logo_url and logo_url not in seen_urls:
            seen_urls.add(logo_url)
            tmdb_logo_urls.append(logo_url)

    return {
        "aliases": aliases_out,
        "official_urls": official_urls,
        "tmdb_logo_urls": tmdb_logo_urls,
    }


def _extract_provider_logo_candidates_from_watch_box(
    payload: dict[str, Any],
    *,
    entity_key: str,
    display_name: str,
) -> list[str]:
    candidates: list[str] = []
    seen: set[str] = set()
    target = entity_key.casefold()
    target_name = display_name.casefold()

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            provider = node.get("provider")
            provider_id = ""
            provider_ref = ""
            logo_url = ""
            if isinstance(provider, dict):
                provider_id = _normalize_text(provider.get("id")).casefold()
                provider_ref = _normalize_text(provider.get("refTagFragment")).casefold()
                logos = provider.get("logos")
                if isinstance(logos, dict):
                    slate = logos.get("slate")
                    if isinstance(slate, dict):
                        logo_url = _normalize_text(slate.get("url"))
            title_text = ""
            title_node = node.get("title")
            if isinstance(title_node, dict):
                title_text = _normalize_text(title_node.get("value")).casefold()
            link_text = _normalize_text(node.get("link")).casefold()

            matches = any(
                [
                    bool(target and target in provider_id),
                    bool(target and target in provider_ref),
                    bool(target_name and target_name in title_text),
                    bool(target and target in link_text),
                ]
            )
            if matches and logo_url and logo_url not in seen:
                seen.add(logo_url)
                candidates.append(logo_url)

            for value in node.values():
                walk(value)
            return
        if isinstance(node, list):
            for item in node:
                walk(item)

    walk(payload)
    return candidates


def _collect_external_logo_candidates(
    *,
    entity: InventoryEntity,
    display_name: str,
    core_row: dict[str, Any],
    override: OverrideConfig | None,
    context: SyncRunContext | None,
    tmdb_hints: dict[str, Any],
    allow_brandfetch_lookup: bool,
    allow_logopedia_lookup: bool,
    allow_imdb_lookup: bool,
) -> tuple[dict[str, list[str]], list[AttemptRecord], str | None]:
    by_source: dict[str, list[str]] = defaultdict(list)
    attempts: list[AttemptRecord] = []
    unresolved_reason: str | None = None
    imdb_company_hints: dict[str, list[str]] = {"aliases": [], "company_urls": [], "source_urls": []}
    if entity.entity_type == "production" and context is not None:
        imdb_company_hints = context.production_imdb_hints_by_key.get(entity.entity_key, imdb_company_hints)

    # Brandfetch candidate domains
    domains: list[str] = []
    hint_urls = (tmdb_hints or {}).get("official_urls") or []
    homepage = _normalize_text(hint_urls[0] if hint_urls else "")
    homepage_domain = normalize_domain(homepage)
    if homepage_domain:
        domains.append(homepage_domain)

    if override:
        for urls in override.logo_source_urls_by_source.values():
            for url in urls:
                domain = normalize_domain(url)
                if domain:
                    domains.append(domain)

    for url in imdb_company_hints.get("company_urls", []):
        domain = normalize_domain(url)
        if domain:
            domains.append(domain)

    domains.extend(_heuristic_domains_for_name(display_name))
    domain_seen: set[str] = set()
    ordered_domains: list[str] = []
    for domain in domains:
        if not domain or domain in domain_seen:
            continue
        domain_seen.add(domain)
        ordered_domains.append(domain)

    if allow_brandfetch_lookup:
        for domain in ordered_domains:
            started = time.perf_counter()
            try:
                urls = fetch_brandfetch_logo_candidates(domain)
                _merge_source_urls(by_source, "official", urls[:8])
                attempts.append(
                    AttemptRecord(
                        source="official",
                        attempt_url=f"https://{domain}",
                        outcome="success",
                        failure_reason=None,
                        duration_ms=int((time.perf_counter() - started) * 1000),
                        details={"provider": "brandfetch", "candidate_count": len(urls)},
                    )
                )
                if urls:
                    break
            except BrandfetchAuthError:
                reason = "brandfetch_auth_missing"
                unresolved_reason = unresolved_reason or reason
                attempts.append(
                    AttemptRecord(
                        source="official",
                        attempt_url=f"https://{domain}",
                        outcome="failed",
                        failure_reason=reason,
                        duration_ms=int((time.perf_counter() - started) * 1000),
                        details={"provider": "brandfetch"},
                    )
                )
                break
            except BrandfetchNotFoundError:
                reason = "brandfetch_not_found"
                unresolved_reason = unresolved_reason or reason
                attempts.append(
                    AttemptRecord(
                        source="official",
                        attempt_url=f"https://{domain}",
                        outcome="failed",
                        failure_reason=reason,
                        duration_ms=int((time.perf_counter() - started) * 1000),
                        details={"provider": "brandfetch"},
                    )
                )
            except BrandfetchRequestError as exc:
                reason = _reason_from_exception(exc)
                unresolved_reason = unresolved_reason or reason
                attempts.append(
                    AttemptRecord(
                        source="official",
                        attempt_url=f"https://{domain}",
                        outcome="failed",
                        failure_reason=reason,
                        duration_ms=int((time.perf_counter() - started) * 1000),
                        details={"provider": "brandfetch", "error": str(exc)},
                    )
                )
    else:
        attempts.append(
            AttemptRecord(
                source="official",
                attempt_url=None,
                outcome="skipped",
                failure_reason="cached_discovery_reused",
                duration_ms=0,
                details={"provider": "brandfetch"},
            )
        )

    aliases = [
        *([alias for alias in (tmdb_hints or {}).get("aliases") or [] if isinstance(alias, str)]),
        *([alias for alias in imdb_company_hints.get("aliases", []) if isinstance(alias, str)]),
    ]
    if allow_logopedia_lookup:
        try:
            logopedia_urls = fetch_logopedia_logo_candidates(display_name, aliases=aliases)
            _merge_source_urls(by_source, "catalog", logopedia_urls[:12])
            attempts.append(
                AttemptRecord(
                    source="catalog",
                    attempt_url=f"https://logos.fandom.com/wiki/{_logopedia_title_slug(display_name)}",
                    outcome="success",
                    failure_reason=None,
                    duration_ms=0,
                    details={"provider": "logopedia", "candidate_count": len(logopedia_urls)},
                )
            )
        except LogopediaNoFilesError:
            unresolved_reason = unresolved_reason or "logopedia_no_files"
            attempts.append(
                AttemptRecord(
                    source="catalog",
                    attempt_url=f"https://logos.fandom.com/wiki/{_logopedia_title_slug(display_name)}",
                    outcome="failed",
                    failure_reason="logopedia_no_files",
                    duration_ms=0,
                    details={"provider": "logopedia"},
                )
            )
        except LogopediaRequestError as exc:
            reason = _reason_from_exception(exc)
            unresolved_reason = unresolved_reason or reason
            attempts.append(
                AttemptRecord(
                    source="catalog",
                    attempt_url=f"https://logos.fandom.com/wiki/{_logopedia_title_slug(display_name)}",
                    outcome="failed",
                    failure_reason=reason,
                    duration_ms=0,
                    details={"provider": "logopedia", "error": str(exc)},
                )
            )
    else:
        attempts.append(
            AttemptRecord(
                source="catalog",
                attempt_url=f"https://logos.fandom.com/wiki/{_logopedia_title_slug(display_name)}",
                outcome="skipped",
                failure_reason="cached_discovery_reused",
                duration_ms=0,
                details={"provider": "logopedia"},
            )
        )

    if entity.entity_type == "production":
        imdb_source_urls = [url for url in imdb_company_hints.get("source_urls", []) if _normalize_text(url)]
        if imdb_source_urls or imdb_company_hints.get("aliases") or imdb_company_hints.get("company_urls"):
            attempts.append(
                AttemptRecord(
                    source="imdb",
                    attempt_url=imdb_source_urls[0] if imdb_source_urls else "https://www.imdb.com/",
                    outcome="success",
                    failure_reason=None,
                    duration_ms=0,
                    details={
                        "provider": "imdb_companycredits",
                        "source_url_count": len(imdb_source_urls),
                        "alias_count": len(imdb_company_hints.get("aliases", [])),
                        "company_url_count": len(imdb_company_hints.get("company_urls", [])),
                    },
                )
            )
        else:
            attempts.append(
                AttemptRecord(
                    source="imdb",
                    attempt_url="https://www.imdb.com/",
                    outcome="failed",
                    failure_reason="imdb_companycredits_not_found",
                    duration_ms=0,
                    details={"provider": "imdb_companycredits"},
                )
            )

    # IMDb fallback for streaming providers.
    if entity.entity_type == "streaming" and context is not None and allow_imdb_lookup:
        provider_id = core_row.get("provider_id")
        imdb_ids = context.provider_imdb_ids_by_provider_id.get(provider_id, []) if isinstance(provider_id, int) else []
        imdb_candidates: list[str] = []
        for imdb_id in imdb_ids:
            if imdb_id not in context.imdb_watch_box_cache:
                try:
                    payload = fetch_hero_watch_box(imdb_id)
                    context.imdb_watch_box_cache[imdb_id] = payload if isinstance(payload, dict) else {}
                except Exception:  # noqa: BLE001
                    context.imdb_watch_box_cache[imdb_id] = {}
            payload = context.imdb_watch_box_cache.get(imdb_id) or {}
            urls = _extract_provider_logo_candidates_from_watch_box(
                payload,
                entity_key=entity.entity_key,
                display_name=display_name,
            )
            imdb_candidates.extend(urls)
            if urls:
                break

        if imdb_candidates:
            _merge_source_urls(by_source, "imdb", imdb_candidates)
            attempts.append(
                AttemptRecord(
                    source="imdb",
                    attempt_url="https://www.imdb.com/",
                    outcome="success",
                    failure_reason=None,
                    duration_ms=0,
                    details={"provider": "imdb_watch_box", "candidate_count": len(imdb_candidates)},
                )
            )
        else:
            unresolved_reason = unresolved_reason or "imdb_provider_not_found"
            attempts.append(
                AttemptRecord(
                    source="imdb",
                    attempt_url="https://www.imdb.com/",
                    outcome="failed",
                    failure_reason="imdb_provider_not_found",
                    duration_ms=0,
                    details={"provider": "imdb_watch_box"},
                )
            )
    elif entity.entity_type == "streaming" and context is not None:
        attempts.append(
            AttemptRecord(
                source="imdb",
                attempt_url="https://www.imdb.com/",
                outcome="skipped",
                failure_reason="cached_discovery_reused",
                duration_ms=0,
                details={"provider": "imdb_watch_box"},
            )
        )

    return dict(by_source), attempts, unresolved_reason


def _tmdb_logo_candidate(row: dict[str, Any]) -> list[str]:
    path = _normalize_text(row.get("tmdb_logo_path"))
    if not path:
        return []
    if path.startswith("http://") or path.startswith("https://"):
        return [path]
    if not path.startswith("/"):
        path = "/" + path
    return [f"https://image.tmdb.org/t/p/original{path}"]


def _detect_base_logo_format(
    *,
    wikimedia_logo_file: str,
    logo_source_url: str,
    hosted_logo_url: str,
) -> str:
    values = [wikimedia_logo_file, logo_source_url, hosted_logo_url]
    for value in values:
        lowered = _normalize_text(value).lower()
        if not lowered:
            continue
        if ".svg" in lowered:
            return "svg"
        if ".png" in lowered or lowered.endswith(".jpg") or lowered.endswith(".jpeg") or lowered.endswith(".webp"):
            return "png"
    return "unknown"


def _build_logo_candidates(
    *,
    override: OverrideConfig | None,
    core_row: dict[str, Any] | None,
    wikimedia_logo_file: str,
    extra_by_source: dict[str, list[str]] | None = None,
    extra_tmdb_logo_urls: list[str] | None = None,
) -> dict[str, list[str]]:
    by_source: dict[str, list[str]] = defaultdict(list)

    if override:
        for source, urls in override.logo_source_urls_by_source.items():
            _merge_source_urls(by_source, source, urls)

    if extra_by_source:
        for source, urls in extra_by_source.items():
            _merge_source_urls(by_source, source, urls)

    row = core_row or {}
    _merge_source_urls(by_source, "tmdb", _tmdb_logo_candidate(row))
    if extra_tmdb_logo_urls:
        _merge_source_urls(by_source, "tmdb", extra_tmdb_logo_urls)

    _merge_source_urls(by_source, "wikimedia", _commons_file_urls(wikimedia_logo_file))

    return dict(by_source)


def _capped_candidates(
    candidates: dict[str, list[str]],
) -> dict[str, list[str]]:
    capped: dict[str, list[str]] = {}
    for source, urls in (candidates or {}).items():
        limit = LOGO_SOURCE_CAPS.get(source, 8)
        seen: set[str] = set()
        bucket: list[str] = []
        for value in _expand_candidate_urls(source, urls):
            text = _normalize_text(value)
            if not text or text in seen:
                continue
            seen.add(text)
            bucket.append(text)
            if len(bucket) >= limit:
                break
        if bucket:
            capped[source] = bucket
    return capped


def _attempt_source_name(source: str) -> str:
    text = _normalize_text(source).lower()
    if text in ATTEMPT_TABLE_SOURCES:
        return text
    if text == "imdb":
        return "catalog"
    return "catalog"


def _logo_asset_source_name(source: str) -> str:
    text = _normalize_text(source).lower()
    if text in LOGO_ASSET_TABLE_SOURCES:
        return text
    if text == "variant":
        return "catalog"
    return "catalog"


def _ordered_candidate_sources(
    candidates: dict[str, list[str]],
    source_priority: list[str],
) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for source in source_priority:
        text = _normalize_text(source).lower()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    for source in sorted(candidates.keys()):
        text = _normalize_text(source).lower()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def _build_resolution_status(
    *,
    wikidata_id: str,
    wikipedia_url: str,
    hosted_logo_url: str,
    hosted_logo_black_url: str,
    hosted_logo_white_url: str,
    base_logo_format: str,
    reason: str | None,
) -> tuple[str, str | None]:
    complete = all(
        [
            bool(wikidata_id),
            bool(wikipedia_url),
            bool(hosted_logo_url),
            bool(hosted_logo_black_url),
            bool(hosted_logo_white_url),
            base_logo_format in {"png", "svg"},
        ]
    )
    if complete:
        return "resolved", None

    if reason in {
        "download_failed",
        "logo_decode_failed",
        "transparent_extraction_failed",
        "s3_upload_failed",
        "missing_dimension_row",
    }:
        return "failed", reason

    return "manual_required", reason or "incomplete_metadata"


def _process_entity(
    db,
    *,
    entity: InventoryEntity,
    core_row: dict[str, Any] | None,
    override: OverrideConfig | None,
    run_id: str,
    args: argparse.Namespace,
    summary: SyncSummary,
    s3_client,
    context: SyncRunContext | None = None,
) -> None:
    summary.processed += 1
    attempts: list[AttemptRecord] = []

    if entity.entity_type == "network":
        row_table = "networks"
        id_field = "id"
        logo_kind = "networks"
    elif entity.entity_type == "streaming":
        row_table = "watch_providers"
        id_field = "provider_id"
        logo_kind = "watch-providers"
    else:
        row_table = "production_companies"
        id_field = "id"
        logo_kind = "production-companies"
    row_type = entity.entity_type

    core_row = dict(core_row or {})
    entity_id_value = _normalize_text(core_row.get(id_field)) or ""
    display_name = (
        override.display_name_override if override and override.display_name_override else entity.display_name
    )

    patch: dict[str, Any] = {}
    unresolved_reason: str | None = None

    try:
        override_aliases = override.aliases_override if override else []
        generated_aliases = _derive_metadata_aliases(entity.entity_type, display_name)
        tmdb_hints = _resolve_tmdb_network_hints(
            entity=entity,
            core_row=core_row,
            context=context,
            aliases=[*override_aliases, *generated_aliases],
        )
        aliases = [
            *override_aliases,
            *generated_aliases,
            *[alias for alias in (tmdb_hints.get("aliases") or []) if isinstance(alias, str)],
        ]
        metadata = _resolve_entity_metadata(display_name, core_row.get("wikidata_id"), aliases)

        wikidata_id = _normalize_text(override.wikidata_id_override if override else "") or _normalize_text(
            metadata.get("wikidata_id")
        )
        wikipedia_url = _normalize_text(override.wikipedia_url_override if override else "") or _normalize_text(
            metadata.get("wikipedia_url")
        )
        wikimedia_logo_file = _normalize_text(metadata.get("wikimedia_logo_file"))

        existing_wikidata = _normalize_text(core_row.get("wikidata_id"))
        existing_wikipedia = _normalize_text(core_row.get("wikipedia_url"))
        existing_logo_file = _normalize_text(core_row.get("wikimedia_logo_file"))

        if wikidata_id and (args.force or not existing_wikidata):
            if wikidata_id != existing_wikidata:
                patch["wikidata_id"] = wikidata_id
            if not existing_wikidata:
                summary.wikidata_linked += 1

        if wikipedia_url and (args.force or not existing_wikipedia):
            if wikipedia_url != existing_wikipedia:
                patch["wikipedia_url"] = wikipedia_url
            if not existing_wikipedia:
                summary.wikipedia_linked += 1

        if wikimedia_logo_file and (args.force or not existing_logo_file):
            if wikimedia_logo_file != existing_logo_file:
                patch["wikimedia_logo_file"] = wikimedia_logo_file

        if patch:
            patch["link_enriched_at"] = _now_iso()
            patch["link_enrichment_source"] = "wikidata"
            summary.links_enriched += 1

        merged_row = {**core_row, **patch}
        has_base_logo = bool(_normalize_text(merged_row.get("hosted_logo_url")))
        existing_asset_source_urls: dict[str, list[str]] = {}
        discovery_state: dict[str, dict[str, Any]] = {}
        if not args.skip_s3 and core_row:
            existing_asset_source_urls = _load_existing_logo_asset_source_urls(
                db,
                entity_type=entity.entity_type,
                entity_key=entity.entity_key,
            )
            discovery_state = _load_discovery_state(
                db,
                entity_type=entity.entity_type,
                entity_key=entity.entity_key,
            )

        external_candidates: dict[str, list[str]] = {}
        for source_name, urls in existing_asset_source_urls.items():
            if source_name in {"official", "catalog", "imdb"}:
                _merge_source_urls(external_candidates, source_name, urls)

        has_cached_official = bool(existing_asset_source_urls.get("official"))
        has_cached_logopedia = _source_has_logopedia_urls(existing_asset_source_urls.get("catalog", []))
        has_cached_imdb = bool(existing_asset_source_urls.get("imdb"))
        official_locked = _is_discovery_locked(discovery_state.get("official"))
        catalog_locked = _is_discovery_locked(discovery_state.get("catalog"))
        imdb_locked = _is_discovery_locked(discovery_state.get("imdb"))

        allow_brandfetch_lookup = bool(
            args.refresh_external_sources or (not official_locked and not has_cached_official)
        )
        allow_logopedia_lookup = bool(
            args.refresh_external_sources or (not catalog_locked and not has_cached_logopedia)
        )
        allow_imdb_lookup = bool(args.refresh_external_sources or (not imdb_locked and not has_cached_imdb))
        should_collect_external = bool(
            not args.skip_s3
            and not args.dry_run
            and (
                allow_brandfetch_lookup
                or allow_logopedia_lookup
                or (entity.entity_type == "streaming" and allow_imdb_lookup)
                or (entity.entity_type == "production" and allow_imdb_lookup)
            )
        )

        external_attempts: list[AttemptRecord] = []
        external_reason: str | None = None
        if should_collect_external:
            external_new_candidates, external_attempts, external_reason = _collect_external_logo_candidates(
                entity=entity,
                display_name=display_name,
                core_row=merged_row,
                override=override,
                context=context,
                tmdb_hints=tmdb_hints,
                allow_brandfetch_lookup=allow_brandfetch_lookup,
                allow_logopedia_lookup=allow_logopedia_lookup,
                allow_imdb_lookup=allow_imdb_lookup,
            )
            for source_name, urls in external_new_candidates.items():
                _merge_source_urls(external_candidates, source_name, urls)
            attempts.extend(external_attempts)
            if core_row:
                for discovery_source in DISCOVERY_SOURCES:
                    source_attempts = [item for item in external_attempts if item.source == discovery_source]
                    if not source_attempts:
                        continue
                    outcomes = {item.outcome for item in source_attempts}
                    if "success" in outcomes:
                        outcome = "success"
                    elif "failed" in outcomes:
                        outcome = "failed"
                    else:
                        outcome = "skipped"
                    reason = next(
                        (
                            _normalize_text(item.failure_reason) or None
                            for item in source_attempts
                            if _normalize_text(item.failure_reason)
                        ),
                        None,
                    )
                    candidate_count = len(external_new_candidates.get(discovery_source, []))
                    _upsert_discovery_state(
                        db,
                        entity_type=entity.entity_type,
                        entity_key=entity.entity_key,
                        source=discovery_source,
                        outcome=outcome,
                        reason=reason,
                        cached_candidate_count=candidate_count,
                        previous_row=discovery_state.get(discovery_source),
                    )
        elif not args.skip_s3:
            official_skip_reason = (
                "discovery_locked"
                if official_locked and not args.refresh_external_sources
                else "cached_discovery_reused"
            )
            catalog_skip_reason = (
                "discovery_locked"
                if catalog_locked and not args.refresh_external_sources
                else "cached_discovery_reused"
            )
            imdb_skip_reason = (
                "discovery_locked"
                if imdb_locked and not args.refresh_external_sources
                else "cached_discovery_reused"
            )
            attempts.append(
                AttemptRecord(
                    source="official",
                    attempt_url=None,
                    outcome="skipped",
                    failure_reason=official_skip_reason,
                    duration_ms=0,
                    details={"provider": "brandfetch"},
                )
            )
            attempts.append(
                AttemptRecord(
                    source="catalog",
                    attempt_url=None,
                    outcome="skipped",
                    failure_reason=catalog_skip_reason,
                    duration_ms=0,
                    details={"provider": "logopedia"},
                )
            )
            if entity.entity_type in {"streaming", "production"}:
                attempts.append(
                    AttemptRecord(
                        source="imdb",
                        attempt_url=None,
                        outcome="skipped",
                        failure_reason=imdb_skip_reason,
                        duration_ms=0,
                        details={"provider": "imdb"},
                    )
                )

        source_priority = _source_priority(override)
        candidates = _capped_candidates(
            _build_logo_candidates(
                override=override,
                core_row=merged_row,
                wikimedia_logo_file=_normalize_text(merged_row.get("wikimedia_logo_file")) or wikimedia_logo_file,
                extra_by_source=external_candidates,
                extra_tmdb_logo_urls=[url for url in (tmdb_hints.get("tmdb_logo_urls") or []) if isinstance(url, str)],
            )
        )
        if (
            not args.skip_s3
            and not args.dry_run
            and context is not None
            and not context.svg_rasterizer_available
            and _candidates_contain_svg(candidates)
        ):
            raise FatalSyncError(
                "svg_rasterizer_unavailable: cairosvg is required "
                "for SVG logo candidates in sync_networks_streaming_links"
            )

        selected_logo_source = ""
        selected_logo_url = ""
        selected_logo_patch: dict[str, Any] = {}

        if not core_row:
            unresolved_reason = "missing_dimension_row"

        if not args.skip_s3 and not args.dry_run and core_row:
            existing_sha = _load_existing_logo_assets_sha(
                db,
                entity_type=entity.entity_type,
                entity_key=entity.entity_key,
            )
            existing_asset_index = _load_existing_logo_asset_index(
                db,
                entity_type=entity.entity_type,
                entity_key=entity.entity_key,
            )
            seen_urls: set[str] = set()
            successful_by_source: dict[str, tuple[str, dict[str, Any]]] = {}
            mirrored_urls_this_run: set[tuple[str, str]] = set()

            for source_name in _ordered_candidate_sources(candidates, source_priority):
                urls = candidates.get(source_name, [])
                if not urls:
                    if source_name in {"official", "catalog", "override", "tmdb", "wikimedia"}:
                        attempts.append(
                            AttemptRecord(
                                source=_attempt_source_name(source_name),
                                attempt_url=None,
                                outcome="skipped",
                                failure_reason="no_candidate_url",
                                duration_ms=0,
                            )
                        )
                    continue
                for source_rank, candidate_url in enumerate(urls, start=1):
                    summary.logo_assets_discovered += 1

                    mirror_status = "failed"
                    failure_reason: str | None = None
                    logo_patch: dict[str, Any] = {}

                    if candidate_url in seen_urls:
                        mirror_status = "skipped"
                        failure_reason = "duplicate_url"
                        summary.logo_assets_skipped += 1
                        attempts.append(
                            AttemptRecord(
                                source=_attempt_source_name(source_name),
                                attempt_url=candidate_url,
                                outcome="skipped",
                                failure_reason=failure_reason,
                                duration_ms=0,
                            )
                        )
                        duplicate_url_asset_row = {
                            "entity_type": entity.entity_type,
                            "entity_key": entity.entity_key,
                            "entity_id": entity_id_value or None,
                            "display_name": display_name,
                            "source": _logo_asset_source_name(source_name),
                            "source_url": candidate_url,
                            "source_rank": source_rank,
                            "run_id": run_id,
                            "base_logo_format": _detect_base_logo_format(
                                wikimedia_logo_file="",
                                logo_source_url=candidate_url,
                                hosted_logo_url="",
                            ),
                            "mirror_status": mirror_status,
                            "failure_reason": failure_reason,
                            "is_primary": False,
                        }
                        _upsert_logo_asset(db, row=duplicate_url_asset_row)
                        duplicate_key = (duplicate_url_asset_row["source"], candidate_url)
                        existing_asset_index[duplicate_key] = duplicate_url_asset_row
                        continue

                    seen_urls.add(candidate_url)
                    source_key = _logo_asset_source_name(source_name)
                    cached_asset_row = existing_asset_index.get((source_key, candidate_url))
                    if cached_asset_row and not args.force:
                        cached_status = _normalize_text(cached_asset_row.get("mirror_status"))
                        cached_logo_patch = _logo_patch_from_asset_row(cached_asset_row)
                        cached_hosted_url = _normalize_text(cached_logo_patch.get("hosted_logo_url"))
                        if cached_status == "mirrored" and cached_hosted_url:
                            summary.logo_assets_skipped += 1
                            attempts.append(
                                AttemptRecord(
                                    source=_attempt_source_name(source_name),
                                    attempt_url=candidate_url,
                                    outcome="skipped",
                                    failure_reason="already_mirrored",
                                    duration_ms=0,
                                )
                            )
                            cached_sha = _normalize_text(cached_logo_patch.get("hosted_logo_sha256"))
                            if cached_sha:
                                existing_sha.add(cached_sha)
                            if source_name not in successful_by_source:
                                successful_by_source[source_name] = (candidate_url, cached_logo_patch)
                            cached_asset_update = {
                                "entity_type": entity.entity_type,
                                "entity_key": entity.entity_key,
                                "entity_id": entity_id_value or None,
                                "display_name": display_name,
                                "source": source_key,
                                "source_url": candidate_url,
                                "source_rank": source_rank,
                                "run_id": run_id,
                                "hosted_logo_key": _normalize_text(cached_logo_patch.get("hosted_logo_key")) or None,
                                "hosted_logo_url": cached_hosted_url or None,
                                "hosted_logo_sha256": _normalize_text(cached_logo_patch.get("hosted_logo_sha256"))
                                or None,
                                "hosted_logo_content_type": (
                                    _normalize_text(cached_logo_patch.get("hosted_logo_content_type")) or None
                                ),
                                "hosted_logo_bytes": cached_logo_patch.get("hosted_logo_bytes"),
                                "hosted_logo_etag": _normalize_text(cached_logo_patch.get("hosted_logo_etag")) or None,
                                "base_logo_format": _normalize_text(cached_asset_row.get("base_logo_format"))
                                or _detect_base_logo_format(
                                    wikimedia_logo_file="",
                                    logo_source_url=candidate_url,
                                    hosted_logo_url=cached_hosted_url,
                                ),
                                "pixel_width": None,
                                "pixel_height": None,
                                "mirror_status": "mirrored",
                                "failure_reason": None,
                                "is_primary": False,
                            }
                            _upsert_logo_asset(db, row=cached_asset_update)
                            existing_asset_index[(source_key, candidate_url)] = cached_asset_update
                            continue

                    started = time.perf_counter()
                    try:
                        logo_patch = (
                            mirror_external_logo_row(
                                {
                                    id_field: core_row.get(id_field),
                                    "hosted_logo_url": None,
                                    "hosted_logo_key": None,
                                    "hosted_logo_sha256": None,
                                },
                                kind=logo_kind,
                                id_field=id_field,
                                source_url=candidate_url,
                                force=False,
                                s3_client=s3_client,
                                source=_attempt_source_name(source_name),
                            )
                            or {}
                        )
                        duration_ms = int((time.perf_counter() - started) * 1000)
                        hosted_sha = _normalize_text(logo_patch.get("hosted_logo_sha256"))
                        if hosted_sha and hosted_sha in existing_sha:
                            mirror_status = "skipped"
                            failure_reason = "duplicate_sha"
                            summary.logo_assets_skipped += 1
                            attempts.append(
                                AttemptRecord(
                                    source=_attempt_source_name(source_name),
                                    attempt_url=candidate_url,
                                    outcome="skipped",
                                    failure_reason=failure_reason,
                                    duration_ms=duration_ms,
                                )
                            )
                        elif logo_patch:
                            mirror_status = "mirrored"
                            summary.logo_assets_mirrored += 1
                            mirrored_urls_this_run.add((_logo_asset_source_name(source_name), candidate_url))
                            if hosted_sha:
                                existing_sha.add(hosted_sha)
                            attempts.append(
                                AttemptRecord(
                                    source=_attempt_source_name(source_name),
                                    attempt_url=candidate_url,
                                    outcome="success",
                                    failure_reason=None,
                                    duration_ms=duration_ms,
                                )
                            )
                            if source_name not in successful_by_source:
                                successful_by_source[source_name] = (candidate_url, logo_patch)
                        else:
                            mirror_status = "failed"
                            failure_reason = "logo_decode_failed"
                            summary.logo_assets_failed += 1
                            attempts.append(
                                AttemptRecord(
                                    source=_attempt_source_name(source_name),
                                    attempt_url=candidate_url,
                                    outcome="failed",
                                    failure_reason=failure_reason,
                                    duration_ms=duration_ms,
                                )
                            )
                    except Exception as exc:  # noqa: BLE001
                        duration_ms = int((time.perf_counter() - started) * 1000)
                        failure_reason = _reason_from_exception(exc)
                        mirror_status = "failed"
                        summary.logo_assets_failed += 1
                        unresolved_reason = unresolved_reason or failure_reason
                        attempts.append(
                            AttemptRecord(
                                source=_attempt_source_name(source_name),
                                attempt_url=candidate_url,
                                outcome="failed",
                                failure_reason=failure_reason,
                                duration_ms=duration_ms,
                                details={"error": str(exc)},
                            )
                        )

                    source_key = _logo_asset_source_name(source_name)
                    logo_asset_row = {
                        "entity_type": entity.entity_type,
                        "entity_key": entity.entity_key,
                        "entity_id": entity_id_value or None,
                        "display_name": display_name,
                        "source": source_key,
                        "source_url": candidate_url,
                        "source_rank": source_rank,
                        "run_id": run_id,
                        "hosted_logo_key": _normalize_text(logo_patch.get("hosted_logo_key")) or None,
                        "hosted_logo_url": _normalize_text(logo_patch.get("hosted_logo_url")) or None,
                        "hosted_logo_sha256": _normalize_text(logo_patch.get("hosted_logo_sha256")) or None,
                        "hosted_logo_content_type": _normalize_text(logo_patch.get("hosted_logo_content_type")) or None,
                        "hosted_logo_bytes": logo_patch.get("hosted_logo_bytes"),
                        "hosted_logo_etag": _normalize_text(logo_patch.get("hosted_logo_etag")) or None,
                        "base_logo_format": _detect_base_logo_format(
                            wikimedia_logo_file="",
                            logo_source_url=candidate_url,
                            hosted_logo_url=_normalize_text(logo_patch.get("hosted_logo_url")),
                        ),
                        "pixel_width": None,
                        "pixel_height": None,
                        "mirror_status": mirror_status,
                        "failure_reason": failure_reason,
                        "is_primary": False,
                    }
                    _upsert_logo_asset(db, row=logo_asset_row)
                    existing_asset_index[(source_key, candidate_url)] = logo_asset_row

            for source_name, source_url in _superseded_failed_logopedia_svg_urls(existing_asset_index):
                _mark_logo_asset_skipped(
                    db,
                    entity_type=entity.entity_type,
                    entity_key=entity.entity_key,
                    source=source_name,
                    source_url=source_url,
                    reason="raster_variant_mirrored",
                )
                updated_row = existing_asset_index.get((source_name, source_url), {})
                existing_asset_index[(source_name, source_url)] = {
                    **updated_row,
                    "mirror_status": "skipped",
                    "failure_reason": "raster_variant_mirrored",
                }

            for source_name in source_priority:
                candidate = successful_by_source.get(source_name)
                if not candidate:
                    continue
                selected_logo_source = source_name
                selected_logo_url, selected_logo_patch = candidate
                break

            if selected_logo_patch:
                patch.update(selected_logo_patch)
                if (_logo_asset_source_name(selected_logo_source), selected_logo_url) in mirrored_urls_this_run:
                    summary.logos_mirrored += 1
                merged_row = {**merged_row, **selected_logo_patch}
                has_base_logo = bool(_normalize_text(merged_row.get("hosted_logo_url")))
                _reset_logo_asset_primary_flags(
                    db,
                    entity_type=entity.entity_type,
                    entity_key=entity.entity_key,
                )
                _mark_logo_asset_primary(
                    db,
                    entity_type=entity.entity_type,
                    entity_key=entity.entity_key,
                    source=_logo_asset_source_name(selected_logo_source),
                    source_url=selected_logo_url,
                )
            else:
                has_base_logo = bool(_normalize_text(merged_row.get("hosted_logo_url")))

        if not has_base_logo:
            if not (_normalize_text(wikidata_id) or _normalize_text(existing_wikidata)):
                unresolved_reason = unresolved_reason or "no_wikidata_match"
            elif not any(candidates.values()):
                unresolved_reason = unresolved_reason or external_reason or "no_logo_claim"
            else:
                unresolved_reason = unresolved_reason or external_reason or "download_failed"

        merged_row = {**core_row, **patch}
        base_logo_url = _normalize_text(merged_row.get("hosted_logo_url"))
        variant_source_url = base_logo_url or selected_logo_url

        if not args.skip_s3 and not args.dry_run and core_row:
            existing_black = _normalize_text(merged_row.get("hosted_logo_black_url"))
            existing_white = _normalize_text(merged_row.get("hosted_logo_white_url"))
            if variant_source_url and (args.force or not (existing_black and existing_white)):
                started = time.perf_counter()
                try:
                    variant_result = mirror_logo_monochrome_variants_row(
                        {**merged_row, **patch},
                        kind=logo_kind,
                        id_field=id_field,
                        source_url=variant_source_url,
                        force=bool(args.force),
                        s3_client=s3_client,
                        source=_attempt_source_name(selected_logo_source or "wikimedia"),
                    )
                    duration_ms = int((time.perf_counter() - started) * 1000)
                    if isinstance(variant_result, MonochromeLogoMirrorResult):
                        patch.update(variant_result.patch)
                        summary.variants_black_mirrored += int(variant_result.black_mirrored)
                        summary.variants_white_mirrored += int(variant_result.white_mirrored)
                    attempts.append(
                        AttemptRecord(
                            source="variant",
                            attempt_url=variant_source_url,
                            outcome="success",
                            failure_reason=None,
                            duration_ms=duration_ms,
                        )
                    )
                except Exception as exc:  # noqa: BLE001
                    duration_ms = int((time.perf_counter() - started) * 1000)
                    attempts.append(
                        AttemptRecord(
                            source="variant",
                            attempt_url=variant_source_url,
                            outcome="failed",
                            failure_reason=_reason_from_exception(exc),
                            duration_ms=duration_ms,
                            details={"error": str(exc)},
                        )
                    )
                    unresolved_reason = unresolved_reason or _reason_from_exception(exc)
            elif not variant_source_url:
                attempts.append(
                    AttemptRecord(
                        source="variant",
                        attempt_url=None,
                        outcome="skipped",
                        failure_reason="no_source_url",
                        duration_ms=0,
                    )
                )

        if patch and core_row and not args.dry_run:
            _update_core_row(db, table=row_table, id_field=id_field, entity_id=core_row.get(id_field), patch=patch)

        merged_row = {**core_row, **patch}
        final_wikidata = _normalize_text(merged_row.get("wikidata_id")) or wikidata_id
        final_wikipedia = _normalize_text(merged_row.get("wikipedia_url")) or wikipedia_url
        final_logo_url = _normalize_text(merged_row.get("hosted_logo_url"))
        final_logo_black = _normalize_text(merged_row.get("hosted_logo_black_url"))
        final_logo_white = _normalize_text(merged_row.get("hosted_logo_white_url"))
        final_logo_file = _normalize_text(merged_row.get("wikimedia_logo_file"))
        base_logo_format = _detect_base_logo_format(
            wikimedia_logo_file=final_logo_file,
            logo_source_url=selected_logo_url,
            hosted_logo_url=final_logo_url,
        )
        resolution_status, resolution_reason = _build_resolution_status(
            wikidata_id=final_wikidata,
            wikipedia_url=final_wikipedia,
            hosted_logo_url=final_logo_url,
            hosted_logo_black_url=final_logo_black,
            hosted_logo_white_url=final_logo_white,
            base_logo_format=base_logo_format,
            reason=unresolved_reason,
        )

        completion_payload = {
            "entity_type": entity.entity_type,
            "entity_key": entity.entity_key,
            "entity_id": entity_id_value or None,
            "display_name": display_name,
            "available_show_count": int(entity.available_show_count),
            "added_show_count": int(entity.added_show_count),
            "wikidata_id": final_wikidata or None,
            "wikipedia_url": final_wikipedia or None,
            "hosted_logo_url": final_logo_url or None,
            "hosted_logo_black_url": final_logo_black or None,
            "hosted_logo_white_url": final_logo_white or None,
            "base_logo_format": base_logo_format,
            "resolution_status": resolution_status,
            "resolution_reason": resolution_reason,
            "source_priority": source_priority,
            "last_run_id": run_id,
            "last_attempt_at": _now_iso(),
            "updated_at": _now_iso(),
        }

        completion_row = completion_payload
        if not args.dry_run:
            completion_row = _upsert_completion(db, completion_payload)
            completion_id = _normalize_text(completion_row.get("id"))
            if completion_id:
                _insert_attempts(
                    db,
                    completion_id=completion_id,
                    run_id=run_id,
                    entity_type=entity.entity_type,
                    entity_key=entity.entity_key,
                    attempts=attempts,
                )

        if resolution_status != "resolved":
            summary.unresolved_logos.append(
                UnresolvedLogo(
                    type=row_type,
                    id=entity_id_value or entity.entity_key,
                    name=display_name,
                    reason=resolution_reason or "incomplete_metadata",
                )
            )
    except FatalSyncError:
        raise
    except Exception as exc:  # noqa: BLE001
        summary.failures += 1
        reason = _reason_from_exception(exc)
        summary.unresolved_logos.append(
            UnresolvedLogo(
                type=row_type,
                id=entity_id_value or entity.entity_key,
                name=display_name,
                reason=reason,
            )
        )
        try:
            if not args.dry_run:
                completion_payload = {
                    "entity_type": entity.entity_type,
                    "entity_key": entity.entity_key,
                    "entity_id": entity_id_value or None,
                    "display_name": display_name,
                    "available_show_count": int(entity.available_show_count),
                    "added_show_count": int(entity.added_show_count),
                    "base_logo_format": "unknown",
                    "resolution_status": "failed",
                    "resolution_reason": reason,
                    "source_priority": _source_priority(override),
                    "last_run_id": run_id,
                    "last_attempt_at": _now_iso(),
                    "updated_at": _now_iso(),
                }
                completion_row = _upsert_completion(db, completion_payload)
                completion_id = _normalize_text(completion_row.get("id"))
                if completion_id:
                    _insert_attempts(
                        db,
                        completion_id=completion_id,
                        run_id=run_id,
                        entity_type=entity.entity_type,
                        entity_key=entity.entity_key,
                        attempts=attempts,
                    )
        except Exception:
            summary.failures += 1

        if args.verbose:
            print(f"ERROR {row_table} {id_field}={entity_id_value or '<missing>'}: {exc}", file=sys.stderr)


def _refresh_completion_snapshot(
    db,
    *,
    inventory: dict[tuple[str, str], InventoryEntity],
    summary: SyncSummary,
) -> None:
    completion_rows: dict[tuple[str, str], dict[str, Any]] = {}
    query = db.schema("admin").table("network_streaming_completion").select("*").order("updated_at", desc=True)
    for row in _iter_rows_paged(query):
        entity_type = _normalize_text(row.get("entity_type"))
        entity_key = _name_key(row.get("entity_key"))
        key = (entity_type, entity_key)
        if key not in inventory or key in completion_rows:
            continue
        completion_rows[key] = row

    unresolved: list[UnresolvedLogo] = []
    resolved_count = 0
    for key, entity in inventory.items():
        row = completion_rows.get(key)
        if not row:
            unresolved.append(
                UnresolvedLogo(
                    type=entity.entity_type,
                    id=entity.entity_key,
                    name=entity.display_name,
                    reason="missing_completion_row",
                )
            )
            continue
        status = _normalize_text(row.get("resolution_status"))
        if status == "resolved":
            resolved_count += 1
            continue
        unresolved.append(
            UnresolvedLogo(
                type=entity.entity_type,
                id=_normalize_text(row.get("entity_id")) or entity.entity_key,
                name=_normalize_text(row.get("display_name")) or entity.display_name,
                reason=_normalize_text(row.get("resolution_reason")) or "incomplete_metadata",
            )
        )

    summary.completion_total = len(inventory)
    summary.completion_resolved = resolved_count
    summary.completion_unresolved = len(unresolved)
    summary.completion_percent = round((resolved_count / len(inventory)) * 100.0, 2) if inventory else 100.0
    summary.unresolved_logos = unresolved


def _select_entities(
    inventory: dict[tuple[str, str], InventoryEntity],
    *,
    unresolved_only: bool,
    unresolved_keys: set[tuple[str, str]],
    limit: int | None,
) -> list[InventoryEntity]:
    rows = [
        inventory[key]
        for key in sorted(inventory.keys(), key=lambda item: (item[0], inventory[item].display_name.casefold()))
        if not unresolved_only or key in unresolved_keys
    ]

    if limit is None or limit <= 0:
        return rows

    by_type: dict[str, int] = defaultdict(int)
    selected: list[InventoryEntity] = []
    for row in rows:
        current = by_type[row.entity_type]
        if current >= limit:
            continue
        by_type[row.entity_type] = current + 1
        selected.append(row)
    return selected


def _filter_entities_by_target(
    entities: list[InventoryEntity],
    *,
    entity_type: str | None,
    entity_keys: list[str] | None,
) -> list[InventoryEntity]:
    normalized_type = _normalize_text(entity_type).lower()
    normalized_keys = {_name_key(value) for value in (entity_keys or []) if _name_key(value)}
    return [
        row
        for row in entities
        if (not normalized_type or row.entity_type == normalized_type)
        and (not normalized_keys or row.entity_key in normalized_keys)
    ]


def run_sync(args: argparse.Namespace) -> SyncSummary:
    load_env()
    db = create_supabase_admin_client()
    summary = SyncSummary()
    s3_client = None if args.skip_s3 or args.dry_run else get_s3_client()
    run_id = _normalize_text(args.resume_run_id) or f"network-streaming-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}"
    summary.run_id = run_id
    summary.run_status = "running"
    started_at_iso = _now_iso()

    requested_entity_type = _normalize_text(getattr(args, "entity_type", None)).lower()
    requested_inventory_types = (
        {requested_entity_type}
        if requested_entity_type in {"network", "streaming", "production"}
        else None
    )
    inventory = _load_used_inventory(db, include_types=requested_inventory_types)
    network_lookup = (
        _load_dimension_lookup(db, table="networks", id_field="id", name_field="name")
        if requested_inventory_types is None or "network" in requested_inventory_types
        else {}
    )
    provider_lookup = (
        _load_dimension_lookup(
            db,
            table="watch_providers",
            id_field="provider_id",
            name_field="provider_name",
        )
        if requested_inventory_types is None or "streaming" in requested_inventory_types
        else {}
    )
    production_lookup = (
        _load_dimension_lookup(
            db,
            table="production_companies",
            id_field="id",
            name_field="name",
        )
        if requested_inventory_types is None or "production" in requested_inventory_types
        else {}
    )
    overrides = _load_overrides(db)

    unresolved_keys = _load_unresolved_keys(db, used_keys=set(inventory.keys())) if args.unresolved_only else set()
    selected_entities = _select_entities(
        inventory,
        unresolved_only=bool(args.unresolved_only),
        unresolved_keys=unresolved_keys,
        limit=args.limit,
    )
    selected_entities = _filter_entities_by_target(
        selected_entities,
        entity_type=getattr(args, "entity_type", None),
        entity_keys=getattr(args, "entity_key", None),
    )
    selected_types = {row.entity_type for row in selected_entities}
    production_entity_keys = {row.entity_key for row in selected_entities if row.entity_type == "production"}
    include_production_hints = "production" in selected_types and (
        bool(getattr(args, "refresh_external_sources", False))
        or not bool(getattr(args, "unresolved_only", False))
    )
    context = _build_sync_context(
        db,
        include_network_hints="network" in selected_types,
        include_streaming_hints="streaming" in selected_types,
        include_production_hints=include_production_hints,
        production_entity_keys=production_entity_keys,
    )
    summary.svg_rasterizer_available = bool(context.svg_rasterizer_available)

    start_after = _parse_start_after(args.start_after)
    if args.resume_run_id and not start_after:
        start_after = _load_sync_run_cursor(db, run_id=run_id)
    entities = _apply_start_after_cursor(selected_entities, start_after=start_after)

    batch_size = max(1, int(args.batch_size or 25))
    max_runtime_sec = max(1, int(args.max_runtime_sec or 840))
    runtime_started = time.perf_counter()
    last_cursor: tuple[str, str] | None = start_after
    fatal_error: str | None = None
    interrupted = False

    if not args.dry_run:
        _upsert_sync_run_state(
            db,
            run_id=run_id,
            status="running",
            summary=summary,
            cursor=last_cursor,
            started_at=started_at_iso,
            finished_at=None,
            error_message=None,
        )

    try:
        for idx, entity in enumerate(entities):
            elapsed = time.perf_counter() - runtime_started
            if elapsed >= max_runtime_sec:
                summary.run_status = "stopped"
                summary.resume_cursor_entity_type = last_cursor[0] if last_cursor else None
                summary.resume_cursor_entity_key = last_cursor[1] if last_cursor else None
                break

            pair = (entity.entity_type, entity.entity_key)
            if entity.entity_type == "network":
                core_row = network_lookup.get(entity.entity_key)
            elif entity.entity_type == "streaming":
                core_row = provider_lookup.get(entity.entity_key)
            else:
                core_row = production_lookup.get(entity.entity_key)
            override = overrides.get(pair)
            try:
                _process_entity(
                    db,
                    entity=entity,
                    core_row=core_row,
                    override=override,
                    run_id=run_id,
                    args=args,
                    summary=summary,
                    s3_client=s3_client,
                    context=context,
                )
            except FatalSyncError as exc:
                summary.failures += 1
                summary.run_status = "failed"
                fatal_error = str(exc)
                summary.resume_cursor_entity_type = entity.entity_type
                summary.resume_cursor_entity_key = entity.entity_key
                last_cursor = (entity.entity_type, entity.entity_key)
                if not args.dry_run:
                    _upsert_sync_run_state(
                        db,
                        run_id=run_id,
                        status="failed",
                        summary=summary,
                        cursor=last_cursor,
                        started_at=started_at_iso,
                        finished_at=_now_iso(),
                        error_message=fatal_error,
                    )
                break

            last_cursor = pair
            if not args.dry_run and ((idx + 1) % batch_size == 0):
                _upsert_sync_run_state(
                    db,
                    run_id=run_id,
                    status="running",
                    summary=summary,
                    cursor=last_cursor,
                    started_at=started_at_iso,
                    finished_at=None,
                    error_message=None,
                )
    except KeyboardInterrupt:
        interrupted = True
        summary.run_status = "failed"
        summary.failures += 1
        fatal_error = "keyboard_interrupt"
        summary.resume_cursor_entity_type = last_cursor[0] if last_cursor else None
        summary.resume_cursor_entity_key = last_cursor[1] if last_cursor else None

    if not args.dry_run and not interrupted:
        _refresh_completion_snapshot(db, inventory=inventory, summary=summary)
    elif args.dry_run:
        summary.completion_total = len(inventory)
        summary.completion_resolved = 0
        summary.completion_unresolved = len(inventory)
        summary.completion_percent = 0.0

    if summary.run_status not in {"stopped", "failed"}:
        summary.run_status = "completed"
        summary.resume_cursor_entity_type = None
        summary.resume_cursor_entity_key = None

    if summary.run_status == "stopped" and last_cursor:
        summary.resume_cursor_entity_type = last_cursor[0]
        summary.resume_cursor_entity_key = last_cursor[1]

    if not args.dry_run:
        _upsert_sync_run_state(
            db,
            run_id=run_id,
            status=summary.run_status,
            summary=summary,
            cursor=(summary.resume_cursor_entity_type, summary.resume_cursor_entity_key)
            if summary.resume_cursor_entity_type and summary.resume_cursor_entity_key
            else None,
            started_at=started_at_iso,
            finished_at=_now_iso(),
            error_message=fatal_error,
        )

    return summary


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    summary = run_sync(args)
    summary_dict = asdict(summary)

    print(f"run_id={summary.run_id}")
    print(f"run_status={summary.run_status}")
    print(f"resume_cursor_entity_type={summary.resume_cursor_entity_type or ''}")
    print(f"resume_cursor_entity_key={summary.resume_cursor_entity_key or ''}")
    print(f"svg_rasterizer_available={'true' if summary.svg_rasterizer_available else 'false'}")
    print("Summary")
    for key in (
        "processed",
        "links_enriched",
        "wikidata_linked",
        "wikipedia_linked",
        "logos_mirrored",
        "variants_black_mirrored",
        "variants_white_mirrored",
        "logo_assets_discovered",
        "logo_assets_mirrored",
        "logo_assets_skipped",
        "logo_assets_failed",
        "completion_total",
        "completion_resolved",
        "completion_unresolved",
        "failures",
    ):
        print(f"{key}={summary_dict[key]}")

    print(f"completion_percent={summary.completion_percent:.2f}")
    print(f"unresolved_logos={len(summary.unresolved_logos)}")
    print("Unresolved Logos")
    for item in summary.unresolved_logos:
        print("unresolved_logo=" + json.dumps(asdict(item), ensure_ascii=False))

    return 0 if summary.run_status != "failed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
