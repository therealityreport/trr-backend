from __future__ import annotations

import json
import logging
import os
import re
import threading
import urllib.error
import urllib.request
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from functools import lru_cache
from queue import Empty, SimpleQueue
from time import perf_counter
from typing import Any, Literal
from urllib.parse import parse_qs, quote, unquote, urljoin, urlparse, urlunparse
from uuid import UUID, uuid4

from bs4 import BeautifulSoup
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field, HttpUrl

from api.auth import AdminUser
from api.deps import SupabaseAdminClient, get_list_result
from trr_backend.db import pg
from trr_backend.ingestion.show_cast_matrix_scraper import (
    is_missing_fandom_page,
    is_missing_wikipedia_page,
)
from trr_backend.integrations.fandom import (
    build_fandom_wiki_url_from_name,
    is_allowlisted_fandom_domain,
    load_fandom_community_allowlist,
    load_fandom_community_allowlist_with_source,
    normalize_fandom_community_domain,
    refresh_fandom_community_allowlist_cache,
    search_allowlisted_fandom_wikis,
    search_fandom_community_wiki_candidates,
    search_real_housewives_wiki,
)
from trr_backend.integrations.fandom_discovery import discover_fandom_candidate_pages
from trr_backend.socials.platforms import infer_platform_from_url

router = APIRouter(prefix="/admin/shows", tags=["admin-show-links"])
fandom_router = APIRouter(prefix="/admin/fandom", tags=["admin-fandom"])
_BRAVO_VARIANT = "default"
logger = logging.getLogger(__name__)

EntityType = Literal["show", "season", "person"]
LinkGroup = Literal["official", "social", "knowledge", "cast_announcements", "other"]
LinkStatus = Literal["pending", "approved", "rejected"]
_WIKIPEDIA_MISSING_ARTICLE_DETAIL = "Wikipedia does not have an article with this exact name."
_WIKIPEDIA_SHOW_VARIANT_MISMATCH_DETAIL = "Wikipedia link points to a different show/version."
_WIKIPEDIA_FETCH_ERROR_DETAIL = "Could not verify the Wikipedia page right now. Try again."


class LinkDiscoverRequest(BaseModel):
    include_seasons: bool = True
    include_people: bool = True


class LinkBulkAddRequest(BaseModel):
    inputs: list[str] = Field(default_factory=list)
    input_text: str | None = None


class LinkCreateRequest(BaseModel):
    entity_type: EntityType
    entity_id: UUID
    link_group: LinkGroup
    link_kind: str
    url: HttpUrl
    label: str | None = None
    season_number: int | None = Field(default=None, ge=0, le=200)
    status: LinkStatus = "approved"
    confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    source: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class LinkPatchRequest(BaseModel):
    link_group: LinkGroup | None = None
    link_kind: str | None = None
    url: HttpUrl | None = None
    label: str | None = None
    status: LinkStatus | None = None
    confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    metadata: dict[str, Any] | None = None


class FandomAllowlistUpdateRequest(BaseModel):
    domains: list[str] = Field(default_factory=list)


def _slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower().strip()).strip("-")


def _normalize_link_kind(value: str) -> str:
    normalized = str(value or "").strip().lower()
    if normalized == "wikia":
        return "fandom"
    return normalized


def _canonicalize_url(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    parsed = urlparse(raw)
    if not parsed.scheme or not parsed.netloc:
        return raw
    scheme = parsed.scheme.lower()
    hostname = (parsed.hostname or "").lower()
    if not hostname:
        return raw

    netloc = hostname
    if parsed.port and not ((scheme == "http" and parsed.port == 80) or (scheme == "https" and parsed.port == 443)):
        netloc = f"{netloc}:{parsed.port}"

    if parsed.username:
        auth = parsed.username
        if parsed.password:
            auth = f"{auth}:{parsed.password}"
        netloc = f"{auth}@{netloc}"

    path = parsed.path or "/"
    # Normalize encoded wiki paths so %28...%29 and (...) collapse to one URL key.
    try:
        decoded_path = unquote(path)
        path = quote(decoded_path, safe="/()_-,.:+")
    except Exception:  # noqa: BLE001
        pass
    if path != "/":
        path = path.rstrip("/")
    return urlunparse((scheme, netloc, path, "", parsed.query, ""))


def _url_key(value: str) -> str:
    return _canonicalize_url(value).lower()


def _source_timeout_seconds(source: str, *, default: float = 20.0) -> float:
    env_key = f"TRR_LINK_TIMEOUT_{source.strip().upper()}_SECONDS"
    raw = str(os.getenv(env_key) or "").strip()
    if not raw:
        return default
    try:
        parsed = float(raw)
    except ValueError:
        return default
    if parsed <= 0:
        return default
    return parsed


def _positive_float_env(key: str, default: float) -> float:
    raw = str(os.getenv(key) or "").strip()
    if not raw:
        return default
    try:
        parsed = float(raw)
    except ValueError:
        return default
    return parsed if parsed > 0 else default


def _positive_int_env(key: str, default: int) -> int:
    raw = str(os.getenv(key) or "").strip()
    if not raw:
        return default
    try:
        parsed = int(raw)
    except ValueError:
        return default
    return parsed if parsed > 0 else default


def _link_discovery_run_timeout_seconds() -> float:
    return _positive_float_env("TRR_LINK_DISCOVERY_RUN_TIMEOUT_SECONDS", 12 * 60.0)


def _link_discovery_stage_timeout_seconds(stage: str) -> float:
    normalized = str(stage or "").strip().lower()
    if normalized == "show":
        return _positive_float_env("TRR_LINK_DISCOVERY_STAGE_SHOW_TIMEOUT_SECONDS", 120.0)
    if normalized == "season":
        return _positive_float_env("TRR_LINK_DISCOVERY_STAGE_SEASON_TIMEOUT_SECONDS", 240.0)
    if normalized == "people":
        return _positive_float_env("TRR_LINK_DISCOVERY_STAGE_PEOPLE_TIMEOUT_SECONDS", 300.0)
    return _positive_float_env("TRR_LINK_DISCOVERY_STAGE_TIMEOUT_SECONDS", 180.0)


def _link_discovery_stage_fandom_candidate_cap(stage: str) -> int:
    normalized = str(stage or "").strip().lower()
    if normalized == "show":
        return _positive_int_env("TRR_LINK_DISCOVERY_STAGE_SHOW_FANDOM_MAX", 180)
    if normalized == "season":
        return _positive_int_env("TRR_LINK_DISCOVERY_STAGE_SEASON_FANDOM_MAX", 600)
    if normalized == "people":
        return _positive_int_env("TRR_LINK_DISCOVERY_STAGE_PEOPLE_FANDOM_MAX", 900)
    return _positive_int_env("TRR_LINK_DISCOVERY_STAGE_FANDOM_MAX", 300)


def _link_discovery_people_fandom_candidates_per_person_cap() -> int:
    return _positive_int_env("TRR_LINK_DISCOVERY_PEOPLE_FANDOM_MAX_PER_PERSON", 40)


def _link_discovery_season_fandom_candidates_per_domain_cap() -> int:
    return _positive_int_env("TRR_LINK_DISCOVERY_SEASON_FANDOM_MAX_PER_DOMAIN", 30)


def _link_discovery_wikidata_cast_scan_cap() -> int:
    return _positive_int_env("TRR_LINK_DISCOVERY_WIKIDATA_CAST_SCAN_MAX", 300)


def _start_discovery_stage_budget(
    stats: dict[str, Any] | None,
    *,
    stage: str,
) -> dict[str, Any]:
    timeout_seconds = _link_discovery_stage_timeout_seconds(stage)
    max_candidates = _link_discovery_stage_fandom_candidate_cap(stage)
    if stats is not None:
        stats["__stage_name"] = stage
        stats["__stage_started_perf"] = perf_counter()
        stats["__stage_deadline_perf"] = (perf_counter() + timeout_seconds) if timeout_seconds > 0 else 0.0
        stats["__stage_fandom_candidates_start"] = int(stats.get("fandom_candidates_tested") or 0)
        stats["__stage_max_fandom_candidates"] = int(max_candidates)
        stats["__stage_budget_exhausted"] = False
        stats["__stage_budget_exhausted_reason"] = ""
    return {
        "stage": stage,
        "timeout_seconds": timeout_seconds,
        "max_fandom_candidates": max_candidates,
    }


def _stage_budget_exhausted(stats: dict[str, Any] | None) -> bool:
    if not isinstance(stats, dict):
        return False
    if stats.get("__stage_budget_exhausted") is True:
        return True
    deadline_perf = float(stats.get("__stage_deadline_perf") or 0.0)
    max_candidates = int(stats.get("__stage_max_fandom_candidates") or 0)
    tested = int(stats.get("fandom_candidates_tested") or 0)
    start_tested = int(stats.get("__stage_fandom_candidates_start") or 0)
    stage_tested = max(0, tested - start_tested)
    timed_out = deadline_perf > 0 and perf_counter() >= deadline_perf
    over_cap = max_candidates > 0 and stage_tested >= max_candidates
    if not timed_out and not over_cap:
        return False
    stats["__stage_budget_exhausted"] = True
    reason = "stage_timeout_budget" if timed_out else "stage_fandom_candidate_cap"
    stats["__stage_budget_exhausted_reason"] = reason
    stats["skipped_fetch_budget"] = int(stats.get("skipped_fetch_budget") or 0) + 1
    stage_name = str(stats.get("__stage_name") or "").strip().lower()
    if stage_name:
        key = f"{stage_name}_skipped_fetch_budget"
        stats[key] = int(stats.get(key) or 0) + 1
    return True


def _can_attempt_fandom_candidate(stats: dict[str, Any] | None) -> bool:
    if _stage_budget_exhausted(stats):
        return False
    if stats is not None:
        stats["fandom_candidates_tested"] = int(stats.get("fandom_candidates_tested") or 0) + 1
    return True


def _extract_constraint_name_from_error(error: Exception) -> str:
    diag = getattr(error, "diag", None)
    constraint = str(getattr(diag, "constraint_name", "") or "").strip()
    if constraint:
        return constraint
    message = str(error or "")
    match = re.search(r'constraint "?([a-zA-Z0-9_]+)"?', message, flags=re.IGNORECASE)
    return str(match.group(1) if match else "").strip()


def _is_duplicate_violation(error: Exception, *, constraint: str | None = None) -> bool:
    code = str(getattr(error, "pgcode", "") or "").strip()
    cause = getattr(error, "__cause__", None)
    if not code and cause is not None:
        code = str(getattr(cause, "pgcode", "") or "").strip()
    message = str(error or "").lower()
    is_duplicate = code == "23505" or "duplicate key value violates unique constraint" in message
    if not is_duplicate:
        return False
    if not constraint:
        return True
    extracted = _extract_constraint_name_from_error(error).lower()
    if extracted:
        return extracted == constraint.lower()
    return constraint.lower() in message


def _upsert_link(
    db: SupabaseAdminClient,
    *,
    show_id: str,
    entity_type: str,
    entity_id: str,
    link_group: str,
    link_kind: str,
    url: str,
    label: str | None,
    season_number: int,
    status: str,
    confidence: float | None,
    source: str | None,
    discovered_by: str | None,
    metadata: dict[str, Any] | None,
    actor: str,
) -> dict[str, Any]:
    canonical_url = _canonicalize_url(url)
    normalized_kind = _normalize_link_kind(link_kind)
    payload = {
        "show_id": show_id,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "link_group": link_group,
        "link_kind": normalized_kind,
        "url": canonical_url,
        "url_key": _url_key(canonical_url),
        "label": label,
        "season_number": max(0, season_number),
        "status": status,
        "confidence": confidence,
        "source": source,
        "discovered_by": discovered_by,
        "metadata": metadata or {},
        "created_by": actor,
        "updated_by": actor,
    }
    try:
        response = (
            db.schema("core")
            .table("entity_links")
            .upsert(
                payload,
                on_conflict="show_id,entity_type,entity_id,link_kind,season_number,url_key",
            )
            .execute()
        )
        rows = get_list_result(response, "upserting entity links")
    except Exception as exc:  # noqa: BLE001
        if not _is_duplicate_violation(exc, constraint="entity_links_unique_active"):
            raise
        existing_response = (
            db.schema("core")
            .table("entity_links")
            .select("*")
            .eq("show_id", show_id)
            .eq("entity_type", entity_type)
            .eq("entity_id", entity_id)
            .eq("link_kind", normalized_kind)
            .eq("season_number", max(0, season_number))
            .eq("url_key", payload["url_key"])
            .order("updated_at", desc=True)
            .limit(1)
            .execute()
        )
        rows = get_list_result(existing_response, "fetching existing entity links after duplicate")
    return rows[0] if rows else payload


def _show_exists(show_id: str) -> bool:
    row = pg.fetch_one("SELECT id FROM core.shows WHERE id = %s", [show_id])
    return bool(row)


_SOCIAL_HANDLE_INPUT_RE = re.compile(
    r"^(instagram|ig|tiktok|twitter|x|youtube|yt|facebook|fb|threads|reddit)\s*[:=@]?\s*(@?[a-z0-9._-]+)$",
    flags=re.IGNORECASE,
)
_URL_WITHOUT_SCHEME_RE = re.compile(r"^[a-z0-9][a-z0-9.-]+\.[a-z]{2,}(?:/|$)", flags=re.IGNORECASE)
_SEASON_NUMBER_RE = re.compile(r"(?:season|series|s)\s*[_-]?\s*(\d{1,3})", flags=re.IGNORECASE)
_SOCIAL_LINK_KINDS = {"instagram", "tiktok", "twitter", "youtube", "facebook", "threads", "reddit"}
_SHOW_CURATED_FANDOM_BASE_URLS: dict[str, tuple[str, ...]] = {
    "the traitors": (
        "https://thetraitorsuk.fandom.com/wiki/The_Traitors_US",
        "https://thetraitors.fandom.com/wiki/The_Traitors_(US)",
    ),
    "the traitors us": (
        "https://thetraitorsuk.fandom.com/wiki/The_Traitors_US",
        "https://thetraitors.fandom.com/wiki/The_Traitors_(US)",
    ),
    "the traitors american tv series": (
        "https://thetraitorsuk.fandom.com/wiki/The_Traitors_US",
        "https://thetraitors.fandom.com/wiki/The_Traitors_(US)",
    ),
}
_LINK_KIND_LABELS: dict[str, str] = {
    "imdb": "IMDb",
    "tmdb": "TMDb",
    "wikipedia": "Wikipedia",
    "wikidata": "Wikidata",
    "tvdb": "TVDB",
    "tvmaze": "TVmaze",
    "ratinggraph": "RatingGraph",
    "freebase": "Freebase",
    "famous_birthdays": "Famous Birthdays",
    "google_kg": "Google Knowledge Graph",
    "trakt": "Trakt.tv",
    "x_topic": "X Topic",
    "fandom": "Fandom",
    "wikia": "Fandom",
    "instagram": "Instagram",
    "tiktok": "TikTok",
    "twitter": "Twitter/X",
    "youtube": "YouTube",
    "facebook": "Facebook",
    "threads": "Threads",
    "reddit": "Reddit",
    "official_page": "Official Page",
    "network_blog": "Network Blog",
    "bravo_profile": "BravoTV Profile",
    "external": "External Link",
}


def _normalize_lookup_value(value: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(value or "").lower()).strip()


def _normalize_show_network_tokens(values: list[str] | tuple[str, ...] | None) -> set[str]:
    tokens: set[str] = set()
    for value in values or []:
        normalized = _normalize_lookup_value(value)
        if not normalized:
            continue
        if "bravo" in normalized:
            tokens.add("bravo")
        if "peacock" in normalized:
            tokens.add("peacock")
        if "nbc" in normalized or "nbcuniversal" in normalized.replace(" ", ""):
            tokens.add("nbc")
    return tokens


def _effective_fandom_allowlist(
    *,
    show_name: str | None,
    network_tokens: set[str] | None = None,
    base_allowlist: list[str] | tuple[str, ...] | None = None,
    additional_domains: list[str] | tuple[str, ...] | None = None,
) -> tuple[str, ...]:
    resolved_base = base_allowlist if base_allowlist is not None else load_fandom_community_allowlist()
    domains: set[str] = set()
    for value in resolved_base:
        normalized = normalize_fandom_community_domain(str(value or ""))
        if normalized:
            domains.add(normalized)
    domains.update(_curated_show_fandom_domains(show_name))
    if network_tokens and "bravo" in network_tokens:
        domains.add("real-housewives.fandom.com")
    for raw_domain in additional_domains or []:
        normalized = normalize_fandom_community_domain(str(raw_domain or ""))
        if normalized:
            domains.add(normalized)
    return tuple(sorted(domains))


def _curated_show_fandom_base_urls(show_name: str | None) -> tuple[str, ...]:
    show_name_norm = _normalize_lookup_value(show_name)
    if not show_name_norm:
        return ()
    direct = _SHOW_CURATED_FANDOM_BASE_URLS.get(show_name_norm)
    if direct:
        return direct
    for key, urls in _SHOW_CURATED_FANDOM_BASE_URLS.items():
        if show_name_norm.startswith(f"{key} ") or show_name_norm.endswith(f" {key}"):
            return urls
    return ()


def _curated_show_fandom_domains(show_name: str | None) -> set[str]:
    domains: set[str] = set()
    for url in _curated_show_fandom_base_urls(show_name):
        parsed = urlparse(url)
        host = str(parsed.hostname or "").strip().lower()
        if host:
            domains.add(host)
    return domains


def _extract_season_number_from_text(value: str | None) -> int | None:
    text = str(value or "").strip()
    if not text:
        return None
    match = _SEASON_NUMBER_RE.search(text.replace("_", " "))
    if not match:
        return None
    try:
        parsed = int(match.group(1))
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _extract_wiki_slug_title(path: str | None) -> str | None:
    raw_path = str(path or "").strip()
    if "/wiki/" not in raw_path:
        return None
    slug = raw_path.split("/wiki/", 1)[1].strip()
    if not slug:
        return None
    return slug.split("?", 1)[0].split("#", 1)[0]


def _is_fandom_seed_url(url: str | None) -> bool:
    parsed = urlparse(str(url or "").strip())
    host = str(parsed.hostname or "").strip().lower()
    if not host or not (host.endswith("fandom.com") or host.endswith("wikia.com")):
        return False
    path = unquote(parsed.path or "").strip()
    if not path or path == "/":
        return True
    return "/wiki/" not in path


_FANDOM_SKIPPED_PAGE_PREFIXES = (
    "special:",
    "file:",
    "category:",
    "template:",
    "user:",
    "help:",
    "forum:",
    "talk:",
    "message wall:",
)


def _extract_fandom_page_title_from_url(url: str | None) -> str | None:
    parsed = urlparse(str(url or "").strip())
    slug = _extract_wiki_slug_title(unquote(parsed.path or ""))
    if not slug:
        return None
    title = str(slug).replace("_", " ").strip()
    if not title:
        return None
    lower_title = title.casefold()
    if any(lower_title.startswith(prefix) for prefix in _FANDOM_SKIPPED_PAGE_PREFIXES):
        return None
    return re.sub(r"\s+", " ", title)


def _build_fandom_link_metadata(url: str, *, title: str | None = None) -> dict[str, Any]:
    metadata: dict[str, Any] = {}
    parsed = urlparse(str(url or "").strip())
    host = str(parsed.hostname or "").strip().lower()
    page_title = str(title or "").strip() or _extract_fandom_page_title_from_url(url)
    if page_title:
        metadata["fandom_title"] = page_title
        metadata["page_title"] = page_title
    if host:
        metadata["favicon_url"] = f"https://{host}/favicon.ico"
    return metadata


def _extract_fandom_wiki_urls_from_html(html: str, *, base_url: str, max_results: int = 200) -> list[str]:
    if not html:
        return []
    base_parsed = urlparse(str(base_url or "").strip())
    base_host = str(base_parsed.hostname or "").strip().lower()
    if not base_host:
        return []

    soup = BeautifulSoup(html, "html.parser")
    urls: list[str] = []
    seen: set[str] = set()
    for anchor in soup.select("a[href]"):
        href = str(anchor.get("href") or "").strip()
        if not href:
            continue
        resolved = _canonicalize_url(urljoin(f"https://{base_host}", href))
        if not resolved:
            continue
        parsed = urlparse(resolved)
        host = str(parsed.hostname or "").strip().lower()
        if host != base_host:
            continue
        title = _extract_fandom_page_title_from_url(resolved)
        if not title:
            continue
        key = _url_key(resolved)
        if key in seen:
            continue
        seen.add(key)
        urls.append(resolved)
        if len(urls) >= max_results:
            break
    return urls


def _fandom_allpages_prefixes(query: str | None) -> list[str]:
    normalized = re.sub(r"\s+", " ", str(query or "").strip())
    if not normalized:
        return []
    raw_tokens = [token for token in re.split(r"[\s_]+", normalized) if token]
    if not raw_tokens:
        return []
    prefixes: list[str] = []
    for candidate in (
        raw_tokens[0],
        "_".join(raw_tokens[:2]) if len(raw_tokens) >= 2 else "",
        "_".join(raw_tokens[:3]) if len(raw_tokens) >= 3 else "",
    ):
        cleaned = str(candidate or "").strip()
        if cleaned and cleaned not in prefixes:
            prefixes.append(cleaned)
    return prefixes


def _search_fandom_allpages_html_candidates(
    *,
    community_domain: str,
    query: str,
    max_results: int = 50,
    stats: dict[str, Any] | None = None,
) -> list[str]:
    host = str(community_domain or "").strip().lower()
    if not host:
        return []
    candidates: list[str] = []
    seen: set[str] = set()
    for prefix in _fandom_allpages_prefixes(query):
        if not _can_attempt_fandom_candidate(stats):
            break
        all_pages_url = (
            f"https://{host}/wiki/Special:AllPages?from={quote(prefix)}&to=&namespace=0"
        )
        status_code, html, resolved_url, _ = _fetch_html_with_status(
            all_pages_url,
            timeout=_source_timeout_seconds("fandom"),
        )
        if status_code is None or status_code >= 400 or not html:
            continue
        resolved = _canonicalize_url(resolved_url or all_pages_url) or all_pages_url
        for page_url in _extract_fandom_wiki_urls_from_html(html, base_url=resolved, max_results=max_results):
            key = _url_key(page_url)
            if key in seen:
                continue
            seen.add(key)
            candidates.append(page_url)
            if len(candidates) >= max_results:
                return candidates[:max_results]
    return candidates[:max_results]


def _score_fandom_show_candidate_url(url: str, *, show_name: str | None) -> int:
    title = _extract_fandom_page_title_from_url(url)
    if not title or not show_name:
        return 0
    show_tokens = {
        token
        for token in _normalize_lookup_value(show_name).split()
        if token and token not in {"the", "tv", "show", "series", "season", "episode"}
    }
    title_tokens = {token for token in _normalize_lookup_value(title).split() if token}
    if not show_tokens or not title_tokens:
        return 0
    shared = show_tokens.intersection(title_tokens)
    if not shared:
        return 0
    score = len(shared) * 100
    if show_tokens.issubset(title_tokens):
        score += 200
    if "season" in title_tokens or "episode" in title_tokens:
        score -= 100
    return score


def _show_fandom_source_priority(source: str | None) -> int:
    normalized = str(source or "").strip().lower()
    if normalized == "core.entity_links":
        return 0
    if normalized == "curated_fandom_base":
        return 1
    if normalized == "bravo_default":
        return 2
    if normalized.endswith(":derived_show_page"):
        return 3
    return 4


def _collect_seeded_fandom_candidate_urls_by_domain(
    seed_urls: list[str],
    *,
    fandom_allowlist: list[str] | tuple[str, ...] | None = None,
    stats: dict[str, Any] | None = None,
) -> dict[str, list[str]]:
    resolved_allowlist = fandom_allowlist if fandom_allowlist is not None else load_fandom_community_allowlist()
    by_domain: dict[str, list[str]] = {}
    seen_by_domain: dict[str, set[str]] = {}

    def add_candidate(candidate_url: str) -> None:
        canonical = _canonicalize_url(candidate_url)
        if not canonical:
            return
        parsed = urlparse(canonical)
        host = str(parsed.hostname or "").strip().lower()
        if not host:
            return
        if not is_allowlisted_fandom_domain(host, allowlist=resolved_allowlist):
            return
        title = _extract_fandom_page_title_from_url(canonical)
        if not title:
            return
        if host not in seen_by_domain:
            seen_by_domain[host] = set()
        key = _url_key(canonical)
        if key in seen_by_domain[host]:
            return
        seen_by_domain[host].add(key)
        bucket = by_domain.get(host)
        if bucket is None:
            by_domain[host] = [canonical]
        else:
            bucket.append(canonical)

    for raw_seed_url in seed_urls:
        if not _can_attempt_fandom_candidate(stats):
            break
        canonical_seed = _canonicalize_url(raw_seed_url)
        if not canonical_seed:
            continue
        parsed = urlparse(canonical_seed)
        host = str(parsed.hostname or "").strip().lower()
        if not host or not is_allowlisted_fandom_domain(host, allowlist=resolved_allowlist):
            continue
        add_candidate(canonical_seed)
        status_code, html, resolved_url, _ = _fetch_html_with_status(
            canonical_seed,
            timeout=_source_timeout_seconds("fandom"),
        )
        if status_code is None or status_code >= 400 or not html:
            continue
        resolved = _canonicalize_url(resolved_url or canonical_seed)
        if not resolved or is_missing_fandom_page(html, resolved):
            continue
        add_candidate(resolved)
        for page_url in _extract_fandom_wiki_urls_from_html(html, base_url=resolved):
            add_candidate(page_url)

        all_pages_url = f"https://{host}/wiki/Special:AllPages"
        if not _can_attempt_fandom_candidate(stats):
            break
        status_code, html, resolved_url, _ = _fetch_html_with_status(
            all_pages_url,
            timeout=_source_timeout_seconds("fandom"),
        )
        if status_code is None or status_code >= 400 or not html:
            continue
        resolved_all_pages = _canonicalize_url(resolved_url or all_pages_url) or all_pages_url
        for page_url in _extract_fandom_wiki_urls_from_html(html, base_url=resolved_all_pages):
            add_candidate(page_url)
    return by_domain


def _score_fandom_season_candidate_url(url: str, *, show_name: str | None, season_number: int) -> int:
    title = _extract_fandom_page_title_from_url(url)
    if not title:
        return 0
    detected_season = _extract_season_number_from_text(title)
    if detected_season != season_number:
        return 0
    score = 300
    if show_name:
        show_tokens = {
            token
            for token in _normalize_lookup_value(show_name).split()
            if token and token not in {"the", "season", "series", "tv", "show"}
        }
        title_tokens = {token for token in _normalize_lookup_value(title).split() if token}
        if show_tokens and show_tokens.intersection(title_tokens):
            score += 100
    return score


def _try_expand_social_handle_input(value: str) -> str | None:
    match = _SOCIAL_HANDLE_INPUT_RE.fullmatch(str(value or "").strip())
    if not match:
        return None
    raw_platform = match.group(1).lower()
    platform = {
        "ig": "instagram",
        "yt": "youtube",
        "x": "twitter",
        "fb": "facebook",
    }.get(raw_platform, raw_platform)
    handle = match.group(2).strip()
    canonical_handle = handle.lstrip("@")
    if not canonical_handle:
        return None
    if platform == "instagram":
        return f"https://www.instagram.com/{canonical_handle}"
    if platform == "tiktok":
        return f"https://www.tiktok.com/@{canonical_handle}"
    if platform == "twitter":
        return f"https://x.com/{canonical_handle}"
    if platform == "youtube":
        return f"https://www.youtube.com/@{canonical_handle}"
    if platform == "facebook":
        return f"https://www.facebook.com/{canonical_handle}"
    if platform == "threads":
        return f"https://www.threads.net/@{canonical_handle}"
    if platform == "reddit":
        return f"https://www.reddit.com/user/{canonical_handle}"
    return None


def _normalize_submitted_link_input(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""

    expanded_handle = _try_expand_social_handle_input(raw)
    candidate = expanded_handle or raw
    if "://" not in candidate and _URL_WITHOUT_SCHEME_RE.match(candidate):
        candidate = f"https://{candidate}"
    return _canonicalize_url(candidate)


def _load_show_wikidata_id_from_links(show_id: str) -> str | None:
    row = pg.fetch_one(
        """
        SELECT url
        FROM core.entity_links
        WHERE show_id = %s
          AND entity_type = 'show'
          AND season_number = 0
          AND lower(link_kind) = 'wikidata'
          AND lower(status) <> 'rejected'
        ORDER BY
          CASE WHEN lower(status) = 'approved' THEN 0 ELSE 1 END,
          updated_at DESC NULLS LAST,
          created_at DESC NULLS LAST
        LIMIT 1
        """,
        [show_id],
    )
    return _extract_wikidata_item_id(str((row or {}).get("url") or "").strip())


def _resolve_show_wikidata_id(show_id: str, value: Any) -> str | None:
    direct = _extract_wikidata_item_id(str(value or "").strip())
    if direct:
        return direct
    return _load_show_wikidata_id_from_links(show_id)


def _load_show_link_classifier_context(show_id: str) -> dict[str, Any]:
    show = pg.fetch_one(
        """
        SELECT id, name, imdb_id, tmdb_id, wikidata_id, networks
        FROM core.shows
        WHERE id = %s
        """,
        [show_id],
    )
    if not show:
        return {
            "show_id": show_id,
            "show_name": "",
            "show_name_norm": "",
            "show_imdb_id": None,
            "show_tmdb_id": None,
            "show_wikidata_id": None,
            "show_networks": [],
            "is_bravo_show": False,
            "seasons_by_number": {},
            "seasons_by_wikidata": {},
            "people_by_id": {},
            "people_by_name": {},
            "people_by_slug": {},
            "people_by_imdb": {},
            "people_by_tmdb": {},
            "people_by_wikidata": {},
        }
    show_wikidata_id = _resolve_show_wikidata_id(show_id, show.get("wikidata_id"))

    season_rows = pg.fetch_all(
        """
        SELECT id, season_number, external_wikidata_id, tmdb_season_id
        FROM core.seasons
        WHERE show_id = %s
        """,
        [show_id],
    )
    seasons_by_number: dict[int, dict[str, Any]] = {}
    seasons_by_wikidata: dict[str, dict[str, Any]] = {}
    for row in season_rows:
        season_number = int(row.get("season_number") or 0)
        if season_number > 0:
            seasons_by_number[season_number] = row
        wikidata_id = _extract_wikidata_item_id(str(row.get("external_wikidata_id") or "").strip())
        if wikidata_id:
            seasons_by_wikidata[wikidata_id] = row

    person_rows = pg.fetch_all(
        """
        SELECT DISTINCT
          p.id::text AS person_id,
          p.full_name,
          p.external_ids,
          cf.source_url AS fandom_url,
          ct.imdb_id AS cast_tmdb_imdb_id,
          ct.tmdb_id AS cast_tmdb_tmdb_id,
          ct.wikidata_id AS cast_tmdb_wikidata_id
        FROM core.show_cast sc
        JOIN core.people p ON p.id = sc.person_id
        LEFT JOIN core.cast_fandom cf ON cf.person_id = p.id AND cf.source = 'fandom'
        LEFT JOIN core.cast_tmdb ct ON ct.person_id = p.id
        WHERE sc.show_id = %s
        """,
        [show_id],
    )

    people_by_id: dict[str, dict[str, Any]] = {}
    people_by_name: dict[str, dict[str, Any]] = {}
    people_by_slug: dict[str, dict[str, Any]] = {}
    people_by_imdb: dict[str, dict[str, Any]] = {}
    people_by_tmdb: dict[str, dict[str, Any]] = {}
    people_by_wikidata: dict[str, dict[str, Any]] = {}

    for row in person_rows:
        person_id = str(row.get("person_id") or "").strip()
        if not person_id:
            continue
        external_ids = row.get("external_ids") if isinstance(row.get("external_ids"), dict) else {}
        person_name = str(row.get("full_name") or "").strip()
        person_name_norm = _normalize_lookup_value(person_name)
        imdb_id = _extract_imdb_person_id(
            str(external_ids.get("imdb") or external_ids.get("imdb_id") or row.get("cast_tmdb_imdb_id") or "")
        )
        tmdb_id = _extract_tmdb_person_id(
            external_ids.get("tmdb") or external_ids.get("tmdb_id") or row.get("cast_tmdb_tmdb_id")
        )
        wikidata_id = _extract_wikidata_item_id(
            str(
                external_ids.get("wikidata")
                or external_ids.get("wikidata_id")
                or row.get("cast_tmdb_wikidata_id")
                or ""
            )
        )
        fandom_title = _extract_wiki_slug_title(urlparse(str(row.get("fandom_url") or "").strip()).path)
        fandom_name_norm = _normalize_lookup_value(
            (fandom_title or "").replace("_", " ").split("(", 1)[0].strip()
        )
        person_record = {
            "id": person_id,
            "name": person_name,
            "name_norm": person_name_norm,
            "imdb_id": imdb_id,
            "tmdb_id": tmdb_id,
            "wikidata_id": wikidata_id,
            "fandom_name_norm": fandom_name_norm,
        }
        people_by_id[person_id] = person_record
        if person_name_norm and person_name_norm not in people_by_name:
            people_by_name[person_name_norm] = person_record
        if fandom_name_norm and fandom_name_norm not in people_by_slug:
            people_by_slug[fandom_name_norm] = person_record
        if imdb_id:
            people_by_imdb[imdb_id.lower()] = person_record
        if tmdb_id:
            people_by_tmdb[tmdb_id] = person_record
        if wikidata_id:
            people_by_wikidata[wikidata_id] = person_record

    show_network_values = [
        str(value).strip() for value in (show.get("networks") or []) if isinstance(value, str) and str(value).strip()
    ]
    show_networks = sorted(_normalize_show_network_tokens(show_network_values))

    return {
        "show_id": show_id,
        "show_name": str(show.get("name") or "").strip(),
        "show_name_norm": _normalize_lookup_value(str(show.get("name") or "").strip()),
        "show_imdb_id": str(show.get("imdb_id") or "").strip().lower() or None,
        "show_tmdb_id": str(show.get("tmdb_id") or "").strip() or None,
        "show_wikidata_id": show_wikidata_id,
        "show_networks": show_networks,
        "is_bravo_show": "bravo" in show_networks,
        "seasons_by_number": seasons_by_number,
        "seasons_by_wikidata": seasons_by_wikidata,
        "people_by_id": people_by_id,
        "people_by_name": people_by_name,
        "people_by_slug": people_by_slug,
        "people_by_imdb": people_by_imdb,
        "people_by_tmdb": people_by_tmdb,
        "people_by_wikidata": people_by_wikidata,
    }


def _find_person_match_for_title(context: dict[str, Any], title: str | None) -> dict[str, Any] | None:
    normalized = _normalize_lookup_value(str(title or ""))
    if not normalized:
        return None
    direct = context["people_by_name"].get(normalized) or context["people_by_slug"].get(normalized)
    if direct:
        return direct

    candidate_tokens = {token for token in normalized.split() if token}
    if not candidate_tokens:
        return None

    for person in context["people_by_id"].values():
        name_tokens = {token for token in str(person.get("name_norm") or "").split() if token}
        if not name_tokens:
            continue
        if (
            candidate_tokens == name_tokens
            or candidate_tokens.issubset(name_tokens)
            or name_tokens.issubset(candidate_tokens)
        ):
            return person
    return None


def _resolve_entity_wikidata_id(
    context: dict[str, Any],
    *,
    entity_type: str,
    entity_id: str,
    season_number: int,
) -> str | None:
    if entity_type == "show":
        return context.get("show_wikidata_id")
    if entity_type == "season":
        season_row = context["seasons_by_number"].get(season_number)
        if season_row:
            return _extract_wikidata_item_id(str(season_row.get("external_wikidata_id") or "").strip())
        for row in context["seasons_by_number"].values():
            if str(row.get("id") or "") == entity_id:
                return _extract_wikidata_item_id(str(row.get("external_wikidata_id") or "").strip())
        return None
    if entity_type == "person":
        person_row = context["people_by_id"].get(entity_id)
        if person_row:
            return _extract_wikidata_item_id(str(person_row.get("wikidata_id") or "").strip())
    return None


def _build_connected_knowledge_rows(
    context: dict[str, Any],
    *,
    entity_type: str,
    entity_id: str,
    season_number: int,
    primary_kind: str,
    primary_url: str,
) -> list[dict[str, Any]]:
    companion_rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = {(str(primary_kind), _url_key(primary_url))}

    def _append_companion_row(
        *,
        link_kind: str,
        label: str,
        url: str,
        source: str,
    ) -> None:
        canonical_url = _canonicalize_url(url)
        if not canonical_url:
            return
        normalized_kind = _normalize_link_kind(link_kind)
        link_group = "social" if normalized_kind in _SOCIAL_LINK_KINDS else "knowledge"
        dedupe_key = (normalized_kind, _url_key(canonical_url))
        if dedupe_key in seen:
            return
        seen.add(dedupe_key)
        companion_rows.append(
            {
                "entity_type": entity_type,
                "entity_id": entity_id,
                "season_number": season_number,
                "link_group": link_group,
                "link_kind": normalized_kind,
                "label": label,
                "url": canonical_url,
                "source": source,
                "metadata": {"connected_from_kind": primary_kind},
                "status": "approved",
                "confidence": 0.95,
            }
        )

    wikidata_id = _resolve_entity_wikidata_id(
        context,
        entity_type=entity_type,
        entity_id=entity_id,
        season_number=season_number,
    )
    if not wikidata_id and primary_kind == "wikidata":
        wikidata_id = _extract_wikidata_item_id(primary_url)
    if not wikidata_id and primary_kind == "wikipedia":
        wikidata_id = _resolve_wikipedia_wikidata_id(primary_url)

    if wikidata_id:
        _append_companion_row(
            link_kind="wikidata",
            label="Wikidata",
            url=f"https://www.wikidata.org/wiki/{wikidata_id}",
            source="connected_wikidata",
        )

        summary, fetch_error = _fetch_wikidata_summary(wikidata_id)
        wikidata_summary = summary if summary and not fetch_error else None

        wikipedia_url = str((wikidata_summary or {}).get("enwiki_url") or "").strip() if wikidata_summary else None
        if not wikipedia_url:
            wikipedia_url = _resolve_wikidata_enwiki_url(wikidata_id)
        if wikipedia_url:
            _append_companion_row(
                link_kind="wikipedia",
                label="Wikipedia",
                url=wikipedia_url,
                source="connected_wikidata",
            )

        if wikidata_summary:
            imdb_id = str(wikidata_summary.get("imdb_id") or "").strip()
            tmdb_tv_id = str(wikidata_summary.get("tmdb_tv_id") or "").strip()
            tmdb_person_id = str(wikidata_summary.get("tmdb_person_id") or "").strip()
            tvdb_id = str(wikidata_summary.get("tvdb_id") or "").strip()
            tvmaze_show_id = str(wikidata_summary.get("tvmaze_show_id") or "").strip()
            tvmaze_season_id = str(wikidata_summary.get("tvmaze_season_id") or "").strip()
            ratinggraph_tv_show_id = str(wikidata_summary.get("ratinggraph_tv_show_id") or "").strip()
            freebase_id = str(wikidata_summary.get("freebase_id") or "").strip()
            famous_birthdays_id = str(wikidata_summary.get("famous_birthdays_id") or "").strip()
            google_kg_id = str(wikidata_summary.get("google_kg_id") or "").strip()
            trakt_id = str(wikidata_summary.get("trakt_id") or "").strip()
            x_topic_id = str(wikidata_summary.get("x_topic_id") or "").strip()
            twitter_usernames = [
                str(value).strip()
                for value in (wikidata_summary.get("twitter_usernames") or [])
                if str(value).strip()
            ]
            instagram_usernames = [
                str(value).strip()
                for value in (wikidata_summary.get("instagram_usernames") or [])
                if str(value).strip()
            ]
            facebook_usernames = [
                str(value).strip()
                for value in (wikidata_summary.get("facebook_usernames") or [])
                if str(value).strip()
            ]
            youtube_channel_ids = [
                str(value).strip()
                for value in (wikidata_summary.get("youtube_channel_ids") or [])
                if str(value).strip()
            ]
            tiktok_usernames = [
                str(value).strip()
                for value in (wikidata_summary.get("tiktok_usernames") or [])
                if str(value).strip()
            ]
            reddit_usernames = [
                str(value).strip()
                for value in (wikidata_summary.get("reddit_usernames") or [])
                if str(value).strip()
            ]

            if entity_type == "person" and re.fullmatch(r"nm\d+", imdb_id, flags=re.IGNORECASE):
                _append_companion_row(
                    link_kind="imdb",
                    label="IMDb",
                    url=f"https://www.imdb.com/name/{imdb_id}/",
                    source="connected_wikidata_external_ids",
                )
            elif entity_type in {"show", "season"} and re.fullmatch(r"tt\d+", imdb_id, flags=re.IGNORECASE):
                _append_companion_row(
                    link_kind="imdb",
                    label="IMDb",
                    url=f"https://www.imdb.com/title/{imdb_id}/",
                    source="connected_wikidata_external_ids",
                )

            if entity_type == "person" and re.fullmatch(r"\d+", tmdb_person_id):
                _append_companion_row(
                    link_kind="tmdb",
                    label="TMDb",
                    url=f"https://www.themoviedb.org/person/{tmdb_person_id}",
                    source="connected_wikidata_external_ids",
                )
            elif entity_type in {"show", "season"} and re.fullmatch(r"\d+", tmdb_tv_id):
                _append_companion_row(
                    link_kind="tmdb",
                    label="TMDb",
                    url=f"https://www.themoviedb.org/tv/{tmdb_tv_id}",
                    source="connected_wikidata_external_ids",
                )

            if entity_type in {"show", "season"} and re.fullmatch(r"\d+", tvdb_id):
                _append_companion_row(
                    link_kind="tvdb",
                    label="TVDB",
                    url=f"https://www.thetvdb.com/series/{tvdb_id}",
                    source="connected_wikidata_external_ids",
                )
            if entity_type == "show" and re.fullmatch(r"\d+", tvmaze_show_id):
                _append_companion_row(
                    link_kind="tvmaze",
                    label="TVmaze",
                    url=f"https://www.tvmaze.com/shows/{tvmaze_show_id}",
                    source="connected_wikidata_external_ids",
                )
            if entity_type == "season" and re.fullmatch(r"\d+", tvmaze_season_id):
                _append_companion_row(
                    link_kind="tvmaze",
                    label="TVmaze",
                    url=f"https://www.tvmaze.com/seasons/{tvmaze_season_id}",
                    source="connected_wikidata_external_ids",
                )
            if entity_type == "show" and ratinggraph_tv_show_id:
                _append_companion_row(
                    link_kind="ratinggraph",
                    label="RatingGraph",
                    url=f"https://www.ratingraph.com/tv-shows/{quote(ratinggraph_tv_show_id)}",
                    source="connected_wikidata_external_ids",
                )
            if entity_type == "person" and freebase_id:
                _append_companion_row(
                    link_kind="freebase",
                    label="Freebase",
                    url=_wikidata_freebase_url(freebase_id),
                    source="connected_wikidata_identifiers",
                )
            if entity_type == "person" and famous_birthdays_id:
                _append_companion_row(
                    link_kind="famous_birthdays",
                    label="Famous Birthdays",
                    url=_wikidata_famous_birthdays_url(famous_birthdays_id),
                    source="connected_wikidata_identifiers",
                )
            if entity_type == "person" and google_kg_id:
                _append_companion_row(
                    link_kind="google_kg",
                    label="Google Knowledge Graph",
                    url=_wikidata_google_kg_url(google_kg_id),
                    source="connected_wikidata_identifiers",
                )
            if entity_type == "show" and trakt_id:
                _append_companion_row(
                    link_kind="trakt",
                    label="Trakt.tv",
                    url=_wikidata_trakt_url(trakt_id),
                    source="connected_wikidata_external_ids",
                )
            if entity_type == "show" and x_topic_id:
                _append_companion_row(
                    link_kind="x_topic",
                    label="X Topic",
                    url=_wikidata_x_topic_url(x_topic_id),
                    source="connected_wikidata_external_ids",
                )
            if entity_type == "person":
                for handle in twitter_usernames:
                    social_url = _social_url_from_platform_handle("twitter", handle)
                    if not social_url:
                        continue
                    _append_companion_row(
                        link_kind="twitter",
                        label="Twitter/X",
                        url=social_url,
                        source="connected_wikidata_social",
                    )
                for handle in instagram_usernames:
                    social_url = _social_url_from_platform_handle("instagram", handle)
                    if not social_url:
                        continue
                    _append_companion_row(
                        link_kind="instagram",
                        label="Instagram",
                        url=social_url,
                        source="connected_wikidata_social",
                    )
                for handle in facebook_usernames:
                    social_url = _social_url_from_platform_handle("facebook", handle)
                    if not social_url:
                        continue
                    _append_companion_row(
                        link_kind="facebook",
                        label="Facebook",
                        url=social_url,
                        source="connected_wikidata_social",
                    )
                for handle in youtube_channel_ids:
                    social_url = _social_url_from_platform_handle("youtube", handle)
                    if not social_url:
                        continue
                    _append_companion_row(
                        link_kind="youtube",
                        label="YouTube",
                        url=social_url,
                        source="connected_wikidata_social",
                    )
                for handle in tiktok_usernames:
                    social_url = _social_url_from_platform_handle("tiktok", handle)
                    if not social_url:
                        continue
                    _append_companion_row(
                        link_kind="tiktok",
                        label="TikTok",
                        url=social_url,
                        source="connected_wikidata_social",
                    )
                for handle in reddit_usernames:
                    social_url = _social_url_from_platform_handle("reddit", handle)
                    if not social_url:
                        continue
                    _append_companion_row(
                        link_kind="reddit",
                        label="Reddit",
                        url=social_url,
                        source="connected_wikidata_social",
                    )
    return companion_rows


def _derive_season_wikipedia_url_from_show_wikipedia(show_wikipedia_url: str, season_number: int) -> str | None:
    title = _extract_wikipedia_title(show_wikipedia_url)
    if not title:
        return None
    if season_number <= 0:
        return None
    return f"https://en.wikipedia.org/wiki/{quote((f'{title} season {season_number}').replace(' ', '_'))}"


def _sync_show_wikipedia_links(
    *,
    show_id: str,
    show_wikipedia_url: str,
    actor: str,
    exclude_link_id: str | None = None,
) -> None:
    canonical_show_url = _canonicalize_url(show_wikipedia_url)
    if not canonical_show_url:
        return

    summary, _ = _fetch_wikipedia_page_summary(canonical_show_url)
    if summary and summary.get("url"):
        canonical_show_url = _canonicalize_url(str(summary["url"]))
    if not canonical_show_url:
        return

    show_params: list[Any] = [canonical_show_url, _url_key(canonical_show_url), actor, show_id]
    show_exclusion_sql = ""
    if exclude_link_id:
        show_exclusion_sql = " AND id <> %s::uuid"
        show_params.append(str(exclude_link_id))

    pg.execute_returning(
        f"""
        UPDATE core.entity_links
        SET url = %s,
            url_key = %s,
            updated_by = %s,
            updated_at = NOW()
        WHERE show_id = %s::uuid
          AND entity_type = 'show'
          AND season_number = 0
          AND lower(link_kind) = 'wikipedia'
          {show_exclusion_sql}
        RETURNING id
        """,
        show_params,
    )

    season_rows = pg.fetch_all(
        """
        SELECT id::text AS id, season_number, entity_id::text AS season_id, source, discovered_by
        FROM core.entity_links
        WHERE show_id = %s::uuid
          AND entity_type = 'season'
          AND season_number > 0
          AND lower(link_kind) = 'wikipedia'
        """,
        [show_id],
    )
    if not season_rows:
        return

    season_metadata_rows = pg.fetch_all(
        """
        SELECT id::text AS season_id, season_number, external_wikidata_id
        FROM core.seasons
        WHERE show_id = %s::uuid
        """,
        [show_id],
    )
    season_meta_by_id = {str(row.get("season_id") or ""): row for row in season_metadata_rows}
    season_meta_by_number = {
        int(row.get("season_number") or 0): row
        for row in season_metadata_rows
        if int(row.get("season_number") or 0) > 0
    }

    for row in season_rows:
        source = str(row.get("source") or "").strip().lower()
        discovered_by = str(row.get("discovered_by") or "").strip().lower()
        if source == "manual" or discovered_by == "manual":
            continue

        season_number = int(row.get("season_number") or 0)
        if season_number <= 0:
            continue

        season_id = str(row.get("season_id") or "")
        season_meta = season_meta_by_id.get(season_id) or season_meta_by_number.get(season_number)
        season_wikidata_id = _extract_wikidata_item_id(str((season_meta or {}).get("external_wikidata_id") or ""))

        next_season_url: str | None = None
        if season_wikidata_id:
            next_season_url = _resolve_wikidata_enwiki_url(season_wikidata_id)
        if not next_season_url:
            next_season_url = _derive_season_wikipedia_url_from_show_wikipedia(canonical_show_url, season_number)
        if not next_season_url:
            continue

        canonical_season_url = _canonicalize_url(next_season_url)
        if not canonical_season_url:
            continue

        pg.execute_returning(
            """
            UPDATE core.entity_links
            SET url = %s,
                url_key = %s,
                updated_by = %s,
                updated_at = NOW()
            WHERE id = %s::uuid
              AND show_id = %s::uuid
            RETURNING id
            """,
            [canonical_season_url, _url_key(canonical_season_url), actor, str(row.get("id") or ""), show_id],
        )


def _classify_submitted_link_input(
    raw_input: str,
    context: dict[str, Any],
) -> tuple[list[dict[str, Any]], str | None]:
    submitted = str(raw_input or "").strip()
    if not submitted:
        return [], "Input is blank."

    canonical_input = _normalize_submitted_link_input(submitted)
    if not canonical_input:
        return [], "Input is blank."
    parsed = urlparse(canonical_input)
    if not parsed.scheme.startswith("http") or not parsed.netloc:
        return [], "Input must be a valid URL or recognizable social handle."

    host = (parsed.hostname or "").lower()
    path = unquote(parsed.path or "")
    query = parse_qs(parsed.query)

    entity_type = "show"
    entity_id = str(context.get("show_id") or "")
    season_number = 0
    link_group: LinkGroup = "other"
    link_kind = "external"
    label: str | None = None
    source = "manual_classifier"
    metadata: dict[str, Any] = {"submitted_input": submitted}
    canonical_url = canonical_input

    if "imdb.com" in host:
        title_match = re.search(r"/title/(tt\d+)", path, flags=re.IGNORECASE)
        person_match = re.search(r"/name/(nm\d+)", path, flags=re.IGNORECASE)
        if title_match:
            imdb_id = title_match.group(1).lower()
            link_group = "knowledge"
            link_kind = "imdb"
            label = "IMDb"
            canonical_url = f"https://www.imdb.com/title/{imdb_id}/"
            metadata["imdb_id"] = imdb_id
            season_from_query = str((query.get("season") or [""])[0] or "").strip()
            if season_from_query.isdigit():
                season_candidate = int(season_from_query)
                season_row = context["seasons_by_number"].get(season_candidate)
                if season_row:
                    entity_type = "season"
                    entity_id = str(season_row.get("id"))
                    season_number = season_candidate
        elif person_match:
            imdb_id = person_match.group(1).lower()
            link_group = "knowledge"
            link_kind = "imdb"
            label = "IMDb"
            canonical_url = f"https://www.imdb.com/name/{imdb_id}/"
            metadata["imdb_id"] = imdb_id
            person_row = context["people_by_imdb"].get(imdb_id)
            if person_row:
                entity_type = "person"
                entity_id = str(person_row.get("id"))
                label = f"IMDb · {person_row.get('name') or 'Cast'}"

    elif "themoviedb.org" in host:
        season_match = re.search(r"/tv/(\d+)/(?:season|seasons)/(\d+)", path, flags=re.IGNORECASE)
        show_match = re.search(r"/tv/(\d+)", path, flags=re.IGNORECASE)
        person_match = re.search(r"/person/(\d+)", path, flags=re.IGNORECASE)
        if person_match:
            tmdb_id = person_match.group(1)
            link_group = "knowledge"
            link_kind = "tmdb"
            label = "TMDb"
            canonical_url = f"https://www.themoviedb.org/person/{tmdb_id}"
            metadata["tmdb_id"] = tmdb_id
            person_row = context["people_by_tmdb"].get(tmdb_id)
            if person_row:
                entity_type = "person"
                entity_id = str(person_row.get("id"))
                label = f"TMDb · {person_row.get('name') or 'Cast'}"
        elif season_match:
            show_tmdb_id = season_match.group(1)
            season_candidate = int(season_match.group(2))
            link_group = "knowledge"
            link_kind = "tmdb"
            label = f"TMDb Season {season_candidate}"
            canonical_url = f"https://www.themoviedb.org/tv/{show_tmdb_id}/season/{season_candidate}"
            metadata["tmdb_show_id"] = show_tmdb_id
            metadata["season_number"] = season_candidate
            season_row = context["seasons_by_number"].get(season_candidate)
            if season_row:
                entity_type = "season"
                entity_id = str(season_row.get("id"))
                season_number = season_candidate
        elif show_match:
            tmdb_id = show_match.group(1)
            link_group = "knowledge"
            link_kind = "tmdb"
            label = "TMDb"
            canonical_url = f"https://www.themoviedb.org/tv/{tmdb_id}"
            metadata["tmdb_id"] = tmdb_id

    elif "wikipedia.org" in host and "/wiki/" in path:
        link_group = "knowledge"
        link_kind = "wikipedia"
        label = "Wikipedia"
        resolved_wikipedia_url, wiki_title, wikipedia_error = _resolve_wikipedia_url(canonical_input)
        if wikipedia_error == "missing":
            return [], _WIKIPEDIA_MISSING_ARTICLE_DETAIL
        if wikipedia_error == "fetch_error":
            return [], _WIKIPEDIA_FETCH_ERROR_DETAIL
        if resolved_wikipedia_url:
            canonical_url = resolved_wikipedia_url
        wiki_title = str(wiki_title or "").strip()
        metadata["wikipedia_title"] = wiki_title
        season_candidate = _extract_season_number_from_text(wiki_title)
        if season_candidate is not None:
            season_row = context["seasons_by_number"].get(season_candidate)
            if season_row:
                entity_type = "season"
                entity_id = str(season_row.get("id"))
                season_number = season_candidate
        if entity_type == "show":
            person_row = _find_person_match_for_title(context, wiki_title)
            if person_row:
                entity_type = "person"
                entity_id = str(person_row.get("id"))
                label = f"Wikipedia · {person_row.get('name') or 'Cast'}"
        if entity_type == "show":
            expected_show_wikidata_id = _extract_wikidata_item_id(str(context.get("show_wikidata_id") or "").strip())
            if expected_show_wikidata_id:
                candidate_wikidata_id = _resolve_wikipedia_wikidata_id(canonical_url)
                if candidate_wikidata_id and candidate_wikidata_id != expected_show_wikidata_id:
                    return [], _WIKIPEDIA_SHOW_VARIANT_MISMATCH_DETAIL

    elif "wikidata.org" in host:
        wikidata_id = _extract_wikidata_item_id(canonical_input)
        if wikidata_id:
            link_group = "knowledge"
            link_kind = "wikidata"
            label = "Wikidata"
            canonical_url = f"https://www.wikidata.org/wiki/{wikidata_id}"
            metadata["wikidata_id"] = wikidata_id
            season_row = context["seasons_by_wikidata"].get(wikidata_id)
            person_row = context["people_by_wikidata"].get(wikidata_id)
            if season_row:
                entity_type = "season"
                entity_id = str(season_row.get("id"))
                season_number = int(season_row.get("season_number") or 0)
            elif person_row:
                entity_type = "person"
                entity_id = str(person_row.get("id"))
                label = f"Wikidata · {person_row.get('name') or 'Cast'}"

    elif "thetvdb.com" in host:
        link_group = "knowledge"
        link_kind = "tvdb"
        label = "TVDB"
        canonical_url = canonical_input

    elif "tvmaze.com" in host:
        link_group = "knowledge"
        link_kind = "tvmaze"
        label = "TVmaze"
        canonical_url = canonical_input

    elif "ratingraph.com" in host:
        link_group = "knowledge"
        link_kind = "ratinggraph"
        label = "RatingGraph"
        canonical_url = canonical_input

    elif "trakt.tv" in host:
        link_group = "knowledge"
        link_kind = "trakt"
        label = "Trakt.tv"
        canonical_url = canonical_input

    elif "famousbirthdays.com" in host:
        link_group = "knowledge"
        link_kind = "famous_birthdays"
        label = "Famous Birthdays"
        canonical_url = canonical_input

    elif "g.co" == host and re.search(r"^/kg/(m|g)/", path.lower()):
        link_group = "knowledge"
        link_kind = "freebase"
        label = "Freebase"
        canonical_url = canonical_input

    elif ("g.co" == host and path.lower().startswith("/kg")) or (
        "google.com" in host and "kgmid=" in canonical_input.lower()
    ):
        link_group = "knowledge"
        link_kind = "google_kg"
        label = "Google Knowledge Graph"
        canonical_url = canonical_input

    elif "x.com" in host and path.lower().startswith("/i/topics/"):
        link_group = "knowledge"
        link_kind = "x_topic"
        label = "X Topic"
        canonical_url = canonical_input

    elif host.endswith("fandom.com") or host.endswith("wikia.com"):
        link_group = "knowledge"
        link_kind = "fandom"
        wiki_title = _extract_wiki_slug_title(path)
        wiki_title_text = str(wiki_title or "").replace("_", " ").strip()
        if _is_fandom_seed_url(canonical_input):
            metadata["fandom_seed_domain"] = host
            metadata["fandom_seed_url"] = canonical_input
        metadata["fandom_title"] = wiki_title_text
        metadata["page_title"] = wiki_title_text
        if host:
            metadata["favicon_url"] = f"https://{host}/favicon.ico"
        label = wiki_title_text or "Fandom"
        season_candidate = _extract_season_number_from_text(wiki_title_text)
        if season_candidate is not None:
            season_row = context["seasons_by_number"].get(season_candidate)
            if season_row:
                entity_type = "season"
                entity_id = str(season_row.get("id"))
                season_number = season_candidate
                label = wiki_title_text or f"Season {season_candidate}"
        if entity_type == "show":
            person_row = _find_person_match_for_title(context, wiki_title_text)
            if person_row:
                entity_type = "person"
                entity_id = str(person_row.get("id"))
                label = wiki_title_text or str(person_row.get("name") or "Cast")
        if entity_type == "show":
            curated_domains = _curated_show_fandom_domains(context.get("show_name"))
            if curated_domains and host not in curated_domains:
                return [], "Fandom link is for a different community."

    elif "bravotv.com" in host:
        if not bool(context.get("is_bravo_show")):
            return [], "BravoTV links are only allowed for Bravo-network shows."
        profile_match = re.search(r"/people/([a-z0-9-]+)", path, flags=re.IGNORECASE)
        if profile_match:
            link_group = "official"
            link_kind = "bravo_profile"
            slug = _normalize_lookup_value(profile_match.group(1).replace("-", " "))
            person_row = context["people_by_slug"].get(slug) or _find_person_match_for_title(context, slug)
            if person_row:
                entity_type = "person"
                entity_id = str(person_row.get("id"))
                label = f"BravoTV · {person_row.get('name') or 'Profile'}"
            else:
                label = "BravoTV Profile"
        else:
            link_group = "official"
            link_kind = "official_page"
            label = "Official Page"

    elif "peacocktv.com" in host and path.lower().startswith("/blog/show/"):
        link_group = "cast_announcements"
        link_kind = "network_blog"
        label = "Peacock Blog"

    elif "nbc.com" in host and path.lower().startswith("/nbc-insider/franchise/"):
        link_group = "cast_announcements"
        link_kind = "network_blog"
        label = "NBC Insider"

    else:
        platform = infer_platform_from_url(canonical_input, fallback="")
        if platform in _SOCIAL_LINK_KINDS:
            link_group = "social"
            link_kind = "twitter" if platform == "x" else platform
            label = _LINK_KIND_LABELS.get(link_kind, link_kind.title())
            metadata["platform"] = link_kind
        else:
            link_group = "other"
            link_kind = "external"
            label = "External Link"

    primary_row = {
        "entity_type": entity_type,
        "entity_id": entity_id,
        "season_number": season_number,
        "link_group": link_group,
        "link_kind": _normalize_link_kind(link_kind),
        "label": label or _LINK_KIND_LABELS.get(_normalize_link_kind(link_kind), "External Link"),
        "url": canonical_url,
        "source": source,
        "metadata": metadata,
    }

    rows = [primary_row]
    if primary_row["link_kind"] in {"imdb", "tmdb", "wikipedia", "wikidata"}:
        rows.extend(
            _build_connected_knowledge_rows(
                context,
                entity_type=entity_type,
                entity_id=entity_id,
                season_number=season_number,
                primary_kind=primary_row["link_kind"],
                primary_url=canonical_url,
            )
        )
    return rows, None


_IMDB_PERSON_ID_RE = re.compile(r"nm\d+")
_TMDB_PERSON_ID_RE = re.compile(r"\d+")
_PERSON_SOURCE_LINK_KINDS = {"wikipedia", "wikidata", "fandom", "wikia", "imdb", "tmdb", "bravo_profile"}
_PERSON_SOCIAL_LINK_KINDS = set(_SOCIAL_LINK_KINDS)
_AUTO_APPROVE_KNOWLEDGE_LINK_KINDS = {
    "wikipedia",
    "wikidata",
    "fandom",
    "wikia",
    "imdb",
    "tmdb",
    "tvdb",
    "tvmaze",
    "ratinggraph",
    "freebase",
    "famous_birthdays",
    "google_kg",
    "trakt",
    "x_topic",
}
_IMDB_MISSING_PATTERNS = (
    "404 error",
    "requested url was not found",
    "no results found for",
    "page not found",
)
_TMDB_MISSING_PATTERNS = (
    "oops, we can't find that page",
    "the page you requested could not be found",
    "page not found",
)
_BRAVO_MISSING_PATTERNS = (
    "page not found",
    "we couldn't find this page",
    "oops",
)
_IMDB_CHALLENGE_PATTERNS = (
    "javascript is disabled",
    "please enable javascript",
    "reference id:",
    "security challenge",
    "captcha",
)
_TMDB_CHALLENGE_PATTERNS = (
    "verify you are human",
    "just a moment",
    "cloudflare",
    "captcha",
    "please enable javascript",
)
_INSTAGRAM_MISSING_PATTERNS = (
    "sorry, this page isn't available",
    "the link you followed may be broken",
    "page isn't available",
)
_TWITTER_MISSING_PATTERNS = (
    "this account doesn’t exist",
    "this account doesn't exist",
    "this page doesn’t exist",
    "this page doesn't exist",
)
_TIKTOK_MISSING_PATTERNS = (
    "couldn't find this account",
    "couldn’t find this account",
)
_YOUTUBE_MISSING_PATTERNS = (
    "this channel does not exist",
    "this account has been terminated",
)
_FACEBOOK_MISSING_PATTERNS = (
    "this content isn't available right now",
    "this page isn't available",
    "content not found",
)
_REDDIT_MISSING_PATTERNS = (
    "sorry, nobody on reddit goes by that name",
    "page not found",
    "sorry, there aren’t any communities on reddit with that name",
)


def _extract_imdb_person_id(value: str | None) -> str | None:
    candidate = str(value or "").strip()
    if not candidate:
        return None
    if _IMDB_PERSON_ID_RE.fullmatch(candidate):
        return candidate
    parsed = urlparse(candidate)
    if parsed.scheme and parsed.netloc:
        if "imdb.com" not in parsed.netloc.lower():
            return None
        path = unquote(parsed.path or "")
        match = re.search(r"/name/(nm\d+)", path, flags=re.IGNORECASE)
        if match:
            return match.group(1)
    return None


def _extract_tmdb_person_id(value: Any) -> str | None:
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        normalized = str(int(value))
        return normalized if _TMDB_PERSON_ID_RE.fullmatch(normalized) else None
    candidate = str(value or "").strip()
    if not candidate:
        return None
    if _TMDB_PERSON_ID_RE.fullmatch(candidate):
        return candidate
    parsed = urlparse(candidate)
    if parsed.scheme and parsed.netloc:
        if "themoviedb.org" not in parsed.netloc.lower():
            return None
        path = unquote(parsed.path or "")
        match = re.search(r"/person/(\d+)", path, flags=re.IGNORECASE)
        if match:
            return match.group(1)
    return None


def _normalize_social_handle(value: Any) -> str | None:
    candidate = str(value or "").strip()
    if not candidate:
        return None
    normalized = candidate.lstrip("@").strip()
    return normalized or None


def _wikidata_freebase_url(freebase_id: str) -> str:
    return f"https://g.co/kg{quote(str(freebase_id or '').strip(), safe='/')}"


def _wikidata_famous_birthdays_url(identifier: str) -> str:
    slug = str(identifier or "").strip()
    if slug.endswith(".html"):
        slug = slug[: -len(".html")]
    return f"https://www.famousbirthdays.com/{quote(slug)}.html"


def _wikidata_google_kg_url(identifier: str) -> str:
    return f"https://www.google.com/search?kgmid={quote(str(identifier or '').strip(), safe='/')}"


def _wikidata_trakt_url(identifier: str) -> str:
    slug = str(identifier or "").strip().lstrip("/")
    return f"https://trakt.tv/{quote(slug, safe='/')}"


def _wikidata_x_topic_url(identifier: str) -> str:
    return f"https://x.com/i/topics/{quote(str(identifier or '').strip())}"


def _social_url_from_platform_handle(platform: str, handle: str) -> str | None:
    normalized = _normalize_social_handle(handle)
    if not normalized:
        return None
    if platform == "twitter":
        return f"https://x.com/{normalized}"
    if platform == "instagram":
        return f"https://www.instagram.com/{normalized}/"
    if platform == "facebook":
        return f"https://www.facebook.com/{normalized}"
    if platform == "youtube":
        return f"https://www.youtube.com/channel/{normalized}"
    if platform == "tiktok":
        return f"https://www.tiktok.com/@{normalized}"
    if platform == "reddit":
        return f"https://www.reddit.com/user/{normalized}"
    return None


def _add_person_social_and_identifier_rows(
    *,
    found: list[dict[str, Any]],
    seen_keys: set[tuple[str, str, str]],
    person_id: str,
    person_name: str,
    source: str,
    freebase_id: str | None = None,
    famous_birthdays_id: str | None = None,
    google_kg_id: str | None = None,
    twitter_usernames: list[str] | None = None,
    instagram_usernames: list[str] | None = None,
    facebook_usernames: list[str] | None = None,
    youtube_channel_ids: list[str] | None = None,
    tiktok_usernames: list[str] | None = None,
    reddit_usernames: list[str] | None = None,
) -> None:
    def add_row(kind: str, label: str, url: str, *, link_group: str = "knowledge") -> None:
        canonical = _canonicalize_url(url)
        if not canonical:
            return
        if link_group == "social":
            validated_social_url = _validated_person_social_url(canonical, kind=kind)
            if not validated_social_url:
                return
            canonical = validated_social_url
        key = (person_id, kind, _url_key(canonical))
        if key in seen_keys:
            return
        seen_keys.add(key)
        found.append(
            {
                "entity_type": "person",
                "entity_id": person_id,
                "season_number": 0,
                "link_group": link_group,
                "link_kind": kind,
                "label": f"{person_name} {label}" if person_name else label,
                "url": canonical,
                "source": source,
                "status": "approved",
                "confidence": 0.95,
            }
        )

    freebase_value = str(freebase_id or "").strip()
    if freebase_value:
        add_row("freebase", "Freebase", _wikidata_freebase_url(freebase_value))
    famous_birthdays_value = str(famous_birthdays_id or "").strip()
    if famous_birthdays_value:
        add_row("famous_birthdays", "Famous Birthdays", _wikidata_famous_birthdays_url(famous_birthdays_value))
    google_kg_value = str(google_kg_id or "").strip()
    if google_kg_value:
        add_row("google_kg", "Google Knowledge Graph", _wikidata_google_kg_url(google_kg_value))

    for platform, handles in (
        ("twitter", twitter_usernames or []),
        ("instagram", instagram_usernames or []),
        ("facebook", facebook_usernames or []),
        ("youtube", youtube_channel_ids or []),
        ("tiktok", tiktok_usernames or []),
        ("reddit", reddit_usernames or []),
    ):
        for handle in handles:
            social_url = _social_url_from_platform_handle(platform, str(handle or ""))
            if not social_url:
                continue
            add_row(platform, _LINK_KIND_LABELS.get(platform, platform.title()), social_url, link_group="social")


def _normalize_tmdb_external_id_value(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _fetch_tmdb_external_ids_payload(tmdb_person_id: str) -> dict[str, str]:
    tmdb_value = str(tmdb_person_id or "").strip()
    if not re.fullmatch(r"\d+", tmdb_value):
        return {}
    try:
        from trr_backend.integrations.tmdb_person import fetch_tmdb_external_ids
    except Exception:  # noqa: BLE001
        return {}

    try:
        payload = fetch_tmdb_external_ids(int(tmdb_value))
    except Exception:  # noqa: BLE001
        return {}
    if payload is None:
        return {}

    return {
        "imdb_id": str(payload.imdb_id or "").strip(),
        "wikidata_id": str(payload.wikidata_id or "").strip(),
        "freebase_id": str(payload.freebase_id or "").strip(),
        "freebase_mid": str(payload.freebase_mid or "").strip(),
        "facebook_id": str(payload.facebook_id or "").strip(),
        "instagram_id": str(payload.instagram_id or "").strip(),
        "tiktok_id": str(payload.tiktok_id or "").strip(),
        "twitter_id": str(payload.twitter_id or "").strip(),
        "youtube_id": str(payload.youtube_id or "").strip(),
    }


def _persist_tmdb_external_ids_for_person(person_id: str, tmdb_person_id: str, payload: dict[str, str]) -> None:
    person_value = str(person_id or "").strip()
    tmdb_value = str(tmdb_person_id or "").strip()
    if not person_value or not re.fullmatch(r"\d+", tmdb_value):
        return
    if not isinstance(payload, dict):
        return

    pg.execute_returning(
        """
        INSERT INTO core.cast_tmdb (
          person_id,
          tmdb_id,
          imdb_id,
          freebase_mid,
          freebase_id,
          wikidata_id,
          facebook_id,
          instagram_id,
          tiktok_id,
          twitter_id,
          youtube_id,
          fetched_at
        )
        VALUES (
          %s::uuid,
          %s::int,
          NULLIF(%s, ''),
          NULLIF(%s, ''),
          NULLIF(%s, ''),
          NULLIF(%s, ''),
          NULLIF(%s, ''),
          NULLIF(%s, ''),
          NULLIF(%s, ''),
          NULLIF(%s, ''),
          NULLIF(%s, ''),
          NOW()
        )
        ON CONFLICT (person_id)
        DO UPDATE SET
          tmdb_id = EXCLUDED.tmdb_id,
          imdb_id = COALESCE(NULLIF(EXCLUDED.imdb_id, ''), core.cast_tmdb.imdb_id),
          freebase_mid = COALESCE(NULLIF(EXCLUDED.freebase_mid, ''), core.cast_tmdb.freebase_mid),
          freebase_id = COALESCE(NULLIF(EXCLUDED.freebase_id, ''), core.cast_tmdb.freebase_id),
          wikidata_id = COALESCE(NULLIF(EXCLUDED.wikidata_id, ''), core.cast_tmdb.wikidata_id),
          facebook_id = COALESCE(NULLIF(EXCLUDED.facebook_id, ''), core.cast_tmdb.facebook_id),
          instagram_id = COALESCE(NULLIF(EXCLUDED.instagram_id, ''), core.cast_tmdb.instagram_id),
          tiktok_id = COALESCE(NULLIF(EXCLUDED.tiktok_id, ''), core.cast_tmdb.tiktok_id),
          twitter_id = COALESCE(NULLIF(EXCLUDED.twitter_id, ''), core.cast_tmdb.twitter_id),
          youtube_id = COALESCE(NULLIF(EXCLUDED.youtube_id, ''), core.cast_tmdb.youtube_id),
          fetched_at = NOW()
        RETURNING person_id
        """,
        [
            person_value,
            tmdb_value,
            str(payload.get("imdb_id") or ""),
            str(payload.get("freebase_mid") or ""),
            str(payload.get("freebase_id") or ""),
            str(payload.get("wikidata_id") or ""),
            str(payload.get("facebook_id") or ""),
            str(payload.get("instagram_id") or ""),
            str(payload.get("tiktok_id") or ""),
            str(payload.get("twitter_id") or ""),
            str(payload.get("youtube_id") or ""),
        ],
    )


def _extract_bravo_person_slug(value: str | None) -> str | None:
    candidate = str(value or "").strip()
    if not candidate:
        return None
    parsed = urlparse(candidate)
    if parsed.scheme and parsed.netloc:
        if "bravotv.com" not in parsed.netloc.lower():
            return None
        path = unquote(parsed.path or "").strip()
        match = re.search(r"/people/([a-z0-9-]+)", path, flags=re.IGNORECASE)
        return match.group(1).lower() if match else None
    slug = _slug(candidate)
    return slug if slug else None


def _fetch_html_with_status(
    url: str,
    *,
    timeout: float = 20.0,
) -> tuple[int | None, str | None, str | None, str | None]:
    request = urllib.request.Request(
        url,
        headers={
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "user-agent": "TRR-Backend/1.0",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read() or b""
            charset = response.headers.get_content_charset() or "utf-8"
            html = raw.decode(charset, errors="replace")
            return int(response.getcode() or 200), html, str(response.geturl() or url), None
    except urllib.error.HTTPError as exc:
        raw = exc.read() or b""
        charset = exc.headers.get_content_charset() if exc.headers else None
        html = raw.decode(charset or "utf-8", errors="replace")
        return int(exc.code), html or None, str(exc.geturl() or url), None
    except Exception as exc:  # noqa: BLE001
        return None, None, None, str(exc)


def _is_missing_imdb_person_page(html: str, resolved_url: str, imdb_id: str) -> bool:
    path = unquote(urlparse(resolved_url).path or "")
    match = re.search(r"/name/(nm\d+)", path, flags=re.IGNORECASE)
    if not match:
        return True
    if match.group(1).lower() != imdb_id.lower():
        return True
    lowered = (html or "").casefold()
    return any(marker in lowered for marker in _IMDB_MISSING_PATTERNS)


def _is_missing_tmdb_person_page(html: str, resolved_url: str, tmdb_id: str) -> bool:
    path = unquote(urlparse(resolved_url).path or "")
    match = re.search(r"/person/(\d+)", path, flags=re.IGNORECASE)
    if not match:
        return True
    if match.group(1) != tmdb_id:
        return True
    lowered = (html or "").casefold()
    return any(marker in lowered for marker in _TMDB_MISSING_PATTERNS)


def _is_missing_bravo_person_page(html: str, resolved_url: str) -> bool:
    path = unquote(urlparse(resolved_url).path or "").strip().lower()
    if not path.startswith("/people/"):
        return True
    lowered = (html or "").casefold()
    return any(marker in lowered for marker in _BRAVO_MISSING_PATTERNS)


def _is_access_challenge_page(html: str, *, status_code: int | None, markers: tuple[str, ...]) -> bool:
    lowered = (html or "").casefold()
    if not lowered:
        return False
    if not any(marker in lowered for marker in markers):
        return False
    if status_code in {202, 401, 403, 429, 503}:
        return True
    return any(
        marker in lowered
        for marker in (
            "captcha",
            "cloudflare",
            "verify you are human",
            "security challenge",
            "reference id:",
        )
    )


def _is_missing_social_profile_page(kind: str, html: str, resolved_url: str) -> bool:
    normalized_kind = _normalize_link_kind(kind)
    lowered = (html or "").casefold()
    if not lowered:
        return False

    parsed = urlparse(resolved_url)
    path = unquote(parsed.path or "").strip().lower()
    if normalized_kind == "instagram":
        return any(marker in lowered for marker in _INSTAGRAM_MISSING_PATTERNS)
    if normalized_kind == "twitter":
        return any(marker in lowered for marker in _TWITTER_MISSING_PATTERNS)
    if normalized_kind == "tiktok":
        return any(marker in lowered for marker in _TIKTOK_MISSING_PATTERNS)
    if normalized_kind == "youtube":
        return any(marker in lowered for marker in _YOUTUBE_MISSING_PATTERNS)
    if normalized_kind == "facebook":
        return any(marker in lowered for marker in _FACEBOOK_MISSING_PATTERNS)
    if normalized_kind == "reddit":
        return any(marker in lowered for marker in _REDDIT_MISSING_PATTERNS) or path.startswith("/user/deleted")
    if normalized_kind == "threads":
        return "sorry, this page isn't available" in lowered
    return False


def _validate_person_social_url(
    url: str,
    *,
    kind: str,
) -> tuple[str | None, Literal["valid", "invalid", "fetch_error"]]:
    candidate = _canonicalize_url(str(url or "").strip())
    if not candidate:
        return None, "invalid"
    normalized_kind = _normalize_link_kind(kind)
    if normalized_kind not in _PERSON_SOCIAL_LINK_KINDS:
        return None, "invalid"

    inferred_platform = infer_platform_from_url(candidate, fallback="")
    if inferred_platform == "x":
        inferred_platform = "twitter"
    if inferred_platform and inferred_platform != normalized_kind:
        return None, "invalid"

    timeout_source = normalized_kind if normalized_kind in _PERSON_SOCIAL_LINK_KINDS else "social"
    status_code, html, final_url, fetch_error = _fetch_html_with_status(
        candidate,
        timeout=_source_timeout_seconds(timeout_source, default=15.0),
    )
    if status_code is None:
        return (None, "fetch_error") if fetch_error else (None, "invalid")

    resolved = _canonicalize_url(final_url or candidate)
    if html and _is_missing_social_profile_page(normalized_kind, html, resolved):
        return None, "invalid"
    if status_code in {404, 410}:
        return None, "invalid"
    if status_code >= 500:
        return None, "fetch_error"
    if status_code in {401, 403, 429}:
        return None, "fetch_error"
    if status_code >= 400:
        return None, "invalid"
    if not html:
        return (None, "fetch_error") if fetch_error else (None, "invalid")
    return resolved, "valid"


@lru_cache(maxsize=4096)
def _validated_person_social_url(
    url: str,
    *,
    kind: str,
) -> str | None:
    resolved, outcome = _validate_person_social_url(url, kind=kind)
    if outcome != "valid":
        return None
    return resolved


@lru_cache(maxsize=256)
def _resolve_wikidata_enwiki_url(wikidata_id: str) -> str | None:
    item_id = str(wikidata_id or "").strip()
    if not re.fullmatch(r"Q\d+", item_id):
        return None
    summary, fetch_error = _fetch_wikidata_summary(item_id)
    if fetch_error or not summary:
        return None
    return summary.get("enwiki_url")


def _extract_wikidata_claim_scalar(entity: dict[str, Any], property_ids: tuple[str, ...]) -> str | None:
    claims = entity.get("claims") if isinstance(entity.get("claims"), dict) else {}
    for property_id in property_ids:
        claim_rows = claims.get(property_id) if isinstance(claims, dict) else None
        if not isinstance(claim_rows, list):
            continue
        for claim_row in claim_rows:
            if not isinstance(claim_row, dict):
                continue
            mainsnak = claim_row.get("mainsnak") if isinstance(claim_row.get("mainsnak"), dict) else {}
            if str(mainsnak.get("snaktype") or "value") != "value":
                continue
            datavalue = mainsnak.get("datavalue") if isinstance(mainsnak.get("datavalue"), dict) else {}
            value = datavalue.get("value")
            if isinstance(value, str):
                candidate = value.strip()
                if candidate:
                    return candidate
            if isinstance(value, (int, float)):
                return str(int(value))
    return None


def _extract_wikidata_claim_scalars(entity: dict[str, Any], property_ids: tuple[str, ...]) -> list[str]:
    claims = entity.get("claims") if isinstance(entity.get("claims"), dict) else {}
    values: list[str] = []
    seen: set[str] = set()
    for property_id in property_ids:
        claim_rows = claims.get(property_id) if isinstance(claims, dict) else None
        if not isinstance(claim_rows, list):
            continue
        for claim_row in claim_rows:
            if not isinstance(claim_row, dict):
                continue
            mainsnak = claim_row.get("mainsnak") if isinstance(claim_row.get("mainsnak"), dict) else {}
            if str(mainsnak.get("snaktype") or "value") != "value":
                continue
            datavalue = mainsnak.get("datavalue") if isinstance(mainsnak.get("datavalue"), dict) else {}
            value = datavalue.get("value")
            candidate = ""
            if isinstance(value, str):
                candidate = value.strip()
            elif isinstance(value, (int, float)):
                candidate = str(int(value))
            if not candidate:
                continue
            key = candidate.lower()
            if key in seen:
                continue
            seen.add(key)
            values.append(candidate)
    return values


def _extract_wikidata_claim_entity_ids(entity: dict[str, Any], property_ids: tuple[str, ...]) -> list[str]:
    claims = entity.get("claims") if isinstance(entity.get("claims"), dict) else {}
    found: list[str] = []
    seen: set[str] = set()
    for property_id in property_ids:
        claim_rows = claims.get(property_id) if isinstance(claims, dict) else None
        if not isinstance(claim_rows, list):
            continue
        for claim_row in claim_rows:
            if not isinstance(claim_row, dict):
                continue
            mainsnak = claim_row.get("mainsnak") if isinstance(claim_row.get("mainsnak"), dict) else {}
            if str(mainsnak.get("snaktype") or "value") != "value":
                continue
            datavalue = mainsnak.get("datavalue") if isinstance(mainsnak.get("datavalue"), dict) else {}
            value = datavalue.get("value")
            if isinstance(value, dict):
                entity_id = str(value.get("id") or "").strip()
                if not entity_id:
                    numeric_id = value.get("numeric-id")
                    if isinstance(numeric_id, int):
                        entity_id = f"Q{numeric_id}"
                if re.fullmatch(r"Q\d+", entity_id) and entity_id not in seen:
                    seen.add(entity_id)
                    found.append(entity_id)
            elif isinstance(value, str):
                entity_id = value.strip()
                if re.fullmatch(r"Q\d+", entity_id) and entity_id not in seen:
                    seen.add(entity_id)
                    found.append(entity_id)
    return found


@lru_cache(maxsize=512)
def _fetch_wikidata_summary(wikidata_id: str) -> tuple[dict[str, Any] | None, bool]:
    item_id = str(wikidata_id or "").strip()
    if not re.fullmatch(r"Q\d+", item_id):
        return None, False
    request = urllib.request.Request(
        f"https://www.wikidata.org/wiki/Special:EntityData/{item_id}.json",
        headers={"accept": "application/json", "user-agent": "TRR-Backend/1.0"},
    )
    try:
        with urllib.request.urlopen(request, timeout=_source_timeout_seconds("wikidata")) as response:
            payload = json.loads((response.read() or b"{}").decode("utf-8", errors="replace"))
    except Exception:  # noqa: BLE001
        return None, True

    entities = payload.get("entities") if isinstance(payload, dict) else None
    entity = entities.get(item_id) if isinstance(entities, dict) else None
    if not isinstance(entity, dict):
        return None, False

    sitelinks = entity.get("sitelinks") if isinstance(entity.get("sitelinks"), dict) else {}
    enwiki = sitelinks.get("enwiki") if isinstance(sitelinks.get("enwiki"), dict) else {}
    enwiki_title = str(enwiki.get("title") or "").strip()

    labels = entity.get("labels") if isinstance(entity.get("labels"), dict) else {}
    en_label_payload = labels.get("en") if isinstance(labels.get("en"), dict) else {}
    en_label = str(en_label_payload.get("value") or "").strip()

    imdb_id = _extract_wikidata_claim_scalar(entity, ("P345",))
    tmdb_tv_id = _extract_wikidata_claim_scalar(entity, ("P4983",))
    tmdb_person_id = _extract_wikidata_claim_scalar(entity, ("P4985",))
    tvdb_id = _extract_wikidata_claim_scalar(entity, ("P4835",))
    tvmaze_show_id = _extract_wikidata_claim_scalar(entity, ("P8600",))
    tvmaze_season_id = _extract_wikidata_claim_scalar(entity, ("P10669",))
    tvdb_season_id = _extract_wikidata_claim_scalar(entity, ("P12397",))
    ratinggraph_tv_show_id = _extract_wikidata_claim_scalar(entity, ("P12544",))
    freebase_id = _extract_wikidata_claim_scalar(entity, ("P646",))
    famous_birthdays_id = _extract_wikidata_claim_scalar(entity, ("P11194",))
    google_kg_id = _extract_wikidata_claim_scalar(entity, ("P2671",))
    trakt_id = _extract_wikidata_claim_scalar(entity, ("P8013",))
    x_topic_id = _extract_wikidata_claim_scalar(entity, ("P8672",))
    twitter_usernames = _extract_wikidata_claim_scalars(entity, ("P2002",))
    instagram_usernames = _extract_wikidata_claim_scalars(entity, ("P2003",))
    facebook_usernames = _extract_wikidata_claim_scalars(entity, ("P2013",))
    youtube_channel_ids = _extract_wikidata_claim_scalars(entity, ("P2397",))
    tiktok_usernames = _extract_wikidata_claim_scalars(entity, ("P7085",))
    reddit_usernames = _extract_wikidata_claim_scalars(entity, ("P4265",))
    season_item_ids = _extract_wikidata_claim_entity_ids(entity, ("P527",))
    cast_item_ids = _extract_wikidata_claim_entity_ids(entity, ("P161", "P371"))
    part_of_series_item_ids = _extract_wikidata_claim_entity_ids(entity, ("P179",))
    enwiki_url = f"https://en.wikipedia.org/wiki/{quote(enwiki_title.replace(' ', '_'))}" if enwiki_title else ""

    return (
        {
            "item_id": item_id,
            "label": en_label,
            "enwiki_title": enwiki_title,
            "enwiki_url": enwiki_url,
            "imdb_id": str(imdb_id or "").strip(),
            "tmdb_tv_id": str(tmdb_tv_id or "").strip(),
            "tmdb_person_id": str(tmdb_person_id or "").strip(),
            "tvdb_id": str(tvdb_id or "").strip(),
            "tvmaze_show_id": str(tvmaze_show_id or "").strip(),
            "tvmaze_season_id": str(tvmaze_season_id or "").strip(),
            "tvdb_season_id": str(tvdb_season_id or "").strip(),
            "ratinggraph_tv_show_id": str(ratinggraph_tv_show_id or "").strip(),
            "freebase_id": str(freebase_id or "").strip(),
            "famous_birthdays_id": str(famous_birthdays_id or "").strip(),
            "google_kg_id": str(google_kg_id or "").strip(),
            "trakt_id": str(trakt_id or "").strip(),
            "x_topic_id": str(x_topic_id or "").strip(),
            "twitter_usernames": twitter_usernames,
            "instagram_usernames": instagram_usernames,
            "facebook_usernames": facebook_usernames,
            "youtube_channel_ids": youtube_channel_ids,
            "tiktok_usernames": tiktok_usernames,
            "reddit_usernames": reddit_usernames,
            "season_item_ids": season_item_ids,
            "cast_item_ids": cast_item_ids,
            "part_of_series_item_ids": part_of_series_item_ids,
        },
        False,
    )


@lru_cache(maxsize=1024)
def _normalized_person_name(value: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(value or "").lower()).strip()


def _person_name_candidates_match(expected_name: str | None, candidates: list[str | None]) -> bool:
    expected = _normalized_person_name(expected_name)
    if not expected:
        return True
    expected_tokens = {token for token in expected.split() if token}
    if not expected_tokens:
        return False

    for candidate in candidates:
        normalized = _normalized_person_name(candidate)
        if not normalized:
            continue
        if normalized == expected:
            return True
        candidate_tokens = {token for token in normalized.split() if token}
        if expected_tokens.issubset(candidate_tokens):
            return True
    return False


def _extract_person_page_name_candidates(html: str, resolved_url: str) -> set[str]:
    candidates: set[str] = set()

    path = unquote(urlparse(resolved_url).path or "")
    slug = ""
    if "/wiki/" in path:
        slug = path.split("/wiki/", 1)[1]
    elif "/" in path:
        slug = path.rsplit("/", 1)[-1]
    if slug:
        clean_slug = re.sub(r"\s*\(.*?\)\s*$", "", slug.replace("_", " ")).strip()
        normalized = _normalized_person_name(clean_slug)
        if normalized:
            candidates.add(normalized)

    soup = BeautifulSoup(html or "", "html.parser")
    heading = soup.select_one("h1")
    if heading is not None:
        heading_text = re.sub(r"\[\s*\d+\s*]", "", heading.get_text(" ", strip=True))
        heading_text = re.sub(r"\s*\(.*?\)\s*$", "", heading_text).strip()
        normalized = _normalized_person_name(heading_text)
        if normalized:
            candidates.add(normalized)

    if soup.title is not None:
        title_text = soup.title.get_text(" ", strip=True)
        head = re.split(r"\s+[-|]\s+", title_text, maxsplit=1)[0].strip()
        head = re.sub(r"\s*\(.*?\)\s*$", "", head).strip()
        normalized = _normalized_person_name(head)
        if normalized:
            candidates.add(normalized)

    og_title = soup.select_one('meta[property="og:title"]')
    if og_title is not None:
        og_title_text = str(og_title.get("content") or "").strip()
        head = re.split(r"\s+[-|]\s+", og_title_text, maxsplit=1)[0].strip()
        head = re.sub(r"\s*\(.*?\)\s*$", "", head).strip()
        normalized = _normalized_person_name(head)
        if normalized:
            candidates.add(normalized)

    return candidates


def _person_page_matches_expected_name(expected_name: str | None, html: str, resolved_url: str) -> bool:
    candidates = list(_extract_person_page_name_candidates(html, resolved_url))
    return _person_name_candidates_match(expected_name, candidates)


def _extract_wikidata_item_id(value: str | None) -> str | None:
    candidate = str(value or "").strip()
    if not candidate:
        return None
    parsed = urlparse(candidate)
    if parsed.scheme and parsed.netloc:
        path = unquote(parsed.path or "").strip()
        if "/wiki/" in path:
            candidate = path.split("/wiki/", 1)[1].strip()
        else:
            candidate = path.rsplit("/", 1)[-1].strip()
    return candidate if re.fullmatch(r"Q\d+", candidate) else None


def _extract_wikipedia_title(value: str | None) -> str | None:
    candidate = str(value or "").strip()
    if not candidate:
        return None
    parsed = urlparse(candidate)
    if parsed.scheme and parsed.netloc:
        if "wikipedia.org" not in parsed.netloc.lower():
            return None
        path = unquote(parsed.path or "").strip()
        if "/wiki/" not in path:
            return None
        candidate = path.split("/wiki/", 1)[1].strip()

    candidate = candidate.split("#", 1)[0].split("?", 1)[0].strip()
    if not candidate or candidate.lower().startswith("special:"):
        return None
    return candidate.replace("_", " ")


@lru_cache(maxsize=2048)
def _fetch_wikipedia_page_summary(value: str) -> tuple[dict[str, str] | None, bool]:
    title = _extract_wikipedia_title(value)
    if not title:
        return None, False

    request = urllib.request.Request(
        (
            "https://en.wikipedia.org/w/api.php?action=query&format=json&redirects=1"
            f"&prop=info&inprop=url&titles={quote(title)}"
        ),
        headers={"accept": "application/json", "user-agent": "TRR-Backend/1.0"},
    )
    try:
        with urllib.request.urlopen(request, timeout=_source_timeout_seconds("wikipedia")) as response:
            payload = json.loads((response.read() or b"{}").decode("utf-8", errors="replace"))
    except Exception:  # noqa: BLE001
        return None, True

    query = payload.get("query") if isinstance(payload, dict) else None
    pages = query.get("pages") if isinstance(query, dict) else None
    if not isinstance(pages, dict) or not pages:
        return None, False

    first_page = next(iter(pages.values()))
    if not isinstance(first_page, dict):
        return None, False
    if first_page.get("missing") is not None:
        return None, False

    canonical_title = str(first_page.get("title") or "").strip()
    if not canonical_title:
        return None, False
    canonical_url = str(first_page.get("fullurl") or "").strip()
    if not canonical_url:
        canonical_url = f"https://en.wikipedia.org/wiki/{quote(canonical_title.replace(' ', '_'))}"

    return {
        "title": canonical_title,
        "url": canonical_url,
    }, False


@lru_cache(maxsize=2048)
def _resolve_wikipedia_wikidata_id(value: str) -> str | None:
    title = _extract_wikipedia_title(value)
    if not title:
        return None

    request = urllib.request.Request(
        (
            "https://en.wikipedia.org/w/api.php?action=query&format=json&redirects=1"
            f"&prop=pageprops&titles={quote(title)}"
        ),
        headers={"accept": "application/json", "user-agent": "TRR-Backend/1.0"},
    )
    try:
        with urllib.request.urlopen(request, timeout=_source_timeout_seconds("wikipedia")) as response:
            payload = json.loads((response.read() or b"{}").decode("utf-8", errors="replace"))
    except Exception:  # noqa: BLE001
        return None

    query = payload.get("query") if isinstance(payload, dict) else None
    pages = query.get("pages") if isinstance(query, dict) else None
    if not isinstance(pages, dict) or not pages:
        return None

    first_page = next(iter(pages.values()))
    if not isinstance(first_page, dict):
        return None
    if first_page.get("missing") is not None:
        return None

    pageprops = first_page.get("pageprops") if isinstance(first_page.get("pageprops"), dict) else {}
    item_id = str(pageprops.get("wikibase_item") or "").strip()
    return item_id if re.fullmatch(r"Q\d+", item_id) else None


def _resolve_wikipedia_url(value: str) -> tuple[str | None, str | None, str | None]:
    canonical_candidate = _canonicalize_url(value)
    if not canonical_candidate:
        return None, None, "invalid"

    summary, summary_fetch_error = _fetch_wikipedia_page_summary(canonical_candidate)
    if summary and summary.get("url"):
        canonical_candidate = _canonicalize_url(str(summary.get("url")))
    summary_title = summary.get("title") if isinstance(summary, dict) else None
    wiki_title = str(summary_title or _extract_wikipedia_title(canonical_candidate) or "").strip() or None

    if summary:
        return canonical_candidate, wiki_title, None

    if not summary and not summary_fetch_error:
        return None, wiki_title, "missing"

    status_code, html, final_url, fetch_error = _fetch_html_with_status(
        canonical_candidate,
        timeout=_source_timeout_seconds("wikipedia"),
    )
    if status_code is None or fetch_error:
        return None, wiki_title, "fetch_error"
    if status_code in {404, 410}:
        return None, wiki_title, "missing"
    if status_code >= 500:
        return None, wiki_title, "fetch_error"
    if status_code >= 400:
        return None, wiki_title, "invalid"
    if not html:
        return None, wiki_title, "fetch_error"

    resolved = _canonicalize_url(final_url or canonical_candidate)
    if is_missing_wikipedia_page(html, resolved):
        return None, wiki_title, "missing"
    return resolved, wiki_title, None


def _load_show_wikidata_id(show_id: str) -> str | None:
    row = pg.fetch_one(
        """
        SELECT wikidata_id
        FROM core.shows
        WHERE id = %s
        LIMIT 1
        """,
        [show_id],
    )
    if not row:
        return _load_show_wikidata_id_from_links(show_id)
    return _resolve_show_wikidata_id(show_id, row.get("wikidata_id"))


def _validate_person_knowledge_url(
    url: str,
    *,
    kind: str,
    expected_name: str | None = None,
    fandom_allowlist: list[str] | tuple[str, ...] | None = None,
) -> tuple[str | None, Literal["valid", "invalid", "fetch_error"]]:
    candidate = str(url or "").strip()
    if not candidate:
        return None, "invalid"
    normalized_kind = _normalize_link_kind(kind)
    if not normalized_kind:
        return None, "invalid"
    resolved_allowlist = fandom_allowlist if fandom_allowlist is not None else load_fandom_community_allowlist()

    if normalized_kind == "imdb":
        imdb_id = _extract_imdb_person_id(candidate)
        if not imdb_id:
            return None, "invalid"
        canonical_url = f"https://www.imdb.com/name/{imdb_id}/"
        status_code, html, final_url, _ = _fetch_html_with_status(
            canonical_url,
            timeout=_source_timeout_seconds("imdb"),
        )
        if status_code is None:
            return None, "fetch_error"
        if status_code in {404, 410}:
            return None, "invalid"
        if status_code >= 500:
            return None, "fetch_error"
        if not html:
            return (None, "fetch_error") if status_code >= 400 else (None, "invalid")
        resolved = final_url or canonical_url
        if _is_missing_imdb_person_page(html, resolved, imdb_id):
            return None, "invalid"
        owner_match = _person_page_matches_expected_name(expected_name, html, resolved) if expected_name else True
        is_challenge = _is_access_challenge_page(
            html,
            status_code=status_code,
            markers=_IMDB_CHALLENGE_PATTERNS,
        )
        if is_challenge:
            # Challenge pages are treated as valid only when identity is still strongly verifiable.
            # If we expect a specific owner and cannot confirm it, keep the link unverifiable.
            if expected_name and not owner_match:
                return None, "fetch_error"
            if not _is_missing_imdb_person_page(html, resolved, imdb_id):
                return canonical_url, "valid"
            return None, "fetch_error"
        if status_code >= 400:
            return None, "invalid"
        if expected_name and not owner_match:
            return None, "invalid"
        return canonical_url, "valid"

    if normalized_kind == "tmdb":
        tmdb_id = _extract_tmdb_person_id(candidate)
        if not tmdb_id:
            return None, "invalid"
        canonical_url = f"https://www.themoviedb.org/person/{tmdb_id}"
        status_code, html, final_url, _ = _fetch_html_with_status(
            canonical_url,
            timeout=_source_timeout_seconds("tmdb"),
        )
        if status_code is None:
            return None, "fetch_error"
        if status_code in {404, 410}:
            return None, "invalid"
        if status_code >= 500:
            return None, "fetch_error"
        if not html:
            return (None, "fetch_error") if status_code >= 400 else (None, "invalid")
        resolved = final_url or canonical_url
        if _is_missing_tmdb_person_page(html, resolved, tmdb_id):
            return None, "invalid"
        owner_match = _person_page_matches_expected_name(expected_name, html, resolved) if expected_name else True
        is_challenge = _is_access_challenge_page(
            html,
            status_code=status_code,
            markers=_TMDB_CHALLENGE_PATTERNS,
        )
        if is_challenge:
            # Challenge pages are treated as valid only when identity is still strongly verifiable.
            # If we expect a specific owner and cannot confirm it, keep the link unverifiable.
            if expected_name and not owner_match:
                return None, "fetch_error"
            if not _is_missing_tmdb_person_page(html, resolved, tmdb_id):
                return canonical_url, "valid"
            return None, "fetch_error"
        if status_code >= 400:
            return None, "invalid"
        if expected_name and not owner_match:
            return None, "invalid"
        return canonical_url, "valid"

    if normalized_kind == "bravo_profile":
        bravo_slug = _extract_bravo_person_slug(candidate)
        if not bravo_slug:
            return None, "invalid"
        canonical_url = f"https://www.bravotv.com/people/{bravo_slug}"
        status_code, html, final_url, _ = _fetch_html_with_status(
            canonical_url,
            timeout=_source_timeout_seconds("bravo"),
        )
        if status_code is None:
            return None, "fetch_error"
        if status_code in {404, 410}:
            return None, "invalid"
        if status_code >= 500:
            return None, "fetch_error"
        if status_code >= 400:
            return None, "invalid"
        if not html:
            return None, "invalid"
        resolved = final_url or canonical_url
        if _is_missing_bravo_person_page(html, resolved):
            return None, "invalid"
        if expected_name and not _person_page_matches_expected_name(expected_name, html, resolved):
            return None, "invalid"
        return canonical_url, "valid"

    if normalized_kind == "wikidata":
        item_id = _extract_wikidata_item_id(candidate)
        if not item_id:
            return None, "invalid"
        summary, fetch_error = _fetch_wikidata_summary(item_id)
        if fetch_error:
            return None, "fetch_error"
        if not summary:
            return None, "invalid"
        if expected_name and not _person_name_candidates_match(
            expected_name,
            [summary.get("label"), summary.get("enwiki_title")],
        ):
            return None, "invalid"
        return f"https://www.wikidata.org/wiki/{item_id}", "valid"
    if normalized_kind == "wikipedia":
        summary, summary_fetch_error = _fetch_wikipedia_page_summary(candidate)
        if not summary_fetch_error:
            if not summary:
                return None, "invalid"
            if expected_name and not _person_name_candidates_match(expected_name, [summary.get("title")]):
                return None, "invalid"
            return str(summary.get("url") or candidate), "valid"

    if normalized_kind == "fandom" and not is_allowlisted_fandom_domain(
        candidate,
        allowlist=resolved_allowlist,
    ):
        return None, "invalid"

    timeout_source = (
        "wikipedia"
        if normalized_kind == "wikipedia"
        else "fandom"
        if normalized_kind == "fandom"
        else "wikidata"
    )
    status_code, html, final_url, error = _fetch_html_with_status(
        candidate,
        timeout=_source_timeout_seconds(timeout_source),
    )
    if status_code is None:
        return (None, "fetch_error") if error else (None, "invalid")
    if status_code in {404, 410}:
        return None, "invalid"
    if status_code >= 500:
        return None, "fetch_error"
    if status_code >= 400:
        return None, "invalid"
    if not html:
        return (None, "fetch_error") if error else (None, "invalid")
    resolved = _canonicalize_url(final_url or candidate)
    if normalized_kind == "wikipedia" and is_missing_wikipedia_page(html, resolved):
        return None, "invalid"
    if normalized_kind == "fandom" and is_missing_fandom_page(html, resolved):
        return None, "invalid"
    if normalized_kind == "fandom" and not is_allowlisted_fandom_domain(
        resolved,
        allowlist=resolved_allowlist,
    ):
        return None, "invalid"
    if expected_name and not _person_page_matches_expected_name(expected_name, html, resolved):
        return None, "invalid"
    return resolved, "valid"


@lru_cache(maxsize=1024)
def _validated_person_knowledge_url(
    url: str,
    *,
    kind: str,
    expected_name: str | None = None,
    fandom_allowlist: list[str] | tuple[str, ...] | None = None,
) -> str | None:
    resolved, outcome = _validate_person_knowledge_url(
        url,
        kind=kind,
        expected_name=expected_name,
        fandom_allowlist=fandom_allowlist,
    )
    if outcome != "valid":
        return None
    return resolved


def _load_preapproved_person_source_url(
    *,
    person_id: str,
    link_kind: str,
    candidate_url: str,
) -> str | None:
    normalized_kind = _normalize_link_kind(link_kind)
    if normalized_kind not in {"imdb", "tmdb"}:
        return None
    canonical_candidate = _canonicalize_url(candidate_url)
    candidate_key = _url_key(canonical_candidate)
    stripped_key = candidate_key.rstrip("/")
    candidate_keys = sorted({candidate_key, stripped_key, f"{stripped_key}/"})
    row = pg.fetch_one(
        """
        SELECT url
        FROM core.entity_links
        WHERE entity_type = 'person'
          AND entity_id = %s
          AND link_kind = %s
          AND status = 'approved'
          AND url_key = ANY(%s::text[])
        ORDER BY updated_at DESC
        LIMIT 1
        """,
        [
            person_id,
            normalized_kind,
            candidate_keys,
        ],
    )
    existing_url = str((row or {}).get("url") or "").strip()
    return _canonicalize_url(existing_url) if existing_url else None


def _validated_or_carried_person_source_url(
    *,
    person_id: str,
    candidate_url: str,
    kind: str,
    expected_name: str,
    fandom_allowlist: list[str] | tuple[str, ...] | None = None,
) -> str | None:
    resolved, outcome = _validate_person_knowledge_url(
        candidate_url,
        kind=kind,
        expected_name=expected_name,
        fandom_allowlist=fandom_allowlist,
    )
    if outcome == "valid" and resolved:
        return resolved
    if outcome == "fetch_error":
        carried_url = _load_preapproved_person_source_url(
            person_id=person_id,
            link_kind=kind,
            candidate_url=candidate_url,
        )
        if carried_url:
            return carried_url
        # When upstream source pages are temporarily blocked/challenged, keep trusted external-id links usable.
        return _canonicalize_url(candidate_url)
    return None


def _discover_show_links(show_id: str, *, stats: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    show = pg.fetch_one(
        """
        SELECT id, name, imdb_id, tmdb_id, networks, wikidata_id, external_ids
        FROM core.shows
        WHERE id = %s
        """,
        [show_id],
    )
    if not show:
        return []

    show_name = str(show.get("name") or "").strip()
    show_slug = _slug(show_name)
    network_values = [str(n).strip() for n in (show.get("networks") or []) if isinstance(n, str) and str(n).strip()]
    networks = _normalize_show_network_tokens(network_values)
    external_ids = show.get("external_ids") if isinstance(show.get("external_ids"), dict) else {}
    show_wikidata_id = _resolve_show_wikidata_id(show_id, show.get("wikidata_id"))
    show_imdb_id = str(show.get("imdb_id") or "").strip().lower()
    show_tmdb_id = str(show.get("tmdb_id") or "").strip()
    show_tvdb_id = str(external_ids.get("tvdb") or external_ids.get("tvdb_id") or "").strip()
    show_tvmaze_id = str(external_ids.get("tvmaze") or external_ids.get("tvmaze_id") or "").strip()
    show_trakt_id = str(external_ids.get("trakt") or external_ids.get("trakt_id") or "").strip()
    show_x_topic_id = str(
        external_ids.get("x_topic") or external_ids.get("x_topic_id") or external_ids.get("twitter_topic_id") or ""
    ).strip()
    show_ratinggraph_tv_show_id = str(
        external_ids.get("ratinggraph")
        or external_ids.get("ratinggraph_tv_show_id")
        or external_ids.get("ratingraph")
        or ""
    ).strip()
    if show_wikidata_id and (
        not show_imdb_id
        or not show_tmdb_id
        or not show_tvdb_id
        or not show_tvmaze_id
        or not show_trakt_id
        or not show_x_topic_id
        or not show_ratinggraph_tv_show_id
    ):
        wikidata_summary, wikidata_fetch_error = _fetch_wikidata_summary(show_wikidata_id)
        if not wikidata_fetch_error and wikidata_summary:
            candidate_imdb_id = str(wikidata_summary.get("imdb_id") or "").strip()
            candidate_tmdb_tv_id = str(wikidata_summary.get("tmdb_tv_id") or "").strip()
            candidate_tvdb_id = str(wikidata_summary.get("tvdb_id") or "").strip()
            candidate_tvmaze_id = str(wikidata_summary.get("tvmaze_show_id") or "").strip()
            candidate_trakt_id = str(wikidata_summary.get("trakt_id") or "").strip()
            candidate_x_topic_id = str(wikidata_summary.get("x_topic_id") or "").strip()
            candidate_ratinggraph_tv_show_id = str(wikidata_summary.get("ratinggraph_tv_show_id") or "").strip()
            if not show_imdb_id and re.fullmatch(r"tt\d+", candidate_imdb_id, flags=re.IGNORECASE):
                show_imdb_id = candidate_imdb_id.lower()
            if not show_tmdb_id and re.fullmatch(r"\d+", candidate_tmdb_tv_id):
                show_tmdb_id = candidate_tmdb_tv_id
            if not show_tvdb_id and re.fullmatch(r"\d+", candidate_tvdb_id):
                show_tvdb_id = candidate_tvdb_id
            if not show_tvmaze_id and re.fullmatch(r"\d+", candidate_tvmaze_id):
                show_tvmaze_id = candidate_tvmaze_id
            if not show_trakt_id and candidate_trakt_id:
                show_trakt_id = candidate_trakt_id
            if not show_x_topic_id and candidate_x_topic_id:
                show_x_topic_id = candidate_x_topic_id
            if not show_ratinggraph_tv_show_id and candidate_ratinggraph_tv_show_id:
                show_ratinggraph_tv_show_id = candidate_ratinggraph_tv_show_id
    curated_fandom_domains = _curated_show_fandom_domains(show_name)
    fandom_allowlist = _effective_fandom_allowlist(
        show_name=show_name,
        network_tokens=networks,
        base_allowlist=load_fandom_community_allowlist(),
    )

    discovered: list[dict[str, Any]] = []

    if show_slug:
        if "bravo" in networks:
            discovered.append(
                {
                    "entity_type": "show",
                    "entity_id": show_id,
                    "season_number": 0,
                    "link_group": "official",
                    "link_kind": "official_page",
                    "label": "BravoTV show page",
                    "url": f"https://www.bravotv.com/{show_slug}",
                    "source": "derived",
                }
            )
        show_wikipedia_url, _, wikipedia_error = _resolve_wikipedia_url(
            f"https://en.wikipedia.org/wiki/{quote(show_name.replace(' ', '_'))}"
        )
        if wikipedia_error is None and show_wikipedia_url:
            if show_wikidata_id:
                candidate_wikidata_id = _resolve_wikipedia_wikidata_id(show_wikipedia_url)
                if candidate_wikidata_id and candidate_wikidata_id != show_wikidata_id:
                    show_wikipedia_url = None
            if show_wikipedia_url:
                discovered.append(
                    {
                        "entity_type": "show",
                        "entity_id": show_id,
                        "season_number": 0,
                        "link_group": "knowledge",
                        "link_kind": "wikipedia",
                        "label": "Wikipedia",
                        "url": show_wikipedia_url,
                        "source": "derived",
                    }
                )
        existing_show_fandom_links = pg.fetch_all(
            """
            SELECT url
            FROM core.entity_links
            WHERE show_id = %s
              AND entity_type = 'show'
              AND season_number = 0
              AND lower(link_kind) IN ('fandom', 'wikia')
              AND lower(status) <> 'rejected'
            ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST
            LIMIT 10
            """,
            [show_id],
        )
        fandom_candidates: list[tuple[str, str]] = []
        for row in existing_show_fandom_links:
            raw_url = str(row.get("url") or "").strip()
            if raw_url:
                fandom_candidates.append((raw_url, "core.entity_links"))
        fandom_candidates.extend((url, "curated_fandom_base") for url in _curated_show_fandom_base_urls(show_name))
        if "bravo" in networks and show_name:
            bravo_wiki_candidate = search_real_housewives_wiki(show_name)
            if bravo_wiki_candidate:
                fandom_candidates.append((bravo_wiki_candidate, "bravo_default"))
        fandom_rows_by_url: dict[str, dict[str, Any]] = {}
        for raw_url, fandom_source in fandom_candidates:
            if _stage_budget_exhausted(stats):
                break
            canonical_raw_url = _canonicalize_url(raw_url)
            if not canonical_raw_url:
                continue
            parsed = urlparse(canonical_raw_url)
            if not parsed.scheme.startswith("http"):
                continue
            candidate_host = str(parsed.hostname or "").strip().lower()
            if curated_fandom_domains and candidate_host and candidate_host not in curated_fandom_domains:
                continue
            if not is_allowlisted_fandom_domain(canonical_raw_url, allowlist=fandom_allowlist):
                continue
            candidate_urls: list[str] = [canonical_raw_url]
            if _is_fandom_seed_url(canonical_raw_url) and show_name:
                derived_candidates: list[str] = []
                direct_candidate = build_fandom_wiki_url_from_name(show_name, candidate_host)
                if direct_candidate:
                    derived_candidates.append(direct_candidate)
                if not _stage_budget_exhausted(stats):
                    derived_candidates.extend(
                        search_fandom_community_wiki_candidates(
                            show_name,
                            community_domain=candidate_host,
                            timeout_seconds=_source_timeout_seconds("fandom"),
                            max_results=12,
                        )
                    )
                derived_candidates.extend(
                    _search_fandom_allpages_html_candidates(
                        community_domain=candidate_host,
                        query=show_name,
                        max_results=40,
                        stats=stats,
                    )
                )
                ranked = sorted(
                    {
                        _canonicalize_url(candidate)
                        for candidate in derived_candidates
                        if _canonicalize_url(candidate)
                    },
                    key=lambda candidate: _score_fandom_show_candidate_url(candidate, show_name=show_name),
                    reverse=True,
                )
                candidate_urls.extend(ranked)

            found_derived_show_page = False
            for idx, candidate_url in enumerate(candidate_urls):
                if idx > 0 and found_derived_show_page:
                    break
                if idx > 0 and _score_fandom_show_candidate_url(candidate_url, show_name=show_name) <= 0:
                    continue
                if not _can_attempt_fandom_candidate(stats):
                    break
                status_code, html, resolved_url, fetch_error = _fetch_html_with_status(
                    candidate_url,
                    timeout=_source_timeout_seconds("fandom"),
                )
                if fetch_error or not html:
                    continue
                if status_code is not None and status_code >= 400:
                    continue
                resolved = str(resolved_url or candidate_url)
                if is_missing_fandom_page(html, resolved):
                    continue
                normalized = _canonicalize_url(resolved)
                if not normalized:
                    continue
                title = None
                soup = BeautifulSoup(html, "html.parser")
                heading = soup.select_one("span.mw-page-title-main") or soup.select_one("h1")
                if heading is not None:
                    title = str(heading.get_text(" ", strip=True) or "").strip() or None
                if not title:
                    title = _extract_fandom_page_title_from_url(normalized)
                source_value = fandom_source if idx == 0 else f"{fandom_source}:derived_show_page"
                priority = _show_fandom_source_priority(source_value)
                existing = fandom_rows_by_url.get(normalized)
                existing_priority = existing.get("priority") if isinstance(existing, dict) else None
                if existing is None or not isinstance(existing_priority, int) or priority < existing_priority:
                    fandom_rows_by_url[normalized] = {
                        "url": normalized,
                        "source": source_value,
                        "title": title,
                        "priority": priority,
                    }
                elif existing is not None and not str(existing.get("title") or "").strip() and title:
                    existing["title"] = title
                if idx > 0:
                    found_derived_show_page = True
        for fandom_row in fandom_rows_by_url.values():
            fandom_url = str(fandom_row.get("url") or "").strip()
            fandom_source = str(fandom_row.get("source") or "").strip()
            fandom_title = str(fandom_row.get("title") or "").strip() or None
            if not fandom_url:
                continue
            discovered.append(
                {
                    "entity_type": "show",
                    "entity_id": show_id,
                    "season_number": 0,
                    "link_group": "knowledge",
                    "link_kind": "fandom",
                    "label": fandom_title or "Fandom",
                    "url": fandom_url,
                    "source": fandom_source,
                    "metadata": _build_fandom_link_metadata(fandom_url, title=fandom_title),
                }
            )

    if show_wikidata_id:
        discovered.append(
            {
                "entity_type": "show",
                "entity_id": show_id,
                "season_number": 0,
                "link_group": "knowledge",
                "link_kind": "wikidata",
                "label": "Wikidata",
                "url": f"https://www.wikidata.org/wiki/{show_wikidata_id}",
                "source": "core.shows.wikidata_id",
            }
        )

    if show_imdb_id:
        discovered.append(
            {
                "entity_type": "show",
                "entity_id": show_id,
                "season_number": 0,
                "link_group": "knowledge",
                "link_kind": "imdb",
                "label": "IMDb",
                "url": f"https://www.imdb.com/title/{show_imdb_id}/",
                "source": "core.shows.imdb_id",
            }
        )

    if show_tmdb_id:
        discovered.append(
            {
                "entity_type": "show",
                "entity_id": show_id,
                "season_number": 0,
                "link_group": "knowledge",
                "link_kind": "tmdb",
                "label": "TMDb",
                "url": f"https://www.themoviedb.org/tv/{show_tmdb_id}",
                "source": "core.shows.tmdb_id",
            }
        )

    if re.fullmatch(r"\d+", show_tvdb_id):
        discovered.append(
            {
                "entity_type": "show",
                "entity_id": show_id,
                "season_number": 0,
                "link_group": "knowledge",
                "link_kind": "tvdb",
                "label": "TVDB",
                "url": f"https://www.thetvdb.com/series/{show_tvdb_id}",
                "source": (
                    "core.shows.external_ids"
                    if external_ids.get("tvdb") or external_ids.get("tvdb_id")
                    else "core.shows.wikidata_id"
                ),
            }
        )

    if re.fullmatch(r"\d+", show_tvmaze_id):
        discovered.append(
            {
                "entity_type": "show",
                "entity_id": show_id,
                "season_number": 0,
                "link_group": "knowledge",
                "link_kind": "tvmaze",
                "label": "TVmaze",
                "url": f"https://www.tvmaze.com/shows/{show_tvmaze_id}",
                "source": (
                    "core.shows.external_ids"
                    if external_ids.get("tvmaze") or external_ids.get("tvmaze_id")
                    else "core.shows.wikidata_id"
                ),
            }
        )

    if show_trakt_id:
        discovered.append(
            {
                "entity_type": "show",
                "entity_id": show_id,
                "season_number": 0,
                "link_group": "knowledge",
                "link_kind": "trakt",
                "label": "Trakt.tv",
                "url": _wikidata_trakt_url(show_trakt_id),
                "source": (
                    "core.shows.external_ids"
                    if external_ids.get("trakt") or external_ids.get("trakt_id")
                    else "core.shows.wikidata_id"
                ),
            }
        )

    if show_x_topic_id:
        discovered.append(
            {
                "entity_type": "show",
                "entity_id": show_id,
                "season_number": 0,
                "link_group": "knowledge",
                "link_kind": "x_topic",
                "label": "X Topic",
                "url": _wikidata_x_topic_url(show_x_topic_id),
                "source": (
                    "core.shows.external_ids"
                    if external_ids.get("x_topic")
                    or external_ids.get("x_topic_id")
                    or external_ids.get("twitter_topic_id")
                    else "core.shows.wikidata_id"
                ),
            }
        )

    if show_ratinggraph_tv_show_id:
        discovered.append(
            {
                "entity_type": "show",
                "entity_id": show_id,
                "season_number": 0,
                "link_group": "knowledge",
                "link_kind": "ratinggraph",
                "label": "RatingGraph",
                "url": f"https://www.ratingraph.com/tv-shows/{quote(show_ratinggraph_tv_show_id)}",
                "source": (
                    "core.shows.external_ids"
                    if external_ids.get("ratinggraph")
                    or external_ids.get("ratinggraph_tv_show_id")
                    or external_ids.get("ratingraph")
                    else "core.shows.wikidata_id"
                ),
            }
        )

    if "peacock" in networks:
        discovered.append(
            {
                "entity_type": "show",
                "entity_id": show_id,
                "season_number": 0,
                "link_group": "cast_announcements",
                "link_kind": "network_blog",
                "label": "Peacock Blog",
                "url": f"https://www.peacocktv.com/blog/show/{show_slug}",
                "source": "network_default",
            }
        )
    if "nbc" in networks:
        discovered.append(
            {
                "entity_type": "show",
                "entity_id": show_id,
                "season_number": 0,
                "link_group": "cast_announcements",
                "link_kind": "network_blog",
                "label": "NBC Insider",
                "url": f"https://www.nbc.com/nbc-insider/franchise/{show_slug}",
                "source": "network_default",
            }
        )

    if "bravo" in networks:
        discovered.extend(
            [
                {
                    "entity_type": "show",
                    "entity_id": show_id,
                    "season_number": 0,
                    "link_group": "social",
                    "link_kind": "instagram",
                    "label": "Instagram",
                    "url": "https://www.instagram.com/BravoTV",
                    "source": "network_default",
                },
                {
                    "entity_type": "show",
                    "entity_id": show_id,
                    "season_number": 0,
                    "link_group": "social",
                    "link_kind": "tiktok",
                    "label": "TikTok",
                    "url": "https://www.tiktok.com/@BravoTV",
                    "source": "network_default",
                },
                {
                    "entity_type": "show",
                    "entity_id": show_id,
                    "season_number": 0,
                    "link_group": "social",
                    "link_kind": "twitter",
                    "label": "Twitter/X",
                    "url": "https://x.com/BravoTV",
                    "source": "network_default",
                },
                {
                    "entity_type": "show",
                    "entity_id": show_id,
                    "season_number": 0,
                    "link_group": "social",
                    "link_kind": "youtube",
                    "label": "YouTube",
                    "url": "https://www.youtube.com/@Bravo",
                    "source": "network_default",
                },
            ]
        )

    if isinstance(external_ids, dict):
        for kind in ("instagram", "tiktok", "twitter", "youtube"):
            handle = str(external_ids.get(kind) or external_ids.get(f"{kind}_id") or "").strip()
            if not handle:
                continue
            canonical = handle.lstrip("@")
            if kind == "instagram":
                url = f"https://www.instagram.com/{canonical}"
            elif kind == "tiktok":
                url = f"https://www.tiktok.com/@{canonical}"
            elif kind == "twitter":
                url = f"https://x.com/{canonical}"
            else:
                url = f"https://www.youtube.com/@{canonical}"
            discovered.append(
                {
                    "entity_type": "show",
                    "entity_id": show_id,
                    "season_number": 0,
                    "link_group": "social",
                    "link_kind": kind,
                    "label": kind.title(),
                    "url": url,
                    "source": "core.shows.external_ids",
                }
            )

    snapshot = pg.fetch_one(
        """
        SELECT payload
        FROM core.show_source_latest
        WHERE show_id = %s AND source_id = 'bravo' AND variant = %s
        LIMIT 1
        """,
        [show_id, _BRAVO_VARIANT],
    )
    payload = snapshot.get("payload") if snapshot and isinstance(snapshot.get("payload"), dict) else {}
    normalized = payload.get("normalized") if isinstance(payload, dict) else {}
    news_items = (
        normalized.get("news_show")
        if isinstance(normalized, dict) and isinstance(normalized.get("news_show"), list)
        else []
    )
    for item in news_items:
        if not isinstance(item, dict):
            continue
        headline = str(item.get("headline") or "").strip()
        article_url = str(item.get("article_url") or "").strip()
        if not article_url:
            continue
        if not re.search(r"\b(cast|friend\s*of|full[-\s]*time|joins|returning|returns)\b", headline, re.IGNORECASE):
            continue
        discovered.append(
            {
                "entity_type": "show",
                "entity_id": show_id,
                "season_number": int(item.get("season_number") or 0),
                "link_group": "cast_announcements",
                "link_kind": "cast_announcement",
                "label": headline or "Cast announcement",
                "url": article_url,
                "source": "bravo_snapshot",
                "metadata": {"published_at": item.get("published_at")},
            }
        )

    return discovered


def _collect_show_fandom_seed_urls(
    show_id: str,
    *,
    show_name: str | None,
    show_fandom_seed_urls: list[str] | None,
    fandom_allowlist: list[str] | tuple[str, ...] | None = None,
) -> list[str]:
    resolved_allowlist = fandom_allowlist if fandom_allowlist is not None else load_fandom_community_allowlist()
    curated_domains = _curated_show_fandom_domains(show_name)
    candidates: list[str] = []
    if show_fandom_seed_urls:
        candidates.extend(show_fandom_seed_urls)

    try:
        existing_rows = pg.fetch_all(
            """
            SELECT url
            FROM core.entity_links
            WHERE show_id = %s
              AND entity_type = 'show'
              AND season_number = 0
              AND lower(link_kind) IN ('fandom', 'wikia')
              AND lower(status) <> 'rejected'
            ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST
            LIMIT 50
            """,
            [show_id],
        )
    except Exception:  # noqa: BLE001
        existing_rows = []

    for row in existing_rows:
        raw_url = str(row.get("url") or "").strip()
        if raw_url:
            candidates.append(raw_url)

    normalized: list[str] = []
    seen: set[str] = set()
    for raw_url in candidates:
        canonical = _canonicalize_url(raw_url)
        if not canonical:
            continue
        parsed = urlparse(canonical)
        if not parsed.scheme.startswith("http"):
            continue
        host = str(parsed.hostname or "").strip().lower()
        if curated_domains and host and host not in curated_domains:
            continue
        if not is_allowlisted_fandom_domain(canonical, allowlist=resolved_allowlist):
            continue
        dedupe_key = _url_key(canonical)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        normalized.append(canonical)
    return normalized


def _extract_fandom_domains_from_urls(
    urls: list[str],
    *,
    fandom_allowlist: list[str] | tuple[str, ...] | None = None,
) -> list[str]:
    resolved_allowlist = fandom_allowlist if fandom_allowlist is not None else load_fandom_community_allowlist()
    domains: list[str] = []
    for url in urls:
        parsed = urlparse(str(url or "").strip())
        host = str(parsed.hostname or "").strip().lower()
        if not host:
            continue
        if not is_allowlisted_fandom_domain(host, allowlist=resolved_allowlist):
            continue
        if host in domains:
            continue
        domains.append(host)
    return domains


def _build_fandom_season_search_queries(
    *,
    show_name: str,
    season_number: int,
) -> list[str]:
    queries: list[str] = []
    cleaned_show_name = str(show_name or "").strip()
    if cleaned_show_name:
        queries.append(f"{cleaned_show_name} season {season_number}")
        queries.append(f"{cleaned_show_name} series {season_number}")
    queries.append(f"season {season_number}")
    deduped: list[str] = []
    seen: set[str] = set()
    for query in queries:
        normalized = re.sub(r"\s+", " ", query).strip().lower()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(query.strip())
    return deduped


def _derive_fandom_season_urls_from_show_seed(seed_url: str, *, season_number: int) -> list[str]:
    canonical_seed = _canonicalize_url(seed_url)
    if not canonical_seed:
        return []
    parsed = urlparse(canonical_seed)
    host = str(parsed.hostname or "").strip().lower()
    if not host:
        return []
    seed_title = _extract_wiki_slug_title(unquote(parsed.path or ""))
    if not seed_title:
        return []
    base_title = str(seed_title).strip()
    if not base_title:
        return []

    variants = [
        f"{base_title}_season_{season_number}",
        f"{base_title}_(season_{season_number})",
        f"{base_title}_series_{season_number}",
    ]
    urls: list[str] = []
    seen: set[str] = set()
    for variant in variants:
        candidate = _canonicalize_url(f"https://{host}/wiki/{quote(variant, safe='()_')}")
        if not candidate:
            continue
        dedupe_key = _url_key(candidate)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        urls.append(candidate)
    return urls


def _extract_page_title_candidates(html: str, resolved_url: str) -> set[str]:
    candidates: set[str] = set()

    path = unquote(urlparse(resolved_url).path or "")
    if "/wiki/" in path:
        slug = path.split("/wiki/", 1)[1].split("/", 1)[0]
        slug_text = slug.replace("_", " ").strip()
        if slug_text:
            candidates.add(slug_text)

    soup = BeautifulSoup(html or "", "html.parser")
    heading = soup.select_one("h1")
    if heading is not None:
        heading_text = heading.get_text(" ", strip=True).strip()
        if heading_text:
            candidates.add(heading_text)

    if soup.title is not None:
        title_text = soup.title.get_text(" ", strip=True).strip()
        if title_text:
            head = re.split(r"\s+[-|]\s+", title_text, maxsplit=1)[0].strip()
            if head:
                candidates.add(head)

    og_title = soup.select_one('meta[property="og:title"]')
    if og_title is not None:
        og_title_text = str(og_title.get("content") or "").strip()
        if og_title_text:
            head = re.split(r"\s+[-|]\s+", og_title_text, maxsplit=1)[0].strip()
            if head:
                candidates.add(head)

    return candidates


def _validated_fandom_season_url(
    candidate_url: str,
    *,
    season_number: int,
    show_name: str | None = None,
    fandom_allowlist: list[str] | tuple[str, ...] | None = None,
) -> str | None:
    resolved_allowlist = fandom_allowlist if fandom_allowlist is not None else load_fandom_community_allowlist()
    canonical_candidate = _canonicalize_url(candidate_url)
    if not canonical_candidate:
        return None
    if not is_allowlisted_fandom_domain(canonical_candidate, allowlist=resolved_allowlist):
        return None

    status_code, html, final_url, _ = _fetch_html_with_status(
        canonical_candidate,
        timeout=_source_timeout_seconds("fandom"),
    )
    if status_code is None or status_code >= 400 or not html:
        return None

    resolved = _canonicalize_url(final_url or canonical_candidate)
    if not resolved:
        return None
    if is_missing_fandom_page(html, resolved):
        return None
    if not is_allowlisted_fandom_domain(resolved, allowlist=resolved_allowlist):
        return None

    candidates = _extract_page_title_candidates(html, resolved)
    season_matches = any(_extract_season_number_from_text(value) == season_number for value in candidates)
    if not season_matches:
        return None
    if show_name:
        expected_tokens = {
            token
            for token in _normalize_lookup_value(show_name).split()
            if token and token not in {"the", "season", "series", "tv", "show"}
        }
        if expected_tokens:
            candidate_tokens = {
                token
                for value in candidates
                for token in _normalize_lookup_value(value).split()
                if token
            }
            if not expected_tokens.intersection(candidate_tokens):
                return None
    return resolved


def _discover_season_links(
    show_id: str,
    *,
    show_fandom_seed_urls: list[str] | None = None,
    stats: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    rows = pg.fetch_all(
        """
        SELECT id, season_number, external_wikidata_id, external_ids, tmdb_season_id
        FROM core.seasons
        WHERE show_id = %s
        """,
        [show_id],
    )
    show_name_row = pg.fetch_one("SELECT name, wikidata_id, tmdb_id, networks FROM core.shows WHERE id = %s", [show_id])
    show_name = str(show_name_row.get("name") or "").strip() if show_name_row else ""
    show_wikidata_id = _resolve_show_wikidata_id(show_id, (show_name_row or {}).get("wikidata_id"))
    show_tmdb_id = str((show_name_row or {}).get("tmdb_id") or "").strip()
    network_values = [
        str(value).strip() for value in ((show_name_row or {}).get("networks") or []) if isinstance(value, str)
    ]
    network_tokens = _normalize_show_network_tokens(network_values)
    fandom_allowlist = _effective_fandom_allowlist(
        show_name=show_name,
        network_tokens=network_tokens,
        base_allowlist=load_fandom_community_allowlist(),
    )
    season_fandom_seed_urls = _collect_show_fandom_seed_urls(
        show_id,
        show_name=show_name,
        show_fandom_seed_urls=show_fandom_seed_urls,
        fandom_allowlist=fandom_allowlist,
    )
    fandom_domains = _extract_fandom_domains_from_urls(season_fandom_seed_urls, fandom_allowlist=fandom_allowlist)
    season_fandom_seed_urls_by_domain: dict[str, list[str]] = {}
    for seed_url in season_fandom_seed_urls:
        host = str(urlparse(seed_url).hostname or "").strip().lower()
        if not host:
            continue
        bucket = season_fandom_seed_urls_by_domain.get(host)
        if bucket is None:
            season_fandom_seed_urls_by_domain[host] = [seed_url]
        elif seed_url not in bucket:
            bucket.append(seed_url)
    seeded_fandom_candidates_by_domain = _collect_seeded_fandom_candidate_urls_by_domain(
        season_fandom_seed_urls,
        fandom_allowlist=fandom_allowlist,
        stats=stats,
    )
    per_domain_candidate_cap = _link_discovery_season_fandom_candidates_per_domain_cap()
    show_wikipedia_row = pg.fetch_one(
        """
        SELECT url
        FROM core.entity_links
        WHERE show_id = %s
          AND entity_type = 'show'
          AND season_number = 0
          AND lower(link_kind) = 'wikipedia'
          AND lower(status) <> 'rejected'
        ORDER BY
          CASE WHEN lower(status) = 'approved' THEN 0 ELSE 1 END,
          updated_at DESC NULLS LAST,
          created_at DESC NULLS LAST
        LIMIT 1
        """,
        [show_id],
    )
    show_wikipedia_seed_url: str | None = None
    raw_show_wikipedia_url = str((show_wikipedia_row or {}).get("url") or "").strip()
    if raw_show_wikipedia_url:
        resolved_show_wikipedia_url, _, show_wikipedia_error = _resolve_wikipedia_url(raw_show_wikipedia_url)
        if show_wikipedia_error is None and resolved_show_wikipedia_url:
            show_wikipedia_seed_url = resolved_show_wikipedia_url

    season_wikidata_by_number: dict[int, str] = {}
    if show_wikidata_id:
        show_wikidata_summary, show_wikidata_fetch_error = _fetch_wikidata_summary(show_wikidata_id)
        if not show_wikidata_fetch_error and isinstance(show_wikidata_summary, dict):
            related_season_ids = show_wikidata_summary.get("season_item_ids")
            if isinstance(related_season_ids, list):
                for season_item_id in related_season_ids:
                    candidate_item_id = _extract_wikidata_item_id(str(season_item_id or "").strip())
                    if not candidate_item_id:
                        continue
                    season_summary, season_summary_fetch_error = _fetch_wikidata_summary(candidate_item_id)
                    if season_summary_fetch_error or not isinstance(season_summary, dict):
                        continue
                    inferred_season_number = _extract_season_number_from_text(
                        str(season_summary.get("enwiki_title") or "").strip()
                    ) or _extract_season_number_from_text(str(season_summary.get("label") or "").strip())
                    if inferred_season_number and inferred_season_number not in season_wikidata_by_number:
                        season_wikidata_by_number[inferred_season_number] = candidate_item_id

    found: list[dict[str, Any]] = []
    for row in rows:
        if _stage_budget_exhausted(stats):
            break
        season_id = str(row.get("id"))
        season_number = int(row.get("season_number") or 0)
        if season_number <= 0:
            continue
        season_external_ids = row.get("external_ids") if isinstance(row.get("external_ids"), dict) else {}
        season_tmdb_id = str(
            row.get("tmdb_season_id")
            or season_external_ids.get("tmdb")
            or season_external_ids.get("tmdb_id")
            or season_external_ids.get("tmdb_season_id")
            or ""
        ).strip()
        season_tvmaze_id = str(
            season_external_ids.get("tvmaze")
            or season_external_ids.get("tvmaze_id")
            or season_external_ids.get("tvmaze_season_id")
            or ""
        ).strip()

        if re.fullmatch(r"\d+", show_tmdb_id):
            found.append(
                {
                    "entity_type": "season",
                    "entity_id": season_id,
                    "season_number": season_number,
                    "link_group": "knowledge",
                    "link_kind": "tmdb",
                    "label": f"Season {season_number} TMDb",
                    "url": f"https://www.themoviedb.org/tv/{show_tmdb_id}/season/{season_number}",
                    "source": (
                        "core.seasons.tmdb_season_id"
                        if re.fullmatch(r"\d+", season_tmdb_id)
                        else "core.shows.tmdb_id"
                    ),
                }
            )

        wikidata = _extract_wikidata_item_id(str(row.get("external_wikidata_id") or "").strip())
        wikidata_source = "core.seasons.external_wikidata_id"
        if not wikidata:
            wikidata = season_wikidata_by_number.get(season_number)
            if wikidata:
                wikidata_source = "show_wikidata_season_claims"
        wikidata_summary = None
        season_tvdb_id = str(
            season_external_ids.get("tvdb")
            or season_external_ids.get("tvdb_id")
            or season_external_ids.get("tvdb_season_id")
            or ""
        ).strip()
        if wikidata:
            wikidata_summary, wikidata_fetch_error = _fetch_wikidata_summary(wikidata)
            if wikidata_fetch_error:
                wikidata_summary = None
            if isinstance(wikidata_summary, dict):
                candidate_tvmaze_id = str(wikidata_summary.get("tvmaze_season_id") or "").strip()
                if re.fullmatch(r"\d+", candidate_tvmaze_id):
                    season_tvmaze_id = candidate_tvmaze_id
                candidate_tvdb_season_id = str(wikidata_summary.get("tvdb_season_id") or "").strip()
                if re.fullmatch(r"\d+", candidate_tvdb_season_id) and not season_tvdb_id:
                    season_tvdb_id = candidate_tvdb_season_id
        if wikidata:
            found.append(
                {
                    "entity_type": "season",
                    "entity_id": season_id,
                    "season_number": season_number,
                    "link_group": "knowledge",
                    "link_kind": "wikidata",
                    "label": f"Season {season_number} Wikidata",
                    "url": f"https://www.wikidata.org/wiki/{wikidata}",
                    "source": wikidata_source,
                    "metadata": {
                        "tvdb_season_id": season_tvdb_id if season_tvdb_id else None,
                        "part_of_series_item_ids": (
                            wikidata_summary.get("part_of_series_item_ids")
                            if isinstance(wikidata_summary, dict)
                            else []
                        ),
                    },
                }
            )
        if re.fullmatch(r"\d+", season_tvmaze_id):
            found.append(
                {
                    "entity_type": "season",
                    "entity_id": season_id,
                    "season_number": season_number,
                    "link_group": "knowledge",
                    "link_kind": "tvmaze",
                    "label": f"Season {season_number} TVmaze",
                    "url": f"https://www.tvmaze.com/seasons/{season_tvmaze_id}",
                    "source": (
                        "core.seasons.external_ids"
                        if season_external_ids.get("tvmaze")
                        or season_external_ids.get("tvmaze_id")
                        or season_external_ids.get("tvmaze_season_id")
                        else "season_wikidata_external_ids"
                    ),
                }
            )
        season_wikipedia_candidates: list[tuple[str, str]] = []
        season_wikipedia_url = _resolve_wikidata_enwiki_url(wikidata) if wikidata else None
        if season_wikipedia_url:
            season_wikipedia_candidates.append((season_wikipedia_url, "wikidata_sitelink"))
        if show_wikipedia_seed_url:
            derived_from_show = _derive_season_wikipedia_url_from_show_wikipedia(
                show_wikipedia_seed_url,
                season_number,
            )
            if derived_from_show:
                season_wikipedia_candidates.append((derived_from_show, "derived_show_wikipedia"))
        if show_name:
            season_wikipedia_candidates.append(
                (
                    "https://en.wikipedia.org/wiki/"
                    f"{quote((show_name + ' season ' + str(season_number)).replace(' ', '_'))}",
                    "derived",
                )
            )

        resolved_season_wikipedia_url: str | None = None
        resolved_season_wikipedia_source: str | None = None
        for season_candidate_url, season_candidate_source in season_wikipedia_candidates:
            candidate_url, _, wikipedia_error = _resolve_wikipedia_url(season_candidate_url)
            if wikipedia_error is None and candidate_url:
                resolved_season_wikipedia_url = candidate_url
                resolved_season_wikipedia_source = season_candidate_source
                break

        if resolved_season_wikipedia_url:
            found.append(
                {
                    "entity_type": "season",
                    "entity_id": season_id,
                    "season_number": season_number,
                    "link_group": "knowledge",
                    "link_kind": "wikipedia",
                    "label": f"Season {season_number} Wikipedia",
                    "url": resolved_season_wikipedia_url,
                    "source": resolved_season_wikipedia_source or "derived",
                }
            )

        if season_number <= 0 or not fandom_domains:
            continue

        season_fandom_queries = _build_fandom_season_search_queries(
            show_name=show_name,
            season_number=season_number,
        )
        for fandom_domain in fandom_domains:
            if _stage_budget_exhausted(stats):
                break
            resolved_fandom_url: str | None = None
            domain_seeded_candidates = seeded_fandom_candidates_by_domain.get(fandom_domain, [])
            ranked_seeded_candidates = sorted(
                domain_seeded_candidates,
                key=lambda candidate_url: _score_fandom_season_candidate_url(
                    candidate_url,
                    show_name=show_name if show_name else None,
                    season_number=season_number,
                ),
                reverse=True,
            )
            for candidate in ranked_seeded_candidates:
                if not _can_attempt_fandom_candidate(stats):
                    break
                candidate_score = _score_fandom_season_candidate_url(
                    candidate,
                    show_name=show_name if show_name else None,
                    season_number=season_number,
                )
                if candidate_score <= 0:
                    continue
                validated = _validated_fandom_season_url(
                    candidate,
                    season_number=season_number,
                    show_name=show_name if show_name else None,
                    fandom_allowlist=fandom_allowlist,
                )
                if validated:
                    resolved_fandom_url = validated
                    break
            direct_candidates: list[str] = []
            if show_name:
                direct_from_name = build_fandom_wiki_url_from_name(f"{show_name} season {season_number}", fandom_domain)
                if direct_from_name:
                    direct_candidates.append(direct_from_name)
            for seed_url in season_fandom_seed_urls_by_domain.get(fandom_domain, []):
                direct_candidates.extend(
                    _derive_fandom_season_urls_from_show_seed(
                        seed_url,
                        season_number=season_number,
                    )
                )

            seen_direct_candidates: set[str] = set()
            for candidate in direct_candidates[: max(1, per_domain_candidate_cap)]:
                if not _can_attempt_fandom_candidate(stats):
                    break
                candidate_key = _url_key(candidate)
                if candidate_key in seen_direct_candidates:
                    continue
                seen_direct_candidates.add(candidate_key)
                validated = _validated_fandom_season_url(
                    candidate,
                    season_number=season_number,
                    show_name=show_name if show_name else None,
                    fandom_allowlist=fandom_allowlist,
                )
                if validated:
                    resolved_fandom_url = validated
                    break

            if resolved_fandom_url:
                fandom_title = _extract_fandom_page_title_from_url(resolved_fandom_url)
                found.append(
                    {
                        "entity_type": "season",
                        "entity_id": season_id,
                        "season_number": season_number,
                        "link_group": "knowledge",
                        "link_kind": "fandom",
                        "label": fandom_title or f"Season {season_number}",
                        "url": resolved_fandom_url,
                        "source": f"fandom_domain_seed:{fandom_domain}",
                        "metadata": _build_fandom_link_metadata(resolved_fandom_url, title=fandom_title),
                    }
                )
                continue

            for query in season_fandom_queries:
                candidates = search_fandom_community_wiki_candidates(
                    query,
                    community_domain=fandom_domain,
                    timeout_seconds=_source_timeout_seconds("fandom"),
                    max_results=max(1, min(8, per_domain_candidate_cap)),
                )
                for candidate in candidates[: max(1, per_domain_candidate_cap)]:
                    if not _can_attempt_fandom_candidate(stats):
                        break
                    validated = _validated_fandom_season_url(
                        candidate,
                        season_number=season_number,
                        show_name=show_name if show_name else None,
                        fandom_allowlist=fandom_allowlist,
                    )
                    if validated:
                        resolved_fandom_url = validated
                        break
                if resolved_fandom_url:
                    break
            if not resolved_fandom_url:
                discovery_query = season_fandom_queries[0] if season_fandom_queries else f"season {season_number}"
                discovery_candidates = discover_fandom_candidate_pages(
                    query_name=discovery_query,
                    entity_kind="season",
                    season_number=season_number,
                    manual_page_urls=(
                        [*ranked_seeded_candidates, *direct_candidates]
                        if ranked_seeded_candidates or direct_candidates
                        else None
                    ),
                    community_domains=[fandom_domain],
                    include_allpages_scan=True,
                    allpages_max_pages=5,
                    max_candidates=25,
                )
                for candidate in discovery_candidates:
                    if not _can_attempt_fandom_candidate(stats):
                        break
                    validated = _validated_fandom_season_url(
                        candidate.url,
                        season_number=season_number,
                        show_name=show_name if show_name else None,
                        fandom_allowlist=fandom_allowlist,
                    )
                    if validated:
                        resolved_fandom_url = validated
                        break
            if not resolved_fandom_url:
                continue
            fandom_title = _extract_fandom_page_title_from_url(resolved_fandom_url)
            found.append(
                {
                    "entity_type": "season",
                    "entity_id": season_id,
                    "season_number": season_number,
                    "link_group": "knowledge",
                    "link_kind": "fandom",
                    "label": fandom_title or f"Season {season_number}",
                    "url": resolved_fandom_url,
                    "source": f"fandom_domain_search:{fandom_domain}",
                    "metadata": _build_fandom_link_metadata(resolved_fandom_url, title=fandom_title),
                }
            )
    return found


def _resolve_person_external_identifier(
    external_ids: dict[str, Any],
    *,
    keys: tuple[str, ...],
    fallback_value: Any,
    extractor: Any,
) -> tuple[str | None, str | None]:
    for key in keys:
        value = external_ids.get(key)
        extracted = extractor(value)
        if extracted:
            return str(extracted), "core.people.external_ids"
    fallback = extractor(fallback_value)
    if fallback:
        return str(fallback), "core.cast_tmdb"
    return None, None


def _build_person_link_row(
    *,
    person_id: str,
    link_kind: str,
    label: str,
    url: str,
    source: str,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_kind = _normalize_link_kind(link_kind)
    if normalized_kind in _SOCIAL_LINK_KINDS:
        link_group = "social"
    elif normalized_kind in _AUTO_APPROVE_KNOWLEDGE_LINK_KINDS:
        link_group = "knowledge"
    else:
        link_group = "official"
    return {
        "entity_type": "person",
        "entity_id": person_id,
        "season_number": 0,
        "link_group": link_group,
        "link_kind": normalized_kind,
        "label": label,
        "url": url,
        "source": source,
        "status": "approved",
        "confidence": 0.95,
        "metadata": metadata or {},
    }


def _discover_fandom_candidates_for_person(
    *,
    name: str,
    seeded_fandom_url: str | None,
    show_fandom_domains: list[str] | None,
    seeded_domain_candidates: dict[str, list[str]] | None,
    is_bravo_show: bool,
    fandom_allowlist: list[str] | tuple[str, ...],
    stats: dict[str, Any] | None = None,
) -> list[str]:
    max_candidates_per_person = _link_discovery_people_fandom_candidates_per_person_cap()
    candidates: list[str] = []
    if seeded_fandom_url:
        candidates.append(seeded_fandom_url)
    if name:
        for fandom_domain in show_fandom_domains or []:
            for seeded_candidate in (seeded_domain_candidates or {}).get(fandom_domain, []):
                if _score_fandom_candidate_url(seeded_candidate, expected_name=name) > 0:
                    candidates.append(seeded_candidate)
            direct_candidate = build_fandom_wiki_url_from_name(name, fandom_domain)
            if direct_candidate:
                candidates.append(direct_candidate)
            if not _stage_budget_exhausted(stats):
                search_candidates = search_fandom_community_wiki_candidates(
                    name,
                    community_domain=fandom_domain,
                    timeout_seconds=_source_timeout_seconds("fandom"),
                    max_results=max(5, min(12, max_candidates_per_person)),
                )
                for candidate in search_candidates[:max_candidates_per_person]:
                    if candidate and _score_fandom_candidate_url(candidate, expected_name=name) > 0:
                        candidates.append(candidate)
            allpages_candidates = _search_fandom_allpages_html_candidates(
                community_domain=fandom_domain,
                query=name,
                max_results=max_candidates_per_person,
                stats=stats,
            )
            for candidate in allpages_candidates[:max_candidates_per_person]:
                if candidate and _score_fandom_candidate_url(candidate, expected_name=name) > 0:
                    candidates.append(candidate)
            if len(candidates) >= max_candidates_per_person:
                break
    if not is_bravo_show or not name:
        deduped: list[str] = []
        seen: set[str] = set()
        for candidate in candidates:
            cleaned = str(candidate or "").strip()
            if not cleaned or cleaned in seen:
                continue
            seen.add(cleaned)
            deduped.append(cleaned)
        return deduped

    primary = search_real_housewives_wiki(name)
    if primary:
        candidates.append(primary)
    candidates.extend(
        search_allowlisted_fandom_wikis(
            name,
            allowlist=fandom_allowlist,
            max_results=5,
        )
    )

    deduped: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        cleaned = str(candidate or "").strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        deduped.append(cleaned)
        if len(deduped) >= max_candidates_per_person:
            break
    return deduped


def _extract_person_name_from_fandom_url(url: str | None) -> str | None:
    raw = str(url or "").strip()
    if not raw:
        return None
    parsed = urlparse(raw)
    if "/wiki/" not in parsed.path:
        return None
    slug = parsed.path.split("/wiki/", 1)[1].split("/", 1)[0]
    if not slug:
        return None
    return unquote(slug).replace("_", " ").strip() or None


def _score_fandom_candidate_url(url: str, *, expected_name: str) -> int:
    candidate_name = _extract_person_name_from_fandom_url(url)
    expected = _normalized_person_name(expected_name)
    candidate = _normalized_person_name(candidate_name)
    if not expected or not candidate:
        return 0
    if not _person_name_candidates_match(expected_name, [candidate_name]):
        return 0
    if candidate == expected:
        return 300
    expected_tokens = [token for token in expected.split() if token]
    candidate_tokens = [token for token in candidate.split() if token]
    if not expected_tokens or not candidate_tokens:
        return 0
    expected_token_set = set(expected_tokens)
    candidate_token_set = set(candidate_tokens)
    if expected_token_set.issubset(candidate_token_set):
        score = 220
        # Reward exact trailing-token surname alignment when present.
        if len(expected_tokens) >= 2 and expected_tokens[-1] == candidate_tokens[-1]:
            score += 40
        return score
    return 0


def _discover_people_links(
    show_id: str,
    *,
    show_fandom_seed_urls: list[str] | None = None,
    stats: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    show = pg.fetch_one("SELECT name, networks, wikidata_id FROM core.shows WHERE id = %s", [show_id]) or {}
    show_name = str(show.get("name") or "").strip()
    network_values = [
        str(value).strip() for value in (show.get("networks") or []) if isinstance(value, str) and str(value).strip()
    ]
    networks = _normalize_show_network_tokens(network_values)
    is_bravo_show = "bravo" in networks
    show_wikidata_id = _resolve_show_wikidata_id(show_id, show.get("wikidata_id"))
    fandom_allowlist = _effective_fandom_allowlist(
        show_name=show_name,
        network_tokens=networks,
        base_allowlist=load_fandom_community_allowlist(),
    )

    show_cast_wikidata_candidates: dict[str, dict[str, str]] = {}
    season_cast_wikidata_candidates: dict[str, dict[str, str]] = {}
    wikidata_cast_scan_cap = _link_discovery_wikidata_cast_scan_cap()
    wikidata_cast_scanned = 0
    if show_wikidata_id:
        show_wikidata_summary, show_wikidata_fetch_error = _fetch_wikidata_summary(show_wikidata_id)
        if not show_wikidata_fetch_error and isinstance(show_wikidata_summary, dict):
            related_cast_item_ids = show_wikidata_summary.get("cast_item_ids")
            if isinstance(related_cast_item_ids, list):
                for cast_item_id in related_cast_item_ids:
                    if wikidata_cast_scanned >= wikidata_cast_scan_cap:
                        break
                    candidate_item_id = _extract_wikidata_item_id(str(cast_item_id or "").strip())
                    if not candidate_item_id:
                        continue
                    wikidata_cast_scanned += 1
                    person_summary, person_summary_fetch_error = _fetch_wikidata_summary(candidate_item_id)
                    if person_summary_fetch_error or not isinstance(person_summary, dict):
                        continue
                    label = str(person_summary.get("label") or "").strip()
                    enwiki_title = str(person_summary.get("enwiki_title") or "").strip()
                    if not label and not enwiki_title:
                        continue
                    show_cast_wikidata_candidates[candidate_item_id] = {
                        "label": label,
                        "enwiki_title": enwiki_title,
                        "source": "show_wikidata_cast_claims",
                    }
            related_season_item_ids = show_wikidata_summary.get("season_item_ids")
            if isinstance(related_season_item_ids, list):
                for season_item_id in related_season_item_ids:
                    if wikidata_cast_scanned >= wikidata_cast_scan_cap:
                        break
                    season_candidate_id = _extract_wikidata_item_id(str(season_item_id or "").strip())
                    if not season_candidate_id:
                        continue
                    season_summary, season_fetch_error = _fetch_wikidata_summary(season_candidate_id)
                    if season_fetch_error or not isinstance(season_summary, dict):
                        continue
                    season_cast_item_ids = season_summary.get("cast_item_ids")
                    if not isinstance(season_cast_item_ids, list):
                        continue
                    for cast_item_id in season_cast_item_ids:
                        if wikidata_cast_scanned >= wikidata_cast_scan_cap:
                            break
                        cast_candidate_id = _extract_wikidata_item_id(str(cast_item_id or "").strip())
                        if not cast_candidate_id or cast_candidate_id in show_cast_wikidata_candidates:
                            continue
                        wikidata_cast_scanned += 1
                        person_summary, person_summary_fetch_error = _fetch_wikidata_summary(cast_candidate_id)
                        if person_summary_fetch_error or not isinstance(person_summary, dict):
                            continue
                        label = str(person_summary.get("label") or "").strip()
                        enwiki_title = str(person_summary.get("enwiki_title") or "").strip()
                        if not label and not enwiki_title:
                            continue
                        season_cast_wikidata_candidates[cast_candidate_id] = {
                            "label": label,
                            "enwiki_title": enwiki_title,
                            "source": "season_wikidata_cast_claims",
                        }
    merged_cast_wikidata_candidates: dict[str, dict[str, str]] = {
        **season_cast_wikidata_candidates,
        **show_cast_wikidata_candidates,
    }

    housewife_friend_ids: set[str] = set()
    if is_bravo_show:
        role_rows = pg.fetch_all(
            """
            SELECT DISTINCT sra.person_id::text AS person_id
            FROM core.show_cast_role_assignments sra
            JOIN core.show_role_catalog rc ON rc.id = sra.role_id
            WHERE sra.show_id = %s
              AND lower(rc.name) IN ('housewife', 'friend')
            """,
            [show_id],
        )
        housewife_friend_ids = {str(row.get("person_id") or "").strip() for row in role_rows if row.get("person_id")}

    rows = pg.fetch_all(
        """
        SELECT DISTINCT
          p.id,
          p.full_name,
          p.external_ids,
          cf.source_url AS fandom_url,
          ct.imdb_id AS cast_tmdb_imdb_id,
          ct.tmdb_id AS cast_tmdb_tmdb_id,
          ct.wikidata_id AS cast_tmdb_wikidata_id,
          ct.facebook_id AS cast_tmdb_facebook_id,
          ct.instagram_id AS cast_tmdb_instagram_id,
          ct.tiktok_id AS cast_tmdb_tiktok_id,
          ct.twitter_id AS cast_tmdb_twitter_id,
          ct.youtube_id AS cast_tmdb_youtube_id,
          ct.freebase_id AS cast_tmdb_freebase_id,
          ct.freebase_mid AS cast_tmdb_freebase_mid
        FROM core.v_show_cast sc
        JOIN core.people p ON p.id = sc.person_id
        LEFT JOIN core.cast_fandom cf ON cf.person_id = p.id AND cf.source = 'fandom'
        LEFT JOIN core.cast_tmdb ct ON ct.person_id = p.id
        WHERE sc.show_id = %s
        """,
        [show_id],
    )
    people_fandom_seed_urls = _collect_show_fandom_seed_urls(
        show_id,
        show_name=show_name,
        show_fandom_seed_urls=show_fandom_seed_urls,
        fandom_allowlist=fandom_allowlist,
    )
    show_fandom_domains = _extract_fandom_domains_from_urls(people_fandom_seed_urls, fandom_allowlist=fandom_allowlist)
    seeded_fandom_candidates_by_domain = _collect_seeded_fandom_candidate_urls_by_domain(
        people_fandom_seed_urls,
        fandom_allowlist=fandom_allowlist,
        stats=stats,
    )
    found: list[dict[str, Any]] = []
    seen_person_links: set[tuple[str, str, str]] = set()
    matched_show_cast_wikidata_candidates: set[str] = set()
    tmdb_external_ids_cache: dict[str, dict[str, str]] = {}

    def append_person_row(row: dict[str, Any]) -> None:
        person_key = str(row.get("entity_id") or "").strip()
        link_kind = _normalize_link_kind(str(row.get("link_kind") or "").strip())
        canonical_url = _canonicalize_url(str(row.get("url") or "").strip())
        if not person_key or not link_kind or not canonical_url:
            return
        dedupe_key = (person_key, link_kind, _url_key(canonical_url))
        if dedupe_key in seen_person_links:
            return
        seen_person_links.add(dedupe_key)
        row["url"] = canonical_url
        row["link_kind"] = link_kind
        found.append(row)
    for row in rows:
        if _stage_budget_exhausted(stats):
            break
        person_id = str(row.get("id"))
        name = str(row.get("full_name") or "").strip()
        fandom_url = str(row.get("fandom_url") or "").strip()
        external_ids = row.get("external_ids") if isinstance(row.get("external_ids"), dict) else {}
        imdb_id, imdb_source = _resolve_person_external_identifier(
            external_ids,
            keys=("imdb", "imdb_id"),
            fallback_value=row.get("cast_tmdb_imdb_id"),
            extractor=_extract_imdb_person_id,
        )
        tmdb_id, tmdb_source = _resolve_person_external_identifier(
            external_ids,
            keys=("tmdb", "tmdb_id"),
            fallback_value=row.get("cast_tmdb_tmdb_id"),
            extractor=_extract_tmdb_person_id,
        )
        wikidata_id, wikidata_source = _resolve_person_external_identifier(
            external_ids,
            keys=("wikidata", "wikidata_id"),
            fallback_value=row.get("cast_tmdb_wikidata_id"),
            extractor=_extract_wikidata_item_id,
        )
        cast_tmdb_freebase_id = _normalize_tmdb_external_id_value(row.get("cast_tmdb_freebase_id"))
        cast_tmdb_freebase_mid = _normalize_tmdb_external_id_value(row.get("cast_tmdb_freebase_mid"))
        cast_tmdb_facebook_id = _normalize_tmdb_external_id_value(row.get("cast_tmdb_facebook_id"))
        cast_tmdb_instagram_id = _normalize_tmdb_external_id_value(row.get("cast_tmdb_instagram_id"))
        cast_tmdb_tiktok_id = _normalize_tmdb_external_id_value(row.get("cast_tmdb_tiktok_id"))
        cast_tmdb_twitter_id = _normalize_tmdb_external_id_value(row.get("cast_tmdb_twitter_id"))
        cast_tmdb_youtube_id = _normalize_tmdb_external_id_value(row.get("cast_tmdb_youtube_id"))
        person_wikidata_summary: dict[str, Any] | None = None
        if not wikidata_id and name and merged_cast_wikidata_candidates:
            for candidate_item_id in sorted(merged_cast_wikidata_candidates.keys()):
                if candidate_item_id in matched_show_cast_wikidata_candidates:
                    continue
                candidate = merged_cast_wikidata_candidates[candidate_item_id]
                if _person_name_candidates_match(
                    name,
                    [
                        str(candidate.get("label") or "").strip() or None,
                        str(candidate.get("enwiki_title") or "").strip() or None,
                    ],
                ):
                    wikidata_id = candidate_item_id
                    wikidata_source = str(candidate.get("source") or "show_wikidata_cast_claims")
                    matched_show_cast_wikidata_candidates.add(candidate_item_id)
                    break
        if wikidata_id:
            person_wikidata_raw, person_wikidata_fetch_error = _fetch_wikidata_summary(wikidata_id)
            if not person_wikidata_fetch_error and isinstance(person_wikidata_raw, dict):
                person_wikidata_summary = person_wikidata_raw
                if not imdb_id:
                    candidate_imdb_id = str(person_wikidata_summary.get("imdb_id") or "").strip().lower()
                    if re.fullmatch(r"nm\d+", candidate_imdb_id, flags=re.IGNORECASE):
                        imdb_id = candidate_imdb_id
                        imdb_source = imdb_source or "wikidata_person_external_ids"
                if not tmdb_id:
                    candidate_tmdb_person_id = str(person_wikidata_summary.get("tmdb_person_id") or "").strip()
                    if re.fullmatch(r"\d+", candidate_tmdb_person_id):
                        tmdb_id = candidate_tmdb_person_id
                        tmdb_source = tmdb_source or "wikidata_person_external_ids"

        tmdb_external_payload: dict[str, str] = {}
        if tmdb_id:
            tmdb_external_payload = tmdb_external_ids_cache.get(tmdb_id, {})
            if not tmdb_external_payload:
                tmdb_external_payload = _fetch_tmdb_external_ids_payload(tmdb_id)
                tmdb_external_ids_cache[tmdb_id] = tmdb_external_payload
            if tmdb_external_payload:
                if not imdb_id:
                    candidate_imdb = _extract_imdb_person_id(tmdb_external_payload.get("imdb_id"))
                    if candidate_imdb:
                        imdb_id = candidate_imdb
                        imdb_source = imdb_source or "tmdb_external_ids"
                if not wikidata_id:
                    candidate_wikidata = _extract_wikidata_item_id(tmdb_external_payload.get("wikidata_id"))
                    if candidate_wikidata:
                        wikidata_id = candidate_wikidata
                        wikidata_source = wikidata_source or "tmdb_external_ids"
                try:
                    _persist_tmdb_external_ids_for_person(person_id, tmdb_id, tmdb_external_payload)
                except Exception:  # noqa: BLE001
                    pass

        if wikidata_id and person_wikidata_summary is None:
            person_wikidata_raw, person_wikidata_fetch_error = _fetch_wikidata_summary(wikidata_id)
            if not person_wikidata_fetch_error and isinstance(person_wikidata_raw, dict):
                person_wikidata_summary = person_wikidata_raw

        if imdb_id and name:
            imdb_url = _validated_or_carried_person_source_url(
                person_id=person_id,
                candidate_url=f"https://www.imdb.com/name/{imdb_id}/",
                kind="imdb",
                expected_name=name,
                fandom_allowlist=fandom_allowlist,
            )
            if imdb_url and imdb_source:
                append_person_row(
                    _build_person_link_row(
                        person_id=person_id,
                        link_kind="imdb",
                        label=f"{name} IMDb",
                        url=imdb_url,
                        source=imdb_source,
                    )
                )

        if tmdb_id and name:
            tmdb_url = _validated_or_carried_person_source_url(
                person_id=person_id,
                candidate_url=f"https://www.themoviedb.org/person/{tmdb_id}",
                kind="tmdb",
                expected_name=name,
                fandom_allowlist=fandom_allowlist,
            )
            if tmdb_url and tmdb_source:
                append_person_row(
                    _build_person_link_row(
                        person_id=person_id,
                        link_kind="tmdb",
                        label=f"{name} TMDb",
                        url=tmdb_url,
                        source=tmdb_source,
                    )
                )

        if wikidata_id:
            wikidata_url = _validated_person_knowledge_url(
                f"https://www.wikidata.org/wiki/{wikidata_id}",
                kind="wikidata",
                expected_name=name if name else None,
                fandom_allowlist=fandom_allowlist,
            )
            if wikidata_url and wikidata_source:
                append_person_row(
                    _build_person_link_row(
                        person_id=person_id,
                        link_kind="wikidata",
                        label=f"{name} Wikidata" if name else "Wikidata",
                        url=wikidata_url,
                        source=wikidata_source,
                    )
                )

        if name:
            wikipedia_url = _validated_person_knowledge_url(
                f"https://en.wikipedia.org/wiki/{quote(name.replace(' ', '_'))}",
                kind="wikipedia",
                expected_name=name,
                fandom_allowlist=fandom_allowlist,
            )
            if wikipedia_url:
                append_person_row(
                    _build_person_link_row(
                        person_id=person_id,
                        link_kind="wikipedia",
                        label=f"{name} Wikipedia",
                        url=wikipedia_url,
                        source="derived_validated",
                    )
                )

        if cast_tmdb_freebase_id or cast_tmdb_freebase_mid:
            _add_person_social_and_identifier_rows(
                found=found,
                seen_keys=seen_person_links,
                person_id=person_id,
                person_name=name,
                source="core.cast_tmdb_social_ids",
                freebase_id=cast_tmdb_freebase_id or cast_tmdb_freebase_mid,
            )
        if (
            cast_tmdb_twitter_id
            or cast_tmdb_instagram_id
            or cast_tmdb_facebook_id
            or cast_tmdb_youtube_id
            or cast_tmdb_tiktok_id
        ):
            _add_person_social_and_identifier_rows(
                found=found,
                seen_keys=seen_person_links,
                person_id=person_id,
                person_name=name,
                source="core.cast_tmdb_social_ids",
                twitter_usernames=[cast_tmdb_twitter_id] if cast_tmdb_twitter_id else [],
                instagram_usernames=[cast_tmdb_instagram_id] if cast_tmdb_instagram_id else [],
                facebook_usernames=[cast_tmdb_facebook_id] if cast_tmdb_facebook_id else [],
                youtube_channel_ids=[cast_tmdb_youtube_id] if cast_tmdb_youtube_id else [],
                tiktok_usernames=[cast_tmdb_tiktok_id] if cast_tmdb_tiktok_id else [],
            )

        tmdb_payload_freebase = _normalize_tmdb_external_id_value(tmdb_external_payload.get("freebase_id"))
        tmdb_payload_freebase_mid = _normalize_tmdb_external_id_value(tmdb_external_payload.get("freebase_mid"))
        if tmdb_payload_freebase or tmdb_payload_freebase_mid:
            _add_person_social_and_identifier_rows(
                found=found,
                seen_keys=seen_person_links,
                person_id=person_id,
                person_name=name,
                source="tmdb_external_ids_social",
                freebase_id=tmdb_payload_freebase or tmdb_payload_freebase_mid,
            )
        if tmdb_external_payload:
            tmdb_twitter = str(tmdb_external_payload.get("twitter_id") or "").strip()
            tmdb_instagram = str(tmdb_external_payload.get("instagram_id") or "").strip()
            tmdb_facebook = str(tmdb_external_payload.get("facebook_id") or "").strip()
            tmdb_youtube = str(tmdb_external_payload.get("youtube_id") or "").strip()
            tmdb_tiktok = str(tmdb_external_payload.get("tiktok_id") or "").strip()
            _add_person_social_and_identifier_rows(
                found=found,
                seen_keys=seen_person_links,
                person_id=person_id,
                person_name=name,
                source="tmdb_external_ids_social",
                twitter_usernames=[tmdb_twitter] if tmdb_twitter else [],
                instagram_usernames=[tmdb_instagram] if tmdb_instagram else [],
                facebook_usernames=[tmdb_facebook] if tmdb_facebook else [],
                youtube_channel_ids=[tmdb_youtube] if tmdb_youtube else [],
                tiktok_usernames=[tmdb_tiktok] if tmdb_tiktok else [],
            )

        if person_wikidata_summary:
            _add_person_social_and_identifier_rows(
                found=found,
                seen_keys=seen_person_links,
                person_id=person_id,
                person_name=name,
                source="connected_wikidata_identifiers",
                freebase_id=str(person_wikidata_summary.get("freebase_id") or "").strip() or None,
                famous_birthdays_id=str(person_wikidata_summary.get("famous_birthdays_id") or "").strip() or None,
                google_kg_id=str(person_wikidata_summary.get("google_kg_id") or "").strip() or None,
            )
            _add_person_social_and_identifier_rows(
                found=found,
                seen_keys=seen_person_links,
                person_id=person_id,
                person_name=name,
                source="connected_wikidata_social",
                twitter_usernames=[
                    str(value).strip()
                    for value in (person_wikidata_summary.get("twitter_usernames") or [])
                    if str(value).strip()
                ],
                instagram_usernames=[
                    str(value).strip()
                    for value in (person_wikidata_summary.get("instagram_usernames") or [])
                    if str(value).strip()
                ],
                facebook_usernames=[
                    str(value).strip()
                    for value in (person_wikidata_summary.get("facebook_usernames") or [])
                    if str(value).strip()
                ],
                youtube_channel_ids=[
                    str(value).strip()
                    for value in (person_wikidata_summary.get("youtube_channel_ids") or [])
                    if str(value).strip()
                ],
                tiktok_usernames=[
                    str(value).strip()
                    for value in (person_wikidata_summary.get("tiktok_usernames") or [])
                    if str(value).strip()
                ],
                reddit_usernames=[
                    str(value).strip()
                    for value in (person_wikidata_summary.get("reddit_usernames") or [])
                    if str(value).strip()
                ],
            )

        fandom_candidates = _discover_fandom_candidates_for_person(
            name=name,
            seeded_fandom_url=fandom_url if fandom_url else None,
            show_fandom_domains=show_fandom_domains,
            seeded_domain_candidates=seeded_fandom_candidates_by_domain,
            is_bravo_show=is_bravo_show,
            fandom_allowlist=fandom_allowlist,
            stats=stats,
        )
        max_candidates_per_person = _link_discovery_people_fandom_candidates_per_person_cap()
        ranked_fandom_candidates = sorted(
            fandom_candidates,
            key=lambda candidate_url: _score_fandom_candidate_url(candidate_url, expected_name=name),
            reverse=True,
        )
        seen_fandom_urls: set[str] = set()
        for fandom_candidate in ranked_fandom_candidates[: max(1, max_candidates_per_person)]:
            if not _can_attempt_fandom_candidate(stats):
                break
            if not is_allowlisted_fandom_domain(fandom_candidate, allowlist=fandom_allowlist):
                continue
            validated_fandom_url = _validated_person_knowledge_url(
                fandom_candidate,
                kind="fandom",
                expected_name=name if name else None,
                fandom_allowlist=fandom_allowlist,
            )
            if validated_fandom_url:
                canonical_fandom_url = _canonicalize_url(validated_fandom_url)
                if not canonical_fandom_url:
                    continue
                dedupe_key = _url_key(canonical_fandom_url)
                if dedupe_key in seen_fandom_urls:
                    continue
                seen_fandom_urls.add(dedupe_key)
                fandom_title = _extract_fandom_page_title_from_url(canonical_fandom_url)
                append_person_row(
                    _build_person_link_row(
                        person_id=person_id,
                        link_kind="fandom",
                        label=fandom_title or (name if name else "Fandom"),
                        url=canonical_fandom_url,
                        source="core.cast_fandom" if fandom_candidate == fandom_url else "fandom_search",
                        metadata=_build_fandom_link_metadata(canonical_fandom_url, title=fandom_title),
                    )
                )
        if is_bravo_show and person_id in housewife_friend_ids and name:
            slug = _slug(name)
            if slug:
                bravo_profile_url = _validated_person_knowledge_url(
                    f"https://www.bravotv.com/people/{slug}",
                    kind="bravo_profile",
                    expected_name=name,
                    fandom_allowlist=fandom_allowlist,
                )
                if bravo_profile_url:
                    append_person_row(
                        _build_person_link_row(
                            person_id=person_id,
                            link_kind="bravo_profile",
                            label=f"{name} Bravo profile",
                            url=bravo_profile_url,
                            source="cast_matrix_sync",
                        )
                    )
    return found


def _load_show_cast_names_by_person_id(show_id: str) -> dict[str, str]:
    rows = pg.fetch_all(
        """
        SELECT DISTINCT
          sc.person_id::text AS person_id,
          COALESCE(p.full_name, sc.cast_member_name) AS person_name
        FROM core.v_show_cast sc
        LEFT JOIN core.people p ON p.id = sc.person_id
        WHERE sc.show_id = %s
        """,
        [show_id],
    )
    out: dict[str, str] = {}
    for row in rows:
        person_id = str(row.get("person_id") or "").strip()
        person_name = str(row.get("person_name") or "").strip()
        if person_id and person_name:
            out[person_id] = person_name
    return out


def _scan_invalid_person_knowledge_links(show_id: str) -> dict[str, Any]:
    supported_link_kinds = _PERSON_SOURCE_LINK_KINDS
    cast_people = _load_show_cast_names_by_person_id(show_id)
    fandom_allowlist = load_fandom_community_allowlist()
    links = pg.fetch_all(
        """
        SELECT
          id::text AS id,
          entity_id::text AS person_id,
          link_kind,
          status,
          url
        FROM core.entity_links
        WHERE show_id = %s
          AND entity_type = 'person'
          AND link_kind = ANY(%s::text[])
        """,
        [show_id, sorted(supported_link_kinds)],
    )

    invalid_rows: list[dict[str, Any]] = []
    pending_promotions: list[dict[str, Any]] = []
    validation_failures = 0
    for row in links:
        link_id = str(row.get("id") or "").strip()
        person_id = str(row.get("person_id") or "").strip()
        link_kind = _normalize_link_kind(str(row.get("link_kind") or "").strip().lower())
        status = str(row.get("status") or "").strip().lower()
        url = str(row.get("url") or "").strip()
        if not link_id or not person_id or link_kind not in supported_link_kinds or not url:
            continue

        expected_name = cast_people.get(person_id)
        if not expected_name:
            invalid_rows.append({**row, "reason": "person_not_in_show_cast"})
            continue

        resolved, outcome = _validate_person_knowledge_url(
            url,
            kind=link_kind,
            expected_name=expected_name,
            fandom_allowlist=fandom_allowlist,
        )
        if status == "pending":
            if outcome == "valid":
                pending_promotions.append({**row, "resolved_url": resolved or url})
            else:
                invalid_rows.append({**row, "reason": "pending_not_allowed_for_person_source"})
                if outcome == "fetch_error":
                    validation_failures += 1
            continue
        if outcome == "fetch_error":
            validation_failures += 1
            continue
        if outcome != "valid":
            invalid_rows.append({**row, "reason": "invalid_or_mismatched_owner"})

    return {
        "scanned_rows": links,
        "scanned": len(links),
        "invalid_rows": invalid_rows,
        "pending_promotions": pending_promotions,
        "validation_failures": validation_failures,
    }


def _delete_entity_links_by_id(link_ids: list[str], *, conn: Any | None = None) -> int:
    ids = [str(link_id).strip() for link_id in link_ids if str(link_id).strip()]
    if not ids:
        return 0
    if conn is not None:
        with pg.db_cursor(conn=conn) as cur:
            cur.execute(
                """
                DELETE FROM core.entity_links
                WHERE id = ANY(%s::uuid[])
                RETURNING id
                """,
                [ids],
            )
            deleted_rows = [dict(row) for row in cur.fetchall()]
    else:
        deleted_rows = pg.execute_returning(
            """
            DELETE FROM core.entity_links
            WHERE id = ANY(%s::uuid[])
            RETURNING id
            """,
            [ids],
        )
    return len(deleted_rows)


def _reason_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    out: dict[str, int] = {}
    for row in rows:
        reason = str(row.get("reason") or "").strip().lower()
        if not reason:
            continue
        out[reason] = out.get(reason, 0) + 1
    return out


def _promote_pending_person_source_links(rows: list[dict[str, Any]], *, conn: Any | None = None) -> int:
    promoted = 0
    for row in rows:
        link_id = str(row.get("id") or "").strip()
        resolved_url = _canonicalize_url(str(row.get("resolved_url") or row.get("url") or "").strip())
        if not link_id or not resolved_url:
            continue
        try:
            if conn is not None:
                with pg.db_cursor(conn=conn) as cur:
                    cur.execute(
                        """
                        UPDATE core.entity_links
                        SET
                          status = 'approved',
                          confidence = 0.95,
                          url = %s,
                          url_key = %s,
                          updated_at = NOW()
                        WHERE id = %s::uuid
                        RETURNING id
                        """,
                        [resolved_url, _url_key(resolved_url), link_id],
                    )
                    updated = [dict(row) for row in cur.fetchall()]
            else:
                updated = pg.execute_returning(
                    """
                    UPDATE core.entity_links
                    SET
                      status = 'approved',
                      confidence = 0.95,
                      url = %s,
                      url_key = %s,
                      updated_at = NOW()
                    WHERE id = %s::uuid
                    RETURNING id
                    """,
                    [resolved_url, _url_key(resolved_url), link_id],
                )
        except Exception as exc:  # noqa: BLE001
            if _is_duplicate_violation(exc, constraint="entity_links_unique_active"):
                _delete_entity_links_by_id([link_id], conn=conn)
                continue
            raise
        promoted += len(updated)
    return promoted


def _cleanup_invalid_person_knowledge_links(show_id: str) -> dict[str, Any]:
    scan = _scan_invalid_person_knowledge_links(show_id)
    invalid_rows = scan.get("invalid_rows") if isinstance(scan.get("invalid_rows"), list) else []
    pending_promotions = scan.get("pending_promotions") if isinstance(scan.get("pending_promotions"), list) else []
    invalid_ids = [str(row.get("id") or "").strip() for row in invalid_rows if row.get("id")]

    with pg.db_connection() as conn:
        promoted = _promote_pending_person_source_links(pending_promotions, conn=conn)
        deleted = _delete_entity_links_by_id(invalid_ids, conn=conn)
    return {
        "scanned": int(scan.get("scanned") or 0),
        "invalid": len(invalid_rows),
        "promoted": promoted,
        "deleted": deleted,
        "deleted_by_reason": _reason_counts(invalid_rows),
        "validation_failures": int(scan.get("validation_failures") or 0),
    }


def _scan_invalid_person_social_links(show_id: str) -> dict[str, Any]:
    supported_link_kinds = _PERSON_SOCIAL_LINK_KINDS
    cast_people = _load_show_cast_names_by_person_id(show_id)
    links = pg.fetch_all(
        """
        SELECT
          id::text AS id,
          entity_id::text AS person_id,
          link_kind,
          status,
          url
        FROM core.entity_links
        WHERE show_id = %s
          AND entity_type = 'person'
          AND link_kind = ANY(%s::text[])
          AND lower(status) <> 'rejected'
        """,
        [show_id, sorted(supported_link_kinds)],
    )

    invalid_rows: list[dict[str, Any]] = []
    pending_promotions: list[dict[str, Any]] = []
    validation_failures = 0
    for row in links:
        link_id = str(row.get("id") or "").strip()
        person_id = str(row.get("person_id") or "").strip()
        link_kind = _normalize_link_kind(str(row.get("link_kind") or "").strip().lower())
        status = str(row.get("status") or "").strip().lower()
        url = str(row.get("url") or "").strip()
        if not link_id or not person_id or link_kind not in supported_link_kinds or not url:
            continue

        expected_name = cast_people.get(person_id)
        if not expected_name:
            invalid_rows.append({**row, "reason": "person_not_in_show_cast"})
            continue

        resolved, outcome = _validate_person_social_url(url, kind=link_kind)
        if status == "pending":
            if outcome == "valid":
                pending_promotions.append({**row, "resolved_url": resolved or url})
            else:
                invalid_rows.append({**row, "reason": "pending_not_allowed_for_person_social"})
                if outcome == "fetch_error":
                    validation_failures += 1
            continue
        if outcome == "fetch_error":
            validation_failures += 1
            continue
        if outcome != "valid":
            invalid_rows.append({**row, "reason": "social_profile_missing_or_invalid"})

    return {
        "scanned_rows": links,
        "scanned": len(links),
        "invalid_rows": invalid_rows,
        "pending_promotions": pending_promotions,
        "validation_failures": validation_failures,
    }


def _cleanup_invalid_person_social_links(show_id: str) -> dict[str, Any]:
    scan = _scan_invalid_person_social_links(show_id)
    invalid_rows = scan.get("invalid_rows") if isinstance(scan.get("invalid_rows"), list) else []
    pending_promotions = scan.get("pending_promotions") if isinstance(scan.get("pending_promotions"), list) else []
    invalid_ids = [str(row.get("id") or "").strip() for row in invalid_rows if row.get("id")]

    with pg.db_connection() as conn:
        promoted = _promote_pending_person_source_links(pending_promotions, conn=conn)
        deleted = _delete_entity_links_by_id(invalid_ids, conn=conn)
    return {
        "scanned": int(scan.get("scanned") or 0),
        "invalid": len(invalid_rows),
        "promoted": promoted,
        "deleted": deleted,
        "deleted_by_reason": _reason_counts(invalid_rows),
        "validation_failures": int(scan.get("validation_failures") or 0),
    }


def _scan_invalid_show_knowledge_links(show_id: str) -> dict[str, Any]:
    show_row = pg.fetch_one(
        """
        SELECT name, wikidata_id, networks
        FROM core.shows
        WHERE id = %s
        LIMIT 1
        """,
        [show_id],
    )
    show_name = str((show_row or {}).get("name") or "").strip()
    network_values = [
        str(value).strip() for value in ((show_row or {}).get("networks") or []) if isinstance(value, str)
    ]
    network_tokens = _normalize_show_network_tokens(network_values)
    show_wikidata_id = _resolve_show_wikidata_id(show_id, (show_row or {}).get("wikidata_id"))
    curated_fandom_domains = _curated_show_fandom_domains(show_name)
    fandom_allowlist = _effective_fandom_allowlist(
        show_name=show_name,
        network_tokens=network_tokens,
        base_allowlist=load_fandom_community_allowlist(),
    )

    links = pg.fetch_all(
        """
        SELECT
          id::text AS id,
          entity_type,
          link_kind,
          status,
          url,
          source,
          discovered_by
        FROM core.entity_links
        WHERE show_id = %s
          AND entity_type IN ('show', 'season')
          AND lower(link_kind) IN ('fandom', 'wikia', 'wikipedia')
          AND lower(status) <> 'rejected'
        """,
        [show_id],
    )

    invalid_rows: list[dict[str, Any]] = []
    validation_failures = 0
    for row in links:
        link_kind = _normalize_link_kind(str(row.get("link_kind") or "").strip().lower())
        entity_type = str(row.get("entity_type") or "").strip().lower()
        url = _canonicalize_url(str(row.get("url") or "").strip())
        if not url or link_kind not in {"fandom", "wikipedia"}:
            continue

        if link_kind == "fandom":
            parsed = urlparse(url)
            host = str(parsed.hostname or "").strip().lower()
            if entity_type == "show" and curated_fandom_domains and host and host not in curated_fandom_domains:
                invalid_rows.append({**row, "reason": "fandom_domain_mismatch"})
                continue
            if not is_allowlisted_fandom_domain(url, allowlist=fandom_allowlist):
                invalid_rows.append({**row, "reason": "fandom_not_allowlisted"})
                continue
            # Show-level fandom seed domains are valid discovery seeds even without a /wiki page path.
            # Keep them persisted so refresh can continue deriving season/cast candidates from them.
            if entity_type == "show" and _is_fandom_seed_url(url):
                continue

            status_code, html, resolved_url, fetch_error = _fetch_html_with_status(
                url,
                timeout=_source_timeout_seconds("fandom"),
            )
            if fetch_error or status_code is None:
                validation_failures += 1
                continue
            if status_code >= 400 or not html:
                invalid_rows.append({**row, "reason": "fandom_missing_or_invalid"})
                continue
            resolved = str(resolved_url or url)
            if is_missing_fandom_page(html, resolved):
                invalid_rows.append({**row, "reason": "fandom_missing_or_invalid"})
                continue
            continue

        resolved_wikipedia_url, _, wikipedia_error = _resolve_wikipedia_url(url)
        if wikipedia_error == "fetch_error":
            validation_failures += 1
            continue
        if wikipedia_error == "missing":
            invalid_rows.append({**row, "reason": "wikipedia_missing"})
            continue
        if show_wikidata_id and entity_type == "show":
            candidate_wikidata_id = _resolve_wikipedia_wikidata_id(resolved_wikipedia_url or url)
            if candidate_wikidata_id and candidate_wikidata_id != show_wikidata_id:
                invalid_rows.append({**row, "reason": "wikipedia_variant_mismatch"})
                continue
        if show_wikidata_id and entity_type == "season":
            candidate_wikidata_id = _resolve_wikipedia_wikidata_id(resolved_wikipedia_url or url)
            if candidate_wikidata_id:
                season_summary, season_fetch_error = _fetch_wikidata_summary(candidate_wikidata_id)
                if season_fetch_error:
                    validation_failures += 1
                    continue
                if isinstance(season_summary, dict):
                    part_of_series_item_ids = season_summary.get("part_of_series_item_ids")
                    if (
                        isinstance(part_of_series_item_ids, list)
                        and part_of_series_item_ids
                        and show_wikidata_id not in part_of_series_item_ids
                    ):
                        invalid_rows.append({**row, "reason": "wikipedia_variant_mismatch"})
                        continue

    return {
        "scanned_rows": links,
        "scanned": len(links),
        "invalid_rows": invalid_rows,
        "validation_failures": validation_failures,
    }


def _cleanup_invalid_show_knowledge_links(show_id: str) -> dict[str, Any]:
    scan = _scan_invalid_show_knowledge_links(show_id)
    invalid_rows = scan.get("invalid_rows") if isinstance(scan.get("invalid_rows"), list) else []

    invalid_ids: list[str] = []
    for row in invalid_rows:
        row_id = str(row.get("id") or "").strip()
        if not row_id:
            continue
        invalid_ids.append(row_id)

    with pg.db_connection() as conn:
        deleted = _delete_entity_links_by_id(invalid_ids, conn=conn)

    return {
        "scanned": int(scan.get("scanned") or 0),
        "invalid": len(invalid_rows),
        "deleted": deleted,
        "manual_skipped": 0,
        "deleted_by_reason": _reason_counts(invalid_rows),
        "validation_failures": int(scan.get("validation_failures") or 0),
    }


def _promote_pending_links_to_approved(show_id: str, *, include_people: bool) -> int:
    entity_types = ["show", "season"]
    if include_people:
        entity_types.append("person")
    rows = pg.execute_returning(
        """
        UPDATE core.entity_links
        SET
          status = 'approved',
          confidence = CASE
            WHEN confidence IS NULL THEN 0.95
            WHEN confidence < 0.95 THEN 0.95
            ELSE confidence
          END,
          updated_at = NOW()
        WHERE show_id = %s::uuid
          AND entity_type = ANY(%s::text[])
          AND lower(status) = 'pending'
          AND (
            (
              lower(link_group) = 'knowledge'
              AND lower(link_kind) = ANY(%s::text[])
            )
            OR lower(link_kind) = 'network_blog'
          )
        RETURNING id
        """,
        [show_id, entity_types, sorted(_AUTO_APPROVE_KNOWLEDGE_LINK_KINDS)],
    )
    return len(rows)


@fandom_router.get("/allowlist")
def get_fandom_allowlist(_: AdminUser) -> dict[str, Any]:
    domains, source = load_fandom_community_allowlist_with_source()
    return {
        "domains": list(domains),
        "source": source,
        "count": len(domains),
    }


@fandom_router.put("/allowlist")
def put_fandom_allowlist(payload: FandomAllowlistUpdateRequest, admin: AdminUser) -> dict[str, Any]:
    actor = str(admin.get("email") or admin.get("id") or "admin")
    normalized_domains: list[str] = []
    for raw_domain in payload.domains:
        normalized = normalize_fandom_community_domain(raw_domain)
        if normalized and normalized not in normalized_domains:
            normalized_domains.append(normalized)
    if not normalized_domains:
        raise HTTPException(status_code=400, detail="At least one valid fandom domain is required.")

    try:
        with pg.db_connection() as conn:
            with pg.db_cursor(conn=conn) as cur:
                cur.execute(
                    """
                    UPDATE core.fandom_community_allowlist
                    SET is_active = false, updated_at = NOW(), updated_by = %s
                    """,
                    [actor],
                )
                for domain in normalized_domains:
                    cur.execute(
                        """
                        INSERT INTO core.fandom_community_allowlist (domain, is_active, updated_by, updated_at)
                        VALUES (%s, true, %s, NOW())
                        ON CONFLICT (domain)
                        DO UPDATE SET
                          is_active = EXCLUDED.is_active,
                          updated_by = EXCLUDED.updated_by,
                          updated_at = NOW()
                        """,
                        [domain, actor],
                    )
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=502, detail=f"Failed to update fandom allowlist: {exc}") from exc

    refresh_fandom_community_allowlist_cache()
    return {
        "domains": normalized_domains,
        "count": len(normalized_domains),
        "updated_by": actor,
    }


def _infer_legacy_knowledge_kind_from_url(url: str) -> str | None:
    host = str(urlparse(str(url or "").strip()).hostname or "").strip().lower()
    if not host:
        return None
    if "wikipedia.org" in host:
        return "wikipedia"
    if "wikidata.org" in host:
        return "wikidata"
    return None


def _normalize_legacy_knowledge_link_kinds(show_id: str) -> int:
    rows = pg.fetch_all(
        """
        SELECT id::text AS id, url, link_kind
        FROM core.entity_links
        WHERE show_id = %s
          AND lower(status) <> 'rejected'
          AND lower(link_kind) IN ('kg', 'knowledge_graph', 'knowledge')
        """,
        [show_id],
    )
    if not rows:
        return 0

    normalized_count = 0
    with pg.db_connection() as conn:
        for row in rows:
            link_id = str(row.get("id") or "").strip()
            if not link_id:
                continue
            normalized_kind = _infer_legacy_knowledge_kind_from_url(str(row.get("url") or "").strip())
            if not normalized_kind:
                continue
            try:
                with pg.db_cursor(conn=conn) as cur:
                    cur.execute(
                        """
                        UPDATE core.entity_links
                        SET link_kind = %s,
                            updated_at = NOW()
                        WHERE id = %s::uuid
                        RETURNING id
                        """,
                        [normalized_kind, link_id],
                    )
                    updated_rows = [dict(updated_row) for updated_row in cur.fetchall()]
            except Exception as exc:  # noqa: BLE001
                if _is_duplicate_violation(exc, constraint="entity_links_unique_active"):
                    _delete_entity_links_by_id([link_id], conn=conn)
                    continue
                raise
            normalized_count += len(updated_rows)
    return normalized_count


def _count_discovery_scan_targets(show_id: str) -> dict[str, int]:
    season_row = pg.fetch_one(
        """
        SELECT count(*)::int AS season_count
        FROM core.seasons
        WHERE show_id = %s::uuid
          AND season_number > 0
        """,
        [show_id],
    )
    people_row = pg.fetch_one(
        """
        SELECT count(DISTINCT person_id)::int AS people_count
        FROM core.show_cast
        WHERE show_id = %s::uuid
        """,
        [show_id],
    )
    return {
        "show_scanned": 1,
        "season_scanned": int((season_row or {}).get("season_count") or 0),
        "people_scanned": int((people_row or {}).get("people_count") or 0),
    }


def _collect_bulk_link_inputs(payload: LinkBulkAddRequest) -> list[str]:
    raw_entries: list[str] = []
    if payload.input_text:
        raw_entries.extend(re.split(r"[\n,;]+", payload.input_text))
    raw_entries.extend(payload.inputs)

    deduped: list[str] = []
    seen: set[str] = set()
    for raw_entry in raw_entries:
        value = str(raw_entry or "").strip()
        if not value:
            continue
        key = value.casefold()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(value)
    return deduped


def _iso_utc_now() -> str:
    return datetime.now(UTC).isoformat()


def _sse_event(event: str, payload: dict[str, Any]) -> str:
    return f"event: {event}\ndata: {json.dumps(payload, default=str)}\n\n"


def _run_show_link_discovery(
    *,
    show_id_str: str,
    payload: LinkDiscoverRequest,
    db: SupabaseAdminClient,
    actor: str,
    stage_callback: Callable[[str, dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    stage_callback = stage_callback or (lambda _stage, _payload: None)
    run_id = str(uuid4())
    started_at = _iso_utc_now()
    started_perf = perf_counter()
    run_timeout_seconds = _link_discovery_run_timeout_seconds()

    def emit(stage: str, payload_in: dict[str, Any]) -> None:
        elapsed_ms = int((perf_counter() - started_perf) * 1000)
        payload_with_meta = {
            "show_id": show_id_str,
            "run_id": run_id,
            "timestamp": _iso_utc_now(),
            "elapsed_ms": elapsed_ms,
            **payload_in,
        }
        stage_callback(stage, payload_with_meta)

    def run_timeout_payload(stage: str) -> dict[str, Any]:
        elapsed_ms = int((perf_counter() - started_perf) * 1000)
        return {
            "reason": "server_processing_timeout",
            "stage": stage,
            "elapsed_ms": elapsed_ms,
            "budget_ms": int(run_timeout_seconds * 1000),
            "detail": f"Discovery exceeded timeout budget at stage {stage}.",
        }

    scan_targets = _count_discovery_scan_targets(show_id_str)
    discovery_stats: dict[str, Any] = {"fandom_candidates_tested": 0, "skipped_fetch_budget": 0}
    stage_timings_ms: dict[str, int] = {
        "show_stage_ms": 0,
        "season_stage_ms": 0,
        "people_stage_ms": 0,
        "validation_ms": 0,
    }

    timed_out = False
    timeout_context: dict[str, Any] | None = None
    discovered: list[dict[str, Any]] = []

    show_budget = _start_discovery_stage_budget(discovery_stats, stage="show")
    emit("show_discovery_started", {"budget": show_budget})
    show_started_perf = perf_counter()
    discovered_show_rows = _discover_show_links(show_id_str, stats=discovery_stats)
    discovered.extend(discovered_show_rows)
    stage_timings_ms["show_stage_ms"] = int((perf_counter() - show_started_perf) * 1000)
    emit(
        "show_discovery_completed",
        {
            "rows": len(discovered_show_rows),
            "stage_elapsed_ms": stage_timings_ms["show_stage_ms"],
            "budget_exhausted": bool(discovery_stats.get("__stage_budget_exhausted") is True),
            "budget_reason": str(discovery_stats.get("__stage_budget_exhausted_reason") or "") or None,
        },
    )

    if run_timeout_seconds > 0 and (perf_counter() - started_perf) >= run_timeout_seconds:
        timed_out = True
        timeout_context = run_timeout_payload("show_discovery")
        emit("run_timeout", timeout_context)

    show_fandom_seed_urls = [
        str(row.get("url") or "").strip()
        for row in discovered
        if str(row.get("entity_type") or "").strip().lower() == "show"
        and _normalize_link_kind(str(row.get("link_kind") or "").strip().lower()) == "fandom"
    ]

    if payload.include_seasons and not timed_out:
        season_budget = _start_discovery_stage_budget(discovery_stats, stage="season")
        emit("season_discovery_started", {"seed_urls": len(show_fandom_seed_urls), "budget": season_budget})
        season_started_perf = perf_counter()
        season_rows = _discover_season_links(
            show_id_str,
            show_fandom_seed_urls=show_fandom_seed_urls,
            stats=discovery_stats,
        )
        discovered.extend(season_rows)
        stage_timings_ms["season_stage_ms"] = int((perf_counter() - season_started_perf) * 1000)
        emit(
            "season_discovery_completed",
            {
                "rows": len(season_rows),
                "stage_elapsed_ms": stage_timings_ms["season_stage_ms"],
                "budget_exhausted": bool(discovery_stats.get("__stage_budget_exhausted") is True),
                "budget_reason": str(discovery_stats.get("__stage_budget_exhausted_reason") or "") or None,
            },
        )
        if run_timeout_seconds > 0 and (perf_counter() - started_perf) >= run_timeout_seconds:
            timed_out = True
            timeout_context = run_timeout_payload("season_discovery")
            emit("run_timeout", timeout_context)

    if payload.include_people and not timed_out:
        people_budget = _start_discovery_stage_budget(discovery_stats, stage="people")
        emit("people_discovery_started", {"seed_urls": len(show_fandom_seed_urls), "budget": people_budget})
        people_started_perf = perf_counter()
        people_rows = _discover_people_links(
            show_id_str,
            show_fandom_seed_urls=show_fandom_seed_urls,
            stats=discovery_stats,
        )
        discovered.extend(people_rows)
        stage_timings_ms["people_stage_ms"] = int((perf_counter() - people_started_perf) * 1000)
        emit(
            "people_discovery_completed",
            {
                "rows": len(people_rows),
                "stage_elapsed_ms": stage_timings_ms["people_stage_ms"],
                "budget_exhausted": bool(discovery_stats.get("__stage_budget_exhausted") is True),
                "budget_reason": str(discovery_stats.get("__stage_budget_exhausted_reason") or "") or None,
            },
        )
        if run_timeout_seconds > 0 and (perf_counter() - started_perf) >= run_timeout_seconds:
            timed_out = True
            timeout_context = run_timeout_payload("people_discovery")
            emit("run_timeout", timeout_context)

    upserted = 0
    approved_added = 0
    by_group: dict[str, int] = {}
    by_kind: dict[str, int] = {}
    by_entity_type: dict[str, int] = {}
    fandom_links_by_entity: dict[str, int] = {"show": 0, "season": 0, "person": 0}
    fandom_domains_used: set[str] = set()
    tmdb_season_links_discovered = 0
    wikidata_identifier_links_added = 0
    wikidata_social_links_added = 0
    tmdb_social_links_added = 0
    emit("upsert_started", {"rows": len(discovered)})
    for row in discovered:
        entity_type = str(row.get("entity_type") or "").strip().lower()
        link_kind = _normalize_link_kind(str(row.get("link_kind") or "").strip().lower())
        source = str(row.get("source") or "").strip().lower()
        parsed = urlparse(str(row["url"]))
        if not parsed.scheme.startswith("http"):
            continue
        default_status = "approved"
        row_status = str(row.get("status") or default_status).strip().lower()
        status = row_status if row_status in {"pending", "approved", "rejected"} else default_status
        if entity_type == "person" and link_kind in _PERSON_SOURCE_LINK_KINDS and status != "approved":
            continue
        confidence_raw = row.get("confidence")
        if isinstance(confidence_raw, (int, float)):
            confidence = float(confidence_raw)
        else:
            confidence = 0.95 if status == "approved" else 0.65
        _upsert_link(
            db,
            show_id=show_id_str,
            entity_type=entity_type or row["entity_type"],
            entity_id=str(row["entity_id"]),
            link_group=row["link_group"],
            link_kind=link_kind or _normalize_link_kind(str(row["link_kind"])),
            url=str(row["url"]),
            label=(str(row.get("label")) if row.get("label") else None),
            season_number=int(row.get("season_number") or 0),
            status=status,
            confidence=confidence,
            source=(str(row.get("source")) if row.get("source") else None),
            discovered_by="backend_discovery",
            metadata=(row.get("metadata") if isinstance(row.get("metadata"), dict) else {}),
            actor=actor,
        )
        upserted += 1
        if status == "approved":
            approved_added += 1
        by_group[row["link_group"]] = by_group.get(row["link_group"], 0) + 1
        by_kind[link_kind] = by_kind.get(link_kind, 0) + 1
        by_entity_type[entity_type] = by_entity_type.get(entity_type, 0) + 1
        if link_kind == "fandom" and entity_type in {"show", "season", "person"}:
            fandom_links_by_entity[entity_type] = fandom_links_by_entity.get(entity_type, 0) + 1
            host = str(urlparse(str(row.get("url") or "")).hostname or "").strip().lower()
            if host:
                fandom_domains_used.add(host)
        if link_kind == "tmdb" and entity_type == "season":
            tmdb_season_links_discovered += 1
        if source == "connected_wikidata_identifiers":
            wikidata_identifier_links_added += 1
        if source == "connected_wikidata_social":
            wikidata_social_links_added += 1
        if source in {"tmdb_external_ids_social", "core.cast_tmdb_social_ids"}:
            tmdb_social_links_added += 1
    emit("upsert_completed", {"upserted": upserted, "approved_added": approved_added})

    validation_started_perf = perf_counter()
    legacy_rows_normalized = _normalize_legacy_knowledge_link_kinds(show_id_str)

    invalid_people_cleanup: dict[str, Any] = {"scanned": 0, "deleted": 0, "promoted": 0, "validation_failures": 0}
    invalid_people_social_cleanup: dict[str, Any] = {
        "scanned": 0,
        "deleted": 0,
        "promoted": 0,
        "validation_failures": 0,
    }
    if payload.include_people:
        invalid_people_cleanup = _cleanup_invalid_person_knowledge_links(show_id_str)
        invalid_people_social_cleanup = _cleanup_invalid_person_social_links(show_id_str)
    invalid_show_cleanup = _cleanup_invalid_show_knowledge_links(show_id_str)
    pending_promoted = _promote_pending_links_to_approved(show_id_str, include_people=payload.include_people)
    links_validated = (
        int(invalid_show_cleanup.get("scanned") or 0)
        + int(invalid_people_cleanup.get("scanned") or 0)
        + int(invalid_people_social_cleanup.get("scanned") or 0)
    )
    links_promoted = (
        int(pending_promoted or 0)
        + int(invalid_people_cleanup.get("promoted") or 0)
        + int(invalid_people_social_cleanup.get("promoted") or 0)
    )
    deleted_invalid = (
        int(invalid_people_cleanup.get("deleted") or 0)
        + int(invalid_people_social_cleanup.get("deleted") or 0)
        + int(invalid_show_cleanup.get("deleted") or 0)
    )
    skipped_fetch_error = (
        int(invalid_people_cleanup.get("validation_failures") or 0)
        + int(invalid_people_social_cleanup.get("validation_failures") or 0)
        + int(invalid_show_cleanup.get("validation_failures") or 0)
    )
    skipped_fetch_budget = int(discovery_stats.get("skipped_fetch_budget") or 0)
    stage_timings_ms["validation_ms"] = int((perf_counter() - validation_started_perf) * 1000)
    validation_reasons: dict[str, int] = {}
    for bucket in (
        invalid_people_cleanup.get("deleted_by_reason"),
        invalid_people_social_cleanup.get("deleted_by_reason"),
        invalid_show_cleanup.get("deleted_by_reason"),
    ):
        if not isinstance(bucket, dict):
            continue
        for reason, count in bucket.items():
            key = str(reason).strip().lower()
            if not key:
                continue
            validation_reasons[key] = validation_reasons.get(key, 0) + int(count or 0)
    stage_counts = {
        "show_scanned": int(scan_targets.get("show_scanned") or 0),
        "season_scanned": int(scan_targets.get("season_scanned") or 0) if payload.include_seasons else 0,
        "people_scanned": int(scan_targets.get("people_scanned") or 0) if payload.include_people else 0,
        "legacy_rows_normalized": legacy_rows_normalized,
        "links_validated": links_validated,
        "links_promoted": links_promoted,
    }
    finished_at = _iso_utc_now()
    duration_ms = int((perf_counter() - started_perf) * 1000)
    emit(
        "cleanup_completed",
        {
            "links_validated": links_validated,
            "links_promoted": links_promoted,
            "deleted_invalid": deleted_invalid,
            "skipped_fetch_error": skipped_fetch_error,
            "skipped_fetch_budget": skipped_fetch_budget,
            "stage_elapsed_ms": stage_timings_ms["validation_ms"],
        },
    )
    if timed_out and timeout_context:
        logger.warning(
            "show_links_discovery_timed_out run_id=%s show_id=%s stage=%s elapsed_ms=%s budget_ms=%s",
            run_id,
            show_id_str,
            timeout_context.get("stage"),
            timeout_context.get("elapsed_ms"),
            timeout_context.get("budget_ms"),
        )
    logger.info(
        "show_links_discovery_completed run_id=%s show_id=%s duration_ms=%s discovered=%s timed_out=%s",
        run_id,
        show_id_str,
        duration_ms,
        upserted,
        timed_out,
    )
    return {
        "show_id": show_id_str,
        "run_id": run_id,
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_ms": duration_ms,
        "status": "timed_out" if timed_out else "ok",
        "timed_out": timed_out,
        "timeout": timeout_context,
        "discovered": upserted,
        "status_counts": {
            "approved_added": approved_added,
            "deleted_invalid": deleted_invalid,
            "skipped_fetch_error": skipped_fetch_error,
            "skipped_fetch_budget": skipped_fetch_budget,
        },
        "validation_reasons": validation_reasons,
        "counts_by_group": by_group,
        "counts_by_kind": by_kind,
        "counts_by_entity_type": by_entity_type,
        "fandom_domains_used": sorted(fandom_domains_used),
        "fandom_links_by_entity": fandom_links_by_entity,
        "fandom_candidates_tested": int(discovery_stats.get("fandom_candidates_tested") or 0),
        "tmdb_season_links_discovered": tmdb_season_links_discovered,
        "wikidata_identifier_links_added": wikidata_identifier_links_added,
        "wikidata_social_links_added": wikidata_social_links_added,
        "tmdb_social_links_added": tmdb_social_links_added,
        "invalid_people_links_deleted": int(invalid_people_cleanup.get("deleted") or 0),
        "pending_person_source_links_promoted": int(invalid_people_cleanup.get("promoted") or 0),
        "invalid_people_links_validation_failures": int(invalid_people_cleanup.get("validation_failures") or 0),
        "invalid_people_social_links_deleted": int(invalid_people_social_cleanup.get("deleted") or 0),
        "pending_person_social_links_promoted": int(invalid_people_social_cleanup.get("promoted") or 0),
        "invalid_people_social_links_validation_failures": int(
            invalid_people_social_cleanup.get("validation_failures") or 0
        ),
        "invalid_show_links_deleted": int(invalid_show_cleanup.get("deleted") or 0),
        "invalid_show_links_manual_skipped": int(invalid_show_cleanup.get("manual_skipped") or 0),
        "invalid_show_links_validation_failures": int(invalid_show_cleanup.get("validation_failures") or 0),
        "pending_links_promoted": pending_promoted,
        "stage_counts": stage_counts,
        "stage_timings_ms": stage_timings_ms,
        "show_stage_ms": int(stage_timings_ms["show_stage_ms"]),
        "season_stage_ms": int(stage_timings_ms["season_stage_ms"]),
        "people_stage_ms": int(stage_timings_ms["people_stage_ms"]),
        "validation_ms": int(stage_timings_ms["validation_ms"]),
    }


@router.post("/{show_id}/links/discover")
def discover_show_links(
    show_id: UUID,
    payload: LinkDiscoverRequest,
    db: SupabaseAdminClient,
    admin: AdminUser,
) -> dict[str, Any]:
    show_id_str = str(show_id)
    if not _show_exists(show_id_str):
        raise HTTPException(status_code=404, detail="Show not found")
    actor = str(admin.get("email") or admin.get("id") or "admin")
    timeout_budget_seconds = _link_discovery_run_timeout_seconds()
    logger.info(
        "show_links_discovery_requested show_id=%s actor=%s timeout_budget_s=%s",
        show_id_str,
        actor,
        timeout_budget_seconds,
    )
    result = _run_show_link_discovery(show_id_str=show_id_str, payload=payload, db=db, actor=actor)
    result["timeout_budget_ms"] = int(timeout_budget_seconds * 1000)
    return result


@router.post("/{show_id}/links/discover/stream")
def discover_show_links_stream(
    show_id: UUID,
    payload: LinkDiscoverRequest,
    db: SupabaseAdminClient,
    admin: AdminUser,
) -> StreamingResponse:
    show_id_str = str(show_id)
    if not _show_exists(show_id_str):
        raise HTTPException(status_code=404, detail="Show not found")
    actor = str(admin.get("email") or admin.get("id") or "admin")

    def event_stream() -> Iterator[str]:
        event_queue: SimpleQueue[tuple[str, dict[str, Any]]] = SimpleQueue()
        done = threading.Event()
        result_box: dict[str, Any] = {}
        error_box: dict[str, Any] = {}
        heartbeat_interval_seconds = 5.0
        stream_started_perf = perf_counter()

        def on_stage(stage: str, stage_payload: dict[str, Any]) -> None:
            event_queue.put(("progress", {"stage": stage, **stage_payload}))

        def worker() -> None:
            try:
                result_box["result"] = _run_show_link_discovery(
                    show_id_str=show_id_str,
                    payload=payload,
                    db=db,
                    actor=actor,
                    stage_callback=on_stage,
                )
            except Exception as exc:  # noqa: BLE001
                error_box["error"] = str(exc)
            finally:
                done.set()

        worker_thread = threading.Thread(target=worker, daemon=True)
        worker_thread.start()
        yield _sse_event(
            "progress",
            {
                "show_id": show_id_str,
                "stage": "starting",
                "message": "Starting links discovery...",
                "timestamp": _iso_utc_now(),
            },
        )

        while True:
            if done.is_set() and event_queue.empty():
                break
            try:
                event_name, event_payload = event_queue.get(timeout=heartbeat_interval_seconds)
            except Empty:
                elapsed_ms = int((perf_counter() - stream_started_perf) * 1000)
                yield _sse_event(
                    "progress",
                    {
                        "show_id": show_id_str,
                        "stage": "heartbeat",
                        "heartbeat": True,
                        "elapsed_ms": elapsed_ms,
                        "timestamp": _iso_utc_now(),
                        "message": "Discovery still running...",
                    },
                )
                continue
            yield _sse_event(event_name, {"show_id": show_id_str, **event_payload})

        error_detail = str(error_box.get("error") or "").strip()
        if error_detail:
            yield _sse_event(
                "error",
                {
                    "show_id": show_id_str,
                    "stage": "error",
                    "error": "Links discovery failed",
                    "detail": error_detail,
                },
            )
            return

        result = result_box.get("result") if isinstance(result_box.get("result"), dict) else {}
        if isinstance(result, dict):
            yield _sse_event(
                "complete",
                {
                    "show_id": show_id_str,
                    "run_id": result.get("run_id"),
                    "duration_ms": result.get("duration_ms"),
                    "stage_counts": result.get("stage_counts"),
                    "stage_timings_ms": result.get("stage_timings_ms"),
                    "status_counts": result.get("status_counts"),
                    "validation_reasons": result.get("validation_reasons"),
                    "timed_out": result.get("timed_out"),
                    "timeout": result.get("timeout"),
                    "status": result.get("status"),
                    "discovered": result.get("discovered"),
                    "result": result,
                },
            )

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@router.post("/{show_id}/links/add")
def add_show_links(
    show_id: UUID,
    payload: LinkBulkAddRequest,
    db: SupabaseAdminClient,
    admin: AdminUser,
) -> dict[str, Any]:
    show_id_str = str(show_id)
    if not _show_exists(show_id_str):
        raise HTTPException(status_code=404, detail="Show not found")

    inputs = _collect_bulk_link_inputs(payload)
    if not inputs:
        raise HTTPException(status_code=400, detail="Provide at least one link or handle.")
    if len(inputs) > 100:
        raise HTTPException(status_code=400, detail="Add at most 100 links at a time.")

    actor = str(admin.get("email") or admin.get("id") or "admin")
    context = _load_show_link_classifier_context(show_id_str)
    upserted_count = 0
    connected_count = 0
    errors: list[dict[str, str]] = []
    assignments: list[dict[str, Any]] = []
    seen_keys: set[tuple[str, str, int, str, str]] = set()

    for raw_input in inputs:
        rows, error = _classify_submitted_link_input(raw_input, context)
        if error:
            errors.append({"input": raw_input, "error": error})
            continue

        for index, row in enumerate(rows):
            entity_type = str(row.get("entity_type") or "").strip().lower()
            entity_id = str(row.get("entity_id") or "").strip()
            if entity_type not in {"show", "season", "person"} or not entity_id:
                errors.append({"input": raw_input, "error": "Could not determine where this link belongs."})
                continue

            canonical_url = _canonicalize_url(str(row.get("url") or ""))
            if not canonical_url or not urlparse(canonical_url).scheme.startswith("http"):
                errors.append({"input": raw_input, "error": "Could not normalize URL."})
                continue

            season_number = int(row.get("season_number") or 0)
            link_kind = _normalize_link_kind(str(row.get("link_kind") or "external"))
            dedupe_key = (entity_type, entity_id, season_number, link_kind, _url_key(canonical_url))
            if dedupe_key in seen_keys:
                continue
            seen_keys.add(dedupe_key)

            link_group = str(row.get("link_group") or "other")
            if link_group not in {"official", "social", "knowledge", "cast_announcements", "other"}:
                link_group = "other"
            label = str(row.get("label") or _LINK_KIND_LABELS.get(link_kind, "External Link")).strip() or None
            metadata = row.get("metadata") if isinstance(row.get("metadata"), dict) else {}

            persisted = _upsert_link(
                db,
                show_id=show_id_str,
                entity_type=entity_type,
                entity_id=entity_id,
                link_group=link_group,
                link_kind=link_kind,
                url=canonical_url,
                label=label,
                season_number=season_number,
                status="approved",
                confidence=0.95,
                source=str(row.get("source") or "manual_classifier"),
                discovered_by="manual_classifier",
                metadata={**metadata, "submitted_input": raw_input},
                actor=actor,
            )
            upserted_count += 1
            if index > 0:
                connected_count += 1
            assignments.append(
                {
                    "input": raw_input,
                    "id": persisted.get("id"),
                    "entity_type": entity_type,
                    "entity_id": entity_id,
                    "season_number": season_number,
                    "link_group": link_group,
                    "link_kind": link_kind,
                    "url": canonical_url,
                    "label": label,
                    "connected": index > 0,
                }
            )

    return {
        "show_id": show_id_str,
        "submitted_count": len(inputs),
        "added": upserted_count,
        "connected_links_added": connected_count,
        "errors": errors,
        "assignments": assignments,
    }


@router.get("/{show_id}/links")
def list_show_links(
    show_id: UUID,
    _: AdminUser,
    status: Literal["all", "pending", "approved", "rejected"] = Query(default="all"),
    entity_type: EntityType | Literal["all"] = Query(default="all"),
    view: Literal["all", "active"] = Query(default="all"),
) -> list[dict[str, Any]]:
    show_id_str = str(show_id)
    if not _show_exists(show_id_str):
        raise HTTPException(status_code=404, detail="Show not found")

    params: list[Any] = [show_id_str]
    clauses = ["show_id = %s"]
    if view == "active":
        clauses.append("lower(status) = 'approved'")
    elif status != "all":
        clauses.append("status = %s")
        params.append(status)
    if entity_type != "all":
        clauses.append("entity_type = %s")
        params.append(entity_type)

    if view == "active":
        _normalize_legacy_knowledge_link_kinds(show_id_str)

    rows = pg.fetch_all(
        f"""
        SELECT *
        FROM core.entity_links
        WHERE {" AND ".join(clauses)}
        ORDER BY link_group, season_number DESC, updated_at DESC NULLS LAST, created_at DESC
        """,
        params,
    )
    if view != "active":
        return rows

    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str, str, int, str, str]] = set()
    for row in rows:
        entity_type_value = str(row.get("entity_type") or "").strip().lower()
        entity_id_value = str(row.get("entity_id") or "").strip()
        season_number_value = int(row.get("season_number") or 0)
        link_kind_value = _normalize_link_kind(str(row.get("link_kind") or "").strip().lower())
        url_value = _canonicalize_url(str(row.get("url") or "").strip())
        url_key_value = _url_key(url_value)
        dedupe_key = (
            entity_type_value,
            entity_id_value,
            season_number_value,
            link_kind_value,
            url_key_value,
        )
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        if url_value:
            row["url"] = url_value
            row["url_key"] = url_key_value
        deduped.append(row)
    return deduped


@router.post("/{show_id}/links")
def create_show_link(
    show_id: UUID,
    payload: LinkCreateRequest,
    db: SupabaseAdminClient,
    admin: AdminUser,
) -> dict[str, Any]:
    show_id_str = str(show_id)
    if not _show_exists(show_id_str):
        raise HTTPException(status_code=404, detail="Show not found")

    actor = str(admin.get("email") or admin.get("id") or "admin")
    return _upsert_link(
        db,
        show_id=show_id_str,
        entity_type=payload.entity_type,
        entity_id=str(payload.entity_id),
        link_group=payload.link_group,
        link_kind=_normalize_link_kind(payload.link_kind),
        url=str(payload.url),
        label=payload.label,
        season_number=int(payload.season_number or 0),
        status=payload.status,
        confidence=payload.confidence,
        source=payload.source,
        discovered_by="manual",
        metadata=payload.metadata,
        actor=actor,
    )


@router.patch("/{show_id}/links/{link_id}")
def patch_show_link(
    show_id: UUID,
    link_id: UUID,
    payload: LinkPatchRequest,
    db: SupabaseAdminClient,
    admin: AdminUser,
) -> dict[str, Any]:
    show_id_str = str(show_id)
    actor = str(admin.get("email") or admin.get("id") or "admin")

    get_response = (
        db.schema("core")
        .table("entity_links")
        .select("*")
        .eq("id", str(link_id))
        .eq("show_id", show_id_str)
        .limit(1)
        .execute()
    )
    rows = get_list_result(get_response, "fetching entity link")
    if not rows:
        raise HTTPException(status_code=404, detail="Link not found")
    current = rows[0]
    current_link_kind = _normalize_link_kind(str(current.get("link_kind") or ""))
    current_entity_type = str(current.get("entity_type") or "").strip().lower()

    updates = payload.model_dump(exclude_unset=True)
    next_link_kind = current_link_kind
    if "link_kind" in updates and updates["link_kind"] is not None:
        next_link_kind = _normalize_link_kind(str(updates["link_kind"]))
    if "url" in updates and updates["url"] is not None:
        canonical_url = _canonicalize_url(str(updates["url"]))
        if next_link_kind == "wikipedia":
            resolved_wikipedia_url, _, wikipedia_error = _resolve_wikipedia_url(canonical_url)
            if wikipedia_error == "missing":
                raise HTTPException(status_code=400, detail=_WIKIPEDIA_MISSING_ARTICLE_DETAIL)
            if resolved_wikipedia_url:
                canonical_url = _canonicalize_url(resolved_wikipedia_url)
            if current_entity_type == "show":
                expected_show_wikidata_id = _load_show_wikidata_id(show_id_str)
                if expected_show_wikidata_id:
                    candidate_wikidata_id = _resolve_wikipedia_wikidata_id(canonical_url)
                    if candidate_wikidata_id and candidate_wikidata_id != expected_show_wikidata_id:
                        raise HTTPException(status_code=400, detail=_WIKIPEDIA_SHOW_VARIANT_MISMATCH_DETAIL)
        updates["url"] = canonical_url
        updates["url_key"] = _url_key(canonical_url)
    if "link_kind" in updates and updates["link_kind"] is not None:
        updates["link_kind"] = next_link_kind
    updates["updated_by"] = actor

    response = (
        db.schema("core")
        .table("entity_links")
        .update(updates)
        .eq("id", str(link_id))
        .eq("show_id", show_id_str)
        .execute()
    )
    updated_rows = get_list_result(response, "updating entity link")
    result_row = updated_rows[0] if updated_rows else {**current, **updates}

    wikipedia_url_for_cascade = str(
        (result_row or {}).get("url") or updates.get("url") or current.get("url") or ""
    ).strip()
    if current_entity_type == "show" and next_link_kind == "wikipedia" and wikipedia_url_for_cascade:
        _sync_show_wikipedia_links(
            show_id=show_id_str,
            show_wikipedia_url=wikipedia_url_for_cascade,
            actor=actor,
            exclude_link_id=str(link_id),
        )

    return result_row


@router.delete("/{show_id}/links/{link_id}")
def delete_show_link(
    show_id: UUID,
    link_id: UUID,
    db: SupabaseAdminClient,
    _: AdminUser,
) -> dict[str, Any]:
    response = (
        db.schema("core").table("entity_links").delete().eq("id", str(link_id)).eq("show_id", str(show_id)).execute()
    )
    rows = get_list_result(response, "deleting entity link")
    if not rows:
        raise HTTPException(status_code=404, detail="Link not found")
    return {"deleted": True, "id": str(link_id)}
