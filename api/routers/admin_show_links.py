from __future__ import annotations

import json
import os
import re
import urllib.error
import urllib.request
from functools import lru_cache
from typing import Any, Literal
from urllib.parse import quote, unquote, urlparse, urlunparse
from uuid import UUID

from bs4 import BeautifulSoup
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field, HttpUrl

from api.auth import AdminUser
from api.deps import SupabaseAdminClient, get_list_result
from trr_backend.db import pg
from trr_backend.ingestion.show_cast_matrix_scraper import (
    is_missing_fandom_page,
    is_missing_wikipedia_page,
)
from trr_backend.integrations.fandom import (
    is_allowlisted_fandom_domain,
    load_fandom_community_allowlist,
    load_fandom_community_allowlist_with_source,
    normalize_fandom_community_domain,
    refresh_fandom_community_allowlist_cache,
    search_allowlisted_fandom_wikis,
    search_real_housewives_wiki,
)
from trr_backend.integrations.franchise_rules import (
    classify_show_franchise,
    default_rules_by_key,
    get_candidate_urls_for_rule,
    is_fallback_link_metadata,
)

router = APIRouter(prefix="/admin/shows", tags=["admin-show-links"])
fandom_router = APIRouter(prefix="/admin/fandom", tags=["admin-fandom"])
_BRAVO_VARIANT = "default"

EntityType = Literal["show", "season", "person"]
LinkGroup = Literal["official", "social", "knowledge", "cast_announcements", "other"]
LinkStatus = Literal["pending", "approved", "rejected"]


class LinkDiscoverRequest(BaseModel):
    include_seasons: bool = True
    include_people: bool = True


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


_IMDB_PERSON_ID_RE = re.compile(r"nm\d+")
_TMDB_PERSON_ID_RE = re.compile(r"\d+")
_PERSON_SOURCE_LINK_KINDS = {"wikipedia", "wikidata", "fandom", "wikia", "imdb", "tmdb", "bravo_profile"}
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


@lru_cache(maxsize=256)
def _resolve_wikidata_enwiki_url(wikidata_id: str) -> str | None:
    item_id = str(wikidata_id or "").strip()
    if not re.fullmatch(r"Q\d+", item_id):
        return None
    summary, fetch_error = _fetch_wikidata_summary(item_id)
    if fetch_error or not summary:
        return None
    return summary.get("enwiki_url")


@lru_cache(maxsize=512)
def _fetch_wikidata_summary(wikidata_id: str) -> tuple[dict[str, str] | None, bool]:
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
    if not enwiki_title:
        return None, False

    labels = entity.get("labels") if isinstance(entity.get("labels"), dict) else {}
    en_label_payload = labels.get("en") if isinstance(labels.get("en"), dict) else {}
    en_label = str(en_label_payload.get("value") or "").strip()

    return (
        {
            "item_id": item_id,
            "label": en_label,
            "enwiki_title": enwiki_title,
            "enwiki_url": f"https://en.wikipedia.org/wiki/{quote(enwiki_title.replace(' ', '_'))}",
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
    return None


def _discover_show_links(show_id: str) -> list[dict[str, Any]]:
    show = pg.fetch_one(
        """
        SELECT id, name, networks, wikidata_id, external_ids
        FROM core.shows
        WHERE id = %s
        """,
        [show_id],
    )
    if not show:
        return []

    show_name = str(show.get("name") or "").strip()
    show_slug = _slug(show_name)
    networks = [str(n).strip().lower() for n in (show.get("networks") or []) if isinstance(n, str)]
    external_ids = show.get("external_ids") if isinstance(show.get("external_ids"), dict) else {}
    fandom_allowlist = load_fandom_community_allowlist()

    discovered: list[dict[str, Any]] = []

    if show_slug:
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
        discovered.append(
            {
                "entity_type": "show",
                "entity_id": show_id,
                "season_number": 0,
                "link_group": "knowledge",
                "link_kind": "wikipedia",
                "label": "Wikipedia",
                "url": f"https://en.wikipedia.org/wiki/{quote(show_name.replace(' ', '_'))}",
                "source": "derived",
            }
        )
        existing_show_fandom_links = pg.fetch_all(
            """
            SELECT url, metadata, source, status
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
        explicit_fandom_urls: list[tuple[str, str]] = []
        fallback_fandom_urls: list[tuple[str, str]] = []
        seen_fandom_urls: set[str] = set()
        for row in existing_show_fandom_links:
            raw_url = str(row.get("url") or "").strip()
            if not raw_url:
                continue
            parsed = urlparse(raw_url)
            if not parsed.scheme.startswith("http"):
                continue
            if not is_allowlisted_fandom_domain(raw_url, allowlist=fandom_allowlist):
                continue
            normalized = raw_url.rstrip("/")
            if normalized in seen_fandom_urls:
                continue
            seen_fandom_urls.add(normalized)
            if is_fallback_link_metadata(row.get("metadata"), str(row.get("source") or "")):
                fallback_fandom_urls.append((normalized, "franchise_rule"))
            else:
                explicit_fandom_urls.append((normalized, "core.entity_links"))

        fandom_urls: list[tuple[str, str, dict[str, Any] | None]] = []
        if explicit_fandom_urls:
            fandom_urls.extend((url, source, None) for url, source in explicit_fandom_urls)
        elif fallback_fandom_urls:
            fandom_urls.extend((url, source, None) for url, source in fallback_fandom_urls)
        else:
            franchise_rules = default_rules_by_key()
            franchise_key = classify_show_franchise(show_name, show.get("networks"), franchise_rules)
            derived_rule = franchise_rules.get(franchise_key) if franchise_key else None
            derived_candidates = get_candidate_urls_for_rule(derived_rule or {})
            if derived_candidates:
                for candidate in derived_candidates:
                    candidate_url = str(candidate.get("url") or "").strip()
                    if not candidate_url:
                        continue
                    fandom_urls.append(
                        (
                            candidate_url,
                            "franchise_rule_derived",
                            {
                                "rule_scope": "franchise_fallback",
                                "franchise_key": franchise_key,
                                "is_fallback": True,
                                "source_rank": int(candidate.get("source_rank") or 100),
                                "include_allpages_scan": bool(candidate.get("include_allpages_scan")),
                                "rule_version": int(derived_rule.get("rule_version") or 1) if derived_rule else 1,
                            },
                        )
                    )
            elif "real housewives" in show_name.lower():
                derived_fandom_url = f"https://real-housewives.fandom.com/wiki/{quote(show_name.replace(' ', '_'))}"
                fandom_urls.append((derived_fandom_url, "derived", None))

        for fandom_url, fandom_source, fandom_metadata in fandom_urls:
            discovered.append(
                {
                    "entity_type": "show",
                    "entity_id": show_id,
                    "season_number": 0,
                    "link_group": "knowledge",
                    "link_kind": "fandom",
                    "label": "Fandom",
                    "url": fandom_url,
                    "source": fandom_source,
                    "metadata": fandom_metadata or {},
                }
            )

    wikidata_id = str(show.get("wikidata_id") or "").strip()
    if wikidata_id:
        discovered.append(
            {
                "entity_type": "show",
                "entity_id": show_id,
                "season_number": 0,
                "link_group": "knowledge",
                "link_kind": "wikidata",
                "label": "Wikidata",
                "url": f"https://www.wikidata.org/wiki/{wikidata_id}",
                "source": "core.shows.wikidata_id",
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


def _discover_season_links(show_id: str) -> list[dict[str, Any]]:
    rows = pg.fetch_all(
        """
        SELECT id, season_number, external_wikidata_id, external_ids
        FROM core.seasons
        WHERE show_id = %s
        """,
        [show_id],
    )
    show_name_row = pg.fetch_one("SELECT name FROM core.shows WHERE id = %s", [show_id])
    show_name = str(show_name_row.get("name") or "").strip() if show_name_row else ""
    found: list[dict[str, Any]] = []
    for row in rows:
        season_id = str(row.get("id"))
        season_number = int(row.get("season_number") or 0)
        if season_number <= 0:
            continue
        wikidata = str(row.get("external_wikidata_id") or "").strip()
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
                    "source": "core.seasons.external_wikidata_id",
                }
            )
        season_wikipedia_url = _resolve_wikidata_enwiki_url(wikidata) if wikidata else None
        if not season_wikipedia_url and show_name:
            season_wikipedia_url = (
                "https://en.wikipedia.org/wiki/"
                f"{quote((show_name + ' season ' + str(season_number)).replace(' ', '_'))}"
            )
        if season_wikipedia_url:
            found.append(
                {
                    "entity_type": "season",
                    "entity_id": season_id,
                    "season_number": season_number,
                    "link_group": "knowledge",
                    "link_kind": "wikipedia",
                    "label": f"Season {season_number} Wikipedia",
                    "url": season_wikipedia_url,
                    "source": "wikidata_sitelink" if wikidata and season_wikipedia_url else "derived",
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
) -> dict[str, Any]:
    return {
        "entity_type": "person",
        "entity_id": person_id,
        "season_number": 0,
        "link_group": "knowledge" if link_kind in {"wikidata", "wikipedia", "fandom", "imdb", "tmdb"} else "official",
        "link_kind": link_kind,
        "label": label,
        "url": url,
        "source": source,
        "status": "approved",
        "confidence": 0.95,
    }


def _discover_fandom_candidates_for_person(
    *,
    name: str,
    seeded_fandom_url: str | None,
    is_bravo_show: bool,
    fandom_allowlist: list[str] | tuple[str, ...],
) -> list[str]:
    candidates: list[str] = []
    if seeded_fandom_url:
        candidates.append(seeded_fandom_url)
    if not is_bravo_show or not name:
        return candidates

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
    if candidate == expected:
        return 300
    expected_tokens = {token for token in expected.split() if token}
    candidate_tokens = {token for token in candidate.split() if token}
    if expected_tokens and candidate_tokens and expected_tokens.issubset(candidate_tokens):
        return 200
    if expected_tokens and candidate_tokens and expected_tokens.intersection(candidate_tokens):
        return 100
    return 0


def _discover_people_links(show_id: str) -> list[dict[str, Any]]:
    show = pg.fetch_one("SELECT networks FROM core.shows WHERE id = %s", [show_id]) or {}
    networks = [str(value).strip().lower() for value in (show.get("networks") or []) if isinstance(value, str)]
    is_bravo_show = "bravo" in networks
    fandom_allowlist = load_fandom_community_allowlist()

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
          ct.wikidata_id AS cast_tmdb_wikidata_id
        FROM core.v_show_cast sc
        JOIN core.people p ON p.id = sc.person_id
        LEFT JOIN core.cast_fandom cf ON cf.person_id = p.id AND cf.source = 'fandom'
        LEFT JOIN core.cast_tmdb ct ON ct.person_id = p.id
        WHERE sc.show_id = %s
        """,
        [show_id],
    )
    found: list[dict[str, Any]] = []
    for row in rows:
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

        if imdb_id and name:
            imdb_url = _validated_or_carried_person_source_url(
                person_id=person_id,
                candidate_url=f"https://www.imdb.com/name/{imdb_id}/",
                kind="imdb",
                expected_name=name,
                fandom_allowlist=fandom_allowlist,
            )
            if imdb_url and imdb_source:
                found.append(
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
                found.append(
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
                found.append(
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
                found.append(
                    _build_person_link_row(
                        person_id=person_id,
                        link_kind="wikipedia",
                        label=f"{name} Wikipedia",
                        url=wikipedia_url,
                        source="derived_validated",
                    )
                )

        fandom_candidates = _discover_fandom_candidates_for_person(
            name=name,
            seeded_fandom_url=fandom_url if fandom_url else None,
            is_bravo_show=is_bravo_show,
            fandom_allowlist=fandom_allowlist,
        )
        ranked_fandom_candidates = sorted(
            fandom_candidates,
            key=lambda candidate_url: _score_fandom_candidate_url(candidate_url, expected_name=name),
            reverse=True,
        )
        for fandom_candidate in ranked_fandom_candidates:
            if not is_allowlisted_fandom_domain(fandom_candidate, allowlist=fandom_allowlist):
                continue
            validated_fandom_url = _validated_person_knowledge_url(
                fandom_candidate,
                kind="fandom",
                expected_name=name if name else None,
                fandom_allowlist=fandom_allowlist,
            )
            if validated_fandom_url:
                found.append(
                    _build_person_link_row(
                        person_id=person_id,
                        link_kind="fandom",
                        label=f"{name} Fandom" if name else "Fandom",
                        url=validated_fandom_url,
                        source="core.cast_fandom" if fandom_candidate == fandom_url else "fandom_search",
                    )
                )
                break
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
                    found.append(
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


def _cleanup_invalid_person_knowledge_links(show_id: str) -> dict[str, int]:
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
        "validation_failures": int(scan.get("validation_failures") or 0),
    }


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
    discovered = _discover_show_links(show_id_str)
    if payload.include_seasons:
        discovered.extend(_discover_season_links(show_id_str))
    if payload.include_people:
        discovered.extend(_discover_people_links(show_id_str))

    upserted = 0
    by_group: dict[str, int] = {}
    for row in discovered:
        entity_type = str(row.get("entity_type") or "").strip().lower()
        link_kind = _normalize_link_kind(str(row.get("link_kind") or "").strip().lower())
        parsed = urlparse(str(row["url"]))
        if not parsed.scheme.startswith("http"):
            continue
        row_status = str(row.get("status") or "pending").strip().lower()
        status = row_status if row_status in {"pending", "approved", "rejected"} else "pending"
        if entity_type == "person" and link_kind in _PERSON_SOURCE_LINK_KINDS and status != "approved":
            continue
        confidence_raw = row.get("confidence")
        if isinstance(confidence_raw, (int, float)):
            confidence = float(confidence_raw)
        else:
            if entity_type == "person" and link_kind in _PERSON_SOURCE_LINK_KINDS:
                confidence = 0.95
            else:
                confidence = 0.9 if status == "approved" else 0.65
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
        by_group[row["link_group"]] = by_group.get(row["link_group"], 0) + 1

    invalid_people_cleanup = {"deleted": 0, "promoted": 0, "validation_failures": 0}
    if payload.include_people:
        invalid_people_cleanup = _cleanup_invalid_person_knowledge_links(show_id_str)

    return {
        "show_id": show_id_str,
        "discovered": upserted,
        "counts_by_group": by_group,
        "invalid_people_links_deleted": int(invalid_people_cleanup.get("deleted") or 0),
        "pending_person_source_links_promoted": int(invalid_people_cleanup.get("promoted") or 0),
        "invalid_people_links_validation_failures": int(invalid_people_cleanup.get("validation_failures") or 0),
    }


@router.get("/{show_id}/links")
def list_show_links(
    show_id: UUID,
    _: AdminUser,
    status: Literal["all", "pending", "approved", "rejected"] = Query(default="all"),
    entity_type: EntityType | Literal["all"] = Query(default="all"),
) -> list[dict[str, Any]]:
    show_id_str = str(show_id)
    if not _show_exists(show_id_str):
        raise HTTPException(status_code=404, detail="Show not found")

    params: list[Any] = [show_id_str]
    clauses = ["show_id = %s"]
    if status != "all":
        clauses.append("status = %s")
        params.append(status)
    if entity_type != "all":
        clauses.append("entity_type = %s")
        params.append(entity_type)

    return pg.fetch_all(
        f"""
        SELECT *
        FROM core.entity_links
        WHERE {" AND ".join(clauses)}
        ORDER BY link_group, season_number DESC, created_at DESC
        """,
        params,
    )


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

    updates = payload.model_dump(exclude_unset=True)
    if "url" in updates and updates["url"] is not None:
        canonical_url = _canonicalize_url(str(updates["url"]))
        updates["url"] = canonical_url
        updates["url_key"] = _url_key(canonical_url)
    if "link_kind" in updates and updates["link_kind"] is not None:
        updates["link_kind"] = _normalize_link_kind(str(updates["link_kind"]))
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
    return updated_rows[0] if updated_rows else {**current, **updates}


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
