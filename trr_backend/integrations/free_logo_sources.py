from __future__ import annotations

import base64
import re
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qs, quote, quote_plus, unquote, urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup

from trr_backend.integrations.logopedia import LogopediaError, fetch_logopedia_logo_candidates

WIKIMEDIA_MEDIASEARCH_URL = (
    "https://commons.wikimedia.org/w/index.php?search={query}&title=Special%3AMediaSearch&type=image&filemime=svg"
)
LOGOS_1000_SEARCH_URL = "https://1000logos.net/?s={query}"
LOGOS_1000_ARTICLE_SLUG_URL = "https://1000logos.net/{slug}-logo/"
LOGOS_1000_WP_SEARCH_URL = "https://1000logos.net/wp-json/wp/v2/posts?search={query}&per_page=6&_fields=link,slug,title"
LOGOS_FANDOM_SEARCH_URL = "https://logos.fandom.com/wiki/Special:Search?query={query}"
LOGOS_FANDOM_IMAGE_ONLY_SEARCH_URL = (
    "https://logos.fandom.com/wiki/Special:Search?scope=internal&query={query}&ns%5B0%5D=6&filter=imageOnly"
)
LOGOS_FANDOM_PAGE_URL = "https://logos.fandom.com/wiki/{slug}"
WORLDVECTORLOGO_SEARCH_URL = "https://worldvectorlogo.com/search?q={query}"
SEEKLOGO_SEARCH_URL = "https://seeklogo.com/search?q={query}"
LOGOWIK_SEARCH_URL = "https://logowik.com/search?q={query}"
LOGOWINE_SEARCH_URL = "https://www.logo.wine/?s={query}"
LOGOSEARCH_SEARCH_URL = "https://logosear.ch/search.html?q={query}"
SIMPLE_ICONS_CDN_URL = "https://cdn.simpleicons.org/{slug}"
SIMPLE_ICONS_SEARCH_URL = "https://simpleicons.org/?q={query}"
FANDOM_BRAND_LOGO_PAGE_URL = "https://www.fandom.com/brand/graphic-assets/logo.html"
FANDOM_STANDARD_WORDMARK_URL = "https://www.fandom.com/brand/images/Fandom_logo_2021_logotype_1.png"
FANDOM_STANDARD_ICON_URL = "https://www.fandom.com/brand/images/Logo_transparent_1.png"
DEFAULT_CONNECT_TIMEOUT_SECONDS = 5.0
DEFAULT_READ_TIMEOUT_SECONDS = 15.0
DEFAULT_RETRY_ATTEMPTS = 2
DEFAULT_RETRY_BACKOFF_MS = 250
DEFAULT_LOGOS_FANDOM_SUGGESTION_LIMIT = 30

FREE_LOGO_SOURCE_PROVIDERS: tuple[str, ...] = (
    "wikimedia_commons",
    "logos_fandom",
    "logos1000",
    "official_site",
    "brand_guidelines",
    "favicon_appicons",
    "worldvectorlogo",
    "seeklogo",
    "logowik",
    "logo_wine",
    "logosearch",
    "simple_icons",
)
_SEARCH_TERM_SOURCE_PROVIDERS = {
    "wikimedia_commons",
    "logos_fandom",
    "worldvectorlogo",
    "seeklogo",
    "logowik",
    "logo_wine",
    "logosearch",
    "simple_icons",
}
_HOST_OR_URL_SOURCE_PROVIDERS = {"official_site", "brand_guidelines", "favicon_appicons"}
_SLUG_SOURCE_PROVIDERS = {"logos1000"}

_GUIDELINE_PATH_HINTS = (
    "brand-guidelines",
    "brand-guideline",
    "brand-assets",
    "media-kit",
    "press-kit",
    "press",
)

_IMAGE_META_KEYS = (
    "og:image",
    "twitter:image",
    "twitter:image:src",
)

_GENERIC_QUERY_TOKENS = {
    "www",
    "com",
    "org",
    "net",
    "tv",
    "co",
    "io",
    "app",
    "the",
    "official",
    "brand",
    "brands",
    "logo",
    "logos",
}
_LOGOS_FANDOM_DISALLOWED_PAGE_PREFIXES = (
    "special:",
    "file:",
    "category:",
    "template:",
    "user:",
    "help:",
    "talk:",
    "mediawiki:",
    "forum:",
    "message_wall:",
    "thread:",
    "blog:",
    "module:",
)


@dataclass(frozen=True)
class FreeLogoCandidate:
    url: str
    source_provider: str
    discovered_from: str
    context: str | None = None


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _safe_get(
    session: requests.Session,
    url: str,
    *,
    timeout_seconds: float,
    attempts: int = DEFAULT_RETRY_ATTEMPTS,
) -> requests.Response | None:
    read_timeout = timeout_seconds if timeout_seconds > 0 else DEFAULT_READ_TIMEOUT_SECONDS
    timeout = (min(DEFAULT_CONNECT_TIMEOUT_SECONDS, read_timeout), read_timeout)
    for attempt in range(max(1, attempts)):
        try:
            response = session.get(
                url,
                headers={"accept": "text/html,application/json", "user-agent": "TRR-Backend/1.0"},
                timeout=timeout,
            )
        except requests.RequestException:
            response = None
        if response is not None and response.status_code < 500 and response.status_code != 429:
            return response
        if attempt + 1 < attempts and DEFAULT_RETRY_BACKOFF_MS > 0:
            time.sleep(DEFAULT_RETRY_BACKOFF_MS / 1000)
    return response


def _normalize_candidate_url(url: str, *, base_url: str) -> str:
    text = _normalize_text(url)
    if not text:
        return ""
    if text.startswith("data:image/svg+xml;base64,"):
        return text
    if text.startswith("//"):
        parsed = urlparse(base_url)
        return f"{parsed.scheme or 'https'}:{text}"
    if text.startswith("http://") or text.startswith("https://"):
        return text
    return urljoin(base_url, text)


def _looks_like_logo_candidate(url: str, *, context: str = "") -> bool:
    lowered_url = _normalize_text(url).lower()
    _ = context
    if lowered_url.startswith("data:image/svg+xml;base64,"):
        return True
    if "data:" in lowered_url:
        return False
    if any(ext in lowered_url for ext in (".svg", ".png", ".webp", ".jpg", ".jpeg", ".avif", ".gif", ".ico")):
        return True
    if "special:filepath/" in lowered_url:
        return True
    parsed = urlparse(lowered_url)
    if _normalize_text(parsed.netloc).casefold() == "cdn.simpleicons.org":
        return True
    return False


def _dedupe_candidates(candidates: list[FreeLogoCandidate]) -> list[FreeLogoCandidate]:
    seen: set[tuple[str, str]] = set()
    out: list[FreeLogoCandidate] = []
    for candidate in candidates:
        key = (_normalize_text(candidate.url), _normalize_text(candidate.source_provider))
        if not key[0] or key in seen:
            continue
        seen.add(key)
        out.append(candidate)
    return out


def _score_candidate(candidate: FreeLogoCandidate) -> tuple[int, int]:
    provider = _normalize_text(candidate.source_provider).lower()
    url = _normalize_text(candidate.url).lower()
    context = _normalize_text(candidate.context).lower()

    provider_score = {
        "brand_guidelines": 0,
        "official_site": 10,
        "favicon_appicons": 12,
        "wikimedia_commons": 20,
        "logos_fandom": 25,
        "logos1000": 30,
        "worldvectorlogo": 34,
        "seeklogo": 35,
        "logowik": 36,
        "logo_wine": 37,
        "logosearch": 38,
        "simple_icons": 42,
    }.get(provider, 40)

    format_score = 20
    if ".svg" in url:
        format_score = 0
    elif ".png" in url:
        format_score = 5
    elif ".webp" in url:
        format_score = 10
    elif any(ext in url for ext in (".jpg", ".jpeg", ".avif")):
        format_score = 15

    cue_bonus = 0
    if "wordmark" in url or "wordmark" in context:
        cue_bonus -= 3
    if "logo" in url or "logo" in context:
        cue_bonus -= 2
    if "favicon" in url or "apple-touch-icon" in url:
        cue_bonus += 5

    return provider_score, format_score + cue_bonus


def _slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", _normalize_text(value).casefold()).strip("-")


def _is_favicon_like(url: str, context: str = "") -> bool:
    lowered = f"{url} {context}".lower()
    return any(
        token in lowered
        for token in (
            "favicon",
            "apple-touch-icon",
            "mask-icon",
            "mstile",
            "android-chrome",
            "site.webmanifest",
        )
    )


def _candidate_terms(target_label: str, target_key: str, aliases: list[str] | None = None) -> list[str]:
    values = [target_label, target_key, *(aliases or [])]
    out: list[str] = []
    seen: set[str] = set()
    for raw in values:
        text = _normalize_text(raw)
        if not text:
            continue
        key = text.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(text)
    return out


def _derive_brand_query_term(target_label: str, target_key: str, aliases: list[str] | None = None) -> str:
    for raw in [target_label, *(aliases or []), target_key]:
        text = _normalize_text(raw)
        if not text:
            continue
        host = _normalize_hostname_from_url(text)
        if host:
            parts = host.split(".")
            if len(parts) >= 2 and parts[0] in {"www", "m", "en", "mobile", "amp"}:
                text = parts[1]
            else:
                text = parts[0]
        text = re.sub(r"[_-]+", " ", text)
        text = re.sub(r"[^a-zA-Z0-9 ]+", " ", text)
        terms = [token for token in text.split() if token and token.casefold() not in _GENERIC_QUERY_TOKENS]
        if terms:
            return " ".join(terms[:3])
    fallback = _normalize_text(target_label) or _normalize_text(target_key)
    return fallback or "brand"


def _normalize_hostname_from_url(value: str) -> str:
    text = _normalize_text(value)
    if not text:
        return ""
    parsed = urlparse(text if "://" in text else f"https://{text}")
    host = _normalize_text(parsed.netloc or parsed.path).lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def _1000logos_article_url(slug: str) -> str:
    normalized_slug = _normalize_source_query_value("logos1000", slug)
    if not normalized_slug:
        return ""
    final_slug = normalized_slug if normalized_slug.endswith("-logo") else f"{normalized_slug}-logo"
    return f"https://1000logos.net/{final_slug}/"


def _humanize_query_fragment(value: str) -> str:
    text = unquote(_normalize_text(value))
    if not text:
        return ""
    text = re.sub(r"[_/]+", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip(" -")


def _extract_search_term_from_url(source_provider: str, value: str) -> str:
    parsed = urlparse(value if "://" in value else f"https://placeholder.local/{value.lstrip('/')}")
    if not (_normalize_text(parsed.netloc) or _normalize_text(parsed.path)):
        return ""

    query_params = parse_qs(parsed.query)
    for key in ("query", "q", "search", "s"):
        values = query_params.get(key) or []
        if not values:
            continue
        normalized = _humanize_query_fragment(values[0])
        if normalized:
            return normalized

    path = _normalize_text(parsed.path).strip("/")
    if not path:
        return ""
    if _normalize_text(source_provider).casefold() == "logos_fandom" and path.casefold().startswith("wiki/"):
        path = path[5:]
    if path.casefold().startswith("special:search"):
        return ""
    return _humanize_query_fragment(path)


def _normalize_logos_fandom_page_slug(value: str) -> str:
    text = _normalize_text(unquote(value))
    if not text:
        return ""

    is_absolute_url = "://" in text
    parsed = urlparse(text if is_absolute_url else f"https://placeholder.local/{text.lstrip('/')}")
    host = _normalize_text(parsed.netloc).casefold()
    path = _normalize_text(unquote(parsed.path)).strip("/") if parsed.path else ""

    if is_absolute_url and host and "logos.fandom.com" not in host:
        return ""
    if path.casefold().startswith("wiki/"):
        path = path[5:]

    raw_candidate = path or text.strip("/")
    if not raw_candidate or raw_candidate.casefold().startswith("special:search"):
        return ""
    if not any(token in raw_candidate for token in ("/", "_", "(", ")")):
        if not (is_absolute_url and host and "logos.fandom.com" in host):
            return ""
    if " " in raw_candidate:
        return ""
    segments = [
        re.sub(r"\s+", "_", _normalize_text(segment))
        for segment in raw_candidate.split("/")
        if _normalize_text(segment)
    ]
    return "/".join(segments)


def _coerce_logos_fandom_page_slug(value: str, *, allow_simple: bool = False) -> str:
    page_slug = _normalize_logos_fandom_page_slug(value)
    if page_slug or not allow_simple:
        return page_slug

    text = _normalize_text(unquote(value)).strip("/")
    if not text or " " in text or text.casefold().startswith("special:search"):
        return ""
    if "://" in text:
        return ""
    return re.sub(r"\s+", "_", text)


def _normalize_explicit_logos_fandom_url(value: str) -> str:
    text = _normalize_text(value)
    if "://" not in text:
        return ""
    parsed = urlparse(text)
    host = _normalize_text(parsed.netloc).casefold()
    path = _normalize_text(unquote(parsed.path))
    if "logos.fandom.com" not in host or not path:
        return ""
    normalized_path = "/" + path.lstrip("/")
    normalized_query = _normalize_text(parsed.query)
    return urlunparse(("https", "logos.fandom.com", normalized_path, "", normalized_query, ""))


def _logos_fandom_query_search_term(value: str) -> str:
    page_slug = _normalize_logos_fandom_page_slug(value)
    if page_slug:
        return _humanize_query_fragment(page_slug)
    return re.sub(r"\s+", " ", _extract_search_term_from_url("logos_fandom", value) or _normalize_text(value)).strip()


def _logos_fandom_query_links(value: str) -> list[str]:
    explicit_url = _normalize_explicit_logos_fandom_url(value)
    if explicit_url:
        return [explicit_url]
    page_slug = _normalize_logos_fandom_page_slug(value)
    if page_slug:
        return [LOGOS_FANDOM_PAGE_URL.format(slug=page_slug)]
    search_term = _logos_fandom_query_search_term(value)
    if not search_term:
        return []
    return [
        LOGOS_FANDOM_SEARCH_URL.format(query=quote_plus(search_term)),
        LOGOS_FANDOM_IMAGE_ONLY_SEARCH_URL.format(query=quote_plus(search_term)),
    ]


def _default_logos_fandom_query_values(target_label: str, target_key: str) -> list[str]:
    target_host = _normalize_hostname_from_url(target_key or target_label)
    normalized_label = _normalize_text(target_label).casefold()
    if target_host == "imdb.com" or normalized_label in {"imdb", "imdb.com"}:
        return ["IMDb", "IMDb/Special_Logos"]
    if target_host == "bravotv.com" or normalized_label in {
        "bravo",
        "bravo tv",
        "bravo (united states)",
        "bravotv.com",
    }:
        return ["Bravo_(United_States)", "Bravo_(United_States)/Special_Logos"]
    return []


def _is_valid_logos_fandom_suggestion_slug(value: str) -> bool:
    slug = _normalize_logos_fandom_page_slug(value)
    if not slug:
        return False
    if any(segment.casefold().startswith(_LOGOS_FANDOM_DISALLOWED_PAGE_PREFIXES) for segment in slug.split("/")):
        return False
    if slug.casefold().endswith((".svg", ".png", ".jpg", ".jpeg", ".gif", ".webp", ".avif")):
        return False
    return True


def suggest_logos_fandom_query_values(
    *,
    target_label: str,
    target_key: str,
    current_query_values: list[str] | tuple[str, ...] | None = None,
    timeout_seconds: float = DEFAULT_READ_TIMEOUT_SECONDS,
    limit: int = DEFAULT_LOGOS_FANDOM_SUGGESTION_LIMIT,
    session: requests.Session | None = None,
) -> list[dict[str, str]]:
    active_session = session or requests.Session()
    normalized_current_values = _normalize_source_query_values("logos_fandom", list(current_query_values or []))
    if not normalized_current_values:
        normalized_current_values = _default_logos_fandom_query_values(target_label, target_key)
    if not normalized_current_values:
        return []

    current_set = {value.casefold() for value in normalized_current_values}
    target_tokens = _target_host_tokens(target_key, [_derive_brand_query_term(target_label, target_key)])
    seen: set[str] = set()
    suggestions: list[dict[str, str]] = []

    def add_suggestion(query_value: str, *, reason: str, discovered_from: str) -> None:
        normalized_value = _normalize_source_query_value("logos_fandom", query_value)
        if not _is_valid_logos_fandom_suggestion_slug(normalized_value):
            return
        normalized_key = normalized_value.casefold()
        if normalized_key in current_set or normalized_key in seen:
            return
        seen.add(normalized_key)
        suggestions.append(
            {
                "query_value": normalized_value,
                "query_link": LOGOS_FANDOM_PAGE_URL.format(slug=normalized_value),
                "reason": reason,
                "discovered_from": discovered_from,
            }
        )

    for current_value in normalized_current_values:
        page_slug = _coerce_logos_fandom_page_slug(current_value, allow_simple=True)
        if not page_slug:
            continue
        page_url = LOGOS_FANDOM_PAGE_URL.format(slug=page_slug)
        if not page_slug.casefold().endswith("/special_logos"):
            add_suggestion(f"{page_slug}/Special_Logos", reason="special_logos_page", discovered_from=page_url)

        response = _safe_get(active_session, page_url, timeout_seconds=timeout_seconds)
        if response is None or response.status_code >= 400:
            continue
        soup = BeautifulSoup(response.text, "html.parser")
        container = soup.select_one(".mw-parser-output") or soup.body or soup
        linked_slugs: list[str] = []
        for anchor in container.find_all("a", href=True):
            slug = _normalize_logos_fandom_page_slug(anchor.get("href") or "")
            if not _is_valid_logos_fandom_suggestion_slug(slug):
                continue
            if slug.casefold() in current_set:
                continue
            linked_slugs.append(slug)

        def _score(slug: str) -> tuple[int, int, str]:
            slug_lower = slug.casefold()
            humanized = _humanize_query_fragment(slug)
            candidate_tokens = set(re.split(r"[^a-z0-9]+", humanized.casefold()))
            overlap = len({token for token in candidate_tokens if len(token) >= 2} & target_tokens)
            special_bonus = 0 if "/special_logos" in slug_lower else 1
            return (special_bonus, -overlap, slug_lower)

        for slug in sorted(set(linked_slugs), key=_score):
            add_suggestion(slug, reason=f"linked_from:{page_slug}", discovered_from=page_url)
            if len(suggestions) >= max(1, limit):
                return suggestions[:limit]

    return suggestions[:limit]


def _is_png_or_svg_logo_url(url: str) -> bool:
    lowered_url = _normalize_text(url).casefold()
    if not lowered_url:
        return False
    return lowered_url.startswith("data:image/svg+xml") or ".svg" in lowered_url or ".png" in lowered_url


def _normalize_source_query_value(source_provider: str, value: str) -> str:
    provider = _normalize_text(source_provider).casefold()
    text = _normalize_text(value)
    if not text:
        return ""

    if provider == "logos_fandom":
        page_slug = _normalize_logos_fandom_page_slug(text)
        if page_slug:
            return page_slug
        extracted = _logos_fandom_query_search_term(text)
        return re.sub(r"\s+", " ", extracted or text).strip()

    if provider in _SEARCH_TERM_SOURCE_PROVIDERS:
        extracted = _extract_search_term_from_url(provider, text)
        return re.sub(r"\s+", " ", extracted or text).strip()

    if provider in _SLUG_SOURCE_PROVIDERS:
        parsed = urlparse(text if "://" in text else f"https://placeholder.local/{text.lstrip('/')}")
        slug = _normalize_text(parsed.path).strip("/") or _normalize_text(text).strip("/")
        if slug.endswith(".html"):
            slug = slug[:-5]
        return _slugify(slug)

    if provider in _HOST_OR_URL_SOURCE_PROVIDERS:
        parsed = urlparse(text if "://" in text else f"https://{text}")
        host = _normalize_text(parsed.netloc or parsed.path).lower()
        path = _normalize_text(parsed.path if parsed.netloc else "")
        if host.startswith("www."):
            host = host[4:]
        if not host:
            return ""
        normalized = f"https://{host}"
        if path and path != "/":
            normalized = f"{normalized}{path.rstrip('/')}"
        return normalized

    return text


def _normalize_source_query_values(source_provider: str, values: list[str] | tuple[str, ...] | str | None) -> list[str]:
    if values is None:
        return []
    raw_values = values if isinstance(values, (list, tuple)) else [values]
    normalized_values: list[str] = []
    seen: set[str] = set()
    for raw_value in raw_values:
        normalized = _normalize_source_query_value(source_provider, _normalize_text(raw_value))
        if not normalized:
            continue
        normalized_key = normalized.casefold()
        if normalized_key in seen:
            continue
        seen.add(normalized_key)
        normalized_values.append(normalized)
    return normalized_values


def _preserve_query_override_values(values: list[str] | tuple[str, ...] | str | None) -> list[str]:
    if values is None:
        return []
    raw_values = values if isinstance(values, (list, tuple)) else [values]
    preserved_values: list[str] = []
    seen: set[str] = set()
    for raw_value in raw_values:
        normalized = _normalize_text(raw_value)
        if not normalized:
            continue
        normalized_key = normalized.casefold()
        if normalized_key in seen:
            continue
        seen.add(normalized_key)
        preserved_values.append(normalized)
    return preserved_values


def _dedupe_query_links(query_links: list[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for query_link in query_links:
        normalized = _normalize_text(query_link)
        if not normalized:
            continue
        normalized_key = normalized.casefold()
        if normalized_key in seen:
            continue
        seen.add(normalized_key)
        deduped.append(normalized)
    return deduped


def get_source_query_kind(source_provider: str) -> str:
    provider = _normalize_text(source_provider).casefold()
    if provider == "related_network_streaming":
        return "readonly"
    if provider in _SLUG_SOURCE_PROVIDERS:
        return "slug"
    if provider in _HOST_OR_URL_SOURCE_PROVIDERS:
        return "host_or_url"
    return "search_term"


def build_source_query_profile(
    *,
    source_provider: str,
    target_label: str,
    target_key: str,
    query_override: str | list[str] | tuple[str, ...] | None = None,
) -> dict[str, Any]:
    provider = _normalize_text(source_provider).casefold()
    query_kind = get_source_query_kind(provider)
    brand_query_term = _derive_brand_query_term(target_label, target_key)
    target_host = _normalize_hostname_from_url(target_key or target_label)

    if provider == "related_network_streaming":
        host_text = target_host or _normalize_text(target_key) or _normalize_text(target_label)
        return {
            "source_provider": provider,
            "editable": False,
            "refreshable": False,
            "query_kind": query_kind,
            "default_query_value": host_text,
            "effective_query_value": host_text,
            "query_values": [host_text] if host_text else [],
            "query_links": [f"host match: {host_text}" if host_text else "host match"],
        }

    if provider in _SLUG_SOURCE_PROVIDERS:
        default_query_value = _slugify(f"{brand_query_term}-logo") or _slugify(brand_query_term)
        query_values = _normalize_source_query_values(provider, query_override) or [default_query_value]
        effective_query_value = query_values[0]
        return {
            "source_provider": provider,
            "editable": True,
            "refreshable": True,
            "query_kind": query_kind,
            "default_query_value": default_query_value,
            "effective_query_value": effective_query_value,
            "query_values": query_values,
            "query_links": [_1000logos_article_url(query_value) for query_value in query_values],
        }

    if provider in _HOST_OR_URL_SOURCE_PROVIDERS:
        default_query_value = f"https://{target_host}" if target_host else ""
        query_values = _normalize_source_query_values(provider, query_override) or (
            [default_query_value] if default_query_value else []
        )
        effective_query_value = query_values[0] if query_values else default_query_value
        return {
            "source_provider": provider,
            "editable": True,
            "refreshable": True,
            "query_kind": query_kind,
            "default_query_value": default_query_value,
            "effective_query_value": effective_query_value,
            "query_values": query_values,
            "query_links": query_values if query_values else ["https://"],
        }

    default_query_value = brand_query_term
    default_query_values = _default_logos_fandom_query_values(target_label, target_key) if provider == "logos_fandom" else []
    if provider == "logos_fandom" and default_query_values:
        default_query_value = default_query_values[0]
    raw_query_values = _preserve_query_override_values(query_override) if provider == "logos_fandom" else []
    query_values = (
        raw_query_values
        if provider == "logos_fandom" and raw_query_values
        else _normalize_source_query_values(provider, query_override) or [default_query_value]
    )
    if provider == "logos_fandom" and default_query_values and query_override is None:
        query_values = default_query_values
    effective_query_value = query_values[0]
    query_links: list[str] = []
    for query_value in query_values:
        if provider == "wikimedia_commons":
            query_links.extend(
                [
                    WIKIMEDIA_MEDIASEARCH_URL.format(query=quote_plus(f"{query_value} logo")),
                    WIKIMEDIA_MEDIASEARCH_URL.format(query=quote_plus(f"{query_value} icon")),
                ]
            )
        elif provider == "logos_fandom":
            if default_query_values and query_override is None and query_value in default_query_values:
                query_links.append(LOGOS_FANDOM_PAGE_URL.format(slug=query_value))
            else:
                query_links.extend(_logos_fandom_query_links(query_value))
        elif provider == "worldvectorlogo":
            query_links.append(WORLDVECTORLOGO_SEARCH_URL.format(query=quote_plus(query_value)))
        elif provider == "seeklogo":
            query_links.append(SEEKLOGO_SEARCH_URL.format(query=quote_plus(query_value)))
        elif provider == "logowik":
            query_links.append(LOGOWIK_SEARCH_URL.format(query=quote_plus(query_value)))
        elif provider == "logo_wine":
            query_links.append(LOGOWINE_SEARCH_URL.format(query=quote_plus(query_value)))
        elif provider == "logosearch":
            query_links.append(LOGOSEARCH_SEARCH_URL.format(query=quote_plus(query_value)))
        elif provider == "simple_icons":
            query_links.append(SIMPLE_ICONS_SEARCH_URL.format(query=quote_plus(query_value)))
        else:
            query_links.append(query_value)

    return {
        "source_provider": provider,
        "editable": provider in FREE_LOGO_SOURCE_PROVIDERS,
        "refreshable": provider in FREE_LOGO_SOURCE_PROVIDERS,
        "query_kind": query_kind,
        "default_query_value": default_query_value,
        "effective_query_value": effective_query_value,
        "query_values": query_values,
        "query_links": _dedupe_query_links(query_links),
    }


def _target_host_tokens(target_key: str, terms: list[str]) -> set[str]:
    tokens: set[str] = set()
    target_host = _normalize_hostname_from_url(target_key)
    if target_host:
        first = target_host.split(".", 1)[0]
        if len(first) >= 2:
            tokens.add(first.casefold())
    for term in terms:
        for token in re.split(r"[^a-z0-9]+", term.casefold()):
            if len(token) < 2:
                continue
            if token in _GENERIC_QUERY_TOKENS:
                continue
            tokens.add(token)
    return tokens


def _host_matches_target(candidate_host: str, target_host: str) -> bool:
    host = _normalize_text(candidate_host).casefold()
    target = _normalize_text(target_host).casefold()
    if not host or not target:
        return False
    return host == target or host.endswith(f".{target}") or target.endswith(f".{host}")


def _is_known_noise_asset(candidate: FreeLogoCandidate) -> bool:
    provider = _normalize_text(candidate.source_provider).casefold()
    lowered = f"{candidate.url} {candidate.context or ''} {candidate.discovered_from}".lower()

    if provider == "logos1000":
        if any(
            token in lowered
            for token in (
                "/assets/images/social/",
                "h_menu",
                "h_search",
                "h_logo",
                "site-logo",
                "iconmonstr",
                "1000logos-white.svg",
                "1000logos_logo.svg",
                "pattern.webp",
                "ratingraph.webp",
                "film.webp",
                "-poster-",
            )
        ):
            return True
    if provider == "logos_fandom":
        if any(
            token in lowered
            for token in (
                "logopedia_info",
                "fandomdesktop",
                "fandommobile",
                "wikia/services",
                "anyclip",
                "theater-aux-poster",
                "site-logo",
                "wiki-wordmark",
                "wds-company-logo",
            )
        ):
            return True
    if provider == "worldvectorlogo":
        if any(token in lowered for token in ("/logo/worldvectorlogo", "/api/v1", "site-logo")):
            return True
    if provider == "seeklogo":
        if any(token in lowered for token in ("shutterstock", "googleads", "doubleclick", "seeklogo.com/assets/img")):
            return True
    if provider == "logosearch":
        if any(token in lowered for token in ("favicon.svg", "/images/navbar/", "social-footer")):
            return True
    if provider == "logowik":
        if any(token in lowered for token in ("preloader_thumb", "logowik-logo.svg")):
            return True
    if provider == "logo_wine":
        if any(token in lowered for token in ("logo.wine/logo.svg", "popular svelte", "/contact-us", "/terms")):
            return True
    return False


def _candidate_match_score(candidate: FreeLogoCandidate, *, tokens: set[str]) -> int:
    if not tokens:
        return 0
    lowered = f"{candidate.url} {candidate.context or ''} {candidate.discovered_from}".lower()
    return sum(1 for token in tokens if token in lowered)


def _has_url_or_context_token_match(candidate: FreeLogoCandidate, *, tokens: set[str]) -> bool:
    if not tokens:
        return True
    lowered = f"{candidate.url} {candidate.context or ''}".lower()
    return any(token in lowered for token in tokens)


def _logos_fandom_url_matches_target(*, url: str, tokens: set[str]) -> bool:
    lowered_url = _normalize_text(url).casefold()
    if not tokens:
        return True
    if any(token in lowered_url for token in tokens):
        return True
    if "youtube" in tokens and ("yt_" in lowered_url or "/yt" in lowered_url):
        return True
    return False


def _provider_min_match_score(provider: str, *, tokens: set[str]) -> int:
    normalized = _normalize_text(provider).casefold()
    base = 1
    if normalized in {"seeklogo", "worldvectorlogo", "logosearch", "logowik"}:
        base = 2
    token_cap = max(1, len(tokens))
    return min(base, token_cap)


def _expand_wikimedia_search_terms(query_terms: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for term in query_terms[:2]:
        normalized = _normalize_text(term)
        if not normalized:
            continue
        for variant in (f"{normalized} logo", f"{normalized} icon", normalized):
            key = variant.casefold()
            if key in seen:
                continue
            seen.add(key)
            out.append(variant)
    return out or query_terms[:1]


def search_wikimedia_commons_logo_candidates(
    query_terms: list[str],
    *,
    limit: int = 8,
    timeout_seconds: float = DEFAULT_READ_TIMEOUT_SECONDS,
    session: requests.Session | None = None,
) -> list[FreeLogoCandidate]:
    session = session or requests.Session()
    candidates: list[FreeLogoCandidate] = []
    match_tokens = _target_host_tokens("", query_terms)

    for term in _expand_wikimedia_search_terms(query_terms):
        search_url = WIKIMEDIA_MEDIASEARCH_URL.format(query=quote_plus(term))
        response = _safe_get(session, search_url, timeout_seconds=timeout_seconds)
        if response is None or response.status_code >= 400 or not response.text:
            continue
        soup = BeautifulSoup(response.text, "html.parser")
        for anchor in soup.select("a[href*='/wiki/File:'], a[href*='title=File:']"):
            href = _normalize_text(anchor.get("href"))
            if not href:
                continue
            source_page = _normalize_candidate_url(href, base_url=str(response.url or search_url))
            if "File:" not in source_page:
                continue
            title_fragment = source_page.split("File:", 1)[1]
            title_fragment = title_fragment.split("#", 1)[0]
            title_fragment = title_fragment.split("?", 1)[0]
            title_fragment = title_fragment.replace("_", " ")
            lowered_title = title_fragment.casefold()
            if match_tokens and not any(token in lowered_title for token in match_tokens):
                continue
            file_name = title_fragment.strip().replace(" ", "_")
            if not file_name:
                continue
            encoded = quote(file_name, safe="()_-")
            candidates.append(
                FreeLogoCandidate(
                    url=f"https://commons.wikimedia.org/wiki/Special:FilePath/{encoded}?width=2048",
                    source_provider="wikimedia_commons",
                    discovered_from=source_page,
                    context=f"media_search:svg:{term}",
                )
            )
            if len(candidates) >= limit:
                break
        if len(candidates) >= limit:
            break

    return _dedupe_candidates(candidates)[:limit]


def _extract_images_from_html(
    html: str,
    *,
    base_url: str,
    source_provider: str,
    discovered_from: str,
    limit: int,
    context_hint: str = "",
) -> list[FreeLogoCandidate]:
    soup = BeautifulSoup(html, "html.parser")
    candidates: list[FreeLogoCandidate] = []

    for meta in soup.select("meta[property],meta[name]"):
        key = _normalize_text(meta.get("property") or meta.get("name")).lower()
        content = _normalize_text(meta.get("content"))
        if key in _IMAGE_META_KEYS and content:
            candidate_url = _normalize_candidate_url(content, base_url=base_url)
            if candidate_url and _looks_like_logo_candidate(candidate_url, context=key):
                candidate_provider = (
                    "favicon_appicons"
                    if source_provider == "official_site" and _is_favicon_like(candidate_url, key)
                    else source_provider
                )
                candidates.append(
                    FreeLogoCandidate(
                        url=candidate_url,
                        source_provider=candidate_provider,
                        discovered_from=discovered_from,
                        context=f"meta:{key}",
                    )
                )
                if candidates and _is_known_noise_asset(candidates[-1]):
                    candidates.pop()
                    continue
                if len(candidates) >= limit:
                    return _dedupe_candidates(candidates)[:limit]

    selector_attrs = (
        ("link[rel][href]", "href"),
        ("img[src]", "src"),
        ("img[data-src]", "data-src"),
        ("img[data-original]", "data-original"),
        ("img[data-lazy-src]", "data-lazy-src"),
    )
    for selector, attr in selector_attrs:
        for node in soup.select(selector):
            raw_url = _normalize_text(node.get(attr))
            if not raw_url:
                continue
            context = " ".join(
                [
                    _normalize_text(node.get("rel")),
                    _normalize_text(node.get("class")),
                    _normalize_text(node.get("id")),
                    _normalize_text(node.get("alt")),
                ]
            )
            candidate_url = _normalize_candidate_url(raw_url, base_url=base_url)
            if not candidate_url or not _looks_like_logo_candidate(candidate_url, context=context):
                continue
            candidate_provider = (
                "favicon_appicons"
                if source_provider == "official_site" and _is_favicon_like(candidate_url, context)
                else source_provider
            )
            candidates.append(
                FreeLogoCandidate(
                    url=candidate_url,
                    source_provider=candidate_provider,
                    discovered_from=discovered_from,
                    context=context_hint or context,
                )
            )
            if candidates and _is_known_noise_asset(candidates[-1]):
                candidates.pop()
                continue
            if len(candidates) >= limit:
                return _dedupe_candidates(candidates)[:limit]

    return _dedupe_candidates(candidates)[:limit]


def _extract_brand_guideline_links(html: str, *, base_url: str, limit: int = 4) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    links: list[str] = []
    seen: set[str] = set()
    for anchor in soup.select("a[href]"):
        href = _normalize_text(anchor.get("href"))
        if not href:
            continue
        text = f"{href} {_normalize_text(anchor.get_text())}".lower()
        if not any(hint in text for hint in _GUIDELINE_PATH_HINTS):
            continue
        resolved = _normalize_candidate_url(href, base_url=base_url)
        if not resolved or resolved in seen:
            continue
        seen.add(resolved)
        links.append(resolved)
        if len(links) >= limit:
            break
    return links


def _is_fandom_community_host(host: str) -> bool:
    normalized = _normalize_text(host).lower()
    return bool(normalized) and (
        normalized.endswith(".fandom.com")
        or normalized.endswith(".wikia.com")
        or normalized in {"fandom.com", "wikia.com"}
    )


def _build_official_entry_urls(host: str, path: str) -> list[str]:
    normalized_host = _normalize_text(host).lower()
    normalized_path = _normalize_text(path)
    base_url = f"https://{normalized_host}"

    candidates: list[str] = []
    seen: set[str] = set()

    def add(url: str) -> None:
        normalized_url = _normalize_text(url)
        if not normalized_url or normalized_url in seen:
            return
        seen.add(normalized_url)
        candidates.append(normalized_url)

    if normalized_path and normalized_path != "/":
        add(f"{base_url}{normalized_path.rstrip('/')}")
    else:
        add(base_url)

    if _is_fandom_community_host(normalized_host):
        add(f"{base_url}/wiki/Home_Page")
        add(FANDOM_BRAND_LOGO_PAGE_URL)

    return candidates


def _fandom_brand_asset_candidates(*, discovered_from: str) -> list[FreeLogoCandidate]:
    return [
        FreeLogoCandidate(
            url=FANDOM_STANDARD_WORDMARK_URL,
            source_provider="official_site",
            discovered_from=discovered_from,
            context="fandom_brand_wordmark",
        ),
        FreeLogoCandidate(
            url=FANDOM_STANDARD_ICON_URL,
            source_provider="official_site",
            discovered_from=discovered_from,
            context="fandom_brand_icon",
        ),
    ]


def _is_fandom_brand_asset_page(url: str) -> bool:
    return _normalize_text(url).rstrip("/") == FANDOM_BRAND_LOGO_PAGE_URL.rstrip("/")


def _extract_fandom_page_title(entry_url: str) -> str:
    parsed = urlparse(entry_url)
    path = _normalize_text(parsed.path)
    if not path or path == "/":
        return "Home_Page"
    if path.startswith("/wiki/"):
        title = path.split("/wiki/", 1)[1].strip("/")
        return title or "Home_Page"
    return "Home_Page"


def _fetch_fandom_headhtml(
    session: requests.Session,
    *,
    entry_url: str,
    timeout_seconds: float,
) -> tuple[str, str] | None:
    parsed = urlparse(entry_url)
    host = _normalize_text(parsed.netloc).lower()
    if host.startswith("www."):
        host = host[4:]
    if not host.endswith((".fandom.com", ".wikia.com")):
        return None

    response = None
    try:
        response = session.get(
            f"https://{host}/api.php",
            params={
                "action": "parse",
                "page": _extract_fandom_page_title(entry_url),
                "prop": "headhtml",
                "format": "json",
            },
            headers={"accept": "application/json", "user-agent": "TRR-Backend/1.0"},
            timeout=(min(DEFAULT_CONNECT_TIMEOUT_SECONDS, timeout_seconds), timeout_seconds),
        )
    except requests.RequestException:
        return None
    if response is None or response.status_code >= 400:
        return None
    try:
        payload = response.json()
    except ValueError:
        return None
    parse_payload = payload.get("parse") if isinstance(payload, dict) else None
    headhtml = parse_payload.get("headhtml") if isinstance(parse_payload, dict) else None
    html = headhtml.get("*") if isinstance(headhtml, dict) else None
    if not isinstance(html, str) or not html.strip():
        return None
    return entry_url, html


def _extract_inline_svg_data_urls(
    *,
    html: str,
    source_provider: str,
    discovered_from: str,
    limit: int,
) -> list[FreeLogoCandidate]:
    soup = BeautifulSoup(html, "html.parser")
    candidates: list[FreeLogoCandidate] = []
    for svg in soup.select("svg"):
        label_hint = " ".join(
            [
                _normalize_text(svg.get("aria-label")),
                _normalize_text(svg.get("id")),
                _normalize_text(svg.get("class")),
            ]
        ).lower()
        if any(token in label_hint for token in ("menu", "search", "guide", "chevron", "close")):
            continue
        if not any(token in label_hint for token in ("logo", "brand", "wordmark", "icon", "symbol", "google")):
            continue
        raw_svg = _normalize_text(str(svg))
        if "<path" not in raw_svg and "<text" not in raw_svg:
            continue
        encoded_svg = base64.b64encode(raw_svg.encode("utf-8")).decode("ascii")
        candidates.append(
            FreeLogoCandidate(
                url=f"data:image/svg+xml;base64,{encoded_svg}",
                source_provider=source_provider,
                discovered_from=discovered_from,
                context="inline_svg",
            )
        )
        if len(candidates) >= limit:
            break
    return _dedupe_candidates(candidates)[:limit]


def extract_official_logo_candidates(
    source_urls: list[str],
    *,
    limit: int = 12,
    timeout_seconds: float = DEFAULT_READ_TIMEOUT_SECONDS,
    session: requests.Session | None = None,
) -> list[FreeLogoCandidate]:
    session = session or requests.Session()
    candidates: list[FreeLogoCandidate] = []
    checked: set[str] = set()

    for source_url in source_urls:
        normalized_url = _normalize_text(source_url)
        if not normalized_url:
            continue
        parsed = urlparse(normalized_url if "://" in normalized_url else f"https://{normalized_url}")
        host = _normalize_text(parsed.netloc or parsed.path).lower()
        path = _normalize_text(parsed.path if parsed.netloc else "")
        if host.startswith("www."):
            host = host[4:]
        if not host:
            continue
        for entry_url in _build_official_entry_urls(host, path):
            if entry_url in checked:
                continue
            checked.add(entry_url)

            if _is_fandom_brand_asset_page(entry_url):
                candidates.extend(_fandom_brand_asset_candidates(discovered_from=entry_url))
                if len(candidates) >= limit:
                    break
                continue

            response = _safe_get(session, entry_url, timeout_seconds=timeout_seconds)
            if response is None or response.status_code >= 400 or not response.text:
                fandom_headhtml = _fetch_fandom_headhtml(session, entry_url=entry_url, timeout_seconds=timeout_seconds)
                if fandom_headhtml is None:
                    continue
                discovered_from, html = fandom_headhtml
            else:
                discovered_from = str(response.url or entry_url)
                html = response.text
            page_provider = (
                "brand_guidelines"
                if any(hint in discovered_from.casefold() for hint in _GUIDELINE_PATH_HINTS)
                else "official_site"
            )

            candidates.extend(
                _extract_inline_svg_data_urls(
                    html=html,
                    source_provider=page_provider,
                    discovered_from=discovered_from,
                    limit=limit,
                )
            )
            if len(candidates) >= limit:
                break

            candidates.extend(
                _extract_images_from_html(
                    html,
                    base_url=discovered_from,
                    source_provider=page_provider,
                    discovered_from=discovered_from,
                    limit=limit,
                    context_hint="homepage" if page_provider == "official_site" else "brand_guidelines",
                )
            )
            if len(candidates) >= limit:
                break

            guideline_links = _extract_brand_guideline_links(html, base_url=discovered_from, limit=3)
            for guideline_url in guideline_links:
                guideline_host = _normalize_hostname_from_url(guideline_url)
                if guideline_host and not _host_matches_target(guideline_host, host):
                    continue
                guideline_response = _safe_get(session, guideline_url, timeout_seconds=timeout_seconds)
                if guideline_response is None or guideline_response.status_code >= 400 or not guideline_response.text:
                    continue
                candidates.extend(
                    _extract_images_from_html(
                        guideline_response.text,
                        base_url=str(guideline_response.url or guideline_url),
                        source_provider="brand_guidelines",
                        discovered_from=str(guideline_response.url or guideline_url),
                        limit=limit,
                        context_hint="brand_guidelines",
                    )
                )
                if len(candidates) >= limit:
                    break
            if len(candidates) >= limit:
                break
        if len(candidates) >= limit:
            break

    deduped = _dedupe_candidates(candidates)
    filtered = [row for row in deduped if row.source_provider != "official_site" or _is_png_or_svg_logo_url(row.url)]
    return filtered[:limit]


def search_1000logos_logo_candidates(
    query_terms: list[str],
    *,
    exact_slug: str | None = None,
    limit: int = 8,
    timeout_seconds: float = DEFAULT_READ_TIMEOUT_SECONDS,
    session: requests.Session | None = None,
) -> list[FreeLogoCandidate]:
    session = session or requests.Session()
    candidates: list[FreeLogoCandidate] = []
    article_urls: list[str] = []
    seen_articles: set[str] = set()
    match_tokens = _target_host_tokens("", query_terms)

    normalized_exact_slug = _normalize_source_query_value("logos1000", exact_slug or "")
    if normalized_exact_slug:
        fallback_article = _1000logos_article_url(normalized_exact_slug)
        if fallback_article not in seen_articles:
            seen_articles.add(fallback_article)
            article_urls.append(fallback_article)

    for term in query_terms[:2]:
        wp_search_url = LOGOS_1000_WP_SEARCH_URL.format(query=quote_plus(term))
        wp_response = _safe_get(session, wp_search_url, timeout_seconds=timeout_seconds)
        if wp_response is not None and wp_response.status_code < 400:
            try:
                wp_rows = wp_response.json()
            except ValueError:
                wp_rows = []
            if isinstance(wp_rows, list):
                for row in wp_rows:
                    if not isinstance(row, dict):
                        continue
                    link = _normalize_text(row.get("link"))
                    if not link or "-logo" not in link:
                        continue
                    lowered_link = link.casefold()
                    if match_tokens and not any(token in lowered_link for token in match_tokens):
                        continue
                    if link in seen_articles:
                        continue
                    seen_articles.add(link)
                    article_urls.append(link)
                    if len(article_urls) >= 5:
                        break

        search_url = LOGOS_1000_SEARCH_URL.format(query=quote_plus(term))
        response = _safe_get(session, search_url, timeout_seconds=timeout_seconds)
        if response is None or response.status_code >= 400 or not response.text:
            continue

        soup = BeautifulSoup(response.text, "html.parser")
        for anchor in soup.select("a[href]"):
            href = _normalize_text(anchor.get("href"))
            if not href:
                continue
            resolved = _normalize_candidate_url(href, base_url=str(response.url or search_url))
            if "1000logos.net" not in resolved:
                continue
            if "-logo" not in resolved:
                continue
            lowered_resolved = resolved.casefold()
            if match_tokens and not any(token in lowered_resolved for token in match_tokens):
                continue
            if resolved in seen_articles:
                continue
            seen_articles.add(resolved)
            article_urls.append(resolved)
            if len(article_urls) >= 4:
                break

        slug = re.sub(r"[^a-z0-9]+", "-", term.casefold()).strip("-")
        if slug:
            fallback_article = _1000logos_article_url(slug)
            if fallback_article not in seen_articles:
                seen_articles.add(fallback_article)
                article_urls.append(fallback_article)

    for article_url in article_urls:
        response = _safe_get(session, article_url, timeout_seconds=timeout_seconds)
        if response is None or response.status_code >= 400 or not response.text:
            continue
        page_soup = BeautifulSoup(response.text, "html.parser")
        parsed_candidates: list[FreeLogoCandidate] = []
        content_root = page_soup.select_one(".entry-content") or page_soup
        for image in content_root.select("img[data-src], img[src], a[href] img[data-src], a[href] img[src]"):
            source_url = _normalize_candidate_url(
                _normalize_text(image.get("data-src") or image.get("src")),
                base_url=str(response.url or article_url),
            )
            if not source_url or not _looks_like_logo_candidate(source_url):
                continue
            parsed_candidates.append(
                FreeLogoCandidate(
                    url=source_url,
                    source_provider="logos1000",
                    discovered_from=str(response.url or article_url),
                    context=_normalize_text(image.get("alt")) or "entry-content",
                )
            )
        for candidate in parsed_candidates:
            lowered = candidate.url.lower()
            if "/wp-content/uploads/" not in lowered and "special:filepath/" not in lowered:
                continue
            if not _has_url_or_context_token_match(candidate, tokens=match_tokens):
                continue
            if _candidate_match_score(candidate, tokens=match_tokens) < _provider_min_match_score(
                "logos1000",
                tokens=match_tokens,
            ):
                continue
            candidates.append(candidate)
            if len(candidates) >= limit:
                break
        if len(candidates) >= limit:
            break

    return _dedupe_candidates([row for row in candidates if not _is_known_noise_asset(row)])[:limit]


def _search_aggregator_logo_candidates(
    query_terms: list[str],
    *,
    source_provider: str,
    search_urls: list[str],
    host_contains: str,
    limit: int = 8,
    timeout_seconds: float = DEFAULT_READ_TIMEOUT_SECONDS,
    session: requests.Session | None = None,
) -> list[FreeLogoCandidate]:
    session = session or requests.Session()
    candidates: list[FreeLogoCandidate] = []
    visited_pages: set[str] = set()
    match_tokens = _target_host_tokens("", query_terms)

    for term in query_terms[:2]:
        if len(candidates) >= limit:
            break
        slug = _slugify(term)
        for template in search_urls:
            search_url = template.format(query=quote_plus(term), slug=slug)
            if search_url in visited_pages:
                continue
            response = _safe_get(session, search_url, timeout_seconds=timeout_seconds)
            if response is None or response.status_code >= 400 or not response.text:
                continue
            current_url = str(response.url or search_url)
            visited_pages.add(current_url)

            parsed_candidates = _extract_images_from_html(
                response.text,
                base_url=current_url,
                source_provider=source_provider,
                discovered_from=current_url,
                limit=limit,
                context_hint="search",
            )
            for candidate in parsed_candidates:
                if _is_known_noise_asset(candidate):
                    continue
                if match_tokens and _candidate_match_score(candidate, tokens=match_tokens) <= 0:
                    continue
                candidates.append(candidate)
            if len(candidates) >= limit:
                break

            soup = BeautifulSoup(response.text, "html.parser")
            article_urls: list[str] = []
            seen_articles: set[str] = set()
            for anchor in soup.select("a[href]"):
                href = _normalize_text(anchor.get("href"))
                if not href:
                    continue
                resolved = _normalize_candidate_url(href, base_url=current_url)
                if host_contains not in resolved:
                    continue
                text_hint = f"{resolved} {_normalize_text(anchor.get_text())}".lower()
                if "logo" not in text_hint and "/brands/" not in text_hint and "/brand/" not in text_hint:
                    continue
                if resolved in seen_articles:
                    continue
                seen_articles.add(resolved)
                article_urls.append(resolved)
                if len(article_urls) >= 4:
                    break

            for article_url in article_urls:
                page = _safe_get(session, article_url, timeout_seconds=timeout_seconds)
                if page is None or page.status_code >= 400 or not page.text:
                    continue
                discovered_from = str(page.url or article_url)
                page_candidates = _extract_images_from_html(
                    page.text,
                    base_url=discovered_from,
                    source_provider=source_provider,
                    discovered_from=discovered_from,
                    limit=limit,
                    context_hint="article",
                )
                for candidate in page_candidates:
                    if _is_known_noise_asset(candidate):
                        continue
                    if match_tokens and _candidate_match_score(candidate, tokens=match_tokens) <= 0:
                        continue
                    candidates.append(candidate)
                if len(candidates) >= limit:
                    break
            if len(candidates) >= limit:
                break

    return _dedupe_candidates(candidates)[:limit]


def search_worldvectorlogo_logo_candidates(
    query_terms: list[str],
    *,
    limit: int = 8,
    timeout_seconds: float = DEFAULT_READ_TIMEOUT_SECONDS,
    session: requests.Session | None = None,
) -> list[FreeLogoCandidate]:
    session = session or requests.Session()
    candidates: list[FreeLogoCandidate] = []
    match_tokens = _target_host_tokens("", query_terms)
    for term in query_terms[:2]:
        search_url = WORLDVECTORLOGO_SEARCH_URL.format(query=quote_plus(term))
        response = _safe_get(session, search_url, timeout_seconds=timeout_seconds)
        if response is None or response.status_code >= 400 or not response.text:
            continue
        soup = BeautifulSoup(response.text, "html.parser")
        for anchor in soup.select("main a[href*='/logo/']"):
            image = anchor.select_one("img[src*='cdn.worldvectorlogo.com/logos/']")
            if image is None:
                continue
            article_url = _normalize_candidate_url(
                _normalize_text(anchor.get("href")),
                base_url=str(response.url or search_url),
            )
            source_url = _normalize_candidate_url(
                _normalize_text(image.get("src")),
                base_url=str(response.url or search_url),
            )
            if not source_url or not _looks_like_logo_candidate(source_url):
                continue
            title = _normalize_text(anchor.get_text(" ", strip=True)) or _normalize_text(image.get("alt"))
            candidate = FreeLogoCandidate(
                url=source_url,
                source_provider="worldvectorlogo",
                discovered_from=article_url,
                context=f"search_result:{title}",
            )
            if _is_known_noise_asset(candidate):
                continue
            if _candidate_match_score(candidate, tokens=match_tokens) < _provider_min_match_score(
                "worldvectorlogo",
                tokens=match_tokens,
            ):
                continue
            candidates.append(candidate)
            if len(candidates) >= limit:
                break
        if len(candidates) >= limit:
            break
    return _dedupe_candidates(candidates)[:limit]


def search_seeklogo_logo_candidates(
    query_terms: list[str],
    *,
    limit: int = 8,
    timeout_seconds: float = DEFAULT_READ_TIMEOUT_SECONDS,
    session: requests.Session | None = None,
) -> list[FreeLogoCandidate]:
    session = session or requests.Session()
    candidates: list[FreeLogoCandidate] = []
    match_tokens = _target_host_tokens("", query_terms)
    for term in query_terms[:2]:
        search_url = SEEKLOGO_SEARCH_URL.format(query=quote_plus(term))
        response = _safe_get(session, search_url, timeout_seconds=timeout_seconds)
        if response is None or response.status_code >= 400 or not response.text:
            continue
        soup = BeautifulSoup(response.text, "html.parser")
        for anchor in soup.select("a[href*='/vector-logo/']"):
            href = _normalize_text(anchor.get("href"))
            image = anchor.select_one("img[src]")
            if not href or image is None:
                continue
            source_url = _normalize_candidate_url(
                _normalize_text(image.get("src")),
                base_url=str(response.url or search_url),
            )
            if not _looks_like_logo_candidate(source_url):
                continue
            context = f"{_normalize_text(anchor.get_text())} {_normalize_text(image.get('alt'))}"
            candidate = FreeLogoCandidate(
                url=source_url,
                source_provider="seeklogo",
                discovered_from=_normalize_candidate_url(href, base_url=str(response.url or search_url)),
                context=context,
            )
            if _is_known_noise_asset(candidate):
                continue
            if _candidate_match_score(candidate, tokens=match_tokens) < _provider_min_match_score(
                "seeklogo",
                tokens=match_tokens,
            ):
                continue
            candidates.append(candidate)
            if len(candidates) >= limit:
                break
        if len(candidates) >= limit:
            break
    return _dedupe_candidates(candidates)[:limit]


def search_logowik_logo_candidates(
    query_terms: list[str],
    *,
    limit: int = 8,
    timeout_seconds: float = DEFAULT_READ_TIMEOUT_SECONDS,
    session: requests.Session | None = None,
) -> list[FreeLogoCandidate]:
    session = session or requests.Session()
    candidates: list[FreeLogoCandidate] = []
    match_tokens = _target_host_tokens("", query_terms)
    for term in query_terms[:2]:
        search_url = LOGOWIK_SEARCH_URL.format(query=quote_plus(term))
        response = _safe_get(session, search_url, timeout_seconds=timeout_seconds)
        if response is None or response.status_code >= 400 or not response.text:
            continue
        soup = BeautifulSoup(response.text, "html.parser")
        article_urls: list[str] = []
        seen_articles: set[str] = set()
        for anchor in soup.select("main a[href]"):
            href = _normalize_text(anchor.get("href"))
            if not href:
                continue
            article_url = _normalize_candidate_url(href, base_url=str(response.url or search_url))
            if "logowik.com/" not in article_url or article_url.endswith("/search"):
                continue
            text_hint = f"{article_url} {_normalize_text(anchor.get_text(' ', strip=True))}".casefold()
            if match_tokens and not any(token in text_hint for token in match_tokens):
                continue
            if article_url in seen_articles:
                continue
            seen_articles.add(article_url)
            article_urls.append(article_url)
            if len(article_urls) >= max(limit, 12):
                break

        for article_url in article_urls:
            article_response = _safe_get(session, article_url, timeout_seconds=timeout_seconds)
            if article_response is None or article_response.status_code >= 400 or not article_response.text:
                continue
            page_soup = BeautifulSoup(article_response.text, "html.parser")
            page_candidates: list[FreeLogoCandidate] = []

            for img in page_soup.select("img[src], img[data-src]"):
                raw_url = _normalize_text(img.get("data-src") or img.get("src"))
                source_url = _normalize_candidate_url(raw_url, base_url=str(article_response.url or article_url))
                if not source_url or not _looks_like_logo_candidate(source_url):
                    continue
                if "content/uploads/" not in source_url:
                    continue
                page_candidates.append(
                    FreeLogoCandidate(
                        url=source_url,
                        source_provider="logowik",
                        discovered_from=str(article_response.url or article_url),
                        context=_normalize_text(img.get("alt")) or "article_image",
                    )
                )

            for textarea in page_soup.select("textarea"):
                match = re.search(r"https?://[^\\s'\"\\]]+\\.(?:svg|png|webp|jpg|jpeg)", textarea.text, re.IGNORECASE)
                if not match:
                    continue
                source_url = _normalize_text(match.group(0))
                page_candidates.append(
                    FreeLogoCandidate(
                        url=source_url,
                        source_provider="logowik",
                        discovered_from=str(article_response.url or article_url),
                        context="embed_snippet",
                    )
                )

            for candidate in _dedupe_candidates(page_candidates):
                if _is_known_noise_asset(candidate):
                    continue
                if not _has_url_or_context_token_match(candidate, tokens=match_tokens):
                    continue
                if _candidate_match_score(candidate, tokens=match_tokens) < _provider_min_match_score(
                    "logowik",
                    tokens=match_tokens,
                ):
                    continue
                candidates.append(candidate)
                if len(candidates) >= limit:
                    break
            if len(candidates) >= limit:
                break
        if len(candidates) >= limit:
            break
    return _dedupe_candidates(candidates)[:limit]


def search_logo_wine_logo_candidates(
    query_terms: list[str],
    *,
    limit: int = 8,
    timeout_seconds: float = DEFAULT_READ_TIMEOUT_SECONDS,
    session: requests.Session | None = None,
) -> list[FreeLogoCandidate]:
    session = session or requests.Session()
    candidates: list[FreeLogoCandidate] = []
    match_tokens = _target_host_tokens("", query_terms)
    for term in query_terms[:2]:
        search_url = LOGOWINE_SEARCH_URL.format(query=quote_plus(term))
        response = _safe_get(session, search_url, timeout_seconds=timeout_seconds)
        if response is None or response.status_code >= 400 or not response.text:
            continue
        soup = BeautifulSoup(response.text, "html.parser")
        article_urls: list[str] = []
        seen_articles: set[str] = set()
        for anchor in soup.select("a[href^='/logo/']"):
            article_url = _normalize_candidate_url(
                _normalize_text(anchor.get("href")),
                base_url=str(response.url or search_url),
            )
            if not article_url:
                continue
            context = _normalize_text(anchor.get_text(" ", strip=True))
            lowered = f"{article_url} {context}".casefold()
            if match_tokens and (
                sum(1 for token in match_tokens if token in lowered)
                < _provider_min_match_score("logo_wine", tokens=match_tokens)
            ):
                continue
            if article_url in seen_articles:
                continue
            seen_articles.add(article_url)
            article_urls.append(article_url)
            if len(article_urls) >= max(limit, 12):
                break
        for article_url in article_urls:
            page = _safe_get(session, article_url, timeout_seconds=timeout_seconds)
            if page is None or page.status_code >= 400 or not page.text:
                continue
            page_soup = BeautifulSoup(page.text, "html.parser")
            for anchor in page_soup.select("a[href]"):
                href = _normalize_text(anchor.get("href"))
                if not href:
                    continue
                source_url = _normalize_candidate_url(href, base_url=str(page.url or article_url))
                if not _looks_like_logo_candidate(source_url):
                    continue
                if ".logo.wine.svg" not in source_url and "download.logo.wine" not in source_url:
                    continue
                candidate = FreeLogoCandidate(
                    url=source_url,
                    source_provider="logo_wine",
                    discovered_from=str(page.url or article_url),
                    context=_normalize_text(anchor.get_text(" ", strip=True)) or "logo_page",
                )
                if _candidate_match_score(candidate, tokens=match_tokens) < _provider_min_match_score(
                    "logo_wine",
                    tokens=match_tokens,
                ):
                    continue
                candidates.append(candidate)
                if len(candidates) >= limit:
                    return _dedupe_candidates(candidates)[:limit]
    return _dedupe_candidates(candidates)[:limit]


def search_logosearch_logo_candidates(
    query_terms: list[str],
    *,
    limit: int = 8,
    timeout_seconds: float = DEFAULT_READ_TIMEOUT_SECONDS,
    session: requests.Session | None = None,
) -> list[FreeLogoCandidate]:
    session = session or requests.Session()
    candidates: list[FreeLogoCandidate] = []
    match_tokens = _target_host_tokens("", query_terms)
    for term in query_terms[:2]:
        search_url = LOGOSEARCH_SEARCH_URL.format(query=quote_plus(term))
        response = _safe_get(session, search_url, timeout_seconds=timeout_seconds)
        if response is None or response.status_code >= 400 or not response.text:
            continue
        soup = BeautifulSoup(response.text, "html.parser")
        for image in soup.select("#search_results img.boxedlogo[src], img.boxedlogo[src]"):
            source_url = _normalize_candidate_url(
                _normalize_text(image.get("src")),
                base_url=str(response.url or search_url),
            )
            if not _looks_like_logo_candidate(source_url):
                continue
            anchor = image.find_parent("a")
            discovered_from = _normalize_candidate_url(
                _normalize_text(anchor.get("href")) if anchor is not None else str(response.url or search_url),
                base_url=str(response.url or search_url),
            )
            candidate = FreeLogoCandidate(
                url=source_url,
                source_provider="logosearch",
                discovered_from=discovered_from,
                context=_normalize_text(anchor.get_text(" ", strip=True)) if anchor is not None else "search",
            )
            if _is_known_noise_asset(candidate):
                continue
            if _candidate_match_score(candidate, tokens=match_tokens) < _provider_min_match_score(
                "logosearch",
                tokens=match_tokens,
            ):
                continue
            candidates.append(candidate)
            if len(candidates) >= limit:
                break
        if len(candidates) >= limit:
            break
    return _dedupe_candidates(candidates)[:limit]


def search_simple_icons_logo_candidates(
    query_terms: list[str],
    *,
    limit: int = 6,
    timeout_seconds: float = DEFAULT_READ_TIMEOUT_SECONDS,
    session: requests.Session | None = None,
) -> list[FreeLogoCandidate]:
    session = session or requests.Session()
    candidates: list[FreeLogoCandidate] = []
    seen_slugs: set[str] = set()

    for term in query_terms:
        slug = _slugify(term).replace("-", "")
        if not slug or slug in seen_slugs:
            continue
        seen_slugs.add(slug)
        icon_url = SIMPLE_ICONS_CDN_URL.format(slug=slug)
        response = _safe_get(session, icon_url, timeout_seconds=timeout_seconds, attempts=1)
        if response is None or response.status_code >= 400:
            continue
        candidates.append(
            FreeLogoCandidate(
                url=icon_url,
                source_provider="simple_icons",
                discovered_from=SIMPLE_ICONS_SEARCH_URL.format(query=quote_plus(term)),
                context="icon-pack",
            )
        )
        if len(candidates) >= limit:
            break

    return _dedupe_candidates(candidates)[:limit]


def collect_free_logo_candidates(
    *,
    target_label: str,
    target_key: str,
    discovered_from_urls: list[str] | None = None,
    aliases: list[str] | None = None,
    source_provider: str | None = None,
    query_override: str | list[str] | tuple[str, ...] | None = None,
    limit_per_source: int = 8,
    timeout_seconds: float = DEFAULT_READ_TIMEOUT_SECONDS,
    session: requests.Session | None = None,
) -> list[FreeLogoCandidate]:
    session = session or requests.Session()
    normalized_provider = _normalize_text(source_provider).casefold()
    raw_query_values = (
        [_normalize_text(value) for value in query_override]
        if isinstance(query_override, (list, tuple))
        else [_normalize_text(query_override)]
        if query_override is not None
        else []
    )
    normalized_query_values = (
        _normalize_source_query_values(normalized_provider, query_override) if normalized_provider else []
    )
    if normalized_provider and len(raw_query_values) > 1:
        candidates: list[FreeLogoCandidate] = []
        for query_value in raw_query_values:
            if not query_value:
                continue
            candidates.extend(
                collect_free_logo_candidates(
                    target_label=target_label,
                    target_key=target_key,
                    discovered_from_urls=discovered_from_urls,
                    aliases=aliases,
                    source_provider=normalized_provider,
                    query_override=query_value,
                    limit_per_source=limit_per_source,
                    timeout_seconds=timeout_seconds,
                    session=session,
                )
            )
        return _dedupe_candidates(candidates)
    query_profile = (
        build_source_query_profile(
            source_provider=normalized_provider,
            target_label=target_label,
            target_key=target_key,
            query_override=query_override,
        )
        if normalized_provider
        else None
    )
    brand_query_term = (
        _normalize_text((query_profile or {}).get("effective_query_value"))
        if query_profile and query_profile.get("query_kind") == "search_term"
        else _derive_brand_query_term(target_label, target_key, aliases=aliases)
    )
    if normalized_provider == "logos_fandom":
        raw_effective_query = _normalize_text(query_override)
        if not raw_effective_query:
            raw_effective_query = _normalize_text((query_profile or {}).get("effective_query_value"))
        fandom_page_slug = _coerce_logos_fandom_page_slug(raw_effective_query, allow_simple=True)
        if fandom_page_slug:
            brand_query_term = _humanize_query_fragment(fandom_page_slug) or brand_query_term
    terms = [brand_query_term]
    match_tokens = _target_host_tokens(target_key, terms)
    target_host = _normalize_hostname_from_url(target_key)
    discovered_from_urls = [_normalize_text(url) for url in (discovered_from_urls or []) if _normalize_text(url)]

    def _provider_enabled(provider_name: str) -> bool:
        return not normalized_provider or normalized_provider == provider_name

    candidates: list[FreeLogoCandidate] = []
    if _provider_enabled("wikimedia_commons"):
        candidates.extend(
            search_wikimedia_commons_logo_candidates(
                terms,
                limit=limit_per_source,
                timeout_seconds=timeout_seconds,
                session=session,
            )
        )

    if _provider_enabled("logos_fandom"):
        raw_effective_query = _normalize_text(query_override)
        if not raw_effective_query:
            raw_effective_query = _normalize_text((query_profile or {}).get("effective_query_value"))
        fandom_lookup_name = _coerce_logos_fandom_page_slug(raw_effective_query, allow_simple=True) or brand_query_term
        fandom_aliases: list[str] = []
        for alias in (
            brand_query_term,
            _logos_fandom_query_search_term(raw_effective_query),
            _normalize_text((query_profile or {}).get("effective_query_value")),
        ):
            normalized_alias = _normalize_text(alias)
            if not normalized_alias or "://" in normalized_alias:
                continue
            if normalized_alias.casefold() in {value.casefold() for value in fandom_aliases}:
                continue
            fandom_aliases.append(normalized_alias)
        try:
            logopedia_urls = fetch_logopedia_logo_candidates(
                fandom_lookup_name,
                aliases=fandom_aliases or terms,
                timeout_seconds=timeout_seconds,
                session=session,
            )
        except LogopediaError:
            logopedia_urls = []
        added_from_fandom = 0
        for url in logopedia_urls:
            lowered_url = _normalize_text(url).casefold()
            if "static.wikia.nocookie.net/logopedia/images/" not in lowered_url:
                continue
            if not _logos_fandom_url_matches_target(url=url, tokens=match_tokens):
                continue
            discovered_from_links = _logos_fandom_query_links(
                raw_effective_query or _normalize_text((query_profile or {}).get("effective_query_value"))
            )
            discovered_from = (
                discovered_from_links[0]
                if discovered_from_links
                else (f"https://logos.fandom.com/wiki/Special:Search?query={quote_plus(brand_query_term)}")
            )
            candidates.append(
                FreeLogoCandidate(
                    url=url,
                    source_provider="logos_fandom",
                    discovered_from=discovered_from,
                    context="search",
                )
            )
            added_from_fandom += 1
            if added_from_fandom >= limit_per_source:
                break

    if _provider_enabled("logos1000"):
        candidates.extend(
            search_1000logos_logo_candidates(
                terms,
                exact_slug=_normalize_text((query_profile or {}).get("effective_query_value"))
                if normalized_provider == "logos1000"
                else None,
                limit=limit_per_source,
                timeout_seconds=timeout_seconds,
                session=session,
            )
        )

    if _provider_enabled("worldvectorlogo"):
        candidates.extend(
            search_worldvectorlogo_logo_candidates(
                terms,
                limit=limit_per_source,
                timeout_seconds=timeout_seconds,
                session=session,
            )
        )

    if _provider_enabled("seeklogo"):
        candidates.extend(
            search_seeklogo_logo_candidates(
                terms,
                limit=limit_per_source,
                timeout_seconds=timeout_seconds,
                session=session,
            )
        )

    if _provider_enabled("logowik"):
        candidates.extend(
            search_logowik_logo_candidates(
                terms,
                limit=limit_per_source,
                timeout_seconds=timeout_seconds,
                session=session,
            )
        )

    if _provider_enabled("logo_wine"):
        candidates.extend(
            search_logo_wine_logo_candidates(
                terms,
                limit=limit_per_source,
                timeout_seconds=timeout_seconds,
                session=session,
            )
        )

    if _provider_enabled("logosearch"):
        candidates.extend(
            search_logosearch_logo_candidates(
                terms,
                limit=limit_per_source,
                timeout_seconds=timeout_seconds,
                session=session,
            )
        )

    if _provider_enabled("simple_icons"):
        candidates.extend(
            search_simple_icons_logo_candidates(
                terms,
                limit=limit_per_source,
                timeout_seconds=timeout_seconds,
                session=session,
            )
        )

    official_sources: list[str] = []
    if normalized_provider in _HOST_OR_URL_SOURCE_PROVIDERS:
        source_seed = _normalize_text((query_profile or {}).get("effective_query_value"))
        if source_seed:
            official_sources.append(source_seed)
    for url in discovered_from_urls:
        host = _normalize_hostname_from_url(url)
        if not host:
            continue
        if target_host and not _host_matches_target(host, target_host):
            continue
        if url not in official_sources:
            official_sources.append(url)
    if target_host and not official_sources:
        for url in (f"https://{target_host}", f"https://www.{target_host}"):
            if url not in official_sources:
                official_sources.append(url)

    if (
        _provider_enabled("official_site")
        or _provider_enabled("brand_guidelines")
        or _provider_enabled("favicon_appicons")
    ):
        official_candidates = extract_official_logo_candidates(
            official_sources,
            limit=limit_per_source * 2,
            timeout_seconds=timeout_seconds,
            session=session,
        )
        if normalized_provider:
            official_candidates = [
                row
                for row in official_candidates
                if _normalize_text(row.source_provider).casefold() == normalized_provider
            ]
        candidates.extend(official_candidates)

    deduped = _dedupe_candidates(candidates)
    filtered = [
        row
        for row in deduped
        if not _is_known_noise_asset(row)
        and (
            row.source_provider in {"simple_icons", "favicon_appicons"}
            or _candidate_match_score(row, tokens=match_tokens)
            >= _provider_min_match_score(row.source_provider, tokens=match_tokens)
            or row.source_provider in {"official_site", "brand_guidelines"}
        )
    ]
    ranked = filtered or [row for row in deduped if not _is_known_noise_asset(row)]
    ranked.sort(
        key=lambda candidate: (
            *_score_candidate(candidate),
            -_candidate_match_score(candidate, tokens=match_tokens),
        )
    )
    return ranked
