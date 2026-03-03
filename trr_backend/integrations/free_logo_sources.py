from __future__ import annotations

import base64
import re
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote, quote_plus, urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from trr_backend.integrations.logopedia import LogopediaError, fetch_logopedia_logo_candidates

WIKIMEDIA_MEDIASEARCH_URL = (
    "https://commons.wikimedia.org/w/index.php"
    "?search={query}&title=Special%3AMediaSearch&type=image&filemime=svg"
)
LOGOS_1000_SEARCH_URL = "https://1000logos.net/?s={query}"
LOGOS_1000_ARTICLE_SLUG_URL = "https://1000logos.net/{slug}-logo/"
LOGOS_1000_WP_SEARCH_URL = "https://1000logos.net/wp-json/wp/v2/posts?search={query}&per_page=6&_fields=link,slug,title"
WORLDVECTORLOGO_SEARCH_URL = "https://worldvectorlogo.com/search?q={query}"
SEEKLOGO_SEARCH_URL = "https://seeklogo.com/search?q={query}"
LOGOWIK_SEARCH_URL = "https://logowik.com/search?q={query}"
LOGOWINE_SEARCH_URL = "https://www.logo.wine/?s={query}"
LOGOSEARCH_SEARCH_URL = "https://logosear.ch/search.html?q={query}"
SIMPLE_ICONS_CDN_URL = "https://cdn.simpleicons.org/{slug}"
SIMPLE_ICONS_SEARCH_URL = "https://simpleicons.org/?q={query}"
DEFAULT_CONNECT_TIMEOUT_SECONDS = 5.0
DEFAULT_READ_TIMEOUT_SECONDS = 15.0
DEFAULT_RETRY_ATTEMPTS = 2
DEFAULT_RETRY_BACKOFF_MS = 250

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
        terms = [
            token
            for token in text.split()
            if token and token.casefold() not in _GENERIC_QUERY_TOKENS
        ]
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
        if host.startswith("www."):
            host = host[4:]
        if not host:
            continue
        homepage = f"https://{host}"
        if homepage in checked:
            continue
        checked.add(homepage)

        response = _safe_get(session, homepage, timeout_seconds=timeout_seconds)
        if response is None or response.status_code >= 400 or not response.text:
            continue

        candidates.extend(
            _extract_inline_svg_data_urls(
                html=response.text,
                source_provider="official_site",
                discovered_from=str(response.url or homepage),
                limit=limit,
            )
        )
        if len(candidates) >= limit:
            break

        candidates.extend(
            _extract_images_from_html(
                response.text,
                base_url=str(response.url or homepage),
                source_provider="official_site",
                discovered_from=str(response.url or homepage),
                limit=limit,
                context_hint="homepage",
            )
        )
        if len(candidates) >= limit:
            break

        guideline_links = _extract_brand_guideline_links(response.text, base_url=str(response.url or homepage), limit=3)
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

    return _dedupe_candidates(candidates)[:limit]


def search_1000logos_logo_candidates(
    query_terms: list[str],
    *,
    limit: int = 8,
    timeout_seconds: float = DEFAULT_READ_TIMEOUT_SECONDS,
    session: requests.Session | None = None,
) -> list[FreeLogoCandidate]:
    session = session or requests.Session()
    candidates: list[FreeLogoCandidate] = []
    article_urls: list[str] = []
    seen_articles: set[str] = set()
    match_tokens = _target_host_tokens("", query_terms)

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
            fallback_article = LOGOS_1000_ARTICLE_SLUG_URL.format(slug=slug)
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
        for image in content_root.select(
            "img[data-src], img[src], a[href] img[data-src], a[href] img[src]"
        ):
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
    limit_per_source: int = 8,
    timeout_seconds: float = DEFAULT_READ_TIMEOUT_SECONDS,
    session: requests.Session | None = None,
) -> list[FreeLogoCandidate]:
    session = session or requests.Session()
    brand_query_term = _derive_brand_query_term(target_label, target_key, aliases=aliases)
    terms = [brand_query_term]
    match_tokens = _target_host_tokens(target_key, terms)
    target_host = _normalize_hostname_from_url(target_key)
    discovered_from_urls = [
        _normalize_text(url) for url in (discovered_from_urls or []) if _normalize_text(url)
    ]
    normalized_provider = _normalize_text(source_provider).casefold()

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
        try:
            logopedia_urls = fetch_logopedia_logo_candidates(
                brand_query_term,
                aliases=terms,
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
            discovered_from = (
                "https://logos.fandom.com/wiki/Special:Search?query="
                f"{quote_plus(brand_query_term)}"
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
    for url in discovered_from_urls:
        host = _normalize_hostname_from_url(url)
        if not host:
            continue
        if target_host and not _host_matches_target(host, target_host):
            continue
        if url not in official_sources:
            official_sources.append(url)
    if target_host:
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
