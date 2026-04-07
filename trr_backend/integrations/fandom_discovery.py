from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from urllib.parse import quote, urljoin, urlparse

from bs4 import BeautifulSoup

from trr_backend.integrations.fandom import (
    build_fandom_wiki_url_from_name,
    fetch_html,
    is_allowlisted_fandom_domain,
    load_fandom_community_allowlist,
    search_allowlisted_fandom_wikis,
)

_SPECIAL_ALLPAGES_PATH = "/wiki/Special:AllPages"
_SEASON_TOKEN_RE = re.compile(r"\bseason\s+(\d{1,2})\b", re.IGNORECASE)
_SKIPPED_ALLPAGES_PREFIXES = (
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


@dataclass(frozen=True)
class FandomCandidatePage:
    url: str
    title: str
    source: str
    domain: str
    score: float


def _normalize_space(value: str | None) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _tokenize(value: str) -> set[str]:
    return {part for part in re.findall(r"[a-z0-9]+", value.lower()) if part}


def _extract_wiki_title_from_url(url: str) -> str:
    parsed = urlparse(url)
    path = parsed.path or ""
    if "/wiki/" not in path:
        return ""
    raw = path.split("/wiki/", 1)[1]
    raw = raw.split("/", 1)[0]
    return _normalize_space(raw.replace("_", " "))


def _normalize_domain(value: str) -> str:
    parsed = urlparse(value)
    host = parsed.netloc or parsed.path
    host = host.strip().lower().strip(".")
    if host.startswith("www."):
        host = host[4:]
    return host


def _is_content_allpages_title(title: str) -> bool:
    normalized = _normalize_space(title)
    if not normalized:
        return False
    lowered = normalized.casefold()
    return not any(lowered.startswith(prefix) for prefix in _SKIPPED_ALLPAGES_PREFIXES)


def parse_allpages_html_page(
    html: str,
    *,
    current_url: str,
) -> tuple[list[str], str | None]:
    if not html:
        return [], None

    soup = BeautifulSoup(html, "html.parser")
    titles: list[str] = []
    for anchor in soup.select("div.mw-allpages-body a[href]"):
        title = _normalize_space(anchor.get_text(" ", strip=True))
        if not _is_content_allpages_title(title):
            continue
        titles.append(title)

    next_url: str | None = None
    for anchor in soup.select("div.mw-allpages-nav a[href]"):
        label = _normalize_space(anchor.get_text(" ", strip=True))
        if "next page" not in label.casefold():
            continue
        href = str(anchor.get("href") or "").strip()
        if not href:
            continue
        next_url = urljoin(current_url, href)
        break

    return titles, next_url


def _score_candidate(
    *,
    title: str,
    query_name: str,
    entity_kind: str,
    season_number: int | None,
) -> float:
    score = 0.0
    title_norm = _normalize_space(title)
    if not title_norm:
        return score

    title_tokens = _tokenize(title_norm)
    query_tokens = _tokenize(query_name)
    overlap = len(title_tokens & query_tokens)
    if overlap:
        score += overlap * 4.0

    if title_norm.casefold() == query_name.casefold():
        score += 12.0
    elif query_name.casefold() in title_norm.casefold():
        score += 5.0

    if entity_kind == "season":
        season = season_number or 0
        if season > 0:
            if re.search(rf"\bseason\s+{season}\b", title_norm, flags=re.IGNORECASE):
                score += 10.0
            matched = _SEASON_TOKEN_RE.search(title_norm)
            if matched and int(matched.group(1)) != season:
                score -= 6.0

    if title_norm.casefold().endswith("gallery"):
        score -= 3.0

    return score


def _allpages_api_titles(
    domain: str,
    *,
    max_pages: int,
    timeout_seconds: float,
) -> list[str]:
    titles: list[str] = []
    continuation: str | None = None
    pages = 0
    while pages < max_pages:
        pages += 1
        query_params = [
            ("action", "query"),
            ("list", "allpages"),
            ("aplimit", "500"),
            ("format", "json"),
        ]
        if continuation:
            query_params.append(("apcontinue", continuation))
        query = "&".join(f"{k}={quote(v, safe='')}" for k, v in query_params)
        url = f"https://{domain}/api.php?{query}"
        status, body, _error = fetch_html(url, timeout=timeout_seconds, headers={"accept": "application/json"})
        if status != 200 or not body:
            break
        try:
            import json

            payload = json.loads(body)
        except ValueError:
            break
        query_obj = payload.get("query") if isinstance(payload, dict) else None
        allpages = query_obj.get("allpages") if isinstance(query_obj, dict) else None
        if isinstance(allpages, list):
            for item in allpages:
                if not isinstance(item, dict):
                    continue
                title = _normalize_space(item.get("title"))
                if title:
                    titles.append(title)
        cont_obj = payload.get("continue") if isinstance(payload, dict) else None
        continuation = cont_obj.get("apcontinue") if isinstance(cont_obj, dict) else None
        if not continuation:
            break
    return titles


def _allpages_html_titles(
    domain: str,
    *,
    max_pages: int,
    timeout_seconds: float,
) -> list[str]:
    titles: list[str] = []
    url = f"https://{domain}{_SPECIAL_ALLPAGES_PATH}"
    seen_urls: set[str] = set()
    pages = 0

    while url and pages < max_pages and url not in seen_urls:
        seen_urls.add(url)
        pages += 1
        status, body, _error = fetch_html(url, timeout=timeout_seconds)
        if status != 200 or not body:
            break
        page_titles, next_url = parse_allpages_html_page(body, current_url=url)
        titles.extend(page_titles)
        url = next_url

    return titles


def list_allpages_titles(
    domain: str,
    *,
    max_pages: int = 2,
    timeout_seconds: float = 20.0,
) -> list[str]:
    titles = _allpages_api_titles(domain, max_pages=max_pages, timeout_seconds=timeout_seconds)
    if titles:
        return titles
    return _allpages_html_titles(domain, max_pages=max_pages, timeout_seconds=timeout_seconds)


def discover_fandom_candidate_pages(
    *,
    query_name: str,
    entity_kind: str,
    season_number: int | None = None,
    manual_page_urls: Iterable[str] | None = None,
    community_domains: Iterable[str] | None = None,
    include_allpages_scan: bool = False,
    allpages_max_pages: int = 2,
    max_candidates: int = 8,
) -> list[FandomCandidatePage]:
    raw_allowlist = tuple(community_domains or load_fandom_community_allowlist())
    domains = [_normalize_domain(item) for item in raw_allowlist if _normalize_domain(item)]
    domains = [domain for domain in domains if is_allowlisted_fandom_domain(domain, allowlist=raw_allowlist)]
    if not domains:
        return []

    query_name = _normalize_space(query_name)
    if not query_name:
        return []

    candidates: list[FandomCandidatePage] = []
    seen_urls: set[str] = set()

    def add_candidate(url: str, source: str, title_hint: str | None = None) -> None:
        normalized_url = _normalize_space(url)
        if not normalized_url or normalized_url in seen_urls:
            return
        if not is_allowlisted_fandom_domain(normalized_url, allowlist=raw_allowlist):
            return
        seen_urls.add(normalized_url)
        title = _normalize_space(title_hint) or _extract_wiki_title_from_url(normalized_url)
        domain = _normalize_domain(normalized_url)
        score = _score_candidate(
            title=title,
            query_name=query_name,
            entity_kind=entity_kind,
            season_number=season_number,
        )
        candidates.append(
            FandomCandidatePage(
                url=normalized_url,
                title=title or normalized_url,
                source=source,
                domain=domain,
                score=score,
            )
        )

    for manual_url in manual_page_urls or ():
        add_candidate(str(manual_url), "manual")

    search_hits = search_allowlisted_fandom_wikis(
        query_name,
        allowlist=raw_allowlist,
        max_results=max(max_candidates * 2, 10),
    )
    for url in search_hits:
        add_candidate(url, "search")

    for domain in domains:
        direct = build_fandom_wiki_url_from_name(query_name, domain)
        if direct:
            add_candidate(direct, "direct")

    if include_allpages_scan:
        for domain in domains:
            titles = list_allpages_titles(domain, max_pages=max(1, allpages_max_pages))
            for title in titles:
                score = _score_candidate(
                    title=title,
                    query_name=query_name,
                    entity_kind=entity_kind,
                    season_number=season_number,
                )
                if score <= 0:
                    continue
                url = f"https://{domain}/wiki/{quote(title.replace(' ', '_'))}"
                add_candidate(url, "allpages", title_hint=title)

    candidates.sort(key=lambda item: item.score, reverse=True)
    return candidates[: max(1, max_candidates)]
