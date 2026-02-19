"""BravoTV parsing helpers for show/person sync workflows."""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup, Tag

_DEFAULT_HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "accept-language": "en-US,en;q=0.9",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
}

_SOCIAL_HOSTS = {
    "instagram.com": "instagram",
    "www.instagram.com": "instagram",
    "x.com": "twitter",
    "www.x.com": "twitter",
    "twitter.com": "twitter",
    "www.twitter.com": "twitter",
    "facebook.com": "facebook",
    "www.facebook.com": "facebook",
    "tiktok.com": "tiktok",
    "www.tiktok.com": "tiktok",
    "youtube.com": "youtube",
    "www.youtube.com": "youtube",
    "youtu.be": "youtube",
}

_GLOBAL_SOCIAL_HANDLES = {
    "bravo",
    "bravotv",
    "bravocon",
    "nbc",
    "peacock",
    "watchwhathappenslive",
    "wwhl",
    "eonline",
}

_RUNTIME_RE = re.compile(r"(\b\d{1,2}:\d{2}\b|\b\d{1,3}\s*(?:min|mins|minutes?)\b)", re.IGNORECASE)
_SEASON_RE = re.compile(r"(?:\bseason\s*(\d{1,2})\b|\bs(\d{1,2})\b)", re.IGNORECASE)
_AIRS_RE = re.compile(r"\b(?:tune\s*in|airs?|watch\s+new|new\s+episodes?)\b", re.IGNORECASE)
_DAY_TIME_RE = re.compile(
    r"\b(?:mondays?|tuesdays?|wednesdays?|thursdays?|fridays?|saturdays?|sundays?)\b"
    r"(?:\s+at)?\s+\d{1,2}(?::\d{2})?(?:\s*/\s*\d{1,2}c)?\b",
    re.IGNORECASE,
)
_SHOW_ART_HINT_RE = re.compile(r"\b(?:key\s*art|poster|logo|hero|lead|cast)\b", re.IGNORECASE)
_MONTH_DATE_RE = re.compile(
    r"\b(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|"
    r"aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)"
    r"\.?\s+\d{1,2},\s+\d{4}\b",
    re.IGNORECASE,
)
_SHOW_TOKEN_STOPWORDS = {
    "bravo",
    "tv",
    "show",
    "watch",
    "news",
    "daily",
    "dish",
    "full",
    "episode",
    "episodes",
    "season",
}
_PEOPLE_NOT_FOUND_MESSAGE_RE = re.compile(
    r"sorry\s+we\s+couldn[’']?t\s+find\s+what\s+you\s+were\s+looking\s+for",
    re.IGNORECASE,
)


@dataclass
class BravoFetchResult:
    url: str
    html: str
    soup: BeautifulSoup


def _canonicalize_url(url: str, *, keep_page_query: bool = False) -> str:
    parsed = urlparse(url)
    query_items = parse_qsl(parsed.query, keep_blank_values=False)
    filtered: list[tuple[str, str]] = []
    for key, value in query_items:
        k = key.lower().strip()
        if keep_page_query and k == "page":
            filtered.append(("page", value))
            continue
        if k.startswith("utm_") or k in {"fbclid", "gclid", "mc_cid", "mc_eid", "output", "amp"}:
            continue
    query = urlencode(filtered)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path.rstrip("/"), "", query, ""))


def _extract_text(node: Tag | None) -> str | None:
    if not node:
        return None
    text = node.get_text(" ", strip=True)
    return text if text else None


def _first_non_empty(*values: str | None) -> str | None:
    for value in values:
        if isinstance(value, str):
            trimmed = value.strip()
            if trimmed:
                return trimmed
    return None


def _meta_content(soup: BeautifulSoup, *, property_name: str | None = None, name: str | None = None) -> str | None:
    attrs: dict[str, str] = {}
    if property_name:
        attrs["property"] = property_name
    if name:
        attrs["name"] = name
    if not attrs:
        return None
    tag = soup.find("meta", attrs=attrs)
    if isinstance(tag, Tag):
        return _first_non_empty(tag.get("content"))
    return None


def _fetch_html(url: str, *, timeout: float = 25.0) -> BravoFetchResult:
    response = requests.get(url, headers=_DEFAULT_HEADERS, timeout=timeout, allow_redirects=True)
    response.raise_for_status()
    html = response.text
    return BravoFetchResult(
        url=_canonicalize_url(response.url),
        html=html,
        soup=BeautifulSoup(html, "html.parser"),
    )


def _looks_like_person_url(url: str) -> bool:
    parsed = urlparse(url)
    path = parsed.path.lower()
    return "/people/" in path and not path.endswith("/photos")


def _extract_person_slug(url: str) -> str | None:
    parsed = urlparse(url)
    parts = [part for part in parsed.path.split("/") if part]
    for index, part in enumerate(parts):
        if part.lower() == "people" and index + 1 < len(parts):
            slug = parts[index + 1].strip().lower()
            return slug or None
    return None


def _url_path_has_video_or_news(url: str) -> bool:
    path = urlparse(url).path.lower()
    return any(segment in path for segment in ("/video", "/videos", "/watch", "/news", "/the-daily-dish"))


def _looks_like_non_show_image(url: str) -> bool:
    lowered = url.lower()
    return any(
        token in lowered
        for token in (
            "/styles/playlist_thumbnail/",
            "/media_mpx/thumbnails/",
            "/themes/custom/reality/images/",
            "/themes/custom/lifestyle/images/icons/",
            "/cast_head_shot_",
            "bravo-logo",
        )
    )


def _show_tokens(show_title: str | None) -> list[str]:
    if not isinstance(show_title, str):
        return []
    tokens = []
    for token in re.split(r"[^a-z0-9]+", show_title.lower()):
        if len(token) >= 4 and token not in _SHOW_TOKEN_STOPWORDS:
            tokens.append(token)
    return tokens


def _show_slug_from_url(show_url: str | None) -> str | None:
    if not isinstance(show_url, str):
        return None
    path_parts = [part for part in urlparse(show_url).path.split("/") if part]
    if not path_parts:
        return None
    slug = path_parts[0].strip().lower()
    return slug or None


def _person_relevance_phrases(person_urls: list[str] | None) -> list[str]:
    phrases: list[str] = []
    for person_url in person_urls or []:
        slug = _extract_person_slug(person_url)
        if not slug:
            continue
        phrase = slug.replace("-", " ").strip().lower()
        if len(phrase) < 5:
            continue
        phrases.append(phrase)
    # Preserve order while deduping.
    return list(dict.fromkeys(phrases))


def _is_show_relevant_news_item(
    item: dict[str, Any],
    *,
    show_url: str,
    show_title: str | None,
    person_urls: list[str] | None,
) -> bool:
    article_url = item.get("article_url") if isinstance(item.get("article_url"), str) else ""
    headline = item.get("headline") if isinstance(item.get("headline"), str) else ""
    path = urlparse(article_url).path.lower()
    searchable = f"{headline} {article_url}".lower()

    show_slug = _show_slug_from_url(show_url)
    if show_slug and (show_slug in path or show_slug in searchable):
        return True

    show_title_phrase = (
        " ".join(re.split(r"[^a-z0-9]+", show_title.lower())).strip()
        if isinstance(show_title, str)
        else ""
    )
    if show_title_phrase and len(show_title_phrase) >= 4 and show_title_phrase in searchable:
        return True

    for phrase in _person_relevance_phrases(person_urls):
        if phrase in searchable:
            return True

    show_tokens = _show_tokens(show_title)
    if len(show_tokens) >= 2:
        matched = sum(1 for token in show_tokens if token in searchable)
        if matched >= 2:
            return True

    return False


def _filter_show_relevant_news(
    items: list[dict[str, Any]],
    *,
    show_url: str,
    show_title: str | None,
    person_urls: list[str] | None,
) -> list[dict[str, Any]]:
    if not items:
        return []
    return [
        item
        for item in items
        if _is_show_relevant_news_item(
            item,
            show_url=show_url,
            show_title=show_title,
            person_urls=person_urls,
        )
    ]


def _looks_like_show_specific_image(
    *,
    url: str,
    alt: str | None,
    show_title: str | None,
) -> bool:
    searchable = f"{url.lower()} {(alt or '').lower()}"
    if _SHOW_ART_HINT_RE.search(searchable):
        return True
    for token in _show_tokens(show_title):
        if token in searchable:
            return True
    return False


def _collect_image_candidates(
    soup: BeautifulSoup,
    base_url: str,
    *,
    show_title: str | None = None,
    limit: int = 40,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[str] = set()

    # Prefer metadata hero/key-art images first.
    for meta_url in (
        _meta_content(soup, property_name="og:image"),
        _meta_content(soup, name="twitter:image"),
    ):
        if len(out) >= limit:
            break
        if not meta_url:
            continue
        resolved = _canonicalize_url(urljoin(base_url, meta_url))
        if not resolved or resolved in seen:
            continue
        seen.add(resolved)
        out.append(
            {
                "url": resolved,
                "alt": "Show image",
                "width": None,
                "height": None,
            }
        )

    for image in soup.find_all("img"):
        if len(out) >= limit:
            break
        src = _first_non_empty(
            image.get("data-src"),
            image.get("data-original"),
            image.get("src"),
        )
        if not src:
            continue
        resolved = _canonicalize_url(urljoin(base_url, src))
        if not resolved or resolved in seen:
            continue
        if _url_path_has_video_or_news(resolved):
            continue
        if _looks_like_non_show_image(resolved):
            continue

        parent_anchor = image.find_parent("a", href=True)
        if isinstance(parent_anchor, Tag):
            anchor_href = _first_non_empty(parent_anchor.get("href"))
            if anchor_href:
                anchor_url = _canonicalize_url(urljoin(base_url, anchor_href))
                if _url_path_has_video_or_news(anchor_url):
                    continue

        alt = _first_non_empty(image.get("alt"))
        if not _looks_like_show_specific_image(url=resolved, alt=alt, show_title=show_title):
            continue

        width = _first_non_empty(image.get("width"))
        height = _first_non_empty(image.get("height"))
        try:
            w_val = int(width) if isinstance(width, str) else None
            h_val = int(height) if isinstance(height, str) else None
        except ValueError:
            w_val = None
            h_val = None
        if (w_val is not None and w_val < 120) or (h_val is not None and h_val < 120):
            continue

        seen.add(resolved)
        out.append(
            {
                "url": resolved,
                "alt": alt,
                "width": width,
                "height": height,
            }
        )
    return out


def _collect_person_urls(soup: BeautifulSoup, base_url: str) -> list[str]:
    urls: set[str] = set()
    for anchor in soup.find_all("a", href=True):
        href = _first_non_empty(anchor.get("href"))
        if not href:
            continue
        resolved = _canonicalize_url(urljoin(base_url, href))
        if not _looks_like_person_url(resolved):
            continue
        urls.add(resolved)
    return sorted(urls)


def _merge_person_urls(*groups: Iterable[str]) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()
    for group in groups:
        for raw_url in group:
            if not isinstance(raw_url, str):
                continue
            normalized = _canonicalize_url(raw_url)
            if not _looks_like_person_url(normalized):
                continue
            if normalized in seen:
                continue
            seen.add(normalized)
            merged.append(normalized)
    return merged


def _derive_season_number(*parts: str | None) -> int | None:
    joined = " ".join([part for part in parts if isinstance(part, str)])
    match = _SEASON_RE.search(joined)
    if not match:
        return None
    for group in match.groups():
        if group and group.isdigit():
            return int(group)
    return None


def _extract_runtime(*parts: str | None) -> str | None:
    joined = " ".join([part for part in parts if isinstance(part, str)])
    match = _RUNTIME_RE.search(joined)
    if not match:
        return None
    return match.group(1).strip()


def _normalize_published_at(value: str | None) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    if not cleaned:
        return None

    # RFC3339/ISO variants.
    try:
        return datetime.fromisoformat(cleaned.replace("Z", "+00:00")).isoformat()
    except ValueError:
        pass

    # Month-name date labels often found in rendered cards.
    for fmt in ("%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(cleaned, fmt).date().isoformat()  # noqa: DTZ007
        except ValueError:
            continue

    return cleaned


def _extract_published_at_from_card(card: Tag | None) -> str | None:
    if not isinstance(card, Tag):
        return None

    time_node = card.find("time")
    if isinstance(time_node, Tag):
        from_datetime = _normalize_published_at(_first_non_empty(time_node.get("datetime")))
        if from_datetime:
            return from_datetime
        from_text = _normalize_published_at(_extract_text(time_node))
        if from_text:
            return from_text

    card_text = _extract_text(card)
    if isinstance(card_text, str):
        month_match = _MONTH_DATE_RE.search(card_text)
        if month_match:
            normalized = _normalize_published_at(month_match.group(0))
            if normalized:
                return normalized

    return None


def _extract_published_at_from_page(soup: BeautifulSoup, html: str) -> str | None:
    for prop in ("article:published_time", "article:modified_time", "og:updated_time"):
        candidate = _normalize_published_at(_meta_content(soup, property_name=prop))
        if candidate:
            return candidate

    for name in ("datePublished", "dateModified"):
        candidate = _normalize_published_at(_meta_content(soup, name=name))
        if candidate:
            return candidate

    for regex in (
        re.compile(r'"datePublished"\s*:\s*"([^"]+)"'),
        re.compile(r'"dateCreated"\s*:\s*"([^"]+)"'),
        re.compile(r'"dateModified"\s*:\s*"([^"]+)"'),
    ):
        match = regex.search(html)
        if not match:
            continue
        candidate = _normalize_published_at(match.group(1))
        if candidate:
            return candidate

    month_match = _MONTH_DATE_RE.search(html)
    if month_match:
        candidate = _normalize_published_at(month_match.group(0))
        if candidate:
            return candidate

    return None


def _hydrate_items_published_at(
    items: list[dict[str, Any]],
    *,
    url_key: str,
    max_lookups: int = 30,
) -> list[dict[str, Any]]:
    cache: dict[str, str | None] = {}
    lookups = 0

    for item in items:
        if item.get("published_at"):
            continue
        raw_url = item.get(url_key)
        if not isinstance(raw_url, str) or not raw_url.strip():
            continue
        url = raw_url.strip()

        if url in cache:
            item["published_at"] = cache[url]
            continue

        if lookups >= max_lookups:
            continue
        lookups += 1

        try:
            fetched = _fetch_html(url)
            published_at = _extract_published_at_from_page(fetched.soup, fetched.html)
        except requests.RequestException:
            published_at = None

        cache[url] = published_at
        item["published_at"] = published_at

    return items


def _collect_video_items(soup: BeautifulSoup, base_url: str) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    seen: set[str] = set()

    card_nodes = soup.select("article.teaser.teaser--watch.video")
    if not card_nodes:
        card_nodes = soup.select("article")

    for card in card_nodes:
        if not isinstance(card, Tag):
            continue
        anchor = card.find("a", href=True)
        if not isinstance(anchor, Tag):
            continue

        href = _first_non_empty(anchor.get("href"))
        if not href:
            continue
        resolved = _canonicalize_url(urljoin(base_url, href))
        path = urlparse(resolved).path.lower()
        if "/video" not in path and "/watch" not in path and "/videos" not in path:
            continue
        if resolved in seen:
            continue

        title = _first_non_empty(
            _extract_text(card.find(["h1", "h2", "h3", "h4"])),
            _extract_text(anchor),
            _first_non_empty(anchor.get("title")),
        )
        if not title:
            continue

        kicker = _first_non_empty(
            _extract_text(card.find(attrs={"class": re.compile("kicker|eyebrow", re.IGNORECASE)})),
            _extract_text(card.find("p")),
        )

        runtime = _extract_runtime(title, kicker, _extract_text(card))
        season_number = _derive_season_number(title, kicker, path.replace("-", " "))
        path_match = re.search(r"/season-(\d{1,2})/", path)
        if path_match and path_match.group(1).isdigit():
            season_number = int(path_match.group(1))

        image_url = None
        image_tag = card.find("img")
        if isinstance(image_tag, Tag):
            src = _first_non_empty(image_tag.get("data-src"), image_tag.get("src"), image_tag.get("srcset"))
            if src:
                image_url = _canonicalize_url(urljoin(base_url, src.split(" ")[0]))

        seen.add(resolved)
        items.append(
            {
                "title": title,
                "runtime": runtime,
                "kicker": kicker,
                "image_url": image_url,
                "clip_url": resolved,
                "season_number": season_number,
                "published_at": _extract_published_at_from_card(card),
            }
        )

    if items:
        return items

    # Fallback for simpler markup variants without teaser article wrappers.
    for anchor in soup.select("a[href]"):
        href = _first_non_empty(anchor.get("href"))
        if not href:
            continue
        resolved = _canonicalize_url(urljoin(base_url, href))
        path = urlparse(resolved).path.lower()
        if "/video" not in path and "/watch" not in path and "/videos" not in path:
            continue
        if resolved in seen:
            continue

        card = anchor.find_parent(["article", "li", "div"]) or anchor
        title = _first_non_empty(
            _extract_text(card.find(["h1", "h2", "h3", "h4"]) if isinstance(card, Tag) else None),
            _extract_text(anchor),
            _first_non_empty(anchor.get("title")),
        )
        if not title:
            continue

        kicker = None
        if isinstance(card, Tag):
            kicker = _first_non_empty(
                _extract_text(card.find(attrs={"class": re.compile("kicker|eyebrow", re.IGNORECASE)})),
                _extract_text(card.find("p")),
            )

        runtime = _extract_runtime(title, kicker, _extract_text(card) if isinstance(card, Tag) else None)
        season_number = _derive_season_number(title, kicker, path.replace("-", " "))

        image_url = None
        image_tag = card.find("img") if isinstance(card, Tag) else None
        if isinstance(image_tag, Tag):
            src = _first_non_empty(image_tag.get("data-src"), image_tag.get("src"))
            if src:
                image_url = _canonicalize_url(urljoin(base_url, src))

        seen.add(resolved)
        items.append(
            {
                "title": title,
                "runtime": runtime,
                "kicker": kicker,
                "image_url": image_url,
                "clip_url": resolved,
                "season_number": season_number,
                "published_at": _extract_published_at_from_card(card if isinstance(card, Tag) else None),
            }
        )
    return items


def _collect_news_items(soup: BeautifulSoup, base_url: str) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    seen: set[str] = set()

    card_nodes = soup.select("article")
    for card in card_nodes:
        if not isinstance(card, Tag):
            continue
        anchor = card.find("a", href=True)
        if not isinstance(anchor, Tag):
            continue

        href = _first_non_empty(anchor.get("href"))
        if not href:
            continue
        resolved = _canonicalize_url(urljoin(base_url, href))
        path = urlparse(resolved).path.lower()
        if "/news" not in path and "/the-daily-dish" not in path:
            continue
        if resolved in seen:
            continue

        headline = _first_non_empty(
            _extract_text(card.find(["h1", "h2", "h3", "h4"])),
            _extract_text(anchor),
            _first_non_empty(anchor.get("title")),
        )
        if not headline or len(headline) < 8:
            continue

        image_url = None
        image_tag = card.find("img")
        if isinstance(image_tag, Tag):
            src = _first_non_empty(image_tag.get("data-src"), image_tag.get("src"), image_tag.get("srcset"))
            if src:
                image_url = _canonicalize_url(urljoin(base_url, src.split(" ")[0]))

        seen.add(resolved)
        items.append(
            {
                "headline": headline,
                "image_url": image_url,
                "article_url": resolved,
                "published_at": _extract_published_at_from_card(card),
            }
        )

    if items:
        return items

    for anchor in soup.select("a[href]"):
        href = _first_non_empty(anchor.get("href"))
        if not href:
            continue
        resolved = _canonicalize_url(urljoin(base_url, href))
        path = urlparse(resolved).path.lower()
        if "/news" not in path and "/the-daily-dish" not in path:
            continue
        if resolved in seen:
            continue

        card = anchor.find_parent(["article", "li", "div"]) or anchor
        headline = _first_non_empty(
            _extract_text(card.find(["h1", "h2", "h3", "h4"]) if isinstance(card, Tag) else None),
            _extract_text(anchor),
            _first_non_empty(anchor.get("title")),
        )
        if not headline or len(headline) < 8:
            continue

        image_url = None
        image_tag = card.find("img") if isinstance(card, Tag) else None
        if isinstance(image_tag, Tag):
            src = _first_non_empty(image_tag.get("data-src"), image_tag.get("src"))
            if src:
                image_url = _canonicalize_url(urljoin(base_url, src))

        seen.add(resolved)
        items.append(
            {
                "headline": headline,
                "image_url": image_url,
                "article_url": resolved,
                "published_at": _extract_published_at_from_card(card if isinstance(card, Tag) else None),
            }
        )
    return items


def _extract_airs_text(soup: BeautifulSoup) -> str | None:
    candidates: list[tuple[int, str]] = []
    seen: set[str] = set()
    for node in soup.select("p,span,div"):
        text = _extract_text(node)
        if not text:
            continue
        if len(text) > 220:
            continue
        day_match = _DAY_TIME_RE.search(text)
        if day_match:
            candidate = day_match.group(0).strip()
            if candidate and candidate.lower() not in seen:
                seen.add(candidate.lower())
                candidates.append((2, candidate))
            continue
        if _AIRS_RE.search(text):
            candidate = text.strip()
            if candidate and candidate.lower() not in seen:
                seen.add(candidate.lower())
                candidates.append((1, candidate))
    if not candidates:
        return None
    candidates.sort(key=lambda row: (-row[0], len(row[1])))
    return candidates[0][1]


def _is_missing_person_page(
    soup: BeautifulSoup,
    *,
    page_title: str | None,
) -> bool:
    lowered_title = (page_title or "").lower()
    if "page not found" in lowered_title:
        return True

    hero = _extract_text(soup.find("h1"))
    if isinstance(hero, str) and "page not found" in hero.lower():
        return True

    body_text = soup.get_text(" ", strip=True)
    return bool(_PEOPLE_NOT_FOUND_MESSAGE_RE.search(body_text))


def _extract_social_links(soup: BeautifulSoup, base_url: str) -> dict[str, str]:
    social: dict[str, str] = {}

    for anchor in soup.select("a[href]"):
        href = _first_non_empty(anchor.get("href"))
        if not href:
            continue
        resolved = _canonicalize_url(urljoin(base_url, href))
        parsed = urlparse(resolved)
        platform = _SOCIAL_HOSTS.get(parsed.netloc.lower())
        if not platform:
            continue

        path_parts = [part for part in parsed.path.split("/") if part]
        handle = path_parts[0].lstrip("@") if path_parts else ""
        handle = handle.strip().lower()
        if not handle or handle in _GLOBAL_SOCIAL_HANDLES:
            continue

        if platform not in social:
            social[platform] = handle

    return social


def _dedupe_by_key(items: Iterable[dict[str, Any]], key: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in items:
        value = item.get(key)
        if not isinstance(value, str):
            continue
        normalized = _canonicalize_url(value)
        if normalized in seen:
            continue
        seen.add(normalized)
        item[key] = normalized
        out.append(item)
    return out


def parse_show_page(show_url: str) -> dict[str, Any]:
    fetched = _fetch_html(show_url)
    soup = fetched.soup

    title = _first_non_empty(
        _meta_content(soup, property_name="og:title"),
        _extract_text(soup.find("h1")),
        _extract_text(soup.find("title")),
    )
    description = _first_non_empty(
        _meta_content(soup, property_name="og:description"),
        _meta_content(soup, name="description"),
        _extract_text(soup.find("p")),
    )

    return {
        "canonical_url": fetched.url,
        "title": title,
        "description": description,
        "airs_text": _extract_airs_text(soup),
        "person_urls": _collect_person_urls(soup, fetched.url),
        "image_candidates": _collect_image_candidates(soup, fetched.url, show_title=title),
    }


def parse_show_videos(show_url: str, *, max_pages: int = 6) -> list[dict[str, Any]]:
    base = _canonicalize_url(show_url)
    parsed = urlparse(base)

    candidate_paths = [
        f"{parsed.path.rstrip('/')}/watch/videos",
        f"{parsed.path.rstrip('/')}/videos",
        f"{parsed.path.rstrip('/')}/watch",
    ]

    all_items: list[dict[str, Any]] = []
    for path in candidate_paths:
        if not path or path == "/":
            continue
        root_url = _canonicalize_url(urlunparse((parsed.scheme, parsed.netloc, path, "", "", "")))
        had_items = False
        for page in range(1, max_pages + 1):
            page_url = root_url if page == 1 else f"{root_url}?page={page}"
            try:
                fetched = _fetch_html(page_url)
            except requests.RequestException:
                break
            items = _collect_video_items(fetched.soup, fetched.url)
            if not items:
                if page == 1:
                    continue
                break
            had_items = True
            all_items.extend(items)
        if had_items:
            break

    deduped = _dedupe_by_key(all_items, "clip_url")
    return _hydrate_items_published_at(deduped, url_key="clip_url")


def parse_show_news(
    show_url: str,
    *,
    max_pages: int = 4,
    show_title: str | None = None,
    person_urls: list[str] | None = None,
) -> list[dict[str, Any]]:
    base = _canonicalize_url(show_url)
    parsed = urlparse(base)
    root = _canonicalize_url(urlunparse((parsed.scheme, parsed.netloc, f"{parsed.path.rstrip('/')}/news", "", "", "")))

    all_items: list[dict[str, Any]] = []
    for page in range(1, max_pages + 1):
        page_url = root if page == 1 else f"{root}?page={page}"
        try:
            fetched = _fetch_html(page_url)
        except requests.RequestException:
            if page == 1:
                return []
            break
        items = _collect_news_items(fetched.soup, fetched.url)
        if not items:
            break
        all_items.extend(items)

    deduped = _dedupe_by_key(all_items, "article_url")
    hydrated = _hydrate_items_published_at(deduped, url_key="article_url")
    return _filter_show_relevant_news(
        hydrated,
        show_url=show_url,
        show_title=show_title,
        person_urls=person_urls,
    )


def parse_person_page(person_url: str) -> dict[str, Any]:
    fetched = _fetch_html(person_url)
    soup = fetched.soup

    title = _first_non_empty(
        _meta_content(soup, property_name="og:title"),
        _extract_text(soup.find("h1")),
        _extract_text(soup.find("title")),
    )
    bio = _first_non_empty(
        _meta_content(soup, property_name="og:description"),
        _meta_content(soup, name="description"),
    )

    if _is_missing_person_page(soup, page_title=title):
        raise requests.RequestException(f"Bravo person page not found: {person_url}")

    if not bio:
        for node in soup.select("main p, article p, section p"):
            text = _extract_text(node)
            if text and len(text) > 60:
                bio = text
                break

    hero_image = _first_non_empty(
        _meta_content(soup, property_name="og:image"),
        _meta_content(soup, name="twitter:image"),
    )
    if hero_image:
        hero_image = _canonicalize_url(urljoin(fetched.url, hero_image))

    videos = _hydrate_items_published_at(_collect_video_items(soup, fetched.url), url_key="clip_url")
    news = _hydrate_items_published_at(_collect_news_items(soup, fetched.url), url_key="article_url")

    return {
        "canonical_url": fetched.url,
        "slug": _extract_person_slug(fetched.url),
        "name": title,
        "bio": bio,
        "hero_image_url": hero_image,
        "social_links": _extract_social_links(soup, fetched.url),
        "videos": _dedupe_by_key(videos, "clip_url"),
        "news": _dedupe_by_key(news, "article_url"),
    }


def _candidate_result(url: str, *, status: str, error: str | None = None) -> dict[str, Any]:
    result: dict[str, Any] = {
        "url": _canonicalize_url(url),
        "status": status,
    }
    if error:
        result["error"] = error
    return result


def parse_bravo_show_bundle(
    show_url: str,
    *,
    include_people: bool = True,
    include_videos: bool = True,
    include_news: bool = True,
    person_url_candidates: list[str] | None = None,
    max_people: int = 40,
) -> dict[str, Any]:
    show = parse_show_page(show_url)
    discovered_person_urls = list(show.get("person_urls") or [])
    candidate_person_urls = _merge_person_urls(
        person_url_candidates or [],
        discovered_person_urls,
    )

    videos = parse_show_videos(show["canonical_url"]) if include_videos else []
    news = (
        parse_show_news(
            show["canonical_url"],
            show_title=show.get("title"),
            person_urls=candidate_person_urls,
        )
        if include_news
        else []
    )

    people: list[dict[str, Any]] = []
    person_candidate_results: list[dict[str, Any]] = []
    if include_people:
        for person_url in candidate_person_urls[:max_people]:
            try:
                person = parse_person_page(person_url)
                people.append(person)
                person_candidate_results.append(
                    _candidate_result(
                        str(person.get("canonical_url") or person_url),
                        status="ok",
                    )
                )
            except requests.RequestException as exc:
                error_text = str(exc).strip()
                if "not found" in error_text.lower():
                    person_candidate_results.append(_candidate_result(person_url, status="missing"))
                else:
                    person_candidate_results.append(
                        _candidate_result(person_url, status="error", error=error_text or "request_failed")
                    )
                continue

    resolved_person_urls = _merge_person_urls(
        [
            str(person.get("canonical_url") or "").strip()
            for person in people
            if isinstance(person, dict)
        ]
    )
    if include_people:
        discovered_person_urls = resolved_person_urls
    else:
        discovered_person_urls = candidate_person_urls

    return {
        "show": {
            "canonical_url": show.get("canonical_url"),
            "title": show.get("title"),
            "description": show.get("description"),
            "airs_text": show.get("airs_text"),
        },
        "image_candidates": show.get("image_candidates") or [],
        "discovered_person_urls": discovered_person_urls,
        "person_candidate_results": person_candidate_results,
        "videos": videos,
        "news": news,
        "people": people,
        "raw": {
            "show": show,
            "videos": videos,
            "news": news,
            "people": people,
        },
    }
