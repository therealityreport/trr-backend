"""Google News RSS parsing helpers for show news sync workflows."""

from __future__ import annotations

import html
import re
import xml.etree.ElementTree as ET
from collections.abc import Callable, Sequence
from datetime import UTC
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import parse_qsl, quote_plus, urlencode, urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup, Tag

_DEFAULT_HEADERS = {
    "accept": "application/rss+xml,application/xml;q=0.9,text/xml;q=0.8,*/*;q=0.5",
    "accept-language": "en-US,en;q=0.9",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
}
_DEFAULT_MAX_FEATURED_IMAGE_PROBES = 8
_DEFAULT_MAX_CANONICAL_URL_PROBES = 25
_TRACKING_QUERY_PREFIXES = ("utm_", "ga_", "fbclid", "gclid", "mc_", "igshid")
_TRACKING_QUERY_KEYS = {
    "oc",
    "ceid",
    "hl",
    "gl",
    "cmpid",
    "ref",
    "rss",
    "output",
}


def _http_url(value: str | None) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    if not cleaned:
        return None
    if cleaned.startswith("//"):
        cleaned = f"https:{cleaned}"
    parsed = urlparse(cleaned)
    if parsed.scheme.lower() not in {"http", "https"}:
        return None
    if not parsed.netloc:
        return None
    return cleaned


def normalize_article_url(value: str | None) -> str | None:
    resolved = _http_url(value)
    if not resolved:
        return None
    parsed = urlparse(resolved)
    scheme = parsed.scheme.lower()
    host = parsed.netloc.lower()
    if host.startswith("www."):
        host = host.removeprefix("www.")
    path = parsed.path or "/"
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")
    filtered_query = []
    for key, val in parse_qsl(parsed.query, keep_blank_values=False):
        lowered = key.lower()
        if lowered in _TRACKING_QUERY_KEYS:
            continue
        if any(lowered.startswith(prefix) for prefix in _TRACKING_QUERY_PREFIXES):
            continue
        filtered_query.append((key, val))
    filtered_query.sort(key=lambda part: (part[0], part[1]))
    normalized_query = urlencode(filtered_query, doseq=True)
    return urlunparse((scheme, host, path, "", normalized_query, ""))


def _local_name(tag: str) -> str:
    if "}" in tag:
        return tag.rsplit("}", 1)[-1]
    return tag


def _child_text(item: ET.Element, name: str) -> str | None:
    for child in list(item):
        if _local_name(child.tag) != name:
            continue
        text = (child.text or "").strip()
        if text:
            return text
    return None


def _strip_html(value: str | None) -> str | None:
    if not value:
        return None
    cleaned = re.sub(r"<[^>]+>", " ", value)
    cleaned = html.unescape(cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned or None


def _image_url_from_description(description_html: str | None, *, base_url: str) -> str | None:
    if not isinstance(description_html, str) or not description_html.strip():
        return None
    soup = BeautifulSoup(description_html, "html.parser")
    image_tag = soup.find("img")
    if not isinstance(image_tag, Tag):
        return None
    for attr in ("src", "data-src", "data-original"):
        value = image_tag.get(attr)
        if not isinstance(value, str) or not value.strip():
            continue
        resolved = _http_url(urljoin(base_url, value.strip()))
        if resolved:
            return resolved
    return None


def _parse_pubdate(pub_date: str | None) -> str | None:
    if not pub_date:
        return None
    try:
        parsed = parsedate_to_datetime(pub_date)
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _image_url_from_item(item: ET.Element) -> str | None:
    for child in list(item):
        local = _local_name(child.tag)
        if local == "enclosure":
            url = (child.attrib.get("url") or "").strip()
            content_type = (child.attrib.get("type") or "").strip().lower()
            if url and content_type.startswith("image/"):
                return url
        if local in {"content", "thumbnail"}:
            url = (child.attrib.get("url") or "").strip()
            medium = (child.attrib.get("medium") or "").strip().lower()
            content_type = (child.attrib.get("type") or "").strip().lower()
            if url and (medium == "image" or content_type.startswith("image/") or local == "thumbnail"):
                return url
    return None


def _meta_content(soup: BeautifulSoup, *, property_name: str | None = None, name: str | None = None) -> str | None:
    attrs: dict[str, Any] = {}
    if property_name:
        attrs["property"] = property_name
    if name:
        attrs["name"] = name
    if not attrs:
        return None
    tag = soup.find("meta", attrs=attrs)
    if isinstance(tag, Tag):
        content = tag.get("content")
        return str(content).strip() if isinstance(content, str) and content.strip() else None
    return None


def _extract_featured_image_from_html(html_text: str, *, page_url: str) -> str | None:
    soup = BeautifulSoup(html_text or "", "html.parser")
    candidates = [
        _meta_content(soup, property_name="og:image:secure_url"),
        _meta_content(soup, property_name="og:image"),
        _meta_content(soup, name="twitter:image"),
        _meta_content(soup, name="twitter:image:src"),
    ]
    image_link = soup.find("link", attrs={"rel": "image_src"})
    if isinstance(image_link, Tag):
        href = image_link.get("href")
        if isinstance(href, str) and href.strip():
            candidates.append(href.strip())
    first_image_tag = soup.find("img")
    if isinstance(first_image_tag, Tag):
        src = first_image_tag.get("src")
        if isinstance(src, str) and src.strip():
            candidates.append(src.strip())
    for raw in candidates:
        if not raw:
            continue
        absolute = _http_url(urljoin(page_url, raw))
        if absolute:
            return absolute
    return None


def _resolve_featured_image(article_url: str, *, timeout: float) -> str | None:
    response = requests.get(article_url, headers=_DEFAULT_HEADERS, timeout=timeout, allow_redirects=True)
    response.raise_for_status()
    page_url = str(response.url or article_url)
    return _extract_featured_image_from_html(response.text, page_url=page_url)


def _resolve_canonical_article_url(article_url: str, *, timeout: float) -> str | None:
    response = requests.get(article_url, headers=_DEFAULT_HEADERS, timeout=timeout, allow_redirects=True)
    response.raise_for_status()
    return normalize_article_url(str(response.url or article_url))


def _enrich_items_with_featured_images(
    items: list[dict[str, Any]],
    *,
    timeout: float,
    max_probes: int = 15,
    heartbeat_cb: Callable[[], None] | None = None,
) -> tuple[int, int, list[str]]:
    if max_probes <= 0:
        return (0, 0, [])
    filled = 0
    probes = 0
    errors: list[str] = []
    for item in items:
        if probes >= max_probes:
            break
        if heartbeat_cb:
            heartbeat_cb()
        image_url = _http_url(str(item.get("image_url") or "").strip() or None)
        if image_url:
            item["image_url"] = image_url
            continue
        article_url = _http_url(str(item.get("article_url") or "").strip() or None)
        if not article_url:
            continue
        probes += 1
        try:
            resolved = _resolve_featured_image(article_url, timeout=timeout)
        except requests.RequestException as exc:
            errors.append(f"{article_url}: {exc}")
            continue
        if not resolved:
            continue
        item["image_url"] = resolved
        filled += 1
    return (filled, probes, errors)


def _enrich_items_with_canonical_urls(
    items: list[dict[str, Any]],
    *,
    timeout: float,
    max_probes: int = _DEFAULT_MAX_CANONICAL_URL_PROBES,
    heartbeat_cb: Callable[[], None] | None = None,
) -> tuple[int, int, list[str]]:
    if max_probes <= 0:
        return (0, 0, [])
    resolved_count = 0
    probes = 0
    errors: list[str] = []
    for item in items:
        if heartbeat_cb:
            heartbeat_cb()
        article_url = _http_url(str(item.get("article_url") or "").strip() or None)
        if not article_url:
            continue
        normalized_existing = normalize_article_url(str(item.get("canonical_article_url") or "").strip() or None)
        if normalized_existing:
            item["canonical_article_url"] = normalized_existing
            continue
        normalized_article = normalize_article_url(article_url)
        if normalized_article and "news.google.com" not in normalized_article:
            item["canonical_article_url"] = normalized_article
            continue
        if probes >= max_probes:
            if normalized_article:
                item["canonical_article_url"] = normalized_article
            continue
        probes += 1
        try:
            canonical = _resolve_canonical_article_url(article_url, timeout=timeout)
        except requests.RequestException as exc:
            errors.append(f"{article_url}: {exc}")
            if normalized_article:
                item["canonical_article_url"] = normalized_article
            continue
        if canonical:
            item["canonical_article_url"] = canonical
            resolved_count += 1
            continue
        if normalized_article:
            item["canonical_article_url"] = normalized_article
    return (resolved_count, probes, errors)


def parse_rss_items(xml_text: str) -> list[dict[str, Any]]:
    payload = (xml_text or "").strip()
    if not payload:
        return []
    root = ET.fromstring(payload)
    channel = next((child for child in list(root) if _local_name(child.tag) == "channel"), None)
    if channel is None:
        return []

    out: list[dict[str, Any]] = []
    for index, item in enumerate(child for child in list(channel) if _local_name(child.tag) == "item"):
        title = _child_text(item, "title")
        link = _child_text(item, "link")
        if not link:
            continue

        source_name: str | None = None
        source_url: str | None = None
        for child in list(item):
            if _local_name(child.tag) != "source":
                continue
            source_name = (child.text or "").strip() or None
            source_url = (child.attrib.get("url") or "").strip() or None
            break

        publisher_domain = None
        if source_url:
            publisher_domain = urlparse(source_url).netloc.lower().removeprefix("www.")
        if not publisher_domain:
            publisher_domain = urlparse(link).netloc.lower().removeprefix("www.")
        if not publisher_domain:
            publisher_domain = None

        description_raw = _child_text(item, "description")
        description = _strip_html(description_raw)
        image_url = _http_url(_image_url_from_item(item))
        if not image_url:
            image_url = _image_url_from_description(description_raw, base_url=link.strip())

        out.append(
            {
                "headline": title,
                "article_url": link.strip(),
                "canonical_article_url": normalize_article_url(link.strip()),
                "published_at": _parse_pubdate(_child_text(item, "pubDate")),
                "publisher_name": source_name,
                "publisher_url": source_url,
                "publisher_domain": publisher_domain,
                "summary": description,
                "image_url": image_url,
                "feed_rank": index,
            }
        )
    return out


def topic_url_to_rss_candidates(topic_url: str) -> list[str]:
    raw_url = (topic_url or "").strip()
    if not raw_url:
        return []

    parsed = urlparse(raw_url)
    scheme = parsed.scheme or "https"
    netloc = parsed.netloc or "news.google.com"
    path = parsed.path or "/"
    query = parsed.query

    base = urlunparse((scheme, netloc, path, "", query, ""))
    candidates: list[str] = [base]

    rss_path = path
    if "/rss/" not in rss_path:
        if "/topics/" in rss_path:
            rss_path = rss_path.replace("/topics/", "/rss/topics/", 1)
        elif rss_path.startswith("/topics/"):
            rss_path = "/rss" + rss_path
        else:
            rss_path = "/rss" + (rss_path if rss_path.startswith("/") else f"/{rss_path}")
    rss_candidate = urlunparse((scheme, netloc, rss_path, "", query, ""))
    candidates.append(rss_candidate)

    deduped: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        normalized = candidate.strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(normalized)
    return deduped


def _build_search_query(show_name: str, show_aliases: Sequence[str] | None) -> str:
    terms: list[str] = []
    primary = (show_name or "").strip()
    if primary:
        terms.append(f'"{primary}"')
    for alias in show_aliases or []:
        cleaned = str(alias or "").strip()
        if not cleaned:
            continue
        if cleaned.lower() == primary.lower():
            continue
        terms.append(f'"{cleaned}"')
    if not terms:
        return "real housewives"
    if len(terms) == 1:
        return terms[0]
    return " OR ".join(terms[:6])


def build_search_rss_url(
    show_name: str,
    show_aliases: Sequence[str] | None = None,
    *,
    hl: str = "en-US",
    gl: str = "US",
    ceid: str = "US:en",
) -> str:
    query = _build_search_query(show_name, show_aliases)
    query_string = urlencode({"q": query, "hl": hl, "gl": gl, "ceid": ceid}, quote_via=quote_plus)
    return f"https://news.google.com/rss/search?{query_string}"


def _fetch_rss(url: str, *, timeout: float) -> str:
    response = requests.get(url, headers=_DEFAULT_HEADERS, timeout=timeout, allow_redirects=True)
    response.raise_for_status()
    return response.text


def fetch_google_news(
    *,
    topic_url: str,
    show_name: str,
    show_aliases: Sequence[str] | None = None,
    timeout: float = 20.0,
    max_featured_image_probes: int = _DEFAULT_MAX_FEATURED_IMAGE_PROBES,
    max_canonical_url_probes: int = _DEFAULT_MAX_CANONICAL_URL_PROBES,
    heartbeat_cb: Callable[[], None] | None = None,
) -> dict[str, Any]:
    topic_candidates = topic_url_to_rss_candidates(topic_url)
    attempted_feeds: list[str] = []
    errors: list[str] = []

    for candidate in topic_candidates:
        if heartbeat_cb:
            heartbeat_cb()
        attempted_feeds.append(candidate)
        try:
            items = parse_rss_items(_fetch_rss(candidate, timeout=timeout))
        except (ET.ParseError, requests.RequestException) as exc:
            errors.append(f"{candidate}: {exc}")
            continue
        if items:
            canonical_urls_resolved, canonical_urls_probed, canonical_url_errors = _enrich_items_with_canonical_urls(
                items,
                timeout=timeout,
                max_probes=max_canonical_url_probes,
                heartbeat_cb=heartbeat_cb,
            )
            featured_images_added, featured_images_probed, featured_image_errors = _enrich_items_with_featured_images(
                items,
                timeout=timeout,
                max_probes=max_featured_image_probes,
                heartbeat_cb=heartbeat_cb,
            )
            return {
                "items": items,
                "resolved_feed_url": candidate,
                "fallback_used": False,
                "attempted_feeds": attempted_feeds,
                "errors": errors,
                "featured_images_added": featured_images_added,
                "featured_images_probed": featured_images_probed,
                "featured_image_errors": featured_image_errors,
                "canonical_urls_resolved": canonical_urls_resolved,
                "canonical_urls_probed": canonical_urls_probed,
                "canonical_url_errors": canonical_url_errors,
            }

    fallback_url = build_search_rss_url(show_name, show_aliases)
    attempted_feeds.append(fallback_url)
    try:
        fallback_items = parse_rss_items(_fetch_rss(fallback_url, timeout=timeout))
    except (ET.ParseError, requests.RequestException) as exc:
        errors.append(f"{fallback_url}: {exc}")
        raise RuntimeError("Failed to fetch Google News RSS feed") from exc
    canonical_urls_resolved, canonical_urls_probed, canonical_url_errors = _enrich_items_with_canonical_urls(
        fallback_items,
        timeout=timeout,
        max_probes=max_canonical_url_probes,
        heartbeat_cb=heartbeat_cb,
    )
    featured_images_added, featured_images_probed, featured_image_errors = _enrich_items_with_featured_images(
        fallback_items,
        timeout=timeout,
        max_probes=max_featured_image_probes,
        heartbeat_cb=heartbeat_cb,
    )

    return {
        "items": fallback_items,
        "resolved_feed_url": fallback_url,
        "fallback_used": True,
        "attempted_feeds": attempted_feeds,
        "errors": errors,
        "featured_images_added": featured_images_added,
        "featured_images_probed": featured_images_probed,
        "featured_image_errors": featured_image_errors,
        "canonical_urls_resolved": canonical_urls_resolved,
        "canonical_urls_probed": canonical_urls_probed,
        "canonical_url_errors": canonical_url_errors,
    }
