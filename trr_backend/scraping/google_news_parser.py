"""Google News RSS parsing helpers for show news sync workflows."""

from __future__ import annotations

import html
import re
import xml.etree.ElementTree as ET
from collections.abc import Sequence
from datetime import UTC
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import quote_plus, urlencode, urlparse, urlunparse

import requests

_DEFAULT_HEADERS = {
    "accept": "application/rss+xml,application/xml;q=0.9,text/xml;q=0.8,*/*;q=0.5",
    "accept-language": "en-US,en;q=0.9",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
}


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

        description = _strip_html(_child_text(item, "description"))

        out.append(
            {
                "headline": title,
                "article_url": link.strip(),
                "published_at": _parse_pubdate(_child_text(item, "pubDate")),
                "publisher_name": source_name,
                "publisher_url": source_url,
                "publisher_domain": publisher_domain,
                "summary": description,
                "image_url": _image_url_from_item(item),
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
        terms.append(f"\"{primary}\"")
    for alias in show_aliases or []:
        cleaned = str(alias or "").strip()
        if not cleaned:
            continue
        if cleaned.lower() == primary.lower():
            continue
        terms.append(f"\"{cleaned}\"")
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
) -> dict[str, Any]:
    topic_candidates = topic_url_to_rss_candidates(topic_url)
    attempted_feeds: list[str] = []
    errors: list[str] = []

    for candidate in topic_candidates:
        attempted_feeds.append(candidate)
        try:
            items = parse_rss_items(_fetch_rss(candidate, timeout=timeout))
        except (ET.ParseError, requests.RequestException) as exc:
            errors.append(f"{candidate}: {exc}")
            continue
        if items:
            return {
                "items": items,
                "resolved_feed_url": candidate,
                "fallback_used": False,
                "attempted_feeds": attempted_feeds,
                "errors": errors,
            }

    fallback_url = build_search_rss_url(show_name, show_aliases)
    attempted_feeds.append(fallback_url)
    try:
        fallback_items = parse_rss_items(_fetch_rss(fallback_url, timeout=timeout))
    except (ET.ParseError, requests.RequestException) as exc:
        errors.append(f"{fallback_url}: {exc}")
        raise RuntimeError("Failed to fetch Google News RSS feed") from exc

    return {
        "items": fallback_items,
        "resolved_feed_url": fallback_url,
        "fallback_used": True,
        "attempted_feeds": attempted_feeds,
        "errors": errors,
    }
