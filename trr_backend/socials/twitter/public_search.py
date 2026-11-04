"""Explicit, small public-search reads; never a scheduled or bulk fallback.

Lurkbird permits personal use but prohibits automated bulk scraping. Keep this
adapter opt-in, capped at two pages/40 records, with no retry or cookie state.
Its upstream authentication, completeness and freshness are unknown.
"""

from __future__ import annotations

import hashlib
import re
import time
from datetime import UTC, datetime
from urllib.parse import parse_qs, quote, unquote, urlencode, urljoin, urlsplit

import requests
from bs4 import BeautifulSoup

_ORIGIN = "https://lurkbird.com"
_MAX_BYTES = 2_000_000


def _fetch(url: str) -> bytes:
    with requests.Session() as session:
        session.trust_env = False
        with session.get(
            url,
            timeout=(10, 20),
            allow_redirects=False,
            stream=True,
            headers={"Accept": "text/html", "User-Agent": "TRR-public-search-research/1.0"},
        ) as response:
            if response.status_code != 200:
                raise ValueError(f"Public search HTTP {response.status_code}")
            body = bytearray()
            deadline = time.monotonic() + 20
            for chunk in response.iter_content(65536):
                body.extend(chunk)
                if len(body) > _MAX_BYTES or time.monotonic() > deadline:
                    raise ValueError("Public search response exceeded limits")
            return bytes(body)


def _parse(body: bytes, *, url: str) -> tuple[list[dict], str | None]:
    soup = BeautifulSoup(body, "html.parser")
    feed = soup.select_one("ol.feed")
    if feed is None:
        raise ValueError("Public search feed unavailable; blocked or unsupported page")
    posts = []
    for article in feed.select("article.post"):
        post_id = str(article.get("id", "")).removeprefix("post-")
        text, author, date = (
            article.select_one(".post__text"),
            article.select_one(".post__handle"),
            article.select_one("time"),
        )
        username = author.get_text(strip=True).lstrip("@") if author else ""
        timestamp = date.get("datetime") if date else None
        if not re.fullmatch(r"[0-9]{2,20}", post_id) or not re.fullmatch(r"[A-Za-z0-9_]{1,15}", username):
            raise ValueError("Public search record identity invalid")
        if text is None or not timestamp:
            raise ValueError("Public search record text or timestamp missing")
        datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        posts.append(
            {
                "tweet_id": post_id,
                "username": username,
                "date_time": timestamp,
                "text": text.get_text(),
                "url": f"https://x.com/{username}/status/{post_id}",
                "text_scope": "mirror display text; may omit media links or truncate long posts",
            }
        )
    link = feed.select_one("a.load-more[href]")
    next_url = urljoin(url, str(link["href"])) if link else None
    if next_url:
        parsed, current = urlsplit(next_url), urlsplit(url)
        if (parsed.scheme, parsed.netloc, unquote(parsed.path)) != ("https", "lurkbird.com", unquote(current.path)):
            raise ValueError("Public search continuation left the query or provider")
        params = parse_qs(parsed.query)
        if not params.get("cursor") or len(next_url) > 10000:
            raise ValueError("Invalid public search continuation")
        if params.get("product") != parse_qs(current.query).get("product"):
            raise ValueError("Public search continuation changed sort")
    return posts, next_url


def search_public_posts(
    query: str, *, provider: str, sort: str = "latest", max_pages: int = 1, max_items: int = 20
) -> dict:
    """Small explicit research query; no global completeness or rank guarantees."""
    if provider != "lurkbird":
        raise ValueError("Explicit provider='lurkbird' is required")
    if not isinstance(query, str) or not query.strip() or len(query) > 300:
        raise ValueError("query must contain 1-300 characters")
    if sort not in {"latest", "top"}:
        raise ValueError("sort must be latest or top")
    if type(max_pages) is not int or not 1 <= max_pages <= 2:
        raise ValueError("max_pages must be 1-2; bulk collection is unsupported")
    if type(max_items) is not int or not 1 <= max_items <= 40:
        raise ValueError("max_items must be 1-40; bulk collection is unsupported")
    url = f"{_ORIGIN}/search/{quote(query.strip(), safe='')}/?{urlencode({'product': sort})}"
    result = {
        "provider": provider,
        "query": query,
        "sort": sort,
        "posts": [],
        "pages": [],
        "complete": False,
        "coverage": "provider-selected subset; global coverage and freshness unknown",
        "upstream_authentication": "unknown",
        "authentication_provided": False,
        "external_dependency": True,
        "usage": "explicit bounded research only; provider prohibits automated bulk scraping",
        "status": "partial",
        "stop_reason": "max_pages",
        "error": None,
    }
    seen, visited = set(), set()
    deadline = time.monotonic() + 120
    for _ in range(max_pages):
        if time.monotonic() >= deadline:
            result["stop_reason"] = "deadline"
            break
        if url in visited:
            result["stop_reason"] = "repeated_cursor"
            break
        visited.add(url)
        try:
            body = _fetch(url)
            posts, next_url = _parse(body, url=url)
            added, truncated = 0, False
            for post in posts:
                if post["tweet_id"] in seen:
                    continue
                if len(result["posts"]) >= max_items:
                    truncated = True
                    break
                result["posts"].append(post)
                seen.add(post["tweet_id"])
                added += 1
            result["pages"].append(
                {
                    "url": url,
                    "retrieved_at": datetime.now(UTC).isoformat(),
                    "sha256": hashlib.sha256(body).hexdigest(),
                    "added": added,
                }
            )
            if not posts:
                result["stop_reason"] = "empty_unverified"
                break
            if truncated or (len(result["posts"]) >= max_items and next_url):
                result["stop_reason"] = "max_items"
                break
            if not next_url:
                result["stop_reason"] = "no_continuation"
                break
            if not added:
                result["stop_reason"] = "repeated_page"
                break
            url = next_url
        except (ValueError, requests.RequestException) as exc:
            result["stop_reason"] = "error"
            result["error"] = str(exc) if isinstance(exc, ValueError) else "Public search request failed"
            break
    if not result["posts"]:
        result["status"] = "unavailable"
    return result
