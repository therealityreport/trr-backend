from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from urllib.parse import quote

import requests

LOGOPEDIA_API_URL = "https://logos.fandom.com/api.php"


class LogopediaError(RuntimeError):
    """Base logopedia lookup error."""


class LogopediaNoFilesError(LogopediaError):
    """Raised when no logo-like files are found."""


class LogopediaRequestError(LogopediaError):
    """Raised for network/request failures."""


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _name_to_slug(name: str) -> str:
    cleaned = _normalize_text(name).replace(" ", "_")
    return quote(cleaned, safe="()_-")


def _candidate_titles(name: str, aliases: list[str] | None = None) -> list[str]:
    aliases = aliases or []
    base = [name, *aliases]
    out: list[str] = []
    seen: set[str] = set()
    for raw in base:
        text = _normalize_text(raw)
        if not text:
            continue
        variants = {
            text,
            f"{text} (United States)",
            f"{text} (US)",
            text.replace("&", "and"),
        }
        for variant in variants:
            key = variant.casefold()
            if key in seen:
                continue
            seen.add(key)
            out.append(variant)
    return out


def _extract_image_candidates(page: Mapping[str, Any]) -> list[str]:
    candidates: list[tuple[tuple[int, int], str]] = []
    for image in page.get("imageinfo") or []:
        if not isinstance(image, Mapping):
            continue
        url = _normalize_text(image.get("url"))
        if not url:
            continue
        lower = url.lower()
        # Prefer logo-ish assets and transparent/vector-friendly formats.
        rank = 100
        if "logo" in lower or "wordmark" in lower:
            rank -= 15
        if ".svg" in lower:
            rank -= 10
        elif ".png" in lower:
            rank -= 5
        elif ".jpg" in lower or ".jpeg" in lower or ".webp" in lower:
            rank += 5
        size = int(image.get("size") or 0) if isinstance(image.get("size"), int) else 0
        candidates.append(((rank, -size), url))

    candidates.sort(key=lambda item: item[0])
    seen: set[str] = set()
    ordered: list[str] = []
    for _, url in candidates:
        if url in seen:
            continue
        seen.add(url)
        ordered.append(url)
    return ordered


def _fetch_titles_via_search(
    name: str,
    *,
    timeout_seconds: float,
    session: requests.Session,
) -> list[str]:
    try:
        response = session.get(
            LOGOPEDIA_API_URL,
            params={
                "action": "query",
                "format": "json",
                "list": "search",
                "srsearch": name,
                "srlimit": 5,
            },
            headers={"accept": "application/json", "user-agent": "TRR-Backend/1.0"},
            timeout=timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()
    except requests.RequestException as exc:
        raise LogopediaRequestError(f"logopedia_request_failed: {exc}") from exc
    except ValueError as exc:
        raise LogopediaRequestError("logopedia_invalid_json") from exc

    query = payload.get("query") if isinstance(payload, Mapping) else None
    rows = query.get("search") if isinstance(query, Mapping) else None
    if not isinstance(rows, list):
        return []

    out: list[str] = []
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        title = _normalize_text(row.get("title"))
        if title:
            out.append(title)
    return out


def fetch_logopedia_logo_candidates(
    name: str,
    *,
    aliases: list[str] | None = None,
    timeout_seconds: float = 20.0,
    session: requests.Session | None = None,
) -> list[str]:
    session = session or requests.Session()

    titles = _candidate_titles(name, aliases)
    titles.extend(_fetch_titles_via_search(name, timeout_seconds=timeout_seconds, session=session))

    seen_titles: set[str] = set()
    ordered_titles: list[str] = []
    for title in titles:
        key = title.casefold()
        if key in seen_titles:
            continue
        seen_titles.add(key)
        ordered_titles.append(title)

    all_candidates: list[str] = []
    seen_urls: set[str] = set()

    for title in ordered_titles:
        try:
            response = session.get(
                LOGOPEDIA_API_URL,
                params={
                    "action": "query",
                    "format": "json",
                    "prop": "imageinfo",
                    "generator": "images",
                    "titles": title,
                    "gimlimit": 50,
                    "iiprop": "url|size|mime",
                },
                headers={"accept": "application/json", "user-agent": "TRR-Backend/1.0"},
                timeout=timeout_seconds,
            )
            response.raise_for_status()
            payload = response.json()
        except requests.RequestException as exc:
            raise LogopediaRequestError(f"logopedia_request_failed: {exc}") from exc
        except ValueError as exc:
            raise LogopediaRequestError("logopedia_invalid_json") from exc

        query = payload.get("query") if isinstance(payload, Mapping) else None
        pages = query.get("pages") if isinstance(query, Mapping) else None
        if not isinstance(pages, Mapping):
            continue

        page_candidates: list[str] = []
        for page in pages.values():
            if not isinstance(page, Mapping):
                continue
            page_candidates.extend(_extract_image_candidates(page))

        for url in page_candidates:
            if url in seen_urls:
                continue
            seen_urls.add(url)
            all_candidates.append(url)

    if not all_candidates:
        raise LogopediaNoFilesError("logopedia_no_files")

    return all_candidates
