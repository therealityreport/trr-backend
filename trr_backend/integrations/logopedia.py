from __future__ import annotations

import os
import time
from collections.abc import Mapping
from typing import Any
from urllib.parse import quote

import requests

LOGOPEDIA_API_URL = "https://logos.fandom.com/api.php"
DEFAULT_CONNECT_TIMEOUT_SECONDS = 5.0
DEFAULT_READ_TIMEOUT_SECONDS = 20.0
DEFAULT_RETRY_ATTEMPTS = 2
DEFAULT_RETRY_BACKOFF_MS = 300


class LogopediaError(RuntimeError):
    """Base logopedia lookup error."""


class LogopediaNoFilesError(LogopediaError):
    """Raised when no logo-like files are found."""


class LogopediaRequestError(LogopediaError):
    """Raised for network/request failures."""


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _timeout_tuple(timeout_seconds: float | None = None) -> tuple[float, float]:
    read_timeout = DEFAULT_READ_TIMEOUT_SECONDS
    if isinstance(timeout_seconds, (int, float)) and timeout_seconds > 0:
        read_timeout = float(timeout_seconds)
    else:
        raw = _normalize_text(os.getenv("LOGOPEDIA_TIMEOUT_SEC"))
        if raw:
            try:
                parsed = float(raw)
                if parsed > 0:
                    read_timeout = parsed
            except ValueError:
                pass
    connect_timeout = min(DEFAULT_CONNECT_TIMEOUT_SECONDS, read_timeout)
    return connect_timeout, read_timeout


def _retry_attempts() -> int:
    raw = _normalize_text(os.getenv("LOGOPEDIA_RETRY_ATTEMPTS"))
    if raw:
        try:
            parsed = int(raw)
            if parsed > 0:
                return min(parsed, 5)
        except ValueError:
            pass
    return DEFAULT_RETRY_ATTEMPTS


def _retry_backoff_ms() -> int:
    raw = _normalize_text(os.getenv("LOGOPEDIA_RETRY_BACKOFF_MS"))
    if raw:
        try:
            parsed = int(raw)
            if parsed >= 0:
                return min(parsed, 5_000)
        except ValueError:
            pass
    return DEFAULT_RETRY_BACKOFF_MS


def _is_retryable_status(status_code: int) -> bool:
    return status_code == 429 or status_code >= 500


def _fetch_json_with_retry(
    *,
    session: requests.Session,
    params: dict[str, Any],
    timeout_seconds: float | None,
) -> Mapping[str, Any]:
    timeout = _timeout_tuple(timeout_seconds)
    retry_attempts = _retry_attempts()
    retry_backoff_ms = _retry_backoff_ms()

    response: requests.Response | None = None
    last_error: Exception | None = None
    for attempt in range(retry_attempts):
        try:
            response = session.get(
                LOGOPEDIA_API_URL,
                params=params,
                headers={"accept": "application/json", "user-agent": "TRR-Backend/1.0"},
                timeout=timeout,
            )
        except (requests.ConnectTimeout, requests.ReadTimeout, requests.Timeout) as exc:
            last_error = LogopediaRequestError("logopedia_timeout")
            if attempt + 1 < retry_attempts:
                if retry_backoff_ms > 0:
                    time.sleep(retry_backoff_ms / 1000)
                continue
            raise last_error from exc
        except requests.RequestException as exc:
            last_error = LogopediaRequestError("logopedia_request_failed")
            if attempt + 1 < retry_attempts:
                if retry_backoff_ms > 0:
                    time.sleep(retry_backoff_ms / 1000)
                continue
            raise last_error from exc

        if _is_retryable_status(response.status_code):
            if attempt + 1 < retry_attempts:
                if retry_backoff_ms > 0:
                    time.sleep(retry_backoff_ms / 1000)
                continue
            raise LogopediaRequestError(f"logopedia_http_{response.status_code}")
        if response.status_code >= 400:
            raise LogopediaRequestError(f"logopedia_http_{response.status_code}")
        break

    if response is None:
        if isinstance(last_error, LogopediaRequestError):
            raise last_error
        raise LogopediaRequestError("logopedia_request_failed")

    try:
        payload = response.json()
    except ValueError as exc:
        raise LogopediaRequestError("logopedia_invalid_json") from exc
    if not isinstance(payload, Mapping):
        raise LogopediaRequestError("logopedia_invalid_json")
    return payload


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
    payload = _fetch_json_with_retry(
        session=session,
        params={
            "action": "query",
            "format": "json",
            "list": "search",
            "srsearch": name,
            "srlimit": 5,
        },
        timeout_seconds=timeout_seconds,
    )

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
        payload = _fetch_json_with_retry(
            session=session,
            params={
                "action": "query",
                "format": "json",
                "prop": "imageinfo",
                "generator": "images",
                "titles": title,
                "gimlimit": 50,
                "iiprop": "url|size|mime",
            },
            timeout_seconds=timeout_seconds,
        )

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
