from __future__ import annotations

import os
import random
import re
import time
from typing import Any, Mapping

import requests

TMDB_API_BASE_URL = "https://api.themoviedb.org/3"


class TmdbClientError(RuntimeError):
    def __init__(self, message: str, *, status_code: int | None = None, body_snippet: str | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.body_snippet = body_snippet


def parse_tmdb_list_id(value: str | int) -> int:
    if isinstance(value, int):
        return value

    raw = str(value).strip()
    if not raw:
        raise ValueError("TMDb list value is empty.")

    if raw.isdigit():
        return int(raw)

    # Examples:
    # - https://www.themoviedb.org/list/8301263
    # - https://www.themoviedb.org/list/8301263?language=en-US
    match = re.search(r"/list/([0-9]+)", raw)
    if match:
        return int(match.group(1))

    raise ValueError(f"Unable to parse TMDb list id from: {value!r}")


def _require_api_key(api_key: str | None) -> str:
    resolved = (api_key or os.getenv("TMDB_API_KEY") or "").strip()
    if not resolved:
        raise RuntimeError("TMDB_API_KEY is not set.")
    return resolved


def resolve_api_key(api_key: str | None = None) -> str | None:
    """
    Best-effort API key resolution for callers that want to continue when the key is missing.
    """

    resolved = (api_key or os.getenv("TMDB_API_KEY") or "").strip()
    return resolved or None


def _request_json(
    session: requests.Session,
    url: str,
    *,
    params: Mapping[str, Any] | None = None,
    timeout_seconds: float = 20.0,
) -> dict[str, Any]:
    headers = {
        "accept": "application/json",
        "user-agent": "Mozilla/5.0",
    }
    max_attempts = 3

    last_response: requests.Response | None = None
    for attempt in range(max_attempts):
        try:
            resp = session.get(url, params=params, headers=headers, timeout=timeout_seconds)
        except requests.RequestException as exc:
            if attempt < max_attempts - 1:
                delay = 1.0 * (2**attempt)
                jitter = random.uniform(0.0, delay * 0.25)
                time.sleep(delay + jitter)
                continue
            raise TmdbClientError(f"TMDb request failed: {exc}") from exc

        last_response = resp
        if resp.status_code == 200:
            break

        retryable = resp.status_code == 429 or 500 <= resp.status_code < 600
        if retryable and attempt < max_attempts - 1:
            delay = 1.0 * (2**attempt)
            retry_after = (resp.headers.get("Retry-After") or "").strip()
            if retry_after.isdigit():
                delay = max(delay, float(retry_after))
            jitter = random.uniform(0.0, delay * 0.25)
            time.sleep(delay + jitter)
            continue

        raise TmdbClientError(
            f"TMDb request failed with HTTP {resp.status_code}.",
            status_code=resp.status_code,
            body_snippet=(resp.text or "")[:400],
        )

    if last_response is None:
        raise TmdbClientError("TMDb request failed (no response).")
    resp = last_response

    try:
        payload = resp.json()
    except ValueError as exc:
        raise TmdbClientError(
            "TMDb returned non-JSON response.",
            status_code=resp.status_code,
            body_snippet=(resp.text or "")[:400],
        ) from exc

    if not isinstance(payload, dict):
        raise TmdbClientError("TMDb returned unexpected JSON shape (not an object).")
    return payload


def fetch_list_items(
    list_id: str | int,
    *,
    api_key: str | None = None,
    session: requests.Session | None = None,
) -> list[dict[str, Any]]:
    """
    Fetch all items for a TMDb list.

    Note: Lists may contain movies and TV shows; callers should filter by
    `media_type` as needed.
    """

    api_key = _require_api_key(api_key)
    list_id_int = parse_tmdb_list_id(list_id)
    session = session or requests.Session()

    items: list[dict[str, Any]] = []
    page = 1
    while True:
        url = f"{TMDB_API_BASE_URL}/list/{list_id_int}"
        payload = _request_json(session, url, params={"api_key": api_key, "page": page})
        page_items = payload.get("items")
        if isinstance(page_items, list):
            items.extend([i for i in page_items if isinstance(i, dict)])

        total_pages = payload.get("total_pages")
        if isinstance(total_pages, int) and total_pages > 0:
            if page >= total_pages:
                break
            page += 1
            continue

        # If pagination is not provided, treat as single page.
        break

    return items


def fetch_tv_external_ids(
    tv_id: int,
    *,
    api_key: str | None = None,
    session: requests.Session | None = None,
    cache: dict[tuple[int, str, tuple[str, ...]], dict[str, Any]] | None = None,
    language: str = "en-US",
    prefer_append: bool = False,
) -> dict[str, Any]:
    """
    Fetch TMDb external ids for a TV series.

    When `prefer_append=True`, this uses `/tv/{id}?append_to_response=external_ids` and extracts the appended
    payload (so it can share a cache with other TV-details calls). Otherwise it calls `/tv/{id}/external_ids`.
    """

    if prefer_append:
        payload = fetch_tv_details(
            tv_id,
            api_key=api_key,
            session=session,
            language=language,
            append_to_response=["external_ids"],
            cache=cache,
        )
        ext = payload.get("external_ids")
        if isinstance(ext, Mapping):
            return dict(ext)
        raise TmdbClientError("TMDb response missing external_ids.")

    api_key = _require_api_key(api_key)
    session = session or requests.Session()
    url = f"{TMDB_API_BASE_URL}/tv/{int(tv_id)}/external_ids"
    return _request_json(session, url, params={"api_key": api_key})


def find_by_imdb_id(
    imdb_id: str,
    *,
    api_key: str | None = None,
    session: requests.Session | None = None,
) -> dict[str, Any]:
    """
    Resolve a TMDb record from an IMDb id via `/find/{external_id}`.

    Callers should inspect `tv_results` (and optionally other *_results keys).
    """

    api_key = _require_api_key(api_key)
    session = session or requests.Session()
    url = f"{TMDB_API_BASE_URL}/find/{imdb_id}"
    return _request_json(session, url, params={"api_key": api_key, "external_source": "imdb_id"})


def fetch_tv_details(
    tv_id: int,
    *,
    language: str = "en-US",
    api_key: str | None = None,
    session: requests.Session | None = None,
    append_to_response: list[str] | None = None,
    cache: dict[tuple[int, str, tuple[str, ...]], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """
    Fetch a TV series details payload from TMDb.

    Returns the full JSON object as returned by `/3/tv/{id}`.

    Callers may pass a per-run `cache` dict keyed by tmdb id to avoid refetching.
    """

    tv_id_int = int(tv_id)
    append_parts = [p.strip() for p in (append_to_response or []) if isinstance(p, str) and p.strip()]
    append_key = tuple(sorted(set(append_parts)))
    cache_key = (tv_id_int, str(language or "en-US"), append_key)

    if cache is not None and cache_key in cache:
        return cache[cache_key]

    api_key = _require_api_key(api_key)
    session = session or requests.Session()
    url = f"{TMDB_API_BASE_URL}/tv/{tv_id_int}"
    params: dict[str, Any] = {"api_key": api_key, "language": language}
    if append_key:
        params["append_to_response"] = ",".join(append_key)
    payload = _request_json(session, url, params=params)
    if cache is not None:
        cache[cache_key] = payload
    return payload


def fetch_tv_alternative_titles(
    tv_id: int,
    *,
    api_key: str | None = None,
    session: requests.Session | None = None,
    cache: dict[tuple[int, str, tuple[str, ...]], dict[str, Any]] | None = None,
    language: str = "en-US",
    prefer_append: bool = True,
) -> dict[str, Any]:
    """
    Fetch TMDb TV alternative titles.

    Prefers using `/tv/{id}?append_to_response=alternative_titles` so it can share a cache with TV-details calls.
    """

    if prefer_append:
        payload = fetch_tv_details(
            tv_id,
            api_key=api_key,
            session=session,
            language=language,
            append_to_response=["alternative_titles"],
            cache=cache,
        )
        alt = payload.get("alternative_titles")
        if isinstance(alt, Mapping):
            return dict(alt)
        raise TmdbClientError("TMDb response missing alternative_titles.")

    api_key = _require_api_key(api_key)
    session = session or requests.Session()
    url = f"{TMDB_API_BASE_URL}/tv/{int(tv_id)}/alternative_titles"
    return _request_json(session, url, params={"api_key": api_key})


def fetch_tv_watch_providers(
    tv_id: int,
    *,
    api_key: str | None = None,
    session: requests.Session | None = None,
) -> dict[str, Any]:
    api_key = _require_api_key(api_key)
    session = session or requests.Session()
    url = f"{TMDB_API_BASE_URL}/tv/{int(tv_id)}/watch/providers"
    return _request_json(session, url, params={"api_key": api_key})


def fetch_tv_images(
    tv_id: int,
    *,
    language: str = "en-US",
    include_image_language: str = "en,null",
    api_key: str | None = None,
    session: requests.Session | None = None,
    cache: dict[tuple[int, str, str], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """
    Fetch a TV series images payload from TMDb.

    Returns the full JSON object as returned by `/3/tv/{id}/images`.

    Callers may pass a per-run `cache` dict keyed by (tmdb id, language, include_image_language).
    """

    tv_id_int = int(tv_id)
    cache_key = (tv_id_int, str(language or "en-US"), str(include_image_language or ""))
    if cache is not None and cache_key in cache:
        return cache[cache_key]

    api_key = _require_api_key(api_key)
    session = session or requests.Session()
    url = f"{TMDB_API_BASE_URL}/tv/{tv_id_int}/images"
    payload = _request_json(
        session,
        url,
        params={
            "api_key": api_key,
            "language": language,
            "include_image_language": include_image_language,
        },
    )
    if cache is not None:
        cache[cache_key] = payload
    return payload
