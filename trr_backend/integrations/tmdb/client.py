from __future__ import annotations

import os
import re
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
    try:
        resp = session.get(url, params=params, headers=headers, timeout=timeout_seconds)
    except requests.RequestException as exc:
        raise TmdbClientError(f"TMDb request failed: {exc}") from exc

    if resp.status_code != 200:
        raise TmdbClientError(
            f"TMDb request failed with HTTP {resp.status_code}.",
            status_code=resp.status_code,
            body_snippet=(resp.text or "")[:400],
        )

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
) -> dict[str, Any]:
    api_key = _require_api_key(api_key)
    session = session or requests.Session()
    url = f"{TMDB_API_BASE_URL}/tv/{int(tv_id)}/external_ids"
    return _request_json(session, url, params={"api_key": api_key})
