from __future__ import annotations

import random
import re
import time
from dataclasses import dataclass
from typing import Any, Mapping

import requests

IMDB_API_BASE_URL = "https://api.imdbapi.dev"
_IMDB_TITLE_ID_RE = re.compile(r"^(tt[0-9]+)$")


class ImdbCreditsClientError(RuntimeError):
    def __init__(self, message: str, *, status_code: int | None = None, body_snippet: str | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.body_snippet = body_snippet


@dataclass(frozen=True)
class ImdbTitleCredits:
    imdb_id: str
    credits: list[dict[str, Any]]
    total_count: int | None = None


def _request_json(
    session: requests.Session,
    url: str,
    *,
    params: Mapping[str, Any] | None = None,
    timeout_seconds: float = 20.0,
    sleep_ms: int = 0,
) -> dict[str, Any]:
    headers = {
        "accept": "application/json",
        "user-agent": "Mozilla/5.0",
    }
    max_attempts = 3

    last_response: requests.Response | None = None
    for attempt in range(max_attempts):
        if sleep_ms:
            time.sleep(sleep_ms / 1000.0)

        try:
            resp = session.get(url, params=params, headers=headers, timeout=timeout_seconds)
        except requests.RequestException as exc:
            if attempt < max_attempts - 1:
                delay = 1.0 * (2**attempt)
                jitter = random.uniform(0.0, delay * 0.25)
                time.sleep(delay + jitter)
                continue
            raise ImdbCreditsClientError(f"IMDb credits request failed: {exc}") from exc

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

        raise ImdbCreditsClientError(
            f"IMDb credits request failed with HTTP {resp.status_code}.",
            status_code=resp.status_code,
            body_snippet=(resp.text or "")[:400],
        )

    if last_response is None:
        raise ImdbCreditsClientError("IMDb credits request failed (no response).")

    try:
        payload = last_response.json()
    except ValueError as exc:
        raise ImdbCreditsClientError(
            "IMDb credits returned non-JSON response.",
            status_code=last_response.status_code,
            body_snippet=(last_response.text or "")[:400],
        ) from exc

    if not isinstance(payload, dict):
        raise ImdbCreditsClientError("IMDb credits returned unexpected JSON shape (not an object).")
    return payload


def fetch_title_credits(
    imdb_id: str,
    *,
    session: requests.Session | None = None,
    timeout_seconds: float = 20.0,
    sleep_ms: int = 0,
) -> ImdbTitleCredits:
    imdb_id = str(imdb_id or "").strip()
    if not _IMDB_TITLE_ID_RE.match(imdb_id):
        raise ValueError(f"Invalid IMDb id: {imdb_id!r}")

    session = session or requests.Session()
    credits: list[dict[str, Any]] = []
    total_count: int | None = None
    page_token: str | None = None

    while True:
        params = {"pageToken": page_token} if page_token else None
        url = f"{IMDB_API_BASE_URL}/titles/{imdb_id}/credits"
        payload = _request_json(
            session,
            url,
            params=params,
            timeout_seconds=timeout_seconds,
            sleep_ms=sleep_ms,
        )

        if total_count is None:
            raw_total = payload.get("totalCount")
            if isinstance(raw_total, int):
                total_count = raw_total
            elif isinstance(raw_total, str) and raw_total.strip().isdigit():
                total_count = int(raw_total.strip())

        page_items = payload.get("credits")
        if isinstance(page_items, list):
            credits.extend([item for item in page_items if isinstance(item, dict)])

        next_token = payload.get("nextPageToken")
        if isinstance(next_token, str) and next_token.strip():
            page_token = next_token.strip()
            continue
        break

    return ImdbTitleCredits(imdb_id=imdb_id, credits=credits, total_count=total_count)
