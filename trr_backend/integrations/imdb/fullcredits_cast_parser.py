from __future__ import annotations

import json
import os
import random
import re
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup

from trr_backend.integrations.imdb.episodic_client import IMDB_JOB_CATEGORY_SELF

_IMDB_NAME_ID_RE = re.compile(r"(nm\d+)", re.IGNORECASE)
_IMDB_TITLE_ID_RE = re.compile(r"^tt\d+$", re.IGNORECASE)
_IMDB_CAST_GROUP_ID_RE = re.compile(r"amzn1\.imdb\.concept\.name_credit_group\.[a-z0-9\-]+", re.IGNORECASE)


class ImdbFullCreditsError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        body_snippet: str | None = None,
        is_blocked: bool = False,  # Indicates 202/403/429 blocked/rate-limited
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.body_snippet = body_snippet
        self.is_blocked = is_blocked


@dataclass(frozen=True)
class CastRow:
    name_id: str
    name: str
    billing_order: int | None
    raw_role_text: str | None
    job_category_id: str | None


class HttpImdbFullCreditsClient:
    def __init__(
        self,
        *,
        session: requests.Session | None = None,
        extra_headers: Mapping[str, str] | None = None,
        timeout_seconds: float = 30.0,
    ) -> None:
        self._session = session or requests.Session()
        self._extra_headers = dict(extra_headers or {})
        self._timeout_seconds = timeout_seconds

    def fetch_fullcredits_page(
        self,
        imdb_series_id: str,
        *,
        verbose: bool = False,
    ) -> str:
        """
        Fetch IMDb full credits page HTML with retry logic for blocked requests.

        Args:
            imdb_series_id: IMDb title ID (e.g., "tt1720601")
            verbose: If True, save debug HTML artifacts on blocked responses

        Returns:
            HTML content of full credits page

        Raises:
            ValueError: If imdb_series_id is invalid format
            ImdbFullCreditsError: If request fails after retries (with is_blocked=True for 202/403/429)
        """
        imdb_series_id = str(imdb_series_id or "").strip()
        if not _IMDB_TITLE_ID_RE.match(imdb_series_id):
            raise ValueError(f"Invalid IMDb id: {imdb_series_id!r}")

        url = f"https://www.imdb.com/title/{imdb_series_id}/fullcredits/"
        headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "accept-language": "en-US,en;q=0.9",
            "user-agent": "Mozilla/5.0",
            **self._extra_headers,
        }

        # Environment-configurable retry settings
        # max_retries = additional attempts after first request
        # So total attempts = 1 + max_retries
        max_retries = int(os.getenv("IMDB_FULLCREDITS_MAX_RETRIES", "2"))  # Default 2 retries = 3 total attempts
        base_delay = float(os.getenv("IMDB_FULLCREDITS_RETRY_BASE_DELAY_SEC", "5.0"))

        last_response: requests.Response | None = None
        last_exception: Exception | None = None

        for attempt in range(1 + max_retries):  # 1 initial + N retries
            try:
                resp = self._session.get(url, headers=headers, timeout=self._timeout_seconds)
                last_response = resp
            except requests.RequestException as exc:
                last_exception = exc
                if attempt < max_retries:
                    delay = base_delay * (2**attempt)
                    jitter = random.uniform(0, delay * 0.25)
                    time.sleep(delay + jitter)
                    continue
                # Exhausted retries on network error
                raise ImdbFullCreditsError(f"IMDb request failed: {exc}") from exc

            # Success case
            if resp.status_code == 200:
                return resp.text or ""

            # Blocked/rate-limited responses (202=queued, 403=forbidden, 429=rate-limited)
            is_blocked = resp.status_code in {202, 403, 429}

            # Retry if blocked and have retries left
            if is_blocked and attempt < max_retries:
                # Exponential backoff with jitter
                delay = base_delay * (2**attempt)
                jitter = random.uniform(0, delay * 0.25)
                time.sleep(delay + jitter)
                continue

            # Exhausted retries or non-retryable error
            # Save debug artifact ONCE (on final blocked attempt)
            if verbose and is_blocked:
                self._save_debug_html(imdb_series_id, resp)

            raise ImdbFullCreditsError(
                f"IMDb fullcredits {'blocked/rate-limited' if is_blocked else 'request failed'} "
                f"with HTTP {resp.status_code} (after {attempt + 1} attempt(s)).",
                status_code=resp.status_code,
                body_snippet=(resp.text or "")[:200],
                is_blocked=is_blocked,
            )

        # Should never reach here, but satisfy type checker
        if last_response:
            raise ImdbFullCreditsError(
                f"IMDb fullcredits request failed with HTTP {last_response.status_code}.",
                status_code=last_response.status_code,
                body_snippet=(last_response.text or "")[:200],
                is_blocked=last_response.status_code in {202, 403, 429},
            )
        if last_exception:
            raise ImdbFullCreditsError(f"IMDb request failed: {last_exception}") from last_exception
        raise ImdbFullCreditsError("IMDb fullcredits request failed (no response).")

    def _save_debug_html(self, imdb_series_id: str, resp: requests.Response) -> None:
        """
        Save blocked response HTML to debug_html/ directory (strips sensitive headers).

        Uses Path.cwd() for testability (tests can monkeypatch.chdir).
        """
        debug_dir = Path.cwd() / "debug_html"
        debug_dir.mkdir(exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"imdb_fullcredits_{imdb_series_id}_{timestamp}_http{resp.status_code}.html"
        filepath = debug_dir / filename

        try:
            filepath.write_text(resp.text or "", encoding="utf-8")
            print(f"Debug HTML saved: {filepath}")
        except Exception as exc:
            print(f"Warning: Failed to save debug HTML: {exc}")


def _extract_imdb_name_id(value: str | None) -> str | None:
    if not value:
        return None
    match = _IMDB_NAME_ID_RE.search(value)
    if not match:
        return None
    return match.group(1).lower()


def _extract_cast_group_id_from_soup(soup: BeautifulSoup) -> str | None:
    for option in soup.select("select#jump-to option"):
        label = option.get_text(strip=True)
        if "cast" not in label.casefold():
            continue
        value = str(option.get("value") or "").strip()
        if value.startswith("#"):
            value = value[1:]
        if _IMDB_CAST_GROUP_ID_RE.match(value or ""):
            return value

    for span in soup.find_all("span", id=_IMDB_CAST_GROUP_ID_RE):
        label = span.get_text(strip=True)
        if label and label.casefold() == "cast":
            return span.get("id")

    return None


def _extract_cast_group_id_from_html(html: str) -> str | None:
    match = re.search(
        r'id="(' + _IMDB_CAST_GROUP_ID_RE.pattern + r')"[^>]*>\s*Cast\s*<',
        html,
        re.IGNORECASE,
    )
    if match:
        return match.group(1)
    match = re.search(
        r'value="#(' + _IMDB_CAST_GROUP_ID_RE.pattern + r')"[^>]*>\s*Cast',
        html,
        re.IGNORECASE,
    )
    if match:
        return match.group(1)
    return None


def _extract_cast_group_id_from_next_data(html: str) -> str | None:
    match = re.search(
        r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>',
        html,
        re.S,
    )
    if not match:
        return None
    try:
        payload = json.loads(match.group(1))
    except json.JSONDecodeError:
        return None

    def walk(value: Any) -> str | None:
        if isinstance(value, Mapping):
            grouping_id = value.get("groupingId")
            text = value.get("text")
            if (
                isinstance(grouping_id, str)
                and _IMDB_CAST_GROUP_ID_RE.match(grouping_id)
                and isinstance(text, str)
                and text.casefold() == "cast"
            ):
                return grouping_id
            for child in value.values():
                found = walk(child)
                if found:
                    return found
        elif isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
            for item in value:
                found = walk(item)
                if found:
                    return found
        return None

    return walk(payload)


def _build_role_text(role_anchor) -> str | None:
    if role_anchor is None:
        return None
    base_text = role_anchor.get_text(" ", strip=True)
    if not base_text:
        return None

    parent = getattr(role_anchor, "parent", None)
    if parent is not None and getattr(parent, "parent", None) is not None:
        combined = parent.parent.get_text(" ", strip=True)
        if combined and base_text.casefold() in combined.casefold():
            return combined

    return base_text


def _parse_cast_items_from_section(section, *, series_id: str | None, job_category_id: str | None) -> list[CastRow]:
    rows: list[CastRow] = []
    items = section.find_all("li", attrs={"data-testid": "name-credits-list-item"})
    for idx, item in enumerate(items, start=1):
        name_anchor = None
        name = ""
        for candidate in item.find_all("a", href=re.compile(r"/name/nm\d+", re.IGNORECASE)):
            candidate_name = candidate.get_text(strip=True)
            if candidate_name:
                name_anchor = candidate
                name = candidate_name
                break
        if not name_anchor:
            continue
        name_id = _extract_imdb_name_id(name_anchor.get("href"))
        if not name_id or not name:
            continue

        role_anchor = None
        if series_id:
            role_anchor = item.find(
                "a",
                href=re.compile(rf"/title/{re.escape(series_id)}/characters/", re.IGNORECASE),
            )
        if role_anchor is None:
            role_anchor = item.find("a", href=re.compile(r"/characters/", re.IGNORECASE))

        raw_role_text = _build_role_text(role_anchor)

        rows.append(
            CastRow(
                name_id=name_id,
                name=name,
                billing_order=idx,
                raw_role_text=raw_role_text,
                job_category_id=job_category_id,
            )
        )
    return rows


def _parse_cast_items_from_legacy_table(
    soup: BeautifulSoup,
    *,
    job_category_id: str | None,
) -> list[CastRow]:
    table = soup.find("table", class_=re.compile(r"\bcast_list\b"))
    if not table:
        return []

    rows: list[CastRow] = []
    for idx, row in enumerate(table.find_all("tr"), start=1):
        name_anchor = row.find("a", href=re.compile(r"/name/nm\d+", re.IGNORECASE))
        if not name_anchor:
            continue
        name_id = _extract_imdb_name_id(name_anchor.get("href"))
        name = name_anchor.get_text(strip=True)
        if not name_id or not name:
            continue
        tds = row.find_all("td")
        raw_role_text = None
        if tds:
            raw_role_text = tds[-1].get_text(" ", strip=True)

        rows.append(
            CastRow(
                name_id=name_id,
                name=name,
                billing_order=idx,
                raw_role_text=raw_role_text,
                job_category_id=job_category_id,
            )
        )
    return rows


def parse_fullcredits_cast_html(html: str, *, series_id: str | None = None) -> list[CastRow]:
    soup = BeautifulSoup(html, "html.parser")

    job_category_id = _extract_cast_group_id_from_soup(soup)
    if not job_category_id:
        job_category_id = _extract_cast_group_id_from_html(html)
    if not job_category_id:
        job_category_id = _extract_cast_group_id_from_next_data(html)
    if not job_category_id:
        job_category_id = IMDB_JOB_CATEGORY_SELF

    cast_section = None
    if job_category_id:
        cast_section = soup.find(attrs={"data-testid": f"sub-section-{job_category_id}"})

    if cast_section is not None:
        rows = _parse_cast_items_from_section(
            cast_section,
            series_id=series_id,
            job_category_id=job_category_id,
        )
        if rows:
            return rows

    legacy_rows = _parse_cast_items_from_legacy_table(soup, job_category_id=job_category_id)
    if legacy_rows:
        return legacy_rows

    return []


def fetch_fullcredits_cast(
    series_id: str,
    *,
    extra_headers: Mapping[str, str] | None = None,
) -> list[CastRow]:
    client = HttpImdbFullCreditsClient(extra_headers=extra_headers)
    html = client.fetch_fullcredits_page(series_id)
    return parse_fullcredits_cast_html(html, series_id=series_id)


def is_self_role_text(value: str | None) -> bool:
    if not value:
        return False
    return value.strip().casefold().startswith("self")


def filter_self_cast_rows(rows: Sequence[CastRow]) -> list[CastRow]:
    return [row for row in rows if is_self_role_text(row.raw_role_text)]
