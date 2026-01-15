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
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from trr_backend.integrations.imdb.credits_client import ImdbTitleCredits

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


def normalize_api_credits_to_cast_rows(
    credits_response: ImdbTitleCredits,
    *,
    job_category_filter: str | None = None,
) -> list[CastRow]:
    """
    Map JSON API credits response (from api.imdbapi.dev) to CastRow format.

    This enables fallback from HTML scraping to JSON API when IMDb blocks
    the /fullcredits/ page with 202/403/429 responses.

    Args:
        credits_response: Response from fetch_title_credits()
        job_category_filter: Optional category filter (e.g., "self" for reality shows)

    Returns:
        List of CastRow instances compatible with sync_show_cast pipeline

    Example:
        >>> from trr_backend.integrations.imdb.credits_client import fetch_title_credits
        >>> credits = fetch_title_credits("tt1720601")
        >>> rows = normalize_api_credits_to_cast_rows(credits, job_category_filter="self")
        >>> len(rows)
        25
    """
    from trr_backend.integrations.imdb.credits_client import ImdbTitleCredits

    if not isinstance(credits_response, ImdbTitleCredits):
        raise TypeError(f"Expected ImdbTitleCredits, got {type(credits_response).__name__}")

    # Crew categories to exclude (we only want cast: actor/actress/self)
    crew_categories = {
        "writer",
        "producer",
        "director",
        "cinematographer",
        "editor",
        "composer",
        "production_designer",
        "executive_producer",
        "co_producer",
    }

    rows: list[CastRow] = []
    for idx, credit in enumerate(credits_response.credits, start=1):
        # Extract fields from JSON API structure
        # API returns: {"name": {"id": "nm0000148", "displayName": "..."}, "category": "actor", "characters": ["Self"]}
        name_dict = credit.get("name") or {}
        name_id = name_dict.get("id")  # e.g., "nm0000148"
        name = name_dict.get("displayName")
        category = (credit.get("category") or "").strip().lower()
        characters = credit.get("characters") or []

        # Filter out crew categories (only include cast: actor/actress/self)
        if category in crew_categories:
            continue

        # Apply additional category filter if specified (e.g., job_category_filter="self")
        if job_category_filter and category != job_category_filter.lower():
            continue

        # Skip invalid entries
        if not name_id or not name:
            continue

        # Build role text from characters list
        role_text = None
        if isinstance(characters, list) and characters:
            role_text = ", ".join(str(char) for char in characters if char)

        # Map to job_category_id for filtering by filter_self_cast_rows()
        # Check if category is "self" OR if "Self" appears in characters (for reality shows)
        job_category_id = None
        is_self = category == "self"
        if not is_self and isinstance(characters, list):
            # Check if any character contains "self" (case-insensitive)
            is_self = any("self" in str(char).lower() for char in characters if char)

        if is_self:
            job_category_id = IMDB_JOB_CATEGORY_SELF

        rows.append(
            CastRow(
                name_id=name_id.strip().lower() if name_id else "",
                name=name.strip() if name else "",
                billing_order=idx,
                raw_role_text=role_text,
                job_category_id=job_category_id,
            )
        )

    return rows


def normalize_graphql_credits_to_cast_rows(
    graphql_edges: list[dict[str, Any]],
) -> list[CastRow]:
    """
    Map GraphQL persisted query credits to CastRow format.

    This enables fallback from HTML scraping to GraphQL API when IMDb blocks
    the /fullcredits/ page with 202/403/429 responses.

    Args:
        graphql_edges: List of credit edges from GraphQL pagination
            (each edge contains node with credit details)

    Returns:
        List of CastRow instances compatible with sync_show_cast pipeline

    Example:
        >>> from trr_backend.integrations.imdb.graphql_operations import fetch_title_credits_paginated_v2
        >>> edges = fetch_title_credits_paginated_v2("tt1720601")
        >>> rows = normalize_graphql_credits_to_cast_rows(edges)
        >>> len(rows)
        945  # All credits - use select_show_cast_from_graphql() to filter
    """
    rows: list[CastRow] = []

    for idx, edge in enumerate(graphql_edges, start=1):
        node = edge.get("node", {})

        # Extract name information
        name_dict = node.get("name", {})
        name_id = name_dict.get("id")  # e.g., "nm0000148"
        name = name_dict.get("nameText", {}).get("text")

        if not name_id or not name:
            continue

        # Extract role/character information
        # GraphQL structure: {"characters": [{"name": "Dr. Smith"}]}
        characters_list = node.get("characters") or []
        role_parts = []
        for char_obj in characters_list:
            if isinstance(char_obj, dict):
                char_name = char_obj.get("name")
                if char_name:
                    role_parts.append(str(char_name))

        raw_role_text = ", ".join(role_parts) if role_parts else None

        # Map to job_category_id for filtering by filter_self_cast_rows()
        # Check if raw_role_text starts with "Self" (for reality shows)
        job_category_id = None
        if is_self_role_text(raw_role_text):
            job_category_id = IMDB_JOB_CATEGORY_SELF

        # GraphQL also provides category field
        category = node.get("category", {}).get("id")
        if category and "self" in category.lower():
            job_category_id = IMDB_JOB_CATEGORY_SELF

        rows.append(
            CastRow(
                name_id=name_id.strip().lower() if name_id else "",
                name=name.strip() if name else "",
                billing_order=idx,
                raw_role_text=raw_role_text,
                job_category_id=job_category_id,
            )
        )

    return rows


def fetch_fullcredits_cast_with_fallback(
    series_id: str,
    *,
    extra_headers: Mapping[str, str] | None = None,
    verbose: bool = False,
) -> tuple[list[CastRow], str]:
    """
    Fetch full credits cast with 3-tier fallback: HTML → GraphQL → JSON API.

    This is the recommended entry point for fetching IMDb cast data, as it handles
    202/403/429 blocking gracefully by falling back through multiple data sources.

    Tier order is configurable via IMDB_CAST_PRIMARY_SOURCE:
    - "html" (default): HTML → GraphQL → JSON API (conservative rollout)
    - "graphql": GraphQL → HTML → JSON API (maximum reliability)

    Args:
        series_id: IMDb series ID (e.g., "tt1720601")
        extra_headers: Optional HTTP headers (for debugging only, not recommended)
        verbose: If True, log fallback events and save debug HTML

    Returns:
        Tuple of (cast_rows, source_type) where:
        - cast_rows: List of CastRow instances
        - source_type: One of:
            - "fullcredits_html"
            - "credits_graphql_paginated"
            - "credits_graphql_paginated_partial"
            - "credits_api_top_billed"

    Raises:
        ImdbFullCreditsError: If all tiers fail
        ValueError: If series_id is invalid

    Example:
        >>> rows, source = fetch_fullcredits_cast_with_fallback("tt1720601", verbose=True)
        >>> source
        'fullcredits_html'  # or 'credits_graphql_paginated' if HTML was blocked
    """
    # Feature flags
    enable_graphql = os.getenv("IMDB_GRAPHQL_ENABLED", "1").strip().lower() not in {"0", "false", "no"}
    enable_json_api = os.getenv("IMDB_FULLCREDITS_ENABLE_API_FALLBACK", "1").strip().lower() not in {"0", "false", "no"}

    # Determine tier order
    primary_source = os.getenv("IMDB_CAST_PRIMARY_SOURCE", "html").strip().lower()

    # Build tier list based on primary source
    if primary_source == "graphql" and enable_graphql:
        # GraphQL-first (maximum reliability)
        tiers = ["graphql", "html", "json_api"]
    else:
        # HTML-first (default - conservative rollout)
        tiers = ["html", "graphql", "json_api"]

    # Track errors for final fallback message
    errors: dict[str, Exception] = {}

    for tier in tiers:
        try:
            if tier == "html":
                return _try_html_fetch(series_id, extra_headers, verbose)

            elif tier == "graphql" and enable_graphql:
                return _try_graphql_fetch(series_id, extra_headers, verbose)

            elif tier == "json_api" and enable_json_api:
                return _try_json_api_fetch(series_id, verbose)

        except Exception as exc:
            errors[tier] = exc
            if verbose:
                print(f"⚠️  {tier.upper()} tier failed for {series_id}: {exc}")
            continue

    # All tiers failed
    error_summary = "; ".join(f"{tier}: {err}" for tier, err in errors.items())
    raise ImdbFullCreditsError(
        f"All fallback tiers failed for {series_id}. {error_summary}",
        status_code=getattr(errors.get("html"), "status_code", None),
        is_blocked=getattr(errors.get("html"), "is_blocked", False),
    )


def _try_html_fetch(
    series_id: str,
    extra_headers: Mapping[str, str] | None,
    verbose: bool,
) -> tuple[list[CastRow], str]:
    """Try HTML scraping tier."""
    client = HttpImdbFullCreditsClient(extra_headers=extra_headers)
    html = client.fetch_fullcredits_page(series_id, verbose=verbose)
    cast_rows = parse_fullcredits_cast_html(html, series_id=series_id)

    if verbose:
        print(f"✅ HTML fetch succeeded: {len(cast_rows)} credits")

    return cast_rows, "fullcredits_html"


def _try_graphql_fetch(
    series_id: str,
    extra_headers: Mapping[str, str] | None,
    verbose: bool,
) -> tuple[list[CastRow], str]:
    """Try GraphQL persisted query tier with cast selection filtering."""
    # Import here to avoid circular dependency
    from trr_backend.integrations.imdb.graphql_operations import (
        fetch_title_credits_paginated_v2,
        select_show_cast_from_graphql,
    )
    from trr_backend.integrations.imdb.graphql_persisted_client import (
        ImdbGraphQLPersistedClient,
    )

    # Create client with optional extra headers
    graphql_client = ImdbGraphQLPersistedClient(extra_headers=extra_headers or {})

    # Fetch all credits (unfiltered)
    all_edges = fetch_title_credits_paginated_v2(series_id, client=graphql_client)

    # Apply cast selection policy to filter to main cast
    filtered_edges, is_partial = select_show_cast_from_graphql(all_edges)

    # Normalize to CastRow format
    cast_rows = normalize_graphql_credits_to_cast_rows(filtered_edges)

    # Determine source_type based on partial flag
    source_type = "credits_graphql_paginated_partial" if is_partial else "credits_graphql_paginated"

    if verbose:
        print(
            f"✅ GraphQL fetch succeeded: {len(all_edges)} total credits → "
            f"{len(cast_rows)} main cast (partial={is_partial})"
        )

    return cast_rows, source_type


def _try_json_api_fetch(
    series_id: str,
    verbose: bool,
) -> tuple[list[CastRow], str]:
    """Try JSON API tier (top-billed only - last resort)."""
    # Import here to avoid circular dependency
    from trr_backend.integrations.imdb.credits_client import fetch_title_credits

    credits_response = fetch_title_credits(series_id)
    cast_rows = normalize_api_credits_to_cast_rows(credits_response)

    if verbose:
        print(f"✅ JSON API fallback succeeded: {len(cast_rows)} credits (PARTIAL - top-billed cast only)")

    return cast_rows, "credits_api_top_billed"
