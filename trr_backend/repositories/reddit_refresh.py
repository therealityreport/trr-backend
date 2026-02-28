"""Backend-owned Reddit refresh jobs and canonical persistence for period windows."""

from __future__ import annotations

import json
import logging
import os
import random
import re
import time
from collections import Counter
from datetime import UTC, datetime
from typing import Any

import requests
from psycopg2.extras import Json

from trr_backend.db import pg

logger = logging.getLogger(__name__)

REDDIT_USER_AGENT_DEFAULT = "TRRBackendRedditRefresh/1.0 (+https://thereality.report)"
REDDIT_TIMEOUT_SECONDS_DEFAULT = 20
REDDIT_MAX_HTTP_RETRIES_DEFAULT = 5
REDDIT_PAGE_COOLDOWN_SECONDS_DEFAULT = 0.25
REDDIT_RATE_LIMIT_DELAY_SECONDS_DEFAULT = 3.5
REDDIT_MAX_PAGES_DEFAULT = 500
REDDIT_MAX_SEARCH_PAGES_PER_QUERY_DEFAULT = 20
REDDIT_MAX_BACKFILL_QUERIES_DEFAULT = 12
REDDIT_MAX_COMMENTS_POSTS_PER_RUN_DEFAULT = 60
REDDIT_COMMENT_TREE_DEPTH_DEFAULT = 12
REDDIT_COMMENT_LIMIT_DEFAULT = 500

FRANCHISE_EXCLUDE_TERMS = (
    "rhoa",
    "rhobh",
    "rhop",
    "rhonj",
    "rhony",
    "rhoc",
    "rhom",
    "rhodubai",
    "wife swap",
    "real housewives edition",
)

DEFAULT_RHOSLC_TERMS = (
    "RHOSLC",
    "Real Housewives of Salt Lake City",
    "Salt Lake City",
    "SLC",
)

TOKEN_MARKER_RE = re.compile(r":[^:\s]+:")
LEADING_DECOR_RE = re.compile(r"^[^\w]+", flags=re.UNICODE)
TRAILING_DECOR_RE = re.compile(r"[^\w]+$", flags=re.UNICODE)
WORD_TOKEN_RE = re.compile(r"[a-zA-Z][a-zA-Z0-9']+")
ACRONYM_TERM_RE = re.compile(r"^[a-z0-9]{2,6}$")
SEED_POST_ID_RE = re.compile(r"/comments/([a-z0-9]{5,9})(?:/|$)", flags=re.IGNORECASE)


class RedditRefreshError(Exception):
    def __init__(self, message: str, *, status: int = 500, retry_after_seconds: float | None = None) -> None:
        super().__init__(message)
        self.status = status
        self.retry_after_seconds = retry_after_seconds


def _env_int(name: str, default: int, *, minimum: int = 1, maximum: int | None = None) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        parsed = int(raw)
    except ValueError:
        return default
    bounded = max(minimum, parsed)
    if maximum is not None:
        bounded = min(maximum, bounded)
    return bounded


def _env_float(name: str, default: float, *, minimum: float = 0.0, maximum: float | None = None) -> float:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        parsed = float(raw)
    except ValueError:
        return default
    bounded = max(minimum, parsed)
    if maximum is not None:
        bounded = min(maximum, bounded)
    return bounded


def _collapse_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def to_canonical_flair_key(value: str | None) -> str:
    if not isinstance(value, str):
        return ""
    next_value = _collapse_whitespace(value).lower()
    if not next_value:
        return ""
    next_value = TOKEN_MARKER_RE.sub(" ", next_value)
    next_value = _collapse_whitespace(next_value)
    previous = ""
    while previous != next_value:
        previous = next_value
        next_value = LEADING_DECOR_RE.sub("", next_value)
        next_value = TRAILING_DECOR_RE.sub("", next_value)
    return _collapse_whitespace(next_value)


def _normalize_text(value: str) -> str:
    return _collapse_whitespace((value or "").lower())


def _normalize_subreddit(value: str) -> str:
    text = (value or "").strip()
    text = re.sub(r"^https?://(?:www\.)?reddit\.com/r/", "", text, flags=re.IGNORECASE)
    text = re.sub(r"^r/", "", text, flags=re.IGNORECASE)
    text = text.strip("/")
    text = text.split("/", 1)[0]
    return text.lower()


def _json_value(value: Any) -> Json:
    return Json(value if value is not None else {})


def _parse_iso(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _iso_utc(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _build_terms(show_name: str, show_aliases: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in [*DEFAULT_RHOSLC_TERMS, show_name, *show_aliases]:
        if not isinstance(raw, str):
            continue
        text = _normalize_text(raw)
        if not text or len(text) < 2:
            continue
        if text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def _build_cast_terms(cast_names: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in cast_names:
        if not isinstance(raw, str):
            continue
        text = _normalize_text(raw)
        if not text or len(text) < 3:
            continue
        if text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def _extract_word_tokens(value: str) -> list[str]:
    return [token.lower() for token in WORD_TOKEN_RE.findall(value or "")]


def _compile_term_pattern(term: str) -> re.Pattern[str]:
    normalized = _collapse_whitespace(term)
    escaped = re.escape(normalized).replace(r"\ ", r"\s+")
    if ACRONYM_TERM_RE.fullmatch(normalized.lower()):
        return re.compile(rf"(?<![a-z0-9]){escaped}(?![a-z0-9])", flags=re.IGNORECASE)
    return re.compile(escaped, flags=re.IGNORECASE)


def _extract_seed_post_id(url: str) -> str | None:
    text = str(url or "").strip()
    if not text:
        return None
    if re.fullmatch(r"[a-z0-9]{5,9}", text, flags=re.IGNORECASE):
        return text.lower()
    match = SEED_POST_ID_RE.search(text)
    if match:
        return match.group(1).lower()
    return None


def _merge_by_post_id(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_id: dict[str, dict[str, Any]] = {}
    for row in rows:
        post_id = str(row.get("reddit_post_id") or "").strip()
        if not post_id:
            continue
        existing = by_id.get(post_id)
        if existing is None:
            by_id[post_id] = dict(row)
            continue
        merged_sorts = sorted(
            {
                *(existing.get("source_sorts") or []),
                *(row.get("source_sorts") or []),
            }
        )
        existing["source_sorts"] = merged_sorts
        if int(row.get("num_comments") or 0) > int(existing.get("num_comments") or 0):
            existing["num_comments"] = int(row.get("num_comments") or 0)
        if int(row.get("score") or 0) > int(existing.get("score") or 0):
            existing["score"] = int(row.get("score") or 0)
        if not existing.get("selftext") and row.get("selftext"):
            existing["selftext"] = row.get("selftext")
        if not existing.get("link_flair_text") and row.get("link_flair_text"):
            existing["link_flair_text"] = row.get("link_flair_text")
        posted_existing = _parse_iso(existing.get("posted_at"))
        posted_new = _parse_iso(row.get("posted_at"))
        if posted_new and (posted_existing is None or posted_new > posted_existing):
            existing["posted_at"] = row.get("posted_at")
        existing["raw_payload"] = row.get("raw_payload") or existing.get("raw_payload")
    return list(by_id.values())


def _filter_by_window(
    rows: list[dict[str, Any]],
    *,
    period_start: datetime | None,
    period_end: datetime | None,
) -> list[dict[str, Any]]:
    if period_start is None and period_end is None:
        return rows
    out: list[dict[str, Any]] = []
    for row in rows:
        posted_at = _parse_iso(row.get("posted_at"))
        if posted_at is None:
            continue
        if period_start and posted_at < period_start:
            continue
        if period_end and posted_at > period_end:
            continue
        out.append(row)
    return out


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except Exception:  # noqa: BLE001
        return 0


def _parse_listing_rows(children: list[dict[str, Any]], *, source_sort: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for child in children:
        data = child.get("data") if isinstance(child, dict) else None
        if not isinstance(data, dict):
            continue
        post_id = str(data.get("id") or "").strip()
        if not post_id:
            continue
        created_utc = data.get("created_utc")
        posted_at: str | None = None
        try:
            if created_utc is not None:
                posted_at = datetime.fromtimestamp(float(created_utc), tz=UTC).isoformat().replace("+00:00", "Z")
        except Exception:  # noqa: BLE001
            posted_at = None

        title = str(data.get("title") or "").strip()
        permalink = data.get("permalink")
        if isinstance(permalink, str) and permalink.startswith("/"):
            permalink = f"https://www.reddit.com{permalink}"

        out.append(
            {
                "reddit_post_id": post_id,
                "title": title,
                "selftext": str(data.get("selftext") or "") or None,
                "url": str(data.get("url") or "") or permalink,
                "permalink": permalink,
                "author": str(data.get("author") or "") or None,
                "score": _safe_int(data.get("score")),
                "num_comments": _safe_int(data.get("num_comments")),
                "posted_at": posted_at,
                "link_flair_text": str(data.get("link_flair_text") or "") or None,
                "source_sorts": [source_sort],
                "raw_payload": data,
            }
        )
    return out


class RedditHttpClient:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.timeout_seconds = _env_float(
            "REDDIT_FETCH_TIMEOUT_SECONDS",
            REDDIT_TIMEOUT_SECONDS_DEFAULT,
            minimum=1.0,
        )
        self.max_retries = _env_int(
            "REDDIT_FETCH_MAX_RETRIES",
            REDDIT_MAX_HTTP_RETRIES_DEFAULT,
            minimum=1,
            maximum=8,
        )
        self.page_cooldown = _env_float(
            "REDDIT_PAGE_COOLDOWN_SECONDS",
            REDDIT_PAGE_COOLDOWN_SECONDS_DEFAULT,
            minimum=0.0,
        )
        self.rate_limit_delay = _env_float(
            "REDDIT_RATE_LIMIT_DELAY_SECONDS",
            REDDIT_RATE_LIMIT_DELAY_SECONDS_DEFAULT,
            minimum=0.0,
        )
        self.client_id = (os.getenv("REDDIT_CLIENT_ID") or "").strip()
        self.client_secret = (os.getenv("REDDIT_CLIENT_SECRET") or "").strip()
        self.user_agent = (os.getenv("REDDIT_USER_AGENT") or "").strip() or REDDIT_USER_AGENT_DEFAULT
        self._oauth_token: str | None = None
        self._oauth_expires_at: float = 0.0

    def _auth_headers(self, *, use_oauth: bool) -> dict[str, str]:
        headers = {"User-Agent": self.user_agent}
        if use_oauth:
            token = self._get_oauth_token()
            if token:
                headers["Authorization"] = f"Bearer {token}"
        return headers

    def _get_oauth_token(self) -> str | None:
        if not self.client_id or not self.client_secret:
            return None
        now = time.time()
        if self._oauth_token and now < (self._oauth_expires_at - 30):
            return self._oauth_token
        try:
            response = self.session.post(
                "https://www.reddit.com/api/v1/access_token",
                headers={"User-Agent": self.user_agent},
                auth=(self.client_id, self.client_secret),
                data={"grant_type": "client_credentials"},
                timeout=self.timeout_seconds,
            )
            if response.status_code >= 400:
                logger.warning("[reddit_refresh_oauth_failed] status=%s", response.status_code)
                return None
            payload = response.json() if response.content else {}
            token = str(payload.get("access_token") or "").strip()
            expires_in = float(payload.get("expires_in") or 3600)
            if not token:
                return None
            self._oauth_token = token
            self._oauth_expires_at = time.time() + max(60.0, expires_in)
            return token
        except Exception as exc:  # noqa: BLE001
            logger.warning("[reddit_refresh_oauth_exception] %s", exc)
            return None

    def get_json(self, path: str, *, params: dict[str, Any]) -> dict[str, Any]:
        supports_oauth = bool(self.client_id and self.client_secret)
        base_urls = (
            ["https://oauth.reddit.com", "https://www.reddit.com"]
            if supports_oauth
            else ["https://www.reddit.com"]
        )
        last_error: Exception | None = None

        for base_index, base_url in enumerate(base_urls):
            use_oauth = base_url.startswith("https://oauth")
            for attempt in range(1, self.max_retries + 1):
                try:
                    response = self.session.get(
                        f"{base_url}{path}",
                        params=params,
                        headers=self._auth_headers(use_oauth=use_oauth),
                        timeout=self.timeout_seconds,
                    )
                    if response.status_code == 429:
                        retry_after = response.headers.get("Retry-After")
                        delay = self.rate_limit_delay
                        if retry_after:
                            try:
                                delay = max(delay, float(retry_after))
                            except ValueError:
                                pass
                        if attempt >= self.max_retries:
                            raise RedditRefreshError(
                                "Reddit rate limit hit, try again shortly.",
                                status=429,
                                retry_after_seconds=delay,
                            )
                        time.sleep(delay + random.uniform(0, 0.35))
                        continue
                    if response.status_code >= 500 and attempt < self.max_retries:
                        time.sleep((0.2 * (2 ** (attempt - 1))) + random.uniform(0, 0.1))
                        continue
                    if response.status_code >= 400:
                        raise RedditRefreshError(
                            f"Reddit request failed ({response.status_code})",
                            status=response.status_code,
                        )
                    payload = response.json() if response.content else {}
                    if self.page_cooldown > 0:
                        time.sleep(self.page_cooldown)
                    return payload if isinstance(payload, dict) else {}
                except RedditRefreshError:
                    raise
                except Exception as exc:  # noqa: BLE001
                    last_error = exc
                    if attempt >= self.max_retries:
                        break
                    time.sleep((0.2 * (2 ** (attempt - 1))) + random.uniform(0, 0.1))
            if base_index < len(base_urls) - 1:
                continue

        if last_error is not None:
            raise RedditRefreshError(f"Reddit request failed: {last_error}", status=502) from last_error
        raise RedditRefreshError("Reddit request failed", status=502)


_HTTP_CLIENT = RedditHttpClient()


def _window_complete_for_page(
    *,
    rows: list[dict[str, Any]],
    period_start: datetime | None,
    reached_period_start: bool,
) -> bool:
    if period_start is None:
        return True
    if reached_period_start:
        return True
    oldest: datetime | None = None
    for row in rows:
        posted = _parse_iso(row.get("posted_at"))
        if posted is None:
            continue
        oldest = posted if oldest is None or posted < oldest else oldest
    return oldest is not None and oldest <= period_start


def _fetch_new_window_exhaustive(
    *,
    subreddit: str,
    period_start: datetime | None,
    period_end: datetime | None,
    max_pages: int,
) -> tuple[list[dict[str, Any]], int, bool]:
    rows: list[dict[str, Any]] = []
    pages_fetched = 0
    after: str | None = None
    reached_period_start = False

    for _ in range(max_pages):
        params: dict[str, Any] = {"limit": 100, "raw_json": 1}
        if after:
            params["after"] = after
        payload = _HTTP_CLIENT.get_json(f"/r/{subreddit}/new.json", params=params)
        listing = payload.get("data") if isinstance(payload, dict) else None
        children = listing.get("children") if isinstance(listing, dict) else []
        parsed_rows = _parse_listing_rows(children if isinstance(children, list) else [], source_sort="new")
        pages_fetched += 1
        if parsed_rows:
            rows.extend(parsed_rows)
            reached_period_start = _window_complete_for_page(
                rows=parsed_rows,
                period_start=period_start,
                reached_period_start=reached_period_start,
            )
            if reached_period_start:
                break
        after_value = listing.get("after") if isinstance(listing, dict) else None
        after = str(after_value) if after_value else None
        if after is None:
            # Terminal page: exhaustive only if period_start was actually reached.
            return rows, pages_fetched, bool(period_start is None or reached_period_start)

    if period_start is None:
        return rows, pages_fetched, True
    return rows, pages_fetched, reached_period_start


def _fetch_sample_sorts(
    *,
    subreddit: str,
    sort_modes: list[str],
    limit_per_mode: int,
) -> tuple[list[dict[str, Any]], dict[str, list[str]]]:
    rows: list[dict[str, Any]] = []
    diagnostics = {"successful_sorts": [], "failed_sorts": [], "rate_limited_sorts": []}

    for sort in sort_modes:
        try:
            payload = _HTTP_CLIENT.get_json(
                f"/r/{subreddit}/{sort}.json",
                params={"limit": max(1, min(100, limit_per_mode)), "raw_json": 1},
            )
            listing = payload.get("data") if isinstance(payload, dict) else None
            children = listing.get("children") if isinstance(listing, dict) else []
            rows.extend(
                _parse_listing_rows(
                    children if isinstance(children, list) else [],
                    source_sort=sort,
                )
            )
            diagnostics["successful_sorts"].append(sort)
        except RedditRefreshError as exc:
            diagnostics["failed_sorts"].append(sort)
            if exc.status == 429:
                diagnostics["rate_limited_sorts"].append(sort)
            logger.warning(
                "[reddit_refresh_sort_failed] subreddit=%s sort=%s status=%s",
                subreddit,
                sort,
                exc.status,
            )
    return rows, diagnostics


def _fetch_search_backfill(
    *,
    subreddit: str,
    tracked_flairs: list[str],
    show_aliases: list[str],
    show_terms: list[str],
    period_start: datetime | None,
    period_end: datetime | None,
    max_pages_per_query: int,
    max_total_queries: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    query_diagnostics: list[dict[str, Any]] = []

    canonical_seen: set[str] = set()
    queries: list[tuple[str, str, str]] = []
    for flair in tracked_flairs:
        canon = to_canonical_flair_key(flair)
        if not canon or canon in canonical_seen:
            continue
        canonical_seen.add(canon)
        # Primary exact flair query.
        queries.append((flair, f'flair:"{flair}"', "flair_exact"))
        if len(queries) >= max_total_queries:
            break
        # Gap-fill query: phrase search for flair text can recover older posts that flair: search misses.
        queries.append((flair, f'"{flair}"', "flair_phrase"))
        if len(queries) >= max_total_queries:
            break

    alias_seen: set[str] = set()
    for alias in show_aliases:
        alias_text = _collapse_whitespace(str(alias or ""))
        if not alias_text:
            continue
        alias_key = alias_text.lower()
        if alias_key in alias_seen:
            continue
        alias_seen.add(alias_key)
        if len(alias_text) < 3 or len(alias_text) > 48:
            continue
        queries.append((alias_text, alias_text, "show_alias_term"))
        if len(queries) >= max_total_queries:
            break

    show_term_seen: set[str] = set()
    for term in show_terms:
        term_text = _collapse_whitespace(str(term or ""))
        if len(term_text) < 8:
            continue
        term_key = term_text.lower()
        if term_key in show_term_seen:
            continue
        show_term_seen.add(term_key)
        queries.append((term_text, f'"{term_text}"', "show_term_phrase"))
        if len(queries) >= max_total_queries:
            break

    if len(queries) < max_total_queries:
        # Additional listing-style recovery path that avoids subreddit search index gaps.
        queries.append(("top_year", "", "top_year_listing"))

    pages_total = 0
    rows_total = 0
    rows_window_total = 0
    all_complete = True

    for flair, query, query_kind in queries:
        after: str | None = None
        pages = 0
        rows_fetched = 0
        rows_in_window = 0
        reached_period_start = False

        for _ in range(max_pages_per_query):
            params: dict[str, Any] = {"raw_json": 1, "limit": 100}
            path = f"/r/{subreddit}/search.json"
            if query_kind == "top_year_listing":
                params["t"] = "year"
                path = f"/r/{subreddit}/top.json"
            else:
                params.update(
                    {
                        "q": query,
                        "restrict_sr": "1",
                        "sort": "new",
                        "t": "all",
                        "type": "link",
                        "include_over_18": "on",
                    }
                )
            if after:
                params["after"] = after

            payload = _HTTP_CLIENT.get_json(path, params=params)
            listing = payload.get("data") if isinstance(payload, dict) else None
            children = listing.get("children") if isinstance(listing, dict) else []
            source_sort = "top" if query_kind == "top_year_listing" else "new"
            parsed_rows = _parse_listing_rows(children if isinstance(children, list) else [], source_sort=source_sort)
            pages += 1
            pages_total += 1
            if parsed_rows:
                rows.extend(parsed_rows)
                rows_fetched += len(parsed_rows)
                rows_total += len(parsed_rows)
                reached_period_start = _window_complete_for_page(
                    rows=parsed_rows,
                    period_start=period_start,
                    reached_period_start=reached_period_start,
                )
                filtered = _filter_by_window(parsed_rows, period_start=period_start, period_end=period_end)
                rows_in_window += len(filtered)
                rows_window_total += len(filtered)

            after_value = listing.get("after") if isinstance(listing, dict) else None
            after = str(after_value) if after_value else None
            if after is None:
                break

        query_complete = bool(period_start is None or reached_period_start)
        all_complete = all_complete and query_complete
        query_diagnostics.append(
            {
                "flair": flair,
                "query": query,
                "query_kind": query_kind,
                "pages_fetched": pages,
                "rows_fetched": rows_fetched,
                "rows_in_window": rows_in_window,
                "reached_period_start": reached_period_start,
                "complete": query_complete,
            }
        )

    return rows, {
        "enabled": True,
        "queries_run": len(query_diagnostics),
        "pages_fetched": pages_total,
        "rows_fetched": rows_total,
        "rows_in_window": rows_window_total,
        "complete": all_complete,
        "query_diagnostics": query_diagnostics,
    }


def _apply_match_metadata(
    *,
    rows: list[dict[str, Any]],
    subreddit: str,
    terms: list[str],
    cast_terms: list[str],
    analysis_flares: list[str],
    analysis_all_flares: list[str],
    force_include_flares: list[str],
    show_focused: bool,
) -> tuple[list[dict[str, Any]], dict[str, list[str]], int]:
    del subreddit  # Not currently needed for scoring, retained for API compatibility.
    scan_keys = {
        to_canonical_flair_key(flair) for flair in analysis_flares if to_canonical_flair_key(flair)
    }
    all_keys = {
        to_canonical_flair_key(flair) for flair in analysis_all_flares if to_canonical_flair_key(flair)
    }
    forced_keys = {
        to_canonical_flair_key(flair) for flair in force_include_flares if to_canonical_flair_key(flair)
    }
    has_tracked_flair_rules = bool(scan_keys or all_keys or forced_keys)
    term_patterns = [(term, _compile_term_pattern(term)) for term in terms if term]
    cast_term_patterns = [(term, _compile_term_pattern(term)) for term in cast_terms if term]
    include_counter: Counter[str] = Counter()
    exclude_counter: Counter[str] = Counter()
    output: list[dict[str, Any]] = []
    tracked_rows = 0

    for row in rows:
        title = str(row.get("title") or "")
        selftext = str(row.get("selftext") or "")
        searchable = _collapse_whitespace(f"{title} {selftext}")
        searchable_lower = searchable.lower()

        matched_terms = [term for term, pattern in term_patterns if pattern.search(searchable)]
        matched_cast_terms = [term for term, pattern in cast_term_patterns if pattern.search(searchable)]
        cross_show_terms = [term for term in FRANCHISE_EXCLUDE_TERMS if term in searchable_lower]

        flair_text = str(row.get("link_flair_text") or "") or None
        canonical_flair = to_canonical_flair_key(flair_text)
        has_scan_term = bool(matched_terms)

        forced_flair_match = bool(canonical_flair and canonical_flair in forced_keys)
        all_flair_match = bool(canonical_flair and canonical_flair in all_keys)
        scan_flair_match = bool(canonical_flair and canonical_flair in scan_keys and has_scan_term)
        passes_flair_filter = bool(
            forced_flair_match
            or all_flair_match
            or scan_flair_match
            or not has_tracked_flair_rules
        )
        flair_mode: str | None = None
        if forced_flair_match:
            flair_mode = "forced"
        elif all_flair_match:
            flair_mode = "all"
        elif scan_flair_match:
            flair_mode = "scan_term"
        if passes_flair_filter and has_tracked_flair_rules:
            tracked_rows += 1

        has_show_signal = bool(matched_terms or matched_cast_terms)
        is_show_match = True if show_focused else (has_show_signal and not cross_show_terms)
        if not terms and not cast_terms:
            is_show_match = True
        if flair_mode is None and is_show_match:
            flair_mode = "show_match"

        match_score = 0
        match_score += 40 if is_show_match else 0
        match_score += min(len(matched_terms) * 8, 24)
        match_score += min(len(matched_cast_terms) * 6, 18)
        match_score -= min(len(cross_show_terms) * 12, 36)

        include_thread = bool(is_show_match or passes_flair_filter)
        if include_thread:
            for token in _extract_word_tokens(title):
                include_counter[token] += 1
        else:
            for token in _extract_word_tokens(title):
                exclude_counter[token] += 1
            continue

        enriched = dict(row)
        enriched.update(
            {
                "text": row.get("selftext"),
                "matched_terms": matched_terms,
                "matched_cast_terms": matched_cast_terms,
                "cross_show_terms": cross_show_terms,
                "is_show_match": is_show_match,
                "passes_flair_filter": passes_flair_filter,
                "match_score": match_score,
                "suggested_include_terms": [],
                "suggested_exclude_terms": [],
                "canonical_flair_key": canonical_flair,
                "flair_mode": flair_mode,
            }
        )
        output.append(enriched)

    output.sort(
        key=lambda row: (
            int(row.get("match_score") or 0),
            int(row.get("num_comments") or 0),
            int(row.get("score") or 0),
        ),
        reverse=True,
    )

    hints = {
        "suggested_include_terms": [term for term, _count in include_counter.most_common(8)],
        "suggested_exclude_terms": [term for term, _count in exclude_counter.most_common(8)],
    }

    for thread in output:
        thread["suggested_include_terms"] = hints["suggested_include_terms"]
        thread["suggested_exclude_terms"] = hints["suggested_exclude_terms"]

    return output, hints, tracked_rows


def _fetch_submission_by_post_id(post_id: str, *, source_sort: str = "seed_url") -> dict[str, Any] | None:
    normalized_id = str(post_id or "").strip().lower()
    if not normalized_id:
        return None

    supports_oauth = bool(_HTTP_CLIENT.client_id and _HTTP_CLIENT.client_secret)
    base_urls = (
        ["https://oauth.reddit.com", "https://www.reddit.com"]
        if supports_oauth
        else ["https://www.reddit.com"]
    )

    for base_url in base_urls:
        use_oauth = base_url.startswith("https://oauth")
        endpoint = f"{base_url}/comments/{normalized_id}.json"
        for attempt in range(1, _HTTP_CLIENT.max_retries + 1):
            response = _HTTP_CLIENT.session.get(
                endpoint,
                params={"limit": 1, "depth": 1, "raw_json": 1},
                headers=_HTTP_CLIENT._auth_headers(use_oauth=use_oauth),
                timeout=_HTTP_CLIENT.timeout_seconds,
            )
            if response.status_code == 429:
                delay = _HTTP_CLIENT.rate_limit_delay
                retry_after = response.headers.get("Retry-After")
                if retry_after:
                    try:
                        delay = max(delay, float(retry_after))
                    except ValueError:
                        pass
                if attempt >= _HTTP_CLIENT.max_retries:
                    raise RedditRefreshError("Reddit rate limit hit, try again shortly.", status=429)
                time.sleep(delay + random.uniform(0, 0.35))
                continue
            if response.status_code >= 500 and attempt < _HTTP_CLIENT.max_retries:
                time.sleep((0.2 * (2 ** (attempt - 1))) + random.uniform(0, 0.1))
                continue
            if response.status_code >= 400:
                raise RedditRefreshError(
                    f"Reddit submission fetch failed ({response.status_code})",
                    status=response.status_code,
                )

            payload = response.json() if response.content else []
            if not isinstance(payload, list) or len(payload) < 1:
                return None
            listing = payload[0].get("data") if isinstance(payload[0], dict) else None
            children = listing.get("children") if isinstance(listing, dict) else []
            parsed_rows = _parse_listing_rows(children if isinstance(children, list) else [], source_sort=source_sort)
            if not parsed_rows:
                return None
            for row in parsed_rows:
                if str(row.get("reddit_post_id") or "").strip().lower() == normalized_id:
                    return row
            return parsed_rows[0]

    return None


def _fetch_seed_rows(seed_post_urls: list[str]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    requested_urls = [str(item or "").strip() for item in seed_post_urls if str(item or "").strip()]
    failed_urls: list[str] = []
    failed_ids: list[str] = []
    ingested_ids: list[str] = []
    parsed_ids = 0

    for seed in requested_urls:
        post_id = _extract_seed_post_id(seed)
        if not post_id:
            failed_urls.append(seed)
            continue
        if post_id in seen_ids:
            continue
        seen_ids.add(post_id)
        parsed_ids += 1
        try:
            row = _fetch_submission_by_post_id(post_id, source_sort="seed_url")
            if not row:
                failed_ids.append(post_id)
                failed_urls.append(seed)
                continue
            rows.append(row)
            ingested_ids.append(post_id)
        except Exception:  # noqa: BLE001
            failed_ids.append(post_id)
            failed_urls.append(seed)

    diagnostics = {
        "seed_urls_requested": len(requested_urls),
        "seed_urls_parsed": parsed_ids,
        "seed_urls_ingested": len(ingested_ids),
        "seed_urls_failed": len(failed_urls),
        "seed_ingested_post_ids": ingested_ids,
        "seed_failed_post_ids": failed_ids,
        "seed_failed_urls": failed_urls[:30],
    }
    return rows, diagnostics


def _walk_comment_nodes(
    nodes: list[dict[str, Any]],
    *,
    post_id: str,
    depth: int,
    flattened: list[dict[str, Any]],
) -> None:
    for node in nodes:
        if not isinstance(node, dict):
            continue
        kind = node.get("kind")
        data = node.get("data") if isinstance(node.get("data"), dict) else {}
        if kind != "t1":
            continue
        comment_id = str(data.get("id") or "").strip()
        if not comment_id:
            continue
        parent_fullname = str(data.get("parent_id") or "")
        parent_comment_id = parent_fullname[3:] if parent_fullname.startswith("t1_") else None
        created_at_utc = None
        created_raw = data.get("created_utc")
        try:
            if created_raw is not None:
                created_at_utc = (
                    datetime.fromtimestamp(float(created_raw), tz=UTC)
                    .isoformat()
                    .replace("+00:00", "Z")
                )
        except Exception:  # noqa: BLE001
            created_at_utc = None
        flattened.append(
            {
                "reddit_comment_id": comment_id,
                "reddit_post_id": post_id,
                "parent_comment_id": parent_comment_id,
                "author": str(data.get("author") or "") or None,
                "body": str(data.get("body") or ""),
                "score": _safe_int(data.get("score")),
                "depth": depth,
                "created_at_utc": created_at_utc,
                "raw_payload": data,
            }
        )
        replies = data.get("replies")
        if isinstance(replies, dict):
            rep_data = replies.get("data") if isinstance(replies.get("data"), dict) else None
            rep_children = rep_data.get("children") if isinstance(rep_data, dict) else None
            if isinstance(rep_children, list) and rep_children:
                _walk_comment_nodes(
                    rep_children,
                    post_id=post_id,
                    depth=depth + 1,
                    flattened=flattened,
                )


def _fetch_post_comments_tree(post_id: str) -> list[dict[str, Any]]:
    base_urls = ["https://oauth.reddit.com", "https://www.reddit.com"]
    supports_oauth = bool(_HTTP_CLIENT.client_id and _HTTP_CLIENT.client_secret)
    if not supports_oauth:
        base_urls = ["https://www.reddit.com"]

    last_exc: Exception | None = None
    for base_url in base_urls:
        use_oauth = base_url.startswith("https://oauth")
        url = f"{base_url}/comments/{post_id}.json"
        for attempt in range(1, _HTTP_CLIENT.max_retries + 1):
            try:
                resp = _HTTP_CLIENT.session.get(
                    url,
                    params={
                        "limit": _env_int(
                            "REDDIT_COMMENT_LIMIT",
                            REDDIT_COMMENT_LIMIT_DEFAULT,
                            minimum=50,
                            maximum=500,
                        ),
                        "depth": _env_int(
                            "REDDIT_COMMENT_TREE_DEPTH",
                            REDDIT_COMMENT_TREE_DEPTH_DEFAULT,
                            minimum=1,
                            maximum=20,
                        ),
                        "raw_json": 1,
                    },
                    headers=_HTTP_CLIENT._auth_headers(use_oauth=use_oauth),
                    timeout=_HTTP_CLIENT.timeout_seconds,
                )
                if resp.status_code == 429:
                    delay = _HTTP_CLIENT.rate_limit_delay
                    retry_after = resp.headers.get("Retry-After")
                    if retry_after:
                        try:
                            delay = max(delay, float(retry_after))
                        except ValueError:
                            pass
                    if attempt >= _HTTP_CLIENT.max_retries:
                        raise RedditRefreshError("Reddit rate limit hit, try again shortly.", status=429)
                    time.sleep(delay + random.uniform(0, 0.35))
                    continue
                if resp.status_code >= 500 and attempt < _HTTP_CLIENT.max_retries:
                    time.sleep((0.2 * (2 ** (attempt - 1))) + random.uniform(0, 0.1))
                    continue
                if resp.status_code >= 400:
                    raise RedditRefreshError(
                        f"Reddit comments fetch failed ({resp.status_code})",
                        status=resp.status_code,
                    )
                payload = resp.json() if resp.content else []
                if not isinstance(payload, list) or len(payload) < 2:
                    return []
                listing = payload[1].get("data") if isinstance(payload[1], dict) else None
                children = listing.get("children") if isinstance(listing, dict) else []
                if not isinstance(children, list):
                    return []
                flattened: list[dict[str, Any]] = []
                _walk_comment_nodes(children, post_id=post_id, depth=0, flattened=flattened)
                if _HTTP_CLIENT.page_cooldown > 0:
                    time.sleep(_HTTP_CLIENT.page_cooldown)
                return flattened
            except RedditRefreshError:
                raise
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                if attempt >= _HTTP_CLIENT.max_retries:
                    break
                time.sleep((0.2 * (2 ** (attempt - 1))) + random.uniform(0, 0.1))

    if last_exc is not None:
        raise RedditRefreshError(f"Failed to fetch post comments: {last_exc}", status=502) from last_exc
    return []


def _upsert_posts(rows: list[dict[str, Any]], *, conn: Any) -> None:
    if not rows:
        return
    tuples: list[tuple[Any, ...]] = []
    for row in rows:
        tuples.append(
            (
                row.get("reddit_post_id"),
                row.get("subreddit"),
                row.get("title") or "",
                row.get("selftext"),
                row.get("url"),
                row.get("permalink"),
                row.get("author"),
                _safe_int(row.get("score")),
                _safe_int(row.get("num_comments")),
                _parse_iso(row.get("posted_at")),
                row.get("link_flair_text"),
                row.get("canonical_flair_key") or to_canonical_flair_key(row.get("link_flair_text")),
                _json_value(row.get("source_sorts") or []),
                _json_value(row.get("raw_payload") or {}),
            )
        )

    pg.execute_values_no_return(
        """
        insert into social.reddit_posts (
          reddit_post_id,
          subreddit,
          title,
          selftext,
          url,
          permalink,
          author,
          score,
          num_comments,
          posted_at,
          link_flair_text,
          canonical_flair_key,
          source_sorts,
          raw_payload
        )
        values %s
        on conflict (reddit_post_id) do update
        set subreddit = excluded.subreddit,
            title = excluded.title,
            selftext = excluded.selftext,
            url = excluded.url,
            permalink = excluded.permalink,
            author = excluded.author,
            score = excluded.score,
            num_comments = excluded.num_comments,
            posted_at = excluded.posted_at,
            link_flair_text = excluded.link_flair_text,
            canonical_flair_key = excluded.canonical_flair_key,
            source_sorts = excluded.source_sorts,
            raw_payload = excluded.raw_payload,
            last_seen_at = now(),
            updated_at = now()
        """,
        tuples,
        conn=conn,
    )


def _upsert_comments(rows: list[dict[str, Any]], *, conn: Any) -> int:
    if not rows:
        return 0
    tuples: list[tuple[Any, ...]] = []
    for row in rows:
        tuples.append(
            (
                row.get("reddit_comment_id"),
                row.get("reddit_post_id"),
                row.get("parent_comment_id"),
                row.get("author"),
                row.get("body") or "",
                _safe_int(row.get("score")),
                _safe_int(row.get("depth")),
                _parse_iso(row.get("created_at_utc")),
                _json_value(row.get("raw_payload") or {}),
            )
        )

    pg.execute_values_no_return(
        """
        insert into social.reddit_comments (
          reddit_comment_id,
          reddit_post_id,
          parent_comment_id,
          author,
          body,
          score,
          depth,
          created_at_utc,
          raw_payload
        )
        values %s
        on conflict (reddit_comment_id) do update
        set reddit_post_id = excluded.reddit_post_id,
            parent_comment_id = excluded.parent_comment_id,
            author = excluded.author,
            body = excluded.body,
            score = excluded.score,
            depth = excluded.depth,
            created_at_utc = excluded.created_at_utc,
            raw_payload = excluded.raw_payload,
            last_seen_at = now(),
            updated_at = now()
        """,
        tuples,
        conn=conn,
    )
    return len(tuples)


def _replace_period_matches(
    *,
    community_id: str,
    season_id: str,
    period_key: str,
    period_start: datetime | None,
    period_end: datetime | None,
    run_id: str,
    rows: list[dict[str, Any]],
    conn: Any,
) -> None:
    with pg.db_cursor(conn=conn) as cur:
        cur.execute(
            """
            delete from social.reddit_period_post_matches
            where community_id = %s
              and season_id = %s
              and period_key = %s
            """,
            [community_id, season_id, period_key],
        )

    if not rows:
        return

    tuples: list[tuple[Any, ...]] = []
    for row in rows:
        tuples.append(
            (
                community_id,
                season_id,
                period_key,
                period_start,
                period_end,
                row.get("reddit_post_id"),
                run_id,
                bool(row.get("is_show_match")),
                bool(row.get("passes_flair_filter", True)),
                _json_value(row.get("matched_terms") or []),
                _json_value(row.get("matched_cast_terms") or []),
                _json_value(row.get("cross_show_terms") or []),
                _safe_int(row.get("match_score")),
                _json_value(row.get("source_sorts") or []),
                row.get("link_flair_text"),
                row.get("canonical_flair_key") or to_canonical_flair_key(row.get("link_flair_text")),
                row.get("flair_mode"),
            )
        )

    pg.execute_values_no_return(
        """
        insert into social.reddit_period_post_matches (
          community_id,
          season_id,
          period_key,
          period_start,
          period_end,
          reddit_post_id,
          run_id,
          is_show_match,
          passes_flair_filter,
          matched_terms,
          matched_cast_terms,
          cross_show_terms,
          match_score,
          source_sorts,
          link_flair_text,
          canonical_flair_key,
          flair_mode
        )
        values %s
        on conflict (community_id, season_id, period_key, reddit_post_id) do update
        set run_id = excluded.run_id,
            is_show_match = excluded.is_show_match,
            passes_flair_filter = excluded.passes_flair_filter,
            matched_terms = excluded.matched_terms,
            matched_cast_terms = excluded.matched_cast_terms,
            cross_show_terms = excluded.cross_show_terms,
            match_score = excluded.match_score,
            source_sorts = excluded.source_sorts,
            link_flair_text = excluded.link_flair_text,
            canonical_flair_key = excluded.canonical_flair_key,
            flair_mode = excluded.flair_mode,
            updated_at = now()
        """,
        tuples,
        conn=conn,
    )


def _base_thread_projection(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "reddit_post_id": row.get("reddit_post_id"),
        "title": row.get("title") or "",
        "text": row.get("selftext"),
        "url": row.get("url") or row.get("permalink"),
        "permalink": row.get("permalink"),
        "author": row.get("author"),
        "score": _safe_int(row.get("score")),
        "num_comments": _safe_int(row.get("num_comments")),
        "posted_at": _iso_utc(_parse_iso(row.get("posted_at"))) if row.get("posted_at") else None,
        "link_flair_text": row.get("link_flair_text"),
        "source_sorts": row.get("source_sorts") if isinstance(row.get("source_sorts"), list) else [],
        "matched_terms": row.get("matched_terms") if isinstance(row.get("matched_terms"), list) else [],
        "matched_cast_terms": row.get("matched_cast_terms") if isinstance(row.get("matched_cast_terms"), list) else [],
        "cross_show_terms": row.get("cross_show_terms") if isinstance(row.get("cross_show_terms"), list) else [],
        "is_show_match": bool(row.get("is_show_match")),
        "passes_flair_filter": bool(row.get("passes_flair_filter", True)),
        "match_score": _safe_int(row.get("match_score")),
        "flair_mode": row.get("flair_mode"),
        "suggested_include_terms": [],
        "suggested_exclude_terms": [],
    }


def get_cached_period_payload(*, community_id: str, season_id: str, period_key: str) -> dict[str, Any] | None:
    run = pg.fetch_one(
        """
        select id,
               subreddit,
               status,
               diagnostics,
               total_rows,
               matched_rows,
               tracked_flair_rows,
               created_at,
               completed_at
        from social.reddit_refresh_runs
        where community_id = %s
          and season_id = %s
          and period_key = %s
          and status in ('completed', 'partial')
        order by created_at desc
        limit 1
        """,
        [community_id, season_id, period_key],
    )
    if not run:
        return None

    diagnostics = run.get("diagnostics") if isinstance(run.get("diagnostics"), dict) else {}
    result_payload = diagnostics.get("result") if isinstance(diagnostics.get("result"), dict) else {}

    rows = pg.fetch_all(
        """
        select p.reddit_post_id,
               p.title,
               p.selftext,
               p.url,
               p.permalink,
               p.author,
               p.score,
               p.num_comments,
               p.posted_at,
               p.link_flair_text,
               m.source_sorts,
               m.matched_terms,
               m.matched_cast_terms,
               m.cross_show_terms,
               m.is_show_match,
               m.passes_flair_filter,
               m.match_score,
               m.flair_mode
        from social.reddit_period_post_matches m
        join social.reddit_posts p on p.reddit_post_id = m.reddit_post_id
        where m.community_id = %s
          and m.season_id = %s
          and m.period_key = %s
        order by m.match_score desc, p.num_comments desc, p.score desc
        """,
        [community_id, season_id, period_key],
    )

    threads = [_base_thread_projection(row) for row in rows]
    tracked_flair_rows = sum(1 for row in rows if bool(row.get("passes_flair_filter", True)))
    fetched_at = _iso_utc(_parse_iso(run.get("completed_at") or run.get("created_at")))
    if not fetched_at:
        fetched_at = datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")
    hints = diagnostics.get("hints") if isinstance(diagnostics.get("hints"), dict) else None
    if hints is None:
        hints = (
            result_payload.get("hints")
            if isinstance(result_payload.get("hints"), dict)
            else {"suggested_include_terms": [], "suggested_exclude_terms": []}
        )
    return {
        "subreddit": run.get("subreddit") or result_payload.get("subreddit"),
        "fetched_at": fetched_at,
        "collection_mode": "exhaustive_window",
        "sources_fetched": (
            result_payload.get("sources_fetched")
            if isinstance(result_payload.get("sources_fetched"), list)
            else ["new"]
        ),
        "successful_sorts": (
            result_payload.get("successful_sorts")
            if isinstance(result_payload.get("successful_sorts"), list)
            else ["new"]
        ),
        "failed_sorts": (
            result_payload.get("failed_sorts") if isinstance(result_payload.get("failed_sorts"), list) else []
        ),
        "rate_limited_sorts": (
            result_payload.get("rate_limited_sorts")
            if isinstance(result_payload.get("rate_limited_sorts"), list)
            else []
        ),
        "listing_pages_fetched": _safe_int(diagnostics.get("listing_pages_fetched")),
        "max_pages_applied": _safe_int(diagnostics.get("max_pages_applied")),
        "window_exhaustive_complete": diagnostics.get("window_exhaustive_complete"),
        "search_backfill": diagnostics.get("search_backfill"),
        "seed_urls": diagnostics.get("seed_urls"),
        "totals": {
            "fetched_rows": len(rows),
            "matched_rows": len(rows),
            "tracked_flair_rows": tracked_flair_rows,
        },
        "window_start": result_payload.get("window_start") if isinstance(result_payload, dict) else None,
        "window_end": result_payload.get("window_end") if isinstance(result_payload, dict) else None,
        "terms": result_payload.get("terms") if isinstance(result_payload.get("terms"), list) else [],
        "hints": hints,
        "threads": threads,
    }


def _update_run(
    run_id: str,
    *,
    status: str,
    diagnostics: dict[str, Any] | None = None,
    error_message: str | None = None,
    total_rows: int | None = None,
    matched_rows: int | None = None,
    tracked_flair_rows: int | None = None,
    set_started: bool = False,
    set_completed: bool = False,
) -> None:
    row = pg.fetch_one(
        "select diagnostics from social.reddit_refresh_runs where id = %s",
        [run_id],
    )
    existing_diag = row.get("diagnostics") if isinstance(row, dict) and isinstance(row.get("diagnostics"), dict) else {}
    merged_diag = dict(existing_diag)
    if isinstance(diagnostics, dict):
        merged_diag.update(diagnostics)

    values = {
        "status": status,
        "diagnostics": json.dumps(merged_diag, ensure_ascii=True),
        "error_message": error_message,
        "updated_at": datetime.now(tz=UTC),
        "started_at": datetime.now(tz=UTC) if set_started else None,
        "completed_at": datetime.now(tz=UTC) if set_completed else None,
        "total_rows": total_rows,
        "matched_rows": matched_rows,
        "tracked_flair_rows": tracked_flair_rows,
    }

    pg.execute_returning(
        """
        update social.reddit_refresh_runs
        set status = %(status)s,
            diagnostics = %(diagnostics)s::jsonb,
            error_message = coalesce(%(error_message)s, error_message),
            total_rows = coalesce(%(total_rows)s, total_rows),
            matched_rows = coalesce(%(matched_rows)s, matched_rows),
            tracked_flair_rows = coalesce(%(tracked_flair_rows)s, tracked_flair_rows),
            started_at = coalesce(%(started_at)s, started_at),
            completed_at = coalesce(%(completed_at)s, completed_at),
            updated_at = %(updated_at)s
        where id = %(run_id)s::uuid
        returning id
        """,
        {
            "status": values["status"],
            "diagnostics": values["diagnostics"],
            "error_message": values["error_message"],
            "total_rows": values["total_rows"],
            "matched_rows": values["matched_rows"],
            "tracked_flair_rows": values["tracked_flair_rows"],
            "started_at": values["started_at"],
            "completed_at": values["completed_at"],
            "updated_at": values["updated_at"],
            "run_id": run_id,
        },
    )


def create_or_reuse_refresh_run(*, payload: dict[str, Any]) -> dict[str, Any]:
    community_id = str(payload.get("community_id") or "").strip()
    season_id = str(payload.get("season_id") or "").strip()
    period_key = str(payload.get("period_key") or "").strip()
    subreddit = _normalize_subreddit(str(payload.get("subreddit") or ""))
    if not community_id:
        raise ValueError("community_id is required")
    if not season_id:
        raise ValueError("season_id is required")
    if not period_key:
        raise ValueError("period_key is required")
    if not subreddit:
        raise ValueError("subreddit is required")

    existing = pg.fetch_one(
        """
        select *
        from social.reddit_refresh_runs
        where community_id = %s
          and season_id = %s
          and period_key = %s
          and status in ('queued', 'running')
        order by created_at desc
        limit 1
        """,
        [community_id, season_id, period_key],
    )
    if existing:
        existing["reused"] = True
        return existing

    row = pg.fetch_one(
        """
        insert into social.reddit_refresh_runs (
          community_id,
          season_id,
          period_key,
          subreddit,
          status,
          request_payload,
          diagnostics,
          created_at,
          updated_at
        )
        values (%s, %s, %s, %s, 'queued', %s::jsonb, '{}'::jsonb, now(), now())
        returning *
        """,
        [community_id, season_id, period_key, subreddit, json.dumps(payload, ensure_ascii=True)],
    )
    if not row:
        raise RuntimeError("Failed to create refresh run")
    row["reused"] = False
    return row


def _discover_window(payload: dict[str, Any]) -> dict[str, Any]:
    subreddit = _normalize_subreddit(str(payload.get("subreddit") or ""))
    show_name = str(payload.get("show_name") or "").strip()
    show_aliases = [str(item) for item in (payload.get("show_aliases") or []) if isinstance(item, str)]
    cast_names = [str(item) for item in (payload.get("cast_names") or []) if isinstance(item, str)]
    analysis_flares = [str(item) for item in (payload.get("analysis_flares") or []) if isinstance(item, str)]
    analysis_all_flares = [str(item) for item in (payload.get("analysis_all_flares") or []) if isinstance(item, str)]
    force_include_flares = [str(item) for item in (payload.get("force_include_flares") or []) if isinstance(item, str)]
    seed_post_urls = [str(item) for item in (payload.get("seed_post_urls") or []) if isinstance(item, str)]

    normalized_analysis_flares: list[str] = []
    for value in analysis_flares:
        normalized = _collapse_whitespace(value)
        if normalized:
            normalized_analysis_flares.append(normalized)
    normalized_analysis_all_flares: list[str] = []
    for value in analysis_all_flares:
        normalized = _collapse_whitespace(value)
        if normalized:
            normalized_analysis_all_flares.append(normalized)
    normalized_force_include_flares: list[str] = []
    for value in force_include_flares:
        normalized = _collapse_whitespace(value)
        if normalized:
            normalized_force_include_flares.append(normalized)
    tracked_flairs = [
        *normalized_analysis_all_flares,
        *normalized_analysis_flares,
        *normalized_force_include_flares,
    ]

    period_start = _parse_iso(payload.get("period_start"))
    period_end = _parse_iso(payload.get("period_end"))
    if period_start and period_end and period_start > period_end:
        raise ValueError("period_start must be before period_end")

    exhaustive = bool(payload.get("exhaustive_window")) and (period_start is not None or period_end is not None)
    search_backfill_enabled = bool(payload.get("search_backfill"))

    sort_modes = (
        payload.get("sort_modes")
        if isinstance(payload.get("sort_modes"), list)
        else ["new", "hot", "top"]
    )
    normalized_sorts = [
        str(sort).strip().lower()
        for sort in sort_modes
        if str(sort).strip().lower() in {"new", "hot", "top"}
    ]
    if not normalized_sorts:
        normalized_sorts = ["new", "hot", "top"]

    limit_per_mode = _env_int("REDDIT_SAMPLE_LIMIT_PER_MODE", 35, minimum=1, maximum=100)
    try:
        requested_limit = int(payload.get("limit_per_mode") or 35)
        limit_per_mode = max(1, min(100, requested_limit))
    except Exception:  # noqa: BLE001
        pass

    max_pages = _env_int("REDDIT_EXHAUSTIVE_MAX_PAGES", REDDIT_MAX_PAGES_DEFAULT, minimum=10, maximum=1000)
    try:
        requested_pages = int(payload.get("max_pages") or max_pages)
        max_pages = max(10, min(1000, requested_pages))
    except Exception:  # noqa: BLE001
        pass

    listing_rows: list[dict[str, Any]] = []
    listing_pages = 0
    window_exhaustive_complete: bool | None = None
    terms = _build_terms(show_name, show_aliases)
    cast_terms = _build_cast_terms(cast_names)
    diagnostics = {
        "successful_sorts": [],
        "failed_sorts": [],
        "rate_limited_sorts": [],
    }

    if exhaustive:
        listing_rows, listing_pages, window_exhaustive_complete = _fetch_new_window_exhaustive(
            subreddit=subreddit,
            period_start=period_start,
            period_end=period_end,
            max_pages=max_pages,
        )
        diagnostics["successful_sorts"] = ["new"]
    else:
        listing_rows, sort_diag = _fetch_sample_sorts(
            subreddit=subreddit,
            sort_modes=normalized_sorts,
            limit_per_mode=limit_per_mode,
        )
        listing_pages = len(sort_diag["successful_sorts"])
        diagnostics.update(sort_diag)

    listing_rows = _filter_by_window(listing_rows, period_start=period_start, period_end=period_end)

    search_backfill_diag: dict[str, Any] | None = None
    if exhaustive and search_backfill_enabled and tracked_flairs:
        backfill_rows, search_backfill_diag = _fetch_search_backfill(
            subreddit=subreddit,
            tracked_flairs=tracked_flairs,
            show_aliases=show_aliases,
            show_terms=terms,
            period_start=period_start,
            period_end=period_end,
            max_pages_per_query=_env_int(
                "REDDIT_BACKFILL_MAX_PAGES_PER_QUERY",
                REDDIT_MAX_SEARCH_PAGES_PER_QUERY_DEFAULT,
                minimum=1,
                maximum=50,
            ),
            max_total_queries=_env_int(
                "REDDIT_BACKFILL_MAX_QUERIES",
                REDDIT_MAX_BACKFILL_QUERIES_DEFAULT,
                minimum=1,
                maximum=30,
            ),
        )
        backfill_rows = _filter_by_window(backfill_rows, period_start=period_start, period_end=period_end)
        listing_rows.extend(backfill_rows)

    seed_diag = {
        "seed_urls_requested": 0,
        "seed_urls_parsed": 0,
        "seed_urls_ingested": 0,
        "seed_urls_failed": 0,
        "seed_ingested_post_ids": [],
        "seed_failed_post_ids": [],
        "seed_failed_urls": [],
    }
    if seed_post_urls:
        seeded_rows, seed_diag = _fetch_seed_rows(seed_post_urls)
        seeded_rows = _filter_by_window(seeded_rows, period_start=period_start, period_end=period_end)
        listing_rows.extend(seeded_rows)

    merged_rows = _merge_by_post_id(listing_rows)
    matched_rows, hints, tracked_rows = _apply_match_metadata(
        rows=merged_rows,
        subreddit=subreddit,
        terms=terms,
        cast_terms=cast_terms,
        analysis_flares=normalized_analysis_flares,
        analysis_all_flares=normalized_analysis_all_flares,
        force_include_flares=normalized_force_include_flares,
        show_focused=bool(payload.get("is_show_focused")),
    )

    result = {
        "subreddit": subreddit,
        "fetched_at": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
        "collection_mode": "exhaustive_window" if exhaustive else "sample",
        "sources_fetched": diagnostics.get("successful_sorts") or [],
        "successful_sorts": diagnostics.get("successful_sorts") or [],
        "failed_sorts": diagnostics.get("failed_sorts") or [],
        "rate_limited_sorts": diagnostics.get("rate_limited_sorts") or [],
        "listing_pages_fetched": listing_pages,
        "max_pages_applied": max_pages,
        "window_exhaustive_complete": window_exhaustive_complete,
        "search_backfill": search_backfill_diag,
        "seed_urls": seed_diag,
        "totals": {
            "fetched_rows": len(merged_rows),
            "matched_rows": len(matched_rows),
            "tracked_flair_rows": tracked_rows,
        },
        "window_start": _iso_utc(period_start),
        "window_end": _iso_utc(period_end),
        "terms": terms,
        "hints": hints,
        "threads": matched_rows,
    }
    return result


def execute_refresh_run(run_id: str) -> dict[str, Any]:
    run = pg.fetch_one(
        """
        select id, community_id, season_id, period_key, subreddit, request_payload
        from social.reddit_refresh_runs
        where id = %s::uuid
        """,
        [run_id],
    )
    if not run:
        raise ValueError("Refresh run not found")

    request_payload = run.get("request_payload") if isinstance(run.get("request_payload"), dict) else {}
    _update_run(run_id, status="running", set_started=True)

    try:
        result = _discover_window(request_payload)

        comment_errors = 0
        comments_upserted = 0
        fetch_comments = bool(request_payload.get("fetch_comments"))
        comment_posts_cap = _env_int(
            "REDDIT_COMMENTS_MAX_POSTS_PER_RUN",
            REDDIT_MAX_COMMENTS_POSTS_PER_RUN_DEFAULT,
            minimum=0,
            maximum=500,
        )
        target_threads = result.get("threads") if isinstance(result.get("threads"), list) else []
        comment_targets = (
            target_threads[:comment_posts_cap]
            if fetch_comments and comment_posts_cap > 0
            else []
        )

        with pg.db_connection() as conn:
            _upsert_posts(
                [
                    {
                        **thread,
                        "subreddit": result.get("subreddit"),
                        "selftext": thread.get("text"),
                    }
                    for thread in target_threads
                ],
                conn=conn,
            )

        pending_comment_rows: list[dict[str, Any]] = []
        with pg.db_connection() as conn:
            _replace_period_matches(
                community_id=str(run.get("community_id")),
                season_id=str(run.get("season_id")),
                period_key=str(run.get("period_key")),
                period_start=_parse_iso(result.get("window_start")),
                period_end=_parse_iso(result.get("window_end")),
                run_id=run_id,
                rows=target_threads,
                conn=conn,
            )

        for thread in comment_targets:
            post_id = str(thread.get("reddit_post_id") or "").strip()
            if not post_id:
                continue
            try:
                comments = _fetch_post_comments_tree(post_id)
                if comments:
                    pending_comment_rows.extend(comments)
                if len(pending_comment_rows) >= 2_000:
                    with pg.db_connection() as conn:
                        comments_upserted += _upsert_comments(pending_comment_rows, conn=conn)
                    pending_comment_rows.clear()
            except Exception as exc:  # noqa: BLE001
                comment_errors += 1
                logger.warning("[reddit_refresh_comments_failed] post_id=%s error=%s", post_id, exc)

        if pending_comment_rows:
            with pg.db_connection() as conn:
                comments_upserted += _upsert_comments(pending_comment_rows, conn=conn)

        search_backfill = (
            result.get("search_backfill")
            if isinstance(result.get("search_backfill"), dict)
            else None
        )
        seed_urls = result.get("seed_urls") if isinstance(result.get("seed_urls"), dict) else None
        incomplete_listing = (
            result.get("collection_mode") == "exhaustive_window"
            and result.get("window_exhaustive_complete") is False
        )
        incomplete_backfill = bool(search_backfill) and bool(search_backfill.get("complete") is False)
        status = "partial" if incomplete_listing or incomplete_backfill else "completed"

        diagnostics = {
            "listing_pages_fetched": result.get("listing_pages_fetched"),
            "max_pages_applied": result.get("max_pages_applied"),
            "window_exhaustive_complete": result.get("window_exhaustive_complete"),
            "search_backfill": search_backfill,
            "seed_urls": seed_urls,
            "window_start": result.get("window_start"),
            "window_end": result.get("window_end"),
            "terms": result.get("terms"),
            "hints": result.get("hints"),
            "comments": {
                "enabled": fetch_comments,
                "attempted_posts": len(comment_targets),
                "upserted_rows": comments_upserted,
                "errors": comment_errors,
            },
            "result": result,
        }

        _update_run(
            run_id,
            status=status,
            diagnostics=diagnostics,
            total_rows=int(result.get("totals", {}).get("fetched_rows") or 0),
            matched_rows=int(result.get("totals", {}).get("matched_rows") or 0),
            tracked_flair_rows=int(result.get("totals", {}).get("tracked_flair_rows") or 0),
            set_completed=True,
        )
        return get_refresh_run(run_id)
    except Exception as exc:  # noqa: BLE001
        logger.exception("[reddit_refresh_failed] run_id=%s", run_id)
        _update_run(
            run_id,
            status="failed",
            diagnostics={"error_type": exc.__class__.__name__},
            error_message=str(exc),
            set_completed=True,
        )
        raise


def get_refresh_run(run_id: str) -> dict[str, Any]:
    row = pg.fetch_one(
        """
        select id,
               community_id,
               season_id,
               period_key,
               subreddit,
               status,
               request_payload,
               diagnostics,
               error_message,
               total_rows,
               matched_rows,
               tracked_flair_rows,
               started_at,
               completed_at,
               created_at,
               updated_at
        from social.reddit_refresh_runs
        where id = %s::uuid
        """,
        [run_id],
    )
    if not row:
        raise ValueError("Refresh run not found")

    active_counts = pg.fetch_one(
        """
        select
          count(*) filter (where status = 'running') as running_total,
          count(*) filter (where status = 'queued') as queued_total,
          count(*) filter (where status = 'queued' and created_at < %s::timestamptz) as queued_ahead
        from social.reddit_refresh_runs
        where status in ('queued', 'running')
        """,
        [row.get("created_at")],
    ) or {}

    running_total = _safe_int(active_counts.get("running_total"))
    queued_total = _safe_int(active_counts.get("queued_total"))
    this_run_is_running = 1 if str(row.get("status") or "").strip().lower() == "running" else 0
    this_run_is_queued = 1 if str(row.get("status") or "").strip().lower() == "queued" else 0

    diagnostics = row.get("diagnostics") if isinstance(row.get("diagnostics"), dict) else {}
    payload = diagnostics.get("result") if isinstance(diagnostics.get("result"), dict) else None

    return {
        "run_id": row.get("id"),
        "community_id": row.get("community_id"),
        "season_id": row.get("season_id"),
        "period_key": row.get("period_key"),
        "subreddit": row.get("subreddit"),
        "status": row.get("status"),
        "error": row.get("error_message"),
        "totals": {
            "fetched_rows": _safe_int(row.get("total_rows")),
            "matched_rows": _safe_int(row.get("matched_rows")),
            "tracked_flair_rows": _safe_int(row.get("tracked_flair_rows")),
        },
        "queue": {
            "running_total": running_total,
            "queued_total": queued_total,
            "other_running": max(0, running_total - this_run_is_running),
            "other_queued": max(0, queued_total - this_run_is_queued),
            "queued_ahead": _safe_int(active_counts.get("queued_ahead")),
        },
        "diagnostics": diagnostics,
        "discovery": payload,
        "started_at": _iso_utc(_parse_iso(row.get("started_at"))),
        "completed_at": _iso_utc(_parse_iso(row.get("completed_at"))),
        "created_at": _iso_utc(_parse_iso(row.get("created_at"))),
        "updated_at": _iso_utc(_parse_iso(row.get("updated_at"))),
    }
