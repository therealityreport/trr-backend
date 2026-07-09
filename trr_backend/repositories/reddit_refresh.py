"""Backend-owned Reddit refresh jobs and canonical persistence for period windows."""

from __future__ import annotations

import json
import logging
import os
import random
import re
import socket
import threading
import time
from collections import Counter
from collections.abc import Callable, Iterator, Mapping
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, date, datetime, timedelta
from datetime import time as dt_time
from hashlib import sha1
from html import unescape
from typing import Any
from zoneinfo import ZoneInfo

import requests
from psycopg2.extras import Json

from trr_backend.db import pg
from trr_backend.job_plane import canonical_execution_mode, execution_backend_canonical, execution_owner_label
from trr_backend.modal_dispatch import (
    dispatch_reddit_refresh,
    get_modal_reddit_runtime_health,
    modal_dispatch_ready,
    modal_execution_metadata,
    modal_reddit_refresh_function_name,
)

logger = logging.getLogger(__name__)

REDDIT_USER_AGENT_DEFAULT = "TRRBackendRedditRefresh/1.0 (+https://thereality.report)"
REDDIT_TIMEOUT_SECONDS_DEFAULT = 20
REDDIT_MAX_HTTP_RETRIES_DEFAULT = 5
REDDIT_PAGE_COOLDOWN_SECONDS_DEFAULT = 0.05
REDDIT_RATE_LIMIT_DELAY_SECONDS_DEFAULT = 3.5
REDDIT_MAX_PAGES_DEFAULT = 10_000
REDDIT_MAX_SEARCH_PAGES_PER_QUERY_DEFAULT = 20
REDDIT_MAX_BACKFILL_QUERIES_DEFAULT = 12
REDDIT_MAX_COMMENTS_POSTS_PER_RUN_DEFAULT = 60
REDDIT_COMMENT_TREE_DEPTH_DEFAULT = 12
REDDIT_COMMENT_LIMIT_DEFAULT = 500
REDDIT_REFRESH_STALE_QUEUED_SECONDS_DEFAULT = 300
REDDIT_REFRESH_STALE_RUNNING_SECONDS_DEFAULT = 1200
REDDIT_REFRESH_ORPHANED_QUEUED_REUSE_GRACE_SECONDS_DEFAULT = 300
REDDIT_ADAPTIVE_DEEP_MAX_PAGES = 10_000
REDDIT_ADAPTIVE_DEEP_MAX_BACKFILL_QUERIES = 30
REDDIT_ADAPTIVE_DEEP_MAX_BACKFILL_PAGES_PER_QUERY = 50
REDDIT_REFRESH_CLAIM_LEASE_SECONDS_DEFAULT = 300
REDDIT_BACKFILL_OPERATION_TYPE = "admin_reddit_refresh_backfill"
REDDIT_BACKFILL_POLL_SECONDS_DEFAULT = 2.0
REDDIT_ANALYTICS_TIMEZONE = "America/New_York"

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
HTML_ANCHOR_TAG_RE = re.compile(r"</?a(?:\s+[^>]*)?>", flags=re.IGNORECASE)
HTML_HREF_RE = re.compile(r"""href=(["'])(.*?)\1""", flags=re.IGNORECASE)
REDDIT_MEDIA_URL_RE = re.compile(
    r"""https?://[^\s\)"\]<>'`]+?\.(?:jpg|jpeg|png|gif|webp|mp4)(?:\?[^\s\)"\]<>'`]*)?""",
    flags=re.IGNORECASE,
)

HINT_STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "by",
    "for",
    "from",
    "has",
    "have",
    "in",
    "is",
    "it",
    "of",
    "on",
    "or",
    "that",
    "the",
    "this",
    "to",
    "with",
    "episode",
    "episodes",
    "season",
    "thread",
    "threads",
    "discussion",
    "discussions",
    "live",
    "weekly",
    "trailer",
    "preview",
    "post",
    "posts",
}


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


def _claim_lease_seconds() -> int:
    return _env_int(
        "REDDIT_REFRESH_CLAIM_LEASE_SECONDS",
        REDDIT_REFRESH_CLAIM_LEASE_SECONDS_DEFAULT,
        minimum=30,
        maximum=86_400,
    )


def _default_worker_id() -> str:
    return f"reddit-refresh:{socket.gethostname()}:{os.getpid()}"


def _base_progress_snapshot() -> dict[str, Any]:
    return {
        "stage": "discovering_posts",
        "listing_pages_fetched": 0,
        "search_pages_fetched": 0,
        "rows_discovered_raw": 0,
        "rows_matched": 0,
        "comments_targets_total": 0,
        "comments_targets_done": 0,
        "comments_rows_upserted": 0,
        "detail_posts_total": 0,
        "detail_posts_done": 0,
        "comments_upserted": 0,
        "media_queued": 0,
        "media_mirrored": 0,
        "updated_at": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
    }


def _build_terminal_summary(
    *,
    mode: str,
    status: str,
    progress: dict[str, Any],
    error_count: int = 0,
    force_rescrape: bool | None = None,
) -> dict[str, Any]:
    summary = {
        "mode": mode,
        "status": status,
        "stage": str(progress.get("stage") or "unknown"),
        "rows_matched": _safe_int(progress.get("rows_matched")),
        "comments_rows_upserted": _safe_int(progress.get("comments_rows_upserted")),
        "detail_posts_total": _safe_int(progress.get("detail_posts_total")),
        "detail_posts_done": _safe_int(progress.get("detail_posts_done")),
        "comments_upserted": _safe_int(progress.get("comments_upserted")),
        "media_queued": _safe_int(progress.get("media_queued")),
        "media_mirrored": _safe_int(progress.get("media_mirrored")),
        "error_count": max(0, int(error_count)),
        "updated_at": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
    }
    if force_rescrape is not None:
        summary["force_rescrape"] = bool(force_rescrape)
    return summary


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


def _normalize_string_list(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    deduped: dict[str, str] = {}
    for raw in values:
        if not isinstance(raw, str):
            continue
        normalized = raw.strip()
        if not normalized:
            continue
        key = normalized.lower()
        if key in deduped:
            continue
        deduped[key] = normalized
    return [deduped[key] for key in sorted(deduped.keys())]


def _is_canonical_reddit_container_key(value: str | None) -> bool:
    normalized = str(value or "").strip().lower()
    if normalized in {"period-preseason", "period-postseason"}:
        return True
    return bool(re.fullmatch(r"episode-\d+", normalized))


def _canonical_reddit_container_sort_key(value: str | None) -> tuple[int, int]:
    normalized = str(value or "").strip().lower()
    if normalized == "period-preseason":
        return (0, 0)
    if normalized == "period-postseason":
        return (2, 0)
    match = re.fullmatch(r"episode-(\d+)", normalized)
    if match:
        return (1, int(match.group(1)))
    return (9, 0)


def _canonical_reddit_container_keys_for_season(season_id: str) -> list[str]:
    rows = pg.fetch_all(
        """
        select distinct episode_number
        from core.episodes
        where season_id = %s
          and episode_number is not null
          and episode_number >= 1
        order by episode_number asc
        """,
        [season_id],
    )
    episode_keys = [
        f"episode-{int(row.get('episode_number'))}"
        for row in rows
        if isinstance(row.get("episode_number"), int) and int(row.get("episode_number")) >= 1
    ]
    if not episode_keys:
        return ["period-preseason", "episode-1", "period-postseason"]
    return ["period-preseason", *episode_keys, "period-postseason"]


def _raw_reddit_container_key_sql(*, period_key_expr: str) -> str:
    lowered = f"lower(coalesce({period_key_expr}, ''))"
    return f"""
        case
          when {lowered} in ('period-preseason', 'period-postseason') then {lowered}
          when {lowered} ~ '^episode-[0-9]+$' then {lowered}
          when {lowered} ~ '^community:[^:]+:season:[^:]+:container:[a-z0-9-]+$'
            then substring({lowered} from 'container:([a-z0-9-]+)$')
          else null
        end
    """


def _canonical_reddit_match_window_ranges_for_season(season_id: str) -> list[dict[str, str]]:
    zone = ZoneInfo(REDDIT_ANALYTICS_TIMEZONE)
    rows = pg.fetch_all(
        """
        select episode_number, air_date
        from core.episodes
        where season_id = %s
          and air_date is not null
          and episode_number is not null
        order by episode_number asc, air_date asc
        """,
        [season_id],
    )
    episode_starts: list[tuple[int, datetime]] = []
    seen_numbers: set[int] = set()
    for row in rows:
        raw_num = row.get("episode_number")
        air_date = row.get("air_date")
        if not isinstance(raw_num, int) or raw_num < 1 or not isinstance(air_date, date):
            continue
        if raw_num in seen_numbers:
            continue
        seen_numbers.add(raw_num)
        episode_starts.append(
            (
                raw_num,
                datetime.combine(air_date, dt_time.min, tzinfo=zone).astimezone(UTC),
            )
        )
    if not episode_starts:
        return []

    ranges: list[dict[str, str]] = []
    preseason_start = (episode_starts[0][1] - timedelta(days=45)).isoformat()
    ranges.append(
        {
            "container_key": "period-preseason",
            "start": preseason_start,
            "end": episode_starts[0][1].isoformat(),
        }
    )
    for index, (episode_number, start_utc) in enumerate(episode_starts):
        next_start = episode_starts[index + 1][1] if index + 1 < len(episode_starts) else start_utc + timedelta(days=7)
        if next_start <= start_utc:
            next_start = start_utc + timedelta(days=7)
        ranges.append(
            {
                "container_key": f"episode-{episode_number}",
                "start": start_utc.isoformat(),
                "end": next_start.isoformat(),
            }
        )
    last_end = datetime.fromisoformat(ranges[-1]["end"])
    ranges.append(
        {
            "container_key": "period-postseason",
            "start": last_end.isoformat(),
            "end": (last_end + timedelta(days=7)).isoformat(),
        }
    )
    return ranges


def _canonical_reddit_match_container_key_sql(
    *,
    season_id: str | None,
    period_key_expr: str,
    period_start_expr: str,
    period_end_expr: str,
    posted_at_expr: str,
) -> str:
    direct_container_sql = _raw_reddit_container_key_sql(period_key_expr=period_key_expr)
    normalized_season_id = str(season_id or "").strip()
    if not normalized_season_id:
        return f"coalesce({direct_container_sql}, 'unmapped')"

    window_ranges = _canonical_reddit_match_window_ranges_for_season(normalized_season_id)
    if not window_ranges:
        return f"coalesce({direct_container_sql}, 'unmapped')"

    fallback_clauses: list[str] = []
    for window in window_ranges:
        start = window["start"]
        end = window["end"]
        container_key = window["container_key"]
        fallback_clauses.append(
            f"""
            when {period_start_expr} is not null
             and {period_end_expr} is not null
             and {period_start_expr} >= timestamptz '{start}'
             and {period_end_expr} <= timestamptz '{end}'
            then '{container_key}'
            """
        )
        fallback_clauses.append(
            f"""
            when {posted_at_expr} is not null
             and {posted_at_expr} >= timestamptz '{start}'
             and {posted_at_expr} < timestamptz '{end}'
            then '{container_key}'
            """
        )
    fallback_sql = "\n".join(fallback_clauses)
    return f"""
        coalesce(
          {direct_container_sql},
          case
            {fallback_sql}
            else 'unmapped'
          end
        )
    """


def _canonical_reddit_container_key_sql(
    *,
    period_key_expr: str,
    request_payload_expr: str | None = None,
) -> str:
    request_container_expr = "null"
    request_stable_expr = "null"
    if request_payload_expr:
        request_container_expr = f"nullif(lower(coalesce({request_payload_expr}->>'container_key', '')), '')"
        request_stable_expr = f"nullif(lower(coalesce({request_payload_expr}->>'period_stable_key', '')), '')"
    return f"""
        lower(
          coalesce(
            {request_container_expr},
            {request_stable_expr},
            nullif({period_key_expr}, '')
          )
        )
    """


def _build_run_config_hash(payload: dict[str, Any]) -> str:
    canonical_payload = {
        "mode": str(payload.get("mode") or "sync_posts").strip().lower() or "sync_posts",
        "subreddit": _normalize_subreddit(str(payload.get("subreddit") or "")),
        "coverage_mode": _normalize_coverage_mode(payload.get("coverage_mode")),
        "max_pages": _coerce_int(payload.get("max_pages"), default=0, minimum=0, maximum=10_000),
        "max_backfill_queries": _coerce_int(
            payload.get("max_backfill_queries"),
            default=0,
            minimum=0,
            maximum=1_000,
        ),
        "max_backfill_pages_per_query": _coerce_int(
            payload.get("max_backfill_pages_per_query"),
            default=0,
            minimum=0,
            maximum=1_000,
        ),
        "search_backfill": bool(payload.get("search_backfill")),
        "exhaustive_window": bool(payload.get("exhaustive_window")),
        "fetch_comments": bool(payload.get("fetch_comments")),
        "comment_delta_only": bool(payload.get("comment_delta_only", True)),
        "force_rescrape": bool(payload.get("force_rescrape")),
        "preserve_existing_assignments": bool(payload.get("preserve_existing_assignments", True)),
        "period_start": _iso_utc(_parse_iso(payload.get("period_start"))),
        "period_end": _iso_utc(_parse_iso(payload.get("period_end"))),
        "show_name": _normalize_text(str(payload.get("show_name") or "")),
        "show_aliases": _normalize_string_list(payload.get("show_aliases")),
        "cast_names": _normalize_string_list(payload.get("cast_names")),
        "analysis_flairs": _normalize_string_list(payload.get("analysis_flairs")),
        "analysis_all_flairs": _normalize_string_list(payload.get("analysis_all_flairs")),
        "force_include_flairs": _normalize_string_list(payload.get("force_include_flairs")),
        "seed_post_urls": _normalize_string_list(payload.get("seed_post_urls")),
        "sort_modes": _normalize_string_list(payload.get("sort_modes")),
        "limit_per_mode": _coerce_int(payload.get("limit_per_mode"), default=0, minimum=0, maximum=1_000),
    }
    canonical_json = json.dumps(canonical_payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return sha1(canonical_json.encode("utf-8")).hexdigest()


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


def _derive_refresh_run_phase(diagnostics: dict[str, Any], status: str) -> str | None:
    progress = diagnostics.get("progress") if isinstance(diagnostics.get("progress"), dict) else {}
    phase = str(progress.get("stage") or diagnostics.get("phase") or "").strip().lower()
    if phase:
        return phase
    normalized_status = str(status or "").strip().lower()
    if normalized_status in {"queued", "running", "cancelling"}:
        return normalized_status
    return None


def _derive_failure_reason_code(
    *,
    status: str,
    diagnostics: dict[str, Any],
    error_message: str | None,
    stalled: bool,
) -> str | None:
    direct_code = str(diagnostics.get("failure_reason_code") or "").strip().lower()
    if direct_code:
        return direct_code
    normalized_error = str(error_message or "").strip().lower()
    normalized_status = str(status or "").strip().lower()
    if "queued reddit refresh run expired before execution" in normalized_error:
        return "stale_queue"
    if "reddit request failed (403)" in normalized_error:
        return "reddit_http_403"
    if normalized_status == "partial" and str(diagnostics.get("status_resolution") or "").strip().lower() in {
        "strict_completeness",
        "coverage_incomplete",
    }:
        final_completeness = diagnostics.get("final_completeness")
        if isinstance(final_completeness, dict):
            listing_complete = final_completeness.get("listing_complete")
            backfill_complete = final_completeness.get("backfill_complete")
            if listing_complete is False or backfill_complete is False:
                return "coverage_incomplete"
    if stalled and normalized_status in {"queued", "running", "cancelling"}:
        return "stalled_heartbeat"
    return None


def _default_operator_hint(reason_code: str | None, *, status: str) -> str | None:
    if reason_code == "reddit_http_403":
        return (
            "Reddit blocked the live scrape for this window. "
            "Showing cached posts when available and continuing season sync."
        )
    if reason_code == "worker_unavailable":
        return "No healthy remote worker or dispatcher is available right now. Retry after worker health is restored."
    if reason_code == "stale_queue":
        return "This queued Reddit refresh never started and was expired. Start the sync again."
    if reason_code == "stalled_heartbeat":
        normalized_status = str(status or "").strip().lower()
        if normalized_status == "queued":
            return "This queued Reddit refresh appears stranded in the backend queue. Retry the window sync."
        return "This Reddit refresh stopped reporting heartbeats and appears stalled. Retry the window sync."
    if reason_code == "coverage_incomplete":
        return (
            "Posts were stored for this Reddit window, but the crawl could not prove exhaustive coverage. "
            "Analytics remain usable while this container stays marked partial."
        )
    return None


def _derive_operator_hint(
    *,
    status: str,
    diagnostics: dict[str, Any],
    error_message: str | None,
    stalled: bool,
) -> str | None:
    direct_hint = str(diagnostics.get("operator_hint") or "").strip()
    if direct_hint:
        return direct_hint
    return _default_operator_hint(
        _derive_failure_reason_code(
            status=status,
            diagnostics=diagnostics,
            error_message=error_message,
            stalled=stalled,
        ),
        status=status,
    )


def _is_orphaned_queued_run(
    row: Mapping[str, Any],
    *,
    grace_seconds: int,
) -> bool:
    status = str(row.get("status") or "").strip().lower()
    if status != "queued":
        return False
    if str(row.get("claimed_by_worker_id") or "").strip():
        return False
    if str(row.get("claim_token") or "").strip():
        return False
    if _parse_iso(row.get("heartbeat_at")) is not None:
        return False
    reference_time = _parse_iso(row.get("updated_at")) or _parse_iso(row.get("created_at"))
    if reference_time is None:
        return False
    return (datetime.now(tz=UTC) - reference_time) >= timedelta(seconds=grace_seconds)


def _collect_partial_failures(
    *,
    status: str,
    diagnostics: dict[str, Any],
    error_message: str | None,
) -> list[dict[str, Any]]:
    failures: list[dict[str, Any]] = []
    normalized_status = str(status or "").strip().lower()
    normalized_error = str(error_message or "").strip()
    if normalized_error and normalized_status in {"partial", "failed"}:
        failure: dict[str, Any] = {
            "phase": _derive_refresh_run_phase(diagnostics, normalized_status),
            "reason": normalized_error,
        }
        reason_code = str(diagnostics.get("failure_reason_code") or "").strip().lower()
        if reason_code:
            failure["failure_reason_code"] = reason_code
        operator_hint = str(diagnostics.get("operator_hint") or "").strip()
        if operator_hint:
            failure["operator_hint"] = operator_hint
        failures.append(failure)

    final_completeness = (
        diagnostics.get("final_completeness") if isinstance(diagnostics.get("final_completeness"), dict) else {}
    )
    if final_completeness.get("listing_complete") is False:
        failures.append({"phase": "listing", "reason": "listing_incomplete"})
    if final_completeness.get("backfill_complete") is False:
        failures.append({"phase": "search_backfill", "reason": "backfill_incomplete"})

    result_payload = diagnostics.get("result") if isinstance(diagnostics.get("result"), dict) else {}
    search_backfill = (
        result_payload.get("search_backfill") if isinstance(result_payload.get("search_backfill"), dict) else {}
    )
    query_diagnostics = (
        search_backfill.get("query_diagnostics") if isinstance(search_backfill.get("query_diagnostics"), list) else []
    )
    for query_diag in query_diagnostics:
        if not isinstance(query_diag, dict):
            continue
        if query_diag.get("complete") is True and not query_diag.get("error"):
            continue
        failures.append(
            {
                "phase": str(query_diag.get("query_kind") or "search_backfill"),
                "reason": str(query_diag.get("error") or "incomplete_query"),
                "query": str(query_diag.get("query") or "").strip() or None,
                "label": str(query_diag.get("flair") or "").strip() or None,
            }
        )

    detail_errors = diagnostics.get("errors") if isinstance(diagnostics.get("errors"), list) else []
    for detail_error in detail_errors:
        if not isinstance(detail_error, dict):
            continue
        reason = str(detail_error.get("error") or detail_error.get("reason") or "").strip()
        if not reason:
            continue
        failures.append(
            {
                "phase": str(detail_error.get("phase") or "details"),
                "reason": reason,
                "reddit_post_id": str(detail_error.get("reddit_post_id") or "").strip() or None,
            }
        )

    deduped: list[dict[str, Any]] = []
    seen: set[str] = set()
    for failure in failures:
        key = json.dumps(failure, sort_keys=True, default=str)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(failure)
    return deduped


def _is_refresh_run_stalled(
    *,
    status: str,
    heartbeat_at: datetime | None,
    updated_at: datetime | None,
    created_at: datetime | None,
) -> bool:
    normalized_status = str(status or "").strip().lower()
    if normalized_status not in {"queued", "running", "cancelling"}:
        return False
    now_utc = datetime.now(tz=UTC)
    last_activity = heartbeat_at or updated_at or created_at
    if last_activity is None:
        return False
    age_seconds = max(0.0, (now_utc - last_activity).total_seconds())
    if normalized_status == "queued":
        return age_seconds >= REDDIT_REFRESH_STALE_QUEUED_SECONDS_DEFAULT
    return age_seconds >= REDDIT_REFRESH_STALE_RUNNING_SECONDS_DEFAULT


def _build_refresh_run_meta(row: Mapping[str, Any]) -> dict[str, Any]:
    diagnostics = row.get("diagnostics") if isinstance(row.get("diagnostics"), dict) else {}
    status = str(row.get("status") or "").strip().lower()
    heartbeat_at = _parse_iso(row.get("heartbeat_at"))
    updated_at = _parse_iso(row.get("updated_at"))
    created_at = _parse_iso(row.get("created_at"))
    completed_at = _parse_iso(row.get("completed_at"))
    cache_age_seconds = (
        max(0, int((datetime.now(tz=UTC) - completed_at).total_seconds())) if completed_at is not None else None
    )
    if status == "partial":
        cache_status = "partial"
    elif completed_at is not None:
        cache_status = "stale" if cache_age_seconds is not None and cache_age_seconds > 86_400 else "fresh"
    else:
        cache_status = "miss"
    stalled = _is_refresh_run_stalled(
        status=status,
        heartbeat_at=heartbeat_at,
        updated_at=updated_at,
        created_at=created_at,
    )
    failure_reason_code = _derive_failure_reason_code(
        status=status,
        diagnostics=diagnostics,
        error_message=str(row.get("error_message") or "").strip() or None,
        stalled=stalled,
    )
    operator_hint = _derive_operator_hint(
        status=status,
        diagnostics=diagnostics,
        error_message=str(row.get("error_message") or "").strip() or None,
        stalled=stalled,
    )
    return {
        "phase": _derive_refresh_run_phase(diagnostics, status),
        "partial_failures": _collect_partial_failures(
            status=status,
            diagnostics=diagnostics,
            error_message=str(row.get("error_message") or "").strip() or None,
        ),
        "stalled": stalled,
        "failure_reason_code": failure_reason_code,
        "operator_hint": operator_hint,
        "cache_status": cache_status,
        "cache_age_seconds": cache_age_seconds,
        "run_status": status,
    }


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


def _sanitize_reddit_media_url(value: str | None) -> str:
    normalized = unescape(str(value or "")).strip()
    if not normalized:
        return ""
    normalized = HTML_ANCHOR_TAG_RE.sub("", normalized).split("<", 1)[0].strip()
    return normalized.strip("\"'")


def _reddit_media_type_from_url(value: str) -> str:
    path = str(value or "").split("?", 1)[0].lower()
    return "video" if path.endswith(".mp4") else "image"


def _extract_reddit_media_urls(value: str | None) -> list[tuple[str, str]]:
    text = str(value or "")
    if not text:
        return []

    candidates: list[str] = []
    candidates.extend(match.group(2) for match in HTML_HREF_RE.finditer(text))
    candidates.extend(match.group(0) for match in REDDIT_MEDIA_URL_RE.finditer(text))

    output: list[tuple[str, str]] = []
    seen: set[str] = set()
    for raw in candidates:
        normalized = _sanitize_reddit_media_url(raw)
        if not normalized or normalized in seen:
            continue
        if not REDDIT_MEDIA_URL_RE.fullmatch(normalized):
            continue
        seen.add(normalized)
        output.append((normalized, _reddit_media_type_from_url(normalized)))
    return output


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


def _coerce_int(value: Any, *, default: int, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except Exception:  # noqa: BLE001
        parsed = default
    return max(minimum, min(maximum, parsed))


def _normalize_coverage_mode(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"adaptive_deep", "max_coverage"}:
        return normalized
    return "standard"


def _is_result_incomplete(result: dict[str, Any]) -> tuple[bool, bool]:
    search_backfill = result.get("search_backfill") if isinstance(result.get("search_backfill"), dict) else None
    incomplete_listing = (
        result.get("collection_mode") == "exhaustive_window" and result.get("window_exhaustive_complete") is False
    )
    incomplete_backfill = bool(search_backfill) and bool(search_backfill.get("complete") is False)
    return incomplete_listing, incomplete_backfill


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
        self._state_lock = threading.Lock()
        # Adaptive cooldown: starts at configured minimum, increases on 429s, decays on success
        self._adaptive_cooldown: float = self.page_cooldown
        self._adaptive_cooldown_min: float = self.page_cooldown
        self._adaptive_cooldown_max: float = max(0.5, self.page_cooldown * 10)

        if not self.client_id:
            logger.warning(
                "[reddit_http] OAuth not configured (REDDIT_CLIENT_ID not set) — "
                "rate limits will be ~6x lower than authenticated requests"
            )

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
        with self._state_lock:
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
            with self._state_lock:
                self._oauth_token = token
                self._oauth_expires_at = time.time() + max(60.0, expires_in)
            return token
        except Exception as exc:  # noqa: BLE001
            logger.warning("[reddit_refresh_oauth_exception] %s", exc)
            return None

    def get_json(self, path: str, *, params: dict[str, Any]) -> dict[str, Any]:
        supports_oauth = bool(self.client_id and self.client_secret)
        base_urls = (
            ["https://oauth.reddit.com", "https://www.reddit.com"] if supports_oauth else ["https://www.reddit.com"]
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
                        # Adaptive backoff: increase cooldown on rate limit
                        with self._state_lock:
                            self._adaptive_cooldown = min(
                                self._adaptive_cooldown_max,
                                self._adaptive_cooldown * 2,
                            )
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
                    # Adaptive cooldown: use current adaptive value, decay toward minimum on success
                    with self._state_lock:
                        current_cooldown = self._adaptive_cooldown
                    if current_cooldown > 0:
                        time.sleep(current_cooldown)
                    with self._state_lock:
                        self._adaptive_cooldown = max(
                            self._adaptive_cooldown_min,
                            self._adaptive_cooldown * 0.9,
                        )
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
_column_exists_cache: dict[tuple[str, str, str], bool] = {}

ProgressCallback = Callable[[dict[str, Any]], None]


_COLUMN_EXISTS_SQL = """
    select exists (
      select 1
      from information_schema.columns
      where table_schema = %s
        and table_name = %s
        and column_name = %s
    ) as exists
"""


def _column_exists(schema: str, table: str, column: str, *, conn: Any = None) -> bool:
    key = (schema, table, column)
    cached = _column_exists_cache.get(key)
    if cached is not None:
        return cached
    if conn is not None:
        # Reuse the caller's held connection: acquiring a second pooled
        # connection mid-write exhausts the pool at low TRR_DB_POOL_MAXCONN.
        with pg.db_cursor(conn=conn, label="column-exists") as cur:
            cur.execute(_COLUMN_EXISTS_SQL, [schema, table, column])
            row = cur.fetchone() or {}
    else:
        row = pg.fetch_one(_COLUMN_EXISTS_SQL, [schema, table, column]) or {}
    result = bool(row.get("exists"))
    _column_exists_cache[key] = result
    return result


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
    progress_callback: ProgressCallback | None = None,
) -> tuple[list[dict[str, Any]], int, bool]:
    rows: list[dict[str, Any]] = []
    pages_fetched = 0
    after: str | None = None
    submitted_cursor: str | None = None
    reached_period_start = False
    exhausted_listing = False

    # Producer-consumer: prefetch next page while processing current page.
    # This overlaps network IO + cooldown with CPU-bound row processing.
    prefetch_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="listing_prefetch")

    def _fetch_listing_page(cursor: str | None) -> dict[str, Any]:
        params: dict[str, Any] = {"limit": 100, "raw_json": 1}
        if cursor:
            params["after"] = cursor
        return _HTTP_CLIENT.get_json(f"/r/{subreddit}/new.json", params=params)

    # Kick off first fetch
    pending_future = prefetch_pool.submit(_fetch_listing_page, None)

    try:
        for _ in range(max_pages):
            payload = pending_future.result()
            listing = payload.get("data") if isinstance(payload, dict) else None
            children = listing.get("children") if isinstance(listing, dict) else []
            parsed_rows = _parse_listing_rows(children if isinstance(children, list) else [], source_sort="new")
            pages_fetched += 1

            # Determine the next cursor early so we can prefetch while processing
            after_value = listing.get("after") if isinstance(listing, dict) else None
            after = str(after_value) if after_value else None
            if after is not None and after == submitted_cursor:
                # Reddit repeated the cursor we just fetched with; further
                # requests would return identical pages until max_pages burns.
                logger.warning(
                    "[reddit_refresh] r/%s new-listing repeated after-cursor %s; treating listing as exhausted",
                    subreddit,
                    after,
                )
                exhausted_listing = True
                after = None

            # Prefetch next page while we process current results
            should_stop = False
            if after is not None and pages_fetched < max_pages:
                submitted_cursor = after
                pending_future = prefetch_pool.submit(_fetch_listing_page, after)
            else:
                should_stop = True  # will stop after processing this page

            if parsed_rows:
                rows.extend(parsed_rows)
                if progress_callback:
                    progress_callback(
                        {
                            "listing_pages_fetched": pages_fetched,
                            "listing_rows_fetched": len(rows),
                        }
                    )
                reached_period_start = _window_complete_for_page(
                    rows=parsed_rows,
                    period_start=period_start,
                    reached_period_start=reached_period_start,
                )
                if reached_period_start:
                    break

            if should_stop:
                if after is None:
                    exhausted_listing = True
                    return rows, pages_fetched, bool(period_start is None or reached_period_start or exhausted_listing)
                break
    finally:
        prefetch_pool.shutdown(wait=False)

    if period_start is None:
        return rows, pages_fetched, True
    return rows, pages_fetched, bool(reached_period_start or exhausted_listing)


def _fetch_sample_sorts(
    *,
    subreddit: str,
    sort_modes: list[str],
    limit_per_mode: int,
) -> tuple[list[dict[str, Any]], dict[str, list[str]]]:
    rows: list[dict[str, Any]] = []
    diagnostics = {"successful_sorts": [], "failed_sorts": [], "rate_limited_sorts": []}
    first_error: RedditRefreshError | None = None

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
            if first_error is None:
                first_error = exc
            diagnostics["failed_sorts"].append(sort)
            if exc.status == 429:
                diagnostics["rate_limited_sorts"].append(sort)
            logger.warning(
                "[reddit_refresh_sort_failed] subreddit=%s sort=%s status=%s",
                subreddit,
                sort,
                exc.status,
            )
    if not diagnostics["successful_sorts"]:
        raise first_error or RedditRefreshError("Failed to fetch subreddit threads", status=502)
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
    progress_callback: ProgressCallback | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    query_diagnostics: list[dict[str, Any]] = []

    canonical_seen: set[str] = set()
    # Query tuple = (label, query, query_kind, required_for_completeness).
    # We require exact flair coverage for completeness, while gap-fill queries
    # are optional enhancers and should not force perpetual partial status.
    queries: list[tuple[str, str, str, bool]] = []
    for flair in tracked_flairs:
        canon = to_canonical_flair_key(flair)
        if not canon or canon in canonical_seen:
            continue
        canonical_seen.add(canon)
        # Primary exact flair query.
        queries.append((flair, f'flair:"{flair}"', "flair_exact", True))
        if len(queries) >= max_total_queries:
            break
        # Gap-fill query: phrase search for flair text can recover older posts that flair: search misses.
        queries.append((flair, f'"{flair}"', "flair_phrase", False))
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
        queries.append((alias_text, alias_text, "show_alias_term", False))
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
        queries.append((term_text, f'"{term_text}"', "show_term_phrase", False))
        if len(queries) >= max_total_queries:
            break

    if len(queries) < max_total_queries:
        # Additional listing-style recovery path that avoids subreddit search index gaps.
        queries.append(("top_year", "", "top_year_listing", False))

    # -- Per-query worker (pagination is sequential within each query) --
    def _run_single_query(
        flair: str,
        query: str,
        query_kind: str,
        required: bool,
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        after: str | None = None
        pages = 0
        q_rows: list[dict[str, Any]] = []
        rows_fetched = 0
        rows_in_window = 0
        reached_period_start = False
        exhausted_query = False
        query_error: str | None = None

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

            try:
                payload = _HTTP_CLIENT.get_json(path, params=params)
            except Exception as exc:  # noqa: BLE001
                query_error = str(exc) or exc.__class__.__name__
                break
            listing = payload.get("data") if isinstance(payload, dict) else None
            children = listing.get("children") if isinstance(listing, dict) else []
            source_sort = "top" if query_kind == "top_year_listing" else "new"
            parsed_rows = _parse_listing_rows(children if isinstance(children, list) else [], source_sort=source_sort)
            pages += 1
            if parsed_rows:
                q_rows.extend(parsed_rows)
                rows_fetched += len(parsed_rows)
                reached_period_start = _window_complete_for_page(
                    rows=parsed_rows,
                    period_start=period_start,
                    reached_period_start=reached_period_start,
                )
                filtered = _filter_by_window(parsed_rows, period_start=period_start, period_end=period_end)
                rows_in_window += len(filtered)

            after_value = listing.get("after") if isinstance(listing, dict) else None
            next_after = str(after_value) if after_value else None
            if next_after is not None and next_after == after:
                logger.warning(
                    "[reddit_refresh] r/%s %s repeated after-cursor %s; treating query as exhausted",
                    subreddit,
                    query_kind,
                    next_after,
                )
                exhausted_query = True
                break
            after = next_after
            if after is None:
                exhausted_query = True
                break

        query_complete = bool((period_start is None or reached_period_start or exhausted_query) and query_error is None)
        diag = {
            "flair": flair,
            "query": query,
            "query_kind": query_kind,
            "required": required,
            "pages_fetched": pages,
            "rows_fetched": rows_fetched,
            "rows_in_window": rows_in_window,
            "reached_period_start": reached_period_start,
            "exhausted_results": exhausted_query,
            "complete": query_complete,
            "error": query_error,
        }
        return q_rows, diag

    # -- Run queries in parallel (4 workers) --
    pages_total = 0
    rows_total = 0
    rows_window_total = 0
    all_complete = True
    required_queries_run = 0
    required_queries_complete = 0
    optional_queries_incomplete = 0
    _backfill_lock = threading.Lock()

    backfill_worker_count = min(4, len(queries)) if queries else 1
    with ThreadPoolExecutor(max_workers=backfill_worker_count, thread_name_prefix="backfill") as pool:
        futures = {
            pool.submit(_run_single_query, flair, query, query_kind, required): (
                flair,
                query,
                query_kind,
                required,
            )
            for flair, query, query_kind, required in queries
        }
        for future in as_completed(futures):
            flair, query, query_kind, required = futures[future]
            try:
                q_rows, diag = future.result()
            except Exception as exc:  # noqa: BLE001
                q_rows = []
                diag = {
                    "flair": flair,
                    "query": query,
                    "query_kind": query_kind,
                    "required": required,
                    "pages_fetched": 0,
                    "rows_fetched": 0,
                    "rows_in_window": 0,
                    "reached_period_start": False,
                    "exhausted_results": False,
                    "complete": False,
                    "error": str(exc) or exc.__class__.__name__,
                }
            with _backfill_lock:
                rows.extend(q_rows)
                pages_total += diag["pages_fetched"]
                rows_total += diag["rows_fetched"]
                rows_window_total += diag["rows_in_window"]
                if bool(diag.get("required")):
                    required_queries_run += 1
                    if diag["complete"]:
                        required_queries_complete += 1
                    else:
                        all_complete = False
                elif not diag["complete"]:
                    optional_queries_incomplete += 1
                query_diagnostics.append(diag)
                if progress_callback:
                    progress_callback(
                        {
                            "search_pages_fetched": pages_total,
                            "search_rows_fetched": rows_total,
                        }
                    )

    return rows, {
        "enabled": True,
        "queries_run": len(query_diagnostics),
        "pages_fetched": pages_total,
        "rows_fetched": rows_total,
        "rows_in_window": rows_window_total,
        "required_queries_run": required_queries_run,
        "required_queries_complete": required_queries_complete,
        "optional_queries_incomplete": optional_queries_incomplete,
        "complete": all_complete,
        "query_diagnostics": query_diagnostics,
    }


def _is_hint_token_eligible(token: str) -> bool:
    normalized = str(token or "").strip().lower()
    if len(normalized) < 3:
        return False
    if normalized in HINT_STOPWORDS:
        return False
    if normalized.isnumeric():
        return False
    return True


def _apply_match_metadata(
    *,
    rows: list[dict[str, Any]],
    subreddit: str,
    terms: list[str],
    cast_terms: list[str],
    analysis_flairs: list[str],
    analysis_all_flairs: list[str],
    force_include_flairs: list[str],
    show_focused: bool,
) -> tuple[list[dict[str, Any]], dict[str, list[str]], int]:
    del subreddit  # Not currently needed for scoring, retained for API compatibility.
    scan_keys = {to_canonical_flair_key(flair) for flair in analysis_flairs if to_canonical_flair_key(flair)}
    all_keys = {to_canonical_flair_key(flair) for flair in analysis_all_flairs if to_canonical_flair_key(flair)}
    forced_keys = {to_canonical_flair_key(flair) for flair in force_include_flairs if to_canonical_flair_key(flair)}
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
            forced_flair_match or all_flair_match or scan_flair_match or not has_tracked_flair_rules
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
                if _is_hint_token_eligible(token):
                    include_counter[token] += 1
        else:
            for token in _extract_word_tokens(title):
                if _is_hint_token_eligible(token):
                    exclude_counter[token] += 1
            continue

        # Derive match_type for period_post_matches classification:
        # - show_focused communities include all posts → 'all'
        # - flair-based matches (forced, all) → 'flair'
        # - scan_term (matched by search term + flair) → 'scan'
        # - show_match or no flair rules → 'flair' (default)
        if show_focused:
            match_type = "all"
        elif flair_mode in ("forced", "all"):
            match_type = "flair"
        elif flair_mode == "scan_term":
            match_type = "scan"
        else:
            match_type = "flair"

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
                "match_type": match_type,
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
    base_urls = ["https://oauth.reddit.com", "https://www.reddit.com"] if supports_oauth else ["https://www.reddit.com"]

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
                created_at_utc = datetime.fromtimestamp(float(created_raw), tz=UTC).isoformat().replace("+00:00", "Z")
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


_THUMBNAIL_SKIP_VALUES = {"self", "default", "nsfw", "spoiler", ""}


def _derive_post_type(post: dict[str, Any]) -> str:
    """Derive a post_type classification from Reddit API data."""
    if post.get("poll_data"):
        return "poll"
    if post.get("is_gallery"):
        return "gallery"
    hint = str(post.get("post_hint") or "")
    if post.get("is_video") or hint in ("hosted:video", "rich:video"):
        return "video"
    if hint == "image":
        return "image"
    if hint == "link":
        return "link"
    if post.get("is_self"):
        return "text"
    # External URL that isn't self-post defaults to link
    url = str(post.get("url") or "")
    permalink = str(post.get("permalink") or "")
    if url and permalink and not url.endswith(permalink):
        return "link"
    return "link"


def _upsert_posts(rows: list[dict[str, Any]], *, conn: Any) -> set[str]:
    """Upsert posts into social.reddit_posts. Returns set of discovered flair texts."""
    discovered_flairs: set[str] = set()
    if not rows:
        return discovered_flairs

    # Check which Sprint-3 columns exist (backward compat before migration).
    has_upvote_ratio = _column_exists("social", "reddit_posts", "upvote_ratio", conn=conn)
    has_is_self = _column_exists("social", "reddit_posts", "is_self", conn=conn)
    has_post_type = _column_exists("social", "reddit_posts", "post_type", conn=conn)
    has_thumbnail = _column_exists("social", "reddit_posts", "thumbnail", conn=conn)
    has_media_metadata = _column_exists("social", "reddit_posts", "media_metadata", conn=conn)
    has_poll_data = _column_exists("social", "reddit_posts", "poll_data", conn=conn)
    has_content_url = _column_exists("social", "reddit_posts", "content_url", conn=conn)
    has_is_nsfw = _column_exists("social", "reddit_posts", "is_nsfw", conn=conn)
    has_is_spoiler = _column_exists("social", "reddit_posts", "is_spoiler", conn=conn)
    has_author_flair_text = _column_exists("social", "reddit_posts", "author_flair_text", conn=conn)

    tuples: list[tuple[Any, ...]] = []
    for row in rows:
        raw = row.get("raw_payload") or {}

        # Collect discovered flairs
        flair_text = row.get("link_flair_text")
        if flair_text and isinstance(flair_text, str) and flair_text.strip():
            discovered_flairs.add(flair_text.strip())

        base = (
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
        extra: tuple[Any, ...] = ()
        if has_upvote_ratio:
            extra += (raw.get("upvote_ratio"),)
        if has_is_self:
            extra += (bool(raw.get("is_self", False)),)
        if has_post_type:
            extra += (_derive_post_type(raw),)
        if has_thumbnail:
            thumb = raw.get("thumbnail")
            if isinstance(thumb, str) and thumb.lower() in _THUMBNAIL_SKIP_VALUES:
                thumb = None
            extra += (thumb,)
        if has_media_metadata:
            mm = raw.get("media_metadata") or raw.get("gallery_data") or {}
            extra += (_json_value(mm if mm else {}),)
        if has_poll_data:
            extra += (_json_value(raw.get("poll_data")) if raw.get("poll_data") else Json(None),)
        if has_content_url:
            post_url = raw.get("url") or ""
            permalink = raw.get("permalink") or ""
            content_url = post_url if (post_url and permalink and not post_url.endswith(permalink)) else None
            extra += (content_url,)
        if has_is_nsfw:
            extra += (bool(raw.get("over_18", False)),)
        if has_is_spoiler:
            extra += (bool(raw.get("spoiler", False)),)
        if has_author_flair_text:
            extra += (raw.get("author_flair_text"),)

        tuples.append(base + extra)

    # Build dynamic column lists for Sprint-3 columns
    extra_cols: list[str] = []
    extra_conflict_sets: list[str] = []
    if has_upvote_ratio:
        extra_cols.append("upvote_ratio")
        extra_conflict_sets.append("upvote_ratio = excluded.upvote_ratio")
    if has_is_self:
        extra_cols.append("is_self")
        extra_conflict_sets.append("is_self = excluded.is_self")
    if has_post_type:
        extra_cols.append("post_type")
        extra_conflict_sets.append("post_type = excluded.post_type")
    if has_thumbnail:
        extra_cols.append("thumbnail")
        extra_conflict_sets.append("thumbnail = excluded.thumbnail")
    if has_media_metadata:
        extra_cols.append("media_metadata")
        extra_conflict_sets.append("media_metadata = excluded.media_metadata")
    if has_poll_data:
        extra_cols.append("poll_data")
        extra_conflict_sets.append("poll_data = excluded.poll_data")
    if has_content_url:
        extra_cols.append("content_url")
        extra_conflict_sets.append("content_url = excluded.content_url")
    if has_is_nsfw:
        extra_cols.append("is_nsfw")
        extra_conflict_sets.append("is_nsfw = excluded.is_nsfw")
    if has_is_spoiler:
        extra_cols.append("is_spoiler")
        extra_conflict_sets.append("is_spoiler = excluded.is_spoiler")
    if has_author_flair_text:
        extra_cols.append("author_flair_text")
        extra_conflict_sets.append("author_flair_text = excluded.author_flair_text")

    extra_cols_sql = (",\n          " + ",\n          ".join(extra_cols)) if extra_cols else ""
    extra_conflict_sql = (
        (",\n            " + ",\n            ".join(extra_conflict_sets)) if extra_conflict_sets else ""
    )

    sql = f"""
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
          raw_payload{extra_cols_sql}
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
            raw_payload = excluded.raw_payload{extra_conflict_sql},
            last_seen_at = now(),
            updated_at = now()
        """

    pg.execute_values_no_return(sql, tuples, conn=conn)
    return discovered_flairs


def _load_existing_post_comment_counts(
    *,
    post_ids: list[str],
    conn: Any,
) -> dict[str, int]:
    if not post_ids:
        return {}
    try:
        with pg.db_cursor(conn=conn) as cur:
            cur.execute(
                """
                select reddit_post_id, num_comments
                from social.reddit_posts
                where reddit_post_id = any(%s::text[])
                """,
                [post_ids],
            )
            rows = cur.fetchall() or []
    except Exception:  # noqa: BLE001
        # Non-fatal optimization failure; keep comment refresh functional.
        return {}

    counts: dict[str, int] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        post_id = str(row.get("reddit_post_id") or "").strip()
        if not post_id:
            continue
        counts[post_id] = _safe_int(row.get("num_comments"))
    return counts


def _upsert_comments(rows: list[dict[str, Any]], *, conn: Any) -> int:
    if not rows:
        return 0

    # Check which Sprint-4 columns exist (backward compat before migration).
    has_author_flair_text = _column_exists("social", "reddit_comments", "author_flair_text", conn=conn)
    has_is_submitter = _column_exists("social", "reddit_comments", "is_submitter", conn=conn)
    has_controversiality = _column_exists("social", "reddit_comments", "controversiality", conn=conn)
    has_ups = _column_exists("social", "reddit_comments", "ups", conn=conn)
    has_downs = _column_exists("social", "reddit_comments", "downs", conn=conn)
    has_gildings = _column_exists("social", "reddit_comments", "gildings", conn=conn)
    has_body_html = _column_exists("social", "reddit_comments", "body_html", conn=conn)

    tuples: list[tuple[Any, ...]] = []
    for row in rows:
        raw = row.get("raw_payload") or {}
        base = (
            row.get("reddit_comment_id"),
            row.get("reddit_post_id"),
            row.get("parent_comment_id"),
            row.get("author"),
            row.get("body") or "",
            _safe_int(row.get("score")),
            _safe_int(row.get("depth")),
            _parse_iso(row.get("created_at_utc")),
            _json_value(raw),
        )
        extra: tuple[Any, ...] = ()
        if has_author_flair_text:
            extra += (row.get("author_flair_text") or raw.get("author_flair_text"),)
        if has_is_submitter:
            extra += (bool(row.get("is_submitter", raw.get("is_submitter", False))),)
        if has_controversiality:
            extra += (_safe_int(row.get("controversiality", raw.get("controversiality", 0))),)
        if has_ups:
            val = row.get("ups") if row.get("ups") is not None else raw.get("ups")
            extra += (_safe_int(val) if val is not None else None,)
        if has_downs:
            val = row.get("downs") if row.get("downs") is not None else raw.get("downs", 0)
            extra += (_safe_int(val),)
        if has_gildings:
            gildings_val = row.get("gildings") if row.get("gildings") is not None else raw.get("gildings", {})
            extra += (_json_value(gildings_val if gildings_val else {}),)
        if has_body_html:
            extra += (row.get("body_html") or raw.get("body_html"),)
        tuples.append(base + extra)

    # Build dynamic column lists for Sprint-4 columns
    extra_cols: list[str] = []
    extra_conflict_sets: list[str] = []
    if has_author_flair_text:
        extra_cols.append("author_flair_text")
        extra_conflict_sets.append("author_flair_text = excluded.author_flair_text")
    if has_is_submitter:
        extra_cols.append("is_submitter")
        extra_conflict_sets.append("is_submitter = excluded.is_submitter")
    if has_controversiality:
        extra_cols.append("controversiality")
        extra_conflict_sets.append("controversiality = excluded.controversiality")
    if has_ups:
        extra_cols.append("ups")
        extra_conflict_sets.append("ups = excluded.ups")
    if has_downs:
        extra_cols.append("downs")
        extra_conflict_sets.append("downs = excluded.downs")
    if has_gildings:
        extra_cols.append("gildings")
        extra_conflict_sets.append("gildings = excluded.gildings")
    if has_body_html:
        extra_cols.append("body_html")
        extra_conflict_sets.append("body_html = excluded.body_html")

    extra_cols_sql = (",\n          " + ",\n          ".join(extra_cols)) if extra_cols else ""
    extra_conflict_sql = (
        (",\n            " + ",\n            ".join(extra_conflict_sets)) if extra_conflict_sets else ""
    )

    sql = f"""
        insert into social.reddit_comments (
          reddit_comment_id,
          reddit_post_id,
          parent_comment_id,
          author,
          body,
          score,
          depth,
          created_at_utc,
          raw_payload{extra_cols_sql}
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
            raw_payload = excluded.raw_payload{extra_conflict_sql},
            last_seen_at = now(),
            updated_at = now()
        """

    pg.execute_values_no_return(sql, tuples, conn=conn)
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
    preserve_existing_assignments: bool = True,
) -> None:
    has_flair_mode = _column_exists("social", "reddit_period_post_matches", "flair_mode", conn=conn)
    has_match_type = _column_exists("social", "reddit_period_post_matches", "match_type", conn=conn)
    if not rows:
        # Preserve existing assignments when a refresh returns no rows
        # (for example due to partial coverage or transient Reddit limits).
        if not preserve_existing_assignments:
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
        return

    tuples: list[tuple[Any, ...]] = []
    post_ids_to_keep: list[str] = []
    seen_post_ids: set[str] = set()
    for row in rows:
        reddit_post_id = str(row.get("reddit_post_id") or "").strip()
        if not reddit_post_id:
            continue
        if reddit_post_id not in seen_post_ids:
            seen_post_ids.add(reddit_post_id)
            post_ids_to_keep.append(reddit_post_id)
        base_tuple = (
            community_id,
            season_id,
            period_key,
            period_start,
            period_end,
            reddit_post_id,
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
        )
        extra = ()
        if has_flair_mode:
            extra += (row.get("flair_mode"),)
        if has_match_type:
            extra += (row.get("match_type") or "flair",)
        tuples.append((*base_tuple, *extra))
    if preserve_existing_assignments and post_ids_to_keep:
        with pg.db_cursor(conn=conn) as cur:
            cur.execute(
                """
                select reddit_post_id
                from social.reddit_period_post_matches
                where community_id = %s
                  and season_id = %s
                  and period_key <> %s
                  and reddit_post_id = any(%s::text[])
                """,
                [community_id, season_id, period_key, post_ids_to_keep],
            )
            existing_rows = cur.fetchall() or []
        assigned_elsewhere = {
            str(row.get("reddit_post_id") or "").strip()
            for row in existing_rows
            if isinstance(row, dict) and str(row.get("reddit_post_id") or "").strip()
        }
        if assigned_elsewhere:
            tuples = [row_tuple for row_tuple in tuples if str(row_tuple[5] or "").strip() not in assigned_elsewhere]

    if not tuples:
        if not preserve_existing_assignments:
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
        return

    # Build column list dynamically based on available optional columns
    base_cols = [
        "community_id",
        "season_id",
        "period_key",
        "period_start",
        "period_end",
        "reddit_post_id",
        "run_id",
        "is_show_match",
        "passes_flair_filter",
        "matched_terms",
        "matched_cast_terms",
        "cross_show_terms",
        "match_score",
        "source_sorts",
        "link_flair_text",
        "canonical_flair_key",
    ]
    # These are always updated on conflict
    conflict_update_cols = [
        "run_id",
        "is_show_match",
        "passes_flair_filter",
        "matched_terms",
        "matched_cast_terms",
        "cross_show_terms",
        "match_score",
        "source_sorts",
        "link_flair_text",
        "canonical_flair_key",
    ]
    if has_flair_mode:
        base_cols.append("flair_mode")
        conflict_update_cols.append("flair_mode")
    if has_match_type:
        base_cols.append("match_type")
        conflict_update_cols.append("match_type")

    cols_str = ",\n              ".join(base_cols)
    conflict_set_str = ",\n                ".join(f"{col} = excluded.{col}" for col in conflict_update_cols)
    sql = f"""
        insert into social.reddit_period_post_matches (
          {cols_str}
        )
        values %s
        on conflict (community_id, season_id, period_key, reddit_post_id) do update
        set {conflict_set_str},
            updated_at = now()
    """
    pg.execute_values_no_return(sql, tuples, conn=conn)

    if not preserve_existing_assignments:
        with pg.db_cursor(conn=conn) as cur:
            cur.execute(
                """
                delete from social.reddit_period_post_matches
                where community_id = %s
                  and season_id = %s
                  and period_key = %s
                  and not (reddit_post_id = any(%s::text[]))
                """,
                [community_id, season_id, period_key, post_ids_to_keep],
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


def _fetch_cached_run_row(*, community_id: str, season_id: str, period_key: str) -> dict[str, Any] | None:
    return pg.fetch_one(
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


def _container_key_from_period_key(period_key: str) -> str | None:
    normalized = str(period_key or "").strip().lower()
    if not normalized:
        return None
    if normalized.startswith("period-") or normalized.startswith("episode-"):
        return normalized
    stable_match = re.match(
        r"^community:[^:]+:season:[^:]+:container:([a-z0-9-]+)$",
        normalized,
        flags=re.IGNORECASE,
    )
    if not stable_match:
        return None
    container_key = str(stable_match.group(1) or "").strip().lower()
    return container_key or None


def _resolve_cached_period_key(*, community_id: str, season_id: str, period_key: str) -> str | None:
    normalized_period_key = str(period_key or "").strip()
    if not normalized_period_key:
        return None

    direct = _fetch_cached_run_row(
        community_id=community_id,
        season_id=season_id,
        period_key=normalized_period_key,
    )
    if direct:
        return normalized_period_key

    container_key = _container_key_from_period_key(normalized_period_key)
    if not container_key:
        return None

    resolved = pg.fetch_one(
        """
        select period_key
        from social.reddit_refresh_runs
        where community_id = %s
          and season_id = %s
          and status in ('completed', 'partial')
          and (
            lower(coalesce(request_payload->>'container_key', '')) = %s
            or lower(coalesce(request_payload->>'period_stable_key', '')) = %s
          )
        order by completed_at desc nulls last, created_at desc
        limit 1
        """,
        [community_id, season_id, container_key, container_key],
    )
    resolved_period_key = str((resolved or {}).get("period_key") or "").strip()
    return resolved_period_key or None


def resolve_cached_period_key(*, community_id: str, season_id: str, period_key: str) -> str | None:
    """Resolve a provided cache key to the latest canonical stored period key.

    This is intentionally lightweight and does not read large discovery payload rows.
    """
    return _resolve_cached_period_key(
        community_id=community_id,
        season_id=season_id,
        period_key=period_key,
    )


def get_cached_period_payload(*, community_id: str, season_id: str, period_key: str) -> dict[str, Any] | None:
    resolved_period_key = _resolve_cached_period_key(
        community_id=community_id,
        season_id=season_id,
        period_key=period_key,
    )
    if not resolved_period_key:
        return None

    run = _fetch_cached_run_row(
        community_id=community_id,
        season_id=season_id,
        period_key=resolved_period_key,
    )
    if not run:
        return None

    diagnostics = run.get("diagnostics") if isinstance(run.get("diagnostics"), dict) else {}
    result_payload = diagnostics.get("result") if isinstance(diagnostics.get("result"), dict) else {}

    has_flair_mode = _column_exists("social", "reddit_period_post_matches", "flair_mode")
    if has_flair_mode:
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
            [community_id, season_id, resolved_period_key],
        )
    else:
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
                   null::text as flair_mode
            from social.reddit_period_post_matches m
            join social.reddit_posts p on p.reddit_post_id = m.reddit_post_id
            where m.community_id = %s
              and m.season_id = %s
              and m.period_key = %s
            order by m.match_score desc, p.num_comments desc, p.score desc
            """,
            [community_id, season_id, resolved_period_key],
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


def get_cached_period_payload_snapshot(*, community_id: str, season_id: str, period_key: str) -> dict[str, Any] | None:
    resolved_period_key = _resolve_cached_period_key(
        community_id=community_id,
        season_id=season_id,
        period_key=period_key,
    )
    if not resolved_period_key:
        return None
    run = _fetch_cached_run_row(
        community_id=community_id,
        season_id=season_id,
        period_key=resolved_period_key,
    )
    if not run:
        return None
    discovery = get_cached_period_payload(
        community_id=community_id,
        season_id=season_id,
        period_key=resolved_period_key,
    )
    if discovery is None:
        return None
    run_meta = _build_refresh_run_meta(run)
    return {
        "discovery": discovery,
        "resolved_period_key": resolved_period_key,
        "cache_status": run_meta.get("cache_status"),
        "cache_age_seconds": run_meta.get("cache_age_seconds"),
        "run_status": run_meta.get("run_status"),
        "phase": run_meta.get("phase"),
        "partial_failures": run_meta.get("partial_failures") or [],
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
    claim_token: str | None = None,
    release_claim: bool = False,
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
        "claim_token": str(claim_token or "").strip() or None,
        "release_claim": bool(release_claim),
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
            heartbeat_at = now(),
            lease_expires_at = case
              when %(release_claim)s then null
              else now() + (%(lease_seconds)s::int * interval '1 second')
            end,
            claim_token = case when %(release_claim)s then null else claim_token end,
            claimed_by_worker_id = case when %(release_claim)s then null else claimed_by_worker_id end,
            updated_at = %(updated_at)s
        where id = %(run_id)s::uuid
          and (%(claim_token)s::text is null or claim_token = %(claim_token)s::text)
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
            "lease_seconds": _claim_lease_seconds(),
            "claim_token": values["claim_token"],
            "release_claim": values["release_claim"],
            "updated_at": values["updated_at"],
            "run_id": run_id,
        },
    )


def create_or_reuse_refresh_run(*, payload: dict[str, Any]) -> dict[str, Any]:
    normalized_payload = dict(payload)
    run_config_hash = str(normalized_payload.get("run_config_hash") or "").strip().lower()
    if not run_config_hash:
        run_config_hash = _build_run_config_hash(normalized_payload)
        normalized_payload["run_config_hash"] = run_config_hash

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

    stale_queued_seconds = _env_int(
        "REDDIT_REFRESH_STALE_QUEUED_SECONDS",
        REDDIT_REFRESH_STALE_QUEUED_SECONDS_DEFAULT,
        minimum=30,
        maximum=86_400,
    )
    stale_cutoff = datetime.now(tz=UTC) - timedelta(seconds=stale_queued_seconds)
    stale_rows = pg.execute_returning(
        """
        update social.reddit_refresh_runs
        set status = 'failed',
            error_message = coalesce(
              error_message,
              'Queued reddit refresh run expired before execution. Start a new refresh.'
            ),
            completed_at = coalesce(completed_at, now()),
            updated_at = now(),
            diagnostics = coalesce(diagnostics, '{}'::jsonb) || jsonb_build_object(
              'stale_queue_recovered', true,
              'stale_queue_cutoff', %s::timestamptz
            )
        where community_id = %s
          and season_id = %s
          and period_key = %s
          and status = 'queued'
          and updated_at < %s::timestamptz
        returning id
        """,
        [stale_cutoff, community_id, season_id, period_key, stale_cutoff],
    )
    if stale_rows:
        logger.warning(
            "[reddit_refresh_stale_queue_recovered] community_id=%s season_id=%s period_key=%s recovered=%s",
            community_id,
            season_id,
            period_key,
            len(stale_rows),
        )

    stale_running_cutoff = _env_int(
        "REDDIT_REFRESH_STALE_RUNNING_SECONDS",
        REDDIT_REFRESH_STALE_RUNNING_SECONDS_DEFAULT,
        minimum=120,
        maximum=86_400,
    )
    orphaned_queued_reuse_grace_seconds = _env_int(
        "REDDIT_REFRESH_ORPHANED_QUEUED_REUSE_GRACE_SECONDS",
        REDDIT_REFRESH_ORPHANED_QUEUED_REUSE_GRACE_SECONDS_DEFAULT,
        minimum=15,
        maximum=3_600,
    )

    active_runs = pg.fetch_all(
        """
        select *
        from social.reddit_refresh_runs
        where community_id = %s
          and season_id = %s
          and period_key = %s
          and status in ('queued', 'running')
        order by created_at desc
        limit 5
        """,
        [community_id, season_id, period_key],
    )
    for existing in active_runs:
        # Auto-recover stale running runs during dedup check so orphaned runs
        # don't block new runs indefinitely.
        existing_status = str(existing.get("status") or "").strip().lower()
        if existing_status == "running":
            existing_updated = _parse_iso(existing.get("updated_at"))
            now = datetime.now(tz=UTC)
            if existing_updated and (now - existing_updated) > timedelta(seconds=stale_running_cutoff):
                existing_id = str(existing.get("id") or "")
                logger.warning(
                    "[reddit_refresh_dedup_stale_recovery] marking stale run=%s as failed (updated_at=%s, cutoff=%ss)",
                    existing_id[:8],
                    existing_updated.isoformat(),
                    stale_running_cutoff,
                )
                _update_run(
                    existing_id,
                    status="failed",
                    diagnostics={
                        "stale_running_recovered": True,
                        "stale_running_cutoff_seconds": stale_running_cutoff,
                        "recovered_during": "dedup_check",
                    },
                    error_message=(
                        "Reddit refresh run was stuck in running state and auto-recovered during dedup check."
                    ),
                    set_completed=True,
                )
                continue  # Skip this stale run — let dedup continue or create a new run
        if existing_status == "queued" and _is_orphaned_queued_run(
            existing,
            grace_seconds=orphaned_queued_reuse_grace_seconds,
        ):
            existing_id = str(existing.get("id") or "")
            logger.warning(
                "[reddit_refresh_orphaned_queue_recovered] marking orphaned queued run=%s as failed",
                existing_id[:8],
            )
            _update_run(
                existing_id,
                status="failed",
                diagnostics={
                    "stale_queue_recovered": True,
                    "stale_queue_reuse_grace_seconds": orphaned_queued_reuse_grace_seconds,
                    "failure_reason_code": "stale_queue",
                    "operator_hint": _default_operator_hint("stale_queue", status="failed"),
                    "recovered_during": "dedup_check",
                },
                error_message="Queued reddit refresh run expired before execution. Start a new refresh.",
                set_completed=True,
            )
            continue

        existing_payload = existing.get("request_payload") if isinstance(existing.get("request_payload"), dict) else {}
        existing_hash = str(existing_payload.get("run_config_hash") or "").strip().lower()
        if not existing_hash:
            existing_hash = _build_run_config_hash(existing_payload)
        if existing_hash != run_config_hash:
            continue
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
        [community_id, season_id, period_key, subreddit, json.dumps(normalized_payload, ensure_ascii=True)],
    )
    if not row:
        raise RuntimeError("Failed to create refresh run")
    row["reused"] = False
    return row


def _claim_refresh_run_for_execution(
    run_id: str,
    *,
    worker_id: str | None = None,
) -> dict[str, Any]:
    normalized_worker = str(worker_id or "").strip() or _default_worker_id()
    lease_seconds = _claim_lease_seconds()
    claimed = pg.fetch_one(
        """
        update social.reddit_refresh_runs
        set status = 'running',
            started_at = coalesce(started_at, now()),
            claim_token = gen_random_uuid()::text,
            claimed_by_worker_id = %s,
            lease_expires_at = now() + (%s::int * interval '1 second'),
            heartbeat_at = now(),
            attempt_count = coalesce(attempt_count, 0) + 1,
            next_retry_at = null,
            updated_at = now()
        where id = %s::uuid
          and (
            status = 'queued'
            or (
              status = 'running'
              and (
                lease_expires_at is null
                or lease_expires_at < now()
                or coalesce(heartbeat_at, updated_at, created_at) < now() - interval '5 minutes'
              )
            )
          )
        returning id, community_id, season_id, period_key, subreddit, request_payload, status, updated_at, claim_token
        """,
        [normalized_worker, lease_seconds, run_id],
    )
    if claimed:
        return claimed

    row = pg.fetch_one(
        """
        select id, community_id, season_id, period_key, subreddit, request_payload, status, updated_at, claim_token
        from social.reddit_refresh_runs
        where id = %s::uuid
        """,
        [run_id],
    )
    if not row:
        raise ValueError("Refresh run not found")

    status = str(row.get("status") or "").strip().lower()
    if status == "running":
        stale_running_seconds = _env_int(
            "REDDIT_REFRESH_STALE_RUNNING_SECONDS",
            REDDIT_REFRESH_STALE_RUNNING_SECONDS_DEFAULT,
            minimum=120,
            maximum=86_400,
        )
        updated_at = _parse_iso(row.get("updated_at"))
        now = datetime.now(tz=UTC)
        if updated_at and (now - updated_at) > timedelta(seconds=stale_running_seconds):
            _update_run(
                run_id,
                status="failed",
                diagnostics={
                    "stale_running_recovered": True,
                    "stale_running_cutoff_seconds": stale_running_seconds,
                },
                error_message=("Reddit refresh run was stuck in running state and marked failed. Start refresh again."),
                set_completed=True,
            )
            raise RuntimeError("Reddit refresh run recovered from stale running state")
        raise RuntimeError("Reddit refresh run is already running")

    if status in {"completed", "partial", "failed", "cancelled"}:
        raise RuntimeError(f"Reddit refresh run is already {status}")

    raise RuntimeError(f"Reddit refresh run cannot be executed from status '{status or 'unknown'}'")


def claim_next_refresh_run(*, worker_id: str | None = None) -> dict[str, Any] | None:
    normalized_worker = str(worker_id or "").strip() or _default_worker_id()
    lease_seconds = _claim_lease_seconds()
    row = pg.fetch_one(
        """
        with candidate as (
          select id
          from social.reddit_refresh_runs
          where status in ('queued', 'running')
            and coalesce(next_retry_at, now()) <= now()
            and (
              status = 'queued'
              or lease_expires_at is null
              or lease_expires_at < now()
              or coalesce(heartbeat_at, updated_at, created_at) < now() - interval '5 minutes'
            )
          order by
            case when status = 'queued' then 0 else 1 end,
            created_at asc
          limit 1
          for update skip locked
        )
        update social.reddit_refresh_runs r
        set status = 'running',
            started_at = coalesce(r.started_at, now()),
            claim_token = gen_random_uuid()::text,
            claimed_by_worker_id = %s,
            lease_expires_at = now() + (%s::int * interval '1 second'),
            heartbeat_at = now(),
            attempt_count = coalesce(r.attempt_count, 0) + 1,
            next_retry_at = null,
            updated_at = now()
        from candidate
        where r.id = candidate.id
        returning r.id,
                  r.community_id,
                  r.season_id,
                  r.period_key,
                  r.subreddit,
                  r.request_payload,
                  r.status,
                  r.updated_at,
                  r.claim_token
        """,
        [normalized_worker, lease_seconds],
    )
    return dict(row) if row else None


def _touch_refresh_run_heartbeat(*, run_id: str, claim_token: str | None = None) -> None:
    token = str(claim_token or "").strip() or None
    pg.execute_returning(
        """
        update social.reddit_refresh_runs
        set heartbeat_at = now(),
            lease_expires_at = now() + (%s::int * interval '1 second'),
            updated_at = now()
        where id = %s::uuid
          and status = 'running'
          and (%s::text is null or claim_token = %s::text)
        returning id
        """,
        [_claim_lease_seconds(), run_id, token, token],
    )


def _discover_window(
    payload: dict[str, Any],
    *,
    progress_callback: ProgressCallback | None = None,
) -> dict[str, Any]:
    subreddit = _normalize_subreddit(str(payload.get("subreddit") or ""))
    show_name = str(payload.get("show_name") or "").strip()
    show_aliases = [str(item) for item in (payload.get("show_aliases") or []) if isinstance(item, str)]
    cast_names = [str(item) for item in (payload.get("cast_names") or []) if isinstance(item, str)]
    analysis_flairs = [str(item) for item in (payload.get("analysis_flairs") or []) if isinstance(item, str)]
    analysis_all_flairs = [str(item) for item in (payload.get("analysis_all_flairs") or []) if isinstance(item, str)]
    force_include_flairs = [str(item) for item in (payload.get("force_include_flairs") or []) if isinstance(item, str)]
    seed_post_urls = [str(item) for item in (payload.get("seed_post_urls") or []) if isinstance(item, str)]

    normalized_analysis_flairs: list[str] = []
    for value in analysis_flairs:
        normalized = _collapse_whitespace(value)
        if normalized:
            normalized_analysis_flairs.append(normalized)
    normalized_analysis_all_flairs: list[str] = []
    for value in analysis_all_flairs:
        normalized = _collapse_whitespace(value)
        if normalized:
            normalized_analysis_all_flairs.append(normalized)
    normalized_force_include_flairs: list[str] = []
    for value in force_include_flairs:
        normalized = _collapse_whitespace(value)
        if normalized:
            normalized_force_include_flairs.append(normalized)
    tracked_flairs = [
        *normalized_analysis_all_flairs,
        *normalized_analysis_flairs,
        *normalized_force_include_flairs,
    ]

    period_start = _parse_iso(payload.get("period_start"))
    period_end = _parse_iso(payload.get("period_end"))
    if period_start and period_end and period_start > period_end:
        raise ValueError("period_start must be before period_end")

    exhaustive = bool(payload.get("exhaustive_window")) and (period_start is not None or period_end is not None)
    search_backfill_enabled = bool(payload.get("search_backfill"))

    sort_modes = payload.get("sort_modes") if isinstance(payload.get("sort_modes"), list) else ["new", "hot", "top"]
    normalized_sorts = [
        str(sort).strip().lower() for sort in sort_modes if str(sort).strip().lower() in {"new", "hot", "top"}
    ]
    if not normalized_sorts:
        normalized_sorts = ["new", "hot", "top"]

    limit_per_mode = _env_int("REDDIT_SAMPLE_LIMIT_PER_MODE", 35, minimum=1, maximum=100)
    try:
        requested_limit = int(payload.get("limit_per_mode") or 35)
        limit_per_mode = max(1, min(100, requested_limit))
    except Exception:  # noqa: BLE001
        pass

    max_pages = _env_int("REDDIT_EXHAUSTIVE_MAX_PAGES", REDDIT_MAX_PAGES_DEFAULT, minimum=1, maximum=10_000)
    try:
        requested_pages = int(payload.get("max_pages") or max_pages)
        max_pages = max(1, min(10_000, requested_pages))
    except Exception:  # noqa: BLE001
        pass
    max_backfill_pages_per_query = _env_int(
        "REDDIT_BACKFILL_MAX_PAGES_PER_QUERY",
        REDDIT_MAX_SEARCH_PAGES_PER_QUERY_DEFAULT,
        minimum=1,
        maximum=50,
    )
    if payload.get("max_backfill_pages_per_query") is not None:
        max_backfill_pages_per_query = _coerce_int(
            payload.get("max_backfill_pages_per_query"),
            default=max_backfill_pages_per_query,
            minimum=1,
            maximum=50,
        )
    max_backfill_queries = _env_int(
        "REDDIT_BACKFILL_MAX_QUERIES",
        REDDIT_MAX_BACKFILL_QUERIES_DEFAULT,
        minimum=1,
        maximum=30,
    )
    if payload.get("max_backfill_queries") is not None:
        max_backfill_queries = _coerce_int(
            payload.get("max_backfill_queries"),
            default=max_backfill_queries,
            minimum=1,
            maximum=30,
        )

    listing_rows: list[dict[str, Any]] = []
    listing_pages = 0
    window_exhaustive_complete: bool | None = None
    listing_rows_fetched = 0
    search_pages_fetched = 0
    search_rows_fetched = 0
    seed_rows_fetched = 0
    terms = _build_terms(show_name, show_aliases)
    cast_terms = _build_cast_terms(cast_names)
    diagnostics = {
        "successful_sorts": [],
        "failed_sorts": [],
        "rate_limited_sorts": [],
    }

    if exhaustive:

        def on_listing_progress(update: dict[str, Any]) -> None:
            nonlocal listing_pages, listing_rows_fetched
            listing_pages = max(listing_pages, _safe_int(update.get("listing_pages_fetched")))
            listing_rows_fetched = max(listing_rows_fetched, _safe_int(update.get("listing_rows_fetched")))
            if progress_callback:
                progress_callback(
                    {
                        "listing_pages_fetched": listing_pages,
                        "rows_discovered_raw": listing_rows_fetched + search_rows_fetched + seed_rows_fetched,
                    }
                )

        try:
            listing_rows, listing_pages, window_exhaustive_complete = _fetch_new_window_exhaustive(
                subreddit=subreddit,
                period_start=period_start,
                period_end=period_end,
                max_pages=max_pages,
                progress_callback=on_listing_progress,
            )
        except TypeError as exc:
            if "progress_callback" not in str(exc):
                raise
            listing_rows, listing_pages, window_exhaustive_complete = _fetch_new_window_exhaustive(
                subreddit=subreddit,
                period_start=period_start,
                period_end=period_end,
                max_pages=max_pages,
            )
            listing_rows_fetched = len(listing_rows)
            if progress_callback:
                progress_callback(
                    {
                        "listing_pages_fetched": listing_pages,
                        "rows_discovered_raw": listing_rows_fetched + search_rows_fetched + seed_rows_fetched,
                    }
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

        def on_search_progress(update: dict[str, Any]) -> None:
            if not progress_callback:
                return
            progress_callback(
                {
                    "listing_pages_fetched": listing_pages,
                    "search_pages_fetched": max(
                        _safe_int(update.get("search_pages_fetched")),
                        _safe_int(search_pages_fetched),
                    ),
                    "rows_discovered_raw": listing_rows_fetched
                    + max(_safe_int(update.get("search_rows_fetched")), _safe_int(search_rows_fetched))
                    + seed_rows_fetched,
                }
            )

        try:
            backfill_rows, search_backfill_diag = _fetch_search_backfill(
                subreddit=subreddit,
                tracked_flairs=tracked_flairs,
                show_aliases=show_aliases,
                show_terms=terms,
                period_start=period_start,
                period_end=period_end,
                max_pages_per_query=max_backfill_pages_per_query,
                max_total_queries=max_backfill_queries,
                progress_callback=on_search_progress,
            )
        except TypeError as exc:
            if "progress_callback" not in str(exc):
                raise
            backfill_rows, search_backfill_diag = _fetch_search_backfill(
                subreddit=subreddit,
                tracked_flairs=tracked_flairs,
                show_aliases=show_aliases,
                show_terms=terms,
                period_start=period_start,
                period_end=period_end,
                max_pages_per_query=max_backfill_pages_per_query,
                max_total_queries=max_backfill_queries,
            )
        search_pages_fetched = _safe_int(search_backfill_diag.get("pages_fetched")) if search_backfill_diag else 0
        search_rows_fetched = _safe_int(search_backfill_diag.get("rows_fetched")) if search_backfill_diag else 0
        backfill_rows = _filter_by_window(backfill_rows, period_start=period_start, period_end=period_end)
        listing_rows.extend(backfill_rows)
        if progress_callback:
            progress_callback(
                {
                    "listing_pages_fetched": listing_pages,
                    "search_pages_fetched": search_pages_fetched,
                    "rows_discovered_raw": listing_rows_fetched + search_rows_fetched + seed_rows_fetched,
                }
            )

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
        seed_rows_fetched = len(seeded_rows)
        if progress_callback:
            progress_callback(
                {
                    "listing_pages_fetched": listing_pages,
                    "search_pages_fetched": search_pages_fetched,
                    "rows_discovered_raw": listing_rows_fetched + search_rows_fetched + seed_rows_fetched,
                }
            )

    merged_rows = _merge_by_post_id(listing_rows)
    matched_rows, hints, tracked_rows = _apply_match_metadata(
        rows=merged_rows,
        subreddit=subreddit,
        terms=terms,
        cast_terms=cast_terms,
        analysis_flairs=normalized_analysis_flairs,
        analysis_all_flairs=normalized_analysis_all_flairs,
        force_include_flairs=normalized_force_include_flairs,
        show_focused=bool(payload.get("is_show_focused")),
    )
    if progress_callback:
        progress_callback(
            {
                "listing_pages_fetched": listing_pages,
                "search_pages_fetched": search_pages_fetched,
                "rows_discovered_raw": listing_rows_fetched + search_rows_fetched + seed_rows_fetched,
                "rows_matched": len(matched_rows),
            }
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
        "max_backfill_queries_applied": max_backfill_queries,
        "max_backfill_pages_per_query_applied": max_backfill_pages_per_query,
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


def _build_pass_summary(pass_index: int, result: dict[str, Any]) -> dict[str, Any]:
    totals = result.get("totals") if isinstance(result.get("totals"), dict) else {}
    search_backfill = result.get("search_backfill") if isinstance(result.get("search_backfill"), dict) else {}
    return {
        "pass_index": pass_index,
        "max_pages": _safe_int(result.get("max_pages_applied")),
        "backfill_queries": _safe_int(result.get("max_backfill_queries_applied")),
        "backfill_pages_per_query": _safe_int(result.get("max_backfill_pages_per_query_applied")),
        "listing_pages_fetched": _safe_int(result.get("listing_pages_fetched")),
        "search_pages_fetched": _safe_int(search_backfill.get("pages_fetched")),
        "fetched_rows": _safe_int(totals.get("fetched_rows")),
        "matched_rows": _safe_int(totals.get("matched_rows")),
        "tracked_flair_rows": _safe_int(totals.get("tracked_flair_rows")),
        "window_exhaustive_complete": result.get("window_exhaustive_complete"),
        "search_backfill_complete": search_backfill.get("complete"),
    }


def _merge_discovery_pass_results(results: list[dict[str, Any]]) -> dict[str, Any]:
    if len(results) == 1:
        return results[0]

    final = dict(results[-1])
    sources_seen: set[str] = set()
    merged_sources: list[str] = []
    terms_seen: set[str] = set()
    merged_terms: list[str] = []
    include_seen: set[str] = set()
    merged_include_hints: list[str] = []
    exclude_seen: set[str] = set()
    merged_exclude_hints: list[str] = []
    listing_pages_total = 0
    max_pages_applied = 0
    max_backfill_queries_applied = 0
    max_backfill_pages_per_query_applied = 0
    search_pages_total = 0
    search_rows_total = 0
    search_rows_in_window_total = 0
    search_queries_total = 0
    query_diagnostics: list[dict[str, Any]] = []
    min_window_start: datetime | None = None
    max_window_end: datetime | None = None
    max_fetched_rows = 0

    threads_by_id: dict[str, dict[str, Any]] = {}
    for result_index, result in enumerate(results, start=1):
        listing_pages_total += _safe_int(result.get("listing_pages_fetched"))
        max_pages_applied = max(max_pages_applied, _safe_int(result.get("max_pages_applied")))
        max_backfill_queries_applied = max(
            max_backfill_queries_applied,
            _safe_int(result.get("max_backfill_queries_applied")),
        )
        max_backfill_pages_per_query_applied = max(
            max_backfill_pages_per_query_applied,
            _safe_int(result.get("max_backfill_pages_per_query_applied")),
        )
        totals = result.get("totals") if isinstance(result.get("totals"), dict) else {}
        max_fetched_rows = max(max_fetched_rows, _safe_int(totals.get("fetched_rows")))
        for source in result.get("sources_fetched") or []:
            source_value = str(source).strip()
            if not source_value or source_value in sources_seen:
                continue
            sources_seen.add(source_value)
            merged_sources.append(source_value)
        for term in result.get("terms") or []:
            term_value = str(term).strip()
            if not term_value or term_value in terms_seen:
                continue
            terms_seen.add(term_value)
            merged_terms.append(term_value)
        hints = result.get("hints") if isinstance(result.get("hints"), dict) else {}
        for term in hints.get("suggested_include_terms") or []:
            term_value = str(term).strip()
            if not term_value or term_value in include_seen:
                continue
            include_seen.add(term_value)
            merged_include_hints.append(term_value)
        for term in hints.get("suggested_exclude_terms") or []:
            term_value = str(term).strip()
            if not term_value or term_value in exclude_seen:
                continue
            exclude_seen.add(term_value)
            merged_exclude_hints.append(term_value)
        window_start = _parse_iso(result.get("window_start"))
        window_end = _parse_iso(result.get("window_end"))
        if window_start:
            min_window_start = window_start if min_window_start is None else min(min_window_start, window_start)
        if window_end:
            max_window_end = window_end if max_window_end is None else max(max_window_end, window_end)

        search_backfill = result.get("search_backfill") if isinstance(result.get("search_backfill"), dict) else None
        if search_backfill:
            search_pages_total += _safe_int(search_backfill.get("pages_fetched"))
            search_rows_total += _safe_int(search_backfill.get("rows_fetched"))
            search_rows_in_window_total += _safe_int(search_backfill.get("rows_in_window"))
            search_queries_total += _safe_int(search_backfill.get("queries_run"))
            raw_query_diag = search_backfill.get("query_diagnostics")
            if isinstance(raw_query_diag, list):
                for item in raw_query_diag:
                    if not isinstance(item, dict):
                        continue
                    query_diagnostics.append({"pass_index": result_index, **item})

        for thread in result.get("threads") or []:
            if not isinstance(thread, dict):
                continue
            post_id = str(thread.get("reddit_post_id") or "").strip()
            if not post_id:
                continue
            existing = threads_by_id.get(post_id)
            if existing is None:
                threads_by_id[post_id] = dict(thread)
                continue
            merged_sorts = sorted(
                {
                    *(existing.get("source_sorts") or []),
                    *(thread.get("source_sorts") or []),
                }
            )
            existing_posted = _parse_iso(existing.get("posted_at"))
            thread_posted = _parse_iso(thread.get("posted_at"))
            prefer_thread = (
                bool(thread_posted and (existing_posted is None or thread_posted > existing_posted))
                or _safe_int(thread.get("num_comments")) > _safe_int(existing.get("num_comments"))
                or _safe_int(thread.get("score")) > _safe_int(existing.get("score"))
            )
            base = dict(thread if prefer_thread else existing)
            base["source_sorts"] = merged_sorts
            threads_by_id[post_id] = base

    merged_threads = list(threads_by_id.values())
    merged_threads.sort(
        key=lambda row: (
            _parse_iso(row.get("posted_at")) or datetime.min.replace(tzinfo=UTC),
            _safe_int(row.get("num_comments")),
            _safe_int(row.get("score")),
        ),
        reverse=True,
    )
    tracked_flair_rows = sum(1 for row in merged_threads if bool(row.get("passes_flair_filter")))

    final_search_backfill = final.get("search_backfill") if isinstance(final.get("search_backfill"), dict) else None
    merged_search_backfill = None
    if final_search_backfill is not None:
        merged_search_backfill = dict(final_search_backfill)
        merged_search_backfill["queries_run"] = search_queries_total
        merged_search_backfill["pages_fetched"] = search_pages_total
        merged_search_backfill["rows_fetched"] = search_rows_total
        merged_search_backfill["rows_in_window"] = search_rows_in_window_total
        merged_search_backfill["query_diagnostics"] = query_diagnostics

    final["fetched_at"] = datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")
    final["sources_fetched"] = merged_sources
    final["listing_pages_fetched"] = listing_pages_total
    final["max_pages_applied"] = max_pages_applied
    final["max_backfill_queries_applied"] = max_backfill_queries_applied
    final["max_backfill_pages_per_query_applied"] = max_backfill_pages_per_query_applied
    final["search_backfill"] = merged_search_backfill
    final["terms"] = merged_terms
    final["hints"] = {
        "suggested_include_terms": merged_include_hints[:8],
        "suggested_exclude_terms": merged_exclude_hints[:8],
    }
    final["threads"] = merged_threads
    final["totals"] = {
        "fetched_rows": max(max_fetched_rows, len(merged_threads)),
        "matched_rows": len(merged_threads),
        "tracked_flair_rows": tracked_flair_rows,
    }
    final["window_start"] = _iso_utc(min_window_start) or final.get("window_start")
    final["window_end"] = _iso_utc(max_window_end) or final.get("window_end")
    return final


def _run_detail_sync_phase(
    *,
    community_id: str,
    season_id: str,
    period_key: str,
    force_rescrape: bool,
    progress: dict[str, Any],
    apply_progress: Callable[..., None],
) -> dict[str, Any]:
    from trr_backend.media.s3_mirror import mirror_reddit_media  # lazy import

    has_detail_scraped_at = _column_exists("social", "reddit_posts", "detail_scraped_at")
    detail_filter = ""
    if has_detail_scraped_at and not force_rescrape:
        detail_filter = "and rp.detail_scraped_at is null"

    with pg.db_connection() as conn:
        with pg.db_cursor(conn=conn) as cur:
            cur.execute(
                f"""
                select rp.reddit_post_id,
                       rp.url,
                       rp.raw_payload
                from social.reddit_period_post_matches rpm
                join social.reddit_posts rp
                  on rp.reddit_post_id = rpm.reddit_post_id
                where rpm.community_id = %s
                  and rpm.season_id = %s
                  and rpm.period_key = %s
                  {detail_filter}
                order by rp.posted_at desc nulls last
                """,
                [community_id, season_id, period_key],
            )
            target_posts = cur.fetchall() or []

    apply_progress(
        {
            "stage": "syncing_details",
            "detail_posts_total": len(target_posts),
            "detail_posts_done": 0,
            "comments_upserted": 0,
            "media_queued": 0,
            "media_mirrored": 0,
        },
        force=True,
    )

    detail_posts_done = 0
    comments_upserted = 0
    media_queued = 0
    media_mirrored = 0
    detail_errors: list[dict[str, str]] = []

    def _process_single_detail_post(
        post_row: dict[str, Any],
        *,
        has_detail_scraped_at_value: bool,
    ) -> dict[str, Any]:
        result: dict[str, Any] = {
            "comments_upserted": 0,
            "media_queued": 0,
            "media_mirrored": 0,
            "error": None,
            "reddit_post_id": None,
            "skipped": False,
        }

        pid = str(post_row.get("reddit_post_id") or "").strip()
        result["reddit_post_id"] = pid
        if not pid:
            result["skipped"] = True
            return result

        try:
            comments = _fetch_post_comments_tree(pid)

            for comment in comments:
                raw = comment.get("raw_payload") or {}
                comment["author_flair_text"] = raw.get("author_flair_text")
                comment["is_submitter"] = raw.get("is_submitter", False)
                comment["controversiality"] = raw.get("controversiality", 0)
                comment["ups"] = raw.get("ups")
                comment["downs"] = raw.get("downs", 0)
                comment["gildings"] = raw.get("gildings", {})
                comment["body_html"] = raw.get("body_html")

            if comments:
                with pg.db_connection() as conn:
                    result["comments_upserted"] = _upsert_comments(comments, conn=conn)

            post_raw = post_row.get("raw_payload") if isinstance(post_row.get("raw_payload"), dict) else {}
            media_urls: list[tuple[str, str]] = []

            post_url = str(post_raw.get("url") or post_row.get("url") or "").strip()
            if post_url and re.search(r"\.(jpg|jpeg|png|gif|webp|mp4)(\?|$)", post_url, re.IGNORECASE):
                media_urls.append((post_url, "video" if post_url.lower().endswith(".mp4") else "image"))

            thumb = str(post_raw.get("thumbnail") or "").strip()
            if thumb and thumb.lower() not in {"self", "default", "nsfw", "spoiler", ""} and thumb.startswith("http"):
                media_urls.append((thumb, "thumbnail"))

            preview = post_raw.get("preview")
            if isinstance(preview, dict):
                for image in preview.get("images") or []:
                    if not isinstance(image, dict):
                        continue
                    source = image.get("source")
                    if isinstance(source, dict) and source.get("url"):
                        media_urls.append((str(source["url"]).replace("&amp;", "&"), "image"))

            media_metadata = post_raw.get("media_metadata") or {}
            if isinstance(media_metadata, dict):
                for meta in media_metadata.values():
                    if not isinstance(meta, dict):
                        continue
                    source_url = meta.get("s", {}).get("u") if isinstance(meta.get("s"), dict) else None
                    if source_url:
                        media_urls.append((str(source_url).replace("&amp;", "&"), "image"))

            for comment in comments:
                for text in (str(comment.get("body") or ""), str(comment.get("body_html") or "")):
                    media_urls.extend(_extract_reddit_media_urls(text))

            unique_media: list[tuple[str, str]] = []
            seen_urls: set[str] = set()
            for url, media_type in media_urls:
                normalized_url = _sanitize_reddit_media_url(url)
                if not normalized_url or normalized_url in seen_urls:
                    continue
                seen_urls.add(normalized_url)
                unique_media.append((normalized_url, media_type))
            result["media_queued"] = len(unique_media)

            def _mirror_single(url_mtype: tuple[str, str]) -> tuple[str, bool, Exception | None]:
                source_url, media_type = url_mtype
                try:
                    mirrored = mirror_reddit_media(
                        source_url=source_url,
                        reddit_post_id=pid,
                        reddit_comment_id=None,
                        media_type=media_type,
                    )
                    return source_url, isinstance(mirrored, dict) and mirrored.get("status") == "mirrored", None
                except Exception as exc:  # noqa: BLE001
                    return source_url, False, exc

            media_workers = min(8, len(unique_media)) if unique_media else 1
            if unique_media:
                with ThreadPoolExecutor(max_workers=media_workers, thread_name_prefix="media") as media_pool:
                    for completed_url, mirrored_ok, mirror_exc in media_pool.map(_mirror_single, unique_media):
                        if mirror_exc:
                            logger.warning(
                                "[sync_details_media_mirror_failed] post_id=%s url=%s error=%s",
                                pid,
                                completed_url,
                                mirror_exc,
                            )
                        elif mirrored_ok:
                            result["media_mirrored"] += 1

            if has_detail_scraped_at_value:
                with pg.db_connection() as conn:
                    with pg.db_cursor(conn=conn) as cur:
                        cur.execute(
                            """
                            update social.reddit_posts
                            set detail_scraped_at = now(),
                                updated_at = now()
                            where reddit_post_id = %s
                            """,
                            [pid],
                        )
        except Exception as post_exc:  # noqa: BLE001
            logger.warning("[sync_details_post_failed] post_id=%s error=%s", pid, post_exc)
            result["error"] = {
                "phase": "details",
                "reddit_post_id": pid,
                "error": f"{post_exc.__class__.__name__}: {post_exc}",
            }

        return result

    detail_workers = min(4, len(target_posts)) if target_posts else 1
    with ThreadPoolExecutor(max_workers=detail_workers, thread_name_prefix="detail") as pool:
        futures = {
            pool.submit(
                _process_single_detail_post,
                row,
                has_detail_scraped_at_value=has_detail_scraped_at,
            ): row
            for row in target_posts
        }
        for future in as_completed(futures):
            result = future.result()
            comments_upserted += result["comments_upserted"]
            media_queued += result["media_queued"]
            media_mirrored += result["media_mirrored"]
            if result.get("error"):
                detail_errors.append(result["error"])
            detail_posts_done += 1
            apply_progress(
                {
                    "detail_posts_done": detail_posts_done,
                    "comments_upserted": comments_upserted,
                    "media_queued": media_queued,
                    "media_mirrored": media_mirrored,
                }
            )

    return {
        "status": "completed" if not detail_errors else "partial",
        "detail_posts_total": len(target_posts),
        "detail_posts_done": detail_posts_done,
        "comments_upserted": comments_upserted,
        "media_queued": media_queued,
        "media_mirrored": media_mirrored,
        "errors": detail_errors[:50],
        "error_count": len(detail_errors),
    }


def execute_refresh_run(
    run_id: str,
    *,
    preclaimed_run: dict[str, Any] | None = None,
    worker_id: str | None = None,
    raise_on_failure: bool = True,
) -> dict[str, Any]:
    run = (
        dict(preclaimed_run)
        if isinstance(preclaimed_run, dict)
        else _claim_refresh_run_for_execution(
            run_id,
            worker_id=worker_id,
        )
    )
    claim_token = str(run.get("claim_token") or "").strip() or None

    request_payload = run.get("request_payload") if isinstance(run.get("request_payload"), dict) else {}
    progress: dict[str, Any] = _base_progress_snapshot()
    last_progress_emit = 0.0
    monotonic_fields = {
        "listing_pages_fetched",
        "search_pages_fetched",
        "rows_discovered_raw",
        "rows_matched",
        "comments_targets_total",
        "comments_targets_done",
        "comments_rows_upserted",
        "detail_posts_total",
        "detail_posts_done",
        "comments_upserted",
        "media_queued",
        "media_mirrored",
    }

    logger.info(
        "[reddit_refresh_execute_start] run_id=%s mode=%s worker_id=%s claim_token=%s",
        run_id[:8],
        str(request_payload.get("mode") or "sync_posts").strip() or "sync_posts",
        str(worker_id or "").strip() or _default_worker_id(),
        claim_token[:8] if claim_token else None,
    )

    def emit_progress(*, force: bool = False) -> None:
        nonlocal last_progress_emit
        now = time.monotonic()
        if not force and (now - last_progress_emit) < 3.0:
            return
        progress["updated_at"] = datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")
        _touch_refresh_run_heartbeat(run_id=run_id, claim_token=claim_token)
        _update_run(run_id, status="running", diagnostics={"progress": dict(progress)}, claim_token=claim_token)
        last_progress_emit = now

    def apply_progress(update: dict[str, Any], *, force: bool = False) -> None:
        for key, value in update.items():
            if value is None:
                continue
            if key in monotonic_fields:
                progress[key] = max(_safe_int(progress.get(key)), _safe_int(value))
            else:
                progress[key] = value
        emit_progress(force=force)

    emit_progress(force=True)

    try:
        run_mode = str(request_payload.get("mode") or "sync_posts").strip()
        if run_mode == "sync_details":
            community_id = str(run.get("community_id") or "").strip()
            season_id = str(run.get("season_id") or "").strip()
            period_key = str(run.get("period_key") or "").strip()
            force_rescrape = bool(request_payload.get("force_rescrape"))
            detail_result = _run_detail_sync_phase(
                community_id=community_id,
                season_id=season_id,
                period_key=period_key,
                force_rescrape=force_rescrape,
                progress=progress,
                apply_progress=apply_progress,
            )
            apply_progress({"stage": "finalizing"}, force=True)

            status = str(detail_result.get("status") or "completed")
            terminal_summary = _build_terminal_summary(
                mode="sync_details",
                status=status,
                progress={**progress, "stage": "finalizing"},
                error_count=_safe_int(detail_result.get("error_count")),
                force_rescrape=force_rescrape,
            )
            diagnostics = {
                "mode": "sync_details",
                "force_rescrape": force_rescrape,
                "detail_posts_total": _safe_int(detail_result.get("detail_posts_total")),
                "detail_posts_done": _safe_int(detail_result.get("detail_posts_done")),
                "comments_upserted": _safe_int(detail_result.get("comments_upserted")),
                "media_queued": _safe_int(detail_result.get("media_queued")),
                "media_mirrored": _safe_int(detail_result.get("media_mirrored")),
                "errors": detail_result.get("errors") or [],
                "error_count": _safe_int(detail_result.get("error_count")),
                "terminal_summary": terminal_summary,
                "lifecycle": {
                    "worker_id": str(worker_id or "").strip() or None,
                    "claim_token_prefix": claim_token[:8] if claim_token else None,
                    "claim_released": True,
                },
                "progress": {
                    **progress,
                    "stage": "finalizing",
                    "updated_at": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
                },
            }

            logger.info(
                (
                    "[reddit_refresh_execute_complete] run_id=%s mode=sync_details "
                    "status=%s detail_posts_done=%s error_count=%s"
                ),
                run_id[:8],
                status,
                _safe_int(detail_result.get("detail_posts_done")),
                _safe_int(detail_result.get("error_count")),
            )

            _update_run(
                run_id,
                status=status,
                diagnostics=diagnostics,
                total_rows=_safe_int(detail_result.get("detail_posts_total")),
                matched_rows=max(
                    0,
                    _safe_int(detail_result.get("detail_posts_done")) - _safe_int(detail_result.get("error_count")),
                ),
                set_completed=True,
                claim_token=claim_token,
                release_claim=True,
            )
            return get_refresh_run(run_id)

        coverage_mode = _normalize_coverage_mode(request_payload.get("coverage_mode"))
        discover_payload = dict(request_payload)
        if coverage_mode == "max_coverage":
            discover_payload["max_pages"] = max(
                _coerce_int(
                    discover_payload.get("max_pages"),
                    default=REDDIT_ADAPTIVE_DEEP_MAX_PAGES,
                    minimum=1,
                    maximum=10_000,
                ),
                REDDIT_ADAPTIVE_DEEP_MAX_PAGES,
            )
            discover_payload["max_backfill_queries"] = max(
                _coerce_int(
                    discover_payload.get("max_backfill_queries"),
                    default=REDDIT_ADAPTIVE_DEEP_MAX_BACKFILL_QUERIES,
                    minimum=1,
                    maximum=30,
                ),
                REDDIT_ADAPTIVE_DEEP_MAX_BACKFILL_QUERIES,
            )
            discover_payload["max_backfill_pages_per_query"] = max(
                _coerce_int(
                    discover_payload.get("max_backfill_pages_per_query"),
                    default=REDDIT_ADAPTIVE_DEEP_MAX_BACKFILL_PAGES_PER_QUERY,
                    minimum=1,
                    maximum=50,
                ),
                REDDIT_ADAPTIVE_DEEP_MAX_BACKFILL_PAGES_PER_QUERY,
            )

        pass_results: list[dict[str, Any]] = []
        pass_summaries: list[dict[str, Any]] = []

        first_result = _discover_window(discover_payload, progress_callback=apply_progress)
        pass_results.append(first_result)
        pass_summaries.append(_build_pass_summary(1, first_result))

        incomplete_listing, incomplete_backfill = _is_result_incomplete(first_result)
        if coverage_mode == "adaptive_deep" and (incomplete_listing or incomplete_backfill):
            apply_progress({"stage": "discovering_posts"}, force=True)
            second_pass_payload = dict(request_payload)
            second_pass_payload["max_pages"] = max(
                _coerce_int(
                    second_pass_payload.get("max_pages"),
                    default=REDDIT_ADAPTIVE_DEEP_MAX_PAGES,
                    minimum=1,
                    maximum=10_000,
                ),
                REDDIT_ADAPTIVE_DEEP_MAX_PAGES,
            )
            second_pass_payload["max_backfill_queries"] = max(
                _coerce_int(
                    second_pass_payload.get("max_backfill_queries"),
                    default=REDDIT_ADAPTIVE_DEEP_MAX_BACKFILL_QUERIES,
                    minimum=1,
                    maximum=30,
                ),
                REDDIT_ADAPTIVE_DEEP_MAX_BACKFILL_QUERIES,
            )
            second_pass_payload["max_backfill_pages_per_query"] = max(
                _coerce_int(
                    second_pass_payload.get("max_backfill_pages_per_query"),
                    default=REDDIT_ADAPTIVE_DEEP_MAX_BACKFILL_PAGES_PER_QUERY,
                    minimum=1,
                    maximum=50,
                ),
                REDDIT_ADAPTIVE_DEEP_MAX_BACKFILL_PAGES_PER_QUERY,
            )

            # Optimization: only run incomplete parts on second pass. Collect
            # post IDs already seen to avoid re-processing.
            first_threads = first_result.get("threads") if isinstance(first_result.get("threads"), list) else []
            seen_post_ids = {
                str(t.get("reddit_post_id") or "").strip()
                for t in first_threads
                if isinstance(t, dict) and str(t.get("reddit_post_id") or "").strip()
            }

            # If listing was complete, skip it on the second pass (set max_pages=0)
            if not incomplete_listing:
                second_pass_payload["max_pages"] = 0
                logger.info(
                    "[adaptive_deep_pass2] listing already complete, skipping listing phase. seen_posts=%d",
                    len(seen_post_ids),
                )

            # If backfill was complete, skip it
            if not incomplete_backfill:
                second_pass_payload["max_backfill_queries"] = 0
                logger.info("[adaptive_deep_pass2] backfill already complete, skipping backfill phase.")

            # Pass seen_post_ids to help deduplicate on merge
            second_pass_payload["_seen_post_ids"] = seen_post_ids

            second_result = _discover_window(second_pass_payload, progress_callback=apply_progress)
            pass_results.append(second_result)
            pass_summaries.append(_build_pass_summary(2, second_result))

        result = _merge_discovery_pass_results(pass_results)
        apply_progress(
            {
                "rows_matched": int(result.get("totals", {}).get("matched_rows") or 0),
            },
            force=True,
        )

        comment_errors = 0
        comments_upserted = 0
        run_full_sync = run_mode == "sync_full"
        fetch_comments = bool(request_payload.get("fetch_comments")) and not run_full_sync
        comment_delta_only = bool(request_payload.get("comment_delta_only", True))
        preserve_existing_assignments = bool(request_payload.get("preserve_existing_assignments", True))
        comment_posts_cap = _env_int(
            "REDDIT_COMMENTS_MAX_POSTS_PER_RUN",
            REDDIT_MAX_COMMENTS_POSTS_PER_RUN_DEFAULT,
            minimum=0,
            maximum=500,
        )
        target_threads = result.get("threads") if isinstance(result.get("threads"), list) else []
        comment_targets: list[dict[str, Any]] = []
        existing_comment_counts: dict[str, int] = {}
        apply_progress({"stage": "persisting_posts"}, force=True)

        with pg.db_connection() as conn:
            if fetch_comments and comment_delta_only and comment_posts_cap > 0:
                post_ids = [
                    str((thread if isinstance(thread, dict) else {}).get("reddit_post_id") or "").strip()
                    for thread in target_threads
                ]
                existing_comment_counts = _load_existing_post_comment_counts(
                    post_ids=[post_id for post_id in post_ids if post_id],
                    conn=conn,
                )
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
        if fetch_comments and comment_posts_cap > 0:
            if comment_delta_only:
                for thread in target_threads:
                    if not isinstance(thread, dict):
                        continue
                    post_id = str(thread.get("reddit_post_id") or "").strip()
                    if not post_id:
                        continue
                    discovered_comments = _safe_int(thread.get("num_comments"))
                    existing_comments = existing_comment_counts.get(post_id)
                    if existing_comments is None or discovered_comments != existing_comments:
                        comment_targets.append(thread)
            else:
                comment_targets = [thread for thread in target_threads if isinstance(thread, dict)]
            if len(comment_targets) > comment_posts_cap:
                comment_targets = comment_targets[:comment_posts_cap]

        pending_comment_rows: list[dict[str, Any]] = []
        apply_progress({"stage": "persisting_period_matches"}, force=True)
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
                preserve_existing_assignments=preserve_existing_assignments,
            )

        apply_progress(
            {
                "stage": "fetching_comments",
                "comments_targets_total": len(comment_targets),
            },
            force=True,
        )

        # Parallel comment fetching: use ThreadPoolExecutor to fetch multiple
        # comment trees concurrently, then batch-upsert results.
        _comment_lock = threading.Lock()

        def _fetch_comments_for_thread(
            thread: dict[str, Any],
        ) -> tuple[str, list[dict[str, Any]] | None, Exception | None]:
            post_id = str(thread.get("reddit_post_id") or "").strip()
            if not post_id:
                return "", None, None
            try:
                comments = _fetch_post_comments_tree(post_id)
                return post_id, comments, None
            except Exception as exc:  # noqa: BLE001
                return post_id, None, exc

        comment_worker_count = min(5, len(comment_targets)) if comment_targets else 1
        with ThreadPoolExecutor(max_workers=comment_worker_count, thread_name_prefix="comments") as comment_pool:
            futures = {comment_pool.submit(_fetch_comments_for_thread, thread): thread for thread in comment_targets}
            for future in as_completed(futures):
                post_id, comments, exc = future.result()
                if exc:
                    comment_errors += 1
                    logger.warning("[reddit_refresh_comments_failed] post_id=%s error=%s", post_id, exc)
                elif comments:
                    pending_comment_rows.extend(comments)
                    if len(pending_comment_rows) >= 2_000:
                        with pg.db_connection() as conn:
                            comments_upserted += _upsert_comments(pending_comment_rows, conn=conn)
                        apply_progress({"comments_rows_upserted": comments_upserted})
                        pending_comment_rows.clear()
                apply_progress({"comments_targets_done": _safe_int(progress.get("comments_targets_done")) + 1})

        if pending_comment_rows:
            with pg.db_connection() as conn:
                comments_upserted += _upsert_comments(pending_comment_rows, conn=conn)
            apply_progress({"comments_rows_upserted": comments_upserted})

        search_backfill = result.get("search_backfill") if isinstance(result.get("search_backfill"), dict) else None
        seed_urls = result.get("seed_urls") if isinstance(result.get("seed_urls"), dict) else None
        incomplete_listing, incomplete_backfill = _is_result_incomplete(result)
        # In max_coverage mode, treat listing incompleteness as non-fatal when
        # search backfill completed. Reddit listing pagination can exhaust well
        # before the period start on high-volume communities, while backfill
        # still delivers complete in-window coverage.
        listing_incomplete_non_fatal = (
            coverage_mode == "max_coverage" and incomplete_listing and not incomplete_backfill and bool(search_backfill)
        )
        discovery_status = (
            "partial"
            if (incomplete_backfill or (incomplete_listing and not listing_incomplete_non_fatal))
            else "completed"
        )
        detail_result: dict[str, Any] | None = None
        if run_full_sync:
            detail_result = _run_detail_sync_phase(
                community_id=str(run.get("community_id") or "").strip(),
                season_id=str(run.get("season_id") or "").strip(),
                period_key=str(run.get("period_key") or "").strip(),
                force_rescrape=bool(request_payload.get("force_rescrape")),
                progress=progress,
                apply_progress=apply_progress,
            )
        apply_progress({"stage": "finalizing"}, force=True)
        status = (
            "partial"
            if discovery_status == "partial" or str((detail_result or {}).get("status") or "completed") == "partial"
            else "completed"
        )
        final_completeness = {
            "listing_complete": not incomplete_listing,
            "backfill_complete": not incomplete_backfill,
        }
        detail_errors = (
            (detail_result or {}).get("errors") if isinstance((detail_result or {}).get("errors"), list) else []
        )
        error_count = comment_errors + len(detail_errors)
        useful_posts_stored = any(
            _safe_int(result.get("totals", {}).get(metric)) > 0
            for metric in ("fetched_rows", "matched_rows", "tracked_flair_rows")
        )
        coverage_incomplete = discovery_status == "partial" and useful_posts_stored

        diagnostics = {
            "mode": "sync_full" if run_full_sync else "sync_posts",
            "coverage_mode": coverage_mode,
            "passes_run": len(pass_results),
            "passes": pass_summaries,
            "final_completeness": final_completeness,
            "listing_pages_fetched": result.get("listing_pages_fetched"),
            "max_pages_applied": result.get("max_pages_applied"),
            "window_exhaustive_complete": result.get("window_exhaustive_complete"),
            "search_backfill": search_backfill,
            "seed_urls": seed_urls,
            "window_start": result.get("window_start"),
            "window_end": result.get("window_end"),
            "terms": result.get("terms"),
            "hints": result.get("hints"),
            "discovered_flairs": sorted(
                {
                    str(thread.get("link_flair_text") or "").strip()
                    for thread in target_threads
                    if isinstance(thread, dict) and str(thread.get("link_flair_text") or "").strip()
                }
            ),
            "comments": {
                "enabled": fetch_comments,
                "delta_only": comment_delta_only,
                "preserve_existing_assignments": preserve_existing_assignments,
                "attempted_posts": len(comment_targets),
                "candidate_posts": len(target_threads),
                "skipped_posts": max(0, len(target_threads) - len(comment_targets)),
                "upserted_rows": comments_upserted,
                "errors": comment_errors,
            },
            "force_rescrape": bool(request_payload.get("force_rescrape")),
            "detail_posts_total": _safe_int((detail_result or {}).get("detail_posts_total")),
            "detail_posts_done": _safe_int((detail_result or {}).get("detail_posts_done")),
            "comments_upserted": _safe_int((detail_result or {}).get("comments_upserted")),
            "media_queued": _safe_int((detail_result or {}).get("media_queued")),
            "media_mirrored": _safe_int((detail_result or {}).get("media_mirrored")),
            "errors": detail_errors,
            "error_count": error_count,
            "status_resolution": (
                "listing_incomplete_backfill_complete_max_coverage"
                if listing_incomplete_non_fatal
                else ("coverage_incomplete" if coverage_incomplete else "strict_completeness")
            ),
            "terminal_summary": _build_terminal_summary(
                mode="sync_full" if run_full_sync else "sync_posts",
                status=status,
                progress={**progress, "stage": "finalizing"},
                error_count=error_count,
                force_rescrape=bool(request_payload.get("force_rescrape")) if run_full_sync else None,
            ),
            "lifecycle": {
                "worker_id": str(worker_id or "").strip() or None,
                "claim_token_prefix": claim_token[:8] if claim_token else None,
                "claim_released": True,
            },
            "progress": {
                **progress,
                "stage": "finalizing",
                "updated_at": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
            },
            "result": result,
        }
        if coverage_incomplete:
            diagnostics["failure_reason_code"] = "coverage_incomplete"
            diagnostics["operator_hint"] = _default_operator_hint("coverage_incomplete", status=status)

        logger.info(
            "[reddit_refresh_execute_complete] run_id=%s mode=%s status=%s rows_matched=%s error_count=%s",
            run_id[:8],
            "sync_full" if run_full_sync else "sync_posts",
            status,
            int(result.get("totals", {}).get("matched_rows") or 0),
            error_count,
        )

        _update_run(
            run_id,
            status=status,
            diagnostics=diagnostics,
            total_rows=int(result.get("totals", {}).get("fetched_rows") or 0),
            matched_rows=int(result.get("totals", {}).get("matched_rows") or 0),
            tracked_flair_rows=int(result.get("totals", {}).get("tracked_flair_rows") or 0),
            set_completed=True,
            claim_token=claim_token,
            release_claim=True,
        )
        return get_refresh_run(run_id)
    except Exception as exc:  # noqa: BLE001
        if isinstance(exc, RedditRefreshError) and exc.status == 403:
            logger.warning("[reddit_refresh_partial_403] run_id=%s", run_id)
            partial_diagnostics = {
                "error_type": exc.__class__.__name__,
                "failure_reason_code": "reddit_http_403",
                "operator_hint": _default_operator_hint("reddit_http_403", status="partial"),
                "terminal_summary": _build_terminal_summary(
                    mode=str(request_payload.get("mode") or "sync_posts").strip() or "sync_posts",
                    status="partial",
                    progress={**progress, "stage": "finalizing"},
                    error_count=1,
                ),
                "lifecycle": {
                    "worker_id": str(worker_id or "").strip() or None,
                    "claim_token_prefix": claim_token[:8] if claim_token else None,
                    "claim_released": True,
                },
                "progress": {
                    **progress,
                    "stage": "finalizing",
                    "updated_at": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
                },
            }
            _update_run(
                run_id,
                status="partial",
                diagnostics=partial_diagnostics,
                error_message=str(exc),
                set_completed=True,
                claim_token=claim_token,
                release_claim=True,
            )
            return get_refresh_run(run_id)
        logger.exception("[reddit_refresh_failed] run_id=%s", run_id)
        _update_run(
            run_id,
            status="failed",
            diagnostics={
                "error_type": exc.__class__.__name__,
                "failure_reason_code": _derive_failure_reason_code(
                    status="failed",
                    diagnostics={},
                    error_message=str(exc),
                    stalled=False,
                ),
                "operator_hint": _derive_operator_hint(
                    status="failed",
                    diagnostics={},
                    error_message=str(exc),
                    stalled=False,
                ),
                "terminal_summary": _build_terminal_summary(
                    mode=str(request_payload.get("mode") or "sync_posts").strip() or "sync_posts",
                    status="failed",
                    progress=progress,
                    error_count=1,
                ),
                "lifecycle": {
                    "worker_id": str(worker_id or "").strip() or None,
                    "claim_token_prefix": claim_token[:8] if claim_token else None,
                    "claim_released": True,
                },
                "progress": {
                    **progress,
                    "updated_at": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
                },
            },
            error_message=str(exc),
            set_completed=True,
            claim_token=claim_token,
            release_claim=True,
        )
        if raise_on_failure:
            raise
        return get_refresh_run(run_id)


def run_reddit_refresh_worker_loop(
    *,
    worker_id: str | None = None,
    poll_seconds: float = 2.0,
    once: bool = False,
) -> int:
    normalized_worker = str(worker_id or "").strip() or _default_worker_id()
    safe_poll = max(0.2, float(poll_seconds))
    logger.info(
        "[reddit_refresh_worker_loop_start] worker_id=%s once=%s poll_seconds=%.2f",
        normalized_worker,
        once,
        safe_poll,
    )

    while True:
        claimed = claim_next_refresh_run(worker_id=normalized_worker)
        if not claimed:
            if once:
                logger.info("[reddit_refresh_worker_no_work] worker_id=%s once=true", normalized_worker)
                return 1
            time.sleep(safe_poll)
            continue

        run_id = str(claimed.get("id") or "").strip()
        logger.info(
            "[reddit_refresh_claimed] worker_id=%s run_id=%s attempt=%s",
            normalized_worker,
            run_id[:8] if run_id else None,
            _safe_int(claimed.get("attempt_count")),
        )
        try:
            execute_refresh_run(
                run_id,
                preclaimed_run=claimed,
                worker_id=normalized_worker,
                raise_on_failure=False,
            )
        except Exception:  # noqa: BLE001
            logger.exception(
                "[reddit_refresh_worker_run_error] worker_id=%s run_id=%s",
                normalized_worker,
                run_id[:8] if run_id else None,
            )
        if once:
            logger.info("[reddit_refresh_worker_once_complete] worker_id=%s run_id=%s", normalized_worker, run_id[:8])
            return 0


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
               claimed_by_worker_id,
               claim_token,
               lease_expires_at,
               heartbeat_at,
               attempt_count,
               next_retry_at,
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

    active_counts = (
        pg.fetch_one(
            """
        select
          count(*) filter (where status = 'running') as running_total,
          count(*) filter (where status = 'queued') as queued_total,
          count(*) filter (where status = 'queued' and created_at < %s::timestamptz) as queued_ahead
        from social.reddit_refresh_runs
        where status in ('queued', 'running')
          and community_id = %s
        """,
            [row.get("created_at"), row.get("community_id")],
        )
        or {}
    )

    running_total = _safe_int(active_counts.get("running_total"))
    queued_total = _safe_int(active_counts.get("queued_total"))
    this_run_is_running = 1 if str(row.get("status") or "").strip().lower() == "running" else 0
    this_run_is_queued = 1 if str(row.get("status") or "").strip().lower() == "queued" else 0
    queued_ahead = _safe_int(active_counts.get("queued_ahead"))
    queue_position = queued_ahead + 1 if this_run_is_queued else None

    diagnostics = row.get("diagnostics") if isinstance(row.get("diagnostics"), dict) else {}
    payload = diagnostics.get("result") if isinstance(diagnostics.get("result"), dict) else None
    request_payload = row.get("request_payload") if isinstance(row.get("request_payload"), dict) else {}

    run_meta = _build_refresh_run_meta(row)
    return {
        "run_id": row.get("id"),
        "community_id": row.get("community_id"),
        "season_id": row.get("season_id"),
        "period_key": row.get("period_key"),
        "subreddit": row.get("subreddit"),
        "execution_owner": execution_owner_label(),
        "execution_mode_canonical": canonical_execution_mode(),
        "execution_backend_canonical": execution_backend_canonical(),
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
            "queued_ahead": queued_ahead,
        },
        "queue_position": queue_position,
        "active_jobs": running_total + queued_total,
        "run_config_hash": str(request_payload.get("run_config_hash") or "").strip() or None,
        "claimed_by_worker_id": str(row.get("claimed_by_worker_id") or "").strip() or None,
        "claim_token": str(row.get("claim_token") or "").strip() or None,
        "lease_expires_at": _iso_utc(_parse_iso(row.get("lease_expires_at"))),
        "heartbeat_at": _iso_utc(_parse_iso(row.get("heartbeat_at"))),
        "attempt_count": _safe_int(row.get("attempt_count")),
        "next_retry_at": _iso_utc(_parse_iso(row.get("next_retry_at"))),
        "diagnostics": diagnostics,
        "discovered_flairs": diagnostics.get("discovered_flairs") or [],
        "discovery": payload,
        "started_at": _iso_utc(_parse_iso(row.get("started_at"))),
        "completed_at": _iso_utc(_parse_iso(row.get("completed_at"))),
        "created_at": _iso_utc(_parse_iso(row.get("created_at"))),
        "updated_at": _iso_utc(_parse_iso(row.get("updated_at"))),
        "phase": run_meta.get("phase"),
        "partial_failures": run_meta.get("partial_failures"),
        "stalled": run_meta.get("stalled"),
        "failure_reason_code": run_meta.get("failure_reason_code"),
        "operator_hint": run_meta.get("operator_hint"),
    }


def build_reddit_refresh_save_proof(run_id: str) -> dict[str, Any]:
    row = pg.fetch_one(
        """
        select id::text as id,
               community_id,
               season_id,
               period_key,
               subreddit,
               status,
               total_rows,
               matched_rows,
               tracked_flair_rows,
               diagnostics
        from social.reddit_refresh_runs
        where id = %s::uuid
        """,
        [run_id],
    )
    if not row:
        raise ValueError("Refresh run not found")

    materialized_row = (
        pg.fetch_one(
            """
            select count(*)::int as total
            from social.reddit_period_post_matches
            where run_id = %s::uuid
            """,
            [run_id],
        )
        or {}
    )
    post_row = (
        pg.fetch_one(
            """
            select count(distinct m.post_id)::int as total
            from social.reddit_period_post_matches m
            join social.reddit_posts p on p.id = m.post_id
            where m.run_id = %s::uuid
            """,
            [run_id],
        )
        or {}
    )
    diagnostics = row.get("diagnostics") if isinstance(row.get("diagnostics"), dict) else {}
    result = diagnostics.get("result") if isinstance(diagnostics.get("result"), dict) else {}
    totals = result.get("totals") if isinstance(result.get("totals"), dict) else {}
    fetched_count = max(_safe_int(row.get("total_rows")), _safe_int(totals.get("fetched_rows")))
    upserted_count = max(
        _safe_int(row.get("matched_rows")),
        _safe_int(totals.get("matched_rows")),
        _safe_int(post_row.get("total")),
    )
    materialized_count = _safe_int(materialized_row.get("total"))
    return {
        "run_id": str(row.get("id") or ""),
        "community_id": row.get("community_id"),
        "season_id": row.get("season_id"),
        "period_key": row.get("period_key"),
        "subreddit": row.get("subreddit"),
        "status": row.get("status"),
        "fetched_count": fetched_count,
        "upserted_count": upserted_count,
        "materialized_count": materialized_count,
        "tracked_flair_rows": _safe_int(row.get("tracked_flair_rows")),
        "verified": fetched_count > 0 and upserted_count > 0 and materialized_count > 0,
    }


def list_refresh_runs(
    *,
    community_id: str | None = None,
    season_id: str | None = None,
    period_key: str | None = None,
    statuses: list[str] | None = None,
    limit: int = 25,
) -> list[dict[str, Any]]:
    normalized_limit = max(1, min(int(limit), 100))
    normalized_statuses = [
        str(status or "").strip().lower() for status in (statuses or []) if str(status or "").strip()
    ]
    allowed_statuses = {"queued", "running", "completed", "partial", "failed", "cancelled"}
    if normalized_statuses and any(status not in allowed_statuses for status in normalized_statuses):
        raise ValueError("status must be one of: queued, running, completed, partial, failed, cancelled")

    where_clauses = ["1 = 1"]
    params: list[Any] = []

    if community_id:
        where_clauses.append("community_id = %s::uuid")
        params.append(community_id)
    if season_id:
        where_clauses.append("season_id = %s::uuid")
        params.append(season_id)
    if period_key:
        where_clauses.append("period_key = %s")
        params.append(period_key)
    if normalized_statuses:
        where_clauses.append("lower(status) = any(%s)")
        params.append(normalized_statuses)

    params.append(normalized_limit)

    rows = pg.fetch_all(
        f"""
        select id,
               community_id,
               season_id,
               period_key,
               subreddit,
               status,
               error_message,
               request_payload,
               claimed_by_worker_id,
               heartbeat_at,
               lease_expires_at,
               attempt_count,
               next_retry_at,
               started_at,
               completed_at,
               created_at,
               updated_at
        from social.reddit_refresh_runs
        where {" and ".join(where_clauses)}
        order by created_at desc
        limit %s
        """,
        params,
    )

    results: list[dict[str, Any]] = []
    for row in rows:
        payload = row.get("request_payload") if isinstance(row.get("request_payload"), dict) else {}
        run_meta = _build_refresh_run_meta(row)
        results.append(
            {
                "run_id": row.get("id"),
                "community_id": row.get("community_id"),
                "season_id": row.get("season_id"),
                "period_key": row.get("period_key"),
                "subreddit": row.get("subreddit"),
                "execution_owner": execution_owner_label(),
                "execution_mode_canonical": canonical_execution_mode(),
                "execution_backend_canonical": execution_backend_canonical(),
                "status": row.get("status"),
                "error": row.get("error_message"),
                "client_session_id": str(payload.get("client_session_id") or "").strip() or None,
                "client_workflow_id": str(payload.get("client_workflow_id") or "").strip() or None,
                "claimed_by_worker_id": str(row.get("claimed_by_worker_id") or "").strip() or None,
                "heartbeat_at": _iso_utc(_parse_iso(row.get("heartbeat_at"))),
                "lease_expires_at": _iso_utc(_parse_iso(row.get("lease_expires_at"))),
                "attempt_count": _safe_int(row.get("attempt_count")),
                "next_retry_at": _iso_utc(_parse_iso(row.get("next_retry_at"))),
                "started_at": _iso_utc(_parse_iso(row.get("started_at"))),
                "completed_at": _iso_utc(_parse_iso(row.get("completed_at"))),
                "created_at": _iso_utc(_parse_iso(row.get("created_at"))),
                "updated_at": _iso_utc(_parse_iso(row.get("updated_at"))),
                "phase": run_meta.get("phase"),
                "partial_failures": run_meta.get("partial_failures"),
                "stalled": run_meta.get("stalled"),
                "failure_reason_code": run_meta.get("failure_reason_code"),
                "operator_hint": run_meta.get("operator_hint"),
            }
        )
    return results


def _fetch_latest_refresh_runs_for_season(
    *,
    community_id: str,
    season_id: str,
    canonical_only: bool = False,
) -> list[dict[str, Any]]:
    if canonical_only:
        container_key_sql = _canonical_reddit_container_key_sql(
            period_key_expr="period_key",
            request_payload_expr="request_payload",
        )
        rows = pg.fetch_all(
            f"""
            with scoped as (
              select
                *,
                {container_key_sql} as canonical_container_key
              from social.reddit_refresh_runs
              where community_id = %s
                and season_id = %s
            ),
            ranked as (
              select *,
                     row_number() over (
                       partition by canonical_container_key
                       order by created_at desc, updated_at desc, id desc
                     ) as rn
              from scoped
              where canonical_container_key in ('period-preseason', 'period-postseason')
                 or canonical_container_key ~ '^episode-[0-9]+$'
            )
            select *
            from ranked
            where rn = 1
            order by created_at asc, canonical_container_key asc
            """,
            [community_id, season_id],
        )
    else:
        rows = pg.fetch_all(
            """
            with ranked as (
              select *,
                     row_number() over (
                       partition by period_key
                       order by created_at desc, updated_at desc, id desc
                     ) as rn
              from social.reddit_refresh_runs
              where community_id = %s
                and season_id = %s
            )
            select *
            from ranked
            where rn = 1
            order by created_at asc, period_key asc
            """,
            [community_id, season_id],
        )
    return [dict(row) for row in rows]


def _classify_reddit_backfill_candidate(row: Mapping[str, Any]) -> tuple[bool, str | None]:
    status = str(row.get("status") or "").strip().lower()
    diagnostics = row.get("diagnostics") if isinstance(row.get("diagnostics"), dict) else {}
    error_message = str(row.get("error_message") or "").strip() or None
    failure_reason_code = _derive_failure_reason_code(
        status=status,
        diagnostics=diagnostics,
        error_message=error_message,
        stalled=False,
    )
    tracked_flair_rows = _safe_int(row.get("tracked_flair_rows"))
    if status == "failed":
        return True, "failed"
    if failure_reason_code in {"reddit_http_403", "coverage_incomplete"}:
        return True, failure_reason_code
    if tracked_flair_rows <= 0 and status in {"partial", "failed"}:
        return True, "zero_tracked"
    return False, None


def _fetch_reddit_container_enrichment_coverage(
    *,
    community_id: str,
    season_id: str,
) -> dict[str, dict[str, int]]:
    has_detail_scraped_at = _column_exists("social", "reddit_posts", "detail_scraped_at")
    detail_scraped_expr = "p.detail_scraped_at is not null" if has_detail_scraped_at else "false"
    canonical_container_sql = _canonical_reddit_match_container_key_sql(
        season_id=season_id,
        period_key_expr="m.period_key",
        period_start_expr="m.period_start",
        period_end_expr="m.period_end",
        posted_at_expr="p.posted_at",
    )
    rows = pg.fetch_all(
        f"""
        with scoped as (
          select
            {canonical_container_sql} as canonical_container_key,
            m.reddit_post_id,
            m.passes_flair_filter,
            {detail_scraped_expr} as detail_scraped,
            exists(
              select 1
              from social.reddit_comments rc
              where rc.reddit_post_id = m.reddit_post_id
            ) as has_saved_comments,
            exists(
              select 1
              from social.reddit_media_mirrors rmm
              where rmm.reddit_post_id = m.reddit_post_id
            ) as has_media
          from social.reddit_period_post_matches m
          join social.reddit_posts p on p.reddit_post_id = m.reddit_post_id
          where m.community_id = %s
            and m.season_id = %s
        ),
        dedup as (
          select distinct on (canonical_container_key, reddit_post_id)
                 canonical_container_key,
                 reddit_post_id,
                 passes_flair_filter,
                 detail_scraped,
                 has_saved_comments,
                 has_media
          from scoped
          where canonical_container_key = any(%s)
          order by canonical_container_key, reddit_post_id
        )
        select
          canonical_container_key,
          count(*) filter (where passes_flair_filter)::int as tracked_post_count,
          count(*) filter (where passes_flair_filter and detail_scraped)::int as detail_scraped_post_count,
          count(*) filter (where passes_flair_filter and has_saved_comments)::int as comment_saved_post_count,
          count(*) filter (where passes_flair_filter and has_media)::int as media_post_count
        from dedup
        group by canonical_container_key
        """,
        [community_id, season_id, _canonical_reddit_container_keys_for_season(season_id)],
    )
    coverage: dict[str, dict[str, int]] = {}
    for row in rows:
        key = str(row.get("canonical_container_key") or "").strip().lower()
        if not key:
            continue
        coverage[key] = {
            "tracked_post_count": _safe_int(row.get("tracked_post_count")),
            "detail_scraped_post_count": _safe_int(row.get("detail_scraped_post_count")),
            "comment_saved_post_count": _safe_int(row.get("comment_saved_post_count")),
            "media_post_count": _safe_int(row.get("media_post_count")),
        }
    return coverage


def list_reddit_refresh_backfill_targets(
    *,
    community_id: str,
    season_id: str,
    container_keys: list[str] | None = None,
    detail_refresh: bool = False,
) -> dict[str, Any]:
    normalized_container_keys = [
        str(item or "").strip().lower()
        for item in (container_keys or [])
        if _is_canonical_reddit_container_key(str(item or "").strip())
    ]
    requested_keys = set(normalized_container_keys)
    latest_rows = _fetch_latest_refresh_runs_for_season(
        community_id=community_id,
        season_id=season_id,
        canonical_only=True,
    )
    rows_by_container_key = {
        str(row.get("canonical_container_key") or row.get("period_key") or "").strip().lower(): row
        for row in latest_rows
        if str(row.get("canonical_container_key") or row.get("period_key") or "").strip()
    }
    enrichment_coverage = (
        _fetch_reddit_container_enrichment_coverage(community_id=community_id, season_id=season_id)
        if detail_refresh
        else {}
    )

    targets: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []

    if requested_keys:
        ordered_keys = sorted(requested_keys, key=_canonical_reddit_container_sort_key)
    else:
        ordered_keys = _canonical_reddit_container_keys_for_season(season_id)

    for period_key in ordered_keys:
        row = rows_by_container_key.get(period_key)
        if row is None:
            skipped.append(
                {
                    "container_key": period_key,
                    "reason": "no_previous_run",
                    "latest_run_id": None,
                    "latest_run_status": None,
                }
            )
            continue

        diagnostics = row.get("diagnostics") if isinstance(row.get("diagnostics"), dict) else {}
        request_payload = row.get("request_payload") if isinstance(row.get("request_payload"), dict) else {}
        latest_run_status = str(row.get("status") or "").strip().lower() or None
        latest_run_id = str(row.get("id") or "").strip() or None
        stale, stale_reason_code = _classify_reddit_backfill_candidate(row)

        coverage = enrichment_coverage.get(period_key) or {}
        needs_enrichment = bool(
            detail_refresh
            and (
                _safe_int(coverage.get("tracked_post_count")) > _safe_int(coverage.get("detail_scraped_post_count"))
                or _safe_int(coverage.get("tracked_post_count")) > _safe_int(coverage.get("comment_saved_post_count"))
                or _safe_int(coverage.get("tracked_post_count")) > _safe_int(coverage.get("media_post_count"))
            )
        )

        if requested_keys or stale or needs_enrichment:
            targets.append(
                {
                    "container_key": period_key,
                    "latest_run_id": latest_run_id,
                    "latest_run_status": latest_run_status,
                    "stale": stale,
                    "stale_reason_code": stale_reason_code,
                    "failure_reason_code": _derive_failure_reason_code(
                        status=latest_run_status or "",
                        diagnostics=diagnostics,
                        error_message=str(row.get("error_message") or "").strip() or None,
                        stalled=False,
                    ),
                    "operator_hint": _derive_operator_hint(
                        status=latest_run_status or "",
                        diagnostics=diagnostics,
                        error_message=str(row.get("error_message") or "").strip() or None,
                        stalled=False,
                    ),
                    "tracked_flair_rows": _safe_int(row.get("tracked_flair_rows")),
                    "matched_rows": _safe_int(row.get("matched_rows")),
                    "needs_enrichment": needs_enrichment,
                    "enrichment_coverage": coverage,
                    "request_payload": request_payload,
                }
            )
            continue

        skipped.append(
            {
                "container_key": period_key,
                "reason": "fresh_successful_run",
                "latest_run_id": latest_run_id,
                "latest_run_status": latest_run_status,
                "failure_reason_code": _derive_failure_reason_code(
                    status=latest_run_status or "",
                    diagnostics=diagnostics,
                    error_message=str(row.get("error_message") or "").strip() or None,
                    stalled=False,
                ),
                "tracked_flair_rows": _safe_int(row.get("tracked_flair_rows")),
                "needs_enrichment": needs_enrichment,
            }
        )

    return {
        "targets": targets,
        "skipped": skipped,
        "summary": {
            "requested_container_count": len(ordered_keys),
            "target_count": len(targets),
            "stale_container_count": sum(1 for item in targets if bool(item.get("stale"))),
            "enrichment_target_count": sum(1 for item in targets if bool(item.get("needs_enrichment"))),
            "requested_container_keys": ordered_keys,
        },
    }


def _reddit_backfill_sse_chunk(event_type: str, payload: dict[str, Any]) -> str:
    return f"event: {event_type}\ndata: {json.dumps(payload, ensure_ascii=True, default=str)}\n\n"


def _kickoff_reddit_refresh_run_for_backfill(
    *,
    serialized_payload: dict[str, Any],
    operation_worker_id: str,
) -> dict[str, Any]:
    run_row = create_or_reuse_refresh_run(payload=serialized_payload)
    run_id = str(run_row.get("id") or "").strip()
    if not run_id:
        raise RuntimeError("reddit_refresh_run_id_missing")

    reused = bool(run_row.get("reused"))
    run = get_refresh_run(run_id)
    execution_mode = canonical_execution_mode()
    execution_owner = execution_owner_label()
    execution_backend = execution_backend_canonical()
    should_dispatch_modal = execution_backend == "modal" and (
        not reused
        or (
            isinstance(run, dict)
            and str(run.get("status") or "").strip().lower() == "queued"
            and not str(run.get("claimed_by_worker_id") or "").strip()
            and not str(run.get("heartbeat_at") or "").strip()
        )
    )
    modal_dispatched = False
    if should_dispatch_modal:
        modal_ready, modal_reason = modal_dispatch_ready(function_name=modal_reddit_refresh_function_name())
        if not modal_ready:
            raise RuntimeError(f"REDDIT_REMOTE_DISPATCH_UNAVAILABLE:{modal_reason or 'modal_dispatch_unavailable'}")
        reddit_runtime_health = get_modal_reddit_runtime_health()
        if not bool(reddit_runtime_health.get("healthy")):
            missing_env = reddit_runtime_health.get("missing_env")
            missing_env_list = (
                [str(item).strip() for item in missing_env if str(item).strip()]
                if isinstance(missing_env, list)
                else []
            )
            missing_env_text = f" Missing: {', '.join(missing_env_list)}." if missing_env_list else ""
            raise RuntimeError(
                "REDDIT_REMOTE_RUNTIME_UNHEALTHY:"
                f"{str(reddit_runtime_health.get('reason') or 'reddit_runtime_unhealthy').strip()}{missing_env_text}"
            )
        modal_dispatched = dispatch_reddit_refresh(run_id=run_id)
        if not modal_dispatched:
            raise RuntimeError("REDDIT_REMOTE_DISPATCH_UNAVAILABLE:modal_dispatch_failed")
        metadata = modal_execution_metadata()
        execution_mode = metadata["execution_mode_canonical"]
        execution_owner = metadata["execution_owner"]
        execution_backend = metadata["execution_backend_canonical"]
        run = get_refresh_run(run_id)
    elif not reused and execution_mode == "local":
        execute_refresh_run(run_id, worker_id=operation_worker_id)
        run = get_refresh_run(run_id)

    if modal_dispatched and isinstance(run, dict):
        run = {**run, **modal_execution_metadata()}
    return {
        "run": run,
        "reused": reused,
        "execution_owner": execution_owner,
        "execution_mode_canonical": execution_mode,
        "execution_backend_canonical": execution_backend,
    }


def build_reddit_refresh_backfill_operation_producer(
    *,
    request_payload: dict[str, Any],
    operation_id: str | None = None,
):
    from trr_backend.repositories import admin_operations

    community_id = str(request_payload.get("community_id") or "").strip()
    season_id = str(request_payload.get("season_id") or "").strip()
    if not community_id:
        raise ValueError("request_payload.community_id is required")
    if not season_id:
        raise ValueError("request_payload.season_id is required")

    raw_container_keys_value = request_payload.get("container_keys")
    raw_container_keys = raw_container_keys_value if isinstance(raw_container_keys_value, list) else []
    requested_container_keys = [
        str(item or "").strip().lower()
        for item in raw_container_keys
        if _is_canonical_reddit_container_key(str(item or "").strip())
    ]
    mode = str(request_payload.get("mode") or "sync_full").strip().lower() or "sync_full"
    if mode not in {"sync_posts", "sync_details", "sync_full"}:
        mode = "sync_full"
    detail_refresh = bool(request_payload.get("detail_refresh"))
    poll_seconds = _env_float(
        "REDDIT_BACKFILL_OPERATION_POLL_SECONDS",
        REDDIT_BACKFILL_POLL_SECONDS_DEFAULT,
        minimum=0.5,
        maximum=30.0,
    )
    normalized_operation_id = str(operation_id or "").strip() or None
    operation_worker_id = f"admin-op:reddit-backfill:{normalized_operation_id or 'local'}"

    def _producer() -> Iterator[str]:
        plan = list_reddit_refresh_backfill_targets(
            community_id=community_id,
            season_id=season_id,
            container_keys=requested_container_keys,
            detail_refresh=detail_refresh,
        )
        targets = list(plan.get("targets") or [])
        skipped = list(plan.get("skipped") or [])
        started: list[dict[str, Any]] = []
        failures: list[dict[str, Any]] = []
        total_targets = len(targets)

        yield _reddit_backfill_sse_chunk(
            "progress",
            {
                "stage": "planning",
                "message": (
                    "Planned Reddit detail-enrichment session."
                    if detail_refresh
                    else "Planned Reddit stale-window recovery session."
                ),
                "community_id": community_id,
                "season_id": season_id,
                "mode": "sync_details" if detail_refresh else mode,
                "detail_refresh": detail_refresh,
                "summary": {
                    **(plan.get("summary") or {}),
                    "started_count": 0,
                    "failed_count": 0,
                    "completed_count": 0,
                },
                "targets": [
                    {
                        "container_key": item.get("container_key"),
                        "latest_run_id": item.get("latest_run_id"),
                        "latest_run_status": item.get("latest_run_status"),
                        "stale_reason_code": item.get("stale_reason_code"),
                        "needs_enrichment": item.get("needs_enrichment"),
                    }
                    for item in targets
                ],
                "skipped": skipped,
            },
        )

        if total_targets == 0:
            yield _reddit_backfill_sse_chunk(
                "complete",
                {
                    "status": "completed",
                    "message": (
                        "No Reddit season windows need detail enrichment."
                        if detail_refresh
                        else "No stale Reddit season windows needed recovery."
                    ),
                    "community_id": community_id,
                    "season_id": season_id,
                    "started": [],
                    "skipped": skipped,
                    "summary": {
                        **(plan.get("summary") or {}),
                        "started_count": 0,
                        "failed_count": 0,
                        "completed_count": 0,
                    },
                },
            )
            return

        for index, target in enumerate(targets, start=1):
            if normalized_operation_id and admin_operations.is_cancel_requested(normalized_operation_id):
                yield _reddit_backfill_sse_chunk(
                    "error",
                    {
                        "stage": "operation",
                        "message": (
                            "Reddit detail-enrichment session cancelled by operator."
                            if detail_refresh
                            else "Reddit backfill session cancelled by operator."
                        ),
                        "cancel_requested": True,
                        "started": started,
                        "skipped": skipped,
                        "failures": failures,
                    },
                )
                return

            request_payload_row = (
                target.get("request_payload") if isinstance(target.get("request_payload"), dict) else {}
            )
            if not request_payload_row:
                missing_payload = {
                    "container_key": target.get("container_key"),
                    "reason": "missing_request_payload",
                    "latest_run_id": target.get("latest_run_id"),
                }
                skipped.append(missing_payload)
                yield _reddit_backfill_sse_chunk(
                    "progress",
                    {
                        "stage": "skipping",
                        "message": f"Skipping {target.get('container_key')} because request payload is unavailable.",
                        "current_index": index,
                        "target_count": total_targets,
                        "current_target": missing_payload,
                        "started": started,
                        "skipped": skipped,
                        "failures": failures,
                    },
                )
                continue

            container_key = str(target.get("container_key") or "").strip().lower()
            yield _reddit_backfill_sse_chunk(
                "progress",
                {
                    "stage": "dispatching",
                    "message": (
                        f"Starting Reddit detail enrichment for {container_key}."
                        if detail_refresh
                        else f"Starting Reddit recovery for {container_key}."
                    ),
                    "current_index": index,
                    "target_count": total_targets,
                    "current_target": {
                        "container_key": container_key,
                        "latest_run_id": target.get("latest_run_id"),
                        "latest_run_status": target.get("latest_run_status"),
                        "stale_reason_code": target.get("stale_reason_code"),
                    },
                    "started": started,
                    "skipped": skipped,
                    "failures": failures,
                },
            )

            run_payload = dict(request_payload_row)
            run_payload["mode"] = "sync_details" if detail_refresh else mode
            run_payload.pop("run_config_hash", None)
            if detail_refresh:
                run_payload["force_rescrape"] = False

            try:
                kickoff = _kickoff_reddit_refresh_run_for_backfill(
                    serialized_payload=run_payload,
                    operation_worker_id=operation_worker_id,
                )
            except Exception as exc:  # noqa: BLE001
                failure = {
                    "container_key": container_key,
                    "latest_run_id": target.get("latest_run_id"),
                    "latest_run_status": target.get("latest_run_status"),
                    "error": str(exc),
                }
                failures.append(failure)
                yield _reddit_backfill_sse_chunk(
                    "progress",
                    {
                        "stage": "dispatch_failed",
                        "message": (
                            f"Failed to start Reddit detail enrichment for {container_key}."
                            if detail_refresh
                            else f"Failed to start Reddit recovery for {container_key}."
                        ),
                        "current_index": index,
                        "target_count": total_targets,
                        "current_target": failure,
                        "started": started,
                        "skipped": skipped,
                        "failures": failures,
                    },
                )
                continue

            initial_run = kickoff.get("run") if isinstance(kickoff.get("run"), dict) else {}
            current_run = initial_run
            run_id = str(initial_run.get("run_id") or "").strip() or None
            started_entry = {
                "container_key": container_key,
                "latest_run_id": target.get("latest_run_id"),
                "latest_run_status": target.get("latest_run_status"),
                "stale": bool(target.get("stale")),
                "stale_reason_code": target.get("stale_reason_code"),
                "reused": bool(kickoff.get("reused")),
                "execution_owner": kickoff.get("execution_owner"),
                "execution_mode_canonical": kickoff.get("execution_mode_canonical"),
                "execution_backend_canonical": kickoff.get("execution_backend_canonical"),
                "run": current_run,
            }
            started.append(started_entry)

            yield _reddit_backfill_sse_chunk(
                "progress",
                {
                    "stage": "waiting_for_run",
                    "message": (
                        f"Waiting for Reddit detail-enrichment run for {container_key}."
                        if detail_refresh
                        else f"Waiting for Reddit recovery run for {container_key}."
                    ),
                    "current_index": index,
                    "target_count": total_targets,
                    "current_target": started_entry,
                    "started": started,
                    "skipped": skipped,
                    "failures": failures,
                },
            )

            if run_id:
                while True:
                    if normalized_operation_id and admin_operations.is_cancel_requested(normalized_operation_id):
                        yield _reddit_backfill_sse_chunk(
                            "error",
                            {
                                "stage": "operation",
                                "message": (
                                    "Reddit detail-enrichment session cancelled by operator."
                                    if detail_refresh
                                    else "Reddit backfill session cancelled by operator."
                                ),
                                "cancel_requested": True,
                                "started": started,
                                "skipped": skipped,
                                "failures": failures,
                            },
                        )
                        return
                    current_run = get_refresh_run(run_id)
                    started_entry["run"] = current_run
                    current_status = str(current_run.get("status") or "").strip().lower()
                    yield _reddit_backfill_sse_chunk(
                        "progress",
                        {
                            "stage": "waiting_for_run",
                            "message": (
                                f"Reddit detail-enrichment run for {container_key} is {current_status or 'unknown'}."
                                if detail_refresh
                                else f"Reddit recovery run for {container_key} is {current_status or 'unknown'}."
                            ),
                            "current_index": index,
                            "target_count": total_targets,
                            "current_target": started_entry,
                            "started": started,
                            "skipped": skipped,
                            "failures": failures,
                        },
                    )
                    if current_status in {"completed", "partial", "failed", "cancelled"}:
                        break
                    time.sleep(poll_seconds)

            if str((started_entry.get("run") or {}).get("status") or "").strip().lower() == "failed":
                failures.append(
                    {
                        "container_key": container_key,
                        "latest_run_id": target.get("latest_run_id"),
                        "latest_run_status": target.get("latest_run_status"),
                        "run_id": run_id,
                        "error": (started_entry.get("run") or {}).get("error"),
                    }
                )

        completed_count = sum(
            1
            for item in started
            if str((item.get("run") or {}).get("status") or "").strip().lower() in {"completed", "partial"}
        )
        failed_count = len(failures)
        yield _reddit_backfill_sse_chunk(
            "complete",
            {
                "status": "completed",
                "message": (
                    "Reddit detail-enrichment session finished."
                    if detail_refresh
                    else "Reddit stale-window recovery session finished."
                ),
                "community_id": community_id,
                "season_id": season_id,
                "started": started,
                "skipped": skipped,
                "failures": failures,
                "summary": {
                    **(plan.get("summary") or {}),
                    "started_count": len(started),
                    "skipped_count": len(skipped),
                    "completed_count": completed_count,
                    "failed_count": failed_count,
                },
            },
        )

    return _producer


def _normalize_analytics_scope(scope: str | None) -> str:
    normalized = str(scope or "").strip().lower()
    if normalized in {"season", "all"}:
        return normalized
    raise ValueError("scope must be one of: season, all")


def _build_analytics_filters(
    *,
    community_id: str,
    scope: str,
    season_id: str | None,
    container_key: str | None = None,
    flair_key: str | None = None,
) -> tuple[str, list[Any]]:
    normalized_scope = _normalize_analytics_scope(scope)
    clauses = ["m.community_id = %s"]
    params: list[Any] = [community_id]

    if normalized_scope == "season":
        normalized_season_id = str(season_id or "").strip()
        if not normalized_season_id:
            raise ValueError("season_id is required when scope=season")
        clauses.append("m.season_id = %s")
        params.append(normalized_season_id)

    normalized_container_key = str(container_key or "").strip()
    if normalized_container_key:
        if normalized_scope == "season" and season_id:
            canonical_container_sql = _canonical_reddit_match_container_key_sql(
                season_id=season_id,
                period_key_expr="m.period_key",
                period_start_expr="m.period_start",
                period_end_expr="m.period_end",
                posted_at_expr="p.posted_at",
            )
            clauses.append(f"{canonical_container_sql} = %s")
        else:
            direct_container_sql = _raw_reddit_container_key_sql(period_key_expr="m.period_key")
            clauses.append(f"coalesce({direct_container_sql}, 'unmapped') = %s")
        params.append(normalized_container_key)

    normalized_flair_key = to_canonical_flair_key(flair_key)
    if normalized_flair_key:
        clauses.append("coalesce(nullif(m.canonical_flair_key, ''), nullif(p.canonical_flair_key, ''), '') = %s")
        params.append(normalized_flair_key)

    return " and ".join(clauses), params


def _fetch_reddit_analytics_extras(
    *,
    community_id: str,
    scope: str,
    season_id: str | None = None,
) -> dict[str, Any]:
    normalized_scope = _normalize_analytics_scope(scope)
    has_detail_scraped_at = _column_exists("social", "reddit_posts", "detail_scraped_at")
    detail_scraped_expr = "p.detail_scraped_at is not null" if has_detail_scraped_at else "false"
    where_sql, where_params = _build_analytics_filters(
        community_id=community_id,
        scope=normalized_scope,
        season_id=season_id,
    )
    canonical_container_keys: list[str] = []
    analytics_where_sql = where_sql
    analytics_where_params = list(where_params)
    canonical_container_sql = _canonical_reddit_match_container_key_sql(
        season_id=season_id,
        period_key_expr="m.period_key",
        period_start_expr="m.period_start",
        period_end_expr="m.period_end",
        posted_at_expr="p.posted_at",
    )
    if normalized_scope == "season" and season_id:
        canonical_container_keys = _canonical_reddit_container_keys_for_season(season_id)
        analytics_where_sql = f"{where_sql} and {canonical_container_sql} = any(%s)"
        analytics_where_params.append(canonical_container_keys)
    coverage_row = (
        pg.fetch_one(
            f"""
        with dedup as (
          select distinct on (canonical_container_key, m.reddit_post_id)
                 {canonical_container_sql} as canonical_container_key,
                 m.reddit_post_id,
                 m.is_show_match,
                 m.passes_flair_filter,
                 p.num_comments,
                 {detail_scraped_expr} as detail_scraped,
                 exists(
                   select 1
                   from social.reddit_comments rc
                   where rc.reddit_post_id = m.reddit_post_id
                 ) as has_saved_comments,
                 exists(
                   select 1
                   from social.reddit_media_mirrors rmm
                   where rmm.reddit_post_id = m.reddit_post_id
                 ) as has_media,
                 exists(
                   select 1
                   from social.reddit_media_mirrors rmm
                   where rmm.reddit_post_id = m.reddit_post_id
                     and rmm.status = 'mirrored'
                 ) as has_mirrored_media,
                 greatest(
                   coalesce(p.updated_at, 'epoch'::timestamptz),
                   coalesce(m.updated_at, 'epoch'::timestamptz)
                 ) as row_updated_at
          from social.reddit_period_post_matches m
          join social.reddit_posts p on p.reddit_post_id = m.reddit_post_id
          where {analytics_where_sql}
          order by canonical_container_key, m.reddit_post_id, m.updated_at desc
        )
        select
          count(*)::int as post_count,
          count(*) filter (where passes_flair_filter)::int as tracked_post_count,
          count(*) filter (where is_show_match)::int as show_match_post_count,
          count(*) filter (where passes_flair_filter and detail_scraped)::int as detail_scraped_post_count,
          count(*) filter (where passes_flair_filter and has_saved_comments)::int as comment_saved_post_count,
          count(*) filter (where passes_flair_filter and has_media)::int as media_post_count,
          count(*) filter (where passes_flair_filter and has_mirrored_media)::int as mirrored_media_post_count,
          coalesce(sum(num_comments), 0)::bigint as reported_comment_count,
          max(row_updated_at) as latest_data_timestamp
        from dedup
        """,
            analytics_where_params,
        )
        or {}
    )

    container_statuses: list[dict[str, Any]] = []
    latest_run_timestamp: str | None = None
    latest_run_status: str | None = None
    unmapped_post_count = 0
    unmapped_tracked_post_count = 0

    if normalized_scope == "season" and season_id:
        unmapped_row = (
            pg.fetch_one(
                f"""
                with dedup as (
                  select distinct on (m.reddit_post_id)
                         m.reddit_post_id,
                         m.passes_flair_filter
                  from social.reddit_period_post_matches m
                  join social.reddit_posts p on p.reddit_post_id = m.reddit_post_id
                  where {where_sql}
                    and {canonical_container_sql} = 'unmapped'
                  order by m.reddit_post_id, m.updated_at desc
                )
                select
                  count(*)::int as unmapped_post_count,
                  count(*) filter (where passes_flair_filter)::int as unmapped_tracked_post_count
                from dedup
                """,
                where_params,
            )
            or {}
        )
        unmapped_post_count = _safe_int(unmapped_row.get("unmapped_post_count"))
        unmapped_tracked_post_count = _safe_int(unmapped_row.get("unmapped_tracked_post_count"))
        latest_rows = _fetch_latest_refresh_runs_for_season(
            community_id=community_id,
            season_id=season_id,
            canonical_only=True,
        )
        coverage_by_period_rows = pg.fetch_all(
            f"""
            with dedup as (
              select distinct on (canonical_container_key, m.reddit_post_id)
                     {canonical_container_sql} as canonical_container_key,
                     m.reddit_post_id,
                     m.is_show_match,
                     m.passes_flair_filter,
                     {detail_scraped_expr} as detail_scraped,
                     exists(
                       select 1
                       from social.reddit_comments rc
                       where rc.reddit_post_id = m.reddit_post_id
                     ) as has_saved_comments,
                     exists(
                       select 1
                       from social.reddit_media_mirrors rmm
                       where rmm.reddit_post_id = m.reddit_post_id
                     ) as has_media,
                     exists(
                       select 1
                       from social.reddit_media_mirrors rmm
                       where rmm.reddit_post_id = m.reddit_post_id
                         and rmm.status = 'mirrored'
                     ) as has_mirrored_media
              from social.reddit_period_post_matches m
              join social.reddit_posts p on p.reddit_post_id = m.reddit_post_id
              where {analytics_where_sql}
              order by canonical_container_key, m.reddit_post_id, m.updated_at desc
            )
            select
              canonical_container_key,
              count(*)::int as matched_post_count,
              count(*) filter (where passes_flair_filter)::int as tracked_post_count,
              count(*) filter (where is_show_match)::int as show_match_post_count,
              count(*) filter (where passes_flair_filter and detail_scraped)::int as detail_scraped_post_count,
              count(*) filter (where passes_flair_filter and has_saved_comments)::int as comment_saved_post_count,
              count(*) filter (where passes_flair_filter and has_media)::int as media_post_count,
              count(*) filter (where passes_flair_filter and has_mirrored_media)::int as mirrored_media_post_count
            from dedup
            group by canonical_container_key
            """,
            analytics_where_params,
        )
        coverage_by_period = {
            str(row.get("canonical_container_key") or "").strip(): row
            for row in coverage_by_period_rows
            if str(row.get("canonical_container_key") or "").strip()
        }
        rows_by_container_key = {
            str(row.get("canonical_container_key") or row.get("period_key") or "").strip().lower(): row
            for row in latest_rows
            if str(row.get("canonical_container_key") or row.get("period_key") or "").strip()
        }
        for period_key in canonical_container_keys:
            row = rows_by_container_key.get(period_key)
            coverage_row_by_period = coverage_by_period.get(period_key) or {}
            tracked_post_count = _safe_int(coverage_row_by_period.get("tracked_post_count"))
            detail_scraped_post_count = _safe_int(coverage_row_by_period.get("detail_scraped_post_count"))
            comment_saved_post_count = _safe_int(coverage_row_by_period.get("comment_saved_post_count"))
            media_post_count = _safe_int(coverage_row_by_period.get("media_post_count"))
            mirrored_media_post_count = _safe_int(coverage_row_by_period.get("mirrored_media_post_count"))
            if row is None:
                container_statuses.append(
                    {
                        "container_key": period_key,
                        "latest_run_id": None,
                        "latest_run_status": None,
                        "latest_run_timestamp": None,
                        "failure_reason_code": None,
                        "operator_hint": "No Reddit refresh run recorded for this season window yet.",
                        "stale": True,
                        "stale_reason_code": "no_previous_run",
                        "tracked_post_count": tracked_post_count,
                        "matched_post_count": _safe_int(coverage_row_by_period.get("matched_post_count")),
                        "show_match_post_count": _safe_int(coverage_row_by_period.get("show_match_post_count")),
                        "detail_scraped_post_count": detail_scraped_post_count,
                        "comment_saved_post_count": comment_saved_post_count,
                        "media_post_count": media_post_count,
                        "mirrored_media_post_count": mirrored_media_post_count,
                        "detail_coverage_pct": round((detail_scraped_post_count / tracked_post_count) * 100, 1)
                        if tracked_post_count > 0
                        else None,
                        "comment_coverage_pct": round((comment_saved_post_count / tracked_post_count) * 100, 1)
                        if tracked_post_count > 0
                        else None,
                        "media_coverage_pct": round((media_post_count / tracked_post_count) * 100, 1)
                        if tracked_post_count > 0
                        else None,
                        "mirrored_media_coverage_pct": round((mirrored_media_post_count / tracked_post_count) * 100, 1)
                        if tracked_post_count > 0
                        else None,
                    }
                )
                continue
            diagnostics = row.get("diagnostics") if isinstance(row.get("diagnostics"), dict) else {}
            latest_status = str(row.get("status") or "").strip().lower()
            failure_reason_code = _derive_failure_reason_code(
                status=latest_status,
                diagnostics=diagnostics,
                error_message=str(row.get("error_message") or "").strip() or None,
                stalled=False,
            )
            operator_hint = _derive_operator_hint(
                status=latest_status,
                diagnostics=diagnostics,
                error_message=str(row.get("error_message") or "").strip() or None,
                stalled=False,
            )
            stale, stale_reason_code = _classify_reddit_backfill_candidate(row)
            latest_run_at = _iso_utc(_parse_iso(row.get("completed_at")) or _parse_iso(row.get("updated_at")))
            if latest_run_at and (
                latest_run_timestamp is None
                or (_parse_iso(latest_run_at) or datetime.min.replace(tzinfo=UTC))
                > (_parse_iso(latest_run_timestamp) or datetime.min.replace(tzinfo=UTC))
            ):
                latest_run_timestamp = latest_run_at
                latest_run_status = latest_status or None
            container_statuses.append(
                {
                    "container_key": period_key,
                    "latest_run_id": str(row.get("id") or "").strip() or None,
                    "latest_run_status": latest_status or None,
                    "latest_run_timestamp": latest_run_at,
                    "failure_reason_code": failure_reason_code,
                    "operator_hint": operator_hint,
                    "stale": stale,
                    "stale_reason_code": stale_reason_code,
                    "tracked_post_count": tracked_post_count,
                    "matched_post_count": _safe_int(coverage_row_by_period.get("matched_post_count")),
                    "show_match_post_count": _safe_int(coverage_row_by_period.get("show_match_post_count")),
                    "detail_scraped_post_count": detail_scraped_post_count,
                    "comment_saved_post_count": comment_saved_post_count,
                    "media_post_count": media_post_count,
                    "mirrored_media_post_count": mirrored_media_post_count,
                    "detail_coverage_pct": round((detail_scraped_post_count / tracked_post_count) * 100, 1)
                    if tracked_post_count > 0
                    else None,
                    "comment_coverage_pct": round((comment_saved_post_count / tracked_post_count) * 100, 1)
                    if tracked_post_count > 0
                    else None,
                    "media_coverage_pct": round((media_post_count / tracked_post_count) * 100, 1)
                    if tracked_post_count > 0
                    else None,
                    "mirrored_media_coverage_pct": round((mirrored_media_post_count / tracked_post_count) * 100, 1)
                    if tracked_post_count > 0
                    else None,
                }
            )

    tracked_post_count = _safe_int(coverage_row.get("tracked_post_count"))
    detail_scraped_post_count = _safe_int(coverage_row.get("detail_scraped_post_count"))
    comment_saved_post_count = _safe_int(coverage_row.get("comment_saved_post_count"))
    media_post_count = _safe_int(coverage_row.get("media_post_count"))
    mirrored_media_post_count = _safe_int(coverage_row.get("mirrored_media_post_count"))

    return {
        "freshness": {
            "latest_data_timestamp": _iso_utc(_parse_iso(coverage_row.get("latest_data_timestamp"))),
            "latest_run_timestamp": latest_run_timestamp,
            "latest_canonical_run_timestamp": latest_run_timestamp,
            "latest_run_status": latest_run_status,
        },
        "coverage": {
            "tracked_post_count": tracked_post_count,
            "detail_scraped_post_count": detail_scraped_post_count,
            "comment_saved_post_count": comment_saved_post_count,
            "media_post_count": media_post_count,
            "mirrored_media_post_count": mirrored_media_post_count,
            "reported_comment_count": _safe_int(coverage_row.get("reported_comment_count")),
            "detail_coverage_pct": round((detail_scraped_post_count / tracked_post_count) * 100, 1)
            if tracked_post_count > 0
            else None,
            "comment_coverage_pct": round((comment_saved_post_count / tracked_post_count) * 100, 1)
            if tracked_post_count > 0
            else None,
            "media_coverage_pct": round((media_post_count / tracked_post_count) * 100, 1)
            if tracked_post_count > 0
            else None,
            "mirrored_media_coverage_pct": round((mirrored_media_post_count / tracked_post_count) * 100, 1)
            if tracked_post_count > 0
            else None,
            "scope": "canonical_windows" if normalized_scope == "season" and season_id else "all_posts",
            "container_count": len(container_statuses),
            "stale_container_count": sum(1 for item in container_statuses if bool(item.get("stale"))),
            "recovered_container_count": sum(1 for item in container_statuses if not bool(item.get("stale"))),
            "unmapped_post_count": unmapped_post_count,
            "unmapped_tracked_post_count": unmapped_tracked_post_count,
        },
        "container_statuses": container_statuses,
    }


def get_reddit_community_analytics_summary(
    *,
    community_id: str,
    scope: str,
    season_id: str | None = None,
) -> dict[str, Any]:
    where_sql, where_params = _build_analytics_filters(
        community_id=community_id,
        scope=scope,
        season_id=season_id,
    )
    row = (
        pg.fetch_one(
            f"""
        with dedup as (
          select distinct on (m.reddit_post_id)
                 m.reddit_post_id,
                 m.season_id,
                 m.is_show_match,
                 m.passes_flair_filter,
                 p.num_comments,
                 p.score,
                 p.posted_at,
                 greatest(
                   coalesce(p.updated_at, 'epoch'::timestamptz),
                   coalesce(m.updated_at, 'epoch'::timestamptz)
                 ) as row_updated_at
          from social.reddit_period_post_matches m
          join social.reddit_posts p on p.reddit_post_id = m.reddit_post_id
          where {where_sql}
          order by m.reddit_post_id, m.updated_at desc
        )
        select
          count(*)::int as post_count,
          count(*) filter (where passes_flair_filter)::int as tracked_flair_post_count,
          count(*) filter (where is_show_match)::int as show_match_post_count,
          coalesce(sum(num_comments), 0)::bigint as comment_count,
          coalesce(sum(score), 0)::bigint as score_sum,
          count(distinct season_id)::int as season_count,
          max(row_updated_at) as updated_at
        from dedup
        """,
            where_params,
        )
        or {}
    )
    extras = _fetch_reddit_analytics_extras(
        community_id=community_id,
        scope=scope,
        season_id=season_id,
    )
    return {
        "scope": _normalize_analytics_scope(scope),
        "season_id": season_id,
        "totals": {
            "post_count": _safe_int(row.get("post_count")),
            "tracked_flair_post_count": _safe_int(row.get("tracked_flair_post_count")),
            "show_match_post_count": _safe_int(row.get("show_match_post_count")),
            "comment_count": _safe_int(row.get("comment_count")),
            "score_sum": _safe_int(row.get("score_sum")),
            "season_count": _safe_int(row.get("season_count")),
        },
        "diagnostics": {
            "updated_at": _iso_utc(_parse_iso(row.get("updated_at"))),
            "source_table": "social.reddit_period_post_matches",
            "row_count": _safe_int(row.get("post_count")),
        },
        "freshness": extras.get("freshness"),
        "coverage": extras.get("coverage"),
        "container_statuses": extras.get("container_statuses"),
    }


def get_reddit_community_show_breakdown(
    *,
    community_id: str,
    scope: str,
    season_id: str | None = None,
) -> dict[str, Any]:
    where_sql, where_params = _build_analytics_filters(
        community_id=community_id,
        scope=scope,
        season_id=season_id,
    )
    rows = pg.fetch_all(
        f"""
        with dedup as (
          select distinct on (s.show_id, m.reddit_post_id)
                 s.show_id::text as show_id,
                 sh.name as show_name,
                 m.reddit_post_id,
                 m.is_show_match,
                 m.passes_flair_filter,
                 p.num_comments,
                 p.score,
                 greatest(
                   coalesce(p.updated_at, 'epoch'::timestamptz),
                   coalesce(m.updated_at, 'epoch'::timestamptz)
                 ) as row_updated_at
          from social.reddit_period_post_matches m
          join social.reddit_posts p on p.reddit_post_id = m.reddit_post_id
          join core.seasons s on s.id = m.season_id
          join core.shows sh on sh.id = s.show_id
          where {where_sql}
          order by s.show_id, m.reddit_post_id, m.updated_at desc
        )
        select
          show_id,
          show_name,
          count(*)::int as post_count,
          count(*) filter (where passes_flair_filter)::int as tracked_flair_post_count,
          count(*) filter (where is_show_match)::int as show_match_post_count,
          coalesce(sum(num_comments), 0)::bigint as comment_count,
          coalesce(sum(score), 0)::bigint as score_sum,
          max(row_updated_at) as updated_at
        from dedup
        group by show_id, show_name
        order by tracked_flair_post_count desc, post_count desc, show_name asc
        """,
        where_params,
    )
    extras = _fetch_reddit_analytics_extras(
        community_id=community_id,
        scope=scope,
        season_id=season_id,
    )
    return {
        "scope": _normalize_analytics_scope(scope),
        "season_id": season_id,
        "shows": [
            {
                "show_id": row.get("show_id"),
                "show_name": row.get("show_name"),
                "post_count": _safe_int(row.get("post_count")),
                "tracked_flair_post_count": _safe_int(row.get("tracked_flair_post_count")),
                "show_match_post_count": _safe_int(row.get("show_match_post_count")),
                "comment_count": _safe_int(row.get("comment_count")),
                "score_sum": _safe_int(row.get("score_sum")),
                "updated_at": _iso_utc(_parse_iso(row.get("updated_at"))),
            }
            for row in rows
        ],
        "freshness": extras.get("freshness"),
        "coverage": extras.get("coverage"),
        "container_statuses": extras.get("container_statuses"),
    }


def get_reddit_community_flair_breakdown(
    *,
    community_id: str,
    scope: str,
    season_id: str | None = None,
) -> dict[str, Any]:
    where_sql, where_params = _build_analytics_filters(
        community_id=community_id,
        scope=scope,
        season_id=season_id,
    )
    rows = pg.fetch_all(
        f"""
        with dedup as (
          select distinct on (m.reddit_post_id)
                 m.reddit_post_id,
                 coalesce(nullif(m.canonical_flair_key, ''), nullif(p.canonical_flair_key, ''), '') as flair_key,
                 coalesce(nullif(m.link_flair_text, ''), nullif(p.link_flair_text, ''), '(No Flair)') as flair_label,
                 m.is_show_match,
                 m.passes_flair_filter,
                 p.num_comments,
                 p.score,
                 greatest(
                   coalesce(p.updated_at, 'epoch'::timestamptz),
                   coalesce(m.updated_at, 'epoch'::timestamptz)
                 ) as row_updated_at
          from social.reddit_period_post_matches m
          join social.reddit_posts p on p.reddit_post_id = m.reddit_post_id
          where {where_sql}
          order by m.reddit_post_id, m.updated_at desc
        )
        select
          flair_key,
          min(flair_label) as flair_label,
          count(*)::int as post_count,
          count(*) filter (where passes_flair_filter)::int as tracked_flair_post_count,
          count(*) filter (where is_show_match)::int as show_match_post_count,
          coalesce(sum(num_comments), 0)::bigint as comment_count,
          coalesce(sum(score), 0)::bigint as score_sum,
          max(row_updated_at) as updated_at
        from dedup
        group by flair_key
        order by tracked_flair_post_count desc, post_count desc, flair_label asc
        """,
        where_params,
    )
    extras = _fetch_reddit_analytics_extras(
        community_id=community_id,
        scope=scope,
        season_id=season_id,
    )
    return {
        "scope": _normalize_analytics_scope(scope),
        "season_id": season_id,
        "flairs": [
            {
                "flair_key": row.get("flair_key") or "",
                "flair_label": row.get("flair_label") or "(No Flair)",
                "post_count": _safe_int(row.get("post_count")),
                "tracked_flair_post_count": _safe_int(row.get("tracked_flair_post_count")),
                "show_match_post_count": _safe_int(row.get("show_match_post_count")),
                "comment_count": _safe_int(row.get("comment_count")),
                "score_sum": _safe_int(row.get("score_sum")),
                "updated_at": _iso_utc(_parse_iso(row.get("updated_at"))),
            }
            for row in rows
        ],
        "freshness": extras.get("freshness"),
        "coverage": extras.get("coverage"),
        "container_statuses": extras.get("container_statuses"),
    }


def list_reddit_community_posts(
    *,
    community_id: str,
    scope: str,
    season_id: str | None = None,
    container_key: str | None = None,
    flair_key: str | None = None,
    page: int = 1,
    per_page: int = 50,
) -> dict[str, Any]:
    normalized_page = max(1, int(page))
    normalized_per_page = max(1, min(int(per_page), 200))
    offset = (normalized_page - 1) * normalized_per_page
    where_sql, where_params = _build_analytics_filters(
        community_id=community_id,
        scope=scope,
        season_id=season_id,
        container_key=container_key,
        flair_key=flair_key,
    )
    has_flair_mode = _column_exists("social", "reddit_period_post_matches", "flair_mode")
    flair_mode_select = "m.flair_mode" if has_flair_mode else "null::text as flair_mode"
    count_row = (
        pg.fetch_one(
            f"""
        select count(*)::int as total_count
        from (
          select distinct on (m.reddit_post_id) m.reddit_post_id
          from social.reddit_period_post_matches m
          join social.reddit_posts p on p.reddit_post_id = m.reddit_post_id
          where {where_sql}
          order by m.reddit_post_id, m.updated_at desc
        ) dedup
        """,
            where_params,
        )
        or {}
    )
    rows = pg.fetch_all(
        f"""
        with dedup as (
          select distinct on (m.reddit_post_id)
                 m.reddit_post_id,
                 p.title,
                 p.selftext,
                 p.url,
                 p.permalink,
                 p.author,
                 p.score,
                 p.num_comments,
                 p.posted_at,
                 coalesce(nullif(m.link_flair_text, ''), p.link_flair_text) as link_flair_text,
                 m.source_sorts,
                 m.matched_terms,
                 m.matched_cast_terms,
                 m.cross_show_terms,
                 m.is_show_match,
                 m.passes_flair_filter,
                 m.match_score,
                 {flair_mode_select}
          from social.reddit_period_post_matches m
          join social.reddit_posts p on p.reddit_post_id = m.reddit_post_id
          where {where_sql}
          order by m.reddit_post_id, m.updated_at desc
        )
        select *
        from dedup
        order by posted_at desc nulls last, num_comments desc, score desc
        limit %s
        offset %s
        """,
        [*where_params, normalized_per_page, offset],
    )
    extras = _fetch_reddit_analytics_extras(
        community_id=community_id,
        scope=scope,
        season_id=season_id,
    )
    return {
        "scope": _normalize_analytics_scope(scope),
        "season_id": season_id,
        "container_key": str(container_key or "").strip() or None,
        "flair_key": to_canonical_flair_key(flair_key) or None,
        "pagination": {
            "page": normalized_page,
            "per_page": normalized_per_page,
            "total_count": _safe_int(count_row.get("total_count")),
        },
        "posts": [_base_thread_projection(row) for row in rows],
        "freshness": extras.get("freshness"),
        "coverage": extras.get("coverage"),
        "container_statuses": extras.get("container_statuses"),
    }


def get_reddit_community_flair_detail(
    *,
    community_id: str,
    flair_key: str,
    scope: str,
    season_id: str | None = None,
    container_key: str | None = None,
    page: int = 1,
    per_page: int = 50,
) -> dict[str, Any]:
    normalized_flair_key = to_canonical_flair_key(flair_key)
    if not normalized_flair_key:
        raise ValueError("flair_key is required")
    posts_payload = list_reddit_community_posts(
        community_id=community_id,
        scope=scope,
        season_id=season_id,
        container_key=container_key,
        flair_key=normalized_flair_key,
        page=page,
        per_page=per_page,
    )
    total = posts_payload.get("pagination", {}).get("total_count", 0)
    label_row = (
        pg.fetch_one(
            """
        select
          coalesce(
            nullif(max(link_flair_text), ''),
            '(No Flair)'
          ) as flair_label
        from social.reddit_posts
        where coalesce(nullif(canonical_flair_key, ''), '') = %s
        """,
            [normalized_flair_key],
        )
        or {}
    )
    extras = _fetch_reddit_analytics_extras(
        community_id=community_id,
        scope=scope,
        season_id=season_id,
    )
    return {
        "scope": _normalize_analytics_scope(scope),
        "season_id": season_id,
        "flair": {
            "flair_key": normalized_flair_key,
            "flair_label": label_row.get("flair_label") or "(No Flair)",
            "post_count": _safe_int(total),
        },
        "posts": posts_payload.get("posts", []),
        "pagination": posts_payload.get("pagination", {}),
        "freshness": extras.get("freshness"),
        "coverage": extras.get("coverage"),
        "container_statuses": extras.get("container_statuses"),
    }
