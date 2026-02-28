"""Season-scoped social analytics + ingest helpers."""

from __future__ import annotations

import copy
import csv
import glob
import hashlib
import io
import json
import logging
import mimetypes
import os
import re
import subprocess
import tempfile
import time as time_module
from collections import Counter, defaultdict
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, time, timedelta
from functools import lru_cache
from pathlib import Path
from threading import Lock
from types import SimpleNamespace
from typing import Any
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import requests

from trr_backend.db import pg
from trr_backend.socials.crawlee_runtime import (
    AuthPreflightError,
    AuthPreflightResult,
    build_auth_context,
    build_runtime_config,
    check_platform_auth,
    classify_exception,
    is_auth_strict_for_platform,
    should_use_crawlee,
)
from trr_backend.socials.platforms import SOCIAL_SUPPORTED_PLATFORMS

logger = logging.getLogger(__name__)

SUPPORTED_PLATFORMS = SOCIAL_SUPPORTED_PLATFORMS
SUPPORTED_SCOPES = ("bravo", "creator", "community")
SUPPORTED_INGEST_MODES = ("posts_only", "posts_and_comments", "comments_only")
SUPPORTED_SYNC_STRATEGIES = ("incremental", "full_refresh")
SUPPORTED_COMMENT_REFRESH_POLICIES = ("balanced", "missing_only")
DEFAULT_COMMENT_REFRESH_POLICY = "balanced"
MAX_COMMENT_ANCHOR_SOURCE_IDS_PER_PLATFORM = 5000
POSTSEASON_WEEK_COUNT = 3
JOB_PROGRESS_MIN_DELTA = 5
JOB_PROGRESS_MAX_INTERVAL_SECONDS = 3
COMMENT_STALE_RECHECK_INTERVAL = timedelta(hours=24)
QUIET_POST_FORCE_RECHECK_AGE = timedelta(days=14)
SOCIAL_WORKER_HEARTBEAT_STALE_SECONDS_DEFAULT = 180
SOCIAL_WORKER_HEARTBEAT_INTERVAL_SECONDS_DEFAULT = 15
SOCIAL_JOB_STALE_SECONDS_DEFAULT = 300
SOCIAL_QUEUE_STATUS_CACHE_TTL_SECONDS_DEFAULT = 5
SOCIAL_WORKER_HEALTH_CACHE_TTL_SECONDS_DEFAULT = 5
QUEUE_STATUS_STAGES = ("queued", "pending", "running", "retrying", "failed", "cancelled", "completed")
SOCIAL_MEDIA_MIRROR_MAX_BYTES_DEFAULT = 50 * 1024 * 1024
SOCIAL_MEDIA_MIRROR_CHUNK_SIZE_BYTES = 64 * 1024
SOCIAL_MEDIA_MIRROR_DOWNLOAD_RETRIES_DEFAULT = 3
SOCIAL_MEDIA_MIRROR_DOWNLOAD_RETRY_BACKOFF_SECONDS_DEFAULT = 1
INSTAGRAM_MEDIA_MIRROR_STAGE = "media_mirror"
COMMENT_MEDIA_MIRROR_STAGE = "comment_media_mirror"
INSTAGRAM_MEDIA_MIRROR_JOB_TYPE = "instagram_media_mirror"
TIKTOK_MEDIA_MIRROR_JOB_TYPE = "tiktok_media_mirror"
YOUTUBE_MEDIA_MIRROR_JOB_TYPE = "youtube_media_mirror"
TWITTER_MEDIA_MIRROR_JOB_TYPE = "twitter_media_mirror"
FACEBOOK_MEDIA_MIRROR_JOB_TYPE = "facebook_media_mirror"
THREADS_MEDIA_MIRROR_JOB_TYPE = "threads_media_mirror"
INSTAGRAM_COMMENT_MEDIA_MIRROR_JOB_TYPE = "instagram_comment_media_mirror"
TIKTOK_COMMENT_MEDIA_MIRROR_JOB_TYPE = "tiktok_comment_media_mirror"
YOUTUBE_COMMENT_MEDIA_MIRROR_JOB_TYPE = "youtube_comment_media_mirror"
TWITTER_COMMENT_MEDIA_MIRROR_JOB_TYPE = "twitter_comment_media_mirror"
FACEBOOK_COMMENT_MEDIA_MIRROR_JOB_TYPE = "facebook_comment_media_mirror"
THREADS_COMMENT_MEDIA_MIRROR_JOB_TYPE = "threads_comment_media_mirror"
PLATFORM_MEDIA_MIRROR_JOB_TYPES = {
    "instagram": INSTAGRAM_MEDIA_MIRROR_JOB_TYPE,
    "tiktok": TIKTOK_MEDIA_MIRROR_JOB_TYPE,
    "youtube": YOUTUBE_MEDIA_MIRROR_JOB_TYPE,
    "twitter": TWITTER_MEDIA_MIRROR_JOB_TYPE,
    "facebook": FACEBOOK_MEDIA_MIRROR_JOB_TYPE,
    "threads": THREADS_MEDIA_MIRROR_JOB_TYPE,
}
PLATFORM_COMMENT_MEDIA_MIRROR_JOB_TYPES = {
    "instagram": INSTAGRAM_COMMENT_MEDIA_MIRROR_JOB_TYPE,
    "tiktok": TIKTOK_COMMENT_MEDIA_MIRROR_JOB_TYPE,
    "youtube": YOUTUBE_COMMENT_MEDIA_MIRROR_JOB_TYPE,
    "twitter": TWITTER_COMMENT_MEDIA_MIRROR_JOB_TYPE,
    "facebook": FACEBOOK_COMMENT_MEDIA_MIRROR_JOB_TYPE,
    "threads": THREADS_COMMENT_MEDIA_MIRROR_JOB_TYPE,
}
PLATFORM_POST_TABLES = {
    "instagram": "instagram_posts",
    "tiktok": "tiktok_posts",
    "youtube": "youtube_videos",
    "twitter": "twitter_tweets",
    "facebook": "facebook_posts",
    "threads": "meta_threads_posts",
}
PLATFORM_COMMENT_TABLES = {
    "instagram": "instagram_comments",
    "tiktok": "tiktok_comments",
    "youtube": "youtube_comments",
    "facebook": "facebook_comments",
    "threads": "meta_threads_comments",
}
PLATFORM_SOURCE_ID_COLUMN = {
    "instagram": "shortcode",
    "tiktok": "video_id",
    "youtube": "video_id",
    "twitter": "tweet_id",
    "facebook": "post_id",
    "threads": "post_id",
}
PLATFORM_POSTED_AT_COLUMN = {
    "instagram": "posted_at",
    "tiktok": "posted_at",
    "youtube": "published_at",
    "twitter": "created_at",
    "facebook": "posted_at",
    "threads": "posted_at",
}
INSTAGRAM_SHORTCODE_RE = re.compile(r"^[A-Za-z0-9_-]{5,32}$")
YOUTUBE_HANDLE_URL_RE = re.compile(r"/@([A-Za-z0-9._-]+)")
FIELD_UNSET = object()
ANALYTICS_CACHE_TTL_SECONDS = 15.0

POSITIVE_WORDS = {
    "amazing",
    "awesome",
    "best",
    "binge",
    "brilliant",
    "classic",
    "excellent",
    "excited",
    "favorite",
    "fun",
    "funny",
    "good",
    "great",
    "happy",
    "hilarious",
    "iconic",
    "impressed",
    "incredible",
    "interesting",
    "love",
    "loved",
    "loving",
    "obsessed",
    "perfect",
    "solid",
    "strong",
    "stunning",
    "wow",
}

NEGATIVE_WORDS = {
    "annoying",
    "awful",
    "bad",
    "boring",
    "chaotic",
    "confusing",
    "cringe",
    "disappointing",
    "dull",
    "fake",
    "hate",
    "hated",
    "horrible",
    "lazy",
    "mess",
    "mid",
    "negative",
    "problem",
    "rough",
    "sad",
    "slow",
    "terrible",
    "toxic",
    "trash",
    "weak",
    "worse",
    "worst",
}

STOPWORDS = {
    "about",
    "after",
    "again",
    "also",
    "because",
    "before",
    "being",
    "bravo",
    "from",
    "have",
    "just",
    "like",
    "more",
    "only",
    "over",
    "really",
    "season",
    "show",
    "that",
    "their",
    "there",
    "these",
    "they",
    "this",
    "those",
    "very",
    "what",
    "with",
    "would",
    "your",
}

TOKEN_RE = re.compile(r"[a-zA-Z][a-zA-Z0-9']+")
MENTION_TOKEN_RE = re.compile(r"@([A-Za-z0-9_.]+)")
TRAILER_MARKER_RE = re.compile(
    r"\b(first look|sneak peek|trailer|season announcement|official trailer)\b",
    re.IGNORECASE,
)
YOUTUBE_GENERIC_SEASON_TERM_RE = re.compile(r"^(season\s+\d+|s\d+)$", re.IGNORECASE)

NEGATION_WORDS = {
    "aint",
    "aren't",
    "cant",
    "can't",
    "didnt",
    "didn't",
    "doesnt",
    "doesn't",
    "dont",
    "don't",
    "hardly",
    "isnt",
    "isn't",
    "never",
    "no",
    "not",
    "nothing",
    "wasnt",
    "wasn't",
    "without",
    "wont",
    "won't",
}

INTENSIFIER_WEIGHTS = {
    "absolutely": 1.5,
    "crazy": 1.2,
    "extremely": 1.6,
    "incredibly": 1.5,
    "really": 1.25,
    "so": 1.2,
    "super": 1.35,
    "totally": 1.35,
    "very": 1.2,
}

DIMINISHER_WEIGHTS = {
    "barely": 0.7,
    "kind": 0.8,
    "kinda": 0.8,
    "slightly": 0.7,
    "somewhat": 0.8,
    "sort": 0.8,
}

CONTRAST_MARKERS = {"but", "however", "though", "although", "yet"}
DEFAULT_GEMINI_SENTIMENT_MODEL = "gemini-2.5-flash"
SENTIMENT_GEMINI_MAX_COMMENTS_CAP = 120
SENTIMENT_GEMINI_BATCH_SIZE_CAP = 25


@dataclass(slots=True)
class SeasonContext:
    season_id: str
    show_id: str
    show_name: str | None
    season_number: int
    anchor_date: date
    show_slug: str | None = None


@dataclass(slots=True)
class WeekWindow:
    week_index: int
    start_local: datetime
    end_local: datetime
    week_type: str = "episode"
    episode_number: int | None = None


@dataclass(slots=True)
class IngestResult:
    platform: str
    source_scope: str
    account: str
    job_id: str
    status: str
    posts: int
    comments: int
    error: str | None = None


@dataclass(slots=True)
class IngestOptions:
    platforms: set[str] | None
    source_scope: str
    sync_strategy: str
    max_posts_per_target: int
    max_comments_per_post: int
    max_replies_per_post: int
    fetch_replies: bool
    ingest_mode: str
    date_start: datetime | None
    date_end: datetime | None
    comment_refresh_policy: str = DEFAULT_COMMENT_REFRESH_POLICY
    comment_anchor_source_ids: dict[str, set[str]] | None = None


@dataclass(slots=True)
class CommentLifecycleSnapshot:
    active_count: int
    total_count: int
    latest_comment_created_at: datetime | None
    last_seen_at: datetime | None
    last_checked_at: datetime | None


@dataclass(slots=True)
class CommentRefreshDecision:
    should_refresh: bool
    reason: str


@dataclass(slots=True)
class EpisodeSentimentContext:
    summary: str
    terms: set[str]


@dataclass(slots=True)
class SentimentAnalyzerContext:
    cast_terms: set[str]
    cast_phrases: set[str]
    episode_terms: set[str]
    episode_summary: str


@dataclass(slots=True)
class SentimentRuleResult:
    label: str
    score: float
    confidence: float
    ambiguous: bool


class SocialWorkerUnavailableError(ValueError):
    """Raised when queue mode is enabled but no healthy social worker is active."""

    def __init__(self, message: str, *, worker_health: dict[str, Any]):
        super().__init__(message)
        self.worker_health = worker_health


# ---------------------------------------------------------------------------
# General helpers
# ---------------------------------------------------------------------------


def _now_utc() -> datetime:
    return datetime.now(tz=UTC)


def _env_truthy(name: str, *, default: bool = False) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def is_queue_enabled() -> bool:
    return _env_truthy("SOCIAL_QUEUE_ENABLED", default=False)


def _resolve_positive_int_env(name: str, default: int, *, minimum: int = 1) -> int:
    raw = (os.getenv(name) or "").strip()
    if not raw:
        return max(minimum, default)
    try:
        return max(minimum, int(raw))
    except ValueError:
        return max(minimum, default)


def _normalize_worker_stage(stage: str | None) -> str:
    value = (stage or "").strip().lower()
    if not value:
        return "any"
    if value in {"posts", "comments", "any"}:
        return value
    return "any"


def _normalize_worker_status(status: str | None) -> str:
    value = (status or "").strip().lower()
    if value in {"starting", "idle", "working", "stopped"}:
        return value
    return "idle"


_schema_ready_cache: tuple[float, bool] | None = None
_worker_health_cache: tuple[float, int | None, dict[str, Any]] | None = None
_worker_health_cache_lock = Lock()
_queue_status_cache: tuple[float, int, int, dict[str, Any]] | None = None
_queue_status_cache_lock = Lock()


def _worker_heartbeat_schema_ready() -> bool:
    global _schema_ready_cache
    now = time_module.monotonic()
    if _schema_ready_cache and (now - _schema_ready_cache[0]) < 60:
        return _schema_ready_cache[1]
    result = _relation_exists("social.scrape_workers")
    _schema_ready_cache = (now, result)
    return result


def update_worker_heartbeat(
    worker_id: str,
    *,
    stage: str | None,
    status: str,
    run_id: str | None = None,
    current_job_id: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    global _worker_health_cache
    if not _worker_heartbeat_schema_ready():
        return None

    row = pg.fetch_one(
        """
        insert into social.scrape_workers (
          worker_id,
          stage,
          status,
          run_id,
          current_job_id,
          metadata,
          started_at,
          last_seen_at,
          updated_at
        )
        values (
          %s,
          %s,
          %s,
          %s,
          %s,
          %s::jsonb,
          now(),
          now(),
          now()
        )
        on conflict (worker_id)
        do update set
          stage = excluded.stage,
          status = excluded.status,
          run_id = excluded.run_id,
          current_job_id = excluded.current_job_id,
          metadata = excluded.metadata,
          last_seen_at = now(),
          updated_at = now()
        returning
          worker_id,
          stage,
          status,
          run_id::text as run_id,
          current_job_id::text as current_job_id,
          metadata,
          started_at,
          last_seen_at,
          updated_at
        """,
        [
            worker_id,
            _normalize_worker_stage(stage),
            _normalize_worker_status(status),
            run_id,
            current_job_id,
            json.dumps(metadata or {}),
        ],
    )
    with _worker_health_cache_lock:
        _worker_health_cache = None
    return row


def mark_worker_stopped(
    worker_id: str,
    *,
    stage: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    return update_worker_heartbeat(
        worker_id,
        stage=stage,
        status="stopped",
        run_id=None,
        current_job_id=None,
        metadata=metadata,
    )


def _query_worker_health(*, stale_after_seconds: int | None = None) -> dict[str, Any]:
    stale_seconds = stale_after_seconds or _resolve_positive_int_env(
        "SOCIAL_WORKER_HEARTBEAT_STALE_SECONDS",
        SOCIAL_WORKER_HEARTBEAT_STALE_SECONDS_DEFAULT,
        minimum=5,
    )
    payload: dict[str, Any] = {
        "healthy": False,
        "healthy_workers": 0,
        "active_workers": 0,
        "total_workers": 0,
        "stale_after_seconds": stale_seconds,
        "workers": [],
        "reason": None,
    }

    if not _worker_heartbeat_schema_ready():
        payload["reason"] = "worker_heartbeat_schema_missing"
        return payload

    try:
        with pg.db_connection() as conn:
            with pg.db_cursor(conn=conn) as cur:
                cur.execute("set local statement_timeout = '5000'")
                workers = pg.fetch_all_with_cursor(
                    cur,
                    """
                    select
                      worker_id,
                      stage,
                      status,
                      run_id::text as run_id,
                      current_job_id::text as current_job_id,
                      metadata,
                      started_at,
                      last_seen_at,
                      updated_at,
                      (
                        status in ('starting', 'idle', 'working')
                        and last_seen_at >= now() - (%s * interval '1 second')
                      ) as is_healthy
                    from social.scrape_workers
                    order by last_seen_at desc
                    limit 100
                    """,
                    [stale_seconds],
                )
    except Exception as exc:  # noqa: BLE001
        logger.warning("Worker health query timed out or failed: %s", exc)
        payload["reason"] = "health_query_failed"
        return payload
    healthy_workers = sum(1 for worker in workers if bool(worker.get("is_healthy")))
    active_workers = sum(1 for worker in workers if str(worker.get("status") or "") != "stopped")
    payload.update(
        {
            "healthy": healthy_workers > 0,
            "healthy_workers": healthy_workers,
            "active_workers": active_workers,
            "total_workers": len(workers),
            "workers": workers,
            "reason": None if healthy_workers > 0 else "no_healthy_workers",
        }
    )
    return payload


def get_worker_health(*, stale_after_seconds: int | None = None) -> dict[str, Any]:
    global _worker_health_cache
    cache_ttl_seconds = _resolve_positive_int_env(
        "SOCIAL_WORKER_HEALTH_CACHE_TTL_SECONDS",
        SOCIAL_WORKER_HEALTH_CACHE_TTL_SECONDS_DEFAULT,
        minimum=0,
    )
    if cache_ttl_seconds <= 0:
        return _query_worker_health(stale_after_seconds=stale_after_seconds)

    now = time_module.monotonic()
    with _worker_health_cache_lock:
        if _worker_health_cache is not None:
            cached_at, cached_stale_after, cached_payload = _worker_health_cache
            if cached_stale_after == stale_after_seconds and (now - cached_at) < cache_ttl_seconds:
                return copy.deepcopy(cached_payload)

    payload = _query_worker_health(stale_after_seconds=stale_after_seconds)
    with _worker_health_cache_lock:
        _worker_health_cache = (time_module.monotonic(), stale_after_seconds, payload)
    return copy.deepcopy(payload)


def assert_worker_available_when_queue_enabled() -> dict[str, Any]:
    if not is_queue_enabled():
        return {
            "healthy": True,
            "healthy_workers": 0,
            "active_workers": 0,
            "total_workers": 0,
            "workers": [],
            "reason": "queue_disabled",
        }

    health = get_worker_health()
    if bool(health.get("healthy")):
        return health

    reason = str(health.get("reason") or "no_healthy_workers")
    if reason == "worker_heartbeat_schema_missing":
        message = "Social worker heartbeat schema is missing. Apply migration 0130 before enabling queue mode."
    else:
        message = "No healthy social ingest workers are reporting heartbeats."
    raise SocialWorkerUnavailableError(message, worker_health=health)


def _empty_queue_status_counts() -> dict[str, int]:
    return dict.fromkeys(QUEUE_STATUS_STAGES, 0)


def get_queue_status(
    *,
    recent_failures_limit: int = 20,
    statement_timeout_ms: int = 5000,
) -> dict[str, Any]:
    global _queue_status_cache
    safe_recent_failures_limit = max(1, min(int(recent_failures_limit), 100))
    safe_statement_timeout_ms = max(1000, min(int(statement_timeout_ms), 30000))
    cache_ttl_seconds = _resolve_positive_int_env(
        "SOCIAL_QUEUE_STATUS_CACHE_TTL_SECONDS",
        SOCIAL_QUEUE_STATUS_CACHE_TTL_SECONDS_DEFAULT,
        minimum=0,
    )

    if cache_ttl_seconds > 0:
        now = time_module.monotonic()
        with _queue_status_cache_lock:
            if _queue_status_cache is not None:
                cached_at, cached_limit, cached_timeout, cached_payload = _queue_status_cache
                if (
                    cached_limit == safe_recent_failures_limit
                    and cached_timeout == safe_statement_timeout_ms
                    and (now - cached_at) < cache_ttl_seconds
                ):
                    return copy.deepcopy(cached_payload)

    def _finalize(payload: dict[str, Any]) -> dict[str, Any]:
        if cache_ttl_seconds > 0:
            with _queue_status_cache_lock:
                _queue_status_cache = (
                    time_module.monotonic(),
                    safe_recent_failures_limit,
                    safe_statement_timeout_ms,
                    payload,
                )
        return copy.deepcopy(payload)

    queue_payload: dict[str, Any] = {
        "by_status": _empty_queue_status_counts(),
        "by_platform": {},
        "by_job_type": {},
        "recent_failures": [],
    }
    errors: list[str] = []

    try:
        if not _relation_exists("social.scrape_jobs"):
            queue_payload["error"] = "scrape_jobs_table_missing"
            return _finalize({
                "queue_enabled": is_queue_enabled(),
                "workers": get_worker_health(),
                "queue": queue_payload,
            })
    except Exception as exc:  # noqa: BLE001
        errors.append(f"scrape_jobs_relation_check_failed: {exc}")

    try:
        with pg.db_connection() as conn:
            with pg.db_cursor(conn=conn) as cur:
                cur.execute("set local statement_timeout = %s", [str(safe_statement_timeout_ms)])
                aggregate_rows = pg.fetch_all_with_cursor(
                    cur,
                    """
                    select
                      coalesce(platform, 'unknown') as platform,
                      coalesce(job_type, 'unknown') as job_type,
                      coalesce(status, 'unknown') as status,
                      count(*)::bigint as total
                    from social.scrape_jobs
                    group by 1, 2, 3
                    """,
                )
        by_status = _empty_queue_status_counts()
        by_platform: dict[str, dict[str, int]] = {}
        by_job_type: dict[str, dict[str, int]] = {}

        for row in aggregate_rows:
            status = str(row.get("status") or "unknown").strip().lower() or "unknown"
            platform = str(row.get("platform") or "unknown").strip().lower() or "unknown"
            job_type = str(row.get("job_type") or "unknown").strip().lower() or "unknown"
            total = int(row.get("total") or 0)

            by_status[status] = by_status.get(status, 0) + total
            platform_bucket = by_platform.setdefault(platform, {})
            platform_bucket[status] = int(platform_bucket.get(status) or 0) + total
            job_type_bucket = by_job_type.setdefault(job_type, {})
            job_type_bucket[status] = int(job_type_bucket.get(status) or 0) + total

        queue_payload["by_status"] = by_status
        queue_payload["by_platform"] = by_platform
        queue_payload["by_job_type"] = by_job_type
    except Exception as exc:  # noqa: BLE001
        logger.warning("Queue status aggregate query failed: %s", exc)
        errors.append(f"queue_aggregate_query_failed: {exc}")

    try:
        features = _scrape_jobs_features()
        select_last_error_code = (
            "last_error_code" if features.get("has_queue_fields") else "null::text as last_error_code"
        )
        select_last_error_class = (
            "last_error_class" if features.get("has_queue_fields") else "null::text as last_error_class"
        )

        with pg.db_connection() as conn:
            with pg.db_cursor(conn=conn) as cur:
                cur.execute("set local statement_timeout = %s", [str(safe_statement_timeout_ms)])
                recent_failures = pg.fetch_all_with_cursor(
                    cur,
                    f"""
                    select
                      id::text as id,
                      platform,
                      job_type,
                      status,
                      error_message,
                      {select_last_error_code},
                      {select_last_error_class},
                      created_at,
                      completed_at
                    from social.scrape_jobs
                    where status in ('failed', 'retrying')
                    order by coalesce(completed_at, created_at) desc
                    limit %s
                    """,
                    [safe_recent_failures_limit],
                )
        queue_payload["recent_failures"] = [
            {
                "id": str(row.get("id") or ""),
                "platform": str(row.get("platform") or ""),
                "job_type": str(row.get("job_type") or ""),
                "status": str(row.get("status") or ""),
                "error_message": str(row.get("error_message") or "") or None,
                "last_error_code": str(row.get("last_error_code") or "") or None,
                "last_error_class": str(row.get("last_error_class") or "") or None,
                "created_at": _iso(_coerce_dt(row.get("created_at"))),
                "completed_at": _iso(_coerce_dt(row.get("completed_at"))),
            }
            for row in recent_failures
        ]
    except Exception as exc:  # noqa: BLE001
        logger.warning("Queue status recent-failures query failed: %s", exc)
        errors.append(f"queue_recent_failures_query_failed: {exc}")

    if errors:
        queue_payload["error"] = "; ".join(errors)

    workers_payload: dict[str, Any]
    try:
        workers_payload = get_worker_health()
    except Exception as exc:  # noqa: BLE001
        logger.warning("Queue status worker-health query failed: %s", exc)
        errors.append(f"queue_worker_health_failed: {exc}")
        queue_payload["error"] = "; ".join(errors)
        workers_payload = {
            "healthy": False,
            "healthy_workers": 0,
            "active_workers": 0,
            "total_workers": 0,
            "stale_after_seconds": _resolve_positive_int_env(
                "SOCIAL_WORKER_HEARTBEAT_STALE_SECONDS",
                SOCIAL_WORKER_HEARTBEAT_STALE_SECONDS_DEFAULT,
                minimum=30,
            ),
            "workers": [],
            "reason": "health_query_failed",
        }

    return _finalize({
        "queue_enabled": is_queue_enabled(),
        "workers": workers_payload,
        "queue": queue_payload,
    })


def _resolve_depth_defaults(
    *,
    max_posts_per_target: int | None,
    max_comments_per_post: int | None,
    max_replies_per_post: int | None,
    fetch_replies: bool,
) -> tuple[int, int, int, bool]:
    default_comments = _resolve_positive_int_env(
        "SOCIAL_DEFAULT_MAX_COMMENTS_PER_POST",
        200,
        minimum=0,
    )
    default_replies = _resolve_positive_int_env(
        "SOCIAL_DEFAULT_MAX_REPLIES_PER_POST",
        100,
        minimum=0,
    )
    resolved_posts = max(1, int(max_posts_per_target or 1))
    resolved_comments = default_comments if max_comments_per_post is None else max(0, int(max_comments_per_post))
    resolved_replies = default_replies if max_replies_per_post is None else max(0, int(max_replies_per_post))
    return (
        resolved_posts,
        resolved_comments,
        resolved_replies,
        fetch_replies,
    )


def _normalize_comment_refresh_policy(raw: Any) -> str:
    normalized = str(raw or DEFAULT_COMMENT_REFRESH_POLICY).strip().lower()
    if normalized not in SUPPORTED_COMMENT_REFRESH_POLICIES:
        return DEFAULT_COMMENT_REFRESH_POLICY
    return normalized


def _normalize_platform_name(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if not normalized:
        return ""

    normalized = re.sub(r"[^a-z0-9]+", " ", normalized).strip()
    tokens = [token for token in normalized.split() if token]
    if not tokens:
        return ""

    token_aliases = {
        "x": "twitter",
        "ig": "instagram",
        "insta": "instagram",
        "fb": "facebook",
        "meta": "facebook",
    }
    if tokens[0] == "meta" and len(tokens) > 1 and tokens[1] == "threads":
        canonical = "threads"
    else:
        canonical = token_aliases.get(tokens[0], tokens[0])
    return canonical


def _normalize_comment_anchor_source_ids(
    raw: Any,
    *,
    allowed_platforms: set[str] | None = None,
    per_platform_limit: int = MAX_COMMENT_ANCHOR_SOURCE_IDS_PER_PLATFORM,
) -> tuple[dict[str, set[str]], list[str]]:
    if not isinstance(raw, dict):
        return {}, []

    normalized: dict[str, set[str]] = {}
    overflow_platforms: list[str] = []
    for key, value in raw.items():
        platform = _normalize_platform_name(key)
        if platform not in SUPPORTED_PLATFORMS:
            continue
        if allowed_platforms is not None and platform not in allowed_platforms:
            continue

        source_ids: list[str] = []
        for item in value if isinstance(value, list) else []:
            normalized_id = str(item or "").strip()
            if normalized_id:
                source_ids.append(normalized_id)
        unique_source_ids = set(source_ids)
        if len(unique_source_ids) > per_platform_limit:
            overflow_platforms.append(platform)
            continue
        normalized[platform] = unique_source_ids
    return normalized, overflow_platforms


def _serialize_comment_anchor_source_ids(
    source_ids: dict[str, set[str]] | None,
) -> dict[str, list[str]] | None:
    if source_ids is None:
        return None
    return {platform: sorted(values) for platform, values in source_ids.items()}


def _target_source_ids_for_platform(opts: IngestOptions, platform: str) -> set[str] | None:
    normalized_platform = _normalize_platform_name(platform)
    source_ids = opts.comment_anchor_source_ids or {}
    if normalized_platform not in source_ids:
        return None
    return set(source_ids.get(normalized_platform) or set())


def _coerce_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, date):
        return datetime.combine(value, time.min, tzinfo=UTC)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
        except ValueError:
            return None
    return None


def _iso(dt: datetime | None) -> str | None:
    if not dt:
        return None
    return dt.astimezone(UTC).isoformat()


def _to_et_date(dt: datetime | None, tz_name: str) -> date | None:
    if dt is None:
        return None
    zone = ZoneInfo(tz_name)
    return dt.astimezone(zone).date()


def _relation_exists(qualified_name: str) -> bool:
    row = pg.fetch_one("select to_regclass(%s) is not null as exists", [qualified_name]) or {}
    return bool(row.get("exists"))


_column_exists_cache: dict[tuple[str, str, str], bool] = {}
_scrape_jobs_features_cache: dict[str, bool] | None = None
_analytics_cache: dict[tuple[Any, ...], tuple[float, dict[str, Any]]] = {}
_analytics_cache_lock = Lock()


def _column_exists(schema: str, table: str, column: str) -> bool:
    key = (schema, table, column)
    cached = _column_exists_cache.get(key)
    if cached is not None:
        return cached
    row = (
        pg.fetch_one(
            """
        select exists (
          select 1
          from information_schema.columns
          where table_schema = %s
            and table_name = %s
            and column_name = %s
        ) as exists
        """,
            [schema, table, column],
        )
        or {}
    )
    result = bool(row.get("exists"))
    _column_exists_cache[key] = result
    return result


def _scrape_jobs_features() -> dict[str, bool]:
    global _scrape_jobs_features_cache
    if _scrape_jobs_features_cache is not None:
        return _scrape_jobs_features_cache
    has_run_id = _column_exists("social", "scrape_jobs", "run_id")
    has_queue_fields = all(
        [
            _column_exists("social", "scrape_jobs", "attempt_count"),
            _column_exists("social", "scrape_jobs", "max_attempts"),
            _column_exists("social", "scrape_jobs", "priority"),
            _column_exists("social", "scrape_jobs", "available_at"),
            _column_exists("social", "scrape_jobs", "claimed_at"),
            _column_exists("social", "scrape_jobs", "heartbeat_at"),
            _column_exists("social", "scrape_jobs", "worker_id"),
            _column_exists("social", "scrape_jobs", "last_error_code"),
            _column_exists("social", "scrape_jobs", "last_error_class"),
        ]
    )
    _scrape_jobs_features_cache = {
        "has_run_id": has_run_id,
        "has_queue_fields": has_queue_fields,
    }
    return _scrape_jobs_features_cache


def _instagram_posts_has_column(column: str) -> bool:
    """Best-effort schema guard for additive instagram_posts columns."""
    try:
        return _column_exists("social", "instagram_posts", column)
    except Exception:
        # Fall back to existing behavior if schema introspection is unavailable.
        return True


def _platform_posts_has_column(platform: str, column: str) -> bool:
    table = PLATFORM_POST_TABLES.get((platform or "").strip().lower())
    if not table:
        return False
    try:
        return _column_exists("social", table, column)
    except Exception:
        return True


def _platform_comments_has_column(platform: str, column: str) -> bool:
    table = PLATFORM_COMMENT_TABLES.get((platform or "").strip().lower())
    if not table:
        return False
    try:
        return _column_exists("social", table, column)
    except Exception:
        return True


def _tiktok_comments_has_column(column: str) -> bool:
    return _platform_comments_has_column("tiktok", column)


def _youtube_is_short_expr(alias: str) -> str:
    if _platform_posts_has_column("youtube", "is_short"):
        return f"coalesce({alias}.is_short, false)"
    return "false"


def _platform_thumbnail_expr(alias: str, platform: str) -> str:
    normalized = (platform or "").strip().lower()
    if normalized == "instagram":
        return _instagram_posts_thumbnail_expr(alias)
    if normalized == "tiktok":
        return (
            "coalesce("
            f"nullif(to_jsonb({alias}) ->> 'hosted_thumbnail_url', ''), "
            f"nullif(to_jsonb({alias}) -> 'hosted_media_urls' ->> 0, ''), "
            f"nullif({alias}.thumbnail_url, '')"
            ")"
        )
    if normalized == "youtube":
        return (
            "coalesce("
            f"nullif(to_jsonb({alias}) ->> 'hosted_thumbnail_url', ''), "
            f"nullif(to_jsonb({alias}) -> 'hosted_media_urls' ->> 0, ''), "
            f"nullif({alias}.thumbnail_url, '')"
            ")"
        )
    if normalized == "twitter":
        return (
            "coalesce("
            f"nullif(to_jsonb({alias}) ->> 'hosted_thumbnail_url', ''), "
            f"nullif(to_jsonb({alias}) -> 'hosted_media_urls' ->> 0, ''), "
            f"nullif({alias}.media_urls ->> 0, '')"
            ")"
        )
    if normalized in {"facebook", "threads"}:
        return (
            "coalesce("
            f"nullif(to_jsonb({alias}) ->> 'hosted_thumbnail_url', ''), "
            f"nullif(to_jsonb({alias}) -> 'hosted_media_urls' ->> 0, ''), "
            f"nullif({alias}.thumbnail_url, ''), "
            f"nullif({alias}.media_urls ->> 0, '')"
            ")"
        )
    return f"nullif({alias}.thumbnail_url, '')"


def _platform_hosted_media_expr(alias: str) -> str:
    return f"coalesce(to_jsonb({alias}) -> 'hosted_media_urls', '[]'::jsonb)"


def _instagram_posts_thumbnail_expr(alias: str = "p") -> str:
    return (
        "coalesce("
        f"nullif(to_jsonb({alias}) ->> 'hosted_thumbnail_url', ''), "
        f"nullif(to_jsonb({alias}) -> 'hosted_media_urls' ->> 0, ''), "
        f"nullif({alias}.thumbnail_url, '')"
        ")"
    )


def _instagram_posts_json_array_expr(alias: str, key: str) -> str:
    return f"coalesce(to_jsonb({alias}) -> '{key}', '[]'::jsonb)"


def _instagram_posts_json_text_expr(alias: str, key: str) -> str:
    return f"(to_jsonb({alias}) ->> '{key}')"


def _instagram_posts_json_int_expr(alias: str, key: str) -> str:
    return f"nullif(to_jsonb({alias}) ->> '{key}', '')::integer"


def _instagram_posts_json_timestamptz_expr(alias: str, key: str) -> str:
    return f"nullif(to_jsonb({alias}) ->> '{key}', '')::timestamptz"


def _instagram_posts_duration_expr(alias: str = "p") -> str:
    return f"nullif(to_jsonb({alias}) ->> 'duration_seconds', '')::integer"


def _assert_social_queue_schema_ready() -> None:
    missing: list[str] = []
    if not _relation_exists("social.scrape_runs"):
        missing.append("social.scrape_runs table (migration 0121)")
    for col, migration in (
        ("run_id", "0122"),
        ("available_at", "0122"),
        ("priority", "0122"),
        ("attempt_count", "0122"),
    ):
        if not _column_exists("social", "scrape_jobs", col):
            missing.append(f"social.scrape_jobs.{col} column (migration {migration})")

    if missing:
        details = "; ".join(missing)
        raise ValueError(
            f"Social ingest queue schema is not migrated. Apply migrations 0121, 0122, 0123. Missing: {details}"
        )


def _slug_words(value: str) -> list[str]:
    cleaned = re.sub(r"[^a-zA-Z0-9 ]+", " ", value).strip()
    return [token for token in cleaned.split() if token]


def _normalize_unique_terms(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for raw in values:
        value = str(raw or "").strip()
        if not value:
            continue
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(value)
    return out


def _derive_show_terms(show_name: str | None) -> tuple[list[str], list[str]]:
    if not show_name:
        return [], []

    words = _slug_words(show_name)
    if not words:
        return [], []

    lower_name = show_name.lower()
    hashtags: list[str] = ["".join(words)]
    keywords: list[str] = [show_name]

    # Canonical RHOSLC aliases requested by product requirements.
    if "salt lake city" in lower_name:
        keywords.extend(["Salt Lake City", "RHOSLC"])
        hashtags.append("RHOSLC")

    # Generic Real Housewives acronym fallback (e.g., RHOBH, RHOP).
    franchise_match = re.search(r"real housewives of (.+)", lower_name)
    if franchise_match:
        suffix_words = [
            token for token in _slug_words(franchise_match.group(1)) if token.lower() not in {"the", "of", "and"}
        ]
        if suffix_words:
            acronym = f"rho{''.join(token[0].lower() for token in suffix_words)}"
            keywords.append(acronym.upper())
            hashtags.append(acronym)

    return _normalize_unique_terms(hashtags), _normalize_unique_terms(keywords)


def _text_contains_any_term(*, text: str | None, hashtags: list[str], keywords: list[str]) -> bool:
    term_hashtags = [str(tag).strip().lstrip("#").lower() for tag in hashtags if str(tag).strip()]
    term_keywords = [str(kw).strip().lower() for kw in keywords if str(kw).strip()]
    if not term_hashtags and not term_keywords:
        return True

    haystack = (text or "").lower()
    if not haystack:
        return False

    for tag in term_hashtags:
        if f"#{tag}" in haystack or re.search(rf"\b{re.escape(tag)}\b", haystack):
            return True
    for keyword in term_keywords:
        if keyword in haystack:
            return True
    return False


def _youtube_title_is_cross_show_excluded(title: str | None) -> bool:
    text = str(title or "").lower()
    if not text:
        return False
    return "wife swap" in text and "real housewives edition" in text


def _youtube_video_matches_show_terms(
    *,
    title: str | None,
    description: str | None,
    hashtags: list[str],
    keywords: list[str],
) -> bool:
    """Apply strict show matching for YouTube ingestion.

    Rules:
    - Match if show terms appear in title or description.
    - Match if configured show hashtags appear in description with ``#`` prefix.
    - Exclude known cross-show promos by title pattern.
    """
    if _youtube_title_is_cross_show_excluded(title):
        return False

    show_hashtags = [
        str(tag).strip().lstrip("#").lower()
        for tag in hashtags
        if str(tag).strip() and not YOUTUBE_GENERIC_SEASON_TERM_RE.fullmatch(str(tag).strip().lstrip("#"))
    ]
    show_keywords = [
        str(keyword).strip()
        for keyword in keywords
        if str(keyword).strip() and not YOUTUBE_GENERIC_SEASON_TERM_RE.fullmatch(str(keyword).strip())
    ]

    if not show_hashtags and not show_keywords:
        return _text_contains_any_term(text=f"{title or ''} {description or ''}", hashtags=hashtags, keywords=keywords)

    title_match = _text_contains_any_term(text=title, hashtags=show_hashtags, keywords=show_keywords)
    if title_match:
        return True

    description_text = str(description or "")
    if not description_text:
        return False

    description_lower = description_text.lower()
    if any(f"#{tag}" in description_lower for tag in show_hashtags):
        return True

    return _text_contains_any_term(text=description_text, hashtags=show_hashtags, keywords=show_keywords)


def _youtube_filter_sample(
    *,
    reason: str,
    title: str | None,
    video_id: str | None,
) -> dict[str, str]:
    payload: dict[str, str] = {"reason": reason}
    cleaned_title = str(title or "").strip()
    cleaned_video_id = str(video_id or "").strip()
    if cleaned_title:
        payload["title"] = cleaned_title
    if cleaned_video_id:
        payload["video_id"] = cleaned_video_id
    return payload


def _build_twitter_or_query(*, hashtags: list[str], keywords: list[str]) -> str:
    terms: list[str] = []
    for keyword in keywords:
        value = str(keyword or "").strip()
        if not value:
            continue
        if " " in value:
            terms.append(f'"{value}"')
        else:
            terms.append(value)
    for hashtag in hashtags:
        value = str(hashtag or "").strip().lstrip("#")
        if not value:
            continue
        terms.append(f"#{value}")

    unique_terms = _normalize_unique_terms(terms)
    if not unique_terms:
        return ""
    if len(unique_terms) == 1:
        return unique_terms[0]
    return f"({' OR '.join(unique_terms)})"


def _load_instagram_cookies() -> dict[str, str]:
    """
    Resolve Instagram auth cookies for season ingest.

    Resolution order:
    1) SOCIAL_INSTAGRAM_COOKIES_JSON / INSTAGRAM_COOKIES_JSON (inline JSON object)
    2) SOCIAL_INSTAGRAM_COOKIES_FILE / INSTAGRAM_COOKIES_FILE (path to JSON file)
    3) scripts/socials/instagram/instagram_cookies.json (repo-local default)
    """
    from trr_backend.socials.instagram import load_cookies_from_file

    raw_json = (os.getenv("SOCIAL_INSTAGRAM_COOKIES_JSON") or "").strip() or (
        os.getenv("INSTAGRAM_COOKIES_JSON") or ""
    ).strip()
    if raw_json:
        try:
            parsed = json.loads(raw_json)
        except json.JSONDecodeError:
            logger.warning("Invalid Instagram cookies JSON from env; falling back to file-based cookies")
        else:
            if isinstance(parsed, dict):
                cookies = {str(k): str(v) for k, v in parsed.items() if v is not None and not str(k).startswith("_")}
                if cookies:
                    return cookies
            logger.warning("Instagram cookies JSON env value is not an object; falling back to file-based cookies")

    file_candidates: list[str] = []
    file_candidates.extend(
        [
            (os.getenv("SOCIAL_INSTAGRAM_COOKIES_FILE") or "").strip(),
            (os.getenv("INSTAGRAM_COOKIES_FILE") or "").strip(),
        ]
    )
    default_path = Path(__file__).resolve().parents[2] / "scripts" / "socials" / "instagram" / "instagram_cookies.json"
    file_candidates.append(str(default_path))

    for raw_path in file_candidates:
        if not raw_path:
            continue
        path = Path(raw_path).expanduser()
        if not path.is_file():
            continue
        try:
            cookies = load_cookies_from_file(str(path))
        except Exception as exc:
            logger.warning("Failed to load Instagram cookies from %s: %s", path, exc)
            continue
        if cookies:
            return cookies

    return {}


def _load_twitter_auth() -> tuple[dict[str, str], str | None]:
    """
    Resolve Twitter auth for season ingest.

    Returns (cookies_dict, bearer_token | None).
    Resolution:
    1) SOCIAL_TWITTER_COOKIES_JSON / TWITTER_COOKIES_JSON (inline JSON)
    2) SOCIAL_TWITTER_COOKIES_FILE / TWITTER_COOKIES_FILE (path to JSON file)
    3) SOCIAL_TWITTER_BEARER_TOKEN / TWITTER_BEARER_TOKEN (bearer token)
    """

    cookies: dict[str, str] = {}
    raw_cookie_header = (os.getenv("SOCIAL_TWITTER_COOKIES_HEADER") or "").strip() or (
        os.getenv("TWITTER_COOKIES_HEADER") or ""
    ).strip()
    if raw_cookie_header:
        cookies = _parse_cookie_header(raw_cookie_header)

    raw_json = (os.getenv("SOCIAL_TWITTER_COOKIES_JSON") or "").strip() or (
        os.getenv("TWITTER_COOKIES_JSON") or ""
    ).strip()
    if raw_json and not cookies:
        try:
            parsed = json.loads(raw_json)
            cookies = _coerce_cookie_map(parsed)
        except json.JSONDecodeError:
            # Allow passing a raw "Cookie:" header string in *_COOKIES_JSON vars.
            cookies = _parse_cookie_header(raw_json)
            if not cookies:
                logger.warning("Invalid Twitter cookies JSON from env")

    if not cookies:
        file_path = (os.getenv("SOCIAL_TWITTER_COOKIES_FILE") or "").strip() or (
            os.getenv("TWITTER_COOKIES_FILE") or ""
        ).strip()
        if file_path:
            p = Path(file_path).expanduser()
            if p.is_file():
                try:
                    with open(p) as f:
                        parsed = json.load(f)
                    cookies = _coerce_cookie_map(parsed)
                except Exception as exc:
                    logger.warning("Failed to load Twitter cookies from %s: %s", p, exc)
    if not cookies:
        cookies = _load_twitter_browser_cookies()

    bearer_token = (
        (os.getenv("SOCIAL_TWITTER_BEARER_TOKEN") or "").strip()
        or (os.getenv("TWITTER_BEARER_TOKEN") or "").strip()
        or None
    )

    return cookies, bearer_token


def _coerce_cookie_map(payload: Any) -> dict[str, str]:
    out: dict[str, str] = {}
    if isinstance(payload, dict):
        # Playwright/Chrome export shape: {"cookies": [{name, value, ...}, ...]}
        nested = payload.get("cookies")
        if isinstance(nested, list):
            return _coerce_cookie_map(nested)
        # Single cookie object shape.
        name = payload.get("name")
        value = payload.get("value")
        if name is not None and value is not None:
            out[str(name)] = str(value)
            return out
        # Flat cookie map shape.
        for key, value in payload.items():
            if value is None:
                continue
            key_str = str(key)
            if key_str.startswith("_"):
                continue
            if isinstance(value, (str, int, float, bool)):
                out[key_str] = str(value)
        return out
    if isinstance(payload, list):
        for item in payload:
            if not isinstance(item, dict):
                continue
            name = item.get("name")
            value = item.get("value")
            if name is None or value is None:
                continue
            out[str(name)] = str(value)
    return out


def _parse_cookie_header(raw: str) -> dict[str, str]:
    cookies: dict[str, str] = {}
    value = str(raw or "").strip()
    if not value:
        return cookies
    if value.lower().startswith("cookie:"):
        value = value.split(":", 1)[1].strip()
    for token in value.split(";"):
        pair = token.strip()
        if not pair or "=" not in pair:
            continue
        key, val = pair.split("=", 1)
        key = key.strip()
        if not key or key.startswith("_"):
            continue
        cookies[key] = val.strip()
    return cookies


def _load_twitter_browser_cookies() -> dict[str, str]:
    """
    Best-effort local browser cookie fallback for X/Twitter auth.

    Disabled unless SOCIAL_TWITTER_BROWSER_COOKIE_FALLBACK is truthy.
    """
    if not _env_truthy("SOCIAL_TWITTER_BROWSER_COOKIE_FALLBACK", default=False):
        return {}
    try:
        import browser_cookie3  # type: ignore[import-not-found]
    except Exception:
        return {}

    browser = (os.getenv("SOCIAL_TWITTER_BROWSER") or "auto").strip().lower()
    domains = [
        token.strip().lower()
        for token in (os.getenv("SOCIAL_TWITTER_COOKIE_DOMAINS") or "x.com,twitter.com").split(",")
        if token.strip()
    ]
    preferred_names = {
        token.strip()
        for token in (os.getenv("SOCIAL_TWITTER_COOKIE_NAMES") or "auth_token,ct0,twid").split(",")
        if token.strip()
    }

    browser_loader_candidates: list[Any] = []
    browser_order = ["chrome", "chromium", "brave", "edge", "opera", "vivaldi", "firefox", "safari"]
    if browser != "auto":
        browser_order = [browser]
    for browser_name in browser_order:
        browser_loader_candidates.append(getattr(browser_cookie3, browser_name, None))
    loaders = [loader for loader in browser_loader_candidates if callable(loader)]

    cookies: dict[str, str] = {}
    for loader in loaders:
        for domain in domains:
            try:
                jar = loader(domain_name=domain)
            except TypeError:
                try:
                    jar = loader()
                except Exception:
                    continue
            except Exception:
                continue
            for cookie in jar:
                name = str(getattr(cookie, "name", "") or "")
                value = str(getattr(cookie, "value", "") or "")
                cookie_domain = str(getattr(cookie, "domain", "") or "").lower()
                if not name or not value:
                    continue
                if preferred_names and name not in preferred_names:
                    continue
                if domain and not cookie_domain.endswith(domain):
                    continue
                cookies[name] = value
            if cookies.get("auth_token") and cookies.get("ct0"):
                return cookies
    return cookies


def _load_twikit_credentials(twitter_cookies: Mapping[str, Any] | None = None) -> dict[str, str] | None:
    """
    Load twikit auth from env vars, cookie file, or Twitter cookie sources.

    Resolution order (first match wins):
    1) TWIKIT_COOKIES_JSON – inline JSON cookie payload
    2) TWIKIT_COOKIES_FILE – JSON file with auth_token + ct0 keys
    3) TWIKIT_AUTH_TOKEN + TWIKIT_CT0 – inline cookie values
    4) TWIKIT_USERNAME + TWIKIT_PASSWORD (+ TWIKIT_EMAIL) – login creds
    5) Twitter cookie sources (preloaded cookies/header/JSON/file)
    6) Browser cookie fallback
    """

    def _extract_auth_ct0(payload: Any) -> tuple[str | None, str | None]:
        auth_token: str | None = None
        ct0: str | None = None
        if isinstance(payload, dict):
            if payload.get("auth_token") and payload.get("ct0"):
                return str(payload.get("auth_token")), str(payload.get("ct0"))
            nested = payload.get("cookies")
            if isinstance(nested, list):
                return _extract_auth_ct0(nested)
            name = payload.get("name")
            value = payload.get("value")
            if name == "auth_token" and value is not None:
                auth_token = str(value)
            if name == "ct0" and value is not None:
                ct0 = str(value)
            return auth_token, ct0
        if isinstance(payload, list):
            for item in payload:
                if not isinstance(item, dict):
                    continue
                name = item.get("name")
                value = item.get("value")
                if name == "auth_token" and value is not None:
                    auth_token = str(value)
                elif name == "ct0" and value is not None:
                    ct0 = str(value)
            return auth_token, ct0
        return None, None

    def _from_cookie_map(cookies: Mapping[str, Any] | None) -> dict[str, str] | None:
        if not cookies:
            return None
        auth_token = str(cookies.get("auth_token") or "").strip()
        ct0 = str(cookies.get("ct0") or "").strip()
        if auth_token and ct0:
            return {"auth_token": auth_token, "ct0": ct0}
        return None

    # 1) TWIKIT_COOKIES_JSON
    raw_cookie_json = (os.getenv("TWIKIT_COOKIES_JSON") or "").strip()
    if raw_cookie_json:
        try:
            parsed = json.loads(raw_cookie_json)
            auth_token, ct0 = _extract_auth_ct0(parsed)
            if auth_token and ct0:
                return {"auth_token": auth_token, "ct0": ct0}
        except Exception:
            logger.warning("Invalid TWIKIT_COOKIES_JSON payload; expected JSON cookie object/list")

    # 2) TWIKIT_COOKIES_FILE
    cookie_file = (os.getenv("TWIKIT_COOKIES_FILE") or "").strip()
    if cookie_file:
        p = Path(cookie_file).expanduser()
        if p.is_file():
            try:
                with open(p) as f:
                    parsed = json.load(f)
                auth_token, ct0 = _extract_auth_ct0(parsed)
                if auth_token and ct0:
                    return {"auth_token": auth_token, "ct0": ct0}
            except Exception as exc:
                logger.warning("Failed to load twikit cookies from %s: %s", p, exc)
        else:
            logger.debug("TWIKIT_COOKIES_FILE does not exist: %s", p)

    # 3) Inline TWIKIT cookie env vars
    auth_token = (os.getenv("TWIKIT_AUTH_TOKEN") or "").strip()
    ct0 = (os.getenv("TWIKIT_CT0") or "").strip()
    if auth_token and ct0:
        return {"auth_token": auth_token, "ct0": ct0}

    # 4) TWIKIT login credentials
    username = (os.getenv("TWIKIT_USERNAME") or "").strip()
    password = (os.getenv("TWIKIT_PASSWORD") or "").strip()
    email = (os.getenv("TWIKIT_EMAIL") or "").strip()
    if username and password:
        return {"username": username, "email": email or username, "password": password}

    # 5a) Preloaded Twitter cookies from caller.
    from_preloaded = _from_cookie_map(twitter_cookies)
    if from_preloaded:
        return from_preloaded

    # 5b) Twitter cookie header env vars.
    raw_cookie_header = (os.getenv("SOCIAL_TWITTER_COOKIES_HEADER") or "").strip() or (
        os.getenv("TWITTER_COOKIES_HEADER") or ""
    ).strip()
    if raw_cookie_header:
        parsed = _parse_cookie_header(raw_cookie_header)
        from_header = _from_cookie_map(parsed)
        if from_header:
            return from_header

    # 5c) Twitter JSON cookie env vars.
    raw_twitter_cookie_json = (os.getenv("SOCIAL_TWITTER_COOKIES_JSON") or "").strip() or (
        os.getenv("TWITTER_COOKIES_JSON") or ""
    ).strip()
    if raw_twitter_cookie_json:
        try:
            parsed = json.loads(raw_twitter_cookie_json)
            from_twitter_json = _from_cookie_map(_coerce_cookie_map(parsed))
            if from_twitter_json:
                return from_twitter_json
        except json.JSONDecodeError:
            parsed_header = _parse_cookie_header(raw_twitter_cookie_json)
            from_twitter_header = _from_cookie_map(parsed_header)
            if from_twitter_header:
                return from_twitter_header

    # 5d) Twitter cookie file env vars.
    twitter_cookie_file = (os.getenv("SOCIAL_TWITTER_COOKIES_FILE") or "").strip() or (
        os.getenv("TWITTER_COOKIES_FILE") or ""
    ).strip()
    if twitter_cookie_file:
        p = Path(twitter_cookie_file).expanduser()
        if p.is_file():
            try:
                with open(p) as f:
                    parsed = json.load(f)
                from_twitter_file = _from_cookie_map(_coerce_cookie_map(parsed))
                if from_twitter_file:
                    return from_twitter_file
            except Exception as exc:
                logger.debug("Failed to derive twikit credentials from Twitter cookie file %s: %s", p, exc)

    # 6) Browser cookie fallback.
    browser_cookies = _load_twitter_browser_cookies()
    from_browser = _from_cookie_map(browser_cookies)
    if from_browser:
        return from_browser

    return None


def _load_tiktok_cookies() -> dict[str, str]:
    """
    Resolve TikTok cookies for season ingest.

    Resolution:
    1) SOCIAL_TIKTOK_COOKIES_JSON / TIKTOK_COOKIES_JSON (inline JSON)
    2) SOCIAL_TIKTOK_COOKIES_FILE / TIKTOK_COOKIES_FILE (path to JSON file)
    """
    raw_json = (os.getenv("SOCIAL_TIKTOK_COOKIES_JSON") or "").strip() or (
        os.getenv("TIKTOK_COOKIES_JSON") or ""
    ).strip()
    if raw_json:
        try:
            parsed = json.loads(raw_json)
            if isinstance(parsed, dict):
                cookies = {str(k): str(v) for k, v in parsed.items() if v is not None and not str(k).startswith("_")}
                if cookies:
                    return cookies
        except json.JSONDecodeError:
            logger.warning("Invalid TikTok cookies JSON from env")

    file_path = (os.getenv("SOCIAL_TIKTOK_COOKIES_FILE") or "").strip() or (
        os.getenv("TIKTOK_COOKIES_FILE") or ""
    ).strip()
    if file_path:
        p = Path(file_path).expanduser()
        if p.is_file():
            try:
                with open(p) as f:
                    parsed = json.load(f)
                if isinstance(parsed, dict):
                    return {str(k): str(v) for k, v in parsed.items() if v is not None}
            except Exception as exc:
                logger.warning("Failed to load TikTok cookies from %s: %s", p, exc)

    return {}


def _load_facebook_cookies() -> dict[str, str]:
    """Resolve Facebook cookies for season ingest/debug scraping."""
    raw_json = (os.getenv("SOCIAL_FACEBOOK_COOKIES_JSON") or "").strip() or (
        os.getenv("FACEBOOK_COOKIES_JSON") or ""
    ).strip()
    if raw_json:
        try:
            parsed = json.loads(raw_json)
            if isinstance(parsed, dict):
                cookies = {str(k): str(v) for k, v in parsed.items() if v is not None and not str(k).startswith("_")}
                if cookies:
                    return cookies
        except json.JSONDecodeError:
            logger.warning("Invalid Facebook cookies JSON from env")

    file_path = (os.getenv("SOCIAL_FACEBOOK_COOKIES_FILE") or "").strip() or (
        os.getenv("FACEBOOK_COOKIES_FILE") or ""
    ).strip()
    if file_path:
        p = Path(file_path).expanduser()
        if p.is_file():
            try:
                with open(p) as f:
                    parsed = json.load(f)
                if isinstance(parsed, dict):
                    return {str(k): str(v) for k, v in parsed.items() if v is not None}
            except Exception as exc:
                logger.warning("Failed to load Facebook cookies from %s: %s", p, exc)
    return {}


def _load_threads_cookies() -> dict[str, str]:
    """Resolve Threads cookies for season ingest/debug scraping."""
    raw_json = (os.getenv("SOCIAL_THREADS_COOKIES_JSON") or "").strip() or (
        os.getenv("THREADS_COOKIES_JSON") or ""
    ).strip()
    if raw_json:
        try:
            parsed = json.loads(raw_json)
            if isinstance(parsed, dict):
                cookies = {str(k): str(v) for k, v in parsed.items() if v is not None and not str(k).startswith("_")}
                if cookies:
                    return cookies
        except json.JSONDecodeError:
            logger.warning("Invalid Threads cookies JSON from env")

    file_path = (os.getenv("SOCIAL_THREADS_COOKIES_FILE") or "").strip() or (
        os.getenv("THREADS_COOKIES_FILE") or ""
    ).strip()
    if file_path:
        p = Path(file_path).expanduser()
        if p.is_file():
            try:
                with open(p) as f:
                    parsed = json.load(f)
                if isinstance(parsed, dict):
                    return {str(k): str(v) for k, v in parsed.items() if v is not None}
            except Exception as exc:
                logger.warning("Failed to load Threads cookies from %s: %s", p, exc)
    return {}


# ---------------------------------------------------------------------------
# Context + targets
# ---------------------------------------------------------------------------


def get_season_context(season_id: str) -> SeasonContext:
    season = pg.fetch_one(
        """
        select
          s.id::text as season_id,
          s.show_id::text as show_id,
          s.season_number,
          s.air_date,
          sh.name as show_name,
          lower(trim(both '-' from regexp_replace(regexp_replace(coalesce(sh.name, ''), '&', ' and ', 'gi'), '[^a-z0-9]+', '-', 'gi'))) as show_slug
        from core.seasons s
        join core.shows sh on sh.id = s.show_id
        where s.id = %s
        """,
        [season_id],
    )
    if not season:
        raise ValueError(f"Season {season_id} not found")

    episode = pg.fetch_one(
        """
        select e.air_date
        from core.episodes e
        where e.season_id = %s
          and e.air_date is not null
        order by e.air_date asc
        limit 1
        """,
        [season_id],
    )

    anchor: date
    if episode and isinstance(episode.get("air_date"), date):
        anchor = episode["air_date"]
    elif isinstance(season.get("air_date"), date):
        anchor = season["air_date"]
    else:
        anchor = datetime.now(tz=ZoneInfo("America/New_York")).date()

    return SeasonContext(
        season_id=str(season["season_id"]),
        show_id=str(season["show_id"]),
        show_name=season.get("show_name"),
        show_slug=season.get("show_slug"),
        season_number=int(season.get("season_number") or 0),
        anchor_date=anchor,
    )


def _default_targets(context: SeasonContext, *, source_scope: str = "bravo") -> list[dict[str, Any]]:
    if source_scope != "bravo":
        return []

    default_hashtags, default_keywords = _derive_show_terms(context.show_name)
    keywords = _normalize_unique_terms([*default_keywords, f"season {context.season_number}"])
    hashtags = _normalize_unique_terms(default_hashtags)

    defaults = [
        {
            "platform": "instagram",
            "source_scope": source_scope,
            "timezone": "America/New_York",
            "accounts": ["bravotv", "bravodailydish", "bravowwhl"],
            "hashtags": hashtags,
            "keywords": keywords,
            "is_active": True,
            "config": {"include_comments": True},
        },
        {
            "platform": "tiktok",
            "source_scope": source_scope,
            "timezone": "America/New_York",
            "accounts": ["bravotv", "bravowwhl"],
            "hashtags": hashtags,
            "keywords": keywords,
            "is_active": True,
            "config": {"include_comments": True},
        },
        {
            "platform": "twitter",
            "source_scope": source_scope,
            "timezone": "America/New_York",
            "accounts": ["BravoTV", "BravoWWHL"],
            "hashtags": hashtags,
            "keywords": keywords,
            "is_active": True,
            "config": {"include_replies": True},
        },
        {
            "platform": "youtube",
            "source_scope": source_scope,
            "timezone": "America/New_York",
            "accounts": ["bravo", "wwhl"],
            "hashtags": [],
            "keywords": keywords,
            "is_active": True,
            "config": {"include_comments": True},
        },
        {
            "platform": "facebook",
            "source_scope": source_scope,
            "timezone": "America/New_York",
            "accounts": ["Bravo"],
            "hashtags": hashtags,
            "keywords": keywords,
            "is_active": True,
            "config": {"include_comments": True},
        },
        {
            "platform": "threads",
            "source_scope": source_scope,
            "timezone": "America/New_York",
            "accounts": ["bravotv"],
            "hashtags": hashtags,
            "keywords": keywords,
            "is_active": True,
            "config": {"include_comments": True},
        },
    ]
    return defaults


def _season_identity(season_id: str) -> dict[str, Any]:
    season = pg.fetch_one(
        """
        select
          s.id::text as season_id,
          s.show_id::text as show_id,
          s.season_number,
          sh.name as show_name
        from core.seasons s
        join core.shows sh on sh.id = s.show_id
        where s.id = %s
        """,
        [season_id],
    )
    if not season:
        raise ValueError(f"Season {season_id} not found")
    return season


def get_targets(season_id: str, *, source_scope: str = "bravo") -> dict[str, Any]:
    rows = pg.fetch_all(
        """
        select
          t.season_id::text as season_id,
          t.show_id::text as show_id,
          s.season_number,
          sh.name as show_name,
          t.platform,
          t.source_scope,
          t.timezone,
          t.accounts,
          t.hashtags,
          t.keywords,
          t.is_active,
          t.config,
          t.updated_by,
          t.updated_at,
          t.created_at
        from social.season_targets t
        join core.seasons s on s.id = t.season_id
        join core.shows sh on sh.id = t.show_id
        where t.season_id = %s
          and t.source_scope = %s
        order by t.platform asc
        """,
        [season_id, source_scope],
    )
    using_defaults = False
    if rows:
        season_number = int(rows[0].get("season_number") or 0)
        show_name = rows[0].get("show_name")
        targets = [
            {key: value for key, value in row.items() if key not in {"season_number", "show_name"}} for row in rows
        ]
        return {
            "season_id": str(rows[0].get("season_id") or season_id),
            "show_id": str(rows[0].get("show_id") or ""),
            "season_number": season_number,
            "show_name": show_name,
            "source_scope": source_scope,
            "targets": targets,
            "using_defaults": False,
        }

    context = get_season_context(season_id)
    if not rows:
        rows = _default_targets(context, source_scope=source_scope)
        using_defaults = len(rows) > 0

    return {
        "season_id": context.season_id,
        "show_id": context.show_id,
        "season_number": context.season_number,
        "show_name": context.show_name,
        "source_scope": source_scope,
        "targets": rows,
        "using_defaults": using_defaults,
    }


def _normalize_account_handle(value: Any) -> str:
    return str(value or "").strip().lstrip("@").lower()


def _extract_youtube_handle_from_value(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    match = YOUTUBE_HANDLE_URL_RE.search(raw)
    if match:
        return _normalize_account_handle(match.group(1))
    if raw.startswith("@"):
        return _normalize_account_handle(raw)
    if re.fullmatch(r"[A-Za-z0-9._-]+", raw):
        return _normalize_account_handle(raw)
    return ""


def _youtube_handles_match(expected_handle: str, candidate_handle: str) -> bool:
    expected = _normalize_account_handle(expected_handle)
    candidate = _normalize_account_handle(candidate_handle)
    if not expected or not candidate:
        return False
    if expected == candidate:
        return True
    if expected == f"{candidate}tv" or candidate == f"{expected}tv":
        return True
    if expected.startswith(candidate) or candidate.startswith(expected):
        return True
    return False


def _youtube_video_matches_owner_identity(
    video: Any,
    *,
    expected_handle: str,
    expected_channel_id: str | None,
) -> bool:
    normalized_expected_handle = _normalize_account_handle(expected_handle)
    normalized_expected_channel_id = str(expected_channel_id or "").strip()

    video_channel_id = str(getattr(video, "channel_id", "") or "").strip()
    if normalized_expected_channel_id and video_channel_id:
        return video_channel_id == normalized_expected_channel_id

    if not normalized_expected_handle:
        return True

    candidate_values = [
        getattr(video, "channel_title", None),
        getattr(video, "url", None),
    ]
    raw_data = getattr(video, "raw_data", None)
    if isinstance(raw_data, dict):
        candidate_values.extend(
            [
                raw_data.get("uploader_id"),
                raw_data.get("channel"),
                raw_data.get("uploader"),
                raw_data.get("channel_url"),
                raw_data.get("uploader_url"),
                raw_data.get("webpage_url"),
            ]
        )
    owner_candidates = {
        handle for handle in (_extract_youtube_handle_from_value(value) for value in candidate_values) if handle
    }
    if owner_candidates:
        return any(_youtube_handles_match(normalized_expected_handle, handle) for handle in owner_candidates)
    return True


def _target_accounts_by_platform(
    season_id: str,
    *,
    source_scope: str,
    context: SeasonContext | None = None,
) -> dict[str, set[str]]:
    rows = pg.fetch_all(
        """
        select platform, accounts, is_active
        from social.season_targets
        where season_id = %s
          and source_scope = %s
        """,
        [season_id, source_scope],
    )

    target_rows: list[dict[str, Any]]
    if rows:
        target_rows = list(rows)
        present_platforms = {
            str(row.get("platform") or "").strip().lower()
            for row in target_rows
            if str(row.get("platform") or "").strip()
        }
        resolved_context = context
        if source_scope == "bravo":
            if resolved_context is None:
                resolved_context = get_season_context(season_id)
            for default_target in _default_targets(resolved_context, source_scope=source_scope):
                platform = str(default_target.get("platform") or "").strip().lower()
                if not platform or platform in present_platforms:
                    continue
                target_rows.append(default_target)
                present_platforms.add(platform)
    else:
        resolved_context = context or get_season_context(season_id)
        target_rows = _default_targets(resolved_context, source_scope=source_scope)

    accounts_by_platform: dict[str, set[str]] = {platform: set() for platform in SUPPORTED_PLATFORMS}

    for target in target_rows:
        if not bool(target.get("is_active", True)):
            continue
        platform = str(target.get("platform") or "").strip().lower()
        if platform not in accounts_by_platform:
            continue
        for account in target.get("accounts") or []:
            normalized = _normalize_account_handle(account)
            if normalized:
                accounts_by_platform[platform].add(normalized)

    return accounts_by_platform


def put_targets(
    season_id: str,
    *,
    source_scope: str,
    targets: list[dict[str, Any]],
    updated_by: str | None,
) -> dict[str, Any]:
    context = get_season_context(season_id)

    if source_scope not in SUPPORTED_SCOPES:
        raise ValueError(f"Unsupported source scope: {source_scope}")

    upserted: list[dict[str, Any]] = []
    for target in targets:
        platform = str(target.get("platform") or "").strip().lower()
        if platform not in {"instagram", "tiktok", "twitter", "youtube", "facebook", "threads", "reddit"}:
            raise ValueError(f"Unsupported platform: {platform}")

        accounts = target.get("accounts") or []
        hashtags = target.get("hashtags") or []
        keywords = target.get("keywords") or []
        timezone = str(target.get("timezone") or "America/New_York")
        is_active = bool(target.get("is_active", True))
        config = target.get("config") or {}

        row = pg.fetch_one(
            """
            insert into social.season_targets (
              season_id,
              show_id,
              platform,
              source_scope,
              timezone,
              accounts,
              hashtags,
              keywords,
              is_active,
              config,
              updated_by,
              updated_at
            )
            values (
              %s,
              %s,
              %s,
              %s,
              %s,
              %s::jsonb,
              %s::jsonb,
              %s::jsonb,
              %s,
              %s::jsonb,
              %s,
              now()
            )
            on conflict (season_id, platform, source_scope)
            do update set
              show_id = excluded.show_id,
              timezone = excluded.timezone,
              accounts = excluded.accounts,
              hashtags = excluded.hashtags,
              keywords = excluded.keywords,
              is_active = excluded.is_active,
              config = excluded.config,
              updated_by = excluded.updated_by,
              updated_at = now()
            returning
              season_id::text,
              show_id::text,
              platform,
              source_scope,
              timezone,
              accounts,
              hashtags,
              keywords,
              is_active,
              config,
              updated_by,
              updated_at,
              created_at
            """,
            [
                season_id,
                context.show_id,
                platform,
                source_scope,
                timezone,
                json.dumps(accounts),
                json.dumps(hashtags),
                json.dumps(keywords),
                is_active,
                json.dumps(config),
                updated_by,
            ],
        )
        if row:
            upserted.append(row)

    return {
        "season_id": context.season_id,
        "show_id": context.show_id,
        "season_number": context.season_number,
        "show_name": context.show_name,
        "source_scope": source_scope,
        "targets": sorted(upserted, key=lambda item: str(item.get("platform") or "")),
    }


# ---------------------------------------------------------------------------
# Ingest helpers
# ---------------------------------------------------------------------------


def _touch_worker_heartbeat_for_job(
    *,
    job_id: str,
    status: str,
    metadata: dict[str, Any] | None = None,
) -> None:
    if not _worker_heartbeat_schema_ready():
        return
    pg.fetch_one(
        """
        update social.scrape_workers as w
        set
          stage = coalesce(j.config->>'stage', j.metadata->>'stage', j.job_type, w.stage),
          status = %s,
          run_id = coalesce(j.run_id, w.run_id),
          current_job_id = j.id,
          metadata = coalesce(w.metadata, '{}'::jsonb) || %s::jsonb,
          last_seen_at = now(),
          updated_at = now()
        from social.scrape_jobs as j
        where j.id = %s::uuid
          and j.worker_id is not null
          and w.worker_id = j.worker_id
        returning w.worker_id
        """,
        [_normalize_worker_status(status), json.dumps(metadata or {}), job_id],
    )


def _clear_worker_heartbeat_for_job(
    *,
    job_id: str,
    status: str = "idle",
    metadata: dict[str, Any] | None = None,
) -> None:
    if not _worker_heartbeat_schema_ready():
        return
    pg.fetch_one(
        """
        update social.scrape_workers as w
        set
          status = %s,
          current_job_id = null,
          metadata = coalesce(w.metadata, '{}'::jsonb) || %s::jsonb,
          last_seen_at = now(),
          updated_at = now()
        from social.scrape_jobs as j
        where j.id = %s::uuid
          and j.worker_id is not null
          and w.worker_id = j.worker_id
        returning w.worker_id
        """,
        [_normalize_worker_status(status), json.dumps(metadata or {}), job_id],
    )


def _create_job(
    context: SeasonContext,
    *,
    run_id: str | None,
    platform: str,
    source_scope: str,
    job_type: str,
    stage: str,
    config: dict[str, Any],
    initiated_by: str | None,
    status: str,
    priority: int = 100,
    worker_id: str | None = None,
    preclaim: bool = False,
    conn: Any | None = None,
) -> str:
    effective_status = status
    if preclaim and worker_id:
        effective_status = "running"
    with pg.db_cursor(conn=conn) as cur:
        row = pg.fetch_one_with_cursor(
            cur,
            """
        insert into social.scrape_jobs (
          run_id,
          platform,
          job_type,
          config,
          status,
          available_at,
          priority,
          show_id,
          season_id,
          source_scope,
          initiated_by,
          metadata,
          worker_id,
          started_at,
          claimed_at,
          heartbeat_at,
          attempt_count
        )
        values (
          %s,
          %s,
          %s,
          %s::jsonb,
          %s,
          now(),
          %s,
          %s,
          %s,
          %s,
          %s,
          %s::jsonb,
          %s,
          case when %s then now() else null end,
          case when %s then now() else null end,
          case when %s then now() else null end,
          case when %s then 1 else 0 end
        )
        returning id::text
        """,
            [
                run_id,
                platform,
                job_type,
                json.dumps(config),
                effective_status,
                priority,
                context.show_id,
                context.season_id,
                source_scope,
                initiated_by,
                json.dumps({"stage": stage}),
                worker_id,
                preclaim,
                preclaim,
                preclaim,
                preclaim,
            ],
        )
    if not row:
        raise RuntimeError("Failed to create scrape job")
    return str(row["id"])


def _set_job_running(job_id: str, *, worker_id: str | None = None) -> None:
    pg.fetch_one(
        """
        update social.scrape_jobs
        set
          status = 'running',
          started_at = coalesce(started_at, now()),
          claimed_at = now(),
          heartbeat_at = now(),
          worker_id = coalesce(%s, worker_id),
          attempt_count = attempt_count + 1
        where id = %s
        returning id::text
        """,
        [worker_id, job_id],
    )


def _touch_job_heartbeat(job_id: str, *, worker_id: str | None = None) -> None:
    pg.fetch_one(
        """
        update social.scrape_jobs
        set
          heartbeat_at = now(),
          worker_id = coalesce(%s, worker_id)
        where id = %s
        returning id::text
        """,
        [worker_id, job_id],
    )
    _touch_worker_heartbeat_for_job(
        job_id=job_id,
        status="working",
        metadata={"source": "job_heartbeat"},
    )


def _update_job_progress(
    job_id: str,
    *,
    items_found: int,
    metadata: dict[str, Any] | None = None,
) -> None:
    pg.fetch_one(
        """
        update social.scrape_jobs
        set
          items_found = %s,
          metadata = coalesce(metadata, '{}'::jsonb) || %s::jsonb,
          heartbeat_at = now()
        where id = %s
          and status in ('queued', 'pending', 'retrying', 'running')
        returning id::text
        """,
        [items_found, json.dumps(metadata or {}), job_id],
    )
    _touch_worker_heartbeat_for_job(
        job_id=job_id,
        status="working",
        metadata={"source": "job_progress"},
    )


def _finish_job(
    job_id: str,
    *,
    status: str,
    items_found: int,
    error_message: str | None = None,
    metadata: dict[str, Any] | None = None,
    last_error_code: str | None = None,
    last_error_class: str | None = None,
    next_available_at: datetime | None = None,
) -> None:
    is_terminal = status in {"completed", "failed", "cancelled"}
    completed_expr = "now()" if is_terminal else "completed_at"
    pg.fetch_one(
        """
        update social.scrape_jobs
        set
          status = %s,
          items_found = %s,
          error_message = %s,
          completed_at = """
        + completed_expr
        + """,
          metadata = coalesce(%s::jsonb, '{}'::jsonb),
          heartbeat_at = now(),
          last_error_code = %s,
          last_error_class = %s,
          available_at = coalesce(%s, available_at)
        where id = %s
        returning id::text
        """,
        [
            status,
            items_found,
            error_message,
            json.dumps(metadata or {}),
            last_error_code,
            last_error_class,
            next_available_at,
            job_id,
        ],
    )
    if is_terminal:
        _clear_worker_heartbeat_for_job(
            job_id=job_id,
            status="idle",
            metadata={"source": "job_finish", "job_status": status},
        )
    else:
        _touch_worker_heartbeat_for_job(
            job_id=job_id,
            status="working",
            metadata={"source": "job_finish", "job_status": status},
        )


def _create_run(
    context: SeasonContext,
    *,
    source_scope: str,
    initiated_by: str | None,
    config: dict[str, Any],
    status: str,
) -> str:
    row = pg.fetch_one(
        """
        insert into social.scrape_runs (
          season_id,
          show_id,
          source_scope,
          status,
          initiated_by,
          config,
          started_at
        )
        values (
          %s,
          %s,
          %s,
          %s,
          %s,
          %s::jsonb,
          case when %s = 'running' then now() else null end
        )
        returning id::text
        """,
        [
            context.season_id,
            context.show_id,
            source_scope,
            status,
            initiated_by,
            json.dumps(config),
            status,
        ],
    )
    if not row:
        raise RuntimeError("Failed to create social scrape run")
    return str(row["id"])


def _set_run_status(run_id: str, status: str) -> None:
    pg.fetch_one(
        """
        update social.scrape_runs
        set
          status = %s,
          started_at = case
            when %s = 'running' then coalesce(started_at, now())
            else started_at
          end,
          completed_at = case
            when %s in ('completed', 'failed', 'cancelled') then coalesce(completed_at, now())
            else completed_at
          end,
          cancelled_at = case
            when %s = 'cancelled' then coalesce(cancelled_at, now())
            else cancelled_at
          end
        where id = %s
        returning id::text
        """,
        [status, status, status, status, run_id],
    )


def _update_run_summary(run_id: str) -> dict[str, Any]:
    summary_row = (
        pg.fetch_one(
            """
        with stats as (
          select
            count(*)::int as total_jobs,
            count(*) filter (where status = 'completed')::int as completed_jobs,
            count(*) filter (where status = 'failed')::int as failed_jobs,
            count(*) filter (where status in ('queued', 'pending', 'retrying', 'running'))::int as active_jobs,
            coalesce(sum(items_found), 0)::int as items_found_total
          from social.scrape_jobs
          where run_id = %s
        ),
        stage_stats as (
          select
            coalesce(config->>'stage', 'unknown') as stage,
            count(*)::int as total,
            count(*) filter (where status = 'completed')::int as completed,
            count(*) filter (where status = 'failed')::int as failed,
            count(*) filter (where status in ('queued', 'pending', 'retrying', 'running'))::int as active
          from social.scrape_jobs
          where run_id = %s
          group by coalesce(config->>'stage', 'unknown')
        )
        select
          (select row_to_json(stats) from stats) as stats,
          coalesce((select jsonb_object_agg(stage, jsonb_build_object(
            'total', total,
            'completed', completed,
            'failed', failed,
            'active', active
          )) from stage_stats), '{}'::jsonb) as stage_counts
        """,
            [run_id, run_id],
        )
        or {}
    )

    stats = summary_row.get("stats") or {}
    stage_counts = summary_row.get("stage_counts") or {}
    summary = {
        "total_jobs": int(stats.get("total_jobs") or 0),
        "completed_jobs": int(stats.get("completed_jobs") or 0),
        "failed_jobs": int(stats.get("failed_jobs") or 0),
        "active_jobs": int(stats.get("active_jobs") or 0),
        "items_found_total": int(stats.get("items_found_total") or 0),
        "stage_counts": stage_counts,
    }
    pg.fetch_one(
        """
        update social.scrape_runs
        set summary = %s::jsonb
        where id = %s
        returning id::text
        """,
        [json.dumps(summary), run_id],
    )
    return summary


def _finalize_run_status(run_id: str) -> dict[str, Any]:
    summary = _update_run_summary(run_id)
    active_jobs = int(summary.get("active_jobs") or 0)
    failed_jobs = int(summary.get("failed_jobs") or 0)
    current = pg.fetch_one("select status from social.scrape_runs where id = %s", [run_id]) or {}
    if str(current.get("status")) == "cancelled":
        return summary
    if active_jobs > 0:
        _set_run_status(run_id, "running")
    elif failed_jobs > 0:
        _set_run_status(run_id, "failed")
    else:
        _set_run_status(run_id, "completed")
    return summary


def _normalize_non_negative_int(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _new_job_progress_state() -> dict[str, Any]:
    return {
        "last_items_found": -1,
        "last_progress_at": None,
    }


def _build_progress_activity_payload(activity: dict[str, Any] | None, *, now: datetime) -> dict[str, Any]:
    payload: dict[str, Any] = {"last_progress_at": _iso(now)}
    if not activity:
        return payload
    if activity.get("phase"):
        payload["phase"] = str(activity.get("phase"))
    for field in ("pages_scanned", "posts_checked", "matched_posts"):
        if field in activity and activity.get(field) is not None:
            payload[field] = _normalize_non_negative_int(activity.get(field))
    return payload


def _emit_job_progress(
    *,
    job_id: str,
    stage: str,
    platform: str,
    account: str,
    scraped_posts: int,
    scraped_comments: int,
    posts_upserted: int,
    comments_upserted: int,
    activity: dict[str, Any] | None,
    progress_state: dict[str, Any],
    force: bool = False,
) -> bool:
    now = _now_utc()
    scraped_posts_value = _normalize_non_negative_int(scraped_posts)
    scraped_comments_value = _normalize_non_negative_int(scraped_comments)
    items_found = scraped_posts_value + scraped_comments_value
    last_items_found = _normalize_non_negative_int(progress_state.get("last_items_found"))
    last_progress_at = _coerce_dt(progress_state.get("last_progress_at"))
    elapsed_seconds = (
        float("inf")
        if last_progress_at is None
        else (
            now - (last_progress_at if last_progress_at.tzinfo else last_progress_at.replace(tzinfo=UTC))
        ).total_seconds()
    )
    delta = abs(items_found - last_items_found)
    if not force:
        if delta < JOB_PROGRESS_MIN_DELTA and elapsed_seconds < JOB_PROGRESS_MAX_INTERVAL_SECONDS:
            return False

    metadata = {
        "stage": stage,
        "platform": platform,
        "account": account,
        "stage_counters": {
            "posts": scraped_posts_value,
            "comments": scraped_comments_value,
        },
        "persist_counters": {
            "posts_upserted": _normalize_non_negative_int(posts_upserted),
            "comments_upserted": _normalize_non_negative_int(comments_upserted),
        },
        "activity": _build_progress_activity_payload(activity, now=now),
    }
    _update_job_progress(job_id, items_found=items_found, metadata=metadata)
    progress_state["last_items_found"] = items_found
    progress_state["last_progress_at"] = now
    return True


def _resolve_social_job_stale_seconds() -> int:
    return _resolve_positive_int_env(
        "SOCIAL_JOB_STALE_SECONDS",
        SOCIAL_JOB_STALE_SECONDS_DEFAULT,
        minimum=30,
    )


def recover_stale_running_jobs(
    *,
    run_id: str | None = None,
    stage: str | None = None,
    platform: str | None = None,
    stale_after_seconds: int | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    stale_seconds = max(30, stale_after_seconds or _resolve_social_job_stale_seconds())
    stale_limit = max(1, min(int(limit), 250))
    recovered_rows = pg.fetch_all(
        """
        with stale_jobs as (
          select j.id
          from social.scrape_jobs j
          where j.status = 'running'
            and coalesce(j.heartbeat_at, j.started_at, j.claimed_at, j.created_at) <
              now() - (%s * interval '1 second')
            and (%s::uuid is null or j.run_id = %s::uuid)
            and (
              %s::text is null
              or coalesce(j.config->>'stage', j.metadata->>'stage', j.job_type) = %s::text
            )
            and (%s::text is null or j.platform = %s::text)
          order by coalesce(j.heartbeat_at, j.started_at, j.claimed_at, j.created_at) asc
          for update skip locked
          limit %s
        )
        update social.scrape_jobs as j
        set
          status = case
            when j.attempt_count < j.max_attempts then 'retrying'
            else 'failed'
          end,
          completed_at = case
            when j.attempt_count < j.max_attempts then j.completed_at
            else now()
          end,
          available_at = case
            when j.attempt_count < j.max_attempts
              then now() + (
                greatest(5, least(300, 5 * (2 ^ greatest(0, j.attempt_count - 1))))
                * interval '1 second'
              )
            else j.available_at
          end,
          error_message = format(
            'stale_heartbeat_timeout: no heartbeat for >= %%s seconds',
            %s
          ),
          heartbeat_at = now(),
          last_error_code = 'stale_heartbeat_timeout',
          last_error_class = 'HeartbeatTimeout',
          metadata = coalesce(j.metadata, '{}'::jsonb) || jsonb_build_object(
            'error_code', 'stale_heartbeat_timeout',
            'error_class', 'HeartbeatTimeout',
            'retryable', (j.attempt_count < j.max_attempts),
            'stale_heartbeat_timeout_seconds', %s,
            'stale_recovered_at', now()
          )
        from stale_jobs
        where j.id = stale_jobs.id
        returning
          j.id::text as id,
          j.run_id::text as run_id,
          j.platform,
          j.status,
          j.attempt_count,
          j.max_attempts
        """,
        [stale_seconds, run_id, run_id, stage, stage, platform, platform, stale_limit, stale_seconds, stale_seconds],
    )
    if not recovered_rows:
        return []

    for row in recovered_rows:
        if row.get("id"):
            _clear_worker_heartbeat_for_job(
                job_id=str(row.get("id")),
                status="idle",
                metadata={"source": "stale_recovery", "job_status": row.get("status")},
            )

    affected_run_ids = sorted({str(row.get("run_id") or "") for row in recovered_rows if row.get("run_id")})
    for stale_run_id in affected_run_ids:
        _finalize_run_status(stale_run_id)
    return recovered_rows


def _parse_platform_time(ts: Any) -> datetime | None:
    """Parse a platform timestamp (unix int/float or ISO string) to UTC datetime."""
    if isinstance(ts, int):
        return datetime.fromtimestamp(ts, tz=UTC)
    if isinstance(ts, float):
        return datetime.fromtimestamp(int(ts), tz=UTC)
    return _coerce_dt(ts)


# Backwards-compatible aliases
_parse_instagram_time = _parse_platform_time
_parse_tiktok_time = _parse_platform_time


def _as_text_list(value: Any, *, prefix: str = "", strip_prefix: str | None = None) -> list[str]:
    items: list[Any]
    if isinstance(value, list):
        items = value
    elif isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (TypeError, json.JSONDecodeError):
            return []
        items = parsed if isinstance(parsed, list) else []
    else:
        return []

    out: list[str] = []
    seen: set[str] = set()
    for item in items:
        text = str(item or "").strip()
        if not text:
            continue
        if strip_prefix and text.startswith(strip_prefix):
            text = text[len(strip_prefix) :].strip()
        if prefix and not text.startswith(prefix):
            text = f"{prefix}{text}"
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(text)
    return out


def _extract_shortcode_from_permalink(value: str | None) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if "/" not in text:
        return text if INSTAGRAM_SHORTCODE_RE.match(text) else ""
    parsed = urlparse(text)
    segments = [segment for segment in parsed.path.split("/") if segment]
    if len(segments) >= 2 and segments[0] in {"p", "reel", "tv"}:
        candidate = str(segments[1] or "").strip()
        return candidate if INSTAGRAM_SHORTCODE_RE.match(candidate) else ""
    return ""


def _parse_instagram_caption_hashtags(text: str | None) -> list[str]:
    if not text:
        return []
    return _normalize_unique_terms([tag.lstrip("#") for tag in re.findall(r"(?<![\w.])#([A-Za-z0-9_]+)", text)])


def _parse_instagram_caption_mentions(text: str | None) -> list[str]:
    if not text:
        return []
    mentions = [f"@{mention}" for mention in re.findall(r"(?<![\w.])@([A-Za-z0-9_.]+)", text)]
    return _normalize_unique_terms(mentions)


def _enrich_instagram_post_from_permalink(
    *,
    post: Any,
    scraper: Any,
    now_utc: datetime,
) -> None:
    from trr_backend.socials.instagram import resolve_instagram_media

    shortcode = str(getattr(post, "shortcode", "") or "").strip()
    if not shortcode:
        post.metadata_source = None
        post.metadata_scraped_at = now_utc
        post.metadata_error = "missing_shortcode"
        post.media_retrieval_meta = {
            "selected_source": None,
            "attempts": [],
        }
        return

    def _api_fetcher(value: str) -> dict[str, Any] | None:
        return scraper.fetch_post_info(value, delay=0.0)

    resolution = resolve_instagram_media(
        shortcode,
        session=getattr(scraper, "session", None),
        cookies=getattr(scraper, "cookies", None),
        fetch_post_info=_api_fetcher,
    )
    metadata = resolution.metadata
    attempts = list(resolution.attempts or [])
    post.media_retrieval_meta = {
        "selected_source": resolution.source,
        "attempts": attempts,
    }

    if resolution.source:
        post.metadata_source = resolution.source

    caption = str(getattr(post, "caption", "") or "")
    profile_tags = _as_text_list(getattr(post, "profile_tags", []))
    hashtags = _parse_instagram_caption_hashtags(caption)
    mentions = _parse_instagram_caption_mentions(caption)
    collaborators = _as_text_list(getattr(post, "collaborators", []), strip_prefix="@")
    duration_seconds = None
    raw_post_type = str(getattr(post, "post_type", "") or "").strip().lower()
    if raw_post_type in {"reel", "carousel", "post"}:
        post_format = raw_post_type
    elif raw_post_type in {"video", "image"}:
        post_format = "post"
    else:
        post_format = "post"

    if metadata is not None:
        if metadata.taken_at is not None:
            post.taken_at = metadata.taken_at
        profile_tags = metadata.profile_tags or profile_tags
        collaborators = metadata.collaborators or collaborators
        hashtags = metadata.hashtags or hashtags
        mentions = metadata.mentions or mentions
        duration_seconds = metadata.duration_seconds
        post_format = metadata.post_format or post_format
        if metadata.thumbnail_url:
            post.thumbnail_url = metadata.thumbnail_url
        if metadata.media_urls:
            post.media_urls = metadata.media_urls

    post.post_format = post_format
    post.profile_tags = profile_tags
    post.collaborators = collaborators
    post.hashtags = hashtags
    post.mentions = mentions
    post.duration_seconds = duration_seconds
    post.metadata_scraped_at = now_utc
    if metadata is None:
        reason = ""
        for attempt in reversed(attempts):
            candidate = str(attempt.get("reason_code") or "").strip()
            if candidate:
                reason = candidate
                break
        post.metadata_error = reason or "instagram_media_not_found"
    else:
        post.metadata_error = None


def _resolve_instagram_media_for_shortcode(
    *,
    shortcode: str,
) -> dict[str, Any]:
    from trr_backend.socials.instagram import InstagramScraper, resolve_instagram_media

    cookies = _load_instagram_cookies()
    scraper = InstagramScraper(cookies=cookies)

    def _api_fetcher(value: str) -> dict[str, Any] | None:
        return scraper.fetch_post_info(value, delay=0.0)

    resolution = resolve_instagram_media(
        shortcode,
        session=scraper.session,
        cookies=scraper.cookies,
        fetch_post_info=_api_fetcher,
    )
    metadata = resolution.metadata
    media_urls = [str(url).strip() for url in ((metadata.media_urls if metadata else []) or []) if str(url).strip()]
    thumbnail_url = str((metadata.thumbnail_url if metadata else "") or "").strip() or (
        media_urls[0] if media_urls else ""
    )
    return {
        "source": resolution.source,
        "media_type": resolution.media_type,
        "media_urls": media_urls,
        "thumbnail_url": thumbnail_url or None,
        "attempts": list(resolution.attempts or []),
        "metadata": metadata,
    }


def _resolve_youtube_media_for_video_id(
    *,
    video_id: str,
) -> dict[str, Any]:
    from trr_backend.socials.youtube import resolve_youtube_media

    resolution = resolve_youtube_media(video_id)
    media_urls = [str(url).strip() for url in (resolution.media_urls or []) if str(url).strip()]
    thumbnail_url = str(resolution.thumbnail_url or "").strip() or (media_urls[0] if media_urls else "")
    return {
        "source": resolution.source,
        "media_type": "video",
        "media_urls": media_urls,
        "thumbnail_url": thumbnail_url or None,
        "attempts": list(resolution.attempts or []),
    }


def _resolve_tiktok_media_for_video_id(
    *,
    video_id: str,
    canonical_url: str | None = None,
    allow_ytdlp: bool = True,
    validate_download_url: bool = False,
) -> dict[str, Any]:
    from trr_backend.socials.tiktok import resolve_tiktok_media

    resolution = resolve_tiktok_media(
        video_id,
        canonical_url=canonical_url,
        allow_ytdlp=allow_ytdlp,
        validate_download_url=validate_download_url,
    )
    media_urls = [str(url).strip() for url in (resolution.media_urls or []) if str(url).strip()]
    thumbnail_url = str(resolution.thumbnail_url or "").strip() or (media_urls[0] if media_urls else "")
    return {
        "source": resolution.source,
        "media_type": "video",
        "media_urls": media_urls,
        "thumbnail_url": thumbnail_url or None,
        "attempts": list(resolution.attempts or []),
    }


def _normalize_instagram_mirror_attempts(value: Any) -> list[dict[str, Any]]:
    attempts: list[dict[str, Any]] = []
    if not isinstance(value, list):
        return attempts
    for item in value:
        if not isinstance(item, dict):
            continue
        attempts.append(
            {
                "source": str(item.get("source") or ""),
                "success": bool(item.get("success")),
                "reason_code": str(item.get("reason_code") or "") or None,
                "http_status": _normalize_non_negative_int(item.get("http_status")) or None,
                "selected_url_count": _normalize_non_negative_int(item.get("selected_url_count")),
                "error_type": str(item.get("error_type") or "") or None,
                "error_message": str(item.get("error_message") or "")[:240] or None,
            }
        )
    return attempts


def _infer_extension_from_media_url(url: str, content_type: str | None) -> str:
    parsed = urlparse(url)
    suffix = Path(parsed.path).suffix.lower()
    if suffix in {".jpg", ".jpeg", ".png", ".gif", ".webp", ".mp4", ".mov", ".m4v", ".webm"}:
        return suffix
    guessed = mimetypes.guess_extension((content_type or "").split(";", 1)[0].strip().lower())
    if guessed:
        return guessed
    return ".bin"


def _is_video_like_media_url(url: str) -> bool:
    value = str(url or "").strip().lower()
    if not value:
        return False
    parsed = urlparse(value)
    host = parsed.netloc
    path = parsed.path
    if "video.twimg.com" in host:
        return True
    return path.endswith((".mp4", ".mov", ".m4v", ".webm"))


def _is_page_like_media_url(url: str) -> bool:
    value = str(url or "").strip()
    if not value:
        return False
    parsed = urlparse(value)
    host = (parsed.netloc or "").strip().lower()
    path = (parsed.path or "").strip().lower()
    if not host:
        return False
    if path.endswith((".html", ".htm")):
        return True
    if _is_video_like_media_url(value):
        return False
    if path.endswith((".jpg", ".jpeg", ".png", ".gif", ".webp", ".avif", ".bmp")):
        return False
    if "tiktok.com" in host and "/video/" in path:
        return True
    if ("youtube.com" in host and (path == "/watch" or path.startswith("/shorts/") or path.startswith("/embed/"))) or (
        host == "youtu.be"
    ):
        return True
    if ("twitter.com" in host or host == "x.com" or host.endswith(".x.com")) and "/status/" in path:
        return True
    if "instagram.com" in host and (path.startswith("/p/") or path.startswith("/reel/") or path.startswith("/tv/")):
        return True
    if "facebook.com" in host and ("/reel/" in path or "/videos/" in path or "/posts/" in path):
        return True
    if "threads.net" in host and "/post/" in path:
        return True
    return False


def _is_twitter_thumb_placeholder_url(url: str) -> bool:
    value = str(url or "").strip().lower()
    if not value:
        return False
    parsed = urlparse(value)
    if "pbs.twimg.com" not in parsed.netloc:
        return False
    path = parsed.path
    return "/tweet_video_thumb/" in path or "/ext_tw_video_thumb/" in path


def _select_thumbnail_candidate(media_urls: list[str], fallback: str = "") -> str:
    for candidate in media_urls:
        if not _is_video_like_media_url(candidate):
            return str(candidate).strip()
    return fallback or (str(media_urls[0]).strip() if media_urls else "")


def _merge_twitter_media_urls(existing_urls: list[str], summary_urls: list[str]) -> list[str]:
    existing = _normalize_unique_terms([str(url).strip() for url in (existing_urls or []) if str(url).strip()])
    summary = _normalize_unique_terms([str(url).strip() for url in (summary_urls or []) if str(url).strip()])
    if not summary:
        return existing
    if not existing:
        return summary

    existing_has_video = any(_is_video_like_media_url(url) for url in existing)
    existing_all_placeholders = all(_is_twitter_thumb_placeholder_url(url) for url in existing)
    if existing_all_placeholders and any(_is_video_like_media_url(url) for url in summary):
        return summary
    if existing_has_video:
        merged = list(existing)
        for url in summary:
            if url not in merged:
                merged.append(url)
        return merged
    return summary


def _classify_mirror_download_exception(exc: Exception) -> tuple[str, bool]:
    if isinstance(exc, requests.exceptions.Timeout):
        return "request_timeout", True
    if isinstance(exc, requests.exceptions.ConnectionError):
        return "connection_error", True
    if isinstance(exc, requests.exceptions.HTTPError):
        status_code = getattr(getattr(exc, "response", None), "status_code", None)
        if status_code in {429, 500, 502, 503, 504}:
            return f"http_{status_code}", True
        if status_code in {401, 403}:
            return f"http_{status_code}_auth_or_expired", False
        if status_code == 404:
            return "http_404_not_found", False
        if status_code is not None:
            return f"http_{status_code}", False
        return "http_error", False
    if isinstance(exc, requests.exceptions.RequestException):
        return "request_error", True

    reason = str(exc or "").strip().lower()
    if not reason:
        return "mirror_exception", False
    if reason in {"asset_too_large", "empty_response_body", "invalid_source_url"}:
        return reason, False
    return reason, False


def _is_retryable_mirror_reason(reason: str) -> bool:
    normalized = str(reason or "").strip().lower()
    if normalized.startswith("download_failed:") or normalized.startswith("upload_failed:"):
        normalized = normalized.split(":", 1)[1]
    return normalized in {
        "request_timeout",
        "connection_error",
        "request_error",
        "http_429",
        "http_500",
        "http_502",
        "http_503",
        "http_504",
    }


def _build_mirror_source_key(platform: str, post: Any, *, source_urls: list[str]) -> str:
    normalized_platform = (platform or "").strip().lower()
    raw_source_id = ""
    if normalized_platform == "instagram":
        raw_source_id = _extract_shortcode_from_permalink(str(getattr(post, "shortcode", "") or ""))
    elif normalized_platform == "tiktok":
        raw_source_id = str(getattr(post, "video_id", "") or "").strip()
    elif normalized_platform == "youtube":
        raw_source_id = str(getattr(post, "video_id", "") or "").strip()
    elif normalized_platform == "twitter":
        raw_source_id = str(getattr(post, "tweet_id", "") or "").strip()
    elif normalized_platform in {"facebook", "threads"}:
        raw_source_id = str(getattr(post, "post_id", "") or "").strip()

    if raw_source_id:
        safe_source_id = re.sub(r"[^A-Za-z0-9_-]", "", raw_source_id)[:64]
        if safe_source_id:
            return safe_source_id

    fallback_id = str(getattr(post, "id", "") or getattr(post, "pk", "") or "").strip()
    if not fallback_id:
        seed = "|".join(source_urls) or f"{normalized_platform}-media"
        fallback_id = hashlib.sha1(seed.encode("utf-8", "ignore")).hexdigest()[:12]
    safe = re.sub(r"[^A-Za-z0-9_-]", "", fallback_id)[:24] or "asset"
    return f"unknown-{safe}" if normalized_platform == "instagram" else safe


def _build_mirror_shortcode(post: Any, *, source_urls: list[str]) -> str:
    return _build_mirror_source_key("instagram", post, source_urls=source_urls)


# ---------------------------------------------------------------------------
# Structured post & media display-name helpers
# ---------------------------------------------------------------------------

_PLATFORM_DISPLAY_LABELS = {
    "instagram": "Instagram",
    "tiktok": "TikTok",
    "youtube": "YouTube",
    "twitter": "Twitter",
    "facebook": "Facebook",
    "threads": "Threads",
}


def _resolve_show_slug(context: SeasonContext) -> str:
    if context.show_slug and context.show_slug.strip():
        return re.sub(r"[^A-Za-z0-9]", "", context.show_slug.strip())
    if context.show_name:
        return re.sub(r"[^A-Za-z0-9]", "", context.show_name)
    return "UnknownShow"


def _resolve_post_username(platform: str, post_row: dict[str, Any]) -> str:
    raw = ""
    if (platform or "").strip().lower() == "youtube":
        raw = str(post_row.get("channel_title") or post_row.get("post_username") or post_row.get("source_account") or "").strip()
    else:
        raw = str(post_row.get("username") or post_row.get("post_username") or post_row.get("source_account") or "").strip()
    raw = raw.lstrip("@")
    sanitized = re.sub(r"[^A-Za-z0-9_]", "", raw)
    return sanitized or "unknown"


def _compute_daily_post_number(
    platform: str,
    season_id: str,
    username: str,
    posted_at: datetime,
    post_source_id: str,
) -> int:
    normalized_platform = (platform or "").strip().lower()
    table = PLATFORM_POST_TABLES.get(normalized_platform)
    posted_at_column = PLATFORM_POSTED_AT_COLUMN.get(normalized_platform)
    source_id_column = PLATFORM_SOURCE_ID_COLUMN.get(normalized_platform)
    if not table or not posted_at_column or not source_id_column:
        return 1

    tz = ZoneInfo("America/New_York")
    local_dt = posted_at.astimezone(tz) if posted_at.tzinfo else posted_at.replace(tzinfo=tz)
    day_start = local_dt.replace(hour=0, minute=0, second=0, microsecond=0)
    day_end = day_start + timedelta(days=1)

    if normalized_platform == "youtube":
        account_expr = "coalesce(nullif(source_account, ''), nullif(channel_title, ''), '')"
    else:
        account_expr = "coalesce(nullif(username, ''), nullif(source_account, ''), '')"
    normalized_username = username.strip().lower().lstrip("@")

    reply_filter = ""
    if normalized_platform == "twitter":
        reply_filter = " and p.is_reply = false"

    try:
        rows = pg.fetch_all(
            f"""
            select p.{source_id_column} as source_id
            from social.{table} p
            where p.season_id = %s
              and ltrim(lower({account_expr}), '@') = %s
              and p.{posted_at_column} >= %s
              and p.{posted_at_column} < %s{reply_filter}
            order by p.{posted_at_column} asc, p.{source_id_column} asc
            """,
            [season_id, normalized_username, day_start, day_end],
        )
    except Exception:  # noqa: BLE001
        return 1
    for idx, row in enumerate(rows, 1):
        if str(row.get("source_id") or "").strip() == str(post_source_id).strip():
            return idx
    return 1


def _build_post_display_name(
    *,
    show_slug: str,
    username: str,
    platform: str,
    post_number: int,
) -> str:
    platform_label = _PLATFORM_DISPLAY_LABELS.get(platform.lower(), platform.capitalize())
    return f"{show_slug}{username}{platform_label}Post{post_number}"


def _build_media_object_name(
    display_name: str,
    *,
    is_thumbnail: bool,
    slide_number: int | None,
) -> str:
    if is_thumbnail:
        return f"{display_name}_Thumbnail"
    if slide_number is not None:
        return f"{display_name}_S{slide_number}"
    return display_name


def ensure_media_mirror_s3_ready() -> None:
    from trr_backend.media.s3_mirror import get_s3_config

    get_s3_config()


def _mirror_platform_media_to_s3_result(
    context: SeasonContext,
    *,
    platform: str,
    post: Any,
    week_index: int | None,
    prefer_first_media_as_thumbnail: bool = True,
    display_name: str | None = None,
) -> dict[str, Any]:
    from trr_backend.media.s3_mirror import (
        build_hosted_url,
        get_s3_bucket,
        get_s3_client,
        get_s3_prefix,
    )

    max_asset_bytes = _resolve_positive_int_env(
        "SOCIAL_MEDIA_MIRROR_MAX_BYTES",
        SOCIAL_MEDIA_MIRROR_MAX_BYTES_DEFAULT,
        minimum=1024,
    )
    download_retries = _resolve_positive_int_env(
        "SOCIAL_MEDIA_MIRROR_DOWNLOAD_RETRIES",
        SOCIAL_MEDIA_MIRROR_DOWNLOAD_RETRIES_DEFAULT,
        minimum=1,
    )
    retry_backoff_seconds = _resolve_positive_int_env(
        "SOCIAL_MEDIA_MIRROR_DOWNLOAD_RETRY_BACKOFF_SECONDS",
        SOCIAL_MEDIA_MIRROR_DOWNLOAD_RETRY_BACKOFF_SECONDS_DEFAULT,
        minimum=0,
    )

    normalized_platform = (platform or "").strip().lower()
    source_media_urls = [str(url).strip() for url in (getattr(post, "media_urls", []) or []) if str(url or "").strip()]
    source_media_urls = _normalize_unique_terms(source_media_urls)
    source_thumbnail_url = str(getattr(post, "thumbnail_url", "") or "").strip()
    if prefer_first_media_as_thumbnail and not source_thumbnail_url and source_media_urls:
        source_thumbnail_url = source_media_urls[0]
    if not source_thumbnail_url and not source_media_urls:
        return {
            "hosted_thumbnail_url": None,
            "hosted_media_urls": [],
            "status": "pending",
            "error": None,
            "retryable_error": False,
            "source_count": 0,
            "mirrored_count": 0,
        }

    try:
        s3_client = get_s3_client()
        bucket = get_s3_bucket()
        prefix = get_s3_prefix()
    except Exception as exc:  # noqa: BLE001
        return {
            "hosted_thumbnail_url": None,
            "hosted_media_urls": [],
            "status": "failed",
            "error": f"s3_setup_failed:{exc}",
            "retryable_error": False,
            "source_count": len(source_media_urls) + (1 if source_thumbnail_url else 0),
            "mirrored_count": 0,
        }

    source_urls = ([source_thumbnail_url] if source_thumbnail_url else []) + source_media_urls
    source_key = _build_mirror_source_key(normalized_platform, post, source_urls=source_urls)
    week_part = f"week-{week_index}" if week_index is not None else "week-unknown"
    key_root = (
        f"social/{normalized_platform}/{context.show_id}/{context.season_number}/{week_part}/{source_key}"
    ).strip("/")
    if prefix:
        key_root = f"{prefix.strip('/')}/{key_root}"

    errors: list[str] = []
    hosted_thumbnail_url: str | None = None
    hosted_media_urls: list[str] = []
    mirrored_count = 0
    uploaded_by_source_url: dict[str, str] = {}
    saw_retryable_error = False

    def _download_with_ytdlp(source_url: str) -> tuple[str, str]:
        temp_dir = tempfile.mkdtemp(prefix=f"{normalized_platform}-mirror-ytdlp-")
        output_template = os.path.join(temp_dir, "media.%(ext)s")
        try:
            cmd = [
                os.getenv("SOCIAL_MEDIA_MIRROR_YTDLP_BIN", "yt-dlp"),
                "--no-playlist",
                "--merge-output-format",
                "mp4",
                "-f",
                "bv*+ba/b",
                "-o",
                output_template,
                source_url,
            ]
            subprocess.run(
                cmd,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=180,
            )
            sized_candidates: list[tuple[int, str]] = []
            for candidate in sorted(glob.glob(os.path.join(temp_dir, "media.*"))):
                if not os.path.exists(candidate):
                    continue
                try:
                    size = os.path.getsize(candidate)
                except OSError:
                    continue
                if size > 0:
                    sized_candidates.append((size, candidate))
            if not sized_candidates:
                raise RuntimeError("ytdlp_no_output")
            sized_candidates.sort(reverse=True)
            candidate = sized_candidates[0][1]
            return candidate, "video/mp4"
        except Exception:
            try:
                for candidate in glob.glob(os.path.join(temp_dir, "media.*")):
                    try:
                        os.remove(candidate)
                    except OSError:
                        pass
                os.rmdir(temp_dir)
            except OSError:
                pass
            raise

    def _download_and_upload(source_url: str, *, object_name: str, is_thumbnail: bool) -> str:
        if source_url in uploaded_by_source_url:
            return uploaded_by_source_url[source_url]

        if not source_url.lower().startswith(("http://", "https://")):
            raise RuntimeError("invalid_source_url")

        content_type = "application/octet-stream"
        temp_path: str | None = None
        used_ytdlp = False
        if not is_thumbnail and normalized_platform in {"tiktok", "youtube"} and _is_page_like_media_url(source_url):
            try:
                temp_path, content_type = _download_with_ytdlp(source_url)
                used_ytdlp = True
            except Exception as exc:  # noqa: BLE001
                raise RuntimeError(f"download_failed:ytdlp_failed:{exc.__class__.__name__}") from exc
        else:
            for attempt in range(download_retries):
                try:
                    with requests.get(
                        source_url,
                        timeout=(10, 60),
                        headers={
                            "user-agent": (
                                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
                            ),
                            "accept": "*/*",
                        },
                        stream=True,
                    ) as response:
                        response.raise_for_status()
                        content_type = response.headers.get("Content-Type") or "application/octet-stream"
                        total_bytes = 0
                        with tempfile.NamedTemporaryFile(
                            prefix=f"{normalized_platform}-mirror-",
                            suffix=".bin",
                            delete=False,
                        ) as temp_file:
                            temp_path = temp_file.name
                            for chunk in response.iter_content(chunk_size=SOCIAL_MEDIA_MIRROR_CHUNK_SIZE_BYTES):
                                if not chunk:
                                    continue
                                total_bytes += len(chunk)
                                if total_bytes > max_asset_bytes:
                                    raise RuntimeError("asset_too_large")
                                temp_file.write(chunk)
                    if total_bytes <= 0 or not temp_path:
                        raise RuntimeError("empty_response_body")
                    break
                except Exception as exc:  # noqa: BLE001
                    reason, retryable = _classify_mirror_download_exception(exc)
                    if temp_path:
                        try:
                            os.remove(temp_path)
                        except OSError:
                            pass
                        temp_path = None
                    if retryable and attempt + 1 < download_retries:
                        if retry_backoff_seconds > 0:
                            time_module.sleep(retry_backoff_seconds * (2**attempt))
                        continue
                    raise RuntimeError(f"download_failed:{reason}") from exc

        if not temp_path:
            raise RuntimeError("empty_response_body")
        try:
            if os.path.getsize(temp_path) > max_asset_bytes:
                raise RuntimeError("asset_too_large")
        except OSError as exc:
            raise RuntimeError("empty_response_body") from exc

        ext = (
            (Path(temp_path).suffix.lower() or _infer_extension_from_media_url(source_url, content_type))
            if used_ytdlp
            else _infer_extension_from_media_url(source_url, content_type)
        )
        key = f"{key_root}/{object_name}{ext}"
        try:
            with open(temp_path, "rb") as fileobj:
                s3_client.upload_fileobj(
                    fileobj,
                    bucket,
                    key,
                    ExtraArgs={
                        "ContentType": content_type.split(";", 1)[0].strip() or "application/octet-stream",
                        "CacheControl": "public, max-age=31536000, immutable",
                    },
                )
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"upload_failed:{exc.__class__.__name__}") from exc
        try:
            os.remove(temp_path)
            if used_ytdlp:
                try:
                    os.rmdir(os.path.dirname(temp_path))
                except OSError:
                    pass
        except OSError:
            pass

        hosted_url = build_hosted_url(key)
        uploaded_by_source_url[source_url] = hosted_url
        return hosted_url

    if source_thumbnail_url:
        try:
            thumb_obj_name = (
                _build_media_object_name(display_name, is_thumbnail=True, slide_number=None)
                if display_name
                else "thumbnail"
            )
            hosted_thumbnail_url = _download_and_upload(
                source_thumbnail_url,
                object_name=thumb_obj_name,
                is_thumbnail=True,
            )
            mirrored_count += 1
        except Exception as exc:  # noqa: BLE001
            reason = str(exc or "").strip() or "mirror_exception"
            errors.append(f"thumbnail:{reason}")
            saw_retryable_error = saw_retryable_error or _is_retryable_mirror_reason(reason)

    is_multi = len(source_media_urls) > 1
    for idx, media_url in enumerate(source_media_urls):
        try:
            if display_name:
                # Clear dedupe cache when media URL matches thumbnail so it gets its own S3 key
                if media_url in uploaded_by_source_url and media_url == source_thumbnail_url:
                    del uploaded_by_source_url[media_url]
                media_obj_name = _build_media_object_name(
                    display_name, is_thumbnail=False, slide_number=idx + 1 if is_multi else None,
                )
            else:
                media_obj_name = f"media-{idx + 1:02d}"
            hosted_url = _download_and_upload(
                media_url,
                object_name=media_obj_name,
                is_thumbnail=False,
            )
            hosted_media_urls.append(hosted_url)
            mirrored_count += 1
        except Exception as exc:  # noqa: BLE001
            reason = str(exc or "").strip() or "mirror_exception"
            errors.append(f"media[{idx}]:{reason}")
            saw_retryable_error = saw_retryable_error or _is_retryable_mirror_reason(reason)

    source_count = len(source_media_urls) + (1 if source_thumbnail_url else 0)
    if source_count == 0:
        status = "pending"
    elif mirrored_count == source_count:
        status = "mirrored"
    elif mirrored_count > 0:
        status = "partial"
    else:
        status = "failed"
    error_text = "; ".join(errors)[:1500] if errors else None
    return {
        "hosted_thumbnail_url": hosted_thumbnail_url,
        "hosted_media_urls": hosted_media_urls,
        "status": status,
        "error": error_text,
        "retryable_error": bool(error_text and saw_retryable_error),
        "source_count": source_count,
        "mirrored_count": mirrored_count,
    }


def _mirror_instagram_media_to_s3_result(
    context: SeasonContext,
    *,
    post: Any,
    week_index: int | None,
) -> dict[str, Any]:
    return _mirror_platform_media_to_s3_result(context, platform="instagram", post=post, week_index=week_index)


def _mirror_instagram_media_to_s3(
    context: SeasonContext,
    *,
    post: Any,
    week_index: int | None,
) -> tuple[str | None, list[str], str | None, str | None]:
    result = _mirror_instagram_media_to_s3_result(context, post=post, week_index=week_index)
    return (
        result.get("hosted_thumbnail_url"),
        list(result.get("hosted_media_urls") or []),
        str(result.get("status") or "") or None,
        str(result.get("error") or "") or None,
    )


def _mirror_instagram_profile_pics_for_post(
    post_row: dict[str, Any],
    *,
    post: Any,
) -> dict[str, Any]:
    """Mirror profile pictures for a post's owner and tagged users to S3.

    Returns dict with:
      hosted_owner_profile_pic_url: str | None
      hosted_tagged_profile_pics: dict[str, str]  (username -> hosted_url)
      profile_pic_mirror_status: str
      profile_pic_mirror_error: str | None
    """
    import hashlib
    import io

    import requests as http_requests

    from trr_backend.media.s3_mirror import (
        build_hosted_url,
        build_instagram_profile_pic_s3_key,
        get_s3_bucket,
        get_s3_client,
        guess_ext_from_content_type,
    )

    # Collect all (username, profile_pic_url) pairs
    pic_urls: dict[str, str] = {}  # username -> source_url

    owner_detail = getattr(post, "owner_detail", None)
    if owner_detail and getattr(owner_detail, "profile_pic_url", None):
        pic_urls[owner_detail.username] = owner_detail.profile_pic_url

    for detail_list_attr in ("tagged_users_detail", "collaborators_detail"):
        for user in getattr(post, detail_list_attr, []) or []:
            url = (
                getattr(user, "profile_pic_url", None)
                if hasattr(user, "profile_pic_url")
                else user.get("profile_pic_url")
                if isinstance(user, dict)
                else None
            )
            uname = (
                getattr(user, "username", None)
                if hasattr(user, "username")
                else user.get("username")
                if isinstance(user, dict)
                else None
            )
            if url and uname and uname not in pic_urls:
                pic_urls[uname] = url

    if not pic_urls:
        return {
            "hosted_owner_profile_pic_url": None,
            "hosted_tagged_profile_pics": {},
            "profile_pic_mirror_status": "skipped",
            "profile_pic_mirror_error": None,
        }

    try:
        s3_client = get_s3_client()
        bucket = get_s3_bucket()
    except Exception as exc:
        return {
            "hosted_owner_profile_pic_url": None,
            "hosted_tagged_profile_pics": {},
            "profile_pic_mirror_status": "failed",
            "profile_pic_mirror_error": f"s3_setup_failed:{exc}",
        }

    errors: list[str] = []
    hosted_map: dict[str, str] = {}
    # Dedup by source URL (multiple usernames can share same pic URL)
    url_to_hosted: dict[str, str] = {}

    for username, source_url in pic_urls.items():
        if source_url in url_to_hosted:
            hosted_map[username] = url_to_hosted[source_url]
            continue
        try:
            resp = http_requests.get(
                source_url,
                timeout=(10, 30),
                headers={
                    "user-agent": (
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/131.0.0.0 Safari/537.36"
                    ),
                    "accept": "*/*",
                },
            )
            resp.raise_for_status()
            data = resp.content
            if not data:
                raise RuntimeError("empty_response")
            sha256 = hashlib.sha256(data).hexdigest()
            content_type = resp.headers.get("Content-Type", "application/octet-stream")
            ext = guess_ext_from_content_type(content_type)
            key = build_instagram_profile_pic_s3_key(username, sha256, ext)
            s3_client.upload_fileobj(
                io.BytesIO(data),
                bucket,
                key,
                ExtraArgs={
                    "ContentType": content_type.split(";", 1)[0].strip() or "application/octet-stream",
                    "CacheControl": "public, max-age=31536000, immutable",
                },
            )
            hosted_url = build_hosted_url(key)
            url_to_hosted[source_url] = hosted_url
            hosted_map[username] = hosted_url
        except Exception as exc:
            errors.append(f"{username}:{exc.__class__.__name__}")
            logger.debug("Failed to mirror profile pic for %s: %s", username, exc)

    owner_username = owner_detail.username if owner_detail else None
    hosted_owner_pic = hosted_map.get(owner_username) if owner_username else None
    tagged_pics = {u: url for u, url in hosted_map.items() if u != owner_username}

    mirrored = len(hosted_map)
    total = len(pic_urls)
    if mirrored == total:
        status = "mirrored"
    elif mirrored > 0:
        status = "partial"
    else:
        status = "failed"

    return {
        "hosted_owner_profile_pic_url": hosted_owner_pic,
        "hosted_tagged_profile_pics": tagged_pics,
        "profile_pic_mirror_status": status,
        "profile_pic_mirror_error": "; ".join(errors)[:1500] if errors else None,
    }


def _update_instagram_profile_pic_mirror_fields(
    *,
    post_id: str,
    hosted_owner_profile_pic_url: str | None,
    hosted_tagged_profile_pics: dict[str, str] | None,
    profile_pic_mirror_status: str | None,
    profile_pic_mirror_error: str | None,
    conn: Any | None = None,
) -> None:
    """Update profile picture mirror columns on an instagram post."""
    updates: list[str] = []
    params: list[Any] = []

    if _instagram_posts_has_column("hosted_owner_profile_pic_url"):
        updates.append("hosted_owner_profile_pic_url = %s")
        params.append(hosted_owner_profile_pic_url)
    if _instagram_posts_has_column("hosted_tagged_profile_pics") and hosted_tagged_profile_pics is not None:
        updates.append("hosted_tagged_profile_pics = %s")
        params.append(json.dumps(hosted_tagged_profile_pics))
    if _instagram_posts_has_column("profile_pic_mirror_status"):
        updates.append("profile_pic_mirror_status = %s")
        params.append(profile_pic_mirror_status)
    if _instagram_posts_has_column("profile_pic_mirror_error"):
        updates.append("profile_pic_mirror_error = %s")
        params.append(profile_pic_mirror_error)

    if not updates:
        return

    params.append(post_id)
    sql = f"update social.instagram_posts set {', '.join(updates)} where id = %s::uuid"
    with pg.db_cursor(conn=conn) as cur:
        cur.execute(sql, params)


def _count_stored_comments(post_ids: list[str], platform: str) -> dict[str, int]:
    """Return {post_db_id: stored_comment_count} for a batch of posts."""
    if not post_ids:
        return {}
    comment_table_map = {
        "instagram": ("instagram_comments", "post_id"),
        "tiktok": ("tiktok_comments", "post_id"),
        "youtube": ("youtube_comments", "video_id"),
        "facebook": ("facebook_comments", "post_id"),
        "threads": ("meta_threads_comments", "post_id"),
    }
    entry = comment_table_map.get(platform)
    if not entry:
        return {}
    table, fk_col = entry
    placeholders = ",".join(["%s"] * len(post_ids))
    active_filter = " and is_missing = false" if _comment_lifecycle_supported(table) else ""
    rows = pg.fetch_all(
        f"SELECT {fk_col}::text AS pid, count(*)::int AS cnt "
        f"FROM social.{table} WHERE {fk_col} IN ({placeholders}){active_filter} GROUP BY {fk_col}",
        post_ids,
    )
    return {str(r["pid"]): int(r["cnt"]) for r in rows}


def _youtube_effective_comment_count(*, reported_count: int | None, saved_count: int | None) -> int:
    return max(_normalize_non_negative_int(reported_count), _normalize_non_negative_int(saved_count))


def _sync_youtube_video_comment_counts(
    video_db_ids: list[str],
    *,
    conn: Any | None = None,
) -> int:
    """Sync youtube_videos.comments_count to at least the saved comments total."""
    ids = sorted({str(value).strip() for value in video_db_ids if str(value).strip()})
    if not ids:
        return 0

    placeholders = ",".join(["%s"] * len(ids))
    params: list[Any] = [*ids, *ids]
    sql = f"""
        with comment_counts as (
          select
            c.video_id::text as video_id,
            count(*)::int as saved_count
          from social.youtube_comments c
          where c.video_id in ({placeholders})
          group by c.video_id
        ),
        updated as (
          update social.youtube_videos v
          set comments_count = greatest(
            coalesce(v.comments_count, 0),
            coalesce(cc.saved_count, 0)
          )
          from comment_counts cc
          where v.id::text = cc.video_id
            and v.id in ({placeholders})
          returning v.id::text as id
        )
        select id
        from updated
    """
    with pg.db_cursor(conn=conn) as cur:
        rows = pg.fetch_all_with_cursor(cur, sql, params)
    return len(rows)


def backfill_youtube_comment_counts_for_season(
    season_id: str,
    *,
    source_account: str | None = None,
    date_start: datetime | None = None,
    date_end: datetime | None = None,
) -> dict[str, Any]:
    """Backfill youtube video comments_count from saved comments for an optional season scope."""
    conditions = ["v.season_id = %s"]
    params: list[Any] = [season_id]

    normalized_account = str(source_account or "").strip().lower().lstrip("@")
    if normalized_account:
        conditions.append(
            "ltrim(lower(coalesce(nullif(v.source_account, ''), nullif(v.channel_title, ''), '')), '@') = %s"
        )
        params.append(normalized_account)
    if date_start:
        conditions.append("v.published_at >= %s")
        params.append(date_start)
    if date_end:
        conditions.append("v.published_at <= %s")
        params.append(date_end)

    where = " and ".join(conditions)
    rows = pg.fetch_all(
        f"""
        select v.id::text as id
        from social.youtube_videos v
        where {where}
        """,
        params,
    )
    video_ids = [str(row.get("id") or "") for row in rows if str(row.get("id") or "").strip()]
    synced = _sync_youtube_video_comment_counts(video_ids)
    return {
        "season_id": season_id,
        "source_account": normalized_account or None,
        "date_start": _iso(date_start),
        "date_end": _iso(date_end),
        "videos_scanned": len(video_ids),
        "youtube_comment_count_backfilled": synced,
    }


def _youtube_row_matches_owner_identity(
    row: dict[str, Any],
    *,
    expected_handle: str,
    expected_channel_id: str | None,
) -> bool:
    normalized_expected_handle = _normalize_account_handle(expected_handle)
    normalized_expected_channel_id = str(expected_channel_id or "").strip()
    row_channel_id = str(row.get("channel_id") or "").strip()
    if normalized_expected_channel_id and row_channel_id:
        return row_channel_id == normalized_expected_channel_id
    if not normalized_expected_handle:
        return True

    raw_data = row.get("raw_data") if isinstance(row.get("raw_data"), dict) else {}
    candidate_values: list[Any] = [
        row.get("source_account"),
        row.get("channel_title"),
        raw_data.get("channel"),
        raw_data.get("uploader"),
        raw_data.get("uploader_id"),
        raw_data.get("channel_url"),
        raw_data.get("uploader_url"),
        raw_data.get("webpage_url"),
    ]
    owner_candidates = {
        handle for handle in (_extract_youtube_handle_from_value(value) for value in candidate_values) if handle
    }
    if owner_candidates:
        return any(_youtube_handles_match(normalized_expected_handle, handle) for handle in owner_candidates)
    return True


def _cleanup_mismatched_youtube_rows(
    *,
    context: SeasonContext,
    account: str,
    expected_handle: str,
    expected_channel_id: str | None,
) -> dict[str, int]:
    normalized_account = _normalize_account_handle(account)
    if not normalized_account:
        return {"scanned": 0, "videos_deleted": 0, "comments_deleted": 0}

    rows = pg.fetch_all(
        """
        select
          v.id::text as id,
          v.video_id,
          v.channel_id,
          v.channel_title,
          v.source_account,
          coalesce(to_jsonb(v) -> 'raw_data', '{}'::jsonb) as raw_data
        from social.youtube_videos v
        where v.season_id = %s
          and ltrim(lower(coalesce(nullif(v.source_account, ''), nullif(v.channel_title, ''), '')), '@') = %s
        """,
        [context.season_id, normalized_account],
    )
    stale_ids = [
        str(row.get("id") or "")
        for row in rows
        if str(row.get("id") or "")
        and not _youtube_row_matches_owner_identity(
            row,
            expected_handle=expected_handle,
            expected_channel_id=expected_channel_id,
        )
    ]
    if not stale_ids:
        return {"scanned": len(rows), "videos_deleted": 0, "comments_deleted": 0}

    with pg.db_connection() as conn:
        with pg.db_cursor(conn=conn) as cur:
            deleted_comments = pg.fetch_all_with_cursor(
                cur,
                "delete from social.youtube_comments where video_id = any(%s::uuid[]) returning id::text as id",
                [stale_ids],
            )
            deleted_videos = pg.fetch_all_with_cursor(
                cur,
                "delete from social.youtube_videos where id = any(%s::uuid[]) returning id::text as id",
                [stale_ids],
            )
    return {
        "scanned": len(rows),
        "videos_deleted": len(deleted_videos),
        "comments_deleted": len(deleted_comments),
    }


def _count_stored_replies(tweet_ids: list[str]) -> dict[str, int]:
    """Return {tweet_id: stored_reply_count} for Twitter anchor tweets."""
    if not tweet_ids:
        return {}
    placeholders = ",".join(["%s"] * len(tweet_ids))
    active_filter = " and is_missing = false" if _comment_lifecycle_supported("twitter_tweets") else ""
    rows = pg.fetch_all(
        f"SELECT reply_to_tweet_id AS tid, count(*)::int AS cnt "
        f"FROM social.twitter_tweets "
        f"WHERE reply_to_tweet_id IN ({placeholders}) AND is_reply = true{active_filter} "
        f"GROUP BY reply_to_tweet_id",
        tweet_ids,
    )
    return {str(r["tid"]): int(r["cnt"]) for r in rows}


def _count_stored_quotes(tweet_ids: list[str]) -> dict[str, int]:
    """Return {tweet_id: stored_quote_count} for Twitter anchor tweets."""
    if not tweet_ids:
        return {}
    placeholders = ",".join(["%s"] * len(tweet_ids))
    active_filter = " and is_missing = false" if _comment_lifecycle_supported("twitter_tweets") else ""
    rows = pg.fetch_all(
        f"SELECT quoted_tweet_id AS tid, count(*)::int AS cnt "
        f"FROM social.twitter_tweets "
        f"WHERE quoted_tweet_id IN ({placeholders}) AND is_quote = true{active_filter} "
        f"GROUP BY quoted_tweet_id",
        tweet_ids,
    )
    return {str(r["tid"]): int(r["cnt"]) for r in rows}


def _comment_lifecycle_supported(table: str) -> bool:
    try:
        return all(
            _column_exists("social", table, column)
            for column in ("is_missing", "missing_at", "first_seen_at", "last_seen_at", "last_seen_run_id")
        )
    except Exception:
        return False


def _build_comment_snapshot_map(rows: list[dict[str, Any]]) -> dict[str, CommentLifecycleSnapshot]:
    snapshots: dict[str, CommentLifecycleSnapshot] = {}
    for row in rows:
        anchor_id = str(row.get("anchor_id") or "")
        if not anchor_id:
            continue
        snapshots[anchor_id] = CommentLifecycleSnapshot(
            active_count=int(row.get("active_count") or 0),
            total_count=int(row.get("total_count") or 0),
            latest_comment_created_at=_coerce_dt(row.get("latest_comment_created_at")),
            last_seen_at=_coerce_dt(row.get("last_seen_at")),
            last_checked_at=_coerce_dt(row.get("last_checked_at")),
        )
    return snapshots


def _load_comment_lifecycle_snapshots(anchor_ids: list[str], *, platform: str) -> dict[str, CommentLifecycleSnapshot]:
    if not anchor_ids:
        return {}

    placeholders = ",".join(["%s"] * len(anchor_ids))
    normalized_platform = (platform or "").strip().lower()

    if normalized_platform == "twitter":
        lifecycle_supported = _comment_lifecycle_supported("twitter_tweets")
        if lifecycle_supported:
            rows = pg.fetch_all(
                f"""
                select
                  reply_to_tweet_id as anchor_id,
                  count(*) filter (where is_missing = false)::int as active_count,
                  count(*)::int as total_count,
                  max(created_at) filter (where is_missing = false) as latest_comment_created_at,
                  max(last_seen_at) filter (where is_missing = false) as last_seen_at,
                  coalesce(max(last_seen_at), max(scraped_at)) as last_checked_at
                from social.twitter_tweets
                where is_reply = true
                  and reply_to_tweet_id in ({placeholders})
                group by reply_to_tweet_id
                """,
                anchor_ids,
            )
        else:
            rows = pg.fetch_all(
                f"""
                select
                  reply_to_tweet_id as anchor_id,
                  count(*)::int as active_count,
                  count(*)::int as total_count,
                  max(created_at) as latest_comment_created_at,
                  null::timestamptz as last_seen_at,
                  max(scraped_at) as last_checked_at
                from social.twitter_tweets
                where is_reply = true
                  and reply_to_tweet_id in ({placeholders})
                group by reply_to_tweet_id
                """,
                anchor_ids,
            )
        return _build_comment_snapshot_map(rows)

    table_map = {
        "instagram": ("instagram_comments", "post_id", "comment_id"),
        "tiktok": ("tiktok_comments", "post_id", "comment_id"),
        "youtube": ("youtube_comments", "video_id", "comment_id"),
        "facebook": ("facebook_comments", "post_id", "comment_id"),
        "threads": ("meta_threads_comments", "post_id", "comment_id"),
    }
    entry = table_map.get(normalized_platform)
    if not entry:
        return {}

    table, anchor_col, _external_id_col = entry
    lifecycle_supported = _comment_lifecycle_supported(table)
    if lifecycle_supported:
        rows = pg.fetch_all(
            f"""
            select
              {anchor_col}::text as anchor_id,
              count(*) filter (where is_missing = false)::int as active_count,
              count(*)::int as total_count,
              max(created_at) filter (where is_missing = false) as latest_comment_created_at,
              max(last_seen_at) filter (where is_missing = false) as last_seen_at,
              coalesce(max(last_seen_at), max(scraped_at)) as last_checked_at
            from social.{table}
            where {anchor_col} in ({placeholders})
            group by {anchor_col}
            """,
            anchor_ids,
        )
    else:
        rows = pg.fetch_all(
            f"""
            select
              {anchor_col}::text as anchor_id,
              count(*)::int as active_count,
              count(*)::int as total_count,
              max(created_at) as latest_comment_created_at,
              null::timestamptz as last_seen_at,
              max(scraped_at) as last_checked_at
            from social.{table}
            where {anchor_col} in ({placeholders})
            group by {anchor_col}
            """,
            anchor_ids,
        )
    return _build_comment_snapshot_map(rows)


def _decide_comment_refresh(
    *,
    sync_strategy: str,
    expected_count: int,
    snapshot: CommentLifecycleSnapshot | None,
    post_published_at: datetime | None,
    refresh_policy: str = DEFAULT_COMMENT_REFRESH_POLICY,
    now: datetime | None = None,
) -> CommentRefreshDecision:
    if sync_strategy == "full_refresh":
        return CommentRefreshDecision(should_refresh=True, reason="full_refresh")

    normalized_policy = _normalize_comment_refresh_policy(refresh_policy)
    now_utc = now or _now_utc()
    if snapshot is None:
        if normalized_policy == "missing_only":
            if expected_count > 0:
                return CommentRefreshDecision(should_refresh=True, reason="count_gap")
            return CommentRefreshDecision(should_refresh=False, reason="up_to_date")
        return CommentRefreshDecision(should_refresh=True, reason="never_checked")

    active_stored = int(snapshot.active_count) if snapshot else 0
    if expected_count > active_stored:
        return CommentRefreshDecision(should_refresh=True, reason="count_gap")
    if expected_count < active_stored:
        if normalized_policy == "missing_only":
            return CommentRefreshDecision(should_refresh=False, reason="count_drop")
        return CommentRefreshDecision(should_refresh=True, reason="count_drop")

    if normalized_policy == "missing_only":
        return CommentRefreshDecision(should_refresh=False, reason="up_to_date")

    quiet_anchor = snapshot.latest_comment_created_at or post_published_at
    if quiet_anchor:
        quiet_age = now_utc - (quiet_anchor if quiet_anchor.tzinfo else quiet_anchor.replace(tzinfo=UTC))
        if quiet_age >= QUIET_POST_FORCE_RECHECK_AGE:
            return CommentRefreshDecision(should_refresh=True, reason="quiet_post_force_recheck")

    if not snapshot.last_checked_at:
        return CommentRefreshDecision(should_refresh=True, reason="never_checked")

    checked_at = snapshot.last_checked_at
    if not checked_at.tzinfo:
        checked_at = checked_at.replace(tzinfo=UTC)
    if (now_utc - checked_at) >= COMMENT_STALE_RECHECK_INTERVAL:
        return CommentRefreshDecision(should_refresh=True, reason="stale_recheck")

    return CommentRefreshDecision(should_refresh=False, reason="up_to_date")


def _is_comment_fetch_complete(
    *,
    fetch_failed: bool,
    fail_reason: str | None,
    auth_failed: bool = False,
    fetched_count: int,
    max_comments_per_post: int,
    expected_count: int | None = None,
) -> bool:
    if fetch_failed:
        return False
    if auth_failed:
        return False
    if fail_reason:
        return False
    expected = max(0, int(expected_count or 0))
    # Guard against false-complete responses (common on protected threads):
    # when upstream reports interactions but fetch returns none, keep job incomplete.
    if expected > 0 and fetched_count <= 0:
        return False
    # Conservative guard: if we hit local cap, we cannot guarantee full coverage.
    if max_comments_per_post > 0 and fetched_count >= max_comments_per_post:
        return False
    return True


def _effective_reply_limit(opts: IngestOptions) -> int:
    if opts.max_replies_per_post is None:
        return max(0, int(opts.max_comments_per_post or 0))
    return max(0, int(opts.max_replies_per_post))


def _trim_nested_comment_replies(comments: list[Any], *, max_replies_per_post: int) -> None:
    limit = max(0, int(max_replies_per_post))
    if limit <= 0:
        for comment in comments:
            try:
                comment.replies = []
            except Exception:
                pass
        return

    for comment in comments:
        replies = list(getattr(comment, "replies", []) or [])
        if len(replies) > limit:
            replies = replies[:limit]
        try:
            comment.replies = replies
        except Exception:
            pass


def _mark_missing_comments_for_anchor(
    *,
    platform: str,
    anchor_id: str,
    observed_comment_ids: set[str],
    conn: Any | None = None,
) -> int:
    normalized_platform = (platform or "").strip().lower()
    if normalized_platform == "twitter":
        if not _comment_lifecycle_supported("twitter_tweets"):
            return 0
        base_sql = """
            update social.twitter_tweets
            set
              is_missing = true,
              missing_at = coalesce(missing_at, now())
            where is_reply = true
              and reply_to_tweet_id = %s
              and is_missing = false
        """
        params: list[Any] = [anchor_id]
        if observed_comment_ids:
            placeholders = ",".join(["%s"] * len(observed_comment_ids))
            base_sql += f" and tweet_id not in ({placeholders})"
            params.extend(sorted(observed_comment_ids))
        base_sql += " returning id::text"
        with pg.db_cursor(conn=conn) as cur:
            rows = pg.fetch_all_with_cursor(cur, base_sql, params)
        return len(rows)

    table_map = {
        "instagram": ("instagram_comments", "post_id", "comment_id"),
        "tiktok": ("tiktok_comments", "post_id", "comment_id"),
        "youtube": ("youtube_comments", "video_id", "comment_id"),
        "facebook": ("facebook_comments", "post_id", "comment_id"),
        "threads": ("meta_threads_comments", "post_id", "comment_id"),
    }
    entry = table_map.get(normalized_platform)
    if not entry:
        return 0

    table, anchor_col, external_id_col = entry
    if not _comment_lifecycle_supported(table):
        return 0

    sql = f"""
        update social.{table}
        set
          is_missing = true,
          missing_at = coalesce(missing_at, now())
        where {anchor_col} = %s
          and is_missing = false
    """
    params = [anchor_id]
    if observed_comment_ids:
        placeholders = ",".join(["%s"] * len(observed_comment_ids))
        sql += f" and {external_id_col} not in ({placeholders})"
        params.extend(sorted(observed_comment_ids))
    sql += " returning id::text"
    with pg.db_cursor(conn=conn) as cur:
        rows = pg.fetch_all_with_cursor(cur, sql, params)
    return len(rows)


def _reconcile_post_comment_count(
    *,
    platform: str,
    post_db_id: str,
    conn: Any | None = None,
) -> int | None:
    """
    After a complete comment fetch, reconcile the post's comments_count
    with the actual number of active (non-missing) comments in the DB.

    Instagram (and other platforms) often over-report comments_count
    (deleted/hidden comments are counted). This causes coverage to never
    reach 100%. Reconciling ensures reported == saved when all fetchable
    comments have been saved.
    """
    normalized_platform = (platform or "").strip().lower()

    if normalized_platform == "twitter":
        # For Twitter, post_db_id is the external tweet_id (not a DB UUID),
        # matching the convention used by _mark_missing_comments_for_anchor.
        comment_table = "twitter_tweets"
        post_table = "twitter_tweets"
        if not _comment_lifecycle_supported(comment_table):
            active_filter = ""
        else:
            active_filter = "and t.is_missing = false"

        with pg.db_cursor(conn=conn) as cur:
            reply_row = pg.fetch_one_with_cursor(
                cur,
                f"""
                select count(*)::int as active_count
                from social.{comment_table} t
                where t.reply_to_tweet_id = %s
                  and t.is_reply = true
                  {active_filter}
                """,
                [post_db_id],
            )
            quote_row = pg.fetch_one_with_cursor(
                cur,
                f"""
                select count(*)::int as active_count
                from social.{comment_table} t
                where t.quoted_tweet_id = %s
                  and t.is_quote = true
                  {active_filter}
                """,
                [post_db_id],
            )
        active_replies = int((reply_row or {}).get("active_count") or 0)
        active_quotes = int((quote_row or {}).get("active_count") or 0)

        with pg.db_cursor(conn=conn) as cur:
            current_row = pg.fetch_one_with_cursor(
                cur,
                f"""
                select coalesce(replies_count, 0) as replies_count,
                       coalesce(quotes, 0) as quotes
                from social.{post_table}
                where tweet_id = %s and is_reply = false
                """,
                [post_db_id],
            )
        current_replies = int((current_row or {}).get("replies_count") or 0)
        current_quotes = int((current_row or {}).get("quotes") or 0)
        target_replies = max(0, active_replies)
        target_quotes = max(0, active_quotes)

        if target_replies != current_replies or target_quotes != current_quotes:
            with pg.db_cursor(conn=conn) as cur:
                pg.fetch_one_with_cursor(
                    cur,
                    f"update social.{post_table} set replies_count = %s, quotes = %s"
                    f" where tweet_id = %s and is_reply = false returning id::text",
                    [target_replies, target_quotes, post_db_id],
                )
            logger.info(
                "[%s] Reconciled reply/quote counts for tweet %s: replies %d -> %d, quotes %d -> %d",
                normalized_platform,
                post_db_id,
                current_replies,
                target_replies,
                current_quotes,
                target_quotes,
            )
            return target_replies + target_quotes
        return current_replies + current_quotes

    table_map = {
        "instagram": ("instagram_posts", "instagram_comments", "post_id"),
        "tiktok": ("tiktok_posts", "tiktok_comments", "post_id"),
        "youtube": ("youtube_videos", "youtube_comments", "video_id"),
        "facebook": ("facebook_posts", "facebook_comments", "post_id"),
        "threads": ("meta_threads_posts", "meta_threads_comments", "post_id"),
    }
    entry = table_map.get(normalized_platform)
    if not entry:
        return None

    post_table, comment_table, anchor_col = entry
    comments_col = "comments_count"

    active_filter = "and c.is_missing = false" if _comment_lifecycle_supported(comment_table) else ""
    with pg.db_cursor(conn=conn) as cur:
        count_row = pg.fetch_one_with_cursor(
            cur,
            f"""
            select count(*)::int as active_count
            from social.{comment_table} c
            where c.{anchor_col} = %s::uuid
              {active_filter}
            """,
            [post_db_id],
        )
    active_count = int((count_row or {}).get("active_count") or 0)

    with pg.db_cursor(conn=conn) as cur:
        current_row = pg.fetch_one_with_cursor(
            cur,
            f"select {comments_col} from social.{post_table} where id = %s::uuid",
            [post_db_id],
        )
    current_count = int((current_row or {}).get(comments_col) or 0)

    # Only reconcile downward: if the actual count is less than reported,
    # update to the actual count. This prevents the "forever stale" state.
    if active_count < current_count:
        with pg.db_cursor(conn=conn) as cur:
            pg.fetch_one_with_cursor(
                cur,
                f"update social.{post_table} set {comments_col} = %s where id = %s::uuid returning id::text",
                [active_count, post_db_id],
            )
        logger.info(
            "[%s] Reconciled comments_count for post %s: %d -> %d",
            normalized_platform,
            post_db_id,
            current_count,
            active_count,
        )
        return active_count
    return current_count


def _apply_twitter_public_summary(
    *,
    tweet_id: str,
    summary: dict[str, Any] | None,
    conn: Any | None = None,
) -> bool:
    if not summary:
        return False
    normalized_id = str(tweet_id or "").strip()
    if not normalized_id:
        return False

    username = str(summary.get("username") or "").strip()
    display_name = str(summary.get("display_name") or "").strip()
    user_id = str(summary.get("user_id") or "").strip()
    user_profile_url = str(summary.get("user_profile_url") or "").strip()
    user_avatar_url = str(summary.get("user_avatar_url") or "").strip()
    replies_count = _normalize_non_negative_int(summary.get("replies"))
    likes = _normalize_non_negative_int(summary.get("likes"))
    media_urls = summary.get("media_urls") if isinstance(summary.get("media_urls"), list) else []

    updates: list[str] = []
    params: list[Any] = []
    if replies_count > 0:
        updates.append("replies_count = greatest(coalesce(replies_count, 0), %s)")
        params.append(replies_count)
    if likes > 0:
        updates.append("likes = greatest(coalesce(likes, 0), %s)")
        params.append(likes)
    if username:
        updates.append("username = coalesce(nullif(username, ''), %s)")
        params.append(username)
    if display_name:
        updates.append("display_name = coalesce(nullif(display_name, ''), %s)")
        params.append(display_name)
    if _platform_posts_has_column("twitter", "user_id") and user_id:
        updates.append("user_id = coalesce(nullif(user_id, ''), %s)")
        params.append(user_id)
    if _platform_posts_has_column("twitter", "user_profile_url"):
        resolved_profile_url = user_profile_url or (f"https://x.com/{username}" if username else "")
        if resolved_profile_url:
            updates.append("user_profile_url = coalesce(nullif(user_profile_url, ''), %s)")
            params.append(resolved_profile_url)
    if _platform_posts_has_column("twitter", "user_avatar_url") and user_avatar_url:
        updates.append("user_avatar_url = coalesce(nullif(user_avatar_url, ''), %s)")
        params.append(user_avatar_url)
    if _platform_posts_has_column("twitter", "media_urls") and media_urls:
        updates.append(
            "media_urls = case "
            "when media_urls is null or jsonb_typeof(media_urls) <> 'array' or jsonb_array_length(media_urls) = 0 "
            "then %s::jsonb else media_urls end"
        )
        params.append(json.dumps(media_urls))

    if not updates:
        return False

    sql = (
        "update social.twitter_tweets set "
        + ", ".join(updates)
        + " where tweet_id = %s and is_reply = false returning id::text"
    )
    params.append(normalized_id)
    with pg.db_cursor(conn=conn) as cur:
        row = pg.fetch_one_with_cursor(cur, sql, params)
    return bool(row)


def _load_existing_posts(
    platform: str,
    context: SeasonContext,
    account: str,
    date_start: datetime | None,
    date_end: datetime | None,
    source_ids: set[str] | None = None,
) -> list[dict[str, Any]]:
    """Load existing posts from the DB for the comment-only stage."""
    table_map = {
        "instagram": (
            "instagram_posts",
            "posted_at",
            "ltrim(lower(coalesce(nullif(source_account, ''), nullif(username, ''), '')), '@')",
            "shortcode",
        ),
        "tiktok": (
            "tiktok_posts",
            "posted_at",
            "ltrim(lower(coalesce(nullif(source_account, ''), nullif(username, ''), '')), '@')",
            "video_id",
        ),
        "youtube": (
            "youtube_videos",
            "published_at",
            "ltrim(lower(coalesce(nullif(source_account, ''), nullif(channel_title, ''), '')), '@')",
            "video_id",
        ),
        "twitter": (
            "twitter_tweets",
            "created_at",
            "ltrim(lower(coalesce(nullif(source_account, ''), nullif(username, ''), '')), '@')",
            "tweet_id",
        ),
        "facebook": (
            "facebook_posts",
            "posted_at",
            "ltrim(lower(coalesce(nullif(source_account, ''), nullif(username, ''), '')), '@')",
            "post_id",
        ),
        "threads": (
            "meta_threads_posts",
            "posted_at",
            "ltrim(lower(coalesce(nullif(source_account, ''), nullif(username, ''), '')), '@')",
            "post_id",
        ),
    }
    entry = table_map.get(platform)
    if not entry:
        return []
    table, ts_col, account_expr, source_id_col = entry

    normalized_account = str(account or "").strip().lower().lstrip("@")
    conditions = ["season_id = %s", f"{account_expr} = %s"]
    params: list[Any] = [context.season_id, normalized_account]
    if date_start:
        conditions.append(f"{ts_col} >= %s")
        params.append(date_start)
    if date_end:
        conditions.append(f"{ts_col} <= %s")
        params.append(date_end)
    if source_ids is not None:
        normalized_source_ids = sorted({str(item or "").strip() for item in source_ids if str(item or "").strip()})
        if not normalized_source_ids:
            return []
        conditions.append(f"{source_id_col} = any(%s)")
        params.append(normalized_source_ids)

    where = " AND ".join(conditions)
    sql = f"SELECT * FROM social.{table} WHERE {where} ORDER BY {ts_col} ASC"
    return pg.fetch_all(sql, params)


def _post_identity_from_row(platform: str, row: dict[str, Any]) -> str:
    normalized_platform = (platform or "").strip().lower()
    if normalized_platform == "instagram":
        return str(row.get("shortcode") or "")
    if normalized_platform == "tiktok":
        return str(row.get("video_id") or "")
    if normalized_platform == "youtube":
        return str(row.get("video_id") or "")
    if normalized_platform == "twitter":
        return str(row.get("tweet_id") or "")
    if normalized_platform in {"facebook", "threads"}:
        return str(row.get("post_id") or "")
    return ""


def _build_existing_post_lookup(platform: str, rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    normalized_platform = (platform or "").strip().lower()
    lookup: dict[str, dict[str, Any]] = {}
    for row in rows:
        if normalized_platform == "twitter" and bool(row.get("is_reply")):
            continue
        identity = _post_identity_from_row(normalized_platform, row)
        if identity:
            lookup[identity] = row
    return lookup


def _expected_comment_count_for_platform(
    platform: str,
    row_or_post: dict[str, Any],
    *,
    snapshot: CommentLifecycleSnapshot | None = None,
) -> int:
    normalized_platform = (platform or "").strip().lower()
    if normalized_platform == "twitter":
        replies = _normalize_non_negative_int(row_or_post.get("replies_count"))
        quotes = _normalize_non_negative_int(row_or_post.get("quotes"))
        return replies + quotes
    if normalized_platform == "threads":
        return _normalize_non_negative_int(row_or_post.get("replies_count"))
    expected = _normalize_non_negative_int(row_or_post.get("comments_count"))
    if normalized_platform == "youtube" and expected <= 0 and snapshot is not None:
        return _normalize_non_negative_int(snapshot.active_count)
    return expected


def _pg_upsert(
    table: str,
    payload: dict[str, Any],
    *,
    conflict_col: str,
    conn: Any | None = None,
) -> dict[str, Any] | None:
    """Upsert a row into social.{table} using direct SQL (psycopg2).

    This avoids Supabase PostgREST schema-cache (PGRST002) errors that
    occur intermittently with the ``social`` schema.
    """
    from psycopg2.extras import Json as PgJson

    adapted: dict[str, Any] = {}
    for key, value in payload.items():
        if isinstance(value, (dict, list)):
            adapted[key] = PgJson(value)
        else:
            adapted[key] = value

    cols = list(adapted.keys())
    col_list = ", ".join(cols)
    placeholders = ", ".join(["%s"] * len(cols))
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c != conflict_col)

    sql = f"""
        INSERT INTO social.{table} ({col_list})
        VALUES ({placeholders})
        ON CONFLICT ({conflict_col}) DO UPDATE SET {updates}
        RETURNING *
    """
    with pg.db_cursor(conn=conn) as cur:
        return pg.fetch_one_with_cursor(cur, sql, list(adapted.values()))


def _new_comment_persist_stats() -> dict[str, int]:
    return {"comments_fetched": 0, "comments_upserted": 0, "comments_skipped_missing_id": 0}


def _comment_stats_payload(stats: dict[str, int]) -> dict[str, int]:
    return {
        "comments_fetched": int(stats.get("comments_fetched") or 0),
        "comments_upserted": int(stats.get("comments_upserted") or 0),
        "comments_skipped_missing_id": int(stats.get("comments_skipped_missing_id") or 0),
    }


def _merge_comment_persist_stats(target: dict[str, int], delta: dict[str, int]) -> None:
    for key in ("comments_fetched", "comments_upserted", "comments_skipped_missing_id"):
        target[key] = int(target.get(key) or 0) + int(delta.get(key) or 0)


def _savepoint_name(*, prefix: str, index: int) -> str:
    safe_prefix = re.sub(r"[^a-zA-Z0-9_]", "_", str(prefix or "sp")).strip("_") or "sp"
    return f"{safe_prefix}_{max(1, int(index))}"


def _create_savepoint(conn: Any, name: str) -> None:
    with pg.db_cursor(conn=conn) as cur:
        cur.execute(f"SAVEPOINT {name}")


def _rollback_to_savepoint(conn: Any, name: str) -> None:
    with pg.db_cursor(conn=conn) as cur:
        cur.execute(f"ROLLBACK TO SAVEPOINT {name}")


def _release_savepoint(conn: Any, name: str) -> None:
    with pg.db_cursor(conn=conn) as cur:
        cur.execute(f"RELEASE SAVEPOINT {name}")


def _upsert_instagram_post(
    context: SeasonContext,
    *,
    job_id: str,
    account: str,
    post: Any,
    conn: Any | None = None,
) -> dict[str, Any] | None:
    posted_at = _parse_instagram_time(getattr(post, "taken_at", None))
    media_urls = [str(url).strip() for url in (getattr(post, "media_urls", []) or []) if str(url).strip()]
    thumbnail_url = str(getattr(post, "thumbnail_url", "") or "").strip() or (media_urls[0] if media_urls else None)
    profile_tags = _as_text_list(getattr(post, "profile_tags", []))
    collaborators = _as_text_list(getattr(post, "collaborators", []))
    hashtags = _as_text_list(getattr(post, "hashtags", []), strip_prefix="#")
    mentions = _as_text_list(getattr(post, "mentions", []), prefix="@", strip_prefix="@")
    hosted_media_urls = [str(url).strip() for url in (getattr(post, "hosted_media_urls", []) or []) if str(url).strip()]
    hosted_thumbnail_url = str(getattr(post, "hosted_thumbnail_url", "") or "").strip() or None
    media_mirror_status = getattr(post, "media_mirror_status", None)
    media_mirror_error = getattr(post, "media_mirror_error", None)
    media_mirror_attempt_count = getattr(post, "media_mirror_attempt_count", None)
    media_mirror_last_attempt_at = _coerce_dt(getattr(post, "media_mirror_last_attempt_at", None))
    media_mirror_last_job_id = str(getattr(post, "media_mirror_last_job_id", "") or "").strip() or None
    metadata_scraped_at = _coerce_dt(getattr(post, "metadata_scraped_at", None))
    duration_seconds = getattr(post, "duration_seconds", None)
    try:
        duration_seconds = int(duration_seconds) if duration_seconds is not None else None
    except (TypeError, ValueError):
        duration_seconds = None

    raw_data = post.to_dict() if hasattr(post, "to_dict") else {}
    media_retrieval_meta = getattr(post, "media_retrieval_meta", None)
    if isinstance(media_retrieval_meta, dict):
        raw_data = dict(raw_data or {})
        raw_data["media_retrieval_meta"] = {
            "selected_source": str(media_retrieval_meta.get("selected_source") or "") or None,
            "attempts": _normalize_instagram_mirror_attempts(media_retrieval_meta.get("attempts")),
        }

    payload = {
        "shortcode": getattr(post, "shortcode", ""),
        "media_id": getattr(post, "pk", None),
        "username": getattr(post, "username", account),
        "user_id": None,
        "caption": getattr(post, "caption", None),
        "media_type": getattr(post, "post_type", None),
        "media_urls": media_urls,
        "thumbnail_url": thumbnail_url,
        "likes": int(getattr(post, "likes", 0) or 0),
        "comments_count": int(getattr(post, "comments", 0) or 0),
        "views": int(getattr(post, "video_views", 0) or 0),
        "posted_at": posted_at,
        "scraped_at": _now_utc(),
        "raw_data": raw_data,
        "show_id": context.show_id,
        "season_id": context.season_id,
        "job_id": job_id,
        "source_account": account,
    }

    # Serialize rich user detail objects
    tagged_users_detail_raw = getattr(post, "tagged_users_detail", []) or []
    tagged_users_detail = [u.to_dict() if hasattr(u, "to_dict") else u for u in tagged_users_detail_raw]
    collaborators_detail_raw = getattr(post, "collaborators_detail", []) or []
    collaborators_detail = [u.to_dict() if hasattr(u, "to_dict") else u for u in collaborators_detail_raw]
    owner_detail = getattr(post, "owner_detail", None)
    child_posts_data = getattr(post, "child_posts_data", []) or []

    # Coerce video_duration
    video_duration_raw = getattr(post, "video_duration", None)
    try:
        video_duration = float(video_duration_raw) if video_duration_raw is not None else None
    except (TypeError, ValueError):
        video_duration = None

    optional_payload = {
        "post_format": getattr(post, "post_format", None),
        "profile_tags": profile_tags,
        "collaborators": collaborators,
        "hashtags": hashtags,
        "mentions": mentions,
        "duration_seconds": duration_seconds,
        "metadata_source": getattr(post, "metadata_source", None),
        "metadata_scraped_at": metadata_scraped_at,
        "metadata_error": getattr(post, "metadata_error", None),
        # Enhanced metadata (migration 0147)
        "tagged_users_detail": tagged_users_detail,
        "collaborators_detail": collaborators_detail,
        "owner_profile_pic_url": (
            owner_detail.profile_pic_url
            if owner_detail and hasattr(owner_detail, "profile_pic_url")
            else getattr(post, "owner_profile_pic_url", None)
        ),
        "owner_full_name": (
            owner_detail.full_name
            if owner_detail and hasattr(owner_detail, "full_name")
            else getattr(post, "owner_full_name", None)
        ),
        "owner_is_verified": (
            owner_detail.is_verified
            if owner_detail and hasattr(owner_detail, "is_verified")
            else getattr(post, "owner_is_verified", None)
        ),
        "product_type": getattr(post, "product_type", None),
        "video_play_count": getattr(post, "video_play_count", None),
        "alt_text": getattr(post, "alt_text", None),
        "width": getattr(post, "width", None),
        "height": getattr(post, "height", None),
        "is_comments_disabled": getattr(post, "is_comments_disabled", None),
        "music_info": getattr(post, "music_info", None),
        "video_duration": video_duration,
        "child_posts_data": child_posts_data if child_posts_data else [],
    }
    for key, value in optional_payload.items():
        if _instagram_posts_has_column(key):
            payload[key] = value

    # Preserve hosted URLs unless new hosted values are explicitly supplied.
    if hosted_thumbnail_url and _instagram_posts_has_column("hosted_thumbnail_url"):
        payload["hosted_thumbnail_url"] = hosted_thumbnail_url
    if hosted_media_urls and _instagram_posts_has_column("hosted_media_urls"):
        payload["hosted_media_urls"] = hosted_media_urls
    if _instagram_posts_has_column("media_mirror_status"):
        payload["media_mirror_status"] = media_mirror_status
    if _instagram_posts_has_column("media_mirror_error"):
        payload["media_mirror_error"] = media_mirror_error
    if media_mirror_attempt_count is not None and _instagram_posts_has_column("media_mirror_attempt_count"):
        payload["media_mirror_attempt_count"] = max(0, int(media_mirror_attempt_count))
    if media_mirror_last_attempt_at is not None and _instagram_posts_has_column("media_mirror_last_attempt_at"):
        payload["media_mirror_last_attempt_at"] = media_mirror_last_attempt_at
    if media_mirror_last_job_id and _instagram_posts_has_column("media_mirror_last_job_id"):
        payload["media_mirror_last_job_id"] = media_mirror_last_job_id

    # Profile picture mirror fields (migration 0147)
    hosted_owner_pic = getattr(post, "hosted_owner_profile_pic_url", None)
    if hosted_owner_pic and _instagram_posts_has_column("hosted_owner_profile_pic_url"):
        payload["hosted_owner_profile_pic_url"] = hosted_owner_pic
    hosted_tagged_pics = getattr(post, "hosted_tagged_profile_pics", None)
    if hosted_tagged_pics and _instagram_posts_has_column("hosted_tagged_profile_pics"):
        payload["hosted_tagged_profile_pics"] = hosted_tagged_pics
    if _instagram_posts_has_column("profile_pic_mirror_status"):
        payload["profile_pic_mirror_status"] = getattr(post, "profile_pic_mirror_status", None)
    if _instagram_posts_has_column("profile_pic_mirror_error"):
        payload["profile_pic_mirror_error"] = getattr(post, "profile_pic_mirror_error", None)

    return _pg_upsert("instagram_posts", payload, conflict_col="shortcode", conn=conn)


def _instagram_post_source_urls(post_row: dict[str, Any]) -> tuple[str, list[str]]:
    source_media_urls = _normalize_unique_terms(_as_text_list(post_row.get("media_urls")))
    source_thumbnail_url = str(post_row.get("thumbnail_url") or "").strip() or (
        source_media_urls[0] if source_media_urls else ""
    )
    return source_thumbnail_url, source_media_urls


def _platform_post_source_urls(platform: str, post_row: dict[str, Any]) -> tuple[str, list[str]]:
    normalized_platform = (platform or "").strip().lower()
    if normalized_platform == "instagram":
        return _instagram_post_source_urls(post_row)

    source_media_urls = _normalize_unique_terms(_as_text_list(post_row.get("media_urls")))
    source_thumbnail_url = str(post_row.get("thumbnail_url") or "").strip()
    if not source_thumbnail_url and source_media_urls:
        # For Twitter (and other platforms without a dedicated thumbnail_url column),
        # prefer a non-video URL as the thumbnail/cover image.
        source_thumbnail_url = _select_thumbnail_candidate(source_media_urls)
    return source_thumbnail_url, source_media_urls


def _platform_source_id(platform: str, post_row: dict[str, Any]) -> str:
    normalized_platform = (platform or "").strip().lower()
    source_col = PLATFORM_SOURCE_ID_COLUMN.get(normalized_platform)
    if not source_col:
        return ""
    return str(post_row.get(source_col) or post_row.get("source_id") or "").strip()


@lru_cache(maxsize=1)
def _expected_cdn_host() -> str | None:
    raw = str(os.getenv("AWS_CDN_BASE_URL") or "").strip()
    if not raw:
        return None
    parsed = urlparse(raw)
    host = (parsed.netloc or "").strip().lower()
    if not host:
        return None
    return host


def _url_host(value: str) -> str:
    parsed = urlparse(str(value or "").strip())
    return (parsed.netloc or "").strip().lower()


def _hosted_urls_need_cdn_host_repair(*, hosted_thumbnail_url: str, hosted_media_urls: list[str]) -> bool:
    expected_host = _expected_cdn_host()
    if not expected_host:
        return False

    candidates: list[str] = []
    if hosted_thumbnail_url:
        candidates.append(hosted_thumbnail_url)
    candidates.extend([str(url).strip() for url in (hosted_media_urls or []) if str(url).strip()])

    for url in candidates:
        host = _url_host(url)
        if not host:
            # Malformed URL should be remirrored to restore a canonical hosted URL.
            return True
        if host != expected_host:
            return True
    return False


def _hosted_media_urls_need_content_repair(*, hosted_media_urls: list[str]) -> bool:
    for url in hosted_media_urls or []:
        if _is_page_like_media_url(url):
            # HTML/page wrappers are not valid canonical hosted media assets.
            return True
    return False


def _source_media_urls_need_quality_repair(*, platform: str, source_media_urls: list[str]) -> bool:
    normalized_platform = (platform or "").strip().lower()
    urls = [str(url).strip() for url in (source_media_urls or []) if str(url).strip()]
    if not urls:
        return False
    if any(_is_page_like_media_url(url) for url in urls):
        return True
    if normalized_platform in {"tiktok", "youtube"} and not any(_is_video_like_media_url(url) for url in urls):
        return True
    return False


def _source_media_urls_support_ytdlp_mirror(*, platform: str, source_media_urls: list[str]) -> bool:
    normalized_platform = (platform or "").strip().lower()
    if normalized_platform not in {"tiktok", "youtube"}:
        return False
    urls = [str(url).strip() for url in (source_media_urls or []) if str(url).strip()]
    return bool(urls) and all(_is_page_like_media_url(url) for url in urls)


def _platform_post_needs_media_mirror(platform: str, post_row: dict[str, Any]) -> bool:
    normalized_platform = (platform or "").strip().lower()
    hosted_thumbnail_url = str(post_row.get("hosted_thumbnail_url") or "").strip()
    hosted_media_urls = _as_text_list(post_row.get("hosted_media_urls"))
    if _hosted_urls_need_cdn_host_repair(
        hosted_thumbnail_url=hosted_thumbnail_url,
        hosted_media_urls=hosted_media_urls,
    ):
        return True
    if _hosted_media_urls_need_content_repair(hosted_media_urls=hosted_media_urls):
        return True

    source_thumbnail_url, source_media_urls = _platform_post_source_urls(platform, post_row)
    source_id = _platform_source_id(platform, post_row)
    if normalized_platform == "youtube":
        if not source_id:
            return False
    elif not source_thumbnail_url and not source_media_urls:
        return False

    if normalized_platform in {"tiktok", "youtube"} and not hosted_media_urls:
        return True
    if (
        normalized_platform in {"tiktok", "youtube"}
        and hosted_media_urls
        and not any(_is_video_like_media_url(url) for url in hosted_media_urls)
    ):
        return True
    if source_thumbnail_url and not hosted_thumbnail_url:
        return True
    if source_media_urls and len(hosted_media_urls) < len(source_media_urls):
        return True
    return False


def _instagram_post_needs_media_mirror(post_row: dict[str, Any]) -> bool:
    return _platform_post_needs_media_mirror("instagram", post_row)


def _platform_comment_media_needs_mirror(platform: str, comment_row: dict[str, Any]) -> bool:
    normalized_platform = (platform or "").strip().lower()
    if normalized_platform == "twitter":
        return False
    source_media_urls = _as_text_list(comment_row.get("media_urls"))
    if not source_media_urls:
        return False
    hosted_media_urls = _as_text_list(comment_row.get("hosted_media_urls"))
    mirror_status = str(comment_row.get("media_mirror_status") or "").strip().lower()
    if mirror_status in {"pending", "partial", "failed"}:
        return True
    if hosted_media_urls and len(hosted_media_urls) >= len(source_media_urls):
        return False
    return True


def _tiktok_comment_needs_media_mirror(comment_row: dict[str, Any]) -> bool:
    return _platform_comment_media_needs_mirror("tiktok", comment_row)


def _update_platform_post_media_mirror_fields(
    *,
    platform: str,
    post_id: str,
    hosted_thumbnail_url: str | None | object = FIELD_UNSET,
    hosted_media_urls: list[str] | object = FIELD_UNSET,
    media_mirror_status: str | None | object = FIELD_UNSET,
    media_mirror_error: str | None | object = FIELD_UNSET,
    media_mirror_attempt_count: int | object = FIELD_UNSET,
    media_mirror_last_attempt_at: datetime | None | object = FIELD_UNSET,
    media_mirror_last_job_id: str | None | object = FIELD_UNSET,
    conn: Any | None = None,
) -> None:
    normalized_platform = (platform or "").strip().lower()
    table = PLATFORM_POST_TABLES.get(normalized_platform)
    if not table:
        return

    assignments: list[str] = []
    params: list[Any] = []

    def _add(column: str, value: Any, *, as_jsonb: bool = False) -> None:
        if as_jsonb:
            assignments.append(f"{column} = %s::jsonb")
            params.append(json.dumps(value))
            return
        assignments.append(f"{column} = %s")
        params.append(value)

    if hosted_thumbnail_url is not FIELD_UNSET and _platform_posts_has_column(
        normalized_platform, "hosted_thumbnail_url"
    ):
        _add("hosted_thumbnail_url", hosted_thumbnail_url)
    if hosted_media_urls is not FIELD_UNSET and _platform_posts_has_column(normalized_platform, "hosted_media_urls"):
        _add("hosted_media_urls", list(hosted_media_urls or []), as_jsonb=True)
    if media_mirror_status is not FIELD_UNSET and _platform_posts_has_column(
        normalized_platform, "media_mirror_status"
    ):
        _add("media_mirror_status", media_mirror_status)
    if media_mirror_error is not FIELD_UNSET and _platform_posts_has_column(normalized_platform, "media_mirror_error"):
        _add("media_mirror_error", media_mirror_error)
    if media_mirror_attempt_count is not FIELD_UNSET and _platform_posts_has_column(
        normalized_platform, "media_mirror_attempt_count"
    ):
        _add("media_mirror_attempt_count", max(0, int(media_mirror_attempt_count)))
    if media_mirror_last_attempt_at is not FIELD_UNSET and _platform_posts_has_column(
        normalized_platform, "media_mirror_last_attempt_at"
    ):
        _add("media_mirror_last_attempt_at", media_mirror_last_attempt_at)
    if media_mirror_last_job_id is not FIELD_UNSET and _platform_posts_has_column(
        normalized_platform, "media_mirror_last_job_id"
    ):
        _add("media_mirror_last_job_id", media_mirror_last_job_id)

    if not assignments:
        return

    sql = f"update social.{table} set {', '.join(assignments)} where id = %s::uuid returning id::text"
    params.append(post_id)
    with pg.db_cursor(conn=conn) as cur:
        pg.fetch_one_with_cursor(cur, sql, params)


def _update_instagram_post_media_mirror_fields(
    *,
    post_id: str,
    hosted_thumbnail_url: str | None | object = FIELD_UNSET,
    hosted_media_urls: list[str] | object = FIELD_UNSET,
    media_mirror_status: str | None | object = FIELD_UNSET,
    media_mirror_error: str | None | object = FIELD_UNSET,
    media_mirror_attempt_count: int | object = FIELD_UNSET,
    media_mirror_last_attempt_at: datetime | None | object = FIELD_UNSET,
    media_mirror_last_job_id: str | None | object = FIELD_UNSET,
    conn: Any | None = None,
) -> None:
    _update_platform_post_media_mirror_fields(
        platform="instagram",
        post_id=post_id,
        hosted_thumbnail_url=hosted_thumbnail_url,
        hosted_media_urls=hosted_media_urls,
        media_mirror_status=media_mirror_status,
        media_mirror_error=media_mirror_error,
        media_mirror_attempt_count=media_mirror_attempt_count,
        media_mirror_last_attempt_at=media_mirror_last_attempt_at,
        media_mirror_last_job_id=media_mirror_last_job_id,
        conn=conn,
    )


def _update_instagram_post_source_media_fields(
    *,
    post_id: str,
    thumbnail_url: str | None | object = FIELD_UNSET,
    media_urls: list[str] | object = FIELD_UNSET,
    conn: Any | None = None,
) -> None:
    assignments: list[str] = []
    params: list[Any] = []

    def _add(column: str, value: Any, *, as_jsonb: bool = False) -> None:
        if as_jsonb:
            assignments.append(f"{column} = %s::jsonb")
            params.append(json.dumps(value))
            return
        assignments.append(f"{column} = %s")
        params.append(value)

    if thumbnail_url is not FIELD_UNSET and _instagram_posts_has_column("thumbnail_url"):
        _add("thumbnail_url", thumbnail_url)
    if media_urls is not FIELD_UNSET and _instagram_posts_has_column("media_urls"):
        _add("media_urls", list(media_urls or []), as_jsonb=True)

    if not assignments:
        return

    sql = f"update social.instagram_posts set {', '.join(assignments)} where id = %s::uuid returning id::text"
    params.append(post_id)
    with pg.db_cursor(conn=conn) as cur:
        pg.fetch_one_with_cursor(cur, sql, params)


def _enqueue_platform_media_mirror_job(
    context: SeasonContext,
    *,
    platform: str,
    run_id: str | None,
    source_scope: str,
    account: str,
    post_row: dict[str, Any],
    week_index: int | None,
    parent_job_id: str | None,
    conn: Any | None = None,
) -> str | None:
    normalized_platform = (platform or "").strip().lower()
    job_type = PLATFORM_MEDIA_MIRROR_JOB_TYPES.get(normalized_platform)
    if not job_type:
        return None

    post_id = str(post_row.get("id") or "").strip()
    source_id = _platform_source_id(normalized_platform, post_row)
    if not post_id:
        return None
    if not _platform_post_needs_media_mirror(normalized_platform, post_row):
        return None

    existing = None
    with pg.db_cursor(conn=conn) as cur:
        existing = pg.fetch_one_with_cursor(
            cur,
            """
            select id::text as id
            from social.scrape_jobs
            where platform = %s
              and status in ('queued', 'pending', 'retrying', 'running')
              and coalesce(config->>'stage', metadata->>'stage', job_type) = %s
              and config->>'post_id' = %s
              and (%s::uuid is null or run_id = %s::uuid)
            order by created_at desc
            limit 1
            """,
            [normalized_platform, INSTAGRAM_MEDIA_MIRROR_STAGE, post_id, run_id, run_id],
        )
    if existing and existing.get("id"):
        return str(existing["id"])

    mirror_job_status = "queued" if is_queue_enabled() else "pending"
    config = {
        "run_id": run_id,
        "season_id": context.season_id,
        "show_id": context.show_id,
        "platform": normalized_platform,
        "source_scope": source_scope,
        "stage": INSTAGRAM_MEDIA_MIRROR_STAGE,
        "job_type": job_type,
        "account": account,
        "post_id": post_id,
        "source_id": source_id,
        "shortcode": source_id if normalized_platform == "instagram" else None,
        "week_index": week_index,
        "parent_job_id": parent_job_id,
    }
    mirror_job_id = _create_job(
        context,
        run_id=run_id,
        platform=normalized_platform,
        source_scope=source_scope,
        job_type=job_type,
        stage=INSTAGRAM_MEDIA_MIRROR_STAGE,
        config=config,
        initiated_by=None,
        status=mirror_job_status,
        priority=250,
        conn=conn,
    )
    _update_platform_post_media_mirror_fields(
        platform=normalized_platform,
        post_id=post_id,
        media_mirror_status="pending",
        media_mirror_error=None,
        media_mirror_last_job_id=mirror_job_id,
        conn=conn,
    )
    return mirror_job_id


def _enqueue_instagram_media_mirror_job(
    context: SeasonContext,
    *,
    run_id: str | None,
    source_scope: str,
    account: str,
    post_row: dict[str, Any],
    week_index: int | None,
    parent_job_id: str | None,
    conn: Any | None = None,
) -> str | None:
    return _enqueue_platform_media_mirror_job(
        context,
        platform="instagram",
        run_id=run_id,
        source_scope=source_scope,
        account=account,
        post_row=post_row,
        week_index=week_index,
        parent_job_id=parent_job_id,
        conn=conn,
    )


def _update_tiktok_comment_media_mirror_fields(
    *,
    comment_id: str,
    hosted_media_urls: list[str] | object = FIELD_UNSET,
    media_mirror_status: str | None | object = FIELD_UNSET,
    media_mirror_error: str | None | object = FIELD_UNSET,
    media_mirror_attempt_count: int | object = FIELD_UNSET,
    media_mirror_last_attempt_at: datetime | None | object = FIELD_UNSET,
    media_mirror_last_job_id: str | None | object = FIELD_UNSET,
    conn: Any | None = None,
) -> None:
    assignments: list[str] = []
    params: list[Any] = []

    def _add(column: str, value: Any, *, as_jsonb: bool = False) -> None:
        if as_jsonb:
            assignments.append(f"{column} = %s::jsonb")
            params.append(json.dumps(value))
            return
        assignments.append(f"{column} = %s")
        params.append(value)

    if hosted_media_urls is not FIELD_UNSET and _tiktok_comments_has_column("hosted_media_urls"):
        _add("hosted_media_urls", list(hosted_media_urls or []), as_jsonb=True)
    if media_mirror_status is not FIELD_UNSET and _tiktok_comments_has_column("media_mirror_status"):
        _add("media_mirror_status", media_mirror_status)
    if media_mirror_error is not FIELD_UNSET and _tiktok_comments_has_column("media_mirror_error"):
        _add("media_mirror_error", media_mirror_error)
    if media_mirror_attempt_count is not FIELD_UNSET and _tiktok_comments_has_column("media_mirror_attempt_count"):
        _add("media_mirror_attempt_count", max(0, int(media_mirror_attempt_count)))
    if media_mirror_last_attempt_at is not FIELD_UNSET and _tiktok_comments_has_column("media_mirror_last_attempt_at"):
        _add("media_mirror_last_attempt_at", media_mirror_last_attempt_at)
    if media_mirror_last_job_id is not FIELD_UNSET and _tiktok_comments_has_column("media_mirror_last_job_id"):
        _add("media_mirror_last_job_id", media_mirror_last_job_id)

    if not assignments:
        return

    params.append(comment_id)
    with pg.db_cursor(conn=conn) as cur:
        pg.fetch_one_with_cursor(
            cur,
            f"update social.tiktok_comments set {', '.join(assignments)} where comment_id = %s returning id::text",
            params,
        )


def _enqueue_tiktok_comment_media_mirror_job(
    context: SeasonContext,
    *,
    run_id: str | None,
    source_scope: str,
    account: str,
    comment_row: dict[str, Any],
    parent_job_id: str | None,
    conn: Any | None = None,
) -> str | None:
    if not _tiktok_comments_has_column("media_urls"):
        return None

    comment_id = str(comment_row.get("comment_id") or "").strip()
    comment_db_id = str(comment_row.get("id") or "").strip()
    post_id = str(comment_row.get("post_id") or "").strip()
    if not comment_id or not post_id:
        return None
    if not _tiktok_comment_needs_media_mirror(comment_row):
        return None

    with pg.db_cursor(conn=conn) as cur:
        existing = pg.fetch_one_with_cursor(
            cur,
            """
            select id::text as id
            from social.scrape_jobs
            where platform = 'tiktok'
              and status in ('queued', 'pending', 'retrying', 'running')
              and coalesce(config->>'stage', metadata->>'stage', job_type) = %s
              and config->>'comment_id' = %s
              and (%s::uuid is null or run_id = %s::uuid)
            order by created_at desc
            limit 1
            """,
            [COMMENT_MEDIA_MIRROR_STAGE, comment_id, run_id, run_id],
        )
    if existing and existing.get("id"):
        return str(existing["id"])

    mirror_job_status = "queued" if is_queue_enabled() else "pending"
    config = {
        "run_id": run_id,
        "season_id": context.season_id,
        "show_id": context.show_id,
        "platform": "tiktok",
        "source_scope": source_scope,
        "stage": COMMENT_MEDIA_MIRROR_STAGE,
        "job_type": TIKTOK_COMMENT_MEDIA_MIRROR_JOB_TYPE,
        "account": account,
        "comment_id": comment_id,
        "comment_db_id": comment_db_id,
        "post_id": post_id,
        "parent_job_id": parent_job_id,
    }
    mirror_job_id = _create_job(
        context,
        run_id=run_id,
        platform="tiktok",
        source_scope=source_scope,
        job_type=TIKTOK_COMMENT_MEDIA_MIRROR_JOB_TYPE,
        stage=COMMENT_MEDIA_MIRROR_STAGE,
        config=config,
        initiated_by=None,
        status=mirror_job_status,
        priority=275,
        conn=conn,
    )
    _update_tiktok_comment_media_mirror_fields(
        comment_id=comment_id,
        media_mirror_status="pending",
        media_mirror_error=None,
        media_mirror_last_job_id=mirror_job_id,
        conn=conn,
    )
    return mirror_job_id


def _upsert_instagram_comment_tree(
    context: SeasonContext,
    *,
    job_id: str | None,
    run_id: str | None,
    account: str,
    post_id: str,
    comment: Any,
    parent_comment_db_id: str | None = None,
    observed_comment_ids: set[str] | None = None,
    persist_stats: dict[str, int] | None = None,
    conn: Any | None = None,
) -> int:
    if persist_stats is not None:
        persist_stats["comments_fetched"] = int(persist_stats.get("comments_fetched") or 0) + 1

    created_at = _parse_instagram_time(getattr(comment, "created_at", None))
    comment_external_id = str(getattr(comment, "comment_id", "") or "")
    if observed_comment_ids is not None and comment_external_id:
        observed_comment_ids.add(comment_external_id)
    if not comment_external_id.strip():
        if persist_stats is not None:
            persist_stats["comments_skipped_missing_id"] = (
                int(persist_stats.get("comments_skipped_missing_id") or 0) + 1
            )
        total = 0
        for reply in getattr(comment, "replies", []) or []:
            total += _upsert_instagram_comment_tree(
                context,
                job_id=job_id,
                run_id=run_id,
                account=account,
                post_id=post_id,
                comment=reply,
                parent_comment_db_id=parent_comment_db_id,
                observed_comment_ids=observed_comment_ids,
                persist_stats=persist_stats,
                conn=conn,
            )
        return total

    payload = {
        "comment_id": comment_external_id,
        "post_id": post_id,
        "parent_comment_id": parent_comment_db_id,
        "username": getattr(comment, "username", ""),
        "user_id": getattr(comment, "user_id", None),
        "text": getattr(comment, "text", ""),
        "likes": int(getattr(comment, "likes", 0) or 0),
        "is_reply": bool(getattr(comment, "is_reply", False)),
        "reply_count": int(getattr(comment, "reply_count", 0) or 0),
        "created_at": created_at,
        "scraped_at": _now_utc(),
        "raw_data": comment.to_dict() if hasattr(comment, "to_dict") else {},
        "season_id": context.season_id,
        "source_account": account,
    }
    if job_id:
        payload["job_id"] = job_id
    if _comment_lifecycle_supported("instagram_comments"):
        payload["is_missing"] = False
        payload["missing_at"] = None
        payload["last_seen_at"] = _now_utc()
        if run_id is not None:
            payload["last_seen_run_id"] = run_id
    # Author detail columns (migration 0147)
    if _column_exists("social", "instagram_comments", "author_profile_pic_url"):
        payload["author_profile_pic_url"] = str(getattr(comment, "owner_profile_pic_url", "") or "").strip() or None
    if _column_exists("social", "instagram_comments", "author_is_verified"):
        payload["author_is_verified"] = getattr(comment, "owner_is_verified", None)
    row = _pg_upsert("instagram_comments", payload, conflict_col="comment_id", conn=conn)
    comment_db_id = (row or {}).get("id")
    if persist_stats is not None and row:
        persist_stats["comments_upserted"] = int(persist_stats.get("comments_upserted") or 0) + 1

    total = 1 if row else 0
    for reply in getattr(comment, "replies", []) or []:
        total += _upsert_instagram_comment_tree(
            context,
            job_id=job_id,
            run_id=run_id,
            account=account,
            post_id=post_id,
            comment=reply,
            parent_comment_db_id=comment_db_id,
            observed_comment_ids=observed_comment_ids,
            persist_stats=persist_stats,
            conn=conn,
        )
    return total


def _ingest_instagram(
    context: SeasonContext,
    *,
    run_id: str | None,
    account: str,
    hashtags: list[str],
    keywords: list[str],
    opts: IngestOptions,
    job_id: str,
    stage: str = "posts",
) -> tuple[int, int, dict[str, Any]]:
    from trr_backend.socials.instagram import InstagramScraper, ScrapeConfig

    try:
        post_delay_seconds = float((os.getenv("SOCIAL_INSTAGRAM_DELAY_SEC") or "").strip() or "0.15")
    except ValueError:
        post_delay_seconds = 0.15
    try:
        comment_delay_seconds = float((os.getenv("SOCIAL_INSTAGRAM_COMMENT_DELAY_SEC") or "").strip() or "0.25")
    except ValueError:
        comment_delay_seconds = 0.25

    cookies = _load_instagram_cookies()
    instagram_authenticated = bool(cookies.get("sessionid"))
    if not instagram_authenticated:
        logger.warning("Instagram ingest running without sessionid cookie; results may be limited to ~12 recent posts")
    scraper = InstagramScraper(cookies=cookies)

    retrieval_meta: dict[str, Any] = {"instagram_authenticated": instagram_authenticated}
    matched_post_count = 0
    scraped_post_count = 0
    scraped_comment_count = 0
    comment_errors = 0
    comment_fail_reasons: set[str] = set()
    comment_fail_reason_counts: Counter[str] = Counter()
    comment_refresh_reasons: Counter[str] = Counter()
    missing_marked = 0
    incomplete_comment_fetches = 0
    comments_mark_missing_skipped = 0
    rolled_back_posts = 0
    rolled_back_comments = 0
    mirror_jobs_enqueued = 0
    mirror_job_enqueue_errors = 0
    comment_stats = _new_comment_persist_stats()
    skipped_keyword = 0
    total_scraped = 0
    progress_state = _new_job_progress_state()
    activity: dict[str, Any] = {
        "phase": f"{stage}_start",
        "pages_scanned": 0,
        "posts_checked": 0,
        "matched_posts": 0,
    }

    def _flush_progress(*, force: bool = False) -> None:
        _emit_job_progress(
            job_id=job_id,
            stage=stage,
            platform="instagram",
            account=account,
            scraped_posts=scraped_post_count,
            scraped_comments=scraped_comment_count,
            posts_upserted=matched_post_count,
            comments_upserted=int(comment_stats.get("comments_upserted") or 0),
            activity=activity,
            progress_state=progress_state,
            force=force,
        )

    _flush_progress(force=True)

    if stage == "comments":
        # Comment-only stage: read existing posts from DB instead of re-scraping
        target_source_ids = _target_source_ids_for_platform(opts, "instagram")
        existing_posts = _load_existing_posts(
            "instagram",
            context,
            account,
            opts.date_start,
            opts.date_end,
            source_ids=target_source_ids,
        )
        logger.info(
            "[instagram] Comments stage: %d existing posts from DB for account=%s",
            len(existing_posts),
            account,
        )
        retrieval_meta["source"] = "db"
        snapshots = _load_comment_lifecycle_snapshots(
            [str(r["id"]) for r in existing_posts if r.get("id")],
            platform="instagram",
        )
        skipped_synced = 0
        for row in existing_posts:
            shortcode = str(row.get("shortcode") or "")
            post_db_id = str(row.get("id") or "")
            if not shortcode:
                continue
            expected = int(row.get("comments_count") or 0)
            decision = _decide_comment_refresh(
                sync_strategy=opts.sync_strategy,
                expected_count=expected,
                snapshot=snapshots.get(post_db_id),
                post_published_at=_coerce_dt(row.get("posted_at")),
                refresh_policy=opts.comment_refresh_policy,
            )
            comment_refresh_reasons[decision.reason] += 1
            if not decision.should_refresh:
                skipped_synced += 1
                continue
            scraped_post_count += 1
            activity["phase"] = "comments_fetch"
            activity["posts_checked"] = scraped_post_count
            activity["matched_posts"] = matched_post_count
            if scraped_post_count % 10 == 0:
                _touch_job_heartbeat(job_id)
            _flush_progress()
            comments: list[Any] = []
            fail_reason = ""
            observed_comment_ids: set[str] = set()
            local_comment_stats = _new_comment_persist_stats()
            fetch_failed = False
            try:
                comments = scraper.fetch_comments(
                    shortcode,
                    max_comments=opts.max_comments_per_post,
                    fetch_replies=opts.fetch_replies,
                    delay=comment_delay_seconds,
                )
                fail_reason = str(getattr(scraper, "last_comment_fetch_reason", "") or "")
                if fail_reason:
                    comment_fail_reasons.add(fail_reason)
                    comment_fail_reason_counts[fail_reason] += 1
                    if not comments:
                        comment_errors += 1
            except Exception:
                fetch_failed = True
                comment_errors += 1
                logger.exception(
                    "[instagram] Failed to fetch comments for post %s (shortcode=%s)",
                    post_db_id,
                    shortcode,
                )

            if not fetch_failed:
                try:
                    with pg.db_connection() as conn:
                        for comment in comments:
                            _upsert_instagram_comment_tree(
                                context,
                                job_id=job_id,
                                run_id=run_id,
                                account=account,
                                post_id=post_db_id,
                                comment=comment,
                                observed_comment_ids=observed_comment_ids,
                                persist_stats=local_comment_stats,
                                conn=conn,
                            )

                        is_complete = _is_comment_fetch_complete(
                            fetch_failed=False,
                            fail_reason=fail_reason,
                            auth_failed=bool(getattr(scraper, "comments_auth_failed", False)),
                            fetched_count=len(comments),
                            max_comments_per_post=opts.max_comments_per_post,
                        )
                        if is_complete:
                            missing_marked += _mark_missing_comments_for_anchor(
                                platform="instagram",
                                anchor_id=post_db_id,
                                observed_comment_ids=observed_comment_ids,
                                conn=conn,
                            )
                            _reconcile_post_comment_count(
                                platform="instagram",
                                post_db_id=post_db_id,
                                conn=conn,
                            )
                        else:
                            incomplete_comment_fetches += 1
                            comments_mark_missing_skipped += 1
                except Exception:
                    fetch_failed = True
                    comment_errors += 1
                    rolled_back_comments += int(local_comment_stats.get("comments_upserted") or 0)
                    logger.exception(
                        "[instagram] Failed to persist comments for post %s (shortcode=%s)",
                        post_db_id,
                        shortcode,
                    )

            if fetch_failed:
                incomplete_comment_fetches += 1
                comments_mark_missing_skipped += 1
                continue

            matched_post_count += 1
            activity["matched_posts"] = matched_post_count
            _merge_comment_persist_stats(comment_stats, local_comment_stats)
            scraped_comment_count = int(comment_stats.get("comments_fetched") or 0)
            _flush_progress()
        if skipped_synced:
            logger.info("[instagram] Skipped %d posts whose comments are already synced", skipped_synced)
    else:
        # Posts stage (or posts+comments): scrape from live API
        config = ScrapeConfig(
            username=account,
            hashtags=[],
            date_start=opts.date_start,
            date_end=opts.date_end,
            delay_seconds=post_delay_seconds,
            max_pages=None,
            show_id=context.show_id,
            season_number=context.season_number,
        )
        logger.info("[instagram] Scraping account=%s date_range=%s..%s", account, opts.date_start, opts.date_end)
        post_limit = opts.max_posts_per_target if opts.max_posts_per_target > 0 else None

        def _on_scrape_progress(payload: dict[str, Any]) -> None:
            nonlocal scraped_post_count
            activity["phase"] = str(payload.get("phase") or "scrape_posts")
            activity["pages_scanned"] = _normalize_non_negative_int(payload.get("pages_scanned"))
            activity["posts_checked"] = _normalize_non_negative_int(payload.get("posts_checked"))
            if payload.get("matched_posts") is not None:
                activity["matched_posts"] = _normalize_non_negative_int(payload.get("matched_posts"))
            scraped_post_count = max(scraped_post_count, _normalize_non_negative_int(payload.get("posts_checked")))
            _flush_progress()

        posts = scraper.scrape(config, progress_cb=_on_scrape_progress)
        retrieval_meta.update(dict(getattr(scraper, "last_retrieval_meta", {}) or {}))
        try:
            instagram_week_windows, _ = _resolve_week_windows(
                context,
                timezone="America/New_York",
                source_scope=opts.source_scope,
                now_utc=_now_utc(),
            )
        except Exception:  # noqa: BLE001
            instagram_week_windows = []

        existing_post_lookup: dict[str, dict[str, Any]] = {}
        snapshots: dict[str, CommentLifecycleSnapshot] = {}
        if opts.sync_strategy == "incremental" and opts.max_comments_per_post > 0:
            existing_rows = _load_existing_posts("instagram", context, account, opts.date_start, opts.date_end)
            existing_post_lookup = _build_existing_post_lookup("instagram", existing_rows)
            snapshots = _load_comment_lifecycle_snapshots(
                [str(row.get("id")) for row in existing_post_lookup.values() if row.get("id")],
                platform="instagram",
            )

        skipped_synced = 0
        for post in posts:
            total_scraped += 1
            scraped_post_count = max(scraped_post_count, total_scraped)
            activity["phase"] = "posts_filter"
            activity["posts_checked"] = scraped_post_count
            caption = str(getattr(post, "caption", "") or "")
            if not _text_contains_any_term(text=caption, hashtags=hashtags, keywords=keywords):
                skipped_keyword += 1
                _flush_progress()
                continue
            if post_limit and matched_post_count >= post_limit:
                break

            existing_row = existing_post_lookup.get(str(getattr(post, "shortcode", "") or ""))
            if existing_row and opts.max_comments_per_post > 0:
                expected_comment_count = _expected_comment_count_for_platform(
                    "instagram",
                    {"comments_count": int(getattr(post, "comments", 0) or 0)},
                )
                decision = _decide_comment_refresh(
                    sync_strategy=opts.sync_strategy,
                    expected_count=expected_comment_count,
                    snapshot=snapshots.get(str(existing_row.get("id") or "")),
                    post_published_at=_coerce_dt(existing_row.get("posted_at")),
                    refresh_policy=opts.comment_refresh_policy,
                )
                comment_refresh_reasons[decision.reason] += 1
                if not decision.should_refresh:
                    skipped_synced += 1
                    activity["phase"] = "posts_skip_up_to_date"
                    _flush_progress()
                    continue

            now_utc = _now_utc()
            try:
                _enrich_instagram_post_from_permalink(post=post, scraper=scraper, now_utc=now_utc)
            except Exception:  # noqa: BLE001
                logger.exception(
                    "[instagram] Metadata enrichment failed for shortcode=%s",
                    getattr(post, "shortcode", "?"),
                )
                post.metadata_source = None
                post.metadata_scraped_at = now_utc
                post.metadata_error = "metadata_enrichment_exception"

            post_week_index = None
            post_ts = _parse_instagram_time(getattr(post, "taken_at", None))
            if post_ts and instagram_week_windows:
                window = _week_for_timestamp(post_ts, windows=instagram_week_windows, timezone="America/New_York")
                post_week_index = window.week_index if window else None

            source_thumbnail_url, source_media_urls = _instagram_post_source_urls(
                {
                    "thumbnail_url": getattr(post, "thumbnail_url", None),
                    "media_urls": getattr(post, "media_urls", []),
                }
            )
            if source_thumbnail_url or source_media_urls:
                post.media_mirror_status = "pending"
                post.media_mirror_error = None

            comments: list[Any] = []
            fail_reason = ""
            if opts.max_comments_per_post > 0:
                try:
                    comments = scraper.fetch_comments(
                        getattr(post, "shortcode", ""),
                        max_comments=opts.max_comments_per_post,
                        fetch_replies=opts.fetch_replies,
                        delay=comment_delay_seconds,
                    )
                    fail_reason = str(getattr(scraper, "last_comment_fetch_reason", "") or "")
                    if fail_reason:
                        comment_fail_reasons.add(fail_reason)
                        comment_fail_reason_counts[fail_reason] += 1
                        if not comments:
                            comment_errors += 1
                except Exception:
                    comment_errors += 1
                    incomplete_comment_fetches += 1
                    comments_mark_missing_skipped += 1
                    logger.exception(
                        "[instagram] Failed to fetch comments for shortcode=%s",
                        getattr(post, "shortcode", "?"),
                    )
                    continue

            local_comment_stats = _new_comment_persist_stats()
            observed_comment_ids: set[str] = set()
            mirror_job_id: str | None = None
            upserted: dict[str, Any] | None = None
            try:
                with pg.db_connection() as conn:
                    upserted = _upsert_instagram_post(context, job_id=job_id, account=account, post=post, conn=conn)
                    if not upserted:
                        _flush_progress()
                        continue

                    activity["phase"] = "posts_upsert"
                    _flush_progress()

                    if opts.max_comments_per_post > 0:
                        for comment in comments:
                            _upsert_instagram_comment_tree(
                                context,
                                job_id=job_id,
                                run_id=run_id,
                                account=account,
                                post_id=str(upserted["id"]),
                                comment=comment,
                                observed_comment_ids=observed_comment_ids,
                                persist_stats=local_comment_stats,
                                conn=conn,
                            )

                        is_complete = _is_comment_fetch_complete(
                            fetch_failed=False,
                            fail_reason=fail_reason,
                            auth_failed=bool(getattr(scraper, "comments_auth_failed", False)),
                            fetched_count=len(comments),
                            max_comments_per_post=opts.max_comments_per_post,
                        )
                        if is_complete:
                            missing_marked += _mark_missing_comments_for_anchor(
                                platform="instagram",
                                anchor_id=str(upserted["id"]),
                                observed_comment_ids=observed_comment_ids,
                                conn=conn,
                            )
                            _reconcile_post_comment_count(
                                platform="instagram",
                                post_db_id=str(upserted["id"]),
                                conn=conn,
                            )
                        else:
                            incomplete_comment_fetches += 1
                            comments_mark_missing_skipped += 1

                    try:
                        mirror_job_id = _enqueue_instagram_media_mirror_job(
                            context,
                            run_id=run_id,
                            source_scope=opts.source_scope,
                            account=account,
                            post_row=upserted,
                            week_index=post_week_index,
                            parent_job_id=job_id,
                            conn=conn,
                        )
                    except Exception:  # noqa: BLE001
                        mirror_job_enqueue_errors += 1
                        logger.exception(
                            "[instagram] Failed to enqueue media mirror job for post=%s shortcode=%s",
                            str((upserted or {}).get("id") or ""),
                            getattr(post, "shortcode", "?"),
                        )
            except Exception:
                comment_errors += 1
                if upserted:
                    rolled_back_posts += 1
                rolled_back_comments += int(local_comment_stats.get("comments_upserted") or 0)
                logger.exception(
                    "[instagram] Failed to sync post/comment transaction for post %s (shortcode=%s)",
                    (upserted or {}).get("id"),
                    getattr(post, "shortcode", "?"),
                )
                continue

            matched_post_count += 1
            activity["matched_posts"] = matched_post_count
            if mirror_job_id:
                mirror_jobs_enqueued += 1
            _merge_comment_persist_stats(comment_stats, local_comment_stats)
            scraped_comment_count = int(comment_stats.get("comments_fetched") or 0)
            _flush_progress()

    activity["phase"] = f"{stage}_end"
    activity["matched_posts"] = matched_post_count
    _flush_progress(force=True)

    if skipped_synced:
        logger.info("[instagram] Skipped %d posts already up-to-date", skipped_synced)
    if comment_errors:
        retrieval_meta["comment_errors"] = comment_errors
    if comment_fail_reasons:
        retrieval_meta["comment_fail_reasons"] = sorted(comment_fail_reasons)
    if comment_fail_reason_counts:
        retrieval_meta["comment_fetch_failures_by_reason"] = dict(comment_fail_reason_counts)
    if comment_refresh_reasons:
        retrieval_meta["comment_refresh_decisions"] = dict(comment_refresh_reasons)
    if missing_marked:
        retrieval_meta["comments_marked_missing"] = missing_marked
    if incomplete_comment_fetches:
        retrieval_meta["incomplete_comment_fetches"] = incomplete_comment_fetches
    if comments_mark_missing_skipped:
        retrieval_meta["comments_mark_missing_skipped"] = comments_mark_missing_skipped
    if rolled_back_posts:
        retrieval_meta["rolled_back_posts"] = rolled_back_posts
    if rolled_back_comments:
        retrieval_meta["rolled_back_comments"] = rolled_back_comments
    if mirror_jobs_enqueued:
        retrieval_meta["media_mirror_jobs_enqueued"] = mirror_jobs_enqueued
    if mirror_job_enqueue_errors:
        retrieval_meta["media_mirror_job_enqueue_errors"] = mirror_job_enqueue_errors
    comment_media_mirror_jobs_enqueued = int(comment_stats.get("comment_media_mirror_jobs_enqueued") or 0)
    comment_media_mirror_job_enqueue_errors = int(comment_stats.get("comment_media_mirror_job_enqueue_errors") or 0)
    if comment_media_mirror_jobs_enqueued:
        retrieval_meta["comment_media_mirror_jobs_enqueued"] = comment_media_mirror_jobs_enqueued
    if comment_media_mirror_job_enqueue_errors:
        retrieval_meta["comment_media_mirror_job_enqueue_errors"] = comment_media_mirror_job_enqueue_errors
    retrieval_meta["comment_stats"] = _comment_stats_payload(comment_stats)
    retrieval_meta["scrape_counters"] = {"posts": scraped_post_count, "comments": scraped_comment_count}
    retrieval_meta["persist_counters"] = {
        "posts_upserted": matched_post_count,
        "comments_upserted": int(comment_stats.get("comments_upserted") or 0),
    }
    retrieval_meta["activity"] = _build_progress_activity_payload(activity, now=_now_utc())
    if scraper.comments_auth_failed:
        retrieval_meta["comments_auth_failed"] = True
    logger.info(
        "[instagram] Done: scraped=%d matched=%d skipped_keyword=%d comments=%d comment_errors=%d auth_failed=%s",
        total_scraped,
        matched_post_count,
        skipped_keyword,
        scraped_comment_count,
        comment_errors,
        scraper.comments_auth_failed,
    )
    return scraped_post_count, scraped_comment_count, retrieval_meta


def _upsert_tiktok_post(
    context: SeasonContext, *, job_id: str, account: str, post: Any, conn: Any | None = None
) -> dict[str, Any] | None:
    posted_at = _parse_tiktok_time(getattr(post, "create_time", None))
    media_urls = [str(url).strip() for url in (getattr(post, "media_urls", []) or []) if str(url).strip()]
    thumbnail_url = str(getattr(post, "thumbnail_url", "") or "").strip() or (media_urls[0] if media_urls else None)
    description = str(getattr(post, "description", "") or "")
    hashtags = _as_text_list(
        [
            *_as_text_list(getattr(post, "hashtags", []), strip_prefix="#"),
            *_parse_hashtags(description),
        ],
        strip_prefix="#",
    )
    mentions = _as_text_list(
        [
            *_as_text_list(getattr(post, "mentions", []), prefix="@", strip_prefix="@"),
            *_parse_mentions(description),
        ],
        prefix="@",
        strip_prefix="@",
    )
    payload = {
        "video_id": getattr(post, "video_id", ""),
        "aweme_id": getattr(post, "video_id", ""),
        "username": getattr(post, "username", account),
        "user_id": None,
        "nickname": getattr(post, "author_nickname", None),
        "description": getattr(post, "description", None),
        "hashtags": hashtags,
        "music_info": {
            "title": getattr(post, "music_title", None),
            "author": getattr(post, "music_author", None),
        },
        "likes": int(getattr(post, "likes", 0) or 0),
        "comments_count": int(getattr(post, "comments", 0) or 0),
        "shares": int(getattr(post, "shares", 0) or 0),
        "views": int(getattr(post, "views", 0) or 0),
        "duration_seconds": int(getattr(post, "duration", 0) or 0),
        "thumbnail_url": thumbnail_url,
        "posted_at": posted_at,
        "scraped_at": _now_utc(),
        "raw_data": post.to_dict() if hasattr(post, "to_dict") else {},
        "show_id": context.show_id,
        "season_id": context.season_id,
        "job_id": job_id,
        "source_account": account,
    }
    if _platform_posts_has_column("tiktok", "saves"):
        payload["saves"] = int(getattr(post, "saves", 0) or 0)
    if _platform_posts_has_column("tiktok", "mentions"):
        payload["mentions"] = mentions
    if _platform_posts_has_column("tiktok", "media_urls"):
        payload["media_urls"] = media_urls
    return _pg_upsert("tiktok_posts", payload, conflict_col="video_id", conn=conn)


def _upsert_tiktok_comment_tree(
    context: SeasonContext,
    *,
    job_id: str | None,
    run_id: str | None,
    account: str,
    post_id: str,
    comment: Any,
    parent_comment_db_id: str | None = None,
    source_scope: str = "bravo",
    observed_comment_ids: set[str] | None = None,
    persist_stats: dict[str, int] | None = None,
    conn: Any | None = None,
) -> int:
    if persist_stats is not None:
        persist_stats["comments_fetched"] = int(persist_stats.get("comments_fetched") or 0) + 1

    created_at = _parse_tiktok_time(getattr(comment, "created_at", None))
    comment_external_id = str(getattr(comment, "comment_id", "") or "")
    if observed_comment_ids is not None and comment_external_id:
        observed_comment_ids.add(comment_external_id)
    if not comment_external_id.strip():
        if persist_stats is not None:
            persist_stats["comments_skipped_missing_id"] = (
                int(persist_stats.get("comments_skipped_missing_id") or 0) + 1
            )
        total = 0
        for reply in getattr(comment, "replies", []) or []:
            total += _upsert_tiktok_comment_tree(
                context,
                job_id=job_id,
                run_id=run_id,
                account=account,
                post_id=post_id,
                comment=reply,
                parent_comment_db_id=parent_comment_db_id,
                observed_comment_ids=observed_comment_ids,
                persist_stats=persist_stats,
                source_scope=source_scope,
                conn=conn,
            )
        return total
    media_urls = [str(url).strip() for url in (getattr(comment, "media_urls", []) or []) if str(url).strip()]
    payload = {
        "comment_id": comment_external_id,
        "post_id": post_id,
        "parent_comment_id": parent_comment_db_id,
        "username": getattr(comment, "username", ""),
        "user_id": getattr(comment, "user_id", None),
        "nickname": getattr(comment, "nickname", None),
        "text": getattr(comment, "text", ""),
        "likes": int(getattr(comment, "likes", 0) or 0),
        "is_reply": bool(getattr(comment, "is_reply", False)),
        "reply_count": int(getattr(comment, "reply_count", 0) or 0),
        "created_at": created_at,
        "scraped_at": _now_utc(),
        "raw_data": comment.to_dict() if hasattr(comment, "to_dict") else {},
        "season_id": context.season_id,
        "source_account": account,
    }
    if _tiktok_comments_has_column("comment_language"):
        payload["comment_language"] = str(getattr(comment, "comment_language", "") or "").strip() or None
    if _tiktok_comments_has_column("is_author_liked"):
        is_author_liked = getattr(comment, "is_author_liked", None)
        payload["is_author_liked"] = bool(is_author_liked) if isinstance(is_author_liked, (bool, int)) else None
    if _tiktok_comments_has_column("aweme_id"):
        payload["aweme_id"] = str(getattr(comment, "aweme_id", "") or "").strip() or None
    if _tiktok_comments_has_column("parent_source_comment_id"):
        payload["parent_source_comment_id"] = (
            str(getattr(comment, "parent_source_comment_id", "") or "").strip() or None
        )
    if _tiktok_comments_has_column("user_url"):
        payload["user_url"] = str(getattr(comment, "user_url", "") or "").strip() or None
    if _tiktok_comments_has_column("user_bio"):
        payload["user_bio"] = str(getattr(comment, "user_bio", "") or "").strip() or None
    if _tiktok_comments_has_column("user_avatar_url"):
        payload["user_avatar_url"] = str(getattr(comment, "user_avatar_url", "") or "").strip() or None
    if _tiktok_comments_has_column("user_region"):
        payload["user_region"] = str(getattr(comment, "user_region", "") or "").strip() or None
    if _tiktok_comments_has_column("user_language"):
        payload["user_language"] = str(getattr(comment, "user_language", "") or "").strip() or None
    if _tiktok_comments_has_column("media_urls"):
        payload["media_urls"] = media_urls
    if media_urls and _tiktok_comments_has_column("media_mirror_status"):
        payload["media_mirror_status"] = "pending"
    if media_urls and _tiktok_comments_has_column("media_mirror_error"):
        payload["media_mirror_error"] = None
    if job_id:
        payload["job_id"] = job_id
    if _comment_lifecycle_supported("tiktok_comments"):
        payload["is_missing"] = False
        payload["missing_at"] = None
        payload["last_seen_at"] = _now_utc()
        payload["last_seen_run_id"] = run_id
    row = _pg_upsert("tiktok_comments", payload, conflict_col="comment_id", conn=conn)
    comment_db_id = (row or {}).get("id")
    if persist_stats is not None and row:
        persist_stats["comments_upserted"] = int(persist_stats.get("comments_upserted") or 0) + 1
    if row and media_urls:
        try:
            mirror_job_id = _enqueue_tiktok_comment_media_mirror_job(
                context,
                run_id=run_id,
                account=account,
                comment_row=row,
                parent_job_id=job_id,
                source_scope=source_scope,
                conn=conn,
            )
            if mirror_job_id and persist_stats is not None:
                persist_stats["comment_media_mirror_jobs_enqueued"] = (
                    int(persist_stats.get("comment_media_mirror_jobs_enqueued") or 0) + 1
                )
        except Exception:  # noqa: BLE001
            if persist_stats is not None:
                persist_stats["comment_media_mirror_job_enqueue_errors"] = (
                    int(persist_stats.get("comment_media_mirror_job_enqueue_errors") or 0) + 1
                )

    total = 1 if row else 0
    for reply in getattr(comment, "replies", []) or []:
        total += _upsert_tiktok_comment_tree(
            context,
            job_id=job_id,
            run_id=run_id,
            account=account,
            post_id=post_id,
            comment=reply,
            parent_comment_db_id=comment_db_id,
            observed_comment_ids=observed_comment_ids,
            persist_stats=persist_stats,
            source_scope=source_scope,
            conn=conn,
        )
    return total


def _ingest_tiktok(
    context: SeasonContext,
    *,
    run_id: str | None,
    account: str,
    hashtags: list[str],
    keywords: list[str],
    opts: IngestOptions,
    job_id: str,
    stage: str = "posts",
) -> tuple[int, int, dict[str, Any]]:
    from trr_backend.socials.tiktok import TikTokScrapeConfig, TikTokScraper

    tiktok_cookies = _load_tiktok_cookies()
    scraper = TikTokScraper(cookies=tiktok_cookies)

    retrieval_meta: dict[str, Any] = {}
    matched_post_count = 0
    scraped_post_count = 0
    scraped_comment_count = 0
    comment_errors = 0
    comment_fail_reasons: set[str] = set()
    comment_refresh_reasons: Counter[str] = Counter()
    missing_marked = 0
    incomplete_comment_fetches = 0
    comments_mark_missing_skipped = 0
    mirror_jobs_enqueued = 0
    mirror_job_enqueue_errors = 0
    comment_stats = _new_comment_persist_stats()
    skipped_keyword = 0
    total_scraped = 0
    progress_state = _new_job_progress_state()
    activity: dict[str, Any] = {
        "phase": f"{stage}_start",
        "pages_scanned": 0,
        "posts_checked": 0,
        "matched_posts": 0,
    }

    def _flush_progress(*, force: bool = False) -> None:
        _emit_job_progress(
            job_id=job_id,
            stage=stage,
            platform="tiktok",
            account=account,
            scraped_posts=scraped_post_count,
            scraped_comments=scraped_comment_count,
            posts_upserted=matched_post_count,
            comments_upserted=int(comment_stats.get("comments_upserted") or 0),
            activity=activity,
            progress_state=progress_state,
            force=force,
        )

    _flush_progress(force=True)

    if stage == "comments":
        target_source_ids = _target_source_ids_for_platform(opts, "tiktok")
        existing_posts = _load_existing_posts(
            "tiktok",
            context,
            account,
            opts.date_start,
            opts.date_end,
            source_ids=target_source_ids,
        )
        logger.info("[tiktok] Comments stage: %d existing posts from DB for account=%s", len(existing_posts), account)
        retrieval_meta["source"] = "db"
        snapshots = _load_comment_lifecycle_snapshots(
            [str(r["id"]) for r in existing_posts if r.get("id")],
            platform="tiktok",
        )
        skipped_synced = 0
        for row in existing_posts:
            video_id = str(row.get("video_id") or "")
            post_db_id = str(row.get("id") or "")
            if not video_id:
                continue
            expected = int(row.get("comments_count") or 0)
            decision = _decide_comment_refresh(
                sync_strategy=opts.sync_strategy,
                expected_count=expected,
                snapshot=snapshots.get(post_db_id),
                post_published_at=_coerce_dt(row.get("posted_at")),
                refresh_policy=opts.comment_refresh_policy,
            )
            comment_refresh_reasons[decision.reason] += 1
            if not decision.should_refresh:
                skipped_synced += 1
                continue
            matched_post_count += 1
            scraped_post_count += 1
            activity["phase"] = "comments_fetch"
            activity["posts_checked"] = scraped_post_count
            activity["matched_posts"] = matched_post_count
            if matched_post_count % 10 == 0:
                _touch_job_heartbeat(job_id)
            _flush_progress()
            fetch_failed = False
            comments: list[Any] = []
            fail_reason = ""
            observed_comment_ids: set[str] = set()
            local_comment_stats = _new_comment_persist_stats()
            try:
                comments = scraper.fetch_comments(
                    video_id,
                    username=account,
                    max_comments=opts.max_comments_per_post,
                    fetch_replies=opts.fetch_replies,
                    delay=0.5,
                )
                _trim_nested_comment_replies(comments, max_replies_per_post=_effective_reply_limit(opts))
                fail_reason = str(
                    getattr(scraper, "last_comment_fetch_reason", "")
                    or getattr(scraper, "_last_api_fail_reason", "")
                    or ""
                )
                if fail_reason:
                    comment_fail_reasons.add(fail_reason)
                    if not comments:
                        comment_errors += 1
            except Exception:
                fetch_failed = True
                comment_errors += 1
                logger.exception(
                    "[tiktok] Failed to fetch comments for post %s (video_id=%s)",
                    post_db_id,
                    video_id,
                )

            if not fetch_failed:
                try:
                    with pg.db_connection() as conn:
                        for comment in comments:
                            _upsert_tiktok_comment_tree(
                                context,
                                job_id=job_id,
                                run_id=run_id,
                                account=account,
                                post_id=post_db_id,
                                comment=comment,
                                source_scope=opts.source_scope,
                                observed_comment_ids=observed_comment_ids,
                                persist_stats=local_comment_stats,
                                conn=conn,
                            )

                        is_complete = _is_comment_fetch_complete(
                            fetch_failed=False,
                            fail_reason=fail_reason,
                            auth_failed=bool(getattr(scraper, "comments_auth_failed", False)),
                            fetched_count=len(comments),
                            max_comments_per_post=opts.max_comments_per_post,
                        )
                        if is_complete:
                            missing_marked += _mark_missing_comments_for_anchor(
                                platform="tiktok",
                                anchor_id=post_db_id,
                                observed_comment_ids=observed_comment_ids,
                                conn=conn,
                            )
                            _reconcile_post_comment_count(
                                platform="tiktok",
                                post_db_id=post_db_id,
                                conn=conn,
                            )
                        else:
                            incomplete_comment_fetches += 1
                            comments_mark_missing_skipped += 1
                except Exception:
                    fetch_failed = True
                    comment_errors += 1
                    logger.exception(
                        "[tiktok] Failed to persist comments for post %s (video_id=%s)",
                        post_db_id,
                        video_id,
                    )

            if fetch_failed:
                incomplete_comment_fetches += 1
                comments_mark_missing_skipped += 1
                continue

            _merge_comment_persist_stats(comment_stats, local_comment_stats)
            scraped_comment_count = int(comment_stats.get("comments_fetched") or 0)
            _flush_progress()
        if skipped_synced:
            logger.info("[tiktok] Skipped %d posts whose comments are already synced", skipped_synced)
    else:
        config = TikTokScrapeConfig(
            username=account,
            hashtags=[],
            date_start=opts.date_start,
            date_end=opts.date_end,
            delay_seconds=1.0,
            max_pages=None,
            show_id=context.show_id,
            season_number=context.season_number,
        )
        logger.info("[tiktok] Scraping account=%s date_range=%s..%s", account, opts.date_start, opts.date_end)
        post_limit = opts.max_posts_per_target if opts.max_posts_per_target > 0 else None

        def _on_scrape_progress(payload: dict[str, Any]) -> None:
            nonlocal scraped_post_count
            activity["phase"] = str(payload.get("phase") or "scrape_posts")
            activity["pages_scanned"] = _normalize_non_negative_int(payload.get("pages_scanned"))
            activity["posts_checked"] = _normalize_non_negative_int(payload.get("posts_checked"))
            if payload.get("matched_posts") is not None:
                activity["matched_posts"] = _normalize_non_negative_int(payload.get("matched_posts"))
            scraped_post_count = max(scraped_post_count, _normalize_non_negative_int(payload.get("posts_checked")))
            _flush_progress()

        posts = scraper.scrape(config, progress_cb=_on_scrape_progress)
        retrieval_meta.update(dict(getattr(scraper, "last_retrieval_meta", {}) or {}))

        existing_post_lookup: dict[str, dict[str, Any]] = {}
        snapshots: dict[str, CommentLifecycleSnapshot] = {}
        if opts.sync_strategy == "incremental" and opts.max_comments_per_post > 0:
            existing_rows = _load_existing_posts("tiktok", context, account, opts.date_start, opts.date_end)
            existing_post_lookup = _build_existing_post_lookup("tiktok", existing_rows)
            snapshots = _load_comment_lifecycle_snapshots(
                [str(row.get("id")) for row in existing_post_lookup.values() if row.get("id")],
                platform="tiktok",
            )

        skipped_synced = 0
        for post in posts:
            total_scraped += 1
            scraped_post_count = max(scraped_post_count, total_scraped)
            activity["phase"] = "posts_filter"
            activity["posts_checked"] = scraped_post_count
            description = str(getattr(post, "description", "") or "")
            if not _text_contains_any_term(text=description, hashtags=hashtags, keywords=keywords):
                skipped_keyword += 1
                _flush_progress()
                continue
            if post_limit and matched_post_count >= post_limit:
                break

            existing_row = existing_post_lookup.get(str(getattr(post, "video_id", "") or ""))
            if existing_row and opts.max_comments_per_post > 0:
                expected_comment_count = _expected_comment_count_for_platform(
                    "tiktok",
                    {"comments_count": int(getattr(post, "comments", 0) or 0)},
                )
                decision = _decide_comment_refresh(
                    sync_strategy=opts.sync_strategy,
                    expected_count=expected_comment_count,
                    snapshot=snapshots.get(str(existing_row.get("id") or "")),
                    post_published_at=_coerce_dt(existing_row.get("posted_at")),
                    refresh_policy=opts.comment_refresh_policy,
                )
                comment_refresh_reasons[decision.reason] += 1
                if not decision.should_refresh:
                    skipped_synced += 1
                    activity["phase"] = "posts_skip_up_to_date"
                    _flush_progress()
                    continue

            upserted: dict[str, Any] | None = None
            try:
                with pg.db_connection() as conn:
                    upserted = _upsert_tiktok_post(context, job_id=job_id, account=account, post=post, conn=conn)
            except Exception:
                comment_errors += 1
                logger.exception(
                    "[tiktok] Failed to upsert post for video_id=%s",
                    getattr(post, "video_id", "?"),
                )
                _flush_progress()
                continue

            if not upserted:
                _flush_progress()
                continue
            matched_post_count += 1
            activity["phase"] = "posts_upsert"
            activity["matched_posts"] = matched_post_count
            _flush_progress()

            if stage == "posts":
                try:
                    mirror_job_id = _enqueue_platform_media_mirror_job(
                        context,
                        platform="tiktok",
                        run_id=run_id,
                        source_scope=opts.source_scope,
                        account=account,
                        post_row=upserted,
                        week_index=None,
                        parent_job_id=job_id,
                        conn=None,
                    )
                    if mirror_job_id:
                        mirror_jobs_enqueued += 1
                except Exception:  # noqa: BLE001
                    mirror_job_enqueue_errors += 1
                    logger.exception(
                        "[tiktok] Failed to enqueue media mirror job for post=%s video_id=%s",
                        str(upserted.get("id") or ""),
                        str(getattr(post, "video_id", "") or ""),
                    )

            if opts.max_comments_per_post > 0:
                comments: list[Any] = []
                fail_reason = ""
                fetch_failed = False
                observed_comment_ids: set[str] = set()
                local_comment_stats = _new_comment_persist_stats()
                try:
                    comments = scraper.fetch_comments(
                        getattr(post, "video_id", ""),
                        username=account,
                        max_comments=opts.max_comments_per_post,
                        fetch_replies=opts.fetch_replies,
                        delay=0.5,
                    )
                    _trim_nested_comment_replies(comments, max_replies_per_post=_effective_reply_limit(opts))
                    fail_reason = str(
                        getattr(scraper, "last_comment_fetch_reason", "")
                        or getattr(scraper, "_last_api_fail_reason", "")
                        or ""
                    )
                    if fail_reason:
                        comment_fail_reasons.add(fail_reason)
                        if not comments:
                            comment_errors += 1
                except Exception:
                    fetch_failed = True
                    comment_errors += 1
                    logger.exception(
                        "[tiktok] Failed to fetch comments for post %s (video_id=%s)",
                        upserted.get("id"),
                        getattr(post, "video_id", "?"),
                    )
                    incomplete_comment_fetches += 1
                    comments_mark_missing_skipped += 1

                if not fetch_failed:
                    try:
                        with pg.db_connection() as conn:
                            for comment in comments:
                                _upsert_tiktok_comment_tree(
                                    context,
                                    job_id=job_id,
                                    run_id=run_id,
                                    account=account,
                                    post_id=str(upserted["id"]),
                                    comment=comment,
                                    source_scope=opts.source_scope,
                                    observed_comment_ids=observed_comment_ids,
                                    persist_stats=local_comment_stats,
                                    conn=conn,
                                )

                            is_complete = _is_comment_fetch_complete(
                                fetch_failed=False,
                                fail_reason=fail_reason,
                                auth_failed=bool(getattr(scraper, "comments_auth_failed", False)),
                                fetched_count=len(comments),
                                max_comments_per_post=opts.max_comments_per_post,
                            )
                            if is_complete:
                                missing_marked += _mark_missing_comments_for_anchor(
                                    platform="tiktok",
                                    anchor_id=str(upserted["id"]),
                                    observed_comment_ids=observed_comment_ids,
                                    conn=conn,
                                )
                                _reconcile_post_comment_count(
                                    platform="tiktok",
                                    post_db_id=str(upserted["id"]),
                                    conn=conn,
                                )
                            else:
                                incomplete_comment_fetches += 1
                                comments_mark_missing_skipped += 1
                    except Exception:
                        fetch_failed = True
                        comment_errors += 1
                        logger.exception(
                            "[tiktok] Failed to persist comments for post %s (video_id=%s)",
                            upserted.get("id"),
                            getattr(post, "video_id", "?"),
                        )
                        incomplete_comment_fetches += 1
                        comments_mark_missing_skipped += 1

                if not fetch_failed:
                    _merge_comment_persist_stats(comment_stats, local_comment_stats)
                    scraped_comment_count = int(comment_stats.get("comments_fetched") or 0)
                    activity["phase"] = "comments_fetch"
                    _flush_progress()

    activity["phase"] = f"{stage}_end"
    activity["matched_posts"] = matched_post_count
    _flush_progress(force=True)

    if skipped_synced:
        logger.info("[tiktok] Skipped %d posts already up-to-date", skipped_synced)
    if comment_errors:
        retrieval_meta["comment_errors"] = comment_errors
    if comment_fail_reasons:
        retrieval_meta["comment_fail_reasons"] = sorted(comment_fail_reasons)
    if comment_refresh_reasons:
        retrieval_meta["comment_refresh_decisions"] = dict(comment_refresh_reasons)
    if missing_marked:
        retrieval_meta["comments_marked_missing"] = missing_marked
    if incomplete_comment_fetches:
        retrieval_meta["incomplete_comment_fetches"] = incomplete_comment_fetches
    if comments_mark_missing_skipped:
        retrieval_meta["comments_mark_missing_skipped"] = comments_mark_missing_skipped
    if mirror_jobs_enqueued:
        retrieval_meta["media_mirror_jobs_enqueued"] = mirror_jobs_enqueued
    if mirror_job_enqueue_errors:
        retrieval_meta["media_mirror_job_enqueue_errors"] = mirror_job_enqueue_errors
    retrieval_meta["comment_stats"] = _comment_stats_payload(comment_stats)
    retrieval_meta["scrape_counters"] = {"posts": scraped_post_count, "comments": scraped_comment_count}
    retrieval_meta["persist_counters"] = {
        "posts_upserted": matched_post_count,
        "comments_upserted": int(comment_stats.get("comments_upserted") or 0),
    }
    retrieval_meta["activity"] = _build_progress_activity_payload(activity, now=_now_utc())
    logger.info(
        "[tiktok] Done: scraped=%d matched=%d skipped_keyword=%d comments=%d comment_errors=%d",
        total_scraped,
        matched_post_count,
        skipped_keyword,
        scraped_comment_count,
        comment_errors,
    )
    return scraped_post_count, scraped_comment_count, retrieval_meta


def _upsert_youtube_video(
    context: SeasonContext,
    *,
    job_id: str,
    account: str,
    video: Any,
    conn: Any | None = None,
) -> dict[str, Any] | None:
    published_at = _parse_instagram_time(getattr(video, "published_at", None))
    source_surface_raw = str(getattr(video, "source_surface", "") or "").strip().lower()
    source_surface = source_surface_raw if source_surface_raw in {"videos", "shorts", "search"} else None
    title = str(getattr(video, "title", "") or "")
    description = str(getattr(video, "description", "") or "")
    token_text = f"{title}\n{description}".strip()
    hashtags = _as_text_list(_parse_hashtags(token_text), strip_prefix="#")
    mentions = _as_text_list(_parse_mentions(token_text), prefix="@", strip_prefix="@")
    payload = {
        "video_id": getattr(video, "video_id", ""),
        "channel_id": getattr(video, "channel_id", None),
        "channel_title": getattr(video, "channel_title", None),
        "title": getattr(video, "title", ""),
        "description": getattr(video, "description", None),
        "duration": getattr(video, "duration", None),
        "duration_seconds": int(getattr(video, "duration_seconds", 0) or 0),
        "views": int(getattr(video, "views", 0) or 0),
        "likes": int(getattr(video, "likes", 0) or 0),
        "comments_count": int(getattr(video, "comments", 0) or 0),
        "thumbnail_url": getattr(video, "thumbnail_url", None),
        "published_at": published_at,
        "scraped_at": _now_utc(),
        "raw_data": video.to_dict() if hasattr(video, "to_dict") else {},
        "show_id": context.show_id,
        "season_id": context.season_id,
        "job_id": job_id,
        "source_account": account,
    }
    if _platform_posts_has_column("youtube", "hashtags"):
        payload["hashtags"] = hashtags
    if _platform_posts_has_column("youtube", "mentions"):
        payload["mentions"] = mentions
    if _platform_posts_has_column("youtube", "is_short"):
        payload["is_short"] = bool(getattr(video, "is_short", False))
    if _platform_posts_has_column("youtube", "source_surface"):
        payload["source_surface"] = source_surface
    return _pg_upsert("youtube_videos", payload, conflict_col="video_id", conn=conn)


def _upsert_youtube_comment_tree(
    context: SeasonContext,
    *,
    job_id: str | None,
    run_id: str | None,
    account: str,
    video_db_id: str,
    comment: Any,
    parent_comment_db_id: str | None = None,
    observed_comment_ids: set[str] | None = None,
    persist_stats: dict[str, int] | None = None,
    conn: Any | None = None,
) -> int:
    if persist_stats is not None:
        persist_stats["comments_fetched"] = int(persist_stats.get("comments_fetched") or 0) + 1

    created_at = _parse_instagram_time(getattr(comment, "created_at", None))
    comment_external_id = str(getattr(comment, "comment_id", "") or "")
    if observed_comment_ids is not None and comment_external_id:
        observed_comment_ids.add(comment_external_id)
    if not comment_external_id.strip():
        if persist_stats is not None:
            persist_stats["comments_skipped_missing_id"] = (
                int(persist_stats.get("comments_skipped_missing_id") or 0) + 1
            )
        total = 0
        for reply in getattr(comment, "replies", []) or []:
            total += _upsert_youtube_comment_tree(
                context,
                job_id=job_id,
                run_id=run_id,
                account=account,
                video_db_id=video_db_id,
                comment=reply,
                parent_comment_db_id=parent_comment_db_id,
                observed_comment_ids=observed_comment_ids,
                persist_stats=persist_stats,
                conn=conn,
            )
        return total
    payload = {
        "comment_id": comment_external_id,
        "video_id": video_db_id,
        "parent_comment_id": parent_comment_db_id,
        "author": getattr(comment, "author", ""),
        "author_channel_id": getattr(comment, "author_channel_id", None),
        "text": getattr(comment, "text", ""),
        "likes": int(getattr(comment, "likes", 0) or 0),
        "is_reply": bool(getattr(comment, "is_reply", False)),
        "reply_count": int(getattr(comment, "reply_count", 0) or 0),
        "created_at": created_at,
        "scraped_at": _now_utc(),
        "raw_data": comment.to_dict() if hasattr(comment, "to_dict") else {},
        "season_id": context.season_id,
        "source_account": account,
    }
    if job_id:
        payload["job_id"] = job_id
    if _comment_lifecycle_supported("youtube_comments"):
        payload["is_missing"] = False
        payload["missing_at"] = None
        payload["last_seen_at"] = _now_utc()
        payload["last_seen_run_id"] = run_id
    row = _pg_upsert("youtube_comments", payload, conflict_col="comment_id", conn=conn)
    comment_db_id = (row or {}).get("id")
    if persist_stats is not None and row:
        persist_stats["comments_upserted"] = int(persist_stats.get("comments_upserted") or 0) + 1

    total = 1 if row else 0
    for reply in getattr(comment, "replies", []) or []:
        total += _upsert_youtube_comment_tree(
            context,
            job_id=job_id,
            run_id=run_id,
            account=account,
            video_db_id=video_db_id,
            comment=reply,
            parent_comment_db_id=comment_db_id,
            observed_comment_ids=observed_comment_ids,
            persist_stats=persist_stats,
            conn=conn,
        )
    return total


def _ingest_youtube(
    context: SeasonContext,
    *,
    run_id: str | None,
    account: str,
    hashtags: list[str],
    keywords: list[str],
    opts: IngestOptions,
    job_id: str,
    stage: str = "posts",
) -> tuple[int, int, dict[str, Any]]:
    from trr_backend.socials.youtube import YouTubeScrapeConfig, YouTubeScraper

    scraper = YouTubeScraper()
    canonical_owner_handle = _normalize_account_handle(account)
    canonical_channel_id: str | None = None
    resolve_channel_identity = getattr(scraper, "resolve_channel_identity", None)
    if callable(resolve_channel_identity):
        try:
            resolved_identity = resolve_channel_identity(account, delay=0.25)
            resolved_handle = _normalize_account_handle((resolved_identity or {}).get("canonical_handle"))
            resolved_channel_id = str((resolved_identity or {}).get("channel_id") or "").strip() or None
            if resolved_handle:
                canonical_owner_handle = resolved_handle
            canonical_channel_id = resolved_channel_id
        except Exception:  # noqa: BLE001
            logger.exception("[youtube] Failed to resolve canonical channel identity for @%s", account)
    cleanup_summary = {"videos_deleted": 0, "comments_deleted": 0}
    try:
        cleanup_summary = _cleanup_mismatched_youtube_rows(
            context=context,
            account=account,
            expected_handle=canonical_owner_handle or account,
            expected_channel_id=canonical_channel_id,
        )
    except Exception:  # noqa: BLE001
        logger.exception("[youtube] Ownership cleanup failed for @%s", account)
    if cleanup_summary["videos_deleted"] or cleanup_summary["comments_deleted"]:
        logger.info(
            "[youtube] Ownership cleanup removed mismatched rows season=%s account=%s videos=%d comments=%d",
            context.season_id,
            account,
            cleanup_summary["videos_deleted"],
            cleanup_summary["comments_deleted"],
        )

    retrieval_meta: dict[str, Any] = {}
    matched_video_count = 0
    scraped_video_count = 0
    scraped_comment_count = 0
    comment_errors = 0
    comment_fail_reasons: set[str] = set()
    comment_refresh_reasons: Counter[str] = Counter()
    missing_marked = 0
    incomplete_comment_fetches = 0
    comments_mark_missing_skipped = 0
    mirror_jobs_enqueued = 0
    mirror_job_enqueue_errors = 0
    comment_stats = _new_comment_persist_stats()
    skipped_keyword = 0
    total_scraped = 0
    videos_scanned = 0
    videos_matched_show_terms = 0
    videos_filtered_show_terms = 0
    videos_filtered_owner = 0
    videos_skipped_up_to_date = 0
    youtube_comment_count_synced = 0
    youtube_comment_count_sync_targets: set[str] = set()
    filter_samples: list[dict[str, str]] = []
    max_filter_samples = 8
    progress_state = _new_job_progress_state()
    activity: dict[str, Any] = {
        "phase": f"{stage}_start",
        "pages_scanned": 0,
        "posts_checked": 0,
        "matched_posts": 0,
    }

    def _flush_progress(*, force: bool = False) -> None:
        _emit_job_progress(
            job_id=job_id,
            stage=stage,
            platform="youtube",
            account=account,
            scraped_posts=scraped_video_count,
            scraped_comments=scraped_comment_count,
            posts_upserted=matched_video_count,
            comments_upserted=int(comment_stats.get("comments_upserted") or 0),
            activity=activity,
            progress_state=progress_state,
            force=force,
        )

    def _record_filter_sample(*, reason: str, title: str | None, video_id: str | None) -> None:
        if len(filter_samples) >= max_filter_samples:
            return
        filter_samples.append(_youtube_filter_sample(reason=reason, title=title, video_id=video_id))

    _flush_progress(force=True)

    if stage == "comments":
        target_source_ids = _target_source_ids_for_platform(opts, "youtube")
        existing_posts = _load_existing_posts(
            "youtube",
            context,
            account,
            opts.date_start,
            opts.date_end,
            source_ids=target_source_ids,
        )
        videos_scanned = len([row for row in existing_posts if str(row.get("video_id") or "").strip()])
        videos_matched_show_terms = videos_scanned
        logger.info("[youtube] Comments stage: %d existing videos from DB for account=%s", len(existing_posts), account)
        retrieval_meta["source"] = "db"
        snapshots = _load_comment_lifecycle_snapshots(
            [str(r["id"]) for r in existing_posts if r.get("id")],
            platform="youtube",
        )
        skipped_synced = 0
        for row in existing_posts:
            vid_id = str(row.get("video_id") or "")
            post_db_id = str(row.get("id") or "")
            if not vid_id:
                continue
            snapshot = snapshots.get(post_db_id)
            expected = _expected_comment_count_for_platform("youtube", row, snapshot=snapshot)
            decision = _decide_comment_refresh(
                sync_strategy=opts.sync_strategy,
                expected_count=expected,
                snapshot=snapshot,
                post_published_at=_coerce_dt(row.get("published_at")),
                refresh_policy=opts.comment_refresh_policy,
            )
            comment_refresh_reasons[decision.reason] += 1
            if not decision.should_refresh:
                skipped_synced += 1
                videos_skipped_up_to_date += 1
                _record_filter_sample(
                    reason="up_to_date",
                    title=str(row.get("text") or ""),
                    video_id=vid_id,
                )
                continue
            matched_video_count += 1
            scraped_video_count += 1
            youtube_comment_count_sync_targets.add(post_db_id)
            activity["phase"] = "comments_fetch"
            activity["posts_checked"] = scraped_video_count
            activity["matched_posts"] = matched_video_count
            if matched_video_count % 10 == 0:
                _touch_job_heartbeat(job_id)
            _flush_progress()
            fetch_failed = False
            comments: list[Any] = []
            fail_reason = ""
            observed_comment_ids: set[str] = set()
            local_comment_stats = _new_comment_persist_stats()
            try:
                comments = scraper.fetch_comments(
                    vid_id,
                    max_comments=opts.max_comments_per_post,
                    fetch_replies=opts.fetch_replies,
                    delay=0.5,
                )
                _trim_nested_comment_replies(comments, max_replies_per_post=_effective_reply_limit(opts))
                fail_reason = str(getattr(scraper, "last_comment_fetch_reason", "") or "")
                if fail_reason:
                    comment_fail_reasons.add(fail_reason)
                    if not comments:
                        comment_errors += 1
            except Exception:
                fetch_failed = True
                comment_errors += 1
                comment_fail_reasons.add("fetch_exception")
                logger.exception(
                    "[youtube] Failed to fetch comments for video %s (video_id=%s)",
                    post_db_id,
                    vid_id,
                )

            if not fetch_failed:
                try:
                    with pg.db_connection() as conn:
                        for comment in comments:
                            _upsert_youtube_comment_tree(
                                context,
                                job_id=job_id,
                                run_id=run_id,
                                account=account,
                                video_db_id=post_db_id,
                                comment=comment,
                                observed_comment_ids=observed_comment_ids,
                                persist_stats=local_comment_stats,
                                conn=conn,
                            )

                        is_complete = _is_comment_fetch_complete(
                            fetch_failed=False,
                            fail_reason=fail_reason,
                            auth_failed=bool(getattr(scraper, "comments_auth_failed", False)),
                            fetched_count=len(comments),
                            max_comments_per_post=opts.max_comments_per_post,
                        )
                        if is_complete:
                            missing_marked += _mark_missing_comments_for_anchor(
                                platform="youtube",
                                anchor_id=post_db_id,
                                observed_comment_ids=observed_comment_ids,
                                conn=conn,
                            )
                            _reconcile_post_comment_count(
                                platform="youtube",
                                post_db_id=post_db_id,
                                conn=conn,
                            )
                        else:
                            incomplete_comment_fetches += 1
                            comments_mark_missing_skipped += 1
                except Exception:
                    fetch_failed = True
                    comment_errors += 1
                    comment_fail_reasons.add("persist_exception")
                    logger.exception(
                        "[youtube] Failed to persist comments for video %s (video_id=%s)",
                        post_db_id,
                        vid_id,
                    )

            if fetch_failed:
                incomplete_comment_fetches += 1
                comments_mark_missing_skipped += 1
                continue

            _merge_comment_persist_stats(comment_stats, local_comment_stats)
            scraped_comment_count = int(comment_stats.get("comments_fetched") or 0)
            _flush_progress()

        if youtube_comment_count_sync_targets:
            with pg.db_connection() as conn:
                youtube_comment_count_synced += _sync_youtube_video_comment_counts(
                    list(youtube_comment_count_sync_targets),
                    conn=conn,
                )
        if skipped_synced:
            logger.info("[youtube] Skipped %d videos whose comments are already synced", skipped_synced)
    else:
        config = YouTubeScrapeConfig(
            channel_handle=account,
            keywords=keywords,
            date_start=opts.date_start,
            date_end=opts.date_end,
            delay_seconds=1.0,
            max_results=None if opts.max_posts_per_target <= 0 else max(1, opts.max_posts_per_target),
            show_id=context.show_id,
            season_number=context.season_number,
        )
        logger.info(
            "[youtube] Scraping channel=%s keywords=%s date_range=%s..%s",
            account,
            keywords,
            opts.date_start,
            opts.date_end,
        )
        post_limit = opts.max_posts_per_target if opts.max_posts_per_target > 0 else None

        def _on_scrape_progress(payload: dict[str, Any]) -> None:
            nonlocal scraped_video_count
            activity["phase"] = str(payload.get("phase") or "scrape_posts")
            activity["pages_scanned"] = _normalize_non_negative_int(payload.get("pages_scanned"))
            activity["posts_checked"] = _normalize_non_negative_int(payload.get("posts_checked"))
            if payload.get("matched_posts") is not None:
                activity["matched_posts"] = _normalize_non_negative_int(payload.get("matched_posts"))
            scraped_video_count = max(scraped_video_count, _normalize_non_negative_int(payload.get("posts_checked")))
            _flush_progress()

        videos = scraper.scrape(config, progress_cb=_on_scrape_progress)
        retrieval_meta.update(dict(getattr(scraper, "last_retrieval_meta", {}) or {}))

        existing_post_lookup: dict[str, dict[str, Any]] = {}
        snapshots: dict[str, CommentLifecycleSnapshot] = {}
        if opts.sync_strategy == "incremental" and opts.max_comments_per_post > 0:
            existing_rows = _load_existing_posts("youtube", context, account, opts.date_start, opts.date_end)
            existing_post_lookup = _build_existing_post_lookup("youtube", existing_rows)
            snapshots = _load_comment_lifecycle_snapshots(
                [str(row.get("id")) for row in existing_post_lookup.values() if row.get("id")],
                platform="youtube",
            )

        skipped_synced = 0
        for video in videos:
            total_scraped += 1
            videos_scanned += 1
            scraped_video_count = max(scraped_video_count, total_scraped)
            activity["phase"] = "posts_filter"
            activity["posts_checked"] = scraped_video_count
            video_title = str(getattr(video, "title", "") or "")
            video_id = str(getattr(video, "video_id", "") or "")
            if not _youtube_video_matches_show_terms(
                title=video_title,
                description=getattr(video, "description", ""),
                hashtags=hashtags,
                keywords=keywords,
            ):
                skipped_keyword += 1
                videos_filtered_show_terms += 1
                _record_filter_sample(reason="show_terms_filtered", title=video_title, video_id=video_id)
                _flush_progress()
                continue
            if not _youtube_video_matches_owner_identity(
                video,
                expected_handle=canonical_owner_handle or account,
                expected_channel_id=canonical_channel_id,
            ):
                videos_filtered_owner += 1
                _record_filter_sample(reason="ownership_filtered", title=video_title, video_id=video_id)
                _flush_progress()
                continue
            videos_matched_show_terms += 1
            if post_limit and matched_video_count >= post_limit:
                break

            existing_row = existing_post_lookup.get(video_id)
            if existing_row and opts.max_comments_per_post > 0:
                existing_post_id = str(existing_row.get("id") or "")
                snapshot = snapshots.get(existing_post_id)
                expected_comment_count = _expected_comment_count_for_platform(
                    "youtube",
                    {"comments_count": int(getattr(video, "comments", 0) or 0)},
                    snapshot=snapshot,
                )
                decision = _decide_comment_refresh(
                    sync_strategy=opts.sync_strategy,
                    expected_count=expected_comment_count,
                    snapshot=snapshot,
                    post_published_at=_coerce_dt(existing_row.get("published_at")),
                    refresh_policy=opts.comment_refresh_policy,
                )
                comment_refresh_reasons[decision.reason] += 1
                if not decision.should_refresh:
                    skipped_synced += 1
                    videos_skipped_up_to_date += 1
                    _record_filter_sample(reason="up_to_date", title=video_title, video_id=video_id)
                    activity["phase"] = "posts_skip_up_to_date"
                    _flush_progress()
                    continue

            upserted: dict[str, Any] | None = None
            try:
                with pg.db_connection() as conn:
                    upserted = _upsert_youtube_video(context, job_id=job_id, account=account, video=video, conn=conn)
            except Exception:
                comment_errors += 1
                comment_fail_reasons.add("persist_exception")
                logger.exception(
                    "[youtube] Failed to upsert video video_id=%s",
                    getattr(video, "video_id", "?"),
                )
                _flush_progress()
                continue

            if not upserted:
                _flush_progress()
                continue
            matched_video_count += 1
            activity["phase"] = "posts_upsert"
            activity["matched_posts"] = matched_video_count
            youtube_comment_count_sync_targets.add(str(upserted["id"]))
            _flush_progress()

            if stage == "posts":
                try:
                    mirror_job_id = _enqueue_platform_media_mirror_job(
                        context,
                        platform="youtube",
                        run_id=run_id,
                        source_scope=opts.source_scope,
                        account=account,
                        post_row=upserted,
                        week_index=None,
                        parent_job_id=job_id,
                        conn=None,
                    )
                    if mirror_job_id:
                        mirror_jobs_enqueued += 1
                except Exception:  # noqa: BLE001
                    mirror_job_enqueue_errors += 1
                    logger.exception(
                        "[youtube] Failed to enqueue media mirror job for video=%s video_id=%s",
                        str(upserted.get("id") or ""),
                        str(getattr(video, "video_id", "") or ""),
                    )

            if opts.max_comments_per_post > 0:
                comments: list[Any] = []
                fail_reason = ""
                fetch_failed = False
                observed_comment_ids: set[str] = set()
                local_comment_stats = _new_comment_persist_stats()
                try:
                    comments = scraper.fetch_comments(
                        getattr(video, "video_id", ""),
                        max_comments=opts.max_comments_per_post,
                        fetch_replies=opts.fetch_replies,
                        delay=0.5,
                    )
                    _trim_nested_comment_replies(comments, max_replies_per_post=_effective_reply_limit(opts))
                    fail_reason = str(getattr(scraper, "last_comment_fetch_reason", "") or "")
                    if fail_reason:
                        comment_fail_reasons.add(fail_reason)
                        if not comments:
                            comment_errors += 1
                except Exception:
                    fetch_failed = True
                    comment_errors += 1
                    comment_fail_reasons.add("fetch_exception")
                    logger.exception(
                        "[youtube] Failed to fetch comments for video %s (video_id=%s)",
                        upserted.get("id"),
                        getattr(video, "video_id", "?"),
                    )
                    incomplete_comment_fetches += 1
                    comments_mark_missing_skipped += 1

                if not fetch_failed:
                    try:
                        with pg.db_connection() as conn:
                            for comment in comments:
                                _upsert_youtube_comment_tree(
                                    context,
                                    job_id=job_id,
                                    run_id=run_id,
                                    account=account,
                                    video_db_id=str(upserted["id"]),
                                    comment=comment,
                                    observed_comment_ids=observed_comment_ids,
                                    persist_stats=local_comment_stats,
                                    conn=conn,
                                )

                            is_complete = _is_comment_fetch_complete(
                                fetch_failed=False,
                                fail_reason=fail_reason,
                                auth_failed=bool(getattr(scraper, "comments_auth_failed", False)),
                                fetched_count=len(comments),
                                max_comments_per_post=opts.max_comments_per_post,
                            )
                            if is_complete:
                                missing_marked += _mark_missing_comments_for_anchor(
                                    platform="youtube",
                                    anchor_id=str(upserted["id"]),
                                    observed_comment_ids=observed_comment_ids,
                                    conn=conn,
                                )
                                _reconcile_post_comment_count(
                                    platform="youtube",
                                    post_db_id=str(upserted["id"]),
                                    conn=conn,
                                )
                            else:
                                incomplete_comment_fetches += 1
                                comments_mark_missing_skipped += 1
                    except Exception:
                        fetch_failed = True
                        comment_errors += 1
                        comment_fail_reasons.add("persist_exception")
                        logger.exception(
                            "[youtube] Failed to persist comments for video %s (video_id=%s)",
                            upserted.get("id"),
                            getattr(video, "video_id", "?"),
                        )
                        incomplete_comment_fetches += 1
                        comments_mark_missing_skipped += 1

                if not fetch_failed:
                    _merge_comment_persist_stats(comment_stats, local_comment_stats)
                    scraped_comment_count = int(comment_stats.get("comments_fetched") or 0)
                    activity["phase"] = "comments_fetch"
                    _flush_progress()

        if youtube_comment_count_sync_targets:
            with pg.db_connection() as conn:
                youtube_comment_count_synced += _sync_youtube_video_comment_counts(
                    list(youtube_comment_count_sync_targets),
                    conn=conn,
                )

    activity["phase"] = f"{stage}_end"
    activity["matched_posts"] = matched_video_count
    _flush_progress(force=True)

    if skipped_synced:
        logger.info("[youtube] Skipped %d videos already up-to-date", skipped_synced)
    if comment_errors:
        retrieval_meta["comment_errors"] = comment_errors
    if comment_fail_reasons:
        retrieval_meta["comment_fail_reasons"] = sorted(comment_fail_reasons)
    if comment_refresh_reasons:
        retrieval_meta["comment_refresh_decisions"] = dict(comment_refresh_reasons)
    if missing_marked:
        retrieval_meta["comments_marked_missing"] = missing_marked
    if incomplete_comment_fetches:
        retrieval_meta["incomplete_comment_fetches"] = incomplete_comment_fetches
    if comments_mark_missing_skipped:
        retrieval_meta["comments_mark_missing_skipped"] = comments_mark_missing_skipped
    if mirror_jobs_enqueued:
        retrieval_meta["media_mirror_jobs_enqueued"] = mirror_jobs_enqueued
    if mirror_job_enqueue_errors:
        retrieval_meta["media_mirror_job_enqueue_errors"] = mirror_job_enqueue_errors
    retrieval_meta["videos_scanned"] = videos_scanned
    retrieval_meta["videos_matched_show_terms"] = videos_matched_show_terms
    retrieval_meta["videos_filtered_show_terms"] = videos_filtered_show_terms
    retrieval_meta["videos_filtered_owner"] = videos_filtered_owner
    retrieval_meta["videos_skipped_up_to_date"] = videos_skipped_up_to_date
    retrieval_meta["canonical_owner_handle"] = canonical_owner_handle or account
    retrieval_meta["canonical_channel_id"] = canonical_channel_id
    retrieval_meta["ownership_cleanup"] = cleanup_summary
    retrieval_meta["youtube_comment_count_sync_targets"] = len(youtube_comment_count_sync_targets)
    retrieval_meta["youtube_comment_count_synced"] = youtube_comment_count_synced
    retrieval_meta["filter_samples"] = filter_samples
    retrieval_meta["comment_stats"] = _comment_stats_payload(comment_stats)
    retrieval_meta["scrape_counters"] = {"posts": scraped_video_count, "comments": scraped_comment_count}
    retrieval_meta["persist_counters"] = {
        "posts_upserted": matched_video_count,
        "comments_upserted": int(comment_stats.get("comments_upserted") or 0),
    }
    retrieval_meta["activity"] = _build_progress_activity_payload(activity, now=_now_utc())
    logger.info(
        "[youtube] Done: scraped=%d matched=%d skipped_keyword=%d comments=%d comment_errors=%d",
        total_scraped,
        matched_video_count,
        skipped_keyword,
        scraped_comment_count,
        comment_errors,
    )
    return scraped_video_count, scraped_comment_count, retrieval_meta


def _upsert_tweet(
    context: SeasonContext,
    *,
    job_id: str | None,
    run_id: str | None,
    account: str,
    tweet: Any,
    persist_stats: dict[str, int] | None = None,
    conn: Any | None = None,
) -> dict[str, Any] | None:
    if persist_stats is not None:
        persist_stats["comments_fetched"] = int(persist_stats.get("comments_fetched") or 0) + 1

    tweet_id = str(getattr(tweet, "tweet_id", "") or "")
    if not tweet_id.strip():
        if persist_stats is not None:
            persist_stats["comments_skipped_missing_id"] = (
                int(persist_stats.get("comments_skipped_missing_id") or 0) + 1
            )
        return None

    created_at = _parse_platform_time(getattr(tweet, "created_at", None))
    username = str(getattr(tweet, "username", "") or "").strip()
    user_profile_url = str(getattr(tweet, "user_profile_url", "") or "").strip()
    if not user_profile_url and username:
        user_profile_url = f"https://x.com/{username}"

    payload = {
        "tweet_id": tweet_id,
        "username": username,
        "display_name": getattr(tweet, "display_name", None),
        "user_verified": bool(getattr(tweet, "user_verified", False)),
        "text": getattr(tweet, "text", ""),
        "hashtags": getattr(tweet, "hashtags", []) or [],
        "mentions": getattr(tweet, "mentions", []) or [],
        "media_urls": getattr(tweet, "media_urls", []) or [],
        "likes": int(getattr(tweet, "likes", 0) or 0),
        "retweets": int(getattr(tweet, "retweets", 0) or 0),
        "replies_count": int(getattr(tweet, "replies", 0) or 0),
        "quotes": int(getattr(tweet, "quotes", 0) or 0),
        "views": int(getattr(tweet, "views", 0) or 0),
        "is_reply": bool(getattr(tweet, "is_reply", False)),
        "is_retweet": bool(getattr(tweet, "is_retweet", False)),
        "is_quote": bool(getattr(tweet, "is_quote", False)),
        "reply_to_tweet_id": getattr(tweet, "reply_to_tweet_id", None),
        "quoted_tweet_id": getattr(tweet, "quoted_tweet_id", None),
        "created_at": created_at,
        "scraped_at": _now_utc(),
        "raw_data": tweet.to_dict() if hasattr(tweet, "to_dict") else {},
        "show_id": context.show_id,
        "season_id": context.season_id,
        "source_account": account,
    }
    if _platform_posts_has_column("twitter", "user_id"):
        payload["user_id"] = str(getattr(tweet, "user_id", "") or "").strip() or None
    if _platform_posts_has_column("twitter", "user_profile_url"):
        payload["user_profile_url"] = user_profile_url or None
    if _platform_posts_has_column("twitter", "user_avatar_url"):
        payload["user_avatar_url"] = str(getattr(tweet, "user_avatar_url", "") or "").strip() or None
    if job_id:
        payload["job_id"] = job_id
    if _comment_lifecycle_supported("twitter_tweets"):
        payload["is_missing"] = False
        payload["missing_at"] = None
        payload["last_seen_at"] = _now_utc()
        payload["last_seen_run_id"] = run_id
    row = _pg_upsert("twitter_tweets", payload, conflict_col="tweet_id", conn=conn)
    if persist_stats is not None and row:
        persist_stats["comments_upserted"] = int(persist_stats.get("comments_upserted") or 0) + 1
    return row


def _ingest_twitter(
    context: SeasonContext,
    *,
    run_id: str | None,
    account: str,
    hashtags: list[str],
    keywords: list[str],
    opts: IngestOptions,
    job_id: str,
    include_reply_records: bool = True,
    hydrate_audience_replies: bool = False,
    stage: str = "posts",
) -> tuple[int, int, dict[str, Any]]:
    from trr_backend.socials.twitter import TwitterScrapeConfig, TwitterScraper

    date_start = opts.date_start or datetime.combine(context.anchor_date, time.min, tzinfo=UTC)
    date_end = opts.date_end or _now_utc()
    keyword_list = [kw for kw in keywords if isinstance(kw, str) and kw.strip()]
    hashtag_list = [tag for tag in hashtags if isinstance(tag, str) and tag.strip()]

    twitter_cookies, twitter_bearer = _load_twitter_auth()
    twikit_creds = _load_twikit_credentials()
    # If no explicit Twitter cookies but twikit has auth_token/ct0, use them
    # so GraphQL endpoints (including TweetDetail for replies) get authenticated access.
    if not twitter_cookies.get("ct0") and twikit_creds:
        if twikit_creds.get("auth_token") and twikit_creds.get("ct0"):
            twitter_cookies = {**twitter_cookies, "auth_token": twikit_creds["auth_token"], "ct0": twikit_creds["ct0"]}
            logger.info("[twitter] Using twikit auth_token/ct0 cookies for GraphQL requests")
    scraper = TwitterScraper(cookies=twitter_cookies, bearer_token=twitter_bearer, twikit_credentials=twikit_creds)

    retrieval_meta: dict[str, Any] = {}
    scraped_post_count = 0
    scraped_reply_count = 0
    upserted_post_count = 0
    skipped_keyword = 0
    hydrated_replies = 0
    hydrated_quotes = 0
    comment_errors = 0
    quote_errors = 0
    comment_fail_reasons: set[str] = set()
    quote_fail_reasons: set[str] = set()
    comment_refresh_reasons: Counter[str] = Counter()
    missing_marked = 0
    incomplete_comment_fetches = 0
    comments_mark_missing_skipped = 0
    quote_stats: dict[str, int] = {"quotes_fetched": 0, "quotes_upserted": 0}
    link_preview_media_hits = 0
    link_preview_media_misses = 0
    mirror_jobs_enqueued = 0
    mirror_job_enqueue_errors = 0
    comment_stats = _new_comment_persist_stats()
    progress_state = _new_job_progress_state()
    activity: dict[str, Any] = {
        "phase": f"{stage}_start",
        "pages_scanned": 0,
        "posts_checked": 0,
        "matched_posts": 0,
    }

    def _flush_progress(*, force: bool = False) -> None:
        _emit_job_progress(
            job_id=job_id,
            stage=stage,
            platform="twitter",
            account=account,
            scraped_posts=scraped_post_count,
            scraped_comments=scraped_reply_count,
            posts_upserted=upserted_post_count,
            comments_upserted=int(comment_stats.get("comments_upserted") or 0),
            activity=activity,
            progress_state=progress_state,
            force=force,
        )

    _flush_progress(force=True)

    if stage == "comments":
        # Comment-only stage: read existing tweets from DB and hydrate replies
        target_source_ids = _target_source_ids_for_platform(opts, "twitter")
        existing_posts = _load_existing_posts(
            "twitter",
            context,
            account,
            date_start,
            date_end,
            source_ids=target_source_ids,
        )
        # Filter to non-reply tweets only (anchor posts)
        anchor_rows = [r for r in existing_posts if not r.get("is_reply")]
        logger.info("[twitter] Comments stage: %d anchor tweets from DB for account=%s", len(anchor_rows), account)
        retrieval_meta["source"] = "db"
        per_post_limit = _effective_reply_limit(opts)
        snapshots = _load_comment_lifecycle_snapshots(
            [str(r["tweet_id"]) for r in anchor_rows if r.get("tweet_id")],
            platform="twitter",
        )
        skipped_synced = 0
        if per_post_limit > 0:
            anchor_rows_iter = (
                anchor_rows if opts.max_posts_per_target <= 0 else anchor_rows[: opts.max_posts_per_target]
            )
            for row in anchor_rows_iter:
                tweet_id = str(row.get("tweet_id") or "")
                if not tweet_id:
                    continue
                try:
                    public_summary = scraper.fetch_public_tweet_summary(tweet_id, delay=0.0)
                except Exception:
                    public_summary = None
                if public_summary:
                    try:
                        with pg.db_connection() as conn:
                            _apply_twitter_public_summary(
                                tweet_id=tweet_id,
                                summary=public_summary,
                                conn=conn,
                            )
                    except Exception:
                        logger.debug(
                            "[twitter] Failed to apply public summary for tweet_id=%s",
                            tweet_id,
                            exc_info=True,
                        )
                expected = max(
                    int(row.get("replies_count") or 0),
                    _normalize_non_negative_int((public_summary or {}).get("replies")),
                ) + int(row.get("quotes") or 0)
                decision = _decide_comment_refresh(
                    sync_strategy=opts.sync_strategy,
                    expected_count=expected,
                    snapshot=snapshots.get(tweet_id),
                    post_published_at=_coerce_dt(row.get("created_at")),
                    refresh_policy=opts.comment_refresh_policy,
                )
                comment_refresh_reasons[decision.reason] += 1
                if not decision.should_refresh:
                    skipped_synced += 1
                    continue
                scraped_post_count += 1
                activity["phase"] = "comments_fetch"
                activity["posts_checked"] = scraped_post_count
                activity["matched_posts"] = scraped_post_count
                if scraped_post_count % 10 == 0:
                    _touch_job_heartbeat(job_id)
                _flush_progress()
                fetch_failed = False
                quote_fetch_failed = False
                replies: list[Any] = []
                quotes: list[Any] = []
                fail_reason = ""
                quote_fail_reason = ""
                observed_reply_ids: set[str] = set()
                local_comment_stats = _new_comment_persist_stats()
                try:
                    replies = scraper.fetch_tweet_replies(tweet_id, delay=0.5)[:per_post_limit]
                    fail_reason = str(getattr(scraper, "last_reply_fetch_reason", "") or "")
                    if fail_reason:
                        comment_fail_reasons.add(fail_reason)
                        if not replies:
                            comment_errors += 1
                except Exception:
                    fetch_failed = True
                    comment_errors += 1
                    comment_fail_reasons.add("fetch_exception")
                    logger.exception("[twitter] Failed to fetch replies for tweet %s", tweet_id)

                try:
                    quotes = scraper.fetch_tweet_quotes(tweet_id, delay=0.5, max_pages=5)[:per_post_limit]
                    quote_fail_reason = str(getattr(scraper, "last_quote_fetch_reason", "") or "")
                    if quote_fail_reason:
                        quote_fail_reasons.add(quote_fail_reason)
                        if not quotes:
                            quote_errors += 1
                except Exception:
                    quote_fetch_failed = True
                    quote_errors += 1
                    quote_fail_reasons.add("fetch_exception")
                    logger.exception("[twitter] Failed to fetch quotes for tweet %s", tweet_id)
                quote_stats["quotes_fetched"] += len(quotes)

                if not fetch_failed:
                    try:
                        with pg.db_connection() as conn:
                            for reply in replies:
                                if not getattr(reply, "reply_to_tweet_id", None):
                                    reply.reply_to_tweet_id = tweet_id
                                reply.is_reply = True
                                reply_id = str(getattr(reply, "tweet_id", "") or "")
                                if reply_id:
                                    observed_reply_ids.add(reply_id)
                                if _upsert_tweet(
                                    context,
                                    job_id=job_id,
                                    run_id=run_id,
                                    account=account,
                                    tweet=reply,
                                    persist_stats=local_comment_stats,
                                    conn=conn,
                                ):
                                    hydrated_replies += 1

                            for quote in quotes:
                                quote.is_reply = False
                                quote.reply_to_tweet_id = None
                                quote.is_quote = True
                                if not getattr(quote, "quoted_tweet_id", None):
                                    quote.quoted_tweet_id = tweet_id
                                if str(getattr(quote, "quoted_tweet_id", "") or "") != tweet_id:
                                    continue
                                if _upsert_tweet(
                                    context,
                                    job_id=job_id,
                                    run_id=run_id,
                                    account=account,
                                    tweet=quote,
                                    persist_stats=None,
                                    conn=conn,
                                ):
                                    hydrated_quotes += 1
                                    quote_stats["quotes_upserted"] += 1

                            is_complete = _is_comment_fetch_complete(
                                fetch_failed=False,
                                fail_reason=fail_reason,
                                auth_failed=bool(getattr(scraper, "comments_auth_failed", False)),
                                fetched_count=(len(replies) + len(quotes)),
                                max_comments_per_post=per_post_limit,
                                expected_count=expected,
                            )
                            if is_complete:
                                missing_marked += _mark_missing_comments_for_anchor(
                                    platform="twitter",
                                    anchor_id=tweet_id,
                                    observed_comment_ids=observed_reply_ids,
                                    conn=conn,
                                )
                                _reconcile_post_comment_count(
                                    platform="twitter",
                                    post_db_id=tweet_id,
                                    conn=conn,
                                )
                            else:
                                incomplete_comment_fetches += 1
                                comments_mark_missing_skipped += 1
                    except Exception:
                        fetch_failed = True
                        comment_errors += 1
                        comment_fail_reasons.add("persist_exception")
                        logger.exception("[twitter] Failed to persist replies for tweet %s", tweet_id)

                if fetch_failed:
                    incomplete_comment_fetches += 1
                    comments_mark_missing_skipped += 1
                    continue

                if quote_fetch_failed:
                    quote_fail_reason = "fetch_exception"
                    if quote_fail_reason:
                        quote_fail_reasons.add(quote_fail_reason)

                _merge_comment_persist_stats(comment_stats, local_comment_stats)
                scraped_reply_count = int(comment_stats.get("comments_fetched") or 0)
                _flush_progress()
        scraped_reply_count = int(comment_stats.get("comments_fetched") or 0)
        if skipped_synced:
            logger.info("[twitter] Skipped %d tweets whose replies are already synced", skipped_synced)
    else:
        # Posts stage: scrape from live API
        term_query = _build_twitter_or_query(hashtags=hashtag_list, keywords=keyword_list)
        query = f"from:{account} {term_query}".strip() if term_query else f"from:{account}"
        config = TwitterScrapeConfig(
            query=query or account,
            date_start=date_start,
            date_end=date_end,
            include_replies=True,
            include_links=True,
            delay_seconds=1.0,
            max_pages=10,
            show_id=context.show_id,
            season_number=context.season_number,
        )
        logger.info("[twitter] Searching query=%r date_range=%s..%s", config.query, date_start, date_end)
        post_limit = opts.max_posts_per_target if opts.max_posts_per_target > 0 else None

        def _on_scrape_progress(payload: dict[str, Any]) -> None:
            nonlocal scraped_post_count
            activity["phase"] = str(payload.get("phase") or "scrape_posts")
            activity["pages_scanned"] = _normalize_non_negative_int(payload.get("pages_scanned"))
            activity["posts_checked"] = _normalize_non_negative_int(payload.get("posts_checked"))
            if payload.get("matched_posts") is not None:
                activity["matched_posts"] = _normalize_non_negative_int(payload.get("matched_posts"))
            scraped_post_count = max(scraped_post_count, _normalize_non_negative_int(payload.get("posts_checked")))
            _flush_progress()

        scraped_tweets = scraper.scrape(config, progress_cb=_on_scrape_progress)
        tweets = scraped_tweets if not post_limit else scraped_tweets[:post_limit]
        retrieval_meta.update(dict(getattr(scraper, "last_retrieval_meta", {}) or {}))
        logger.info("[twitter] Scraper returned %d tweets", len(tweets))

        existing_post_lookup: dict[str, dict[str, Any]] = {}
        snapshots: dict[str, CommentLifecycleSnapshot] = {}
        if opts.sync_strategy == "incremental" and opts.max_comments_per_post > 0:
            existing_rows = _load_existing_posts("twitter", context, account, date_start, date_end)
            existing_post_lookup = _build_existing_post_lookup("twitter", existing_rows)
            snapshots = _load_comment_lifecycle_snapshots(
                list(existing_post_lookup.keys()),
                platform="twitter",
            )

        skipped_synced = 0
        anchor_posts: list[Any] = []
        for tweet_index, tweet in enumerate(tweets, start=1):
            scraped_post_count = max(scraped_post_count, tweet_index)
            activity["phase"] = "posts_filter"
            activity["posts_checked"] = scraped_post_count
            text = str(getattr(tweet, "text", "") or "")
            if not _text_contains_any_term(text=text, hashtags=hashtag_list, keywords=keyword_list):
                skipped_keyword += 1
                _flush_progress()
                continue
            is_reply = bool(getattr(tweet, "is_reply", False))
            if not is_reply:
                public_summary: dict[str, Any] | None = None
                try:
                    public_summary = scraper.fetch_public_tweet_summary(
                        str(getattr(tweet, "tweet_id", "") or ""),
                        delay=0.0,
                    )
                except Exception:
                    public_summary = None
                if public_summary:
                    current_replies = _normalize_non_negative_int(getattr(tweet, "replies", 0))
                    tweet.replies = max(current_replies, _normalize_non_negative_int(public_summary.get("replies")))
                    current_likes = _normalize_non_negative_int(getattr(tweet, "likes", 0))
                    tweet.likes = max(current_likes, _normalize_non_negative_int(public_summary.get("likes")))
                    if not getattr(tweet, "username", None):
                        tweet.username = str(public_summary.get("username") or "").strip()
                    if not getattr(tweet, "display_name", None):
                        tweet.display_name = str(public_summary.get("display_name") or "").strip()
                    if not getattr(tweet, "user_id", None):
                        tweet.user_id = str(public_summary.get("user_id") or "").strip() or None
                    if not getattr(tweet, "user_profile_url", None):
                        tweet.user_profile_url = str(public_summary.get("user_profile_url") or "").strip() or None
                    if not getattr(tweet, "user_avatar_url", None):
                        tweet.user_avatar_url = str(public_summary.get("user_avatar_url") or "").strip() or None
                    tweet.media_urls = _merge_twitter_media_urls(
                        list(getattr(tweet, "media_urls", []) or []),
                        list(public_summary.get("media_urls") or []),
                    )
                preview_media_count = int(getattr(tweet, "link_preview_media_count", 0) or 0)
                if preview_media_count > 0:
                    link_preview_media_hits += preview_media_count
                elif "http" in text.lower() and not list(getattr(tweet, "media_urls", []) or []):
                    link_preview_media_misses += 1
                anchor_posts.append(tweet)
                existing_row = existing_post_lookup.get(str(getattr(tweet, "tweet_id", "") or ""))
                if existing_row and opts.max_comments_per_post > 0:
                    expected_comment_count = _expected_comment_count_for_platform(
                        "twitter",
                        {
                            "replies_count": int(getattr(tweet, "replies", 0) or 0),
                            "quotes": int(getattr(tweet, "quotes", 0) or 0),
                        },
                    )
                    decision = _decide_comment_refresh(
                        sync_strategy=opts.sync_strategy,
                        expected_count=expected_comment_count,
                        snapshot=snapshots.get(str(getattr(tweet, "tweet_id", "") or "")),
                        post_published_at=_coerce_dt(existing_row.get("created_at")),
                        refresh_policy=opts.comment_refresh_policy,
                    )
                    comment_refresh_reasons[decision.reason] += 1
                    if not decision.should_refresh:
                        skipped_synced += 1
                        activity["phase"] = "posts_skip_up_to_date"
                        _flush_progress()
                        continue
            if is_reply and not include_reply_records:
                _flush_progress()
                continue

            upserted = None
            try:
                with pg.db_connection() as conn:
                    upserted = _upsert_tweet(
                        context,
                        job_id=job_id,
                        run_id=run_id,
                        account=account,
                        tweet=tweet,
                        conn=conn,
                    )
            except Exception:
                comment_errors += 1
                comment_fail_reasons.add("persist_exception")
                logger.exception(
                    "[twitter] Failed to upsert tweet tweet_id=%s",
                    getattr(tweet, "tweet_id", "?"),
                )
                _flush_progress()
                continue

            if not upserted:
                _flush_progress()
                continue
            if is_reply:
                scraped_reply_count += 1
            else:
                upserted_post_count += 1
                activity["matched_posts"] = upserted_post_count
                if stage == "posts":
                    try:
                        mirror_job_id = _enqueue_platform_media_mirror_job(
                            context,
                            platform="twitter",
                            run_id=run_id,
                            source_scope=opts.source_scope,
                            account=account,
                            post_row=upserted,
                            week_index=None,
                            parent_job_id=job_id,
                            conn=None,
                        )
                        if mirror_job_id:
                            mirror_jobs_enqueued += 1
                    except Exception:  # noqa: BLE001
                        mirror_job_enqueue_errors += 1
                        logger.exception(
                            "[twitter] Failed to enqueue media mirror job for post=%s tweet_id=%s",
                            str((upserted or {}).get("id") or ""),
                            str(getattr(tweet, "tweet_id", "") or ""),
                        )
            activity["phase"] = "posts_upsert"
            _flush_progress()

        if hydrate_audience_replies and opts.max_comments_per_post > 0:
            per_post_limit = _effective_reply_limit(opts)
            if per_post_limit > 0:
                anchor_posts_iter = (
                    anchor_posts if opts.max_posts_per_target <= 0 else anchor_posts[: opts.max_posts_per_target]
                )
                for tweet in anchor_posts_iter:
                    tweet_id = str(getattr(tweet, "tweet_id", "") or "")
                    if not tweet_id:
                        continue
                    fetch_failed = False
                    quote_fetch_failed = False
                    fail_reason = ""
                    quote_fail_reason = ""
                    quotes: list[Any] = []
                    observed_reply_ids: set[str] = set()
                    local_comment_stats = _new_comment_persist_stats()
                    try:
                        replies = scraper.fetch_tweet_replies(tweet_id, delay=0.5)[:per_post_limit]
                        fail_reason = str(getattr(scraper, "last_reply_fetch_reason", "") or "")
                        if fail_reason:
                            comment_fail_reasons.add(fail_reason)
                            if not replies:
                                comment_errors += 1
                    except Exception:
                        fetch_failed = True
                        comment_errors += 1
                        comment_fail_reasons.add("fetch_exception")
                        logger.exception("[twitter] Failed to fetch replies for tweet %s", tweet_id)
                        incomplete_comment_fetches += 1
                        comments_mark_missing_skipped += 1

                    try:
                        quotes = scraper.fetch_tweet_quotes(tweet_id, delay=0.5, max_pages=5)[:per_post_limit]
                        quote_fail_reason = str(getattr(scraper, "last_quote_fetch_reason", "") or "")
                        if quote_fail_reason:
                            quote_fail_reasons.add(quote_fail_reason)
                            if not quotes:
                                quote_errors += 1
                    except Exception:
                        quote_fetch_failed = True
                        quote_errors += 1
                        quote_fail_reason = "fetch_exception"
                        quote_fail_reasons.add(quote_fail_reason)
                        logger.exception("[twitter] Failed to fetch quotes for tweet %s", tweet_id)
                    quote_stats["quotes_fetched"] += len(quotes)

                    if not fetch_failed:
                        try:
                            with pg.db_connection() as conn:
                                for reply in replies:
                                    if not getattr(reply, "reply_to_tweet_id", None):
                                        reply.reply_to_tweet_id = tweet_id
                                    reply.is_reply = True
                                    reply_id = str(getattr(reply, "tweet_id", "") or "")
                                    if reply_id:
                                        observed_reply_ids.add(reply_id)
                                    if _upsert_tweet(
                                        context,
                                        job_id=job_id,
                                        run_id=run_id,
                                        account=account,
                                        tweet=reply,
                                        persist_stats=local_comment_stats,
                                        conn=conn,
                                    ):
                                        hydrated_replies += 1

                                for quote in quotes:
                                    quote.is_reply = False
                                    quote.reply_to_tweet_id = None
                                    quote.is_quote = True
                                    if not getattr(quote, "quoted_tweet_id", None):
                                        quote.quoted_tweet_id = tweet_id
                                    if str(getattr(quote, "quoted_tweet_id", "") or "") != tweet_id:
                                        continue
                                    if _upsert_tweet(
                                        context,
                                        job_id=job_id,
                                        run_id=run_id,
                                        account=account,
                                        tweet=quote,
                                        persist_stats=None,
                                        conn=conn,
                                    ):
                                        hydrated_quotes += 1
                                        quote_stats["quotes_upserted"] += 1

                                is_complete = _is_comment_fetch_complete(
                                    fetch_failed=False,
                                    fail_reason=fail_reason,
                                    auth_failed=bool(getattr(scraper, "comments_auth_failed", False)),
                                    fetched_count=(len(replies) + len(quotes)),
                                    max_comments_per_post=per_post_limit,
                                    expected_count=(
                                        _normalize_non_negative_int(getattr(tweet, "replies", 0))
                                        + _normalize_non_negative_int(getattr(tweet, "quotes", 0))
                                    ),
                                )
                                if is_complete:
                                    missing_marked += _mark_missing_comments_for_anchor(
                                        platform="twitter",
                                        anchor_id=tweet_id,
                                        observed_comment_ids=observed_reply_ids,
                                        conn=conn,
                                    )
                                    _reconcile_post_comment_count(
                                        platform="twitter",
                                        post_db_id=tweet_id,
                                        conn=conn,
                                    )
                                else:
                                    incomplete_comment_fetches += 1
                                    comments_mark_missing_skipped += 1
                        except Exception:
                            fetch_failed = True
                            comment_errors += 1
                            comment_fail_reasons.add("persist_exception")
                            logger.exception("[twitter] Failed to persist replies for tweet %s", tweet_id)
                            incomplete_comment_fetches += 1
                            comments_mark_missing_skipped += 1

                    if not fetch_failed:
                        _merge_comment_persist_stats(comment_stats, local_comment_stats)
                        scraped_reply_count = int(comment_stats.get("comments_fetched") or 0)
                        activity["phase"] = "comments_fetch"
                        _flush_progress()
                    if quote_fetch_failed and not quote_fail_reason:
                        quote_fail_reasons.add("fetch_exception")
            scraped_reply_count = int(comment_stats.get("comments_fetched") or 0)
        if skipped_synced:
            logger.info("[twitter] Skipped %d posts already up-to-date", skipped_synced)

    if comment_errors:
        retrieval_meta["comment_errors"] = comment_errors
    if comment_fail_reasons:
        retrieval_meta["comment_fail_reasons"] = sorted(comment_fail_reasons)
    if comment_refresh_reasons:
        retrieval_meta["comment_refresh_decisions"] = dict(comment_refresh_reasons)
    if missing_marked:
        retrieval_meta["comments_marked_missing"] = missing_marked
    if incomplete_comment_fetches:
        retrieval_meta["incomplete_comment_fetches"] = incomplete_comment_fetches
    if comments_mark_missing_skipped:
        retrieval_meta["comments_mark_missing_skipped"] = comments_mark_missing_skipped
    if mirror_jobs_enqueued:
        retrieval_meta["media_mirror_jobs_enqueued"] = mirror_jobs_enqueued
    if mirror_job_enqueue_errors:
        retrieval_meta["media_mirror_job_enqueue_errors"] = mirror_job_enqueue_errors
    if quote_errors:
        retrieval_meta["quote_errors"] = quote_errors
    if quote_fail_reasons:
        retrieval_meta["quote_fail_reasons"] = sorted(quote_fail_reasons)
    retrieval_meta["quote_stats"] = {
        "quotes_fetched": int(quote_stats.get("quotes_fetched") or 0),
        "quotes_upserted": int(quote_stats.get("quotes_upserted") or 0),
    }
    retrieval_meta["twitter_link_preview_media"] = {
        "resolved": int(link_preview_media_hits),
        "missing": int(link_preview_media_misses),
    }
    retrieval_meta["comment_stats"] = _comment_stats_payload(comment_stats)
    retrieval_meta["scrape_counters"] = {"posts": scraped_post_count, "comments": scraped_reply_count}
    retrieval_meta["persist_counters"] = {
        "posts_upserted": upserted_post_count,
        "comments_upserted": int(comment_stats.get("comments_upserted") or 0),
    }
    activity["phase"] = f"{stage}_end"
    retrieval_meta["activity"] = _build_progress_activity_payload(activity, now=_now_utc())

    _flush_progress(force=True)

    logger.info(
        (
            "[twitter] Done: posts=%d replies=%d quotes=%d hydrated_replies=%d "
            "hydrated_quotes=%d skipped_keyword=%d comment_errors=%d quote_errors=%d"
        ),
        scraped_post_count,
        scraped_reply_count,
        int(quote_stats.get("quotes_upserted") or 0),
        hydrated_replies,
        hydrated_quotes,
        skipped_keyword,
        comment_errors,
        quote_errors,
    )
    retrieval_meta["hydrated_replies"] = hydrated_replies
    retrieval_meta["hydrated_quotes"] = hydrated_quotes
    return scraped_post_count, scraped_reply_count, retrieval_meta


def _upsert_facebook_post(
    context: SeasonContext,
    *,
    job_id: str,
    account: str,
    post: Any,
    conn: Any | None = None,
) -> dict[str, Any] | None:
    scraped_at = _now_utc()
    posted_at = _parse_platform_time(getattr(post, "posted_at", None)) or scraped_at
    media_urls = [str(url).strip() for url in (getattr(post, "media_urls", []) or []) if str(url).strip()]
    thumbnail_url = str(getattr(post, "thumbnail_url", "") or "").strip() or (media_urls[0] if media_urls else None)
    caption = str(getattr(post, "caption", "") or "").strip() or None
    hashtags = _as_text_list(_parse_hashtags(caption), strip_prefix="#")
    mentions = _as_text_list(_parse_mentions(caption), prefix="@", strip_prefix="@")
    raw_payload = post.to_dict() if hasattr(post, "to_dict") else {}
    if isinstance(raw_payload, dict) and raw_payload.get("posted_at") is None:
        ingest_meta = dict(raw_payload.get("_ingest") or {})
        ingest_meta["posted_at_fallback"] = "scraped_at"
        raw_payload["_ingest"] = ingest_meta
    payload = {
        "post_id": str(getattr(post, "post_id", "") or "").strip(),
        "page_id": None,
        "username": str(getattr(post, "username", account) or "").strip() or account,
        "user_id": None,
        "caption": caption,
        "post_type": str(getattr(post, "post_type", "feed") or "feed").strip().lower(),
        "media_type": str(getattr(post, "post_type", "feed") or "feed").strip().lower(),
        "media_urls": media_urls,
        "thumbnail_url": thumbnail_url,
        "likes": int(getattr(post, "likes", 0) or 0),
        "comments_count": int(getattr(post, "comments", 0) or 0),
        "shares": int(getattr(post, "shares", 0) or 0),
        "views": int(getattr(post, "views", 0) or 0),
        "posted_at": posted_at,
        "scraped_at": scraped_at,
        "raw_data": raw_payload,
        "show_id": context.show_id,
        "season_id": context.season_id,
        "job_id": job_id,
        "source_account": account,
    }
    if _platform_posts_has_column("facebook", "hashtags"):
        payload["hashtags"] = hashtags
    if _platform_posts_has_column("facebook", "mentions"):
        payload["mentions"] = mentions
    if media_urls or thumbnail_url:
        payload["media_mirror_status"] = "pending"
        payload["media_mirror_error"] = None
    return _pg_upsert("facebook_posts", payload, conflict_col="post_id", conn=conn)


def _upsert_facebook_comment_tree(
    context: SeasonContext,
    *,
    job_id: str | None,
    run_id: str | None,
    account: str,
    post_id: str,
    comment: Any,
    parent_comment_db_id: str | None = None,
    observed_comment_ids: set[str] | None = None,
    persist_stats: dict[str, int] | None = None,
    conn: Any | None = None,
) -> int:
    if persist_stats is not None:
        persist_stats["comments_fetched"] = int(persist_stats.get("comments_fetched") or 0) + 1
    comment_external_id = str(getattr(comment, "comment_id", "") or "").strip()
    if observed_comment_ids is not None and comment_external_id:
        observed_comment_ids.add(comment_external_id)
    if not comment_external_id:
        return 0

    media_urls = [str(url).strip() for url in (getattr(comment, "media_urls", []) or []) if str(url).strip()]
    scraped_at = _now_utc()
    created_at = _parse_platform_time(getattr(comment, "created_at", None)) or scraped_at
    raw_payload = comment.to_dict() if hasattr(comment, "to_dict") else {}
    if isinstance(raw_payload, dict) and raw_payload.get("created_at") is None:
        ingest_meta = dict(raw_payload.get("_ingest") or {})
        ingest_meta["created_at_fallback"] = "scraped_at"
        raw_payload["_ingest"] = ingest_meta
    payload = {
        "comment_id": comment_external_id,
        "post_id": post_id,
        "parent_comment_id": parent_comment_db_id,
        "parent_source_comment_id": str(getattr(comment, "parent_source_comment_id", "") or "").strip() or None,
        "username": str(getattr(comment, "username", "") or "").strip() or account,
        "user_id": None,
        "text": str(getattr(comment, "text", "") or ""),
        "likes": int(getattr(comment, "likes", 0) or 0),
        "is_reply": bool(getattr(comment, "is_reply", False)),
        "reply_count": int(getattr(comment, "reply_count", 0) or 0),
        "created_at": created_at,
        "scraped_at": scraped_at,
        "raw_data": raw_payload,
        "season_id": context.season_id,
        "source_account": account,
        "media_urls": media_urls,
    }
    if job_id:
        payload["job_id"] = job_id
    if media_urls:
        payload["media_mirror_status"] = "pending"
        payload["media_mirror_error"] = None
    if _comment_lifecycle_supported("facebook_comments"):
        payload["is_missing"] = False
        payload["missing_at"] = None
        payload["last_seen_at"] = scraped_at
        payload["last_seen_run_id"] = run_id

    row = _pg_upsert("facebook_comments", payload, conflict_col="comment_id", conn=conn)
    comment_db_id = (row or {}).get("id")
    if persist_stats is not None and row:
        persist_stats["comments_upserted"] = int(persist_stats.get("comments_upserted") or 0) + 1
    total = 1 if row else 0
    for reply in getattr(comment, "replies", []) or []:
        total += _upsert_facebook_comment_tree(
            context,
            job_id=job_id,
            run_id=run_id,
            account=account,
            post_id=post_id,
            comment=reply,
            parent_comment_db_id=comment_db_id,
            observed_comment_ids=observed_comment_ids,
            persist_stats=persist_stats,
            conn=conn,
        )
    return total


def _ingest_facebook(
    context: SeasonContext,
    *,
    run_id: str | None,
    account: str,
    hashtags: list[str],
    keywords: list[str],
    opts: IngestOptions,
    job_id: str,
    stage: str = "posts",
) -> tuple[int, int, dict[str, Any]]:
    from trr_backend.socials.facebook import FacebookScrapeConfig, FacebookScraper

    scraper = FacebookScraper(cookies=_load_facebook_cookies())
    retrieval_meta: dict[str, Any] = {"facebook_authenticated": bool(scraper.cookies)}
    post_count = 0
    comment_count = 0
    matched_posts = 0
    mirror_jobs_enqueued = 0
    progress_state = _new_job_progress_state()
    activity: dict[str, Any] = {"phase": f"{stage}_start", "pages_scanned": 0, "posts_checked": 0, "matched_posts": 0}

    def _flush_progress(*, force: bool = False) -> None:
        _emit_job_progress(
            job_id=job_id,
            stage=stage,
            platform="facebook",
            account=account,
            scraped_posts=post_count,
            scraped_comments=comment_count,
            posts_upserted=matched_posts,
            comments_upserted=comment_count,
            activity=activity,
            progress_state=progress_state,
            force=force,
        )

    _flush_progress(force=True)
    if stage == "comments":
        target_source_ids = _target_source_ids_for_platform(opts, "facebook")
        existing_posts = _load_existing_posts(
            "facebook",
            context,
            account,
            opts.date_start,
            opts.date_end,
            source_ids=target_source_ids,
        )
        for row in existing_posts:
            source_id = str(row.get("post_id") or "")
            post_db_id = str(row.get("id") or "")
            if not source_id or not post_db_id:
                continue
            comments = scraper.fetch_comments(
                source_id,
                max_comments=opts.max_comments_per_post,
                fetch_replies=opts.fetch_replies,
            )
            local_stats = _new_comment_persist_stats()
            observed_comment_ids: set[str] = set()
            with pg.db_connection() as conn:
                for comment in comments:
                    _upsert_facebook_comment_tree(
                        context,
                        job_id=job_id,
                        run_id=run_id,
                        account=account,
                        post_id=post_db_id,
                        comment=comment,
                        observed_comment_ids=observed_comment_ids,
                        persist_stats=local_stats,
                        conn=conn,
                    )
            post_count += 1
            matched_posts += 1
            comment_count += int(local_stats.get("comments_upserted") or 0)
            activity["phase"] = "comments_fetch"
            activity["posts_checked"] = post_count
            activity["matched_posts"] = matched_posts
            _flush_progress()
    else:
        config = FacebookScrapeConfig(
            page_handle=account,
            date_start=opts.date_start,
            date_end=opts.date_end,
            delay_seconds=max(0.5, float(os.getenv("SOCIAL_FACEBOOK_DELAY_SEC", "1.0"))),
            max_pages=opts.max_posts_per_target if opts.max_posts_per_target > 0 else None,
            include_feed=True,
            include_reels=True,
            include_photos=True,
        )

        def _on_progress(payload: dict[str, Any]) -> None:
            activity["phase"] = str(payload.get("phase") or "scrape_posts")
            activity["pages_scanned"] = _normalize_non_negative_int(payload.get("pages_scanned"))
            activity["posts_checked"] = _normalize_non_negative_int(payload.get("posts_checked"))
            activity["matched_posts"] = _normalize_non_negative_int(payload.get("matched_posts"))
            _flush_progress()

        posts = scraper.scrape(config, progress_cb=_on_progress)
        retrieval_meta.update(dict(getattr(scraper, "last_retrieval_meta", {}) or {}))
        for post in posts:
            post_count += 1
            text = str(getattr(post, "caption", "") or "")
            if not _text_contains_any_term(text=text, hashtags=hashtags, keywords=keywords):
                _flush_progress()
                continue
            local_stats = _new_comment_persist_stats()
            observed_comment_ids: set[str] = set()
            mirror_job_id: str | None = None
            with pg.db_connection() as conn:
                upserted = _upsert_facebook_post(context, job_id=job_id, account=account, post=post, conn=conn)
                if not upserted:
                    continue
                if opts.max_comments_per_post > 0:
                    comments = scraper.fetch_comments(
                        getattr(post, "url", "") or getattr(post, "post_id", ""),
                        max_comments=opts.max_comments_per_post,
                        fetch_replies=opts.fetch_replies,
                    )
                    for comment in comments:
                        _upsert_facebook_comment_tree(
                            context,
                            job_id=job_id,
                            run_id=run_id,
                            account=account,
                            post_id=str(upserted["id"]),
                            comment=comment,
                            observed_comment_ids=observed_comment_ids,
                            persist_stats=local_stats,
                            conn=conn,
                        )
                mirror_job_id = _enqueue_platform_media_mirror_job(
                    context,
                    platform="facebook",
                    run_id=run_id,
                    source_scope=opts.source_scope,
                    account=account,
                    post_row=upserted,
                    week_index=None,
                    parent_job_id=job_id,
                    conn=conn,
                )
            matched_posts += 1
            comment_count += int(local_stats.get("comments_upserted") or 0)
            if mirror_job_id:
                mirror_jobs_enqueued += 1
            activity["phase"] = "posts_upsert"
            activity["matched_posts"] = matched_posts
            _flush_progress()

    activity["phase"] = f"{stage}_end"
    _flush_progress(force=True)
    retrieval_meta["media_mirror_jobs_enqueued"] = mirror_jobs_enqueued
    retrieval_meta["scrape_counters"] = {"posts": post_count, "comments": comment_count}
    retrieval_meta["persist_counters"] = {"posts_upserted": matched_posts, "comments_upserted": comment_count}
    return post_count, comment_count, retrieval_meta


def _upsert_meta_threads_post(
    context: SeasonContext,
    *,
    job_id: str,
    account: str,
    post: Any,
    conn: Any | None = None,
) -> dict[str, Any] | None:
    scraped_at = _now_utc()
    posted_at = _parse_platform_time(getattr(post, "posted_at", None)) or scraped_at
    media_urls = [str(url).strip() for url in (getattr(post, "media_urls", []) or []) if str(url).strip()]
    thumbnail_url = str(getattr(post, "thumbnail_url", "") or "").strip() or (media_urls[0] if media_urls else None)
    text = str(getattr(post, "text", "") or "").strip() or None
    hashtags = _as_text_list(_parse_hashtags(text), strip_prefix="#")
    mentions = _as_text_list(_parse_mentions(text), prefix="@", strip_prefix="@")
    raw_payload = post.to_dict() if hasattr(post, "to_dict") else {}
    if isinstance(raw_payload, dict) and raw_payload.get("posted_at") is None:
        ingest_meta = dict(raw_payload.get("_ingest") or {})
        ingest_meta["posted_at_fallback"] = "scraped_at"
        raw_payload["_ingest"] = ingest_meta
    payload = {
        "post_id": str(getattr(post, "post_id", "") or "").strip(),
        "thread_item_id": str(getattr(post, "post_id", "") or "").strip(),
        "username": str(getattr(post, "username", account) or "").strip() or account,
        "user_id": None,
        "text": text,
        "media_type": "mixed" if len(media_urls) > 1 else ("image" if media_urls else "text"),
        "media_urls": media_urls,
        "thumbnail_url": thumbnail_url,
        "likes": int(getattr(post, "likes", 0) or 0),
        "replies_count": int(getattr(post, "replies", 0) or 0),
        "reposts": int(getattr(post, "reposts", 0) or 0),
        "quotes": int(getattr(post, "quotes", 0) or 0),
        "views": int(getattr(post, "views", 0) or 0),
        "posted_at": posted_at,
        "scraped_at": scraped_at,
        "raw_data": raw_payload,
        "show_id": context.show_id,
        "season_id": context.season_id,
        "job_id": job_id,
        "source_account": account,
    }
    if _platform_posts_has_column("threads", "hashtags"):
        payload["hashtags"] = hashtags
    if _platform_posts_has_column("threads", "mentions"):
        payload["mentions"] = mentions
    if media_urls or thumbnail_url:
        payload["media_mirror_status"] = "pending"
        payload["media_mirror_error"] = None
    return _pg_upsert("meta_threads_posts", payload, conflict_col="post_id", conn=conn)


def _upsert_meta_threads_comment_tree(
    context: SeasonContext,
    *,
    job_id: str | None,
    run_id: str | None,
    account: str,
    post_id: str,
    comment: Any,
    parent_comment_db_id: str | None = None,
    observed_comment_ids: set[str] | None = None,
    persist_stats: dict[str, int] | None = None,
    conn: Any | None = None,
) -> int:
    if persist_stats is not None:
        persist_stats["comments_fetched"] = int(persist_stats.get("comments_fetched") or 0) + 1
    comment_external_id = str(getattr(comment, "comment_id", "") or "").strip()
    if observed_comment_ids is not None and comment_external_id:
        observed_comment_ids.add(comment_external_id)
    if not comment_external_id:
        return 0
    media_urls = [str(url).strip() for url in (getattr(comment, "media_urls", []) or []) if str(url).strip()]
    scraped_at = _now_utc()
    created_at = _parse_platform_time(getattr(comment, "created_at", None)) or scraped_at
    raw_payload = comment.to_dict() if hasattr(comment, "to_dict") else {}
    if isinstance(raw_payload, dict) and raw_payload.get("created_at") is None:
        ingest_meta = dict(raw_payload.get("_ingest") or {})
        ingest_meta["created_at_fallback"] = "scraped_at"
        raw_payload["_ingest"] = ingest_meta
    payload = {
        "comment_id": comment_external_id,
        "post_id": post_id,
        "parent_comment_id": parent_comment_db_id,
        "parent_source_comment_id": str(getattr(comment, "parent_source_comment_id", "") or "").strip() or None,
        "username": str(getattr(comment, "username", "") or "").strip() or account,
        "user_id": None,
        "text": str(getattr(comment, "text", "") or ""),
        "likes": int(getattr(comment, "likes", 0) or 0),
        "is_reply": bool(getattr(comment, "is_reply", True)),
        "reply_count": int(getattr(comment, "reply_count", 0) or 0),
        "created_at": created_at,
        "scraped_at": scraped_at,
        "raw_data": raw_payload,
        "season_id": context.season_id,
        "source_account": account,
        "media_urls": media_urls,
    }
    if job_id:
        payload["job_id"] = job_id
    if media_urls:
        payload["media_mirror_status"] = "pending"
        payload["media_mirror_error"] = None
    if _comment_lifecycle_supported("meta_threads_comments"):
        payload["is_missing"] = False
        payload["missing_at"] = None
        payload["last_seen_at"] = scraped_at
        payload["last_seen_run_id"] = run_id

    row = _pg_upsert("meta_threads_comments", payload, conflict_col="comment_id", conn=conn)
    comment_db_id = (row or {}).get("id")
    if persist_stats is not None and row:
        persist_stats["comments_upserted"] = int(persist_stats.get("comments_upserted") or 0) + 1
    total = 1 if row else 0
    for reply in getattr(comment, "replies", []) or []:
        total += _upsert_meta_threads_comment_tree(
            context,
            job_id=job_id,
            run_id=run_id,
            account=account,
            post_id=post_id,
            comment=reply,
            parent_comment_db_id=comment_db_id,
            observed_comment_ids=observed_comment_ids,
            persist_stats=persist_stats,
            conn=conn,
        )
    return total


def _ingest_threads(
    context: SeasonContext,
    *,
    run_id: str | None,
    account: str,
    hashtags: list[str],
    keywords: list[str],
    opts: IngestOptions,
    job_id: str,
    stage: str = "posts",
) -> tuple[int, int, dict[str, Any]]:
    from trr_backend.socials.threads import ThreadsScrapeConfig, ThreadsScraper

    scraper = ThreadsScraper(cookies=_load_threads_cookies())
    retrieval_meta: dict[str, Any] = {"threads_authenticated": bool(scraper.cookies)}
    post_count = 0
    comment_count = 0
    matched_posts = 0
    mirror_jobs_enqueued = 0
    progress_state = _new_job_progress_state()
    activity: dict[str, Any] = {"phase": f"{stage}_start", "pages_scanned": 0, "posts_checked": 0, "matched_posts": 0}

    def _flush_progress(*, force: bool = False) -> None:
        _emit_job_progress(
            job_id=job_id,
            stage=stage,
            platform="threads",
            account=account,
            scraped_posts=post_count,
            scraped_comments=comment_count,
            posts_upserted=matched_posts,
            comments_upserted=comment_count,
            activity=activity,
            progress_state=progress_state,
            force=force,
        )

    _flush_progress(force=True)
    if stage == "comments":
        target_source_ids = _target_source_ids_for_platform(opts, "threads")
        existing_posts = _load_existing_posts(
            "threads",
            context,
            account,
            opts.date_start,
            opts.date_end,
            source_ids=target_source_ids,
        )
        for row in existing_posts:
            source_id = str(row.get("post_id") or "")
            post_db_id = str(row.get("id") or "")
            if not source_id or not post_db_id:
                continue
            comments = scraper.fetch_comments(
                source_id,
                max_comments=opts.max_comments_per_post,
                fetch_replies=opts.fetch_replies,
            )
            local_stats = _new_comment_persist_stats()
            observed_comment_ids: set[str] = set()
            with pg.db_connection() as conn:
                for comment in comments:
                    _upsert_meta_threads_comment_tree(
                        context,
                        job_id=job_id,
                        run_id=run_id,
                        account=account,
                        post_id=post_db_id,
                        comment=comment,
                        observed_comment_ids=observed_comment_ids,
                        persist_stats=local_stats,
                        conn=conn,
                    )
            post_count += 1
            matched_posts += 1
            comment_count += int(local_stats.get("comments_upserted") or 0)
            activity["phase"] = "comments_fetch"
            activity["posts_checked"] = post_count
            activity["matched_posts"] = matched_posts
            _flush_progress()
    else:
        config = ThreadsScrapeConfig(
            username=account,
            date_start=opts.date_start,
            date_end=opts.date_end,
            delay_seconds=max(0.5, float(os.getenv("SOCIAL_THREADS_DELAY_SEC", "1.0"))),
            max_pages=opts.max_posts_per_target if opts.max_posts_per_target > 0 else None,
        )

        def _on_progress(payload: dict[str, Any]) -> None:
            activity["phase"] = str(payload.get("phase") or "scrape_posts")
            activity["pages_scanned"] = _normalize_non_negative_int(payload.get("pages_scanned"))
            activity["posts_checked"] = _normalize_non_negative_int(payload.get("posts_checked"))
            activity["matched_posts"] = _normalize_non_negative_int(payload.get("matched_posts"))
            _flush_progress()

        posts = scraper.scrape(config, progress_cb=_on_progress)
        retrieval_meta.update(dict(getattr(scraper, "last_retrieval_meta", {}) or {}))
        for post in posts:
            post_count += 1
            text = str(getattr(post, "text", "") or "")
            if not _text_contains_any_term(text=text, hashtags=hashtags, keywords=keywords):
                _flush_progress()
                continue
            local_stats = _new_comment_persist_stats()
            observed_comment_ids: set[str] = set()
            mirror_job_id: str | None = None
            with pg.db_connection() as conn:
                upserted = _upsert_meta_threads_post(context, job_id=job_id, account=account, post=post, conn=conn)
                if not upserted:
                    continue
                if opts.max_comments_per_post > 0:
                    comments = scraper.fetch_comments(
                        getattr(post, "url", "") or getattr(post, "post_id", ""),
                        max_comments=opts.max_comments_per_post,
                        fetch_replies=opts.fetch_replies,
                    )
                    for comment in comments:
                        _upsert_meta_threads_comment_tree(
                            context,
                            job_id=job_id,
                            run_id=run_id,
                            account=account,
                            post_id=str(upserted["id"]),
                            comment=comment,
                            observed_comment_ids=observed_comment_ids,
                            persist_stats=local_stats,
                            conn=conn,
                        )
                mirror_job_id = _enqueue_platform_media_mirror_job(
                    context,
                    platform="threads",
                    run_id=run_id,
                    source_scope=opts.source_scope,
                    account=account,
                    post_row=upserted,
                    week_index=None,
                    parent_job_id=job_id,
                    conn=conn,
                )
            matched_posts += 1
            comment_count += int(local_stats.get("comments_upserted") or 0)
            if mirror_job_id:
                mirror_jobs_enqueued += 1
            activity["phase"] = "posts_upsert"
            activity["matched_posts"] = matched_posts
            _flush_progress()

    activity["phase"] = f"{stage}_end"
    _flush_progress(force=True)
    retrieval_meta["media_mirror_jobs_enqueued"] = mirror_jobs_enqueued
    retrieval_meta["scrape_counters"] = {"posts": post_count, "comments": comment_count}
    retrieval_meta["persist_counters"] = {"posts_upserted": matched_posts, "comments_upserted": comment_count}
    return post_count, comment_count, retrieval_meta


def _retry_backoff_seconds(attempt_count: int) -> int:
    return max(5, min(300, 5 * (2 ** max(0, attempt_count - 1))))


def _safe_percent(numerator: int | float, denominator: int | float, *, precision: int = 1) -> float | None:
    den = float(denominator)
    if den <= 0:
        return None
    num = max(0.0, float(numerator))
    pct = (num * 100.0) / den
    return round(min(100.0, pct), precision)


def _median_int(values: list[int]) -> int:
    if not values:
        return 0
    ordered = sorted(int(value) for value in values)
    mid = len(ordered) // 2
    if len(ordered) % 2 == 1:
        return ordered[mid]
    return int(round((ordered[mid - 1] + ordered[mid]) / 2.0))


def _normalize_job_error_code(
    *,
    raw_error_code: str | None,
    error_message: str | None,
    error_class: str | None,
) -> str:
    haystack = " ".join(
        token.strip().lower() for token in [raw_error_code or "", error_message or "", error_class or ""] if token
    )
    if not haystack:
        return "UNKNOWN"

    if any(marker in haystack for marker in ("429", "rate limit", "rate_limited", "throttle", "quota")):
        return "RATE_LIMIT"
    if "blocked" in haystack:
        return "BLOCKED"
    if any(
        marker in haystack
        for marker in (
            "auth",
            "unauthorized",
            "forbidden",
            "login",
            "cookie",
            "token",
            "credential",
            "permission",
        )
    ):
        return "AUTH"
    if any(
        marker in haystack
        for marker in (
            "json",
            "decode",
            "parse",
            "parser",
            "schema",
            "validation",
            "keyerror",
            "valueerror",
            "typeerror",
            "indexerror",
        )
    ):
        return "PARSER"
    if any(
        marker in haystack
        for marker in (
            "network",
            "connection",
            "timeout",
            "timed out",
            "dns",
            "ssl",
            "502",
            "503",
            "504",
            "refused",
            "reset",
            "unreachable",
            "fetch failed",
        )
    ):
        return "NETWORK"
    return "UNKNOWN"


def _classify_job_error(exc: Exception) -> tuple[str, str, bool]:
    message = str(exc).lower()
    error_class = exc.__class__.__name__
    transient_markers = (
        "timeout",
        "temporar",
        "connection",
        "network",
        "429",
        "502",
        "503",
        "504",
        "rate limit",
        "json",
        "decode",
        "unexpected",
        "login_required",
    )
    if any(marker in message for marker in transient_markers):
        return "transient_error", error_class, True
    return "fatal_error", error_class, False


def _run_platform_media_mirror_stage(
    *,
    context: SeasonContext,
    platform: str,
    job_id: str,
    config: dict[str, Any],
) -> tuple[int, int, dict[str, Any]]:
    normalized_platform = (platform or "").strip().lower()
    table = PLATFORM_POST_TABLES.get(normalized_platform)
    source_id_column = PLATFORM_SOURCE_ID_COLUMN.get(normalized_platform)
    posted_at_column = PLATFORM_POSTED_AT_COLUMN.get(normalized_platform)
    if not table or not source_id_column or not posted_at_column:
        raise ValueError(f"mirror_platform_not_supported:{platform}")

    post_id = str(config.get("post_id") or "").strip()
    if not post_id:
        raise ValueError("mirror_missing_post_id")

    thumbnail_expr = (
        "coalesce(nullif(p.media_urls ->> 0, ''), '') as thumbnail_url"
        if normalized_platform == "twitter"
        else "coalesce(nullif(p.thumbnail_url, ''), '') as thumbnail_url"
    )
    media_urls_expr = (
        "p.media_urls as media_urls"
        if _platform_posts_has_column(normalized_platform, "media_urls")
        else "'[]'::jsonb as media_urls"
    )
    username_expr = (
        "coalesce(nullif(p.channel_title, ''), nullif(p.source_account, ''), '') as post_username"
        if normalized_platform == "youtube"
        else "coalesce(nullif(p.username, ''), nullif(p.source_account, ''), '') as post_username"
    )

    post_row = pg.fetch_one(
        f"""
        select
          p.id::text as id,
          p.{source_id_column} as source_id,
          {thumbnail_expr},
          {media_urls_expr},
          {username_expr},
          coalesce(to_jsonb(p) -> 'raw_data', '{{}}'::jsonb) as raw_data,
          p.{posted_at_column} as posted_at,
          coalesce(to_jsonb(p) ->> 'hosted_thumbnail_url', '') as hosted_thumbnail_url,
          coalesce(to_jsonb(p) -> 'hosted_media_urls', '[]'::jsonb) as hosted_media_urls,
          coalesce(to_jsonb(p) ->> 'media_mirror_status', '') as media_mirror_status,
          coalesce(to_jsonb(p) ->> 'media_mirror_error', '') as media_mirror_error
        from social.{table} p
        where p.season_id = %s
          and p.id = %s::uuid
        """,
        [context.season_id, post_id],
    )
    if not post_row:
        raise ValueError(f"mirror_post_not_found:{post_id}")

    source_id = str(post_row.get("source_id") or "").strip()
    source_thumbnail_url, source_media_urls = _platform_post_source_urls(normalized_platform, post_row)

    # Compute structured display name for S3 file naming
    _show_slug = _resolve_show_slug(context)
    _post_username = _resolve_post_username(normalized_platform, post_row)
    _posted_at_dt = _coerce_dt(post_row.get("posted_at"))
    _post_number = (
        _compute_daily_post_number(normalized_platform, context.season_id, _post_username, _posted_at_dt, source_id)
        if _posted_at_dt
        else 1
    )
    _display_name = _build_post_display_name(
        show_slug=_show_slug,
        username=_post_username,
        platform=normalized_platform,
        post_number=_post_number,
    )

    mirror_selected_source: str | None = None
    mirror_attempts: list[dict[str, Any]] = []
    mirror_status = str(post_row.get("media_mirror_status") or "").strip().lower()
    raw_data = post_row.get("raw_data") if isinstance(post_row.get("raw_data"), dict) else {}
    source_media_needs_quality_repair = _source_media_urls_need_quality_repair(
        platform=normalized_platform,
        source_media_urls=source_media_urls,
    )
    if normalized_platform == "instagram":
        media_meta = (raw_data or {}).get("media_retrieval_meta") if isinstance(raw_data, dict) else None
        if isinstance(media_meta, dict):
            mirror_selected_source = str(media_meta.get("selected_source") or "") or None
            mirror_attempts = _normalize_instagram_mirror_attempts(media_meta.get("attempts"))

        needs_re_resolve = (not source_thumbnail_url and not source_media_urls) or mirror_status in {
            "failed",
            "partial",
        } or source_media_needs_quality_repair
        if needs_re_resolve and source_id:
            try:
                resolution_payload = _resolve_instagram_media_for_shortcode(shortcode=source_id)
                resolved_media_urls = _normalize_unique_terms(
                    [str(url).strip() for url in (resolution_payload.get("media_urls") or []) if str(url).strip()]
                )
                resolved_thumbnail_url = str(resolution_payload.get("thumbnail_url") or "").strip()
                mirror_selected_source = str(resolution_payload.get("source") or "") or mirror_selected_source
                mirror_attempts = _normalize_instagram_mirror_attempts(resolution_payload.get("attempts"))
                if resolved_thumbnail_url or resolved_media_urls:
                    source_thumbnail_url = resolved_thumbnail_url or (
                        resolved_media_urls[0] if resolved_media_urls else ""
                    )
                    source_media_urls = resolved_media_urls
                    _update_instagram_post_source_media_fields(
                        post_id=post_id,
                        thumbnail_url=source_thumbnail_url or FIELD_UNSET,
                        media_urls=source_media_urls or FIELD_UNSET,
                    )
            except Exception as exc:  # noqa: BLE001
                if not mirror_attempts:
                    mirror_attempts = [
                        {
                            "source": "api_media_info",
                            "success": False,
                            "reason_code": "instagram_api_failed",
                            "http_status": None,
                            "selected_url_count": 0,
                            "error_type": exc.__class__.__name__,
                            "error_message": str(exc)[:240],
                        }
                    ]
    elif normalized_platform == "tiktok":
        needs_re_resolve = (not source_media_urls) or mirror_status in {
            "failed",
            "partial",
        } or source_media_needs_quality_repair
        if needs_re_resolve and source_id:
            try:
                canonical_url = str(raw_data.get("url") or "").strip() if isinstance(raw_data, dict) else ""
                if not canonical_url and isinstance(raw_data, dict):
                    username = str(raw_data.get("username") or "").strip().lstrip("@")
                    if username:
                        canonical_url = f"https://www.tiktok.com/@{username}/video/{source_id}"
                allow_ytdlp = bool(source_media_needs_quality_repair or not source_media_urls)
                resolution_payload = _resolve_tiktok_media_for_video_id(
                    video_id=source_id,
                    canonical_url=canonical_url or None,
                    allow_ytdlp=allow_ytdlp,
                    validate_download_url=False,
                )
                resolved_media_urls = _normalize_unique_terms(
                    [str(url).strip() for url in (resolution_payload.get("media_urls") or []) if str(url).strip()]
                )
                resolved_thumbnail_url = str(resolution_payload.get("thumbnail_url") or "").strip()
                mirror_selected_source = str(resolution_payload.get("source") or "") or mirror_selected_source
                mirror_attempts = _normalize_instagram_mirror_attempts(resolution_payload.get("attempts"))
                if resolved_thumbnail_url:
                    source_thumbnail_url = resolved_thumbnail_url
                if allow_ytdlp and canonical_url:
                    # Prefer canonical page URL so media download can use yt-dlp fallback.
                    source_media_urls = [canonical_url]
                elif resolved_media_urls:
                    source_media_urls = resolved_media_urls
                elif canonical_url:
                    source_media_urls = [canonical_url]
            except Exception as exc:  # noqa: BLE001
                if not mirror_attempts:
                    mirror_attempts = [
                        {
                            "source": "watch_page_json",
                            "success": False,
                            "reason_code": "tiktok_media_resolve_failed",
                            "http_status": None,
                            "selected_url_count": 0,
                            "error_type": exc.__class__.__name__,
                            "error_message": str(exc)[:240],
                        }
                    ]
    elif normalized_platform == "youtube":
        needs_re_resolve = (not source_media_urls) or mirror_status in {
            "failed",
            "partial",
        } or source_media_needs_quality_repair
        if needs_re_resolve and source_id:
            try:
                resolution_payload = _resolve_youtube_media_for_video_id(video_id=source_id)
                resolved_media_urls = _normalize_unique_terms(
                    [str(url).strip() for url in (resolution_payload.get("media_urls") or []) if str(url).strip()]
                )
                resolved_thumbnail_url = str(resolution_payload.get("thumbnail_url") or "").strip()
                mirror_selected_source = str(resolution_payload.get("source") or "") or mirror_selected_source
                mirror_attempts = _normalize_instagram_mirror_attempts(resolution_payload.get("attempts"))
                if resolved_thumbnail_url:
                    source_thumbnail_url = resolved_thumbnail_url
                canonical_watch_url = f"https://www.youtube.com/watch?v={source_id}" if source_id else ""
                if resolved_media_urls and not _source_media_urls_need_quality_repair(
                    platform="youtube",
                    source_media_urls=resolved_media_urls,
                ):
                    source_media_urls = resolved_media_urls
                elif canonical_watch_url:
                    source_media_urls = [canonical_watch_url]
            except Exception as exc:  # noqa: BLE001
                if not mirror_attempts:
                    mirror_attempts = [
                        {
                            "source": "watch_page_streaming_data",
                            "success": False,
                            "reason_code": "youtube_media_resolve_failed",
                            "http_status": None,
                            "selected_url_count": 0,
                            "error_type": exc.__class__.__name__,
                            "error_message": str(exc)[:240],
                        }
                    ]
    elif normalized_platform == "twitter":
        source_has_video = any(_is_video_like_media_url(url) for url in source_media_urls)
        source_has_only_thumb_placeholders = bool(source_media_urls) and all(
            _is_twitter_thumb_placeholder_url(url) for url in source_media_urls
        )
        needs_re_resolve = (
            (not source_media_urls)
            or (mirror_status in {"failed", "partial"})
            or (source_has_only_thumb_placeholders and not source_has_video)
            or source_media_needs_quality_repair
        )
        if needs_re_resolve and source_id:
            try:
                from trr_backend.socials.twitter import TwitterScraper

                twitter_cookies, twitter_bearer = _load_twitter_auth()
                twikit_creds = _load_twikit_credentials()
                if not twitter_cookies.get("ct0") and twikit_creds:
                    if twikit_creds.get("auth_token") and twikit_creds.get("ct0"):
                        twitter_cookies = {
                            **twitter_cookies,
                            "auth_token": twikit_creds["auth_token"],
                            "ct0": twikit_creds["ct0"],
                        }
                scraper = TwitterScraper(
                    cookies=twitter_cookies,
                    bearer_token=twitter_bearer,
                    twikit_credentials=twikit_creds,
                )
                summary = scraper.fetch_public_tweet_summary(source_id, delay=0.0) or {}
                resolved_media_urls = _merge_twitter_media_urls(
                    source_media_urls,
                    list(summary.get("media_urls") or []),
                )
                if resolved_media_urls:
                    source_media_urls = resolved_media_urls
                    source_thumbnail_url = _select_thumbnail_candidate(
                        resolved_media_urls,
                        fallback=source_thumbnail_url,
                    )
                    mirror_selected_source = "public_tweet_summary"
                    mirror_attempts = [
                        {
                            "source": "public_tweet_summary",
                            "success": True,
                            "reason_code": None,
                            "http_status": None,
                            "selected_url_count": len(resolved_media_urls),
                            "error_type": None,
                            "error_message": None,
                        }
                    ]
            except Exception as exc:  # noqa: BLE001
                if not mirror_attempts:
                    mirror_attempts = [
                        {
                            "source": "public_tweet_summary",
                            "success": False,
                            "reason_code": "twitter_media_resolve_failed",
                            "http_status": None,
                            "selected_url_count": 0,
                            "error_type": exc.__class__.__name__,
                            "error_message": str(exc)[:240],
                        }
                    ]
    elif normalized_platform == "threads":
        needs_re_resolve = (not source_media_urls) or (mirror_status in {"failed", "partial"}) or source_media_needs_quality_repair
        if needs_re_resolve and source_id:
            try:
                from trr_backend.socials.threads import resolve_threads_media

                raw_data = config.get("_raw_data") or {}
                resolution = resolve_threads_media(raw_data, validate_urls=False)
                if resolution.media_urls:
                    source_media_urls = resolution.media_urls
                    source_thumbnail_url = resolution.thumbnail_url or _select_thumbnail_candidate(
                        resolution.media_urls,
                        fallback=source_thumbnail_url,
                    )
                    mirror_selected_source = resolution.source or "threads_graphql_post_data"
                    mirror_attempts = [a for a in resolution.attempts if a.get("success")]
                    if not mirror_attempts:
                        mirror_attempts = resolution.attempts[:1] or [
                            {
                                "source": "threads_graphql_post_data",
                                "success": True,
                                "reason_code": None,
                                "http_status": None,
                                "selected_url_count": len(resolution.media_urls),
                                "error_type": None,
                                "error_message": None,
                            }
                        ]
            except Exception as exc:  # noqa: BLE001
                if not mirror_attempts:
                    mirror_attempts = [
                        {
                            "source": "threads_graphql_post_data",
                            "success": False,
                            "reason_code": "threads_media_resolve_failed",
                            "http_status": None,
                            "selected_url_count": 0,
                            "error_type": exc.__class__.__name__,
                            "error_message": str(exc)[:240],
                        }
                    ]
    attempt_count = max(1, _normalize_non_negative_int(config.get("_attempt_count")))
    now_utc = _now_utc()
    _update_platform_post_media_mirror_fields(
        platform=normalized_platform,
        post_id=post_id,
        media_mirror_status="pending",
        media_mirror_last_attempt_at=now_utc,
        media_mirror_attempt_count=attempt_count,
        media_mirror_last_job_id=job_id,
    )

    source_media_still_needs_quality_repair = _source_media_urls_need_quality_repair(
        platform=normalized_platform,
        source_media_urls=source_media_urls,
    )
    source_media_supports_ytdlp = _source_media_urls_support_ytdlp_mirror(
        platform=normalized_platform,
        source_media_urls=source_media_urls,
    )
    missing_source_media = (not source_thumbnail_url and not source_media_urls) or (
        source_media_still_needs_quality_repair and not source_media_supports_ytdlp
    )
    youtube_media_missing = normalized_platform == "youtube" and (
        not source_media_urls or (source_media_still_needs_quality_repair and not source_media_supports_ytdlp)
    )
    if missing_source_media or youtube_media_missing:
        if source_media_still_needs_quality_repair:
            missing_reason = f"{normalized_platform}_media_unresolved"
        elif normalized_platform == "instagram":
            missing_reason = "instagram_media_not_found"
        elif normalized_platform == "tiktok":
            missing_reason = "tiktok_media_not_found"
        elif normalized_platform == "youtube":
            missing_reason = "youtube_media_not_found"
        elif normalized_platform == "threads":
            missing_reason = "threads_media_not_found"
        else:
            missing_reason = "no_source_media"
        if normalized_platform == "instagram" and not mirror_attempts:
            mirror_attempts = [
                {
                    "source": "api_media_info",
                    "success": False,
                    "reason_code": "instagram_media_not_found",
                    "http_status": None,
                    "selected_url_count": 0,
                    "error_type": None,
                    "error_message": None,
                }
            ]
        if normalized_platform == "youtube" and not mirror_attempts:
            mirror_attempts = [
                {
                    "source": "watch_page_streaming_data",
                    "success": False,
                    "reason_code": "youtube_media_not_found",
                    "http_status": None,
                    "selected_url_count": 0,
                    "error_type": None,
                    "error_message": None,
                }
            ]
        if normalized_platform == "tiktok" and not mirror_attempts:
            mirror_attempts = [
                {
                    "source": "watch_page_json",
                    "success": False,
                    "reason_code": "tiktok_media_not_found",
                    "http_status": None,
                    "selected_url_count": 0,
                    "error_type": None,
                    "error_message": None,
                }
            ]
        _update_platform_post_media_mirror_fields(
            platform=normalized_platform,
            post_id=post_id,
            media_mirror_status="failed",
            media_mirror_error=missing_reason,
            media_mirror_last_attempt_at=now_utc,
            media_mirror_attempt_count=attempt_count,
            media_mirror_last_job_id=job_id,
        )
        return (
            1,
            0,
            {
                "activity": {"phase": "media_mirror_end", "last_progress_at": _iso(now_utc)},
                "mirror": {
                    "status": "failed",
                    "error": missing_reason,
                    "selected_source": mirror_selected_source,
                    "attempts": mirror_attempts,
                },
            },
        )

    week_index_raw = config.get("week_index")
    week_index: int | None = None
    if week_index_raw is not None and str(week_index_raw).strip():
        try:
            week_index = int(week_index_raw)
        except (TypeError, ValueError):
            week_index = None

    if week_index is None:
        posted_at = _coerce_dt(post_row.get("posted_at"))
        if posted_at is not None:
            try:
                windows, _ = _resolve_week_windows(
                    context,
                    timezone="America/New_York",
                    source_scope=str(config.get("source_scope") or "bravo"),
                    now_utc=_now_utc(),
                )
                week_window = _week_for_timestamp(posted_at, windows=windows, timezone="America/New_York")
                week_index = week_window.week_index if week_window else None
            except Exception:  # noqa: BLE001
                week_index = None

    mirror_post = {
        "id": post_id,
        "shortcode": source_id,
        "video_id": source_id,
        "tweet_id": source_id,
        "thumbnail_url": source_thumbnail_url,
        "media_urls": source_media_urls,
    }
    result = _mirror_platform_media_to_s3_result(
        context,
        platform=normalized_platform,
        post=SimpleNamespace(**mirror_post),
        week_index=week_index,
        display_name=_display_name,
    )
    _update_platform_post_media_mirror_fields(
        platform=normalized_platform,
        post_id=post_id,
        hosted_thumbnail_url=(
            result.get("hosted_thumbnail_url") if result.get("hosted_thumbnail_url") is not None else FIELD_UNSET
        ),
        hosted_media_urls=(
            list(result.get("hosted_media_urls") or []) if result.get("hosted_media_urls") else FIELD_UNSET
        ),
        media_mirror_status=str(result.get("status") or "") or None,
        media_mirror_error=str(result.get("error") or "") or None,
        media_mirror_last_attempt_at=now_utc,
        media_mirror_attempt_count=attempt_count,
        media_mirror_last_job_id=job_id,
    )

    if bool(result.get("retryable_error")) and str(result.get("status") or "") in {"partial", "failed"}:
        raise RuntimeError(str(result.get("error") or "media_mirror_retryable"))

    # Profile picture mirroring (Instagram only, migration 0147)
    profile_pic_result: dict[str, Any] | None = None
    if normalized_platform == "instagram" and _instagram_posts_has_column("hosted_owner_profile_pic_url"):
        try:
            # Build a lightweight post object with the detail attributes from raw_data
            profile_post = SimpleNamespace(
                owner_detail=None,
                tagged_users_detail=[],
                collaborators_detail=[],
            )
            # Extract owner detail from raw_data
            rd = raw_data if isinstance(raw_data, dict) else {}
            owner_detail_raw = rd.get("owner_detail")
            if isinstance(owner_detail_raw, dict) and owner_detail_raw.get("username"):
                profile_post.owner_detail = SimpleNamespace(**owner_detail_raw)
            elif rd.get("username"):
                profile_post.owner_detail = SimpleNamespace(
                    username=str(rd.get("username") or ""),
                    profile_pic_url=None,
                    full_name=None,
                    user_id=None,
                    is_verified=None,
                )

            for attr in ("tagged_users_detail", "collaborators_detail"):
                detail_list = rd.get(attr)
                if isinstance(detail_list, list):
                    setattr(
                        profile_post,
                        attr,
                        [SimpleNamespace(**u) for u in detail_list if isinstance(u, dict) and u.get("username")],
                    )

            has_any_pic = (
                (profile_post.owner_detail and getattr(profile_post.owner_detail, "profile_pic_url", None))
                or any(getattr(u, "profile_pic_url", None) for u in profile_post.tagged_users_detail)
                or any(getattr(u, "profile_pic_url", None) for u in profile_post.collaborators_detail)
            )
            if has_any_pic:
                profile_pic_result = _mirror_instagram_profile_pics_for_post(post_row, post=profile_post)
                _update_instagram_profile_pic_mirror_fields(
                    post_id=post_id,
                    hosted_owner_profile_pic_url=profile_pic_result.get("hosted_owner_profile_pic_url"),
                    hosted_tagged_profile_pics=profile_pic_result.get("hosted_tagged_profile_pics"),
                    profile_pic_mirror_status=profile_pic_result.get("profile_pic_mirror_status"),
                    profile_pic_mirror_error=profile_pic_result.get("profile_pic_mirror_error"),
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to mirror Instagram profile pics for post %s: %s", post_id, exc)

    mirrored_assets = _normalize_non_negative_int(result.get("mirrored_count"))
    source_assets = _normalize_non_negative_int(result.get("source_count"))
    metadata = {
        "activity": {"phase": "media_mirror_end", "last_progress_at": _iso(now_utc)},
        "mirror": {
            "platform": normalized_platform,
            "post_id": post_id,
            "source_id": source_id,
            "status": str(result.get("status") or ""),
            "error": str(result.get("error") or "") or None,
            "mirrored_assets": mirrored_assets,
            "source_assets": source_assets,
            "week_index": week_index,
            "selected_source": mirror_selected_source,
            "attempts": mirror_attempts,
        },
    }
    if profile_pic_result:
        metadata["profile_pic_mirror"] = {
            "status": profile_pic_result.get("profile_pic_mirror_status"),
            "error": profile_pic_result.get("profile_pic_mirror_error"),
            "owner_mirrored": bool(profile_pic_result.get("hosted_owner_profile_pic_url")),
            "tagged_mirrored": len(profile_pic_result.get("hosted_tagged_profile_pics") or {}),
        }
    return 1, mirrored_assets, metadata


def _run_tiktok_comment_media_mirror_stage(
    *,
    context: SeasonContext,
    job_id: str,
    config: dict[str, Any],
) -> tuple[int, int, dict[str, Any]]:
    if not _tiktok_comments_has_column("media_urls"):
        return (
            0,
            0,
            {
                "activity": {"phase": "comment_media_mirror_end", "last_progress_at": _iso(_now_utc())},
                "comment_media_mirror": {
                    "status": "skipped",
                    "error": "tiktok_comment_media_columns_missing",
                },
            },
        )

    comment_id = str(config.get("comment_id") or "").strip()
    comment_db_id = str(config.get("comment_db_id") or "").strip()
    if not comment_id and not comment_db_id:
        raise ValueError("comment_media_mirror_missing_comment_id")

    comment_row = pg.fetch_one(
        """
        select
          c.id::text as id,
          c.comment_id,
          c.post_id::text as post_id,
          coalesce(to_jsonb(c) -> 'media_urls', '[]'::jsonb) as media_urls,
          coalesce(to_jsonb(c) -> 'hosted_media_urls', '[]'::jsonb) as hosted_media_urls,
          coalesce(to_jsonb(c) ->> 'media_mirror_status', '') as media_mirror_status,
          coalesce(to_jsonb(c) ->> 'media_mirror_error', '') as media_mirror_error,
          p.video_id as video_id,
          p.posted_at
        from social.tiktok_comments c
        left join social.tiktok_posts p on p.id = c.post_id
        where c.season_id = %s
          and (
            (%s <> '' and c.comment_id = %s)
            or (%s <> '' and c.id = nullif(%s, '')::uuid)
          )
        order by c.created_at desc nulls last
        limit 1
        """,
        [context.season_id, comment_id, comment_id, comment_db_id, comment_db_id],
    )
    if not comment_row:
        raise ValueError("comment_media_mirror_comment_not_found")

    if not _tiktok_comment_needs_media_mirror(comment_row):
        return (
            1,
            0,
            {
                "activity": {"phase": "comment_media_mirror_end", "last_progress_at": _iso(_now_utc())},
                "comment_media_mirror": {
                    "status": "up_to_date",
                    "comment_id": str(comment_row.get("comment_id") or ""),
                    "source_count": len(_as_text_list(comment_row.get("media_urls"))),
                    "mirrored_count": len(_as_text_list(comment_row.get("hosted_media_urls"))),
                },
            },
        )

    source_media_urls = _normalize_unique_terms(_as_text_list(comment_row.get("media_urls")))
    if not source_media_urls:
        _update_tiktok_comment_media_mirror_fields(
            comment_id=str(comment_row.get("comment_id") or ""),
            media_mirror_status="failed",
            media_mirror_error="tiktok_comment_media_not_found",
            media_mirror_last_attempt_at=_now_utc(),
            media_mirror_last_job_id=job_id,
        )
        return (
            1,
            0,
            {
                "activity": {"phase": "comment_media_mirror_end", "last_progress_at": _iso(_now_utc())},
                "comment_media_mirror": {
                    "status": "failed",
                    "error": "tiktok_comment_media_not_found",
                    "comment_id": str(comment_row.get("comment_id") or ""),
                    "source_count": 0,
                    "mirrored_count": 0,
                },
            },
        )

    attempt_count = max(1, _normalize_non_negative_int(config.get("_attempt_count")))
    now_utc = _now_utc()
    _update_tiktok_comment_media_mirror_fields(
        comment_id=str(comment_row.get("comment_id") or ""),
        media_mirror_status="pending",
        media_mirror_last_attempt_at=now_utc,
        media_mirror_attempt_count=attempt_count,
        media_mirror_last_job_id=job_id,
    )

    week_index_raw = config.get("week_index")
    week_index: int | None = None
    if week_index_raw is not None and str(week_index_raw).strip():
        try:
            week_index = int(week_index_raw)
        except (TypeError, ValueError):
            week_index = None
    if week_index is None:
        posted_at = _coerce_dt(comment_row.get("posted_at"))
        if posted_at is not None:
            try:
                windows, _ = _resolve_week_windows(
                    context,
                    timezone="America/New_York",
                    source_scope=str(config.get("source_scope") or "bravo"),
                    now_utc=_now_utc(),
                )
                week_window = _week_for_timestamp(posted_at, windows=windows, timezone="America/New_York")
                week_index = week_window.week_index if week_window else None
            except Exception:  # noqa: BLE001
                week_index = None

    mirror_comment = SimpleNamespace(
        id=str(comment_row.get("id") or ""),
        video_id=f"comment-{str(comment_row.get('comment_id') or '')}",
        thumbnail_url="",
        media_urls=source_media_urls,
    )
    result = _mirror_platform_media_to_s3_result(
        context,
        platform="tiktok",
        post=mirror_comment,
        week_index=week_index,
        prefer_first_media_as_thumbnail=False,
    )
    _update_tiktok_comment_media_mirror_fields(
        comment_id=str(comment_row.get("comment_id") or ""),
        hosted_media_urls=list(result.get("hosted_media_urls") or []),
        media_mirror_status=str(result.get("status") or "") or None,
        media_mirror_error=str(result.get("error") or "") or None,
        media_mirror_last_attempt_at=now_utc,
        media_mirror_attempt_count=attempt_count,
        media_mirror_last_job_id=job_id,
    )
    if bool(result.get("retryable_error")) and str(result.get("status") or "") in {"partial", "failed"}:
        raise RuntimeError(str(result.get("error") or "comment_media_mirror_retryable"))

    mirrored_assets = _normalize_non_negative_int(result.get("mirrored_count"))
    metadata = {
        "activity": {"phase": "comment_media_mirror_end", "last_progress_at": _iso(now_utc)},
        "comment_media_mirror": {
            "platform": "tiktok",
            "comment_id": str(comment_row.get("comment_id") or ""),
            "post_id": str(comment_row.get("post_id") or ""),
            "status": str(result.get("status") or ""),
            "error": str(result.get("error") or "") or None,
            "source_count": _normalize_non_negative_int(result.get("source_count")),
            "mirrored_count": mirrored_assets,
            "week_index": week_index,
        },
    }
    return 1, mirrored_assets, metadata


def _run_instagram_media_mirror_stage(
    *,
    context: SeasonContext,
    job_id: str,
    config: dict[str, Any],
) -> tuple[int, int, dict[str, Any]]:
    return _run_platform_media_mirror_stage(
        context=context,
        platform="instagram",
        job_id=job_id,
        config=config,
    )


def _run_platform_stage(
    *,
    context: SeasonContext,
    run_id: str | None,
    platform: str,
    stage: str,
    account: str,
    hashtags: list[str],
    keywords: list[str],
    opts: IngestOptions,
    job_id: str,
    config: dict[str, Any] | None = None,
) -> tuple[int, int, dict[str, Any]]:
    normalized_platform = (platform or "").strip().lower()
    if stage not in {"posts", "comments", INSTAGRAM_MEDIA_MIRROR_STAGE, COMMENT_MEDIA_MIRROR_STAGE}:
        raise ValueError(f"Unsupported ingest stage: {stage}")

    if stage == INSTAGRAM_MEDIA_MIRROR_STAGE:
        return _run_platform_media_mirror_stage(
            context=context,
            platform=normalized_platform,
            job_id=job_id,
            config=dict(config or {}),
        )
    if stage == COMMENT_MEDIA_MIRROR_STAGE:
        if normalized_platform == "tiktok":
            return _run_tiktok_comment_media_mirror_stage(
                context=context,
                job_id=job_id,
                config=dict(config or {}),
            )
        raise ValueError(f"Unsupported comment media mirror platform: {normalized_platform}")

    if stage == "posts":
        stage_opts = replace(opts, max_comments_per_post=0, fetch_replies=False)
        if normalized_platform == "instagram":
            return _ingest_instagram(
                context,
                run_id=run_id,
                account=account,
                hashtags=hashtags,
                keywords=keywords,
                opts=stage_opts,
                job_id=job_id,
                stage=stage,
            )
        if normalized_platform == "tiktok":
            return _ingest_tiktok(
                context,
                run_id=run_id,
                account=account,
                hashtags=hashtags,
                keywords=keywords,
                opts=stage_opts,
                job_id=job_id,
                stage=stage,
            )
        if normalized_platform == "youtube":
            return _ingest_youtube(
                context,
                run_id=run_id,
                account=account,
                hashtags=hashtags,
                keywords=keywords,
                opts=stage_opts,
                job_id=job_id,
                stage=stage,
            )
        if normalized_platform == "twitter":
            return _ingest_twitter(
                context,
                run_id=run_id,
                account=account,
                hashtags=hashtags,
                keywords=keywords,
                opts=stage_opts,
                job_id=job_id,
                include_reply_records=False,
                hydrate_audience_replies=False,
                stage=stage,
            )
        if normalized_platform == "facebook":
            return _ingest_facebook(
                context,
                run_id=run_id,
                account=account,
                hashtags=hashtags,
                keywords=keywords,
                opts=stage_opts,
                job_id=job_id,
                stage=stage,
            )
        if normalized_platform == "threads":
            return _ingest_threads(
                context,
                run_id=run_id,
                account=account,
                hashtags=hashtags,
                keywords=keywords,
                opts=stage_opts,
                job_id=job_id,
                stage=stage,
            )
    else:
        if opts.max_comments_per_post <= 0:
            return 0, 0, {}
        if normalized_platform == "instagram":
            posts, comments, meta = _ingest_instagram(
                context,
                run_id=run_id,
                account=account,
                hashtags=hashtags,
                keywords=keywords,
                opts=opts,
                job_id=job_id,
                stage=stage,
            )
            return posts, comments, meta
        if normalized_platform == "tiktok":
            posts, comments, meta = _ingest_tiktok(
                context,
                run_id=run_id,
                account=account,
                hashtags=hashtags,
                keywords=keywords,
                opts=opts,
                job_id=job_id,
                stage=stage,
            )
            return posts, comments, meta
        if normalized_platform == "youtube":
            posts, comments, meta = _ingest_youtube(
                context,
                run_id=run_id,
                account=account,
                hashtags=hashtags,
                keywords=keywords,
                opts=opts,
                job_id=job_id,
                stage=stage,
            )
            return posts, comments, meta
        if normalized_platform == "twitter":
            posts, comments, meta = _ingest_twitter(
                context,
                run_id=run_id,
                account=account,
                hashtags=hashtags,
                keywords=keywords,
                opts=opts,
                job_id=job_id,
                include_reply_records=False,
                hydrate_audience_replies=True,
                stage=stage,
            )
            return posts, comments, meta
        if normalized_platform == "facebook":
            posts, comments, meta = _ingest_facebook(
                context,
                run_id=run_id,
                account=account,
                hashtags=hashtags,
                keywords=keywords,
                opts=opts,
                job_id=job_id,
                stage=stage,
            )
            return posts, comments, meta
        if normalized_platform == "threads":
            posts, comments, meta = _ingest_threads(
                context,
                run_id=run_id,
                account=account,
                hashtags=hashtags,
                keywords=keywords,
                opts=opts,
                job_id=job_id,
                stage=stage,
            )
            return posts, comments, meta

    raise RuntimeError(f"Platform {normalized_platform} ingest is not supported")


def _run_platform_stage_via_crawlee(
    *,
    context: SeasonContext,
    run_id: str | None,
    platform: str,
    stage: str,
    account: str,
    hashtags: list[str],
    keywords: list[str],
    opts: IngestOptions,
    job_id: str,
    config: dict[str, Any],
    runtime_config: Any,
    auth_preflight: AuthPreflightResult,
) -> tuple[int, int, dict[str, Any]]:
    normalized_platform = (platform or "").strip().lower()

    def runner() -> tuple[int, int, dict[str, Any]]:
        return _run_platform_stage(
            context=context,
            run_id=run_id,
            platform=normalized_platform,
            stage=stage,
            account=account,
            hashtags=hashtags,
            keywords=keywords,
            opts=opts,
            job_id=job_id,
            config=config,
        )

    if normalized_platform == "instagram":
        from trr_backend.socials.instagram.crawlee_adapter import run_stage_with_crawlee

        return run_stage_with_crawlee(
            stage=stage,
            account=account,
            config=config,
            runtime_config=runtime_config,
            auth_preflight=auth_preflight,
            stage_runner=runner,
        )
    if normalized_platform == "tiktok":
        from trr_backend.socials.tiktok.crawlee_adapter import run_stage_with_crawlee

        return run_stage_with_crawlee(
            stage=stage,
            account=account,
            config=config,
            runtime_config=runtime_config,
            auth_preflight=auth_preflight,
            stage_runner=runner,
        )
    if normalized_platform == "twitter":
        from trr_backend.socials.twitter.crawlee_adapter import run_stage_with_crawlee

        return run_stage_with_crawlee(
            stage=stage,
            account=account,
            config=config,
            runtime_config=runtime_config,
            auth_preflight=auth_preflight,
            stage_runner=runner,
        )
    if normalized_platform == "youtube":
        from trr_backend.socials.youtube.crawlee_adapter import run_stage_with_crawlee

        return run_stage_with_crawlee(
            stage=stage,
            account=account,
            config=config,
            runtime_config=runtime_config,
            auth_preflight=auth_preflight,
            stage_runner=runner,
        )
    if normalized_platform == "facebook":
        from trr_backend.socials.facebook.crawlee_adapter import run_stage_with_crawlee

        return run_stage_with_crawlee(
            stage=stage,
            account=account,
            config=config,
            runtime_config=runtime_config,
            auth_preflight=auth_preflight,
            stage_runner=runner,
        )
    if normalized_platform == "threads":
        from trr_backend.socials.threads.crawlee_adapter import run_stage_with_crawlee

        return run_stage_with_crawlee(
            stage=stage,
            account=account,
            config=config,
            runtime_config=runtime_config,
            auth_preflight=auth_preflight,
            stage_runner=runner,
        )
    raise RuntimeError(f"Platform {normalized_platform} ingest is not supported")


def _claim_next_job(
    *,
    worker_id: str | None = None,
    run_id: str | None = None,
    stage: str | None = None,
    platform: str | None = None,
) -> dict[str, Any] | None:
    recover_stale_running_jobs(run_id=run_id, stage=stage, platform=platform, limit=25)
    return pg.fetch_one(
        """
        with candidate as (
          select id
          from social.scrape_jobs
          where status in ('queued', 'pending', 'retrying')
            and available_at <= now()
            and (%s::uuid is null or run_id = %s::uuid)
            and (
              %s::text is null
              or coalesce(config->>'stage', metadata->>'stage', job_type) = %s::text
            )
            and (%s::text is null or platform = %s::text)
            and (
              worker_id is null
              or worker_id = %s::text
              or (worker_id is not null and %s::text is not null and %s::text like worker_id || '%%')
            )
          order by priority asc, created_at asc
          for update skip locked
          limit 1
        )
        update social.scrape_jobs as j
        set
          status = 'running',
          started_at = coalesce(j.started_at, now()),
          claimed_at = now(),
          heartbeat_at = now(),
          worker_id = coalesce(%s, j.worker_id),
          attempt_count = j.attempt_count + 1
        from candidate
        where j.id = candidate.id
        returning
          j.id::text,
          j.run_id::text as run_id,
          j.platform,
          j.job_type,
          j.status,
          j.config,
          j.metadata,
          j.attempt_count,
          j.max_attempts,
          j.source_scope,
          j.season_id::text as season_id
        """,
        [run_id, run_id, stage, stage, platform, platform, worker_id, worker_id, worker_id, worker_id],
    )


def _execute_claimed_job(job: dict[str, Any], *, worker_id: str | None = None) -> dict[str, Any]:
    job_id = str(job.get("id") or "")
    run_id = str(job.get("run_id") or "")
    platform = _normalize_platform_name(job.get("platform"))
    config = dict(job.get("config") or {})
    config["_attempt_count"] = max(1, int(job.get("attempt_count") or 1))
    config["_max_attempts"] = max(1, int(job.get("max_attempts") or 1))
    stage = str(config.get("stage") or ((job.get("metadata") or {}).get("stage")) or "posts")
    context = get_season_context(str(config.get("season_id") or job.get("season_id") or ""))
    if platform not in SUPPORTED_PLATFORMS:
        raise ValueError(f"Unsupported platform in queued job: {platform}")
    sync_strategy = str(config.get("sync_strategy") or "incremental").strip().lower()
    if sync_strategy not in SUPPORTED_SYNC_STRATEGIES:
        sync_strategy = "incremental"
    raw_max_posts = config.get("max_posts_per_target")
    raw_max_comments = config.get("max_comments_per_post")
    raw_max_replies = config.get("max_replies_per_post")
    raw_mode = str(config.get("ingest_mode") or "posts_and_comments")
    raw_comment_refresh_policy = config.get("comment_refresh_policy")
    raw_comment_anchor_source_ids = config.get("comment_anchor_source_ids")
    raw_fetch_replies = bool(config.get("fetch_replies", True))
    resolved_posts, resolved_comments, resolved_replies, resolved_fetch_replies = _resolve_depth_defaults(
        max_posts_per_target=max(0, int(raw_max_posts or 1000)),
        max_comments_per_post=(None if raw_max_comments is None else max(0, int(raw_max_comments))),
        max_replies_per_post=(None if raw_max_replies is None else max(0, int(raw_max_replies))),
        fetch_replies=raw_fetch_replies,
    )
    normalized_mode = (raw_mode or "posts_and_comments").strip().lower()
    if normalized_mode == "posts_only":
        resolved_comments = 0
        resolved_replies = 0
        resolved_fetch_replies = False
    elif normalized_mode == "comments_only":
        resolved_posts = 0
    normalized_comment_refresh_policy = _normalize_comment_refresh_policy(raw_comment_refresh_policy)
    normalized_comment_anchor_source_ids: dict[str, set[str]] = {}
    if isinstance(raw_comment_anchor_source_ids, list):
        normalized_source_ids = {
            str(item or "").strip() for item in raw_comment_anchor_source_ids if str(item or "").strip()
        }
        if len(normalized_source_ids) <= MAX_COMMENT_ANCHOR_SOURCE_IDS_PER_PLATFORM:
            normalized_comment_anchor_source_ids[platform] = normalized_source_ids
    elif isinstance(raw_comment_anchor_source_ids, dict):
        normalized_comment_anchor_source_ids, _ = _normalize_comment_anchor_source_ids(
            raw_comment_anchor_source_ids,
            allowed_platforms={platform},
        )
    opts = IngestOptions(
        platforms=None,
        source_scope=str(config.get("source_scope") or job.get("source_scope") or "bravo"),
        sync_strategy=sync_strategy,
        max_posts_per_target=resolved_posts,
        max_comments_per_post=resolved_comments,
        max_replies_per_post=resolved_replies,
        fetch_replies=resolved_fetch_replies,
        ingest_mode=normalized_mode,
        date_start=_coerce_dt(config.get("date_start")),
        date_end=_coerce_dt(config.get("date_end")),
        comment_refresh_policy=normalized_comment_refresh_policy,
        comment_anchor_source_ids=normalized_comment_anchor_source_ids or None,
    )
    account = str(config.get("account") or "")
    hashtags = [str(item).strip().lstrip("#") for item in (config.get("hashtags") or []) if str(item).strip()]
    keywords = [str(item).strip() for item in (config.get("keywords") or []) if str(item).strip()]
    use_crawlee = should_use_crawlee(platform) and stage in {"posts", "comments"}
    crawlee_active_execution = False
    crawlee_fallback_to_legacy = False
    runtime_config = build_runtime_config(platform)
    auth_preflight: AuthPreflightResult | None = None

    try:
        _touch_job_heartbeat(job_id, worker_id=worker_id)
        if use_crawlee:
            twitter_cookies: dict[str, str] | None = None
            twitter_bearer: str | None = None
            if platform == "twitter":
                twitter_cookies, twitter_bearer = _load_twitter_auth()
            auth_preflight = check_platform_auth(
                platform=platform,
                instagram_cookies=_load_instagram_cookies() if platform == "instagram" else None,
                tiktok_cookies=_load_tiktok_cookies() if platform == "tiktok" else None,
                facebook_cookies=_load_facebook_cookies() if platform == "facebook" else None,
                threads_cookies=_load_threads_cookies() if platform == "threads" else None,
                twitter_cookies=twitter_cookies,
                twitter_bearer=twitter_bearer,
                twikit_credentials=_load_twikit_credentials() if platform == "twitter" else None,
            )
            if not auth_preflight.ok:
                strict_auth = runtime_config.auth_strict or is_auth_strict_for_platform(platform)
                if platform == "instagram" and not strict_auth:
                    crawlee_fallback_to_legacy = True
                    posts_count, comments_count, retrieval_meta = _run_platform_stage(
                        context=context,
                        run_id=run_id or None,
                        platform=platform,
                        stage=stage,
                        account=account,
                        hashtags=hashtags,
                        keywords=keywords,
                        opts=opts,
                        job_id=job_id,
                        config=config,
                    )
                    retrieval_meta = dict(retrieval_meta or {})
                    retrieval_meta["crawler_runtime"] = {
                        "engine": "legacy_fallback_after_auth_preflight",
                        "platform": platform,
                        "stage": stage,
                        "request_mode": normalized_mode,
                        "crawlee_available": True,
                        "max_concurrency": runtime_config.max_concurrency,
                        "max_retries": runtime_config.max_retries,
                        "requests_total": 0,
                        "requests_handled": 0,
                        "retries_total": 0,
                        "blocked_events": 0,
                        "session_rotations": 0,
                        "max_proxy_tier_used": 0,
                        "crawlee_request_count": 0,
                        "crawlee_retry_count": 0,
                        "crawlee_session_pool_used": False,
                    }
                    retrieval_meta["auth_context"] = build_auth_context(
                        auth_preflight,
                        fallback_to_legacy=True,
                    )
                else:
                    raise AuthPreflightError(auth_preflight)
            else:
                crawlee_active_execution = True
                posts_count, comments_count, retrieval_meta = _run_platform_stage_via_crawlee(
                    context=context,
                    run_id=run_id or None,
                    platform=platform,
                    stage=stage,
                    account=account,
                    hashtags=hashtags,
                    keywords=keywords,
                    opts=opts,
                    job_id=job_id,
                    config=config,
                    runtime_config=runtime_config,
                    auth_preflight=auth_preflight,
                )
        else:
            posts_count, comments_count, retrieval_meta = _run_platform_stage(
                context=context,
                run_id=run_id or None,
                platform=platform,
                stage=stage,
                account=account,
                hashtags=hashtags,
                keywords=keywords,
                opts=opts,
                job_id=job_id,
                config=config,
            )
        persist_counters = {
            "posts_upserted": _normalize_non_negative_int(
                (retrieval_meta.get("persist_counters") or {}).get("posts_upserted")
            ),
            "comments_upserted": _normalize_non_negative_int(
                (retrieval_meta.get("persist_counters") or {}).get("comments_upserted")
            ),
        }
        activity = dict(retrieval_meta.get("activity") or {})
        if not activity:
            activity = {"phase": f"{stage}_end", "last_progress_at": _iso(_now_utc())}
        fetch_counters = _comment_stats_payload(dict(retrieval_meta.get("comment_stats") or {}))
        crawler_runtime = dict(retrieval_meta.get("crawler_runtime") or {})
        auth_context = dict(retrieval_meta.get("auth_context") or {})
        base_metadata = {
            "stage": stage,
            "stage_counters": {"posts": posts_count, "comments": comments_count},
            "persist_counters": persist_counters,
            "activity": activity,
            "fetch_counters": fetch_counters,
            "platform": platform,
            "account": account,
            "crawler_runtime": crawler_runtime,
            "auth_context": auth_context,
            "retrieval_meta": retrieval_meta,
        }
        run_is_cancelled = False
        if run_id:
            run_state = pg.fetch_one("select status from social.scrape_runs where id = %s", [run_id]) or {}
            run_is_cancelled = str(run_state.get("status") or "") == "cancelled"
        if run_is_cancelled:
            _finish_job(
                job_id,
                status="cancelled",
                items_found=posts_count + comments_count,
                metadata=base_metadata,
            )
            return (
                pg.fetch_one(
                    """
                select
                  id::text,
                  run_id::text as run_id,
                  platform,
                  job_type,
                  status,
                  items_found,
                  error_message,
                  metadata
                from social.scrape_jobs
                where id = %s
                """,
                    [job_id],
                )
                or {}
            )
        _finish_job(
            job_id,
            status="completed",
            items_found=posts_count + comments_count,
            metadata=base_metadata,
        )
    except Exception as exc:  # noqa: BLE001
        run_is_cancelled = False
        if run_id:
            run_state = pg.fetch_one("select status from social.scrape_runs where id = %s", [run_id]) or {}
            run_is_cancelled = str(run_state.get("status") or "") == "cancelled"
        if run_is_cancelled:
            _finish_job(
                job_id,
                status="cancelled",
                items_found=0,
                error_message="Cancelled by user request",
                metadata={
                    "stage": stage,
                    "platform": platform,
                    "account": account,
                },
            )
            if run_id:
                _finalize_run_status(run_id)
            return (
                pg.fetch_one(
                    """
                select
                  id::text,
                  run_id::text as run_id,
                  platform,
                  job_type,
                  status,
                  items_found,
                  error_message,
                  metadata
                from social.scrape_jobs
                where id = %s
                """,
                    [job_id],
                )
                or {}
            )
        runtime_error_code = str(getattr(exc, "error_code", "") or "").strip().lower()
        runtime_error_class = str(getattr(exc, "error_class", "") or "").strip()
        runtime_retryable = getattr(exc, "retryable", None)
        runtime_metadata = getattr(exc, "runtime_metadata", None)
        if runtime_error_code:
            error_code = runtime_error_code
            error_class = runtime_error_class or exc.__class__.__name__
            transient = (
                bool(runtime_retryable)
                if runtime_retryable is not None
                else error_code
                in {
                    "blocked",
                    "rate_limited",
                    "network",
                }
            )
        elif crawlee_active_execution:
            error_code, error_class, transient = classify_exception(exc)
        else:
            error_code, error_class, transient = _classify_job_error(exc)
        attempt_count = int(job.get("attempt_count") or 1)
        max_attempts = int(job.get("max_attempts") or 1)
        can_retry = transient and attempt_count < max_attempts
        next_available_at = _now_utc() + timedelta(seconds=_retry_backoff_seconds(attempt_count)) if can_retry else None
        existing_job = (
            pg.fetch_one(
                "select items_found, metadata from social.scrape_jobs where id = %s",
                [job_id],
            )
            or {}
        )
        existing_metadata = dict(existing_job.get("metadata") or {})
        existing_activity = dict(existing_metadata.get("activity") or {})
        existing_activity["phase"] = "failed"
        existing_activity["last_progress_at"] = _iso(_now_utc())
        existing_metadata.update(
            {
                "stage": stage,
                "platform": platform,
                "account": account,
                "error": str(exc),
                "error_code": error_code,
                "error_class": error_class,
                "job_error_code": _normalize_job_error_code(
                    raw_error_code=error_code,
                    error_message=str(exc),
                    error_class=error_class,
                ),
                "failure_reason_code": error_code,
                "retryable": can_retry,
                "activity": existing_activity,
            }
        )
        if isinstance(runtime_metadata, dict):
            if isinstance(runtime_metadata.get("crawler_runtime"), dict):
                existing_metadata["crawler_runtime"] = runtime_metadata["crawler_runtime"]
            if isinstance(runtime_metadata.get("auth_context"), dict):
                existing_metadata["auth_context"] = runtime_metadata["auth_context"]
        elif auth_preflight is not None:
            existing_metadata["auth_context"] = build_auth_context(
                auth_preflight,
                fallback_to_legacy=crawlee_fallback_to_legacy,
            )
        _finish_job(
            job_id,
            status="retrying" if can_retry else "failed",
            items_found=_normalize_non_negative_int(existing_job.get("items_found")),
            error_message=str(exc),
            metadata=existing_metadata,
            last_error_code=error_code,
            last_error_class=error_class,
            next_available_at=next_available_at,
        )
        logger.exception("Social ingest job failed: job_id=%s stage=%s platform=%s", job_id, stage, platform)
    finally:
        if run_id:
            _finalize_run_status(run_id)

    return (
        pg.fetch_one(
            """
        select
          id::text,
          run_id::text as run_id,
          platform,
          job_type,
          status,
          items_found,
          error_message,
          metadata
        from social.scrape_jobs
        where id = %s
        """,
            [job_id],
        )
        or {}
    )


def _fetch_next_preclaimed_job(
    *,
    worker_id: str,
    run_id: str,
    stage: str | None = None,
    platform: str | None = None,
) -> dict[str, Any] | None:
    """Fetch the next pre-claimed running job owned by this worker (no status change needed)."""
    return pg.fetch_one(
        """
        select
          id::text,
          run_id::text as run_id,
          platform,
          job_type,
          status,
          config,
          metadata,
          attempt_count,
          max_attempts,
          source_scope,
          season_id::text as season_id
        from social.scrape_jobs
        where status = 'running'
          and run_id = %s::uuid
          and worker_id = %s
          and (
            %s::text is null
            or coalesce(config->>'stage', metadata->>'stage', job_type) = %s::text
          )
          and (%s::text is null or platform = %s::text)
        order by priority asc, created_at asc
        limit 1
        """,
        [run_id, worker_id, stage, stage, platform, platform],
    )


def execute_run(
    run_id: str,
    *,
    worker_id: str | None = None,
    stage: str | None = None,
    platform: str | None = None,
) -> dict[str, Any]:
    platform = _normalize_platform_name(platform)
    _set_run_status(run_id, "running")
    while True:
        recover_stale_running_jobs(run_id=run_id, stage=stage, platform=platform, limit=25)
        job = _claim_next_job(worker_id=worker_id, run_id=run_id, stage=stage, platform=platform)
        if not job and worker_id:
            job = _fetch_next_preclaimed_job(
                worker_id=worker_id, run_id=run_id, stage=stage, platform=platform,
            )
        if not job:
            break
        run_state = pg.fetch_one("select status from social.scrape_runs where id = %s", [run_id]) or {}
        if str(run_state.get("status")) == "cancelled":
            _finish_job(str(job.get("id")), status="cancelled", items_found=0, metadata={"stage": "cancelled"})
            continue
        _execute_claimed_job(job, worker_id=worker_id)

    _finalize_run_status(run_id)
    return (
        pg.fetch_one(
            """
        select
          id::text,
          season_id::text as season_id,
          show_id::text as show_id,
          source_scope,
          status,
          config,
          summary,
          created_at,
          started_at,
          completed_at,
          cancelled_at
        from social.scrape_runs
        where id = %s
        """,
            [run_id],
        )
        or {}
    )


def process_next_queued_job(
    *, worker_id: str, stage: str | None = None, platform: str | None = None
) -> dict[str, Any] | None:
    platform = _normalize_platform_name(platform)
    recover_stale_running_jobs(run_id=None, stage=stage, platform=platform, limit=25)
    job = _claim_next_job(worker_id=worker_id, run_id=None, stage=stage, platform=platform)
    if not job:
        return None
    return _execute_claimed_job(job, worker_id=worker_id)


def ingest_season(
    season_id: str,
    *,
    platforms: list[str] | None,
    source_scope: str,
    sync_strategy: str = "incremental",
    max_posts_per_target: int | None,
    max_comments_per_post: int | None,
    max_replies_per_post: int | None = 100,
    fetch_replies: bool,
    ingest_mode: str = "posts_and_comments",
    date_start: datetime | None,
    date_end: datetime | None,
    comment_refresh_policy: str = DEFAULT_COMMENT_REFRESH_POLICY,
    comment_anchor_source_ids: dict[str, list[str]] | None = None,
    initiated_by: str | None,
    inline_worker_id: str | None = None,
) -> dict[str, Any]:
    _assert_social_queue_schema_ready()
    context = get_season_context(season_id)
    if source_scope not in SUPPORTED_SCOPES:
        raise ValueError(f"Unsupported source scope: {source_scope}")

    normalized_mode = (ingest_mode or "posts_and_comments").strip().lower()
    if normalized_mode not in SUPPORTED_INGEST_MODES:
        raise ValueError(f"Unsupported ingest mode: {ingest_mode}")
    normalized_sync_strategy = (sync_strategy or "incremental").strip().lower()
    if normalized_sync_strategy not in SUPPORTED_SYNC_STRATEGIES:
        raise ValueError(f"Unsupported sync strategy: {sync_strategy}")
    normalized_comment_refresh_policy = (comment_refresh_policy or DEFAULT_COMMENT_REFRESH_POLICY).strip().lower()
    if normalized_comment_refresh_policy not in SUPPORTED_COMMENT_REFRESH_POLICIES:
        raise ValueError(f"Unsupported comment refresh policy: {comment_refresh_policy}")

    platform_filter = {_normalize_platform_name(p) for p in platforms or [] if isinstance(p, str) and p.strip()}
    if platform_filter:
        unsupported = platform_filter - set(SUPPORTED_PLATFORMS)
        if unsupported:
            raise ValueError(f"Unsupported platforms requested: {', '.join(sorted(unsupported))}")
    normalized_comment_anchor_source_ids, overflow_platforms = _normalize_comment_anchor_source_ids(
        comment_anchor_source_ids,
        allowed_platforms=(platform_filter or None),
    )

    resolved_posts, resolved_comments, resolved_replies, resolved_fetch_replies = _resolve_depth_defaults(
        max_posts_per_target=max_posts_per_target,
        max_comments_per_post=max_comments_per_post,
        max_replies_per_post=max_replies_per_post,
        fetch_replies=fetch_replies,
    )
    if normalized_mode == "posts_only":
        resolved_comments = 0
        resolved_replies = 0
        resolved_fetch_replies = False
    elif normalized_mode == "comments_only":
        # Comments-only runs should not cap anchor iteration by default.
        resolved_posts = 0

    stage_plan = (
        ["posts"]
        if normalized_mode == "posts_only"
        else ["comments"]
        if normalized_mode == "comments_only"
        else ["posts", "comments"]
    )

    opts = IngestOptions(
        platforms=platform_filter or None,
        source_scope=source_scope,
        sync_strategy=normalized_sync_strategy,
        max_posts_per_target=resolved_posts,
        max_comments_per_post=resolved_comments,
        max_replies_per_post=resolved_replies,
        fetch_replies=resolved_fetch_replies,
        ingest_mode=normalized_mode,
        date_start=date_start,
        date_end=date_end,
        comment_refresh_policy=normalized_comment_refresh_policy,
        comment_anchor_source_ids=normalized_comment_anchor_source_ids or None,
    )

    targets_payload = get_targets(season_id, source_scope=source_scope)
    targets: list[dict[str, Any]] = []
    for target in targets_payload.get("targets", []):
        if not target.get("is_active", True):
            continue
        target_platform = _normalize_platform_name(target.get("platform"))
        if target_platform not in SUPPORTED_PLATFORMS:
            logger.warning(
                "Skipping ingest target with unsupported platform token: %s (season=%s source_scope=%s)",
                target.get("platform"),
                context.season_id,
                source_scope,
            )
            continue
        if platform_filter and target_platform not in platform_filter:
            continue
        targets.append(dict(target) | {"platform": target_platform})

    if not targets:
        return {
            "season_id": context.season_id,
            "show_id": context.show_id,
            "season_number": context.season_number,
            "source_scope": source_scope,
            "run_id": None,
            "stages": stage_plan,
            "queued_or_started_jobs": 0,
            "message": "No active targets configured for selected platforms",
        }

    run_config = {
        "season_id": context.season_id,
        "show_id": context.show_id,
        "source_scope": source_scope,
        "platforms": sorted(platform_filter) if platform_filter else "all",
        "date_start": _iso(opts.date_start),
        "date_end": _iso(opts.date_end),
        "sync_strategy": opts.sync_strategy,
        "max_posts_per_target": opts.max_posts_per_target,
        "max_comments_per_post": opts.max_comments_per_post,
        "max_replies_per_post": opts.max_replies_per_post,
        "fetch_replies": opts.fetch_replies,
        "ingest_mode": opts.ingest_mode,
        "comment_refresh_policy": opts.comment_refresh_policy,
        "comment_anchor_source_ids": _serialize_comment_anchor_source_ids(opts.comment_anchor_source_ids),
        "comment_anchor_source_ids_overflow_platforms": sorted(overflow_platforms),
    }
    run_id = _create_run(
        context,
        source_scope=source_scope,
        initiated_by=initiated_by,
        config=run_config,
        status="queued",
    )

    queue_enabled = is_queue_enabled()
    initial_job_status = "queued" if queue_enabled else "pending"
    job_ids: list[str] = []
    skipped_comment_jobs = 0

    for target in targets:
        platform = str(target.get("platform") or "")
        accounts = [str(item).strip() for item in (target.get("accounts") or []) if str(item).strip()]
        target_hashtags = [
            str(item).strip().lstrip("#") for item in (target.get("hashtags") or []) if str(item).strip()
        ]
        target_keywords = [str(item).strip() for item in (target.get("keywords") or []) if str(item).strip()]
        fallback_hashtags, fallback_keywords = _derive_show_terms(context.show_name)
        hashtags = _normalize_unique_terms([*target_hashtags, *fallback_hashtags])
        keywords = _normalize_unique_terms([*target_keywords, *fallback_keywords])
        platform_source_ids = normalized_comment_anchor_source_ids.get(platform)
        platform_has_explicit_targets = platform in normalized_comment_anchor_source_ids
        platform_source_ids_list = sorted(platform_source_ids) if platform_source_ids is not None else None
        for account in accounts:
            base_config = {
                "run_id": run_id,
                "season_id": context.season_id,
                "show_id": context.show_id,
                "platform": platform,
                "source_scope": source_scope,
                "account": account,
                "hashtags": hashtags,
                "keywords": keywords,
                "date_start": _iso(opts.date_start),
                "date_end": _iso(opts.date_end),
                "sync_strategy": opts.sync_strategy,
                "max_posts_per_target": opts.max_posts_per_target,
                "max_comments_per_post": opts.max_comments_per_post,
                "max_replies_per_post": opts.max_replies_per_post,
                "fetch_replies": opts.fetch_replies,
                "ingest_mode": opts.ingest_mode,
                "comment_refresh_policy": opts.comment_refresh_policy,
            }
            if platform_has_explicit_targets:
                base_config["comment_anchor_source_ids"] = platform_source_ids_list
            if normalized_mode != "comments_only":
                job_ids.append(
                    _create_job(
                        context,
                        run_id=run_id,
                        platform=platform,
                        source_scope=source_scope,
                        job_type="posts",
                        stage="posts",
                        config={**base_config, "stage": "posts"},
                        initiated_by=initiated_by,
                        status=initial_job_status,
                        priority=100,
                        worker_id=inline_worker_id,
                        preclaim=bool(inline_worker_id),
                    )
                )
            if normalized_mode in {"posts_and_comments", "comments_only"} and opts.max_comments_per_post > 0:
                if (
                    normalized_mode == "comments_only"
                    and opts.comment_refresh_policy == "missing_only"
                    and platform_has_explicit_targets
                    and not platform_source_ids_list
                ):
                    skipped_comment_jobs += 1
                    continue
                job_ids.append(
                    _create_job(
                        context,
                        run_id=run_id,
                        platform=platform,
                        source_scope=source_scope,
                        job_type="comments",
                        stage="comments",
                        config={**base_config, "stage": "comments"},
                        initiated_by=initiated_by,
                        status=initial_job_status,
                        priority=200,
                        worker_id=inline_worker_id,
                        preclaim=bool(inline_worker_id),
                    )
                )

    summary = _update_run_summary(run_id)
    if skipped_comment_jobs:
        summary = dict(summary or {})
        summary["skipped_comment_jobs"] = skipped_comment_jobs
    return {
        "season_id": context.season_id,
        "show_id": context.show_id,
        "season_number": context.season_number,
        "source_scope": source_scope,
        "run_id": run_id,
        "status": "queued" if queue_enabled else "pending",
        "stages": stage_plan,
        "queued_or_started_jobs": len(job_ids),
        "summary": summary,
    }


def cancel_run(season_id: str, run_id: str, *, cancelled_by: str | None = None) -> dict[str, Any]:
    _assert_social_queue_schema_ready()
    run_row = pg.fetch_one(
        """
        select id::text, season_id::text, status
        from social.scrape_runs
        where id = %s and season_id = %s
        """,
        [run_id, season_id],
    )
    if not run_row:
        raise ValueError("Run not found")

    pg.fetch_one(
        """
        update social.scrape_runs
        set
          status = 'cancelled',
          cancelled_at = now(),
          completed_at = now(),
          summary = coalesce(summary, '{}'::jsonb) || jsonb_build_object('cancelled_by', %s)
        where id = %s
        returning id::text
        """,
        [cancelled_by, run_id],
    )
    cancelled_jobs = pg.execute_returning(
        """
        update social.scrape_jobs
        set
          status = 'cancelled',
          completed_at = now(),
          error_message = coalesce(error_message, 'Cancelled by user request')
        where run_id = %s
          and status in ('queued', 'pending', 'retrying', 'running')
        returning id::text
        """,
        [run_id],
    )
    summary = _update_run_summary(run_id)
    return {
        "run_id": run_id,
        "season_id": season_id,
        "status": "cancelled",
        "cancelled_jobs": len(cancelled_jobs),
        "summary": summary,
    }


def requeue_media_mirror_jobs(
    season_id: str,
    *,
    platform: str,
    source_scope: str = "bravo",
    limit: int = 1000,
    failed_only: bool = False,
    date_start: datetime | None = None,
    date_end: datetime | None = None,
) -> dict[str, Any]:
    normalized_platform = (platform or "").strip().lower()
    table = PLATFORM_POST_TABLES.get(normalized_platform)
    source_id_column = PLATFORM_SOURCE_ID_COLUMN.get(normalized_platform)
    posted_at_column = PLATFORM_POSTED_AT_COLUMN.get(normalized_platform)
    if not table or not source_id_column or not posted_at_column:
        raise ValueError(f"Unsupported platform: {platform}")

    context = get_season_context(season_id)
    safe_limit = max(1, min(int(limit), 5000))
    account_expr = (
        "coalesce(nullif(p.source_account, ''), nullif(p.channel_title, ''), '')"
        if normalized_platform == "youtube"
        else "coalesce(nullif(p.source_account, ''), nullif(p.username, ''), '')"
    )
    thumbnail_expr = (
        "coalesce(nullif(p.media_urls ->> 0, ''), '')"
        if normalized_platform == "twitter"
        else "coalesce(nullif(p.thumbnail_url, ''), '')"
    )
    media_urls_expr = "p.media_urls" if _platform_posts_has_column(normalized_platform, "media_urls") else "'[]'::jsonb"
    start_dt = _coerce_dt(date_start)
    end_dt = _coerce_dt(date_end)
    if start_dt and end_dt and end_dt < start_dt:
        end_dt = start_dt
    window_filter = ""
    params: list[Any] = [season_id]
    if start_dt:
        window_filter += f" and p.{posted_at_column} >= %s"
        params.append(start_dt)
    if end_dt:
        window_filter += f" and p.{posted_at_column} <= %s"
        params.append(end_dt)
    params.append(safe_limit)
    rows = pg.fetch_all(
        f"""
        select
          p.id::text as id,
          p.{source_id_column} as source_id,
          {account_expr} as account,
          p.{posted_at_column} as posted_at,
          {thumbnail_expr} as thumbnail_url,
          {media_urls_expr} as media_urls,
          coalesce(to_jsonb(p) ->> 'hosted_thumbnail_url', '') as hosted_thumbnail_url,
          coalesce(to_jsonb(p) -> 'hosted_media_urls', '[]'::jsonb) as hosted_media_urls,
          coalesce(to_jsonb(p) ->> 'media_mirror_status', '') as media_mirror_status
        from social.{table} p
        where p.season_id = %s
          {window_filter}
        order by coalesce(p.{posted_at_column}, p.scraped_at) desc
        limit %s
        """,
        params,
    )

    try:
        week_windows, _ = _resolve_week_windows(
            context,
            timezone="America/New_York",
            source_scope=source_scope,
            now_utc=_now_utc(),
        )
    except Exception:  # noqa: BLE001
        week_windows = []

    scanned = 0
    queued = 0
    skipped = 0
    failed = 0
    with pg.db_connection() as conn:
        for row in rows:
            scanned += 1
            mirror_status = str(row.get("media_mirror_status") or "").strip().lower()
            if failed_only and mirror_status not in {"failed", "partial"}:
                skipped += 1
                continue
            if not _platform_post_needs_media_mirror(normalized_platform, row):
                skipped += 1
                continue
            week_index: int | None = None
            posted_at = _coerce_dt(row.get("posted_at"))
            if posted_at and week_windows:
                week_window = _week_for_timestamp(posted_at, windows=week_windows, timezone="America/New_York")
                week_index = week_window.week_index if week_window else None
            try:
                job_id = _enqueue_platform_media_mirror_job(
                    context,
                    platform=normalized_platform,
                    run_id=None,
                    source_scope=source_scope,
                    account=str(row.get("account") or ""),
                    post_row=row,
                    week_index=week_index,
                    parent_job_id=f"manual-{normalized_platform}-mirror-requeue",
                    conn=conn,
                )
            except Exception:  # noqa: BLE001
                failed += 1
                logger.exception(
                    "[%s] Failed to enqueue mirror requeue job for post=%s source_id=%s",
                    normalized_platform,
                    str(row.get("id") or ""),
                    str(row.get("source_id") or ""),
                )
                continue
            if job_id:
                queued += 1
            else:
                skipped += 1

    return {
        "season_id": season_id,
        "platform": normalized_platform,
        "source_scope": source_scope,
        "failed_only": bool(failed_only),
        "window_applied": bool(start_dt or end_dt),
        "window": {
            "start": _iso(start_dt),
            "end": _iso(end_dt),
        },
        "eligible_in_window": len(rows),
        "scanned": scanned,
        "queued_jobs": queued,
        "skipped": skipped,
        "failed": failed,
    }


def requeue_instagram_media_mirror_jobs(
    season_id: str,
    *,
    source_scope: str = "bravo",
    limit: int = 1000,
    failed_only: bool = False,
    date_start: datetime | None = None,
    date_end: datetime | None = None,
) -> dict[str, Any]:
    return requeue_media_mirror_jobs(
        season_id,
        platform="instagram",
        source_scope=source_scope,
        limit=limit,
        failed_only=failed_only,
        date_start=date_start,
        date_end=date_end,
    )


def list_jobs(
    season_id: str,
    *,
    limit: int = 50,
    run_id: str | None = None,
    status: str | None = None,
    platform: str | None = None,
) -> list[dict[str, Any]]:
    safe_limit = max(1, min(int(limit), 250))
    features = _scrape_jobs_features()
    has_run_id = bool(features.get("has_run_id"))
    has_queue_fields = bool(features.get("has_queue_fields"))

    select_run_id = "j.run_id::text as run_id" if has_run_id else "null::text as run_id"
    select_attempt_count = "j.attempt_count" if has_queue_fields else "0::int as attempt_count"
    select_max_attempts = "j.max_attempts" if has_queue_fields else "0::int as max_attempts"
    select_priority = "j.priority" if has_queue_fields else "0::int as priority"
    select_available_at = "j.available_at" if has_queue_fields else "null::timestamptz as available_at"
    select_claimed_at = "j.claimed_at" if has_queue_fields else "null::timestamptz as claimed_at"
    select_heartbeat_at = "j.heartbeat_at" if has_queue_fields else "null::timestamptz as heartbeat_at"
    select_worker_id = "j.worker_id" if has_queue_fields else "null::text as worker_id"
    select_last_error_code = "j.last_error_code" if has_queue_fields else "null::text as last_error_code"
    select_last_error_class = "j.last_error_class" if has_queue_fields else "null::text as last_error_class"

    where_clauses = ["season_id = %s"]
    where_params: list[Any] = [season_id]
    if run_id and has_run_id:
        where_clauses.append("run_id = %s")
        where_params.append(run_id)
    if status:
        where_clauses.append("status = %s")
        where_params.append(status)
    if platform:
        where_clauses.append("platform = %s")
        where_params.append(platform)
    where_sql = " and ".join(where_clauses)

    select_sql = f"""
        select
          j.id::text,
          {select_run_id},
          j.platform,
          j.job_type,
          j.status,
          j.items_found,
          j.error_message,
          j.started_at,
          j.completed_at,
          j.created_at,
          j.config,
          j.metadata,
          j.source_scope,
          j.initiated_by,
          {select_attempt_count},
          {select_max_attempts},
          {select_priority},
          {select_available_at},
          {select_claimed_at},
          {select_heartbeat_at},
          {select_worker_id},
          {select_last_error_code},
          {select_last_error_class}
    """

    # Unscoped polling is the hottest path; preselect IDs to avoid scanning heavy JSON columns
    # before ORDER/LIMIT when no specific run_id is requested.
    use_unscoped_fast_path = not (run_id and has_run_id)
    if use_unscoped_fast_path:
        sql = f"""
            with candidate_jobs as (
              select id
              from social.scrape_jobs
              where {where_sql}
              order by created_at desc
              limit %s
            )
            {select_sql}
            from social.scrape_jobs j
            join candidate_jobs c on c.id = j.id
            order by j.created_at desc
        """
        params = [*where_params, safe_limit]
    else:
        sql = f"""
            {select_sql}
            from social.scrape_jobs j
            where {where_sql}
            order by j.created_at desc
            limit %s
        """
        params = [*where_params, safe_limit]

    rows = pg.fetch_all(sql, params)
    for row in rows:
        metadata = row.get("metadata")
        metadata_map = metadata if isinstance(metadata, dict) else {}
        metadata_error_code = metadata_map.get("job_error_code")
        normalized = _normalize_job_error_code(
            raw_error_code=str(row.get("last_error_code") or ""),
            error_message=str(row.get("error_message") or ""),
            error_class=str(row.get("last_error_class") or ""),
        )
        row["job_error_code"] = (
            str(metadata_error_code).strip().upper()
            if isinstance(metadata_error_code, str) and metadata_error_code.strip()
            else normalized
        )
    return rows


def list_runs(
    season_id: str,
    *,
    limit: int = 50,
    status: str | None = None,
    source_scope: str | None = None,
    run_id: str | None = None,
) -> list[dict[str, Any]]:
    safe_limit = max(1, min(int(limit), 250))
    sql = """
        select
          id::text,
          season_id::text as season_id,
          show_id::text as show_id,
          source_scope,
          status,
          config,
          summary,
          initiated_by,
          created_at,
          started_at,
          completed_at,
          cancelled_at
        from social.scrape_runs
        where season_id = %s
    """
    params: list[Any] = [season_id]
    if status:
        sql += " and status = %s"
        params.append(status)
    if source_scope:
        sql += " and source_scope = %s"
        params.append(source_scope)
    if run_id:
        sql += " and id = %s::uuid"
        params.append(run_id)
    sql += " order by created_at desc limit %s"
    params.append(safe_limit)
    return pg.fetch_all(sql, params)


def list_run_summaries(
    season_id: str,
    *,
    limit: int = 20,
    source_scope: str | None = None,
) -> list[dict[str, Any]]:
    runs = list_runs(season_id, limit=limit, source_scope=source_scope)
    if not runs:
        return []

    run_ids = [str(run.get("id") or "") for run in runs if str(run.get("id") or "").strip()]
    if not run_ids:
        return []

    job_rows = pg.fetch_all(
        """
        select
          run_id::text as run_id,
          platform,
          status,
          error_message,
          last_error_code,
          last_error_class,
          metadata
        from social.scrape_jobs
        where season_id = %s
          and run_id = any(%s::uuid[])
        """,
        [season_id, run_ids],
    )

    by_run: dict[str, dict[str, Any]] = {}
    for row in job_rows:
        run_id = str(row.get("run_id") or "")
        if not run_id:
            continue
        bucket = by_run.setdefault(
            run_id,
            {
                "affected_platforms": set(),
                "error_counts": Counter(),
                "failed_jobs": 0,
                "active_jobs": 0,
                "total_jobs": 0,
            },
        )
        bucket["total_jobs"] += 1
        platform = str(row.get("platform") or "").strip().lower()
        if platform:
            bucket["affected_platforms"].add(platform)
        status = str(row.get("status") or "").strip().lower()
        if status == "failed":
            bucket["failed_jobs"] += 1
        if status in {"queued", "pending", "retrying", "running"}:
            bucket["active_jobs"] += 1
        if status in {"failed", "retrying"}:
            metadata = row.get("metadata")
            metadata_map = metadata if isinstance(metadata, dict) else {}
            metadata_error_code = metadata_map.get("job_error_code")
            normalized = (
                str(metadata_error_code).strip().upper()
                if isinstance(metadata_error_code, str) and metadata_error_code.strip()
                else _normalize_job_error_code(
                    raw_error_code=str(row.get("last_error_code") or ""),
                    error_message=str(row.get("error_message") or ""),
                    error_class=str(row.get("last_error_class") or ""),
                )
            )
            bucket["error_counts"][normalized] += 1

    now_utc = _now_utc()
    summaries: list[dict[str, Any]] = []
    for run in runs:
        run_id = str(run.get("id") or "")
        if not run_id:
            continue
        summary = run.get("summary") if isinstance(run.get("summary"), dict) else {}
        aggregate = by_run.get(run_id) or {}
        started_at = run.get("started_at")
        completed_at = run.get("completed_at")
        duration_seconds: int | None = None
        if isinstance(started_at, datetime):
            end_ref = completed_at if isinstance(completed_at, datetime) else now_utc
            duration_seconds = max(0, int((end_ref - started_at).total_seconds()))

        total_jobs = int(summary.get("total_jobs") or aggregate.get("total_jobs") or 0)
        completed_jobs = int(summary.get("completed_jobs") or 0)
        failed_jobs = int(summary.get("failed_jobs") or aggregate.get("failed_jobs") or 0)
        active_jobs = int(summary.get("active_jobs") or aggregate.get("active_jobs") or 0)
        if completed_jobs == 0 and total_jobs > 0:
            completed_jobs = max(0, total_jobs - failed_jobs - active_jobs)
        items_found_total = int(summary.get("items_found_total") or 0)
        stage_counts = summary.get("stage_counts") if isinstance(summary.get("stage_counts"), dict) else {}
        affected_platforms = sorted(aggregate.get("affected_platforms") or set())
        error_counts = dict(sorted((aggregate.get("error_counts") or Counter()).items()))
        success_rate_pct = _safe_percent(completed_jobs, max(1, total_jobs))

        summaries.append(
            {
                "run_id": run_id,
                "status": run.get("status"),
                "source_scope": run.get("source_scope"),
                "created_at": _iso(run.get("created_at")),
                "started_at": _iso(started_at),
                "completed_at": _iso(completed_at),
                "duration_seconds": duration_seconds,
                "total_jobs": total_jobs,
                "completed_jobs": completed_jobs,
                "failed_jobs": failed_jobs,
                "active_jobs": active_jobs,
                "items_found_total": items_found_total,
                "stage_counts": stage_counts,
                "affected_platforms": affected_platforms,
                "error_counts": error_counts,
                "success_rate_pct": success_rate_pct,
            }
        )
    return summaries


# ---------------------------------------------------------------------------
# Analytics + exports
# ---------------------------------------------------------------------------


def _default_sentiment_context() -> SentimentAnalyzerContext:
    return SentimentAnalyzerContext(cast_terms=set(), cast_phrases=set(), episode_terms=set(), episode_summary="")


def _normalize_sentiment_text_key(text: str | None) -> str:
    raw = (text or "").strip().lower()
    if not raw:
        return ""
    return re.sub(r"\s+", " ", raw)


def _tokenize_handle_terms(value: str | None) -> set[str]:
    if not value:
        return set()
    return {token.lower() for token in re.split(r"[^A-Za-z0-9]+", value) if token and len(token) >= 3}


def _build_cast_entity_terms(names: list[str]) -> tuple[set[str], set[str]]:
    cast_terms: set[str] = set()
    cast_phrases: set[str] = set()
    for raw_name in names:
        cleaned = str(raw_name or "").strip()
        if not cleaned:
            continue
        lower_phrase = cleaned.lower()
        cast_phrases.add(lower_phrase)
        for token in TOKEN_RE.findall(cleaned):
            lower = token.lower()
            if len(lower) >= 3:
                cast_terms.add(lower)
    return cast_terms, cast_phrases


def _load_cast_names_for_show(show_id: str) -> list[str]:
    try:
        rows = pg.fetch_all(
            """
            select distinct p.full_name
            from core.credits c
            join core.people p on p.id = c.person_id
            where c.show_id = %s
              and lower(coalesce(c.credit_category, '')) = 'self'
              and nullif(trim(coalesce(p.full_name, '')), '') is not null
            order by p.full_name asc
            """,
            [show_id],
        )
        names = [str(row.get("full_name") or "").strip() for row in rows]
        names = [name for name in names if name]
        if names:
            return names
    except Exception:  # noqa: BLE001
        logger.warning("Failed to load cast names from core.credits for show %s", show_id, exc_info=True)

    try:
        fallback_rows = pg.fetch_all(
            """
            select distinct cast_member_name
            from core.v_show_cast
            where show_id = %s
              and nullif(trim(coalesce(cast_member_name, '')), '') is not null
            order by cast_member_name asc
            """,
            [show_id],
        )
        return [
            str(row.get("cast_member_name") or "").strip()
            for row in fallback_rows
            if str(row.get("cast_member_name") or "").strip()
        ]
    except Exception:  # noqa: BLE001
        logger.warning("Failed to load cast names from core.v_show_cast for show %s", show_id, exc_info=True)
        return []


def _load_episode_sentiment_context(season_id: str) -> EpisodeSentimentContext:
    try:
        rows = pg.fetch_all(
            """
            select episode_number, title, coalesce(nullif(overview, ''), nullif(synopsis, ''), '') as summary
            from core.episodes
            where season_id = %s
            order by episode_number asc
            """,
            [season_id],
        )
    except Exception:  # noqa: BLE001
        logger.warning("Failed to load episode sentiment context for season %s", season_id, exc_info=True)
        return EpisodeSentimentContext(summary="", terms=set())

    terms: set[str] = set()
    snippets: list[str] = []
    for row in rows:
        title = str(row.get("title") or "").strip()
        summary = str(row.get("summary") or "").strip()
        episode_number = row.get("episode_number")
        for token in TOKEN_RE.findall(f"{title} {summary}"):
            lower = token.lower()
            if len(lower) >= 4 and lower not in STOPWORDS:
                terms.add(lower)
        if title or summary:
            short_summary = re.sub(r"\s+", " ", summary)
            if len(short_summary) > 90:
                short_summary = f"{short_summary[:87].rstrip()}..."
            episode_label = f"E{episode_number}" if isinstance(episode_number, int) else "Episode"
            if short_summary:
                snippets.append(f"{episode_label}: {title or 'Untitled'} — {short_summary}")
            else:
                snippets.append(f"{episode_label}: {title or 'Untitled'}")

    return EpisodeSentimentContext(summary="; ".join(snippets[:10]), terms=terms)


def _build_sentiment_context(context: SeasonContext) -> SentimentAnalyzerContext:
    cast_names = _load_cast_names_for_show(context.show_id)
    cast_terms, cast_phrases = _build_cast_entity_terms(cast_names)
    episode_context = _load_episode_sentiment_context(context.season_id)
    return SentimentAnalyzerContext(
        cast_terms=cast_terms,
        cast_phrases=cast_phrases,
        episode_terms=episode_context.terms,
        episode_summary=episode_context.summary,
    )


def _has_negation(tokens: list[str], index: int) -> bool:
    start = max(0, index - 3)
    return any(token in NEGATION_WORDS for token in tokens[start:index])


def _modifier_weight(tokens: list[str], index: int) -> float:
    if index <= 0:
        return 1.0
    weight = 1.0
    prev = tokens[index - 1]
    weight *= INTENSIFIER_WEIGHTS.get(prev, 1.0)
    weight *= DIMINISHER_WEIGHTS.get(prev, 1.0)
    if index >= 2 and tokens[index - 1] == "of":
        weight *= DIMINISHER_WEIGHTS.get(tokens[index - 2], 1.0)
    return weight


def _score_with_contrast(tokens: list[str], contributions: list[float]) -> float:
    total = sum(contributions)
    markers = [idx for idx, token in enumerate(tokens) if token in CONTRAST_MARKERS]
    if not markers:
        return total

    pivot = markers[-1]
    before = sum(contributions[:pivot])
    after = sum(contributions[pivot + 1 :])
    if before == 0 or after == 0:
        return total
    return (before * 0.6) + (after * 1.4)


def _rule_based_sentiment_for_text(
    text: str | None,
    *,
    analyzer_context: SentimentAnalyzerContext,
) -> SentimentRuleResult:
    raw_text = str(text or "").strip()
    if not raw_text:
        return SentimentRuleResult(label="neutral", score=0.0, confidence=1.0, ambiguous=False)

    lowered = raw_text.lower()
    tokens = [token.lower() for token in TOKEN_RE.findall(raw_text)]
    if not tokens:
        return SentimentRuleResult(label="neutral", score=0.0, confidence=0.95, ambiguous=False)

    mention_terms = {
        token.lower() for mention in MENTION_TOKEN_RE.findall(raw_text) for token in _tokenize_handle_terms(mention)
    }
    blocked_entity_terms = set(analyzer_context.cast_terms)
    blocked_entity_terms.update(mention_terms)

    contributions: list[float] = [0.0 for _ in tokens]
    sentiment_hits = 0
    for index, token in enumerate(tokens):
        if token in blocked_entity_terms:
            continue
        base = 0.0
        if token in POSITIVE_WORDS:
            base = 1.0
        elif token in NEGATIVE_WORDS:
            base = -1.0
        if base == 0.0:
            continue

        sentiment_hits += 1
        if _has_negation(tokens, index):
            base *= -1.0
        contributions[index] = base * _modifier_weight(tokens, index)

    if sentiment_hits == 0:
        has_context_only_signal = any(token in analyzer_context.cast_terms for token in tokens) or any(
            token in analyzer_context.episode_terms for token in tokens
        )
        confidence = 0.94 if has_context_only_signal else 0.85
        return SentimentRuleResult(label="neutral", score=0.0, confidence=confidence, ambiguous=False)

    score = _score_with_contrast(tokens, contributions)
    exclamation_count = lowered.count("!")
    if exclamation_count > 0:
        score *= min(1.35, 1.0 + (0.08 * exclamation_count))

    if score >= 0.55:
        label = "positive"
    elif score <= -0.55:
        label = "negative"
    else:
        label = "neutral"

    confidence = min(0.99, abs(score) / max(1.0, sentiment_hits * 0.9))
    if label == "neutral":
        confidence = min(confidence, 0.45)
    confidence = max(0.15, confidence)

    ambiguous = label == "neutral" or confidence < 0.5 or abs(score) < 0.9
    return SentimentRuleResult(label=label, score=score, confidence=confidence, ambiguous=ambiguous)


def _score_from_label(label: str) -> int:
    if label == "positive":
        return 1
    if label == "negative":
        return -1
    return 0


def _resolve_sentiment_gemini_model_selection() -> tuple[str, str, str | None]:
    custom = (os.getenv("SOCIAL_SENTIMENT_GEMINI_MODEL") or "").strip()
    if custom:
        return custom, "SOCIAL_SENTIMENT_GEMINI_MODEL", None

    pro = (os.getenv("GEMINI_MODEL_PRO") or "").strip()
    if pro:
        return pro, "GEMINI_MODEL_PRO", "SOCIAL_SENTIMENT_GEMINI_MODEL->GEMINI_MODEL_PRO"

    google_alias = (os.getenv("GOOGLE_GEMINI_MODEL") or "").strip()
    if google_alias:
        return (
            google_alias,
            "GOOGLE_GEMINI_MODEL",
            "SOCIAL_SENTIMENT_GEMINI_MODEL->GEMINI_MODEL_PRO->GOOGLE_GEMINI_MODEL",
        )

    canonical = (os.getenv("GEMINI_MODEL") or "").strip()
    if canonical:
        return (
            canonical,
            "GEMINI_MODEL",
            "SOCIAL_SENTIMENT_GEMINI_MODEL->GEMINI_MODEL_PRO->GOOGLE_GEMINI_MODEL->GEMINI_MODEL",
        )

    fast = (os.getenv("GEMINI_MODEL_FAST") or "").strip()
    if fast:
        return (
            fast,
            "GEMINI_MODEL_FAST",
            "SOCIAL_SENTIMENT_GEMINI_MODEL->GEMINI_MODEL_PRO->GOOGLE_GEMINI_MODEL->GEMINI_MODEL->GEMINI_MODEL_FAST",
        )

    return (
        DEFAULT_GEMINI_SENTIMENT_MODEL,
        "default",
        "SOCIAL_SENTIMENT_GEMINI_MODEL->GEMINI_MODEL_PRO->GOOGLE_GEMINI_MODEL->GEMINI_MODEL->GEMINI_MODEL_FAST->default",
    )


def _resolve_sentiment_gemini_model() -> str:
    model_name, _model_source, _fallback_path = _resolve_sentiment_gemini_model_selection()
    return model_name


def _resolve_sentiment_gemini_max_comments() -> int:
    raw = (os.getenv("SOCIAL_SENTIMENT_GEMINI_MAX_COMMENTS") or "").strip()
    if raw.isdigit():
        return max(1, min(int(raw), SENTIMENT_GEMINI_MAX_COMMENTS_CAP))
    return min(40, SENTIMENT_GEMINI_MAX_COMMENTS_CAP)


def _resolve_sentiment_gemini_batch_size() -> int:
    raw = (os.getenv("SOCIAL_SENTIMENT_GEMINI_BATCH_SIZE") or "").strip()
    if raw.isdigit():
        return max(1, min(int(raw), SENTIMENT_GEMINI_BATCH_SIZE_CAP))
    return min(10, SENTIMENT_GEMINI_BATCH_SIZE_CAP)


def _extract_gemini_text(response: Any) -> str:
    text = getattr(response, "text", None)
    if isinstance(text, str) and text.strip():
        return text
    candidates = getattr(response, "candidates", None) or []
    for candidate in candidates:
        content = getattr(candidate, "content", None)
        parts = getattr(content, "parts", None) or []
        for part in parts:
            candidate_text = getattr(part, "text", None)
            if isinstance(candidate_text, str) and candidate_text.strip():
                return candidate_text
    return ""


def _build_gemini_text_generator(*, api_key: str, model_name: str):
    try:
        from google import genai as google_genai  # type: ignore

        client = google_genai.Client(api_key=api_key)

        def _generate(prompt: str) -> Any:
            return client.models.generate_content(model=model_name, contents=prompt)

        return _generate, "google-genai"
    except Exception:  # noqa: BLE001
        return None, None


def _parse_gemini_sentiment_payload(raw: str) -> dict[int, tuple[str, float]]:
    if not raw:
        return {}
    candidate = raw.strip()
    candidate = re.sub(r"^```(?:json)?\s*", "", candidate, flags=re.IGNORECASE)
    candidate = re.sub(r"\s*```$", "", candidate)

    payload: Any
    try:
        payload = json.loads(candidate)
    except json.JSONDecodeError:
        match = re.search(r"(\[[\s\S]*\]|\{[\s\S]*\})", candidate)
        if not match:
            return {}
        try:
            payload = json.loads(match.group(1))
        except json.JSONDecodeError:
            return {}

    items: list[dict[str, Any]]
    if isinstance(payload, list):
        items = [item for item in payload if isinstance(item, dict)]
    elif isinstance(payload, dict):
        raw_items = payload.get("items")
        if isinstance(raw_items, list):
            items = [item for item in raw_items if isinstance(item, dict)]
        else:
            return {}
    else:
        return {}

    parsed: dict[int, tuple[str, float]] = {}
    for item in items:
        index_raw = item.get("index")
        sentiment_raw = str(item.get("sentiment") or "").strip().lower()
        confidence_raw = item.get("confidence")
        if sentiment_raw not in {"positive", "neutral", "negative"}:
            continue
        if not isinstance(index_raw, int):
            continue
        confidence = 0.5
        if isinstance(confidence_raw, (int, float)):
            confidence = max(0.0, min(float(confidence_raw), 1.0))
        parsed[index_raw] = (sentiment_raw, confidence)
    return parsed


def _classify_ambiguous_sentiments_with_gemini(
    entries: list[tuple[str, str]],
    *,
    context: SeasonContext,
    analyzer_context: SentimentAnalyzerContext,
) -> dict[str, tuple[str, float]]:
    if not entries:
        return {}
    if not _env_truthy("SOCIAL_SENTIMENT_GEMINI_ENABLED", default=False):
        return {}

    api_key = (os.getenv("GEMINI_API_KEY") or "").strip() or (os.getenv("GOOGLE_GEMINI_API_KEY") or "").strip()
    if not api_key:
        logger.info("Gemini sentiment disambiguation enabled but API key is missing; falling back to rules")
        return {}

    model_name, model_source, fallback_path = _resolve_sentiment_gemini_model_selection()
    batch_size = _resolve_sentiment_gemini_batch_size()
    max_comments = _resolve_sentiment_gemini_max_comments()
    limited_entries = entries[:max_comments]

    generate_content, sdk_name = _build_gemini_text_generator(api_key=api_key, model_name=model_name)
    if generate_content is None:
        logger.warning("Gemini sentiment disambiguation unavailable: no supported SDK import found")
        return {}

    logger.info(
        "Gemini sentiment route=pro model=%s sdk=%s source=%s fallback_path=%s",
        model_name,
        sdk_name,
        model_source,
        fallback_path or "none",
    )

    cast_preview = ", ".join(sorted(analyzer_context.cast_phrases)[:20]) or "None"
    episode_summary = analyzer_context.episode_summary or "Unavailable"
    overrides: dict[str, tuple[str, float]] = {}

    for offset in range(0, len(limited_entries), batch_size):
        batch = limited_entries[offset : offset + batch_size]
        batch_comments = [{"index": index, "text": text} for index, (_, text) in enumerate(batch)]
        prompt = (
            "Classify each social comment sentiment for a Bravo reality TV season.\n"
            f"Show: {context.show_name or context.show_id}\n"
            f"Season: {context.season_number}\n"
            f"Episode context: {episode_summary}\n"
            f"Cast/entity names (treat as entities, not inherently sentiment): {cast_preview}\n"
            "Rules: label each comment as positive, neutral, or negative based on intent/context.\n"
            "Return only valid JSON as an array of objects: "
            '[{"index":0,"sentiment":"positive|neutral|negative","confidence":0.0}].\n'
            f"Comments: {json.dumps(batch_comments, ensure_ascii=True)}"
        )

        try:
            response = generate_content(prompt)
            parsed = _parse_gemini_sentiment_payload(_extract_gemini_text(response))
        except Exception:  # noqa: BLE001
            logger.warning("Gemini sentiment request failed; falling back to rule-based sentiment", exc_info=True)
            continue

        for index, (sentiment, confidence) in parsed.items():
            if index < 0 or index >= len(batch):
                continue
            key = batch[index][0]
            overrides[key] = (sentiment, confidence)

    return overrides


def _apply_optional_gemini_sentiment(
    rows: list[dict[str, Any]],
    *,
    context: SeasonContext,
    analyzer_context: SentimentAnalyzerContext,
) -> None:
    if not rows or not _env_truthy("SOCIAL_SENTIMENT_GEMINI_ENABLED", default=False):
        return

    deduped: dict[str, str] = {}
    for row in rows:
        if row.get("kind") != "comment":
            continue
        if not bool(row.get("_sentiment_ambiguous")):
            continue
        text_key = str(row.get("_sentiment_key") or "")
        if not text_key or text_key in deduped:
            continue
        deduped[text_key] = str(row.get("text") or "")

    if not deduped:
        return

    entries = list(deduped.items())
    overrides = _classify_ambiguous_sentiments_with_gemini(entries, context=context, analyzer_context=analyzer_context)
    if not overrides:
        return

    for row in rows:
        if row.get("kind") != "comment":
            continue
        text_key = str(row.get("_sentiment_key") or "")
        override = overrides.get(text_key)
        if not override:
            continue
        sentiment, confidence = override
        row["sentiment"] = sentiment
        row["sentiment_score"] = _score_from_label(sentiment)
        row["_sentiment_confidence"] = confidence
        row["_sentiment_ambiguous"] = False


def sentiment_for_text(text: str | None) -> tuple[str, int]:
    result = _rule_based_sentiment_for_text(text, analyzer_context=_default_sentiment_context())
    if result.label == "neutral":
        return "neutral", 0
    rounded = int(round(result.score))
    if rounded == 0:
        rounded = _score_from_label(result.label)
    return result.label, rounded


def _text_is_trailer_marker(value: str | None) -> bool:
    if not value:
        return False
    return bool(TRAILER_MARKER_RE.search(value))


def _video_matches_season(video: dict[str, Any], season_number: int) -> bool:
    raw = video.get("season_number")
    if isinstance(raw, int):
        return raw == season_number
    if isinstance(raw, str):
        stripped = raw.strip()
        if stripped.isdigit():
            return int(stripped) == season_number

    searchable = f"{video.get('title') or ''} {video.get('kicker') or ''}".lower()
    return f"season {season_number}" in searchable or f"s{season_number}" in searchable


def _load_bravo_snapshot_videos(show_id: str, season_number: int) -> list[dict[str, Any]]:
    row = pg.fetch_one(
        """
        select payload
        from core.show_source_latest
        where show_id = %s
          and source_id = 'bravo'
          and variant = 'default'
        limit 1
        """,
        [show_id],
    )
    if not row:
        return []

    payload = row.get("payload")
    if not isinstance(payload, dict):
        return []

    normalized = payload.get("normalized")
    if not isinstance(normalized, dict):
        return []

    videos_raw = normalized.get("videos_show")
    if not isinstance(videos_raw, list):
        return []

    videos: list[dict[str, Any]] = []
    for item in videos_raw:
        if not isinstance(item, dict):
            continue
        if not _video_matches_season(item, season_number):
            continue
        videos.append(item)
    return videos


def _find_week_zero_start_from_snapshot(
    *,
    context: SeasonContext,
    premiere_local: datetime,
    timezone: str,
) -> datetime | None:
    zone = ZoneInfo(timezone)
    candidates: list[tuple[datetime, bool]] = []

    for video in _load_bravo_snapshot_videos(context.show_id, context.season_number):
        published_at = _coerce_dt(video.get("published_at"))
        if not published_at:
            continue
        published_local = published_at.astimezone(zone)
        if published_local >= premiere_local:
            continue
        marker_hit = _text_is_trailer_marker(f"{video.get('title') or ''} {video.get('kicker') or ''}")
        candidates.append((published_local, marker_hit))

    if not candidates:
        return None

    marker_only = [value for value, marker in candidates if marker]
    if marker_only:
        return min(marker_only)
    return min(value for value, _ in candidates)


def _find_week_zero_start_from_social_rows(
    *,
    season_id: str,
    season_number: int,
    premiere_utc: datetime,
    timezone: str,
) -> datetime | None:
    lookback_utc = premiere_utc - timedelta(days=180)
    rows = pg.fetch_all(
        """
        with source_rows as (
          select posted_at as ts, caption as text from social.instagram_posts where season_id = %s
          union all
          select posted_at as ts, description as text from social.tiktok_posts where season_id = %s
          union all
          select created_at as ts, text as text from social.twitter_tweets where season_id = %s
          union all
          select published_at as ts, coalesce(title, '') || ' ' || coalesce(description, '') as text
          from social.youtube_videos
          where season_id = %s
        )
        select ts, text
        from source_rows
        where ts is not null
          and ts >= %s
          and ts < %s
        order by ts asc
        """,
        [season_id, season_id, season_id, season_id, lookback_utc, premiere_utc],
    )
    if not rows:
        return None

    zone = ZoneInfo(timezone)
    trailer_like_season_specific: list[datetime] = []
    trailer_like_generic: list[datetime] = []
    season_marker_variants = (
        f"season {season_number}",
        f"s{season_number}",
    )
    for row in rows:
        ts = _coerce_dt(row.get("ts"))
        if ts is None:
            continue
        text = str(row.get("text") or "")
        if not _text_is_trailer_marker(text):
            continue
        text_lower = text.lower()
        ts_local = ts.astimezone(zone)
        if any(marker in text_lower for marker in season_marker_variants):
            trailer_like_season_specific.append(ts_local)
        else:
            trailer_like_generic.append(ts_local)

    if trailer_like_season_specific:
        return min(trailer_like_season_specific)
    if trailer_like_generic:
        return min(trailer_like_generic)
    return _coerce_dt(rows[0].get("ts")).astimezone(zone) if rows[0].get("ts") else None


def _parse_local_override_datetime(value: Any, timezone: str) -> datetime | None:
    zone = ZoneInfo(timezone)
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo:
            return value.astimezone(zone)
        return value.replace(tzinfo=zone)
    if isinstance(value, date):
        return datetime.combine(value, time.min, tzinfo=zone)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw):
            parsed_date = datetime.fromisoformat(raw).date()
            return datetime.combine(parsed_date, time.min, tzinfo=zone)
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo:
            return parsed.astimezone(zone)
        return parsed.replace(tzinfo=zone)
    return None


def _find_week_zero_start_override(
    *,
    season_id: str,
    source_scope: str,
    timezone: str,
    premiere_local: datetime,
) -> datetime | None:
    rows = pg.fetch_all(
        """
        select config
        from social.season_targets
        where season_id = %s
          and source_scope = %s
          and is_active = true
        """,
        [season_id, source_scope],
    )
    if not rows:
        return None

    candidates: list[datetime] = []
    for row in rows:
        config = row.get("config")
        if not isinstance(config, dict):
            continue
        for key in ("preseason_start", "preseason_start_at", "week_zero_start"):
            parsed = _parse_local_override_datetime(config.get(key), timezone)
            if parsed is None:
                continue
            if parsed >= premiere_local:
                continue
            candidates.append(parsed)
            break

    if not candidates:
        return None
    return min(candidates)


def _resolve_week_windows(
    context: SeasonContext,
    *,
    timezone: str,
    source_scope: str,
    now_utc: datetime,
) -> tuple[list[WeekWindow], datetime]:
    zone = ZoneInfo(timezone)
    now_local = now_utc.astimezone(zone)
    episode_rows = pg.fetch_all(
        """
        select episode_number, air_date
        from core.episodes
        where season_id = %s
          and air_date is not null
        order by episode_number asc, air_date asc
        """,
        [context.season_id],
    )

    episode_starts: list[tuple[int, datetime]] = []
    seen_numbers: set[int] = set()
    for row in episode_rows:
        raw_num = row.get("episode_number")
        air_date = row.get("air_date")
        if not isinstance(raw_num, int) or raw_num < 1 or not isinstance(air_date, date):
            continue
        if raw_num in seen_numbers:
            continue
        seen_numbers.add(raw_num)
        episode_starts.append((raw_num, datetime.combine(air_date, time(20, 0), tzinfo=zone)))

    if not episode_starts:
        anchor_local = datetime.combine(context.anchor_date, time.min, tzinfo=zone)
        return [WeekWindow(1, anchor_local, now_local, week_type="episode", episode_number=1)], anchor_local

    premiere_local = episode_starts[0][1]
    trailer_start_local = _find_week_zero_start_override(
        season_id=context.season_id,
        source_scope=source_scope,
        timezone=timezone,
        premiere_local=premiere_local,
    )
    if trailer_start_local is None:
        trailer_start_local = _find_week_zero_start_from_snapshot(
            context=context,
            premiere_local=premiere_local,
            timezone=timezone,
        )
    if trailer_start_local is None:
        trailer_start_local = _find_week_zero_start_from_social_rows(
            season_id=context.season_id,
            season_number=context.season_number,
            premiere_utc=premiere_local.astimezone(UTC),
            timezone=timezone,
        )
    if trailer_start_local is None:
        trailer_start_local = premiere_local - timedelta(days=7)
    if trailer_start_local >= premiere_local:
        trailer_start_local = premiere_local - timedelta(days=1)

    windows: list[WeekWindow] = [
        WeekWindow(0, trailer_start_local, premiere_local, week_type="preseason", episode_number=None)
    ]

    next_week_index = 1
    final_episode_end: datetime | None = None
    for idx, (episode_number, episode_start) in enumerate(episode_starts):
        next_episode_start = episode_starts[idx + 1][1] if idx + 1 < len(episode_starts) else None
        episode_end = episode_start + timedelta(days=7)
        if next_episode_start is not None and next_episode_start < episode_end:
            episode_end = next_episode_start
        if episode_end <= episode_start:
            continue
        windows.append(
            WeekWindow(
                next_week_index,
                episode_start,
                episode_end,
                week_type="episode",
                episode_number=episode_number,
            )
        )
        final_episode_end = episode_end
        next_week_index += 1

        if next_episode_start is None or next_episode_start <= episode_end:
            continue

        bye_start = episode_end
        while bye_start < next_episode_start:
            bye_end = min(bye_start + timedelta(days=7), next_episode_start)
            if bye_end <= bye_start:
                break
            windows.append(
                WeekWindow(
                    next_week_index,
                    bye_start,
                    bye_end,
                    week_type="bye",
                    episode_number=None,
                )
            )
            next_week_index += 1
            bye_start = bye_end

    if final_episode_end is not None:
        postseason_start = final_episode_end
        for _ in range(POSTSEASON_WEEK_COUNT):
            postseason_end = postseason_start + timedelta(days=7)
            windows.append(
                WeekWindow(
                    next_week_index,
                    postseason_start,
                    postseason_end,
                    week_type="postseason",
                    episode_number=None,
                )
            )
            next_week_index += 1
            postseason_start = postseason_end

    return windows, trailer_start_local


def _format_month_day_label(value: datetime, *, timezone: str) -> str:
    local_value = value.astimezone(ZoneInfo(timezone))
    return f"{local_value.strftime('%b')} {local_value.day}"


def _week_window_label(window: WeekWindow, *, timezone: str) -> str:
    if window.week_type == "preseason":
        return "Pre-Season"
    if window.week_type == "bye":
        start_label = _format_month_day_label(window.start_local, timezone=timezone)
        end_label = _format_month_day_label(
            window.end_local - timedelta(microseconds=1),
            timezone=timezone,
        )
        return f"BYE WEEK ({start_label}-{end_label})"
    return f"Week {window.week_index}"


def _week_for_timestamp(ts: datetime, *, windows: list[WeekWindow], timezone: str) -> WeekWindow | None:
    local_dt = ts.astimezone(ZoneInfo(timezone))
    for window in windows:
        if window.start_local <= local_dt < window.end_local:
            return window
    return None


def _rows_for_platform(
    season_id: str,
    *,
    platform: str,
    start_dt: datetime,
    end_dt: datetime,
    source_scope: str,
    target_accounts_by_platform: dict[str, set[str]] | None = None,
    include_post_text: bool = True,
) -> list[dict[str, Any]]:
    account_map = (
        target_accounts_by_platform
        if target_accounts_by_platform is not None
        else _target_accounts_by_platform(season_id, source_scope=source_scope)
    )
    platform_accounts = set(account_map.get(platform, set()))
    requires_target_accounts = source_scope in {"bravo", "creator"}
    if requires_target_accounts and not platform_accounts:
        return []
    apply_account_filter = source_scope != "community" and bool(platform_accounts)
    platform_accounts_list = sorted(platform_accounts)
    instagram_post_text_expr = "p.caption" if include_post_text else "left(coalesce(p.caption, ''), 240)"
    tiktok_post_text_expr = "p.description" if include_post_text else "left(coalesce(p.description, ''), 240)"
    youtube_post_text_expr = "v.title" if include_post_text else "left(coalesce(v.title, ''), 240)"
    twitter_post_text_expr = "t.text" if include_post_text else "left(coalesce(t.text, ''), 240)"
    facebook_post_text_expr = "p.caption" if include_post_text else "left(coalesce(p.caption, ''), 240)"
    threads_post_text_expr = "p.text" if include_post_text else "left(coalesce(p.text, ''), 240)"

    if platform == "instagram":
        thumbnail_expr = _instagram_posts_thumbnail_expr("p")
        account_filter_posts = (
            "and ltrim(lower(coalesce(nullif(p.username, ''), nullif(p.source_account, ''), '')), '@') = any(%s)"
            if apply_account_filter
            else ""
        )
        account_filter_comments = (
            "and ltrim(lower(coalesce(nullif(p.username, ''), nullif(p.source_account, ''), '')), '@') = any(%s)"
            if apply_account_filter
            else ""
        )
        params: list[Any] = [season_id, start_dt, end_dt]
        if apply_account_filter:
            params.append(platform_accounts_list)
        params.extend([season_id, start_dt, end_dt])
        if apply_account_filter:
            params.append(platform_accounts_list)
        return pg.fetch_all(
            f"""
            with posts as (
              select
                'instagram'::text as platform,
                'post'::text as kind,
                p.shortcode as source_id,
                {instagram_post_text_expr} as text,
                greatest(
                  0,
                  coalesce(p.likes, 0) + coalesce(p.comments_count, 0) + coalesce(p.views, 0)
                )::bigint as engagement,
                p.posted_at as ts,
                ('https://www.instagram.com/p/' || p.shortcode || '/')::text as url,
                p.username as author,
                {thumbnail_expr}::text as thumbnail_url,
                greatest(0, coalesce(p.comments_count, 0))::bigint as reported_comments
              from social.instagram_posts p
              where p.season_id = %s
                and p.posted_at >= %s
                and p.posted_at <= %s
                {account_filter_posts}
            ), comments as (
              select
                'instagram'::text as platform,
                'comment'::text as kind,
                c.comment_id as source_id,
                c.text as text,
                greatest(0, coalesce(c.likes, 0))::bigint as engagement,
                c.created_at as ts,
                ('https://www.instagram.com/p/' || p.shortcode || '/')::text as url,
                c.username as author,
                {thumbnail_expr}::text as thumbnail_url,
                0::bigint as reported_comments
              from social.instagram_comments c
              join social.instagram_posts p on p.id = c.post_id
              where c.season_id = %s
                and c.created_at >= %s
                and c.created_at <= %s
                {account_filter_comments}
            )
            select * from posts
            union all
            select * from comments
            """,
            params,
        )

    if platform == "tiktok":
        thumbnail_expr = _platform_thumbnail_expr("p", "tiktok")
        account_filter_posts = (
            "and ltrim(lower(coalesce(nullif(p.username, ''), nullif(p.source_account, ''), '')), '@') = any(%s)"
            if apply_account_filter
            else ""
        )
        account_filter_comments = (
            "and ltrim(lower(coalesce(nullif(p.username, ''), nullif(p.source_account, ''), '')), '@') = any(%s)"
            if apply_account_filter
            else ""
        )
        params = [season_id, start_dt, end_dt]
        if apply_account_filter:
            params.append(platform_accounts_list)
        params.extend([season_id, start_dt, end_dt])
        if apply_account_filter:
            params.append(platform_accounts_list)
        return pg.fetch_all(
            f"""
            with posts as (
              select
                'tiktok'::text as platform,
                'post'::text as kind,
                p.video_id as source_id,
                {tiktok_post_text_expr} as text,
                greatest(
                  0,
                  coalesce(p.likes, 0)
                  + coalesce(p.comments_count, 0)
                  + coalesce(p.shares, 0)
                  + coalesce(p.views, 0)
                )::bigint as engagement,
                p.posted_at as ts,
                ('https://www.tiktok.com/@' || p.username || '/video/' || p.video_id)::text as url,
                p.username as author,
                {thumbnail_expr}::text as thumbnail_url,
                greatest(0, coalesce(p.comments_count, 0))::bigint as reported_comments
              from social.tiktok_posts p
              where p.season_id = %s
                and p.posted_at >= %s
                and p.posted_at <= %s
                {account_filter_posts}
            ), comments as (
              select
                'tiktok'::text as platform,
                'comment'::text as kind,
                c.comment_id as source_id,
                c.text as text,
                greatest(0, coalesce(c.likes, 0))::bigint as engagement,
                c.created_at as ts,
                ('https://www.tiktok.com/@' || p.username || '/video/' || p.video_id)::text as url,
                c.username as author,
                {thumbnail_expr}::text as thumbnail_url,
                0::bigint as reported_comments
              from social.tiktok_comments c
              join social.tiktok_posts p on p.id = c.post_id
              where c.season_id = %s
                and c.created_at >= %s
                and c.created_at <= %s
                {account_filter_comments}
            )
            select * from posts
            union all
            select * from comments
            """,
            params,
        )

    if platform == "youtube":
        thumbnail_expr = _platform_thumbnail_expr("v", "youtube")
        youtube_is_short_expr = _youtube_is_short_expr("v")
        account_filter_videos = (
            "and ltrim(lower(coalesce(nullif(v.channel_title, ''), nullif(v.source_account, ''), '')), '@') = any(%s)"
            if apply_account_filter
            else ""
        )
        account_filter_comments = (
            "and ltrim(lower(coalesce(nullif(v.channel_title, ''), nullif(v.source_account, ''), '')), '@') = any(%s)"
            if apply_account_filter
            else ""
        )
        params = [season_id, start_dt, end_dt]
        if apply_account_filter:
            params.append(platform_accounts_list)
        params.extend([season_id, start_dt, end_dt])
        if apply_account_filter:
            params.append(platform_accounts_list)
        return pg.fetch_all(
            f"""
            with videos as (
              select
                'youtube'::text as platform,
                'post'::text as kind,
                v.video_id as source_id,
                {youtube_post_text_expr} as text,
                greatest(
                  0,
                  coalesce(v.views, 0) + coalesce(v.likes, 0) + coalesce(v.comments_count, 0)
                )::bigint as engagement,
                v.published_at as ts,
                (
                  case
                    when {youtube_is_short_expr} then 'https://www.youtube.com/shorts/' || v.video_id
                    else 'https://www.youtube.com/watch?v=' || v.video_id
                  end
                )::text as url,
                v.channel_title as author,
                {thumbnail_expr}::text as thumbnail_url,
                greatest(0, coalesce(v.comments_count, 0))::bigint as reported_comments,
                {youtube_is_short_expr} as is_short
              from social.youtube_videos v
              where v.season_id = %s
                and v.published_at >= %s
                and v.published_at <= %s
                {account_filter_videos}
            ), comments as (
              select
                'youtube'::text as platform,
                'comment'::text as kind,
                c.comment_id as source_id,
                c.text as text,
                greatest(0, coalesce(c.likes, 0))::bigint as engagement,
                c.created_at as ts,
                ('https://www.youtube.com/watch?v=' || v.video_id)::text as url,
                c.author as author,
                {thumbnail_expr}::text as thumbnail_url,
                0::bigint as reported_comments,
                false as is_short
              from social.youtube_comments c
              join social.youtube_videos v on v.id = c.video_id
              where c.season_id = %s
                and c.created_at >= %s
                and c.created_at <= %s
                {account_filter_comments}
            )
            select * from videos
            union all
            select * from comments
            """,
            params,
        )

    if platform == "twitter":
        thumbnail_expr = _platform_thumbnail_expr("t", "twitter")
        if apply_account_filter:
            return pg.fetch_all(
                f"""
                with recursive in_scope_posts as (
                  select p.tweet_id
                  from social.twitter_tweets p
                  where p.season_id = %s
                    and p.is_reply = false
                    and p.created_at >= %s
                    and p.created_at <= %s
                    and ltrim(lower(coalesce(nullif(p.username, ''), nullif(p.source_account, ''), '')), '@') = any(%s)
                ), legacy_thread_replies as (
                  select r.tweet_id
                  from social.twitter_tweets r
                  where r.season_id = %s
                    and r.is_reply = true
                    and (r.source_account is null or btrim(r.source_account) = '')
                    and r.reply_to_tweet_id in (select tweet_id from in_scope_posts)
                  union
                  select child.tweet_id
                  from social.twitter_tweets child
                  join legacy_thread_replies parent on child.reply_to_tweet_id = parent.tweet_id
                  where child.season_id = %s
                    and child.is_reply = true
                    and (child.source_account is null or btrim(child.source_account) = '')
                ), posts as (
                  select
                    'twitter'::text as platform,
                    'post'::text as kind,
                    t.tweet_id as source_id,
                    {twitter_post_text_expr} as text,
                    greatest(
                      0,
                      coalesce(t.likes, 0)
                      + coalesce(t.retweets, 0)
                      + coalesce(t.replies_count, 0)
                      + coalesce(t.quotes, 0)
                      + coalesce(t.views, 0)
                    )::bigint as engagement,
                    t.created_at as ts,
                    (
                      'https://x.com/'
                      || coalesce(nullif(t.username, ''), nullif(t.source_account, ''), 'i')
                      || '/status/'
                      || t.tweet_id
                    )::text as url,
                    coalesce(nullif(t.username, ''), nullif(t.source_account, ''), '') as author,
                    {thumbnail_expr}::text as thumbnail_url,
                    greatest(0, coalesce(t.replies_count, 0) + coalesce(t.quotes, 0))::bigint as reported_comments
                  from social.twitter_tweets t
                  where t.season_id = %s
                    and t.created_at >= %s
                    and t.created_at <= %s
                    and t.is_reply = false
                    and ltrim(lower(coalesce(nullif(t.username, ''), nullif(t.source_account, ''), '')), '@') = any(%s)
                ), comments as (
                  select
                    'twitter'::text as platform,
                    'comment'::text as kind,
                    t.tweet_id as source_id,
                    t.text as text,
                    greatest(
                      0,
                      coalesce(t.likes, 0)
                      + coalesce(t.retweets, 0)
                      + coalesce(t.replies_count, 0)
                      + coalesce(t.quotes, 0)
                      + coalesce(t.views, 0)
                    )::bigint as engagement,
                    t.created_at as ts,
                    (
                      'https://x.com/'
                      || coalesce(nullif(t.username, ''), nullif(t.source_account, ''), 'i')
                      || '/status/'
                      || t.tweet_id
                    )::text as url,
                    coalesce(nullif(t.username, ''), nullif(t.source_account, ''), '') as author,
                    {thumbnail_expr}::text as thumbnail_url,
                    0::bigint as reported_comments
                  from social.twitter_tweets t
                  where t.season_id = %s
                    and t.created_at >= %s
                    and t.created_at <= %s
                    and t.is_reply = true
                    and (
                      ltrim(lower(coalesce(nullif(t.source_account, ''), nullif(t.username, ''), '')), '@') = any(%s)
                      or t.tweet_id in (select tweet_id from legacy_thread_replies)
                    )
                )
                select * from posts
                union all
                select * from comments
                """,
                [
                    season_id,
                    start_dt,
                    end_dt,
                    platform_accounts_list,
                    season_id,
                    season_id,
                    season_id,
                    start_dt,
                    end_dt,
                    platform_accounts_list,
                    season_id,
                    start_dt,
                    end_dt,
                    platform_accounts_list,
                ],
            )

        return pg.fetch_all(
            f"""
            with posts as (
              select
                'twitter'::text as platform,
                'post'::text as kind,
                t.tweet_id as source_id,
                {twitter_post_text_expr} as text,
                greatest(
                  0,
                  coalesce(t.likes, 0)
                  + coalesce(t.retweets, 0)
                  + coalesce(t.replies_count, 0)
                  + coalesce(t.quotes, 0)
                  + coalesce(t.views, 0)
                )::bigint as engagement,
                t.created_at as ts,
                (
                  'https://x.com/'
                  || coalesce(nullif(t.username, ''), nullif(t.source_account, ''), 'i')
                  || '/status/'
                  || t.tweet_id
                )::text as url,
                coalesce(nullif(t.username, ''), nullif(t.source_account, ''), '') as author,
                {thumbnail_expr}::text as thumbnail_url,
                greatest(0, coalesce(t.replies_count, 0) + coalesce(t.quotes, 0))::bigint as reported_comments
              from social.twitter_tweets t
              where t.season_id = %s
                and t.created_at >= %s
                and t.created_at <= %s
                and t.is_reply = false
            ), comments as (
              select
                'twitter'::text as platform,
                'comment'::text as kind,
                t.tweet_id as source_id,
                t.text as text,
                greatest(
                  0,
                  coalesce(t.likes, 0)
                  + coalesce(t.retweets, 0)
                  + coalesce(t.replies_count, 0)
                  + coalesce(t.quotes, 0)
                  + coalesce(t.views, 0)
                )::bigint as engagement,
                t.created_at as ts,
                (
                  'https://x.com/'
                  || coalesce(nullif(t.username, ''), nullif(t.source_account, ''), 'i')
                  || '/status/'
                  || t.tweet_id
                )::text as url,
                coalesce(nullif(t.username, ''), nullif(t.source_account, ''), '') as author,
                {thumbnail_expr}::text as thumbnail_url,
                0::bigint as reported_comments
              from social.twitter_tweets t
              where t.season_id = %s
                and t.created_at >= %s
                and t.created_at <= %s
                and t.is_reply = true
            )
            select * from posts
            union all
            select * from comments
            """,
            [season_id, start_dt, end_dt, season_id, start_dt, end_dt],
        )

    if platform == "facebook":
        thumbnail_expr = _platform_thumbnail_expr("p", "facebook")
        account_filter_posts = (
            "and ltrim(lower(coalesce(nullif(p.username, ''), nullif(p.source_account, ''), '')), '@') = any(%s)"
            if apply_account_filter
            else ""
        )
        account_filter_comments = (
            "and ltrim(lower(coalesce(nullif(p.username, ''), nullif(p.source_account, ''), '')), '@') = any(%s)"
            if apply_account_filter
            else ""
        )
        params = [season_id, start_dt, end_dt]
        if apply_account_filter:
            params.append(platform_accounts_list)
        params.extend([season_id, start_dt, end_dt])
        if apply_account_filter:
            params.append(platform_accounts_list)
        return pg.fetch_all(
            f"""
            with posts as (
              select
                'facebook'::text as platform,
                'post'::text as kind,
                p.post_id as source_id,
                {facebook_post_text_expr} as text,
                greatest(
                  0,
                  coalesce(p.likes, 0)
                  + coalesce(p.comments_count, 0)
                  + coalesce(p.shares, 0)
                  + coalesce(p.views, 0)
                )::bigint as engagement,
                p.posted_at as ts,
                (
                  case
                    when lower(coalesce(p.post_type, '')) = 'reel' then 'https://www.facebook.com/reel/' || p.post_id
                    else (
                      'https://www.facebook.com/'
                      || coalesce(nullif(p.username, ''), nullif(p.source_account, ''), '')
                      || '/posts/'
                      || p.post_id
                    )
                  end
                )::text as url,
                coalesce(nullif(p.username, ''), nullif(p.source_account, ''), '') as author,
                {thumbnail_expr}::text as thumbnail_url,
                greatest(0, coalesce(p.comments_count, 0))::bigint as reported_comments
              from social.facebook_posts p
              where p.season_id = %s
                and p.posted_at >= %s
                and p.posted_at <= %s
                {account_filter_posts}
            ), comments as (
              select
                'facebook'::text as platform,
                'comment'::text as kind,
                c.comment_id as source_id,
                c.text as text,
                greatest(0, coalesce(c.likes, 0))::bigint as engagement,
                c.created_at as ts,
                (
                  case
                    when lower(coalesce(p.post_type, '')) = 'reel' then 'https://www.facebook.com/reel/' || p.post_id
                    else (
                      'https://www.facebook.com/'
                      || coalesce(nullif(p.username, ''), nullif(p.source_account, ''), '')
                      || '/posts/'
                      || p.post_id
                    )
                  end
                )::text as url,
                c.username as author,
                {thumbnail_expr}::text as thumbnail_url,
                0::bigint as reported_comments
              from social.facebook_comments c
              join social.facebook_posts p on p.id = c.post_id
              where c.season_id = %s
                and c.created_at >= %s
                and c.created_at <= %s
                {account_filter_comments}
            )
            select * from posts
            union all
            select * from comments
            """,
            params,
        )

    if platform == "threads":
        thumbnail_expr = _platform_thumbnail_expr("p", "threads")
        account_filter_posts = (
            "and ltrim(lower(coalesce(nullif(p.username, ''), nullif(p.source_account, ''), '')), '@') = any(%s)"
            if apply_account_filter
            else ""
        )
        account_filter_comments = (
            "and ltrim(lower(coalesce(nullif(p.username, ''), nullif(p.source_account, ''), '')), '@') = any(%s)"
            if apply_account_filter
            else ""
        )
        params = [season_id, start_dt, end_dt]
        if apply_account_filter:
            params.append(platform_accounts_list)
        params.extend([season_id, start_dt, end_dt])
        if apply_account_filter:
            params.append(platform_accounts_list)
        return pg.fetch_all(
            f"""
            with posts as (
              select
                'threads'::text as platform,
                'post'::text as kind,
                p.post_id as source_id,
                {threads_post_text_expr} as text,
                greatest(
                  0,
                  coalesce(p.likes, 0)
                  + coalesce(p.replies_count, 0)
                  + coalesce(p.reposts, 0)
                  + coalesce(p.quotes, 0)
                  + coalesce(p.views, 0)
                )::bigint as engagement,
                p.posted_at as ts,
                (
                  'https://www.threads.com/@'
                  || coalesce(nullif(p.username, ''), nullif(p.source_account, ''), '')
                  || '/post/'
                  || p.post_id
                )::text as url,
                coalesce(nullif(p.username, ''), nullif(p.source_account, ''), '') as author,
                {thumbnail_expr}::text as thumbnail_url,
                greatest(0, coalesce(p.replies_count, 0))::bigint as reported_comments
              from social.meta_threads_posts p
              where p.season_id = %s
                and p.posted_at >= %s
                and p.posted_at <= %s
                {account_filter_posts}
            ), comments as (
              select
                'threads'::text as platform,
                'comment'::text as kind,
                c.comment_id as source_id,
                c.text as text,
                greatest(0, coalesce(c.likes, 0))::bigint as engagement,
                c.created_at as ts,
                (
                  'https://www.threads.com/@'
                  || coalesce(nullif(p.username, ''), nullif(p.source_account, ''), '')
                  || '/post/'
                  || p.post_id
                )::text as url,
                c.username as author,
                {thumbnail_expr}::text as thumbnail_url,
                0::bigint as reported_comments
              from social.meta_threads_comments c
              join social.meta_threads_posts p on p.id = c.post_id
              where c.season_id = %s
                and c.created_at >= %s
                and c.created_at <= %s
                {account_filter_comments}
            )
            select * from posts
            union all
            select * from comments
            """,
            params,
        )

    return []


def _compute_post_metadata(
    season_id: str,
    *,
    platforms: list[str],
    start_dt: datetime,
    end_dt: datetime,
    target_accounts_by_platform: dict[str, set[str]],
) -> dict[str, Any] | None:
    """Compute post metadata coverage across all platform tables."""
    platform_queries: dict[str, dict[str, str | None]] = {
        "instagram": {
            "table": "social.instagram_posts",
            "ts_col": "posted_at",
            "text_expr": "p.caption",
            "token_text_expr": "p.caption",
            "hashtags_col": "hashtags",
            "mentions_col": "mentions",
            "collaborators_col": "collaborators",
            "media_type_col": "media_type",
        },
        "tiktok": {
            "table": "social.tiktok_posts",
            "ts_col": "posted_at",
            "text_expr": "p.description",
            "token_text_expr": "p.description",
            "hashtags_col": "hashtags",
            "mentions_col": "mentions",
            "collaborators_col": None,
            "media_type_col": None,
        },
        "youtube": {
            "table": "social.youtube_videos",
            "ts_col": "published_at",
            "text_expr": "coalesce(nullif(trim(p.description), ''), nullif(trim(p.title), ''))",
            "token_text_expr": "coalesce(p.description, p.title, '')",
            "hashtags_col": "hashtags",
            "mentions_col": "mentions",
            "collaborators_col": None,
            "media_type_col": None,
        },
        "twitter": {
            "table": "social.twitter_tweets",
            "ts_col": "created_at",
            "text_expr": "p.text",
            "token_text_expr": "p.text",
            "hashtags_col": "hashtags",
            "mentions_col": "mentions",
            "collaborators_col": None,
            "media_type_col": None,
        },
        "facebook": {
            "table": "social.facebook_posts",
            "ts_col": "posted_at",
            "text_expr": "p.caption",
            "token_text_expr": "p.caption",
            "hashtags_col": "hashtags",
            "mentions_col": "mentions",
            "collaborators_col": None,
            "media_type_col": "post_type",
        },
        "threads": {
            "table": "social.meta_threads_posts",
            "ts_col": "posted_at",
            "text_expr": "p.text",
            "token_text_expr": "p.text",
            "hashtags_col": "hashtags",
            "mentions_col": "mentions",
            "collaborators_col": None,
            "media_type_col": "media_type",
        },
    }

    total_posts = 0
    posts_with_caption = 0
    posts_with_tags = 0
    posts_with_mentions = 0
    posts_with_collaborators = 0
    content_type_counts: dict[str, int] = {}

    for platform in platforms:
        cfg = platform_queries.get((platform or "").strip().lower())
        if not cfg:
            continue
        table = str(cfg["table"])
        ts_col = str(cfg["ts_col"])
        text_expr = str(cfg["text_expr"])
        token_text_expr = str(cfg["token_text_expr"])
        hashtags_col_raw = cfg.get("hashtags_col")
        mentions_col_raw = cfg.get("mentions_col")
        collaborators_col_raw = cfg.get("collaborators_col")
        media_type_col_raw = cfg.get("media_type_col")

        hashtags_col = (
            str(hashtags_col_raw)
            if hashtags_col_raw and _platform_posts_has_column(platform, str(hashtags_col_raw))
            else None
        )
        mentions_col = (
            str(mentions_col_raw)
            if mentions_col_raw and _platform_posts_has_column(platform, str(mentions_col_raw))
            else None
        )
        collaborators_col = (
            str(collaborators_col_raw)
            if collaborators_col_raw and _platform_posts_has_column(platform, str(collaborators_col_raw))
            else None
        )
        media_type_col = (
            str(media_type_col_raw)
            if media_type_col_raw and _platform_posts_has_column(platform, str(media_type_col_raw))
            else None
        )

        account_handles = sorted(target_accounts_by_platform.get(platform, set()))
        if platform == "youtube":
            account_filter = (
                "and ltrim(lower(coalesce(nullif(p.channel_title, ''), nullif(p.source_account, ''), '')), '@') = any(%s)"
                if account_handles
                else ""
            )
        else:
            account_filter = (
                "and ltrim(lower(coalesce(nullif(p.username, ''), nullif(p.source_account, ''), '')), '@') = any(%s)"
                if account_handles
                else ""
            )

        has_caption_expr = f"nullif(trim({text_expr}), '') is not null"
        hashtag_regex_expr = f"coalesce({token_text_expr}, '') ~ '(?<![A-Za-z0-9_.])#[A-Za-z0-9_]+'"
        mention_regex_expr = f"coalesce({token_text_expr}, '') ~ '(?<![A-Za-z0-9_.])@[A-Za-z0-9_.]+'"

        hashtag_json_expr = (
            f"(p.{hashtags_col} is not null and jsonb_typeof(p.{hashtags_col}) = 'array' and jsonb_array_length(p.{hashtags_col}) > 0)"
            if hashtags_col
            else None
        )
        mention_json_expr = (
            f"(p.{mentions_col} is not null and jsonb_typeof(p.{mentions_col}) = 'array' and jsonb_array_length(p.{mentions_col}) > 0)"
            if mentions_col
            else None
        )
        collaborators_json_expr = (
            f"(p.{collaborators_col} is not null and jsonb_typeof(p.{collaborators_col}) = 'array' and jsonb_array_length(p.{collaborators_col}) > 0)"
            if collaborators_col
            else None
        )

        has_tags_expr = (
            f"({hashtag_json_expr} or {hashtag_regex_expr})"
            if hashtag_json_expr
            else hashtag_regex_expr
        )
        has_mentions_expr = (
            f"({mention_json_expr} or {mention_regex_expr})"
            if mention_json_expr
            else mention_regex_expr
        )

        selects = [
            "count(*) as total",
            f"count(*) filter (where {has_caption_expr}) as has_caption",
            f"count(*) filter (where {has_tags_expr}) as has_tags",
            f"count(*) filter (where {has_mentions_expr}) as has_mentions",
        ]
        if collaborators_json_expr:
            selects.append(
                f"count(*) filter (where {collaborators_json_expr}) as has_collaborators"
            )
        if media_type_col:
            selects.append(f"coalesce(nullif(trim(p.{media_type_col}), ''), 'other') as mtype")
            group_by = f"group by coalesce(nullif(trim(p.{media_type_col}), ''), 'other')"
        else:
            group_by = ""

        query_selects = ", ".join(selects)
        params: list[Any] = [season_id, start_dt, end_dt]
        if account_handles:
            params.append(account_handles)

        sql = f"""
            select {query_selects}
            from {table} p
            where p.season_id = %s
              and p.{ts_col} >= %s
              and p.{ts_col} <= %s
              {account_filter}
            {group_by}
        """
        db_rows = pg.fetch_all(sql, params)
        if media_type_col:
            for row in db_rows:
                count = int(row.get("total") or 0)
                total_posts += count
                posts_with_caption += int(row.get("has_caption") or 0)
                posts_with_tags += int(row.get("has_tags") or 0)
                posts_with_mentions += int(row.get("has_mentions") or 0)
                if collaborators_json_expr:
                    posts_with_collaborators += int(row.get("has_collaborators") or 0)
                mtype = str(row.get("mtype") or "other").lower().strip()
                if not mtype:
                    mtype = "other"
                content_type_counts[mtype] = content_type_counts.get(mtype, 0) + count
        elif db_rows:
            row = db_rows[0]
            count = int(row.get("total") or 0)
            total_posts += count
            posts_with_caption += int(row.get("has_caption") or 0)
            posts_with_tags += int(row.get("has_tags") or 0)
            posts_with_mentions += int(row.get("has_mentions") or 0)
            if collaborators_json_expr:
                posts_with_collaborators += int(row.get("has_collaborators") or 0)
            # Assign a generic content type for platforms without media_type column
            generic_type = "video" if platform in ("tiktok", "youtube") else "post"
            if count > 0:
                content_type_counts[generic_type] = content_type_counts.get(generic_type, 0) + count

    if total_posts == 0:
        return None

    def _coverage(with_count: int) -> dict[str, Any]:
        return {
            "posts_with": with_count,
            "pct": round((with_count * 100.0) / total_posts, 1) if total_posts > 0 else None,
        }

    # Normalize raw media_type codes to canonical labels
    media_type_normalize: dict[str, str] = {
        "1": "photo", "2": "photo", "image": "photo",
        "8": "video", "reel": "video",
        "19": "album", "carousel": "album", "carousel_album": "album",
        "sidecar": "album",
        "graphimage": "photo", "graphvideo": "video", "graphsidecar": "album",
    }
    normalized_counts: dict[str, int] = {}
    for raw_key, count in content_type_counts.items():
        key = media_type_normalize.get(raw_key, raw_key)
        normalized_counts[key] = normalized_counts.get(key, 0) + count

    # Build content type buckets
    buckets = []
    for key, count in sorted(normalized_counts.items(), key=lambda kv: -kv[1]):
        buckets.append({
            "key": key,
            "count": count,
            "pct": round((count * 100.0) / total_posts, 1) if total_posts > 0 else None,
        })

    return {
        "total_posts": total_posts,
        "captions": _coverage(posts_with_caption),
        "tags": _coverage(posts_with_tags),
        "mentions": _coverage(posts_with_mentions),
        "collaborators": _coverage(posts_with_collaborators),
        "content_types": {
            "total_posts": total_posts,
            "buckets": buckets,
        },
    }


def _build_rows(
    season_id: str,
    *,
    platforms: list[str],
    start_dt: datetime,
    end_dt: datetime,
    source_scope: str,
    season_context: SeasonContext,
    analyzer_context: SentimentAnalyzerContext,
    target_accounts_by_platform: dict[str, set[str]],
    include_post_text: bool = True,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for platform in platforms:
        try:
            platform_rows = _rows_for_platform(
                season_id,
                platform=platform,
                start_dt=start_dt,
                end_dt=end_dt,
                source_scope=source_scope,
                target_accounts_by_platform=target_accounts_by_platform,
                include_post_text=include_post_text,
            )
        except TypeError as exc:
            if "include_post_text" not in str(exc):
                raise
            platform_rows = _rows_for_platform(
                season_id,
                platform=platform,
                start_dt=start_dt,
                end_dt=end_dt,
                source_scope=source_scope,
                target_accounts_by_platform=target_accounts_by_platform,
            )
        rows.extend(platform_rows)

    normalized: list[dict[str, Any]] = []
    comment_sentiment_cache: dict[str, tuple[str, int, float, bool, str]] = {}
    for row in rows:
        ts = _coerce_dt(row.get("ts"))
        if ts is None:
            continue
        kind = str(row.get("kind") or "").lower()
        text_value = row.get("text")
        text = str(text_value) if text_value is not None else ""
        if kind == "comment":
            sentiment_cache_key = (
                f"{str(row.get('platform') or '').lower()}:{str(row.get('source_id') or '')}:"
                f"{hashlib.sha1(_normalize_sentiment_text_key(text).encode('utf-8', 'ignore')).hexdigest()}"
            )
            cached_sentiment = comment_sentiment_cache.get(sentiment_cache_key)
            if cached_sentiment is None:
                rule_result = _rule_based_sentiment_for_text(text, analyzer_context=analyzer_context)
                sentiment = rule_result.label
                if sentiment == "neutral":
                    score = 0
                else:
                    rounded = int(round(rule_result.score))
                    score = rounded if rounded != 0 else _score_from_label(sentiment)
                confidence = rule_result.confidence
                ambiguous = rule_result.ambiguous
                sentiment_key = _normalize_sentiment_text_key(text)
                cached_sentiment = (sentiment, score, confidence, ambiguous, sentiment_key)
                comment_sentiment_cache[sentiment_cache_key] = cached_sentiment
            sentiment, score, confidence, ambiguous, sentiment_key = cached_sentiment
        else:
            sentiment = "neutral"
            score = 0
            confidence = 1.0
            ambiguous = False
            sentiment_key = ""
        normalized.append(
            {
                "platform": str(row.get("platform") or ""),
                "kind": kind,
                "source_id": str(row.get("source_id") or ""),
                "text": text,
                "engagement": int(row.get("engagement") or 0),
                "ts": ts,
                "url": str(row.get("url") or ""),
                "author": str(row.get("author") or ""),
                "thumbnail_url": str(row.get("thumbnail_url") or "") or None,
                "reported_comments": int(row.get("reported_comments") or 0),
                "is_short": bool(row.get("is_short")) if str(row.get("platform") or "") == "youtube" else False,
                "sentiment": sentiment,
                "sentiment_score": score,
                "_sentiment_confidence": confidence,
                "_sentiment_ambiguous": ambiguous,
                "_sentiment_key": sentiment_key,
            }
        )

    _apply_optional_gemini_sentiment(normalized, context=season_context, analyzer_context=analyzer_context)
    normalized.sort(key=lambda item: item["ts"], reverse=True)
    return normalized


def _build_drivers(
    rows: list[dict[str, Any]],
    *,
    analyzer_context: SentimentAnalyzerContext | None = None,
) -> dict[str, list[dict[str, Any]]]:
    effective_context = analyzer_context or _default_sentiment_context()
    token_counts: Counter[str] = Counter()
    token_scores: defaultdict[str, int] = defaultdict(int)
    blocked_terms = set(STOPWORDS)
    blocked_terms.update(effective_context.cast_terms)

    account_terms: set[str] = set()
    for row in rows:
        account_terms.update(_tokenize_handle_terms(str(row.get("author") or "")))
        for mention in MENTION_TOKEN_RE.findall(str(row.get("text") or "")):
            account_terms.update(_tokenize_handle_terms(mention))
    blocked_terms.update(account_terms)

    for row in rows:
        if row["kind"] != "comment":
            continue
        text = row.get("text") or ""
        sentiment = str(row.get("sentiment") or "neutral").lower()
        sentiment_score = _score_from_label(sentiment)
        if sentiment_score == 0:
            continue
        row_mentions = {
            token.lower()
            for mention in MENTION_TOKEN_RE.findall(str(text))
            for token in _tokenize_handle_terms(mention)
        }
        tokens = [token.lower() for token in TOKEN_RE.findall(text)]
        for token in tokens:
            if len(token) < 4 or token in blocked_terms or token in row_mentions:
                continue
            token_counts[token] += 1
            token_scores[token] += sentiment_score

    positive = [
        {
            "term": term,
            "count": token_counts[term],
            "score": token_scores[term],
        }
        for term in token_counts
        if token_counts[term] >= 2 and token_scores[term] > 0
    ]
    negative = [
        {
            "term": term,
            "count": token_counts[term],
            "score": token_scores[term],
        }
        for term in token_counts
        if token_counts[term] >= 2 and token_scores[term] < 0
    ]

    positive.sort(key=lambda item: (-item["score"], -item["count"], item["term"]))
    negative.sort(key=lambda item: (item["score"], -item["count"], item["term"]))

    return {
        "positive": positive[:10],
        "negative": negative[:10],
    }


def _platform_int_payload(values: dict[str, Any] | None) -> dict[str, int]:
    payload = values if isinstance(values, dict) else {}
    return {platform: int(payload.get(platform, 0) or 0) for platform in SUPPORTED_PLATFORMS}


def _analytics_cache_key(
    *,
    season_id: str,
    platforms: list[str] | None,
    timezone: str,
    week: int | None,
    source_scope: str,
    include_rows: bool,
    include_jobs: bool,
    include_flags: bool,
    include_schedule: bool,
    include_benchmark: bool,
) -> tuple[Any, ...]:
    platform_key = tuple(sorted(platforms or []))
    return (
        season_id,
        platform_key,
        timezone,
        week,
        source_scope,
        include_rows,
        include_jobs,
        include_flags,
        include_schedule,
        include_benchmark,
    )


def _get_cached_analytics(cache_key: tuple[Any, ...]) -> dict[str, Any] | None:
    now = time_module.monotonic()
    with _analytics_cache_lock:
        cached = _analytics_cache.get(cache_key)
        if cached is None:
            return None
        expires_at, payload = cached
        if expires_at <= now:
            _analytics_cache.pop(cache_key, None)
            return None
        return copy.deepcopy(payload)


def _set_cached_analytics(cache_key: tuple[Any, ...], payload: dict[str, Any]) -> None:
    with _analytics_cache_lock:
        _analytics_cache[cache_key] = (
            time_module.monotonic() + ANALYTICS_CACHE_TTL_SECONDS,
            copy.deepcopy(payload),
        )


def get_analytics(
    season_id: str,
    *,
    platforms: list[str] | None,
    timezone: str,
    week: int | None,
    source_scope: str,
    include_rows: bool = False,
    include_jobs: bool = False,
    include_flags: bool = True,
    include_schedule: bool = True,
    include_benchmark: bool = True,
) -> dict[str, Any]:
    cache_key = _analytics_cache_key(
        season_id=season_id,
        platforms=platforms,
        timezone=timezone,
        week=week,
        source_scope=source_scope,
        include_rows=include_rows,
        include_jobs=include_jobs,
        include_flags=include_flags,
        include_schedule=include_schedule,
        include_benchmark=include_benchmark,
    )
    cached_response = _get_cached_analytics(cache_key)
    if cached_response is not None:
        return cached_response

    context = get_season_context(season_id)
    available_platforms = [
        platform for platform in (platforms or list(SUPPORTED_PLATFORMS)) if platform in SUPPORTED_PLATFORMS
    ]
    if not available_platforms:
        available_platforms = list(SUPPORTED_PLATFORMS)

    now = _now_utc()
    week_windows, week_zero_start_local = _resolve_week_windows(
        context,
        timezone=timezone,
        source_scope=source_scope,
        now_utc=now,
    )
    windows_by_index = {item.week_index: item for item in week_windows}

    selected_window = windows_by_index.get(week) if week is not None else None
    if week is not None and selected_window is None:
        raise ValueError(f"Week {week} is not available for this season")

    latest_window_end_local = max(
        (item.end_local for item in week_windows),
        default=week_zero_start_local,
    )
    latest_window_end_utc = latest_window_end_local.astimezone(UTC)

    if selected_window:
        start_dt = selected_window.start_local.astimezone(UTC)
        end_dt = (selected_window.end_local - timedelta(microseconds=1)).astimezone(UTC)
    else:
        start_dt = week_zero_start_local.astimezone(UTC)
        end_dt = min(now, latest_window_end_utc)
    if end_dt < start_dt:
        end_dt = start_dt

    sentiment_context = _build_sentiment_context(context)
    target_accounts_by_platform = _target_accounts_by_platform(
        season_id,
        source_scope=source_scope,
        context=context,
    )
    rows = _build_rows(
        season_id,
        platforms=available_platforms,
        start_dt=start_dt,
        end_dt=end_dt,
        source_scope=source_scope,
        season_context=context,
        analyzer_context=sentiment_context,
        target_accounts_by_platform=target_accounts_by_platform,
        include_post_text=include_rows,
    )

    try:
        post_metadata = _compute_post_metadata(
            season_id,
            platforms=available_platforms,
            start_dt=start_dt,
            end_dt=end_dt,
            target_accounts_by_platform=target_accounts_by_platform,
        )
    except Exception as exc:
        exc_text = str(exc).lower()
        if exc_text.startswith("database pool initialization failed") or "invalid input syntax for type uuid" in exc_text:
            logger.debug("Skipping data quality post metadata due unavailable database URL")
            post_metadata = None
        else:
            raise

    posts = [row for row in rows if row["kind"] == "post"]
    comments = [row for row in rows if row["kind"] == "comment"]

    sentiment_counts = {
        "positive": sum(1 for row in comments if row["sentiment"] == "positive"),
        "neutral": sum(1 for row in comments if row["sentiment"] == "neutral"),
        "negative": sum(1 for row in comments if row["sentiment"] == "negative"),
    }

    total_comments = max(1, len(comments))
    sentiment_mix = {
        "positive": round(sentiment_counts["positive"] / total_comments, 4),
        "neutral": round(sentiment_counts["neutral"] / total_comments, 4),
        "negative": round(sentiment_counts["negative"] / total_comments, 4),
        "counts": sentiment_counts,
    }

    visible_windows = [selected_window] if selected_window else week_windows
    weekly_map: dict[int, dict[str, Any]] = {
        item.week_index: {
            "post_volume": 0,
            "comment_volume": 0,
            "engagement": 0,
            "sentiment": {"positive": 0, "neutral": 0, "negative": 0},
            "week_start": item.start_local,
            "week_end": item.end_local,
            "week_type": item.week_type,
            "episode_number": item.episode_number,
            "label": _week_window_label(item, timezone=timezone),
        }
        for item in visible_windows
    }

    for row in rows:
        week_window = _week_for_timestamp(row["ts"], windows=visible_windows, timezone=timezone)
        if not week_window:
            continue
        entry = weekly_map[week_window.week_index]
        entry["engagement"] += int(row["engagement"])
        if row["kind"] == "post":
            entry["post_volume"] += 1
        else:
            entry["comment_volume"] += 1
            entry["sentiment"][row["sentiment"]] += 1

    weekly = []
    for week_index in sorted(weekly_map):
        entry = weekly_map[week_index]
        week_end_inclusive = entry["week_end"] - timedelta(microseconds=1)
        weekly.append(
            {
                "week_index": week_index,
                "label": entry["label"],
                "start": _iso(entry["week_start"].astimezone(UTC)),
                "end": _iso(week_end_inclusive.astimezone(UTC)),
                "week_type": entry["week_type"],
                "episode_number": entry["episode_number"],
                "post_volume": entry["post_volume"],
                "comment_volume": entry["comment_volume"],
                "engagement": entry["engagement"],
                "sentiment": entry["sentiment"],
            }
        )

    weekly_platform_posts_map: dict[int, dict[str, int]] = {
        item.week_index: dict.fromkeys(SUPPORTED_PLATFORMS, 0) for item in visible_windows
    }
    weekly_platform_comments_map: dict[int, dict[str, int]] = {
        item.week_index: dict.fromkeys(SUPPORTED_PLATFORMS, 0) for item in visible_windows
    }
    weekly_platform_reported_comments_map: dict[int, dict[str, int]] = {
        item.week_index: dict.fromkeys(SUPPORTED_PLATFORMS, 0) for item in visible_windows
    }
    weekly_platform_engagement_map: dict[int, dict[str, int]] = {
        item.week_index: dict.fromkeys(SUPPORTED_PLATFORMS, 0) for item in visible_windows
    }
    local_zone = ZoneInfo(timezone)
    weekly_daily_activity_map: dict[int, dict[str, Any]] = {}
    day_seconds = 24 * 60 * 60
    for window in visible_windows:
        week_start_local = window.start_local.astimezone(local_zone)
        week_end_local = window.end_local.astimezone(local_zone)
        duration_seconds = max(0.0, (week_end_local - week_start_local).total_seconds())
        day_count = max(1, int((duration_seconds + day_seconds - 1) // day_seconds))
        days: list[dict[str, Any]] = []
        for day_index in range(day_count):
            day_start = week_start_local + timedelta(days=day_index)
            days.append(
                {
                    "day_index": day_index,
                    "date_local": day_start.date().isoformat(),
                    "posts": dict.fromkeys(SUPPORTED_PLATFORMS, 0),
                    "comments": dict.fromkeys(SUPPORTED_PLATFORMS, 0),
                    "reported_comments": dict.fromkeys(SUPPORTED_PLATFORMS, 0),
                    "total_posts": 0,
                    "total_comments": 0,
                    "total_reported_comments": 0,
                }
            )
        weekly_daily_activity_map[window.week_index] = {
            "week_index": window.week_index,
            "days": days,
            "week_start": week_start_local,
            "week_start_date": week_start_local.date(),
        }
    for row in rows:
        week_window = _week_for_timestamp(row["ts"], windows=visible_windows, timezone=timezone)
        if not week_window:
            continue
        platform = row["platform"]
        if platform in weekly_platform_engagement_map[week_window.week_index]:
            weekly_platform_engagement_map[week_window.week_index][platform] += int(row["engagement"] or 0)
        if row["kind"] == "post" and platform in weekly_platform_posts_map[week_window.week_index]:
            weekly_platform_posts_map[week_window.week_index][platform] += 1
            weekly_platform_reported_comments_map[week_window.week_index][platform] += int(
                row.get("reported_comments") or 0
            )
        elif row["kind"] == "comment" and platform in weekly_platform_comments_map[week_window.week_index]:
            weekly_platform_comments_map[week_window.week_index][platform] += 1

        day_bucket = weekly_daily_activity_map.get(week_window.week_index)
        if not day_bucket:
            continue
        day_entries = day_bucket.get("days") or []
        if not day_entries:
            continue
        local_ts = row["ts"].astimezone(local_zone)
        day_index = (local_ts.date() - day_bucket["week_start_date"]).days
        if day_index < 0:
            continue
        if day_index >= len(day_entries):
            day_index = len(day_entries) - 1
        day_entry = day_entries[day_index]
        if platform not in day_entry["posts"]:
            continue
        if row["kind"] == "post":
            day_entry["posts"][platform] += 1
            day_entry["total_posts"] += 1
            reported_comments = int(row.get("reported_comments") or 0)
            day_entry["reported_comments"][platform] += reported_comments
            day_entry["total_reported_comments"] += reported_comments
        elif row["kind"] == "comment":
            day_entry["comments"][platform] += 1
            day_entry["total_comments"] += 1

    weekly_platform_posts: list[dict[str, Any]] = []
    for week_entry in weekly:
        week_index = int(week_entry["week_index"])
        platform_posts = weekly_platform_posts_map.get(
            week_index,
            dict.fromkeys(SUPPORTED_PLATFORMS, 0),
        )
        platform_comments = weekly_platform_comments_map.get(
            week_index,
            dict.fromkeys(SUPPORTED_PLATFORMS, 0),
        )
        platform_reported_comments = weekly_platform_reported_comments_map.get(
            week_index,
            dict.fromkeys(SUPPORTED_PLATFORMS, 0),
        )
        saved_total_comments = int(sum(platform_comments.values()))
        total_reported_comments = int(sum(platform_reported_comments.values()))
        comments_saved_pct: float | None = None
        if total_reported_comments > 0:
            comments_saved_pct = round(min(100.0, (saved_total_comments * 100.0) / total_reported_comments), 1)
        weekly_platform_posts.append(
            {
                "week_index": week_index,
                "label": week_entry["label"],
                "start": week_entry["start"],
                "end": week_entry["end"],
                "week_type": week_entry.get("week_type"),
                "episode_number": week_entry.get("episode_number"),
                "posts": _platform_int_payload(platform_posts),
                "comments": _platform_int_payload(platform_comments),
                "reported_comments": _platform_int_payload(platform_reported_comments),
                "total_posts": int(sum(platform_posts.values())),
                "total_comments": saved_total_comments,
                "total_reported_comments": total_reported_comments,
                "comments_saved_pct": comments_saved_pct,
            }
        )

    weekly_platform_engagement: list[dict[str, Any]] = []
    for week_entry in weekly:
        week_index = int(week_entry["week_index"])
        platform_engagement = weekly_platform_engagement_map.get(
            week_index,
            dict.fromkeys(SUPPORTED_PLATFORMS, 0),
        )
        engagement_payload = _platform_int_payload(platform_engagement)
        total_engagement = int(sum(engagement_payload.values()))
        weekly_platform_engagement.append(
            {
                "week_index": week_index,
                "label": week_entry["label"],
                "start": week_entry["start"],
                "end": week_entry["end"],
                "week_type": week_entry.get("week_type"),
                "episode_number": week_entry.get("episode_number"),
                "engagement": engagement_payload,
                "total_engagement": total_engagement,
                "has_data": total_engagement > 0,
            }
        )

    weekly_daily_activity: list[dict[str, Any]] = []
    for week_entry in weekly:
        week_index = int(week_entry["week_index"])
        day_entries = (weekly_daily_activity_map.get(week_index) or {}).get("days") or []
        days_payload: list[dict[str, Any]] = []
        for day_entry in day_entries:
            posts_payload = day_entry.get("posts") or {}
            comments_payload = day_entry.get("comments") or {}
            reported_comments_payload = day_entry.get("reported_comments") or {}
            days_payload.append(
                {
                    "day_index": int(day_entry.get("day_index", 0)),
                    "date_local": str(day_entry.get("date_local") or ""),
                    "posts": _platform_int_payload(posts_payload),
                    "comments": _platform_int_payload(comments_payload),
                    "reported_comments": _platform_int_payload(reported_comments_payload),
                    "total_posts": int(day_entry.get("total_posts", 0)),
                    "total_comments": int(day_entry.get("total_comments", 0)),
                    "total_reported_comments": int(day_entry.get("total_reported_comments", 0)),
                }
            )

        weekly_daily_activity.append(
            {
                "week_index": week_index,
                "label": week_entry["label"],
                "start": week_entry["start"],
                "end": week_entry["end"],
                "week_type": week_entry.get("week_type"),
                "episode_number": week_entry.get("episode_number"),
                "days": days_payload,
            }
        )

    comments_saved_by_platform = dict.fromkeys(SUPPORTED_PLATFORMS, 0)
    reported_comments_by_platform = dict.fromkeys(SUPPORTED_PLATFORMS, 0)
    for week_row in weekly_platform_posts:
        comments_payload = week_row.get("comments") if isinstance(week_row.get("comments"), dict) else {}
        reported_payload = (
            week_row.get("reported_comments") if isinstance(week_row.get("reported_comments"), dict) else {}
        )
        for platform in SUPPORTED_PLATFORMS:
            comments_saved_by_platform[platform] += int(comments_payload.get(platform, 0))
            reported_comments_by_platform[platform] += int(reported_payload.get(platform, 0))

    platform_comments_saved_pct = {
        platform: (
            _safe_percent(
                comments_saved_by_platform.get(platform, 0),
                reported_comments_by_platform.get(platform, 0),
            )
            if platform in available_platforms
            else None
        )
        for platform in SUPPORTED_PLATFORMS
    }
    total_saved_comments = sum(comments_saved_by_platform.values())
    total_reported_comments = sum(reported_comments_by_platform.values())
    comments_saved_pct_overall = _safe_percent(total_saved_comments, total_reported_comments)
    youtube_posts = [row for row in posts if str(row.get("platform") or "") == "youtube"]
    youtube_reels_count = sum(
        1 for row in youtube_posts if bool(row.get("is_short")) or "/shorts/" in str(row.get("url") or "")
    )
    youtube_videos_count = max(0, len(youtube_posts) - youtube_reels_count)
    youtube_content_breakdown = {
        "videos_count": youtube_videos_count,
        "reels_count": youtube_reels_count,
        "total_count": len(youtube_posts),
    }

    last_post_dt = max((row["ts"] for row in posts), default=None)
    last_comment_dt = max((row["ts"] for row in comments), default=None)
    freshness_anchor = max(
        [value for value in [last_post_dt, last_comment_dt] if isinstance(value, datetime)],
        default=None,
    )
    data_freshness_minutes: int | None = None
    if isinstance(freshness_anchor, datetime):
        data_freshness_minutes = max(0, int((now - freshness_anchor).total_seconds() // 60))

    weekly_flags: list[dict[str, Any]] = []
    weekly_by_index = sorted(weekly, key=lambda item: int(item.get("week_index", 0)))
    trend_weeks = [item for item in weekly_by_index if str(item.get("week_type") or "") != "bye"]
    weekly_posts_by_index = {
        int(item.get("week_index", 0)): item
        for item in weekly_platform_posts
        if isinstance(item.get("week_index"), int)
    }
    for idx, week_entry in enumerate(trend_weeks):
        week_index = int(week_entry.get("week_index", 0))
        post_volume = int(week_entry.get("post_volume") or 0)
        if post_volume == 0:
            weekly_flags.append(
                {
                    "week_index": week_index,
                    "code": "zero_activity",
                    "severity": "info",
                    "message": "No posts captured in this week window.",
                }
            )

        trailing_values = [int(item.get("post_volume") or 0) for item in trend_weeks[max(0, idx - 2) : idx]]
        trailing_avg = (sum(trailing_values) / len(trailing_values)) if trailing_values else 0.0
        if trailing_avg > 0:
            if post_volume >= 6 and post_volume >= (2.0 * trailing_avg):
                weekly_flags.append(
                    {
                        "week_index": week_index,
                        "code": "spike",
                        "severity": "warn",
                        "message": f"Post volume spike vs trailing 2-week average ({trailing_avg:.1f}).",
                    }
                )
            if trailing_avg >= 4 and post_volume <= (0.4 * trailing_avg):
                weekly_flags.append(
                    {
                        "week_index": week_index,
                        "code": "drop",
                        "severity": "warn",
                        "message": f"Post volume drop vs trailing 2-week average ({trailing_avg:.1f}).",
                    }
                )

        weekly_post_row = weekly_posts_by_index.get(week_index) or {}
        week_reported_comments = int(weekly_post_row.get("total_reported_comments") or 0)
        week_saved_pct = weekly_post_row.get("comments_saved_pct")
        if week_reported_comments > 0 and isinstance(week_saved_pct, (int, float)) and float(week_saved_pct) < 60.0:
            weekly_flags.append(
                {
                    "week_index": week_index,
                    "code": "comment_gap",
                    "severity": "warn",
                    "message": f"Only {float(week_saved_pct):.1f}% of expected comments are saved.",
                }
            )

    schedule_profile: dict[str, Any] | None = None
    if include_schedule:
        schedule_platforms: list[dict[str, Any]] = []
        for platform in [item for item in SUPPORTED_PLATFORMS if item in available_platforms]:
            day_values: list[int] = []
            for week_row in weekly_daily_activity:
                for day_row in week_row.get("days") or []:
                    posts_payload = day_row.get("posts") or {}
                    day_values.append(int(posts_payload.get(platform, 0)))
            zero_days = sum(1 for value in day_values if value <= 0)
            active_days = sum(1 for value in day_values if value > 0)
            schedule_platforms.append(
                {
                    "platform": platform,
                    "zero_days": zero_days,
                    "peak_day_posts": max(day_values) if day_values else 0,
                    "median_day_posts": _median_int(day_values),
                    "active_days": active_days,
                }
            )
        schedule_profile = {
            "timezone": timezone,
            "platforms": schedule_platforms,
        }

    benchmark: dict[str, Any] | None = None
    if include_benchmark:
        benchmark_weeks = sorted(weekly, key=lambda item: int(item.get("week_index", 0)))
        current_week_entry: dict[str, Any] | None = None
        if week is not None:
            current_week_entry = next(
                (item for item in benchmark_weeks if int(item.get("week_index", -1)) == week),
                None,
            )
        if current_week_entry is None and benchmark_weeks:
            current_week_entry = benchmark_weeks[-1]

        def _metrics_payload(item: dict[str, Any] | None) -> dict[str, int]:
            return {
                "posts": int((item or {}).get("post_volume") or 0),
                "comments": int((item or {}).get("comment_volume") or 0),
                "engagement": int((item or {}).get("engagement") or 0),
            }

        def _delta_pct(current_value: int, baseline_value: int) -> float | None:
            if baseline_value <= 0:
                return None
            return round(((current_value - baseline_value) * 100.0) / float(baseline_value), 1)

        current_index = int((current_week_entry or {}).get("week_index", -1))
        previous_week_entry = next(
            (item for item in benchmark_weeks if int(item.get("week_index", -1)) == current_index - 1),
            None,
        )
        trailing_prev_weeks = [item for item in benchmark_weeks if int(item.get("week_index", -1)) < current_index][-3:]
        trailing_avg_metrics = {
            "posts": 0.0,
            "comments": 0.0,
            "engagement": 0.0,
        }
        if trailing_prev_weeks:
            trailing_avg_metrics = {
                "posts": (
                    sum(int(item.get("post_volume") or 0) for item in trailing_prev_weeks) / len(trailing_prev_weeks)
                ),
                "comments": sum(int(item.get("comment_volume") or 0) for item in trailing_prev_weeks)
                / len(trailing_prev_weeks),
                "engagement": sum(int(item.get("engagement") or 0) for item in trailing_prev_weeks)
                / len(trailing_prev_weeks),
            }

        consistency_score: dict[str, float | None] = {}
        for platform in [item for item in SUPPORTED_PLATFORMS if item in available_platforms]:
            total_days = 0
            active_days = 0
            for week_row in weekly_daily_activity:
                for day_row in week_row.get("days") or []:
                    total_days += 1
                    posts_payload = day_row.get("posts") or {}
                    if int(posts_payload.get(platform, 0)) > 0:
                        active_days += 1
            consistency_score[platform] = _safe_percent(active_days, total_days)

        current_metrics = _metrics_payload(current_week_entry)
        previous_metrics = _metrics_payload(previous_week_entry)
        benchmark = {
            "week_index": int((current_week_entry or {}).get("week_index", -1)),
            "current": current_metrics,
            "previous_week": {
                "week_index": int((previous_week_entry or {}).get("week_index", -1)) if previous_week_entry else None,
                "metrics": previous_metrics,
                "delta_pct": {
                    "posts": _delta_pct(current_metrics["posts"], previous_metrics["posts"]),
                    "comments": _delta_pct(current_metrics["comments"], previous_metrics["comments"]),
                    "engagement": _delta_pct(current_metrics["engagement"], previous_metrics["engagement"]),
                },
            },
            "trailing_3_week_avg": {
                "window_size": len(trailing_prev_weeks),
                "metrics": {
                    "posts": round(trailing_avg_metrics["posts"], 1),
                    "comments": round(trailing_avg_metrics["comments"], 1),
                    "engagement": round(trailing_avg_metrics["engagement"], 1),
                },
                "delta_pct": {
                    "posts": _delta_pct(current_metrics["posts"], int(round(trailing_avg_metrics["posts"]))),
                    "comments": _delta_pct(current_metrics["comments"], int(round(trailing_avg_metrics["comments"]))),
                    "engagement": _delta_pct(
                        current_metrics["engagement"],
                        int(round(trailing_avg_metrics["engagement"])),
                    ),
                },
            },
            "consistency_score_pct": consistency_score,
        }

    platform_breakdown = []
    for platform in available_platforms:
        platform_rows = [row for row in rows if row["platform"] == platform]
        platform_comments = [row for row in platform_rows if row["kind"] == "comment"]
        platform_breakdown.append(
            {
                "platform": platform,
                "posts": sum(1 for row in platform_rows if row["kind"] == "post"),
                "comments": len(platform_comments),
                "engagement": sum(int(row["engagement"]) for row in platform_rows),
                "sentiment": {
                    "positive": sum(1 for row in platform_comments if row["sentiment"] == "positive"),
                    "neutral": sum(1 for row in platform_comments if row["sentiment"] == "neutral"),
                    "negative": sum(1 for row in platform_comments if row["sentiment"] == "negative"),
                },
            }
        )

    leaderboards = {
        "bravo_content": [
            {
                "platform": row["platform"],
                "source_id": row["source_id"],
                "text": row["text"][:240],
                "engagement": row["engagement"],
                "url": row["url"],
                "timestamp": _iso(row["ts"]),
                "thumbnail_url": row.get("thumbnail_url"),
            }
            for row in sorted(posts, key=lambda item: item["engagement"], reverse=True)[:15]
        ],
        "viewer_discussion": [
            {
                "platform": row["platform"],
                "source_id": row["source_id"],
                "text": row["text"][:240],
                "engagement": row["engagement"],
                "url": row["url"],
                "timestamp": _iso(row["ts"]),
                "sentiment": row["sentiment"],
                "thumbnail_url": row.get("thumbnail_url"),
            }
            for row in sorted(comments, key=lambda item: item["engagement"], reverse=True)[:20]
        ],
    }

    jobs = list_jobs(season_id, limit=25) if include_jobs else []

    response: dict[str, Any] = {
        "window": {
            "start": _iso(start_dt),
            "end": _iso(end_dt),
            "timezone": timezone,
            "week_anchor": str(context.anchor_date),
            "week_zero_start": _iso(week_zero_start_local.astimezone(UTC)),
            "week": week,
            "source_scope": source_scope,
        },
        "summary": {
            "show_id": context.show_id,
            "season_id": context.season_id,
            "season_number": context.season_number,
            "show_name": context.show_name,
            "total_posts": len(posts),
            "total_comments": len(comments),
            "total_engagement": sum(int(row["engagement"]) for row in rows),
            "sentiment_mix": sentiment_mix,
            "deltas": {
                "posts": None,
                "comments": None,
                "engagement": None,
            },
            "data_quality": {
                "comments_saved_pct_overall": comments_saved_pct_overall,
                "platform_comments_saved_pct": platform_comments_saved_pct,
                "last_post_at": _iso(last_post_dt),
                "last_comment_at": _iso(last_comment_dt),
                "data_freshness_minutes": data_freshness_minutes,
                "youtube_content_breakdown": youtube_content_breakdown,
                "post_metadata": post_metadata,
            },
        },
        "weekly": weekly,
        "weekly_platform_posts": weekly_platform_posts,
        "weekly_platform_engagement": weekly_platform_engagement,
        "weekly_daily_activity": weekly_daily_activity,
        "platform_breakdown": platform_breakdown,
        "themes": _build_drivers(comments, analyzer_context=sentiment_context),
        "leaderboards": leaderboards,
        "jobs": jobs,
    }
    if include_flags:
        response["weekly_flags"] = weekly_flags
    if include_schedule and schedule_profile is not None:
        response["schedule_profile"] = schedule_profile
    if include_benchmark and benchmark is not None:
        response["benchmark"] = benchmark

    if include_rows:
        lookup_windows = visible_windows
        rows_payload: list[dict[str, Any]] = []
        for row in rows:
            bucket = _week_for_timestamp(row["ts"], windows=lookup_windows, timezone=timezone)
            rows_payload.append(
                {
                    "week_index": bucket.week_index if bucket else None,
                    "platform": row["platform"],
                    "kind": row["kind"],
                    "source_id": row["source_id"],
                    "timestamp": _iso(row["ts"]),
                    "author": row["author"],
                    "url": row["url"],
                    "engagement": row["engagement"],
                    "sentiment": row["sentiment"],
                    "text": row["text"],
                    "thumbnail_url": row.get("thumbnail_url"),
                }
            )
        response["rows"] = rows_payload

    _set_cached_analytics(cache_key, response)
    return response


# ---------------------------------------------------------------------------
# Comment coverage (saved vs platform-reported)
# ---------------------------------------------------------------------------


def _comments_coverage_for_platform(
    season_id: str,
    *,
    platform: str,
    start_dt: datetime,
    end_dt: datetime,
    source_scope: str,
    target_accounts_by_platform: dict[str, set[str]],
) -> dict[str, int]:
    requires_target_accounts = source_scope in {"bravo", "creator"}
    platform_accounts = set(target_accounts_by_platform.get(platform, set()))
    if requires_target_accounts and not platform_accounts:
        return {
            "posts_scanned": 0,
            "stale_posts_count": 0,
            "saved_comments": 0,
            "reported_comments": 0,
        }

    apply_account_filter = source_scope != "community" and bool(platform_accounts)
    account_handles_list = sorted(platform_accounts)
    instagram_active_filter = "and c.is_missing = false" if _comment_lifecycle_supported("instagram_comments") else ""
    tiktok_active_filter = "and c.is_missing = false" if _comment_lifecycle_supported("tiktok_comments") else ""
    youtube_active_filter = "and c.is_missing = false" if _comment_lifecycle_supported("youtube_comments") else ""
    twitter_reply_active_filter_r = "and r.is_missing = false" if _comment_lifecycle_supported("twitter_tweets") else ""
    twitter_reply_active_filter_child = (
        "and child.is_missing = false" if _comment_lifecycle_supported("twitter_tweets") else ""
    )
    twitter_reply_active_filter_t = "and t.is_missing = false" if _comment_lifecycle_supported("twitter_tweets") else ""
    twitter_quote_active_filter_q = "and q.is_missing = false" if _comment_lifecycle_supported("twitter_tweets") else ""

    if platform == "instagram":
        account_filter = (
            "and ltrim(lower(coalesce(nullif(p.username, ''), nullif(p.source_account, ''), '')), '@') = any(%s)"
            if apply_account_filter
            else ""
        )
        params: list[Any] = [season_id, start_dt, end_dt]
        if apply_account_filter:
            params.append(account_handles_list)
        row = (
            pg.fetch_one(
                f"""
            with posts as (
              select
                p.id,
                greatest(0, coalesce(p.comments_count, 0))::bigint as reported_comments
              from social.instagram_posts p
              where p.season_id = %s
                and p.posted_at >= %s
                and p.posted_at <= %s
                {account_filter}
            ), comment_counts as (
              select c.post_id, count(*)::bigint as saved_comments
              from social.instagram_comments c
              join posts p on p.id = c.post_id
              where true
                {instagram_active_filter}
              group by c.post_id
            )
            select
              count(*)::bigint as posts_scanned,
              coalesce(
                sum(case when coalesce(cc.saved_comments, 0) < p.reported_comments then 1 else 0 end),
                0
              )::bigint as stale_posts_count,
              coalesce(sum(coalesce(cc.saved_comments, 0)), 0)::bigint as saved_comments,
              coalesce(sum(p.reported_comments), 0)::bigint as reported_comments
            from posts p
            left join comment_counts cc on cc.post_id = p.id
            """,
                params,
            )
            or {}
        )
    elif platform == "tiktok":
        account_filter = (
            "and ltrim(lower(coalesce(nullif(p.username, ''), nullif(p.source_account, ''), '')), '@') = any(%s)"
            if apply_account_filter
            else ""
        )
        params = [season_id, start_dt, end_dt]
        if apply_account_filter:
            params.append(account_handles_list)
        row = (
            pg.fetch_one(
                f"""
            with posts as (
              select
                p.id,
                greatest(0, coalesce(p.comments_count, 0))::bigint as reported_comments
              from social.tiktok_posts p
              where p.season_id = %s
                and p.posted_at >= %s
                and p.posted_at <= %s
                {account_filter}
            ), comment_counts as (
              select c.post_id, count(*)::bigint as saved_comments
              from social.tiktok_comments c
              join posts p on p.id = c.post_id
              where true
                {tiktok_active_filter}
              group by c.post_id
            )
            select
              count(*)::bigint as posts_scanned,
              coalesce(
                sum(case when coalesce(cc.saved_comments, 0) < p.reported_comments then 1 else 0 end),
                0
              )::bigint as stale_posts_count,
              coalesce(sum(coalesce(cc.saved_comments, 0)), 0)::bigint as saved_comments,
              coalesce(sum(p.reported_comments), 0)::bigint as reported_comments
            from posts p
            left join comment_counts cc on cc.post_id = p.id
            """,
                params,
            )
            or {}
        )
    elif platform == "youtube":
        account_filter = (
            "and ltrim(lower(coalesce(nullif(v.channel_title, ''), nullif(v.source_account, ''), '')), '@') = any(%s)"
            if apply_account_filter
            else ""
        )
        params = [season_id, start_dt, end_dt]
        if apply_account_filter:
            params.append(account_handles_list)
        row = (
            pg.fetch_one(
                f"""
            with posts as (
              select
                v.id,
                greatest(0, coalesce(v.comments_count, 0))::bigint as reported_comments
              from social.youtube_videos v
              where v.season_id = %s
                and v.published_at >= %s
                and v.published_at <= %s
                {account_filter}
            ), comment_counts as (
              select c.video_id as post_id, count(*)::bigint as saved_comments
              from social.youtube_comments c
              join posts p on p.id = c.video_id
              where true
                {youtube_active_filter}
              group by c.video_id
            )
            select
              count(*)::bigint as posts_scanned,
              coalesce(
                sum(case when coalesce(cc.saved_comments, 0) < p.reported_comments then 1 else 0 end),
                0
              )::bigint as stale_posts_count,
              coalesce(sum(coalesce(cc.saved_comments, 0)), 0)::bigint as saved_comments,
              coalesce(sum(p.reported_comments), 0)::bigint as reported_comments
            from posts p
            left join comment_counts cc on cc.post_id = p.id
            """,
                params,
            )
            or {}
        )
    elif platform == "twitter":
        if apply_account_filter:
            row = (
                pg.fetch_one(
                    f"""
                with recursive posts as (
                  select
                    t.tweet_id,
                    greatest(0, coalesce(t.replies_count, 0) + coalesce(t.quotes, 0))::bigint as reported_comments
                  from social.twitter_tweets t
                  where t.season_id = %s
                    and t.is_reply = false
                    and t.created_at >= %s
                    and t.created_at <= %s
                    and ltrim(lower(coalesce(nullif(t.username, ''), nullif(t.source_account, ''), '')), '@') = any(%s)
                ), thread_replies as (
                  select
                    r.tweet_id,
                    r.reply_to_tweet_id as root_tweet_id
                  from social.twitter_tweets r
                  where r.season_id = %s
                    and r.is_reply = true
                    {twitter_reply_active_filter_r}
                    and r.reply_to_tweet_id in (select tweet_id from posts)
                  union
                  select
                    child.tweet_id,
                    parent.root_tweet_id
                  from social.twitter_tweets child
                  join thread_replies parent on child.reply_to_tweet_id = parent.tweet_id
                  where child.season_id = %s
                    and child.is_reply = true
                    {twitter_reply_active_filter_child}
                ), reply_counts as (
                  select root_tweet_id, count(*)::bigint as saved_comments
                  from thread_replies
                  group by root_tweet_id
                ), quote_counts as (
                  select
                    q.quoted_tweet_id as root_tweet_id,
                    count(*)::bigint as saved_quotes
                  from social.twitter_tweets q
                  where q.season_id = %s
                    and q.is_quote = true
                    {twitter_quote_active_filter_q}
                    and q.quoted_tweet_id in (select tweet_id from posts)
                  group by q.quoted_tweet_id
                ), combined_counts as (
                  select
                    p.tweet_id,
                    coalesce(rc.saved_comments, 0)::bigint as saved_replies,
                    coalesce(qc.saved_quotes, 0)::bigint as saved_quotes,
                    (coalesce(rc.saved_comments, 0) + coalesce(qc.saved_quotes, 0))::bigint as saved_interactions
                  from posts p
                  left join reply_counts rc on rc.root_tweet_id = p.tweet_id
                  left join quote_counts qc on qc.root_tweet_id = p.tweet_id
                )
                select
                  count(*)::bigint as posts_scanned,
                  coalesce(
                    sum(case when coalesce(cc.saved_interactions, 0) < p.reported_comments then 1 else 0 end),
                    0
                  )::bigint as stale_posts_count,
                  coalesce(sum(coalesce(cc.saved_interactions, 0)), 0)::bigint as saved_comments,
                  coalesce(sum(p.reported_comments), 0)::bigint as reported_comments
                from posts p
                left join combined_counts cc on cc.tweet_id = p.tweet_id
                """,
                    [season_id, start_dt, end_dt, account_handles_list, season_id, season_id, season_id],
                )
                or {}
            )
        else:
            row = (
                pg.fetch_one(
                    f"""
                with posts as (
                  select
                    t.tweet_id,
                    greatest(0, coalesce(t.replies_count, 0) + coalesce(t.quotes, 0))::bigint as reported_comments
                  from social.twitter_tweets t
                  where t.season_id = %s
                    and t.is_reply = false
                    and t.created_at >= %s
                    and t.created_at <= %s
                ), reply_counts as (
                  select t.reply_to_tweet_id as root_tweet_id, count(*)::bigint as saved_comments
                  from social.twitter_tweets t
                  where t.season_id = %s
                    and t.is_reply = true
                    {twitter_reply_active_filter_t}
                    and t.reply_to_tweet_id in (select tweet_id from posts)
                  group by t.reply_to_tweet_id
                ), quote_counts as (
                  select q.quoted_tweet_id as root_tweet_id, count(*)::bigint as saved_quotes
                  from social.twitter_tweets q
                  where q.season_id = %s
                    and q.is_quote = true
                    {twitter_quote_active_filter_q}
                    and q.quoted_tweet_id in (select tweet_id from posts)
                  group by q.quoted_tweet_id
                ), combined_counts as (
                  select
                    p.tweet_id,
                    coalesce(rc.saved_comments, 0)::bigint as saved_replies,
                    coalesce(qc.saved_quotes, 0)::bigint as saved_quotes,
                    (coalesce(rc.saved_comments, 0) + coalesce(qc.saved_quotes, 0))::bigint as saved_interactions
                  from posts p
                  left join reply_counts rc on rc.root_tweet_id = p.tweet_id
                  left join quote_counts qc on qc.root_tweet_id = p.tweet_id
                )
                select
                  count(*)::bigint as posts_scanned,
                  coalesce(
                    sum(case when coalesce(cc.saved_interactions, 0) < p.reported_comments then 1 else 0 end),
                    0
                  )::bigint as stale_posts_count,
                  coalesce(sum(coalesce(cc.saved_interactions, 0)), 0)::bigint as saved_comments,
                  coalesce(sum(p.reported_comments), 0)::bigint as reported_comments
                from posts p
                left join combined_counts cc on cc.tweet_id = p.tweet_id
                """,
                    [season_id, start_dt, end_dt, season_id, season_id],
                )
                or {}
            )
    elif platform == "facebook":
        account_filter = (
            "and ltrim(lower(coalesce(nullif(p.username, ''), nullif(p.source_account, ''), '')), '@') = any(%s)"
            if apply_account_filter
            else ""
        )
        params = [season_id, start_dt, end_dt]
        if apply_account_filter:
            params.append(account_handles_list)
        row = (
            pg.fetch_one(
                f"""
            with posts as (
              select
                p.id,
                greatest(0, coalesce(p.comments_count, 0))::bigint as reported_comments
              from social.facebook_posts p
              where p.season_id = %s
                and p.posted_at >= %s
                and p.posted_at <= %s
                {account_filter}
            ), comment_counts as (
              select c.post_id, count(*)::bigint as saved_comments
              from social.facebook_comments c
              join posts p on p.id = c.post_id
              group by c.post_id
            )
            select
              count(*)::bigint as posts_scanned,
              coalesce(
                sum(case when coalesce(cc.saved_comments, 0) < p.reported_comments then 1 else 0 end),
                0
              )::bigint as stale_posts_count,
              coalesce(sum(coalesce(cc.saved_comments, 0)), 0)::bigint as saved_comments,
              coalesce(sum(p.reported_comments), 0)::bigint as reported_comments
            from posts p
            left join comment_counts cc on cc.post_id = p.id
            """,
                params,
            )
            or {}
        )
    elif platform == "threads":
        account_filter = (
            "and ltrim(lower(coalesce(nullif(p.username, ''), nullif(p.source_account, ''), '')), '@') = any(%s)"
            if apply_account_filter
            else ""
        )
        params = [season_id, start_dt, end_dt]
        if apply_account_filter:
            params.append(account_handles_list)
        row = (
            pg.fetch_one(
                f"""
            with posts as (
              select
                p.id,
                greatest(0, coalesce(p.replies_count, 0))::bigint as reported_comments
              from social.meta_threads_posts p
              where p.season_id = %s
                and p.posted_at >= %s
                and p.posted_at <= %s
                {account_filter}
            ), comment_counts as (
              select c.post_id, count(*)::bigint as saved_comments
              from social.meta_threads_comments c
              join posts p on p.id = c.post_id
              group by c.post_id
            )
            select
              count(*)::bigint as posts_scanned,
              coalesce(
                sum(case when coalesce(cc.saved_comments, 0) < p.reported_comments then 1 else 0 end),
                0
              )::bigint as stale_posts_count,
              coalesce(sum(coalesce(cc.saved_comments, 0)), 0)::bigint as saved_comments,
              coalesce(sum(p.reported_comments), 0)::bigint as reported_comments
            from posts p
            left join comment_counts cc on cc.post_id = p.id
            """,
                params,
            )
            or {}
        )
    else:
        row = {}

    return {
        "posts_scanned": int(row.get("posts_scanned") or 0),
        "stale_posts_count": int(row.get("stale_posts_count") or 0),
        "saved_comments": int(row.get("saved_comments") or 0),
        "reported_comments": int(row.get("reported_comments") or 0),
    }


def get_comments_coverage(
    season_id: str,
    *,
    platforms: list[str] | None,
    timezone: str,
    source_scope: str,
    date_start: datetime | None = None,
    date_end: datetime | None = None,
) -> dict[str, Any]:
    context = get_season_context(season_id)
    available_platforms = [
        platform for platform in (platforms or list(SUPPORTED_PLATFORMS)) if platform in SUPPORTED_PLATFORMS
    ]
    if not available_platforms:
        available_platforms = list(SUPPORTED_PLATFORMS)

    start_dt, end_dt, now_utc = _resolve_coverage_window(
        context=context,
        timezone=timezone,
        source_scope=source_scope,
        date_start=date_start,
        date_end=date_end,
    )

    target_accounts_by_platform = _target_accounts_by_platform(
        season_id,
        source_scope=source_scope,
        context=context,
    )

    by_platform: dict[str, dict[str, Any]] = {}
    total_saved = 0
    total_reported = 0
    total_stale_posts = 0
    total_posts = 0
    for platform in available_platforms:
        stats = _comments_coverage_for_platform(
            season_id,
            platform=platform,
            start_dt=start_dt,
            end_dt=end_dt,
            source_scope=source_scope,
            target_accounts_by_platform=target_accounts_by_platform,
        )
        saved = int(stats.get("saved_comments") or 0)
        reported = int(stats.get("reported_comments") or 0)
        stale_posts = int(stats.get("stale_posts_count") or 0)
        posts_scanned = int(stats.get("posts_scanned") or 0)
        total_saved += saved
        total_reported += reported
        total_stale_posts += stale_posts
        total_posts += posts_scanned
        by_platform[platform] = {
            "saved_comments": saved,
            "reported_comments": reported,
            "coverage_pct": _safe_percent(saved, reported),
            "up_to_date": saved >= reported,
            "stale_posts_count": stale_posts,
            "posts_scanned": posts_scanned,
        }

    return {
        "season_id": context.season_id,
        "show_id": context.show_id,
        "season_number": context.season_number,
        "source_scope": source_scope,
        "platforms": available_platforms,
        "window": {
            "start": _iso(start_dt),
            "end": _iso(end_dt),
            "timezone": timezone,
        },
        "total_saved_comments": total_saved,
        "total_reported_comments": total_reported,
        "coverage_pct": _safe_percent(total_saved, total_reported),
        "up_to_date": total_saved >= total_reported,
        "stale_posts_count": total_stale_posts,
        "posts_scanned": total_posts,
        "by_platform": by_platform,
        "evaluated_at": _iso(now_utc),
    }


def _resolve_coverage_window(
    *,
    context: SeasonContext,
    timezone: str,
    source_scope: str,
    date_start: datetime | None,
    date_end: datetime | None,
) -> tuple[datetime, datetime, datetime]:
    now_utc = _now_utc()
    if date_start is None and date_end is None:
        _, week_zero_start_local = _resolve_week_windows(
            context,
            timezone=timezone,
            source_scope=source_scope,
            now_utc=now_utc,
        )
        start_dt = week_zero_start_local.astimezone(UTC)
        end_dt = now_utc
    else:
        start_dt = _coerce_dt(date_start)
        if start_dt is None:
            start_dt = datetime.combine(context.anchor_date, time.min, tzinfo=UTC)
        end_dt = _coerce_dt(date_end) or now_utc
    if end_dt < start_dt:
        end_dt = start_dt
    return start_dt, end_dt, now_utc


def _mirror_coverage_for_platform(
    season_id: str,
    *,
    platform: str,
    start_dt: datetime,
    end_dt: datetime,
    source_scope: str,
    target_accounts_by_platform: dict[str, set[str]] | None = None,
) -> dict[str, int]:
    normalized_platform = (platform or "").strip().lower()
    table = PLATFORM_POST_TABLES.get(normalized_platform)
    source_id_column = PLATFORM_SOURCE_ID_COLUMN.get(normalized_platform)
    posted_at_column = PLATFORM_POSTED_AT_COLUMN.get(normalized_platform)
    if not table or not source_id_column or not posted_at_column:
        return {
            "posts_scanned": 0,
            "needs_mirror_count": 0,
            "mirrored_count": 0,
            "failed_count": 0,
            "partial_count": 0,
            "pending_count": 0,
        }

    requires_target_accounts = source_scope in {"bravo", "creator"}
    account_handles = set((target_accounts_by_platform or {}).get(normalized_platform, set()))
    if requires_target_accounts and not account_handles:
        return {
            "posts_scanned": 0,
            "needs_mirror_count": 0,
            "mirrored_count": 0,
            "failed_count": 0,
            "partial_count": 0,
            "pending_count": 0,
        }

    account_handles_list = sorted(account_handles)
    account_expr = (
        "coalesce(nullif(p.source_account, ''), nullif(p.channel_title, ''), '')"
        if normalized_platform == "youtube"
        else "coalesce(nullif(p.source_account, ''), nullif(p.username, ''), '')"
    )
    account_filter = f"and ltrim(lower({account_expr}), '@') = any(%s)" if account_handles_list else ""
    media_urls_expr = "p.media_urls" if _platform_posts_has_column(normalized_platform, "media_urls") else "'[]'::jsonb"
    thumbnail_expr = (
        "coalesce(nullif(p.media_urls ->> 0, ''), '')"
        if normalized_platform == "twitter"
        else "coalesce(nullif(p.thumbnail_url, ''), '')"
    )
    twitter_root_filter = "and p.is_reply = false" if normalized_platform == "twitter" else ""
    params: list[Any] = [season_id, start_dt, end_dt]
    if account_handles_list:
        params.append(account_handles_list)
    rows = pg.fetch_all(
        f"""
        select
          p.id::text as id,
          p.{source_id_column} as source_id,
          p.{posted_at_column} as posted_at,
          {thumbnail_expr} as thumbnail_url,
          {media_urls_expr} as media_urls,
          coalesce(to_jsonb(p) ->> 'hosted_thumbnail_url', '') as hosted_thumbnail_url,
          coalesce(to_jsonb(p) -> 'hosted_media_urls', '[]'::jsonb) as hosted_media_urls,
          coalesce(to_jsonb(p) ->> 'media_mirror_status', '') as media_mirror_status
        from social.{table} p
        where p.season_id = %s
          and p.{posted_at_column} >= %s
          and p.{posted_at_column} <= %s
          {twitter_root_filter}
          {account_filter}
        """,
        params,
    )

    posts_scanned = 0
    needs_mirror_count = 0
    mirrored_count = 0
    failed_count = 0
    partial_count = 0
    pending_count = 0
    for row in rows:
        posts_scanned += 1
        mirror_status = str(row.get("media_mirror_status") or "").strip().lower()
        if mirror_status == "failed":
            failed_count += 1
        elif mirror_status == "partial":
            partial_count += 1
        elif mirror_status == "pending":
            pending_count += 1

        if _platform_post_needs_media_mirror(normalized_platform, row):
            needs_mirror_count += 1
        else:
            mirrored_count += 1

    return {
        "posts_scanned": posts_scanned,
        "needs_mirror_count": needs_mirror_count,
        "mirrored_count": mirrored_count,
        "failed_count": failed_count,
        "partial_count": partial_count,
        "pending_count": pending_count,
    }


def _comment_media_coverage_for_platform(
    season_id: str,
    *,
    platform: str,
    start_dt: datetime,
    end_dt: datetime,
    source_scope: str,
    target_accounts_by_platform: dict[str, set[str]] | None = None,
) -> dict[str, int]:
    normalized_platform = (platform or "").strip().lower()
    if normalized_platform != "tiktok":
        return {
            "items_scanned": 0,
            "needs_mirror_count": 0,
            "mirrored_count": 0,
            "failed_count": 0,
            "pending_count": 0,
        }
    if not _tiktok_comments_has_column("media_urls"):
        return {
            "items_scanned": 0,
            "needs_mirror_count": 0,
            "mirrored_count": 0,
            "failed_count": 0,
            "pending_count": 0,
        }

    requires_target_accounts = source_scope in {"bravo", "creator"}
    account_handles = set((target_accounts_by_platform or {}).get("tiktok", set()))
    if requires_target_accounts and not account_handles:
        return {
            "items_scanned": 0,
            "needs_mirror_count": 0,
            "mirrored_count": 0,
            "failed_count": 0,
            "pending_count": 0,
        }

    account_filter = (
        "and ltrim(lower(coalesce(nullif(p.source_account, ''), nullif(p.username, ''), '')), '@') = any(%s)"
        if account_handles
        else ""
    )
    params: list[Any] = [season_id, start_dt, end_dt]
    if account_handles:
        params.append(sorted(account_handles))
    rows = pg.fetch_all(
        f"""
        select
          coalesce(to_jsonb(c) -> 'media_urls', '[]'::jsonb) as media_urls,
          coalesce(to_jsonb(c) -> 'hosted_media_urls', '[]'::jsonb) as hosted_media_urls,
          coalesce(to_jsonb(c) ->> 'media_mirror_status', '') as media_mirror_status
        from social.tiktok_comments c
        join social.tiktok_posts p on p.id = c.post_id
        where p.season_id = %s
          and p.posted_at >= %s
          and p.posted_at <= %s
          {account_filter}
        """,
        params,
    )

    items_scanned = 0
    needs_mirror_count = 0
    mirrored_count = 0
    failed_count = 0
    pending_count = 0
    for row in rows:
        source_urls = _as_text_list(row.get("media_urls"))
        if not source_urls:
            continue
        hosted_urls = _as_text_list(row.get("hosted_media_urls"))
        source_count = len(source_urls)
        hosted_count = min(len(hosted_urls), source_count)
        missing_count = max(source_count - hosted_count, 0)
        status = str(row.get("media_mirror_status") or "").strip().lower()

        items_scanned += source_count
        mirrored_count += hosted_count
        needs_mirror_count += missing_count
        if missing_count <= 0:
            continue
        if status == "failed":
            failed_count += missing_count
        else:
            pending_count += missing_count

    return {
        "items_scanned": items_scanned,
        "needs_mirror_count": needs_mirror_count,
        "mirrored_count": mirrored_count,
        "failed_count": failed_count,
        "pending_count": pending_count,
    }


def get_mirror_coverage(
    season_id: str,
    *,
    platforms: list[str] | None,
    timezone: str,
    source_scope: str,
    date_start: datetime | None = None,
    date_end: datetime | None = None,
) -> dict[str, Any]:
    context = get_season_context(season_id)
    available_platforms = [
        platform for platform in (platforms or list(SUPPORTED_PLATFORMS)) if platform in SUPPORTED_PLATFORMS
    ]
    if not available_platforms:
        available_platforms = list(SUPPORTED_PLATFORMS)

    start_dt, end_dt, now_utc = _resolve_coverage_window(
        context=context,
        timezone=timezone,
        source_scope=source_scope,
        date_start=date_start,
        date_end=date_end,
    )

    target_accounts_by_platform = _target_accounts_by_platform(
        season_id,
        source_scope=source_scope,
        context=context,
    )

    by_platform: dict[str, dict[str, Any]] = {}
    total_posts = 0
    total_needs_mirror = 0
    total_mirrored = 0
    total_failed = 0
    total_partial = 0
    total_pending = 0
    total_comment_media_items_scanned = 0
    total_comment_media_needs_mirror = 0
    total_comment_media_mirrored = 0
    total_comment_media_failed = 0
    total_comment_media_pending = 0
    for platform in available_platforms:
        stats = _mirror_coverage_for_platform(
            season_id,
            platform=platform,
            start_dt=start_dt,
            end_dt=end_dt,
            source_scope=source_scope,
            target_accounts_by_platform=target_accounts_by_platform,
        )
        posts_scanned = int(stats.get("posts_scanned") or 0)
        needs_mirror_count = int(stats.get("needs_mirror_count") or 0)
        mirrored_count = int(stats.get("mirrored_count") or 0)
        failed_count = int(stats.get("failed_count") or 0)
        partial_count = int(stats.get("partial_count") or 0)
        pending_count = int(stats.get("pending_count") or 0)
        total_posts += posts_scanned
        total_needs_mirror += needs_mirror_count
        total_mirrored += mirrored_count
        total_failed += failed_count
        total_partial += partial_count
        total_pending += pending_count
        comment_media_stats = _comment_media_coverage_for_platform(
            season_id,
            platform=platform,
            start_dt=start_dt,
            end_dt=end_dt,
            source_scope=source_scope,
            target_accounts_by_platform=target_accounts_by_platform,
        )
        comment_media_items_scanned = int(comment_media_stats.get("items_scanned") or 0)
        comment_media_needs_mirror_count = int(comment_media_stats.get("needs_mirror_count") or 0)
        comment_media_mirrored_count = int(comment_media_stats.get("mirrored_count") or 0)
        comment_media_failed_count = int(comment_media_stats.get("failed_count") or 0)
        comment_media_pending_count = int(comment_media_stats.get("pending_count") or 0)
        total_comment_media_items_scanned += comment_media_items_scanned
        total_comment_media_needs_mirror += comment_media_needs_mirror_count
        total_comment_media_mirrored += comment_media_mirrored_count
        total_comment_media_failed += comment_media_failed_count
        total_comment_media_pending += comment_media_pending_count
        by_platform[platform] = {
            "posts_scanned": posts_scanned,
            "needs_mirror_count": needs_mirror_count,
            "mirrored_count": mirrored_count,
            "failed_count": failed_count,
            "partial_count": partial_count,
            "pending_count": pending_count,
            "comment_media_items_scanned": comment_media_items_scanned,
            "comment_media_needs_mirror_count": comment_media_needs_mirror_count,
            "comment_media_mirrored_count": comment_media_mirrored_count,
            "comment_media_failed_count": comment_media_failed_count,
            "comment_media_pending_count": comment_media_pending_count,
            "up_to_date": needs_mirror_count == 0 and comment_media_needs_mirror_count == 0,
        }

    return {
        "season_id": context.season_id,
        "show_id": context.show_id,
        "season_number": context.season_number,
        "source_scope": source_scope,
        "platforms": available_platforms,
        "window": {
            "start": _iso(start_dt),
            "end": _iso(end_dt),
            "timezone": timezone,
        },
        "up_to_date": total_needs_mirror == 0 and total_comment_media_needs_mirror == 0,
        "needs_mirror_count": total_needs_mirror,
        "mirrored_count": total_mirrored,
        "failed_count": total_failed,
        "partial_count": total_partial,
        "pending_count": total_pending,
        "posts_scanned": total_posts,
        "comment_media_items_scanned": total_comment_media_items_scanned,
        "comment_media_needs_mirror_count": total_comment_media_needs_mirror,
        "comment_media_mirrored_count": total_comment_media_mirrored,
        "comment_media_failed_count": total_comment_media_failed,
        "comment_media_pending_count": total_comment_media_pending,
        "by_platform": by_platform,
        "evaluated_at": _iso(now_utc),
    }


# ---------------------------------------------------------------------------
# Week detail (drill-down into individual posts + comments for one week)
# ---------------------------------------------------------------------------

_MENTION_RE = re.compile(r"@([\w.]+)")
_HASHTAG_RE = re.compile(r"(?<![\w.])#([A-Za-z0-9_]+)")


def _json_text_list(value: Any, *, prefix: str = "", strip_prefix: str | None = None) -> list[str]:
    return _as_text_list(value, prefix=prefix, strip_prefix=strip_prefix)


def _parse_mentions(text: str | None) -> list[str]:
    """Extract @mentions from text."""
    if not text:
        return []
    return [f"@{m}" for m in _MENTION_RE.findall(text)]


def _parse_hashtags(text: str | None) -> list[str]:
    if not text:
        return []
    return _normalize_unique_terms([tag.lstrip("#") for tag in _HASHTAG_RE.findall(text)])


_INSTAGRAM_CUSTOM_COVER_HINT_KEYS = {
    "has_custom_thumbnail",
    "is_custom_thumbnail",
    "custom_thumbnail_url",
    "custom_thumbnail_src",
    "has_custom_cover",
    "is_custom_cover",
    "custom_cover_url",
    "custom_cover_photo_url",
    "custom_cover_media",
}


def _instagram_has_custom_cover_hint(node: Any) -> bool:
    if isinstance(node, dict):
        for key, value in node.items():
            normalized_key = str(key or "").strip().lower()
            if normalized_key in _INSTAGRAM_CUSTOM_COVER_HINT_KEYS:
                if isinstance(value, bool):
                    if value:
                        return True
                elif isinstance(value, (int, float)):
                    if value != 0:
                        return True
                elif isinstance(value, str):
                    candidate = value.strip().lower()
                    if candidate and candidate not in {"0", "false", "none", "null"}:
                        return True
                elif value:
                    return True
            if _instagram_has_custom_cover_hint(value):
                return True
        return False
    if isinstance(node, list):
        return any(_instagram_has_custom_cover_hint(item) for item in node)
    return False


def _normalize_media_url_for_compare(url: str | None) -> str:
    value = str(url or "").strip()
    if not value:
        return ""
    parsed = urlparse(value)
    host = parsed.netloc.lower().strip()
    path = str(parsed.path or "").strip().rstrip("/")
    return f"{host}{path}".lower()


def _instagram_cover_source_from_post_row(post_row: dict[str, Any]) -> tuple[str | None, str | None]:
    media_type = str(post_row.get("media_type") or "").strip().lower()
    post_format = str(post_row.get("post_format") or "").strip().lower()
    is_video_like = media_type in {"video", "reel"} or post_format in {"video", "reel"}
    if not is_video_like:
        return None, None

    raw_data = post_row.get("raw_data") if isinstance(post_row.get("raw_data"), dict) else {}
    if _instagram_has_custom_cover_hint(raw_data):
        return "custom_cover", "high"

    thumbnail_url = str(post_row.get("thumbnail_url") or "").strip()
    if not thumbnail_url:
        return "unknown", "none"

    source_media_urls = _as_text_list(post_row.get("source_media_urls"))
    if not source_media_urls:
        source_media_urls = _as_text_list(post_row.get("media_urls"))
    video_urls = [url for url in source_media_urls if _is_video_like_media_url(url)]
    if not video_urls:
        return "unknown", "none"

    normalized_thumb = _normalize_media_url_for_compare(thumbnail_url)
    normalized_video_urls = {
        normalized
        for normalized in (_normalize_media_url_for_compare(url) for url in video_urls)
        if normalized
    }
    if normalized_thumb and normalized_thumb in normalized_video_urls:
        return "still_frame_or_default", "low"

    return ("still_frame_or_default", "low") if len(video_urls) >= 1 else ("unknown", "none")


def _week_detail_instagram(
    season_id: str,
    *,
    start_dt: datetime,
    end_dt: datetime,
    account_handles: set[str],
    max_comments: int,
    post_limit: int = 20,
    post_offset: int = 0,
) -> dict[str, Any]:
    thumbnail_expr = _instagram_posts_thumbnail_expr("p")
    hosted_media_urls_expr = _instagram_posts_json_array_expr("p", "hosted_media_urls")
    post_format_expr = _instagram_posts_json_text_expr("p", "post_format")
    profile_tags_expr = _instagram_posts_json_array_expr("p", "profile_tags")
    collaborators_expr = _instagram_posts_json_array_expr("p", "collaborators")
    hashtags_expr = _instagram_posts_json_array_expr("p", "hashtags")
    mentions_expr = _instagram_posts_json_array_expr("p", "mentions")
    duration_expr = _instagram_posts_duration_expr("p")
    mirror_attempt_count_expr = _instagram_posts_json_int_expr("p", "media_mirror_attempt_count")
    mirror_last_attempt_at_expr = _instagram_posts_json_timestamptz_expr("p", "media_mirror_last_attempt_at")
    mirror_last_job_id_expr = _instagram_posts_json_text_expr("p", "media_mirror_last_job_id")

    account_handles_list = sorted(account_handles)
    account_filter = (
        "and ltrim(lower(coalesce(nullif(p.username, ''), nullif(p.source_account, ''), '')), '@') = any(%s)"
        if account_handles_list
        else ""
    )
    query_params = [season_id, start_dt, end_dt]
    if account_handles_list:
        query_params.append(account_handles_list)

    count_rows = pg.fetch_all(
        f"""
        select count(*)::int as post_count
        from social.instagram_posts p
        where p.season_id = %s
          and p.posted_at >= %s
          and p.posted_at <= %s
        {account_filter}
        """,
        query_params,
    )
    total_post_count = int((count_rows[0] if count_rows else {}).get("post_count") or 0)

    effective_post_limit = max(0, post_limit + post_offset)
    posts_params = query_params
    posts = pg.fetch_all(
        f"""
        select
          p.id,
          p.shortcode as source_id,
          p.username as author,
          p.caption as text,
          coalesce(p.likes, 0) as likes,
          coalesce(p.comments_count, 0) as comments_count,
          coalesce(p.views, 0) as views,
          p.media_type,
          coalesce(p.media_urls, '[]'::jsonb) as source_media_urls,
          {hosted_media_urls_expr} as hosted_media_urls,
          coalesce(nullif(p.thumbnail_url, ''), '') as source_thumbnail_url,
          coalesce(nullif(to_jsonb(p) ->> 'hosted_thumbnail_url', ''), '') as hosted_thumbnail_url,
          {thumbnail_expr} as thumbnail_url,
          {post_format_expr} as post_format,
          {profile_tags_expr} as profile_tags,
          {collaborators_expr} as collaborators,
          {hashtags_expr} as hashtags,
          {mentions_expr} as mentions,
          {duration_expr} as duration_seconds,
          coalesce(to_jsonb(p) -> 'raw_data', '{{}}'::jsonb) as raw_data,
          {mirror_attempt_count_expr} as media_mirror_attempt_count,
          {mirror_last_attempt_at_expr} as media_mirror_last_attempt_at,
          {mirror_last_job_id_expr} as media_mirror_last_job_id,
          p.posted_at as ts
        from social.instagram_posts p
        where p.season_id = %s
          and p.posted_at >= %s
          and p.posted_at <= %s
          {account_filter}
        order by p.posted_at desc
        {"" if effective_post_limit <= 0 else f"limit {effective_post_limit}"}
        """,
        posts_params,
    )

    post_ids = [p["id"] for p in posts]
    comments_by_post: dict[Any, list[dict]] = defaultdict(list)
    comment_counts_by_post: dict[Any, int] = defaultdict(int)

    if post_ids:
        # Get total comment counts per post
        count_rows = pg.fetch_all(
            """
            select c.post_id, count(*)::int as cnt
            from social.instagram_comments c
            where c.post_id = any(%s::uuid[])
            group by c.post_id
            """,
            [post_ids],
        )
        for row in count_rows:
            comment_counts_by_post[row["post_id"]] = row["cnt"]

        # Get top N comments per post using lateral join (0 = no cap)
        limit_clause = f"limit {max_comments}" if max_comments > 0 else "limit all"
        comment_rows = pg.fetch_all(
            f"""
            select sub.*
            from unnest(%s::uuid[]) as pid(id)
            cross join lateral (
              select
                c.comment_id,
                c.post_id,
                c.username as author,
                c.text,
                coalesce(c.likes, 0) as likes,
                coalesce(c.is_reply, false) as is_reply,
                coalesce(c.reply_count, 0) as reply_count,
                c.created_at
              from social.instagram_comments c
              where c.post_id = pid.id
              order by c.likes desc nulls last, c.created_at asc
              {limit_clause}
            ) sub
            """,
            [post_ids],
        )
        for row in comment_rows:
            comments_by_post[row["post_id"]].append(row)

    result_posts = []
    total_engagement = 0
    total_comments_count = 0
    saved_comments_count = 0
    for p in posts:
        engagement = p["likes"] + p["comments_count"] + p["views"]
        total_engagement += engagement
        db_comment_count = comment_counts_by_post.get(p["id"], 0)
        saved_comments_count += db_comment_count
        total_comments_count += p["comments_count"]
        post_comments = comments_by_post.get(p["id"], [])

        source_media_urls = _json_text_list(p.get("source_media_urls"))
        hosted_media_urls = _json_text_list(p.get("hosted_media_urls"))
        media_urls = hosted_media_urls or source_media_urls
        source_thumbnail_url = str(p.get("source_thumbnail_url") or "").strip() or None
        hosted_thumbnail_url = str(p.get("hosted_thumbnail_url") or "").strip() or None
        thumbnail_url = str(p.get("thumbnail_url") or "").strip() or source_thumbnail_url
        cover_source, cover_source_confidence = _instagram_cover_source_from_post_row(
            {
                "media_type": p.get("media_type"),
                "post_format": p.get("post_format"),
                "raw_data": p.get("raw_data"),
                "thumbnail_url": thumbnail_url,
                "source_media_urls": source_media_urls,
                "media_urls": source_media_urls,
            }
        )
        hashtags = _json_text_list(p.get("hashtags"), strip_prefix="#")
        mentions = _json_text_list(p.get("mentions"), prefix="@", strip_prefix="@")
        if not hashtags:
            hashtags = _parse_hashtags(p.get("text"))
        if not mentions:
            mentions = _parse_mentions(p.get("text"))
        profile_tags = _json_text_list(p.get("profile_tags"), prefix="@", strip_prefix="@")
        collaborators = _json_text_list(p.get("collaborators"), prefix="@", strip_prefix="@")

        result_posts.append(
            {
                "source_id": p["source_id"],
                "author": p["author"] or "",
                "text": p.get("text") or "",
                "url": f"https://www.instagram.com/p/{p['source_id']}/" if p["source_id"] else "",
                "posted_at": _iso(p["ts"]),
                "likes": p["likes"],
                "comments_count": p["comments_count"],
                "views": p["views"],
                "media_type": p.get("media_type"),
                "media_urls": media_urls,
                "source_media_urls": source_media_urls,
                "hosted_media_urls": hosted_media_urls,
                "thumbnail_url": thumbnail_url,
                "source_thumbnail_url": source_thumbnail_url,
                "hosted_thumbnail_url": hosted_thumbnail_url,
                "cover_source": cover_source,
                "cover_source_confidence": cover_source_confidence,
                "post_format": p.get("post_format"),
                "profile_tags": profile_tags,
                "collaborators": collaborators,
                "hashtags": hashtags,
                "mentions": mentions,
                "duration_seconds": p.get("duration_seconds"),
                "media_mirror_attempt_count": p.get("media_mirror_attempt_count"),
                "media_mirror_last_attempt_at": _iso(_coerce_dt(p.get("media_mirror_last_attempt_at"))),
                "media_mirror_last_job_id": str(p.get("media_mirror_last_job_id") or "") or None,
                "engagement": engagement,
                "total_comments_available": db_comment_count,
                "comments": [
                    {
                        "comment_id": c["comment_id"],
                        "author": c["author"] or "",
                        "text": c["text"] or "",
                        "likes": c["likes"],
                        "is_reply": c["is_reply"],
                        "reply_count": c["reply_count"],
                        "created_at": _iso(c["created_at"]),
                    }
                    for c in post_comments
                ],
            }
        )

    return {
        "posts": result_posts,
        "total_posts": total_post_count,
        "totals": {
            "posts": len(result_posts),
            "total_comments": total_comments_count,
            "total_engagement": total_engagement,
            "expected_comments_total": total_comments_count,
            "saved_comments_total": saved_comments_count,
            "comments_saved_pct": _safe_percent(saved_comments_count, total_comments_count),
        },
    }


def _week_detail_tiktok(
    season_id: str,
    *,
    start_dt: datetime,
    end_dt: datetime,
    account_handles: set[str],
    max_comments: int,
    post_limit: int = 20,
    post_offset: int = 0,
) -> dict[str, Any]:
    thumbnail_expr = _platform_thumbnail_expr("p", "tiktok")
    mentions_expr = (
        "coalesce(p.mentions, '[]'::jsonb)"
        if _platform_posts_has_column("tiktok", "mentions")
        else "'[]'::jsonb"
    )
    account_handles_list = sorted(account_handles)
    account_filter = (
        "and ltrim(lower(coalesce(nullif(p.username, ''), nullif(p.source_account, ''), '')), '@') = any(%s)"
        if account_handles_list
        else ""
    )
    params = [season_id, start_dt, end_dt]
    if account_handles_list:
        params.append(account_handles_list)
    count_rows = pg.fetch_all(
        f"""
        select count(*)::int as post_count
        from social.tiktok_posts p
        where p.season_id = %s
          and p.posted_at >= %s
          and p.posted_at <= %s
        {account_filter}
        """,
        params,
    )
    total_post_count = int((count_rows[0] if count_rows else {}).get("post_count") or 0)
    effective_post_limit = max(0, post_limit + post_offset)
    posts = pg.fetch_all(
        f"""
        select
          p.id,
          p.video_id as source_id,
          p.username as author,
          p.nickname,
          p.description as text,
          coalesce(p.likes, 0) as likes,
          coalesce(p.comments_count, 0) as comments_count,
          coalesce(p.shares, 0) as shares,
          coalesce(p.views, 0) as views,
          coalesce(p.hashtags, '[]'::jsonb) as hashtags,
          {mentions_expr} as mentions,
          {thumbnail_expr} as thumbnail_url,
          p.duration_seconds,
          p.posted_at as ts
        from social.tiktok_posts p
        where p.season_id = %s
          and p.posted_at >= %s
          and p.posted_at <= %s
          {account_filter}
        order by p.posted_at desc
        {"" if effective_post_limit <= 0 else f"limit {effective_post_limit}"}
        """,
        params,
    )

    post_ids = [p["id"] for p in posts]
    comments_by_post: dict[Any, list[dict]] = defaultdict(list)
    comment_counts_by_post: dict[Any, int] = defaultdict(int)

    if post_ids:
        count_rows = pg.fetch_all(
            """
            select c.post_id, count(*)::int as cnt
            from social.tiktok_comments c
            where c.post_id = any(%s::uuid[])
            group by c.post_id
            """,
            [post_ids],
        )
        for row in count_rows:
            comment_counts_by_post[row["post_id"]] = row["cnt"]

        limit_clause = f"limit {max_comments}" if max_comments > 0 else "limit all"
        comment_rows = pg.fetch_all(
            f"""
            select sub.*
            from unnest(%s::uuid[]) as pid(id)
            cross join lateral (
              select
                c.comment_id,
                c.post_id,
                c.username as author,
                c.text,
                coalesce(c.likes, 0) as likes,
                coalesce(c.is_reply, false) as is_reply,
                coalesce(c.reply_count, 0) as reply_count,
                c.created_at,
                coalesce(to_jsonb(c) -> 'media_urls', '[]'::jsonb) as media_urls,
                coalesce(to_jsonb(c) -> 'hosted_media_urls', '[]'::jsonb) as hosted_media_urls,
                nullif(coalesce(to_jsonb(c) ->> 'media_mirror_status', ''), '') as media_mirror_status
              from social.tiktok_comments c
              where c.post_id = pid.id
              order by c.likes desc nulls last, c.created_at asc
              {limit_clause}
            ) sub
            """,
            [post_ids],
        )
        for row in comment_rows:
            comments_by_post[row["post_id"]].append(row)

    result_posts = []
    total_engagement = 0
    total_comments_count = 0
    saved_comments_count = 0
    for p in posts:
        engagement = p["likes"] + p["comments_count"] + p["shares"] + p["views"]
        total_engagement += engagement
        total_comments_count += p["comments_count"]
        db_comment_count = comment_counts_by_post.get(p["id"], 0)
        saved_comments_count += db_comment_count
        post_comments = comments_by_post.get(p["id"], [])

        hashtags = _json_text_list(p.get("hashtags"), strip_prefix="#")
        if not hashtags:
            hashtags = _parse_hashtags(p.get("text"))
        mentions = _json_text_list(p.get("mentions"), prefix="@", strip_prefix="@")
        if not mentions:
            mentions = _parse_mentions(p.get("text"))

        author = p["author"] or ""
        result_posts.append(
            {
                "source_id": p["source_id"],
                "author": author,
                "nickname": p.get("nickname") or "",
                "text": p.get("text") or "",
                "url": f"https://www.tiktok.com/@{author}/video/{p['source_id']}" if p["source_id"] and author else "",
                "posted_at": _iso(p["ts"]),
                "likes": p["likes"],
                "comments_count": p["comments_count"],
                "shares": p["shares"],
                "views": p["views"],
                "hashtags": hashtags,
                "thumbnail_url": p.get("thumbnail_url"),
                "duration_seconds": p.get("duration_seconds"),
                "mentions": mentions,
                "engagement": engagement,
                "total_comments_available": db_comment_count,
                "comments": [
                    {
                        "comment_id": c["comment_id"],
                        "author": c["author"] or "",
                        "text": c["text"] or "",
                        "likes": c["likes"],
                        "is_reply": c["is_reply"],
                        "reply_count": c["reply_count"],
                        "created_at": _iso(c["created_at"]),
                        "media_urls": _as_text_list(c.get("media_urls")),
                        "hosted_media_urls": _as_text_list(c.get("hosted_media_urls")),
                        "media_mirror_status": c.get("media_mirror_status"),
                    }
                    for c in post_comments
                ],
            }
        )

    return {
        "posts": result_posts,
        "total_posts": total_post_count,
        "totals": {
            "posts": len(result_posts),
            "total_comments": total_comments_count,
            "total_engagement": total_engagement,
            "expected_comments_total": total_comments_count,
            "saved_comments_total": saved_comments_count,
            "comments_saved_pct": _safe_percent(saved_comments_count, total_comments_count),
        },
    }


def _week_detail_youtube(
    season_id: str,
    *,
    start_dt: datetime,
    end_dt: datetime,
    account_handles: set[str],
    max_comments: int,
    post_limit: int = 20,
    post_offset: int = 0,
) -> dict[str, Any]:
    thumbnail_expr = _platform_thumbnail_expr("v", "youtube")
    youtube_is_short_expr = _youtube_is_short_expr("v")
    hashtags_expr = (
        "coalesce(v.hashtags, '[]'::jsonb)"
        if _platform_posts_has_column("youtube", "hashtags")
        else "'[]'::jsonb"
    )
    mentions_expr = (
        "coalesce(v.mentions, '[]'::jsonb)"
        if _platform_posts_has_column("youtube", "mentions")
        else "'[]'::jsonb"
    )
    account_handles_list = sorted(account_handles)
    account_filter = (
        "and ltrim(lower(coalesce(nullif(v.channel_title, ''), nullif(v.source_account, ''), '')), '@') = any(%s)"
        if account_handles_list
        else ""
    )
    params = [season_id, start_dt, end_dt]
    if account_handles_list:
        params.append(account_handles_list)
    count_rows = pg.fetch_all(
        f"""
        select count(*)::int as post_count
        from social.youtube_videos v
        where v.season_id = %s
          and v.published_at >= %s
          and v.published_at <= %s
        {account_filter}
        """,
        params,
    )
    total_post_count = int((count_rows[0] if count_rows else {}).get("post_count") or 0)
    effective_post_limit = max(0, post_limit + post_offset)
    posts = pg.fetch_all(
        f"""
        select
          v.id,
          v.video_id as source_id,
          v.channel_title as author,
          v.title,
          v.description as text,
          coalesce(v.views, 0) as views,
          coalesce(v.likes, 0) as likes,
          coalesce(v.comments_count, 0) as comments_count,
          {youtube_is_short_expr} as is_short,
          {hashtags_expr} as hashtags,
          {mentions_expr} as mentions,
          {thumbnail_expr} as thumbnail_url,
          v.duration_seconds,
          v.published_at as ts
        from social.youtube_videos v
        where v.season_id = %s
          and v.published_at >= %s
          and v.published_at <= %s
          {account_filter}
        order by v.published_at desc
        {"" if effective_post_limit <= 0 else f"limit {effective_post_limit}"}
        """,
        params,
    )

    post_ids = [p["id"] for p in posts]
    comments_by_post: dict[Any, list[dict]] = defaultdict(list)
    comment_counts_by_post: dict[Any, int] = defaultdict(int)

    if post_ids:
        count_rows = pg.fetch_all(
            """
            select c.video_id, count(*)::int as cnt
            from social.youtube_comments c
            where c.video_id = any(%s::uuid[])
            group by c.video_id
            """,
            [post_ids],
        )
        for row in count_rows:
            comment_counts_by_post[row["video_id"]] = row["cnt"]

        limit_clause = f"limit {max_comments}" if max_comments > 0 else "limit all"
        comment_rows = pg.fetch_all(
            f"""
            select sub.*
            from unnest(%s::uuid[]) as pid(id)
            cross join lateral (
              select
                c.comment_id,
                c.video_id,
                c.author,
                c.text,
                coalesce(c.likes, 0) as likes,
                coalesce(c.is_reply, false) as is_reply,
                coalesce(c.reply_count, 0) as reply_count,
                c.created_at
              from social.youtube_comments c
              where c.video_id = pid.id
              order by c.likes desc nulls last, c.created_at asc
              {limit_clause}
            ) sub
            """,
            [post_ids],
        )
        for row in comment_rows:
            comments_by_post[row["video_id"]].append(row)

    result_posts = []
    total_engagement = 0
    total_comments_count = 0
    expected_comments_count = 0
    saved_comments_count = 0
    for p in posts:
        db_comment_count = comment_counts_by_post.get(p["id"], 0)
        expected_comments_count += int(p["comments_count"] or 0)
        saved_comments_count += db_comment_count
        effective_comments_count = _youtube_effective_comment_count(
            reported_count=p["comments_count"],
            saved_count=db_comment_count,
        )
        engagement = p["views"] + p["likes"] + effective_comments_count
        total_engagement += engagement
        total_comments_count += effective_comments_count
        post_comments = comments_by_post.get(p["id"], [])
        token_text = f"{p.get('title') or ''}\n{p.get('text') or ''}"
        hashtags = _json_text_list(p.get("hashtags"), strip_prefix="#")
        if not hashtags:
            hashtags = _parse_hashtags(token_text)
        mentions = _json_text_list(p.get("mentions"), prefix="@", strip_prefix="@")
        if not mentions:
            mentions = _parse_mentions(token_text)

        result_posts.append(
            {
                "source_id": p["source_id"],
                "author": p["author"] or "",
                "title": p.get("title") or "",
                "text": p.get("text") or "",
                "url": (
                    f"https://www.youtube.com/shorts/{p['source_id']}"
                    if p.get("is_short")
                    else f"https://www.youtube.com/watch?v={p['source_id']}"
                )
                if p["source_id"]
                else "",
                "posted_at": _iso(p["ts"]),
                "views": p["views"],
                "likes": p["likes"],
                "comments_count": effective_comments_count,
                "hashtags": hashtags,
                "mentions": mentions,
                "thumbnail_url": p.get("thumbnail_url"),
                "duration_seconds": p.get("duration_seconds"),
                "engagement": engagement,
                "total_comments_available": db_comment_count,
                "comments": [
                    {
                        "comment_id": c["comment_id"],
                        "author": c["author"] or "",
                        "text": c["text"] or "",
                        "likes": c["likes"],
                        "is_reply": c["is_reply"],
                        "reply_count": c["reply_count"],
                        "created_at": _iso(c["created_at"]),
                    }
                    for c in post_comments
                ],
            }
        )

    return {
        "posts": result_posts,
        "total_posts": total_post_count,
        "totals": {
            "posts": len(result_posts),
            "total_comments": total_comments_count,
            "total_engagement": total_engagement,
            "expected_comments_total": expected_comments_count,
            "saved_comments_total": saved_comments_count,
            "comments_saved_pct": _safe_percent(saved_comments_count, expected_comments_count),
        },
    }


def _week_detail_twitter(
    season_id: str,
    *,
    start_dt: datetime,
    end_dt: datetime,
    account_handles: set[str],
    max_comments: int,
    post_limit: int = 20,
    post_offset: int = 0,
) -> dict[str, Any]:
    """Twitter week detail with optional recursive reply chains for scoped accounts."""
    thumbnail_expr = _platform_thumbnail_expr("t", "twitter")

    account_handles_list = sorted(account_handles)
    account_filter = (
        "and ltrim(lower(coalesce(nullif(t.username, ''), nullif(t.source_account, ''), '')), '@') = any(%s)"
        if account_handles_list
        else ""
    )
    posts_params = [season_id, start_dt, end_dt]
    if account_handles_list:
        posts_params.append(account_handles_list)
    count_rows = pg.fetch_all(
        f"""
        select count(*)::int as post_count
        from social.twitter_tweets t
        where t.season_id = %s
          and t.is_reply = false
          and t.created_at >= %s
          and t.created_at <= %s
        {account_filter}
        """,
        posts_params,
    )
    total_post_count = int((count_rows[0] if count_rows else {}).get("post_count") or 0)
    effective_post_limit = max(0, post_limit + post_offset)

    posts = pg.fetch_all(
        f"""
        select
          t.tweet_id as source_id,
          coalesce(nullif(t.username, ''), nullif(t.source_account, ''), '') as author,
          t.display_name,
          t.text,
          coalesce(t.likes, 0) as likes,
          coalesce(t.retweets, 0) as retweets,
          coalesce(t.replies_count, 0) as replies_count,
          coalesce(t.quotes, 0) as quotes,
          coalesce(t.views, 0) as views,
          nullif(coalesce(to_jsonb(t) ->> 'user_id', ''), '') as user_id,
          nullif(coalesce(to_jsonb(t) ->> 'user_profile_url', ''), '') as user_profile_url,
          nullif(coalesce(to_jsonb(t) ->> 'user_avatar_url', ''), '') as user_avatar_url,
          t.hashtags,
          t.mentions,
          t.media_urls,
          {thumbnail_expr} as thumbnail_url,
          t.created_at as ts
        from social.twitter_tweets t
        where t.season_id = %s
          and t.is_reply = false
          and t.created_at >= %s
          and t.created_at <= %s
          {account_filter}
        order by t.created_at desc
        {"" if effective_post_limit <= 0 else f"limit {effective_post_limit}"}
        """,
        posts_params,
    )

    # For account-scoped runs: get full recursive reply chains for each post.
    # For unscoped runs: get direct replies only.
    post_tweet_ids = [p["source_id"] for p in posts if p["source_id"]]
    comments_by_root: dict[str, list[dict]] = defaultdict(list)
    comment_counts_by_root: dict[str, int] = defaultdict(int)

    if post_tweet_ids:
        if account_handles_list:
            # Full recursive reply chain (matches existing analytics CTE pattern)
            reply_rows = pg.fetch_all(
                """
                with recursive root_posts as (
                  select tweet_id from unnest(%s::text[]) as t(tweet_id)
                ), thread_replies as (
                  select r.tweet_id, r.reply_to_tweet_id,
                         r.reply_to_tweet_id as root_tweet_id
                  from social.twitter_tweets r
                  where r.season_id = %s
                    and r.is_reply = true
                    and r.reply_to_tweet_id in (select tweet_id from root_posts)
                  union
                  select child.tweet_id, child.reply_to_tweet_id,
                         parent.root_tweet_id
                  from social.twitter_tweets child
                  join thread_replies parent on child.reply_to_tweet_id = parent.tweet_id
                  where child.season_id = %s
                    and child.is_reply = true
                )
                select
                  tr.root_tweet_id,
                  t.tweet_id as comment_id,
                  coalesce(nullif(t.username, ''), nullif(t.source_account, ''), '') as author,
                  coalesce(t.display_name, '') as display_name,
                  nullif(coalesce(to_jsonb(t) ->> 'user_id', ''), '') as user_id,
                  nullif(coalesce(to_jsonb(t) ->> 'user_profile_url', ''), '') as user_url,
                  nullif(coalesce(to_jsonb(t) ->> 'user_avatar_url', ''), '') as user_avatar_url,
                  t.text,
                  coalesce(t.likes, 0) as likes,
                  true as is_reply,
                  coalesce(t.replies_count, 0) as reply_count,
                  t.created_at
                from thread_replies tr
                join social.twitter_tweets t on t.tweet_id = tr.tweet_id
                where t.season_id = %s
                order by tr.root_tweet_id, t.likes desc nulls last, t.created_at asc
                """,
                [post_tweet_ids, season_id, season_id, season_id],
            )
        else:
            # Non-bravo: direct replies only
            reply_rows = pg.fetch_all(
                """
                select
                  t.reply_to_tweet_id as root_tweet_id,
                  t.tweet_id as comment_id,
                  coalesce(nullif(t.username, ''), nullif(t.source_account, ''), '') as author,
                  coalesce(t.display_name, '') as display_name,
                  nullif(coalesce(to_jsonb(t) ->> 'user_id', ''), '') as user_id,
                  nullif(coalesce(to_jsonb(t) ->> 'user_profile_url', ''), '') as user_url,
                  nullif(coalesce(to_jsonb(t) ->> 'user_avatar_url', ''), '') as user_avatar_url,
                  t.text,
                  coalesce(t.likes, 0) as likes,
                  true as is_reply,
                  coalesce(t.replies_count, 0) as reply_count,
                  t.created_at
                from social.twitter_tweets t
                where t.season_id = %s
                  and t.is_reply = true
                  and t.reply_to_tweet_id = any(%s)
                order by t.reply_to_tweet_id, t.likes desc nulls last, t.created_at asc
                """,
                [season_id, post_tweet_ids],
            )

        # Group and count (max_comments=0 means no cap)
        for row in reply_rows:
            root_id = row["root_tweet_id"]
            comment_counts_by_root[root_id] = comment_counts_by_root.get(root_id, 0) + 1
            if max_comments == 0 or len(comments_by_root[root_id]) < max_comments:
                comments_by_root[root_id].append(row)

    quote_counts_by_root = _count_stored_quotes(post_tweet_ids) if post_tweet_ids else {}

    result_posts = []
    total_engagement = 0
    total_comments_count = 0
    expected_comments_count = 0
    total_quotes_saved = 0
    for p in posts:
        engagement = p["likes"] + p["retweets"] + p["replies_count"] + p["quotes"] + p["views"]
        total_engagement += engagement
        expected_comments_count += int(p["replies_count"] or 0) + int(p["quotes"] or 0)
        total_comments_count += comment_counts_by_root.get(p["source_id"], 0)
        total_quotes_saved += int(quote_counts_by_root.get(p["source_id"], 0))

        hashtags = p.get("hashtags")
        if isinstance(hashtags, str):
            try:
                hashtags = json.loads(hashtags)
            except (json.JSONDecodeError, TypeError):
                hashtags = None

        mentions = p.get("mentions")
        if isinstance(mentions, str):
            try:
                mentions = json.loads(mentions)
            except (json.JSONDecodeError, TypeError):
                mentions = None
        if isinstance(mentions, list):
            mentions = [f"@{m}" if not str(m).startswith("@") else str(m) for m in mentions]

        media_urls = p.get("media_urls")
        if isinstance(media_urls, str):
            try:
                media_urls = json.loads(media_urls)
            except (json.JSONDecodeError, TypeError):
                media_urls = None

        author = p["author"] or "i"
        post_comments = comments_by_root.get(p["source_id"], [])

        result_posts.append(
            {
                "source_id": p["source_id"],
                "author": p["author"] or "",
                "display_name": p.get("display_name") or "",
                "text": p.get("text") or "",
                "url": f"https://x.com/{author}/status/{p['source_id']}" if p["source_id"] else "",
                "posted_at": _iso(p["ts"]),
                "likes": p["likes"],
                "retweets": p["retweets"],
                "replies_count": p["replies_count"],
                "quotes": p["quotes"],
                "views": p["views"],
                "hashtags": hashtags or [],
                "mentions": mentions or [],
                "media_urls": media_urls,
                "thumbnail_url": p.get("thumbnail_url"),
                "user": {
                    "id": p.get("user_id"),
                    "username": p["author"] or "",
                    "display_name": p.get("display_name") or "",
                    "url": p.get("user_profile_url"),
                    "avatar_url": p.get("user_avatar_url"),
                },
                "engagement": engagement,
                "total_comments_available": comment_counts_by_root.get(p["source_id"], 0),
                "total_quotes_in_db": int(quote_counts_by_root.get(p["source_id"], 0)),
                "comments": [
                    {
                        "comment_id": c["comment_id"],
                        "author": c["author"] or "",
                        "text": c["text"] or "",
                        "likes": c["likes"],
                        "is_reply": c["is_reply"],
                        "reply_count": c["reply_count"],
                        "created_at": _iso(c["created_at"]),
                        "user": {
                            "id": c.get("user_id"),
                            "username": c["author"] or "",
                            "display_name": c.get("display_name") or "",
                            "url": c.get("user_url"),
                            "avatar_url": c.get("user_avatar_url"),
                        },
                    }
                    for c in post_comments
                ],
            }
        )

    return {
        "posts": result_posts,
        "total_posts": total_post_count,
        "totals": {
            "posts": len(result_posts),
            "total_comments": total_comments_count,
            "total_engagement": total_engagement,
            "expected_comments_total": expected_comments_count,
            "saved_comments_total": total_comments_count + total_quotes_saved,
            "comments_saved_pct": _safe_percent(total_comments_count + total_quotes_saved, expected_comments_count),
        },
    }


def _week_detail_facebook(
    season_id: str,
    *,
    start_dt: datetime,
    end_dt: datetime,
    account_handles: set[str],
    max_comments: int,
    post_limit: int = 20,
    post_offset: int = 0,
) -> dict[str, Any]:
    thumbnail_expr = _platform_thumbnail_expr("p", "facebook")
    hashtags_expr = (
        "coalesce(p.hashtags, '[]'::jsonb)"
        if _platform_posts_has_column("facebook", "hashtags")
        else "'[]'::jsonb"
    )
    mentions_expr = (
        "coalesce(p.mentions, '[]'::jsonb)"
        if _platform_posts_has_column("facebook", "mentions")
        else "'[]'::jsonb"
    )
    account_handles_list = sorted(account_handles)
    account_filter = (
        "and ltrim(lower(coalesce(nullif(p.username, ''), nullif(p.source_account, ''), '')), '@') = any(%s)"
        if account_handles_list
        else ""
    )
    params = [season_id, start_dt, end_dt]
    if account_handles_list:
        params.append(account_handles_list)
    count_rows = pg.fetch_all(
        f"""
        select count(*)::int as post_count
        from social.facebook_posts p
        where p.season_id = %s
          and p.posted_at >= %s
          and p.posted_at <= %s
        {account_filter}
        """,
        params,
    )
    total_post_count = int((count_rows[0] if count_rows else {}).get("post_count") or 0)
    effective_post_limit = max(0, post_limit + post_offset)
    posts = pg.fetch_all(
        f"""
        select
          p.id,
          p.post_id as source_id,
          coalesce(nullif(p.username, ''), nullif(p.source_account, ''), '') as author,
          p.caption as text,
          coalesce(p.post_type, 'feed') as post_type,
          coalesce(p.likes, 0) as likes,
          coalesce(p.comments_count, 0) as comments_count,
          coalesce(p.shares, 0) as shares,
          coalesce(p.views, 0) as views,
          {hashtags_expr} as hashtags,
          {mentions_expr} as mentions,
          {thumbnail_expr} as thumbnail_url,
          p.posted_at as ts
        from social.facebook_posts p
        where p.season_id = %s
          and p.posted_at >= %s
          and p.posted_at <= %s
          {account_filter}
        order by p.posted_at desc
        {"" if effective_post_limit <= 0 else f"limit {effective_post_limit}"}
        """,
        params,
    )
    post_ids = [p["id"] for p in posts]
    comments_by_post: dict[Any, list[dict[str, Any]]] = defaultdict(list)
    comment_counts_by_post: dict[Any, int] = defaultdict(int)
    if post_ids:
        count_rows = pg.fetch_all(
            """
            select c.post_id, count(*)::int as cnt
            from social.facebook_comments c
            where c.post_id = any(%s::uuid[])
            group by c.post_id
            """,
            [post_ids],
        )
        for row in count_rows:
            comment_counts_by_post[row["post_id"]] = row["cnt"]
        limit_clause = f"limit {max_comments}" if max_comments > 0 else "limit all"
        comment_rows = pg.fetch_all(
            f"""
            select sub.*
            from unnest(%s::uuid[]) as pid(id)
            cross join lateral (
              select
                c.comment_id,
                c.post_id,
                c.username as author,
                c.text,
                coalesce(c.likes, 0) as likes,
                coalesce(c.is_reply, false) as is_reply,
                coalesce(c.reply_count, 0) as reply_count,
                c.created_at
              from social.facebook_comments c
              where c.post_id = pid.id
              order by c.likes desc nulls last, c.created_at asc
              {limit_clause}
            ) sub
            """,
            [post_ids],
        )
        for row in comment_rows:
            comments_by_post[row["post_id"]].append(row)

    result_posts = []
    total_engagement = 0
    total_comments_count = 0
    saved_comments_count = 0
    for p in posts:
        engagement = p["likes"] + p["comments_count"] + p["shares"] + p["views"]
        total_engagement += engagement
        total_comments_count += int(p["comments_count"] or 0)
        db_comment_count = int(comment_counts_by_post.get(p["id"], 0) or 0)
        saved_comments_count += db_comment_count
        post_comments = comments_by_post.get(p["id"], [])
        hashtags = _json_text_list(p.get("hashtags"), strip_prefix="#")
        if not hashtags:
            hashtags = _parse_hashtags(p.get("text"))
        mentions = _json_text_list(p.get("mentions"), prefix="@", strip_prefix="@")
        if not mentions:
            mentions = _parse_mentions(p.get("text"))
        url = (
            f"https://www.facebook.com/reel/{p['source_id']}"
            if str(p.get("post_type") or "").lower() == "reel"
            else f"https://www.facebook.com/{p['author'] or ''}/posts/{p['source_id']}"
        )
        result_posts.append(
            {
                "source_id": p["source_id"],
                "author": p["author"] or "",
                "text": p.get("text") or "",
                "url": url if p["source_id"] else "",
                "posted_at": _iso(p["ts"]),
                "likes": p["likes"],
                "comments_count": p["comments_count"],
                "shares": p["shares"],
                "views": p["views"],
                "hashtags": hashtags,
                "mentions": mentions,
                "post_type": p.get("post_type") or "feed",
                "thumbnail_url": p.get("thumbnail_url"),
                "engagement": engagement,
                "total_comments_available": db_comment_count,
                "comments": [
                    {
                        "comment_id": c["comment_id"],
                        "author": c["author"] or "",
                        "text": c["text"] or "",
                        "likes": c["likes"],
                        "is_reply": c["is_reply"],
                        "reply_count": c["reply_count"],
                        "created_at": _iso(c["created_at"]),
                    }
                    for c in post_comments
                ],
            }
        )

    return {
        "posts": result_posts,
        "total_posts": total_post_count,
        "totals": {
            "posts": len(result_posts),
            "total_comments": total_comments_count,
            "total_engagement": total_engagement,
            "expected_comments_total": total_comments_count,
            "saved_comments_total": saved_comments_count,
            "comments_saved_pct": _safe_percent(saved_comments_count, total_comments_count),
        },
    }


def _week_detail_threads(
    season_id: str,
    *,
    start_dt: datetime,
    end_dt: datetime,
    account_handles: set[str],
    max_comments: int,
    post_limit: int = 20,
    post_offset: int = 0,
) -> dict[str, Any]:
    thumbnail_expr = _platform_thumbnail_expr("p", "threads")
    hashtags_expr = (
        "coalesce(p.hashtags, '[]'::jsonb)"
        if _platform_posts_has_column("threads", "hashtags")
        else "'[]'::jsonb"
    )
    mentions_expr = (
        "coalesce(p.mentions, '[]'::jsonb)"
        if _platform_posts_has_column("threads", "mentions")
        else "'[]'::jsonb"
    )
    account_handles_list = sorted(account_handles)
    account_filter = (
        "and ltrim(lower(coalesce(nullif(p.username, ''), nullif(p.source_account, ''), '')), '@') = any(%s)"
        if account_handles_list
        else ""
    )
    params = [season_id, start_dt, end_dt]
    if account_handles_list:
        params.append(account_handles_list)
    count_rows = pg.fetch_all(
        f"""
        select count(*)::int as post_count
        from social.meta_threads_posts p
        where p.season_id = %s
          and p.posted_at >= %s
          and p.posted_at <= %s
        {account_filter}
        """,
        params,
    )
    total_post_count = int((count_rows[0] if count_rows else {}).get("post_count") or 0)
    effective_post_limit = max(0, post_limit + post_offset)
    posts = pg.fetch_all(
        f"""
        select
          p.id,
          p.post_id as source_id,
          coalesce(nullif(p.username, ''), nullif(p.source_account, ''), '') as author,
          p.text,
          coalesce(p.likes, 0) as likes,
          coalesce(p.replies_count, 0) as replies_count,
          coalesce(p.reposts, 0) as reposts,
          coalesce(p.quotes, 0) as quotes,
          coalesce(p.views, 0) as views,
          {hashtags_expr} as hashtags,
          {mentions_expr} as mentions,
          {thumbnail_expr} as thumbnail_url,
          p.posted_at as ts
        from social.meta_threads_posts p
        where p.season_id = %s
          and p.posted_at >= %s
          and p.posted_at <= %s
          {account_filter}
        order by p.posted_at desc
        {"" if effective_post_limit <= 0 else f"limit {effective_post_limit}"}
        """,
        params,
    )
    post_ids = [p["id"] for p in posts]
    comments_by_post: dict[Any, list[dict[str, Any]]] = defaultdict(list)
    comment_counts_by_post: dict[Any, int] = defaultdict(int)
    if post_ids:
        count_rows = pg.fetch_all(
            """
            select c.post_id, count(*)::int as cnt
            from social.meta_threads_comments c
            where c.post_id = any(%s::uuid[])
            group by c.post_id
            """,
            [post_ids],
        )
        for row in count_rows:
            comment_counts_by_post[row["post_id"]] = row["cnt"]
        limit_clause = f"limit {max_comments}" if max_comments > 0 else "limit all"
        comment_rows = pg.fetch_all(
            f"""
            select sub.*
            from unnest(%s::uuid[]) as pid(id)
            cross join lateral (
              select
                c.comment_id,
                c.post_id,
                c.username as author,
                c.text,
                coalesce(c.likes, 0) as likes,
                coalesce(c.is_reply, true) as is_reply,
                coalesce(c.reply_count, 0) as reply_count,
                c.created_at
              from social.meta_threads_comments c
              where c.post_id = pid.id
              order by c.likes desc nulls last, c.created_at asc
              {limit_clause}
            ) sub
            """,
            [post_ids],
        )
        for row in comment_rows:
            comments_by_post[row["post_id"]].append(row)

    result_posts = []
    total_engagement = 0
    total_comments_count = 0
    saved_comments_count = 0
    for p in posts:
        engagement = p["likes"] + p["replies_count"] + p["reposts"] + p["quotes"] + p["views"]
        total_engagement += engagement
        total_comments_count += int(p["replies_count"] or 0)
        db_comment_count = int(comment_counts_by_post.get(p["id"], 0) or 0)
        saved_comments_count += db_comment_count
        post_comments = comments_by_post.get(p["id"], [])
        hashtags = _json_text_list(p.get("hashtags"), strip_prefix="#")
        if not hashtags:
            hashtags = _parse_hashtags(p.get("text"))
        mentions = _json_text_list(p.get("mentions"), prefix="@", strip_prefix="@")
        if not mentions:
            mentions = _parse_mentions(p.get("text"))
        result_posts.append(
            {
                "source_id": p["source_id"],
                "author": p["author"] or "",
                "text": p.get("text") or "",
                "url": f"https://www.threads.com/@{p['author'] or ''}/post/{p['source_id']}" if p["source_id"] else "",
                "posted_at": _iso(p["ts"]),
                "likes": p["likes"],
                "replies_count": p["replies_count"],
                "reposts": p["reposts"],
                "quotes": p["quotes"],
                "views": p["views"],
                "hashtags": hashtags,
                "mentions": mentions,
                "thumbnail_url": p.get("thumbnail_url"),
                "engagement": engagement,
                "total_comments_available": db_comment_count,
                "comments": [
                    {
                        "comment_id": c["comment_id"],
                        "author": c["author"] or "",
                        "text": c["text"] or "",
                        "likes": c["likes"],
                        "is_reply": c["is_reply"],
                        "reply_count": c["reply_count"],
                        "created_at": _iso(c["created_at"]),
                    }
                    for c in post_comments
                ],
            }
        )

    return {
        "posts": result_posts,
        "total_posts": total_post_count,
        "totals": {
            "posts": len(result_posts),
            "total_comments": total_comments_count,
            "total_engagement": total_engagement,
            "expected_comments_total": total_comments_count,
            "saved_comments_total": saved_comments_count,
            "comments_saved_pct": _safe_percent(saved_comments_count, total_comments_count),
        },
    }


_WEEK_DETAIL_HANDLERS: dict[str, Any] = {
    "instagram": _week_detail_instagram,
    "tiktok": _week_detail_tiktok,
    "youtube": _week_detail_youtube,
    "twitter": _week_detail_twitter,
    "facebook": _week_detail_facebook,
    "threads": _week_detail_threads,
}


def get_week_detail(
    season_id: str,
    *,
    week_index: int,
    platforms: list[str] | None,
    timezone: str,
    source_scope: str,
    max_comments_per_post: int = 25,
    post_limit: int = 20,
    post_offset: int = 0,
) -> dict[str, Any]:
    """Return detailed post-level data for a single week of a season."""
    post_limit = max(0, int(post_limit))
    if post_offset < 0:
        post_offset = 0

    context = get_season_context(season_id)
    available_platforms = [p for p in (platforms or list(SUPPORTED_PLATFORMS)) if p in SUPPORTED_PLATFORMS]
    if not available_platforms:
        available_platforms = list(SUPPORTED_PLATFORMS)

    now = _now_utc()
    week_windows, _week_zero_start_local = _resolve_week_windows(
        context,
        timezone=timezone,
        source_scope=source_scope,
        now_utc=now,
    )
    windows_by_index = {w.week_index: w for w in week_windows}
    window = windows_by_index.get(week_index)
    if window is None:
        raise ValueError(f"Week {week_index} is not available for this season")

    start_dt = window.start_local.astimezone(UTC)
    end_dt = (window.end_local - timedelta(microseconds=1)).astimezone(UTC)
    if end_dt < start_dt:
        end_dt = start_dt

    target_accounts_by_platform = _target_accounts_by_platform(
        season_id,
        source_scope=source_scope,
        context=context,
    )
    requires_target_accounts = source_scope in {"bravo", "creator"}

    platform_results: dict[str, Any] = {}
    merged_posts: list[tuple[datetime | None, str, dict[str, Any]]] = []
    grand_posts = 0
    grand_comments = 0
    grand_engagement = 0
    grand_expected_comments = 0
    grand_saved_comments = 0

    for platform in available_platforms:
        handler = _WEEK_DETAIL_HANDLERS.get(platform)
        if not handler:
            continue
        account_handles = set(target_accounts_by_platform.get(platform, set()))
        if requires_target_accounts and not account_handles:
            result = {
                "posts": [],
                "total_posts": 0,
                "totals": {
                    "posts": 0,
                    "total_comments": 0,
                    "total_engagement": 0,
                },
            }
            platform_results[platform] = result
            continue
        result = handler(
            season_id,
            start_dt=start_dt,
            end_dt=end_dt,
            account_handles=account_handles,
            max_comments=max_comments_per_post,
            post_limit=post_limit,
            post_offset=post_offset,
        )
        result_total_posts = int(result.get("total_posts", len(result.get("posts") or [])) or 0)
        platform_results[platform] = {
            "posts": [],
            "total_posts": result_total_posts,
            "totals": result.get("totals", {}),
        }
        for post in result.get("posts") or []:
            merged_posts.append((_coerce_dt(post.get("posted_at")), platform, post))
        totals = result.get("totals", {})
        grand_posts += int(totals.get("posts", 0))
        grand_comments += totals.get("total_comments", 0)
        grand_engagement += totals.get("total_engagement", 0)
        grand_expected_comments += int(totals.get("expected_comments_total") or 0)
        grand_saved_comments += int(totals.get("saved_comments_total") or 0)

    merged_posts.sort(
        key=lambda item: item[0] or datetime.min.replace(tzinfo=UTC),
        reverse=True,
    )
    if post_limit <= 0:
        page_posts = merged_posts
        page_end = len(merged_posts)
    else:
        page_end = post_offset + post_limit
        page_posts = merged_posts[post_offset:page_end]
    paged_total_posts = len(merged_posts)

    posts_by_platform: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for _, platform, post in page_posts:
        posts_by_platform[platform].append(post)
    for platform in available_platforms:
        if platform in platform_results:
            platform_results[platform]["posts"] = posts_by_platform.get(platform, [])
            if not posts_by_platform.get(platform):
                platform_results[platform]["posts"] = []
    grand_posts = 0
    for platform in available_platforms:
        grand_posts += int(platform_results.get(platform, {}).get("total_posts", 0) or 0)

    week_end_inclusive = window.end_local - timedelta(microseconds=1)
    latest_run = pg.fetch_one(
        """
        select id::text as run_id
        from social.scrape_runs
        where season_id = %s
          and source_scope = %s
        order by created_at desc
        limit 1
        """,
        [season_id, source_scope],
    )
    return {
        "week": {
            "week_index": week_index,
            "label": _week_window_label(window, timezone=timezone),
            "start": _iso(window.start_local.astimezone(UTC)),
            "end": _iso(week_end_inclusive.astimezone(UTC)),
            "week_type": window.week_type,
            "episode_number": window.episode_number,
        },
        "season": {
            "season_id": context.season_id,
            "show_id": context.show_id,
            "show_name": context.show_name,
            "show_slug": context.show_slug,
            "season_number": context.season_number,
        },
        "source_scope": source_scope,
        "platforms": platform_results,
        "pagination": {
            "limit": post_limit,
            "offset": post_offset,
            "returned": len(page_posts),
            "total": paged_total_posts,
            "has_more": page_end < paged_total_posts,
        },
        "totals": {
            "posts": grand_posts,
            "total_comments": grand_comments,
            "total_engagement": grand_engagement,
            "expected_comments_total": grand_expected_comments,
            "saved_comments_total": grand_saved_comments,
            "comments_saved_pct": _safe_percent(grand_saved_comments, grand_expected_comments),
        },
        "diagnostics": {
            "run_id": (latest_run or {}).get("run_id"),
            "generated_at": _iso(_now_utc()),
            "source_scope": source_scope,
        },
    }


# ---------------------------------------------------------------------------
# Post detail — all comments for a single post, threaded
# ---------------------------------------------------------------------------


def _thread_comments(flat: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Group flat comment list into threaded tree: top-level first, replies nested."""
    by_id: dict[Any, dict[str, Any]] = {}
    roots: list[dict[str, Any]] = []
    for c in flat:
        node = {**c, "replies": []}
        by_id[c["id"]] = node

    for c in flat:
        node = by_id[c["id"]]
        parent = c.get("parent_comment_id")
        if parent and parent in by_id:
            by_id[parent]["replies"].append(node)
        else:
            roots.append(node)

    return roots


def _serialize_comment_tree(node: dict[str, Any]) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "comment_id": node.get("comment_id") or "",
        "author": node.get("author") or "",
        "text": node.get("text") or "",
        "likes": node.get("likes", 0),
        "is_reply": node.get("is_reply", False),
        "reply_count": node.get("reply_count", 0),
        "created_at": _iso(node.get("created_at")),
        "replies": [_serialize_comment_tree(r) for r in node.get("replies", [])],
    }
    media_urls = _as_text_list(node.get("media_urls"))
    hosted_media_urls = _as_text_list(node.get("hosted_media_urls"))
    if media_urls:
        payload["media_urls"] = media_urls
    if hosted_media_urls:
        payload["hosted_media_urls"] = hosted_media_urls
    media_mirror_status = str(node.get("media_mirror_status") or "").strip() or None
    if media_mirror_status:
        payload["media_mirror_status"] = media_mirror_status

    for key in ("comment_language", "aweme_id", "parent_source_comment_id", "is_author_liked"):
        if key in node:
            payload[key] = node.get(key)

    user_payload = {
        "id": str(node.get("user_id") or "").strip() or None,
        "username": str(node.get("author") or "").strip() or None,
        "display_name": str(node.get("display_name") or node.get("nickname") or "").strip() or None,
        "url": str(node.get("user_url") or "").strip() or None,
        "bio": str(node.get("user_bio") or "").strip() or None,
        "avatar_url": str(node.get("user_avatar_url") or "").strip() or None,
        "region": str(node.get("user_region") or "").strip() or None,
        "language": str(node.get("user_language") or "").strip() or None,
    }
    if any(value for value in user_payload.values()):
        payload["user"] = user_payload
    return payload


def get_post_comments(
    season_id: str,
    *,
    platform: str,
    source_id: str,
) -> dict[str, Any]:
    """Return ALL comments for a single post, threaded by parent_comment_id."""
    if platform not in SUPPORTED_PLATFORMS:
        raise ValueError(f"Unsupported platform: {platform}")

    if platform == "instagram":
        thumbnail_expr = _instagram_posts_thumbnail_expr("p")
        hosted_media_urls_expr = _instagram_posts_json_array_expr("p", "hosted_media_urls")
        post_format_expr = _instagram_posts_json_text_expr("p", "post_format")
        profile_tags_expr = _instagram_posts_json_array_expr("p", "profile_tags")
        collaborators_expr = _instagram_posts_json_array_expr("p", "collaborators")
        hashtags_expr = _instagram_posts_json_array_expr("p", "hashtags")
        mentions_expr = _instagram_posts_json_array_expr("p", "mentions")
        duration_expr = _instagram_posts_duration_expr("p")
        mirror_attempt_count_expr = _instagram_posts_json_int_expr("p", "media_mirror_attempt_count")
        mirror_last_attempt_at_expr = _instagram_posts_json_timestamptz_expr("p", "media_mirror_last_attempt_at")
        mirror_last_job_id_expr = _instagram_posts_json_text_expr("p", "media_mirror_last_job_id")

        post = pg.fetch_one(
            f"""
            select p.id, p.shortcode as source_id, p.username as author, p.caption as text,
                   coalesce(p.likes, 0) as likes, coalesce(p.comments_count, 0) as comments_count,
                   coalesce(p.views, 0) as views,
                   p.media_type,
                   coalesce(p.media_urls, '[]'::jsonb) as source_media_urls,
                   {hosted_media_urls_expr} as hosted_media_urls,
                   coalesce(nullif(p.thumbnail_url, ''), '') as source_thumbnail_url,
                   coalesce(nullif(to_jsonb(p) ->> 'hosted_thumbnail_url', ''), '') as hosted_thumbnail_url,
                   {thumbnail_expr} as thumbnail_url,
                   {post_format_expr} as post_format,
                   {profile_tags_expr} as profile_tags,
                   {collaborators_expr} as collaborators,
                   {hashtags_expr} as hashtags,
                   {mentions_expr} as mentions,
                   {duration_expr} as duration_seconds,
                   coalesce(to_jsonb(p) -> 'raw_data', '{{}}'::jsonb) as raw_data,
                   {mirror_attempt_count_expr} as media_mirror_attempt_count,
                   {mirror_last_attempt_at_expr} as media_mirror_last_attempt_at,
                   {mirror_last_job_id_expr} as media_mirror_last_job_id,
                   p.posted_at as ts
            from social.instagram_posts p
            where p.season_id = %s and p.shortcode = %s
            """,
            [season_id, source_id],
        )
        if not post:
            raise ValueError("Post not found")

        comments = pg.fetch_all(
            """
            select c.id, c.comment_id, c.parent_comment_id,
                   c.username as author, c.text,
                   coalesce(c.likes, 0) as likes,
                   coalesce(c.is_reply, false) as is_reply,
                   coalesce(c.reply_count, 0) as reply_count,
                   c.created_at
            from social.instagram_comments c
            where c.post_id = %s
            order by c.likes desc nulls last, c.created_at asc
            """,
            [post["id"]],
        )

        engagement = post["likes"] + post["comments_count"] + post["views"]
        hashtags = _json_text_list(post.get("hashtags"), strip_prefix="#")
        mentions = _json_text_list(post.get("mentions"), prefix="@", strip_prefix="@")
        if not hashtags:
            hashtags = _parse_hashtags(post.get("text"))
        if not mentions:
            mentions = _parse_mentions(post.get("text"))
        profile_tags = _json_text_list(post.get("profile_tags"), prefix="@", strip_prefix="@")
        collaborators = _json_text_list(post.get("collaborators"), prefix="@", strip_prefix="@")
        source_media_urls = _json_text_list(post.get("source_media_urls"))
        hosted_media_urls = _json_text_list(post.get("hosted_media_urls"))
        media_urls = hosted_media_urls or source_media_urls
        source_thumbnail_url = str(post.get("source_thumbnail_url") or "").strip() or None
        hosted_thumbnail_url = str(post.get("hosted_thumbnail_url") or "").strip() or None
        thumbnail_url = str(post.get("thumbnail_url") or "").strip() or source_thumbnail_url
        cover_source, cover_source_confidence = _instagram_cover_source_from_post_row(
            {
                "media_type": post.get("media_type"),
                "post_format": post.get("post_format"),
                "raw_data": post.get("raw_data"),
                "thumbnail_url": thumbnail_url,
                "source_media_urls": source_media_urls,
                "media_urls": source_media_urls,
            }
        )
        return {
            "platform": "instagram",
            "source_id": source_id,
            "author": post["author"] or "",
            "text": post.get("text") or "",
            "url": f"https://www.instagram.com/p/{source_id}/",
            "posted_at": _iso(post["ts"]),
            "thumbnail_url": thumbnail_url,
            "media_urls": media_urls,
            "source_media_urls": source_media_urls,
            "hosted_media_urls": hosted_media_urls,
            "source_thumbnail_url": source_thumbnail_url,
            "hosted_thumbnail_url": hosted_thumbnail_url,
            "cover_source": cover_source,
            "cover_source_confidence": cover_source_confidence,
            "post_format": post.get("post_format"),
            "profile_tags": profile_tags,
            "collaborators": collaborators,
            "hashtags": hashtags,
            "mentions": mentions,
            "duration_seconds": post.get("duration_seconds"),
            "media_mirror_attempt_count": post.get("media_mirror_attempt_count"),
            "media_mirror_last_attempt_at": _iso(_coerce_dt(post.get("media_mirror_last_attempt_at"))),
            "media_mirror_last_job_id": str(post.get("media_mirror_last_job_id") or "") or None,
            "stats": {
                "likes": post["likes"],
                "comments_count": post["comments_count"],
                "views": post["views"],
                "engagement": engagement,
            },
            "total_comments_in_db": len(comments),
            "comments": [_serialize_comment_tree(node) for node in _thread_comments(comments)],
        }

    if platform == "tiktok":
        thumbnail_expr = _platform_thumbnail_expr("p", "tiktok")
        tiktok_saves_expr = (
            "coalesce(p.saves, 0)"
            if _platform_posts_has_column("tiktok", "saves")
            else (
                "case when coalesce(to_jsonb(p) ->> 'saves', '') ~ '^-?[0-9]+$' "
                "then greatest(((to_jsonb(p) ->> 'saves')::int), 0) else 0 end"
            )
        )
        mentions_expr = (
            "coalesce(p.mentions, '[]'::jsonb)"
            if _platform_posts_has_column("tiktok", "mentions")
            else "'[]'::jsonb"
        )
        post = pg.fetch_one(
            f"""
            select p.id, p.video_id as source_id, p.username as author, p.description as text,
                   coalesce(p.likes, 0) as likes, coalesce(p.comments_count, 0) as comments_count,
                   coalesce(p.shares, 0) as shares, {tiktok_saves_expr} as saves, coalesce(p.views, 0) as views,
                   coalesce(p.hashtags, '[]'::jsonb) as hashtags,
                   {mentions_expr} as mentions,
                   {thumbnail_expr} as thumbnail_url,
                   coalesce(to_jsonb(p) -> 'media_urls', '[]'::jsonb) as source_media_urls,
                   coalesce(to_jsonb(p) -> 'hosted_media_urls', '[]'::jsonb) as hosted_media_urls,
                   coalesce(nullif(p.thumbnail_url, ''), '') as source_thumbnail_url,
                   coalesce(nullif(to_jsonb(p) ->> 'hosted_thumbnail_url', ''), '') as hosted_thumbnail_url,
                   p.posted_at as ts
            from social.tiktok_posts p
            where p.season_id = %s and p.video_id = %s
            """,
            [season_id, source_id],
        )
        if not post:
            raise ValueError("Post not found")

        comments = pg.fetch_all(
            """
            select c.id, c.comment_id, c.parent_comment_id,
                   coalesce(nullif(c.username, ''), nullif(c.source_account, ''), '') as author,
                   c.user_id,
                   c.nickname,
                   c.text,
                   coalesce(c.likes, 0) as likes,
                   coalesce(c.is_reply, false) as is_reply,
                   coalesce(c.reply_count, 0) as reply_count,
                   c.created_at,
                   nullif(coalesce(to_jsonb(c) ->> 'comment_language', ''), '') as comment_language,
                   case
                     when lower(coalesce(to_jsonb(c) ->> 'is_author_liked', '')) in ('true', 'false')
                     then (to_jsonb(c) ->> 'is_author_liked')::boolean
                     else null
                   end as is_author_liked,
                   nullif(coalesce(to_jsonb(c) ->> 'aweme_id', ''), '') as aweme_id,
                   nullif(coalesce(to_jsonb(c) ->> 'parent_source_comment_id', ''), '') as parent_source_comment_id,
                   nullif(coalesce(to_jsonb(c) ->> 'user_url', ''), '') as user_url,
                   nullif(coalesce(to_jsonb(c) ->> 'user_bio', ''), '') as user_bio,
                   nullif(coalesce(to_jsonb(c) ->> 'user_avatar_url', ''), '') as user_avatar_url,
                   nullif(coalesce(to_jsonb(c) ->> 'user_region', ''), '') as user_region,
                   nullif(coalesce(to_jsonb(c) ->> 'user_language', ''), '') as user_language,
                   coalesce(to_jsonb(c) -> 'media_urls', '[]'::jsonb) as media_urls,
                   coalesce(to_jsonb(c) -> 'hosted_media_urls', '[]'::jsonb) as hosted_media_urls,
                   nullif(coalesce(to_jsonb(c) ->> 'media_mirror_status', ''), '') as media_mirror_status
            from social.tiktok_comments c
            where c.post_id = %s
            order by c.likes desc nulls last, c.created_at asc
            """,
            [post["id"]],
        )

        engagement = post["likes"] + post["comments_count"] + post["shares"] + post["views"]
        source_media_urls = _json_text_list(post.get("source_media_urls"))
        hosted_media_urls = _json_text_list(post.get("hosted_media_urls"))
        media_urls = hosted_media_urls or source_media_urls
        source_thumbnail_url = str(post.get("source_thumbnail_url") or "").strip() or None
        hosted_thumbnail_url = str(post.get("hosted_thumbnail_url") or "").strip() or None
        hashtags = _json_text_list(post.get("hashtags"), strip_prefix="#")
        if not hashtags:
            hashtags = _parse_hashtags(post.get("text"))
        mentions = _json_text_list(post.get("mentions"), prefix="@", strip_prefix="@")
        if not mentions:
            mentions = _parse_mentions(post.get("text"))
        return {
            "platform": "tiktok",
            "source_id": source_id,
            "author": post["author"] or "",
            "text": post.get("text") or "",
            "hashtags": hashtags,
            "mentions": mentions,
            "url": f"https://www.tiktok.com/@{post['author'] or ''}/video/{source_id}",
            "posted_at": _iso(post["ts"]),
            "thumbnail_url": post.get("thumbnail_url"),
            "media_urls": media_urls,
            "source_media_urls": source_media_urls,
            "hosted_media_urls": hosted_media_urls,
            "source_thumbnail_url": source_thumbnail_url,
            "hosted_thumbnail_url": hosted_thumbnail_url,
            "stats": {
                "likes": post["likes"],
                "comments_count": post["comments_count"],
                "shares": post["shares"],
                "saves": int(post.get("saves") or 0),
                "views": post["views"],
                "engagement": engagement,
            },
            "total_comments_in_db": len(comments),
            "comments": [_serialize_comment_tree(node) for node in _thread_comments(comments)],
        }

    if platform == "youtube":
        thumbnail_expr = _platform_thumbnail_expr("v", "youtube")
        youtube_is_short_expr = _youtube_is_short_expr("v")
        hashtags_expr = (
            "coalesce(v.hashtags, '[]'::jsonb)"
            if _platform_posts_has_column("youtube", "hashtags")
            else "'[]'::jsonb"
        )
        mentions_expr = (
            "coalesce(v.mentions, '[]'::jsonb)"
            if _platform_posts_has_column("youtube", "mentions")
            else "'[]'::jsonb"
        )
        post = pg.fetch_one(
            f"""
            select v.id, v.video_id as source_id, v.channel_title as author,
                   v.title, v.description as text,
                   coalesce(v.views, 0) as views, coalesce(v.likes, 0) as likes,
                   coalesce(v.comments_count, 0) as comments_count,
                   {hashtags_expr} as hashtags,
                   {mentions_expr} as mentions,
                   {youtube_is_short_expr} as is_short,
                   {thumbnail_expr} as thumbnail_url,
                   coalesce(to_jsonb(v) -> 'media_urls', '[]'::jsonb) as source_media_urls,
                   coalesce(to_jsonb(v) -> 'hosted_media_urls', '[]'::jsonb) as hosted_media_urls,
                   coalesce(nullif(v.thumbnail_url, ''), '') as source_thumbnail_url,
                   coalesce(nullif(to_jsonb(v) ->> 'hosted_thumbnail_url', ''), '') as hosted_thumbnail_url,
                   v.published_at as ts
            from social.youtube_videos v
            where v.season_id = %s and v.video_id = %s
            """,
            [season_id, source_id],
        )
        if not post:
            raise ValueError("Post not found")

        comments = pg.fetch_all(
            """
            select c.id, c.comment_id, c.parent_comment_id,
                   c.author, c.text,
                   coalesce(c.likes, 0) as likes,
                   coalesce(c.is_reply, false) as is_reply,
                   coalesce(c.reply_count, 0) as reply_count,
                   c.created_at
            from social.youtube_comments c
            where c.video_id = %s
            order by c.likes desc nulls last, c.created_at asc
            """,
            [post["id"]],
        )

        effective_comments_count = _youtube_effective_comment_count(
            reported_count=post["comments_count"],
            saved_count=len(comments),
        )
        engagement = post["views"] + post["likes"] + effective_comments_count
        source_media_urls = _json_text_list(post.get("source_media_urls"))
        hosted_media_urls = _json_text_list(post.get("hosted_media_urls"))
        media_urls = hosted_media_urls or source_media_urls
        source_thumbnail_url = str(post.get("source_thumbnail_url") or "").strip() or None
        hosted_thumbnail_url = str(post.get("hosted_thumbnail_url") or "").strip() or None
        token_text = f"{post.get('title') or ''}\n{post.get('text') or ''}"
        hashtags = _json_text_list(post.get("hashtags"), strip_prefix="#")
        if not hashtags:
            hashtags = _parse_hashtags(token_text)
        mentions = _json_text_list(post.get("mentions"), prefix="@", strip_prefix="@")
        if not mentions:
            mentions = _parse_mentions(token_text)
        return {
            "platform": "youtube",
            "source_id": source_id,
            "author": post["author"] or "",
            "title": post.get("title") or "",
            "text": post.get("text") or "",
            "hashtags": hashtags,
            "mentions": mentions,
            "url": (
                f"https://www.youtube.com/shorts/{source_id}"
                if bool(post.get("is_short"))
                else f"https://www.youtube.com/watch?v={source_id}"
            ),
            "posted_at": _iso(post["ts"]),
            "thumbnail_url": post.get("thumbnail_url"),
            "media_urls": media_urls,
            "source_media_urls": source_media_urls,
            "hosted_media_urls": hosted_media_urls,
            "source_thumbnail_url": source_thumbnail_url,
            "hosted_thumbnail_url": hosted_thumbnail_url,
            "stats": {
                "views": post["views"],
                "likes": post["likes"],
                "comments_count": effective_comments_count,
                "engagement": engagement,
            },
            "total_comments_in_db": len(comments),
            "comments": [_serialize_comment_tree(node) for node in _thread_comments(comments)],
        }

    if platform == "twitter":
        thumbnail_expr = _platform_thumbnail_expr("t", "twitter")
        hosted_media_expr = _platform_hosted_media_expr("t")
        post = pg.fetch_one(
            f"""
            select t.tweet_id as source_id,
                   coalesce(nullif(t.username, ''), nullif(t.source_account, ''), '') as author,
                   nullif(coalesce(to_jsonb(t) ->> 'user_id', ''), '') as user_id,
                   coalesce(
                     nullif(coalesce(to_jsonb(t) ->> 'user_profile_url', ''), ''),
                     case
                       when coalesce(nullif(t.username, ''), nullif(t.source_account, '')) is null then null
                       else 'https://x.com/' || ltrim(
                         coalesce(nullif(t.username, ''), nullif(t.source_account, '')),
                         '@'
                       )
                     end
                   ) as user_profile_url,
                   nullif(coalesce(to_jsonb(t) ->> 'user_avatar_url', ''), '') as user_avatar_url,
                   t.display_name, t.text,
                   coalesce(t.likes, 0) as likes,
                   coalesce(t.retweets, 0) as retweets,
                   coalesce(t.replies_count, 0) as replies_count,
                   coalesce(t.quotes, 0) as quotes,
                   coalesce(t.views, 0) as views,
                   {thumbnail_expr} as thumbnail_url,
                   coalesce(t.media_urls, '[]'::jsonb) as source_media_urls,
                   {hosted_media_expr} as hosted_media_urls,
                   coalesce(nullif(to_jsonb(t) ->> 'hosted_thumbnail_url', ''), '') as hosted_thumbnail_url,
                   t.created_at as ts
            from social.twitter_tweets t
            where t.season_id = %s and t.tweet_id = %s and t.is_reply = false
            """,
            [season_id, source_id],
        )
        if not post:
            raise ValueError("Post not found")

        # Full recursive reply chain
        reply_rows = pg.fetch_all(
            f"""
            with recursive thread_replies as (
              select r.tweet_id, r.reply_to_tweet_id,
                     r.reply_to_tweet_id as parent_id
              from social.twitter_tweets r
              where r.season_id = %s
                and r.is_reply = true
                and r.reply_to_tweet_id = %s
              union
              select child.tweet_id, child.reply_to_tweet_id,
                     child.reply_to_tweet_id as parent_id
              from social.twitter_tweets child
              join thread_replies parent on child.reply_to_tweet_id = parent.tweet_id
              where child.season_id = %s
                and child.is_reply = true
            )
            select
              t.tweet_id as id,
              t.tweet_id as comment_id,
              t.reply_to_tweet_id as parent_comment_id,
              coalesce(nullif(t.username, ''), nullif(t.source_account, ''), '') as author,
              coalesce(t.display_name, '') as display_name,
              nullif(coalesce(to_jsonb(t) ->> 'user_id', ''), '') as user_id,
              coalesce(
                nullif(coalesce(to_jsonb(t) ->> 'user_profile_url', ''), ''),
                case
                  when coalesce(nullif(t.username, ''), nullif(t.source_account, '')) is null then null
                  else 'https://x.com/' || ltrim(
                    coalesce(nullif(t.username, ''), nullif(t.source_account, '')),
                    '@'
                  )
                end
              ) as user_url,
              nullif(coalesce(to_jsonb(t) ->> 'user_avatar_url', ''), '') as user_avatar_url,
              t.text,
              coalesce(t.likes, 0) as likes,
              true as is_reply,
              coalesce(t.replies_count, 0) as reply_count,
              t.media_urls,
              {hosted_media_expr} as hosted_media_urls,
              t.created_at
            from thread_replies tr
            join social.twitter_tweets t on t.tweet_id = tr.tweet_id
            where t.season_id = %s
            order by t.likes desc nulls last, t.created_at asc
            """,
            [season_id, source_id, season_id, season_id],
        )

        quote_rows = pg.fetch_all(
            f"""
            select
              t.tweet_id as comment_id,
              coalesce(nullif(t.username, ''), nullif(t.source_account, ''), '') as author,
              nullif(coalesce(to_jsonb(t) ->> 'user_id', ''), '') as user_id,
              coalesce(
                nullif(coalesce(to_jsonb(t) ->> 'user_profile_url', ''), ''),
                case
                  when coalesce(nullif(t.username, ''), nullif(t.source_account, '')) is null then null
                  else 'https://x.com/' || ltrim(
                    coalesce(nullif(t.username, ''), nullif(t.source_account, '')),
                    '@'
                  )
                end
              ) as user_url,
              nullif(coalesce(to_jsonb(t) ->> 'user_avatar_url', ''), '') as user_avatar_url,
              coalesce(t.display_name, '') as display_name,
              t.text,
              coalesce(t.likes, 0) as likes,
              coalesce(t.retweets, 0) as retweets,
              coalesce(t.replies_count, 0) as reply_count,
              coalesce(t.quotes, 0) as quotes,
              coalesce(t.views, 0) as views,
              t.media_urls,
              {hosted_media_expr} as hosted_media_urls,
              {thumbnail_expr} as thumbnail_url,
              t.created_at
            from social.twitter_tweets t
            where t.season_id = %s
              and t.is_quote = true
              and t.quoted_tweet_id = %s
            order by t.likes desc nulls last, t.created_at asc
            """,
            [season_id, source_id],
        )

        # Thread replies: direct replies to root go under root, deeper replies nest
        threaded: list[dict[str, Any]] = []
        by_id: dict[str, dict[str, Any]] = {}
        for r in reply_rows:
            node = {**r, "replies": []}
            by_id[r["id"]] = node
        for r in reply_rows:
            node = by_id[r["id"]]
            parent_id = r.get("parent_comment_id")
            if parent_id == source_id or parent_id not in by_id:
                threaded.append(node)
            else:
                by_id[parent_id]["replies"].append(node)

        engagement = post["likes"] + post["retweets"] + post["replies_count"] + post["quotes"] + post["views"]
        author = post["author"] or "i"
        source_media_urls = _json_text_list(post.get("source_media_urls"))
        hosted_media_urls = _json_text_list(post.get("hosted_media_urls"))
        media_urls = hosted_media_urls or source_media_urls
        source_thumbnail_url = _select_thumbnail_candidate(source_media_urls) if source_media_urls else None
        hosted_thumbnail_url = str(post.get("hosted_thumbnail_url") or "").strip() or None
        return {
            "platform": "twitter",
            "source_id": source_id,
            "author": post["author"] or "",
            "user": {
                "id": post.get("user_id"),
                "username": post["author"] or "",
                "display_name": post.get("display_name") or "",
                "url": post.get("user_url"),
                "avatar_url": post.get("user_avatar_url"),
            },
            "display_name": post.get("display_name") or "",
            "text": post.get("text") or "",
            "url": f"https://x.com/{author}/status/{source_id}",
            "posted_at": _iso(post["ts"]),
            "thumbnail_url": post.get("thumbnail_url"),
            "media_urls": media_urls,
            "source_media_urls": source_media_urls,
            "hosted_media_urls": hosted_media_urls,
            "source_thumbnail_url": source_thumbnail_url,
            "hosted_thumbnail_url": hosted_thumbnail_url,
            "stats": {
                "likes": post["likes"],
                "retweets": post["retweets"],
                "replies_count": post["replies_count"],
                "quotes": post["quotes"],
                "views": post["views"],
                "engagement": engagement,
            },
            "total_comments_in_db": len(reply_rows),
            "total_quotes_in_db": len(quote_rows),
            "comments": [_serialize_comment_tree(node) for node in threaded],
            "quotes": [
                {
                    "comment_id": q["comment_id"],
                    "author": q["author"] or "",
                    "display_name": q.get("display_name") or "",
                    "url": (
                        f"https://x.com/{str(q.get('author') or 'i').strip().lstrip('@')}/status/{q['comment_id']}"
                        if q.get("comment_id")
                        else ""
                    ),
                    "text": q["text"] or "",
                    "likes": q["likes"],
                    "retweets": q["retweets"],
                    "reply_count": q["reply_count"],
                    "quotes": q["quotes"],
                    "views": q["views"],
                    "is_reply": False,
                    "media_urls": q.get("media_urls"),
                    "hosted_media_urls": q.get("hosted_media_urls"),
                    "thumbnail_url": q.get("thumbnail_url"),
                    "created_at": _iso(q["created_at"]),
                    "user": {
                        "id": q.get("user_id"),
                        "username": q["author"] or "",
                        "display_name": q.get("display_name") or "",
                        "url": q.get("user_url"),
                        "avatar_url": q.get("user_avatar_url"),
                    },
                }
                for q in quote_rows
            ],
        }

    if platform == "facebook":
        thumbnail_expr = _platform_thumbnail_expr("p", "facebook")
        hashtags_expr = (
            "coalesce(p.hashtags, '[]'::jsonb)"
            if _platform_posts_has_column("facebook", "hashtags")
            else "'[]'::jsonb"
        )
        mentions_expr = (
            "coalesce(p.mentions, '[]'::jsonb)"
            if _platform_posts_has_column("facebook", "mentions")
            else "'[]'::jsonb"
        )
        post = pg.fetch_one(
            f"""
            select
              p.id,
              p.post_id as source_id,
              coalesce(nullif(p.username, ''), nullif(p.source_account, ''), '') as author,
              p.caption as text,
              coalesce(p.post_type, 'feed') as post_type,
              coalesce(p.likes, 0) as likes,
              coalesce(p.comments_count, 0) as comments_count,
              coalesce(p.shares, 0) as shares,
              coalesce(p.views, 0) as views,
              {hashtags_expr} as hashtags,
              {mentions_expr} as mentions,
              {thumbnail_expr} as thumbnail_url,
              coalesce(to_jsonb(p) -> 'media_urls', '[]'::jsonb) as source_media_urls,
              coalesce(to_jsonb(p) -> 'hosted_media_urls', '[]'::jsonb) as hosted_media_urls,
              coalesce(nullif(p.thumbnail_url, ''), '') as source_thumbnail_url,
              coalesce(nullif(to_jsonb(p) ->> 'hosted_thumbnail_url', ''), '') as hosted_thumbnail_url,
              p.posted_at as ts
            from social.facebook_posts p
            where p.season_id = %s and p.post_id = %s
            """,
            [season_id, source_id],
        )
        if not post:
            raise ValueError("Post not found")
        comments = pg.fetch_all(
            """
            select
              c.id,
              c.comment_id,
              c.parent_comment_id,
              c.username as author,
              c.text,
              coalesce(c.likes, 0) as likes,
              coalesce(c.is_reply, false) as is_reply,
              coalesce(c.reply_count, 0) as reply_count,
              c.created_at
            from social.facebook_comments c
            where c.post_id = %s
            order by c.likes desc nulls last, c.created_at asc
            """,
            [post["id"]],
        )
        engagement = post["likes"] + post["comments_count"] + post["shares"] + post["views"]
        url = (
            f"https://www.facebook.com/reel/{source_id}"
            if str(post.get("post_type") or "").lower() == "reel"
            else f"https://www.facebook.com/{post['author'] or ''}/posts/{source_id}"
        )
        source_media_urls = _json_text_list(post.get("source_media_urls"))
        hosted_media_urls = _json_text_list(post.get("hosted_media_urls"))
        media_urls = hosted_media_urls or source_media_urls
        source_thumbnail_url = str(post.get("source_thumbnail_url") or "").strip() or None
        hosted_thumbnail_url = str(post.get("hosted_thumbnail_url") or "").strip() or None
        hashtags = _json_text_list(post.get("hashtags"), strip_prefix="#")
        if not hashtags:
            hashtags = _parse_hashtags(post.get("text"))
        mentions = _json_text_list(post.get("mentions"), prefix="@", strip_prefix="@")
        if not mentions:
            mentions = _parse_mentions(post.get("text"))
        return {
            "platform": "facebook",
            "source_id": source_id,
            "author": post["author"] or "",
            "text": post.get("text") or "",
            "hashtags": hashtags,
            "mentions": mentions,
            "url": url,
            "posted_at": _iso(post["ts"]),
            "thumbnail_url": post.get("thumbnail_url"),
            "media_urls": media_urls,
            "source_media_urls": source_media_urls,
            "hosted_media_urls": hosted_media_urls,
            "source_thumbnail_url": source_thumbnail_url,
            "hosted_thumbnail_url": hosted_thumbnail_url,
            "stats": {
                "likes": post["likes"],
                "comments_count": post["comments_count"],
                "shares": post["shares"],
                "views": post["views"],
                "engagement": engagement,
            },
            "total_comments_in_db": len(comments),
            "comments": [_serialize_comment_tree(node) for node in _thread_comments(comments)],
        }

    if platform == "threads":
        thumbnail_expr = _platform_thumbnail_expr("p", "threads")
        hashtags_expr = (
            "coalesce(p.hashtags, '[]'::jsonb)"
            if _platform_posts_has_column("threads", "hashtags")
            else "'[]'::jsonb"
        )
        mentions_expr = (
            "coalesce(p.mentions, '[]'::jsonb)"
            if _platform_posts_has_column("threads", "mentions")
            else "'[]'::jsonb"
        )
        post = pg.fetch_one(
            f"""
            select
              p.id,
              p.post_id as source_id,
              coalesce(nullif(p.username, ''), nullif(p.source_account, ''), '') as author,
              p.text,
              coalesce(p.likes, 0) as likes,
              coalesce(p.replies_count, 0) as replies_count,
              coalesce(p.reposts, 0) as reposts,
              coalesce(p.quotes, 0) as quotes,
              coalesce(p.views, 0) as views,
              {hashtags_expr} as hashtags,
              {mentions_expr} as mentions,
              {thumbnail_expr} as thumbnail_url,
              coalesce(to_jsonb(p) -> 'media_urls', '[]'::jsonb) as source_media_urls,
              coalesce(to_jsonb(p) -> 'hosted_media_urls', '[]'::jsonb) as hosted_media_urls,
              coalesce(nullif(p.thumbnail_url, ''), '') as source_thumbnail_url,
              coalesce(nullif(to_jsonb(p) ->> 'hosted_thumbnail_url', ''), '') as hosted_thumbnail_url,
              p.posted_at as ts
            from social.meta_threads_posts p
            where p.season_id = %s and p.post_id = %s
            """,
            [season_id, source_id],
        )
        if not post:
            raise ValueError("Post not found")
        comments = pg.fetch_all(
            """
            select
              c.id,
              c.comment_id,
              c.parent_comment_id,
              c.username as author,
              c.text,
              coalesce(c.likes, 0) as likes,
              coalesce(c.is_reply, true) as is_reply,
              coalesce(c.reply_count, 0) as reply_count,
              c.created_at
            from social.meta_threads_comments c
            where c.post_id = %s
            order by c.likes desc nulls last, c.created_at asc
            """,
            [post["id"]],
        )
        engagement = post["likes"] + post["replies_count"] + post["reposts"] + post["quotes"] + post["views"]
        source_media_urls = _json_text_list(post.get("source_media_urls"))
        hosted_media_urls = _json_text_list(post.get("hosted_media_urls"))
        media_urls = hosted_media_urls or source_media_urls
        source_thumbnail_url = str(post.get("source_thumbnail_url") or "").strip() or None
        hosted_thumbnail_url = str(post.get("hosted_thumbnail_url") or "").strip() or None
        hashtags = _json_text_list(post.get("hashtags"), strip_prefix="#")
        if not hashtags:
            hashtags = _parse_hashtags(post.get("text"))
        mentions = _json_text_list(post.get("mentions"), prefix="@", strip_prefix="@")
        if not mentions:
            mentions = _parse_mentions(post.get("text"))
        return {
            "platform": "threads",
            "source_id": source_id,
            "author": post["author"] or "",
            "text": post.get("text") or "",
            "hashtags": hashtags,
            "mentions": mentions,
            "url": f"https://www.threads.com/@{post['author'] or ''}/post/{source_id}",
            "posted_at": _iso(post["ts"]),
            "thumbnail_url": post.get("thumbnail_url"),
            "media_urls": media_urls,
            "source_media_urls": source_media_urls,
            "hosted_media_urls": hosted_media_urls,
            "source_thumbnail_url": source_thumbnail_url,
            "hosted_thumbnail_url": hosted_thumbnail_url,
            "stats": {
                "likes": post["likes"],
                "replies_count": post["replies_count"],
                "reposts": post["reposts"],
                "quotes": post["quotes"],
                "views": post["views"],
                "engagement": engagement,
            },
            "total_comments_in_db": len(comments),
            "comments": [_serialize_comment_tree(node) for node in _thread_comments(comments)],
        }

    raise ValueError(f"Unsupported platform: {platform}")


def refresh_post_comments(
    season_id: str,
    *,
    platform: str,
    source_id: str,
    max_comments_per_post: int = 100000,
    fetch_replies: bool = True,
) -> dict[str, Any]:
    """Re-sync comments for a single post/video/reply thread."""
    normalized_platform = (platform or "").strip().lower()
    if normalized_platform not in SUPPORTED_PLATFORMS:
        raise ValueError(f"Unsupported platform: {platform}")

    context = get_season_context(season_id)
    try:
        max_comments = max(0, int(max_comments_per_post))
    except (TypeError, ValueError):
        max_comments = 0

    if normalized_platform == "instagram":
        row = pg.fetch_one(
            """
            select p.id::text as id,
                   coalesce(nullif(p.source_account, ''), nullif(p.username, ''), '') as account
            from social.instagram_posts p
            where p.season_id = %s and p.shortcode = %s
            """,
            [season_id, source_id],
        )
        if not row:
            raise ValueError("Post not found")

        from trr_backend.socials.instagram import InstagramScraper

        account = str(row.get("account") or "")
        scraper = InstagramScraper(cookies=_load_instagram_cookies())
        comments: list[Any] = []
        upserted = 0
        fetch_failed = False
        fail_reason = ""
        observed_comment_ids: set[str] = set()
        comments_marked_missing = 0
        if max_comments > 0:
            try:
                comments = scraper.fetch_comments(
                    source_id,
                    max_comments=max_comments,
                    fetch_replies=fetch_replies,
                    delay=0.25,
                )
                fail_reason = str(getattr(scraper, "last_comment_fetch_reason", "") or "")
            except Exception:
                fetch_failed = True
                fail_reason = str(getattr(scraper, "last_comment_fetch_reason", "") or "fetch_exception")
                logger.exception(
                    "[instagram] refresh_post_comments failed for shortcode=%s",
                    source_id,
                )
        else:
            fail_reason = "fetch_disabled"

        with pg.db_connection() as conn:
            for comment in comments:
                upserted += _upsert_instagram_comment_tree(
                    context,
                    job_id=None,
                    run_id=None,
                    account=account,
                    post_id=str(row["id"]),
                    comment=comment,
                    observed_comment_ids=observed_comment_ids,
                    conn=conn,
                )
            is_complete = _is_comment_fetch_complete(
                fetch_failed=fetch_failed,
                fail_reason=fail_reason,
                auth_failed=bool(getattr(scraper, "comments_auth_failed", False)),
                fetched_count=len(comments),
                max_comments_per_post=max_comments,
            )
            incomplete_reason = None if is_complete else (fail_reason or "incomplete_or_capped")
            if is_complete and max_comments > 0:
                comments_marked_missing = _mark_missing_comments_for_anchor(
                    platform="instagram",
                    anchor_id=str(row["id"]),
                    observed_comment_ids=observed_comment_ids,
                    conn=conn,
                )
                _reconcile_post_comment_count(
                    platform="instagram",
                    post_db_id=str(row["id"]),
                    conn=conn,
                )
        total_comments = _count_stored_comments([str(row["id"])], "instagram").get(str(row["id"]), 0)
        return {
            "platform": normalized_platform,
            "source_id": source_id,
            "comments_fetched": len(comments),
            "comments_upserted": upserted,
            "total_comments_in_db": total_comments,
            "fetch_replies": fetch_replies,
            "max_comments_per_post": max_comments,
            "comment_fetch_reason": fail_reason,
            "comments_auth_failed": bool(getattr(scraper, "comments_auth_failed", False)),
            "fetch_failed": fetch_failed,
            "is_complete": is_complete,
            "comments_marked_missing": comments_marked_missing,
            "incomplete_reason": incomplete_reason,
            "comment_fail_reasons": [fail_reason] if fail_reason else [],
        }

    if normalized_platform == "tiktok":
        row = pg.fetch_one(
            """
            select p.id::text as id,
                   coalesce(nullif(p.source_account, ''), nullif(p.username, ''), '') as account
            from social.tiktok_posts p
            where p.season_id = %s and p.video_id = %s
            """,
            [season_id, source_id],
        )
        if not row:
            raise ValueError("Post not found")

        from trr_backend.socials.tiktok import TikTokScraper

        account = str(row.get("account") or "")
        scraper = TikTokScraper(cookies=_load_tiktok_cookies())
        comments: list[Any] = []
        upserted = 0
        fetch_failed = False
        fail_reason = ""
        observed_comment_ids: set[str] = set()
        comments_marked_missing = 0
        if max_comments > 0:
            try:
                comments = scraper.fetch_comments(
                    source_id,
                    username=account,
                    max_comments=max_comments,
                    fetch_replies=fetch_replies,
                    delay=0.5,
                )
                _trim_nested_comment_replies(comments, max_replies_per_post=max_comments if fetch_replies else 0)
                fail_reason = str(
                    getattr(scraper, "last_comment_fetch_reason", "")
                    or getattr(scraper, "_last_api_fail_reason", "")
                    or ""
                )
            except Exception:
                fetch_failed = True
                fail_reason = str(
                    getattr(scraper, "last_comment_fetch_reason", "")
                    or getattr(scraper, "_last_api_fail_reason", "")
                    or "fetch_exception"
                )
                logger.exception("[tiktok] refresh_post_comments failed for video_id=%s", source_id)
        else:
            fail_reason = "fetch_disabled"

        with pg.db_connection() as conn:
            for comment in comments:
                upserted += _upsert_tiktok_comment_tree(
                    context,
                    job_id=None,
                    run_id=None,
                    account=account,
                    post_id=str(row["id"]),
                    comment=comment,
                    observed_comment_ids=observed_comment_ids,
                    conn=conn,
                )
            is_complete = _is_comment_fetch_complete(
                fetch_failed=fetch_failed,
                fail_reason=fail_reason,
                auth_failed=bool(getattr(scraper, "comments_auth_failed", False)),
                fetched_count=len(comments),
                max_comments_per_post=max_comments,
            )
            incomplete_reason = None if is_complete else (fail_reason or "incomplete_or_capped")
            if is_complete and max_comments > 0:
                comments_marked_missing = _mark_missing_comments_for_anchor(
                    platform="tiktok",
                    anchor_id=str(row["id"]),
                    observed_comment_ids=observed_comment_ids,
                    conn=conn,
                )
                _reconcile_post_comment_count(
                    platform="tiktok",
                    post_db_id=str(row["id"]),
                    conn=conn,
                )
        total_comments = _count_stored_comments([str(row["id"])], "tiktok").get(str(row["id"]), 0)
        return {
            "platform": normalized_platform,
            "source_id": source_id,
            "comments_fetched": len(comments),
            "comments_upserted": upserted,
            "total_comments_in_db": total_comments,
            "fetch_replies": fetch_replies,
            "max_comments_per_post": max_comments,
            "comment_fetch_reason": fail_reason,
            "comments_auth_failed": bool(getattr(scraper, "comments_auth_failed", False)),
            "fetch_failed": fetch_failed,
            "is_complete": is_complete,
            "comments_marked_missing": comments_marked_missing,
            "incomplete_reason": incomplete_reason,
            "comment_fail_reasons": [fail_reason] if fail_reason else [],
        }

    if normalized_platform == "youtube":
        row = pg.fetch_one(
            """
            select v.id::text as id,
                   coalesce(nullif(v.source_account, ''), nullif(v.channel_title, ''), '') as account
            from social.youtube_videos v
            where v.season_id = %s and v.video_id = %s
            """,
            [season_id, source_id],
        )
        if not row:
            raise ValueError("Post not found")

        from trr_backend.socials.youtube import YouTubeScraper

        account = str(row.get("account") or "")
        scraper = YouTubeScraper()
        comments: list[Any] = []
        upserted = 0
        fetch_failed = False
        fail_reason = ""
        observed_comment_ids: set[str] = set()
        comments_marked_missing = 0
        if max_comments > 0:
            try:
                comments = scraper.fetch_comments(
                    source_id,
                    max_comments=max_comments,
                    fetch_replies=fetch_replies,
                    delay=0.5,
                )
                _trim_nested_comment_replies(comments, max_replies_per_post=max_comments if fetch_replies else 0)
                fail_reason = str(getattr(scraper, "last_comment_fetch_reason", "") or "")
            except Exception:
                fetch_failed = True
                fail_reason = str(getattr(scraper, "last_comment_fetch_reason", "") or "fetch_exception")
                logger.exception("[youtube] refresh_post_comments failed for video_id=%s", source_id)
        else:
            fail_reason = "fetch_disabled"

        with pg.db_connection() as conn:
            for comment in comments:
                upserted += _upsert_youtube_comment_tree(
                    context,
                    job_id=None,
                    run_id=None,
                    account=account,
                    video_db_id=str(row["id"]),
                    comment=comment,
                    observed_comment_ids=observed_comment_ids,
                    conn=conn,
                )
            is_complete = _is_comment_fetch_complete(
                fetch_failed=fetch_failed,
                fail_reason=fail_reason,
                auth_failed=bool(getattr(scraper, "comments_auth_failed", False)),
                fetched_count=len(comments),
                max_comments_per_post=max_comments,
            )
            incomplete_reason = None if is_complete else (fail_reason or "incomplete_or_capped")
            if is_complete and max_comments > 0:
                comments_marked_missing = _mark_missing_comments_for_anchor(
                    platform="youtube",
                    anchor_id=str(row["id"]),
                    observed_comment_ids=observed_comment_ids,
                    conn=conn,
                )
                _reconcile_post_comment_count(
                    platform="youtube",
                    post_db_id=str(row["id"]),
                    conn=conn,
                )
            comment_count_synced = _sync_youtube_video_comment_counts([str(row["id"])], conn=conn)
        total_comments = _count_stored_comments([str(row["id"])], "youtube").get(str(row["id"]), 0)
        return {
            "platform": normalized_platform,
            "source_id": source_id,
            "comments_fetched": len(comments),
            "comments_upserted": upserted,
            "total_comments_in_db": total_comments,
            "fetch_replies": fetch_replies,
            "max_comments_per_post": max_comments,
            "comment_fetch_reason": fail_reason,
            "comments_auth_failed": bool(getattr(scraper, "comments_auth_failed", False)),
            "fetch_failed": fetch_failed,
            "is_complete": is_complete,
            "comments_marked_missing": comments_marked_missing,
            "incomplete_reason": incomplete_reason,
            "comment_fail_reasons": [fail_reason] if fail_reason else [],
            "youtube_comment_count_synced": comment_count_synced,
        }

    if normalized_platform == "facebook":
        row = pg.fetch_one(
            """
            select p.id::text as id,
                   coalesce(nullif(p.source_account, ''), nullif(p.username, ''), '') as account
            from social.facebook_posts p
            where p.season_id = %s and p.post_id = %s
            """,
            [season_id, source_id],
        )
        if not row:
            raise ValueError("Post not found")
        from trr_backend.socials.facebook import FacebookScraper

        account = str(row.get("account") or "")
        scraper = FacebookScraper(cookies=_load_facebook_cookies())
        comments: list[Any] = []
        upserted = 0
        fetch_failed = False
        fail_reason = ""
        observed_comment_ids: set[str] = set()
        comments_marked_missing = 0
        if max_comments > 0:
            try:
                comments = scraper.fetch_comments(
                    f"https://www.facebook.com/reel/{source_id}",
                    max_comments=max_comments,
                    fetch_replies=fetch_replies,
                    delay_seconds=1.0,
                )
                fail_reason = str(getattr(scraper, "last_comment_fetch_reason", "") or "")
            except Exception:
                fetch_failed = True
                fail_reason = str(getattr(scraper, "last_comment_fetch_reason", "") or "fetch_exception")
                logger.exception("[facebook] refresh_post_comments failed for post_id=%s", source_id)
        else:
            fail_reason = "fetch_disabled"

        with pg.db_connection() as conn:
            for comment in comments:
                upserted += _upsert_facebook_comment_tree(
                    context,
                    job_id=None,
                    run_id=None,
                    account=account,
                    post_id=str(row["id"]),
                    comment=comment,
                    observed_comment_ids=observed_comment_ids,
                    conn=conn,
                )
            is_complete = _is_comment_fetch_complete(
                fetch_failed=fetch_failed,
                fail_reason=fail_reason,
                auth_failed=bool(getattr(scraper, "comments_auth_failed", False)),
                fetched_count=len(comments),
                max_comments_per_post=max_comments,
            )
            incomplete_reason = None if is_complete else (fail_reason or "incomplete_or_capped")
            if is_complete and max_comments > 0:
                comments_marked_missing = _mark_missing_comments_for_anchor(
                    platform="facebook",
                    anchor_id=str(row["id"]),
                    observed_comment_ids=observed_comment_ids,
                    conn=conn,
                )
                _reconcile_post_comment_count(
                    platform="facebook",
                    post_db_id=str(row["id"]),
                    conn=conn,
                )
        total_comments = _count_stored_comments([str(row["id"])], "facebook").get(str(row["id"]), 0)
        return {
            "platform": normalized_platform,
            "source_id": source_id,
            "comments_fetched": len(comments),
            "comments_upserted": upserted,
            "total_comments_in_db": total_comments,
            "fetch_replies": fetch_replies,
            "max_comments_per_post": max_comments,
            "comment_fetch_reason": fail_reason,
            "comments_auth_failed": bool(getattr(scraper, "comments_auth_failed", False)),
            "fetch_failed": fetch_failed,
            "is_complete": is_complete,
            "comments_marked_missing": comments_marked_missing,
            "incomplete_reason": incomplete_reason,
            "comment_fail_reasons": [fail_reason] if fail_reason else [],
        }

    if normalized_platform == "threads":
        row = pg.fetch_one(
            """
            select p.id::text as id,
                   coalesce(nullif(p.source_account, ''), nullif(p.username, ''), '') as account
            from social.meta_threads_posts p
            where p.season_id = %s and p.post_id = %s
            """,
            [season_id, source_id],
        )
        if not row:
            raise ValueError("Post not found")
        from trr_backend.socials.threads import ThreadsScraper

        account = str(row.get("account") or "")
        scraper = ThreadsScraper(cookies=_load_threads_cookies())
        comments: list[Any] = []
        upserted = 0
        fetch_failed = False
        fail_reason = ""
        observed_comment_ids: set[str] = set()
        comments_marked_missing = 0
        if max_comments > 0:
            try:
                comments = scraper.fetch_comments(
                    f"https://www.threads.com/@{account}/post/{source_id}",
                    max_comments=max_comments,
                    fetch_replies=fetch_replies,
                    delay_seconds=1.0,
                )
                fail_reason = str(getattr(scraper, "last_comment_fetch_reason", "") or "")
            except Exception:
                fetch_failed = True
                fail_reason = str(getattr(scraper, "last_comment_fetch_reason", "") or "fetch_exception")
                logger.exception("[threads] refresh_post_comments failed for post_id=%s", source_id)
        else:
            fail_reason = "fetch_disabled"

        with pg.db_connection() as conn:
            for comment in comments:
                upserted += _upsert_meta_threads_comment_tree(
                    context,
                    job_id=None,
                    run_id=None,
                    account=account,
                    post_id=str(row["id"]),
                    comment=comment,
                    observed_comment_ids=observed_comment_ids,
                    conn=conn,
                )
            is_complete = _is_comment_fetch_complete(
                fetch_failed=fetch_failed,
                fail_reason=fail_reason,
                auth_failed=bool(getattr(scraper, "comments_auth_failed", False)),
                fetched_count=len(comments),
                max_comments_per_post=max_comments,
            )
            incomplete_reason = None if is_complete else (fail_reason or "incomplete_or_capped")
            if is_complete and max_comments > 0:
                comments_marked_missing = _mark_missing_comments_for_anchor(
                    platform="threads",
                    anchor_id=str(row["id"]),
                    observed_comment_ids=observed_comment_ids,
                    conn=conn,
                )
                _reconcile_post_comment_count(
                    platform="threads",
                    post_db_id=str(row["id"]),
                    conn=conn,
                )
        total_comments = _count_stored_comments([str(row["id"])], "threads").get(str(row["id"]), 0)
        return {
            "platform": normalized_platform,
            "source_id": source_id,
            "comments_fetched": len(comments),
            "comments_upserted": upserted,
            "total_comments_in_db": total_comments,
            "fetch_replies": fetch_replies,
            "max_comments_per_post": max_comments,
            "comment_fetch_reason": fail_reason,
            "comments_auth_failed": bool(getattr(scraper, "comments_auth_failed", False)),
            "fetch_failed": fetch_failed,
            "is_complete": is_complete,
            "comments_marked_missing": comments_marked_missing,
            "incomplete_reason": incomplete_reason,
            "comment_fail_reasons": [fail_reason] if fail_reason else [],
        }

    row = pg.fetch_one(
        """
        select t.tweet_id,
               t.replies_count,
               t.quotes,
               coalesce(nullif(t.source_account, ''), nullif(t.username, ''), '') as account
        from social.twitter_tweets t
        where t.season_id = %s and t.tweet_id = %s and t.is_reply = false
        """,
        [season_id, source_id],
    )
    if not row:
        raise ValueError("Post not found")

    from trr_backend.socials.twitter import TwitterScraper

    account = str(row.get("account") or "")
    twitter_cookies, twitter_bearer = _load_twitter_auth()
    twikit_creds = _load_twikit_credentials()
    if not twitter_cookies.get("ct0") and twikit_creds:
        if twikit_creds.get("auth_token") and twikit_creds.get("ct0"):
            twitter_cookies = {**twitter_cookies, "auth_token": twikit_creds["auth_token"], "ct0": twikit_creds["ct0"]}
    has_session_artifacts = bool(
        twitter_cookies.get("auth_token") and twitter_cookies.get("ct0")
    ) or bool(twikit_creds and twikit_creds.get("auth_token") and twikit_creds.get("ct0"))

    scraper = TwitterScraper(cookies=twitter_cookies, bearer_token=twitter_bearer, twikit_credentials=twikit_creds)
    replies: list[Any] = []
    quotes: list[Any] = []
    fetch_failed = False
    quote_fetch_failed = False
    fail_reason = ""
    quote_fail_reason = ""
    observed_reply_ids: set[str] = set()
    comments_marked_missing = 0
    if fetch_replies and max_comments > 0:
        try:
            replies = scraper.fetch_tweet_replies(source_id, delay=0.5)[:max_comments]
            fail_reason = str(getattr(scraper, "last_reply_fetch_reason", "") or "")
        except Exception:
            fetch_failed = True
            fail_reason = str(getattr(scraper, "last_reply_fetch_reason", "") or "fetch_exception")
            logger.exception("[twitter] refresh_post_comments failed for tweet_id=%s", source_id)
        try:
            quotes = scraper.fetch_tweet_quotes(source_id, delay=0.5, max_pages=5)[:max_comments]
            quote_fail_reason = str(getattr(scraper, "last_quote_fetch_reason", "") or "")
            if quote_fail_reason and not quotes:
                quote_fetch_failed = True
        except Exception:
            quote_fetch_failed = True
            quote_fail_reason = "fetch_exception"
            logger.exception("[twitter] refresh_post_comments failed to fetch quotes for tweet_id=%s", source_id)
    else:
        fail_reason = "fetch_disabled"

    upserted = 0
    quotes_upserted = 0
    with pg.db_connection() as conn:
        for reply in replies:
            if not getattr(reply, "reply_to_tweet_id", None):
                reply.reply_to_tweet_id = source_id
            reply.is_reply = True
            reply_id = str(getattr(reply, "tweet_id", "") or "")
            if reply_id:
                observed_reply_ids.add(reply_id)
            if _upsert_tweet(
                context,
                job_id=None,
                run_id=None,
                account=account,
                tweet=reply,
                conn=conn,
            ):
                upserted += 1
        for quote in quotes:
            quote.is_reply = False
            quote.reply_to_tweet_id = None
            quote.is_quote = True
            if not getattr(quote, "quoted_tweet_id", None):
                quote.quoted_tweet_id = source_id
            if str(getattr(quote, "quoted_tweet_id", "") or "") != source_id:
                continue
            if _upsert_tweet(
                context,
                job_id=None,
                run_id=None,
                account=account,
                tweet=quote,
                persist_stats=None,
                conn=conn,
            ):
                quotes_upserted += 1
        inferred_auth_failed = (
            (not has_session_artifacts)
            and (
                str(fail_reason).startswith("http_404")
                or str(quote_fail_reason).startswith("http_404")
            )
        )
        auth_failed = bool(getattr(scraper, "comments_auth_failed", False)) or inferred_auth_failed
        is_complete = _is_comment_fetch_complete(
            fetch_failed=fetch_failed,
            fail_reason=fail_reason,
            auth_failed=auth_failed,
            fetched_count=(len(replies) + len(quotes)),
            max_comments_per_post=max_comments,
            expected_count=(
                _normalize_non_negative_int(row.get("replies_count")) + _normalize_non_negative_int(row.get("quotes"))
            ),
        )
        incomplete_reason = None if is_complete else (fail_reason or "incomplete_or_capped")
        if is_complete and max_comments > 0:
            comments_marked_missing = _mark_missing_comments_for_anchor(
                platform="twitter",
                anchor_id=source_id,
                observed_comment_ids=observed_reply_ids,
                conn=conn,
            )
            _reconcile_post_comment_count(
                platform="twitter",
                post_db_id=source_id,
                conn=conn,
            )
    total_comments = _count_stored_replies([source_id]).get(source_id, 0)
    total_quotes = _count_stored_quotes([source_id]).get(source_id, 0)
    return {
        "platform": normalized_platform,
        "source_id": source_id,
        "comments_fetched": len(replies),
        "comments_upserted": upserted,
        "quotes_fetched": len(quotes),
        "quotes_upserted": quotes_upserted,
        "total_comments_in_db": total_comments,
        "total_quotes_in_db": total_quotes,
        "fetch_replies": fetch_replies,
        "max_comments_per_post": max_comments,
        "comment_fetch_reason": fail_reason,
        "quote_fetch_reason": quote_fail_reason or None,
        "comments_auth_failed": auth_failed,
        "auth_session_artifacts_present": has_session_artifacts,
        "fetch_failed": fetch_failed,
        "quote_fetch_failed": quote_fetch_failed,
        "is_complete": is_complete,
        "comments_marked_missing": comments_marked_missing,
        "incomplete_reason": incomplete_reason,
        "comment_fail_reasons": [fail_reason] if fail_reason else [],
    }


def build_csv(snapshot: dict[str, Any]) -> str:
    rows = snapshot.get("rows") or []
    summary = snapshot.get("summary") or {}
    data_quality = summary.get("data_quality") if isinstance(summary.get("data_quality"), dict) else {}
    weekly_flags = snapshot.get("weekly_flags") if isinstance(snapshot.get("weekly_flags"), list) else []
    benchmark = snapshot.get("benchmark") if isinstance(snapshot.get("benchmark"), dict) else {}
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(
        [
            "week_index",
            "platform",
            "kind",
            "source_id",
            "timestamp",
            "author",
            "url",
            "engagement",
            "sentiment",
            "text",
        ]
    )
    for row in rows:
        writer.writerow(
            [
                row.get("week_index"),
                row.get("platform"),
                row.get("kind"),
                row.get("source_id"),
                row.get("timestamp"),
                row.get("author"),
                row.get("url"),
                row.get("engagement"),
                row.get("sentiment"),
                row.get("text"),
            ]
        )
    writer.writerow([])
    writer.writerow(["section", "key", "value"])

    if data_quality:
        writer.writerow(["data_quality", "comments_saved_pct_overall", data_quality.get("comments_saved_pct_overall")])
        writer.writerow(["data_quality", "data_freshness_minutes", data_quality.get("data_freshness_minutes")])
        writer.writerow(["data_quality", "last_post_at", data_quality.get("last_post_at")])
        writer.writerow(["data_quality", "last_comment_at", data_quality.get("last_comment_at")])

    for flag in weekly_flags:
        if not isinstance(flag, dict):
            continue
        writer.writerow(
            [
                "weekly_flags",
                f"week_{flag.get('week_index')}_{flag.get('code')}",
                flag.get("message"),
            ]
        )

    if benchmark:
        current = benchmark.get("current") if isinstance(benchmark.get("current"), dict) else {}
        prev = benchmark.get("previous_week") if isinstance(benchmark.get("previous_week"), dict) else {}
        trailing = (
            benchmark.get("trailing_3_week_avg") if isinstance(benchmark.get("trailing_3_week_avg"), dict) else {}
        )
        writer.writerow(["benchmark", "week_index", benchmark.get("week_index")])
        writer.writerow(["benchmark", "current_posts", current.get("posts")])
        writer.writerow(["benchmark", "current_comments", current.get("comments")])
        writer.writerow(["benchmark", "current_engagement", current.get("engagement")])
        writer.writerow(["benchmark", "previous_week_index", prev.get("week_index")])
        writer.writerow(["benchmark", "trailing_window_size", trailing.get("window_size")])
    return output.getvalue()


def build_pdf(snapshot: dict[str, Any]) -> bytes:
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import LETTER
        from reportlab.lib.styles import getSampleStyleSheet
        from reportlab.lib.units import inch
        from reportlab.platypus import PageBreak, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("PDF export requires reportlab") from exc

    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=LETTER,
        leftMargin=0.6 * inch,
        rightMargin=0.6 * inch,
        topMargin=0.6 * inch,
        bottomMargin=0.6 * inch,
    )
    styles = getSampleStyleSheet()

    summary = snapshot.get("summary") or {}
    window = snapshot.get("window") or {}
    weekly = snapshot.get("weekly") or []
    platform_breakdown = snapshot.get("platform_breakdown") or []
    themes = snapshot.get("themes") or {}
    discussions = (snapshot.get("leaderboards") or {}).get("viewer_discussion") or []
    data_quality = summary.get("data_quality") if isinstance(summary.get("data_quality"), dict) else {}
    weekly_flags = snapshot.get("weekly_flags") if isinstance(snapshot.get("weekly_flags"), list) else []
    benchmark = snapshot.get("benchmark") if isinstance(snapshot.get("benchmark"), dict) else {}

    story = []
    story.append(Paragraph("Season Social Analytics Report", styles["Title"]))
    story.append(
        Paragraph(
            (
                f"Show: {summary.get('show_name') or 'Unknown'} | "
                f"Season: {summary.get('season_number') or 'N/A'} | "
                f"Window: {window.get('start')} to {window.get('end')} | "
                f"Timezone: {window.get('timezone') or 'America/New_York'}"
            ),
            styles["Normal"],
        )
    )
    story.append(Spacer(1, 0.2 * inch))

    summary_table = Table(
        [
            ["Metric", "Value"],
            ["Total Posts", str(summary.get("total_posts") or 0)],
            ["Total Comments", str(summary.get("total_comments") or 0)],
            ["Total Engagement", str(summary.get("total_engagement") or 0)],
            [
                "Sentiment Mix",
                (
                    f"P {((summary.get('sentiment_mix') or {}).get('positive') or 0):.1%} | "
                    f"N {((summary.get('sentiment_mix') or {}).get('neutral') or 0):.1%} | "
                    f"Neg {((summary.get('sentiment_mix') or {}).get('negative') or 0):.1%}"
                ),
            ],
        ],
        hAlign="LEFT",
        colWidths=[2.1 * inch, 4.6 * inch],
    )
    summary_table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#111827")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#d4d4d8")),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("ALIGN", (0, 0), (-1, -1), "LEFT"),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ]
        )
    )
    story.append(summary_table)
    story.append(Spacer(1, 0.2 * inch))

    if data_quality:
        story.append(Paragraph("Data Quality", styles["Heading2"]))
        quality_rows = [
            ["Metric", "Value"],
            ["Coverage (Overall)", str(data_quality.get("comments_saved_pct_overall"))],
            ["Freshness (Minutes)", str(data_quality.get("data_freshness_minutes"))],
            ["Last Post", str(data_quality.get("last_post_at") or "-")],
            ["Last Comment", str(data_quality.get("last_comment_at") or "-")],
        ]
        quality_table = Table(quality_rows, hAlign="LEFT", colWidths=[2.2 * inch, 4.5 * inch])
        quality_table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#3f3f46")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                    ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#e4e4e7")),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ]
            )
        )
        story.append(quality_table)
        story.append(Spacer(1, 0.2 * inch))

    if weekly:
        story.append(Paragraph("Weekly Trend", styles["Heading2"]))
        weekly_table_data = [["Week", "Posts", "Comments", "Engagement", "Positive", "Neutral", "Negative"]]
        for row in weekly[:16]:
            sentiment = row.get("sentiment") or {}
            weekly_table_data.append(
                [
                    row.get("label"),
                    row.get("post_volume"),
                    row.get("comment_volume"),
                    row.get("engagement"),
                    sentiment.get("positive", 0),
                    sentiment.get("neutral", 0),
                    sentiment.get("negative", 0),
                ]
            )
        weekly_table = Table(weekly_table_data, hAlign="LEFT")
        weekly_table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#334155")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                    ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#e4e4e7")),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ]
            )
        )
        story.append(weekly_table)
        story.append(Spacer(1, 0.2 * inch))

    if weekly_flags:
        story.append(Paragraph("Weekly Flags", styles["Heading2"]))
        flags_data = [["Week", "Code", "Severity", "Message"]]
        for flag in weekly_flags[:40]:
            if not isinstance(flag, dict):
                continue
            flags_data.append(
                [
                    str(flag.get("week_index")),
                    str(flag.get("code") or ""),
                    str(flag.get("severity") or ""),
                    str(flag.get("message") or ""),
                ]
            )
        flags_table = Table(flags_data, hAlign="LEFT")
        flags_table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#52525b")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                    ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#e4e4e7")),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ]
            )
        )
        story.append(flags_table)
        story.append(Spacer(1, 0.2 * inch))

    if benchmark:
        story.append(Paragraph("Benchmark", styles["Heading2"]))
        benchmark_rows = [
            ["Metric", "Value"],
            ["Week Index", str(benchmark.get("week_index"))],
            ["Current Posts", str((benchmark.get("current") or {}).get("posts"))],
            ["Current Comments", str((benchmark.get("current") or {}).get("comments"))],
            ["Current Engagement", str((benchmark.get("current") or {}).get("engagement"))],
            ["Prev Week Index", str((benchmark.get("previous_week") or {}).get("week_index"))],
            ["Trailing Window", str((benchmark.get("trailing_3_week_avg") or {}).get("window_size"))],
        ]
        benchmark_table = Table(benchmark_rows, hAlign="LEFT", colWidths=[2.2 * inch, 4.5 * inch])
        benchmark_table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1f2937")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                    ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#e4e4e7")),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ]
            )
        )
        story.append(benchmark_table)
        story.append(Spacer(1, 0.2 * inch))

    if platform_breakdown:
        story.append(Paragraph("Platform Breakdown", styles["Heading2"]))
        platform_table_data = [["Platform", "Posts", "Comments", "Engagement", "Positive", "Neutral", "Negative"]]
        for row in platform_breakdown:
            sentiment = row.get("sentiment") or {}
            platform_table_data.append(
                [
                    row.get("platform"),
                    row.get("posts"),
                    row.get("comments"),
                    row.get("engagement"),
                    sentiment.get("positive", 0),
                    sentiment.get("neutral", 0),
                    sentiment.get("negative", 0),
                ]
            )
        platform_table = Table(platform_table_data, hAlign="LEFT")
        platform_table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1d4ed8")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                    ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#e4e4e7")),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ]
            )
        )
        story.append(platform_table)

    story.append(PageBreak())
    story.append(Paragraph("Appendix", styles["Title"]))

    positive_themes = themes.get("positive") or []
    negative_themes = themes.get("negative") or []

    story.append(Paragraph("Top Positive Drivers", styles["Heading2"]))
    if positive_themes:
        for theme in positive_themes[:10]:
            story.append(
                Paragraph(
                    f"{theme.get('term')} (count {theme.get('count')}, score {theme.get('score')})",
                    styles["Normal"],
                )
            )
    else:
        story.append(Paragraph("No positive drivers identified.", styles["Normal"]))

    story.append(Spacer(1, 0.15 * inch))
    story.append(Paragraph("Top Negative Drivers", styles["Heading2"]))
    if negative_themes:
        for theme in negative_themes[:10]:
            story.append(
                Paragraph(
                    f"{theme.get('term')} (count {theme.get('count')}, score {theme.get('score')})",
                    styles["Normal"],
                )
            )
    else:
        story.append(Paragraph("No negative drivers identified.", styles["Normal"]))

    story.append(Spacer(1, 0.2 * inch))
    story.append(Paragraph("Viewer Discussion Highlights", styles["Heading2"]))
    discussion_table_data = [["Platform", "Sentiment", "Engagement", "Comment Excerpt"]]
    for row in discussions[:20]:
        text = str(row.get("text") or "").strip()
        excerpt = text if len(text) <= 140 else f"{text[:137]}..."
        discussion_table_data.append(
            [
                row.get("platform"),
                row.get("sentiment"),
                row.get("engagement"),
                excerpt,
            ]
        )

    if len(discussion_table_data) == 1:
        discussion_table_data.append(["-", "-", "-", "No comments available in this filter window."])

    discussion_table = Table(
        discussion_table_data,
        hAlign="LEFT",
        colWidths=[0.9 * inch, 0.9 * inch, 0.9 * inch, 4.9 * inch],
    )
    discussion_table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#0f766e")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#e4e4e7")),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ]
        )
    )
    story.append(discussion_table)

    doc.build(story)
    return buffer.getvalue()


def pdf_filename(show_id: str, season_number: int, generated_at: datetime | None = None) -> str:
    ts = (generated_at or _now_utc()).strftime("%Y%m%d")
    return f"social_report_{show_id}_s{season_number}_{ts}.pdf"
