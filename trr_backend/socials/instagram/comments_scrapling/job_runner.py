from __future__ import annotations

import asyncio
import logging
import os
import time
from collections import Counter
from contextlib import suppress
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from trr_backend.db import pg
from trr_backend.socials.instagram.comments_scrapling.fetcher import (
    InstagramCommentsFetchResult,
    InstagramCommentsScraplingFetcher,
    InstagramCommentsWarmupError,
)
from trr_backend.socials.instagram.comments_scrapling.persistence import persist_instagram_comments_for_post
from trr_backend.socials.instagram.comments_scrapling.proxy import select_comments_proxy
from trr_backend.socials.instagram.comments_scrapling.session import resolve_comments_scrapling_session
from trr_backend.socials.instagram.scraper import InstagramComment

logger = logging.getLogger("socials.instagram.comments_scrapling.job_runner")

_RUN_FATAL_COMMENTS_ERROR_CODES = {
    "instagram_comments_auth_failed",
    "instagram_comments_warmup_auth_failed",
    "instagram_comments_warmup_no_cookies",
    "instagram_comments_warmup_transport_error",
    "instagram_comments_cookie_bridge_failed",
    "instagram_comments_proxy_bridge_failed",
}
_QUEUE_DEFAULT_ATTEMPT_COUNT = 1
_QUEUE_DEFAULT_MAX_ATTEMPTS = 12
_DEFAULT_CANCEL_CHECK_EVERY_POSTS = 5
_DEFAULT_JOB_HEARTBEAT_INTERVAL_SECONDS = 30
# Phase 1.5: mid-run warmup refresh defaults. Triggered when the runner sees
# >= _DEFAULT_MID_RUN_WARMUP_AUTH_THRESHOLD consecutive auth-failed posts OR
# every _DEFAULT_MID_RUN_WARMUP_EVERY_POSTS successful posts (whichever first).
# Both are env-overridable through the job config so tests can flip them.
_DEFAULT_MID_RUN_WARMUP_AUTH_THRESHOLD = 3
_DEFAULT_MID_RUN_WARMUP_EVERY_POSTS = 50
# Phase 1.4: incomplete-run raise threshold. The shard only raises
# instagram_comments_incomplete_retryable when at least this fraction of the
# target list is incomplete, so a single bad post does not force the whole
# shard into retrying.
_DEFAULT_INCOMPLETE_RAISE_RATIO = 4  # raise when ratio is >= 1/_RAISE_RATIO
_MIN_INCOMPLETE_RAISE_TARGETS = 1
# Phase 1.7: cap on per-comment failure entries persisted in
# social.scrape_jobs.metadata.comment_failures so a runaway shard cannot bloat
# the job-metadata column.
_COMMENT_FAILURE_METADATA_MAX_ENTRIES = 200
_RECONCILABLE_REPORTED_GAP_MAX_DEFAULT = 2
_RECONCILABLE_REPORTED_GAP_RATIO_DEFAULT = 0.02
_RECONCILABLE_REPORTED_GAP_REASONS = {
    "hidden_comments_unavailable_reconciled",
    "hidden_comments_unresolved",
    "reply_tail_budget_exhausted",
    "reply_tail_incomplete",
}
_REPLY_ONLY_RETRY_REASONS = {
    "http_429",
    "reply_tail_budget_exhausted",
    "reply_tail_incomplete",
}
_TERMINAL_COVERAGE_GAP_MAX_DEFAULT = 150
_TERMINAL_COVERAGE_GAP_RATIO_DEFAULT = 0.10
_TERMINAL_COVERAGE_GAP_REASONS = {
    "http_429",
    "http_500",
    "http_502",
    "http_503",
    "http_504",
    "pagination_deadline_exceeded",
    "pagination_page_cap_reached",
    "pagination_repeated_cursor",
    "transport_error",
    "transport_timeout",
}
_INCOMPLETE_RETRY_STALL_ATTEMPTS_DEFAULT = 3
_INCOMPLETE_RETRY_STALL_REASONS = {
    "hidden_comments_unresolved",
    "hidden_comments_unavailable",
    "hidden_comments_blocked",
}


@dataclass(slots=True)
class CommentsScraplingRuntimeError(Exception):
    message: str
    error_code: str
    retryable: bool = False
    runtime_metadata: dict[str, Any] | None = None

    def __str__(self) -> str:
        return self.message


@dataclass(slots=True)
class ScraplingJobCancelledError(Exception):
    message: str
    cancel_scope: str
    job_status: str | None = None
    run_status: str | None = None
    runtime_metadata: dict[str, Any] | None = None

    def __str__(self) -> str:
        return self.message


ScraplingJobCancelled = ScraplingJobCancelledError


@dataclass(slots=True)
class ScraplingJobLeaseLostError(Exception):
    message: str
    job_status: str | None = None
    job_worker_id: str | None = None
    claimed_at: Any | None = None
    runtime_metadata: dict[str, Any] | None = None

    def __str__(self) -> str:
        return self.message


def _worker_id_matches_claim(row_worker_id: Any, current_worker_id: str | None) -> bool:
    row_value = str(row_worker_id or "").strip()
    current_value = str(current_worker_id or "").strip()
    if not current_value:
        return True
    if not row_value:
        return False
    return row_value == current_value or current_value.startswith(f"{row_value}:")


def _raise_if_job_lease_lost(
    *,
    job_id: str,
    worker_id: str | None,
    runtime_metadata: dict[str, Any] | None = None,
    conn: Any | None = None,
) -> None:
    if not job_id:
        return
    try:
        job_state = (
            pg.fetch_one(
                """
                select status, worker_id, claimed_at
                from social.scrape_jobs
                where id = %s
                """,
                [job_id],
                conn=conn,
            )
            or {}
        )
    except pg.DatabaseServiceUnavailableError as exc:
        logger.warning(
            "Skipping comments lease check after database saturation: job_id=%s error=%s",
            job_id,
            exc,
        )
        return
    job_status = str(job_state.get("status") or "").strip().lower() or None
    job_worker_id = str(job_state.get("worker_id") or "").strip() or None
    claimed_at = job_state.get("claimed_at")
    if job_status == "running" and claimed_at is not None and _worker_id_matches_claim(job_worker_id, worker_id):
        return
    metadata = dict(runtime_metadata or {})
    raise ScraplingJobLeaseLostError(
        "Instagram comments Scrapling job lost its active queue lease.",
        job_status=job_status,
        job_worker_id=job_worker_id,
        claimed_at=claimed_at,
        runtime_metadata=metadata,
    )


def _raise_if_cancelled(
    *,
    job_id: str,
    run_id: str,
    runtime_metadata: dict[str, Any] | None = None,
    conn: Any | None = None,
) -> None:
    if not job_id:
        return
    started_at = time.perf_counter()
    try:
        job_state = pg.fetch_one("select status from social.scrape_jobs where id = %s", [job_id], conn=conn) or {}
    except pg.DatabaseServiceUnavailableError as exc:
        logger.warning(
            "Skipping comments cancellation check after database saturation: job_id=%s error=%s",
            job_id,
            exc,
        )
        return
    job_status = str(job_state.get("status") or "").strip().lower() or None
    run_status: str | None = None
    if run_id:
        try:
            run_state = pg.fetch_one("select status from social.scrape_runs where id = %s", [run_id], conn=conn) or {}
        except pg.DatabaseServiceUnavailableError as exc:
            logger.warning(
                "Skipping comments run cancellation check after database saturation: run_id=%s error=%s",
                run_id,
                exc,
            )
            return
        run_status = str(run_state.get("status") or "").strip().lower() or None
    cancel_scope = "job" if job_status == "cancelled" else "run" if run_status == "cancelled" else None
    if not cancel_scope:
        return

    metadata = dict(runtime_metadata or {})
    logger.info(
        "instagram_comments_scrapling cancellation_detected",
        extra={
            "event": "scrapling_job_cancelled",
            "job_id": job_id,
            "run_id": run_id or None,
            "cancel_scope": cancel_scope,
            "job_status": job_status,
            "run_status": run_status,
            "check_latency_ms": round((time.perf_counter() - started_at) * 1000, 2),
            "request_count": metadata.get("request_count"),
            "warmup_cookie_count": metadata.get("warmup_cookie_count"),
        },
    )
    raise ScraplingJobCancelledError(
        "Instagram comments Scrapling job was cancelled.",
        cancel_scope=cancel_scope,
        job_status=job_status,
        run_status=run_status,
        runtime_metadata=metadata,
    )


def _resolve_job_heartbeat_interval_seconds() -> int:
    raw = (
        os.environ.get("SOCIAL_INSTAGRAM_COMMENTS_JOB_HEARTBEAT_INTERVAL_SEC")
        or os.environ.get("SOCIAL_JOB_HEARTBEAT_INTERVAL_SEC")
        or _DEFAULT_JOB_HEARTBEAT_INTERVAL_SECONDS
    )
    try:
        value = int(raw)
    except (TypeError, ValueError):
        value = _DEFAULT_JOB_HEARTBEAT_INTERVAL_SECONDS
    return max(5, min(value, 300))


async def _maintain_comments_job_heartbeat(
    *,
    job_id: str,
    worker_id: str | None,
    interval_seconds: int,
) -> None:
    from trr_backend.repositories import social_season_analytics as repo

    if not job_id:
        return
    while True:
        try:
            repo._touch_job_heartbeat(job_id, worker_id=worker_id)
        except pg.DatabaseServiceUnavailableError as exc:
            logger.warning(
                "Skipping comments job heartbeat after database saturation: job_id=%s error=%s",
                job_id,
                exc,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Comments job heartbeat failed: job_id=%s error=%s", job_id, exc)
        await asyncio.sleep(interval_seconds)


def _comments_scrape_is_complete(
    *,
    result: InstagramCommentsFetchResult,
    max_comments_per_post: int,
) -> bool:
    if result.fetch_failed or result.auth_failed:
        return False
    reported_comment_count = _extract_reported_comment_count(result)
    observed_comment_count = _extract_observed_comment_count(result)
    if reported_comment_count is not None and observed_comment_count >= reported_comment_count:
        return True
    if str(getattr(result, "fetch_reason", "") or "") == "hidden_comments_unavailable_reconciled":
        return observed_comment_count > 0
    if reported_comment_count is not None and observed_comment_count < reported_comment_count:
        if max_comments_per_post <= 0 or reported_comment_count <= max_comments_per_post:
            return False
    if max_comments_per_post <= 0:
        return True
    return len(result.comments) < max_comments_per_post


def _persisted_comment_coverage_is_complete(
    *,
    result: InstagramCommentsFetchResult,
    stored_total_comments: int,
    max_comments_per_post: int,
) -> bool:
    if result.auth_failed:
        return False
    if not bool(getattr(result, "retryable", False)):
        return False
    reported_comment_count = _extract_reported_comment_count(result)
    if reported_comment_count is None:
        return False
    target_count = reported_comment_count
    if max_comments_per_post > 0:
        target_count = min(target_count, max_comments_per_post)
    return int(stored_total_comments or 0) >= target_count


def _reported_count_gap_is_tolerable(*, unresolved_gap: int, target_count: int) -> bool:
    gap = max(0, int(unresolved_gap or 0))
    target = max(0, int(target_count or 0))
    if gap <= 0:
        return True
    if target <= 0:
        return False
    try:
        max_absolute_gap = int(
            os.environ.get("SOCIAL_INSTAGRAM_COMMENTS_RECONCILABLE_GAP_MAX")
            or os.environ.get("SOCIAL_INSTAGRAM_COMMENTS_HIDDEN_UNAVAILABLE_GAP_MAX")
            or _RECONCILABLE_REPORTED_GAP_MAX_DEFAULT
        )
    except (TypeError, ValueError):
        max_absolute_gap = _RECONCILABLE_REPORTED_GAP_MAX_DEFAULT
    max_absolute_gap = max(0, min(max_absolute_gap, 50))
    try:
        max_ratio = float(
            os.environ.get("SOCIAL_INSTAGRAM_COMMENTS_RECONCILABLE_GAP_RATIO")
            or os.environ.get("SOCIAL_INSTAGRAM_COMMENTS_HIDDEN_UNAVAILABLE_GAP_RATIO")
            or _RECONCILABLE_REPORTED_GAP_RATIO_DEFAULT
        )
    except (TypeError, ValueError):
        max_ratio = _RECONCILABLE_REPORTED_GAP_RATIO_DEFAULT
    max_ratio = max(0.0, min(max_ratio, 0.25))
    ratio_gap = int(target * max_ratio)
    if ratio_gap < target * max_ratio:
        ratio_gap += 1
    return gap <= max(max_absolute_gap, ratio_gap)


def _persisted_comment_coverage_gap_is_reconcilable(
    *,
    result: InstagramCommentsFetchResult,
    stored_total_comments: int,
    max_comments_per_post: int,
) -> bool:
    if result.auth_failed:
        return False
    reason = str(getattr(result, "fetch_reason", "") or "").strip()
    if reason not in _RECONCILABLE_REPORTED_GAP_REASONS:
        return False
    reported_comment_count = _extract_reported_comment_count(result)
    if reported_comment_count is None:
        return False
    target_count = reported_comment_count
    if max_comments_per_post > 0:
        target_count = min(target_count, max_comments_per_post)
    stored_total = int(stored_total_comments or 0)
    if stored_total <= 0 or stored_total >= target_count:
        return False
    return _reported_count_gap_is_tolerable(
        unresolved_gap=target_count - stored_total,
        target_count=target_count,
    )


def _terminal_pagination_coverage_gap_is_reconcilable(
    *,
    result: InstagramCommentsFetchResult,
    stored_total_comments: int,
    max_comments_per_post: int,
) -> bool:
    if result.auth_failed:
        return False
    if not result.retryable:
        return False
    reason = str(getattr(result, "fetch_reason", "") or "").strip()
    if reason not in _TERMINAL_COVERAGE_GAP_REASONS:
        return False
    reported_comment_count = _extract_reported_comment_count(result)
    if reported_comment_count is None:
        return False
    target_count = reported_comment_count
    if max_comments_per_post > 0:
        target_count = min(target_count, max_comments_per_post)
    stored_total = int(stored_total_comments or 0)
    if stored_total <= 0 or stored_total >= target_count:
        return False
    unresolved_gap = target_count - stored_total
    try:
        max_absolute_gap = int(
            os.environ.get("SOCIAL_INSTAGRAM_COMMENTS_TERMINAL_GAP_MAX")
            or _TERMINAL_COVERAGE_GAP_MAX_DEFAULT
        )
    except (TypeError, ValueError):
        max_absolute_gap = _TERMINAL_COVERAGE_GAP_MAX_DEFAULT
    max_absolute_gap = max(0, min(max_absolute_gap, 500))
    try:
        max_ratio = float(
            os.environ.get("SOCIAL_INSTAGRAM_COMMENTS_TERMINAL_GAP_RATIO")
            or _TERMINAL_COVERAGE_GAP_RATIO_DEFAULT
        )
    except (TypeError, ValueError):
        max_ratio = _TERMINAL_COVERAGE_GAP_RATIO_DEFAULT
    max_ratio = max(0.0, min(max_ratio, 0.25))
    ratio_gap = int(target_count * max_ratio)
    if ratio_gap < target_count * max_ratio:
        ratio_gap += 1
    return unresolved_gap <= max(max_absolute_gap, ratio_gap)


def _safe_int(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def _job_attempt_state(job: dict[str, Any]) -> tuple[int, int]:
    attempt_count = _safe_int(job.get("attempt_count"))
    max_attempts = _safe_int(job.get("max_attempts"))
    normalized_attempt_count = max(1, attempt_count if attempt_count is not None else _QUEUE_DEFAULT_ATTEMPT_COUNT)
    normalized_max_attempts = max(1, max_attempts if max_attempts is not None else _QUEUE_DEFAULT_MAX_ATTEMPTS)
    # Phase 1.3 / audit: enforce max_attempts >= attempt_count + 1 when retryable
    # state is missing or malformed so transient transport blips actually retry.
    # Without this, a row carrying attempt_count = max_attempts (e.g. 1 == 1)
    # would short-circuit can_retry to False on the first transient failure.
    normalized_max_attempts = max(normalized_max_attempts, normalized_attempt_count + 1)
    return (normalized_attempt_count, normalized_max_attempts)


def _attach_incomplete_activity(
    activity: dict[str, Any],
    *,
    incomplete_target_source_ids: list[str],
    incomplete_fetch_reasons: dict[str, str],
    auth_failed_target_source_ids: list[str] | None = None,
    auth_failed_fetch_reasons: dict[str, str] | None = None,
) -> dict[str, Any]:
    incomplete_targets = list(
        dict.fromkeys(str(item or "").strip() for item in incomplete_target_source_ids if str(item or "").strip())
    )
    if incomplete_targets:
        activity["incomplete_target_source_ids"] = incomplete_targets
    if incomplete_fetch_reasons:
        activity["incomplete_fetch_reasons"] = dict(incomplete_fetch_reasons)
    auth_failed_targets = list(
        dict.fromkeys(
            str(item or "").strip() for item in (auth_failed_target_source_ids or []) if str(item or "").strip()
        )
    )
    if auth_failed_targets:
        activity["auth_failed_target_source_ids"] = auth_failed_targets
    if auth_failed_fetch_reasons:
        activity["auth_failed_fetch_reasons"] = dict(auth_failed_fetch_reasons)
    return activity


def _config_truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _is_database_capacity_error(exc: BaseException) -> bool:
    if isinstance(exc, pg.DatabaseServiceUnavailableError):
        return True
    message = str(exc).lower()
    return any(
        marker in message
        for marker in (
            "emaxconnsession",
            "max clients reached",
            "pool exhausted",
            "database pool",
            "session_pool_capacity",
        )
    )


def _is_comments_transport_error(exc: BaseException) -> bool:
    message = str(exc).lower()
    if not message:
        return False
    return any(
        marker in message
        for marker in (
            "wrong_version_number",
            "wrong version number",
            "ssl:",
            "ssl connection",
            "record layer failure",
            "closed unexpectedly",
            "transport error",
            "transporterror",
            "proxy error",
            "proxyerror",
            "connecterror",
            "readerror",
            "connection reset",
            "server disconnected",
            "remote protocol error",
            "network is unreachable",
            "temporarily unavailable",
        )
    )


def _extract_reported_comment_count(result: InstagramCommentsFetchResult) -> int | None:
    reported = getattr(result, "reported_comment_count", None)
    if reported is None:
        reported = getattr(result, "comments_count", None)
    if reported is not None:
        return _safe_int(reported)
    for attr_name in ("runtime_metadata", "raw_metadata"):
        metadata = getattr(result, attr_name, None)
        if not isinstance(metadata, dict):
            continue
        for key in ("reported_comment_count", "comments_count", "comment_count"):
            parsed = _safe_int(metadata.get(key))
            if parsed is not None:
                return parsed
    return None


def _extract_observed_comment_count(result: InstagramCommentsFetchResult) -> int:
    for attr_name in (
        "flattened_comment_count",
        "flattened_comments_count",
        "observed_comment_count",
        "observed_comments_count",
        "comments_observed_count",
        "total_comments_observed",
    ):
        parsed = _safe_int(getattr(result, attr_name, None))
        if parsed is not None:
            return parsed
    for attr_name in ("runtime_metadata", "raw_metadata", "metadata"):
        metadata = getattr(result, attr_name, None)
        if not isinstance(metadata, dict):
            continue
        for key in (
            "flattened_comment_count",
            "flattened_comments_count",
            "observed_comment_count",
            "observed_comments_count",
            "comments_observed_count",
            "total_comments_observed",
        ):
            parsed = _safe_int(metadata.get(key))
            if parsed is not None:
                return parsed
    return _count_comment_tree(getattr(result, "comments", None))


def _count_comment_tree(comments: Any) -> int:
    if not isinstance(comments, list):
        return 0
    total = 0
    stack = list(comments)
    seen: set[int] = set()
    while stack:
        comment = stack.pop()
        marker = id(comment)
        if marker in seen:
            continue
        seen.add(marker)
        total += 1
        replies = comment.get("replies") if isinstance(comment, dict) else getattr(comment, "replies", None)
        if isinstance(replies, list):
            stack.extend(replies)
    return total


def _load_expected_comment_counts(
    *,
    repo: Any,
    account_handle: str,
    target_source_ids: list[str],
) -> dict[str, int]:
    normalized_account = str(account_handle or "").strip().lower().lstrip("@")
    requested = list(dict.fromkeys(str(item or "").strip() for item in target_source_ids if str(item or "").strip()))
    if not normalized_account or not requested:
        return {}
    owner_match_clause = repo._social_account_profile_owner_match_sql("instagram", alias="p")
    reported_comments_expr = repo._instagram_reported_comments_sql("p")
    rows = pg.fetch_all(
        f"""
        with requested as (
          select
            nullif(shortcode, '')::text as shortcode,
            ordinality::int as sort_order
          from unnest(%s::text[]) with ordinality as request(shortcode, ordinality)
          where nullif(shortcode, '') is not null
        ),
        owner_posts as (
          select
            p.shortcode::text as shortcode,
            {reported_comments_expr}::bigint as reported_comments,
            row_number() over (
              partition by p.shortcode
              order by p.posted_at desc nulls last, p.id desc
            ) as row_number
          from social.instagram_posts p
          join requested r on r.shortcode = p.shortcode
          where {owner_match_clause}
        )
        select
          r.shortcode,
          coalesce(op.reported_comments, 0)::bigint as reported_comments
        from requested r
        left join owner_posts op on op.shortcode = r.shortcode and op.row_number = 1
        order by r.sort_order
        """,
        [requested, normalized_account],
    )
    counts: dict[str, int] = {}
    for row in rows:
        shortcode = str(row.get("shortcode") or "").strip()
        reported = _safe_int(row.get("reported_comments"))
        if shortcode and reported is not None:
            counts[shortcode] = reported
    return counts


def _coerce_comment_timestamp(value: Any) -> tuple[int, str]:
    if value is None:
        return 0, ""
    if hasattr(value, "timestamp"):
        try:
            timestamp = int(value.timestamp())
        except (TypeError, ValueError, OSError):
            timestamp = 0
        iso_value = value.isoformat() if hasattr(value, "isoformat") else str(value)
        return max(0, timestamp), iso_value
    raw_value = str(value or "").strip()
    return 0, raw_value


def _json_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if str(item or "").strip()]
    return []


def _comment_url_from_row(row: dict[str, Any], *, shortcode: str, comment_id: str) -> str | None:
    existing = str(row.get("comment_url") or "").strip()
    if existing:
        return existing
    normalized_shortcode = str(shortcode or "").strip()
    normalized_comment_id = str(comment_id or "").strip()
    if not (normalized_shortcode and normalized_comment_id):
        return None
    return f"https://www.instagram.com/p/{normalized_shortcode}/c/{normalized_comment_id}/"


def _load_persisted_replies_by_parent(
    *,
    account_handle: str,
    shortcode: str,
) -> dict[str, list[InstagramComment]]:
    normalized_shortcode = str(shortcode or "").strip()
    if not normalized_shortcode:
        return {}
    rows = pg.fetch_all(
        """
        select
          reply.parent_comment_external_id::text as parent_comment_external_id,
          reply.comment_id::text as comment_id,
          coalesce(reply.text, '')::text as text,
          coalesce(reply.username, '')::text as username,
          coalesce(reply.user_id, '')::text as user_id,
          coalesce(reply.likes, 0)::int as likes,
          reply.created_at,
          coalesce(reply.reply_count, 0)::int as reply_count,
          coalesce(reply.reply_depth, 1)::int as reply_depth,
          reply.media_urls,
          reply.hosted_media_urls,
          reply.author_full_name,
          reply.author_profile_pic_url,
          reply.author_profile_pic_url_hd,
          reply.author_is_verified,
          reply.comment_url,
          reply.author_fbid_v2,
          reply.author_is_mentionable,
          reply.author_is_private,
          reply.author_latest_reel_media,
          reply.author_profile_pic_id,
          reply.raw_data,
          reply.source_snapshot_type
        from social.instagram_comments reply
        join social.instagram_posts post on post.id = reply.post_id
        where post.shortcode = %s
          and coalesce(reply.is_reply, false) = true
          and coalesce(reply.is_missing, false) = false
          and reply.deleted_at is null
          and nullif(reply.comment_id, '') is not null
          and nullif(reply.parent_comment_external_id, '') is not null
        order by reply.parent_comment_external_id, reply.created_at asc nulls last, reply.comment_id asc
        """,
        [normalized_shortcode],
    )
    replies_by_parent: dict[str, list[InstagramComment]] = {}
    for row in rows:
        parent_id = str(row.get("parent_comment_external_id") or "").strip()
        comment_id = str(row.get("comment_id") or "").strip()
        if not (parent_id and comment_id):
            continue
        created_at, date_time = _coerce_comment_timestamp(row.get("created_at"))
        reply = InstagramComment(
            comment_id=comment_id,
            text=str(row.get("text") or ""),
            username=str(row.get("username") or ""),
            user_id=str(row.get("user_id") or ""),
            created_at=created_at,
            date_time=date_time,
            likes=int(row.get("likes") or 0),
            is_reply=True,
            parent_comment_id=parent_id,
            reply_count=int(row.get("reply_count") or 0),
            reply_depth=max(1, int(row.get("reply_depth") or 1)),
            media_urls=_json_list(row.get("media_urls")),
            hosted_media_urls=_json_list(row.get("hosted_media_urls")),
            owner_full_name=str(row.get("author_full_name") or "") or None,
            owner_profile_pic_url=str(row.get("author_profile_pic_url") or "") or None,
            owner_profile_pic_url_hd=str(row.get("author_profile_pic_url_hd") or "") or None,
            owner_is_verified=(
                bool(row.get("author_is_verified")) if row.get("author_is_verified") is not None else None
            ),
            owner_fbid_v2=str(row.get("author_fbid_v2") or "") or None,
            owner_is_mentionable=(
                bool(row.get("author_is_mentionable")) if row.get("author_is_mentionable") is not None else None
            ),
            owner_is_private=(
                bool(row.get("author_is_private")) if row.get("author_is_private") is not None else None
            ),
            owner_latest_reel_media=_safe_int(row.get("author_latest_reel_media")),
            owner_profile_pic_id=str(row.get("author_profile_pic_id") or "") or None,
            is_hidden_by_instagram=bool((row.get("raw_data") or {}).get("is_hidden_by_instagram"))
            if isinstance(row.get("raw_data"), dict)
            else False,
            source_snapshot_type=str(row.get("source_snapshot_type") or "full_comments_scrape"),
            post_shortcode=normalized_shortcode,
            post_url=f"https://www.instagram.com/p/{normalized_shortcode}/",
            comment_url=_comment_url_from_row(row, shortcode=normalized_shortcode, comment_id=comment_id),
        )
        replies_by_parent.setdefault(parent_id, []).append(reply)
    return replies_by_parent


def _load_persisted_top_level_comments_for_reply_retry(
    *,
    account_handle: str,
    shortcode: str,
) -> list[InstagramComment]:
    del account_handle
    normalized_shortcode = str(shortcode or "").strip()
    if not normalized_shortcode:
        return []
    rows = pg.fetch_all(
        """
        select
          top.comment_id::text as comment_id,
          coalesce(top.text, '')::text as text,
          coalesce(top.username, '')::text as username,
          coalesce(top.user_id, '')::text as user_id,
          coalesce(top.likes, 0)::int as likes,
          top.created_at,
          coalesce(top.reply_count, 0)::int as reply_count,
          top.media_urls,
          top.hosted_media_urls,
          top.author_full_name,
          top.author_profile_pic_url,
          top.author_profile_pic_url_hd,
          top.author_is_verified,
          top.comment_url,
          top.author_fbid_v2,
          top.author_is_mentionable,
          top.author_is_private,
          top.author_latest_reel_media,
          top.author_profile_pic_id,
          top.raw_data,
          top.source_snapshot_type,
          coalesce(reply_counts.saved_reply_count, 0)::int as saved_reply_count
        from social.instagram_comments top
        join social.instagram_posts post on post.id = top.post_id
        left join lateral (
          select count(*)::int as saved_reply_count
          from social.instagram_comments reply
          where reply.post_id = top.post_id
            and reply.parent_comment_external_id = top.comment_id
            and coalesce(reply.is_reply, false) = true
            and coalesce(reply.is_missing, false) = false
            and reply.deleted_at is null
            and nullif(reply.comment_id, '') is not null
        ) reply_counts on true
        where post.shortcode = %s
          and coalesce(top.is_reply, false) = false
          and top.parent_comment_external_id is null
          and coalesce(top.is_missing, false) = false
          and top.deleted_at is null
          and nullif(top.comment_id, '') is not null
          and coalesce(top.reply_count, 0) > coalesce(reply_counts.saved_reply_count, 0)
        order by
          (coalesce(top.reply_count, 0) - coalesce(reply_counts.saved_reply_count, 0)) desc,
          top.created_at asc nulls last,
          top.comment_id asc
        """,
        [normalized_shortcode],
    )
    comments: list[InstagramComment] = []
    for row in rows:
        comment_id = str(row.get("comment_id") or "").strip()
        if not comment_id:
            continue
        created_at, date_time = _coerce_comment_timestamp(row.get("created_at"))
        comments.append(
            InstagramComment(
                comment_id=comment_id,
                text=str(row.get("text") or ""),
                username=str(row.get("username") or ""),
                user_id=str(row.get("user_id") or ""),
                created_at=created_at,
                date_time=date_time,
                likes=int(row.get("likes") or 0),
                is_reply=False,
                parent_comment_id=None,
                reply_count=int(row.get("reply_count") or 0),
                reply_depth=0,
                media_urls=_json_list(row.get("media_urls")),
                hosted_media_urls=_json_list(row.get("hosted_media_urls")),
                owner_full_name=str(row.get("author_full_name") or "") or None,
                owner_profile_pic_url=str(row.get("author_profile_pic_url") or "") or None,
                owner_profile_pic_url_hd=str(row.get("author_profile_pic_url_hd") or "") or None,
                owner_is_verified=(
                    bool(row.get("author_is_verified")) if row.get("author_is_verified") is not None else None
                ),
                owner_fbid_v2=str(row.get("author_fbid_v2") or "") or None,
                owner_is_mentionable=(
                    bool(row.get("author_is_mentionable")) if row.get("author_is_mentionable") is not None else None
                ),
                owner_is_private=(
                    bool(row.get("author_is_private")) if row.get("author_is_private") is not None else None
                ),
                owner_latest_reel_media=_safe_int(row.get("author_latest_reel_media")),
                owner_profile_pic_id=str(row.get("author_profile_pic_id") or "") or None,
                is_hidden_by_instagram=bool((row.get("raw_data") or {}).get("is_hidden_by_instagram"))
                if isinstance(row.get("raw_data"), dict)
                else False,
                source_snapshot_type=str(row.get("source_snapshot_type") or "full_comments_scrape"),
                post_shortcode=normalized_shortcode,
                post_url=f"https://www.instagram.com/p/{normalized_shortcode}/",
                comment_url=_comment_url_from_row(row, shortcode=normalized_shortcode, comment_id=comment_id),
            )
        )
    return comments


def _post_latency_metadata(samples: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "samples": samples[-25:],
        "slowest": sorted(
            samples,
            key=lambda sample: int(sample.get("total_elapsed_ms") or 0),
            reverse=True,
        )[:10],
        "sample_count": len(samples),
    }


def _comment_completeness_metadata(samples: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "complete_posts": sum(1 for sample in samples if sample.get("is_complete")),
        "incomplete_posts": sum(1 for sample in samples if not sample.get("is_complete")),
        "completion_reasons": dict(Counter(str(sample.get("completion_reason") or "unknown") for sample in samples)),
    }


def _retry_rebalance_metadata(
    *,
    comments_shard_count: int,
    target_source_ids: list[str],
    processed_posts: int,
    incomplete_target_source_ids: list[str] | None = None,
    auth_failed_target_source_ids: list[str] | None = None,
) -> dict[str, Any] | None:
    retry_targets = [
        str(item or "").strip()
        for item in (incomplete_target_source_ids or [])
        if str(item or "").strip()
    ]
    retry_targets.extend(
        str(item or "").strip()
        for item in (auth_failed_target_source_ids or [])
        if str(item or "").strip()
    )
    retry_targets.extend(target_source_ids[max(0, processed_posts) :])
    remaining_targets = list(dict.fromkeys(retry_targets))
    if comments_shard_count <= 1 and not remaining_targets:
        return None
    return {
        "remaining_target_source_ids": remaining_targets,
        "eligible": bool(remaining_targets),
    }


def _reply_checkpoint_summary(fetcher_metadata: dict[str, Any]) -> dict[str, Any]:
    checkpoint_metadata = fetcher_metadata.get("reply_checkpoint_metadata")
    if not isinstance(checkpoint_metadata, dict):
        return {
            "total_count": 0,
            "retained_count": 0,
            "dropped_count": 0,
            "truncated": False,
            "stop_reasons": {},
            "latest": None,
        }
    items = [
        item
        for item in (checkpoint_metadata.get("items") or [])
        if isinstance(item, dict)
    ]
    return {
        "total_count": int(checkpoint_metadata.get("total_count") or len(items)),
        "retained_count": len(items),
        "dropped_count": int(checkpoint_metadata.get("dropped_count") or 0),
        "truncated": bool(checkpoint_metadata.get("truncated")),
        "stop_reasons": dict(Counter(str(item.get("stop_reason") or "unknown") for item in items)),
        "latest": items[-1] if items else None,
    }


def _top_level_checkpoint_summary(
    fetcher_metadata: dict[str, Any],
    top_level_checkpoints: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    checkpoint_metadata = fetcher_metadata.get("top_level_checkpoint_metadata")
    runtime_items: list[dict[str, Any]] = []
    if isinstance(checkpoint_metadata, dict):
        runtime_items = [
            item for item in (checkpoint_metadata.get("items") or []) if isinstance(item, dict)
        ]
    merged_by_shortcode: dict[str, dict[str, Any]] = {}
    for checkpoint in [*runtime_items, *top_level_checkpoints.values()]:
        shortcode = str(
            checkpoint.get("target_shortcode")
            or checkpoint.get("source_id")
            or checkpoint.get("shortcode")
            or ""
        ).strip()
        if shortcode:
            merged_by_shortcode[shortcode] = dict(checkpoint)
    items = list(merged_by_shortcode.values())
    return {
        "total_count": int(
            checkpoint_metadata.get("total_count") if isinstance(checkpoint_metadata, dict) else len(items)
            or len(items)
        ),
        "retained_count": len(items),
        "dropped_count": int(
            checkpoint_metadata.get("dropped_count") if isinstance(checkpoint_metadata, dict) else 0
            or 0
        ),
        "truncated": bool(checkpoint_metadata.get("truncated")) if isinstance(checkpoint_metadata, dict) else False,
        "stop_reasons": dict(Counter(str(item.get("stop_reason") or "unknown") for item in items)),
        "items": items,
        "latest": items[-1] if items else None,
    }


def _top_level_resume_cursors_from_job(job: dict[str, Any]) -> dict[str, str]:
    metadata = job.get("metadata")
    if not isinstance(metadata, dict):
        return {}
    candidates: list[Any] = []
    for key in ("top_level_checkpoints", "instagram_comments_top_level_checkpoints"):
        value = metadata.get(key)
        if isinstance(value, list):
            candidates.extend(value)
    summary = metadata.get("top_level_checkpoint_summary")
    if isinstance(summary, dict) and isinstance(summary.get("items"), list):
        candidates.extend(summary.get("items") or [])
    runtime = metadata.get("fetcher_runtime")
    if isinstance(runtime, dict):
        checkpoint_metadata = runtime.get("top_level_checkpoint_metadata")
        if isinstance(checkpoint_metadata, dict) and isinstance(checkpoint_metadata.get("items"), list):
            candidates.extend(checkpoint_metadata.get("items") or [])

    cursors: dict[str, str] = {}
    for item in candidates:
        if not isinstance(item, dict):
            continue
        shortcode = str(
            item.get("target_shortcode")
            or item.get("source_id")
            or item.get("shortcode")
            or ""
        ).strip()
        stop_reason = str(item.get("stop_reason") or "").strip()
        cursor = str(item.get("next_top_level_cursor") or "").strip()
        if not cursor and stop_reason != "pagination_repeated_cursor":
            cursor = str(item.get("last_top_level_cursor") or "").strip()
        if shortcode and cursor:
            cursors[shortcode] = cursor
    return cursors


def _top_level_resume_cursor_params_from_job(job: dict[str, Any]) -> dict[str, str]:
    metadata = job.get("metadata")
    if not isinstance(metadata, dict):
        return {}
    candidates: list[Any] = []
    for key in ("top_level_checkpoints", "instagram_comments_top_level_checkpoints"):
        value = metadata.get(key)
        if isinstance(value, list):
            candidates.extend(value)
    summary = metadata.get("top_level_checkpoint_summary")
    if isinstance(summary, dict) and isinstance(summary.get("items"), list):
        candidates.extend(summary.get("items") or [])
    runtime = metadata.get("fetcher_runtime")
    if isinstance(runtime, dict):
        checkpoint_metadata = runtime.get("top_level_checkpoint_metadata")
        if isinstance(checkpoint_metadata, dict) and isinstance(checkpoint_metadata.get("items"), list):
            candidates.extend(checkpoint_metadata.get("items") or [])

    params: dict[str, str] = {}
    for item in candidates:
        if not isinstance(item, dict):
            continue
        shortcode = str(
            item.get("target_shortcode")
            or item.get("source_id")
            or item.get("shortcode")
            or ""
        ).strip()
        if not shortcode:
            continue
        stop_reason = str(item.get("stop_reason") or "").strip()
        cursor = str(item.get("next_top_level_cursor") or "").strip()
        cursor_param = str(item.get("next_top_level_cursor_param") or "").strip()
        if not cursor and stop_reason != "pagination_repeated_cursor":
            cursor = str(item.get("last_top_level_cursor") or "").strip()
            cursor_param = str(item.get("last_top_level_cursor_param") or "").strip()
        if cursor and cursor_param in {"min_id", "max_id"}:
            params[shortcode] = cursor_param
    return params


def _reply_resume_cursors_from_job(job: dict[str, Any]) -> dict[str, str]:
    metadata = job.get("metadata")
    if not isinstance(metadata, dict):
        return {}
    candidates: list[Any] = []
    summary = metadata.get("reply_checkpoint_summary")
    if isinstance(summary, dict):
        latest = summary.get("latest")
        if isinstance(latest, dict):
            candidates.append(latest)
        if isinstance(summary.get("items"), list):
            candidates.extend(summary.get("items") or [])
    runtime = metadata.get("fetcher_runtime")
    if isinstance(runtime, dict):
        checkpoint_metadata = runtime.get("reply_checkpoint_metadata")
        if isinstance(checkpoint_metadata, dict) and isinstance(checkpoint_metadata.get("items"), list):
            candidates.extend(checkpoint_metadata.get("items") or [])

    cursors: dict[str, str] = {}
    for item in candidates:
        if not isinstance(item, dict):
            continue
        parent_comment_id = str(item.get("parent_comment_id") or "").strip()
        cursor = str(item.get("next_reply_cursor") or item.get("last_reply_cursor") or "").strip()
        if parent_comment_id and cursor:
            cursors[parent_comment_id] = cursor
    return cursors


def _reply_resume_cursor_params_from_job(job: dict[str, Any]) -> dict[str, str]:
    metadata = job.get("metadata")
    if not isinstance(metadata, dict):
        return {}
    candidates: list[Any] = []
    summary = metadata.get("reply_checkpoint_summary")
    if isinstance(summary, dict):
        latest = summary.get("latest")
        if isinstance(latest, dict):
            candidates.append(latest)
        if isinstance(summary.get("items"), list):
            candidates.extend(summary.get("items") or [])
    runtime = metadata.get("fetcher_runtime")
    if isinstance(runtime, dict):
        checkpoint_metadata = runtime.get("reply_checkpoint_metadata")
        if isinstance(checkpoint_metadata, dict) and isinstance(checkpoint_metadata.get("items"), list):
            candidates.extend(checkpoint_metadata.get("items") or [])

    params: dict[str, str] = {}
    for item in candidates:
        if not isinstance(item, dict):
            continue
        parent_comment_id = str(item.get("parent_comment_id") or "").strip()
        cursor_param = str(item.get("next_reply_cursor_param") or item.get("last_reply_cursor_param") or "").strip()
        if parent_comment_id and cursor_param in {"min_id", "max_id"}:
            params[parent_comment_id] = cursor_param
    return params


def _metadata_string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return list(dict.fromkeys(str(item or "").strip() for item in value if str(item or "").strip()))


def _prior_retry_incomplete_targets(job: dict[str, Any]) -> list[str]:
    metadata = job.get("metadata")
    if not isinstance(metadata, dict):
        return []
    candidates: list[Any] = [
        metadata.get("incomplete_target_source_ids"),
        metadata.get("auth_failed_target_source_ids"),
    ]
    runtime_metadata = metadata.get("runtime_metadata")
    if isinstance(runtime_metadata, dict):
        candidates.extend(
            [
                runtime_metadata.get("incomplete_target_source_ids"),
                runtime_metadata.get("auth_failed_target_source_ids"),
            ]
        )
    retry_rebalance = metadata.get("retry_rebalance")
    if isinstance(retry_rebalance, dict):
        candidates.append(retry_rebalance.get("remaining_target_source_ids"))
    targets: list[str] = []
    for candidate in candidates:
        targets.extend(_metadata_string_list(candidate))
    return list(dict.fromkeys(targets))


def _incomplete_retry_has_stalled(
    *,
    job: dict[str, Any],
    attempt_count: int,
    retryable_incomplete_targets: list[str],
    retry_fetch_reasons: dict[str, str | None],
    comments_fetched: int,
) -> dict[str, Any] | None:
    try:
        stall_attempts = int(
            os.environ.get("SOCIAL_INSTAGRAM_COMMENTS_INCOMPLETE_STALL_ATTEMPTS")
            or _INCOMPLETE_RETRY_STALL_ATTEMPTS_DEFAULT
        )
    except (TypeError, ValueError):
        stall_attempts = _INCOMPLETE_RETRY_STALL_ATTEMPTS_DEFAULT
    stall_attempts = max(2, min(stall_attempts, 20))
    if attempt_count < stall_attempts:
        return None
    current_targets = list(dict.fromkeys(str(item or "").strip() for item in retryable_incomplete_targets if item))
    if not current_targets:
        return None
    prior_targets = _prior_retry_incomplete_targets(job)
    if set(prior_targets) != set(current_targets):
        return None
    normalized_reasons = {
        str(reason or "").strip().lower()
        for shortcode, reason in retry_fetch_reasons.items()
        if shortcode in current_targets
    }
    if not normalized_reasons or not normalized_reasons.issubset(_INCOMPLETE_RETRY_STALL_REASONS):
        return None
    prior_items_found = _safe_int(job.get("items_found")) or 0
    if prior_items_found > 0 and comments_fetched > prior_items_found:
        return None
    return {
        "stalled": True,
        "attempt_count": attempt_count,
        "stall_attempts": stall_attempts,
        "target_source_ids": current_targets,
        "fetch_reasons": {target: retry_fetch_reasons.get(target) for target in current_targets},
        "prior_items_found": prior_items_found,
        "current_comments_fetched": comments_fetched,
    }


def _prior_incomplete_fetch_reason(job: dict[str, Any], shortcode: str) -> str:
    metadata = job.get("metadata")
    if not isinstance(metadata, dict):
        metadata = {}
    normalized_shortcode = str(shortcode or "").strip()
    reason_maps: list[Any] = [
        metadata.get("incomplete_fetch_reasons"),
        metadata.get("auth_failed_fetch_reasons"),
    ]
    runtime_metadata = metadata.get("runtime_metadata")
    if isinstance(runtime_metadata, dict):
        reason_maps.append(runtime_metadata.get("incomplete_fetch_reasons"))
    post_fetch_failures = metadata.get("post_fetch_failures")
    if isinstance(post_fetch_failures, dict):
        reason_maps.append(post_fetch_failures.get("fetch_reasons"))
    for reason_map in reason_maps:
        if not isinstance(reason_map, dict):
            continue
        reason = str(reason_map.get(normalized_shortcode) or "").strip()
        if reason:
            return reason
    return str(metadata.get("failure_reason_code") or metadata.get("error_code") or "").strip()


def _auth_context_metadata(auth_session: Any | None) -> dict[str, Any]:
    if auth_session is None:
        return {}
    metadata = dict(getattr(auth_session, "metadata", {}) or {})
    return {
        "session_source": getattr(auth_session, "source", None),
        "browser_account_id": getattr(auth_session, "browser_account_id", None),
        "session_account_id": getattr(auth_session, "session_account_id", None),
        "validation_category": getattr(auth_session, "validation_category", None),
        "validation_reason": getattr(auth_session, "validation_reason", None),
        "validated": bool(getattr(auth_session, "validated", False)),
        "stale_ok": bool(getattr(auth_session, "stale_ok", False)),
        "resolver_version": getattr(auth_session, "resolver_version", None),
        "browser_session_used": bool(metadata.get("browser_session_used")),
        "cookie_source_fingerprint": metadata.get("fingerprint"),
    }


def _abort_queued_sibling_shards_after_run_fatal_error(
    *,
    repo: Any,
    run_id: str,
    failed_job_id: str,
    stage: str,
    account_handle: str,
    mode: str,
    source_scope: str,
    error_code: str,
    error_class: str,
    error_message: str,
) -> int:
    if not run_id or not failed_job_id:
        return 0
    normalized_error_code = str(error_code or "").strip().lower()
    if normalized_error_code not in _RUN_FATAL_COMMENTS_ERROR_CODES:
        return 0
    sibling_rows = pg.fetch_all(
        """
        select id::text as id, coalesce(items_found, 0)::int as items_found
        from social.scrape_jobs
        where run_id = %s::uuid
          and id <> %s::uuid
          and status in ('queued', 'pending', 'retrying')
          and coalesce(config->>'stage', metadata->>'stage', job_type, '') = %s
        order by created_at asc
        """,
        [run_id, failed_job_id, stage],
    )
    aborted = 0
    for sibling in sibling_rows:
        sibling_id = str(sibling.get("id") or "").strip()
        if not sibling_id:
            continue
        repo._finish_job(
            sibling_id,
            status="failed",
            items_found=_safe_int(sibling.get("items_found")) or 0,
            error_message=(
                f"Aborted because sibling shard {failed_job_id} failed with run-level "
                f"Instagram comments error {normalized_error_code}: {error_message}"
            ),
            metadata={
                "stage": stage,
                "platform": "instagram",
                "account": account_handle,
                "mode": mode,
                "source_scope": source_scope,
                "error_code": normalized_error_code,
                "error_class": "SiblingShardAborted",
                "aborted_by_sibling_job_id": failed_job_id,
                "aborted_by_error_code": normalized_error_code,
                "activity": {"phase": "aborted_by_run_fatal_error"},
            },
            last_error_code=normalized_error_code,
            last_error_class=error_class or "SiblingShardAborted",
        )
        aborted += 1
    if aborted:
        logger.warning(
            "Aborted %d queued Instagram comments sibling shard(s) after run-level failure: "
            "run_id=%s job_id=%s error=%s",
            aborted,
            run_id,
            failed_job_id,
            normalized_error_code,
        )
    return aborted


def run_instagram_comments_scrapling_job(job: dict[str, Any], *, worker_id: str | None = None) -> dict[str, Any]:
    from trr_backend.repositories import social_season_analytics as repo

    job_runner_started_at = repo._now_utc()
    job_id = str(job.get("id") or "").strip()
    run_id = str(job.get("run_id") or "").strip()
    config = dict(job.get("config") or {})
    account_handle = str(config.get("account") or "").strip().lower().lstrip("@")
    stage = str(config.get("stage") or repo.INSTAGRAM_COMMENTS_SCRAPLING_STAGE).strip().lower()
    mode = str(config.get("mode") or "profile").strip().lower()
    source_scope = str(config.get("source_scope") or "bravo").strip().lower() or "bravo"
    max_comments_per_post = max(0, int(config.get("max_comments_per_post") or 0))
    fetch_replies = bool(config.get("fetch_replies", True))
    target_source_ids = [
        str(item or "").strip()
        for item in (config.get("target_source_ids") or ([config.get("source_id")] if config.get("source_id") else []))
        if str(item or "").strip()
    ]
    attempt_count, max_attempts = _job_attempt_state(job)
    comments_shard_index = max(1, int(config.get("comments_shard_index") or 1))
    comments_shard_count = max(1, int(config.get("comments_shard_count") or 1))
    comments_shard_target_count = max(0, int(config.get("comments_shard_target_count") or len(target_source_ids)))
    cancel_check_every_posts = max(
        1,
        _safe_int(config.get("comments_cancel_check_every_posts")) or _DEFAULT_CANCEL_CHECK_EVERY_POSTS,
    )
    skipped_complete_target_source_ids: list[str] = []
    top_level_resume_cursors_by_shortcode = _top_level_resume_cursors_from_job(job)
    top_level_resume_cursor_params_by_shortcode = _top_level_resume_cursor_params_from_job(job)
    reply_resume_cursors_by_parent = _reply_resume_cursors_from_job(job)
    reply_resume_cursor_params_by_parent = _reply_resume_cursor_params_from_job(job)
    skip_complete_retry_targets = any(
        _config_truthy(config.get(flag))
        for flag in (
            "comments_retry_rebalance",
            "comments_retry_incomplete",
            "comments_target_gap_repair",
            "comments_skip_complete_targets",
        )
    ) or str(config.get("target_filter") or "").strip().lower() == "incomplete" or _config_truthy(
        config.get("incomplete_fill")
    ) or str(job.get("status") or "").strip().lower() == "retrying" or attempt_count > 1
    if skip_complete_retry_targets and target_source_ids:
        try:
            incomplete_targets = repo._instagram_filter_incomplete_comment_targets(account_handle, target_source_ids)
            incomplete_target_set = set(incomplete_targets)
            skipped_complete_target_source_ids = [
                target for target in target_source_ids if target not in incomplete_target_set
            ]
        except pg.DatabaseServiceUnavailableError as exc:
            logger.warning(
                "Continuing comments retry without complete-target prefilter after database saturation: "
                "job_id=%s error=%s",
                job_id,
                exc,
            )
    shard_metadata = {
        "comments_shard_index": comments_shard_index,
        "comments_shard_count": comments_shard_count,
        "comments_shard_target_count": comments_shard_target_count,
    }
    if not account_handle:
        raise CommentsScraplingRuntimeError(
            "Instagram comments Scrapling job is missing an account handle.",
            error_code="instagram_comments_account_missing",
            retryable=False,
        )
    if not target_source_ids:
        raise CommentsScraplingRuntimeError(
            "Instagram comments Scrapling job has no target post shortcodes.",
            error_code="instagram_comments_targets_missing",
            retryable=False,
        )
    try:
        expected_comment_counts_by_shortcode = _load_expected_comment_counts(
            repo=repo,
            account_handle=account_handle,
            target_source_ids=target_source_ids,
        )
    except Exception as exc:  # noqa: BLE001
        expected_comment_counts_by_shortcode = {}
        logger.warning(
            "Continuing Instagram comments job without expected comment counts: job_id=%s error=%s",
            job_id,
            exc,
            exc_info=True,
        )

    progress_state = repo._new_job_progress_state()
    processed_posts = 0
    comments_upserted = 0
    comments_fetched = 0
    comments_marked_missing = 0
    mirror_jobs_enqueued = 0
    mirror_job_enqueue_errors = 0
    activity: dict[str, Any] = {
        "phase": "comments_scrapling_start",
        "posts_checked": 0,
        "matched_posts": 0,
        "saved_posts": 0,
        "total_posts": len(target_source_ids),
        **shard_metadata,
    }
    fetcher_metadata: dict[str, Any] = {}
    top_level_checkpoints_by_shortcode: dict[str, dict[str, Any]] = {}
    post_latency_samples: list[dict[str, Any]] = []
    incomplete_target_source_ids: list[str] = []
    incomplete_fetch_reasons: dict[str, str] = {}
    auth_failed_target_source_ids: list[str] = []
    auth_failed_fetch_reasons: dict[str, str] = {}
    consecutive_post_auth_failures = 0
    consecutive_post_fetch_failures = 0
    successful_target_fetches = 0
    post_auth_failure_circuit_limit = _safe_int(config.get("post_auth_failure_circuit_limit")) or 3
    post_fetch_failure_circuit_limit = _safe_int(config.get("post_fetch_failure_circuit_limit")) or 3
    # Phase 1.5: mid-run warmup refresh state.
    mid_run_warmup_auth_threshold = (
        _safe_int(config.get("comments_warmup_refresh_auth_threshold"))
        or _DEFAULT_MID_RUN_WARMUP_AUTH_THRESHOLD
    )
    mid_run_warmup_every_posts = (
        _safe_int(config.get("comments_warmup_refresh_every_posts"))
        or _DEFAULT_MID_RUN_WARMUP_EVERY_POSTS
    )
    posts_since_last_warmup = 0
    mid_run_warmup_count = 0
    last_mid_run_warmup_reason: str | None = None
    # Phase 1.7: accumulator for per-comment failure attribution. Capped via
    # _COMMENT_FAILURE_METADATA_MAX_ENTRIES so a runaway shard cannot bloat
    # social.scrape_jobs.metadata. Persisted under metadata.comment_failures.
    failed_comment_ids: list[dict[str, Any]] = []
    failed_comment_ids_truncated = False
    incomplete_retry_stall_metadata: dict[str, Any] | None = None
    warmup_completed_at = None
    first_post_persisted_at = None
    auth_context: dict[str, Any] = {}
    terminal_status = str(job.get("status") or "").strip().lower() or None
    terminal_error_message: str | None = None

    def progress_metadata_common() -> dict[str, Any]:
        return {
            "post_auth_failures": {
                "target_source_ids": list(dict.fromkeys(auth_failed_target_source_ids)),
                "fetch_reasons": dict(auth_failed_fetch_reasons),
                "circuit_limit": post_auth_failure_circuit_limit,
            },
            "post_fetch_failures": {
                "target_source_ids": list(dict.fromkeys(incomplete_target_source_ids)),
                "fetch_reasons": dict(incomplete_fetch_reasons),
                "circuit_limit": post_fetch_failure_circuit_limit,
            },
            "incomplete_target_source_ids": list(dict.fromkeys(incomplete_target_source_ids)),
            "incomplete_fetch_reasons": dict(incomplete_fetch_reasons),
            "auth_failed_target_source_ids": list(dict.fromkeys(auth_failed_target_source_ids)),
            "auth_failed_fetch_reasons": dict(auth_failed_fetch_reasons),
            "top_level_checkpoints": list(top_level_checkpoints_by_shortcode.values()),
            # Phase 1.5: mid-run warmup-refresh telemetry.
            "mid_run_warmup": {
                "count": mid_run_warmup_count,
                "auth_threshold": mid_run_warmup_auth_threshold,
                "every_posts": mid_run_warmup_every_posts,
                "last_reason": last_mid_run_warmup_reason,
                "posts_since_last_warmup": posts_since_last_warmup,
            },
            # Phase 1.7: per-comment failure attribution. Operational metadata
            # only — comments-table column expansion is intentionally deferred.
            "comment_failures": {
                "entries": list(failed_comment_ids),
                "count": len(failed_comment_ids),
                "truncated": failed_comment_ids_truncated,
                "max_entries": _COMMENT_FAILURE_METADATA_MAX_ENTRIES,
            },
            "incomplete_retry_stalled": incomplete_retry_stall_metadata,
        }

    def terminal_metadata_common() -> dict[str, Any]:
        return {
            **shard_metadata,
            "post_latency": _post_latency_metadata(post_latency_samples),
            "comment_completeness": _comment_completeness_metadata(post_latency_samples),
            **progress_metadata_common(),
            "reply_checkpoint_summary": _reply_checkpoint_summary(fetcher_metadata),
            "top_level_checkpoint_summary": _top_level_checkpoint_summary(
                fetcher_metadata,
                top_level_checkpoints_by_shortcode,
            ),
            "timing": {
                "job_runner_started_at": repo._iso(job_runner_started_at),
                "warmup_completed_at": repo._iso(warmup_completed_at),
                "first_post_persisted_at": repo._iso(first_post_persisted_at),
            },
        }

    # Single event loop: fetcher is created, warmed up, used for all
    # shortcodes, and closed within one asyncio.run(). The httpx client
    # and Patchright browser share the same loop lifetime.
    async def _run_job() -> tuple[dict[str, Any], dict[str, Any]]:
        nonlocal processed_posts, comments_upserted, comments_fetched
        nonlocal comments_marked_missing, mirror_jobs_enqueued, mirror_job_enqueue_errors
        nonlocal activity, fetcher_metadata
        nonlocal consecutive_post_auth_failures, consecutive_post_fetch_failures, successful_target_fetches
        nonlocal warmup_completed_at, first_post_persisted_at, auth_context
        # Phase 1.5 / 1.7: scope mid-run warmup counters and per-comment failure
        # truncation flag to the outer function so progress_metadata_common()
        # reads the live values and _maybe_refresh_warmup() writes propagate.
        nonlocal posts_since_last_warmup, mid_run_warmup_count, last_mid_run_warmup_reason
        nonlocal failed_comment_ids_truncated, incomplete_retry_stall_metadata

        session = resolve_comments_scrapling_session(
            browser_account_id=account_handle,
            caller_context=f"comments_scrapling:{mode}:{account_handle}",
        )
        auth_context = _auth_context_metadata(session.auth_session)
        use_shard_proxy_session = _config_truthy(config.get("comments_proxy_shard_sessions"))
        proxy_session_key = (
            f"{account_handle}:comments:{comments_shard_index}"
            if use_shard_proxy_session and comments_shard_count > 1
            else str(session.browser_account_id or account_handle).strip().lower().lstrip("@")
        )
        proxy_config = select_comments_proxy(session_key=proxy_session_key or account_handle)
        fetcher = InstagramCommentsScraplingFetcher(
            cookies=session.cookies,
            raw_cookies=session.auth_session.cookies,
            browser_account_id=session.browser_account_id,
            proxy_config=proxy_config,
        )
        heartbeat_task = asyncio.create_task(
            _maintain_comments_job_heartbeat(
                job_id=job_id,
                worker_id=worker_id,
                interval_seconds=_resolve_job_heartbeat_interval_seconds(),
            )
        )
        auth_metadata: dict[str, Any] = {}
        try:
            try:
                await fetcher.warmup()
                warmup_completed_at = repo._now_utc()
            except InstagramCommentsWarmupError as exc:
                raise CommentsScraplingRuntimeError(
                    str(exc),
                    error_code=exc.error_code,
                    retryable=exc.retryable,
                    runtime_metadata=dict(fetcher.runtime_metadata),
                ) from exc
            except Exception as exc:  # noqa: BLE001
                if _is_comments_transport_error(exc):
                    raise CommentsScraplingRuntimeError(
                        str(exc),
                        error_code="instagram_comments_warmup_transport_error",
                        retryable=True,
                        runtime_metadata=dict(fetcher.runtime_metadata),
                    ) from exc
                raise
            auth_metadata = dict(session.auth_session.metadata or {})
            _raise_if_cancelled(
                job_id=job_id,
                run_id=run_id,
                runtime_metadata=dict(fetcher.runtime_metadata),
            )
            _raise_if_job_lease_lost(
                job_id=job_id,
                worker_id=worker_id,
                runtime_metadata=dict(fetcher.runtime_metadata),
            )
            if not repo._touch_job_heartbeat(job_id, worker_id=worker_id):
                _raise_if_job_lease_lost(
                    job_id=job_id,
                    worker_id=worker_id,
                    runtime_metadata=dict(fetcher.runtime_metadata),
                )
            repo._emit_job_progress(
                job_id=job_id,
                stage=stage,
                platform="instagram",
                account=account_handle,
                scraped_posts=0,
                scraped_comments=0,
                posts_upserted=0,
                comments_upserted=0,
                activity=activity,
                progress_state=progress_state,
                worker_id=worker_id,
                force=True,
                extra_metadata=progress_metadata_common(),
            )

            def should_poll_cancellation(post_index: int) -> bool:
                return (
                    post_index == 1
                    or post_index == len(target_source_ids)
                    or (post_index - 1) % cancel_check_every_posts == 0
                )

            async def _maybe_refresh_warmup(*, reason: str) -> bool:
                """Phase 1.5: re-run fetcher.warmup() mid-run.

                Returns True when a refresh actually fired so callers can reset
                their counters. Failures are logged and swallowed — a stale
                warmup will still pass through the normal auth-failed circuit.
                """
                nonlocal posts_since_last_warmup, mid_run_warmup_count, last_mid_run_warmup_reason
                try:
                    await fetcher.warmup()
                except InstagramCommentsWarmupError as exc:
                    logger.warning(
                        "Mid-run Instagram comments warmup refresh failed: job_id=%s reason=%s error=%s",
                        job_id,
                        reason,
                        exc,
                    )
                    return False
                except Exception:  # noqa: BLE001
                    logger.warning(
                        "Mid-run Instagram comments warmup refresh raised unexpectedly: job_id=%s reason=%s",
                        job_id,
                        reason,
                        exc_info=True,
                    )
                    return False
                posts_since_last_warmup = 0
                mid_run_warmup_count += 1
                last_mid_run_warmup_reason = reason
                logger.info(
                    "Instagram comments warmup refreshed mid-run: job_id=%s reason=%s count=%d",
                    job_id,
                    reason,
                    mid_run_warmup_count,
                )
                return True

            for index, shortcode in enumerate(target_source_ids, start=1):
                post_started_at = time.monotonic()
                if not repo._touch_job_heartbeat(job_id, worker_id=worker_id):
                    _raise_if_job_lease_lost(
                        job_id=job_id,
                        worker_id=worker_id,
                        runtime_metadata=dict(fetcher.runtime_metadata),
                    )
                if should_poll_cancellation(index):
                    _raise_if_cancelled(
                        job_id=job_id,
                        run_id=run_id,
                        runtime_metadata=dict(fetcher.runtime_metadata),
                    )
                _raise_if_job_lease_lost(
                    job_id=job_id,
                    worker_id=worker_id,
                    runtime_metadata=dict(fetcher.runtime_metadata),
                )
                # Phase 1.5: mid-run warmup refresh trigger. Fires when the
                # consecutive auth-failure threshold is reached OR when
                # mid_run_warmup_every_posts successful posts have elapsed
                # since the last warmup. Skipped for shortcodes pre-classified
                # as already-complete because no fetch will run for them.
                if shortcode not in skipped_complete_target_source_ids:
                    if (
                        mid_run_warmup_auth_threshold > 0
                        and consecutive_post_auth_failures >= mid_run_warmup_auth_threshold
                    ):
                        await _maybe_refresh_warmup(reason="consecutive_auth_failures")
                    elif (
                        mid_run_warmup_every_posts > 0
                        and posts_since_last_warmup >= mid_run_warmup_every_posts
                    ):
                        await _maybe_refresh_warmup(reason="post_count_threshold")
                if shortcode in skipped_complete_target_source_ids:
                    total_elapsed_ms = int((time.monotonic() - post_started_at) * 1000)
                    post_latency_samples.append(
                        {
                            "shortcode": shortcode,
                            "fetch_elapsed_ms": 0,
                            "persist_elapsed_ms": 0,
                            "total_elapsed_ms": total_elapsed_ms,
                            "comments_fetched": 0,
                            "comments_upserted": 0,
                            "comments_marked_missing": 0,
                            "fetch_reason": "already_complete",
                            "is_complete": True,
                            "completion_reason": "already_complete",
                            "reported_comment_count": None,
                        }
                    )
                    processed_posts += 1
                    activity = {
                        "phase": "comments_scrapling_running",
                        "posts_checked": processed_posts,
                        "matched_posts": processed_posts,
                        "saved_posts": processed_posts,
                        "total_posts": len(target_source_ids),
                        **shard_metadata,
                    }
                    repo._emit_job_progress(
                        job_id=job_id,
                        stage=stage,
                        platform="instagram",
                        account=account_handle,
                        scraped_posts=processed_posts,
                        scraped_comments=comments_fetched,
                        posts_upserted=processed_posts,
                        comments_upserted=comments_upserted,
                        activity=activity,
                        progress_state=progress_state,
                        worker_id=worker_id,
                        force=index == len(target_source_ids),
                        extra_metadata=progress_metadata_common(),
                    )
                    continue
                fetch_started_at = time.monotonic()
                fetch_kwargs: dict[str, Any] = {
                    "max_comments": max_comments_per_post,
                    "fetch_replies": fetch_replies,
                    "expected_comment_count": expected_comment_counts_by_shortcode.get(shortcode),
                }
                resume_cursor = top_level_resume_cursors_by_shortcode.get(shortcode)
                if resume_cursor:
                    fetch_kwargs["top_level_cursor"] = resume_cursor
                    resume_cursor_param = top_level_resume_cursor_params_by_shortcode.get(shortcode)
                    if resume_cursor_param:
                        fetch_kwargs["top_level_cursor_param"] = resume_cursor_param
                if fetch_replies:
                    try:
                        persisted_replies_by_parent = _load_persisted_replies_by_parent(
                            account_handle=account_handle,
                            shortcode=shortcode,
                        )
                    except pg.DatabaseServiceUnavailableError as exc:
                        logger.warning(
                            "Continuing comments fetch without persisted reply hydration after database saturation: "
                            "job_id=%s shortcode=%s error=%s",
                            job_id,
                            shortcode,
                            exc,
                        )
                        persisted_replies_by_parent = {}
                    if persisted_replies_by_parent:
                        fetch_kwargs["persisted_replies_by_parent"] = persisted_replies_by_parent
                    prior_incomplete_reason = _prior_incomplete_fetch_reason(job, shortcode)
                    if (
                        prior_incomplete_reason in _REPLY_ONLY_RETRY_REASONS
                        and not resume_cursor
                    ):
                        try:
                            persisted_top_level_comments = _load_persisted_top_level_comments_for_reply_retry(
                                account_handle=account_handle,
                                shortcode=shortcode,
                            )
                        except pg.DatabaseServiceUnavailableError as exc:
                            logger.warning(
                                "Continuing comments fetch without persisted top-level reply retry after database "
                                "saturation: job_id=%s shortcode=%s error=%s",
                                job_id,
                                shortcode,
                                exc,
                            )
                            persisted_top_level_comments = []
                        if persisted_top_level_comments:
                            fetch_kwargs["persisted_top_level_comments"] = persisted_top_level_comments
                            fetch_kwargs["reply_only"] = True
                if reply_resume_cursors_by_parent:
                    fetch_kwargs["reply_resume_cursors"] = reply_resume_cursors_by_parent
                if reply_resume_cursor_params_by_parent:
                    fetch_kwargs["reply_resume_cursor_params"] = reply_resume_cursor_params_by_parent
                result = await fetcher.fetch_comments_for_shortcode(shortcode, **fetch_kwargs)
                fetch_elapsed_ms = int((time.monotonic() - fetch_started_at) * 1000)
                top_level_checkpoint = getattr(result, "top_level_checkpoint", None)
                if isinstance(top_level_checkpoint, dict):
                    top_level_checkpoints_by_shortcode[shortcode] = dict(top_level_checkpoint)
                # Phase 1.7: accumulate per-comment failure attribution into the
                # bounded shard-level list so it lands in scrape_jobs metadata.
                result_failed_comment_ids = getattr(result, "failed_comment_ids", None) or []
                for entry in result_failed_comment_ids:
                    if not isinstance(entry, dict):
                        continue
                    if len(failed_comment_ids) >= _COMMENT_FAILURE_METADATA_MAX_ENTRIES:
                        failed_comment_ids_truncated = True
                        break
                    failed_comment_ids.append(entry)
                _raise_if_job_lease_lost(
                    job_id=job_id,
                    worker_id=worker_id,
                    runtime_metadata=dict(fetcher.runtime_metadata),
                )
                observed_comment_count = _extract_observed_comment_count(result)
                if result.auth_failed and not result.comments:
                    normalized_auth_failed_shortcode = str(shortcode or "").strip()
                    if normalized_auth_failed_shortcode:
                        auth_failed_target_source_ids.append(normalized_auth_failed_shortcode)
                        auth_failed_fetch_reasons[normalized_auth_failed_shortcode] = str(
                            result.fetch_reason or "auth_failed"
                        )
                    consecutive_post_auth_failures += 1
                    should_skip_post_auth_failure = (
                        successful_target_fetches > 0
                        or consecutive_post_auth_failures < post_auth_failure_circuit_limit
                    )
                    if should_skip_post_auth_failure:
                        total_elapsed_ms = int((time.monotonic() - post_started_at) * 1000)
                        post_latency_samples.append(
                            {
                                "shortcode": shortcode,
                                "fetch_elapsed_ms": fetch_elapsed_ms,
                                "persist_elapsed_ms": 0,
                                "total_elapsed_ms": total_elapsed_ms,
                                "comments_fetched": 0,
                                "comments_upserted": 0,
                                "comments_marked_missing": 0,
                                "fetch_reason": result.fetch_reason,
                                "is_complete": False,
                                "completion_reason": "post_auth_failed_skipped",
                                "reported_comment_count": _extract_reported_comment_count(result),
                            }
                        )
                        processed_posts += 1
                        activity = {
                            "phase": "comments_scrapling_running",
                            "posts_checked": processed_posts,
                            "matched_posts": processed_posts,
                            "saved_posts": processed_posts,
                            "total_posts": len(target_source_ids),
                            **shard_metadata,
                        }
                        _attach_incomplete_activity(
                            activity,
                            incomplete_target_source_ids=incomplete_target_source_ids,
                            incomplete_fetch_reasons=incomplete_fetch_reasons,
                            auth_failed_target_source_ids=auth_failed_target_source_ids,
                            auth_failed_fetch_reasons=auth_failed_fetch_reasons,
                        )
                        repo._emit_job_progress(
                            job_id=job_id,
                            stage=stage,
                            platform="instagram",
                            account=account_handle,
                            scraped_posts=processed_posts,
                            scraped_comments=comments_fetched,
                            posts_upserted=processed_posts,
                            comments_upserted=comments_upserted,
                            activity=activity,
                            progress_state=progress_state,
                            worker_id=worker_id,
                            force=index == len(target_source_ids),
                            extra_metadata=progress_metadata_common(),
                        )
                        continue
                    raise CommentsScraplingRuntimeError(
                        f"Instagram auth failed while fetching comments for {shortcode}.",
                        error_code="instagram_comments_auth_failed",
                        retryable=successful_target_fetches > 0,
                        runtime_metadata={
                            "shortcode": shortcode,
                            "fetch_reason": result.fetch_reason,
                            "auth_failed_target_source_ids": list(dict.fromkeys(auth_failed_target_source_ids)),
                            "auth_failed_fetch_reasons": dict(auth_failed_fetch_reasons),
                            "post_auth_failure_circuit_limit": post_auth_failure_circuit_limit,
                        },
                    )
                consecutive_post_auth_failures = 0
                # Phase 1.5: every successful (or non-auth-failed) post advances the
                # mid-run-warmup post counter. Skip-paths above continued before
                # this point so they do not advance the counter spuriously.
                posts_since_last_warmup += 1
                if result.fetch_failed and not result.comments:
                    normalized_incomplete_shortcode = str(shortcode or "").strip()
                    consecutive_post_fetch_failures += 1
                    should_skip_post_fetch_failure = bool(result.retryable) and (
                        successful_target_fetches > 0
                        or consecutive_post_fetch_failures < post_fetch_failure_circuit_limit
                    )
                    if should_skip_post_fetch_failure:
                        if normalized_incomplete_shortcode:
                            incomplete_target_source_ids.append(normalized_incomplete_shortcode)
                            incomplete_fetch_reasons[normalized_incomplete_shortcode] = str(
                                result.fetch_reason or "retryable_post_fetch_failed"
                            )
                        total_elapsed_ms = int((time.monotonic() - post_started_at) * 1000)
                        post_latency_samples.append(
                            {
                                "shortcode": shortcode,
                                "fetch_elapsed_ms": fetch_elapsed_ms,
                                "persist_elapsed_ms": 0,
                                "total_elapsed_ms": total_elapsed_ms,
                                "comments_fetched": 0,
                                "observed_comment_count": 0,
                                "top_level_comment_count": 0,
                                "comments_upserted": 0,
                                "comments_marked_missing": 0,
                                "fetch_reason": result.fetch_reason,
                                "is_complete": False,
                                "completion_reason": "post_fetch_failed_retryable_skipped",
                                "reported_comment_count": _extract_reported_comment_count(result),
                            }
                        )
                        processed_posts += 1
                        activity = {
                            "phase": "comments_scrapling_running",
                            "posts_checked": processed_posts,
                            "matched_posts": processed_posts,
                            "saved_posts": processed_posts,
                            "total_posts": len(target_source_ids),
                            **shard_metadata,
                        }
                        _attach_incomplete_activity(
                            activity,
                            incomplete_target_source_ids=incomplete_target_source_ids,
                            incomplete_fetch_reasons=incomplete_fetch_reasons,
                            auth_failed_target_source_ids=auth_failed_target_source_ids,
                            auth_failed_fetch_reasons=auth_failed_fetch_reasons,
                        )
                        repo._emit_job_progress(
                            job_id=job_id,
                            stage=stage,
                            platform="instagram",
                            account=account_handle,
                            scraped_posts=processed_posts,
                            scraped_comments=comments_fetched,
                            posts_upserted=processed_posts,
                            comments_upserted=comments_upserted,
                            activity=activity,
                            progress_state=progress_state,
                            worker_id=worker_id,
                            force=index == len(target_source_ids),
                            extra_metadata=progress_metadata_common(),
                        )
                        continue
                    raise CommentsScraplingRuntimeError(
                        f"Instagram comments fetch failed for {shortcode}.",
                        error_code=str(result.fetch_reason or "instagram_comments_fetch_failed"),
                        retryable=bool(result.retryable),
                        runtime_metadata={
                            "shortcode": shortcode,
                            "fetch_reason": result.fetch_reason,
                            "post_fetch_failure_circuit_limit": post_fetch_failure_circuit_limit,
                            "consecutive_post_fetch_failures": consecutive_post_fetch_failures,
                            "incomplete_target_source_ids": list(dict.fromkeys(incomplete_target_source_ids)),
                            "incomplete_fetch_reasons": dict(incomplete_fetch_reasons),
                        },
                    )
                consecutive_post_fetch_failures = 0
                is_complete = _comments_scrape_is_complete(
                    result=result,
                    max_comments_per_post=max_comments_per_post,
                )
                completion_reason = (
                    "pagination_exhausted"
                    if is_complete
                    else "max_comments_cap"
                    if max_comments_per_post and len(result.comments) >= max_comments_per_post
                    else "incomplete_fetch"
                )
                persist_started_at = time.monotonic()
                stored_coverage_complete = False
                stored_coverage_reconciled_gap = False
                stored_coverage_terminal_gap = False
                with pg.db_connection(label="instagram-comments-scrapling-persist") as persist_conn:
                    if should_poll_cancellation(index):
                        _raise_if_cancelled(
                            job_id=job_id,
                            run_id=run_id,
                            runtime_metadata=dict(fetcher.runtime_metadata),
                            conn=persist_conn,
                        )
                    _raise_if_job_lease_lost(
                        job_id=job_id,
                        worker_id=worker_id,
                        runtime_metadata=dict(fetcher.runtime_metadata),
                        conn=persist_conn,
                    )
                    persisted = persist_instagram_comments_for_post(
                        account_handle=account_handle,
                        shortcode=shortcode,
                        comments=result.comments,
                        run_id=run_id or None,
                        job_id=job_id,
                        is_complete=is_complete,
                        source_scope=source_scope,
                        enable_media_followups=_config_truthy(config.get("comments_enable_media_followups")),
                        conn=persist_conn,
                    )
                    stored_coverage_complete = _persisted_comment_coverage_is_complete(
                        result=result,
                        stored_total_comments=persisted.stored_total_comments,
                        max_comments_per_post=max_comments_per_post,
                    )
                    stored_coverage_reconciled_gap = _persisted_comment_coverage_gap_is_reconcilable(
                        result=result,
                        stored_total_comments=persisted.stored_total_comments,
                        max_comments_per_post=max_comments_per_post,
                    )
                    stored_coverage_terminal_gap = _terminal_pagination_coverage_gap_is_reconcilable(
                        result=result,
                        stored_total_comments=persisted.stored_total_comments,
                        max_comments_per_post=max_comments_per_post,
                    )
                    if stored_coverage_reconciled_gap or stored_coverage_terminal_gap:
                        repo._reconcile_post_comment_count(
                            platform="instagram",
                            post_db_id=persisted.post_id,
                            conn=persist_conn,
                        )
                    persist_conn.commit()
                _raise_if_job_lease_lost(
                    job_id=job_id,
                    worker_id=worker_id,
                    runtime_metadata=dict(fetcher.runtime_metadata),
                )
                persist_elapsed_ms = int((time.monotonic() - persist_started_at) * 1000)
                total_elapsed_ms = int((time.monotonic() - post_started_at) * 1000)
                if first_post_persisted_at is None:
                    first_post_persisted_at = repo._now_utc()
                effective_is_complete = (
                    is_complete
                    or stored_coverage_complete
                    or stored_coverage_reconciled_gap
                    or stored_coverage_terminal_gap
                )
                if result.fetch_failed and result.retryable and not effective_is_complete:
                    normalized_incomplete_shortcode = str(shortcode or "").strip()
                    if normalized_incomplete_shortcode:
                        incomplete_target_source_ids.append(normalized_incomplete_shortcode)
                        incomplete_fetch_reasons[normalized_incomplete_shortcode] = str(
                            result.fetch_reason or "retryable_incomplete_fetch"
                        )
                if stored_coverage_complete and not is_complete:
                    completion_reason = "stored_comment_coverage_complete"
                elif stored_coverage_reconciled_gap and not is_complete:
                    completion_reason = "stored_comment_coverage_reconciled_gap"
                elif stored_coverage_terminal_gap and not is_complete:
                    completion_reason = "stored_comment_coverage_terminal_gap_reconciled"
                top_level_checkpoint_for_sample = getattr(result, "top_level_checkpoint", None)
                pages_seen_for_post: int | None = None
                last_cursor_param_for_post: str | None = None
                if isinstance(top_level_checkpoint_for_sample, dict):
                    raw_pages_seen = top_level_checkpoint_for_sample.get("pages_seen")
                    if isinstance(raw_pages_seen, int) and raw_pages_seen >= 0:
                        pages_seen_for_post = raw_pages_seen
                    raw_cursor_param = top_level_checkpoint_for_sample.get("last_top_level_cursor_param")
                    if isinstance(raw_cursor_param, str) and raw_cursor_param.strip():
                        last_cursor_param_for_post = raw_cursor_param.strip()
                post_latency_samples.append(
                    {
                        "shortcode": shortcode,
                        "fetch_elapsed_ms": fetch_elapsed_ms,
                        "persist_elapsed_ms": persist_elapsed_ms,
                        "total_elapsed_ms": total_elapsed_ms,
                        "comments_fetched": observed_comment_count,
                        "observed_comment_count": observed_comment_count,
                        "top_level_comment_count": len(result.comments),
                        "comments_upserted": persisted.comments_upserted,
                        "comments_marked_missing": persisted.comments_marked_missing,
                        "fetch_reason": result.fetch_reason,
                        "stored_total_comments": persisted.stored_total_comments,
                        "is_complete": effective_is_complete,
                        "completion_reason": completion_reason,
                        "reported_comment_count": _extract_reported_comment_count(result),
                        # Phase A5 follow-up diagnostics: surface pagination
                        # depth + last cursor direction so operators can
                        # spot repeated_cursor stops without grepping logs.
                        "pages_seen": pages_seen_for_post,
                        "last_cursor_param": last_cursor_param_for_post,
                    }
                )
                processed_posts += 1
                comments_fetched += observed_comment_count
                comments_upserted += persisted.comments_upserted
                successful_target_fetches += 1
                comments_marked_missing += persisted.comments_marked_missing
                mirror_jobs_enqueued += persisted.comment_media_mirror_jobs_enqueued
                mirror_job_enqueue_errors += persisted.comment_media_mirror_job_enqueue_errors
                activity = {
                    "phase": "comments_scrapling_running",
                    "posts_checked": processed_posts,
                    "matched_posts": processed_posts,
                    "saved_posts": processed_posts,
                    "total_posts": len(target_source_ids),
                    **shard_metadata,
                }
                _attach_incomplete_activity(
                    activity,
                    incomplete_target_source_ids=incomplete_target_source_ids,
                    incomplete_fetch_reasons=incomplete_fetch_reasons,
                    auth_failed_target_source_ids=auth_failed_target_source_ids,
                    auth_failed_fetch_reasons=auth_failed_fetch_reasons,
                )
                repo._emit_job_progress(
                    job_id=job_id,
                    stage=stage,
                    platform="instagram",
                    account=account_handle,
                    scraped_posts=processed_posts,
                    scraped_comments=comments_fetched,
                    posts_upserted=processed_posts,
                    comments_upserted=comments_upserted,
                    activity=activity,
                    progress_state=progress_state,
                    worker_id=worker_id,
                    force=index == len(target_source_ids),
                    extra_metadata=progress_metadata_common(),
                )

            retryable_incomplete_targets = list(
                dict.fromkeys([*incomplete_target_source_ids, *auth_failed_target_source_ids])
            )
            # Phase 1.4 / audit: only raise when at least 1/_DEFAULT_INCOMPLETE_RAISE_RATIO
            # of the target list is incomplete. Single-post failures persist their
            # retry targets in metadata via _retry_rebalance_metadata(...) without
            # forcing the whole shard into retrying.
            incomplete_raise_threshold = max(
                _MIN_INCOMPLETE_RAISE_TARGETS,
                len(target_source_ids) // _DEFAULT_INCOMPLETE_RAISE_RATIO,
            )
            if retryable_incomplete_targets and len(retryable_incomplete_targets) >= incomplete_raise_threshold:
                retry_fetch_reasons = {
                    shortcode: incomplete_fetch_reasons.get(shortcode) or auth_failed_fetch_reasons.get(shortcode)
                    for shortcode in retryable_incomplete_targets
                }
                stalled_retry = _incomplete_retry_has_stalled(
                    job=job,
                    attempt_count=attempt_count,
                    retryable_incomplete_targets=retryable_incomplete_targets,
                    retry_fetch_reasons=retry_fetch_reasons,
                    comments_fetched=comments_fetched,
                )
                if stalled_retry:
                    incomplete_retry_stall_metadata = stalled_retry
                    logger.info(
                        "Instagram comments incomplete retry stalled; completing shard without requeue: "
                        "job_id=%s attempt=%s targets=%s",
                        job_id,
                        attempt_count,
                        retryable_incomplete_targets,
                    )
                else:
                    raise CommentsScraplingRuntimeError(
                        "Instagram comments Scrapling job had retryable incomplete posts.",
                        error_code="instagram_comments_incomplete_retryable",
                        retryable=True,
                        runtime_metadata={
                            "incomplete_target_source_ids": retryable_incomplete_targets,
                            "incomplete_fetch_reasons": retry_fetch_reasons,
                            "auth_failed_target_source_ids": list(dict.fromkeys(auth_failed_target_source_ids)),
                            "auth_failed_fetch_reasons": dict(auth_failed_fetch_reasons),
                            "incomplete_raise_threshold": incomplete_raise_threshold,
                            "target_source_ids_count": len(target_source_ids),
                        },
                    )

            return auth_metadata, dict(fetcher.runtime_metadata)
        finally:
            heartbeat_task.cancel()
            with suppress(asyncio.CancelledError):
                await heartbeat_task
            fetcher_metadata = dict(fetcher.runtime_metadata)
            await fetcher.aclose()

    try:
        auth_metadata, fetcher_metadata = asyncio.run(_run_job())
        _raise_if_job_lease_lost(
            job_id=job_id,
            worker_id=worker_id,
            runtime_metadata=dict(fetcher_metadata),
        )
        metadata = {
            "stage": stage,
            "platform": "instagram",
            "account": account_handle,
            "mode": mode,
            "source_scope": source_scope,
            "target_source_ids": target_source_ids,
            "auth_failed_target_source_ids": list(dict.fromkeys(auth_failed_target_source_ids)),
            "auth_failed_fetch_reasons": dict(auth_failed_fetch_reasons),
            **terminal_metadata_common(),
            "stage_counters": {"posts": processed_posts, "comments": comments_fetched},
            "skipped_complete_target_source_ids": skipped_complete_target_source_ids,
            "persist_counters": {
                "posts_upserted": processed_posts,
                "comments_upserted": comments_upserted,
                "comments_marked_missing": comments_marked_missing,
                "comment_media_mirror_jobs_enqueued": mirror_jobs_enqueued,
                "comment_media_mirror_job_enqueue_errors": mirror_job_enqueue_errors,
            },
            "activity": {"phase": "comments_scrapling_end", "last_progress_at": repo._iso(repo._now_utc())},
            "fetch_counters": {
                "request_count": fetcher_metadata.get("request_count", 0),
                "target_posts": len(target_source_ids),
            },
            "auth_context": {
                **auth_context,
                "metadata_source": auth_metadata.get("source"),
            },
            "fetcher_runtime": fetcher_metadata,
        }
        repo._finish_job(
            job_id,
            status="completed",
            items_found=processed_posts + comments_fetched,
            metadata=metadata,
            expected_worker_id=worker_id,
        )
        terminal_status = "completed"
        terminal_error_message = None
    except ScraplingJobLeaseLostError as exc:
        logger.warning(
            "Instagram comments Scrapling job stopped after losing queue lease: job_id=%s status=%s worker_id=%s",
            job_id,
            exc.job_status,
            exc.job_worker_id,
        )
        terminal_status = exc.job_status or "unknown"
        terminal_error_message = str(exc)
    except ScraplingJobCancelledError as exc:
        repo._finish_job(
            job_id,
            status="cancelled",
            items_found=processed_posts + comments_fetched,
            error_message=str(exc),
            metadata={
                "stage": stage,
                "platform": "instagram",
                "account": account_handle,
                "mode": mode,
                "source_scope": source_scope,
                "target_source_ids": target_source_ids,
                "incomplete_target_source_ids": list(dict.fromkeys(incomplete_target_source_ids)),
                "auth_failed_target_source_ids": list(dict.fromkeys(auth_failed_target_source_ids)),
                "skipped_complete_target_source_ids": skipped_complete_target_source_ids,
                "incomplete_fetch_reasons": dict(incomplete_fetch_reasons),
                "auth_failed_fetch_reasons": dict(auth_failed_fetch_reasons),
                **terminal_metadata_common(),
                "cancelled": True,
                "cancel_scope": exc.cancel_scope,
                "job_status_at_cancel": exc.job_status,
                "run_status_at_cancel": exc.run_status,
                "activity": {"phase": "cancelled", "last_progress_at": repo._iso(repo._now_utc())},
                "stage_counters": {"posts": processed_posts, "comments": comments_fetched},
                "persist_counters": {
                    "posts_upserted": processed_posts,
                    "comments_upserted": comments_upserted,
                    "comments_marked_missing": comments_marked_missing,
                    "comment_media_mirror_jobs_enqueued": mirror_jobs_enqueued,
                    "comment_media_mirror_job_enqueue_errors": mirror_job_enqueue_errors,
                },
                "runtime_metadata": exc.runtime_metadata,
                "auth_context": auth_context,
                "fetcher_runtime": fetcher_metadata,
                "retry_rebalance": _retry_rebalance_metadata(
                    comments_shard_count=comments_shard_count,
                    target_source_ids=target_source_ids,
                    processed_posts=processed_posts,
                    incomplete_target_source_ids=incomplete_target_source_ids,
                    auth_failed_target_source_ids=auth_failed_target_source_ids,
                ),
            },
            last_error_code="instagram_comments_scrapling_cancelled",
            last_error_class=exc.__class__.__name__,
            expected_worker_id=worker_id,
        )
        terminal_status = "cancelled"
        terminal_error_message = str(exc)
    except Exception as exc:  # noqa: BLE001
        runtime_error_code = str(getattr(exc, "error_code", "") or "").strip().lower()
        runtime_metadata = repo._metadata_dict(getattr(exc, "runtime_metadata", None))
        retry_incomplete_targets = [
            str(item or "").strip()
            for item in runtime_metadata.get("incomplete_target_source_ids") or []
            if str(item or "").strip()
        ]
        retry_incomplete_targets = list(dict.fromkeys(retry_incomplete_targets))
        database_capacity_error = _is_database_capacity_error(exc)
        transport_error = _is_comments_transport_error(exc)
        error_code = (
            "instagram_comments_database_capacity"
            if database_capacity_error
            else "instagram_comments_transport_error"
            if transport_error
            else runtime_error_code or "instagram_comments_scrapling_failed"
        )
        error_class = str(getattr(exc, "error_class", "") or exc.__class__.__name__).strip()
        retryable = bool(getattr(exc, "retryable", False)) or database_capacity_error or transport_error
        can_retry = retryable and attempt_count < max_attempts
        retry_rebalance = _retry_rebalance_metadata(
            comments_shard_count=comments_shard_count,
            target_source_ids=target_source_ids,
            processed_posts=processed_posts,
            incomplete_target_source_ids=retry_incomplete_targets or incomplete_target_source_ids,
            auth_failed_target_source_ids=auth_failed_target_source_ids,
        )
        retry_target_source_ids = retry_incomplete_targets or [
            str(item or "").strip()
            for item in ((retry_rebalance or {}).get("remaining_target_source_ids") or [])
            if str(item or "").strip()
        ]
        retry_target_source_ids = list(dict.fromkeys(retry_target_source_ids))
        if can_retry and retry_target_source_ids:
            try:
                repo._update_job_config(
                    job_id,
                    config_updates={
                        "target_source_ids": retry_target_source_ids,
                        "comments_shard_target_count": len(retry_target_source_ids),
                        "comments_retry_incomplete": True,
                        "comments_retry_incomplete_source_job_id": job_id,
                    },
                )
            except pg.DatabaseServiceUnavailableError as update_exc:
                logger.warning(
                    "Deferred comments retry target narrowing after database saturation: job_id=%s error=%s",
                    job_id,
                    update_exc,
                )
        next_available_at = (
            repo._now_utc() + timedelta(seconds=repo._retry_backoff_seconds(attempt_count)) if can_retry else None
        )
        repo._finish_job(
            job_id,
            status="retrying" if can_retry else "failed",
            items_found=processed_posts + comments_fetched,
            error_message=str(exc),
            metadata={
                "stage": stage,
                "platform": "instagram",
                "account": account_handle,
                "mode": mode,
                "source_scope": source_scope,
                "target_source_ids": target_source_ids,
                "incomplete_target_source_ids": list(dict.fromkeys(incomplete_target_source_ids)),
                "auth_failed_target_source_ids": list(dict.fromkeys(auth_failed_target_source_ids)),
                "skipped_complete_target_source_ids": skipped_complete_target_source_ids,
                "incomplete_fetch_reasons": dict(incomplete_fetch_reasons),
                "auth_failed_fetch_reasons": dict(auth_failed_fetch_reasons),
                **terminal_metadata_common(),
                "error_code": error_code,
                "error_class": error_class,
                "retryable": retryable,
                "can_retry": can_retry,
                "retry_exhausted": bool(retryable and not can_retry),
                "attempt_count": attempt_count,
                "max_attempts": max_attempts,
                "activity": {"phase": "failed", "last_progress_at": repo._iso(repo._now_utc())},
                "persist_counters": {
                    "posts_upserted": processed_posts,
                    "comments_upserted": comments_upserted,
                },
                "runtime_metadata": runtime_metadata or getattr(exc, "runtime_metadata", None),
                "auth_context": auth_context,
                "fetcher_runtime": fetcher_metadata,
                "retry_rebalance": retry_rebalance,
            },
            last_error_code=error_code,
            last_error_class=error_class,
            next_available_at=next_available_at,
            expected_worker_id=worker_id,
        )
        terminal_status = "retrying" if can_retry else "failed"
        terminal_error_message = str(exc)
        if terminal_status == "failed":
            try:
                _abort_queued_sibling_shards_after_run_fatal_error(
                    repo=repo,
                    run_id=run_id,
                    failed_job_id=job_id,
                    stage=stage,
                    account_handle=account_handle,
                    mode=mode,
                    source_scope=source_scope,
                    error_code=error_code,
                    error_class=error_class,
                    error_message=str(exc),
                )
            except pg.DatabaseServiceUnavailableError as abort_exc:
                logger.warning(
                    "Deferred sibling comments shard abort after database saturation: run_id=%s job_id=%s error=%s",
                    run_id,
                    job_id,
                    abort_exc,
                )
    finally:
        if run_id:
            try:
                finalize_result = repo._finalize_run_status(run_id)
                if isinstance(finalize_result, dict) and finalize_result.get("finalize_deferred"):
                    logger.warning(
                        "Retrying deferred comments run-status reconciliation: run_id=%s error=%s",
                        run_id,
                        finalize_result.get("error"),
                    )
                    time.sleep(2.0)
                    repo._finalize_run_status(run_id, force_recompute=True)
            except pg.DatabaseServiceUnavailableError as exc:
                logger.warning(
                    "Deferred final comments run-status reconciliation after database saturation: run_id=%s error=%s",
                    run_id,
                    exc,
                )

    try:
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
    except pg.DatabaseServiceUnavailableError as exc:
        logger.warning(
            "Returning degraded comments job summary after database saturation: job_id=%s error=%s",
            job_id,
            exc,
        )
        return {
            "id": job_id,
            "run_id": run_id or None,
            "platform": "instagram",
            "job_type": str(job.get("job_type") or "comments").strip() or "comments",
            "status": terminal_status or "unknown",
            "items_found": processed_posts + comments_fetched,
            "error_message": terminal_error_message,
            "metadata": {
                "degraded_summary": True,
                "database_service_unavailable": True,
                **terminal_metadata_common(),
            },
        }
