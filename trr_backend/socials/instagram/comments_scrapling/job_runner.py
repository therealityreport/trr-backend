from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import uuid
from collections import Counter
from collections.abc import Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from trr_backend.db import pg
from trr_backend.socials.instagram.comments_scrapling.counts import (
    child_reply_count,
    parent_comment_count,
    parentless_reply_ids,
)
from trr_backend.socials.instagram.comments_scrapling.fetcher import (
    BROWSER_SESSION_INVALIDATED_REASON,
    InstagramCommentsFetchResult,
    InstagramCommentsScraplingFetcher,
    InstagramCommentsWarmupError,
    normalize_comments_load_strategy,
)
from trr_backend.socials.instagram.comments_scrapling.persistence import (
    PersistedInstagramComments,
    persist_instagram_comments_for_post,
)
from trr_backend.socials.instagram.comments_scrapling.proxy import select_comments_proxy
from trr_backend.socials.instagram.comments_scrapling.session import resolve_comments_scrapling_session
from trr_backend.socials.instagram.scraper import InstagramComment

logger = logging.getLogger("socials.instagram.comments_scrapling.job_runner")


class _LifecycleProxy:
    def __getattr__(self, name: str) -> Any:
        import trr_backend.socials.control_plane.run_lifecycle as lifecycle

        return getattr(lifecycle, name)


lifecycle = _LifecycleProxy()


_RUN_FATAL_COMMENTS_ERROR_CODES = {
    "instagram_comments_auth_failed",
    "instagram_comments_browser_session_invalidated",
    "instagram_comments_warmup_auth_failed",
    "instagram_comments_warmup_no_cookies",
    "instagram_comments_warmup_transport_error",
    "instagram_comments_endpoint_auth_blocked",
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
_RECONCILABLE_REPORTED_GAP_MAX_DEFAULT = 1
_RECONCILABLE_REPORTED_GAP_RATIO_DEFAULT = 0.0
_RECONCILABLE_REPORTED_GAP_REASONS = {
    "auth_relay_fallback_recovered",
    "auth_rendered_fallback_recovered",
    "coauthor_auth_relay_fallback_recovered",
    "coauthor_auth_rendered_fallback_recovered",
    "hidden_comments_unavailable_reconciled",
    "hidden_comments_unresolved",
    "reply_tail_budget_exhausted",
    "reply_tail_incomplete",
}
_RECONCILABLE_REPLY_ONLY_AUTH_BLOCK_REASONS = {
    "html_challenge_or_auth_required",
    "instagram_comments_endpoint_auth_blocked",
}
_COAUTHOR_STATUS_ONLY_FETCH_REASONS = {
    "comments_endpoint_status_only",
    "coauthor_comments_endpoint_empty",
}
_COAUTHOR_AUTH_RENDERED_FALLBACK_ENV = "SOCIAL_INSTAGRAM_COMMENTS_COAUTHOR_AUTH_RENDERED_FALLBACK"
_AUTH_RENDERED_FALLBACK_ENV = "SOCIAL_INSTAGRAM_COMMENTS_AUTH_RENDERED_FALLBACK"
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
_INCOMPLETE_RETRY_STALL_ATTEMPTS_DEFAULT = 2
_INCOMPLETE_RETRY_STALL_REASONS = {
    *_RECONCILABLE_REPORTED_GAP_REASONS,
    "hidden_comments_unresolved",
    "hidden_comments_unavailable",
    "hidden_comments_blocked",
    "html_challenge_or_auth_required",
    *_COAUTHOR_STATUS_ONLY_FETCH_REASONS,
    "pagination_deadline_exceeded",
    "persisted_reply_topology_gap",
    "reply_tail_incomplete",
    "reply_tail_budget_exhausted",
    "transport_error",
    "transport_timeout",
}
_TERMINAL_MISSING_CLASSIFIED_REASON = "coverage_terminal_missing_classified"
_PARENTLESS_REPLY_ATTACH_FAILED_REASON = "parentless_reply_attach_failed"
_PERSISTED_REPLY_TOPOLOGY_GAP_REASON = "persisted_reply_topology_gap"
_BROWSER_SESSION_INVALIDATED_ERROR_CODE = "instagram_comments_browser_session_invalidated"


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
    if not job_id:
        return
    while True:
        try:
            lifecycle.touch_job_heartbeat(job_id, worker_id=worker_id)
        except pg.DatabaseServiceUnavailableError as exc:
            logger.warning(
                "Skipping comments job heartbeat after database saturation: job_id=%s error=%s",
                job_id,
                exc,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Comments job heartbeat failed: job_id=%s error=%s", job_id, exc)
        await asyncio.sleep(interval_seconds)


def _result_parentless_reply_ids(result: InstagramCommentsFetchResult) -> list[str]:
    comments = getattr(result, "comments", None)
    return parentless_reply_ids(comments) if isinstance(comments, list) else []


def _classified_missing_count(result: InstagramCommentsFetchResult) -> int:
    metadata = getattr(result, "diagnostic_metadata", None)
    if not isinstance(metadata, dict):
        return 0
    reason_counts = metadata.get("missing_reason_counts")
    if not isinstance(reason_counts, dict):
        return 0
    total = 0
    for value in reason_counts.values():
        try:
            parsed = int(value or 0)
        except (TypeError, ValueError):
            parsed = 0
        total += max(0, parsed)
    return total


def _metadata_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return False


def _metadata_indicates_browser_session_invalidated(metadata: Any, *, _depth: int = 0) -> bool:
    if _depth > 4 or not isinstance(metadata, Mapping):
        return False
    for key, value in metadata.items():
        normalized_key = str(key or "").strip().lower()
        if normalized_key in {"session_invalidated", "browser_session_invalidated"} and _metadata_bool(value):
            return True
        if (
            normalized_key in {"reason", "fetch_reason", "stop_reason", "last_error_code", "error_code"}
            and str(value or "").strip() == BROWSER_SESSION_INVALIDATED_REASON
        ):
            return True
        if normalized_key == "text_markers" and isinstance(value, Mapping):
            if _metadata_bool(value.get("session_invalidated")):
                return True
        if isinstance(value, Mapping) and _metadata_indicates_browser_session_invalidated(
            value,
            _depth=_depth + 1,
        ):
            return True
        if isinstance(value, (list, tuple)):
            for item in value[:8]:
                if _metadata_indicates_browser_session_invalidated(item, _depth=_depth + 1):
                    return True
    return False


def _fetch_result_indicates_browser_session_invalidated(result: Any) -> bool:
    return str(
        getattr(result, "fetch_reason", "") or ""
    ).strip() == BROWSER_SESSION_INVALIDATED_REASON or _metadata_indicates_browser_session_invalidated(
        _fetch_result_diagnostic_metadata(result)
    )


def _persisted_reply_gap_total(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _has_persisted_reply_topology_gap(value: Any) -> bool:
    return _persisted_reply_gap_total(value) > 0


def _reply_only_fast_path_reason(
    *,
    prior_incomplete_reason: str | None,
    incomplete_fill_enabled: bool,
    resume_cursor: str | None,
) -> str | None:
    if str(resume_cursor or "").strip():
        return None
    normalized_prior_reason = str(prior_incomplete_reason or "").strip().lower()
    if normalized_prior_reason in _REPLY_ONLY_RETRY_REASONS:
        return normalized_prior_reason
    if normalized_prior_reason == _PERSISTED_REPLY_TOPOLOGY_GAP_REASON:
        return _PERSISTED_REPLY_TOPOLOGY_GAP_REASON
    if incomplete_fill_enabled:
        return _PERSISTED_REPLY_TOPOLOGY_GAP_REASON
    return None


def _persisted_reply_topology_metadata(persisted: PersistedInstagramComments) -> dict[str, Any]:
    samples = list(getattr(persisted, "stored_reply_gap_samples", None) or [])
    return {
        "stored_parent_comments": int(getattr(persisted, "stored_parent_comments", 0) or 0),
        "stored_child_replies": int(getattr(persisted, "stored_child_replies", 0) or 0),
        "expected_child_replies": int(getattr(persisted, "expected_child_replies", 0) or 0),
        "stored_reply_gap_total": int(getattr(persisted, "stored_reply_gap_total", 0) or 0),
        "stored_reply_gap_parent_count": int(getattr(persisted, "stored_reply_gap_parent_count", 0) or 0),
        "stored_reply_gap_samples": samples,
    }


def _comments_scrape_is_complete(
    *,
    result: InstagramCommentsFetchResult,
    max_comments_per_post: int,
) -> bool:
    if result.fetch_failed or result.auth_failed:
        return False
    if _result_parentless_reply_ids(result):
        return False
    reported_comment_count = _extract_reported_comment_count(result)
    observed_comment_count = _extract_observed_comment_count(result)
    if reported_comment_count is not None and observed_comment_count >= reported_comment_count:
        return True
    if str(getattr(result, "fetch_reason", "") or "") == "hidden_comments_unavailable_reconciled":
        return observed_comment_count > 0
    if str(getattr(result, "fetch_reason", "") or "") == _TERMINAL_MISSING_CLASSIFIED_REASON:
        return (
            reported_comment_count is not None
            and observed_comment_count > 0
            and observed_comment_count + _classified_missing_count(result) >= reported_comment_count
        )
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
    stored_reply_gap_total: int = 0,
    max_comments_per_post: int,
) -> bool:
    if result.auth_failed:
        return False
    if _result_parentless_reply_ids(result):
        return False
    if _has_persisted_reply_topology_gap(stored_reply_gap_total):
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
    stored_reply_gap_total: int = 0,
    max_comments_per_post: int,
) -> bool:
    if result.auth_failed:
        return False
    if _result_parentless_reply_ids(result):
        return False
    if _has_persisted_reply_topology_gap(stored_reply_gap_total):
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
    stored_reply_gap_total: int = 0,
    max_comments_per_post: int,
) -> bool:
    if result.auth_failed:
        return False
    if _result_parentless_reply_ids(result):
        return False
    if _has_persisted_reply_topology_gap(stored_reply_gap_total):
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
            os.environ.get("SOCIAL_INSTAGRAM_COMMENTS_TERMINAL_GAP_MAX") or _TERMINAL_COVERAGE_GAP_MAX_DEFAULT
        )
    except (TypeError, ValueError):
        max_absolute_gap = _TERMINAL_COVERAGE_GAP_MAX_DEFAULT
    max_absolute_gap = max(0, min(max_absolute_gap, 500))
    try:
        max_ratio = float(
            os.environ.get("SOCIAL_INSTAGRAM_COMMENTS_TERMINAL_GAP_RATIO") or _TERMINAL_COVERAGE_GAP_RATIO_DEFAULT
        )
    except (TypeError, ValueError):
        max_ratio = _TERMINAL_COVERAGE_GAP_RATIO_DEFAULT
    max_ratio = max(0.0, min(max_ratio, 0.25))
    ratio_gap = int(target_count * max_ratio)
    if ratio_gap < target_count * max_ratio:
        ratio_gap += 1
    return unresolved_gap <= max(max_absolute_gap, ratio_gap)


def _reply_only_auth_blocked_coverage_gap_is_reconcilable(
    *,
    result: InstagramCommentsFetchResult,
    stored_total_comments: int,
    max_comments_per_post: int,
    reply_only: bool = False,
) -> bool:
    if not result.auth_failed:
        return False
    if _result_parentless_reply_ids(result):
        return False
    reason = str(getattr(result, "fetch_reason", "") or "").strip()
    if reason not in _RECONCILABLE_REPLY_ONLY_AUTH_BLOCK_REASONS:
        return False
    diagnostic_metadata = getattr(result, "diagnostic_metadata", None)
    strategy_decision = (
        diagnostic_metadata.get("strategy_decision") if isinstance(diagnostic_metadata, Mapping) else None
    )
    result_reply_only = isinstance(strategy_decision, Mapping) and bool(strategy_decision.get("reply_only"))
    if not (reply_only or result_reply_only):
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


def _classify_unavailable_instagram_comment_gap(
    *,
    conn: Any,
    post_id: str,
    result: InstagramCommentsFetchResult,
    stored_total_comments: int,
    max_comments_per_post: int,
    run_id: str | None,
    job_id: str | None,
    reason: str,
) -> int:
    normalized_post_id = str(post_id or "").strip()
    if not normalized_post_id:
        return 0
    reported_comment_count = _extract_reported_comment_count(result)
    if reported_comment_count is None:
        return 0
    target_count = reported_comment_count
    if max_comments_per_post > 0:
        target_count = min(target_count, max_comments_per_post)
    stored_total = max(0, int(stored_total_comments or 0))
    if stored_total >= target_count:
        return 0
    normalized_reason = str(reason or "unavailable_comment_gap").strip() or "unavailable_comment_gap"
    normalized_run_id = _uuid_or_none(run_id)
    normalized_job_id = _uuid_or_none(job_id)
    with pg.db_cursor(conn=conn, label="instagram_comments_classify_unavailable_gap") as cur:
        count_row = (
            pg.fetch_one_with_cursor(
                cur,
                """
            select
              count(*) filter (where coalesce(c.is_missing, false) = true)::int as classified_missing_comments,
              max(p.season_id::text) as season_id,
              max(
                ltrim(lower(coalesce(nullif(p.source_account, ''), nullif(p.username, ''), '')), '@')
              ) as source_account,
              max(coalesce(p.fb_comment_count, 0))::int as facebook_comment_count
            from social.instagram_posts p
            left join social.instagram_comments c
              on c.post_id = p.id
             and coalesce(c.source_snapshot_type, '') = 'classified_missing_comments'
            where p.id = %s::uuid
            group by p.id
            """,
                [normalized_post_id],
            )
            or {}
        )
        existing_missing = max(0, int(count_row.get("classified_missing_comments") or 0))
        facebook_comment_count = max(0, int(count_row.get("facebook_comment_count") or 0))
        residual = max(0, target_count - stored_total - existing_missing - facebook_comment_count)
        if residual <= 0:
            return 0
        season_id = str(count_row.get("season_id") or "").strip() or None
        source_account = str(count_row.get("source_account") or "").strip().lstrip("@") or None
        rows = pg.fetch_all_with_cursor(
            cur,
            """
            insert into social.instagram_comments (
              comment_id,
              post_id,
              username,
              text,
              likes,
              is_reply,
              reply_count,
              raw_data,
              season_id,
              job_id,
              source_account,
              is_missing,
              missing_at,
              first_seen_at,
              last_seen_at,
              last_seen_run_id,
              source_snapshot_type,
              status
            )
            select
              concat('__missing__:', %s::text, ':', gs.i::text) as comment_id,
              %s::uuid as post_id,
              '__classified_missing__' as username,
              'Classified unavailable Instagram comment' as text,
              0 as likes,
              false as is_reply,
              0 as reply_count,
              jsonb_build_object(
                'classification_reason', %s::text,
                'reported_comment_count', %s::int,
                'stored_total_comments', %s::int,
                'facebook_comment_count', %s::int,
                'run_id', %s::text,
                'job_id', %s::text
              ) as raw_data,
              %s::uuid as season_id,
              %s::uuid as job_id,
              %s::text as source_account,
              true as is_missing,
              now() as missing_at,
              now() as first_seen_at,
              now() as last_seen_at,
              %s::uuid as last_seen_run_id,
              'classified_missing_comments' as source_snapshot_type,
              'Unavailable' as status
            from generate_series(%s::int + 1, %s::int + %s::int) as gs(i)
            on conflict (post_id, comment_id) do nothing
            returning id::text
            """,
            [
                normalized_post_id,
                normalized_post_id,
                normalized_reason,
                target_count,
                stored_total,
                facebook_comment_count,
                normalized_run_id,
                normalized_job_id,
                season_id,
                normalized_job_id,
                source_account,
                normalized_run_id,
                existing_missing,
                existing_missing,
                residual,
            ],
        )
    return len(rows)


def _uuid_or_none(value: Any) -> str | None:
    normalized = str(value or "").strip()
    if not normalized:
        return None
    try:
        return str(uuid.UUID(normalized))
    except ValueError:
        return None


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
    # max_attempts is the queue's stop condition. Do not inflate it here, or
    # retryable incomplete shards can keep extending their own budget forever.
    normalized_max_attempts = max(1, max_attempts if max_attempts is not None else _QUEUE_DEFAULT_MAX_ATTEMPTS)
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


def _config_env_truthy(value: Any, env_name: str, *, default: bool = True) -> bool:
    raw = value
    if raw is None:
        raw = os.getenv(env_name)
    if raw is None:
        return default
    if isinstance(raw, bool):
        return raw
    return str(raw or "").strip().lower() in {"1", "true", "yes", "on"}


def _metadata_indicates_collaborator_post(metadata: Any) -> bool:
    if not isinstance(metadata, dict):
        return False
    if bool(metadata.get("is_collaborator_post") or metadata.get("is_collaborator")):
        return True
    collaborators = metadata.get("collaborator_handles") or metadata.get("collaborators")
    return isinstance(collaborators, list) and any(str(item or "").strip() for item in collaborators)


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


def _build_cumulative_counters(
    job_id: str,
    *,
    posts: int,
    comments: int,
    comments_upserted: int,
    comments_inserted: int = 0,
    comments_refreshed: int = 0,
    comments_changed: int = 0,
) -> dict[str, int]:
    """Preserve prior retry-attempt progress before the same job row restarts."""
    try:
        row = pg.fetch_one("select metadata from social.scrape_jobs where id = %s::uuid", [job_id]) or {}
    except Exception as exc:  # noqa: BLE001
        logger.warning("Unable to load prior comments counters for retry progress: job_id=%s error=%s", job_id, exc)
        row = {}
    metadata = lifecycle.metadata_dict(row.get("metadata"))
    prior = lifecycle.metadata_dict(metadata.get("cumulative_counters"))

    def as_int(value: Any) -> int:
        try:
            return max(0, int(value or 0))
        except (TypeError, ValueError):
            return 0

    return {
        "posts": as_int(prior.get("posts")) + as_int(posts),
        "comments": as_int(prior.get("comments")) + as_int(comments),
        "comments_upserted": as_int(prior.get("comments_upserted")) + as_int(comments_upserted),
        "comments_inserted": as_int(prior.get("comments_inserted")) + as_int(comments_inserted),
        "comments_refreshed": as_int(prior.get("comments_refreshed")) + as_int(comments_refreshed),
        "comments_changed": as_int(prior.get("comments_changed")) + as_int(comments_changed),
    }


def _cumulative_items_found(counters: dict[str, Any]) -> int:
    try:
        posts = max(0, int(counters.get("posts") or 0))
    except (TypeError, ValueError):
        posts = 0
    try:
        comments = max(0, int(counters.get("comments") or 0))
    except (TypeError, ValueError):
        comments = 0
    return posts + comments


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
            "net::err_timed_out",
            "net::err_connection_closed",
            "net::err_connection_reset",
            "timed out",
            "timeout",
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
    fetchable_comments_sql = getattr(repo, "_instagram_fetchable_comments_sql", None)
    reported_comments_expr = (
        fetchable_comments_sql("p") if callable(fetchable_comments_sql) else repo._instagram_reported_comments_sql("p")
    )
    # For coauthor posts (e.g. @peacock owner / @thetraitorsus collaborator),
    # the row materialized under the collaborator's profile commonly has
    # comments_count = 0 because the metric lives on the owner's row. Filtering
    # to lower(p.source_account) = profile_account would silently zero out
    # expected_comments, which in turn keeps the status-only / coauthor
    # fallback chain in fetcher.py from ever firing. Take the max across all
    # rows for the shortcode so we honor whichever row was crawled with full
    # metadata.
    rows = pg.fetch_all(
        f"""
        with requested as (
          select
            nullif(shortcode, '')::text as shortcode,
            ordinality::int as sort_order
          from unnest(%s::text[]) with ordinality as request(shortcode, ordinality)
          where nullif(shortcode, '') is not null
        ),
        shortcode_max as (
          select
            r.shortcode,
            max(({reported_comments_expr})::bigint) as reported_comments
          from requested r
          join social.instagram_posts p on p.shortcode = r.shortcode
          group by r.shortcode
        )
        select
          r.shortcode,
          coalesce(sm.reported_comments, 0)::bigint as reported_comments
        from requested r
        left join shortcode_max sm on sm.shortcode = r.shortcode
        order by r.sort_order
        """,
        [requested],
    )
    counts: dict[str, int] = {}
    for row in rows:
        shortcode = str(row.get("shortcode") or "").strip()
        reported = _safe_int(row.get("reported_comments"))
        if shortcode and reported is not None:
            counts[shortcode] = reported
    return counts


def _load_comment_target_metadata(
    *,
    account_handle: str,
    target_source_ids: list[str],
) -> dict[str, dict[str, Any]]:
    normalized_account = _normalize_instagram_handle(account_handle)
    requested = list(dict.fromkeys(str(item or "").strip() for item in target_source_ids if str(item or "").strip()))
    if not normalized_account or not requested:
        return {}
    rows = pg.fetch_all(
        """
        with requested as (
          select
            nullif(shortcode, '')::text as shortcode,
            ordinality::int as sort_order
          from unnest(%s::text[]) with ordinality as request(shortcode, ordinality)
          where nullif(shortcode, '') is not null
        ),
        materialized_rows as (
          select
            r.shortcode,
            r.sort_order,
            p.id::text as materialized_post_id,
            nullif(p.source_account, '') as source_account,
            nullif(p.username, '') as username,
            nullif(coalesce(to_jsonb(p) ->> 'owner_username', ''), '') as owner_username,
            coalesce(p.collaborators, '[]'::jsonb) as collaborators,
            coalesce(to_jsonb(p) -> 'collaborators_detail', '[]'::jsonb) as collaborators_detail,
            nullif(p.media_type, '') as media_type,
            nullif(coalesce(to_jsonb(p) ->> 'product_type', p.raw_data ->> 'product_type', ''), '') as product_type,
            'materialized'::text as profile_source_surface,
            case
              when ltrim(lower(coalesce(nullif(p.source_account, ''), '')), '@') = %s then 'profile_source_account'
              when ltrim(
                lower(coalesce(nullif(p.username, ''), nullif(to_jsonb(p) ->> 'owner_username', ''), '')),
                '@'
              ) = %s
                then 'owner_username'
              else 'shortcode'
            end as profile_match_mode,
            null::text as catalog_collaborator_handle,
            p.posted_at,
            case
              when ltrim(lower(coalesce(nullif(p.source_account, ''), '')), '@') = %s then 4
              when ltrim(
                lower(coalesce(nullif(p.username, ''), nullif(to_jsonb(p) ->> 'owner_username', ''), '')),
                '@'
              ) = %s then 3
              else 1
            end as profile_match_rank
          from social.instagram_posts p
          join requested r on r.shortcode = p.shortcode
        ),
        catalog_collaborator_rows as (
          select
            r.shortcode,
            r.sort_order,
            coalesce(materialized_post.materialized_post_id, cp.id::text) as materialized_post_id,
            nullif(cp.source_account, '') as source_account,
            nullif(cp.source_account, '') as username,
            nullif(
              coalesce(
                nullif(cp.owner_username, ''),
                nullif(cp.raw_data ->> 'ownerUsername', ''),
                nullif(cp.raw_data ->> 'owner_username', ''),
                nullif(cp.source_account, '')
              ),
              ''
            ) as owner_username,
            coalesce(nullif(cp.collaborators, '[]'::jsonb), jsonb_build_array(m.collaborator_handle)) as collaborators,
            coalesce(
              nullif(to_jsonb(cp) -> 'collaborators_detail', '[]'::jsonb),
              nullif(cp.raw_data -> 'collaborators_detail', '[]'::jsonb),
              jsonb_build_array(jsonb_build_object('username', m.collaborator_handle, 'source', m.collaborator_source))
            ) as collaborators_detail,
            nullif(cp.media_type, '') as media_type,
            nullif(coalesce(to_jsonb(cp) ->> 'product_type', cp.raw_data ->> 'product_type', ''), '') as product_type,
            'catalog'::text as profile_source_surface,
            'catalog_collaborator'::text as profile_match_mode,
            nullif(m.collaborator_handle, '') as catalog_collaborator_handle,
            cp.posted_at,
            5 as profile_match_rank
          from social.instagram_account_catalog_post_collaborators m
          join social.instagram_account_catalog_posts cp
            on cp.id = m.catalog_post_id
          join requested r
            on r.shortcode = cp.source_id
          left join lateral (
            select mp.id::text as materialized_post_id
            from social.instagram_posts mp
            where mp.shortcode = cp.source_id
            order by mp.posted_at desc nulls last, mp.id desc
            limit 1
          ) materialized_post on true
          where ltrim(lower(coalesce(nullif(m.collaborator_handle, ''), '')), '@') = %s
            and ltrim(lower(coalesce(nullif(cp.source_account, ''), '')), '@') <> %s
        ),
        candidate_rows as (
          select * from materialized_rows
          union all
          select * from catalog_collaborator_rows
        ),
        ranked_rows as (
          select
            *,
            row_number() over (
              partition by shortcode
              order by
                profile_match_rank desc,
                posted_at desc nulls last,
                materialized_post_id desc
            ) as row_number
          from candidate_rows
        )
        select
          shortcode,
          materialized_post_id,
          source_account,
          username,
          owner_username,
          collaborators,
          collaborators_detail,
          media_type,
          product_type,
          profile_source_surface,
          profile_match_mode,
          catalog_collaborator_handle
        from ranked_rows
        where row_number = 1
        order by sort_order
        """,
        [
            requested,
            normalized_account,
            normalized_account,
            normalized_account,
            normalized_account,
            normalized_account,
            normalized_account,
        ],
    )
    target_metadata: dict[str, dict[str, Any]] = {}
    for row in rows:
        shortcode = str(row.get("shortcode") or "").strip()
        if not shortcode:
            continue
        collaborators = _metadata_string_list(row.get("collaborators"))
        collaborator_handles = list(
            dict.fromkeys(
                handle
                for handle in (
                    *(_normalize_instagram_handle(item) for item in collaborators),
                    *_metadata_handle_list(row.get("collaborators")),
                    *_metadata_handle_list(row.get("collaborators_detail")),
                    _normalize_instagram_handle(row.get("catalog_collaborator_handle")),
                )
                if handle
            )
        )
        source_account = str(row.get("source_account") or "").strip() or None
        username = str(row.get("username") or "").strip() or None
        owner_username = str(row.get("owner_username") or "").strip() or username or source_account
        normalized_owner = _normalize_instagram_handle(owner_username)
        normalized_source_account = _normalize_instagram_handle(source_account)
        is_collaborator_post = bool(
            normalized_owner
            and normalized_owner != normalized_account
            and (
                normalized_account in collaborator_handles
                or normalized_source_account == normalized_account
                or _normalize_instagram_handle(row.get("catalog_collaborator_handle")) == normalized_account
                or row.get("profile_match_mode") == "catalog_collaborator"
            )
        )
        has_collaborators = bool(collaborator_handles)
        target_metadata[shortcode] = {
            "source_id": shortcode,
            "account_handle": normalized_account,
            "profile_account": normalized_account,
            "selected_profile_account": normalized_account,
            "source_account": source_account,
            "username": username,
            "owner_username": owner_username,
            "caption_author": owner_username,
            "caption_writer": owner_username,
            "original_author": owner_username,
            "collaborators": collaborators,
            "collaborator_handles": collaborator_handles,
            "collaborators_detail": row.get("collaborators_detail") or [],
            "media_type": row.get("media_type"),
            "product_type": row.get("product_type"),
            "materialized_post_id": row.get("materialized_post_id"),
            "profile_source_surface": row.get("profile_source_surface") or "materialized",
            "profile_match_mode": row.get("profile_match_mode") or "shortcode",
            "is_collaborator_post": is_collaborator_post,
            "is_collaborator": is_collaborator_post,
            "has_collaborators": has_collaborators,
        }
    return target_metadata


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
            owner_is_private=(bool(row.get("author_is_private")) if row.get("author_is_private") is not None else None),
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
        str(item or "").strip() for item in (incomplete_target_source_ids or []) if str(item or "").strip()
    ]
    retry_targets.extend(
        str(item or "").strip() for item in (auth_failed_target_source_ids or []) if str(item or "").strip()
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
    items = [item for item in (checkpoint_metadata.get("items") or []) if isinstance(item, dict)]
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
        runtime_items = [item for item in (checkpoint_metadata.get("items") or []) if isinstance(item, dict)]
    merged_by_shortcode: dict[str, dict[str, Any]] = {}
    for checkpoint in [*runtime_items, *top_level_checkpoints.values()]:
        shortcode = str(
            checkpoint.get("target_shortcode") or checkpoint.get("source_id") or checkpoint.get("shortcode") or ""
        ).strip()
        if shortcode:
            merged_by_shortcode[shortcode] = dict(checkpoint)
    items = list(merged_by_shortcode.values())
    return {
        "total_count": int(
            checkpoint_metadata.get("total_count")
            if isinstance(checkpoint_metadata, dict)
            else len(items) or len(items)
        ),
        "retained_count": len(items),
        "dropped_count": int(
            checkpoint_metadata.get("dropped_count") if isinstance(checkpoint_metadata, dict) else 0 or 0
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
        shortcode = str(item.get("target_shortcode") or item.get("source_id") or item.get("shortcode") or "").strip()
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
        shortcode = str(item.get("target_shortcode") or item.get("source_id") or item.get("shortcode") or "").strip()
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
    values: list[str] = []
    for item in value:
        text: str | None = None
        if isinstance(item, dict):
            for key in ("username", "handle", "user_name", "collaborator_handle", "source_account"):
                candidate = str(item.get(key) or "").strip()
                if candidate:
                    text = candidate
                    break
            if text is None:
                for key in ("user", "owner", "profile"):
                    nested = item.get(key)
                    if not isinstance(nested, dict):
                        continue
                    for nested_key in ("username", "handle", "user_name"):
                        candidate = str(nested.get(nested_key) or "").strip()
                        if candidate:
                            text = candidate
                            break
                    if text is not None:
                        break
        else:
            candidate = str(item or "").strip()
            if candidate:
                text = candidate
        if text:
            values.append(text)
    return list(dict.fromkeys(values))


def _normalize_instagram_handle(value: Any) -> str:
    return str(value or "").strip().strip("/").lower().lstrip("@")


def _metadata_handle_list(value: Any) -> list[str]:
    if isinstance(value, dict):
        candidates = [value]
    elif isinstance(value, list):
        candidates = list(value)
    elif isinstance(value, str):
        candidates = value.replace(",", " ").replace(";", " ").split()
    else:
        candidates = []

    handles: list[str] = []
    seen: set[str] = set()
    for item in candidates:
        raw_value: Any = item
        if isinstance(item, dict):
            raw_value = None
            for key in ("username", "handle", "user_name", "collaborator_handle", "source_account"):
                if str(item.get(key) or "").strip():
                    raw_value = item.get(key)
                    break
            if raw_value is None:
                for key in ("user", "owner", "profile"):
                    nested = item.get(key)
                    if not isinstance(nested, dict):
                        continue
                    for nested_key in ("username", "handle", "user_name"):
                        if str(nested.get(nested_key) or "").strip():
                            raw_value = nested.get(nested_key)
                            break
                    if raw_value is not None:
                        break
        handle = _normalize_instagram_handle(raw_value)
        if not handle or handle in seen:
            continue
        seen.add(handle)
        handles.append(handle)
    return handles


def _fetch_result_diagnostic_metadata(result: Any) -> dict[str, Any] | None:
    metadata = getattr(result, "diagnostic_metadata", None)
    if isinstance(metadata, dict) and metadata:
        return dict(metadata)
    legacy_metadata = getattr(result, "metadata", None)
    if isinstance(legacy_metadata, dict) and legacy_metadata:
        return dict(legacy_metadata)
    return None


def _comment_phase_name(comment: Any, *, fallback: str | None = None) -> str | None:
    phase = str(getattr(comment, "phase", "") or "").strip().lower()
    if phase:
        return phase
    if bool(getattr(comment, "is_reply", False)) or str(getattr(comment, "parent_comment_id", "") or "").strip():
        return "child"
    if bool(getattr(comment, "is_ranked", False)):
        return "ranked"
    return fallback


def _walk_comment_tree(comments: list[Any], *, fallback_phase: str | None = None) -> list[Any]:
    walked: list[Any] = []
    for comment in comments or []:
        walked.append(comment)
        child_fallback = "child"
        walked.extend(_walk_comment_tree(list(getattr(comment, "replies", []) or []), fallback_phase=child_fallback))
    return walked


def _counter_from_mapping(value: Any) -> Counter[str]:
    counts: Counter[str] = Counter()
    if not isinstance(value, dict):
        return counts
    for key, raw_count in value.items():
        name = str(key or "").strip()
        if not name:
            continue
        try:
            count = int(raw_count or 0)
        except (TypeError, ValueError):
            count = 0
        if count > 0:
            counts[name] += count
    return counts


def _comment_capture_metadata_from_fetch_result(result: InstagramCommentsFetchResult) -> dict[str, Any]:
    metadata = _fetch_result_diagnostic_metadata(result) or {}
    phase_counts: Counter[str] = Counter()
    for key in ("phase_counts", "comment_phase_counts"):
        phase_counts.update(_counter_from_mapping(metadata.get(key)))
    if not phase_counts:
        for comment in _walk_comment_tree(list(getattr(result, "comments", []) or [])):
            phase = _comment_phase_name(comment)
            if phase:
                phase_counts[phase] += 1

    cursor_param_counts: Counter[str] = Counter()
    cursor_shape_counts: Counter[str] = Counter()
    for key in ("cursor_param_counts", "cursor_diagnostics"):
        value = metadata.get(key)
        if isinstance(value, dict):
            cursor_param_counts.update(_counter_from_mapping(value.get("cursor_param_counts")))
            cursor_shape_counts.update(_counter_from_mapping(value.get("cursor_shape_counts")))
            cursor_shape_counts.update(_counter_from_mapping(value.get("cursor_shapes")))
        else:
            cursor_param_counts.update(_counter_from_mapping(value))
    cursor_shape_counts.update(_counter_from_mapping(metadata.get("cursor_shape_counts")))
    cursor_shape_counts.update(_counter_from_mapping(metadata.get("cursor_shapes")))

    checkpoint = getattr(result, "top_level_checkpoint", None)
    if isinstance(checkpoint, dict):
        for key in ("last_top_level_cursor_param", "next_top_level_cursor_param"):
            cursor_param = str(checkpoint.get(key) or "").strip()
            if cursor_param:
                cursor_param_counts[cursor_param] += 1
        diagnostic_metadata = checkpoint.get("diagnostic_metadata")
        if isinstance(diagnostic_metadata, dict):
            cursor_shape_counts.update(_counter_from_mapping(diagnostic_metadata.get("cursor_shape_counts")))
            cursor_shape_counts.update(_counter_from_mapping(diagnostic_metadata.get("cursor_shapes")))

    sample: dict[str, Any] = {}
    if isinstance(checkpoint, dict):
        sample = {
            key: checkpoint.get(key)
            for key in (
                "target_shortcode",
                "source_id",
                "stop_reason",
                "last_top_level_cursor_param",
                "next_top_level_cursor_param",
                "pages_seen",
            )
            if checkpoint.get(key) is not None
        }
    return {
        "phase_counts": dict(phase_counts),
        "cursor_param_counts": dict(cursor_param_counts),
        "cursor_shape_counts": dict(cursor_shape_counts),
        "sample": sample,
    }


def _post_comments_audit_table_available(conn: Any) -> bool:
    try:
        with pg.db_cursor(conn=conn, label="instagram_post_comments_audit_available") as cur:
            row = (
                pg.fetch_one_with_cursor(
                    cur,
                    "select to_regclass('social.instagram_post_comments_audit') is not null as available",
                    [],
                )
                or {}
            )
        return bool(row.get("available"))
    except Exception as exc:  # noqa: BLE001
        logger.debug("Instagram post comments audit table probe failed: %s", exc)
        return False


def _first_comment_cursor_payload(comments: list[Any]) -> dict[str, Any]:
    for comment in _walk_comment_tree(comments):
        payload = getattr(comment, "cursor_payload", None)
        if isinstance(payload, dict) and payload:
            return dict(payload)
    return {}


def _insert_instagram_post_comments_audit(
    *,
    conn: Any,
    run_id: str | None,
    job_id: str | None,
    post_id: str,
    shortcode: str,
    account_handle: str,
    result: InstagramCommentsFetchResult,
    capture_metadata: Mapping[str, Any],
    fetched_parent_count: int,
    fetched_child_count: int,
    fetched_total_count: int,
    target_metadata: Mapping[str, Any] | None = None,
) -> None:
    if not post_id or not _post_comments_audit_table_available(conn):
        return
    phase_counts = _counter_from_mapping(capture_metadata.get("phase_counts"))
    status_counts: Counter[str] = Counter()
    covered_count = 0
    spam_count = 0
    inactive_count = 0
    for comment in _walk_comment_tree(list(getattr(result, "comments", []) or [])):
        status = str(getattr(comment, "status", "") or "Active").strip() or "Active"
        status_counts[status] += 1
        if bool(getattr(comment, "is_covered", False)):
            covered_count += 1
        if bool(getattr(comment, "did_report_as_spam", False)):
            spam_count += 1
        if status != "Active":
            inactive_count += 1
    checkpoint = getattr(result, "top_level_checkpoint", None)
    checkpoint = checkpoint if isinstance(checkpoint, dict) else {}
    cursor_payload = _first_comment_cursor_payload(list(getattr(result, "comments", []) or []))
    cursor_payload.update(
        {
            key: value
            for key, value in {
                "stop_reason": getattr(result, "fetch_reason", None),
                "top_level_checkpoint": checkpoint or None,
            }.items()
            if value
        }
    )
    reported_total = _extract_reported_comment_count(result) or 0
    reported_fb = (
        _safe_int((target_metadata or {}).get("fb_comment_count"))
        or _safe_int((target_metadata or {}).get("reported_fb_comment_count"))
        or 0
    )
    fetched_fb_crossposts = int(phase_counts.get("fb_crosspost") or 0)
    fetched_instagram_parent_count = max(0, int(fetched_parent_count or 0) - fetched_fb_crossposts)
    fetched_instagram_total_count = max(0, int(fetched_total_count or 0) - fetched_fb_crossposts)
    cursor_param = (
        str(
            checkpoint.get("next_top_level_cursor_param") or checkpoint.get("last_top_level_cursor_param") or ""
        ).strip()
        or None
    )
    cursor_min_id = (
        str(checkpoint.get("next_top_level_cursor") or checkpoint.get("last_top_level_cursor") or "").strip() or None
    )
    params = [
        str(run_id or "").strip() or None,
        str(job_id or "").strip() or None,
        post_id,
        str(shortcode or "").strip() or None,
        str(account_handle or "").strip() or None,
        reported_total,
        reported_fb,
        fetched_instagram_total_count,
        fetched_instagram_parent_count,
        max(0, int(fetched_child_count or 0)),
        int(phase_counts.get("ranked") or 0),
        int(phase_counts.get("headload") or 0),
        int(phase_counts.get("fb_crosspost") or 0),
        int(phase_counts.get("child") or 0),
        json.dumps(dict(phase_counts), sort_keys=True),
        covered_count,
        spam_count,
        inactive_count,
        json.dumps(dict(status_counts), sort_keys=True),
        str(getattr(result, "fetch_reason", "") or "").strip() or None,
        cursor_min_id,
        cursor_param,
        json.dumps(cursor_payload, sort_keys=True, default=str),
        str((getattr(result, "diagnostic_metadata", None) or {}).get("comment_filter_param") or "").strip() or None,
        max(0, reported_total - fetched_instagram_parent_count),
        max(0, int(phase_counts.get("child") or 0) - int(fetched_child_count or 0)),
        max(0, reported_total - fetched_instagram_total_count),
    ]
    try:
        with pg.db_cursor(conn=conn, label="instagram_post_comments_audit_insert") as cur:
            pg.fetch_one_with_cursor(
                cur,
                """
                insert into social.instagram_post_comments_audit (
                  scrape_run_id,
                  scrape_job_id,
                  post_id,
                  shortcode,
                  source_account,
                  reported_comment_count,
                  reported_fb_comment_count,
                  fetched_comment_count,
                  fetched_parent_comment_count,
                  fetched_child_comment_count,
                  phase_ranked_count,
                  phase_headload_count,
                  phase_fb_crosspost_count,
                  phase_child_count,
                  phase_counts,
                  covered_comment_count,
                  spam_report_count,
                  inactive_status_count,
                  status_counts,
                  cursor_stop_reason,
                  cursor_min_id,
                  cursor_param,
                  cursor_payload,
                  comment_filter_param,
                  reported_parent_gap_count,
                  reported_child_gap_count,
                  reported_total_gap_count
                )
                values (
                  %s::uuid, %s::uuid, %s::uuid, %s, %s, %s, %s, %s, %s, %s,
                  %s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s::jsonb, %s, %s,
                  %s, %s::jsonb, %s, %s, %s, %s
                )
                returning id::text
                """,
                params,
            )
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Instagram post comments audit insert failed: job_id=%s shortcode=%s error=%s",
            job_id,
            shortcode,
            exc,
        )


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


def _retryable_incomplete_target_source_ids(
    *,
    incomplete_target_source_ids: Sequence[str],
    incomplete_fetch_reasons: Mapping[str, str],
    auth_failed_target_source_ids: Sequence[str] | None = None,
) -> list[str]:
    targets: list[str] = []
    for item in incomplete_target_source_ids:
        target = str(item or "").strip()
        if not target:
            continue
        reason = str(incomplete_fetch_reasons.get(target) or "").strip()
        if reason == _TERMINAL_MISSING_CLASSIFIED_REASON:
            continue
        targets.append(target)
    for item in auth_failed_target_source_ids or []:
        target = str(item or "").strip()
        if target:
            targets.append(target)
    return list(dict.fromkeys(targets))


def _incomplete_retry_has_stalled(
    *,
    job: dict[str, Any],
    attempt_count: int,
    retryable_incomplete_targets: list[str],
    retry_fetch_reasons: dict[str, str | None],
    comments_fetched: int,
    zero_comment_incomplete_targets: list[str] | None = None,
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
    if not set(current_targets).issubset(set(prior_targets)):
        return None
    current_reason_by_target = {
        target: str(retry_fetch_reasons.get(target) or "").strip().lower() for target in current_targets
    }
    normalized_reasons = set(current_reason_by_target.values())
    if not normalized_reasons or not normalized_reasons.issubset(_INCOMPLETE_RETRY_STALL_REASONS):
        return None
    prior_reason_by_target = {
        target: _prior_incomplete_fetch_reason(job, target).strip().lower() for target in current_targets
    }
    prior_reasons = set(prior_reason_by_target.values())
    if not prior_reasons or not prior_reasons.issubset(_INCOMPLETE_RETRY_STALL_REASONS):
        return None
    prior_items_found = _safe_int(job.get("items_found")) or 0
    return {
        "stalled": True,
        "attempt_count": attempt_count,
        "stall_attempts": stall_attempts,
        "target_source_ids": current_targets,
        "zero_comment_target_source_ids": [
            target
            for target in current_targets
            if target
            in {str(item or "").strip() for item in (zero_comment_incomplete_targets or []) if str(item or "").strip()}
        ],
        "fetch_reasons": {target: retry_fetch_reasons.get(target) for target in current_targets},
        "prior_fetch_reasons": prior_reason_by_target,
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
        "comments_auth_validation_mode": metadata.get("comments_auth_validation_mode"),
        "comments_profile_graphql_validation": metadata.get("comments_profile_graphql_validation"),
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
        finish_job = getattr(repo, "_finish_job", None)
        finish = finish_job if callable(finish_job) else lifecycle.finish_job
        finish(
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

    job_runner_started_at = lifecycle.now_utc()
    job_id = str(job.get("id") or "").strip()
    run_id = str(job.get("run_id") or "").strip()
    config = dict(job.get("config") or {})
    account_handle = str(config.get("account") or "").strip().lower().lstrip("@")
    stage = str(config.get("stage") or repo.INSTAGRAM_COMMENTS_SCRAPLING_STAGE).strip().lower()
    mode = str(config.get("mode") or "profile").strip().lower()
    source_scope = str(config.get("source_scope") or "network").strip().lower() or "network"
    max_comments_per_post = max(0, int(config.get("max_comments_per_post") or 0))
    fetch_replies = bool(config.get("fetch_replies", True))
    comments_load_strategy = normalize_comments_load_strategy(config.get("comments_load_strategy"))
    single_session_load_all = comments_load_strategy == "single_session_load_all"
    default_comments_session_scope = (
        "post_continuous"
        if single_session_load_all and mode == "single_post"
        else "profile_single_worker"
        if single_session_load_all
        else "cursor_api_worker"
    )
    comments_session_scope = (
        str(config.get("comments_session_scope") or default_comments_session_scope).strip()
        or default_comments_session_scope
    )
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
    incomplete_fill_enabled = str(config.get("target_filter") or "").strip().lower() == "incomplete" or _config_truthy(
        config.get("incomplete_fill")
    )
    skip_complete_retry_targets = (
        any(
            _config_truthy(config.get(flag))
            for flag in (
                "comments_retry_rebalance",
                "comments_retry_incomplete",
                "comments_target_gap_repair",
                "comments_skip_complete_targets",
            )
        )
        or incomplete_fill_enabled
        or str(job.get("status") or "").strip().lower() == "retrying"
        or attempt_count > 1
    )
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
        "comments_load_strategy": comments_load_strategy,
        "comments_session_scope": comments_session_scope,
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
    try:
        target_metadata_by_shortcode = _load_comment_target_metadata(
            account_handle=account_handle,
            target_source_ids=target_source_ids,
        )
    except Exception as exc:  # noqa: BLE001
        target_metadata_by_shortcode = {}
        logger.warning(
            "Continuing Instagram comments job without target metadata: job_id=%s error=%s",
            job_id,
            exc,
            exc_info=True,
        )

    progress_state = lifecycle.new_job_progress_state()
    processed_posts = 0
    comments_upserted = 0
    comments_inserted = 0
    comments_refreshed = 0
    comments_changed = 0
    comments_fetched = 0
    parent_comments_fetched = 0
    child_replies_fetched = 0
    parentless_replies_fetched = 0
    comments_marked_missing = 0
    mirror_jobs_enqueued = 0
    mirror_job_enqueue_errors = 0
    saved_once_per_post_target_source_ids: list[str] = []
    activity: dict[str, Any] = {
        "phase": "comments_scrapling_start",
        "posts_checked": 0,
        "matched_posts": 0,
        "saved_posts": 0,
        "total_posts": len(target_source_ids),
        "comments_load_strategy": comments_load_strategy,
        "comments_session_scope": comments_session_scope,
        **shard_metadata,
    }
    fetcher_metadata: dict[str, Any] = {}
    top_level_checkpoints_by_shortcode: dict[str, dict[str, Any]] = {}
    comment_phase_counts: Counter[str] = Counter()
    comment_cursor_param_counts: Counter[str] = Counter()
    comment_cursor_shape_counts: Counter[str] = Counter()
    comment_capture_samples: list[dict[str, Any]] = []
    post_latency_samples: list[dict[str, Any]] = []
    incomplete_target_source_ids: list[str] = []
    incomplete_fetch_reasons: dict[str, str] = {}
    zero_comment_incomplete_target_source_ids: list[str] = []
    coauthor_status_only_target_source_ids: list[str] = []
    post_fetch_failure_metadata_by_shortcode: dict[str, dict[str, Any]] = {}
    auth_failed_target_source_ids: list[str] = []
    auth_failed_fetch_reasons: dict[str, str] = {}
    consecutive_post_auth_failures = 0
    consecutive_post_fetch_failures = 0
    successful_target_fetches = 0
    post_auth_failure_circuit_limit = _safe_int(config.get("post_auth_failure_circuit_limit")) or 3
    post_fetch_failure_circuit_limit = _safe_int(config.get("post_fetch_failure_circuit_limit")) or 3
    # Phase 1.5: mid-run warmup refresh state.
    mid_run_warmup_auth_threshold = (
        _safe_int(config.get("comments_warmup_refresh_auth_threshold")) or _DEFAULT_MID_RUN_WARMUP_AUTH_THRESHOLD
    )
    mid_run_warmup_every_posts = (
        _safe_int(config.get("comments_warmup_refresh_every_posts")) or _DEFAULT_MID_RUN_WARMUP_EVERY_POSTS
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
    comments_endpoint_probe: dict[str, Any] = {}
    current_target_fetch: dict[str, Any] = {}
    terminal_status = str(job.get("status") or "").strip().lower() or None
    terminal_error_message: str | None = None

    def progress_metadata_common() -> dict[str, Any]:
        return {
            "persist_counters": {
                "posts_upserted": processed_posts,
                "comments_upserted": comments_upserted,
                "comments_inserted": comments_inserted,
                "comments_refreshed": comments_refreshed,
                "comments_changed": comments_changed,
                "db_rows_written": comments_upserted,
                "new_instagram_comments_saved": comments_inserted,
                "existing_comment_rows_seen": comments_refreshed,
                "existing_comment_rows_updated": max(0, comments_changed - comments_inserted),
                "comments_marked_missing": comments_marked_missing,
                "comment_media_mirror_jobs_enqueued": mirror_jobs_enqueued,
                "comment_media_mirror_job_enqueue_errors": mirror_job_enqueue_errors,
            },
            "fetch_counters": {
                "comments_fetched": comments_fetched,
                "parent_comments_fetched": parent_comments_fetched,
                "child_replies_fetched": child_replies_fetched,
                "parentless_replies_fetched": parentless_replies_fetched,
            },
            "comments_load_strategy": comments_load_strategy,
            "comments_session_scope": comments_session_scope,
            "comments_strategy": {
                "selected": comments_load_strategy,
                "session_scope": comments_session_scope,
                "api_first": True,
                "rendered_dom_canonical": False,
                "single_session_load_all": single_session_load_all,
                "same_job_auth_retry_suppressed": single_session_load_all,
                "saved_once_per_post": {
                    "enabled": True,
                    "count": len(saved_once_per_post_target_source_ids),
                    "target_source_ids": list(dict.fromkeys(saved_once_per_post_target_source_ids)),
                },
            },
            "comments_endpoint_probe": dict(comments_endpoint_probe),
            "current_target_fetch": dict(current_target_fetch) if current_target_fetch else None,
            "post_auth_failures": {
                "target_source_ids": list(dict.fromkeys(auth_failed_target_source_ids)),
                "fetch_reasons": dict(auth_failed_fetch_reasons),
                "circuit_limit": post_auth_failure_circuit_limit,
            },
            "post_fetch_failures": {
                "target_source_ids": list(dict.fromkeys(incomplete_target_source_ids)),
                "fetch_reasons": dict(incomplete_fetch_reasons),
                "reason_counts": dict(Counter(incomplete_fetch_reasons.values())),
                "zero_comment_target_source_ids": list(dict.fromkeys(zero_comment_incomplete_target_source_ids)),
                "coauthor_status_only_target_source_ids": list(dict.fromkeys(coauthor_status_only_target_source_ids)),
                "target_metadata": dict(post_fetch_failure_metadata_by_shortcode),
                "circuit_limit": post_fetch_failure_circuit_limit,
            },
            "incomplete_target_source_ids": list(dict.fromkeys(incomplete_target_source_ids)),
            "incomplete_fetch_reasons": dict(incomplete_fetch_reasons),
            "zero_comment_incomplete_target_source_ids": list(dict.fromkeys(zero_comment_incomplete_target_source_ids)),
            "coauthor_status_only_target_source_ids": list(dict.fromkeys(coauthor_status_only_target_source_ids)),
            "auth_failed_target_source_ids": list(dict.fromkeys(auth_failed_target_source_ids)),
            "auth_failed_fetch_reasons": dict(auth_failed_fetch_reasons),
            "target_metadata_summary": {
                "loaded": bool(target_metadata_by_shortcode),
                "count": len(target_metadata_by_shortcode),
                "collaborator_posts": sum(
                    1 for item in target_metadata_by_shortcode.values() if bool(item.get("is_collaborator_post"))
                ),
                "posts_with_collaborators": sum(
                    1 for item in target_metadata_by_shortcode.values() if bool(item.get("has_collaborators"))
                ),
            },
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
            "comment_capture": {
                "phase_counts": dict(comment_phase_counts),
                "cursor_param_counts": dict(comment_cursor_param_counts),
                "cursor_shape_counts": dict(comment_cursor_shape_counts),
                "samples": list(comment_capture_samples),
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
                "job_runner_started_at": lifecycle.format_time(job_runner_started_at),
                "warmup_completed_at": lifecycle.format_time(warmup_completed_at),
                "first_post_persisted_at": lifecycle.format_time(first_post_persisted_at),
            },
        }

    # Single event loop: fetcher is created, warmed up, used for all
    # shortcodes, and closed within one asyncio.run(). The httpx client
    # and Patchright browser share the same loop lifetime.
    async def _run_job() -> tuple[dict[str, Any], dict[str, Any]]:
        nonlocal processed_posts, comments_upserted, comments_inserted, comments_refreshed
        nonlocal comments_changed, comments_fetched, parent_comments_fetched, child_replies_fetched
        nonlocal parentless_replies_fetched, comments_marked_missing
        nonlocal mirror_jobs_enqueued, mirror_job_enqueue_errors
        nonlocal activity, fetcher_metadata
        nonlocal consecutive_post_auth_failures, consecutive_post_fetch_failures, successful_target_fetches
        nonlocal warmup_completed_at, first_post_persisted_at, auth_context, comments_endpoint_probe
        nonlocal current_target_fetch
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
                warmup_completed_at = lifecycle.now_utc()
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
            comments_auth_validation_mode = (
                str(
                    config.get("comments_auth_validation_mode")
                    or config.get("auth_validation_mode")
                    or auth_context.get("comments_auth_validation_mode")
                    or auth_metadata.get("comments_auth_validation_mode")
                    or "comments_endpoint"
                )
                .strip()
                .lower()
                or "comments_endpoint"
            )
            auth_context["comments_auth_validation_mode"] = comments_auth_validation_mode
            auth_context["comments_profile_graphql_validation"] = comments_auth_validation_mode == "graphql_profile"
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
            if not lifecycle.touch_job_heartbeat(job_id, worker_id=worker_id):
                _raise_if_job_lease_lost(
                    job_id=job_id,
                    worker_id=worker_id,
                    runtime_metadata=dict(fetcher.runtime_metadata),
                )
            probe_method = getattr(fetcher, "validate_comments_endpoint", None)
            if (
                comments_auth_validation_mode == "comments_endpoint"
                and callable(probe_method)
                and asyncio.iscoroutinefunction(probe_method)
            ):
                comments_endpoint_probe = await probe_method(target_source_ids[0], mode=comments_auth_validation_mode)
                probe_status = (
                    str(comments_endpoint_probe.get("status") or comments_endpoint_probe.get("result") or "")
                    .strip()
                    .lower()
                )
                probe_reason = str(comments_endpoint_probe.get("reason") or "").strip()
                if probe_status == "auth_blocked":
                    runtime_metadata = {
                        **dict(fetcher.runtime_metadata),
                        "comments_endpoint_probe": dict(comments_endpoint_probe),
                    }
                    if (
                        probe_reason == BROWSER_SESSION_INVALIDATED_REASON
                        or _metadata_indicates_browser_session_invalidated(comments_endpoint_probe)
                    ):
                        raise CommentsScraplingRuntimeError(
                            "Instagram comments browser session was invalidated before target processing.",
                            error_code=_BROWSER_SESSION_INVALIDATED_ERROR_CODE,
                            retryable=False,
                            runtime_metadata=runtime_metadata,
                        )
                    raise CommentsScraplingRuntimeError(
                        "Instagram comments endpoint auth validation failed before target processing.",
                        error_code="instagram_comments_endpoint_auth_blocked",
                        retryable=False,
                        runtime_metadata=runtime_metadata,
                    )
                if probe_status == "transport_blocked":
                    comments_endpoint_probe = {
                        **dict(comments_endpoint_probe),
                        "advisory_continue": True,
                    }
                    logger.warning(
                        "Instagram comments endpoint transport validation was inconclusive before target "
                        "processing; continuing with target fetch budget. job_id=%s run_id=%s reason=%s",
                        job_id,
                        run_id,
                        comments_endpoint_probe.get("reason"),
                    )
            lifecycle.emit_job_progress(
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
                if not lifecycle.touch_job_heartbeat(job_id, worker_id=worker_id):
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
                    elif mid_run_warmup_every_posts > 0 and posts_since_last_warmup >= mid_run_warmup_every_posts:
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
                            "comments_load_strategy": comments_load_strategy,
                            "comments_session_scope": comments_session_scope,
                            "saved_once_per_post": False,
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
                    lifecycle.emit_job_progress(
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
                    "load_strategy": comments_load_strategy,
                }
                target_metadata = target_metadata_by_shortcode.get(shortcode)
                if target_metadata:
                    fetch_kwargs["target_metadata"] = dict(target_metadata)
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
                    reply_only_reason = _reply_only_fast_path_reason(
                        prior_incomplete_reason=_prior_incomplete_fetch_reason(job, shortcode),
                        incomplete_fill_enabled=incomplete_fill_enabled,
                        resume_cursor=resume_cursor,
                    )
                    if reply_only_reason:
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
                endpoint_status = (
                    str(comments_endpoint_probe.get("status") or comments_endpoint_probe.get("result") or "")
                    .strip()
                    .lower()
                )
                endpoint_reason = str(comments_endpoint_probe.get("reason") or "").strip() or None
                current_target_fetch = {
                    "phase": "fetching",
                    "shortcode": shortcode,
                    "post_index": index,
                    "total_posts": len(target_source_ids),
                    "started_at": lifecycle.format_time(lifecycle.now_utc()),
                    "expected_comment_count": expected_comment_counts_by_shortcode.get(shortcode),
                    "max_comments_per_post": max_comments_per_post,
                    "fetch_replies": fetch_replies,
                    "comments_load_strategy": comments_load_strategy,
                    "comments_session_scope": comments_session_scope,
                    "reply_only": bool(fetch_kwargs.get("reply_only")),
                    "auth_probe_status": endpoint_status or None,
                    "auth_probe_reason": endpoint_reason,
                    "fallback_expected": bool(comments_endpoint_probe.get("advisory_continue")),
                    "fallback_reason": comments_endpoint_probe.get("advisory_reason"),
                }
                if isinstance(target_metadata, Mapping):
                    current_target_fetch.update(
                        {
                            "owner_username": target_metadata.get("owner_username"),
                            "caption_author": target_metadata.get("caption_author")
                            or target_metadata.get("caption_writer")
                            or target_metadata.get("original_author"),
                            "collaborator_handles": list(target_metadata.get("collaborator_handles") or []),
                            "is_collaborator_post": bool(target_metadata.get("is_collaborator_post")),
                        }
                    )
                activity = {
                    "phase": "comments_scrapling_fetching",
                    "posts_checked": processed_posts,
                    "matched_posts": processed_posts,
                    "saved_posts": processed_posts,
                    "total_posts": len(target_source_ids),
                    "current_shortcode": shortcode,
                    "current_post_index": index,
                    "fallback_expected": bool(comments_endpoint_probe.get("advisory_continue")),
                    **shard_metadata,
                }
                result = await fetcher.fetch_comments_for_shortcode(shortcode, **fetch_kwargs)
                fetch_elapsed_ms = int((time.monotonic() - fetch_started_at) * 1000)
                current_target_fetch = {
                    **current_target_fetch,
                    "phase": "fetched",
                    "fetched_at": lifecycle.format_time(lifecycle.now_utc()),
                    "fetch_elapsed_ms": fetch_elapsed_ms,
                    "comments_fetched": len(result.comments),
                    "observed_comment_count": _extract_observed_comment_count(result),
                    "parent_comments_fetched": parent_comment_count(result.comments),
                    "child_replies_fetched": child_reply_count(result.comments),
                    "fetch_failed": bool(result.fetch_failed),
                    "auth_failed": bool(result.auth_failed),
                    "retryable": bool(result.retryable),
                    "fetch_reason": result.fetch_reason,
                }
                activity = {
                    "phase": "comments_scrapling_fetched",
                    "posts_checked": processed_posts,
                    "matched_posts": processed_posts,
                    "saved_posts": processed_posts,
                    "total_posts": len(target_source_ids),
                    "current_shortcode": shortcode,
                    "current_post_index": index,
                    "comments_fetched": len(result.comments),
                    "fetch_reason": result.fetch_reason,
                    **shard_metadata,
                }
                top_level_checkpoint = getattr(result, "top_level_checkpoint", None)
                if isinstance(top_level_checkpoint, dict):
                    top_level_checkpoints_by_shortcode[shortcode] = dict(top_level_checkpoint)
                capture_metadata = _comment_capture_metadata_from_fetch_result(result)
                comment_phase_counts.update(_counter_from_mapping(capture_metadata.get("phase_counts")))
                comment_cursor_param_counts.update(_counter_from_mapping(capture_metadata.get("cursor_param_counts")))
                comment_cursor_shape_counts.update(_counter_from_mapping(capture_metadata.get("cursor_shape_counts")))
                capture_sample = capture_metadata.get("sample")
                if isinstance(capture_sample, dict) and capture_sample:
                    if len(comment_capture_samples) >= 25:
                        comment_capture_samples.pop(0)
                    comment_capture_samples.append(dict(capture_sample))
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
                if result.auth_failed and _fetch_result_indicates_browser_session_invalidated(result):
                    raise CommentsScraplingRuntimeError(
                        f"Instagram comments browser session was invalidated while fetching {shortcode}.",
                        error_code=_BROWSER_SESSION_INVALIDATED_ERROR_CODE,
                        retryable=False,
                        runtime_metadata={
                            "shortcode": shortcode,
                            "fetch_reason": result.fetch_reason,
                            "auth_failed": True,
                            "fetcher_runtime": dict(fetcher.runtime_metadata),
                            "diagnostic_metadata": _fetch_result_diagnostic_metadata(result),
                        },
                    )
                observed_comment_count = _extract_observed_comment_count(result)
                fetched_parent_count = parent_comment_count(result.comments)
                fetched_child_count = child_reply_count(result.comments)
                fetched_parentless_count = len(_result_parentless_reply_ids(result))
                if result.auth_failed and not result.comments:
                    normalized_auth_failed_shortcode = str(shortcode or "").strip()
                    if normalized_auth_failed_shortcode:
                        auth_failed_target_source_ids.append(normalized_auth_failed_shortcode)
                        auth_failed_fetch_reasons[normalized_auth_failed_shortcode] = str(
                            result.fetch_reason or "auth_failed"
                        )
                    consecutive_post_auth_failures += 1
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
                            "comments_load_strategy": comments_load_strategy,
                            "comments_session_scope": comments_session_scope,
                            "saved_once_per_post": False,
                            "same_job_auth_retry_suppressed": single_session_load_all,
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
                    lifecycle.emit_job_progress(
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
                    if (
                        post_auth_failure_circuit_limit > 0
                        and consecutive_post_auth_failures >= post_auth_failure_circuit_limit
                    ):
                        remaining_target_source_ids = [
                            str(item or "").strip() for item in target_source_ids[index:] if str(item or "").strip()
                        ]
                        auth_failed_targets = list(dict.fromkeys(auth_failed_target_source_ids))
                        incomplete_targets = list(dict.fromkeys([*auth_failed_targets, *remaining_target_source_ids]))
                        raise CommentsScraplingRuntimeError(
                            "Instagram comments auth failed repeatedly while fetching target posts.",
                            error_code="instagram_comments_auth_failed",
                            retryable=False,
                            runtime_metadata={
                                "shortcode": shortcode,
                                "fetch_reason": result.fetch_reason,
                                "auth_failed": True,
                                "post_auth_failure_circuit_open": True,
                                "post_auth_failure_circuit_limit": post_auth_failure_circuit_limit,
                                "consecutive_post_auth_failures": consecutive_post_auth_failures,
                                "auth_failed_target_source_ids": auth_failed_targets,
                                "auth_failed_fetch_reasons": dict(auth_failed_fetch_reasons),
                                "incomplete_target_source_ids": incomplete_targets,
                                "incomplete_fetch_reasons": {
                                    target: auth_failed_fetch_reasons.get(target, "auth_failed")
                                    for target in incomplete_targets
                                },
                                "remaining_target_source_ids": remaining_target_source_ids,
                                "target_source_ids_count": len(target_source_ids),
                                "processed_posts": processed_posts,
                                "fetcher_runtime": dict(fetcher.runtime_metadata),
                                "diagnostic_metadata": _fetch_result_diagnostic_metadata(result),
                            },
                        )
                    continue
                consecutive_post_auth_failures = 0
                # Phase 1.5: every successful (or non-auth-failed) post advances the
                # mid-run-warmup post counter. Skip-paths above continued before
                # this point so they do not advance the counter spuriously.
                posts_since_last_warmup += 1
                if result.fetch_failed and not result.comments:
                    normalized_incomplete_shortcode = str(shortcode or "").strip()
                    normalized_fetch_reason = str(result.fetch_reason or "retryable_post_fetch_failed").strip()
                    result_metadata = _fetch_result_diagnostic_metadata(result)
                    target_metadata = target_metadata_by_shortcode.get(shortcode) or {}
                    if normalized_incomplete_shortcode and (isinstance(result_metadata, dict) or target_metadata):
                        failure_metadata: dict[str, Any] = {
                            "fetch_reason": normalized_fetch_reason,
                            "target_metadata": dict(target_metadata),
                        }
                        if result_metadata:
                            failure_metadata["fetcher_metadata"] = dict(result_metadata)
                        post_fetch_failure_metadata_by_shortcode[normalized_incomplete_shortcode] = failure_metadata
                    if (
                        normalized_incomplete_shortcode
                        and normalized_fetch_reason in _COAUTHOR_STATUS_ONLY_FETCH_REASONS
                    ):
                        coauthor_status_only_target_source_ids.append(normalized_incomplete_shortcode)
                    consecutive_post_fetch_failures += 1
                    if bool(result.retryable):
                        if normalized_incomplete_shortcode:
                            incomplete_target_source_ids.append(normalized_incomplete_shortcode)
                            incomplete_fetch_reasons[normalized_incomplete_shortcode] = normalized_fetch_reason
                            zero_comment_incomplete_target_source_ids.append(normalized_incomplete_shortcode)
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
                                "comments_load_strategy": comments_load_strategy,
                                "comments_session_scope": comments_session_scope,
                                "saved_once_per_post": False,
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
                        lifecycle.emit_job_progress(
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
                            "coauthor_status_only_target_source_ids": list(
                                dict.fromkeys(coauthor_status_only_target_source_ids)
                            ),
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
                stored_coverage_auth_blocked_gap = False
                missing_classified = 0
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
                        stored_reply_gap_total=persisted.stored_reply_gap_total,
                        max_comments_per_post=max_comments_per_post,
                    )
                    stored_coverage_reconciled_gap = _persisted_comment_coverage_gap_is_reconcilable(
                        result=result,
                        stored_total_comments=persisted.stored_total_comments,
                        stored_reply_gap_total=persisted.stored_reply_gap_total,
                        max_comments_per_post=max_comments_per_post,
                    )
                    stored_coverage_terminal_gap = _terminal_pagination_coverage_gap_is_reconcilable(
                        result=result,
                        stored_total_comments=persisted.stored_total_comments,
                        stored_reply_gap_total=persisted.stored_reply_gap_total,
                        max_comments_per_post=max_comments_per_post,
                    )
                    stored_coverage_auth_blocked_gap = _reply_only_auth_blocked_coverage_gap_is_reconcilable(
                        result=result,
                        stored_total_comments=persisted.stored_total_comments,
                        max_comments_per_post=max_comments_per_post,
                        reply_only=bool(fetch_kwargs.get("reply_only")),
                    )
                    if (
                        stored_coverage_reconciled_gap
                        or stored_coverage_terminal_gap
                        or stored_coverage_auth_blocked_gap
                    ):
                        repo._reconcile_post_comment_count(
                            platform="instagram",
                            post_db_id=persisted.post_id,
                            conn=persist_conn,
                        )
                    if stored_coverage_reconciled_gap:
                        missing_classification_reason = "stored_comment_coverage_reconciled_gap"
                    elif stored_coverage_terminal_gap:
                        missing_classification_reason = "stored_comment_coverage_terminal_gap_reconciled"
                    elif stored_coverage_auth_blocked_gap:
                        missing_classification_reason = str(result.fetch_reason or "html_challenge_or_auth_required")
                    else:
                        missing_classification_reason = None
                    if missing_classification_reason:
                        missing_classified = _classify_unavailable_instagram_comment_gap(
                            conn=persist_conn,
                            post_id=persisted.post_id,
                            result=result,
                            stored_total_comments=persisted.stored_total_comments,
                            max_comments_per_post=max_comments_per_post,
                            run_id=run_id or None,
                            job_id=job_id,
                            reason=missing_classification_reason,
                        )
                    _insert_instagram_post_comments_audit(
                        conn=persist_conn,
                        run_id=run_id or None,
                        job_id=job_id,
                        post_id=persisted.post_id,
                        shortcode=shortcode,
                        account_handle=account_handle,
                        result=result,
                        capture_metadata=capture_metadata,
                        fetched_parent_count=fetched_parent_count,
                        fetched_child_count=fetched_child_count,
                        fetched_total_count=observed_comment_count,
                        target_metadata=target_metadata_by_shortcode.get(shortcode) or {},
                    )
                    persist_conn.commit()
                saved_once_per_post_target_source_ids.append(shortcode)
                _raise_if_job_lease_lost(
                    job_id=job_id,
                    worker_id=worker_id,
                    runtime_metadata=dict(fetcher.runtime_metadata),
                )
                persist_elapsed_ms = int((time.monotonic() - persist_started_at) * 1000)
                total_elapsed_ms = int((time.monotonic() - post_started_at) * 1000)
                if first_post_persisted_at is None:
                    first_post_persisted_at = lifecycle.now_utc()
                persisted_reply_topology_complete = not _has_persisted_reply_topology_gap(
                    persisted.stored_reply_gap_total
                )
                reply_topology_gate_complete = persisted_reply_topology_complete or stored_coverage_auth_blocked_gap
                effective_is_complete = reply_topology_gate_complete and (
                    is_complete
                    or stored_coverage_complete
                    or stored_coverage_reconciled_gap
                    or stored_coverage_terminal_gap
                    or stored_coverage_auth_blocked_gap
                )
                if (
                    (result.fetch_failed and result.retryable)
                    or fetched_parentless_count > 0
                    or not reply_topology_gate_complete
                    or (completion_reason == "incomplete_fetch" and not result.auth_failed)
                ) and not effective_is_complete:
                    normalized_incomplete_shortcode = str(shortcode or "").strip()
                    if fetched_parentless_count > 0:
                        normalized_fetch_reason = _PARENTLESS_REPLY_ATTACH_FAILED_REASON
                    elif result.fetch_failed and result.retryable:
                        normalized_fetch_reason = str(result.fetch_reason or "retryable_incomplete_fetch").strip()
                    elif not reply_topology_gate_complete:
                        normalized_fetch_reason = _PERSISTED_REPLY_TOPOLOGY_GAP_REASON
                    elif completion_reason == "incomplete_fetch":
                        normalized_fetch_reason = str(result.fetch_reason or "hidden_comments_unresolved").strip()
                    else:
                        normalized_fetch_reason = "retryable_incomplete_fetch"
                    if normalized_incomplete_shortcode:
                        incomplete_target_source_ids.append(normalized_incomplete_shortcode)
                        incomplete_fetch_reasons[normalized_incomplete_shortcode] = normalized_fetch_reason
                        result_metadata = _fetch_result_diagnostic_metadata(result)
                        target_metadata = target_metadata_by_shortcode.get(shortcode) or {}
                        if (
                            isinstance(result_metadata, dict)
                            or target_metadata
                            or not persisted_reply_topology_complete
                        ):
                            failure_metadata: dict[str, Any] = {
                                "fetch_reason": normalized_fetch_reason,
                                "target_metadata": dict(target_metadata),
                            }
                            if result_metadata:
                                failure_metadata["fetcher_metadata"] = dict(result_metadata)
                            if not persisted_reply_topology_complete:
                                failure_metadata["persisted_reply_topology"] = _persisted_reply_topology_metadata(
                                    persisted
                                )
                            post_fetch_failure_metadata_by_shortcode[normalized_incomplete_shortcode] = failure_metadata
                        if normalized_fetch_reason in _COAUTHOR_STATUS_ONLY_FETCH_REASONS:
                            coauthor_status_only_target_source_ids.append(normalized_incomplete_shortcode)
                if stored_coverage_complete and not is_complete:
                    completion_reason = "stored_comment_coverage_complete"
                elif stored_coverage_reconciled_gap and not is_complete:
                    completion_reason = "stored_comment_coverage_reconciled_gap"
                elif stored_coverage_terminal_gap and not is_complete:
                    completion_reason = "stored_comment_coverage_terminal_gap_reconciled"
                elif stored_coverage_auth_blocked_gap and not is_complete:
                    completion_reason = "stored_comment_coverage_auth_blocked_gap_reconciled"
                elif not persisted_reply_topology_complete:
                    completion_reason = _PERSISTED_REPLY_TOPOLOGY_GAP_REASON
                operator_status = "complete" if effective_is_complete else "incomplete_retryable"
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
                        "parent_comments_fetched": fetched_parent_count,
                        "child_replies_fetched": fetched_child_count,
                        "parentless_replies_fetched": fetched_parentless_count,
                        "comments_upserted": persisted.comments_upserted,
                        "comments_inserted": persisted.comments_inserted,
                        "comments_refreshed": persisted.comments_refreshed,
                        "comments_changed": persisted.comments_changed,
                        "db_rows_written": persisted.comments_upserted,
                        "new_instagram_comments_saved": persisted.comments_inserted,
                        "existing_comment_rows_seen": persisted.comments_refreshed,
                        "existing_comment_rows_updated": max(
                            0,
                            persisted.comments_changed - persisted.comments_inserted,
                        ),
                        "comments_marked_missing": persisted.comments_marked_missing + missing_classified,
                        "fetch_reason": result.fetch_reason,
                        "stored_total_comments": persisted.stored_total_comments,
                        "stored_parent_comments": persisted.stored_parent_comments,
                        "stored_child_replies": persisted.stored_child_replies,
                        "expected_child_replies": persisted.expected_child_replies,
                        "stored_reply_gap_total": persisted.stored_reply_gap_total,
                        "stored_reply_gap_parent_count": persisted.stored_reply_gap_parent_count,
                        "stored_reply_gap_samples": list(persisted.stored_reply_gap_samples or []),
                        "is_complete": effective_is_complete,
                        "operator_status": operator_status,
                        "completion_reason": completion_reason,
                        "reported_comment_count": _extract_reported_comment_count(result),
                        "comments_load_strategy": comments_load_strategy,
                        "comments_session_scope": comments_session_scope,
                        "saved_once_per_post": shortcode in saved_once_per_post_target_source_ids,
                        # Phase A5 follow-up diagnostics: surface pagination
                        # depth + last cursor direction so operators can
                        # spot repeated_cursor stops without grepping logs.
                        "pages_seen": pages_seen_for_post,
                        "last_cursor_param": last_cursor_param_for_post,
                    }
                )
                processed_posts += 1
                comments_fetched += observed_comment_count
                parent_comments_fetched += fetched_parent_count
                child_replies_fetched += fetched_child_count
                parentless_replies_fetched += fetched_parentless_count
                comments_upserted += persisted.comments_upserted
                comments_inserted += persisted.comments_inserted
                comments_refreshed += persisted.comments_refreshed
                comments_changed += persisted.comments_changed
                successful_target_fetches += 1
                comments_marked_missing += persisted.comments_marked_missing + missing_classified
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
                lifecycle.emit_job_progress(
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

            retryable_incomplete_targets = _retryable_incomplete_target_source_ids(
                incomplete_target_source_ids=incomplete_target_source_ids,
                incomplete_fetch_reasons=incomplete_fetch_reasons,
                auth_failed_target_source_ids=[] if single_session_load_all else auth_failed_target_source_ids,
            )
            # Any target-level gap should be retried automatically. Earlier
            # versions only retried when enough posts in a shard failed, which
            # stranded small but real reported-vs-saved gaps in metadata.
            incomplete_raise_threshold = _MIN_INCOMPLETE_RAISE_TARGETS
            if retryable_incomplete_targets:
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
                    zero_comment_incomplete_targets=zero_comment_incomplete_target_source_ids,
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
                elif incomplete_fill_enabled and attempt_count >= max_attempts:
                    incomplete_retry_stall_metadata = {
                        "stalled": False,
                        "retry_exhausted": True,
                        "attempt_count": attempt_count,
                        "max_attempts": max_attempts,
                        "target_source_ids": retryable_incomplete_targets,
                        "fetch_reasons": retry_fetch_reasons,
                        "current_comments_fetched": comments_fetched,
                        "completion_status": "attempted_incomplete_fill",
                    }
                    logger.info(
                        "Instagram comments incomplete fill exhausted its one-pass retry budget; "
                        "completing shard with unresolved targets: job_id=%s targets=%s",
                        job_id,
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
                            "zero_comment_incomplete_target_source_ids": list(
                                dict.fromkeys(zero_comment_incomplete_target_source_ids)
                            ),
                            "coauthor_status_only_target_source_ids": list(
                                dict.fromkeys(coauthor_status_only_target_source_ids)
                            ),
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
        cumulative_counters = _build_cumulative_counters(
            job_id,
            posts=processed_posts,
            comments=comments_fetched,
            comments_upserted=comments_upserted,
            comments_inserted=comments_inserted,
            comments_refreshed=comments_refreshed,
            comments_changed=comments_changed,
        )
        metadata = {
            "stage": stage,
            "platform": "instagram",
            "account": account_handle,
            "mode": mode,
            "source_scope": source_scope,
            "comments_load_strategy": comments_load_strategy,
            "comments_session_scope": comments_session_scope,
            "target_source_ids": target_source_ids,
            "auth_failed_target_source_ids": list(dict.fromkeys(auth_failed_target_source_ids)),
            "auth_failed_fetch_reasons": dict(auth_failed_fetch_reasons),
            **terminal_metadata_common(),
            "stage_counters": {"posts": processed_posts, "comments": comments_fetched},
            "cumulative_counters": cumulative_counters,
            "skipped_complete_target_source_ids": skipped_complete_target_source_ids,
            "persist_counters": {
                "posts_upserted": processed_posts,
                "comments_upserted": comments_upserted,
                "comments_inserted": comments_inserted,
                "comments_refreshed": comments_refreshed,
                "comments_changed": comments_changed,
                "db_rows_written": comments_upserted,
                "new_instagram_comments_saved": comments_inserted,
                "existing_comment_rows_seen": comments_refreshed,
                "existing_comment_rows_updated": max(0, comments_changed - comments_inserted),
                "comments_marked_missing": comments_marked_missing,
                "comment_media_mirror_jobs_enqueued": mirror_jobs_enqueued,
                "comment_media_mirror_job_enqueue_errors": mirror_job_enqueue_errors,
            },
            "activity": {
                "phase": "comments_scrapling_end",
                "last_progress_at": lifecycle.format_time(lifecycle.now_utc()),
            },
            "fetch_counters": {
                "request_count": fetcher_metadata.get("request_count", 0),
                "target_posts": len(target_source_ids),
                "comments_fetched": comments_fetched,
                "parent_comments_fetched": parent_comments_fetched,
                "child_replies_fetched": child_replies_fetched,
                "parentless_replies_fetched": parentless_replies_fetched,
            },
            "comment_phase_counts": dict(comment_phase_counts),
            "cursor_diagnostics": {
                "cursor_param_counts": dict(comment_cursor_param_counts),
                "cursor_shape_counts": dict(comment_cursor_shape_counts),
                "samples": list(comment_capture_samples),
            },
            "auth_context": {
                **auth_context,
                "metadata_source": auth_metadata.get("source"),
            },
            "fetcher_runtime": fetcher_metadata,
        }
        lifecycle.finish_job(
            job_id,
            status="completed",
            items_found=_cumulative_items_found(cumulative_counters),
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
        cumulative_counters = _build_cumulative_counters(
            job_id,
            posts=processed_posts,
            comments=comments_fetched,
            comments_upserted=comments_upserted,
            comments_inserted=comments_inserted,
            comments_refreshed=comments_refreshed,
            comments_changed=comments_changed,
        )
        lifecycle.finish_job(
            job_id,
            status="cancelled",
            items_found=_cumulative_items_found(cumulative_counters),
            error_message=str(exc),
            metadata={
                "stage": stage,
                "platform": "instagram",
                "account": account_handle,
                "mode": mode,
                "source_scope": source_scope,
                "comments_load_strategy": comments_load_strategy,
                "comments_session_scope": comments_session_scope,
                "target_source_ids": target_source_ids,
                "incomplete_target_source_ids": list(dict.fromkeys(incomplete_target_source_ids)),
                "zero_comment_incomplete_target_source_ids": list(
                    dict.fromkeys(zero_comment_incomplete_target_source_ids)
                ),
                "auth_failed_target_source_ids": list(dict.fromkeys(auth_failed_target_source_ids)),
                "skipped_complete_target_source_ids": skipped_complete_target_source_ids,
                "incomplete_fetch_reasons": dict(incomplete_fetch_reasons),
                "auth_failed_fetch_reasons": dict(auth_failed_fetch_reasons),
                **terminal_metadata_common(),
                "cancelled": True,
                "cancel_scope": exc.cancel_scope,
                "job_status_at_cancel": exc.job_status,
                "run_status_at_cancel": exc.run_status,
                "activity": {"phase": "cancelled", "last_progress_at": lifecycle.format_time(lifecycle.now_utc())},
                "stage_counters": {"posts": processed_posts, "comments": comments_fetched},
                "cumulative_counters": cumulative_counters,
                "persist_counters": {
                    "posts_upserted": processed_posts,
                    "comments_upserted": comments_upserted,
                    "comments_inserted": comments_inserted,
                    "comments_refreshed": comments_refreshed,
                    "comments_changed": comments_changed,
                    "db_rows_written": comments_upserted,
                    "new_instagram_comments_saved": comments_inserted,
                    "existing_comment_rows_seen": comments_refreshed,
                    "existing_comment_rows_updated": max(0, comments_changed - comments_inserted),
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
                    auth_failed_target_source_ids=[] if single_session_load_all else auth_failed_target_source_ids,
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
        runtime_metadata = lifecycle.metadata_dict(getattr(exc, "runtime_metadata", None))
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
            auth_failed_target_source_ids=[] if single_session_load_all else auth_failed_target_source_ids,
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
            lifecycle.now_utc() + timedelta(seconds=lifecycle.retry_backoff_seconds(attempt_count))
            if can_retry
            else None
        )
        cumulative_counters = _build_cumulative_counters(
            job_id,
            posts=processed_posts,
            comments=comments_fetched,
            comments_upserted=comments_upserted,
            comments_inserted=comments_inserted,
            comments_refreshed=comments_refreshed,
            comments_changed=comments_changed,
        )
        lifecycle.finish_job(
            job_id,
            status="retrying" if can_retry else "failed",
            items_found=_cumulative_items_found(cumulative_counters),
            error_message=str(exc),
            metadata={
                "stage": stage,
                "platform": "instagram",
                "account": account_handle,
                "mode": mode,
                "source_scope": source_scope,
                "comments_load_strategy": comments_load_strategy,
                "comments_session_scope": comments_session_scope,
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
                "activity": {"phase": "failed", "last_progress_at": lifecycle.format_time(lifecycle.now_utc())},
                "stage_counters": {"posts": processed_posts, "comments": comments_fetched},
                "cumulative_counters": cumulative_counters,
                "persist_counters": {
                    "posts_upserted": processed_posts,
                    "comments_upserted": comments_upserted,
                    "comments_inserted": comments_inserted,
                    "comments_refreshed": comments_refreshed,
                    "comments_changed": comments_changed,
                    "db_rows_written": comments_upserted,
                    "new_instagram_comments_saved": comments_inserted,
                    "existing_comment_rows_seen": comments_refreshed,
                    "existing_comment_rows_updated": max(0, comments_changed - comments_inserted),
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
                finalize_result = lifecycle.finalize_run_status(run_id)
                if isinstance(finalize_result, dict) and finalize_result.get("finalize_deferred"):
                    logger.warning(
                        "Retrying deferred comments run-status reconciliation: run_id=%s error=%s",
                        run_id,
                        finalize_result.get("error"),
                    )
                    time.sleep(2.0)
                    lifecycle.finalize_run_status(run_id, force_recompute=True)
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
