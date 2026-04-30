from __future__ import annotations

import asyncio
import logging
import time
from collections import Counter
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

logger = logging.getLogger("socials.instagram.comments_scrapling.job_runner")

_RUN_FATAL_COMMENTS_ERROR_CODES = {
    "instagram_comments_auth_failed",
    "instagram_comments_warmup_auth_failed",
    "instagram_comments_warmup_no_cookies",
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


def _comments_scrape_is_complete(
    *,
    result: InstagramCommentsFetchResult,
    max_comments_per_post: int,
) -> bool:
    if result.fetch_failed or result.auth_failed:
        return False
    reported_comment_count = _extract_reported_comment_count(result)
    if reported_comment_count is not None and len(result.comments) < reported_comment_count:
        if max_comments_per_post <= 0 or reported_comment_count <= max_comments_per_post:
            return False
    if max_comments_per_post <= 0:
        return True
    return len(result.comments) < max_comments_per_post


def _safe_int(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


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
    reported = getattr(result, "reported_comment_count", None) or getattr(result, "comments_count", None)
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
) -> dict[str, Any] | None:
    retry_targets = [
        str(item or "").strip()
        for item in (incomplete_target_source_ids or [])
        if str(item or "").strip()
    ]
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
    comments_shard_index = max(1, int(config.get("comments_shard_index") or 1))
    comments_shard_count = max(1, int(config.get("comments_shard_count") or 1))
    comments_shard_target_count = max(0, int(config.get("comments_shard_target_count") or len(target_source_ids)))
    skipped_complete_target_source_ids: list[str] = []
    skip_complete_retry_targets = any(
        _config_truthy(config.get(flag))
        for flag in (
            "comments_retry_rebalance",
            "comments_retry_incomplete",
            "comments_target_gap_repair",
            "comments_skip_complete_targets",
        )
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
    post_latency_samples: list[dict[str, Any]] = []
    incomplete_target_source_ids: list[str] = []
    incomplete_fetch_reasons: dict[str, str] = {}
    auth_failed_target_source_ids: list[str] = []
    auth_failed_fetch_reasons: dict[str, str] = {}
    consecutive_post_auth_failures = 0
    successful_target_fetches = 0
    post_auth_failure_circuit_limit = _safe_int(config.get("post_auth_failure_circuit_limit")) or 3
    warmup_completed_at = None
    first_post_persisted_at = None
    auth_context: dict[str, Any] = {}
    terminal_status = str(job.get("status") or "").strip().lower() or None
    terminal_error_message: str | None = None

    def terminal_metadata_common() -> dict[str, Any]:
        return {
            **shard_metadata,
            "post_latency": _post_latency_metadata(post_latency_samples),
            "comment_completeness": _comment_completeness_metadata(post_latency_samples),
            "post_auth_failures": {
                "target_source_ids": list(dict.fromkeys(auth_failed_target_source_ids)),
                "fetch_reasons": dict(auth_failed_fetch_reasons),
                "circuit_limit": post_auth_failure_circuit_limit,
            },
            "reply_checkpoint_summary": _reply_checkpoint_summary(fetcher_metadata),
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
        nonlocal consecutive_post_auth_failures, successful_target_fetches
        nonlocal warmup_completed_at, first_post_persisted_at, auth_context

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
            auth_metadata = dict(session.auth_session.metadata or {})
            _raise_if_cancelled(
                job_id=job_id,
                run_id=run_id,
                runtime_metadata=dict(fetcher.runtime_metadata),
            )
            repo._touch_job_heartbeat(job_id, worker_id=worker_id)
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
                force=True,
            )

            for index, shortcode in enumerate(target_source_ids, start=1):
                post_started_at = time.monotonic()
                repo._touch_job_heartbeat(job_id, worker_id=worker_id)
                _raise_if_cancelled(
                    job_id=job_id,
                    run_id=run_id,
                    runtime_metadata=dict(fetcher.runtime_metadata),
                )
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
                        force=index == len(target_source_ids),
                    )
                    continue
                fetch_started_at = time.monotonic()
                result = await fetcher.fetch_comments_for_shortcode(
                    shortcode,
                    max_comments=max_comments_per_post,
                    fetch_replies=fetch_replies,
                    expected_comment_count=expected_comment_counts_by_shortcode.get(shortcode),
                )
                fetch_elapsed_ms = int((time.monotonic() - fetch_started_at) * 1000)
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
                            force=index == len(target_source_ids),
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
                if result.fetch_failed and not result.comments:
                    raise CommentsScraplingRuntimeError(
                        f"Instagram comments fetch failed for {shortcode}.",
                        error_code=str(result.fetch_reason or "instagram_comments_fetch_failed"),
                        retryable=bool(result.retryable),
                        runtime_metadata={"shortcode": shortcode, "fetch_reason": result.fetch_reason},
                    )
                is_complete = _comments_scrape_is_complete(
                    result=result,
                    max_comments_per_post=max_comments_per_post,
                )
                completion_reason = (
                    "max_comments_cap"
                    if max_comments_per_post and len(result.comments) >= max_comments_per_post
                    else "pagination_exhausted"
                    if is_complete
                    else "incomplete_fetch"
                )
                persist_started_at = time.monotonic()
                with pg.db_connection(label="instagram-comments-scrapling-persist") as persist_conn:
                    _raise_if_cancelled(
                        job_id=job_id,
                        run_id=run_id,
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
                        conn=persist_conn,
                    )
                    persist_conn.commit()
                persist_elapsed_ms = int((time.monotonic() - persist_started_at) * 1000)
                total_elapsed_ms = int((time.monotonic() - post_started_at) * 1000)
                if first_post_persisted_at is None:
                    first_post_persisted_at = repo._now_utc()
                if result.fetch_failed and result.retryable:
                    normalized_incomplete_shortcode = str(shortcode or "").strip()
                    if normalized_incomplete_shortcode:
                        incomplete_target_source_ids.append(normalized_incomplete_shortcode)
                        incomplete_fetch_reasons[normalized_incomplete_shortcode] = str(
                            result.fetch_reason or "retryable_incomplete_fetch"
                        )
                post_latency_samples.append(
                    {
                        "shortcode": shortcode,
                        "fetch_elapsed_ms": fetch_elapsed_ms,
                        "persist_elapsed_ms": persist_elapsed_ms,
                        "total_elapsed_ms": total_elapsed_ms,
                        "comments_fetched": len(result.comments),
                        "comments_upserted": persisted.comments_upserted,
                        "comments_marked_missing": persisted.comments_marked_missing,
                        "fetch_reason": result.fetch_reason,
                        "is_complete": is_complete,
                        "completion_reason": completion_reason,
                        "reported_comment_count": _extract_reported_comment_count(result),
                    }
                )
                processed_posts += 1
                comments_fetched += len(result.comments)
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
                    force=index == len(target_source_ids),
                )

            retryable_incomplete_targets = list(dict.fromkeys(incomplete_target_source_ids))
            if retryable_incomplete_targets:
                raise CommentsScraplingRuntimeError(
                    "Instagram comments Scrapling job had retryable incomplete posts.",
                    error_code="instagram_comments_incomplete_retryable",
                    retryable=True,
                    runtime_metadata={
                        "incomplete_target_source_ids": retryable_incomplete_targets,
                        "incomplete_fetch_reasons": {
                            shortcode: incomplete_fetch_reasons.get(shortcode)
                            for shortcode in retryable_incomplete_targets
                        },
                    },
                )

            return auth_metadata, dict(fetcher.runtime_metadata)
        finally:
            fetcher_metadata = dict(fetcher.runtime_metadata)
            await fetcher.aclose()

    try:
        auth_metadata, fetcher_metadata = asyncio.run(_run_job())
        metadata = {
            "stage": stage,
            "platform": "instagram",
            "account": account_handle,
            "mode": mode,
            "source_scope": source_scope,
            "target_source_ids": target_source_ids,
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
        )
        terminal_status = "completed"
        terminal_error_message = None
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
                "skipped_complete_target_source_ids": skipped_complete_target_source_ids,
                "incomplete_fetch_reasons": dict(incomplete_fetch_reasons),
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
                ),
            },
            last_error_code="instagram_comments_scrapling_cancelled",
            last_error_class=exc.__class__.__name__,
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
        attempt_count = int(job.get("attempt_count") or 1)
        max_attempts = int(job.get("max_attempts") or 1)
        can_retry = retryable and attempt_count < max_attempts
        retry_rebalance = _retry_rebalance_metadata(
            comments_shard_count=comments_shard_count,
            target_source_ids=target_source_ids,
            processed_posts=processed_posts,
            incomplete_target_source_ids=retry_incomplete_targets or incomplete_target_source_ids,
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
                "skipped_complete_target_source_ids": skipped_complete_target_source_ids,
                "incomplete_fetch_reasons": dict(incomplete_fetch_reasons),
                **terminal_metadata_common(),
                "error_code": error_code,
                "error_class": error_class,
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
                repo._finalize_run_status(run_id)
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
