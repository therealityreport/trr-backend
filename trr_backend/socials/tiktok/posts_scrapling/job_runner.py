"""Worker job orchestrator for the TikTok posts Scrapling lane.

Flow: session → proxy → fetcher → warmup → resolve_sec_uid → paginate → persist.
Keeps yt-dlp as independent co-primary — this lane is an alternative path.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import time
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from trr_backend.db import pg
from trr_backend.socials.tiktok.posts_scrapling.fetcher import TikTokPostsScraplingFetcher
from trr_backend.socials.tiktok.posts_scrapling.persistence import persist_tiktok_posts
from trr_backend.socials.tiktok.posts_scrapling.proxy import select_tiktok_posts_proxy
from trr_backend.socials.tiktok.posts_scrapling.session import resolve_tiktok_posts_session

logger = logging.getLogger("socials.tiktok.posts_scrapling.job_runner")


class _LifecycleProxy:
    def __getattr__(self, name: str) -> Any:
        import trr_backend.socials.control_plane.run_lifecycle as lifecycle

        return getattr(lifecycle, name)


lifecycle = _LifecycleProxy()


@dataclass(slots=True)
class TikTokPostsScraplingRuntimeError(Exception):
    message: str
    error_code: str
    retryable: bool = False
    runtime_metadata: dict[str, Any] | None = None

    def __str__(self) -> str:
        return self.message


@dataclass(slots=True)
class TikTokPostsScraplingCancelledError(Exception):
    message: str
    cancel_scope: str
    job_status: str | None = None
    run_status: str | None = None
    runtime_metadata: dict[str, Any] | None = None

    def __str__(self) -> str:
        return self.message


TikTokPostsScraplingCancelled = TikTokPostsScraplingCancelledError

_OPERATION_TIMEOUT_SECONDS_DEFAULT = 240.0
_OPERATION_HEARTBEAT_INTERVAL_SECONDS_DEFAULT = 30.0


def _resolve_operation_timeout_seconds() -> float:
    raw_value = str(os.getenv("SOCIAL_TIKTOK_POSTS_SCRAPLING_OPERATION_TIMEOUT_SECONDS") or "").strip()
    if not raw_value:
        return _OPERATION_TIMEOUT_SECONDS_DEFAULT
    try:
        parsed = float(raw_value)
    except (TypeError, ValueError):
        return _OPERATION_TIMEOUT_SECONDS_DEFAULT
    if parsed <= 0:
        return 0.0
    return min(max(parsed, 5.0), 900.0)


def _resolve_operation_heartbeat_interval_seconds() -> float:
    raw_value = str(os.getenv("SOCIAL_TIKTOK_POSTS_SCRAPLING_HEARTBEAT_INTERVAL_SECONDS") or "").strip()
    if not raw_value:
        return _OPERATION_HEARTBEAT_INTERVAL_SECONDS_DEFAULT
    try:
        parsed = float(raw_value)
    except (TypeError, ValueError):
        return _OPERATION_HEARTBEAT_INTERVAL_SECONDS_DEFAULT
    return min(max(parsed, 1.0), 120.0)


def _raise_if_cancelled(
    *,
    job_id: str,
    run_id: str,
    runtime_metadata: dict[str, Any] | None = None,
) -> None:
    if not job_id:
        return
    started_at = time.perf_counter()
    try:
        job_state = pg.fetch_one("select status from social.scrape_jobs where id = %s", [job_id]) or {}
    except pg.DatabaseServiceUnavailableError as exc:
        logger.warning(
            "Skipping TikTok posts cancellation check after database saturation: job_id=%s error=%s",
            job_id,
            exc,
        )
        return
    job_status = str(job_state.get("status") or "").strip().lower() or None
    run_status: str | None = None
    if run_id:
        try:
            run_state = pg.fetch_one("select status from social.scrape_runs where id = %s", [run_id]) or {}
        except pg.DatabaseServiceUnavailableError as exc:
            logger.warning(
                "Skipping TikTok posts run cancellation check after database saturation: run_id=%s error=%s",
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
        "tiktok_posts_scrapling cancellation_detected",
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
    raise TikTokPostsScraplingCancelledError(
        "TikTok posts Scrapling job was cancelled.",
        cancel_scope=cancel_scope,
        job_status=job_status,
        run_status=run_status,
        runtime_metadata=metadata,
    )


def run_tiktok_posts_scrapling_job(job: dict[str, Any], *, worker_id: str | None = None) -> dict[str, Any]:
    job_id = str(job.get("id") or "").strip()
    run_id = str(job.get("run_id") or "").strip()
    config = dict(job.get("config") or {})
    account_handle = str(config.get("account") or "").strip().lower().lstrip("@")
    stage = str(config.get("stage") or "tiktok_posts_scrapling").strip().lower()
    max_pages_raw = config.get("max_pages")
    max_pages: int | None = int(max_pages_raw) if max_pages_raw not in (None, 0, "") else None
    season_id = str(config.get("season_id") or "").strip() or None

    if not account_handle:
        raise TikTokPostsScraplingRuntimeError(
            "TikTok posts Scrapling job is missing an account handle.",
            error_code="tiktok_posts_account_missing",
            retryable=False,
        )

    progress_state = lifecycle.new_job_progress_state()
    posts_fetched = 0
    posts_upserted = 0
    posts_skipped = 0
    posts_skipped_by_reason: dict[str, int] = {}
    pages_fetched = 0
    fetcher_metadata: dict[str, Any] = {}
    last_cursor: str | None = None
    stop_reason: str | None = None
    terminal_status = "unknown"
    terminal_error_message: str | None = None

    def _merge_skipped_reasons(reasons: dict[str, int]) -> None:
        for reason, count in reasons.items():
            normalized_reason = str(reason or "").strip() or "unknown"
            posts_skipped_by_reason[normalized_reason] = int(posts_skipped_by_reason.get(normalized_reason) or 0) + int(
                count or 0
            )

    def _listing_progress(*, partial: bool | None = None) -> dict[str, Any]:
        is_partial = stop_reason not in {None, "completed"} if partial is None else partial
        return {
            "page_index": pages_fetched,
            "posts_seen": posts_fetched,
            "posts_upserted": posts_upserted,
            "posts_skipped": posts_skipped,
            "cursor": last_cursor,
            "stop_reason": stop_reason,
            "partial": bool(is_partial),
        }

    def _stage_counters() -> dict[str, int]:
        return {"posts": posts_fetched, "pages": pages_fetched}

    def _persist_counters() -> dict[str, Any]:
        return {
            "posts_upserted": posts_upserted,
            "posts_skipped": posts_skipped,
            "posts_skipped_by_reason": dict(posts_skipped_by_reason),
        }

    async def _run_job() -> dict[str, Any]:
        nonlocal posts_fetched, posts_upserted, posts_skipped, pages_fetched
        nonlocal fetcher_metadata, last_cursor, stop_reason

        session = resolve_tiktok_posts_session()
        proxy_config = select_tiktok_posts_proxy()
        fetcher = TikTokPostsScraplingFetcher(
            cookies=session.cookies,
            raw_cookies=session.raw_cookies,
            proxy_config=proxy_config,
        )

        def _fetcher_runtime_metadata() -> dict[str, Any]:
            return dict(getattr(fetcher, "runtime_metadata", {}) or {})

        async def _await_operation_with_heartbeat(awaitable: Any, *, phase: str) -> Any:
            nonlocal fetcher_metadata
            task = asyncio.create_task(awaitable)
            started_at = time.monotonic()
            timeout_seconds = _resolve_operation_timeout_seconds()
            heartbeat_interval_seconds = _resolve_operation_heartbeat_interval_seconds()
            while True:
                elapsed = time.monotonic() - started_at
                if timeout_seconds > 0:
                    remaining = timeout_seconds - elapsed
                    if remaining <= 0:
                        task.cancel()
                        with contextlib.suppress(asyncio.CancelledError):
                            await task
                        fetcher_metadata = _fetcher_runtime_metadata()
                        raise TikTokPostsScraplingRuntimeError(
                            f"TikTok posts operation timed out while {phase} for @{account_handle}.",
                            error_code=f"tiktok_posts_{phase}_timeout",
                            retryable=True,
                            runtime_metadata={
                                **fetcher_metadata,
                                "phase": phase,
                                "operation_timeout_seconds": timeout_seconds,
                            },
                        )
                    wait_seconds = min(heartbeat_interval_seconds, remaining)
                else:
                    wait_seconds = heartbeat_interval_seconds
                done, _pending = await asyncio.wait({task}, timeout=wait_seconds)
                if task in done:
                    return task.result()
                fetcher_metadata = _fetcher_runtime_metadata()
                lifecycle.touch_job_heartbeat(job_id, worker_id=worker_id)
                _raise_if_cancelled(job_id=job_id, run_id=run_id, runtime_metadata=fetcher_metadata)

        try:
            await _await_operation_with_heartbeat(fetcher.warmup(account_handle), phase="warmup")
            fetcher_metadata = _fetcher_runtime_metadata()
            lifecycle.touch_job_heartbeat(job_id, worker_id=worker_id)
            _raise_if_cancelled(job_id=job_id, run_id=run_id, runtime_metadata=fetcher_metadata)

            sec_uid = await _await_operation_with_heartbeat(
                fetcher.resolve_sec_uid(account_handle),
                phase="resolve_sec_uid",
            )
            fetcher_metadata = _fetcher_runtime_metadata()
            _raise_if_cancelled(job_id=job_id, run_id=run_id, runtime_metadata=fetcher_metadata)
            cursor: str | None = None

            while True:
                lifecycle.touch_job_heartbeat(job_id, worker_id=worker_id)
                fetcher_metadata = _fetcher_runtime_metadata()
                _raise_if_cancelled(job_id=job_id, run_id=run_id, runtime_metadata=fetcher_metadata)
                result = await _await_operation_with_heartbeat(
                    fetcher.fetch_posts_page(sec_uid=sec_uid, cursor=cursor),
                    phase="fetch_posts_page",
                )
                fetcher_metadata = _fetcher_runtime_metadata()
                last_cursor = str(result.cursor or "").strip() or None

                if result.auth_failed:
                    raise TikTokPostsScraplingRuntimeError(
                        f"TikTok auth failed while fetching posts for @{account_handle}.",
                        error_code="tiktok_posts_auth_failed",
                        retryable=False,
                        runtime_metadata={**fetcher_metadata, "fetch_reason": result.fetch_reason},
                    )
                if result.fetch_failed and not result.posts:
                    raise TikTokPostsScraplingRuntimeError(
                        f"TikTok posts fetch failed for @{account_handle}: {result.fetch_reason}",
                        error_code=str(result.fetch_reason or "tiktok_posts_fetch_failed"),
                        retryable=bool(result.retryable),
                        runtime_metadata={**fetcher_metadata, "fetch_reason": result.fetch_reason},
                    )

                persisted_page = False
                if result.posts:
                    persisted = persist_tiktok_posts(
                        account_handle=account_handle,
                        post_items=result.posts,
                        run_id=run_id or None,
                        job_id=job_id,
                        season_id=season_id,
                    )
                    posts_fetched += len(result.posts)
                    posts_upserted += persisted.posts_upserted
                    posts_skipped += persisted.posts_skipped
                    _merge_skipped_reasons(dict(getattr(persisted, "posts_skipped_by_reason", {}) or {}))
                    persisted_page = True

                pages_fetched += 1
                if not result.has_more or not result.cursor:
                    stop_reason = "completed"
                elif max_pages and pages_fetched >= max_pages:
                    stop_reason = "max_pages"
                else:
                    stop_reason = None

                if persisted_page:
                    fetcher_metadata = _fetcher_runtime_metadata()
                    _raise_if_cancelled(job_id=job_id, run_id=run_id, runtime_metadata=fetcher_metadata)

                lifecycle.emit_job_progress(
                    job_id=job_id,
                    stage=stage,
                    platform="tiktok",
                    account=account_handle,
                    scraped_posts=posts_fetched,
                    scraped_comments=0,
                    posts_upserted=posts_upserted,
                    comments_upserted=0,
                    activity={
                        "phase": "tiktok_posts_scrapling_running",
                        "pages_fetched": pages_fetched,
                        "listing_progress": _listing_progress(partial=stop_reason not in {None, "completed"}),
                    },
                    progress_state=progress_state,
                    force=not result.has_more or bool(stop_reason),
                )

                if stop_reason:
                    break
                cursor = result.cursor

            fetcher_metadata = _fetcher_runtime_metadata()
            _raise_if_cancelled(job_id=job_id, run_id=run_id, runtime_metadata=fetcher_metadata)
            return fetcher_metadata
        finally:
            fetcher_metadata = _fetcher_runtime_metadata()
            await fetcher.aclose()

    try:
        fetcher_metadata = asyncio.run(_run_job())
        metadata = {
            "stage": stage,
            "platform": "tiktok",
            "account": account_handle,
            "stage_counters": _stage_counters(),
            "persist_counters": _persist_counters(),
            "listing_progress": _listing_progress(partial=stop_reason not in {None, "completed"}),
            "stop_reason": stop_reason,
            "activity": {
                "phase": "tiktok_posts_scrapling_end",
                "last_progress_at": lifecycle.format_time(lifecycle.now_utc()),
            },
            "runtime_metadata": {
                "fetcher_runtime": fetcher_metadata,
                "listing_progress": _listing_progress(partial=stop_reason not in {None, "completed"}),
            },
            "fetcher_runtime": fetcher_metadata,
        }
        lifecycle.finish_job(
            job_id,
            status="completed",
            items_found=posts_fetched,
            metadata=metadata,
        )
        terminal_status = "completed"
        terminal_error_message = None
    except TikTokPostsScraplingCancelledError as exc:
        lifecycle.finish_job(
            job_id,
            status="cancelled",
            items_found=posts_fetched,
            error_message=str(exc),
            metadata={
                "stage": stage,
                "platform": "tiktok",
                "account": account_handle,
                "cancelled": True,
                "cancel_scope": exc.cancel_scope,
                "job_status_at_cancel": exc.job_status,
                "run_status_at_cancel": exc.run_status,
                "activity": {"phase": "cancelled", "last_progress_at": lifecycle.format_time(lifecycle.now_utc())},
                "stage_counters": _stage_counters(),
                "persist_counters": _persist_counters(),
                "listing_progress": _listing_progress(partial=True),
                "stop_reason": stop_reason,
                "runtime_metadata": exc.runtime_metadata,
                "fetcher_runtime": fetcher_metadata,
            },
            last_error_code="tiktok_posts_scrapling_cancelled",
            last_error_class=exc.__class__.__name__,
        )
        terminal_status = "cancelled"
        terminal_error_message = str(exc)
    except Exception as exc:  # noqa: BLE001
        runtime_error_code = str(getattr(exc, "error_code", "") or "").strip().lower()
        error_code = runtime_error_code or "tiktok_posts_scrapling_failed"
        error_class = str(getattr(exc, "error_class", "") or exc.__class__.__name__).strip()
        retryable = bool(getattr(exc, "retryable", False))
        attempt_count = int(job.get("attempt_count") or 1)
        max_attempts = int(job.get("max_attempts") or 1)
        can_retry = retryable and attempt_count < max_attempts
        next_available_at = (
            lifecycle.now_utc() + timedelta(seconds=lifecycle.retry_backoff_seconds(attempt_count))
            if can_retry
            else None
        )
        if not stop_reason:
            stop_reason = error_code
        lifecycle.finish_job(
            job_id,
            status="retrying" if can_retry else "failed",
            items_found=posts_fetched,
            error_message=str(exc),
            metadata={
                "stage": stage,
                "platform": "tiktok",
                "account": account_handle,
                "error_code": error_code,
                "error_class": error_class,
                "activity": {"phase": "failed", "last_progress_at": lifecycle.format_time(lifecycle.now_utc())},
                "stage_counters": _stage_counters(),
                "persist_counters": _persist_counters(),
                "listing_progress": _listing_progress(partial=True),
                "stop_reason": stop_reason,
                "runtime_metadata": getattr(exc, "runtime_metadata", None),
                "fetcher_runtime": fetcher_metadata,
            },
            last_error_code=error_code,
            last_error_class=error_class,
            next_available_at=next_available_at,
        )
        terminal_status = "retrying" if can_retry else "failed"
        terminal_error_message = str(exc)
    finally:
        if run_id:
            try:
                lifecycle.finalize_run_status(run_id)
            except pg.DatabaseServiceUnavailableError as exc:
                logger.warning(
                    "Deferred final TikTok posts run-status reconciliation after database saturation: "
                    "run_id=%s error=%s",
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
            "Returning degraded TikTok posts job summary after database saturation: job_id=%s error=%s",
            job_id,
            exc,
        )
        return {
            "id": job_id,
            "run_id": run_id or None,
            "platform": "tiktok",
            "job_type": str(job.get("job_type") or "posts").strip() or "posts",
            "status": terminal_status,
            "items_found": posts_fetched,
            "error_message": terminal_error_message,
            "metadata": {
                "degraded_summary": True,
                "database_service_unavailable": True,
                "stage": stage,
                "platform": "tiktok",
                "account": account_handle,
                "stage_counters": _stage_counters(),
                "persist_counters": _persist_counters(),
                "listing_progress": _listing_progress(partial=terminal_status != "completed"),
                "stop_reason": stop_reason,
                "fetcher_runtime": fetcher_metadata,
            },
        }
