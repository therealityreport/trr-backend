from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from trr_backend.db import pg
from trr_backend.socials.instagram.posts_scrapling.fetcher import InstagramPostsScraplingFetcher

try:
    from trr_backend.socials.instagram.posts_scrapling.fetcher import InstagramPostsWarmupError
except ImportError:  # pragma: no cover - removed once the fetcher worker lands.

    class InstagramPostsWarmupError(RuntimeError):
        error_code = "instagram_posts_warmup_failed"
        retryable = False


from trr_backend.socials.instagram.posts_scrapling.persistence import persist_instagram_posts
from trr_backend.socials.instagram.posts_scrapling.proxy import select_posts_proxy
from trr_backend.socials.instagram.posts_scrapling.session import resolve_posts_scrapling_session

logger = logging.getLogger("socials.instagram.posts_scrapling.job_runner")


@dataclass(slots=True)
class PostsScraplingRuntimeError(Exception):
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
            "Skipping posts cancellation check after database saturation: job_id=%s error=%s",
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
                "Skipping posts run cancellation check after database saturation: run_id=%s error=%s",
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
        "instagram_posts_scrapling cancellation_detected",
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
        "Instagram posts Scrapling job was cancelled.",
        cancel_scope=cancel_scope,
        job_status=job_status,
        run_status=run_status,
        runtime_metadata=metadata,
    )


def run_instagram_posts_scrapling_job(job: dict[str, Any], *, worker_id: str | None = None) -> dict[str, Any]:
    from trr_backend.repositories import social_season_analytics as repo

    job_id = str(job.get("id") or "").strip()
    run_id = str(job.get("run_id") or "").strip()
    config = dict(job.get("config") or {})
    account_handle = str(config.get("account") or "").strip().lower().lstrip("@")
    stage = str(config.get("stage") or "posts_scrapling").strip().lower()
    max_pages_raw = config.get("max_pages")
    max_pages: int | None = int(max_pages_raw) if max_pages_raw not in (None, 0, "") else None
    fast_mode = bool(config.get("fast_mode", False))
    source_scope = str(config.get("source_scope") or "bravo").strip().lower() or "bravo"
    season_id = str(config.get("season_id") or "").strip() or None

    if not account_handle:
        raise PostsScraplingRuntimeError(
            "Instagram posts Scrapling job is missing an account handle.",
            error_code="instagram_posts_account_missing",
            retryable=False,
        )

    progress_state = repo._new_job_progress_state()
    posts_fetched = 0
    posts_upserted = 0
    posts_skipped = 0
    posts_skipped_by_reason: dict[str, int] = {}
    pages_fetched = 0
    fetcher_metadata: dict[str, Any] = {}

    async def _run_job() -> tuple[dict[str, Any], dict[str, Any]]:
        nonlocal posts_fetched, posts_upserted, posts_skipped, posts_skipped_by_reason
        nonlocal pages_fetched, fetcher_metadata

        session = resolve_posts_scrapling_session(
            browser_account_id=account_handle,
            caller_context=f"posts_scrapling:{account_handle}",
        )
        proxy_config = select_posts_proxy()
        fetcher = InstagramPostsScraplingFetcher(
            cookies=session.cookies,
            raw_cookies=session.auth_session.cookies,
            browser_account_id=session.browser_account_id,
            proxy_config=proxy_config,
            fast_mode=fast_mode,
        )
        try:
            try:
                await fetcher.warmup(account_handle)
            except InstagramPostsWarmupError as exc:
                raise PostsScraplingRuntimeError(
                    str(exc),
                    error_code=str(getattr(exc, "error_code", "") or "instagram_posts_warmup_failed"),
                    retryable=bool(getattr(exc, "retryable", False)),
                    runtime_metadata=dict(fetcher.runtime_metadata),
                ) from exc
            auth_metadata = dict(session.auth_session.metadata or {})
            fetcher_metadata = dict(fetcher.runtime_metadata)

            repo._touch_job_heartbeat(job_id, worker_id=worker_id)
            cursor: str | None = None

            while True:
                repo._touch_job_heartbeat(job_id, worker_id=worker_id)
                _raise_if_cancelled(
                    job_id=job_id,
                    run_id=run_id,
                    runtime_metadata=dict(fetcher.runtime_metadata),
                )
                result = await fetcher.fetch_posts_page(account_handle, cursor=cursor)
                fetcher_metadata = dict(fetcher.runtime_metadata)

                if result.auth_failed:
                    raise PostsScraplingRuntimeError(
                        f"Instagram auth failed while fetching posts for @{account_handle}.",
                        error_code="instagram_posts_auth_failed",
                        retryable=False,
                        runtime_metadata={"fetch_reason": result.fetch_reason},
                    )
                if result.fetch_failed and not result.posts:
                    raise PostsScraplingRuntimeError(
                        f"Instagram posts fetch failed for @{account_handle}.",
                        error_code=str(result.fetch_reason or "instagram_posts_fetch_failed"),
                        retryable=bool(result.retryable),
                        runtime_metadata={"fetch_reason": result.fetch_reason},
                    )

                if result.posts:
                    persisted = persist_instagram_posts(
                        account_handle=account_handle,
                        post_nodes=result.posts,
                        run_id=run_id or None,
                        job_id=job_id,
                        season_id=season_id,
                        source_scope=source_scope,
                    )
                    posts_fetched += len(result.posts)
                    posts_upserted += persisted.posts_upserted
                    posts_skipped += persisted.posts_skipped
                    for reason, count in persisted.posts_skipped_by_reason.items():
                        posts_skipped_by_reason[reason] = posts_skipped_by_reason.get(reason, 0) + int(count or 0)

                pages_fetched += 1
                repo._emit_job_progress(
                    job_id=job_id,
                    stage=stage,
                    platform="instagram",
                    account=account_handle,
                    scraped_posts=posts_fetched,
                    scraped_comments=0,
                    posts_upserted=posts_upserted,
                    comments_upserted=0,
                    activity={"phase": "posts_scrapling_running", "pages_fetched": pages_fetched},
                    progress_state=progress_state,
                    force=not result.has_next_page,
                )

                if not result.has_next_page or not result.end_cursor:
                    break
                if max_pages and pages_fetched >= max_pages:
                    break
                cursor = result.end_cursor

            fetcher_metadata = dict(fetcher.runtime_metadata)
            return auth_metadata, fetcher_metadata
        finally:
            fetcher_metadata = dict(fetcher.runtime_metadata)
            await fetcher.aclose()

    try:
        auth_metadata, fetcher_metadata = asyncio.run(_run_job())
        metadata = {
            "stage": stage,
            "platform": "instagram",
            "account": account_handle,
            "fast_mode": fast_mode,
            "source_scope": source_scope,
            "stage_counters": {"posts": posts_fetched, "pages": pages_fetched},
            "persist_counters": {
                "posts_upserted": posts_upserted,
                "posts_skipped": posts_skipped,
                "posts_skipped_by_reason": posts_skipped_by_reason,
            },
            "posts_scrapling_persist_diagnostics": {
                "posts_upserted": posts_upserted,
                "posts_skipped": posts_skipped,
                "posts_skipped_by_reason": posts_skipped_by_reason,
            },
            "activity": {"phase": "posts_scrapling_end", "last_progress_at": repo._iso(repo._now_utc())},
            "auth_context": {
                "session_source": auth_metadata.get("source"),
                "browser_account_id": auth_metadata.get("browser_account_id"),
                "validation_category": auth_metadata.get("validation_category"),
            },
            "fetcher_runtime": fetcher_metadata,
        }
        repo._finish_job(
            job_id,
            status="completed",
            items_found=posts_fetched,
            metadata=metadata,
        )
        terminal_status = "completed"
        terminal_error_message: str | None = None
    except ScraplingJobCancelledError as exc:
        repo._finish_job(
            job_id,
            status="cancelled",
            items_found=posts_fetched,
            error_message=str(exc),
            metadata={
                "stage": stage,
                "platform": "instagram",
                "account": account_handle,
                "fast_mode": fast_mode,
                "source_scope": source_scope,
                "cancelled": True,
                "cancel_scope": exc.cancel_scope,
                "job_status_at_cancel": exc.job_status,
                "run_status_at_cancel": exc.run_status,
                "activity": {"phase": "cancelled", "last_progress_at": repo._iso(repo._now_utc())},
                "stage_counters": {"posts": posts_fetched, "pages": pages_fetched},
                "persist_counters": {
                    "posts_upserted": posts_upserted,
                    "posts_skipped": posts_skipped,
                    "posts_skipped_by_reason": posts_skipped_by_reason,
                },
                "posts_scrapling_persist_diagnostics": {
                    "posts_upserted": posts_upserted,
                    "posts_skipped": posts_skipped,
                    "posts_skipped_by_reason": posts_skipped_by_reason,
                },
                "runtime_metadata": exc.runtime_metadata,
                "fetcher_runtime": fetcher_metadata,
            },
            last_error_code="instagram_posts_scrapling_cancelled",
            last_error_class=exc.__class__.__name__,
        )
        terminal_status = "cancelled"
        terminal_error_message = str(exc)
    except Exception as exc:  # noqa: BLE001
        runtime_error_code = str(getattr(exc, "error_code", "") or "").strip().lower()
        error_code = runtime_error_code or "instagram_posts_scrapling_failed"
        error_class = str(getattr(exc, "error_class", "") or exc.__class__.__name__).strip()
        retryable = bool(getattr(exc, "retryable", False))
        attempt_count = int(job.get("attempt_count") or 1)
        max_attempts = int(job.get("max_attempts") or 1)
        can_retry = retryable and attempt_count < max_attempts
        next_available_at = (
            repo._now_utc() + timedelta(seconds=repo._retry_backoff_seconds(attempt_count)) if can_retry else None
        )
        repo._finish_job(
            job_id,
            status="retrying" if can_retry else "failed",
            items_found=posts_fetched,
            error_message=str(exc),
            metadata={
                "stage": stage,
                "platform": "instagram",
                "account": account_handle,
                "fast_mode": fast_mode,
                "source_scope": source_scope,
                "error_code": error_code,
                "error_class": error_class,
                "activity": {"phase": "failed", "last_progress_at": repo._iso(repo._now_utc())},
                "persist_counters": {
                    "posts_upserted": posts_upserted,
                    "posts_skipped": posts_skipped,
                    "posts_skipped_by_reason": posts_skipped_by_reason,
                },
                "posts_scrapling_persist_diagnostics": {
                    "posts_upserted": posts_upserted,
                    "posts_skipped": posts_skipped,
                    "posts_skipped_by_reason": posts_skipped_by_reason,
                },
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
                repo._finalize_run_status(run_id)
            except pg.DatabaseServiceUnavailableError as exc:
                logger.warning(
                    "Deferred final posts run-status reconciliation after database saturation: run_id=%s error=%s",
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
            "Returning degraded posts job summary after database saturation: job_id=%s error=%s",
            job_id,
            exc,
        )
        return {
            "id": job_id,
            "run_id": run_id or None,
            "platform": "instagram",
            "job_type": str(job.get("job_type") or "posts").strip() or "posts",
            "status": terminal_status or "unknown",
            "items_found": posts_fetched,
            "error_message": terminal_error_message,
            "metadata": {
                "degraded_summary": True,
                "database_service_unavailable": True,
            },
        }
