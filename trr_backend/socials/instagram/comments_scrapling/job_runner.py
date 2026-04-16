from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from trr_backend.db import pg
from trr_backend.socials.instagram.comments_scrapling.fetcher import InstagramCommentsScraplingFetcher
from trr_backend.socials.instagram.comments_scrapling.persistence import persist_instagram_comments_for_post
from trr_backend.socials.instagram.comments_scrapling.proxy import select_comments_proxy
from trr_backend.socials.instagram.comments_scrapling.session import resolve_comments_scrapling_session


@dataclass(slots=True)
class CommentsScraplingRuntimeError(Exception):
    message: str
    error_code: str
    retryable: bool = False
    runtime_metadata: dict[str, Any] | None = None

    def __str__(self) -> str:
        return self.message


def run_instagram_comments_scrapling_job(job: dict[str, Any], *, worker_id: str | None = None) -> dict[str, Any]:
    from trr_backend.repositories import social_season_analytics as repo

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

    progress_state = repo._new_job_progress_state()
    processed_posts = 0
    comments_upserted = 0
    comments_fetched = 0
    comments_marked_missing = 0
    mirror_jobs_enqueued = 0
    mirror_job_enqueue_errors = 0
    activity: dict[str, Any] = {
        "phase": "comments_scrapling_start",
        "posts_checked": len(target_source_ids),
        "matched_posts": 0,
        "saved_posts": 0,
        "total_posts": len(target_source_ids),
    }
    fetcher_metadata: dict[str, Any] = {}

    # Single event loop: fetcher is created, warmed up, used for all
    # shortcodes, and closed within one asyncio.run(). The httpx client
    # and Patchright browser share the same loop lifetime.
    async def _run_job() -> tuple[dict[str, Any], dict[str, Any]]:
        nonlocal processed_posts, comments_upserted, comments_fetched
        nonlocal comments_marked_missing, mirror_jobs_enqueued, mirror_job_enqueue_errors
        nonlocal activity, fetcher_metadata

        session = resolve_comments_scrapling_session(
            browser_account_id=account_handle,
            caller_context=f"comments_scrapling:{mode}:{account_handle}",
        )
        proxy_config = select_comments_proxy()
        fetcher = InstagramCommentsScraplingFetcher(
            cookies=session.cookies,
            raw_cookies=session.auth_session.cookies,
            browser_account_id=session.browser_account_id,
            proxy_config=proxy_config,
        )
        try:
            await fetcher.warmup()
            auth_metadata = dict(session.auth_session.metadata or {})
            fetcher_metadata = dict(fetcher.runtime_metadata)

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
                repo._touch_job_heartbeat(job_id, worker_id=worker_id)
                result = await fetcher.fetch_comments_for_shortcode(
                    shortcode,
                    max_comments=max_comments_per_post,
                    fetch_replies=fetch_replies,
                )
                if result.auth_failed:
                    raise CommentsScraplingRuntimeError(
                        f"Instagram auth failed while fetching comments for {shortcode}.",
                        error_code="instagram_comments_auth_failed",
                        retryable=False,
                        runtime_metadata={"shortcode": shortcode, "fetch_reason": result.fetch_reason},
                    )
                if result.fetch_failed and not result.comments:
                    raise CommentsScraplingRuntimeError(
                        f"Instagram comments fetch failed for {shortcode}.",
                        error_code=str(result.fetch_reason or "instagram_comments_fetch_failed"),
                        retryable=bool(result.retryable),
                        runtime_metadata={"shortcode": shortcode, "fetch_reason": result.fetch_reason},
                    )
                persisted = persist_instagram_comments_for_post(
                    account_handle=account_handle,
                    shortcode=shortcode,
                    comments=result.comments,
                    run_id=run_id or None,
                    job_id=job_id,
                    is_complete=not result.fetch_failed and not result.auth_failed and max_comments_per_post > 0,
                    source_scope=source_scope,
                )
                processed_posts += 1
                comments_fetched += len(result.comments)
                comments_upserted += persisted.comments_upserted
                comments_marked_missing += persisted.comments_marked_missing
                mirror_jobs_enqueued += persisted.comment_media_mirror_jobs_enqueued
                mirror_job_enqueue_errors += persisted.comment_media_mirror_job_enqueue_errors
                activity = {
                    "phase": "comments_scrapling_running",
                    "posts_checked": len(target_source_ids),
                    "matched_posts": index,
                    "saved_posts": processed_posts,
                    "total_posts": len(target_source_ids),
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

            return auth_metadata, fetcher_metadata
        finally:
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
            "stage_counters": {"posts": processed_posts, "comments": comments_fetched},
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
                "session_source": auth_metadata.get("source"),
                "browser_account_id": auth_metadata.get("browser_account_id"),
                "validation_category": auth_metadata.get("validation_category"),
            },
            "fetcher_runtime": fetcher_metadata,
        }
        repo._finish_job(
            job_id,
            status="completed",
            items_found=processed_posts + comments_fetched,
            metadata=metadata,
        )
    except Exception as exc:  # noqa: BLE001
        runtime_error_code = str(getattr(exc, "error_code", "") or "").strip().lower()
        error_code = runtime_error_code or "instagram_comments_scrapling_failed"
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
            items_found=processed_posts + comments_fetched,
            error_message=str(exc),
            metadata={
                "stage": stage,
                "platform": "instagram",
                "account": account_handle,
                "mode": mode,
                "source_scope": source_scope,
                "target_source_ids": target_source_ids,
                "error_code": error_code,
                "error_class": error_class,
                "activity": {"phase": "failed", "last_progress_at": repo._iso(repo._now_utc())},
                "persist_counters": {
                    "posts_upserted": processed_posts,
                    "comments_upserted": comments_upserted,
                },
                "runtime_metadata": getattr(exc, "runtime_metadata", None),
                "fetcher_runtime": fetcher_metadata,
            },
            last_error_code=error_code,
            last_error_class=error_class,
            next_available_at=next_available_at,
        )
    finally:
        if run_id:
            repo._finalize_run_status(run_id)

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
