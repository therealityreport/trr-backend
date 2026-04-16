"""Worker job orchestrator for the TikTok posts Scrapling lane.

Flow: session → proxy → fetcher → warmup → resolve_sec_uid → paginate → persist.
Keeps yt-dlp as independent co-primary — this lane is an alternative path.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from trr_backend.db import pg
from trr_backend.socials.tiktok.posts_scrapling.fetcher import TikTokPostsScraplingFetcher
from trr_backend.socials.tiktok.posts_scrapling.persistence import persist_tiktok_posts
from trr_backend.socials.tiktok.posts_scrapling.proxy import select_tiktok_posts_proxy
from trr_backend.socials.tiktok.posts_scrapling.session import resolve_tiktok_posts_session


@dataclass(slots=True)
class TikTokPostsScraplingRuntimeError(Exception):
    message: str
    error_code: str
    retryable: bool = False
    runtime_metadata: dict[str, Any] | None = None

    def __str__(self) -> str:
        return self.message


def run_tiktok_posts_scrapling_job(job: dict[str, Any], *, worker_id: str | None = None) -> dict[str, Any]:
    from trr_backend.repositories import social_season_analytics as repo

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

    progress_state = repo._new_job_progress_state()
    posts_fetched = 0
    posts_upserted = 0
    pages_fetched = 0
    fetcher_metadata: dict[str, Any] = {}

    async def _run_job() -> dict[str, Any]:
        nonlocal posts_fetched, posts_upserted, pages_fetched, fetcher_metadata

        session = resolve_tiktok_posts_session()
        proxy_config = select_tiktok_posts_proxy()
        fetcher = TikTokPostsScraplingFetcher(
            cookies=session.cookies,
            raw_cookies=session.raw_cookies,
            proxy_config=proxy_config,
        )
        try:
            await fetcher.warmup(account_handle)
            fetcher_metadata = dict(fetcher.runtime_metadata)
            repo._touch_job_heartbeat(job_id, worker_id=worker_id)

            sec_uid = await fetcher.resolve_sec_uid(account_handle)
            cursor: str | None = None

            while True:
                repo._touch_job_heartbeat(job_id, worker_id=worker_id)
                result = await fetcher.fetch_posts_page(sec_uid=sec_uid, cursor=cursor)

                if result.auth_failed:
                    raise TikTokPostsScraplingRuntimeError(
                        f"TikTok auth failed while fetching posts for @{account_handle}.",
                        error_code="tiktok_posts_auth_failed",
                        retryable=False,
                        runtime_metadata={"fetch_reason": result.fetch_reason},
                    )
                if result.fetch_failed and not result.posts:
                    raise TikTokPostsScraplingRuntimeError(
                        f"TikTok posts fetch failed for @{account_handle}: {result.fetch_reason}",
                        error_code=str(result.fetch_reason or "tiktok_posts_fetch_failed"),
                        retryable=bool(result.retryable),
                        runtime_metadata={"fetch_reason": result.fetch_reason},
                    )

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

                pages_fetched += 1
                repo._emit_job_progress(
                    job_id=job_id,
                    stage=stage,
                    platform="tiktok",
                    account=account_handle,
                    scraped_posts=posts_fetched,
                    scraped_comments=0,
                    posts_upserted=posts_upserted,
                    comments_upserted=0,
                    activity={"phase": "tiktok_posts_scrapling_running", "pages_fetched": pages_fetched},
                    progress_state=progress_state,
                    force=not result.has_more,
                )

                if not result.has_more or not result.cursor:
                    break
                if max_pages and pages_fetched >= max_pages:
                    break
                cursor = result.cursor

            return fetcher_metadata
        finally:
            await fetcher.aclose()

    try:
        fetcher_metadata = asyncio.run(_run_job())
        metadata = {
            "stage": stage,
            "platform": "tiktok",
            "account": account_handle,
            "stage_counters": {"posts": posts_fetched, "pages": pages_fetched},
            "persist_counters": {"posts_upserted": posts_upserted},
            "activity": {"phase": "tiktok_posts_scrapling_end", "last_progress_at": repo._iso(repo._now_utc())},
            "fetcher_runtime": fetcher_metadata,
        }
        repo._finish_job(
            job_id,
            status="completed",
            items_found=posts_fetched,
            metadata=metadata,
        )
    except Exception as exc:  # noqa: BLE001
        runtime_error_code = str(getattr(exc, "error_code", "") or "").strip().lower()
        error_code = runtime_error_code or "tiktok_posts_scrapling_failed"
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
                "platform": "tiktok",
                "account": account_handle,
                "error_code": error_code,
                "error_class": error_class,
                "activity": {"phase": "failed", "last_progress_at": repo._iso(repo._now_utc())},
                "persist_counters": {"posts_upserted": posts_upserted},
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
