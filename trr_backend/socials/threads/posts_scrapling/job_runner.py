from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import timedelta
from typing import Any

from trr_backend.db import pg

from .fetcher import ThreadsPostsFetchResult, ThreadsPostsScraplingFetcher
from .persistence import persist_threads_posts
from .proxy import select_threads_posts_proxy
from .session import resolve_threads_posts_session


@dataclass(slots=True)
class ThreadsPostsScraplingRuntimeError(Exception):
    message: str
    error_code: str
    retryable: bool = False
    runtime_metadata: dict[str, Any] | None = None

    def __str__(self) -> str:
        return self.message


def run_threads_posts_scrapling_job(job: dict[str, Any], *, worker_id: str | None = None) -> dict[str, Any]:
    from trr_backend.repositories import social_season_analytics as repo
    from trr_backend.socials.threads import ThreadsScrapeConfig, ThreadsScraper

    job_id = str(job.get("id") or "").strip()
    run_id = str(job.get("run_id") or "").strip()
    config = dict(job.get("config") or {})
    account_handle = str(config.get("account") or "").strip().lower().lstrip("@")
    stage = str(config.get("stage") or "threads_posts_scrapling").strip().lower()
    max_pages_raw = config.get("max_pages")
    max_pages: int | None = int(max_pages_raw) if max_pages_raw not in (None, 0, "") else None
    fast_mode = bool(config.get("fast_mode", False))
    season_id = str(config.get("season_id") or "").strip() or None

    if not account_handle:
        raise ThreadsPostsScraplingRuntimeError(
            "Threads posts Scrapling job is missing an account handle.",
            error_code="threads_posts_account_missing",
            retryable=False,
        )

    progress_state = repo._new_job_progress_state()
    posts_fetched = 0
    posts_upserted = 0
    fetcher_metadata: dict[str, Any] = {}

    async def _run_job() -> tuple[dict[str, Any], dict[str, Any]]:
        nonlocal posts_fetched, posts_upserted, fetcher_metadata

        session = resolve_threads_posts_session()
        proxy_config = select_threads_posts_proxy()
        fetcher = ThreadsPostsScraplingFetcher(
            cookies=session.cookies,
            raw_cookies=session.raw_cookies,
            proxy_config=proxy_config,
            fast_mode=fast_mode,
        )
        try:
            await fetcher.warmup(account_handle)
            repo._touch_job_heartbeat(job_id, worker_id=worker_id)
            result = await fetcher.fetch_posts(account_handle, max_pages=max_pages)
            fetcher_metadata = dict(fetcher.runtime_metadata)

            if result.fetch_failed and not result.posts:
                legacy_scraper = ThreadsScraper(cookies=session.raw_cookies)
                legacy_config = ThreadsScrapeConfig(
                    username=account_handle,
                    delay_seconds=0,
                    max_pages=max_pages,
                    fast_mode=fast_mode,
                )
                legacy_posts = legacy_scraper.scrape(legacy_config)
                legacy_runtime = dict(getattr(legacy_scraper, "runtime_metadata", {}) or {})
                legacy_runtime["fallback_to_legacy"] = True
                fetcher_metadata = {
                    **fetcher_metadata,
                    "legacy_runtime": legacy_runtime,
                    "fallback_chain": list(fetcher_metadata.get("fallback_chain") or []) + ["legacy_threads_scraper"],
                    "transport": "legacy_threads_scraper",
                    "request_count": int(fetcher_metadata.get("request_count") or 0)
                    + int(legacy_runtime.get("request_count") or 0),
                    "complete": bool(legacy_runtime.get("complete")),
                    "retryable": bool(legacy_runtime.get("retryable")),
                    "stop_reason": legacy_runtime.get("stop_reason") or result.fetch_reason,
                }
                result = ThreadsPostsFetchResult(
                    posts=list(legacy_posts),
                    fetch_failed=False,
                    auth_failed=False,
                    retryable=False,
                    fetch_reason=result.fetch_reason,
                )

            if result.fetch_failed and not result.posts:
                raise ThreadsPostsScraplingRuntimeError(
                    f"Threads posts fetch failed for @{account_handle}.",
                    error_code=str(result.fetch_reason or "threads_posts_fetch_failed"),
                    retryable=bool(result.retryable),
                    runtime_metadata={"fetch_reason": result.fetch_reason},
                )

            persisted = persist_threads_posts(
                account_handle=account_handle,
                posts=list(result.posts),
                run_id=run_id or None,
                job_id=job_id or None,
                season_id=season_id,
            )
            posts_fetched = len(result.posts)
            posts_upserted = persisted.posts_upserted
            repo._emit_job_progress(
                job_id=job_id,
                stage=stage,
                platform="threads",
                account=account_handle,
                scraped_posts=posts_fetched,
                scraped_comments=0,
                posts_upserted=posts_upserted,
                comments_upserted=0,
                activity={"phase": "threads_posts_scrapling_running", "pages_fetched": 1},
                progress_state=progress_state,
                force=True,
            )
            return {"cookie_source": session.cookie_source}, dict(fetcher.runtime_metadata) | {
                "fallback_chain": list(
                    fetcher_metadata.get("fallback_chain") or fetcher.runtime_metadata.get("fallback_chain") or []
                )
            }
        finally:
            final_runtime = dict(fetcher.runtime_metadata)
            if fetcher_metadata:
                final_runtime.update(fetcher_metadata)
            fetcher_metadata = final_runtime
            await fetcher.aclose()

    try:
        auth_metadata, fetcher_metadata = asyncio.run(_run_job())
        metadata = {
            "stage": stage,
            "platform": "threads",
            "account": account_handle,
            "fast_mode": fast_mode,
            "stage_counters": {"posts": posts_fetched},
            "persist_counters": {"posts_upserted": posts_upserted},
            "fetch_counters": {"request_count": int(fetcher_metadata.get("request_count") or 0)},
            "activity": {"phase": "threads_posts_scrapling_end", "last_progress_at": repo._iso(repo._now_utc())},
            "auth_context": auth_metadata,
            "fetcher_runtime": fetcher_metadata,
            "source_runtime": fetcher_metadata,
        }
        repo._finish_job(
            job_id,
            status="completed",
            items_found=posts_fetched,
            metadata=metadata,
        )
    except Exception as exc:  # noqa: BLE001
        runtime_error_code = str(getattr(exc, "error_code", "") or "").strip().lower()
        error_code = runtime_error_code or "threads_posts_scrapling_failed"
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
                "platform": "threads",
                "account": account_handle,
                "fast_mode": fast_mode,
                "error_code": error_code,
                "error_class": error_class,
                "activity": {"phase": "failed", "last_progress_at": repo._iso(repo._now_utc())},
                "persist_counters": {"posts_upserted": posts_upserted},
                "runtime_metadata": getattr(exc, "runtime_metadata", None),
                "fetcher_runtime": fetcher_metadata,
                "source_runtime": fetcher_metadata,
                "fetch_counters": {"request_count": int(fetcher_metadata.get("request_count") or 0)},
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
