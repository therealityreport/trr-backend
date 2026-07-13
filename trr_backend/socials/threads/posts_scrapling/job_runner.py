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
from trr_backend.socials.instagram import auth_cooldown
from trr_backend.socials.post_persist_truthfulness import apply_post_persist_truthfulness_metadata
from trr_backend.socials.rollout_flags import resolve_rollout_flag

from .fetcher import ThreadsPostsFetchResult, ThreadsPostsScraplingFetcher
from .persistence import persist_threads_posts
from .proxy import select_threads_posts_proxy
from .session import resolve_threads_posts_session

logger = logging.getLogger("socials.threads.posts_scrapling.job_runner")


class _LifecycleProxy:
    def __getattr__(self, name: str) -> Any:
        import trr_backend.socials.control_plane.run_lifecycle as lifecycle

        return getattr(lifecycle, name)


lifecycle = _LifecycleProxy()


@dataclass(slots=True)
class ThreadsPostsScraplingRuntimeError(Exception):
    message: str
    error_code: str
    retryable: bool = False
    runtime_metadata: dict[str, Any] | None = None

    def __str__(self) -> str:
        return self.message


@dataclass(slots=True)
class ThreadsPostsScraplingCancelledError(Exception):
    message: str
    cancel_scope: str
    job_status: str | None = None
    run_status: str | None = None
    runtime_metadata: dict[str, Any] | None = None

    def __str__(self) -> str:
        return self.message


ThreadsPostsScraplingCancelled = ThreadsPostsScraplingCancelledError

_OPERATION_TIMEOUT_SECONDS_DEFAULT = 240.0
_OPERATION_HEARTBEAT_INTERVAL_SECONDS_DEFAULT = 30.0
_THREADS_POSTS_SCRAPLING_ENABLED_ENV = "SOCIAL_THREADS_POSTS_SCRAPLING_ENABLED"
_TRANSIENT_ERROR_SNIPPETS = (
    "too many 429",
    "too many 500",
    "too many 502",
    "too many 503",
    "too many 504",
    "500 error responses",
    "502 error responses",
    "503 error responses",
    "504 error responses",
    "max retries exceeded",
    "read timed out",
    "connection aborted",
    "connection reset",
    "connection refused",
    "temporarily unavailable",
    "timeout",
)

_SENSITIVE_KEY_PARTS = (
    "api_proxy_url",
    "auth_token",
    "bearer",
    "browser_proxy",
    "cookie_value",
    "csrftoken",
    "password",
    "raw_cookie",
    "raw_cookies",
    "secret",
    "sessionid",
    "token",
)
_SAFE_SENSITIVE_KEY_EXCEPTIONS = {
    "auth_failed",
    "cookies_supplied",
    "selected_proxy_fingerprint",
    "proxy_fingerprint",
    "warmup_cookie_count",
    "warmup_cookie_names",
}


def _safe_metadata_value(value: Any) -> Any:
    if isinstance(value, dict):
        return _safe_runtime_metadata(value)
    if isinstance(value, list):
        return [_safe_metadata_value(item) for item in value]
    if isinstance(value, tuple):
        return [_safe_metadata_value(item) for item in value]
    return value


def _safe_runtime_metadata(metadata: dict[str, Any] | None) -> dict[str, Any]:
    safe: dict[str, Any] = {}
    for key, value in dict(metadata or {}).items():
        normalized_key = str(key or "").strip()
        lower_key = normalized_key.lower()
        if lower_key == "warmup_cookie_delta":
            safe["warmup_cookie_delta_names"] = sorted(str(name) for name in dict(value or {}).keys())
            continue
        if any(part in lower_key for part in _SENSITIVE_KEY_PARTS) and lower_key not in _SAFE_SENSITIVE_KEY_EXCEPTIONS:
            safe[f"{normalized_key}_present"] = bool(value)
            continue
        safe[normalized_key] = _safe_metadata_value(value)
    return safe


def _safe_auth_context(session: Any) -> dict[str, Any]:
    raw_cookies = dict(getattr(session, "raw_cookies", {}) or {})
    return {
        "cookie_source": str(getattr(session, "cookie_source", "") or "").strip() or None,
        "cookie_count": len(raw_cookies),
        "has_sessionid": bool(raw_cookies.get("sessionid")),
        "has_csrftoken": bool(raw_cookies.get("csrftoken")),
    }


def _auth_cooldown_metadata(cooldown: Any) -> dict[str, Any] | None:
    if cooldown is None:
        return None
    if hasattr(cooldown, "to_metadata"):
        return dict(cooldown.to_metadata())
    return dict(cooldown) if isinstance(cooldown, dict) else None


def _raise_if_threads_auth_cooldown_active(account_handle: str) -> None:
    cooldown = auth_cooldown.get_active_cooldown("threads", account_handle)
    metadata = _auth_cooldown_metadata(cooldown)
    if not metadata:
        return
    raise ThreadsPostsScraplingRuntimeError(
        f"Threads posts auth cooldown is active for @{account_handle}.",
        error_code=str(metadata.get("last_error_code") or "threads_posts_auth_cooldown_active"),
        retryable=True,
        runtime_metadata={"auth_cooldown": metadata, "auth_cooldown_active": True},
    )


def _resolve_operation_timeout_seconds() -> float:
    raw_value = str(os.getenv("SOCIAL_THREADS_POSTS_SCRAPLING_OPERATION_TIMEOUT_SECONDS") or "").strip()
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
    raw_value = str(os.getenv("SOCIAL_THREADS_POSTS_SCRAPLING_HEARTBEAT_INTERVAL_SECONDS") or "").strip()
    if not raw_value:
        return _OPERATION_HEARTBEAT_INTERVAL_SECONDS_DEFAULT
    try:
        parsed = float(raw_value)
    except (TypeError, ValueError):
        return _OPERATION_HEARTBEAT_INTERVAL_SECONDS_DEFAULT
    return min(max(parsed, 1.0), 120.0)


def _resolve_threads_posts_scrapling_rollout_flag() -> dict[str, Any]:
    return resolve_rollout_flag(_THREADS_POSTS_SCRAPLING_ENABLED_ENV, default_enabled=True)


def _transient_exception_error_code(exc: BaseException) -> str | None:
    normalized = f"{exc.__class__.__name__}: {exc}".strip().lower()
    if any(snippet in normalized for snippet in _TRANSIENT_ERROR_SNIPPETS):
        return "threads_posts_transient_transport_error"
    return None


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
            "Skipping Threads posts cancellation check after database saturation: job_id=%s error=%s",
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
                "Skipping Threads posts run cancellation check after database saturation: run_id=%s error=%s",
                run_id,
                exc,
            )
            return
        run_status = str(run_state.get("status") or "").strip().lower() or None
    cancel_scope = "job" if job_status == "cancelled" else "run" if run_status == "cancelled" else None
    if not cancel_scope:
        return

    metadata = _safe_runtime_metadata(runtime_metadata)
    logger.info(
        "threads_posts_scrapling cancellation_detected",
        extra={
            "event": "scrapling_job_cancelled",
            "job_id": job_id,
            "run_id": run_id or None,
            "cancel_scope": cancel_scope,
            "job_status": job_status,
            "run_status": run_status,
            "check_latency_ms": round((time.perf_counter() - started_at) * 1000, 2),
            "request_count": metadata.get("request_count"),
        },
    )
    raise ThreadsPostsScraplingCancelledError(
        "Threads posts Scrapling job was cancelled.",
        cancel_scope=cancel_scope,
        job_status=job_status,
        run_status=run_status,
        runtime_metadata=metadata,
    )


def run_threads_posts_scrapling_job(job: dict[str, Any], *, worker_id: str | None = None) -> dict[str, Any]:
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
    pipeline_ingest_mode = str(config.get("pipeline_ingest_mode") or "").strip().lower()
    rollout_flag = _resolve_threads_posts_scrapling_rollout_flag()
    threads_posts_scrapling_enabled = bool(rollout_flag["enabled"])

    if not account_handle:
        raise ThreadsPostsScraplingRuntimeError(
            "Threads posts Scrapling job is missing an account handle.",
            error_code="threads_posts_account_missing",
            retryable=False,
        )

    progress_state = lifecycle.new_job_progress_state()
    posts_fetched = 0
    posts_upserted = 0
    materialized_posts_upserted = 0
    catalog_posts_upserted = 0
    required_catalog_upsert_failures = 0
    posts_skipped = 0
    posts_skipped_by_reason: dict[str, int] = {}
    pages_fetched = 0
    fetcher_metadata: dict[str, Any] = {}
    auth_metadata: dict[str, Any] = {}
    stop_reason: str | None = None
    persistence_state = "not_started"
    terminal_status = "unknown"
    terminal_error_message: str | None = None
    started_monotonic = time.monotonic()

    def _merge_skipped_reasons(reasons: dict[str, int]) -> None:
        for reason, count in reasons.items():
            normalized_reason = str(reason or "").strip() or "unknown"
            posts_skipped_by_reason[normalized_reason] = int(posts_skipped_by_reason.get(normalized_reason) or 0) + int(
                count or 0
            )

    def _stage_counters() -> dict[str, int]:
        return {"posts": posts_fetched, "pages": pages_fetched}

    def _persist_counters() -> dict[str, Any]:
        counters = {
            "posts_upserted": posts_upserted,
            "materialized_posts_upserted": materialized_posts_upserted,
            "catalog_posts_upserted": catalog_posts_upserted,
            "posts_skipped": posts_skipped,
            "posts_skipped_by_reason": dict(posts_skipped_by_reason),
        }
        if required_catalog_upsert_failures:
            counters["required_catalog_upsert_failures"] = required_catalog_upsert_failures
        return counters

    def _listing_progress(*, partial: bool | None = None) -> dict[str, Any]:
        is_partial = stop_reason not in {None, "completed"} if partial is None else partial
        return {
            "page_index": pages_fetched,
            "posts_seen": posts_fetched,
            "posts_upserted": posts_upserted,
            "posts_skipped": posts_skipped,
            "stop_reason": stop_reason,
            "partial": bool(is_partial),
        }

    def _fetch_counters() -> dict[str, int]:
        return {"request_count": int(fetcher_metadata.get("request_count") or 0)}

    def _fetcher_state() -> dict[str, Any]:
        return {
            "transport": fetcher_metadata.get("transport"),
            "fallback_chain": list(fetcher_metadata.get("fallback_chain") or []),
            "stop_reason": fetcher_metadata.get("stop_reason"),
            "retryable": bool(fetcher_metadata.get("retryable")),
            "complete": bool(fetcher_metadata.get("complete")),
            "request_count": int(fetcher_metadata.get("request_count") or 0),
            "selected_proxy_fingerprint": fetcher_metadata.get("selected_proxy_fingerprint"),
        }

    def _persistence_state_payload() -> dict[str, Any]:
        return {"state": persistence_state, **_persist_counters()}

    def _activity(phase: str) -> dict[str, Any]:
        return {
            "phase": phase,
            "last_progress_at": lifecycle.format_time(lifecycle.now_utc()),
            "runtime_seconds": round(time.monotonic() - started_monotonic, 3),
        }

    def _runtime_payload() -> dict[str, Any]:
        return {"runtime_seconds": round(time.monotonic() - started_monotonic, 3)}

    def _terminal_metadata(
        phase: str,
        *,
        partial: bool,
        runtime_metadata: dict[str, Any] | None = None,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        metadata = {
            "stage": stage,
            "platform": "threads",
            "account": account_handle,
            "fast_mode": fast_mode,
            "pipeline_ingest_mode": pipeline_ingest_mode or None,
            "threads_posts_scrapling_enabled": threads_posts_scrapling_enabled,
            "rollout_flags": {
                "threads_posts_scrapling": dict(rollout_flag),
            },
            "progress": {
                "scraped_posts": posts_fetched,
                "scraped_comments": 0,
                "posts_upserted": posts_upserted,
                "comments_upserted": 0,
                "pages_fetched": pages_fetched,
            },
            "stage_counters": _stage_counters(),
            "persist_counters": _persist_counters(),
            "threads_posts_scrapling_persist_diagnostics": _persist_counters(),
            "listing_progress": _listing_progress(partial=partial),
            "runtime": _runtime_payload(),
            "fetcher_state": _fetcher_state(),
            "persistence_state": _persistence_state_payload(),
            "stop_reason": stop_reason,
            "fetch_counters": _fetch_counters(),
            "activity": _activity(phase),
            "auth_context": auth_metadata,
            "runtime_metadata": runtime_metadata
            or {
                "fetcher_runtime": fetcher_metadata,
                "listing_progress": _listing_progress(partial=partial),
                "persistence_state": _persistence_state_payload(),
            },
            "fetcher_runtime": fetcher_metadata,
            "source_runtime": fetcher_metadata,
        }
        if extra:
            metadata.update(extra)
        return metadata

    async def _run_job() -> dict[str, Any]:
        nonlocal posts_fetched, posts_upserted, materialized_posts_upserted, catalog_posts_upserted
        nonlocal required_catalog_upsert_failures
        nonlocal posts_skipped, pages_fetched
        nonlocal fetcher_metadata, auth_metadata, stop_reason, persistence_state

        _raise_if_threads_auth_cooldown_active(account_handle)
        session = resolve_threads_posts_session()
        auth_metadata = _safe_auth_context(session)
        proxy_config = select_threads_posts_proxy()
        fetcher = ThreadsPostsScraplingFetcher(
            cookies=session.cookies,
            raw_cookies=session.raw_cookies,
            proxy_config=proxy_config,
            fast_mode=fast_mode,
        )

        def _fetcher_runtime_metadata() -> dict[str, Any]:
            return _safe_runtime_metadata(dict(getattr(fetcher, "runtime_metadata", {}) or {}))

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
                        raise ThreadsPostsScraplingRuntimeError(
                            f"Threads posts operation timed out while {phase} for @{account_handle}.",
                            error_code=f"threads_posts_{phase}_timeout",
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
            fetcher_metadata = _fetcher_runtime_metadata()
            _raise_if_cancelled(
                job_id=job_id,
                run_id=run_id,
                runtime_metadata={**fetcher_metadata, "auth_context": auth_metadata},
            )
            await _await_operation_with_heartbeat(fetcher.warmup(account_handle), phase="warmup")
            fetcher_metadata = _fetcher_runtime_metadata()
            lifecycle.touch_job_heartbeat(job_id, worker_id=worker_id)
            _raise_if_cancelled(job_id=job_id, run_id=run_id, runtime_metadata=fetcher_metadata)

            result = await _await_operation_with_heartbeat(
                fetcher.fetch_posts(account_handle, max_pages=max_pages),
                phase="fetch_posts",
            )
            fetcher_metadata = _fetcher_runtime_metadata()
            pages_fetched = max(1, int(fetcher_metadata.get("pages_fetched") or 0))
            stop_reason = str(result.fetch_reason or fetcher_metadata.get("stop_reason") or "").strip() or None
            _raise_if_cancelled(job_id=job_id, run_id=run_id, runtime_metadata=fetcher_metadata)

            if result.auth_failed and not result.posts:
                error_code = str(result.fetch_reason or "threads_posts_auth_failed").strip()
                cooldown = auth_cooldown.record_auth_block("threads", account_handle, error_code)
                cooldown_metadata = _auth_cooldown_metadata(cooldown)
                raise ThreadsPostsScraplingRuntimeError(
                    f"Threads posts auth failed for @{account_handle}.",
                    error_code=error_code,
                    retryable=True,
                    runtime_metadata={
                        **fetcher_metadata,
                        "fetch_reason": result.fetch_reason,
                        "auth_cooldown": cooldown_metadata,
                        "auth_cooldown_recorded": bool(cooldown_metadata),
                    },
                )

            if result.fetch_failed and not result.posts:
                legacy_scraper = ThreadsScraper(
                    cookies=session.raw_cookies,
                    proxy_url=proxy_config.api_proxy_url if proxy_config else None,
                )
                legacy_config = ThreadsScrapeConfig(
                    username=account_handle,
                    delay_seconds=0,
                    max_pages=max_pages,
                    fast_mode=fast_mode,
                )
                legacy_posts = await _await_operation_with_heartbeat(
                    asyncio.to_thread(legacy_scraper.scrape, legacy_config),
                    phase="legacy_scraper",
                )
                legacy_runtime = _safe_runtime_metadata(dict(getattr(legacy_scraper, "runtime_metadata", {}) or {}))
                legacy_pages_fetched = int(
                    dict(getattr(legacy_scraper, "last_retrieval_meta", {}) or {}).get("pages_scanned") or 0
                )
                legacy_runtime["fallback_to_legacy"] = True
                fetcher_metadata = _safe_runtime_metadata(
                    {
                        **fetcher_metadata,
                        "legacy_runtime": legacy_runtime,
                        "fallback_chain": list(fetcher_metadata.get("fallback_chain") or [])
                        + ["legacy_threads_scraper"],
                        "transport": "legacy_threads_scraper",
                        "request_count": int(fetcher_metadata.get("request_count") or 0)
                        + int(legacy_runtime.get("request_count") or 0),
                        "complete": bool(legacy_runtime.get("complete")),
                        "retryable": bool(legacy_runtime.get("retryable")),
                        "stop_reason": legacy_runtime.get("stop_reason") or result.fetch_reason,
                        "pages_fetched": legacy_pages_fetched,
                    }
                )
                pages_fetched = max(1, int(fetcher_metadata.get("pages_fetched") or 0))
                stop_reason = str(fetcher_metadata.get("stop_reason") or result.fetch_reason or "").strip() or None
                result = ThreadsPostsFetchResult(
                    posts=list(legacy_posts),
                    fetch_failed=False,
                    auth_failed=False,
                    retryable=False,
                    fetch_reason=result.fetch_reason,
                )
                _raise_if_cancelled(job_id=job_id, run_id=run_id, runtime_metadata=fetcher_metadata)

            if result.fetch_failed and not result.posts:
                stop_reason = str(result.fetch_reason or "threads_posts_fetch_failed").strip()
                raise ThreadsPostsScraplingRuntimeError(
                    f"Threads posts fetch failed for @{account_handle}.",
                    error_code=str(result.fetch_reason or "threads_posts_fetch_failed"),
                    retryable=bool(result.retryable),
                    runtime_metadata={**fetcher_metadata, "fetch_reason": result.fetch_reason},
                )

            persistence_state = "running"
            persisted = persist_threads_posts(
                account_handle=account_handle,
                posts=list(result.posts),
                run_id=run_id or None,
                job_id=job_id or None,
                season_id=season_id,
                pipeline_ingest_mode=pipeline_ingest_mode,
            )
            posts_fetched = len(result.posts)
            materialized_posts_upserted = persisted.posts_upserted
            catalog_posts_upserted = int(getattr(persisted, "catalog_posts_upserted", 0) or 0)
            required_catalog_upsert_failures = int(getattr(persisted, "required_catalog_upsert_failures", 0) or 0)
            posts_upserted = max(materialized_posts_upserted, catalog_posts_upserted)
            posts_skipped = persisted.posts_skipped
            _merge_skipped_reasons(dict(getattr(persisted, "posts_skipped_by_reason", {}) or {}))
            persistence_state = "completed"
            fetch_incomplete = bool(result.fetch_failed or result.retryable) or (
                "complete" in fetcher_metadata
                and not bool(fetcher_metadata["complete"])
                and stop_reason not in {"max_posts_reached", "max_pages"}
            )
            if fetch_incomplete:
                raise ThreadsPostsScraplingRuntimeError(
                    f"Threads posts fetch was incomplete for @{account_handle}; saved posts will be retried.",
                    error_code=str(result.fetch_reason or "threads_posts_fetch_incomplete"),
                    retryable=True,
                    runtime_metadata={
                        **fetcher_metadata,
                        "fetch_incomplete": True,
                        "fetch_reason": result.fetch_reason,
                    },
                )
            if required_catalog_upsert_failures:
                raise ThreadsPostsScraplingRuntimeError(
                    f"Threads shared catalog persistence was incomplete for @{account_handle}; "
                    "saved posts will be retried.",
                    error_code="threads_shared_catalog_persistence_incomplete",
                    retryable=True,
                    runtime_metadata={
                        **fetcher_metadata,
                        "required_catalog_upsert_failures": required_catalog_upsert_failures,
                    },
                )
            if posts_fetched > 0 and posts_upserted > 0:
                with contextlib.suppress(Exception):
                    auth_cooldown.clear_cooldown("threads", account_handle)
            if stop_reason in {None, "", "complete"}:
                stop_reason = "completed"
            final_fetcher_runtime = _fetcher_runtime_metadata()
            if fetcher_metadata:
                final_fetcher_runtime.update(fetcher_metadata)
            fetcher_metadata = _safe_runtime_metadata(final_fetcher_runtime)
            _raise_if_cancelled(job_id=job_id, run_id=run_id, runtime_metadata=fetcher_metadata)

            lifecycle.emit_job_progress(
                job_id=job_id,
                stage=stage,
                platform="threads",
                account=account_handle,
                scraped_posts=posts_fetched,
                scraped_comments=0,
                posts_upserted=posts_upserted,
                comments_upserted=0,
                activity={
                    "phase": "threads_posts_scrapling_running",
                    "pages_fetched": pages_fetched,
                    "listing_progress": _listing_progress(partial=stop_reason != "completed"),
                },
                progress_state=progress_state,
                force=True,
            )
            return fetcher_metadata
        finally:
            final_runtime = _fetcher_runtime_metadata()
            if fetcher_metadata:
                final_runtime.update(fetcher_metadata)
            fetcher_metadata = _safe_runtime_metadata(final_runtime)
            await fetcher.aclose()

    try:
        if not threads_posts_scrapling_enabled:
            stop_reason = "threads_posts_scrapling_disabled"
            raise ThreadsPostsScraplingRuntimeError(
                f"Threads posts Scrapling job is disabled by {_THREADS_POSTS_SCRAPLING_ENABLED_ENV}.",
                error_code="threads_posts_scrapling_disabled",
                retryable=False,
                runtime_metadata={
                    "disabled_reason": "disabled_by_env",
                    "threads_posts_scrapling_enabled": False,
                    "rollout_flag": dict(rollout_flag),
                },
            )

        fetcher_metadata = asyncio.run(_run_job())
        _raise_if_cancelled(job_id=job_id, run_id=run_id, runtime_metadata=fetcher_metadata)
        metadata = _terminal_metadata(
            "threads_posts_scrapling_end",
            partial=stop_reason not in {None, "completed"},
        )
        metadata = apply_post_persist_truthfulness_metadata(
            metadata,
            platform="threads",
            account=account_handle,
            status="completed",
            posts_checked=posts_fetched,
            posts_upserted=posts_upserted,
            posts_skipped=posts_skipped,
            posts_skipped_by_reason=posts_skipped_by_reason,
            alias_keys=("threads_posts_scrapling_persist_diagnostics",),
        )
        metadata["threads_posts_scrapling_persist_diagnostics"] = metadata["persist_counters"]
        lifecycle.finish_job(
            job_id,
            status="completed",
            items_found=posts_fetched,
            metadata=metadata,
        )
        terminal_status = "completed"
        terminal_error_message = None
    except ThreadsPostsScraplingCancelledError as exc:
        metadata = _terminal_metadata(
            "cancelled",
            partial=True,
            runtime_metadata={
                "cancellation": _safe_runtime_metadata(exc.runtime_metadata),
                "fetcher_runtime": fetcher_metadata,
                "listing_progress": _listing_progress(partial=True),
                "persistence_state": _persistence_state_payload(),
            },
            extra={
                "cancelled": True,
                "cancel_scope": exc.cancel_scope,
                "job_status_at_cancel": exc.job_status,
                "run_status_at_cancel": exc.run_status,
                "error_code": "threads_posts_scrapling_cancelled",
                "error_class": exc.__class__.__name__,
            },
        )
        lifecycle.finish_job(
            job_id,
            status="cancelled",
            items_found=posts_fetched,
            error_message=str(exc),
            metadata=metadata,
            last_error_code="threads_posts_scrapling_cancelled",
            last_error_class=exc.__class__.__name__,
        )
        terminal_status = "cancelled"
        terminal_error_message = str(exc)
    except Exception as exc:  # noqa: BLE001
        runtime_error_code = str(getattr(exc, "error_code", "") or "").strip().lower()
        transient_error_code = _transient_exception_error_code(exc)
        error_code = runtime_error_code or transient_error_code or "threads_posts_scrapling_failed"
        error_class = str(getattr(exc, "error_class", "") or exc.__class__.__name__).strip()
        retryable = bool(getattr(exc, "retryable", False)) or bool(transient_error_code)
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
        runtime_metadata = _safe_runtime_metadata(dict(getattr(exc, "runtime_metadata", {}) or {}))
        lifecycle.finish_job(
            job_id,
            status="retrying" if can_retry else "failed",
            items_found=posts_fetched,
            error_message=str(exc),
            metadata=_terminal_metadata(
                "failed",
                partial=True,
                runtime_metadata={
                    "error": runtime_metadata,
                    "fetcher_runtime": fetcher_metadata,
                    "listing_progress": _listing_progress(partial=True),
                    "persistence_state": _persistence_state_payload(),
                },
                extra={
                    "error_code": error_code,
                    "error_class": error_class,
                    "retryable": retryable,
                },
            ),
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
                    "Deferred final Threads posts run-status reconciliation after database saturation: "
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
            "Returning degraded Threads posts job summary after database saturation: job_id=%s error=%s",
            job_id,
            exc,
        )
        return {
            "id": job_id,
            "run_id": run_id or None,
            "platform": "threads",
            "job_type": str(job.get("job_type") or "posts").strip() or "posts",
            "status": terminal_status,
            "items_found": posts_fetched,
            "error_message": terminal_error_message,
            "metadata": {
                "degraded_summary": True,
                "database_service_unavailable": True,
                "stage": stage,
                "platform": "threads",
                "account": account_handle,
                "threads_posts_scrapling_enabled": threads_posts_scrapling_enabled,
                "rollout_flags": {
                    "threads_posts_scrapling": dict(rollout_flag),
                },
                "stage_counters": _stage_counters(),
                "persist_counters": _persist_counters(),
                "listing_progress": _listing_progress(partial=terminal_status != "completed"),
                "stop_reason": stop_reason,
                "fetcher_runtime": fetcher_metadata,
                "persistence_state": _persistence_state_payload(),
                "activity": _activity("degraded_summary"),
            },
        }
