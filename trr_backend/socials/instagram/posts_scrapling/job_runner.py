from __future__ import annotations

import asyncio
import logging
import os
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
from trr_backend.socials.instagram.posts_scrapling.proxy import posts_proxy_feature_flags, select_posts_proxy
from trr_backend.socials.instagram.posts_scrapling.session import resolve_posts_scrapling_session

logger = logging.getLogger("socials.instagram.posts_scrapling.job_runner")


class _LifecycleProxy:
    def __getattr__(self, name: str) -> Any:
        import trr_backend.socials.control_plane.run_lifecycle as lifecycle

        return getattr(lifecycle, name)


lifecycle = _LifecycleProxy()


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


def _normalize_proxy_session_part(value: Any) -> str | None:
    normalized = str(value or "").strip().lower().lstrip("@")
    return normalized or None


def _coerce_proxy_session_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _posts_pagination_timeout_guard_seconds(config: dict[str, Any]) -> float | None:
    raw = config.get("pagination_timeout_guard_seconds")
    if raw in (None, ""):
        raw = os.getenv("SOCIAL_INSTAGRAM_POSTS_PAGINATION_TIMEOUT_GUARD_SECONDS") or ""
    if raw in (None, ""):
        return 105 * 60.0
    try:
        parsed = float(raw)
    except (TypeError, ValueError):
        return 105 * 60.0
    return parsed if parsed > 0 else None


def _posts_bidirectional_walk_enabled() -> bool:
    return str(os.getenv("SOCIAL_INSTAGRAM_POSTS_BIDIRECTIONAL_WALK_ENABLED") or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _posts_pagination_doc_ids_attempted(metadata: dict[str, Any]) -> list[str]:
    for key in ("doc_ids_attempted", "profile_posts_doc_ids_attempted", "profile_posts_doc_ids"):
        value = metadata.get(key)
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
    return []


def _posts_runtime_proxy_fingerprint(metadata: dict[str, Any]) -> str | None:
    for key in ("proxy_fingerprint", "selected_proxy_fingerprint"):
        value = str(metadata.get(key) or "").strip()
        if value:
            return value
    proxy_identity = metadata.get("proxy_identity")
    if isinstance(proxy_identity, dict):
        value = str(proxy_identity.get("configured_fingerprint") or "").strip()
        if value:
            return value
    return None


def _posts_node_identity(post: dict[str, Any]) -> str | None:
    if not isinstance(post, dict):
        return None
    for key in ("id", "pk", "media_id", "code", "shortcode"):
        value = str(post.get(key) or "").strip()
        if value:
            return value
    return None


def _posts_pagination_stop_reason(result: Any) -> str | None:
    reason = str(getattr(result, "fetch_reason", "") or "").strip().lower()
    if reason in {"pagination_doc_id_stale", "graphql_no_doc_id_succeeded"}:
        return reason
    if "cursor" in reason and ("expired" in reason or "stale" in reason or "invalid" in reason):
        return "cursor_expired_restart_required"
    return None


def _posts_proxy_session_key(
    *,
    account_handle: str,
    stage: str,
    config: dict[str, Any],
    job_metadata: dict[str, Any],
    browser_account_id: str | None,
) -> str:
    account = (
        _normalize_proxy_session_part(account_handle)
        or _normalize_proxy_session_part(browser_account_id)
        or "unknown"
    )
    normalized_stage = _normalize_proxy_session_part(stage) or "posts_scrapling"

    detail_shard_count = max(1, _coerce_proxy_session_int(config.get("details_refresh_shard_count"), 1))
    detail_shard_index = config.get("details_refresh_shard_index")
    if detail_shard_count > 1 and detail_shard_index not in (None, ""):
        return f"{account}:{normalized_stage}:details:{_coerce_proxy_session_int(detail_shard_index)}"

    shard_count = max(1, _coerce_proxy_session_int(config.get("shard_count") or config.get("posts_shard_count"), 1))
    shard_index = config.get("shard_index", config.get("posts_shard_index"))
    if shard_count > 1 and shard_index not in (None, ""):
        return f"{account}:{normalized_stage}:posts:{_coerce_proxy_session_int(shard_index)}"

    worker_lane = _normalize_proxy_session_part(
        config.get("runner_lane") or config.get("worker_lane") or job_metadata.get("worker_lane")
    )
    if worker_lane:
        return f"{account}:{normalized_stage}:lane:{worker_lane}"

    return str(browser_account_id or account).strip().lower().lstrip("@") or account


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
    job_metadata = dict(job.get("metadata") or {})
    account_handle = str(config.get("account") or "").strip().lower().lstrip("@")
    stage = str(config.get("stage") or "posts_scrapling").strip().lower()
    max_pages_raw = config.get("max_pages")
    max_pages: int | None = int(max_pages_raw) if max_pages_raw not in (None, 0, "") else None
    fast_mode = bool(config.get("fast_mode", False))
    source_scope = str(config.get("source_scope") or "network").strip().lower() or "network"
    season_id = str(config.get("season_id") or "").strip() or None

    if not account_handle:
        raise PostsScraplingRuntimeError(
            "Instagram posts Scrapling job is missing an account handle.",
            error_code="instagram_posts_account_missing",
            retryable=False,
        )

    progress_state = lifecycle.new_job_progress_state()
    posts_fetched = 0
    posts_upserted = 0
    posts_skipped = 0
    posts_skipped_by_reason: dict[str, int] = {}
    pages_fetched = 0
    reverse_posts_fetched = 0
    reverse_posts_upserted = 0
    reverse_pages_fetched = 0
    reverse_stop_reason: str | None = None
    fetcher_metadata: dict[str, Any] = {}
    pagination_state: dict[str, Any] = {}
    stop_reason: str | None = None
    bidirectional_probe_done = False
    bidirectional_reverse_started = False
    bidirectional_reverse_error: str | None = None
    timeout_guard_seconds = _posts_pagination_timeout_guard_seconds(config)
    started_monotonic = time.monotonic()

    async def _run_job() -> tuple[dict[str, Any], dict[str, Any]]:
        nonlocal posts_fetched, posts_upserted, posts_skipped, posts_skipped_by_reason
        nonlocal pages_fetched, fetcher_metadata, bidirectional_probe_done
        nonlocal reverse_posts_fetched, reverse_posts_upserted, reverse_pages_fetched, reverse_stop_reason
        nonlocal bidirectional_reverse_started, bidirectional_reverse_error

        session = resolve_posts_scrapling_session(
            browser_account_id=account_handle,
            caller_context=f"posts_scrapling:{account_handle}",
        )
        proxy_session_key = _posts_proxy_session_key(
            account_handle=account_handle,
            stage=stage,
            config=config,
            job_metadata=job_metadata,
            browser_account_id=session.browser_account_id,
        )
        proxy_config = select_posts_proxy(session_key=proxy_session_key or account_handle)
        fetcher = InstagramPostsScraplingFetcher(
            cookies=session.cookies,
            raw_cookies=session.auth_session.cookies,
            browser_account_id=session.browser_account_id,
            proxy_config=proxy_config,
            fast_mode=fast_mode,
        )
        proxy_flags = posts_proxy_feature_flags()
        forward_seen_post_ids: set[str] = set()
        reverse_seen_post_ids: set[str] = set()
        reverse_task: asyncio.Task[None] | None = None

        def _fetcher_runtime_metadata() -> dict[str, Any]:
            return {**dict(fetcher.runtime_metadata), "proxy_session_key": proxy_session_key}

        async def _run_reverse_listing(snapshot: dict[str, Any]) -> None:
            nonlocal reverse_posts_fetched, reverse_posts_upserted, reverse_pages_fetched, reverse_stop_reason
            nonlocal bidirectional_reverse_error

            reverse_proxy_session_key = f"{proxy_session_key}:reverse"
            reverse_proxy_config = select_posts_proxy(session_key=reverse_proxy_session_key)
            reverse_fetcher = InstagramPostsScraplingFetcher(
                cookies=session.cookies,
                raw_cookies=session.auth_session.cookies,
                browser_account_id=session.browser_account_id,
                proxy_config=reverse_proxy_config,
                fast_mode=fast_mode,
            )
            try:
                await reverse_fetcher.apply_warmup_snapshot(snapshot)
                reverse_cursor: str | None = None
                while True:
                    if proxy_flags["page_proxy_rotation_enabled"]:
                        page_proxy_config = select_posts_proxy(
                            session_key=reverse_proxy_session_key,
                            page_index=reverse_pages_fetched,
                        )
                        await reverse_fetcher.set_api_proxy_config(
                            page_proxy_config,
                            reason=f"reverse_page_{reverse_pages_fetched}",
                        )
                    result = await reverse_fetcher.fetch_posts_page(
                        account_handle,
                        cursor=reverse_cursor,
                        direction="reverse",
                    )
                    if result.auth_failed or (result.fetch_failed and not result.posts):
                        reverse_stop_reason = str(result.fetch_reason or "reverse_fetch_failed").strip()
                        bidirectional_reverse_error = reverse_stop_reason
                        break

                    page_ids = {
                        identity for post in result.posts if (identity := _posts_node_identity(post))
                    }
                    overlap = bool(page_ids & forward_seen_post_ids)
                    if result.posts:
                        persisted = persist_instagram_posts(
                            account_handle=account_handle,
                            post_nodes=result.posts,
                            run_id=run_id or None,
                            job_id=job_id,
                            season_id=season_id,
                            source_scope=source_scope,
                        )
                        reverse_posts_fetched += len(result.posts)
                        reverse_posts_upserted += persisted.posts_upserted
                        reverse_seen_post_ids.update(page_ids)

                    reverse_pages_fetched += 1
                    reverse_stop_reason = "bidirectional_overlap" if overlap else None
                    if not reverse_stop_reason and (not result.has_next_page or not result.end_cursor):
                        reverse_stop_reason = "completed"
                    elif not reverse_stop_reason and max_pages and reverse_pages_fetched >= max_pages:
                        reverse_stop_reason = "max_pages"

                    repo.persist_instagram_profile_pagination_state(
                        run_id=run_id or None,
                        job_id=job_id,
                        account_handle=account_handle,
                        source_scope=source_scope,
                        direction="reverse",
                        cursor_in=reverse_cursor,
                        end_cursor=result.end_cursor,
                        page_index=reverse_pages_fetched,
                        posts_seen=reverse_posts_fetched,
                        posts_upserted=reverse_posts_upserted,
                        doc_id_used=str(reverse_fetcher.runtime_metadata.get("doc_id_used") or "").strip() or None,
                        doc_ids_attempted=_posts_pagination_doc_ids_attempted(dict(reverse_fetcher.runtime_metadata)),
                        proxy_fingerprint=_posts_runtime_proxy_fingerprint(dict(reverse_fetcher.runtime_metadata)),
                        proxy_session_key=reverse_proxy_session_key,
                        stop_reason=reverse_stop_reason,
                        partial=reverse_stop_reason != "completed",
                        completed=reverse_stop_reason == "completed",
                        metadata={
                            "bidirectional_reverse_walker": True,
                            "overlap_with_forward": overlap,
                        },
                    )
                    if reverse_stop_reason:
                        break
                    reverse_cursor = result.end_cursor
            except Exception as exc:  # noqa: BLE001
                bidirectional_reverse_error = str(exc)
                reverse_stop_reason = "reverse_walker_failed"
                logger.warning(
                    "instagram_posts_scrapling reverse_walker_failed account=%s error=%s",
                    account_handle,
                    exc,
                    exc_info=True,
                )
            finally:
                await reverse_fetcher.aclose()

        try:
            try:
                await fetcher.warmup(account_handle)
            except InstagramPostsWarmupError as exc:
                raise PostsScraplingRuntimeError(
                    str(exc),
                    error_code=str(getattr(exc, "error_code", "") or "instagram_posts_warmup_failed"),
                    retryable=bool(getattr(exc, "retryable", False)),
                    runtime_metadata=_fetcher_runtime_metadata(),
                ) from exc
            auth_metadata = dict(session.auth_session.metadata or {})
            fetcher_metadata = _fetcher_runtime_metadata()

            lifecycle.touch_job_heartbeat(job_id, worker_id=worker_id)
            restart_requested = bool(config.get("restart") or config.get("restart_pagination"))
            cursor: str | None = (
                str(config.get("resume_cursor") or config.get("resume_frontier_cursor") or "").strip() or None
            )
            if not cursor and not restart_requested:
                pagination_state = repo.latest_instagram_profile_pagination_state(
                    account_handle=account_handle,
                    source_scope=source_scope,
                    run_id=run_id or None,
                    direction="forward",
                )
                cursor = str(pagination_state.get("end_cursor") or "").strip() or None

            while True:
                timeout_guard_elapsed = (
                    timeout_guard_seconds is not None
                    and (time.monotonic() - started_monotonic) >= timeout_guard_seconds
                )
                if timeout_guard_elapsed:
                    stop_reason = "timeout_guard"
                    pagination_state = repo.persist_instagram_profile_pagination_state(
                        run_id=run_id or None,
                        job_id=job_id,
                        account_handle=account_handle,
                        source_scope=source_scope,
                        direction="forward",
                        cursor_in=cursor,
                        end_cursor=cursor,
                        page_index=pages_fetched,
                        posts_seen=posts_fetched,
                        posts_upserted=posts_upserted,
                        doc_id_used=str(fetcher_metadata.get("doc_id_used") or "").strip() or None,
                        doc_ids_attempted=_posts_pagination_doc_ids_attempted(fetcher_metadata),
                        proxy_fingerprint=_posts_runtime_proxy_fingerprint(fetcher_metadata),
                        proxy_session_key=proxy_session_key,
                        stop_reason=stop_reason,
                        partial=True,
                        completed=False,
                        metadata={"reason": stop_reason, "listing_progress": True},
                    )
                    break
                lifecycle.touch_job_heartbeat(job_id, worker_id=worker_id)
                _raise_if_cancelled(
                    job_id=job_id,
                    run_id=run_id,
                    runtime_metadata=_fetcher_runtime_metadata(),
                )
                if proxy_flags["page_proxy_rotation_enabled"]:
                    page_proxy_config = select_posts_proxy(
                        session_key=proxy_session_key or account_handle,
                        page_index=pages_fetched,
                    )
                    await fetcher.set_api_proxy_config(
                        page_proxy_config,
                        reason=f"page_{pages_fetched}",
                    )
                    fetcher_metadata = _fetcher_runtime_metadata()
                result = await fetcher.fetch_posts_page(account_handle, cursor=cursor)
                fetcher_metadata = _fetcher_runtime_metadata()

                if result.auth_failed:
                    raise PostsScraplingRuntimeError(
                        f"Instagram auth failed while fetching posts for @{account_handle}.",
                        error_code="instagram_posts_auth_failed",
                        retryable=False,
                        runtime_metadata={**_fetcher_runtime_metadata(), "fetch_reason": result.fetch_reason},
                    )
                if result.fetch_failed and not result.posts:
                    stop_reason = _posts_pagination_stop_reason(result)
                    if stop_reason == "cursor_expired_restart_required":
                        pagination_state = repo.persist_instagram_profile_pagination_state(
                            run_id=run_id or None,
                            job_id=job_id,
                            account_handle=account_handle,
                            source_scope=source_scope,
                            direction="forward",
                            cursor_in=cursor,
                            end_cursor=cursor,
                            page_index=pages_fetched,
                            posts_seen=posts_fetched,
                            posts_upserted=posts_upserted,
                            doc_id_used=str(fetcher_metadata.get("doc_id_used") or "").strip() or None,
                            doc_ids_attempted=_posts_pagination_doc_ids_attempted(fetcher_metadata),
                            proxy_fingerprint=_posts_runtime_proxy_fingerprint(fetcher_metadata),
                            proxy_session_key=proxy_session_key,
                            stop_reason=stop_reason,
                            partial=True,
                            completed=False,
                            metadata={"fetch_reason": getattr(result, "fetch_reason", None)},
                        )
                    raise PostsScraplingRuntimeError(
                        f"Instagram posts fetch failed for @{account_handle}.",
                        error_code=stop_reason or str(result.fetch_reason or "instagram_posts_fetch_failed"),
                        retryable=bool(result.retryable),
                        runtime_metadata={**_fetcher_runtime_metadata(), "fetch_reason": result.fetch_reason},
                    )

                if result.posts:
                    if not bidirectional_probe_done and _posts_bidirectional_walk_enabled():
                        probe_metadata = await fetcher.probe_bidirectional_walk(
                            account_handle,
                            forward_posts=result.posts,
                        )
                        bidirectional_probe_done = True
                        if probe_metadata.get("passed") and reverse_task is None:
                            bidirectional_reverse_started = True
                            reverse_task = asyncio.create_task(_run_reverse_listing(fetcher.warmup_snapshot()))
                        fetcher_metadata = _fetcher_runtime_metadata()
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
                    forward_seen_post_ids.update(
                        identity for post in result.posts if (identity := _posts_node_identity(post))
                    )

                pages_fetched += 1
                stop_reason = _posts_pagination_stop_reason(result)
                if not stop_reason and (not result.has_next_page or not result.end_cursor):
                    stop_reason = "completed"
                elif not stop_reason and max_pages and pages_fetched >= max_pages:
                    stop_reason = "max_pages"
                pagination_state = repo.persist_instagram_profile_pagination_state(
                    run_id=run_id or None,
                    job_id=job_id,
                    account_handle=account_handle,
                    source_scope=source_scope,
                    direction="forward",
                    cursor_in=cursor,
                    end_cursor=result.end_cursor,
                    page_index=pages_fetched,
                    posts_seen=posts_fetched,
                    posts_upserted=posts_upserted,
                    doc_id_used=str(fetcher_metadata.get("doc_id_used") or "").strip() or None,
                    doc_ids_attempted=_posts_pagination_doc_ids_attempted(fetcher_metadata),
                    proxy_fingerprint=_posts_runtime_proxy_fingerprint(fetcher_metadata),
                    proxy_session_key=proxy_session_key,
                    stop_reason=stop_reason,
                    partial=stop_reason != "completed",
                    completed=stop_reason == "completed",
                    metadata={
                        "fetch_reason": getattr(result, "fetch_reason", None),
                        "has_next_page": bool(result.has_next_page),
                    },
                )
                lifecycle.emit_job_progress(
                    job_id=job_id,
                    stage=stage,
                    platform="instagram",
                    account=account_handle,
                    scraped_posts=posts_fetched,
                    scraped_comments=0,
                    posts_upserted=posts_upserted,
                    comments_upserted=0,
                    activity={
                        "phase": "posts_scrapling_running",
                        "pages_fetched": pages_fetched,
                        "listing_progress": {
                            "page_index": pages_fetched,
                            "posts_seen": posts_fetched,
                            "posts_upserted": posts_upserted,
                            "end_cursor": result.end_cursor,
                            "stop_reason": stop_reason,
                        },
                    },
                    progress_state=progress_state,
                    force=not result.has_next_page,
                )

                if stop_reason == "pagination_doc_id_stale":
                    break
                if not result.has_next_page or not result.end_cursor:
                    break
                if max_pages and pages_fetched >= max_pages:
                    break
                cursor = result.end_cursor

            if reverse_task is not None:
                await reverse_task
                fetcher_metadata = _fetcher_runtime_metadata()

            if stop_reason in {"timeout_guard", "pagination_doc_id_stale"}:
                raise PostsScraplingRuntimeError(
                    f"Instagram posts pagination stopped for @{account_handle}: {stop_reason}.",
                    error_code=stop_reason,
                    retryable=stop_reason == "timeout_guard",
                    runtime_metadata={
                        **_fetcher_runtime_metadata(),
                        "pagination_state": pagination_state,
                        "listing_progress": {
                            "page_index": pages_fetched,
                            "posts_seen": posts_fetched,
                            "posts_upserted": posts_upserted,
                            "stop_reason": stop_reason,
                            "partial": True,
                        },
                    },
                )

            fetcher_metadata = _fetcher_runtime_metadata()
            return auth_metadata, fetcher_metadata
        finally:
            if reverse_task is not None and not reverse_task.done():
                reverse_task.cancel()
                try:
                    await reverse_task
                except asyncio.CancelledError:
                    pass
            fetcher_metadata = _fetcher_runtime_metadata()
            await fetcher.aclose()

    try:
        auth_metadata, fetcher_metadata = asyncio.run(_run_job())
        metadata = {
            "stage": stage,
            "platform": "instagram",
            "account": account_handle,
            "fast_mode": fast_mode,
            "source_scope": source_scope,
            "stage_counters": {
                "posts": posts_fetched + reverse_posts_fetched,
                "pages": pages_fetched + reverse_pages_fetched,
                "forward_posts": posts_fetched,
                "forward_pages": pages_fetched,
                "reverse_posts": reverse_posts_fetched,
                "reverse_pages": reverse_pages_fetched,
            },
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
            "pagination_state": pagination_state,
            "listing_progress": {
                "page_index": pages_fetched,
                "posts_seen": posts_fetched,
                "posts_upserted": posts_upserted,
                "stop_reason": stop_reason,
                "partial": stop_reason not in {None, "completed"},
            },
            "bidirectional_listing": {
                "reverse_started": bidirectional_reverse_started,
                "reverse_pages_fetched": reverse_pages_fetched,
                "reverse_posts_seen": reverse_posts_fetched,
                "reverse_posts_upserted": reverse_posts_upserted,
                "reverse_stop_reason": reverse_stop_reason,
                "reverse_error": bidirectional_reverse_error,
            },
            "resume_cursor_saved": bool((pagination_state or {}).get("end_cursor")),
            "posts_acceleration_flags": repo.instagram_posts_acceleration_flags(),
            "activity": {
                "phase": "posts_scrapling_end",
                "last_progress_at": lifecycle.format_time(lifecycle.now_utc()),
            },
            "auth_context": {
                "session_source": auth_metadata.get("source"),
                "browser_account_id": auth_metadata.get("browser_account_id"),
                "validation_category": auth_metadata.get("validation_category"),
                "proxy_session_key": fetcher_metadata.get("proxy_session_key"),
            },
            "fetcher_runtime": fetcher_metadata,
        }
        lifecycle.finish_job(
            job_id,
            status="completed",
            items_found=posts_fetched + reverse_posts_fetched,
            metadata=metadata,
        )
        terminal_status = "completed"
        terminal_error_message: str | None = None
    except ScraplingJobCancelledError as exc:
        lifecycle.finish_job(
            job_id,
            status="cancelled",
            items_found=posts_fetched + reverse_posts_fetched,
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
                "activity": {"phase": "cancelled", "last_progress_at": lifecycle.format_time(lifecycle.now_utc())},
                "stage_counters": {
                    "posts": posts_fetched + reverse_posts_fetched,
                    "pages": pages_fetched + reverse_pages_fetched,
                    "forward_posts": posts_fetched,
                    "forward_pages": pages_fetched,
                    "reverse_posts": reverse_posts_fetched,
                    "reverse_pages": reverse_pages_fetched,
                },
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
                "pagination_state": pagination_state,
                "listing_progress": {
                    "page_index": pages_fetched,
                    "posts_seen": posts_fetched,
                    "posts_upserted": posts_upserted,
                    "stop_reason": stop_reason,
                    "partial": stop_reason not in {None, "completed"},
                },
                "bidirectional_listing": {
                    "reverse_started": bidirectional_reverse_started,
                    "reverse_pages_fetched": reverse_pages_fetched,
                    "reverse_posts_seen": reverse_posts_fetched,
                    "reverse_posts_upserted": reverse_posts_upserted,
                    "reverse_stop_reason": reverse_stop_reason,
                    "reverse_error": bidirectional_reverse_error,
                },
                "resume_cursor_saved": bool((pagination_state or {}).get("end_cursor")),
                "posts_acceleration_flags": repo.instagram_posts_acceleration_flags(),
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
            lifecycle.now_utc() + timedelta(seconds=lifecycle.retry_backoff_seconds(attempt_count))
            if can_retry
            else None
        )
        lifecycle.finish_job(
            job_id,
            status="retrying" if can_retry else "failed",
            items_found=posts_fetched + reverse_posts_fetched,
            error_message=str(exc),
            metadata={
                "stage": stage,
                "platform": "instagram",
                "account": account_handle,
                "fast_mode": fast_mode,
                "source_scope": source_scope,
                "error_code": error_code,
                "error_class": error_class,
                "activity": {"phase": "failed", "last_progress_at": lifecycle.format_time(lifecycle.now_utc())},
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
                "pagination_state": pagination_state,
                "listing_progress": {
                    "page_index": pages_fetched,
                    "posts_seen": posts_fetched,
                    "posts_upserted": posts_upserted,
                    "stop_reason": stop_reason or error_code,
                    "partial": True,
                },
                "bidirectional_listing": {
                    "reverse_started": bidirectional_reverse_started,
                    "reverse_pages_fetched": reverse_pages_fetched,
                    "reverse_posts_seen": reverse_posts_fetched,
                    "reverse_posts_upserted": reverse_posts_upserted,
                    "reverse_stop_reason": reverse_stop_reason,
                    "reverse_error": bidirectional_reverse_error,
                },
                "resume_cursor_saved": bool((pagination_state or {}).get("end_cursor")),
                "posts_acceleration_flags": repo.instagram_posts_acceleration_flags(),
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
            "items_found": posts_fetched + reverse_posts_fetched,
            "error_message": terminal_error_message,
            "metadata": {
                "degraded_summary": True,
                "database_service_unavailable": True,
            },
        }
