"""Incremental Crawlee runtime wrapper around existing platform stages."""

from __future__ import annotations

import asyncio
import os
from collections.abc import Callable
from dataclasses import dataclass
from threading import Thread
from typing import Any
from uuid import uuid4

from .auth_preflight import AuthPreflightResult, build_auth_context
from .config import CrawleeRuntimeConfig
from .error_taxonomy import classify_exception
from .request_keys import build_request_key

StageRunner = Callable[[], tuple[int, int, dict[str, Any]]]


class CrawleeRuntimeError(RuntimeError):
    """Raised when Crawlee runtime exhausts retries or hits fatal failures."""

    def __init__(
        self,
        message: str,
        *,
        error_code: str,
        error_class: str,
        retryable: bool,
        runtime_metadata: dict[str, Any],
    ):
        super().__init__(message)
        self.error_code = error_code
        self.error_class = error_class
        self.retryable = retryable
        self.runtime_metadata = runtime_metadata


@dataclass(frozen=True)
class _CrawleeImportStatus:
    available: bool
    version: str | None
    request_class: Any | None = None
    request_queue_class: Any | None = None
    session_pool_class: Any | None = None
    proxy_configuration_class: Any | None = None


@dataclass
class _RuntimeCounters:
    requests_total: int = 0
    requests_handled: int = 0
    retries_total: int = 0
    blocked_events: int = 0
    session_rotations: int = 0
    max_proxy_tier_used: int = 0
    crawlee_request_count: int = 0
    crawlee_retry_count: int = 0
    crawlee_session_pool_used: bool = False


def _detect_crawlee() -> _CrawleeImportStatus:
    try:
        import crawlee  # type: ignore[import-not-found]
        from crawlee import Request  # type: ignore[import-not-found]
        from crawlee.proxy_configuration import ProxyConfiguration  # type: ignore[import-not-found]
        from crawlee.sessions import SessionPool  # type: ignore[import-not-found]
        from crawlee.storages import RequestQueue  # type: ignore[import-not-found]
    except Exception:
        return _CrawleeImportStatus(available=False, version=None)

    return _CrawleeImportStatus(
        available=True,
        version=str(getattr(crawlee, "__version__", "unknown")),
        request_class=Request,
        request_queue_class=RequestQueue,
        session_pool_class=SessionPool,
        proxy_configuration_class=ProxyConfiguration,
    )


def _proxy_urls_for_platform(platform: str) -> list[str]:
    suffix = (platform or "").strip().upper()
    raw = os.getenv(f"SOCIAL_CRAWLEE_PROXY_URLS_{suffix}") or os.getenv("SOCIAL_CRAWLEE_PROXY_URLS") or ""
    return [entry.strip() for entry in raw.split(",") if entry.strip()]


def _build_runtime_meta(
    *,
    platform: str,
    stage: str,
    request_key: str,
    request_mode: str,
    runtime_config: CrawleeRuntimeConfig,
    crawlee_status: _CrawleeImportStatus,
    counters: _RuntimeCounters,
) -> dict[str, Any]:
    return {
        "engine": "crawlee_python_incremental",
        "platform": platform,
        "stage": stage,
        "request_key": request_key,
        "request_mode": request_mode,
        "crawlee_available": crawlee_status.available,
        "crawlee_version": crawlee_status.version,
        "max_concurrency": runtime_config.max_concurrency,
        "max_retries": runtime_config.max_retries,
        "requests_total": counters.requests_total,
        "requests_handled": counters.requests_handled,
        "retries_total": counters.retries_total,
        "blocked_events": counters.blocked_events,
        "session_rotations": counters.session_rotations,
        "max_proxy_tier_used": counters.max_proxy_tier_used,
        "crawlee_request_count": counters.crawlee_request_count,
        "crawlee_retry_count": counters.crawlee_retry_count,
        "crawlee_session_pool_used": counters.crawlee_session_pool_used,
    }


def _run_coroutine(coro: Any) -> Any:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    payload: dict[str, Any] = {}

    def _thread_main() -> None:
        try:
            payload["result"] = asyncio.run(coro)
        except Exception as exc:  # noqa: BLE001
            payload["error"] = exc

    thread = Thread(target=_thread_main, daemon=True)
    thread.start()
    thread.join()
    if "error" in payload:
        raise payload["error"]
    return payload.get("result")


def _execute_with_internal_retry(
    *,
    platform: str,
    stage: str,
    request_mode: str,
    request_key: str,
    runtime_config: CrawleeRuntimeConfig,
    auth_preflight: AuthPreflightResult,
    stage_runner: StageRunner,
    crawlee_status: _CrawleeImportStatus,
) -> tuple[int, int, dict[str, Any]]:
    counters = _RuntimeCounters(
        requests_total=1,
        crawlee_request_count=1,
        crawlee_session_pool_used=False,
    )
    attempts = max(1, int(runtime_config.max_retries))

    for attempt in range(1, attempts + 1):
        try:
            posts_count, comments_count, retrieval_meta = stage_runner()
            counters.requests_handled = 1
            runtime_meta = _build_runtime_meta(
                platform=platform,
                stage=stage,
                request_key=request_key,
                request_mode=request_mode,
                runtime_config=runtime_config,
                crawlee_status=crawlee_status,
                counters=counters,
            )
            merged_meta = dict(retrieval_meta or {})
            merged_meta["crawler_runtime"] = runtime_meta
            merged_meta["auth_context"] = build_auth_context(auth_preflight)
            return posts_count, comments_count, merged_meta
        except Exception as exc:  # noqa: BLE001
            error_code, error_class, retryable = classify_exception(exc)
            if error_code in {"blocked", "rate_limited"}:
                counters.blocked_events += 1
            if retryable and attempt < attempts:
                counters.retries_total += 1
                counters.crawlee_retry_count += 1
                counters.session_rotations += 1
                continue
            raise CrawleeRuntimeError(
                f"crawlee_stage_failed:{platform}:{stage}:{error_code}",
                error_code=error_code,
                error_class=error_class,
                retryable=retryable,
                runtime_metadata={
                    "crawler_runtime": _build_runtime_meta(
                        platform=platform,
                        stage=stage,
                        request_key=request_key,
                        request_mode=request_mode,
                        runtime_config=runtime_config,
                        crawlee_status=crawlee_status,
                        counters=counters,
                    ),
                    "auth_context": build_auth_context(auth_preflight),
                },
            ) from exc

    raise CrawleeRuntimeError(
        f"crawlee_stage_failed:{platform}:{stage}:unknown",
        error_code="unknown",
        error_class="RuntimeError",
        retryable=False,
        runtime_metadata={
            "crawler_runtime": _build_runtime_meta(
                platform=platform,
                stage=stage,
                request_key=request_key,
                request_mode=request_mode,
                runtime_config=runtime_config,
                crawlee_status=crawlee_status,
                counters=counters,
            ),
            "auth_context": build_auth_context(auth_preflight),
        },
    )


def execute_platform_stage_with_crawlee(
    *,
    platform: str,
    stage: str,
    account: str,
    request_mode: str,
    runtime_config: CrawleeRuntimeConfig,
    auth_preflight: AuthPreflightResult,
    stage_runner: StageRunner,
    config: dict[str, Any],
) -> tuple[int, int, dict[str, Any]]:
    """
    Execute an existing stage runner through Crawlee request/session/proxy primitives.

    The business logic remains in the existing platform stage runner while queue/retry/session/proxy
    behavior is centralized in this runtime layer.
    """
    crawlee_status = _detect_crawlee()
    request_key = build_request_key(
        platform=platform,
        target=account or str(config.get("account") or ""),
        post_id=str(config.get("post_id") or ""),
        cursor=str(config.get("cursor") or ""),
        reply_cursor=str(config.get("reply_cursor") or ""),
        mode=request_mode,
    )
    if not crawlee_status.available:
        return _execute_with_internal_retry(
            platform=platform,
            stage=stage,
            request_mode=request_mode,
            request_key=request_key,
            runtime_config=runtime_config,
            auth_preflight=auth_preflight,
            stage_runner=stage_runner,
            crawlee_status=crawlee_status,
        )

    async def _execute_with_queue() -> tuple[int, int, dict[str, Any]]:
        request_class = crawlee_status.request_class
        request_queue_class = crawlee_status.request_queue_class
        session_pool_class = crawlee_status.session_pool_class
        proxy_configuration_class = crawlee_status.proxy_configuration_class
        if (
            request_class is None
            or request_queue_class is None
            or session_pool_class is None
            or proxy_configuration_class is None
        ):
            return _execute_with_internal_retry(
                platform=platform,
                stage=stage,
                request_mode=request_mode,
                request_key=request_key,
                runtime_config=runtime_config,
                auth_preflight=auth_preflight,
                stage_runner=stage_runner,
                crawlee_status=_CrawleeImportStatus(available=False, version=crawlee_status.version),
            )

        queue_name = f"trr_social_{platform}_{stage}_{uuid4().hex[:12]}"
        request_queue = await request_queue_class.open(name=queue_name)
        counters = _RuntimeCounters(
            requests_total=1,
            crawlee_request_count=1,
            crawlee_session_pool_used=True,
        )
        proxy_urls = _proxy_urls_for_platform(platform)
        proxy_configuration = proxy_configuration_class(proxy_urls=proxy_urls or [None])
        request = request_class.from_url(
            url=f"https://trr.invalid/{platform}/{stage}",
            unique_key=request_key,
            max_retries=max(1, int(runtime_config.max_retries)),
            user_data={"platform": platform, "stage": stage, "mode": request_mode},
        )
        await request_queue.add_request(request)

        try:
            async with session_pool_class(max_pool_size=max(1, int(runtime_config.max_concurrency))) as session_pool:
                while True:
                    queued_request = await request_queue.fetch_next_request()
                    if queued_request is None:
                        break
                    session = await session_pool.get_session()
                    proxy_info = await proxy_configuration.new_proxy_info(
                        session_id=session.id,
                        request=queued_request,
                        proxy_tier=queued_request.last_proxy_tier,
                    )
                    if proxy_info is not None and proxy_info.proxy_tier is not None:
                        counters.max_proxy_tier_used = max(counters.max_proxy_tier_used, proxy_info.proxy_tier + 1)
                    try:
                        posts_count, comments_count, retrieval_meta = stage_runner()
                        session.mark_good()
                        counters.requests_handled += 1
                        await request_queue.mark_request_as_handled(queued_request)
                        runtime_meta = _build_runtime_meta(
                            platform=platform,
                            stage=stage,
                            request_key=request_key,
                            request_mode=request_mode,
                            runtime_config=runtime_config,
                            crawlee_status=crawlee_status,
                            counters=counters,
                        )
                        merged_meta = dict(retrieval_meta or {})
                        merged_meta["crawler_runtime"] = runtime_meta
                        merged_meta["auth_context"] = build_auth_context(auth_preflight)
                        return posts_count, comments_count, merged_meta
                    except Exception as exc:  # noqa: BLE001
                        error_code, error_class, retryable = classify_exception(exc)
                        if error_code in {"blocked", "rate_limited"}:
                            counters.blocked_events += 1
                            session.retire()
                        else:
                            session.mark_bad()
                        configured_retries = queued_request.max_retries
                        max_retries = (
                            configured_retries if configured_retries is not None else runtime_config.max_retries
                        )
                        can_retry = retryable and queued_request.retry_count < max(1, int(max_retries))
                        if can_retry:
                            queued_request.retry_count += 1
                            queued_request.session_rotation_count = (queued_request.session_rotation_count or 0) + 1
                            counters.retries_total += 1
                            counters.crawlee_retry_count += 1
                            counters.session_rotations += 1
                            await request_queue.reclaim_request(queued_request, forefront=False)
                            continue
                        await request_queue.mark_request_as_handled(queued_request)
                        raise CrawleeRuntimeError(
                            f"crawlee_stage_failed:{platform}:{stage}:{error_code}",
                            error_code=error_code,
                            error_class=error_class,
                            retryable=retryable,
                            runtime_metadata={
                                "crawler_runtime": _build_runtime_meta(
                                    platform=platform,
                                    stage=stage,
                                    request_key=request_key,
                                    request_mode=request_mode,
                                    runtime_config=runtime_config,
                                    crawlee_status=crawlee_status,
                                    counters=counters,
                                ),
                                "auth_context": build_auth_context(auth_preflight),
                            },
                        ) from exc
        finally:
            await request_queue.drop()

        raise CrawleeRuntimeError(
            f"crawlee_stage_failed:{platform}:{stage}:unknown",
            error_code="unknown",
            error_class="RuntimeError",
            retryable=False,
            runtime_metadata={
                "crawler_runtime": _build_runtime_meta(
                    platform=platform,
                    stage=stage,
                    request_key=request_key,
                    request_mode=request_mode,
                    runtime_config=runtime_config,
                    crawlee_status=crawlee_status,
                    counters=counters,
                ),
                "auth_context": build_auth_context(auth_preflight),
            },
        )

    return _run_coroutine(_execute_with_queue())
