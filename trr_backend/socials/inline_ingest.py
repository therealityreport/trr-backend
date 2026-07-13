"""Inline social ingest worker helpers.

This module keeps testable inline worker orchestration out of the oversized
social analytics router without owning repository/database behavior.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from concurrent.futures import CancelledError, Future, wait
from typing import Any


def _raise_first_with_all_failures(futures: list[Future]) -> None:
    wait(futures)
    failures: list[tuple[int, BaseException]] = []
    for idx, future in enumerate(futures):
        try:
            failure = future.exception()
        except CancelledError as exc:
            failure = exc
        if failure is not None:
            failures.append((idx, failure))
    if not failures:
        return

    summary = "; ".join(f"future[{idx}]: {type(exc).__name__}: {exc}" for idx, exc in failures)
    first_exc = failures[0][1]
    raise RuntimeError(f"inline ingest had {len(failures)} platform failure(s): {summary}") from first_exc


def normalize_target_platforms(
    platforms: Sequence[str] | None,
    *,
    supported_platforms: Sequence[str],
) -> list[str]:
    ordered = list(platforms) if platforms is not None else list(supported_platforms)
    deduped: list[str] = []
    for platform in ordered:
        normalized = str(platform or "").strip().lower()
        if not normalized or normalized in deduped:
            continue
        deduped.append(normalized)
    return deduped or list(supported_platforms)


def run_inline_season_ingest_execution(
    run_id: str,
    *,
    platforms: Sequence[str] | None,
    supported_platforms: Sequence[str],
    ingest_mode: str,
    worker_prefix: str,
    comments_workers_cap: int,
    execute_run: Callable[..., Any],
    thread_pool_executor_factory: Callable[..., Any],
) -> None:
    target_platforms = normalize_target_platforms(platforms, supported_platforms=supported_platforms)
    normalized_mode = str(ingest_mode or "").strip().lower()

    if normalized_mode == "comments_only":
        max_workers = min(max(1, int(comments_workers_cap or 1)), max(1, len(target_platforms)))
        with thread_pool_executor_factory(max_workers=max_workers) as pool:
            futures = [
                pool.submit(
                    execute_run,
                    run_id,
                    worker_id=f"{worker_prefix}:comments:{platform}",
                    stage="comments",
                    platform=platform,
                )
                for platform in target_platforms
            ]
            _raise_first_with_all_failures(futures)
        return

    if len(target_platforms) > 1:
        with thread_pool_executor_factory(max_workers=len(target_platforms)) as pool:
            futures = [
                pool.submit(
                    execute_run,
                    run_id,
                    worker_id=f"{worker_prefix}:{platform}",
                    platform=platform,
                )
                for platform in target_platforms
            ]
            _raise_first_with_all_failures(futures)
        return

    execute_run(
        run_id,
        worker_id=worker_prefix,
        platform=target_platforms[0] if target_platforms else None,
    )
