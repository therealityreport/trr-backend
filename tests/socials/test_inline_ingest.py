from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor

import pytest

from trr_backend.socials.inline_ingest import _raise_first_with_all_failures, run_inline_season_ingest_execution


class _CountingFuture(Future):
    def __init__(self) -> None:
        super().__init__()
        self.exception_calls = 0

    def exception(self, timeout: float | None = None):  # noqa: ANN201
        self.exception_calls += 1
        return super().exception(timeout=timeout)


def test_inline_failure_aggregation_inspects_cancelled_futures_once() -> None:
    cancelled = _CountingFuture()
    failed = _CountingFuture()
    succeeded = _CountingFuture()
    assert cancelled.cancel() is True
    assert cancelled.set_running_or_notify_cancel() is False
    failed.set_exception(ValueError("youtube failed"))
    succeeded.set_result(None)

    with pytest.raises(RuntimeError) as exc_info:
        _raise_first_with_all_failures([cancelled, failed, succeeded])

    assert "inline ingest had 2 platform failure(s)" in str(exc_info.value)
    assert "future[0]: CancelledError" in str(exc_info.value)
    assert "future[1]: ValueError: youtube failed" in str(exc_info.value)
    assert [future.exception_calls for future in (cancelled, failed, succeeded)] == [1, 1, 1]


def test_inline_ingest_runs_each_target_platform() -> None:
    calls: list[str] = []

    def execute_run(run_id: str, *, worker_id: str, platform: str) -> None:
        assert run_id == "run-1"
        assert worker_id == f"api-background:{platform}"
        calls.append(platform)

    run_inline_season_ingest_execution(
        "run-1",
        platforms=["instagram", "youtube", "tiktok"],
        supported_platforms=["instagram", "youtube", "tiktok"],
        ingest_mode="posts_and_comments",
        worker_prefix="api-background",
        comments_workers_cap=1,
        execute_run=execute_run,
        thread_pool_executor_factory=ThreadPoolExecutor,
    )

    assert sorted(calls) == ["instagram", "tiktok", "youtube"]


def test_inline_ingest_surfaces_all_platform_failures() -> None:
    calls: list[str] = []

    def execute_run(run_id: str, *, worker_id: str, platform: str) -> None:
        calls.append(platform)
        if platform in {"instagram", "youtube"}:
            raise ValueError(f"{platform} failed")

    with pytest.raises(RuntimeError) as exc_info:
        run_inline_season_ingest_execution(
            "run-1",
            platforms=["instagram", "youtube", "tiktok"],
            supported_platforms=["instagram", "youtube", "tiktok"],
            ingest_mode="posts_and_comments",
            worker_prefix="api-background",
            comments_workers_cap=1,
            execute_run=execute_run,
            thread_pool_executor_factory=ThreadPoolExecutor,
        )

    message = str(exc_info.value)
    assert "inline ingest had 2 platform failure(s)" in message
    assert "future[0]: ValueError: instagram failed" in message
    assert "future[1]: ValueError: youtube failed" in message
    assert sorted(calls) == ["instagram", "tiktok", "youtube"]


def test_inline_comments_only_surfaces_all_platform_failures() -> None:
    calls: list[str] = []

    def execute_run(run_id: str, *, worker_id: str, stage: str, platform: str) -> None:
        assert stage == "comments"
        assert worker_id == f"api-background:comments:{platform}"
        calls.append(platform)
        if platform in {"instagram", "youtube"}:
            raise ValueError(f"{platform} comments failed")

    with pytest.raises(RuntimeError) as exc_info:
        run_inline_season_ingest_execution(
            "run-1",
            platforms=["instagram", "youtube", "tiktok"],
            supported_platforms=["instagram", "youtube", "tiktok"],
            ingest_mode="comments_only",
            worker_prefix="api-background",
            comments_workers_cap=3,
            execute_run=execute_run,
            thread_pool_executor_factory=ThreadPoolExecutor,
        )

    message = str(exc_info.value)
    assert "inline ingest had 2 platform failure(s)" in message
    assert "future[0]: ValueError: instagram comments failed" in message
    assert "future[1]: ValueError: youtube comments failed" in message
    assert sorted(calls) == ["instagram", "tiktok", "youtube"]
