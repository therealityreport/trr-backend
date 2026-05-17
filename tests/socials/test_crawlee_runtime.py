from __future__ import annotations

import asyncio
import time

import pytest

from trr_backend.socials.crawlee_runtime import runtime
from trr_backend.socials.crawlee_runtime.auth_preflight import AuthPreflightResult
from trr_backend.socials.crawlee_runtime.config import CrawleeRuntimeConfig


def _runtime_config(*, max_retries: int = 3) -> CrawleeRuntimeConfig:
    return CrawleeRuntimeConfig(
        enabled=True,
        platform="instagram",
        max_concurrency=1,
        max_retries=max_retries,
        auth_strict=False,
        enabled_platforms=("instagram",),
        force_legacy_platforms=(),
    )


def _auth_result() -> AuthPreflightResult:
    return AuthPreflightResult(
        ok=True,
        platform="instagram",
        auth_mode="cookies",
        auth_source="tests",
        missing=(),
        account_ref="@codexhuli",
    )


def test_execute_with_internal_retry_uses_one_second_first_backoff(monkeypatch) -> None:
    sleep_calls: list[float] = []

    monkeypatch.setattr(runtime.time, "sleep", lambda seconds: sleep_calls.append(seconds))

    attempts = {"count": 0}

    def _stage_runner():
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise TimeoutError("timed out")
        return 1, 0, {"ok": True}

    runtime._execute_with_internal_retry(
        platform="instagram",
        stage="posts",
        request_mode="queue",
        request_key="req-1",
        runtime_config=_runtime_config(max_retries=2),
        auth_preflight=_auth_result(),
        stage_runner=_stage_runner,
        crawlee_status=runtime._CrawleeImportStatus(available=True, version="test"),
    )

    assert sleep_calls == [1]


def test_execute_with_internal_retry_treats_max_retries_as_retries(monkeypatch) -> None:
    monkeypatch.setattr(runtime.time, "sleep", lambda _seconds: None)
    attempts = {"count": 0}

    def _stage_runner():
        attempts["count"] += 1
        if attempts["count"] <= 2:
            raise TimeoutError("timed out")
        return 1, 0, {"ok": True}

    runtime._execute_with_internal_retry(
        platform="instagram",
        stage="posts",
        request_mode="queue",
        request_key="req-1",
        runtime_config=_runtime_config(max_retries=2),
        auth_preflight=_auth_result(),
        stage_runner=_stage_runner,
        crawlee_status=runtime._CrawleeImportStatus(available=False, version=None),
    )

    assert attempts["count"] == 3


def test_run_coroutine_cancels_timed_out_coroutine() -> None:
    state = {"done": False}

    async def _slow() -> None:
        await asyncio.sleep(0.2)
        state["done"] = True

    async def _invoke() -> None:
        with pytest.raises(TimeoutError):
            runtime._run_coroutine(_slow(), join_timeout_seconds=0.05)

    asyncio.run(_invoke())
    time.sleep(0.1)

    assert state["done"] is False
