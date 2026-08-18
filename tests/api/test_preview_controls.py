"""Focused behavior tests for the isolated read-only preview controls."""

from __future__ import annotations

import asyncio

import pytest

from api import main as api_main


def test_preview_lifespan_skips_broker_and_all_background_tasks(monkeypatch: pytest.MonkeyPatch) -> None:
    """Read-only preview startup remains select-only and state-free."""
    lifecycle_calls: list[str] = []
    broker_calls: list[str] = []
    background_coroutines: list[object] = []

    monkeypatch.setenv("TRR_PREVIEW_READ_ONLY", "1")
    monkeypatch.setattr(api_main, "_validate_startup_config", lambda: lifecycle_calls.append("validated"))
    monkeypatch.setattr(api_main, "_prewarm_database_pool", lambda: lifecycle_calls.append("prewarmed"))
    monkeypatch.setattr(api_main, "_cast_screentime_stale_sweeper_enabled", lambda: True)
    monkeypatch.setattr(api_main, "_modal_runtime_scheduler_enabled", lambda: True)

    async def _init_broker() -> None:
        broker_calls.append("init")

    async def _shutdown_broker() -> None:
        broker_calls.append("shutdown")

    def _record_background_task(coroutine: object) -> None:
        close = getattr(coroutine, "close", None)
        if callable(close):
            close()
        background_coroutines.append(coroutine)

    monkeypatch.setattr(api_main, "init_broker", _init_broker)
    monkeypatch.setattr(api_main, "shutdown_broker", _shutdown_broker)
    monkeypatch.setattr(api_main.asyncio, "create_task", _record_background_task)

    async def _exercise_lifespan() -> None:
        async with api_main.lifespan(api_main.app):
            pass

    asyncio.run(_exercise_lifespan())

    assert lifecycle_calls == ["validated", "prewarmed"]
    assert broker_calls == []
    assert background_coroutines == []


def test_default_lifespan_still_manages_the_broker(monkeypatch: pytest.MonkeyPatch) -> None:
    """Normal runtimes retain their existing broker lifecycle by default."""
    broker_calls: list[str] = []

    monkeypatch.delenv("TRR_PREVIEW_READ_ONLY", raising=False)
    monkeypatch.setattr(api_main, "_validate_startup_config", lambda: None)
    monkeypatch.setattr(api_main, "_prewarm_database_pool", lambda: None)
    monkeypatch.setattr(api_main, "_cast_screentime_stale_sweeper_enabled", lambda: False)
    monkeypatch.setattr(api_main, "_modal_runtime_scheduler_enabled", lambda: False)

    async def _init_broker() -> None:
        broker_calls.append("init")

    async def _shutdown_broker() -> None:
        broker_calls.append("shutdown")

    monkeypatch.setattr(api_main, "init_broker", _init_broker)
    monkeypatch.setattr(api_main, "shutdown_broker", _shutdown_broker)

    async def _exercise_lifespan() -> None:
        async with api_main.lifespan(api_main.app):
            pass

    asyncio.run(_exercise_lifespan())

    assert broker_calls == ["init", "shutdown"]
