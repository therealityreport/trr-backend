"""Crawlee adapter for YouTube stage execution."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from trr_backend.socials.crawlee_runtime import (
    AuthPreflightResult,
    CrawleeRuntimeConfig,
    execute_platform_stage_with_crawlee,
)


def run_stage_with_crawlee(
    *,
    stage: str,
    account: str,
    config: dict[str, Any],
    runtime_config: CrawleeRuntimeConfig,
    auth_preflight: AuthPreflightResult,
    stage_runner: Callable[[], tuple[int, int, dict[str, Any]]],
) -> tuple[int, int, dict[str, Any]]:
    return execute_platform_stage_with_crawlee(
        platform="youtube",
        stage=stage,
        account=account,
        request_mode=str(config.get("ingest_mode") or "posts_and_comments"),
        runtime_config=runtime_config,
        auth_preflight=auth_preflight,
        stage_runner=stage_runner,
        config=config,
    )
