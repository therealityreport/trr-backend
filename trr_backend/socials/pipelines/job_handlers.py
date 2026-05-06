"""Platform job handler registry for claimed social jobs."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol


class PlatformJobHandler(Protocol):
    """Executable handler for one platform/stage pair."""

    platform: str
    stage: str

    def supports(self, platform: str, stage: str) -> bool:
        """Return true when this handler owns the claimed job."""

    def execute(self, job: dict[str, Any], *, worker_id: str | None = None) -> dict[str, Any]:
        """Execute the claimed job and return the persisted job row."""


@dataclass(frozen=True)
class FunctionPlatformJobHandler:
    platform: str
    stage: str
    execute_func: Callable[[dict[str, Any]], dict[str, Any]]

    def supports(self, platform: str, stage: str) -> bool:
        return self.platform == _normalize_key(platform) and self.stage == _normalize_key(stage)

    def execute(self, job: dict[str, Any], *, worker_id: str | None = None) -> dict[str, Any]:
        return self.execute_func(job, worker_id=worker_id)  # type: ignore[misc]


def _normalize_key(value: Any) -> str:
    return str(value or "").strip().lower()


def registered_platform_job_handlers() -> tuple[PlatformJobHandler, ...]:
    from trr_backend.socials.instagram.jobs import instagram_job_handlers
    from trr_backend.socials.threads.jobs import threads_job_handlers
    from trr_backend.socials.tiktok.jobs import tiktok_job_handlers

    return (*instagram_job_handlers(), *tiktok_job_handlers(), *threads_job_handlers())


def resolve_platform_job_handler(platform: str, stage: str) -> PlatformJobHandler | None:
    normalized_platform = _normalize_key(platform)
    normalized_stage = _normalize_key(stage)
    matches = [
        handler
        for handler in registered_platform_job_handlers()
        if handler.supports(normalized_platform, normalized_stage)
    ]
    if len(matches) > 1:
        raise RuntimeError(f"multiple_social_job_handlers:{normalized_platform}:{normalized_stage}")
    return matches[0] if matches else None


__all__ = [
    "FunctionPlatformJobHandler",
    "PlatformJobHandler",
    "registered_platform_job_handlers",
    "resolve_platform_job_handler",
]
