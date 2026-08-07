"""Import-neutral types for claimed social job handlers."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol


class PlatformJobHandler(Protocol):
    """Executable handler for one platform/stage pair."""

    @property
    def platform(self) -> str:
        """Platform key owned by this handler."""
        ...

    @property
    def stage(self) -> str:
        """Stage key owned by this handler."""
        ...

    def supports(self, platform: str, stage: str) -> bool:
        """Return true when this handler owns the claimed job."""
        ...

    def execute(self, job: dict[str, Any], *, worker_id: str | None = None) -> dict[str, Any]:
        """Execute the claimed job and return the persisted job row."""
        ...


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


__all__ = ["FunctionPlatformJobHandler", "PlatformJobHandler"]
