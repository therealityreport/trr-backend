"""Import-neutral types for claimed social job handlers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


class PlatformJobHandler(Protocol):
    """Executable handler for one platform/stage pair."""

    @property
    def platform(self) -> str: ...

    @property
    def stage(self) -> str: ...

    def supports(self, platform: str, stage: str) -> bool:
        """Return true when this handler owns the claimed job."""

        ...

    def execute(self, job: dict[str, Any], *, worker_id: str | None = None) -> dict[str, Any]:
        """Execute the claimed job and return the persisted job row."""

        ...


class PlatformJobExecutor(Protocol):
    """Callable contract for a claimed social job executor."""

    def __call__(
        self,
        job: dict[str, Any],
        *,
        worker_id: str | None,
    ) -> dict[str, Any]: ...


@dataclass(frozen=True)
class FunctionPlatformJobHandler:
    platform: str
    stage: str
    execute_func: PlatformJobExecutor

    def supports(self, platform: str, stage: str) -> bool:
        return self.platform == _normalize_key(platform) and self.stage == _normalize_key(stage)

    def execute(self, job: dict[str, Any], *, worker_id: str | None = None) -> dict[str, Any]:
        return self.execute_func(job, worker_id=worker_id)


def _normalize_key(value: Any) -> str:
    return str(value or "").strip().lower()


__all__ = ["FunctionPlatformJobHandler", "PlatformJobHandler"]
