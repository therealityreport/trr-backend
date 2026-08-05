"""Configured execution port for shared claimed social jobs."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol


class SharedClaimedJobExecutor(Protocol):
    """Execute one shared claimed job through the configured composition root."""

    def __call__(
        self,
        job: Mapping[str, Any],
        *,
        worker_id: str | None = None,
    ) -> dict[str, Any]: ...


_shared_claimed_job_executor: SharedClaimedJobExecutor | None = None


def configure_shared_claimed_job_executor(executor: SharedClaimedJobExecutor) -> None:
    global _shared_claimed_job_executor

    _shared_claimed_job_executor = executor


def execute_shared_claimed_job(
    job: Mapping[str, Any],
    *,
    worker_id: str | None = None,
) -> dict[str, Any]:
    executor = _shared_claimed_job_executor
    if executor is None:
        raise RuntimeError("shared claimed-job executor is not configured")
    return executor(job, worker_id=worker_id)


__all__ = [
    "SharedClaimedJobExecutor",
    "configure_shared_claimed_job_executor",
    "execute_shared_claimed_job",
]
