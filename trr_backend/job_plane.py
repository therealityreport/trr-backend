"""Runtime helpers for selecting local vs remote long-job execution ownership."""

from __future__ import annotations

import os
from typing import Literal

ExecutionMode = Literal["local", "remote"]


def canonical_execution_mode() -> ExecutionMode:
    raw = str(os.getenv("TRR_JOB_PLANE_MODE") or "").strip().lower()
    if raw in {"remote", "worker", "aws", "ec2"}:
        return "remote"
    if raw in {"local", "api", "inprocess", "in_process"}:
        return "local"
    # Backward-safe default unless explicitly enabled.
    return "local"


def long_job_enforce_remote() -> bool:
    raw = str(os.getenv("TRR_LONG_JOB_ENFORCE_REMOTE") or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def is_remote_job_plane_enabled() -> bool:
    return canonical_execution_mode() == "remote" or long_job_enforce_remote()


def execution_owner_label() -> str:
    return "remote_worker" if is_remote_job_plane_enabled() else "local_api"
