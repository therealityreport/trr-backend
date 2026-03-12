"""Runtime helpers for selecting local vs remote long-job execution ownership."""

from __future__ import annotations

import os
from typing import Literal

ExecutionMode = Literal["local", "remote"]
RemoteExecutor = Literal["legacy_worker", "modal"]
ExecutionBackendCanonical = Literal["local", "legacy_worker", "modal"]


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


def canonical_remote_executor() -> RemoteExecutor:
    raw = str(os.getenv("TRR_REMOTE_EXECUTOR") or "").strip().lower()
    if raw in {"modal"}:
        return "modal"
    if raw in {"legacy_worker", "worker", "aws", "ec2"}:
        return "legacy_worker"

    modal_enabled = str(os.getenv("TRR_MODAL_ENABLED") or "").strip().lower() in {"1", "true", "yes", "on"}
    return "modal" if modal_enabled else "legacy_worker"


def execution_backend_canonical() -> ExecutionBackendCanonical:
    if not is_remote_job_plane_enabled():
        return "local"
    return canonical_remote_executor()


def is_modal_remote_executor_enabled() -> bool:
    return is_remote_job_plane_enabled() and canonical_remote_executor() == "modal"


def execution_owner_label() -> str:
    return "remote_worker" if is_remote_job_plane_enabled() else "local_api"


def execution_metadata() -> dict[str, str | bool]:
    return {
        "execution_mode_canonical": canonical_execution_mode(),
        "execution_owner": execution_owner_label(),
        "execution_backend_canonical": execution_backend_canonical(),
        "remote_job_plane_enforced": is_remote_job_plane_enabled(),
    }
