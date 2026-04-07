from __future__ import annotations

import os
from pathlib import Path
from typing import MutableMapping

WORKSPACE_TO_RUNTIME_ENV = {
    "WORKSPACE_TRR_JOB_PLANE_MODE": "TRR_JOB_PLANE_MODE",
    "WORKSPACE_TRR_LONG_JOB_ENFORCE_REMOTE": "TRR_LONG_JOB_ENFORCE_REMOTE",
    "WORKSPACE_TRR_REMOTE_EXECUTOR": "TRR_REMOTE_EXECUTOR",
    "WORKSPACE_TRR_MODAL_ENABLED": "TRR_MODAL_ENABLED",
    "WORKSPACE_TRR_MODAL_APP_NAME": "TRR_MODAL_APP_NAME",
    "WORKSPACE_TRR_MODAL_ADMIN_OPERATION_FUNCTION": "TRR_MODAL_ADMIN_OPERATION_FUNCTION",
    "WORKSPACE_TRR_MODAL_GOOGLE_NEWS_FUNCTION": "TRR_MODAL_GOOGLE_NEWS_FUNCTION",
    "WORKSPACE_TRR_MODAL_REDDIT_REFRESH_FUNCTION": "TRR_MODAL_REDDIT_REFRESH_FUNCTION",
    "WORKSPACE_TRR_MODAL_SOCIAL_JOB_FUNCTION": "TRR_MODAL_SOCIAL_JOB_FUNCTION",
    "WORKSPACE_TRR_MODAL_SOCIAL_RECOVERY_FUNCTION": "TRR_MODAL_SOCIAL_RECOVERY_FUNCTION",
    "WORKSPACE_TRR_MODAL_RUNTIME_SECRET_NAME": "TRR_MODAL_RUNTIME_SECRET_NAME",
    "WORKSPACE_TRR_MODAL_SOCIAL_SECRET_NAME": "TRR_MODAL_SOCIAL_SECRET_NAME",
}


def _strip_matching_quotes(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        return value[1:-1]
    return value


def _parse_workspace_env_file(path: Path) -> dict[str, str]:
    parsed: dict[str, str] = {}
    if not path.is_file():
        return parsed
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        parsed[key] = _strip_matching_quotes(value.strip())
    return parsed


def apply_workspace_runtime_env(
    *,
    repo_root: Path,
    environ: MutableMapping[str, str] | None = None,
) -> dict[str, str]:
    runtime_environ = environ if environ is not None else os.environ
    workspace_env = _parse_workspace_env_file(repo_root.parent / ".logs" / "workspace" / "pids.env")
    applied: dict[str, str] = {}

    for workspace_key, runtime_key in WORKSPACE_TO_RUNTIME_ENV.items():
        value = workspace_env.get(workspace_key)
        if not value or runtime_environ.get(runtime_key):
            continue
        runtime_environ[runtime_key] = value
        applied[runtime_key] = value

    return applied
