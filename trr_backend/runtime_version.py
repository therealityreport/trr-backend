"""Project-import-neutral runtime version stamp construction."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

_COMMIT_ENV_NAMES = (
    "TRR_RUNTIME_VERSION",
    "TRR_DEPLOY_VERSION",
    "RENDER_GIT_COMMIT",
    "VERCEL_GIT_COMMIT_SHA",
    "RAILWAY_GIT_COMMIT_SHA",
    "COMMIT_SHA",
    "GIT_COMMIT_SHA",
)


def build_runtime_version_stamp(
    *,
    getenv: Callable[[str], str | None],
    modal_environment: str | None,
    modal_function: str | None,
    execution_backend: str,
) -> dict[str, Any]:
    """Build the shared runtime version payload without importing project modules."""

    commit_sha = ""
    for env_name in _COMMIT_ENV_NAMES:
        value = str(getenv(env_name) or "").strip()
        if value:
            commit_sha = value
            break

    modal_image = str(getenv("MODAL_IMAGE_ID") or getenv("MODAL_IMAGE_TAG") or "").strip() or None
    modal_env = modal_environment or None
    effective_modal_function = modal_function or None

    return {
        "commit_sha": commit_sha or None,
        "modal_image": modal_image,
        "modal_environment": modal_env,
        "modal_function": effective_modal_function,
        "execution_backend": execution_backend,
        "label": " · ".join(
            part
            for part in (
                (commit_sha[:12] if commit_sha else None),
                (f"modal:{modal_env}" if modal_env else None),
                modal_image,
            )
            if part
        )
        or execution_backend,
    }


__all__ = ["build_runtime_version_stamp"]
