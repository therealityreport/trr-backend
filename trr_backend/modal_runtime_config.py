"""Import-neutral Modal runtime environment helpers."""

from __future__ import annotations

import math
import os

_INSTAGRAM_PAYLOAD_READ_MODE_ENV = "SOCIAL_INSTAGRAM_PAYLOAD_READ_MODE"
_MODAL_INSTAGRAM_PAYLOAD_READ_MODE_ENV = "TRR_MODAL_INSTAGRAM_PAYLOAD_READ_MODE"
_INSTAGRAM_PAYLOAD_READ_MODES = frozenset({"legacy", "compare", "sidecar"})
_INSTAGRAM_PAYLOAD_COMPARE_SAMPLE_RATE_ENV = "SOCIAL_INSTAGRAM_PAYLOAD_COMPARE_SAMPLE_RATE"
_MODAL_INSTAGRAM_PAYLOAD_COMPARE_SAMPLE_RATE_ENV = "TRR_MODAL_INSTAGRAM_PAYLOAD_COMPARE_SAMPLE_RATE"
_DEFAULT_INSTAGRAM_PAYLOAD_COMPARE_SAMPLE_RATE = 0.1


def _env_flag(name: str, *, default: bool = False) -> bool:
    raw = str(os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def preview_read_only_env() -> dict[str, str]:
    """Return the explicit deployment-time preview control, when opted in."""
    if _env_flag("TRR_PREVIEW_READ_ONLY", default=False):
        return {"TRR_PREVIEW_READ_ONLY": "1"}
    return {}


def preview_read_only_function_kwargs() -> dict[str, dict[str, str]]:
    """Avoid adding an empty decorator env map to normal production functions."""
    preview_env = preview_read_only_env()
    return {"env": preview_env} if preview_env else {}


def api_runtime_env() -> dict[str, str]:
    """Return explicit, non-secret API rollout controls for Modal containers."""
    read_mode = str(os.getenv(_MODAL_INSTAGRAM_PAYLOAD_READ_MODE_ENV) or "legacy").strip().lower()
    if read_mode not in _INSTAGRAM_PAYLOAD_READ_MODES:
        allowed = ", ".join(sorted(_INSTAGRAM_PAYLOAD_READ_MODES))
        raise RuntimeError(f"{_MODAL_INSTAGRAM_PAYLOAD_READ_MODE_ENV} must be one of: {allowed}")
    if read_mode != "compare":
        sample_rate = 0.0
    else:
        raw_sample_rate = str(os.getenv(_MODAL_INSTAGRAM_PAYLOAD_COMPARE_SAMPLE_RATE_ENV) or "").strip()
        try:
            sample_rate = float(raw_sample_rate) if raw_sample_rate else _DEFAULT_INSTAGRAM_PAYLOAD_COMPARE_SAMPLE_RATE
        except ValueError as exc:
            raise RuntimeError(
                f"{_MODAL_INSTAGRAM_PAYLOAD_COMPARE_SAMPLE_RATE_ENV} must be greater than 0 and at most 1"
            ) from exc
        if not math.isfinite(sample_rate) or not 0.0 < sample_rate <= 1.0:
            raise RuntimeError(
                f"{_MODAL_INSTAGRAM_PAYLOAD_COMPARE_SAMPLE_RATE_ENV} must be greater than 0 and at most 1"
            )
    runtime_env = {
        _INSTAGRAM_PAYLOAD_READ_MODE_ENV: read_mode,
        _INSTAGRAM_PAYLOAD_COMPARE_SAMPLE_RATE_ENV: format(sample_rate, "g"),
    }
    runtime_env.update(preview_read_only_env())
    return runtime_env
