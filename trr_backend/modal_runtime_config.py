"""Import-neutral Modal runtime environment helpers."""

from __future__ import annotations

import math
import os
from collections.abc import Mapping

MAINTENANCE_CRON_ENV = "TRR_MODAL_ALWAYS_ON_SCHEDULES_ENABLED"
MAINTENANCE_API_ENV = "TRR_MODAL_RUNTIME_SCHEDULER_ENABLED"


def maintenance_runtime_env(environ: Mapping[str, str] | None = None) -> dict[str, str]:
    """Bind runtime ownership to the same inputs that build Modal schedules.

    Function-level env is applied after named secrets by Modal. This prevents a
    stale runtime secret from enabling the API scheduler alongside deployed cron.
    Empty ownership remains importable for state-free tooling, but is rejected by
    deployment preflight and the existing production startup validation.
    """
    env = os.environ if environ is None else environ

    def enabled(key: str) -> bool:
        return str(env.get(key) or "").strip().lower() in {"1", "true", "yes", "on"}

    cron, api = enabled(MAINTENANCE_CRON_ENV), enabled(MAINTENANCE_API_ENV)
    if cron and api:
        raise RuntimeError("Modal maintenance has duplicate active owners; select exactly one before deployment.")
    return {
        MAINTENANCE_CRON_ENV: "1" if cron else "0",
        MAINTENANCE_API_ENV: "1" if api else "0",
        "TRR_MODAL_MAINTENANCE_OWNER_REQUIRED": "1",
        "TRR_MODAL_RUNTIME_HEARTBEAT_INTERVAL_SECONDS": "60",
        "TRR_MODAL_RUNTIME_SOCIAL_RECOVERY_INTERVAL_SECONDS": "120",
    }


def validate_api_maintenance_scaling(environ: Mapping[str, str]) -> None:
    """API-owned timers require one continuously available replica."""
    api = str(environ.get(MAINTENANCE_API_ENV) or "").strip().lower() in {"1", "true", "yes", "on"}
    cron = str(environ.get(MAINTENANCE_CRON_ENV) or "").strip().lower() in {"1", "true", "yes", "on"}
    if api and not cron:
        if (environ.get("TRR_MODAL_API_MIN_CONTAINERS", "1"), environ.get("TRR_MODAL_API_MAX_CONTAINERS", "8")) != (
            "1",
            "1",
        ):
            raise RuntimeError("API maintenance ownership requires API min/max containers both exactly 1.")


def validate_maintenance_deployment(environ: Mapping[str, str]) -> dict[str, str]:
    config = maintenance_runtime_env(environ)
    if config[MAINTENANCE_CRON_ENV] == config[MAINTENANCE_API_ENV] == "0":
        raise RuntimeError("Modal maintenance has no active owner; select exactly one before deployment.")
    validate_api_maintenance_scaling(environ)
    return config


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
