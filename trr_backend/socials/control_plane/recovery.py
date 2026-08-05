# ruff: noqa: F822
"""Recovery and remediation flows for the social control plane."""

from __future__ import annotations

from typing import Any

from trr_backend.socials.control_plane.run_lifecycle import reconcile_run_summaries
from trr_backend.socials.provider_registry import LateNamespaceProvider

_LEGACY_EXPORT_NAMES = (
    "cancel_active_jobs",
    "cancel_claimed_job_before_processing",
    "cancel_dispatch_blocked_jobs",
    "cancel_stuck_jobs",
    "debug_ingest_job_with_openai",
    "dismiss_recent_failures",
    "recover_stale_running_jobs",
    "reset_social_ingest_health",
)


def _publish_provider_binding(name: str, value: Any) -> None:
    globals()[name] = value


_PROVIDER = LateNamespaceProvider(
    globals(),
    prefix="SOCIAL_RECOVERY_PROVIDER",
    bindings={name: name for name in _LEGACY_EXPORT_NAMES},
    publisher=lambda name, value: _publish_provider_binding(name, value),
    unconfigured_message="SOCIAL_RECOVERY_PROVIDER_UNCONFIGURED: provider publication has not completed",
)


def _require_provider_ready() -> dict[str, Any]:
    return _PROVIDER.require()  # type: ignore[return-value]


def _configure_legacy_provider(provider: dict[str, Any]) -> None:
    """Publish compatibility exports after the exact provider finishes loading."""

    _PROVIDER.configure(provider)


def __getattr__(name: str) -> Any:
    if name in _LEGACY_EXPORT_NAMES:
        _require_provider_ready()
        return globals()[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "cancel_active_jobs",
    "cancel_claimed_job_before_processing",
    "cancel_dispatch_blocked_jobs",
    "cancel_stuck_jobs",
    "debug_ingest_job_with_openai",
    "dismiss_recent_failures",
    "reconcile_run_summaries",
    "recover_stale_running_jobs",
    "reset_social_ingest_health",
]
