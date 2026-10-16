"""Explicit composition root for legacy social control-plane providers."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

SOCIAL_CONTROL_PLANE_PROVIDER_NAMES = (
    "queue_status",
    "run_lifecycle",
    "dispatch_runtime",
    "dispatch",
    "recovery",
    "runtime",
    "shared_accounts",
)


def register_social_control_plane_providers(provider_namespace: Mapping[str, Any]) -> None:
    """Verify every control-plane publication from explicit outer composition."""
    from trr_backend.socials.control_plane import (
        dispatch,
        dispatch_runtime,
        queue_status,
        recovery,
        run_lifecycle,
        runtime,
        shared_accounts,
    )

    provider_modules = (
        ("queue_status", queue_status),
        ("run_lifecycle", run_lifecycle),
        ("dispatch_runtime", dispatch_runtime),
        ("dispatch", dispatch),
        ("recovery", recovery),
        ("runtime", runtime),
        ("shared_accounts", shared_accounts),
    )
    for provider_name, provider_module in provider_modules:
        if provider_module._require_provider_ready() is not provider_namespace:
            raise RuntimeError(
                "SOCIAL_CONTROL_PLANE_PROVIDER_INVALID: "
                f"{provider_name} is not bound to the completed provider namespace"
            )
