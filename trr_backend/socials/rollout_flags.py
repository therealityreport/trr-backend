"""Shared rollout flag parsing for social worker lanes."""

from __future__ import annotations

import os
from typing import Any

TRUE_VALUES = frozenset({"1", "true", "yes", "on", "enabled"})
FALSE_VALUES = frozenset({"0", "false", "no", "off", "disabled"})


def resolve_rollout_flag(env_var: str, *, default_enabled: bool) -> dict[str, Any]:
    """Resolve a boolean rollout flag with metadata safe for job records."""
    normalized_env_var = str(env_var or "").strip()
    if not normalized_env_var:
        raise ValueError("env_var is required")

    raw_value = str(os.getenv(normalized_env_var) or "").strip()
    if not raw_value:
        enabled = bool(default_enabled)
    else:
        normalized = raw_value.lower()
        if normalized in TRUE_VALUES:
            enabled = True
        elif normalized in FALSE_VALUES:
            enabled = False
        else:
            enabled = bool(default_enabled)

    return {
        "env_var": normalized_env_var,
        "enabled": enabled,
        "default_enabled": bool(default_enabled),
        "configured_value": raw_value or None,
    }
