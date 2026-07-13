"""Source-scope normalization helpers for social analytics."""

from __future__ import annotations

from typing import Any

SUPPORTED_SCOPES = ("network", "creator", "community", "news")
LEGACY_SOURCE_SCOPE_ALIASES = {
    "bravo": "network",
}


def normalize_source_scope(value: Any, *, default: str = "network") -> str:
    normalized = str(value or default).strip().lower() or default
    canonical = LEGACY_SOURCE_SCOPE_ALIASES.get(normalized, normalized)
    if canonical not in SUPPORTED_SCOPES:
        raise ValueError(f"Unsupported source scope: {value}")
    return canonical


def normalize_source_scope_input(value: Any, *, default: str = "network") -> str:
    normalized = str(value or default).strip().lower() or default
    canonical = normalize_source_scope(normalized, default=default)
    return normalized if normalized in LEGACY_SOURCE_SCOPE_ALIASES else canonical


def source_scope_is_network_family(value: Any) -> bool:
    return normalize_source_scope(value) == "network"

