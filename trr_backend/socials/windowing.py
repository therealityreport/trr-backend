"""Configured port for canonical social week-window resolution."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Protocol

from trr_backend.socials.provider_registry import register_legacy_patchable_namespace


class WeekWindowResolver(Protocol):
    """Resolve one canonical season week without importing the social monolith."""

    def __call__(
        self,
        season_id: str,
        *,
        week_index: int,
        timezone: str = "America/New_York",
        source_scope: str = "network",
        now_utc: datetime | None = None,
    ) -> dict[str, Any]: ...


_week_window_resolver: WeekWindowResolver | None = None


def configure_week_window_resolver(resolver: WeekWindowResolver) -> WeekWindowResolver:
    global _week_window_resolver

    _week_window_resolver = resolver
    return resolver


def resolve_week_window(
    season_id: str,
    *,
    week_index: int,
    timezone: str = "America/New_York",
    source_scope: str = "network",
    now_utc: datetime | None = None,
) -> dict[str, Any]:
    resolver = _week_window_resolver
    if resolver is None:
        raise RuntimeError("Social week-window resolver is not configured")
    return resolver(
        season_id,
        week_index=week_index,
        timezone=timezone,
        source_scope=source_scope,
        now_utc=now_utc,
    )


__all__ = [
    "WeekWindowResolver",
    "configure_week_window_resolver",
    "resolve_week_window",
]

register_legacy_patchable_namespace(globals(), ("resolve_week_window",))
