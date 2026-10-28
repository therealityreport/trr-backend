"""Canonical season calendar reads without content aggregation or analysis."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from types import ModuleType
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from trr_backend.socials.control_plane.dispatch_runtime import register_provider_publication_callback

_provider: ModuleType | None = None


def _publish_provider(provider: ModuleType) -> None:
    global _provider
    _provider = provider


register_provider_publication_callback(_publish_provider)


def get_periods(
    season_id: str,
    *,
    timezone: str = "America/New_York",
    source_scope: str = "network",
) -> dict[str, Any]:
    """Use the same calendar as analytics, preserving its inclusive end bounds."""
    if _provider is None:
        raise RuntimeError("ANALYTICS_PERIODS_PROVIDER_UNCONFIGURED")
    try:
        ZoneInfo(timezone)
    except (ValueError, ZoneInfoNotFoundError) as exc:
        raise ValueError("timezone must be a valid IANA timezone") from exc

    windows = _provider.list_canonical_week_windows(season_id, timezone=timezone, source_scope=source_scope)
    weekly = []
    for window in windows:
        exclusive_end = datetime.fromisoformat(window["date_end"].replace("Z", "+00:00"))
        inclusive_end = (exclusive_end - timedelta(microseconds=1)).astimezone(UTC)
        weekly.append(
            {
                "week_index": window["week_index"],
                "label": window["label"],
                "week_type": window["week_type"],
                "episode_number": window["episode_number"],
                "start": window["date_start"],
                "end": inclusive_end.isoformat().replace("+00:00", "Z"),
            }
        )
    return {"season_id": season_id, "timezone": timezone, "source_scope": source_scope, "weekly": weekly}
