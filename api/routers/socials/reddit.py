"""Reddit refresh and Reddit analytics route surface."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from ._surfaces import RouteRecord, routes_matching

ROUTE_PREFIXES = ("/admin/socials/reddit/",)


def surface_routes(router: Any) -> list[RouteRecord]:
    return routes_matching(router, ROUTE_PREFIXES)


def serialize_reddit_refresh_payload(payload: Any) -> dict[str, Any]:
    data = payload.model_dump()
    period_stable_key = getattr(payload, "period_stable_key", None)
    period_label = getattr(payload, "period_label", None)
    run_config_hash = getattr(payload, "run_config_hash", None)
    period_start = getattr(payload, "period_start", None)
    period_end = getattr(payload, "period_end", None)

    if isinstance(period_stable_key, str):
        normalized_stable_key = period_stable_key.strip()
        data["period_stable_key"] = normalized_stable_key or None
    if isinstance(period_label, str):
        normalized_period_label = period_label.strip()
        data["period_label"] = normalized_period_label or None
    if isinstance(run_config_hash, str):
        normalized_hash = run_config_hash.strip().lower()
        data["run_config_hash"] = normalized_hash or None
    if isinstance(period_start, datetime):
        data["period_start"] = period_start.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if isinstance(period_end, datetime):
        data["period_end"] = period_end.astimezone(UTC).isoformat().replace("+00:00", "Z")
    data["community_id"] = str(payload.community_id)
    data["season_id"] = str(payload.season_id)
    return data


def normalize_reddit_backfill_container_keys(values: list[str]) -> list[str]:
    return [item.strip() for item in values if isinstance(item, str) and item.strip()]


def serialize_reddit_backfill_payload(payload: Any) -> dict[str, Any]:
    return {
        "community_id": str(payload.community_id),
        "season_id": str(payload.season_id),
        "container_keys": normalize_reddit_backfill_container_keys(payload.container_keys),
        "mode": payload.mode,
        "detail_refresh": bool(payload.detail_refresh),
    }
