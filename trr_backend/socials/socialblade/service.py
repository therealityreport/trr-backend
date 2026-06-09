"""Shared SocialBlade refresh and persistence flow."""

from __future__ import annotations

import logging
import os
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

from trr_backend.repositories.socialblade_growth import (
    get_growth_data,
    insert_growth_snapshot,
    merge_chart_data,
    normalize_socialblade_account_handle,
    normalize_socialblade_platform,
    upsert_growth_data,
)
from trr_backend.socials.platforms import SOCIALBLADE_SUPPORTED_PLATFORMS, normalize_source_scope

logger = logging.getLogger(__name__)

SocialBladeScraper = Callable[[str], dict[str, Any]]
_DEFAULT_FRESHNESS_HOURS = 24
_DEFAULT_MIN_REUSABLE_CHART_POINTS = 30
_DEFAULT_MIN_REUSABLE_PAGE_CAPTURE_POINTS = 60
_AUTHENTICATED_HISTORY_SOURCES = frozenset({"authenticated_api", "page_trpc_capture", "page_trpc_capture_short"})
SUPPORTED_SOCIALBLADE_PLATFORMS = SOCIALBLADE_SUPPORTED_PLATFORMS
INSTAGRAM_FOLLOWING_SCRAPE_ENABLED_ENV = "SOCIALBLADE_INSTAGRAM_FOLLOWING_SCRAPE_ENABLED"
INSTAGRAM_FOLLOWING_PAGE_SIZE_ENV = "SOCIALBLADE_INSTAGRAM_FOLLOWING_PAGE_SIZE"
INSTAGRAM_FOLLOWING_MAX_PAGES_ENV = "SOCIALBLADE_INSTAGRAM_FOLLOWING_MAX_PAGES"
INSTAGRAM_FOLLOWING_MAX_RELATIONSHIPS_ENV = "SOCIALBLADE_INSTAGRAM_FOLLOWING_MAX_RELATIONSHIPS"


class SocialBladeRefreshError(RuntimeError):
    """Raised when a SocialBlade refresh cannot be completed."""


def sanitize_socialblade_handle(handle: str, *, platform: str | None = None) -> str:
    """Normalize a user-supplied social account handle for SocialBlade lookups."""
    return normalize_socialblade_account_handle(handle, platform=platform)


def sanitize_socialblade_platform(platform: str | None) -> str:
    normalized = normalize_socialblade_platform(platform)
    if normalized not in SUPPORTED_SOCIALBLADE_PLATFORMS:
        raise SocialBladeRefreshError(f"Unsupported SocialBlade platform: {platform}")
    return normalized


def normalize_socialblade_source_scope(value: str | None, *, default: str = "network") -> str:
    return normalize_source_scope(value, default=default)


def socialblade_freshness_hours() -> int:
    """Read the freshness reuse window from env."""
    raw = str(os.getenv("SOCIALBLADE_FRESHNESS_HOURS") or "").strip()
    if not raw:
        return _DEFAULT_FRESHNESS_HOURS
    try:
        parsed = int(raw)
    except ValueError:
        return _DEFAULT_FRESHNESS_HOURS
    return max(1, parsed)


def socialblade_auto_refresh_enabled() -> bool:
    """Return whether season-run sidecar refreshes are enabled."""
    raw = str(os.getenv("SOCIALBLADE_AUTO_REFRESH_ENABLED") or "true").strip().lower()
    if not raw:
        return True
    return raw in {"1", "true", "yes", "on"}


def _env_flag(name: str, *, default: bool = False) -> bool:
    raw = str(os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def _positive_int_env(name: str, default: int, *, minimum: int = 1, maximum: int = 10_000) -> int:
    raw = str(os.getenv(name) or "").strip()
    try:
        parsed = int(raw or default)
    except (TypeError, ValueError):
        parsed = default
    return max(minimum, min(maximum, parsed))


def socialblade_instagram_following_scrape_enabled() -> bool:
    """Return whether SocialBlade workers should run the Instagram following-list sidecar."""
    return _env_flag(INSTAGRAM_FOLLOWING_SCRAPE_ENABLED_ENV, default=True)


def socialblade_instagram_following_config() -> dict[str, int]:
    """Resolve Instagram following-list limits for the SocialBlade sidecar."""
    page_size = _positive_int_env(INSTAGRAM_FOLLOWING_PAGE_SIZE_ENV, 200, minimum=1, maximum=200)
    max_pages = _positive_int_env(INSTAGRAM_FOLLOWING_MAX_PAGES_ENV, 25, minimum=1, maximum=25)
    max_relationships = _positive_int_env(
        INSTAGRAM_FOLLOWING_MAX_RELATIONSHIPS_ENV,
        page_size * max_pages,
        minimum=1,
        maximum=5_000,
    )
    return {
        "page_size": page_size,
        "max_pages": max_pages,
        "max_relationships": max_relationships,
    }


def _safe_instagram_following_retrieval_meta(metadata: dict[str, Any]) -> dict[str, Any]:
    retrieval_meta = metadata.get("retrieval_meta") if isinstance(metadata.get("retrieval_meta"), dict) else {}
    allowed = {
        "profile_id",
        "pages_fetched",
        "next_cursor",
        "has_more",
        "max_pages",
        "max_relationships",
    }
    return {key: retrieval_meta.get(key) for key in allowed if key in retrieval_meta}


def _run_instagram_following_sidecar_stage(
    *,
    handle: str,
    source_scope: str,
    config: dict[str, Any],
) -> tuple[int, int, dict[str, Any]]:
    from trr_backend.socials.instagram.profile_stages import _run_instagram_profile_following_stage

    return _run_instagram_profile_following_stage(
        run_id=None,  # type: ignore[arg-type]
        source_scope=normalize_socialblade_source_scope(source_scope),
        account_handle=handle,
        config=config,
        job_id=None,  # type: ignore[arg-type]
    )


def attach_instagram_following_scrape(
    payload: dict[str, Any],
    *,
    handle: str,
    source: str,
    source_scope: str = "network",
    platform: str = "instagram",
    enabled: bool = True,
) -> dict[str, Any]:
    """Run and annotate the Instagram following-list scrape for SocialBlade worker payloads.

    Following-list capture is useful but secondary to SocialBlade growth data,
    so failures are recorded on the payload instead of failing the refresh.
    """
    rendered = dict(payload or {})
    normalized_platform = sanitize_socialblade_platform(platform)
    normalized_source_scope = normalize_socialblade_source_scope(source_scope)
    safe_handle = sanitize_socialblade_handle(handle, platform=normalized_platform)
    annotation: dict[str, Any] = {
        "enabled": bool(enabled),
        "stage": "instagram_profile_following",
        "platform": normalized_platform,
        "handle": safe_handle or handle,
        "source": source,
        "source_scope": normalized_source_scope,
        "relationship_type": "following",
    }
    if normalized_platform != "instagram":
        rendered["instagram_following_scrape"] = {**annotation, "status": "skipped", "reason": "platform_not_instagram"}
        return rendered
    if not enabled:
        rendered["instagram_following_scrape"] = {**annotation, "status": "skipped", "reason": "disabled_by_call"}
        return rendered
    if not socialblade_instagram_following_scrape_enabled():
        rendered["instagram_following_scrape"] = {**annotation, "status": "skipped", "reason": "disabled_by_env"}
        return rendered
    if not safe_handle:
        rendered["instagram_following_scrape"] = {**annotation, "status": "skipped", "reason": "invalid_handle"}
        return rendered
    if rendered.get("error") and not rendered.get("username"):
        rendered["instagram_following_scrape"] = {
            **annotation,
            "status": "skipped",
            "reason": "socialblade_scrape_failed",
        }
        return rendered

    config = {
        **socialblade_instagram_following_config(),
        "delay_seconds": 0,
        "browser_account_id": safe_handle,
        "source": source,
        "source_scope": normalized_source_scope,
        "source_stage": "socialblade_worker",
    }
    try:
        _posts_inserted, _comments_inserted, metadata = _run_instagram_following_sidecar_stage(
            handle=safe_handle,
            source_scope=normalized_source_scope,
            config=config,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "SocialBlade Instagram following sidecar failed",
            extra={"handle": safe_handle, "source": source, "source_scope": normalized_source_scope},
            exc_info=True,
        )
        rendered["instagram_following_scrape"] = {
            **annotation,
            "status": "failed",
            "reason": "instagram_following_scrape_failed",
            "error": str(exc),
        }
        return rendered

    metadata = metadata if isinstance(metadata, dict) else {}
    rendered["instagram_following_scrape"] = {
        **annotation,
        "status": "completed",
        "relationships_fetched": metadata.get("relationships_fetched"),
        "relationships_upserted": metadata.get("relationships_upserted"),
        "relationships_missing": metadata.get("relationships_missing"),
        "snapshot_id": metadata.get("snapshot_id"),
        "source_is_complete": metadata.get("source_is_complete"),
        "relationship_mismatches": metadata.get("relationship_mismatches") or [],
        "retrieval_meta": _safe_instagram_following_retrieval_meta(metadata),
    }
    return rendered


def scrape_socialblade_then_following(
    scraper: SocialBladeScraper,
    handle: str,
    *,
    source: str,
    source_scope: str = "network",
    platform: str = "instagram",
    enabled: bool = True,
) -> dict[str, Any]:
    """Run the SocialBlade scraper, then run the Instagram following sidecar."""
    payload = scraper(handle)
    return attach_instagram_following_scrape(
        payload,
        handle=handle,
        platform=platform,
        source=source,
        source_scope=source_scope,
        enabled=enabled,
    )


def get_scraped_at_datetime(data: dict[str, Any] | None) -> datetime | None:
    """Parse the scraped timestamp from a stored SocialBlade payload."""
    if not data:
        return None
    raw = data.get("scraped_at")
    if isinstance(raw, datetime):
        return raw.astimezone(UTC) if raw.tzinfo else raw.replace(tzinfo=UTC)
    rendered = str(raw or "").strip()
    if not rendered:
        return None
    try:
        parsed = datetime.fromisoformat(rendered.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(UTC) if parsed.tzinfo else parsed.replace(tzinfo=UTC)


def _latest_metrics_date(data: dict[str, Any] | None) -> str | None:
    metrics = (data or {}).get("daily_channel_metrics_60day")
    if not isinstance(metrics, dict):
        return None
    dates: list[str] = []
    for row in metrics.get("data") or []:
        if not isinstance(row, dict):
            continue
        rendered = str(row.get("Date") or "").strip()[:10]
        if len(rendered) == 10:
            dates.append(rendered)
    return max(dates) if dates else None


def _chart_end_date(data: dict[str, Any] | None) -> str | None:
    chart = (data or {}).get("daily_total_followers_chart")
    if not isinstance(chart, dict):
        return None
    date_range = chart.get("date_range")
    if isinstance(date_range, dict):
        rendered = str(date_range.get("to") or "").strip()[:10]
        if len(rendered) == 10:
            return rendered
    dates: list[str] = []
    for point in chart.get("data") or []:
        if not isinstance(point, dict):
            continue
        rendered = str(point.get("date") or "").strip()[:10]
        if len(rendered) == 10:
            dates.append(rendered)
    return max(dates) if dates else None


def _chart_lags_metrics(data: dict[str, Any] | None) -> bool:
    latest_metrics_date = _latest_metrics_date(data)
    if latest_metrics_date is None:
        return False
    chart_end_date = _chart_end_date(data)
    if chart_end_date is None:
        return True
    return chart_end_date < latest_metrics_date


def _chart_point_count(data: dict[str, Any] | None) -> int:
    chart = (data or {}).get("daily_total_followers_chart")
    if not isinstance(chart, dict):
        return 0
    try:
        return int(chart.get("total_data_points") or 0)
    except (TypeError, ValueError):
        return 0


def _metrics_row_count(data: dict[str, Any] | None) -> int:
    metrics = (data or {}).get("daily_channel_metrics_60day")
    if not isinstance(metrics, dict):
        return 0
    try:
        return int(metrics.get("row_count") or 0)
    except (TypeError, ValueError):
        return 0


def is_growth_data_fresh(
    data: dict[str, Any] | None,
    *,
    freshness_hours: int | None = None,
) -> bool:
    """Return whether an existing SocialBlade snapshot can be reused."""
    if not data or not bool(data.get("stats_refreshed", False)):
        return False
    scraped_at = get_scraped_at_datetime(data)
    if scraped_at is None:
        return False
    history_source = str((data or {}).get("history_source") or "").strip().lower()
    if history_source and history_source not in _AUTHENTICATED_HISTORY_SOURCES:
        return False
    if history_source in {"page_trpc_capture", "page_trpc_capture_short"} and max(
        _chart_point_count(data), _metrics_row_count(data)
    ) < (
        _DEFAULT_MIN_REUSABLE_PAGE_CAPTURE_POINTS
    ):
        return False
    if _chart_lags_metrics(data):
        return False
    if not history_source:
        if _chart_point_count(data) < _DEFAULT_MIN_REUSABLE_CHART_POINTS:
            return False
    ttl = timedelta(hours=freshness_hours or socialblade_freshness_hours())
    return datetime.now(tz=UTC) - scraped_at <= ttl


def build_refresh_metadata(
    payload: dict[str, Any],
    *,
    status: str,
    source: str,
    force: bool,
    skipped_reason: str | None = None,
) -> dict[str, Any]:
    """Attach refresh status metadata to a SocialBlade response payload."""
    rendered = dict(payload)
    rendered["refresh_status"] = status
    rendered["refresh_source"] = source
    rendered["refresh_forced"] = force
    if skipped_reason:
        rendered["refresh_skipped_reason"] = skipped_reason
    return rendered


def persist_scraped_payload(
    *,
    person_id: str | None,
    handle: str,
    payload: dict[str, Any],
    source: str,
    force: bool,
    platform: str = "instagram",
) -> dict[str, Any]:
    """Merge and persist a scraped SocialBlade payload."""
    normalized_platform = sanitize_socialblade_platform(platform)
    existing = get_growth_data(person_id, handle, platform=normalized_platform)
    merged = merge_chart_data(existing, payload)
    stored = upsert_growth_data(person_id, handle, merged, platform=normalized_platform)
    try:
        snapshot = insert_growth_snapshot(
            person_id,
            handle,
            payload,
            platform=normalized_platform,
            growth_data_id=stored.get("row_id"),
            source=source,
            force=force,
        )
        stored["snapshot_id"] = snapshot.get("id")
    except Exception:  # noqa: BLE001
        logger.warning(
            "SocialBlade snapshot insert failed",
            extra={
                "person_id": person_id,
                "platform": normalized_platform,
                "handle": handle,
                "source": source,
            },
            exc_info=True,
        )
    logger.info(
        "SocialBlade refresh persisted",
        extra={
            "person_id": person_id,
            "platform": normalized_platform,
            "handle": handle,
            "source": source,
            "force": force,
            "had_existing": bool(existing),
        },
    )
    return build_refresh_metadata(
        stored,
        status="refreshed",
        source=source,
        force=force,
    )


def refresh_and_persist_socialblade(
    *,
    person_id: str | None,
    handle: str,
    scraper: SocialBladeScraper,
    source: str,
    force: bool = False,
    platform: str = "instagram",
) -> dict[str, Any]:
    """Run the SocialBlade scrape, preserve historical chart data, and persist the result."""
    normalized_platform = sanitize_socialblade_platform(platform)
    safe_handle = sanitize_socialblade_handle(handle, platform=normalized_platform)
    if not safe_handle:
        raise SocialBladeRefreshError("Invalid handle")

    existing = get_growth_data(person_id, safe_handle, platform=normalized_platform)
    freshness_hours = socialblade_freshness_hours()
    if existing and not force and is_growth_data_fresh(existing, freshness_hours=freshness_hours):
        return build_refresh_metadata(
            existing,
            status="skipped",
            source=source,
            force=force,
            skipped_reason=f"fresh_within_{freshness_hours}h",
        )

    fresh_data = scraper(safe_handle)
    if "error" in fresh_data and not fresh_data.get("username"):
        raise SocialBladeRefreshError(f"Scrape failed: {fresh_data['error']}")
    return persist_scraped_payload(
        person_id=person_id,
        handle=safe_handle,
        payload=fresh_data,
        source=source,
        force=force,
        platform=normalized_platform,
    )


def queue_refresh_decision(
    *,
    person_id: str | None,
    handle: str,
    force: bool = False,
    platform: str = "instagram",
) -> tuple[str, dict[str, Any] | None, str | None]:
    """Classify a batch refresh item as accepted or skipped before dispatch."""
    normalized_platform = sanitize_socialblade_platform(platform)
    safe_handle = sanitize_socialblade_handle(handle, platform=normalized_platform)
    if not safe_handle:
        return "error", None, "Invalid handle"

    existing = get_growth_data(person_id, safe_handle, platform=normalized_platform)
    freshness_hours = socialblade_freshness_hours()
    if existing and not force and is_growth_data_fresh(existing, freshness_hours=freshness_hours):
        return "skipped", existing, f"fresh_within_{freshness_hours}h"
    return "accepted", existing, None
