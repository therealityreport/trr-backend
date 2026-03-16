"""Shared SocialBlade refresh and persistence flow."""

from __future__ import annotations

import logging
import os
import re
from datetime import UTC, datetime, timedelta
from typing import Any, Callable

from trr_backend.repositories.socialblade_growth import (
    get_growth_data,
    merge_chart_data,
    upsert_growth_data,
)

logger = logging.getLogger(__name__)

SocialBladeScraper = Callable[[str], dict[str, Any]]
_DEFAULT_FRESHNESS_HOURS = 24


class SocialBladeRefreshError(RuntimeError):
    """Raised when a SocialBlade refresh cannot be completed."""


def sanitize_socialblade_handle(handle: str) -> str:
    """Normalize a user-supplied Instagram handle for SocialBlade lookups."""
    return re.sub(r"[^a-zA-Z0-9_.]", "", str(handle or "").strip())


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


def is_growth_data_fresh(
    data: dict[str, Any] | None,
    *,
    freshness_hours: int | None = None,
) -> bool:
    """Return whether an existing SocialBlade snapshot can be reused."""
    scraped_at = get_scraped_at_datetime(data)
    if scraped_at is None:
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
    person_id: str,
    handle: str,
    payload: dict[str, Any],
    source: str,
    force: bool,
) -> dict[str, Any]:
    """Merge and persist a scraped SocialBlade payload."""
    existing = get_growth_data(person_id, handle)
    merged = merge_chart_data(existing, payload)
    stored = upsert_growth_data(person_id, handle, merged)
    logger.info(
        "SocialBlade refresh persisted",
        extra={
            "person_id": person_id,
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
    person_id: str,
    handle: str,
    scraper: SocialBladeScraper,
    source: str,
    force: bool = False,
) -> dict[str, Any]:
    """Run the SocialBlade scrape, preserve historical chart data, and persist the result."""
    safe_handle = sanitize_socialblade_handle(handle)
    if not safe_handle:
        raise SocialBladeRefreshError("Invalid handle")

    existing = get_growth_data(person_id, safe_handle)
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
    )


def queue_refresh_decision(
    *,
    person_id: str,
    handle: str,
    force: bool = False,
) -> tuple[str, dict[str, Any] | None, str | None]:
    """Classify a batch refresh item as accepted or skipped before dispatch."""
    safe_handle = sanitize_socialblade_handle(handle)
    if not safe_handle:
        return "error", None, "Invalid handle"

    existing = get_growth_data(person_id, safe_handle)
    freshness_hours = socialblade_freshness_hours()
    if existing and not force and is_growth_data_fresh(existing, freshness_hours=freshness_hours):
        return "skipped", existing, f"fresh_within_{freshness_hours}h"
    return "accepted", existing, None
