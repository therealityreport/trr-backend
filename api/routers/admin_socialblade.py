"""Admin endpoints for SocialBlade growth data."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field

from api.auth import AdminUser

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/people", tags=["admin-socialblade"])


def _scrape_socialblade_person_page(handle: str) -> dict[str, Any]:
    from trr_backend.modal_dispatch import dispatch_socialblade_scrape_sync
    from trr_backend.socials.socialblade.auth import (
        load_socialblade_cookies_from_sources,
        refresh_socialblade_cookies,
    )
    from trr_backend.socials.socialblade.scraper import scrape_socialblade

    try:
        refresh_socialblade_cookies("person_page_refresh")
        cookies = load_socialblade_cookies_from_sources()
        return scrape_socialblade(handle, cookies)
    except Exception:  # noqa: BLE001
        logger.warning(
            "Local SocialBlade scrape unavailable; falling back to Modal",
            extra={"handle": handle},
            exc_info=True,
        )
        return dispatch_socialblade_scrape_sync(handle=handle)


class SocialBladeRefreshRequest(BaseModel):
    handle: str
    force: bool = False


class SocialBladeBatchRefreshItem(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    person_id: str = Field(alias="personId")
    handle: str


class SocialBladeBatchRefreshRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    items: list[SocialBladeBatchRefreshItem]
    source: str
    force: bool = False


@router.get("/{person_id}/socialblade")
async def get_socialblade_data(
    person_id: str,
    _admin: AdminUser,
    handle: str = Query(..., description="Instagram handle"),
) -> dict[str, Any]:
    """Retrieve stored SocialBlade growth data for a person."""
    from trr_backend.socials.socialblade.service import sanitize_socialblade_handle

    safe_handle = sanitize_socialblade_handle(handle)
    if not safe_handle:
        raise HTTPException(status_code=400, detail="Invalid handle")

    from trr_backend.repositories.socialblade_growth import get_growth_data

    data = get_growth_data(person_id, safe_handle)
    if not data:
        raise HTTPException(
            status_code=404,
            detail=f"No SocialBlade data found for @{safe_handle}",
        )
    return data


@router.post("/{person_id}/socialblade/refresh")
async def refresh_socialblade_data(
    person_id: str,
    body: SocialBladeRefreshRequest,
    _admin: AdminUser,
) -> dict[str, Any]:
    """Trigger a fresh SocialBlade scrape, preferring the local shared-browser path."""
    from trr_backend.socials.socialblade.service import (
        SocialBladeRefreshError,
        refresh_and_persist_socialblade,
        sanitize_socialblade_handle,
    )

    safe_handle = sanitize_socialblade_handle(body.handle)
    if not safe_handle:
        raise HTTPException(status_code=400, detail="Invalid handle")

    try:
        return refresh_and_persist_socialblade(
            person_id=person_id,
            handle=safe_handle,
            scraper=_scrape_socialblade_person_page,
            source="person_page",
            force=body.force,
        )
    except SocialBladeRefreshError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.post("/socialblade/refresh-batch")
async def refresh_socialblade_data_batch(
    body: SocialBladeBatchRefreshRequest,
    _admin: AdminUser,
) -> dict[str, Any]:
    """Refresh SocialBlade rows for multiple cast members."""
    from trr_backend.modal_dispatch import dispatch_socialblade_scrape
    from trr_backend.socials.socialblade.service import (
        SocialBladeRefreshError,
        queue_refresh_decision,
        refresh_and_persist_socialblade,
        sanitize_socialblade_handle,
        socialblade_auto_refresh_enabled,
    )

    source = body.source.strip().lower()
    if source not in {"cast_comparison", "season_run"}:
        raise HTTPException(status_code=400, detail="Invalid source")

    if source == "season_run" and not socialblade_auto_refresh_enabled():
        return {
            "accepted": [],
            "skipped": [
                {
                    "personId": item.person_id,
                    "handle": sanitize_socialblade_handle(item.handle),
                    "reason": "auto_refresh_disabled",
                }
                for item in body.items
                if sanitize_socialblade_handle(item.handle)
            ],
            "errors": [],
        }

    accepted: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()

    for item in body.items:
        safe_handle = sanitize_socialblade_handle(item.handle)
        if not safe_handle:
            errors.append(
                {
                    "personId": item.person_id,
                    "handle": item.handle,
                    "reason": "invalid_handle",
                }
            )
            continue

        dedupe_key = (item.person_id, safe_handle)
        if dedupe_key in seen:
            skipped.append(
                {
                    "personId": item.person_id,
                    "handle": safe_handle,
                    "reason": "duplicate_request",
                }
            )
            continue
        seen.add(dedupe_key)

        status, existing, reason = queue_refresh_decision(
            person_id=item.person_id,
            handle=safe_handle,
            force=body.force,
        )
        if status == "error":
            errors.append(
                {
                    "personId": item.person_id,
                    "handle": safe_handle,
                    "reason": reason or "invalid_request",
                }
            )
            continue
        if status == "skipped":
            skipped.append(
                {
                    "personId": item.person_id,
                    "handle": safe_handle,
                    "reason": reason,
                    "scrapedAt": existing.get("scraped_at") if existing else None,
                    "freshnessStatus": existing.get("freshness_status") if existing else "missing",
                }
            )
            continue

        if source == "cast_comparison":
            try:
                refreshed = refresh_and_persist_socialblade(
                    person_id=item.person_id,
                    handle=safe_handle,
                    scraper=_scrape_socialblade_person_page,
                    source=source,
                    force=body.force,
                )
            except SocialBladeRefreshError as exc:
                errors.append(
                    {
                        "personId": item.person_id,
                        "handle": safe_handle,
                        "reason": str(exc),
                    }
                )
                continue

            accepted.append(
                {
                    "personId": item.person_id,
                    "handle": safe_handle,
                    "refreshStatus": refreshed.get("refresh_status"),
                    "scrapedAt": refreshed.get("scraped_at"),
                }
            )
            continue

        dispatch_result = dispatch_socialblade_scrape(
            person_id=item.person_id,
            handle=safe_handle,
            source=source,
            force=body.force,
        )
        if not dispatch_result.get("dispatched"):
            errors.append(
                {
                    "personId": item.person_id,
                    "handle": safe_handle,
                    "reason": dispatch_result.get("reason") or "dispatch_failed",
                }
            )
            continue

        accepted.append(
            {
                "personId": item.person_id,
                "handle": safe_handle,
                "callId": dispatch_result.get("call_id"),
            }
        )

    logger.info(
        "SocialBlade batch refresh evaluated",
        extra={
            "source": source,
            "force": body.force,
            "requested_count": len(body.items),
            "accepted_count": len(accepted),
            "skipped_count": len(skipped),
            "error_count": len(errors),
        },
    )
    return {
        "accepted": accepted,
        "skipped": skipped,
        "errors": errors,
    }
