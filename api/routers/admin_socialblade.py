"""Admin endpoints for SocialBlade growth data."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any, cast

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field
from starlette.concurrency import run_in_threadpool

from api.auth import InternalAdminUser

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/people", tags=["admin-socialblade"])


def _scrape_socialblade_person_page(handle: str) -> dict[str, Any]:
    from trr_backend.socials.socialblade.auth import (
        load_socialblade_cookies_from_sources,
        refresh_socialblade_cookies,
    )
    from trr_backend.socials.socialblade.scraper import scrape_socialblade
    from trr_backend.socials.socialblade.service import SocialBladeRefreshError

    try:
        refresh_socialblade_cookies("person_page_refresh", allow_headless_fallback=False)
        cookies = load_socialblade_cookies_from_sources()
        return scrape_socialblade(
            handle,
            cookies,
            platform="instagram",
            allow_login_fallback=False,
            allow_visible_browser_retry=True,
        )
    except SocialBladeRefreshError:
        raise
    except Exception as exc:  # noqa: BLE001
        logger.warning("Local SocialBlade scrape failed", extra={"handle": handle}, exc_info=True)
        raise SocialBladeRefreshError(str(exc)) from exc


class SocialBladeRefreshRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    handle: str
    force: bool = False
    source_scope: str = Field(default="network", alias="sourceScope")


class SocialBladeBatchRefreshItem(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    person_id: str = Field(alias="personId")
    handle: str


class SocialBladeBatchRefreshRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    items: list[SocialBladeBatchRefreshItem]
    source: str
    force: bool = False
    source_scope: str = Field(default="network", alias="sourceScope")


def _utcnow_iso() -> str:
    return datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")


def _to_iso_string(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        rendered = value.isoformat()
    else:
        rendered = str(value or "").strip()
    if not rendered:
        return None
    return rendered.replace("+00:00", "Z")


def _dedupe_nonempty_strings(values: list[str] | None) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values or []:
        rendered = str(value or "").strip()
        if not rendered or rendered in seen:
            continue
        seen.add(rendered)
        deduped.append(rendered)
    return deduped


def _normalize_person_id_or_400(person_id: str | None) -> str | None:
    from trr_backend.repositories.socialblade_growth import normalize_socialblade_person_id

    try:
        return normalize_socialblade_person_id(person_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


def _modal_call_status_response(inspection: dict[str, Any], *, call_id: str) -> dict[str, Any]:
    status = str(inspection.get("status") or "unknown").strip().lower() or "unknown"
    terminal = bool(inspection.get("terminal")) or status in {"completed", "failed", "cancelled"}
    raw_status = str(inspection.get("raw_status") or "").strip().lower() or None
    task_id = str(inspection.get("task_id") or "").strip() or None
    reason = str(inspection.get("reason") or "").strip() or None
    error = str(inspection.get("error") or "").strip() or None
    checked_at = _to_iso_string(inspection.get("checked_at")) or _utcnow_iso()
    return {
        "callId": str(inspection.get("function_call_id") or call_id).strip(),
        "status": status,
        "rawStatus": raw_status,
        "taskId": task_id,
        "finished": terminal,
        "terminal": terminal,
        "reason": reason,
        "error": error,
        "checkedAt": checked_at,
    }


def _snapshot_history_status(row: dict[str, Any], raw_response: dict[str, Any]) -> str:
    if bool(row.get("stats_refreshed")):
        return "completed"
    if str(raw_response.get("last_attempt_error") or raw_response.get("error") or "").strip():
        return "failed"
    return "attempted"


def _snapshot_history_item(row: dict[str, Any]) -> dict[str, Any]:
    raw_response_value = row.get("raw_response")
    raw_response = raw_response_value if isinstance(raw_response_value, dict) else {}
    error = str(raw_response.get("last_attempt_error") or raw_response.get("error") or "").strip() or None
    reason = str(raw_response.get("reason") or raw_response.get("last_attempt_history_source") or "").strip() or None
    return {
        "snapshotId": str(row.get("id") or "").strip() or None,
        "personId": str(row.get("person_id") or "").strip() or None,
        "handle": str(row.get("account_handle") or row.get("instagram_handle") or "").strip() or None,
        "platform": str(row.get("platform") or "instagram").strip().lower() or "instagram",
        "scrapedAt": _to_iso_string(row.get("scraped_at")),
        "status": _snapshot_history_status(row, raw_response),
        "statsRefreshed": bool(row.get("stats_refreshed")),
        "source": str(row.get("refresh_source") or row.get("snapshot_source") or "").strip() or None,
        "snapshotSource": str(row.get("snapshot_source") or "").strip() or None,
        "refreshSource": str(row.get("refresh_source") or "").strip() or None,
        "forced": bool(row.get("refresh_forced")),
        "reason": reason,
        "error": error,
    }


@router.get("/socialblade/calls/{call_id}")
async def get_socialblade_modal_call_status(
    call_id: str,
    _admin: InternalAdminUser = cast(InternalAdminUser, None),
) -> dict[str, Any]:
    """Inspect the live Modal function-call status for a SocialBlade scrape."""
    safe_call_id = str(call_id or "").strip()
    if not safe_call_id:
        raise HTTPException(status_code=400, detail="Invalid Modal call id")

    from trr_backend.modal_dispatch import inspect_modal_function_call

    inspection = await run_in_threadpool(inspect_modal_function_call, safe_call_id)
    return _modal_call_status_response(inspection, call_id=safe_call_id)


@router.get("/socialblade/history")
async def get_socialblade_history(
    person_ids: list[str] | None = Query(default=None, alias="personId"),
    handles: list[str] | None = Query(default=None, alias="handle"),
    platform: str = Query(default="instagram", min_length=1),
    limit: int = Query(default=25, ge=1, le=100),
    _admin: InternalAdminUser = cast(InternalAdminUser, None),
) -> dict[str, Any]:
    """Return compact SocialBlade snapshot history for a cast/member set."""
    from trr_backend.db import pg
    from trr_backend.repositories.socialblade_growth import (
        normalize_socialblade_account_handle,
        normalize_socialblade_person_id,
        normalize_socialblade_platform,
        socialblade_growth_snapshots_table_exists,
    )

    normalized_platform = normalize_socialblade_platform(platform)
    try:
        safe_person_ids = _dedupe_nonempty_strings(
            [normalized for value in person_ids or [] if (normalized := normalize_socialblade_person_id(value))]
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    safe_handles = _dedupe_nonempty_strings(
        [normalize_socialblade_account_handle(handle, platform=normalized_platform) for handle in handles or []]
    )
    if not safe_person_ids and not safe_handles:
        raise HTTPException(status_code=400, detail="At least one personId or handle is required")

    checked_at = _utcnow_iso()
    if not socialblade_growth_snapshots_table_exists():
        return {
            "items": [],
            "count": 0,
            "source": "socialblade_growth_snapshots",
            "reason": "socialblade_growth_snapshots_missing",
            "checkedAt": checked_at,
        }

    filters: list[str] = []
    params: list[Any] = [normalized_platform]
    if safe_person_ids:
        filters.append("person_id::text = any(%s)")
        params.append(safe_person_ids)
    if safe_handles:
        filters.append("account_handle = any(%s)")
        params.append(safe_handles)
    params.append(limit)

    rows = pg.fetch_all(
        f"""
        select
          id::text as id,
          person_id::text as person_id,
          platform,
          account_handle,
          instagram_handle,
          scraped_at,
          stats_refreshed,
          snapshot_source,
          refresh_source,
          refresh_forced,
          raw_response
        from pipeline.socialblade_growth_snapshots
        where platform = %s
          and ({" or ".join(filters)})
        order by scraped_at desc nulls last, id desc
        limit %s
        """,
        params,
    )
    items = [_snapshot_history_item(row) for row in rows]
    return {
        "items": items,
        "count": len(items),
        "source": "socialblade_growth_snapshots",
        "filters": {
            "personIds": safe_person_ids,
            "handles": safe_handles,
            "platform": normalized_platform,
            "limit": limit,
        },
        "checkedAt": checked_at,
    }


@router.get("/socialblade/cookies/health")
async def get_socialblade_cookie_health(
    validate: bool = Query(default=True),
    handle: str | None = Query(default=None),
    _admin: InternalAdminUser = cast(InternalAdminUser, None),
) -> dict[str, Any]:
    """Return redacted SocialBlade cookie health for admin preflight panels."""
    from trr_backend.socials.socialblade.auth import socialblade_cookie_health_report
    from trr_backend.socials.socialblade.service import sanitize_socialblade_handle

    validation_handle = sanitize_socialblade_handle(handle or "") or None
    return await run_in_threadpool(
        socialblade_cookie_health_report,
        validate=validate,
        validation_handle=validation_handle,
    )


@router.get("/{person_id}/socialblade")
async def get_socialblade_data(
    person_id: str,
    _admin: InternalAdminUser,
    handle: str = Query(..., description="Instagram handle"),
) -> dict[str, Any]:
    """Retrieve stored SocialBlade growth data for a person."""
    from trr_backend.socials.socialblade.service import sanitize_socialblade_handle

    safe_person_id = _normalize_person_id_or_400(person_id)
    safe_handle = sanitize_socialblade_handle(handle)
    if not safe_handle:
        raise HTTPException(status_code=400, detail="Invalid handle")

    from trr_backend.repositories.socialblade_growth import get_growth_data

    data = get_growth_data(safe_person_id, safe_handle, platform="instagram")
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
    _admin: InternalAdminUser,
) -> dict[str, Any]:
    """Trigger a fresh SocialBlade scrape, preferring the local shared-browser path."""
    from trr_backend.socials.socialblade.service import (
        SocialBladeRefreshError,
        normalize_socialblade_source_scope,
        refresh_and_persist_socialblade,
        sanitize_socialblade_handle,
        scrape_socialblade_then_following,
    )

    safe_person_id = _normalize_person_id_or_400(person_id)
    safe_handle = sanitize_socialblade_handle(body.handle)
    if not safe_handle:
        raise HTTPException(status_code=400, detail="Invalid handle")
    try:
        source_scope = normalize_socialblade_source_scope(body.source_scope)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    try:
        return await run_in_threadpool(
            refresh_and_persist_socialblade,
            person_id=safe_person_id,
            handle=safe_handle,
            scraper=lambda normalized_handle: scrape_socialblade_then_following(
                _scrape_socialblade_person_page,
                normalized_handle,
                source="person_page",
                source_scope=source_scope,
                platform="instagram",
            ),
            source="person_page",
            force=body.force,
            platform="instagram",
        )
    except SocialBladeRefreshError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Unexpected SocialBlade person refresh failure",
            extra={"person_id": safe_person_id},
            exc_info=True,
        )
        raise HTTPException(status_code=502, detail=str(exc) or "SocialBlade refresh failed") from exc


@router.post("/socialblade/refresh-batch")
async def refresh_socialblade_data_batch(
    body: SocialBladeBatchRefreshRequest,
    _admin: InternalAdminUser,
) -> dict[str, Any]:
    """Refresh SocialBlade rows for multiple cast members."""
    from trr_backend.modal_dispatch import dispatch_socialblade_scrape
    from trr_backend.socials.socialblade.auth import socialblade_cookie_health_report
    from trr_backend.socials.socialblade.service import (
        normalize_socialblade_source_scope,
        queue_refresh_decision,
        sanitize_socialblade_handle,
        socialblade_auto_refresh_enabled,
    )

    source = body.source.strip().lower()
    if source not in {"cast_comparison", "season_run"}:
        raise HTTPException(status_code=400, detail="Invalid source")
    try:
        source_scope = normalize_socialblade_source_scope(body.source_scope)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    for item in body.items:
        _normalize_person_id_or_400(item.person_id)

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
    pending_dispatch: list[tuple[SocialBladeBatchRefreshItem, str]] = []

    for item in body.items:
        safe_person_id = _normalize_person_id_or_400(item.person_id)
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

        dedupe_key = (safe_person_id or "", safe_handle)
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
            person_id=safe_person_id,
            handle=safe_handle,
            force=body.force,
            platform="instagram",
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

        pending_dispatch.append((item.model_copy(update={"person_id": safe_person_id}), safe_handle))

    if pending_dispatch:
        validation_handle = pending_dispatch[0][1]
        cookie_health = await run_in_threadpool(
            socialblade_cookie_health_report,
            validate=True,
            validation_handle=validation_handle,
        )
        if not bool(cookie_health.get("healthy")):
            reason = str(cookie_health.get("reason") or "unknown").strip()
            raise HTTPException(
                status_code=409,
                detail={
                    "message": f"SocialBlade session preflight failed before batch dispatch: {reason}",
                    "code": "SOCIALBLADE_SESSION_PREFLIGHT_FAILED",
                    "reason": reason,
                    "retryable": True,
                    "cookieHealth": cookie_health,
                },
            )

    for item, safe_handle in pending_dispatch:
        dispatch_result = dispatch_socialblade_scrape(
            person_id=item.person_id,
            handle=safe_handle,
            source=source,
            force=body.force,
            platform="instagram",
            scrape_following=True,
            source_scope=source_scope,
        )
        if not dispatch_result.get("dispatched"):
            errors.append(
                {
                    "personId": item.person_id,
                    "handle": safe_handle,
                    "reason": dispatch_result.get("reason") or dispatch_result.get("error") or "dispatch_failed",
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
            "source_scope": source_scope,
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
