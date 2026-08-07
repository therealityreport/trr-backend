"""Internal-admin operations for fenced show-season media watches."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, cast
from urllib.parse import urlsplit, urlunsplit
from uuid import UUID, uuid4

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from api.auth import InternalAdminUser
from trr_backend import modal_dispatch
from trr_backend.db import pg
from trr_backend.repositories import media_watchers

router = APIRouter(prefix="/admin/media-watchers", tags=["admin-media-watchers"])


class CreateMediaWatchRequest(BaseModel):
    show_id: UUID
    season_id: UUID
    target_season_number: int = Field(ge=1, le=999)
    nbcumv_show_id: str = Field(min_length=1, max_length=200)
    bravo_show_uuid: UUID
    source_season_rules: dict[str, Any] = Field(default_factory=dict)
    qualification_rules_version: str = Field(min_length=1, max_length=100)
    r2_prefix: str = Field(min_length=1, max_length=500)
    desktop_folder_name: str = Field(min_length=1, max_length=200)
    sources: list[str] = Field(default_factory=lambda: ["nbcumv", "bravo"])
    resource_types: list[str] = Field(default_factory=lambda: ["image"])
    poll_interval_seconds: int = Field(default=60, ge=1, le=86_400)
    overlap_seconds: int = Field(default=300, ge=0, le=3_600)
    backfill_mode: bool = False


def _watch_or_404(watch_id: str) -> dict[str, Any]:
    watch = media_watchers.get_watch(watch_id)
    if not watch:
        raise HTTPException(status_code=404, detail="Media watch not found")
    return watch


def _claim_watch_for_admin_run(*, watch_id: str, lease_owner: str) -> dict[str, Any] | None:
    """Claim one selected active watch without creating a second scheduler."""
    return pg.fetch_one(
        """
        UPDATE core.show_season_media_watches
        SET lease_owner = %s,
            lease_expires_at = now() + interval '180 seconds',
            lease_heartbeat_at = now(),
            lease_fence = lease_fence + 1,
            last_checked_at = now()
        WHERE id = %s::uuid
          AND status = 'active'
          AND (lease_expires_at IS NULL OR lease_expires_at <= now())
        RETURNING id::text AS id, lease_fence
        """,
        [lease_owner, watch_id],
    )


def _launch_claimed_watch(*, watch_id: str, backfill: bool) -> dict[str, Any]:
    """Claim exactly one watch, then use the sole Modal worker entrypoint."""
    owner = f"admin-media-watch:{uuid4().hex}"
    claim = _claim_watch_for_admin_run(watch_id=watch_id, lease_owner=owner)
    if not claim:
        watch = _watch_or_404(watch_id)
        raise HTTPException(
            status_code=409,
            detail={
                "code": "WATCH_NOT_CLAIMABLE",
                "status": watch.get("status"),
                "message": "Watch is paused, disabled, or already leased by another worker.",
            },
        )
    watch = _watch_or_404(watch_id)
    lease_fence = int(claim["lease_fence"])
    dispatch = modal_dispatch.dispatch_show_season_media_watch_worker(
        watch=watch,
        lease_owner=owner,
        lease_fence=lease_fence,
        backfill=backfill,
    )
    if not dispatch.get("dispatched"):
        raise HTTPException(
            status_code=503,
            detail="Media watch worker dispatch failed; lease will expire safely.",
        )
    return {
        "watch_id": watch_id,
        "lease_owner": owner,
        "lease_fence": lease_fence,
        "backfill": backfill,
        "call_id": dispatch.get("call_id"),
        "status": "dispatched",
    }


def _recent_runs(watch_id: str, *, limit: int) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in pg.fetch_all(
            """
            SELECT id::text AS id, watch_id::text AS watch_id, lease_fence,
                   status, summary, continuation, error_detail, started_at,
                   completed_at, created_at, updated_at
            FROM core.show_season_media_watch_runs
            WHERE watch_id = %s::uuid
            ORDER BY created_at DESC
            LIMIT %s
            """,
            [watch_id, limit],
        )
    ]


def _manifest_revisions(watch_id: str) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in pg.fetch_all(
            """
            SELECT id::text AS revision_id, watch_id::text AS watch_id,
                   media_asset_id::text AS media_asset_id, sha256,
                   bytes AS size_bytes, content_type, hosted_bucket, hosted_key,
                   hosted_url, fetched_at, created_at
            FROM core.media_source_revisions
            WHERE watch_id = %s::uuid
              AND acquisition_state = 'db_committed'
              AND hosted_key IS NOT NULL
              AND hosted_url IS NOT NULL
            ORDER BY created_at ASC, id ASC
            """,
            [watch_id],
        )
    ]


def _credential_free_url(value: Any) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    parsed = urlsplit(raw)
    if parsed.scheme != "https" or not parsed.netloc or parsed.username or parsed.password:
        return None
    # Signed query strings can be credentials; the local sync client obtains a
    # fresh short-lived transport URL separately if the bucket is private.
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))


@router.post("")
def create_media_watch(payload: CreateMediaWatchRequest, user: InternalAdminUser) -> dict[str, Any]:
    try:
        watch = media_watchers.create_watch(
            show_id=str(payload.show_id),
            season_id=str(payload.season_id),
            target_season_number=payload.target_season_number,
            nbcumv_show_id=payload.nbcumv_show_id,
            bravo_show_uuid=str(payload.bravo_show_uuid),
            source_season_rules=payload.source_season_rules,
            qualification_rules_version=payload.qualification_rules_version,
            r2_prefix=payload.r2_prefix,
            desktop_folder_name=payload.desktop_folder_name,
            sources=payload.sources,
            resource_types=payload.resource_types,
            poll_interval_seconds=payload.poll_interval_seconds,
            overlap_seconds=payload.overlap_seconds,
            backfill_mode=payload.backfill_mode,
            created_by=str(user.get("id") or "internal-admin"),
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail="Failed to create media watch") from exc
    return {"watch": watch}


@router.get("/{watch_id}/status")
def media_watch_status(watch_id: UUID, _: InternalAdminUser) -> dict[str, Any]:
    watch = _watch_or_404(str(watch_id))
    return {"watch": watch, "recent_run": next(iter(_recent_runs(str(watch_id), limit=1)), None)}


@router.get("/{watch_id}/runs")
def media_watch_recent_runs(
    watch_id: UUID,
    limit: int = Query(default=20, ge=1, le=200),
    _: InternalAdminUser = cast(InternalAdminUser, None),
) -> dict[str, Any]:
    _watch_or_404(str(watch_id))
    return {"watch_id": str(watch_id), "runs": _recent_runs(str(watch_id), limit=limit)}


@router.get("/{watch_id}/runs/recent")
def media_watch_recent_run(watch_id: UUID, _: InternalAdminUser) -> dict[str, Any]:
    _watch_or_404(str(watch_id))
    return {"watch_id": str(watch_id), "run": next(iter(_recent_runs(str(watch_id), limit=1)), None)}


@router.post("/{watch_id}/pause")
def pause_media_watch(watch_id: UUID, _: InternalAdminUser) -> dict[str, Any]:
    watch = media_watchers.pause_watch(watch_id=str(watch_id))
    if not watch:
        raise HTTPException(status_code=404, detail="Media watch not found")
    return {"watch": watch}


@router.post("/{watch_id}/resume")
def resume_media_watch(watch_id: UUID, _: InternalAdminUser) -> dict[str, Any]:
    watch = media_watchers.resume_watch(watch_id=str(watch_id))
    if not watch:
        raise HTTPException(status_code=409, detail="Media watch is not paused or disabled")
    return {"watch": watch}


@router.post("/{watch_id}/run-now")
def run_media_watch_now(watch_id: UUID, _: InternalAdminUser) -> dict[str, Any]:
    return _launch_claimed_watch(watch_id=str(watch_id), backfill=False)


@router.post("/{watch_id}/baseline")
def run_media_watch_baseline(watch_id: UUID, _: InternalAdminUser) -> dict[str, Any]:
    watch = _watch_or_404(str(watch_id))
    if watch.get("baseline_completed_at"):
        raise HTTPException(
            status_code=409,
            detail="Baseline already completed; create a new watch for a new baseline generation",
        )
    return _launch_claimed_watch(watch_id=str(watch_id), backfill=False)


@router.post("/{watch_id}/backfill")
def backfill_media_watch(watch_id: UUID, _: InternalAdminUser) -> dict[str, Any]:
    _watch_or_404(str(watch_id))
    return _launch_claimed_watch(watch_id=str(watch_id), backfill=True)


@router.get("/{watch_id}/manifest")
def media_watch_revision_manifest(
    watch_id: UUID,
    expires_in_seconds: int = Query(default=900, ge=60, le=3600),
    _: InternalAdminUser = cast(InternalAdminUser, None),
) -> dict[str, Any]:
    _watch_or_404(str(watch_id))
    generated_at = datetime.now(UTC)
    revisions: list[dict[str, Any]] = []
    for revision in _manifest_revisions(str(watch_id)):
        hosted_url = _credential_free_url(revision.get("hosted_url"))
        if not hosted_url:
            continue
        revisions.append(
            {
                "revision_id": revision["revision_id"],
                "media_asset_id": revision["media_asset_id"],
                "sha256": revision["sha256"],
                "size_bytes": revision["size_bytes"],
                "content_type": revision["content_type"],
                "hosted_bucket": revision["hosted_bucket"],
                "hosted_key": revision["hosted_key"],
                "hosted_url": hosted_url,
                "fetched_at": revision.get("fetched_at"),
            }
        )
    return {
        "watch_id": str(watch_id),
        "generated_at": generated_at.isoformat().replace("+00:00", "Z"),
        "expires_at": (generated_at + timedelta(seconds=expires_in_seconds)).isoformat().replace("+00:00", "Z"),
        "revisions": revisions,
    }
