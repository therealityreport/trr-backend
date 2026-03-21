"""
Internal Screenalytics ingest endpoints (service-to-service).
"""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, Query

from api.screenalytics_auth import require_screenalytics_service_token
from trr_backend.db import pg
from trr_backend.db.admin import create_supabase_admin_client
from trr_backend.repositories.tagging_references import build_owner_facebank_initial_reference_profile

router = APIRouter(prefix="/screenalytics", tags=["screenalytics"])


def _select_initial_facebank_photos(
    *,
    person_id: UUID,
    limit: int,
    seed_only: bool,
    show_id: UUID | None,
    show_name: str | None,
) -> list[dict]:
    db = create_supabase_admin_client()
    profile = build_owner_facebank_initial_reference_profile(
        db,
        str(person_id),
        show_id=str(show_id) if show_id else None,
        show_name=show_name,
        max_refs=limit,
        seed_only=seed_only,
    )
    rows: list[dict] = []
    for item in profile.get("used") or []:
        rows.append(
            {
                "served_url": item.get("served_url"),
                "source_url": item.get("source_url"),
                "hosted_url": item.get("hosted_url"),
                "hosted_key": item.get("hosted_key"),
                "media_asset_id": item.get("media_asset_id"),
                "is_primary": bool(item.get("is_primary")),
                "width": item.get("width"),
                "height": item.get("height"),
                "kind": item.get("kind") or "gallery",
                "source": item.get("source"),
                "selection_bucket": item.get("selection_bucket"),
                "selection_reasons": item.get("selection_reasons") or [],
                "facebank_seed": bool(item.get("facebank_seed")),
                "rank": item.get("rank"),
            }
        )
    return rows


@router.get("/episodes/{episode_id}/cast")
def get_episode_cast(
    episode_id: UUID,
    credit_category: str | None = Query(default=None),
    limit: int = Query(default=200, le=500),
    offset: int = Query(default=0, ge=0),
    _: None = Depends(require_screenalytics_service_token),
) -> list[dict]:
    sql = (
        "SELECT * FROM core.v_episode_cast "
        "WHERE episode_id = %s "
        + ("AND credit_category = %s " if credit_category else "")
        + "ORDER BY billing_order NULLS LAST "
        "LIMIT %s OFFSET %s"
    )
    params: list[object] = [str(episode_id)]
    if credit_category:
        params.append(credit_category)
    params.extend([limit, offset])
    return pg.fetch_all(sql, params)


@router.get("/seasons/{season_id}/cast")
def get_season_cast(
    season_id: UUID,
    limit: int = Query(default=200, le=500),
    offset: int = Query(default=0, ge=0),
    _: None = Depends(require_screenalytics_service_token),
) -> list[dict]:
    sql = "SELECT * FROM core.v_season_cast WHERE season_id = %s ORDER BY episodes_in_season DESC LIMIT %s OFFSET %s"
    return pg.fetch_all(sql, [str(season_id), limit, offset])


@router.get("/people/{person_id}/photos")
def get_person_photos(
    person_id: UUID,
    limit: int = Query(default=200, le=500),
    offset: int = Query(default=0, ge=0),
    seed_only: bool = Query(default=False),
    selection_profile: str | None = Query(default=None),
    show_id: UUID | None = Query(default=None),
    show_name: str | None = Query(default=None),
    _: None = Depends(require_screenalytics_service_token),
) -> list[dict]:
    if selection_profile == "facebank_initial":
        if offset != 0:
            return []
        return _select_initial_facebank_photos(
            person_id=person_id,
            limit=limit,
            seed_only=seed_only,
            show_id=show_id,
            show_name=show_name,
        )

    sql = (
        "SELECT served_url, hosted_key, is_primary, width, height, kind "
        "FROM core.v_person_images "
        "WHERE person_id = %s "
        + ("AND facebank_seed = true " if seed_only else "")
        + "ORDER BY is_primary DESC NULLS LAST, position ASC NULLS LAST "
        + "LIMIT %s OFFSET %s"
    )
    return pg.fetch_all(sql, [str(person_id), limit, offset])
