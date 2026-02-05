"""
Internal Screenalytics ingest endpoints (service-to-service).
"""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, Query

from api.screenalytics_auth import require_screenalytics_service_token
from trr_backend.db import pg

router = APIRouter(prefix="/screenalytics", tags=["screenalytics"])


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
    _: None = Depends(require_screenalytics_service_token),
) -> list[dict]:
    sql = (
        "SELECT served_url, hosted_key, is_primary, width, height, kind "
        "FROM core.v_person_images "
        "WHERE person_id = %s "
        + ("AND facebank_seed = true " if seed_only else "")
        "ORDER BY is_primary DESC NULLS LAST, position ASC NULLS LAST "
        "LIMIT %s OFFSET %s"
    )
    return pg.fetch_all(sql, [str(person_id), limit, offset])
