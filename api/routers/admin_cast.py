from __future__ import annotations

from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from api.auth import AdminUser
from api.deps import SupabaseAdminClient, get_list_result
from trr_backend.db import pg

router = APIRouter(prefix="/admin", tags=["admin"])


class PeopleOverridePatch(BaseModel):
    full_name_override: str | None = None
    instagram_handle: str | None = None
    external_ids_override: dict[str, Any] | None = None
    notes: str | None = None


class ShowCastOverridePatch(BaseModel):
    credit_category: str | None = "Self"
    friend_of: bool | None = None
    role_override: str | None = None
    billing_order_override: int | None = None
    notes_override: str | None = None
    tags_override: list[str] | None = None


@router.get("/shows/{show_id}/cast")
def list_admin_cast(
    show_id: UUID,
    _: AdminUser,
    season_id: UUID | None = Query(default=None),
    limit: int = Query(default=200, le=500),
    offset: int = Query(default=0, ge=0),
) -> list[dict]:
    season_join = ""
    season_select = "NULL::int AS episodes_in_season"
    params: list[object] = []

    if season_id:
        season_join = (
            "LEFT JOIN core.v_season_cast vsc "
            "ON vsc.show_id = sc.show_id AND vsc.person_id = sc.person_id AND vsc.season_id = %s"
        )
        season_select = "vsc.episodes_in_season"
        params.append(str(season_id))

    params.append(str(show_id))
    params.extend([limit, offset])

    sql = f"""
        SELECT
            sc.show_id,
            sc.person_id,
            sc.credit_category,
            sc.role AS role_source,
            sc.billing_order AS billing_order_source,
            sc.notes AS notes_source,

            p.full_name,
            p.external_ids,

            po.full_name_override,
            po.instagram_handle AS instagram_override,
            po.external_ids_override,
            po.notes AS person_notes,

            COALESCE(po.full_name_override, p.full_name) AS display_name,
            COALESCE(
                NULLIF(po.instagram_handle, ''),
                ct.instagram_id,
                p.external_ids->>'instagram_id',
                p.external_ids->>'instagram'
            ) AS instagram_handle,

            COALESCE(sco.role_override, sc.role) AS role_effective,
            COALESCE(sco.billing_order_override, sc.billing_order) AS billing_order_effective,
            COALESCE(sco.notes_override, sc.notes) AS notes_effective,
            COALESCE(sco.friend_of, false) AS friend_of,
            sco.tags_override,

            ct.tmdb_id,
            ct.imdb_id AS tmdb_imdb_id,
            ct.profile_path,

            cf.source_url AS fandom_url,
            cf.full_name AS fandom_full_name,
            cf.summary AS fandom_summary,

            {season_select}

        FROM core.show_cast sc
        JOIN core.people p ON p.id = sc.person_id
        LEFT JOIN core.people_overrides po ON po.person_id = sc.person_id
        LEFT JOIN core.show_cast_overrides sco
          ON sco.show_id = sc.show_id
         AND sco.person_id = sc.person_id
         AND sco.credit_category = sc.credit_category
        LEFT JOIN core.cast_tmdb ct ON ct.person_id = sc.person_id
        LEFT JOIN core.cast_fandom cf ON cf.person_id = sc.person_id AND cf.source = 'fandom'
        {season_join}
        WHERE sc.show_id = %s
        ORDER BY COALESCE(sco.billing_order_override, sc.billing_order) NULLS LAST,
                 COALESCE(po.full_name_override, p.full_name)
        LIMIT %s OFFSET %s
    """
    return pg.fetch_all(sql, params)


@router.patch("/people/{person_id}/overrides")
def upsert_people_override(
    person_id: UUID,
    payload: PeopleOverridePatch,
    db: SupabaseAdminClient,
    _: AdminUser,
) -> dict:
    data = payload.model_dump(exclude_unset=True)
    if not data:
        raise HTTPException(status_code=400, detail="No fields provided")

    row = {"person_id": str(person_id), **data}
    response = db.schema("core").table("people_overrides").upsert(row, on_conflict="person_id").execute()
    rows = get_list_result(response, "upserting people_overrides")
    return rows[0] if rows else row


@router.patch("/shows/{show_id}/cast/{person_id}/overrides")
def upsert_show_cast_override(
    show_id: UUID,
    person_id: UUID,
    payload: ShowCastOverridePatch,
    db: SupabaseAdminClient,
    _: AdminUser,
) -> dict:
    data = payload.model_dump(exclude_unset=True)
    if not data:
        raise HTTPException(status_code=400, detail="No fields provided")

    credit_category = data.pop("credit_category", "Self") or "Self"
    row = {
        "show_id": str(show_id),
        "person_id": str(person_id),
        "credit_category": credit_category,
        **data,
    }
    response = (
        db.schema("core")
        .table("show_cast_overrides")
        .upsert(row, on_conflict="show_id,person_id,credit_category")
        .execute()
    )
    rows = get_list_result(response, "upserting show_cast_overrides")
    return rows[0] if rows else row
