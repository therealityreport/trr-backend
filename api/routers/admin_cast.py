from __future__ import annotations

from typing import Any
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from api.auth import AdminUser, InternalAdminUser
from api.deps import SupabaseAdminClient, get_list_result
from trr_backend.db import pg

router = APIRouter(prefix="/admin", tags=["admin"])


class PeopleOverridePatch(BaseModel):
    full_name_override: str | None = None
    instagram_handle: str | None = None
    tiktok_handle: str | None = None
    twitter_handle: str | None = None
    youtube_handle: str | None = None
    external_ids_override: dict[str, Any] | None = None
    notes: str | None = None


class ShowCastOverridePatch(BaseModel):
    credit_category: str | None = "Self"
    friend_of: bool | None = None
    role_override: str | None = None
    billing_order_override: int | None = None
    notes_override: str | None = None
    tags_override: list[str] | None = None


class CastSummaryBatchRequest(BaseModel):
    show_ids: list[str] = Field(..., min_length=1, max_length=200)


class CastSummaryMember(BaseModel):
    person_id: str
    full_name: str | None = None
    photo_url: str | None = None


class CastSummaryShow(BaseModel):
    show_id: str
    cast_members: list[CastSummaryMember]


class CastSummaryBatchResponse(BaseModel):
    shows: list[CastSummaryShow]


def _group_cast_summary_rows(
    show_ids: list[str],
    rows: list[dict[str, Any]],
) -> CastSummaryBatchResponse:
    ordered_show_ids = list(dict.fromkeys(show_ids))
    cast_members_by_show_id: dict[str, list[CastSummaryMember]] = {
        show_id: [] for show_id in ordered_show_ids
    }

    for row in rows:
        show_id = str(row.get("show_id") or "").strip()
        person_id = str(row.get("person_id") or "").strip()
        if not show_id or not person_id or show_id not in cast_members_by_show_id:
            continue

        full_name_value = row.get("full_name")
        full_name = full_name_value.strip() if isinstance(full_name_value, str) else None
        photo_url_value = row.get("photo_url")
        photo_url = photo_url_value.strip() if isinstance(photo_url_value, str) else None
        cast_members_by_show_id[show_id].append(
            CastSummaryMember(person_id=person_id, full_name=full_name, photo_url=photo_url)
        )

    return CastSummaryBatchResponse(
        shows=[
            CastSummaryShow(
                show_id=show_id,
                cast_members=cast_members_by_show_id.get(show_id, []),
            )
            for show_id in ordered_show_ids
        ]
    )


@router.post("/shows/cast-summary", response_model=CastSummaryBatchResponse)
def get_admin_cast_summary(
    payload: CastSummaryBatchRequest,
    _: InternalAdminUser,
) -> CastSummaryBatchResponse:
    show_ids = [show_id.strip() for show_id in payload.show_ids if show_id.strip()]
    if not show_ids:
        raise HTTPException(status_code=400, detail="show_ids must include at least one id")

    rows = pg.fetch_all(
        """
        SELECT DISTINCT
            sc.show_id::text AS show_id,
            sc.person_id::text AS person_id,
            COALESCE(po.full_name_override, p.full_name) AS full_name,
            COALESCE(photo.thumb_url, photo.display_url, photo.hosted_url, photo.url) AS photo_url
        FROM core.v_show_cast sc
        JOIN core.people p ON p.id = sc.person_id
        LEFT JOIN core.people_overrides po ON po.person_id = sc.person_id
        LEFT JOIN LATERAL (
          SELECT
            cp.thumb_url,
            cp.display_url,
            cp.hosted_url,
            cp.url
          FROM core.v_cast_photos AS cp
          WHERE cp.person_id = sc.person_id
          ORDER BY
            CASE
              WHEN lower(COALESCE(cp.context_section, '')) = 'bravo_profile' THEN 0
              WHEN lower(COALESCE(cp.context_section, '')) IN (
                'official season announcement',
                'official_season_announcement'
              ) THEN 1
              ELSE 2
            END,
            cp.gallery_index ASC NULLS LAST
          LIMIT 1
        ) AS photo ON true
        WHERE sc.show_id::text = ANY(%s::text[])
        ORDER BY sc.show_id::text, COALESCE(po.full_name_override, p.full_name)
        """,
        [show_ids],
    )
    return _group_cast_summary_rows(show_ids, rows)


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
            NULL::text AS notes_source,

            p.full_name,
            p.external_ids,

            po.full_name_override,
            po.instagram_handle AS instagram_override,
            po.tiktok_handle AS tiktok_override,
            po.twitter_handle AS twitter_override,
            po.youtube_handle AS youtube_override,
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
            COALESCE(sco.notes_override, NULL::text) AS notes_effective,
            COALESCE(sco.friend_of, false) AS friend_of,
            sco.tags_override,

            ct.tmdb_id,
            ct.imdb_id AS tmdb_imdb_id,
            ct.profile_path,

            cf.source_url AS fandom_url,
            cf.full_name AS fandom_full_name,
            cf.summary AS fandom_summary,

            {season_select}

        FROM core.v_show_cast sc
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
