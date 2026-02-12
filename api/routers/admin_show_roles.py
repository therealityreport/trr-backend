from __future__ import annotations

import re
from typing import Any
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from api.auth import AdminUser
from api.deps import SupabaseAdminClient, get_list_result
from trr_backend.db import pg

router = APIRouter(prefix="/admin/shows", tags=["admin-show-roles"])


class RoleCreateRequest(BaseModel):
    name: str = Field(min_length=1, max_length=80)
    sort_order: int = 0


class RolePatchRequest(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=80)
    is_active: bool | None = None
    sort_order: int | None = None


class CastRoleAssignRequest(BaseModel):
    season_number: int | None = Field(default=None, ge=0, le=200)
    role_ids: list[UUID] = Field(default_factory=list)
    source: str = "manual"
    confidence: float | None = Field(default=None, ge=0.0, le=1.0)


def _normalize_role_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower().strip()).strip("_")


def _show_exists(show_id: str) -> bool:
    row = pg.fetch_one("SELECT id FROM core.shows WHERE id = %s", [show_id])
    return bool(row)


@router.get("/{show_id}/roles")
def list_show_roles(
    show_id: UUID,
    _: AdminUser,
    include_inactive: bool = Query(default=False),
) -> list[dict[str, Any]]:
    show_id_str = str(show_id)
    if not _show_exists(show_id_str):
        raise HTTPException(status_code=404, detail="Show not found")

    params: list[Any] = [show_id_str]
    filter_sql = ""
    if not include_inactive:
        filter_sql = "AND is_active = true"

    return pg.fetch_all(
        f"""
        SELECT *
        FROM core.show_role_catalog
        WHERE show_id = %s
        {filter_sql}
        ORDER BY sort_order ASC, name ASC
        """,
        params,
    )


@router.post("/{show_id}/roles")
def create_show_role(
    show_id: UUID,
    payload: RoleCreateRequest,
    db: SupabaseAdminClient,
    admin: AdminUser,
) -> dict[str, Any]:
    show_id_str = str(show_id)
    if not _show_exists(show_id_str):
        raise HTTPException(status_code=404, detail="Show not found")

    name = payload.name.strip()
    normalized = _normalize_role_name(name)
    if not normalized:
        raise HTTPException(status_code=400, detail="Role name is invalid")

    actor = str(admin.get("email") or admin.get("id") or "admin")
    row = {
        "show_id": show_id_str,
        "name": name,
        "normalized_name": normalized,
        "sort_order": payload.sort_order,
        "is_active": True,
        "created_by": actor,
        "updated_by": actor,
    }
    response = (
        db.schema("core")
        .table("show_role_catalog")
        .upsert(row, on_conflict="show_id,normalized_name")
        .execute()
    )
    rows = get_list_result(response, "upserting show role")
    return rows[0] if rows else row


@router.patch("/{show_id}/roles/{role_id}")
def patch_show_role(
    show_id: UUID,
    role_id: UUID,
    payload: RolePatchRequest,
    db: SupabaseAdminClient,
    admin: AdminUser,
) -> dict[str, Any]:
    updates = payload.model_dump(exclude_unset=True)
    if not updates:
        raise HTTPException(status_code=400, detail="No fields provided")

    if "name" in updates and updates["name"] is not None:
        updates["name"] = updates["name"].strip()
        updates["normalized_name"] = _normalize_role_name(updates["name"])

    updates["updated_by"] = str(admin.get("email") or admin.get("id") or "admin")

    response = (
        db.schema("core")
        .table("show_role_catalog")
        .update(updates)
        .eq("id", str(role_id))
        .eq("show_id", str(show_id))
        .execute()
    )
    rows = get_list_result(response, "updating show role")
    if not rows:
        raise HTTPException(status_code=404, detail="Role not found")
    return rows[0]


@router.post("/{show_id}/cast/{person_id}/roles")
def replace_cast_roles(
    show_id: UUID,
    person_id: UUID,
    payload: CastRoleAssignRequest,
    db: SupabaseAdminClient,
    admin: AdminUser,
) -> dict[str, Any]:
    show_id_str = str(show_id)
    person_id_str = str(person_id)
    season_number = int(payload.season_number or 0)
    actor = str(admin.get("email") or admin.get("id") or "admin")

    if not _show_exists(show_id_str):
        raise HTTPException(status_code=404, detail="Show not found")

    delete_resp = (
        db.schema("core")
        .table("show_cast_role_assignments")
        .delete()
        .eq("show_id", show_id_str)
        .eq("person_id", person_id_str)
        .eq("season_number", season_number)
        .execute()
    )
    get_list_result(delete_resp, "deleting existing role assignments")

    if not payload.role_ids:
        return {
            "show_id": show_id_str,
            "person_id": person_id_str,
            "season_number": season_number,
            "assigned": 0,
            "roles": [],
        }

    role_rows = pg.fetch_all(
        """
        SELECT id::text AS id
        FROM core.show_role_catalog
        WHERE show_id = %s
          AND id = ANY(%s::uuid[])
          AND is_active = true
        """,
        [show_id_str, [str(role_id) for role_id in payload.role_ids]],
    )
    valid_role_ids = [row["id"] for row in role_rows]

    rows = [
        {
            "show_id": show_id_str,
            "person_id": person_id_str,
            "season_number": season_number,
            "role_id": role_id,
            "source": payload.source,
            "confidence": payload.confidence,
            "created_by": actor,
            "updated_by": actor,
        }
        for role_id in valid_role_ids
    ]
    if rows:
        insert_resp = db.schema("core").table("show_cast_role_assignments").insert(rows).execute()
        get_list_result(insert_resp, "inserting role assignments")

    assigned_roles = pg.fetch_all(
        """
        SELECT sra.*, rc.name AS role_name
        FROM core.show_cast_role_assignments sra
        JOIN core.show_role_catalog rc ON rc.id = sra.role_id
        WHERE sra.show_id = %s
          AND sra.person_id = %s
          AND sra.season_number = %s
        ORDER BY rc.sort_order ASC, rc.name ASC
        """,
        [show_id_str, person_id_str, season_number],
    )

    return {
        "show_id": show_id_str,
        "person_id": person_id_str,
        "season_number": season_number,
        "assigned": len(assigned_roles),
        "roles": assigned_roles,
    }


@router.get("/{show_id}/cast-role-members")
def list_cast_with_roles(
    show_id: UUID,
    _: AdminUser,
    sort_by: str = Query(default="episodes"),
    order: str = Query(default="desc"),
    seasons: str | None = Query(default=None),
    roles: str | None = Query(default=None),
    has_image: bool | None = Query(default=None),
    archive_mode: str = Query(default="all"),
) -> list[dict[str, Any]]:
    show_id_str = str(show_id)
    if not _show_exists(show_id_str):
        raise HTTPException(status_code=404, detail="Show not found")
    archive_mode = archive_mode.lower().strip()
    if archive_mode not in {"all", "exclude", "only"}:
        raise HTTPException(status_code=400, detail="archive_mode must be one of: all, exclude, only")

    season_numbers = [int(value) for value in (seasons or "").split(",") if value.strip().isdigit()]
    role_names = [value.strip().lower() for value in (roles or "").split(",") if value.strip()]

    rows = pg.fetch_all(
        """
        SELECT
          c.show_id,
          c.person_id,
          c.person_name,
          c.total_episodes,
          c.archive_episodes,
          c.seasons_appeared,
          c.season_numbers,
          c.latest_season,
          c.roles,
          cp.display_url AS photo_url
        FROM core.v_show_cast_roles_enriched c
        LEFT JOIN LATERAL (
          SELECT display_url
          FROM core.v_cast_photos p
          WHERE p.person_id = c.person_id
          ORDER BY p.gallery_index ASC NULLS LAST
          LIMIT 1
        ) cp ON true
        WHERE c.show_id = %s
        """,
        [show_id_str],
    )

    filtered: list[dict[str, Any]] = []
    for row in rows:
        row_roles = [str(value).strip().lower() for value in (row.get("roles") or []) if isinstance(value, str)]
        row_seasons = [
            int(value)
            for value in (row.get("season_numbers") or [])
            if isinstance(value, int) and value > 0
        ]
        if season_numbers and not any(value in row_seasons for value in season_numbers):
            continue
        if role_names and not any(role in row_roles for role in role_names):
            continue
        if has_image is True and not row.get("photo_url"):
            continue
        if has_image is False and row.get("photo_url"):
            continue
        archive_episodes = int(row.get("archive_episodes") or 0)
        regular_episodes = int(row.get("total_episodes") or 0)
        if archive_mode == "exclude" and archive_episodes > 0:
            continue
        if archive_mode == "only" and not (archive_episodes > 0 and regular_episodes <= 0):
            continue
        filtered.append(row)

    reverse = order.lower() != "asc"
    if sort_by == "name":
        filtered.sort(key=lambda item: str(item.get("person_name") or "").lower(), reverse=reverse)
    elif sort_by == "season":
        filtered.sort(key=lambda item: int(item.get("latest_season") or 0), reverse=reverse)
    else:
        filtered.sort(key=lambda item: int(item.get("total_episodes") or 0), reverse=reverse)

    return filtered
