from __future__ import annotations

import re
from typing import Any

from fastapi import APIRouter, Header, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field

from api.auth import InternalAdminUser
from trr_backend.repositories import recent_people as recent_people_repo

router = APIRouter(prefix="/admin/recent-people", tags=["admin-recent-people"])

_UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I)
_DEFAULT_LIMIT = 20


class RecentPersonViewRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    person_id: str = Field(alias="personId")
    show_id: str | None = Field(default=None, alias="showId")


def _parse_limit(raw: int) -> int:
    return min(max(int(raw), 1), 50)


def _user_uid(admin: dict[str, Any], explicit_uid: str | None) -> str:
    normalized = str(explicit_uid or "").strip()
    if normalized:
        return normalized
    return str(admin.get("email") or admin.get("id") or "admin")


@router.get("")
def list_recent_people(
    limit: int = Query(default=_DEFAULT_LIMIT, ge=1, le=50),
    x_trr_admin_user_uid: str | None = Header(default=None, alias="X-TRR-Admin-User-Uid"),
    admin: InternalAdminUser = None,
) -> dict[str, Any]:
    people, _query_count = recent_people_repo.list_recent_people(
        _user_uid(admin or {}, x_trr_admin_user_uid),
        limit=limit,
    )
    return {
        "people": people,
        "pagination": {
            "limit": _parse_limit(limit),
            "count": len(people),
        },
    }


@router.post("")
def record_recent_person(
    body: RecentPersonViewRequest,
    x_trr_admin_user_uid: str | None = Header(default=None, alias="X-TRR-Admin-User-Uid"),
    admin: InternalAdminUser = None,
) -> dict[str, bool]:
    person_id = str(body.person_id or "").strip()
    if not _UUID_RE.fullmatch(person_id):
        raise HTTPException(status_code=400, detail="personId must be a valid UUID")
    payload, _query_count = recent_people_repo.record_recent_person_view(
        firebase_uid=_user_uid(admin or {}, x_trr_admin_user_uid),
        person_id=person_id,
        show_context=str(body.show_id).strip() if body.show_id else None,
        cap=_DEFAULT_LIMIT,
    )
    return payload
