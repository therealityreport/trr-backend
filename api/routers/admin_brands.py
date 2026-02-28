"""Admin endpoints for brands shows/franchise workflows."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from api.auth import AdminUser
from trr_backend.repositories import brands_franchises

router = APIRouter(prefix="/admin/brands", tags=["admin-brands"])


class UpdateFranchiseRuleRequest(BaseModel):
    name: str | None = None
    primary_url: str | None = None
    review_allpages_url: str | None = None
    match_terms: list[str] | None = None
    aliases: list[str] | None = None
    community_domains: list[str] | None = None
    include_allpages_scan: bool | None = None
    source_rank: int | None = Field(default=None, ge=0)
    network_terms: list[str] | None = None
    is_active: bool | None = None


class ApplyFranchiseRuleRequest(BaseModel):
    missing_only: bool = True
    dry_run: bool = True


def _is_service_unavailable_error(error: RuntimeError) -> bool:
    message = str(error).strip().lower()
    return (
        "table is unavailable" in message
        or "run backend migrations" in message
        or "schema" in message and "missing" in message
        or "is not migrated" in message
        or "connection pool exhausted" in message
        or "database pool initialization failed" in message
    )


def _to_http_exception(error: Exception) -> HTTPException:
    if isinstance(error, KeyError):
        detail = str(error).strip().strip('"').strip("'") or "Not found"
        return HTTPException(status_code=404, detail=detail)
    if isinstance(error, ValueError):
        return HTTPException(status_code=400, detail=str(error))
    if isinstance(error, RuntimeError) and _is_service_unavailable_error(error):
        return HTTPException(status_code=503, detail=str(error))
    return HTTPException(status_code=500, detail=str(error) or "Internal server error")


@router.get("/shows-franchises")
def get_shows_franchises(
    q: str = Query(default=""),
    limit: int = Query(default=300, ge=1, le=1000),
    _: AdminUser = None,
) -> dict[str, Any]:
    try:
        return brands_franchises.list_shows_franchises(q=q, limit=limit)
    except Exception as error:  # noqa: BLE001
        raise _to_http_exception(error) from error


@router.get("/franchise-rules")
def get_franchise_rules(_: AdminUser = None) -> dict[str, Any]:
    try:
        return brands_franchises.list_franchise_rules()
    except Exception as error:  # noqa: BLE001
        raise _to_http_exception(error) from error


@router.put("/franchise-rules/{franchise_key}")
def put_franchise_rule(
    franchise_key: str,
    payload: UpdateFranchiseRuleRequest,
    user: AdminUser = None,
) -> dict[str, Any]:
    actor = str((user or {}).get("id") or "admin")
    try:
        return brands_franchises.update_franchise_rule(
            franchise_key=franchise_key,
            payload=payload.model_dump(exclude_none=True),
            actor=actor,
        )
    except Exception as error:  # noqa: BLE001
        raise _to_http_exception(error) from error


@router.post("/franchise-rules/{franchise_key}/apply")
def post_apply_franchise_rule(
    franchise_key: str,
    payload: ApplyFranchiseRuleRequest,
    user: AdminUser = None,
) -> dict[str, Any]:
    actor = str((user or {}).get("id") or "admin")
    try:
        return brands_franchises.apply_franchise_rule(
            franchise_key=franchise_key,
            missing_only=payload.missing_only,
            dry_run=payload.dry_run,
            actor=actor,
        )
    except Exception as error:  # noqa: BLE001
        raise _to_http_exception(error) from error
