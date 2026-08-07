"""Admin endpoints for person external ID writes."""

from __future__ import annotations

import logging
from datetime import date
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from collections.abc import Mapping

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from api.auth import InternalAdminUser
from api.routers.admin_people_reads import invalidate_person_read_cache
from trr_backend.db.pg import (
    database_service_unavailable_detail,
    is_database_service_unavailable_error,
)
from trr_backend.problem import problem_http_exception
from trr_backend.repositories import person_external_ids as external_ids_repo

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/admin/people", tags=["admin-person-external-ids"])


class PersonExternalIdInput(BaseModel):
    source_id: str = Field(min_length=1)
    external_id: str = ""
    valid_from: date | None = None
    valid_to: date | None = None
    is_primary: bool | None = True


class SyncPersonExternalIdsRequest(BaseModel):
    external_ids: list[PersonExternalIdInput] = Field(default_factory=list)


class PersonExternalIdRecord(BaseModel):
    id: int | None = None
    source_id: str
    external_id: str
    is_primary: bool = True
    valid_from: str | None = None
    valid_to: str | None = None
    observed_at: str | None = None
    created_at: str | None = None
    updated_at: str | None = None


class SyncPersonExternalIdsResponse(BaseModel):
    external_ids: list[PersonExternalIdRecord]


def _database_unavailable_exception(error: Exception, request: Request) -> HTTPException:
    detail = database_service_unavailable_detail(error)
    return problem_http_exception(
        request,
        code=str(detail.get("code") or "DATABASE_SERVICE_UNAVAILABLE"),
        status=503,
        message=str(detail.get("message") or "Database service unavailable."),
        retryable=bool(detail.get("retryable", True)),
        extra={
            "reason": detail.get("reason"),
            "retry_after_ms": detail.get("retry_after_ms"),
        },
    )


def _to_input_records(payload: SyncPersonExternalIdsRequest) -> list[dict[str, Any]]:
    return [row.model_dump(mode="python") for row in payload.external_ids]


@router.put("/{person_id}/external-ids", response_model=SyncPersonExternalIdsResponse)
def sync_person_external_ids(
    request: Request,
    person_id: str,
    payload: SyncPersonExternalIdsRequest,
    _: InternalAdminUser = cast(InternalAdminUser, None),
) -> SyncPersonExternalIdsResponse:
    try:
        rows = external_ids_repo.sync_person_external_ids(
            person_id, cast("list[Mapping[str, Any]]", _to_input_records(payload))
        )
    except external_ids_repo.UnsupportedPersonExternalIdSourceError as error:
        raise HTTPException(status_code=400, detail=str(error)) from error
    except external_ids_repo.PersonExternalIdNotFoundError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error
    except external_ids_repo.PersonExternalIdConflictError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error
    except Exception as error:
        if is_database_service_unavailable_error(error):
            raise _database_unavailable_exception(error, request) from error
        logger.exception("[admin-person-external-ids] sync failed person_id=%s", person_id)
        raise HTTPException(status_code=500, detail="Failed to update person external IDs.") from error

    invalidate_person_read_cache(person_id=person_id)
    logger.info("[admin-person-external-ids] route=sync person_id=%s count=%s", person_id, len(rows))
    return SyncPersonExternalIdsResponse(external_ids=[PersonExternalIdRecord(**row) for row in rows])
