"""Strict API v2 admin endpoints for migrated show and person writes."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, cast
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request
from pydantic import ValidationError

from api.auth import InternalAdminUser
from api.schemas.v2.admin_show_person_writes import (
    AdminEffectivePersonSocialHandlesRequestV2,
    AdminEffectivePersonSocialHandlesResponseV2,
    AdminPersonCanonicalProfileSourceOrderRequestV2,
    AdminPersonCanonicalProfileSourceOrderResponseV2,
    AdminShowPersonWriteProblemResponseV2,
    AdminShowUpdateRequestV2,
    AdminShowWriteResponseV2,
)
from trr_backend.db.pg import database_service_unavailable_detail, is_database_service_unavailable_error
from trr_backend.problem import problem_http_exception
from trr_backend.repositories import admin_show_person_writes as repository

if TYPE_CHECKING:
    from api.schemas.v2.admin_show_person_writes import AdminEffectivePersonSocialHandlesV2

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/admin", tags=["admin-show-person-writes-v2"])

_PROBLEM_RESPONSES: dict[int | str, dict[str, Any]] = {
    400: {"model": AdminShowPersonWriteProblemResponseV2, "description": "The write request is invalid."},
    404: {"model": AdminShowPersonWriteProblemResponseV2, "description": "The requested resource was not found."},
    500: {"model": AdminShowPersonWriteProblemResponseV2, "description": "The write request failed."},
    503: {"model": AdminShowPersonWriteProblemResponseV2, "description": "The database is unavailable."},
}


def _problem(request: Request, *, code: str, status: int, message: str, retryable: bool = False) -> HTTPException:
    return problem_http_exception(
        request,
        code=code,
        status=status,
        message=message,
        retryable=retryable,
    )


def _database_problem(error: Exception, request: Request) -> HTTPException:
    detail = database_service_unavailable_detail(error)
    return problem_http_exception(
        request,
        code=str(detail.get("code") or "DATABASE_SERVICE_UNAVAILABLE"),
        status=503,
        message=str(detail.get("message") or "Database service unavailable."),
        retryable=bool(detail.get("retryable", True)),
        extra={"reason": detail.get("reason"), "retry_after_ms": detail.get("retry_after_ms")},
    )


def _unexpected_problem(error: Exception, request: Request, *, operation: str) -> HTTPException:
    if isinstance(error, HTTPException):
        return error
    if is_database_service_unavailable_error(error):
        return _database_problem(error, request)
    logger.exception("[admin-show-person-writes-v2] %s failed", operation)
    return _problem(
        request,
        code="ADMIN_SHOW_PERSON_WRITE_FAILED",
        status=500,
        message="The admin write request could not be completed.",
    )


async def _json_body(request: Request, *, operation: str) -> dict[str, Any]:
    try:
        payload = await request.json()
    except Exception as error:
        raise _problem(
            request,
            code="INVALID_JSON_BODY",
            status=400,
            message="The request body must be valid JSON.",
        ) from error
    if not isinstance(payload, dict):
        raise _problem(
            request,
            code="INVALID_REQUEST_BODY",
            status=400,
            message=f"{operation} requires a JSON object body.",
        )
    return payload


async def _validated_body(model: type[Any], request: Request, *, operation: str) -> Any:
    payload = await _json_body(request, operation=operation)
    try:
        return model.model_validate(payload)
    except ValidationError as error:
        raise _problem(
            request,
            code="INVALID_REQUEST_BODY",
            status=400,
            message=f"{operation} request body is invalid.",
        ) from error


def _path_uuid(request: Request, field_name: str) -> str:
    try:
        return str(UUID(str(request.path_params.get(field_name) or "").strip()))
    except (TypeError, ValueError, AttributeError) as error:
        raise _problem(
            request,
            code=f"INVALID_{field_name.upper()}",
            status=400,
            message=f"{field_name} must be a valid UUID.",
        ) from error


@router.patch(
    "/shows/{show_id}",
    response_model=AdminShowWriteResponseV2,
    operation_id="updateAdminShowV2",
    summary="Patch an admin core show",
    responses=_PROBLEM_RESPONSES,
)
async def patch_show(request: Request, _: InternalAdminUser) -> AdminShowWriteResponseV2:
    show_id = _path_uuid(request, "show_id")
    body = await _validated_body(AdminShowUpdateRequestV2, request, operation="Show update")
    try:
        show, _query_count = repository.update_show(show_id, body.model_dump(exclude_unset=True))
        if show is None:
            raise _problem(request, code="SHOW_NOT_FOUND", status=404, message="Show not found.")
        return AdminShowWriteResponseV2(show=show)
    except Exception as error:
        raise _unexpected_problem(error, request, operation="patch-show") from error


@router.patch(
    "/people/{person_id}/canonical-profile-source-order",
    response_model=AdminPersonCanonicalProfileSourceOrderResponseV2,
    operation_id="updateAdminPersonCanonicalProfileSourceOrderV2",
    summary="Set the canonical profile source order for a core person",
    responses=_PROBLEM_RESPONSES,
)
async def patch_person_canonical_profile_source_order(
    request: Request,
    _: InternalAdminUser,
) -> AdminPersonCanonicalProfileSourceOrderResponseV2:
    person_id = _path_uuid(request, "person_id")
    body = await _validated_body(
        AdminPersonCanonicalProfileSourceOrderRequestV2,
        request,
        operation="Canonical profile source order update",
    )
    try:
        person, _query_count = repository.update_person_canonical_profile_source_order(person_id, body.source_order)
        if person is None:
            raise _problem(request, code="PERSON_NOT_FOUND", status=404, message="Person not found.")
        return AdminPersonCanonicalProfileSourceOrderResponseV2(person=person)
    except ValueError as error:
        raise _problem(request, code="INVALID_SOURCE_ORDER", status=400, message=str(error)) from error
    except Exception as error:
        raise _unexpected_problem(error, request, operation="patch-person-canonical-profile-source-order") from error


@router.post(
    "/people/effective-social-handles",
    response_model=AdminEffectivePersonSocialHandlesResponseV2,
    operation_id="listAdminEffectivePersonSocialHandlesV2",
    summary="Batch read effective social handles for core people",
    responses=_PROBLEM_RESPONSES,
)
async def list_effective_person_social_handles(
    request: Request,
    _: InternalAdminUser,
) -> AdminEffectivePersonSocialHandlesResponseV2:
    body = await _validated_body(
        AdminEffectivePersonSocialHandlesRequestV2,
        request,
        operation="Effective person social handles",
    )
    try:
        handles, _query_count = repository.list_effective_person_social_handles(
            [str(person_id) for person_id in body.person_ids]
        )
        return AdminEffectivePersonSocialHandlesResponseV2(
            handles=cast("list[AdminEffectivePersonSocialHandlesV2]", handles)
        )
    except Exception as error:
        raise _unexpected_problem(error, request, operation="list-effective-person-social-handles") from error
