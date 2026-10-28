"""Strict, authenticated HTTP boundary for the Instagram ingestion service."""

from __future__ import annotations

import logging
import re
from typing import Annotated, cast
from uuid import UUID

from fastapi import APIRouter, Body, Depends, Header, HTTPException, Query, Request
from fastapi.exceptions import RequestValidationError, ResponseValidationError
from fastapi.routing import APIRoute
from pydantic import ValidationError

from api.auth import InternalAdminUser
from api.schemas.v2.instagram_ingestion import (
    InstagramDiagnosticsQueryV2,
    InstagramIngestionCapabilitiesV2,
    InstagramIngestionCommandV2,
    InstagramIngestionControlV2,
    InstagramIngestionDiagnosticsV2,
    InstagramIngestionLaunchV2,
    InstagramIngestionProblemResponseV2,
    InstagramIngestionProgressV2,
    InstagramRecentRunsQueryV2,
    InstagramRecentRunsV2,
    InstagramReplayResultV2,
    InstagramReplayV2,
    InstagramRetryTargetsV2,
)
from trr_backend.ingestion.api_service import IngestionAPI
from trr_backend.ingestion.domain import IngestionError
from trr_backend.problem import problem_response

logger = logging.getLogger(__name__)


class IngestionRoute(APIRoute):
    """Normalize dependency and validation failures within this API only."""

    def get_route_handler(self):
        handler = super().get_route_handler()

        async def typed_handler(request: Request):
            try:
                return await handler(request)
            except RequestValidationError:
                return problem_response(
                    request,
                    code="INGESTION_VALIDATION_ERROR",
                    status=422,
                    message="The ingestion request is invalid.",
                    retryable=False,
                )
            except HTTPException as exc:
                # Auth exceptions come from the shared verified-admin dependency.
                codes = {401: "AUTHENTICATION_REQUIRED", 403: "ADMIN_ACCESS_REQUIRED"}
                detail = exc.detail if isinstance(exc.detail, str) else "The ingestion request failed."
                return problem_response(
                    request,
                    code=codes.get(exc.status_code, "INGESTION_REQUEST_ERROR"),
                    status=exc.status_code,
                    message=detail,
                    retryable=False,
                    headers=exc.headers,
                )
            except IngestionError as exc:
                return problem_response(
                    request,
                    code=exc.code,
                    status=exc.status,
                    message="The ingestion operation could not be completed.",
                    retryable=False,
                )
            except LookupError:
                return problem_response(
                    request,
                    code="INGESTION_NOT_FOUND",
                    status=404,
                    message="The requested run or evidence was not found for this account.",
                    retryable=False,
                )
            except (ValidationError, ResponseValidationError):
                logger.error("Instagram ingestion service returned an invalid contract")
                return problem_response(
                    request,
                    code="INGESTION_INVALID_RESPONSE",
                    status=500,
                    message="The ingestion service returned an invalid response.",
                    retryable=False,
                )
            except Exception:
                logger.error("Instagram ingestion service request failed")
                return problem_response(
                    request,
                    code="INGESTION_SERVICE_ERROR",
                    status=500,
                    message="The ingestion service request failed.",
                    retryable=False,
                )

        return typed_handler


router = APIRouter(
    prefix="/admin/social/profiles/instagram/{handle}/ingestion",
    tags=["instagram-ingestion-v2"],
    route_class=IngestionRoute,
    responses={
        status: {"model": InstagramIngestionProblemResponseV2}
        for status in (400, 401, 403, 404, 409, 410, 422, 500, 503)
    },
)


def normalized_handle(handle: str) -> str:
    normalized = handle.strip().lstrip("@").lower()
    if not re.fullmatch(r"[a-z0-9._]{1,30}", normalized):
        raise HTTPException(422, "Invalid Instagram account handle.")
    return normalized


Handle = Annotated[str, Depends(normalized_handle)]


def idempotency_key(value: Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=200)]) -> str:
    key = value.strip()
    if not key:
        raise HTTPException(422, "Idempotency-Key must not be blank.")
    return key


IdempotencyKey = Annotated[str, Depends(idempotency_key)]


def configured_service(request: Request, handle: Handle) -> IngestionAPI:
    service = getattr(request.app.state, "instagram_ingestion", None)
    if service is None:
        raise HTTPException(503, "Instagram ingestion is not enabled.")
    return cast(IngestionAPI, service)


Service = Annotated[IngestionAPI, Depends(configured_service)]


def _initiator(user: dict) -> str:
    return str(user.get("admin_uid") or user["id"])


@router.get(
    "/capabilities", response_model=InstagramIngestionCapabilitiesV2, operation_id="getInstagramIngestionCapabilitiesV2"
)
def capabilities(request: Request, handle: Handle, _: InternalAdminUser):
    service = getattr(request.app.state, "instagram_ingestion", None)
    if service is None:
        return InstagramIngestionCapabilitiesV2(
            enabled=False, account=handle, disabled_reason="Instagram ingestion is not configured."
        )
    return service.capabilities(handle)


@router.post(
    "/runs", response_model=InstagramIngestionCommandV2, status_code=202, operation_id="launchInstagramIngestionRunV2"
)
def launch(
    payload: InstagramIngestionLaunchV2, handle: Handle, user: InternalAdminUser, key: IdempotencyKey, service: Service
):
    # The service resolves accepted keys before checking current new-run gates.
    return service.launch(handle, payload, idempotency_key=key, initiated_by=_initiator(user))


@router.get("/runs/recent", response_model=InstagramRecentRunsV2, operation_id="listRecentInstagramIngestionRunsV2")
def recent(
    handle: Handle,
    _: InternalAdminUser,
    service: Service,
    query: Annotated[InstagramRecentRunsQueryV2, Query()],
):
    return service.recent(handle, source_scope=query.source_scope, limit=query.limit)


@router.get("/runs/{run_id}", response_model=InstagramIngestionProgressV2, operation_id="getInstagramIngestionRunV2")
def progress(run_id: UUID, handle: Handle, _: InternalAdminUser, service: Service):
    return service.progress(handle, run_id)


@router.post(
    "/runs/{run_id}/cancel",
    response_model=InstagramIngestionCommandV2,
    status_code=202,
    operation_id="cancelInstagramIngestionRunV2",
)
def cancel(
    run_id: UUID,
    handle: Handle,
    user: InternalAdminUser,
    key: IdempotencyKey,
    service: Service,
    payload: Annotated[InstagramIngestionControlV2 | None, Body()] = None,
):
    return service.control(handle, run_id, action="cancel", idempotency_key=key, initiated_by=_initiator(user))


@router.post(
    "/runs/{run_id}/resume",
    response_model=InstagramIngestionCommandV2,
    status_code=202,
    operation_id="resumeInstagramIngestionRunV2",
)
def resume(
    run_id: UUID,
    handle: Handle,
    user: InternalAdminUser,
    key: IdempotencyKey,
    service: Service,
    payload: Annotated[InstagramIngestionControlV2 | None, Body()] = None,
):
    return service.control(handle, run_id, action="resume", idempotency_key=key, initiated_by=_initiator(user))


@router.post(
    "/runs/{run_id}/retry-targets",
    response_model=InstagramIngestionCommandV2,
    status_code=202,
    operation_id="retryInstagramIngestionTargetsV2",
)
def retry(
    payload: InstagramRetryTargetsV2,
    run_id: UUID,
    handle: Handle,
    user: InternalAdminUser,
    key: IdempotencyKey,
    service: Service,
):
    return service.retry(handle, run_id, payload, idempotency_key=key, initiated_by=_initiator(user))


@router.get(
    "/runs/{run_id}/diagnostics",
    response_model=InstagramIngestionDiagnosticsV2,
    operation_id="getInstagramIngestionDiagnosticsV2",
)
def diagnostics(
    run_id: UUID,
    handle: Handle,
    _: InternalAdminUser,
    service: Service,
    query: Annotated[InstagramDiagnosticsQueryV2, Query()],
):
    return service.diagnostics(handle, run_id, limit=query.limit)


@router.post(
    "/runs/{run_id}/replay", response_model=InstagramReplayResultV2, operation_id="replayInstagramIngestionEvidenceV2"
)
def replay(
    payload: InstagramReplayV2,
    run_id: UUID,
    handle: Handle,
    user: InternalAdminUser,
    key: IdempotencyKey,
    service: Service,
):
    if payload.apply:
        capability = InstagramIngestionCapabilitiesV2.model_validate(service.capabilities(handle))
        if not capability.replay_apply_enabled:
            raise HTTPException(503, "Instagram ingestion replay apply is not enabled.")
    return service.replay(handle, run_id, payload, idempotency_key=key, initiated_by=_initiator(user))
