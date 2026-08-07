# ruff: noqa: F401, F403, F405, I001, UP037
"""Profile-scoped catalog operation routes."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from fastapi import APIRouter

from ._shared import *
from .catalog_reads import *

if TYPE_CHECKING:
    from collections.abc import Callable

    # These underscore-prefixed helpers (and the ``InstagramCommentsLoadStrategy``
    # alias) are re-exported at runtime by the star imports above via each
    # module's dynamic ``__all__``; the imports below only make them visible to
    # static type checkers.
    from ._shared import (
        _clear_account_profile_caches,
        _lookup_error_to_not_found,
        _require_instagram_auth_refresh_confirmation,
        _resolve_account_profile_catalog_freshness,
        _to_social_read_http_exception,
        _value_error_to_bad_request,
    )

router = APIRouter()


def _cancel_catalog_run_in_background(
    *,
    platform: str,
    account_handle: str,
    run_id: str,
    cancelled_by: str | None,
) -> None:
    from trr_backend.socials.control_plane.shared_accounts import cancel_social_account_catalog_run

    if TYPE_CHECKING:
        # ``reconcile_cancelled_shared_run`` is injected into the launch
        # module's globals at runtime by its provider bridge, so static
        # checkers cannot see it as an import symbol.
        reconcile_cancelled_shared_run = cast("Callable[[str], dict[str, Any]]", None)
    else:
        from trr_backend.socials.pipelines.account_catalog.launch import reconcile_cancelled_shared_run

    normalized_platform = str(platform or "").strip().lower()
    normalized_account = str(account_handle or "").strip().lower()
    normalized_run_id = str(run_id or "").strip()
    if not normalized_run_id:
        return

    def _runner() -> None:
        logger.info(
            "[catalog-cancel] finalize_start platform=%s account=%s run_id=%s",
            normalized_platform,
            normalized_account,
            normalized_run_id,
        )
        try:
            cancel_social_account_catalog_run(
                platform=normalized_platform,
                account_handle=normalized_account,
                run_id=normalized_run_id,
                cancelled_by=cancelled_by,
                reconcile_summary=False,
            )
        finally:
            try:
                reconcile_cancelled_shared_run(normalized_run_id)
            except Exception:  # noqa: BLE001
                logger.exception(
                    "[catalog-cancel] reconcile_failed platform=%s account=%s run_id=%s",
                    normalized_platform,
                    normalized_account,
                    normalized_run_id,
                )
            _clear_account_profile_caches()

    Thread(
        target=_runner,
        name=f"catalog-cancel:{normalized_platform}:{normalized_account[:24]}",
        daemon=True,
    ).start()


class CatalogReviewResolveRequest(BaseModel):
    resolution_action: Literal["assign_show", "mark_non_show"]
    show_id: UUID | None = None

    @model_validator(mode="after")
    def validate_resolution(self) -> CatalogReviewResolveRequest:
        if self.resolution_action == "assign_show" and self.show_id is None:
            raise ValueError("show_id is required when assigning a show hashtag")
        return self


class SocialAccountCompletionRetryTargetsRequest(BaseModel):
    run_id: UUID | None = None
    retry_targets: dict[str, list[dict[str, Any]]] | list[dict[str, Any]] = Field(default_factory=dict)
    source_scope: str = Field(default="network", max_length=64)
    # Mirrors ``_shared.InstagramCommentsLoadStrategy``; spelled inline because
    # the alias reaches this module through a wildcard import, which static
    # checkers cannot treat as a type alias.
    comments_load_strategy: Literal[
        "instagram_comments_endpoint_cursor",
        "cursor_api",
        "single_session_load_all",
        "public_relay",
    ] = Field(default="public_relay")
    comments_worker_count: int | None = Field(default=None, ge=1, le=24)
    dispatch_immediately: bool = Field(default=True)
    dry_run: bool = Field(default=False)


def _to_optional_request_header_value(value: str | None) -> str | None:
    if not value:
        return None
    normalized = str(value).strip()
    return normalized or None


def _start_social_catalog_gap_analysis_operation(
    *,
    platform: str,
    account_handle: str,
    request: Request,
) -> dict[str, Any]:
    from trr_backend.pipeline.admin_operations import ensure_operation_execution
    from trr_backend.repositories import admin_operations as admin_operations_repo
    from trr_backend.socials.control_plane import (
        SOCIAL_CATALOG_GAP_ANALYSIS_OPERATION_TYPE,
        build_social_account_catalog_gap_analysis_operation_producer,
    )

    request_payload = {
        "platform": platform,
        "account_handle": account_handle,
    }
    producer = build_social_account_catalog_gap_analysis_operation_producer(request_payload=request_payload)
    request_id = _to_optional_request_header_value(request.headers.get("x-trr-request-id"))
    client_session_id = _to_optional_request_header_value(request.headers.get("x-trr-tab-session-id"))
    client_workflow_id = _to_optional_request_header_value(request.headers.get("x-trr-flow-key"))

    operation, attached = admin_operations_repo.create_or_attach_operation(
        operation_type=SOCIAL_CATALOG_GAP_ANALYSIS_OPERATION_TYPE,
        request_payload=request_payload,
        initiated_by=None,
        request_id=request_id,
        client_session_id=client_session_id,
        client_workflow_id=client_workflow_id,
        allow_attach=True,
    )
    operation_id = str(operation.get("id") or "").strip()
    if not operation_id:
        raise RuntimeError("Failed to create social catalog gap-analysis operation")
    if not attached:
        ensure_operation_execution(operation_id, producer=producer, request_id=request_id)

    refreshed = admin_operations_repo.get_operation(operation_id) or operation
    refreshed["attached"] = attached
    return refreshed


@router.post("/profiles/{platform}/{account_handle}/catalog/retry-targets")
async def post_social_account_catalog_retry_targets_route(
    platform: str,
    account_handle: str,
    payload: SocialAccountCompletionRetryTargetsRequest,
    user: InternalAdminUser,
) -> dict[str, Any]:
    from trr_backend.socials.control_plane.runtime import (
        SocialIngestConflictError,
        SocialIngestValidationError,
    )
    from trr_backend.socials.pipelines.comments.instagram import enqueue_instagram_completion_retry_targets

    if platform.strip().lower() != "instagram":
        raise HTTPException(
            status_code=400,
            detail={
                "code": "SOCIAL_ACCOUNT_COMPLETION_RETRIES_UNSUPPORTED_PLATFORM",
                "message": "Completion retry targets are Instagram-only.",
            },
        )
    try:
        return await run_in_threadpool(
            enqueue_instagram_completion_retry_targets,
            account_handle=account_handle,
            retry_targets=payload.retry_targets,
            run_id=str(payload.run_id) if payload.run_id else None,
            source_scope=payload.source_scope,
            comments_load_strategy=payload.comments_load_strategy,
            comments_worker_count=payload.comments_worker_count,
            dispatch_immediately=payload.dispatch_immediately,
            dry_run=payload.dry_run,
            initiated_by=(user or {}).get("email"),
        )
    except SocialIngestConflictError as exc:
        raise HTTPException(
            status_code=409,
            detail={"code": exc.code, "message": str(exc), **jsonable_encoder(exc.detail)},
        ) from exc
    except SocialIngestValidationError as exc:
        raise HTTPException(
            status_code=400,
            detail={"code": exc.code, "message": str(exc), **jsonable_encoder(getattr(exc, "detail", {}) or {})},
        ) from exc


@router.post("/profiles/{platform}/{account_handle}/catalog/gap-analysis/run")
def post_social_account_catalog_gap_analysis_run_route(
    platform: str,
    account_handle: str,
    request: Request,
    _: InternalAdminUser = cast(Any, None),
) -> dict[str, Any]:
    try:
        operation = _start_social_catalog_gap_analysis_operation(
            platform=platform,
            account_handle=account_handle,
            request=request,
        )
        payload = social_profile_reads.get_catalog_gap_analysis_status(
            platform=platform,
            account_handle=account_handle,
        )
        payload["attached"] = bool(operation.get("attached"))
        payload["operation_id"] = str(operation.get("id") or payload.get("operation_id") or "").strip() or None
        return payload
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Failed to start social account catalog gap analysis: platform=%s account=%s",
            platform,
            account_handle,
        )
        raise _to_social_read_http_exception(exc) from exc


@router.post("/profiles/{platform}/{account_handle}/catalog/runs/{run_id}/cancel")
def post_social_account_catalog_run_cancel_route(
    platform: str,
    account_handle: str,
    run_id: UUID,
    background_tasks: BackgroundTasks,
    user: InternalAdminUser,
) -> dict[str, Any]:
    if TYPE_CHECKING:
        # ``request_cancel_social_account_catalog_run`` is injected into the
        # launch module's globals at runtime by its provider bridge, so static
        # checkers cannot see it as an import symbol.
        request_cancel_social_account_catalog_run = cast("Callable[..., dict[str, Any]]", None)
    else:
        from trr_backend.socials.pipelines.account_catalog.launch import request_cancel_social_account_catalog_run

    try:
        result = request_cancel_social_account_catalog_run(
            platform=platform,
            account_handle=account_handle,
            run_id=str(run_id),
            cancelled_by=(user or {}).get("email"),
        )
        background_tasks.add_task(
            _cancel_catalog_run_in_background,
            platform=platform,
            account_handle=account_handle,
            run_id=str(run_id),
            cancelled_by=(user or {}).get("email"),
        )
        return result
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc


@router.post("/profiles/{platform}/{account_handle}/catalog/runs/{run_id}/dismiss")
def post_social_account_catalog_run_dismiss_route(
    platform: str,
    account_handle: str,
    run_id: UUID,
    user: InternalAdminUser,
) -> dict[str, Any]:
    from trr_backend.socials.control_plane.shared_accounts import dismiss_social_account_catalog_run

    try:
        result = dismiss_social_account_catalog_run(
            platform=platform,
            account_handle=account_handle,
            run_id=str(run_id),
            dismissed_by=(user or {}).get("email"),
        )
        _clear_account_profile_caches()
        return result
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc


@router.post("/profiles/{platform}/{account_handle}/catalog/review-queue/{item_id}/resolve")
def post_social_account_catalog_review_queue_resolve_route(
    platform: str,
    account_handle: str,
    item_id: UUID,
    payload: CatalogReviewResolveRequest,
    user: InternalAdminUser,
) -> dict[str, Any]:
    from trr_backend.socials.control_plane.shared_accounts import resolve_social_account_catalog_review_queue_item

    del platform, account_handle
    try:
        response = resolve_social_account_catalog_review_queue_item(
            item_id=str(item_id),
            resolution_action=payload.resolution_action,
            show_id=str(payload.show_id) if payload.show_id else None,
            updated_by=(user or {}).get("email"),
        )
        _clear_account_profile_caches()
        return response
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc


@router.post("/profiles/{platform}/{account_handle}/catalog/freshness")
def post_social_account_catalog_freshness_route(
    platform: str,
    account_handle: str,
    force: bool = Query(default=False, description="Bypass cached freshness and run the deep live-profile probe"),
    statement_timeout_ms: int = Query(default=3000, ge=1000, le=30000),
    _: InternalAdminUser = cast(Any, None),
) -> dict[str, Any]:
    try:
        return _resolve_account_profile_catalog_freshness(
            platform=platform,
            account_handle=account_handle,
            force=bool(force),
            statement_timeout_ms=statement_timeout_ms,
            loader=social_profile_reads.get_catalog_freshness,
        )
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Failed to fetch social account catalog freshness: platform=%s account=%s",
            platform,
            account_handle,
        )
        raise _to_social_read_http_exception(exc) from exc


@router.post("/profiles/{platform}/{account_handle}/catalog/runs/{run_id}/manual-auth")
@router.post("/profiles/{platform}/{account_handle}/catalog/runs/{run_id}/repair-auth")
async def post_social_account_catalog_run_repair_auth_route(
    platform: str,
    account_handle: str,
    run_id: UUID,
    payload: CatalogRepairAuthRequest,
    background_tasks: BackgroundTasks,
    user: InternalAdminUser,
) -> dict[str, Any]:
    from trr_backend.socials.control_plane.runtime import SocialIngestValidationError
    from trr_backend.socials.control_plane import (
        execute_social_account_catalog_run_auth_repair,
        request_social_account_catalog_run_auth_repair,
    )

    try:
        _require_instagram_auth_refresh_confirmation(platform, payload.operator_confirmation)
        result = request_social_account_catalog_run_auth_repair(
            platform=platform,
            account_handle=account_handle,
            run_id=str(run_id),
            initiated_by=(user or {}).get("email"),
        )
        background_tasks.add_task(
            execute_social_account_catalog_run_auth_repair,
            platform=platform,
            account_handle=account_handle,
            run_id=str(run_id),
            initiated_by=(user or {}).get("email"),
            allow_cookie_refresh=bool(payload.allow_cookie_refresh),
        )
        _clear_account_profile_caches()
        return result
    except SocialIngestValidationError as exc:
        raise HTTPException(status_code=400, detail={"code": exc.code, "message": str(exc)}) from exc
    except ValueError as exc:
        raise _value_error_to_bad_request(exc) from exc
    except LookupError as exc:
        raise _lookup_error_to_not_found(exc) from exc


__all__ = [name for name in globals() if not name.startswith("__")]
