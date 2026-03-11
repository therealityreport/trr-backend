"""Helpers for dispatching selected long-running jobs to Modal."""

from __future__ import annotations

import logging
import os
from datetime import UTC, datetime
from typing import Any

from trr_backend.job_plane import execution_backend_canonical

logger = logging.getLogger(__name__)

_SUPPORTED_ADMIN_OPERATION_TYPES = frozenset(
    {
        "admin_asset_batch_jobs",
        "admin_scrape_import_images",
        "admin_show_links_discover",
        "admin_show_bravo_preview",
        "admin_show_refresh",
        "admin_show_refresh_photos",
        "admin_person_refresh_images",
        "admin_person_reprocess_images",
    }
)

_REMOTE_EXECUTION_MODE = "remote"
_REMOTE_EXECUTION_OWNER = "remote_worker"
_MODAL_EXECUTION_BACKEND = "modal"


def _env_flag(name: str, *, default: bool = False) -> bool:
    raw = str(os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def modal_dispatch_enabled() -> bool:
    return _env_flag("TRR_MODAL_ENABLED", default=False) and execution_backend_canonical() == _MODAL_EXECUTION_BACKEND


def modal_admin_function_name() -> str:
    return str(os.getenv("TRR_MODAL_ADMIN_OPERATION_FUNCTION") or "run_admin_operation").strip()


def modal_google_news_function_name() -> str:
    return str(os.getenv("TRR_MODAL_GOOGLE_NEWS_FUNCTION") or "run_google_news_sync").strip()


def modal_reddit_refresh_function_name() -> str:
    return str(os.getenv("TRR_MODAL_REDDIT_REFRESH_FUNCTION") or "run_reddit_refresh").strip()


def modal_social_job_function_name() -> str:
    return str(os.getenv("TRR_MODAL_SOCIAL_JOB_FUNCTION") or "run_social_job").strip()


def modal_social_recovery_function_name() -> str:
    return str(os.getenv("TRR_MODAL_SOCIAL_RECOVERY_FUNCTION") or "sweep_social_dispatch_queue").strip()


def modal_app_name() -> str:
    return str(os.getenv("TRR_MODAL_APP_NAME") or "trr-backend-jobs").strip()


def modal_execution_metadata() -> dict[str, str]:
    return {
        "execution_mode_canonical": _REMOTE_EXECUTION_MODE,
        "execution_owner": _REMOTE_EXECUTION_OWNER,
        "execution_backend_canonical": _MODAL_EXECUTION_BACKEND,
    }


def modal_dispatch_config() -> dict[str, str]:
    return {
        "app_name": modal_app_name(),
        "admin_function": modal_admin_function_name(),
        "google_news_function": modal_google_news_function_name(),
        "reddit_refresh_function": modal_reddit_refresh_function_name(),
        "social_job_function": modal_social_job_function_name(),
        "social_recovery_function": modal_social_recovery_function_name(),
    }


def modal_dispatch_ready(*, function_name: str) -> tuple[bool, str | None]:
    if execution_backend_canonical() != _MODAL_EXECUTION_BACKEND:
        return False, "remote_executor_not_modal"
    if not _env_flag("TRR_MODAL_ENABLED", default=False):
        return False, "modal_disabled"
    app_name = modal_app_name()
    normalized_function = str(function_name or "").strip()
    if not app_name:
        return False, "modal_app_name_missing"
    if not normalized_function:
        return False, "modal_function_name_missing"
    return True, None


def _dispatcher_worker_id(dispatcher_name: str) -> str:
    normalized = str(dispatcher_name or "dispatcher").strip().lower().replace(" ", "-")
    return f"modal:{normalized}-dispatcher"


def _record_dispatcher_heartbeat(
    *,
    dispatcher_name: str,
    status: str,
    metadata_updates: dict[str, Any] | None = None,
    supported_platforms: list[str] | None = None,
) -> None:
    try:
        from trr_backend.repositories.social_season_analytics import update_worker_heartbeat

        metadata = {
            "dispatcher_name": dispatcher_name,
            "execution_backend_canonical": _MODAL_EXECUTION_BACKEND,
            "execution_mode_canonical": _REMOTE_EXECUTION_MODE,
            **(metadata_updates or {}),
        }
        update_worker_heartbeat(
            _dispatcher_worker_id(dispatcher_name),
            stage="any",
            status=status,
            metadata=metadata,
            supported_platforms=supported_platforms,
        )
    except Exception:  # noqa: BLE001
        logger.debug("Failed to update Modal dispatcher heartbeat: dispatcher=%s", dispatcher_name, exc_info=True)


def supports_admin_operation(operation_type: str) -> bool:
    normalized = str(operation_type or "").strip().lower()
    return normalized in _SUPPORTED_ADMIN_OPERATION_TYPES


def _spawn_named_modal_function(
    *,
    function_name: str,
    log_label: str,
    kwargs: dict[str, str],
    dispatcher_name: str,
    supported_platforms: list[str] | None = None,
) -> dict[str, Any]:
    ready, reason = modal_dispatch_ready(function_name=function_name)
    if not ready:
        _record_dispatcher_heartbeat(
            dispatcher_name=dispatcher_name,
            status="idle",
            metadata_updates={
                "dispatch_enabled": False,
                "last_dispatch_error": reason,
                "last_dispatch_error_at": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
            },
            supported_platforms=supported_platforms,
        )
        return {
            "dispatched": False,
            "reason": reason,
            "call_id": None,
        }

    app_name = modal_app_name()
    normalized_function = str(function_name or "").strip()
    try:
        import modal

        fn = modal.Function.from_name(app_name, normalized_function)
        call = fn.spawn(**kwargs)
        call_id = str(getattr(call, "object_id", "") or "").strip() or None
        _record_dispatcher_heartbeat(
            dispatcher_name=dispatcher_name,
            status="idle",
            metadata_updates={
                "dispatch_enabled": True,
                "last_dispatch_success_at": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
                "last_dispatch_error": None,
                "last_dispatch_label": log_label,
                "last_dispatch_function": normalized_function,
                "last_dispatch_call_id": call_id,
                "last_dispatch_kwargs": kwargs,
            },
            supported_platforms=supported_platforms,
        )
        logger.info(
            "Dispatched %s job to Modal: app=%s function=%s kwargs=%s call_id=%s",
            log_label,
            app_name,
            normalized_function,
            kwargs,
            call_id,
        )
        return {
            "dispatched": True,
            "reason": None,
            "call_id": call_id,
        }
    except Exception as exc:  # noqa: BLE001
        _record_dispatcher_heartbeat(
            dispatcher_name=dispatcher_name,
            status="idle",
            metadata_updates={
                "dispatch_enabled": True,
                "last_dispatch_error": str(exc),
                "last_dispatch_error_at": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
                "last_dispatch_label": log_label,
                "last_dispatch_function": normalized_function,
                "last_dispatch_kwargs": kwargs,
            },
            supported_platforms=supported_platforms,
        )
        logger.exception(
            "Failed to dispatch %s job to Modal: app=%s function=%s kwargs=%s",
            log_label,
            app_name,
            normalized_function,
            kwargs,
        )
        return {
            "dispatched": False,
            "reason": str(exc),
            "call_id": None,
        }


def dispatch_admin_operation(*, operation_id: str, operation_type: str) -> bool:
    normalized_type = str(operation_type or "").strip().lower()
    if not modal_dispatch_enabled():
        return False
    if not supports_admin_operation(normalized_type):
        return False
    result = _spawn_named_modal_function(
        function_name=modal_admin_function_name(),
        log_label="admin operation",
        kwargs={"operation_id": operation_id, "operation_type": normalized_type},
        dispatcher_name="admin",
    )
    return bool(result.get("dispatched"))


def dispatch_google_news_sync(*, job_id: str) -> bool:
    result = _spawn_named_modal_function(
        function_name=modal_google_news_function_name(),
        log_label="google-news sync",
        kwargs={"job_id": job_id},
        dispatcher_name="google-news",
    )
    return bool(result.get("dispatched"))


def dispatch_reddit_refresh(*, run_id: str) -> bool:
    result = _spawn_named_modal_function(
        function_name=modal_reddit_refresh_function_name(),
        log_label="reddit refresh",
        kwargs={"run_id": run_id},
        dispatcher_name="reddit",
    )
    return bool(result.get("dispatched"))


def dispatch_social_job(*, job_id: str) -> dict[str, Any]:
    return _spawn_named_modal_function(
        function_name=modal_social_job_function_name(),
        log_label="social ingest",
        kwargs={"job_id": job_id},
        dispatcher_name="social",
        supported_platforms=["instagram", "tiktok", "twitter", "youtube", "facebook", "threads"],
    )
