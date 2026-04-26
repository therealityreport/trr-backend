"""Helpers for dispatching selected long-running jobs to Modal."""

from __future__ import annotations

import logging
import os
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from datetime import UTC, datetime
from threading import Lock
from time import monotonic
from typing import Any

from trr_backend.job_plane import execution_backend_canonical
from trr_backend.socials.platforms import SOCIAL_SUPPORTED_PLATFORMS

logger = logging.getLogger(__name__)

_SUPPORTED_ADMIN_OPERATION_TYPES = frozenset(
    {
        "admin_asset_batch_jobs",
        "admin_scrape_import_images",
        "admin_show_links_discover",
        "admin_show_bravo_preview",
        "admin_show_refresh",
        "admin_show_refresh_photos",
        "admin_bravotv_image_run",
        "admin_person_refresh_images",
        "admin_person_refresh_profile",
        "admin_person_reprocess_images",
        "admin_reddit_refresh_backfill",
    }
)

_REMOTE_EXECUTION_MODE = "remote"
_REMOTE_EXECUTION_OWNER = "remote_worker"
_MODAL_EXECUTION_BACKEND = "modal"
_REDDIT_RUNTIME_HEALTH_CACHE_TTL_SECONDS = max(
    5,
    int(str(os.getenv("TRR_MODAL_REDDIT_RUNTIME_HEALTH_CACHE_TTL_SECONDS") or "60").strip() or "60"),
)
_REDDIT_RUNTIME_HEALTH_CACHE: dict[str, Any] = {
    "expires_at": 0.0,
    "payload": None,
}
_REDDIT_RUNTIME_HEALTH_CACHE_LOCK = Lock()
_MODAL_PENDING_INVOCATION_STATUS = "pending"
_MODAL_RUNNING_INVOCATION_STATUS = "running"
_MODAL_COMPLETED_INVOCATION_STATUS = "completed"
_MODAL_FAILED_INVOCATION_STATUS = "failed"
_MODAL_UNKNOWN_INVOCATION_STATUS = "unknown"
_MODAL_FAILED_INPUT_STATUSES = frozenset({"failure", "init_failure", "terminated", "timeout"})


def _env_flag(name: str, *, default: bool = False) -> bool:
    raw = str(os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def _env_int(name: str, *, default: int, minimum: int = 1) -> int:
    raw = str(os.getenv(name) or "").strip()
    if not raw:
        return default
    try:
        return max(minimum, int(raw))
    except ValueError:
        logger.warning("[modal-dispatch] invalid integer env %s=%r; using %s", name, raw, default)
        return default


def _modal_sdk_worker_count() -> int:
    return _env_int("TRR_MODAL_SDK_CALL_WORKERS", default=2)


_MODAL_SDK_EXECUTOR = ThreadPoolExecutor(
    max_workers=_modal_sdk_worker_count(),
    thread_name_prefix="modal-sdk-call",
)


def _modal_sdk_timeout_seconds() -> float:
    raw = str(os.getenv("TRR_MODAL_SDK_CALL_TIMEOUT_SECONDS") or "15").strip()
    try:
        return max(0.1, float(raw))
    except ValueError:
        logger.warning("[modal-dispatch] invalid TRR_MODAL_SDK_CALL_TIMEOUT_SECONDS=%r; using 15", raw)
        return 15.0


def _run_modal_sdk_call(label: str, callback: Callable[[], Any]) -> Any:
    timeout_seconds = _modal_sdk_timeout_seconds()
    future = _MODAL_SDK_EXECUTOR.submit(callback)
    try:
        return future.result(timeout=timeout_seconds)
    except FutureTimeoutError as exc:
        raise TimeoutError(f"modal_{label}_timeout_after_{timeout_seconds:g}s") from exc


def modal_dispatch_enabled() -> bool:
    return _env_flag("TRR_MODAL_ENABLED", default=False) and execution_backend_canonical() == _MODAL_EXECUTION_BACKEND


def modal_admin_function_name() -> str:
    return str(os.getenv("TRR_MODAL_ADMIN_OPERATION_FUNCTION") or "run_admin_operation_v2").strip()


def modal_google_news_function_name() -> str:
    return str(os.getenv("TRR_MODAL_GOOGLE_NEWS_FUNCTION") or "run_google_news_sync").strip()


def modal_reddit_refresh_function_name() -> str:
    return str(os.getenv("TRR_MODAL_REDDIT_REFRESH_FUNCTION") or "run_reddit_refresh").strip()


def modal_reddit_runtime_probe_function_name() -> str:
    return str(os.getenv("TRR_MODAL_REDDIT_RUNTIME_PROBE_FUNCTION") or "probe_reddit_refresh_runtime").strip()


def modal_social_job_function_name() -> str:
    return str(os.getenv("TRR_MODAL_SOCIAL_JOB_FUNCTION") or "run_social_job").strip()


def modal_social_posts_job_function_name() -> str:
    return str(os.getenv("TRR_MODAL_SOCIAL_POSTS_JOB_FUNCTION") or "run_social_posts_job").strip()


def modal_social_media_job_function_name() -> str:
    return str(os.getenv("TRR_MODAL_SOCIAL_MEDIA_JOB_FUNCTION") or "run_social_media_job").strip()


def modal_social_comments_job_function_name() -> str:
    return str(os.getenv("TRR_MODAL_SOCIAL_COMMENTS_JOB_FUNCTION") or "run_social_comments_job").strip()


def modal_social_job_function_name_for_stage(stage: str | None) -> str:
    normalized_stage = str(stage or "").strip().lower()
    if normalized_stage in {"comments", "comments_scrapling"}:
        return modal_social_comments_job_function_name() or modal_social_job_function_name()
    if normalized_stage in {"media_mirror", "comment_media_mirror"}:
        return modal_social_media_job_function_name() or modal_social_job_function_name()
    if normalized_stage in {
        "",
        "any",
        "posts",
        "shared_account_discovery",
        "shared_account_posts",
        "post_classify",
        "season_materialize",
        "analytics_refresh",
    }:
        return modal_social_posts_job_function_name() or modal_social_job_function_name()
    return modal_social_job_function_name()


def modal_social_job_function_names() -> list[str]:
    names = [
        modal_social_job_function_name(),
        modal_social_posts_job_function_name(),
        modal_social_media_job_function_name(),
        modal_social_comments_job_function_name(),
    ]
    deduped: list[str] = []
    for name in names:
        normalized = str(name or "").strip()
        if normalized and normalized not in deduped:
            deduped.append(normalized)
    return deduped


def modal_social_recovery_function_name() -> str:
    return str(os.getenv("TRR_MODAL_SOCIAL_RECOVERY_FUNCTION") or "sweep_social_dispatch_queue").strip()


def modal_socialblade_function_name() -> str:
    return str(os.getenv("TRR_MODAL_SOCIALBLADE_FUNCTION") or "run_socialblade_scrape").strip()


def modal_app_name() -> str:
    return str(os.getenv("TRR_MODAL_APP_NAME") or "trr-backend-jobs").strip()


def modal_environment_name() -> str | None:
    value = str(os.getenv("MODAL_ENVIRONMENT") or "").strip()
    return value or None


def modal_execution_metadata() -> dict[str, str]:
    return {
        "execution_mode_canonical": _REMOTE_EXECUTION_MODE,
        "execution_owner": _REMOTE_EXECUTION_OWNER,
        "execution_backend_canonical": _MODAL_EXECUTION_BACKEND,
    }


def modal_dispatch_config() -> dict[str, Any]:
    return {
        "app_name": modal_app_name(),
        "modal_environment": modal_environment_name(),
        "admin_function": modal_admin_function_name(),
        "google_news_function": modal_google_news_function_name(),
        "reddit_refresh_function": modal_reddit_refresh_function_name(),
        "reddit_runtime_probe_function": modal_reddit_runtime_probe_function_name(),
        "social_job_function": modal_social_job_function_name(),
        "social_posts_job_function": modal_social_posts_job_function_name(),
        "social_media_job_function": modal_social_media_job_function_name(),
        "social_comments_job_function": modal_social_comments_job_function_name(),
        "social_job_function_names": modal_social_job_function_names(),
        "social_required_function_names": [
            modal_social_job_function_name(),
            modal_social_posts_job_function_name(),
            modal_social_media_job_function_name(),
            modal_social_comments_job_function_name(),
        ],
        "social_recovery_function": modal_social_recovery_function_name(),
        "socialblade_function": modal_socialblade_function_name(),
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


def _classify_modal_resolution_error(error: str) -> str:
    normalized = str(error or "").strip().lower()
    if not normalized:
        return "modal_resolution_failed"
    if "function" in normalized and "not found" in normalized:
        return "modal_function_not_found"
    if "app" in normalized and "not found" in normalized:
        return "modal_app_not_found"
    if "modal" in normalized and "unavailable" in normalized:
        return "modal_sdk_unavailable"
    if "timeout" in normalized:
        return "modal_sdk_timeout"
    return "modal_resolution_failed"


def resolve_modal_function(function_name: str) -> dict[str, Any]:
    normalized_function = str(function_name or "").strip()
    app_name = modal_app_name()
    environment_name = modal_environment_name()
    ready, reason = modal_dispatch_ready(function_name=normalized_function)
    payload: dict[str, Any] = {
        "resolved": False,
        "reason": reason,
        "error": None,
        "app_name": app_name,
        "function_name": normalized_function,
        "modal_environment": environment_name,
        "dispatch_config": modal_dispatch_config(),
        "execution_metadata": modal_execution_metadata(),
    }
    if not ready:
        return payload

    try:
        import modal

        def _resolve() -> Any:
            fn = modal.Function.from_name(app_name, normalized_function)
            hydrate = getattr(fn, "hydrate", None)
            if callable(hydrate):
                hydrate()
            return fn

        _run_modal_sdk_call("resolve_function", _resolve)
    except Exception as exc:  # noqa: BLE001
        payload["reason"] = _classify_modal_resolution_error(str(exc))
        payload["error"] = str(exc)
        return payload

    payload["resolved"] = True
    payload["reason"] = None
    return payload


def _utcnow_iso() -> str:
    return datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")


def _iter_modal_input_infos(nodes: list[Any] | None) -> Any:
    for node in nodes or []:
        yield node
        children = getattr(node, "children", None)
        if isinstance(children, list):
            yield from _iter_modal_input_infos(children)


def inspect_modal_function_call(function_call_id: str) -> dict[str, Any]:
    normalized_call_id = str(function_call_id or "").strip()
    checked_at = _utcnow_iso()
    payload: dict[str, Any] = {
        "function_call_id": normalized_call_id or None,
        "status": _MODAL_UNKNOWN_INVOCATION_STATUS,
        "raw_status": None,
        "task_id": None,
        "checked_at": checked_at,
        "reason": None,
        "nonterminal": False,
        "terminal": False,
    }
    if not normalized_call_id:
        payload["reason"] = "modal_call_id_missing"
        return payload

    if not modal_app_name():
        payload["reason"] = "modal_app_name_missing"
        return payload

    try:
        import modal
    except Exception as exc:  # noqa: BLE001
        payload["reason"] = "modal_sdk_unavailable"
        payload["error"] = str(exc)
        return payload

    try:
        function_call = modal.FunctionCall.from_id(normalized_call_id)
        call_graph = function_call.get_call_graph()
    except Exception as exc:  # noqa: BLE001
        payload["reason"] = "modal_call_inspection_failed"
        payload["error"] = str(exc)
        return payload

    target = None
    for input_info in _iter_modal_input_infos(call_graph if isinstance(call_graph, list) else []):
        if str(getattr(input_info, "function_call_id", "") or "").strip() == normalized_call_id:
            target = input_info
            break

    if target is None:
        payload["reason"] = "modal_call_not_found"
        return payload

    raw_status_value = getattr(getattr(target, "status", None), "name", getattr(target, "status", None))
    raw_status = str(raw_status_value or "").strip().lower()
    task_id = str(getattr(target, "task_id", "") or "").strip() or None
    normalized_status = _MODAL_UNKNOWN_INVOCATION_STATUS
    blocked_reason: str | None = None

    if raw_status == "pending":
        if task_id:
            normalized_status = _MODAL_RUNNING_INVOCATION_STATUS
        else:
            normalized_status = _MODAL_PENDING_INVOCATION_STATUS
            blocked_reason = "modal_capacity_pending"
    elif raw_status == "success":
        normalized_status = _MODAL_COMPLETED_INVOCATION_STATUS
    elif raw_status in _MODAL_FAILED_INPUT_STATUSES:
        normalized_status = _MODAL_FAILED_INVOCATION_STATUS
        blocked_reason = f"modal_{raw_status}"
    else:
        blocked_reason = "modal_call_status_unknown"

    payload.update(
        {
            "status": normalized_status,
            "raw_status": raw_status or None,
            "task_id": task_id,
            "reason": blocked_reason,
            "nonterminal": normalized_status in {_MODAL_PENDING_INVOCATION_STATUS, _MODAL_RUNNING_INVOCATION_STATUS},
            "terminal": normalized_status in {_MODAL_COMPLETED_INVOCATION_STATUS, _MODAL_FAILED_INVOCATION_STATUS},
        }
    )
    return payload


def _normalize_reddit_runtime_health_payload(raw: Any) -> dict[str, Any]:
    payload = raw if isinstance(raw, dict) else {}
    healthy = bool(payload.get("healthy"))
    reason = str(payload.get("reason") or ("ok" if healthy else "reddit_runtime_probe_failed")).strip()
    missing_env = payload.get("missing_env")
    warnings = payload.get("warnings")
    return {
        "healthy": healthy,
        "reason": reason,
        "missing_env": [str(item).strip() for item in missing_env if str(item).strip()]
        if isinstance(missing_env, list)
        else [],
        "warnings": [str(item).strip() for item in warnings if str(item).strip()] if isinstance(warnings, list) else [],
        "supports_oauth": bool(payload.get("supports_oauth")),
        "user_agent_configured": bool(payload.get("user_agent_configured")),
        "uses_default_user_agent": bool(payload.get("uses_default_user_agent")),
        "effective_user_agent": str(payload.get("effective_user_agent") or "").strip() or None,
        "execution_backend_canonical": _MODAL_EXECUTION_BACKEND,
    }


def get_modal_reddit_runtime_health(*, force_refresh: bool = False) -> dict[str, Any]:
    if not force_refresh:
        with _REDDIT_RUNTIME_HEALTH_CACHE_LOCK:
            cached_payload = _REDDIT_RUNTIME_HEALTH_CACHE.get("payload")
            cached_expires_at = float(_REDDIT_RUNTIME_HEALTH_CACHE.get("expires_at") or 0.0)
            if isinstance(cached_payload, dict) and monotonic() < cached_expires_at:
                return dict(cached_payload)

    probe_function_name = modal_reddit_runtime_probe_function_name()
    ready, reason = modal_dispatch_ready(function_name=probe_function_name)
    if not ready:
        payload = {
            "healthy": False,
            "reason": reason or "modal_dispatch_unavailable",
            "missing_env": [],
            "warnings": [],
            "supports_oauth": False,
            "user_agent_configured": False,
            "uses_default_user_agent": False,
            "effective_user_agent": None,
            "execution_backend_canonical": _MODAL_EXECUTION_BACKEND,
        }
    else:
        app_name = modal_app_name()
        try:
            import modal

            probe = modal.Function.from_name(app_name, probe_function_name)
            payload = _normalize_reddit_runtime_health_payload(probe.remote())
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "Failed to probe Reddit runtime health from Modal: app=%s function=%s",
                app_name,
                probe_function_name,
            )
            payload = {
                "healthy": False,
                "reason": "reddit_runtime_probe_failed",
                "error": str(exc),
                "missing_env": [],
                "warnings": [],
                "supports_oauth": False,
                "user_agent_configured": False,
                "uses_default_user_agent": False,
                "effective_user_agent": None,
                "execution_backend_canonical": _MODAL_EXECUTION_BACKEND,
            }

    with _REDDIT_RUNTIME_HEALTH_CACHE_LOCK:
        _REDDIT_RUNTIME_HEALTH_CACHE["payload"] = dict(payload)
        _REDDIT_RUNTIME_HEALTH_CACHE["expires_at"] = monotonic() + _REDDIT_RUNTIME_HEALTH_CACHE_TTL_SECONDS
    return dict(payload)


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
        from trr_backend.db import pg
        from trr_backend.socials.control_plane import _resolve_runtime_version_stamp, update_worker_heartbeat

        existing_metadata: dict[str, Any] = {}
        try:
            row = pg.fetch_one(
                """
                select metadata
                  from social.scrape_workers
                 where worker_id = %s
                """,
                [_dispatcher_worker_id(dispatcher_name)],
            )
            if isinstance((row or {}).get("metadata"), dict):
                existing_metadata = dict((row or {}).get("metadata") or {})
        except Exception:  # noqa: BLE001
            existing_metadata = {}

        metadata = {
            **existing_metadata,
            "dispatcher_name": dispatcher_name,
            "execution_backend_canonical": _MODAL_EXECUTION_BACKEND,
            "execution_mode_canonical": _REMOTE_EXECUTION_MODE,
            "runtime_version": dict(_resolve_runtime_version_stamp()),
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
    kwargs: dict[str, Any],
    dispatcher_name: str,
    supported_platforms: list[str] | None = None,
) -> dict[str, Any]:
    ready, reason = modal_dispatch_ready(function_name=function_name)
    app_name = modal_app_name()
    normalized_function = str(function_name or "").strip()
    environment_name = modal_environment_name()
    if not ready:
        _record_dispatcher_heartbeat(
            dispatcher_name=dispatcher_name,
            status="idle",
            metadata_updates={
                "dispatch_enabled": False,
                "last_dispatch_error": reason,
                "last_dispatch_error_code": reason,
                "last_dispatch_blocked_reason": reason,
                "last_dispatch_error_at": datetime.now(tz=UTC).isoformat().replace("+00:00", "Z"),
                "last_dispatch_app_name": app_name,
                "last_dispatch_function": normalized_function,
                "last_dispatch_modal_environment": environment_name,
            },
            supported_platforms=supported_platforms,
        )
        return {
            "dispatched": False,
            "reason": reason,
            "reason_code": reason,
            "call_id": None,
            "app_name": app_name,
            "function_name": normalized_function,
            "modal_environment": environment_name,
            "log_label": log_label,
            "execution_metadata": modal_execution_metadata(),
            "dispatch_config": modal_dispatch_config(),
        }

    try:
        import modal

        def _spawn() -> Any:
            fn = modal.Function.from_name(app_name, normalized_function)
            return fn.spawn(**kwargs)

        call = _run_modal_sdk_call("spawn_function", _spawn)
        call_id = str(getattr(call, "object_id", "") or "").strip() or None
        _record_dispatcher_heartbeat(
            dispatcher_name=dispatcher_name,
            status="idle",
            metadata_updates={
                "dispatch_enabled": True,
                "last_dispatch_success_at": _utcnow_iso(),
                "last_dispatch_error": None,
                "last_dispatch_error_code": None,
                "last_dispatch_blocked_reason": None,
                "last_dispatch_label": log_label,
                "last_dispatch_function": normalized_function,
                "last_dispatch_call_id": call_id,
                "last_dispatch_kwargs": kwargs,
                "last_dispatch_app_name": app_name,
                "last_dispatch_modal_environment": environment_name,
            },
            supported_platforms=supported_platforms,
        )
        logger.info(
            "Dispatched %s job to Modal: app=%s function=%s environment=%s kwargs=%s call_id=%s",
            log_label,
            app_name,
            normalized_function,
            environment_name or "default",
            kwargs,
            call_id,
        )
        return {
            "dispatched": True,
            "reason": None,
            "reason_code": None,
            "call_id": call_id,
            "app_name": app_name,
            "function_name": normalized_function,
            "modal_environment": environment_name,
            "log_label": log_label,
            "execution_metadata": modal_execution_metadata(),
            "dispatch_config": modal_dispatch_config(),
        }
    except Exception as exc:  # noqa: BLE001
        reason_code = _classify_modal_resolution_error(str(exc))
        _record_dispatcher_heartbeat(
            dispatcher_name=dispatcher_name,
            status="idle",
            metadata_updates={
                "dispatch_enabled": reason_code
                not in {
                    "modal_sdk_unavailable",
                    "modal_sdk_timeout",
                    "modal_app_not_found",
                    "modal_function_not_found",
                    "modal_resolution_failed",
                },
                "last_dispatch_error": str(exc),
                "last_dispatch_error_code": reason_code,
                "last_dispatch_blocked_reason": reason_code,
                "last_dispatch_error_at": _utcnow_iso(),
                "last_dispatch_label": log_label,
                "last_dispatch_function": normalized_function,
                "last_dispatch_kwargs": kwargs,
                "last_dispatch_app_name": app_name,
                "last_dispatch_modal_environment": environment_name,
            },
            supported_platforms=supported_platforms,
        )
        logger.exception(
            "Failed to dispatch %s job to Modal: app=%s function=%s environment=%s kwargs=%s",
            log_label,
            app_name,
            normalized_function,
            environment_name or "default",
            kwargs,
        )
        return {
            "dispatched": False,
            "reason": str(exc),
            "reason_code": reason_code,
            "call_id": None,
            "app_name": app_name,
            "function_name": normalized_function,
            "modal_environment": environment_name,
            "log_label": log_label,
            "execution_metadata": modal_execution_metadata(),
            "dispatch_config": modal_dispatch_config(),
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


def dispatch_social_job(*, job_id: str, stage: str | None = None) -> dict[str, Any]:
    return _spawn_named_modal_function(
        function_name=modal_social_job_function_name_for_stage(stage),
        log_label="social ingest",
        kwargs={"job_id": job_id},
        dispatcher_name="social",
        supported_platforms=list(SOCIAL_SUPPORTED_PLATFORMS),
    )


def dispatch_socialblade_scrape_sync(*, handle: str) -> dict[str, Any]:
    """Synchronous dispatch — calls Modal .remote() and blocks until result.

    Unlike other dispatchers that use .spawn() (fire-and-forget), this uses
    .remote() because the frontend waits synchronously for the scrape result.
    """
    function_name = modal_socialblade_function_name()
    ready, reason = modal_dispatch_ready(function_name=function_name)
    if not ready:
        return {"error": f"Modal not ready: {reason}", "dispatched": False}

    app_name = modal_app_name()
    try:
        import modal

        fn = modal.Function.from_name(app_name, function_name)
        result = fn.remote(handle=handle)
        logger.info(
            "SocialBlade scrape completed via Modal: app=%s function=%s handle=%s",
            app_name,
            function_name,
            handle,
        )
        return result
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Failed to dispatch SocialBlade scrape to Modal: app=%s function=%s handle=%s",
            app_name,
            function_name,
            handle,
        )
        return {"error": str(exc), "dispatched": False}


def dispatch_socialblade_scrape(
    *,
    person_id: str,
    handle: str,
    source: str,
    force: bool = False,
) -> dict[str, Any]:
    """Asynchronous SocialBlade dispatch for cast and season-level refreshes."""
    return _spawn_named_modal_function(
        function_name=modal_socialblade_function_name(),
        log_label="socialblade refresh",
        kwargs={
            "person_id": person_id,
            "handle": handle,
            "source": source,
            "force": force,
        },
        dispatcher_name="socialblade",
    )
