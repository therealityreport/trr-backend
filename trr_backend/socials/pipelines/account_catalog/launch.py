# ruff: noqa: F821, UP037
"""Catalog launch orchestration for social account backfill runs."""

from __future__ import annotations

import asyncio
import os
from collections.abc import Mapping, Sequence
from typing import Any

import trr_backend.socials.social_season_analytics_impl as _core
from trr_backend.socials.pipelines.comments.instagram import (
    preview_social_account_comments_scrape as _preview_comments_scrape,
)
from trr_backend.socials.pipelines.comments.instagram import (
    start_social_account_comments_scrape as _start_comments_scrape,
)

_RESERVED_CORE_EXPORTS = {
    "__builtins__",
    "__cached__",
    "__doc__",
    "__file__",
    "__loader__",
    "__name__",
    "__package__",
    "__spec__",
    "_core",
    "_IMPORTED_CORE_NAMES",
    "_LOCAL_ROOM_NAMES",
    "_RESERVED_CORE_EXPORTS",
    "_sync_core_overrides",
}
_IMPORTED_CORE_NAMES: set[str] = set()
for _name, _value in _core.__dict__.items():
    if _name in _RESERVED_CORE_EXPORTS:
        continue
    globals()[_name] = _value
    _IMPORTED_CORE_NAMES.add(_name)
_LOCAL_ROOM_NAMES: set[str] = set()
_LOCAL_ROOM_FUNCTIONS: dict[str, Any] = {}
_CORE_ROOM_WRAPPERS: dict[str, Any] = {}


def _sync_core_overrides() -> None:
    for _name in _IMPORTED_CORE_NAMES - _LOCAL_ROOM_NAMES:
        if hasattr(_core, _name):
            globals()[_name] = getattr(_core, _name)


def _room_callable(name: str, local_impl: Any) -> Any:
    candidate = getattr(_core, name, None)
    if callable(candidate) and candidate is not _CORE_ROOM_WRAPPERS.get(name):
        return candidate
    return local_impl


def _catalog_comments_auth_metadata(result: Mapping[str, Any] | None) -> dict[str, Any]:
    payload = _metadata_dict(result)
    if not payload:
        return {}
    if not payload.get("comments_auth_probe") and not payload.get("auth_repair_attempted"):
        return {}
    return {
        "auth_repair_attempted": bool(payload.get("auth_repair_attempted")),
        "auth_repair_status": str(payload.get("auth_repair_status") or "skipped").strip().lower() or "skipped",
        "auth_repair_reason": str(payload.get("auth_repair_reason") or "").strip() or None,
        "comments_auth_probe": _metadata_dict(payload.get("comments_auth_probe")) or None,
    }


def _instagram_posts_launch_auth_check_enabled() -> bool:
    raw = str(os.getenv("SOCIAL_INSTAGRAM_POSTS_LAUNCH_AUTH_CHECK") or "").strip().lower()
    if raw:
        return raw in {"1", "true", "yes", "on"}
    return not bool(os.getenv("PYTEST_CURRENT_TEST"))


def _posts_launch_auth_metadata(
    *,
    attempted: bool = False,
    status: str = "skipped",
    reason: str | None = None,
    probe: dict[str, Any] | None = None,
    repair_result: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "posts_auth_probe": probe or None,
        "auth_repair_attempted": bool(attempted),
        "auth_repair_status": str(status or "skipped").strip().lower() or "skipped",
        "auth_repair_reason": str(reason or "").strip() or None,
        "auth_repair_result": repair_result or None,
    }


def _public_posts_launch_auth_metadata(metadata: Mapping[str, Any] | None) -> dict[str, Any]:
    data = _metadata_dict(metadata)
    if not data:
        return {}
    return {
        "posts_auth_probe": _metadata_dict(data.get("posts_auth_probe")) or None,
        "auth_repair_attempted": bool(data.get("auth_repair_attempted")),
        "auth_repair_status": str(data.get("auth_repair_status") or "skipped").strip().lower() or "skipped",
        "auth_repair_reason": str(data.get("auth_repair_reason") or "").strip() or None,
    }


def _probe_instagram_posts_endpoint_for_launch(*, account_handle: str) -> dict[str, Any]:
    """Probe the profile-post GraphQL endpoint without persisting any posts."""

    async def _probe() -> dict[str, Any]:
        from trr_backend.socials.instagram.posts_scrapling.fetcher import InstagramPostsScraplingFetcher
        from trr_backend.socials.instagram.posts_scrapling.proxy import select_posts_proxy
        from trr_backend.socials.instagram.posts_scrapling.session import resolve_posts_scrapling_session

        session = resolve_posts_scrapling_session(
            browser_account_id=account_handle,
            caller_context=f"posts_launch_auth_probe:{account_handle}",
        )
        cookie_fingerprint = _instagram_cookie_fingerprint(session.auth_session.cookies)[:16]
        proxy_session_key = str(session.browser_account_id or account_handle).strip().lower().lstrip("@")
        fetcher = InstagramPostsScraplingFetcher(
            cookies=session.cookies,
            raw_cookies=session.auth_session.cookies,
            browser_account_id=session.browser_account_id,
            proxy_config=select_posts_proxy(session_key=proxy_session_key or account_handle),
            fast_mode=True,
        )
        try:
            await fetcher.warmup(account_handle)
            result = await fetcher.fetch_posts_page(account_handle, cursor=None)
            metadata = _metadata_dict(fetcher.runtime_metadata)
            if bool(result.auth_failed):
                status = "auth_blocked"
            elif bool(result.fetch_failed):
                status = "transport_blocked" if bool(result.retryable) else "fetch_blocked"
            else:
                status = "valid"
            return {
                "mode": "profile_posts_endpoint",
                "account_handle": account_handle,
                "status": status,
                "result": status,
                "reason": str(result.fetch_reason or "").strip() or None,
                "retryable": bool(result.retryable),
                "request_count": int(result.request_count or metadata.get("request_count") or 0),
                "posts_seen": len(result.posts or []),
                "has_next_page": bool(result.has_next_page),
                "doc_id_used": _metadata_dict(metadata.get("profile_posts_doc_ids")).get("used"),
                "profile_posts_doc_ids": _metadata_dict(metadata.get("profile_posts_doc_ids")),
                "proxy_identity": _metadata_dict(metadata.get("proxy_identity")),
                "cookie_fingerprint": cookie_fingerprint,
                "cookie_fingerprint_algorithm": "sha256:16",
                "auth_source": str(session.auth_session.source or "").strip() or None,
            }
        finally:
            await fetcher.aclose()

    try:
        return asyncio.run(_probe())
    except Exception as exc:  # noqa: BLE001
        error_code = str(getattr(exc, "error_code", "") or "").strip().lower()
        if error_code in {
            "instagram_posts_warmup_auth_failed",
            "instagram_posts_warmup_no_cookies",
            "instagram_posts_cookie_bridge_failed",
            "instagram_posts_auth_failed",
        }:
            status = "auth_blocked"
            retryable = False
        elif error_code in {"instagram_posts_warmup_transport_error"}:
            status = "transport_blocked"
            retryable = True
        else:
            status = "transport_blocked"
            retryable = True
        return {
            "mode": "profile_posts_endpoint",
            "account_handle": account_handle,
            "status": status,
            "result": status,
            "reason": error_code or exc.__class__.__name__,
            "retryable": retryable,
            "exception_class": exc.__class__.__name__,
        }


def _ensure_instagram_posts_auth_ready_for_launch(*, account_handle: str) -> dict[str, Any]:
    if not _instagram_posts_launch_auth_check_enabled():
        return _posts_launch_auth_metadata()

    first_probe = _probe_instagram_posts_endpoint_for_launch(account_handle=account_handle)
    first_status = str(first_probe.get("status") or first_probe.get("result") or "").strip().lower()
    if first_status == "valid":
        return _posts_launch_auth_metadata(status="skipped", probe=first_probe)
    if first_status != "auth_blocked":
        return _posts_launch_auth_metadata(
            status="skipped",
            reason=str(first_probe.get("reason") or first_status or "posts_auth_probe_not_valid").strip() or None,
            probe=first_probe,
        )

    repair_result = refresh_platform_cookies_interactive(
        "instagram",
        headless=True,
        timeout_seconds=300,
        account_handle=account_handle,
    )
    repair_payload = _metadata_dict(repair_result)
    if not bool(repair_payload.get("success")):
        reason = str(repair_payload.get("reason") or "instagram_auth_repair_failed").strip().lower()
        return _posts_launch_auth_metadata(
            attempted=True,
            status="failed",
            reason=reason,
            probe=first_probe,
            repair_result=repair_payload,
        )

    second_probe = _probe_instagram_posts_endpoint_for_launch(account_handle=account_handle)
    second_status = str(second_probe.get("status") or second_probe.get("result") or "").strip().lower()
    if second_status == "valid":
        return _posts_launch_auth_metadata(
            attempted=True,
            status="succeeded",
            probe=second_probe,
            repair_result=repair_payload,
        )

    reason = str(second_probe.get("reason") or second_status or "posts_auth_probe_failed_after_repair").strip()
    return _posts_launch_auth_metadata(
        attempted=True,
        status="failed",
        reason=reason or "posts_auth_probe_failed_after_repair",
        probe=second_probe,
        repair_result=repair_payload,
    )


def _blocked_instagram_posts_launch_payload(
    *,
    run_id: str | None,
    account_handle: str,
    source_scope: str,
    launch_group_id: str,
    selected_tasks: Sequence[Any] | None,
    effective_selected_tasks: Sequence[Any] | None,
    posts_auth_metadata: Mapping[str, Any],
    timing: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    public_auth_metadata = _public_posts_launch_auth_metadata(posts_auth_metadata)
    reason = str(public_auth_metadata.get("auth_repair_reason") or "posts_auth_blocked").strip().lower()
    normalized_selected_tasks = _normalize_social_account_catalog_backfill_selected_tasks(selected_tasks)
    normalized_effective_tasks = _normalize_optional_social_account_catalog_backfill_selected_tasks(
        effective_selected_tasks
    )
    metadata_updates = {
        "launch_state": "blocked_auth",
        "launch_task_resolution_pending": False,
        "launch_completed_at": _iso(_now_utc()),
        "selected_tasks": normalized_selected_tasks,
        "effective_selected_tasks": normalized_effective_tasks,
        "partial_scrape": True,
        "stop_reason": "checkpoint_required" if "checkpoint" in reason else "posts_auth_blocked",
        "blocked_reason": reason,
        **public_auth_metadata,
        **_catalog_stage_graph_metadata(
            selected_tasks=normalized_selected_tasks,
            effective_selected_tasks=normalized_effective_tasks,
            detail_status="blocked",
            comments_status="blocked" if "comments" in normalized_effective_tasks else "skipped",
            comments_blocker_reasons=["posts_auth_blocked"] if "comments" in normalized_effective_tasks else [],
            media_status="blocked" if "media" in normalized_effective_tasks else "skipped",
            enrichment_status="blocked",
            finalization_status="completed",
            timing=timing,
        ),
    }
    normalized_run_id = str(run_id or "").strip() or None
    if normalized_run_id:
        _merge_catalog_run_config(run_id=normalized_run_id, metadata_updates=metadata_updates)
        _set_run_status(normalized_run_id, "failed")
    return {
        "run_id": normalized_run_id,
        "status": "failed",
        "platform": "instagram",
        "account_handle": account_handle,
        "launch_group_id": launch_group_id,
        "selected_tasks": normalized_selected_tasks,
        "effective_selected_tasks": normalized_effective_tasks,
        "catalog_run_id": normalized_run_id,
        "comments_run_id": None,
        "catalog_status": "failed",
        "comments_status": None,
        "catalog_bootstrap_required": None,
        "comments_deferred_until_catalog_complete": False,
        "attached_followups": {},
        "partial_scrape": True,
        "stop_reason": metadata_updates["stop_reason"],
        "blocked_reason": reason,
        **public_auth_metadata,
        **_catalog_stage_graph_metadata(
            selected_tasks=normalized_selected_tasks,
            effective_selected_tasks=normalized_effective_tasks,
            detail_status="blocked",
            comments_status="blocked" if "comments" in normalized_effective_tasks else "skipped",
            comments_blocker_reasons=["posts_auth_blocked"] if "comments" in normalized_effective_tasks else [],
            media_status="blocked" if "media" in normalized_effective_tasks else "skipped",
            enrichment_status="blocked",
            finalization_status="completed",
            timing=timing,
        ),
    }


def _instagram_catalog_backfill_force_detail_fetch_enabled() -> bool:
    raw = (os.getenv("SOCIAL_INSTAGRAM_CATALOG_BACKFILL_FORCE_DETAIL_FETCH") or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


_CATALOG_STAGE_GRAPH_STAGES = (
    "target_readiness",
    "detail_refresh",
    "comments",
    "media",
    "enrichment",
    "finalization",
)


def _catalog_stage_entry(
    status: str,
    *,
    selected: bool = False,
    blocker_reasons: Sequence[Any] | None = None,
    timing_ms: float | None = None,
    **extra: Any,
) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "status": str(status or "pending").strip().lower() or "pending",
        "selected": bool(selected),
        "blocker_reasons": [
            str(reason or "").strip() for reason in list(blocker_reasons or []) if str(reason or "").strip()
        ],
    }
    if timing_ms is not None:
        entry["timing_ms"] = round(float(timing_ms or 0.0), 1)
    for key, value in extra.items():
        if value is not None:
            entry[key] = value
    return entry


def _catalog_stage_graph(
    *,
    selected_tasks: Sequence[Any] | None,
    effective_selected_tasks: Sequence[Any] | None,
    target_readiness: Mapping[str, Any] | None = None,
    detail_status: str | None = None,
    comments_status: str | None = None,
    comments_blocker_reasons: Sequence[Any] | None = None,
    media_status: str | None = None,
    enrichment_status: str | None = None,
    finalization_status: str | None = None,
) -> dict[str, dict[str, Any]]:
    effective_tasks = set(
        _normalize_optional_social_account_catalog_backfill_selected_tasks(effective_selected_tasks) or []
    )
    selected = set(_normalize_optional_social_account_catalog_backfill_selected_tasks(selected_tasks) or [])
    readiness = _metadata_dict(target_readiness)
    readiness_blockers = list(readiness.get("blocker_reasons") or [])
    comments_blockers = [
        str(reason or "").strip()
        for reason in list(comments_blocker_reasons or readiness.get("comments_blocker_reasons") or [])
        if str(reason or "").strip()
    ]
    graph = {
        "target_readiness": _catalog_stage_entry(
            str(readiness.get("status") or ("completed" if readiness else "pending")),
            selected=bool(effective_tasks & {"comments", "media", "post_details"}),
            blocker_reasons=readiness_blockers,
            timing_ms=readiness.get("timing_ms"),
            saved_source_ids_count=_normalize_non_negative_int(readiness.get("saved_source_ids_count")),
            commentable_target_count=_normalize_non_negative_int(readiness.get("commentable_target_count")),
            comments_target_source_ids_count=_normalize_non_negative_int(
                readiness.get("comments_target_source_ids_count")
            ),
            detail_gap_count=_normalize_non_negative_int(readiness.get("detail_gap_count")),
        ),
        "detail_refresh": _catalog_stage_entry(
            detail_status or ("pending" if "post_details" in effective_tasks else "skipped"),
            selected="post_details" in selected or "post_details" in effective_tasks,
        ),
        "comments": _catalog_stage_entry(
            comments_status or ("pending" if "comments" in effective_tasks else "skipped"),
            selected="comments" in selected or "comments" in effective_tasks,
            blocker_reasons=comments_blockers,
        ),
        "media": _catalog_stage_entry(
            media_status or ("pending" if "media" in effective_tasks else "skipped"),
            selected="media" in selected or "media" in effective_tasks,
        ),
        "enrichment": _catalog_stage_entry(
            enrichment_status or "pending",
            selected=bool(effective_tasks & {"post_details", "media"}),
        ),
        "finalization": _catalog_stage_entry(finalization_status or "pending", selected=True),
    }
    return {stage: graph[stage] for stage in _CATALOG_STAGE_GRAPH_STAGES}


def _comments_status_from_posts_stage(
    *,
    platform: str,
    effective_selected_tasks: Sequence[Any] | None,
    job_status: str | None,
) -> str:
    selected = set(_normalize_optional_social_account_catalog_backfill_selected_tasks(effective_selected_tasks) or [])
    if "comments" not in selected:
        return "skipped"
    normalized_platform = str(platform or "").strip().lower()
    if normalized_platform in {"tiktok", "twitter", "youtube", "threads"}:
        return str(job_status or "").strip().lower() or "pending"
    return "skipped"


def _catalog_stage_graph_metadata(
    *,
    selected_tasks: Sequence[Any] | None,
    effective_selected_tasks: Sequence[Any] | None,
    target_readiness: Mapping[str, Any] | None = None,
    detail_status: str | None = None,
    comments_status: str | None = None,
    comments_blocker_reasons: Sequence[Any] | None = None,
    media_status: str | None = None,
    enrichment_status: str | None = None,
    finalization_status: str | None = None,
    timing: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "pipeline_strategy": "stage_graph",
        "stage_graph": _catalog_stage_graph(
            selected_tasks=selected_tasks,
            effective_selected_tasks=effective_selected_tasks,
            target_readiness=target_readiness,
            detail_status=detail_status,
            comments_status=comments_status,
            comments_blocker_reasons=comments_blocker_reasons,
            media_status=media_status,
            enrichment_status=enrichment_status,
            finalization_status=finalization_status,
        ),
    }
    if target_readiness:
        metadata["target_readiness"] = _metadata_dict(target_readiness)
    timing_payload = _metadata_dict(timing)
    if timing_payload:
        metadata["timing"] = timing_payload
    return metadata


def _catalog_comments_blockers_from_error(exc: Exception) -> list[str]:
    code = str(getattr(exc, "code", "") or "").strip().upper()
    message = str(exc or "").strip().lower()
    if code == "SOCIAL_ACCOUNT_COMMENTS_NOTHING_TO_REFRESH":
        if "saved instagram posts" in message:
            return ["missing_source_ids"]
        return ["target_count_zero"]
    if code == "SOCIAL_INSTAGRAM_COMMENTS_AUTH_REPAIR_FAILED":
        if "checkpoint" in message:
            return ["checkpoint_required"]
        return ["auth_probe_failed"]
    return [code.lower() if code else "comments_launch_failed"]


def build_instagram_backfill_target_readiness(
    account_handle: str,
    *,
    coverage: Mapping[str, Any] | None = None,
    refresh_policy: str = "stale_or_missing",
) -> dict[str, Any]:
    started_at = time_module.perf_counter()
    normalized_account = _normalize_social_account_profile_handle(account_handle)
    coverage_payload = _metadata_dict(coverage)
    blocker_reasons: list[str] = []
    comments_blocker_reasons: list[str] = []
    try:
        target_counts = _room_callable(
            "_instagram_social_account_comments_target_counts",
            _instagram_social_account_comments_target_counts,
        )(normalized_account)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "[catalog-launch] target readiness counts failed account=%s error=%s",
            normalized_account,
            exc,
        )
        target_counts = {}
        blocker_reasons.append("target_readiness_failed")
    try:
        preview = _room_callable(
            "preview_social_account_comments_scrape",
            _preview_comments_scrape,
        )(
            "instagram",
            normalized_account,
            mode="profile",
            refresh_policy=refresh_policy,
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "[catalog-launch] target readiness preview failed account=%s error=%s",
            normalized_account,
            exc,
        )
        preview = {}
        if "target_readiness_failed" not in blocker_reasons:
            blocker_reasons.append("target_readiness_failed")

    saved_source_ids_count = max(
        _normalize_non_negative_int(coverage_payload.get("materialized_posts")),
        _normalize_non_negative_int(coverage_payload.get("catalog_posts")),
        _normalize_non_negative_int(target_counts.get("available_posts")),
    )
    comments_target_source_ids_count = _normalize_non_negative_int(preview.get("target_source_ids_count"))
    commentable_target_count = _normalize_non_negative_int(target_counts.get("eligible_posts"))
    detail_gap_counts = _metadata_dict(coverage_payload.get("detail_gap_counts"))
    detail_gap_count = _normalize_non_negative_int(detail_gap_counts.get("posts_needing_detail_refresh"))
    if saved_source_ids_count <= 0:
        blocker_reasons.append("missing_source_ids")
        comments_blocker_reasons.append("missing_source_ids")
    if comments_target_source_ids_count <= 0:
        comments_blocker_reasons.append("target_count_zero")
    can_start_comments = saved_source_ids_count > 0 and comments_target_source_ids_count > 0
    status = "completed" if not blocker_reasons else "blocked"
    return {
        "status": status,
        "account_handle": normalized_account,
        "saved_source_ids_count": saved_source_ids_count,
        "commentable_target_count": commentable_target_count,
        "comments_target_source_ids_count": comments_target_source_ids_count,
        "sample_target_source_ids": _as_text_list(preview.get("sample_target_source_ids"))[:12],
        "incomplete_comment_target_count": _normalize_non_negative_int(target_counts.get("missing_posts"))
        + _normalize_non_negative_int(target_counts.get("stale_posts")),
        "media_candidate_count": saved_source_ids_count,
        "detail_gap_count": detail_gap_count,
        "can_start_comments": can_start_comments,
        "blocker_reasons": list(dict.fromkeys(blocker_reasons)),
        "comments_blocker_reasons": list(dict.fromkeys(comments_blocker_reasons)),
        "refresh_policy": str(refresh_policy or "stale_or_missing").strip().lower() or "stale_or_missing",
        "timing_ms": round((time_module.perf_counter() - started_at) * 1000, 1),
        "comments_preview": {
            "comments_shard_count": _normalize_non_negative_int(preview.get("comments_shard_count")) or None,
            "comments_sharding_enabled": bool(preview.get("comments_sharding_enabled")),
            "recommended_comments_shard_count": _normalize_non_negative_int(
                preview.get("recommended_comments_shard_count")
            )
            or None,
            "target_priority": str(preview.get("target_priority") or "").strip() or None,
        },
    }


def start_social_account_catalog_backfill(
    platform: str,
    account_handle: str,
    *,
    source_scope: str = "network",
    date_start: datetime | None = None,
    date_end: datetime | None = None,
    initiated_by: str | None = None,
    inline_worker_id: str | None = None,
    allow_local_dev_inline_bypass: bool = False,
    execution_preference: Literal["auto", "prefer_local_inline"] = "auto",
    resume_frontier_cursor: str | None = None,
    resume_frontier_snapshot: Mapping[str, Any] | None = None,
    catalog_action: str | None = None,
    catalog_action_scope: str | None = None,
    social_account_post_details_only: bool = False,
    details_refresh_skip_detail_fetch: bool | None = None,
    details_refresh_force_detail_fetch: bool | None = None,
    details_refresh_skip_media_followups: bool | None = None,
    tiktok_comments_in_posts_stage: bool = False,
    tiktok_direct_comment_api_override: bool = False,
    twitter_comments_in_posts_stage: bool = False,
    comment_anchor_source_ids: dict[str, list[str]] | None = None,
    selected_tasks: Sequence[Any] | None = None,
    effective_selected_tasks: Sequence[Any] | None = None,
    launch_group_id: str | None = None,
    existing_run_id: str | None = None,
) -> dict[str, Any]:
    _sync_core_overrides()
    normalized_platform = _normalize_social_account_profile_platform(platform)
    normalized_account = _normalize_social_account_profile_handle(account_handle)
    normalized_execution_preference = str(execution_preference or "auto").strip().lower() or "auto"
    if normalized_execution_preference not in {"auto", "prefer_local_inline"}:
        normalized_execution_preference = "auto"
    action_seed = resolve_social_account_catalog_action_seed(
        date_start=date_start,
        date_end=date_end,
        resume_frontier_cursor=resume_frontier_cursor,
        catalog_action=catalog_action,
        catalog_action_scope=catalog_action_scope,
    )
    normalized_date_start = action_seed["date_start"]
    normalized_date_end = action_seed["date_end"]
    normalized_resume_cursor = action_seed["resume_frontier_cursor"]
    normalized_catalog_action = action_seed["catalog_action"]
    normalized_catalog_action_scope = action_seed["catalog_action_scope"]
    normalized_resume_snapshot = dict(resume_frontier_snapshot or {}) if normalized_resume_cursor else None
    if normalized_platform not in set(CATALOG_SUPPORTED_PLATFORMS):
        raise ValueError("Catalog backfill is not supported for this platform.")
    _assert_social_account_profile_exists(normalized_platform, normalized_account)
    run_id = str(existing_run_id or "").strip() or None
    reserved_here = run_id is None
    if reserved_here:
        reservation = _reserve_social_account_catalog_launch(
            platform=normalized_platform,
            account_handle=normalized_account,
            source_scope=source_scope,
            initiated_by=initiated_by,
            placeholder_config={
                **_build_social_account_catalog_launch_placeholder_config(
                    platform=normalized_platform,
                    account_handle=normalized_account,
                    source_scope=source_scope,
                    date_start=normalized_date_start,
                    date_end=normalized_date_end,
                    allow_local_dev_inline_bypass=allow_local_dev_inline_bypass,
                    execution_preference=normalized_execution_preference,
                    launch_group_id=launch_group_id,
                    resume_frontier_cursor=normalized_resume_cursor,
                    catalog_action=normalized_catalog_action,
                    catalog_action_scope=normalized_catalog_action_scope,
                    task_resolution_pending=False,
                    comment_anchor_source_ids=comment_anchor_source_ids,
                ),
                **_catalog_stage_graph_metadata(
                    selected_tasks=[],
                    effective_selected_tasks=[],
                    finalization_status="pending",
                ),
            },
            initial_status=_catalog_launch_initial_status(
                allow_local_dev_inline_bypass=allow_local_dev_inline_bypass,
            ),
        )
        run_id = str(reservation.get("run_id") or "").strip() or None
        logger.info(
            "[catalog-launch] kickoff_reserved platform=%s account=%s run_id=%s lock_wait_ms=%.1f lock_held_ms=%.1f",
            normalized_platform,
            normalized_account,
            run_id,
            float(reservation.get("lock_wait_ms") or 0.0),
            float(reservation.get("lock_held_ms") or 0.0),
        )

    try:
        if (
            is_queue_enabled()
            and not allow_local_dev_inline_bypass
            and _shared_account_catalog_requires_modal_executor(
                platform=normalized_platform,
                pipeline_ingest_mode=SHARED_ACCOUNT_CATALOG_BACKFILL_INGEST_MODE,
            )
        ):
            assert_worker_available_when_queue_enabled(
                required_execution_backend="modal",
                platform=normalized_platform,
            )
        skip_implicit_frontier_resume = (
            normalized_platform == "instagram"
            and normalized_catalog_action == "backfill"
            and normalized_catalog_action_scope == "full_history"
        )
        if (
            not skip_implicit_frontier_resume
            and normalized_date_start is None
            and normalized_date_end is None
            and normalized_resume_cursor is None
        ):
            frontier = _latest_account_frontier(normalized_platform, normalized_account)
            next_cursor = str(frontier.get("next_cursor") or "").strip() or None
            if next_cursor and not frontier.get("exhausted"):
                normalized_resume_cursor = next_cursor
                normalized_resume_snapshot = {
                    "id": frontier.get("id"),
                    "run_id": frontier.get("run_id"),
                    "next_cursor": next_cursor,
                    "total_posts": frontier.get("total_posts"),
                    "posts_checked": frontier.get("posts_checked") or 0,
                    "posts_saved": frontier.get("posts_saved") or 0,
                    "pages_scanned": frontier.get("pages_scanned") or 0,
                    "last_transport": frontier.get("last_transport"),
                }
        ingest_kwargs = {
            "platforms": [normalized_platform],
            "source_scope": source_scope,
            "accounts_override": [normalized_account],
            "date_start": normalized_date_start,
            "date_end": normalized_date_end,
            "pipeline_ingest_mode": SHARED_ACCOUNT_CATALOG_BACKFILL_INGEST_MODE,
            "initiated_by": initiated_by,
            "inline_worker_id": inline_worker_id,
            "allow_local_dev_inline_bypass": allow_local_dev_inline_bypass,
            "execution_preference": normalized_execution_preference,
            "allow_ephemeral_accounts_override_sources": True,
            "resume_frontier_cursor": normalized_resume_cursor,
            "resume_frontier_snapshot": normalized_resume_snapshot,
            "catalog_action": normalized_catalog_action,
            "catalog_action_scope": normalized_catalog_action_scope,
            "social_account_post_details_only": social_account_post_details_only,
            "details_refresh_skip_detail_fetch": details_refresh_skip_detail_fetch,
            "details_refresh_force_detail_fetch": details_refresh_force_detail_fetch,
            "details_refresh_skip_media_followups": details_refresh_skip_media_followups,
            "tiktok_comments_in_posts_stage": tiktok_comments_in_posts_stage,
            "tiktok_direct_comment_api_override": tiktok_direct_comment_api_override,
            "selected_tasks": selected_tasks,
            "effective_selected_tasks": effective_selected_tasks,
            "comment_anchor_source_ids": comment_anchor_source_ids,
            "launch_group_id": launch_group_id,
            "existing_run_id": run_id,
            "defer_initial_dispatch": not reserved_here,
        }
        if "twitter_comments_in_posts_stage" in getattr(
            getattr(ingest_shared_accounts, "__code__", None),
            "co_varnames",
            (),
        ):
            ingest_kwargs["twitter_comments_in_posts_stage"] = twitter_comments_in_posts_stage
        result = ingest_shared_accounts(**ingest_kwargs)
        if run_id:
            _merge_catalog_run_config(
                run_id=run_id,
                metadata_updates={
                    "launch_state": "ready",
                    "launch_task_resolution_pending": False,
                    "launch_completed_at": _iso(_now_utc()),
                    **_catalog_stage_graph_metadata(
                        selected_tasks=_normalize_optional_social_account_catalog_backfill_selected_tasks(
                            (result or {}).get("selected_tasks")
                        )
                        or [],
                        effective_selected_tasks=_normalize_optional_social_account_catalog_backfill_selected_tasks(
                            (result or {}).get("effective_selected_tasks")
                        )
                        or [],
                        finalization_status="completed",
                    ),
                },
            )
        return result
    except Exception as exc:  # noqa: BLE001
        if reserved_here and run_id:
            _record_social_account_catalog_launch_failure(
                run_id=run_id,
                error_message=str(exc),
            )
        raise


def begin_social_account_catalog_backfill_launch(
    platform: str,
    account_handle: str,
    *,
    source_scope: str = "network",
    date_start: datetime | None = None,
    date_end: datetime | None = None,
    initiated_by: str | None = None,
    allow_local_dev_inline_bypass: bool = False,
    execution_preference: Literal["auto", "prefer_local_inline"] = "auto",
    selected_tasks: Sequence[Any] | None = None,
    comment_anchor_source_ids: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    _sync_core_overrides()
    normalized_platform = _normalize_social_account_profile_platform(platform)
    normalized_account = _normalize_social_account_profile_handle(account_handle)
    if normalized_platform not in set(CATALOG_SUPPORTED_PLATFORMS):
        raise ValueError("Catalog backfill is not supported for this platform.")
    _assert_social_account_profile_exists(normalized_platform, normalized_account)
    normalized_execution_preference = str(execution_preference or "auto").strip().lower() or "auto"
    if normalized_execution_preference not in {"auto", "prefer_local_inline"}:
        normalized_execution_preference = "auto"
    action_seed = resolve_social_account_catalog_action_seed(
        date_start=date_start,
        date_end=date_end,
        catalog_action="backfill",
    )
    launch_group_id = str(uuid4())
    normalized_selected_tasks = _normalize_social_account_catalog_backfill_selected_tasks(selected_tasks)
    reservation = _reserve_social_account_catalog_launch(
        platform=normalized_platform,
        account_handle=normalized_account,
        source_scope=source_scope,
        initiated_by=initiated_by,
        placeholder_config={
            **_build_social_account_catalog_launch_placeholder_config(
                platform=normalized_platform,
                account_handle=normalized_account,
                source_scope=source_scope,
                date_start=action_seed["date_start"],
                date_end=action_seed["date_end"],
                allow_local_dev_inline_bypass=allow_local_dev_inline_bypass,
                execution_preference=normalized_execution_preference,
                launch_group_id=launch_group_id,
                resume_frontier_cursor=action_seed["resume_frontier_cursor"],
                catalog_action=action_seed["catalog_action"],
                catalog_action_scope=action_seed["catalog_action_scope"],
                selected_tasks=normalized_selected_tasks,
                comment_anchor_source_ids=comment_anchor_source_ids,
                task_resolution_pending=True,
            ),
            **_catalog_stage_graph_metadata(
                selected_tasks=normalized_selected_tasks,
                effective_selected_tasks=normalized_selected_tasks,
                finalization_status="pending",
            ),
        },
        initial_status=_catalog_launch_initial_status(
            allow_local_dev_inline_bypass=allow_local_dev_inline_bypass,
        ),
    )
    run_id = str(reservation.get("run_id") or "").strip()
    initial_status = _catalog_launch_initial_status(
        allow_local_dev_inline_bypass=allow_local_dev_inline_bypass,
    )
    logger.info(
        (
            "[catalog-launch] kickoff_reserved platform=%s account=%s run_id=%s status=%s "
            "lock_wait_ms=%.1f lock_held_ms=%.1f"
        ),
        normalized_platform,
        normalized_account,
        run_id,
        initial_status,
        float(reservation.get("lock_wait_ms") or 0.0),
        float(reservation.get("lock_held_ms") or 0.0),
    )
    return {
        "run_id": run_id,
        "status": initial_status,
        "platform": normalized_platform,
        "account_handle": normalized_account,
        "launch_group_id": launch_group_id,
        "selected_tasks": normalized_selected_tasks,
        "effective_selected_tasks": normalized_selected_tasks,
        "catalog_run_id": run_id,
        "comments_run_id": None,
        "catalog_status": initial_status,
        "comments_status": None,
        "catalog_bootstrap_required": None,
        "comments_deferred_until_catalog_complete": False,
        "post_details_skipped_reason": None,
        "launch_state": "pending",
        "launch_task_resolution_pending": True,
        "attached_followups": {},
        "catalog_action": action_seed["catalog_action"],
        "catalog_action_scope": action_seed["catalog_action_scope"],
        "ingest_mode": SHARED_ACCOUNT_CATALOG_BACKFILL_INGEST_MODE,
        **_catalog_stage_graph_metadata(
            selected_tasks=normalized_selected_tasks,
            effective_selected_tasks=normalized_selected_tasks,
            finalization_status="pending",
        ),
    }


def finalize_social_account_catalog_backfill_launch(
    platform: str,
    account_handle: str,
    *,
    run_id: str,
    source_scope: str = "network",
    date_start: datetime | None = None,
    date_end: datetime | None = None,
    initiated_by: str | None = None,
    allow_local_dev_inline_bypass: bool = False,
    execution_preference: Literal["auto", "prefer_local_inline"] = "auto",
    selected_tasks: Sequence[Any] | None = None,
    launch_group_id: str | None = None,
    comment_anchor_source_ids: dict[str, list[str]] | None = None,
    catalog_action: str | None = None,
    catalog_action_scope: str | None = None,
) -> dict[str, Any]:
    _sync_core_overrides()
    normalized_platform = _normalize_social_account_profile_platform(platform)
    normalized_account = _normalize_social_account_profile_handle(account_handle)
    started_at = time_module.perf_counter()
    try:
        _merge_catalog_run_config(
            run_id=run_id,
            metadata_updates={
                "launch_state": "finalizing",
                "launch_task_resolution_pending": True,
                "launch_finalizing_started_at": _iso(_now_utc()),
            },
        )
        result = _room_callable(
            "launch_social_account_catalog_backfill",
            launch_social_account_catalog_backfill,
        )(
            normalized_platform,
            normalized_account,
            source_scope=source_scope,
            date_start=date_start,
            date_end=date_end,
            initiated_by=initiated_by,
            allow_local_dev_inline_bypass=allow_local_dev_inline_bypass,
            execution_preference=execution_preference,
            selected_tasks=selected_tasks,
            comment_anchor_source_ids=comment_anchor_source_ids,
            existing_catalog_run_id=run_id,
            launch_group_id_override=launch_group_id,
            catalog_action=catalog_action,
            catalog_action_scope=catalog_action_scope,
        )
        logger.info(
            "[catalog-launch] finalize_complete platform=%s account=%s run_id=%s total_ms=%.1f comments_run_id=%s",
            normalized_platform,
            normalized_account,
            run_id,
            round((time_module.perf_counter() - started_at) * 1000, 1),
            str(result.get("comments_run_id") or "").strip() or None,
        )
        return result
    except Exception as exc:  # noqa: BLE001
        _record_social_account_catalog_launch_failure(
            run_id=run_id,
            error_message=str(exc),
        )
        logger.exception(
            "[catalog-launch] finalize_failed platform=%s account=%s run_id=%s",
            normalized_platform,
            normalized_account,
            run_id,
        )
        raise


def launch_social_account_catalog_backfill(
    platform: str,
    account_handle: str,
    *,
    source_scope: str = "network",
    date_start: datetime | None = None,
    date_end: datetime | None = None,
    initiated_by: str | None = None,
    inline_worker_id: str | None = None,
    allow_local_dev_inline_bypass: bool = False,
    execution_preference: Literal["auto", "prefer_local_inline"] = "auto",
    selected_tasks: Sequence[Any] | None = None,
    existing_catalog_run_id: str | None = None,
    launch_group_id_override: str | None = None,
    comment_anchor_source_ids: dict[str, list[str]] | None = None,
    catalog_action: str | None = None,
    catalog_action_scope: str | None = None,
) -> dict[str, Any]:
    _sync_core_overrides()
    normalized_platform = _normalize_social_account_profile_platform(platform)
    normalized_account = _normalize_social_account_profile_handle(account_handle)
    launch_started_at = time_module.perf_counter()
    coverage_ms = 0.0
    catalog_launch_ms = 0.0
    comments_launch_ms = 0.0
    normalized_selected_tasks = _normalize_social_account_catalog_backfill_selected_tasks(selected_tasks)
    normalized_comment_anchor_source_ids, _comment_anchor_overflow_platforms = _normalize_comment_anchor_source_ids(
        comment_anchor_source_ids,
        allowed_platforms={normalized_platform},
    )
    instagram_targeted_comment_source_ids = (
        sorted(normalized_comment_anchor_source_ids.get("instagram") or [])
        if normalized_platform == "instagram"
        else []
    )
    instagram_targeted_comments_only = bool(
        normalized_platform == "instagram"
        and instagram_targeted_comment_source_ids
        and set(normalized_selected_tasks) <= {"comments"}
    )
    normalized_execution_preference = str(execution_preference or "auto").strip().lower() or "auto"
    if normalized_execution_preference not in {"auto", "prefer_local_inline"}:
        normalized_execution_preference = "auto"
    action_seed = resolve_social_account_catalog_action_seed(
        date_start=date_start,
        date_end=date_end,
        catalog_action=catalog_action,
        catalog_action_scope=catalog_action_scope,
    )
    normalized_date_start = action_seed["date_start"]
    normalized_date_end = action_seed["date_end"]
    normalized_catalog_action = action_seed["catalog_action"]
    normalized_catalog_action_scope = action_seed["catalog_action_scope"]
    bounded_window_scope = normalized_catalog_action_scope
    if normalized_platform == "tiktok":
        launch_group_id = str(launch_group_id_override or uuid4())
        effective_selected_tasks = _effective_social_account_catalog_backfill_selected_tasks(
            normalized_platform,
            normalized_selected_tasks,
        )
        catalog_launch_started_at = time_module.perf_counter()
        catalog_result = _room_callable(
            "start_social_account_catalog_backfill",
            start_social_account_catalog_backfill,
        )(
            normalized_platform,
            normalized_account,
            source_scope=source_scope,
            date_start=normalized_date_start,
            date_end=normalized_date_end,
            initiated_by=initiated_by,
            inline_worker_id=inline_worker_id,
            allow_local_dev_inline_bypass=allow_local_dev_inline_bypass,
            execution_preference=execution_preference,
            catalog_action=normalized_catalog_action,
            catalog_action_scope=normalized_catalog_action_scope,
            details_refresh_skip_detail_fetch="post_details" not in effective_selected_tasks,
            details_refresh_skip_media_followups="media" not in effective_selected_tasks,
            tiktok_comments_in_posts_stage="comments" in effective_selected_tasks,
            tiktok_direct_comment_api_override=(
                "comments" in effective_selected_tasks and _tiktok_catalog_comment_override_enabled()
            ),
            selected_tasks=normalized_selected_tasks,
            effective_selected_tasks=effective_selected_tasks,
            comment_anchor_source_ids=comment_anchor_source_ids,
            launch_group_id=launch_group_id,
            existing_run_id=existing_catalog_run_id,
        )
        catalog_launch_ms = round((time_module.perf_counter() - catalog_launch_started_at) * 1000, 1)
        catalog_run_id = str((catalog_result or {}).get("run_id") or "").strip() or None
        if catalog_run_id:
            _merge_catalog_run_config(
                run_id=catalog_run_id,
                metadata_updates={
                    "selected_tasks": normalized_selected_tasks,
                    "effective_selected_tasks": effective_selected_tasks,
                    **_catalog_stage_graph_metadata(
                        selected_tasks=normalized_selected_tasks,
                        effective_selected_tasks=effective_selected_tasks,
                        detail_status=str((catalog_result or {}).get("status") or "").strip().lower() or "pending",
                        comments_status=_comments_status_from_posts_stage(
                            platform=normalized_platform,
                            effective_selected_tasks=effective_selected_tasks,
                            job_status=str((catalog_result or {}).get("status") or "").strip().lower() or "pending",
                        ),
                        media_status=(
                            str((catalog_result or {}).get("status") or "").strip().lower() or "pending"
                            if "media" in effective_selected_tasks
                            else "skipped"
                        ),
                        finalization_status="completed",
                        timing={
                            "coverage_ms": coverage_ms,
                            "catalog_launch_ms": catalog_launch_ms,
                            "comments_launch_ms": comments_launch_ms,
                            "total_ms": round((time_module.perf_counter() - launch_started_at) * 1000, 1),
                        },
                    ),
                },
            )
        logger.info(
            (
                "[catalog-launch] launch_complete platform=%s account=%s run_id=%s "
                "existing_run_id=%s coverage_ms=%.1f catalog_launch_ms=%.1f "
                "comments_launch_ms=%.1f total_ms=%.1f selected_tasks=%s "
                "effective_selected_tasks=%s"
            ),
            normalized_platform,
            normalized_account,
            catalog_run_id,
            str(existing_catalog_run_id or "").strip() or None,
            coverage_ms,
            catalog_launch_ms,
            comments_launch_ms,
            round((time_module.perf_counter() - launch_started_at) * 1000, 1),
            normalized_selected_tasks,
            effective_selected_tasks,
        )
        return {
            "run_id": catalog_run_id,
            "status": str((catalog_result or {}).get("status") or "").strip() or None,
            "platform": normalized_platform,
            "account_handle": normalized_account,
            "launch_group_id": launch_group_id,
            "selected_tasks": normalized_selected_tasks,
            "effective_selected_tasks": effective_selected_tasks,
            "post_details_skipped_reason": (
                "forced_for_comments"
                if "comments" in normalized_selected_tasks and "post_details" not in normalized_selected_tasks
                else None
            ),
            "catalog_run_id": catalog_run_id,
            "comments_run_id": None,
            "catalog_status": str((catalog_result or {}).get("status") or "").strip() or None,
            "comments_status": None,
            "catalog_action": normalized_catalog_action,
            "catalog_action_scope": normalized_catalog_action_scope,
            "catalog_bootstrap_required": False,
            "comments_deferred_until_catalog_complete": False,
            "attached_followups": {},
            **_catalog_stage_graph_metadata(
                selected_tasks=normalized_selected_tasks,
                effective_selected_tasks=effective_selected_tasks,
                detail_status=str((catalog_result or {}).get("status") or "").strip().lower() or "pending",
                comments_status=_comments_status_from_posts_stage(
                    platform=normalized_platform,
                    effective_selected_tasks=effective_selected_tasks,
                    job_status=str((catalog_result or {}).get("status") or "").strip().lower() or "pending",
                ),
                media_status=(
                    str((catalog_result or {}).get("status") or "").strip().lower() or "pending"
                    if "media" in effective_selected_tasks
                    else "skipped"
                ),
                finalization_status="completed",
                timing={
                    "coverage_ms": coverage_ms,
                    "catalog_launch_ms": catalog_launch_ms,
                    "comments_launch_ms": comments_launch_ms,
                    "total_ms": round((time_module.perf_counter() - launch_started_at) * 1000, 1),
                },
            ),
        }
    if normalized_platform != "instagram":
        launch_group_id = str(launch_group_id_override or uuid4())
        effective_selected_tasks = _effective_social_account_catalog_backfill_selected_tasks(
            normalized_platform,
            normalized_selected_tasks,
        )
        catalog_launch_started_at = time_module.perf_counter()
        catalog_result = _room_callable(
            "start_social_account_catalog_backfill",
            start_social_account_catalog_backfill,
        )(
            normalized_platform,
            normalized_account,
            source_scope=source_scope,
            date_start=normalized_date_start,
            date_end=normalized_date_end,
            initiated_by=initiated_by,
            inline_worker_id=inline_worker_id,
            allow_local_dev_inline_bypass=allow_local_dev_inline_bypass,
            execution_preference=execution_preference,
            catalog_action=normalized_catalog_action,
            catalog_action_scope=normalized_catalog_action_scope,
            social_account_post_details_only=effective_selected_tasks == ["post_details"],
            details_refresh_skip_detail_fetch="post_details" not in effective_selected_tasks,
            details_refresh_skip_media_followups="media" not in effective_selected_tasks,
            launch_group_id=launch_group_id,
            twitter_comments_in_posts_stage=(
                normalized_platform == "twitter" and "comments" in effective_selected_tasks
            ),
            selected_tasks=normalized_selected_tasks,
            effective_selected_tasks=effective_selected_tasks,
            comment_anchor_source_ids=comment_anchor_source_ids,
            existing_run_id=existing_catalog_run_id,
        )
        catalog_launch_ms = round((time_module.perf_counter() - catalog_launch_started_at) * 1000, 1)
        catalog_run_id = str((catalog_result or {}).get("run_id") or "").strip() or None
        if catalog_run_id:
            _merge_catalog_run_config(
                run_id=catalog_run_id,
                metadata_updates={
                    "selected_tasks": normalized_selected_tasks,
                    "effective_selected_tasks": effective_selected_tasks,
                    **_catalog_stage_graph_metadata(
                        selected_tasks=normalized_selected_tasks,
                        effective_selected_tasks=effective_selected_tasks,
                        detail_status=str((catalog_result or {}).get("status") or "").strip().lower() or "pending",
                        comments_status=_comments_status_from_posts_stage(
                            platform=normalized_platform,
                            effective_selected_tasks=effective_selected_tasks,
                            job_status=str((catalog_result or {}).get("status") or "").strip().lower() or "pending",
                        ),
                        media_status=(
                            str((catalog_result or {}).get("status") or "").strip().lower() or "pending"
                            if "media" in effective_selected_tasks
                            else "skipped"
                        ),
                        finalization_status="completed",
                        timing={
                            "coverage_ms": coverage_ms,
                            "catalog_launch_ms": catalog_launch_ms,
                            "comments_launch_ms": comments_launch_ms,
                            "total_ms": round((time_module.perf_counter() - launch_started_at) * 1000, 1),
                        },
                    ),
                },
            )
        logger.info(
            (
                "[catalog-launch] launch_complete platform=%s account=%s run_id=%s "
                "existing_run_id=%s coverage_ms=%.1f catalog_launch_ms=%.1f "
                "comments_launch_ms=%.1f total_ms=%.1f selected_tasks=%s "
                "effective_selected_tasks=%s"
            ),
            normalized_platform,
            normalized_account,
            catalog_run_id,
            str(existing_catalog_run_id or "").strip() or None,
            coverage_ms,
            catalog_launch_ms,
            comments_launch_ms,
            round((time_module.perf_counter() - launch_started_at) * 1000, 1),
            normalized_selected_tasks,
            effective_selected_tasks,
        )
        return {
            "run_id": catalog_run_id,
            "status": str((catalog_result or {}).get("status") or "").strip() or None,
            "platform": normalized_platform,
            "account_handle": normalized_account,
            "launch_group_id": launch_group_id,
            "selected_tasks": normalized_selected_tasks,
            "effective_selected_tasks": effective_selected_tasks,
            "post_details_skipped_reason": (
                "forced_for_comments"
                if "comments" in normalized_selected_tasks and "post_details" not in normalized_selected_tasks
                else None
            ),
            "catalog_run_id": catalog_run_id,
            "comments_run_id": None,
            "catalog_status": str((catalog_result or {}).get("status") or "").strip() or None,
            "comments_status": None,
            "catalog_action": normalized_catalog_action,
            "catalog_action_scope": normalized_catalog_action_scope,
            "catalog_bootstrap_required": False,
            "comments_deferred_until_catalog_complete": False,
            "attached_followups": {},
            **_catalog_stage_graph_metadata(
                selected_tasks=normalized_selected_tasks,
                effective_selected_tasks=effective_selected_tasks,
                detail_status=str((catalog_result or {}).get("status") or "").strip().lower() or "pending",
                comments_status=_comments_status_from_posts_stage(
                    platform=normalized_platform,
                    effective_selected_tasks=effective_selected_tasks,
                    job_status=str((catalog_result or {}).get("status") or "").strip().lower() or "pending",
                ),
                media_status=(
                    str((catalog_result or {}).get("status") or "").strip().lower() or "pending"
                    if "media" in effective_selected_tasks
                    else "skipped"
                ),
                finalization_status="completed",
                timing={
                    "coverage_ms": coverage_ms,
                    "catalog_launch_ms": catalog_launch_ms,
                    "comments_launch_ms": comments_launch_ms,
                    "total_ms": round((time_module.perf_counter() - launch_started_at) * 1000, 1),
                },
            ),
        }

    launch_group_id = str(launch_group_id_override or uuid4())
    catalog_result: dict[str, Any] | None = None
    comments_result: dict[str, Any] | None = None
    deferred_comments_followup: dict[str, Any] | None = None
    attached_followups: dict[str, dict[str, Any]] = {}
    post_details_skipped_reason: str | None = None
    target_readiness: dict[str, Any] | None = None
    comments_blocker_reasons: list[str] = []
    comments_started_before_detail_complete = False
    if normalized_platform == "instagram":
        coverage_started_at = time_module.perf_counter()
        use_fast_existing_posts_launch_state = bool(
            existing_catalog_run_id
            and bounded_window_scope == "full_history"
            and "post_details" in normalized_selected_tasks
        )
        if use_fast_existing_posts_launch_state:
            materialized_posts = _materialized_social_account_total_posts(
                "instagram",
                normalized_account,
                date_start=normalized_date_start,
                date_end=normalized_date_end,
            )
            coverage = {
                "platform": "instagram",
                "account_handle": normalized_account,
                "catalog_posts": materialized_posts,
                "materialized_posts": materialized_posts,
                "expected_total_posts": materialized_posts,
                "completion_target_posts": materialized_posts,
                "missing_catalog_posts": 0,
                "missing_materialized_posts": 0,
                "detail_gap_counts": {},
                "details_complete": False,
                "bootstrap_required": materialized_posts <= 0,
                "fast_launch_state": True,
            }
        else:
            coverage = _instagram_materialization_state(
                normalized_account,
                date_start=normalized_date_start,
                date_end=normalized_date_end,
            )
        coverage_ms = round((time_module.perf_counter() - coverage_started_at) * 1000, 1)
        requires_catalog_bootstrap = bool(coverage.get("bootstrap_required"))
        if instagram_targeted_comments_only:
            requires_catalog_bootstrap = False
        effective_selected_tasks = list(normalized_selected_tasks)
        stored_post_count = max(
            _normalize_non_negative_int(coverage.get("materialized_posts")),
            _normalize_non_negative_int(coverage.get("catalog_posts")),
        )
        if set(effective_selected_tasks) == {"post_details"} and stored_post_count > 0:
            requires_catalog_bootstrap = False
        if "comments" in effective_selected_tasks:
            if instagram_targeted_comment_source_ids:
                target_count = len(instagram_targeted_comment_source_ids)
                comments_shard_count = _instagram_comments_profile_shard_count(target_count)
                target_readiness = {
                    "status": "completed",
                    "account_handle": normalized_account,
                    "saved_source_ids_count": target_count,
                    "commentable_target_count": target_count,
                    "comments_target_source_ids_count": target_count,
                    "sample_target_source_ids": instagram_targeted_comment_source_ids[:12],
                    "incomplete_comment_target_count": target_count,
                    "media_candidate_count": target_count,
                    "detail_gap_count": 0,
                    "can_start_comments": True,
                    "blocker_reasons": [],
                    "comments_blocker_reasons": [],
                    "refresh_policy": "explicit_targets",
                    "explicit_comment_anchor_source_ids": True,
                    "comments_preview": {
                        "comments_shard_count": comments_shard_count,
                        "comments_sharding_enabled": comments_shard_count > 1,
                        "recommended_comments_shard_count": _instagram_comments_recommended_shard_count(
                            target_count=target_count
                        ),
                        "target_priority": "explicit_anchor",
                    },
                    "timing_ms": coverage_ms,
                }
            elif use_fast_existing_posts_launch_state and not requires_catalog_bootstrap:
                materialized_count = _normalize_non_negative_int(coverage.get("materialized_posts"))
                comments_shard_count = _instagram_comments_profile_shard_count(materialized_count)
                target_readiness = {
                    "status": "completed",
                    "account_handle": normalized_account,
                    "saved_source_ids_count": materialized_count,
                    "commentable_target_count": materialized_count,
                    "comments_target_source_ids_count": materialized_count,
                    "incomplete_comment_target_count": materialized_count,
                    "media_candidate_count": materialized_count,
                    "detail_gap_count": 0,
                    "can_start_comments": materialized_count > 0,
                    "blocker_reasons": [],
                    "comments_blocker_reasons": [],
                    "refresh_policy": "stale_or_missing",
                    "comments_preview": {
                        "comments_shard_count": comments_shard_count,
                        "comments_sharding_enabled": comments_shard_count > 1,
                        "recommended_comments_shard_count": _instagram_comments_recommended_shard_count(
                            target_count=materialized_count
                        ),
                        "target_priority": "missing_first_recent",
                    },
                    "timing_ms": coverage_ms,
                }
            else:
                target_readiness = build_instagram_backfill_target_readiness(
                    normalized_account,
                    coverage=coverage,
                    refresh_policy="stale_or_missing",
                )
            comments_blocker_reasons = [
                str(reason or "").strip()
                for reason in list(target_readiness.get("comments_blocker_reasons") or [])
                if str(reason or "").strip()
            ]
        else:
            target_readiness = {
                "status": "completed",
                "account_handle": normalized_account,
                "saved_source_ids_count": max(
                    _normalize_non_negative_int(coverage.get("materialized_posts")),
                    _normalize_non_negative_int(coverage.get("catalog_posts")),
                ),
                "commentable_target_count": 0,
                "comments_target_source_ids_count": 0,
                "incomplete_comment_target_count": 0,
                "media_candidate_count": max(
                    _normalize_non_negative_int(coverage.get("materialized_posts")),
                    _normalize_non_negative_int(coverage.get("catalog_posts")),
                ),
                "detail_gap_count": _normalize_non_negative_int(
                    _metadata_dict(coverage.get("detail_gap_counts")).get("posts_needing_detail_refresh")
                ),
                "can_start_comments": False,
                "blocker_reasons": [],
                "comments_blocker_reasons": [],
                "timing_ms": coverage_ms,
            }
    else:
        requires_catalog_bootstrap = False
        effective_selected_tasks = list(normalized_selected_tasks)
    catalog_tasks = (
        [task for task in effective_selected_tasks if task in ("post_details", "comments", "media")]
        if requires_catalog_bootstrap
        else [task for task in effective_selected_tasks if task in ("post_details", "media")]
    )
    catalog_selected = bool(catalog_tasks)
    catalog_details_refresh_only = catalog_selected and not requires_catalog_bootstrap
    comments_deferred_until_catalog_complete = False
    media_attachment_id = _catalog_media_attachment_id(launch_group_id) if "media" in effective_selected_tasks else None
    force_detail_fetch = (
        "post_details" in effective_selected_tasks and _instagram_catalog_backfill_force_detail_fetch_enabled()
    )
    posts_auth_metadata: dict[str, Any] = {}
    public_posts_auth_metadata: dict[str, Any] = {}
    if catalog_selected and not catalog_details_refresh_only:
        posts_auth_metadata = _ensure_instagram_posts_auth_ready_for_launch(account_handle=normalized_account)
        public_posts_auth_metadata = _public_posts_launch_auth_metadata(posts_auth_metadata)
        if public_posts_auth_metadata.get("auth_repair_status") == "failed":
            return _blocked_instagram_posts_launch_payload(
                run_id=existing_catalog_run_id,
                account_handle=normalized_account,
                source_scope=source_scope,
                launch_group_id=launch_group_id,
                selected_tasks=normalized_selected_tasks,
                effective_selected_tasks=effective_selected_tasks,
                posts_auth_metadata=posts_auth_metadata,
                timing={
                    "coverage_ms": coverage_ms,
                    "total_ms": round((time_module.perf_counter() - launch_started_at) * 1000, 1),
                },
            )
    elif catalog_selected:
        public_posts_auth_metadata = {
            "auth_repair_status": "skipped",
            "auth_repair_reason": "details_refresh_only_existing_catalog",
            "auth_repair_attempted": False,
        }

    if not catalog_selected and not any(task in effective_selected_tasks for task in ("comments", "media")):
        no_work_payload = _complete_catalog_launch_no_work(
            run_id=existing_catalog_run_id,
            platform=normalized_platform,
            account_handle=normalized_account,
            launch_group_id=launch_group_id,
            selected_tasks=normalized_selected_tasks,
            effective_selected_tasks=effective_selected_tasks,
            post_details_skipped_reason=post_details_skipped_reason,
        )
        no_work_payload.update(
            _catalog_stage_graph_metadata(
                selected_tasks=normalized_selected_tasks,
                effective_selected_tasks=effective_selected_tasks,
                detail_status="skipped",
                comments_status="skipped",
                media_status="skipped",
                enrichment_status="skipped",
                finalization_status="completed",
                timing={
                    "coverage_ms": coverage_ms,
                    "total_ms": round((time_module.perf_counter() - launch_started_at) * 1000, 1),
                },
            )
        )
        logger.info(
            (
                "[catalog-launch] launch_complete_no_work platform=%s account=%s run_id=%s "
                "existing_run_id=%s coverage_ms=%.1f total_ms=%.1f selected_tasks=%s "
                "effective_selected_tasks=%s reason=%s"
            ),
            normalized_platform,
            normalized_account,
            no_work_payload.get("run_id"),
            str(existing_catalog_run_id or "").strip() or None,
            coverage_ms,
            round((time_module.perf_counter() - launch_started_at) * 1000, 1),
            normalized_selected_tasks,
            effective_selected_tasks,
            no_work_payload.get("no_work_reason"),
        )
        return no_work_payload

    if catalog_selected:
        catalog_launch_started_at = time_module.perf_counter()
        catalog_result = _room_callable(
            "start_social_account_catalog_backfill",
            start_social_account_catalog_backfill,
        )(
            normalized_platform,
            normalized_account,
            source_scope=source_scope,
            date_start=normalized_date_start,
            date_end=normalized_date_end,
            initiated_by=initiated_by,
            inline_worker_id=inline_worker_id,
            allow_local_dev_inline_bypass=allow_local_dev_inline_bypass,
            execution_preference=execution_preference,
            catalog_action=normalized_catalog_action,
            catalog_action_scope=normalized_catalog_action_scope,
            social_account_post_details_only=catalog_details_refresh_only,
            details_refresh_skip_detail_fetch="post_details" not in catalog_tasks,
            details_refresh_force_detail_fetch=force_detail_fetch,
            details_refresh_skip_media_followups="media" not in catalog_tasks,
            selected_tasks=normalized_selected_tasks,
            effective_selected_tasks=effective_selected_tasks,
            launch_group_id=launch_group_id,
            existing_run_id=existing_catalog_run_id,
        )
        catalog_launch_ms = round((time_module.perf_counter() - catalog_launch_started_at) * 1000, 1)
        if media_attachment_id:
            attached_followups["media"] = _build_attached_media_followup(
                attachment_id=media_attachment_id,
                source="catalog_media_mirror",
                status=str((catalog_result or {}).get("status") or "queued").strip().lower() or "queued",
            )

    if "comments" in effective_selected_tasks:
        comments_launch_started_at = time_module.perf_counter()
        can_start_comments_from_targets = bool(
            normalized_platform == "instagram" and _metadata_dict(target_readiness).get("can_start_comments")
        )
        defer_comments_until_catalog_complete = bool(
            catalog_result is not None
            and (
                not can_start_comments_from_targets
                or bool(allow_local_dev_inline_bypass)
                or normalized_execution_preference == "prefer_local_inline"
            )
        )
        if defer_comments_until_catalog_complete:
            comments_deferred_until_catalog_complete = True
            catalog_run_id = str((catalog_result or {}).get("run_id") or "").strip()
            catalog_run_config = _metadata_dict((catalog_result or {}).get("config"))
            if catalog_run_id and not catalog_run_config:
                try:
                    catalog_run_row = _load_catalog_run_row_by_id(catalog_run_id)
                    catalog_run_config = _metadata_dict(catalog_run_row.get("config"))
                except pg.DatabaseServiceUnavailableError as exc:
                    logger.warning(
                        "Continuing catalog comments followup without loaded catalog run config: run_id=%s error=%s",
                        catalog_run_id,
                        exc,
                    )
            deferred_comments_followup = {
                "state": "pending",
                "platform": normalized_platform,
                "account_handle": normalized_account,
                "source_scope": source_scope,
                "refresh_policy": "stale_or_missing",
                "comments_enable_media_followups": "media" in effective_selected_tasks,
                "allow_local_dev_inline_bypass": bool(allow_local_dev_inline_bypass),
                "launch_group_id": launch_group_id,
                "runtime_version": _metadata_dict(catalog_run_config.get("required_runtime_version"))
                or dict(_resolve_effective_runtime_version(required_execution_backend="modal")),
                "created_by_runtime_version": _metadata_dict(catalog_run_config.get("created_by_runtime_version"))
                or dict(_resolve_runtime_version_stamp()),
            }
            attached_followups["comments"] = _build_attached_comments_followup(
                run_id=None,
                status="pending",
                source="deferred_after_catalog",
                state="pending",
            )
            if catalog_run_id and deferred_comments_followup:
                _merge_catalog_run_config(
                    run_id=catalog_run_id,
                    metadata_updates={
                        "selected_tasks": normalized_selected_tasks,
                        "effective_selected_tasks": effective_selected_tasks,
                        **public_posts_auth_metadata,
                        "deferred_comments_followup": deferred_comments_followup,
                        "attached_followups": attached_followups,
                        **_catalog_stage_graph_metadata(
                            selected_tasks=normalized_selected_tasks,
                            effective_selected_tasks=effective_selected_tasks,
                            target_readiness=target_readiness,
                            detail_status=str((catalog_result or {}).get("status") or "").strip().lower() or "pending",
                            comments_status="pending",
                            comments_blocker_reasons=comments_blocker_reasons,
                            media_status=(
                                str((catalog_result or {}).get("status") or "").strip().lower() or "pending"
                                if "media" in effective_selected_tasks
                                else "skipped"
                            ),
                            enrichment_status="pending",
                            finalization_status="pending",
                            timing={
                                "coverage_ms": coverage_ms,
                                "catalog_launch_ms": catalog_launch_ms,
                                "comments_launch_ms": comments_launch_ms,
                                "total_ms": round((time_module.perf_counter() - launch_started_at) * 1000, 1),
                            },
                        ),
                    },
                )
        else:
            comments_source = "new_run"
            try:
                comments_result = _room_callable(
                    "start_social_account_comments_scrape",
                    _start_comments_scrape,
                )(
                    normalized_platform,
                    normalized_account,
                    mode="profile",
                    source_scope=source_scope,
                    max_posts=None,
                    max_comments_per_post=None,
                    refresh_policy="stale_or_missing",
                    initiated_by=initiated_by,
                    inline_worker_id=None if catalog_result else inline_worker_id,
                    allow_local_dev_inline_bypass=allow_local_dev_inline_bypass,
                    comments_enable_media_followups="media" in effective_selected_tasks,
                    launch_group_id=launch_group_id,
                    skip_launch_auth_probe=bool(catalog_result) or bool(instagram_targeted_comment_source_ids),
                    target_source_ids=instagram_targeted_comment_source_ids or None,
                )
            except SocialIngestConflictError as exc:
                if exc.code == "SOCIAL_ACCOUNT_COMMENTS_LAUNCH_IN_PROGRESS":
                    comments_result = {
                        "run_id": None,
                        "status": str(exc.detail.get("status") or "pending").strip().lower() or "pending",
                        "launch_in_progress": True,
                    }
                    comments_source = "launch_in_progress"
                    logger.info(
                        (
                            "[catalog-launch] comments launch already in progress platform=%s "
                            "account=%s catalog_run_id=%s"
                        ),
                        normalized_platform,
                        normalized_account,
                        str((catalog_result or {}).get("run_id") or "").strip() or None,
                    )
                elif exc.code != "SOCIAL_ACCOUNT_COMMENTS_RUN_ALREADY_ACTIVE":
                    raise
                else:
                    active_comments_run_id = str(exc.detail.get("run_id") or "").strip()
                    if not active_comments_run_id:
                        raise
                    comments_result = {
                        "run_id": active_comments_run_id,
                        "status": str(exc.detail.get("status") or "running").strip().lower() or "running",
                        "reused_active_run": True,
                    }
                    comments_source = "reused_run"
                    logger.info(
                        (
                            "[catalog-launch] reusing active comments run platform=%s account=%s "
                            "catalog_run_id=%s comments_run_id=%s"
                        ),
                        normalized_platform,
                        normalized_account,
                        str((catalog_result or {}).get("run_id") or "").strip() or None,
                        active_comments_run_id,
                    )
                    catalog_run_id_for_media = str((catalog_result or {}).get("run_id") or "").strip() or None
                    if media_attachment_id and catalog_run_id_for_media:
                        try:
                            media_followup = _enqueue_instagram_account_media_repair_jobs(
                                run_id=catalog_run_id_for_media,
                                source_scope=source_scope,
                                account_handle=normalized_account,
                            )
                            media_followup_status = (
                                "queued" if media_followup["enqueued_job_count"] > 0 else "completed"
                            )
                        except Exception:
                            logger.exception(
                                (
                                    "[catalog-launch] media_followup_enqueue_failed platform=%s account=%s "
                                    "catalog_run_id=%s comments_run_id=%s"
                                ),
                                normalized_platform,
                                normalized_account,
                                catalog_run_id_for_media,
                                active_comments_run_id,
                            )
                            media_followup = {
                                "enqueued_job_ids": [],
                                "enqueued_job_count": 0,
                            }
                            media_followup_status = "failed"
                        attached_followups["media"] = _build_attached_media_followup(
                            attachment_id=media_attachment_id,
                            source="catalog_media_mirror",
                            status=media_followup_status,
                            enqueued_job_ids=media_followup["enqueued_job_ids"],
                            enqueued_job_count=media_followup["enqueued_job_count"],
                        )
            attached_followups["comments"] = _build_attached_comments_followup(
                run_id=str((comments_result or {}).get("run_id") or "").strip() or None,
                status=str((comments_result or {}).get("status") or "").strip().lower() or "pending",
                source=comments_source,
            )
            comments_started_before_detail_complete = bool(catalog_result is not None and comments_result)
            if media_attachment_id and comments_source != "reused_run":
                attached_followups["media"] = _build_attached_media_followup(
                    attachment_id=media_attachment_id,
                    source="comments_media_followups",
                    status=str((comments_result or {}).get("status") or "").strip().lower() or "pending",
                )
        comments_launch_ms = round((time_module.perf_counter() - comments_launch_started_at) * 1000, 1)

    catalog_run_id = str((catalog_result or {}).get("run_id") or "").strip() or None
    comments_run_id = str((comments_result or {}).get("run_id") or "").strip() or None
    comments_auth_metadata = _catalog_comments_auth_metadata(comments_result)
    if catalog_run_id and not comments_deferred_until_catalog_complete:
        catalog_metadata_updates: dict[str, Any] = {
            "selected_tasks": normalized_selected_tasks,
            "effective_selected_tasks": effective_selected_tasks,
        }
        if public_posts_auth_metadata:
            catalog_metadata_updates.update(public_posts_auth_metadata)
        if comments_run_id:
            catalog_metadata_updates["comments_run_id"] = comments_run_id
        if comments_auth_metadata:
            catalog_metadata_updates.update(comments_auth_metadata)
        if attached_followups:
            catalog_metadata_updates["attached_followups"] = attached_followups
        catalog_metadata_updates.update(
            _catalog_stage_graph_metadata(
                selected_tasks=normalized_selected_tasks,
                effective_selected_tasks=effective_selected_tasks,
                target_readiness=target_readiness,
                detail_status=str((catalog_result or {}).get("status") or "").strip().lower() or "pending",
                comments_status=str((comments_result or {}).get("status") or "").strip().lower() or "pending",
                comments_blocker_reasons=comments_blocker_reasons,
                media_status=(
                    str((catalog_result or {}).get("status") or "").strip().lower() or "pending"
                    if "media" in effective_selected_tasks
                    else "skipped"
                ),
                enrichment_status="pending",
                finalization_status="pending",
                timing={
                    "coverage_ms": coverage_ms,
                    "catalog_launch_ms": catalog_launch_ms,
                    "comments_launch_ms": comments_launch_ms,
                    "total_ms": round((time_module.perf_counter() - launch_started_at) * 1000, 1),
                },
            )
        )
        if comments_started_before_detail_complete:
            catalog_metadata_updates["comments_started_before_detail_complete"] = True
        _merge_catalog_run_config(
            run_id=catalog_run_id,
            metadata_updates=catalog_metadata_updates,
        )

    primary_run_id = (
        str((catalog_result or {}).get("run_id") or (comments_result or {}).get("run_id") or "").strip() or None
    )
    primary_status = (
        str((catalog_result or {}).get("status") or (comments_result or {}).get("status") or "").strip() or None
    )
    logger.info(
        (
            "[catalog-launch] launch_complete platform=%s account=%s run_id=%s "
            "existing_run_id=%s coverage_ms=%.1f catalog_launch_ms=%.1f "
            "comments_launch_ms=%.1f total_ms=%.1f selected_tasks=%s "
            "effective_selected_tasks=%s"
        ),
        normalized_platform,
        normalized_account,
        primary_run_id,
        str(existing_catalog_run_id or "").strip() or None,
        coverage_ms,
        catalog_launch_ms,
        comments_launch_ms,
        round((time_module.perf_counter() - launch_started_at) * 1000, 1),
        normalized_selected_tasks,
        effective_selected_tasks,
    )
    payload = {
        "run_id": primary_run_id,
        "status": primary_status,
        "platform": normalized_platform,
        "account_handle": normalized_account,
        "launch_group_id": launch_group_id,
        "selected_tasks": normalized_selected_tasks,
        "effective_selected_tasks": effective_selected_tasks,
        "post_details_skipped_reason": post_details_skipped_reason,
        "catalog_run_id": catalog_run_id,
        "comments_run_id": comments_run_id,
        "catalog_status": str((catalog_result or {}).get("status") or "").strip() or None,
        "comments_status": str((comments_result or {}).get("status") or "").strip() or None,
        "catalog_action": normalized_catalog_action,
        "catalog_action_scope": normalized_catalog_action_scope,
        "catalog_bootstrap_required": requires_catalog_bootstrap if catalog_selected else False,
        "comments_deferred_until_catalog_complete": comments_deferred_until_catalog_complete,
        "attached_followups": attached_followups,
        "comments_started_before_detail_complete": comments_started_before_detail_complete,
        **public_posts_auth_metadata,
        **_catalog_stage_graph_metadata(
            selected_tasks=normalized_selected_tasks,
            effective_selected_tasks=effective_selected_tasks,
            target_readiness=target_readiness,
            detail_status=str((catalog_result or {}).get("status") or "").strip().lower()
            or ("pending" if catalog_selected else "skipped"),
            comments_status=str((comments_result or {}).get("status") or "").strip().lower()
            or ("pending" if "comments" in effective_selected_tasks else "skipped"),
            comments_blocker_reasons=comments_blocker_reasons,
            media_status=(
                str((catalog_result or {}).get("status") or "").strip().lower() or "pending"
                if "media" in effective_selected_tasks
                else "skipped"
            ),
            enrichment_status="pending" if catalog_selected else "skipped",
            finalization_status="pending" if catalog_selected else "completed",
            timing={
                "coverage_ms": coverage_ms,
                "catalog_launch_ms": catalog_launch_ms,
                "comments_launch_ms": comments_launch_ms,
                "total_ms": round((time_module.perf_counter() - launch_started_at) * 1000, 1),
            },
        ),
    }
    if comments_auth_metadata:
        payload.update(comments_auth_metadata)
    return payload


_LOCAL_ROOM_NAMES = {
    "start_social_account_catalog_backfill",
    "begin_social_account_catalog_backfill_launch",
    "finalize_social_account_catalog_backfill_launch",
    "launch_social_account_catalog_backfill",
}
_LOCAL_ROOM_FUNCTIONS = {_name: globals()[_name] for _name in _LOCAL_ROOM_NAMES}
_CORE_ROOM_WRAPPERS = {_name: getattr(_core, _name, None) for _name in _LOCAL_ROOM_NAMES}
__all__ = [
    "start_social_account_catalog_backfill",
    "begin_social_account_catalog_backfill_launch",
    "finalize_social_account_catalog_backfill_launch",
    "launch_social_account_catalog_backfill",
]
