# ruff: noqa: F821
"""Catalog run progress read models for social account backfill runs."""

from __future__ import annotations

from typing import Any

import trr_backend.socials.social_season_analytics_impl as _core

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


def _catalog_progress_stage_graph_payload(
    *,
    run_config: Mapping[str, Any],
    stages_payload: Mapping[str, Any],
    job_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    stage_graph = _metadata_dict(run_config.get("stage_graph"))
    target_readiness = _metadata_dict(run_config.get("target_readiness"))
    timing = _metadata_dict(run_config.get("timing"))
    queue_drain: dict[str, dict[str, int]] = {}
    for stage_name, raw_stage in _metadata_dict(stages_payload).items():
        stage = _metadata_dict(raw_stage)
        waiting = _normalize_non_negative_int(stage.get("jobs_waiting"))
        active = _normalize_non_negative_int(stage.get("jobs_active"))
        remaining = max(
            _normalize_non_negative_int(stage.get("jobs_total"))
            - _normalize_non_negative_int(stage.get("jobs_completed"))
            - _normalize_non_negative_int(stage.get("jobs_failed")),
            0,
        )
        if waiting > 0 or active > 0 or remaining > 0:
            queue_drain[str(stage_name)] = {
                "jobs_waiting": waiting,
                "jobs_active": active,
                "jobs_remaining": remaining,
            }

    first_auth_failure_at = None
    first_auth_failure_code = None
    auth_failure_codes = {
        "instagram_graphql_checkpoint_required",
        "instagram_graphql_cursor_forbidden",
        "instagram_graphql_cursor_unauthorized",
        "checkpoint_required",
        "auth_probe_failed",
    }
    for row in job_rows:
        metadata = _metadata_dict(row.get("metadata"))
        code = (
            str(row.get("last_error_code") or metadata.get("error_code") or metadata.get("auth_reason") or "")
            .strip()
            .lower()
        )
        message = str(row.get("error_message") or metadata.get("error_message") or "").strip().lower()
        is_auth_failure = code in auth_failure_codes or "checkpoint" in message or "unauthorized" in message
        if not is_auth_failure:
            continue
        timestamp = _coerce_dt(row.get("completed_at") or row.get("started_at") or row.get("created_at"))
        if timestamp is None:
            continue
        if first_auth_failure_at is None or timestamp < first_auth_failure_at:
            first_auth_failure_at = timestamp
            first_auth_failure_code = code or None

    payload: dict[str, Any] = {}
    if stage_graph:
        payload["stage_graph"] = stage_graph
    if target_readiness:
        payload["target_readiness"] = target_readiness
    if timing:
        payload["stage_timing"] = timing
    if queue_drain:
        payload["queue_drain_estimate"] = queue_drain
    if first_auth_failure_at is not None:
        payload["first_auth_failure_at"] = _iso(first_auth_failure_at)
        payload["first_auth_failure_code"] = first_auth_failure_code
    return payload


def _catalog_posts_runtime_additive_payload(
    *,
    platform: str,
    account_handle: str,
    run_id: str,
    run_config: Mapping[str, Any],
    job_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    if _normalize_social_account_profile_platform(platform) != "instagram":
        return {}

    pagination_state = (
        run_config.get("pagination_state")
        if isinstance(run_config.get("pagination_state"), dict)
        else latest_instagram_profile_pagination_state(
            account_handle=account_handle,
            source_scope=str(run_config.get("source_scope") or "network"),
            run_id=run_id,
            direction="forward",
        )
    )
    if not isinstance(pagination_state, Mapping):
        pagination_state = {}

    latest_fetcher_runtime: dict[str, Any] = {}
    inline_comments_upserted = 0
    for row in job_rows:
        metadata = _metadata_dict(row.get("metadata"))
        fetcher_runtime = _metadata_dict(metadata.get("fetcher_runtime") or metadata.get("runtime_metadata"))
        if fetcher_runtime:
            latest_fetcher_runtime = fetcher_runtime
        persist_counters = _metadata_dict(metadata.get("persist_counters"))
        inline_comments_upserted += _normalize_non_negative_int(
            metadata.get("inline_comments_upserted")
            or persist_counters.get("inline_comments_upserted")
            or _metadata_dict(metadata.get("posts_scrapling_persist_diagnostics")).get("inline_comments_upserted")
        )

    stop_reason = (
        str(_metadata_dict(pagination_state).get("stop_reason") or "").strip().lower()
        or str(latest_fetcher_runtime.get("fetch_reason") or "").strip().lower()
        or None
    )
    profile_posts_doc_id_metadata = _metadata_dict(latest_fetcher_runtime.get("profile_posts_doc_ids"))
    doc_id_used = (
        str(_metadata_dict(pagination_state).get("doc_id_used") or "").strip()
        or str(
            latest_fetcher_runtime.get("doc_id_used")
            or latest_fetcher_runtime.get("profile_posts_doc_id_used")
            or profile_posts_doc_id_metadata.get("used")
            or profile_posts_doc_id_metadata.get("final_selected")
            or ""
        ).strip()
        or None
    )
    doc_ids_attempted = (
        _metadata_dict(pagination_state).get("doc_ids_attempted")
        if isinstance(_metadata_dict(pagination_state).get("doc_ids_attempted"), list)
        else profile_posts_doc_id_metadata.get("attempted")
        if isinstance(profile_posts_doc_id_metadata.get("attempted"), list)
        else latest_fetcher_runtime.get("doc_ids_attempted")
    )
    runtime_proxy_pacing = _metadata_dict(latest_fetcher_runtime.get("proxy_pacing"))
    proxy_fingerprint = (
        str(_metadata_dict(pagination_state).get("proxy_fingerprint") or "").strip()
        or str(latest_fetcher_runtime.get("proxy_fingerprint") or "").strip()
        or str(latest_fetcher_runtime.get("selected_proxy_fingerprint") or "").strip()
        or None
    )
    feature_flags = instagram_posts_acceleration_flags()
    stage_graph = _metadata_dict(run_config.get("stage_graph"))
    detail_stage = _metadata_dict(stage_graph.get("detail_refresh"))
    target_readiness = _metadata_dict(run_config.get("target_readiness"))
    details_progress = _metadata_dict(run_config.get("detail_refresh") or run_config.get("details_progress"))
    if not details_progress:
        details_progress = {
            "phase": "details_refresh",
            "status": str(detail_stage.get("status") or "").strip().lower() or None,
            "selected": bool(detail_stage.get("selected")),
            "blocker_reasons": list(detail_stage.get("blocker_reasons") or []),
            "detail_gap_count": _normalize_non_negative_int(
                detail_stage.get("detail_gap_count") or target_readiness.get("detail_gap_count")
            ),
            "source": "stage_graph",
        }
    listing_progress = {
        "page_index": _normalize_non_negative_int(_metadata_dict(pagination_state).get("page_index")),
        "posts_seen": _normalize_non_negative_int(_metadata_dict(pagination_state).get("posts_seen")),
        "posts_upserted": _normalize_non_negative_int(_metadata_dict(pagination_state).get("posts_upserted")),
        "end_cursor": str(_metadata_dict(pagination_state).get("end_cursor") or "").strip() or None,
        "partial": bool(_metadata_dict(pagination_state).get("partial")) if pagination_state else None,
        "stop_reason": stop_reason,
    }
    bidirectional_probe = (
        run_config.get("bidirectional_probe")
        if isinstance(run_config.get("bidirectional_probe"), dict)
        else latest_fetcher_runtime.get("bidirectional_probe")
        if isinstance(latest_fetcher_runtime.get("bidirectional_probe"), dict)
        else None
    )
    warmup_pool = (
        run_config.get("warmup_pool")
        if isinstance(run_config.get("warmup_pool"), dict)
        else latest_fetcher_runtime.get("warmup_pool")
        if isinstance(latest_fetcher_runtime.get("warmup_pool"), dict)
        else None
    )
    return {
        "posts_acceleration_flags": feature_flags,
        "pagination_state": dict(pagination_state) if isinstance(pagination_state, Mapping) else {},
        "resume_cursor_saved": bool(_metadata_dict(pagination_state).get("end_cursor")),
        "listing_progress": listing_progress,
        "details_progress": details_progress,
        "inline_comments_upserted": inline_comments_upserted,
        "doc_id_used": doc_id_used,
        "profile_posts_doc_ids": doc_ids_attempted if isinstance(doc_ids_attempted, list) else [],
        "pagination_doc_id_stale": stop_reason == "pagination_doc_id_stale",
        "proxy_pacing": {
            **runtime_proxy_pacing,
            "enabled": bool(_metadata_dict(feature_flags.get("flags")).get("per_ip_pacing_enabled")),
            "proxy_fingerprint": proxy_fingerprint,
            "proxy_session_key": str(_metadata_dict(pagination_state).get("proxy_session_key") or "").strip()
            or str(latest_fetcher_runtime.get("proxy_session_key") or "").strip()
            or None,
        },
        "warmup_pool": warmup_pool,
        "bidirectional_probe": bidirectional_probe,
    }


def _build_catalog_terminal_progress_payload(
    *,
    run_row: Mapping[str, Any],
    job_rows: Sequence[Mapping[str, Any]],
    run_id: str,
    run_config: Mapping[str, Any],
    platform: str,
    account_handle: str,
    recent_log_limit: int,
) -> dict[str, Any]:
    payload = _build_terminal_catalog_run_progress_payload(
        run_row=run_row,
        job_rows=list(job_rows),
        run_id=run_id,
        run_config=run_config,
        platform=platform,
        account_handle=account_handle,
        recent_log_limit=recent_log_limit,
    )
    payload.update(
        _catalog_progress_stage_graph_payload(
            run_config=run_config,
            stages_payload=payload.get("stages") or {},
            job_rows=job_rows,
        )
    )
    payload.update(
        _catalog_posts_runtime_additive_payload(
            platform=platform,
            account_handle=account_handle,
            run_id=run_id,
            run_config=run_config,
            job_rows=job_rows,
        )
    )
    payload["launch_group_id"] = str(run_config.get("launch_group_id") or "").strip() or None
    payload["launch_state"] = str(run_config.get("launch_state") or "").strip().lower() or None
    payload["selected_tasks"] = _normalize_optional_social_account_catalog_backfill_selected_tasks(
        run_config.get("selected_tasks")
    )
    payload["effective_selected_tasks"] = (
        _normalize_optional_social_account_catalog_backfill_selected_tasks(run_config.get("effective_selected_tasks"))
        or payload["selected_tasks"]
    )
    payload["comments_run_id"] = str(run_config.get("comments_run_id") or "").strip() or None
    for key in (
        "posts_auth_probe",
        "auth_repair_attempted",
        "auth_repair_status",
        "auth_repair_reason",
        "partial_scrape",
        "stop_reason",
    ):
        if key in run_config:
            payload[key] = run_config.get(key)

    launch_state = str(payload.get("launch_state") or "").strip().lower()
    stop_reason = str(payload.get("stop_reason") or "").strip().lower()
    if launch_state == "blocked_auth" or stop_reason in {"posts_auth_blocked", "checkpoint_required"}:
        repair_environment = _catalog_run_auth_repair_environment(platform)
        repairable_reason = (
            str(run_config.get(_RUN_AUTH_REPAIR_REPAIRABLE_REASON_KEY) or "").strip().lower()
            or str(run_config.get("auth_repair_reason") or "").strip().lower()
            or str(run_config.get("blocked_reason") or "").strip().lower()
            or stop_reason
            or "posts_auth_blocked"
        )
        repair_status = (
            str(run_config.get(_RUN_AUTH_REPAIR_STATUS_KEY) or "").strip().lower()
            or str(run_config.get("auth_repair_status") or "").strip().lower()
            or "idle"
        )
        payload["run_state"] = "failed"
        payload["operational_state"] = "blocked_auth"
        payload["repair_action"] = str(repair_environment.get("repair_action") or "").strip().lower() or None
        payload["repair_status"] = repair_status
        payload["repairable_reason"] = repairable_reason
        payload["auto_resume_pending"] = bool(run_config.get(_RUN_AUTH_REPAIR_AUTO_RESUME_PENDING_KEY))
        payload["resume_stage"] = str(run_config.get(_RUN_AUTH_REPAIR_RESUME_STAGE_KEY) or "").strip().lower() or "posts"
        payload["repair_environment"] = repair_environment
        diagnostics = _metadata_dict(payload.get("run_diagnostics"))
        payload["run_diagnostics"] = {
            **diagnostics,
            "frontier_stop_reason": stop_reason or diagnostics.get("frontier_stop_reason"),
            "last_error_code": diagnostics.get("last_error_code") or repairable_reason,
            "last_error_message": diagnostics.get("last_error_message"),
        }
    return payload


def get_social_account_catalog_run_progress(
    platform: str,
    account_handle: str,
    run_id: str,
    *,
    recent_log_limit: int = 20,
    fast: bool = False,
) -> dict[str, Any]:
    _sync_core_overrides()
    safe_recent_log_limit = max(1, min(int(recent_log_limit), 100))
    normalized_platform = _normalize_social_account_profile_platform(platform)
    normalized_account = _normalize_social_account_profile_handle(account_handle)
    if normalized_platform not in set(CATALOG_SUPPORTED_PLATFORMS):
        raise ValueError("Catalog backfill is not supported for this platform.")
    if fast:
        features = {"has_run_id": True, "has_queue_fields": True}
    else:
        if not _relation_exists("social.scrape_runs") or not _relation_exists("social.scrape_jobs"):
            raise ValueError("social_ingest_queue_schema_missing")
        features = _scrape_jobs_features()
        if not bool(features.get("has_run_id")):
            raise ValueError("run_progress_requires_scrape_jobs_run_id")
    read_pool_name = SOCIAL_CATALOG_PROGRESS_POOL_NAME if fast else "default"

    def _load_run_and_jobs() -> tuple[dict[str, Any], list[dict[str, Any]]]:
        if fast:
            with pg.db_read_connection(label="catalog_run_progress_fast", pool_name=read_pool_name) as conn:
                loaded_run_row = _load_social_account_catalog_run_row(
                    platform=normalized_platform,
                    account_handle=normalized_account,
                    run_id=run_id,
                    conn=conn,
                    verify_account=False,
                    pool_name=read_pool_name,
                )
                loaded_job_rows = _load_social_account_catalog_jobs(
                    run_id=run_id,
                    platform=normalized_platform,
                    account_handle=normalized_account,
                    conn=conn,
                    features=features,
                    pool_name=read_pool_name,
                )
            return loaded_run_row, loaded_job_rows

        loaded_run_row = _load_social_account_catalog_run_row(
            platform=normalized_platform,
            account_handle=normalized_account,
            run_id=run_id,
            verify_account=True,
            pool_name=read_pool_name,
        )
        loaded_job_rows = _load_social_account_catalog_jobs(
            run_id=run_id,
            platform=normalized_platform,
            account_handle=normalized_account,
            features=features,
            pool_name=read_pool_name,
        )
        return loaded_run_row, loaded_job_rows

    try:
        run_row, job_rows = _load_run_and_jobs()
    except LookupError as exc:
        raise ValueError("run_not_found") from exc

    run_config = _metadata_dict(run_row.get("config"))
    configured_platforms = {
        _normalize_platform_name(value)
        for value in _as_text_list(run_config.get("platforms") or [])
        if _normalize_platform_name(value)
    }
    configured_accounts = {
        _normalize_social_account_profile_handle(value)
        for value in _as_text_list(run_config.get("accounts_override") or [])
        if _normalize_social_account_profile_handle(value)
    }
    if configured_platforms and normalized_platform not in configured_platforms:
        raise ValueError("run_not_found")
    if configured_accounts and normalized_account not in configured_accounts:
        raise ValueError("run_not_found")

    if not job_rows:
        run_config = _metadata_dict(run_row.get("config"))
        launch_state = str(run_config.get("launch_state") or "").strip().lower()
        task_pending = _catalog_launch_task_resolution_pending(run_config.get("launch_task_resolution_pending"))
        created_at = _coerce_dt(run_row.get("created_at"))
        pending_age_seconds = (_now_utc() - created_at).total_seconds() if created_at is not None else None
        fresh_pending_launch = (
            task_pending
            and launch_state in {"pending", "finalizing"}
            and (
                not _catalog_launch_finalizing_is_stale(run_config)
                or (
                    pending_age_seconds is not None
                    and pending_age_seconds < _CATALOG_LAUNCH_FINALIZING_RECOVERY_GRACE_SECONDS
                )
            )
        )
        terminal_zero_job_launch = launch_state in {"blocked_auth", "completed_no_work"}
        recovery_result: dict[str, Any] = {"recovered": False, "reason": "awaiting_finalize"}
        if not fresh_pending_launch and not terminal_zero_job_launch:
            recovery_result = recover_pending_social_account_catalog_launch(
                platform=normalized_platform,
                account_handle=normalized_account,
                run_id=run_id,
            )
        if bool(recovery_result.get("recovered")):
            try:
                run_row, job_rows = _load_run_and_jobs()
            except LookupError as exc:
                raise ValueError("run_not_found") from exc
            run_config = _metadata_dict(run_row.get("config"))
        if not job_rows:
            launch_state = str(run_config.get("launch_state") or "").strip().lower()
            no_work_reason = str(run_config.get("no_work_reason") or "").strip()
            recovery_reason = str(recovery_result.get("reason") or "").strip().lower()
            if (
                launch_state == "completed_no_work"
                or launch_state == "blocked_auth"
                or no_work_reason
                or (recovery_reason == "awaiting_finalize" and launch_state in {"pending", "finalizing"})
                or (recovery_reason == "finalize_in_progress" and launch_state == "finalizing")
            ):
                return _build_catalog_terminal_progress_payload(
                    run_row=run_row,
                    job_rows=[],
                    run_id=run_id,
                    run_config=run_config,
                    platform=normalized_platform,
                    account_handle=normalized_account,
                    recent_log_limit=safe_recent_log_limit,
                )
            raise ValueError("run_not_found")

    if fast:
        return _build_catalog_terminal_progress_payload(
            run_row=run_row,
            job_rows=job_rows,
            run_id=run_id,
            run_config=run_config,
            platform=normalized_platform,
            account_handle=normalized_account,
            recent_log_limit=safe_recent_log_limit,
        )

    repaired_run_config = _repair_finalizing_catalog_launch_after_jobs(
        run_row=run_row,
        job_rows=job_rows,
        platform=normalized_platform,
        account_handle=normalized_account,
    )
    if repaired_run_config:
        run_config = repaired_run_config
        run_row = {**dict(run_row), "config": run_config}

    computed_summary = _summarize_run_progress_job_rows(job_rows)
    single_target_run = (not configured_platforms or configured_platforms == {normalized_platform}) and (
        not configured_accounts or configured_accounts == {normalized_account}
    )
    summary_override: Mapping[str, Any] | None = None
    if single_target_run:
        stored_summary = _metadata_dict(run_row.get("summary"))
        run_status = str(run_row.get("status") or "").strip().lower()
        if _run_progress_summary_needs_refresh(stored_summary, computed_summary) or (
            run_status in _RUN_PROGRESS_ACTIVE_JOB_STATUSES and computed_summary["active_jobs"] == 0
        ):
            if run_status in _RUN_PROGRESS_ACTIVE_JOB_STATUSES and computed_summary["active_jobs"] == 0:
                _finalize_run_status(run_id, force_recompute=True)
            else:
                _update_run_summary(run_id, force_recompute=True)
            refreshed_run_row = pg.fetch_one(
                """
                select
                  id::text as run_id,
                  season_id::text as season_id,
                  status,
                  source_scope,
                  config,
                  summary,
                  created_at,
                  started_at,
                  completed_at
                from social.scrape_runs
                where id = %s::uuid
                  and coalesce(config->>'pipeline_ingest_mode', '') = %s
                limit 1
                """,
                [run_id, SHARED_ACCOUNT_CATALOG_BACKFILL_INGEST_MODE],
            )
            if refreshed_run_row:
                run_row = refreshed_run_row
        refreshed_summary = _metadata_dict(run_row.get("summary"))
        if _run_progress_summary_needs_refresh(refreshed_summary, computed_summary):
            summary_override = computed_summary
    else:
        summary_override = computed_summary

    if _can_fast_path_terminal_catalog_progress(
        run_row=run_row,
        configured_platforms=configured_platforms,
        configured_accounts=configured_accounts,
        normalized_platform=normalized_platform,
        normalized_account=normalized_account,
    ):
        return _build_catalog_terminal_progress_payload(
            run_row=run_row,
            job_rows=job_rows,
            run_id=run_id,
            run_config=run_config,
            platform=normalized_platform,
            account_handle=normalized_account,
            recent_log_limit=safe_recent_log_limit,
        )

    _assert_social_account_profile_exists(normalized_platform, normalized_account)
    recover_stale_unclaimed_dispatched_jobs(
        run_id=run_id,
        platform=normalized_platform,
        account_handle=normalized_account,
        limit=25,
    )
    recover_dispatch_blocked_no_progress_jobs(limit=25)

    payload = _build_run_progress_snapshot_payload(
        run_row=run_row,
        job_rows=job_rows,
        run_id=run_id,
        season_id=str(run_row.get("season_id") or "") or None,
        recent_log_limit=safe_recent_log_limit,
        summary_override=summary_override,
    )
    payload.update(_catalog_run_intent_metadata(run_config))
    payload.update(
        _catalog_progress_stage_graph_payload(
            run_config=run_config,
            stages_payload=payload.get("stages") or {},
            job_rows=job_rows,
        )
    )
    payload["launch_group_id"] = str(run_config.get("launch_group_id") or "").strip() or None
    payload["launch_state"] = str(run_config.get("launch_state") or "").strip().lower() or None
    payload["selected_tasks"] = _normalize_optional_social_account_catalog_backfill_selected_tasks(
        run_config.get("selected_tasks")
    )
    payload["effective_selected_tasks"] = (
        _normalize_optional_social_account_catalog_backfill_selected_tasks(run_config.get("effective_selected_tasks"))
        or payload["selected_tasks"]
    )
    payload["comments_run_id"] = str(run_config.get("comments_run_id") or "").strip() or None
    payload.update(
        _catalog_posts_runtime_additive_payload(
            platform=normalized_platform,
            account_handle=normalized_account,
            run_id=run_id,
            run_config=run_config,
            job_rows=job_rows,
        )
    )
    payload["attached_followups"] = _resolve_run_attached_followups(
        run_config=run_config,
        run_id=run_id,
        run_status=str(run_row.get("status") or "").strip().lower() or None,
        comments_run_id=payload["comments_run_id"],
    )
    resume_state_payload = run_config.get("resume_state") if isinstance(run_config.get("resume_state"), dict) else None
    payload["resume_state"] = resume_state_payload
    partition_progress = _shared_account_partition_progress(
        run_id=run_id,
        platform=normalized_platform,
        account_handle=normalized_account,
    )
    frontier_progress = _shared_account_frontier_progress(
        run_id=run_id,
        platform=normalized_platform,
        account_handle=normalized_account,
    )
    payload["partition_strategy"] = str(
        run_config.get("partition_strategy")
        or frontier_progress.get("strategy")
        or partition_progress.get("partition_strategy")
        or ""
    )
    payload["discovery"] = partition_progress
    payload["frontier"] = frontier_progress
    post_progress = _metadata_dict(payload.get("post_progress"))
    allow_live_profile_refresh = not (
        normalized_platform == "instagram"
        and str(run_row.get("status") or "").strip().lower() in _RUN_PROGRESS_ACTIVE_JOB_STATUSES
    )
    expected_total_posts = max(
        _shared_account_expected_total_posts_from_config(
            run_config,
            platform=normalized_platform,
            account_handle=normalized_account,
        ),
        _normalize_non_negative_int(partition_progress.get("expected_total_posts")),
        _normalize_non_negative_int(frontier_progress.get("expected_total_posts")),
    )
    source_total_posts_current = _normalize_non_negative_int(
        _cached_live_profile_total_posts(normalized_platform, normalized_account)
        if allow_live_profile_refresh
        else _cached_live_profile_total_posts_cached_only(normalized_platform, normalized_account)
    )
    best_known_total_posts = _best_known_social_account_total_posts(
        normalized_platform,
        normalized_account,
        materialized_total_posts=_social_account_profile_total_posts(normalized_platform, normalized_account),
        catalog_total_posts=_shared_catalog_total_posts(normalized_platform, normalized_account),
        allow_live_refresh=allow_live_profile_refresh,
    )
    progress_total_posts = (
        max(
            expected_total_posts,
            _normalize_non_negative_int(post_progress.get("total_posts")),
            source_total_posts_current,
            best_known_total_posts,
        )
        if (
            _shared_catalog_mode(run_config)
            and _coerce_dt(run_config.get("date_start")) is None
            and _coerce_dt(run_config.get("date_end")) is None
            and str(run_config.get("catalog_action_scope") or "").strip().lower() in {"", "full_history"}
        )
        else expected_total_posts
        or _normalize_non_negative_int(post_progress.get("total_posts"))
        or best_known_total_posts
    )
    frontier_expected_total = _normalize_non_negative_int(frontier_progress.get("expected_total_posts"))
    frontier_completed_posts = _normalize_non_negative_int(frontier_progress.get("posts_checked"))
    if (
        frontier_expected_total > 0
        and bool(frontier_progress.get("exhausted"))
        and frontier_completed_posts < frontier_expected_total
    ):
        progress_total_posts = frontier_expected_total
    if progress_total_posts > 0:
        post_progress["total_posts"] = progress_total_posts
        payload["post_progress"] = post_progress
    post_progress["completed_posts"] = max(
        _normalize_non_negative_int(post_progress.get("completed_posts")),
        _normalize_non_negative_int(frontier_progress.get("posts_checked")),
    )
    post_progress["matched_posts"] = max(
        _normalize_non_negative_int(post_progress.get("matched_posts")),
        _normalize_non_negative_int(frontier_progress.get("posts_saved")),
    )
    if progress_total_posts > 0:
        post_progress["completed_posts"] = min(
            _normalize_non_negative_int(post_progress.get("completed_posts")),
            progress_total_posts,
        )
        post_progress["matched_posts"] = min(
            _normalize_non_negative_int(post_progress.get("matched_posts")),
            progress_total_posts,
        )
    payload["post_progress"] = post_progress
    payload["expected_total_posts"] = expected_total_posts or None
    payload["source_total_posts_current"] = source_total_posts_current or None
    completed_posts = _normalize_non_negative_int(post_progress.get("completed_posts"))
    total_posts_for_gap = _normalize_non_negative_int(post_progress.get("total_posts"))
    completion_gap_posts = max(total_posts_for_gap - completed_posts, 0) if total_posts_for_gap > 0 else 0
    completion_gap_reason: str | None = None
    frontier_stop_reason = (
        str(frontier_progress.get("stop_reason") or "").strip().lower()
        or str(_metadata_dict(frontier_progress.get("metadata")).get("frontier_stop_reason") or "").strip().lower()
        or None
    )
    if frontier_stop_reason == "catalog_oldest_stored_post_not_reached":
        completion_gap_reason = "history_boundary_incomplete"
    elif completion_gap_posts > 0 and bool(frontier_progress.get("exhausted")):
        if source_total_posts_current > 0 and completed_posts >= source_total_posts_current:
            completion_gap_reason = "source_total_drift"
        else:
            completion_gap_reason = "fetch_incomplete"
    payload["completion_gap_posts"] = completion_gap_posts
    payload["completion_gap_reason"] = completion_gap_reason
    stages_payload = _metadata_dict(payload.get("stages"))
    posts_stage = _metadata_dict(stages_payload.get(SHARED_ACCOUNT_POSTS_STAGE))
    classify_stage = _metadata_dict(stages_payload.get(POST_CLASSIFY_STAGE))
    posts_total = _normalize_non_negative_int(posts_stage.get("jobs_total"))
    posts_completed = _normalize_non_negative_int(posts_stage.get("jobs_completed"))
    posts_failed = _normalize_non_negative_int(posts_stage.get("jobs_failed"))
    posts_active = _normalize_non_negative_int(posts_stage.get("jobs_active"))
    posts_waiting = _normalize_non_negative_int(posts_stage.get("jobs_waiting"))
    classify_total = _normalize_non_negative_int(classify_stage.get("jobs_total"))
    classify_completed = _normalize_non_negative_int(classify_stage.get("jobs_completed"))
    classify_failed = _normalize_non_negative_int(classify_stage.get("jobs_failed"))
    classify_active = _normalize_non_negative_int(classify_stage.get("jobs_active"))
    classify_waiting = _normalize_non_negative_int(classify_stage.get("jobs_waiting"))
    dismissed_terminal_classify_cancel = (
        str(run_row.get("status") or "").strip().lower() == "completed"
        and bool(str(run_config.get(_RUN_FAILURE_DISMISSED_AT_KEY) or "").strip())
        and classify_total > 0
        and classify_active <= 0
        and classify_waiting <= 0
        and any(
            _run_progress_stage_from_row(row) == POST_CLASSIFY_STAGE
            and str(row.get("status") or "").strip().lower() == "cancelled"
            for row in job_rows
        )
    )
    payload["scrape_complete"] = (
        posts_total > 0
        and posts_completed >= posts_total
        and posts_failed <= 0
        and posts_active <= 0
        and posts_waiting <= 0
    )
    payload["classify_incomplete"] = (
        (not dismissed_terminal_classify_cancel)
        and classify_total > 0
        and (classify_completed + classify_failed < classify_total or classify_active > 0 or classify_waiting > 0)
    )
    if (
        str(payload.get("run_status") or "").strip().lower() == "completed"
        and _normalize_non_negative_int(post_progress.get("completed_posts")) <= 0
        and _normalize_non_negative_int(partition_progress.get("partition_count")) <= 0
        and _normalize_non_negative_int(
            (payload.get("stages") or {}).get(SHARED_ACCOUNT_DISCOVERY_STAGE, {}).get("jobs_completed")
        )
        > 0
        and _normalize_non_negative_int(
            (payload.get("stages") or {}).get(SHARED_ACCOUNT_POSTS_STAGE, {}).get("jobs_total")
        )
        <= 0
    ):
        payload["run_status"] = "failed"
    source_row = _load_shared_account_source_row(
        source_scope=str(run_row.get("source_scope") or payload.get("source_scope") or "network"),
        platform=normalized_platform,
        account_handle=normalized_account,
    )
    shared_profile = _shared_profile_contract(
        source_scope=str(run_row.get("source_scope") or payload.get("source_scope") or "network"),
        platform=normalized_platform,
        account_handle=normalized_account,
        metadata=_metadata_dict((source_row or {}).get("metadata")),
    )
    queued_jobs_by_type = _queued_jobs_by_type(stages_payload)
    dispatch_health = _metadata_dict(payload.get("dispatch_health"))
    recovery_payload = _shared_account_recovery_payload(job_rows=job_rows, now=_now_utc())
    payload["shared_profile"] = shared_profile
    payload["network_name"] = shared_profile["network_name"]
    payload["profile_kind"] = shared_profile["profile_kind"]
    payload["assignment_mode"] = shared_profile["assignment_mode"]
    payload["assignment_rules"] = shared_profile["assignment_rules"]
    payload["queued_jobs_by_type"] = queued_jobs_by_type
    payload["recovery"] = recovery_payload
    payload["capacity_waiting"] = (
        _normalize_non_negative_int(dispatch_health.get("modal_pending_jobs"))
        + _normalize_non_negative_int(dispatch_health.get("modal_running_unclaimed_jobs"))
    ) > 0
    payload["active_transport"] = (
        str(frontier_progress.get("transport") or "").strip().lower()
        or str((_metadata_dict(payload.get("worker_runtime")).get("active_transport")) or "").strip().lower()
        or None
    )
    payload["required_execution_backend"] = (
        str(run_config.get("required_execution_backend") or "").strip().lower() or None
    )
    payload["allow_local_dev_inline_bypass"] = bool(run_config.get("allow_local_dev_inline_bypass"))
    frontier_metadata = _metadata_dict(frontier_progress.get("metadata"))
    declared_runner_strategy = str(run_config.get("runner_strategy") or "").strip().lower() or None
    declared_partition_strategy = str(run_config.get("partition_strategy") or "").strip().lower() or None
    effective_runner_strategy = (
        str((_metadata_dict(payload.get("worker_runtime")).get("runner_strategy")) or "").strip().lower() or None
    )
    effective_partition_strategy = (
        str((_metadata_dict(payload.get("worker_runtime")).get("partition_strategy")) or "").strip().lower() or None
    )
    worker_runtime_payload = _metadata_dict(payload.get("worker_runtime"))
    observed_runtime_versions = list(worker_runtime_payload.get("runtime_versions_observed") or [])
    effective_runtime = (
        _metadata_dict(observed_runtime_versions[0])
        if observed_runtime_versions
        else _metadata_dict(worker_runtime_payload.get("runtime_version"))
    )
    effective_execution_backend = str(effective_runtime.get("execution_backend") or "").strip().lower() or None
    replacement_run_id = str(run_config.get("replacement_run_id") or "").strip() or None
    auto_requeue_status = str(run_config.get("auto_requeue_status") or "").strip().lower() or None
    cancel_reason: str | None = str(run_config.get("cancel_reason") or "").strip().lower() or None
    last_error_code = str(frontier_metadata.get("last_error_code") or "").strip().lower() or None
    last_error_message = str(frontier_metadata.get("last_error_message") or "").strip() or None
    for row in job_rows:
        row_metadata = _metadata_dict(row.get("metadata"))
        if cancel_reason is None:
            cancel_reason = str(row_metadata.get("cancel_reason") or "").strip().lower() or None
        if last_error_code is None:
            last_error_code = str(row.get("last_error_code") or "").strip().lower() or None
        if last_error_message is None:
            last_error_message = str(row.get("error_message") or "").strip() or None
        if cancel_reason and last_error_code and last_error_message:
            break
    if dismissed_terminal_classify_cancel:
        cancel_reason = None
        last_error_code = None
        last_error_message = None
    last_transport_response = _catalog_run_last_transport_response(
        frontier_progress=frontier_progress,
        job_rows=job_rows,
    )
    payload["cancel_reason"] = cancel_reason
    payload["last_error_code"] = last_error_code
    payload["last_error_message"] = last_error_message
    payload["effective_execution_backend"] = effective_execution_backend
    payload["persist_counters"] = _run_progress_persist_counters(job_rows)
    if last_transport_response:
        payload["last_transport_response"] = last_transport_response
    payload["run_state"] = _derive_catalog_run_state(
        run_status=str(payload.get("run_status") or ""),
        scrape_complete=bool(payload.get("scrape_complete")),
        classify_incomplete=bool(payload.get("classify_incomplete")),
        stages_payload=stages_payload,
        frontier_progress=frontier_progress,
        recovery=recovery_payload,
    )
    payload["alerts"] = _build_catalog_run_progress_alerts(
        platform=normalized_platform,
        frontier_progress=frontier_progress,
        payload=payload,
        recovery=recovery_payload,
    )
    repair_environment = _catalog_run_auth_repair_environment(normalized_platform)
    repairable_reason = _catalog_run_repairable_auth_reason(
        platform=normalized_platform,
        job_rows=job_rows,
        frontier_progress=frontier_progress,
        last_error_code=last_error_code,
    )
    resume_stage = _catalog_run_auth_repair_resume_stage(
        repairable_reason=repairable_reason,
        run_config=run_config,
        frontier_progress=frontier_progress,
    )
    configured_repair_status = str(run_config.get(_RUN_AUTH_REPAIR_STATUS_KEY) or "").strip().lower() or None
    repair_status = configured_repair_status or ("idle" if repairable_reason else None)
    auto_resume_pending = bool(run_config.get(_RUN_AUTH_REPAIR_AUTO_RESUME_PENDING_KEY))
    payload["operational_state"] = (
        "runtime_superseded"
        if replacement_run_id and auto_requeue_status in {"queued", "running"}
        else (
            "blocked_auth"
            if repairable_reason
            and (
                str(payload.get("run_state") or "").strip().lower() == "failed"
                or str(payload.get("run_status") or "").strip().lower() == "failed"
                or str(run_row.get("status") or "").strip().lower() == "failed"
            )
            else payload.get("run_state")
        )
    )
    payload["repair_action"] = (
        str(repair_environment.get("repair_action") or "").strip().lower() or None if repairable_reason else None
    )
    payload["repair_status"] = repair_status
    payload["repairable_reason"] = repairable_reason
    payload["auto_resume_pending"] = auto_resume_pending
    payload["resume_stage"] = resume_stage
    payload["repair_environment"] = repair_environment
    persist_diagnostics = _metadata_dict(payload.get("persist_counters"))
    payload["run_diagnostics"] = {
        "cancel_reason": cancel_reason,
        "last_error_code": last_error_code,
        "last_error_message": last_error_message,
        "posts_upserted": _normalize_non_negative_int(persist_diagnostics.get("posts_upserted")),
        "posts_skipped": _normalize_non_negative_int(persist_diagnostics.get("posts_skipped")),
        "posts_skipped_by_reason": _metadata_dict(persist_diagnostics.get("posts_skipped_by_reason")),
        "silent_drop_detected": bool(persist_diagnostics.get("silent_drop_detected")),
        "frontier_auth_reason": str(frontier_metadata.get("auth_reason") or "").strip().lower() or None,
        "frontier_stop_reason": frontier_stop_reason,
        "declared_runner_strategy": declared_runner_strategy,
        "effective_runner_strategy": effective_runner_strategy,
        "declared_partition_strategy": declared_partition_strategy,
        "effective_partition_strategy": effective_partition_strategy,
        "effective_execution_backend": effective_execution_backend,
        "required_execution_backend": payload.get("required_execution_backend"),
        "allow_local_dev_inline_bypass": bool(payload.get("allow_local_dev_inline_bypass")),
        "catalog_oldest_post_at": _iso(_coerce_dt(frontier_progress.get("catalog_oldest_post_at"))),
        "oldest_posted_at_seen": _iso(_coerce_dt(frontier_progress.get("oldest_posted_at_seen"))),
        "newest_posted_at_seen": _iso(_coerce_dt(frontier_progress.get("newest_posted_at_seen"))),
        "last_transport_response": payload.get("last_transport_response"),
        "strategy_mismatch": bool(
            (
                declared_runner_strategy
                and effective_runner_strategy
                and declared_runner_strategy != effective_runner_strategy
            )
            or (
                declared_partition_strategy
                and effective_partition_strategy
                and declared_partition_strategy != effective_partition_strategy
            )
        ),
        "runtime_version_drift": bool(worker_runtime_payload.get("runtime_version_drift")),
        "replacement_run_id": replacement_run_id,
        "auto_requeue_status": auto_requeue_status,
    }
    return payload

_LOCAL_ROOM_NAMES = {
    'get_social_account_catalog_run_progress',
}
_LOCAL_ROOM_FUNCTIONS = {_name: globals()[_name] for _name in _LOCAL_ROOM_NAMES}
_CORE_ROOM_WRAPPERS = {_name: getattr(_core, _name, None) for _name in _LOCAL_ROOM_NAMES}
__all__ = [
    'get_social_account_catalog_run_progress',
]
