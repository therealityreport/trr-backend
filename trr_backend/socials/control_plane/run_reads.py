"""Run-read models for the social control plane."""

from __future__ import annotations

from collections import Counter
from datetime import datetime
from typing import Any

import trr_backend.repositories.social_season_analytics as legacy
import trr_backend.socials.control_plane.run_lifecycle as run_lifecycle


def _fetch_all_control(sql: str, params: list[Any]) -> list[dict[str, Any]]:
    try:
        return legacy.pg.fetch_all(sql, params, pool_name=run_lifecycle.SOCIAL_CONTROL_POOL_NAME)
    except TypeError as exc:
        if "unexpected keyword argument 'pool_name'" not in str(exc):
            raise
        return legacy.pg.fetch_all(sql, params)


def _fetch_one_control(sql: str, params: list[Any]) -> dict[str, Any] | None:
    try:
        return legacy.pg.fetch_one(sql, params, pool_name=run_lifecycle.SOCIAL_CONTROL_POOL_NAME)
    except TypeError as exc:
        if "unexpected keyword argument 'pool_name'" not in str(exc):
            raise
        return legacy.pg.fetch_one(sql, params)


def list_runs(
    season_id: str,
    *,
    limit: int = 50,
    status: str | None = None,
    source_scope: str | None = None,
    run_id: str | None = None,
    client_session_id: str | None = None,
    client_workflow_id: str | None = None,
    platforms: list[str] | None = None,
    week_index: int | None = None,
    date_start: datetime | None = None,
    date_end: datetime | None = None,
) -> list[dict[str, Any]]:
    safe_limit = max(1, min(int(limit), 250))
    normalized_platforms = legacy._resolve_requested_platforms(platforms) if platforms else None
    normalized_date_start = legacy._coerce_dt(date_start)
    normalized_date_end = legacy._coerce_dt(date_end)
    requires_config_filtering = any(
        [
            normalized_platforms is not None,
            week_index is not None,
            normalized_date_start is not None,
            normalized_date_end is not None,
        ]
    )
    sql = """
        select
          id::text,
          season_id::text as season_id,
          show_id::text as show_id,
          source_scope,
          status,
          config,
          summary,
          initiated_by,
          created_at,
          started_at,
          completed_at,
          cancelled_at
        from social.scrape_runs
        where season_id = %s
    """
    params: list[Any] = [season_id]
    if status:
        sql += " and status = %s"
        params.append(status)
    if source_scope:
        sql += " and source_scope = %s"
        params.append(source_scope)
    if run_id:
        sql += " and id = %s::uuid"
        params.append(run_id)
    if client_session_id:
        sql += " and coalesce(config->>'client_session_id', '') = %s"
        params.append(client_session_id)
    if client_workflow_id:
        sql += " and coalesce(config->>'client_workflow_id', '') = %s"
        params.append(client_workflow_id)
    sql += " order by created_at desc limit %s"
    params.append(250 if requires_config_filtering else safe_limit)
    rows = _fetch_all_control(sql, params)
    filtered_rows: list[dict[str, Any]] = []
    for row in rows:
        config = row.get("config") if isinstance(row.get("config"), dict) else {}
        if not legacy._run_matches_scope_filters(
            config,
            platforms=normalized_platforms,
            week_index=week_index,
            date_start=normalized_date_start,
            date_end=normalized_date_end,
        ):
            continue
        summary = row.get("summary") if isinstance(row.get("summary"), dict) else {}
        total_jobs = int(summary.get("total_jobs") or 0)
        completed_jobs = int(summary.get("completed_jobs") or 0)
        failed_jobs = int(summary.get("failed_jobs") or 0)
        active_jobs = int(summary.get("active_jobs") or 0)
        if completed_jobs == 0 and total_jobs > 0:
            completed_jobs = max(0, total_jobs - failed_jobs - active_jobs)
        summary_normalized = dict(summary)
        if summary_normalized:
            summary_normalized["completed_jobs"] = completed_jobs
        row["summary_normalized"] = summary_normalized
        row["execution_owner"] = str(config.get("execution_owner") or "").strip() or None
        row["execution_mode_canonical"] = str(config.get("execution_mode_canonical") or "").strip() or None
        row["execution_backend_canonical"] = str(config.get("execution_backend_canonical") or "").strip() or None
        row["ingest_mode"] = (
            str(config.get("pipeline_ingest_mode") or legacy.LEGACY_SEASON_TARGETED_INGEST_MODE).strip()
            or legacy.LEGACY_SEASON_TARGETED_INGEST_MODE
        )
        row["orchestration_id"] = (
            str(config.get("orchestration_id") or config.get("client_workflow_id") or "").strip() or None
        )
        row["orchestration_scope"] = str(config.get("orchestration_scope") or "").strip() or None
        row["orchestration_slot_key"] = str(config.get("orchestration_slot_key") or "").strip() or None
        row["orchestration_position"] = legacy._normalize_non_negative_int(config.get("orchestration_position"))
        row["orchestration_total_runs"] = legacy._normalize_non_negative_int(config.get("orchestration_total_runs"))
        row["orchestration_week_index"] = (
            legacy._normalize_non_negative_int(config.get("orchestration_week_index"))
            if config.get("orchestration_week_index") is not None
            else None
        )
        row["orchestration_platform"] = str(config.get("orchestration_platform") or "").strip() or None
        filtered_rows.append(row)
        if len(filtered_rows) >= safe_limit:
            break
    return filtered_rows


def list_run_summaries(
    season_id: str,
    *,
    limit: int = 20,
    source_scope: str | None = None,
    client_session_id: str | None = None,
    client_workflow_id: str | None = None,
    platforms: list[str] | None = None,
    week_index: int | None = None,
    date_start: datetime | None = None,
    date_end: datetime | None = None,
) -> list[dict[str, Any]]:
    cache_key = (
        "run_summaries",
        str(season_id or "").strip(),
        int(limit),
        str(source_scope or "").strip().lower() or None,
        str(client_session_id or "").strip() or None,
        str(client_workflow_id or "").strip() or None,
        tuple(
            sorted(
                {str(platform or "").strip().lower() for platform in (platforms or []) if str(platform or "").strip()}
            )
        ),
        int(week_index) if week_index is not None else None,
        legacy._iso(legacy._coerce_dt(date_start)),
        legacy._iso(legacy._coerce_dt(date_end)),
    )
    cached_payload = legacy._get_social_hot_path_cache(cache_key)
    if isinstance(cached_payload, list):
        return cached_payload
    runs = list_runs(
        season_id,
        limit=limit,
        source_scope=source_scope,
        client_session_id=client_session_id,
        client_workflow_id=client_workflow_id,
        platforms=platforms,
        week_index=week_index,
        date_start=date_start,
        date_end=date_end,
    )
    if not runs:
        return []

    run_ids = [str(run.get("id") or "") for run in runs if str(run.get("id") or "").strip()]
    if not run_ids:
        return []

    job_rows = _fetch_all_control(
        """
        select
          run_id::text as run_id,
          platform,
          status,
          error_message,
          last_error_code,
          last_error_class,
          metadata
        from social.scrape_jobs
        where season_id = %s
          and run_id = any(%s::uuid[])
        """,
        [season_id, run_ids],
    )

    by_run: dict[str, dict[str, Any]] = {}
    for row in job_rows:
        run_id = str(row.get("run_id") or "")
        if not run_id:
            continue
        bucket = by_run.setdefault(
            run_id,
            {
                "affected_platforms": set(),
                "error_counts": Counter(),
                "failed_jobs": 0,
                "active_jobs": 0,
                "total_jobs": 0,
            },
        )
        bucket["total_jobs"] += 1
        platform = str(row.get("platform") or "").strip().lower()
        if platform:
            bucket["affected_platforms"].add(platform)
        status = str(row.get("status") or "").strip().lower()
        if status == "failed":
            bucket["failed_jobs"] += 1
        if status in {"queued", "pending", "retrying", "running"}:
            bucket["active_jobs"] += 1
        if status in {"failed", "retrying"}:
            metadata = row.get("metadata")
            metadata_map = metadata if isinstance(metadata, dict) else {}
            metadata_error_code = metadata_map.get("job_error_code")
            normalized = (
                str(metadata_error_code).strip().upper()
                if isinstance(metadata_error_code, str) and metadata_error_code.strip()
                else legacy._normalize_job_error_code(
                    raw_error_code=str(row.get("last_error_code") or ""),
                    error_message=str(row.get("error_message") or ""),
                    error_class=str(row.get("last_error_class") or ""),
                )
            )
            bucket["error_counts"][normalized] += 1

    now_utc = legacy._now_utc()
    summaries: list[dict[str, Any]] = []
    for run in runs:
        run_id = str(run.get("id") or "")
        if not run_id:
            continue
        summary = run.get("summary") if isinstance(run.get("summary"), dict) else {}
        aggregate = by_run.get(run_id) or {}
        started_at = run.get("started_at")
        completed_at = run.get("completed_at")
        duration_seconds: int | None = None
        if isinstance(started_at, datetime):
            end_ref = completed_at if isinstance(completed_at, datetime) else now_utc
            duration_seconds = max(0, int((end_ref - started_at).total_seconds()))

        total_jobs = int(summary.get("total_jobs") or aggregate.get("total_jobs") or 0)
        completed_jobs = int(summary.get("completed_jobs") or 0)
        failed_jobs = int(summary.get("failed_jobs") or aggregate.get("failed_jobs") or 0)
        active_jobs = int(summary.get("active_jobs") or aggregate.get("active_jobs") or 0)
        if completed_jobs == 0 and total_jobs > 0:
            completed_jobs = max(0, total_jobs - failed_jobs - active_jobs)
        items_found_total = int(summary.get("items_found_total") or 0)
        stage_counts = summary.get("stage_counts") if isinstance(summary.get("stage_counts"), dict) else {}
        affected_platforms = sorted(aggregate.get("affected_platforms") or set())
        error_counts = dict(sorted((aggregate.get("error_counts") or Counter()).items()))
        success_rate_pct = legacy._safe_percent(completed_jobs, max(1, total_jobs))

        summaries.append(
            {
                "run_id": run_id,
                "status": run.get("status"),
                "source_scope": run.get("source_scope"),
                "execution_owner": run.get("execution_owner"),
                "execution_mode_canonical": run.get("execution_mode_canonical"),
                "execution_backend_canonical": run.get("execution_backend_canonical"),
                "ingest_mode": run.get("ingest_mode"),
                "orchestration_id": run.get("orchestration_id"),
                "orchestration_scope": run.get("orchestration_scope"),
                "orchestration_slot_key": run.get("orchestration_slot_key"),
                "orchestration_position": run.get("orchestration_position"),
                "orchestration_total_runs": run.get("orchestration_total_runs"),
                "orchestration_week_index": run.get("orchestration_week_index"),
                "orchestration_platform": run.get("orchestration_platform"),
                "created_at": legacy._iso(run.get("created_at")),
                "started_at": legacy._iso(started_at),
                "completed_at": legacy._iso(completed_at),
                "duration_seconds": duration_seconds,
                "total_jobs": total_jobs,
                "completed_jobs": completed_jobs,
                "failed_jobs": failed_jobs,
                "active_jobs": active_jobs,
                "items_found_total": items_found_total,
                "stage_counts": stage_counts,
                "affected_platforms": affected_platforms,
                "error_counts": error_counts,
                "success_rate_pct": success_rate_pct,
            }
        )
    legacy._set_social_hot_path_cache(cache_key, summaries)
    return summaries


def get_run_progress_snapshot(
    season_id: str,
    run_id: str,
    *,
    recent_log_limit: int = 20,
) -> dict[str, Any]:
    safe_recent_log_limit = max(1, min(int(recent_log_limit), 100))
    if not legacy._relation_exists("social.scrape_runs") or not legacy._relation_exists("social.scrape_jobs"):
        raise ValueError("social_ingest_queue_schema_missing")
    features = legacy._scrape_jobs_features()
    if not bool(features.get("has_run_id")):
        raise ValueError("run_progress_requires_scrape_jobs_run_id")

    run_row = _fetch_one_control(
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
          and season_id = %s::uuid
        limit 1
        """,
        [run_id, season_id],
    )
    if not run_row:
        raise ValueError("run_not_found")

    select_worker_id = "j.worker_id" if bool(features.get("has_queue_fields")) else "null::text as worker_id"
    select_last_error_code = (
        "j.last_error_code" if bool(features.get("has_queue_fields")) else "null::text as last_error_code"
    )
    job_rows = _fetch_all_control(
        f"""
        select
          j.id::text as id,
          j.platform,
          j.job_type,
          j.status,
          j.items_found,
          j.error_message,
          j.created_at,
          j.started_at,
          j.completed_at,
          j.config,
          j.metadata,
          {select_worker_id},
          {select_last_error_code}
        from social.scrape_jobs j
        where j.season_id = %s::uuid
          and j.run_id = %s::uuid
        order by coalesce(j.completed_at, j.started_at, j.created_at) desc, j.created_at desc
        """,
        [season_id, run_id],
    )

    computed_summary = legacy._summarize_run_progress_job_rows(job_rows)
    stored_summary = legacy._metadata_dict(run_row.get("summary"))
    run_status = str(run_row.get("status") or "").strip().lower()
    if legacy._run_progress_summary_needs_refresh(stored_summary, computed_summary) or (
        run_status in legacy._RUN_PROGRESS_ACTIVE_JOB_STATUSES and computed_summary["active_jobs"] == 0
    ):
        if run_status in legacy._RUN_PROGRESS_ACTIVE_JOB_STATUSES and computed_summary["active_jobs"] == 0:
            legacy._finalize_run_status(run_id, force_recompute=True)
        else:
            legacy._update_run_summary(run_id, force_recompute=True)
        refreshed_run_row = _fetch_one_control(
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
              and season_id = %s::uuid
            limit 1
            """,
            [run_id, season_id],
        )
        if refreshed_run_row:
            run_row = refreshed_run_row

    return legacy._build_run_progress_snapshot_payload(
        run_row=run_row,
        job_rows=job_rows,
        run_id=run_id,
        season_id=season_id,
        recent_log_limit=safe_recent_log_limit,
    )
