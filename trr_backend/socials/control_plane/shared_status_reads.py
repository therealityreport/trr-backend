"""Shared-account status read models for the social control plane."""

from __future__ import annotations

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


def get_season_shared_status(
    season_id: str,
    *,
    source_scope: str = "bravo",
) -> dict[str, Any]:
    cache_key = (
        "season_shared_status",
        str(season_id or "").strip(),
        str(source_scope or "").strip().lower() or "bravo",
    )
    cached_payload = legacy._get_social_hot_path_cache(cache_key)
    if isinstance(cached_payload, dict):
        return cached_payload
    context = legacy.get_season_context(season_id)
    match_rows = _fetch_all_control(
        """
        select
          status,
          source_id,
          updated_at,
          metadata
        from social.shared_post_matches
        where source_scope = %s
          and matched_season_id = %s::uuid
        order by updated_at desc
        limit 500
        """,
        [source_scope, season_id],
    )
    review_count_row = _fetch_one_control(
        """
        select count(*)::int as count
        from social.shared_post_review_queue
        where source_scope = %s
          and review_status = 'open'
          and exists (
            select 1
            from jsonb_array_elements(coalesce(payload->'candidate_matches', '[]'::jsonb)) as candidate
            where candidate->>'season_id' = %s
          )
        """,
        [source_scope, season_id],
    ) or {"count": 0}
    retained_unassigned_row = _fetch_one_control(
        """
        select count(*)::int as count
        from social.shared_post_matches
        where source_scope = %s
          and status = 'unmatched'
        """,
        [source_scope],
    ) or {"count": 0}
    recent_run = _fetch_one_control(
        """
        select
          r.id::text as id,
          r.status,
          r.config,
          r.summary,
          r.created_at,
          r.started_at,
          r.completed_at
        from social.scrape_runs r
        join (
          select run_id, max(updated_at) as updated_at
          from (
            select run_id, updated_at
            from social.shared_post_matches
            where source_scope = %s
              and matched_season_id = %s::uuid
              and run_id is not null
            union all
            select q.run_id, q.updated_at
            from social.shared_post_review_queue q
            where q.source_scope = %s
              and q.review_status = 'open'
              and q.run_id is not null
              and exists (
                select 1
                from jsonb_array_elements(coalesce(q.payload->'candidate_matches', '[]'::jsonb)) as candidate
                where candidate->>'season_id' = %s
              )
          ) relevant
          group by run_id
          order by max(updated_at) desc nulls last
          limit 1
        ) recent on recent.run_id = r.id
        where coalesce(r.config->>'pipeline_ingest_mode', '') = %s
        limit 1
        """,
        [source_scope, season_id, source_scope, season_id, legacy.SHARED_ACCOUNT_ASYNC_INGEST_MODE],
    )
    stage_counts = {}
    if isinstance(recent_run, dict):
        stage_counts = dict((recent_run.get("summary") or {}).get("stage_counts") or {})
    payload = {
        "season_id": context.season_id,
        "show_id": context.show_id,
        "show_name": context.show_name,
        "season_number": context.season_number,
        "source_scope": source_scope,
        "ingest_mode": legacy.SHARED_ACCOUNT_ASYNC_INGEST_MODE,
        "matched_posts": len(match_rows),
        "matched_source_ids": [
            str(row.get("source_id") or "") for row in match_rows if str(row.get("source_id") or "").strip()
        ],
        "latest_match_at": legacy._iso(match_rows[0].get("updated_at")) if match_rows else None,
        "review_queue_count": legacy._normalize_non_negative_int(review_count_row.get("count")),
        "retained_unassigned_count": legacy._normalize_non_negative_int(retained_unassigned_row.get("count")),
        "shared_scrape_status": legacy._shared_stage_status_summary(stage_counts, legacy.SHARED_ACCOUNT_POSTS_STAGE),
        "classification_status": legacy._shared_stage_status_summary(stage_counts, legacy.POST_CLASSIFY_STAGE),
        "materialization_status": legacy._shared_stage_status_summary(stage_counts, legacy.SEASON_MATERIALIZE_STAGE),
        "latest_shared_run": {
            "run_id": str(recent_run.get("id") or "").strip() or None,
            "status": str(recent_run.get("status") or "").strip() or None,
            "created_at": legacy._iso(recent_run.get("created_at")),
            "started_at": legacy._iso(recent_run.get("started_at")),
            "completed_at": legacy._iso(recent_run.get("completed_at")),
        }
        if recent_run
        else None,
    }
    legacy._set_social_hot_path_cache(cache_key, payload)
    return payload


def list_shared_runs(
    *,
    limit: int = 50,
    status: str | None = None,
    source_scope: str | None = None,
    run_id: str | None = None,
) -> list[dict[str, Any]]:
    safe_limit = max(1, min(int(limit), 250))
    sql = """
        select
          id::text as id,
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
        where coalesce(config->>'pipeline_ingest_mode', '') = %s
    """
    params: list[Any] = [legacy.SHARED_ACCOUNT_ASYNC_INGEST_MODE]
    if status:
        sql += " and status = %s"
        params.append(status)
    if source_scope:
        sql += " and source_scope = %s"
        params.append(source_scope)
    if run_id:
        sql += " and id = %s::uuid"
        params.append(run_id)
    sql += " order by created_at desc limit %s"
    params.append(safe_limit)
    rows = _fetch_all_control(sql, params)
    for row in rows:
        config = dict(row.get("config") or {})
        row["execution_owner"] = str(config.get("execution_owner") or "").strip() or None
        row["execution_mode_canonical"] = str(config.get("execution_mode_canonical") or "").strip() or None
        row["execution_backend_canonical"] = str(config.get("execution_backend_canonical") or "").strip() or None
        row["ingest_mode"] = str(config.get("pipeline_ingest_mode") or legacy.SHARED_ACCOUNT_ASYNC_INGEST_MODE)
    return rows
