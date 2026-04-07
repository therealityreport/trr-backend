"""Repository helpers for the cast screentime control plane."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

import psycopg2
from psycopg2.extras import Json

from trr_backend.db import pg


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    return value


def _json(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return Json(_json_safe(value))
    return value


def _normalize(value: Any) -> Any:
    if isinstance(value, UUID):
        return str(value)
    return value


def create_media_upload_session(payload: dict[str, Any]) -> dict[str, Any]:
    sql = (
        "INSERT INTO ml.analysis_media_upload_sessions "
        "(id, show_id, season_id, episode_id, created_by, status, temp_object_key, content_type, "
        " expected_size_bytes, expected_checksum_sha256, verification_json, expires_at, "
        " video_class, promo_subtype, media_type, media_kind, source_import_type, owner_scope) "
        "VALUES ("
        "COALESCE(%s::uuid, gen_random_uuid()), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
        "%s, %s, %s, %s, %s, %s"
        ") "
        "RETURNING *"
    )
    rows = pg.execute_returning(
        sql,
        [
            _normalize(payload.get("id")),
            _normalize(payload.get("show_id")),
            _normalize(payload.get("season_id")),
            _normalize(payload.get("episode_id")),
            _normalize(payload.get("created_by")),
            payload.get("status", "pending_upload"),
            payload.get("temp_object_key"),
            payload.get("content_type"),
            payload.get("expected_size_bytes"),
            payload.get("expected_checksum_sha256"),
            _json(payload.get("verification_json", {})),
            payload.get("expires_at"),
            payload.get("video_class", "episode"),
            payload.get("promo_subtype"),
            payload.get("media_type", "episode"),
            payload.get("media_kind"),
            payload.get("source_import_type", "upload"),
            payload.get("owner_scope", "season"),
        ],
    )
    return rows[0] if rows else {}


def get_media_upload_session(upload_session_id: str) -> dict[str, Any] | None:
    return pg.fetch_one("SELECT * FROM ml.analysis_media_upload_sessions WHERE id = %s", [upload_session_id])


def update_media_upload_session(upload_session_id: str, payload: dict[str, Any]) -> dict[str, Any] | None:
    if not payload:
        return get_media_upload_session(upload_session_id)

    assignments: list[str] = []
    params: list[Any] = []
    for key, value in payload.items():
        assignments.append(f"{key} = %s")
        params.append(_json(_normalize(value)))
    params.append(upload_session_id)

    rows = pg.execute_returning(
        f"UPDATE ml.analysis_media_upload_sessions SET {', '.join(assignments)} WHERE id = %s RETURNING *",
        params,
    )
    return rows[0] if rows else None


def create_video_asset(payload: dict[str, Any]) -> dict[str, Any]:
    sql = (
        "INSERT INTO ml.analysis_media_assets "
        "(id, episode_id, season_id, show_id, media_asset_id, legacy_screenalytics_video_asset_id, "
        " source_url, source_json, duration_seconds, metadata, video_class, promo_subtype, "
        " media_type, media_kind, source_import_type) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
        "RETURNING *"
    )
    rows = pg.execute_returning(
        sql,
        [
            _normalize(payload.get("id")),
            _normalize(payload.get("episode_id")),
            _normalize(payload.get("season_id")),
            _normalize(payload.get("show_id")),
            _normalize(payload.get("media_asset_id")),
            _normalize(payload.get("legacy_screenalytics_video_asset_id")),
            payload.get("source_url"),
            _json(payload.get("source_json", {})),
            payload.get("duration_seconds"),
            _json(payload.get("metadata", {})),
            payload.get("video_class", "episode"),
            payload.get("promo_subtype"),
            payload.get("media_type", "episode"),
            payload.get("media_kind"),
            payload.get("source_import_type", "upload"),
        ],
    )
    return rows[0] if rows else {}


def get_video_asset(video_asset_id: str) -> dict[str, Any] | None:
    return pg.fetch_one("SELECT * FROM ml.analysis_media_assets WHERE id = %s", [video_asset_id])


def get_video_asset_by_legacy_screenalytics_id(legacy_video_asset_id: str) -> dict[str, Any] | None:
    return pg.fetch_one(
        """
        SELECT *
        FROM ml.analysis_media_assets
        WHERE legacy_screenalytics_video_asset_id = %s::uuid
        LIMIT 1
        """,
        [legacy_video_asset_id],
    )


def resolve_video_asset(video_asset_id: str) -> dict[str, Any] | None:
    return get_video_asset(video_asset_id) or get_video_asset_by_legacy_screenalytics_id(video_asset_id)


def get_video_asset_upload_session_status(video_asset_id: str) -> str | None:
    row = pg.fetch_one(
        """
        SELECT mus.status
        FROM ml.analysis_media_assets va
        LEFT JOIN ml.analysis_media_upload_sessions mus
          ON mus.id::text = coalesce(va.source_json->>'upload_session_id', '')
        WHERE va.id = %s
        LIMIT 1
        """,
        [video_asset_id],
    )
    if not row:
        return None
    status = row.get("status")
    return str(status) if isinstance(status, str) and status else None


def list_video_asset_cast_candidates(video_asset_id: str) -> list[dict[str, Any]]:
    return pg.fetch_all(
        """
        SELECT
          vacc.person_id::text AS person_id,
          p.full_name AS display_name,
          vacc.source,
          vacc.confidence,
          vacc.credit_category,
          vacc.billing_order,
          vacc.role
        FROM ml.analysis_media_cast_candidates vacc
        JOIN core.people p ON p.id = vacc.person_id
        WHERE vacc.video_asset_id = %s
        ORDER BY vacc.billing_order NULLS LAST, p.full_name ASC
        """,
        [video_asset_id],
    )


def resolve_owner_context(
    *,
    owner_scope: str | None = None,
    owner_id: str | None = None,
    show_id: str | None = None,
    season_id: str | None = None,
    episode_id: str | None = None,
) -> dict[str, Any] | None:
    normalized_scope = str(owner_scope or "").strip().lower()
    normalized_owner_id = str(owner_id or "").strip()

    if normalized_scope and normalized_owner_id:
        if normalized_scope == "show":
            return pg.fetch_one(
                """
                SELECT
                  'show'::text AS owner_scope,
                  s.id::text AS owner_id,
                  s.id::text AS show_id,
                  NULL::text AS season_id,
                  NULL::text AS episode_id,
                  NULL::int AS season_number,
                  NULL::int AS episode_number,
                  s.name AS show_name
                FROM core.shows s
                WHERE s.id = %s::uuid
                LIMIT 1
                """,
                [normalized_owner_id],
            )
        if normalized_scope == "season":
            return pg.fetch_one(
                """
                SELECT
                  'season'::text AS owner_scope,
                  s.id::text AS owner_id,
                  s.show_id::text AS show_id,
                  s.id::text AS season_id,
                  NULL::text AS episode_id,
                  s.season_number,
                  NULL::int AS episode_number,
                  sh.name AS show_name
                FROM core.seasons s
                JOIN core.shows sh ON sh.id = s.show_id
                WHERE s.id = %s::uuid
                LIMIT 1
                """,
                [normalized_owner_id],
            )
        if normalized_scope == "episode":
            return pg.fetch_one(
                """
                SELECT
                  'episode'::text AS owner_scope,
                  e.id::text AS owner_id,
                  e.show_id::text AS show_id,
                  e.season_id::text AS season_id,
                  e.id::text AS episode_id,
                  s.season_number,
                  e.episode_number,
                  sh.name AS show_name
                FROM core.episodes e
                JOIN core.shows sh ON sh.id = e.show_id
                LEFT JOIN core.seasons s ON s.id = e.season_id
                WHERE e.id = %s::uuid
                LIMIT 1
                """,
                [normalized_owner_id],
            )
        raise ValueError(f"Unsupported owner_scope: {normalized_scope}")

    if episode_id:
        return resolve_owner_context(owner_scope="episode", owner_id=episode_id)
    if season_id:
        return resolve_owner_context(owner_scope="season", owner_id=season_id)
    if show_id:
        return resolve_owner_context(owner_scope="show", owner_id=show_id)
    return None


def list_target_youtube_accounts(*, show_id: str, season_id: str | None = None) -> list[dict[str, Any]]:
    params: list[Any] = ["youtube", "bravo"]
    filters: list[str] = [
        "platform = %s",
        "source_scope = %s",
        "is_active = true",
    ]
    if season_id:
        filters.append("season_id = %s::uuid")
        params.append(season_id)
    else:
        filters.append("show_id = %s::uuid")
        params.append(show_id)
    return pg.fetch_all(
        f"""
        SELECT
          st.show_id::text AS show_id,
          st.season_id::text AS season_id,
          account.value::text AS account_handle,
          st.config
        FROM social.season_targets st
        CROSS JOIN LATERAL jsonb_array_elements_text(coalesce(st.accounts, '[]'::jsonb)) AS account(value)
        WHERE {" AND ".join(filters)}
        ORDER BY st.updated_at DESC, account.value ASC
        """,
        params,
    )


def get_social_youtube_video(youtube_video_row_id: str) -> dict[str, Any] | None:
    return pg.fetch_one(
        """
        SELECT
          v.*,
          v.id::text AS id,
          v.show_id::text AS show_id,
          v.season_id::text AS season_id,
          v.person_id::text AS person_id
        FROM social.youtube_videos v
        WHERE v.id = %s::uuid
        LIMIT 1
        """,
        [youtube_video_row_id],
    )


def list_candidate_cast_snapshot(
    *,
    video_asset_id: str | None = None,
    show_id: str | None = None,
    season_id: str | None = None,
    episode_id: str | None = None,
) -> list[dict[str, Any]]:
    if video_asset_id:
        rows = list_video_asset_cast_candidates(video_asset_id)
        if rows:
            return rows

    if episode_id:
        try:
            rows = pg.fetch_all(
                """
                SELECT
                  v.person_id::text AS person_id,
                  p.full_name AS display_name,
                  'trr_episode_credits'::text AS source,
                  1.0::float AS confidence,
                  v.credit_category,
                  v.billing_order,
                  v.role
                FROM core.v_episode_cast v
                JOIN core.people p ON p.id = v.person_id
                WHERE v.episode_id = %s::uuid
                ORDER BY v.billing_order NULLS LAST, p.full_name ASC
                """,
                [episode_id],
            )
        except psycopg2.errors.UndefinedTable:
            rows = []
        if rows:
            return rows

    if season_id:
        try:
            rows = pg.fetch_all(
                """
                WITH season_totals AS (
                  SELECT COUNT(*)::float AS total_episodes
                  FROM core.episodes
                  WHERE season_id = %s::uuid
                )
                SELECT
                  v.person_id::text AS person_id,
                  p.full_name AS display_name,
                  'trr_season_credits'::text AS source,
                  CASE
                    WHEN st.total_episodes > 0
                      THEN LEAST(1.0, GREATEST(0.0, v.episodes_in_season::float / st.total_episodes))
                    ELSE 1.0
                  END AS confidence,
                  NULL::text AS credit_category,
                  NULL::int AS billing_order,
                  NULL::text AS role
                FROM core.v_season_cast v
                CROSS JOIN season_totals st
                JOIN core.people p ON p.id = v.person_id
                WHERE v.season_id = %s::uuid
                ORDER BY v.episodes_in_season DESC, p.full_name ASC
                """,
                [season_id, season_id],
            )
        except psycopg2.errors.UndefinedTable:
            rows = []
        if rows:
            return rows

    if show_id:
        return pg.fetch_all(
            """
            SELECT
              sc.person_id::text AS person_id,
              p.full_name AS display_name,
              'trr_show_credits'::text AS source,
              1.0::float AS confidence,
              sc.credit_category,
              sc.billing_order,
              sc.role
            FROM core.v_show_cast sc
            JOIN core.people p ON p.id = sc.person_id
            WHERE sc.show_id = %s::uuid
            ORDER BY sc.billing_order NULLS LAST, p.full_name ASC
            """,
            [show_id],
        )
    return []


def _fetch_episode_cast_rows(episode_id: str) -> list[dict[str, Any]]:
    try:
        return pg.fetch_all(
            """
            SELECT
              v.person_id::text AS person_id,
              p.full_name AS display_name,
              'trr_episode_credits'::text AS source,
              1.0::float AS confidence,
              v.credit_category,
              v.billing_order,
              v.role
            FROM core.v_episode_cast v
            JOIN core.people p ON p.id = v.person_id
            WHERE v.episode_id = %s::uuid
            ORDER BY v.billing_order NULLS LAST, p.full_name ASC
            """,
            [episode_id],
        )
    except psycopg2.errors.UndefinedTable:
        return []


def _fetch_season_cast_rows(season_id: str) -> list[dict[str, Any]]:
    try:
        return pg.fetch_all(
            """
            WITH season_totals AS (
              SELECT COUNT(*)::float AS total_episodes
              FROM core.episodes
              WHERE season_id = %s::uuid
            )
            SELECT
              v.person_id::text AS person_id,
              p.full_name AS display_name,
              'trr_season_credits'::text AS source,
              CASE
                WHEN st.total_episodes > 0
                  THEN LEAST(1.0, GREATEST(0.0, v.episodes_in_season::float / st.total_episodes))
                ELSE 1.0
              END AS confidence,
              NULL::text AS credit_category,
              NULL::int AS billing_order,
              NULL::text AS role
            FROM core.v_season_cast v
            CROSS JOIN season_totals st
            JOIN core.people p ON p.id = v.person_id
            WHERE v.season_id = %s::uuid
            ORDER BY v.episodes_in_season DESC, p.full_name ASC
            """,
            [season_id, season_id],
        )
    except psycopg2.errors.UndefinedTable:
        return []


def _fetch_show_cast_rows(show_id: str) -> list[dict[str, Any]]:
    return pg.fetch_all(
        """
        SELECT
          sc.person_id::text AS person_id,
          p.full_name AS display_name,
          'trr_show_credits'::text AS source,
          1.0::float AS confidence,
          sc.credit_category,
          sc.billing_order,
          sc.role
        FROM core.v_show_cast sc
        JOIN core.people p ON p.id = sc.person_id
        WHERE sc.show_id = %s::uuid
        ORDER BY sc.billing_order NULLS LAST, p.full_name ASC
        """,
        [show_id],
    )


def _approved_facebank_person_ids(person_ids: list[str]) -> set[str]:
    normalized = [person_id for person_id in person_ids if person_id]
    if not normalized:
        return set()
    rows = pg.fetch_all(
        """
        SELECT DISTINCT person_id::text AS person_id
        FROM ml.face_reference_images
        WHERE is_active = true
          AND approved = true
          AND person_id::text = ANY(%s)
        """,
        [normalized],
    )
    return {
        str(row.get("person_id") or "").strip()
        for row in rows
        if isinstance(row, dict) and str(row.get("person_id") or "").strip()
    }


def _scope_order(*, media_type: str | None, owner_scope: str | None) -> list[str]:
    normalized_media_type = str(media_type or "").strip().lower() or "episode"
    normalized_owner_scope = str(owner_scope or "").strip().lower()
    if normalized_media_type == "episode":
        return ["episode", "season", "show"]
    if normalized_owner_scope == "episode":
        return ["episode", "season", "show"]
    if normalized_owner_scope == "season":
        return ["season", "show"]
    return ["show"]


def _sort_scope_rows_with_coverage(rows: list[dict[str, Any]], approved_person_ids: set[str]) -> list[dict[str, Any]]:
    def _sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
        person_id = str(row.get("person_id") or "").strip()
        billing_order = row.get("billing_order")
        confidence = row.get("confidence")
        display_name = str(row.get("display_name") or "").strip().lower()
        try:
            billing_value = int(billing_order) if billing_order is not None else 999999
        except (TypeError, ValueError):
            billing_value = 999999
        try:
            confidence_value = float(confidence) if confidence is not None else 0.0
        except (TypeError, ValueError):
            confidence_value = 0.0
        return (
            0 if person_id in approved_person_ids else 1,
            billing_value,
            -confidence_value,
            display_name,
        )

    return sorted(rows, key=_sort_key)


def build_candidate_cast_snapshot(
    *,
    video_asset_id: str | None = None,
    show_id: str | None = None,
    season_id: str | None = None,
    episode_id: str | None = None,
    media_type: str | None = None,
    owner_scope: str | None = None,
) -> dict[str, Any]:
    direct_rows = list_video_asset_cast_candidates(video_asset_id) if video_asset_id else []
    scope_order = _scope_order(media_type=media_type, owner_scope=owner_scope)
    scope_rows: dict[str, list[dict[str, Any]]] = {
        "episode": _fetch_episode_cast_rows(episode_id) if episode_id and "episode" in scope_order else [],
        "season": _fetch_season_cast_rows(season_id) if season_id and "season" in scope_order else [],
        "show": _fetch_show_cast_rows(show_id) if show_id and "show" in scope_order else [],
    }

    snapshot: list[dict[str, Any]] = []
    seen_person_ids: set[str] = set()
    fallback_scopes_used: list[str] = []

    def _append_rows(rows: list[dict[str, Any]], *, mark_scope: str | None = None) -> None:
        for row in rows:
            person_id = str(row.get("person_id") or "").strip()
            if not person_id or person_id in seen_person_ids:
                continue
            snapshot.append(row)
            seen_person_ids.add(person_id)
            if mark_scope and mark_scope not in fallback_scopes_used:
                fallback_scopes_used.append(mark_scope)

    if direct_rows:
        _append_rows(direct_rows)

    approved_person_ids = _approved_facebank_person_ids(
        [
            str(row.get("person_id") or "").strip()
            for row in [*direct_rows, *scope_rows["episode"], *scope_rows["season"], *scope_rows["show"]]
        ]
    )

    primary_scope = next(
        (scope for scope in scope_order if scope_rows.get(scope)), scope_order[0] if scope_order else "show"
    )
    primary_rows = _sort_scope_rows_with_coverage(scope_rows.get(primary_scope, []), approved_person_ids)
    _append_rows(primary_rows)

    approved_count = len({person_id for person_id in seen_person_ids if person_id in approved_person_ids})
    needs_fallback = not seen_person_ids or approved_count == 0
    if primary_scope == "episode" and len(seen_person_ids) < 2:
        needs_fallback = True

    if needs_fallback:
        for scope in scope_order:
            if scope == primary_scope:
                continue
            rows = _sort_scope_rows_with_coverage(scope_rows.get(scope, []), approved_person_ids)
            before_count = len(seen_person_ids)
            _append_rows(rows, mark_scope=scope)
            if len(seen_person_ids) > before_count:
                approved_count = len({person_id for person_id in seen_person_ids if person_id in approved_person_ids})
            if len(seen_person_ids) >= 4 and approved_count >= 1:
                break

    warnings: list[str] = []
    if not seen_person_ids:
        warnings.append("no_candidate_cast_rows_found")
    if approved_count == 0:
        warnings.append("no_approved_facebank_coverage")
    if primary_scope == "episode" and fallback_scopes_used:
        warnings.append("episode_scope_required_fallback")
    if len(seen_person_ids) < 2:
        warnings.append("sparse_candidate_cast")

    return {
        "snapshot": snapshot,
        "candidate_scope_policy": {
            "media_type": str(media_type or "").strip().lower() or "episode",
            "owner_scope": str(owner_scope or "").strip().lower() or None,
            "primary_scope": primary_scope,
            "scope_order": scope_order,
            "fallback_scopes_used": fallback_scopes_used,
            "preferred_facebank_coverage": True,
        },
        "cast_coverage_summary": {
            "candidate_count": len(snapshot),
            "approved_facebank_coverage_count": approved_count,
            "fallback_scopes_used": fallback_scopes_used,
            "warning": warnings[0] if warnings else None,
            "warnings": warnings,
        },
    }

    return []


def create_run(payload: dict[str, Any]) -> dict[str, Any]:
    sql = (
        "INSERT INTO ml.screentime_runs "
        "(video_asset_id, status, run_type, pipeline_version, execution_backend, review_status, "
        " run_config_json, config_hash, candidate_cast_snapshot_json, candidate_scope_policy_json, "
        " cast_coverage_summary_json, "
        " dispatch_status, dispatch_job_id, dispatch_accepted_at) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
        "RETURNING *"
    )
    rows = pg.execute_returning(
        sql,
        [
            _normalize(payload.get("video_asset_id")),
            payload.get("status", "pending"),
            payload.get("run_type", "cast_screentime"),
            payload.get("pipeline_version"),
            payload.get("execution_backend"),
            payload.get("review_status", "draft"),
            _json(payload.get("run_config_json", {})),
            payload.get("config_hash"),
            _json(payload.get("candidate_cast_snapshot_json", [])),
            _json(payload.get("candidate_scope_policy_json", {})),
            _json(payload.get("cast_coverage_summary_json", {})),
            payload.get("dispatch_status"),
            payload.get("dispatch_job_id"),
            payload.get("dispatch_accepted_at"),
        ],
    )
    return rows[0] if rows else {}


def get_run(run_id: str) -> dict[str, Any] | None:
    return pg.fetch_one("SELECT * FROM ml.screentime_runs WHERE id = %s", [run_id])


def get_run_with_video_asset(run_id: str) -> dict[str, Any] | None:
    return pg.fetch_one(
        """
        SELECT
          r.*,
          va.episode_id,
          va.season_id,
          va.show_id,
          va.media_asset_id,
          va.legacy_screenalytics_video_asset_id,
          va.source_url,
          va.source_json,
          va.duration_seconds,
          va.video_class,
          va.promo_subtype,
          va.media_type,
          va.media_kind,
          va.source_import_type,
          va.metadata AS video_asset_metadata
        FROM ml.screentime_runs r
        JOIN ml.analysis_media_assets va ON va.id = r.video_asset_id
        WHERE r.id = %s
        LIMIT 1
        """,
        [run_id],
    )


def update_run(run_id: str, payload: dict[str, Any]) -> dict[str, Any] | None:
    if not payload:
        return get_run(run_id)
    assignments: list[str] = []
    params: list[Any] = []
    for key, value in payload.items():
        assignments.append(f"{key} = %s")
        params.append(_json(_normalize(value)))
    params.append(run_id)
    rows = pg.execute_returning(
        f"UPDATE ml.screentime_runs SET {', '.join(assignments)} WHERE id = %s RETURNING *",
        params,
    )
    return rows[0] if rows else None


def set_run_heartbeat(run_id: str, *, status: str | None = None) -> dict[str, Any] | None:
    payload: dict[str, Any] = {"worker_heartbeat_at": datetime.now(UTC).isoformat()}
    if status:
        payload["status"] = status
    return update_run(run_id, payload)


def upsert_run_artifacts(run_id: str, artifacts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = [
        (
            _normalize(run_id),
            artifact.get("artifact_key"),
            artifact.get("artifact_kind"),
            artifact.get("s3_key"),
            artifact.get("schema_version"),
            artifact.get("content_type"),
            artifact.get("checksum_sha256"),
            artifact.get("row_count"),
        )
        for artifact in artifacts
    ]
    sql = (
        "INSERT INTO ml.screentime_artifacts "
        "(run_id, artifact_key, artifact_kind, s3_key, schema_version, content_type, checksum_sha256, row_count) "
        "VALUES %s "
        "ON CONFLICT (run_id, artifact_key) DO UPDATE SET "
        "artifact_kind = EXCLUDED.artifact_kind, "
        "s3_key = EXCLUDED.s3_key, "
        "schema_version = EXCLUDED.schema_version, "
        "content_type = EXCLUDED.content_type, "
        "checksum_sha256 = EXCLUDED.checksum_sha256, "
        "row_count = EXCLUDED.row_count, "
        "updated_at = now() "
        "RETURNING *"
    )
    return pg.execute_values_returning(sql, rows)


def replace_run_person_metrics(run_id: str, metrics: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = [
        (
            _normalize(run_id),
            _normalize(metric.get("person_id")),
            metric.get("screen_time_seconds"),
            metric.get("frame_count"),
            metric.get("confidence_avg"),
            _json(metric.get("metadata", {})),
        )
        for metric in metrics
    ]
    with pg.db_connection() as conn, pg.db_cursor(conn=conn) as cur:
        cur.execute("DELETE FROM ml.screentime_person_metrics WHERE run_id = %s", [_normalize(run_id)])
        if not rows:
            return []
        sql = (
            "INSERT INTO ml.screentime_person_metrics "
            "(run_id, person_id, screen_time_seconds, frame_count, confidence_avg, metadata) "
            "VALUES %s RETURNING *"
        )
        return pg.execute_values_returning(sql, rows, conn=conn)


def replace_cast_screentime_segments(run_id: str, segments: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = [
        (
            _normalize(run_id),
            segment.get("segment_key"),
            _normalize(segment.get("person_id")),
            segment.get("start_ms"),
            segment.get("end_ms"),
            segment.get("duration_ms"),
            segment.get("frame_count", 0),
            segment.get("confidence_score"),
            segment.get("similarity_score"),
            segment.get("pose_bucket"),
            segment.get("assignment_source"),
            segment.get("is_counted", True),
            _json(segment.get("classification_json", {})),
            _json(segment.get("metadata", {})),
        )
        for segment in segments
    ]
    with pg.db_connection() as conn, pg.db_cursor(conn=conn) as cur:
        cur.execute("DELETE FROM ml.screentime_segments WHERE run_id = %s", [_normalize(run_id)])
        if not rows:
            return []
        sql = (
            "INSERT INTO ml.screentime_segments "
            "(run_id, segment_key, person_id, start_ms, end_ms, duration_ms, frame_count, confidence_score, "
            " similarity_score, pose_bucket, assignment_source, is_counted, classification_json, metadata) "
            "VALUES %s RETURNING *"
        )
        return pg.execute_values_returning(sql, rows, conn=conn)


def replace_cast_screentime_evidence(run_id: str, evidence_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped_items: dict[str, dict[str, Any]] = {}
    ordered_keys: list[str] = []
    for item in evidence_items:
        evidence_key = str(item.get("evidence_key") or "").strip()
        if not evidence_key:
            continue
        if evidence_key not in deduped_items:
            ordered_keys.append(evidence_key)
        deduped_items[evidence_key] = item
    rows = [
        (
            _normalize(run_id),
            deduped_items[evidence_key].get("segment_key"),
            evidence_key,
            deduped_items[evidence_key].get("evidence_type"),
            deduped_items[evidence_key].get("timestamp_ms"),
            deduped_items[evidence_key].get("object_key"),
            deduped_items[evidence_key].get("content_type"),
            deduped_items[evidence_key].get("ttl_expires_at"),
            _json(deduped_items[evidence_key].get("metadata", {})),
        )
        for evidence_key in ordered_keys
    ]
    with pg.db_connection() as conn, pg.db_cursor(conn=conn) as cur:
        cur.execute("DELETE FROM ml.screentime_evidence WHERE run_id = %s", [_normalize(run_id)])
        if not rows:
            return []
        sql = (
            "INSERT INTO ml.screentime_evidence "
            "(run_id, segment_key, evidence_key, evidence_type, timestamp_ms, "
            "object_key, content_type, ttl_expires_at, metadata) "
            "VALUES %s RETURNING *"
        )
        return pg.execute_values_returning(sql, rows, conn=conn)


def upsert_cast_screentime_evidence(run_id: str, evidence_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not evidence_items:
        return []
    rows = [
        (
            _normalize(run_id),
            item.get("segment_key"),
            item.get("evidence_key"),
            item.get("evidence_type"),
            item.get("timestamp_ms"),
            item.get("object_key"),
            item.get("content_type"),
            item.get("ttl_expires_at"),
            _json(item.get("metadata", {})),
        )
        for item in evidence_items
    ]
    sql = (
        "INSERT INTO ml.screentime_evidence "
        "(run_id, segment_key, evidence_key, evidence_type, timestamp_ms, "
        "object_key, content_type, ttl_expires_at, metadata) "
        "VALUES %s "
        "ON CONFLICT (run_id, evidence_key) DO UPDATE SET "
        "segment_key = EXCLUDED.segment_key, "
        "evidence_type = EXCLUDED.evidence_type, "
        "timestamp_ms = EXCLUDED.timestamp_ms, "
        "object_key = EXCLUDED.object_key, "
        "content_type = EXCLUDED.content_type, "
        "ttl_expires_at = EXCLUDED.ttl_expires_at, "
        "metadata = EXCLUDED.metadata, "
        "updated_at = now() "
        "RETURNING *"
    )
    return pg.execute_values_returning(sql, rows)


def replace_cast_screentime_excluded_sections(run_id: str, sections: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = [
        (
            _normalize(run_id),
            "excluded_section",
            section.get("section_key"),
            section.get("section_type"),
            section.get("start_ms"),
            section.get("end_ms"),
            section.get("duration_ms"),
            section.get("detection_source"),
            section.get("confidence_score"),
            _json(section.get("metadata", {})),
            "run",
            _normalize(run_id),
        )
        for section in sections
    ]
    with pg.db_connection() as conn, pg.db_cursor(conn=conn) as cur:
        cur.execute(
            "DELETE FROM ml.screentime_review_state WHERE run_id = %s AND review_kind = 'excluded_section'",
            [_normalize(run_id)],
        )
        if not rows:
            return []
        sql = (
            "INSERT INTO ml.screentime_review_state "
            "(run_id, review_kind, review_key, section_type, start_ms, end_ms, duration_ms, "
            "detection_source, confidence_score, payload_json, owner_scope, owner_entity_id) "
            "VALUES %s RETURNING *"
        )
        return pg.execute_values_returning(sql, rows, conn=conn)


def list_leaderboard(run_id: str) -> list[dict[str, Any]]:
    return pg.fetch_all(
        """
        SELECT
          rpm.run_id::text AS run_id,
          rpm.person_id::text AS person_id,
          p.full_name AS display_name,
          rpm.screen_time_seconds,
          rpm.frame_count,
          rpm.confidence_avg,
          rpm.metadata
        FROM ml.screentime_person_metrics rpm
        LEFT JOIN core.people p ON p.id = rpm.person_id
        WHERE rpm.run_id = %s
        ORDER BY rpm.screen_time_seconds DESC, rpm.frame_count DESC
        """,
        [_normalize(run_id)],
    )


def list_segments(run_id: str) -> list[dict[str, Any]]:
    return pg.fetch_all(
        """
        SELECT
          seg.*,
          p.full_name AS display_name
        FROM ml.screentime_segments seg
        LEFT JOIN core.people p ON p.id = seg.person_id
        WHERE seg.run_id = %s
        ORDER BY seg.start_ms ASC, seg.segment_key ASC
        """,
        [_normalize(run_id)],
    )


def get_segment(run_id: str, segment_key: str) -> dict[str, Any] | None:
    return pg.fetch_one(
        """
        SELECT
          seg.*,
          p.full_name AS display_name
        FROM ml.screentime_segments seg
        LEFT JOIN core.people p ON p.id = seg.person_id
        WHERE seg.run_id = %s
          AND seg.segment_key = %s
        LIMIT 1
        """,
        [_normalize(run_id), segment_key],
    )


def list_evidence(run_id: str) -> list[dict[str, Any]]:
    return pg.fetch_all(
        """
        SELECT *
        FROM ml.screentime_evidence
        WHERE run_id = %s
        ORDER BY timestamp_ms ASC, evidence_key ASC
        """,
        [_normalize(run_id)],
    )


def list_excluded_sections(run_id: str) -> list[dict[str, Any]]:
    return pg.fetch_all(
        """
        SELECT *
        FROM (
          SELECT
            run_id,
            review_key AS section_key,
            section_type,
            start_ms,
            end_ms,
            duration_ms,
            detection_source,
            confidence_score,
            payload_json AS metadata,
            decided_at,
            updated_at,
            created_at
          FROM ml.screentime_review_state
          WHERE run_id = %s
            AND review_kind = 'excluded_section'
        ) excluded_sections
        ORDER BY start_ms ASC, section_key ASC
        """,
        [_normalize(run_id)],
    )


def list_runs_for_show(
    show_id: str,
    *,
    limit: int = 20,
    video_class: str | None = None,
    media_type: str | None = None,
) -> list[dict[str, Any]]:
    params: list[Any] = [show_id]
    classification_clause = ""
    if media_type:
        params.append(media_type)
        classification_clause = " AND va.media_type = %s"
    elif video_class:
        params.append(video_class)
        classification_clause = " AND va.video_class = %s"
    params.append(limit)
    return pg.fetch_all(
        f"""
        SELECT
          r.*,
          va.show_id,
          va.season_id,
          va.episode_id,
          va.legacy_screenalytics_video_asset_id,
          va.video_class,
          va.promo_subtype,
          va.media_type,
          va.media_kind,
          va.source_import_type
        FROM ml.screentime_runs r
        JOIN ml.analysis_media_assets va ON va.id = r.video_asset_id
        WHERE r.run_type = 'cast_screentime' AND va.show_id = %s
          {classification_clause}
        ORDER BY r.created_at DESC
        LIMIT %s
        """,
        params,
    )


def get_run_artifact(run_id: str, artifact_key: str) -> dict[str, Any] | None:
    return pg.fetch_one(
        """
        SELECT *
        FROM ml.screentime_artifacts
        WHERE run_id = %s
          AND artifact_key = %s
        LIMIT 1
        """,
        [_normalize(run_id), artifact_key],
    )


def list_publish_versions(video_asset_id: str) -> list[dict[str, Any]]:
    return pg.fetch_all(
        """
        SELECT
          pv.*,
          r.status AS run_status,
          r.review_status,
          r.effective_runtime_seconds,
          va.legacy_screenalytics_video_asset_id,
          va.video_class,
          va.promo_subtype,
          va.media_type,
          va.media_kind,
          va.show_id,
          va.season_id,
          va.episode_id
        FROM ml.screentime_publications pv
        JOIN ml.screentime_runs r ON r.id = pv.run_id
        JOIN ml.analysis_media_assets va ON va.id = pv.video_asset_id
        WHERE pv.video_asset_id = %s
        ORDER BY pv.version_number DESC, pv.published_at DESC
        """,
        [_normalize(video_asset_id)],
    )


def get_current_publish_version(video_asset_id: str) -> dict[str, Any] | None:
    return pg.fetch_one(
        """
        SELECT *
        FROM ml.screentime_publications
        WHERE video_asset_id = %s
          AND is_current = true
        LIMIT 1
        """,
        [_normalize(video_asset_id)],
    )


def get_publish_version_for_run(run_id: str) -> dict[str, Any] | None:
    return pg.fetch_one(
        """
        SELECT *
        FROM ml.screentime_publications
        WHERE run_id = %s
        LIMIT 1
        """,
        [_normalize(run_id)],
    )


def publish_run(
    *,
    run_id: str,
    video_asset_id: str,
    published_by: str | None,
    notes_json: dict[str, Any] | None,
    metrics_snapshot_json: dict[str, Any],
) -> dict[str, Any]:
    with pg.db_connection() as conn, pg.db_cursor(conn=conn) as cur:
        existing = pg.fetch_one_with_cursor(
            cur,
            """
            SELECT *
            FROM ml.screentime_publications
            WHERE run_id = %s
            LIMIT 1
            """,
            [_normalize(run_id)],
        )
        if existing:
            return existing

        current = pg.fetch_one_with_cursor(
            cur,
            """
            SELECT *
            FROM ml.screentime_publications
            WHERE video_asset_id = %s
              AND is_current = true
            LIMIT 1
            """,
            [_normalize(video_asset_id)],
        )
        next_version_number = int(current["version_number"]) + 1 if current and current.get("version_number") else 1
        if current:
            cur.execute(
                """
                UPDATE ml.screentime_publications
                SET is_current = false,
                    updated_at = now()
                WHERE id = %s
                """,
                [_normalize(current["id"])],
            )

        cur.execute(
            """
            INSERT INTO ml.screentime_publications
              (video_asset_id, run_id, version_number, published_by, notes_json, metrics_snapshot_json, is_current)
            VALUES
              (%s, %s, %s, %s, %s, %s, true)
            RETURNING *
            """,
            [
                _normalize(video_asset_id),
                _normalize(run_id),
                next_version_number,
                published_by,
                _json(notes_json or {}),
                _json(metrics_snapshot_json),
            ],
        )
        row = cur.fetchone()
        return dict(row) if row else {}


def replace_reference_fingerprints_for_run(
    *,
    run_id: str,
    show_id: str,
    season_id: str | None,
    episode_id: str | None,
    video_asset_id: str,
    fingerprints: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows = [
        (
            _normalize(show_id),
            _normalize(season_id),
            _normalize(episode_id),
            _normalize(video_asset_id),
            _normalize(run_id),
            item.get("scene_key"),
            item.get("fingerprint_type"),
            item.get("fingerprint_hash"),
            item.get("start_ms"),
            item.get("end_ms"),
            item.get("duration_ms"),
            _json(item.get("metadata", {})),
        )
        for item in fingerprints
    ]
    with pg.db_connection() as conn, pg.db_cursor(conn=conn) as cur:
        cur.execute(
            "DELETE FROM ml.screentime_reference_fingerprints WHERE run_id = %s",
            [_normalize(run_id)],
        )
        if not rows:
            return []
        sql = (
            "INSERT INTO ml.screentime_reference_fingerprints "
            "(show_id, season_id, episode_id, video_asset_id, run_id, scene_key, fingerprint_type, fingerprint_hash, "
            " start_ms, end_ms, duration_ms, metadata) "
            "VALUES %s RETURNING *"
        )
        return pg.execute_values_returning(sql, rows, conn=conn)


def list_reference_fingerprints_for_show(show_id: str) -> list[dict[str, Any]]:
    return pg.fetch_all(
        """
        SELECT *
        FROM ml.screentime_reference_fingerprints
        WHERE show_id = %s
        ORDER BY created_at DESC, scene_key ASC
        """,
        [_normalize(show_id)],
    )


def upsert_suggestion_decision(payload: dict[str, Any]) -> dict[str, Any]:
    rows = pg.execute_returning(
        """
        INSERT INTO ml.screentime_review_state (
          show_id,
          season_id,
          episode_id,
          owner_scope,
          owner_entity_id,
          video_asset_id,
          run_id,
          review_kind,
          review_key,
          person_id,
          decision,
          notes_json,
          payload_json,
          decided_by,
          decided_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, 'suggestion_decision', %s, %s, %s, %s, %s, now())
        ON CONFLICT (owner_scope, owner_entity_id, review_kind, review_key) DO UPDATE SET
          show_id = EXCLUDED.show_id,
          season_id = EXCLUDED.season_id,
          episode_id = EXCLUDED.episode_id,
          video_asset_id = EXCLUDED.video_asset_id,
          run_id = EXCLUDED.run_id,
          person_id = EXCLUDED.person_id,
          decision = EXCLUDED.decision,
          notes_json = EXCLUDED.notes_json,
          payload_json = EXCLUDED.payload_json,
          decided_by = EXCLUDED.decided_by,
          decided_at = EXCLUDED.decided_at,
          updated_at = now()
        RETURNING *
        """,
        [
            _normalize(payload.get("show_id")),
            _normalize(payload.get("season_id")),
            _normalize(payload.get("episode_id")),
            payload.get("owner_scope"),
            _normalize(payload.get("owner_entity_id")),
            _normalize(payload.get("video_asset_id")),
            _normalize(payload.get("run_id")),
            payload.get("suggestion_key"),
            _normalize(payload.get("person_id")),
            payload.get("decision"),
            _json(payload.get("notes_json", {})),
            _json(payload.get("suggestion_payload", {})),
            payload.get("decided_by"),
        ],
    )
    if not rows:
        return {}
    row = rows[0]
    row["suggestion_key"] = row.get("review_key")
    row["suggestion_payload"] = row.get("payload_json")
    return row


def list_suggestion_decisions_for_context(
    *, show_id: str, season_id: str | None, episode_id: str | None
) -> list[dict[str, Any]]:
    clauses = ["(owner_scope = 'show' AND owner_entity_id = %s::uuid)"]
    params: list[Any] = [_normalize(show_id)]
    if season_id:
        clauses.append("(owner_scope = 'season' AND owner_entity_id = %s::uuid)")
        params.append(_normalize(season_id))
    if episode_id:
        clauses.append("(owner_scope = 'episode' AND owner_entity_id = %s::uuid)")
        params.append(_normalize(episode_id))
    return pg.fetch_all(
        f"""
        SELECT
          d.*,
          d.review_key AS suggestion_key,
          d.payload_json AS suggestion_payload,
          p.full_name AS display_name
        FROM ml.screentime_review_state d
        LEFT JOIN core.people p ON p.id = d.person_id
        WHERE {" OR ".join(clauses)}
          AND d.review_kind = 'suggestion_decision'
        ORDER BY d.decided_at DESC, d.updated_at DESC
        """,
        params,
    )


def upsert_unknown_review_state(payload: dict[str, Any]) -> dict[str, Any]:
    rows = pg.execute_returning(
        """
        INSERT INTO ml.screentime_review_state (
          show_id,
          season_id,
          episode_id,
          owner_scope,
          owner_entity_id,
          video_asset_id,
          run_id,
          review_kind,
          review_key,
          queue_group,
          candidate_person_id,
          decision,
          escalation_level,
          recommended_action,
          notes_json,
          payload_json,
          decided_by,
          decided_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, 'unknown_review', %s, %s, %s, %s, %s, %s, %s, %s, now())
        ON CONFLICT (owner_scope, owner_entity_id, review_kind, review_key) DO UPDATE SET
          show_id = EXCLUDED.show_id,
          season_id = EXCLUDED.season_id,
          episode_id = EXCLUDED.episode_id,
          video_asset_id = EXCLUDED.video_asset_id,
          run_id = EXCLUDED.run_id,
          queue_group = EXCLUDED.queue_group,
          candidate_person_id = EXCLUDED.candidate_person_id,
          decision = EXCLUDED.decision,
          escalation_level = EXCLUDED.escalation_level,
          recommended_action = EXCLUDED.recommended_action,
          notes_json = EXCLUDED.notes_json,
          payload_json = EXCLUDED.payload_json,
          decided_by = EXCLUDED.decided_by,
          decided_at = EXCLUDED.decided_at,
          updated_at = now()
        RETURNING *
        """,
        [
            _normalize(payload.get("show_id")),
            _normalize(payload.get("season_id")),
            _normalize(payload.get("episode_id")),
            payload.get("owner_scope"),
            _normalize(payload.get("owner_entity_id")),
            _normalize(payload.get("video_asset_id")),
            _normalize(payload.get("run_id")),
            payload.get("queue_key"),
            payload.get("queue_group"),
            _normalize(payload.get("candidate_person_id")),
            payload.get("decision"),
            payload.get("escalation_level"),
            payload.get("recommended_action"),
            _json(payload.get("notes_json", {})),
            _json(payload.get("queue_payload", {})),
            payload.get("decided_by"),
        ],
    )
    if not rows:
        return {}
    row = rows[0]
    row["queue_key"] = row.get("review_key")
    row["queue_payload"] = row.get("payload_json")
    return row


def list_unknown_review_state_for_context(
    *, show_id: str, season_id: str | None, episode_id: str | None
) -> list[dict[str, Any]]:
    clauses = ["(owner_scope = 'show' AND owner_entity_id = %s::uuid)"]
    params: list[Any] = [_normalize(show_id)]
    if season_id:
        clauses.append("(owner_scope = 'season' AND owner_entity_id = %s::uuid)")
        params.append(_normalize(season_id))
    if episode_id:
        clauses.append("(owner_scope = 'episode' AND owner_entity_id = %s::uuid)")
        params.append(_normalize(episode_id))
    return pg.fetch_all(
        f"""
        SELECT
          q.*,
          q.review_key AS queue_key,
          q.payload_json AS queue_payload,
          p.full_name AS candidate_display_name
        FROM ml.screentime_review_state q
        LEFT JOIN core.people p ON p.id = q.candidate_person_id
        WHERE {" OR ".join(clauses)}
          AND q.review_kind = 'unknown_review'
        ORDER BY q.decided_at DESC, q.updated_at DESC
        """,
        params,
    )


def list_current_published_versions_for_show(show_id: str) -> list[dict[str, Any]]:
    return pg.fetch_all(
        """
        SELECT
          pv.*,
          va.show_id,
          va.season_id,
          va.episode_id,
          va.legacy_screenalytics_video_asset_id,
          va.video_class,
          va.promo_subtype,
          va.media_type,
          va.media_kind
        FROM ml.screentime_publications pv
        JOIN ml.analysis_media_assets va ON va.id = pv.video_asset_id
        WHERE pv.is_current = true
          AND va.show_id = %s
          AND va.media_type = 'episode'
        ORDER BY pv.published_at DESC
        """,
        [_normalize(show_id)],
    )


def list_current_published_versions_for_season(season_id: str) -> list[dict[str, Any]]:
    return pg.fetch_all(
        """
        SELECT
          pv.*,
          va.show_id,
          va.season_id,
          va.episode_id,
          va.legacy_screenalytics_video_asset_id,
          va.video_class,
          va.promo_subtype,
          va.media_type,
          va.media_kind
        FROM ml.screentime_publications pv
        JOIN ml.analysis_media_assets va ON va.id = pv.video_asset_id
        WHERE pv.is_current = true
          AND va.season_id = %s
          AND va.media_type = 'episode'
        ORDER BY pv.published_at DESC
        """,
        [_normalize(season_id)],
    )


def reconcile_stale_runs(*, stale_after_seconds: int, show_id: str | None = None) -> list[dict[str, Any]]:
    where_show = "AND va.show_id = %s::uuid" if show_id else ""
    return pg.execute_returning(
        f"""
        UPDATE ml.screentime_runs AS r
        SET status = %s,
            error_message = CASE
              WHEN r.status = 'queued' THEN 'worker_dispatch_expired'
              ELSE 'worker_heartbeat_expired'
            END,
            completed_at = COALESCE(r.completed_at, NOW()),
            worker_heartbeat_at = NOW(),
            updated_at = NOW()
        FROM ml.analysis_media_assets AS va
        WHERE r.video_asset_id = va.id
          AND r.run_type = 'cast_screentime'
          AND (
            (
              r.status = 'running'
              AND COALESCE(r.worker_heartbeat_at, r.started_at, r.created_at) < NOW() - (%s::int * INTERVAL '1 second')
            )
            OR (
              r.status = 'queued'
              AND COALESCE(r.dispatch_accepted_at, r.created_at) < NOW() - (%s::int * INTERVAL '1 second')
            )
          )
          {where_show}
        RETURNING
          r.id::text AS id,
          r.video_asset_id::text AS video_asset_id,
          va.show_id::text AS show_id,
          r.status,
          r.error_message,
          r.completed_at,
          r.worker_heartbeat_at,
          r.dispatch_status,
          r.dispatch_job_id
        """,
        ["failed", int(stale_after_seconds), int(stale_after_seconds), *([show_id] if show_id else [])],
    )
