"""Repository functions for screenalytics v2 runs tables (direct SQL)."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from psycopg2.extras import Json

from trr_backend.db import pg


class ScreenalyticsRepositoryError(RuntimeError):
    pass


def _json(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return Json(value)
    return value


def _normalize(value: Any) -> Any:
    if isinstance(value, UUID):
        return str(value)
    return value


def create_video_asset(payload: dict[str, Any]) -> dict[str, Any]:
    sql = (
        "INSERT INTO screenalytics.video_assets "
        "(episode_id, season_id, show_id, media_asset_id, source_url, duration_seconds, metadata) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s) "
        "RETURNING *"
    )
    params = [
        _normalize(payload.get("episode_id")),
        _normalize(payload.get("season_id")),
        _normalize(payload.get("show_id")),
        _normalize(payload.get("media_asset_id")),
        payload.get("source_url"),
        payload.get("duration_seconds"),
        _json(payload.get("metadata", {})),
    ]
    rows = pg.execute_returning(sql, params)
    return rows[0] if rows else {}


def get_video_asset(video_asset_id: str) -> dict[str, Any] | None:
    sql = "SELECT * FROM screenalytics.video_assets WHERE id = %s"
    return pg.fetch_one(sql, [video_asset_id])


def create_run(payload: dict[str, Any]) -> dict[str, Any]:
    sql = (
        "INSERT INTO screenalytics.runs_v2 "
        "(video_asset_id, status, run_config_json, config_hash, candidate_cast_snapshot_json) "
        "VALUES (%s, %s, %s, %s, %s) "
        "RETURNING *"
    )
    params = [
        _normalize(payload.get("video_asset_id")),
        payload.get("status", "pending"),
        _json(payload.get("run_config_json", {})),
        payload.get("config_hash"),
        _json(payload.get("candidate_cast_snapshot_json", [])),
    ]
    rows = pg.execute_returning(sql, params)
    return rows[0] if rows else {}


def get_run(run_id: str) -> dict[str, Any] | None:
    sql = "SELECT * FROM screenalytics.runs_v2 WHERE id = %s"
    return pg.fetch_one(sql, [run_id])


def get_run_with_video_asset(run_id: str) -> dict[str, Any] | None:
    sql = (
        "SELECT r.*, "
        "va.episode_id, va.season_id, va.show_id, va.media_asset_id, "
        "va.source_url, va.duration_seconds, va.metadata "
        "FROM screenalytics.runs_v2 r "
        "JOIN screenalytics.video_assets va ON va.id = r.video_asset_id "
        "WHERE r.id = %s"
    )
    return pg.fetch_one(sql, [run_id])


def update_run(run_id: str, payload: dict[str, Any]) -> dict[str, Any] | None:
    if not payload:
        return get_run(run_id)

    assignments = []
    params: list[Any] = []
    for key, value in payload.items():
        assignments.append(f"{key} = %s")
        params.append(_json(_normalize(value)))

    params.append(run_id)
    sql = f"UPDATE screenalytics.runs_v2 SET {', '.join(assignments)} WHERE id = %s RETURNING *"
    rows = pg.execute_returning(sql, params)
    return rows[0] if rows else None


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
        "INSERT INTO screenalytics.run_artifacts "
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


def upsert_run_person_metrics(run_id: str, metrics: list[dict[str, Any]]) -> list[dict[str, Any]]:
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
    sql = (
        "INSERT INTO screenalytics.run_person_metrics "
        "(run_id, person_id, screen_time_seconds, frame_count, confidence_avg, metadata) "
        "VALUES %s "
        "ON CONFLICT (run_id, person_id) DO UPDATE SET "
        "screen_time_seconds = EXCLUDED.screen_time_seconds, "
        "frame_count = EXCLUDED.frame_count, "
        "confidence_avg = EXCLUDED.confidence_avg, "
        "metadata = EXCLUDED.metadata, "
        "updated_at = now() "
        "RETURNING *"
    )
    return pg.execute_values_returning(sql, rows)


def list_leaderboard(run_id: str) -> list[dict[str, Any]]:
    sql = "SELECT * FROM screenalytics.run_person_metrics WHERE run_id = %s ORDER BY screen_time_seconds DESC"
    return pg.fetch_all(sql, [_normalize(run_id)])


def list_unknown_clusters(run_id: str) -> list[dict[str, Any]]:
    sql = "SELECT * FROM screenalytics.unknown_clusters WHERE run_id = %s"
    return pg.fetch_all(sql, [_normalize(run_id)])


def upsert_unknown_clusters(run_id: str, clusters: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = [
        (
            _normalize(run_id),
            cluster.get("cluster_id"),
            cluster.get("track_count", 0),
            cluster.get("preview_s3_key"),
            _json(cluster.get("metadata", {})),
        )
        for cluster in clusters
    ]
    sql = (
        "INSERT INTO screenalytics.unknown_clusters "
        "(run_id, cluster_id, track_count, preview_s3_key, metadata) "
        "VALUES %s "
        "ON CONFLICT (run_id, cluster_id) DO UPDATE SET "
        "track_count = EXCLUDED.track_count, "
        "preview_s3_key = EXCLUDED.preview_s3_key, "
        "metadata = EXCLUDED.metadata, "
        "updated_at = now() "
        "RETURNING *"
    )
    return pg.execute_values_returning(sql, rows)


def assign_unknown_cluster(
    run_id: str,
    cluster_id: str,
    person_id: str,
    assigned_by: str | None,
) -> dict[str, Any] | None:
    now = datetime.now(UTC).isoformat()
    sql = (
        "UPDATE screenalytics.unknown_clusters "
        "SET assigned_person_id = %s, assigned_by = %s, assigned_at = %s, updated_at = now() "
        "WHERE run_id = %s AND cluster_id = %s "
        "RETURNING *"
    )
    rows = pg.execute_returning(
        sql,
        [_normalize(person_id), assigned_by, now, _normalize(run_id), cluster_id],
    )
    return rows[0] if rows else None
