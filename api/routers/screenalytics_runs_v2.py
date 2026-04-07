"""Screenalytics v2 run state endpoints (service-to-service)."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from api.screenalytics_auth import require_screenalytics_service_token
from trr_backend.repositories import screenalytics_runs

router = APIRouter(prefix="/screenalytics/v2", tags=["screenalytics-v2"])


class VideoAssetCreateRequest(BaseModel):
    episode_id: UUID | None = None
    season_id: UUID | None = None
    show_id: UUID | None = None
    media_asset_id: UUID | None = None
    source_url: str | None = None
    duration_seconds: float | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class RunCreateRequest(BaseModel):
    video_asset_id: UUID
    run_config_json: dict[str, Any] = Field(default_factory=dict)
    config_hash: str | None = None
    candidate_cast_snapshot_json: list[dict[str, Any]] = Field(default_factory=list)


class RunStatusUpdateRequest(BaseModel):
    status: str
    error_message: str | None = None
    manifest_key: str | None = None
    result_contract_version: str | None = None
    status_reason: str | None = None
    summary_counts: dict[str, Any] | None = None


class ArtifactItem(BaseModel):
    artifact_key: str
    artifact_kind: str
    s3_key: str
    schema_version: str | None = None
    content_type: str | None = None
    checksum_sha256: str | None = None
    row_count: int | None = None


class ArtifactsUpsertRequest(BaseModel):
    artifacts: list[ArtifactItem]


class MetricItem(BaseModel):
    person_id: UUID
    screen_time_seconds: float
    frame_count: int
    confidence_avg: float | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class MetricsUpsertRequest(BaseModel):
    metrics: list[MetricItem]


class UnknownClusterItem(BaseModel):
    cluster_id: str
    track_count: int = 0
    preview_s3_key: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class UnknownClustersUpsertRequest(BaseModel):
    clusters: list[UnknownClusterItem]


class UnknownClusterAssignRequest(BaseModel):
    person_id: UUID
    assigned_by: str | None = None


def _validate_video_asset_payload(payload: VideoAssetCreateRequest) -> None:
    if not (payload.episode_id or payload.season_id or payload.show_id):
        raise HTTPException(
            status_code=400,
            detail="episode_id, season_id, or show_id is required",
        )
    if not (payload.media_asset_id or payload.source_url):
        raise HTTPException(
            status_code=400,
            detail="media_asset_id or source_url is required",
        )


def _compute_config_hash(config: dict[str, Any]) -> str:
    serialized = json.dumps(config, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(serialized).hexdigest()


@router.post("/video-assets")
def create_video_asset(
    request: VideoAssetCreateRequest,
    _: None = Depends(require_screenalytics_service_token),
) -> dict[str, Any]:
    _validate_video_asset_payload(request)
    payload = request.model_dump()
    return screenalytics_runs.create_video_asset(payload)


@router.get("/video-assets/{video_asset_id}")
def get_video_asset(
    video_asset_id: UUID,
    _: None = Depends(require_screenalytics_service_token),
) -> dict[str, Any]:
    result = screenalytics_runs.get_video_asset(str(video_asset_id))
    if not result:
        raise HTTPException(status_code=404, detail="Video asset not found")
    return result


@router.post("/runs")
def create_run(
    request: RunCreateRequest,
    _: None = Depends(require_screenalytics_service_token),
) -> dict[str, Any]:
    payload = request.model_dump()
    if not payload.get("config_hash"):
        payload["config_hash"] = _compute_config_hash(payload.get("run_config_json", {}))
    return screenalytics_runs.create_run(payload)


@router.get("/runs/{run_id}")
def get_run(
    run_id: UUID,
    _: None = Depends(require_screenalytics_service_token),
) -> dict[str, Any]:
    run = screenalytics_runs.get_run_with_video_asset(str(run_id))
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    return run


@router.patch("/runs/{run_id}/status")
def update_run_status(
    run_id: UUID,
    request: RunStatusUpdateRequest,
    _: None = Depends(require_screenalytics_service_token),
) -> dict[str, Any]:
    current = screenalytics_runs.get_run(str(run_id))
    if not current:
        raise HTTPException(status_code=404, detail="Run not found")

    payload: dict[str, Any] = {
        "status": request.status,
        "error_message": request.error_message,
        "manifest_key": request.manifest_key,
        "result_contract_version": request.result_contract_version,
        "status_reason": request.status_reason,
        "summary_counts": request.summary_counts,
    }

    now = datetime.now(UTC).isoformat()
    if request.status == "running" and not current.get("started_at"):
        payload["started_at"] = now
    if request.status in {"success", "failed", "cancelled"} and not current.get("completed_at"):
        payload["completed_at"] = now

    result = screenalytics_runs.update_run(str(run_id), payload)
    if not result:
        raise HTTPException(status_code=404, detail="Run not found")
    return result


@router.get("/runs/{run_id}/result-bundle")
def get_result_bundle(
    run_id: UUID,
    _: None = Depends(require_screenalytics_service_token),
) -> dict[str, Any]:
    result = screenalytics_runs.get_result_bundle(str(run_id))
    if not result:
        raise HTTPException(status_code=404, detail="Run not found")
    return result


@router.post("/runs/{run_id}/artifacts:upsert")
def upsert_artifacts(
    run_id: UUID,
    request: ArtifactsUpsertRequest,
    _: None = Depends(require_screenalytics_service_token),
) -> list[dict[str, Any]]:
    artifacts = [item.model_dump() for item in request.artifacts]
    return screenalytics_runs.upsert_run_artifacts(str(run_id), artifacts)


@router.post("/runs/{run_id}/person-metrics:upsert")
def upsert_person_metrics(
    run_id: UUID,
    request: MetricsUpsertRequest,
    _: None = Depends(require_screenalytics_service_token),
) -> list[dict[str, Any]]:
    metrics = [item.model_dump() for item in request.metrics]
    return screenalytics_runs.upsert_run_person_metrics(str(run_id), metrics)


@router.get("/runs/{run_id}/unknown-clusters")
def list_unknown_clusters(
    run_id: UUID,
    _: None = Depends(require_screenalytics_service_token),
) -> list[dict[str, Any]]:
    return screenalytics_runs.list_unknown_clusters(str(run_id))


@router.post("/runs/{run_id}/unknown-clusters:upsert")
def upsert_unknown_clusters(
    run_id: UUID,
    request: UnknownClustersUpsertRequest,
    _: None = Depends(require_screenalytics_service_token),
) -> list[dict[str, Any]]:
    clusters = [item.model_dump() for item in request.clusters]
    return screenalytics_runs.upsert_unknown_clusters(str(run_id), clusters)


@router.post("/runs/{run_id}/unknown-clusters/{cluster_id}/assign")
def assign_unknown_cluster(
    run_id: UUID,
    cluster_id: str,
    request: UnknownClusterAssignRequest,
    _: None = Depends(require_screenalytics_service_token),
) -> dict[str, Any]:
    result = screenalytics_runs.assign_unknown_cluster(
        str(run_id),
        cluster_id,
        str(request.person_id),
        request.assigned_by,
    )
    if not result:
        raise HTTPException(status_code=404, detail="Cluster not found")
    return result


@router.get("/runs/{run_id}/leaderboard")
def get_leaderboard(
    run_id: UUID,
    _: None = Depends(require_screenalytics_service_token),
) -> list[dict[str, Any]]:
    return screenalytics_runs.list_leaderboard(str(run_id))
