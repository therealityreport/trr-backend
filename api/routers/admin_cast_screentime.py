"""Admin and internal control-plane routes for cast screentime runs."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import subprocess
from datetime import UTC, datetime, timedelta
from typing import Any, Literal
from urllib.parse import parse_qs, urlparse
from uuid import UUID, uuid4

import requests
from botocore.exceptions import ClientError
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from api.auth import CastScreentimeAdminUser
from api.screenalytics_auth import require_screenalytics_service_token
from trr_backend.clients import screenalytics_cast_screentime
from trr_backend.media.s3_mirror import build_public_object_url, get_cdn_base_url, get_s3_bucket, get_s3_client
from trr_backend.repositories import cast_screentime
from trr_backend.socials.youtube import YouTubeScraper, resolve_youtube_media

router = APIRouter(tags=["admin-cast-screentime"])
LOGGER = logging.getLogger(__name__)

_DEFAULT_UPLOAD_EXPIRY_MINUTES = 60
_TERMINAL_STATUSES = {"success", "failed", "cancelled"}
_DEFAULT_STALE_AFTER_SECONDS = 1800
_DEFAULT_SCREENALYTICS_ARTIFACT_BUCKET = "screenalytics-artifacts-prod"


class UploadSessionCreateRequest(BaseModel):
    owner_scope: Literal["show", "season", "episode"] = "season"
    owner_id: UUID | None = None
    show_id: UUID | None = None
    season_id: UUID | None = None
    episode_id: UUID | None = None
    filename: str = "upload.mp4"
    content_type: str = "video/mp4"
    expected_size_bytes: int | None = None
    expected_checksum_sha256: str | None = None
    media_type: Literal["episode", "trailer", "extras"] | None = None
    media_kind: str | None = None
    video_class: Literal["episode", "promo"] | None = "episode"
    promo_subtype: Literal["trailer", "episode_teaser"] | None = None


class ImportVideoAssetRequest(BaseModel):
    source_mode: Literal["youtube_url", "external_url", "social_youtube_row"]
    source_url: str | None = None
    social_youtube_video_id: UUID | None = None
    owner_scope: Literal["show", "season", "episode"] = "season"
    owner_id: UUID
    media_type: Literal["episode", "trailer", "extras"] | None = None
    media_kind: str | None = None
    video_class: Literal["episode", "promo"] | None = "promo"
    promo_subtype: Literal["trailer", "episode_teaser"] | None = "trailer"


class UploadSessionCompleteRequest(BaseModel):
    upload_session_id: UUID


class CreateRunRequest(BaseModel):
    run_config_json: dict[str, Any] = Field(default_factory=dict)


class RunStatusUpdateRequest(BaseModel):
    status: str
    error_message: str | None = None
    manifest_key: str | None = None


class HeartbeatRequest(BaseModel):
    status: str | None = None


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


class MetricsReplaceRequest(BaseModel):
    metrics: list[MetricItem]


class SegmentItem(BaseModel):
    segment_key: str
    person_id: UUID | None = None
    start_ms: int
    end_ms: int
    duration_ms: int
    frame_count: int = 0
    confidence_score: float | None = None
    similarity_score: float | None = None
    pose_bucket: str | None = None
    assignment_source: str
    is_counted: bool = True
    classification_json: dict[str, Any] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)


class SegmentsReplaceRequest(BaseModel):
    segments: list[SegmentItem]


class EvidenceItem(BaseModel):
    segment_key: str
    evidence_key: str
    evidence_type: str
    timestamp_ms: int
    object_key: str
    content_type: str | None = None
    ttl_expires_at: datetime | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class EvidenceReplaceRequest(BaseModel):
    evidence: list[EvidenceItem]


class ExcludedSectionItem(BaseModel):
    section_key: str
    section_type: str
    start_ms: int
    end_ms: int
    duration_ms: int
    detection_source: str
    confidence_score: float | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class ExcludedSectionsReplaceRequest(BaseModel):
    excluded_sections: list[ExcludedSectionItem]


class FinalizeRunRequest(BaseModel):
    status: str
    manifest_key: str | None = None
    error_message: str | None = None
    effective_runtime_seconds: float | None = None
    review_status: str | None = None


class ReviewStatusRequest(BaseModel):
    review_status: str
    notes: dict[str, Any] = Field(default_factory=dict)


class PublishRunRequest(BaseModel):
    notes: dict[str, Any] = Field(default_factory=dict)


class DecisionActionRequest(BaseModel):
    decision: Literal["accept", "reject", "defer"]
    decision_scope: Literal["show", "season", "episode"] | None = None
    notes: dict[str, Any] = Field(default_factory=dict)


class SegmentClipRequest(BaseModel):
    mode: str = Field(default="exact", pattern="^(exact|timestamp)$")
    duration_seconds: int | None = Field(default=None, ge=1, le=20)
    ttl_days: int = Field(default=7, ge=1, le=30)


def _validate_upload_scope(payload: UploadSessionCreateRequest) -> None:
    if payload.owner_id:
        return
    if not (payload.show_id or payload.season_id or payload.episode_id):
        raise HTTPException(
            status_code=400,
            detail="owner_id or one of show_id, season_id, episode_id is required",
        )


def _normalize_media_kind(value: Any) -> str | None:
    candidate = str(value or "").strip()
    return candidate or None


def _compat_media_type_from_legacy(video_class: str | None, promo_subtype: str | None) -> tuple[str, str | None]:
    normalized_video_class = str(video_class or "").strip().lower() or "episode"
    normalized_promo_subtype = str(promo_subtype or "").strip().lower() or None
    if normalized_video_class == "episode":
        return "episode", None
    if normalized_promo_subtype == "trailer":
        return "trailer", None
    if normalized_promo_subtype == "episode_teaser":
        return "extras", "episode_teaser"
    return "extras", None


def _legacy_fields_for_media_type(media_type: str, media_kind: str | None) -> tuple[str, str | None]:
    if media_type == "episode":
        return "episode", None
    if media_type == "trailer":
        return "promo", "trailer"
    if media_kind == "episode_teaser":
        return "promo", "episode_teaser"
    return "promo", None


def _normalize_media_classification(
    *,
    media_type: str | None,
    media_kind: str | None,
    video_class: str | None,
    promo_subtype: str | None,
) -> dict[str, str | None]:
    normalized_media_type = str(media_type or "").strip().lower() or None
    normalized_media_kind = _normalize_media_kind(media_kind)
    if normalized_media_type not in {"episode", "trailer", "extras"}:
        normalized_media_type, normalized_media_kind = _compat_media_type_from_legacy(video_class, promo_subtype)
    legacy_video_class, legacy_promo_subtype = _legacy_fields_for_media_type(
        normalized_media_type, normalized_media_kind
    )
    return {
        "media_type": normalized_media_type,
        "media_kind": normalized_media_kind,
        "video_class": legacy_video_class,
        "promo_subtype": legacy_promo_subtype,
    }


def _validate_media_classification(
    *,
    media_type: str,
    media_kind: str | None,
    owner_scope: str,
) -> None:
    if media_type == "episode" and owner_scope != "episode":
        raise HTTPException(status_code=400, detail="episode media_type requires owner_scope=episode")
    if media_type == "episode" and media_kind:
        raise HTTPException(status_code=400, detail="media_kind is only valid for trailer or extras assets")


def _derive_owner_scope(show_id: str | None, season_id: str | None, episode_id: str | None) -> str | None:
    if episode_id:
        return "episode"
    if season_id:
        return "season"
    if show_id:
        return "show"
    return None


def _nullable_actor_uuid(raw: Any) -> str | None:
    candidate = str(raw or "").strip()
    if not candidate:
        return None
    try:
        return str(UUID(candidate))
    except Exception:  # noqa: BLE001
        return None


def _annotate_video_asset_row(row: dict[str, Any] | None) -> dict[str, Any]:
    if not row:
        return {}
    payload = dict(row)
    show_id = str(payload.get("show_id") or "").strip() or None
    season_id = str(payload.get("season_id") or "").strip() or None
    episode_id = str(payload.get("episode_id") or "").strip() or None
    media = _normalize_media_classification(
        media_type=str(payload.get("media_type") or "").strip() or None,
        media_kind=str(payload.get("media_kind") or "").strip() or None,
        video_class=str(payload.get("video_class") or "").strip() or None,
        promo_subtype=str(payload.get("promo_subtype") or "").strip() or None,
    )
    payload["owner_scope"] = _derive_owner_scope(show_id, season_id, episode_id)
    payload["owner_id"] = episode_id or season_id or show_id
    payload.update(media)
    payload["is_publishable"] = media["media_type"] == "episode"
    if media["media_type"] != "episode":
        payload["publish_block_reason"] = "non_episode_assets_are_not_publishable"
    else:
        payload["publish_block_reason"] = None
    return payload


def _annotate_run_row(row: dict[str, Any] | None) -> dict[str, Any]:
    if not row:
        return {}
    return _annotate_video_asset_row(row)


def _read_run_artifact_payload(run_id: str, artifact_key: str) -> tuple[dict[str, Any], Any]:
    artifact = cast_screentime.get_run_artifact(run_id, artifact_key)
    if not artifact:
        raise HTTPException(status_code=404, detail="Run artifact not found")
    s3_key = str(artifact.get("s3_key") or "").strip()
    if not s3_key:
        raise HTTPException(status_code=409, detail="Run artifact does not have an object key")
    payload_bytes = _read_run_artifact_bytes(s3_key)
    content_type = str(artifact.get("content_type") or "")
    if content_type.startswith("application/json"):
        try:
            payload = json.loads(payload_bytes.decode("utf-8"))
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=500, detail=f"Artifact JSON decode failed: {exc}") from exc
    else:
        payload = payload_bytes.decode("utf-8", errors="replace")
    return artifact, payload


def _build_publish_metrics_snapshot(run: dict[str, Any]) -> dict[str, Any]:
    run_id = str(run["id"])
    leaderboard = cast_screentime.list_leaderboard(run_id)
    return {
        "run_id": run_id,
        "video_asset_id": str(run["video_asset_id"]),
        "show_id": str(run.get("show_id") or "") or None,
        "season_id": str(run.get("season_id") or "") or None,
        "episode_id": str(run.get("episode_id") or "") or None,
        "effective_runtime_seconds": run.get("effective_runtime_seconds"),
        "leaderboard": leaderboard,
    }


def _aggregate_rollup(
    published_versions: list[dict[str, Any]],
    *,
    scope_id: str,
    scope_type: str,
) -> dict[str, Any]:
    totals: dict[str, dict[str, Any]] = {}
    for version in published_versions:
        snapshot = version.get("metrics_snapshot_json")
        if not isinstance(snapshot, dict):
            continue
        leaderboard = snapshot.get("leaderboard")
        if not isinstance(leaderboard, list):
            continue
        for entry in leaderboard:
            if not isinstance(entry, dict):
                continue
            person_id = str(entry.get("person_id") or "").strip()
            if not person_id:
                continue
            state = totals.setdefault(
                person_id,
                {
                    "person_id": person_id,
                    "display_name": entry.get("display_name"),
                    "screen_time_seconds": 0.0,
                    "frame_count": 0,
                    "source_version_count": 0,
                },
            )
            state["screen_time_seconds"] += float(entry.get("screen_time_seconds") or 0.0)
            state["frame_count"] += int(entry.get("frame_count") or 0)
            state["source_version_count"] += 1
            if not state.get("display_name") and entry.get("display_name"):
                state["display_name"] = entry.get("display_name")

    leaderboard = sorted(
        (
            {
                "person_id": item["person_id"],
                "display_name": item.get("display_name"),
                "screen_time_seconds": round(float(item["screen_time_seconds"]), 3),
                "frame_count": int(item["frame_count"]),
                "source_version_count": int(item["source_version_count"]),
            }
            for item in totals.values()
        ),
        key=lambda item: (float(item["screen_time_seconds"]), int(item["frame_count"])),
        reverse=True,
    )
    return {
        scope_type: scope_id,
        "published_asset_count": len(published_versions),
        "leaderboard": leaderboard,
        "published_versions": published_versions,
    }


def _owner_scope_entity_id_for_run(run: dict[str, Any], decision_scope: str | None) -> tuple[str, str]:
    effective_scope = str(
        decision_scope
        or _derive_owner_scope(
            str(run.get("show_id") or "") or None,
            str(run.get("season_id") or "") or None,
            str(run.get("episode_id") or "") or None,
        )
        or "season"
    )
    if effective_scope == "episode":
        owner_entity_id = str(run.get("episode_id") or "").strip()
    elif effective_scope == "show":
        owner_entity_id = str(run.get("show_id") or "").strip()
    else:
        effective_scope = "season"
        owner_entity_id = str(run.get("season_id") or "").strip()
    if not owner_entity_id:
        raise HTTPException(status_code=409, detail=f"Run does not have a resolvable {effective_scope} owner scope")
    return effective_scope, owner_entity_id


def _artifact_payload_or_default(run_id: str, artifact_key: str, *, default: Any) -> Any:
    try:
        _, payload = _read_run_artifact_payload(run_id, artifact_key)
    except HTTPException as exc:
        if exc.status_code == 404:
            return default
        raise
    return payload


def _temp_upload_key(upload_session_id: str, filename: str) -> str:
    clean_name = filename.rsplit("/", 1)[-1].rsplit("\\", 1)[-1] or "upload.mp4"
    return f"tmp/uploads/cast-screentime/{upload_session_id}/{clean_name}"


def _normalize_account_handle(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    candidate = raw
    if "://" in raw or raw.lower().startswith("www."):
        parsed = urlparse(raw if "://" in raw else f"https://{raw}")
        path_parts = [segment for segment in str(parsed.path or "").split("/") if segment]
        if path_parts:
            candidate = path_parts[0]
        else:
            candidate = str(parsed.netloc or "")
    candidate = candidate.strip().lstrip("@")
    candidate = candidate.split("?")[0].split("#")[0].split("/")[0].strip().lower()
    return candidate


def _extract_youtube_handle_from_value(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    parsed = urlparse(raw if "://" in raw else f"https://{raw}")
    if parsed.netloc:
        path_parts = [segment for segment in str(parsed.path or "").split("/") if segment]
        if path_parts and path_parts[0].startswith("@"):
            return _normalize_account_handle(path_parts[0])
    if raw.startswith("@"):
        return _normalize_account_handle(raw)
    if all(ch.isalnum() or ch in "._-" for ch in raw):
        return _normalize_account_handle(raw)
    return ""


def _youtube_handles_match(expected_handle: str, candidate_handle: str) -> bool:
    expected = _normalize_account_handle(expected_handle)
    candidate = _normalize_account_handle(candidate_handle)
    if not expected or not candidate:
        return False
    if expected == candidate:
        return True
    if expected == f"{candidate}tv" or candidate == f"{expected}tv":
        return True
    if expected.startswith(candidate) or candidate.startswith(expected):
        return True
    return False


def _youtube_identity_matches(
    *,
    actual_channel_id: str | None,
    actual_candidates: list[Any],
    expected_handle: str,
    expected_channel_id: str | None,
) -> bool:
    normalized_expected_channel_id = str(expected_channel_id or "").strip()
    normalized_actual_channel_id = str(actual_channel_id or "").strip()
    if normalized_expected_channel_id and normalized_actual_channel_id:
        return normalized_actual_channel_id == normalized_expected_channel_id
    if not expected_handle:
        return True
    owner_candidates = {
        handle for handle in (_extract_youtube_handle_from_value(value) for value in actual_candidates) if handle
    }
    if owner_candidates:
        return any(_youtube_handles_match(expected_handle, candidate) for candidate in owner_candidates)
    return False


def _extract_youtube_video_id(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        raise HTTPException(status_code=400, detail="YouTube URL is required")
    parsed = urlparse(raw)
    host = (parsed.netloc or "").lower()
    path = str(parsed.path or "").strip()
    if host == "youtu.be":
        video_id = path.strip("/").split("/", 1)[0]
        if video_id:
            return video_id
    if "youtube.com" in host:
        if path == "/watch":
            video_id = parse_qs(parsed.query or "").get("v", [""])[0].strip()
            if video_id:
                return video_id
        if path.startswith("/shorts/") or path.startswith("/embed/"):
            parts = [segment for segment in path.split("/") if segment]
            if len(parts) >= 2:
                return parts[1]
    raise HTTPException(status_code=400, detail="Unable to extract YouTube video id from URL")


def _youtube_fetch_video_metadata(video_id: str) -> dict[str, Any]:
    watch_url = f"https://www.youtube.com/watch?v={video_id}"
    try:
        result = subprocess.run(
            ["yt-dlp", "--dump-single-json", "--no-playlist", "--skip-download", watch_url],
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
    except Exception:  # noqa: BLE001
        return {}
    if result.returncode != 0:
        return {}
    try:
        payload = json.loads(result.stdout or "{}")
    except ValueError:
        return {}
    return payload if isinstance(payload, dict) else {}


def _preferred_video_url(urls: list[str]) -> str:
    for candidate in urls:
        normalized = str(candidate or "").strip()
        if normalized and normalized.lower().endswith((".mp4", ".mov", ".m4v", ".webm")):
            return normalized
    for candidate in urls:
        normalized = str(candidate or "").strip()
        if normalized:
            return normalized
    raise HTTPException(status_code=502, detail="No usable video media URL was resolved")


def _public_url_to_object_key(value: str | None) -> str | None:
    url = str(value or "").strip()
    if not url:
        return None
    if url.startswith("s3://"):
        _, _, remainder = url.partition("s3://")
        _, _, key = remainder.partition("/")
        return key.strip() or None
    public_base = get_cdn_base_url().rstrip("/")
    if url.startswith(f"{public_base}/"):
        return url[len(public_base) + 1 :].strip() or None
    return None


def _video_extension_for_content_type(content_type: str | None, filename: str | None = None) -> str:
    normalized = (content_type or "").split(";", 1)[0].strip().lower()
    if normalized == "video/mp4":
        return ".mp4"
    if normalized in {"video/quicktime", "video/mov"}:
        return ".mov"
    if normalized == "video/x-m4v":
        return ".m4v"
    if filename and "." in filename:
        suffix = "." + filename.split(".")[-1].lower()
        if suffix in {".mp4", ".mov", ".m4v", ".mkv"}:
            return suffix
    return ".mp4"


def _presigned_put_url(bucket: str, key: str, *, content_type: str, expires_in_seconds: int) -> str:
    client = get_s3_client()
    return client.generate_presigned_url(
        "put_object",
        Params={"Bucket": bucket, "Key": key, "ContentType": content_type},
        ExpiresIn=expires_in_seconds,
    )


def _presigned_get_url(bucket: str, key: str, *, expires_in_seconds: int) -> str:
    client = get_s3_client()
    return client.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=expires_in_seconds,
    )


def _head_object(bucket: str, key: str) -> dict[str, Any] | None:
    client = get_s3_client()
    try:
        return client.head_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code in {"404", "NoSuchKey", "NotFound"}:
            return None
        raise


def _copy_object(bucket: str, source_key: str, dest_key: str, *, content_type: str) -> None:
    client = get_s3_client()
    client.copy_object(
        Bucket=bucket,
        CopySource={"Bucket": bucket, "Key": source_key},
        Key=dest_key,
        ContentType=content_type,
        MetadataDirective="REPLACE",
    )


def _delete_object(bucket: str, key: str) -> None:
    client = get_s3_client()
    client.delete_object(Bucket=bucket, Key=key)


def _read_object_bytes(bucket: str, key: str) -> bytes:
    client = get_s3_client()
    response = client.get_object(Bucket=bucket, Key=key)
    body = response.get("Body")
    if body is None:
        raise RuntimeError(f"object body missing for {key}")
    return body.read()


def _screenalytics_artifact_bucket() -> str:
    return (
        (os.getenv("SCREENALYTICS_OBJECT_STORAGE_BUCKET") or "").strip()
        or (os.getenv("SCREENALYTICS_OBJECT_STORE_BUCKET") or "").strip()
        or (os.getenv("SCREENALYTICS_S3_BUCKET") or "").strip()
        or _DEFAULT_SCREENALYTICS_ARTIFACT_BUCKET
    )


def _read_run_artifact_bytes(key: str) -> bytes:
    media_bucket = get_s3_bucket()
    buckets_to_try: list[str] = []
    if key.startswith(("derived/runs/", "review/evidence/runs/")):
        artifact_bucket = _screenalytics_artifact_bucket()
        buckets_to_try.append(artifact_bucket)
        if artifact_bucket != media_bucket:
            buckets_to_try.append(media_bucket)
    else:
        buckets_to_try.append(media_bucket)

    last_missing: Exception | None = None
    for bucket in buckets_to_try:
        try:
            return _read_object_bytes(bucket, key)
        except ClientError as exc:
            code = str(exc.response.get("Error", {}).get("Code", ""))
            if code in {"404", "NoSuchKey", "NotFound"}:
                last_missing = exc
                continue
            raise
    if last_missing is not None:
        raise last_missing
    raise RuntimeError(f"object body missing for {key}")


def _parse_probe_fraction(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        parsed = float(value)
        return parsed if parsed > 0 else None
    raw = str(value).strip()
    if not raw or raw in {"0/0", "N/A"}:
        return None
    if "/" in raw:
        numerator, denominator = raw.split("/", 1)
        try:
            numerator_value = float(numerator)
            denominator_value = float(denominator)
        except ValueError:
            return None
        if denominator_value == 0:
            return None
        parsed = numerator_value / denominator_value
        return parsed if parsed > 0 else None
    try:
        parsed = float(raw)
    except ValueError:
        return None
    return parsed if parsed > 0 else None


def _ffprobe_video(url: str) -> dict[str, Any]:
    if not url:
        return {"ok": False, "error": "missing_url"}

    command = [
        "ffprobe",
        "-v",
        "error",
        "-select_streams",
        "v:0",
        "-show_entries",
        "format=duration:stream=width,height,avg_frame_rate,duration",
        "-of",
        "json",
        url,
    ]
    try:
        result = subprocess.run(command, check=False, capture_output=True, text=True, timeout=30)
    except FileNotFoundError:
        return {"ok": False, "error": "ffprobe_not_found"}
    except subprocess.TimeoutExpired:
        return {"ok": False, "error": "ffprobe_timeout"}

    if result.returncode != 0:
        return {"ok": False, "error": "ffprobe_failed", "stderr": (result.stderr or "")[:500]}

    try:
        payload = json.loads(result.stdout or "")
    except ValueError:
        return {"ok": False, "error": "ffprobe_bad_json"}

    if not isinstance(payload, dict):
        return {"ok": False, "error": "ffprobe_bad_payload"}

    streams = payload.get("streams")
    if not isinstance(streams, list) or not streams:
        return {"ok": False, "error": "ffprobe_no_stream"}
    stream0 = streams[0] if isinstance(streams[0], dict) else {}
    fmt = payload.get("format")
    fmt = fmt if isinstance(fmt, dict) else {}

    duration_seconds = _parse_probe_fraction(stream0.get("duration")) or _parse_probe_fraction(fmt.get("duration"))
    fps = _parse_probe_fraction(stream0.get("avg_frame_rate"))
    width = stream0.get("width")
    height = stream0.get("height")
    return {
        "ok": True,
        "error": None,
        "duration_seconds": duration_seconds,
        "fps": fps,
        "width": int(width) if isinstance(width, (int, float)) else None,
        "height": int(height) if isinstance(height, (int, float)) else None,
    }


def _resolve_owner_context_from_request(
    *,
    owner_scope: str | None,
    owner_id: UUID | None,
    show_id: UUID | None,
    season_id: UUID | None,
    episode_id: UUID | None,
) -> dict[str, Any]:
    context = cast_screentime.resolve_owner_context(
        owner_scope=owner_scope,
        owner_id=str(owner_id) if owner_id else None,
        show_id=str(show_id) if show_id else None,
        season_id=str(season_id) if season_id else None,
        episode_id=str(episode_id) if episode_id else None,
    )
    if not context:
        raise HTTPException(status_code=404, detail="Owner scope could not be resolved")
    return context


def _resolve_official_youtube_owners(owner_context: dict[str, Any]) -> list[dict[str, Any]]:
    target_rows = cast_screentime.list_target_youtube_accounts(
        show_id=str(owner_context.get("show_id") or ""),
        season_id=str(owner_context.get("season_id") or "") or None,
    )
    handles = sorted(
        {
            _normalize_account_handle(row.get("account_handle"))
            for row in target_rows
            if _normalize_account_handle(row.get("account_handle"))
        }
    )
    if not handles:
        raise HTTPException(status_code=409, detail="No official YouTube account is configured for the selected owner")
    scraper = YouTubeScraper()
    owners: list[dict[str, Any]] = []
    for handle in handles:
        resolved = scraper.resolve_channel_identity(handle, delay=0.25)
        owners.append(
            {
                "account_handle": handle,
                "canonical_handle": _normalize_account_handle(resolved.get("canonical_handle") or handle),
                "channel_id": str(resolved.get("channel_id") or "").strip() or None,
            }
        )
    return owners


def _assert_youtube_official_match(
    *,
    actual_channel_id: str | None,
    actual_candidates: list[Any],
    expected_owners: list[dict[str, Any]],
) -> dict[str, Any]:
    for owner in expected_owners:
        if _youtube_identity_matches(
            actual_channel_id=actual_channel_id,
            actual_candidates=actual_candidates,
            expected_handle=str(owner.get("canonical_handle") or owner.get("account_handle") or ""),
            expected_channel_id=str(owner.get("channel_id") or "").strip() or None,
        ):
            return owner
    raise HTTPException(status_code=403, detail="YouTube video does not belong to an official configured channel")


def _mirror_remote_video_to_temp_object(
    *,
    source_url: str,
    temp_object_key: str,
    content_type_hint: str | None = None,
) -> dict[str, Any]:
    bucket = get_s3_bucket()
    with requests.get(source_url, stream=True, timeout=(10, 180)) as response:
        response.raise_for_status()
        content_type = (
            str(response.headers.get("Content-Type") or content_type_hint or "video/mp4").split(";", 1)[0].strip()
        )
        client = get_s3_client()
        response.raw.decode_content = True
        client.upload_fileobj(
            response.raw,
            bucket,
            temp_object_key,
            ExtraArgs={"ContentType": content_type},
        )
        return {
            "bucket": bucket,
            "content_type": content_type,
            "etag": str(response.headers.get("ETag") or "").strip().strip('"') or None,
        }


def _copy_existing_object_to_temp_object(
    *,
    source_object_key: str,
    temp_object_key: str,
    content_type_hint: str | None = None,
) -> dict[str, Any]:
    bucket = get_s3_bucket()
    source_head = _head_object(bucket, source_object_key)
    if source_head is None:
        raise HTTPException(status_code=404, detail="Hosted source object was not found")
    _copy_object(
        bucket,
        source_object_key,
        temp_object_key,
        content_type=str(source_head.get("ContentType") or content_type_hint or "video/mp4"),
    )
    return {
        "bucket": bucket,
        "content_type": str(source_head.get("ContentType") or content_type_hint or "video/mp4"),
        "etag": str(source_head.get("ETag") or "").strip().strip('"') or None,
    }


def _promote_session_to_video_asset(
    *,
    upload_session_id: UUID,
    session: dict[str, Any],
    ingest_type: str,
    source_provenance: dict[str, Any],
) -> dict[str, Any]:
    bucket = get_s3_bucket()
    temp_object_key = str(session.get("temp_object_key") or "").strip()
    head = _head_object(bucket, temp_object_key)
    if head is None:
        cast_screentime.update_media_upload_session(
            str(upload_session_id),
            {"status": "failed", "failed_at": datetime.now(UTC).isoformat(), "error_text": "uploaded object not found"},
        )
        raise HTTPException(status_code=400, detail="Uploaded object not found")

    actual_size = int(head.get("ContentLength") or 0)
    actual_content_type = str(head.get("ContentType") or session.get("content_type") or "application/octet-stream")
    expected_size = session.get("expected_size_bytes")
    if expected_size is not None and int(expected_size) != actual_size:
        cast_screentime.update_media_upload_session(
            str(upload_session_id),
            {
                "status": "failed",
                "failed_at": datetime.now(UTC).isoformat(),
                "error_text": "uploaded object size mismatch",
            },
        )
        raise HTTPException(status_code=400, detail="Uploaded object size mismatch")

    checksum = str(session.get("expected_checksum_sha256") or "").strip() or None
    etag = str(head.get("ETag") or "").strip().strip('"') or None
    if checksum and etag and checksum != etag:
        cast_screentime.update_media_upload_session(
            str(upload_session_id),
            {
                "status": "failed",
                "failed_at": datetime.now(UTC).isoformat(),
                "error_text": "uploaded object checksum mismatch",
            },
        )
        raise HTTPException(status_code=400, detail="Uploaded object checksum mismatch")

    verification_json_raw = session.get("verification_json")
    verification_json_existing = verification_json_raw if isinstance(verification_json_raw, dict) else {}
    probe = _ffprobe_video(_presigned_get_url(bucket, temp_object_key, expires_in_seconds=300))
    if not probe.get("ok"):
        cast_screentime.update_media_upload_session(
            str(upload_session_id),
            {
                "status": "failed",
                "failed_at": datetime.now(UTC).isoformat(),
                "error_text": f"video probe failed: {probe.get('error')}",
                "verification_json": {
                    **verification_json_existing,
                    "bucket": bucket,
                    "etag": etag,
                    "content_type": actual_content_type,
                    "probe": probe,
                },
            },
        )
        raise HTTPException(status_code=503, detail=f"Video probe failed: {probe.get('error')}")

    video_asset_id = str(uuid4())
    extension = _video_extension_for_content_type(
        actual_content_type,
        str(verification_json_existing.get("filename") or ""),
    )
    canonical_key = f"source/videos/{video_asset_id}/original{extension}"
    _copy_object(bucket, temp_object_key, canonical_key, content_type=actual_content_type)
    _delete_object(bucket, temp_object_key)

    verification_json = dict(verification_json_existing)
    verification_json.update(
        {
            "bucket": bucket,
            "etag": etag,
            "content_type": actual_content_type,
            "verified_at": datetime.now(UTC).isoformat(),
            "probe": probe,
            "source_provenance": source_provenance,
        }
    )

    owner_scope = str(
        session.get("owner_scope")
        or _derive_owner_scope(
            str(session.get("show_id") or "") or None,
            str(session.get("season_id") or "") or None,
            str(session.get("episode_id") or "") or None,
        )
        or "season"
    )
    media = _normalize_media_classification(
        media_type=str(session.get("media_type") or "").strip() or None,
        media_kind=str(session.get("media_kind") or "").strip() or None,
        video_class=str(session.get("video_class") or "").strip() or None,
        promo_subtype=str(session.get("promo_subtype") or "").strip() or None,
    )
    source_import_type = str(session.get("source_import_type") or "upload")

    source_json = {
        "bucket": bucket,
        "object_key": canonical_key,
        "content_type": actual_content_type,
        "size_bytes": actual_size,
        "checksum_sha256": checksum,
        "etag": etag,
        "upload_session_id": str(upload_session_id),
        "storage_provider": "r2",
        "probe": probe,
        "provenance": source_provenance,
    }

    metadata = {
        "ingest_type": ingest_type,
        "video_probe": probe,
        "owner_scope": owner_scope,
        "is_publishable": media["media_type"] == "episode",
        "media_type": media["media_type"],
    }
    if media["media_kind"]:
        metadata["media_kind"] = media["media_kind"]
    if media["promo_subtype"]:
        metadata["promo_subtype"] = media["promo_subtype"]

    video_asset = cast_screentime.create_video_asset(
        {
            "id": video_asset_id,
            "show_id": session.get("show_id"),
            "season_id": session.get("season_id"),
            "episode_id": session.get("episode_id"),
            "source_url": f"s3://{bucket}/{canonical_key}",
            "source_json": source_json,
            "duration_seconds": probe.get("duration_seconds"),
            "metadata": metadata,
            "video_class": media["video_class"],
            "promo_subtype": media["promo_subtype"],
            "media_type": media["media_type"],
            "media_kind": media["media_kind"],
            "source_import_type": source_import_type,
        }
    )
    cast_screentime.update_media_upload_session(
        str(upload_session_id),
        {
            "status": "promoted",
            "verified_size_bytes": actual_size,
            "verified_checksum_sha256": checksum,
            "verification_json": verification_json,
            "verified_at": datetime.now(UTC).isoformat(),
            "promoted_video_asset_id": video_asset_id,
        },
    )
    return _annotate_video_asset_row(video_asset)


def _default_run_config(video_asset: dict[str, Any]) -> dict[str, Any]:
    source_json = video_asset.get("source_json")
    if not isinstance(source_json, dict):
        source_json = {}
    media_type = str(video_asset.get("media_type") or "").strip().lower() or "episode"
    media_kind = _normalize_media_kind(video_asset.get("media_kind"))
    title_card_auto_exclude = media_type in {"episode", "trailer"}
    flashback_auto_exclude = media_type == "episode"
    return {
        "run_type": "cast_screentime",
        "pipeline_version": "cast_screentime_v1",
        "media_type": media_type,
        "media_kind": media_kind,
        "recognition_backend": "arcface_r100",
        "distance_metric": "cosine",
        "sampling_strategy": {
            "embed_sampling": "uniform",
            "shot_sampling": "pyscenedetect_proxy_video",
            "frame_stride": 3,
        },
        "excluded_section_types": [
            section
            for section in [
                "black_screen",
                "title_card" if title_card_auto_exclude else None,
                "flashback" if flashback_auto_exclude else None,
            ]
            if section
        ],
        "exclusion_policy": {
            "black_screen": {"auto_exclude": True},
            "title_card": {
                "detect": True,
                "auto_exclude": title_card_auto_exclude,
                "reference_match_min_confidence": 0.97 if media_type == "trailer" else 0.0,
            },
            "flashback": {
                "detect": True,
                "auto_exclude": flashback_auto_exclude,
            },
        },
        "confidence_thresholds": {
            "frontal_auto_assign": 0.90,
            "side_profile_auto_assign": 0.95,
            "uncertain_floor": 0.70,
            "cast_suggestion_policy": "conservative",
        },
        "processing_mode": "balanced",
        "clip_generation_policy": "frames_only_p0",
        "effective_runtime_policy": "exclude_marked_sections",
        "localization_mode": "download_full_local",
        "source_object_key": source_json.get("object_key"),
        "suggestion_review_policy": {
            "accepted_decisions_require_rerun_for_official_metrics": True,
        },
    }


def _merge_run_config(video_asset: dict[str, Any], user_config: dict[str, Any]) -> dict[str, Any]:
    base = _default_run_config(video_asset)
    merged = {**base, **(user_config or {})}
    merged["run_type"] = "cast_screentime"
    merged["pipeline_version"] = str(merged.get("pipeline_version") or "cast_screentime_v1")
    return merged


def _compute_config_hash(config: dict[str, Any]) -> str:
    serialized = json.dumps(config, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(serialized).hexdigest()


def _assert_cast_screentime_run(run: dict[str, Any] | None) -> dict[str, Any]:
    if not run:
        raise HTTPException(status_code=404, detail="Run not found")
    if str(run.get("run_type") or "") != "cast_screentime":
        raise HTTPException(status_code=400, detail="Run is not a cast_screentime run")
    return run


def _assert_mutable_run(run: dict[str, Any]) -> None:
    if str(run.get("status") or "") in _TERMINAL_STATUSES:
        raise HTTPException(status_code=409, detail="Run is already terminal")


def _attach_public_url(row: dict[str, Any], key_field: str = "object_key") -> dict[str, Any]:
    payload = dict(row)
    object_key = str(payload.get(key_field) or "").strip()
    if object_key:
        try:
            payload["public_url"] = build_public_object_url(object_key)
        except Exception:  # noqa: BLE001
            payload["public_url"] = None
    else:
        payload["public_url"] = None
    return payload


def _stale_after_seconds() -> int:
    raw = (os.getenv("CAST_SCREENTIME_STALE_AFTER_SECONDS") or "").strip()
    try:
        if raw:
            return max(int(raw), 60)
    except ValueError:
        return _DEFAULT_STALE_AFTER_SECONDS
    return _DEFAULT_STALE_AFTER_SECONDS


def reconcile_stale_runs_once(
    *, show_id: str | None = None, stale_after_seconds: int | None = None
) -> list[dict[str, Any]]:
    return cast_screentime.reconcile_stale_runs(
        stale_after_seconds=stale_after_seconds or _stale_after_seconds(),
        show_id=show_id,
    )


@router.post("/admin/cast-screentime/upload-sessions")
def create_upload_session(
    request: UploadSessionCreateRequest,
    admin_user: CastScreentimeAdminUser,
) -> dict[str, Any]:
    _validate_upload_scope(request)
    owner_context = _resolve_owner_context_from_request(
        owner_scope=request.owner_scope,
        owner_id=request.owner_id,
        show_id=request.show_id,
        season_id=request.season_id,
        episode_id=request.episode_id,
    )
    media = _normalize_media_classification(
        media_type=request.media_type,
        media_kind=request.media_kind,
        video_class=request.video_class,
        promo_subtype=request.promo_subtype,
    )
    _validate_media_classification(
        media_type=str(media["media_type"] or "episode"),
        media_kind=media["media_kind"],
        owner_scope=str(owner_context.get("owner_scope") or request.owner_scope or "season"),
    )
    upload_session_id = str(uuid4())
    temp_object_key = _temp_upload_key(upload_session_id, request.filename)
    expires_at = datetime.now(UTC) + timedelta(minutes=_DEFAULT_UPLOAD_EXPIRY_MINUTES)
    session_row = cast_screentime.create_media_upload_session(
        {
            "show_id": owner_context.get("show_id"),
            "season_id": owner_context.get("season_id"),
            "episode_id": owner_context.get("episode_id"),
            "created_by": _nullable_actor_uuid(admin_user.get("id")),
            "status": "pending_upload",
            "temp_object_key": temp_object_key,
            "content_type": request.content_type,
            "expected_size_bytes": request.expected_size_bytes,
            "expected_checksum_sha256": request.expected_checksum_sha256,
            "verification_json": {
                "filename": request.filename,
                "owner_scope": owner_context.get("owner_scope"),
                "owner_id": owner_context.get("owner_id"),
            },
            "expires_at": expires_at.isoformat(),
            "video_class": media["video_class"],
            "promo_subtype": media["promo_subtype"],
            "media_type": media["media_type"],
            "media_kind": media["media_kind"],
            "source_import_type": "upload",
            "owner_scope": owner_context.get("owner_scope"),
        }
    )
    bucket = get_s3_bucket()
    return {
        "upload_session_id": session_row["id"],
        "put_url": _presigned_put_url(
            bucket,
            temp_object_key,
            content_type=request.content_type,
            expires_in_seconds=_DEFAULT_UPLOAD_EXPIRY_MINUTES * 60,
        ),
        "temp_object_key": temp_object_key,
        "bucket": bucket,
        "expires_at": expires_at.isoformat(),
        "owner_scope": owner_context.get("owner_scope"),
        "owner_id": owner_context.get("owner_id"),
        "media_type": media["media_type"],
        "media_kind": media["media_kind"],
        "video_class": media["video_class"],
        "promo_subtype": media["promo_subtype"],
    }


@router.post("/admin/cast-screentime/upload-sessions/{upload_session_id}/complete")
def complete_upload_session(
    upload_session_id: UUID,
    request: UploadSessionCompleteRequest,
    _: CastScreentimeAdminUser,
) -> dict[str, Any]:
    if upload_session_id != request.upload_session_id:
        raise HTTPException(status_code=400, detail="Path/body upload session mismatch")

    session = cast_screentime.get_media_upload_session(str(upload_session_id))
    if not session:
        raise HTTPException(status_code=404, detail="Upload session not found")
    video_asset = _promote_session_to_video_asset(
        upload_session_id=upload_session_id,
        session=session,
        ingest_type="direct_r2_upload",
        source_provenance={"source_mode": "upload"},
    )
    return {
        "upload_session_id": str(upload_session_id),
        "video_asset": video_asset,
    }


@router.get("/admin/cast-screentime/video-assets/{video_asset_id}")
def get_video_asset(video_asset_id: UUID, _: CastScreentimeAdminUser) -> dict[str, Any]:
    video_asset = cast_screentime.get_video_asset(str(video_asset_id))
    if not video_asset:
        raise HTTPException(status_code=404, detail="Video asset not found")
    return _annotate_video_asset_row(video_asset)


@router.post("/admin/cast-screentime/video-assets/import")
def import_video_asset(
    request: ImportVideoAssetRequest,
    admin_user: CastScreentimeAdminUser,
) -> dict[str, Any]:
    owner_context = _resolve_owner_context_from_request(
        owner_scope=request.owner_scope,
        owner_id=request.owner_id,
        show_id=None,
        season_id=None,
        episode_id=None,
    )
    media = _normalize_media_classification(
        media_type=request.media_type,
        media_kind=request.media_kind,
        video_class=request.video_class,
        promo_subtype=request.promo_subtype,
    )
    _validate_media_classification(
        media_type=str(media["media_type"] or "episode"),
        media_kind=media["media_kind"],
        owner_scope=str(owner_context.get("owner_scope") or request.owner_scope or "season"),
    )
    upload_session_id = UUID(str(uuid4()))
    temp_object_key = _temp_upload_key(str(upload_session_id), "import.mp4")
    expires_at = datetime.now(UTC) + timedelta(minutes=_DEFAULT_UPLOAD_EXPIRY_MINUTES)
    session = cast_screentime.create_media_upload_session(
        {
            "id": str(upload_session_id),
            "show_id": owner_context.get("show_id"),
            "season_id": owner_context.get("season_id"),
            "episode_id": owner_context.get("episode_id"),
            "created_by": _nullable_actor_uuid(admin_user.get("id")),
            "status": "pending_upload",
            "temp_object_key": temp_object_key,
            "content_type": "video/mp4",
            "verification_json": {
                "owner_scope": owner_context.get("owner_scope"),
                "owner_id": owner_context.get("owner_id"),
                "source_mode": request.source_mode,
            },
            "expires_at": expires_at.isoformat(),
            "video_class": media["video_class"],
            "promo_subtype": media["promo_subtype"],
            "media_type": media["media_type"],
            "media_kind": media["media_kind"],
            "source_import_type": {
                "youtube_url": "youtube_url_import",
                "social_youtube_row": "social_youtube_import",
                "external_url": "external_url_import",
            }[request.source_mode],
            "owner_scope": owner_context.get("owner_scope"),
        }
    )

    source_provenance: dict[str, Any] = {"source_mode": request.source_mode}
    try:
        if request.source_mode == "youtube_url":
            source_url = str(request.source_url or "").strip()
            if not source_url:
                raise HTTPException(status_code=400, detail="source_url is required for youtube_url imports")
            video_id = _extract_youtube_video_id(source_url)
            metadata = _youtube_fetch_video_metadata(video_id)
            expected_owners = _resolve_official_youtube_owners(owner_context)
            matched_owner = _assert_youtube_official_match(
                actual_channel_id=str(metadata.get("channel_id") or "").strip() or None,
                actual_candidates=[
                    metadata.get("channel"),
                    metadata.get("uploader"),
                    metadata.get("uploader_id"),
                    metadata.get("channel_url"),
                    metadata.get("uploader_url"),
                    metadata.get("webpage_url"),
                    source_url,
                ],
                expected_owners=expected_owners,
            )
            resolution = resolve_youtube_media(video_id)
            video_url = _preferred_video_url([str(url).strip() for url in resolution.media_urls if str(url).strip()])
            mirror_meta = _mirror_remote_video_to_temp_object(
                source_url=video_url,
                temp_object_key=temp_object_key,
                content_type_hint="video/mp4",
            )
            source_provenance = {
                "source_mode": request.source_mode,
                "requested_url": source_url,
                "resolved_video_id": video_id,
                "resolved_media_url": video_url,
                "resolved_media_source": resolution.source,
                "matched_official_owner": matched_owner,
                "metadata": {
                    "title": metadata.get("title"),
                    "channel_id": metadata.get("channel_id"),
                    "uploader": metadata.get("uploader"),
                    "webpage_url": metadata.get("webpage_url"),
                },
            }
            cast_screentime.update_media_upload_session(
                str(upload_session_id),
                {
                    "status": "uploaded",
                    "content_type": mirror_meta.get("content_type"),
                    "verification_json": {
                        **(
                            session.get("verification_json")
                            if isinstance(session.get("verification_json"), dict)
                            else {}
                        ),
                        "mirror": mirror_meta,
                        "source_provenance": source_provenance,
                    },
                },
            )
        elif request.source_mode == "social_youtube_row":
            if not request.social_youtube_video_id:
                raise HTTPException(
                    status_code=400, detail="social_youtube_video_id is required for social_youtube_row imports"
                )
            row = cast_screentime.get_social_youtube_video(str(request.social_youtube_video_id))
            if not row:
                raise HTTPException(status_code=404, detail="Social YouTube video row not found")
            expected_owners = _resolve_official_youtube_owners(owner_context)
            raw_data = row.get("raw_data") if isinstance(row.get("raw_data"), dict) else {}
            matched_owner = _assert_youtube_official_match(
                actual_channel_id=str(row.get("channel_id") or "").strip() or None,
                actual_candidates=[
                    row.get("source_account"),
                    row.get("channel_title"),
                    raw_data.get("channel"),
                    raw_data.get("uploader"),
                    raw_data.get("uploader_id"),
                    raw_data.get("channel_url"),
                    raw_data.get("uploader_url"),
                    raw_data.get("webpage_url"),
                ],
                expected_owners=expected_owners,
            )
            hosted_media_urls = row.get("hosted_media_urls") if isinstance(row.get("hosted_media_urls"), list) else []
            preferred_hosted_url = _preferred_video_url(
                [str(url).strip() for url in hosted_media_urls if str(url).strip()]
            )
            hosted_object_key = _public_url_to_object_key(preferred_hosted_url)
            if hosted_object_key:
                mirror_meta = _copy_existing_object_to_temp_object(
                    source_object_key=hosted_object_key,
                    temp_object_key=temp_object_key,
                    content_type_hint="video/mp4",
                )
            else:
                mirror_meta = _mirror_remote_video_to_temp_object(
                    source_url=preferred_hosted_url,
                    temp_object_key=temp_object_key,
                    content_type_hint="video/mp4",
                )
            source_provenance = {
                "source_mode": request.source_mode,
                "social_youtube_video_id": str(request.social_youtube_video_id),
                "video_id": row.get("video_id"),
                "source_account": row.get("source_account"),
                "matched_official_owner": matched_owner,
                "preferred_hosted_url": preferred_hosted_url,
                "hosted_object_key": hosted_object_key,
            }
            cast_screentime.update_media_upload_session(
                str(upload_session_id),
                {
                    "status": "uploaded",
                    "content_type": mirror_meta.get("content_type"),
                    "verification_json": {
                        **(
                            session.get("verification_json")
                            if isinstance(session.get("verification_json"), dict)
                            else {}
                        ),
                        "mirror": mirror_meta,
                        "source_provenance": source_provenance,
                    },
                },
            )
        else:
            source_url = str(request.source_url or "").strip()
            if not source_url:
                raise HTTPException(status_code=400, detail="source_url is required for external_url imports")
            mirror_meta = _mirror_remote_video_to_temp_object(
                source_url=source_url,
                temp_object_key=temp_object_key,
                content_type_hint="video/mp4",
            )
            source_provenance = {
                "source_mode": request.source_mode,
                "requested_url": source_url,
            }
            cast_screentime.update_media_upload_session(
                str(upload_session_id),
                {
                    "status": "uploaded",
                    "content_type": mirror_meta.get("content_type"),
                    "verification_json": {
                        **(
                            session.get("verification_json")
                            if isinstance(session.get("verification_json"), dict)
                            else {}
                        ),
                        "mirror": mirror_meta,
                        "source_provenance": source_provenance,
                    },
                },
            )
    except Exception:
        cast_screentime.update_media_upload_session(
            str(upload_session_id),
            {
                "status": "failed",
                "failed_at": datetime.now(UTC).isoformat(),
                "error_text": "remote import failed",
            },
        )
        raise

    session = cast_screentime.get_media_upload_session(str(upload_session_id))
    if not session:
        raise HTTPException(status_code=500, detail="Import session disappeared before promotion")
    video_asset = _promote_session_to_video_asset(
        upload_session_id=upload_session_id,
        session=session,
        ingest_type=request.source_mode,
        source_provenance=source_provenance,
    )
    return {"upload_session_id": str(upload_session_id), "video_asset": video_asset}


@router.post("/admin/cast-screentime/video-assets/{video_asset_id}/runs")
def create_run(
    video_asset_id: UUID,
    request: CreateRunRequest,
    _: CastScreentimeAdminUser,
) -> dict[str, Any]:
    video_asset = cast_screentime.get_video_asset(str(video_asset_id))
    if not video_asset:
        raise HTTPException(status_code=404, detail="Video asset not found")

    upload_status = cast_screentime.get_video_asset_upload_session_status(str(video_asset_id))
    if upload_status not in {"verified", "promoted"}:
        raise HTTPException(status_code=409, detail="Video asset upload session is not verified")

    run_config = _merge_run_config(video_asset, request.run_config_json)
    source_json = video_asset.get("source_json")
    if not isinstance(source_json, dict) or not source_json.get("object_key"):
        raise HTTPException(status_code=409, detail="Video asset does not have a verified source object key")

    annotated_asset = _annotate_video_asset_row(video_asset)
    snapshot_bundle = cast_screentime.build_candidate_cast_snapshot(
        video_asset_id=str(video_asset_id),
        show_id=str(video_asset.get("show_id") or "") or None,
        season_id=str(video_asset.get("season_id") or "") or None,
        episode_id=str(video_asset.get("episode_id") or "") or None,
        media_type=str(annotated_asset.get("media_type") or "") or None,
        owner_scope=str(annotated_asset.get("owner_scope") or "") or None,
    )
    run_config["candidate_scope_policy"] = snapshot_bundle.get("candidate_scope_policy") or {}
    run_config["cast_coverage_summary"] = snapshot_bundle.get("cast_coverage_summary") or {}
    run = cast_screentime.create_run(
        {
            "video_asset_id": str(video_asset_id),
            "status": "pending",
            "run_type": "cast_screentime",
            "pipeline_version": str(run_config.get("pipeline_version") or "cast_screentime_v1"),
            "execution_backend": "screenalytics",
            "review_status": "draft",
            "run_config_json": run_config,
            "config_hash": _compute_config_hash(run_config),
            "candidate_cast_snapshot_json": snapshot_bundle.get("snapshot") or [],
            "candidate_scope_policy_json": snapshot_bundle.get("candidate_scope_policy") or {},
            "cast_coverage_summary_json": snapshot_bundle.get("cast_coverage_summary") or {},
        }
    )

    dispatch_state = "queued"
    dispatch_result: dict[str, Any] | None = None
    try:
        dispatch_result = screenalytics_cast_screentime.start_run(str(run["id"]))
        cast_screentime.update_run(
            str(run["id"]),
            {
                "status": "queued",
                "dispatch_status": str(dispatch_result.get("state") or "queued"),
                "dispatch_job_id": str(dispatch_result.get("job_id") or "").strip() or None,
                "dispatch_accepted_at": datetime.now(UTC).isoformat(),
            },
        )
    except screenalytics_cast_screentime.ScreenalyticsCastScreentimeClientError as exc:
        dispatch_state = "dispatch_failed"
        cast_screentime.update_run(
            str(run["id"]),
            {
                "status": "failed",
                "dispatch_status": "dispatch_failed",
                "error_message": str(exc),
                "completed_at": datetime.now(UTC).isoformat(),
            },
        )
    run_payload = cast_screentime.get_run_with_video_asset(str(run["id"])) or run
    return {"run": _annotate_run_row(run_payload), "dispatch_state": dispatch_state, "dispatch_result": dispatch_result}


@router.get("/admin/cast-screentime/runs/{run_id}")
def get_run(run_id: UUID, _: CastScreentimeAdminUser) -> dict[str, Any]:
    run = cast_screentime.get_run_with_video_asset(str(run_id))
    return _annotate_run_row(_assert_cast_screentime_run(run))


@router.get("/admin/cast-screentime/runs/{run_id}/leaderboard")
def get_leaderboard(run_id: UUID, _: CastScreentimeAdminUser) -> dict[str, Any]:
    run = _assert_cast_screentime_run(cast_screentime.get_run_with_video_asset(str(run_id)))
    return {
        "run_id": str(run_id),
        "video_asset_id": str(run["video_asset_id"]),
        "leaderboard": cast_screentime.list_leaderboard(str(run_id)),
    }


@router.get("/admin/cast-screentime/runs/{run_id}/segments")
def get_segments(run_id: UUID, _: CastScreentimeAdminUser) -> dict[str, Any]:
    _assert_cast_screentime_run(cast_screentime.get_run_with_video_asset(str(run_id)))
    return {"run_id": str(run_id), "segments": cast_screentime.list_segments(str(run_id))}


@router.get("/admin/cast-screentime/runs/{run_id}/evidence")
def get_evidence(run_id: UUID, _: CastScreentimeAdminUser) -> dict[str, Any]:
    _assert_cast_screentime_run(cast_screentime.get_run_with_video_asset(str(run_id)))
    return {
        "run_id": str(run_id),
        "evidence": [_attach_public_url(item) for item in cast_screentime.list_evidence(str(run_id))],
    }


@router.get("/admin/cast-screentime/runs/{run_id}/artifacts/{artifact_key}")
def get_run_artifact(
    run_id: UUID,
    artifact_key: str,
    _: CastScreentimeAdminUser,
) -> dict[str, Any]:
    _assert_cast_screentime_run(cast_screentime.get_run_with_video_asset(str(run_id)))
    artifact, payload = _read_run_artifact_payload(str(run_id), artifact_key)
    return {"run_id": str(run_id), "artifact": artifact, "payload": payload}


@router.post("/admin/cast-screentime/runs/{run_id}/segments/{segment_key}/clip")
def generate_segment_clip(
    run_id: UUID,
    segment_key: str,
    request: SegmentClipRequest,
    _: CastScreentimeAdminUser,
) -> dict[str, Any]:
    _assert_cast_screentime_run(cast_screentime.get_run_with_video_asset(str(run_id)))
    try:
        result = screenalytics_cast_screentime.generate_segment_clip(
            str(run_id),
            segment_key=segment_key,
            mode=request.mode,
            duration_seconds=request.duration_seconds,
            ttl_days=request.ttl_days,
        )
    except screenalytics_cast_screentime.ScreenalyticsCastScreentimeClientError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    evidence_payload = result.get("evidence")
    if not isinstance(evidence_payload, dict):
        raise HTTPException(status_code=502, detail="Screenalytics clip generation returned no evidence payload")
    rows = cast_screentime.upsert_cast_screentime_evidence(str(run_id), [evidence_payload])
    if not rows:
        raise HTTPException(status_code=500, detail="Failed to persist generated clip evidence")
    return {"run_id": str(run_id), "evidence": _attach_public_url(rows[0])}


@router.get("/admin/cast-screentime/runs/{run_id}/excluded-sections")
def get_excluded_sections(run_id: UUID, _: CastScreentimeAdminUser) -> dict[str, Any]:
    _assert_cast_screentime_run(cast_screentime.get_run_with_video_asset(str(run_id)))
    return {"run_id": str(run_id), "excluded_sections": cast_screentime.list_excluded_sections(str(run_id))}


@router.post("/admin/cast-screentime/runs/{run_id}/review-status")
def set_review_status(
    run_id: UUID,
    request: ReviewStatusRequest,
    admin_user: CastScreentimeAdminUser,
) -> dict[str, Any]:
    run = _assert_cast_screentime_run(cast_screentime.get_run(str(run_id)))
    if str(run.get("status") or "") != "success":
        raise HTTPException(status_code=409, detail="Only successful runs can enter review flow")
    allowed = {
        "draft": {"ready_for_review"},
        "ready_for_review": {"in_review"},
        "in_review": {"approved", "rejected"},
        "rejected": {"in_review"},
        "approved": set(),
    }
    current = str(run.get("review_status") or "draft")
    next_status = request.review_status
    if next_status not in allowed.get(current, set()):
        raise HTTPException(status_code=409, detail=f"Invalid review status transition: {current} -> {next_status}")
    return (
        cast_screentime.update_run(
            str(run_id),
            {
                "review_status": next_status,
                "reviewed_at": datetime.now(UTC).isoformat(),
                "reviewed_by": str(admin_user.get("id") or ""),
                "review_notes_json": request.notes,
            },
        )
        or {}
    )


@router.post("/admin/cast-screentime/runs/{run_id}/publish")
def publish_run(
    run_id: UUID,
    request: PublishRunRequest,
    admin_user: CastScreentimeAdminUser,
) -> dict[str, Any]:
    run = _assert_cast_screentime_run(cast_screentime.get_run_with_video_asset(str(run_id)))
    media_type = str(_annotate_run_row(run).get("media_type") or "episode")
    if media_type != "episode":
        raise HTTPException(status_code=409, detail="Only episode assets can be published into canonical rollups")
    if str(run.get("status") or "") != "success":
        raise HTTPException(status_code=409, detail="Only successful runs can be published")
    if str(run.get("review_status") or "draft") != "approved":
        raise HTTPException(status_code=409, detail="Run must be approved before publishing")

    existing = cast_screentime.get_publish_version_for_run(str(run_id))
    if existing:
        return {"publish_version": existing, "reference_fingerprint_count": 0, "already_published": True}

    metrics_snapshot = _build_publish_metrics_snapshot(run)
    publish_version = cast_screentime.publish_run(
        run_id=str(run_id),
        video_asset_id=str(run["video_asset_id"]),
        published_by=str(admin_user.get("id") or ""),
        notes_json=request.notes,
        metrics_snapshot_json=metrics_snapshot,
    )

    raw_reference_fingerprints = _artifact_payload_or_default(str(run_id), "reference_fingerprints.json", default=[])
    raw_title_card_references = _artifact_payload_or_default(
        str(run_id), "title_card_reference_signatures.json", default=[]
    )
    if not isinstance(raw_reference_fingerprints, list):
        raise HTTPException(status_code=500, detail="reference_fingerprints.json must be a JSON array")
    if not isinstance(raw_title_card_references, list):
        raise HTTPException(status_code=500, detail="title_card_reference_signatures.json must be a JSON array")
    reference_fingerprints = [item for item in raw_reference_fingerprints if isinstance(item, dict)]
    title_card_references = [item for item in raw_title_card_references if isinstance(item, dict)]
    inserted = cast_screentime.replace_reference_fingerprints_for_run(
        run_id=str(run_id),
        show_id=str(run.get("show_id") or ""),
        season_id=str(run.get("season_id") or "") or None,
        episode_id=str(run.get("episode_id") or "") or None,
        video_asset_id=str(run["video_asset_id"]),
        fingerprints=[*reference_fingerprints, *title_card_references],
    )
    reference_fingerprint_count = len(inserted)
    LOGGER.info(
        "cast_screentime_publish run_id=%s video_asset_id=%s version=%s reference_fingerprints=%s title_card_refs=%s",
        str(run_id),
        str(run["video_asset_id"]),
        publish_version.get("version_number"),
        len(reference_fingerprints),
        len(title_card_references),
    )

    return {
        "publish_version": publish_version,
        "reference_fingerprint_count": reference_fingerprint_count,
        "already_published": False,
    }


@router.get("/admin/cast-screentime/video-assets/{video_asset_id}/publish-history")
def get_publish_history(video_asset_id: UUID, _: CastScreentimeAdminUser) -> dict[str, Any]:
    video_asset = cast_screentime.get_video_asset(str(video_asset_id))
    if not video_asset:
        raise HTTPException(status_code=404, detail="Video asset not found")
    annotated = _annotate_video_asset_row(video_asset)
    return {
        "video_asset_id": str(video_asset_id),
        "media_type": annotated.get("media_type"),
        "media_kind": annotated.get("media_kind"),
        "video_class": annotated.get("video_class"),
        "promo_subtype": annotated.get("promo_subtype"),
        "publish_history": cast_screentime.list_publish_versions(str(video_asset_id)),
    }


@router.get("/admin/cast-screentime/shows/{show_id}/runs")
def list_show_runs(
    show_id: UUID,
    _: CastScreentimeAdminUser,
    limit: int = Query(default=20, ge=1, le=100),
    media_type: Literal["episode", "trailer", "extras"] | None = Query(default=None),
    video_class: Literal["episode", "promo"] | None = Query(default=None),
) -> dict[str, Any]:
    rows = cast_screentime.list_runs_for_show(
        str(show_id),
        limit=limit,
        video_class=video_class,
        media_type=media_type,
    )
    return {"show_id": str(show_id), "runs": [_annotate_run_row(row) for row in rows]}


@router.get("/admin/cast-screentime/shows/{show_id}/published-rollups")
def get_show_published_rollups(show_id: UUID, _: CastScreentimeAdminUser) -> dict[str, Any]:
    try:
        return _aggregate_rollup(
            cast_screentime.list_current_published_versions_for_show(str(show_id)),
            scope_id=str(show_id),
            scope_type="show_id",
        )
    except Exception:
        LOGGER.exception("cast_screentime_show_rollup_failed show_id=%s", show_id)
        raise


@router.get("/admin/cast-screentime/seasons/{season_id}/published-rollups")
def get_season_published_rollups(season_id: UUID, _: CastScreentimeAdminUser) -> dict[str, Any]:
    try:
        return _aggregate_rollup(
            cast_screentime.list_current_published_versions_for_season(str(season_id)),
            scope_id=str(season_id),
            scope_type="season_id",
        )
    except Exception:
        LOGGER.exception("cast_screentime_season_rollup_failed season_id=%s", season_id)
        raise


@router.get("/admin/cast-screentime/runs/{run_id}/decision-state")
def get_decision_state(run_id: UUID, _: CastScreentimeAdminUser) -> dict[str, Any]:
    run = _assert_cast_screentime_run(cast_screentime.get_run_with_video_asset(str(run_id)))
    show_id = str(run.get("show_id") or "").strip()
    if not show_id:
        raise HTTPException(status_code=409, detail="Run does not have a show scope")
    decision_effect_summary = (
        "Accepted suggestions and unknown-review decisions only affect future reruns; "
        "they do not retroactively rewrite this run's official named metrics."
    )
    return {
        "run_id": str(run_id),
        "rerun_required_for_metrics": True,
        "decision_effect_summary": decision_effect_summary,
        "suggestion_decisions": cast_screentime.list_suggestion_decisions_for_context(
            show_id=show_id,
            season_id=str(run.get("season_id") or "") or None,
            episode_id=str(run.get("episode_id") or "") or None,
        ),
        "unknown_review_state": cast_screentime.list_unknown_review_state_for_context(
            show_id=show_id,
            season_id=str(run.get("season_id") or "") or None,
            episode_id=str(run.get("episode_id") or "") or None,
        ),
    }


@router.post("/admin/cast-screentime/runs/{run_id}/suggestions/{suggestion_key}/decision")
def set_suggestion_decision(
    run_id: UUID,
    suggestion_key: str,
    request: DecisionActionRequest,
    admin_user: CastScreentimeAdminUser,
) -> dict[str, Any]:
    run = _assert_cast_screentime_run(cast_screentime.get_run_with_video_asset(str(run_id)))
    suggestions_payload = _artifact_payload_or_default(str(run_id), "cast_suggestions.json", default=[])
    if not isinstance(suggestions_payload, list):
        raise HTTPException(status_code=500, detail="cast_suggestions.json must be a JSON array")
    suggestion = next(
        (
            item
            for item in suggestions_payload
            if isinstance(item, dict) and str(item.get("suggestion_key") or "") == suggestion_key
        ),
        None,
    )
    if not suggestion:
        raise HTTPException(status_code=404, detail="Suggestion not found for run")
    person_id = str(suggestion.get("person_id") or "").strip()
    if not person_id:
        raise HTTPException(status_code=409, detail="Suggestion is missing a candidate person id")
    owner_scope, owner_entity_id = _owner_scope_entity_id_for_run(run, request.decision_scope)
    row = cast_screentime.upsert_suggestion_decision(
        {
            "show_id": str(run.get("show_id") or ""),
            "season_id": str(run.get("season_id") or "") or None,
            "episode_id": str(run.get("episode_id") or "") or None,
            "owner_scope": owner_scope,
            "owner_entity_id": owner_entity_id,
            "video_asset_id": str(run["video_asset_id"]),
            "run_id": str(run_id),
            "suggestion_key": suggestion_key,
            "person_id": person_id,
            "decision": request.decision,
            "notes_json": request.notes,
            "suggestion_payload": suggestion,
            "decided_by": str(admin_user.get("id") or ""),
        }
    )
    decision_effect_summary = (
        "Decision stored for future eligibility only; rerun required for official metrics "
        "to change."
    )
    return {
        "run_id": str(run_id),
        "decision": row,
        "rerun_required_for_metrics": True,
        "decision_effect_summary": decision_effect_summary,
    }


@router.post("/admin/cast-screentime/runs/{run_id}/unknown-review/{queue_key}/decision")
def set_unknown_review_decision(
    run_id: UUID,
    queue_key: str,
    request: DecisionActionRequest,
    admin_user: CastScreentimeAdminUser,
) -> dict[str, Any]:
    run = _assert_cast_screentime_run(cast_screentime.get_run_with_video_asset(str(run_id)))
    queues_payload = _artifact_payload_or_default(str(run_id), "unknown_review_queues.json", default=[])
    if not isinstance(queues_payload, list):
        raise HTTPException(status_code=500, detail="unknown_review_queues.json must be a JSON array")
    queue = next(
        (item for item in queues_payload if isinstance(item, dict) and str(item.get("queue_key") or "") == queue_key),
        None,
    )
    if not queue:
        raise HTTPException(status_code=404, detail="Unknown review queue not found for run")
    owner_scope, owner_entity_id = _owner_scope_entity_id_for_run(run, request.decision_scope)
    row = cast_screentime.upsert_unknown_review_state(
        {
            "show_id": str(run.get("show_id") or ""),
            "season_id": str(run.get("season_id") or "") or None,
            "episode_id": str(run.get("episode_id") or "") or None,
            "owner_scope": owner_scope,
            "owner_entity_id": owner_entity_id,
            "video_asset_id": str(run["video_asset_id"]),
            "run_id": str(run_id),
            "queue_key": queue_key,
            "queue_group": str(queue.get("queue_group") or queue_key),
            "candidate_person_id": str(queue.get("candidate_person_id") or "") or None,
            "decision": request.decision,
            "escalation_level": str(queue.get("escalation_level") or "episode"),
            "recommended_action": str(queue.get("recommended_action") or "episode_review"),
            "notes_json": request.notes,
            "queue_payload": queue,
            "decided_by": str(admin_user.get("id") or ""),
        }
    )
    decision_effect_summary = (
        "Decision stored for future eligibility only; rerun required for official metrics "
        "to change."
    )
    return {
        "run_id": str(run_id),
        "decision": row,
        "rerun_required_for_metrics": True,
        "decision_effect_summary": decision_effect_summary,
    }


@router.post("/admin/cast-screentime/runs/reconcile-stale")
def reconcile_stale_runs(
    _: CastScreentimeAdminUser,
    show_id: UUID | None = Query(default=None),
    stale_after_seconds: int = Query(default=_DEFAULT_STALE_AFTER_SECONDS, ge=60, le=86400),
) -> dict[str, Any]:
    effective_stale_after = stale_after_seconds or _stale_after_seconds()
    reconciled = reconcile_stale_runs_once(
        stale_after_seconds=effective_stale_after,
        show_id=str(show_id) if show_id else None,
    )
    return {
        "reconciled_run_count": len(reconciled),
        "stale_after_seconds": effective_stale_after,
        "runs": reconciled,
    }


@router.post("/internal/screenalytics/cast-screentime/runs/{run_id}/heartbeat")
def heartbeat(
    run_id: UUID,
    request: HeartbeatRequest,
    _: None = Depends(require_screenalytics_service_token),
) -> dict[str, Any]:
    run = _assert_cast_screentime_run(cast_screentime.get_run(str(run_id)))
    _assert_mutable_run(run)
    result = cast_screentime.set_run_heartbeat(str(run_id), status=request.status)
    if not result:
        raise HTTPException(status_code=404, detail="Run not found")
    return result


@router.patch("/internal/screenalytics/cast-screentime/runs/{run_id}/status")
def update_status(
    run_id: UUID,
    request: RunStatusUpdateRequest,
    _: None = Depends(require_screenalytics_service_token),
) -> dict[str, Any]:
    run = _assert_cast_screentime_run(cast_screentime.get_run(str(run_id)))
    _assert_mutable_run(run)
    payload: dict[str, Any] = {
        "status": request.status,
        "error_message": request.error_message,
        "manifest_key": request.manifest_key,
        "worker_heartbeat_at": datetime.now(UTC).isoformat(),
        "dispatch_status": request.status,
    }
    now = datetime.now(UTC).isoformat()
    if request.status == "running" and not run.get("started_at"):
        payload["started_at"] = now
    if request.status in _TERMINAL_STATUSES and not run.get("completed_at"):
        payload["completed_at"] = now
    result = cast_screentime.update_run(str(run_id), payload)
    if not result:
        raise HTTPException(status_code=404, detail="Run not found")
    return result


@router.post("/internal/screenalytics/cast-screentime/runs/{run_id}/artifacts:upsert")
def upsert_artifacts(
    run_id: UUID,
    request: ArtifactsUpsertRequest,
    _: None = Depends(require_screenalytics_service_token),
) -> list[dict[str, Any]]:
    _assert_cast_screentime_run(cast_screentime.get_run(str(run_id)))
    return cast_screentime.upsert_run_artifacts(str(run_id), [item.model_dump() for item in request.artifacts])


@router.post("/internal/screenalytics/cast-screentime/runs/{run_id}/segments:replace")
def replace_segments(
    run_id: UUID,
    request: SegmentsReplaceRequest,
    _: None = Depends(require_screenalytics_service_token),
) -> list[dict[str, Any]]:
    run = _assert_cast_screentime_run(cast_screentime.get_run(str(run_id)))
    _assert_mutable_run(run)
    return cast_screentime.replace_cast_screentime_segments(
        str(run_id), [item.model_dump() for item in request.segments]
    )


@router.post("/internal/screenalytics/cast-screentime/runs/{run_id}/evidence:replace")
def replace_evidence(
    run_id: UUID,
    request: EvidenceReplaceRequest,
    _: None = Depends(require_screenalytics_service_token),
) -> list[dict[str, Any]]:
    run = _assert_cast_screentime_run(cast_screentime.get_run(str(run_id)))
    _assert_mutable_run(run)
    return cast_screentime.replace_cast_screentime_evidence(
        str(run_id), [item.model_dump() for item in request.evidence]
    )


@router.post("/internal/screenalytics/cast-screentime/runs/{run_id}/excluded-sections:replace")
def replace_excluded_sections(
    run_id: UUID,
    request: ExcludedSectionsReplaceRequest,
    _: None = Depends(require_screenalytics_service_token),
) -> list[dict[str, Any]]:
    run = _assert_cast_screentime_run(cast_screentime.get_run(str(run_id)))
    _assert_mutable_run(run)
    return cast_screentime.replace_cast_screentime_excluded_sections(
        str(run_id),
        [item.model_dump() for item in request.excluded_sections],
    )


@router.post("/internal/screenalytics/cast-screentime/runs/{run_id}/person-metrics:replace")
def replace_person_metrics(
    run_id: UUID,
    request: MetricsReplaceRequest,
    _: None = Depends(require_screenalytics_service_token),
) -> list[dict[str, Any]]:
    run = _assert_cast_screentime_run(cast_screentime.get_run(str(run_id)))
    _assert_mutable_run(run)
    return cast_screentime.replace_run_person_metrics(str(run_id), [item.model_dump() for item in request.metrics])


@router.post("/internal/screenalytics/cast-screentime/runs/{run_id}/finalize")
def finalize_run(
    run_id: UUID,
    request: FinalizeRunRequest,
    _: None = Depends(require_screenalytics_service_token),
) -> dict[str, Any]:
    run = _assert_cast_screentime_run(cast_screentime.get_run(str(run_id)))
    _assert_mutable_run(run)
    if request.status not in _TERMINAL_STATUSES:
        raise HTTPException(status_code=400, detail="Finalize status must be a terminal run status")
    payload: dict[str, Any] = {
        "status": request.status,
        "manifest_key": request.manifest_key,
        "error_message": request.error_message,
        "effective_runtime_seconds": request.effective_runtime_seconds,
        "worker_heartbeat_at": datetime.now(UTC).isoformat(),
        "completed_at": datetime.now(UTC).isoformat(),
        "dispatch_status": request.status,
    }
    if request.status == "success":
        payload["review_status"] = request.review_status or "ready_for_review"
    result = cast_screentime.update_run(str(run_id), payload)
    if not result:
        raise HTTPException(status_code=404, detail="Run not found")
    return result
