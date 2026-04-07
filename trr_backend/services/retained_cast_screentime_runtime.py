"""Backend-owned screentime execution runtime for retained control-plane runs."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import subprocess
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Thread
from typing import Any

from trr_backend.media.s3_mirror import get_s3_bucket, get_s3_client
from trr_backend.repositories import cast_screentime
from trr_backend.services import cast_screentime_artifacts, face_reference_embeddings
from trr_backend.vision import people_count_engine

LOGGER = logging.getLogger(__name__)

_DEFAULT_SAMPLE_STRIDE_SECONDS = 1.0
_DEFAULT_CLIP_TTL_DAYS = 7
_DEFAULT_EVIDENCE_JPEG_QUALITY = 85


def enqueue_run(run_id: str) -> dict[str, Any]:
    """Queue a backend-owned screentime run for asynchronous execution."""

    job_id = f"backend:{run_id}"
    if _env_bool("CAST_SCREENTIME_BACKEND_SYNC", default=False):
        run_screentime_analysis(run_id)
        return {"run_id": run_id, "state": "success", "job_id": job_id, "mode": "backend"}

    thread = Thread(
        target=_run_in_background,
        args=(run_id,),
        name=f"cast-screentime-{run_id[:8]}",
        daemon=True,
    )
    thread.start()
    return {"run_id": run_id, "state": "queued", "job_id": job_id, "mode": "backend"}


def load_run_contract(run_id: str) -> dict[str, Any]:
    run = cast_screentime.get_run_with_video_asset(run_id)
    if not run:
        raise ValueError(f"Run not found: {run_id}")
    if str(run.get("run_type") or "") != "cast_screentime":
        raise ValueError(f"Run {run_id} is not a cast screentime run")
    source_json = run.get("source_json")
    if not isinstance(source_json, dict) or not str(source_json.get("object_key") or "").strip():
        raise ValueError(f"Run {run_id} is missing a canonical source object key")
    run_config_json = run.get("run_config_json")
    if not isinstance(run_config_json, dict):
        run["run_config_json"] = {}
    candidate_cast_snapshot_json = run.get("candidate_cast_snapshot_json")
    if not isinstance(candidate_cast_snapshot_json, list):
        run["candidate_cast_snapshot_json"] = []
    return run


def run_screentime_analysis(run_id: str) -> dict[str, Any]:
    run_contract = load_run_contract(run_id)
    runtime_config = _ensure_runtime_run_config(run_contract)
    now = datetime.now(UTC).isoformat()
    cast_screentime.update_run(
        run_id,
        {
            "status": "running",
            "dispatch_status": "running",
            "started_at": str(run_contract.get("started_at") or now),
            "worker_heartbeat_at": now,
            "run_config_json": runtime_config,
            "config_hash": _compute_config_hash(runtime_config),
        },
    )
    try:
        analysis = analyze_run_contract({**run_contract, "run_config_json": runtime_config})
        persisted = _persist_analysis(run_id, analysis)
        completed = cast_screentime.update_run(
            run_id,
            {
                "status": "success",
                "dispatch_status": "success",
                "review_status": "ready_for_review",
                "completed_at": datetime.now(UTC).isoformat(),
                "worker_heartbeat_at": datetime.now(UTC).isoformat(),
                "effective_runtime_seconds": analysis.get("effective_runtime_seconds"),
                "run_config_json": runtime_config,
                "config_hash": _compute_config_hash(runtime_config),
                "error_message": None,
                "manifest_key": persisted.get("manifest_key"),
            },
        )
        return {
            "run_id": run_id,
            "status": "success",
            "artifact_count": persisted.get("artifact_count", 0),
            "segment_count": persisted.get("segment_count", 0),
            "evidence_count": persisted.get("evidence_count", 0),
            "run": completed or {},
        }
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("retained_cast_screentime_runtime_failed run_id=%s", run_id)
        cast_screentime.update_run(
            run_id,
            {
                "status": "failed",
                "dispatch_status": "failed",
                "completed_at": datetime.now(UTC).isoformat(),
                "worker_heartbeat_at": datetime.now(UTC).isoformat(),
                "error_message": str(exc),
                "run_config_json": runtime_config,
                "config_hash": _compute_config_hash(runtime_config),
            },
        )
        raise


def generate_segment_clip(
    run_id: str,
    *,
    segment_key: str,
    mode: str,
    duration_seconds: int | None = None,
    ttl_days: int = _DEFAULT_CLIP_TTL_DAYS,
) -> dict[str, Any]:
    run_contract = load_run_contract(run_id)
    segment = cast_screentime.get_segment(run_id, segment_key)
    if not segment:
        raise ValueError(f"Segment not found: {segment_key}")

    start_ms = int(segment.get("start_ms") or 0)
    end_ms = int(segment.get("end_ms") or start_ms)
    clip_mode = str(mode or "exact").strip().lower()
    if clip_mode not in {"exact", "timestamp"}:
        raise ValueError(f"Unsupported clip mode: {mode}")
    if clip_mode == "exact":
        clip_start_ms = start_ms
        clip_end_ms = max(end_ms, clip_start_ms + 250)
        evidence_key = f"clip-exact-{segment_key}"
        evidence_type = "exact_segment_clip"
        timestamp_ms = start_ms
    else:
        clip_duration_ms = int(duration_seconds or 5) * 1000
        midpoint_ms = int(round((start_ms + end_ms) / 2))
        clip_start_ms = max(midpoint_ms - int(clip_duration_ms / 2), 0)
        clip_end_ms = clip_start_ms + clip_duration_ms
        evidence_key = f"clip-timestamp-{int(duration_seconds or 5)}s-{segment_key}"
        evidence_type = "timestamp_clip"
        timestamp_ms = midpoint_ms

    total_duration_ms = int(round(float(run_contract.get("duration_seconds") or 0.0) * 1000))
    if total_duration_ms > 0 and clip_end_ms > total_duration_ms:
        clip_end_ms = total_duration_ms
        if clip_mode == "timestamp":
            clip_start_ms = max(clip_end_ms - int((duration_seconds or 5) * 1000), 0)
    if clip_end_ms <= clip_start_ms:
        raise RuntimeError("clip window collapsed after duration clamp")

    clip_payload = _render_segment_clip_bytes(
        run_contract,
        start_seconds=clip_start_ms / 1000.0,
        end_seconds=clip_end_ms / 1000.0,
    )
    object_key = f"review/evidence/runs/{run_id}/clips/{evidence_key}.mp4"
    _upload_object_bytes(object_key, clip_payload, content_type="video/mp4")
    evidence_item = {
        "segment_key": segment_key,
        "evidence_key": evidence_key,
        "evidence_type": evidence_type,
        "timestamp_ms": timestamp_ms,
        "object_key": object_key,
        "content_type": "video/mp4",
        "ttl_expires_at": (datetime.now(UTC) + timedelta(days=max(ttl_days, 1))).isoformat(),
        "metadata": {
            "mode": clip_mode,
            "clip_start_ms": clip_start_ms,
            "clip_end_ms": clip_end_ms,
            "duration_seconds": round((clip_end_ms - clip_start_ms) / 1000.0, 3),
            "person_id": segment.get("person_id"),
            "display_name": ((segment.get("metadata") or {}).get("display_name")),
        },
    }
    persisted = cast_screentime.upsert_cast_screentime_evidence(run_id, [evidence_item])
    evidence_row = persisted[0] if persisted else evidence_item
    return {"run_id": run_id, "segment_key": segment_key, "mode": clip_mode, "evidence": evidence_row}


def analyze_run_contract(run_contract: dict[str, Any]) -> dict[str, Any]:
    """Run a lean backend analysis pass over the canonical source video."""

    cv2 = _lazy_cv2()
    run_id = str(run_contract["id"])
    stride_seconds = _sampling_stride_seconds(run_contract)

    with tempfile.TemporaryDirectory(prefix=f"cast-screentime-{run_id}-") as work_dir_raw:
        work_dir = Path(work_dir_raw)
        video_path = _localize_source_video(run_contract, work_dir)
        capture = cv2.VideoCapture(str(video_path))
        if not capture.isOpened():
            raise RuntimeError(f"failed to open localized video {video_path}")

        fps = float(capture.get(cv2.CAP_PROP_FPS) or 0.0)
        usable_fps = fps if fps > 0.001 else 24.0
        frame_duration_ms = max(int(round(1000.0 / usable_fps)), 1)
        sample_stride_frames = max(int(round(usable_fps * stride_seconds)), 1)
        candidate_person_ids = {
            str(item.get("person_id") or "").strip()
            for item in list(run_contract.get("candidate_cast_snapshot_json") or [])
            if isinstance(item, dict) and str(item.get("person_id") or "").strip()
        }

        segments: list[dict[str, Any]] = []
        evidence: list[dict[str, Any]] = []
        evidence_bytes: dict[str, bytes] = {}
        sample_shots: list[dict[str, Any]] = []
        sample_index = 0

        try:
            frame_idx = 0
            while True:
                ok, frame = capture.read()
                if not ok or frame is None:
                    break
                if frame_idx % sample_stride_frames != 0:
                    frame_idx += 1
                    continue

                timestamp_ms = int(round((frame_idx / usable_fps) * 1000))
                raw_face_count, _model_id, raw_faces = people_count_engine._detect_faces_retinaface(frame)
                filtered_faces, face_filter_decisions = people_count_engine._adaptive_filter_faces(
                    raw_faces, image=frame
                )
                identity_matches = people_count_engine._match_faces_to_people(
                    filtered_faces,
                    frame,
                    candidate_person_ids=candidate_person_ids or None,
                )
                detections = people_count_engine._normalize_face_detections(
                    filtered_faces,
                    frame,
                    identity_matches=identity_matches,
                    filter_diagnostics=face_filter_decisions,
                )
                sample_shots.append(
                    {
                        "shot_key": f"shot-{sample_index:04d}-{timestamp_ms:08d}",
                        "start_ms": timestamp_ms,
                        "end_ms": timestamp_ms + frame_duration_ms,
                        "duration_ms": frame_duration_ms,
                        "frame_count": 1,
                        "observation_count": len(detections),
                        "raw_face_count": raw_face_count,
                    }
                )
                for detection_index, detection in enumerate(detections):
                    segment_key = f"{detection.get('person_id') or 'unknown'}-{timestamp_ms:08d}-{detection_index:02d}"
                    is_counted = (
                        bool(detection.get("person_id")) and str(detection.get("match_status") or "") == "matched"
                    )
                    segment = {
                        "segment_key": segment_key,
                        "person_id": detection.get("person_id"),
                        "start_ms": timestamp_ms,
                        "end_ms": timestamp_ms + frame_duration_ms,
                        "duration_ms": frame_duration_ms,
                        "frame_count": 1,
                        "confidence_score": detection.get("confidence"),
                        "similarity_score": detection.get("match_similarity"),
                        "pose_bucket": None,
                        "assignment_source": "retained_backend_runtime",
                        "is_counted": is_counted,
                        "classification_json": {
                            "bbox": detection.get("bbox"),
                            "match_status": detection.get("match_status"),
                            "match_reason": detection.get("match_reason"),
                            "filter_decision": detection.get("filter_decision"),
                        },
                        "metadata": {
                            "display_name": detection.get("person_name"),
                            "sample_index": sample_index,
                            "frame_idx": frame_idx,
                        },
                    }
                    segments.append(segment)
                    crop_bytes = _encode_evidence_crop(
                        frame, detection.get("square_crop_bbox") or detection.get("bbox")
                    )
                    object_key = f"review/evidence/runs/{run_id}/{segment_key}/still_{timestamp_ms:08d}.jpg"
                    evidence.append(
                        {
                            "segment_key": segment_key,
                            "evidence_key": f"still-{timestamp_ms:08d}-{detection_index:02d}",
                            "evidence_type": "proof_frame",
                            "timestamp_ms": timestamp_ms,
                            "object_key": object_key,
                            "content_type": "image/jpeg",
                            "ttl_expires_at": None,
                            "metadata": {
                                "bbox": detection.get("bbox"),
                                "person_id": detection.get("person_id"),
                                "person_name": detection.get("person_name"),
                            },
                        }
                    )
                    evidence_bytes[object_key] = crop_bytes

                frame_idx += 1
                sample_index += 1
        finally:
            capture.release()

    metrics = _aggregate_metrics(segments)
    video_duration_ms = int(round(float(run_contract.get("duration_seconds") or 0.0) * 1000))
    scenes = _build_scenes_from_segments(segments, total_duration_ms=video_duration_ms)
    artifact_lists = {
        "shots": sample_shots,
        "segments": segments,
        "scenes": scenes,
        "excluded_sections": [],
        "person_metrics": metrics,
        "title_card_candidates": [],
        "title_card_reference_signatures": [],
        "confessional_candidates": [],
        "cast_suggestions": _build_cast_suggestions(run_contract, segments),
        "unknown_review_queues": _build_unknown_review_queues(segments),
        "reference_fingerprints": _build_reference_fingerprints(scenes),
    }
    return {
        "segments": artifact_lists["segments"],
        "evidence": evidence,
        "evidence_bytes": evidence_bytes,
        "excluded_sections": artifact_lists["excluded_sections"],
        "metrics": artifact_lists["person_metrics"],
        "shots": artifact_lists["shots"],
        "scenes": artifact_lists["scenes"],
        "title_card_candidates": artifact_lists["title_card_candidates"],
        "title_card_reference_signatures": artifact_lists["title_card_reference_signatures"],
        "confessional_candidates": artifact_lists["confessional_candidates"],
        "cast_suggestions": artifact_lists["cast_suggestions"],
        "unknown_review_queues": artifact_lists["unknown_review_queues"],
        "reference_fingerprints": artifact_lists["reference_fingerprints"],
        "effective_runtime_seconds": float(run_contract.get("duration_seconds") or 0.0),
    }


def _persist_analysis(run_id: str, analysis: dict[str, Any]) -> dict[str, Any]:
    manifest_payload = {
        "run_id": run_id,
        "generated_at": datetime.now(UTC).isoformat(),
        "artifact_schema_version": cast_screentime_artifacts.SEGMENTS.schema_version,
        "segment_count": len(list(analysis.get("segments") or [])),
        "evidence_count": len(list(analysis.get("evidence") or [])),
    }
    manifest_artifact = _upload_json_artifact(run_id, "manifest.json", manifest_payload, artifact_kind="manifest")
    artifacts = [manifest_artifact]
    artifacts.extend(
        [
            _upload_json_artifact(
                run_id,
                cast_screentime_artifacts.SHOTS.key,
                analysis.get("shots") or [],
                artifact_kind=cast_screentime_artifacts.SHOTS.artifact_kind,
            ),
            _upload_json_artifact(
                run_id,
                cast_screentime_artifacts.SEGMENTS.key,
                analysis.get("segments") or [],
                artifact_kind=cast_screentime_artifacts.SEGMENTS.artifact_kind,
            ),
            _upload_json_artifact(
                run_id,
                cast_screentime_artifacts.SCENES.key,
                analysis.get("scenes") or [],
                artifact_kind=cast_screentime_artifacts.SCENES.artifact_kind,
            ),
            _upload_json_artifact(
                run_id,
                cast_screentime_artifacts.EXCLUDED_SECTIONS.key,
                analysis.get("excluded_sections") or [],
                artifact_kind=cast_screentime_artifacts.EXCLUDED_SECTIONS.artifact_kind,
            ),
            _upload_json_artifact(
                run_id,
                cast_screentime_artifacts.PERSON_METRICS.key,
                analysis.get("metrics") or [],
                artifact_kind=cast_screentime_artifacts.PERSON_METRICS.artifact_kind,
            ),
            _upload_json_artifact(
                run_id,
                cast_screentime_artifacts.TITLE_CARD_CANDIDATES.key,
                analysis.get("title_card_candidates") or [],
                artifact_kind=cast_screentime_artifacts.TITLE_CARD_CANDIDATES.artifact_kind,
            ),
            _upload_json_artifact(
                run_id,
                cast_screentime_artifacts.TITLE_CARD_REFERENCE_SIGNATURES.key,
                analysis.get("title_card_reference_signatures") or [],
                artifact_kind=cast_screentime_artifacts.TITLE_CARD_REFERENCE_SIGNATURES.artifact_kind,
            ),
            _upload_json_artifact(
                run_id,
                cast_screentime_artifacts.CONFESSIONAL_CANDIDATES.key,
                analysis.get("confessional_candidates") or [],
                artifact_kind=cast_screentime_artifacts.CONFESSIONAL_CANDIDATES.artifact_kind,
            ),
            _upload_json_artifact(
                run_id,
                cast_screentime_artifacts.CAST_SUGGESTIONS.key,
                analysis.get("cast_suggestions") or [],
                artifact_kind=cast_screentime_artifacts.CAST_SUGGESTIONS.artifact_kind,
            ),
            _upload_json_artifact(
                run_id,
                cast_screentime_artifacts.UNKNOWN_REVIEW_QUEUES.key,
                analysis.get("unknown_review_queues") or [],
                artifact_kind=cast_screentime_artifacts.UNKNOWN_REVIEW_QUEUES.artifact_kind,
            ),
            _upload_json_artifact(
                run_id,
                cast_screentime_artifacts.REFERENCE_FINGERPRINTS.key,
                analysis.get("reference_fingerprints") or [],
                artifact_kind=cast_screentime_artifacts.REFERENCE_FINGERPRINTS.artifact_kind,
            ),
        ]
    )
    for item in list(analysis.get("evidence") or []):
        object_key = str(item.get("object_key") or "").strip()
        payload = (analysis.get("evidence_bytes") or {}).get(object_key)
        if not object_key or payload is None:
            continue
        _upload_object_bytes(
            object_key, payload, content_type=str(item.get("content_type") or "application/octet-stream")
        )
    cast_screentime.upsert_run_artifacts(run_id, artifacts)
    cast_screentime.replace_cast_screentime_segments(run_id, list(analysis.get("segments") or []))
    cast_screentime.replace_cast_screentime_evidence(run_id, list(analysis.get("evidence") or []))
    cast_screentime.replace_cast_screentime_excluded_sections(run_id, list(analysis.get("excluded_sections") or []))
    cast_screentime.replace_run_person_metrics(run_id, list(analysis.get("metrics") or []))
    return {
        "manifest_key": manifest_artifact["s3_key"],
        "artifact_count": len(artifacts),
        "segment_count": len(list(analysis.get("segments") or [])),
        "evidence_count": len(list(analysis.get("evidence") or [])),
    }


def _run_in_background(run_id: str) -> None:
    try:
        run_screentime_analysis(run_id)
    except Exception:  # noqa: BLE001
        LOGGER.exception("retained_cast_screentime_background_failed run_id=%s", run_id)


def _ensure_runtime_run_config(run_contract: dict[str, Any]) -> dict[str, Any]:
    existing = dict(run_contract.get("run_config_json") or {})
    existing.setdefault("execution_backend", "trr_backend_local_runtime")
    existing.setdefault("artifact_schema_version", cast_screentime_artifacts.SEGMENTS.schema_version)
    existing.setdefault("embedding_contract_key", face_reference_embeddings.FACE_REFERENCE_EMBEDDING_CONTRACT_KEY)
    existing.setdefault("sampling_stride_seconds", _sampling_stride_seconds(run_contract))
    existing.setdefault(
        "candidate_cast_snapshot_count",
        len([item for item in list(run_contract.get("candidate_cast_snapshot_json") or []) if isinstance(item, dict)]),
    )
    return existing


def _sampling_stride_seconds(run_contract: dict[str, Any]) -> float:
    run_config = run_contract.get("run_config_json")
    if isinstance(run_config, dict):
        configured = run_config.get("sampling_stride_seconds")
        if isinstance(configured, (int, float)) and float(configured) > 0:
            return float(configured)
        processing_mode = str(run_config.get("processing_mode") or "").strip().lower()
        if processing_mode == "balanced":
            return 1.0
        if processing_mode == "fast":
            return 2.0
    return _DEFAULT_SAMPLE_STRIDE_SECONDS


def _localize_source_video(run_contract: dict[str, Any], work_dir: Path) -> Path:
    source_json = run_contract.get("source_json")
    object_key = str((source_json or {}).get("object_key") or "").strip()
    if not object_key:
        raise RuntimeError("run contract is missing source_json.object_key")
    bucket = get_s3_bucket()
    client = get_s3_client()
    response = client.get_object(Bucket=bucket, Key=object_key)
    body = response["Body"].read()
    suffix = Path(object_key).suffix or ".mp4"
    local_path = work_dir / f"source{suffix}"
    local_path.write_bytes(body)
    return local_path


def _upload_json_artifact(run_id: str, filename: str, payload: Any, *, artifact_kind: str) -> dict[str, Any]:
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    checksum = hashlib.sha256(serialized).hexdigest()
    object_key = f"derived/runs/{run_id}/{filename}"
    _upload_object_bytes(object_key, serialized, content_type="application/json")
    row_count = len(payload) if isinstance(payload, list) else 1
    return {
        "artifact_key": filename,
        "artifact_kind": artifact_kind,
        "s3_key": object_key,
        "schema_version": cast_screentime_artifacts.ARTIFACT_REGISTRY.get(
            filename, cast_screentime_artifacts.SEGMENTS
        ).schema_version,
        "content_type": "application/json",
        "checksum_sha256": checksum,
        "row_count": row_count,
    }


def _upload_object_bytes(object_key: str, payload: bytes, *, content_type: str) -> None:
    client = get_s3_client()
    client.put_object(Bucket=get_s3_bucket(), Key=object_key, Body=payload, ContentType=content_type)


def _encode_evidence_crop(frame: Any, bbox: Any) -> bytes:
    cv2 = _lazy_cv2()
    if not bbox or not isinstance(bbox, (list, tuple)) or len(bbox) < 4:
        success, encoded = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, _DEFAULT_EVIDENCE_JPEG_QUALITY])
        if not success:
            raise RuntimeError("failed to encode proof frame")
        return encoded.tobytes()

    height, width = frame.shape[:2]
    x1 = max(0, min(width - 1, int(round(float(bbox[0]) * width))))
    y1 = max(0, min(height - 1, int(round(float(bbox[1]) * height))))
    x2 = max(x1 + 1, min(width, int(round(float(bbox[2]) * width))))
    y2 = max(y1 + 1, min(height, int(round(float(bbox[3]) * height))))
    crop = frame[y1:y2, x1:x2]
    if crop.size == 0:
        crop = frame
    success, encoded = cv2.imencode(".jpg", crop, [cv2.IMWRITE_JPEG_QUALITY, _DEFAULT_EVIDENCE_JPEG_QUALITY])
    if not success:
        raise RuntimeError("failed to encode proof frame crop")
    return encoded.tobytes()


def _render_segment_clip_bytes(run_contract: dict[str, Any], *, start_seconds: float, end_seconds: float) -> bytes:
    with tempfile.TemporaryDirectory(prefix=f"cast-screentime-clip-{run_contract['id']}-") as work_dir_raw:
        work_dir = Path(work_dir_raw)
        input_path = _localize_source_video(run_contract, work_dir)
        output_path = work_dir / "clip.mp4"
        try:
            result = subprocess.run(
                [
                    "ffmpeg",
                    "-y",
                    "-ss",
                    f"{max(start_seconds, 0.0):.3f}",
                    "-to",
                    f"{max(end_seconds, 0.0):.3f}",
                    "-i",
                    str(input_path),
                    "-c:v",
                    "libx264",
                    "-preset",
                    "veryfast",
                    "-crf",
                    "23",
                    "-an",
                    str(output_path),
                ],
                check=False,
                capture_output=True,
                text=True,
                timeout=120,
            )
        except FileNotFoundError as exc:
            raise RuntimeError("ffmpeg is not installed") from exc
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError("ffmpeg timed out during clip generation") from exc
        if result.returncode != 0:
            raise RuntimeError((result.stderr or result.stdout or "ffmpeg clip generation failed").strip()[:500])
        return output_path.read_bytes()


def _aggregate_metrics(segments: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for segment in segments:
        person_id = str(segment.get("person_id") or "").strip()
        if not person_id or not segment.get("is_counted", True):
            continue
        state = grouped.setdefault(
            person_id,
            {
                "person_id": person_id,
                "screen_time_seconds": 0.0,
                "frame_count": 0,
                "confidence_weighted_total": 0.0,
                "confidence_weight": 0,
                "display_name": None,
                "segment_count": 0,
            },
        )
        state["screen_time_seconds"] += float(segment.get("duration_ms") or 0) / 1000.0
        state["frame_count"] += int(segment.get("frame_count") or 0)
        confidence = segment.get("confidence_score")
        if confidence is not None:
            weight = max(int(segment.get("frame_count") or 0), 1)
            state["confidence_weighted_total"] += float(confidence) * weight
            state["confidence_weight"] += weight
        state["segment_count"] += 1
        display_name = (segment.get("metadata") or {}).get("display_name")
        if display_name and not state["display_name"]:
            state["display_name"] = display_name

    metrics: list[dict[str, Any]] = []
    for state in grouped.values():
        weight = int(state["confidence_weight"] or 0)
        metrics.append(
            {
                "person_id": state["person_id"],
                "screen_time_seconds": round(float(state["screen_time_seconds"]), 3),
                "frame_count": int(state["frame_count"]),
                "confidence_avg": round(float(state["confidence_weighted_total"]) / weight, 4) if weight else None,
                "metadata": {
                    "display_name": state.get("display_name"),
                    "segment_count": int(state["segment_count"]),
                },
            }
        )
    metrics.sort(key=lambda item: float(item["screen_time_seconds"]), reverse=True)
    return metrics


def _build_scenes_from_segments(segments: list[dict[str, Any]], *, total_duration_ms: int) -> list[dict[str, Any]]:
    if not segments:
        if total_duration_ms <= 0:
            return []
        return [
            {"scene_key": "scene-0000", "start_ms": 0, "end_ms": total_duration_ms, "duration_ms": total_duration_ms}
        ]

    ordered = sorted(segments, key=lambda item: int(item.get("start_ms") or 0))
    scenes: list[dict[str, Any]] = []
    current_start = int(ordered[0].get("start_ms") or 0)
    current_end = int(ordered[0].get("end_ms") or current_start)
    scene_index = 0
    for segment in ordered[1:]:
        start_ms = int(segment.get("start_ms") or 0)
        end_ms = int(segment.get("end_ms") or start_ms)
        if start_ms - current_end > 5000:
            scenes.append(
                {
                    "scene_key": f"scene-{scene_index:04d}",
                    "start_ms": current_start,
                    "end_ms": current_end,
                    "duration_ms": max(current_end - current_start, 0),
                }
            )
            scene_index += 1
            current_start = start_ms
            current_end = end_ms
        else:
            current_end = max(current_end, end_ms)
    scenes.append(
        {
            "scene_key": f"scene-{scene_index:04d}",
            "start_ms": current_start,
            "end_ms": current_end,
            "duration_ms": max(current_end - current_start, 0),
        }
    )
    return scenes


def _build_cast_suggestions(run_contract: dict[str, Any], segments: list[dict[str, Any]]) -> list[dict[str, Any]]:
    candidate_names = {
        str(item.get("person_id") or "").strip(): str(item.get("display_name") or "").strip()
        for item in list(run_contract.get("candidate_cast_snapshot_json") or [])
        if isinstance(item, dict) and str(item.get("person_id") or "").strip()
    }
    suggestions: list[dict[str, Any]] = []
    for person_id, display_name in candidate_names.items():
        if any(str(segment.get("person_id") or "").strip() == person_id for segment in segments):
            continue
        suggestions.append(
            {
                "suggestion_key": f"suggestion-{person_id}",
                "person_id": person_id,
                "display_name": display_name or None,
                "reason": "candidate_not_seen_in_backend_runtime",
            }
        )
    return suggestions


def _build_unknown_review_queues(segments: list[dict[str, Any]]) -> list[dict[str, Any]]:
    queues: list[dict[str, Any]] = []
    unknown_segments = [segment for segment in segments if not segment.get("person_id")]
    for index, segment in enumerate(unknown_segments):
        queues.append(
            {
                "queue_key": f"unknown-{index:04d}",
                "queue_group": "unassigned_segments",
                "recommended_action": "episode_review",
                "escalation_level": "episode",
                "segment_key": segment.get("segment_key"),
                "start_ms": segment.get("start_ms"),
                "end_ms": segment.get("end_ms"),
            }
        )
    return queues


def _build_reference_fingerprints(scenes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    fingerprints: list[dict[str, Any]] = []
    for scene in scenes:
        material = json.dumps(scene, sort_keys=True, separators=(",", ":")).encode("utf-8")
        fingerprints.append(
            {
                "scene_key": scene.get("scene_key"),
                "fingerprint_type": "scene_window",
                "fingerprint_hash": hashlib.sha256(material).hexdigest(),
                "start_ms": scene.get("start_ms"),
                "end_ms": scene.get("end_ms"),
                "duration_ms": scene.get("duration_ms"),
                "metadata": {"generated_by": "retained_backend_runtime"},
            }
        )
    return fingerprints


def _compute_config_hash(config: dict[str, Any]) -> str:
    normalized = json.dumps(config, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _env_bool(name: str, *, default: bool) -> bool:
    raw = str(os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return default


def _lazy_cv2():
    try:
        import cv2  # type: ignore
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("OpenCV is required for backend screentime execution") from exc
    return cv2
