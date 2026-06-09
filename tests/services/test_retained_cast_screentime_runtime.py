from __future__ import annotations

from pathlib import Path

from trr_backend.services import retained_cast_screentime_runtime as runtime


def test_run_screentime_analysis_persists_retained_outputs_and_finalizes(monkeypatch) -> None:
    run_contract = {
        "id": "run-123",
        "run_type": "cast_screentime",
        "video_asset_id": "asset-1",
        "status": "queued",
        "review_status": "draft",
        "run_config_json": {"processing_mode": "balanced"},
        "candidate_cast_snapshot_json": [{"person_id": "person-1", "display_name": "Person One"}],
        "source_json": {"object_key": "source/videos/asset-1/original.mp4"},
        "duration_seconds": 42.5,
    }

    analysis = {
        "segments": [
            {
                "segment_key": "segment-1",
                "person_id": "person-1",
                "start_ms": 0,
                "end_ms": 2500,
                "duration_ms": 2500,
                "frame_count": 3,
                "confidence_score": 0.91,
                "similarity_score": 0.88,
                "pose_bucket": "frontal",
                "assignment_source": "retained_backend_runtime",
                "is_counted": True,
                "classification_json": {},
                "metadata": {"display_name": "Person One"},
            }
        ],
        "evidence": [
            {
                "segment_key": "segment-1",
                "evidence_key": "still-00000000",
                "evidence_type": "proof_frame",
                "timestamp_ms": 0,
                "object_key": "review/evidence/runs/run-123/segment-1/still_00000000.jpg",
                "content_type": "image/jpeg",
                "ttl_expires_at": None,
                "metadata": {},
            }
        ],
        "evidence_bytes": {
            "review/evidence/runs/run-123/segment-1/still_00000000.jpg": b"jpeg",
        },
        "excluded_sections": [],
        "metrics": [
            {
                "person_id": "person-1",
                "screen_time_seconds": 2.5,
                "frame_count": 3,
                "confidence_avg": 0.91,
                "metadata": {"display_name": "Person One"},
            }
        ],
        "shots": [{"shot_key": "shot-0001", "start_ms": 0, "end_ms": 2500, "duration_ms": 2500}],
        "scenes": [{"scene_key": "scene-0001", "start_ms": 0, "end_ms": 2500, "duration_ms": 2500}],
        "title_card_candidates": [],
        "title_card_reference_signatures": [],
        "confessional_candidates": [],
        "cast_suggestions": [],
        "unknown_review_queues": [],
        "reference_fingerprints": [],
        "effective_runtime_seconds": 40.0,
    }

    update_calls: list[dict[str, object]] = []
    artifact_payloads: list[list[dict[str, object]]] = []
    uploaded_evidence: list[tuple[str, bytes, str]] = []

    monkeypatch.setattr(runtime, "load_run_contract", lambda run_id: dict(run_contract, id=run_id))
    monkeypatch.setattr(runtime, "analyze_run_contract", lambda contract: analysis)
    monkeypatch.setattr(
        runtime.cast_screentime,
        "update_run",
        lambda run_id, payload: (
            update_calls.append({"run_id": run_id, "payload": payload}) or {"id": run_id, **payload}
        ),
    )
    monkeypatch.setattr(
        runtime,
        "_upload_json_artifact",
        lambda run_id, filename, payload, *, artifact_kind: {
            "artifact_key": filename,
            "artifact_kind": artifact_kind,
            "s3_key": f"derived/runs/{run_id}/{filename}",
            "schema_version": "cast_screentime.v1",
            "content_type": "application/json",
            "checksum_sha256": f"sha-{filename}",
            "row_count": len(payload) if isinstance(payload, list) else 1,
        },
    )
    monkeypatch.setattr(
        runtime,
        "_upload_object_bytes",
        lambda object_key, payload, *, content_type: uploaded_evidence.append((object_key, payload, content_type)),
    )
    monkeypatch.setattr(
        runtime.cast_screentime,
        "upsert_run_artifacts",
        lambda run_id, artifacts: artifact_payloads.append(artifacts) or artifacts,
    )
    monkeypatch.setattr(runtime.cast_screentime, "replace_cast_screentime_segments", lambda run_id, segments: segments)
    monkeypatch.setattr(runtime.cast_screentime, "replace_cast_screentime_evidence", lambda run_id, evidence: evidence)
    monkeypatch.setattr(
        runtime.cast_screentime,
        "replace_cast_screentime_excluded_sections",
        lambda run_id, sections: sections,
    )
    monkeypatch.setattr(runtime.cast_screentime, "replace_run_person_metrics", lambda run_id, metrics: metrics)

    result = runtime.run_screentime_analysis("run-123")

    assert result["run_id"] == "run-123"
    assert result["status"] == "success"
    assert update_calls[0]["payload"]["status"] == "running"
    assert update_calls[-1]["payload"]["status"] == "success"
    assert update_calls[-1]["payload"]["review_status"] == "ready_for_review"
    assert "execution_backend" in update_calls[-1]["payload"]["run_config_json"]
    assert "embedding_contract_key" in update_calls[-1]["payload"]["run_config_json"]
    persisted_keys = {item["artifact_key"] for item in artifact_payloads[-1]}
    assert {
        "segments.json",
        "shots.json",
        "scenes.json",
        "excluded_sections.json",
        "person_metrics.json",
    } <= persisted_keys
    assert uploaded_evidence == [("review/evidence/runs/run-123/segment-1/still_00000000.jpg", b"jpeg", "image/jpeg")]


def test_analyze_run_contract_counts_sample_window_duration(monkeypatch) -> None:
    class _FakeCapture:
        def __init__(self) -> None:
            self._frame_idx = 0

        def isOpened(self) -> bool:  # noqa: N802 - mirrors cv2.VideoCapture
            return True

        def get(self, prop: int) -> float:
            if prop == _FakeCv2.CAP_PROP_FPS:
                return 5.0
            if prop == _FakeCv2.CAP_PROP_FRAME_COUNT:
                return 10.0
            return 0.0

        def read(self):
            if self._frame_idx >= 10:
                return False, None
            self._frame_idx += 1
            return True, object()

        def release(self) -> None:
            return None

    class _FakeCv2:
        CAP_PROP_FPS = 1
        CAP_PROP_FRAME_COUNT = 2

        @staticmethod
        def VideoCapture(_path: str):  # noqa: N802 - mirrors cv2 module
            return _FakeCapture()

    monkeypatch.setattr(runtime, "_lazy_cv2", lambda: _FakeCv2)
    monkeypatch.setattr(runtime, "_localize_source_video", lambda _contract, work_dir: Path(work_dir) / "video.mp4")
    monkeypatch.setattr(runtime, "_encode_evidence_crop", lambda _frame, _bbox: b"jpeg")
    monkeypatch.setattr(
        runtime.screen_time_face_matching,
        "detect_faces",
        lambda _frame: (
            1,
            "deepface:ArcFace:retinaface:cosine:512d:l2_unit",
            [{"bbox": [0, 0, 10, 10], "confidence": 0.95, "square_crop_bbox": [0, 0, 10, 10]}],
        ),
    )
    monkeypatch.setattr(
        runtime.screen_time_face_matching,
        "filter_faces_for_screen_time",
        lambda raw_faces, *, image: (raw_faces, []),
    )
    monkeypatch.setattr(
        runtime.screen_time_face_matching,
        "match_faces_to_cast",
        lambda *_args, **_kwargs: [{"person_id": "person-1", "similarity": 0.91, "match_status": "matched"}],
    )
    monkeypatch.setattr(
        runtime.screen_time_face_matching,
        "normalize_screen_time_detections",
        lambda *_args, **_kwargs: [
            {
                "person_id": "person-1",
                "person_name": "Person One",
                "match_status": "matched",
                "match_reason": "threshold",
                "confidence": 0.95,
                "match_similarity": 0.91,
                "bbox": [0, 0, 10, 10],
                "square_crop_bbox": [0, 0, 10, 10],
                "filter_decision": "accepted",
            }
        ],
    )

    analysis = runtime.analyze_run_contract(
        {
            "id": "run-123",
            "run_type": "cast_screentime",
            "run_config_json": {"processing_mode": "balanced"},
            "candidate_cast_snapshot_json": [{"person_id": "person-1", "display_name": "Person One"}],
            "source_json": {"object_key": "source/videos/asset-1/original.mp4"},
            "duration_seconds": 2.0,
        }
    )

    assert [shot["duration_ms"] for shot in analysis["shots"]] == [1000, 1000]
    assert [segment["duration_ms"] for segment in analysis["segments"]] == [1000, 1000]
    assert analysis["metrics"][0]["person_id"] == "person-1"
    assert analysis["metrics"][0]["screen_time_seconds"] == 2.0
    assert analysis["metrics"][0]["frame_count"] == 2
    assert analysis["metrics"][0]["confidence_avg"] == 0.95
    assert analysis["metrics"][0]["metadata"] == {"display_name": "Person One", "segment_count": 2}


def test_generate_segment_clip_persists_backend_generated_clip(monkeypatch) -> None:
    run_contract = {
        "id": "run-123",
        "video_asset_id": "asset-1",
        "source_json": {"object_key": "source/videos/asset-1/original.mp4"},
        "duration_seconds": 42.5,
    }
    segment = {
        "segment_key": "segment-1",
        "person_id": "person-1",
        "start_ms": 1000,
        "end_ms": 4000,
        "duration_ms": 3000,
        "metadata": {"display_name": "Person One"},
    }
    persisted_evidence: list[list[dict[str, object]]] = []
    uploaded_clips: list[tuple[str, bytes, str]] = []

    monkeypatch.setattr(runtime, "load_run_contract", lambda run_id: dict(run_contract, id=run_id))
    monkeypatch.setattr(runtime.cast_screentime, "get_segment", lambda run_id, segment_key: segment)
    monkeypatch.setattr(runtime, "_render_segment_clip_bytes", lambda *args, **kwargs: b"mp4")
    monkeypatch.setattr(
        runtime,
        "_upload_object_bytes",
        lambda object_key, payload, *, content_type: uploaded_clips.append((object_key, payload, content_type)),
    )
    monkeypatch.setattr(
        runtime.cast_screentime,
        "upsert_cast_screentime_evidence",
        lambda run_id, evidence_items: (
            persisted_evidence.append(evidence_items)
            or [{**evidence_items[0], "created_at": "2026-04-03T00:00:00+00:00"}]
        ),
    )

    result = runtime.generate_segment_clip(
        "run-123",
        segment_key="segment-1",
        mode="timestamp",
        duration_seconds=8,
        ttl_days=7,
    )

    assert result["run_id"] == "run-123"
    assert result["evidence"]["evidence_type"] == "timestamp_clip"
    assert result["evidence"]["segment_key"] == "segment-1"
    assert persisted_evidence[-1][0]["metadata"]["mode"] == "timestamp"
    assert uploaded_clips == [
        (
            "review/evidence/runs/run-123/clips/clip-timestamp-8s-segment-1.mp4",
            b"mp4",
            "video/mp4",
        )
    ]
