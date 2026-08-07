from __future__ import annotations

from typing import Any

import pytest

from trr_backend.vision import people_count_service as client


def test_count_people_uses_local_backend_without_screenalytics_api_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("SCREENALYTICS_API_URL", raising=False)
    monkeypatch.setenv("TRR_ADMIN_IMAGE_EXECUTION_BACKEND", "local")

    captured: dict[str, Any] = {}

    def _fake_local(payload: dict[str, object]) -> dict[str, object]:
        captured.update(payload)
        return {
            "people_count": 2,
            "face_count": 2,
            "face_count_raw": 2,
            "face_count_filtered": 2,
            "face_filter_thresholds": {"min_side_px": 56, "min_area_ratio": 0.004},
            "detector": "retinaface",
            "model": "retinaface_antelopev2",
            "detections": [],
        }

    monkeypatch.setattr(client, "_invoke_people_count_local", _fake_local)

    result = client.count_people("https://example.com/image.jpg")

    assert captured["image_url"] == "https://example.com/image.jpg"
    assert result.people_count == 2
    assert result.detector == "retinaface"


def test_count_people_uses_modal_backend_when_configured(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRR_ADMIN_IMAGE_EXECUTION_BACKEND", "modal")
    monkeypatch.setenv("TRR_MODAL_ENABLED", "1")
    monkeypatch.setenv("TRR_MODAL_APP_NAME", "trr-backend-jobs")
    monkeypatch.setenv("TRR_MODAL_VISION_FUNCTION", "run_admin_vision")

    captured: dict[str, Any] = {}

    def _fake_modal(payload: dict[str, object], *, batch: bool = False) -> dict[str, object]:
        captured["payload"] = payload
        captured["batch"] = batch
        return {
            "people_count": 1,
            "face_count": 1,
            "face_count_raw": 1,
            "face_count_filtered": 1,
            "face_filter_thresholds": {"min_side_px": 56, "min_area_ratio": 0.004},
            "detector": "modal",
            "model": "run_admin_vision",
            "detections": [],
        }

    monkeypatch.setattr(client, "_invoke_people_count_modal", _fake_modal)

    result = client.count_people("https://example.com/remote.jpg", candidate_person_ids=["abc"])

    assert captured["batch"] is False
    assert captured["payload"]["candidate_person_ids"] == ["abc"]
    assert result.people_count == 1
    assert result.detector == "modal"


def test_count_people_batch_uses_local_backend_without_screenalytics_api_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("SCREENALYTICS_API_URL", raising=False)
    monkeypatch.setenv("TRR_ADMIN_IMAGE_EXECUTION_BACKEND", "local")

    def _fake_batch(payload: dict[str, object]) -> dict[str, object]:
        return {
            "results": [
                {
                    "image_url": "https://example.com/a.jpg",
                    "result": {
                        "people_count": 3,
                        "face_count": 3,
                        "face_count_raw": 3,
                        "face_count_filtered": 3,
                        "face_filter_thresholds": {"min_side_px": 56, "min_area_ratio": 0.004},
                        "detector": "retinaface",
                        "model": "retinaface_antelopev2",
                        "detections": [],
                    },
                    "error": None,
                }
            ]
        }

    monkeypatch.setattr(client, "_invoke_people_count_batch_local", _fake_batch)

    results = client.count_people_batch([{"image_url": "https://example.com/a.jpg"}])

    assert len(results) == 1
    assert results[0] is not None
    assert results[0].people_count == 3
