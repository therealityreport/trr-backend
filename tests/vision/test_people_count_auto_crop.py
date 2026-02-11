from __future__ import annotations

from trr_backend.clients.screenalytics import FaceBbox, PeopleCountResult, auto_thumbnail_crop


def _result(*detections: FaceBbox) -> PeopleCountResult:
    return PeopleCountResult(
        people_count=1,
        face_count=1,
        detector="simulated",
        model="sim",
        detections=list(detections),
    )


def test_auto_thumbnail_crop_uses_face_torso_when_both_available() -> None:
    result = _result(
        FaceBbox(x1=0.42, y1=0.10, x2=0.58, y2=0.28, confidence=0.95, kind="face"),
        FaceBbox(x1=0.30, y1=0.02, x2=0.72, y2=0.92, confidence=0.89, kind="person"),
    )
    crop = auto_thumbnail_crop(result)
    assert crop is not None
    assert crop["mode"] == "auto"
    assert crop["strategy"] == "face_torso_v2"
    assert 45 <= float(crop["x"]) <= 55
    assert 18 <= float(crop["y"]) <= 40
    assert 1.0 <= float(crop["zoom"]) <= 1.6


def test_auto_thumbnail_crop_face_only_fallback() -> None:
    result = _result(
        FaceBbox(x1=0.20, y1=0.08, x2=0.40, y2=0.30, confidence=0.91, kind="face"),
    )
    crop = auto_thumbnail_crop(result)
    assert crop is not None
    assert float(crop["x"]) < 40
    assert float(crop["y"]) > 15
    assert 1.0 <= float(crop["zoom"]) <= 1.6


def test_auto_thumbnail_crop_person_only_fallback() -> None:
    result = _result(
        FaceBbox(x1=0.48, y1=0.08, x2=0.82, y2=0.95, confidence=0.86, kind="person"),
    )
    crop = auto_thumbnail_crop(result)
    assert crop is not None
    assert float(crop["x"]) > 60
    assert float(crop["y"]) > 20
    assert 1.0 <= float(crop["zoom"]) <= 1.6


def test_auto_thumbnail_crop_returns_none_without_detections() -> None:
    result = _result()
    crop = auto_thumbnail_crop(result)
    assert crop is None
