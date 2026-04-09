from __future__ import annotations

from trr_backend.clients.screenalytics import FaceBbox, PeopleCountResult
from trr_backend.services.person_images import detection


def _result(*detections: FaceBbox) -> PeopleCountResult:
    return PeopleCountResult(
        people_count=1,
        face_count=1,
        detector="simulated",
        model="sim",
        detections=list(detections),
    )


def test_build_auto_thumbnail_crop_payload_adds_timestamp(monkeypatch) -> None:
    monkeypatch.setattr(
        detection.screenalytics,
        "auto_thumbnail_crop",
        lambda _result: {"x": 48.0, "y": 28.0, "zoom": 1.2},
    )

    payload = detection.build_auto_thumbnail_crop_payload(_result())

    assert payload is not None
    assert payload["x"] == 48.0
    assert payload["generated_at"]


def test_build_auto_thumbnail_crop_payload_falls_back_to_face_centroid(monkeypatch) -> None:
    monkeypatch.setattr(detection.screenalytics, "auto_thumbnail_crop", lambda _result: None)
    monkeypatch.setattr(detection.screenalytics, "face_centroid", lambda _result: (44.5, 21.0))

    payload = detection.build_auto_thumbnail_crop_payload(_result())

    assert payload == {
        "x": 44.5,
        "y": 21.0,
        "zoom": 1,
        "mode": "auto",
        "strategy": "face_centroid_v1",
        "generated_at": payload["generated_at"],
    }


def test_count_people_with_fallback_retries_legacy_signature(monkeypatch) -> None:
    calls: list[tuple[str, dict[str, object]]] = []
    expected = _result(FaceBbox(x1=0.1, y1=0.1, x2=0.2, y2=0.2, confidence=0.9))

    def fake_count_people(image_url: str, **kwargs):
        calls.append((image_url, kwargs))
        if "owner_person_id" in kwargs:
            raise TypeError("legacy runtime")
        return expected

    monkeypatch.setattr(detection.screenalytics, "count_people", fake_count_people)

    result = detection.count_people_with_fallback(
        "https://example.com/image.jpg",
        candidate_person_ids=["person-1"],
        owner_person_id="person-1",
        prefer_fast_pass=True,
    )

    assert result is expected
    assert len(calls) == 2
    assert calls[0][1]["owner_person_id"] == "person-1"
    assert calls[1][1] == {"candidate_person_ids": ["person-1"]}


def test_count_people_batch_with_fallback_retries_without_fast_pass(monkeypatch) -> None:
    calls: list[dict[str, object]] = []
    expected = [_result()]

    def fake_count_people_batch(image_requests, **kwargs):
        calls.append(kwargs)
        if "prefer_fast_pass" in kwargs:
            raise TypeError("legacy runtime")
        return expected

    monkeypatch.setattr(detection.screenalytics, "count_people_batch", fake_count_people_batch)

    result = detection.count_people_batch_with_fallback([{"image_url": "https://example.com/image.jpg"}])

    assert result is expected
    assert calls == [{"prefer_fast_pass": True}, {}]
