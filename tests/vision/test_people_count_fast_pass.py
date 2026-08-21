from __future__ import annotations

import pytest

from trr_backend.vision import people_count_engine as engine

pytestmark = pytest.mark.vision


def test_compute_people_count_fast_pass_skips_reference_centroid_builders(
    monkeypatch,
) -> None:
    monkeypatch.setattr(engine, "_download_image", lambda url: object())
    monkeypatch.setattr(engine, "_ensure_detectors_available", lambda mode: None)
    monkeypatch.setattr(engine, "_detect_faces_retinaface", lambda image: (0, "retinaface", []))
    monkeypatch.setattr(engine, "_adaptive_filter_faces", lambda faces, image: ([], {}))
    monkeypatch.setattr(engine, "_count_people_yolo", lambda image: (1, "yolo", []))
    monkeypatch.setattr(engine, "_match_faces_to_people", lambda *args, **kwargs: {})
    monkeypatch.setattr(engine, "_normalize_face_detections", lambda *args, **kwargs: [])
    monkeypatch.setattr(engine, "_normalize_person_detections", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        engine,
        "_build_owner_reference_centroid_profile",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("owner reference centroid should be skipped")),
    )
    monkeypatch.setattr(
        engine,
        "_build_person_reference_centroids",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("person reference centroids should be skipped")),
    )

    result = engine.compute_people_count(
        {
            "image_url": "https://example.com/image.jpg",
            "mode": "faces_then_yolo",
            "owner_person_id": "person-1",
            "owner_reference_images": [{"url": "https://example.com/ref.jpg"}],
            "person_reference_images": [
                {"person_id": "person-2", "references": [{"url": "https://example.com/2.jpg"}]}
            ],
            "prefer_fast_pass": True,
        }
    )

    assert result["people_count"] == 1
    assert result["detector"] == "yolo"
    assert result["reference_profile"] is None
