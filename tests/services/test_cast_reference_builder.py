from __future__ import annotations

import types

import numpy as np
import pytest

from trr_backend.services import cast_reference_builder as builder


@pytest.fixture(autouse=True)
def _fake_cv2(monkeypatch) -> None:
    fake_cv2 = types.SimpleNamespace(
        COLOR_BGR2GRAY=0,
        CV_64F=0,
        cvtColor=lambda image, _conversion: image,
        Laplacian=lambda _image, _depth: types.SimpleNamespace(var=lambda: 0.0),
    )
    monkeypatch.setattr(builder, "_lazy_cv2", lambda: fake_cv2)


def _image():
    rng = np.random.default_rng(1)
    return rng.integers(0, 255, size=(240, 240, 3), dtype=np.uint8)


def _face(*, bbox=None, score=0.95, embedding=None):
    return types.SimpleNamespace(
        bbox=np.asarray(bbox or [40, 40, 180, 180], dtype=np.float32),
        det_score=score,
        normed_embedding=np.asarray(embedding or [1.0] + [0.0] * 511, dtype=np.float32),
    )


def test_build_cast_reference_seed_accepts_single_quality_gallery_face(monkeypatch) -> None:
    monkeypatch.setenv("CAST_REFERENCE_MIN_BLUR_SCORE", "0")
    face = _face()
    monkeypatch.setattr(builder, "_load_image", lambda _source: _image())
    monkeypatch.setattr(builder.cast_reference_face_matching, "detect_faces", lambda _image: (1, "model", [face]))

    result = builder.build_cast_reference_seed(
        image_source="gallery.jpg",
        assigned_person_id="person-1",
    )

    assert result["status"] == "ready"
    assert result["embedding"][0] == 1.0
    assert result["metadata"]["builder"] == "cast_reference_builder"
    assert result["metadata"]["assigned_person_id"] == "person-1"
    assert result["metadata"]["selected_face_index"] == 0


def test_build_cast_reference_seed_queues_multiface_gallery_without_selection(monkeypatch) -> None:
    monkeypatch.setenv("CAST_REFERENCE_MIN_BLUR_SCORE", "0")
    monkeypatch.setattr(builder, "_load_image", lambda _source: _image())
    monkeypatch.setattr(
        builder.cast_reference_face_matching,
        "detect_faces",
        lambda _image: (2, "model", [_face(), _face(bbox=[20, 20, 120, 120])]),
    )

    result = builder.build_cast_reference_seed(image_source="gallery.jpg")

    assert result["status"] == "review"
    assert result["review_reason"] == "multiple_faces_requires_human_selection"
    assert len(result["metadata"]["candidate_faces"]) == 2


def test_build_cast_reference_seed_accepts_review_selected_multiface(monkeypatch) -> None:
    monkeypatch.setenv("CAST_REFERENCE_MIN_BLUR_SCORE", "0")
    monkeypatch.setattr(builder, "_load_image", lambda _source: _image())
    monkeypatch.setattr(
        builder.cast_reference_face_matching,
        "detect_faces",
        lambda _image: (2, "model", [_face(embedding=[0.0, 1.0] + [0.0] * 510), _face()]),
    )

    result = builder.build_cast_reference_seed(image_source="gallery.jpg", selected_face_index=1)

    assert result["status"] == "ready"
    assert result["metadata"]["selected_face_index"] == 1
    assert result["embedding"][0] == 1.0


def test_build_cast_reference_seed_queues_low_quality_face(monkeypatch) -> None:
    monkeypatch.setenv("CAST_REFERENCE_MIN_BLUR_SCORE", "0")
    monkeypatch.setattr(builder, "_load_image", lambda _source: _image())
    monkeypatch.setattr(
        builder.cast_reference_face_matching,
        "detect_faces",
        lambda _image: (1, "model", [_face(bbox=[1, 1, 20, 20])]),
    )

    result = builder.build_cast_reference_seed(image_source="gallery.jpg")

    assert result["status"] == "review"
    assert result["review_reason"] == "selected_face_failed_quality_filter"
    assert "face_too_small" in result["metadata"]["selected_face_quality"]["reasons"]
