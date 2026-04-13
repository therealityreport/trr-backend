from __future__ import annotations

from types import SimpleNamespace

from trr_backend.vision.people_count_service import auto_thumbnail_crop


def test_auto_thumbnail_crop_prefers_matched_face_over_higher_confidence() -> None:
    """When a matched face exists, prefer it over a higher-confidence unmatched face."""
    result = SimpleNamespace(
        detections=[
            SimpleNamespace(
                kind="face",
                x1=0.1,
                y1=0.1,
                x2=0.3,
                y2=0.4,
                confidence=0.95,
                match_status="below_threshold",
                match_similarity=0.07,
                person_id=None,
            ),
            SimpleNamespace(
                kind="face",
                x1=0.6,
                y1=0.1,
                x2=0.8,
                y2=0.4,
                confidence=0.85,
                match_status="matched",
                match_similarity=0.76,
                person_id="owner-uuid",
            ),
            SimpleNamespace(
                kind="person",
                x1=0.55,
                y1=0.0,
                x2=0.85,
                y2=0.9,
                confidence=0.80,
            ),
        ],
    )
    crop = auto_thumbnail_crop(result)
    assert crop is not None
    # Should center on the matched face (x ~ 0.7 -> 70%), not the unmatched face (x ~ 0.2 -> 20%)
    assert crop["x"] > 50, f"Expected crop centered on matched face (right side), got x={crop['x']}"


def test_auto_thumbnail_crop_falls_back_to_confidence_when_no_match_info() -> None:
    """When no match_status exists on faces, fall back to highest confidence."""
    result = SimpleNamespace(
        detections=[
            SimpleNamespace(
                kind="face",
                x1=0.1,
                y1=0.1,
                x2=0.3,
                y2=0.4,
                confidence=0.95,
            ),
            SimpleNamespace(
                kind="face",
                x1=0.6,
                y1=0.1,
                x2=0.8,
                y2=0.4,
                confidence=0.85,
            ),
        ],
    )
    crop = auto_thumbnail_crop(result)
    assert crop is not None
    # Should center on the higher-confidence face (left side)
    assert crop["x"] < 50, f"Expected crop centered on higher-confidence face, got x={crop['x']}"
