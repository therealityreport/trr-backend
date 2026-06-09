from __future__ import annotations

import numpy as np

from trr_backend.vision import screen_time_face_matching as matching


def test_detect_faces_uses_deepface_represent(monkeypatch) -> None:
    class _FakeDeepFace:
        @staticmethod
        def represent(**kwargs):
            assert kwargs["model_name"] == "ArcFace"
            assert kwargs["detector_backend"] == "retinaface"
            return [
                {
                    "embedding": [1.0] + [0.0] * 511,
                    "facial_area": {"x": 10, "y": 20, "w": 30, "h": 40},
                    "face_confidence": 0.93,
                }
            ]

    monkeypatch.setattr(matching, "_deepface_module", None)
    monkeypatch.setattr(matching, "get_deepface_runtime", lambda: _FakeDeepFace)

    count, model_id, faces = matching.detect_faces(np.zeros((100, 100, 3), dtype=np.uint8))

    assert count == 1
    assert model_id == matching.SCREEN_TIME_FACE_CONTRACT_KEY
    assert faces[0].bbox.tolist() == [10.0, 20.0, 40.0, 60.0]
    assert faces[0].det_score == 0.93
    assert matching.extract_face_embedding(faces[0])[0] == 1.0


def test_match_faces_to_cast_blocks_mixed_deepface_and_reference_contracts() -> None:
    face = matching.DeepFaceScreenTimeFace(
        bbox=np.asarray([10, 20, 40, 60], dtype=np.float32),
        det_score=0.93,
        embedding=np.asarray([1.0] + [0.0] * 511, dtype=np.float32),
        facial_area={"x": 10, "y": 20, "w": 30, "h": 40},
        raw={},
    )

    matches = matching.match_faces_to_cast([face], np.zeros((100, 100, 3), dtype=np.uint8))

    assert matches[0]["match_status"] == "unassigned"
    assert matches[0]["match_reason"] == "reference_contract_mismatch"
    assert str(matches[0]["face_contract_key"]).startswith("deepface:")
    assert str(matches[0]["reference_contract_key"]).startswith("insightface:")
