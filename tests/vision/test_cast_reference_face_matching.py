from __future__ import annotations

import types

import numpy as np
import pytest

from trr_backend.services.face_reference_contract import FACE_REFERENCE_EMBEDDING_CONTRACT_KEY
from trr_backend.vision import cast_reference_face_matching as matching

pytestmark = pytest.mark.vision


def test_detect_faces_uses_insightface_faceanalysis(monkeypatch) -> None:
    face = types.SimpleNamespace(
        bbox=np.asarray([10, 20, 40, 60], dtype=np.float32),
        det_score=0.93,
        normed_embedding=np.asarray([1.0] + [0.0] * 511, dtype=np.float32),
    )
    model = types.SimpleNamespace(get=lambda _image: [face])
    monkeypatch.setattr(matching, "_face_analysis_model", model)

    count, model_id, faces = matching.detect_faces(np.zeros((100, 100, 3), dtype=np.uint8))

    assert count == 1
    assert model_id == FACE_REFERENCE_EMBEDDING_CONTRACT_KEY
    assert faces == [face]
    assert matching.extract_face_embedding(faces[0])[0] == 1.0
