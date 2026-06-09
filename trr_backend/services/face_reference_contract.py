"""Shared cast-reference embedding contract metadata."""

from __future__ import annotations

import os

FACE_REFERENCE_EMBEDDING_PROVIDER = "insightface"
FACE_REFERENCE_EMBEDDING_MODEL_NAME = str(
    os.getenv("CAST_REFERENCE_INSIGHTFACE_PROFILE") or os.getenv("INSIGHTFACE_PROFILE") or "antelopev2"
).strip()
FACE_REFERENCE_EMBEDDING_MODEL_VERSION = "faceanalysis-v1"
FACE_REFERENCE_EMBEDDING_DETECTOR = "insightface.FaceAnalysis"
FACE_REFERENCE_EMBEDDING_NORMALIZATION = "normed_embedding"
FACE_REFERENCE_EMBEDDING_DIMENSIONS = 512
FACE_REFERENCE_EMBEDDING_CONTRACT_KEY = (
    f"insightface:{FACE_REFERENCE_EMBEDDING_MODEL_NAME}:"
    f"faceanalysis:normed_embedding:{FACE_REFERENCE_EMBEDDING_DIMENSIONS}d:l2_unit"
)
