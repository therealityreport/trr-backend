"""Shared face-reference embedding contract metadata.

The admin vision worker only needs the contract key when matching retained
people-count references. Keep this module free of DeepFace/TensorFlow imports so
the Modal vision image can stay on the lighter InsightFace/YOLO runtime.
"""

from __future__ import annotations

FACE_REFERENCE_EMBEDDING_PROVIDER = "deepface"
FACE_REFERENCE_EMBEDDING_MODEL_NAME = "ArcFace"
FACE_REFERENCE_EMBEDDING_MODEL_VERSION = "v1"
FACE_REFERENCE_EMBEDDING_DETECTOR = "retinaface"
FACE_REFERENCE_EMBEDDING_NORMALIZATION = "base"
FACE_REFERENCE_EMBEDDING_DIMENSIONS = 512
FACE_REFERENCE_EMBEDDING_CONTRACT_KEY = "deepface:arcface:retinaface:base:512d:l2_unit"
