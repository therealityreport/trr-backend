"""Backend-owned artifact registry for retained cast-screentime flows."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class CastScreentimeArtifact:
    key: str
    artifact_kind: str
    schema_version: str
    default_payload: Any
    description: str


SHOTS = CastScreentimeArtifact(
    key="shots.json",
    artifact_kind="shots",
    schema_version="cast_screentime.v1",
    default_payload=[],
    description="Shot boundaries and cut metadata.",
)
SEGMENTS = CastScreentimeArtifact(
    key="segments.json",
    artifact_kind="segments",
    schema_version="cast_screentime.v1",
    default_payload=[],
    description="Reviewable person-assigned segments.",
)
SCENES = CastScreentimeArtifact(
    key="scenes.json",
    artifact_kind="scenes",
    schema_version="cast_screentime.v1",
    default_payload=[],
    description="Scene groupings derived from shots and semantic cues.",
)
EXCLUDED_SECTIONS = CastScreentimeArtifact(
    key="excluded_sections.json",
    artifact_kind="excluded_sections",
    schema_version="cast_screentime.v1",
    default_payload=[],
    description="Sections intentionally excluded from official totals.",
)
PERSON_METRICS = CastScreentimeArtifact(
    key="person_metrics.json",
    artifact_kind="person_metrics",
    schema_version="cast_screentime.v1",
    default_payload=[],
    description="Per-person totals and aggregate metrics.",
)
TITLE_CARD_CANDIDATES = CastScreentimeArtifact(
    key="title_card_candidates.json",
    artifact_kind="title_card_candidates",
    schema_version="cast_screentime.v1",
    default_payload=[],
    description="Reviewable title-card candidates.",
)
TITLE_CARD_REFERENCE_SIGNATURES = CastScreentimeArtifact(
    key="title_card_reference_signatures.json",
    artifact_kind="title_card_reference_signatures",
    schema_version="cast_screentime.v1",
    default_payload=[],
    description="Reference signatures harvested from approved title cards.",
)
CONFESSIONAL_CANDIDATES = CastScreentimeArtifact(
    key="confessional_candidates.json",
    artifact_kind="confessional_candidates",
    schema_version="cast_screentime.v1",
    default_payload=[],
    description="Reviewable confessional candidates.",
)
CAST_SUGGESTIONS = CastScreentimeArtifact(
    key="cast_suggestions.json",
    artifact_kind="cast_suggestions",
    schema_version="cast_screentime.v1",
    default_payload=[],
    description="Unknown-to-known suggestion candidates for operator review.",
)
UNKNOWN_REVIEW_QUEUES = CastScreentimeArtifact(
    key="unknown_review_queues.json",
    artifact_kind="unknown_review_queues",
    schema_version="cast_screentime.v1",
    default_payload=[],
    description="Grouped unknown detections that need operator review.",
)
REFERENCE_FINGERPRINTS = CastScreentimeArtifact(
    key="reference_fingerprints.json",
    artifact_kind="reference_fingerprints",
    schema_version="cast_screentime.v1",
    default_payload=[],
    description="Published reference signatures reused by later runs.",
)


ARTIFACT_REGISTRY: dict[str, CastScreentimeArtifact] = {
    artifact.key: artifact
    for artifact in (
        SHOTS,
        SEGMENTS,
        SCENES,
        EXCLUDED_SECTIONS,
        PERSON_METRICS,
        TITLE_CARD_CANDIDATES,
        TITLE_CARD_REFERENCE_SIGNATURES,
        CONFESSIONAL_CANDIDATES,
        CAST_SUGGESTIONS,
        UNKNOWN_REVIEW_QUEUES,
        REFERENCE_FINGERPRINTS,
    )
}
