from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal

POST_DETAIL_PART = "post_detail"
CANONICAL_POST_ROW_PART = "canonical_post_row"
MEDIA_ASSETS_PART = "media_assets"
HOSTED_MEDIA_PART = "hosted_media"
COMMENTS_PART = "comments"
REPLIES_PART = "replies"
COMMENT_MEDIA_PART = "comment_media"
AUTHOR_AVATAR_PART = "author_avatar"
COLLABORATORS_PART = "collaborators"
TAGS_PART = "tags"
LOCATION_PART = "location"
MUSIC_PART = "music"
AD_FLAGS_PART = "ad_flags"

INSTAGRAM_SNAPSHOT_PARTS: tuple[str, ...] = (
    POST_DETAIL_PART,
    CANONICAL_POST_ROW_PART,
    MEDIA_ASSETS_PART,
    HOSTED_MEDIA_PART,
    COMMENTS_PART,
    REPLIES_PART,
    COMMENT_MEDIA_PART,
    AUTHOR_AVATAR_PART,
    COLLABORATORS_PART,
    TAGS_PART,
    LOCATION_PART,
    MUSIC_PART,
    AD_FLAGS_PART,
)
SNAPSHOT_PART_NAMES = INSTAGRAM_SNAPSHOT_PARTS

CAPTURED_STATE = "captured"
RETRYABLE_STATE = "retryable"
SOURCE_UNAVAILABLE_STATE = "source_unavailable"
BLOCKED_STATE = "blocked"
DEFERRED_STATE = "deferred"

SNAPSHOT_COMPLETION_STATES: tuple[str, ...] = (
    CAPTURED_STATE,
    RETRYABLE_STATE,
    SOURCE_UNAVAILABLE_STATE,
    BLOCKED_STATE,
    DEFERRED_STATE,
)

SnapshotPartName = Literal[
    "post_detail",
    "canonical_post_row",
    "media_assets",
    "hosted_media",
    "comments",
    "replies",
    "comment_media",
    "author_avatar",
    "collaborators",
    "tags",
    "location",
    "music",
    "ad_flags",
]
SnapshotPartState = Literal[
    "captured",
    "retryable",
    "source_unavailable",
    "blocked",
    "deferred",
]

__all__ = [
    "AD_FLAGS_PART",
    "AUTHOR_AVATAR_PART",
    "BLOCKED_STATE",
    "CANONICAL_POST_ROW_PART",
    "CAPTURED_STATE",
    "COLLABORATORS_PART",
    "COMMENT_MEDIA_PART",
    "COMMENTS_PART",
    "DEFERRED_STATE",
    "HOSTED_MEDIA_PART",
    "INSTAGRAM_SNAPSHOT_PARTS",
    "LOCATION_PART",
    "MEDIA_ASSETS_PART",
    "MUSIC_PART",
    "POST_DETAIL_PART",
    "REPLIES_PART",
    "RETRYABLE_STATE",
    "SNAPSHOT_COMPLETION_STATES",
    "SNAPSHOT_PART_NAMES",
    "SOURCE_UNAVAILABLE_STATE",
    "TAGS_PART",
    "SnapshotCompletionSummary",
    "SnapshotPartCompletion",
    "SnapshotPartName",
    "SnapshotPartState",
    "SnapshotRetryMetadata",
    "SnapshotRetryTarget",
    "SnapshotSourceUnavailableEvidence",
    "build_instagram_snapshot_completion",
    "build_snapshot_completion_summary",
    "summarize_snapshot_completion",
]


@dataclass(frozen=True)
class SnapshotSourceUnavailableEvidence:
    reason: str
    source: str | None = None
    observed_at: datetime | str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def to_metadata(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"reason": self.reason}
        if self.source:
            payload["source"] = self.source
        if self.observed_at:
            payload["observed_at"] = _metadata_datetime(self.observed_at)
        if self.metadata:
            payload["metadata"] = dict(self.metadata)
        return payload

    to_dict = to_metadata


@dataclass(frozen=True)
class SnapshotRetryMetadata:
    reason: str | None = None
    retry_key: str | None = None
    attempt_count: int | None = None
    max_attempts: int | None = None
    retry_after: datetime | str | None = None
    priority: int | None = None
    error_code: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def to_metadata(self) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        if self.reason:
            payload["reason"] = self.reason
        if self.retry_key:
            payload["retry_key"] = self.retry_key
        if self.attempt_count is not None:
            payload["attempt_count"] = self.attempt_count
        if self.max_attempts is not None:
            payload["max_attempts"] = self.max_attempts
        if self.retry_after:
            payload["retry_after"] = _metadata_datetime(self.retry_after)
        if self.priority is not None:
            payload["priority"] = self.priority
        if self.error_code:
            payload["error_code"] = self.error_code
        if self.metadata:
            payload["metadata"] = dict(self.metadata)
        return payload

    to_dict = to_metadata


@dataclass(frozen=True)
class SnapshotRetryTarget:
    part: str
    retry_key: str
    reason: str
    attempt_count: int | None = None
    max_attempts: int | None = None
    retry_after: datetime | str | None = None
    priority: int | None = None
    error_code: str | None = None
    target: Mapping[str, Any] = field(default_factory=dict)
    metadata: Mapping[str, Any] = field(default_factory=dict)

    def to_metadata(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "part": self.part,
            "state": RETRYABLE_STATE,
            "retry_key": self.retry_key,
            "reason": self.reason,
        }
        if self.target:
            payload["target"] = dict(self.target)
        if self.attempt_count is not None:
            payload["attempt_count"] = self.attempt_count
        if self.max_attempts is not None:
            payload["max_attempts"] = self.max_attempts
        if self.retry_after:
            payload["retry_after"] = _metadata_datetime(self.retry_after)
        if self.priority is not None:
            payload["priority"] = self.priority
        if self.error_code:
            payload["error_code"] = self.error_code
        if self.metadata:
            payload["metadata"] = dict(self.metadata)
        return payload

    to_dict = to_metadata


@dataclass(frozen=True)
class SnapshotPartCompletion:
    part: str
    state: SnapshotPartState
    reason: str | None = None
    evidence: Mapping[str, Any] = field(default_factory=dict)
    retry: SnapshotRetryTarget | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    @property
    def is_terminal(self) -> bool:
        return self.state in {CAPTURED_STATE, SOURCE_UNAVAILABLE_STATE}

    def to_metadata(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "part": self.part,
            "state": self.state,
        }
        if self.reason:
            payload["reason"] = self.reason
        if self.evidence:
            payload["evidence"] = dict(self.evidence)
        if self.retry:
            payload["retry"] = self.retry.to_metadata()
        if self.metadata:
            payload["metadata"] = dict(self.metadata)
        return payload

    to_dict = to_metadata


@dataclass(frozen=True)
class SnapshotCompletionSummary:
    parts: tuple[SnapshotPartCompletion, ...]
    retry_targets: tuple[SnapshotRetryTarget, ...]
    target: Mapping[str, Any] = field(default_factory=dict)

    @property
    def parts_by_name(self) -> dict[str, SnapshotPartCompletion]:
        return {part.part: part for part in self.parts}

    @property
    def part_states(self) -> dict[str, str]:
        return {part.part: part.state for part in self.parts}

    @property
    def state_counts(self) -> dict[str, int]:
        counts = dict.fromkeys(SNAPSHOT_COMPLETION_STATES, 0)
        for part in self.parts:
            counts[part.state] = counts.get(part.state, 0) + 1
        return counts

    @property
    def complete(self) -> bool:
        return all(part.is_terminal for part in self.parts)

    @property
    def captured_parts(self) -> tuple[str, ...]:
        return self._parts_for_state(CAPTURED_STATE)

    @property
    def retryable_parts(self) -> tuple[str, ...]:
        return self._parts_for_state(RETRYABLE_STATE)

    @property
    def missing_parts(self) -> tuple[str, ...]:
        return tuple(part.part for part in self.parts if part.state != CAPTURED_STATE)

    @property
    def source_unavailable_parts(self) -> tuple[str, ...]:
        return self._parts_for_state(SOURCE_UNAVAILABLE_STATE)

    @property
    def blocked_parts(self) -> tuple[str, ...]:
        return self._parts_for_state(BLOCKED_STATE)

    @property
    def deferred_parts(self) -> tuple[str, ...]:
        return self._parts_for_state(DEFERRED_STATE)

    @property
    def retry_targets_by_part(self) -> dict[str, SnapshotRetryTarget]:
        return {target.part: target for target in self.retry_targets}

    def to_metadata(self) -> dict[str, Any]:
        return {
            "complete": self.complete,
            "state": "complete" if self.complete else "incomplete",
            "target": dict(self.target),
            "state_counts": self.state_counts,
            "captured_parts": list(self.captured_parts),
            "missing_parts": list(self.missing_parts),
            "retryable_parts": list(self.retryable_parts),
            "source_unavailable_parts": list(self.source_unavailable_parts),
            "blocked_parts": list(self.blocked_parts),
            "deferred_parts": list(self.deferred_parts),
            "parts": {part.part: part.to_metadata() for part in self.parts},
            "retry_targets": [target.to_metadata() for target in self.retry_targets],
        }

    to_dict = to_metadata

    def _parts_for_state(self, state: str) -> tuple[str, ...]:
        return tuple(part.part for part in self.parts if part.state == state)


def build_snapshot_completion_summary(
    *,
    captured_parts: Iterable[str] | Mapping[str, Any] = (),
    missing_parts: Iterable[str] | Mapping[str, Any] | None = None,
    source_unavailable_evidence: Mapping[str, Any] | Iterable[Mapping[str, Any]] = (),
    blocked_parts: Iterable[str] | Mapping[str, Any] = (),
    deferred_parts: Iterable[str] | Mapping[str, Any] = (),
    retry_metadata: Mapping[str, Any] | None = None,
    expected_parts: Iterable[str] = INSTAGRAM_SNAPSHOT_PARTS,
    snapshot_id: str | None = None,
    post_id: str | None = None,
    shortcode: str | None = None,
    account_handle: str | None = None,
    target_metadata: Mapping[str, Any] | None = None,
) -> SnapshotCompletionSummary:
    expected = tuple(_normalize_part_name(part) for part in expected_parts)
    _validate_unique_parts(expected)

    captured = _normalize_part_mapping(captured_parts, expected_parts=expected, label="captured_parts")
    explicit_missing = (
        _normalize_part_mapping(missing_parts, expected_parts=expected, label="missing_parts")
        if missing_parts is not None
        else {}
    )
    unavailable = _normalize_source_unavailable_evidence(source_unavailable_evidence, expected_parts=expected)
    blocked = _normalize_part_mapping(blocked_parts, expected_parts=expected, label="blocked_parts")
    deferred = _normalize_part_mapping(deferred_parts, expected_parts=expected, label="deferred_parts")
    retry_by_part = _normalize_retry_metadata(retry_metadata, expected_parts=expected)

    target = _snapshot_target(
        snapshot_id=snapshot_id,
        post_id=post_id,
        shortcode=shortcode,
        account_handle=account_handle,
        target_metadata=target_metadata or {},
    )

    parts: list[SnapshotPartCompletion] = []
    retry_targets: list[SnapshotRetryTarget] = []
    for part in expected:
        captured_evidence = captured.get(part)
        if part in captured:
            parts.append(
                SnapshotPartCompletion(
                    part=part,
                    state=CAPTURED_STATE,
                    reason=_reason_from_evidence(captured_evidence),
                    evidence=_metadata_from_value(captured_evidence),
                )
            )
            continue

        missing_evidence = explicit_missing.get(part, {})
        if part in blocked:
            blocked_evidence = _metadata_from_value(blocked[part])
            parts.append(
                SnapshotPartCompletion(
                    part=part,
                    state=BLOCKED_STATE,
                    reason=_reason_from_evidence(blocked[part]) or _reason_from_evidence(missing_evidence),
                    evidence=blocked_evidence,
                    metadata=_part_metadata(missing_evidence=missing_evidence),
                )
            )
            continue

        if part in deferred:
            deferred_evidence = _metadata_from_value(deferred[part])
            parts.append(
                SnapshotPartCompletion(
                    part=part,
                    state=DEFERRED_STATE,
                    reason=_reason_from_evidence(deferred[part]) or _reason_from_evidence(missing_evidence),
                    evidence=deferred_evidence,
                    metadata=_part_metadata(missing_evidence=missing_evidence),
                )
            )
            continue

        if part in unavailable:
            evidence = unavailable[part]
            parts.append(
                SnapshotPartCompletion(
                    part=part,
                    state=SOURCE_UNAVAILABLE_STATE,
                    reason=_reason_from_evidence(evidence) or _reason_from_evidence(missing_evidence),
                    evidence=evidence,
                    metadata=_part_metadata(missing_evidence=missing_evidence),
                )
            )
            continue

        retry_target = _build_retry_target(
            part=part,
            target=target,
            missing_evidence=missing_evidence,
            retry_metadata=retry_by_part.get(part, {}),
        )
        retry_targets.append(retry_target)
        parts.append(
            SnapshotPartCompletion(
                part=part,
                state=RETRYABLE_STATE,
                reason=retry_target.reason,
                evidence=_metadata_from_value(missing_evidence),
                retry=retry_target,
            )
        )

    return SnapshotCompletionSummary(parts=tuple(parts), retry_targets=tuple(retry_targets), target=target)


build_instagram_snapshot_completion = build_snapshot_completion_summary
summarize_snapshot_completion = build_snapshot_completion_summary


def _build_retry_target(
    *,
    part: str,
    target: Mapping[str, Any],
    missing_evidence: Any,
    retry_metadata: Mapping[str, Any],
) -> SnapshotRetryTarget:
    metadata = dict(retry_metadata)
    reason = _string_value(metadata.pop("reason", None)) or _reason_from_evidence(missing_evidence) or f"{part}_missing"
    retry_key = _string_value(metadata.pop("retry_key", None)) or _default_retry_key(part=part, target=target)
    attempt_count = _optional_int(metadata.pop("attempt_count", None))
    max_attempts = _optional_int(metadata.pop("max_attempts", None))
    retry_after = metadata.pop("retry_after", None)
    priority = _optional_int(metadata.pop("priority", None))
    error_code = _string_value(metadata.pop("error_code", None))

    nested_metadata = metadata.pop("metadata", {})
    merged_metadata = _metadata_from_value(nested_metadata)
    merged_metadata.update(dict(metadata))

    return SnapshotRetryTarget(
        part=part,
        retry_key=retry_key,
        reason=reason,
        attempt_count=attempt_count,
        max_attempts=max_attempts,
        retry_after=retry_after,
        priority=priority,
        error_code=error_code,
        target=target,
        metadata=merged_metadata,
    )


def _default_retry_key(*, part: str, target: Mapping[str, Any]) -> str:
    target_id = (
        _string_value(target.get("snapshot_id"))
        or _string_value(target.get("post_id"))
        or _string_value(target.get("shortcode"))
        or _string_value(target.get("account_handle"))
        or "unknown"
    )
    return f"instagram_snapshot:{target_id}:{part}"


def _normalize_part_mapping(
    value: Iterable[str] | Mapping[str, Any] | None,
    *,
    expected_parts: tuple[str, ...],
    label: str,
) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, Mapping):
        normalized = {_normalize_part_name(part): evidence for part, evidence in value.items() if _string_value(part)}
    elif isinstance(value, str):
        normalized = {_normalize_part_name(value): {}}
    else:
        normalized = {_normalize_part_name(part): {} for part in value if _string_value(part)}
    _validate_known_parts(normalized, expected_parts=expected_parts, label=label)
    return normalized


def _normalize_source_unavailable_evidence(
    value: Mapping[str, Any] | Iterable[Mapping[str, Any]] | None,
    *,
    expected_parts: tuple[str, ...],
) -> dict[str, dict[str, Any]]:
    if value is None:
        return {}

    normalized: dict[str, dict[str, Any]] = {}
    if isinstance(value, Mapping):
        for part, evidence in value.items():
            normalized_part = _normalize_part_name(part)
            metadata = _metadata_from_value(evidence)
            if _has_source_unavailable_evidence(metadata):
                normalized[normalized_part] = metadata
    else:
        for item in value:
            metadata = _metadata_from_value(item)
            part = _normalize_part_name(metadata.pop("part", ""))
            if _has_source_unavailable_evidence(metadata):
                normalized[part] = metadata

    _validate_known_parts(normalized, expected_parts=expected_parts, label="source_unavailable_evidence")
    return normalized


def _normalize_retry_metadata(
    value: Mapping[str, Any] | None,
    *,
    expected_parts: tuple[str, ...],
) -> dict[str, dict[str, Any]]:
    if value is None:
        return {}
    normalized = {
        _normalize_part_name(part): _metadata_from_value(metadata)
        for part, metadata in value.items()
        if _string_value(part)
    }
    _validate_known_parts(normalized, expected_parts=expected_parts, label="retry_metadata")
    return normalized


def _snapshot_target(
    *,
    snapshot_id: str | None,
    post_id: str | None,
    shortcode: str | None,
    account_handle: str | None,
    target_metadata: Mapping[str, Any],
) -> dict[str, Any]:
    target: dict[str, Any] = {}
    if snapshot_id:
        target["snapshot_id"] = snapshot_id
    if post_id:
        target["post_id"] = post_id
    if shortcode:
        target["shortcode"] = shortcode
    if account_handle:
        target["account_handle"] = account_handle
    if target_metadata:
        target["metadata"] = dict(target_metadata)
    return target


def _part_metadata(*, missing_evidence: Any) -> dict[str, Any]:
    metadata = _metadata_from_value(missing_evidence)
    return {"missing_evidence": metadata} if metadata else {}


def _metadata_from_value(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, SnapshotSourceUnavailableEvidence | SnapshotRetryMetadata):
        return value.to_metadata()
    if isinstance(value, Mapping):
        return {
            str(key): _metadata_datetime(item) if isinstance(item, datetime) else item
            for key, item in value.items()
            if item is not None
        }
    if isinstance(value, str):
        stripped = value.strip()
        return {"reason": stripped} if stripped else {}
    return {"value": value}


def _reason_from_evidence(value: Any) -> str | None:
    metadata = _metadata_from_value(value)
    reason = _string_value(metadata.get("reason"))
    if reason:
        return reason
    error_code = _string_value(metadata.get("error_code"))
    return error_code or None


def _has_source_unavailable_evidence(value: Mapping[str, Any]) -> bool:
    return any(_evidence_value_present(item) for item in value.values())


def _evidence_value_present(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, Mapping | list | tuple | set):
        return bool(value)
    return True


def _metadata_datetime(value: datetime | str) -> str:
    return value.isoformat() if isinstance(value, datetime) else value


def _normalize_part_name(value: Any) -> str:
    return _string_value(value).lower()


def _string_value(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def _optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _validate_unique_parts(parts: tuple[str, ...]) -> None:
    duplicates = sorted({part for part in parts if parts.count(part) > 1})
    if duplicates:
        raise ValueError(f"Duplicate Instagram snapshot parts: {', '.join(duplicates)}")


def _validate_known_parts(parts: Mapping[str, Any], *, expected_parts: tuple[str, ...], label: str) -> None:
    unknown = sorted(set(parts) - set(expected_parts))
    if unknown:
        raise ValueError(f"Unknown Instagram snapshot {label}: {', '.join(unknown)}")
