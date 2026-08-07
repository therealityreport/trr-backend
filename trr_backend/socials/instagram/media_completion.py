"""Pure Instagram media completion gates.

This module intentionally has no database or queue side effects. Callers pass
the current snapshot rows and optional stale queue evidence, then receive a
progress-compatible payload with media-specific gaps and retry targets.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, cast

CAPTURED = "captured"
RETRYABLE = "retryable"
SOURCE_UNAVAILABLE = "source_unavailable"
BLOCKED = "blocked"
DEFERRED = "deferred"

MEDIA_MIRROR_STAGE = "media_mirror"
COMMENT_MEDIA_MIRROR_STAGE = "comment_media_mirror"

_COMPLETE_MIRROR_STATUSES = {"complete", "completed", "mirrored", "up_to_date"}
_RETRYABLE_MIRROR_STATUSES = {"failed", "partial", "pending", "queued", "retrying", "running"}
_DEFERRED_MIRROR_STATUSES = {"deferred"}
_UNAVAILABLE_MIRROR_STATUSES = {"source_unavailable", "unavailable", "unrecoverable"}
_TERMINAL_SOURCE_UNAVAILABLE_REASONS = {
    "asset_too_large",
    "empty_response_body",
    "http_403_auth_or_expired",
    "http_404",
    "http_404_not_found",
    "invalid_source_url",
    "media_deleted",
    "not_served_by_source",
    "private_or_removed",
    "source_deleted",
    "source_not_available",
    "source_not_served",
    "source_removed",
    "source_unavailable",
    "unavailable",
    "unrecoverable",
    "unsupported_media",
}
_RETRY_EXHAUSTION_REASONS = {
    "attempts_exhausted",
    "max_attempts_exceeded",
    "max_retries_exhausted",
    "retry_budget_exhausted",
    "retry_exhausted",
    "retries_exhausted",
}


@dataclass(frozen=True, slots=True)
class HostedMediaEvidence:
    """Post-level source and hosted media evidence for one snapshot/post."""

    post_id: str | None = None
    source_id: str | None = None
    shortcode: str | None = None
    source_media_urls: Sequence[str] = field(default_factory=tuple)
    hosted_media_urls: Sequence[str] = field(default_factory=tuple)
    source_thumbnail_url: str | None = None
    hosted_thumbnail_url: str | None = None
    media_mirror_status: str | None = None
    media_mirror_error: str | None = None
    source_unavailable_evidence: Any = None

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> HostedMediaEvidence:
        return cls(
            post_id=_first_text(row, "post_id", "id"),
            source_id=_first_text(row, "source_id", "source_shortcode", "shortcode"),
            shortcode=_first_text(row, "shortcode", "source_id", "source_shortcode"),
            source_media_urls=_as_text_list(row.get("source_media_urls")) or _as_text_list(row.get("media_urls")),
            hosted_media_urls=_as_text_list(row.get("hosted_media_urls")),
            source_thumbnail_url=_first_text(row, "source_thumbnail_url", "thumbnail_url"),
            hosted_thumbnail_url=_first_text(row, "hosted_thumbnail_url"),
            media_mirror_status=_first_text(row, "media_mirror_status"),
            media_mirror_error=_first_text(row, "media_mirror_error", "last_error_code", "error_message"),
            source_unavailable_evidence=_first_present(
                row,
                "source_unavailable_evidence",
                "media_source_unavailable_evidence",
                "source_unavailable",
            ),
        )


@dataclass(frozen=True, slots=True)
class AuthorAvatarEvidence:
    """Author avatar source and hosted mirror evidence."""

    avatar_id: str | None = None
    post_id: str | None = None
    comment_id: str | None = None
    username: str | None = None
    source_avatar_url: str | None = None
    hosted_avatar_url: str | None = None
    media_mirror_status: str | None = None
    media_mirror_error: str | None = None
    source_unavailable_evidence: Any = None

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> AuthorAvatarEvidence:
        return cls(
            avatar_id=_first_text(row, "avatar_id", "author_profile_pic_id", "owner_profile_pic_id", "user_id"),
            post_id=_first_text(row, "post_id", "id"),
            comment_id=_first_text(row, "comment_id"),
            username=_first_text(row, "username", "author", "owner_username"),
            source_avatar_url=_first_text(
                row,
                "source_avatar_url",
                "author_profile_pic_url_hd",
                "author_profile_pic_url",
                "owner_profile_pic_url_hd",
                "owner_profile_pic_url",
                "user_avatar_url",
                "profile_pic_url_hd",
                "profile_pic_url",
            ),
            hosted_avatar_url=_first_text(
                row,
                "hosted_avatar_url",
                "hosted_author_profile_pic_url",
                "hosted_owner_profile_pic_url",
                "hosted_user_avatar_url",
                "hosted_profile_pic_url",
            ),
            media_mirror_status=_first_text(row, "avatar_mirror_status", "media_mirror_status"),
            media_mirror_error=_first_text(row, "avatar_mirror_error", "media_mirror_error"),
            source_unavailable_evidence=_first_present(
                row,
                "avatar_source_unavailable_evidence",
                "source_unavailable_evidence",
                "source_unavailable",
            ),
        )


@dataclass(frozen=True, slots=True)
class CommentMediaEvidence:
    """Comment media source and hosted mirror evidence for one comment/reply."""

    comment_id: str | None = None
    post_id: str | None = None
    shortcode: str | None = None
    is_reply: bool = False
    source_media_urls: Sequence[str] = field(default_factory=tuple)
    hosted_media_urls: Sequence[str] = field(default_factory=tuple)
    media_mirror_status: str | None = None
    media_mirror_error: str | None = None
    source_unavailable_evidence: Any = None

    @classmethod
    def from_row(cls, row: Mapping[str, Any]) -> CommentMediaEvidence:
        return cls(
            comment_id=_first_text(row, "comment_id", "external_id", "id"),
            post_id=_first_text(row, "post_id"),
            shortcode=_first_text(row, "shortcode", "post_source_id", "source_id"),
            is_reply=bool(row.get("is_reply")),
            source_media_urls=_as_text_list(row.get("source_media_urls")) or _as_text_list(row.get("media_urls")),
            hosted_media_urls=_as_text_list(row.get("hosted_media_urls")),
            media_mirror_status=_first_text(row, "media_mirror_status"),
            media_mirror_error=_first_text(row, "media_mirror_error", "last_error_code", "error_message"),
            source_unavailable_evidence=_first_present(
                row,
                "source_unavailable_evidence",
                "comment_media_source_unavailable_evidence",
                "source_unavailable",
            ),
        )


@dataclass(frozen=True, slots=True)
class MediaGateResult:
    gate: str
    state: str
    status: str
    completed: bool
    source_count: int
    hosted_count: int
    missing_count: int = 0
    source_unavailable_count: int = 0
    blocked_count: int = 0
    deferred_count: int = 0
    gaps: tuple[dict[str, Any], ...] = field(default_factory=tuple)
    retry_targets: tuple[dict[str, Any], ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "gate": self.gate,
            "state": self.state,
            "status": self.status,
            "completed": self.completed,
            "source_count": self.source_count,
            "hosted_count": self.hosted_count,
            "missing_count": self.missing_count,
            "source_unavailable_count": self.source_unavailable_count,
            "blocked_count": self.blocked_count,
            "deferred_count": self.deferred_count,
            "gaps": [dict(gap) for gap in self.gaps],
            "retry_targets": [dict(target) for target in self.retry_targets],
        }


def classify_hosted_media_gate(
    evidence: HostedMediaEvidence | Mapping[str, Any] | None,
    *,
    stale_media_claims: Mapping[str, Any] | Sequence[Mapping[str, Any]] | None = None,
) -> MediaGateResult:
    media = _coerce_hosted_media_evidence(evidence)
    if media is None:
        return _empty_gate("hosted_media")

    source_media_urls = _as_text_list(media.source_media_urls)
    hosted_media_urls = _as_text_list(media.hosted_media_urls)
    source_thumbnail_url = _text_or_none(media.source_thumbnail_url)
    hosted_thumbnail_url = _text_or_none(media.hosted_thumbnail_url)
    source_media_count = len(source_media_urls)
    hosted_media_count = min(len(hosted_media_urls), source_media_count)
    source_count = source_media_count + int(bool(source_thumbnail_url))
    hosted_count = hosted_media_count + int(bool(hosted_thumbnail_url))
    target_identity = _target_identity(
        post_id=media.post_id,
        source_id=media.source_id or media.shortcode,
        shortcode=media.shortcode,
    )
    return _classify_gate_counts(
        gate="hosted_media",
        target_type="hosted_media",
        retry_stage=MEDIA_MIRROR_STAGE,
        source_count=source_count,
        hosted_count=hosted_count,
        mirror_status=media.media_mirror_status,
        mirror_error=media.media_mirror_error,
        source_unavailable_evidence=media.source_unavailable_evidence,
        stale_media_claims=stale_media_claims,
        target_identity=target_identity,
    )


def classify_author_avatar_gate(
    evidence: AuthorAvatarEvidence | Mapping[str, Any] | Iterable[AuthorAvatarEvidence | Mapping[str, Any]] | None,
    *,
    stale_media_claims: Mapping[str, Any] | Sequence[Mapping[str, Any]] | None = None,
) -> MediaGateResult:
    avatars = _coerce_avatar_evidence_list(evidence)
    if not avatars:
        return _empty_gate("author_avatar")

    source_count = 0
    hosted_count = 0
    missing_targets: list[dict[str, Any]] = []
    unavailable_count = 0
    deferred_count = 0
    for avatar in avatars:
        source_url = _text_or_none(avatar.source_avatar_url)
        hosted_url = _text_or_none(avatar.hosted_avatar_url)
        status = _normalize_status(avatar.media_mirror_status)
        if not source_url:
            continue
        source_count += 1
        if hosted_url and status not in _RETRYABLE_MIRROR_STATUSES and status not in _DEFERRED_MIRROR_STATUSES:
            hosted_count += 1
            continue
        target = _target_identity(
            post_id=avatar.post_id,
            comment_id=avatar.comment_id,
            username=avatar.username,
            avatar_id=avatar.avatar_id,
        )
        if _has_source_unavailable_evidence(
            avatar.source_unavailable_evidence,
            {"status": avatar.media_mirror_status, "reason": avatar.media_mirror_error},
        ):
            unavailable_count += 1
            missing_targets.append(
                _gap(
                    "author_avatar",
                    "author_avatar",
                    SOURCE_UNAVAILABLE,
                    "source_unavailable",
                    MEDIA_MIRROR_STAGE,
                    1,
                    target,
                    retryable=False,
                    terminal=True,
                )
            )
            continue
        if status in _DEFERRED_MIRROR_STATUSES:
            deferred_count += 1
            missing_targets.append(
                _gap(
                    "author_avatar",
                    "author_avatar",
                    DEFERRED,
                    "mirror_deferred",
                    MEDIA_MIRROR_STAGE,
                    1,
                    target,
                    retryable=False,
                    terminal=False,
                )
            )
            continue
        missing_targets.append(
            _gap(
                "author_avatar",
                "author_avatar",
                RETRYABLE,
                "missing_hosted_avatar",
                MEDIA_MIRROR_STAGE,
                1,
                target,
            )
        )

    return _finalize_aggregate_gate(
        gate="author_avatar",
        retry_stage=MEDIA_MIRROR_STAGE,
        source_count=source_count,
        hosted_count=hosted_count,
        source_unavailable_count=unavailable_count,
        deferred_count=deferred_count,
        gaps=missing_targets,
        stale_media_claims=stale_media_claims,
    )


def classify_comment_media_gate(
    evidence: Iterable[CommentMediaEvidence | Mapping[str, Any]] | None,
    *,
    stale_media_claims: Mapping[str, Any] | Sequence[Mapping[str, Any]] | None = None,
) -> MediaGateResult:
    comments = _coerce_comment_media_evidence_list(evidence)
    if not comments:
        return _empty_gate("comment_media")

    source_count = 0
    hosted_count = 0
    unavailable_count = 0
    deferred_count = 0
    gaps: list[dict[str, Any]] = []
    for comment in comments:
        source_urls = _as_text_list(comment.source_media_urls)
        hosted_urls = _as_text_list(comment.hosted_media_urls)
        comment_source_count = len(source_urls)
        if comment_source_count <= 0:
            continue
        comment_hosted_count = min(len(hosted_urls), comment_source_count)
        source_count += comment_source_count
        status = _normalize_status(comment.media_mirror_status)
        status_blocks_completion = status in _RETRYABLE_MIRROR_STATUSES or status in _DEFERRED_MIRROR_STATUSES
        if comment_hosted_count >= comment_source_count and not status_blocks_completion:
            hosted_count += comment_source_count
            continue
        hosted_count += comment_hosted_count
        missing_count = max(comment_source_count - comment_hosted_count, 0)
        if missing_count <= 0 and status_blocks_completion:
            missing_count = comment_source_count
        target = _target_identity(
            post_id=comment.post_id,
            comment_id=comment.comment_id,
            source_id=comment.shortcode,
            shortcode=comment.shortcode,
            is_reply=comment.is_reply,
        )
        if _has_source_unavailable_evidence(
            comment.source_unavailable_evidence,
            {"status": comment.media_mirror_status, "reason": comment.media_mirror_error},
        ):
            unavailable_count += missing_count
            gaps.append(
                _gap(
                    "comment_media",
                    "comment_media",
                    SOURCE_UNAVAILABLE,
                    "source_unavailable",
                    COMMENT_MEDIA_MIRROR_STAGE,
                    missing_count,
                    target,
                    retryable=False,
                    terminal=True,
                )
            )
            continue
        if status in _DEFERRED_MIRROR_STATUSES:
            deferred_count += missing_count
            gaps.append(
                _gap(
                    "comment_media",
                    "comment_media",
                    DEFERRED,
                    "mirror_deferred",
                    COMMENT_MEDIA_MIRROR_STAGE,
                    missing_count,
                    target,
                    retryable=False,
                    terminal=False,
                )
            )
            continue
        gaps.append(
            _gap(
                "comment_media",
                "comment_media",
                RETRYABLE,
                "missing_hosted_comment_media",
                COMMENT_MEDIA_MIRROR_STAGE,
                missing_count,
                target,
            )
        )

    return _finalize_aggregate_gate(
        gate="comment_media",
        retry_stage=COMMENT_MEDIA_MIRROR_STAGE,
        source_count=source_count,
        hosted_count=hosted_count,
        source_unavailable_count=unavailable_count,
        deferred_count=deferred_count,
        gaps=gaps,
        stale_media_claims=stale_media_claims,
    )


def build_media_completion_payload(
    *,
    hosted_media: HostedMediaEvidence | Mapping[str, Any] | None = None,
    author_avatars: AuthorAvatarEvidence
    | Mapping[str, Any]
    | Iterable[AuthorAvatarEvidence | Mapping[str, Any]]
    | None = None,
    comment_media: Iterable[CommentMediaEvidence | Mapping[str, Any]] | None = None,
    stale_media_claims: Mapping[str, Any] | Sequence[Mapping[str, Any]] | None = None,
    comment_text_reply_retry_targets: Iterable[Mapping[str, Any] | str] | None = None,
    snapshot_id: str | None = None,
    source_id: str | None = None,
) -> dict[str, Any]:
    """Build the benchmark/progress payload for all media completion gates."""

    normalized_stale_claims = _normalize_stale_media_claims(stale_media_claims)
    hosted_media_gate = classify_hosted_media_gate(hosted_media, stale_media_claims=normalized_stale_claims)
    avatar_gate = classify_author_avatar_gate(author_avatars, stale_media_claims=normalized_stale_claims)
    comment_media_gate = classify_comment_media_gate(comment_media, stale_media_claims=normalized_stale_claims)
    gates = (hosted_media_gate, avatar_gate, comment_media_gate)
    gaps = [gap for gate in gates for gap in gate.gaps]
    media_mirror_retry_targets = [target for gate in (hosted_media_gate, avatar_gate) for target in gate.retry_targets]
    comment_media_mirror_retry_targets = list(comment_media_gate.retry_targets)
    normalized_comment_text_reply_targets = _normalize_comment_text_reply_retry_targets(
        comment_text_reply_retry_targets
    )

    blocked = any(gate.state == BLOCKED for gate in gates)
    retryable = bool(media_mirror_retry_targets or comment_media_mirror_retry_targets)
    deferred = any(gate.state == DEFERRED for gate in gates)
    source_unavailable = any(gate.source_unavailable_count > 0 for gate in gates)
    completed = not blocked and not retryable and not deferred
    if blocked:
        status = BLOCKED
    elif retryable or deferred:
        status = "partial"
    elif source_unavailable:
        status = SOURCE_UNAVAILABLE
    else:
        status = "complete"

    source_count = sum(gate.source_count for gate in gates)
    hosted_count = sum(gate.hosted_count for gate in gates)
    missing_count = sum(gate.missing_count for gate in gates)
    payload: dict[str, Any] = {
        "completed": completed,
        "status": status,
        "snapshot_id": _text_or_none(snapshot_id),
        "source_id": _text_or_none(source_id),
        "summary": {
            "source_count": source_count,
            "hosted_count": hosted_count,
            "missing_count": missing_count,
            "source_unavailable_count": sum(gate.source_unavailable_count for gate in gates),
            "blocked_count": sum(gate.blocked_count for gate in gates),
            "deferred_count": sum(gate.deferred_count for gate in gates),
            "gap_count": len(gaps),
            "media_mirror_retry_target_count": len(media_mirror_retry_targets),
            "comment_media_mirror_retry_target_count": len(comment_media_mirror_retry_targets),
            "comment_text_reply_retry_target_count": len(normalized_comment_text_reply_targets),
        },
        "gates": {
            "hosted_media": hosted_media_gate.to_dict(),
            "author_avatar": avatar_gate.to_dict(),
            "comment_media": comment_media_gate.to_dict(),
        },
        "gaps": gaps,
        "media_mirror_status": _progress_status_from_gates((hosted_media_gate, avatar_gate)),
        "comment_media_mirror_status": _progress_status_from_gates((comment_media_gate,)),
        "author_avatar_status": avatar_gate.to_dict(),
        "media_mirror_retry_targets": media_mirror_retry_targets,
        "comment_media_mirror_retry_targets": comment_media_mirror_retry_targets,
        "comment_text_reply_retry_targets": normalized_comment_text_reply_targets,
        "retry_targets": {
            "media_mirror": media_mirror_retry_targets,
            "comment_media_mirror": comment_media_mirror_retry_targets,
            "comment_text_reply": normalized_comment_text_reply_targets,
        },
        "stale_media_claims": normalized_stale_claims,
    }
    return {key: value for key, value in payload.items() if value is not None}


def _classify_gate_counts(
    *,
    gate: str,
    target_type: str,
    retry_stage: str,
    source_count: int,
    hosted_count: int,
    mirror_status: str | None,
    mirror_error: str | None,
    source_unavailable_evidence: Any,
    stale_media_claims: Mapping[str, Any] | Sequence[Mapping[str, Any]] | None,
    target_identity: Mapping[str, Any],
) -> MediaGateResult:
    normalized_stale_claims = _normalize_stale_media_claims(stale_media_claims)
    stale_count = _stale_count_for_stage(normalized_stale_claims, retry_stage)
    normalized_source_count = max(0, int(source_count))
    normalized_hosted_count = min(max(0, int(hosted_count)), normalized_source_count)
    missing_count = max(normalized_source_count - normalized_hosted_count, 0)
    status = _normalize_status(mirror_status)

    if stale_count > 0:
        stale_gap = _gap(
            gate,
            target_type,
            BLOCKED,
            "stale_media_queue_claims",
            retry_stage,
            stale_count,
            target_identity,
            retryable=False,
            terminal=False,
        )
        stale_gap["stale_claim_count"] = stale_count
        return MediaGateResult(
            gate=gate,
            state=BLOCKED,
            status=BLOCKED,
            completed=False,
            source_count=normalized_source_count,
            hosted_count=normalized_hosted_count,
            missing_count=missing_count,
            blocked_count=stale_count,
            gaps=(stale_gap,),
        )

    if normalized_source_count <= 0:
        return MediaGateResult(
            gate=gate,
            state=CAPTURED,
            status="not_needed",
            completed=True,
            source_count=0,
            hosted_count=0,
        )

    if (
        normalized_hosted_count >= normalized_source_count
        and status not in _RETRYABLE_MIRROR_STATUSES
        and status not in _DEFERRED_MIRROR_STATUSES
    ):
        return MediaGateResult(
            gate=gate,
            state=CAPTURED,
            status="complete",
            completed=True,
            source_count=normalized_source_count,
            hosted_count=normalized_hosted_count,
        )

    effective_missing_count = missing_count if missing_count > 0 else normalized_source_count
    evidence = source_unavailable_evidence
    if _has_source_unavailable_evidence(evidence, {"status": mirror_status, "reason": mirror_error}):
        source_unavailable_gap = _gap(
            gate,
            target_type,
            SOURCE_UNAVAILABLE,
            "source_unavailable",
            retry_stage,
            effective_missing_count,
            target_identity,
            retryable=False,
            terminal=True,
        )
        return MediaGateResult(
            gate=gate,
            state=SOURCE_UNAVAILABLE,
            status=SOURCE_UNAVAILABLE,
            completed=True,
            source_count=normalized_source_count,
            hosted_count=normalized_hosted_count,
            missing_count=missing_count,
            source_unavailable_count=effective_missing_count,
            gaps=(source_unavailable_gap,),
        )

    if status in _DEFERRED_MIRROR_STATUSES:
        deferred_gap = _gap(
            gate,
            target_type,
            DEFERRED,
            "mirror_deferred",
            retry_stage,
            effective_missing_count,
            target_identity,
            retryable=False,
            terminal=False,
        )
        return MediaGateResult(
            gate=gate,
            state=DEFERRED,
            status=DEFERRED,
            completed=False,
            source_count=normalized_source_count,
            hosted_count=normalized_hosted_count,
            missing_count=missing_count,
            deferred_count=effective_missing_count,
            gaps=(deferred_gap,),
        )

    retry_gap = _gap(
        gate,
        target_type,
        RETRYABLE,
        _missing_reason_for_gate(gate),
        retry_stage,
        effective_missing_count,
        target_identity,
    )
    return MediaGateResult(
        gate=gate,
        state=RETRYABLE,
        status="partial",
        completed=False,
        source_count=normalized_source_count,
        hosted_count=normalized_hosted_count,
        missing_count=missing_count,
        gaps=(retry_gap,),
        retry_targets=(_retry_target_from_gap(retry_gap),),
    )


def _finalize_aggregate_gate(
    *,
    gate: str,
    retry_stage: str,
    source_count: int,
    hosted_count: int,
    source_unavailable_count: int,
    deferred_count: int,
    gaps: list[dict[str, Any]],
    stale_media_claims: Mapping[str, Any] | Sequence[Mapping[str, Any]] | None,
) -> MediaGateResult:
    normalized_stale_claims = _normalize_stale_media_claims(stale_media_claims)
    stale_count = _stale_count_for_stage(normalized_stale_claims, retry_stage)
    missing_count = max(0, source_count - hosted_count - source_unavailable_count)
    if stale_count > 0:
        stale_gap = _gap(
            gate,
            gate,
            BLOCKED,
            "stale_media_queue_claims",
            retry_stage,
            stale_count,
            {},
            retryable=False,
            terminal=False,
        )
        stale_gap["stale_claim_count"] = stale_count
        return MediaGateResult(
            gate=gate,
            state=BLOCKED,
            status=BLOCKED,
            completed=False,
            source_count=source_count,
            hosted_count=hosted_count,
            missing_count=missing_count,
            source_unavailable_count=source_unavailable_count,
            blocked_count=stale_count,
            deferred_count=deferred_count,
            gaps=(*gaps, stale_gap),
        )

    retryable_gaps = [gap for gap in gaps if gap.get("state") == RETRYABLE and gap.get("retryable")]
    if retryable_gaps:
        state = RETRYABLE
        status = "partial"
        completed = False
    elif deferred_count > 0:
        state = DEFERRED
        status = DEFERRED
        completed = False
    elif source_unavailable_count > 0:
        state = SOURCE_UNAVAILABLE
        status = SOURCE_UNAVAILABLE
        completed = True
    else:
        state = CAPTURED
        status = "complete" if source_count > 0 else "not_needed"
        completed = True
    return MediaGateResult(
        gate=gate,
        state=state,
        status=status,
        completed=completed,
        source_count=source_count,
        hosted_count=hosted_count,
        missing_count=missing_count,
        source_unavailable_count=source_unavailable_count,
        deferred_count=deferred_count,
        gaps=tuple(gaps),
        retry_targets=tuple(_retry_target_from_gap(gap) for gap in retryable_gaps),
    )


def _progress_status_from_gates(gates: Sequence[MediaGateResult]) -> dict[str, Any]:
    source_count = sum(gate.source_count for gate in gates)
    hosted_count = sum(gate.hosted_count for gate in gates)
    failed_count = sum(gate.missing_count for gate in gates if gate.state == RETRYABLE)
    pending_count = sum(gate.blocked_count + gate.deferred_count for gate in gates)
    partial_count = sum(gate.missing_count for gate in gates if gate.state in {RETRYABLE, DEFERRED, BLOCKED})
    if any(gate.state == BLOCKED for gate in gates):
        status = BLOCKED
    elif any(gate.state == DEFERRED for gate in gates):
        status = DEFERRED
    elif any(gate.state == RETRYABLE for gate in gates):
        status = "partial"
    elif any(gate.state == SOURCE_UNAVAILABLE for gate in gates):
        status = SOURCE_UNAVAILABLE
    elif source_count <= 0:
        status = "not_needed"
    elif hosted_count >= source_count:
        status = "complete"
    else:
        status = "partial"
    return {
        "status": status,
        "source_count": source_count,
        "mirrored_count": min(hosted_count, source_count),
        "hosted_count": min(hosted_count, source_count),
        "failed_count": failed_count,
        "pending_count": pending_count,
        "partial_count": partial_count,
        "source_unavailable_count": sum(gate.source_unavailable_count for gate in gates),
        "blocked_count": sum(gate.blocked_count for gate in gates),
        "failure_reason": _first_gap_reason(gates),
    }


def _first_gap_reason(gates: Sequence[MediaGateResult]) -> str | None:
    for gate in gates:
        for gap in gate.gaps:
            reason = _text_or_none(gap.get("reason"))
            if reason:
                return reason
    return None


def _empty_gate(gate: str) -> MediaGateResult:
    return MediaGateResult(
        gate=gate,
        state=CAPTURED,
        status="not_needed",
        completed=True,
        source_count=0,
        hosted_count=0,
    )


def _gap(
    gate: str,
    target_type: str,
    state: str,
    reason: str,
    retry_stage: str,
    missing_count: int,
    target_identity: Mapping[str, Any],
    *,
    retryable: bool = True,
    terminal: bool = False,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "gate": gate,
        "target_type": target_type,
        "state": state,
        "reason": reason,
        "retry_stage": retry_stage,
        "missing_count": max(0, int(missing_count)),
        "retryable": bool(retryable),
        "terminal": bool(terminal),
    }
    payload.update({key: value for key, value in target_identity.items() if value is not None})
    return payload


def _retry_target_from_gap(gap: Mapping[str, Any]) -> dict[str, Any]:
    keep_keys = (
        "retry_stage",
        "target_type",
        "reason",
        "missing_count",
        "post_id",
        "comment_id",
        "source_id",
        "shortcode",
        "username",
        "avatar_id",
        "is_reply",
    )
    target = {key: gap.get(key) for key in keep_keys if gap.get(key) is not None}
    target["stage"] = target.pop("retry_stage")
    return target


def _target_identity(**values: Any) -> dict[str, Any]:
    return {key: _text_or_none(value) if key != "is_reply" else bool(value) for key, value in values.items()}


def _missing_reason_for_gate(gate: str) -> str:
    if gate == "hosted_media":
        return "missing_hosted_media"
    if gate == "comment_media":
        return "missing_hosted_comment_media"
    if gate == "author_avatar":
        return "missing_hosted_avatar"
    return "missing_hosted_asset"


def _coerce_hosted_media_evidence(
    evidence: HostedMediaEvidence | Mapping[str, Any] | None,
) -> HostedMediaEvidence | None:
    if evidence is None:
        return None
    if isinstance(evidence, HostedMediaEvidence):
        return evidence
    if isinstance(evidence, Mapping):
        return HostedMediaEvidence.from_row(evidence)
    raise TypeError(f"Unsupported hosted media evidence: {type(evidence)!r}")


def _coerce_avatar_evidence_list(
    evidence: AuthorAvatarEvidence | Mapping[str, Any] | Iterable[AuthorAvatarEvidence | Mapping[str, Any]] | None,
) -> list[AuthorAvatarEvidence]:
    if evidence is None:
        return []
    if isinstance(evidence, AuthorAvatarEvidence):
        return [evidence]
    if isinstance(evidence, Mapping):
        return [AuthorAvatarEvidence.from_row(cast("Mapping[str, Any]", evidence))]
    avatars: list[AuthorAvatarEvidence] = []
    for item in evidence:
        if isinstance(item, AuthorAvatarEvidence):
            avatars.append(item)
        elif isinstance(item, Mapping):
            avatars.append(AuthorAvatarEvidence.from_row(item))
        else:
            raise TypeError(f"Unsupported avatar evidence: {type(item)!r}")
    return avatars


def _coerce_comment_media_evidence_list(
    evidence: Iterable[CommentMediaEvidence | Mapping[str, Any]] | None,
) -> list[CommentMediaEvidence]:
    if evidence is None:
        return []
    comments: list[CommentMediaEvidence] = []
    for item in evidence:
        if isinstance(item, CommentMediaEvidence):
            comments.append(item)
        elif isinstance(item, Mapping):
            comments.append(CommentMediaEvidence.from_row(item))
        else:
            raise TypeError(f"Unsupported comment media evidence: {type(item)!r}")
    return comments


def _normalize_comment_text_reply_retry_targets(
    targets: Iterable[Mapping[str, Any] | str] | None,
) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    seen: set[tuple[tuple[str, str], ...]] = set()
    for item in targets or []:
        if isinstance(item, Mapping):
            candidate = {
                str(key): value for key, value in item.items() if value is not None and str(key).strip() and value != ""
            }
        else:
            candidate = {"source_id": str(item or "").strip()}
        if not candidate.get("target_type"):
            candidate["target_type"] = "comment_text_reply"
        normalized_items = tuple(sorted((key, str(value)) for key, value in candidate.items()))
        if not normalized_items or normalized_items in seen:
            continue
        seen.add(normalized_items)
        normalized.append(candidate)
    return normalized


def _normalize_stale_media_claims(
    claims: Mapping[str, Any] | Sequence[Mapping[str, Any]] | None,
) -> dict[str, Any]:
    if not claims:
        return {"total": 0, "by_stage": {}, "by_platform": {}}
    if isinstance(claims, Mapping):
        nested_claims = claims.get("stale_media_claims")
        if isinstance(nested_claims, Mapping):
            claims = nested_claims
    by_stage: dict[str, int] = {}
    by_platform: dict[str, int] = {}
    total = 0

    def add(stage: Any, platform: Any, count: Any) -> None:
        nonlocal total
        normalized_count = max(0, _safe_int(count))
        if normalized_count <= 0:
            return
        normalized_stage = _normalize_status(stage) or "unknown"
        by_stage[normalized_stage] = by_stage.get(normalized_stage, 0) + normalized_count
        normalized_platform = _normalize_status(platform)
        if normalized_platform:
            by_platform[normalized_platform] = by_platform.get(normalized_platform, 0) + normalized_count
        total += normalized_count

    if isinstance(claims, Mapping):
        raw_by_stage = claims.get("by_stage")
        raw_by_platform = claims.get("by_platform")
        if isinstance(raw_by_stage, Mapping):
            for stage, count in raw_by_stage.items():
                add(stage, None, count)
        if isinstance(raw_by_platform, Mapping):
            for platform, count in raw_by_platform.items():
                normalized_platform = _normalize_status(platform) or "unknown"
                by_platform[normalized_platform] = max(by_platform.get(normalized_platform, 0), _safe_int(count))
        if not by_stage:
            total = max(0, _safe_int(claims.get("total")), sum(by_platform.values()))
        else:
            total = max(total, _safe_int(claims.get("total")))
        return {"total": total, "by_stage": by_stage, "by_platform": by_platform}

    for item in claims:
        if not isinstance(item, Mapping):
            continue
        add(item.get("stage"), item.get("platform"), item.get("total") or item.get("count") or 1)
    return {"total": total, "by_stage": by_stage, "by_platform": by_platform}


def _stale_count_for_stage(claims: Mapping[str, Any] | None, stage: str) -> int:
    if not isinstance(claims, Mapping):
        return 0
    by_stage = claims.get("by_stage")
    if isinstance(by_stage, Mapping) and by_stage:
        return max(0, _safe_int(by_stage.get(stage)))
    return max(0, _safe_int(claims.get("total")))


def _has_source_unavailable_evidence(*evidence_values: Any) -> bool:
    for evidence in evidence_values:
        for item in _evidence_items(evidence):
            if _evidence_item_marks_source_unavailable(item):
                return True
    return False


def _evidence_items(value: Any) -> list[Any]:
    if value is None or value is False:
        return []
    if isinstance(value, Mapping):
        items: list[Any] = [value]
        for key in ("source_unavailable_evidence", "evidence", "details", "causes"):
            nested = value.get(key)
            if nested is not value:
                items.extend(_evidence_items(nested))
        return items
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return []
        try:
            parsed = json.loads(stripped)
        except json.JSONDecodeError:
            return [stripped]
        return _evidence_items(parsed)
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        items = []
        for item in value:
            items.extend(_evidence_items(item))
        return items
    return []


def _evidence_item_marks_source_unavailable(item: Any) -> bool:
    if isinstance(item, Mapping):
        reason_text = " ".join(
            str(item.get(key) or "")
            for key in ("reason", "failure_reason", "error_code", "code", "status", "error", "message")
        )
        status = _normalize_status(item.get("status"))
        source_unavailable_flag = bool(item.get("source_unavailable"))
        return _reason_marks_source_unavailable(reason_text) or (
            source_unavailable_flag and status not in _RETRY_EXHAUSTION_REASONS
        )
    return _reason_marks_source_unavailable(str(item or ""))


def _reason_marks_source_unavailable(reason: str) -> bool:
    normalized = _normalize_reason(reason)
    if not normalized:
        return False
    tokens = set(normalized.split(":")) | set(normalized.replace(";", ":").replace(",", ":").split(":"))
    tokens |= set(normalized.split())
    has_terminal_reason = any(
        reason in normalized or reason in tokens for reason in _TERMINAL_SOURCE_UNAVAILABLE_REASONS
    )
    if has_terminal_reason:
        return True
    has_retry_exhaustion_only = any(reason in normalized or reason in tokens for reason in _RETRY_EXHAUSTION_REASONS)
    return False if has_retry_exhaustion_only else normalized in _UNAVAILABLE_MIRROR_STATUSES


def _normalize_reason(value: Any) -> str:
    normalized = str(value or "").strip().lower().replace("-", "_")
    normalized = normalized.replace("download_failed:", "").replace("upload_failed:", "")
    normalized = normalized.replace("media[0]:", "").replace("media:", "").replace("thumbnail:", "")
    return normalized


def _normalize_status(value: Any) -> str:
    return str(value or "").strip().lower().replace("-", "_")


def _as_text_list(value: Any) -> list[str]:
    items: list[Any]
    if value is None:
        return []
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return []
        try:
            parsed = json.loads(stripped)
        except json.JSONDecodeError:
            items = [stripped]
        else:
            items = parsed if isinstance(parsed, list) else [stripped]
    elif isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        items = list(value)
    else:
        return []

    out: list[str] = []
    seen: set[str] = set()
    for item in items:
        text = str(item or "").strip()
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(text)
    return out


def _first_text(row: Mapping[str, Any], *keys: str) -> str | None:
    return _text_or_none(_first_present(row, *keys))


def _first_present(row: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row.get(key) is not None:
            return row.get(key)
    return None


def _text_or_none(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


__all__ = [
    "BLOCKED",
    "CAPTURED",
    "COMMENT_MEDIA_MIRROR_STAGE",
    "DEFERRED",
    "MEDIA_MIRROR_STAGE",
    "RETRYABLE",
    "SOURCE_UNAVAILABLE",
    "AuthorAvatarEvidence",
    "CommentMediaEvidence",
    "HostedMediaEvidence",
    "MediaGateResult",
    "build_media_completion_payload",
    "classify_author_avatar_gate",
    "classify_comment_media_gate",
    "classify_hosted_media_gate",
]
