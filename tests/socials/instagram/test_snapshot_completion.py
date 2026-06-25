from __future__ import annotations

import pytest

from trr_backend.socials.instagram.snapshot_completion import (
    AD_FLAGS_PART,
    AUTHOR_AVATAR_PART,
    BLOCKED_STATE,
    CANONICAL_POST_ROW_PART,
    CAPTURED_STATE,
    COLLABORATORS_PART,
    COMMENT_MEDIA_PART,
    COMMENTS_PART,
    DEFERRED_STATE,
    HOSTED_MEDIA_PART,
    INSTAGRAM_SNAPSHOT_PARTS,
    LOCATION_PART,
    MEDIA_ASSETS_PART,
    MUSIC_PART,
    POST_DETAIL_PART,
    REPLIES_PART,
    RETRYABLE_STATE,
    SOURCE_UNAVAILABLE_STATE,
    TAGS_PART,
    SnapshotRetryMetadata,
    SnapshotSourceUnavailableEvidence,
    build_snapshot_completion_summary,
)


def test_snapshot_part_names_cover_plan_contract() -> None:
    assert INSTAGRAM_SNAPSHOT_PARTS == (
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


def test_captured_parts_are_not_retried_and_missing_parts_get_targeted_retries() -> None:
    summary = build_snapshot_completion_summary(
        captured_parts={
            POST_DETAIL_PART: {"source": "graphql_permalink"},
            CANONICAL_POST_ROW_PART: {"row_id": "post-1"},
            COMMENTS_PART: {"count": 25},
        },
        missing_parts={
            HOSTED_MEDIA_PART: {"reason": "mirror_not_complete"},
            COMMENT_MEDIA_PART: {"reason": "comment_media_missing"},
        },
        retry_metadata={
            HOSTED_MEDIA_PART: SnapshotRetryMetadata(
                reason="mirror_pending",
                attempt_count=2,
                max_attempts=5,
                priority=10,
                metadata={"lane": "media_mirror"},
            ),
            COMMENTS_PART: {"reason": "should_not_retry_captured"},
        },
        expected_parts=(
            POST_DETAIL_PART,
            CANONICAL_POST_ROW_PART,
            COMMENTS_PART,
            HOSTED_MEDIA_PART,
            COMMENT_MEDIA_PART,
        ),
        shortcode="ABC123",
        account_handle="bravotv",
    )

    assert summary.complete is False
    assert summary.part_states[POST_DETAIL_PART] == CAPTURED_STATE
    assert summary.part_states[COMMENTS_PART] == CAPTURED_STATE
    assert summary.part_states[HOSTED_MEDIA_PART] == RETRYABLE_STATE
    assert summary.part_states[COMMENT_MEDIA_PART] == RETRYABLE_STATE
    assert [target.part for target in summary.retry_targets] == [HOSTED_MEDIA_PART, COMMENT_MEDIA_PART]

    metadata = summary.to_metadata()
    hosted_retry = metadata["parts"][HOSTED_MEDIA_PART]["retry"]
    assert hosted_retry["retry_key"] == "instagram_snapshot:ABC123:hosted_media"
    assert hosted_retry["reason"] == "mirror_pending"
    assert hosted_retry["attempt_count"] == 2
    assert hosted_retry["metadata"] == {"lane": "media_mirror"}
    assert COMMENTS_PART not in metadata["retryable_parts"]


def test_source_unavailable_requires_non_empty_evidence() -> None:
    summary = build_snapshot_completion_summary(
        captured_parts=(),
        source_unavailable_evidence={
            MUSIC_PART: SnapshotSourceUnavailableEvidence(
                reason="no_music_attached",
                source="permalink_payload",
            ),
            LOCATION_PART: {},
        },
        expected_parts=(MUSIC_PART, LOCATION_PART),
        shortcode="NOLOC1",
    )

    assert summary.part_states[MUSIC_PART] == SOURCE_UNAVAILABLE_STATE
    assert summary.part_states[LOCATION_PART] == RETRYABLE_STATE
    assert [target.part for target in summary.retry_targets] == [LOCATION_PART]

    metadata = summary.to_metadata()
    assert metadata["parts"][MUSIC_PART]["evidence"] == {
        "reason": "no_music_attached",
        "source": "permalink_payload",
    }
    assert MUSIC_PART not in metadata["retryable_parts"]


def test_blocked_and_deferred_parts_are_not_retry_targets() -> None:
    summary = build_snapshot_completion_summary(
        captured_parts={POST_DETAIL_PART: {"source": "permalink"}},
        blocked_parts={COMMENTS_PART: {"reason": "auth_checkpoint", "error_code": "instagram_checkpoint"}},
        deferred_parts={HOSTED_MEDIA_PART: "media_worker_budget_paused"},
        expected_parts=(POST_DETAIL_PART, COMMENTS_PART, HOSTED_MEDIA_PART, REPLIES_PART),
        retry_metadata={REPLIES_PART: {"reason": "reply_count_gap", "retry_key": "custom-reply-retry"}},
    )

    assert summary.part_states[POST_DETAIL_PART] == CAPTURED_STATE
    assert summary.part_states[COMMENTS_PART] == BLOCKED_STATE
    assert summary.part_states[HOSTED_MEDIA_PART] == DEFERRED_STATE
    assert summary.part_states[REPLIES_PART] == RETRYABLE_STATE
    assert [target.to_metadata()["retry_key"] for target in summary.retry_targets] == ["custom-reply-retry"]

    metadata = summary.to_metadata()
    assert metadata["blocked_parts"] == [COMMENTS_PART]
    assert metadata["deferred_parts"] == [HOSTED_MEDIA_PART]
    assert metadata["retryable_parts"] == [REPLIES_PART]


def test_summary_is_complete_when_all_parts_are_captured_or_source_unavailable() -> None:
    summary = build_snapshot_completion_summary(
        captured_parts=(POST_DETAIL_PART, CANONICAL_POST_ROW_PART),
        source_unavailable_evidence={AD_FLAGS_PART: "ad_flags_absent_from_source"},
        expected_parts=(POST_DETAIL_PART, CANONICAL_POST_ROW_PART, AD_FLAGS_PART),
    )

    assert summary.complete is True
    assert summary.retry_targets == ()
    assert summary.to_metadata()["state"] == "complete"


def test_unknown_part_names_fail_fast() -> None:
    with pytest.raises(ValueError, match="Unknown Instagram snapshot captured_parts"):
        build_snapshot_completion_summary(captured_parts=("unknown_part",))
