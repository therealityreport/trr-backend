from __future__ import annotations

from trr_backend.socials.instagram.media_completion import (
    AuthorAvatarEvidence,
    CommentMediaEvidence,
    HostedMediaEvidence,
    build_media_completion_payload,
    classify_author_avatar_gate,
    classify_hosted_media_gate,
)


def test_source_media_urls_are_partial_until_hosted_media_exists() -> None:
    payload = build_media_completion_payload(
        hosted_media=HostedMediaEvidence(
            post_id="post-1",
            shortcode="C123",
            source_media_urls=("https://scontent.test/media-1.jpg",),
            source_thumbnail_url="https://scontent.test/thumb.jpg",
            hosted_media_urls=(),
            hosted_thumbnail_url=None,
        ),
        comment_text_reply_retry_targets=[{"source_id": "C123", "reason": "missing_comments"}],
    )

    assert payload["completed"] is False
    assert payload["status"] == "partial"
    assert payload["summary"]["media_mirror_retry_target_count"] == 1
    assert payload["summary"]["comment_text_reply_retry_target_count"] == 1
    assert payload["media_mirror_retry_targets"] == [
        {
            "target_type": "hosted_media",
            "reason": "missing_hosted_media",
            "missing_count": 2,
            "post_id": "post-1",
            "source_id": "C123",
            "shortcode": "C123",
            "stage": "media_mirror",
        }
    ]
    assert payload["comment_text_reply_retry_targets"] == [
        {"source_id": "C123", "reason": "missing_comments", "target_type": "comment_text_reply"}
    ]
    assert payload["retry_targets"]["media_mirror"] == payload["media_mirror_retry_targets"]
    assert payload["retry_targets"]["comment_text_reply"] == payload["comment_text_reply_retry_targets"]


def test_source_unavailable_evidence_is_terminal_but_retry_exhaustion_is_not() -> None:
    unavailable = classify_hosted_media_gate(
        HostedMediaEvidence(
            post_id="post-1",
            source_media_urls=("https://scontent.test/media-1.jpg",),
            media_mirror_status="unrecoverable",
            media_mirror_error="media[0]:download_failed:invalid_source_url",
        )
    )

    assert unavailable.completed is True
    assert unavailable.state == "source_unavailable"
    assert unavailable.retry_targets == ()
    assert unavailable.source_unavailable_count == 1

    retry_exhausted = classify_hosted_media_gate(
        HostedMediaEvidence(
            post_id="post-1",
            source_media_urls=("https://scontent.test/media-1.jpg",),
            media_mirror_status="failed",
            media_mirror_error="max_retries_exhausted",
        )
    )

    assert retry_exhausted.completed is False
    assert retry_exhausted.state == "retryable"
    assert retry_exhausted.retry_targets[0]["stage"] == "media_mirror"


def test_comment_media_retry_targets_stay_separate_from_comment_text_retries() -> None:
    payload = build_media_completion_payload(
        comment_media=[
            CommentMediaEvidence(
                comment_id="comment-1",
                post_id="post-1",
                shortcode="C123",
                source_media_urls=("https://scontent.test/comment-media.gif",),
                hosted_media_urls=(),
            )
        ],
        comment_text_reply_retry_targets=[{"source_id": "C123", "reason": "pagination_deadline_exceeded"}],
    )

    assert payload["completed"] is False
    assert payload["media_mirror_retry_targets"] == []
    assert payload["comment_media_mirror_retry_targets"] == [
        {
            "target_type": "comment_media",
            "reason": "missing_hosted_comment_media",
            "missing_count": 1,
            "post_id": "post-1",
            "comment_id": "comment-1",
            "source_id": "C123",
            "shortcode": "C123",
            "is_reply": False,
            "stage": "comment_media_mirror",
        }
    ]
    assert payload["retry_targets"]["comment_media_mirror"] == payload["comment_media_mirror_retry_targets"]
    assert payload["retry_targets"]["comment_text_reply"] == [
        {
            "source_id": "C123",
            "reason": "pagination_deadline_exceeded",
            "target_type": "comment_text_reply",
        }
    ]


def test_author_avatar_requires_hosted_avatar_when_source_avatar_exists() -> None:
    retryable = classify_author_avatar_gate(
        [
            AuthorAvatarEvidence(
                comment_id="comment-1",
                username="viewer",
                source_avatar_url="https://scontent.test/avatar.jpg",
                hosted_avatar_url=None,
            )
        ]
    )

    assert retryable.completed is False
    assert retryable.state == "retryable"
    assert retryable.retry_targets == (
        {
            "target_type": "author_avatar",
            "reason": "missing_hosted_avatar",
            "missing_count": 1,
            "comment_id": "comment-1",
            "username": "viewer",
            "stage": "media_mirror",
        },
    )

    unavailable = classify_author_avatar_gate(
        [
            AuthorAvatarEvidence(
                comment_id="comment-1",
                username="viewer",
                source_avatar_url="https://scontent.test/avatar.jpg",
                source_unavailable_evidence={"reason": "http_404_not_found", "source": "avatar_mirror"},
            )
        ]
    )

    assert unavailable.completed is True
    assert unavailable.state == "source_unavailable"
    assert unavailable.retry_targets == ()


def test_stale_media_claims_block_completion_even_when_assets_are_mirrored() -> None:
    payload = build_media_completion_payload(
        hosted_media=HostedMediaEvidence(
            post_id="post-1",
            shortcode="C123",
            source_media_urls=("https://scontent.test/media-1.jpg",),
            hosted_media_urls=("https://cdn.test/media-1.jpg",),
        ),
        comment_media=[
            CommentMediaEvidence(
                comment_id="comment-1",
                source_media_urls=("https://scontent.test/comment-media.gif",),
                hosted_media_urls=("https://cdn.test/comment-media.gif",),
            )
        ],
        stale_media_claims={
            "total": 2,
            "by_stage": {"media_mirror": 1, "comment_media_mirror": 1},
            "by_platform": {"instagram": 2},
        },
    )

    assert payload["completed"] is False
    assert payload["status"] == "blocked"
    assert payload["summary"]["blocked_count"] == 2
    assert payload["media_mirror_status"]["status"] == "blocked"
    assert payload["comment_media_mirror_status"]["status"] == "blocked"
    assert {gap["reason"] for gap in payload["gaps"]} == {"stale_media_queue_claims"}


def test_platform_only_stale_media_claims_block_completion() -> None:
    payload = build_media_completion_payload(
        hosted_media=HostedMediaEvidence(
            post_id="post-1",
            shortcode="C123",
            source_media_urls=("https://scontent.test/media-1.jpg",),
            hosted_media_urls=("https://cdn.test/media-1.jpg",),
        ),
        stale_media_claims={"by_platform": {"instagram": 1}},
    )

    assert payload["completed"] is False
    assert payload["status"] == "blocked"
    assert payload["stale_media_claims"]["total"] == 1
    assert payload["media_mirror_status"]["status"] == "blocked"
