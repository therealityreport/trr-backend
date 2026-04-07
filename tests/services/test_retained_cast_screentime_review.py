from __future__ import annotations

from trr_backend.services import retained_cast_screentime_review as review


def test_build_review_summary_derives_reviewed_leaderboard_from_segments_and_exclusions(monkeypatch) -> None:
    run = {
        "id": "run-1",
        "video_asset_id": "asset-1",
        "show_id": "show-1",
        "season_id": "season-1",
        "episode_id": "episode-1",
        "media_type": "episode",
        "effective_runtime_seconds": 42.5,
    }

    monkeypatch.setattr(
        review.cast_screentime,
        "list_leaderboard",
        lambda _run_id: [
            {
                "person_id": "person-1",
                "display_name": "Person One",
                "screen_time_seconds": 12.5,
                "frame_count": 120,
                "confidence_avg": 0.92,
            }
        ],
    )
    monkeypatch.setattr(
        review.cast_screentime,
        "list_segments",
        lambda _run_id: [
            {
                "segment_key": "segment-1",
                "person_id": "person-1",
                "start_ms": 0,
                "end_ms": 5000,
                "duration_ms": 5000,
                "frame_count": 50,
                "is_counted": True,
                "metadata": {"display_name": "Person One"},
            },
            {
                "segment_key": "segment-2",
                "person_id": "person-1",
                "start_ms": 5000,
                "end_ms": 8000,
                "duration_ms": 3000,
                "frame_count": 30,
                "is_counted": True,
                "metadata": {"display_name": "Person One"},
            },
            {
                "segment_key": "segment-3",
                "person_id": "person-2",
                "start_ms": 8000,
                "end_ms": 10000,
                "duration_ms": 2000,
                "frame_count": 20,
                "is_counted": False,
                "metadata": {"display_name": "Person Two"},
            },
        ],
    )
    monkeypatch.setattr(
        review.cast_screentime,
        "list_excluded_sections",
        lambda _run_id: [
            {
                "section_key": "cold-open",
                "section_type": "intro",
                "start_ms": 1000,
                "end_ms": 2500,
                "duration_ms": 1500,
                "detection_source": "manual",
            }
        ],
    )
    monkeypatch.setattr(
        review.cast_screentime,
        "list_suggestion_decisions_for_context",
        lambda **_kwargs: [{"suggestion_key": "suggest-person-1", "decision": "accept"}],
    )
    monkeypatch.setattr(
        review.cast_screentime,
        "list_unknown_review_state_for_context",
        lambda **_kwargs: [{"queue_key": "unknown-1", "decision": "defer"}],
    )

    summary = review.build_review_summary(run)

    assert summary["publication_mode"] == "canonical_episode"
    assert summary["is_canonical_publication"] is True
    assert summary["excluded_section_count"] == 1
    assert summary["excluded_overlap_ms"] == 1500
    assert summary["decision_counts"] == {"suggestion_decisions": 1, "unknown_review_state": 1}
    assert summary["rerun_required_for_identity_changes"] is True
    assert summary["raw_leaderboard"][0]["screen_time_seconds"] == 12.5
    assert summary["reviewed_leaderboard"][0]["person_id"] == "person-1"
    assert summary["reviewed_leaderboard"][0]["screen_time_seconds"] == 6.5
    assert summary["reviewed_leaderboard"][0]["frame_count"] == 80
