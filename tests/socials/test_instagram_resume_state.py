from __future__ import annotations

from datetime import UTC, datetime, timedelta

from trr_backend.socials.instagram.resume_state import InstagramResumeState


def test_resume_state_from_metadata_rejects_stale_state() -> None:
    stale_payload = {
        "next_cursor": "cursor-2",
        "pages_scanned": 3,
        "posts_checked": 100,
        "seen_cursors": ["cursor-1", "cursor-2"],
        "best_before": (datetime.now(UTC) - timedelta(hours=1)).isoformat(),
        "last_transport": "authenticated",
    }

    state = InstagramResumeState.from_metadata(stale_payload)

    assert state is None


def test_resume_state_to_metadata_caps_seen_cursors() -> None:
    state = InstagramResumeState(
        next_cursor="cursor-2",
        pages_scanned=3,
        posts_checked=100,
        seen_cursors=[f"cursor-{idx}" for idx in range(200)],
        best_before=datetime.now(UTC) + timedelta(hours=1),
        last_transport="authenticated",
    )

    payload = state.to_metadata()

    assert len(payload["seen_cursors"]) == 128
    assert payload["seen_cursors"][0] == "cursor-72"
