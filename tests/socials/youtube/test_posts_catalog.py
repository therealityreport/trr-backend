from __future__ import annotations

from datetime import UTC, datetime

from trr_backend.socials.youtube.posts_catalog.catalog import (
    _bounded_window_no_hit_completed,
    _coerce_dt,
    _shared_youtube_bounded_window_page_cap,
)


def test_shared_youtube_bounded_window_page_cap_parses_json_date_strings(
    monkeypatch,
) -> None:
    monkeypatch.delenv("SOCIAL_YOUTUBE_SHARED_BOUNDED_WINDOW_MAX_PAGES", raising=False)

    cap = _shared_youtube_bounded_window_page_cap(
        {
            "date_start": "2026-05-01T00:00:00Z",
            "date_end": "2026-05-18T23:59:59+00:00",
        }
    )

    assert cap == 6
    assert _coerce_dt("2026-05-18T23:59:59Z") == datetime(2026, 5, 18, 23, 59, 59, tzinfo=UTC)


def test_bounded_window_no_hit_completed_is_non_retryable_empty_result() -> None:
    assert _bounded_window_no_hit_completed(
        {
            "yt_dlp_channel_fallback_skip_reason": "bounded_window_no_hits_after_channel_scan",
            "continuation_failure_count": 0,
            "matched_posts": 0,
            "posts_checked": 516,
        }
    )


def test_bounded_window_no_hit_completed_rejects_unchecked_empty_page() -> None:
    assert not _bounded_window_no_hit_completed(
        {
            "yt_dlp_channel_fallback_skip_reason": "bounded_window_no_hits_after_channel_scan",
            "continuation_failure_count": 0,
            "matched_posts": 0,
            "posts_checked": 0,
        }
    )
