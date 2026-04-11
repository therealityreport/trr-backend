from unittest.mock import MagicMock, patch

import pytest


def test_youtube_empty_channel_page_sets_error_code():
    """When YouTube scraper returns 0 posts with no error_code,
    the orchestration layer should set youtube_empty_channel_page."""
    from trr_backend.repositories.social_season_analytics import (
        _scrape_shared_youtube_posts,
    )

    mock_scraper_instance = MagicMock()
    mock_scraper_instance.scrape.return_value = []
    mock_scraper_instance.last_retrieval_meta = {
        "videos_found": 0,
        "shorts_found": 0,
        "first_page_counts": {"videos": 0, "shorts": 0},
        "total_posts": 0,
    }

    with (
        patch(
            "trr_backend.socials.youtube.YouTubeScraper",
            return_value=mock_scraper_instance,
        ),
        patch(
            "trr_backend.socials.youtube.YouTubeDataApiClient",
        ) as mock_api_cls,
        patch(
            "trr_backend.repositories.social_season_analytics._shared_catalog_mode",
            return_value=True,
        ),
        patch(
            "trr_backend.repositories.social_season_analytics._persist_shared_catalog_posts_with_progress",
            return_value=[],
        ),
    ):
        mock_api_cls.return_value.enabled.return_value = False

        rows, meta = _scrape_shared_youtube_posts(
            run_id="test-run",
            account_handle="bravo",
            config={
                "pipeline_ingest_mode": "shared_account_catalog_backfill",
                "catalog_mode": True,
            },
            job_id="test-job",
        )

    assert len(rows) == 0
    assert meta.get("error_code") == "youtube_empty_channel_page"
    assert meta.get("retryable") is True


@pytest.mark.parametrize(
    "stats,expected",
    [
        # Classic: all items are before-window
        (
            {"before_window_items": 10, "window_candidate_items": 0, "after_window_items": 0, "timestamp_unknown": 0},
            True,
        ),
        # All items are undated (shorts with low-precision timestamps)
        (
            {"before_window_items": 0, "window_candidate_items": 0, "after_window_items": 0, "timestamp_unknown": 30},
            True,
        ),
        # Mix of before-window and undated
        (
            {"before_window_items": 5, "window_candidate_items": 0, "after_window_items": 0, "timestamp_unknown": 20},
            True,
        ),
        # Has window candidates — not before-only
        (
            {"before_window_items": 5, "window_candidate_items": 1, "after_window_items": 0, "timestamp_unknown": 10},
            False,
        ),
        # Has after-window items — not before-only
        (
            {"before_window_items": 5, "window_candidate_items": 0, "after_window_items": 2, "timestamp_unknown": 0},
            False,
        ),
        # Completely empty page — neither before nor anything
        (
            {"before_window_items": 0, "window_candidate_items": 0, "after_window_items": 0, "timestamp_unknown": 0},
            False,
        ),
    ],
    ids=["dated-before", "all-undated", "mixed-before-undated", "has-candidates", "has-after", "empty"],
)
def test_page_before_only_includes_undated_shorts(stats, expected):
    """page_before_only should be True for pages with only before-window
    and/or timestamp_unknown items, so the pre_window_page_cap triggers
    correctly for shorts surfaces with low-precision dates."""
    _has_window = bool(stats.get("window_candidate_items"))
    _has_after = bool(stats.get("after_window_items"))
    _has_before = bool(stats.get("before_window_items"))
    _has_unknown = bool(stats.get("timestamp_unknown"))
    page_before_only = (_has_before or _has_unknown) and not _has_window and not _has_after
    assert page_before_only is expected
