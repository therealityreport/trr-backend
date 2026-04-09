import pytest
from unittest.mock import patch, MagicMock


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

    with patch(
        "trr_backend.socials.youtube.YouTubeScraper",
        return_value=mock_scraper_instance,
    ), patch(
        "trr_backend.socials.youtube.YouTubeDataApiClient",
    ) as mock_api_cls, patch(
        "trr_backend.repositories.social_season_analytics._shared_catalog_mode",
        return_value=True,
    ), patch(
        "trr_backend.repositories.social_season_analytics._persist_shared_catalog_posts_with_progress",
        return_value=[],
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
