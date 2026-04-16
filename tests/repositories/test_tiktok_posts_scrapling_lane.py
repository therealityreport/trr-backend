from __future__ import annotations

from unittest.mock import patch


def test_tiktok_posts_scrapling_stage_constant():
    from trr_backend.repositories.social_season_analytics import TIKTOK_POSTS_SCRAPLING_STAGE

    assert TIKTOK_POSTS_SCRAPLING_STAGE == "tiktok_posts_scrapling"


def test_dispatch_routes_tiktok_posts_scrapling_stage():
    """Verify _execute_claimed_job dispatches to the posts_scrapling runner
    when platform=tiktok and stage=tiktok_posts_scrapling."""
    from trr_backend.repositories.social_season_analytics import TIKTOK_POSTS_SCRAPLING_STAGE

    job = {
        "id": "00000000-0000-0000-0000-000000000002",
        "platform": "tiktok",
        "config": {"stage": TIKTOK_POSTS_SCRAPLING_STAGE, "account": "testuser"},
        "metadata": {},
        "attempt_count": 1,
        "max_attempts": 1,
    }
    with patch("trr_backend.socials.tiktok.posts_scrapling.job_runner.run_tiktok_posts_scrapling_job") as mock_runner:
        mock_runner.return_value = {"status": "completed"}
        from trr_backend.repositories.social_season_analytics import _execute_claimed_job

        result = _execute_claimed_job(job, worker_id="test-worker")
        mock_runner.assert_called_once()
        assert result == {"status": "completed"}
