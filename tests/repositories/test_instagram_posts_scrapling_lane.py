from __future__ import annotations

from unittest.mock import patch


def test_instagram_posts_scrapling_stage_constant():
    from trr_backend.repositories.social_season_analytics import INSTAGRAM_POSTS_SCRAPLING_STAGE

    assert INSTAGRAM_POSTS_SCRAPLING_STAGE == "posts_scrapling"


def test_dispatch_routes_instagram_posts_scrapling_stage():
    """Verify _execute_claimed_job dispatches to the posts_scrapling runner
    when platform=instagram and stage=posts_scrapling."""
    from trr_backend.repositories.social_season_analytics import INSTAGRAM_POSTS_SCRAPLING_STAGE

    job = {
        "id": "00000000-0000-0000-0000-000000000001",
        "platform": "instagram",
        "config": {"stage": INSTAGRAM_POSTS_SCRAPLING_STAGE, "account": "testuser"},
        "metadata": {},
        "attempt_count": 1,
        "max_attempts": 1,
    }
    with patch(
        "trr_backend.socials.instagram.posts_scrapling.job_runner.run_instagram_posts_scrapling_job"
    ) as mock_runner:
        mock_runner.return_value = {"status": "completed"}
        from trr_backend.repositories.social_season_analytics import _execute_claimed_job

        result = _execute_claimed_job(job, worker_id="test-worker")
        mock_runner.assert_called_once()
        assert result == {"status": "completed"}
