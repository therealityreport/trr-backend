from __future__ import annotations

from unittest.mock import patch


def test_threads_posts_scrapling_stage_constant() -> None:
    from trr_backend.socials.social_season_analytics_impl import THREADS_POSTS_SCRAPLING_STAGE

    assert THREADS_POSTS_SCRAPLING_STAGE == "threads_posts_scrapling"


def test_dispatch_routes_threads_posts_scrapling_stage() -> None:
    from trr_backend.socials.social_season_analytics_impl import THREADS_POSTS_SCRAPLING_STAGE

    job = {
        "id": "00000000-0000-0000-0000-000000000003",
        "platform": "threads",
        "config": {"stage": THREADS_POSTS_SCRAPLING_STAGE, "account": "bravotv"},
        "metadata": {},
        "attempt_count": 1,
        "max_attempts": 1,
    }

    with patch("trr_backend.socials.threads.posts_scrapling.job_runner.run_threads_posts_scrapling_job") as mock_runner:
        mock_runner.return_value = {"status": "completed"}
        from trr_backend.socials.social_season_analytics_impl import _execute_claimed_job

        result = _execute_claimed_job(job, worker_id="test-worker")
        mock_runner.assert_called_once()
        assert result == {"status": "completed"}
