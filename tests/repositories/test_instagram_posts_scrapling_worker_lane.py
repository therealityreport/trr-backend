"""Lane-enforcement test for Instagram posts Scrapling — mirrors the comments
lane pattern at test_instagram_comments_scrapling_lane.py."""

from __future__ import annotations

from unittest.mock import patch

from trr_backend.socials import social_season_analytics_impl as repo


def test_instagram_posts_scrapling_worker_lane_constant() -> None:
    assert repo.INSTAGRAM_POSTS_SCRAPLING_WORKER_LANE == "instagram_posts_scrapling"


def test_required_worker_lane_is_detected_on_instagram_posts_job_config() -> None:
    assert (
        repo._job_requires_dedicated_worker_lane(
            {"required_worker_lane": repo.INSTAGRAM_POSTS_SCRAPLING_WORKER_LANE},
        )
        is True
    )


def test_required_worker_lane_name_is_normalized_for_instagram_posts() -> None:
    """Whitespace/case insensitivity — same safeguard as comments lane."""
    assert (
        repo._job_required_worker_lane(
            {"required_worker_lane": "  Instagram_Posts_Scrapling  "},
        )
        == repo.INSTAGRAM_POSTS_SCRAPLING_WORKER_LANE
    )


def test_worker_health_filters_by_instagram_posts_scrapling_lane() -> None:
    """A posts worker without the lane metadata must NOT satisfy the health check."""
    fake_payload = {
        "workers": [
            {
                "metadata": {"worker_lane": None},
                "supported_platforms": ["instagram"],
                "is_healthy": True,
                "is_fresh": True,
                "status": "working",
            },
        ]
    }
    with patch.object(repo, "_query_worker_health", return_value=fake_payload):
        health = repo.get_worker_health_for_lane(
            required_worker_lane=repo.INSTAGRAM_POSTS_SCRAPLING_WORKER_LANE,
            platform="instagram",
        )
    assert health["healthy"] is False
    assert health["healthy_workers"] == 0
