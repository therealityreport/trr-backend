"""Lane-enforcement test for TikTok posts Scrapling."""

from __future__ import annotations

from unittest.mock import patch

from trr_backend.socials import social_season_analytics_impl as repo


def test_tiktok_posts_scrapling_worker_lane_constant() -> None:
    assert repo.TIKTOK_POSTS_SCRAPLING_WORKER_LANE == "tiktok_posts_scrapling"


def test_required_worker_lane_is_detected_on_tiktok_posts_job_config() -> None:
    assert (
        repo._job_requires_dedicated_worker_lane(
            {"required_worker_lane": repo.TIKTOK_POSTS_SCRAPLING_WORKER_LANE},
        )
        is True
    )


def test_required_worker_lane_name_is_normalized_for_tiktok_posts() -> None:
    assert (
        repo._job_required_worker_lane(
            {"required_worker_lane": "  TikTok_Posts_Scrapling  "},
        )
        == repo.TIKTOK_POSTS_SCRAPLING_WORKER_LANE
    )


def test_worker_health_filters_by_tiktok_posts_scrapling_lane() -> None:
    fake_payload = {
        "workers": [
            {
                "metadata": {"worker_lane": None},
                "supported_platforms": ["tiktok"],
                "is_healthy": True,
                "is_fresh": True,
                "status": "working",
            },
        ]
    }
    with patch.object(repo, "_query_worker_health", return_value=fake_payload):
        health = repo.get_worker_health_for_lane(
            required_worker_lane=repo.TIKTOK_POSTS_SCRAPLING_WORKER_LANE,
            platform="tiktok",
        )
    assert health["healthy"] is False
    assert health["healthy_workers"] == 0
