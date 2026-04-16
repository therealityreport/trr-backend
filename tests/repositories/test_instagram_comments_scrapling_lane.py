"""P3-9 lane-enforcement test — verifies the comments Scrapling lane can only
be claimed by a worker heartbeating `instagram_comments_scrapling`.

This is the contract that prevents the main posts worker from silently
stealing a Scrapling job it cannot run. Without this enforcement, losing
a comments worker doesn't surface to operators — posts workers claim the
jobs, fail opaquely, and the queue backs up.
"""

from __future__ import annotations

from unittest.mock import patch

from trr_backend.repositories import social_season_analytics as repo


def test_required_worker_lane_is_detected_on_job_config() -> None:
    """`_job_requires_dedicated_worker_lane` must return True for the Scrapling
    lane so the generalized worker-health check kicks in."""
    assert (
        repo._job_requires_dedicated_worker_lane(
            {"required_worker_lane": repo.INSTAGRAM_COMMENTS_SCRAPLING_WORKER_LANE},
        )
        is True
    )


def test_required_worker_lane_name_is_normalized() -> None:
    """Whitespace/case-insensitive normalization so misconfigured env
    can't bypass the lane filter."""
    assert (
        repo._job_required_worker_lane(
            {"required_worker_lane": "  Instagram_Comments_Scrapling  "},
        )
        == repo.INSTAGRAM_COMMENTS_SCRAPLING_WORKER_LANE
    )


def test_get_worker_health_for_lane_filters_by_metadata_lane() -> None:
    """Only workers whose heartbeat metadata has the matching lane should
    be counted healthy for that lane — otherwise the posts pool would
    satisfy a Scrapling-lane health check."""
    fake_payload = {
        "workers": [
            # Healthy posts worker — must NOT satisfy the Scrapling lane.
            {
                "metadata": {"worker_lane": None},
                "supported_platforms": ["instagram"],
                "is_healthy": True,
                "is_fresh": True,
                "status": "working",
            },
            # Stale Scrapling worker — counts as a lane worker but not healthy.
            {
                "metadata": {"worker_lane": "instagram_comments_scrapling"},
                "supported_platforms": ["instagram"],
                "is_healthy": False,
                "is_fresh": False,
                "status": "idle",
            },
        ]
    }
    with patch.object(repo, "_query_worker_health", return_value=fake_payload):
        health = repo.get_worker_health_for_lane(
            required_worker_lane="instagram_comments_scrapling",
            platform="instagram",
        )
    assert health["healthy"] is False
    assert health["total_workers"] == 1, "Posts worker must not be counted as a lane worker"
    assert health["healthy_workers"] == 0
    assert health["reason"] == "no_healthy_lane_workers"
    assert health["required_worker_lane"] == "instagram_comments_scrapling"


def test_get_worker_health_for_lane_passes_when_scrapling_worker_healthy() -> None:
    """A healthy worker with matching lane metadata must satisfy the check."""
    fake_payload = {
        "workers": [
            {
                "metadata": {"worker_lane": "instagram_comments_scrapling"},
                "supported_platforms": ["instagram"],
                "is_healthy": True,
                "is_fresh": True,
                "status": "working",
            }
        ]
    }
    with patch.object(repo, "_query_worker_health", return_value=fake_payload):
        health = repo.get_worker_health_for_lane(
            required_worker_lane="instagram_comments_scrapling",
            platform="instagram",
        )
    assert health["healthy"] is True
    assert health["healthy_workers"] == 1
    assert health["reason"] is None
