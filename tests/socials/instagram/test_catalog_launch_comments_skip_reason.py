"""Unit tests for derive_comments_skip_reason (run-config-driven comments-skip messaging)."""

from __future__ import annotations

from trr_backend.socials.pipelines.account_catalog import launch


def test_comments_not_selected_when_comments_absent() -> None:
    result = launch.derive_comments_skip_reason(
        {
            "stage_graph": {"comments": {"selected": False}},
            "effective_selected_tasks": ["post_details", "media"],
        }
    )
    assert result["reason"] == "comments_not_selected"
    assert result["detail"]
    assert result["operator_action"]


def test_posts_auth_blocked_from_stop_reason_checkpoint() -> None:
    result = launch.derive_comments_skip_reason(
        {
            "stage_graph": {"comments": {"selected": True}},
            "stop_reason": "checkpoint_required",
        }
    )
    assert result["reason"] == "posts_auth_blocked"
    assert result["detail"] == "checkpoint_required"
    assert "manual checkpoint" not in result["operator_action"].lower()
    assert "checkpoint" in result["operator_action"].lower()


def test_posts_auth_blocked_from_probe_reason() -> None:
    result = launch.derive_comments_skip_reason(
        {
            "effective_selected_tasks": ["comments"],
            "posts_auth_probe": {"reason": "checkpoint_required"},
        }
    )
    assert result["reason"] == "posts_auth_blocked"


def test_posts_auth_blocked_from_comments_blocker_reasons() -> None:
    result = launch.derive_comments_skip_reason(
        {
            "stage_graph": {
                "comments": {
                    "selected": True,
                    "blocker_reasons": ["posts_auth_blocked"],
                }
            }
        }
    )
    assert result["reason"] == "posts_auth_blocked"


def test_no_commentable_targets_when_count_zero() -> None:
    result = launch.derive_comments_skip_reason(
        {
            "effective_selected_tasks": ["comments"],
            "target_readiness": {
                "can_start_comments": False,
                "commentable_target_count": 0,
            },
        }
    )
    assert result["reason"] == "no_commentable_targets"
    assert result["operator_action"]


def test_authenticated_comments_not_requested_public_lane_healthy() -> None:
    result = launch.derive_comments_skip_reason(
        {
            "stage_graph": {
                "comments": {
                    "selected": True,
                    "blocker_reasons": ["strict_authenticated_probe_not_requested"],
                }
            },
            "target_readiness": {
                "can_start_comments": True,
                "commentable_target_count": 12,
            },
        }
    )
    assert result["reason"] == "authenticated_comments_not_requested"
    assert result["detail"] == "public lane healthy"


def test_comments_running_or_complete_default_branch() -> None:
    result = launch.derive_comments_skip_reason(
        {
            "effective_selected_tasks": ["comments"],
            "target_readiness": {
                "can_start_comments": True,
                "commentable_target_count": 8,
            },
        }
    )
    assert result["reason"] == "comments_running_or_complete"


def test_defensive_against_empty_and_none_config() -> None:
    # Empty config -> comments not selected (rule 1, the safe default).
    empty = launch.derive_comments_skip_reason({})
    assert empty["reason"] == "comments_not_selected"
    # None-valued keys must not raise.
    noned = launch.derive_comments_skip_reason(
        {
            "stage_graph": None,
            "effective_selected_tasks": None,
            "target_readiness": None,
            "posts_auth_probe": None,
            "comments_blocker_reasons": None,
            "stop_reason": None,
        }
    )
    assert noned["reason"] == "comments_not_selected"


def test_no_branch_emits_literal_manual_checkpoint() -> None:
    configs = [
        {"effective_selected_tasks": ["post_details"]},
        {"effective_selected_tasks": ["comments"], "stop_reason": "checkpoint_required"},
        {
            "effective_selected_tasks": ["comments"],
            "target_readiness": {"can_start_comments": False, "commentable_target_count": 0},
        },
        {
            "stage_graph": {
                "comments": {
                    "selected": True,
                    "blocker_reasons": ["strict_authenticated_probe_not_requested"],
                }
            },
            "target_readiness": {"can_start_comments": True, "commentable_target_count": 3},
        },
        {
            "effective_selected_tasks": ["comments"],
            "target_readiness": {"can_start_comments": True, "commentable_target_count": 3},
        },
    ]
    for config in configs:
        result = launch.derive_comments_skip_reason(config)
        for value in result.values():
            assert "manual checkpoint" not in str(value).lower()
