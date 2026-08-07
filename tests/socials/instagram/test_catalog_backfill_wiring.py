from __future__ import annotations

from typing import Any

import api.routers.socials as socials_router
from trr_backend.socials.pipelines.account_catalog import launch, progress
from trr_backend.socials.pipelines.comments import instagram as comments


def test_comments_dispatch_stays_owned_by_control_plane_after_core_sync(monkeypatch) -> None:
    from trr_backend.socials.control_plane import dispatch_runtime

    calls: list[dict[str, object]] = []

    def fake_dispatch(*, run_id=None, limit=None):
        calls.append({"run_id": run_id, "limit": limit})
        return {"dispatched_job_ids": ["job-1"]}

    monkeypatch.setattr(dispatch_runtime, "dispatch_due_social_jobs", fake_dispatch)
    monkeypatch.setattr(
        comments._core,
        "dispatch_due_social_jobs",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("legacy dispatcher restored")),
    )

    comments._sync_core_overrides()
    result = comments.dispatch_due_social_jobs(run_id="run-1", limit=3)

    assert "dispatch_due_social_jobs" in comments._LOCAL_ROOM_NAMES
    assert result == {"dispatched_job_ids": ["job-1"]}
    assert calls == [{"run_id": "run-1", "limit": 3}]


def test_budget_worker_limit_caps_requested_workers() -> None:
    decision = {"state": "reduced", "limits": {"effective_max_concurrent_jobs": 1}}

    assert launch._apply_budget_worker_limit(6, decision) == 1
    assert launch._apply_budget_worker_limit(None, decision) == 1


def test_adaptive_worker_plan_ramps_healthy_instagram_backfill() -> None:
    plan = launch._instagram_backfill_worker_plan(
        selected_tasks=["post_details", "comments"],
        target_readiness={
            "blocker_reasons": [],
            "comments_blocker_reasons": [],
            "comments_target_source_ids_count": 25,
        },
        budget_decision={"state": "normal", "limits": {"effective_max_concurrent_jobs": 4}},
    )

    assert plan["state"] == "ramped"
    assert plan["healthy"] is True
    assert plan["details_refresh_worker_count"] == 2
    assert plan["comments_worker_count"] == 2
    assert plan["effective_ceiling"] == 2
    assert plan["runbook_state"]["phase"] == "live_apply"
    assert plan["runbook_state"]["binding_cap"] == 2
    assert plan["runbook_state"]["speed_canary_cap"] == 4
    assert plan["runbook_state"]["cap4_canary"]["mode"] == "metadata_only"
    assert plan["runbook_state"]["minimum_sample_floor"] == 25


def test_adaptive_worker_plan_marks_paused_budget_as_blocked_metadata() -> None:
    plan = launch._instagram_backfill_worker_plan(
        selected_tasks=["post_details", "comments"],
        target_readiness={
            "blocker_reasons": [],
            "comments_blocker_reasons": [],
            "comments_target_source_ids_count": 25,
        },
        budget_decision={
            "state": "identity_blocked",
            "lane": "instagram_backfill",
            "account": "bravotv",
            "reasons": ["identity_blocked"],
            "limits": {"effective_max_concurrent_jobs": 0},
        },
    )

    assert plan["state"] == "blocked_budget"
    assert plan["healthy"] is False
    assert plan["effective_ceiling"] == 0
    assert plan["blocked_budget"]["state"] == "identity_blocked"
    assert plan["blocked_budget"]["reason"] == "identity_blocked"
    assert plan["blocked_budget"]["runbook_state"]["binding_cap"] == 2
    assert "status" not in plan


def test_target_readiness_keeps_twenty_five_sample_ids(monkeypatch) -> None:
    sample_ids = [f"SC{i:02d}" for i in range(30)]

    monkeypatch.setattr(
        launch._core,
        "_instagram_social_account_comments_target_counts",
        lambda _account: {
            "available_posts": 30,
            "eligible_posts": 30,
            "missing_posts": 0,
            "stale_posts": 0,
        },
        raising=False,
    )
    monkeypatch.setattr(
        launch._core,
        "preview_social_account_comments_scrape",
        lambda *_args, **_kwargs: {
            "target_source_ids_count": 30,
            "sample_target_source_ids": sample_ids,
            "comments_shard_count": 2,
            "comments_sharding_enabled": True,
            "recommended_comments_shard_count": 2,
            "target_priority": "stale_or_missing",
        },
        raising=False,
    )

    readiness = launch.build_instagram_backfill_target_readiness("BravoTV")

    assert readiness["sample_target_source_ids"] == sample_ids[:25]


def test_initial_instagram_completion_metadata_stores_snapshot_summary() -> None:
    metadata = launch._initial_instagram_completion_metadata(
        account_handle="bravotv",
        effective_selected_tasks=["post_details", "media"],
    )

    snapshot = metadata["snapshot_completion_summary"]
    assert snapshot["complete"] is False
    assert "post_detail" in snapshot["deferred_parts"]
    assert "hosted_media" in snapshot["deferred_parts"]
    assert metadata["media_completion"]["status"] == "pending"


def test_progress_payload_exposes_budget_and_blocks_on_media_queue() -> None:
    payload = progress._catalog_completion_progress_payload(
        run_config={
            "effective_selected_tasks": ["media"],
            "budget_decision": {"state": "reduced", "limits": {"effective_max_concurrent_jobs": 1}},
            "adaptive_worker_plan": {"state": "unchanged", "effective_ceiling": 1},
            "timing": {"per_stage_ms": {"target_readiness": 12.5, "catalog_dispatch": 4.0}},
            "snapshot_completion_summary": {"state": "incomplete"},
            "media_completion": {"target": {"account_handle": "bravotv"}},
        },
        stages_payload={
            "media_mirror": {"jobs_waiting": 1, "jobs_active": 0},
            "comment_media_mirror": {"jobs_waiting": 0, "jobs_active": 1},
        },
    )

    assert payload["budget_decision"]["state"] == "reduced"
    assert payload["adaptive_worker_plan"]["effective_ceiling"] == 1
    assert payload["per_stage_timing_ms"]["target_readiness"] == 12.5
    assert payload["snapshot_completion_summary"]["state"] == "incomplete"
    assert payload["media_completion"]["status"] == "blocked"
    assert payload["media_completion"]["completed"] is False
    assert payload["media_completion"]["stale_media_claims"]["total"] == 2


def test_progress_payload_maps_paused_budget_to_blocked_budget_metadata() -> None:
    payload = progress._catalog_completion_progress_payload(
        run_config={
            "effective_selected_tasks": ["post_details"],
            "budget_decision": {
                "state": "paused",
                "lane": "instagram_backfill",
                "account": "bravotv",
                "reasons": ["proxy_cooldown_active"],
                "limits": {"effective_max_concurrent_jobs": 0},
                "runbook_state": {
                    "phase": "live_apply",
                    "binding_cap": 2,
                    "speed_canary_cap": 4,
                    "minimum_sample_floor": 25,
                },
            },
        },
        stages_payload={},
    )

    assert payload["budget_decision"]["state"] == "paused"
    assert payload["budget_blocked"] is True
    assert payload["operational_state"] == "blocked_budget"
    assert payload["blocked_reason"] == "proxy_cooldown_active"
    assert payload["blocked_budget"]["state"] == "paused"
    assert payload["blocked_budget"]["runbook_state"]["binding_cap"] == 2
    assert "run_status" not in payload


def test_completion_retry_targets_bucket_into_queue_primitives(monkeypatch) -> None:
    calls: dict[str, list[Any]] = {
        "media": [],
        "comment_media": [],
        "comment_text": [],
        "dispatch": [],
    }

    monkeypatch.setattr(
        comments,
        "_load_instagram_post_for_completion_retry",
        lambda target: {"id": "post-1", "shortcode": target["source_id"]},
    )
    monkeypatch.setattr(
        comments,
        "_load_instagram_comment_for_completion_retry",
        lambda target: {"id": "comment-db-1", "comment_id": target["comment_id"], "post_id": "post-1"},
    )
    monkeypatch.setattr(comments, "_resolve_media_mirror_stage_context", lambda *args, **kwargs: object())

    def _enqueue_media(*args, **kwargs):
        calls["media"].append(kwargs)
        return "media-job-1"

    def _enqueue_comment_media(*args, **kwargs):
        calls["comment_media"].append(kwargs)
        return "comment-media-job-1"

    def _enqueue_comment_text(**kwargs):
        calls["comment_text"].append(kwargs)
        return {"enqueue": {"performed": True}, "selected_target_source_ids": kwargs["shortcodes"]}

    monkeypatch.setattr(comments, "_enqueue_instagram_media_mirror_job", _enqueue_media)
    monkeypatch.setattr(comments, "_enqueue_platform_comment_media_mirror_job", _enqueue_comment_media)
    monkeypatch.setattr(comments, "enqueue_instagram_comments_audit_cursor_retries", _enqueue_comment_text)
    monkeypatch.setattr(comments, "dispatch_due_social_jobs", lambda **kwargs: calls["dispatch"].append(kwargs))

    result = comments.enqueue_instagram_completion_retry_targets(
        account_handle="BravoTV",
        run_id="run-1",
        retry_targets={
            "media_mirror": [{"stage": "media_mirror", "source_id": "ABC"}],
            "comment_media_mirror": [{"stage": "comment_media_mirror", "comment_id": "178"}],
            "comment_text_reply": [{"stage": "comment_text_reply", "source_id": "XYZ"}],
        },
    )

    assert result["created_media_mirror_job_ids"] == ["media-job-1"]
    assert result["created_comment_media_mirror_job_ids"] == ["comment-media-job-1"]
    assert result["comment_text_reply_enqueue"]["selected_target_source_ids"] == ["XYZ"]
    assert calls["media"][0]["account"] == "bravotv"
    assert calls["comment_media"][0]["account"] == "bravotv"
    assert calls["dispatch"] == [{"run_id": "run-1"}]


def test_completion_retry_targets_skip_complete_and_prioritize_impact(monkeypatch) -> None:
    calls: dict[str, list[object]] = {
        "media": [],
        "comment_text": [],
        "dispatch": [],
    }

    monkeypatch.setattr(
        comments,
        "_load_instagram_post_for_completion_retry",
        lambda target: {"id": f"post-{target['source_id']}", "shortcode": target["source_id"]},
    )

    def _enqueue_media(*args, **kwargs):
        calls["media"].append(kwargs)
        return f"media-job-{kwargs['post_row']['shortcode']}"

    def _enqueue_comment_text(**kwargs):
        calls["comment_text"].append(kwargs)
        return {"enqueue": {"performed": True}, "selected_target_source_ids": kwargs["shortcodes"]}

    monkeypatch.setattr(comments, "_enqueue_instagram_media_mirror_job", _enqueue_media)
    monkeypatch.setattr(comments, "enqueue_instagram_comments_audit_cursor_retries", _enqueue_comment_text)
    monkeypatch.setattr(comments, "dispatch_due_social_jobs", lambda **kwargs: calls["dispatch"].append(kwargs))

    result = comments.enqueue_instagram_completion_retry_targets(
        account_handle="BravoTV",
        run_id="run-1",
        retry_targets=[
            {"stage": "media_mirror", "source_id": "LOW", "missing_media_count": 1},
            {"stage": "media_mirror", "source_id": "DONE", "state": "captured", "missing_media_count": 99},
            {"stage": "media_mirror", "source_id": "HIGH", "missing_media_count": 7},
            {"stage": "comment_text_reply", "source_id": "LOW-COMMENTS", "reported_comments": 3},
            {"stage": "comment_text_reply", "source_id": "HIGH-COMMENTS", "reported_comments": 30},
        ],
    )

    assert result["created_media_mirror_job_ids"] == ["media-job-HIGH", "media-job-LOW"]
    assert result["comment_text_reply_enqueue"]["selected_target_source_ids"] == [
        "HIGH-COMMENTS",
        "LOW-COMMENTS",
    ]
    assert result["effective_target_count"] == 4
    assert result["skipped_targets"] == [
        {
            "target": {"stage": "media_mirror", "source_id": "DONE", "state": "captured", "missing_media_count": 99},
            "reason": "already_complete",
        }
    ]


def test_backfill_response_surfaces_worker_cap_truth_for_over_budget_request() -> None:
    """A detail_worker_count=8 request under budget normal must surface requested=8 +
    applied=2 + the honest v4 binding-cap note (real LIVE_APPLY cap == 2)."""

    plan = launch._instagram_backfill_worker_plan(
        selected_tasks=["post_details"],
        target_readiness={"blocker_reasons": [], "comments_blocker_reasons": []},
        budget_decision={"state": "normal", "limits": {"effective_max_concurrent_jobs": 2}},
        details_refresh_worker_count=8,
    )
    result = {"adaptive_worker_plan": plan, "effective_selected_tasks": ["post_details"]}

    transparency = socials_router._build_catalog_worker_cap_transparency(result)

    assert transparency["requested_details_worker_count"] == 8
    assert transparency["details_refresh_worker_count"] == 2
    assert transparency["live_apply_binding_cap"] == 2
    assert transparency["worker_cap_note"] == (
        "requested 8, applied 2 (v4 binding cap 2; set enable_cap4_canary for 4)"
    )
    # Fresh insert (no dedupe signal in result) does not emit a deduped key.
    assert "deduped" not in transparency


def test_backfill_response_surfaces_idempotency_dedupe_outcome() -> None:
    """When the launch result reports a deduped reservation, the worker-cap transparency
    block surfaces deduped=True so callers know the submit was idempotently coalesced."""

    plan = launch._instagram_backfill_worker_plan(
        selected_tasks=["post_details"],
        target_readiness={"blocker_reasons": [], "comments_blocker_reasons": []},
        budget_decision={"state": "normal", "limits": {"effective_max_concurrent_jobs": 2}},
        details_refresh_worker_count=8,
    )

    transparency = socials_router._build_catalog_worker_cap_transparency(
        {"adaptive_worker_plan": plan, "deduped": True}
    )

    assert transparency["deduped"] is True


def test_backfill_response_surfaces_comments_not_selected_skip_reason() -> None:
    """When comments are not selected, the response must surface the precise skip reason
    instead of any hardcoded narrative."""

    result = {"effective_selected_tasks": ["post_details"]}

    skip = socials_router._build_catalog_comments_skip_transparency(result)

    assert skip["comments_skip_reason"] == "comments_not_selected"
    assert skip["comments_skip_detail"] == "comments task not selected for this run"
    assert skip["comments_operator_action"] == ("Relaunch with the comments task selected to scrape comments.")


def test_backfill_response_surfaces_posts_auth_blocked_skip_reason() -> None:
    """A checkpoint-blocked run with comments selected surfaces posts_auth_blocked so the
    dashboard shows the concrete operator remediation, never 'manual checkpoint'."""

    result = {
        "effective_selected_tasks": ["post_details", "comments"],
        "stop_reason": "checkpoint_required",
    }

    skip = socials_router._build_catalog_comments_skip_transparency(result)

    assert skip["comments_skip_reason"] == "posts_auth_blocked"
    assert skip["comments_skip_detail"] == "checkpoint_required"
    assert "manual checkpoint" not in skip["comments_operator_action"]
