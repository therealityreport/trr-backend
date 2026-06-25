from __future__ import annotations

from datetime import datetime, timezone

from trr_backend.socials.pipelines.comments.instagram import (
    SocialWorkerUnavailableError,
    _build_comments_scrape_run_progress_payload,
    _instagram_comments_audit_cursor_counts_by_shortcode,
    _load_instagram_comments_audit_cursor_rows,
    _normalize_instagram_comments_audit_retry_stop_reasons,
    _split_instagram_comments_audit_cursor_targets_into_active_run,
    enqueue_instagram_comments_audit_cursor_retries,
)


def test_comments_progress_payload_surfaces_network_spend_and_target_rows() -> None:
    now = datetime(2026, 6, 12, tzinfo=timezone.utc)
    payload = _build_comments_scrape_run_progress_payload(
        platform="instagram",
        account_handle="bravotv",
        rows=[
            {
                "run_id": "11111111-1111-1111-1111-111111111111",
                "run_status": "running",
                "created_at": now,
                "started_at": now,
                "run_config": {"target_source_ids_count": 1, "comments_shard_count": 1},
                "job_id": "22222222-2222-2222-2222-222222222222",
                "job_status": "failed",
                "items_found": 120,
                "job_created_at": now,
                "job_started_at": now,
                "last_error_code": "instagram_comments_incomplete_retryable",
                "config": {
                    "target_source_ids": ["C123"],
                    "comments_shard_index": 1,
                    "comments_shard_count": 1,
                    "comments_shard_target_count": 1,
                },
                "metadata": {
                    "fetcher_runtime": {
                        "bytes_by_host": {
                            "i.instagram.com": 12_000_000,
                            "static.cdninstagram.com": 250_000,
                        },
                        "request_count_by_host": {
                            "i.instagram.com": 38,
                            "static.cdninstagram.com": 4,
                        },
                        "network_policy": {
                            "mode": "production",
                            "blocked_request_count_by_host": {"static.cdninstagram.com": 38},
                            "blocked_bytes_estimate_by_host": {"static.cdninstagram.com": 0},
                        },
                        "retry_reason_counts": {"network_stopped": 1},
                    },
                    "retry_rebalance": {"remaining_target_source_ids": ["C123"]},
                    "post_fetch_failures": {
                        "target_source_ids": ["C123"],
                        "fetch_reasons": {"C123": "network_stopped"},
                        "reason_counts": {"network_stopped": 1},
                    },
                    "comment_capture": {
                        "latest": {"stop_reason": "network_stopped"},
                        "samples": [{"shortcode": "C123", "stop_reason": "network_stopped"}],
                    },
                    "post_latency": {
                        "samples": [
                            {
                                "shortcode": "C123",
                                "reported_comment_count": 200,
                                "stored_total_comments": 120,
                                "observed_comment_count": 120,
                            }
                        ]
                    },
                    "top_level_checkpoint_summary": {
                        "items": [
                            {
                                "target_shortcode": "C123",
                                "stop_reason": "network_stopped",
                                "pages_seen": 12,
                            }
                        ]
                    },
                    "reply_checkpoint_summary": {
                        "items": [{"target_shortcode": "C123", "parent_comment_id": "parent-1"}]
                    },
                },
            }
        ],
    )

    assert payload["recommended_next_action"] == "retry_network_stopped_targets"
    assert payload["network_spend"]["observed_proxy_bytes"] == 12_250_000
    assert payload["network_spend"]["static_cdninstagram_bytes"] == 250_000
    assert payload["network_spend"]["static_cdninstagram_blocked_request_count"] == 38
    assert payload["summary"]["static_cdninstagram_blocked_requests"] == 38
    assert payload["retry_progress"]["network_stopped_target_count"] == 1
    assert payload["retry_progress"]["network_stopped_target_source_ids"] == ["C123"]
    target_row = payload["target_progress_rows"][0]
    assert target_row["shortcode"] == "C123"
    assert target_row["saved_comment_count"] == 120
    assert target_row["reported_comment_count"] == 200
    assert target_row["missing_comment_gap"] == 80
    assert target_row["network_stopped"] is True
    assert target_row["has_top_level_cursor"] is True
    assert target_row["reply_resume_count"] == 1


def test_comments_progress_payload_classifies_pagination_deadline_as_cursor_recovery() -> None:
    now = datetime(2026, 6, 22, tzinfo=timezone.utc)
    payload = _build_comments_scrape_run_progress_payload(
        platform="instagram",
        account_handle="bravotv",
        rows=[
            {
                "run_id": "11111111-1111-1111-1111-111111111111",
                "run_status": "failed",
                "created_at": now,
                "started_at": now,
                "completed_at": now,
                "run_config": {"target_source_ids_count": 1, "comments_shard_count": 1},
                "job_id": "22222222-2222-2222-2222-222222222222",
                "job_status": "failed",
                "items_found": 1121,
                "job_created_at": now,
                "job_started_at": now,
                "job_completed_at": now,
                "last_error_code": "instagram_comments_incomplete_retryable",
                "config": {
                    "target_source_ids": ["DPU4Pw6FU7N"],
                    "comments_shard_index": 1,
                    "comments_shard_count": 1,
                    "comments_shard_target_count": 1,
                },
                "metadata": {
                    "retry_rebalance": {"remaining_target_source_ids": ["DPU4Pw6FU7N"]},
                    "incomplete_fetch_reasons": {"DPU4Pw6FU7N": "pagination_deadline_exceeded"},
                    "comment_capture": {
                        "latest": {"stop_reason": "pagination_deadline_exceeded"},
                        "samples": [
                            {
                                "shortcode": "DPU4Pw6FU7N",
                                "stop_reason": "pagination_deadline_exceeded",
                            }
                        ],
                    },
                    "post_latency": {
                        "samples": [
                            {
                                "shortcode": "DPU4Pw6FU7N",
                                "reported_comment_count": 1851,
                                "stored_total_comments": 1121,
                                "observed_comment_count": 1121,
                            }
                        ]
                    },
                    "top_level_checkpoint_summary": {
                        "items": [
                            {
                                "target_shortcode": "DPU4Pw6FU7N",
                                "stop_reason": "pagination_deadline_exceeded",
                                "pages_seen": 18,
                            }
                        ]
                    },
                },
            }
        ],
    )

    assert payload["recommended_next_action"] == "retry_cursor_deadline_targets"
    assert payload["audit_cursor_recovery_target_count"] == 1
    assert payload["retry_progress"]["audit_cursor_recovery_target_source_ids"] == ["DPU4Pw6FU7N"]
    target_row = payload["target_progress_rows"][0]
    assert target_row["cursor_recovery_available"] is True
    assert target_row["retryable"] is True
    assert target_row["missing_comment_gap"] == 730


def test_comments_progress_payload_uses_database_counts_for_auth_blocked_target_rows() -> None:
    now = datetime(2026, 6, 22, tzinfo=timezone.utc)
    payload = _build_comments_scrape_run_progress_payload(
        platform="instagram",
        account_handle="bravotv",
        target_count_rows={
            "DNvplU4WKFt": {
                "reported_comment_count": 171,
                "saved_comment_count": 124,
                "missing_comment_gap": 47,
            }
        },
        rows=[
            {
                "run_id": "11111111-1111-1111-1111-111111111111",
                "run_status": "completed",
                "created_at": now,
                "started_at": now,
                "completed_at": now,
                "run_config": {"target_source_ids_count": 1, "comments_shard_count": 1},
                "job_id": "22222222-2222-2222-2222-222222222222",
                "job_status": "completed",
                "items_found": 73,
                "job_created_at": now,
                "job_started_at": now,
                "job_completed_at": now,
                "config": {
                    "target_source_ids": ["DNvplU4WKFt"],
                    "comments_shard_index": 1,
                    "comments_shard_count": 1,
                    "comments_shard_target_count": 1,
                },
                "metadata": {
                    "retry_rebalance": {"remaining_target_source_ids": ["DNvplU4WKFt"]},
                    "post_auth_failures": {
                        "target_source_ids": ["DNvplU4WKFt"],
                        "fetch_reasons": {"DNvplU4WKFt": "html_challenge_or_auth_required"},
                    },
                    "fetcher_runtime": {
                        "comments_auth_validation": {
                            "mode": "comments_endpoint",
                            "status": "auth_blocked",
                            "result": "auth_blocked",
                            "reason": "html_challenge_or_auth_required",
                            "retryable": False,
                        },
                    },
                    "comment_completeness": {
                        "complete_posts": 0,
                        "incomplete_posts": 1,
                        "completion_reasons": {"post_auth_failed_skipped": 1},
                    },
                    "post_latency": {
                        "samples": [
                            {
                                "shortcode": "DNvplU4WKFt",
                                "reported_comment_count": 171,
                                "saved_comment_count": 0,
                                "observed_comment_count": 0,
                            }
                        ]
                    },
                    "current_target_fetch": {
                        "shortcode": "DNvplU4WKFt",
                        "phase": "fetched",
                        "auth_failed": True,
                        "reported_comment_count": 171,
                        "observed_comment_count": 0,
                    },
                },
            }
        ],
    )

    assert payload["operational_state"] == "blocked_auth"
    target_row = payload["target_progress_rows"][0]
    assert target_row["saved_comment_count"] == 124
    assert target_row["saved_comment_count_source"] == "database"
    assert target_row["reported_comment_count"] == 171
    assert target_row["missing_comment_gap"] == 47
    assert target_row["auth_failed"] is True


def test_comments_progress_counts_use_rollup_not_live_comment_count(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    def _fetch_all(query, params=None, **_kwargs):
        calls.append({"query": query, "params": params})
        return [
            {
                "shortcode": "DVMdEy8AbLL",
                "post_id": "post-1",
                "reported_comment_count": 6274,
                "saved_comment_count": 1501,
                "missing_comment_gap": 4773,
            }
        ]

    monkeypatch.setattr(
        "trr_backend.socials.pipelines.comments.instagram.pg.fetch_all",
        _fetch_all,
    )

    rows = _instagram_comments_audit_cursor_counts_by_shortcode(
        shortcodes=["DVMdEy8AbLL"],
        active_run_id="11111111-1111-1111-1111-111111111111",
    )

    query = str(calls[0]["query"])
    assert "social.instagram_post_comment_rollups" in query
    assert "social.instagram_comments c" not in query
    assert rows["DVMdEy8AbLL"]["saved_comment_count"] == 1501
    assert rows["DVMdEy8AbLL"]["missing_comment_gap"] == 4773


def test_audit_cursor_retry_defaults_include_network_stops() -> None:
    stop_reasons = _normalize_instagram_comments_audit_retry_stop_reasons(None)

    assert "pagination_deadline_exceeded" in stop_reasons
    assert "pagination_page_cap_reached" in stop_reasons
    assert "network_stopped" in stop_reasons
    assert "static_cdn_budget_exhausted" in stop_reasons


def test_audit_cursor_recovery_show_filter_uses_saved_post_metadata(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    def _fetch_all(query, params=None, **_kwargs):
        calls.append({"query": query, "params": params})
        return []

    monkeypatch.setattr(
        "trr_backend.socials.pipelines.comments.instagram.pg.fetch_all",
        _fetch_all,
    )

    _load_instagram_comments_audit_cursor_rows(
        account_handle="bravotv",
        limit=5,
        show_ids=["show-1"],
        season_ids=["season-1"],
        show_filters=["Summer House"],
    )

    query = str(calls[0]["query"])
    params = calls[0]["params"]
    assert "left join social.instagram_posts p" in query
    assert "left join core.shows sh" in query
    assert "p.show_id::text = any" in query
    assert "p.season_id::text = any" in query
    assert "p.caption" in query
    assert ["show-1"] in params
    assert ["season-1"] in params
    assert "summerhouse" in next(item for item in params if isinstance(item, list) and "summerhouse" in item)


def test_audit_cursor_retry_attaches_to_active_run_when_worker_guard_blocks(monkeypatch) -> None:
    calls: list[dict[str, object]] = []

    monkeypatch.setattr(
        "trr_backend.socials.pipelines.comments.instagram.get_instagram_comments_audit_cursor_recovery",
        lambda **_kwargs: {
            "ok": True,
            "active_run": {"run_id": "11111111-1111-1111-1111-111111111111", "status": "running"},
            "selected_target_source_ids": ["C123"],
            "progress_rows": [{"shortcode": "C123", "missing_comment_gap": 42}],
        },
    )

    def _raise_worker_unavailable(*_args, **_kwargs):
        raise SocialWorkerUnavailableError(
            "Modal social dispatch is required for this social ingest job.",
            worker_health={"reason": "modal_resolution_failed"},
        )

    monkeypatch.setattr(
        "trr_backend.socials.pipelines.comments.instagram.start_social_account_comments_scrape",
        _raise_worker_unavailable,
    )

    def _split(**kwargs):
        calls.append(kwargs)
        return {
            "run_id": kwargs["run_id"],
            "created_target_job_ids": ["22222222-2222-2222-2222-222222222222"],
            "created_target_job_count": 1,
        }

    monkeypatch.setattr(
        "trr_backend.socials.pipelines.comments.instagram._split_instagram_comments_audit_cursor_targets_into_active_run",
        _split,
    )

    payload = enqueue_instagram_comments_audit_cursor_retries(
        account_handle="bravotv",
        limit=1,
        batch_size=1,
        dry_run=False,
    )

    assert payload["enqueue"]["performed"] is True
    assert payload["enqueue"]["mode"] == "active_run_split"
    assert calls == [
        {
            "run_id": "11111111-1111-1111-1111-111111111111",
            "account_handle": "bravotv",
            "target_source_ids": ["C123"],
            "batch_size": 1,
            "initiated_by": "audit-cursor-retry",
            "dispatch_immediately": True,
            "force_rerun_existing": False,
        }
    ]


def test_audit_cursor_split_creates_standalone_target_job_without_source_job(monkeypatch) -> None:
    created_jobs: list[dict[str, object]] = []
    dispatched: list[str] = []

    monkeypatch.setattr(
        "trr_backend.socials.pipelines.comments.instagram.pg.fetch_all",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        "trr_backend.socials.pipelines.comments.instagram.pg.fetch_one",
        lambda *_args, **_kwargs: {
            "run_id": "11111111-1111-1111-1111-111111111111",
            "source_scope": "network",
            "initiated_by": "admin",
            "source_priority": 108,
            "existing_job_count": 3,
            "run_config": {
                "platform": "instagram",
                "account": "bravotv",
                "source_scope": "network",
                "mode": "profile",
                "comments_load_strategy": "cursor_api",
                "comments_session_scope": "cursor_api_worker",
                "comments_shard_count": 3,
                "required_execution_backend": "modal",
            },
        },
    )

    def _create_job(_context, **kwargs):
        created_jobs.append(kwargs)
        return "22222222-2222-2222-2222-222222222222"

    monkeypatch.setattr(
        "trr_backend.socials.pipelines.comments.instagram._create_job",
        _create_job,
    )
    monkeypatch.setattr(
        "trr_backend.socials.pipelines.comments.instagram.dispatch_due_social_jobs",
        lambda *, run_id: dispatched.append(run_id),
    )

    payload = _split_instagram_comments_audit_cursor_targets_into_active_run(
        run_id="11111111-1111-1111-1111-111111111111",
        account_handle="bravotv",
        target_source_ids=["DTgXh94kXyo"],
        batch_size=1,
        initiated_by="audit-cursor-retry",
        dispatch_immediately=True,
    )

    assert payload["created_target_job_ids"] == ["22222222-2222-2222-2222-222222222222"]
    assert payload["pending_target_source_ids"] == []
    assert dispatched == ["11111111-1111-1111-1111-111111111111"]
    created_config = created_jobs[0]["config"]
    assert created_jobs[0]["priority"] == 104
    assert created_config["target_source_ids"] == ["DTgXh94kXyo"]
    assert created_config["comments_audit_cursor_retry"] is True
    assert created_config["comments_audit_cursor_retry_standalone"] is True
    assert created_config["comments_load_strategy"] == "instagram_comments_endpoint_cursor"
    assert created_config["comments_session_scope"] == "instagram_comments_endpoint_cursor_worker"
    assert created_config["comments_target_batch_size"] == 1
    assert created_config["max_comments_per_post"] == 0


def test_audit_cursor_split_force_rerun_replaces_existing_one_target_job(monkeypatch) -> None:
    created_jobs: list[dict[str, object]] = []
    fetch_one_calls: list[dict[str, object]] = []

    monkeypatch.setattr(
        "trr_backend.socials.pipelines.comments.instagram.pg.fetch_all",
        lambda *_args, **_kwargs: [
            {
                "run_id": "11111111-1111-1111-1111-111111111111",
                "source_scope": "network",
                "initiated_by": "admin",
                "job_id": "33333333-3333-3333-3333-333333333333",
                "status": "queued",
                "priority": 104,
                "target_count": 1,
                "matched_targets": ["DTgXh94kXyo"],
                "config": {
                    "platform": "instagram",
                    "account": "bravotv",
                    "source_scope": "network",
                    "target_source_ids": ["DTgXh94kXyo"],
                    "comments_load_strategy": "cursor_api",
                    "comments_session_scope": "cursor_api_worker",
                    "comments_audit_cursor_retry": True,
                },
                "metadata": {
                    "dispatch": {
                        "remote_invocation_id": "fc-pending",
                        "remote_invocation_status": "pending",
                    }
                },
            }
        ],
    )

    def _fetch_one(query, params=None, **_kwargs):
        fetch_one_calls.append({"query": query, "params": params})
        return {"id": "33333333-3333-3333-3333-333333333333"}

    monkeypatch.setattr(
        "trr_backend.socials.pipelines.comments.instagram.pg.fetch_one",
        _fetch_one,
    )

    def _create_job(_context, **kwargs):
        created_jobs.append(kwargs)
        return "44444444-4444-4444-4444-444444444444"

    monkeypatch.setattr(
        "trr_backend.socials.pipelines.comments.instagram._create_job",
        _create_job,
    )
    monkeypatch.setattr(
        "trr_backend.socials.pipelines.comments.instagram.dispatch_due_social_jobs",
        lambda *, run_id: None,
    )

    payload = _split_instagram_comments_audit_cursor_targets_into_active_run(
        run_id="11111111-1111-1111-1111-111111111111",
        account_handle="bravotv",
        target_source_ids=["DTgXh94kXyo"],
        batch_size=1,
        initiated_by="audit-cursor-retry",
        dispatch_immediately=True,
        force_rerun_existing=True,
    )

    assert payload["created_target_job_ids"] == ["44444444-4444-4444-4444-444444444444"]
    assert payload["cancelled_source_job_ids"] == ["33333333-3333-3333-3333-333333333333"]
    assert payload["force_rerun_existing"] is True
    assert created_jobs[0]["priority"] == 104
    assert created_jobs[0]["config"]["comments_audit_cursor_retry_force_rerun"] is True
    assert created_jobs[0]["config"]["comments_load_strategy"] == "instagram_comments_endpoint_cursor"
    assert created_jobs[0]["config"]["comments_session_scope"] == "instagram_comments_endpoint_cursor_worker"
    assert any(call["params"] and call["params"][-2] is True for call in fetch_one_calls)
