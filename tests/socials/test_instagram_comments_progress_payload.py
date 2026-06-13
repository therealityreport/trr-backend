from __future__ import annotations

from datetime import datetime, timezone

from trr_backend.socials.pipelines.comments.instagram import (
    SocialWorkerUnavailableError,
    _build_comments_scrape_run_progress_payload,
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


def test_audit_cursor_retry_defaults_include_network_stops() -> None:
    stop_reasons = _normalize_instagram_comments_audit_retry_stop_reasons(None)

    assert "pagination_deadline_exceeded" in stop_reasons
    assert "pagination_page_cap_reached" in stop_reasons
    assert "network_stopped" in stop_reasons
    assert "static_cdn_budget_exhausted" in stop_reasons


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
    assert created_config["comments_target_batch_size"] == 1
    assert created_config["max_comments_per_post"] == 0
