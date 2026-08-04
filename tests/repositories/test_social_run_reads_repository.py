"""Focused run-read repository tests for extracted control-plane seams."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, cast

import pytest

import trr_backend.repositories.social_season_analytics as social_repo
import trr_backend.socials.control_plane.run_reads as run_reads


def test_legacy_list_runs_delegates_to_control_plane_run_reads(monkeypatch: pytest.MonkeyPatch) -> None:
    expected = [{"id": "run-1"}]

    monkeypatch.setattr(run_reads, "list_runs", lambda *_args, **_kwargs: expected)

    payload = social_repo.list_runs("season-1")

    assert payload is expected


def test_legacy_list_run_summaries_delegates_to_control_plane_run_reads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = [{"id": "run-1", "status": "running"}]

    monkeypatch.setattr(run_reads, "list_run_summaries", lambda *_args, **_kwargs: expected)

    payload = cast(Any, social_repo).list_run_summaries("season-1")

    assert payload is expected


def test_legacy_get_run_progress_snapshot_delegates_to_control_plane_run_reads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = {"run_id": "run-1", "run_status": "running"}

    monkeypatch.setattr(run_reads, "get_run_progress_snapshot", lambda *_args, **_kwargs: expected)

    payload = social_repo.get_run_progress_snapshot("season-1", "run-1")

    assert payload is expected


def test_list_runs_applies_filters_and_order(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def _fake_fetch_all(sql: str, params: list[object]) -> list[dict[str, object]]:
        captured["sql"] = sql
        captured["params"] = params
        return [
            {
                "id": "run-1",
                "status": "running",
                "config": {
                    "execution_owner": "remote_worker",
                    "execution_mode_canonical": "remote",
                    "client_workflow_id": "social-orch-demo",
                    "orchestration_scope": "single_week_single_platform",
                    "orchestration_slot_key": "week:2|platform:instagram",
                    "orchestration_position": 1,
                    "orchestration_total_runs": 1,
                    "orchestration_week_index": 2,
                    "orchestration_platform": "instagram",
                },
                "summary": {
                    "total_jobs": 12,
                    "completed_jobs": 0,
                    "failed_jobs": 2,
                    "active_jobs": 5,
                },
            }
        ]

    monkeypatch.setattr(social_repo.pg, "fetch_all", _fake_fetch_all)

    rows = social_repo.list_runs(
        "season-1",
        limit=25,
        status="completed",
        source_scope="bravo",
        run_id="123e4567-e89b-12d3-a456-426614174000",
    )

    assert rows == [
        {
            "id": "run-1",
            "status": "running",
            "config": {
                "execution_owner": "remote_worker",
                "execution_mode_canonical": "remote",
                "client_workflow_id": "social-orch-demo",
                "orchestration_scope": "single_week_single_platform",
                "orchestration_slot_key": "week:2|platform:instagram",
                "orchestration_position": 1,
                "orchestration_total_runs": 1,
                "orchestration_week_index": 2,
                "orchestration_platform": "instagram",
            },
            "summary": {
                "total_jobs": 12,
                "completed_jobs": 0,
                "failed_jobs": 2,
                "active_jobs": 5,
            },
            "summary_normalized": {
                "total_jobs": 12,
                "completed_jobs": 5,
                "failed_jobs": 2,
                "active_jobs": 5,
            },
            "execution_owner": "remote_worker",
            "execution_mode_canonical": "remote",
            "execution_backend_canonical": None,
            "ingest_mode": "legacy_season_targeted",
            "orchestration_id": "social-orch-demo",
            "orchestration_scope": "single_week_single_platform",
            "orchestration_slot_key": "week:2|platform:instagram",
            "orchestration_position": 1,
            "orchestration_total_runs": 1,
            "orchestration_week_index": 2,
            "orchestration_platform": "instagram",
        }
    ]
    sql = str(captured["sql"]).lower()
    params = captured["params"]
    assert "from social.scrape_runs" in sql
    assert "where season_id = %s" in sql
    assert "summary" in sql
    assert "created_at" in sql
    assert "started_at" in sql
    assert "completed_at" in sql
    assert "and status = %s" in sql
    assert "and source_scope = %s" in sql
    assert "and id = %s::uuid" in sql
    assert "order by created_at desc limit %s" in sql
    assert params == ["season-1", "completed", "network", "123e4567-e89b-12d3-a456-426614174000", 25]


def test_list_runs_filters_by_scope_config_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    def _fake_fetch_all(_sql: str, _params: list[object]) -> list[dict[str, object]]:
        return [
            {
                "id": "run-match",
                "status": "running",
                "config": {
                    "platforms": ["twitter"],
                    "week_index": 2,
                    "date_start": "2025-08-14T04:00:00+00:00",
                    "date_end": "2025-09-16T23:59:59.999999+00:00",
                },
                "summary": {"total_jobs": 4, "completed_jobs": 1, "failed_jobs": 0, "active_jobs": 3},
            },
            {
                "id": "run-miss-platform",
                "status": "running",
                "config": {
                    "platforms": ["instagram"],
                    "week_index": 2,
                    "date_start": "2025-08-14T04:00:00+00:00",
                    "date_end": "2025-09-16T23:59:59.999999+00:00",
                },
                "summary": {"total_jobs": 4, "completed_jobs": 1, "failed_jobs": 0, "active_jobs": 3},
            },
            {
                "id": "run-miss-window",
                "status": "running",
                "config": {
                    "platforms": ["twitter"],
                    "week_index": 3,
                    "date_start": "2025-08-21T04:00:00+00:00",
                    "date_end": "2025-09-23T23:59:59.999999+00:00",
                },
                "summary": {"total_jobs": 4, "completed_jobs": 1, "failed_jobs": 0, "active_jobs": 3},
            },
        ]

    monkeypatch.setattr(social_repo.pg, "fetch_all", _fake_fetch_all)

    rows = social_repo.list_runs(
        "season-1",
        platforms=["twitter"],
        week_index=2,
        date_start=datetime(2025, 8, 14, 4, 0, tzinfo=UTC),
        date_end=datetime(2025, 9, 16, 23, 59, 59, 999999, tzinfo=UTC),
    )

    assert [row["id"] for row in rows] == ["run-match"]


def test_get_run_progress_snapshot_includes_dynamic_stages_and_per_handle(monkeypatch: pytest.MonkeyPatch) -> None:
    season_id = "11111111-1111-1111-1111-111111111111"
    run_id = "22222222-2222-2222-2222-222222222222"
    run_row = {
        "run_id": run_id,
        "season_id": season_id,
        "status": "running",
        "source_scope": "bravo",
        "config": {"runner_count": 2, "runner_strategy": "adaptive_dual_runner"},
        "summary": {"total_jobs": 4},
        "created_at": datetime(2026, 3, 5, 15, 0, tzinfo=UTC),
        "started_at": datetime(2026, 3, 5, 15, 1, tzinfo=UTC),
        "completed_at": None,
    }
    job_rows = [
        {
            "id": "job-posts",
            "platform": "instagram",
            "job_type": "posts",
            "status": "running",
            "items_found": 12,
            "error_message": None,
            "created_at": datetime(2026, 3, 5, 15, 1, tzinfo=UTC),
            "started_at": datetime(2026, 3, 5, 15, 2, tzinfo=UTC),
            "completed_at": None,
            "config": {"account": "@bravotv", "stage": "posts", "runner_lane": "A"},
            "metadata": {"stage_counters": {"posts": 12}, "persist_counters": {"posts_upserted": 4}},
            "worker_id": "social-worker:a",
        },
        {
            "id": "job-cmm",
            "platform": "instagram",
            "job_type": "comment_media_mirror",
            "status": "completed",
            "items_found": 3,
            "error_message": None,
            "created_at": datetime(2026, 3, 5, 15, 3, tzinfo=UTC),
            "started_at": datetime(2026, 3, 5, 15, 3, tzinfo=UTC),
            "completed_at": datetime(2026, 3, 5, 15, 4, tzinfo=UTC),
            "config": {"account": "@bravowwhl", "runner_lane": "B"},
            "metadata": {"stage": "comment_media_mirror", "persist_counters": {"comments_upserted": 2}},
            "worker_id": "social-worker:b",
        },
        {
            "id": "job-other",
            "platform": "instagram",
            "job_type": "media_enrich",
            "status": "failed",
            "items_found": 1,
            "error_message": "boom",
            "created_at": datetime(2026, 3, 5, 15, 5, tzinfo=UTC),
            "started_at": datetime(2026, 3, 5, 15, 5, tzinfo=UTC),
            "completed_at": datetime(2026, 3, 5, 15, 6, tzinfo=UTC),
            "config": {"account": "@bravowwhl"},
            "metadata": {"activity": {"phase": "enriching"}},
            "worker_id": "social-worker:c",
        },
    ]
    seen_fetch_one_pools: list[str] = []

    monkeypatch.setattr(social_repo, "_relation_exists", lambda _name: True)
    monkeypatch.setattr(social_repo, "_scrape_jobs_features", lambda: {"has_run_id": True, "has_queue_fields": True})

    def _fake_fetch_one(*_args, **kwargs):
        seen_fetch_one_pools.append(kwargs.get("pool_name"))
        return run_row

    monkeypatch.setattr(social_repo.pg, "fetch_one", _fake_fetch_one)
    monkeypatch.setattr(social_repo.pg, "fetch_all", lambda *_args, **_kwargs: job_rows)

    payload = social_repo.get_run_progress_snapshot(season_id, run_id, recent_log_limit=10)

    assert seen_fetch_one_pools[0] == "social_control"
    assert payload["run_id"] == run_id
    assert payload["stages"]["posts"]["jobs_total"] == 1
    assert payload["stages"]["posts"]["jobs_running"] == 1
    assert payload["stages"]["posts"]["jobs_waiting"] == 0
    assert payload["stages"]["comment_media_mirror"]["jobs_total"] == 1
    assert payload["stages"]["other"]["jobs_total"] == 1
    assert payload["worker_runtime"]["active_workers_now"] == 1
    assert payload["worker_runtime"]["scheduler_lanes"] == ["A", "B"]
    per_handle_keys = {(row["platform"], row["account_handle"], row["stage"]) for row in payload["per_handle"]}
    assert ("instagram", "bravotv", "posts") in per_handle_keys
    assert ("instagram", "bravowwhl", "comment_media_mirror") in per_handle_keys
    assert ("instagram", "bravowwhl", "media_enrich") in per_handle_keys
    posts_handle = next(
        row for row in payload["per_handle"] if row["platform"] == "instagram" and row["account_handle"] == "bravotv"
    )
    assert posts_handle["jobs_running"] == 1
    assert posts_handle["jobs_waiting"] == 0
    assert posts_handle["runner_lanes"] == ["A"]
    assert posts_handle["has_started"] is True


def test_get_run_progress_snapshot_counts_only_running_workers_as_active(monkeypatch: pytest.MonkeyPatch) -> None:
    season_id = "11111111-1111-1111-1111-111111111111"
    run_id = "22222222-2222-2222-2222-222222222222"
    run_row = {
        "run_id": run_id,
        "season_id": season_id,
        "status": "running",
        "source_scope": "bravo",
        "config": {"runner_count": 2, "runner_strategy": "adaptive_dual_runner"},
        "summary": {"total_jobs": 2},
        "created_at": datetime(2026, 3, 5, 15, 0, tzinfo=UTC),
        "started_at": datetime(2026, 3, 5, 15, 1, tzinfo=UTC),
        "completed_at": None,
    }
    job_rows = [
        {
            "id": "job-running",
            "platform": "twitter",
            "job_type": "posts",
            "status": "running",
            "items_found": 2,
            "error_message": None,
            "created_at": datetime(2026, 3, 5, 15, 1, tzinfo=UTC),
            "started_at": datetime(2026, 3, 5, 15, 2, tzinfo=UTC),
            "completed_at": None,
            "config": {"account": "@bravotv", "stage": "posts"},
            "metadata": {},
            "worker_id": "social-worker:running",
        },
        {
            "id": "job-queued",
            "platform": "twitter",
            "job_type": "posts",
            "status": "queued",
            "items_found": 0,
            "error_message": None,
            "created_at": datetime(2026, 3, 5, 15, 3, tzinfo=UTC),
            "started_at": None,
            "completed_at": None,
            "config": {"account": "@bravowwhl", "stage": "posts"},
            "metadata": {},
            "worker_id": "social-worker:stale-claim",
        },
    ]

    monkeypatch.setattr(social_repo, "_relation_exists", lambda _name: True)
    monkeypatch.setattr(social_repo, "_scrape_jobs_features", lambda: {"has_run_id": True, "has_queue_fields": True})
    monkeypatch.setattr(social_repo.pg, "fetch_one", lambda *_args, **_kwargs: run_row)
    monkeypatch.setattr(social_repo.pg, "fetch_all", lambda *_args, **_kwargs: job_rows)

    payload = social_repo.get_run_progress_snapshot(season_id, run_id, recent_log_limit=10)

    assert payload["worker_runtime"]["active_workers_now"] == 1


def test_get_run_progress_snapshot_finalizes_stale_running_run_when_jobs_are_terminal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    season_id = "11111111-1111-1111-1111-111111111111"
    run_id = "22222222-2222-2222-2222-222222222222"
    initial_run_row = {
        "run_id": run_id,
        "season_id": season_id,
        "status": "running",
        "source_scope": "bravo",
        "config": {},
        "summary": {"total_jobs": 1, "completed_jobs": 0, "failed_jobs": 0, "active_jobs": 1},
        "created_at": datetime(2026, 3, 5, 15, 0, tzinfo=UTC),
        "started_at": datetime(2026, 3, 5, 15, 1, tzinfo=UTC),
        "completed_at": None,
    }
    refreshed_run_row = dict(initial_run_row)
    refreshed_run_row["status"] = "completed"
    refreshed_run_row["summary"] = {"total_jobs": 1, "completed_jobs": 1, "failed_jobs": 0, "active_jobs": 0}

    run_fetches = {"count": 0}

    def _fake_fetch_one(*_args, **_kwargs):
        run_fetches["count"] += 1
        return initial_run_row if run_fetches["count"] == 1 else refreshed_run_row

    finalized: list[tuple[str, bool]] = []

    monkeypatch.setattr(social_repo, "_relation_exists", lambda _name: True)
    monkeypatch.setattr(social_repo, "_scrape_jobs_features", lambda: {"has_run_id": True, "has_queue_fields": True})
    monkeypatch.setattr(social_repo.pg, "fetch_one", _fake_fetch_one)
    monkeypatch.setattr(
        social_repo.pg,
        "fetch_all",
        lambda *_args, **_kwargs: [
            {
                "id": "job-1",
                "platform": "instagram",
                "job_type": "posts",
                "status": "completed",
                "items_found": 2,
                "error_message": None,
                "created_at": datetime(2026, 3, 5, 15, 1, tzinfo=UTC),
                "started_at": datetime(2026, 3, 5, 15, 2, tzinfo=UTC),
                "completed_at": datetime(2026, 3, 5, 15, 3, tzinfo=UTC),
                "config": {"account": "@bravotv"},
                "metadata": {},
                "worker_id": None,
                "last_error_code": None,
            }
        ],
    )
    monkeypatch.setattr(
        social_repo,
        "_finalize_run_status",
        lambda run_id_arg, force_recompute=False: finalized.append((run_id_arg, force_recompute)),
    )
    monkeypatch.setattr(social_repo, "_update_run_summary", lambda *_args, **_kwargs: None)

    payload = social_repo.get_run_progress_snapshot(season_id, run_id, recent_log_limit=10)

    assert finalized == [(run_id, True)]
    assert payload["run_status"] == "completed"
