"""Focused shared-status read tests for extracted control-plane seams."""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

import pytest

import trr_backend.repositories.social_season_analytics as social_repo
import trr_backend.socials.control_plane.run_lifecycle as run_lifecycle
import trr_backend.socials.control_plane.shared_accounts as shared_reads
from trr_backend.socials.control_plane import SeasonContext


def test_legacy_get_season_shared_status_delegates_to_control_plane_shared_accounts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = {"season_id": "season-1", "matched_posts": 4}

    monkeypatch.setattr(shared_reads, "get_season_shared_status", lambda *_args, **_kwargs: expected)

    payload = social_repo.get_season_shared_status("season-1")

    assert payload is expected


def test_legacy_list_shared_runs_delegates_to_control_plane_shared_accounts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = [{"id": "shared-run-1"}]

    monkeypatch.setattr(shared_reads, "list_shared_runs", lambda **_kwargs: expected)

    payload = social_repo.list_shared_runs()

    assert payload is expected


def test_get_season_shared_status_summarizes_stage_buckets(monkeypatch: pytest.MonkeyPatch) -> None:
    seen_fetch_all_pools: list[str] = []
    seen_fetch_one_pools: list[str] = []
    season_context = SeasonContext(
        season_id="season-1",
        show_id="show-1",
        show_name="Test Show",
        season_number=6,
        anchor_date=date(2025, 1, 7),
    )
    monkeypatch.setattr(social_repo, "get_season_context", lambda season_id: season_context)

    def _fake_fetch_all(sql: str, params: list[Any], *, pool_name: str = "default") -> list[dict[str, Any]]:
        del params
        seen_fetch_all_pools.append(pool_name)
        if "from social.shared_post_matches" in sql:
            return [
                {
                    "status": "matched",
                    "source_id": "ig-1",
                    "updated_at": datetime(2025, 1, 10, tzinfo=UTC),
                    "metadata": {},
                }
            ]
        raise AssertionError(f"Unexpected fetch_all SQL: {sql}")

    def _fake_fetch_one(
        sql: str,
        params: list[Any],
        *,
        pool_name: str = "default",
    ) -> dict[str, Any] | None:
        seen_fetch_one_pools.append(pool_name)
        if "from social.shared_post_review_queue" in sql and "count(*)::int as count" in sql:
            return {"count": 2}
        if "status = 'unmatched'" in sql:
            return {"count": 7}
        if "from social.scrape_runs r" in sql:
            return {
                "id": "shared-run-1",
                "status": "running",
                "config": {"pipeline_ingest_mode": "shared_account_async"},
                "summary": {
                    "stage_counts": {
                        social_repo.SHARED_ACCOUNT_POSTS_STAGE: {"total": 3, "completed": 1, "failed": 0, "active": 2},
                        social_repo.POST_CLASSIFY_STAGE: {"total": 3, "completed": 3, "failed": 0, "active": 0},
                        social_repo.SEASON_MATERIALIZE_STAGE: {"total": 1, "completed": 0, "failed": 0, "active": 0},
                    }
                },
                "created_at": datetime(2025, 1, 10, 15, 0, tzinfo=UTC),
                "started_at": datetime(2025, 1, 10, 15, 1, tzinfo=UTC),
                "completed_at": None,
            }
        raise AssertionError(f"Unexpected fetch_one SQL: {sql} params={params}")

    monkeypatch.setattr(social_repo.pg, "fetch_all", _fake_fetch_all)
    monkeypatch.setattr(social_repo.pg, "fetch_one", _fake_fetch_one)

    payload = social_repo.get_season_shared_status("season-1", source_scope="bravo")

    assert payload["matched_posts"] == 1
    assert payload["review_queue_count"] == 2
    assert payload["retained_unassigned_count"] == 7
    assert payload["shared_scrape_status"]["status"] == "running"
    assert payload["classification_status"]["status"] == "complete"
    assert payload["materialization_status"]["status"] == "queued"
    assert seen_fetch_all_pools == [run_lifecycle.SOCIAL_CONTROL_POOL_NAME]
    assert seen_fetch_one_pools == [run_lifecycle.SOCIAL_CONTROL_POOL_NAME] * 3


def test_list_shared_runs_normalizes_execution_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}
    seen_fetch_all_pools: list[str] = []

    def _fake_fetch_all(
        sql: str,
        params: list[object],
        *,
        pool_name: str = "default",
    ) -> list[dict[str, object]]:
        captured["sql"] = sql
        captured["params"] = params
        seen_fetch_all_pools.append(pool_name)
        return [
            {
                "id": "shared-run-1",
                "season_id": "season-1",
                "show_id": "show-1",
                "source_scope": "bravo",
                "status": "running",
                "config": {
                    "pipeline_ingest_mode": social_repo.SHARED_ACCOUNT_ASYNC_INGEST_MODE,
                    "execution_owner": "remote_worker",
                    "execution_mode_canonical": "remote",
                    "execution_backend_canonical": "modal",
                },
                "summary": {},
                "initiated_by": "admin@example.com",
                "created_at": None,
                "started_at": None,
                "completed_at": None,
                "cancelled_at": None,
            }
        ]

    monkeypatch.setattr(social_repo.pg, "fetch_all", _fake_fetch_all)

    payload = social_repo.list_shared_runs(limit=25, status="running", source_scope="bravo", run_id=None)

    assert payload[0]["execution_owner"] == "remote_worker"
    assert payload[0]["execution_mode_canonical"] == "remote"
    assert payload[0]["execution_backend_canonical"] == "modal"
    assert payload[0]["ingest_mode"] == social_repo.SHARED_ACCOUNT_ASYNC_INGEST_MODE
    assert captured["params"] == [social_repo.SHARED_ACCOUNT_ASYNC_INGEST_MODE, "running", "network", 25]
    assert seen_fetch_all_pools == [run_lifecycle.SOCIAL_CONTROL_POOL_NAME]
