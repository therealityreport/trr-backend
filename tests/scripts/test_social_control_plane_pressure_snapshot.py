from __future__ import annotations

from dataclasses import dataclass

import pytest

from scripts.db import social_control_plane_pressure_snapshot as subject


@dataclass
class FakePg:
    def fetch_all(self, query: str, params: list[object]) -> list[dict[str, object]]:
        normalized = " ".join(query.lower().split())
        if "from pg_stat_activity" in normalized:
            return [
                {
                    "state": "active",
                    "count": 2,
                    "waiting_count": 1,
                    "advisory_lock_query_count": 0,
                    "max_age_seconds": 12,
                },
                {
                    "state": "idle",
                    "count": 1,
                    "waiting_count": 0,
                    "advisory_lock_query_count": 1,
                    "max_age_seconds": 1200,
                },
            ]
        if "from social.scrape_jobs" in normalized:
            assert params == [["cancelling", "pending", "queued", "retrying", "running"]]
            return [
                {
                    "id": "job-1",
                    "run_id": "run-1",
                    "status": "running",
                    "job_type": "shared_account_posts",
                }
            ]
        raise AssertionError(f"unexpected query: {query}")


def test_build_pressure_snapshot_shape_and_stale_advisory_count(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_pg = FakePg()
    monkeypatch.setattr(subject, "pg", fake_pg)
    monkeypatch.setattr(
        subject,
        "find_stale_advisory_sessions",
        lambda min_age_minutes, allowed_lock_keys: [
            {"pid": 101, "lock_key": 9001, "query": "select pg_try_advisory_lock(9001)"}
        ],
    )

    snapshot = subject.build_pressure_snapshot(min_age_minutes=30, allowed_lock_keys=[9001])

    assert snapshot["db_activity"] == {
        "total_sessions": 3,
        "waiting_sessions": 1,
        "advisory_lock_query_sessions": 1,
        "by_state": {
            "active": {
                "state": "active",
                "count": 2,
                "waiting_count": 1,
                "advisory_lock_query_count": 0,
                "max_age_seconds": 12,
            },
            "idle": {
                "state": "idle",
                "count": 1,
                "waiting_count": 0,
                "advisory_lock_query_count": 1,
                "max_age_seconds": 1200,
            },
        },
    }
    assert snapshot["social_jobs"] == [
        {
            "id": "job-1",
            "run_id": "run-1",
            "status": "running",
            "job_type": "shared_account_posts",
        }
    ]
    assert snapshot["stale_advisory_session_count"] == 1
    assert snapshot["stale_advisory_sessions"] == [
        {"pid": 101, "lock_key": 9001, "query": "select pg_try_advisory_lock(9001)"}
    ]
