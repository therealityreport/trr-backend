"""Worker-cap ramp decouples active workers from job count (REVISED §4).

Covers the launch-time cap config, the pure aggregation + ramp-decision helpers,
and the DB-driven orchestrator (``_ramp_instagram_comments_worker_cap``) which
recomputes the public-blocked ratio from job metadata, persists a new cap with a
history entry, and refills queued jobs to the new cap.

Mirrors the SimpleNamespace / fake-pg style of ``test_public_blocked_pause.py``
so no live database is required.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

import trr_backend.socials.pipelines.comments.instagram as pipeline
from trr_backend.socials.pipelines.comments.instagram import (
    _aggregate_instagram_comments_public_blocked_from_jobs,
    _compute_instagram_comments_worker_cap_ramp,
    _instagram_comments_worker_cap_launch_config,
    _normalize_instagram_comments_worker_cap_config,
    _ramp_instagram_comments_worker_cap,
)


# --- launch-time cap config ------------------------------------------------


def test_launch_config_defaults_to_safe_pool_cap_for_public_runs():
    cfg = _instagram_comments_worker_cap_launch_config(
        public_mode=True,
        requested_comments_worker_count=None,
    )
    assert cfg["comments_worker_cap_current"] == 2
    assert cfg["comments_worker_cap_floor"] == 2
    assert cfg["comments_worker_cap_start"] == 2
    assert cfg["comments_worker_cap_steps"] == [3, 4]
    assert cfg["comments_worker_cap_ceiling"] == 4
    assert cfg["comments_worker_cap_pause_reason"] is None
    assert cfg["comments_worker_cap_history"] == []


def test_launch_config_keeps_explicit_worker_count_within_safe_start():
    cfg = _instagram_comments_worker_cap_launch_config(
        public_mode=True,
        requested_comments_worker_count=3,
    )
    assert cfg["comments_worker_cap_current"] == 2


def test_launch_config_does_not_start_above_start_for_large_request():
    cfg = _instagram_comments_worker_cap_launch_config(
        public_mode=True,
        requested_comments_worker_count=40,
    )
    # A larger explicit request never lifts the *starting* cap above 2; the ramp
    # is the only thing that raises the cap.
    assert cfg["comments_worker_cap_current"] == 2


def test_launch_config_empty_for_non_public_runs():
    assert (
        _instagram_comments_worker_cap_launch_config(
            public_mode=False,
            requested_comments_worker_count=None,
        )
        == {}
    )


def test_normalize_returns_none_without_cap_key():
    assert _normalize_instagram_comments_worker_cap_config({"account": "bravotv"}) is None


def test_normalize_fills_defaults_for_partial_config():
    normalized = _normalize_instagram_comments_worker_cap_config(
        {"comments_worker_cap_current": 15}
    )
    assert normalized is not None
    assert normalized["current"] == 4
    assert normalized["floor"] == 2
    assert normalized["ceiling"] == 4
    assert normalized["steps"] == [3, 4]


# --- aggregation from job metadata -----------------------------------------


def test_aggregate_sums_public_blocked_metadata_across_jobs():
    jobs = [
        {
            "metadata": {
                "public_blocked_checked_count": 20,
                "public_blocked_target_source_ids": ["a", "b"],
                "public_blocked_recovered_comments": 5,
            }
        },
        {
            "metadata": {
                "public_blocked_checked_count": 10,
                "public_blocked_target_source_ids": ["c"],
                "public_blocked_recovered_comments": 0,
            }
        },
    ]
    agg = _aggregate_instagram_comments_public_blocked_from_jobs(jobs)
    assert agg["checked"] == 30
    assert agg["blocked"] == 3
    assert agg["recovered_comments"] == 5
    assert agg["ratio"] == round(3 / 30, 4)
    assert agg["hard_block"] is False


def test_aggregate_flags_hard_block_from_fetch_reasons():
    jobs = [
        {
            "metadata": {
                "public_blocked_checked_count": 5,
                "public_blocked_target_source_ids": ["a"],
                "public_blocked_fetch_reasons": {
                    "a": "instagram_comments_endpoint_auth_blocked"
                },
            }
        }
    ]
    agg = _aggregate_instagram_comments_public_blocked_from_jobs(jobs)
    assert agg["hard_block"] is True


def test_aggregate_no_checked_sample_yields_none_ratio():
    agg = _aggregate_instagram_comments_public_blocked_from_jobs([{"metadata": {}}])
    assert agg["checked"] == 0
    assert agg["ratio"] is None


# --- pure ramp decision ----------------------------------------------------


def _cap(current: int) -> dict[str, Any]:
    return {
        "current": current,
        "floor": 2,
        "start": 2,
        "steps": [3, 4],
        "ceiling": 4,
        "history": [],
    }


def test_ramp_up_2_to_3_when_ratio_below_20pct():
    decision = _compute_instagram_comments_worker_cap_ramp(
        cap_config=_cap(2),
        public_blocked={"checked": 30, "ratio": 0.1, "hard_block": False},
    )
    assert decision["changed"] is True
    assert decision["next_cap"] == 3
    assert decision["reason"] == "public_blocked_ratio_low"


def test_ramp_up_3_to_4_when_ratio_below_20pct():
    decision = _compute_instagram_comments_worker_cap_ramp(
        cap_config=_cap(3),
        public_blocked={"checked": 30, "ratio": 0.05, "hard_block": False},
    )
    assert decision["changed"] is True
    assert decision["next_cap"] == 4


def test_ramp_holds_at_ceiling():
    decision = _compute_instagram_comments_worker_cap_ramp(
        cap_config=_cap(4),
        public_blocked={"checked": 30, "ratio": 0.05, "hard_block": False},
    )
    assert decision["changed"] is False
    assert decision["next_cap"] == 4


def test_ramp_does_not_raise_when_ratio_at_or_above_20pct():
    decision = _compute_instagram_comments_worker_cap_ramp(
        cap_config=_cap(2),
        public_blocked={"checked": 30, "ratio": 0.2, "hard_block": False},
    )
    assert decision["changed"] is False
    assert decision["next_cap"] == 2


def test_ramp_does_not_raise_without_checked_sample():
    decision = _compute_instagram_comments_worker_cap_ramp(
        cap_config=_cap(2),
        public_blocked={"checked": 0, "ratio": None, "hard_block": False},
    )
    assert decision["changed"] is False
    assert decision["next_cap"] == 2


def test_ramp_down_to_floor_when_ratio_at_or_above_50pct():
    decision = _compute_instagram_comments_worker_cap_ramp(
        cap_config=_cap(4),
        public_blocked={"checked": 30, "ratio": 0.5, "hard_block": False},
    )
    assert decision["changed"] is True
    assert decision["next_cap"] == 2
    assert decision["reason"] == "public_blocked_ratio_high"


def test_ramp_down_to_floor_on_hard_block_even_with_low_ratio():
    decision = _compute_instagram_comments_worker_cap_ramp(
        cap_config=_cap(4),
        public_blocked={"checked": 30, "ratio": 0.0, "hard_block": True},
    )
    assert decision["changed"] is True
    assert decision["next_cap"] == 2
    assert decision["reason"] == "hard_block"


def test_ramp_down_holds_when_already_at_floor():
    decision = _compute_instagram_comments_worker_cap_ramp(
        cap_config=_cap(2),
        public_blocked={"checked": 30, "ratio": 0.9, "hard_block": False},
    )
    assert decision["changed"] is False
    assert decision["next_cap"] == 2


# --- DB-driven orchestrator -------------------------------------------------


class _FakePg:
    """Minimal fake for the module-level ``pg`` used by the orchestrator."""

    def __init__(self, *, run_config: dict[str, Any], job_rows: list[dict[str, Any]]) -> None:
        self._run_config = run_config
        self._job_rows = job_rows

    def fetch_one(self, _sql: str, _params: list[Any]):
        return {"config": self._run_config}

    def fetch_all(self, _sql: str, _params: list[Any]):
        return list(self._job_rows)


def _patch_orchestrator(
    monkeypatch: pytest.MonkeyPatch,
    *,
    run_config: dict[str, Any],
    job_rows: list[dict[str, Any]],
) -> dict[str, list[Any]]:
    """Patch pg + side-effecting collaborators; return a calls recorder."""
    calls: dict[str, list[Any]] = {"merge": [], "dispatch": [], "rebalance": []}

    monkeypatch.setattr(
        pipeline,
        "pg",
        _FakePg(run_config=run_config, job_rows=job_rows),
    )

    def fake_merge(*, run_id: str, metadata_updates: dict[str, Any], conn: Any = None):
        calls["merge"].append({"run_id": run_id, "metadata_updates": metadata_updates})
        return {"id": run_id, "status": "running", "config": dict(metadata_updates)}

    def fake_dispatch(*, run_id: str | None = None, limit: int | None = None):
        calls["dispatch"].append({"run_id": run_id, "limit": limit})
        return {"dispatched_job_ids": []}

    def fake_rebalance(**kwargs: Any):
        calls["rebalance"].append(kwargs)
        return {"created_job_ids": []}

    monkeypatch.setattr(pipeline, "_merge_catalog_run_config", fake_merge)
    monkeypatch.setattr(pipeline, "dispatch_due_social_jobs", fake_dispatch)
    monkeypatch.setattr(pipeline, "rebalance_slow_instagram_comments_shards", fake_rebalance)
    return calls


def test_orchestrator_ramps_up_persists_history_and_refills(monkeypatch: pytest.MonkeyPatch):
    run_config = {
        "comments_worker_cap_current": 2,
        "comments_worker_cap_floor": 2,
        "comments_worker_cap_start": 2,
        "comments_worker_cap_steps": [3, 4],
        "comments_worker_cap_ceiling": 4,
        "comments_worker_cap_history": [],
    }
    job_rows = [
        {
            "status": "running",
            "metadata": {
                "public_blocked_checked_count": 30,
                "public_blocked_target_source_ids": ["a", "b"],  # 2/30 ~ 6.7%
                "public_blocked_recovered_comments": 12,
            },
        },
    ]
    calls = _patch_orchestrator(monkeypatch, run_config=run_config, job_rows=job_rows)

    result = _ramp_instagram_comments_worker_cap(run_id="run-1", dispatch_immediately=True)

    assert result["changed"] is True
    assert result["cap"] == 3
    assert result["previous_cap"] == 2

    # Cap + history persisted via _merge_catalog_run_config.
    assert len(calls["merge"]) == 1
    updates = calls["merge"][0]["metadata_updates"]
    assert updates["comments_worker_cap_current"] == 3
    assert updates["comments_worker_cap_pause_reason"] is None
    history = updates["comments_worker_cap_history"]
    assert len(history) == 1
    assert history[0]["from"] == 2
    assert history[0]["to"] == 3
    assert history[0]["reason"] == "public_blocked_ratio_low"

    # Refill bounded to cap(3) - active(1 running) = 2 headroom.
    assert len(calls["dispatch"]) == 1
    assert calls["dispatch"][0]["run_id"] == "run-1"
    assert calls["dispatch"][0]["limit"] == 2
    # Slow-shard rebalance reused with public-run arguments.
    assert len(calls["rebalance"]) == 1
    rb = calls["rebalance"][0]
    assert rb["run_id"] == "run-1"
    assert rb["slow_elapsed_seconds"] == 240
    assert rb["slow_posts_per_minute"] == 0.5
    assert rb["min_remaining_targets"] == 10
    assert rb["max_retry_shard_size"] == 10


def test_orchestrator_ramps_down_to_floor_on_high_ratio(monkeypatch: pytest.MonkeyPatch):
    run_config = {
        "comments_worker_cap_current": 4,
        "comments_worker_cap_floor": 2,
        "comments_worker_cap_start": 2,
        "comments_worker_cap_steps": [3, 4],
        "comments_worker_cap_ceiling": 4,
        "comments_worker_cap_history": [],
    }
    job_rows = [
        {
            "status": "running",
            "metadata": {
                "public_blocked_checked_count": 30,
                "public_blocked_target_source_ids": [f"s{i}" for i in range(18)],  # 18/30 = 60%
                "public_blocked_recovered_comments": 1,
            },
        }
    ]
    calls = _patch_orchestrator(monkeypatch, run_config=run_config, job_rows=job_rows)

    result = _ramp_instagram_comments_worker_cap(run_id="run-1", dispatch_immediately=True)

    assert result["changed"] is True
    assert result["cap"] == 2
    updates = calls["merge"][0]["metadata_updates"]
    assert updates["comments_worker_cap_current"] == 2
    assert updates["comments_worker_cap_pause_reason"] == "public_blocked_ratio_high"


def test_orchestrator_noop_when_no_cap_config(monkeypatch: pytest.MonkeyPatch):
    # Non-public run (no cap config) => no mutation, no dispatch.
    calls = _patch_orchestrator(
        monkeypatch,
        run_config={"account": "bravotv"},
        job_rows=[],
    )
    result = _ramp_instagram_comments_worker_cap(run_id="run-1")
    assert result["changed"] is False
    assert result["reason"] == "worker_cap_not_configured"
    assert calls["merge"] == []
    assert calls["dispatch"] == []
    assert calls["rebalance"] == []


def test_orchestrator_noop_without_run_id(monkeypatch: pytest.MonkeyPatch):
    calls = _patch_orchestrator(monkeypatch, run_config={}, job_rows=[])
    result = _ramp_instagram_comments_worker_cap(run_id="")
    assert result["changed"] is False
    assert calls["merge"] == []


def test_orchestrator_holds_without_change(monkeypatch: pytest.MonkeyPatch):
    # ratio in the dead band (20% <= ratio < 50%) => hold, no persistence.
    run_config = {
        "comments_worker_cap_current": 3,
        "comments_worker_cap_floor": 2,
        "comments_worker_cap_start": 2,
        "comments_worker_cap_steps": [3, 4],
        "comments_worker_cap_ceiling": 4,
        "comments_worker_cap_history": [],
    }
    job_rows = [
        {
            "status": "running",
            "metadata": {
                "public_blocked_checked_count": 30,
                "public_blocked_target_source_ids": [f"s{i}" for i in range(9)],  # 30%
            },
        }
    ]
    calls = _patch_orchestrator(monkeypatch, run_config=run_config, job_rows=job_rows)

    result = _ramp_instagram_comments_worker_cap(run_id="run-1")
    assert result["changed"] is False
    assert calls["merge"] == []
    assert calls["dispatch"] == []
