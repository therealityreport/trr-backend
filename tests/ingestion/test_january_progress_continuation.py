"""A worker failure cannot buy another comment stall interval on continuation."""

import json
from datetime import UTC, datetime, timedelta

import pytest

from scripts.ingestion import recover_january_window as operator
from scripts.ingestion.january_progress import UsefulProgressTracker
from scripts.ingestion.recover_january_window import INTENT, RUN, continue_comment_progress
from trr_backend.ingestion.bounded_recovery import USEFUL_PROGRESS_POLICY


@pytest.fixture(autouse=True)
def no_live_phase_queries(monkeypatch):
    monkeypatch.setattr(operator, "recorded_recovery_phase", lambda _: "comments")


def prior(directory, *, age=1200, media=False, held_run=RUN):
    wall = datetime(2026, 10, 27, 8, tzinfo=UTC)
    started = wall - timedelta(hours=1)
    window = {
        "run_id": RUN,
        "account": "bravotv",
        "expected_intent_hash": INTENT,
        "generation": 5,
        "window_id": "old-window",
        "started_at": started.isoformat(),
        "stop_policy": USEFUL_PROGRESS_POLICY,
        "source_revision": "original-source",
    }
    held = {
        "run_id": held_run,
        "generation": 6,
        "state": "cancelled",
        "execution_hold_reason": "bounded_recovery_window_expired",
    }
    baseline = {"at": started.isoformat(), "phase": "comments", "comments": {"canonical_ids": 100}}
    frame = {
        "at": (wall - timedelta(seconds=age)).isoformat(),
        "phase": "media" if media else "comments",
        "comments": {"canonical_ids": 100},
        "stop_decision": {"useful_progress": True},
    }
    for name, value in {
        "window.json": window,
        "held.json": held,
        "worker-end.json": {"stop_reason": "worker_exited"},
        "progress-baseline.json": baseline,
        "progress-1.json": frame,
    }.items():
        (directory / name).write_text(json.dumps(value))
    return wall


@pytest.mark.parametrize(
    "age,reason", [(300, "running"), (900, "useful_progress_stalled"), (1200, "useful_progress_stalled")]
)
def test_failed_predecessor_carries_last_useful_clock_instead_of_restarting_it(tmp_path, age, reason):
    wall = prior(tmp_path, age=age)
    tracker = UsefulProgressTracker()
    result = continue_comment_progress(
        tracker,
        tmp_path,
        expected_generation=6,
        metrics={"canonical_ids": 100, "requests": 9999},
        now=5000,
        wall_now=wall,
    )
    assert result["decision"]["idle_seconds"] == age
    assert result["decision"]["reason"] == reason
    assert tracker.observe("media", {"committed_targets": 35}, now=5001)["idle_seconds"] == 0


def test_real_new_identity_resets_continued_clock(tmp_path):
    wall = prior(tmp_path)
    result = continue_comment_progress(
        UsefulProgressTracker(),
        tmp_path,
        expected_generation=6,
        metrics={"canonical_ids": 101},
        now=5000,
        wall_now=wall,
    )
    assert result["decision"]["useful_progress"] is True
    assert result["decision"]["reason"] == "running" and result["decision"]["idle_seconds"] == 0


@pytest.mark.parametrize("case", ["generation", "run", "media"])
def test_unrelated_predecessor_or_media_state_cannot_seed_comment_phase(tmp_path, case):
    wall = prior(tmp_path, media=case == "media", held_run="another-run" if case == "run" else RUN)
    with pytest.raises(RuntimeError):
        continue_comment_progress(
            UsefulProgressTracker(),
            tmp_path,
            expected_generation=8 if case == "generation" else 6,
            metrics={"canonical_ids": 100},
            now=5000,
            wall_now=wall,
        )


@pytest.mark.parametrize("age", [300, 1200])
def test_two_successive_worker_failures_never_refresh_inherited_clock(tmp_path, age):
    original = tmp_path / "original"
    original.mkdir()
    wall = prior(original, age=age)
    first = continue_comment_progress(
        UsefulProgressTracker(),
        original,
        expected_generation=6,
        metrics={"canonical_ids": 100},
        now=5000,
        wall_now=wall,
    )
    second = tmp_path / "second"
    second.mkdir()
    prior(second)
    w = json.loads((second / "window.json").read_text())
    w.update(generation=7, window_id="next-window", started_at=wall.isoformat())
    h = json.loads((second / "held.json").read_text())
    h["generation"] = 8
    (second / "window.json").write_text(json.dumps(w))
    (second / "held.json").write_text(json.dumps(h))
    (second / "progress-1.json").unlink()
    (second / "progress-baseline.json").write_text(
        json.dumps({"at": wall.isoformat(), "phase": "comments", "comments": {"canonical_ids": 100}})
    )
    (second / "continued-progress.json").write_text(json.dumps(first))
    (second / "comment-progress-seed.json").write_text(json.dumps(dict(first["seed"], window_id="next-window")))
    result = continue_comment_progress(
        UsefulProgressTracker(),
        second,
        expected_generation=8,
        metrics={"canonical_ids": 100},
        now=5100,
        wall_now=wall + timedelta(seconds=100),
    )
    assert result["decision"]["idle_seconds"] == age + 100
    assert result["decision"]["reason"] == ("running" if age == 300 else "useful_progress_stalled")


def test_missing_inherited_seed_is_rejected_before_continuation(tmp_path):
    wall = prior(tmp_path)
    (tmp_path / "continued-progress.json").write_text("{}")
    with pytest.raises(RuntimeError, match="seed is missing"):
        continue_comment_progress(
            UsefulProgressTracker(),
            tmp_path,
            expected_generation=6,
            metrics={"canonical_ids": 100},
            now=5000,
            wall_now=wall,
        )


def test_durable_ended_phase_cannot_reopen_for_a_late_metric_change(tmp_path, monkeypatch):
    wall = prior(tmp_path, age=300)
    monkeypatch.setattr(operator, "recorded_recovery_phase", lambda _: "draining_comments")
    result = continue_comment_progress(
        UsefulProgressTracker(),
        tmp_path,
        expected_generation=6,
        metrics={"canonical_ids": 101},
        now=5000,
        wall_now=wall,
    )
    assert result["decision"]["reason"] == "complete"
    assert result["seed"]["comment_phase_ended"] is True


def media_predecessor(directory):
    wall = prior(directory)
    (directory / "worker-end.json").write_text(json.dumps({"stop_reason": "media_useful_progress_stalled"}))
    (directory / "comment-progress-seed.json").write_text(
        json.dumps(
            {
                "version": 1,
                "window_id": "old-window",
                "last_useful_at": (wall - timedelta(seconds=1200)).isoformat(),
                "metrics": {"canonical_ids": 100},
                "comment_phase_ended": True,
                "stop_reason": "useful_progress_stalled",
            }
        )
    )
    (directory / "progress-media.json").write_text(
        json.dumps(
            {
                "at": wall.isoformat(),
                "phase": "media",
                "comments": {"canonical_ids": 100},
                "stop_decision": {"useful_progress": False},
            }
        )
    )
    return wall


def test_explicit_changed_source_media_repair_keeps_comments_closed(tmp_path, monkeypatch):
    wall = media_predecessor(tmp_path)
    monkeypatch.setattr(operator, "recorded_recovery_phase", lambda _: "media")
    tracker = UsefulProgressTracker()
    result = continue_comment_progress(
        tracker,
        tmp_path,
        expected_generation=6,
        metrics={"canonical_ids": 100},
        now=5000,
        wall_now=wall,
        media_repair_source="reviewed-new-source",
    )
    assert result["decision"]["reason"] == "useful_progress_stalled"
    assert result["decision"]["idle_seconds"] == 1200
    assert result["seed"]["comment_phase_ended"] is True
    assert tracker.observe("media", {"committed_targets": 35}, now=5001)["idle_seconds"] == 0


@pytest.mark.parametrize("source", [None, "original-source"])
def test_media_stall_does_not_automatically_buy_a_new_pass(tmp_path, monkeypatch, source):
    wall = media_predecessor(tmp_path)
    monkeypatch.setattr(operator, "recorded_recovery_phase", lambda _: "media")
    with pytest.raises(RuntimeError):
        continue_comment_progress(
            UsefulProgressTracker(),
            tmp_path,
            expected_generation=6,
            metrics={"canonical_ids": 100},
            now=5000,
            wall_now=wall,
            media_repair_source=source,
        )


def test_media_repair_rejects_unrecorded_phase_before_start(tmp_path):
    wall = media_predecessor(tmp_path)
    with pytest.raises(RuntimeError, match="durable media phase"):
        continue_comment_progress(
            UsefulProgressTracker(),
            tmp_path,
            expected_generation=6,
            metrics={"canonical_ids": 100},
            now=5000,
            wall_now=wall,
            media_repair_source="reviewed-new-source",
        )
