"""Useful recovery timers ignore activity and retain per-target best evidence."""

import pytest

from scripts.ingestion.january_progress import UsefulProgressTracker


def comments(canonical=100, **targets):
    return {"canonical_ids": canonical, "targets": targets}


def test_raw_requests_pages_and_expected_counts_do_not_reset_default_timer():
    tracker = UsefulProgressTracker()
    assert tracker.observe("comments", comments(), now=10)["useful_progress"] is False
    active = {**comments(), "activity": {"requests": 1000, "pages": 500}, "expected_total": 10000}
    result = tracker.observe("comments", active, now=909)
    assert result["reason"] == "running" and result["idle_seconds"] == 899
    result = tracker.observe("comments", active, now=910)
    assert result == {
        "phase": "comments",
        "reason": "useful_progress_stalled",
        "useful_progress": False,
        "idle_seconds": 900,
        "stall_seconds": 900,
    }


@pytest.mark.parametrize("metric", ["observed_ids", "supported_ids", "best_observed_total", "proof_count"])
def test_each_retained_target_frontier_improvement_resets_timer(metric):
    tracker = UsefulProgressTracker(stall_seconds=10)
    tracker.observe("comments", comments(post={metric: 5}), now=0)
    result = tracker.observe("comments", comments(post={metric: 6}), now=10)
    assert result["useful_progress"] is True and result["idle_seconds"] == 0
    assert tracker.observe("comments", comments(post={metric: 6}), now=20)["reason"] == "useful_progress_stalled"


def test_canonical_new_identity_counts_as_useful_progress():
    tracker = UsefulProgressTracker(stall_seconds=10)
    tracker.observe("comments", comments(), now=0)
    assert tracker.observe("comments", comments(101), now=9)["useful_progress"] is True
    assert tracker.observe("comments", comments(101), now=18)["reason"] == "running"
    assert tracker.observe("comments", comments(101), now=19)["reason"] == "useful_progress_stalled"


def test_resets_rebounds_and_same_total_target_swaps_do_not_rearm_timer():
    tracker = UsefulProgressTracker(stall_seconds=10)
    baseline = comments(a={"observed_ids": 4}, b={"observed_ids": 8})
    tracker.observe("comments", baseline, now=0)
    # A grows beyond its own best while B resets: the unchanged aggregate cannot hide improvement.
    swapped = comments(a={"observed_ids": 8}, b={"observed_ids": 4})
    assert tracker.observe("comments", swapped, now=2)["useful_progress"] is True
    assert tracker.observe("comments", baseline, now=5)["useful_progress"] is False
    assert tracker.observe("comments", swapped, now=8)["useful_progress"] is False
    assert tracker.observe("comments", comments(90), now=10)["useful_progress"] is False
    result = tracker.observe("comments", baseline, now=12)
    assert result["reason"] == "useful_progress_stalled"


def test_new_zero_frontier_is_not_progress_but_new_positive_frontier_is():
    tracker = UsefulProgressTracker(stall_seconds=10)
    tracker.observe("comments", comments(), now=0)
    assert tracker.observe("comments", comments(empty={}), now=5)["useful_progress"] is False
    assert tracker.observe("comments", comments(new={"proof_count": 1}), now=9)["useful_progress"] is True


def test_comment_completion_then_media_has_its_own_clock_and_progress():
    tracker = UsefulProgressTracker(stall_seconds=10)
    tracker.observe("comments", comments(), now=0)
    assert tracker.observe("comments", comments(), now=10, complete=True)["reason"] == "complete"
    baseline = {"verified_slots": 5, "committed_targets": 2}
    assert tracker.observe("media", baseline, now=100)["reason"] == "running"
    assert tracker.observe("media", {**baseline, "verified_slots": 6}, now=109)["useful_progress"] is True
    assert tracker.observe("media", {"verified_slots": 6, "committed_targets": 3}, now=118)["useful_progress"] is True
    assert tracker.observe("media", baseline, now=128)["reason"] == "useful_progress_stalled"


@pytest.mark.parametrize("complete, reason", [(False, "useful_progress_stalled"), (True, "complete")])
def test_terminal_reason_latches_despite_later_improvement_or_completion(complete, reason):
    tracker = UsefulProgressTracker(stall_seconds=10)
    tracker.observe("comments", comments(), now=0)
    assert tracker.observe("comments", comments(), now=10, complete=complete)["reason"] == reason
    result = tracker.observe("comments", comments(1000), now=11, complete=True)
    assert result["reason"] == reason and result["useful_progress"] is False


@pytest.mark.parametrize("bad", [-1, True, 1.5, None])
def test_invalid_counter_observation_does_not_poison_baseline(bad):
    tracker = UsefulProgressTracker(stall_seconds=10)
    tracker.observe("comments", comments(), now=0)
    with pytest.raises(ValueError):
        tracker.observe("comments", comments(101, bad={"proof_count": bad}), now=9)
    result = tracker.observe("comments", comments(), now=10)
    assert result["reason"] == "useful_progress_stalled"


def test_invalid_clocks_and_backwards_time_are_rejected():
    tracker = UsefulProgressTracker()
    tracker.observe("comments", comments(), now=10)
    for clock in [9, float("nan"), float("inf"), True]:
        with pytest.raises(ValueError):
            tracker.observe("comments", comments(), now=clock)
    for interval in [0, -1, float("nan"), float("inf"), True]:
        with pytest.raises(ValueError):
            UsefulProgressTracker(stall_seconds=interval)
