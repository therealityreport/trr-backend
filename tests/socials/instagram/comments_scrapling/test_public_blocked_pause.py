"""Public-blocked posts stay retryable and trigger a visible run pause.

Covers the pure threshold helper, the run-config pause recommendation wiring
(``dispatch_control.pause_after_current`` + audit detail), and the resume
remaining-target logic re-adding public-blocked shortcodes.

Mirrors the SimpleNamespace-fake style of ``test_completeness_unknown_count.py``
so no live database is required.
"""

from __future__ import annotations

from types import SimpleNamespace

from trr_backend.socials.instagram.comments_scrapling.job_runner import (
    _PUBLIC_BLOCKED_PAUSE_MIN_BLOCKED,
    _PUBLIC_BLOCKED_PAUSE_MIN_CHECKED,
    _public_blocked_pause_should_trigger,
    _recommend_public_blocked_pause,
    _retry_rebalance_metadata,
)


# --- threshold (a): N blocked posts with 0 recovered comments --------------


def test_pause_triggers_on_blocked_count_with_zero_recovery():
    assert (
        _public_blocked_pause_should_trigger(
            checked=_PUBLIC_BLOCKED_PAUSE_MIN_BLOCKED,
            blocked=_PUBLIC_BLOCKED_PAUSE_MIN_BLOCKED,
            recovered_comments=0,
        )
        is True
    )


def test_blocked_count_with_any_recovery_does_not_trigger_via_threshold_a():
    # 10 blocked but a single recovered comment defeats threshold (a). With
    # checked == blocked == 10 (< 25) threshold (b) cannot fire either.
    assert (
        _public_blocked_pause_should_trigger(
            checked=_PUBLIC_BLOCKED_PAUSE_MIN_BLOCKED,
            blocked=_PUBLIC_BLOCKED_PAUSE_MIN_BLOCKED,
            recovered_comments=1,
        )
        is False
    )


def test_below_blocked_threshold_does_not_trigger():
    assert (
        _public_blocked_pause_should_trigger(
            checked=_PUBLIC_BLOCKED_PAUSE_MIN_BLOCKED - 1,
            blocked=_PUBLIC_BLOCKED_PAUSE_MIN_BLOCKED - 1,
            recovered_comments=0,
        )
        is False
    )


# --- threshold (b): >=25 checked and blocked ratio >= 70% ------------------


def test_pause_triggers_on_high_ratio_even_with_recovered_comments():
    # SA-2 (comment-completeness): thresholds were raised — pause (b) now needs
    # >= _PUBLIC_BLOCKED_PAUSE_MIN_CHECKED (100) checked and a >= 90% genuine-block
    # ratio. 100 checked, 95 blocked => 95% ratio. Even though some comments were
    # recovered (so threshold (a) is off), the ratio threshold (b) still fires.
    assert (
        _public_blocked_pause_should_trigger(
            checked=_PUBLIC_BLOCKED_PAUSE_MIN_CHECKED,
            blocked=95,
            recovered_comments=7,
        )
        is True
    )


def test_ratio_below_threshold_does_not_trigger():
    # 25 checked, 17 blocked => 68% < 70%.
    assert (
        _public_blocked_pause_should_trigger(
            checked=_PUBLIC_BLOCKED_PAUSE_MIN_CHECKED,
            blocked=17,
            recovered_comments=7,
        )
        is False
    )


def test_high_ratio_below_min_checked_does_not_trigger():
    # 100% ratio but only 24 checked (< 25) and recovered>0 (threshold (a) off).
    assert (
        _public_blocked_pause_should_trigger(
            checked=_PUBLIC_BLOCKED_PAUSE_MIN_CHECKED - 1,
            blocked=_PUBLIC_BLOCKED_PAUSE_MIN_CHECKED - 1,
            recovered_comments=3,
        )
        is False
    )


# --- run-config pause recommendation wiring --------------------------------


class _FakeRepo:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def _merge_catalog_run_config(self, *, run_id, metadata_updates, conn=None):
        self.calls.append({"run_id": run_id, "metadata_updates": metadata_updates})
        return {"id": run_id, "status": "running", "config": dict(metadata_updates)}


def test_recommend_pause_sets_dispatch_control_on_run_config():
    repo = _FakeRepo()
    blocked = [f"SHORT{i}" for i in range(12)]
    _recommend_public_blocked_pause(
        repo=repo,
        run_id="run-1",
        job_id="job-1",
        checked=12,
        blocked_target_source_ids=blocked,
        recovered_comments=0,
    )

    assert len(repo.calls) == 1
    call = repo.calls[0]
    assert call["run_id"] == "run-1"
    dispatch_control = call["metadata_updates"]["dispatch_control"]
    assert dispatch_control["pause_after_current"] is True
    assert dispatch_control["pause_reason"] == "public_blocked_repeated"
    pb = dispatch_control["public_blocked"]
    assert pb["checked"] == 12
    assert pb["blocked"] == 12
    assert pb["recovered_comments"] == 0
    assert pb["ratio"] == 1.0
    # Sample is capped and reflects the blocked shortcodes.
    assert pb["blocked_target_source_ids_sample"][:3] == ["SHORT0", "SHORT1", "SHORT2"]


def test_recommend_pause_noop_without_run_id():
    repo = _FakeRepo()
    _recommend_public_blocked_pause(
        repo=repo,
        run_id="",
        job_id="job-1",
        checked=12,
        blocked_target_source_ids=["A"],
        recovered_comments=0,
    )
    assert repo.calls == []


def test_recommend_pause_swallows_repo_failure():
    class _BoomRepo:
        def _merge_catalog_run_config(self, **_kwargs):
            raise RuntimeError("db down")

    # Must not raise — a failed pause write cannot crash the shard.
    _recommend_public_blocked_pause(
        repo=_BoomRepo(),
        run_id="run-1",
        job_id="job-1",
        checked=12,
        blocked_target_source_ids=["A"],
        recovered_comments=0,
    )


# --- resume re-adds public-blocked shortcodes ------------------------------


def test_retry_rebalance_re_adds_public_blocked_even_when_processed_past():
    # processed_posts advanced to the end of the list, so the unprocessed tail is
    # empty — but the public-blocked shortcode must still be re-queued.
    rebalance = _retry_rebalance_metadata(
        comments_shard_count=1,
        target_source_ids=["A", "B", "C"],
        processed_posts=3,
        incomplete_target_source_ids=[],
        auth_failed_target_source_ids=[],
        public_blocked_target_source_ids=["B"],
    )
    assert rebalance is not None
    assert rebalance["remaining_target_source_ids"] == ["B"]
    assert rebalance["eligible"] is True


def test_retry_rebalance_dedupes_public_blocked_with_incomplete():
    rebalance = _retry_rebalance_metadata(
        comments_shard_count=2,
        target_source_ids=["A", "B", "C"],
        processed_posts=3,
        incomplete_target_source_ids=["B"],
        auth_failed_target_source_ids=[],
        public_blocked_target_source_ids=["B", "C"],
    )
    assert rebalance is not None
    assert rebalance["remaining_target_source_ids"] == ["B", "C"]
