"""Pure useful-progress timers for the comments and media recovery phases.

The operator owns database sampling and phase completion. Comment canonical_ids
counts actual distinct saved identities. Per-target observed_ids/supported_ids
must be monotone union cardinalities from retained recovery frontiers, rather
than counts from only the current scan. best_observed_total is the retained best
single-scan total; proof_count counts verified distinct completeness proofs.
Requests, page receipts, retries, and timestamps are activity, not useful progress.
Media verified_slots counts verified saved slots; committed_targets counts saved
media targets. Saved rows must be preserved: these counters do not prove no loss.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass, field

_COMMENT_METRICS = ("observed_ids", "supported_ids", "best_observed_total", "proof_count")
_MEDIA_METRICS = ("verified_slots", "committed_targets")


def _nonnegative_count(value):
    if type(value) is not int or value < 0:
        raise ValueError("Useful progress counters must be nonnegative integers")
    return value


def _finite_time(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError("Progress clock must be finite")
    return value


def _counters(phase, metrics):
    if not isinstance(metrics, Mapping):
        raise ValueError("Progress metrics must be a mapping")
    if phase == "media":
        return {(key,): _nonnegative_count(metrics.get(key, 0)) for key in _MEDIA_METRICS}
    counters: dict[tuple[str, ...], int] = {("canonical_ids",): _nonnegative_count(metrics.get("canonical_ids", 0))}
    targets = metrics.get("targets", {})
    if not isinstance(targets, Mapping):
        raise ValueError("Comment targets must be a mapping")
    for target, frontier in targets.items():
        if not isinstance(target, str) or not target or not isinstance(frontier, Mapping):
            raise ValueError("Each comment target must have an identity and a frontier mapping")
        for key in _COMMENT_METRICS:
            counters[("targets", target, key)] = _nonnegative_count(frontier.get(key, 0))
    return counters


@dataclass
class _Phase:
    last_useful: float
    last_seen: float
    best: dict = field(default_factory=dict)
    reason: str = "running"


class UsefulProgressTracker:
    """Keep independent, terminal-latching phase timers using a monotonic clock.

    First observation seeds each phase's baseline without claiming improvement.
    The caller should sample immediately when entering a phase. New observations
    update per-target maxima, so reset/rebound cycles cannot extend the timer.
    Neither activity nor improvements in another phase reset a phase's timer.
    This object has no I/O and does not authorize retries or release branch holds.
    """

    def __init__(self, stall_seconds=900):
        self.stall_seconds = _finite_time(stall_seconds)
        if self.stall_seconds <= 0:
            raise ValueError("Stall interval must be positive")
        self._phases = {}

    def observe(self, phase, metrics, *, now, complete=False):
        """Return running, complete, or useful_progress_stalled with timer facts.

        A useful improvement sampled exactly at the idle deadline resets the
        timer. Explicit completion takes precedence over that deadline. Once a
        phase stops, later observations cannot restart it or report new progress.
        """
        if phase not in {"comments", "media"}:
            raise ValueError("Unknown recovery phase")
        now = _finite_time(now)
        if type(complete) is not bool:
            raise ValueError("Phase completion must be a boolean")
        counters = _counters(phase, metrics)
        state = self._phases.get(phase)
        if state is not None and now < state.last_seen:
            raise ValueError("Progress clock moved backwards")
        useful = False
        if state is None:
            state = _Phase(last_useful=now, last_seen=now, best=counters)
            self._phases[phase] = state
        elif state.reason == "running":
            useful = any(value > state.best.get(key, 0) for key, value in counters.items())
            for key, value in counters.items():
                state.best[key] = max(state.best.get(key, 0), value)
            if useful:
                state.last_useful = now
        state.last_seen = now
        idle = now - state.last_useful
        if state.reason == "running":
            if complete:
                state.reason = "complete"
            elif idle >= self.stall_seconds:
                state.reason = "useful_progress_stalled"
        return {
            "phase": phase,
            "reason": state.reason,
            "useful_progress": useful,
            "idle_seconds": idle,
            "stall_seconds": self.stall_seconds,
        }
