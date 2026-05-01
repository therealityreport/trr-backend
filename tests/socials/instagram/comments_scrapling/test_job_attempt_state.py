"""Phase 1.3 / 1.4 regression tests for ``_job_attempt_state`` and the
incomplete-run raise threshold."""

from __future__ import annotations

from trr_backend.socials.instagram.comments_scrapling import job_runner as jr


def test_job_attempt_state_floors_attempt_and_max_attempts_to_at_least_one():
    """Both inputs default to 1 when missing; max_attempts is then forced to 2
    so can_retry doesn't short-circuit on the first transient failure."""
    attempt_count, max_attempts = jr._job_attempt_state({})
    assert attempt_count == 1
    assert max_attempts >= attempt_count + 1


def test_job_attempt_state_enforces_max_attempts_above_attempt_count():
    """Phase 1.3 / audit: even when the row carries equal attempt_count and
    max_attempts (e.g. legacy enqueue without explicit max_attempts), the
    runner must allow at least one more retry before terminal failure."""
    attempt_count, max_attempts = jr._job_attempt_state({"attempt_count": 1, "max_attempts": 1})
    assert attempt_count == 1
    assert max_attempts == 2  # 1 + 1


def test_job_attempt_state_preserves_explicit_max_when_above_threshold():
    """Explicit higher max_attempts wins over the +1 floor."""
    attempt_count, max_attempts = jr._job_attempt_state({"attempt_count": 2, "max_attempts": 12})
    assert attempt_count == 2
    assert max_attempts == 12


def test_job_attempt_state_caps_attempt_count_at_one_when_invalid():
    """Garbage values fall back to the queue defaults; the +1 floor still applies."""
    attempt_count, max_attempts = jr._job_attempt_state({"attempt_count": "garbage", "max_attempts": None})
    assert attempt_count == 1
    assert max_attempts >= attempt_count + 1
