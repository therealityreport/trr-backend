"""Tests for trr_backend.socials._retry."""

from __future__ import annotations

import pytest

from trr_backend.socials._retry import (
    exponential_backoff_delay,
    retry_with_backoff,
)

# ---------- exponential_backoff_delay ----------


def test_delay_is_roughly_exponential() -> None:
    """Base 1.5 -> 1.5, 3.0, 6.0, 12.0 before jitter."""
    expected_bases = [1.5, 3.0, 6.0, 12.0]
    for attempt, expected in zip((1, 2, 3, 4), expected_bases, strict=True):
        delay = exponential_backoff_delay(attempt, base_delay=1.5, jitter_ratio=0.0)
        assert delay == pytest.approx(expected)


def test_delay_respects_max_cap() -> None:
    delay = exponential_backoff_delay(10, base_delay=1.0, max_delay=5.0, jitter_ratio=0.0)
    assert delay == pytest.approx(5.0)


def test_delay_returns_zero_for_non_positive_inputs() -> None:
    assert exponential_backoff_delay(0, base_delay=1.0) == 0.0
    assert exponential_backoff_delay(1, base_delay=0.0) == 0.0
    assert exponential_backoff_delay(-1, base_delay=1.0) == 0.0


def test_jitter_stays_within_bounds() -> None:
    for _ in range(100):
        delay = exponential_backoff_delay(3, base_delay=2.0, jitter_ratio=0.25)
        # Base is 2 * 2**2 = 8; +/-25% = [6, 10].
        assert 6.0 <= delay <= 10.0


# ---------- retry_with_backoff ----------


class _Flaky:
    def __init__(self, fail_until: int, exc: Exception) -> None:
        self.calls = 0
        self._fail_until = fail_until
        self._exc = exc

    def __call__(self, *args: object, **kwargs: object) -> str:
        self.calls += 1
        if self.calls < self._fail_until:
            raise self._exc
        return "ok"


def test_retries_on_classified_exception_until_success() -> None:
    flaky = _Flaky(fail_until=3, exc=ValueError("transient"))
    delays: list[float] = []

    @retry_with_backoff(
        max_attempts=5,
        base_delay=0.5,
        should_retry=lambda exc: isinstance(exc, ValueError),
        sleeper=delays.append,
    )
    def call() -> str:
        return flaky()

    assert call() == "ok"
    assert flaky.calls == 3
    # Two sleeps (between the three attempts).
    assert len(delays) == 2


def test_failfast_when_should_retry_returns_false() -> None:
    flaky = _Flaky(fail_until=99, exc=PermissionError("401"))

    @retry_with_backoff(
        max_attempts=5,
        base_delay=0.1,
        should_retry=lambda exc: False,  # mimics IG 401 post-fix
        sleeper=lambda _d: None,
    )
    def call() -> str:
        return flaky()

    with pytest.raises(PermissionError):
        call()
    assert flaky.calls == 1  # no retries


def test_raises_after_exhausting_attempts() -> None:
    flaky = _Flaky(fail_until=99, exc=RuntimeError("always"))

    @retry_with_backoff(
        max_attempts=3,
        base_delay=0.0,
        should_retry=lambda exc: True,
        sleeper=lambda _d: None,
    )
    def call() -> str:
        return flaky()

    with pytest.raises(RuntimeError):
        call()
    assert flaky.calls == 3


def test_invalid_max_attempts_raises() -> None:
    with pytest.raises(ValueError):
        retry_with_backoff(
            max_attempts=0,
            base_delay=1.0,
            should_retry=lambda _exc: True,
        )
