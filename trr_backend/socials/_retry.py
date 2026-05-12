"""Shared retry/backoff utilities for social scrapers.

Before this module, retry + backoff logic was duplicated across:
- `tiktok/http_client.py::_sleep_before_retry`
- `tiktok/scraper.py::_rate_limit`
- `instagram/scraper.py` (multiple fetch_* methods)
- `instagram/cookie_refresh.py`

Each copy drifted (linear vs. exponential, jitter vs. none, different max
attempt handling). Use this module for new call sites and migrate existing
ones opportunistically. The single source of truth prevents the class of
bugs fixed in .claude/plans/fancy-beaming-dijkstra.md#1.

Public API: `exponential_backoff_delay()` and the `retry_with_backoff`
decorator. The decorator classifies exceptions via a caller-supplied
predicate so it stays agnostic to platform-specific failure types.
"""

from __future__ import annotations

import logging
import random
import time
from collections.abc import Callable
from functools import wraps
from typing import Any, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")

_DEFAULT_JITTER_RATIO = 0.25


def exponential_backoff_delay(
    attempt: int,
    *,
    base_delay: float,
    jitter_ratio: float = _DEFAULT_JITTER_RATIO,
    max_delay: float | None = None,
) -> float:
    """Return the delay to sleep before `attempt` (1-indexed).

    Curve: `base_delay * 2 ** (attempt - 1)` plus symmetric jitter of
    +/-(jitter_ratio * base). Zero `base_delay` or `attempt < 1` returns 0.

    Linear backoff starves rate-limit recovery windows faster than
    exponential; jitter prevents worker cohorts from retrying in lockstep.
    """
    if attempt < 1 or base_delay <= 0:
        return 0.0
    delay = base_delay * (2 ** (attempt - 1))
    if max_delay is not None:
        delay = min(delay, max_delay)
    jitter = random.uniform(-delay * jitter_ratio, delay * jitter_ratio)
    return max(0.0, delay + jitter)


def retry_with_backoff(
    *,
    max_attempts: int,
    base_delay: float,
    should_retry: Callable[[BaseException], bool],
    max_delay: float | None = None,
    sleeper: Callable[[float], None] = time.sleep,
    on_retry: Callable[[int, BaseException], None] | None = None,
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorate a callable so it retries on classified exceptions.

    Args:
        max_attempts: Total attempts including the first. Must be >= 1.
        base_delay: Seconds for the first retry; doubles each subsequent.
        should_retry: Predicate that decides whether a raised exception
            warrants another attempt. Return False to fail fast (e.g. for
            Instagram 401/403 which should not be retried).
        max_delay: Optional cap on any single sleep.
        sleeper: Injection hook for tests; defaults to `time.sleep`.
        on_retry: Optional callback (attempt_number, exception) for
            structured logging or metric emission.
    """
    if max_attempts < 1:
        raise ValueError("max_attempts must be >= 1")

    def decorator(fn: Callable[..., T]) -> Callable[..., T]:
        @wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            last_exc: BaseException | None = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except BaseException as exc:  # noqa: BLE001 - classified below
                    last_exc = exc
                    if not should_retry(exc) or attempt >= max_attempts:
                        raise
                    if on_retry is not None:
                        try:
                            on_retry(attempt, exc)
                        except Exception:  # noqa: BLE001 - callback best-effort
                            logger.debug("retry_with_backoff on_retry callback failed", exc_info=True)
                    sleeper(
                        exponential_backoff_delay(
                            attempt,
                            base_delay=base_delay,
                            max_delay=max_delay,
                        )
                    )
            # Unreachable when max_attempts >= 1, but kept for type-checkers.
            assert last_exc is not None
            raise last_exc

        return wrapper

    return decorator
