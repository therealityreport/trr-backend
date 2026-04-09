from __future__ import annotations

import threading
import time
from collections import defaultdict

from trr_backend.socials.instagram.constants import (
    QUERY_TYPE_BROWSER_GRAPHQL_INTERCEPT,
    QUERY_TYPE_PERMALINK_MEDIA,
    QUERY_TYPE_PROFILE_HTML,
)


class InstagramRateController:
    def __init__(self, *, clock=None, sleeper=None) -> None:
        self._clock = clock or time.monotonic
        self._sleeper = sleeper or time.sleep
        self._lock = threading.Lock()
        self._query_timestamps: dict[str, list[float]] = defaultdict(list)
        self._consecutive_success = 0
        self._last_429_at: float | None = None
        self._request_count = 0

    def _window_limit(self, query_type: str) -> tuple[int, float]:
        if query_type == QUERY_TYPE_PROFILE_HTML:
            return (20, 600.0)
        if query_type == QUERY_TYPE_PERMALINK_MEDIA:
            return (60, 600.0)
        if query_type == QUERY_TYPE_BROWSER_GRAPHQL_INTERCEPT:
            return (120, 600.0)
        return (180, 660.0)

    def _adaptive_delay(self, base_delay: float, fast_mode: bool, *, now: float) -> float:
        if self._request_count == 0:
            return 0.0
        if self._last_429_at and (now - self._last_429_at) < 60.0:
            return max(base_delay * 2.0, 1.0)
        if fast_mode:
            if self._consecutive_success >= 20:
                return base_delay * 0.15
            if self._consecutive_success >= 5:
                return base_delay * 0.25
            return base_delay * 0.5
        if self._consecutive_success >= 20:
            return base_delay * 0.5
        return base_delay * 0.75

    def _window_wait(self, query_type: str, *, now: float) -> float:
        max_count, window = self._window_limit(query_type)
        timestamps = [ts for ts in self._query_timestamps[query_type] if ts > now - window]
        self._query_timestamps[query_type] = timestamps
        if len(timestamps) < max_count:
            return 0.0
        return max((min(timestamps) + window + 1.0) - now, 0.0)

    def before_query(self, query_type: str, *, base_delay: float, fast_mode: bool) -> None:
        with self._lock:
            now = self._clock()
            wait_for = max(
                self._adaptive_delay(base_delay, fast_mode, now=now),
                self._window_wait(query_type, now=now),
            )
            if wait_for > 0:
                self._sleeper(wait_for)
            self._query_timestamps[query_type].append(self._clock())
            self._request_count += 1

    def record_response(self, query_type: str, status_code: int) -> None:
        del query_type
        with self._lock:
            if status_code == 429:
                self._last_429_at = self._clock()
                self._consecutive_success = 0
            elif 200 <= status_code < 400:
                self._consecutive_success += 1

