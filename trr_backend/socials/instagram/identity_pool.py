from __future__ import annotations

import threading
import time
import uuid
from dataclasses import dataclass
from typing import Callable
from urllib.parse import urlparse

import requests


class InstagramIdentityPoolExhausted(RuntimeError):
    """Raised when the pool cannot supply another usable identity."""


@dataclass(slots=True)
class InstagramScraperIdentity:
    session_id: str
    generation: int
    proxy_url: str | None
    proxy_label: str | None
    cookies: dict[str, str]
    created_at: float
    request_count: int = 0
    rate_limit_strikes: int = 0
    timeout_strikes: int = 0
    retired: bool = False
    retire_reason: str | None = None
    block_reason: str | None = None


def _default_probe(url: str, timeout_seconds: float) -> bool:
    del url, timeout_seconds
    return True


class InstagramIdentityPool:
    def __init__(
        self,
        *,
        proxy_urls: list[str],
        base_cookies: dict[str, str] | None,
        max_requests: int,
        max_age_seconds: int,
        max_generations: int,
        probe_timeout_seconds: float,
        probe_func: Callable[[str, float], bool] | None = None,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self.proxy_urls = [str(url or "").strip() for url in proxy_urls if str(url or "").strip()]
        self.base_cookies = {str(key): str(value) for key, value in (base_cookies or {}).items() if value is not None}
        self.max_requests = max(1, int(max_requests))
        self.max_age_seconds = max(1, int(max_age_seconds))
        self.max_generations = max(1, int(max_generations))
        self.probe_timeout_seconds = max(0.1, float(probe_timeout_seconds))
        self._probe_func = probe_func or _default_probe
        self._clock = clock or time.monotonic
        self._lock = threading.Lock()
        self._generation = 0
        self._next_index = 0
        self._identities: list[InstagramScraperIdentity] = []
        self._identities_by_id: dict[str, InstagramScraperIdentity] = {}
        self.proxy_probe_failures: list[str] = []
        self._seed_generation()

    @property
    def generation(self) -> int:
        return self._generation

    def acquire(self) -> InstagramScraperIdentity:
        with self._lock:
            while True:
                # First pass: retire any identity that has aged out. Done as
                # an explicit mutation step rather than inside the filter
                # predicate so the iteration and the state change are
                # clearly separated.
                for identity in self._identities:
                    if not identity.retired and self._is_expired_for_age(identity):
                        identity.retired = True
                        identity.retire_reason = "max_age_exceeded"
                active = [identity for identity in self._identities if not identity.retired]
                if active:
                    index = self._next_index % len(active)
                    self._next_index += 1
                    return active[index]
                if self._generation >= self.max_generations:
                    raise InstagramIdentityPoolExhausted(
                        "Instagram identity pool generation cap exhausted; no usable identities remain."
                    )
                self._seed_generation()

    def get(self, session_id: str) -> InstagramScraperIdentity:
        with self._lock:
            return self._identities_by_id[session_id]

    def merge_cookies(self, session_id: str, cookies: dict[str, str]) -> None:
        with self._lock:
            self._identities_by_id[session_id].cookies = {
                str(key): str(value) for key, value in cookies.items() if value is not None
            }

    def record_request(self, session_id: str) -> None:
        with self._lock:
            identity = self._identities_by_id[session_id]
            identity.request_count += 1
            if identity.request_count >= self.max_requests and not identity.retired:
                identity.retired = True
                identity.retire_reason = "max_requests_exceeded"

    def record_success(self, session_id: str) -> None:
        with self._lock:
            identity = self._identities_by_id[session_id]
            identity.rate_limit_strikes = 0
            identity.timeout_strikes = 0

    def record_rate_limited(self, session_id: str) -> None:
        with self._lock:
            identity = self._identities_by_id[session_id]
            identity.rate_limit_strikes += 1
            identity.block_reason = "rate_limited"
            if identity.rate_limit_strikes >= 2 and not identity.retired:
                identity.retired = True
                identity.retire_reason = "rate_limit_strikes_exhausted"

    def record_timeout(self, session_id: str) -> None:
        with self._lock:
            identity = self._identities_by_id[session_id]
            identity.timeout_strikes += 1
            identity.block_reason = "proxy_timeout"
            if identity.timeout_strikes >= 2 and not identity.retired:
                identity.retired = True
                identity.retire_reason = "timeout_strikes_exhausted"

    def retire(self, session_id: str, *, reason: str, block_reason: str | None = None) -> None:
        with self._lock:
            identity = self._identities_by_id[session_id]
            identity.retired = True
            identity.retire_reason = reason
            identity.block_reason = block_reason

    def _seed_generation(self) -> None:
        self._generation += 1
        self._next_index = 0
        self._identities = []
        candidates = self.proxy_urls or [None]
        for proxy_url in candidates:
            normalized_proxy = str(proxy_url or "").strip() or None
            proxy_label = self._proxy_label(normalized_proxy)
            if normalized_proxy is not None and not self._probe_func(normalized_proxy, self.probe_timeout_seconds):
                if proxy_label:
                    self.proxy_probe_failures.append(proxy_label)
                continue
            identity = InstagramScraperIdentity(
                session_id=f"ig-{self._generation}-{uuid.uuid4().hex[:8]}",
                generation=self._generation,
                proxy_url=normalized_proxy,
                proxy_label=proxy_label,
                cookies=dict(self.base_cookies),
                created_at=self._clock(),
            )
            self._identities.append(identity)
            self._identities_by_id[identity.session_id] = identity

    def _is_expired_for_age(self, identity: InstagramScraperIdentity) -> bool:
        """Pure predicate: True iff the identity has aged past max_age_seconds.

        Previously this method also mutated `identity.retired` / `retire_reason`
        as a side effect, which made it unsafe to call inside list
        comprehensions. Callers that want to retire an expired identity
        should do so explicitly after this returns True.
        """
        return (self._clock() - identity.created_at) >= self.max_age_seconds

    @staticmethod
    def _proxy_label(proxy_url: str | None) -> str | None:
        if not proxy_url:
            return None
        parsed = urlparse(proxy_url)
        return parsed.hostname or proxy_url
