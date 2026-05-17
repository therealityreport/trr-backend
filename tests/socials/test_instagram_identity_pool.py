from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pytest

from trr_backend.socials.instagram.identity_pool import (
    InstagramIdentityPool,
    InstagramIdentityPoolExhausted,
)


def test_identity_pool_skips_unhealthy_proxies() -> None:
    seen: list[str] = []

    def _probe(url: str, timeout_seconds: float) -> bool:
        del timeout_seconds
        seen.append(url)
        return url.endswith("healthy")

    pool = InstagramIdentityPool(
        proxy_urls=[
            "http://proxy-dead",
            "http://proxy-healthy",
        ],
        base_cookies={"sessionid": "seed"},
        max_requests=40,
        max_age_seconds=900,
        max_generations=2,
        probe_timeout_seconds=5.0,
        probe_func=_probe,
    )

    identity = pool.acquire()

    assert seen == ["http://proxy-dead", "http://proxy-healthy"]
    assert identity.proxy_url == "http://proxy-healthy"
    assert identity.proxy_label == "proxy-healthy"
    assert pool.proxy_probe_failures == ["proxy-dead"]


def test_identity_pool_retires_after_request_cap() -> None:
    pool = InstagramIdentityPool(
        proxy_urls=[],
        base_cookies={"sessionid": "seed"},
        max_requests=1,
        max_age_seconds=900,
        max_generations=2,
        probe_timeout_seconds=5.0,
    )

    identity = pool.acquire()
    pool.record_request(identity.session_id)

    updated = pool.get(identity.session_id)
    assert updated.retired is True
    assert updated.retire_reason == "max_requests_exceeded"


def test_identity_pool_retires_after_repeated_rate_limit_strikes() -> None:
    pool = InstagramIdentityPool(
        proxy_urls=[],
        base_cookies={"sessionid": "seed"},
        max_requests=40,
        max_age_seconds=900,
        max_generations=2,
        probe_timeout_seconds=5.0,
    )

    identity = pool.acquire()
    pool.record_rate_limited(identity.session_id)
    assert pool.get(identity.session_id).retired is False

    pool.record_rate_limited(identity.session_id)

    updated = pool.get(identity.session_id)
    assert updated.retired is True
    assert updated.retire_reason == "rate_limit_strikes_exhausted"


def test_identity_pool_raises_after_generation_cap_exhausted() -> None:
    pool = InstagramIdentityPool(
        proxy_urls=[],
        base_cookies={"sessionid": "seed"},
        max_requests=40,
        max_age_seconds=900,
        max_generations=1,
        probe_timeout_seconds=5.0,
    )

    identity = pool.acquire()
    pool.retire(identity.session_id, reason="hard_forbidden")

    with pytest.raises(InstagramIdentityPoolExhausted, match="generation cap"):
        pool.acquire()


def test_identity_pool_acquire_is_lock_safe() -> None:
    pool = InstagramIdentityPool(
        proxy_urls=[
            "http://proxy-a",
            "http://proxy-b",
            "http://proxy-c",
            "http://proxy-d",
        ],
        base_cookies={"sessionid": "seed"},
        max_requests=40,
        max_age_seconds=900,
        max_generations=1,
        probe_timeout_seconds=5.0,
        probe_func=lambda *_args, **_kwargs: True,
    )

    with ThreadPoolExecutor(max_workers=4) as executor:
        session_ids = list(executor.map(lambda _n: pool.acquire().session_id, range(4)))

    assert len(set(session_ids)) == 4
