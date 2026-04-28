"""Scrapling runtime scaffold tests."""

from __future__ import annotations

import asyncio

import pytest

from trr_backend.socials.instagram.runtimes.protocol import RuntimeUnsupported
from trr_backend.socials.instagram.runtimes.scrapling_runtime import ScraplingRuntime


def _run(coro):
    return asyncio.run(coro)


def test_healthcheck_stays_unhealthy_when_scrapling_imports() -> None:
    health = ScraplingRuntime().healthcheck()

    assert health.healthy is False
    assert health.reason == "scrapling_runtime_not_wired"


def test_endpoint_methods_raise_runtime_unsupported() -> None:
    runtime = ScraplingRuntime()

    calls = [
        runtime.fetch_profile("bravotv"),
        runtime.fetch_posts("bravotv", limit=3),
        runtime.fetch_post_detail("abc123"),
    ]

    for call in calls:
        with pytest.raises(RuntimeUnsupported):
            _run(call)


def test_fetch_json_raises_runtime_unsupported() -> None:
    runtime = ScraplingRuntime()

    with pytest.raises(RuntimeUnsupported):
        _run(runtime._fetch_json("https://www.instagram.com/api/v1/example"))
