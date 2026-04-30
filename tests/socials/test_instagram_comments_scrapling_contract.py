"""P1-4 contract test — verifies the Scrapling API surface we depend on.

This guards against silent drift when Scrapling ships a minor release that
renames a kwarg, relocates ProxyRotator, or removes an option we pass. It
stays narrow on purpose — couple only to the public surface documented at
https://scrapling.readthedocs.io/, not to internal modules.

If Scrapling changes in a breaking way, THIS is where we want CI to shout
first — long before a production run hits the live fetcher and fails.
"""

from __future__ import annotations

import inspect


def test_scrapling_package_importable() -> None:
    """The lane cannot work without scrapling installed. Guards against the
    false-green CI we had on 2026-04-15 when scrapling was lock-pinned but
    not actually present in the venv."""
    import scrapling

    assert scrapling.__version__, "scrapling is installed but __version__ is falsy"


def test_stealthy_fetcher_public_import() -> None:
    """Exercises the same import path as comments_scrapling.fetcher."""
    from scrapling.fetchers import StealthyFetcher  # noqa: F401


def test_proxy_rotator_public_import() -> None:
    """Exercises the public ProxyRotator path we switched to in P1-4."""
    from scrapling.fetchers import ProxyRotator  # noqa: F401


def test_stealth_session_contains_all_kwargs_we_use() -> None:
    """The fetcher passes a specific set of kwargs to StealthyFetcher.async_fetch.
    If Scrapling renames or removes any of them, Instagram runs will break
    silently (because async_fetch accepts **kwargs, unknown names are dropped
    rather than raising). Assert the set is still honored.
    """
    from scrapling.engines._browsers._types import StealthSession

    expected_kwargs = {
        "cookies",
        "extra_headers",
        "headless",
        "network_idle",
        "load_dom",
        "page_action",
        "proxy_rotator",
        "retries",
        "retry_delay",
        "timeout",
        "wait",
        # capture_xhr removed - Scrapling 0.4.x types it as `str | None`.
        # (XHR URL pattern), not bool. We don't need XHR capture for
        # comments so we dropped it from the _fetch call entirely.
    }
    actual = set(StealthSession.__annotations__.keys())
    missing = expected_kwargs - actual
    assert not missing, (
        f"StealthSession in installed Scrapling no longer supports: {sorted(missing)}. "
        "Update comments_scrapling/fetcher.py async_fetch call and/or Scrapling pin."
    )


def test_async_fetch_is_coroutine_function() -> None:
    """fetcher.py awaits async_fetch — it must remain a coroutine."""
    from scrapling.fetchers import StealthyFetcher

    assert inspect.iscoroutinefunction(StealthyFetcher.async_fetch), (
        "StealthyFetcher.async_fetch is no longer an async method — fetcher.py awaits will break."
    )
