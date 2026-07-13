from __future__ import annotations

import asyncio
import builtins

import pytest

from trr_backend.socials.instagram.runtimes.browser_use_runtime import BrowserUseRuntime
from trr_backend.socials.instagram.runtimes.crawl4ai_runtime import Crawl4aiRuntime
from trr_backend.socials.instagram.runtimes.protocol import RuntimeUnsupported


def _run(coro):
    return asyncio.run(coro)


def test_browser_use_healthcheck_unavailable_without_optional_dependency(monkeypatch: pytest.MonkeyPatch) -> None:
    real_import = builtins.__import__

    def _blocked_import(name, *args, **kwargs):
        if name == "browser_use":
            raise ImportError("missing browser-use")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _blocked_import)

    health = BrowserUseRuntime().healthcheck()

    assert health.healthy is False
    assert health.reason is not None
    assert health.reason.startswith("browser_use_not_installed:")


def test_browser_use_runtime_rejects_steady_state_methods() -> None:
    runtime = BrowserUseRuntime()

    with pytest.raises(RuntimeUnsupported):
        _run(runtime.fetch_profile("bravotv"))
    with pytest.raises(RuntimeUnsupported):
        _run(runtime.fetch_posts("bravotv", limit=5))
    with pytest.raises(RuntimeUnsupported):
        _run(runtime.fetch_post_detail("abc123"))
    with pytest.raises(NotImplementedError):
        _run(runtime.recover_from_checkpoint("https://instagram.com/challenge"))


def test_crawl4ai_healthcheck_unavailable_without_optional_dependency(monkeypatch: pytest.MonkeyPatch) -> None:
    real_import = builtins.__import__

    def _blocked_import(name, *args, **kwargs):
        if name == "crawl4ai":
            raise ImportError("missing crawl4ai")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _blocked_import)

    health = Crawl4aiRuntime().healthcheck()

    assert health.healthy is False
    assert health.reason is not None
    assert health.reason.startswith("crawl4ai_not_installed:")


def test_crawl4ai_runtime_rejects_unimplemented_or_unsupported_methods() -> None:
    runtime = Crawl4aiRuntime()

    with pytest.raises(NotImplementedError):
        _run(runtime.fetch_profile("bravotv"))
    with pytest.raises(RuntimeUnsupported):
        _run(runtime.fetch_posts("bravotv", limit=5))
    with pytest.raises(NotImplementedError):
        _run(runtime.fetch_post_detail("abc123"))
