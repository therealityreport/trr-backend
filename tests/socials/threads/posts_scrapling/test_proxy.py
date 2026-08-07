from __future__ import annotations

import pytest

from trr_backend.socials.threads.posts_scrapling import proxy as threads_proxy


def test_select_threads_posts_proxy_prefers_explicit_urls(monkeypatch: pytest.MonkeyPatch) -> None:
    rotator_calls: list[str | dict[str, str]] = []

    def _fake_build_proxy_rotator(browser_proxy: str | dict[str, str]) -> dict[str, str | dict[str, str]]:
        rotator_calls.append(browser_proxy)
        return {"browser_proxy": browser_proxy}

    monkeypatch.setattr(threads_proxy, "build_proxy_rotator", _fake_build_proxy_rotator)
    monkeypatch.setenv("SOCIAL_THREADS_POSTS_PROXY_URLS", "http://user:pass@proxy.test:8080")

    proxy = threads_proxy.select_threads_posts_proxy()

    assert proxy is not None
    assert proxy.api_proxy_url == "http://user:pass@proxy.test:8080"
    assert proxy.proxy_rotator == {"browser_proxy": "http://user:pass@proxy.test:8080"}
    assert proxy.fingerprint == "proxy.test:8080:explicit"
    assert rotator_calls == ["http://user:pass@proxy.test:8080"]


def test_select_threads_posts_proxy_returns_none_when_decodo_credentials_exist_without_provider(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rotator_calls: list[str | dict[str, str]] = []

    def _fake_build_proxy_rotator(browser_proxy: str | dict[str, str]) -> dict[str, str | dict[str, str]]:
        rotator_calls.append(browser_proxy)
        return {"browser_proxy": browser_proxy}

    monkeypatch.setattr(threads_proxy, "build_proxy_rotator", _fake_build_proxy_rotator)
    monkeypatch.delenv("SOCIAL_THREADS_POSTS_PROXY_URLS", raising=False)
    monkeypatch.delenv("SOCIAL_THREADS_POSTS_PROXY_PROVIDER", raising=False)
    monkeypatch.delenv("SOCIAL_THREADS_POSTS_USE_STICKY_PROXY", raising=False)
    monkeypatch.setenv("DECODO_USERNAME", "user")
    monkeypatch.setenv("DECODO_PASSWORD", "secret")
    monkeypatch.setenv("DECODO_GATEWAY", "gate.decodo.com:7000")

    assert threads_proxy.select_threads_posts_proxy() is None

    monkeypatch.setenv("SOCIAL_THREADS_POSTS_PROXY_PROVIDER", "")
    assert threads_proxy.select_threads_posts_proxy() is None
    assert rotator_calls == []


def test_select_threads_posts_proxy_decodo(monkeypatch: pytest.MonkeyPatch) -> None:
    rotator_calls: list[str | dict[str, str]] = []

    def _fake_build_proxy_rotator(browser_proxy: str | dict[str, str]) -> dict[str, str | dict[str, str]]:
        rotator_calls.append(browser_proxy)
        return {"browser_proxy": browser_proxy}

    monkeypatch.setattr(threads_proxy, "build_proxy_rotator", _fake_build_proxy_rotator)
    monkeypatch.delenv("SOCIAL_THREADS_POSTS_PROXY_URLS", raising=False)
    monkeypatch.setenv("SOCIAL_THREADS_POSTS_PROXY_PROVIDER", "decodo")
    monkeypatch.delenv("SOCIAL_THREADS_POSTS_USE_STICKY_PROXY", raising=False)
    monkeypatch.setenv("DECODO_USERNAME", "user")
    monkeypatch.setenv("DECODO_PASSWORD", "secret")
    monkeypatch.setenv("DECODO_GATEWAY", "gate.decodo.com:7000")

    proxy = threads_proxy.select_threads_posts_proxy()

    assert proxy is not None
    assert proxy.api_proxy_url == "http://user:secret@gate.decodo.com:7000"
    assert proxy.browser_proxy == {
        "server": "http://gate.decodo.com:7000",
        "username": "user",
        "password": "secret",
    }
    assert proxy.proxy_rotator == {"browser_proxy": proxy.browser_proxy}
    assert proxy.fingerprint == "gate.decodo.com:7000:decodo"
    assert proxy.session_mode == "rotating"
    assert isinstance(proxy.browser_proxy, dict)
    assert "-session-" not in proxy.browser_proxy["username"]
    assert "sessionduration" not in proxy.api_proxy_url
    assert rotator_calls == [proxy.browser_proxy]


def test_select_threads_posts_proxy_decodo_sticky_opt_in(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(threads_proxy, "build_proxy_rotator", lambda browser_proxy: {"browser_proxy": browser_proxy})
    monkeypatch.delenv("SOCIAL_THREADS_POSTS_PROXY_URLS", raising=False)
    monkeypatch.setenv("SOCIAL_THREADS_POSTS_PROXY_PROVIDER", "decodo")
    monkeypatch.setenv("SOCIAL_THREADS_POSTS_USE_STICKY_PROXY", "true")
    monkeypatch.setenv("SOCIAL_THREADS_POSTS_PROXY_SESSION_TTL_SECONDS", "600")
    monkeypatch.setenv("DECODO_USERNAME", "user")
    monkeypatch.setenv("DECODO_PASSWORD", "secret")
    monkeypatch.setenv("DECODO_GATEWAY", "gate.decodo.com:7000")

    proxy = threads_proxy.select_threads_posts_proxy(session_key="threads:posts:bravotv")

    assert proxy is not None
    assert isinstance(proxy.browser_proxy, dict)
    assert proxy.session_mode == "sticky"
    assert proxy.api_proxy_url is not None
    assert proxy.browser_proxy["username"].startswith("user-session-")
    assert "-sessionduration-10" in proxy.browser_proxy["username"]
    assert "sessionduration-10" in proxy.api_proxy_url


def test_select_threads_posts_proxy_respects_explicit_none(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SOCIAL_THREADS_POSTS_PROXY_URLS", raising=False)
    monkeypatch.setenv("SOCIAL_THREADS_POSTS_PROXY_PROVIDER", "none")
    monkeypatch.setenv("DECODO_USERNAME", "user")
    monkeypatch.setenv("DECODO_PASSWORD", "secret")
    monkeypatch.setenv("DECODO_GATEWAY", "gate.decodo.com:7000")

    assert threads_proxy.select_threads_posts_proxy() is None
