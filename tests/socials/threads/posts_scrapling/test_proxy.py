from __future__ import annotations

import pytest

from trr_backend.socials.threads.posts_scrapling.proxy import select_threads_posts_proxy


def test_select_threads_posts_proxy_prefers_explicit_urls(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOCIAL_THREADS_POSTS_PROXY_URLS", "http://user:pass@proxy.test:8080")

    proxy = select_threads_posts_proxy()

    assert proxy is not None
    assert proxy.api_proxy_url == "http://user:pass@proxy.test:8080"
    assert proxy.fingerprint == "proxy.test:8080:explicit"


def test_select_threads_posts_proxy_does_not_default_to_decodo(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SOCIAL_THREADS_POSTS_PROXY_URLS", raising=False)
    monkeypatch.delenv("SOCIAL_THREADS_POSTS_PROXY_PROVIDER", raising=False)
    monkeypatch.setenv("DECODO_USERNAME", "user")
    monkeypatch.setenv("DECODO_PASSWORD", "secret")
    monkeypatch.setenv("DECODO_GATEWAY", "gate.decodo.com:7000")

    assert select_threads_posts_proxy() is None
