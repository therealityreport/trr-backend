from __future__ import annotations

import pytest

from trr_backend.socials.threads.posts_scrapling.proxy import select_threads_posts_proxy


def test_select_threads_posts_proxy_prefers_explicit_urls(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOCIAL_THREADS_POSTS_PROXY_URLS", "http://user:pass@proxy.test:8080")

    proxy = select_threads_posts_proxy()

    assert proxy is not None
    assert proxy.api_proxy_url == "http://user:pass@proxy.test:8080"
    assert proxy.fingerprint == "proxy.test:8080:explicit"
