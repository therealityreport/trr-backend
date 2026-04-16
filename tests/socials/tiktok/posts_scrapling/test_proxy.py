from __future__ import annotations


def test_select_tiktok_proxy_returns_none_when_no_env(monkeypatch):
    monkeypatch.delenv("SOCIAL_TIKTOK_POSTS_PROXY_URLS", raising=False)
    monkeypatch.delenv("DECODO_USERNAME", raising=False)
    monkeypatch.delenv("DECODO_PASSWORD", raising=False)
    from trr_backend.socials.tiktok.posts_scrapling.proxy import select_tiktok_posts_proxy

    assert select_tiktok_posts_proxy() is None


def test_select_tiktok_proxy_explicit_url(monkeypatch):
    monkeypatch.setenv("SOCIAL_TIKTOK_POSTS_PROXY_URLS", "http://user:pass@proxy:9090")
    from trr_backend.socials.tiktok.posts_scrapling.proxy import select_tiktok_posts_proxy

    result = select_tiktok_posts_proxy()
    assert result is not None
    assert result.api_proxy_url == "http://user:pass@proxy:9090"


def test_select_tiktok_proxy_decodo(monkeypatch):
    monkeypatch.delenv("SOCIAL_TIKTOK_POSTS_PROXY_URLS", raising=False)
    monkeypatch.setenv("DECODO_USERNAME", "user1")
    monkeypatch.setenv("DECODO_PASSWORD", "secret")
    monkeypatch.setenv("DECODO_GATEWAY", "gate.decodo.com:7000")
    from trr_backend.socials.tiktok.posts_scrapling.proxy import select_tiktok_posts_proxy

    result = select_tiktok_posts_proxy()
    assert result is not None
    assert isinstance(result.browser_proxy, dict)
    assert result.fingerprint == "gate.decodo.com:7000:decodo"
