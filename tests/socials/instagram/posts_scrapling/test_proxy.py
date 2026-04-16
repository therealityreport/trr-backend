from __future__ import annotations


def test_select_posts_proxy_returns_none_when_no_env(monkeypatch):
    """No proxy env vars → None (local dev mode)."""
    monkeypatch.delenv("SOCIAL_INSTAGRAM_POSTS_PROXY_URLS", raising=False)
    monkeypatch.delenv("DECODO_USERNAME", raising=False)
    monkeypatch.delenv("DECODO_PASSWORD", raising=False)
    from trr_backend.socials.instagram.posts_scrapling.proxy import select_posts_proxy

    result = select_posts_proxy()
    assert result is None


def test_select_posts_proxy_explicit_url(monkeypatch):
    """Explicit proxy URL takes precedence over DECODO."""
    monkeypatch.setenv("SOCIAL_INSTAGRAM_POSTS_PROXY_URLS", "http://user:pass@proxy:8080")
    monkeypatch.setenv("DECODO_USERNAME", "decodo_user")
    monkeypatch.setenv("DECODO_PASSWORD", "decodo_pass")
    from trr_backend.socials.instagram.posts_scrapling.proxy import select_posts_proxy

    result = select_posts_proxy()
    assert result is not None
    assert result.api_proxy_url == "http://user:pass@proxy:8080"
    assert result.fingerprint == "proxy:8080:explicit"


def test_select_posts_proxy_decodo(monkeypatch):
    """DECODO credentials → dict-based browser proxy + URL-encoded API proxy."""
    monkeypatch.delenv("SOCIAL_INSTAGRAM_POSTS_PROXY_URLS", raising=False)
    monkeypatch.setenv("DECODO_USERNAME", "user1")
    monkeypatch.setenv("DECODO_PASSWORD", "p@ss!")
    monkeypatch.setenv("DECODO_GATEWAY", "gate.decodo.com:7000")
    from trr_backend.socials.instagram.posts_scrapling.proxy import select_posts_proxy

    result = select_posts_proxy()
    assert result is not None
    assert isinstance(result.browser_proxy, dict)
    assert result.browser_proxy["server"] == "http://gate.decodo.com:7000"
    assert result.browser_proxy["username"] == "user1"
    assert result.browser_proxy["password"] == "p@ss!"
    assert "p%40ss%21" in result.api_proxy_url
    assert result.fingerprint == "gate.decodo.com:7000:decodo"
