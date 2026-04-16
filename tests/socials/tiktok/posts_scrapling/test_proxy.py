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


def test_resolve_tiktok_session_uses_canonical_loader(monkeypatch):
    """Session adapter delegates to the canonical _load_tiktok_cookies()."""
    monkeypatch.setattr(
        "trr_backend.socials.tiktok.posts_scrapling.session._load_tiktok_cookies",
        lambda: {"sessionid": "abc123", "tt_csrf_token": "xyz"},
    )
    from trr_backend.socials.tiktok.posts_scrapling.session import resolve_tiktok_posts_session

    result = resolve_tiktok_posts_session()
    assert result.raw_cookies["sessionid"] == "abc123"
    assert len(result.cookies) == 2
    assert result.cookies[0]["domain"] == ".tiktok.com"
    assert result.cookie_source == "canonical"


def test_resolve_tiktok_session_raises_on_empty(monkeypatch):
    """Raises if canonical loader returns no cookies."""
    import pytest

    monkeypatch.setattr(
        "trr_backend.socials.tiktok.posts_scrapling.session._load_tiktok_cookies",
        lambda: {},
    )
    from trr_backend.socials.tiktok.posts_scrapling.session import resolve_tiktok_posts_session

    with pytest.raises(RuntimeError, match="No TikTok cookies"):
        resolve_tiktok_posts_session()
