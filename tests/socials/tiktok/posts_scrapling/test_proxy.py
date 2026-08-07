from __future__ import annotations

from typing import Any


def test_select_tiktok_proxy_returns_none_when_no_env(monkeypatch):
    monkeypatch.delenv("SOCIAL_TIKTOK_POSTS_PROXY_URLS", raising=False)
    monkeypatch.delenv("SOCIAL_TIKTOK_POSTS_PROXY_PROVIDER", raising=False)
    monkeypatch.delenv("DECODO_USERNAME", raising=False)
    monkeypatch.delenv("DECODO_PASSWORD", raising=False)
    from trr_backend.socials.tiktok.posts_scrapling.proxy import select_tiktok_posts_proxy

    assert select_tiktok_posts_proxy() is None


def test_select_tiktok_proxy_returns_none_when_decodo_credentials_exist_without_provider(monkeypatch):
    from trr_backend.socials.tiktok.posts_scrapling.proxy import select_tiktok_posts_proxy

    monkeypatch.delenv("SOCIAL_TIKTOK_POSTS_PROXY_URLS", raising=False)
    monkeypatch.delenv("SOCIAL_TIKTOK_POSTS_PROXY_PROVIDER", raising=False)
    monkeypatch.setenv("DECODO_USERNAME", "user1")
    monkeypatch.setenv("DECODO_PASSWORD", "secret")
    monkeypatch.setenv("DECODO_GATEWAY", "gate.decodo.com:7000")

    assert select_tiktok_posts_proxy() is None

    monkeypatch.setenv("SOCIAL_TIKTOK_POSTS_PROXY_PROVIDER", "")
    assert select_tiktok_posts_proxy() is None


def test_select_tiktok_proxy_explicit_url(monkeypatch):
    from trr_backend.socials.tiktok.posts_scrapling import proxy

    calls: list[Any] = []

    def fake_build_proxy_rotator(selected: Any) -> object:
        calls.append(selected)
        return {"rotator": selected}

    monkeypatch.setenv("SOCIAL_TIKTOK_POSTS_PROXY_URLS", "http://user:pass@proxy:9090")
    monkeypatch.setattr(proxy, "build_proxy_rotator", fake_build_proxy_rotator)

    result = proxy.select_tiktok_posts_proxy()
    assert result is not None
    assert result.browser_proxy == "http://user:pass@proxy:9090"
    assert result.api_proxy_url == "http://user:pass@proxy:9090"
    assert result.proxy_rotator == {"rotator": "http://user:pass@proxy:9090"}
    assert result.fingerprint == "proxy:9090:explicit"
    assert calls == ["http://user:pass@proxy:9090"]


def test_select_tiktok_proxy_decodo(monkeypatch):
    from trr_backend.socials.tiktok.posts_scrapling import proxy

    calls: list[Any] = []

    def fake_build_proxy_rotator(selected: Any) -> object:
        calls.append(selected)
        return {"rotator": selected}

    monkeypatch.delenv("SOCIAL_TIKTOK_POSTS_PROXY_URLS", raising=False)
    monkeypatch.setenv("SOCIAL_TIKTOK_POSTS_PROXY_PROVIDER", "decodo")
    monkeypatch.setenv("DECODO_USERNAME", "user1")
    monkeypatch.setenv("DECODO_PASSWORD", "secret")
    monkeypatch.setenv("DECODO_GATEWAY", "gate.decodo.com:7000")
    monkeypatch.delenv("SOCIAL_TIKTOK_POSTS_USE_STICKY_PROXY", raising=False)
    monkeypatch.setattr(proxy, "build_proxy_rotator", fake_build_proxy_rotator)

    result = proxy.select_tiktok_posts_proxy()
    assert result is not None
    assert isinstance(result.browser_proxy, dict)
    assert result.browser_proxy == {
        "server": "http://gate.decodo.com:7000",
        "username": "user1",
        "password": "secret",
    }
    assert result.api_proxy_url == "http://user1:secret@gate.decodo.com:7000"
    assert result.proxy_rotator == {"rotator": result.browser_proxy}
    assert result.fingerprint == "gate.decodo.com:7000:decodo"
    assert result.session_mode == "rotating"
    assert "-session-" not in result.browser_proxy["username"]
    assert "sessionduration" not in result.api_proxy_url
    assert calls == [result.browser_proxy]


def test_select_tiktok_proxy_decodo_sticky_opt_in(monkeypatch):
    from trr_backend.socials.tiktok.posts_scrapling import proxy

    monkeypatch.delenv("SOCIAL_TIKTOK_POSTS_PROXY_URLS", raising=False)
    monkeypatch.setenv("SOCIAL_TIKTOK_POSTS_PROXY_PROVIDER", "decodo")
    monkeypatch.setenv("SOCIAL_TIKTOK_POSTS_USE_STICKY_PROXY", "true")
    monkeypatch.setenv("SOCIAL_TIKTOK_POSTS_PROXY_SESSION_TTL_SECONDS", "600")
    monkeypatch.setenv("DECODO_USERNAME", "user1")
    monkeypatch.setenv("DECODO_PASSWORD", "secret")
    monkeypatch.setenv("DECODO_GATEWAY", "gate.decodo.com:7000")
    monkeypatch.setattr(proxy, "build_proxy_rotator", lambda selected: {"rotator": selected})

    result = proxy.select_tiktok_posts_proxy(session_key="tiktok:posts:bravotv")

    assert result is not None
    assert isinstance(result.browser_proxy, dict)
    assert result.session_mode == "sticky"
    assert result.api_proxy_url is not None
    assert result.browser_proxy["username"].startswith("user1-session-")
    assert "-sessionduration-10" in result.browser_proxy["username"]
    assert "sessionduration-10" in result.api_proxy_url


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
