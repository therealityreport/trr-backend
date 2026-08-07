from __future__ import annotations


def test_select_posts_proxy_returns_none_when_no_env(monkeypatch):
    """No proxy env vars → None (local dev mode)."""
    monkeypatch.delenv("SOCIAL_INSTAGRAM_POSTS_PROXY_URLS", raising=False)
    monkeypatch.delenv("DECODO_USERNAME", raising=False)
    monkeypatch.delenv("DECODO_PASSWORD", raising=False)
    from trr_backend.socials.instagram.posts_scrapling.proxy import select_posts_proxy

    result = select_posts_proxy()
    assert result is None


def test_select_posts_proxy_public_mode_refuses_decodo(monkeypatch):
    monkeypatch.delenv("SOCIAL_INSTAGRAM_POSTS_PROXY_URLS", raising=False)
    monkeypatch.setenv("SOCIAL_INSTAGRAM_POSTS_PROXY_PROVIDER", "decodo")
    monkeypatch.setenv("DECODO_USERNAME", "user1")
    monkeypatch.setenv("DECODO_PASSWORD", "p@ss!")
    monkeypatch.setenv("DECODO_GATEWAY", "gate.decodo.com:7000")
    from trr_backend.socials.instagram.posts_scrapling.proxy import select_posts_proxy

    assert select_posts_proxy(public_mode=True) is None


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


def test_select_posts_proxy_decodo_plugin_proxy_url(monkeypatch):
    monkeypatch.delenv("SOCIAL_INSTAGRAM_POSTS_PROXY_URLS", raising=False)
    monkeypatch.setenv("DECODO_PROXY_URL", "http://plugin-user:plugin-pass@proxy-plugin.test:7000")
    monkeypatch.setenv("DECODO_USERNAME", "decodo_user")
    monkeypatch.setenv("DECODO_PASSWORD", "decodo_pass")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_POSTS_PROXY_PROVIDER", "none")
    from trr_backend.socials.instagram.posts_scrapling.proxy import select_posts_proxy

    result = select_posts_proxy()

    assert result is not None
    assert result.api_proxy_url == "http://plugin-user:plugin-pass@proxy-plugin.test:7000"
    assert result.fingerprint == "proxy-plugin.test:7000:explicit"


def test_select_posts_proxy_decodo(monkeypatch):
    """DECODO credentials → dict-based browser proxy + URL-encoded API proxy."""
    monkeypatch.delenv("SOCIAL_INSTAGRAM_POSTS_PROXY_URLS", raising=False)
    monkeypatch.delenv("SOCIAL_INSTAGRAM_POSTS_USE_STICKY_PROXY", raising=False)
    monkeypatch.setenv("SOCIAL_INSTAGRAM_POSTS_PROXY_PROVIDER", "decodo")
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
    assert result.api_proxy_url is not None
    assert "p%40ss%21" in result.api_proxy_url
    assert "-session-" not in result.browser_proxy["username"]
    assert "sessionduration" not in result.api_proxy_url
    assert result.fingerprint == "gate.decodo.com:7000:decodo"
    assert result.session_mode == "rotating"


def test_select_posts_proxy_ignores_decodo_credentials_without_provider(monkeypatch):
    """Decodo creds alone should not move the posts warmup onto a proxy."""
    monkeypatch.delenv("SOCIAL_INSTAGRAM_POSTS_PROXY_URLS", raising=False)
    monkeypatch.delenv("SOCIAL_INSTAGRAM_POSTS_PROXY_PROVIDER", raising=False)
    monkeypatch.setenv("DECODO_USERNAME", "user1")
    monkeypatch.setenv("DECODO_PASSWORD", "p@ss!")
    monkeypatch.setenv("DECODO_GATEWAY", "gate.decodo.com:7000")
    from trr_backend.socials.instagram.posts_scrapling.proxy import select_posts_proxy

    assert select_posts_proxy() is None


def test_select_posts_proxy_decodo_sticky_session(monkeypatch):
    """Sticky session env should scope both browser and http transports to one proxy identity."""
    monkeypatch.delenv("SOCIAL_INSTAGRAM_POSTS_PROXY_URLS", raising=False)
    monkeypatch.setenv("SOCIAL_INSTAGRAM_POSTS_PROXY_PROVIDER", "decodo")
    monkeypatch.setenv("DECODO_USERNAME", "user1")
    monkeypatch.setenv("DECODO_PASSWORD", "p@ss!")
    monkeypatch.setenv("DECODO_GATEWAY", "gate.decodo.com:7000")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_POSTS_USE_STICKY_PROXY", "true")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_POSTS_PROXY_SESSION_TTL_SECONDS", "600")
    from trr_backend.socials.instagram.posts_scrapling.proxy import select_posts_proxy

    result = select_posts_proxy()
    assert result is not None
    assert result.session_mode == "sticky"
    assert isinstance(result.browser_proxy, dict)
    assert result.api_proxy_url is not None
    assert "-session-" in result.browser_proxy["username"]
    assert "-sessionduration-10" in result.browser_proxy["username"]
    assert "sessionduration-10" in result.api_proxy_url


def test_proxy_diagnostics_redact_credentials():
    from trr_backend.socials.instagram.posts_scrapling.proxy import (
        PostsProxyConfig,
        build_posts_proxy_identity,
        redact_proxy_url,
    )

    config = PostsProxyConfig(
        browser_proxy={"server": "http://proxy.example:8080", "username": "user1", "password": "secret"},
        api_proxy_url="http://user1:secret@proxy.example:8080",
        proxy_rotator=None,
        fingerprint="proxy.example:8080:explicit",
        session_mode="explicit",
    )

    metadata = build_posts_proxy_identity(config).to_metadata()
    serialized = repr(metadata)

    assert redact_proxy_url(config.api_proxy_url) == "http://***:***@proxy.example:8080"
    assert "secret" not in serialized
    assert "user1" not in serialized
    assert metadata["redacted_browser_proxy"]["password"] == "***"
    assert metadata["redacted_browser_proxy"]["username"] == "***"


def test_proxy_pacing_identity_uses_observed_fingerprint_only_when_enabled(monkeypatch):
    from trr_backend.socials.instagram.posts_scrapling.proxy import PostsProxyConfig, build_posts_proxy_identity

    config = PostsProxyConfig(
        browser_proxy="http://user:pass@proxy.example:8080",
        api_proxy_url="http://user:pass@proxy.example:8080",
        proxy_rotator=None,
        fingerprint="proxy.example:8080:explicit",
        session_mode="explicit",
    )

    monkeypatch.delenv("SOCIAL_INSTAGRAM_POSTS_PER_IP_PACING_ENABLED", raising=False)
    disabled = build_posts_proxy_identity(
        config,
        observed_identity="203.0.113.10",
        observed_fingerprint="203.0.113.10:asn64500",
    )
    enabled = build_posts_proxy_identity(
        config,
        observed_identity="203.0.113.10",
        observed_fingerprint="203.0.113.10:asn64500",
        per_ip_pacing_enabled=True,
    )

    assert disabled.pacing_identity == "instagram:global"
    assert enabled.pacing_identity == "203.0.113.10:asn64500"
    assert enabled.observed_identity == "203.0.113.10"
    assert enabled.observed_fingerprint == "203.0.113.10:asn64500"


def test_posts_proxy_acceleration_flags_default_disabled(monkeypatch):
    from trr_backend.socials.instagram.posts_scrapling.proxy import posts_proxy_feature_flags

    monkeypatch.delenv("SOCIAL_INSTAGRAM_POSTS_PER_IP_PACING_ENABLED", raising=False)
    monkeypatch.delenv("SOCIAL_INSTAGRAM_POSTS_PAGE_PROXY_ROTATION_ENABLED", raising=False)

    assert posts_proxy_feature_flags() == {
        "per_ip_pacing_enabled": False,
        "page_proxy_rotation_enabled": False,
    }


def test_select_posts_proxy_rotates_explicit_urls_per_page_when_enabled(monkeypatch):
    from trr_backend.socials.instagram.posts_scrapling.proxy import select_posts_proxy

    monkeypatch.setenv(
        "SOCIAL_INSTAGRAM_POSTS_PROXY_URLS",
        "http://user:pass@proxy-a:8080,http://user:pass@proxy-b:8080",
    )
    monkeypatch.setenv("SOCIAL_INSTAGRAM_POSTS_PAGE_PROXY_ROTATION_ENABLED", "1")

    first = select_posts_proxy(session_key="same-shard", page_index=0)
    second = select_posts_proxy(session_key="same-shard", page_index=1)
    third = select_posts_proxy(session_key="same-shard", page_index=2)

    assert first is not None and first.fingerprint == "proxy-a:8080:explicit"
    assert second is not None and second.fingerprint == "proxy-b:8080:explicit"
    assert third is not None and third.fingerprint == "proxy-a:8080:explicit"
    assert second.session_mode == "explicit_page_rotation"
    assert second.rotation_index == 1


def test_resolve_posts_scrapling_session(monkeypatch):
    """Session adapter wraps auth_resolver and converts cookies."""
    from unittest.mock import MagicMock

    mock_auth = MagicMock()
    mock_auth.cookies = {"sessionid": "abc123", "csrftoken": "xyz"}
    mock_auth.browser_account_id = "test_account"
    mock_auth.session_account_id = "test_account"

    monkeypatch.setattr(
        "trr_backend.socials.instagram.posts_scrapling.session.resolve_instagram_auth_session",
        lambda **kw: mock_auth,
    )
    from trr_backend.socials.instagram.posts_scrapling.session import resolve_posts_scrapling_session

    result = resolve_posts_scrapling_session(browser_account_id="test_account", caller_context="test")
    assert result.browser_account_id == "test_account"
    assert len(result.cookies) == 2
    assert result.cookies[0]["domain"] == ".instagram.com"
    assert result.auth_session is mock_auth
