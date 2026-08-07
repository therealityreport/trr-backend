from __future__ import annotations

from trr_backend.socials.socialblade.proxy import select_socialblade_proxy


def test_select_socialblade_proxy_returns_none_without_env(monkeypatch):
    monkeypatch.delenv("SOCIALBLADE_PROXY_URLS", raising=False)
    monkeypatch.delenv("SOCIALBLADE_PROXY_PROVIDER", raising=False)
    monkeypatch.delenv("DECODO_USERNAME", raising=False)
    monkeypatch.delenv("DECODO_PASSWORD", raising=False)

    assert select_socialblade_proxy() is None


def test_select_socialblade_proxy_prefers_explicit_proxy_urls(monkeypatch):
    monkeypatch.setenv("SOCIALBLADE_PROXY_URLS", "http://user:pass@proxy-a:8080,http://user:pass@proxy-b:8080")
    monkeypatch.setenv("SOCIALBLADE_PROXY_PROVIDER", "decodo")
    monkeypatch.setenv("DECODO_USERNAME", "decodo-user")
    monkeypatch.setenv("DECODO_PASSWORD", "decodo-pass")

    proxy = select_socialblade_proxy(session_key="instagram:bravotv")

    assert proxy is not None
    assert proxy.api_proxy_url in {
        "http://user:pass@proxy-a:8080",
        "http://user:pass@proxy-b:8080",
    }
    assert proxy.fingerprint in {"proxy-a:8080:explicit", "proxy-b:8080:explicit"}
    assert proxy.session_mode == "explicit_sharded"


def test_select_socialblade_proxy_builds_sticky_decodo_proxy(monkeypatch):
    monkeypatch.delenv("SOCIALBLADE_PROXY_URLS", raising=False)
    monkeypatch.setenv("SOCIALBLADE_PROXY_PROVIDER", "decodo")
    monkeypatch.setenv("SOCIALBLADE_USE_STICKY_PROXY", "true")
    monkeypatch.setenv("SOCIALBLADE_PROXY_SESSION_TTL_SECONDS", "600")
    monkeypatch.setenv("DECODO_USERNAME", "decodo-user")
    monkeypatch.setenv("DECODO_PASSWORD", "p@ss!")
    monkeypatch.setenv("DECODO_GATEWAY", "gate.decodo.com:7000")

    proxy = select_socialblade_proxy(session_key="instagram:bravotv")

    assert proxy is not None
    assert isinstance(proxy.browser_proxy, dict)
    assert proxy.api_proxy_url is not None
    assert proxy.browser_proxy["server"] == "http://gate.decodo.com:7000"
    assert proxy.browser_proxy["username"].startswith("decodo-user-session-")
    assert proxy.browser_proxy["password"] == "p@ss!"
    assert proxy.api_proxy_url.startswith("http://decodo-user-session-")
    assert "p%40ss%21" in proxy.api_proxy_url
    assert proxy.fingerprint == "gate.decodo.com:7000:decodo"
    assert proxy.session_mode == "sticky"
