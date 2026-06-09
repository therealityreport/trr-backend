from __future__ import annotations

from trr_backend.socials.socialblade import proxy as socialblade_proxy


def test_select_socialblade_decodo_defaults_to_rotating(monkeypatch):
    monkeypatch.setattr(socialblade_proxy, "_build_proxy_rotator", lambda selected: {"selected": selected})
    monkeypatch.delenv("SOCIALBLADE_PROXY_URLS", raising=False)
    monkeypatch.delenv("SOCIALBLADE_USE_STICKY_PROXY", raising=False)
    monkeypatch.setenv("SOCIALBLADE_PROXY_PROVIDER", "decodo")
    monkeypatch.setenv("DECODO_USERNAME", "decodo-user")
    monkeypatch.setenv("DECODO_PASSWORD", "secret")
    monkeypatch.setenv("DECODO_GATEWAY", "gate.decodo.com:7000")

    config = socialblade_proxy.select_socialblade_proxy(session_key="instagram:bravotv")

    assert config is not None
    assert isinstance(config.browser_proxy, dict)
    assert config.browser_proxy["username"] == "decodo-user"
    assert "-session-" not in config.browser_proxy["username"]
    assert "sessionduration" not in config.api_proxy_url
    assert config.session_mode == "rotating"


def test_select_socialblade_decodo_sticky_opt_in(monkeypatch):
    monkeypatch.setattr(socialblade_proxy, "_build_proxy_rotator", lambda selected: {"selected": selected})
    monkeypatch.delenv("SOCIALBLADE_PROXY_URLS", raising=False)
    monkeypatch.setenv("SOCIALBLADE_PROXY_PROVIDER", "decodo")
    monkeypatch.setenv("SOCIALBLADE_USE_STICKY_PROXY", "true")
    monkeypatch.setenv("SOCIALBLADE_PROXY_SESSION_TTL_SECONDS", "600")
    monkeypatch.setenv("DECODO_USERNAME", "decodo-user")
    monkeypatch.setenv("DECODO_PASSWORD", "secret")
    monkeypatch.setenv("DECODO_GATEWAY", "gate.decodo.com:7000")

    config = socialblade_proxy.select_socialblade_proxy(session_key="instagram:bravotv")

    assert config is not None
    assert isinstance(config.browser_proxy, dict)
    assert config.browser_proxy["username"].startswith("decodo-user-session-")
    assert "-sessionduration-10" in config.browser_proxy["username"]
    assert "sessionduration-10" in config.api_proxy_url
    assert config.session_mode == "sticky"
