from __future__ import annotations

from typing import Any, cast

from trr_backend.socials.instagram.comments_scrapling import proxy as comments_proxy


def test_explicit_proxy_urls_default_to_first_without_session_key(monkeypatch):
    monkeypatch.setattr(comments_proxy, "_build_proxy_rotator", lambda selected: {"selected": selected})
    monkeypatch.delenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_PROVIDER", raising=False)
    monkeypatch.setenv(
        "SOCIAL_INSTAGRAM_COMMENTS_PROXY_URLS",
        "http://user-one:secret-one@proxy-one.test:8000,http://user-two:secret-two@proxy-two.test:9000",
    )

    config = comments_proxy.select_comments_proxy()

    assert config is not None
    assert config.browser_proxy == "http://user-one:secret-one@proxy-one.test:8000"
    assert config.api_proxy_url == "http://user-one:secret-one@proxy-one.test:8000"
    assert config.fingerprint == "proxy-one.test:8000:explicit"
    assert config.session_mode == "explicit"


def test_decodo_plugin_proxy_url_does_not_enable_comments_proxy_when_provider_none(monkeypatch):
    monkeypatch.setattr(comments_proxy, "_build_proxy_rotator", lambda selected: {"selected": selected})
    monkeypatch.delenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_URLS", raising=False)
    monkeypatch.setenv("DECODO_PROXY_URL", "http://plugin-user:plugin-pass@proxy-plugin.test:7000")
    monkeypatch.setenv("DECODO_USERNAME", "stale-user")
    monkeypatch.setenv("DECODO_PASSWORD", "stale-pass")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_PROVIDER", "none")

    assert comments_proxy.select_comments_proxy() is None


def test_decodo_plugin_proxy_url_requires_explicit_comments_proxy_provider(monkeypatch):
    monkeypatch.setattr(comments_proxy, "_build_proxy_rotator", lambda selected: {"selected": selected})
    monkeypatch.delenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_URLS", raising=False)
    monkeypatch.setenv("DECODO_PROXY_URL", "http://plugin-user:plugin-pass@proxy-plugin.test:7000")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_PROVIDER", "decodo")

    config = comments_proxy.select_comments_proxy()

    assert config is not None
    assert config.browser_proxy == "http://plugin-user:plugin-pass@proxy-plugin.test:7000"
    assert config.api_proxy_url == "http://plugin-user:plugin-pass@proxy-plugin.test:7000"
    assert config.fingerprint == "proxy-plugin.test:7000:explicit"
    assert config.session_mode == "explicit"


def test_explicit_proxy_urls_shard_deterministically_by_session_key(monkeypatch):
    monkeypatch.setattr(comments_proxy, "_build_proxy_rotator", lambda selected: {"selected": selected})
    monkeypatch.delenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_PROVIDER", raising=False)
    monkeypatch.setenv(
        "SOCIAL_INSTAGRAM_COMMENTS_PROXY_URLS",
        ",".join(
            [
                "http://user-one:secret-one@proxy-one.test:8000",
                "http://user-two:secret-two@proxy-two.test:9000",
                "http://user-three:secret-three@proxy-three.test:7000",
            ]
        ),
    )

    first = comments_proxy.select_comments_proxy(session_key="thetraitorsus:comments:1")
    repeated = comments_proxy.select_comments_proxy(session_key="thetraitorsus:comments:1")
    shard_values = {
        cast(Any, comments_proxy.select_comments_proxy(session_key=f"thetraitorsus:comments:{index}")).browser_proxy
        for index in range(1, 12)
    }

    assert first is not None
    assert repeated is not None
    assert first.browser_proxy == repeated.browser_proxy
    assert first.session_mode == "explicit_sharded"
    assert len(shard_values) > 1


def test_explicit_proxy_fingerprint_never_exposes_credentials(monkeypatch):
    monkeypatch.setattr(comments_proxy, "_build_proxy_rotator", lambda selected: {"selected": selected})
    monkeypatch.delenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_PROVIDER", raising=False)
    monkeypatch.setenv(
        "SOCIAL_INSTAGRAM_COMMENTS_PROXY_URLS",
        "http://sensitive-user:sensitive-password@proxy-one.test:8000",
    )

    config = comments_proxy.select_comments_proxy(session_key="bravotv")

    assert config is not None
    assert config.fingerprint == "proxy-one.test:8000:explicit"
    assert "sensitive-user" not in config.fingerprint
    assert "sensitive-password" not in config.fingerprint


def test_explicit_proxy_rotator_receives_full_list_when_multiple_urls(monkeypatch):
    """Phase 5.1 / audit: ProxyRotator should see every configured URL so
    Scrapling can rotate IPs during warmup. ``browser_proxy`` /
    ``api_proxy_url`` stay pinned to the deterministic per-shard selection so
    api-side requests keep a stable session.
    """
    captured: dict[str, object] = {}

    def _capture(values):
        captured["values"] = values
        return {"rotator_built_with": values}

    monkeypatch.setattr(comments_proxy, "_build_proxy_rotator", _capture)
    monkeypatch.delenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_PROVIDER", raising=False)
    monkeypatch.setenv(
        "SOCIAL_INSTAGRAM_COMMENTS_PROXY_URLS",
        ",".join(
            [
                "http://user-one:secret-one@proxy-one.test:8000",
                "http://user-two:secret-two@proxy-two.test:9000",
                "http://user-three:secret-three@proxy-three.test:7000",
            ]
        ),
    )

    config = comments_proxy.select_comments_proxy(session_key="thetraitorsus:comments:1")

    assert config is not None
    # Rotator received the full list, not a single string.
    assert isinstance(captured["values"], list)
    assert len(captured["values"]) == 3
    assert "http://user-one:secret-one@proxy-one.test:8000" in captured["values"]
    assert "http://user-three:secret-three@proxy-three.test:7000" in captured["values"]
    # The persisted browser_proxy / api_proxy_url stay deterministic single URLs.
    assert config.browser_proxy in captured["values"]
    assert config.api_proxy_url == config.browser_proxy


def test_explicit_proxy_rotator_passes_single_url_when_only_one_configured(monkeypatch):
    """Phase 5.1 edge: a single configured URL must NOT be wrapped as a list."""
    captured: dict[str, object] = {}

    def _capture(value):
        captured["value"] = value
        return {"rotator_built_with": value}

    monkeypatch.setattr(comments_proxy, "_build_proxy_rotator", _capture)
    monkeypatch.delenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_PROVIDER", raising=False)
    monkeypatch.setenv(
        "SOCIAL_INSTAGRAM_COMMENTS_PROXY_URLS",
        "http://user-one:secret-one@proxy-one.test:8000",
    )

    config = comments_proxy.select_comments_proxy(session_key="thetraitorsus:comments:1")

    assert config is not None
    assert captured["value"] == "http://user-one:secret-one@proxy-one.test:8000"


def test_global_decodo_credentials_do_not_enable_comments_proxy_without_provider(monkeypatch):
    monkeypatch.setenv("DECODO_USERNAME", "global-user")
    monkeypatch.setenv("DECODO_PASSWORD", "global-pass")
    monkeypatch.delenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_PROVIDER", raising=False)
    monkeypatch.delenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_URLS", raising=False)
    monkeypatch.delenv("SOCIAL_INSTAGRAM_COMMENTS_FORCE_ROTATING_PROXY", raising=False)
    monkeypatch.delenv("SOCIAL_INSTAGRAM_COMMENTS_USE_STICKY_PROXY", raising=False)

    assert comments_proxy.select_comments_proxy(session_key="bravotv") is None


def test_instagram_public_mode_refuses_comments_proxy_even_when_decodo_is_configured(monkeypatch):
    monkeypatch.setattr(comments_proxy, "_build_proxy_rotator", lambda selected: {"selected": selected})
    monkeypatch.setenv("DECODO_USERNAME", "global-user")
    monkeypatch.setenv("DECODO_PASSWORD", "global-pass")
    monkeypatch.setenv("DECODO_GATEWAY", "gate.decodo.com:7000")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_PROVIDER", "decodo")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_URLS", "http://user:pass@proxy-one.test:8000")

    assert comments_proxy.select_comments_proxy(session_key="bravotv", public_mode=True) is None


def test_explicit_decodo_provider_enables_comments_proxy(monkeypatch):
    monkeypatch.setattr(comments_proxy, "_build_proxy_rotator", lambda selected: {"selected": selected})
    monkeypatch.delenv("DECODO_PROXY_URL", raising=False)
    monkeypatch.setenv("DECODO_USERNAME", "global-user")
    monkeypatch.setenv("DECODO_PASSWORD", "global-pass")
    monkeypatch.setenv("DECODO_GATEWAY", "gate.decodo.com:7000")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_PROVIDER", "decodo")
    monkeypatch.delenv("SOCIAL_INSTAGRAM_COMMENTS_USE_STICKY_PROXY", raising=False)
    monkeypatch.delenv("SOCIAL_INSTAGRAM_COMMENTS_FORCE_ROTATING_PROXY", raising=False)

    config = comments_proxy.select_comments_proxy(session_key="bravotv")

    assert config is not None
    assert isinstance(config.browser_proxy, dict)
    assert config.api_proxy_url is not None
    assert config.browser_proxy["username"] == "global-user"
    assert "-session-" not in config.browser_proxy["username"]
    assert "sessionduration" not in config.api_proxy_url
    assert config.fingerprint == "gate.decodo.com:7000:decodo"
    assert config.session_mode == "rotating"


def test_explicit_decodo_provider_can_opt_into_sticky_comments_proxy(monkeypatch):
    monkeypatch.setattr(comments_proxy, "_build_proxy_rotator", lambda selected: {"selected": selected})
    monkeypatch.delenv("DECODO_PROXY_URL", raising=False)
    monkeypatch.setenv("DECODO_USERNAME", "global-user")
    monkeypatch.setenv("DECODO_PASSWORD", "global-pass")
    monkeypatch.setenv("DECODO_GATEWAY", "gate.decodo.com:7000")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_PROVIDER", "decodo")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_USE_STICKY_PROXY", "true")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_SESSION_TTL_SECONDS", "600")
    monkeypatch.delenv("SOCIAL_INSTAGRAM_COMMENTS_FORCE_ROTATING_PROXY", raising=False)

    config = comments_proxy.select_comments_proxy(session_key="bravotv")

    assert config is not None
    assert isinstance(config.browser_proxy, dict)
    assert config.session_mode == "sticky"
    assert config.api_proxy_url is not None
    assert config.browser_proxy["username"].startswith("global-user-session-")
    assert "-sessionduration-10" in config.browser_proxy["username"]
    assert "sessionduration-10" in config.api_proxy_url


def test_public_mode_returns_none_without_public_proxy_flag(monkeypatch):
    # Default public lane is proxy-free even with Decodo creds present.
    monkeypatch.delenv("SOCIAL_INSTAGRAM_COMMENTS_PUBLIC_PROXY_ENABLED", raising=False)
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_PROVIDER", "decodo")
    monkeypatch.setenv("DECODO_USERNAME", "u")
    monkeypatch.setenv("DECODO_PASSWORD", "p")
    monkeypatch.setenv("DECODO_GATEWAY", "gate.decodo.com:7000")
    assert comments_proxy.select_comments_proxy(public_mode=True, session_key="bravotv:public:0") is None


def test_public_proxy_flag_enables_sticky_per_egress_fingerprint(monkeypatch):
    monkeypatch.setattr(comments_proxy, "_build_proxy_rotator", lambda selected: {"selected": selected})
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_PUBLIC_PROXY_ENABLED", "1")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_PROVIDER", "decodo")
    monkeypatch.delenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_URLS", raising=False)
    monkeypatch.delenv("DECODO_PROXY_URL", raising=False)
    monkeypatch.setenv("DECODO_USERNAME", "u")
    monkeypatch.setenv("DECODO_PASSWORD", "p")
    monkeypatch.setenv("DECODO_GATEWAY", "gate.decodo.com:7000")
    # Even with force-rotating (the production default), the public budgeted lane
    # forces a sticky session so each shard pins a stable egress IP.
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_FORCE_ROTATING_PROXY", "true")

    cfg_a = comments_proxy.select_comments_proxy(public_mode=True, session_key="bravotv:public:0")
    cfg_b = comments_proxy.select_comments_proxy(public_mode=True, session_key="bravotv:public:1")

    assert cfg_a is not None and cfg_b is not None
    assert cfg_a.session_mode == "sticky"
    assert cfg_a.fingerprint.startswith("gate.decodo.com:7000:decodo:")
    # Distinct shards -> distinct fingerprints -> per-egress rate pacing.
    assert cfg_a.fingerprint != cfg_b.fingerprint
    # Same shard -> deterministic, stable fingerprint.
    cfg_a2 = comments_proxy.select_comments_proxy(public_mode=True, session_key="bravotv:public:0")
    assert cfg_a2 is not None
    assert cfg_a2.fingerprint == cfg_a.fingerprint
