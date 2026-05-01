from __future__ import annotations

from trr_backend.socials.instagram.comments_scrapling import proxy as comments_proxy


def test_explicit_proxy_urls_default_to_first_without_session_key(monkeypatch):
    monkeypatch.setattr(comments_proxy, "_build_proxy_rotator", lambda selected: {"selected": selected})
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


def test_explicit_proxy_urls_shard_deterministically_by_session_key(monkeypatch):
    monkeypatch.setattr(comments_proxy, "_build_proxy_rotator", lambda selected: {"selected": selected})
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
        comments_proxy.select_comments_proxy(session_key=f"thetraitorsus:comments:{index}").browser_proxy
        for index in range(1, 12)
    }

    assert first is not None
    assert repeated is not None
    assert first.browser_proxy == repeated.browser_proxy
    assert first.session_mode == "explicit_sharded"
    assert len(shard_values) > 1


def test_explicit_proxy_fingerprint_never_exposes_credentials(monkeypatch):
    monkeypatch.setattr(comments_proxy, "_build_proxy_rotator", lambda selected: {"selected": selected})
    monkeypatch.setenv(
        "SOCIAL_INSTAGRAM_COMMENTS_PROXY_URLS",
        "http://sensitive-user:sensitive-password@proxy-one.test:8000",
    )

    config = comments_proxy.select_comments_proxy(session_key="bravotv")

    assert config is not None
    assert config.fingerprint == "proxy-one.test:8000:explicit"
    assert "sensitive-user" not in config.fingerprint
    assert "sensitive-password" not in config.fingerprint
