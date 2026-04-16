"""Proxy selection and session tests for the Instagram comments Scrapling lane."""

from __future__ import annotations

from types import SimpleNamespace

from trr_backend.socials.instagram.comments_scrapling.proxy import select_comments_proxy
from trr_backend.socials.instagram.comments_scrapling.session import resolve_comments_scrapling_session


def test_select_comments_proxy_prefers_explicit_proxy_urls(monkeypatch) -> None:
    monkeypatch.setenv(
        "SOCIAL_INSTAGRAM_COMMENTS_PROXY_URLS",
        "http://proxy-one:8000, http://proxy-two:9000\nhttp://proxy-three:7000",
    )
    monkeypatch.setenv("DECODO_USERNAME", "ignored-user")
    monkeypatch.setenv("DECODO_PASSWORD", "ignored-pass")

    config = select_comments_proxy()
    assert config is not None
    # Explicit URLs: browser_proxy is a str (first URL), api_proxy_url is the same.
    assert config.browser_proxy == "http://proxy-one:8000"
    assert config.api_proxy_url == "http://proxy-one:8000"
    assert config.proxy_rotator is not None


def test_select_comments_proxy_decodo_browser_proxy_is_dict_with_raw_password(monkeypatch) -> None:
    """Decodo browser proxy must be a dict with un-encoded password so
    Scrapling's ProxyRotator passes it directly to Patchright without
    going through construct_proxy_dict() (which breaks on %3D).
    """
    monkeypatch.delenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_URLS", raising=False)
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_PROVIDER", "decodo")
    monkeypatch.setenv("DECODO_USERNAME", "spq99jlvis")
    monkeypatch.setenv("DECODO_PASSWORD", "z1Snjx5L3xT2ektx=B")
    monkeypatch.setenv("DECODO_GATEWAY", "gate.decodo.com:7000")

    config = select_comments_proxy()
    assert config is not None
    assert isinstance(config.browser_proxy, dict)
    assert config.browser_proxy["password"] == "z1Snjx5L3xT2ektx=B"  # literal =, no %3D
    assert config.browser_proxy["server"] == "http://gate.decodo.com:7000"
    assert config.browser_proxy["username"] == "spq99jlvis"


def test_select_comments_proxy_api_url_has_encoded_credentials(monkeypatch) -> None:
    """httpx needs URL-encoded credentials. The = in the password
    must be %3D in the api_proxy_url."""
    monkeypatch.delenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_URLS", raising=False)
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_PROVIDER", "decodo")
    monkeypatch.setenv("DECODO_USERNAME", "spq99jlvis")
    monkeypatch.setenv("DECODO_PASSWORD", "z1Snjx5L3xT2ektx=B")
    monkeypatch.setenv("DECODO_GATEWAY", "gate.decodo.com:7000")

    config = select_comments_proxy()
    assert config is not None
    assert "%3D" in config.api_proxy_url  # = is encoded
    assert "z1Snjx5L3xT2ektx=B" not in config.api_proxy_url  # raw = is NOT in URL


def test_select_comments_proxy_same_upstream_for_both(monkeypatch) -> None:
    """Both browser_proxy and api_proxy_url must point to the same upstream
    so the IP stays consistent across browser warmup and httpx API calls."""
    monkeypatch.delenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_URLS", raising=False)
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_PROVIDER", "decodo")
    monkeypatch.setenv("DECODO_USERNAME", "user")
    monkeypatch.setenv("DECODO_PASSWORD", "pass")
    monkeypatch.setenv("DECODO_GATEWAY", "gate.decodo.com:7000")

    config = select_comments_proxy()
    assert config is not None
    assert isinstance(config.browser_proxy, dict)
    assert "gate.decodo.com:7000" in config.browser_proxy["server"]
    assert "gate.decodo.com:7000" in config.api_proxy_url


def test_select_comments_proxy_returns_none_when_no_proxy_configured(monkeypatch) -> None:
    monkeypatch.delenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_URLS", raising=False)
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_PROVIDER", "none")
    monkeypatch.delenv("DECODO_USERNAME", raising=False)
    monkeypatch.delenv("DECODO_PASSWORD", raising=False)

    assert select_comments_proxy() is None


def test_resolve_comments_scrapling_session_reuses_instagram_auth_session(monkeypatch) -> None:
    fake_session = SimpleNamespace(
        cookies={"sessionid": "session-cookie", "csrftoken": "csrf-cookie"},
        browser_account_id="bravotv",
    )

    monkeypatch.setattr(
        "trr_backend.socials.instagram.comments_scrapling.session.resolve_instagram_auth_session",
        lambda **_kwargs: fake_session,
    )

    session = resolve_comments_scrapling_session(
        browser_account_id="comment-lane",
        caller_context="unit_test",
    )

    assert session.auth_session is fake_session
    assert session.browser_account_id == "bravotv"
    assert session.cookies == [
        {
            "name": "sessionid",
            "value": "session-cookie",
            "domain": ".instagram.com",
            "path": "/",
        },
        {
            "name": "csrftoken",
            "value": "csrf-cookie",
            "domain": ".instagram.com",
            "path": "/",
        },
    ]
