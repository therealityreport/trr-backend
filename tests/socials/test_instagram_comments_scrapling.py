from __future__ import annotations

from types import SimpleNamespace

from trr_backend.socials.instagram.comments_scrapling.proxy import load_proxy_urls_from_env
from trr_backend.socials.instagram.comments_scrapling.session import resolve_comments_scrapling_session


def test_load_proxy_urls_from_env_prefers_explicit_proxy_list(monkeypatch) -> None:
    monkeypatch.setenv(
        "SOCIAL_INSTAGRAM_COMMENTS_PROXY_URLS",
        "http://proxy-one:8000, http://proxy-two:9000\nhttp://proxy-three:7000",
    )
    monkeypatch.setenv("DECODO_USERNAME", "ignored-user")
    monkeypatch.setenv("DECODO_PASSWORD", "ignored-pass")

    assert load_proxy_urls_from_env() == [
        "http://proxy-one:8000",
        "http://proxy-two:9000",
        "http://proxy-three:7000",
    ]


def test_load_proxy_urls_from_env_builds_decodo_fallback(monkeypatch) -> None:
    monkeypatch.delenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_URLS", raising=False)
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_PROXY_PROVIDER", "decodo")
    monkeypatch.setenv("DECODO_USERNAME", "decodo-user")
    monkeypatch.setenv("DECODO_PASSWORD", "decodo-pass")
    monkeypatch.setenv("DECODO_GATEWAY", "gate.decodo.com:7000")

    assert load_proxy_urls_from_env() == ["http://decodo-user:decodo-pass@gate.decodo.com:7000"]


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
