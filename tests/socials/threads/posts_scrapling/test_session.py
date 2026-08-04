from __future__ import annotations

import pytest

from trr_backend.socials.threads.posts_scrapling import session


def test_resolve_threads_posts_session_uses_patchable_loader_and_converts_cookies(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    raw_cookies = {"sessionid": "cookie", "csrftoken": "csrf"}
    monkeypatch.setattr(session, "_load_threads_cookies", lambda: raw_cookies)

    result = session.resolve_threads_posts_session()

    assert result.raw_cookies is raw_cookies
    assert result.cookies == [
        {
            "name": "sessionid",
            "value": "cookie",
            "domain": ".threads.com",
            "path": "/",
        },
        {
            "name": "csrftoken",
            "value": "csrf",
            "domain": ".threads.com",
            "path": "/",
        },
    ]
    assert result.cookie_source == "canonical"


def test_resolve_threads_posts_session_raises_when_loader_returns_no_cookies(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(session, "_load_threads_cookies", lambda: {})

    with pytest.raises(RuntimeError, match="No Threads cookies"):
        session.resolve_threads_posts_session()
