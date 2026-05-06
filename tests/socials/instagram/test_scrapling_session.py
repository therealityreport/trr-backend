from __future__ import annotations

from types import SimpleNamespace


def test_cookies_to_scrapling_filters_blank_values() -> None:
    from trr_backend.socials.instagram.scrapling_session import cookies_to_scrapling

    result = cookies_to_scrapling({"sessionid": "abc", "csrftoken": "", " ds_user_id ": " 123 "})

    assert result == [
        {"name": "sessionid", "value": "abc", "domain": ".instagram.com", "path": "/"},
        {"name": "ds_user_id", "value": "123", "domain": ".instagram.com", "path": "/"},
    ]


def test_posts_and_comments_sessions_share_adapter(monkeypatch) -> None:
    from trr_backend.socials.instagram.comments_scrapling.session import resolve_comments_scrapling_session
    from trr_backend.socials.instagram.posts_scrapling.session import resolve_posts_scrapling_session

    auth_session = SimpleNamespace(
        cookies={"sessionid": "abc"},
        browser_account_id="bravotv",
        metadata={"source": "test"},
    )

    monkeypatch.setattr(
        "trr_backend.socials.instagram.posts_scrapling.session.resolve_instagram_auth_session",
        lambda **_kwargs: auth_session,
    )
    monkeypatch.setattr(
        "trr_backend.socials.instagram.comments_scrapling.session.resolve_instagram_comments_auth_session",
        lambda **_kwargs: auth_session,
    )

    posts = resolve_posts_scrapling_session(browser_account_id="bravotv", caller_context="posts")
    comments = resolve_comments_scrapling_session(browser_account_id="bravotv", caller_context="comments")

    assert posts.cookies == comments.cookies
    assert posts.auth_session is auth_session
    assert comments.auth_session is auth_session
    assert posts.browser_account_id == "bravotv"
    assert comments.browser_account_id == "bravotv"
