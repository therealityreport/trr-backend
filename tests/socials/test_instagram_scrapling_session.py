from __future__ import annotations

from trr_backend.socials.instagram.auth_resolver import InstagramAuthSession
from trr_backend.socials.instagram.scrapling_session import scrapling_session_from_auth


def _auth_session(
    *,
    browser_account_id: str | None,
    session_account_id: str | None,
) -> InstagramAuthSession:
    return InstagramAuthSession(
        cookies={"sessionid": "session", "csrftoken": "csrf"},
        source="env_json",
        validated=True,
        validation_reason=None,
        validation_category="validated",
        stale_ok=False,
        browser_account_id=browser_account_id,
        session_account_id=session_account_id,
        caller_context=None,
        cookie_file_path=None,
        storage_state_path=None,
        refreshed=False,
        refresh_method=None,
        repaired_from_browser_session=False,
        metadata={},
    )


def test_scrapling_session_uses_cookie_session_account_as_effective_browser_identity() -> None:
    auth_session = _auth_session(browser_account_id="thetraitorsus", session_account_id="codexhuli")

    session = scrapling_session_from_auth(auth_session, browser_account_id="thetraitorsus")

    assert session.browser_account_id == "codexhuli"
    assert session.cookies == [
        {"name": "sessionid", "value": "session", "domain": ".instagram.com", "path": "/"},
        {"name": "csrftoken", "value": "csrf", "domain": ".instagram.com", "path": "/"},
    ]


def test_scrapling_session_falls_back_to_requested_browser_identity_without_cookie_account() -> None:
    auth_session = _auth_session(browser_account_id=None, session_account_id=None)

    session = scrapling_session_from_auth(auth_session, browser_account_id="thetraitorsus")

    assert session.browser_account_id == "thetraitorsus"
