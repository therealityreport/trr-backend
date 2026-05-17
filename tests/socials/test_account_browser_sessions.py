from __future__ import annotations

import json

from trr_backend.socials.account_browser_sessions import AccountBrowserSessionManager
from trr_backend.socials.instagram.scraper import InstagramScraper


def test_account_browser_session_manager_imports_storage_state_and_cookies(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("SOCIAL_BROWSER_SESSION_DIR", str(tmp_path))

    manager = AccountBrowserSessionManager(
        platform="instagram",
        cookie_domains=(".instagram.com",),
    )

    paths = manager.import_bootstrapped_session(
        "Bravo TV",
        {
            "cookies": [
                {"name": "sessionid", "value": "session-123", "domain": ".instagram.com"},
                {"name": "csrftoken", "value": "csrf-123", "domain": ".instagram.com"},
            ],
            "origins": [],
        },
    )

    assert paths.account_id == "bravo-tv"
    assert paths.storage_state_path.exists()
    assert paths.cookie_file_path.exists()
    assert (paths.storage_state_path.stat().st_mode & 0o777) == 0o600
    assert (paths.cookie_file_path.stat().st_mode & 0o777) == 0o600
    storage_state = json.loads(paths.storage_state_path.read_text(encoding="utf-8"))
    assert storage_state["cookies"][0]["name"] == "sessionid"
    assert json.loads(paths.cookie_file_path.read_text(encoding="utf-8")) == {
        "csrftoken": "csrf-123",
        "sessionid": "session-123",
    }


def test_account_browser_session_manager_reset_account_context_removes_files(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("SOCIAL_BROWSER_SESSION_DIR", str(tmp_path))

    manager = AccountBrowserSessionManager(
        platform="instagram",
        cookie_domains=(".instagram.com",),
    )
    paths = manager.import_bootstrapped_session(
        "bravotv",
        {"sessionid": "session-123", "csrftoken": "csrf-123"},
    )

    reset_paths = manager.reset_account_context("bravotv")

    assert reset_paths == paths
    assert not paths.storage_state_path.exists()
    assert not paths.cookie_file_path.exists()


def test_instagram_scraper_resolves_explicit_browser_account_id() -> None:
    scraper = InstagramScraper(cookies={}, browser_account_id="account-a")

    assert scraper._resolved_browser_account_id("fallback-account") == "account-a"
