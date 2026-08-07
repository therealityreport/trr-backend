from __future__ import annotations

import asyncio
import fcntl
import multiprocessing as mp
import time
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from multiprocessing.synchronize import Event as MpSyncEvent

import pytest

from trr_backend.socials.account_browser_sessions import AccountBrowserSessionManager
from trr_backend.socials.instagram.auth_resolver import (
    build_authenticated_instagram_scraper,
    clear_instagram_auth_runtime_state,
    resolve_instagram_auth_session,
    resolve_instagram_comments_auth_session,
    resolve_instagram_comments_auth_validation_mode,
)


def _hold_instagram_auth_lock(lock_path: str, hold_seconds: float, acquired_event: MpSyncEvent) -> None:
    with open(lock_path, "a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            acquired_event.set()
            time.sleep(hold_seconds)
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


@pytest.fixture(autouse=True)
def _clear_auth_resolver_state(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    for key in (
        "SOCIAL_INSTAGRAM_COOKIES_JSON",
        "SOCIAL_INSTAGRAM_COOKIES_FILE",
        "INSTAGRAM_COOKIES_FILE",
        "SOCIAL_INSTAGRAM_SESSION_ACCOUNT_ID",
        "SOCIAL_INSTAGRAM_COOKIE_VALIDATION_USERNAME",
        "SOCIAL_AUTH_INSTAGRAM_USERNAME",
        "SOCIAL_AUTH_INSTAGRAM_PASSWORD",
        "INSTAGRAM_USERNAME",
        "INSTAGRAM_PASSWORD",
        "SOCIAL_BROWSER_SESSION_DIR",
        "SOCIAL_INSTAGRAM_COMMENTS_AUTH_VALIDATION",
        "SOCIAL_INSTAGRAM_AUTH_REPAIR_CONFIRMATION",
        "SOCIAL_INSTAGRAM_INTERACTIVE_LOGIN",
        "SOCIAL_INSTAGRAM_BROWSER_MODE",
        "SOCIAL_INSTAGRAM_CHROME_PROFILE",
    ):
        monkeypatch.delenv(key, raising=False)
    clear_instagram_auth_runtime_state()
    yield
    clear_instagram_auth_runtime_state()


def test_resolve_instagram_auth_session_skips_validation_when_disabled(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv(
        "SOCIAL_INSTAGRAM_COOKIES_JSON",
        '{"sessionid":"env-session","csrftoken":"env-csrf","ds_user_id":"123"}',
    )
    monkeypatch.setenv("SOCIAL_INSTAGRAM_SESSION_ACCOUNT_ID", "bravotv")
    monkeypatch.setenv("SOCIAL_BROWSER_SESSION_DIR", str(tmp_path))

    auth_session = resolve_instagram_auth_session(
        browser_account_id="bravotv",
        caller_context="unit_test",
        require_validation=False,
    )

    assert auth_session.cookies["sessionid"] == "env-session"
    # Bug #7 fix: skipping validation must mark the session as validated,
    # otherwise downstream code triggers a forced refresh. The reason +
    # category remain "validation_skipped" so operators can see why.
    assert auth_session.validated is True
    assert auth_session.validation_reason == "validation_skipped"
    assert auth_session.validation_category == "validation_skipped"
    assert auth_session.session_account_id == "bravotv"
    assert auth_session.caller_context == "unit_test"


def test_resolve_instagram_comments_auth_session_defaults_without_profile_graphql(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv(
        "SOCIAL_INSTAGRAM_COOKIES_JSON",
        '{"sessionid":"env-session","csrftoken":"env-csrf","ds_user_id":"123"}',
    )
    monkeypatch.setenv("SOCIAL_INSTAGRAM_SESSION_ACCOUNT_ID", "bravotv")
    monkeypatch.setenv("SOCIAL_BROWSER_SESSION_DIR", str(tmp_path))

    class _UnexpectedInstagramScraper:
        def __init__(self, **_kwargs: object) -> None:
            pass

        def fetch_posts_graphql(self, *_args: object, **_kwargs: object) -> dict[str, object]:
            raise AssertionError("comments default auth resolution must not call profile GraphQL")

    monkeypatch.setattr("trr_backend.socials.instagram.scraper.InstagramScraper", _UnexpectedInstagramScraper)

    auth_session = resolve_instagram_comments_auth_session(
        browser_account_id="bravotv",
        caller_context="comments_scrapling:profile:bravotv",
    )

    assert auth_session.cookies["sessionid"] == "env-session"
    assert auth_session.validated is True
    assert auth_session.validation_category == "validation_skipped"
    assert auth_session.metadata["comments_auth_validation_mode"] == "comments_endpoint"
    assert auth_session.metadata["comments_profile_graphql_validation"] is False


def test_resolve_instagram_auth_session_prefers_complete_env_over_partial_browser_session(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv(
        "SOCIAL_INSTAGRAM_COOKIES_JSON",
        '{"sessionid":"env-session","csrftoken":"env-csrf","ds_user_id":"123"}',
    )
    monkeypatch.setenv("SOCIAL_INSTAGRAM_SESSION_ACCOUNT_ID", "bravotv")
    monkeypatch.setenv("SOCIAL_BROWSER_SESSION_DIR", str(tmp_path / "browser-sessions"))

    manager = AccountBrowserSessionManager(platform="instagram", cookie_domains=(".instagram.com",))
    manager.import_bootstrapped_session("bravotv", {"sessionid": "partial-browser-session"})

    auth_session = resolve_instagram_auth_session(
        browser_account_id="bravotv",
        caller_context="unit_test",
        require_validation=False,
        browser_session_manager=manager,
    )

    assert auth_session.source == "env_json"
    assert auth_session.cookies == {
        "sessionid": "env-session",
        "csrftoken": "env-csrf",
        "ds_user_id": "123",
    }
    assert auth_session.metadata["browser_session_used"] is False


def test_resolve_instagram_comments_auth_session_schema_only_skips_profile_graphql(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def _fake_resolve(**kwargs: object) -> SimpleNamespace:
        captured.update(kwargs)
        return SimpleNamespace(
            cookies={"sessionid": "env-session", "csrftoken": "env-csrf"},
            browser_account_id="bravotv",
            metadata={},
        )

    monkeypatch.setattr(
        "trr_backend.socials.instagram.auth_resolver.resolve_instagram_auth_session",
        _fake_resolve,
    )

    auth_session = resolve_instagram_comments_auth_session(
        browser_account_id="bravotv",
        caller_context="comments_scrapling:schema:bravotv",
        validation_mode="schema_only",
    )

    assert captured["require_validation"] is False
    assert auth_session.metadata["comments_auth_validation_mode"] == "schema_only"
    assert auth_session.metadata["comments_profile_graphql_validation"] is False


def test_resolve_instagram_comments_auth_session_graphql_profile_can_validate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def _fake_resolve(**kwargs: object) -> SimpleNamespace:
        captured.update(kwargs)
        return SimpleNamespace(
            cookies={"sessionid": "env-session", "csrftoken": "env-csrf"},
            browser_account_id="bravotv",
            metadata={},
        )

    monkeypatch.setattr(
        "trr_backend.socials.instagram.auth_resolver.resolve_instagram_auth_session",
        _fake_resolve,
    )

    auth_session = resolve_instagram_comments_auth_session(
        browser_account_id="bravotv",
        caller_context="comments_scrapling:profile:bravotv",
        validation_mode="graphql_profile",
    )

    assert captured["require_validation"] is True
    assert auth_session.metadata["comments_auth_validation_mode"] == "graphql_profile"
    assert auth_session.metadata["comments_profile_graphql_validation"] is True


def test_instagram_auth_resolver_validation_disables_recovery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from trr_backend.socials.instagram import auth_resolver

    captured: dict[str, Any] = {}

    class _FakeScraper:
        last_retrieval_meta: dict[str, object] = {}

        def __init__(self, *, cookies: dict[str, str], browser_account_id: str | None = None) -> None:
            captured["cookies"] = dict(cookies)
            captured["browser_account_id"] = browser_account_id

        def fetch_posts_graphql(self, username: str, **kwargs: object) -> dict[str, object]:
            captured["username"] = username
            captured["kwargs"] = dict(kwargs)
            return {
                "data": {
                    "xdt_api__v1__feed__user_timeline_graphql_connection": {
                        "edges": [{"node": {"id": "1"}}],
                    },
                },
            }

    monkeypatch.setattr("trr_backend.socials.instagram.scraper.InstagramScraper", _FakeScraper)

    valid, reason, category, stale_ok = auth_resolver._validate_cookies_via_graphql(  # noqa: SLF001
        {"sessionid": "session", "csrftoken": "csrf", "ds_user_id": "123"},
        session_account_id="bravotv",
        require_validation=True,
    )

    assert (valid, reason, category, stale_ok) == (True, None, "validated", False)
    assert captured["browser_account_id"] == "bravotv"
    assert captured["username"] == "bravotv"
    assert captured["kwargs"]["allow_browser_fallback"] is False
    assert captured["kwargs"]["allow_recovery"] is False


def test_resolve_instagram_comments_auth_validation_mode_invalid_falls_back(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COMMENTS_AUTH_VALIDATION", "bad-mode")

    assert resolve_instagram_comments_auth_validation_mode() == "comments_endpoint"
    assert "Invalid SOCIAL_INSTAGRAM_COMMENTS_AUTH_VALIDATION" in caplog.text


def test_resolve_instagram_auth_session_normalizes_invalid_session_key(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("SOCIAL_BROWSER_SESSION_DIR", str(tmp_path))
    monkeypatch.setenv("SOCIAL_INSTAGRAM_SESSION_ACCOUNT_ID", "bravotv")

    manager = AccountBrowserSessionManager(platform="instagram", cookie_domains=(".instagram.com",))
    manager.import_bootstrapped_session(
        "bravotv",
        {"sessionid": "browser-session", "csrftoken": "browser-csrf", "ds_user_id": "456"},
    )

    auth_session = resolve_instagram_auth_session(
        browser_account_id="comment-avatar-refresh",
        require_validation=False,
        browser_session_manager=manager,
    )

    assert auth_session.session_account_id == "bravotv"
    assert auth_session.caller_context == "comment-avatar-refresh"
    assert auth_session.cookies["sessionid"] == "browser-session"
    assert auth_session.source in {"browser_session", "browser_session_promoted"}


def test_resolve_instagram_auth_session_uses_login_account_for_target_handle(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("SOCIAL_BROWSER_SESSION_DIR", str(tmp_path))
    monkeypatch.setenv("SOCIAL_AUTH_INSTAGRAM_USERNAME", "thommycodex")

    manager = AccountBrowserSessionManager(platform="instagram", cookie_domains=(".instagram.com",))
    manager.import_bootstrapped_session(
        "thommycodex",
        {"sessionid": "browser-session", "csrftoken": "browser-csrf", "ds_user_id": "456"},
    )

    auth_session = resolve_instagram_auth_session(
        browser_account_id="thetraitorsus",
        require_validation=False,
        browser_session_manager=manager,
    )

    assert auth_session.session_account_id == "thommycodex"
    assert auth_session.caller_context == "thetraitorsus"
    assert auth_session.cookies["sessionid"] == "browser-session"
    assert auth_session.source in {"browser_session", "browser_session_promoted"}


def test_resolve_instagram_auth_session_validates_against_target_handle(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("SOCIAL_BROWSER_SESSION_DIR", str(tmp_path / "sessions"))
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COOKIES_FILE", str(tmp_path / "instagram-cookies.json"))
    monkeypatch.setenv("SOCIAL_AUTH_INSTAGRAM_USERNAME", "thommycodex")

    manager = AccountBrowserSessionManager(platform="instagram", cookie_domains=(".instagram.com",))
    manager.import_bootstrapped_session(
        "thommycodex",
        {"sessionid": "browser-session", "csrftoken": "browser-csrf", "ds_user_id": "456"},
    )

    captured: dict[str, object] = {}

    class _FakeInstagramScraper:
        def __init__(self, *, cookies: dict[str, str] | None = None, browser_account_id: str | None = None) -> None:
            captured["browser_account_id"] = browser_account_id
            captured["cookies"] = dict(cookies or {})
            self.last_retrieval_meta = {}

        def fetch_posts_graphql(self, username: str, **_kwargs: object) -> dict[str, object]:
            captured["validation_username"] = username
            return {
                "data": {
                    "xdt_api__v1__feed__user_timeline_graphql_connection": {
                        "edges": [{"node": {"id": "post"}}],
                    },
                },
            }

    monkeypatch.setattr("trr_backend.socials.instagram.scraper.InstagramScraper", _FakeInstagramScraper)

    auth_session = resolve_instagram_auth_session(
        browser_account_id="thetraitorsus",
        caller_context="comments_scrapling:gap:thetraitorsus",
        require_validation=True,
        browser_session_manager=manager,
    )

    assert auth_session.session_account_id == "thommycodex"
    assert auth_session.caller_context == "comments_scrapling:gap:thetraitorsus"
    assert auth_session.validated is True
    assert captured["browser_account_id"] == "thommycodex"
    assert captured["validation_username"] == "thetraitorsus"


def test_resolve_instagram_auth_session_does_not_validate_against_generic_context(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("SOCIAL_BROWSER_SESSION_DIR", str(tmp_path / "sessions"))
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COOKIES_FILE", str(tmp_path / "instagram-cookies.json"))

    manager = AccountBrowserSessionManager(platform="instagram", cookie_domains=(".instagram.com",))
    manager.import_bootstrapped_session(
        "bravotv",
        {"sessionid": "browser-session", "csrftoken": "browser-csrf", "ds_user_id": "456"},
    )

    captured: dict[str, object] = {}

    class _FakeInstagramScraper:
        def __init__(self, *, cookies: dict[str, str] | None = None, browser_account_id: str | None = None) -> None:
            captured["browser_account_id"] = browser_account_id
            captured["cookies"] = dict(cookies or {})
            self.last_retrieval_meta = {}

        def fetch_posts_graphql(self, username: str, **_kwargs: object) -> dict[str, object]:
            captured["validation_username"] = username
            return {
                "data": {
                    "xdt_api__v1__feed__user_timeline_graphql_connection": {
                        "edges": [{"node": {"id": "post"}}],
                    },
                },
            }

    monkeypatch.setattr("trr_backend.socials.instagram.scraper.InstagramScraper", _FakeInstagramScraper)

    auth_session = resolve_instagram_auth_session(
        browser_account_id="bravotv",
        caller_context="manual_probe",
        require_validation=True,
        browser_session_manager=manager,
    )

    assert auth_session.validated is True
    assert captured["browser_account_id"] == "bravotv"
    assert captured["validation_username"] == "bravotv"


def test_refresh_interactively_skips_sync_playwright_inside_async_loop(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from trr_backend.socials.instagram import auth_resolver

    monkeypatch.setenv("SOCIAL_INSTAGRAM_INTERACTIVE_LOGIN", "1")

    def _unexpected_interactive_login(**_kwargs: object) -> dict[str, str]:
        raise AssertionError("interactive login should not run inside async worker loop")

    monkeypatch.setattr(auth_resolver, "interactive_chrome_login", _unexpected_interactive_login)

    async def _probe() -> tuple[dict[str, str], str | None]:
        return auth_resolver._refresh_interactively(
            session_account_id="thommycodex",
            cookie_file_path=tmp_path / "cookies.json",
        )

    assert asyncio.run(_probe()) == ({}, None)


def test_refresh_interactively_defaults_to_headless_codex_profile(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from trr_backend.socials.instagram import auth_resolver

    captured: dict[str, object] = {}
    monkeypatch.setenv("SOCIAL_INSTAGRAM_INTERACTIVE_LOGIN", "1")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_AUTH_REPAIR_CONFIRMATION", "I UNDERSTAND INSTAGRAM AUTH RISK")
    monkeypatch.delenv("SOCIAL_INSTAGRAM_BROWSER_MODE", raising=False)
    monkeypatch.delenv("SOCIAL_INSTAGRAM_CHROME_PROFILE", raising=False)
    monkeypatch.delenv("MODAL_TASK_ID", raising=False)
    monkeypatch.delenv("MODAL_ENVIRONMENT", raising=False)

    def _fake_interactive_login(**kwargs: object) -> dict[str, str]:
        captured.update(kwargs)
        return {"sessionid": "fresh-session", "csrftoken": "csrf"}

    monkeypatch.setattr(auth_resolver, "interactive_chrome_login", _fake_interactive_login)

    cookies, method = auth_resolver._refresh_interactively(
        session_account_id="thommycodex",
        cookie_file_path=tmp_path / "cookies.json",
    )

    assert method == "interactive_login"
    assert cookies["sessionid"] == "fresh-session"
    assert captured["chrome_profile_name"] == "codex@thereality.report"
    assert captured["headless"] is True


def test_resolve_instagram_auth_session_repairs_checkpoint_with_interactive_login(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from trr_backend.socials.instagram import auth_resolver

    monkeypatch.setenv("SOCIAL_INSTAGRAM_COOKIES_JSON", '{"sessionid":"stale","csrftoken":"csrf","ds_user_id":"123"}')
    monkeypatch.setenv("SOCIAL_INSTAGRAM_SESSION_ACCOUNT_ID", "bravotv")
    monkeypatch.setenv("SOCIAL_BROWSER_SESSION_DIR", str(tmp_path / "browser-sessions"))
    monkeypatch.setenv("SOCIAL_INSTAGRAM_INTERACTIVE_LOGIN", "1")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_AUTH_REPAIR_CONFIRMATION", "I UNDERSTAND INSTAGRAM AUTH RISK")
    monkeypatch.delenv("MODAL_TASK_ID", raising=False)
    monkeypatch.delenv("MODAL_ENVIRONMENT", raising=False)

    validation_calls: list[dict[str, str]] = []

    def _fake_validate(cookies: dict[str, str], **_kwargs: object) -> tuple[bool, str | None, str, bool]:
        validation_calls.append(dict(cookies))
        if cookies.get("sessionid") == "fresh":
            return True, None, "validated", False
        return False, "checkpoint_required", "checkpoint_required", False

    monkeypatch.setattr(auth_resolver, "_validate_cookies_via_graphql", _fake_validate)
    monkeypatch.setattr(
        auth_resolver,
        "interactive_chrome_login",
        lambda **_kwargs: {"sessionid": "fresh", "csrftoken": "fresh-csrf", "ds_user_id": "456"},
    )

    auth_session = auth_resolver.resolve_instagram_auth_session(
        browser_account_id="bravotv",
        caller_context="posts_launch_auth_probe:bravotv",
    )

    assert [call["sessionid"] for call in validation_calls] == ["stale", "fresh"]
    assert auth_session.validated is True
    assert auth_session.refreshed is True
    assert auth_session.refresh_method == "interactive_login"
    assert auth_session.cookies["sessionid"] == "fresh"


def test_credential_refresh_skips_sync_playwright_inside_async_loop(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from trr_backend.socials.instagram import auth_resolver

    monkeypatch.setenv("SOCIAL_AUTH_INSTAGRAM_USERNAME", "thommycodex")
    monkeypatch.setenv("SOCIAL_AUTH_INSTAGRAM_PASSWORD", "secret")

    def _unexpected_refresh(**_kwargs: object) -> dict[str, str]:
        raise AssertionError("credential refresh should not run inside async worker loop")

    monkeypatch.setattr(auth_resolver, "refresh_instagram_cookies", _unexpected_refresh)

    async def _probe() -> tuple[dict[str, str], str | None]:
        return auth_resolver._refresh_with_credentials(
            session_account_id="thommycodex",
            cookie_file_path=tmp_path / "cookies.json",
        )

    assert asyncio.run(_probe()) == ({}, None)


def test_resolve_instagram_auth_session_promotes_browser_session_with_strict_file_permissions(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    cookie_file = tmp_path / "canonical-instagram-cookies.json"
    monkeypatch.setenv("SOCIAL_BROWSER_SESSION_DIR", str(tmp_path / "browser-sessions"))
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COOKIES_FILE", str(cookie_file))
    monkeypatch.setenv("SOCIAL_INSTAGRAM_SESSION_ACCOUNT_ID", "bravotv")

    manager = AccountBrowserSessionManager(platform="instagram", cookie_domains=(".instagram.com",))
    manager.import_bootstrapped_session(
        "bravotv",
        {"sessionid": "browser-session", "csrftoken": "browser-csrf", "ds_user_id": "456"},
    )

    auth_session = resolve_instagram_auth_session(
        browser_account_id="bravotv",
        require_validation=False,
        browser_session_manager=manager,
    )

    assert auth_session.source == "browser_session_promoted"
    assert cookie_file.exists()
    assert (cookie_file.stat().st_mode & 0o777) == 0o600


def test_resolve_instagram_auth_session_same_process_contention_promotes_once(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from trr_backend.socials.instagram import auth_resolver

    cookie_file = tmp_path / "canonical-instagram-cookies.json"
    monkeypatch.setenv("SOCIAL_BROWSER_SESSION_DIR", str(tmp_path / "browser-sessions"))
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COOKIES_FILE", str(cookie_file))
    monkeypatch.setenv("SOCIAL_INSTAGRAM_SESSION_ACCOUNT_ID", "bravotv")

    manager = AccountBrowserSessionManager(platform="instagram", cookie_domains=(".instagram.com",))
    manager.import_bootstrapped_session(
        "bravotv",
        {"sessionid": "browser-session", "csrftoken": "browser-csrf", "ds_user_id": "456"},
    )

    original_write = auth_resolver._safe_write_cookie_file
    write_calls: list[Path] = []

    def _counting_write(path: Path, cookies: dict[str, str]) -> None:
        write_calls.append(Path(path))
        time.sleep(0.05)
        original_write(path, cookies)

    monkeypatch.setattr(auth_resolver, "_safe_write_cookie_file", _counting_write)

    def _resolve_once(_index: int) -> object:
        return resolve_instagram_auth_session(
            browser_account_id="bravotv",
            require_validation=False,
            browser_session_manager=manager,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(_resolve_once, range(2)))

    assert len(results) == 2
    assert len(write_calls) == 1
    assert cookie_file.exists()


def test_resolve_instagram_auth_session_blocks_on_process_file_lock(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("SOCIAL_BROWSER_SESSION_DIR", str(tmp_path / "browser-sessions"))
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COOKIES_FILE", str(tmp_path / "canonical-instagram-cookies.json"))
    monkeypatch.setenv("SOCIAL_INSTAGRAM_SESSION_ACCOUNT_ID", "bravotv")

    manager = AccountBrowserSessionManager(platform="instagram", cookie_domains=(".instagram.com",))
    paths = manager.import_bootstrapped_session(
        "bravotv",
        {"sessionid": "browser-session", "csrftoken": "browser-csrf", "ds_user_id": "456"},
    )
    lock_path = str(paths.cookie_file_path.with_suffix(paths.cookie_file_path.suffix + ".lock"))

    ctx = mp.get_context("spawn")
    acquired_event = ctx.Event()
    proc = ctx.Process(target=_hold_instagram_auth_lock, args=(lock_path, 0.75, acquired_event))
    proc.start()
    try:
        assert acquired_event.wait(timeout=5)
        started = time.monotonic()
        auth_session = resolve_instagram_auth_session(
            browser_account_id="bravotv",
            require_validation=False,
            browser_session_manager=manager,
        )
        elapsed = time.monotonic() - started
    finally:
        proc.join(timeout=5)

    assert proc.exitcode == 0
    assert elapsed >= 0.6
    assert auth_session.cookies["sessionid"] == "browser-session"


def test_build_authenticated_instagram_scraper_returns_none_without_session_cookie(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "trr_backend.socials.instagram.auth_resolver.resolve_instagram_auth_session",
        lambda **_kwargs: type(
            "_Session",
            (),
            {
                "cookies": {},
                "session_account_id": "bravotv",
            },
        )(),
    )

    assert build_authenticated_instagram_scraper(browser_account_id="bravotv", require_validation=False) is None


def test_build_authenticated_instagram_scraper_bootstraps_managed_session(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("SOCIAL_BROWSER_SESSION_DIR", str(tmp_path))
    monkeypatch.setenv("SOCIAL_INSTAGRAM_SESSION_ACCOUNT_ID", "bravotv")

    manager = AccountBrowserSessionManager(platform="instagram", cookie_domains=(".instagram.com",))
    manager.import_bootstrapped_session(
        "bravotv",
        {"sessionid": "browser-session", "csrftoken": "browser-csrf", "ds_user_id": "456"},
    )

    captured: dict[str, object] = {}

    class _FakeInstagramScraper:
        def __init__(self, *, cookies: dict[str, str] | None = None, browser_account_id: str | None = None) -> None:
            captured["cookies"] = dict(cookies or {})
            captured["browser_account_id"] = browser_account_id
            self.cookies = dict(cookies or {})
            self.browser_account_id = browser_account_id
            self.attached_auth_session = None

        def attach_auth_session(self, auth_session: object) -> None:
            self.attached_auth_session = auth_session
            captured["attached_auth_session"] = auth_session

    monkeypatch.setattr("trr_backend.socials.instagram.scraper.InstagramScraper", _FakeInstagramScraper)

    scraper = build_authenticated_instagram_scraper(
        browser_account_id="comment-avatar-refresh",
        caller_context="comment-avatar-refresh",
        require_validation=False,
    )

    assert scraper is not None
    assert captured["cookies"] == {
        "sessionid": "browser-session",
        "csrftoken": "browser-csrf",
        "ds_user_id": "456",
    }
    assert captured["browser_account_id"] == "bravotv"
    auth_session = captured["attached_auth_session"]
    assert getattr(auth_session, "session_account_id", None) == "bravotv"
    assert getattr(auth_session, "caller_context", None) == "comment-avatar-refresh"
