from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

from trr_backend.socials import browser_cookie_refresh
from trr_backend.socials.facebook import cookie_refresh as facebook_cookie_refresh
from trr_backend.socials.instagram import auth_runtime as instagram_auth_runtime
from trr_backend.socials.instagram import cookie_refresh as instagram_cookie_refresh
from trr_backend.socials.instagram.scraper import load_cookies_from_file
from trr_backend.socials.socialblade import auth as socialblade_auth
from trr_backend.socials.threads import cookie_refresh as threads_cookie_refresh
from trr_backend.socials.tiktok import cookie_refresh as tiktok_cookie_refresh
from trr_backend.socials.twitter import cookie_refresh as twitter_cookie_refresh


@pytest.fixture(autouse=True)
def _disable_social_auth_rate_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOCIAL_AUTH_REFRESH_RATE_LIMIT_DISABLED", "true")


def test_tiktok_cookie_refresh_requires_authenticated_session_cookies() -> None:
    assert "ttwid" not in tiktok_cookie_refresh._SPEC.required_cookie_names_any
    assert any(
        "Maximum number of attempts reached" in pattern for pattern in tiktok_cookie_refresh._SPEC.invalid_body_patterns
    )


def test_socialblade_cookie_contract_requires_login_session() -> None:
    assert socialblade_auth.SOCIALBLADE_REQUIRED_COOKIE_NAMES_ANY == ("cf_clearance",)
    assert socialblade_auth.SOCIALBLADE_REQUIRED_COOKIE_NAMES_ALL == ("session",)

    with pytest.raises(RuntimeError, match="missing_required_cookie:session"):
        socialblade_auth.require_socialblade_authenticated_cookies(
            {"cf_clearance": "cloudflare-only"},
            source="SocialBlade test",
        )


def test_socialblade_cookie_loader_prefers_authenticated_candidate(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    cookie_file = tmp_path / "socialblade-cookies.json"
    cookie_file.write_text(
        json.dumps({"cf_clearance": "from-file", "session": "logged-in"}),
        encoding="utf-8",
    )

    monkeypatch.setenv("SOCIALBLADE_COOKIES_JSON", json.dumps({"cf_clearance": "env-cloudflare-only"}))
    monkeypatch.setenv("SOCIALBLADE_COOKIES_FILE", str(cookie_file))

    assert socialblade_auth.load_socialblade_cookies_from_sources() == {
        "cf_clearance": "from-file",
        "session": "logged-in",
    }


def test_facebook_cookie_refresh_detects_verification_checkpoint() -> None:
    assert "/two_step_verification" in facebook_cookie_refresh._SPEC.invalid_url_markers
    assert any("login code" in pattern.lower() for pattern in facebook_cookie_refresh._SPEC.invalid_body_patterns)


def test_threads_cookie_refresh_prefers_instagram_entrypoint() -> None:
    assert any("Instagram" in pattern for pattern in threads_cookie_refresh._SPEC.pre_login_button_patterns)
    assert 'input[name="email"]' in threads_cookie_refresh._SPEC.username_selectors
    assert 'input[name="pass"]' in threads_cookie_refresh._SPEC.password_selectors


def test_instagram_cookie_validation_username_uses_public_fallback_not_login_identity(monkeypatch) -> None:
    monkeypatch.delenv("SOCIAL_INSTAGRAM_COOKIE_VALIDATION_USERNAME", raising=False)
    monkeypatch.delenv("SOCIAL_INSTAGRAM_COOKIE_VALIDATION_FALLBACK_USERNAME", raising=False)
    monkeypatch.setenv("SOCIAL_AUTH_INSTAGRAM_USERNAME", "private-login@example.com")
    monkeypatch.setenv("INSTAGRAM_USERNAME", "legacy-private-login@example.com")

    assert instagram_auth_runtime._instagram_cookie_validation_username() == "instagram"

    monkeypatch.setenv("SOCIAL_INSTAGRAM_COOKIE_VALIDATION_FALLBACK_USERNAME", "BravoTV")
    assert instagram_auth_runtime._instagram_cookie_validation_username() == "bravotv"

    monkeypatch.setenv("SOCIAL_INSTAGRAM_COOKIE_VALIDATION_USERNAME", "@TheTraitorsUS")
    assert instagram_auth_runtime._instagram_cookie_validation_username() == "thetraitorsus"


def test_instagram_cookie_health_probe_disables_repair_side_effects(monkeypatch: pytest.MonkeyPatch) -> None:
    observed_env: dict[str, str | None] = {}

    class _FakeInstagramScraper:
        def __init__(self, **_kwargs: object) -> None:
            self.last_retrieval_meta: dict[str, object] = {}

        def fetch_posts_graphql(self, *_args: object, **_kwargs: object) -> None:
            observed_env["auto_refresh"] = os.getenv("SOCIAL_INSTAGRAM_COOKIE_AUTO_REFRESH")
            observed_env["graphql_recovery_disabled"] = os.getenv("SOCIAL_INSTAGRAM_GRAPHQL_RECOVERY_DISABLED")
            observed_env["interactive_login"] = os.getenv("SOCIAL_INSTAGRAM_INTERACTIVE_LOGIN")
            self.last_retrieval_meta = {
                "error_code": "instagram_graphql_checkpoint_required",
                "error_message": "checkpoint_required",
                "retrieval_transport": "requests_enriched",
            }
            return None

    monkeypatch.setenv("SOCIAL_INSTAGRAM_COOKIE_VALIDATION_USERNAME", "instagram")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COOKIE_AUTO_REFRESH", "true")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_INTERACTIVE_LOGIN", "1")
    monkeypatch.setattr("trr_backend.socials.instagram.InstagramScraper", _FakeInstagramScraper)
    instagram_auth_runtime._instagram_cookie_validation_cache = None

    result = instagram_auth_runtime._inspect_instagram_cookie_health(
        {"sessionid": "stale-session", "csrftoken": "csrf", "ds_user_id": "123"}
    )

    assert result["valid"] is False
    assert result["reason"] == "checkpoint_required"
    assert observed_env == {
        "auto_refresh": "false",
        "graphql_recovery_disabled": "true",
        "interactive_login": "false",
    }
    assert os.getenv("SOCIAL_INSTAGRAM_COOKIE_AUTO_REFRESH") == "true"
    assert os.getenv("SOCIAL_INSTAGRAM_GRAPHQL_RECOVERY_DISABLED") is None
    assert os.getenv("SOCIAL_INSTAGRAM_INTERACTIVE_LOGIN") == "1"


def test_threads_cookie_refresh_falls_back_to_direct_login(monkeypatch, tmp_path: Path) -> None:
    seen_specs: list[object] = []

    def _fake_refresh(*, spec: object, **_: object) -> dict[str, str]:
        seen_specs.append(spec)
        if spec is threads_cookie_refresh._SPEC:
            raise RuntimeError("public shell")
        return {"sessionid": "fresh-session", "csrftoken": "fresh-csrf"}

    monkeypatch.setattr(threads_cookie_refresh, "refresh_simple_login_cookies", _fake_refresh)

    cookies = threads_cookie_refresh.refresh_threads_cookies(
        username="codex@thereality.report",
        password="secret",
        cookie_file=tmp_path / "threads-cookies.json",
        headless=True,
        timeout_seconds=45,
    )

    assert seen_specs == [threads_cookie_refresh._SPEC, threads_cookie_refresh._DIRECT_LOGIN_SPEC]
    assert cookies["sessionid"] == "fresh-session"
    assert cookies["csrftoken"] == "fresh-csrf"


def test_validate_browser_cookie_session_returns_invalid_on_navigation_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakePlaywrightError(Exception):
        pass

    class _FakePage:
        url = "https://www.threads.com/"

        def goto(self, *_args: object, **_kwargs: object) -> None:
            raise _FakePlaywrightError("net::ERR_HTTP_RESPONSE_CODE_FAILURE")

    class _FakeContext:
        def add_cookies(self, _cookies: list[dict[str, object]]) -> None:
            return None

        def new_page(self) -> _FakePage:
            return _FakePage()

    class _FakeBrowser:
        def new_context(self, **_kwargs: object) -> _FakeContext:
            return _FakeContext()

        def close(self) -> None:
            return None

    class _FakePlaywright:
        chromium = object()

        def __enter__(self) -> _FakePlaywright:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

    fake_module = SimpleNamespace(
        Error=_FakePlaywrightError,
        TimeoutError=TimeoutError,
        sync_playwright=lambda: _FakePlaywright(),
    )
    monkeypatch.setitem(sys.modules, "playwright.sync_api", fake_module)
    monkeypatch.setattr(browser_cookie_refresh, "launch_browser", lambda *_args, **_kwargs: _FakeBrowser())

    is_valid, reason = browser_cookie_refresh.validate_browser_cookie_session(
        cookies={"sessionid": "stale-session", "csrftoken": "csrf"},
        validation_url="https://www.threads.com/",
        cookie_domains=(".threads.com",),
        required_cookie_names_any=("sessionid",),
        required_cookie_names_all=("csrftoken",),
    )

    assert is_valid is False
    assert reason == "validation_navigation_failed:_FakePlaywrightError"


def test_validate_browser_cookie_session_rejects_http_error_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _FakeResponse:
        status = 404

    class _FakeLocator:
        def inner_text(self, **_kwargs: object) -> str:
            return ""

    class _FakePage:
        url = "https://www.threads.com/@bravotv"

        def goto(self, *_args: object, **_kwargs: object) -> _FakeResponse:
            return _FakeResponse()

        def wait_for_timeout(self, _timeout_ms: int) -> None:
            return None

        def locator(self, _selector: str) -> _FakeLocator:
            return _FakeLocator()

    class _FakeContext:
        def add_cookies(self, _cookies: list[dict[str, object]]) -> None:
            return None

        def new_page(self) -> _FakePage:
            return _FakePage()

    class _FakeBrowser:
        def new_context(self, **_kwargs: object) -> _FakeContext:
            return _FakeContext()

        def close(self) -> None:
            return None

    class _FakePlaywright:
        chromium = object()

        def __enter__(self) -> _FakePlaywright:
            return self

        def __exit__(self, *_args: object) -> None:
            return None

    fake_module = SimpleNamespace(
        Error=Exception,
        TimeoutError=TimeoutError,
        sync_playwright=lambda: _FakePlaywright(),
    )
    monkeypatch.setitem(sys.modules, "playwright.sync_api", fake_module)
    monkeypatch.setattr(browser_cookie_refresh, "launch_browser", lambda *_args, **_kwargs: _FakeBrowser())

    is_valid, reason = browser_cookie_refresh.validate_browser_cookie_session(
        cookies={"sessionid": "stale-session", "csrftoken": "csrf"},
        validation_url="https://www.threads.com/@bravotv",
        cookie_domains=(".threads.com",),
        required_cookie_names_any=("sessionid",),
        required_cookie_names_all=("csrftoken",),
    )

    assert is_valid is False
    assert reason == "validation_http_status:404"


def test_cookie_refresh_context_defaults_to_codex_chrome_profile(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    chrome_root = tmp_path / "Chrome"
    profile_dir = chrome_root / "Profile 13"
    profile_dir.mkdir(parents=True)
    (profile_dir / "Preferences").write_text(
        json.dumps({"profile": {"name": "codex"}, "account_info": [{"email": "codex@thereality.report"}]}),
        encoding="utf-8",
    )
    captured: dict[str, object] = {}

    class _FakeContext:
        def close(self) -> None:
            captured["closed"] = True

    class _FakeChromium:
        def launch_persistent_context(self, **kwargs: object) -> _FakeContext:
            captured.update(kwargs)
            return _FakeContext()

    monkeypatch.setattr(browser_cookie_refresh, "_chrome_profile_base_dir", lambda: chrome_root)
    monkeypatch.delenv("SOCIAL_AUTH_CHROME_PROFILE", raising=False)
    monkeypatch.delenv("SOCIAL_COOKIE_REFRESH_CHROME_PROFILE", raising=False)
    monkeypatch.delenv("SOCIAL_TIKTOK_CHROME_PROFILE", raising=False)

    session = browser_cookie_refresh.open_cookie_refresh_context(
        SimpleNamespace(chromium=_FakeChromium()),
        platform="tiktok",
        headless=True,
        viewport={"width": 100, "height": 100},
    )
    session.close()

    assert captured["user_data_dir"] == str(chrome_root)
    assert captured["channel"] == "chrome"
    assert captured["headless"] is True
    assert "--profile-directory=Profile 13" in captured["args"]
    assert captured["closed"] is True


def test_cookie_refresh_context_refuses_inner_profile_user_data_dir(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    chrome_root = tmp_path / "Chrome"
    profile_dir = chrome_root / "Profile 13"
    profile_dir.mkdir(parents=True)
    (profile_dir / "Preferences").write_text(
        json.dumps({"profile": {"name": "codex"}, "account_info": [{"email": "codex@thereality.report"}]}),
        encoding="utf-8",
    )

    def _bad_base_dir() -> Path:
        return profile_dir

    class _FakeChromium:
        def launch_persistent_context(self, **_kwargs: object) -> object:
            raise AssertionError("inner profile path should fail before launch")

    monkeypatch.setattr(browser_cookie_refresh, "_chrome_profile_base_dir", _bad_base_dir)
    monkeypatch.delenv("SOCIAL_AUTH_CHROME_PROFILE", raising=False)
    monkeypatch.delenv("SOCIAL_COOKIE_REFRESH_CHROME_PROFILE", raising=False)

    with pytest.raises(browser_cookie_refresh.ChromeProfileNotAvailableError, match="Chrome user-data root"):
        browser_cookie_refresh.open_cookie_refresh_context(
            SimpleNamespace(chromium=_FakeChromium()),
            platform="instagram",
            headless=True,
            viewport={"width": 100, "height": 100},
        )


def test_cookie_refresh_context_refuses_locked_profile_before_launch(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    chrome_root = tmp_path / "Chrome"
    profile_dir = chrome_root / "Profile 13"
    profile_dir.mkdir(parents=True)
    (profile_dir / "Preferences").write_text(
        json.dumps({"profile": {"name": "codex"}, "account_info": [{"email": "codex@thereality.report"}]}),
        encoding="utf-8",
    )
    (chrome_root / "SingletonLock").touch()

    class _FakeChromium:
        def launch_persistent_context(self, **_kwargs: object) -> object:
            raise AssertionError("locked Chrome profile should fail before launch")

    monkeypatch.setattr(browser_cookie_refresh, "_chrome_profile_base_dir", lambda: chrome_root)
    monkeypatch.delenv("SOCIAL_AUTH_CHROME_PROFILE", raising=False)
    monkeypatch.delenv("SOCIAL_COOKIE_REFRESH_CHROME_PROFILE", raising=False)

    with pytest.raises(browser_cookie_refresh.ChromeProfileLockedError, match="Chrome auth profile is locked"):
        browser_cookie_refresh.open_cookie_refresh_context(
            SimpleNamespace(chromium=_FakeChromium()),
            platform="instagram",
            headless=True,
            viewport={"width": 100, "height": 100},
        )


def test_cookie_refresh_context_refuses_profileless_browser_by_default(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    chrome_root = tmp_path / "Chrome"
    chrome_root.mkdir()

    class _FakeChromium:
        def launch(self, **_kwargs: object) -> object:
            raise AssertionError("profile-less browser should not launch")

        def launch_persistent_context(self, **_kwargs: object) -> object:
            raise AssertionError("missing profile should fail before launch")

    monkeypatch.setattr(browser_cookie_refresh, "_chrome_profile_base_dir", lambda: chrome_root)

    with pytest.raises(browser_cookie_refresh.ChromeProfileNotAvailableError):
        browser_cookie_refresh.open_cookie_refresh_context(
            SimpleNamespace(chromium=_FakeChromium()),
            platform="tiktok",
            headless=True,
            viewport={"width": 100, "height": 100},
        )


def test_cookie_refresh_context_allows_profileless_browser_with_override(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    chrome_root = tmp_path / "Chrome"
    captured: dict[str, object] = {}

    class _FakeContext:
        def close(self) -> None:
            captured["context_closed"] = True

    class _FakeBrowser:
        def new_context(self, **kwargs: object) -> _FakeContext:
            captured["context_kwargs"] = kwargs
            return _FakeContext()

        def close(self) -> None:
            captured["browser_closed"] = True

    class _FakeChromium:
        def launch(self, **kwargs: object) -> _FakeBrowser:
            captured["launch_kwargs"] = kwargs
            return _FakeBrowser()

        def launch_persistent_context(self, **_kwargs: object) -> object:
            raise AssertionError("profile-less override should not launch a persistent Chrome profile")

    monkeypatch.setattr(browser_cookie_refresh, "_chrome_profile_base_dir", lambda: chrome_root)

    session = browser_cookie_refresh.open_cookie_refresh_context(
        SimpleNamespace(chromium=_FakeChromium()),
        platform="socialblade",
        headless=True,
        viewport={"width": 100, "height": 100},
        require_profile=False,
    )
    session.close()

    assert captured["launch_kwargs"]["headless"] is True
    assert captured["context_kwargs"] == {"viewport": {"width": 100, "height": 100}}
    assert captured["browser_closed"] is True


def test_cookie_refresh_context_closes_profileless_browser_when_new_context_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    chrome_root = tmp_path / "Chrome"
    captured: dict[str, int] = {"close_calls": 0}

    class _FakeBrowser:
        def new_context(self, **_kwargs: object) -> object:
            raise RuntimeError("new context failed")

        def close(self) -> None:
            captured["close_calls"] += 1

    monkeypatch.setattr(browser_cookie_refresh, "_chrome_profile_base_dir", lambda: chrome_root)
    monkeypatch.setattr(browser_cookie_refresh, "launch_browser", lambda *_args, **_kwargs: _FakeBrowser())

    with pytest.raises(RuntimeError, match="new context failed"):
        browser_cookie_refresh.open_cookie_refresh_context(
            SimpleNamespace(chromium=object()),
            platform="socialblade",
            headless=True,
            viewport={"width": 100, "height": 100},
            require_profile=False,
        )

    assert captured["close_calls"] == 1


def test_social_auth_refresh_rate_limit_blocks_repeated_attempts(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("SOCIAL_AUTH_REFRESH_RATE_LIMIT_DISABLED", "false")
    monkeypatch.setenv("SOCIAL_AUTH_REFRESH_RATE_LIMIT_DIR", str(tmp_path / "locks"))
    monkeypatch.setenv("SOCIAL_AUTH_REFRESH_MIN_INTERVAL_SECONDS", "3600")

    first = browser_cookie_refresh.reserve_social_auth_refresh_attempt("instagram")

    assert first["reserved"] is True
    with pytest.raises(browser_cookie_refresh.SocialAuthRefreshRateLimitError, match="rate-limited"):
        browser_cookie_refresh.reserve_social_auth_refresh_attempt("instagram")


def test_social_auth_refresh_rate_limit_is_per_platform(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("SOCIAL_AUTH_REFRESH_RATE_LIMIT_DISABLED", "false")
    monkeypatch.setenv("SOCIAL_AUTH_REFRESH_RATE_LIMIT_DIR", str(tmp_path / "locks"))
    monkeypatch.setenv("SOCIAL_AUTH_REFRESH_MIN_INTERVAL_SECONDS", "3600")

    instagram = browser_cookie_refresh.reserve_social_auth_refresh_attempt("instagram")
    tiktok = browser_cookie_refresh.reserve_social_auth_refresh_attempt("tiktok")

    assert instagram["platform"] == "instagram"
    assert tiktok["platform"] == "tiktok"


def test_instagram_cookie_refresh_rejects_unvalidated_graphql_session(monkeypatch, tmp_path: Path) -> None:
    writes: list[Path] = []
    imported_sessions: list[object] = []
    monkeypatch.setenv("SOCIAL_AUTH_CHROME_PROFILE", "missing-test-profile")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_CHROME_PROFILE", "missing-test-profile")
    monkeypatch.setenv("SOCIAL_COOKIE_REFRESH_REQUIRE_CHROME_PROFILE", "false")

    class _FakePlaywrightTimeoutError(Exception):
        pass

    class _Locator:
        @property
        def first(self) -> _Locator:
            return self

        def wait_for(self, **_kwargs: object) -> None:
            return None

        def fill(self, *_args: object, **_kwargs: object) -> None:
            return None

        def click(self, **_kwargs: object) -> None:
            return None

        def is_visible(self, **_kwargs: object) -> bool:
            return False

        def inner_text(self, **_kwargs: object) -> str:
            return ""

    class _Page:
        def __init__(self) -> None:
            self.url = instagram_cookie_refresh.INSTAGRAM_LOGIN_URL

        def goto(self, url: str, **_kwargs: object) -> None:
            self.url = url

        def locator(self, *_args: object, **_kwargs: object) -> _Locator:
            return _Locator()

        def get_by_label(self, *_args: object, **_kwargs: object) -> _Locator:
            return _Locator()

        def get_by_role(self, *_args: object, **_kwargs: object) -> _Locator:
            return _Locator()

        def wait_for_timeout(self, *_args: object, **_kwargs: object) -> None:
            return None

    class _Context:
        def new_page(self) -> _Page:
            return _Page()

        def cookies(self) -> list[dict[str, object]]:
            return [
                {"name": "sessionid", "value": "fresh-session", "domain": ".instagram.com"},
                {"name": "csrftoken", "value": "fresh-csrf", "domain": ".instagram.com"},
            ]

        def storage_state(self) -> dict[str, object]:
            return {"cookies": self.cookies(), "origins": []}

    class _Browser:
        def new_context(self, **_kwargs: object) -> _Context:
            return _Context()

        def close(self) -> None:
            return None

    class _PlaywrightContext:
        def __enter__(self) -> SimpleNamespace:
            chromium = SimpleNamespace(launch=lambda **_kwargs: _Browser())
            return SimpleNamespace(chromium=chromium)

        def __exit__(self, *_args: object) -> bool:
            return False

    sync_api_module = ModuleType("playwright.sync_api")
    sync_api_module.TimeoutError = _FakePlaywrightTimeoutError
    sync_api_module.sync_playwright = lambda: _PlaywrightContext()
    playwright_module = ModuleType("playwright")
    playwright_module.sync_api = sync_api_module
    monkeypatch.setitem(sys.modules, "playwright", playwright_module)
    monkeypatch.setitem(sys.modules, "playwright.sync_api", sync_api_module)
    monkeypatch.setattr(instagram_cookie_refresh, "_write_cookie_file", lambda path, cookies: writes.append(Path(path)))
    monkeypatch.setattr(
        instagram_cookie_refresh._INSTAGRAM_BROWSER_SESSIONS,
        "import_bootstrapped_session",
        lambda *args, **kwargs: imported_sessions.append((args, kwargs)),
    )

    with pytest.raises(RuntimeError, match="checkpoint_required"):
        instagram_cookie_refresh.refresh_instagram_cookies(
            username="operator@example.com",
            password="secret",
            cookie_file=tmp_path / "instagram-cookies.json",
            validation_username="bravotv",
            validator=lambda _cookies: (False, "checkpoint_required"),
        )

    assert writes == []
    assert imported_sessions == []


def test_instagram_cookie_refresh_preserves_challenge_error_when_browser_close_fails(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("SOCIAL_AUTH_CHROME_PROFILE", "missing-test-profile")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_CHROME_PROFILE", "missing-test-profile")
    monkeypatch.setenv("SOCIAL_COOKIE_REFRESH_REQUIRE_CHROME_PROFILE", "false")

    class _FakePlaywrightTimeoutError(Exception):
        pass

    class _Locator:
        @property
        def first(self) -> _Locator:
            return self

        def wait_for(self, **_kwargs: object) -> None:
            return None

        def fill(self, *_args: object, **_kwargs: object) -> None:
            return None

        def click(self, **_kwargs: object) -> None:
            return None

        def is_visible(self, **_kwargs: object) -> bool:
            return False

        def inner_text(self, **_kwargs: object) -> str:
            return ""

    class _Page:
        url = "https://www.instagram.com/challenge/?next=https%3A%2F%2Fwww.instagram.com%2Fbravotv%2F"

        def goto(self, *_args: object, **_kwargs: object) -> None:
            return None

        def locator(self, *_args: object, **_kwargs: object) -> _Locator:
            return _Locator()

        def get_by_label(self, *_args: object, **_kwargs: object) -> _Locator:
            return _Locator()

        def get_by_role(self, *_args: object, **_kwargs: object) -> _Locator:
            return _Locator()

        def wait_for_timeout(self, *_args: object, **_kwargs: object) -> None:
            return None

    class _Context:
        def new_page(self) -> _Page:
            return _Page()

        def cookies(self) -> list[dict[str, object]]:
            return []

    class _Browser:
        def new_context(self, **_kwargs: object) -> _Context:
            return _Context()

        def close(self) -> None:
            raise RuntimeError("Event loop is closed! Is Playwright already stopped?")

    class _PlaywrightContext:
        def __enter__(self) -> SimpleNamespace:
            return SimpleNamespace(chromium=SimpleNamespace(launch=lambda **_kwargs: _Browser()))

        def __exit__(self, *_args: object) -> bool:
            return False

    sync_api_module = ModuleType("playwright.sync_api")
    sync_api_module.TimeoutError = _FakePlaywrightTimeoutError
    sync_api_module.sync_playwright = lambda: _PlaywrightContext()
    playwright_module = ModuleType("playwright")
    playwright_module.sync_api = sync_api_module
    monkeypatch.setitem(sys.modules, "playwright", playwright_module)
    monkeypatch.setitem(sys.modules, "playwright.sync_api", sync_api_module)

    with pytest.raises(RuntimeError, match="requires additional verification"):
        instagram_cookie_refresh.refresh_instagram_cookies(
            username="operator@example.com",
            password="secret",
            cookie_file=tmp_path / "instagram-cookies.json",
            validation_username="bravotv",
        )


def test_instagram_cookie_refresh_schema_only_skips_graphql_validator(monkeypatch, tmp_path: Path) -> None:
    writes: list[tuple[Path, dict[str, str]]] = []
    imported_sessions: list[object] = []
    monkeypatch.setenv("SOCIAL_AUTH_CHROME_PROFILE", "missing-test-profile")
    monkeypatch.setenv("SOCIAL_INSTAGRAM_CHROME_PROFILE", "missing-test-profile")
    monkeypatch.setenv("SOCIAL_COOKIE_REFRESH_REQUIRE_CHROME_PROFILE", "false")

    class _FakePlaywrightTimeoutError(Exception):
        pass

    class _Locator:
        @property
        def first(self) -> _Locator:
            return self

        def wait_for(self, **_kwargs: object) -> None:
            return None

        def fill(self, *_args: object, **_kwargs: object) -> None:
            return None

        def click(self, **_kwargs: object) -> None:
            return None

        def is_visible(self, **_kwargs: object) -> bool:
            return False

        def inner_text(self, **_kwargs: object) -> str:
            return ""

    class _Page:
        url = instagram_cookie_refresh.INSTAGRAM_LOGIN_URL

        def goto(self, url: str, **_kwargs: object) -> None:
            self.url = url

        def locator(self, *_args: object, **_kwargs: object) -> _Locator:
            return _Locator()

        def get_by_label(self, *_args: object, **_kwargs: object) -> _Locator:
            return _Locator()

        def get_by_role(self, *_args: object, **_kwargs: object) -> _Locator:
            return _Locator()

        def wait_for_timeout(self, *_args: object, **_kwargs: object) -> None:
            return None

    class _Context:
        def new_page(self) -> _Page:
            return _Page()

        def cookies(self) -> list[dict[str, object]]:
            return [
                {"name": "sessionid", "value": "fresh-session", "domain": ".instagram.com"},
                {"name": "csrftoken", "value": "fresh-csrf", "domain": ".instagram.com"},
            ]

        def storage_state(self) -> dict[str, object]:
            return {"cookies": self.cookies(), "origins": []}

    class _Browser:
        def new_context(self, **_kwargs: object) -> _Context:
            return _Context()

        def close(self) -> None:
            return None

    class _PlaywrightContext:
        def __enter__(self) -> SimpleNamespace:
            chromium = SimpleNamespace(launch=lambda **_kwargs: _Browser())
            return SimpleNamespace(chromium=chromium)

        def __exit__(self, *_args: object) -> bool:
            return False

    sync_api_module = ModuleType("playwright.sync_api")
    sync_api_module.TimeoutError = _FakePlaywrightTimeoutError
    sync_api_module.sync_playwright = lambda: _PlaywrightContext()
    playwright_module = ModuleType("playwright")
    playwright_module.sync_api = sync_api_module
    monkeypatch.setitem(sys.modules, "playwright", playwright_module)
    monkeypatch.setitem(sys.modules, "playwright.sync_api", sync_api_module)
    monkeypatch.setattr(
        instagram_cookie_refresh,
        "_write_cookie_file",
        lambda path, cookies: writes.append((Path(path), dict(cookies))),
    )
    monkeypatch.setattr(
        instagram_cookie_refresh._INSTAGRAM_BROWSER_SESSIONS,
        "import_bootstrapped_session",
        lambda *args, **kwargs: imported_sessions.append((args, kwargs)),
    )

    cookies = instagram_cookie_refresh.refresh_instagram_cookies(
        username="operator@example.com",
        password="secret",
        cookie_file=tmp_path / "instagram-cookies.json",
        validation_username="bravotv",
        validator=lambda _cookies: (_ for _ in ()).throw(AssertionError("validator should not run")),
        validation_mode="schema_only",
    )

    assert cookies["sessionid"] == "fresh-session"
    assert writes == [(tmp_path / "instagram-cookies.json", cookies)]
    assert imported_sessions


def test_instagram_cookie_refresh_writes_refresh_metadata(tmp_path: Path) -> None:
    cookie_file = tmp_path / "instagram-cookies.json"

    instagram_cookie_refresh._write_cookie_file(
        cookie_file,
        {"sessionid": "fresh-session", "csrftoken": "fresh-csrf"},
    )

    payload = json.loads(cookie_file.read_text(encoding="utf-8"))

    assert payload["sessionid"] == "fresh-session"
    assert "_cookie_refreshed_at" in payload
    assert (cookie_file.stat().st_mode & 0o777) == 0o600


def test_load_cookies_from_file_strips_refresh_metadata(tmp_path: Path) -> None:
    cookie_file = tmp_path / "instagram-cookies.json"
    cookie_file.write_text(
        json.dumps(
            {
                "sessionid": "fresh-session",
                "csrftoken": "fresh-csrf",
                "_cookie_refreshed_at": "2026-04-07T12:00:00Z",
            }
        ),
        encoding="utf-8",
    )

    assert load_cookies_from_file(str(cookie_file)) == {
        "sessionid": "fresh-session",
        "csrftoken": "fresh-csrf",
    }


def test_interactive_instagram_login_reuses_saved_browser_session(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("SOCIAL_BROWSER_SESSION_DIR", str(tmp_path))
    paths = instagram_cookie_refresh._INSTAGRAM_BROWSER_SESSIONS.import_bootstrapped_session(  # noqa: SLF001
        "bravotv",
        {"sessionid": "saved-session", "csrftoken": "saved-csrf"},
    )

    validate_calls: list[dict[str, object]] = []
    writes: list[tuple[Path, dict[str, str]]] = []

    monkeypatch.setattr(
        instagram_cookie_refresh,
        "validate_browser_cookie_session",
        lambda **kwargs: validate_calls.append(kwargs) or (True, None),
    )
    monkeypatch.setattr(
        instagram_cookie_refresh,
        "_validate_saved_cookies_via_graphql",
        lambda *args, **kwargs: (True, None),
    )
    monkeypatch.setattr(
        instagram_cookie_refresh,
        "_write_cookie_file",
        lambda path, cookies: writes.append((Path(path), dict(cookies))),
    )
    monkeypatch.setattr(
        instagram_cookie_refresh,
        "_find_chrome_profile_dir",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("persistent Chrome should not launch")),
    )

    cookies = instagram_cookie_refresh.interactive_chrome_login(
        chrome_profile_name="unused-profile",
        cookie_file=tmp_path / "instagram-cookies.json",
        validation_username="bravotv",
        timeout_seconds=120,
        headless=False,
    )

    assert cookies == {"sessionid": "saved-session", "csrftoken": "saved-csrf"}
    assert validate_calls[0]["validation_url"] == "https://www.instagram.com/bravotv/"
    assert writes == [(tmp_path / "instagram-cookies.json", {"sessionid": "saved-session", "csrftoken": "saved-csrf"})]
    assert paths.cookie_file_path.exists()


def test_interactive_instagram_login_comments_mode_reuses_saved_session_without_graphql(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("SOCIAL_BROWSER_SESSION_DIR", str(tmp_path))
    instagram_cookie_refresh._INSTAGRAM_BROWSER_SESSIONS.import_bootstrapped_session(  # noqa: SLF001
        "bravotv",
        {"sessionid": "saved-session", "csrftoken": "saved-csrf"},
    )

    monkeypatch.setattr(instagram_cookie_refresh, "validate_browser_cookie_session", lambda **_kwargs: (True, None))
    monkeypatch.setattr(
        instagram_cookie_refresh,
        "_validate_saved_cookies_via_graphql",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("GraphQL validation should not run")),
    )
    monkeypatch.setattr(instagram_cookie_refresh, "_write_cookie_file", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        instagram_cookie_refresh,
        "_find_chrome_profile_dir",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("persistent Chrome should not launch")),
    )

    cookies = instagram_cookie_refresh.interactive_chrome_login(
        chrome_profile_name="unused-profile",
        cookie_file=tmp_path / "instagram-cookies.json",
        validation_username="bravotv",
        timeout_seconds=120,
        headless=False,
        validation_mode="comments_endpoint",
    )

    assert cookies == {"sessionid": "saved-session", "csrftoken": "saved-csrf"}


def test_interactive_instagram_login_rejects_saved_session_when_graphql_fails(
    monkeypatch,
    tmp_path: Path,
) -> None:
    chrome_root = tmp_path / "Chrome"
    profile_dir = chrome_root / "Profile 13"
    profile_dir.mkdir(parents=True)
    (profile_dir / "Preferences").write_text(
        json.dumps({"profile": {"name": "codex"}, "account_info": [{"email": "codex@thereality.report"}]}),
        encoding="utf-8",
    )
    monkeypatch.setenv("SOCIAL_BROWSER_SESSION_DIR", str(tmp_path))
    monkeypatch.setattr(browser_cookie_refresh, "_chrome_profile_base_dir", lambda: chrome_root)
    instagram_cookie_refresh._INSTAGRAM_BROWSER_SESSIONS.import_bootstrapped_session(  # noqa: SLF001
        "bravotv",
        {"sessionid": "saved-session", "csrftoken": "saved-csrf"},
    )

    writes: list[tuple[Path, dict[str, str]]] = []
    monkeypatch.setattr(instagram_cookie_refresh, "validate_browser_cookie_session", lambda **_kwargs: (True, None))
    monkeypatch.setattr(
        instagram_cookie_refresh,
        "_validate_saved_cookies_via_graphql",
        lambda *args, **kwargs: (False, "checkpoint_required"),
    )
    monkeypatch.setattr(
        instagram_cookie_refresh,
        "_write_cookie_file",
        lambda path, cookies: writes.append((Path(path), dict(cookies))),
    )

    class _FakeChromium:
        def launch_persistent_context(self, **_kwargs: object) -> object:
            raise RuntimeError("persistent Chrome launched")

    class _PlaywrightContext:
        def __enter__(self) -> SimpleNamespace:
            return SimpleNamespace(chromium=_FakeChromium())

        def __exit__(self, *_args: object) -> bool:
            return False

    sync_api_module = ModuleType("playwright.sync_api")
    sync_api_module.TimeoutError = TimeoutError
    sync_api_module.sync_playwright = lambda: _PlaywrightContext()
    playwright_module = ModuleType("playwright")
    playwright_module.sync_api = sync_api_module
    monkeypatch.setitem(sys.modules, "playwright", playwright_module)
    monkeypatch.setitem(sys.modules, "playwright.sync_api", sync_api_module)

    with pytest.raises(RuntimeError, match="persistent Chrome launched"):
        instagram_cookie_refresh.interactive_chrome_login(
            chrome_profile_name="codex@thereality.report",
            cookie_file=tmp_path / "instagram-cookies.json",
            validation_username="bravotv",
            timeout_seconds=120,
            headless=False,
        )

    assert writes == []


def test_instagram_saved_cookie_validation_uses_profile_posts_graphql(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

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

    valid, reason = instagram_cookie_refresh._validate_saved_cookies_via_graphql(  # noqa: SLF001
        {"sessionid": "session", "csrftoken": "csrf", "ds_user_id": "123"},
        validation_username="bravotv",
        timeout_seconds=120,
    )

    assert valid is True
    assert reason is None
    assert captured["browser_account_id"] == "bravotv"
    assert captured["username"] == "bravotv"
    assert captured["kwargs"]["allow_browser_fallback"] is False
    assert captured["kwargs"]["allow_recovery"] is False


def test_interactive_instagram_login_launches_real_profile_directory(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    chrome_root = tmp_path / "Chrome"
    profile_dir = chrome_root / "Profile 13"
    profile_dir.mkdir(parents=True)
    (profile_dir / "Preferences").write_text(
        json.dumps({"profile": {"name": "codex"}, "account_info": [{"email": "codex@thereality.report"}]}),
        encoding="utf-8",
    )
    monkeypatch.setenv("SOCIAL_BROWSER_SESSION_DIR", str(tmp_path / "sessions"))
    monkeypatch.setattr(browser_cookie_refresh, "_chrome_profile_base_dir", lambda: chrome_root)
    captured: dict[str, object] = {}
    writes: list[tuple[Path, dict[str, str]]] = []

    class _FakePlaywrightTimeoutError(Exception):
        pass

    class _Locator:
        def inner_text(self, **_kwargs: object) -> str:
            return ""

    class _Page:
        url = "https://www.instagram.com/"

        def goto(self, url: str, **_kwargs: object) -> None:
            self.url = url

        def wait_for_timeout(self, *_args: object, **_kwargs: object) -> None:
            return None

        def locator(self, *_args: object, **_kwargs: object) -> _Locator:
            return _Locator()

    class _Context:
        def new_page(self) -> _Page:
            return _Page()

        def cookies(self) -> list[dict[str, object]]:
            return [
                {"name": "sessionid", "value": "fresh-session", "domain": ".instagram.com"},
                {"name": "csrftoken", "value": "fresh-csrf", "domain": ".instagram.com"},
                {"name": "ds_user_id", "value": "123", "domain": ".instagram.com"},
            ]

        def storage_state(self) -> dict[str, object]:
            return {"cookies": self.cookies(), "origins": []}

        def close(self) -> None:
            captured["closed"] = True

    class _FakeChromium:
        def launch_persistent_context(self, **kwargs: object) -> _Context:
            captured.update(kwargs)
            return _Context()

    class _PlaywrightContext:
        def __enter__(self) -> SimpleNamespace:
            return SimpleNamespace(chromium=_FakeChromium())

        def __exit__(self, *_args: object) -> bool:
            return False

    sync_api_module = ModuleType("playwright.sync_api")
    sync_api_module.TimeoutError = _FakePlaywrightTimeoutError
    sync_api_module.sync_playwright = lambda: _PlaywrightContext()
    playwright_module = ModuleType("playwright")
    playwright_module.sync_api = sync_api_module
    monkeypatch.setitem(sys.modules, "playwright", playwright_module)
    monkeypatch.setitem(sys.modules, "playwright.sync_api", sync_api_module)
    monkeypatch.setattr(
        instagram_cookie_refresh,
        "_write_cookie_file",
        lambda path, cookies: writes.append((Path(path), dict(cookies))),
    )

    cookies = instagram_cookie_refresh.interactive_chrome_login(
        chrome_profile_name="codex@thereality.report",
        cookie_file=tmp_path / "instagram-cookies.json",
        validation_username="bravotv",
        timeout_seconds=120,
        headless=False,
        validation_mode="schema_only",
    )

    assert captured["user_data_dir"] == str(chrome_root)
    assert "--profile-directory=Profile 13" in captured["args"]
    assert captured["headless"] is False
    assert captured["closed"] is True
    assert cookies["sessionid"] == "fresh-session"
    assert writes == [(tmp_path / "instagram-cookies.json", cookies)]


def test_refresh_twitter_cookies_retries_headed_after_headless_error_shell(
    monkeypatch,
    tmp_path: Path,
) -> None:
    attempts: list[bool] = []
    monkeypatch.setenv("SOCIAL_TWITTER_COOKIE_REFRESH_ALLOW_HEADED_FALLBACK", "true")

    def _fake_once(*, headless: bool, **_: object) -> dict[str, str]:
        attempts.append(headless)
        if headless:
            raise RuntimeError("Twitter login page returned an error shell before credentials were entered")
        return {"auth_token": "fresh-auth", "ct0": "fresh-ct0"}

    monkeypatch.setattr(twitter_cookie_refresh, "_refresh_twitter_cookies_once", _fake_once)

    cookies = twitter_cookie_refresh.refresh_twitter_cookies(
        username="codex@thereality.report",
        password="secret",
        cookie_file=tmp_path / "twitter-cookies.json",
        headless=True,
        timeout_seconds=45,
    )

    assert attempts == [True, False]
    assert cookies["auth_token"] == "fresh-auth"
    assert cookies["ct0"] == "fresh-ct0"


def test_refresh_twitter_cookies_does_not_retry_headed_by_default(
    monkeypatch,
    tmp_path: Path,
) -> None:
    attempts: list[bool] = []
    monkeypatch.delenv("SOCIAL_TWITTER_COOKIE_REFRESH_ALLOW_HEADED_FALLBACK", raising=False)

    def _fake_once(*, headless: bool, **_: object) -> dict[str, str]:
        attempts.append(headless)
        raise RuntimeError("Twitter login page returned an error shell before credentials were entered")

    monkeypatch.setattr(twitter_cookie_refresh, "_refresh_twitter_cookies_once", _fake_once)

    with pytest.raises(RuntimeError, match="error shell"):
        twitter_cookie_refresh.refresh_twitter_cookies(
            username="codex@thereality.report",
            password="secret",
            cookie_file=tmp_path / "twitter-cookies.json",
            headless=True,
            timeout_seconds=45,
        )

    assert attempts == [True]


def test_threads_validate_session_tokens_requires_graphql_tokens(monkeypatch) -> None:
    from trr_backend.socials.threads.scraper import ThreadsScraper

    scraper = ThreadsScraper(cookies={"sessionid": "s", "csrftoken": "c"})

    token_html = '{"DTSGInitialData":{"token":"dtsg-1"},"LSD":{"token":"lsd-1"},"jazoest":"26474"}'
    monkeypatch.setattr(scraper, "_fetch_html", lambda *a, **k: token_html)
    assert scraper.validate_session_tokens() == (True, None)

    monkeypatch.setattr(scraper, "_fetch_html", lambda *a, **k: "<html>Log in with your Instagram account</html>")
    assert scraper.validate_session_tokens() == (False, "login_prompt_detected")

    monkeypatch.setattr(scraper, "_fetch_html", lambda *a, **k: "<html>anonymous shell without tokens</html>")
    assert scraper.validate_session_tokens() == (False, "graphql_tokens_missing")

    def _boom(*a: object, **k: object) -> str:
        raise TimeoutError("slow")

    monkeypatch.setattr(scraper, "_fetch_html", _boom)
    valid, reason = scraper.validate_session_tokens()
    assert valid is False
    assert reason.startswith("probe_fetch_failed:")


def test_facebook_in_protocol_validator(monkeypatch) -> None:
    from trr_backend.socials.facebook import cookie_refresh as facebook_cookie_refresh

    validate = facebook_cookie_refresh._validate_facebook_cookies_in_protocol

    assert validate({"c_user": "1"}) == (False, "missing_required_cookies")

    class _Resp:
        def __init__(self, url: str, text: str) -> None:
            self.url = url
            self.text = text

    monkeypatch.setattr(
        facebook_cookie_refresh.requests,
        "get",
        lambda *a, **k: _Resp("https://www.facebook.com/login/?next=me", "Log into Facebook"),
    )
    valid, reason = validate({"c_user": "1", "xs": "2"})
    assert valid is False
    assert reason.startswith("login_redirect:")

    monkeypatch.setattr(
        facebook_cookie_refresh.requests,
        "get",
        lambda *a, **k: _Resp("https://www.facebook.com/bravo", "<html>profile timeline</html>"),
    )
    assert validate({"c_user": "1", "xs": "2"}) == (True, None)


def test_threads_refresh_passes_in_protocol_validator(monkeypatch, tmp_path) -> None:
    from trr_backend.socials.threads import cookie_refresh as threads_cookie_refresh_mod

    captured: dict[str, object] = {}

    def _fake_refresh(*, spec: object, validator: object = None, **_: object) -> dict[str, str]:
        captured["validator"] = validator
        return {"sessionid": "s", "csrftoken": "c"}

    monkeypatch.setattr(threads_cookie_refresh_mod, "refresh_simple_login_cookies", _fake_refresh)
    threads_cookie_refresh_mod.refresh_threads_cookies(
        username="u", password="p", cookie_file=str(tmp_path / "t.json")
    )
    assert captured["validator"] is threads_cookie_refresh_mod._validate_threads_cookies_in_protocol
