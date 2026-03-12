from __future__ import annotations

from pathlib import Path

from trr_backend.socials.facebook import cookie_refresh as facebook_cookie_refresh
from trr_backend.socials.threads import cookie_refresh as threads_cookie_refresh
from trr_backend.socials.tiktok import cookie_refresh as tiktok_cookie_refresh
from trr_backend.socials.twitter import cookie_refresh as twitter_cookie_refresh


def test_tiktok_cookie_refresh_requires_authenticated_session_cookies() -> None:
    assert "ttwid" not in tiktok_cookie_refresh._SPEC.required_cookie_names_any
    assert any(
        "Maximum number of attempts reached" in pattern for pattern in tiktok_cookie_refresh._SPEC.invalid_body_patterns
    )


def test_facebook_cookie_refresh_detects_verification_checkpoint() -> None:
    assert "/two_step_verification" in facebook_cookie_refresh._SPEC.invalid_url_markers
    assert any("login code" in pattern.lower() for pattern in facebook_cookie_refresh._SPEC.invalid_body_patterns)


def test_threads_cookie_refresh_prefers_instagram_entrypoint() -> None:
    assert any("Instagram" in pattern for pattern in threads_cookie_refresh._SPEC.pre_login_button_patterns)
    assert 'input[name="email"]' in threads_cookie_refresh._SPEC.username_selectors
    assert 'input[name="pass"]' in threads_cookie_refresh._SPEC.password_selectors


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


def test_refresh_twitter_cookies_retries_headed_after_headless_error_shell(
    monkeypatch,
    tmp_path: Path,
) -> None:
    attempts: list[bool] = []

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
