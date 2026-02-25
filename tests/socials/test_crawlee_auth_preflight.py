from __future__ import annotations

import pytest

from trr_backend.socials.crawlee_runtime.auth_preflight import AuthPreflightError, check_platform_auth
from trr_backend.socials.crawlee_runtime.config import (
    build_runtime_config,
    is_auth_strict_for_platform,
    should_use_crawlee,
)


def test_instagram_preflight_requires_cookies() -> None:
    result = check_platform_auth(
        platform="instagram",
        instagram_cookies={},
        tiktok_cookies=None,
        twitter_cookies=None,
        twitter_bearer=None,
        twikit_credentials=None,
    )
    assert result.ok is False
    assert result.reason == "instagram_cookies_missing"
    assert result.auth_mode == "cookies"


def test_twitter_preflight_accepts_bearer() -> None:
    result = check_platform_auth(
        platform="twitter",
        instagram_cookies=None,
        tiktok_cookies=None,
        twitter_cookies=None,
        twitter_bearer="token-1",
        twikit_credentials=None,
    )
    assert result.ok is True
    assert result.auth_mode == "bearer"


def test_youtube_preflight_public_mode() -> None:
    result = check_platform_auth(
        platform="youtube",
        instagram_cookies=None,
        tiktok_cookies=None,
        twitter_cookies=None,
        twitter_bearer=None,
        twikit_credentials=None,
    )
    assert result.ok is True
    assert result.auth_mode == "public"
    assert result.auth_source == "none"


def test_auth_preflight_error_exposes_non_secret_metadata() -> None:
    result = check_platform_auth(
        platform="tiktok",
        instagram_cookies=None,
        tiktok_cookies={},
        twitter_cookies=None,
        twitter_bearer=None,
        twikit_credentials=None,
    )
    assert result.ok is False
    with pytest.raises(AuthPreflightError) as exc_info:
        raise AuthPreflightError(result)
    exc = exc_info.value
    assert exc.error_code == "auth"
    assert exc.retryable is False
    auth_context = exc.runtime_metadata["auth_context"]
    assert auth_context["auth_preflight_ok"] is False
    assert "missing_hints" in auth_context


def test_should_use_crawlee_respects_flags(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOCIAL_CRAWLEE_ENABLED", "true")
    monkeypatch.setenv("SOCIAL_CRAWLEE_PLATFORMS", "instagram,tiktok")
    monkeypatch.delenv("SOCIAL_CRAWLEE_FORCE_LEGACY_PLATFORMS", raising=False)
    assert should_use_crawlee("instagram") is True
    assert should_use_crawlee("twitter") is False


def test_build_runtime_config_applies_force_legacy(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOCIAL_CRAWLEE_ENABLED", "true")
    monkeypatch.setenv("SOCIAL_CRAWLEE_PLATFORMS", "instagram,tiktok,twitter,youtube")
    monkeypatch.setenv("SOCIAL_CRAWLEE_FORCE_LEGACY_PLATFORMS", "twitter")
    config = build_runtime_config("twitter")
    assert config.enabled is False
    assert "twitter" in config.force_legacy_platforms


def test_instagram_auth_strict_flag_defaults_false(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SOCIAL_CRAWLEE_AUTH_STRICT_INSTAGRAM", raising=False)
    assert is_auth_strict_for_platform("instagram") is False


def test_instagram_auth_strict_flag_can_enable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOCIAL_CRAWLEE_AUTH_STRICT_INSTAGRAM", "true")
    assert is_auth_strict_for_platform("instagram") is True
