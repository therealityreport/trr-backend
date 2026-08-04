from __future__ import annotations

import pytest

from trr_backend.repositories import social_season_analytics as repo
from trr_backend.socials.pipelines import tiktok_cookie_loader


def test_tiktok_cookie_loader_requires_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(tiktok_cookie_loader, "_tiktok_cookie_loader", None)

    with pytest.raises(RuntimeError, match="TikTok cookie loader is not configured"):
        tiktok_cookie_loader.load_tiktok_cookies()


def test_tiktok_cookie_loader_returns_configured_loader_result_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = {"sessionid": "cookie", "tt_csrf_token": "csrf"}
    calls = 0

    def fake_loader() -> dict[str, str]:
        nonlocal calls
        calls += 1
        return expected

    monkeypatch.setattr(tiktok_cookie_loader, "_tiktok_cookie_loader", None)
    tiktok_cookie_loader.configure_tiktok_cookie_loader(fake_loader)

    result = tiktok_cookie_loader.load_tiktok_cookies()

    assert result is expected
    assert calls == 1


def test_tiktok_cookie_loader_observes_compatibility_repo_monkeypatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = {"sessionid": "patched-cookie"}
    monkeypatch.setattr(repo, "_load_tiktok_cookies", lambda: expected)

    result = tiktok_cookie_loader.load_tiktok_cookies()

    assert result is expected
