from __future__ import annotations

import pytest

from trr_backend.repositories import social_season_analytics as repo
from trr_backend.socials.pipelines import threads_cookie_loader


def test_threads_cookie_loader_requires_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(threads_cookie_loader, "_threads_cookie_loader", None)

    with pytest.raises(RuntimeError, match="Threads cookie loader is not configured"):
        threads_cookie_loader.load_threads_cookies()


def test_threads_cookie_loader_returns_configured_loader_result_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = {"sessionid": "cookie", "csrftoken": "csrf"}
    calls = 0

    def fake_loader() -> dict[str, str]:
        nonlocal calls
        calls += 1
        return expected

    monkeypatch.setattr(threads_cookie_loader, "_threads_cookie_loader", None)
    threads_cookie_loader.configure_threads_cookie_loader(fake_loader)

    result = threads_cookie_loader.load_threads_cookies()

    assert result is expected
    assert calls == 1


def test_threads_cookie_loader_observes_compatibility_repo_monkeypatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = {"sessionid": "patched-cookie", "csrftoken": "patched-csrf"}
    monkeypatch.setattr(repo, "_load_threads_cookies", lambda: expected)

    result = threads_cookie_loader.load_threads_cookies()

    assert result is expected
