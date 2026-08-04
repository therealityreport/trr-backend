from __future__ import annotations

import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from trr_backend.repositories import social_season_analytics as repo
from trr_backend.socials.pipelines import facebook_cookie_loader

_PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _run_fresh_interpreter(source: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-c", textwrap.dedent(source)],
        cwd=_PROJECT_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )


def test_facebook_cookie_loader_is_import_neutral_and_unconfigured_in_fresh_interpreter() -> None:
    result = _run_fresh_interpreter(
        """
        import sys

        from trr_backend.socials.pipelines import facebook_cookie_loader

        assert "trr_backend.socials.social_season_analytics_impl" not in sys.modules
        assert facebook_cookie_loader._facebook_cookie_loader is None
        try:
            facebook_cookie_loader.load_facebook_cookies()
        except RuntimeError as exc:
            assert str(exc) == "Facebook cookie loader is not configured"
        else:
            raise AssertionError("unconfigured Facebook cookie loader did not fail")
        """
    )

    assert result.returncode == 0, result.stderr or result.stdout


@pytest.mark.parametrize(
    "composition_import",
    [
        """
        from trr_backend.repositories import social_season_analytics as repo

        assert facebook_cookie_loader._facebook_cookie_loader is not None
        """,
        """
        from unittest.mock import MagicMock
        from trr_backend.socials.facebook.document_fetch import FacebookDocumentFetcher
        from trr_backend.repositories import social_season_analytics as repo

        assert facebook_cookie_loader._facebook_cookie_loader is not None
        session = MagicMock()
        session.get.return_value = MagicMock(status_code=200, text="<html></html>", raise_for_status=lambda: None)
        fetcher = FacebookDocumentFetcher(session=session)
        assert fetcher.runtime_metadata["request_count"] == 0
        """,
    ],
    ids=["legacy-repository", "document-fetch-before-repository"],
)
def test_production_composition_imports_register_loader_in_fresh_interpreter(
    composition_import: str,
) -> None:
    result = _run_fresh_interpreter(
        "from trr_backend.socials.pipelines import facebook_cookie_loader\n" + textwrap.dedent(composition_import)
    )

    assert result.returncode == 0, result.stderr or result.stdout


def test_facebook_cookie_loader_returns_configured_loader_result_unchanged(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = {"c_user": "1", "xs": "token"}
    calls = 0

    def fake_loader() -> dict[str, str]:
        nonlocal calls
        calls += 1
        return expected

    monkeypatch.setattr(facebook_cookie_loader, "_facebook_cookie_loader", None)
    facebook_cookie_loader.configure_facebook_cookie_loader(fake_loader)

    result = facebook_cookie_loader.load_facebook_cookies()

    assert result is expected
    assert calls == 1


def test_facebook_cookie_loader_observes_compatibility_repo_monkeypatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = {"c_user": "patched-user", "xs": "patched-token"}
    monkeypatch.setattr(repo, "_load_facebook_cookies", lambda: expected)

    result = facebook_cookie_loader.load_facebook_cookies()

    assert result is expected
