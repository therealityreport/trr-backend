"""Unit tests for the SocialBlade cookie-repair CLI control flow.

These lock in the Cloudflare-1020 handling contract: a blocked *validation* egress
must never silently discard a structurally-valid cookie set, and the Modal secret
write must only proceed either after a real validation or under the explicit
``--allow-blocked-validation`` escape hatch (and never for a non-1020 failure).
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pytest

from scripts.socials import repair_socialblade_auth as cli
from trr_backend.socials.socialblade.auth import SocialBladeValidationBlockedError

_BLOCK_REASON = "validation_scrape_failed:SocialBlade blocked by Cloudflare (1020 access denied)"
_GOOD_COOKIES = {"cf_clearance": "abc", "session": "xyz"}
# Captured before the autouse fixture stubs it, so the proxy-default tests exercise the real logic.
_REAL_ENSURE_PROXY_DEFAULTS = cli._ensure_repair_proxy_defaults


def _args(**overrides: object) -> argparse.Namespace:
    base = {
        "source_env": Path("/tmp/does-not-matter.env"),
        "chrome_profile": "codex@thereality.report",
        "validation_handle": "bravotv",
        "apply_modal": False,
        "allow_blocked_validation": False,
        "json": True,
    }
    base.update(overrides)
    return argparse.Namespace(**base)


@pytest.fixture(autouse=True)
def _isolate(monkeypatch: pytest.MonkeyPatch) -> None:
    # Never touch the real .env, real proxy env, disk, or Modal in these tests.
    monkeypatch.setattr(cli, "load_env", lambda: None)
    monkeypatch.setattr(cli, "_ensure_repair_proxy_defaults", lambda: {"provider": "decodo", "sticky": "true"})
    monkeypatch.setattr(cli, "socialblade_cookie_file_path", lambda: Path("/tmp/socialblade_cookies.json"))
    monkeypatch.setattr(cli, "write_cookie_file", lambda *a, **k: None)


def test_blocked_validation_without_flag_aborts_before_modal(monkeypatch: pytest.MonkeyPatch) -> None:
    applied: list[object] = []

    def _extract(**_kwargs: object) -> dict[str, str]:
        raise SocialBladeValidationBlockedError(_GOOD_COOKIES, _BLOCK_REASON)

    monkeypatch.setattr(cli, "_parse_args", lambda: _args(apply_modal=True))
    monkeypatch.setattr(cli, "extract_socialblade_cookies_from_chrome_profile", _extract)
    monkeypatch.setattr(cli, "socialblade_cookie_health_report", lambda **_k: {"healthy": False})
    monkeypatch.setattr(cli, "_apply_modal_secret", lambda src: applied.append(src) or {"applied": True})

    assert cli.main() == 2
    assert applied == []  # the Modal secret is never touched on a blocked validation


def test_blocked_validation_with_flag_pushes_unvalidated(monkeypatch: pytest.MonkeyPatch) -> None:
    applied: list[object] = []
    written: list[dict[str, str]] = []

    def _extract(**_kwargs: object) -> dict[str, str]:
        raise SocialBladeValidationBlockedError(_GOOD_COOKIES, _BLOCK_REASON)

    monkeypatch.setattr(cli, "_parse_args", lambda: _args(apply_modal=True, allow_blocked_validation=True))
    monkeypatch.setattr(cli, "extract_socialblade_cookies_from_chrome_profile", _extract)
    # After a bypass, the health report is called with validate=False → schema-only healthy.
    monkeypatch.setattr(cli, "socialblade_cookie_health_report", lambda **_k: {"healthy": True})
    monkeypatch.setattr(cli, "write_cookie_file", lambda _path, cookies: written.append(dict(cookies)))
    monkeypatch.setattr(cli, "_apply_modal_secret", lambda src: applied.append(src) or {"applied": True})

    assert cli.main() == 0
    assert written == [_GOOD_COOKIES]  # the freshly-extracted cookies were persisted
    assert len(applied) == 1  # and pushed to Modal


def test_non_1020_failure_never_bypasses(monkeypatch: pytest.MonkeyPatch) -> None:
    applied: list[object] = []

    def _extract(**_kwargs: object) -> dict[str, str]:
        # A genuine cookie/auth failure surfaces as a plain RuntimeError, not the typed
        # block error — the bypass flag must not rescue it.
        raise RuntimeError("Chrome profile SocialBlade cookies failed validation (validation_missing_followers)")

    monkeypatch.setattr(cli, "_parse_args", lambda: _args(apply_modal=True, allow_blocked_validation=True))
    monkeypatch.setattr(cli, "extract_socialblade_cookies_from_chrome_profile", _extract)
    monkeypatch.setattr(cli, "socialblade_cookie_health_report", lambda **_k: {"healthy": False})
    monkeypatch.setattr(cli, "_apply_modal_secret", lambda src: applied.append(src) or {"applied": True})

    assert cli.main() == 2
    assert applied == []


def test_happy_path_applies_validated(monkeypatch: pytest.MonkeyPatch) -> None:
    applied: list[object] = []

    monkeypatch.setattr(cli, "_parse_args", lambda: _args(apply_modal=True))
    monkeypatch.setattr(cli, "extract_socialblade_cookies_from_chrome_profile", lambda **_k: dict(_GOOD_COOKIES))
    monkeypatch.setattr(cli, "socialblade_cookie_health_report", lambda **_k: {"healthy": True})
    monkeypatch.setattr(cli, "_apply_modal_secret", lambda src: applied.append(src) or {"applied": True})

    assert cli.main() == 0
    assert len(applied) == 1


def test_ensure_repair_proxy_defaults_respects_explicit_empty(monkeypatch: pytest.MonkeyPatch) -> None:
    # Explicit empty provider (the visible-browser test path) is preserved, not overwritten.
    monkeypatch.setenv("SOCIALBLADE_PROXY_PROVIDER", "")
    monkeypatch.delenv("SOCIALBLADE_USE_STICKY_PROXY", raising=False)
    result = _REAL_ENSURE_PROXY_DEFAULTS()
    assert result["provider"] == ""
    assert result["sticky"] == "false"  # unset -> runtime default


def test_ensure_repair_proxy_defaults_sets_decodo_when_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SOCIALBLADE_PROXY_PROVIDER", raising=False)
    monkeypatch.delenv("SOCIALBLADE_USE_STICKY_PROXY", raising=False)
    result = _REAL_ENSURE_PROXY_DEFAULTS()
    assert result["provider"] == "decodo"
    assert result["sticky"] == "false"
