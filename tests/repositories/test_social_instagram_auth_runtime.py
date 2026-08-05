from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

import trr_backend.socials.control_plane as control_plane
import trr_backend.socials.instagram.auth_runtime as auth_runtime
import trr_backend.socials.social_season_analytics_impl as legacy_core

AUTH_ROOM_NAMES = (
    "_default_instagram_cookie_file_path",
    "_instagram_cookie_file_candidates",
    "_instagram_cookie_refresh_target_path",
    "_instagram_auth_credentials",
    "_instagram_cookie_auto_refresh_enabled",
    "_instagram_cookie_validation_username",
    "_load_instagram_cookies_from_sources",
    "_instagram_cookie_fingerprint",
    "_instagram_cookie_structure_detail",
    "_instagram_cookie_schema_result",
    "_instagram_cookie_validation_detail",
    "_inspect_instagram_cookie_health",
    "_validate_instagram_cookie_health",
    "_refresh_instagram_cookies",
    "_ensure_instagram_cookies_fresh",
    "_load_instagram_cookies_legacy",
    "_build_legacy_instagram_auth_session",
    "_load_instagram_cookies",
    "get_instagram_auth_repair_signal",
)


def test_auth_runtime_import_does_not_load_legacy_social_modules() -> None:
    backend_root = Path(__file__).resolve().parents[2]
    code = "\n".join(
        [
            "import importlib",
            "import sys",
            "before = set(sys.modules)",
            "module = importlib.import_module('trr_backend.socials.instagram.auth_runtime')",
            "assert callable(module._load_instagram_cookies)",
            "loaded = set(sys.modules) - before",
            "forbidden = {",
            "    'trr_backend.socials.social_season_analytics_impl',",
            "    'trr_backend.repositories.social_season_analytics',",
            "}",
            "assert not (loaded & forbidden), sorted(loaded & forbidden)",
        ]
    )

    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=backend_root,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr


def test_late_legacy_import_preserves_canonical_leaf_state() -> None:
    backend_root = Path(__file__).resolve().parents[2]
    code = "\n".join(
        [
            "import importlib",
            "import sys",
            "leaf = importlib.import_module('trr_backend.socials.instagram.auth_runtime')",
            "cache = (1.0, 'fingerprint', {'valid': True})",
            "override = {'sessionid': 'leaf-live'}",
            "leaf._write_state('_instagram_cookie_validation_cache', cache)",
            "leaf._write_state('_instagram_cookie_runtime_override', override)",
            "legacy_name = 'trr_backend.socials.social_season_analytics_impl'",
            "assert legacy_name not in sys.modules",
            "legacy = importlib.import_module(legacy_name)",
            "assert leaf._read_state('_instagram_cookie_validation_cache') is cache",
            "assert leaf._read_state('_instagram_cookie_runtime_override') is override",
            "assert legacy._instagram_cookie_validation_cache is cache",
            "assert legacy._instagram_cookie_runtime_override is override",
            "assert legacy._instagram_cookie_refresh_lock is leaf._instagram_cookie_refresh_lock",
        ]
    )

    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=backend_root,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("function_name", AUTH_ROOM_NAMES)
def test_all_legacy_instagram_auth_wrappers_delegate_to_leaf_room(
    function_name: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    def replacement(*args: Any, **kwargs: Any) -> str:
        calls.append((args, kwargs))
        return function_name

    monkeypatch.setitem(auth_runtime._LOCAL_ROOM_FUNCTIONS, function_name, replacement)

    assert getattr(legacy_core, function_name)("arg", marker="value") == function_name
    assert calls == [(("arg",), {"marker": "value"})]


def test_leaf_internal_calls_honor_live_legacy_monolith_overrides(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        legacy_core,
        "_load_instagram_cookies_from_sources",
        lambda: {"sessionid": "legacy-source"},
    )
    monkeypatch.setattr(
        legacy_core,
        "_ensure_instagram_cookies_fresh",
        lambda cookies: {**cookies, "fresh": "yes"},
    )

    assert auth_runtime._load_instagram_cookies_legacy() == {
        "sessionid": "legacy-source",
        "fresh": "yes",
    }


def test_validation_honors_legacy_monolith_inspector_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        legacy_core,
        "_inspect_instagram_cookie_health",
        lambda _cookies: {"valid": False, "reason": "legacy-inspector"},
    )

    assert auth_runtime._validate_instagram_cookie_health({"sessionid": "value"}) == (
        False,
        "legacy-inspector",
    )


def test_refresh_path_honors_legacy_monolith_overrides(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class TrackingLock:
        enter_count = 0

        def __enter__(self) -> TrackingLock:
            self.enter_count += 1
            return self

        def __exit__(self, *_args: Any) -> None:
            return None

    stale = {"sessionid": "stale"}
    fresh = {"sessionid": "fresh"}
    tracking_lock = TrackingLock()
    monkeypatch.setattr(
        legacy_core,
        "_instagram_cookie_refresh_lock",
        tracking_lock,
    )
    monkeypatch.setattr(
        legacy_core,
        "_instagram_cookie_auto_refresh_enabled",
        lambda: True,
    )
    monkeypatch.setattr(
        legacy_core,
        "_validate_instagram_cookie_health",
        lambda cookies: (cookies.get("sessionid") == "fresh", "expired"),
    )
    monkeypatch.setattr(
        legacy_core,
        "_load_instagram_cookies_from_sources",
        lambda: dict(stale),
    )
    monkeypatch.setattr(
        legacy_core,
        "_refresh_instagram_cookies",
        lambda _reason=None: dict(fresh),
    )
    auth_runtime._write_state("_instagram_cookie_runtime_override", None)

    try:
        assert auth_runtime._ensure_instagram_cookies_fresh(stale) == fresh
        assert tracking_lock.enter_count == 1
    finally:
        auth_runtime._write_state("_instagram_cookie_runtime_override", None)


def test_resolver_path_honors_legacy_monolith_loader_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import trr_backend.socials.instagram as instagram_module

    monkeypatch.delenv("INSTAGRAM_AUTH_RESOLVER_V2", raising=False)
    monkeypatch.setattr(
        legacy_core,
        "_load_instagram_cookies_legacy",
        lambda: {"sessionid": "legacy-monolith"},
    )
    monkeypatch.setattr(
        instagram_module,
        "resolve_instagram_auth_session",
        lambda **_kwargs: None,
    )
    auth_runtime._write_state("_instagram_cookie_runtime_override", None)

    try:
        assert auth_runtime._load_instagram_cookies() == {
            "sessionid": "legacy-monolith",
        }
    finally:
        auth_runtime._write_state("_instagram_cookie_runtime_override", None)


def test_probe_mode_restores_environment_after_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    keys = (
        "SOCIAL_INSTAGRAM_COOKIE_AUTO_REFRESH",
        "SOCIAL_INSTAGRAM_GRAPHQL_RECOVERY_DISABLED",
        "SOCIAL_INSTAGRAM_INTERACTIVE_LOGIN",
    )
    monkeypatch.setenv(keys[0], "original")
    monkeypatch.delenv(keys[1], raising=False)
    monkeypatch.setenv(keys[2], "enabled")
    before = {key: os.environ.get(key) for key in keys}

    with pytest.raises(RuntimeError, match="probe failed"):
        with auth_runtime._instagram_cookie_validation_probe_mode():
            assert os.environ[keys[0]] == "false"
            assert os.environ[keys[1]] == "true"
            assert os.environ[keys[2]] == "false"
            raise RuntimeError("probe failed")

    assert {key: os.environ.get(key) for key in keys} == before


def test_cookie_validation_cache_honors_fingerprint_and_ttl(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import trr_backend.socials.instagram as instagram_module

    fetch_calls: list[str] = []
    timestamps = iter((100.0, 120.0, 130.0, 200.0))

    class FakeScraper:
        last_retrieval_meta: dict[str, Any] = {}

        def __init__(
            self,
            *,
            cookies: dict[str, str],
            browser_account_id: str,
        ) -> None:
            self.cookies = cookies
            self.browser_account_id = browser_account_id

        def fetch_posts_graphql(self, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
            fetch_calls.append(str(self.cookies["sessionid"]))
            return {
                "data": {
                    "xdt_api__v1__feed__user_timeline_graphql_connection": {
                        "edges": [{"node": {"id": "post"}}],
                    }
                }
            }

    monkeypatch.setattr(instagram_module, "InstagramScraper", FakeScraper)
    monkeypatch.setattr(auth_runtime.time_module, "monotonic", lambda: next(timestamps))
    monkeypatch.setenv("SOCIAL_INSTAGRAM_COOKIE_VALIDATION_TTL_SECONDS", "60")
    auth_runtime._write_state("_instagram_cookie_validation_cache", None)

    first = {"sessionid": "one", "csrftoken": "csrf", "ds_user_id": "1"}
    second = {"sessionid": "two", "csrftoken": "csrf", "ds_user_id": "1"}
    try:
        assert auth_runtime._inspect_instagram_cookie_health(first)["valid"] is True
        assert auth_runtime._inspect_instagram_cookie_health(first)["valid"] is True
        assert auth_runtime._inspect_instagram_cookie_health(second)["valid"] is True
        assert auth_runtime._inspect_instagram_cookie_health(second)["valid"] is True
        assert fetch_calls == ["one", "two", "two"]
    finally:
        auth_runtime._write_state("_instagram_cookie_validation_cache", None)


def test_cookie_validation_ttl_honors_legacy_resolver_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cookies = {
        "sessionid": "one",
        "csrftoken": "csrf",
        "ds_user_id": "1",
    }
    fingerprint = auth_runtime._instagram_cookie_fingerprint(cookies)
    calls: list[tuple[str, int, int]] = []

    def resolve_ttl(name: str, default: int, *, minimum: int = 1) -> int:
        calls.append((name, default, minimum))
        return 60

    monkeypatch.setattr(legacy_core, "_resolve_positive_int_env", resolve_ttl)
    monkeypatch.setattr(auth_runtime.time_module, "monotonic", lambda: 120.0)
    auth_runtime._write_state(
        "_instagram_cookie_validation_cache",
        (100.0, fingerprint, {"valid": True, "reason": None}),
    )

    try:
        assert auth_runtime._validate_instagram_cookie_health(cookies) == (True, None)
        assert calls == [
            (
                "SOCIAL_INSTAGRAM_COOKIE_VALIDATION_TTL_SECONDS",
                auth_runtime.SOCIAL_INSTAGRAM_COOKIE_VALIDATION_TTL_SECONDS_DEFAULT,
                30,
            )
        ]
    finally:
        auth_runtime._write_state("_instagram_cookie_validation_cache", None)


def test_refresh_owns_override_cache_and_lock_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import trr_backend.socials.instagram as instagram_module
    import trr_backend.socials.instagram.cookie_refresh as cookie_refresh

    refreshed = {
        "sessionid": "fresh",
        "csrftoken": "csrf",
        "ds_user_id": "1",
    }
    resolver_updates: list[dict[str, str]] = []
    monkeypatch.setenv("SOCIAL_AUTH_INSTAGRAM_USERNAME", "operator")
    monkeypatch.setenv("SOCIAL_AUTH_INSTAGRAM_PASSWORD", "secret")
    monkeypatch.setattr(
        cookie_refresh,
        "refresh_instagram_cookies",
        lambda **_kwargs: dict(refreshed),
    )
    monkeypatch.setattr(
        instagram_module,
        "set_instagram_runtime_override",
        lambda value: resolver_updates.append(dict(value or {})),
    )
    auth_runtime._write_state(
        "_instagram_cookie_validation_cache",
        (1.0, "fingerprint", {"valid": True}),
    )
    auth_runtime._write_state("_instagram_cookie_runtime_override", None)

    try:
        assert auth_runtime._refresh_instagram_cookies("expired") == refreshed
        assert auth_runtime._read_state("_instagram_cookie_runtime_override") == refreshed
        assert auth_runtime._read_state("_instagram_cookie_validation_cache") is None
        assert legacy_core._instagram_cookie_runtime_override == refreshed
        assert legacy_core._instagram_cookie_validation_cache is None
        assert legacy_core._instagram_cookie_refresh_lock is auth_runtime._instagram_cookie_refresh_lock
        assert resolver_updates == [refreshed]
    finally:
        auth_runtime._write_state("_instagram_cookie_validation_cache", None)
        auth_runtime._write_state("_instagram_cookie_runtime_override", None)


def test_runtime_override_short_circuits_validation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    override = {"sessionid": "override"}
    auth_runtime._write_state("_instagram_cookie_runtime_override", override)
    monkeypatch.setattr(
        auth_runtime,
        "_instagram_cookie_auto_refresh_enabled",
        lambda: pytest.fail("override must short-circuit validation"),
    )

    try:
        assert auth_runtime._ensure_instagram_cookies_fresh({"sessionid": "stale"}) == override
    finally:
        auth_runtime._write_state("_instagram_cookie_runtime_override", None)


def test_control_plane_keeps_canonical_instagram_cookie_loader_identity() -> None:
    assert control_plane._load_instagram_cookies is auth_runtime._load_instagram_cookies
