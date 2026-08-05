from __future__ import annotations

import hashlib
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

import trr_backend.socials.instagram.posts_control as posts_control
import trr_backend.socials.social_season_analytics_impl as legacy_core

POSTS_CONTROL_ROOM_NAMES = (
    "_social_account_posts_scrapling_start_lock_key",
    "get_active_social_account_posts_scrapling_run",
    "start_instagram_posts_scrapling_scrape",
)


def test_posts_control_import_does_not_load_legacy_social_modules() -> None:
    backend_root = Path(__file__).resolve().parents[2]
    code = "\n".join(
        [
            "import importlib",
            "import sys",
            "before = set(sys.modules)",
            "module = importlib.import_module('trr_backend.socials.instagram.posts_control')",
            "assert callable(module.start_instagram_posts_scrapling_scrape)",
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


def test_late_legacy_import_configures_preloaded_posts_control_leaf() -> None:
    backend_root = Path(__file__).resolve().parents[2]
    code = "\n".join(
        [
            "import importlib",
            "import sys",
            "leaf = importlib.import_module('trr_backend.socials.instagram.posts_control')",
            "legacy_name = 'trr_backend.socials.social_season_analytics_impl'",
            "assert legacy_name not in sys.modules",
            "legacy = importlib.import_module(legacy_name)",
            "assert leaf._LEGACY_NAMESPACE is legacy.__dict__",
            "assert leaf._legacy_value('SocialIngestConflictError') is legacy.SocialIngestConflictError",
            "assert isinstance(leaf._social_account_posts_scrapling_start_lock_key('instagram', 'BravoTV'), int)",
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


@pytest.mark.parametrize("function_name", POSTS_CONTROL_ROOM_NAMES)
def test_all_legacy_posts_control_wrappers_delegate_to_leaf_room(
    function_name: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    def replacement(*args: Any, **kwargs: Any) -> str:
        calls.append((args, kwargs))
        return function_name

    monkeypatch.setitem(posts_control._LOCAL_ROOM_FUNCTIONS, function_name, replacement)

    assert getattr(legacy_core, function_name)("arg", marker="value") == function_name
    assert calls == [(("arg",), {"marker": "value"})]


def test_lock_key_honors_live_legacy_normalizer_overrides(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        legacy_core,
        "_normalize_social_account_profile_platform",
        lambda _value: "patched-platform",
    )
    monkeypatch.setattr(
        legacy_core,
        "_normalize_social_account_profile_handle",
        lambda _value: "patched-account",
    )
    expected = int(
        hashlib.md5(
            b"posts-scrapling-start:patched-platform:patched-account"
        ).hexdigest()[:15],
        16,
    ) % (2**31)

    assert (
        posts_control._social_account_posts_scrapling_start_lock_key(
            "instagram",
            "BravoTV",
        )
        == expected
    )


def test_active_run_lookup_honors_live_pg_and_stage_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, list[str]]] = []

    class FakePg:
        @staticmethod
        def fetch_one(query: str, params: list[str]) -> dict[str, str]:
            calls.append((query, params))
            return {"run_id": "run-1", "status": "running"}

    monkeypatch.setattr(legacy_core, "pg", FakePg)
    monkeypatch.setattr(
        legacy_core,
        "_normalize_social_account_profile_platform",
        lambda _value: "instagram",
    )
    monkeypatch.setattr(
        legacy_core,
        "_normalize_social_account_profile_handle",
        lambda _value: "bravotv",
    )
    monkeypatch.setattr(
        legacy_core,
        "INSTAGRAM_POSTS_SCRAPLING_STAGE",
        "patched-instagram-stage",
    )

    assert posts_control.get_active_social_account_posts_scrapling_run(
        "instagram",
        "BravoTV",
    ) == {"run_id": "run-1", "status": "running"}
    assert calls[0][1] == ["instagram", "bravotv", "patched-instagram-stage"]


def test_validation_error_keeps_legacy_exception_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        legacy_core,
        "_normalize_social_account_profile_handle",
        lambda _value: "",
    )

    with pytest.raises(legacy_core.SocialIngestValidationError):
        posts_control.start_instagram_posts_scrapling_scrape(account_handle="BravoTV")
