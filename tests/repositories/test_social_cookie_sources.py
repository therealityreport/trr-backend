from __future__ import annotations

from pathlib import Path

import pytest

import trr_backend.repositories.social_season_analytics as legacy_repo
import trr_backend.socials.cookie_sources as cookie_sources
import trr_backend.socials.social_season_analytics_impl as social_impl
import trr_backend.socials.socialblade.auth as socialblade_auth


def test_cookie_source_helpers_preserve_compatibility_identity() -> None:
    for name in (
        "_default_platform_cookie_file_path",
        "_platform_cookie_file_candidates",
        "_platform_cookie_refresh_target_path",
        "_select_preferred_cookie_candidate",
    ):
        canonical = getattr(cookie_sources, name)
        assert getattr(social_impl, name) is canonical
        assert getattr(legacy_repo, name) is canonical
        assert getattr(socialblade_auth, name) is canonical


def test_platform_cookie_paths_preserve_env_order_and_default(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    default_path = tmp_path / "default-cookies.json"
    monkeypatch.setenv("COOKIE_PATH_PRIMARY", " ~/primary-cookies.json ")
    monkeypatch.setenv("COOKIE_PATH_EMPTY", "  ")

    candidates = cookie_sources._platform_cookie_file_candidates(
        default_path,
        "COOKIE_PATH_PRIMARY",
        "COOKIE_PATH_EMPTY",
    )

    assert candidates == [Path("~/primary-cookies.json").expanduser(), default_path]
    assert (
        cookie_sources._platform_cookie_refresh_target_path(
            default_path,
            "COOKIE_PATH_PRIMARY",
            "COOKIE_PATH_EMPTY",
        )
        == candidates[0]
    )

    monkeypatch.delenv("COOKIE_PATH_PRIMARY")
    assert cookie_sources._platform_cookie_refresh_target_path(default_path, "COOKIE_PATH_PRIMARY") == default_path


def test_default_platform_cookie_path_remains_under_backend_scripts() -> None:
    expected = Path(__file__).resolve().parents[2] / "scripts" / "socials" / "socialblade" / "socialblade_cookies.json"

    assert cookie_sources._default_platform_cookie_file_path("socialblade") == expected


def test_preferred_cookie_candidate_preserves_completeness_scoring_and_copies() -> None:
    any_only = {"cf_clearance": "clearance"}
    all_only = {"session": "session-token"}
    complete = {"cf_clearance": "complete-clearance", "session": "complete-session"}

    selected = cookie_sources._select_preferred_cookie_candidate(
        [any_only, all_only, complete],
        required_cookie_names_any=("cf_clearance",),
        required_cookie_names_all=("session",),
    )

    assert selected == complete
    assert selected is not complete
    assert (
        cookie_sources._select_preferred_cookie_candidate(
            [any_only, all_only],
            required_cookie_names_any=("cf_clearance",),
            required_cookie_names_all=("session",),
        )
        == all_only
    )
    assert cookie_sources._select_preferred_cookie_candidate([any_only]) == any_only
    assert cookie_sources._select_preferred_cookie_candidate([]) == {}
