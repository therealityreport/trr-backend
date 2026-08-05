from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

import trr_backend.repositories.social_season_analytics as legacy_repo
import trr_backend.socials.control_plane as control_plane
import trr_backend.socials.control_plane.runtime as runtime_surface
import trr_backend.socials.social_season_analytics_impl as canonical_social_analytics


def _build_stamp(
    environment: dict[str, str],
    *,
    modal_environment: str | None = None,
    modal_function: str | None = None,
    execution_backend: str = "local",
) -> dict[str, Any]:
    from trr_backend.runtime_version import build_runtime_version_stamp

    return build_runtime_version_stamp(
        getenv=environment.get,
        modal_environment=modal_environment,
        modal_function=modal_function,
        execution_backend=execution_backend,
    )


@pytest.mark.parametrize(
    ("environment", "expected_commit"),
    (
        (
            {
                "TRR_RUNTIME_VERSION": "aaaaaaaaaaaa1111",
                "TRR_DEPLOY_VERSION": "bbbbbbbbbbbb2222",
            },
            "aaaaaaaaaaaa1111",
        ),
        (
            {
                "TRR_RUNTIME_VERSION": "   ",
                "TRR_DEPLOY_VERSION": "1234567890abcdef",
                "RENDER_GIT_COMMIT": "render-commit",
            },
            "1234567890abcdef",
        ),
    ),
    ids=("precedence", "blank-skipping"),
)
def test_build_runtime_version_stamp_uses_commit_precedence_and_twelve_character_label(
    environment: dict[str, str],
    expected_commit: str,
) -> None:
    assert _build_stamp(environment) == {
        "commit_sha": expected_commit,
        "modal_image": None,
        "modal_environment": None,
        "modal_function": None,
        "execution_backend": "local",
        "label": expected_commit[:12],
    }


@pytest.mark.parametrize(
    ("environment", "expected_image", "expected_label"),
    (
        ({"MODAL_IMAGE_ID": "im-id", "MODAL_IMAGE_TAG": "tag-fallback"}, "im-id", "im-id"),
        ({"MODAL_IMAGE_ID": "   ", "MODAL_IMAGE_TAG": "tag-suppressed"}, None, "local"),
        ({"MODAL_IMAGE_TAG": "tag-only"}, "tag-only", "tag-only"),
    ),
    ids=("image-id-precedence", "whitespace-id-suppresses-tag", "tag-fallback"),
)
def test_build_runtime_version_stamp_preserves_modal_image_selection(
    environment: dict[str, str],
    expected_image: str | None,
    expected_label: str,
) -> None:
    assert _build_stamp(environment) == {
        "commit_sha": None,
        "modal_image": expected_image,
        "modal_environment": None,
        "modal_function": None,
        "execution_backend": "local",
        "label": expected_label,
    }


def test_build_runtime_version_stamp_preserves_modal_identity_and_label_order() -> None:
    assert _build_stamp(
        {
            "TRR_RUNTIME_VERSION": "1234567890abcdef",
            "MODAL_IMAGE_ID": "im-123",
        },
        modal_environment="main",
        modal_function="run_social_job",
        execution_backend="modal",
    ) == {
        "commit_sha": "1234567890abcdef",
        "modal_image": "im-123",
        "modal_environment": "main",
        "modal_function": "run_social_job",
        "execution_backend": "modal",
        "label": "1234567890ab · modal:main · im-123",
    }


def test_build_runtime_version_stamp_falls_back_to_execution_backend_label() -> None:
    assert _build_stamp({}, execution_backend="local") == {
        "commit_sha": None,
        "modal_image": None,
        "modal_environment": None,
        "modal_function": None,
        "execution_backend": "local",
        "label": "local",
    }


def test_runtime_version_leaf_import_is_project_neutral() -> None:
    backend_root = Path(__file__).resolve().parents[2]
    code = "\n".join(
        [
            "import importlib",
            "import sys",
            "before = set(sys.modules)",
            "module = importlib.import_module('trr_backend.runtime_version')",
            "assert callable(module.build_runtime_version_stamp)",
            "loaded = {name for name in set(sys.modules) - before if name.startswith('trr_backend')}",
            "assert loaded <= {'trr_backend', 'trr_backend.runtime_version'}, loaded",
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


def test_monolith_runtime_version_wrapper_delegates_and_preserves_cache_clear(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, Any]] = []

    def fake_getenv(_name: str) -> str | None:
        return None

    def fake_builder(**kwargs: Any) -> dict[str, Any]:
        calls.append(dict(kwargs))
        return {"label": f"stamp-{len(calls)}"}

    monkeypatch.setattr(canonical_social_analytics, "build_runtime_version_stamp", fake_builder)
    monkeypatch.setattr(canonical_social_analytics.os, "getenv", fake_getenv)
    monkeypatch.setattr(canonical_social_analytics, "modal_environment_name", lambda: "main")
    monkeypatch.setattr(canonical_social_analytics, "modal_social_job_function_name", lambda: "run_social_job")
    monkeypatch.setattr(canonical_social_analytics, "execution_backend_canonical", lambda: "modal")
    canonical_social_analytics._resolve_runtime_version_stamp.cache_clear()

    try:
        first = canonical_social_analytics._resolve_runtime_version_stamp()
        second = canonical_social_analytics._resolve_runtime_version_stamp()

        assert callable(canonical_social_analytics._resolve_runtime_version_stamp)
        assert canonical_social_analytics._resolve_runtime_version_stamp.cache_info().maxsize == 1
        assert first is second
        assert first == {"label": "stamp-1"}
        assert calls == [
            {
                "getenv": fake_getenv,
                "modal_environment": "main",
                "modal_function": "run_social_job",
                "execution_backend": "modal",
            }
        ]

        canonical_social_analytics._resolve_runtime_version_stamp.cache_clear()
        assert canonical_social_analytics._resolve_runtime_version_stamp() == {"label": "stamp-2"}
        assert len(calls) == 2
    finally:
        canonical_social_analytics._resolve_runtime_version_stamp.cache_clear()


def test_runtime_version_compatibility_identities_remain_stable() -> None:
    assert legacy_repo is canonical_social_analytics
    assert legacy_repo._resolve_runtime_version_stamp is canonical_social_analytics._resolve_runtime_version_stamp
    assert control_plane._resolve_runtime_version_stamp is runtime_surface._resolve_runtime_version_stamp
    assert control_plane._resolve_runtime_version_stamp is not canonical_social_analytics._resolve_runtime_version_stamp
