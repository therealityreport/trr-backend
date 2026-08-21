from __future__ import annotations

import ast
import inspect
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

import trr_backend.repositories.social_season_analytics as legacy_repo
import trr_backend.socials.control_plane as control_plane
import trr_backend.socials.control_plane.dispatch_runtime as dispatch_runtime
import trr_backend.socials.control_plane.runtime as runtime_surface
import trr_backend.socials.social_season_analytics_impl as canonical_social_analytics

EXPECTED_EXPORTS = (
    "SocialIngestConflictError",
    "SocialIngestValidationError",
    "SocialWorkerUnavailableError",
    "_adapt_payload_json_values",
    "_load_facebook_cookies",
    "_load_instagram_cookies",
    "_load_threads_cookies",
    "_load_tiktok_cookies",
    "_load_twikit_credentials",
    "_load_twitter_auth",
    "_pg_upsert_many",
    "_resolve_runtime_version_stamp",
    "adapt_payload_json_values",
    "check_platform_cookie_health",
    "load_facebook_cookies",
    "load_instagram_cookies",
    "load_threads_cookies",
    "load_tiktok_cookies",
    "load_twikit_credentials",
    "load_twitter_auth",
    "pg_upsert_many",
    "refresh_platform_cookies_interactive",
)
EXPECTED_SIGNATURES = {
    "SocialIngestConflictError": "(code: 'str', message: 'str', *, detail: 'Mapping[str, Any] | None' = None)",
    "SocialIngestValidationError": "(code: 'str', message: 'str', *, detail: 'Mapping[str, Any] | None' = None)",
    "SocialWorkerUnavailableError": "(message: 'str', *, worker_health: 'dict[str, Any]')",
    "_adapt_payload_json_values": "(value: 'Any') -> 'Any'",
    "_load_facebook_cookies": "() -> 'dict[str, str]'",
    "_load_instagram_cookies": "() -> 'dict[str, str]'",
    "_load_threads_cookies": "() -> 'dict[str, str]'",
    "_load_tiktok_cookies": "() -> 'dict[str, str]'",
    "_load_twikit_credentials": "(twitter_cookies: 'Any' = None) -> 'Any'",
    "_load_twitter_auth": "() -> 'Any'",
    "_pg_upsert_many": "(*args: 'Any', **kwargs: 'Any') -> 'Any'",
    "_resolve_runtime_version_stamp": "() -> 'str'",
    "adapt_payload_json_values": "(value: 'Any') -> 'Any'",
    "check_platform_cookie_health": "(*args: 'Any', **kwargs: 'Any') -> 'Any'",
    "load_facebook_cookies": "() -> 'dict[str, str]'",
    "load_instagram_cookies": "() -> 'dict[str, str]'",
    "load_threads_cookies": "() -> 'dict[str, str]'",
    "load_tiktok_cookies": "() -> 'dict[str, str]'",
    "load_twikit_credentials": "(twitter_cookies: 'Any' = None) -> 'Any'",
    "load_twitter_auth": "() -> 'Any'",
    "pg_upsert_many": "(*args: 'Any', **kwargs: 'Any') -> 'Any'",
    "refresh_platform_cookies_interactive": "(*args: 'Any', **kwargs: 'Any') -> 'Any'",
}
PACKAGE_EXPORTS = (
    "SocialIngestConflictError",
    "SocialIngestValidationError",
    "SocialWorkerUnavailableError",
    "_adapt_payload_json_values",
    "_load_facebook_cookies",
    "_load_instagram_cookies",
    "_load_threads_cookies",
    "_load_tiktok_cookies",
    "_load_twikit_credentials",
    "_load_twitter_auth",
    "_pg_upsert_many",
    "_resolve_runtime_version_stamp",
    "check_platform_cookie_health",
    "refresh_platform_cookies_interactive",
)


def test_runtime_surface_preserves_exact_module_exceptions_signatures_aliases_and_package_exports() -> None:
    assert dispatch_runtime.legacy is canonical_social_analytics
    assert legacy_repo is canonical_social_analytics
    assert legacy_repo.__dict__ is canonical_social_analytics.__dict__
    assert runtime_surface._core is canonical_social_analytics

    for name in (
        "SocialIngestConflictError",
        "SocialIngestValidationError",
        "SocialWorkerUnavailableError",
    ):
        assert getattr(runtime_surface, name) is getattr(canonical_social_analytics, name)

    assert tuple(runtime_surface.__all__) == EXPECTED_EXPORTS
    assert {
        name: str(inspect.signature(getattr(runtime_surface, name))) for name in runtime_surface.__all__
    } == EXPECTED_SIGNATURES
    assert all(getattr(control_plane, name) is getattr(runtime_surface, name) for name in PACKAGE_EXPORTS)

    for public_name, private_name in (
        ("adapt_payload_json_values", "_adapt_payload_json_values"),
        ("load_facebook_cookies", "_load_facebook_cookies"),
        ("load_instagram_cookies", "_load_instagram_cookies"),
        ("load_threads_cookies", "_load_threads_cookies"),
        ("load_tiktok_cookies", "_load_tiktok_cookies"),
        ("load_twikit_credentials", "_load_twikit_credentials"),
        ("load_twitter_auth", "_load_twitter_auth"),
        ("pg_upsert_many", "_pg_upsert_many"),
    ):
        assert getattr(runtime_surface, public_name) is getattr(runtime_surface, private_name)

    assert runtime_surface._resolve_runtime_version_stamp is control_plane._resolve_runtime_version_stamp
    assert (
        runtime_surface._resolve_runtime_version_stamp is not canonical_social_analytics._resolve_runtime_version_stamp
    )


def test_runtime_surface_retains_late_monolith_and_repository_monkeypatch_visibility(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def replacement_for(
        marker: object,
        calls: list[tuple[tuple[Any, ...], dict[str, Any]]],
    ) -> Any:
        def replacement(*received_args: Any, **received_kwargs: Any) -> Any:
            calls.append((received_args, received_kwargs))
            return marker

        return replacement

    cases: tuple[
        tuple[str, Any, tuple[Any, ...], dict[str, Any]],
        ...,
    ] = (
        ("_load_facebook_cookies", runtime_surface.load_facebook_cookies, (), {}),
        (
            "_adapt_payload_json_values",
            runtime_surface.adapt_payload_json_values,
            ({"value": object()},),
            {},
        ),
        ("_pg_upsert_many", runtime_surface.pg_upsert_many, ("table", [{"id": 1}]), {"chunk_size": 1}),
        ("_resolve_runtime_version_stamp", runtime_surface._resolve_runtime_version_stamp, (), {}),
        (
            "check_platform_cookie_health",
            runtime_surface.check_platform_cookie_health,
            ("instagram",),
            {"refresh": False},
        ),
        (
            "refresh_platform_cookies_interactive",
            runtime_surface.refresh_platform_cookies_interactive,
            ("instagram",),
            {"headless": True},
        ),
    )

    for index, (target_name, wrapper, args, kwargs) in enumerate(cases):
        original = getattr(canonical_social_analytics, target_name)
        marker = object()
        calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

        patch_owner = legacy_repo if index % 2 else canonical_social_analytics
        with monkeypatch.context() as scoped:
            scoped.setattr(patch_owner, target_name, replacement_for(marker, calls))
            assert wrapper(*args, **kwargs) is marker
            assert calls == [(args, kwargs)]

        assert getattr(canonical_social_analytics, target_name) is original
        assert getattr(legacy_repo, target_name) is original


def test_runtime_surface_cold_import_order_and_ast_proxy_contract() -> None:
    backend_root = Path(__file__).resolve().parents[2]
    runtime_path = backend_root / "trr_backend" / "socials" / "control_plane" / "runtime.py"
    package_path = runtime_path.parent / "__init__.py"
    runtime_tree = ast.parse(runtime_path.read_text())
    package_tree = ast.parse(package_path.read_text())

    forbidden_modules = {
        "trr_backend.repositories.social_season_analytics",
        "trr_backend.socials.social_season_analytics_impl",
    }
    direct_legacy_imports = [
        node
        for node in ast.walk(runtime_tree)
        if (isinstance(node, ast.ImportFrom) and node.module in forbidden_modules)
        or (isinstance(node, ast.Import) and any(alias.name in forbidden_modules for alias in node.names))
    ]
    accepted_proxy_imports = [
        node
        for node in runtime_tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "trr_backend.socials.control_plane.dispatch_runtime"
        and [(alias.name, alias.asname) for alias in node.names] == [("legacy", "_core")]
    ]
    deleted_names = {
        target.id
        for node in runtime_tree.body
        if isinstance(node, ast.Delete)
        for target in node.targets
        if isinstance(target, ast.Name)
    }
    core_references = [
        node
        for node in ast.walk(runtime_tree)
        if isinstance(node, ast.Name) and node.id == "_core" and isinstance(node.ctx, ast.Load)
    ]
    package_imports = {
        node.module: node.lineno
        for node in package_tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module
        in {
            "trr_backend.socials.control_plane.dispatch",
            "trr_backend.socials.control_plane.runtime",
        }
    }

    assert direct_legacy_imports == []
    assert len(accepted_proxy_imports) == 1
    assert "_core" not in deleted_names
    assert core_references
    assert package_imports == {}

    code = "\n".join(
        (
            "import importlib",
            "import sys",
            "runtime = importlib.import_module('trr_backend.socials.control_plane.runtime')",
            "assert 'trr_backend.socials.social_season_analytics_impl' not in sys.modules",
            "owner = importlib.import_module('trr_backend.socials.control_plane.dispatch_runtime')",
            "impl = importlib.import_module('trr_backend.socials.social_season_analytics_impl')",
            "repo = importlib.import_module('trr_backend.repositories.social_season_analytics')",
            "package = importlib.import_module('trr_backend.socials.control_plane')",
            "assert owner.legacy is impl",
            "assert runtime._core is impl",
            "assert repo is impl",
            "assert repo.__dict__ is impl.__dict__",
            "assert package._load_tiktok_cookies is runtime._load_tiktok_cookies",
            "loaded = list(sys.modules)",
            "assert loaded.index('trr_backend.socials.control_plane.dispatch_runtime') < loaded.index(",
            "    'trr_backend.socials.control_plane.runtime'",
            ")",
        )
    )
    environment = dict(os.environ)
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    result = subprocess.run(
        [sys.executable, "-B", "-c", code],
        cwd=backend_root,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
