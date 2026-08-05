"""Compatibility boundary for the dispatch-runtime legacy owner transition."""

from __future__ import annotations

import ast
import os
import subprocess
import sys
from pathlib import Path

import pytest

import trr_backend.repositories.social_season_analytics as legacy_repo
import trr_backend.socials.control_plane.dispatch_runtime as dispatch_runtime
import trr_backend.socials.control_plane.run_lifecycle as run_lifecycle
import trr_backend.socials.social_season_analytics_impl as legacy_impl

BACKEND_ROOT = Path(__file__).resolve().parents[2]
DISPATCH_RUNTIME_PATH = BACKEND_ROOT / "trr_backend" / "socials" / "control_plane" / "dispatch_runtime.py"
LEGACY_MODULES = {
    "trr_backend.repositories.social_season_analytics",
    "trr_backend.socials.social_season_analytics_impl",
}
FRESH_IMPORT_ORDERS = (
    "trr_backend.socials.control_plane.dispatch_runtime",
    "trr_backend.socials.control_plane.run_lifecycle",
    "trr_backend.socials.control_plane",
    "trr_backend.socials.social_season_analytics_impl",
    "trr_backend.repositories.social_season_analytics",
    "trr_backend.socials.control_plane.shared_accounts",
    "trr_backend.socials.control_plane.runtime",
)


def _run_fresh_process(*lines: str) -> subprocess.CompletedProcess[str]:
    environment = dict(os.environ)
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    return subprocess.run(
        [sys.executable, "-B", "-c", "\n".join(lines)],
        cwd=BACKEND_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )


def test_dispatch_runtime_owns_exact_monolith_module_through_lifecycle_loader() -> None:
    assert dispatch_runtime.legacy is legacy_impl
    assert legacy_repo is legacy_impl
    assert legacy_repo.__dict__ is legacy_impl.__dict__
    assert run_lifecycle._legacy_module() is legacy_impl


def test_dispatch_runtime_and_lifecycle_proxy_observe_late_repository_patch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sentinel = object()
    monkeypatch.setattr(legacy_repo, "_r3_38_dispatch_runtime_probe", sentinel, raising=False)

    assert dispatch_runtime.legacy._r3_38_dispatch_runtime_probe is sentinel
    assert run_lifecycle.legacy._r3_38_dispatch_runtime_probe is sentinel


def test_dispatch_runtime_uses_late_module_provider_without_a_legacy_bootstrap_import() -> None:
    source = DISPATCH_RUNTIME_PATH.read_text()
    tree = ast.parse(source)
    direct_legacy_imports = [
        node
        for node in ast.walk(tree)
        if (isinstance(node, ast.ImportFrom) and node.module in LEGACY_MODULES)
        or (isinstance(node, ast.Import) and any(alias.name in LEGACY_MODULES for alias in node.names))
    ]
    lifecycle_imports = [
        alias
        for node in tree.body
        if isinstance(node, ast.ImportFrom)
        and node.module == "trr_backend.socials.control_plane.run_lifecycle"
        for alias in node.names
        if (alias.name, alias.asname) == ("_legacy_module", "_published")
    ]
    registry_imports = [
        alias.name
        for node in tree.body
        if isinstance(node, ast.ImportFrom) and node.module == "trr_backend.socials.provider_registry"
        for alias in node.names
    ]
    deleted_names = {
        target.id
        for node in tree.body
        if isinstance(node, ast.Delete)
        for target in node.targets
        if isinstance(target, ast.Name)
    }
    dynamic_imports = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "importlib"
        and node.func.attr == "import_module"
    ]

    assert direct_legacy_imports == []
    assert len(lifecycle_imports) == 1
    assert {"LateModuleProvider", "adopt_published", "publish_module_slot"}.issubset(registry_imports)
    assert "_published" in deleted_names
    assert dynamic_imports == []
    assert "_published" not in dispatch_runtime.__dict__
    assert dispatch_runtime.register_provider_publication_callback.__self__ is dispatch_runtime._PROVIDER
    assert (
        dispatch_runtime.register_provider_publication_callback.__func__
        is dispatch_runtime._PROVIDER.register_module_publication_callback.__func__
    )


@pytest.mark.parametrize("primary_module", FRESH_IMPORT_ORDERS)
def test_fresh_import_orders_preserve_exact_identity_and_dependent_proxy_contracts(
    primary_module: str,
) -> None:
    result = _run_fresh_process(
        "import importlib",
        f"importlib.import_module({primary_module!r})",
        "owner = importlib.import_module('trr_backend.socials.control_plane.dispatch_runtime')",
        "lifecycle = importlib.import_module('trr_backend.socials.control_plane.run_lifecycle')",
        "impl = importlib.import_module('trr_backend.socials.social_season_analytics_impl')",
        "repo = importlib.import_module('trr_backend.repositories.social_season_analytics')",
        "runtime = importlib.import_module('trr_backend.socials.control_plane.runtime')",
        "shared = importlib.import_module('trr_backend.socials.control_plane.shared_accounts')",
        "review = importlib.import_module('trr_backend.socials.pipelines.account_catalog.review_queue')",
        "dispatch = importlib.import_module('trr_backend.socials.control_plane.dispatch')",
        "assert owner.legacy is impl",
        "assert lifecycle._legacy_module() is impl",
        "assert repo is impl",
        "assert repo.__dict__ is impl.__dict__",
        "assert runtime._core is impl",
        "assert shared.default_targets is impl._default_targets",
        "assert review.get_social_account_catalog_review_queue is impl.get_social_account_catalog_review_queue",
        "assert dispatch.cancel_run is impl.cancel_run",
        "assert '_published' not in owner.__dict__",
        "copied_review = review.get_social_account_catalog_review_queue",
        "sentinel = object()",
        "repo.get_social_account_catalog_review_queue = sentinel",
        "assert owner.legacy.get_social_account_catalog_review_queue is sentinel",
        "assert lifecycle.legacy.get_social_account_catalog_review_queue is sentinel",
        "assert review.get_social_account_catalog_review_queue is copied_review",
    )

    assert result.returncode == 0, result.stderr


def test_repository_patch_before_dispatch_runtime_reload_stays_visible() -> None:
    result = _run_fresh_process(
        "import importlib",
        "repo = importlib.import_module('trr_backend.repositories.social_season_analytics')",
        "owner = importlib.import_module('trr_backend.socials.control_plane.dispatch_runtime')",
        "sentinel = object()",
        "repo._r3_38_dispatch_runtime_reload_probe = sentinel",
        "owner = importlib.reload(owner)",
        "impl = importlib.import_module('trr_backend.socials.social_season_analytics_impl')",
        "assert owner.legacy is impl",
        "assert owner.legacy._r3_38_dispatch_runtime_reload_probe is sentinel",
        "assert '_published' not in owner.__dict__",
    )

    assert result.returncode == 0, result.stderr


def test_dispatch_runtime_defers_callback_until_exact_module_publication() -> None:
    result = _run_fresh_process(
        "import importlib, sys",
        "owner = importlib.import_module('trr_backend.socials.control_plane.dispatch_runtime')",
        "assert 'trr_backend.socials.social_season_analytics_impl' not in sys.modules",
        "observed = []",
        "def callback(module): observed.append(module)",
        "owner.register_provider_publication_callback(callback)",
        "impl = importlib.import_module('trr_backend.socials.social_season_analytics_impl')",
        "assert observed == [impl]",
        "owner.register_provider_publication_callback(callback)",
        "assert observed == [impl]",
    )

    assert result.returncode == 0, result.stderr
