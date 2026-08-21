from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path

import pytest

import trr_backend.socials.social_season_analytics_impl as legacy_core
from trr_backend.socials.control_plane import queue_status, shared_status_reads


def test_shared_status_reads_ordinary_import_defers_provider_until_late_publication() -> None:
    backend_root = Path(__file__).resolve().parents[2]
    script = "\n".join(
        (
            "import sys",
            "legacy_name = 'trr_backend.socials.social_season_analytics_impl'",
            "assert legacy_name not in sys.modules",
            "import trr_backend.socials.control_plane.shared_status_reads as leaf",
            "import trr_backend.socials.control_plane.queue_status as queue_status",
            "assert legacy_name not in sys.modules",
            "assert queue_status._LEGACY_NAMESPACE is None",
            "try:",
            "    leaf.legacy.pg",
            "except RuntimeError as exc:",
            "    assert 'Queue-status provider is not configured' in str(exc)",
            "else:",
            "    raise AssertionError('unpublished provider must fail closed')",
            "import trr_backend.socials.social_season_analytics_impl as legacy",
            "legacy = sys.modules[legacy_name]",
            "assert leaf.legacy is queue_status._legacy_repo()",
            "assert queue_status._LEGACY_NAMESPACE is legacy.__dict__",
            "assert leaf.legacy._get_social_hot_path_cache is legacy.__dict__['_get_social_hot_path_cache']",
        )
    )

    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=backend_root,
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr


def test_shared_status_reads_reuses_exact_queue_status_provider() -> None:
    assert shared_status_reads.legacy is queue_status._legacy_repo()
    assert queue_status._LEGACY_NAMESPACE is legacy_core.__dict__
    assert shared_status_reads.legacy.__name__ == legacy_core.__name__


def test_shared_status_reads_source_keeps_legacy_access_function_scoped() -> None:
    source_path = Path(shared_status_reads.__file__).resolve()
    source = source_path.read_text()
    tree = ast.parse(source, filename=str(source_path))
    parents = {child: parent for parent in ast.walk(tree) for child in ast.iter_child_nodes(parent)}
    legacy_modules = {
        "trr_backend.repositories.social_season_analytics",
        "trr_backend.socials.social_season_analytics_impl",
    }

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            assert all(alias.name not in legacy_modules for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            assert node.module not in legacy_modules
        elif isinstance(node, ast.Call) and node.args:
            function_name = (
                node.func.id
                if isinstance(node.func, ast.Name)
                else node.func.attr
                if isinstance(node.func, ast.Attribute)
                else ""
            )
            if function_name in {"import_module", "__import__"}:
                first_argument = node.args[0]
                assert not (isinstance(first_argument, ast.Constant) and first_argument.value in legacy_modules)
        if not (isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load) and node.id == "legacy"):
            continue
        scope = parents.get(node)
        while scope is not None and not isinstance(
            scope,
            (ast.FunctionDef, ast.AsyncFunctionDef),
        ):
            scope = parents.get(scope)
        assert scope is not None, f"import-time legacy access at line {node.lineno}"

    assert "from trr_backend.socials.control_plane.queue_status import _legacy_repo" in source
    assert "legacy = _legacy_repo()" in source
    assert "legacy.__dict__" not in source


def test_shared_status_reads_provider_fails_deterministically_when_unconfigured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with monkeypatch.context() as scoped:
        scoped.setattr(queue_status, "_LEGACY_NAMESPACE", None)
        with pytest.raises(
            RuntimeError,
            match="Queue-status provider is not configured for read: pg",
        ):
            _ = shared_status_reads.legacy.pg


def test_shared_status_reads_provider_observes_late_legacy_monkeypatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def replacement(_key: object) -> str:
        return "late-provider-value"

    with monkeypatch.context() as scoped:
        scoped.setattr(legacy_core, "_get_social_hot_path_cache", replacement)
        assert shared_status_reads.legacy._get_social_hot_path_cache is replacement


def test_shared_status_reads_provider_assignment_writes_through(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = legacy_core._get_social_hot_path_cache

    def replacement(_key: object) -> str:
        return "write-through-provider-value"

    with monkeypatch.context() as scoped:
        scoped.setattr(
            shared_status_reads.legacy,
            "_get_social_hot_path_cache",
            replacement,
        )
        assert legacy_core._get_social_hot_path_cache is replacement

    assert legacy_core._get_social_hot_path_cache is original
