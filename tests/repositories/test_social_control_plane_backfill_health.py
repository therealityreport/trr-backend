from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path

import pytest

import trr_backend.socials.social_season_analytics_impl as legacy_core
from trr_backend.socials.control_plane import backfill_health, queue_status


def test_backfill_health_ordinary_import_uses_configured_live_provider() -> None:
    backend_root = Path(__file__).resolve().parents[2]
    script = "\n".join(
        (
            "import sys",
            "legacy_name = 'trr_backend.socials.social_season_analytics_impl'",
            "assert legacy_name not in sys.modules",
            "import trr_backend.socials.control_plane.backfill_health as leaf",
            "import trr_backend.socials.control_plane.queue_status as queue_status",
            "legacy = sys.modules[legacy_name]",
            "assert leaf._core is queue_status._legacy_repo()",
            "assert queue_status._LEGACY_NAMESPACE is legacy.__dict__",
            "assert leaf._core._now_utc is legacy.__dict__['_now_utc']",
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


def test_backfill_health_reuses_exact_queue_status_provider() -> None:
    assert backfill_health._core is queue_status._legacy_repo()
    assert queue_status._LEGACY_NAMESPACE is legacy_core.__dict__
    assert backfill_health._core.__name__ == legacy_core.__name__


def test_backfill_health_source_keeps_core_access_function_scoped() -> None:
    source_path = Path(backfill_health.__file__).resolve()
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
        if not (isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id == "_core"):
            continue
        scope = parents.get(node)
        while scope is not None and not isinstance(scope, (ast.FunctionDef, ast.AsyncFunctionDef)):
            scope = parents.get(scope)
        assert scope is not None, f"import-time _core access at line {node.lineno}"

    assert "from trr_backend.socials.control_plane.queue_status import _legacy_repo, get_queue_status" in source
    assert "_core = _legacy_repo()" in source
    assert "_core.__dict__" not in source


def test_backfill_health_provider_fails_deterministically_when_unconfigured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with monkeypatch.context() as scoped:
        scoped.setattr(queue_status, "_LEGACY_NAMESPACE", None)
        with pytest.raises(
            RuntimeError,
            match="Queue-status provider is not configured for read: _now_utc",
        ):
            _ = backfill_health._core._now_utc


def test_backfill_health_provider_observes_late_legacy_monkeypatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def replacement() -> str:
        return "late-provider-value"

    with monkeypatch.context() as scoped:
        scoped.setattr(legacy_core, "_now_utc", replacement)
        assert backfill_health._core._now_utc is replacement


def test_backfill_health_provider_assignment_writes_through(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = legacy_core._now_utc

    def replacement() -> str:
        return "write-through-provider-value"

    with monkeypatch.context() as scoped:
        scoped.setattr(backfill_health._core, "_now_utc", replacement)
        assert legacy_core._now_utc is replacement

    assert legacy_core._now_utc is original
