"""Cold-import contract for the lifecycle late-module provider."""

from __future__ import annotations

import ast
import os
import subprocess
import sys
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[2]
MODULE = "trr_backend.socials.control_plane.run_lifecycle"
LEGACY = "trr_backend.socials.social_season_analytics_impl"
SOURCE = BACKEND_ROOT / "trr_backend" / "socials" / "control_plane" / "run_lifecycle.py"


def _fresh(*lines: str) -> None:
    environment = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
    result = subprocess.run(
        [sys.executable, "-B", "-c", "\n".join(lines)],
        cwd=BACKEND_ROOT,
        env=environment,
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_lifecycle_cold_import_is_fail_closed_without_loading_the_monolith() -> None:
    _fresh(
        "import importlib, sys",
        f"room = importlib.import_module({MODULE!r})",
        f"assert {LEGACY!r} not in sys.modules",
        "assert room._PROVIDER_STATE == 'UNCONFIGURED'",
        "for action in (lambda: room._legacy_module(), lambda: room.legacy._now_utc):",
        "    try:",
        "        action()",
        "    except RuntimeError as error:",
        "        assert 'RUN_LIFECYCLE_PROVIDER_UNCONFIGURED' in str(error)",
        "    else:",
        "        raise AssertionError('cold lifecycle access did not fail closed')",
    )


def test_lifecycle_publishes_the_exact_loaded_module_after_monolith_tail() -> None:
    _fresh(
        "import importlib",
        f"room = importlib.import_module({MODULE!r})",
        f"impl = importlib.import_module({LEGACY!r})",
        "assert room._PROVIDER_STATE == 'READY'",
        "assert room._PROVIDER_NAMESPACE is impl.__dict__",
        "assert room._legacy_module() is impl",
        "assert room.legacy._now_utc is impl._now_utc",
    )


def test_lifecycle_source_has_no_legacy_or_dynamic_import_bootstrap() -> None:
    tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
    forbidden = {LEGACY, "trr_backend.repositories.social_season_analytics"}
    imports = [
        node
        for node in ast.walk(tree)
        if (isinstance(node, ast.ImportFrom) and node.module in forbidden)
        or (isinstance(node, ast.Import) and any(alias.name in forbidden for alias in node.names))
    ]
    dynamic_imports = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "importlib"
    ]
    assert imports == []
    assert dynamic_imports == []
