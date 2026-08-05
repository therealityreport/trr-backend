"""Cold-import contract for the extracted Instagram comments provider leaf."""

from __future__ import annotations

import ast
import os
import subprocess
import sys
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[2]
MODULE = "trr_backend.socials.pipelines.comments.instagram"
LEGACY = "trr_backend.socials.social_season_analytics_impl"
SOURCE = BACKEND_ROOT / "trr_backend" / "socials" / "pipelines" / "comments" / "instagram.py"


def _fresh(*lines: str) -> None:
    result = subprocess.run(
        [sys.executable, "-B", "-c", "\n".join(lines)],
        cwd=BACKEND_ROOT,
        env=dict(os.environ, PYTHONDONTWRITEBYTECODE="1"),
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_comments_cold_import_does_not_load_the_monolith_and_fails_closed() -> None:
    _fresh(
        "import importlib, sys",
        f"room = importlib.import_module({MODULE!r})",
        f"assert {LEGACY!r} not in sys.modules",
        "assert room._PROVIDER_STATE == 'UNCONFIGURED'",
        "try:",
        "    room._require_provider_ready()",
        "except RuntimeError as error:",
        "    assert 'INSTAGRAM_COMMENTS_PROVIDER_UNCONFIGURED' in str(error)",
        "else:",
        "    raise AssertionError('cold comments provider did not fail closed')",
    )


def test_comments_tail_publication_keeps_module_and_room_patch_seams() -> None:
    _fresh(
        "import importlib",
        f"room = importlib.import_module({MODULE!r})",
        f"impl = importlib.import_module({LEGACY!r})",
        "assert room._PROVIDER_STATE == 'READY'",
        "assert room._PROVIDER_NAMESPACE is impl.__dict__",
        "assert room._core is impl",
        "name = 'start_social_account_comments_scrape'",
        "local = room._LOCAL_ROOM_FUNCTIONS[name]",
        "assert room._CORE_ROOM_WRAPPERS[name] is impl.__dict__[name]",
        "assert room._room_callable(name, local) is local",
    )


def test_comments_source_has_no_legacy_import() -> None:
    tree = ast.parse(SOURCE.read_text(encoding="utf-8"))
    forbidden = {LEGACY, "trr_backend.repositories.social_season_analytics"}
    assert [
        node
        for node in ast.walk(tree)
        if (isinstance(node, ast.ImportFrom) and node.module in forbidden)
        or (isinstance(node, ast.Import) and any(alias.name in forbidden for alias in node.names))
    ] == []
