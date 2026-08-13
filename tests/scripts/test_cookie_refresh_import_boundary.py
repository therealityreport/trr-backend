"""Compatibility contract for the social cookie-refresh operator import boundary."""

from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[2]
SOURCE_PATH = BACKEND_ROOT / "trr_backend" / "socials" / "ops" / "cookie_refresh.py"


def _run_fresh_python(lines: list[str]) -> None:
    result = subprocess.run(
        [sys.executable, "-c", "\n".join(lines)],
        cwd=BACKEND_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_cookie_refresh_uses_a_late_control_plane_proxy_without_monolith_import() -> None:
    source = SOURCE_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imports = [node for node in tree.body if isinstance(node, ast.ImportFrom)]

    assert any(
        [(alias.name, alias.asname) for alias in node.names] == [("legacy", "social_repo")]
        and node.module == "trr_backend.socials.control_plane.dispatch_runtime"
        for node in imports
    )
    assert "trr_backend.socials.social_season_analytics_impl" not in source
    assert "trr_backend.repositories.social_season_analytics" not in source
    assert "__import__(" not in source


def test_cookie_refresh_import_succeeds_in_a_fresh_process() -> None:
    _run_fresh_python(
        [
            "import trr_backend.socials.ops.cookie_refresh as leaf",
            "import sys",
            "assert 'trr_backend.socials.social_season_analytics_impl' not in sys.modules",
            (
                "assert set(leaf.PLATFORM_HANDLERS) == {'instagram', 'tiktok', 'twitter', "
                "'facebook', 'threads', 'socialblade'}"
            ),
        ]
    )


def test_cookie_refresh_handler_bindings_are_late_and_preserve_legacy_patches() -> None:
    _run_fresh_python(
        [
            "import importlib",
            "legacy = importlib.import_module('trr_backend.socials.social_season_analytics_impl')",
            "leaf = importlib.import_module('trr_backend.socials.ops.cookie_refresh')",
            "assert leaf.PLATFORM_HANDLERS['instagram'].load is leaf._load_instagram_cookies",
            "legacy._load_instagram_cookies = lambda: {'sessionid': 'before'}",
            "assert leaf.PLATFORM_HANDLERS['instagram'].load() == {'sessionid': 'before'}",
            "legacy._load_instagram_cookies = lambda: {'sessionid': 'after'}",
            "assert leaf.PLATFORM_HANDLERS['instagram'].load() == {'sessionid': 'after'}",
        ]
    )


def test_cookie_refresh_path_helpers_keep_live_proxy_lookup() -> None:
    _run_fresh_python(
        [
            "import importlib",
            "from pathlib import Path",
            "leaf = importlib.import_module('trr_backend.socials.ops.cookie_refresh')",
            "legacy = importlib.import_module('trr_backend.socials.social_season_analytics_impl')",
            "calls = []",
            "legacy._default_tiktok_cookie_file_path = lambda: Path('/tmp/default.json')",
            "legacy._platform_cookie_refresh_target_path = (",
            "    lambda default, *keys: calls.append((default, keys)) or Path('/tmp/late.json')",
            ")",
            "assert leaf._tiktok_cookie_path() == Path('/tmp/late.json')",
            "assert calls == [(",
            "    Path('/tmp/default.json'),",
            "    ('SOCIAL_TIKTOK_COOKIES_FILE', 'TIKTOK_COOKIES_FILE'),",
            ")]",
        ]
    )
