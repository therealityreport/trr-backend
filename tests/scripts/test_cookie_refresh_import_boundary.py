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


def test_cookie_refresh_uses_exact_dispatch_runtime_proxy_without_legacy_import() -> None:
    source = SOURCE_PATH.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imports = [node for node in tree.body if isinstance(node, ast.ImportFrom)]

    assert any(
        node.module == "trr_backend.socials.control_plane.dispatch_runtime"
        and [(alias.name, alias.asname) for alias in node.names] == [("legacy", "social_repo")]
        for node in imports
    )
    assert "trr_backend.repositories.social_season_analytics" not in source
    assert "trr_backend.socials.social_season_analytics_impl" not in source


def test_cookie_refresh_exact_proxy_preserves_import_time_handler_bindings() -> None:
    _run_fresh_python(
        [
            "import importlib",
            "legacy = importlib.import_module('trr_backend.socials.social_season_analytics_impl')",
            "before = object()",
            "after = object()",
            "legacy._load_instagram_cookies = before",
            "leaf = importlib.import_module('trr_backend.socials.ops.cookie_refresh')",
            "runtime = importlib.import_module('trr_backend.socials.control_plane.dispatch_runtime')",
            "repository_alias = importlib.import_module('trr_backend.repositories.social_season_analytics')",
            "assert runtime.legacy is legacy",
            "assert repository_alias is legacy",
            "assert leaf.social_repo is legacy",
            "assert leaf.PLATFORM_HANDLERS['instagram'].load is before",
            "legacy._load_instagram_cookies = after",
            "assert leaf.PLATFORM_HANDLERS['instagram'].load is before",
        ]
    )


def test_cookie_refresh_path_helpers_keep_live_proxy_lookup() -> None:
    _run_fresh_python(
        [
            "import importlib",
            "from pathlib import Path",
            "leaf = importlib.import_module('trr_backend.socials.ops.cookie_refresh')",
            "legacy = leaf.social_repo",
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
