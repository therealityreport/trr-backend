"""Compatibility contract for the analytics-cache invalidator registration boundary."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[3]
ANALYTICS_CACHE_PATH = BACKEND_ROOT / "api" / "routers" / "socials" / "_analytics_cache.py"


def _run_fresh_python(lines: list[str]) -> None:
    result = subprocess.run(
        [sys.executable, "-c", "\n".join(lines)],
        cwd=BACKEND_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_analytics_cache_registration_uses_live_dispatch_runtime_proxy() -> None:
    source = ANALYTICS_CACHE_PATH.read_text(encoding="utf-8")

    assert "register_provider_publication_callback" in source
    assert "register_provider_publication_callback(_publish_week_detail_cache_invalidator)" in source
    assert "social_repo.register_week_detail_cache_invalidator(invalidate_week_detail_cache)" in source
    assert "trr_backend.repositories.social_season_analytics" not in source
    assert "trr_backend.socials.social_season_analytics_impl" not in source


def test_analytics_cache_cold_import_registers_exact_callback() -> None:
    _run_fresh_python(
        [
            "import importlib",
            "router = importlib.import_module('api.routers.socials')",
            "cache = importlib.import_module('api.routers.socials._analytics_cache')",
            "runtime = importlib.import_module('trr_backend.socials.control_plane.dispatch_runtime')",
            "legacy = importlib.import_module('trr_backend.socials.social_season_analytics_impl')",
            "repo = importlib.import_module('trr_backend.repositories.social_season_analytics')",
            "assert runtime.legacy is legacy",
            "assert repo.__dict__ is legacy.__dict__",
            "assert legacy._week_detail_cache_invalidator is cache.invalidate_week_detail_cache",
            "assert router.invalidate_week_detail_cache is cache.invalidate_week_detail_cache",
        ]
    )


def test_analytics_cache_registration_sees_late_alias_patch_after_dispatch_preload() -> None:
    _run_fresh_python(
        [
            "import importlib",
            "dispatch = importlib.import_module('trr_backend.socials.control_plane.dispatch')",
            "repo = importlib.import_module('trr_backend.repositories.social_season_analytics')",
            "calls = []",
            "def patched(callback): calls.append(callback)",
            "repo.register_week_detail_cache_invalidator = patched",
            "router = importlib.import_module('api.routers.socials')",
            "cache = importlib.import_module('api.routers.socials._analytics_cache')",
            "runtime = importlib.import_module('trr_backend.socials.control_plane.dispatch_runtime')",
            "legacy = importlib.import_module('trr_backend.socials.social_season_analytics_impl')",
            "assert runtime.legacy is legacy",
            "assert repo.__dict__ is legacy.__dict__",
            "assert dispatch.register_week_detail_cache_invalidator is not repo.register_week_detail_cache_invalidator",
            "assert calls == [cache.invalidate_week_detail_cache]",
            "assert router.invalidate_week_detail_cache is cache.invalidate_week_detail_cache",
        ]
    )
