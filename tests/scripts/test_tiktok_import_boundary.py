"""Import-boundary guard for the TikTok platform Module.

No module under ``trr_backend.socials.tiktok`` may import the legacy social
season analytics repository (``social_season_analytics_impl`` or its
``trr_backend.repositories`` alias), neither in source nor transitively at
import time. Extracted rooms reach repository helpers only through the
late-published provider namespace; the repository keeps delegating wrappers.
"""

from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path

PACKAGE_DIR = Path(__file__).resolve().parents[2] / "trr_backend" / "socials" / "tiktok"
FORBIDDEN_MODULES = {
    "trr_backend.repositories.social_season_analytics",
    "trr_backend.socials.social_season_analytics_impl",
}


def _package_sources() -> list[Path]:
    return sorted(path for path in PACKAGE_DIR.rglob("*.py") if "__pycache__" not in path.parts)


def test_tiktok_package_has_modules() -> None:
    names = {path.relative_to(PACKAGE_DIR).as_posix() for path in _package_sources()}
    assert {"scraper.py", "cookies.py", "posts/ingest.py", "shared_catalog/scrape.py", "analytics/reads.py"} <= names


def test_tiktok_sources_never_import_the_legacy_repository() -> None:
    offenders: list[str] = []
    for path in _package_sources():
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module in FORBIDDEN_MODULES:
                offenders.append(f"{path.relative_to(PACKAGE_DIR)}:{node.lineno}")
            elif isinstance(node, ast.Import) and any(alias.name in FORBIDDEN_MODULES for alias in node.names):
                offenders.append(f"{path.relative_to(PACKAGE_DIR)}:{node.lineno}")
        source = path.read_text(encoding="utf-8")
        for forbidden in FORBIDDEN_MODULES:
            if forbidden in source and path.name != "scraper.py":
                offenders.append(f"{path.relative_to(PACKAGE_DIR)} mentions {forbidden}")
    assert offenders == []


def test_tiktok_extracted_rooms_delegate_only_through_local_room_registries() -> None:
    """Every provider-backed leaf exposes the registry the repository wrappers call."""

    registry_modules = {
        "cookies.py",
        "parsing/profile.py",
        "parsing/sounds.py",
        "posts/persistence.py",
        "posts/ingest.py",
        "comments/persistence.py",
        "comments/coverage.py",
        "comments/media_mirror.py",
        "shared_catalog/context.py",
        "shared_catalog/config.py",
        "shared_catalog/discovery.py",
        "shared_catalog/scrape.py",
        "shared_catalog/persistence.py",
        "analytics/week.py",
        "analytics/filters.py",
        "analytics/header_stats.py",
        "analytics/reads.py",
        "alerts.py",
        "lane_start.py",
    }
    for relative in sorted(registry_modules):
        source = (PACKAGE_DIR / relative).read_text(encoding="utf-8")
        assert "_LOCAL_ROOM_FUNCTIONS" in source, relative
        assert "_configure_legacy_provider" in source, relative


def test_importing_every_tiktok_module_does_not_load_the_legacy_repository() -> None:
    script = """
import importlib
import pkgutil
import sys

import trr_backend.socials.tiktok as package

for info in pkgutil.walk_packages(package.__path__, package.__name__ + "."):
    importlib.import_module(info.name)

loaded = sorted(
    name
    for name in sys.modules
    if name in {"trr_backend.repositories.social_season_analytics", "trr_backend.socials.social_season_analytics_impl"}
)
assert loaded == [], loaded
print("ok")
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=False,
        cwd=str(PACKAGE_DIR.parents[2]),
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "ok"
