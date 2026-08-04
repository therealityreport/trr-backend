"""Compatibility contract for the account-catalog review-queue import boundary."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parents[2]
REVIEW_QUEUE_PATH = BACKEND_ROOT / "trr_backend" / "socials" / "pipelines" / "account_catalog" / "review_queue.py"


def _run_fresh_python(lines: list[str]) -> None:
    result = subprocess.run(
        [sys.executable, "-c", "\n".join(lines)],
        cwd=BACKEND_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_review_queue_uses_exact_dispatch_runtime_proxy_without_legacy_import() -> None:
    source = REVIEW_QUEUE_PATH.read_text(encoding="utf-8")

    assert "from trr_backend.socials.control_plane.dispatch_runtime import legacy as _core" in source
    assert "get_social_account_catalog_review_queue = _core.get_social_account_catalog_review_queue" in source
    assert (
        "resolve_social_account_catalog_review_queue_item = _core.resolve_social_account_catalog_review_queue_item"
    ) in source
    assert "trr_backend.repositories.social_season_analytics" not in source
    assert "trr_backend.socials.social_season_analytics_impl" not in source


def test_review_queue_cold_import_preserves_lazy_export_and_patch_before_import() -> None:
    _run_fresh_python(
        [
            "import importlib",
            "import sys",
            "module_name = 'trr_backend.socials.pipelines.account_catalog.review_queue'",
            "package = importlib.import_module('trr_backend.socials.pipelines.account_catalog')",
            "assert module_name not in sys.modules",
            "expected = {",
            "    'get_social_account_catalog_review_queue',",
            "    'resolve_social_account_catalog_review_queue_item',",
            "}",
            "assert expected <= set(package.__all__)",
            "legacy = importlib.import_module('trr_backend.socials.social_season_analytics_impl')",
            "before_get = object()",
            "before_resolve = object()",
            "legacy.get_social_account_catalog_review_queue = before_get",
            "legacy.resolve_social_account_catalog_review_queue_item = before_resolve",
            "assert package.get_social_account_catalog_review_queue is before_get",
            "assert package.resolve_social_account_catalog_review_queue_item is before_resolve",
            "bridge = sys.modules[module_name]",
            "runtime = importlib.import_module('trr_backend.socials.control_plane.dispatch_runtime')",
            "assert runtime.legacy is legacy",
            "assert bridge._core is legacy",
            "assert set(bridge.__all__) == expected",
            "assert bridge.get_social_account_catalog_review_queue is before_get",
            "assert bridge.resolve_social_account_catalog_review_queue_item is before_resolve",
        ]
    )


def test_review_queue_preserves_patch_after_import_copy_semantics() -> None:
    _run_fresh_python(
        [
            "import importlib",
            "legacy = importlib.import_module('trr_backend.socials.social_season_analytics_impl')",
            "before_get = object()",
            "before_resolve = object()",
            "after_get = object()",
            "after_resolve = object()",
            "legacy.get_social_account_catalog_review_queue = before_get",
            "legacy.resolve_social_account_catalog_review_queue_item = before_resolve",
            "bridge = importlib.import_module('trr_backend.socials.pipelines.account_catalog.review_queue')",
            "legacy.get_social_account_catalog_review_queue = after_get",
            "legacy.resolve_social_account_catalog_review_queue_item = after_resolve",
            "assert bridge.get_social_account_catalog_review_queue is before_get",
            "assert bridge.resolve_social_account_catalog_review_queue_item is before_resolve",
        ]
    )
