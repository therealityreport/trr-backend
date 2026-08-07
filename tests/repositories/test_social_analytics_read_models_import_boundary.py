from __future__ import annotations

import ast
import importlib
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, cast

import pytest

import trr_backend.socials.analytics.read_models as read_models
import trr_backend.socials.social_season_analytics_impl as legacy_impl

BACKEND_ROOT = Path(__file__).resolve().parents[2]
READ_MODELS_PATH = BACKEND_ROOT / "trr_backend" / "socials" / "analytics" / "read_models.py"
READ_MODELS_MODULE = "trr_backend.socials.analytics.read_models"
LEGACY_IMPL_MODULE = "trr_backend.socials.social_season_analytics_impl"


def test_read_models_source_has_no_legacy_import() -> None:
    tree = ast.parse(READ_MODELS_PATH.read_text())
    forbidden = {
        "trr_backend.socials.social_season_analytics_impl",
        "trr_backend.repositories.social_season_analytics",
    }
    imports = [
        node
        for node in ast.walk(tree)
        if (isinstance(node, ast.ImportFrom) and node.module in forbidden)
        or (isinstance(node, ast.Import) and any(alias.name in forbidden for alias in node.names))
    ]
    assert imports == []


@pytest.mark.parametrize(
    "order",
    [
        (READ_MODELS_MODULE, LEGACY_IMPL_MODULE),
        (LEGACY_IMPL_MODULE, READ_MODELS_MODULE),
    ],
)
def test_fresh_import_orders_publish_exact_provider(order: tuple[str, str]) -> None:
    code = "\n".join(
        (
            "import importlib",
            f"for name in {order!r}:",
            "    importlib.import_module(name)",
            f"room = importlib.import_module({READ_MODELS_MODULE!r})",
            f"legacy = importlib.import_module({LEGACY_IMPL_MODULE!r})",
            "assert room._PROVIDER_STATE == 'READY'",
            "assert room._PROVIDER_NAMESPACE is legacy.__dict__",
            "assert room._CORE_ROOM_WRAPPERS['get_analytics'] is legacy.get_analytics",
            "assert room.get_analytics is not legacy.get_analytics",
        )
    )
    environment = dict(os.environ)
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    result = subprocess.run(
        [sys.executable, "-B", "-c", code],
        cwd=BACKEND_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_provider_publication_is_idempotent_and_rejects_mismatch() -> None:
    provider_view = cast(Any, read_models)
    assert provider_view._PROVIDER_STATE == "READY"
    assert provider_view._PROVIDER_NAMESPACE is legacy_impl.__dict__
    read_models._configure_legacy_provider(legacy_impl.__dict__)
    with pytest.raises(RuntimeError, match="PROVIDER_MISMATCH"):
        read_models._configure_legacy_provider({})


def test_monolith_patch_is_refreshed_before_local_execution(monkeypatch: pytest.MonkeyPatch) -> None:
    marker = object()
    monkeypatch.setitem(legacy_impl.__dict__, "_analytics_cache_key", marker)
    read_models._sync_core_overrides()
    assert read_models._analytics_cache_key is marker


def test_reload_before_provider_is_fail_closed() -> None:
    reloaded = importlib.reload(read_models)
    try:
        with pytest.raises(RuntimeError, match="PROVIDER_UNCONFIGURED"):
            reloaded._sync_core_overrides()
    finally:
        reloaded._configure_legacy_provider(legacy_impl.__dict__)
