"""Compatibility contract for the account-catalog progress provider boundary."""

from __future__ import annotations

import ast
import importlib
import os
import subprocess
import sys
from pathlib import Path

import pytest

BACKEND_ROOT = Path(__file__).resolve().parents[2]
PROGRESS_PATH = BACKEND_ROOT / "trr_backend" / "socials" / "pipelines" / "account_catalog" / "progress.py"
PROGRESS_MODULE = "trr_backend.socials.pipelines.account_catalog.progress"
LEGACY_IMPL_MODULE = "trr_backend.socials.social_season_analytics_impl"


def _run_fresh_python(lines: list[str]) -> None:
    environment = dict(os.environ)
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    result = subprocess.run(
        [sys.executable, "-B", "-c", "\n".join(lines)],
        cwd=BACKEND_ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_progress_source_has_no_legacy_import() -> None:
    tree = ast.parse(PROGRESS_PATH.read_text(encoding="utf-8"))
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
        (PROGRESS_MODULE, LEGACY_IMPL_MODULE),
        (LEGACY_IMPL_MODULE, PROGRESS_MODULE),
    ],
)
def test_fresh_import_orders_publish_exact_provider(order: tuple[str, str]) -> None:
    _run_fresh_python(
        [
            "import importlib",
            f"for name in {order!r}:",
            "    importlib.import_module(name)",
            f"room = importlib.import_module({PROGRESS_MODULE!r})",
            f"legacy = importlib.import_module({LEGACY_IMPL_MODULE!r})",
            "assert room._PROVIDER_STATE == 'READY'",
            "assert room._PROVIDER_NAMESPACE is legacy.__dict__",
            "room_name = 'get_social_account_catalog_run_progress'",
            "assert room._CORE_ROOM_WRAPPERS[room_name] is legacy.__dict__[room_name]",
            "assert room.__dict__[room_name] is not legacy.__dict__[room_name]",
            "assert room._core._relation_exists is legacy._relation_exists",
        ]
    )


def test_provider_is_idempotent_mismatch_rejecting_and_refreshes_monolith_patches() -> None:
    room = importlib.import_module(PROGRESS_MODULE)
    legacy = importlib.import_module(LEGACY_IMPL_MODULE)

    assert room._PROVIDER_STATE == "READY"
    assert room._PROVIDER_NAMESPACE is legacy.__dict__
    room._configure_legacy_provider(legacy.__dict__)
    with pytest.raises(RuntimeError, match="PROVIDER_MISMATCH"):
        room._configure_legacy_provider({})

    original = legacy._relation_exists
    replacement = lambda *_args, **_kwargs: True  # noqa: E731
    legacy._relation_exists = replacement
    try:
        room._sync_core_overrides()
        assert room._relation_exists is replacement
        assert room._core._relation_exists is replacement
    finally:
        legacy._relation_exists = original
        room._sync_core_overrides()


def test_provider_rolls_back_failed_publication_and_fails_closed_before_retry() -> None:
    _run_fresh_python(
        [
            "import importlib",
            f"room = importlib.import_module({PROGRESS_MODULE!r})",
            "assert room._PROVIDER_STATE == 'UNCONFIGURED'",
            "try:",
            "    room._sync_core_overrides()",
            "except RuntimeError as error:",
            "    assert 'PROVIDER_UNCONFIGURED' in str(error)",
            "else:",
            "    raise AssertionError('unconfigured progress provider did not fail closed')",
            "provider = {",
            "    'ordinary_a': object(),",
            "    'ordinary_b': object(),",
            "    'get_social_account_catalog_run_progress': lambda *args, **kwargs: None,",
            "}",
            "original_publish = room._publish_provider_binding",
            "published = []",
            "class PublicationFailure(BaseException):",
            "    pass",
            "def fail_second(name, value):",
            "    published.append(name)",
            "    original_publish(name, value)",
            "    if len(published) == 2:",
            "        raise PublicationFailure('deterministic staged failure')",
            "room._publish_provider_binding = fail_second",
            "try:",
            "    room._configure_legacy_provider(provider)",
            "except PublicationFailure as error:",
            "    assert str(error) == 'deterministic staged failure'",
            "else:",
            "    raise AssertionError('partial provider publication did not fail')",
            "finally:",
            "    room._publish_provider_binding = original_publish",
            "assert room._PROVIDER_STATE == 'UNCONFIGURED'",
            "assert room._PROVIDER_NAMESPACE is None",
            "assert room._IMPORTED_CORE_NAMES == set()",
            "assert room._CORE_ROOM_WRAPPERS == {}",
            "assert 'ordinary_a' not in room.__dict__",
            "assert 'ordinary_b' not in room.__dict__",
            "room._configure_legacy_provider(provider)",
            "assert room._PROVIDER_STATE == 'READY'",
            "assert room._PROVIDER_NAMESPACE is provider",
            "assert room.ordinary_a is provider['ordinary_a']",
            "assert room.ordinary_b is provider['ordinary_b']",
            "room_name = 'get_social_account_catalog_run_progress'",
            "assert room._CORE_ROOM_WRAPPERS[room_name] is provider[room_name]",
            "different = dict(provider)",
            "try:",
            "    room._configure_legacy_provider(different)",
            "except RuntimeError as error:",
            "    assert 'PROVIDER_MISMATCH' in str(error)",
            "else:",
            "    raise AssertionError('different provider identity did not fail')",
        ]
    )
