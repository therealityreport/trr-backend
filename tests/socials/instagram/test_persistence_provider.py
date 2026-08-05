from __future__ import annotations

import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from trr_backend.socials.instagram import persistence


@pytest.fixture(autouse=True)
def _reset_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(persistence, "_LEGACY_NAMESPACE", None)


def test_persistence_import_does_not_load_legacy_or_catalog_modules() -> None:
    backend_root = Path(__file__).resolve().parents[3]
    code = "\n".join(
        [
            "import importlib",
            "import sys",
            "before = set(sys.modules)",
            "leaf = importlib.import_module('trr_backend.socials.instagram.persistence')",
            "assert callable(leaf._upsert_instagram_comment_tree)",
            "assert leaf._LEGACY_NAMESPACE is None",
            "loaded = set(sys.modules) - before",
            "forbidden = {",
            "    'trr_backend.repositories.social_season_analytics',",
            "    'trr_backend.socials.social_season_analytics_impl',",
            "    'trr_backend.socials.instagram.catalog_ingest',",
            "}",
            "assert not (loaded & forbidden), sorted(loaded & forbidden)",
        ]
    )

    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=backend_root,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr


def test_provider_can_be_configured_after_leaf_import() -> None:
    namespace = {"provider": object()}

    persistence._configure_legacy_provider(namespace)

    assert persistence._LEGACY_NAMESPACE is namespace
    assert persistence._core is persistence._LEGACY_PROVIDER
    assert persistence._core.provider is namespace["provider"]


def test_provider_configuration_is_same_namespace_idempotent() -> None:
    namespace: dict[str, Any] = {}

    persistence._configure_legacy_provider(namespace)
    persistence._configure_legacy_provider(namespace)

    assert persistence._LEGACY_NAMESPACE is namespace


def test_provider_configuration_rejects_different_namespace() -> None:
    persistence._configure_legacy_provider({})

    with pytest.raises(RuntimeError, match="Instagram persistence provider is already configured"):
        persistence._configure_legacy_provider({})


def test_unconfigured_provider_read_fails_deterministically() -> None:
    with pytest.raises(
        RuntimeError,
        match="Instagram persistence provider is not configured: _now_utc",
    ):
        _ = persistence._core._now_utc


def test_unconfigured_provider_write_fails_deterministically() -> None:
    with pytest.raises(
        RuntimeError,
        match="Instagram persistence provider is not configured: _probe_cache",
    ):
        persistence._core._probe_cache = True


def test_configured_provider_missing_name_raises_attribute_error() -> None:
    persistence._configure_legacy_provider({})

    with pytest.raises(
        AttributeError,
        match="Instagram persistence provider has no attribute: _now_utc",
    ):
        _ = persistence._core._now_utc


def test_provider_reads_exact_live_namespace_values() -> None:
    first = object()
    second = object()
    namespace = {"provider": first}
    persistence._configure_legacy_provider(namespace)

    assert persistence._core.provider is first

    namespace["provider"] = second
    namespace["late_provider"] = first

    assert persistence._core.provider is second
    assert persistence._core.late_provider is first


def test_comment_cache_writes_through_to_exact_live_namespace() -> None:
    namespace: dict[str, Any] = {}
    persistence._configure_legacy_provider(namespace)

    assert persistence._instagram_comment_cache_attr("_probe_cache") is None
    assert persistence._set_instagram_comment_cache_attr("_probe_cache", True) is True
    assert namespace["_probe_cache"] is True
    assert "_probe_cache" not in vars(persistence._LEGACY_PROVIDER)


def test_catalog_delegates_resolve_module_and_attributes_at_call_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []
    first_result = object()
    second_result = object()
    shared_result = object()
    batch_result = object()

    def first_upsert(*args: Any, **kwargs: Any) -> object:
        calls.append(("first_upsert", args, kwargs))
        return first_result

    catalog = SimpleNamespace(
        _upsert_instagram_post=first_upsert,
        _shared_catalog_instagram_post_payload=lambda *args, **kwargs: (
            calls.append(("shared_payload", args, kwargs)),
            shared_result,
        )[1],
        _batch_upsert_shared_catalog_instagram_posts=lambda *args, **kwargs: (
            calls.append(("batch_upsert", args, kwargs)),
            batch_result,
        )[1],
    )
    monkeypatch.setattr(persistence, "_catalog_ingest_module", lambda: catalog)

    assert persistence._upsert_instagram_post("post", conn="conn") is first_result
    assert persistence._shared_catalog_instagram_post_payload("post") is shared_result
    assert persistence._batch_upsert_shared_catalog_instagram_posts(["post"]) is batch_result

    def second_upsert(*args: Any, **kwargs: Any) -> object:
        calls.append(("second_upsert", args, kwargs))
        return second_result

    catalog._upsert_instagram_post = second_upsert

    assert persistence._upsert_instagram_post("later", conn="late-conn") is second_result
    assert calls == [
        ("first_upsert", ("post",), {"conn": "conn"}),
        ("shared_payload", ("post",), {}),
        ("batch_upsert", (["post"],), {}),
        ("second_upsert", ("later",), {"conn": "late-conn"}),
    ]
