"""Unit contract for late social-provider publication primitives."""

from __future__ import annotations

import sys
from types import ModuleType
from typing import Any, cast

import pytest

from trr_backend.socials.provider_registry import (
    LateModuleProvider,
    LateNamespaceProvider,
    LateProviderProxy,
    LegacyPatchBridge,
)


def test_namespace_provider_is_fail_closed_identity_bound_and_patch_aware() -> None:
    owner: dict[str, Any] = {}
    rooms = {"room"}
    imported: set[str] = set()
    wrappers: dict[str, Any] = {}
    provider = LateNamespaceProvider(
        owner,
        prefix="TEST_PROVIDER",
        room_names=rooms,
        imported_names=imported,
        room_wrappers=wrappers,
    )
    proxy = LateProviderProxy(provider)

    assert owner["_PROVIDER_STATE"] == "UNCONFIGURED"
    with pytest.raises(RuntimeError, match="TEST_PROVIDER_UNCONFIGURED"):
        _ = proxy.ordinary

    ordinary = object()
    baseline_room = lambda: "baseline"  # noqa: E731
    namespace = {"ordinary": ordinary, "room": baseline_room}
    provider.configure(namespace)

    assert owner["_PROVIDER_STATE"] == "READY"
    assert owner["_PROVIDER_NAMESPACE"] is namespace
    assert owner["ordinary"] is ordinary
    assert proxy.ordinary is ordinary
    assert wrappers["room"] is baseline_room
    assert provider.room_callable("room", baseline_room) is baseline_room

    replacement = lambda: "replacement"  # noqa: E731
    namespace["room"] = replacement
    assert provider.room_callable("room", baseline_room) is replacement
    patched = object()
    namespace["ordinary"] = patched
    provider.sync()
    assert owner["ordinary"] is patched

    provider.configure(namespace)
    with pytest.raises(RuntimeError, match="TEST_PROVIDER_MISMATCH"):
        provider.configure(dict(namespace))


def test_namespace_provider_restores_partial_publication_before_retry() -> None:
    owner: dict[str, Any] = {}
    published: list[str] = []

    class PublicationFailure(BaseException):
        pass

    def publish(name: str, value: Any) -> None:
        owner[name] = value
        published.append(name)
        if len(published) == 2:
            raise PublicationFailure("deterministic provider failure")

    provider = LateNamespaceProvider(owner, prefix="ROLLBACK_PROVIDER", publisher=publish)
    namespace = {"first": object(), "second": object()}

    with pytest.raises(PublicationFailure, match="deterministic provider failure"):
        provider.configure(namespace)

    assert owner["_PROVIDER_STATE"] == "UNCONFIGURED"
    assert owner["_PROVIDER_NAMESPACE"] is None
    assert "first" not in owner
    assert "second" not in owner

    provider = LateNamespaceProvider(owner, prefix="ROLLBACK_PROVIDER")
    provider.configure(namespace)
    assert owner["_PROVIDER_STATE"] == "READY"
    assert owner["first"] is namespace["first"]
    assert owner["second"] is namespace["second"]


def test_fixed_bindings_and_module_identity_are_validated() -> None:
    owner: dict[str, Any] = {}
    bindings = LateNamespaceProvider(
        owner,
        prefix="BINDINGS_PROVIDER",
        bindings={"local_name": "provider_name"},
    )
    with pytest.raises(RuntimeError, match="missing compatibility bindings: provider_name"):
        bindings.configure({})
    namespace = {"provider_name": object()}
    bindings.configure(namespace)
    assert owner["local_name"] is namespace["provider_name"]

    module_name = "_test_social_provider_registry_module"
    module = ModuleType(module_name)
    sys.modules[module_name] = module
    try:
        cast(Any, module).marker = object()
        module_provider = LateModuleProvider(owner, prefix="MODULE_PROVIDER")
        with pytest.raises(RuntimeError, match="loaded module namespace"):
            module_provider.configure({})
        module_provider.configure(module.__dict__)
        assert module_provider.require_module() is module
        assert owner["_PROVIDER_MODULE"] is module
    finally:
        sys.modules.pop(module_name, None)


def test_module_publication_callbacks_are_deferred_identity_bound_and_idempotent() -> None:
    owner: dict[str, Any] = {}
    module_name = "_test_social_provider_callbacks_module"
    module = ModuleType(module_name)
    sys.modules[module_name] = module
    try:
        provider = LateModuleProvider(owner, prefix="CALLBACK_PROVIDER")
        observed: list[ModuleType] = []

        def callback(published: ModuleType) -> None:
            observed.append(published)

        provider.register_module_publication_callback(callback)
        provider.register_module_publication_callback(callback)
        assert observed == []

        provider.configure(module.__dict__)
        assert observed == [module]
        assert provider.require_module() is module
        provider.register_module_publication_callback(callback)
        assert observed == [module]

        immediate: list[ModuleType] = []
        provider.register_module_publication_callback(immediate.append)
        assert immediate == [module]
    finally:
        sys.modules.pop(module_name, None)


def test_failed_publication_callback_leaves_provider_ready() -> None:
    owner: dict[str, Any] = {}
    module_name = "_test_social_provider_failed_callback_module"
    module = ModuleType(module_name)
    sys.modules[module_name] = module
    try:
        provider = LateModuleProvider(owner, prefix="FAILED_CALLBACK_PROVIDER")

        def fail(_published: ModuleType) -> None:
            raise ValueError("intentional callback failure")

        provider.register_module_publication_callback(fail)
        with pytest.raises(ValueError, match="intentional callback failure"):
            provider.configure(module.__dict__)

        assert owner["_PROVIDER_STATE"] == "READY"
        assert provider.require_module() is module
        observed: list[ModuleType] = []
        provider.register_module_publication_callback(observed.append)
        assert observed == [module]
    finally:
        sys.modules.pop(module_name, None)


def test_legacy_patch_bridge_preserves_local_callable_and_refreshes_cached_alias() -> None:
    bridge = LegacyPatchBridge()
    leaf: dict[str, Any] = {}
    cached: dict[str, Any] = {}

    def local_value() -> str:
        return "local"

    def legacy_value() -> str:
        return "legacy"

    leaf["read_value"] = local_value
    cached["read_value"] = local_value
    bridge.register_namespace(leaf, ("read_value",))
    bridge.register_aliases(cached, ("read_value",), leaf.__getitem__)
    namespace = {"read_value": legacy_value}
    bridge.publish(namespace)

    assert leaf["read_value"]() == "local"
    assert cached["read_value"] is leaf["read_value"]

    namespace["read_value"] = lambda: "patched"
    assert leaf["read_value"]() == "patched"
