"""Late publication primitives for extracted social provider leaves.

The social analytics monolith publishes its completed namespace only after all
of the extracted leaves have finished importing.  These helpers deliberately
hold no reference to that monolith: a leaf supplies the namespace at its tail,
and callers fail closed until then.
"""

from __future__ import annotations

import sys
from collections.abc import Callable, Iterable, Mapping, MutableMapping
from functools import wraps
from threading import RLock
from types import ModuleType
from typing import Any

UNCONFIGURED = "UNCONFIGURED"
CONFIGURING = "CONFIGURING"
READY = "READY"

_MISSING = object()
_DEFAULT_RESERVED_NAMES = frozenset(
    {
        "__builtins__",
        "__cached__",
        "__doc__",
        "__file__",
        "__loader__",
        "__name__",
        "__package__",
        "__spec__",
        "__getattr__",
        "_ABSENT_PROVIDER_BINDING",
        "_CORE_ROOM_WRAPPERS",
        "_IMPORTED_CORE_NAMES",
        "_LOCAL_ROOM_FUNCTIONS",
        "_LOCAL_ROOM_NAMES",
        "_LegacyModuleProxy",
        "_LegacyProviderProxy",
        "_PROVIDER",
        "_PROVIDER_BRIDGE_NAMES",
        "_PROVIDER_LOCK",
        "_PROVIDER_MODULE",
        "_PROVIDER_NAMESPACE",
        "_PROVIDER_STATE",
        "_PROVIDER_STATE_CONFIGURING",
        "_PROVIDER_STATE_READY",
        "_PROVIDER_STATE_UNCONFIGURED",
        "_RESERVED_CORE_EXPORTS",
        "_configure_legacy_provider",
        "_publish_legacy_provider",
        "_publish_provider_binding",
        "_provider_module",
        "_require_provider_ready",
        "_room_callable",
        "_sync_core_overrides",
        "_core",
        "legacy",
    }
)

Namespace = Mapping[str, Any]
Publisher = Callable[[str, Any], None]
CommitHook = Callable[[Namespace, ModuleType | None, Mapping[str, Any]], Callable[[], None] | None]
PublicationCallback = Callable[[Namespace, ModuleType | None], None]


class LegacyPatchBridge:
    """Keep extracted call surfaces observable to legacy repository patches.

    Extracted modules retain their own implementation until the published
    legacy namespace replaces the corresponding callable. This lets existing
    compatibility patches remain effective without importing the monolith from
    a leaf module.
    """

    def __init__(self) -> None:
        self._lock = RLock()
        self._namespace: Namespace | None = None
        self._baselines: dict[str, Any] = {}
        self._owners: list[tuple[MutableMapping[str, Any], set[str], dict[str, Any]]] = []
        self._aliases: list[tuple[MutableMapping[str, Any], set[str], Callable[[str], Any]]] = []

    def register_namespace(self, owner: MutableMapping[str, Any], names: Iterable[str]) -> None:
        """Register local callable exports that should observe later patches."""

        requested_names = set(names)
        with self._lock:
            registration = next((item for item in self._owners if item[0] is owner), None)
            if registration is None:
                registration = (owner, set(), {})
                self._owners.append(registration)
            _, registered_names, fallbacks = registration
            for name in requested_names:
                current = owner.get(name)
                if callable(current) and not self._is_bridge_wrapper(current):
                    fallbacks[name] = current
            registered_names.update(requested_names)
            if self._namespace is not None:
                self._install_wrappers_locked(registration)

    def register_aliases(
        self,
        owner: MutableMapping[str, Any],
        names: Iterable[str],
        resolve: Callable[[str], Any],
    ) -> None:
        """Refresh already-cached facade exports after their leaf is bridged."""

        requested_names = set(names)
        with self._lock:
            registration = next((item for item in self._aliases if item[0] is owner), None)
            if registration is None:
                registration = (owner, set(), resolve)
                self._aliases.append(registration)
            registration[1].update(requested_names)
            ready = self._namespace is not None
        if ready:
            self._refresh_aliases((registration,))

    def publish(self, namespace: Namespace) -> None:
        """Publish a completed legacy namespace and bridge registered leaves."""

        if not isinstance(namespace, Mapping):
            raise TypeError("LEGACY_PATCH_BRIDGE_INVALID: provider must be a mapping")
        with self._lock:
            self._namespace = namespace
            self._baselines = {name: value for name, value in namespace.items() if callable(value)}
            for registration in self._owners:
                self._install_wrappers_locked(registration)
            aliases = tuple(self._aliases)
        self._refresh_aliases(aliases)

    def _install_wrappers_locked(self, registration: tuple[MutableMapping[str, Any], set[str], dict[str, Any]]) -> None:
        owner, names, fallbacks = registration
        if owner is self._namespace:
            return
        for name in names:
            current = owner.get(name)
            baseline = self._baselines.get(name)
            if not callable(current) or not callable(baseline) or self._is_bridge_wrapper(current):
                continue
            fallback = fallbacks.get(name, current)
            if callable(fallback):
                owner[name] = self._bridge_callable(name, fallback)

    def _bridge_callable(self, name: str, fallback: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(fallback)
        def call(*args: Any, **kwargs: Any) -> Any:
            with self._lock:
                namespace = self._namespace
                baseline = self._baselines.get(name)
                candidate = namespace.get(name) if namespace is not None else None
            if callable(candidate) and candidate is not baseline:
                return candidate(*args, **kwargs)
            return fallback(*args, **kwargs)

        call.__trr_legacy_patch_bridge__ = self  # type: ignore[attr-defined]
        return call

    def _refresh_aliases(
        self,
        registrations: Iterable[tuple[MutableMapping[str, Any], set[str], Callable[[str], Any]]],
    ) -> None:
        for owner, names, resolve in registrations:
            for name in names:
                with self._lock:
                    if name not in owner or name not in self._baselines:
                        continue
                value = resolve(name)
                if callable(value):
                    owner[name] = value

    @staticmethod
    def _is_bridge_wrapper(value: Any) -> bool:
        return hasattr(value, "__trr_legacy_patch_bridge__")


_LEGACY_PATCH_BRIDGE = LegacyPatchBridge()


def register_legacy_patchable_namespace(owner: MutableMapping[str, Any], names: Iterable[str]) -> None:
    """Register an extracted module's local compatibility exports."""

    _LEGACY_PATCH_BRIDGE.register_namespace(owner, names)


def register_legacy_patchable_aliases(
    owner: MutableMapping[str, Any],
    names: Iterable[str],
    resolve: Callable[[str], Any],
) -> None:
    """Register cached facade exports that should follow bridged leaf values."""

    _LEGACY_PATCH_BRIDGE.register_aliases(owner, names, resolve)


def publish_legacy_patch_namespace(namespace: Namespace) -> None:
    """Publish the final legacy namespace for extracted compatibility seams."""

    _LEGACY_PATCH_BRIDGE.publish(namespace)


def provider_module(namespace: Namespace) -> ModuleType | None:
    """Return the loaded module only when it owns this exact namespace."""

    name = namespace.get("__name__")
    candidate = sys.modules.get(name) if isinstance(name, str) else None
    if isinstance(candidate, ModuleType) and candidate.__dict__ is namespace:
        return candidate
    return None


def publish_module_slot(owner: MutableMapping[str, Any], name: str) -> CommitHook:
    """Build a rollback-safe hook that promotes a loaded provider module."""

    def _commit(_namespace: Namespace, module: ModuleType | None, _wrappers: Mapping[str, Any]) -> Callable[[], None]:
        prior = owner[name]
        if module is not None:
            owner[name] = module
        return lambda: owner.__setitem__(name, prior)

    return _commit


def publish_mapping_slot(owner: MutableMapping[str, Any], name: str, key: str) -> CommitHook:
    """Build a rollback-safe hook that promotes one published room wrapper."""

    def _commit(_namespace: Namespace, _module: ModuleType | None, wrappers: Mapping[str, Any]) -> Callable[[], None]:
        prior = owner[name]
        owner[name] = wrappers[key]
        return lambda: owner.__setitem__(name, prior)

    return _commit


class LateProviderProxy:
    """Mutable attribute proxy for a namespace that has not yet been published."""

    def __init__(self, provider: LateNamespaceProvider) -> None:
        object.__setattr__(self, "_provider", provider)

    def __getattr__(self, name: str) -> Any:
        namespace = self._provider.require()
        try:
            return namespace[name]
        except KeyError as exc:
            raise AttributeError(name) from exc

    def __setattr__(self, name: str, value: Any) -> None:
        self._provider.require()[name] = value  # type: ignore[index]

    def __delattr__(self, name: str) -> None:
        namespace = self._provider.require()
        try:
            del namespace[name]  # type: ignore[misc]
        except KeyError as exc:
            raise AttributeError(name) from exc


class LateNamespaceProvider:
    """Atomically publish one namespace into an extracted module's globals.

    The owner supplies mutable containers for the two compatibility seams that
    existing leaves expose: imported bindings and original room wrappers.  The
    registry updates those same containers in place so callers retaining a
    reference continue to observe publication and rollback.
    """

    def __init__(
        self,
        owner: MutableMapping[str, Any],
        *,
        prefix: str,
        room_names: set[str] | None = None,
        imported_names: set[str] | None = None,
        room_wrappers: dict[str, Any] | None = None,
        bindings: Mapping[str, str] | None = None,
        reserved_names: Iterable[str] = (),
        bridge_names: set[str] | None = None,
        required_room_names: Iterable[str] | Callable[[], Iterable[str]] = (),
        publisher: Publisher | None = None,
        commit: CommitHook | None = None,
        copy_bindings: bool = True,
        unconfigured_message: str | None = None,
        mismatch_message: str | None = None,
        configuring_message: str | None = None,
        invalid_message: str | None = None,
        missing_bindings_message: str | None = None,
    ) -> None:
        self._owner = owner
        self._prefix = prefix
        self._room_names = room_names if room_names is not None else set()
        self.imported_names = imported_names if imported_names is not None else set()
        self.room_wrappers = room_wrappers if room_wrappers is not None else {}
        self._bindings = dict(bindings) if bindings is not None else None
        self._reserved_names = _DEFAULT_RESERVED_NAMES | frozenset(reserved_names)
        self._bridge_names = bridge_names if bridge_names is not None else set()
        self._required_room_names = required_room_names
        self._publisher = publisher or self._default_publisher
        self._commit = commit
        self._copy_bindings = copy_bindings
        self._unconfigured_message = (
            unconfigured_message or f"{prefix}_UNCONFIGURED: provider publication has not completed"
        )
        self._mismatch_message = mismatch_message or f"{prefix}_MISMATCH: provider is already configured"
        self._configuring_message = (
            configuring_message or f"{prefix}_CONFIGURING: provider publication is already in progress"
        )
        self._invalid_message = invalid_message or f"{prefix}_INVALID: provider must be a mapping"
        self._missing_bindings_message = (
            missing_bindings_message or f"{prefix}_INVALID: missing compatibility bindings: "
        )
        self._lock = RLock()
        self._state = UNCONFIGURED
        self._namespace: Namespace | None = None
        self._module: ModuleType | None = None
        self._publication_callbacks: list[PublicationCallback] = []
        self._notifying_callbacks: list[PublicationCallback] = []
        self._published_callbacks: list[PublicationCallback] = []
        self._mirror_state()

    @property
    def state(self) -> str:
        return self._state

    @property
    def namespace(self) -> Namespace | None:
        return self._namespace

    @property
    def module(self) -> ModuleType | None:
        return self._module

    def require(self) -> Namespace:
        with self._lock:
            if self._state != READY or self._namespace is None:
                raise RuntimeError(self._unconfigured_message)
            return self._namespace

    def sync(self) -> None:
        namespace = self.require()
        for name in self.imported_names - self._room_names - self._bridge_names:
            if name in namespace:
                self._owner[name] = namespace[name]

    def room_callable(self, name: str, local_impl: Any) -> Any:
        candidate = self.require().get(name)
        if callable(candidate) and candidate is not self.room_wrappers.get(name):
            return candidate
        return local_impl

    def callable_bridge(self, name: str) -> Callable[..., Any]:
        """Return a stable callable that starts failing closed until publication."""

        def _call(*args: Any, **kwargs: Any) -> Any:
            candidate = self.require().get(name)
            if not callable(candidate):
                raise RuntimeError(f"{self._prefix}_CALLABLE_MISSING: {name}")
            return candidate(*args, **kwargs)

        _call.__name__ = name
        _call.__qualname__ = name
        return _call

    def register_publication_callback(self, callback: PublicationCallback) -> None:
        """Run *callback* once after this provider publishes a ready namespace.

        Registering before publication defers the callback; registering after a
        successful publication invokes it immediately with the same exact
        namespace and module identity. A callback failure never rolls the
        provider back from ``READY``. Re-registering a successful or in-flight
        callback is a no-op.
        """

        with self._lock:
            if self._contains_callback(self._published_callbacks, callback) or self._contains_callback(
                self._notifying_callbacks, callback
            ):
                return
            if self._state != READY or self._namespace is None:
                if not self._contains_callback(self._publication_callbacks, callback):
                    self._publication_callbacks.append(callback)
                return
            namespace = self._namespace
            module = self._module
        self._notify_publication_callback(callback, namespace, module)

    def configure(self, namespace: Namespace) -> None:
        if not isinstance(namespace, Mapping):
            raise TypeError(self._invalid_message)

        staged_module = provider_module(namespace)
        with self._lock:
            if self._state == READY:
                if namespace is self._namespace:
                    return
                raise RuntimeError(self._mismatch_message)
            if self._state == CONFIGURING:
                raise RuntimeError(self._configuring_message)
            if self._namespace is not None and namespace is not self._namespace:
                raise RuntimeError(self._mismatch_message)

            staged_names = {name for name in namespace if name not in self._reserved_names}
            staged_wrappers = {name: namespace.get(name) for name in self._room_names}
            required_room_names = self._resolved_required_room_names()
            invalid_wrappers = sorted(name for name in required_room_names if not callable(staged_wrappers.get(name)))
            if invalid_wrappers:
                raise RuntimeError(
                    f"{self._prefix}_INVALID: missing callable room wrappers: " + ", ".join(invalid_wrappers)
                )
            if self._bindings is not None:
                missing_names = sorted(
                    provider_name for provider_name in set(self._bindings.values()) if provider_name not in namespace
                )
                if missing_names:
                    raise RuntimeError(self._missing_bindings_message + ", ".join(missing_names))
                staged_bindings = {
                    local_name: namespace[provider_name] for local_name, provider_name in self._bindings.items()
                }
            elif self._copy_bindings:
                staged_bindings = {
                    name: namespace[name] for name in staged_names - self._room_names - self._bridge_names
                }
            else:
                staged_bindings = {}
            prior_bindings = {name: self._owner.get(name, _MISSING) for name in staged_bindings}
            prior_imported_names = set(self.imported_names)
            prior_room_wrappers = dict(self.room_wrappers)
            prior_namespace = self._namespace
            prior_module = self._module
            prior_state = self._state

            self._state = CONFIGURING
            self._mirror_state()
            undo_commit: Callable[[], None] | None = None
            callbacks: tuple[PublicationCallback, ...] = ()
            try:
                for name in sorted(staged_bindings):
                    self._publisher(name, staged_bindings[name])
                self.imported_names.clear()
                self.imported_names.update(staged_names)
                self.room_wrappers.clear()
                self.room_wrappers.update(staged_wrappers)
                if self._commit is not None:
                    undo_commit = self._commit(namespace, staged_module, staged_wrappers)
                self._namespace = namespace
                self._module = staged_module
                self._state = READY
                self._mirror_state()
                callbacks = tuple(self._publication_callbacks)
                self._publication_callbacks.clear()
            except BaseException:
                if undo_commit is not None:
                    undo_commit()
                for name, prior_value in prior_bindings.items():
                    if prior_value is _MISSING:
                        self._owner.pop(name, None)
                    else:
                        self._owner[name] = prior_value
                self.imported_names.clear()
                self.imported_names.update(prior_imported_names)
                self.room_wrappers.clear()
                self.room_wrappers.update(prior_room_wrappers)
                self._namespace = prior_namespace
                self._module = prior_module
                self._state = prior_state
                self._mirror_state()
                raise

        for callback in callbacks:
            self._notify_publication_callback(callback, namespace, staged_module)

    def _default_publisher(self, name: str, value: Any) -> None:
        self._owner[name] = value

    @staticmethod
    def _contains_callback(callbacks: Iterable[PublicationCallback], callback: PublicationCallback) -> bool:
        return any(existing is callback for existing in callbacks)

    def _notify_publication_callback(
        self,
        callback: PublicationCallback,
        namespace: Namespace,
        module: ModuleType | None,
    ) -> None:
        with self._lock:
            if self._contains_callback(self._published_callbacks, callback) or self._contains_callback(
                self._notifying_callbacks, callback
            ):
                return
            self._notifying_callbacks.append(callback)
        try:
            callback(namespace, module)
        except BaseException:
            with self._lock:
                self._notifying_callbacks.remove(callback)
            raise
        else:
            with self._lock:
                self._notifying_callbacks.remove(callback)
                self._published_callbacks.append(callback)

    def _mirror_state(self) -> None:
        self._owner["_PROVIDER_STATE_UNCONFIGURED"] = UNCONFIGURED
        self._owner["_PROVIDER_STATE_CONFIGURING"] = CONFIGURING
        self._owner["_PROVIDER_STATE_READY"] = READY
        self._owner["_PROVIDER_STATE"] = self._state
        self._owner["_PROVIDER_NAMESPACE"] = self._namespace
        self._owner["_PROVIDER_LOCK"] = self._lock

    def _resolved_required_room_names(self) -> Iterable[str]:
        if callable(self._required_room_names):
            return self._required_room_names()
        return self._required_room_names


class LateModuleProvider(LateNamespaceProvider):
    """A late provider whose namespace must belong to a loaded module."""

    def __init__(
        self,
        owner: MutableMapping[str, Any],
        *,
        prefix: str,
        commit: CommitHook | None = None,
        unconfigured_message: str | None = None,
        mismatch_message: str | None = None,
        configuring_message: str | None = None,
        invalid_message: str | None = None,
    ) -> None:
        super().__init__(
            owner,
            prefix=prefix,
            commit=commit,
            copy_bindings=False,
            unconfigured_message=unconfigured_message,
            mismatch_message=mismatch_message,
            configuring_message=configuring_message,
            invalid_message=invalid_message,
        )
        self._module_publication_callbacks: list[tuple[Callable[[ModuleType], None], PublicationCallback]] = []

    def configure(self, namespace: Namespace) -> None:
        if not isinstance(namespace, Mapping):
            raise TypeError(self._invalid_message)
        if provider_module(namespace) is None:
            raise RuntimeError(f"{self._prefix}_INVALID: provider must be a loaded module namespace")
        super().configure(namespace)

    def require_module(self) -> ModuleType:
        namespace = self.require()
        module = self._module
        if module is None or module.__dict__ is not namespace:
            raise RuntimeError(f"{self._prefix}_INVALID: published module identity is unavailable")
        return module

    def register_module_publication_callback(self, callback: Callable[[ModuleType], None]) -> None:
        """Register a module-identity callback without exposing a namespace proxy."""

        with self._lock:
            adapter: PublicationCallback
            for existing, existing_adapter in self._module_publication_callbacks:
                if existing is callback:
                    adapter = existing_adapter
                    break
            else:

                def _adapter(_namespace: Namespace, module: ModuleType | None) -> None:
                    if module is None:
                        raise RuntimeError(f"{self._prefix}_INVALID: published module identity is unavailable")
                    callback(module)

                adapter = _adapter
                self._module_publication_callbacks.append((callback, adapter))
        self.register_publication_callback(adapter)

    def _mirror_state(self) -> None:
        super()._mirror_state()
        self._owner["_PROVIDER_MODULE"] = self._module


def adopt_published(provider: LateNamespaceProvider, loader: Callable[[], ModuleType]) -> bool:
    """Reuse a lifecycle-published module on leaf reload without importing it."""

    try:
        provider.configure(loader().__dict__)
    except RuntimeError as error:
        if "PROVIDER_UNCONFIGURED" not in str(error):
            raise
        return False
    return True
