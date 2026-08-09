"""Import-neutral registry for admin operation producers and router capabilities."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from enum import Enum, auto
from threading import RLock
from typing import Any


class AdminProvider(Enum):
    ASSET_BATCH_JOBS = auto()
    BRAVOTV_IMAGES = auto()
    PERSON_IMAGES = auto()
    PERSON_PROFILE = auto()
    SCRAPE = auto()
    SHOW_BRAVO = auto()
    SHOW_LINKS = auto()
    SHOW_NEWS = auto()
    SHOW_ROLES = auto()
    SHOW_SYNC = auto()


@dataclass(frozen=True)
class PersonImagesCapabilities:
    _get_tmdb_id: Callable[..., Any]
    RefreshImagesRequest: Callable[..., Any]
    refresh_person_images: Callable[..., Any]


@dataclass(frozen=True)
class PersonProfileCapabilities:
    RefreshProfileRequest: Callable[..., Any]
    _run_person_profile_refresh: Callable[..., Any]


@dataclass(frozen=True)
class ScrapeCapabilities:
    ImportImageItem: Callable[..., Any]
    ImportRequest: Callable[..., Any]
    import_images: Callable[..., Any]


@dataclass(frozen=True)
class ShowBravoCapabilities:
    BravoCommitRequest: Callable[..., Any]
    commit_bravo_import: Callable[..., Any]
    _build_show_cast_index: Callable[..., Any]
    _assert_show_sync_ready_for_bravo: Callable[..., Any]
    _persist_person_profile: Callable[..., Any]
    _import_bravo_person_image: Callable[..., Any]
    _extract_news_from_snapshot: Callable[..., Any]


@dataclass(frozen=True)
class ShowLinksCapabilities:
    _discover_people_links: Callable[..., Any]
    _upsert_link: Callable[..., Any]
    _discover_show_links: Callable[..., Any]
    _discover_season_links: Callable[..., Any]
    _normalize_link_kind: Callable[..., Any]
    _PERSON_SOURCE_LINK_KINDS: Any
    _validate_person_knowledge_url: Callable[..., Any]
    LinkDiscoverRequest: Callable[..., Any]
    _run_show_link_discovery: Callable[..., Any]


@dataclass(frozen=True)
class ShowNewsCapabilities:
    _run_google_news_sync_impl: Callable[..., Any]


@dataclass(frozen=True)
class ShowRolesCapabilities:
    CastMatrixSyncRequest: Callable[..., Any]
    sync_cast_matrix_for_show: Callable[..., Any]


@dataclass(frozen=True)
class ShowSyncCapabilities:
    _resolve_dimension_target: Callable[..., Any]
    build_hosted_url: Callable[..., Any]
    _upsert_dimension_logo_asset_row: Callable[..., Any]
    _set_dimension_asset_primary_flag: Callable[..., Any]
    _upsert_logo_import_audit: Callable[..., Any]
    _detect_base_logo_format: Callable[..., Any]


@dataclass(frozen=True)
class AdminOperationProducer:
    factory: Callable[..., Any]
    accepts_operation_id: bool = False


class AdminOperationRegistry:
    """A fail-fast registry whose dependencies are direct Python callables."""

    def __init__(self) -> None:
        self._lock = RLock()
        self._providers: dict[AdminProvider, object] = {}
        self._producers: dict[str, AdminOperationProducer] = {}

    def register_provider(self, provider: AdminProvider, capabilities: object) -> None:
        with self._lock:
            if provider in self._providers:
                raise RuntimeError(f"Admin provider already registered: {provider.name}")
            self._providers[provider] = capabilities

    def resolve_provider(self, provider: AdminProvider, expected_type: type[Any]) -> Any:
        with self._lock:
            capabilities = self._providers.get(provider)
        if capabilities is None:
            raise RuntimeError(f"Admin provider is not registered: {provider.name}")
        if not isinstance(capabilities, expected_type):
            raise RuntimeError(f"Admin provider has an invalid capability contract: {provider.name}")
        return capabilities

    def register_producer(self, operation_type: str, producer: AdminOperationProducer) -> None:
        with self._lock:
            if operation_type in self._producers:
                raise RuntimeError(f"Admin operation producer already registered: {operation_type}")
            self._producers[operation_type] = producer

    def build_producer(
        self,
        operation_type: str,
        request_payload: dict[str, Any],
        operation_id: str,
    ) -> Any | None:
        with self._lock:
            producer = self._producers.get(operation_type)
        if producer is None:
            return None
        kwargs: dict[str, Any] = {"request_payload": request_payload}
        if producer.accepts_operation_id:
            kwargs["operation_id"] = operation_id
        return producer.factory(**kwargs)


_registry = AdminOperationRegistry()


def late_bound_callable(namespace: Mapping[str, Any], attribute: str) -> Callable[..., Any]:
    """Keep registrations patchable without importing or looking up a peer module."""

    def invoke(*args: Any, **kwargs: Any) -> Any:
        return namespace[attribute](*args, **kwargs)

    return invoke


def register_person_images_capabilities(capabilities: PersonImagesCapabilities) -> None:
    _registry.register_provider(AdminProvider.PERSON_IMAGES, capabilities)


def get_person_images_capabilities() -> PersonImagesCapabilities:
    return _registry.resolve_provider(AdminProvider.PERSON_IMAGES, PersonImagesCapabilities)


def register_person_profile_capabilities(capabilities: PersonProfileCapabilities) -> None:
    _registry.register_provider(AdminProvider.PERSON_PROFILE, capabilities)


def get_person_profile_capabilities() -> PersonProfileCapabilities:
    return _registry.resolve_provider(AdminProvider.PERSON_PROFILE, PersonProfileCapabilities)


def register_scrape_capabilities(capabilities: ScrapeCapabilities) -> None:
    _registry.register_provider(AdminProvider.SCRAPE, capabilities)


def get_scrape_capabilities() -> ScrapeCapabilities:
    return _registry.resolve_provider(AdminProvider.SCRAPE, ScrapeCapabilities)


def register_show_bravo_capabilities(capabilities: ShowBravoCapabilities) -> None:
    _registry.register_provider(AdminProvider.SHOW_BRAVO, capabilities)


def get_show_bravo_capabilities() -> ShowBravoCapabilities:
    return _registry.resolve_provider(AdminProvider.SHOW_BRAVO, ShowBravoCapabilities)


def register_show_links_capabilities(capabilities: ShowLinksCapabilities) -> None:
    _registry.register_provider(AdminProvider.SHOW_LINKS, capabilities)


def get_show_links_capabilities() -> ShowLinksCapabilities:
    return _registry.resolve_provider(AdminProvider.SHOW_LINKS, ShowLinksCapabilities)


def register_show_news_capabilities(capabilities: ShowNewsCapabilities) -> None:
    _registry.register_provider(AdminProvider.SHOW_NEWS, capabilities)


def get_show_news_capabilities() -> ShowNewsCapabilities:
    return _registry.resolve_provider(AdminProvider.SHOW_NEWS, ShowNewsCapabilities)


def register_show_roles_capabilities(capabilities: ShowRolesCapabilities) -> None:
    _registry.register_provider(AdminProvider.SHOW_ROLES, capabilities)


def get_show_roles_capabilities() -> ShowRolesCapabilities:
    return _registry.resolve_provider(AdminProvider.SHOW_ROLES, ShowRolesCapabilities)


def register_show_sync_capabilities(capabilities: ShowSyncCapabilities) -> None:
    _registry.register_provider(AdminProvider.SHOW_SYNC, capabilities)


def get_show_sync_capabilities() -> ShowSyncCapabilities:
    return _registry.resolve_provider(AdminProvider.SHOW_SYNC, ShowSyncCapabilities)


def register_admin_operation_producer(
    operation_type: str,
    factory: Callable[..., Any],
    *,
    accepts_operation_id: bool = False,
) -> None:
    _registry.register_producer(
        operation_type,
        AdminOperationProducer(factory=factory, accepts_operation_id=accepts_operation_id),
    )


def build_registered_admin_operation_producer(
    operation_type: str,
    request_payload: dict[str, Any],
    operation_id: str,
) -> Any | None:
    return _registry.build_producer(operation_type, request_payload, operation_id)
