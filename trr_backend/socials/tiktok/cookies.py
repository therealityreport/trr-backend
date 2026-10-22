# ruff: noqa: F821, UP037
"""TikTok cookie resolution, validation, refresh, and the canonical cookie-loader adapter.

Moved out of ``social_season_analytics_impl``. The legacy repository keeps
same-named wrappers that delegate through ``_LOCAL_ROOM_FUNCTIONS`` and still
owns the shared cookie state (``_tiktok_cookie_validation_cache``,
``_tiktok_cookie_runtime_override``, ``_tiktok_cookie_refresh_lock``). The rooms
below read and write that state through the published provider namespace so the
repository's by-name state helpers and existing test fixtures keep working.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from trr_backend.socials.control_plane.run_lifecycle import _legacy_module as _published_legacy_module
from trr_backend.socials.provider_registry import (
    LateNamespaceProvider,
    LateProviderProxy,
    adopt_published,
    publish_module_slot,
)

if TYPE_CHECKING:
    import logging
    from collections.abc import Sequence
    from typing import Protocol

    SOCIAL_INSTAGRAM_COOKIE_REFRESH_TIMEOUT_SECONDS_DEFAULT: int
    logger: logging.Logger

    class _CookieRefreshCallable(Protocol):
        def __call__(
            self,
            *,
            username: str,
            password: str,
            cookie_file: str,
            headless: bool,
            timeout_seconds: int,
        ) -> dict[str, str]: ...

    def _default_platform_cookie_file_path(platform: str) -> Path: ...

    def _platform_cookie_refresh_target_path(default_path: Path, *env_keys: str) -> Path: ...

    def _sync_cookie_json_env_source(
        *,
        cookies: Mapping[str, Any],
        env_keys: Sequence[str],
        default_env_key: str,
        source_env: Path | None = None,
    ) -> dict[str, Any]: ...

    def _load_cookie_map_from_json_or_file(
        *,
        label: str,
        raw_json_env_keys: tuple[str, ...],
        file_env_keys: tuple[str, ...],
        default_path: Path,
        required_cookie_names_any: tuple[str, ...] = (),
        required_cookie_names_all: tuple[str, ...] = (),
    ) -> dict[str, str]: ...

    def _validate_browser_cookie_health_cached(
        *,
        platform_label: str,
        cookies: dict[str, str],
        cache_entry: tuple[float, str, bool, str | None] | None,
        validation_url: str,
        cookie_domains: tuple[str, ...],
        required_cookie_names_any: tuple[str, ...] = (),
        required_cookie_names_all: tuple[str, ...] = (),
        invalid_url_markers: tuple[str, ...] = (),
        invalid_body_patterns: tuple[str, ...] = (),
    ) -> tuple[tuple[float, str, bool, str | None], bool, str | None]: ...

    def _platform_auth_credentials(username_env: str, password_env: str) -> tuple[str | None, str | None]: ...

    def _platform_cookie_auto_refresh_enabled(toggle_env: str, username_env: str, password_env: str) -> bool: ...

    def _resolve_positive_int_env(name: str, default: int, *, minimum: int = 1) -> int: ...


# Room names are declared before the provider is built so that a late
# ``adopt_published`` (leaf imported after the repository published) still
# captures the repository wrappers for ``_room_callable``.
_LOCAL_ROOM_NAMES: set[str] = {
    "_sync_tiktok_cookie_json_env_source",
    "_default_tiktok_cookie_file_path",
    "_load_tiktok_cookies_from_sources",
    "_validate_tiktok_cookie_health",
    "_store_refreshed_tiktok_cookies",
    "_refresh_tiktok_cookies",
    "_ensure_tiktok_cookies_fresh",
    "_load_tiktok_cookies",
    "_load_tiktok_cookies_adapter",
    "_do_interactive_refresh_tiktok",
}
_IMPORTED_CORE_NAMES: set[str] = set()
_LOCAL_ROOM_FUNCTIONS: dict[str, Any] = {}
_CORE_ROOM_WRAPPERS: dict[str, Any] = {}


_PROVIDER = LateNamespaceProvider(
    globals(),
    prefix="TIKTOK_COOKIES_PROVIDER",
    room_names=_LOCAL_ROOM_NAMES,
    imported_names=_IMPORTED_CORE_NAMES,
    room_wrappers=_CORE_ROOM_WRAPPERS,
    commit=publish_module_slot(globals(), "_core"),
)
_core: Any = LateProviderProxy(_PROVIDER)
_require_provider_ready = _PROVIDER.require
_configure_legacy_provider = _PROVIDER.configure
_sync_core_overrides = _PROVIDER.sync
_room_callable = _PROVIDER.room_callable
adopt_published(_PROVIDER, _published_legacy_module)
del _published_legacy_module


def _sync_tiktok_cookie_json_env_source(cookies: Mapping[str, Any]) -> dict[str, Any]:
    _sync_core_overrides()
    return _sync_cookie_json_env_source(
        cookies=cookies,
        env_keys=("SOCIAL_TIKTOK_COOKIES_JSON", "TIKTOK_COOKIES_JSON"),
        default_env_key="SOCIAL_TIKTOK_COOKIES_JSON",
    )


def _default_tiktok_cookie_file_path() -> Path:
    _sync_core_overrides()
    return _default_platform_cookie_file_path("tiktok")


def _load_tiktok_cookies_from_sources() -> dict[str, str]:
    """
    Resolve TikTok cookies for season ingest.

    Resolution:
    1) SOCIAL_TIKTOK_COOKIES_JSON / TIKTOK_COOKIES_JSON (inline JSON)
    2) SOCIAL_TIKTOK_COOKIES_FILE / TIKTOK_COOKIES_FILE (path to JSON file)
    """
    _sync_core_overrides()
    default_cookie_file_path = _room_callable("_default_tiktok_cookie_file_path", _default_tiktok_cookie_file_path)
    return _load_cookie_map_from_json_or_file(
        label="TikTok",
        raw_json_env_keys=("SOCIAL_TIKTOK_COOKIES_JSON", "TIKTOK_COOKIES_JSON"),
        file_env_keys=("SOCIAL_TIKTOK_COOKIES_FILE", "TIKTOK_COOKIES_FILE"),
        default_path=default_cookie_file_path(),
        required_cookie_names_any=("sessionid", "sessionid_ss", "sid_tt"),
    )


def _validate_tiktok_cookie_health(cookies: dict[str, str]) -> tuple[bool, str | None]:
    _sync_core_overrides()
    cache_entry, is_valid, reason = _validate_browser_cookie_health_cached(
        platform_label="tiktok",
        cookies=cookies,
        cache_entry=_core._tiktok_cookie_validation_cache,
        validation_url="https://www.tiktok.com/messages?lang=en",
        cookie_domains=(".tiktok.com", "www.tiktok.com"),
        required_cookie_names_any=("sessionid", "sessionid_ss", "sid_tt"),
        invalid_url_markers=("/login",),
        invalid_body_patterns=(r"\bLog in\b", r"\bSign up\b", r"Maximum number of attempts reached"),
    )
    _core._tiktok_cookie_validation_cache = cache_entry
    return is_valid, reason


def _store_refreshed_tiktok_cookies(cookies: Mapping[str, Any]) -> dict[str, Any]:
    refreshed = {str(key): str(value) for key, value in cookies.items() if str(key).strip() and value is not None}
    if not refreshed:
        return {"synced": False, "reason": "empty_cookie_payload"}
    _core._tiktok_cookie_runtime_override = dict(refreshed)
    _core._tiktok_cookie_validation_cache = None
    return _room_callable("_sync_tiktok_cookie_json_env_source", _sync_tiktok_cookie_json_env_source)(refreshed)


def _refresh_tiktok_cookies(stale_reason: str | None = None) -> dict[str, str]:
    _sync_core_overrides()
    username, password = _platform_auth_credentials("SOCIAL_AUTH_TIKTOK_USERNAME", "SOCIAL_AUTH_TIKTOK_PASSWORD")
    if not username or not password:
        return {}
    from trr_backend.socials.tiktok.cookie_refresh import refresh_tiktok_cookies

    default_cookie_file_path = _room_callable("_default_tiktok_cookie_file_path", _default_tiktok_cookie_file_path)
    store_refreshed_cookies = _room_callable("_store_refreshed_tiktok_cookies", _store_refreshed_tiktok_cookies)
    try:
        refreshed = cast(_CookieRefreshCallable, refresh_tiktok_cookies)(
            username=username,
            password=password,
            cookie_file=str(
                _platform_cookie_refresh_target_path(
                    default_cookie_file_path(),
                    "SOCIAL_TIKTOK_COOKIES_FILE",
                    "TIKTOK_COOKIES_FILE",
                )
            ),
            headless=(os.getenv("SOCIAL_TIKTOK_COOKIE_REFRESH_HEADLESS") or "true").strip().lower()
            not in {"0", "false", "off", "no"},
            timeout_seconds=_resolve_positive_int_env(
                "SOCIAL_TIKTOK_COOKIE_REFRESH_TIMEOUT_SECONDS",
                SOCIAL_INSTAGRAM_COOKIE_REFRESH_TIMEOUT_SECONDS_DEFAULT,
                minimum=30,
            ),
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("TikTok cookie refresh failed%s: %s", f" ({stale_reason})" if stale_reason else "", exc)
        return {}
    if refreshed:
        sync_result = store_refreshed_cookies(refreshed)
        if sync_result.get("error"):
            logger.warning("TikTok cookie refresh could not sync env JSON source: %s", sync_result.get("error"))
    return refreshed


def _ensure_tiktok_cookies_fresh(cookies: dict[str, str]) -> dict[str, str]:
    _sync_core_overrides()
    runtime_override = _core._tiktok_cookie_runtime_override
    if runtime_override:
        return dict(runtime_override)
    if not _platform_cookie_auto_refresh_enabled(
        "SOCIAL_TIKTOK_COOKIE_AUTO_REFRESH",
        "SOCIAL_AUTH_TIKTOK_USERNAME",
        "SOCIAL_AUTH_TIKTOK_PASSWORD",
    ):
        return cookies
    validate_cookie_health = _room_callable("_validate_tiktok_cookie_health", _validate_tiktok_cookie_health)
    is_valid, reason = validate_cookie_health(cookies)
    if is_valid:
        return cookies
    with _core._tiktok_cookie_refresh_lock:
        latest = (
            _core._tiktok_cookie_runtime_override
            or _room_callable("_load_tiktok_cookies_from_sources", _load_tiktok_cookies_from_sources)()
        )
        latest_valid, _ = validate_cookie_health(dict(latest))
        if latest_valid:
            _core._tiktok_cookie_runtime_override = dict(latest)
            return dict(latest)
        refreshed = _room_callable("_refresh_tiktok_cookies", _refresh_tiktok_cookies)(reason)
        if refreshed:
            refreshed_valid, refreshed_reason = validate_cookie_health(refreshed)
            if refreshed_valid:
                return refreshed
            logger.warning("TikTok cookies refreshed but validation still failed (%s)", refreshed_reason or "unknown")
        return cookies


def _load_tiktok_cookies() -> dict[str, str]:
    cookies = _room_callable("_load_tiktok_cookies_from_sources", _load_tiktok_cookies_from_sources)()
    return _room_callable("_ensure_tiktok_cookies_fresh", _ensure_tiktok_cookies_fresh)(cookies)


def _load_tiktok_cookies_adapter() -> dict[str, str]:
    return _room_callable("_load_tiktok_cookies", _load_tiktok_cookies)()


def _do_interactive_refresh_tiktok(
    username: str,
    password: str,
    headless: bool,
    timeout_seconds: int,
) -> dict[str, str]:
    _sync_core_overrides()
    from trr_backend.socials.tiktok.cookie_refresh import refresh_tiktok_cookies

    default_cookie_file_path = _room_callable("_default_tiktok_cookie_file_path", _default_tiktok_cookie_file_path)
    refreshed = cast(_CookieRefreshCallable, refresh_tiktok_cookies)(
        username=username,
        password=password,
        cookie_file=str(
            _platform_cookie_refresh_target_path(
                default_cookie_file_path(),
                "SOCIAL_TIKTOK_COOKIES_FILE",
                "TIKTOK_COOKIES_FILE",
            )
        ),
        headless=headless,
        timeout_seconds=timeout_seconds,
    )
    if refreshed:
        sync_result = _room_callable("_store_refreshed_tiktok_cookies", _store_refreshed_tiktok_cookies)(refreshed)
        if sync_result.get("error"):
            logger.warning("TikTok cookie refresh could not sync env JSON source: %s", sync_result.get("error"))
    return refreshed


_LOCAL_ROOM_FUNCTIONS.update({_name: globals()[_name] for _name in _LOCAL_ROOM_NAMES})
__all__ = [
    "_default_tiktok_cookie_file_path",
    "_do_interactive_refresh_tiktok",
    "_ensure_tiktok_cookies_fresh",
    "_load_tiktok_cookies",
    "_load_tiktok_cookies_adapter",
    "_load_tiktok_cookies_from_sources",
    "_refresh_tiktok_cookies",
    "_store_refreshed_tiktok_cookies",
    "_sync_tiktok_cookie_json_env_source",
    "_validate_tiktok_cookie_health",
]
