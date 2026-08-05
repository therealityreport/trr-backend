# ruff: noqa: F822
"""Runtime and auth helpers for the social control plane.

This module is the canonical control-plane runtime import surface. Non-Instagram
runtime helpers temporarily bridge to `social_season_analytics_impl` until their
platform-specific modules own the full implementation.
"""

from __future__ import annotations

from typing import Any

from trr_backend.socials.control_plane.dispatch_runtime import legacy as _core
from trr_backend.socials.instagram.auth_runtime import _load_instagram_cookies
from trr_backend.socials.provider_registry import LateNamespaceProvider, publish_module_slot

_PROVIDER_EXPORT_NAMES = (
    "SocialIngestConflictError",
    "SocialIngestValidationError",
    "SocialWorkerUnavailableError",
)
def _publish_provider_binding(name: str, value: Any) -> None:
    globals()[name] = value


_PROVIDER = LateNamespaceProvider(
    globals(),
    prefix="SOCIAL_RUNTIME_PROVIDER",
    bindings={name: name for name in _PROVIDER_EXPORT_NAMES},
    publisher=lambda name, value: _publish_provider_binding(name, value),
    commit=publish_module_slot(globals(), "_core"),
    unconfigured_message="SOCIAL_RUNTIME_PROVIDER_UNCONFIGURED: provider publication has not completed",
    missing_bindings_message="SOCIAL_RUNTIME_PROVIDER_INVALID: missing runtime bindings: ",
)


def _require_provider_ready() -> dict[str, Any]:
    return _PROVIDER.require()  # type: ignore[return-value]


def _configure_legacy_provider(provider: dict[str, Any]) -> None:
    """Publish runtime exception identities after the provider finishes loading."""

    _PROVIDER.configure(provider)


def __getattr__(name: str) -> Any:
    if name in _PROVIDER_EXPORT_NAMES:
        _require_provider_ready()
        return globals()[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def _load_facebook_cookies() -> dict[str, str]:
    return _core._load_facebook_cookies()


def _load_threads_cookies() -> dict[str, str]:
    return _core._load_threads_cookies()


def _load_tiktok_cookies() -> dict[str, str]:
    return _core._load_tiktok_cookies()


def _load_twikit_credentials() -> Any:
    return _core._load_twikit_credentials()


def _load_twitter_auth() -> Any:
    return _core._load_twitter_auth()


def _adapt_payload_json_values(value: Any) -> Any:
    return _core._adapt_payload_json_values(value)


def _pg_upsert_many(*args: Any, **kwargs: Any) -> Any:
    return _core._pg_upsert_many(*args, **kwargs)


def _resolve_runtime_version_stamp() -> str:
    return _core._resolve_runtime_version_stamp()


def check_platform_cookie_health(*args: Any, **kwargs: Any) -> Any:
    return _core.check_platform_cookie_health(*args, **kwargs)


def refresh_platform_cookies_interactive(*args: Any, **kwargs: Any) -> Any:
    return _core.refresh_platform_cookies_interactive(*args, **kwargs)


load_facebook_cookies = _load_facebook_cookies
load_instagram_cookies = _load_instagram_cookies
load_threads_cookies = _load_threads_cookies
load_tiktok_cookies = _load_tiktok_cookies
load_twikit_credentials = _load_twikit_credentials
load_twitter_auth = _load_twitter_auth
adapt_payload_json_values = _adapt_payload_json_values
pg_upsert_many = _pg_upsert_many

__all__ = [
    "SocialIngestConflictError",
    "SocialIngestValidationError",
    "SocialWorkerUnavailableError",
    "_adapt_payload_json_values",
    "_load_facebook_cookies",
    "_load_instagram_cookies",
    "_load_threads_cookies",
    "_load_tiktok_cookies",
    "_load_twikit_credentials",
    "_load_twitter_auth",
    "_pg_upsert_many",
    "_resolve_runtime_version_stamp",
    "adapt_payload_json_values",
    "check_platform_cookie_health",
    "load_facebook_cookies",
    "load_instagram_cookies",
    "load_threads_cookies",
    "load_tiktok_cookies",
    "load_twikit_credentials",
    "load_twitter_auth",
    "pg_upsert_many",
    "refresh_platform_cookies_interactive",
]
