# ruff: noqa: F821, UP037
"""TikTok operator alerts: discovery empty-body transport failure detection, yt-dlp-related
failure classification, and the single-path topology alerts.

``_TIKTOK_SINGLE_PATH_TOPOLOGY_DEFAULT`` stays in the legacy repository and is read
through the published provider namespace.

Moved out of ``social_season_analytics_impl``; the legacy repository keeps
same-named wrappers that delegate through ``_LOCAL_ROOM_FUNCTIONS``.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from trr_backend.socials.control_plane.run_lifecycle import _legacy_module as _published_legacy_module
from trr_backend.socials.provider_registry import (
    LateNamespaceProvider,
    LateProviderProxy,
    adopt_published,
    publish_module_slot,
)

if TYPE_CHECKING:
    # Repository names resolved through the published provider namespace.
    _TIKTOK_SINGLE_PATH_TOPOLOGY_DEFAULT: Any

    def _append_alert(*args: Any, **kwargs: Any) -> Any: ...
    def _metadata_dict(*args: Any, **kwargs: Any) -> Any: ...
    def _normalize_non_negative_int(*args: Any, **kwargs: Any) -> Any: ...


# Room names are declared before the provider is built so that a late
# ``adopt_published`` (leaf imported after the repository published) still
# captures the repository wrappers for ``_room_callable``.
_LOCAL_ROOM_NAMES: set[str] = {
    "_tiktok_discovery_empty_body_transport_failure",
    "_tiktok_recent_failure_looks_ytdlp_related",
    "_build_tiktok_single_path_alerts",
}
_IMPORTED_CORE_NAMES: set[str] = set()
_LOCAL_ROOM_FUNCTIONS: dict[str, Any] = {}
_CORE_ROOM_WRAPPERS: dict[str, Any] = {}


_PROVIDER = LateNamespaceProvider(
    globals(),
    prefix="TIKTOK_ALERTS_PROVIDER",
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


def _tiktok_discovery_empty_body_transport_failure(discovery_meta: Mapping[str, Any] | None) -> bool:
    _sync_core_overrides()
    metadata = _metadata_dict(discovery_meta)
    error_code = str(metadata.get("error_code") or "").strip().lower()
    if error_code == "tiktok_discovery_sec_uid_missing":
        return _normalize_non_negative_int(metadata.get("posts_checked")) <= 0
    if error_code and error_code != "tiktok_discovery_empty_first_page":
        return False
    fetch_posts = _metadata_dict(_metadata_dict(metadata.get("endpoint_responses")).get("fetch_posts"))
    failure_reason = str(fetch_posts.get("failure_reason") or "").strip().lower()
    if failure_reason not in {"non_json_response", "empty_response"}:
        return False
    if _normalize_non_negative_int(fetch_posts.get("content_length")) > 0:
        return False
    return _normalize_non_negative_int(metadata.get("posts_checked")) <= 0


def _tiktok_recent_failure_looks_ytdlp_related(row: Mapping[str, Any]) -> bool:
    if str(row.get("platform") or "").strip().lower() != "tiktok":
        return False
    joined_fields = " ".join(
        str(row.get(field) or "").strip().lower()
        for field in ("job_type", "status", "last_error_code", "last_error_class", "error_message")
    )
    return "ytdlp" in joined_fields.replace("-", "")


def _build_tiktok_single_path_alerts(queue_payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    _sync_core_overrides()
    alerts: list[dict[str, Any]] = []
    topology = dict(_TIKTOK_SINGLE_PATH_TOPOLOGY_DEFAULT)
    primary_live_path = str(topology.get("primary_live_path") or "").strip().lower() or "ytdlp"
    browser_intercept_proven_live = bool(topology.get("browser_intercept_proven_live"))

    if primary_live_path == "ytdlp" and not browser_intercept_proven_live:
        _append_alert(
            alerts,
            code="tiktok_single_path_risk",
            severity="warning",
            message=(
                "TikTok posts currently rely on yt-dlp as the only proven live path. "
                "Browser intercept is not yet proven live, so this is a single-path topology."
            ),
            primary_live_path="yt-dlp",
            browser_intercept_proven_live=browser_intercept_proven_live,
            topology=topology,
        )

    recent_failures = [
        row
        for row in list(queue_payload.get("recent_failures") or [])
        if isinstance(row, dict)
        and _room_callable("_tiktok_recent_failure_looks_ytdlp_related", _tiktok_recent_failure_looks_ytdlp_related)(
            row
        )
    ]
    if recent_failures:
        _append_alert(
            alerts,
            code="tiktok_single_path_degraded",
            severity="critical",
            message=(
                "Recent TikTok queue failures include yt-dlp-related rows, so the primary TikTok path "
                "should be treated as degraded until recovery clears."
            ),
            recent_failure_count=len(recent_failures),
            recent_failure_codes=[
                str(row.get("last_error_code") or "").strip() or str(row.get("error_message") or "").strip()
                for row in recent_failures
                if str(row.get("last_error_code") or "").strip() or str(row.get("error_message") or "").strip()
            ][:5],
            primary_live_path="yt-dlp",
            browser_intercept_proven_live=browser_intercept_proven_live,
            topology=topology,
        )

    return alerts


_LOCAL_ROOM_FUNCTIONS.update({_name: globals()[_name] for _name in _LOCAL_ROOM_NAMES})
__all__ = [
    "_build_tiktok_single_path_alerts",
    "_tiktok_discovery_empty_body_transport_failure",
    "_tiktok_recent_failure_looks_ytdlp_related",
]
