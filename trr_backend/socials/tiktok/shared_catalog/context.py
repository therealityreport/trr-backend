# ruff: noqa: F821, UP037
"""Shared-account TikTok catalog context: account bootstrap (user detail, profile HTML,
profile snapshot) and applying a profile snapshot to scraped posts.

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
    def _load_tiktok_cookies(*args: Any, **kwargs: Any) -> Any: ...
    def _metadata_dict(*args: Any, **kwargs: Any) -> Any: ...
    def _normalize_non_negative_int(*args: Any, **kwargs: Any) -> Any: ...

    logger: Any


# Room names are declared before the provider is built so that a late
# ``adopt_published`` (leaf imported after the repository published) still
# captures the repository wrappers for ``_room_callable``.
_LOCAL_ROOM_NAMES: set[str] = {
    "_bootstrap_shared_tiktok_account_context",
    "_apply_tiktok_profile_snapshot_to_post",
}
_IMPORTED_CORE_NAMES: set[str] = set()
_LOCAL_ROOM_FUNCTIONS: dict[str, Any] = {}
_CORE_ROOM_WRAPPERS: dict[str, Any] = {}


_PROVIDER = LateNamespaceProvider(
    globals(),
    prefix="TIKTOK_SHARED_CATALOG_CONTEXT_PROVIDER",
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


def _bootstrap_shared_tiktok_account_context(
    *,
    account_handle: str,
    delay_seconds: float = 0.35,
    scraper: Any | None = None,
    direct_comment_api_enabled_override: bool | None = None,
) -> dict[str, Any]:
    _sync_core_overrides()
    from trr_backend.socials.tiktok import TikTokScraper

    scraper_kwargs: dict[str, Any] = {"cookies": _load_tiktok_cookies()}
    if direct_comment_api_enabled_override is not None:
        scraper_kwargs["direct_comment_api_enabled_override"] = direct_comment_api_enabled_override
    tiktok_scraper = scraper or TikTokScraper(**scraper_kwargs)
    try:
        user_data = tiktok_scraper.fetch_user_detail(account_handle, delay_seconds)
    except Exception:  # noqa: BLE001
        logger.warning("[tiktok] shared-account user-detail enrichment failed for @%s", account_handle)
        user_data = None
    try:
        html_data = tiktok_scraper._fetch_profile_html(account_handle, delay_seconds)  # noqa: SLF001
    except Exception:  # noqa: BLE001
        logger.warning("[tiktok] shared-account html enrichment failed for @%s", account_handle)
        html_data = None
    try:
        html_items, html_sec_uid = tiktok_scraper._extract_posts_from_html_data(html_data or {}, account_handle)  # noqa: SLF001
    except Exception:  # noqa: BLE001
        logger.warning("[tiktok] shared-account html parsing failed for @%s", account_handle)
        html_items, html_sec_uid = [], None
    try:
        profile_snapshot = tiktok_scraper.build_profile_snapshot(
            account_handle,
            user_data=user_data,
            html_data=html_data,
        )
    except Exception:  # noqa: BLE001
        logger.warning("[tiktok] shared-account profile snapshot build failed for @%s", account_handle)
        profile_snapshot = {}
    sec_uid = str(profile_snapshot.get("sec_uid") or html_sec_uid or "").strip() or None
    total_posts = max(
        _normalize_non_negative_int(profile_snapshot.get("total_posts")),
        len(html_items),
    )
    if total_posts > 0:
        profile_snapshot["total_posts"] = total_posts
    if sec_uid:
        profile_snapshot["sec_uid"] = sec_uid
    return {
        "scraper": tiktok_scraper,
        "user_data": user_data,
        "html_data": html_data,
        "html_items": html_items,
        "sec_uid": sec_uid,
        "profile_snapshot": profile_snapshot,
        "total_posts": total_posts,
    }


def _apply_tiktok_profile_snapshot_to_post(post: Any, snapshot: Mapping[str, Any] | None) -> Any:
    _sync_core_overrides()
    metadata = _metadata_dict(snapshot)
    if not metadata:
        return post
    if not getattr(post, "author_nickname", None) and str(metadata.get("display_name") or "").strip():
        post.author_nickname = str(metadata.get("display_name") or "").strip()
    if not getattr(post, "user_avatar_url", None) and str(metadata.get("avatar_url") or "").strip():
        post.user_avatar_url = str(metadata.get("avatar_url") or "").strip()
    if not getattr(post, "username", None) and str(metadata.get("username") or "").strip():
        post.username = str(metadata.get("username") or "").strip()
    return post


_LOCAL_ROOM_FUNCTIONS.update({_name: globals()[_name] for _name in _LOCAL_ROOM_NAMES})
__all__ = [
    "_apply_tiktok_profile_snapshot_to_post",
    "_bootstrap_shared_tiktok_account_context",
]
