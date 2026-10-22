"""TikTok profile snapshot parsing.

``ProfileParsingMixin`` builds profile snapshots for ``TikTokScraper`` from the
user-detail API payload and the embedded HTML page state.
``extract_profile_preview_sections`` serves the direct admin preview route.

The two repository rooms at the bottom (``_merge_tiktok_profile_snapshots`` and
``_tiktok_profile_snapshot_from_analysis_rows``) were moved out of
``social_season_analytics_impl``. The legacy repository keeps same-named
wrappers that delegate through ``_LOCAL_ROOM_FUNCTIONS`` and binds its helper
namespace through ``_configure_legacy_provider``; this module never imports it.
"""

from collections.abc import Mapping, Sequence
from typing import Any

from trr_backend.socials.provider_registry import register_legacy_patchable_namespace
from trr_backend.socials.tiktok.models import _dict_or_empty


class ProfileParsingMixin:
    """Build TikTok profile snapshots from user-detail and HTML payloads."""

    def _extract_user_detail_avatar_url(self, user_data: dict | None) -> str | None:
        if not isinstance(user_data, dict):
            return None
        user_info = _dict_or_empty(user_data.get("userInfo"))
        user = _dict_or_empty(user_info.get("user"))
        return self._pick_best_avatar_url(
            user.get("avatarLarger"),
            user.get("avatar_larger"),
            user.get("originalAvatarUrl"),
            user.get("avatarMedium"),
            user.get("avatar_medium"),
            user.get("avatarThumb"),
            user.get("avatar_thumb"),
            user.get("avatar_thumb_url"),
            user.get("avatarUrl"),
            user.get("avatar"),
        )

    def _extract_user_detail_profile_snapshot(self, user_data: dict | None, username: str) -> dict[str, Any]:
        if not isinstance(user_data, dict):
            return {}
        user_info = _dict_or_empty(user_data.get("userInfo"))
        user = _dict_or_empty(user_info.get("user"))
        stats = _dict_or_empty(user_info.get("stats"))
        resolved_username = str(
            user.get("uniqueId") or user.get("unique_id") or user.get("uniqueIdModifyTime") or username or ""
        ).strip()
        if "@" in resolved_username:
            resolved_username = resolved_username.split("@")[-1].strip()
        profile_url = f"https://www.tiktok.com/@{resolved_username}" if resolved_username else None
        return {
            "username": resolved_username or username,
            "sec_uid": str(user.get("secUid") or user.get("sec_uid") or "").strip() or None,
            "display_name": str(user.get("nickname") or user.get("nickName") or "").strip() or None,
            "bio": str(user.get("signature") or user.get("bio") or "").strip() or None,
            "avatar_url": self._extract_user_detail_avatar_url(user_data),
            "is_verified": bool(user.get("verified") or user.get("isVerified") or False),
            "follower_count": self._safe_int_metric(
                stats.get("followerCount") or stats.get("follower_count") or user.get("followerCount") or 0
            ),
            "following_count": self._safe_int_metric(
                stats.get("followingCount") or stats.get("following_count") or user.get("followingCount") or 0
            ),
            "total_posts": self._safe_int_metric(
                stats.get("videoCount")
                or stats.get("video_count")
                or stats.get("itemCount")
                or stats.get("item_count")
                or user.get("videoCount")
                or user.get("video_count")
                or user.get("itemCount")
                or user.get("item_count")
                or 0
            ),
            "profile_url": profile_url,
        }

    def _extract_html_profile_snapshot(self, data: dict | None, username: str) -> dict[str, Any]:
        if not isinstance(data, dict):
            return {}
        default_scope = data.get("__DEFAULT_SCOPE__", {}) if isinstance(data.get("__DEFAULT_SCOPE__"), dict) else {}
        webapp_detail = (
            default_scope.get("webapp.user-detail", {})
            if isinstance(default_scope.get("webapp.user-detail"), dict)
            else {}
        )
        user_info = webapp_detail.get("userInfo", {}) if isinstance(webapp_detail.get("userInfo"), dict) else {}
        user = user_info.get("user", {}) if isinstance(user_info.get("user"), dict) else {}
        stats = user_info.get("stats", {}) if isinstance(user_info.get("stats"), dict) else {}
        item_list = user_info.get("itemList", []) if isinstance(user_info.get("itemList"), list) else []
        user_module = data.get("UserModule", {}) if isinstance(data.get("UserModule"), dict) else {}
        users = user_module.get("users", {}) if isinstance(user_module.get("users"), dict) else {}
        user_fallback = users.get(username, {}) if isinstance(users.get(username), dict) else {}
        stats_module = user_module.get("stats", {}) if isinstance(user_module.get("stats"), dict) else {}
        stats_fallback = stats_module.get(username, {}) if isinstance(stats_module.get(username), dict) else {}
        resolved_username = str(
            user.get("uniqueId") or user.get("unique_id") or user_fallback.get("uniqueId") or username or ""
        ).strip()
        profile_url = f"https://www.tiktok.com/@{resolved_username}" if resolved_username else None
        return {
            "username": resolved_username or username,
            "sec_uid": str(user.get("secUid") or user_fallback.get("secUid") or "").strip() or None,
            "display_name": str(
                user.get("nickname")
                or user.get("nickName")
                or user_fallback.get("nickname")
                or user_fallback.get("nickName")
                or ""
            ).strip()
            or None,
            "bio": (
                str(user.get("signature") or user.get("bio") or user_fallback.get("signature") or "").strip() or None
            ),
            "avatar_url": self._pick_best_avatar_url(
                user.get("avatarLarger"),
                user.get("avatar_larger"),
                user.get("originalAvatarUrl"),
                user.get("avatarMedium"),
                user.get("avatar_medium"),
                user.get("avatarThumb"),
                user.get("avatar_thumb"),
                user.get("avatar_thumb_url"),
                user.get("avatarUrl"),
                user.get("avatar"),
                user_fallback.get("avatarLarger"),
                user_fallback.get("avatar_larger"),
                user_fallback.get("originalAvatarUrl"),
                user_fallback.get("avatarMedium"),
                user_fallback.get("avatar_medium"),
                user_fallback.get("avatarThumb"),
                user_fallback.get("avatar_thumb"),
                user_fallback.get("avatar_thumb_url"),
                user_fallback.get("avatarUrl"),
                user_fallback.get("avatar"),
            ),
            "is_verified": bool(
                user.get("verified") or user.get("isVerified") or user_fallback.get("verified") or False
            ),
            "follower_count": self._safe_int_metric(
                stats.get("followerCount")
                or stats.get("follower_count")
                or stats_fallback.get("followerCount")
                or stats_fallback.get("follower_count")
                or 0
            ),
            "following_count": self._safe_int_metric(
                stats.get("followingCount")
                or stats.get("following_count")
                or stats_fallback.get("followingCount")
                or stats_fallback.get("following_count")
                or 0
            ),
            "total_posts": max(
                self._safe_int_metric(
                    stats.get("videoCount")
                    or stats.get("video_count")
                    or stats.get("itemCount")
                    or stats.get("item_count")
                    or stats_fallback.get("videoCount")
                    or stats_fallback.get("video_count")
                    or stats_fallback.get("itemCount")
                    or stats_fallback.get("item_count")
                    or 0
                ),
                len(item_list),
            ),
            "profile_url": profile_url,
        }

    @staticmethod
    def _merge_profile_snapshots(*snapshots: dict[str, Any]) -> dict[str, Any]:
        merged: dict[str, Any] = {}
        for snapshot in snapshots:
            if not isinstance(snapshot, dict):
                continue
            for key in ("username", "sec_uid", "display_name", "bio", "avatar_url", "profile_url"):
                value = str(snapshot.get(key) or "").strip()
                if value and not str(merged.get(key) or "").strip():
                    merged[key] = value
            for key in ("follower_count", "following_count", "total_posts"):
                if snapshot.get(key) is None:
                    continue
                merged[key] = max(int(merged.get(key) or 0), max(0, int(snapshot.get(key) or 0)))
            if snapshot.get("is_verified"):
                merged["is_verified"] = True
        return merged

    def build_profile_snapshot(
        self,
        username: str,
        *,
        user_data: dict | None = None,
        html_data: dict | None = None,
    ) -> dict[str, Any]:
        return self._merge_profile_snapshots(
            self._extract_user_detail_profile_snapshot(user_data, username),
            self._extract_html_profile_snapshot(html_data, username),
        )


def extract_profile_preview_sections(data: Mapping[str, Any]) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
    """Return the user and stats sections from TikTok's profile detail payload."""

    user_info = data.get("userInfo", {})
    if not isinstance(user_info, Mapping):
        return {}, {}

    user_data = user_info.get("user", {})
    stats = user_info.get("stats", {})

    return (
        user_data if isinstance(user_data, Mapping) else {},
        stats if isinstance(stats, Mapping) else {},
    )


# ---------------------------------------------------------------------------
# Repository rooms moved from social_season_analytics_impl
# ---------------------------------------------------------------------------

_LOCAL_ROOM_NAMES: set[str] = {
    "_merge_tiktok_profile_snapshots",
    "_tiktok_profile_snapshot_from_analysis_rows",
}
_LOCAL_ROOM_FUNCTIONS: dict[str, Any] = {}
_LEGACY_NAMESPACE: dict[str, Any] | None = None
_LEGACY_ORIGINALS: dict[str, Any] = {}
_NO_LEGACY_FALLBACK = object()


def _configure_legacy_provider(
    namespace: dict[str, Any],
    originals: Mapping[str, Any],
) -> None:
    """Bind the supported monolith patch surface without importing it."""

    global _LEGACY_NAMESPACE, _LEGACY_ORIGINALS

    _LEGACY_NAMESPACE = namespace
    _LEGACY_ORIGINALS = dict(originals)


def _legacy_value(name: str, local_value: Any = _NO_LEGACY_FALLBACK) -> Any:
    namespace = _LEGACY_NAMESPACE
    if namespace is not None and name in namespace:
        return namespace[name]
    if local_value is not _NO_LEGACY_FALLBACK:
        return local_value
    raise RuntimeError(f"TikTok profile-parsing provider is not configured: {name}")


def _legacy_callable(name: str, local_impl: Any = _NO_LEGACY_FALLBACK) -> Any:
    candidate = _legacy_value(name, local_impl)
    if not callable(candidate):
        raise TypeError(f"TikTok profile-parsing provider is not callable: {name}")
    return candidate


def _room_callable(name: str, local_impl: Any) -> Any:
    candidate = _legacy_value(name, None)
    if callable(candidate) and candidate is not _LEGACY_ORIGINALS.get(name):
        return candidate
    return local_impl


def _merge_tiktok_profile_snapshots(*snapshots: Mapping[str, Any] | None) -> dict[str, Any]:
    metadata_dict = _legacy_callable("_metadata_dict")
    normalize_non_negative_int = _legacy_callable("_normalize_non_negative_int")
    merged: dict[str, Any] = {}
    for snapshot in snapshots:
        metadata = metadata_dict(snapshot)
        if not metadata:
            continue
        for key in ("username", "sec_uid", "display_name", "bio", "avatar_url", "profile_url"):
            value = str(metadata.get(key) or "").strip()
            if value and not str(merged.get(key) or "").strip():
                merged[key] = value
        for key in ("follower_count", "following_count", "total_posts"):
            if metadata.get(key) is None:
                continue
            merged[key] = max(
                normalize_non_negative_int(merged.get(key)), normalize_non_negative_int(metadata.get(key))
            )
        if bool(metadata.get("is_verified")):
            merged["is_verified"] = True
    return merged


def _tiktok_profile_snapshot_from_analysis_rows(
    rows: Sequence[Mapping[str, Any]], account_handle: str
) -> dict[str, Any]:
    metadata_dict = _legacy_callable("_metadata_dict")
    best_profile_avatar_url = _legacy_callable("_best_profile_avatar_url")
    extract_avatar_url_candidates_from_raw_data = _legacy_callable("_extract_avatar_url_candidates_from_raw_data")
    normalize_account_handle = _legacy_callable("_normalize_account_handle")
    platform_profile_url_for_handle = _legacy_callable("_platform_profile_url_for_handle")
    for row in rows:
        row_json = metadata_dict(row)
        raw_data = metadata_dict(row_json.get("raw_data"))
        author = metadata_dict(raw_data.get("author"))
        display_name = str(
            row_json.get("nickname")
            or author.get("nickname")
            or author.get("nickName")
            or raw_data.get("nickname")
            or ""
        ).strip()
        avatar_url = best_profile_avatar_url(
            [
                row_json.get("user_avatar_url"),
                row_json.get("avatar_url"),
                *extract_avatar_url_candidates_from_raw_data(raw_data),
            ]
        )
        snapshot = {
            "username": normalize_account_handle(author.get("uniqueId") or author.get("unique_id") or account_handle),
            "display_name": display_name or None,
            "avatar_url": avatar_url,
            "is_verified": bool(
                author.get("verified") or author.get("isVerified") or raw_data.get("verified") or False
            ),
            "profile_url": platform_profile_url_for_handle("tiktok", account_handle),
        }
        if any(snapshot.values()):
            return snapshot
    return {}


_LOCAL_ROOM_FUNCTIONS.update({_name: globals()[_name] for _name in _LOCAL_ROOM_NAMES})
__all__ = [
    "ProfileParsingMixin",
    "_merge_tiktok_profile_snapshots",
    "_tiktok_profile_snapshot_from_analysis_rows",
    "extract_profile_preview_sections",
]

# Keep the repository rooms patchable through the published legacy namespace
# while the monolith is being retired.
register_legacy_patchable_namespace(globals(), tuple(_LOCAL_ROOM_NAMES))
