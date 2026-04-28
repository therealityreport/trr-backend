"""Shared Instagram profile normalizer."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

NORMALIZER_VERSION = "instagram-profile-normalizer-v1"


@dataclass(slots=True)
class InstagramExternalLink:
    url: str
    title: str | None = None
    shim_url: str | None = None
    link_type: str | None = None
    source: str | None = None


@dataclass(slots=True)
class InstagramProfile:
    username: str
    normalizer_version: str = NORMALIZER_VERSION
    source_shape: str = "web_profile_info"
    profile_id: str | None = None
    pk: str | None = None
    input_url: str | None = None
    url: str | None = None
    full_name: str | None = None
    biography: str | None = None
    followers_count: int | None = None
    follows_count: int | None = None
    posts_count: int | None = None
    highlight_reel_count: int | None = None
    igtv_video_count: int | None = None
    is_business_account: bool | None = None
    joined_recently: bool | None = None
    has_channel: bool | None = None
    is_private: bool | None = None
    is_verified: bool | None = None
    category_name: str | None = None
    business_category_name: str | None = None
    external_url: str | None = None
    external_url_shimmed: str | None = None
    external_links: list[InstagramExternalLink] = field(default_factory=list)
    profile_pic_url: str | None = None
    profile_pic_url_hd: str | None = None
    about_raw: dict[str, Any] | None = None
    country: str | None = None
    date_joined: str | None = None
    date_verified: str | None = None
    former_usernames_count: int | None = None
    raw_data: dict[str, Any] = field(default_factory=dict, repr=False)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def normalize_instagram_profile(payload: dict[str, Any]) -> InstagramProfile:
    """Normalize a web-profile-info or adapter-style profile payload."""

    user, source_shape = _unwrap_profile_payload(payload)
    username = _string_or_none(_first_value(user, "username", "handle")) or ""
    about = _extract_about(user)
    external_links = _extract_external_links(user)
    external_url = _string_or_none(_first_value(user, "external_url", "externalUrl"))
    if not external_url and external_links:
        external_url = external_links[0].url

    return InstagramProfile(
        username=username,
        source_shape=source_shape,
        profile_id=_string_or_none(_first_value(user, "id", "profile_id", "user_id")),
        pk=_string_or_none(_first_value(user, "pk", "profile_pk")),
        input_url=_string_or_none(_first_value(user, "input_url", "inputUrl")),
        url=_string_or_none(_first_value(user, "url", "profile_url", "profileUrl"))
        or (f"https://www.instagram.com/{username}" if username else None),
        full_name=_string_or_none(_first_value(user, "full_name", "fullName")),
        biography=_string_or_none(_first_value(user, "biography", "bio")),
        followers_count=_extract_count(user, "followers_count", "follower_count", "followersCount", "edge_followed_by"),
        follows_count=_extract_count(user, "follows_count", "following_count", "followsCount", "edge_follow"),
        posts_count=_extract_count(user, "posts_count", "media_count", "postsCount", "edge_owner_to_timeline_media"),
        highlight_reel_count=_extract_count(user, "highlight_reel_count", "highlightReelCount"),
        igtv_video_count=_extract_count(user, "igtv_video_count", "igtvVideoCount"),
        is_business_account=_coerce_bool_or_none(_first_value(user, "is_business_account", "isBusinessAccount")),
        joined_recently=_coerce_bool_or_none(_first_value(user, "joined_recently", "joinedRecently")),
        has_channel=_coerce_bool_or_none(_first_value(user, "has_channel", "hasChannel")),
        is_private=_coerce_bool_or_none(_first_value(user, "is_private", "isPrivate")),
        is_verified=_coerce_bool_or_none(_first_value(user, "is_verified", "isVerified")),
        category_name=_string_or_none(_first_value(user, "category_name", "categoryName")),
        business_category_name=_string_or_none(_first_value(user, "business_category_name", "businessCategoryName")),
        external_url=external_url,
        external_url_shimmed=_string_or_none(_first_value(user, "external_url_shimmed", "externalUrlShimmed")),
        external_links=external_links,
        profile_pic_url=_string_or_none(_first_value(user, "profile_pic_url", "profilePicUrl")),
        profile_pic_url_hd=_string_or_none(
            _first_value(user, "profile_pic_url_hd", "profilePicUrlHd", "profilePicUrlHD")
        ),
        about_raw=about,
        country=_string_or_none(_first_value(about or {}, "country", "country_name", "account_country")),
        date_joined=_string_or_none(_first_value(about or {}, "date_joined", "joined_date", "joinedDate")),
        date_verified=_string_or_none(_first_value(about or {}, "date_verified", "verified_date", "verifiedDate")),
        former_usernames_count=_extract_former_usernames_count(about or {}),
        raw_data=dict(user),
    )


def _unwrap_profile_payload(payload: dict[str, Any]) -> tuple[dict[str, Any], str]:
    if not isinstance(payload, dict):
        return {}, "unknown"
    data = payload.get("data")
    if isinstance(data, dict) and isinstance(data.get("user"), dict):
        return data["user"], "web_profile_info"
    if isinstance(payload.get("user"), dict):
        return payload["user"], "profile_user"
    return payload, "profile_adapter"


def _extract_about(user: dict[str, Any]) -> dict[str, Any] | None:
    for key in ("about", "account_about", "accountAbout", "about_this_account"):
        value = user.get(key)
        if isinstance(value, dict):
            return value
    return None


def _extract_external_links(user: dict[str, Any]) -> list[InstagramExternalLink]:
    links: list[InstagramExternalLink] = []
    seen: dict[str, InstagramExternalLink] = {}

    def add(
        url: Any,
        *,
        title: Any = None,
        shim_url: Any = None,
        link_type: Any = None,
        source: str | None = None,
    ) -> None:
        normalized = _string_or_none(url)
        if not normalized:
            return
        existing = seen.get(normalized)
        if existing is not None:
            existing.title = existing.title or _string_or_none(title)
            existing.shim_url = existing.shim_url or _string_or_none(shim_url)
            existing.link_type = existing.link_type or _string_or_none(link_type)
            return
        link = InstagramExternalLink(
            url=normalized,
            title=_string_or_none(title),
            shim_url=_string_or_none(shim_url),
            link_type=_string_or_none(link_type),
            source=source,
        )
        seen[normalized] = link
        links.append(link)

    add(_first_value(user, "external_url", "externalUrl"), source="external_url")

    for key in ("external_urls", "externalUrls"):
        value = user.get(key)
        if isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    add(
                        item.get("url"),
                        title=item.get("title"),
                        shim_url=item.get("lynx_url") or item.get("shim_url"),
                        link_type=item.get("link_type") or item.get("type"),
                        source=key,
                    )
                else:
                    add(item, source=key)

    for key in ("bio_links", "bioLinks"):
        value = user.get(key)
        if isinstance(value, list):
            for item in value:
                if not isinstance(item, dict):
                    add(item, source=key)
                    continue
                add(
                    item.get("url") or item.get("lynx_url"),
                    title=item.get("title"),
                    shim_url=item.get("lynx_url") or item.get("shim_url"),
                    link_type=item.get("link_type") or item.get("type"),
                    source=key,
                )

    return links


def _extract_count(user: dict[str, Any], *keys: str) -> int | None:
    for key in keys:
        value = user.get(key)
        if isinstance(value, dict) and "count" in value:
            return _coerce_int_or_none(value.get("count"))
        parsed = _coerce_int_or_none(value)
        if parsed is not None:
            return parsed
    return None


def _extract_former_usernames_count(about: dict[str, Any]) -> int | None:
    value = _first_value(about, "former_usernames_count", "formerUsernameCount", "former_usernames")
    if isinstance(value, list):
        return len(value)
    return _coerce_int_or_none(value)


def _first_value(mapping: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in mapping and mapping.get(key) is not None:
            return mapping.get(key)
    return None


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _coerce_bool_or_none(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes"}:
            return True
        if normalized in {"false", "0", "no"}:
            return False
    return bool(value)


def _coerce_int_or_none(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
