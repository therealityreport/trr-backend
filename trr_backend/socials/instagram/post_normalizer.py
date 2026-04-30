"""Shared Instagram post and comment normalizers.

These helpers intentionally stop at typed DTOs. Persistence remains owned by
the repository/scraper lanes that call them.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any

NORMALIZER_VERSION = "instagram-post-normalizer-v1"

_SHORTCODE_RE = re.compile(r"instagram\.com/(?:p|reel|tv)/([^/?#]+)/?", re.IGNORECASE)
_HASHTAG_RE = re.compile(r"(?<![\w.])#([A-Za-z0-9_]+)")
_MENTION_RE = re.compile(r"(?<![\w.])@([A-Za-z0-9_.]+)")

_COMMENT_SAMPLE_KEYS = (
    "latestComments",
    "latest_comments",
    "firstComment",
    "first_comment",
    "edge_media_to_parent_comment",
    "edge_media_to_preview_comment",
    "edge_media_preview_comment",
)


@dataclass(slots=True)
class InstagramUser:
    username: str | None = None
    user_id: str | None = None
    full_name: str | None = None
    profile_pic_url: str | None = None
    profile_pic_url_hd: str | None = None
    is_verified: bool | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class InstagramTaggedUser(InstagramUser):
    tag_x: float | None = None
    tag_y: float | None = None
    tag_position_source: str | None = None


@dataclass(slots=True)
class InstagramCaption:
    text: str = ""
    caption_id: str | None = None
    created_at: int | None = None
    is_edited: bool | None = None
    has_translation: bool | None = None
    raw: dict[str, Any] | None = None


@dataclass(slots=True)
class InstagramMediaVariant:
    url: str
    media_type: str
    source: str
    width: int | None = None
    height: int | None = None
    is_thumbnail: bool = False


@dataclass(slots=True)
class InstagramLocation:
    location_id: str | None = None
    name: str | None = None
    slug: str | None = None
    address: str | None = None
    latitude: float | None = None
    longitude: float | None = None


@dataclass(slots=True)
class InstagramChildPost:
    slide_index: int
    source_id: str | None = None
    shortcode: str | None = None
    media_type: str | None = None
    width: int | None = None
    height: int | None = None
    alt_text: str | None = None
    media_variants: list[InstagramMediaVariant] = field(default_factory=list)
    tagged_users: list[InstagramTaggedUser] = field(default_factory=list)


@dataclass(slots=True)
class InstagramCommentAuthor:
    username: str | None = None
    user_id: str | None = None
    full_name: str | None = None
    profile_pic_url: str | None = None
    profile_pic_url_hd: str | None = None
    is_verified: bool | None = None


@dataclass(slots=True)
class InstagramComment:
    comment_id: str
    text: str
    author: InstagramCommentAuthor
    created_at: int | None = None
    created_at_iso: str | None = None
    likes_count: int = 0
    replies_count: int = 0
    parent_comment_id: str | None = None
    replies: list[InstagramComment] = field(default_factory=list)
    raw_data: dict[str, Any] = field(default_factory=dict, repr=False)

    @property
    def username(self) -> str | None:
        return self.author.username

    @property
    def user_id(self) -> str | None:
        return self.author.user_id

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["replies"] = [reply.to_dict() for reply in self.replies]
        return result


@dataclass(slots=True)
class InstagramPost:
    shortcode: str | None
    source_id: str | None
    source_shape: str
    normalizer_version: str = NORMALIZER_VERSION
    media_type: str | None = None
    permalink: str | None = None
    owner: InstagramUser | None = None
    caption: InstagramCaption = field(default_factory=InstagramCaption)
    hashtags: list[str] = field(default_factory=list)
    mentions: list[str] = field(default_factory=list)
    tagged_users: list[InstagramTaggedUser] = field(default_factory=list)
    collaborators: list[InstagramUser] = field(default_factory=list)
    media_variants: list[InstagramMediaVariant] = field(default_factory=list)
    media_urls: list[str] = field(default_factory=list)
    thumbnail_url: str | None = None
    width: int | None = None
    height: int | None = None
    location: InstagramLocation | None = None
    flags: dict[str, bool] = field(default_factory=dict)
    music_info: dict[str, Any] | None = None
    audio_url: str | None = None
    video_duration: float | None = None
    video_play_count: int | None = None
    video_view_count: int | None = None
    context_items: list[dict[str, Any]] = field(default_factory=list)
    child_posts: list[InstagramChildPost] = field(default_factory=list)
    comments: list[InstagramComment] = field(default_factory=list)
    comment_samples_excluded: list[str] = field(default_factory=list)
    raw_data: dict[str, Any] = field(default_factory=dict, repr=False)

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["comments"] = [comment.to_dict() for comment in self.comments]
        return result


def normalize_instagram_post(payload: dict[str, Any], *, account_handle: str | None = None) -> InstagramPost:
    """Normalize one Instagram post-like payload into a canonical typed DTO."""

    node, source_shape = _unwrap_post_payload(payload)
    caption = _extract_caption(node)
    media_variants = _extract_media_variants(node)
    child_posts = _extract_child_posts(node)
    media_urls = _extract_primary_media_urls(node, media_variants, child_posts)
    width, height = _extract_dimensions(node, media_variants)
    thumbnail_url = _extract_thumbnail_url(node, media_variants)
    shortcode = _extract_shortcode(node)
    permalink = _extract_permalink(node, shortcode)
    flags = _extract_flags(node)

    return InstagramPost(
        shortcode=shortcode,
        source_id=_extract_source_id(node),
        source_shape=source_shape,
        media_type=_determine_media_type(node),
        permalink=permalink,
        owner=_extract_owner(node, account_handle=account_handle),
        caption=caption,
        hashtags=_extract_hashtags(node, caption.text),
        mentions=_extract_mentions(node, caption.text),
        tagged_users=_extract_tagged_users(node),
        collaborators=_extract_collaborators(node),
        media_variants=media_variants,
        media_urls=media_urls,
        thumbnail_url=thumbnail_url,
        width=width,
        height=height,
        location=_extract_location(node),
        flags=flags,
        music_info=_extract_music_info(node),
        audio_url=_string_or_none(_first_value(node, "audioUrl", "audio_url")),
        video_duration=_coerce_float(_first_value(node, "videoDuration", "video_duration", "duration")),
        video_play_count=_coerce_int_or_none(_first_value(node, "videoPlayCount", "video_play_count", "play_count")),
        video_view_count=_coerce_int_or_none(
            _first_value(node, "videoViewCount", "video_view_count", "video_view_count", "view_count")
        ),
        context_items=_extract_context_items(node),
        child_posts=child_posts,
        comment_samples_excluded=_detect_comment_sample_keys(node),
        raw_data=dict(node),
    )


def normalize_instagram_comment(
    payload: dict[str, Any],
    *,
    parent_comment_id: str | None = None,
) -> InstagramComment:
    """Normalize a full comments-lane comment or reply payload.

    This function is for dedicated comments payloads only. Post-level sample
    fields such as latestComments and firstComment are deliberately ignored by
    normalize_instagram_post().
    """

    comment_id = _string_or_none(_first_value(payload, "id", "pk")) or ""
    author = _extract_comment_author(payload)
    created_at = _coerce_timestamp(_first_value(payload, "created_at", "timestamp"))
    replies = [
        normalize_instagram_comment(reply, parent_comment_id=comment_id)
        for reply in _extract_reply_rows(payload)
        if isinstance(reply, dict)
    ]
    replies_count = _coerce_int(
        _first_value(payload, "child_comment_count", "repliesCount", "reply_count", "replies_count"),
        default=len(replies),
    )
    if replies and replies_count < len(replies):
        replies_count = len(replies)

    return InstagramComment(
        comment_id=comment_id,
        text=str(_first_value(payload, "text", "comment_text") or ""),
        author=author,
        created_at=created_at,
        created_at_iso=_timestamp_to_iso(created_at),
        likes_count=_coerce_int(
            _first_value(payload, "comment_like_count", "likesCount", "like_count", "likes"),
            default=0,
        ),
        replies_count=replies_count,
        parent_comment_id=parent_comment_id,
        replies=replies,
        raw_data=dict(payload),
    )


def _unwrap_post_payload(payload: dict[str, Any]) -> tuple[dict[str, Any], str]:
    if not isinstance(payload, dict):
        return {}, "unknown"

    items = payload.get("items")
    if isinstance(items, list) and items and isinstance(items[0], dict):
        return items[0], "media_info_rest"

    data = payload.get("data")
    if isinstance(data, dict):
        shortcode_media = data.get("xdt_shortcode_media")
        if isinstance(shortcode_media, dict):
            return shortcode_media, "shortcode_graphql"
        connection = data.get("xdt_api__v1__feed__user_timeline_graphql_connection")
        if isinstance(connection, dict):
            edges = connection.get("edges")
            if isinstance(edges, list) and edges and isinstance(edges[0], dict):
                node = edges[0].get("node")
                if isinstance(node, dict):
                    return node, "xdt_timeline"

    graphql = payload.get("graphql")
    if isinstance(graphql, dict) and isinstance(graphql.get("shortcode_media"), dict):
        return graphql["shortcode_media"], "shortcode_graphql"

    node = payload.get("node")
    if isinstance(node, dict) and len(payload) <= 3:
        return node, "graphql_edge"

    return payload, _detect_source_shape(payload)


def _detect_source_shape(node: dict[str, Any]) -> str:
    typename = str(node.get("__typename") or "")
    if typename.startswith("XDT") or "xdt" in typename.lower() or "code" in node and "pk" in node:
        return "xdt_media_dict"
    if typename.startswith("Graph") or "edge_media_to_caption" in node or "edge_sidecar_to_children" in node:
        return "shortcode_graphql"
    if any(key in node for key in ("shortCode", "displayUrl", "ownerUsername", "coauthorProducers")):
        return "apify_adapter"
    if any(key in node for key in ("og:image", "json_ld", "thumbnailUrl")):
        return "permalink_metadata"
    return "unknown"


def _extract_shortcode(node: dict[str, Any]) -> str | None:
    value = _string_or_none(_first_value(node, "shortcode", "shortCode", "code"))
    if value:
        return value
    for key in ("url", "permalink", "post_url"):
        match = _SHORTCODE_RE.search(str(node.get(key) or ""))
        if match:
            return match.group(1)
    return None


def _extract_source_id(node: dict[str, Any]) -> str | None:
    return _string_or_none(_first_value(node, "pk", "id", "media_id", "mediaId"))


def _extract_permalink(node: dict[str, Any], shortcode: str | None) -> str | None:
    value = _string_or_none(_first_value(node, "url", "permalink", "post_url"))
    if value:
        return value
    return f"https://www.instagram.com/p/{shortcode}/" if shortcode else None


def _extract_owner(node: dict[str, Any], *, account_handle: str | None) -> InstagramUser | None:
    owner = node.get("owner") if isinstance(node.get("owner"), dict) else {}
    user = node.get("user") if isinstance(node.get("user"), dict) else {}
    user_hd_pic = user.get("hd_profile_pic_url_info") if isinstance(user.get("hd_profile_pic_url_info"), dict) else {}
    username = _string_or_none(
        _first_value(node, "ownerUsername", "owner_username") or owner.get("username") or user.get("username")
    )
    if not username:
        username = _string_or_none(account_handle)
    if not username:
        return None
    return InstagramUser(
        username=username,
        user_id=_string_or_none(
            _first_value(node, "ownerId", "owner_id")
            or owner.get("id")
            or owner.get("pk")
            or user.get("id")
            or user.get("pk")
        ),
        full_name=_string_or_none(
            _first_value(node, "ownerFullName", "owner_full_name")
            or owner.get("full_name")
            or owner.get("fullName")
            or user.get("full_name")
            or user.get("fullName")
        ),
        profile_pic_url=_pick_profile_pic(
            _first_value(node, "ownerProfilePicUrlHd", "ownerProfilePicUrl"),
            owner.get("profile_pic_url_hd"),
            owner.get("profilePicUrlHd"),
            owner.get("profile_pic_url"),
            owner.get("profilePicUrl"),
            user_hd_pic.get("url"),
            user.get("profile_pic_url_hd"),
            user.get("profilePicUrlHd"),
            user.get("profile_pic_url"),
            user.get("profilePicUrl"),
        ),
        profile_pic_url_hd=_string_or_none(
            _first_value(node, "ownerProfilePicUrlHd")
            or owner.get("profile_pic_url_hd")
            or owner.get("profilePicUrlHd")
            or user_hd_pic.get("url")
            or user.get("profile_pic_url_hd")
            or user.get("profilePicUrlHd")
        ),
        is_verified=_coerce_bool_or_none(
            _first_value(node, "ownerIsVerified", "owner_is_verified")
            if _first_value(node, "ownerIsVerified", "owner_is_verified") is not None
            else owner.get("is_verified", user.get("is_verified"))
        ),
    )


def _extract_caption(node: dict[str, Any]) -> InstagramCaption:
    raw_caption: dict[str, Any] | None = None
    caption_value = node.get("caption")
    if isinstance(caption_value, dict):
        raw_caption = caption_value
        text = str(caption_value.get("text") or "")
        is_edited = _first_value(caption_value, "is_edited", "isEdited")
        if is_edited is None:
            is_edited = _first_value(node, "caption_is_edited", "captionIsEdited")
        has_translation = _first_value(caption_value, "has_translation", "hasTranslation")
        if has_translation is None:
            has_translation = _first_value(node, "caption_has_translation", "captionHasTranslation")
        return InstagramCaption(
            text=text,
            caption_id=_string_or_none(_first_value(caption_value, "id", "pk")),
            created_at=_coerce_timestamp(_first_value(caption_value, "created_at", "createdAt", "timestamp")),
            is_edited=_coerce_bool_or_none(is_edited),
            has_translation=_coerce_bool_or_none(has_translation),
            raw=raw_caption,
        )
    if isinstance(caption_value, str):
        return InstagramCaption(text=caption_value)

    edges = _edge_nodes(node.get("edge_media_to_caption"))
    for edge_node in edges:
        return InstagramCaption(text=str(edge_node.get("text") or ""), raw=edge_node)

    return InstagramCaption(text=str(_first_value(node, "caption_text", "text") or ""))


def _extract_hashtags(node: dict[str, Any], caption: str) -> list[str]:
    tags: list[str] = []
    raw_tags = node.get("hashtags")
    if isinstance(raw_tags, list):
        for item in raw_tags:
            raw = item.get("name") if isinstance(item, dict) else item
            normalized = _normalize_hashtag(raw)
            if normalized:
                tags.append(normalized)
    tags.extend(_normalize_hashtag(match) for match in _HASHTAG_RE.findall(caption or ""))
    return _dedupe([tag for tag in tags if tag])


def _extract_mentions(node: dict[str, Any], caption: str) -> list[str]:
    mentions: list[str] = []
    raw_mentions = node.get("mentions")
    if isinstance(raw_mentions, list):
        for item in raw_mentions:
            raw = item.get("username") if isinstance(item, dict) else item
            normalized = _normalize_mention(raw)
            if normalized:
                mentions.append(normalized)
    mentions.extend(_normalize_mention(match) for match in _MENTION_RE.findall(caption or ""))
    return _dedupe([mention for mention in mentions if mention])


def _extract_tagged_users(node: dict[str, Any]) -> list[InstagramTaggedUser]:
    tagged: list[InstagramTaggedUser] = []
    seen: set[str] = set()

    def add_user(source: dict[str, Any], position: tuple[float | None, float | None, str | None]) -> None:
        username = _normalize_handle(_first_value(source, "username", "handle"))
        if not username or username.lower() in seen:
            return
        seen.add(username.lower())
        tagged.append(
            InstagramTaggedUser(
                username=username,
                user_id=_string_or_none(_first_value(source, "id", "pk", "user_id", "userId")),
                full_name=_string_or_none(_first_value(source, "full_name", "fullName")),
                profile_pic_url=_pick_profile_pic(
                    _first_value(source, "profile_pic_url_hd", "profilePicUrlHd"),
                    _first_value(source, "profile_pic_url", "profilePicUrl"),
                ),
                profile_pic_url_hd=_string_or_none(_first_value(source, "profile_pic_url_hd", "profilePicUrlHd")),
                is_verified=_coerce_bool_or_none(_first_value(source, "is_verified", "isVerified")),
                tag_x=position[0],
                tag_y=position[1],
                tag_position_source=position[2],
            )
        )

    for edge in _edges(node.get("edge_media_to_tagged_user")):
        edge_node = edge.get("node") if isinstance(edge.get("node"), dict) else {}
        user = edge_node.get("user") if isinstance(edge_node.get("user"), dict) else {}
        add_user(user, _extract_position(edge_node, edge, source_prefix="graphql"))

    usertags = node.get("usertags")
    if isinstance(usertags, dict):
        for item in usertags.get("in") or []:
            if not isinstance(item, dict):
                continue
            user = item.get("user") if isinstance(item.get("user"), dict) else {}
            add_user(user, _extract_position(item, source_prefix="rest_usertags"))

    tagged_users = node.get("taggedUsers")
    if isinstance(tagged_users, list):
        for item in tagged_users:
            if not isinstance(item, dict):
                continue
            user = item.get("user") if isinstance(item.get("user"), dict) else item
            add_user(user, _extract_position(item, source_prefix="apify_tagged_users"))

    return tagged


def _extract_collaborators(node: dict[str, Any]) -> list[InstagramUser]:
    collaborators: list[InstagramUser] = []
    seen: set[str] = set()
    for key in ("coauthor_producers", "invited_coauthor_producers", "coauthorProducers", "coauthors"):
        values = node.get(key)
        if not isinstance(values, list):
            continue
        for value in values:
            if not isinstance(value, dict):
                username = _normalize_handle(value)
                user: dict[str, Any] = {"username": username} if username else {}
            else:
                user = value.get("user") if isinstance(value.get("user"), dict) else value
            username = _normalize_handle(user.get("username"))
            if not username or username.lower() in seen:
                continue
            seen.add(username.lower())
            collaborators.append(
                InstagramUser(
                    username=username,
                    user_id=_string_or_none(_first_value(user, "id", "pk", "user_id", "userId")),
                    full_name=_string_or_none(_first_value(user, "full_name", "fullName")),
                    profile_pic_url=_pick_profile_pic(
                        _first_value(user, "profile_pic_url_hd", "profilePicUrlHd"),
                        _first_value(user, "profile_pic_url", "profilePicUrl"),
                    ),
                    profile_pic_url_hd=_string_or_none(_first_value(user, "profile_pic_url_hd", "profilePicUrlHd")),
                    is_verified=_coerce_bool_or_none(_first_value(user, "is_verified", "isVerified")),
                )
            )
    return collaborators


def _extract_media_variants(node: dict[str, Any]) -> list[InstagramMediaVariant]:
    variants: list[InstagramMediaVariant] = []
    seen: set[str] = set()

    def add(
        url: Any,
        media_type: str,
        source: str,
        *,
        width: Any = None,
        height: Any = None,
        thumbnail: bool = False,
    ) -> None:
        normalized = _string_or_none(url)
        if not normalized or normalized in seen:
            return
        seen.add(normalized)
        variants.append(
            InstagramMediaVariant(
                url=normalized,
                media_type=media_type,
                source=source,
                width=_coerce_int_or_none(width),
                height=_coerce_int_or_none(height),
                is_thumbnail=thumbnail,
            )
        )

    add(_first_value(node, "displayUrl", "display_url"), "image", "display_url", thumbnail=True)
    add(_first_value(node, "thumbnail_src", "thumbnailUrl", "thumbnail_url"), "image", "thumbnail", thumbnail=True)
    add(_first_value(node, "videoUrl", "video_url"), "video", "video_url")

    for item in _as_list(node.get("displayResourceUrls")):
        add(item, "image", "displayResourceUrls", thumbnail=True)
    for item in _as_list(node.get("display_resources")):
        if isinstance(item, dict):
            add(
                item.get("src"),
                "image",
                "display_resources",
                width=item.get("config_width"),
                height=item.get("config_height"),
                thumbnail=True,
            )
    image_versions = node.get("image_versions2")
    image_candidates = image_versions.get("candidates") if isinstance(image_versions, dict) else None
    for item in _as_list(image_candidates):
        if isinstance(item, dict):
            add(
                item.get("url"),
                "image",
                "image_versions2",
                width=item.get("width"),
                height=item.get("height"),
                thumbnail=True,
            )
    for item in _as_list(node.get("video_versions")):
        if isinstance(item, dict):
            add(item.get("url"), "video", "video_versions", width=item.get("width"), height=item.get("height"))

    image_value = node.get("image")
    if isinstance(image_value, list):
        for item in image_value:
            add(item, "image", "json_ld.image", thumbnail=True)
    else:
        add(image_value, "image", "json_ld.image", thumbnail=True)
    video_value = node.get("video")
    if isinstance(video_value, dict):
        add(video_value.get("contentUrl") or video_value.get("url"), "video", "json_ld.video")
        add(video_value.get("thumbnailUrl"), "image", "json_ld.video.thumbnail", thumbnail=True)

    return variants


def _select_primary_variant_url(
    media_variants: list[InstagramMediaVariant],
    *,
    media_type: str | None,
) -> str | None:
    normalized_media_type = str(media_type or "").strip().lower()
    expects_video = normalized_media_type in {"video", "reel"}
    if expects_video:
        for variant in media_variants:
            if variant.media_type == "video":
                return variant.url
    for variant in media_variants:
        if variant.media_type == "image":
            return variant.url
    for variant in media_variants:
        if variant.media_type == "video":
            return variant.url
    return media_variants[0].url if media_variants else None


def _extract_primary_media_urls(
    node: dict[str, Any],
    media_variants: list[InstagramMediaVariant],
    child_posts: list[InstagramChildPost],
) -> list[str]:
    media_type = _determine_media_type(node)
    if media_type == "carousel" and child_posts:
        return _dedupe(
            [
                selected
                for child in child_posts
                for selected in [_select_primary_variant_url(child.media_variants, media_type=child.media_type)]
                if selected
            ]
        )
    selected = _select_primary_variant_url(media_variants, media_type=media_type)
    return [selected] if selected else []


def _extract_child_posts(node: dict[str, Any]) -> list[InstagramChildPost]:
    children: list[InstagramChildPost] = []

    raw_children = node.get("childPosts")
    if isinstance(raw_children, list):
        for index, child in enumerate(raw_children):
            if isinstance(child, dict):
                children.append(_normalize_child_post(child, index))
        if children:
            return children

    carousel = node.get("carousel_media")
    if isinstance(carousel, list):
        for index, child in enumerate(carousel):
            if isinstance(child, dict):
                children.append(_normalize_child_post(child, index))
        if children:
            return children

    sidecar = node.get("edge_sidecar_to_children")
    for index, edge in enumerate(_edges(sidecar)):
        child = edge.get("node") if isinstance(edge.get("node"), dict) else {}
        children.append(_normalize_child_post(child, index))
    return children


def _normalize_child_post(child: dict[str, Any], index: int) -> InstagramChildPost:
    media_variants = _extract_media_variants(child)
    width, height = _extract_dimensions(child, media_variants)
    return InstagramChildPost(
        slide_index=index,
        source_id=_extract_source_id(child),
        shortcode=_extract_shortcode(child),
        media_type=_determine_media_type(child),
        width=width,
        height=height,
        alt_text=_string_or_none(_first_value(child, "alt", "accessibility_caption", "accessibilityCaption")),
        media_variants=media_variants,
        tagged_users=_extract_tagged_users(child),
    )


def _extract_dimensions(
    node: dict[str, Any],
    media_variants: list[InstagramMediaVariant],
) -> tuple[int | None, int | None]:
    dimensions = node.get("dimensions")
    if isinstance(dimensions, dict):
        width = _coerce_int_or_none(dimensions.get("width"))
        height = _coerce_int_or_none(dimensions.get("height"))
        if width or height:
            return width, height

    width = _coerce_int_or_none(_first_value(node, "dimensionsWidth", "original_width", "width"))
    height = _coerce_int_or_none(_first_value(node, "dimensionsHeight", "original_height", "height"))
    if width or height:
        return width, height

    for variant in media_variants:
        if variant.width or variant.height:
            return variant.width, variant.height
    return None, None


def _extract_thumbnail_url(node: dict[str, Any], media_variants: list[InstagramMediaVariant]) -> str | None:
    explicit = _string_or_none(
        _first_value(
            node,
            "thumbnail_src",
            "thumbnailUrl",
            "thumbnail_url",
            "displayUrl",
            "display_url",
        )
    )
    if explicit:
        return explicit
    for variant in media_variants:
        if variant.is_thumbnail:
            return variant.url
    return media_variants[0].url if media_variants else None


def _determine_media_type(node: dict[str, Any]) -> str | None:
    typename = str(node.get("__typename") or "")
    if typename in {"GraphSidecar", "XDTGraphSidecar"}:
        return "carousel"
    if typename in {"GraphVideo", "XDTGraphVideo"}:
        return "reel" if str(node.get("product_type") or node.get("productType") or "").lower() == "clips" else "video"
    if typename in {"GraphImage", "XDTGraphImage"}:
        return "image"

    product_type = str(_first_value(node, "productType", "product_type") or "").strip().lower()
    if product_type == "clips":
        return "reel"

    actor_type = str(_first_value(node, "type", "media_type_name") or "").strip().lower()
    if actor_type in {"reel", "clips"}:
        return "reel"
    if actor_type in {"sidecar", "carousel"}:
        return "carousel"
    if actor_type in {"image", "video"}:
        return actor_type

    media_type = node.get("media_type")
    if media_type == 8 or node.get("carousel_media") or node.get("carousel_media_count") or node.get("childPosts"):
        return "carousel"
    if media_type == 2 or node.get("is_video") is True or _first_value(node, "videoUrl", "video_url"):
        return "video"
    if _as_list(node.get("video_versions")):
        return "video"
    if media_type == 1:
        return "image"
    return None


def _extract_location(node: dict[str, Any]) -> InstagramLocation | None:
    raw_location = node.get("location") if isinstance(node.get("location"), dict) else {}
    location_id = _string_or_none(
        _first_value(node, "locationId", "location_id") or raw_location.get("id") or raw_location.get("pk")
    )
    name = _string_or_none(_first_value(node, "locationName", "location_name") or raw_location.get("name"))
    if not location_id and not name:
        return None
    return InstagramLocation(
        location_id=location_id,
        name=name,
        slug=_string_or_none(raw_location.get("slug")),
        address=_string_or_none(raw_location.get("address")),
        latitude=_coerce_float(_first_value(raw_location, "lat", "latitude")),
        longitude=_coerce_float(_first_value(raw_location, "lng", "longitude")),
    )


def _extract_flags(node: dict[str, Any]) -> dict[str, bool]:
    aliases = {
        "comments_disabled": ("comments_disabled", "isCommentsDisabled", "commenting_disabled"),
        "like_count_disabled": (
            "like_count_disabled",
            "like_and_view_counts_disabled",
            "likeAndViewCountsDisabled",
            "hide_like_and_view_counts",
        ),
        "paid_partnership": ("is_paid_partnership", "isPaidPartnership", "sponsored"),
        "advertisement": ("isAdvertisement", "is_advertisement"),
        "reshare": ("is_reshare", "isReshare"),
        "viewer_has_liked": ("has_liked", "hasLiked"),
        "viewer_has_saved": ("has_viewer_saved", "hasViewerSaved"),
    }
    flags: dict[str, bool] = {}
    for name, keys in aliases.items():
        for key in keys:
            if key in node:
                value = _coerce_bool_or_none(node.get(key))
                if value is not None:
                    flags[name] = value
                break
    return flags


def _extract_music_info(node: dict[str, Any]) -> dict[str, Any] | None:
    music = _first_value(node, "musicInfo", "music_info")
    if isinstance(music, dict):
        return music
    clips_metadata = node.get("clips_metadata")
    if isinstance(clips_metadata, dict) and isinstance(clips_metadata.get("music_info"), dict):
        return clips_metadata["music_info"]
    return None


def _extract_context_items(node: dict[str, Any]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for key in ("context_items", "contextItems", "media_notes", "mediaNotes"):
        value = node.get(key)
        if isinstance(value, list):
            items.extend(item for item in value if isinstance(item, dict))
    for key in ("contextual_highlight_info", "contextualHighlight", "reshare_context", "sharing_friction_info"):
        value = node.get(key)
        if isinstance(value, dict):
            items.append({"source": key, **value})
    return items


def _detect_comment_sample_keys(node: dict[str, Any]) -> list[str]:
    keys = [key for key in _COMMENT_SAMPLE_KEYS if key in node]
    comments_edge = node.get("edge_media_to_comment")
    if isinstance(comments_edge, dict) and comments_edge.get("edges"):
        keys.append("edge_media_to_comment.edges")
    return keys


def _extract_comment_author(payload: dict[str, Any]) -> InstagramCommentAuthor:
    owner = payload.get("owner") if isinstance(payload.get("owner"), dict) else {}
    user = payload.get("user") if isinstance(payload.get("user"), dict) else {}
    return InstagramCommentAuthor(
        username=_string_or_none(
            _first_value(payload, "ownerUsername", "owner_username") or owner.get("username") or user.get("username")
        ),
        user_id=_string_or_none(
            _first_value(payload, "ownerId", "owner_id")
            or owner.get("id")
            or owner.get("pk")
            or user.get("id")
            or user.get("pk")
        ),
        full_name=_string_or_none(
            owner.get("full_name") or owner.get("fullName") or user.get("full_name") or user.get("fullName")
        ),
        profile_pic_url=_pick_profile_pic(
            _first_value(payload, "ownerProfilePicUrlHd", "ownerProfilePicUrl"),
            owner.get("profile_pic_url_hd"),
            owner.get("profilePicUrlHd"),
            owner.get("profile_pic_url"),
            owner.get("profilePicUrl"),
            user.get("profile_pic_url_hd"),
            user.get("profilePicUrlHd"),
            user.get("profile_pic_url"),
            user.get("profilePicUrl"),
        ),
        profile_pic_url_hd=_string_or_none(
            _first_value(payload, "ownerProfilePicUrlHd")
            or owner.get("profile_pic_url_hd")
            or owner.get("profilePicUrlHd")
            or user.get("profile_pic_url_hd")
            or user.get("profilePicUrlHd")
        ),
        is_verified=_coerce_bool_or_none(
            owner.get("is_verified") if "is_verified" in owner else user.get("is_verified")
        ),
    )


def _extract_reply_rows(payload: dict[str, Any]) -> list[Any]:
    replies = payload.get("replies")
    child_comments = payload.get("child_comments")
    rows: list[Any] = []
    if isinstance(replies, list):
        rows.extend(replies)
    if isinstance(child_comments, list):
        rows.extend(child_comments)
    return rows


def _extract_position(
    primary: dict[str, Any],
    secondary: dict[str, Any] | None = None,
    *,
    source_prefix: str,
) -> tuple[float | None, float | None, str | None]:
    candidates = [primary]
    if secondary:
        candidates.append(secondary)
    for idx, item in enumerate(candidates):
        position = item.get("position")
        if isinstance(position, (list, tuple)) and len(position) >= 2:
            return _normalize_position(position[0], position[1], f"{source_prefix}.position_array")
        if isinstance(position, dict):
            return _normalize_position(
                position.get("x", position.get("left")),
                position.get("y", position.get("top")),
                f"{source_prefix}.position_object",
            )
        direct = _normalize_position(item.get("x"), item.get("y"), f"{source_prefix}.xy")
        if direct[0] is not None and direct[1] is not None:
            return direct if idx == 0 else (direct[0], direct[1], f"{source_prefix}.edge_xy")
    return None, None, None


def _normalize_position(x_value: Any, y_value: Any, source: str) -> tuple[float | None, float | None, str | None]:
    x = _coerce_float(x_value)
    y = _coerce_float(y_value)
    if x is None or y is None:
        return None, None, None
    return max(0.0, min(1.0, x)), max(0.0, min(1.0, y)), source


def _edges(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, dict):
        return []
    return [edge for edge in value.get("edges") or [] if isinstance(edge, dict)]


def _edge_nodes(value: Any) -> list[dict[str, Any]]:
    nodes: list[dict[str, Any]] = []
    for edge in _edges(value):
        node = edge.get("node")
        if isinstance(node, dict):
            nodes.append(node)
    return nodes


def _first_value(mapping: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in mapping and mapping.get(key) is not None:
            return mapping.get(key)
    return None


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_handle(value: Any) -> str | None:
    text = _string_or_none(value)
    if not text:
        return None
    text = text.lstrip("@#")
    match = re.search(r"[A-Za-z0-9._]+", text)
    if not match:
        return None
    return match.group(0).rstrip(".,:;!?)]}>'\"") or None


def _normalize_mention(value: Any) -> str | None:
    handle = _normalize_handle(value)
    return f"@{handle}" if handle else None


def _normalize_hashtag(value: Any) -> str | None:
    handle = _normalize_handle(value)
    return handle.replace(".", "") if handle else None


def _dedupe(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(value)
    return out


def _pick_profile_pic(*values: Any) -> str | None:
    for value in values:
        text = _string_or_none(value)
        if text and text.startswith(("http://", "https://")):
            return text
    return None


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


def _coerce_int(value: Any, *, default: int) -> int:
    parsed = _coerce_int_or_none(value)
    return default if parsed is None else parsed


def _coerce_int_or_none(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _coerce_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_timestamp(value: Any) -> int | None:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value).strip()
    if text.isdigit():
        return int(text)
    try:
        return int(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp())
    except ValueError:
        return None


def _timestamp_to_iso(value: int | None) -> str | None:
    if value is None:
        return None
    return datetime.fromtimestamp(value, tz=UTC).isoformat()
