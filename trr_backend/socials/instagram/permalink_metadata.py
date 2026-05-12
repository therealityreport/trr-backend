from __future__ import annotations

import json
import math
import os
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from html import unescape
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

import requests

_DATA_SJS_RE = re.compile(r"<script[^>]*data-sjs[^>]*>(.*?)</script>", re.IGNORECASE | re.DOTALL)
_WRAPPED_JSON_CALL_RE = re.compile(r"^[A-Za-z0-9_$.]+\([^,]*,\s*(\{.*\})\s*\)\s*;?$", re.DOTALL)
_SHARED_DATA_RE = re.compile(r"window\._sharedData\s*=\s*(\{.*?\})\s*;", re.DOTALL)
_ADDITIONAL_DATA_RE = re.compile(
    r"__additionalDataLoaded\s*\(\s*['\"].*?['\"]\s*,\s*(\{.*?\})\s*\)\s*;?",
    re.DOTALL,
)
_LD_JSON_RE = re.compile(
    r"<script[^>]+type=['\"]application/ld\+json['\"][^>]*>(.*?)</script>",
    re.IGNORECASE | re.DOTALL,
)
_OG_IMAGE_RE = re.compile(
    r"<meta\s+property=['\"]og:image['\"]\s+content=['\"](.*?)['\"]",
    re.IGNORECASE,
)
_OG_VIDEO_RE = re.compile(
    r"<meta\s+property=['\"]og:video['\"]\s+content=['\"](.*?)['\"]",
    re.IGNORECASE,
)
_SHORTCODE_RE = re.compile(r"^[A-Za-z0-9_-]{5,32}$")
_DURATION_RE = re.compile(
    r'mediaPresentationDuration="PT(?:(?P<h>\d+)H)?(?:(?P<m>\d+)M)?(?:(?P<s>\d+(?:\.\d+)?)S)?"',
    re.IGNORECASE,
)
_HASHTAG_RE = re.compile(r"(?<![\w.])#([A-Za-z0-9_]+)")
_MENTION_RE = re.compile(r"(?<![\w.])@([A-Za-z0-9_.]+)")

_GRAPHQL_URL = "https://www.instagram.com/graphql/query/"
_MEDIA_INFO_URL = "https://www.instagram.com/api/v1/media/{media_id}/info/"
_SHORTCODE_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
_SHORTCODE_CHAR_MAP = {char: index for index, char in enumerate(_SHORTCODE_ALPHABET)}
_DEFAULT_GRAPHQL_SHORTCODE_DOC_IDS = (
    "27075730382013528",
    "27017844554484188",
    "8845758582119845",
)
_DEFAULT_GRAPHQL_SHORTCODE_DOC_ID = _DEFAULT_GRAPHQL_SHORTCODE_DOC_IDS[-1]
_DEFAULT_POST_ROOT_DOC_ID = "26767101476259141"
_POST_ROOT_FRIENDLY_NAME = "PolarisPostRootQuery"

_DEFAULT_HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "accept-language": "en-US,en;q=0.9",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
}


@dataclass(slots=True)
class InstagramPermalinkMetadata:
    taken_at: datetime | None
    post_format: str
    profile_tags: list[str]
    collaborators: list[str]
    hashtags: list[str]
    mentions: list[str]
    duration_seconds: int | None
    media_type: str | None
    media_urls: list[str]
    thumbnail_url: str | None
    raw_media: dict[str, Any]
    # Rich user detail objects (None = not extracted; [] = extracted but empty)
    tagged_users_detail: list[dict[str, Any]] | None = None
    collaborators_detail: list[dict[str, Any]] | None = None
    child_posts_data: list[dict[str, Any]] | None = None


@dataclass(slots=True)
class InstagramMediaResolution:
    source: str | None
    media_type: str | None
    media_urls: list[str]
    thumbnail_url: str | None
    metadata: InstagramPermalinkMetadata | None
    attempts: list[dict[str, Any]]


@dataclass(slots=True)
class InstagramFacebookCrosspostMetadata:
    comments_count: int | None
    likes_count: int | None
    is_shared_to_fb: bool | None
    crosspost_metadata: dict[str, Any]
    social_context: dict[str, Any]
    facebook_post_id: str | None
    facebook_post_url: str | None
    observed_at: datetime
    source: str
    doc_id_used: str | None
    auth_state: str
    raw_media: dict[str, Any]

    def to_payload(self) -> dict[str, Any]:
        return {
            "comments_count": self.comments_count,
            "likes_count": self.likes_count,
            "is_shared_to_fb": self.is_shared_to_fb,
            "post_id": self.facebook_post_id,
            "post_url": self.facebook_post_url,
            "metadata": dict(self.crosspost_metadata or {}),
            "social_context": dict(self.social_context or {}),
            "observed_at": self.observed_at.isoformat(),
            "source": self.source,
            "doc_id_used": self.doc_id_used,
            "auth_state": self.auth_state,
        }


def _normalize_unique(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for raw in values:
        value = str(raw or "").strip()
        if not value:
            continue
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(value)
    return out


def _coerce_non_negative_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)) and math.isfinite(float(value)):
        return max(0, int(value))
    text = str(value or "").strip()
    if not text:
        return None
    digits = re.sub(r"[^0-9]", "", text)
    if not digits:
        return None
    try:
        return max(0, int(digits))
    except ValueError:
        return None


def _coerce_optional_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in {"true", "1", "yes"}:
        return True
    if text in {"false", "0", "no"}:
        return False
    return None


def _normalize_tag_coord(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    coord: float | None = None
    if isinstance(value, (int, float)):
        coord = float(value)
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            coord = float(text)
        except ValueError:
            return None
    if coord is None or not math.isfinite(coord):
        return None
    return round(min(1.0, max(0.0, coord)), 4)


def _normalized_tag_position(
    x_value: Any,
    y_value: Any,
    *,
    source: str,
) -> tuple[float, float, str] | None:
    x = _normalize_tag_coord(x_value)
    y = _normalize_tag_coord(y_value)
    if x is None or y is None:
        return None
    return (x, y, source)


def _tag_position_from_array(value: Any, *, source: str) -> tuple[float, float, str] | None:
    if not isinstance(value, (list, tuple)) or len(value) < 2:
        return None
    return _normalized_tag_position(value[0], value[1], source=source)


def _tag_position_from_object(value: Any, *, source: str) -> tuple[float, float, str] | None:
    if not isinstance(value, dict):
        return None
    return _normalized_tag_position(
        value.get("x", value.get("left")),
        value.get("y", value.get("top")),
        source=source,
    )


def _extract_rest_tag_position(tagged: dict[str, Any]) -> tuple[float, float, str] | None:
    position = tagged.get("position")
    return _tag_position_from_array(position, source="rest_usertags.position_array") or _tag_position_from_object(
        position, source="rest_usertags.position_object"
    )


def _extract_graphql_tag_position(edge: dict[str, Any], edge_node: dict[str, Any]) -> tuple[float, float, str] | None:
    return (
        _tag_position_from_array(edge_node.get("position"), source="graphql_node.position_array")
        or _tag_position_from_object(edge_node.get("position"), source="graphql_node.position_object")
        or _normalized_tag_position(edge_node.get("x"), edge_node.get("y"), source="graphql_node.xy")
        or _tag_position_from_array(edge.get("position"), source="graphql_edge.position_array")
        or _tag_position_from_object(edge.get("position"), source="graphql_edge.position_object")
        or _normalized_tag_position(edge.get("x"), edge.get("y"), source="graphql_edge.xy")
    )


def _extract_shortcode_and_route(shortcode_or_url: str) -> tuple[str, str]:
    text = str(shortcode_or_url or "").strip()
    if not text:
        return "", "p"
    if "/" not in text:
        return (text, "p") if _SHORTCODE_RE.match(text) else ("", "p")
    parsed = urlparse(text)
    parts = [part for part in parsed.path.split("/") if part]
    if len(parts) >= 2 and parts[0] in {"p", "reel", "tv"}:
        candidate = str(parts[1] or "").strip()
        return (candidate, parts[0]) if _SHORTCODE_RE.match(candidate) else ("", parts[0])
    return "", "p"


def _decode_data_sjs_payload(body: str) -> dict[str, Any] | None:
    stripped = str(body or "").strip()
    if not stripped:
        return None

    candidates: list[str] = [stripped]
    if stripped.endswith(";"):
        candidates.append(stripped[:-1].strip())
    if stripped.startswith("for (;;);"):
        candidates.append(stripped[len("for (;;);") :].strip())
    wrapped = _WRAPPED_JSON_CALL_RE.match(stripped)
    if wrapped:
        candidates.append(str(wrapped.group(1) or "").strip())

    seen: set[str] = set()
    for candidate in candidates:
        normalized = candidate.strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        try:
            parsed = json.loads(normalized)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            return parsed
    return None


def _iter_data_sjs_payloads(html: str) -> list[dict[str, Any]]:
    payloads: list[dict[str, Any]] = []
    if not html:
        return payloads
    for match in _DATA_SJS_RE.finditer(html):
        body = (match.group(1) or "").strip()
        if not body:
            continue
        parsed = _decode_data_sjs_payload(body)
        if parsed is not None:
            payloads.append(parsed)
    return payloads


def _find_shortcode_media_item(node: Any) -> dict[str, Any] | None:
    if isinstance(node, dict):
        media_info = node.get("xdt_api__v1__media__shortcode__web_info")
        if isinstance(media_info, dict):
            items = media_info.get("items")
            if isinstance(items, list) and items and isinstance(items[0], dict):
                return items[0]
        for value in node.values():
            found = _find_shortcode_media_item(value)
            if found is not None:
                return found
        return None
    if isinstance(node, list):
        for value in node:
            found = _find_shortcode_media_item(value)
            if found is not None:
                return found
    return None


def _iter_nested_dicts(node: Any) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    stack = [node]
    while stack:
        current = stack.pop()
        if isinstance(current, dict):
            out.append(current)
            stack.extend(current.values())
        elif isinstance(current, list):
            stack.extend(current)
    return out


def _extract_post_root_doc_ids_from_payload(payload: dict[str, Any]) -> list[str]:
    doc_ids: list[str] = []
    for item in _iter_nested_dicts(payload):
        expected = item.get("expectedPreloaders")
        if isinstance(expected, list):
            for preloader in expected:
                if not isinstance(preloader, dict):
                    continue
                serialized = json.dumps(preloader, sort_keys=True, default=str)
                if _POST_ROOT_FRIENDLY_NAME not in serialized:
                    continue
                query_id = str(
                    preloader.get("queryID")
                    or preloader.get("queryId")
                    or preloader.get("doc_id")
                    or preloader.get("docId")
                    or ""
                ).strip()
                if query_id and query_id not in doc_ids:
                    doc_ids.append(query_id)
        serialized = json.dumps(item, sort_keys=True, default=str)
        if _POST_ROOT_FRIENDLY_NAME not in serialized:
            continue
        query_id = str(
            item.get("queryID") or item.get("queryId") or item.get("doc_id") or item.get("docId") or ""
        ).strip()
        if query_id and query_id not in doc_ids:
            doc_ids.append(query_id)
    return doc_ids


def _post_root_doc_ids_from_html(html: str) -> list[str]:
    doc_ids: list[str] = []
    for payload in _iter_data_sjs_payloads(html):
        for doc_id in _extract_post_root_doc_ids_from_payload(payload):
            if doc_id not in doc_ids:
                doc_ids.append(doc_id)
    return doc_ids


def _post_root_doc_ids_from_env_and_fallback() -> list[str]:
    ids: list[str] = []
    override = str(os.getenv("INSTAGRAM_POST_ROOT_GRAPHQL_DOC_ID") or "").strip()
    if override:
        ids.append(override)
    if _DEFAULT_POST_ROOT_DOC_ID not in ids:
        ids.append(_DEFAULT_POST_ROOT_DOC_ID)
    return ids


def _extract_facebook_url_from_crosspost_payload(*payloads: Any) -> str | None:
    for payload in payloads:
        for item in _iter_nested_dicts(payload):
            for value in item.values():
                if not isinstance(value, str):
                    continue
                text = value.strip()
                if text.startswith("https://www.facebook.com/") or text.startswith("https://facebook.com/"):
                    return text
    return None


def _extract_facebook_post_id_from_crosspost_payload(*payloads: Any) -> str | None:
    preferred_keys = {
        "facebook_post_id",
        "fb_post_id",
        "post_id",
        "postid",
        "target_id",
        "fbid",
        "id",
    }
    for payload in payloads:
        for item in _iter_nested_dicts(payload):
            for raw_key, value in item.items():
                key = str(raw_key or "").strip().lower()
                if key not in preferred_keys:
                    continue
                text = str(value or "").strip()
                if text and "facebook.com" not in text:
                    return text
    return None


def _facebook_crosspost_metadata_from_media(
    media: dict[str, Any],
    *,
    observed_at: datetime,
    doc_id_used: str | None,
    auth_state: str,
) -> InstagramFacebookCrosspostMetadata | None:
    comments_count = _coerce_non_negative_int(media.get("fb_comment_count"))
    likes_count = _coerce_non_negative_int(media.get("fb_like_count"))
    is_shared_to_fb = _coerce_optional_bool(media.get("is_shared_to_fb"))
    crosspost_metadata = media.get("crosspost_metadata") if isinstance(media.get("crosspost_metadata"), dict) else {}
    social_context = media.get("social_context") if isinstance(media.get("social_context"), dict) else {}
    has_field = (
        comments_count is not None
        or likes_count is not None
        or is_shared_to_fb is not None
        or bool(crosspost_metadata)
        or bool(social_context)
    )
    if not has_field:
        return None
    return InstagramFacebookCrosspostMetadata(
        comments_count=comments_count,
        likes_count=likes_count,
        is_shared_to_fb=is_shared_to_fb,
        crosspost_metadata=dict(crosspost_metadata),
        social_context=dict(social_context),
        facebook_post_id=_extract_facebook_post_id_from_crosspost_payload(crosspost_metadata, social_context),
        facebook_post_url=_extract_facebook_url_from_crosspost_payload(crosspost_metadata, social_context),
        observed_at=observed_at,
        source=_POST_ROOT_FRIENDLY_NAME,
        doc_id_used=doc_id_used,
        auth_state=auth_state,
        raw_media=dict(media),
    )


def fetch_permalink_media_item(
    shortcode_or_url: str,
    *,
    session: requests.Session | None = None,
    timeout: tuple[int, int] = (10, 45),
    headers: dict[str, str] | None = None,
    cookies: dict[str, str] | None = None,
) -> dict[str, Any] | None:
    shortcode, preferred_route = _extract_shortcode_and_route(shortcode_or_url)
    if not shortcode:
        return None
    req_headers = {**_DEFAULT_HEADERS, **(headers or {})}
    client = session or requests.Session()
    routes = [preferred_route, *[route for route in ("p", "reel", "tv") if route != preferred_route]]
    last_request_error: requests.RequestException | None = None
    had_success_response = False
    for route in routes:
        url = f"https://www.instagram.com/{route}/{shortcode}/"
        try:
            response = client.get(url, headers=req_headers, cookies=(cookies or None), timeout=timeout)
            response.raise_for_status()
            had_success_response = True
        except requests.RequestException as exc:
            last_request_error = exc
            continue

        payloads = _iter_data_sjs_payloads(response.text or "")
        for payload in payloads:
            found = _find_shortcode_media_item(payload)
            if found is not None:
                return found

    if not had_success_response and last_request_error is not None:
        raise last_request_error
    return None


def _candidate_resolution_score(value: Any) -> tuple[int, int]:
    if not isinstance(value, dict):
        return (0, 0)
    width = int(value.get("width") or value.get("config_width") or 0)
    height = int(value.get("height") or value.get("config_height") or 0)
    return width, height


def _pick_best_url(candidates: Any) -> str | None:
    if not isinstance(candidates, list):
        return None
    best: dict[str, Any] | None = None
    for candidate in candidates:
        if not isinstance(candidate, dict) or not candidate.get("url"):
            continue
        if best is None:
            best = candidate
            continue
        if _candidate_resolution_score(candidate) > _candidate_resolution_score(best):
            best = candidate
    return str(best.get("url") or "").strip() if best else None


def _best_image_url(node: dict[str, Any]) -> str | None:
    image_versions = node.get("image_versions2")
    if isinstance(image_versions, dict):
        direct = str(image_versions.get("url") or "").strip()
        if direct:
            return direct
        best = _pick_best_url(image_versions.get("candidates"))
        if best:
            return best
    display_resources = node.get("display_resources")
    best_display = _pick_best_url(display_resources)
    if best_display:
        return best_display
    for key in ("display_url", "thumbnail_src"):
        value = str(node.get(key) or "").strip()
        if value:
            return value
    return None


def _best_video_url(node: dict[str, Any]) -> str | None:
    best = _pick_best_url(node.get("video_versions"))
    if best:
        return best
    for key in ("video_url",):
        value = str(node.get(key) or "").strip()
        if value:
            return value
    return None


def _media_url_for_node(node: dict[str, Any]) -> str | None:
    if not isinstance(node, dict):
        return None
    video_url = _best_video_url(node)
    if video_url:
        return video_url
    return _best_image_url(node)


def _classify_post_format(media: dict[str, Any]) -> str:
    if str(media.get("product_type") or "").strip().lower() == "clips":
        return "reel"
    carousel_media = media.get("carousel_media")
    carousel_count = media.get("carousel_media_count")
    if isinstance(carousel_media, list) and carousel_media:
        return "carousel"
    if isinstance(carousel_count, int) and carousel_count > 1:
        return "carousel"
    return "post"


def _extract_caption_text(media: dict[str, Any]) -> str:
    caption = media.get("caption")
    if isinstance(caption, dict):
        return str(caption.get("text") or "")
    if isinstance(caption, str):
        return caption
    return ""


def _extract_profile_tags(media: dict[str, Any]) -> list[str]:
    tags: list[str] = []
    usertags = media.get("usertags")
    if not isinstance(usertags, dict):
        return []
    for tagged in usertags.get("in", []) if isinstance(usertags.get("in"), list) else []:
        if not isinstance(tagged, dict):
            continue
        user = tagged.get("user")
        if isinstance(user, dict) and user.get("username"):
            tags.append(str(user["username"]))
    return _normalize_unique(tags)


def _extract_collaborators(media: dict[str, Any]) -> list[str]:
    collabs: list[str] = []
    for key in ("coauthor_producers", "invited_coauthor_producers"):
        values = media.get(key)
        if not isinstance(values, list):
            continue
        for item in values:
            if not isinstance(item, dict):
                continue
            username = item.get("username")
            if not username and isinstance(item.get("user"), dict):
                username = item["user"].get("username")
            if username:
                collabs.append(str(username))
    return _normalize_unique(collabs)


def _extract_tagged_users_detail(media: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract rich tagged-user objects from REST API usertags."""
    details: list[dict[str, Any]] = []
    seen: set[str] = set()
    usertags = media.get("usertags")
    if not isinstance(usertags, dict):
        return details
    for tagged in usertags.get("in", []) if isinstance(usertags.get("in"), list) else []:
        if not isinstance(tagged, dict):
            continue
        user = tagged.get("user")
        if not isinstance(user, dict):
            continue
        username = str(user.get("username") or "").strip()
        if not username or username.lower() in seen:
            continue
        seen.add(username.lower())
        detail = {
            "username": username,
            "user_id": str(user.get("pk") or user.get("id") or "") or None,
            "full_name": str(user.get("full_name") or "").strip() or None,
            "is_verified": bool(user.get("is_verified")) if user.get("is_verified") is not None else None,
            "profile_pic_url": str(user.get("profile_pic_url") or "").strip() or None,
        }
        position = _extract_rest_tag_position(tagged)
        if position is not None:
            detail["tag_x"], detail["tag_y"], detail["tag_position_source"] = position
        details.append(detail)
    return details


def _extract_collaborators_detail(media: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract rich collaborator-user objects from REST API coauthor fields."""
    details: list[dict[str, Any]] = []
    seen: set[str] = set()
    for key in ("coauthor_producers", "invited_coauthor_producers"):
        values = media.get(key)
        if not isinstance(values, list):
            continue
        for item in values:
            if not isinstance(item, dict):
                continue
            user = item if "username" in item else (item.get("user") if isinstance(item.get("user"), dict) else {})
            username = str(user.get("username") or "").strip()
            if not username or username.lower() in seen:
                continue
            seen.add(username.lower())
            details.append(
                {
                    "username": username,
                    "user_id": str(user.get("pk") or user.get("id") or "") or None,
                    "full_name": str(user.get("full_name") or "").strip() or None,
                    "is_verified": bool(user.get("is_verified")) if user.get("is_verified") is not None else None,
                    "profile_pic_url": str(user.get("profile_pic_url") or "").strip() or None,
                }
            )
    return details


def _extract_child_posts_data(media: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract carousel child media details and per-child tagged users."""
    children: list[dict[str, Any]] = []
    carousel_media = media.get("carousel_media")
    if not isinstance(carousel_media, list):
        return children
    for index, item in enumerate(carousel_media):
        if not isinstance(item, dict):
            continue
        child = {
            "slide_index": index,
            "type": "Video" if _best_video_url(item) else "Image",
            "display_url": _best_image_url(item),
            "video_url": _best_video_url(item),
            "width": int(item.get("original_width")) if isinstance(item.get("original_width"), (int, float)) else None,
            "height": (
                int(item.get("original_height")) if isinstance(item.get("original_height"), (int, float)) else None
            ),
            "alt": str(item.get("accessibility_caption") or "").strip() or None,
            "tagged_users_detail": _extract_tagged_users_detail(item),
        }
        children.append(child)
    return children


def _extract_hashtags_mentions(text: str) -> tuple[list[str], list[str]]:
    hashtags = _normalize_unique([str(tag) for tag in _HASHTAG_RE.findall(text or "")])
    mentions = _normalize_unique([f"@{mention}" for mention in _MENTION_RE.findall(text or "")])
    return hashtags, mentions


def _extract_media_urls(media: dict[str, Any]) -> list[str]:
    carousel_media = media.get("carousel_media")
    if isinstance(carousel_media, list) and carousel_media:
        urls = [url for item in carousel_media if isinstance(item, dict) for url in [_media_url_for_node(item)] if url]
        return _normalize_unique(urls)

    primary = _media_url_for_node(media)
    return [primary] if primary else []


def _extract_thumbnail_url(media: dict[str, Any], media_urls: list[str]) -> str | None:
    best_image = _best_image_url(media)
    if best_image:
        return best_image
    return media_urls[0] if media_urls else None


def _find_numeric_field(node: Any, keys: set[str]) -> float | None:
    if isinstance(node, dict):
        for key, value in node.items():
            if key in keys and isinstance(value, (int, float)):
                return float(value)
            found = _find_numeric_field(value, keys)
            if found is not None:
                return found
    elif isinstance(node, list):
        for value in node:
            found = _find_numeric_field(value, keys)
            if found is not None:
                return found
    return None


def _duration_from_video_versions(media: dict[str, Any]) -> int | None:
    versions = media.get("video_versions")
    if not isinstance(versions, list):
        return None
    for version in versions:
        if not isinstance(version, dict):
            continue
        version_url = str(version.get("url") or "").strip()
        if not version_url:
            continue
        parsed = urlparse(version_url)
        raw_efg = (parse_qs(parsed.query).get("efg") or [None])[0]
        if not raw_efg:
            continue
        try:
            decoded = unquote(raw_efg)
            efg_data = json.loads(decoded)
        except Exception:
            continue
        duration_value = _find_numeric_field(efg_data, {"duration_s"})
        if duration_value is None:
            duration_value = _find_numeric_field(efg_data, {"video_duration", "duration"})
        if duration_value and duration_value > 0:
            return int(round(duration_value))
    return None


def _duration_from_mpd(media: dict[str, Any]) -> int | None:
    manifest = str(media.get("video_dash_manifest") or "")
    if not manifest:
        return None
    match = _DURATION_RE.search(manifest)
    if not match:
        return None
    hours = int(match.group("h") or 0)
    minutes = int(match.group("m") or 0)
    seconds = float(match.group("s") or 0)
    total = (hours * 3600) + (minutes * 60) + seconds
    if total <= 0:
        return None
    return int(round(total))


def _extract_duration_seconds(media: dict[str, Any]) -> int | None:
    from_versions = _duration_from_video_versions(media)
    if from_versions is not None:
        return from_versions
    return _duration_from_mpd(media)


def parse_permalink_metadata(media: dict[str, Any]) -> InstagramPermalinkMetadata:
    caption = _extract_caption_text(media)
    hashtags, mentions = _extract_hashtags_mentions(caption)
    media_urls = _extract_media_urls(media)

    raw_taken_at = media.get("taken_at")
    taken_at: datetime | None = None
    if isinstance(raw_taken_at, (int, float)):
        taken_at = datetime.fromtimestamp(int(raw_taken_at), tz=UTC)

    return InstagramPermalinkMetadata(
        taken_at=taken_at,
        post_format=_classify_post_format(media),
        profile_tags=_extract_profile_tags(media),
        collaborators=_extract_collaborators(media),
        hashtags=hashtags,
        mentions=mentions,
        duration_seconds=_extract_duration_seconds(media),
        media_type=str(media.get("media_type")) if media.get("media_type") is not None else None,
        media_urls=media_urls,
        thumbnail_url=_extract_thumbnail_url(media, media_urls),
        raw_media=dict(media),
        tagged_users_detail=_extract_tagged_users_detail(media),
        collaborators_detail=_extract_collaborators_detail(media),
        child_posts_data=_extract_child_posts_data(media),
    )


def _extract_media_item_from_post_info(post_info: Any) -> dict[str, Any] | None:
    if not isinstance(post_info, dict):
        return None
    items = post_info.get("items")
    if isinstance(items, list) and items and isinstance(items[0], dict):
        return items[0]
    nested = post_info.get("data")
    if isinstance(nested, dict):
        nested_items = nested.get("items")
        if isinstance(nested_items, list) and nested_items and isinstance(nested_items[0], dict):
            return nested_items[0]
    return None


def _shortcode_to_media_id(shortcode: str) -> str:
    media_id = 0
    for char in shortcode:
        try:
            media_id = media_id * 64 + _SHORTCODE_CHAR_MAP[char]
        except KeyError as exc:
            raise ValueError(f"Invalid Instagram shortcode character: {char!r}") from exc
    return str(media_id)


def _http_status_from_exception(exc: Exception) -> int | None:
    response = getattr(exc, "response", None)
    status = getattr(response, "status_code", None)
    try:
        return int(status) if status is not None else None
    except (TypeError, ValueError):
        return None


def _resolution_attempt(
    *,
    source: str,
    success: bool,
    reason_code: str | None = None,
    http_status: int | None = None,
    selected_url_count: int = 0,
    error: Exception | str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "source": source,
        "success": bool(success),
        "reason_code": reason_code,
        "http_status": http_status,
        "selected_url_count": max(0, int(selected_url_count)),
    }
    if error:
        payload["error_type"] = error.__class__.__name__ if isinstance(error, Exception) else "Error"
        payload["error_message"] = str(error)[:240]
    return payload


def _graphql_doc_ids() -> list[str]:
    override = str(
        os.getenv("INSTAGRAM_SHORTCODE_GRAPHQL_DOC_IDS") or os.getenv("INSTAGRAM_SHORTCODE_GRAPHQL_DOC_ID") or ""
    ).strip()
    ids: list[str] = []
    if override:
        for candidate in override.split(","):
            normalized = candidate.strip()
            if normalized and normalized not in ids:
                ids.append(normalized)
    for fallback in _DEFAULT_GRAPHQL_SHORTCODE_DOC_IDS:
        if fallback not in ids:
            ids.append(fallback)
    return ids


def _graphql_caption_text(node: dict[str, Any]) -> str:
    edge_caption = node.get("edge_media_to_caption")
    if isinstance(edge_caption, dict):
        edges = edge_caption.get("edges")
        if isinstance(edges, list):
            for edge in edges:
                if not isinstance(edge, dict):
                    continue
                text = str((edge.get("node") or {}).get("text") or "").strip()
                if text:
                    return text
    return str(node.get("accessibility_caption") or "")


def _graphql_node_media_type(node: dict[str, Any]) -> str:
    typename = str(node.get("__typename") or "").strip()
    if typename == "GraphSidecar":
        return "carousel"
    if typename == "GraphVideo" or bool(node.get("is_video")):
        return "video"
    return "image"


def _graphql_node_post_format(node: dict[str, Any]) -> str:
    media_type = _graphql_node_media_type(node)
    if media_type == "carousel":
        return "carousel"
    if str(node.get("product_type") or "").strip().lower() == "clips":
        return "reel"
    return "post"


def _graphql_node_media_urls(node: dict[str, Any]) -> list[str]:
    if not isinstance(node, dict):
        return []
    sidecar = node.get("edge_sidecar_to_children")
    if isinstance(sidecar, dict):
        edges = sidecar.get("edges")
        if isinstance(edges, list) and edges:
            urls = []
            for edge in edges:
                child = (edge or {}).get("node") if isinstance(edge, dict) else None
                if isinstance(child, dict):
                    url = _media_url_for_node(child)
                    if url:
                        urls.append(url)
            return _normalize_unique(urls)
    single = _media_url_for_node(node)
    return [single] if single else []


def _graphql_extract_profile_tags(node: dict[str, Any]) -> list[str]:
    """Extract tagged usernames from a GraphQL node."""
    tags: list[str] = []
    edge_tags = node.get("edge_media_to_tagged_user")
    if isinstance(edge_tags, dict):
        for edge in edge_tags.get("edges", []):
            if not isinstance(edge, dict):
                continue
            user = (edge.get("node") or {}).get("user") or {}
            if isinstance(user, dict):
                username = str(user.get("username") or "").strip()
                if username:
                    tags.append(username)
    # Also check REST-style usertags (some GraphQL responses include both)
    usertags = node.get("usertags")
    if isinstance(usertags, dict):
        for tagged in usertags.get("in", []) if isinstance(usertags.get("in"), list) else []:
            if isinstance(tagged, dict):
                user = tagged.get("user") or {}
                if isinstance(user, dict):
                    username = str(user.get("username") or "").strip()
                    if username:
                        tags.append(username)
    return _normalize_unique(tags)


def _graphql_extract_collaborators(node: dict[str, Any]) -> list[str]:
    """Extract collaborator usernames from a GraphQL node."""
    collabs: list[str] = []
    for key in ("coauthor_producers", "invited_coauthor_producers", "coauthorProducers"):
        values = node.get(key)
        if not isinstance(values, list):
            continue
        for item in values:
            if not isinstance(item, dict):
                continue
            username = item.get("username")
            if not username and isinstance(item.get("user"), dict):
                username = item["user"].get("username")
            if username:
                collabs.append(str(username))
    return _normalize_unique(collabs)


def _graphql_extract_tagged_users_detail(node: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract rich tagged-user objects from a GraphQL node."""
    details: list[dict[str, Any]] = []
    seen: set[str] = set()
    edge_tags = node.get("edge_media_to_tagged_user")
    if isinstance(edge_tags, dict):
        for edge in edge_tags.get("edges", []):
            if not isinstance(edge, dict):
                continue
            user = (edge.get("node") or {}).get("user") or {}
            if not isinstance(user, dict):
                continue
            username = str(user.get("username") or "").strip()
            if not username or username.lower() in seen:
                continue
            seen.add(username.lower())
            detail = {
                "username": username,
                "user_id": str(user.get("id") or user.get("pk") or "") or None,
                "full_name": str(user.get("full_name") or "").strip() or None,
                "is_verified": bool(user.get("is_verified")) if user.get("is_verified") is not None else None,
                "profile_pic_url": str(user.get("profile_pic_url") or "").strip() or None,
            }
            edge_node = edge.get("node") or {}
            if not isinstance(edge_node, dict):
                edge_node = {}
            position = _extract_graphql_tag_position(edge, edge_node)
            if position is not None:
                detail["tag_x"], detail["tag_y"], detail["tag_position_source"] = position
            details.append(detail)
    # Also check REST-style usertags
    usertags = node.get("usertags")
    if isinstance(usertags, dict):
        for tagged in usertags.get("in", []) if isinstance(usertags.get("in"), list) else []:
            if not isinstance(tagged, dict):
                continue
            user = tagged.get("user")
            if not isinstance(user, dict):
                continue
            username = str(user.get("username") or "").strip()
            if not username or username.lower() in seen:
                continue
            seen.add(username.lower())
            detail = {
                "username": username,
                "user_id": str(user.get("pk") or user.get("id") or "") or None,
                "full_name": str(user.get("full_name") or "").strip() or None,
                "is_verified": bool(user.get("is_verified")) if user.get("is_verified") is not None else None,
                "profile_pic_url": str(user.get("profile_pic_url") or "").strip() or None,
            }
            position = _extract_rest_tag_position(tagged)
            if position is not None:
                detail["tag_x"], detail["tag_y"], detail["tag_position_source"] = position
            details.append(detail)
    return details


def _graphql_extract_collaborators_detail(node: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract rich collaborator objects from a GraphQL node."""
    details: list[dict[str, Any]] = []
    seen: set[str] = set()
    for key in ("coauthor_producers", "invited_coauthor_producers", "coauthorProducers"):
        values = node.get(key)
        if not isinstance(values, list):
            continue
        for item in values:
            if not isinstance(item, dict):
                continue
            user = item if "username" in item else (item.get("user") if isinstance(item.get("user"), dict) else {})
            username = str(user.get("username") or "").strip()
            if not username or username.lower() in seen:
                continue
            seen.add(username.lower())
            details.append(
                {
                    "username": username,
                    "user_id": str(user.get("id") or user.get("pk") or "") or None,
                    "full_name": str(user.get("full_name") or "").strip() or None,
                    "is_verified": bool(user.get("is_verified")) if user.get("is_verified") is not None else None,
                    "profile_pic_url": str(user.get("profile_pic_url") or "").strip() or None,
                }
            )
    return details


def _graphql_extract_child_posts_data(node: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract sidecar child media details and per-child tagged users from GraphQL node."""
    children: list[dict[str, Any]] = []
    sidecar = node.get("edge_sidecar_to_children")
    if not isinstance(sidecar, dict):
        return children
    edges = sidecar.get("edges")
    if not isinstance(edges, list):
        return children
    for index, edge in enumerate(edges):
        child = (edge or {}).get("node") if isinstance(edge, dict) else None
        if not isinstance(child, dict):
            continue
        dims = child.get("dimensions")
        width = dims.get("width") if isinstance(dims, dict) else None
        height = dims.get("height") if isinstance(dims, dict) else None
        typename = str(child.get("__typename") or "").strip()
        child_type = typename.replace("Graph", "") if typename else ("Video" if child.get("is_video") else "Image")
        children.append(
            {
                "slide_index": index,
                "type": child_type,
                "display_url": _best_image_url(child),
                "video_url": _best_video_url(child),
                "width": int(width) if isinstance(width, (int, float)) else None,
                "height": int(height) if isinstance(height, (int, float)) else None,
                "alt": str(child.get("accessibility_caption") or "").strip() or None,
                "tagged_users_detail": _graphql_extract_tagged_users_detail(child),
            }
        )
    return children


def _metadata_from_graphql_node(node: dict[str, Any]) -> InstagramPermalinkMetadata | None:
    if not isinstance(node, dict):
        return None
    media_urls = _graphql_node_media_urls(node)
    thumbnail = _best_image_url(node) or (media_urls[0] if media_urls else None)
    caption_text = _graphql_caption_text(node)
    hashtags, mentions = _extract_hashtags_mentions(caption_text)
    taken_at: datetime | None = None
    raw_taken = node.get("taken_at_timestamp")
    if isinstance(raw_taken, (int, float)) and raw_taken > 0:
        taken_at = datetime.fromtimestamp(int(raw_taken), tz=UTC)
    return InstagramPermalinkMetadata(
        taken_at=taken_at,
        post_format=_graphql_node_post_format(node),
        profile_tags=_graphql_extract_profile_tags(node),
        collaborators=_graphql_extract_collaborators(node),
        hashtags=hashtags,
        mentions=mentions,
        duration_seconds=None,
        media_type=_graphql_node_media_type(node),
        media_urls=media_urls,
        thumbnail_url=thumbnail,
        raw_media=dict(node),
        tagged_users_detail=_graphql_extract_tagged_users_detail(node),
        collaborators_detail=_graphql_extract_collaborators_detail(node),
        child_posts_data=_graphql_extract_child_posts_data(node),
    )


def _fetch_shortcode_graphql_node(
    *,
    shortcode: str,
    client: requests.Session,
    timeout: tuple[int, int],
    headers: dict[str, str],
    cookies: dict[str, str] | None,
) -> tuple[dict[str, Any] | None, int | None]:
    body_base = {
        "fb_api_caller_class": "RelayModern",
        "fb_api_req_friendly_name": "PolarisPostActionLoadPostQueryQuery",
        "variables": json.dumps({"shortcode": shortcode}),
    }
    req_headers = {
        **headers,
        "content-type": "application/x-www-form-urlencoded",
        "x-fb-friendly-name": "PolarisPostActionLoadPostQueryQuery",
    }
    last_error: requests.RequestException | None = None
    for doc_id in _graphql_doc_ids():
        body = {**body_base, "doc_id": doc_id}
        try:
            response = client.post(
                _GRAPHQL_URL,
                data=body,
                headers=req_headers,
                cookies=(cookies or None),
                timeout=timeout,
            )
            response.raise_for_status()
            payload = response.json()
        except requests.RequestException as exc:
            last_error = exc
            continue
        node = (payload.get("data") or {}).get("xdt_shortcode_media")
        if isinstance(node, dict):
            return node, response.status_code
    if last_error is not None:
        raise last_error
    return None, None


def _fetch_post_root_graphql_media(
    *,
    shortcode: str,
    doc_id: str,
    client: requests.Session,
    timeout: tuple[int, int],
    headers: dict[str, str],
    cookies: dict[str, str] | None,
) -> dict[str, Any] | None:
    body = {
        "fb_api_caller_class": "RelayModern",
        "fb_api_req_friendly_name": _POST_ROOT_FRIENDLY_NAME,
        "variables": json.dumps({"shortcode": shortcode}),
        "server_timestamps": "true",
        "doc_id": doc_id,
    }
    req_headers = {
        **headers,
        "content-type": "application/x-www-form-urlencoded",
        "x-fb-friendly-name": _POST_ROOT_FRIENDLY_NAME,
        "x-ig-app-id": str(headers.get("x-ig-app-id") or headers.get("X-IG-App-ID") or "936619743392459"),
    }
    csrftoken = (cookies or {}).get("csrftoken")
    if csrftoken:
        req_headers["x-csrftoken"] = str(csrftoken)
    response = client.post(
        _GRAPHQL_URL,
        data=body,
        headers=req_headers,
        cookies=(cookies or None),
        timeout=timeout,
    )
    response.raise_for_status()
    payload = response.json()
    return _find_shortcode_media_item(payload)


def _html_shared_data_node(html: str) -> dict[str, Any] | None:
    shared = _SHARED_DATA_RE.search(html or "")
    if shared:
        try:
            payload = json.loads(shared.group(1))
            post = (((payload.get("entry_data") or {}).get("PostPage") or [{}])[0] or {}).get("graphql") or {}
            node = post.get("shortcode_media")
            if isinstance(node, dict):
                return node
        except Exception:
            pass
    for match in _ADDITIONAL_DATA_RE.finditer(html or ""):
        try:
            payload = json.loads(match.group(1))
        except Exception:
            continue
        node = ((payload.get("graphql") or {}).get("shortcode_media")) if isinstance(payload, dict) else None
        if isinstance(node, dict):
            return node
    return None


def _iter_ld_json_objects(html: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for match in _LD_JSON_RE.finditer(html or ""):
        body = str(match.group(1) or "").strip()
        if not body:
            continue
        try:
            parsed = json.loads(body)
        except Exception:
            continue
        if isinstance(parsed, dict):
            out.append(parsed)
        elif isinstance(parsed, list):
            out.extend(item for item in parsed if isinstance(item, dict))
    return out


def _metadata_from_ld_json(html: str) -> InstagramPermalinkMetadata | None:
    for item in _iter_ld_json_objects(html):
        media_type = "image"
        media_url = ""
        thumbnail_url = ""

        video = item.get("video")
        if isinstance(video, list) and video and isinstance(video[0], dict):
            media_type = "video"
            media_url = str(video[0].get("contentUrl") or "").strip()
            thumbnail_url = str(item.get("thumbnailUrl") or "").strip()
        elif isinstance(video, dict):
            media_type = "video"
            media_url = str(video.get("contentUrl") or "").strip()
            thumbnail_url = str(item.get("thumbnailUrl") or "").strip()
        else:
            image = item.get("image")
            if isinstance(image, str):
                media_url = image.strip()
            elif isinstance(image, list) and image:
                media_url = str(image[0] or "").strip()

        if not media_url:
            continue
        return InstagramPermalinkMetadata(
            taken_at=None,
            post_format="post",
            profile_tags=[],
            collaborators=[],
            hashtags=[],
            mentions=[],
            duration_seconds=None,
            media_type=media_type,
            media_urls=[media_url],
            thumbnail_url=thumbnail_url or media_url,
            raw_media=dict(item),
        )
    return None


def _metadata_from_og_html(html: str) -> InstagramPermalinkMetadata | None:
    og_video = _OG_VIDEO_RE.search(html or "")
    og_image = _OG_IMAGE_RE.search(html or "")
    if og_video:
        video_url = unescape(str(og_video.group(1) or "").strip())
        thumb = unescape(str(og_image.group(1) or "").strip()) if og_image else ""
        if video_url:
            return InstagramPermalinkMetadata(
                taken_at=None,
                post_format="post",
                profile_tags=[],
                collaborators=[],
                hashtags=[],
                mentions=[],
                duration_seconds=None,
                media_type="video",
                media_urls=[video_url],
                thumbnail_url=thumb or video_url,
                raw_media={"og_video": video_url, "og_image": thumb or None},
            )
    if og_image:
        image_url = unescape(str(og_image.group(1) or "").strip())
        if image_url:
            return InstagramPermalinkMetadata(
                taken_at=None,
                post_format="post",
                profile_tags=[],
                collaborators=[],
                hashtags=[],
                mentions=[],
                duration_seconds=None,
                media_type="image",
                media_urls=[image_url],
                thumbnail_url=image_url,
                raw_media={"og_image": image_url},
            )
    return None


def _fetch_permalink_html(
    *,
    shortcode_or_url: str,
    client: requests.Session,
    timeout: tuple[int, int],
    headers: dict[str, str],
    cookies: dict[str, str] | None,
) -> tuple[str | None, int | None]:
    shortcode, preferred_route = _extract_shortcode_and_route(shortcode_or_url)
    if not shortcode:
        return None, None
    routes = [preferred_route, *[route for route in ("p", "reel", "tv") if route != preferred_route]]
    last_error: requests.RequestException | None = None
    for route in routes:
        url = f"https://www.instagram.com/{route}/{shortcode}/"
        try:
            response = client.get(url, headers=headers, cookies=(cookies or None), timeout=timeout)
            response.raise_for_status()
            return response.text or "", response.status_code
        except requests.RequestException as exc:
            last_error = exc
            continue
    if last_error is not None:
        raise last_error
    return None, None


def fetch_instagram_facebook_crosspost_metadata(
    shortcode_or_url: str,
    *,
    session: requests.Session | None = None,
    timeout: tuple[int, int] = (10, 45),
    headers: dict[str, str] | None = None,
    cookies: dict[str, str] | None = None,
) -> InstagramFacebookCrosspostMetadata | None:
    shortcode, _ = _extract_shortcode_and_route(shortcode_or_url)
    if not shortcode:
        return None
    req_headers = {**_DEFAULT_HEADERS, "x-ig-app-id": "936619743392459", **(headers or {})}
    client = session or requests.Session()
    observed_at = datetime.now(tz=UTC)
    html_doc_ids: list[str] = []
    try:
        html, _ = _fetch_permalink_html(
            shortcode_or_url=shortcode,
            client=client,
            timeout=timeout,
            headers=req_headers,
            cookies=cookies,
        )
    except Exception:
        html = None
    if html:
        html_doc_ids = _post_root_doc_ids_from_html(html)
        for payload in _iter_data_sjs_payloads(html):
            media = _find_shortcode_media_item(payload)
            if media is None:
                continue
            metadata = _facebook_crosspost_metadata_from_media(
                media,
                observed_at=observed_at,
                doc_id_used=html_doc_ids[0] if html_doc_ids else None,
                auth_state="inline",
            )
            if metadata and metadata.comments_count is not None:
                return metadata

    doc_ids: list[str] = []
    for doc_id in [*html_doc_ids, *_post_root_doc_ids_from_env_and_fallback()]:
        normalized = str(doc_id or "").strip()
        if normalized and normalized not in doc_ids:
            doc_ids.append(normalized)
    for doc_id in doc_ids:
        try:
            media = _fetch_post_root_graphql_media(
                shortcode=shortcode,
                doc_id=doc_id,
                client=client,
                timeout=timeout,
                headers=req_headers,
                cookies=cookies,
            )
        except requests.RequestException:
            continue
        if not isinstance(media, dict):
            continue
        metadata = _facebook_crosspost_metadata_from_media(
            media,
            observed_at=observed_at,
            doc_id_used=doc_id,
            auth_state="authenticated" if (cookies or {}).get("sessionid") else "anonymous",
        )
        if metadata is not None:
            return metadata
    return None


def _fetch_media_info_via_shortcode(
    *,
    shortcode: str,
    client: requests.Session,
    timeout: tuple[int, int],
    headers: dict[str, str],
    cookies: dict[str, str] | None,
) -> tuple[dict[str, Any] | None, int | None]:
    media_id = _shortcode_to_media_id(shortcode)
    response = client.get(
        _MEDIA_INFO_URL.format(media_id=media_id),
        headers=headers,
        cookies=(cookies or None),
        timeout=timeout,
    )
    response.raise_for_status()
    return response.json(), response.status_code


def resolve_instagram_media(
    shortcode_or_url: str,
    *,
    session: requests.Session | None = None,
    timeout: tuple[int, int] = (10, 45),
    headers: dict[str, str] | None = None,
    cookies: dict[str, str] | None = None,
    fetch_post_info: Callable[[str], dict[str, Any] | None] | None = None,
) -> InstagramMediaResolution:
    shortcode, _ = _extract_shortcode_and_route(shortcode_or_url)
    attempts: list[dict[str, Any]] = []
    if not shortcode:
        attempts.append(
            _resolution_attempt(
                source="api_media_info",
                success=False,
                reason_code="instagram_media_not_found",
            )
        )
        return InstagramMediaResolution(
            source=None,
            media_type=None,
            media_urls=[],
            thumbnail_url=None,
            metadata=None,
            attempts=attempts,
        )

    req_headers = {**_DEFAULT_HEADERS, **(headers or {})}
    client = session or requests.Session()
    html_cache: str | None = None

    # 1) Existing API-based media info path.
    try:
        post_info = fetch_post_info(shortcode) if fetch_post_info is not None else None
        if post_info is None:
            post_info, http_status = _fetch_media_info_via_shortcode(
                shortcode=shortcode,
                client=client,
                timeout=timeout,
                headers=req_headers,
                cookies=cookies,
            )
        else:
            http_status = None
        media_item = _extract_media_item_from_post_info(post_info)
        metadata = parse_permalink_metadata(media_item) if media_item else None
        if metadata and (metadata.media_urls or metadata.thumbnail_url):
            attempts.append(
                _resolution_attempt(
                    source="api_media_info",
                    success=True,
                    selected_url_count=len(metadata.media_urls),
                    http_status=http_status,
                )
            )
            return InstagramMediaResolution(
                source="api_media_info",
                media_type=metadata.media_type,
                media_urls=list(metadata.media_urls),
                thumbnail_url=metadata.thumbnail_url,
                metadata=metadata,
                attempts=attempts,
            )
        attempts.append(
            _resolution_attempt(
                source="api_media_info",
                success=False,
                reason_code="instagram_media_not_found",
                selected_url_count=0,
                http_status=http_status,
            )
        )
    except Exception as exc:  # noqa: BLE001
        attempts.append(
            _resolution_attempt(
                source="api_media_info",
                success=False,
                reason_code="instagram_api_failed",
                http_status=_http_status_from_exception(exc),
                error=exc,
            )
        )

    # 2) HOAIAN-style GraphQL fallback by shortcode.
    try:
        node, status_code = _fetch_shortcode_graphql_node(
            shortcode=shortcode,
            client=client,
            timeout=timeout,
            headers=req_headers,
            cookies=cookies,
        )
        metadata = _metadata_from_graphql_node(node or {})
        if metadata and (metadata.media_urls or metadata.thumbnail_url):
            attempts.append(
                _resolution_attempt(
                    source="graphql_shortcode",
                    success=True,
                    selected_url_count=len(metadata.media_urls),
                    http_status=status_code,
                )
            )
            return InstagramMediaResolution(
                source="graphql_shortcode",
                media_type=metadata.media_type,
                media_urls=list(metadata.media_urls),
                thumbnail_url=metadata.thumbnail_url,
                metadata=metadata,
                attempts=attempts,
            )
        attempts.append(
            _resolution_attempt(
                source="graphql_shortcode",
                success=False,
                reason_code="instagram_media_not_found",
                http_status=status_code,
            )
        )
    except Exception as exc:  # noqa: BLE001
        attempts.append(
            _resolution_attempt(
                source="graphql_shortcode",
                success=False,
                reason_code="instagram_graphql_failed",
                http_status=_http_status_from_exception(exc),
                error=exc,
            )
        )

    # 3) mikesmith-style HTML/JSON extraction fallback.
    try:
        html_cache, html_status = _fetch_permalink_html(
            shortcode_or_url=shortcode_or_url,
            client=client,
            timeout=timeout,
            headers=req_headers,
            cookies=cookies,
        )
        metadata: InstagramPermalinkMetadata | None = None
        media_item = None
        if html_cache:
            payloads = _iter_data_sjs_payloads(html_cache)
            for payload in payloads:
                media_item = _find_shortcode_media_item(payload)
                if media_item is not None:
                    break
            if media_item is not None:
                metadata = parse_permalink_metadata(media_item)
            if metadata is None:
                node = _html_shared_data_node(html_cache)
                metadata = _metadata_from_graphql_node(node or {})
            if metadata is None:
                metadata = _metadata_from_ld_json(html_cache)
        if metadata and (metadata.media_urls or metadata.thumbnail_url):
            attempts.append(
                _resolution_attempt(
                    source="html_json",
                    success=True,
                    selected_url_count=len(metadata.media_urls),
                    http_status=html_status,
                )
            )
            return InstagramMediaResolution(
                source="html_json",
                media_type=metadata.media_type,
                media_urls=list(metadata.media_urls),
                thumbnail_url=metadata.thumbnail_url,
                metadata=metadata,
                attempts=attempts,
            )
        attempts.append(
            _resolution_attempt(
                source="html_json",
                success=False,
                reason_code="instagram_media_not_found",
                http_status=html_status,
            )
        )
    except Exception as exc:  # noqa: BLE001
        attempts.append(
            _resolution_attempt(
                source="html_json",
                success=False,
                reason_code="instagram_html_parse_failed",
                http_status=_http_status_from_exception(exc),
                error=exc,
            )
        )

    # 4) Open Graph fallback.
    if html_cache is None:
        try:
            html_cache, _ = _fetch_permalink_html(
                shortcode_or_url=shortcode_or_url,
                client=client,
                timeout=timeout,
                headers=req_headers,
                cookies=cookies,
            )
        except Exception:
            html_cache = None
    metadata = _metadata_from_og_html(html_cache or "")
    if metadata and (metadata.media_urls or metadata.thumbnail_url):
        attempts.append(
            _resolution_attempt(
                source="og_fallback",
                success=True,
                selected_url_count=len(metadata.media_urls),
            )
        )
        return InstagramMediaResolution(
            source="og_fallback",
            media_type=metadata.media_type,
            media_urls=list(metadata.media_urls),
            thumbnail_url=metadata.thumbnail_url,
            metadata=metadata,
            attempts=attempts,
        )

    attempts.append(
        _resolution_attempt(
            source="og_fallback",
            success=False,
            reason_code="instagram_media_not_found",
        )
    )
    return InstagramMediaResolution(
        source=None,
        media_type=None,
        media_urls=[],
        thumbnail_url=None,
        metadata=None,
        attempts=attempts,
    )


def fetch_permalink_metadata(
    shortcode_or_url: str,
    *,
    session: requests.Session | None = None,
    timeout: tuple[int, int] = (10, 45),
    headers: dict[str, str] | None = None,
    cookies: dict[str, str] | None = None,
) -> InstagramPermalinkMetadata | None:
    resolution = resolve_instagram_media(
        shortcode_or_url,
        session=session,
        timeout=timeout,
        headers=headers,
        cookies=cookies,
        fetch_post_info=None,
    )
    return resolution.metadata
