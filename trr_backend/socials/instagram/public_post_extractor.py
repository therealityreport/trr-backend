"""Public Instagram post page extraction.

This module parses no-login public post pages. It only reads static HTML
application/json script payloads and does not resolve Instagram auth or proxies.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import math
import re
from dataclasses import dataclass, field
from html import unescape
from typing import Any
from urllib.parse import urlparse

from trr_backend.socials._scrapling_http_utils import response_text, status_code
from trr_backend.socials.scrapling_transport import DEFAULT_TRANSPORT, build_fetcher

_APPLICATION_JSON_SCRIPT_RE = re.compile(
    r"<script\b(?=[^>]*\btype\s*=\s*['\"]application/json['\"])[^>]*>(?P<body>.*?)</script>",
    re.IGNORECASE | re.DOTALL,
)
_SHORTCODE_RE = re.compile(r"^[A-Za-z0-9_-]{5,64}$")
_HASHTAG_RE = re.compile(r"(?<![\w.])#([A-Za-z0-9_]+)")
_MENTION_RE = re.compile(r"(?<![\w.])@([A-Za-z0-9_.]+)")

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
class PublicPostMediaCandidate:
    url: str
    media_type: str
    source: str
    width: int | None = None
    height: int | None = None
    slide_index: int | None = None
    is_thumbnail: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "url": self.url,
            "media_type": self.media_type,
            "source": self.source,
            "width": self.width,
            "height": self.height,
            "slide_index": self.slide_index,
            "is_thumbnail": self.is_thumbnail,
        }


@dataclass(slots=True)
class PublicInstagramPost:
    shortcode: str
    owner: dict[str, Any] | None
    caption: str
    taken_at: int | None
    like_count: int | None
    comment_count: int | None
    view_count: int | None
    media_type: str | None
    product_type: str | None
    profile_tags: list[str]
    tagged_users_detail: list[dict[str, Any]]
    coauthors: list[str]
    coauthors_detail: list[dict[str, Any]]
    hashtags: list[str]
    mentions: list[str]
    image_candidates: list[PublicPostMediaCandidate]
    video_candidates: list[PublicPostMediaCandidate]
    media_urls: list[str]
    thumbnail_url: str | None
    children: list[dict[str, Any]] = field(default_factory=list)
    raw_media: dict[str, Any] = field(default_factory=dict, repr=False)

    def to_raw_media(self) -> dict[str, Any]:
        payload = dict(self.raw_media)
        payload.update(
            {
                "shortcode": self.shortcode,
                "code": self.shortcode,
                "user": dict(self.owner or {}),
                "owner": dict(self.owner or {}),
                "caption": {"text": self.caption} if self.caption else None,
                "taken_at": self.taken_at,
                "like_count": self.like_count,
                "comment_count": self.comment_count,
                "view_count": self.view_count,
                "media_type_name": self.media_type,
                "product_type": self.product_type,
                "public_post_extractor": {
                    "profile_tags": list(self.profile_tags),
                    "tagged_users_detail": [dict(item) for item in self.tagged_users_detail],
                    "coauthors": list(self.coauthors),
                    "coauthors_detail": [dict(item) for item in self.coauthors_detail],
                    "image_candidates": [candidate.to_dict() for candidate in self.image_candidates],
                    "video_candidates": [candidate.to_dict() for candidate in self.video_candidates],
                    "media_urls": list(self.media_urls),
                    "thumbnail_url": self.thumbnail_url,
                    "children": [dict(child) for child in self.children],
                },
            }
        )
        return payload


def parse_public_post_from_html(html: str, *, shortcode: str) -> PublicInstagramPost | None:
    """Extract a public post detail from application/json script payloads."""

    normalized_shortcode = _normalize_shortcode(shortcode)
    if not normalized_shortcode:
        return None

    best: dict[str, Any] | None = None
    best_score = 0
    for payload in iter_application_json_payloads(html):
        for candidate in _iter_matching_media_candidates(payload, shortcode=normalized_shortcode):
            score = _candidate_score(candidate)
            if score > best_score:
                best = candidate
                best_score = score

    if best is None:
        return None
    return _post_from_media(best, shortcode=normalized_shortcode)


def iter_application_json_payloads(html: str) -> list[Any]:
    payloads: list[Any] = []
    for match in _APPLICATION_JSON_SCRIPT_RE.finditer(html or ""):
        body = unescape(str(match.group("body") or "")).strip()
        if not body:
            continue
        if body.startswith("<!--"):
            body = body[4:].strip()
        if body.endswith("-->"):
            body = body[:-3].strip()
        try:
            payloads.append(json.loads(body))
        except json.JSONDecodeError:
            continue
    return payloads


def fetch_public_post_html(
    shortcode_or_url: str,
    *,
    fetcher: Any | None = None,
    timeout_ms: int = DEFAULT_TRANSPORT.timeout_ms,
    headers: dict[str, str] | None = None,
) -> tuple[str | None, int | None]:
    """Fetch a public post page through Scrapling's static Fetcher.

    This intentionally accepts no cookies and no proxy settings.
    """

    shortcode, preferred_route = extract_shortcode_and_route(shortcode_or_url)
    if not shortcode:
        return None, None
    routes = [preferred_route, *[route for route in ("p", "reel", "tv") if route != preferred_route]]
    client = fetcher or build_fetcher()
    req_headers = {**_DEFAULT_HEADERS, **(headers or {})}
    last_status: int | None = None
    for route in routes:
        url = f"https://www.instagram.com/{route}/{shortcode}/"
        response = _fetch_with_static_fetcher(
            client,
            url,
            headers=req_headers,
            timeout=max(1, int(timeout_ms or DEFAULT_TRANSPORT.timeout_ms)),
        )
        current_status = status_code(response)
        last_status = current_status or last_status
        if current_status >= 400:
            continue
        text = response_text(response)
        if text:
            return text, current_status or None
    return None, last_status


def extract_shortcode_and_route(shortcode_or_url: str) -> tuple[str, str]:
    text = str(shortcode_or_url or "").strip()
    if not text:
        return "", "p"
    if "/" not in text:
        return (_normalize_shortcode(text), "p") if _normalize_shortcode(text) else ("", "p")
    parsed = urlparse(text)
    parts = [part for part in parsed.path.split("/") if part]
    if len(parts) >= 2 and parts[0] in {"p", "reel", "tv"}:
        shortcode = _normalize_shortcode(parts[1])
        return (shortcode, parts[0]) if shortcode else ("", parts[0])
    return "", "p"


def _fetch_with_static_fetcher(fetcher: Any, url: str, **kwargs: Any) -> Any:
    for method_name in ("fetch", "get", "async_fetch"):
        method = getattr(fetcher, method_name, None)
        if not callable(method):
            continue
        result = method(url, **kwargs)
        if inspect.isawaitable(result):
            return asyncio.run(result)
        return result
    raise RuntimeError("Scrapling Fetcher has no supported fetch method")


def _normalize_shortcode(value: Any) -> str:
    text = str(value or "").strip()
    return text if _SHORTCODE_RE.match(text) else ""


def _iter_matching_media_candidates(node: Any, *, shortcode: str) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    stack = [node]
    while stack:
        current = stack.pop()
        if isinstance(current, dict):
            if _node_shortcode(current) == shortcode:
                matches.append(current)
            stack.extend(current.values())
        elif isinstance(current, list):
            stack.extend(current)
    return matches


def _node_shortcode(node: dict[str, Any]) -> str:
    for key in ("shortcode", "code", "short_code"):
        shortcode = _normalize_shortcode(node.get(key))
        if shortcode:
            return shortcode
    for key in ("permalink", "url", "inputUrl", "input_url"):
        value = str(node.get(key) or "").strip()
        if not value:
            continue
        shortcode, _ = extract_shortcode_and_route(value)
        if shortcode:
            return shortcode
    return ""


def _candidate_score(node: dict[str, Any]) -> int:
    score = 1
    for key in ("media_type", "__typename", "product_type", "image_versions2", "video_versions"):
        if node.get(key) is not None:
            score += 2
    if _owner(node):
        score += 4
    if _caption_text(node):
        score += 3
    if _children(node):
        score += 3
    if _image_candidates(node) or _video_candidates(node):
        score += 5
    if _int_from_first(node, "like_count", "comment_count", "view_count", "play_count") is not None:
        score += 2
    return score


def _post_from_media(media: dict[str, Any], *, shortcode: str) -> PublicInstagramPost:
    caption = _caption_text(media)
    hashtags, mentions = _extract_hashtags_mentions(caption)
    owner = _owner(media)
    children = _children(media)
    image_candidates = _all_image_candidates(media, children=children)
    video_candidates = _all_video_candidates(media, children=children)
    media_urls = _selected_media_urls(media, children=children)
    thumbnail_url = _thumbnail_url(media, image_candidates=image_candidates, media_urls=media_urls)
    tagged_detail = _tagged_users_detail(media)
    coauthor_detail = _coauthors_detail(media)
    return PublicInstagramPost(
        shortcode=shortcode,
        owner=owner,
        caption=caption,
        taken_at=_timestamp(media),
        like_count=_count(media, "edge_media_preview_like", "like_count", "likeCount", "likes"),
        comment_count=_count(media, "edge_media_to_comment", "comment_count", "commentCount", "comments"),
        view_count=_count(media, "view_count", "play_count", "video_view_count", "video_play_count", "videoPlayCount"),
        media_type=_media_type(media),
        product_type=_string_or_none(media.get("product_type") or media.get("productType")),
        profile_tags=_normalize_unique([item.get("username") for item in tagged_detail]),
        tagged_users_detail=tagged_detail,
        coauthors=_normalize_unique([item.get("username") for item in coauthor_detail]),
        coauthors_detail=coauthor_detail,
        hashtags=hashtags,
        mentions=mentions,
        image_candidates=image_candidates,
        video_candidates=video_candidates,
        media_urls=media_urls,
        thumbnail_url=thumbnail_url,
        children=[_child_summary(child, index=index) for index, child in enumerate(children)],
        raw_media=dict(media),
    )


def _children(media: dict[str, Any]) -> list[dict[str, Any]]:
    carousel_media = media.get("carousel_media")
    if isinstance(carousel_media, list):
        return [item for item in carousel_media if isinstance(item, dict)]
    sidecar = media.get("edge_sidecar_to_children")
    edges = sidecar.get("edges") if isinstance(sidecar, dict) else None
    if isinstance(edges, list):
        return [edge.get("node") for edge in edges if isinstance(edge, dict) and isinstance(edge.get("node"), dict)]
    child_posts = media.get("childPosts")
    if isinstance(child_posts, list):
        return [item for item in child_posts if isinstance(item, dict)]
    return []


def _media_type(media: dict[str, Any]) -> str | None:
    typename = str(media.get("__typename") or "").strip()
    if typename in {"GraphSidecar", "XDTGraphSidecar"}:
        return "carousel"
    if typename in {"GraphVideo", "XDTGraphVideo"}:
        return "video"
    if typename in {"GraphImage", "XDTGraphImage"}:
        return "image"
    media_type = media.get("media_type")
    if media_type == 8 or _children(media):
        return "carousel"
    if media_type == 2 or media.get("is_video") is True or _video_candidates(media):
        return "video"
    if media_type == 1 or _image_candidates(media):
        return "image"
    return _string_or_none(media.get("media_type_name") or media.get("type"))


def _caption_text(media: dict[str, Any]) -> str:
    caption = media.get("caption")
    if isinstance(caption, dict):
        text = _string_or_none(caption.get("text"))
        if text:
            return text
    elif isinstance(caption, str):
        return caption
    edge_caption = media.get("edge_media_to_caption")
    edges = edge_caption.get("edges") if isinstance(edge_caption, dict) else None
    if isinstance(edges, list):
        for edge in edges:
            node = edge.get("node") if isinstance(edge, dict) else None
            if isinstance(node, dict):
                text = _string_or_none(node.get("text"))
                if text:
                    return text
    return _string_or_none(media.get("accessibility_caption")) or ""


def _owner(media: dict[str, Any]) -> dict[str, Any] | None:
    for key in ("user", "owner"):
        value = media.get(key)
        if isinstance(value, dict):
            owner = _user_detail(value)
            if owner.get("username") or owner.get("user_id"):
                return owner
    caption = media.get("caption")
    if isinstance(caption, dict) and isinstance(caption.get("user"), dict):
        owner = _user_detail(caption["user"])
        if owner.get("username") or owner.get("user_id"):
            return owner
    return None


def _user_detail(user: dict[str, Any]) -> dict[str, Any]:
    hd_profile_pic = user.get("hd_profile_pic_url_info")
    hd_profile_pic_url = hd_profile_pic.get("url") if isinstance(hd_profile_pic, dict) else None
    return {
        "username": _string_or_none(user.get("username")),
        "user_id": _string_or_none(user.get("pk") or user.get("id")),
        "full_name": _string_or_none(user.get("full_name") or user.get("fullName")),
        "is_verified": _bool_or_none(user.get("is_verified") or user.get("isVerified")),
        "profile_pic_url": _string_or_none(user.get("profile_pic_url") or user.get("profilePicUrl")),
        "profile_pic_url_hd": _string_or_none(
            user.get("profile_pic_url_hd") or user.get("profilePicUrlHd") or hd_profile_pic_url
        ),
    }


def _tagged_users_detail(media: dict[str, Any]) -> list[dict[str, Any]]:
    details: list[dict[str, Any]] = []
    seen: set[str] = set()

    usertags = media.get("usertags")
    tagged_rows = usertags.get("in") if isinstance(usertags, dict) else None
    if isinstance(tagged_rows, list):
        for tagged in tagged_rows:
            if not isinstance(tagged, dict) or not isinstance(tagged.get("user"), dict):
                continue
            _append_user_detail(details, seen, tagged["user"], tagged=tagged)

    edge_tags = media.get("edge_media_to_tagged_user")
    edges = edge_tags.get("edges") if isinstance(edge_tags, dict) else None
    if isinstance(edges, list):
        for edge in edges:
            node = edge.get("node") if isinstance(edge, dict) else None
            user = node.get("user") if isinstance(node, dict) else None
            if isinstance(user, dict):
                _append_user_detail(details, seen, user, tagged=node)

    return details


def _coauthors_detail(media: dict[str, Any]) -> list[dict[str, Any]]:
    details: list[dict[str, Any]] = []
    seen: set[str] = set()
    for key in ("coauthor_producers", "invited_coauthor_producers", "coauthorProducers", "collaborators"):
        rows = media.get(key)
        if not isinstance(rows, list):
            continue
        for row in rows:
            if not isinstance(row, dict):
                continue
            user = row if row.get("username") else row.get("user")
            if isinstance(user, dict):
                _append_user_detail(details, seen, user)
    return details


def _append_user_detail(
    details: list[dict[str, Any]],
    seen: set[str],
    user: dict[str, Any],
    *,
    tagged: dict[str, Any] | None = None,
) -> None:
    detail = _user_detail(user)
    username = str(detail.get("username") or "").strip()
    if not username or username.lower() in seen:
        return
    seen.add(username.lower())
    if tagged:
        position = tagged.get("position")
        x = y = None
        if isinstance(position, (list, tuple)) and len(position) >= 2:
            x, y = position[0], position[1]
        elif isinstance(position, dict):
            x, y = position.get("x", position.get("left")), position.get("y", position.get("top"))
        else:
            x, y = tagged.get("x"), tagged.get("y")
        tag_x = _bounded_float(x)
        tag_y = _bounded_float(y)
        if tag_x is not None and tag_y is not None:
            detail["tag_x"] = tag_x
            detail["tag_y"] = tag_y
    details.append(detail)


def _all_image_candidates(media: dict[str, Any], *, children: list[dict[str, Any]]) -> list[PublicPostMediaCandidate]:
    nodes = children or [media]
    candidates: list[PublicPostMediaCandidate] = []
    for index, node in enumerate(nodes):
        candidates.extend(_image_candidates(node, slide_index=index if children else None))
    return _dedupe_candidates(candidates)


def _all_video_candidates(media: dict[str, Any], *, children: list[dict[str, Any]]) -> list[PublicPostMediaCandidate]:
    nodes = children or [media]
    candidates: list[PublicPostMediaCandidate] = []
    for index, node in enumerate(nodes):
        candidates.extend(_video_candidates(node, slide_index=index if children else None))
    return _dedupe_candidates(candidates)


def _image_candidates(node: dict[str, Any], *, slide_index: int | None = None) -> list[PublicPostMediaCandidate]:
    candidates: list[PublicPostMediaCandidate] = []
    image_versions = node.get("image_versions2")
    if isinstance(image_versions, dict):
        direct = _string_or_none(image_versions.get("url"))
        if direct:
            candidates.append(
                PublicPostMediaCandidate(direct, "image", "image_versions2.url", slide_index=slide_index)
            )
        for row in image_versions.get("candidates") if isinstance(image_versions.get("candidates"), list) else []:
            if isinstance(row, dict):
                _append_candidate(candidates, row, "image", "image_versions2.candidates", slide_index)
    for row in node.get("display_resources") if isinstance(node.get("display_resources"), list) else []:
        if isinstance(row, dict):
            _append_candidate(candidates, row, "image", "display_resources", slide_index)
    for key in ("display_url", "thumbnail_src", "thumbnail_url"):
        url = _string_or_none(node.get(key))
        if url:
            candidates.append(PublicPostMediaCandidate(url, "image", key, slide_index=slide_index, is_thumbnail=True))
    return candidates


def _video_candidates(node: dict[str, Any], *, slide_index: int | None = None) -> list[PublicPostMediaCandidate]:
    candidates: list[PublicPostMediaCandidate] = []
    for row in node.get("video_versions") if isinstance(node.get("video_versions"), list) else []:
        if isinstance(row, dict):
            _append_candidate(candidates, row, "video", "video_versions", slide_index)
    for key in ("video_url", "videoUrl"):
        url = _string_or_none(node.get(key))
        if url:
            candidates.append(PublicPostMediaCandidate(url, "video", key, slide_index=slide_index))
    return candidates


def _append_candidate(
    candidates: list[PublicPostMediaCandidate],
    row: dict[str, Any],
    media_type: str,
    source: str,
    slide_index: int | None,
) -> None:
    url = _string_or_none(row.get("url") or row.get("src"))
    if not url:
        return
    candidates.append(
        PublicPostMediaCandidate(
            url=url,
            media_type=media_type,
            source=source,
            width=_int_or_none(row.get("width") or row.get("config_width")),
            height=_int_or_none(row.get("height") or row.get("config_height")),
            slide_index=slide_index,
        )
    )


def _selected_media_urls(media: dict[str, Any], *, children: list[dict[str, Any]]) -> list[str]:
    nodes = children or [media]
    urls: list[str] = []
    for node in nodes:
        best_video = _best_candidate_url(_video_candidates(node), media_type="video")
        if best_video:
            urls.append(best_video)
            continue
        best_image = _best_candidate_url(_image_candidates(node), media_type="image")
        if best_image:
            urls.append(best_image)
    return _normalize_unique(urls)


def _best_candidate_url(candidates: list[PublicPostMediaCandidate], *, media_type: str) -> str | None:
    typed = [candidate for candidate in candidates if candidate.media_type == media_type and candidate.url]
    if not typed:
        return None
    typed.sort(
        key=lambda item: ((item.width or 0) * (item.height or 0), item.width or 0, item.height or 0),
        reverse=True,
    )
    return typed[0].url


def _thumbnail_url(
    media: dict[str, Any],
    *,
    image_candidates: list[PublicPostMediaCandidate],
    media_urls: list[str],
) -> str | None:
    top_level = _best_candidate_url(_image_candidates(media), media_type="image")
    if top_level:
        return top_level
    if image_candidates:
        return _best_candidate_url(image_candidates, media_type="image")
    return media_urls[0] if media_urls else None


def _child_summary(child: dict[str, Any], *, index: int) -> dict[str, Any]:
    images = _image_candidates(child, slide_index=index)
    videos = _video_candidates(child, slide_index=index)
    return {
        "slide_index": index,
        "shortcode": _node_shortcode(child) or None,
        "media_type": _media_type(child),
        "display_url": _best_candidate_url(images, media_type="image"),
        "video_url": _best_candidate_url(videos, media_type="video"),
        "image_candidates": [candidate.to_dict() for candidate in _dedupe_candidates(images)],
        "video_candidates": [candidate.to_dict() for candidate in _dedupe_candidates(videos)],
        "tagged_users_detail": _tagged_users_detail(child),
    }


def _dedupe_candidates(candidates: list[PublicPostMediaCandidate]) -> list[PublicPostMediaCandidate]:
    seen: set[str] = set()
    out: list[PublicPostMediaCandidate] = []
    for candidate in candidates:
        if candidate.url in seen:
            continue
        seen.add(candidate.url)
        out.append(candidate)
    return out


def _timestamp(media: dict[str, Any]) -> int | None:
    return _int_from_first(media, "taken_at", "taken_at_timestamp", "timestamp", "created_time")


def _count(media: dict[str, Any], *keys: str) -> int | None:
    for key in keys:
        value = media.get(key)
        if isinstance(value, dict):
            parsed = _int_or_none(value.get("count"))
        else:
            parsed = _int_or_none(value)
        if parsed is not None:
            return max(0, parsed)
    return None


def _int_from_first(media: dict[str, Any], *keys: str) -> int | None:
    for key in keys:
        parsed = _int_or_none(media.get(key))
        if parsed is not None:
            return parsed
    return None


def _int_or_none(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)) and math.isfinite(float(value)):
        return int(value)
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return int(float(text.replace(",", "")))
    except ValueError:
        return None


def _bool_or_none(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in {"1", "true", "yes"}:
        return True
    if text in {"0", "false", "no"}:
        return False
    return None


def _bounded_float(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    return round(max(0.0, min(1.0, parsed)), 4)


def _string_or_none(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _normalize_unique(values: list[Any]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text:
            continue
        key = text.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(text)
    return out


def _extract_hashtags_mentions(text: str) -> tuple[list[str], list[str]]:
    hashtags = _normalize_unique(list(_HASHTAG_RE.findall(text or "")))
    mentions = _normalize_unique([f"@{mention}" for mention in _MENTION_RE.findall(text or "")])
    return hashtags, mentions


__all__ = [
    "PublicInstagramPost",
    "PublicPostMediaCandidate",
    "extract_shortcode_and_route",
    "fetch_public_post_html",
    "iter_application_json_payloads",
    "parse_public_post_from_html",
]
