from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

import requests

_DATA_SJS_RE = re.compile(r"<script[^>]*data-sjs[^>]*>(.*?)</script>", re.IGNORECASE | re.DOTALL)
_DURATION_RE = re.compile(
    r'mediaPresentationDuration="PT(?:(?P<h>\d+)H)?(?:(?P<m>\d+)M)?(?:(?P<s>\d+(?:\.\d+)?)S)?"',
    re.IGNORECASE,
)
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


def _extract_shortcode(shortcode_or_url: str) -> str:
    text = str(shortcode_or_url or "").strip()
    if not text:
        return ""
    if "/" not in text:
        return text
    parsed = urlparse(text)
    parts = [part for part in parsed.path.split("/") if part]
    if len(parts) >= 2 and parts[0] in {"p", "reel", "tv"}:
        return parts[1]
    return text


def _iter_data_sjs_payloads(html: str) -> list[dict[str, Any]]:
    payloads: list[dict[str, Any]] = []
    if not html:
        return payloads
    for match in _DATA_SJS_RE.finditer(html):
        body = (match.group(1) or "").strip()
        if not body:
            continue
        try:
            parsed = json.loads(body)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
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


def fetch_permalink_media_item(
    shortcode_or_url: str,
    *,
    session: requests.Session | None = None,
    timeout: tuple[int, int] = (10, 45),
    headers: dict[str, str] | None = None,
    cookies: dict[str, str] | None = None,
) -> dict[str, Any] | None:
    shortcode = _extract_shortcode(shortcode_or_url)
    if not shortcode:
        return None
    url = f"https://www.instagram.com/p/{shortcode}/"
    req_headers = {**_DEFAULT_HEADERS, **(headers or {})}
    client = session or requests.Session()
    response = client.get(url, headers=req_headers, cookies=(cookies or None), timeout=timeout)
    response.raise_for_status()
    payloads = _iter_data_sjs_payloads(response.text or "")
    for payload in payloads:
        found = _find_shortcode_media_item(payload)
        if found is not None:
            return found
    return None


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


def _extract_hashtags_mentions(text: str) -> tuple[list[str], list[str]]:
    hashtags = _normalize_unique([str(tag) for tag in _HASHTAG_RE.findall(text or "")])
    mentions = _normalize_unique([f"@{mention}" for mention in _MENTION_RE.findall(text or "")])
    return hashtags, mentions


def _extract_media_urls(media: dict[str, Any]) -> list[str]:
    urls: list[str] = []

    image_versions = media.get("image_versions2")
    if isinstance(image_versions, dict):
        candidates = image_versions.get("candidates")
        if isinstance(candidates, list):
            for candidate in candidates:
                if isinstance(candidate, dict) and candidate.get("url"):
                    urls.append(str(candidate["url"]))
                    break

    video_versions = media.get("video_versions")
    if isinstance(video_versions, list):
        for version in video_versions:
            if isinstance(version, dict) and version.get("url"):
                urls.append(str(version["url"]))

    carousel_media = media.get("carousel_media")
    if isinstance(carousel_media, list):
        for item in carousel_media:
            if not isinstance(item, dict):
                continue
            image_versions = item.get("image_versions2")
            if isinstance(image_versions, dict):
                candidates = image_versions.get("candidates")
                if isinstance(candidates, list):
                    for candidate in candidates:
                        if isinstance(candidate, dict) and candidate.get("url"):
                            urls.append(str(candidate["url"]))
                            break
            item_video_versions = item.get("video_versions")
            if isinstance(item_video_versions, list):
                for version in item_video_versions:
                    if isinstance(version, dict) and version.get("url"):
                        urls.append(str(version["url"]))
    return _normalize_unique(urls)


def _extract_thumbnail_url(media: dict[str, Any], media_urls: list[str]) -> str | None:
    image_versions = media.get("image_versions2")
    if isinstance(image_versions, dict):
        candidates = image_versions.get("candidates")
        if isinstance(candidates, list):
            for candidate in candidates:
                if isinstance(candidate, dict) and candidate.get("url"):
                    return str(candidate["url"])
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
    )


def fetch_permalink_metadata(
    shortcode_or_url: str,
    *,
    session: requests.Session | None = None,
    timeout: tuple[int, int] = (10, 45),
    headers: dict[str, str] | None = None,
    cookies: dict[str, str] | None = None,
) -> InstagramPermalinkMetadata | None:
    media = fetch_permalink_media_item(
        shortcode_or_url,
        session=session,
        timeout=timeout,
        headers=headers,
        cookies=cookies,
    )
    if not media:
        return None
    return parse_permalink_metadata(media)
