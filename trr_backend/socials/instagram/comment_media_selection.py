"""Select native GIF renditions without changing retained source metadata."""

from collections.abc import Mapping
from typing import Any
from urllib.parse import urlsplit

_UNAVAILABLE_GIF_URL = "https://static.cdninstagram.com/rsrc.php/v4/yr/r/xg_5YoVlvjp.gif"


def _usable_native_gif_url(value: Any) -> bool:
    if (
        not isinstance(value, str)
        or not value.startswith(("https://", "http://"))
        or value != value.strip()
        or value == _UNAVAILABLE_GIF_URL
    ):
        return False
    try:
        parts = urlsplit(value)
        return parts.scheme in {"http", "https"} and bool(parts.hostname) and not parts.username and not parts.password
    except ValueError:
        return False


def select_comment_media(value: Any) -> Any:
    """Match Instagram's nullish proxied-images preference for GIF comments.

    This is an extraction view only. Evidence and DTO metadata retain both
    branches. An empty nonnull proxied object does not permit fallback.
    A selected native GIF also supersedes this mapping's exact known stale
    placeholder references; child attachments cannot supersede parent media.
    """
    if isinstance(value, list):
        return [select_comment_media(item) for item in value]
    if not isinstance(value, Mapping):
        return value
    selected = {}
    has_native_gif = False
    for key, item in value.items():
        if key in {"giphy_media_info", "giphyMediaInfo"} and isinstance(item, Mapping):
            images = item.get("first_party_cdn_proxied_images")
            if images is None:
                images = item.get("images")
            else:
                fixed = images.get("fixed_height") if isinstance(images, Mapping) else None
                images = {"fixed_height": {"url": fixed.get("url")}} if isinstance(fixed, Mapping) else {}
            fixed = images.get("fixed_height") if isinstance(images, Mapping) else None
            url = fixed.get("url") if isinstance(fixed, Mapping) else None
            if _usable_native_gif_url(url):
                has_native_gif = True
            selected[key] = {"images": select_comment_media(images)}
        else:
            selected[key] = select_comment_media(item)
    if has_native_gif:
        for key in ("media_urls", "mediaUrls"):
            if isinstance(selected.get(key), list):
                selected[key] = [url for url in selected[key] if url != _UNAVAILABLE_GIF_URL]
        for key in ("media_url", "mediaUrl"):
            if selected.get(key) == _UNAVAILABLE_GIF_URL:
                selected.pop(key)
    return selected
