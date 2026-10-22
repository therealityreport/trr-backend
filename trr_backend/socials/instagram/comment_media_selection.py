"""Select native GIF renditions without changing retained source metadata."""

from collections.abc import Mapping
from typing import Any


def select_comment_media(value: Any) -> Any:
    """Match Instagram's nullish proxied-images preference for GIF comments.

    This is an extraction view only. Evidence and DTO metadata retain both
    branches. An empty nonnull proxied object does not permit fallback.
    """
    if isinstance(value, list):
        return [select_comment_media(item) for item in value]
    if not isinstance(value, Mapping):
        return value
    selected = {}
    for key, item in value.items():
        if key in {"giphy_media_info", "giphyMediaInfo"} and isinstance(item, Mapping):
            images = item.get("first_party_cdn_proxied_images")
            if images is None:
                images = item.get("images")
            else:
                fixed = images.get("fixed_height") if isinstance(images, Mapping) else None
                images = {"fixed_height": {"url": fixed.get("url")}} if isinstance(fixed, Mapping) else {}
            selected[key] = {"images": select_comment_media(images)}
        else:
            selected[key] = select_comment_media(item)
    return selected
