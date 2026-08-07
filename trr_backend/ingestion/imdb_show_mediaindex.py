from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlparse

from trr_backend.ingestion.imdb_images import fetch_imdb_mediaindex_html as fetch_imdb_mediaindex_html_fallback
from trr_backend.integrations.imdb.mediaindex_images import (
    fetch_imdb_mediaindex_html as fetch_imdb_mediaindex_html_next,
)
from trr_backend.integrations.imdb.mediaindex_images import (
    fetch_imdb_mediaindex_images,
    fetch_imdb_mediaviewer_tags,
    parse_imdb_mediaindex_html,
)


def _normalize_imdb_type(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    return normalized or None


def _normalize_kind_from_imdb_type(imdb_type: str | None, fallback_kind: str = "media") -> str:
    normalized = (imdb_type or "").strip().lower()
    if not normalized:
        return fallback_kind
    if "still" in normalized or "frame" in normalized:
        return "episode_still"
    if "poster" in normalized:
        return "poster"
    if "publicity" in normalized:
        return "promo"
    return fallback_kind


def _flatten_tags_into_metadata(metadata: dict[str, Any], tags: dict[str, Any]) -> dict[str, Any]:
    merged = dict(metadata)
    people_raw = tags.get("people")
    people: list[Any] = people_raw if isinstance(people_raw, list) else []
    titles_raw = tags.get("titles")
    titles: list[Any] = titles_raw if isinstance(titles_raw, list) else []

    people_names: list[str] = []
    people_imdb_ids: list[str] = []
    for item in people:
        if not isinstance(item, dict):
            continue
        imdb_id = str(item.get("imdb_id") or "").strip()
        name = str(item.get("name") or "").strip()
        if imdb_id:
            people_imdb_ids.append(imdb_id)
        if name:
            people_names.append(name)

    title_names: list[str] = []
    title_imdb_ids: list[str] = []
    for item in titles:
        if not isinstance(item, dict):
            continue
        imdb_id = str(item.get("imdb_id") or "").strip()
        title = str(item.get("title") or "").strip()
        if imdb_id:
            title_imdb_ids.append(imdb_id)
        if title:
            title_names.append(title)

    if people_names:
        merged["people_names"] = list(dict.fromkeys(people_names))
    if people_imdb_ids:
        merged["people_imdb_ids"] = list(dict.fromkeys(people_imdb_ids))
    if title_names:
        merged["title_names"] = list(dict.fromkeys(title_names))
    if title_imdb_ids:
        merged["title_imdb_ids"] = list(dict.fromkeys(title_imdb_ids))

    image_type = _normalize_imdb_type(tags.get("image_type"))
    if image_type:
        merged["imdb_image_type"] = image_type

    caption_plain = tags.get("caption_plain")
    if isinstance(caption_plain, str) and caption_plain.strip():
        merged.setdefault("imdb_caption_plain", caption_plain.strip())

    return merged


def fetch_imdb_show_mediaindex_rows(
    imdb_id: str,
    *,
    show_id: str,
    max_pages: int = 25,
    max_images: int | None = None,
    sleep_ms: int = 0,
    include_tags: bool = True,
) -> list[dict[str, Any]]:
    imdb_id = str(imdb_id or "").strip()
    show_id = str(show_id or "").strip()
    if not imdb_id or not show_id:
        return []

    fetched_at = datetime.now(UTC).isoformat()
    try:
        images = fetch_imdb_mediaindex_images(imdb_id, sleep_ms=sleep_ms, max_pages=max_pages)
    except Exception:
        html = fetch_imdb_mediaindex_html_fallback(imdb_id)
        if not html:
            return []
        images, page_info = parse_imdb_mediaindex_html(html, imdb_id=imdb_id)
        seen_ids = {img.imdb_image_id for img in images}
        cursor = page_info.get("end_cursor")
        has_next = page_info.get("has_next_page")
        pages_fetched = 1

        build_id = page_info.get("build_id")

        while has_next and cursor:
            if max_pages is not None and pages_fetched >= max_pages:
                break
            try:
                html = fetch_imdb_mediaindex_html_next(imdb_id, after_cursor=cursor, build_id=build_id)
            except Exception:
                break
            next_images, next_page = parse_imdb_mediaindex_html(html, imdb_id=imdb_id)
            pages_fetched += 1
            for img in next_images:
                if img.imdb_image_id in seen_ids:
                    continue
                seen_ids.add(img.imdb_image_id)
                images.append(img)
            cursor = next_page.get("end_cursor")
            has_next = next_page.get("has_next_page")
            if sleep_ms > 0:
                from time import sleep

                sleep(sleep_ms / 1000.0)
    rows: list[dict[str, Any]] = []

    for idx, image in enumerate(images, start=1):
        if max_images is not None and len(rows) >= max_images:
            break
        parsed = urlparse(image.url)
        metadata = dict(image.metadata or {})
        metadata.setdefault("viewer_url", image.viewer_url)
        metadata.setdefault("viewer_path", image.viewer_path)
        imdb_type = _normalize_imdb_type(image.image_type)
        if imdb_type:
            metadata.setdefault("imdb_image_type", imdb_type)
        if include_tags:
            try:
                tags = fetch_imdb_mediaviewer_tags(imdb_id, image.imdb_image_id, sleep_ms=sleep_ms)
            except Exception:
                tags = {}
            if tags:
                metadata.setdefault("tags", tags)
                metadata = _flatten_tags_into_metadata(metadata, tags)
                imdb_type = _normalize_imdb_type(tags.get("image_type")) or imdb_type
        rows.append(
            {
                "show_id": show_id,
                "source": "imdb",
                "source_image_id": image.imdb_image_id,
                "kind": _normalize_kind_from_imdb_type(imdb_type, "media"),
                "image_type": imdb_type,
                "caption": image.caption,
                "position": image.position or idx,
                "url": image.url,
                "url_path": parsed.path if parsed.path else None,
                "width": image.width,
                "height": image.height,
                "metadata": metadata,
                "fetch_method": "imdb_mediaindex",
                "fetched_from_url": f"https://www.imdb.com/title/{imdb_id}/mediaindex/",
                "fetched_at": fetched_at,
                "updated_at": fetched_at,
            }
        )

    return rows
