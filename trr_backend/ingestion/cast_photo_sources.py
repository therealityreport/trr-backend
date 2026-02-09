"""
Unified cast photo source fetchers.

Each fetcher returns normalized rows ready for upsert into core.cast_photos.
All rows include:
- person_id (UUID)
- source (imdb|tmdb|fandom|fandom-gallery)
- url (non-null)
- url_path
- image_url_canonical (for deduplication)
- source-specific fields
"""

from __future__ import annotations

import hashlib
import re
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlparse
from uuid import UUID

try:
    import requests
except ImportError:
    requests = None


def _canonical_url(url: str) -> str:
    """Create a canonical URL for deduplication (remove query params, lowercase)."""
    if "?" in url:
        url = url.split("?")[0]
    return url.lower().strip()


def _url_path_with_query(raw_url: str | None) -> str | None:
    """Extract path (with query) from a URL."""
    if not raw_url:
        return None
    parsed = urlparse(raw_url)
    if not parsed.path:
        return None
    if parsed.query:
        return f"{parsed.path}?{parsed.query}"
    return parsed.path


def _url_hash(url: str) -> str:
    """Generate a short hash of a URL for source_image_id."""
    return hashlib.sha256(_canonical_url(url).encode()).hexdigest()[:16]


def _normalize_fandom_section_label(value: str | None) -> str | None:
    if not value:
        return None
    cleaned = re.sub(r"[_-]+", " ", value).strip()
    if not cleaned:
        return None
    if cleaned.lower() == cleaned:
        return cleaned.title()
    return cleaned


def _extract_season_number(*values: str | None) -> int | None:
    for value in values:
        if not value:
            continue
        match = re.search(r"\b(?:season|s)\s*([0-9]{1,2})\b", value, re.IGNORECASE)
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                continue
        match = re.search(r"\b([0-9]{1,2})\b", value)
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                continue
    return None


def _infer_fandom_section_tag(*values: str | None) -> str | None:
    text = " ".join([v for v in values if v]).strip().lower()
    if not text:
        return None
    if "confessional" in text:
        return "CONFESSIONAL"
    if "intro" in text or "tagline" in text or "opening" in text:
        return "INTRO"
    if "reunion" in text:
        return "REUNION"
    if "promo" in text or "promotional" in text:
        return "PROMO"
    if "episode" in text or "still" in text:
        return "EPISODE STILL"
    return "OTHER"


def _build_fandom_metadata(
    *,
    section_tag: str | None,
    section_label: str | None,
    source_variant: str | None = None,
    source_page_url: str | None = None,
) -> dict[str, Any] | None:
    metadata: dict[str, Any] = {}
    if section_tag:
        metadata["fandom_section_tag"] = section_tag
    if section_label:
        metadata["fandom_section_label"] = section_label
    if source_variant:
        metadata["source_variant"] = source_variant
    if source_page_url:
        metadata["source_page_url"] = source_page_url
    return metadata or None


# ---------------------------------------------------------------------------
# IMDb Source
# ---------------------------------------------------------------------------


def fetch_imdb_cast_photos(
    imdb_person_id: str,
    person_id: str | UUID,
    *,
    limit: int = 50,
    session: requests.Session | None = None,
    verbose: bool = False,
) -> list[dict[str, Any]]:
    """
    Fetch cast photos from IMDb person gallery.

    Args:
        imdb_person_id: IMDb person ID (nm...)
        person_id: core.people UUID
        limit: Max photos to fetch
        session: Optional requests session for connection reuse
        verbose: Print progress

    Returns:
        List of normalized photo dicts for upsert
    """
    from trr_backend.integrations.imdb.person_gallery import (
        fetch_imdb_person_mediaindex_html,
        fetch_imdb_person_mediaviewer_html,
        parse_imdb_person_mediaindex_images,
        parse_imdb_person_mediaviewer_details,
    )

    try:
        media_html = fetch_imdb_person_mediaindex_html(imdb_person_id, session=session)
        images = parse_imdb_person_mediaindex_images(media_html, imdb_person_id)
    except Exception as exc:
        if verbose:
            print(f"  WARN IMDb mediaindex {imdb_person_id}: {exc}")
        return []

    if not images:
        return []

    images = images[:limit] if limit else images
    mediaindex_url_path = f"/name/{imdb_person_id}/mediaindex/"
    rows: list[dict[str, Any]] = []

    for image in images:
        viewer_id = image.get("viewer_id")
        details: dict[str, Any] = {}

        if viewer_id:
            try:
                viewer_html = fetch_imdb_person_mediaviewer_html(imdb_person_id, viewer_id, session=session)
                details = parse_imdb_person_mediaviewer_details(viewer_html, viewer_id=viewer_id)
            except Exception as exc:
                if verbose:
                    print(f"    WARN mediaviewer {viewer_id}: {exc}")

        # Choose best URL (prefer details if higher resolution)
        url = image.get("url")
        width = image.get("width")
        height = image.get("height")
        url_path = image.get("url_path")

        if details.get("url") and details.get("width"):
            if width is None or details["width"] >= width:
                url = details["url"]
                width = details["width"]
                height = details.get("height")
                url_path = details.get("url_path")

        if not url:
            continue

        source_image_id = str(image.get("source_image_id") or "").strip()
        if not source_image_id:
            continue

        tags: dict[str, Any] = {}
        people_ids = details.get("people_imdb_ids") or []
        people_names = details.get("people_names") or []
        people: list[dict[str, Any]] = []
        for idx, imdb_id in enumerate(people_ids):
            name = people_names[idx] if idx < len(people_names) else None
            people.append({"imdb_id": imdb_id, "name": name})
        if people:
            tags["people"] = people

        title_ids = details.get("title_imdb_ids") or []
        title_names = details.get("title_names") or []
        titles: list[dict[str, Any]] = []
        for idx, imdb_id in enumerate(title_ids):
            title = title_names[idx] if idx < len(title_names) else None
            titles.append({"imdb_id": imdb_id, "title": title})
        if titles:
            tags["titles"] = titles

        caption = details.get("caption")
        if caption:
            tags["caption_plain"] = caption

        metadata: dict[str, Any] | None = None
        if tags:
            metadata = {"tags": tags}

        source_page_url = None
        if viewer_id:
            source_page_url = f"https://www.imdb.com/name/{imdb_person_id}/mediaviewer/{viewer_id}/"
        else:
            source_page_url = f"https://www.imdb.com/name/{imdb_person_id}/mediaindex/"

        if metadata is None:
            metadata = {}
        metadata["source_page_url"] = source_page_url
        metadata["imdb_person_id"] = imdb_person_id
        if viewer_id:
            metadata["imdb_viewer_id"] = viewer_id

        rows.append(
            {
                "person_id": str(person_id),
                "imdb_person_id": imdb_person_id,
                "source": "imdb",
                "source_image_id": source_image_id,
                "viewer_id": viewer_id,
                "mediaindex_url_path": mediaindex_url_path,
                "mediaviewer_url_path": image.get("mediaviewer_url_path"),
                "url": url,
                "url_path": url_path,
                "image_url": url,
                "image_url_canonical": _canonical_url(url),
                "width": width,
                "height": height,
                "caption": details.get("caption"),
                "gallery_index": details.get("gallery_index"),
                "gallery_total": details.get("gallery_total"),
                "people_imdb_ids": details.get("people_imdb_ids"),
                "people_names": details.get("people_names"),
                "title_imdb_ids": details.get("title_imdb_ids"),
                "title_names": details.get("title_names"),
                "source_page_url": source_page_url,
                "metadata": metadata,
                "fetched_at": datetime.now(UTC).isoformat(),
            }
        )

    return rows


# ---------------------------------------------------------------------------
# TMDb Source
# ---------------------------------------------------------------------------


def fetch_tmdb_cast_photos(
    tmdb_person_id: int,
    person_id: str | UUID,
    imdb_person_id: str | None = None,
    *,
    limit: int = 50,
    api_key: str | None = None,
    verbose: bool = False,
) -> list[dict[str, Any]]:
    """
    Fetch cast photos from TMDb person profile images.

    Args:
        tmdb_person_id: TMDb person ID (integer)
        person_id: core.people UUID
        imdb_person_id: Optional IMDb ID for cross-reference
        limit: Max photos to fetch
        api_key: TMDb API key (or from env)
        verbose: Print progress

    Returns:
        List of normalized photo dicts for upsert
    """
    from trr_backend.ingestion.tmdb_person_images import (
        build_tmdb_image_url,
        fetch_tmdb_person_profile_images,
    )

    try:
        images = fetch_tmdb_person_profile_images(tmdb_person_id, api_key=api_key)
    except Exception as exc:
        if verbose:
            print(f"  WARN TMDb person images {tmdb_person_id}: {exc}")
        return []

    if not images:
        return []

    images = images[:limit] if limit else images
    rows: list[dict[str, Any]] = []
    now = datetime.now(UTC).isoformat()

    for idx, img in enumerate(images):
        file_path = img.get("file_path")
        if not file_path:
            continue

        full_url = build_tmdb_image_url(file_path)

        rows.append(
            {
                "person_id": str(person_id),
                "imdb_person_id": imdb_person_id,
                "source": "tmdb",
                "source_image_id": file_path,
                "url": full_url,
                "url_path": file_path,
                "image_url": full_url,
                "image_url_canonical": full_url,
                "width": img.get("width"),
                "height": img.get("height"),
                "position": idx + 1,
                "gallery_index": idx + 1,
                "gallery_total": len(images),
                "fetched_at": now,
                "metadata": {
                    "tmdb_person_id": tmdb_person_id,
                    "aspect_ratio": img.get("aspect_ratio"),
                    "vote_average": img.get("vote_average"),
                    "vote_count": img.get("vote_count"),
                },
            }
        )

    return rows


# ---------------------------------------------------------------------------
# Fandom Person Page Source
# ---------------------------------------------------------------------------


def fetch_fandom_person_cast_photos(
    person_name: str,
    person_id: str | UUID,
    imdb_person_id: str | None = None,
    *,
    limit: int = 50,
    verbose: bool = False,
) -> list[dict[str, Any]]:
    """
    Fetch cast photos from Fandom person wiki page.

    Args:
        person_name: Person name for wiki page lookup
        person_id: core.people UUID
        imdb_person_id: Optional IMDb ID for cross-reference
        limit: Max photos to fetch
        verbose: Print progress

    Returns:
        List of normalized photo dicts for upsert
    """
    from trr_backend.ingestion.fandom_person_scraper import (
        fetch_fandom_person_html,
        parse_fandom_person_html,
    )

    # Build Fandom wiki URL from name
    wiki_name = person_name.replace(" ", "_")
    url = f"https://real-housewives.fandom.com/wiki/{wiki_name}"

    try:
        html, source_page_url = fetch_fandom_person_html(url)
        metadata, photos = parse_fandom_person_html(html, source_url=source_page_url)
        result = {"photos": photos, **metadata}
    except Exception as exc:
        if verbose:
            print(f"  WARN Fandom person {person_name}: {exc}")
        return []

    photos = result.get("photos", [])
    if not photos:
        return []

    photos = photos[:limit] if limit else photos
    rows: list[dict[str, Any]] = []
    now = datetime.now(UTC).isoformat()

    for photo in photos:
        image_url = photo.get("url") or photo.get("image_url")
        if not image_url:
            continue

        section_label = _normalize_fandom_section_label(photo.get("context_section"))
        section_tag = _infer_fandom_section_tag(
            photo.get("context_type"),
            section_label,
            photo.get("caption"),
            photo.get("alt_text"),
        )
        metadata = _build_fandom_metadata(
            section_tag=section_tag,
            section_label=section_label,
            source_page_url=source_page_url,
        )
        if metadata is None:
            metadata = {}
        season_value = photo.get("season")
        if isinstance(season_value, int):
            metadata.setdefault("season_number", season_value)

        # Ensure url and url_path are never null
        url_value = image_url
        url_path = photo.get("url_path") or _url_path_with_query(image_url) or image_url

        rows.append(
            {
                "person_id": str(person_id),
                "imdb_person_id": imdb_person_id,
                "source": "fandom",
                "source_image_id": f"fandom-person-{_url_hash(image_url)}",
                "source_page_url": source_page_url,
                "url": url_value,
                "url_path": url_path,
                "image_url": image_url,
                "thumb_url": photo.get("thumb_url"),
                "image_url_canonical": _canonical_url(image_url),
                "width": photo.get("width"),
                "height": photo.get("height"),
                "caption": photo.get("caption") or photo.get("alt_text"),
                "context_section": photo.get("context_section"),
                "context_type": photo.get("context_type"),
                "season": photo.get("season"),
                "position": photo.get("position"),
                "metadata": metadata,
                "fetched_at": now,
            }
        )

    return rows


# ---------------------------------------------------------------------------
# Fandom Gallery Source
# ---------------------------------------------------------------------------


def fetch_fandom_gallery_cast_photos(
    person_name: str,
    person_id: str | UUID,
    imdb_person_id: str | None = None,
    *,
    limit: int = 50,
    resolve_file_pages: bool = True,
    verbose: bool = False,
) -> list[dict[str, Any]]:
    """
    Fetch cast photos from Fandom gallery page.

    Args:
        person_name: Person name for gallery page lookup
        person_id: core.people UUID
        imdb_person_id: Optional IMDb ID for cross-reference
        limit: Max photos to fetch
        verbose: Print progress

    Returns:
        List of normalized photo dicts for upsert
    """
    from trr_backend.integrations.fandom import fetch_fandom_file_metadata, fetch_fandom_gallery

    try:
        gallery = fetch_fandom_gallery(person_name)
    except Exception as exc:
        if verbose:
            print(f"  WARN Fandom gallery {person_name}: {exc}")
        return []

    if gallery.error:
        if verbose:
            print(f"  WARN Fandom gallery {person_name}: {gallery.error}")
        return []

    if not gallery.images:
        return []

    images = gallery.images[:limit] if limit else gallery.images
    rows: list[dict[str, Any]] = []
    now = datetime.now(UTC).isoformat()
    file_meta_cache: dict[str, Any] = {}

    for image in images:
        image_url = image.url
        if not image_url:
            continue

        section_label = _normalize_fandom_section_label(image.section_label)
        section_tag = _infer_fandom_section_tag(section_label, image.caption)
        season = _extract_season_number(section_label, image.caption)
        file_page_url = image.file_page_url
        file_meta = None
        if resolve_file_pages and file_page_url:
            cached = file_meta_cache.get(file_page_url)
            if cached is None:
                try:
                    cached = fetch_fandom_file_metadata(file_page_url)
                except Exception as exc:
                    if verbose:
                        print(f"  WARN Fandom file {file_page_url}: {exc}")
                    cached = None
                file_meta_cache[file_page_url] = cached
            file_meta = cached

        resolved_url = image_url
        resolved_width = image.width
        resolved_height = image.height
        resolved_mime = None
        resolved_created_at = None
        if file_meta:
            resolved_url = file_meta.file_url or resolved_url
            resolved_width = file_meta.width or resolved_width
            resolved_height = file_meta.height or resolved_height
            resolved_mime = file_meta.mime_type
            resolved_created_at = file_meta.created_at

        metadata = _build_fandom_metadata(
            section_tag=section_tag,
            section_label=section_label,
            source_variant="fandom_gallery",
            source_page_url=file_page_url or image.source_page_url,
        )
        if metadata is None:
            metadata = {}
        if image.source_page_url:
            metadata.setdefault("source_gallery_url", image.source_page_url)
        if file_page_url:
            metadata.setdefault("source_file_url", file_page_url)
        if season:
            metadata.setdefault("season_number", season)
        if resolved_width:
            metadata["image_width"] = resolved_width
        if resolved_height:
            metadata["image_height"] = resolved_height
        if resolved_mime:
            metadata["image_mime_type"] = resolved_mime
        if resolved_created_at:
            metadata["source_created_at"] = resolved_created_at

        # Ensure url and url_path are never null
        url_value = resolved_url or image_url
        url_path = _url_path_with_query(url_value) or url_value

        rows.append(
            {
                "person_id": str(person_id),
                "imdb_person_id": imdb_person_id,
                "source": "fandom",
                "source_image_id": f"fandom-gallery-{_url_hash(url_value)}",
                "source_page_url": file_page_url or image.source_page_url or gallery.url,
                "url": url_value,
                "url_path": url_path,
                "image_url": url_value,
                "thumb_url": image.thumb_url,
                "image_url_canonical": _canonical_url(url_value),
                "caption": image.caption,
                "season": season,
                "width": resolved_width,
                "height": resolved_height,
                "metadata": metadata,
                "fetched_at": now,
            }
        )

    return rows


# ---------------------------------------------------------------------------
# Unified Multi-Source Fetcher
# ---------------------------------------------------------------------------


def fetch_all_cast_photos(
    person_id: str | UUID,
    *,
    imdb_person_id: str | None = None,
    tmdb_person_id: int | None = None,
    person_name: str | None = None,
    sources: list[str] | None = None,
    limit_per_source: int = 50,
    session: requests.Session | None = None,
    verbose: bool = False,
) -> list[dict[str, Any]]:
    """
    Fetch cast photos from all enabled sources.

    Args:
        person_id: core.people UUID
        imdb_person_id: IMDb person ID (required for imdb source)
        tmdb_person_id: TMDb person ID (required for tmdb source)
        person_name: Person name (required for fandom sources)
        sources: List of sources to fetch from. Default: all available
        limit_per_source: Max photos per source
        session: Optional requests session
        verbose: Print progress

    Returns:
        Combined list of photo dicts from all sources
    """
    all_sources = {"imdb", "tmdb", "fandom", "fandom-gallery"}
    enabled = set(sources) if sources else all_sources

    all_rows: list[dict[str, Any]] = []

    # IMDb source
    if "imdb" in enabled and imdb_person_id:
        if verbose:
            print(f"  Fetching IMDb photos for {imdb_person_id}...")
        rows = fetch_imdb_cast_photos(
            imdb_person_id,
            person_id,
            limit=limit_per_source,
            session=session,
            verbose=verbose,
        )
        if verbose:
            print(f"    Found {len(rows)} IMDb photos")
        all_rows.extend(rows)

    # TMDb source
    if "tmdb" in enabled and tmdb_person_id:
        if verbose:
            print(f"  Fetching TMDb photos for {tmdb_person_id}...")
        rows = fetch_tmdb_cast_photos(
            tmdb_person_id,
            person_id,
            imdb_person_id=imdb_person_id,
            limit=limit_per_source,
            verbose=verbose,
        )
        if verbose:
            print(f"    Found {len(rows)} TMDb photos")
        all_rows.extend(rows)

    # Fandom person page source
    if "fandom" in enabled and person_name:
        if verbose:
            print(f"  Fetching Fandom person photos for {person_name}...")
        rows = fetch_fandom_person_cast_photos(
            person_name,
            person_id,
            imdb_person_id=imdb_person_id,
            limit=limit_per_source,
            verbose=verbose,
        )
        if verbose:
            print(f"    Found {len(rows)} Fandom person photos")
        all_rows.extend(rows)

    # Fandom gallery source
    if "fandom-gallery" in enabled and person_name:
        if verbose:
            print(f"  Fetching Fandom gallery photos for {person_name}...")
        rows = fetch_fandom_gallery_cast_photos(
            person_name,
            person_id,
            imdb_person_id=imdb_person_id,
            limit=limit_per_source,
            verbose=verbose,
        )
        if verbose:
            print(f"    Found {len(rows)} Fandom gallery photos")
        all_rows.extend(rows)

    return all_rows
