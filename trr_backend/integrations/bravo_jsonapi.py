from __future__ import annotations

import html
import json
import re
import time
from collections.abc import Iterable
from typing import Any
from urllib.parse import urlparse

import httpx

BRAVO_BASE_URL = "https://www.bravotv.com"
JSONAPI_BASE_URL = f"{BRAVO_BASE_URL}/jsonapi"
DEFAULT_TIMEOUT_SECONDS = 30
DEFAULT_PAGE_SIZE = 50

_SEASON_RE = re.compile(r"Season[- ](\d+)", re.IGNORECASE)
_SETTINGS_RE = re.compile(
    r'<script[^>]*data-drupal-selector="drupal-settings-json"[^>]*>(.*?)</script>',
    re.DOTALL,
)


def _client(client: httpx.Client | None = None) -> httpx.Client:
    return client or httpx.Client(
        timeout=DEFAULT_TIMEOUT_SECONDS,
        follow_redirects=True,
        headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"},
    )


def _get_json(client: httpx.Client, url: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    response = client.get(url, params=params)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise RuntimeError(f"Bravo JSONAPI returned unexpected payload type for {url}")
    return payload


def _get_html(client: httpx.Client, url: str) -> str:
    response = client.get(url)
    response.raise_for_status()
    return response.text


def _absolute_url(value: str | None) -> str | None:
    cleaned = str(value or "").strip()
    if not cleaned:
        return None
    if cleaned.startswith("//"):
        return f"https:{cleaned}"
    if cleaned.startswith("/"):
        return f"{BRAVO_BASE_URL}{cleaned}"
    return cleaned


def _parse_file_name(value: str | None) -> str | None:
    url = _absolute_url(value)
    if not url:
        return None
    parsed = urlparse(url)
    name = parsed.path.rsplit("/", 1)[-1].strip()
    return name or None


def _extract_file_url(attributes: dict[str, Any]) -> str | None:
    candidates = (
        attributes.get("uri", {}).get("url") if isinstance(attributes.get("uri"), dict) else None,
        attributes.get("url"),
        attributes.get("image_style_uri", {}).get("url")
        if isinstance(attributes.get("image_style_uri"), dict)
        else None,
        attributes.get("image_style_uri"),
    )
    for candidate in candidates:
        absolute = _absolute_url(candidate if isinstance(candidate, str) else None)
        if absolute:
            return absolute
    return None


def extract_drupal_settings(page_html: str) -> dict[str, Any]:
    match = _SETTINGS_RE.search(page_html)
    if not match:
        return {}
    try:
        payload = json.loads(match.group(1))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def extract_gallery_metadata(settings: dict[str, Any]) -> dict[str, Any]:
    metadata: dict[str, Any] = {}
    cag = settings.get("mpscall", settings.get("mps", {})).get("cag", {})
    if isinstance(cag, dict):
        field_cast = cag.get("field-cast", "")
        if isinstance(field_cast, str) and field_cast.strip():
            metadata["cast_slugs"] = [value.strip() for value in field_cast.split("|") if value.strip()]
        metadata["show_slug"] = cag.get("show")
        metadata["season_slug"] = cag.get("season")
        metadata["episode_slug"] = cag.get("episode")
        metadata["content_type"] = cag.get("type")

    adobe = settings.get("ls_adobe_analytics", {})
    if isinstance(adobe, dict):
        people_raw = adobe.get("people", "")
        if isinstance(people_raw, str) and people_raw.strip():
            metadata["people_names"] = [html.unescape(part.strip()) for part in people_raw.split(",") if part.strip()]
        metadata["show_name"] = adobe.get("showSite")
        metadata["season_name"] = adobe.get("season")
        metadata["content_id"] = adobe.get("contentID")
        metadata["published_date"] = adobe.get("publishedDate")
        metadata["page_title"] = adobe.get("pageName")

    return {key: value for key, value in metadata.items() if value not in (None, "", [], {})}


def resolve_season_number(metadata: dict[str, Any]) -> int | None:
    for candidate in (
        metadata.get("season_name"),
        metadata.get("season_slug"),
        metadata.get("page_title"),
    ):
        if not isinstance(candidate, str) or not candidate.strip():
            continue
        match = _SEASON_RE.search(candidate.replace("-", " "))
        if match:
            return int(match.group(1))
    return None


def find_person_uuid(name: str, *, client: httpx.Client | None = None) -> str | None:
    clean_name = str(name or "").strip()
    if not clean_name:
        return None
    api_client = _client(client)
    payload = _get_json(
        api_client,
        f"{JSONAPI_BASE_URL}/node/person",
        params={"filter[title]": clean_name, "page[limit]": "1"},
    )
    entries = payload.get("data") or []
    if not isinstance(entries, list) or not entries:
        return None
    first = entries[0]
    return str(first.get("id") or "").strip() or None


def find_show_node(show_name: str, *, client: httpx.Client | None = None) -> dict[str, Any] | None:
    clean_name = str(show_name or "").strip()
    if not clean_name:
        return None
    api_client = _client(client)
    payload = _get_json(
        api_client,
        f"{JSONAPI_BASE_URL}/node/tv_show",
        params={"filter[title]": clean_name, "page[limit]": "1"},
    )
    entries = payload.get("data") or []
    if not isinstance(entries, list) or not entries:
        return None
    entry = entries[0]
    attributes = entry.get("attributes") if isinstance(entry.get("attributes"), dict) else {}
    return {
        "uuid": str(entry.get("id") or "").strip() or None,
        "nid": attributes.get("drupal_internal__nid"),
        "title": attributes.get("title"),
        "path": attributes.get("path", {}).get("alias") if isinstance(attributes.get("path"), dict) else None,
    }


def _paged_gallery_listing(
    *,
    client: httpx.Client,
    params: dict[str, str],
    limit: int | None,
) -> list[dict[str, Any]]:
    galleries: list[dict[str, Any]] = []
    offset = 0
    page_size = max(1, min(DEFAULT_PAGE_SIZE, limit or DEFAULT_PAGE_SIZE))
    base_params = dict(params)

    while True:
        payload = _get_json(
            client,
            f"{JSONAPI_BASE_URL}/node/media_gallery",
            params={**base_params, "page[limit]": str(page_size), "page[offset]": str(offset)},
        )
        entries = payload.get("data") or []
        if not isinstance(entries, list) or not entries:
            break
        for entry in entries:
            attributes = entry.get("attributes") if isinstance(entry.get("attributes"), dict) else {}
            path_obj = attributes.get("path")
            galleries.append(
                {
                    "uuid": str(entry.get("id") or "").strip() or None,
                    "title": attributes.get("title"),
                    "nid": attributes.get("drupal_internal__nid"),
                    "path": path_obj.get("alias") if isinstance(path_obj, dict) else None,
                    "created": attributes.get("created"),
                    "published": attributes.get("published_at") or attributes.get("changed"),
                }
            )
            if limit and len(galleries) >= limit:
                return galleries[:limit]
        if len(entries) < page_size:
            break
        offset += page_size
        time.sleep(0.25)
    return galleries


def fetch_person_galleries(
    person_uuid: str,
    *,
    client: httpx.Client | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    clean_uuid = str(person_uuid or "").strip()
    if not clean_uuid:
        return []
    api_client = _client(client)
    return _paged_gallery_listing(
        client=api_client,
        params={"filter[field_cast.id]": clean_uuid},
        limit=limit,
    )


def fetch_show_galleries(
    show_nid: int | str,
    *,
    client: httpx.Client | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    api_client = _client(client)
    return _paged_gallery_listing(
        client=api_client,
        params={"filter[field_tv_shows.show]": str(show_nid)},
        limit=limit,
    )


def _iter_media_refs(detail_payload: dict[str, Any]) -> Iterable[dict[str, Any]]:
    data = detail_payload.get("data")
    if not isinstance(data, dict):
        return []
    relationships = data.get("relationships") if isinstance(data.get("relationships"), dict) else {}
    field_media_items_raw = relationships.get("field_media_items")
    field_media_items = field_media_items_raw if isinstance(field_media_items_raw, dict) else {}
    refs = field_media_items.get("data")
    return refs if isinstance(refs, list) else []


def fetch_gallery_assets(
    gallery: dict[str, Any],
    *,
    client: httpx.Client | None = None,
) -> list[dict[str, Any]]:
    gallery_uuid = str(gallery.get("uuid") or "").strip()
    if not gallery_uuid:
        return []

    api_client = _client(client)
    detail = _get_json(
        api_client,
        f"{JSONAPI_BASE_URL}/node/media_gallery/{gallery_uuid}",
        params={"include": "field_media_items,field_media_items.field_media_image"},
    )
    included = detail.get("included") or []
    included_items = included if isinstance(included, list) else []

    media_by_id: dict[str, dict[str, Any]] = {}
    file_by_id: dict[str, dict[str, Any]] = {}
    for entry in included_items:
        if not isinstance(entry, dict):
            continue
        entry_id = str(entry.get("id") or "").strip()
        entry_type = str(entry.get("type") or "").strip()
        if not entry_id or not entry_type:
            continue
        if entry_type == "media--image":
            media_by_id[entry_id] = entry
        elif entry_type == "file--file":
            file_by_id[entry_id] = entry

    gallery_path = str(gallery.get("path") or "").strip()
    metadata: dict[str, Any] = {}
    if gallery_path:
        try:
            settings = extract_drupal_settings(_get_html(api_client, f"{BRAVO_BASE_URL}{gallery_path}"))
            metadata = extract_gallery_metadata(settings)
        except Exception:
            metadata = {}
    season_number = resolve_season_number(metadata)

    rows: list[dict[str, Any]] = []
    for position, ref in enumerate(_iter_media_refs(detail)):
        if not isinstance(ref, dict):
            continue
        media_id = str(ref.get("id") or "").strip()
        media_entry = media_by_id.get(media_id) or {}
        media_attributes = media_entry.get("attributes") if isinstance(media_entry.get("attributes"), dict) else {}
        media_relationships = (
            media_entry.get("relationships") if isinstance(media_entry.get("relationships"), dict) else {}
        )
        media_image_rel = (
            media_relationships.get("field_media_image")
            if isinstance(media_relationships.get("field_media_image"), dict)
            else {}
        )
        file_ref = media_image_rel.get("data") if isinstance(media_image_rel, dict) else {}
        file_id = str(file_ref.get("id") or "").strip() if isinstance(file_ref, dict) else ""
        file_entry = file_by_id.get(file_id) or {}
        file_attributes = file_entry.get("attributes") if isinstance(file_entry.get("attributes"), dict) else {}
        file_url = _extract_file_url(file_attributes)
        file_name = (
            str(file_attributes.get("filename") or "").strip()
            or _parse_file_name(file_url)
            or _parse_file_name(
                file_attributes.get("uri", {}).get("url") if isinstance(file_attributes.get("uri"), dict) else None
            )
        )
        if not file_url:
            continue

        row = {
            "gallery_uuid": gallery_uuid,
            "gallery_title": gallery.get("title"),
            "gallery_nid": gallery.get("nid"),
            "gallery_path": gallery.get("path"),
            "gallery_created": gallery.get("created"),
            "gallery_published": gallery.get("published"),
            "gallery_position": position,
            "gallery_people_names": metadata.get("people_names"),
            "gallery_show_name": metadata.get("show_name"),
            "gallery_season_name": metadata.get("season_name"),
            "gallery_episode_slug": metadata.get("episode_slug"),
            "gallery_published_date": metadata.get("published_date"),
            "gallery_page_title": metadata.get("page_title"),
            "season_number": season_number,
            "media_uuid": media_id or None,
            "media_name": media_attributes.get("name"),
            "field_caption": media_attributes.get("field_caption"),
            "field_credit": media_attributes.get("field_credit"),
            "file_uuid": file_id or None,
            "file_url": file_url,
            "file_name": file_name,
            "file_mime": file_attributes.get("filemime"),
            "file_size": file_attributes.get("filesize"),
        }
        rows.append({key: value for key, value in row.items() if value not in (None, "", [], {})})
    return rows
