from __future__ import annotations

import html
import json
import re
import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
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
_GALLERY_ITEM_ID_RE = re.compile(
    r'<div[^>]*class=["\'][^"\']*js-gallery-item-id[^"\']*["\'][^>]*>\s*(?P<item_id>\d+)\s*</div>',
    re.IGNORECASE | re.DOTALL,
)
_GALLERY_IMAGE_ATTR_RE = re.compile(
    r'(?:src|data-src|data-lazy-src)=["\']([^"\']+)["\']',
    re.IGNORECASE,
)
_BRAVO_IMAGE_URL_RE = re.compile(r"/sites/(?:bravo|nbcuniversal)/files/", re.IGNORECASE)
_GALLERY_ORIGINAL_IMAGE_RE = re.compile(
    r"/sites/(?:bravo|nbcuniversal)/files/(?:styles/media_gallery_computer/public/)?"
    r'((?:field_media_items|legacy/(?:photos|images/photo)|\d{4}/\d{2})/[^\s"\'?]+\.(?:jpg|jpeg|png))',
    re.IGNORECASE,
)


class BravoJSONAPIMalformedPageError(RuntimeError):
    """A JSON:API response cannot be used safely by an incremental collector."""


@dataclass(frozen=True)
class BravoJSONAPIPage:
    """One validated collection response and its normalized next-page link."""

    resource: str
    request_url: str
    records: tuple[dict[str, Any], ...]
    included: tuple[dict[str, Any], ...]
    next_url: str | None


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


def _normalize_gallery_file_url(value: str | None) -> str | None:
    absolute = _absolute_url(value)
    if not absolute:
        return None
    parsed = urlparse(absolute)
    path = parsed.path.replace("/styles/media_gallery_computer/public/", "/")
    return f"{parsed.scheme}://{parsed.netloc}{path}"


def _parse_file_name(value: str | None) -> str | None:
    url = _normalize_gallery_file_url(value)
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


def _strip_html_text(value: Any) -> str | None:
    if isinstance(value, dict):
        value = value.get("value")
    text = str(value or "").strip()
    if not text:
        return None
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    normalized = " ".join(text.split())
    return normalized or None


def _build_gallery_item_source_page_url(gallery_path: str | None, gallery_item_id: str | None) -> str | None:
    gallery_url = _absolute_url(gallery_path)
    if not gallery_url:
        return None
    cleaned_item_id = str(gallery_item_id or "").strip()
    if not cleaned_item_id:
        return gallery_url
    return f"{gallery_url}#{cleaned_item_id}"


def _extract_gallery_item_id_lookup(
    page_html: str,
    *,
    gallery_path: str | None,
) -> dict[str, str]:
    cleaned_gallery_path = str(gallery_path or "").strip()
    if not page_html.strip() or not cleaned_gallery_path:
        return {}

    lookup: dict[str, str] = {}
    for match in _GALLERY_ITEM_ID_RE.finditer(page_html):
        item_id = str(match.group("item_id") or "").strip()
        if not item_id:
            continue
        context_before = page_html[max(0, match.start() - 4000) : match.start()]
        context_after = page_html[match.end() : min(len(page_html), match.end() + 1500)]
        for context in (context_before, context_after):
            src_matches = list(_GALLERY_IMAGE_ATTR_RE.finditer(context))
            if not src_matches:
                continue
            for src_match in reversed(src_matches):
                src = _absolute_url(src_match.group(1))
                file_name = _parse_file_name(src)
                if not file_name or file_name in lookup:
                    continue
                lookup[file_name] = item_id
                break
            else:
                continue
            break
        else:
            continue
    return lookup


def _selector_first_text(selector: Any, *css_queries: str) -> str | None:
    for query in css_queries:
        try:
            text = selector.css(query).get()
        except Exception:
            text = None
        cleaned = _strip_html_text(text)
        if cleaned:
            return cleaned
    return None


def _selector_first_attr(selector: Any, css_query: str) -> str | None:
    try:
        value = selector.css(css_query).get()
    except Exception:
        return None
    return str(value or "").strip() or None


def _extract_original_gallery_image_urls(page_html: str) -> list[str]:
    seen: set[str] = set()
    urls: list[str] = []
    for match in _GALLERY_ORIGINAL_IMAGE_RE.finditer(page_html):
        relative_path = str(match.group(1) or "").strip()
        if not relative_path:
            continue
        url = f"{BRAVO_BASE_URL}/sites/bravo/files/{relative_path}"
        if url in seen:
            continue
        seen.add(url)
        urls.append(url)
    return urls


def _extract_gallery_item_id_from_selector(selector: Any) -> str | None:
    attributes = getattr(selector, "attrib", None)
    if attributes is not None and (isinstance(attributes, Mapping) or hasattr(attributes, "get")):
        for key in (
            "data-gallery-item-id",
            "data-gallery-id",
            "data-media-id",
            "data-id",
            "id",
        ):
            value = _coerce_gallery_item_id(attributes.get(key))
            if value:
                match = re.search(r"(\d+)$", value)
                return match.group(1) if match else value
    return _selector_first_text(selector, ".js-gallery-item-id::text", "[class*='gallery-item-id']::text")


def extract_gallery_assets_from_html(
    page_html: str,
    *,
    gallery: dict[str, Any],
) -> list[dict[str, Any]]:
    """Fallback parser for Bravo gallery pages when JSONAPI media includes are incomplete."""
    if not str(page_html or "").strip():
        return []

    # Lazy import: scrapling is only installed on the browser image, and this
    # module is imported by the lean-image API at startup.
    from scrapling import Selector

    page = Selector(page_html)
    settings = extract_drupal_settings(page_html)
    metadata = extract_gallery_metadata(settings)
    season_number = resolve_season_number(metadata)
    gallery_uuid = str(gallery.get("uuid") or "").strip()
    gallery_path = str(gallery.get("path") or "").strip()
    candidate_selectors = (
        "[data-gallery-item-id]",
        "[data-media-id]",
        ".gallery-item",
        ".media-gallery__item",
        ".field--name-field-media-items .field__item",
        "figure",
    )

    rows: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    candidates: list[Any] = []
    for query in candidate_selectors:
        try:
            for candidate in page.css(query):
                candidates.append(candidate)
        except Exception:
            continue
    if not candidates:
        candidates = list(page.css("img"))

    for candidate in candidates:
        image_scope = candidate
        try:
            image = candidate.css("img").first
            if image is not None:
                image_scope = image
        except Exception:
            image_scope = candidate
        raw_file_url = (
            _selector_first_attr(image_scope, "::attr(data-src)")
            or _selector_first_attr(image_scope, "::attr(data-lazy-src)")
            or _selector_first_attr(image_scope, "::attr(src)")
            or _selector_first_attr(image_scope, "::attr(srcset)")
        )
        file_url = _normalize_gallery_file_url(raw_file_url)
        if file_url and "," in file_url:
            file_url = _normalize_gallery_file_url(file_url.split(",", 1)[0].strip().split(" ", 1)[0])
        if not file_url or not _BRAVO_IMAGE_URL_RE.search(file_url):
            continue
        if file_url in seen_urls:
            continue
        seen_urls.add(file_url)
        file_name = _parse_file_name(file_url)
        image_alt = _selector_first_attr(image_scope, "::attr(alt)")
        gallery_item_id = _extract_gallery_item_id_from_selector(candidate)
        row = _build_gallery_row(
            gallery=gallery,
            metadata=metadata,
            gallery_uuid=gallery_uuid,
            gallery_path=gallery_path,
            season_number=season_number,
            position=len(rows),
            media_id=_selector_first_attr(candidate, "::attr(data-media-uuid)") or "",
            media_attributes={
                "field_caption": _selector_first_text(
                    candidate,
                    ".field--name-field-caption::text",
                    ".caption::text",
                    "figcaption::text",
                ),
                "field_credit": _selector_first_text(
                    candidate,
                    ".field--name-field-credit::text",
                    ".credit::text",
                ),
                "field_image_description": _selector_first_text(
                    candidate,
                    ".field--name-field-image-description::text",
                    ".description::text",
                ),
                "field_media_image_alt": image_alt,
            },
            media_internal_id=_coerce_gallery_item_id(_selector_first_attr(candidate, "::attr(data-media-id)")),
            file_id=_selector_first_attr(image_scope, "::attr(data-file-uuid)") or "",
            file_url=file_url,
            file_name=file_name,
            file_attributes={
                "filename": file_name,
            },
            image_alt=image_alt,
            gallery_item_id=gallery_item_id,
        )
        if raw_file_url and _GALLERY_ORIGINAL_IMAGE_RE.search(raw_file_url):
            row["bravotv_html_original_url"] = True
        rows.append(row)
    for row in rows:
        row["bravotv_html_fallback"] = True
    existing_urls = {str(row.get("file_url") or "").strip() for row in rows if str(row.get("file_url") or "").strip()}
    for file_url in _extract_original_gallery_image_urls(page_html):
        if file_url in existing_urls:
            continue
        existing_urls.add(file_url)
        file_name = _parse_file_name(file_url)
        rows.append(
            _build_gallery_row(
                gallery=gallery,
                metadata=metadata,
                gallery_uuid=gallery_uuid,
                gallery_path=gallery_path,
                season_number=season_number,
                position=len(rows),
                media_id="",
                media_attributes={},
                media_internal_id=None,
                file_id="",
                file_url=file_url,
                file_name=file_name,
                file_attributes={"filename": file_name},
                image_alt=None,
                gallery_item_id=None,
            )
            | {"bravotv_html_fallback": True, "bravotv_html_original_url": True}
        )
    return rows


def _coerce_gallery_item_id(value: Any) -> str | None:
    if isinstance(value, int):
        return str(value)
    if isinstance(value, str):
        cleaned = value.strip()
        if cleaned:
            return cleaned
    return None


def _is_anchored_source_page_url(source_page_url: str | None) -> bool:
    text = str(source_page_url or "").strip()
    return bool(text and "#" in text.rsplit("/", 1)[-1])


def _build_gallery_row(
    *,
    gallery: dict[str, Any],
    metadata: dict[str, Any],
    gallery_uuid: str,
    gallery_path: str,
    season_number: int | None,
    position: int,
    media_id: str,
    media_attributes: dict[str, Any],
    media_internal_id: str | None,
    file_id: str,
    file_url: str,
    file_name: str | None,
    file_attributes: dict[str, Any],
    image_alt: str | None,
    gallery_item_id: str | None,
) -> dict[str, Any]:
    source_page_url = _build_gallery_item_source_page_url(gallery_path, gallery_item_id)
    row = {
        "gallery_uuid": gallery_uuid,
        "gallery_title": gallery.get("title"),
        "gallery_nid": gallery.get("nid"),
        "gallery_path": gallery.get("path"),
        "gallery_item_id": gallery_item_id,
        "media_internal_id": media_internal_id,
        "gallery_anchor_resolved": _is_anchored_source_page_url(source_page_url),
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
        "field_image_description": media_attributes.get("field_image_description"),
        "field_media_image_alt": image_alt,
        "file_uuid": file_id or None,
        "file_url": file_url,
        "file_name": file_name,
        "file_mime": file_attributes.get("filemime"),
        "file_size": file_attributes.get("filesize"),
        "source_page_url": source_page_url,
    }
    if not gallery_item_id:
        row["bravotv_unanchored"] = True
    return {key: value for key, value in row.items() if value not in (None, "", [], {})}


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
    person_url = f"{JSONAPI_BASE_URL}/node/person"

    def _entries(payload: dict[str, Any]) -> list[dict[str, Any]]:
        rows = payload.get("data") or []
        return [row for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []

    def _entry_title(entry: dict[str, Any]) -> str:
        raw_attributes = entry.get("attributes")
        attributes = raw_attributes if isinstance(raw_attributes, dict) else {}
        return str(attributes.get("title") or "").strip()

    def _entry_path(entry: dict[str, Any]) -> str:
        raw_attributes = entry.get("attributes")
        attributes = raw_attributes if isinstance(raw_attributes, dict) else {}
        path = attributes.get("path")
        return str(path.get("alias") or "").strip() if isinstance(path, dict) else ""

    def _entry_uuid(entry: dict[str, Any]) -> str | None:
        return str(entry.get("id") or "").strip() or None

    exact_payload = _get_json(api_client, person_url, params={"filter[title]": clean_name, "page[limit]": "1"})
    exact_entries = _entries(exact_payload)
    if exact_entries:
        exact_match = next(
            (entry for entry in exact_entries if _entry_title(entry).casefold() == clean_name.casefold()),
            exact_entries[0],
        )
        return _entry_uuid(exact_match)

    # Newer Bravo profiles are sometimes only discoverable through broader title filters.
    # Keep the old exact lookup first, then rank partial matches by title/path similarity.
    name_tokens = [part for part in re.split(r"[^a-z0-9]+", clean_name.casefold()) if part]
    fallback_payloads: list[dict[str, Any]] = []
    if name_tokens:
        first_last_phrase = " ".join(name_tokens)
        contains_queries = [
            first_last_phrase,
            name_tokens[0],
            name_tokens[-1] if len(name_tokens) > 1 else name_tokens[0],
        ]
        for phrase in dict.fromkeys(contains_queries):
            try:
                fallback_payloads.append(
                    _get_json(
                        api_client,
                        person_url,
                        params={
                            "filter[title-contains][condition][path]": "title",
                            "filter[title-contains][condition][operator]": "CONTAINS",
                            "filter[title-contains][condition][value]": phrase,
                            "page[limit]": "25",
                        },
                    )
                )
            except Exception:
                continue

    candidates: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for payload in fallback_payloads:
        for entry in _entries(payload):
            entry_id = _entry_uuid(entry)
            if not entry_id or entry_id in seen_ids:
                continue
            seen_ids.add(entry_id)
            candidates.append(entry)
    if not candidates:
        return None

    expected_slug = re.sub(r"[^a-z0-9]+", "-", clean_name.casefold()).strip("-")

    def _score(entry: dict[str, Any]) -> tuple[int, str]:
        title = _entry_title(entry).casefold()
        path = _entry_path(entry).casefold()
        haystack = f"{title} {path}"
        score = 0
        if title == clean_name.casefold():
            score += 100
        if expected_slug and expected_slug in path:
            score += 50
        score += sum(10 for token in name_tokens if re.search(rf"\b{re.escape(token)}\b", haystack))
        if clean_name.casefold() in haystack:
            score += 20
        return score, title

    best = max(candidates, key=_score)
    best_score, _ = _score(best)
    return _entry_uuid(best) if best_score > 0 else None


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


def fetch_person_image_assets(
    person_uuid: str,
    *,
    client: httpx.Client | None = None,
) -> list[dict[str, Any]]:
    """Fetch person-level Bravo JSONAPI image relationships such as cover photos."""
    clean_uuid = str(person_uuid or "").strip()
    if not clean_uuid:
        return []
    api_client = _client(client)
    detail = _get_json(
        api_client,
        f"{JSONAPI_BASE_URL}/node/person/{clean_uuid}",
        params={
            "include": ",".join(
                (
                    "field_person_cover_photo",
                    "field_person_cover_photo.field_media_image",
                    "field_person_full_photo",
                    "field_person_full_photo.field_media_image",
                )
            )
        },
    )
    raw_data = detail.get("data")
    data = raw_data if isinstance(raw_data, dict) else {}
    raw_attributes = data.get("attributes")
    attributes = raw_attributes if isinstance(raw_attributes, dict) else {}
    raw_relationships = data.get("relationships")
    relationships = raw_relationships if isinstance(raw_relationships, dict) else {}
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

    person_name = str(attributes.get("title") or "").strip()
    path_obj = attributes.get("path")
    person_path = path_obj.get("alias") if isinstance(path_obj, dict) else None
    gallery = {
        "uuid": clean_uuid,
        "title": f"{person_name} profile images".strip() or "Bravo profile images",
        "nid": attributes.get("drupal_internal__nid"),
        "path": person_path,
        "created": attributes.get("created"),
        "published": attributes.get("publish_on") or attributes.get("changed"),
    }
    metadata = {
        "people_names": [person_name] if person_name else [],
        "page_title": person_name or None,
    }

    rows: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    for field_name in ("field_person_cover_photo", "field_person_full_photo"):
        raw_field_rel = relationships.get(field_name)
        field_rel = raw_field_rel if isinstance(raw_field_rel, dict) else {}
        media_ref = field_rel.get("data") if isinstance(field_rel, dict) else None
        media_refs = media_ref if isinstance(media_ref, list) else [media_ref]
        for media_ref_item in media_refs:
            if not isinstance(media_ref_item, dict):
                continue
            media_id = str(media_ref_item.get("id") or "").strip()
            media_entry = media_by_id.get(media_id) or {}
            raw_media_attributes = media_entry.get("attributes")
            media_attributes = raw_media_attributes if isinstance(raw_media_attributes, dict) else {}
            raw_media_relationships = media_entry.get("relationships")
            media_relationships = raw_media_relationships if isinstance(raw_media_relationships, dict) else {}
            raw_media_image_rel = media_relationships.get("field_media_image")
            media_image_rel = raw_media_image_rel if isinstance(raw_media_image_rel, dict) else {}
            file_ref = media_image_rel.get("data") if isinstance(media_image_rel, dict) else {}
            file_id = str(file_ref.get("id") or "").strip() if isinstance(file_ref, dict) else ""
            file_entry = file_by_id.get(file_id) or {}
            raw_file_attributes = file_entry.get("attributes")
            file_attributes = raw_file_attributes if isinstance(raw_file_attributes, dict) else {}
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
            seen_key = media_id or file_id or file_url
            if seen_key in seen_keys:
                continue
            seen_keys.add(seen_key)
            image_meta = file_ref.get("meta") if isinstance(file_ref, dict) else {}
            image_alt = image_meta.get("alt") if isinstance(image_meta, dict) else None
            row = _build_gallery_row(
                gallery=gallery,
                metadata=metadata,
                gallery_uuid=clean_uuid,
                gallery_path=str(person_path or "").strip(),
                season_number=None,
                position=len(rows),
                media_id=media_id,
                media_attributes=media_attributes,
                media_internal_id=_coerce_gallery_item_id(media_attributes.get("drupal_internal__mid")),
                file_id=file_id,
                file_url=file_url,
                file_name=file_name,
                file_attributes=file_attributes,
                image_alt=image_alt,
                gallery_item_id=None,
            )
            row["bravotv_person_image_field"] = field_name
            rows.append(row)
    return rows


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
    raw_relationships = data.get("relationships")
    relationships = raw_relationships if isinstance(raw_relationships, dict) else {}
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
    gallery_item_id_lookup: dict[str, str] = {}
    gallery_html = ""
    if gallery_path:
        try:
            gallery_html = _get_html(api_client, f"{BRAVO_BASE_URL}{gallery_path}")
            settings = extract_drupal_settings(gallery_html)
            metadata = extract_gallery_metadata(settings)
            gallery_item_id_lookup = _extract_gallery_item_id_lookup(gallery_html, gallery_path=gallery_path)
        except Exception:
            metadata = {}
            gallery_item_id_lookup = {}
    season_number = resolve_season_number(metadata)

    rows: list[dict[str, Any]] = []
    for position, ref in enumerate(_iter_media_refs(detail)):
        if not isinstance(ref, dict):
            continue
        media_id = str(ref.get("id") or "").strip()
        media_entry = media_by_id.get(media_id) or {}
        raw_media_attributes = media_entry.get("attributes")
        media_attributes = raw_media_attributes if isinstance(raw_media_attributes, dict) else {}
        raw_media_relationships = media_entry.get("relationships")
        media_relationships = raw_media_relationships if isinstance(raw_media_relationships, dict) else {}
        raw_media_image_rel = media_relationships.get("field_media_image")
        media_image_rel = raw_media_image_rel if isinstance(raw_media_image_rel, dict) else {}
        file_ref = media_image_rel.get("data") if isinstance(media_image_rel, dict) else {}
        file_id = str(file_ref.get("id") or "").strip() if isinstance(file_ref, dict) else ""
        file_entry = file_by_id.get(file_id) or {}
        raw_file_attributes = file_entry.get("attributes")
        file_attributes = raw_file_attributes if isinstance(raw_file_attributes, dict) else {}
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
        media_internal_id = _coerce_gallery_item_id(media_attributes.get("drupal_internal__mid"))
        gallery_item_id = None
        for candidate_name in (file_name, _parse_file_name(file_url)):
            if candidate_name and candidate_name in gallery_item_id_lookup:
                gallery_item_id = gallery_item_id_lookup[candidate_name]
                break
        image_meta = file_ref.get("meta") if isinstance(file_ref, dict) else {}
        image_alt = image_meta.get("alt") if isinstance(image_meta, dict) else None

        rows.append(
            _build_gallery_row(
                gallery=gallery,
                metadata=metadata,
                gallery_uuid=gallery_uuid,
                gallery_path=gallery_path,
                season_number=season_number,
                position=position,
                media_id=media_id,
                media_attributes=media_attributes,
                media_internal_id=media_internal_id,
                file_id=file_id,
                file_url=file_url,
                file_name=file_name,
                file_attributes=file_attributes,
                image_alt=image_alt,
                gallery_item_id=gallery_item_id,
            )
        )
    if not rows and gallery_html:
        return extract_gallery_assets_from_html(gallery_html, gallery=gallery)
    if gallery_html:
        seen_urls = {str(row.get("file_url") or "").strip() for row in rows if str(row.get("file_url") or "").strip()}
        html_rows = [
            row
            for row in extract_gallery_assets_from_html(gallery_html, gallery=gallery)
            if row.get("bravotv_html_original_url")
        ]
        for html_row in html_rows:
            file_url = str(html_row.get("file_url") or "").strip()
            if not file_url or file_url in seen_urls:
                continue
            seen_urls.add(file_url)
            html_row["bravotv_html_enriched"] = True
            rows.append(html_row)
    return rows


def _jsonapi_collection_url(resource: str) -> str:
    cleaned = str(resource or "").strip().strip("/")
    if not cleaned or ".." in cleaned.split("/") or "?" in cleaned or "#" in cleaned:
        raise ValueError("Bravo JSON:API resource must be a relative resource path")
    return f"{JSONAPI_BASE_URL}/{cleaned}"


def _normalize_jsonapi_next_url(value: Any) -> str | None:
    """Validate a ``links.next`` value before a collector follows it."""
    if value is None:
        return None
    if isinstance(value, Mapping):
        value = value.get("href")
    if not isinstance(value, str) or not value.strip():
        raise BravoJSONAPIMalformedPageError("Bravo JSON:API links.next was not a URL")
    candidate = _absolute_url(value)
    parsed = urlparse(str(candidate or ""))
    hostname = (parsed.hostname or "").casefold()
    if (
        parsed.scheme != "https"
        or hostname not in {"bravotv.com", "www.bravotv.com"}
        or (parsed.port not in (None, 443))
        or not parsed.path.startswith("/jsonapi/")
    ):
        raise BravoJSONAPIMalformedPageError("Bravo JSON:API links.next was outside the official JSON:API origin")
    return candidate


def _validated_jsonapi_records(value: Any, *, field: str, request_url: str) -> tuple[dict[str, Any], ...]:
    if not isinstance(value, list):
        raise BravoJSONAPIMalformedPageError(f"Bravo JSON:API {field} was not a list for {request_url}")
    records: list[dict[str, Any]] = []
    for entry in value:
        if not isinstance(entry, dict):
            raise BravoJSONAPIMalformedPageError(f"Bravo JSON:API {field} contained a non-object for {request_url}")
        entry_id = str(entry.get("id") or "").strip()
        entry_type = str(entry.get("type") or "").strip()
        if not entry_id or not entry_type:
            raise BravoJSONAPIMalformedPageError(
                f"Bravo JSON:API {field} contained a record without type/id for {request_url}"
            )
        records.append(dict(entry))
    return tuple(records)


def fetch_jsonapi_collection_page(
    resource: str,
    *,
    client: httpx.Client | None = None,
    params: Mapping[str, Any] | None = None,
    page_url: str | None = None,
) -> BravoJSONAPIPage:
    """Fetch one validated list response without hiding malformed pagination.

    Collectors own their termination and cap logic; this helper only guarantees
    that every followed ``links.next`` link is an official HTTPS JSON:API URL and
    that page records have durable JSON:API identities.
    """
    request_url = _normalize_jsonapi_next_url(page_url) if page_url else _jsonapi_collection_url(resource)
    if request_url is None:
        raise ValueError("Bravo JSON:API page URL is required")
    payload = _get_json(_client(client), request_url, params=dict(params) if page_url is None and params else None)
    if "data" not in payload:
        raise BravoJSONAPIMalformedPageError(f"Bravo JSON:API page was missing data for {request_url}")
    records = _validated_jsonapi_records(payload.get("data"), field="data", request_url=request_url)
    included_raw = payload.get("included", [])
    included = _validated_jsonapi_records(included_raw, field="included", request_url=request_url)
    links = payload.get("links", {})
    if not isinstance(links, Mapping):
        raise BravoJSONAPIMalformedPageError(f"Bravo JSON:API links was not an object for {request_url}")
    next_url = _normalize_jsonapi_next_url(links.get("next")) if "next" in links else None
    return BravoJSONAPIPage(
        resource=str(resource).strip().strip("/"),
        request_url=request_url,
        records=records,
        included=included,
        next_url=next_url,
    )


def fetch_jsonapi_resource_detail(
    resource: str,
    resource_id: str,
    *,
    client: httpx.Client | None = None,
    params: Mapping[str, Any] | None = None,
) -> tuple[dict[str, Any], tuple[dict[str, Any], ...]]:
    """Fetch a JSON:API detail record with the same strict identity checks."""
    clean_id = str(resource_id or "").strip()
    if not clean_id or "/" in clean_id or "?" in clean_id or "#" in clean_id:
        raise ValueError("Bravo JSON:API resource ID must be a single non-empty identifier")
    request_url = f"{_jsonapi_collection_url(resource)}/{clean_id}"
    payload = _get_json(_client(client), request_url, params=dict(params) if params else None)
    data = payload.get("data")
    if not isinstance(data, dict):
        raise BravoJSONAPIMalformedPageError(f"Bravo JSON:API detail was missing data object for {request_url}")
    records = _validated_jsonapi_records([data], field="data", request_url=request_url)
    included = _validated_jsonapi_records(payload.get("included", []), field="included", request_url=request_url)
    return records[0], included
