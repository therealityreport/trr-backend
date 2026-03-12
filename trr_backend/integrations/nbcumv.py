from __future__ import annotations

import io
import json
import logging
import os
import re
import zipfile
from dataclasses import dataclass
from datetime import UTC, datetime
from fractions import Fraction
from functools import lru_cache
from typing import Any

from PIL import ExifTags, Image, ImageOps
from requests import Session
from requests.exceptions import RequestException

logger = logging.getLogger(__name__)

APPSYNC_URL = os.environ.get(
    "NBCUMV_APPSYNC_URL",
    "https://bfg5dqxssngazhtsf6uo7bzdvm.appsync-api.us-west-2.amazonaws.com/graphql",
)
APPSYNC_API_KEY = os.environ.get("NBCUMV_APPSYNC_API_KEY", "")
BATCH_DOWNLOAD_URL = os.environ.get(
    "NBCUMV_BATCH_DOWNLOAD_URL",
    "https://or1ukny4rd.execute-api.us-west-2.amazonaws.com/v1",
)
DEFAULT_TIMEOUT_SECONDS = 20
DEFAULT_PAGE_SIZE = 100
MAX_FALLBACK_SCAN_PAGES = 100

_IMAGE_FIELDS = """
id
lbx_id
lbx_filename
created
modified
liveDate
location
lbx_airdateText
lbx_caption
lbx_credit
lbx_copyright
lbx_endDate
lbx_episodeTitle
lbx_fileSize
lbx_headline
lbx_height
lbx_keywords
lbx_liveDate
lbx_metadataFilename
lbx_nupNumber
lbx_photographer
lbx_programCategory
lbx_resolutionX
lbx_resolutionY
lbx_season
lbx_seasonNumber
lbx_showTitle
lbx_specialInstructions
lbx_type
lbx_width
divisionIds
eventIds
networkIds
showIds
pubStatus
"""

_IPTC_DATASET_NAMES: dict[tuple[int, int], str] = {
    (2, 5): "object_name",
    (2, 15): "category",
    (2, 20): "supplemental_categories",
    (2, 25): "keywords",
    (2, 40): "special_instructions",
    (2, 55): "date_created",
    (2, 60): "time_created",
    (2, 80): "byline",
    (2, 85): "byline_title",
    (2, 90): "city",
    (2, 92): "sublocation",
    (2, 95): "province_state",
    (2, 101): "country",
    (2, 103): "original_transmission_reference",
    (2, 105): "headline",
    (2, 110): "credit",
    (2, 115): "source",
    (2, 116): "copyright_notice",
    (2, 118): "contact",
    (2, 120): "caption_abstract",
    (2, 122): "writer_editor",
}


@dataclass(frozen=True)
class SearchFilters:
    filename: str | None = None
    lbx_id: str | None = None
    show_id: str | None = None
    created_start: str | None = None
    created_end: str | None = None
    live_date_start: str | None = None
    live_date_end: str | None = None
    search_caption: str | None = None
    limit: int = 25


def _normalize_title(value: str | None) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    if text.endswith(", the"):
        text = f"the {text[:-5].strip()}"
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return " ".join(text.split())


def _filename_key(value: str | None) -> str:
    return str(value or "").strip().lower()


def _session(session: Session | None = None) -> Session:
    return session or Session()


def _graphql_request(query: str, *, session: Session | None = None) -> dict[str, Any]:
    client = _session(session)
    try:
        response = client.post(
            APPSYNC_URL,
            json={"query": query},
            headers={
                "Content-Type": "application/json",
                "x-api-key": APPSYNC_API_KEY,
            },
            timeout=DEFAULT_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
    except RequestException as exc:
        raise RuntimeError(f"NBCUMV GraphQL request failed: {exc}") from exc

    payload = response.json()
    errors = payload.get("errors")
    if isinstance(errors, list) and errors:
        message = "; ".join(str(item.get("message") or item) for item in errors)
        raise RuntimeError(f"NBCUMV GraphQL error: {message}")
    data = payload.get("data")
    if not isinstance(data, dict):
        raise RuntimeError("NBCUMV GraphQL response was missing data")
    return data


def _json_graphql(value: Any) -> str:
    return json.dumps(value, ensure_ascii=True)


def _render_graphql_input(value: Any) -> str:
    if isinstance(value, dict):
        parts = [f"{key}: {_render_graphql_input(inner)}" for key, inner in value.items() if inner is not None]
        return "{ " + ", ".join(parts) + " }"
    if isinstance(value, list):
        return "[" + ", ".join(_render_graphql_input(item) for item in value) + "]"
    return _json_graphql(value)


def _build_search_filter(filters: SearchFilters) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    if filters.filename:
        payload["lbx_filename"] = {"eq": filters.filename}
    if filters.show_id:
        payload["showIds"] = {"contains": filters.show_id}
    if filters.live_date_start and filters.live_date_end:
        payload["liveDate"] = {
            "between": [_iso_day_start(filters.live_date_start), _iso_day_end(filters.live_date_end)]
        }
    return payload


def _caption_matches(item: dict[str, Any], search_caption: str | None) -> bool:
    needle = str(search_caption or "").strip().casefold()
    if not needle:
        return True
    caption = str(item.get("lbx_caption") or "").casefold()
    return needle in caption


def _iso_day_start(day: str) -> str:
    value = str(day or "").strip()
    if not value:
        raise ValueError("Date value is required")
    if "T" in value:
        return value
    return f"{value}T00:00:00.000Z"


def _iso_day_end(day: str) -> str:
    value = str(day or "").strip()
    if not value:
        raise ValueError("Date value is required")
    if "T" in value:
        return value
    return f"{value}T23:59:59.999Z"


def search_images(filters: SearchFilters, *, session: Session | None = None) -> list[dict[str, Any]]:
    limit = max(1, int(filters.limit or 25))
    items: list[dict[str, Any]] = []
    local_caption_filter = str(filters.search_caption or "").strip()

    if filters.created_start and filters.created_end:
        next_token: str | None = None
        while len(items) < limit:
            page_limit = DEFAULT_PAGE_SIZE if local_caption_filter else min(DEFAULT_PAGE_SIZE, limit - len(items))
            filter_payload = _build_search_filter(filters)
            filter_expr = f"filter: {_render_graphql_input(filter_payload)}," if filter_payload else ""
            token_expr = _json_graphql(next_token) if next_token else "null"
            query = f"""
            query {{
              listLBXImages(
                datestart: {_json_graphql(_iso_day_start(filters.created_start))}
                dateend: {_json_graphql(_iso_day_end(filters.created_end))}
                {filter_expr}
                limit: {page_limit}
                nextToken: {token_expr}
              ) {{
                items {{ {_IMAGE_FIELDS} }}
                nextToken
              }}
            }}
            """
            payload = _graphql_request(query, session=session).get("listLBXImages") or {}
            page_items = payload.get("items") or []
            if local_caption_filter:
                items.extend(item for item in page_items if _caption_matches(item, local_caption_filter))
            else:
                items.extend(page_items)
            next_token = payload.get("nextToken")
            if not next_token or not page_items:
                break
    else:
        next_token = None
        filter_payload = _build_search_filter(filters)
        while len(items) < limit:
            page_limit = DEFAULT_PAGE_SIZE if local_caption_filter else min(DEFAULT_PAGE_SIZE, limit - len(items))
            token_expr = _json_graphql(next_token) if next_token else "null"
            filter_expr = _render_graphql_input(filter_payload) if filter_payload else "{}"
            query = f"""
            query {{
              searchImages(
                filter: {filter_expr}
                limit: {page_limit}
                nextToken: {token_expr}
              ) {{
                items {{ {_IMAGE_FIELDS} }}
                nextToken
              }}
            }}
            """
            payload = _graphql_request(query, session=session).get("searchImages") or {}
            page_items = payload.get("items") or []
            if local_caption_filter:
                items.extend(item for item in page_items if _caption_matches(item, local_caption_filter))
            else:
                items.extend(page_items)
            next_token = payload.get("nextToken")
            if not next_token or not page_items:
                break

    if filters.lbx_id and not any(
        str(item.get("lbx_id") or "").strip() == str(filters.lbx_id).strip() for item in items
    ):
        matched = _scan_for_lbx_id(str(filters.lbx_id).strip(), session=session)
        if matched is not None:
            items = [matched]

    return items[:limit]


def _list_show_images_uncached(
    show_id: str,
    *,
    session: Session | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    normalized_show_id = str(show_id or "").strip()
    if not normalized_show_id:
        return []
    items: list[dict[str, Any]] = []
    next_token: str | None = None
    page_limit = DEFAULT_PAGE_SIZE
    max_items = max(1, int(limit)) if limit is not None else None
    while True:
        token_expr = _json_graphql(next_token) if next_token else "null"
        query = f"""
        query {{
          lookImages(
            category: show
            id: {_json_graphql(normalized_show_id)}
            limit: {page_limit}
            nextToken: {token_expr}
          ) {{
            items {{
              img {{ {_IMAGE_FIELDS} }}
            }}
            nextToken
          }}
        }}
        """
        payload = _graphql_request(query, session=session).get("lookImages") or {}
        page_items = payload.get("items") or []
        if not page_items:
            break
        for row in page_items:
            if not isinstance(row, dict):
                continue
            image = row.get("img")
            if isinstance(image, dict):
                items.append(image)
                if max_items is not None and len(items) >= max_items:
                    return items[:max_items]
        next_token = payload.get("nextToken")
        if not next_token:
            break
    return items


@lru_cache(maxsize=64)
def _list_show_images_cached(show_id: str) -> tuple[dict[str, Any], ...]:
    return tuple(_list_show_images_uncached(show_id))


def list_show_images(show_id: str, *, session: Session | None = None, limit: int | None = None) -> list[dict[str, Any]]:
    normalized_show_id = str(show_id or "").strip()
    if not normalized_show_id:
        return []
    if session is None and limit is None:
        return [dict(item) for item in _list_show_images_cached(normalized_show_id)]
    return _list_show_images_uncached(normalized_show_id, session=session, limit=limit)


def build_show_image_index(
    show_id: str,
    *,
    session: Session | None = None,
) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for image in list_show_images(show_id, session=session):
        filename = _filename_key(image.get("lbx_filename"))
        if filename and filename not in index:
            index[filename] = image
    return index


def find_show_image_by_filename(
    show_id: str,
    filename: str | None,
    *,
    session: Session | None = None,
) -> dict[str, Any] | None:
    key = _filename_key(filename)
    if not key:
        return None
    return build_show_image_index(show_id, session=session).get(key)


@lru_cache(maxsize=1)
def list_all_shows() -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    next_token: str | None = None
    while True:
        token_expr = _json_graphql(next_token) if next_token else "null"
        query = f"""
        query {{
          listShows(
            limit: {DEFAULT_PAGE_SIZE}
            nextToken: {token_expr}
          ) {{
            items {{
              id
              title
              slug
              networkIds
            }}
            nextToken
          }}
        }}
        """
        payload = _graphql_request(query).get("listShows") or {}
        page_items = payload.get("items") or []
        items.extend(page_items)
        next_token = payload.get("nextToken")
        if not next_token or not page_items:
            break
    return items


def resolve_show_by_title(title: str | None) -> dict[str, Any] | None:
    normalized = _normalize_title(title)
    if not normalized:
        return None

    exact_matches: list[dict[str, Any]] = []
    fallback_matches: list[dict[str, Any]] = []
    for item in list_all_shows():
        item_title = str(item.get("title") or "")
        normalized_item = _normalize_title(item_title)
        if not normalized_item:
            continue
        if normalized_item == normalized:
            exact_matches.append(item)
            continue
        if normalized_item.startswith(normalized) or normalized.startswith(normalized_item):
            fallback_matches.append(item)

    if exact_matches:
        return exact_matches[0]
    if fallback_matches:
        return fallback_matches[0]
    return None


def _scan_for_lbx_id(lbx_id: str, *, session: Session | None = None) -> dict[str, Any] | None:
    next_token: str | None = None
    for _ in range(MAX_FALLBACK_SCAN_PAGES):
        token_expr = _json_graphql(next_token) if next_token else "null"
        query = f"""
        query {{
          searchImages(
            limit: {DEFAULT_PAGE_SIZE}
            nextToken: {token_expr}
          ) {{
            items {{ {_IMAGE_FIELDS} }}
            nextToken
          }}
        }}
        """
        payload = _graphql_request(query, session=session).get("searchImages") or {}
        for item in payload.get("items") or []:
            if str(item.get("lbx_id") or "").strip() == lbx_id:
                return item
        next_token = payload.get("nextToken")
        if not next_token:
            break
    return None


def fetch_image_by_identity(
    *,
    filename: str | None = None,
    lbx_id: str | None = None,
    show_id: str | None = None,
    session: Session | None = None,
) -> dict[str, Any] | None:
    if filename:
        if show_id:
            image = find_show_image_by_filename(str(show_id), filename, session=session)
            if image:
                return image
        items = search_images(SearchFilters(filename=filename, limit=1), session=session)
        if items:
            return items[0]
    if lbx_id:
        return _scan_for_lbx_id(str(lbx_id).strip(), session=session)
    return None


def request_hires_zip_url(*, lbx_id: str, filename: str, session: Session | None = None) -> str:
    payload = {
        "images": [
            {
                "filename": filename,
                "id": str(lbx_id),
                "resolution": "hiRes",
            }
        ]
    }
    client = _session(session)
    try:
        response = client.post(
            BATCH_DOWNLOAD_URL,
            json=payload,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/zip",
            },
            timeout=DEFAULT_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
    except RequestException as exc:
        raise RuntimeError(f"NBCUMV batch download request failed: {exc}") from exc

    result = response.json()
    location = result.get("location")
    if not isinstance(location, str) or not location.strip():
        raise RuntimeError("NBCUMV batch download response was missing location")
    return location


def download_hires_image(*, lbx_id: str, filename: str, session: Session | None = None) -> tuple[bytes, str | None]:
    zip_url = request_hires_zip_url(lbx_id=lbx_id, filename=filename, session=session)
    client = _session(session)
    try:
        response = client.get(zip_url, timeout=DEFAULT_TIMEOUT_SECONDS)
        response.raise_for_status()
    except RequestException as exc:
        raise RuntimeError(f"NBCUMV ZIP download failed: {exc}") from exc

    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        for member in archive.infolist():
            if member.is_dir():
                continue
            with archive.open(member) as handle:
                data = handle.read()
            return data, _guess_content_type(member.filename)
    raise RuntimeError("NBCUMV ZIP archive did not contain an image file")


def _guess_content_type(filename: str) -> str | None:
    lowered = str(filename or "").lower()
    if lowered.endswith(".jpg") or lowered.endswith(".jpeg"):
        return "image/jpeg"
    if lowered.endswith(".png"):
        return "image/png"
    if lowered.endswith(".webp"):
        return "image/webp"
    return None


def extract_embedded_metadata(image_bytes: bytes) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "exif": {},
        "xmp": {},
        "iptc": {},
    }
    try:
        with Image.open(io.BytesIO(image_bytes)) as image:
            image = ImageOps.exif_transpose(image)
            metadata["dimensions"] = {
                "width": int(image.width),
                "height": int(image.height),
                "mode": str(image.mode),
                "format": str(image.format or ""),
            }
            metadata["exif"] = _extract_exif_dict(image)
            raw_xmp = _extract_raw_xmp(image_bytes)
            metadata["xmp_raw"] = raw_xmp
            if hasattr(image, "getxmp"):
                try:
                    metadata["xmp"] = _json_safe_value(image.getxmp() or {})
                except Exception:
                    metadata["xmp"] = {}
            if "icc_profile" in image.info:
                metadata["icc_profile_bytes"] = len(image.info.get("icc_profile") or b"")
    except Exception as exc:
        logger.warning("Failed to extract embedded metadata from NBCUMV image: %s", exc)
        metadata["error"] = str(exc)

    metadata["iptc"] = _extract_iptc_dict(image_bytes)
    metadata["extracted_at"] = datetime.now(UTC).isoformat()
    sanitized = _json_safe_value(metadata)
    return sanitized if isinstance(sanitized, dict) else metadata


def _json_safe_value(value: Any) -> Any:
    if value is None or isinstance(value, (int, float, bool)):
        return value
    if isinstance(value, str):
        return re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F]", "", value)
    if isinstance(value, bytes):
        try:
            decoded = value.decode("utf-8", errors="replace")
        except Exception:
            return value.hex()
        return re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F]", "", decoded)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Fraction):
        as_float = float(value)
        return int(as_float) if as_float.is_integer() else as_float
    if hasattr(value, "numerator") and hasattr(value, "denominator"):
        try:
            as_float = float(value)
        except Exception:
            return str(value)
        return int(as_float) if as_float.is_integer() else as_float
    if isinstance(value, dict):
        return {str(key): _json_safe_value(inner) for key, inner in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe_value(item) for item in value]
    return str(value)


def _extract_exif_dict(image: Image.Image) -> dict[str, Any]:
    exif = image.getexif()
    result: dict[str, Any] = {}
    for tag_id, value in exif.items():
        tag_name = ExifTags.TAGS.get(tag_id, str(tag_id))
        result[tag_name] = _json_safe_value(value)
    return result


def _extract_raw_xmp(image_bytes: bytes) -> str | None:
    match = re.search(rb"<x:xmpmeta[\s\S]*?</x:xmpmeta>", image_bytes)
    if not match:
        return None
    try:
        return match.group(0).decode("utf-8", errors="replace")
    except Exception:
        return None


def _extract_iptc_dict(image_bytes: bytes) -> dict[str, Any]:
    photoshop_signature = b"Photoshop 3.0\x00"
    start = image_bytes.find(photoshop_signature)
    if start < 0:
        return {}

    offset = start + len(photoshop_signature)
    resources: dict[str, Any] = {}

    while offset < len(image_bytes) - 12:
        if image_bytes[offset : offset + 4] != b"8BIM":
            offset += 1
            continue

        offset += 4
        resource_id = int.from_bytes(image_bytes[offset : offset + 2], "big")
        offset += 2

        name_length = image_bytes[offset]
        offset += 1
        offset += name_length
        if (1 + name_length) % 2 == 1:
            offset += 1

        size = int.from_bytes(image_bytes[offset : offset + 4], "big")
        offset += 4
        data = image_bytes[offset : offset + size]
        offset += size
        if size % 2 == 1:
            offset += 1

        if resource_id != 0x0404:
            continue

        for key, value in _parse_iptc_dataset_block(data).items():
            resources[key] = value

    return resources


def _parse_iptc_dataset_block(data: bytes) -> dict[str, Any]:
    index = 0
    parsed: dict[str, Any] = {}
    while index + 5 <= len(data):
        if data[index] != 0x1C:
            index += 1
            continue
        record = data[index + 1]
        dataset = data[index + 2]
        size = int.from_bytes(data[index + 3 : index + 5], "big")
        index += 5
        value_bytes = data[index : index + size]
        index += size

        key = _IPTC_DATASET_NAMES.get((record, dataset), f"{record}:{dataset}")
        value = value_bytes.decode("utf-8", errors="replace").strip()
        if not value:
            continue
        current = parsed.get(key)
        if current is None:
            parsed[key] = value
        elif isinstance(current, list):
            current.append(value)
        else:
            parsed[key] = [current, value]
    return parsed
