from __future__ import annotations

import gzip
import json
import re
import time
from dataclasses import dataclass, field
from typing import Any, Mapping
from urllib.parse import quote, urlparse
import urllib.error
import urllib.request

try:
    import requests
except Exception:  # noqa: BLE001
    requests = None

from bs4 import BeautifulSoup

_IMDB_TITLE_ID_RE = re.compile(r"^tt\d+$", re.IGNORECASE)
_IMDB_IMAGE_ID_RE = re.compile(r"^rm\d+$", re.IGNORECASE)

_DEFAULT_HEADERS = {
    "accept": "text/html,application/xhtml+xml",
    "accept-language": "en-US,en;q=0.9",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
}


@dataclass(frozen=True)
class ImdbMediaImage:
    imdb_id: str
    imdb_image_id: str
    position: int | None
    caption: str | None
    width: int | None
    height: int | None
    url: str
    viewer_path: str
    viewer_url: str
    image_type: str | None
    metadata: dict[str, Any] = field(default_factory=dict)


def _merge_headers(headers: Mapping[str, str] | None) -> dict[str, str]:
    merged = {**_DEFAULT_HEADERS, **(headers or {})}
    if not any(key.lower() == "accept-encoding" for key in merged):
        merged["accept-encoding"] = "gzip"
    return merged


def _parse_charset(content_type: str | None) -> str | None:
    if not content_type:
        return None
    match = re.search(r"charset=([^\s;]+)", content_type, re.IGNORECASE)
    if not match:
        return None
    return match.group(1).strip("\"'")


def _decode_bytes(data: bytes, content_type: str | None) -> str:
    charset = _parse_charset(content_type) or "utf-8"
    try:
        return data.decode(charset)
    except (LookupError, UnicodeDecodeError):
        return data.decode("utf-8", errors="replace")


def _maybe_decompress(data: bytes, content_encoding: str | None) -> bytes:
    if content_encoding and "gzip" in content_encoding.lower():
        try:
            return gzip.decompress(data)
        except OSError:
            return data
    return data


def fetch_html(
    url: str,
    *,
    timeout: float = 15.0,
    headers: Mapping[str, str] | None = None,
) -> tuple[int | None, str | None, str | None]:
    merged_headers = _merge_headers(headers)
    if requests is not None:
        try:
            resp = requests.get(url, headers=merged_headers, timeout=timeout)
        except requests.RequestException as exc:
            return None, None, str(exc)
        data = resp.content or b""
        text = _decode_bytes(data, resp.headers.get("content-type"))
        return resp.status_code, text, None

    request = urllib.request.Request(url, headers=merged_headers)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as resp:
            data = resp.read() or b""
            data = _maybe_decompress(data, resp.headers.get("Content-Encoding"))
            text = _decode_bytes(data, resp.headers.get("Content-Type"))
            return resp.getcode(), text, None
    except urllib.error.HTTPError as exc:
        data = exc.read() or b""
        data = _maybe_decompress(data, exc.headers.get("Content-Encoding"))
        text = _decode_bytes(data, exc.headers.get("Content-Type"))
        return exc.code, text, str(exc)
    except urllib.error.URLError as exc:
        return None, None, str(exc)


def fetch_imdb_mediaindex_html(
    imdb_id: str,
    *,
    after_cursor: str | None = None,
    build_id: str | None = None,
    timeout_seconds: float = 30.0,
    headers: Mapping[str, str] | None = None,
) -> str:
    imdb_id = str(imdb_id or "").strip()
    if not _IMDB_TITLE_ID_RE.match(imdb_id):
        raise ValueError(f"Invalid IMDb id: {imdb_id!r}")

    if after_cursor and build_id:
        url = (
            f"https://www.imdb.com/_next/data/{quote(build_id)}/title/{quote(imdb_id)}/mediaindex.json"
            f"?after={quote(after_cursor)}"
        )
    else:
        url = f"https://www.imdb.com/title/{quote(imdb_id)}/mediaindex/?ref_=ttmi_mi"
        if after_cursor:
            url = f"{url}&after={quote(after_cursor)}"

    status, html, error = fetch_html(url, timeout=timeout_seconds, headers=headers)
    if error and status is None:
        raise RuntimeError(f"IMDb request failed: {error}")
    if status != 200:
        snippet = (html or "")[:200]
        raise RuntimeError(f"IMDb request failed with HTTP {status}: {snippet}")
    return html or ""


def _extract_payloads(html: str) -> list[dict[str, Any]]:
    payloads: list[dict[str, Any]] = []
    text = (html or "").strip()
    if text.startswith("{"):
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            payload = None
        if isinstance(payload, dict):
            payloads.append(payload)

    soup = BeautifulSoup(html, "html.parser")
    script = soup.find("script", attrs={"id": "__NEXT_DATA__"})
    if script and script.string:
        try:
            data = json.loads(script.string)
        except json.JSONDecodeError:
            data = None
        if isinstance(data, dict):
            payloads.append(data)

    for node in soup.find_all("script", attrs={"type": "application/json"}):
        raw = node.string or node.get_text(strip=True)
        if not raw:
            continue
        raw = raw.strip()
        if raw.startswith("{") or raw.startswith("["):
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if isinstance(data, dict):
                payloads.append(data)

    return payloads


def _find_all_images(node: Any) -> Mapping[str, Any] | None:
    stack = [node]
    while stack:
        current = stack.pop()
        if isinstance(current, Mapping):
            all_images = current.get("all_images")
            if isinstance(all_images, Mapping):
                return all_images
            if {"edges", "pageInfo"} <= set(current.keys()):
                edges = current.get("edges")
                page_info = current.get("pageInfo")
                if isinstance(edges, list) and isinstance(page_info, Mapping):
                    return current
            stack.extend(current.values())
        elif isinstance(current, list):
            stack.extend(current)
    return None


def _extract_page_info(all_images: Mapping[str, Any]) -> dict[str, Any]:
    page_info = all_images.get("pageInfo") if isinstance(all_images.get("pageInfo"), Mapping) else {}
    has_next = page_info.get("hasNextPage")
    if has_next is None:
        has_next = page_info.get("has_next_page")
    end_cursor = page_info.get("endCursor") or page_info.get("end_cursor")
    return {
        "has_next_page": bool(has_next),
        "end_cursor": end_cursor if isinstance(end_cursor, str) and end_cursor.strip() else None,
    }


def _extract_all_images_meta(all_images: Mapping[str, Any]) -> dict[str, Any]:
    meta: dict[str, Any] = {}
    total = all_images.get("total")
    if isinstance(total, int):
        meta["total"] = total
    for key in ("types", "imageTypes", "facets", "imageTypeFacets", "facetsV2"):
        value = all_images.get(key)
        if isinstance(value, (list, dict)):
            meta[key] = value
    return meta


def parse_imdb_mediaindex_html(
    html: str,
    *,
    imdb_id: str,
) -> tuple[list[ImdbMediaImage], dict[str, Any]]:
    imdb_id = str(imdb_id or "").strip()
    if not _IMDB_TITLE_ID_RE.match(imdb_id):
        raise ValueError(f"Invalid IMDb id: {imdb_id!r}")

    payloads = _extract_payloads(html)
    all_images: Mapping[str, Any] | None = None
    build_id: str | None = None
    for payload in payloads:
        if build_id is None and isinstance(payload.get("buildId"), str):
            build_id = payload.get("buildId")
        if all_images is None:
            found = _find_all_images(payload)
            if isinstance(found, Mapping):
                all_images = found

    if not isinstance(all_images, Mapping):
        return [], {"has_next_page": False, "end_cursor": None, "build_id": build_id}

    page_info = _extract_page_info(all_images)
    page_info["build_id"] = build_id
    all_images_meta = _extract_all_images_meta(all_images)

    edges = all_images.get("edges")
    if not isinstance(edges, list):
        return [], page_info

    images: list[ImdbMediaImage] = []
    for edge in edges:
        if not isinstance(edge, Mapping):
            continue
        node = edge.get("node")
        if not isinstance(node, Mapping):
            continue

        image_id = node.get("id")
        if not isinstance(image_id, str) or not _IMDB_IMAGE_ID_RE.match(image_id.strip()):
            continue
        image_id = image_id.strip()

        url = node.get("url")
        if not isinstance(url, str) or not url.strip():
            continue
        url = url.strip()

        width = node.get("width")
        width_val = int(width) if isinstance(width, int) else None
        height = node.get("height")
        height_val = int(height) if isinstance(height, int) else None

        caption = None
        caption_obj = node.get("caption")
        if isinstance(caption_obj, Mapping):
            text = caption_obj.get("plainText")
            if isinstance(text, str) and text.strip():
                caption = text.strip()
        elif isinstance(caption_obj, str) and caption_obj.strip():
            caption = caption_obj.strip()

        position = edge.get("position")
        position_val = int(position) if isinstance(position, int) else None

        image_type = node.get("imageType")
        image_type_val = image_type.strip() if isinstance(image_type, str) and image_type.strip() else None

        viewer_path = f"/title/{imdb_id}/mediaviewer/{image_id}/"
        viewer_url = f"https://www.imdb.com{viewer_path}"

        metadata = {"node": node, "position": position_val}
        if all_images_meta:
            metadata["all_images"] = all_images_meta

        images.append(
            ImdbMediaImage(
                imdb_id=imdb_id,
                imdb_image_id=image_id,
                position=position_val,
                caption=caption,
                width=width_val,
                height=height_val,
                url=url,
                viewer_path=viewer_path,
                viewer_url=viewer_url,
                image_type=image_type_val,
                metadata=metadata,
            )
        )

    return images, page_info


def parse_imdb_mediaindex_images(
    html: str,
    *,
    imdb_id: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    images, page_info = parse_imdb_mediaindex_html(html, imdb_id=imdb_id)
    rows: list[dict[str, Any]] = []
    for image in images:
        url_path = None
        parsed = urlparse(image.url)
        if parsed.path:
            url_path = parsed.path
        metadata = dict(image.metadata or {})
        metadata.setdefault("viewer_url", image.viewer_url)
        metadata.setdefault("viewer_path", image.viewer_path)
        rows.append(
            {
                "imdb_id": image.imdb_id,
                "source": "imdb",
                "source_image_id": image.imdb_image_id,
                "url": image.url,
                "url_path": url_path,
                "image_type": image.image_type,
                "position": image.position,
                "width": image.width,
                "height": image.height,
                "caption": image.caption,
                "metadata": metadata,
                "viewer_path": image.viewer_path,
                "viewer_url": image.viewer_url,
            }
        )
    return rows, page_info


def fetch_imdb_mediaindex_images(
    imdb_id: str,
    *,
    sleep_ms: int = 0,
    max_pages: int = 25,
) -> list[ImdbMediaImage]:
    imdb_id = str(imdb_id or "").strip()
    if not _IMDB_TITLE_ID_RE.match(imdb_id):
        raise ValueError(f"Invalid IMDb id: {imdb_id!r}")

    images: list[ImdbMediaImage] = []
    seen: set[str] = set()
    cursor: str | None = None
    build_id: str | None = None
    pages_fetched = 0

    while True:
        if max_pages is not None and pages_fetched >= max_pages:
            break
        html = fetch_imdb_mediaindex_html(imdb_id, after_cursor=cursor, build_id=build_id)
        parsed, page_info = parse_imdb_mediaindex_html(html, imdb_id=imdb_id)
        pages_fetched += 1

        build_id = page_info.get("build_id") or build_id
        for image in parsed:
            key = image.imdb_image_id.casefold()
            if key in seen:
                continue
            seen.add(key)
            images.append(image)

        next_cursor = page_info.get("end_cursor")
        has_next = page_info.get("has_next_page")
        if not has_next or not next_cursor or next_cursor == cursor:
            break
        cursor = next_cursor
        if sleep_ms > 0:
            time.sleep(sleep_ms / 1000.0)

    return images
