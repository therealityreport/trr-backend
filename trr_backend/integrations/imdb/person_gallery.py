from __future__ import annotations

import gzip
import random
import re
import time
from typing import Any, Mapping
from urllib.parse import quote, urlparse
import urllib.error
import urllib.request

try:
    import requests
except Exception:  # noqa: BLE001
    requests = None

from bs4 import BeautifulSoup

_IMDB_NAME_ID_RE = re.compile(r"^nm\d+$", re.IGNORECASE)
_MEDIA_AMAZON_PREFIX = "https://m.media-amazon.com/images/"
_DEFAULT_HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "accept-language": "en-US,en;q=0.9",
    "user-agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Mobile Safari/537.36",
    "sec-ch-ua": '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
    "sec-ch-ua-mobile": "?1",
    "sec-ch-ua-platform": '"Android"',
    "referer": "https://www.imdb.com/",
}
_TRANSIENT_STATUSES = {429, 500, 502, 503, 504}
_SRCSET_DESC_RE = re.compile(r"^\d+(?:\.\d+)?[wx]$")
_WIDTH_RE = re.compile(r"UX(\d+)", re.IGNORECASE)
_HEIGHT_RE = re.compile(r"UY(\d+)", re.IGNORECASE)
_VIEWER_ID_RE = re.compile(r"/mediaviewer/(rm\d+)/", re.IGNORECASE)
_NAME_ID_RE = re.compile(r"/name/(nm\d+)/", re.IGNORECASE)
_TITLE_ID_RE = re.compile(r"/title/(tt\d+)/", re.IGNORECASE)
_GALLERY_COUNT_RE = re.compile(r"(\d+)\s+of\s+(\d+)", re.IGNORECASE)


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


def _maybe_decompress(data: bytes, content_encoding: str | None) -> bytes:
    if content_encoding and "gzip" in content_encoding.lower():
        try:
            return gzip.decompress(data)
        except OSError:
            return data
    return data


def _decode_bytes(data: bytes, content_type: str | None) -> str:
    charset = _parse_charset(content_type) or "utf-8"
    try:
        return data.decode(charset)
    except (LookupError, UnicodeDecodeError):
        return data.decode("utf-8", errors="replace")


def _sleep_backoff(attempt: int) -> None:
    base = min(20.0, 2**attempt)
    jitter = random.uniform(0.0, 0.4)
    time.sleep(base + jitter)


def _fetch_html_once(
    url: str,
    *,
    timeout_seconds: float,
    headers: Mapping[str, str] | None,
    session: requests.Session | None,
) -> tuple[int | None, str | None, str | None]:
    merged_headers = _merge_headers(headers)
    if requests is not None:
        try:
            requester = session or requests
            resp = requester.get(url, headers=merged_headers, timeout=(5, timeout_seconds))
        except requests.RequestException as exc:
            return None, None, str(exc)
        text = _decode_bytes(resp.content or b"", resp.headers.get("content-type"))
        return resp.status_code, text, None

    request = urllib.request.Request(url, headers=merged_headers)
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as resp:
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


def fetch_imdb_person_mediaindex_html(imdb_person_id: str, *, session: requests.Session | None = None) -> str:
    imdb_person_id = str(imdb_person_id or "").strip()
    if not _IMDB_NAME_ID_RE.match(imdb_person_id):
        raise ValueError(f"Invalid IMDb person id: {imdb_person_id!r}")

    url = f"https://m.imdb.com/name/{quote(imdb_person_id)}/mediaindex/"
    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        status, html, error = _fetch_html_once(url, timeout_seconds=20.0, headers=None, session=session)
        if status == 200 and html:
            return html
        if status in (403, 404):
            raise RuntimeError(f"IMDb mediaindex unavailable for {imdb_person_id} (HTTP {status}).")
        if status in _TRANSIENT_STATUSES or status is None:
            if attempt < max_attempts:
                _sleep_backoff(attempt)
                continue
        if error:
            raise RuntimeError(f"IMDb mediaindex request failed: {error}")
    raise RuntimeError(f"IMDb mediaindex request failed for {imdb_person_id}.")


def fetch_imdb_person_mediaviewer_html(
    imdb_person_id: str,
    viewer_id: str,
    *,
    session: requests.Session | None = None,
) -> str:
    imdb_person_id = str(imdb_person_id or "").strip()
    viewer_id = str(viewer_id or "").strip()
    if not _IMDB_NAME_ID_RE.match(imdb_person_id):
        raise ValueError(f"Invalid IMDb person id: {imdb_person_id!r}")
    if not viewer_id:
        raise ValueError("viewer_id is required")

    url = f"https://m.imdb.com/name/{quote(imdb_person_id)}/mediaviewer/{quote(viewer_id)}/"
    max_attempts = 3
    for attempt in range(1, max_attempts + 1):
        status, html, error = _fetch_html_once(url, timeout_seconds=20.0, headers=None, session=session)
        if status == 200 and html:
            return html
        if status in (403, 404):
            raise RuntimeError(f"IMDb mediaviewer unavailable for {viewer_id} (HTTP {status}).")
        if status in _TRANSIENT_STATUSES or status is None:
            if attempt < max_attempts:
                _sleep_backoff(attempt)
                continue
        if error:
            raise RuntimeError(f"IMDb mediaviewer request failed: {error}")
    raise RuntimeError(f"IMDb mediaviewer request failed for {viewer_id}.")


def _split_srcset(srcset: str) -> list[str]:
    raw = srcset or ""
    parts: list[str] = []
    buf: list[str] = []
    i = 0
    length = len(raw)
    while i < length:
        ch = raw[i]
        if ch == ",":
            j = i + 1
            while j < length and raw[j].isspace():
                j += 1
            lookahead = raw[j : j + 8].lower()
            if lookahead.startswith("http://") or lookahead.startswith("https://") or lookahead.startswith("//"):
                part = "".join(buf).strip()
                if part:
                    parts.append(part)
                buf = []
                i += 1
                continue
        buf.append(ch)
        i += 1
    tail = "".join(buf).strip()
    if tail:
        parts.append(tail)
    return parts


def _parse_srcset(srcset: str) -> list[tuple[str, str | None]]:
    candidates: list[tuple[str, str | None]] = []
    for part in _split_srcset(srcset):
        tokens = part.replace("\n", " ").split()
        if not tokens:
            continue
        url = tokens[0].strip().rstrip(",")
        if not (url.startswith("http://") or url.startswith("https://") or url.startswith("//")):
            continue
        descriptor: str | None = None
        if len(tokens) > 1:
            candidate = tokens[1].strip().rstrip(",")
            if _SRCSET_DESC_RE.match(candidate):
                descriptor = candidate
        candidates.append((url, descriptor))
    return candidates


def _extract_width_from_url(url: str) -> int | None:
    match = _WIDTH_RE.search(url)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def _extract_height_from_url(url: str) -> int | None:
    match = _HEIGHT_RE.search(url)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


def _candidate_width(url: str, descriptor: str | None) -> int | None:
    url_width = _extract_width_from_url(url)
    if descriptor:
        if descriptor.endswith("w"):
            try:
                return int(descriptor[:-1])
            except ValueError:
                return url_width
        if descriptor.endswith("x"):
            try:
                scale = float(descriptor[:-1])
            except ValueError:
                scale = 0.0
            return url_width or int(scale * 1000)
    return url_width


def _pick_best_candidate(srcset: str | None, src: str | None) -> tuple[str | None, int | None]:
    candidates = _parse_srcset(srcset or "")
    if candidates:
        best: tuple[int, str] | None = None
        for url, desc in candidates:
            width = _candidate_width(url, desc)
            score = width or 0
            if best is None or score > best[0]:
                best = (score, url)
        return (best[1] if best else candidates[-1][0], best[0] if best else None)
    if src:
        return src, _extract_width_from_url(src)
    return None, None


def _normalize_image_url(url: str | None) -> str | None:
    if not url:
        return None
    trimmed = url.strip()
    if trimmed.startswith("//"):
        trimmed = f"https:{trimmed}"
    return trimmed


def _extract_source_image_id_from_path(path: str | None) -> str | None:
    if not path:
        return None
    filename = path.rsplit("/", 1)[-1]
    if not filename:
        return None
    if "._V" in filename:
        return filename.split("._V", 1)[0]
    if "._" in filename:
        return filename.split("._", 1)[0]
    return None


def parse_imdb_person_mediaindex_images(html: str, imdb_person_id: str) -> list[dict[str, Any]]:
    imdb_person_id = str(imdb_person_id or "").strip()
    if not _IMDB_NAME_ID_RE.match(imdb_person_id):
        raise ValueError(f"Invalid IMDb person id: {imdb_person_id!r}")
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    section = soup.select_one('section[data-testid="section-images"]')
    images = section.select("img") if section else []
    if not images:
        images = soup.select('a[data-testid^="mosaic-img-"] img')

    ordered_ids: list[str] = []
    best_by_id: dict[str, dict[str, Any]] = {}

    for img in images:
        src = img.get("src")
        srcset = img.get("srcset")
        picked, picked_width = _pick_best_candidate(srcset, src)
        picked = _normalize_image_url(picked)
        if not picked or not picked.startswith(_MEDIA_AMAZON_PREFIX):
            continue

        url_path = urlparse(picked).path
        source_image_id = _extract_source_image_id_from_path(url_path)

        link = img.find_parent("a")
        href = link.get("href") if link else None
        viewer_id = None
        if isinstance(href, str):
            match = _VIEWER_ID_RE.search(href)
            if match:
                viewer_id = match.group(1)
        if not source_image_id:
            source_image_id = viewer_id
        if not source_image_id:
            continue

        width = picked_width or _extract_width_from_url(picked)
        height = _extract_height_from_url(picked)

        row = {
            "imdb_person_id": imdb_person_id,
            "source_image_id": source_image_id,
            "viewer_id": viewer_id,
            "mediaviewer_url_path": href,
            "url": picked,
            "url_path": url_path,
            "width": width,
            "height": height,
        }

        existing = best_by_id.get(source_image_id)
        if existing is None:
            best_by_id[source_image_id] = row
            ordered_ids.append(source_image_id)
            continue

        existing_width = existing.get("width")
        replace = False
        if width is not None and (existing_width is None or width > existing_width):
            replace = True

        if replace:
            if not row.get("viewer_id") and existing.get("viewer_id"):
                row["viewer_id"] = existing.get("viewer_id")
            if not row.get("mediaviewer_url_path") and existing.get("mediaviewer_url_path"):
                row["mediaviewer_url_path"] = existing.get("mediaviewer_url_path")
            best_by_id[source_image_id] = row
        else:
            if not existing.get("viewer_id") and row.get("viewer_id"):
                existing["viewer_id"] = row.get("viewer_id")
            if not existing.get("mediaviewer_url_path") and row.get("mediaviewer_url_path"):
                existing["mediaviewer_url_path"] = row.get("mediaviewer_url_path")

    return [best_by_id[source_image_id] for source_image_id in ordered_ids]


def _extract_caption(soup: BeautifulSoup) -> str | None:
    caption_div = soup.find("div", class_=re.compile(r"ipc-html-content-inner-div"))
    if caption_div:
        text = caption_div.get_text(" ", strip=True)
        if text:
            return text

    def _is_mv_desc(tag: Any) -> bool:
        return bool(tag.find("a", href=re.compile(r"ref_=mv_desc")))

    node = soup.find(_is_mv_desc)
    if node:
        text = node.get_text(" ", strip=True)
        if text:
            return text
    return None


def _extract_section_links(soup: BeautifulSoup, label: str, pattern: re.Pattern[str]) -> tuple[list[str], list[str]]:
    label_lower = label.strip().casefold()
    label_nodes = [
        node
        for node in soup.find_all(["span", "div", "h3"])
        if node.get_text(strip=True).casefold() == label_lower
    ]

    for node in label_nodes:
        container = node.parent or node
        links = container.find_all("a", href=pattern)
        if not links and container.parent:
            links = container.parent.find_all("a", href=pattern)
        if not links:
            continue

        ids: list[str] = []
        names: list[str] = []
        seen: set[str] = set()
        for link in links:
            href = link.get("href") or ""
            match = pattern.search(href)
            if not match:
                continue
            imdb_id = match.group(1)
            if imdb_id in seen:
                continue
            seen.add(imdb_id)
            ids.append(imdb_id)
            names.append(link.get_text(strip=True))
        return ids, names

    return [], []


def _select_mediaviewer_images(soup: BeautifulSoup, viewer_id: str | None) -> list[Any]:
    media_viewer = soup.find("div", attrs={"data-testid": "media-viewer"})
    images = media_viewer.find_all("img") if media_viewer else soup.find_all("img")
    if not images:
        return []

    viewer_key = str(viewer_id or "").strip().lower()
    if viewer_key:
        matched = []
        for img in images:
            data_image_id = str(img.get("data-image-id") or "").lower()
            if data_image_id.startswith(viewer_key):
                matched.append(img)
        if matched:
            return matched

    curr = []
    for img in images:
        data_image_id = str(img.get("data-image-id") or "").lower()
        if data_image_id.endswith("-curr"):
            curr.append(img)
    if curr:
        return curr

    return images


def parse_imdb_person_mediaviewer_details(html: str, *, viewer_id: str | None = None) -> dict[str, Any]:
    if not html:
        return {}

    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)
    gallery_index = None
    gallery_total = None
    match = _GALLERY_COUNT_RE.search(text)
    if match:
        try:
            gallery_index = int(match.group(1))
            gallery_total = int(match.group(2))
        except ValueError:
            gallery_index = None
            gallery_total = None

    best_url = None
    best_width = None
    best_height = None
    best_alt = None

    for img in _select_mediaviewer_images(soup, viewer_id):
        src = img.get("src")
        srcset = img.get("srcset")
        picked, picked_width = _pick_best_candidate(srcset, src)
        full_src = _normalize_image_url(src) if isinstance(src, str) else None
        candidate_url = None
        candidate_width = None
        candidate_height = None

        if full_src and full_src.startswith(_MEDIA_AMAZON_PREFIX) and "_V1_" in full_src:
            candidate_url = full_src
            candidate_width = picked_width or _extract_width_from_url(full_src)
            candidate_height = _extract_height_from_url(full_src)
        else:
            picked = _normalize_image_url(picked)
            if picked and picked.startswith(_MEDIA_AMAZON_PREFIX):
                candidate_url = picked
                candidate_width = picked_width or _extract_width_from_url(picked)
                candidate_height = _extract_height_from_url(picked)

        if not candidate_url:
            continue
        if best_url is None or (candidate_width or 0) > (best_width or 0):
            best_url = candidate_url
            best_width = candidate_width
            best_height = candidate_height
            best_alt = img.get("alt")

    caption = _extract_caption(soup)
    if not caption and isinstance(best_alt, str) and best_alt.strip():
        caption = best_alt.strip()

    people_ids, people_names = _extract_section_links(soup, "People", _NAME_ID_RE)
    title_ids, title_names = _extract_section_links(soup, "Titles", _TITLE_ID_RE)

    url_path = urlparse(best_url).path if best_url else None

    return {
        "url": best_url,
        "url_path": url_path,
        "width": best_width,
        "height": best_height,
        "caption": caption,
        "gallery_index": gallery_index,
        "gallery_total": gallery_total,
        "people_imdb_ids": people_ids or None,
        "people_names": people_names or None,
        "title_imdb_ids": title_ids or None,
        "title_names": title_names or None,
    }
