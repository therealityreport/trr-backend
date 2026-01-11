from __future__ import annotations

import gzip
import random
import re
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Iterable, Mapping
from pathlib import Path
from urllib.parse import quote, urlparse

try:
    import requests
except Exception:  # noqa: BLE001
    requests = None

from bs4 import BeautifulSoup

_IMDB_ID_RE = re.compile(r"^tt\d+$", re.IGNORECASE)
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
_BASE_ID_RE = re.compile(r"^(?P<base>.+?)\._V\d+_", re.IGNORECASE)
_BASE_ID_FALLBACK_RE = re.compile(r"^(?P<base>.+?)\._V\d+", re.IGNORECASE)
_SIZE_RE = re.compile(r"U[XY](\d+)", re.IGNORECASE)


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


def _log_fetch_issue(imdb_id: str, status: int | None, message: str) -> None:
    status_part = f"status={status}" if status is not None else "status=unknown"
    print(f"IMDb images: imdb_id={imdb_id} {status_part} {message}", file=sys.stderr)


def _fetch_html_once(
    url: str,
    *,
    timeout_seconds: float,
    headers: Mapping[str, str] | None,
) -> tuple[int | None, str | None, str | None]:
    merged_headers = _merge_headers(headers)
    if requests is not None:
        try:
            resp = requests.get(url, headers=merged_headers, timeout=(5, timeout_seconds))
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


def fetch_imdb_mediaindex_html(imdb_id: str) -> str | None:
    imdb_id = str(imdb_id or "").strip()
    if not _IMDB_ID_RE.match(imdb_id):
        raise ValueError(f"Invalid IMDb id: {imdb_id!r}")

    urls = [
        f"https://www.imdb.com/title/{quote(imdb_id)}/mediaindex/?ref_=ttmi_mi_all",
        f"https://www.imdb.com/title/{quote(imdb_id)}/mediaindex/",
    ]
    max_attempts = 3
    for url in urls:
        for attempt in range(1, max_attempts + 1):
            status, html, error = _fetch_html_once(url, timeout_seconds=20.0, headers=None)
            if status == 200 and html:
                return html
            if status in (403, 404):
                _log_fetch_issue(imdb_id, status, "blocked/unavailable")
                break
            if status in _TRANSIENT_STATUSES or status is None:
                if attempt < max_attempts:
                    _sleep_backoff(attempt)
                    continue
                _log_fetch_issue(imdb_id, status, "request failed")
                break
            if error:
                _log_fetch_issue(imdb_id, status, "request failed")
            break
    return None


_SRCSET_DESC_RE = re.compile(r"^\d+(?:\.\d+)?[wx]$")


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


def _candidate_width(url: str, descriptor: str | None) -> int:
    url_score = _image_quality_score(url)
    if descriptor:
        if descriptor.endswith("w"):
            try:
                return int(descriptor[:-1])
            except ValueError:
                return url_score
        if descriptor.endswith("x"):
            try:
                scale = float(descriptor[:-1])
            except ValueError:
                scale = 0.0
            return url_score or int(scale * 1000)
    return url_score


def _pick_best_url(srcset: str | None, src: str | None) -> str | None:
    candidates = _parse_srcset(srcset or "")
    if candidates:
        scored: list[tuple[int, str]] = []
        for url, desc in candidates:
            scored.append((_candidate_width(url, desc), url))
        scored.sort(key=lambda item: item[0], reverse=True)
        best = scored[0][1] if scored else None
        return best or candidates[-1][0]
    return src or None


def _normalize_image_url(url: str | None) -> str | None:
    if not url:
        return None
    trimmed = url.strip()
    if trimmed.startswith("//"):
        trimmed = f"https:{trimmed}"
    return trimmed


def _image_base_key(url: str) -> str:
    parsed = urlparse(url)
    filename = (parsed.path or "").rsplit("/", 1)[-1]
    stem = filename.rsplit(".", 1)[0]
    if not stem:
        return url
    if "._V" in stem:
        return stem.split("._V", 1)[0]
    if "._" in stem:
        return stem.split("._", 1)[0]
    match = _BASE_ID_RE.match(stem) or _BASE_ID_FALLBACK_RE.match(stem)
    if match:
        return match.group("base")
    return stem


def _image_quality_score(url: str) -> int:
    matches = [int(val) for val in _SIZE_RE.findall(url) if val.isdigit()]
    if matches:
        return max(matches)
    return 0


def extract_imdb_image_urls(html: str, limit: int = 30) -> list[str]:
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    images = soup.select('section[data-testid="section-images"] img.ipc-image')
    if not images:
        images = soup.select('a[data-testid^="mosaic-img-"] img.ipc-image')
    urls: list[str] = []
    base_order: list[str] = []
    best_by_base: dict[str, tuple[str, int]] = {}
    for img in images:
        src = img.get("src")
        srcset = img.get("srcset")
        picked = _normalize_image_url(_pick_best_url(srcset, src))
        if not picked:
            continue
        if not picked.startswith(_MEDIA_AMAZON_PREFIX):
            continue
        base_source = _normalize_image_url(src) if isinstance(src, str) else None
        if base_source and base_source.startswith(_MEDIA_AMAZON_PREFIX):
            base_key = _image_base_key(base_source)
        else:
            base_key = _image_base_key(picked)
        score = _image_quality_score(picked)
        existing = best_by_base.get(base_key)
        if existing is None:
            best_by_base[base_key] = (picked, score)
            base_order.append(base_key)
        else:
            if score > existing[1]:
                best_by_base[base_key] = (picked, score)
    limit = max(1, int(limit))
    for base_key in base_order:
        url = best_by_base[base_key][0]
        urls.append(url)
        if len(urls) >= limit:
            break
    return urls


def extract_imdb_image_width(url: str) -> int | None:
    score = _image_quality_score(url)
    return score or None


def _iter_chunks(resp: Iterable[bytes]) -> Iterable[bytes]:
    for chunk in resp:
        if chunk:
            yield chunk


def download_image(url: str, dest_path: Path) -> None:
    dest_path = Path(dest_path)
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    headers = _merge_headers({"referer": "https://www.imdb.com/"})

    for attempt in range(1, 4):
        tmp_path = dest_path.with_suffix(dest_path.suffix + ".part")
        try:
            if requests is not None:
                with requests.get(url, headers=headers, stream=True, timeout=(5, 20)) as resp:
                    status = resp.status_code
                    if status in _TRANSIENT_STATUSES:
                        raise RuntimeError(f"HTTP {status}")
                    if status != 200:
                        raise RuntimeError(f"HTTP {status}")
                    content_type = resp.headers.get("content-type", "")
                    if not content_type.startswith("image/"):
                        raise RuntimeError(f"Unexpected content-type {content_type!r}")
                    written = 0
                    with tmp_path.open("wb") as handle:
                        for chunk in _iter_chunks(resp.iter_content(chunk_size=65536)):
                            handle.write(chunk)
                            written += len(chunk)
                    if written <= 0:
                        raise RuntimeError("Empty response body")
            else:
                request = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(request, timeout=20) as resp:
                    status = resp.getcode()
                    if status in _TRANSIENT_STATUSES:
                        raise RuntimeError(f"HTTP {status}")
                    if status != 200:
                        raise RuntimeError(f"HTTP {status}")
                    content_type = resp.headers.get("Content-Type", "")
                    if not content_type.startswith("image/"):
                        raise RuntimeError(f"Unexpected content-type {content_type!r}")
                    written = 0
                    with tmp_path.open("wb") as handle:
                        while True:
                            chunk = resp.read(65536)
                            if not chunk:
                                break
                            handle.write(chunk)
                            written += len(chunk)
                    if written <= 0:
                        raise RuntimeError("Empty response body")
            tmp_path.replace(dest_path)
            return
        except Exception as exc:  # noqa: BLE001
            if tmp_path.exists():
                tmp_path.unlink(missing_ok=True)
            if attempt >= 3:
                raise RuntimeError(f"Failed to download image: {exc}") from exc
            _sleep_backoff(attempt)
