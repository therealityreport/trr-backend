"""Bounded, soft-failing IMDb name-filmography enrichment."""

from __future__ import annotations

import html as html_module
import re
from typing import Any

import requests

_IMDB_PERSON_ID_RE = re.compile(r"^nm\d+$", re.IGNORECASE)
_TITLE_ANCHOR_RE = re.compile(
    r'<a[^>]+href="(/title/(tt\d+)/\?ref_=([^"]+))"[^>]*>([\s\S]*?)</a>',
    re.IGNORECASE,
)
_TAG_RE = re.compile(r"<[^>]*>")
_BASE_URL = "https://m.imdb.com/name"
_TITLE_BASE_URL = "https://www.imdb.com/title"
_TIMEOUT = (1.0, 2.0)
_MAX_HTML_BYTES = 2_000_000
_MAX_CREDITS = 500


def parse_name_filmography_html(value: str, *, max_credits: int = _MAX_CREDITS) -> list[dict[str, str]]:
    credits: list[dict[str, str]] = []
    seen: set[str] = set()
    for match in _TITLE_ANCHOR_RE.finditer(value or ""):
        imdb_title_id = (match.group(2) or "").strip().casefold()
        reference = (match.group(3) or "").strip()
        if not imdb_title_id or imdb_title_id in seen:
            continue
        if "nm_flmg_job_" not in reference or not re.search(r"_cdt_t_\d+", reference, re.IGNORECASE):
            continue
        title = html_module.unescape(_TAG_RE.sub(" ", match.group(4) or ""))
        title = re.sub(r"\s+", " ", title).strip()
        if not title:
            continue
        seen.add(imdb_title_id)
        credits.append(
            {
                "imdb_title_id": imdb_title_id,
                "show_name": title,
                "external_url": f"{_TITLE_BASE_URL}/{imdb_title_id}/",
            }
        )
        if len(credits) >= max(max_credits, 0):
            break
    return credits


def fetch_name_filmography(
    imdb_person_id: str,
    *,
    max_credits: int = _MAX_CREDITS,
) -> list[dict[str, str]]:
    """Fetch one bounded page; any source failure degrades to no enrichment."""

    normalized_id = str(imdb_person_id or "").strip()
    if not _IMDB_PERSON_ID_RE.fullmatch(normalized_id):
        return []

    response: Any | None = None
    try:
        response = requests.get(
            f"{_BASE_URL}/{normalized_id}/fullcredits",
            headers={
                "user-agent": "Mozilla/5.0",
                "accept-language": "en-US,en;q=0.9",
            },
            timeout=_TIMEOUT,
            stream=True,
        )
        if response.status_code != 200:
            return []
        body = bytearray()
        for chunk in response.iter_content(chunk_size=64 * 1024):
            if not chunk:
                continue
            remaining = _MAX_HTML_BYTES - len(body)
            if remaining <= 0:
                break
            body.extend(chunk[:remaining])
            if len(body) >= _MAX_HTML_BYTES:
                break
        return parse_name_filmography_html(
            body.decode("utf-8", errors="replace"),
            max_credits=max_credits,
        )
    except Exception:  # noqa: BLE001 - optional enrichment must preserve local credits.
        return []
    finally:
        if response is not None:
            response.close()
