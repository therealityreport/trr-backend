from __future__ import annotations

import csv
import io
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import requests

SHOWINFO_OVERRIDES_URL = (
    "https://docs.google.com/spreadsheets/d/e/"
    "2PACX-1vRYEzj8yhYYYH_f0aLjvcZ-BgViFPRO2bItefo7jLgcKGWX41zLHZx9OqI4b4qZdE4r7inurNI9fESQ/"
    "pubhtml?gid=1787707970&single=true"
)

_IMDB_ID_RE = re.compile(r"(tt[0-9]+)", re.IGNORECASE)
_TMDB_ID_RE = re.compile(r"([0-9]+)")
_WS_RE = re.compile(r"\s+")


class ShowInfoOverridesError(RuntimeError):
    pass


@dataclass(frozen=True)
class ShowOverride:
    min_episodes: int | None
    skip: bool
    raw: str


@dataclass(frozen=True)
class ShowOverrideIndex:
    by_imdb_id: dict[str, ShowOverride]
    by_tmdb_id: dict[int, ShowOverride]
    by_title_key: dict[str, ShowOverride]

    def lookup(
        self,
        *,
        imdb_id: str | None = None,
        tmdb_id: int | str | None = None,
        title: str | None = None,
        network: str | None = None,
    ) -> ShowOverride | None:
        if imdb_id:
            key = str(imdb_id).strip().lower()
            if key:
                override = self.by_imdb_id.get(key)
                if override is not None:
                    return override

        if tmdb_id is not None:
            try:
                tmdb_key = int(str(tmdb_id).strip())
            except ValueError:
                tmdb_key = None
            if tmdb_key is not None:
                override = self.by_tmdb_id.get(tmdb_key)
                if override is not None:
                    return override

        title_key = _normalize_title_key(title, network)
        if title_key:
            return self.by_title_key.get(title_key)
        return None


def _to_csv_url(pub_url: str) -> str:
    parsed = urlparse(pub_url)
    path = parsed.path.replace("/pubhtml", "/pub")
    query = parse_qs(parsed.query)
    query["output"] = ["csv"]
    new_query = urlencode(query, doseq=True)
    return urlunparse(parsed._replace(path=path, query=new_query))


def _normalize_header(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (value or "").strip().lower())


def _header_index_map(headers: list[str]) -> dict[str, int]:
    return {_normalize_header(h): idx for idx, h in enumerate(headers)}


def _find_header_index(header_map: dict[str, int], candidates: list[str]) -> int | None:
    for candidate in candidates:
        idx = header_map.get(candidate)
        if idx is not None:
            return idx
    return None


def _extract_imdb_id(value: Any) -> str | None:
    if not isinstance(value, str):
        value = str(value or "")
    match = _IMDB_ID_RE.search(value)
    if not match:
        return None
    return match.group(1).lower()


def _extract_tmdb_id(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    raw = str(value).strip()
    if raw.isdigit():
        return int(raw)
    match = _TMDB_ID_RE.search(raw)
    if match:
        return int(match.group(1))
    return None


def _normalize_text(value: Any) -> str | None:
    if not isinstance(value, str):
        value = str(value or "")
    text = _WS_RE.sub(" ", value.strip()).casefold()
    return text or None


def _normalize_title_key(title: Any, network: Any) -> str | None:
    title_norm = _normalize_text(title)
    if not title_norm:
        return None
    network_norm = _normalize_text(network)
    if network_norm:
        return f"{title_norm}::{network_norm}"
    return title_norm


def _parse_override_value(value: Any) -> ShowOverride | None:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None

    upper = raw.upper()
    if upper == "SKIP":
        return ShowOverride(min_episodes=None, skip=True, raw=raw)
    if upper == "Y":
        return None

    if raw.isdigit():
        return ShowOverride(min_episodes=int(raw), skip=False, raw=raw)

    return None


def fetch_showinfo_overrides(
    *,
    url: str | None = None,
    session: requests.Session | None = None,
    timeout_seconds: float = 20.0,
) -> ShowOverrideIndex:
    resolved_url = _to_csv_url(url or SHOWINFO_OVERRIDES_URL)
    session = session or requests.Session()

    try:
        resp = session.get(resolved_url, timeout=timeout_seconds)
    except requests.RequestException as exc:
        raise ShowInfoOverridesError(f"ShowInfo overrides request failed: {exc}") from exc

    if resp.status_code != 200:
        raise ShowInfoOverridesError(f"ShowInfo overrides request failed with HTTP {resp.status_code}.")

    text = resp.text or ""
    reader = csv.reader(io.StringIO(text))
    rows = list(reader)
    if not rows:
        return ShowOverrideIndex(by_imdb_id={}, by_tmdb_id={}, by_title_key={})

    headers = [str(h or "") for h in rows[0]]
    header_map = _header_index_map(headers)

    override_idx = _find_header_index(header_map, ["override", "overrides"])
    if override_idx is None:
        raise ShowInfoOverridesError("ShowInfo overrides missing OVERRIDE column.")

    imdb_idx = _find_header_index(header_map, ["imdbseriesid", "imdbid", "imdb"])
    tmdb_idx = _find_header_index(header_map, ["tmdbseriesid", "tmdbid", "tmdb"])
    show_idx = _find_header_index(header_map, ["show", "showname", "title"])
    network_idx = _find_header_index(header_map, ["network", "channel"])

    by_imdb_id: dict[str, ShowOverride] = {}
    by_tmdb_id: dict[int, ShowOverride] = {}
    by_title_key: dict[str, ShowOverride] = {}

    for row in rows[1:]:
        if not row:
            continue
        override_cell = row[override_idx] if override_idx < len(row) else ""
        override = _parse_override_value(override_cell)
        if override is None:
            continue

        imdb_id = _extract_imdb_id(row[imdb_idx]) if imdb_idx is not None and imdb_idx < len(row) else None
        tmdb_id = _extract_tmdb_id(row[tmdb_idx]) if tmdb_idx is not None and tmdb_idx < len(row) else None
        show_name = row[show_idx] if show_idx is not None and show_idx < len(row) else None
        network = row[network_idx] if network_idx is not None and network_idx < len(row) else None

        if imdb_id:
            by_imdb_id[imdb_id] = override
        if tmdb_id is not None:
            by_tmdb_id[tmdb_id] = override

        title_key = _normalize_title_key(show_name, network)
        if title_key:
            by_title_key[title_key] = override

    return ShowOverrideIndex(by_imdb_id=by_imdb_id, by_tmdb_id=by_tmdb_id, by_title_key=by_title_key)
