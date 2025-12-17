from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any, Iterable
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from trr_backend.integrations.tmdb.client import fetch_list_items, fetch_tv_external_ids, parse_tmdb_list_id


@dataclass(frozen=True)
class ImdbListItem:
    imdb_id: str
    title: str
    year: int | None = None
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class TmdbListItem:
    tmdb_id: int
    imdb_id: str | None
    name: str
    first_air_date: str | None = None
    origin_country: list[str] | None = None
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class CandidateShow:
    imdb_id: str | None
    tmdb_id: int | None
    title: str
    year: int | None = None
    first_air_date: str | None = None
    origin_country: list[str] | None = None
    source_tags: set[str] = field(default_factory=set)

    def merge(self, other: "CandidateShow") -> "CandidateShow":
        self.source_tags |= other.source_tags
        self.imdb_id = self.imdb_id or other.imdb_id
        self.tmdb_id = self.tmdb_id or other.tmdb_id

        if not self.title and other.title:
            self.title = other.title

        self.year = self.year or other.year
        self.first_air_date = self.first_air_date or other.first_air_date
        self.origin_country = self.origin_country or other.origin_country
        return self


_IMDB_LIST_ID_RE = re.compile(r"(ls[0-9]+)")
_IMDB_TITLE_ID_RE = re.compile(r"/title/(tt[0-9]+)/")


def parse_imdb_list_url(url: str) -> str:
    match = _IMDB_LIST_ID_RE.search(url)
    if not match:
        raise ValueError(f"Unable to parse IMDb list id from: {url!r}")
    return match.group(1)


def _parse_imdb_items_from_soup(soup: BeautifulSoup) -> list[ImdbListItem]:
    # Strategy 1: JSON-LD structured data (preferred).
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        text = script.string
        if not text:
            continue
        try:
            data = json.loads(text)
        except Exception:
            continue

        if not isinstance(data, dict):
            continue

        items = data.get("itemListElement")
        if not isinstance(items, list):
            continue

        parsed: list[ImdbListItem] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            tv_series = item.get("item")
            if not isinstance(tv_series, dict):
                continue
            if tv_series.get("@type") not in {"TVSeries", "TVSeriesSeason"}:
                # Lists can mix media; we only want series-level records.
                continue
            name = tv_series.get("name")
            url = tv_series.get("url")
            if not isinstance(name, str) or not name.strip() or not isinstance(url, str):
                continue
            match = _IMDB_TITLE_ID_RE.search(url)
            if not match:
                continue
            parsed.append(ImdbListItem(imdb_id=match.group(1), title=name.strip(), extra={"url": url}))

        if parsed:
            return parsed

    # Strategy 2: HTML parsing fallback.
    parsed: list[ImdbListItem] = []
    for li in soup.find_all("li", class_="ipc-metadata-list-summary-item"):
        link = li.find("a", class_="ipc-title-link-wrapper")
        if not link:
            continue
        href = link.get("href", "")
        title_h3 = link.find("h3")
        if not title_h3:
            continue
        match = _IMDB_TITLE_ID_RE.search(href)
        if not match:
            continue
        raw_title = title_h3.get_text(strip=True)
        title = re.sub(r"^[0-9]+\\.\\s*", "", raw_title).strip()
        if not title:
            continue
        parsed.append(ImdbListItem(imdb_id=match.group(1), title=title, extra={"href": href}))

    return parsed


def _find_next_imdb_list_page(soup: BeautifulSoup, current_url: str) -> str | None:
    for a in soup.find_all("a", href=True):
        aria = (a.get("aria-label") or "").strip()
        text = a.get_text(strip=True)
        if "next" not in (aria + " " + text).casefold():
            continue
        href = a.get("href")
        if not isinstance(href, str) or not href.strip():
            continue
        return urljoin(current_url, href)
    return None


def fetch_imdb_list_items(
    list_id: str,
    *,
    session: requests.Session | None = None,
    max_pages: int = 25,
) -> list[ImdbListItem]:
    session = session or requests.Session()
    headers = {"User-Agent": "Mozilla/5.0"}

    url = f"https://www.imdb.com/list/{list_id}/"
    visited: set[str] = set()
    page_num = 1

    items_by_id: dict[str, ImdbListItem] = {}

    while url and url not in visited and page_num <= max_pages:
        visited.add(url)
        resp = session.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, "html.parser")

        for item in _parse_imdb_items_from_soup(soup):
            items_by_id.setdefault(item.imdb_id, item)

        next_url = _find_next_imdb_list_page(soup, url)
        if next_url:
            url = next_url
            page_num += 1
            continue

        # Fallback pagination pattern (IMDb commonly supports ?page=N).
        candidate_next = f"https://www.imdb.com/list/{list_id}/?page={page_num + 1}"
        if candidate_next in visited:
            break

        try:
            test_resp = session.get(candidate_next, headers=headers, timeout=20)
            if test_resp.status_code != 200:
                break
            test_soup = BeautifulSoup(test_resp.content, "html.parser")
            new_items = _parse_imdb_items_from_soup(test_soup)
            if not new_items:
                break
            url = candidate_next
            page_num += 1
        except requests.RequestException:
            break

    return list(items_by_id.values())


def fetch_tmdb_list_items(
    list_id_or_url: str | int,
    *,
    api_key: str | None = None,
    session: requests.Session | None = None,
    resolve_external_ids: bool = True,
) -> list[TmdbListItem]:
    session = session or requests.Session()
    list_id = parse_tmdb_list_id(list_id_or_url)

    raw_items = fetch_list_items(list_id, api_key=api_key, session=session)

    imdb_id_cache: dict[int, str | None] = {}
    parsed: list[TmdbListItem] = []

    for item in raw_items:
        media_type = item.get("media_type")
        if media_type and media_type != "tv":
            continue
        tmdb_id = item.get("id")
        if not isinstance(tmdb_id, int):
            continue

        name = item.get("name") or item.get("title") or ""
        if not isinstance(name, str) or not name.strip():
            continue

        first_air_date = item.get("first_air_date")
        if not isinstance(first_air_date, str) or not first_air_date.strip():
            first_air_date = None

        origin_country = item.get("origin_country")
        if not isinstance(origin_country, list):
            origin_country = None
        else:
            origin_country = [str(c) for c in origin_country if str(c).strip()]

        imdb_id: str | None = None
        if resolve_external_ids:
            if tmdb_id not in imdb_id_cache:
                try:
                    ext = fetch_tv_external_ids(tmdb_id, api_key=api_key, session=session)
                    raw_imdb = ext.get("imdb_id")
                    imdb_id_cache[tmdb_id] = raw_imdb if isinstance(raw_imdb, str) and raw_imdb else None
                except Exception:
                    imdb_id_cache[tmdb_id] = None
            imdb_id = imdb_id_cache[tmdb_id]

        parsed.append(
            TmdbListItem(
                tmdb_id=tmdb_id,
                imdb_id=imdb_id,
                name=name.strip(),
                first_air_date=first_air_date,
                origin_country=origin_country,
                extra={"tmdb_list_id": list_id},
            )
        )

    return parsed


def merge_candidate_shows(
    imdb_items: Iterable[ImdbListItem],
    tmdb_items: Iterable[TmdbListItem],
    *,
    imdb_source_tag: str,
    tmdb_source_tag: str,
) -> list[CandidateShow]:
    candidates: list[CandidateShow] = []

    for item in imdb_items:
        candidates.append(
            CandidateShow(
                imdb_id=item.imdb_id,
                tmdb_id=None,
                title=item.title,
                year=item.year,
                source_tags={imdb_source_tag},
            )
        )

    for item in tmdb_items:
        candidates.append(
            CandidateShow(
                imdb_id=item.imdb_id,
                tmdb_id=item.tmdb_id,
                title=item.name,
                first_air_date=item.first_air_date,
                origin_country=item.origin_country,
                source_tags={tmdb_source_tag},
            )
        )

    return merge_candidates(candidates)


def merge_candidates(candidates: Iterable[CandidateShow]) -> list[CandidateShow]:
    candidates_by_imdb: dict[str, CandidateShow] = {}
    candidates_by_tmdb: dict[int, CandidateShow] = {}
    loose_candidates: list[CandidateShow] = []

    def upsert(candidate: CandidateShow) -> None:
        if candidate.imdb_id:
            existing = candidates_by_imdb.get(candidate.imdb_id)
            if existing:
                existing.merge(candidate)
                return
        if candidate.tmdb_id is not None:
            existing = candidates_by_tmdb.get(candidate.tmdb_id)
            if existing:
                existing.merge(candidate)
                if candidate.imdb_id:
                    candidates_by_imdb.setdefault(candidate.imdb_id, existing)
                return

        loose_candidates.append(candidate)
        if candidate.imdb_id:
            candidates_by_imdb[candidate.imdb_id] = candidate
        if candidate.tmdb_id is not None:
            candidates_by_tmdb[candidate.tmdb_id] = candidate

    for c in candidates:
        upsert(c)

    # Optional strict merge by (title, year) for loose candidates without IDs.
    def key_for_title_year(c: CandidateShow) -> tuple[str, int] | None:
        year = c.year
        if year is None and c.first_air_date:
            try:
                year = int(c.first_air_date[:4])
            except ValueError:
                year = None
        if year is None:
            return None
        return (c.title.strip().casefold(), year)

    merged_by_title_year: dict[tuple[str, int], CandidateShow] = {}
    result: list[CandidateShow] = []
    for c in loose_candidates:
        if c.imdb_id or c.tmdb_id is not None:
            result.append(c)
            continue
        key = key_for_title_year(c)
        if not key:
            result.append(c)
            continue
        existing = merged_by_title_year.get(key)
        if existing:
            existing.merge(c)
        else:
            merged_by_title_year[key] = c
            result.append(c)

    return result
