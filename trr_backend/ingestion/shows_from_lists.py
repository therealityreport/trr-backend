from __future__ import annotations

import json
import html as html_lib
import re
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from trr_backend.integrations.imdb.list_graphql_client import HttpImdbListGraphqlClient
from trr_backend.integrations.tmdb.client import fetch_list_items, fetch_tv_external_ids, parse_tmdb_list_id


@dataclass(frozen=True)
class ImdbListItem:
    imdb_id: str
    title: str
    year: int | None = None
    imdb_rating: float | None = None
    imdb_vote_count: int | None = None
    description: str | None = None
    release_year: int | None = None
    end_year: int | None = None
    episodes_total: int | None = None
    title_type: str | None = None
    primary_image_url: str | None = None
    primary_image_caption: str | None = None
    certificate: str | None = None
    runtime_seconds: int | None = None
    genres: tuple[str, ...] = ()
    list_rank: int | None = None
    list_item_note: str | None = None
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
    imdb_meta: dict[str, Any] = field(default_factory=dict)
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
        if other.imdb_meta:
            if not self.imdb_meta:
                self.imdb_meta = dict(other.imdb_meta)
            else:
                for k, v in other.imdb_meta.items():
                    if k not in self.imdb_meta or self.imdb_meta[k] is None:
                        self.imdb_meta[k] = v
        return self


_IMDB_LIST_ID_RE = re.compile(r"(ls[0-9]+)")
_IMDB_TITLE_ID_RE = re.compile(r"/title/(tt[0-9]+)/")
_IMDB_TITLE_ID_FULL_RE = re.compile(r"^(tt[0-9]+)$")
_YEAR_RE = re.compile(r"\b(19|20)[0-9]{2}\b")


def _as_int(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        raw = value.strip()
        if raw.isdigit():
            return int(raw)
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, float):
        return value
    if isinstance(value, int):
        return float(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        try:
            return float(raw)
        except ValueError:
            return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_nonempty_str(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def _dedupe_preserve_order(values: Iterable[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    out: list[str] = []
    for v in values:
        if v in seen:
            continue
        out.append(v)
        seen.add(v)
    return tuple(out)


def _parse_imdb_title_list_main_page_payload(payload: Mapping[str, Any], *, list_id: str) -> tuple[int | None, list[ImdbListItem]]:
    data = payload.get("data")
    if not isinstance(data, Mapping):
        raise RuntimeError("IMDb GraphQL response missing `data`.")

    list_obj = data.get("list")
    if not isinstance(list_obj, Mapping):
        raise RuntimeError("IMDb GraphQL response missing `data.list`.")

    search = list_obj.get("titleListItemSearch")
    if not isinstance(search, Mapping):
        raise RuntimeError("IMDb GraphQL response missing `data.list.titleListItemSearch`.")

    total = _as_int(search.get("total"))
    edges = search.get("edges")
    if not isinstance(edges, list):
        raise RuntimeError("IMDb GraphQL response missing `data.list.titleListItemSearch.edges`.")

    items: list[ImdbListItem] = []
    for edge in edges:
        if not isinstance(edge, Mapping):
            continue

        node = edge.get("node")
        node_map = node if isinstance(node, Mapping) else {}

        list_item = edge.get("listItem")
        if not isinstance(list_item, Mapping):
            continue

        imdb_id = _as_nonempty_str(list_item.get("id"))
        if not imdb_id:
            continue

        title_text_obj = list_item.get("titleText")
        title_text_map = title_text_obj if isinstance(title_text_obj, Mapping) else {}
        title = _as_nonempty_str(title_text_map.get("text")) or imdb_id

        ratings_obj = list_item.get("ratingsSummary")
        ratings_map = ratings_obj if isinstance(ratings_obj, Mapping) else {}
        imdb_rating = _as_float(ratings_map.get("aggregateRating"))
        imdb_vote_count = _as_int(ratings_map.get("voteCount"))

        plot_text: str | None = None
        plot_obj = list_item.get("plot")
        if isinstance(plot_obj, Mapping):
            plot_text_obj = plot_obj.get("plotText")
            if isinstance(plot_text_obj, Mapping):
                plot_text = _as_nonempty_str(plot_text_obj.get("plainText"))

        list_item_note = _as_nonempty_str(node_map.get("description"))
        description = plot_text or list_item_note

        release_year = None
        end_year = None
        release_year_obj = list_item.get("releaseYear")
        if isinstance(release_year_obj, Mapping):
            release_year = _as_int(release_year_obj.get("year"))
            end_year = _as_int(release_year_obj.get("endYear"))

        episodes_total = None
        episodes_obj = list_item.get("episodes")
        if isinstance(episodes_obj, Mapping):
            inner = episodes_obj.get("episodes")
            if isinstance(inner, Mapping):
                episodes_total = _as_int(inner.get("total"))

        title_type_obj = list_item.get("titleType")
        title_type_map = title_type_obj if isinstance(title_type_obj, Mapping) else {}
        title_type = _as_nonempty_str(title_type_map.get("id"))

        primary_image_url = None
        primary_image_caption = None
        primary_image_obj = list_item.get("primaryImage")
        if isinstance(primary_image_obj, Mapping):
            primary_image_url = _as_nonempty_str(primary_image_obj.get("url"))
            caption_obj = primary_image_obj.get("caption")
            if isinstance(caption_obj, Mapping):
                primary_image_caption = _as_nonempty_str(caption_obj.get("plainText"))

        certificate_obj = list_item.get("certificate")
        certificate = None
        if isinstance(certificate_obj, Mapping):
            certificate = _as_nonempty_str(certificate_obj.get("rating"))

        runtime_obj = list_item.get("runtime")
        runtime_seconds = None
        if isinstance(runtime_obj, Mapping):
            runtime_seconds = _as_int(runtime_obj.get("seconds"))

        genres_names: list[str] = []
        title_genres_obj = list_item.get("titleGenres")
        if isinstance(title_genres_obj, Mapping):
            genres_obj = title_genres_obj.get("genres")
            if isinstance(genres_obj, list):
                for g in genres_obj:
                    if not isinstance(g, Mapping):
                        continue
                    genre_obj = g.get("genre")
                    if isinstance(genre_obj, Mapping):
                        name = _as_nonempty_str(genre_obj.get("text"))
                        if name:
                            genres_names.append(name)

        list_rank = _as_int(node_map.get("absolutePosition"))

        items.append(
            ImdbListItem(
                imdb_id=imdb_id,
                title=title,
                year=release_year,
                imdb_rating=imdb_rating,
                imdb_vote_count=imdb_vote_count,
                description=description,
                release_year=release_year,
                end_year=end_year,
                episodes_total=episodes_total,
                title_type=title_type,
                primary_image_url=primary_image_url,
                primary_image_caption=primary_image_caption,
                certificate=certificate,
                runtime_seconds=runtime_seconds,
                genres=_dedupe_preserve_order(genres_names),
                list_rank=list_rank,
                list_item_note=list_item_note,
                extra={"source": "graphql_title_list_main_page", "list_id": list_id},
            )
        )

    return total, items


def parse_imdb_list_id(value: str) -> str:
    """
    Parse an IMDb list identifier from either:
    - a bare id (e.g. "ls4106677119")
    - a share URL (e.g. "https://www.imdb.com/list/ls4106677119/?ref_=ext_shr_lnk")
    """

    raw = str(value).strip()
    if not raw:
        raise ValueError("IMDb list value is empty.")

    if raw.startswith("ls") and raw[2:].isdigit():
        return raw

    match = _IMDB_LIST_ID_RE.search(raw)
    if not match:
        raise ValueError(f"Unable to parse IMDb list id from: {value!r}")
    return match.group(1)


def parse_imdb_list_url(url: str) -> str:
    # Backwards-compatible alias.
    return parse_imdb_list_id(url)


def parse_imdb_list_page(html: str) -> list[ImdbListItem]:
    soup = BeautifulSoup(html, "html.parser")

    # A) JSON-LD ItemList (preferred).
    jsonld_items: list[ImdbListItem] = []
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        text = script.string or script.get_text()
        if not text:
            continue
        try:
            data = json.loads(text)
        except Exception:
            continue

        candidates = data if isinstance(data, list) else [data]
        for candidate in candidates:
            if not isinstance(candidate, dict):
                continue

            items = candidate.get("itemListElement")
            if not isinstance(items, list):
                continue

            for item in items:
                if not isinstance(item, dict):
                    continue

                tv_series = item.get("item")
                if not isinstance(tv_series, dict):
                    continue

                name = tv_series.get("name")
                url = tv_series.get("url") or tv_series.get("@id")
                if not isinstance(name, str) or not name.strip() or not isinstance(url, str):
                    continue

                # JSON-LD within <script> can include HTML entities (e.g. &amp;, &apos;).
                name = html_lib.unescape(name).strip()
                if not name:
                    continue

                match = _IMDB_TITLE_ID_RE.search(url)
                if not match:
                    continue

                extra: dict[str, Any] = {"source": "jsonld", "url": url}
                position = item.get("position")
                if isinstance(position, int):
                    extra["rank"] = position

                jsonld_items.append(
                    ImdbListItem(
                        imdb_id=match.group(1),
                        title=name.strip(),
                        year=None,
                        release_year=None,
                        list_rank=position if isinstance(position, int) else None,
                        extra=extra,
                    )
                )

            if jsonld_items:
                # JSON-LD doesn't usually include year/description; keep None and rely on other paths if needed.
                return jsonld_items

    # B) __NEXT_DATA__ JSON (secondary).
    next_script = soup.find("script", attrs={"id": "__NEXT_DATA__", "type": "application/json"})
    if next_script and next_script.string:
        try:
            next_data = json.loads(next_script.string)
        except Exception:
            next_data = None

        if isinstance(next_data, (dict, list)):
            parsed_by_id: dict[str, ImdbListItem] = {}

            def walk(obj: Any) -> None:
                if isinstance(obj, dict):
                    title_id = obj.get("id")
                    if isinstance(title_id, str) and _IMDB_TITLE_ID_FULL_RE.match(title_id):
                        title_text: str | None = None
                        title_text_obj = obj.get("titleText")
                        if isinstance(title_text_obj, dict):
                            raw_text = title_text_obj.get("text")
                            if isinstance(raw_text, str) and raw_text.strip():
                                title_text = raw_text.strip()
                        elif isinstance(title_text_obj, str) and title_text_obj.strip():
                            title_text = title_text_obj.strip()

                        if title_text:
                            title_text = html_lib.unescape(title_text).strip()
                            if title_text:
                                year = None
                                release_year_obj = obj.get("releaseYear")
                                if isinstance(release_year_obj, dict):
                                    year_value = release_year_obj.get("year")
                                    if isinstance(year_value, int):
                                        year = year_value
                                    elif isinstance(year_value, str) and year_value.isdigit():
                                        year = int(year_value)

                                parsed_by_id.setdefault(
                                    title_id,
                                    ImdbListItem(
                                        imdb_id=title_id,
                                        title=title_text,
                                        year=year,
                                        release_year=year,
                                        extra={"source": "next_data"},
                                    ),
                                )

                    for v in obj.values():
                        walk(v)
                elif isinstance(obj, list):
                    for v in obj:
                        walk(v)

            walk(next_data)
            if parsed_by_id:
                return list(parsed_by_id.values())

    # C) HTML fallback (classic + IPC layouts).
    parsed: list[ImdbListItem] = []

    def parse_rank_and_title(text: str) -> tuple[int | None, str]:
        m = re.match(r"^([0-9]+)\.\s*(.+)$", text.strip())
        if not m:
            return None, text.strip()
        return int(m.group(1)), m.group(2).strip()

    def infer_year(container: Any) -> int | None:
        # Try "year" class first, then fallback to regex match.
        year_el = container.find("span", class_=re.compile("year", re.I)) if container else None
        if year_el:
            m = _YEAR_RE.search(year_el.get_text(" ", strip=True))
            if m:
                return int(m.group(0))
        blob = container.get_text(" ", strip=True) if container else ""
        m = _YEAR_RE.search(blob)
        if m:
            return int(m.group(0))
        return None

    def infer_description(container: Any) -> str | None:
        for p in container.find_all("p") if container else []:
            text = p.get_text(" ", strip=True)
            if not text:
                continue
            if len(text) < 20:
                continue
            return text
        return None

    def infer_rank(container: Any, title_text: str | None) -> int | None:
        if title_text:
            rank, _ = parse_rank_and_title(title_text)
            if rank is not None:
                return rank
        idx = container.find("span", class_=re.compile("index", re.I)) if container else None
        if idx:
            m = re.search(r"([0-9]+)", idx.get_text(" ", strip=True))
            if m:
                return int(m.group(1))
        return None

    def container_is_too_broad(container: Any) -> bool:
        name = getattr(container, "name", "")
        return name in {"html", "body", "main"}

    containers = soup.select("li.ipc-metadata-list-summary-item") or soup.select("div.lister-item") or soup.select(
        "div.lister-item-content"
    )

    def parse_container(container: Any) -> None:
        link = None
        for a in container.find_all("a", href=True):
            href_val = a.get("href")
            if not isinstance(href_val, str):
                continue
            if _IMDB_TITLE_ID_RE.search(href_val):
                link = a
                break
        if not link:
            return

        href_val = link.get("href", "")
        match = _IMDB_TITLE_ID_RE.search(href_val)
        if not match:
            return
        imdb_id = match.group(1)
        if imdb_id in seen:
            return
        seen.add(imdb_id)

        title_text: str | None = None
        header = container.find(["h3", "h4"])
        if header:
            title_text = header.get_text(" ", strip=True)
        if not title_text:
            title_text = link.get_text(" ", strip=True)
        if not title_text:
            return

        rank_from_title, title = parse_rank_and_title(title_text)
        rank = rank_from_title or infer_rank(container, title_text)

        is_broad = container_is_too_broad(container)
        year = None if is_broad else infer_year(container)
        description = None if is_broad else infer_description(container)

        extra: dict[str, Any] = {"source": "html", "href": href_val}
        if rank is not None:
            extra["rank"] = rank
        if description:
            extra["description"] = description

        parsed.append(
            ImdbListItem(
                imdb_id=imdb_id,
                title=title,
                year=year,
                release_year=year,
                description=description,
                list_rank=rank,
                extra=extra,
            )
        )

    seen: set[str] = set()
    if containers:
        for container in containers:
            parse_container(container)
        return parsed

    # No recognizable per-item containers: fall back to scanning all title links.
    for a in soup.find_all("a", href=True):
        href = a.get("href")
        if not isinstance(href, str):
            continue
        match = _IMDB_TITLE_ID_RE.search(href)
        if not match:
            continue
        imdb_id = match.group(1)
        if imdb_id in seen:
            continue

        # Prefer nearest item-ish container; avoid parsing the full page as a container.
        container = a.find_parent(["li", "div"]) or a.parent
        if container and container_is_too_broad(container):
            container = None

        if container:
            parse_container(container)
            continue

        title_text = a.get_text(" ", strip=True)
        if not title_text:
            continue
        _, title = parse_rank_and_title(title_text)
        seen.add(imdb_id)
        parsed.append(
            ImdbListItem(
                imdb_id=imdb_id,
                title=title,
                year=None,
                release_year=None,
                extra={"source": "html", "href": href},
            )
        )

    return parsed


def _find_next_imdb_list_page(soup: BeautifulSoup, current_url: str) -> str | None:
    base_url = current_url.split("?", 1)[0]
    if not base_url.endswith("/"):
        base_url = f"{base_url}/"
    return _find_next_imdb_list_page_for_list(soup, current_url, base_url=base_url)


def _find_next_imdb_list_page_for_list(soup: BeautifulSoup, current_url: str, *, base_url: str) -> str | None:
    for a in soup.find_all("a", href=True):
        aria = (a.get("aria-label") or "").strip()
        text = a.get_text(strip=True)
        if "next" not in (aria + " " + text).casefold():
            continue
        href = a.get("href")
        if not isinstance(href, str) or not href.strip():
            continue
        if "page=" not in href:
            continue
        candidate = urljoin(current_url, href)
        if not candidate.startswith(base_url):
            continue
        return candidate
    return None


def fetch_imdb_list_items(
    list_url_or_id: str,
    *,
    session: requests.Session | None = None,
    max_pages: int = 25,
    use_graphql: bool = True,
    graphql_locale: str = "en-US",
) -> list[ImdbListItem]:
    session = session or requests.Session()
    list_id = parse_imdb_list_id(list_url_or_id)

    if use_graphql:
        try:
            client = HttpImdbListGraphqlClient(session=session)

            items_by_id: dict[str, ImdbListItem] = {}
            total: int | None = None

            first = 250
            jump_to_position = 1

            for _ in range(max_pages):
                payload = client.fetch_title_list_main_page(
                    list_id,
                    locale=graphql_locale,
                    first=first,
                    jump_to_position=jump_to_position,
                )

                page_total, page_items = _parse_imdb_title_list_main_page_payload(payload, list_id=list_id)
                if total is None and page_total is not None:
                    total = page_total

                new_ids = 0
                for item in page_items:
                    if item.imdb_id in items_by_id:
                        continue
                    items_by_id[item.imdb_id] = item
                    new_ids += 1

                if total == 0:
                    print(f"IMDb list {list_id}: fetched 0 items via GraphQL TitleListMainPage", file=sys.stderr)
                    return []

                if total is not None and len(items_by_id) >= total:
                    break

                if not page_items or new_ids == 0:
                    break

                jump_to_position += first

            if total is not None and len(items_by_id) < total:
                raise RuntimeError(f"incomplete GraphQL pagination ({len(items_by_id)}/{total})")

            print(
                f"IMDb list {list_id}: fetched {len(items_by_id)} items via GraphQL TitleListMainPage",
                file=sys.stderr,
            )
            return list(items_by_id.values())
        except Exception as exc:  # noqa: BLE001
            print(
                f"IMDb list {list_id}: GraphQL TitleListMainPage failed ({exc.__class__.__name__}: {exc}); "
                "falling back to HTML parsing.",
                file=sys.stderr,
            )

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "en-US,en;q=0.9",
    }

    base_url = f"https://www.imdb.com/list/{list_id}/"
    url = base_url
    visited: set[str] = set()
    page_num = 1
    items_by_id: dict[str, ImdbListItem] = {}

    def fetch_html(url: str) -> str:
        last_status = None
        last_text = ""
        for attempt in range(3):
            resp = session.get(url, headers=headers, timeout=30)
            last_status = resp.status_code
            last_text = resp.text or ""
            if resp.status_code == 200:
                return last_text
            if resp.status_code == 429 or 500 <= resp.status_code < 600:
                if attempt < 2:
                    time.sleep(1.0 * (2**attempt))
                    continue
            break
        snippet = last_text[:200].replace("\n", " ").strip()
        raise RuntimeError(f"IMDb list fetch failed (HTTP {last_status}) for {url}. Snippet: {snippet}")

    while url and url not in visited and page_num <= max_pages:
        print(f"IMDb list {list_id}: fetching page {page_num}…", file=sys.stderr)
        visited.add(url)
        try:
            html = fetch_html(url)
        except RuntimeError:
            if page_num == 1:
                raise
            break
        page_items = parse_imdb_list_page(html)

        new_ids = 0
        for item in page_items:
            if item.imdb_id in items_by_id:
                continue
            items_by_id[item.imdb_id] = item
            new_ids += 1

        if page_items:
            print(f"IMDb list {list_id}: parsed {len(page_items)} items ({new_ids} new)", file=sys.stderr)

        # Stop when the page doesn't yield items, or yields no new ids.
        if not page_items or new_ids == 0:
            break

        soup = BeautifulSoup(html, "html.parser")
        next_url = _find_next_imdb_list_page_for_list(soup, url, base_url=base_url)
        if next_url:
            url = next_url
            page_num += 1
            continue

        page_num += 1
        url = f"{base_url}?page={page_num}"

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

    tv_items: list[dict[str, Any]] = []
    for item in raw_items:
        if not isinstance(item, dict):
            continue
        media_type = item.get("media_type")
        if media_type and media_type != "tv":
            continue
        tv_items.append(item)

    if resolve_external_ids:
        print(
            f"TMDb list {list_id}: fetched {len(tv_items)} tv items; resolving IMDb ids via /tv/{{id}}/external_ids…",
            file=sys.stderr,
        )
    else:
        print(f"TMDb list {list_id}: fetched {len(tv_items)} tv items.", file=sys.stderr)

    for idx, item in enumerate(tv_items, start=1):
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
            if idx == 1 or idx % 10 == 0 or idx == len(tv_items):
                print(f"TMDb list {list_id}: external ids {idx}/{len(tv_items)}", file=sys.stderr)

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
