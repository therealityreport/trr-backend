from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from datetime import date, datetime
from html import unescape
from typing import Any, Iterable, Mapping
from urllib.parse import urlencode

import requests
from bs4 import BeautifulSoup

_IMDB_TITLE_ID_RE = re.compile(r"^(tt[0-9]+)$")
_IMDB_TITLE_HREF_RE = re.compile(r"/title/(tt[0-9]+)/")


class ImdbTitleMetadataClientError(RuntimeError):
    def __init__(self, message: str, *, status_code: int | None = None, body_snippet: str | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.body_snippet = body_snippet


@dataclass(frozen=True)
class ImdbTitlePageMetadata:
    title: str | None
    network: str | None
    total_seasons: int | None
    total_episodes: int | None


@dataclass(frozen=True)
class ImdbEpisodeInfo:
    season: int | None
    episode: int | None
    title: str | None
    air_date: str | None  # YYYY-MM-DD when parseable
    imdb_episode_id: str | None


@dataclass(frozen=True)
class ImdbEpisodesPageMetadata:
    available_seasons: list[int]
    episodes: list[ImdbEpisodeInfo]


def _as_int(value: Any) -> int | None:
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        raw = value.strip()
        if raw.isdigit():
            return int(raw)
    return None


def _iter_jsonld_objects(soup: BeautifulSoup) -> Iterable[dict[str, Any]]:
    for script in soup.find_all("script", attrs={"type": "application/ld+json"}):
        text = script.string or script.get_text()
        if not text:
            continue
        try:
            data = json.loads(text)
        except Exception:
            continue

        if isinstance(data, dict):
            yield data
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    yield item


def _extract_name_list(value: Any) -> list[str]:
    if isinstance(value, str):
        val = unescape(value).strip()
        return [val] if val else []
    if isinstance(value, dict):
        name = value.get("name")
        if isinstance(name, str):
            val = unescape(name).strip()
            return [val] if val else []
        return []
    if isinstance(value, list):
        names: list[str] = []
        for v in value:
            names.extend(_extract_name_list(v))
        return names
    return []


def parse_imdb_title_page(html: str) -> ImdbTitlePageMetadata:
    soup = BeautifulSoup(html, "html.parser")

    # Prefer JSON-LD; it's stable and easy to fixture.
    for obj in _iter_jsonld_objects(soup):
        obj_type = obj.get("@type")
        if obj_type and obj_type not in {"TVSeries", "TVMiniSeries"}:
            continue

        title = obj.get("name")
        title_str = unescape(title).strip() if isinstance(title, str) and title.strip() else None

        total_seasons = _as_int(obj.get("numberOfSeasons"))
        total_episodes = _as_int(obj.get("numberOfEpisodes"))

        # Network isn't consistently available in JSON-LD; use best-effort keys.
        network_names: list[str] = []
        for key in ("publisher", "productionCompany", "provider"):
            network_names.extend(_extract_name_list(obj.get(key)))
        network = ", ".join(dict.fromkeys([n for n in network_names if n])) or None

        return ImdbTitlePageMetadata(
            title=title_str,
            network=network,
            total_seasons=total_seasons,
            total_episodes=total_episodes,
        )

    # Next.js data fallback (tolerant scan).
    next_script = soup.find("script", attrs={"id": "__NEXT_DATA__", "type": "application/json"})
    if next_script and next_script.string:
        try:
            next_data = json.loads(next_script.string)
        except Exception:
            next_data = None

        if isinstance(next_data, (dict, list)):
            networks: list[str] = []
            total_seasons: int | None = None
            total_episodes: int | None = None

            def walk(obj: Any) -> None:
                nonlocal total_seasons, total_episodes
                if isinstance(obj, dict):
                    if total_seasons is None:
                        total_seasons = _as_int(obj.get("numberOfSeasons"))
                    if total_episodes is None:
                        total_episodes = _as_int(obj.get("numberOfEpisodes"))

                    nets = obj.get("networks")
                    if isinstance(nets, list):
                        for n in nets:
                            if isinstance(n, dict):
                                name = n.get("name")
                                if isinstance(name, str) and name.strip():
                                    networks.append(unescape(name).strip())

                    for v in obj.values():
                        walk(v)
                elif isinstance(obj, list):
                    for v in obj:
                        walk(v)

            walk(next_data)
            network = ", ".join(dict.fromkeys([n for n in networks if n])) or None
            return ImdbTitlePageMetadata(
                title=None,
                network=network,
                total_seasons=total_seasons,
                total_episodes=total_episodes,
            )

    return ImdbTitlePageMetadata(title=None, network=None, total_seasons=None, total_episodes=None)


_AIRDATE_PREFIX_RE = re.compile(r"^(?:Air date|Aired|Released)\\s*:?\\s*", re.I)


def _parse_air_date(text: str) -> str | None:
    raw = _AIRDATE_PREFIX_RE.sub("", (text or "").strip())
    if not raw:
        return None

    # Try ISO first.
    try:
        return date.fromisoformat(raw).isoformat()
    except ValueError:
        pass

    # Normalize common IMDb formats.
    normalized = raw.replace(".", "")
    for fmt in ("%b %d, %Y", "%d %b %Y", "%Y %b %d"):
        try:
            return datetime.strptime(normalized, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def parse_imdb_episodes_page(html: str, *, season: int | None = None) -> ImdbEpisodesPageMetadata:
    soup = BeautifulSoup(html, "html.parser")

    seasons: set[int] = set()
    for select in soup.find_all("select"):
        for opt in select.find_all("option"):
            value = opt.get("value")
            text = opt.get_text(" ", strip=True)
            season_val = _as_int(value) if value is not None else _as_int(text)
            if season_val is not None:
                seasons.add(season_val)

    for a in soup.find_all("a", href=True):
        href = a.get("href")
        if not isinstance(href, str):
            continue
        if "season=" not in href:
            continue
        # Parse season query param without relying on URL parsing for robustness.
        m = re.search(r"[?&]season=([0-9]+)", href)
        if m:
            seasons.add(int(m.group(1)))

    episode_cards: list[ImdbEpisodeInfo] = []

    for a in soup.find_all("a", href=True):
        href = a.get("href")
        if not isinstance(href, str):
            continue
        match = _IMDB_TITLE_HREF_RE.search(href)
        if not match:
            continue

        imdb_episode_id = match.group(1)
        container = a.find_parent(["article", "div", "li"]) or a.parent
        if container is None:
            continue

        title = a.get_text(" ", strip=True) or None
        title = unescape(title).strip() if isinstance(title, str) else None
        if not title:
            continue

        episode_no: int | None = None
        for attr in ("data-episode-number", "data-episode", "data-episode-num"):
            episode_no = _as_int(container.get(attr))
            if episode_no is not None:
                break
        if episode_no is None:
            meta = container.find(attrs={"itemprop": "episodeNumber"})
            if meta:
                episode_no = _as_int(meta.get("content") or meta.get_text(" ", strip=True))

        air_date: str | None = None
        air_el = container.find(class_=re.compile(r"airdate|date", re.I))
        if air_el:
            air_date = _parse_air_date(air_el.get_text(" ", strip=True))
        if air_date is None:
            # Fallback to scanning container text for an ISO date.
            m = re.search(r"\\b(20[0-9]{2}-[0-9]{2}-[0-9]{2})\\b", container.get_text(" ", strip=True))
            if m:
                air_date = m.group(1)

        # Keep only plausible episode links.
        if air_date is None and episode_no is None:
            continue

        episode_cards.append(
            ImdbEpisodeInfo(
                season=season,
                episode=episode_no,
                title=title,
                air_date=air_date,
                imdb_episode_id=imdb_episode_id,
            )
        )

    return ImdbEpisodesPageMetadata(available_seasons=sorted(seasons), episodes=episode_cards)


def pick_most_recent_episode(episodes: Iterable[ImdbEpisodeInfo]) -> ImdbEpisodeInfo | None:
    items = list(episodes)
    if not items:
        return None

    def key(ep: ImdbEpisodeInfo) -> tuple[int, str]:
        # Prefer parseable dates; fall back to list order.
        return (1 if ep.air_date else 0, ep.air_date or "")

    best = max(items, key=key)
    if best.air_date:
        return best
    return items[-1]


class HttpImdbTitleMetadataClient:
    def __init__(
        self,
        *,
        session: requests.Session | None = None,
        extra_headers: Mapping[str, str] | None = None,
        timeout_seconds: float = 30.0,
        sleep_ms: int = 0,
    ) -> None:
        self._session = session or requests.Session()
        self._extra_headers = dict(extra_headers or {})
        self._timeout_seconds = timeout_seconds
        self._sleep_ms = max(0, int(sleep_ms))

    def _get(self, url: str, *, params: Mapping[str, Any] | None = None) -> str:
        if self._sleep_ms:
            time.sleep(self._sleep_ms / 1000.0)

        headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "accept-language": "en-US,en;q=0.9",
            "user-agent": "Mozilla/5.0",
            **self._extra_headers,
        }

        try:
            resp = self._session.get(url, params=params, headers=headers, timeout=self._timeout_seconds)
        except requests.RequestException as exc:
            raise ImdbTitleMetadataClientError(f"IMDb request failed: {exc}") from exc

        if resp.status_code != 200:
            raise ImdbTitleMetadataClientError(
                f"IMDb request failed with HTTP {resp.status_code}.",
                status_code=resp.status_code,
                body_snippet=(resp.text or "")[:200],
            )

        return resp.text or ""

    def fetch_title_page(self, imdb_series_id: str) -> str:
        imdb_series_id = str(imdb_series_id).strip()
        if not _IMDB_TITLE_ID_RE.match(imdb_series_id):
            raise ValueError(f"Invalid IMDb id: {imdb_series_id!r}")
        return self._get(f"https://www.imdb.com/title/{imdb_series_id}/")

    def fetch_episodes_page(self, imdb_series_id: str, *, season: int | None = None) -> str:
        imdb_series_id = str(imdb_series_id).strip()
        if not _IMDB_TITLE_ID_RE.match(imdb_series_id):
            raise ValueError(f"Invalid IMDb id: {imdb_series_id!r}")

        params: dict[str, Any] | None = None
        if season is not None:
            params = {"season": str(int(season))}

        url = f"https://www.imdb.com/title/{imdb_series_id}/episodes/"
        # Embed params into the URL only for error readability.
        if params:
            url_for_error = f"{url}?{urlencode(params)}"
        else:
            url_for_error = url
        try:
            return self._get(url, params=params)
        except ImdbTitleMetadataClientError as exc:
            raise ImdbTitleMetadataClientError(
                f"{exc} ({url_for_error})",
                status_code=getattr(exc, "status_code", None),
                body_snippet=getattr(exc, "body_snippet", None),
            ) from exc

