from __future__ import annotations

import json
import os
import re
import time
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime
from html import unescape
from typing import Any
from urllib.parse import urlencode

import requests
from bs4 import BeautifulSoup

from trr_backend.utils.playwright_runtime import exclusive_runtime_lock

_IMDB_TITLE_ID_RE = re.compile(r"^(tt[0-9]+)$")
_IMDB_TITLE_HREF_RE = re.compile(r"/title/(tt[0-9]+)/")
_SCRAPLING_RUNTIME_LOCK_NAME = "imdb-episodes-scrapling"


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


@dataclass(frozen=True)
class ImdbSeasonEpisode:
    season: int
    episode: int
    imdb_episode_id: str | None
    title: str | None
    air_date: str | None  # YYYY-MM-DD when parseable
    overview: str | None
    imdb_rating: float | None
    imdb_vote_count: int | None
    imdb_primary_image_url: str | None
    imdb_primary_image_caption: str | None
    imdb_primary_image_width: int | None
    imdb_primary_image_height: int | None


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
    next_data = _extract_next_data_json(html)
    if isinstance(next_data, Mapping):
        parsed = parse_imdb_episodes_payload(next_data, season=season)
        if parsed.available_seasons or parsed.episodes:
            return parsed

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


def _extract_next_data_json(html: str) -> dict[str, Any] | None:
    soup = BeautifulSoup(html, "html.parser")
    script = soup.find("script", attrs={"id": "__NEXT_DATA__"})
    if not script or not script.string:
        return None
    try:
        data = json.loads(script.string)
    except Exception:
        return None
    return data if isinstance(data, dict) else None


def _env_flag(name: str, *, default: bool) -> bool:
    raw = str(os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw not in {"0", "false", "no", "off"}


def _scrapling_response_html(page: Any) -> str:
    body = getattr(page, "body", None)
    if isinstance(body, bytes):
        return body.decode(getattr(page, "encoding", None) or "utf-8", errors="replace")
    if isinstance(body, str):
        return body

    for attr in ("html", "content", "text"):
        value = getattr(page, attr, None)
        if callable(value):
            value = value()
        if isinstance(value, bytes):
            return value.decode(getattr(page, "encoding", None) or "utf-8", errors="replace")
        if isinstance(value, str) and value:
            return value

    return ""


def _looks_like_episodes_html(html: str) -> bool:
    normalized = html or ""
    if "__NEXT_DATA__" in normalized and "/title/tt" in normalized:
        return True
    return (
        "ipc-title__text" in normalized
        and "/title/tt" in normalized
        and ("episode" in normalized.lower() or "season" in normalized.lower())
    )


def _fetch_episodes_page_via_scrapling(
    url: str,
    *,
    extra_headers: Mapping[str, str],
    timeout_seconds: float,
    verbose: bool,
) -> str | None:
    if not _env_flag("IMDB_EPISODES_SCRAPLING_FALLBACK_ENABLED", default=True):
        return None

    try:
        from scrapling.fetchers import StealthyFetcher
    except Exception as exc:
        if verbose:
            print(f"IMDb episodes Scrapling fallback unavailable: {exc}")
        return None

    wait_selector = "#__NEXT_DATA__, [data-testid='episodes-section'], .ipc-title__text"
    timeout_ms = max(int(timeout_seconds * 1000), 30_000)

    try:
        with exclusive_runtime_lock(_SCRAPLING_RUNTIME_LOCK_NAME):
            page = StealthyFetcher.fetch(
                url,
                headless=True,
                network_idle=True,
                wait_selector=wait_selector,
                timeout=timeout_ms,
                extra_headers=dict(extra_headers),
            )
    except RuntimeError as exc:
        if verbose and not str(exc).startswith("browser_runtime_locked:"):
            print(f"IMDb episodes Scrapling fallback lock failed: {exc}")
        return None
    except Exception as exc:
        if verbose:
            print(f"IMDb episodes Scrapling fallback failed: {exc}")
        return None

    status = getattr(page, "status", None)
    html = _scrapling_response_html(page)
    if status == 200 and _looks_like_episodes_html(html):
        if verbose:
            print(f"IMDb episodes Scrapling fallback succeeded for {url}.")
        return html

    if verbose:
        print(
            "IMDb episodes Scrapling fallback did not load episodes "
            f"(status={status}, bytes={len(html.encode('utf-8', errors='ignore'))})."
        )
    return None


def _extract_imdb_episode_items_from_next_data(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    def _try_path(root: Mapping[str, Any], path: list[str]) -> list[Mapping[str, Any]] | None:
        node: Any = root
        for key in path:
            if not isinstance(node, Mapping):
                return None
            node = node.get(key)
        if isinstance(node, list):
            return [i for i in node if isinstance(i, Mapping)]
        return None

    items = _try_path(payload, ["props", "pageProps", "contentData", "section", "episodes", "items"])
    if items is None:
        items = _try_path(payload, ["pageProps", "contentData", "section", "episodes", "items"])
    if isinstance(items, list):
        return [i for i in items if isinstance(i, Mapping)]

    # Fallback: tolerant scan for an `items` list that looks like episode cards.
    out: list[Mapping[str, Any]] = []
    stack: list[Any] = [payload]
    while stack and len(out) < 2000:
        node = stack.pop()
        if isinstance(node, Mapping):
            maybe_items = node.get("items")
            if isinstance(maybe_items, list) and maybe_items and all(isinstance(x, Mapping) for x in maybe_items[:3]):
                sample = maybe_items[0]
                if isinstance(sample, Mapping) and {"id", "season", "episode", "titleText"} <= set(sample.keys()):
                    out.extend([x for x in maybe_items if isinstance(x, Mapping)])
                    break
            stack.extend(list(node.values()))
        elif isinstance(node, list):
            stack.extend(node)
    return out


def _extract_imdb_available_seasons_from_next_data(payload: Mapping[str, Any]) -> list[int]:
    def _try_path(root: Mapping[str, Any], path: list[str]) -> Any:
        node: Any = root
        for key in path:
            if not isinstance(node, Mapping):
                return None
            node = node.get(key)
        return node

    seasons = _try_path(payload, ["props", "pageProps", "contentData", "data", "title", "episodes", "seasons"])
    if seasons is None:
        seasons = _try_path(payload, ["pageProps", "contentData", "data", "title", "episodes", "seasons"])

    out: list[int] = []
    if isinstance(seasons, list):
        for item in seasons:
            if isinstance(item, Mapping):
                num = _as_int(item.get("number"))
                if num is not None:
                    out.append(num)
            else:
                num = _as_int(item)
                if num is not None:
                    out.append(num)
    return sorted(set(out))


def _parse_imdb_release_date(value: Any) -> str | None:
    if isinstance(value, str):
        return _parse_air_date(value)
    if isinstance(value, Mapping):
        year = _as_int(value.get("year"))
        month = _as_int(value.get("month"))
        day = _as_int(value.get("day"))
        if year and month and day:
            try:
                return date(year, month, day).isoformat()
            except ValueError:
                return None
    return None


def parse_imdb_episodes_payload(payload: Mapping[str, Any], *, season: int | None = None) -> ImdbEpisodesPageMetadata:
    season_numbers = _extract_imdb_available_seasons_from_next_data(payload)
    items = _extract_imdb_episode_items_from_next_data(payload)

    episode_cards: list[ImdbEpisodeInfo] = []
    for item in items:
        imdb_episode_id = item.get("id")
        if not isinstance(imdb_episode_id, str) or not _IMDB_TITLE_ID_RE.match(imdb_episode_id.strip()):
            imdb_episode_id = None

        season_val = _as_int(item.get("season"))
        episode_val = _as_int(item.get("episode"))
        if season_val is None and season is not None:
            season_val = int(season)

        title_obj = item.get("titleText")
        if isinstance(title_obj, str):
            title = unescape(title_obj).strip() or None
        elif isinstance(title_obj, Mapping):
            title_raw = title_obj.get("text")
            title = unescape(title_raw).strip() if isinstance(title_raw, str) and title_raw.strip() else None
        else:
            title = None

        air_date = _parse_imdb_release_date(item.get("releaseDate"))

        if air_date is None and episode_val is None:
            continue

        episode_cards.append(
            ImdbEpisodeInfo(
                season=season_val,
                episode=episode_val,
                title=title,
                air_date=air_date,
                imdb_episode_id=imdb_episode_id,
            )
        )

    return ImdbEpisodesPageMetadata(available_seasons=season_numbers, episodes=episode_cards)


def parse_imdb_season_episodes_payload(
    payload: Mapping[str, Any], *, season: int | None = None
) -> list[ImdbSeasonEpisode]:
    items = _extract_imdb_episode_items_from_next_data(payload)
    episodes: list[ImdbSeasonEpisode] = []
    for item in items:
        imdb_episode_id = item.get("id")
        if not isinstance(imdb_episode_id, str) or not _IMDB_TITLE_ID_RE.match(imdb_episode_id.strip()):
            imdb_episode_id = None

        season_val = _as_int(item.get("season"))
        episode_val = _as_int(item.get("episode"))
        if season_val is None and season is not None:
            season_val = int(season)
        if season_val is None or episode_val is None:
            continue

        title_obj = item.get("titleText")
        if isinstance(title_obj, str):
            title = unescape(title_obj).strip() or None
        elif isinstance(title_obj, Mapping):
            title_raw = title_obj.get("text")
            title = unescape(title_raw).strip() if isinstance(title_raw, str) and title_raw.strip() else None
        else:
            title = None

        plot_obj = item.get("plot")
        if isinstance(plot_obj, str):
            overview = unescape(plot_obj).strip() or None
        elif isinstance(plot_obj, Mapping):
            inner = plot_obj.get("plotText")
            if isinstance(inner, Mapping):
                text = inner.get("plainText")
                overview = unescape(text).strip() if isinstance(text, str) and text.strip() else None
            else:
                overview = None
        else:
            overview = None

        air_date = _parse_imdb_release_date(item.get("releaseDate"))

        rating = item.get("aggregateRating")
        imdb_rating = float(rating) if isinstance(rating, (int, float)) else None

        vote_count = item.get("voteCount")
        imdb_vote_count = int(vote_count) if isinstance(vote_count, int) else None

        image_obj = item.get("image")
        image_url: str | None = None
        image_caption: str | None = None
        image_width: int | None = None
        image_height: int | None = None
        if isinstance(image_obj, Mapping):
            url = image_obj.get("url")
            if isinstance(url, str) and url.strip():
                image_url = url.strip()
            cap = image_obj.get("caption")
            if isinstance(cap, str) and cap.strip():
                image_caption = unescape(cap).strip()
            image_width = _as_int(image_obj.get("maxWidth"))
            image_height = _as_int(image_obj.get("maxHeight"))

        episodes.append(
            ImdbSeasonEpisode(
                season=int(season_val),
                episode=int(episode_val),
                imdb_episode_id=imdb_episode_id,
                title=title,
                air_date=air_date,
                overview=overview,
                imdb_rating=imdb_rating,
                imdb_vote_count=imdb_vote_count,
                imdb_primary_image_url=image_url,
                imdb_primary_image_caption=image_caption,
                imdb_primary_image_width=image_width,
                imdb_primary_image_height=image_height,
            )
        )
    return episodes


def parse_imdb_season_episodes_page(html: str, *, season: int | None = None) -> list[ImdbSeasonEpisode]:
    """
    Parse an IMDb season episodes page into rich per-episode objects.

    Prefers extracting episodes from Next.js `__NEXT_DATA__` (contains plot/rating/image),
    with a best-effort HTML fallback when the JSON payload is unavailable.
    """

    next_data = _extract_next_data_json(html)
    if isinstance(next_data, Mapping):
        episodes = parse_imdb_season_episodes_payload(next_data, season=season)
        if episodes:
            return episodes

    # HTML fallback (limited fields).
    fallback = parse_imdb_episodes_page(html, season=season)
    out: list[ImdbSeasonEpisode] = []
    for ep in fallback.episodes:
        if ep.season is None or ep.episode is None:
            continue
        out.append(
            ImdbSeasonEpisode(
                season=int(ep.season),
                episode=int(ep.episode),
                imdb_episode_id=ep.imdb_episode_id,
                title=ep.title,
                air_date=ep.air_date,
                overview=None,
                imdb_rating=None,
                imdb_vote_count=None,
                imdb_primary_image_url=None,
                imdb_primary_image_caption=None,
                imdb_primary_image_width=None,
                imdb_primary_image_height=None,
            )
        )
    return out


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
        self._next_build_id = os.getenv("IMDB_NEXT_BUILD_ID")

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
            is_blocked = resp.status_code in {202, 403, 429}
            request_url = getattr(resp, "url", None) or url
            if is_blocked and "/episodes/" in str(request_url):
                scrapling_html = _fetch_episodes_page_via_scrapling(
                    str(request_url),
                    extra_headers=headers,
                    timeout_seconds=self._timeout_seconds,
                    verbose=_env_flag("IMDB_EPISODES_SCRAPLING_VERBOSE", default=False),
                )
                if scrapling_html:
                    return scrapling_html

            raise ImdbTitleMetadataClientError(
                f"IMDb request failed with HTTP {resp.status_code}.",
                status_code=resp.status_code,
                body_snippet=(resp.text or "")[:200],
            )

        return resp.text or ""

    def _get_json(self, url: str, *, params: Mapping[str, Any] | None = None) -> dict[str, Any]:
        if self._sleep_ms:
            time.sleep(self._sleep_ms / 1000.0)

        headers = {
            "accept": "application/json",
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

        try:
            data = resp.json()
        except ValueError as exc:
            raise ImdbTitleMetadataClientError("IMDb response was not valid JSON.") from exc

        if not isinstance(data, dict):
            raise ImdbTitleMetadataClientError("IMDb JSON response was not an object.")

        return data

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

    def fetch_episodes_payload(
        self,
        imdb_series_id: str,
        *,
        season: int | None = None,
        allow_html_fallback: bool = True,
    ) -> dict[str, Any]:
        imdb_series_id = str(imdb_series_id).strip()
        if not _IMDB_TITLE_ID_RE.match(imdb_series_id):
            raise ValueError(f"Invalid IMDb id: {imdb_series_id!r}")

        params: dict[str, Any] = {}
        if season is not None:
            params["season"] = str(int(season))

        last_exc: Exception | None = None
        build_id = self._next_build_id or os.getenv("IMDB_NEXT_BUILD_ID")
        if not build_id:
            build_id = self._discover_next_build_id(imdb_series_id)
        if build_id:
            url = f"https://www.imdb.com/_next/data/{build_id}/en-US/title/{imdb_series_id}/episodes.json"
            try:
                return self._get_json(url, params=params if params else None)
            except Exception as exc:  # noqa: BLE001
                last_exc = exc

        if not allow_html_fallback:
            if isinstance(last_exc, Exception):
                raise last_exc
            raise ImdbTitleMetadataClientError("IMDb Next.js payload unavailable (no build id).")

        html = self.fetch_episodes_page(imdb_series_id, season=season)
        next_data = _extract_next_data_json(html)
        if isinstance(next_data, dict):
            build_id_val = next_data.get("buildId")
            if isinstance(build_id_val, str) and build_id_val.strip():
                self._next_build_id = build_id_val.strip()
            return next_data

        if isinstance(last_exc, Exception):
            raise last_exc
        raise ImdbTitleMetadataClientError("IMDb Next.js payload unavailable.")

    def _discover_next_build_id(self, imdb_series_id: str | None = None) -> str | None:
        """Fetch a safe IMDb page to extract the Next.js build id."""
        candidates = ["https://m.imdb.com/"]
        if imdb_series_id and _IMDB_TITLE_ID_RE.match(str(imdb_series_id).strip()):
            candidates.insert(0, f"https://m.imdb.com/title/{imdb_series_id}/")
        candidates.append("https://www.imdb.com/")
        if imdb_series_id and _IMDB_TITLE_ID_RE.match(str(imdb_series_id).strip()):
            candidates.append(f"https://www.imdb.com/title/{imdb_series_id}/")

        for url in candidates:
            try:
                html = self._get(url)
            except ImdbTitleMetadataClientError:
                continue
            next_data = _extract_next_data_json(html)
            if isinstance(next_data, dict):
                build_id_val = next_data.get("buildId")
                if isinstance(build_id_val, str) and build_id_val.strip():
                    self._next_build_id = build_id_val.strip()
                    return self._next_build_id
        return None
