from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import re
import sys
import time
from threading import Lock
from typing import Any, Mapping
from urllib.parse import urlparse
from uuid import UUID

import requests

from trr_backend.integrations.imdb.title_metadata_client import (
    HttpImdbTitleMetadataClient,
    parse_imdb_episodes_page,
    parse_imdb_title_page,
    pick_most_recent_episode,
)
from trr_backend.ingestion.imdb_images import extract_imdb_image_urls, fetch_imdb_mediaindex_html
from trr_backend.integrations.imdb.title_page_metadata import (
    fetch_imdb_title_html,
    parse_imdb_title_html,
)
from trr_backend.integrations.tmdb.client import (
    TmdbClientError,
    fetch_tv_details,
    fetch_tv_watch_providers,
    find_by_imdb_id,
    resolve_api_key,
)
from trr_backend.models.shows import ShowRecord


@dataclass(frozen=True)
class ShowEnrichmentPatch:
    show_id: UUID
    external_ids_update: dict[str, Any]
    show_update: dict[str, Any] = field(default_factory=dict)
    show_images_rows: list[dict[str, Any]] = field(default_factory=list)


@dataclass(frozen=True)
class EnrichFailure:
    show_id: UUID
    name: str
    message: str


@dataclass(frozen=True)
class EnrichSummary:
    attempted: int
    updated: int
    skipped: int
    skipped_complete: int
    failed: int
    patches: list[ShowEnrichmentPatch] = field(default_factory=list)
    failures: list[EnrichFailure] = field(default_factory=list)


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _build_imdb_meta_payload(parsed: Mapping[str, Any], *, imdb_id: str, fetched_at: str) -> dict[str, Any]:
    aggregate_value = parsed.get("aggregate_rating_value")
    aggregate_count = parsed.get("aggregate_rating_count")
    aggregate_rating: dict[str, Any] | None = None
    if aggregate_value is not None or aggregate_count is not None:
        aggregate_rating = {
            "value": aggregate_value,
            "count": aggregate_count,
        }

    return {
        "imdb_id": imdb_id,
        "imdb_url": parsed.get("imdb_url"),
        "title": parsed.get("title"),
        "description": parsed.get("description"),
        "tags": parsed.get("tags") or [],
        "genres": parsed.get("genres") or [],
        "content_rating": parsed.get("content_rating"),
        "keywords": parsed.get("keywords") or [],
        "aggregate_rating": aggregate_rating,
        "poster_image_url": parsed.get("poster_image_url"),
        "date_published": parsed.get("date_published"),
        "runtime_minutes": parsed.get("runtime_minutes"),
        "trailer": parsed.get("trailer"),
        "total_episodes": parsed.get("total_episodes"),
        "total_seasons": parsed.get("total_seasons"),
        "fetched_at": fetched_at,
        "source": "imdb_title_page",
    }


_IMDB_IMAGE_BASE_RE = re.compile(r"^(?P<base>.+?)\._V\d+", re.IGNORECASE)


def _imdb_source_image_id_from_url(url: str) -> str | None:
    parsed = urlparse(url)
    filename = (parsed.path or "").rsplit("/", 1)[-1]
    if not filename:
        return None
    stem = filename.rsplit(".", 1)[0]
    match = _IMDB_IMAGE_BASE_RE.match(stem)
    if match:
        return match.group("base")
    return stem or None


def _build_imdb_show_image_rows_from_urls(
    urls: list[str],
    *,
    show_id: UUID,
    fetched_at: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for position, url in enumerate(urls, start=1):
        source_image_id = _imdb_source_image_id_from_url(url)
        if not source_image_id:
            continue
        url_path = None
        parsed = urlparse(url)
        if parsed.path:
            url_path = parsed.path
        metadata = {"source": "imdb_section_images"}
        rows.append(
            {
                "show_id": str(show_id),
                "source": "imdb",
                "source_image_id": source_image_id,
                "kind": "media",
                "image_type": None,
                "caption": None,
                "position": position,
                "url": url,
                "url_path": url_path,
                "width": None,
                "height": None,
                "metadata": metadata,
                "fetched_at": fetched_at,
                "updated_at": fetched_at,
            }
        )
    return rows


def _as_int(value: Any) -> int | None:
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        raw = value.strip()
        if raw.isdigit():
            return int(raw)
    return None


def _as_str(value: Any) -> str | None:
    if isinstance(value, str):
        s = value.strip()
        return s or None
    return None


def _is_blank(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    return False


def _ensure_show_meta_shape(show_meta: dict[str, Any]) -> dict[str, Any]:
    show_meta.setdefault("show", None)
    show_meta.setdefault("network", None)
    show_meta.setdefault("streaming", None)
    show_meta.setdefault("show_total_seasons", None)
    show_meta.setdefault("show_total_episodes", None)
    show_meta.setdefault("imdb_series_id", None)
    show_meta.setdefault("tmdb_series_id", None)
    show_meta.setdefault("most_recent_episode", None)
    show_meta.setdefault(
        "most_recent_episode_obj",
        {
            "season": None,
            "episode": None,
            "title": None,
            "air_date": None,
            "imdb_episode_id": None,
        },
    )
    show_meta.setdefault("source", {})
    show_meta.setdefault("fetched_at", None)
    show_meta.setdefault("region", None)
    return show_meta


def is_show_meta_complete(show_meta: dict[str, Any]) -> bool:
    """
    Return True when `show_meta` has the required keys and minimum values.

    Notes:
    - `streaming` is allowed to be empty/None (providers may not exist for a region).
    - `tmdb_series_id` is allowed to be None when not resolvable.
    - `most_recent_episode` is allowed to be None when not resolvable.
    """

    if not isinstance(show_meta, dict):
        return False

    # Keys that must exist and be non-blank.
    required_nonblank = ("show", "network", "imdb_series_id")
    for key in required_nonblank:
        if key not in show_meta:
            return False
        if _is_blank(show_meta.get(key)):
            return False

    # Totals must exist and be parseable ints.
    for key in ("show_total_seasons", "show_total_episodes"):
        if key not in show_meta:
            return False
        if _as_int(show_meta.get(key)) is None:
            return False

    # Keys that must exist but may be null/blank.
    for key in ("streaming", "tmdb_series_id", "most_recent_episode"):
        if key not in show_meta:
            return False

    return True


def _build_most_recent_episode_string(ep: Mapping[str, Any]) -> str | None:
    season = _as_int(ep.get("season"))
    episode = _as_int(ep.get("episode"))
    title = _as_str(ep.get("title"))
    air_date = _as_str(ep.get("air_date"))
    imdb_episode_id = _as_str(ep.get("imdb_episode_id"))

    parts: list[str] = []
    if season is not None and episode is not None:
        parts.append(f"S{season}.E{episode}")
    if title:
        if parts:
            parts.append(f"- {title}")
        else:
            parts.append(title)
    if air_date:
        parts.append(f"({air_date})")

    s = " ".join(parts).strip()
    if not s:
        return None
    if imdb_episode_id:
        return f"{s} [imdbEpisodeId={imdb_episode_id}]"
    return s


def _pick_watch_providers(payload: Mapping[str, Any], *, region: str) -> str | None:
    results = payload.get("results")
    if not isinstance(results, dict):
        return None
    region_block = results.get(region)
    if not isinstance(region_block, dict):
        return None

    flatrate = region_block.get("flatrate")
    if not isinstance(flatrate, list):
        return None

    providers: list[str] = []
    for item in flatrate:
        if not isinstance(item, dict):
            continue
        name = item.get("provider_name")
        if isinstance(name, str) and name.strip():
            providers.append(name.strip())

    # Deduplicate while preserving order.
    unique = list(dict.fromkeys(providers))
    return ", ".join(unique) if unique else None


def _extract_tmdb_id_from_find(payload: Mapping[str, Any]) -> int | None:
    tv_results = payload.get("tv_results")
    if not isinstance(tv_results, list) or not tv_results:
        return None
    for item in tv_results:
        if not isinstance(item, dict):
            continue
        tmdb_id = item.get("id")
        if isinstance(tmdb_id, int):
            return tmdb_id
        if isinstance(tmdb_id, str) and tmdb_id.isdigit():
            return int(tmdb_id)
    return None


def _enrich_one_show(
    show: ShowRecord,
    *,
    region: str,
    force_refresh: bool,
    tmdb_api_key: str | None,
    imdb_sleep_ms: int,
    tmdb_find_cache: dict[str, int | None],
    tmdb_details_cache: dict[int, dict[str, Any]],
    tmdb_watch_cache: dict[int, dict[str, Any]],
    imdb_title_cache: dict[str, str],
    imdb_episodes_cache: dict[tuple[str, int | None], str],
    imdb_images_cache: dict[str, list[str]],
    cache_lock: Lock,
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]]] | None:
    external_ids = dict(show.external_ids or {})
    existing_show_meta = external_ids.get("show_meta") if isinstance(external_ids.get("show_meta"), dict) else None

    show_meta: dict[str, Any] = dict(existing_show_meta or {})
    imdb_id = show.imdb_id or _as_str(show_meta.get("imdb_series_id"))
    needs_imdb_meta = bool(imdb_id)

    show_meta_complete = (
        isinstance(existing_show_meta, Mapping)
        and is_show_meta_complete(dict(existing_show_meta))
        and (str(existing_show_meta.get("region") or "").strip().upper() == region.upper())
    )

    # Skip when already complete for this region (unless forced or IMDb meta needed).
    if not force_refresh and show_meta_complete and not needs_imdb_meta:
        return None

    # Fail-safe: if we already fetched for this region and nothing missing is fillable, skip to avoid hammering sources.
    if not force_refresh and isinstance(existing_show_meta, Mapping):
        fetched_at = existing_show_meta.get("fetched_at")
        if isinstance(fetched_at, str) and fetched_at.strip():
            if (str(existing_show_meta.get("region") or "").strip().upper() == region.upper()):
                missing_fields = [
                    key
                    for key in (
                        "imdb_series_id",
                        "tmdb_series_id",
                        "network",
                        "streaming",
                        "show_total_seasons",
                        "show_total_episodes",
                        "most_recent_episode",
                    )
                    if key not in existing_show_meta or _is_blank(existing_show_meta.get(key))
                ]

                tmdb_series_id_existing = show.tmdb_series_id or show.tmdb_id or _as_int(
                    existing_show_meta.get("tmdb_series_id")
                )
                tmdb_possible = bool(tmdb_api_key and (tmdb_series_id_existing or imdb_id))
                imdb_possible = bool(imdb_id)

                def fillable(field: str) -> bool:
                    if field == "tmdb_series_id":
                        return bool(tmdb_api_key and imdb_id)
                    if field == "imdb_series_id":
                        # We currently only get IMDb series id from list ingestion (or prior show_meta).
                        return False
                    if field == "streaming":
                        return tmdb_possible
                    if field in {"network", "show_total_seasons", "show_total_episodes", "most_recent_episode"}:
                        return tmdb_possible or imdb_possible
                    return False

                if missing_fields and not any(fillable(f) for f in missing_fields) and not needs_imdb_meta:
                    return None

    show_meta["show"] = show.name
    if show.imdb_id and (force_refresh or _is_blank(show_meta.get("imdb_series_id"))):
        show_meta["imdb_series_id"] = show.imdb_id

    tmdb_series_id: int | None = show.tmdb_series_id or show.tmdb_id or _as_int(show_meta.get("tmdb_series_id"))
    tmdb_sources: list[str] = []

    # Resolve TMDb id from IMDb id when missing.
    imdb_id_for_find = show.imdb_id or _as_str(show_meta.get("imdb_series_id"))
    if tmdb_series_id is None and tmdb_api_key and imdb_id_for_find and (force_refresh or not show_meta_complete):
        with cache_lock:
            cached = tmdb_find_cache.get(imdb_id_for_find)
        if cached is None and imdb_id_for_find not in tmdb_find_cache:
            session = requests.Session()
            payload = find_by_imdb_id(imdb_id_for_find, api_key=tmdb_api_key, session=session)
            cached = _extract_tmdb_id_from_find(payload)
            with cache_lock:
                tmdb_find_cache[imdb_id_for_find] = cached
        tmdb_series_id = cached
        if tmdb_series_id is not None:
            tmdb_sources.append("find")

    if tmdb_series_id is not None and (force_refresh or show_meta.get("tmdb_series_id") is None):
        show_meta["tmdb_series_id"] = tmdb_series_id

    updates: dict[str, Any] = {"show_meta": show_meta}
    if tmdb_series_id is not None and external_ids.get("tmdb") in (None, "", 0):
        updates["tmdb"] = int(tmdb_series_id)

    # Primary: TMDb TV details + watch providers.
    if tmdb_series_id is not None and tmdb_api_key and (force_refresh or not show_meta_complete):
        session = requests.Session()

        details: dict[str, Any] | None = None
        tmdb_meta_existing = external_ids.get("tmdb_meta")
        if not force_refresh and isinstance(tmdb_meta_existing, Mapping):
            tmdb_meta_id = _as_int(tmdb_meta_existing.get("id"))
            if tmdb_meta_id == tmdb_series_id:
                details = dict(tmdb_meta_existing)

        if details is None:
            with cache_lock:
                details = tmdb_details_cache.get(tmdb_series_id)
            if details is None:
                details = fetch_tv_details(tmdb_series_id, api_key=tmdb_api_key, session=session)
                with cache_lock:
                    tmdb_details_cache[tmdb_series_id] = details
        tmdb_sources.append("details")

        networks = details.get("networks")
        if isinstance(networks, list):
            names: list[str] = []
            for n in networks:
                if isinstance(n, dict):
                    name = n.get("name")
                    if isinstance(name, str) and name.strip():
                        names.append(name.strip())
            if names and (force_refresh or _is_blank(show_meta.get("network"))):
                show_meta["network"] = ", ".join(list(dict.fromkeys(names)))

        if force_refresh or show_meta.get("show_total_seasons") is None:
            value = _as_int(details.get("number_of_seasons"))
            if value is not None:
                show_meta["show_total_seasons"] = value
        if force_refresh or show_meta.get("show_total_episodes") is None:
            value = _as_int(details.get("number_of_episodes"))
            if value is not None:
                show_meta["show_total_episodes"] = value

        last = details.get("last_episode_to_air")
        if isinstance(last, dict) and (force_refresh or _is_blank(show_meta.get("most_recent_episode"))):
            ep_obj = {
                "season": _as_int(last.get("season_number")),
                "episode": _as_int(last.get("episode_number")),
                "title": _as_str(last.get("name")),
                "air_date": _as_str(last.get("air_date")),
                "imdb_episode_id": None,
            }
            show_meta["most_recent_episode_obj"] = ep_obj
            show_meta["most_recent_episode"] = _build_most_recent_episode_string(ep_obj)

        with cache_lock:
            providers_payload = tmdb_watch_cache.get(tmdb_series_id)
        if providers_payload is None:
            providers_payload = fetch_tv_watch_providers(tmdb_series_id, api_key=tmdb_api_key, session=session)
            with cache_lock:
                tmdb_watch_cache[tmdb_series_id] = providers_payload
        tmdb_sources.append("watch_providers")

        if force_refresh or _is_blank(show_meta.get("streaming")):
            value = _pick_watch_providers(providers_payload, region=region.upper())
            if value is not None:
                show_meta["streaming"] = value

    # Fallback: IMDb title + episodes pages, only for missing values.
    imdb_sources: list[str] = []
    need_imdb_title = (force_refresh and tmdb_series_id is None) or _is_blank(show_meta.get("network")) or show_meta.get(
        "show_total_episodes"
    ) is None
    need_imdb_episodes = (force_refresh and tmdb_series_id is None) or show_meta.get(
        "show_total_seasons"
    ) is None or _is_blank(show_meta.get("most_recent_episode"))

    imdb_id = show.imdb_id or _as_str(show_meta.get("imdb_series_id"))
    if imdb_id and (need_imdb_title or need_imdb_episodes) and (force_refresh or not show_meta_complete):
        client = HttpImdbTitleMetadataClient(sleep_ms=imdb_sleep_ms)

        if need_imdb_title:
            with cache_lock:
                title_html = imdb_title_cache.get(imdb_id)
            if title_html is None:
                title_html = client.fetch_title_page(imdb_id)
                with cache_lock:
                    imdb_title_cache[imdb_id] = title_html
            meta = parse_imdb_title_page(title_html)
            imdb_sources.append("title")

            if (force_refresh or _is_blank(show_meta.get("network"))) and meta.network:
                show_meta["network"] = meta.network
            if (force_refresh or show_meta.get("show_total_episodes") is None) and meta.total_episodes is not None:
                show_meta["show_total_episodes"] = int(meta.total_episodes)

        if need_imdb_episodes:
            key = (imdb_id, None)
            with cache_lock:
                episodes_html = imdb_episodes_cache.get(key)
            if episodes_html is None:
                episodes_html = client.fetch_episodes_page(imdb_id)
                with cache_lock:
                    imdb_episodes_cache[key] = episodes_html
            episodes_meta = parse_imdb_episodes_page(episodes_html)
            imdb_sources.append("episodes")

            if (force_refresh or show_meta.get("show_total_seasons") is None) and episodes_meta.available_seasons:
                show_meta["show_total_seasons"] = int(max(episodes_meta.available_seasons))

            if (force_refresh or _is_blank(show_meta.get("most_recent_episode"))) and episodes_meta.available_seasons:
                max_season = max(episodes_meta.available_seasons)
                key = (imdb_id, int(max_season))
                with cache_lock:
                    season_html = imdb_episodes_cache.get(key)
                if season_html is None:
                    season_html = client.fetch_episodes_page(imdb_id, season=max_season)
                    with cache_lock:
                        imdb_episodes_cache[key] = season_html
                season_meta = parse_imdb_episodes_page(season_html, season=max_season)
                picked = pick_most_recent_episode(season_meta.episodes)
                if picked:
                    ep_obj = {
                        "season": picked.season,
                        "episode": picked.episode,
                        "title": picked.title,
                        "air_date": picked.air_date,
                        "imdb_episode_id": picked.imdb_episode_id,
                    }
                    show_meta["most_recent_episode_obj"] = ep_obj
                    show_meta["most_recent_episode"] = _build_most_recent_episode_string(ep_obj)

    fetched_at = _now_utc_iso()

    imdb_meta_update: dict[str, Any] | None = None
    show_images_rows: list[dict[str, Any]] = []
    if imdb_id:
        title_html = None
        with cache_lock:
            title_html = imdb_title_cache.get(imdb_id)
        if title_html is None:
            try:
                if imdb_sleep_ms:
                    time.sleep(imdb_sleep_ms / 1000.0)
                title_html = fetch_imdb_title_html(imdb_id)
                with cache_lock:
                    imdb_title_cache[imdb_id] = title_html
            except Exception as exc:  # noqa: BLE001
                print(f"IMDb title meta: failed imdb_id={imdb_id} error={exc}", file=sys.stderr)
                title_html = None
        if title_html:
            parsed = parse_imdb_title_html(title_html, imdb_id=imdb_id)
            imdb_meta_update = _build_imdb_meta_payload(parsed, imdb_id=imdb_id, fetched_at=fetched_at)

    if imdb_id:
        with cache_lock:
            cached_urls = imdb_images_cache.get(imdb_id)
        fetched_html = False
        if cached_urls is None and imdb_id not in imdb_images_cache:
            html: str | None = None
            try:
                if imdb_sleep_ms:
                    time.sleep(imdb_sleep_ms / 1000.0)
                html = fetch_imdb_mediaindex_html(imdb_id)
            except Exception as exc:  # noqa: BLE001
                print(f"IMDb images: imdb_id={imdb_id} error={exc}", file=sys.stderr)
                html = None
            fetched_html = html is not None
            urls = extract_imdb_image_urls(html) if fetched_html else []
            with cache_lock:
                imdb_images_cache[imdb_id] = urls
            cached_urls = urls
        urls = cached_urls or []
        if urls:
            show_images_rows = _build_imdb_show_image_rows_from_urls(
                urls,
                show_id=show.id,
                fetched_at=fetched_at,
            )
        elif cached_urls is not None and fetched_html:
            print(f"IMDb images: imdb_id={imdb_id} parsed 0 images", file=sys.stderr)

    show_meta["region"] = region.upper()
    show_meta["fetched_at"] = fetched_at

    source: dict[str, Any] = show_meta.get("source") if isinstance(show_meta.get("source"), dict) else {}
    if tmdb_sources:
        source["tmdb"] = "|".join(sorted(set(tmdb_sources), key=tmdb_sources.index))
    if imdb_sources:
        source["imdb"] = "|".join(sorted(set(imdb_sources), key=imdb_sources.index))
    show_meta["source"] = source

    _ensure_show_meta_shape(show_meta)

    show_update = {
        "network": _as_str(show_meta.get("network")),
        "streaming": _as_str(show_meta.get("streaming")),
        "show_total_seasons": _as_int(show_meta.get("show_total_seasons")),
        "show_total_episodes": _as_int(show_meta.get("show_total_episodes")),
        "imdb_series_id": _as_str(show_meta.get("imdb_series_id")) or show.imdb_id,
        "tmdb_series_id": _as_int(show_meta.get("tmdb_series_id")) or show.tmdb_id,
        "most_recent_episode": _as_str(show_meta.get("most_recent_episode")),
    }
    if imdb_meta_update is not None:
        show_update["imdb_meta"] = imdb_meta_update

    return updates, show_update, show_images_rows


def enrich_shows_after_upsert(
    show_rows: list[ShowRecord],
    *,
    region: str = "US",
    concurrency: int = 5,
    max_enrich: int | None = None,
    force_refresh: bool = False,
    dry_run: bool = False,
    imdb_sleep_ms: int = 0,
) -> EnrichSummary:
    """
    Build `external_ids` patches to populate/refresh `external_ids["show_meta"]`.

    Network calls are performed here, but DB writes are performed by the caller.
    """

    region = (region or "US").strip().upper()
    concurrency = max(1, int(concurrency or 1))
    imdb_sleep_ms = max(0, int(imdb_sleep_ms or 0))

    tmdb_api_key = resolve_api_key()
    if not tmdb_api_key:
        # Keep behavior explicit, but do not fail enrichment entirely.
        tmdb_api_key = None

    rows = list(show_rows)
    if max_enrich is not None:
        rows = rows[: max(0, int(max_enrich))]

    skipped_complete = 0
    if not force_refresh:
        remaining: list[ShowRecord] = []
        for show in rows:
            show_meta = show.external_ids.get("show_meta")
            if isinstance(show_meta, dict) and is_show_meta_complete(show_meta):
                if (str(show_meta.get("region") or "").strip().upper() == region.upper()):
                    imdb_id = show.imdb_id or _as_str(show_meta.get("imdb_series_id"))
                    if not imdb_id:
                        skipped_complete += 1
                        continue
            remaining.append(show)
        rows = remaining

    tmdb_find_cache: dict[str, int | None] = {}
    tmdb_details_cache: dict[int, dict[str, Any]] = {}
    tmdb_watch_cache: dict[int, dict[str, Any]] = {}
    imdb_title_cache: dict[str, str] = {}
    imdb_episodes_cache: dict[tuple[str, int | None], str] = {}
    imdb_images_cache: dict[str, list[str]] = {}
    cache_lock = Lock()

    attempted = 0
    updated = 0
    skipped = 0
    failed = 0
    patches: list[ShowEnrichmentPatch] = []
    failures: list[EnrichFailure] = []

    # Keep concurrency polite and simple; a worker fetches and builds one show patch.
    from concurrent.futures import ThreadPoolExecutor, as_completed

    def run_one(
        show: ShowRecord,
    ) -> tuple[UUID, tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]]] | None, str | None]:
        try:
            patch = _enrich_one_show(
                show,
                region=region,
                force_refresh=force_refresh,
                tmdb_api_key=tmdb_api_key,
                imdb_sleep_ms=imdb_sleep_ms,
                tmdb_find_cache=tmdb_find_cache,
                tmdb_details_cache=tmdb_details_cache,
                tmdb_watch_cache=tmdb_watch_cache,
                imdb_title_cache=imdb_title_cache,
                imdb_episodes_cache=imdb_episodes_cache,
                imdb_images_cache=imdb_images_cache,
                cache_lock=cache_lock,
            )
            return show.id, patch, None
        except (TmdbClientError, requests.RequestException, RuntimeError, ValueError) as exc:
            return show.id, None, str(exc)

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = {pool.submit(run_one, show): show for show in rows}
        for fut in as_completed(futures):
            show = futures[fut]
            attempted += 1
            show_id, patch, error = fut.result()
            if error:
                failed += 1
                failures.append(EnrichFailure(show_id=show_id, name=show.name, message=error))
                continue
            if patch is None:
                skipped += 1
                continue

            updated += 1
            external_patch, show_update, show_images_rows = patch
            patches.append(
                ShowEnrichmentPatch(
                    show_id=show_id,
                    external_ids_update=external_patch,
                    show_update=show_update,
                    show_images_rows=show_images_rows,
                )
            )

    return EnrichSummary(
        attempted=attempted,
        updated=updated,
        skipped=skipped,
        skipped_complete=skipped_complete,
        failed=failed,
        patches=patches,
        failures=failures,
    )
