from __future__ import annotations

import re
import sys
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from threading import Lock
from typing import Any
from urllib.parse import urlparse
from uuid import UUID

import requests

from trr_backend.ingestion.imdb_images import (
    extract_imdb_image_urls,
    extract_imdb_image_width,
    fetch_imdb_mediaindex_html,
)
from trr_backend.integrations.imdb.title_metadata_client import (
    HttpImdbTitleMetadataClient,
    parse_imdb_episodes_page,
    parse_imdb_title_page,
    pick_most_recent_episode,
)
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
    show_update: dict[str, Any] = field(default_factory=dict)
    # tmdb_external_ids removed - now written directly to show_update
    # Flat array columns (merged from all sources)
    genres: list[str] | None = None
    keywords: list[str] | None = None
    tags: list[str] | None = None
    networks: list[str] | None = None
    streaming_providers: list[str] | None = None
    tmdb_network_ids: list[int] | None = None
    tmdb_production_company_ids: list[int] | None = None
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
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


_IMDB_IMAGE_BASE_RE = re.compile(r"^(?P<base>.+?)\._V\d+_", re.IGNORECASE)
_IMDB_IMAGE_BASE_FALLBACK_RE = re.compile(r"^(?P<base>.+?)\._V\d+", re.IGNORECASE)


def _imdb_source_image_id_from_url(url: str) -> str | None:
    parsed = urlparse(url)
    filename = (parsed.path or "").rsplit("/", 1)[-1]
    if not filename:
        return None
    stem = filename.rsplit(".", 1)[0]
    match = _IMDB_IMAGE_BASE_RE.match(stem) or _IMDB_IMAGE_BASE_FALLBACK_RE.match(stem)
    if match:
        return match.group("base")
    return stem or None


def _build_imdb_show_image_rows_from_urls(
    urls: list[str],
    *,
    show_id: UUID,
    fetched_at: str,
    fetched_from_url: str | None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for position, url in enumerate(urls, start=1):
        source_image_id = _imdb_source_image_id_from_url(url)
        if not source_image_id:
            continue
        parsed = urlparse(url)
        width = extract_imdb_image_width(url)
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
                "url_path": parsed.path if parsed.path else None,
                "width": width,
                "height": None,
                "fetch_method": "imdb_section_images",
                "fetched_from_url": fetched_from_url,
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


def _as_float(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        try:
            return float(raw)
        except ValueError:
            return None
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
        parts.append(f"- {title}" if parts else title)
    if air_date:
        parts.append(f"({air_date})")

    s = " ".join(parts).strip()
    if not s:
        return None
    if imdb_episode_id:
        return f"{s} [imdbEpisodeId={imdb_episode_id}]"
    return s


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


def _extract_tmdb_network_rows(details: Mapping[str, Any]) -> list[dict[str, Any]]:
    networks = details.get("networks")
    if not isinstance(networks, list):
        return []
    rows: list[dict[str, Any]] = []
    for idx, item in enumerate(networks):
        if not isinstance(item, dict):
            continue
        name = _as_str(item.get("name"))
        if not name:
            continue
        rows.append(
            {
                "network": name,
                "tmdb_network_id": _as_int(item.get("id")),
                "logo_path": _as_str(item.get("logo_path")),
                "origin_country": _as_str(item.get("origin_country")),
                "is_primary": idx == 0,
            }
        )
    return rows


def _extract_tmdb_production_company_ids(details: Mapping[str, Any]) -> list[int]:
    companies = details.get("production_companies")
    if not isinstance(companies, list):
        return []
    out: list[int] = []
    for item in companies:
        if not isinstance(item, dict):
            continue
        company_id = _as_int(item.get("id"))
        if company_id is not None:
            out.append(company_id)
    return out


def _extract_tmdb_genres(details: Mapping[str, Any]) -> list[str]:
    genres = details.get("genres")
    if not isinstance(genres, list):
        return []
    out: list[str] = []
    for item in genres:
        if not isinstance(item, dict):
            continue
        name = _as_str(item.get("name"))
        if name:
            out.append(name)
    return list(dict.fromkeys(out))


def _extract_tmdb_watch_providers(payload: Mapping[str, Any], *, region: str) -> list[dict[str, Any]]:
    results = payload.get("results")
    if not isinstance(results, dict):
        return []
    region_block = results.get(region)
    if not isinstance(region_block, dict):
        return []

    flatrate = region_block.get("flatrate")
    if not isinstance(flatrate, list):
        return []

    providers: list[dict[str, Any]] = []
    for item in flatrate:
        if not isinstance(item, dict):
            continue
        name = _as_str(item.get("provider_name"))
        if not name:
            continue
        providers.append(
            {
                "provider": name,
                "provider_type": "flatrate",
                "tmdb_provider_id": _as_int(item.get("provider_id")),
                "logo_path": _as_str(item.get("logo_path")),
                "display_priority": _as_int(item.get("display_priority")),
            }
        )
    return providers


def _build_imdb_show_patch(parsed: Mapping[str, Any], *, fetched_at: str) -> dict[str, Any]:
    patch: dict[str, Any] = {
        "imdb_meta": dict(parsed),
        "imdb_fetched_at": fetched_at,
    }

    title = _as_str(parsed.get("title"))
    if title:
        patch["imdb_title"] = title

    content_rating = _as_str(parsed.get("content_rating"))
    if content_rating:
        patch["imdb_content_rating"] = content_rating

    rating_value = _as_float(parsed.get("aggregate_rating_value"))
    if rating_value is not None:
        patch["imdb_rating_value"] = rating_value

    rating_count = _as_int(parsed.get("aggregate_rating_count"))
    if rating_count is not None:
        patch["imdb_rating_count"] = rating_count

    date_published = _as_str(parsed.get("date_published"))
    if date_published:
        patch["imdb_date_published"] = date_published

    return patch


def _tmdb_meta_from_tv_details(details: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(details, Mapping):
        return {}
    return dict(details)


def _build_tmdb_show_patch(details: Mapping[str, Any], *, fetched_at: str) -> dict[str, Any]:
    meta = _tmdb_meta_from_tv_details(details)
    patch: dict[str, Any] = {
        "tmdb_meta": meta,
        "tmdb_fetched_at": fetched_at,
    }

    tmdb_name = _as_str(details.get("name"))
    if tmdb_name:
        patch["tmdb_name"] = tmdb_name

    tmdb_status = _as_str(details.get("status"))
    if tmdb_status:
        patch["tmdb_status"] = tmdb_status

    tmdb_type = _as_str(details.get("type"))
    if tmdb_type:
        patch["tmdb_type"] = tmdb_type

    first_air = _as_str(details.get("first_air_date"))
    if first_air:
        patch["tmdb_first_air_date"] = first_air

    last_air = _as_str(details.get("last_air_date"))
    if last_air:
        patch["tmdb_last_air_date"] = last_air

    vote_average = _as_float(details.get("vote_average"))
    if vote_average is not None:
        patch["tmdb_vote_average"] = vote_average

    vote_count = _as_int(details.get("vote_count"))
    if vote_count is not None:
        patch["tmdb_vote_count"] = vote_count

    popularity = _as_float(details.get("popularity"))
    if popularity is not None:
        patch["tmdb_popularity"] = popularity

    return patch


def _build_tmdb_external_ids(details: Mapping[str, Any], *, tmdb_id: int) -> dict[str, Any] | None:
    ext = details.get("external_ids")
    if not isinstance(ext, Mapping):
        return None
    payload = {
        "tmdb_id": tmdb_id,
        "imdb_id": _as_str(ext.get("imdb_id")),
        "tvdb_id": _as_int(ext.get("tvdb_id")),
        "tvrage_id": _as_int(ext.get("tvrage_id")),
        "wikidata_id": _as_str(ext.get("wikidata_id")),
        "facebook_id": _as_str(ext.get("facebook_id")),
        "instagram_id": _as_str(ext.get("instagram_id")),
        "twitter_id": _as_str(ext.get("twitter_id")),
    }
    return payload


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
) -> ShowEnrichmentPatch | None:
    show_update: dict[str, Any] = {}
    # Flat lists for merged attributes (from all sources)
    genres: list[str] = []
    keywords: list[str] = []
    tags: list[str] = []
    networks: list[str] = []
    streaming_providers: list[str] = []
    tmdb_network_ids: list[int] = []
    tmdb_production_company_ids: list[int] = []
    show_images_rows: list[dict[str, Any]] = []

    imdb_id = _as_str(show.imdb_id)
    tmdb_id = _as_int(show.tmdb_id)

    tmdb_sources: list[str] = []
    imdb_sources: list[str] = []

    # Resolve TMDb id from IMDb id if missing.
    if tmdb_id is None and tmdb_api_key and imdb_id:
        with cache_lock:
            cached = tmdb_find_cache.get(imdb_id)
        if cached is None and imdb_id not in tmdb_find_cache:
            session = requests.Session()
            payload = find_by_imdb_id(imdb_id, api_key=tmdb_api_key, session=session)
            cached = _extract_tmdb_id_from_find(payload)
            with cache_lock:
                tmdb_find_cache[imdb_id] = cached
        tmdb_id = cached
        if tmdb_id is not None and show.tmdb_id is None:
            show_update["tmdb_id"] = int(tmdb_id)
            tmdb_sources.append("find")

    fetched_at = _now_utc_iso()

    # TMDb details + providers.
    if tmdb_id is not None and tmdb_api_key:
        session = requests.Session()
        details: dict[str, Any] | None = None
        with cache_lock:
            details = tmdb_details_cache.get(tmdb_id)
        if details is None:
            details = fetch_tv_details(tmdb_id, api_key=tmdb_api_key, session=session)
            with cache_lock:
                tmdb_details_cache[tmdb_id] = details
        tmdb_sources.append("details")

        show_update.update(_build_tmdb_show_patch(details, fetched_at=fetched_at))

        # Extract external IDs and add to show_update
        ext_ids = _build_tmdb_external_ids(details, tmdb_id=tmdb_id)
        if ext_ids:
            for ext_key in ("tvdb_id", "tvrage_id", "wikidata_id", "facebook_id", "instagram_id", "twitter_id"):
                ext_val = ext_ids.get(ext_key)
                if ext_val is not None:
                    show_update[ext_key] = ext_val

        # Add TMDb genres to flat list
        tmdb_genres = _extract_tmdb_genres(details)
        if tmdb_genres:
            genres.extend(tmdb_genres)

        # Extract network names into flat list
        network_rows = _extract_tmdb_network_rows(details)
        if network_rows:
            networks.extend([r["network"] for r in network_rows if r.get("network")])
            tmdb_network_ids.extend(
                [r["tmdb_network_id"] for r in network_rows if r.get("tmdb_network_id") is not None]
            )

        production_company_ids = _extract_tmdb_production_company_ids(details)
        if production_company_ids:
            tmdb_production_company_ids.extend(production_company_ids)

        if force_refresh or show.show_total_seasons is None:
            value = _as_int(details.get("number_of_seasons"))
            if value is not None:
                show_update["show_total_seasons"] = value
        if force_refresh or show.show_total_episodes is None:
            value = _as_int(details.get("number_of_episodes"))
            if value is not None:
                show_update["show_total_episodes"] = value

        if force_refresh or _is_blank(show.description):
            overview = _as_str(details.get("overview"))
            if overview:
                show_update["description"] = overview
        if force_refresh or _is_blank(show.premiere_date):
            first_air_date = _as_str(details.get("first_air_date"))
            if first_air_date:
                show_update["premiere_date"] = first_air_date

        last = details.get("last_episode_to_air")
        if isinstance(last, dict):
            ep_obj = {
                "season": _as_int(last.get("season_number")),
                "episode": _as_int(last.get("episode_number")),
                "title": _as_str(last.get("name")),
                "air_date": _as_str(last.get("air_date")),
                "imdb_episode_id": None,
            }
            show_update["most_recent_episode"] = _build_most_recent_episode_string(ep_obj)
            show_update["most_recent_episode_season"] = ep_obj["season"]
            show_update["most_recent_episode_number"] = ep_obj["episode"]
            show_update["most_recent_episode_title"] = ep_obj["title"]
            show_update["most_recent_episode_air_date"] = ep_obj["air_date"]
            show_update["most_recent_episode_imdb_id"] = None

        with cache_lock:
            providers_payload = tmdb_watch_cache.get(tmdb_id)
        if providers_payload is None:
            providers_payload = fetch_tv_watch_providers(tmdb_id, api_key=tmdb_api_key, session=session)
            with cache_lock:
                tmdb_watch_cache[tmdb_id] = providers_payload
        tmdb_sources.append("watch_providers")

        providers = _extract_tmdb_watch_providers(providers_payload, region=region.upper())
        # Extract provider names into flat list
        streaming_providers.extend([p["provider"] for p in providers if p.get("provider")])

    # IMDb title + episodes for missing values.
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
            show_update.update(_build_imdb_show_patch(parsed, fetched_at=fetched_at))
            imdb_sources.append("title")

            if parsed.get("genres"):
                genres.extend(parsed.get("genres") or [])
            if parsed.get("keywords"):
                keywords.extend(parsed.get("keywords") or [])
            if parsed.get("tags"):
                tags.extend(parsed.get("tags") or [])

            if force_refresh or _is_blank(show.description):
                description = _as_str(parsed.get("description"))
                if description:
                    show_update["description"] = description

            # Use IMDb title page for network + totals when TMDb missing.
            try:
                title_meta = parse_imdb_title_page(title_html)
            except Exception:
                title_meta = None
            if title_meta is not None:
                if not networks and title_meta.network:
                    networks.append(title_meta.network)
                if (force_refresh or show.show_total_episodes is None) and title_meta.total_episodes is not None:
                    show_update["show_total_episodes"] = int(title_meta.total_episodes)
                if (force_refresh or show.show_total_seasons is None) and title_meta.total_seasons is not None:
                    show_update["show_total_seasons"] = int(title_meta.total_seasons)

        need_imdb_episodes = force_refresh or show_update.get("most_recent_episode") is None
        if need_imdb_episodes:
            client = HttpImdbTitleMetadataClient(sleep_ms=imdb_sleep_ms)
            key = (imdb_id, None)
            with cache_lock:
                episodes_html = imdb_episodes_cache.get(key)
            if episodes_html is None:
                episodes_html = client.fetch_episodes_page(imdb_id)
                with cache_lock:
                    imdb_episodes_cache[key] = episodes_html
            episodes_meta = parse_imdb_episodes_page(episodes_html)
            imdb_sources.append("episodes")

            if (force_refresh or show.show_total_seasons is None) and episodes_meta.available_seasons:
                show_update["show_total_seasons"] = int(max(episodes_meta.available_seasons))

            if episodes_meta.available_seasons:
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
                    show_update["most_recent_episode"] = _build_most_recent_episode_string(ep_obj)
                    show_update["most_recent_episode_season"] = ep_obj["season"]
                    show_update["most_recent_episode_number"] = ep_obj["episode"]
                    show_update["most_recent_episode_title"] = ep_obj["title"]
                    show_update["most_recent_episode_air_date"] = ep_obj["air_date"]
                    show_update["most_recent_episode_imdb_id"] = ep_obj["imdb_episode_id"]

        # IMDb images: prefer section-images from title page, fallback to mediaindex
        with cache_lock:
            cached_urls = imdb_images_cache.get(imdb_id)
        fetched_from_url: str | None = None
        if cached_urls is None and imdb_id not in imdb_images_cache:
            urls: list[str] = []
            # Primary: use title page section-images (already fetched above)
            if title_html:
                urls = extract_imdb_image_urls(title_html)
                if urls:
                    fetched_from_url = f"https://www.imdb.com/title/{imdb_id}/"
            # Fallback: mediaindex if section-images yielded nothing
            if not urls:
                try:
                    if imdb_sleep_ms:
                        time.sleep(imdb_sleep_ms / 1000.0)
                    mediaindex_html = fetch_imdb_mediaindex_html(imdb_id)
                    if mediaindex_html:
                        urls = extract_imdb_image_urls(mediaindex_html)
                        if urls:
                            fetched_from_url = f"https://www.imdb.com/title/{imdb_id}/mediaindex/"
                except Exception as exc:  # noqa: BLE001
                    print(f"IMDb images: imdb_id={imdb_id} mediaindex fallback error={exc}", file=sys.stderr)
            with cache_lock:
                imdb_images_cache[imdb_id] = urls
            cached_urls = urls
        urls = cached_urls or []
        if urls:
            show_images_rows = _build_imdb_show_image_rows_from_urls(
                urls,
                show_id=show.id,
                fetched_at=fetched_at,
                fetched_from_url=fetched_from_url,
            )
        elif cached_urls is not None:
            print(f"IMDb images: imdb_id={imdb_id} parsed 0 images", file=sys.stderr)

    # Deduplicate and sort flat arrays
    genres = sorted(set(genres)) if genres else None
    keywords = sorted(set(keywords)) if keywords else None
    tags = sorted(set(tags)) if tags else None
    networks = sorted(set(networks)) if networks else None
    streaming_providers = sorted(set(streaming_providers)) if streaming_providers else None

    if not (
        show_update
        or genres
        or keywords
        or tags
        or networks
        or streaming_providers
        or tmdb_network_ids
        or tmdb_production_company_ids
        or show_images_rows
    ):
        return None

    return ShowEnrichmentPatch(
        show_id=show.id,
        show_update=show_update,
        genres=genres,
        keywords=keywords,
        tags=tags,
        networks=networks,
        streaming_providers=streaming_providers,
        tmdb_network_ids=sorted(set(tmdb_network_ids)) if tmdb_network_ids else None,
        tmdb_production_company_ids=sorted(set(tmdb_production_company_ids)) if tmdb_production_company_ids else None,
        show_images_rows=show_images_rows,
    )


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
    Build enrichment patches for core.shows + related tables (show_images).

    Network calls are performed here, but DB writes are performed by the caller.
    """

    region = (region or "US").strip().upper()
    concurrency = max(1, int(concurrency or 1))
    imdb_sleep_ms = max(0, int(imdb_sleep_ms or 0))

    tmdb_api_key = resolve_api_key() or None

    rows = list(show_rows)
    if max_enrich is not None:
        rows = rows[: max(0, int(max_enrich))]

    skipped_complete = 0

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

    from concurrent.futures import ThreadPoolExecutor, as_completed

    def run_one(show: ShowRecord) -> tuple[UUID, ShowEnrichmentPatch | None, str | None]:
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
            patches.append(patch)

    return EnrichSummary(
        attempted=attempted,
        updated=updated,
        skipped=skipped,
        skipped_complete=skipped_complete,
        failed=failed,
        patches=patches,
        failures=failures,
    )
