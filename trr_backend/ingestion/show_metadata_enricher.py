from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from threading import Lock
from typing import Any, Mapping
from uuid import UUID

import requests

from trr_backend.integrations.imdb.title_metadata_client import (
    HttpImdbTitleMetadataClient,
    parse_imdb_episodes_page,
    parse_imdb_title_page,
    pick_most_recent_episode,
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


@dataclass(frozen=True)
class EnrichFailure:
    show_id: UUID
    title: str
    message: str


@dataclass(frozen=True)
class EnrichSummary:
    attempted: int
    updated: int
    skipped: int
    failed: int
    patches: list[ShowEnrichmentPatch] = field(default_factory=list)
    failures: list[EnrichFailure] = field(default_factory=list)


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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
    cache_lock: Lock,
) -> dict[str, Any] | None:
    external_ids = dict(show.external_ids or {})
    existing_show_meta = external_ids.get("show_meta") if isinstance(external_ids.get("show_meta"), dict) else None

    if not force_refresh and isinstance(existing_show_meta, Mapping):
        fetched_at = existing_show_meta.get("fetched_at")
        if isinstance(fetched_at, str) and fetched_at.strip():
            if (existing_show_meta.get("region") or "").strip().upper() == region.upper():
                missing_fields = [
                    key
                    for key in (
                        "tmdb_series_id",
                        "network",
                        "streaming",
                        "show_total_seasons",
                        "show_total_episodes",
                        "most_recent_episode",
                    )
                    if _is_blank(existing_show_meta.get(key))
                ]

                if not missing_fields:
                    return None

                # If nothing missing is fillable given current inputs, skip to avoid hammering sources.
                imdb_id = show.imdb_id
                tmdb_series_id_existing = _as_int(existing_show_meta.get("tmdb_series_id")) or show.tmdb_id
                tmdb_possible = bool(tmdb_api_key and (tmdb_series_id_existing or imdb_id))
                imdb_possible = bool(imdb_id)

                def fillable(field: str) -> bool:
                    if field == "tmdb_series_id":
                        return bool(tmdb_api_key and imdb_id)
                    if field == "streaming":
                        return tmdb_possible
                    if field in {"network", "show_total_seasons", "show_total_episodes", "most_recent_episode"}:
                        return tmdb_possible or imdb_possible
                    return False

                if not any(fillable(f) for f in missing_fields):
                    return None

    show_meta: dict[str, Any] = dict(existing_show_meta or {}) if not force_refresh else {}
    show_meta["show"] = show.title
    show_meta["imdb_series_id"] = show.imdb_id

    tmdb_series_id: int | None = show.tmdb_id
    tmdb_sources: list[str] = []

    # Resolve TMDb id from IMDb id when missing.
    if tmdb_series_id is None and tmdb_api_key and show.imdb_id:
        with cache_lock:
            cached = tmdb_find_cache.get(show.imdb_id)
        if cached is None and show.imdb_id not in tmdb_find_cache:
            session = requests.Session()
            payload = find_by_imdb_id(show.imdb_id, api_key=tmdb_api_key, session=session)
            cached = _extract_tmdb_id_from_find(payload)
            with cache_lock:
                tmdb_find_cache[show.imdb_id] = cached
        tmdb_series_id = cached
        if tmdb_series_id is not None:
            tmdb_sources.append("find")

    show_meta["tmdb_series_id"] = tmdb_series_id

    updates: dict[str, Any] = {"show_meta": show_meta}
    if tmdb_series_id is not None and external_ids.get("tmdb") in (None, "", 0):
        updates["tmdb"] = int(tmdb_series_id)

    # Primary: TMDb TV details + watch providers.
    if tmdb_series_id is not None and tmdb_api_key:
        session = requests.Session()

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
            if names and _is_blank(show_meta.get("network")):
                show_meta["network"] = ", ".join(list(dict.fromkeys(names)))

        if show_meta.get("show_total_seasons") is None:
            show_meta["show_total_seasons"] = _as_int(details.get("number_of_seasons"))
        if show_meta.get("show_total_episodes") is None:
            show_meta["show_total_episodes"] = _as_int(details.get("number_of_episodes"))

        last = details.get("last_episode_to_air")
        if isinstance(last, dict) and _is_blank(show_meta.get("most_recent_episode")):
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

        if _is_blank(show_meta.get("streaming")):
            show_meta["streaming"] = _pick_watch_providers(providers_payload, region=region.upper())

    # Fallback: IMDb title + episodes pages, only for missing values.
    imdb_sources: list[str] = []
    need_imdb_title = _is_blank(show_meta.get("network")) or show_meta.get("show_total_episodes") is None
    need_imdb_episodes = show_meta.get("show_total_seasons") is None or _is_blank(show_meta.get("most_recent_episode"))

    if show.imdb_id and (need_imdb_title or need_imdb_episodes):
        client = HttpImdbTitleMetadataClient(sleep_ms=imdb_sleep_ms)

        if need_imdb_title:
            with cache_lock:
                title_html = imdb_title_cache.get(show.imdb_id)
            if title_html is None:
                title_html = client.fetch_title_page(show.imdb_id)
                with cache_lock:
                    imdb_title_cache[show.imdb_id] = title_html
            meta = parse_imdb_title_page(title_html)
            imdb_sources.append("title")

            if _is_blank(show_meta.get("network")) and meta.network:
                show_meta["network"] = meta.network
            if show_meta.get("show_total_episodes") is None and meta.total_episodes is not None:
                show_meta["show_total_episodes"] = int(meta.total_episodes)

        if need_imdb_episodes:
            key = (show.imdb_id, None)
            with cache_lock:
                episodes_html = imdb_episodes_cache.get(key)
            if episodes_html is None:
                episodes_html = client.fetch_episodes_page(show.imdb_id)
                with cache_lock:
                    imdb_episodes_cache[key] = episodes_html
            episodes_meta = parse_imdb_episodes_page(episodes_html)
            imdb_sources.append("episodes")

            if show_meta.get("show_total_seasons") is None and episodes_meta.available_seasons:
                show_meta["show_total_seasons"] = int(max(episodes_meta.available_seasons))

            if _is_blank(show_meta.get("most_recent_episode")) and episodes_meta.available_seasons:
                max_season = max(episodes_meta.available_seasons)
                key = (show.imdb_id, int(max_season))
                with cache_lock:
                    season_html = imdb_episodes_cache.get(key)
                if season_html is None:
                    season_html = client.fetch_episodes_page(show.imdb_id, season=max_season)
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

    show_meta["region"] = region.upper()
    show_meta["fetched_at"] = _now_utc_iso()

    source: dict[str, Any] = show_meta.get("source") if isinstance(show_meta.get("source"), dict) else {}
    if tmdb_sources:
        source["tmdb"] = "|".join(sorted(set(tmdb_sources), key=tmdb_sources.index))
    if imdb_sources:
        source["imdb"] = "|".join(sorted(set(imdb_sources), key=imdb_sources.index))
    show_meta["source"] = source

    _ensure_show_meta_shape(show_meta)

    # If we couldn't resolve TMDb id and no TMDb key, keep explicit nulls.
    if show_meta.get("tmdb_series_id") is None:
        show_meta["tmdb_series_id"] = None

    return updates


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

    tmdb_find_cache: dict[str, int | None] = {}
    tmdb_details_cache: dict[int, dict[str, Any]] = {}
    tmdb_watch_cache: dict[int, dict[str, Any]] = {}
    imdb_title_cache: dict[str, str] = {}
    imdb_episodes_cache: dict[tuple[str, int | None], str] = {}
    cache_lock = Lock()

    attempted = 0
    updated = 0
    skipped = 0
    failed = 0
    patches: list[ShowEnrichmentPatch] = []
    failures: list[EnrichFailure] = []

    # Keep concurrency polite and simple; a worker fetches and builds one show patch.
    from concurrent.futures import ThreadPoolExecutor, as_completed

    def run_one(show: ShowRecord) -> tuple[UUID, dict[str, Any] | None, str | None]:
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
                failures.append(EnrichFailure(show_id=show_id, title=show.title, message=error))
                continue
            if patch is None:
                skipped += 1
                continue

            updated += 1
            patches.append(ShowEnrichmentPatch(show_id=show_id, external_ids_update=patch))

    return EnrichSummary(
        attempted=attempted,
        updated=updated,
        skipped=skipped,
        failed=failed,
        patches=patches,
        failures=failures,
    )
