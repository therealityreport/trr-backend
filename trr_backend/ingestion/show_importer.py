from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping
from uuid import UUID, uuid4

import requests
from supabase import Client

from trr_backend.db.supabase import create_supabase_admin_client
from trr_backend.ingestion.show_metadata_enricher import enrich_shows_after_upsert
from trr_backend.ingestion.shows_from_lists import (
    CandidateShow,
    ImdbListItem,
    TmdbListItem,
    fetch_imdb_list_items,
    fetch_tmdb_list_items,
    merge_candidates,
    parse_imdb_list_id,
)
from trr_backend.integrations.imdb.episodic_client import HttpImdbEpisodicClient, IMDB_JOB_CATEGORY_SELF
from trr_backend.integrations.tmdb.client import TmdbClientError, fetch_tv_details, fetch_tv_images
from trr_backend.models.shows import ShowRecord, ShowUpsert
from trr_backend.repositories.show_images import assert_core_show_images_table_exists, upsert_show_images
from trr_backend.repositories.shows import (
    assert_core_shows_table_exists,
    find_show_by_imdb_id,
    find_show_by_tmdb_id,
    insert_show,
    update_show,
)


@dataclass(frozen=True)
class ShowImportResult:
    created: int
    updated: int
    skipped: int
    upserted_show_rows: list[dict[str, Any]]


def _merge_external_ids(existing: Mapping[str, Any] | None, updates: Mapping[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = dict(existing or {})

    # Merge sources list.
    existing_sources = merged.get("import_sources")
    sources: set[str] = set()
    if isinstance(existing_sources, list):
        sources |= {str(s) for s in existing_sources if str(s).strip()}
    new_sources = updates.get("import_sources")
    if isinstance(new_sources, list):
        sources |= {str(s) for s in new_sources if str(s).strip()}
    if sources:
        merged["import_sources"] = sorted(sources)

    # Merge nested metadata dicts.
    for key in ("tmdb_meta", "imdb_meta", "imdb_episodic", "show_meta"):
        existing_value = merged.get(key)
        existing_dict = existing_value if isinstance(existing_value, dict) else {}
        update_value = updates.get(key)
        update_dict = update_value if isinstance(update_value, dict) else {}
        if update_dict:
            merged[key] = {**existing_dict, **update_dict}

    def has_nonempty_str(value: Any) -> bool:
        return isinstance(value, str) and bool(value.strip())

    def has_nonempty_int(value: Any) -> bool:
        if isinstance(value, int):
            return value > 0
        if isinstance(value, str) and value.strip().isdigit():
            return int(value.strip()) > 0
        return False

    def is_missing_str(value: Any) -> bool:
        return not has_nonempty_str(value)

    def is_missing_int(value: Any) -> bool:
        return not has_nonempty_int(value)

    # Only set canonical external ids if missing/blank.
    for key in ("imdb", "wikidata", "facebook", "instagram", "twitter"):
        if is_missing_str(merged.get(key)) and has_nonempty_str(updates.get(key)):
            merged[key] = str(updates[key]).strip()

    for key in ("tmdb", "tvdb", "tvrage"):
        if is_missing_int(merged.get(key)) and has_nonempty_int(updates.get(key)):
            merged[key] = int(updates[key])

    return merged


def _candidate_to_show_upsert(candidate: CandidateShow, *, annotate_imdb_episodic: bool) -> ShowUpsert:
    external_ids: dict[str, Any] = {}
    if candidate.imdb_id:
        external_ids["imdb"] = candidate.imdb_id
        if annotate_imdb_episodic:
            external_ids["imdb_episodic"] = {"supported": True}
    if candidate.tmdb_id is not None:
        external_ids["tmdb"] = int(candidate.tmdb_id)

    tmdb_external_ids = candidate.tmdb_meta.get("external_ids")
    if isinstance(tmdb_external_ids, dict):
        imdb_id = tmdb_external_ids.get("imdb_id")
        if "imdb" not in external_ids and isinstance(imdb_id, str) and imdb_id.strip():
            external_ids["imdb"] = imdb_id.strip()

        def as_int(value: Any) -> int | None:
            if value is None:
                return None
            if isinstance(value, int):
                return value
            if isinstance(value, str):
                raw = value.strip()
                if raw.isdigit():
                    return int(raw)
                return None
            return None

        def as_str(value: Any) -> str | None:
            if not isinstance(value, str):
                return None
            raw = value.strip()
            return raw or None

        tvdb_id = as_int(tmdb_external_ids.get("tvdb_id"))
        if tvdb_id is not None:
            external_ids["tvdb"] = tvdb_id

        tvrage_id = as_int(tmdb_external_ids.get("tvrage_id"))
        if tvrage_id is not None:
            external_ids["tvrage"] = tvrage_id

        wikidata_id = as_str(tmdb_external_ids.get("wikidata_id"))
        if wikidata_id is not None:
            external_ids["wikidata"] = wikidata_id

        for key in ("facebook_id", "instagram_id", "twitter_id"):
            value = as_str(tmdb_external_ids.get(key))
            if value is None:
                continue
            external_ids[key.replace("_id", "")] = value

    if candidate.source_tags:
        external_ids["import_sources"] = sorted(candidate.source_tags)

    tmdb_meta: dict[str, Any] = dict(candidate.tmdb_meta or {})
    if candidate.first_air_date:
        tmdb_meta.setdefault("first_air_date", candidate.first_air_date)
    if candidate.origin_country:
        tmdb_meta.setdefault("origin_country", list(candidate.origin_country))
    if tmdb_meta:
        external_ids["tmdb_meta"] = tmdb_meta

    imdb_meta: dict[str, Any] = dict(candidate.imdb_meta or {})
    if candidate.year is not None:
        imdb_meta.setdefault("year", int(candidate.year))
        imdb_meta.setdefault("release_year", int(candidate.year))
    if imdb_meta:
        external_ids["imdb_meta"] = imdb_meta

    return ShowUpsert(
        title=candidate.title,
        premiere_date=candidate.first_air_date,
        description=None,
        external_ids=external_ids,
    )


def _imdb_meta_from_list_item(item: ImdbListItem) -> dict[str, Any]:
    meta: dict[str, Any] = {}
    if item.imdb_rating is not None:
        meta["rating"] = float(item.imdb_rating)
    if item.imdb_vote_count is not None:
        meta["vote_count"] = int(item.imdb_vote_count)
    if item.description:
        meta["plot"] = item.description
    if item.release_year is not None:
        meta["release_year"] = int(item.release_year)
    if item.end_year is not None:
        meta["end_year"] = int(item.end_year)
    if item.episodes_total is not None:
        meta["episodes_total"] = int(item.episodes_total)
    if item.title_type:
        meta["title_type"] = item.title_type
    if item.primary_image_url:
        meta["primary_image_url"] = item.primary_image_url
    if item.primary_image_caption:
        meta["primary_image_caption"] = item.primary_image_caption
    if item.certificate:
        meta["certificate"] = item.certificate
    if item.runtime_seconds is not None:
        meta["runtime_seconds"] = int(item.runtime_seconds)
    if item.genres:
        meta["genres"] = list(item.genres)
    if item.list_rank is not None:
        meta["list_rank"] = int(item.list_rank)
    if item.list_item_note:
        meta["list_item_note"] = item.list_item_note
    if item.year is not None:
        meta.setdefault("year", int(item.year))
    return meta


def _tmdb_meta_from_tv_details(details: Mapping[str, Any]) -> dict[str, Any]:
    meta: dict[str, Any] = {"_v": 1}

    def copy_scalar(key: str) -> None:
        value = details.get(key)
        if value is None:
            return
        meta[key] = value

    for key in (
        "id",
        "name",
        "original_name",
        "overview",
        "first_air_date",
        "last_air_date",
        "in_production",
        "status",
        "type",
        "tagline",
        "homepage",
        "original_language",
        "origin_country",
        "languages",
        "episode_run_time",
        "number_of_seasons",
        "number_of_episodes",
        "vote_average",
        "vote_count",
        "popularity",
        "poster_path",
        "backdrop_path",
        "adult",
    ):
        copy_scalar(key)

    def copy_list_of_dicts(key: str, allowed_keys: tuple[str, ...]) -> None:
        raw = details.get(key)
        if raw is None:
            return
        if not isinstance(raw, list):
            return
        out: list[dict[str, Any]] = []
        for item in raw:
            if not isinstance(item, Mapping):
                continue
            filtered = {k: item.get(k) for k in allowed_keys if k in item}
            out.append(filtered)
        meta[key] = out

    copy_list_of_dicts("genres", ("id", "name"))
    copy_list_of_dicts("networks", ("id", "name", "logo_path", "origin_country"))
    copy_list_of_dicts("created_by", ("id", "name", "gender", "profile_path", "credit_id"))
    copy_list_of_dicts("production_companies", ("id", "name", "logo_path", "origin_country"))
    copy_list_of_dicts("production_countries", ("iso_3166_1", "name"))
    copy_list_of_dicts("spoken_languages", ("english_name", "iso_639_1", "name"))

    seasons_raw = details.get("seasons")
    if seasons_raw is not None and isinstance(seasons_raw, list):
        seasons: list[dict[str, Any]] = []
        for season in seasons_raw:
            if not isinstance(season, Mapping):
                continue
            seasons.append(
                {
                    "id": season.get("id"),
                    "season_number": season.get("season_number"),
                    "name": season.get("name"),
                    "air_date": season.get("air_date"),
                    "episode_count": season.get("episode_count"),
                    "overview": season.get("overview"),
                    "poster_path": season.get("poster_path"),
                    "vote_average": season.get("vote_average"),
                }
            )
        meta["seasons"] = seasons

    def copy_episode_obj(key: str) -> None:
        raw = details.get(key)
        if raw is None:
            return
        if not isinstance(raw, Mapping):
            return
        meta[key] = {
            "id": raw.get("id"),
            "name": raw.get("name"),
            "air_date": raw.get("air_date"),
            "season_number": raw.get("season_number"),
            "episode_number": raw.get("episode_number"),
            "overview": raw.get("overview"),
            "vote_average": raw.get("vote_average"),
            "vote_count": raw.get("vote_count"),
            "still_path": raw.get("still_path"),
        }

    copy_episode_obj("last_episode_to_air")
    copy_episode_obj("next_episode_to_air")

    alt_raw = details.get("alternative_titles")
    if isinstance(alt_raw, Mapping):
        results = alt_raw.get("results")
        if isinstance(results, list):
            alt_titles: list[dict[str, Any]] = []
            for item in results:
                if not isinstance(item, Mapping):
                    continue
                alt_titles.append(
                    {
                        "iso_3166_1": item.get("iso_3166_1"),
                        "title": item.get("title"),
                        "type": item.get("type"),
                    }
                )
            meta["alternative_titles"] = alt_titles

    external_ids_raw = details.get("external_ids")
    if isinstance(external_ids_raw, Mapping):
        meta["external_ids"] = {
            "imdb_id": external_ids_raw.get("imdb_id"),
            "tvdb_id": external_ids_raw.get("tvdb_id"),
            "wikidata_id": external_ids_raw.get("wikidata_id"),
            "facebook_id": external_ids_raw.get("facebook_id"),
            "instagram_id": external_ids_raw.get("instagram_id"),
            "twitter_id": external_ids_raw.get("twitter_id"),
            "tvrage_id": external_ids_raw.get("tvrage_id"),
        }

    # Certificate/rating is not part of the v3 TV details response. Leave it to IMDb/meta sources if needed.
    return meta


def _is_english_iso_639_1(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    raw = value.strip().casefold()
    return raw == "en" or raw.startswith("en-")


def _tmdb_image_sort_key(image: Mapping[str, Any]) -> tuple[int, int, float, str]:
    iso = image.get("iso_639_1")
    if _is_english_iso_639_1(iso):
        bucket = 0
    elif iso is None or (isinstance(iso, str) and not iso.strip()):
        bucket = 1
    else:
        bucket = 2

    vote_count = image.get("vote_count")
    vote_count_int = int(vote_count) if isinstance(vote_count, int) else 0
    vote_average = image.get("vote_average")
    vote_avg_float = float(vote_average) if isinstance(vote_average, (int, float)) else 0.0
    file_path = str(image.get("file_path") or "")
    return (bucket, -vote_count_int, -vote_avg_float, file_path)


def _normalize_tmdb_images_list(raw: Any) -> list[dict[str, Any]]:
    if not isinstance(raw, list):
        return []

    by_file_path: dict[str, dict[str, Any]] = {}
    for item in raw:
        if not isinstance(item, Mapping):
            continue
        file_path = item.get("file_path")
        if not isinstance(file_path, str) or not file_path.strip():
            continue

        normalized: dict[str, Any] = {
            "iso_639_1": item.get("iso_639_1") if isinstance(item.get("iso_639_1"), str) else None,
            "file_path": file_path,
            "width": item.get("width") if isinstance(item.get("width"), int) else None,
            "height": item.get("height") if isinstance(item.get("height"), int) else None,
            "aspect_ratio": item.get("aspect_ratio") if isinstance(item.get("aspect_ratio"), (int, float)) else None,
            "vote_average": item.get("vote_average") if isinstance(item.get("vote_average"), (int, float)) else None,
            "vote_count": item.get("vote_count") if isinstance(item.get("vote_count"), int) else None,
        }

        existing = by_file_path.get(file_path)
        if existing is None or _tmdb_image_sort_key(normalized) < _tmdb_image_sort_key(existing):
            by_file_path[file_path] = normalized

    return list(by_file_path.values())


def _primary_tmdb_image_file_path(images: list[dict[str, Any]]) -> str | None:
    if not images:
        return None
    sorted_images = sorted(images, key=_tmdb_image_sort_key)
    file_path = sorted_images[0].get("file_path")
    return file_path if isinstance(file_path, str) and file_path.strip() else None


def _tmdb_show_images_rows(
    payload: Mapping[str, Any],
    *,
    show_id: str,
    fetched_at: str,
    source: str = "tmdb",
) -> tuple[list[dict[str, Any]], dict[str, str | None]]:
    kind_to_key = {"poster": "posters", "backdrop": "backdrops", "logo": "logos"}

    rows: list[dict[str, Any]] = []
    primary: dict[str, str | None] = {}

    for kind, key in kind_to_key.items():
        images = _normalize_tmdb_images_list(payload.get(key))
        primary[kind] = _primary_tmdb_image_file_path(images)
        for img in images:
            rows.append(
                {
                    "show_id": show_id,
                    "source": source,
                    "kind": kind,
                    "iso_639_1": img.get("iso_639_1"),
                    "file_path": img.get("file_path"),
                    "width": img.get("width"),
                    "height": img.get("height"),
                    "aspect_ratio": img.get("aspect_ratio"),
                    "vote_average": img.get("vote_average"),
                    "vote_count": img.get("vote_count"),
                    "fetched_at": fetched_at,
                }
            )

    return rows, primary


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_iso8601_utc(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    raw = value.strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _tmdb_meta_is_fresh(
    tmdb_meta: Mapping[str, Any],
    *,
    tmdb_id: int,
    language: str,
    max_age_days: int,
    now: datetime,
) -> bool:
    """
    Return True if an existing `external_ids["tmdb_meta"]` is usable without refetching.
    """

    meta_id = tmdb_meta.get("id")
    if not isinstance(meta_id, int) and not (isinstance(meta_id, str) and meta_id.isdigit()):
        return False
    try:
        meta_id_int = int(meta_id)
    except ValueError:
        return False
    if meta_id_int != int(tmdb_id):
        return False

    if tmdb_meta.get("_v") != 1:
        return False

    # Ensure the v1 schema includes the additional appended payloads we expect.
    if not isinstance(tmdb_meta.get("external_ids"), Mapping):
        return False
    if not isinstance(tmdb_meta.get("alternative_titles"), list):
        return False

    meta_lang = tmdb_meta.get("language")
    if isinstance(meta_lang, str) and meta_lang.strip():
        if meta_lang.strip() != language:
            return False
    else:
        return False

    fetched_at = _parse_iso8601_utc(tmdb_meta.get("fetched_at"))
    if fetched_at is None:
        return False

    if max_age_days <= 0:
        return False

    age = now - fetched_at
    return age.total_seconds() <= (max_age_days * 86400)


def collect_candidates_from_lists(
    *,
    imdb_list_urls: Iterable[str],
    tmdb_lists: Iterable[str | int],
    tmdb_api_key: str | None = None,
    http_session: Any | None = None,
    resolve_tmdb_external_ids: bool = True,
    imdb_use_graphql: bool = True,
) -> list[CandidateShow]:
    session = http_session
    tmdb_session = session if isinstance(session, requests.Session) else requests.Session()

    imdb_candidates: list[CandidateShow] = []
    for url in imdb_list_urls:
        list_id = parse_imdb_list_id(url)
        items: list[ImdbListItem] = fetch_imdb_list_items(url, session=session, use_graphql=bool(imdb_use_graphql))
        tag = f"imdb-list:{list_id}"
        for item in items:
            imdb_candidates.append(
                CandidateShow(
                    imdb_id=item.imdb_id,
                    tmdb_id=None,
                    title=item.title,
                    year=item.year,
                    imdb_meta=_imdb_meta_from_list_item(item),
                    source_tags={tag},
                )
            )

    tmdb_candidates: list[CandidateShow] = []
    for value in tmdb_lists:
        # Keep the list id in tags for traceability.
        try:
            from trr_backend.integrations.tmdb.client import parse_tmdb_list_id

            list_id_int = parse_tmdb_list_id(value)
        except Exception:
            list_id_int = None

        items: list[TmdbListItem] = fetch_tmdb_list_items(
            value,
            api_key=tmdb_api_key,
            session=tmdb_session,
            resolve_external_ids=bool(resolve_tmdb_external_ids),
        )
        list_tag = f"tmdb-list:{list_id_int}" if list_id_int is not None else f"tmdb-list:{value}"

        for item in items:
            tmdb_candidates.append(
                CandidateShow(
                    imdb_id=item.imdb_id,
                    tmdb_id=item.tmdb_id,
                    title=item.name,
                    first_air_date=item.first_air_date,
                    origin_country=item.origin_country,
                    source_tags={list_tag},
                )
            )

    return merge_candidates([*imdb_candidates, *tmdb_candidates])


def annotate_candidates_imdb_episodic(
    candidates: Iterable[CandidateShow],
    *,
    probe_name_id: str,
    probe_job_category_id: str = IMDB_JOB_CATEGORY_SELF,
    extra_headers: Mapping[str, str] | None = None,
) -> dict[str, list[int]]:
    """
    Optional probe to check IMDb episodic GraphQL reachability and discover seasons.

    This requires a real IMDb `nameId` and `jobCategoryId`. If you don't have those,
    do not use this probe (set only `supported=True` flags instead).
    """

    client = HttpImdbEpisodicClient(extra_headers=extra_headers)
    seasons_by_imdb_id: dict[str, list[int]] = {}
    for c in candidates:
        if not c.imdb_id:
            continue
        seasons_by_imdb_id[c.imdb_id] = client.fetch_available_seasons(
            c.imdb_id,
            probe_name_id,
            probe_job_category_id,
        )
    return seasons_by_imdb_id


def upsert_candidates_into_supabase(
    candidates: Iterable[CandidateShow],
    *,
    dry_run: bool,
    annotate_imdb_episodic: bool,
    tmdb_fetch_details: bool = True,
    tmdb_details_max_age_days: int = 90,
    tmdb_details_language: str = "en-US",
    tmdb_fetch_images: bool = False,
    enrich_show_metadata: bool = False,
    enrich_region: str = "US",
    enrich_concurrency: int = 5,
    enrich_max_enrich: int | None = None,
    enrich_force_refresh: bool = False,
    enrich_imdb_sleep_ms: int = 0,
    supabase_client: Client | None = None,
    imdb_episodic_probe_name_id: str | None = None,
    imdb_episodic_probe_job_category_id: str = IMDB_JOB_CATEGORY_SELF,
    imdb_episodic_extra_headers: Mapping[str, str] | None = None,
) -> ShowImportResult:
    candidates_list = list(candidates)

    db = supabase_client or (None if dry_run else create_supabase_admin_client())
    if db is not None:
        assert_core_shows_table_exists(db)

    seasons_by_imdb_id: dict[str, list[int]] = {}
    if annotate_imdb_episodic and imdb_episodic_probe_name_id:
        seasons_by_imdb_id = annotate_candidates_imdb_episodic(
            candidates_list,
            probe_name_id=imdb_episodic_probe_name_id,
            probe_job_category_id=imdb_episodic_probe_job_category_id,
            extra_headers=imdb_episodic_extra_headers,
        )

    created = 0
    updated = 0
    skipped = 0
    upserted_show_rows: list[dict[str, Any]] = []

    tmdb_details_total = sum(1 for c in candidates_list if c.tmdb_id is not None) if tmdb_fetch_details else 0
    tmdb_details_processed = 0
    tmdb_details_fetched = 0
    tmdb_details_skipped_fresh = 0
    tmdb_details_skipped_cached = 0
    tmdb_details_failed = 0
    tmdb_details_session = requests.Session()
    tmdb_details_append = ("alternative_titles", "external_ids")
    tmdb_details_cache: dict[tuple[int, str, tuple[str, ...]], dict[str, Any]] = {}
    now = datetime.now(timezone.utc)

    def _coerce_int(value: Any) -> int | None:
        if isinstance(value, int):
            return value
        if isinstance(value, str):
            raw = value.strip()
            if raw.isdigit():
                return int(raw)
        return None

    for idx, candidate in enumerate(candidates_list, start=1):
        existing: dict[str, Any] | None = None
        if db is not None:
            if candidate.imdb_id:
                existing = find_show_by_imdb_id(db, candidate.imdb_id)
            if existing is None and candidate.tmdb_id is not None:
                existing = find_show_by_tmdb_id(db, int(candidate.tmdb_id))

        existing_external_ids = existing.get("external_ids") if isinstance(existing, dict) else None
        existing_external_ids_map = existing_external_ids if isinstance(existing_external_ids, dict) else {}

        # Stage 1 TMDb details capture (optional): persist curated TV details into external_ids.tmdb_meta.
        if tmdb_fetch_details and candidate.tmdb_id is not None:
            tmdb_details_processed += 1

            tmdb_id_for_details = _coerce_int(existing_external_ids_map.get("tmdb")) or int(candidate.tmdb_id)
            existing_tmdb_meta = existing_external_ids_map.get("tmdb_meta") if isinstance(existing_external_ids_map, dict) else None
            is_fresh = (
                isinstance(existing_tmdb_meta, Mapping)
                and _tmdb_meta_is_fresh(
                    existing_tmdb_meta,
                    tmdb_id=tmdb_id_for_details,
                    language=tmdb_details_language,
                    max_age_days=int(tmdb_details_max_age_days or 0),
                    now=now,
                )
            )

            if is_fresh:
                tmdb_details_skipped_fresh += 1
                if isinstance(existing_tmdb_meta, Mapping):
                    candidate.tmdb_meta = {**dict(existing_tmdb_meta), **candidate.tmdb_meta}
            else:
                try:
                    cache_key = (tmdb_id_for_details, tmdb_details_language, tmdb_details_append)
                    if cache_key in tmdb_details_cache:
                        tmdb_details_skipped_cached += 1
                        details = tmdb_details_cache[cache_key]
                    else:
                        details = fetch_tv_details(
                            tmdb_id_for_details,
                            api_key=None,
                            session=tmdb_details_session,
                            language=tmdb_details_language,
                            append_to_response=list(tmdb_details_append),
                            cache=tmdb_details_cache,
                        )
                        tmdb_details_fetched += 1

                    tmdb_meta = _tmdb_meta_from_tv_details(details)
                    tmdb_meta["language"] = tmdb_details_language
                    tmdb_meta["fetched_at"] = _now_utc_iso()
                    candidate.tmdb_meta = {**candidate.tmdb_meta, **tmdb_meta}
                except TmdbClientError as exc:
                    tmdb_details_failed += 1
                    status = exc.status_code
                    if status in {404, 422}:
                        print(
                            f"TMDb details: skipping tmdb_id={tmdb_id_for_details} (HTTP {status})",
                            file=sys.stderr,
                        )
                    else:
                        print(
                            f"TMDb details: failed tmdb_id={tmdb_id_for_details} "
                            f"(HTTP {status if status is not None else 'unknown'})",
                            file=sys.stderr,
                        )
                except Exception:
                    tmdb_details_failed += 1
                    print(f"TMDb details: failed tmdb_id={tmdb_id_for_details} (unexpected error)", file=sys.stderr)

            if tmdb_details_total:
                if tmdb_details_processed == 1 or tmdb_details_processed % 10 == 0 or tmdb_details_processed == tmdb_details_total:
                    print(
                        f"TMDb details: processed {tmdb_details_processed}/{tmdb_details_total} "
                        f"(fetched={tmdb_details_fetched} "
                        f"skipped_fresh={tmdb_details_skipped_fresh} "
                        f"cached={tmdb_details_skipped_cached} "
                        f"failed={tmdb_details_failed})",
                        file=sys.stderr,
                    )

        show_upsert = _candidate_to_show_upsert(candidate, annotate_imdb_episodic=annotate_imdb_episodic)

        # If probing, attach seasons to external ids for shows with imdb ids.
        if annotate_imdb_episodic and candidate.imdb_id and candidate.imdb_id in seasons_by_imdb_id:
            show_upsert.external_ids.setdefault("imdb_episodic", {})["available_seasons"] = seasons_by_imdb_id[
                candidate.imdb_id
            ]
            show_upsert.external_ids.setdefault("imdb_episodic", {})["reachable"] = True

        if existing is None:
            if dry_run:
                print(
                    f"CREATE show imdb_id={candidate.imdb_id or ''} tmdb_id={candidate.tmdb_id or ''} "
                    f"title={candidate.title!r}"
                )
                created += 1
                upserted_show_rows.append(
                    {
                        "id": str(uuid4()),
                        "title": show_upsert.title,
                        "description": show_upsert.description,
                        "premiere_date": show_upsert.premiere_date,
                        "external_ids": show_upsert.external_ids,
                    }
                )
                continue

            inserted = insert_show(db, show_upsert)
            created += 1
            upserted_show_rows.append(inserted)
            print(f"CREATED show id={inserted.get('id')} title={inserted.get('title')!r}")
            continue

        merged_external_ids = _merge_external_ids(existing_external_ids_map, show_upsert.external_ids)

        patch: dict[str, Any] = {}
        if merged_external_ids != existing_external_ids_map:
            patch["external_ids"] = merged_external_ids
        if not existing.get("premiere_date") and show_upsert.premiere_date:
            patch["premiere_date"] = show_upsert.premiere_date

        if not patch:
            skipped += 1
            upserted_show_rows.append(existing)
            continue

        if dry_run:
            print(
                f"UPDATE show id={existing.get('id')} imdb_id={candidate.imdb_id or ''} "
                f"tmdb_id={candidate.tmdb_id or ''} patch_keys={sorted(patch.keys())}"
            )
            updated += 1
            merged_existing = dict(existing)
            merged_existing.update(patch)
            upserted_show_rows.append(merged_existing)
            continue

        updated_row = update_show(db, existing["id"], patch)
        updated += 1
        upserted_show_rows.append(updated_row)
        print(f"UPDATED show id={updated_row.get('id')} title={updated_row.get('title')!r}")

    if tmdb_fetch_details and tmdb_details_total:
        print(
            "TMDb details summary "
            f"tmdb_details_fetched={tmdb_details_fetched} "
            f"tmdb_details_skipped_fresh={tmdb_details_skipped_fresh} "
            f"tmdb_details_skipped_cached={tmdb_details_skipped_cached} "
            f"tmdb_details_failed={tmdb_details_failed}",
            file=sys.stderr,
        )

    # Optional TMDb images capture (posters/logos/backdrops): persist into core.show_images and set primary_* columns.
    if tmdb_fetch_images:
        tmdb_images_language = "en-US"
        tmdb_images_include_lang = "en-US,null"
        tmdb_images_session = requests.Session()
        tmdb_images_cache: dict[tuple[int, str, str], dict[str, Any]] = {}

        tmdb_images_total = 0
        for row in upserted_show_rows:
            external_ids = row.get("external_ids")
            external_ids_map = external_ids if isinstance(external_ids, dict) else {}
            tmdb_id_val = external_ids_map.get("tmdb")
            if isinstance(tmdb_id_val, int) or (isinstance(tmdb_id_val, str) and tmdb_id_val.strip().isdigit()):
                tmdb_images_total += 1

        if db is not None:
            assert_core_show_images_table_exists(db)

        tmdb_images_processed = 0
        tmdb_images_fetched = 0
        tmdb_images_skipped_cached = 0
        tmdb_images_failed = 0

        for i, row in enumerate(upserted_show_rows, start=1):
            row_id = row.get("id")
            show_id = str(row_id) if row_id is not None else ""
            if not show_id:
                continue

            external_ids = row.get("external_ids")
            external_ids_map = external_ids if isinstance(external_ids, dict) else {}
            tmdb_id_val = external_ids_map.get("tmdb")
            if isinstance(tmdb_id_val, int):
                tmdb_id_int = tmdb_id_val
            elif isinstance(tmdb_id_val, str) and tmdb_id_val.strip().isdigit():
                tmdb_id_int = int(tmdb_id_val.strip())
            else:
                continue

            tmdb_images_processed += 1

            cache_key = (tmdb_id_int, tmdb_images_language, tmdb_images_include_lang)
            fetched_at = _now_utc_iso()

            try:
                if cache_key in tmdb_images_cache:
                    tmdb_images_skipped_cached += 1
                    payload = tmdb_images_cache[cache_key]
                else:
                    payload = fetch_tv_images(
                        tmdb_id_int,
                        api_key=None,
                        session=tmdb_images_session,
                        language=tmdb_images_language,
                        include_image_language=tmdb_images_include_lang,
                        cache=tmdb_images_cache,
                    )
                    tmdb_images_fetched += 1

                image_rows, primary = _tmdb_show_images_rows(payload, show_id=show_id, fetched_at=fetched_at)
                if db is not None and not dry_run:
                    upsert_show_images(db, image_rows)

                patch: dict[str, Any] = {}
                poster = primary.get("poster")
                backdrop = primary.get("backdrop")
                logo = primary.get("logo")

                if isinstance(poster, str) and poster.strip() and poster != row.get("primary_tmdb_poster_path"):
                    patch["primary_tmdb_poster_path"] = poster
                if isinstance(backdrop, str) and backdrop.strip() and backdrop != row.get("primary_tmdb_backdrop_path"):
                    patch["primary_tmdb_backdrop_path"] = backdrop
                if isinstance(logo, str) and logo.strip() and logo != row.get("primary_tmdb_logo_path"):
                    patch["primary_tmdb_logo_path"] = logo

                if patch:
                    if dry_run:
                        print(f"TMDb images UPDATE show id={show_id} patch_keys={sorted(patch.keys())}")
                        row.update(patch)
                    elif db is not None:
                        updated_row = update_show(db, show_id, patch)
                        row.update(updated_row)
            except TmdbClientError as exc:
                tmdb_images_failed += 1
                status = exc.status_code
                if status in {404, 422}:
                    print(f"TMDb images: skipping tmdb_id={tmdb_id_int} (HTTP {status})", file=sys.stderr)
                else:
                    print(
                        f"TMDb images: failed tmdb_id={tmdb_id_int} "
                        f"(HTTP {status if status is not None else 'unknown'})",
                        file=sys.stderr,
                    )
            except Exception:
                tmdb_images_failed += 1
                print(f"TMDb images: failed tmdb_id={tmdb_id_int} (unexpected error)", file=sys.stderr)

            if tmdb_images_total:
                if tmdb_images_processed == 1 or tmdb_images_processed % 10 == 0 or tmdb_images_processed == tmdb_images_total:
                    print(
                        f"TMDb images: processed {tmdb_images_processed}/{tmdb_images_total} "
                        f"(fetched={tmdb_images_fetched} cached={tmdb_images_skipped_cached} failed={tmdb_images_failed})",
                        file=sys.stderr,
                    )

        if tmdb_images_total:
            print(
                "TMDb images summary "
                f"tmdb_images_fetched={tmdb_images_fetched} "
                f"tmdb_images_skipped_cached={tmdb_images_skipped_cached} "
                f"tmdb_images_failed={tmdb_images_failed}",
                file=sys.stderr,
            )

    # Stage 2 enrichment: populate external_ids.show_meta.
    if enrich_show_metadata:
        show_records: list[ShowRecord] = []
        by_id: dict[str, dict[str, Any]] = {}
        for row in upserted_show_rows:
            row_id = row.get("id")
            if not isinstance(row_id, str) or not row_id:
                continue
            by_id[row_id] = row

            try:
                show_id = UUID(row_id)
            except Exception:
                show_id = uuid4()

            external_ids = row.get("external_ids")
            external_ids_map = external_ids if isinstance(external_ids, dict) else {}

            show_records.append(
                ShowRecord(
                    id=show_id,
                    title=str(row.get("title") or ""),
                    description=row.get("description") if isinstance(row.get("description"), str) else None,
                    premiere_date=row.get("premiere_date") if isinstance(row.get("premiere_date"), str) else None,
                    external_ids=external_ids_map,
                )
            )

        summary = enrich_shows_after_upsert(
            show_records,
            region=enrich_region,
            concurrency=enrich_concurrency,
            max_enrich=enrich_max_enrich,
            force_refresh=enrich_force_refresh,
            dry_run=dry_run,
            imdb_sleep_ms=enrich_imdb_sleep_ms,
        )

        print(
            "ENRICH summary "
            f"attempted={summary.attempted} "
            f"updated={summary.updated} "
            f"skipped_complete={summary.skipped_complete} "
            f"skipped={summary.skipped} "
            f"failed={summary.failed}"
        )

        if summary.failures:
            print(f"ENRICH failed={summary.failed} (show metadata).")
            for failure in summary.failures[:10]:
                print(f"ENRICH FAIL show_id={failure.show_id} title={failure.title!r} error={failure.message}")
            if len(summary.failures) > 10:
                print(f"ENRICH FAIL ... and {len(summary.failures) - 10} more")

        for patch in summary.patches:
            row = by_id.get(str(patch.show_id))
            if row is None:
                # Dry-run rows have synthetic UUIDs; fall back to searching by UUID object.
                row = next((r for r in upserted_show_rows if str(r.get("id")) == str(patch.show_id)), None)
            if row is None:
                continue

            existing_external_ids = row.get("external_ids")
            existing_external_ids_map = existing_external_ids if isinstance(existing_external_ids, dict) else {}
            merged_external_ids = _merge_external_ids(existing_external_ids_map, patch.external_ids_update)
            if merged_external_ids == existing_external_ids_map:
                continue

            if dry_run:
                print(f"ENRICH UPDATE show id={patch.show_id} patch_keys=['external_ids']")
                continue

            if db is None:
                raise RuntimeError("Supabase client is not available for enrichment.")
            updated_row = update_show(db, patch.show_id, {"external_ids": merged_external_ids})
            print(f"ENRICH UPDATED show id={updated_row.get('id')} title={updated_row.get('title')!r}")

    return ShowImportResult(created=created, updated=updated, skipped=skipped, upserted_show_rows=upserted_show_rows)


def parse_imdb_headers_json_env() -> dict[str, str] | None:
    raw = (os.getenv("IMDB_EXTRA_HEADERS_JSON") or "").strip()
    if not raw:
        return None
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("IMDB_EXTRA_HEADERS_JSON must be valid JSON object.") from exc
    if not isinstance(value, dict):
        raise ValueError("IMDB_EXTRA_HEADERS_JSON must be a JSON object.")
    headers: dict[str, str] = {}
    for k, v in value.items():
        if v is None:
            continue
        headers[str(k)] = str(v)
    return headers
