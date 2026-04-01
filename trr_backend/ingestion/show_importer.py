from __future__ import annotations

import json
import os
import re
import sys
import time
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import requests

from trr_backend.db.admin import create_supabase_admin_client
from trr_backend.db.session import DbSession
from trr_backend.ingestion.show_metadata_enricher import enrich_shows_after_upsert
from trr_backend.ingestion.showinfo_overrides import (
    ShowInfoOverridesError,
    ShowOverrideIndex,
    fetch_showinfo_overrides,
)
from trr_backend.ingestion.shows_from_lists import (
    CandidateShow,
    ImdbListItem,
    TmdbListItem,
    fetch_imdb_list_items,
    fetch_tmdb_list_items,
    merge_candidates,
    parse_imdb_list_id,
)
from trr_backend.ingestion.tmdb_show_backfill import resolve_tmdb_id_from_find_payload
from trr_backend.integrations.imdb.episodic_client import IMDB_JOB_CATEGORY_SELF, HttpImdbEpisodicClient
from trr_backend.integrations.imdb.fullcredits_cast_parser import (
    HttpImdbFullCreditsClient,
    extract_person_images_from_graphql,
    fetch_fullcredits_cast_with_fallback,
    filter_self_cast_rows,
    normalize_graphql_credits_to_cast_rows,
    parse_fullcredits_html,
)
from trr_backend.integrations.imdb.graphql_operations import (
    fetch_title_credits_paginated_v2,
    select_show_cast_from_graphql,
)
from trr_backend.integrations.imdb.graphql_persisted_client import ImdbGraphQLPersistedClient
from trr_backend.integrations.imdb.title_metadata_client import (
    HttpImdbTitleMetadataClient,
    parse_imdb_episodes_page,
    parse_imdb_episodes_payload,
    parse_imdb_season_episodes_page,
    parse_imdb_season_episodes_payload,
)
from trr_backend.integrations.tmdb.client import (
    TmdbClientError,
    fetch_tv_details,
    fetch_tv_images,
    fetch_tv_season_details,
    find_by_imdb_id,
    resolve_api_key,
)
from trr_backend.models.shows import ShowRecord, ShowUpsert

# Child table functions removed - data now written directly to core.shows array columns
# from trr_backend.repositories.show_child_tables import (...)
from trr_backend.repositories.credits import assert_core_credits_table_exists, insert_credits_ignore_conflicts
from trr_backend.repositories.episodes import (
    assert_core_episodes_table_exists,
    delete_episodes_for_show,
    delete_episodes_for_tmdb_series,
    fetch_episodes_for_show_season,
    upsert_episodes,
)
from trr_backend.repositories.people import (
    assert_core_people_table_exists,
    fetch_people_by_imdb_ids,
    insert_people,
)
from trr_backend.repositories.person_images import upsert_person_images
from trr_backend.repositories.season_images import (
    assert_core_season_images_table_exists,
    delete_tmdb_season_images,
    upsert_season_images,
)
from trr_backend.repositories.seasons import (
    assert_core_seasons_table_exists,
    delete_seasons_for_tmdb_series,
    fetch_seasons_by_show,
    upsert_seasons,
)
from trr_backend.repositories.show_images import (
    assert_core_show_images_table_exists,
    delete_tmdb_show_images,
    upsert_show_images,
)
from trr_backend.repositories.shows import (
    assert_core_shows_table_exists,
    find_show_by_imdb_id,
    find_show_by_tmdb_id,
    insert_show,
    merge_shows,
    update_show,
)

TMDB_IMAGE_BASE_URL = "https://image.tmdb.org/t/p/original"


@dataclass(frozen=True)
class ShowImportResult:
    created: int
    updated: int
    skipped: int
    upserted_show_rows: list[dict[str, Any]]


IMDB_CAST_DEFAULT_MIN_EPISODES = 4
_IMDB_NAME_ID_RE = re.compile(r"(nm[0-9]+)", re.IGNORECASE)
_IMDB_TITLE_ID_RE = re.compile(r"^tt[0-9]+$", re.IGNORECASE)
_SHOW_TYPED_EXTERNAL_ID_KEYS = ("tvdb_id", "tvrage_id", "wikidata_id")
_SHOW_SOCIAL_EXTERNAL_ID_KEYS = ("facebook_id", "instagram_id", "twitter_id")
_IMDB_ALLOWLISTED_CREW_CATEGORIES = (
    "Producers",
    "Editors",
    "Casting Director",
    "Casting Department",
    "Visual Effects",
    "Production Design",
    "Editorial Department",
    "Production Department",
)


def _dedupe_preserve_order(values: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        if value in seen:
            continue
        out.append(value)
        seen.add(value)
    return out


def _merge_external_ids(existing: Any, updates: Mapping[str, Any]) -> dict[str, Any] | None:
    if not updates:
        return None
    existing_map = existing if isinstance(existing, Mapping) else {}
    merged: dict[str, Any] = dict(existing_map)
    changed = False
    for key, value in updates.items():
        if value is None:
            continue
        if isinstance(value, str):
            value = value.strip()
            if not value:
                continue
        if merged.get(key) != value:
            merged[key] = value
            changed = True
    return merged if changed else None


def _build_show_external_ids_updates(
    *,
    resolved_imdb_id: str | None,
    tmdb_id: int | None,
    tmdb_external_ids: Mapping[str, Any] | None,
) -> dict[str, Any]:
    updates: dict[str, Any] = {"imdb": resolved_imdb_id, "tmdb": tmdb_id}
    if tmdb_external_ids:
        for key in (*_SHOW_TYPED_EXTERNAL_ID_KEYS, *_SHOW_SOCIAL_EXTERNAL_ID_KEYS):
            updates[key] = tmdb_external_ids.get(key)
    return updates


def _fetch_tmdb_find_payload(
    imdb_id: str,
    *,
    api_key: str | None,
    session: requests.Session,
    cache: dict[str, dict[str, Any] | None],
    label: str,
) -> dict[str, Any] | None:
    if not imdb_id:
        return None
    if imdb_id in cache:
        return cache[imdb_id]
    if not api_key:
        cache[imdb_id] = None
        return None
    try:
        payload = find_by_imdb_id(imdb_id, api_key=api_key, session=session)
    except TmdbClientError as exc:
        print(
            f"TMDb find: failed imdb_id={imdb_id} (HTTP {exc.status_code}) for {label}",
            file=sys.stderr,
        )
        payload = None
    except Exception as exc:  # noqa: BLE001
        print(
            f"TMDb find: failed imdb_id={imdb_id} (unexpected error={exc}) for {label}",
            file=sys.stderr,
        )
        payload = None
    cache[imdb_id] = payload
    return payload


def _extract_tmdb_person_id(payload: Mapping[str, Any] | None) -> int | None:
    if not isinstance(payload, Mapping):
        return None
    results = payload.get("person_results")
    if not isinstance(results, list):
        return None
    for item in results:
        if not isinstance(item, Mapping):
            continue
        tmdb_id = item.get("id")
        if isinstance(tmdb_id, int):
            return tmdb_id
    return None


def _extract_tmdb_episode_id(payload: Mapping[str, Any] | None) -> int | None:
    if not isinstance(payload, Mapping):
        return None
    results = payload.get("tv_episode_results")
    if not isinstance(results, list):
        return None
    for item in results:
        if not isinstance(item, Mapping):
            continue
        tmdb_id = item.get("id")
        if isinstance(tmdb_id, int):
            return tmdb_id
    return None


def _merge_str_arrays(existing: Sequence[Any] | None, incoming: Sequence[Any] | None) -> list[str] | None:
    if not incoming:
        return None
    existing_values = [str(v).strip() for v in (existing or []) if isinstance(v, str) and str(v).strip()]
    incoming_values = [str(v).strip() for v in incoming if isinstance(v, str) and str(v).strip()]
    merged = sorted(set(existing_values) | set(incoming_values))
    if not merged:
        return None
    if merged == sorted(existing_values):
        return None
    return merged


def _merge_int_arrays(existing: Sequence[Any] | None, incoming: Sequence[Any] | None) -> list[int] | None:
    if not incoming:
        return None
    existing_values = [_coerce_int(v) for v in (existing or [])]
    incoming_values = [_coerce_int(v) for v in incoming]
    merged = sorted({v for v in existing_values + incoming_values if v is not None})
    if not merged:
        return None
    if merged == sorted({v for v in existing_values if v is not None}):
        return None
    return merged


def _filter_ascii_strings(values: Sequence[Any] | None) -> list[str]:
    out: list[str] = []
    for v in values or []:
        if not isinstance(v, str):
            continue
        s = v.strip()
        if not s:
            continue
        if s.isascii():
            out.append(s)
    return out


def _apply_patch_if_changed(
    patch: dict[str, Any],
    *,
    existing: Mapping[str, Any],
    updates: Mapping[str, Any],
) -> None:
    for key, value in updates.items():
        if value is None:
            continue
        if existing.get(key) != value:
            patch[key] = value


def _coerce_str_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        stripped = value.strip()
        return [stripped] if stripped else []
    if isinstance(value, Mapping):
        for key in ("name", "displayName", "nameText", "text"):
            candidate = value.get(key)
            if isinstance(candidate, str) and candidate.strip():
                return [candidate.strip()]
        return []
    if isinstance(value, Sequence):
        items: list[str] = []
        for item in value:
            items.extend(_coerce_str_list(item))
        return items
    return []


def _extract_imdb_name_id(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        match = _IMDB_NAME_ID_RE.search(value)
        if match:
            return match.group(1).lower()
        return None
    if isinstance(value, Mapping):
        for key in ("id", "nameId", "name_id", "personId", "person_id"):
            found = _extract_imdb_name_id(value.get(key))
            if found:
                return found
    return None


def _empty_showinfo_overrides() -> ShowOverrideIndex:
    return ShowOverrideIndex(by_imdb_id={}, by_tmdb_id={}, by_title_key={})


def _load_showinfo_overrides(*, url: str | None, session: requests.Session) -> ShowOverrideIndex:
    try:
        return fetch_showinfo_overrides(url=url, session=session)
    except ShowInfoOverridesError as exc:
        print(f"ShowInfo overrides: failed to load ({exc}); using defaults.", file=sys.stderr)
        return _empty_showinfo_overrides()
    except Exception as exc:
        print(f"ShowInfo overrides: unexpected error ({exc}); using defaults.", file=sys.stderr)
        return _empty_showinfo_overrides()


def _ingest_imdb_cast(
    *,
    db: DbSession | None,
    show_rows: list[dict[str, Any]],
    dry_run: bool,
    refresh_cast: bool,
    imdb_sleep_ms: int,
    overrides_url: str | None,
    default_min_episodes: int,
) -> None:
    if db is None or dry_run:
        print("IMDb cast: skipped (dry_run)", file=sys.stderr)
        return

    assert_core_people_table_exists(db)
    assert_core_credits_table_exists(db)

    overrides_session = requests.Session()
    overrides = _load_showinfo_overrides(url=overrides_url, session=overrides_session)

    people_cache: dict[str, str] = {}
    extra_headers = parse_imdb_headers_json_env() or {}
    tmdb_find_api_key = resolve_api_key()
    tmdb_find_session = requests.Session()
    tmdb_find_cache: dict[str, dict[str, Any] | None] = {}

    total_shows = len(show_rows)
    processed = 0
    skipped_no_imdb = 0
    skipped_override = 0
    failed_credits = 0
    total_memberships = 0

    for idx, row in enumerate(show_rows, start=1):
        row_id = row.get("id")
        show_id = str(row_id) if row_id is not None else ""
        if not show_id:
            continue

        imdb_id_value = row.get("imdb_id") if isinstance(row.get("imdb_id"), str) else None
        imdb_id = imdb_id_value.strip().lower() if isinstance(imdb_id_value, str) else ""
        if not imdb_id or not _IMDB_TITLE_ID_RE.match(imdb_id):
            skipped_no_imdb += 1
            print(f"IMDb cast: skipping show_id={show_id} (missing imdb_id)", file=sys.stderr)
            continue

        tmdb_id = _coerce_int(row.get("tmdb_id"))
        show_name = row.get("name") if isinstance(row.get("name"), str) else None
        # `core.shows.network` was dropped in favor of `networks[]` (Phase 6d).
        show_network = row.get("network") if isinstance(row.get("network"), str) else None
        if not show_network:
            networks_value = row.get("networks")
            if isinstance(networks_value, list) and networks_value:
                first_network = networks_value[0]
                if isinstance(first_network, str) and first_network.strip():
                    show_network = first_network.strip()

        override = overrides.lookup(imdb_id=imdb_id, tmdb_id=tmdb_id, title=show_name, network=show_network)
        if override and override.skip:
            skipped_override += 1
            print(f"IMDb cast: skipping show_id={show_id} imdb_id={imdb_id} (override=SKIP)", file=sys.stderr)
            continue

        min_episodes = default_min_episodes
        if override and override.min_episodes is not None:
            min_episodes = int(override.min_episodes)

        cast_rows = []
        source_type = "fullcredits_html"
        person_images: list[dict[str, Any]] = []
        crew_rows: list[Any] = []
        html_fullcredits_loaded = False
        try:
            if imdb_sleep_ms:
                time.sleep(imdb_sleep_ms / 1000.0)
            graphql_client = ImdbGraphQLPersistedClient(extra_headers=extra_headers)
            edges = fetch_title_credits_paginated_v2(imdb_id, client=graphql_client)
            filtered_edges, is_partial = select_show_cast_from_graphql(
                edges,
                min_episodes_without_photo=min_episodes,
            )
            cast_rows = normalize_graphql_credits_to_cast_rows(filtered_edges)
            person_images = extract_person_images_from_graphql(filtered_edges)
            source_type = "credits_graphql_paginated_partial" if is_partial else "credits_graphql_paginated"
            if is_partial:
                print(
                    f"IMDb cast: partial GraphQL results show_id={show_id} imdb_id={imdb_id}",
                    file=sys.stderr,
                )
        except Exception as exc:  # noqa: BLE001
            print(f"IMDb cast: GraphQL failed imdb_id={imdb_id} error={exc}", file=sys.stderr)
            try:
                cast_rows, source_type, person_images = fetch_fullcredits_cast_with_fallback(
                    imdb_id,
                    extra_headers=extra_headers,
                    primary_source="html",
                    verbose=False,
                )
            except Exception as fallback_exc:  # noqa: BLE001
                failed_credits += 1
                print(
                    f"IMDb cast: failed imdb_id={imdb_id} (fallback error={fallback_exc})",
                    file=sys.stderr,
                )
                continue

        try:
            html_client = HttpImdbFullCreditsClient(extra_headers=extra_headers)
            html = html_client.fetch_fullcredits_page(imdb_id, verbose=False)
            parsed_fullcredits = parse_fullcredits_html(html, series_id=imdb_id)
            html_self_rows = filter_self_cast_rows(parsed_fullcredits.cast_rows)
            if html_self_rows:
                cast_rows = html_self_rows
                source_type = "fullcredits_html"
            crew_rows = parsed_fullcredits.crew_rows
            html_fullcredits_loaded = True
        except Exception as html_exc:  # noqa: BLE001
            print(
                f"IMDb cast: fullcredits HTML unavailable imdb_id={imdb_id} error={html_exc}",
                file=sys.stderr,
            )

        if not cast_rows:
            failed_credits += 1
            print(
                f"IMDb cast: no eligible credits show_id={show_id} imdb_id={imdb_id} min_episodes={min_episodes}",
                file=sys.stderr,
            )
            continue

        cast_rows = filter_self_cast_rows(cast_rows)
        if not cast_rows:
            failed_credits += 1
            print(
                f"IMDb cast: no eligible self-cast show_id={show_id} imdb_id={imdb_id} min_episodes={min_episodes}",
                file=sys.stderr,
            )
            continue

        credit_people_rows = [*cast_rows, *crew_rows]
        missing_ids = [row.name_id for row in credit_people_rows if row.name_id not in people_cache]
        if missing_ids:
            existing_people = fetch_people_by_imdb_ids(db, missing_ids)
            existing_updates: list[tuple[str, dict[str, Any]]] = []
            for person in existing_people:
                ext_ids = person.get("external_ids")
                ext_map = ext_ids if isinstance(ext_ids, Mapping) else {}
                person_imdb = ext_map.get("imdb")
                if isinstance(person_imdb, str) and person_imdb.strip():
                    people_cache[person_imdb.strip().lower()] = str(person.get("id"))
                    if not ext_map.get("tmdb"):
                        payload = _fetch_tmdb_find_payload(
                            person_imdb.strip(),
                            api_key=tmdb_find_api_key,
                            session=tmdb_find_session,
                            cache=tmdb_find_cache,
                            label="person",
                        )
                        tmdb_person_id = _extract_tmdb_person_id(payload)
                        merged_external_ids = _merge_external_ids(ext_map, {"tmdb": tmdb_person_id})
                        if merged_external_ids is not None:
                            person_id = person.get("id")
                            if isinstance(person_id, str) and person_id:
                                existing_updates.append((person_id, merged_external_ids))
            for person_id, merged_external_ids in existing_updates:
                try:
                    db.schema("core").table("people").update({"external_ids": merged_external_ids}).eq(
                        "id", person_id
                    ).execute()
                except Exception as exc:  # noqa: BLE001
                    print(
                        f"IMDb cast: failed to update person external_ids id={person_id} error={exc}",
                        file=sys.stderr,
                    )

        new_people_rows: list[dict[str, Any]] = []
        for row in credit_people_rows:
            imdb_person_id = row.name_id.strip().lower()
            if not imdb_person_id or imdb_person_id in people_cache:
                continue
            if not row.name:
                continue
            payload = _fetch_tmdb_find_payload(
                imdb_person_id,
                api_key=tmdb_find_api_key,
                session=tmdb_find_session,
                cache=tmdb_find_cache,
                label="person",
            )
            tmdb_person_id = _extract_tmdb_person_id(payload)
            external_ids = _merge_external_ids({}, {"imdb": imdb_person_id, "tmdb": tmdb_person_id}) or {
                "imdb": imdb_person_id
            }
            new_people_rows.append(
                {
                    "full_name": row.name,
                    "known_for": "cast",
                    "external_ids": external_ids,
                }
            )

        if new_people_rows:
            inserted_people = insert_people(db, new_people_rows)
            for person in inserted_people:
                ext_ids = person.get("external_ids")
                ext_map = ext_ids if isinstance(ext_ids, Mapping) else {}
                person_imdb = ext_map.get("imdb")
                if isinstance(person_imdb, str) and person_imdb.strip():
                    people_cache[person_imdb.strip().lower()] = str(person.get("id"))

        credit_rows: list[dict[str, Any]] = []
        for row in cast_rows:
            person_id = people_cache.get(row.name_id.strip().lower())
            if not person_id:
                continue
            credit_rows.append(
                {
                    "show_id": show_id,
                    "person_id": person_id,
                    "billing_order": row.billing_order,
                    "role": row.raw_role_text,
                    "credit_category": "Self",
                    "source_type": source_type,
                    "metadata": {
                        "imdb_name_id": row.name_id,
                        "episode_count": row.episode_count,
                        "episodes_label": row.episodes_label,
                        "years_label": row.years_label,
                    },
                }
            )

        if credit_rows:
            if refresh_cast:
                try:
                    delete_resp = (
                        db.schema("core")
                        .table("credits")
                        .delete()
                        .eq("show_id", show_id)
                        .eq("credit_category", "Self")
                        .not_.eq("source_type", "manual")
                        .execute()
                    )
                    if hasattr(delete_resp, "error") and delete_resp.error:
                        raise RuntimeError(str(delete_resp.error))
                except Exception as exc:  # noqa: BLE001
                    print(
                        f"IMDb cast: WARNING failed to delete existing self credits show_id={show_id} error={exc}",
                        file=sys.stderr,
                    )
            insert_credits_ignore_conflicts(db, credit_rows)
            total_memberships += len(credit_rows)

        if html_fullcredits_loaded:
            crew_credit_rows: list[dict[str, Any]] = []
            for row in crew_rows:
                if row.credit_category not in _IMDB_ALLOWLISTED_CREW_CATEGORIES:
                    continue
                person_id = people_cache.get(row.name_id.strip().lower())
                if not person_id:
                    continue
                crew_credit_rows.append(
                    {
                        "show_id": show_id,
                        "person_id": person_id,
                        "billing_order": row.display_order,
                        "role": row.role,
                        "credit_category": row.credit_category,
                        "source_type": "fullcredits_html",
                        "metadata": {
                            "imdb_name_id": row.name_id,
                            "episode_count": row.episode_count,
                            "episodes_label": row.episodes_label,
                            "years_label": row.years_label,
                            "source_page_url": f"https://www.imdb.com/title/{imdb_id}/fullcredits/",
                            "display_order": row.display_order,
                        },
                    }
                )

            if refresh_cast:
                try:
                    delete_resp = (
                        db.schema("core")
                        .table("credits")
                        .delete()
                        .eq("show_id", show_id)
                        .in_("credit_category", list(_IMDB_ALLOWLISTED_CREW_CATEGORIES))
                        .not_.eq("source_type", "manual")
                        .execute()
                    )
                    if hasattr(delete_resp, "error") and delete_resp.error:
                        raise RuntimeError(str(delete_resp.error))
                except Exception as exc:  # noqa: BLE001
                    print(
                        f"IMDb cast: WARNING failed to delete existing crew credits show_id={show_id} error={exc}",
                        file=sys.stderr,
                    )

            if crew_credit_rows:
                insert_credits_ignore_conflicts(db, crew_credit_rows)

        if person_images:
            try:
                upsert_person_images(db, person_images, verbose=False)
            except Exception as exc:  # noqa: BLE001
                print(f"IMDb cast: person_images upsert failed imdb_id={imdb_id} error={exc}", file=sys.stderr)

        processed += 1
        if idx == 1 or idx % 10 == 0 or idx == total_shows:
            print(
                f"IMDb cast: processed {idx}/{total_shows} "
                f"(memberships={total_memberships} skipped_no_imdb={skipped_no_imdb} "
                f"skipped_override={skipped_override} failed={failed_credits})",
                file=sys.stderr,
            )

    if total_shows:
        print(
            "IMDb cast summary "
            f"processed={processed} memberships={total_memberships} skipped_no_imdb={skipped_no_imdb} "
            f"skipped_override={skipped_override} failed={failed_credits}",
            file=sys.stderr,
        )


def _candidate_to_show_upsert(
    candidate: CandidateShow,
    *,
    resolved_imdb_id: str | None,
) -> ShowUpsert:
    tmdb_id_column = int(candidate.tmdb_id) if candidate.tmdb_id is not None else None

    # Extract listed_on from source_tags
    listed_on: list[str] = []
    for tag in candidate.source_tags or set():
        if tag.startswith("imdb-list:") and "imdb" not in listed_on:
            listed_on.append("imdb")
        elif tag.startswith("tmdb-list:") and "tmdb" not in listed_on:
            listed_on.append("tmdb")

    # Extract genres from imdb_meta if available
    genres: list[str] | None = None
    imdb_meta = candidate.imdb_meta or {}
    if imdb_meta.get("genres"):
        genres = list(imdb_meta["genres"])

    return ShowUpsert(
        name=candidate.title,
        tmdb_id=tmdb_id_column,
        imdb_id=resolved_imdb_id,
        premiere_date=candidate.first_air_date,
        description=None,
        listed_on=sorted(listed_on) if listed_on else None,
        genres=genres,
    )


def _imdb_meta_from_list_item(item: ImdbListItem) -> dict[str, Any]:
    meta: dict[str, Any] = {}
    meta["title"] = item.title
    meta["imdb_url"] = f"https://www.imdb.com/title/{item.imdb_id}/"
    if item.description:
        meta["description"] = item.description
    if item.imdb_rating is not None:
        meta["rating_value"] = float(item.imdb_rating)
    if item.imdb_vote_count is not None:
        meta["rating_count"] = int(item.imdb_vote_count)
    if item.end_year is not None:
        meta["end_year"] = int(item.end_year)
    if item.episodes_total is not None:
        meta["total_episodes"] = int(item.episodes_total)
    if item.primary_image_url:
        meta["poster_image_url"] = item.primary_image_url
    if item.primary_image_caption:
        meta["poster_image_caption"] = item.primary_image_caption
    if item.certificate:
        meta["content_rating"] = item.certificate
    if item.runtime_seconds is not None:
        meta["runtime_minutes"] = int(item.runtime_seconds // 60)
    if item.genres:
        meta["genres"] = list(item.genres)
    return meta


def _merge_meta(existing: Mapping[str, Any] | None, incoming: Mapping[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = dict(existing) if isinstance(existing, Mapping) else {}
    for key, value in incoming.items():
        if key not in merged or merged[key] is None:
            merged[key] = value
    return merged


def _coerce_float(value: Any) -> float | None:
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


def _build_imdb_show_patch_from_meta(
    meta: Mapping[str, Any],
    *,
    fallback_title: str | None,
    fetched_at: str,
) -> dict[str, Any]:
    patch: dict[str, Any] = {}
    if not meta:
        return patch

    patch["imdb_meta"] = dict(meta)
    patch["imdb_fetched_at"] = fetched_at

    title = meta.get("title") or fallback_title
    if isinstance(title, str) and title.strip():
        patch["imdb_title"] = title.strip()

    content_rating = meta.get("content_rating")
    if isinstance(content_rating, str) and content_rating.strip():
        patch["imdb_content_rating"] = content_rating.strip()

    rating_value = _coerce_float(meta.get("rating_value") or meta.get("aggregate_rating_value"))
    if rating_value is not None:
        patch["imdb_rating_value"] = rating_value

    rating_count = _coerce_int(meta.get("rating_count") or meta.get("aggregate_rating_count"))
    if rating_count is not None:
        patch["imdb_rating_count"] = rating_count

    date_published = meta.get("date_published")
    if isinstance(date_published, str) and date_published.strip():
        patch["imdb_date_published"] = date_published.strip()

    end_year = _coerce_int(meta.get("end_year"))
    if end_year is not None:
        patch["imdb_end_year"] = end_year

    return patch


def _extract_tmdb_networks(details: Mapping[str, Any]) -> tuple[list[str], list[int]]:
    networks = details.get("networks")
    if not isinstance(networks, list):
        return [], []
    names: list[str] = []
    ids: list[int] = []
    for item in networks:
        if not isinstance(item, Mapping):
            continue
        name = item.get("name")
        if isinstance(name, str) and name.strip():
            names.append(name.strip())
        network_id = _coerce_int(item.get("id"))
        if network_id is not None:
            ids.append(network_id)
    return names, ids


def _extract_tmdb_production_company_ids(details: Mapping[str, Any]) -> list[int]:
    companies = details.get("production_companies")
    if not isinstance(companies, list):
        return []
    ids: list[int] = []
    for item in companies:
        if not isinstance(item, Mapping):
            continue
        company_id = _coerce_int(item.get("id"))
        if company_id is not None:
            ids.append(company_id)
    return ids


def _build_tmdb_show_patch(details: Mapping[str, Any], *, fetched_at: str) -> dict[str, Any]:
    meta = _tmdb_meta_from_tv_details(details)
    patch: dict[str, Any] = {
        "tmdb_meta": meta,
        "tmdb_fetched_at": fetched_at,
    }

    name = details.get("name")
    if isinstance(name, str) and name.strip():
        patch["tmdb_name"] = name.strip()

    status = details.get("status")
    if isinstance(status, str) and status.strip():
        patch["tmdb_status"] = status.strip()

    series_type = details.get("type")
    if isinstance(series_type, str) and series_type.strip():
        patch["tmdb_type"] = series_type.strip()

    first_air = details.get("first_air_date")
    if isinstance(first_air, str) and first_air.strip():
        patch["tmdb_first_air_date"] = first_air.strip()

    last_air = details.get("last_air_date")
    if isinstance(last_air, str) and last_air.strip():
        patch["tmdb_last_air_date"] = last_air.strip()

    vote_average = _coerce_float(details.get("vote_average"))
    if vote_average is not None:
        patch["tmdb_vote_average"] = vote_average

    vote_count = _coerce_int(details.get("vote_count"))
    if vote_count is not None:
        patch["tmdb_vote_count"] = vote_count

    popularity = _coerce_float(details.get("popularity"))
    if popularity is not None:
        patch["tmdb_popularity"] = popularity

    return patch


def _tmdb_external_ids_from_tv_details(details: Mapping[str, Any], *, tmdb_id: int) -> dict[str, Any] | None:
    external_ids = details.get("external_ids")
    if not isinstance(external_ids, Mapping):
        return None
    return {
        "tmdb_id": int(tmdb_id),
        "imdb_id": external_ids.get("imdb_id"),
        "tvdb_id": external_ids.get("tvdb_id"),
        "tvrage_id": external_ids.get("tvrage_id"),
        "wikidata_id": external_ids.get("wikidata_id"),
        "facebook_id": external_ids.get("facebook_id"),
        "instagram_id": external_ids.get("instagram_id"),
        "twitter_id": external_ids.get("twitter_id"),
    }


def _tmdb_details_is_fresh(
    show_row: Mapping[str, Any] | None,
    *,
    max_age_days: int,
    now: datetime,
) -> bool:
    if not isinstance(show_row, Mapping):
        return False
    fetched_at_raw = show_row.get("tmdb_fetched_at")
    fetched_at = _parse_iso8601_utc(fetched_at_raw)
    if fetched_at is None:
        return False
    if max_age_days <= 0:
        return False
    age = now - fetched_at
    return age.total_seconds() <= (max_age_days * 86400)


def _tmdb_meta_from_tv_details(details: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(details, Mapping):
        return {}
    return dict(details)


def _is_english_iso_639_1(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    raw = value.strip().casefold()
    return raw.startswith("en")


def _tmdb_image_sort_key(image: Mapping[str, Any]) -> tuple[int, int, float, str]:
    iso = image.get("iso_639_1")
    if _is_english_iso_639_1(iso):
        bucket = 0
    elif iso is None or (isinstance(iso, str) and not iso.strip()):
        bucket = 1
    else:
        bucket = 2

    # vote data is now in image["raw"]["vote_count"] and image["raw"]["vote_average"]
    raw = image.get("raw") if isinstance(image.get("raw"), Mapping) else {}
    vote_count = raw.get("vote_count")
    vote_count_int = int(vote_count) if isinstance(vote_count, int) else 0
    vote_average = raw.get("vote_average")
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
            # vote_average and vote_count are stored in metadata, not as columns
            "raw": dict(item),
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
    tmdb_id: int,
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
            file_path = img.get("file_path")
            width = img.get("width")
            width_int = int(width) if isinstance(width, int) else 0
            height = img.get("height")
            height_int = int(height) if isinstance(height, int) else 0
            aspect_ratio = img.get("aspect_ratio")
            if isinstance(aspect_ratio, (int, float)):
                aspect_ratio_val: float = float(aspect_ratio)
            elif height_int > 0 and width_int > 0:
                aspect_ratio_val = float(width_int) / float(height_int)
            else:
                aspect_ratio_val = 0.0

            rows.append(
                {
                    "show_id": show_id,
                    "tmdb_id": int(tmdb_id),
                    "source": source,
                    "source_image_id": file_path,
                    "kind": kind,
                    "iso_639_1": img.get("iso_639_1"),
                    "file_path": file_path,
                    "url_path": file_path,
                    "url": f"{TMDB_IMAGE_BASE_URL}{file_path}" if isinstance(file_path, str) else None,
                    "image_type": kind,
                    "caption": None,
                    "position": None,
                    "width": width_int,
                    "height": height_int,
                    "aspect_ratio": aspect_ratio_val,
                    "fetch_method": "tmdb_images_api",
                    "metadata": img.get("raw") if isinstance(img.get("raw"), Mapping) else None,
                    "fetched_at": fetched_at,
                    "updated_at": fetched_at,
                }
            )

    return rows, primary


def _now_utc_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _coerce_int(value: Any) -> int | None:
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        raw = value.strip()
        if raw.isdigit():
            return int(raw)
    return None


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
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def _tmdb_meta_is_fresh(
    tmdb_meta: Mapping[str, Any],
    *,
    tmdb_id: int,
    language: str,
    max_age_days: int,
    now: datetime,
) -> bool:
    """
    Return True if an existing `core.shows.tmdb_meta` payload is usable without refetching.
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
    imdb_extra_headers: Mapping[str, str] | None = None,
) -> list[CandidateShow]:
    session = http_session
    tmdb_session = session if isinstance(session, requests.Session) else requests.Session()

    imdb_candidates: list[CandidateShow] = []
    for url in imdb_list_urls:
        list_id = parse_imdb_list_id(url)
        items: list[ImdbListItem] = fetch_imdb_list_items(
            url,
            session=session,
            use_graphql=bool(imdb_use_graphql),
            extra_headers=imdb_extra_headers,
        )
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
    tmdb_refresh_images: bool = False,
    imdb_fetch_episodes: bool = False,
    imdb_refresh_episodes: bool = False,
    imdb_fetch_cast: bool = False,
    imdb_refresh_cast: bool = False,
    imdb_cast_overrides_url: str | None = None,
    imdb_cast_default_min_episodes: int = IMDB_CAST_DEFAULT_MIN_EPISODES,
    tmdb_fetch_seasons: bool = False,
    tmdb_refresh_seasons: bool = False,
    enrich_show_metadata: bool = False,
    enrich_region: str = "US",
    enrich_concurrency: int = 5,
    enrich_max_enrich: int | None = None,
    enrich_force_refresh: bool = False,
    enrich_imdb_sleep_ms: int = 0,
    supabase_client: DbSession | None = None,
    imdb_episodic_probe_name_id: str | None = None,
    imdb_episodic_probe_job_category_id: str = IMDB_JOB_CATEGORY_SELF,
    imdb_episodic_extra_headers: Mapping[str, str] | None = None,
) -> ShowImportResult:
    candidates_list = list(candidates)

    db = supabase_client or (None if dry_run else create_supabase_admin_client())
    if db is not None:
        assert_core_shows_table_exists(db)

    if annotate_imdb_episodic and imdb_episodic_probe_name_id:
        annotate_candidates_imdb_episodic(
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
    now = datetime.now(UTC)
    tmdb_find_api_key = resolve_api_key()
    tmdb_find_session = requests.Session()
    tmdb_find_cache: dict[str, dict[str, Any] | None] = {}

    for idx, candidate in enumerate(candidates_list, start=1):  # noqa: B007
        tmdb_id = int(candidate.tmdb_id) if candidate.tmdb_id is not None else None
        resolved_imdb_id = candidate.imdb_id.strip() if isinstance(candidate.imdb_id, str) else None

        existing_by_imdb: dict[str, Any] | None = None
        existing_by_tmdb: dict[str, Any] | None = None
        if db is not None:
            if resolved_imdb_id:
                existing_by_imdb = find_show_by_imdb_id(db, resolved_imdb_id)
            if tmdb_id is not None:
                existing_by_tmdb = find_show_by_tmdb_id(db, tmdb_id)
                if existing_by_tmdb and not resolved_imdb_id:
                    imdb_from_row = existing_by_tmdb.get("imdb_id")
                    if isinstance(imdb_from_row, str) and imdb_from_row.strip():
                        resolved_imdb_id = imdb_from_row.strip()
                        if existing_by_imdb is None:
                            existing_by_imdb = find_show_by_imdb_id(db, resolved_imdb_id)

        if tmdb_id is None and resolved_imdb_id:
            payload = _fetch_tmdb_find_payload(
                resolved_imdb_id,
                api_key=tmdb_find_api_key,
                session=tmdb_find_session,
                cache=tmdb_find_cache,
                label="show",
            )
            resolved_tmdb_id, _reason = resolve_tmdb_id_from_find_payload(
                payload or {},
                show_name=candidate.title,
                premiere_date=candidate.first_air_date,
            )
            if resolved_tmdb_id is not None:
                tmdb_id = int(resolved_tmdb_id)
                candidate.tmdb_id = tmdb_id
                if db is not None and existing_by_tmdb is None:
                    existing_by_tmdb = find_show_by_tmdb_id(db, tmdb_id)

        tmdb_details: dict[str, Any] | None = None
        tmdb_external_ids: dict[str, Any] | None = None
        fetched_at = _now_utc_iso()

        if tmdb_fetch_details and tmdb_id is not None:
            tmdb_details_processed += 1
            should_fetch = True
            if db is not None:
                existing_tmdb_row = existing_by_tmdb or existing_by_imdb
                if _tmdb_details_is_fresh(
                    existing_tmdb_row,
                    max_age_days=int(tmdb_details_max_age_days or 0),
                    now=now,
                ):
                    tmdb_details_skipped_fresh += 1
                    if resolved_imdb_id:
                        should_fetch = False
            if should_fetch:
                try:
                    cache_key = (tmdb_id, tmdb_details_language, tmdb_details_append)
                    if cache_key in tmdb_details_cache:
                        tmdb_details_skipped_cached += 1
                        details = tmdb_details_cache[cache_key]
                    else:
                        details = fetch_tv_details(
                            tmdb_id,
                            api_key=None,
                            session=tmdb_details_session,
                            language=tmdb_details_language,
                            append_to_response=list(tmdb_details_append),
                            cache=tmdb_details_cache,
                        )
                        tmdb_details_fetched += 1

                    tmdb_details = details
                    tmdb_external_ids = _tmdb_external_ids_from_tv_details(details, tmdb_id=tmdb_id)
                    if not resolved_imdb_id and tmdb_external_ids:
                        imdb_from_tmdb = tmdb_external_ids.get("imdb_id")
                        if isinstance(imdb_from_tmdb, str) and imdb_from_tmdb.strip():
                            resolved_imdb_id = imdb_from_tmdb.strip()
                            if db is not None and existing_by_imdb is None:
                                existing_by_imdb = find_show_by_imdb_id(db, resolved_imdb_id)
                except TmdbClientError as exc:
                    tmdb_details_failed += 1
                    status = exc.status_code
                    if status in {404, 422}:
                        print(
                            f"TMDb details: skipping tmdb_id={tmdb_id} (HTTP {status})",
                            file=sys.stderr,
                        )
                    else:
                        print(
                            f"TMDb details: failed tmdb_id={tmdb_id} "
                            f"(HTTP {status if status is not None else 'unknown'})",
                            file=sys.stderr,
                        )
                except Exception:
                    tmdb_details_failed += 1
                    print(f"TMDb details: failed tmdb_id={tmdb_id} (unexpected error)", file=sys.stderr)

            if tmdb_details_total:
                if (
                    tmdb_details_processed == 1
                    or tmdb_details_processed % 10 == 0
                    or tmdb_details_processed == tmdb_details_total
                ):
                    print(
                        f"TMDb details: processed {tmdb_details_processed}/{tmdb_details_total} "
                        f"(fetched={tmdb_details_fetched} "
                        f"skipped_fresh={tmdb_details_skipped_fresh} "
                        f"cached={tmdb_details_skipped_cached} "
                        f"failed={tmdb_details_failed})",
                        file=sys.stderr,
                    )

        tmdb_show_patch: dict[str, Any] = {}
        tmdb_network_names: list[str] = []
        tmdb_network_ids: list[int] = []
        tmdb_production_company_ids: list[int] = []
        if tmdb_details:
            tmdb_show_patch = _build_tmdb_show_patch(tmdb_details, fetched_at=fetched_at)
            tmdb_network_names, tmdb_network_ids = _extract_tmdb_networks(tmdb_details)
            tmdb_production_company_ids = _extract_tmdb_production_company_ids(tmdb_details)

        show_upsert = _candidate_to_show_upsert(
            candidate,
            resolved_imdb_id=resolved_imdb_id,
        )

        existing = existing_by_imdb or existing_by_tmdb
        if existing_by_imdb and existing_by_tmdb:
            if str(existing_by_imdb.get("id")) != str(existing_by_tmdb.get("id")):
                if dry_run:
                    print(
                        f"MERGE show source_id={existing_by_tmdb.get('id')} -> target_id={existing_by_imdb.get('id')}"
                    )
                elif db is not None:
                    merge_shows(
                        db,
                        source_show_id=existing_by_tmdb.get("id"),
                        target_show_id=existing_by_imdb.get("id"),
                    )
                existing = existing_by_imdb

        created_now = False
        if existing is None:
            if dry_run:
                print(f"CREATE show imdb_id={resolved_imdb_id or ''} tmdb_id={tmdb_id or ''} name={candidate.title!r}")
                created += 1
                created_now = True
                upserted_show_rows.append(
                    {
                        "id": str(uuid4()),
                        "name": show_upsert.name,
                        "description": show_upsert.description,
                        "premiere_date": show_upsert.premiere_date,
                        "tmdb_id": show_upsert.tmdb_id,
                        "imdb_id": show_upsert.imdb_id,
                    }
                )
            else:
                inserted = insert_show(db, show_upsert)
                created += 1
                created_now = True
                upserted_show_rows.append(inserted)
                print(f"CREATED show id={inserted.get('id')} name={inserted.get('name')!r}")
            existing = upserted_show_rows[-1]
        else:
            patch: dict[str, Any] = {}
            if resolved_imdb_id and existing.get("imdb_id") != resolved_imdb_id:
                patch["imdb_id"] = resolved_imdb_id
            if tmdb_id is not None and existing.get("tmdb_id") is None:
                patch["tmdb_id"] = tmdb_id
            if not existing.get("premiere_date") and show_upsert.premiere_date:
                patch["premiere_date"] = show_upsert.premiere_date

            # Union listed_on arrays
            if show_upsert.listed_on:
                merged_listed_on = _merge_str_arrays(existing.get("listed_on"), show_upsert.listed_on)
                if merged_listed_on is not None:
                    patch["listed_on"] = merged_listed_on

            # Merge genres if we have new ones
            if show_upsert.genres:
                merged_genres = _merge_str_arrays(existing.get("genres"), show_upsert.genres)
                if merged_genres is not None:
                    patch["genres"] = merged_genres

            if tmdb_network_names:
                merged_networks = _merge_str_arrays(existing.get("networks"), tmdb_network_names)
                if merged_networks is not None:
                    patch["networks"] = merged_networks

            merged_tmdb_network_ids = _merge_int_arrays(existing.get("tmdb_network_ids"), tmdb_network_ids)
            if merged_tmdb_network_ids is not None:
                patch["tmdb_network_ids"] = merged_tmdb_network_ids

            merged_tmdb_company_ids = _merge_int_arrays(
                existing.get("tmdb_production_company_ids"), tmdb_production_company_ids
            )
            if merged_tmdb_company_ids is not None:
                patch["tmdb_production_company_ids"] = merged_tmdb_company_ids

            if candidate.imdb_meta:
                existing_imdb_meta = (
                    existing.get("imdb_meta") if isinstance(existing.get("imdb_meta"), Mapping) else None
                )
                merged_imdb_meta = _merge_meta(existing_imdb_meta, candidate.imdb_meta)
                imdb_patch = _build_imdb_show_patch_from_meta(
                    merged_imdb_meta,
                    fallback_title=candidate.title,
                    fetched_at=fetched_at,
                )
                _apply_patch_if_changed(patch, existing=existing, updates=imdb_patch)

            if tmdb_show_patch:
                _apply_patch_if_changed(patch, existing=existing, updates=tmdb_show_patch)

            # Add typed external IDs from TMDb if available and existing is missing them.
            # Social IDs are no longer top-level columns on core.shows.
            if tmdb_external_ids:
                for ext_key in _SHOW_TYPED_EXTERNAL_ID_KEYS:
                    ext_val = tmdb_external_ids.get(ext_key)
                    if ext_val is not None and existing.get(ext_key) is None:
                        patch[ext_key] = ext_val

            merged_external_ids = _merge_external_ids(
                existing.get("external_ids"),
                _build_show_external_ids_updates(
                    resolved_imdb_id=resolved_imdb_id,
                    tmdb_id=tmdb_id,
                    tmdb_external_ids=tmdb_external_ids,
                ),
            )
            if merged_external_ids is not None:
                patch["external_ids"] = merged_external_ids

            if not patch:
                skipped += 1
                upserted_show_rows.append(existing)
            elif dry_run:
                print(
                    f"UPDATE show id={existing.get('id')} imdb_id={resolved_imdb_id or ''} "
                    f"tmdb_id={tmdb_id or ''} patch_keys={sorted(patch.keys())}"
                )
                updated += 1
                merged_existing = dict(existing)
                merged_existing.update(patch)
                upserted_show_rows.append(merged_existing)
                existing = merged_existing
            else:
                updated_row = update_show(db, existing["id"], patch)
                updated += 1
                upserted_show_rows.append(updated_row)
                existing = updated_row
                print(f"UPDATED show id={updated_row.get('id')} name={updated_row.get('name')!r}")

        if created_now:
            post_patch: dict[str, Any] = {}
            if candidate.imdb_meta:
                imdb_patch = _build_imdb_show_patch_from_meta(
                    candidate.imdb_meta,
                    fallback_title=candidate.title,
                    fetched_at=fetched_at,
                )
                _apply_patch_if_changed(post_patch, existing=existing, updates=imdb_patch)
            if tmdb_show_patch:
                _apply_patch_if_changed(post_patch, existing=existing, updates=tmdb_show_patch)
            if tmdb_network_names:
                post_patch["networks"] = sorted(set(tmdb_network_names))
            if tmdb_network_ids:
                post_patch["tmdb_network_ids"] = sorted(set(tmdb_network_ids))
            if tmdb_production_company_ids:
                post_patch["tmdb_production_company_ids"] = sorted(set(tmdb_production_company_ids))

            merged_external_ids = _merge_external_ids(
                existing.get("external_ids"),
                _build_show_external_ids_updates(
                    resolved_imdb_id=resolved_imdb_id,
                    tmdb_id=tmdb_id,
                    tmdb_external_ids=tmdb_external_ids,
                ),
            )
            if merged_external_ids is not None:
                post_patch["external_ids"] = merged_external_ids

            if post_patch:
                if dry_run:
                    existing = {**existing, **post_patch}
                    upserted_show_rows[-1] = existing
                elif db is not None:
                    updated_row = update_show(db, existing["id"], post_patch)
                    existing = updated_row
                    upserted_show_rows[-1] = updated_row

    # If a show appears in multiple list sources (or TMDb external id resolution is skipped),
    # we can end up with duplicate entries in `upserted_show_rows`. Downstream pipelines
    # (episodes, seasons, images, enrichment) should run once per show UUID.
    if upserted_show_rows:
        by_id: dict[str, dict[str, Any]] = {}
        order: list[str] = []
        for row in upserted_show_rows:
            row_id = row.get("id")
            row_id_str = str(row_id) if row_id is not None else ""
            if not row_id_str:
                continue
            if row_id_str not in by_id:
                order.append(row_id_str)
            by_id[row_id_str] = row

        deduped_rows = [by_id[row_id] for row_id in order if row_id in by_id]
        if len(deduped_rows) != len(upserted_show_rows):
            print(
                f"Deduplicated upserted shows: {len(upserted_show_rows)} -> {len(deduped_rows)}",
                file=sys.stderr,
            )
        upserted_show_rows = deduped_rows

    if tmdb_fetch_details and tmdb_details_total:
        print(
            "TMDb details summary "
            f"tmdb_details_fetched={tmdb_details_fetched} "
            f"tmdb_details_skipped_fresh={tmdb_details_skipped_fresh} "
            f"tmdb_details_skipped_cached={tmdb_details_skipped_cached} "
            f"tmdb_details_failed={tmdb_details_failed}",
            file=sys.stderr,
        )

    if imdb_fetch_cast:
        _ingest_imdb_cast(
            db=db,
            show_rows=upserted_show_rows,
            dry_run=dry_run,
            refresh_cast=imdb_refresh_cast,
            imdb_sleep_ms=enrich_imdb_sleep_ms,
            overrides_url=imdb_cast_overrides_url,
            default_min_episodes=imdb_cast_default_min_episodes,
        )

    # Optional seasons + episodes pipeline:
    # 1) IMDb season-scoped episode enumeration (no credits)
    # 2) TMDb season enrichment (details + external_ids + season posters) (no credits)
    if imdb_fetch_episodes or tmdb_fetch_seasons:
        if db is None or dry_run:
            print("Seasons/Episodes: skipped (dry_run)", file=sys.stderr)
        else:
            assert_core_seasons_table_exists(db)
            assert_core_episodes_table_exists(db)
            if tmdb_fetch_seasons:
                assert_core_season_images_table_exists(db)

            imdb_client = HttpImdbTitleMetadataClient(
                extra_headers=imdb_episodic_extra_headers,
                sleep_ms=enrich_imdb_sleep_ms,
            )
            imdb_payload_cache: dict[tuple[str, int | None], dict[str, Any]] = {}
            imdb_html_cache: dict[tuple[str, int | None], str] = {}

            tmdb_season_language = "en-US"
            tmdb_season_include_lang = "en,null"
            tmdb_season_session = requests.Session()
            tmdb_season_cache: dict[tuple[int, int, str, tuple[str, ...], str], dict[str, Any]] = {}

            total_shows = len(upserted_show_rows)
            for idx, row in enumerate(upserted_show_rows, start=1):
                row_id = row.get("id")
                show_id = str(row_id) if row_id is not None else ""
                if not show_id:
                    continue

                imdb_id_raw = row.get("imdb_id") if isinstance(row.get("imdb_id"), str) else None
                imdb_series_id_str = imdb_id_raw.strip() if isinstance(imdb_id_raw, str) else ""
                imdb_series_id_str = imdb_series_id_str or None

                tmdb_id_int = _coerce_int(row.get("tmdb_id"))

                # Refresh behavior: delete TMDb season/episode/image rows before any ingestion so we don't
                # wipe out freshly imported IMDb fields later in the run.
                if tmdb_fetch_seasons and tmdb_refresh_seasons and tmdb_id_int is not None:
                    delete_tmdb_season_images(db, tmdb_series_id=tmdb_id_int)
                    delete_episodes_for_tmdb_series(db, tmdb_series_id=tmdb_id_int)
                    delete_seasons_for_tmdb_series(db, tmdb_series_id=tmdb_id_int)

                if imdb_fetch_episodes and imdb_refresh_episodes:
                    delete_episodes_for_show(db, show_id=show_id)

                # --- IMDb: enumerate episodes per season (no credits) ---
                if imdb_fetch_episodes:
                    if not imdb_series_id_str:
                        print(f"IMDb episodes: skipping show_id={show_id} (missing imdb_series_id)", file=sys.stderr)
                    else:
                        try:
                            cache_key = (imdb_series_id_str, None)
                            overview: Any
                            payload = imdb_payload_cache.get(cache_key)
                            if payload is None:
                                try:
                                    payload = imdb_client.fetch_episodes_payload(
                                        imdb_series_id_str, allow_html_fallback=False
                                    )
                                    imdb_payload_cache[cache_key] = payload
                                except Exception:  # noqa: BLE001
                                    payload = None

                            if isinstance(payload, Mapping):
                                overview = parse_imdb_episodes_payload(payload)
                            else:
                                if cache_key in imdb_html_cache:
                                    overview_html = imdb_html_cache[cache_key]
                                else:
                                    overview_html = imdb_client.fetch_episodes_page(imdb_series_id_str)
                                    imdb_html_cache[cache_key] = overview_html
                                overview = parse_imdb_episodes_page(overview_html)
                            season_numbers = [int(s) for s in overview.available_seasons if isinstance(s, int)]
                            season_numbers = [s for s in season_numbers if 0 <= s <= 50]
                            if not season_numbers:
                                print(
                                    f"IMDb episodes: no seasons found show_id={show_id} imdb_id={imdb_series_id_str}",
                                    file=sys.stderr,
                                )
                            else:
                                fetched_at = _now_utc_iso()
                                upsert_seasons(
                                    db,
                                    [
                                        {
                                            "show_id": show_id,
                                            "season_number": season_no,
                                            "imdb_series_id": imdb_series_id_str,
                                            "language": "en-US",
                                            "fetched_at": fetched_at,
                                            "external_ids": _merge_external_ids(
                                                {},
                                                {"imdb": imdb_series_id_str},
                                            )
                                            or None,
                                        }
                                        for season_no in season_numbers
                                    ],
                                )
                                season_rows = fetch_seasons_by_show(db, show_id=show_id, season_numbers=season_numbers)
                                season_id_by_number: dict[int, str] = {}
                                for s in season_rows:
                                    sn = s.get("season_number")
                                    sid = s.get("id")
                                    if isinstance(sn, int) and isinstance(sid, str) and sid:
                                        season_id_by_number[sn] = sid

                                seasons_failed = 0
                                episodes_upserted = 0

                                for season_no in season_numbers:
                                    season_id_val = season_id_by_number.get(season_no)
                                    if not season_id_val:
                                        continue
                                    try:
                                        existing_eps = fetch_episodes_for_show_season(
                                            db, show_id=show_id, season_number=int(season_no)
                                        )
                                        existing_by_number: dict[int, dict[str, Any]] = {}
                                        for existing_ep in existing_eps:
                                            ep_no = existing_ep.get("episode_number")
                                            if isinstance(ep_no, int):
                                                existing_by_number[ep_no] = existing_ep

                                        cache_key = (imdb_series_id_str, int(season_no))
                                        payload = imdb_payload_cache.get(cache_key)
                                        if payload is None:
                                            try:
                                                payload = imdb_client.fetch_episodes_payload(
                                                    imdb_series_id_str,
                                                    season=season_no,
                                                    allow_html_fallback=False,
                                                )
                                                imdb_payload_cache[cache_key] = payload
                                            except Exception:  # noqa: BLE001
                                                payload = None

                                        if isinstance(payload, Mapping):
                                            episodes = parse_imdb_season_episodes_payload(payload, season=season_no)
                                        else:
                                            if cache_key in imdb_html_cache:
                                                season_html = imdb_html_cache[cache_key]
                                            else:
                                                season_html = imdb_client.fetch_episodes_page(
                                                    imdb_series_id_str, season=season_no
                                                )
                                                imdb_html_cache[cache_key] = season_html
                                            episodes = parse_imdb_season_episodes_page(season_html, season=season_no)
                                        episode_rows: list[dict[str, Any]] = []
                                        for ep in episodes:
                                            if ep.season != season_no:
                                                continue
                                            existing = existing_by_number.get(int(ep.episode), {})
                                            episode_row: dict[str, Any] = {
                                                "show_id": show_id,
                                                "season_id": season_id_val,
                                                "season_number": int(season_no),
                                                "episode_number": int(ep.episode),
                                                "fetched_at": fetched_at,
                                            }
                                            if ep.imdb_episode_id:
                                                episode_row["imdb_episode_id"] = ep.imdb_episode_id
                                            if ep.title:
                                                episode_row["title"] = ep.title
                                            if ep.overview:
                                                # IMDb is authoritative for canonical episode text when present.
                                                episode_row["overview"] = ep.overview
                                                episode_row["synopsis"] = ep.overview
                                            if ep.air_date:
                                                episode_row["air_date"] = ep.air_date
                                            if ep.imdb_rating is not None:
                                                episode_row["imdb_rating"] = ep.imdb_rating
                                            if ep.imdb_vote_count is not None:
                                                episode_row["imdb_vote_count"] = ep.imdb_vote_count
                                            if ep.imdb_primary_image_url:
                                                episode_row["imdb_primary_image_url"] = ep.imdb_primary_image_url
                                            if ep.imdb_primary_image_caption:
                                                episode_row["imdb_primary_image_caption"] = (
                                                    ep.imdb_primary_image_caption
                                                )
                                            if ep.imdb_primary_image_width is not None:
                                                episode_row["imdb_primary_image_width"] = ep.imdb_primary_image_width
                                            if ep.imdb_primary_image_height is not None:
                                                episode_row["imdb_primary_image_height"] = ep.imdb_primary_image_height

                                            tmdb_episode_id = existing.get("tmdb_episode_id")
                                            if tmdb_episode_id is None and tmdb_id_int is None and ep.imdb_episode_id:
                                                payload = _fetch_tmdb_find_payload(
                                                    ep.imdb_episode_id,
                                                    api_key=tmdb_find_api_key,
                                                    session=tmdb_find_session,
                                                    cache=tmdb_find_cache,
                                                    label="episode",
                                                )
                                                tmdb_episode_id = _extract_tmdb_episode_id(payload)
                                            if isinstance(tmdb_episode_id, int):
                                                episode_row["tmdb_episode_id"] = tmdb_episode_id

                                            external_updates: dict[str, Any] = {}
                                            if ep.imdb_episode_id:
                                                external_updates["imdb"] = ep.imdb_episode_id
                                            if isinstance(tmdb_episode_id, int):
                                                external_updates["tmdb"] = tmdb_episode_id
                                            merged_external_ids = _merge_external_ids(
                                                existing.get("external_ids"),
                                                external_updates,
                                            )
                                            if merged_external_ids is not None:
                                                episode_row["external_ids"] = merged_external_ids
                                            episode_rows.append(episode_row)

                                        if episode_rows:
                                            upsert_episodes(db, episode_rows)
                                            episodes_upserted += len(episode_rows)
                                    except Exception as exc:  # noqa: BLE001
                                        seasons_failed += 1
                                        print(
                                            f"IMDb episodes: failed show_id={show_id} imdb_id={imdb_series_id_str} "
                                            f"season={season_no} error={exc}",
                                            file=sys.stderr,
                                        )

                                print(
                                    "IMDb episodes summary "
                                    f"show_id={show_id} imdb_id={imdb_series_id_str} "
                                    f"seasons={len(season_numbers)} seasons_failed={seasons_failed} "
                                    f"episodes_upserted={episodes_upserted}",
                                    file=sys.stderr,
                                )
                        except Exception as exc:  # noqa: BLE001
                            print(
                                f"IMDb episodes: failed show_id={show_id} imdb_id={imdb_series_id_str} "
                                f"(unexpected error={exc})",
                                file=sys.stderr,
                            )

                # --- TMDb: enrich seasons/episodes and persist season posters (no credits) ---
                if tmdb_fetch_seasons:
                    if tmdb_id_int is None:
                        print(f"TMDb seasons: skipping show_id={show_id} (missing tmdb_series_id)", file=sys.stderr)
                    else:
                        tmdb_meta_map: Mapping[str, Any] = {}
                        tmdb_meta = row.get("tmdb_meta")
                        if isinstance(tmdb_meta, Mapping):
                            tmdb_meta_map = tmdb_meta
                        else:
                            external_ids = row.get("external_ids")
                            external_ids_map = external_ids if isinstance(external_ids, dict) else {}
                            legacy_meta = external_ids_map.get("tmdb_meta")
                            tmdb_meta_map = legacy_meta if isinstance(legacy_meta, Mapping) else {}
                        raw_seasons = tmdb_meta_map.get("seasons")
                        season_numbers: list[int] = []
                        if isinstance(raw_seasons, list):
                            for s in raw_seasons:
                                if not isinstance(s, Mapping):
                                    continue
                                sn = s.get("season_number")
                                if isinstance(sn, int):
                                    season_numbers.append(sn)
                                elif isinstance(sn, str) and sn.strip().isdigit():
                                    season_numbers.append(int(sn.strip()))
                        season_numbers = sorted({n for n in season_numbers if 0 <= n <= 50})
                        if not season_numbers:
                            print(
                                f"TMDb seasons: no seasons found show_id={show_id} tmdb_id={tmdb_id_int}",
                                file=sys.stderr,
                            )
                        else:
                            upsert_seasons(
                                db,
                                [
                                    {
                                        "show_id": show_id,
                                        "season_number": season_no,
                                        "tmdb_series_id": int(tmdb_id_int),
                                        "language": tmdb_season_language,
                                        "external_ids": _merge_external_ids(
                                            {},
                                            {"tmdb": int(tmdb_id_int)},
                                        )
                                        or None,
                                    }
                                    for season_no in season_numbers
                                ],
                            )
                            season_rows = fetch_seasons_by_show(db, show_id=show_id, season_numbers=season_numbers)
                            season_id_by_number: dict[int, str] = {}
                            season_existing_by_number: dict[int, dict[str, Any]] = {}
                            for s in season_rows:
                                sn = s.get("season_number")
                                sid = s.get("id")
                                if isinstance(sn, int) and isinstance(sid, str) and sid:
                                    season_id_by_number[sn] = sid
                                    season_existing_by_number[sn] = s

                            seasons_failed = 0
                            seasons_fetched = 0
                            episodes_upserted = 0
                            posters_upserted = 0

                            for season_no in season_numbers:
                                season_id_val = season_id_by_number.get(season_no)
                                if not season_id_val:
                                    continue
                                existing_season = season_existing_by_number.get(season_no, {})

                                fetched_at = _now_utc_iso()
                                try:
                                    payload = fetch_tv_season_details(
                                        int(tmdb_id_int),
                                        int(season_no),
                                        api_key=None,
                                        session=tmdb_season_session,
                                        language=tmdb_season_language,
                                        include_image_language=tmdb_season_include_lang,
                                        append_to_response=["external_ids", "images"],
                                        cache=tmdb_season_cache,
                                    )
                                    seasons_fetched += 1

                                    ext = payload.get("external_ids")
                                    ext_map = ext if isinstance(ext, Mapping) else {}

                                    season_patch: dict[str, Any] = {
                                        "show_id": show_id,
                                        "season_number": int(season_no),
                                        "tmdb_series_id": int(tmdb_id_int),
                                        "tmdb_season_id": payload.get("id")
                                        if isinstance(payload.get("id"), int)
                                        else None,
                                        "tmdb_season_object_id": payload.get("_id")
                                        if isinstance(payload.get("_id"), str)
                                        else None,
                                        "name": payload.get("name") if isinstance(payload.get("name"), str) else None,
                                        "overview": payload.get("overview")
                                        if isinstance(payload.get("overview"), str)
                                        else None,
                                        "air_date": payload.get("air_date")
                                        if isinstance(payload.get("air_date"), str)
                                        else None,
                                        "poster_path": payload.get("poster_path")
                                        if isinstance(payload.get("poster_path"), str)
                                        else None,
                                        "external_tvdb_id": ext_map.get("tvdb_id")
                                        if isinstance(ext_map.get("tvdb_id"), int)
                                        else None,
                                        "external_wikidata_id": ext_map.get("wikidata_id")
                                        if isinstance(ext_map.get("wikidata_id"), str)
                                        else None,
                                        "language": tmdb_season_language,
                                        "fetched_at": fetched_at,
                                    }
                                    external_updates: dict[str, Any] = {}
                                    if isinstance(imdb_series_id_str, str) and imdb_series_id_str.strip():
                                        external_updates["imdb"] = imdb_series_id_str.strip()
                                    tmdb_season_id_val = season_patch.get("tmdb_season_id")
                                    if isinstance(tmdb_season_id_val, int):
                                        external_updates["tmdb"] = tmdb_season_id_val
                                    elif tmdb_id_int is not None:
                                        external_updates["tmdb"] = int(tmdb_id_int)
                                    merged_external_ids = _merge_external_ids(
                                        existing_season.get("external_ids"),
                                        external_updates,
                                    )
                                    if merged_external_ids is not None:
                                        season_patch["external_ids"] = merged_external_ids
                                    if isinstance(season_patch.get("name"), str) and season_patch["name"].strip():
                                        season_patch["title"] = season_patch["name"]
                                    if (
                                        isinstance(season_patch.get("air_date"), str)
                                        and season_patch["air_date"].strip()
                                    ):
                                        season_patch["premiere_date"] = season_patch["air_date"]

                                    upsert_seasons(db, [season_patch])

                                    existing_eps = fetch_episodes_for_show_season(
                                        db, show_id=show_id, season_number=int(season_no)
                                    )
                                    existing_by_number: dict[int, dict[str, Any]] = {}
                                    for e in existing_eps:
                                        ep_no = e.get("episode_number")
                                        if isinstance(ep_no, int):
                                            existing_by_number[ep_no] = e

                                    episode_rows: list[dict[str, Any]] = []
                                    raw_eps = payload.get("episodes")
                                    if isinstance(raw_eps, list):
                                        for ep in raw_eps:
                                            if not isinstance(ep, Mapping):
                                                continue
                                            ep_no = ep.get("episode_number")
                                            if not isinstance(ep_no, int):
                                                continue

                                            existing = existing_by_number.get(ep_no, {})
                                            existing_title = existing.get("title")
                                            existing_overview = existing.get("overview")
                                            existing_synopsis = existing.get("synopsis")
                                            existing_air_date = existing.get("air_date")

                                            tmdb_title = ep.get("name") if isinstance(ep.get("name"), str) else None
                                            tmdb_overview = (
                                                ep.get("overview") if isinstance(ep.get("overview"), str) else None
                                            )
                                            tmdb_air = (
                                                ep.get("air_date") if isinstance(ep.get("air_date"), str) else None
                                            )

                                            title_val = (
                                                existing_title.strip()
                                                if isinstance(existing_title, str) and existing_title.strip()
                                                else (
                                                    tmdb_title.strip()
                                                    if isinstance(tmdb_title, str) and tmdb_title.strip()
                                                    else None
                                                )
                                            )
                                            air_val = (
                                                existing_air_date
                                                if isinstance(existing_air_date, str) and existing_air_date.strip()
                                                else (
                                                    tmdb_air.strip()
                                                    if isinstance(tmdb_air, str) and tmdb_air.strip()
                                                    else None
                                                )
                                            )

                                            episode_row: dict[str, Any] = {
                                                "show_id": show_id,
                                                "season_id": season_id_val,
                                                "season_number": int(season_no),
                                                "episode_number": int(ep_no),
                                                "tmdb_series_id": int(tmdb_id_int),
                                                "fetched_at": fetched_at,
                                            }
                                            if title_val:
                                                episode_row["title"] = title_val
                                            if (
                                                isinstance(tmdb_overview, str)
                                                and tmdb_overview.strip()
                                                and not (
                                                    isinstance(existing_overview, str) and existing_overview.strip()
                                                )
                                            ):
                                                overview_value = tmdb_overview.strip()
                                                episode_row["overview"] = overview_value
                                                if not (
                                                    isinstance(existing_synopsis, str) and existing_synopsis.strip()
                                                ):
                                                    episode_row["synopsis"] = overview_value
                                            if air_val:
                                                episode_row["air_date"] = air_val

                                            if isinstance(ep.get("id"), int):
                                                episode_row["tmdb_episode_id"] = ep.get("id")
                                            if (
                                                isinstance(ep.get("episode_type"), str)
                                                and ep.get("episode_type").strip()
                                            ):
                                                episode_row["episode_type"] = ep.get("episode_type")
                                            if (
                                                isinstance(ep.get("production_code"), str)
                                                and ep.get("production_code").strip()
                                            ):
                                                episode_row["production_code"] = ep.get("production_code")
                                            if isinstance(ep.get("runtime"), int):
                                                episode_row["runtime"] = ep.get("runtime")
                                            if isinstance(ep.get("still_path"), str) and ep.get("still_path").strip():
                                                episode_row["still_path"] = ep.get("still_path")
                                            if isinstance(ep.get("vote_average"), (int, float)):
                                                episode_row["tmdb_vote_average"] = float(ep.get("vote_average"))
                                            if isinstance(ep.get("vote_count"), int):
                                                episode_row["tmdb_vote_count"] = ep.get("vote_count")

                                            existing_external_ids = existing.get("external_ids")
                                            existing_imdb_episode_id = existing.get("imdb_episode_id")
                                            external_updates: dict[str, Any] = {}
                                            if (
                                                isinstance(existing_imdb_episode_id, str)
                                                and existing_imdb_episode_id.strip()
                                            ):
                                                external_updates["imdb"] = existing_imdb_episode_id.strip()
                                            tmdb_episode_id = episode_row.get("tmdb_episode_id")
                                            if isinstance(tmdb_episode_id, int):
                                                external_updates["tmdb"] = tmdb_episode_id
                                            merged_external_ids = _merge_external_ids(
                                                existing_external_ids,
                                                external_updates,
                                            )
                                            if merged_external_ids is not None:
                                                episode_row["external_ids"] = merged_external_ids

                                            episode_rows.append(episode_row)

                                    if episode_rows:
                                        upsert_episodes(db, episode_rows)
                                        episodes_upserted += len(episode_rows)

                                    images_obj = payload.get("images")
                                    images_map = images_obj if isinstance(images_obj, Mapping) else {}
                                    posters = images_map.get("posters")
                                    poster_rows: list[dict[str, Any]] = []
                                    if isinstance(posters, list):
                                        # TMDb season images payload here is "posters". Do not label as backdrop.
                                        shared_metadata = {
                                            "image_roles": ["poster"],
                                            "season_backdrop": False,
                                        }
                                        for poster in posters:
                                            if not isinstance(poster, Mapping):
                                                continue
                                            file_path = poster.get("file_path")
                                            if not isinstance(file_path, str) or not file_path.strip():
                                                continue
                                            width = poster.get("width")
                                            height = poster.get("height")
                                            aspect_ratio = poster.get("aspect_ratio")
                                            if not isinstance(width, int) or not isinstance(height, int):
                                                continue
                                            if isinstance(aspect_ratio, (int, float)):
                                                aspect_ratio_val: float = float(aspect_ratio)
                                            elif height > 0:
                                                aspect_ratio_val = float(width) / float(height)
                                            else:
                                                aspect_ratio_val = 0.0

                                            url_original = f"https://image.tmdb.org/t/p/original{file_path}"
                                            poster_rows.append(
                                                {
                                                    "show_id": show_id,
                                                    "season_id": season_id_val,
                                                    "tmdb_series_id": int(tmdb_id_int),
                                                    "season_number": int(season_no),
                                                    "source": "tmdb",
                                                    "kind": "poster",
                                                    "iso_639_1": poster.get("iso_639_1")
                                                    if isinstance(poster.get("iso_639_1"), str)
                                                    else None,
                                                    "file_path": file_path,
                                                    "source_image_id": file_path,
                                                    "url": url_original,
                                                    "url_path": file_path,
                                                    "width": int(width),
                                                    "height": int(height),
                                                    "aspect_ratio": aspect_ratio_val,
                                                    "fetched_at": fetched_at,
                                                    "metadata": dict(shared_metadata),
                                                }
                                            )

                                    if poster_rows:
                                        upsert_season_images(db, poster_rows)
                                        posters_upserted += len(poster_rows)

                                except TmdbClientError as exc:
                                    seasons_failed += 1
                                    print(
                                        f"TMDb seasons: failed tmdb_id={tmdb_id_int} season={season_no} "
                                        f"(HTTP {exc.status_code if exc.status_code is not None else 'unknown'})",
                                        file=sys.stderr,
                                    )
                                except Exception as exc:  # noqa: BLE001
                                    seasons_failed += 1
                                    print(
                                        f"TMDb seasons: failed tmdb_id={tmdb_id_int} season={season_no} "
                                        f"(unexpected error={exc})",
                                        file=sys.stderr,
                                    )

                                if season_numbers and (
                                    seasons_fetched == 1
                                    or seasons_fetched % 5 == 0
                                    or seasons_fetched == len(season_numbers)
                                ):
                                    print(
                                        f"TMDb seasons: processed {seasons_fetched}/{len(season_numbers)} show_id={show_id} tmdb_id={tmdb_id_int} "  # noqa: E501
                                        f"(failed={seasons_failed})",
                                        file=sys.stderr,
                                    )

                            print(
                                "TMDb seasons summary "
                                f"show_id={show_id} tmdb_id={tmdb_id_int} "
                                f"seasons={len(season_numbers)} fetched={seasons_fetched} failed={seasons_failed} "
                                f"episodes_upserted={episodes_upserted} season_posters_upserted={posters_upserted}",
                                file=sys.stderr,
                            )

                if idx == 1 or idx % 10 == 0 or idx == total_shows:
                    print(f"Seasons/Episodes: processed {idx}/{total_shows}", file=sys.stderr)

    # Optional TMDb images capture (posters/logos/backdrops): persist into core.show_images and set primary_* columns.
    if tmdb_fetch_images:
        tmdb_images_language = "en-US"
        tmdb_images_include_lang = "en,null"
        tmdb_images_session = requests.Session()
        tmdb_images_cache: dict[tuple[int, str, str], dict[str, Any]] = {}

        tmdb_images_total = sum(1 for row in upserted_show_rows if _coerce_int(row.get("tmdb_id")) is not None)

        if db is not None:
            assert_core_show_images_table_exists(db)

        tmdb_images_processed = 0
        tmdb_images_fetched = 0
        tmdb_images_skipped_cached = 0
        tmdb_images_failed = 0

        for _i, row in enumerate(upserted_show_rows, start=1):
            row_id = row.get("id")
            show_id = str(row_id) if row_id is not None else ""
            if not show_id:
                continue

            tmdb_id_val = row.get("tmdb_id")
            tmdb_id_int: int | None = None
            if isinstance(tmdb_id_val, int):
                tmdb_id_int = tmdb_id_val
            elif isinstance(tmdb_id_val, str) and tmdb_id_val.strip().isdigit():
                tmdb_id_int = int(tmdb_id_val.strip())

            if tmdb_id_int is None:
                tmdb_meta = row.get("tmdb_meta")
                if isinstance(tmdb_meta, Mapping):
                    tmdb_id_int = _coerce_int(tmdb_meta.get("id"))

            if tmdb_id_int is None:
                external_ids = row.get("external_ids")
                external_ids_map = external_ids if isinstance(external_ids, dict) else {}
                tmdb_id_val = external_ids_map.get("tmdb")
                if isinstance(tmdb_id_val, int):
                    tmdb_id_int = tmdb_id_val
                elif isinstance(tmdb_id_val, str) and tmdb_id_val.strip().isdigit():
                    tmdb_id_int = int(tmdb_id_val.strip())
                else:
                    legacy_meta = external_ids_map.get("tmdb_meta")
                    tmdb_id_int = _coerce_int(legacy_meta.get("id")) if isinstance(legacy_meta, Mapping) else None

            if tmdb_id_int is None:
                continue

            tmdb_images_processed += 1

            cache_key = (tmdb_id_int, tmdb_images_language, tmdb_images_include_lang)
            fetched_at = _now_utc_iso()

            try:
                if tmdb_refresh_images and db is not None and not dry_run:
                    delete_tmdb_show_images(db, tmdb_id=tmdb_id_int)

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

                image_rows, primary = _tmdb_show_images_rows(
                    payload,
                    show_id=show_id,
                    tmdb_id=tmdb_id_int,
                    fetched_at=fetched_at,
                )
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
                if (
                    tmdb_images_processed == 1
                    or tmdb_images_processed % 10 == 0
                    or tmdb_images_processed == tmdb_images_total
                ):
                    print(
                        f"TMDb images: processed {tmdb_images_processed}/{tmdb_images_total} "
                        f"(fetched={tmdb_images_fetched} cached={tmdb_images_skipped_cached} failed={tmdb_images_failed})",  # noqa: E501
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

    # Stage 2 enrichment: populate core.shows metadata columns.
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

            show_records.append(
                ShowRecord(
                    id=show_id,
                    name=str(row.get("name") or ""),
                    description=row.get("description") if isinstance(row.get("description"), str) else None,
                    premiere_date=row.get("premiere_date") if isinstance(row.get("premiere_date"), str) else None,
                    imdb_id=(row.get("imdb_id") if isinstance(row.get("imdb_id"), str) else None),
                    tmdb_id=(int(row.get("tmdb_id")) if isinstance(row.get("tmdb_id"), int) else None),
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
                print(f"ENRICH FAIL show_id={failure.show_id} name={failure.name!r} error={failure.message}")
            if len(summary.failures) > 10:
                print(f"ENRICH FAIL ... and {len(summary.failures) - 10} more")

        for patch in summary.patches:
            row = by_id.get(str(patch.show_id))
            if row is None:
                # Dry-run rows have synthetic UUIDs; fall back to searching by UUID object.
                row = next((r for r in upserted_show_rows if str(r.get("id")) == str(patch.show_id)), None)
            if row is None:
                continue

            update_patch: dict[str, Any] = {}
            for key, value in (patch.show_update or {}).items():
                if row.get(key) != value:
                    update_patch[key] = value

            merged_genres = _merge_str_arrays(row.get("genres"), patch.genres)
            if merged_genres is not None:
                update_patch["genres"] = merged_genres

            merged_keywords = _merge_str_arrays(row.get("keywords"), patch.keywords)
            if merged_keywords is not None:
                update_patch["keywords"] = merged_keywords

            merged_tags = _merge_str_arrays(row.get("tags"), patch.tags)
            if merged_tags is not None:
                update_patch["tags"] = merged_tags

            merged_networks = _merge_str_arrays(row.get("networks"), patch.networks)
            if merged_networks is not None:
                update_patch["networks"] = merged_networks

            merged_streaming = _merge_str_arrays(row.get("streaming_providers"), patch.streaming_providers)
            if merged_streaming is not None:
                update_patch["streaming_providers"] = merged_streaming

            merged_tmdb_network_ids = _merge_int_arrays(row.get("tmdb_network_ids"), patch.tmdb_network_ids)
            if merged_tmdb_network_ids is not None:
                update_patch["tmdb_network_ids"] = merged_tmdb_network_ids

            merged_tmdb_company_ids = _merge_int_arrays(
                row.get("tmdb_production_company_ids"), patch.tmdb_production_company_ids
            )
            if merged_tmdb_company_ids is not None:
                update_patch["tmdb_production_company_ids"] = merged_tmdb_company_ids

            merged_alt_names = _merge_str_arrays(
                _filter_ascii_strings(row.get("alternative_names")),
                _filter_ascii_strings(patch.alternative_names),
            )
            if merged_alt_names is not None:
                update_patch["alternative_names"] = merged_alt_names

            if patch.show_images_rows:
                if dry_run:
                    print(f"ENRICH images show_id={patch.show_id} rows={len(patch.show_images_rows)} source=imdb")
                elif supabase_client is not None:
                    try:
                        upsert_show_images(supabase_client, patch.show_images_rows)
                    except Exception as exc:  # noqa: BLE001
                        print(f"ENRICH images failed show_id={patch.show_id} error={exc}", file=sys.stderr)

            if patch.alternative_name_rows:
                if dry_run:
                    print(
                        f"ENRICH alt_names show_id={patch.show_id} rows={len(patch.alternative_name_rows)} source=tmdb"
                    )
                elif supabase_client is not None:
                    try:
                        rows = [
                            {
                                **r,
                                "show_id": str(patch.show_id),
                            }
                            for r in patch.alternative_name_rows
                        ]
                        supabase_client.schema("core").table("show_alternative_names").upsert(
                            rows,
                            on_conflict="show_id,name,language,country,source",
                        ).execute()
                    except Exception as exc:  # noqa: BLE001
                        print(f"ENRICH alt_names failed show_id={patch.show_id} error={exc}", file=sys.stderr)

            if not update_patch:
                continue

            if dry_run:
                print(f"ENRICH UPDATE show id={patch.show_id} patch_keys={sorted(update_patch.keys())}")
                continue

            if db is None:
                raise RuntimeError("Supabase client is not available for enrichment.")
            updated_row = update_show(db, patch.show_id, update_patch)
            print(f"ENRICH UPDATED show id={updated_row.get('id')} name={updated_row.get('name')!r}")

    return ShowImportResult(created=created, updated=updated, skipped=skipped, upserted_show_rows=upserted_show_rows)


def parse_imdb_headers_json_env() -> dict[str, str] | None:
    raw = (os.getenv("IMDB_EXTRA_HEADERS_JSON") or "").strip()
    if not raw:
        raw = (os.getenv("IMDB_HEADERS_JSON") or "").strip()
    if not raw:
        return None
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("IMDB_EXTRA_HEADERS_JSON/IMDB_HEADERS_JSON must be valid JSON object.") from exc
    if not isinstance(value, dict):
        raise ValueError("IMDB_EXTRA_HEADERS_JSON/IMDB_HEADERS_JSON must be a JSON object.")
    headers: dict[str, str] = {}
    for k, v in value.items():
        if v is None:
            continue
        headers[str(k)] = str(v)
    return headers
