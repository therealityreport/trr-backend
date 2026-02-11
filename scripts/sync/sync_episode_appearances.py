#!/usr/bin/env python3
"""Sync per-episode cast presence into the credits v2 model.

Legacy tables (`core.episode_appearances`, `core.show_cast`) were replaced by:
- `core.credits` (show-level membership)
- `core.credit_occurrences` (per-episode presence)

This script uses IMDb episodic credits to populate `core.credit_occurrences`.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date
from typing import Any

import requests

from scripts._sync_common import (
    add_show_filter_args,
    extract_imdb_series_id,
    extract_most_recent_episode,
    fetch_show_rows,
    filter_show_rows_for_sync,
    load_env_and_db,
)
from trr_backend.ingestion.show_importer import parse_imdb_headers_json_env
from trr_backend.integrations.imdb.episodic_client import (
    IMDB_JOB_CATEGORY_SELF,
    HttpImdbEpisodicClient,
    ImdbEpisodeCredit,
)
from trr_backend.integrations.imdb.fullcredits_cast_parser import (
    CastRow,
    fetch_fullcredits_cast_with_fallback,
    filter_self_cast_rows,
)
from trr_backend.integrations.tmdb.client import TmdbClientError, find_by_imdb_id, resolve_api_key
from trr_backend.repositories.credits import (
    assert_core_credit_occurrences_table_exists,
    assert_core_credits_table_exists,
    insert_credit_occurrences_ignore_conflicts,
    insert_credits_ignore_conflicts,
)
from trr_backend.repositories.people import (
    assert_core_people_table_exists,
    fetch_people_by_imdb_ids,
    insert_people,
)
from trr_backend.repositories.shows import assert_core_shows_table_exists
from trr_backend.repositories.sync_state import (
    assert_core_sync_state_table_exists,
    mark_sync_state_failed,
    mark_sync_state_in_progress,
    mark_sync_state_success,
)


@dataclass(frozen=True)
class EpisodicCreditsResult:
    cast_row: CastRow
    credits: Sequence[ImdbEpisodeCredit]
    error: str | None = None


@dataclass(frozen=True)
class EpisodeMeta:
    id: str | None
    air_date: str | None


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="sync_episode_appearances",
        description="Sync core.credit_occurrences from IMDb episodic credits (Self only).",
    )
    add_show_filter_args(parser)
    parser.add_argument("--limit-cast", type=int, default=None, help="Optional cap on cast per show.")
    parser.add_argument("--concurrency", type=int, default=4, help="Parallelism for IMDb episodic credits.")
    return parser.parse_args(argv)


def _merge_external_ids(existing: object, updates: dict[str, object]) -> dict[str, object] | None:
    existing_map = existing if isinstance(existing, dict) else {}
    merged: dict[str, object] = dict(existing_map)
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


def _fetch_tmdb_find_payload(
    imdb_id: str,
    *,
    api_key: str | None,
    session: requests.Session,
    cache: dict[str, dict[str, object] | None],
) -> dict[str, object] | None:
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
        print(f"TMDb find failed imdb_id={imdb_id} (HTTP {exc.status_code})", file=sys.stderr)
        payload = None
    except Exception as exc:  # noqa: BLE001
        print(f"TMDb find failed imdb_id={imdb_id} (unexpected error={exc})", file=sys.stderr)
        payload = None
    cache[imdb_id] = payload
    return payload


def _extract_tmdb_person_id(payload: dict[str, object] | None) -> int | None:
    if not isinstance(payload, dict):
        return None
    results = payload.get("person_results")
    if not isinstance(results, list):
        return None
    for item in results:
        if not isinstance(item, dict):
            continue
        tmdb_id = item.get("id")
        if isinstance(tmdb_id, int):
            return tmdb_id
    return None


def _coerce_int(value: object) -> int | None:
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    return None


def _coerce_air_date(value: object) -> str | None:
    if isinstance(value, str):
        cleaned = value.strip()
        return cleaned or None
    return None


def _air_date_from_year(year: int | None) -> str | None:
    if year is None:
        return None
    try:
        return date(int(year), 1, 1).isoformat()
    except ValueError:
        return None


def _air_year_from_air_date(value: str | None) -> int | None:
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    if len(cleaned) < 4:
        return None
    year = cleaned[:4]
    if not year.isdigit():
        return None
    return int(year)


def _fetch_episode_index(db, *, show_id: str) -> dict[str, EpisodeMeta]:
    response = (
        db.schema("core").table("episodes").select("id,imdb_episode_id,air_date").eq("show_id", show_id).execute()
    )
    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Supabase error listing episodes for show_id={show_id}: {response.error}")
    data = response.data or []
    if not isinstance(data, list):
        return {}

    index: dict[str, EpisodeMeta] = {}
    for row in data:
        imdb_id = str(row.get("imdb_episode_id") or "").strip()
        if not imdb_id:
            continue
        index[imdb_id] = EpisodeMeta(
            id=str(row.get("id") or "").strip() or None,
            air_date=_coerce_air_date(row.get("air_date")),
        )
    return index


def _fetch_episodic_credits(
    *,
    series_id: str,
    cast_row: CastRow,
    extra_headers: dict[str, str] | None,
) -> EpisodicCreditsResult:
    job_category_id = cast_row.job_category_id or IMDB_JOB_CATEGORY_SELF
    client = HttpImdbEpisodicClient(extra_headers=extra_headers)
    try:
        seasons = client.fetch_available_seasons(series_id, cast_row.name_id, job_category_id)
        credits = client.fetch_episode_credits_for_seasons(
            series_id,
            cast_row.name_id,
            job_category_id,
            seasons=seasons,
        )
    except Exception as exc:  # noqa: BLE001
        return EpisodicCreditsResult(cast_row=cast_row, credits=(), error=str(exc))

    return EpisodicCreditsResult(cast_row=cast_row, credits=credits, error=None)


def _chunk(values: list[str], *, size: int) -> Sequence[list[str]]:
    step = max(1, int(size))
    return [values[i : i + step] for i in range(0, len(values), step)]


def _pick_credit_id(
    credits_by_person: Mapping[str, list[dict[str, Any]]],
    *,
    person_id: str,
    role: str | None,
) -> str | None:
    rows = credits_by_person.get(person_id) or []
    if not rows:
        return None

    role_norm = (role or "").strip()
    if role_norm:
        for row in rows:
            if str(row.get("role") or "").strip() == role_norm:
                return str(row.get("id") or "") or None

    for row in rows:
        if not str(row.get("role") or "").strip():
            return str(row.get("id") or "") or None

    return str(rows[0].get("id") or "") or None


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    if args.skip_db:
        print("ERROR: --skip-db is not supported for this script (database access required).", file=sys.stderr)
        return 2

    db = load_env_and_db(skip_db=args.skip_db)
    assert_core_shows_table_exists(db)
    assert_core_people_table_exists(db)
    assert_core_credits_table_exists(db)
    assert_core_credit_occurrences_table_exists(db)
    if not args.dry_run and not args.skip_db:
        assert_core_sync_state_table_exists(db)

    show_rows = fetch_show_rows(db, args)
    if not show_rows:
        print("No shows matched the filters.")
        return 0

    filter_result = filter_show_rows_for_sync(
        db,
        show_rows,
        table_name="credit_occurrences",
        incremental=bool(args.incremental),
        resume=bool(args.resume),
        force=bool(args.force),
        since=args.since,
        check_total_seasons=False,
        verbose=bool(args.verbose),
    )
    show_rows = filter_result.selected
    if not show_rows:
        print("No shows need credit_occurrences sync.")
        return 0

    extra_headers = parse_imdb_headers_json_env()
    tmdb_find_api_key = resolve_api_key()
    tmdb_find_session = requests.Session()
    tmdb_find_cache: dict[str, dict[str, object] | None] = {}
    concurrency = max(1, int(args.concurrency or 1))

    cast_rows_total = 0
    cast_rows_self = 0
    people_inserted = 0
    credits_inserted = 0
    occurrences_inserted = 0
    occurrences_skipped_missing_episode = 0
    failures: list[str] = []

    people_cache: dict[str, str] = {}

    for show in show_rows:
        imdb_series_id = extract_imdb_series_id(show)
        show_id = str(show.get("id") or "")
        if not show_id or not imdb_series_id:
            if args.verbose:
                print(f"SKIP show id={show_id or show.get('id')} (missing imdb_series_id)")
            continue
        if not args.dry_run:
            mark_sync_state_in_progress(db, table_name="credit_occurrences", show_id=show_id)

        try:
            try:
                episode_index = _fetch_episode_index(db, show_id=show_id)
            except Exception as exc:  # noqa: BLE001
                print(f"WARNING: Unable to load episodes for show_id={show_id}: {exc}", file=sys.stderr)
                episode_index = {}

            cast_rows, source_type, _person_images = fetch_fullcredits_cast_with_fallback(
                imdb_series_id,
                extra_headers=extra_headers,
                verbose=bool(args.verbose),
                primary_source="graphql",
            )

            cast_rows_total += len(cast_rows)
            self_rows = filter_self_cast_rows(cast_rows)
            cast_rows_self += len(self_rows)
            if args.limit_cast is not None:
                self_rows = self_rows[: max(0, int(args.limit_cast))]

            # Ensure people exist.
            name_ids = [row.name_id.strip().lower() for row in self_rows if row.name_id]
            missing_ids = [name_id for name_id in name_ids if name_id not in people_cache]
            if missing_ids:
                existing_people = fetch_people_by_imdb_ids(db, missing_ids)
                existing_updates: list[tuple[str, dict[str, object]]] = []
                for person in existing_people:
                    imdb_id = str((person.get("external_ids") or {}).get("imdb") or "").strip().lower()
                    if imdb_id:
                        people_cache[imdb_id] = str(person.get("id"))
                        if not (person.get("external_ids") or {}).get("tmdb"):
                            payload = _fetch_tmdb_find_payload(
                                imdb_id,
                                api_key=tmdb_find_api_key,
                                session=tmdb_find_session,
                                cache=tmdb_find_cache,
                            )
                            tmdb_person_id = _extract_tmdb_person_id(payload)
                            merged_external_ids = _merge_external_ids(
                                person.get("external_ids"),
                                {"tmdb": tmdb_person_id},
                            )
                            person_id = person.get("id")
                            if merged_external_ids is not None and isinstance(person_id, str):
                                existing_updates.append((person_id, merged_external_ids))
                for person_id, merged_external_ids in existing_updates:
                    try:
                        db.schema("core").table("people").update({"external_ids": merged_external_ids}).eq(
                            "id", person_id
                        ).execute()
                    except Exception as exc:  # noqa: BLE001
                        print(f"WARNING: failed to update person external_ids id={person_id} error={exc}")

                new_people_map: dict[str, str] = {}
                for row in self_rows:
                    key = row.name_id.strip().lower()
                    if not key or key in people_cache:
                        continue
                    new_people_map.setdefault(key, row.name)

                new_people_rows = [
                    {
                        "full_name": name,
                        "external_ids": _merge_external_ids(
                            {},
                            {
                                "imdb": imdb_id,
                                "tmdb": _extract_tmdb_person_id(
                                    _fetch_tmdb_find_payload(
                                        imdb_id,
                                        api_key=tmdb_find_api_key,
                                        session=tmdb_find_session,
                                        cache=tmdb_find_cache,
                                    )
                                ),
                            },
                        )
                        or {"imdb": imdb_id},
                    }
                    for imdb_id, name in new_people_map.items()
                ]
                if new_people_rows and not args.dry_run:
                    inserted = insert_people(db, new_people_rows)
                    people_inserted += len(inserted)
                    for person in inserted:
                        imdb_id = str((person.get("external_ids") or {}).get("imdb") or "").strip().lower()
                        if imdb_id:
                            people_cache[imdb_id] = str(person.get("id"))
                elif new_people_rows:
                    people_inserted += len(new_people_rows)
                    for row in new_people_rows:
                        imdb_id = str((row.get("external_ids") or {}).get("imdb") or "").strip().lower()
                        if imdb_id:
                            people_cache[imdb_id] = f"dry-run-{imdb_id}"

            # Replace scraped credits (Self only) for this show.
            credit_rows: list[dict[str, object]] = []
            for row in self_rows:
                person_id = people_cache.get(row.name_id.strip().lower())
                if not person_id:
                    continue
                credit_rows.append(
                    {
                        "show_id": show_id,
                        "person_id": person_id,
                        "credit_category": "Self",
                        "role": row.raw_role_text,
                        "billing_order": row.billing_order,
                        "source_type": source_type,
                        "metadata": {},
                    }
                )

            if credit_rows and not args.dry_run:
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
                    raise RuntimeError(f"Supabase error deleting existing credits: {delete_resp.error}")

                inserted_credits = insert_credits_ignore_conflicts(db, credit_rows)
                credits_inserted += len(inserted_credits)
            elif credit_rows:
                credits_inserted += len(credit_rows)

            # Fetch credit ids for mapping to occurrences (limit to this source_type).
            credits_resp = (
                db.schema("core")
                .table("credits")
                .select("id,person_id,role")
                .eq("show_id", show_id)
                .eq("credit_category", "Self")
                .eq("source_type", source_type)
                .execute()
            )
            if hasattr(credits_resp, "error") and credits_resp.error:
                raise RuntimeError(f"Supabase error fetching credits: {credits_resp.error}")
            credits_rows = credits_resp.data or []
            if not isinstance(credits_rows, list):
                credits_rows = []

            credits_by_person: dict[str, list[dict[str, Any]]] = {}
            for row in credits_rows:
                pid = str(row.get("person_id") or "")
                if not pid:
                    continue
                credits_by_person.setdefault(pid, []).append(row)

            # Delete existing occurrences for these credits so the episodic scrape is authoritative.
            credit_ids = [str(r.get("id") or "") for r in credits_rows if r.get("id")]
            credit_ids = [cid for cid in credit_ids if cid]
            if credit_ids and not args.dry_run:
                for batch in _chunk(credit_ids, size=200):
                    del_resp = db.schema("core").table("credit_occurrences").delete().in_("credit_id", batch).execute()
                    if hasattr(del_resp, "error") and del_resp.error:
                        raise RuntimeError(f"Supabase error deleting credit_occurrences: {del_resp.error}")

            occurrence_by_key: dict[tuple[str, str], dict[str, Any]] = {}

            with ThreadPoolExecutor(max_workers=concurrency) as pool:
                futures = {
                    pool.submit(
                        _fetch_episodic_credits,
                        series_id=imdb_series_id,
                        cast_row=row,
                        extra_headers=extra_headers,
                    ): row
                    for row in self_rows
                }
                for future in as_completed(futures):
                    result = future.result()
                    if result.error:
                        failures.append(f"{result.cast_row.name_id}: {result.error}")
                        continue

                    person_id = people_cache.get(result.cast_row.name_id.strip().lower())
                    if not person_id:
                        failures.append(f"{result.cast_row.name_id}: missing person_id")
                        continue

                    credit_id = _pick_credit_id(
                        credits_by_person,
                        person_id=person_id,
                        role=result.cast_row.raw_role_text,
                    )
                    if not credit_id:
                        failures.append(f"{result.cast_row.name_id}: missing credit_id")
                        continue

                    for credit in result.credits:
                        imdb_episode_id = str(getattr(credit.episode, "title_id", "") or "").strip()
                        if not imdb_episode_id:
                            continue
                        meta = episode_index.get(imdb_episode_id)
                        if not meta or not meta.id:
                            occurrences_skipped_missing_episode += 1
                            continue

                        key = (credit_id, meta.id)
                        row = occurrence_by_key.get(key)
                        if row is None:
                            row = {
                                "credit_id": credit_id,
                                "episode_id": meta.id,
                                "appearance_type": "appears",
                                "attributes": [],
                                "is_archive_footage": False,
                            }

                        air_year = (
                            credit.episode.year
                            if credit.episode.year is not None
                            else _air_year_from_air_date(meta.air_date)
                        )
                        if row.get("air_year") is None and air_year is not None:
                            row["air_year"] = int(air_year)

                        credit_text = (credit.job or "").strip() or None
                        if not row.get("credit_text") and credit_text:
                            row["credit_text"] = credit_text

                        attrs = [a.strip() for a in (credit.attributes or ()) if isinstance(a, str) and a.strip()]
                        if attrs:
                            existing_attrs = row.get("attributes")
                            existing_list = existing_attrs if isinstance(existing_attrs, list) else []
                            merged = list(dict.fromkeys([*existing_list, *attrs]))
                            row["attributes"] = merged

                        is_archive = bool(credit.is_archive_footage)
                        row["is_archive_footage"] = bool(row.get("is_archive_footage")) or is_archive
                        if row["is_archive_footage"]:
                            row["appearance_type"] = "archive_footage"

                        occurrence_by_key[key] = row

            occurrence_rows = list(occurrence_by_key.values())
            if occurrence_rows:
                if args.dry_run:
                    occurrences_inserted += len(occurrence_rows)
                else:
                    inserted = insert_credit_occurrences_ignore_conflicts(db, occurrence_rows)
                    occurrences_inserted += len(inserted)

            if not args.dry_run:
                mark_sync_state_success(
                    db,
                    table_name="credit_occurrences",
                    show_id=show_id,
                    last_seen_most_recent_episode=extract_most_recent_episode(show),
                )
        except Exception as exc:  # noqa: BLE001
            failures.append(f"{imdb_series_id}: {exc}")
            if not args.dry_run:
                mark_sync_state_failed(db, table_name="credit_occurrences", show_id=show_id, error=exc)
            continue

    print("Summary")
    print(f"shows_processed={len(show_rows)}")
    print(f"cast_rows_total={cast_rows_total}")
    print(f"cast_rows_self={cast_rows_self}")
    print(f"people_inserted={people_inserted}")
    print(f"credits_inserted={credits_inserted}")
    print(f"occurrences_inserted={occurrences_inserted}")
    print(f"occurrences_skipped_missing_episode={occurrences_skipped_missing_episode}")
    print(f"failures={len(failures)}")

    if failures:
        for failure in failures[:10]:
            print(f"- {failure}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
