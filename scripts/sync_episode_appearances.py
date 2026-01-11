#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date

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
    fetch_fullcredits_cast,
    filter_self_cast_rows,
)
from trr_backend.repositories.episode_appearances import (
    assert_core_episode_appearances_table_exists,
    upsert_episode_appearances,
)
from trr_backend.repositories.people import (
    assert_core_people_table_exists,
    fetch_people_by_imdb_ids,
    insert_people,
)
from trr_backend.repositories.show_cast import (
    assert_core_show_cast_table_exists,
    upsert_show_cast,
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
    season_number: int | None
    episode_number: int | None
    air_date: str | None
    tmdb_episode_id: int | None


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="sync_episode_appearances",
        description="Sync core.episode_appearances (aggregated per person/show).",
    )
    add_show_filter_args(parser)
    parser.add_argument("--limit-cast", type=int, default=None, help="Optional cap on cast per show.")
    parser.add_argument("--concurrency", type=int, default=4, help="Parallelism for IMDb episodic credits.")
    return parser.parse_args(argv)


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


def _fetch_episode_index(db, *, show_id: str) -> dict[str, EpisodeMeta]:
    response = (
        db.schema("core")
        .table("episodes")
        .select("imdb_episode_id,season_number,episode_number,air_date,tmdb_episode_id")
        .eq("show_id", show_id)
        .execute()
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
            season_number=_coerce_int(row.get("season_number")),
            episode_number=_coerce_int(row.get("episode_number")),
            air_date=_coerce_air_date(row.get("air_date")),
            tmdb_episode_id=_coerce_int(row.get("tmdb_episode_id")),
        )
    return index


def _fetch_season_tmdb_ids(db, *, show_id: str) -> dict[int, int]:
    response = (
        db.schema("core").table("seasons").select("season_number,tmdb_season_id").eq("show_id", show_id).execute()
    )
    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Supabase error listing seasons for show_id={show_id}: {response.error}")
    data = response.data or []
    if not isinstance(data, list):
        return {}

    season_map: dict[int, int] = {}
    for row in data:
        season_number = _coerce_int(row.get("season_number"))
        tmdb_season_id = _coerce_int(row.get("tmdb_season_id"))
        if season_number is None or tmdb_season_id is None:
            continue
        season_map[season_number] = tmdb_season_id
    return season_map


def _episode_sort_key(imdb_id: str, meta: EpisodeMeta | None, credit: ImdbEpisodeCredit) -> tuple:
    season_number = meta.season_number if meta and meta.season_number is not None else credit.episode.season_number
    episode_number = meta.episode_number if meta and meta.episode_number is not None else credit.episode.episode_number
    air_date = meta.air_date if meta and meta.air_date is not None else _air_date_from_year(credit.episode.year)

    season_sort = season_number if season_number is not None else 9_999
    episode_sort = episode_number if episode_number is not None else 9_999
    air_sort = air_date or "9999-12-31"
    return (season_number is None, season_sort, episode_number is None, episode_sort, air_sort, imdb_id)


def _build_rollup_row(
    *,
    show_id: str,
    show_name: str | None,
    tmdb_show_id: int | None,
    imdb_show_id: str | None,
    person_id: str,
    cast_member_name: str | None,
    credits: Sequence[ImdbEpisodeCredit],
    episode_index: dict[str, EpisodeMeta],
    season_tmdb_ids: dict[int, int],
) -> dict[str, object] | None:
    episode_map: dict[str, tuple[EpisodeMeta | None, ImdbEpisodeCredit]] = {}
    for credit in credits:
        if credit.is_archive_footage:
            continue
        imdb_id = str(credit.episode.title_id or "").strip()
        if not imdb_id:
            continue
        if imdb_id not in episode_map:
            episode_map[imdb_id] = (episode_index.get(imdb_id), credit)

    if not episode_map:
        return None

    ordered = sorted(
        episode_map.items(),
        key=lambda item: _episode_sort_key(item[0], item[1][0], item[1][1]),
    )

    imdb_episode_ids: list[str] = []
    tmdb_episode_ids: list[int] = []
    tmdb_seen: set[int] = set()
    season_numbers: set[int] = set()

    for imdb_id, (meta, credit) in ordered:
        imdb_episode_ids.append(imdb_id)

        season_number = meta.season_number if meta and meta.season_number is not None else credit.episode.season_number
        if isinstance(season_number, int):
            season_numbers.add(season_number)

        tmdb_episode_id = meta.tmdb_episode_id if meta and meta.tmdb_episode_id is not None else None
        if tmdb_episode_id is not None and tmdb_episode_id not in tmdb_seen:
            tmdb_seen.add(tmdb_episode_id)
            tmdb_episode_ids.append(tmdb_episode_id)

    seasons_sorted = sorted(season_numbers)
    tmdb_seasons_sorted: list[int] = []
    for season_number in seasons_sorted:
        tmdb_season_id = season_tmdb_ids.get(season_number)
        if tmdb_season_id is not None:
            tmdb_seasons_sorted.append(tmdb_season_id)

    return {
        "show_id": show_id,
        "person_id": person_id,
        "show_name": show_name,
        "cast_member_name": cast_member_name,
        "tmdb_show_id": tmdb_show_id,
        "imdb_show_id": imdb_show_id,
        "seasons": seasons_sorted,
        "tmdb_season_ids": tmdb_seasons_sorted,
        "imdb_episode_title_ids": imdb_episode_ids,
        "tmdb_episode_ids": tmdb_episode_ids,
    }


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


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    db = load_env_and_db()

    assert_core_shows_table_exists(db)
    assert_core_people_table_exists(db)
    assert_core_show_cast_table_exists(db)
    assert_core_episode_appearances_table_exists(db)
    if not args.dry_run:
        assert_core_sync_state_table_exists(db)

    show_rows = fetch_show_rows(db, args)
    if not show_rows:
        print("No shows matched the filters.")
        return 0

    filter_result = filter_show_rows_for_sync(
        db,
        show_rows,
        table_name="episode_appearances",
        incremental=bool(args.incremental),
        resume=bool(args.resume),
        force=bool(args.force),
        since=args.since,
        check_total_seasons=False,
        verbose=bool(args.verbose),
    )
    show_rows = filter_result.selected
    if not show_rows:
        print("No shows need episode_appearances sync.")
        return 0

    extra_headers = parse_imdb_headers_json_env()
    concurrency = max(1, int(args.concurrency or 1))

    cast_rows_total = 0
    cast_rows_self = 0
    people_inserted = 0
    rollups_inserted = 0
    rollups_skipped = 0
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
            mark_sync_state_in_progress(db, table_name="episode_appearances", show_id=show_id)

        try:
            show_name = str(show.get("name") or "").strip() or None
            tmdb_show_id = _coerce_int(show.get("tmdb_series_id"))
            if tmdb_show_id is None:
                tmdb_show_id = _coerce_int((show.get("external_ids") or {}).get("tmdb"))
            imdb_show_id = str(show.get("imdb_series_id") or "").strip() or None
            if imdb_show_id is None:
                imdb_show_id = str((show.get("external_ids") or {}).get("imdb") or "").strip() or None

            try:
                episode_index = _fetch_episode_index(db, show_id=show_id)
            except Exception as exc:  # noqa: BLE001
                print(f"WARNING: Unable to load episodes for show_id={show_id}: {exc}", file=sys.stderr)
                episode_index = {}
            try:
                season_tmdb_ids = _fetch_season_tmdb_ids(db, show_id=show_id)
            except Exception as exc:  # noqa: BLE001
                print(f"WARNING: Unable to load seasons for show_id={show_id}: {exc}", file=sys.stderr)
                season_tmdb_ids = {}

            cast_rows = fetch_fullcredits_cast(imdb_series_id, extra_headers=extra_headers)

            cast_rows_total += len(cast_rows)
            self_rows = filter_self_cast_rows(cast_rows)
            cast_rows_self += len(self_rows)
            if args.limit_cast is not None:
                self_rows = self_rows[: max(0, int(args.limit_cast))]

            name_ids = [row.name_id.strip().lower() for row in self_rows if row.name_id]
            missing_ids = [name_id for name_id in name_ids if name_id not in people_cache]
            if missing_ids:
                existing_people = fetch_people_by_imdb_ids(db, missing_ids)
                for person in existing_people:
                    imdb_id = str((person.get("external_ids") or {}).get("imdb") or "").strip().lower()
                    if imdb_id:
                        people_cache[imdb_id] = str(person.get("id"))

                new_people_map: dict[str, str] = {}
                for row in self_rows:
                    key = row.name_id.strip().lower()
                    if not key or key in people_cache:
                        continue
                    new_people_map.setdefault(key, row.name)

                new_people_rows = [
                    {"full_name": name, "external_ids": {"imdb": imdb_id}} for imdb_id, name in new_people_map.items()
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

            show_cast_rows = []
            for row in self_rows:
                person_id = people_cache.get(row.name_id.strip().lower())
                if not person_id:
                    continue
                show_cast_rows.append(
                    {
                        "show_id": show_id,
                        "person_id": person_id,
                        "billing_order": row.billing_order,
                        "role": row.raw_role_text,
                        "credit_category": "Self",
                    }
                )

            if show_cast_rows and not args.dry_run:
                upsert_show_cast(db, show_cast_rows)

            rollup_rows: list[dict[str, object]] = []
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

                    rollup_row = _build_rollup_row(
                        show_id=show_id,
                        show_name=show_name,
                        tmdb_show_id=tmdb_show_id,
                        imdb_show_id=imdb_show_id,
                        person_id=person_id,
                        cast_member_name=result.cast_row.name,
                        credits=result.credits,
                        episode_index=episode_index,
                        season_tmdb_ids=season_tmdb_ids,
                    )
                    if rollup_row is None:
                        rollups_skipped += 1
                        continue
                    rollup_rows.append(rollup_row)

            if rollup_rows:
                if args.dry_run:
                    rollups_inserted += len(rollup_rows)
                else:
                    upsert_episode_appearances(db, rollup_rows)
                    rollups_inserted += len(rollup_rows)

            if not args.dry_run:
                mark_sync_state_success(
                    db,
                    table_name="episode_appearances",
                    show_id=show_id,
                    last_seen_most_recent_episode=extract_most_recent_episode(show),
                )
        except Exception as exc:  # noqa: BLE001
            failures.append(f"{imdb_series_id}: {exc}")
            if not args.dry_run:
                mark_sync_state_failed(db, table_name="episode_appearances", show_id=show_id, error=exc)
            continue

    print("Summary")
    print(f"shows_processed={len(show_rows)}")
    print(f"cast_rows_total={cast_rows_total}")
    print(f"cast_rows_self={cast_rows_self}")
    print(f"people_inserted={people_inserted}")
    print(f"rollups_inserted={rollups_inserted}")
    print(f"rollups_skipped={rollups_skipped}")
    print(f"failures={len(failures)}")

    if failures:
        for failure in failures[:10]:
            print(f"- {failure}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
