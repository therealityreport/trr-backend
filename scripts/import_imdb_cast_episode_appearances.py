#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Iterable, Sequence

from dotenv import load_dotenv

from trr_backend.db.supabase import create_supabase_admin_client
from trr_backend.ingestion.show_importer import parse_imdb_headers_json_env
from trr_backend.integrations.imdb.episodic_client import (
    HttpImdbEpisodicClient,
    IMDB_JOB_CATEGORY_SELF,
    ImdbEpisodeCredit,
)
from trr_backend.integrations.imdb.fullcredits_cast_parser import (
    CastRow,
    fetch_fullcredits_cast,
    filter_self_cast_rows,
)
from trr_backend.repositories.episode_appearances import (
    assert_core_episode_appearances_table_exists,
    fetch_existing_episode_ids,
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
from trr_backend.repositories.shows import assert_core_shows_table_exists, find_show_by_imdb_id


@dataclass(frozen=True)
class EpisodicCreditsResult:
    cast_row: CastRow
    credits: Sequence[ImdbEpisodeCredit]
    error: str | None = None


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="import_imdb_cast_episode_appearances.py",
        description="Import IMDb full credits cast (Self only) and episode appearances.",
    )
    parser.add_argument("--imdb-series-id", required=True, help="IMDb series id (tt...).")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and summarize without writing to Supabase.")
    parser.add_argument("--limit-cast", type=int, default=None, help="Optional cap on number of cast members.")
    parser.add_argument(
        "--concurrency",
        type=int,
        default=4,
        help="Parallelism for IMDb episodic credits fetch (default: 4).",
    )
    return parser.parse_args(argv)


def _build_credit_text(credit: ImdbEpisodeCredit) -> str | None:
    job = (credit.job or "").strip()
    if not job:
        return None
    as_attrs = [attr for attr in credit.attributes if isinstance(attr, str) and attr.lower().startswith("as ")]
    if as_attrs:
        joined = "; ".join(as_attrs)
        return f"{job} ({joined})"
    return job


def _normalize_episode_appearance_rows(
    *,
    show_id: str,
    person_id: str,
    credits: Sequence[ImdbEpisodeCredit],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for credit in credits:
        episode = credit.episode
        rows.append(
            {
                "show_id": show_id,
                "person_id": person_id,
                "episode_imdb_id": episode.title_id,
                "season_number": episode.season_number,
                "episode_number": episode.episode_number,
                "episode_title": episode.title,
                "air_year": episode.year,
                "credit_category": "Self",
                "credit_text": _build_credit_text(credit),
                "attributes": list(credit.attributes),
                "is_archive_footage": bool(credit.is_archive_footage),
            }
        )
    return rows


def _dedupe_appearance_rows(rows: Iterable[dict[str, object]]) -> list[dict[str, object]]:
    deduped: list[dict[str, object]] = []
    seen: set[tuple[str, str, str, str]] = set()
    for row in rows:
        key = (
            str(row.get("show_id")),
            str(row.get("person_id")),
            str(row.get("episode_imdb_id")),
            str(row.get("credit_category")),
        )
        if key in seen:
            continue
        deduped.append(row)
        seen.add(key)
    return deduped


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
    load_dotenv()

    series_id = str(args.imdb_series_id).strip()
    dry_run = bool(args.dry_run)
    limit_cast = int(args.limit_cast) if args.limit_cast is not None else None
    concurrency = max(1, int(args.concurrency or 1))

    extra_headers = parse_imdb_headers_json_env()

    db = create_supabase_admin_client()
    assert_core_shows_table_exists(db)
    assert_core_people_table_exists(db)
    assert_core_show_cast_table_exists(db)
    assert_core_episode_appearances_table_exists(db)

    show = find_show_by_imdb_id(db, series_id)
    if not show:
        print(
            f"ERROR: core.shows missing imdb id {series_id}. Run list import first (import_shows_from_lists.py).",
            file=sys.stderr,
        )
        return 1

    show_id = str(show.get("id"))

    cast_rows = fetch_fullcredits_cast(series_id, extra_headers=extra_headers)
    cast_rows_total = len(cast_rows)

    self_rows = filter_self_cast_rows(cast_rows)
    cast_rows_self = len(self_rows)

    if limit_cast is not None:
        self_rows = self_rows[: max(0, limit_cast)]

    name_ids = [row.name_id for row in self_rows]
    existing_people = fetch_people_by_imdb_ids(db, name_ids)
    people_cache: dict[str, str] = {}
    for person in existing_people:
        imdb_id = str((person.get("external_ids") or {}).get("imdb") or "").strip().lower()
        if imdb_id:
            people_cache[imdb_id] = str(person.get("id"))

    new_people_rows: list[dict[str, object]] = []
    for row in self_rows:
        if row.name_id.lower() in people_cache:
            continue
        new_people_rows.append(
            {
                "full_name": row.name,
                "external_ids": {"imdb": row.name_id},
            }
        )

    people_upserted = 0
    if new_people_rows and not dry_run:
        inserted = insert_people(db, new_people_rows)
        people_upserted = len(inserted)
        for person in inserted:
            imdb_id = str((person.get("external_ids") or {}).get("imdb") or "").strip().lower()
            if imdb_id:
                people_cache[imdb_id] = str(person.get("id"))

    if new_people_rows and dry_run:
        people_upserted = len(new_people_rows)
        for row in new_people_rows:
            imdb_id = str((row.get("external_ids") or {}).get("imdb") or "").strip().lower()
            if imdb_id:
                people_cache[imdb_id] = f"dry-run-{imdb_id}"

    show_cast_rows: list[dict[str, object]] = []
    for row in self_rows:
        person_id = people_cache.get(row.name_id.lower())
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

    if show_cast_rows and not dry_run:
        upsert_show_cast(db, show_cast_rows)

    appearances_inserted = 0
    appearances_skipped = 0
    failures: list[str] = []

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = {
            pool.submit(
                _fetch_episodic_credits,
                series_id=series_id,
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

            person_id = people_cache.get(result.cast_row.name_id.lower())
            if not person_id:
                failures.append(f"{result.cast_row.name_id}: missing person_id")
                continue

            appearance_rows = _normalize_episode_appearance_rows(
                show_id=show_id,
                person_id=person_id,
                credits=result.credits,
            )
            appearance_rows = _dedupe_appearance_rows(appearance_rows)

            if dry_run:
                appearances_inserted += len(appearance_rows)
                continue

            existing_episode_ids = fetch_existing_episode_ids(
                db,
                show_id=show_id,
                person_id=person_id,
                credit_category="Self",
            )
            new_rows = [
                row
                for row in appearance_rows
                if str(row.get("episode_imdb_id")) not in existing_episode_ids
            ]

            appearances_skipped += len(appearance_rows) - len(new_rows)
            if not new_rows:
                continue

            upsert_episode_appearances(db, new_rows)
            appearances_inserted += len(new_rows)

    print("Summary")
    print(f"cast_rows_total={cast_rows_total}")
    print(f"cast_rows_self={cast_rows_self}")
    print(f"people_upserted={people_upserted}")
    print(f"appearances_inserted={appearances_inserted}")
    print(f"appearances_skipped={appearances_skipped}")
    print(f"failures={len(failures)}")

    if failures:
        print("Failures:")
        for failure in failures[:10]:
            print(f"- {failure}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
