from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Iterable, Mapping
from uuid import UUID, uuid4

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
from trr_backend.models.shows import ShowRecord, ShowUpsert
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

    # Only set core external ids if missing.
    for key in ("imdb", "tmdb"):
        if key not in merged and key in updates:
            merged[key] = updates[key]

    return merged


def _candidate_to_show_upsert(candidate: CandidateShow, *, annotate_imdb_episodic: bool) -> ShowUpsert:
    external_ids: dict[str, Any] = {}
    if candidate.imdb_id:
        external_ids["imdb"] = candidate.imdb_id
        if annotate_imdb_episodic:
            external_ids["imdb_episodic"] = {"supported": True}
    if candidate.tmdb_id is not None:
        external_ids["tmdb"] = int(candidate.tmdb_id)

    if candidate.source_tags:
        external_ids["import_sources"] = sorted(candidate.source_tags)

    tmdb_meta: dict[str, Any] = {}
    if candidate.first_air_date:
        tmdb_meta["first_air_date"] = candidate.first_air_date
    if candidate.origin_country:
        tmdb_meta["origin_country"] = list(candidate.origin_country)
    if tmdb_meta:
        external_ids["tmdb_meta"] = tmdb_meta

    imdb_meta: dict[str, Any] = {}
    if candidate.year is not None:
        imdb_meta["year"] = int(candidate.year)
    if imdb_meta:
        external_ids["imdb_meta"] = imdb_meta

    return ShowUpsert(
        title=candidate.title,
        premiere_date=candidate.first_air_date,
        description=None,
        external_ids=external_ids,
    )


def collect_candidates_from_lists(
    *,
    imdb_list_urls: Iterable[str],
    tmdb_lists: Iterable[str | int],
    tmdb_api_key: str | None = None,
    http_session: Any | None = None,
) -> list[CandidateShow]:
    session = http_session

    imdb_candidates: list[CandidateShow] = []
    for url in imdb_list_urls:
        list_id = parse_imdb_list_id(url)
        items: list[ImdbListItem] = fetch_imdb_list_items(url, session=session)
        tag = f"imdb-list:{list_id}"
        for item in items:
            imdb_candidates.append(
                CandidateShow(
                    imdb_id=item.imdb_id,
                    tmdb_id=None,
                    title=item.title,
                    year=item.year,
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
            session=session,
            resolve_external_ids=True,
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
    db = supabase_client or (None if dry_run else create_supabase_admin_client())
    if db is not None:
        assert_core_shows_table_exists(db)

    seasons_by_imdb_id: dict[str, list[int]] = {}
    if annotate_imdb_episodic and imdb_episodic_probe_name_id:
        seasons_by_imdb_id = annotate_candidates_imdb_episodic(
            candidates,
            probe_name_id=imdb_episodic_probe_name_id,
            probe_job_category_id=imdb_episodic_probe_job_category_id,
            extra_headers=imdb_episodic_extra_headers,
        )

    created = 0
    updated = 0
    skipped = 0
    upserted_show_rows: list[dict[str, Any]] = []

    for candidate in candidates:
        show_upsert = _candidate_to_show_upsert(candidate, annotate_imdb_episodic=annotate_imdb_episodic)

        # If probing, attach seasons to external ids for shows with imdb ids.
        if annotate_imdb_episodic and candidate.imdb_id and candidate.imdb_id in seasons_by_imdb_id:
            show_upsert.external_ids.setdefault("imdb_episodic", {})["available_seasons"] = seasons_by_imdb_id[
                candidate.imdb_id
            ]
            show_upsert.external_ids.setdefault("imdb_episodic", {})["reachable"] = True

        existing: dict[str, Any] | None = None
        if db is not None:
            if candidate.imdb_id:
                existing = find_show_by_imdb_id(db, candidate.imdb_id)
            if existing is None and candidate.tmdb_id is not None:
                existing = find_show_by_tmdb_id(db, int(candidate.tmdb_id))

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

        existing_external_ids = existing.get("external_ids")
        existing_external_ids_map = existing_external_ids if isinstance(existing_external_ids, dict) else {}
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
