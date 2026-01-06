#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone
from typing import Any, Mapping

from supabase import Client

from trr_backend.integrations.tmdb.client import TmdbClientError, fetch_tv_details, resolve_api_key
from trr_backend.media.s3_mirror import mirror_tmdb_logo_row
from trr_backend.repositories.shows import update_show
from trr_backend.repositories.sync_state import (
    assert_core_sync_state_table_exists,
    mark_sync_state_failed,
    mark_sync_state_in_progress,
    mark_sync_state_success,
)
from trr_backend.utils.env import load_env

from scripts._sync_common import (
    add_show_filter_args,
    extract_most_recent_episode,
    filter_show_rows_for_sync,
    load_env_and_db,
)


NETWORK_FIELDS = (
    "id,name,origin_country,tmdb_logo_path,logo_path,hosted_logo_key,hosted_logo_url,hosted_logo_sha256,"
    "hosted_logo_content_type,hosted_logo_bytes,hosted_logo_etag,hosted_logo_at"
)


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="sync_tmdb_show_entities",
        description="Sync TMDb networks + production companies into core tables and update show arrays.",
    )
    add_show_filter_args(parser)
    parser.add_argument("--skip-s3", action="store_true", help="Skip S3 logo mirroring.")
    return parser.parse_args(argv)


def _require_supabase_db_url() -> None:
    if not (os.getenv("SUPABASE_DB_URL") or "").strip():
        raise RuntimeError("SUPABASE_DB_URL must be set for TMDb entity sync scripts.")


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _coerce_int_list(values: list[str]) -> list[int]:
    out: list[int] = []
    for raw in values:
        s = str(raw).strip()
        if s.isdigit():
            out.append(int(s))
    return out


def _coerce_str_list(values: list[str]) -> list[str]:
    out: list[str] = []
    for raw in values:
        s = str(raw).strip()
        if s:
            out.append(s)
    return out


def _should_process_all(args: argparse.Namespace) -> bool:
    return bool(args.all) or not (args.show_id or args.tmdb_show_id or args.imdb_series_id)


def _dedupe_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    ordered: list[dict[str, Any]] = []
    for row in rows:
        row_id = str(row.get("id") or "")
        if not row_id or row_id in seen:
            continue
        seen.add(row_id)
        ordered.append(row)
    return ordered


def _fetch_show_rows(db: Client, args: argparse.Namespace) -> list[dict[str, Any]]:
    fields = "id,name,tmdb_id,networks,tmdb_network_ids,tmdb_production_company_ids,most_recent_episode"
    rows: list[dict[str, Any]] = []

    if _should_process_all(args):
        query = db.schema("core").table("shows").select(fields)
        if args.limit is not None:
            query = query.limit(max(0, int(args.limit)))
        response = query.execute()
        if hasattr(response, "error") and response.error:
            raise RuntimeError(f"Supabase error listing shows: {response.error}")
        data = response.data or []
        rows = data if isinstance(data, list) else []
        return rows

    show_ids = _coerce_str_list(args.show_id or [])
    tmdb_ids = _coerce_int_list(args.tmdb_show_id or [])
    imdb_ids = _coerce_str_list(args.imdb_series_id or [])

    if show_ids:
        response = db.schema("core").table("shows").select(fields).in_("id", show_ids).execute()
        if hasattr(response, "error") and response.error:
            raise RuntimeError(f"Supabase error listing shows by id: {response.error}")
        data = response.data or []
        if isinstance(data, list):
            rows.extend(data)

    if tmdb_ids:
        response = db.schema("core").table("shows").select(fields).in_("tmdb_id", tmdb_ids).execute()
        if hasattr(response, "error") and response.error:
            raise RuntimeError(f"Supabase error listing shows by tmdb_id: {response.error}")
        data = response.data or []
        if isinstance(data, list):
            rows.extend(data)

    if imdb_ids:
        response = db.schema("core").table("shows").select(fields).in_("imdb_id", imdb_ids).execute()
        if hasattr(response, "error") and response.error:
            raise RuntimeError(f"Supabase error listing shows by imdb_id: {response.error}")
        data = response.data or []
        if isinstance(data, list):
            rows.extend(data)

    rows = _dedupe_rows(rows)
    if args.limit is not None:
        rows = rows[: max(0, int(args.limit))]
    return rows


def _merge_str_arrays(existing: object, incoming: list[str]) -> list[str] | None:
    if not incoming:
        return None
    existing_values = [
        str(v).strip()
        for v in (existing if isinstance(existing, list) else [])
        if isinstance(v, str) and str(v).strip()
    ]
    incoming_values = [str(v).strip() for v in incoming if isinstance(v, str) and str(v).strip()]
    merged = sorted(set(existing_values) | set(incoming_values))
    if not merged or merged == sorted(existing_values):
        return None
    return merged


def _merge_int_arrays(existing: object, incoming: list[int]) -> list[int] | None:
    if not incoming:
        return None
    existing_values = [v for v in (existing if isinstance(existing, list) else []) if isinstance(v, int)]
    merged = sorted(set(existing_values) | set(incoming))
    if not merged or merged == sorted(existing_values):
        return None
    return merged


def _extract_network_rows(details: Mapping[str, Any], *, fetched_at: str) -> list[dict[str, Any]]:
    networks = details.get("networks")
    if not isinstance(networks, list):
        return []
    rows: list[dict[str, Any]] = []
    for item in networks:
        if not isinstance(item, Mapping):
            continue
        network_id = item.get("id")
        name = item.get("name")
        if not isinstance(network_id, int) or not isinstance(name, str) or not name.strip():
            continue
        rows.append(
            {
                "id": network_id,
                "name": name.strip(),
                "origin_country": item.get("origin_country"),
                "tmdb_logo_path": item.get("logo_path"),
                "tmdb_meta": dict(item),
                "tmdb_fetched_at": fetched_at,
            }
        )
    return rows


def _extract_company_rows(details: Mapping[str, Any], *, fetched_at: str) -> list[dict[str, Any]]:
    companies = details.get("production_companies")
    if not isinstance(companies, list):
        return []
    rows: list[dict[str, Any]] = []
    for item in companies:
        if not isinstance(item, Mapping):
            continue
        company_id = item.get("id")
        name = item.get("name")
        if not isinstance(company_id, int) or not isinstance(name, str) or not name.strip():
            continue
        rows.append(
            {
                "id": company_id,
                "name": name.strip(),
                "origin_country": item.get("origin_country"),
                "tmdb_logo_path": item.get("logo_path"),
                "tmdb_meta": dict(item),
                "tmdb_fetched_at": fetched_at,
            }
        )
    return rows


def _upsert_rows(db: Client, *, table: str, rows: list[dict[str, Any]], on_conflict: str) -> list[dict[str, Any]]:
    if not rows:
        return []
    response = db.schema("core").table(table).upsert(rows, on_conflict=on_conflict).execute()
    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Supabase error upserting {table}: {response.error}")
    data = response.data or []
    return data if isinstance(data, list) else []


def _fetch_entities_for_logo(db: Client, *, table: str, ids: list[int]) -> list[dict[str, Any]]:
    if not ids:
        return []
    response = db.schema("core").table(table).select(NETWORK_FIELDS).in_("id", ids).execute()
    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Supabase error fetching {table} rows: {response.error}")
    data = response.data or []
    return data if isinstance(data, list) else []


def _mirror_logo_rows(
    db: Client,
    *,
    table: str,
    kind: str,
    ids: list[int],
    processed: set[int],
    dry_run: bool,
    force: bool,
    s3_client,
) -> int:
    to_process = [entity_id for entity_id in ids if entity_id not in processed]
    if not to_process:
        return 0
    processed.update(to_process)
    rows = _fetch_entities_for_logo(db, table=table, ids=to_process)
    mirrored = 0
    for row in rows:
        patch = mirror_tmdb_logo_row(row, kind=kind, force=force, s3_client=s3_client)
        if not patch:
            continue
        mirrored += 1
        if dry_run:
            continue
        response = db.schema("core").table(table).update(patch).eq("id", row.get("id")).execute()
        if hasattr(response, "error") and response.error:
            raise RuntimeError(f"Supabase error updating {table} logo: {response.error}")
    return mirrored


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    load_env()
    _require_supabase_db_url()

    api_key = resolve_api_key() or None
    if not api_key:
        raise RuntimeError("TMDB_API_KEY is required for TMDb entity sync.")

    db = load_env_and_db()
    if not args.dry_run:
        assert_core_sync_state_table_exists(db)

    show_rows = _fetch_show_rows(db, args)
    if not show_rows:
        print("No shows matched the filters.")
        return 0

    filter_result = filter_show_rows_for_sync(
        db,
        show_rows,
        table_name="tmdb_show_entities",
        incremental=bool(args.incremental),
        resume=bool(args.resume),
        force=bool(args.force),
        since=args.since,
        check_total_seasons=False,
        verbose=bool(args.verbose),
    )
    show_rows = filter_result.selected
    if not show_rows:
        print("No shows need TMDb entity sync.")
        return 0

    processed_network_ids: set[int] = set()
    processed_company_ids: set[int] = set()
    s3_client = None
    if not args.skip_s3 and not args.dry_run:
        from trr_backend.media.s3_mirror import get_s3_client

        s3_client = get_s3_client()

    total_networks_upserted = 0
    total_companies_upserted = 0
    total_logos_mirrored = 0
    total_show_updates = 0
    failures: list[str] = []

    for show in show_rows:
        show_id = str(show.get("id") or "").strip()
        tmdb_id = show.get("tmdb_id")
        if not show_id or not isinstance(tmdb_id, int):
            if args.verbose:
                print(f"SKIP show id={show.get('id')} (missing tmdb_id)")
            continue

        if not args.dry_run:
            mark_sync_state_in_progress(db, table_name="tmdb_show_entities", show_id=show_id)

        try:
            fetched_at = _now_utc_iso()
            details = fetch_tv_details(tmdb_id, api_key=api_key)
            network_rows = _extract_network_rows(details, fetched_at=fetched_at)
            company_rows = _extract_company_rows(details, fetched_at=fetched_at)

            if network_rows and not args.dry_run:
                total_networks_upserted += len(_upsert_rows(db, table="networks", rows=network_rows, on_conflict="id"))
            elif network_rows:
                total_networks_upserted += len(network_rows)

            if company_rows and not args.dry_run:
                total_companies_upserted += len(
                    _upsert_rows(db, table="production_companies", rows=company_rows, on_conflict="id")
                )
            elif company_rows:
                total_companies_upserted += len(company_rows)

            patch: dict[str, Any] = {}
            network_names = [row["name"] for row in network_rows if row.get("name")]
            network_ids = [row["id"] for row in network_rows if isinstance(row.get("id"), int)]
            company_ids = [row["id"] for row in company_rows if isinstance(row.get("id"), int)]

            merged_networks = _merge_str_arrays(show.get("networks"), network_names)
            if merged_networks is not None:
                patch["networks"] = merged_networks

            merged_network_ids = _merge_int_arrays(show.get("tmdb_network_ids"), network_ids)
            if merged_network_ids is not None:
                patch["tmdb_network_ids"] = merged_network_ids

            merged_company_ids = _merge_int_arrays(show.get("tmdb_production_company_ids"), company_ids)
            if merged_company_ids is not None:
                patch["tmdb_production_company_ids"] = merged_company_ids

            if patch:
                total_show_updates += 1
                if args.dry_run:
                    if args.verbose:
                        print(f"UPDATE show id={show_id} patch_keys={sorted(patch.keys())}")
                else:
                    update_show(db, show_id, patch)

            if not args.skip_s3 and s3_client is not None:
                total_logos_mirrored += _mirror_logo_rows(
                    db,
                    table="networks",
                    kind="networks",
                    ids=network_ids,
                    processed=processed_network_ids,
                    dry_run=args.dry_run,
                    force=bool(args.force),
                    s3_client=s3_client,
                )
                total_logos_mirrored += _mirror_logo_rows(
                    db,
                    table="production_companies",
                    kind="production-companies",
                    ids=company_ids,
                    processed=processed_company_ids,
                    dry_run=args.dry_run,
                    force=bool(args.force),
                    s3_client=s3_client,
                )

            if not args.dry_run:
                mark_sync_state_success(
                    db,
                    table_name="tmdb_show_entities",
                    show_id=show_id,
                    last_seen_most_recent_episode=extract_most_recent_episode(show),
                )
        except (TmdbClientError, RuntimeError, ValueError) as exc:
            failures.append(f"{tmdb_id}: {exc}")
            if not args.dry_run:
                mark_sync_state_failed(db, table_name="tmdb_show_entities", show_id=show_id, error=exc)

    print("Summary")
    print(f"shows_processed={len(show_rows)}")
    print(f"networks_upserted={total_networks_upserted}")
    print(f"production_companies_upserted={total_companies_upserted}")
    print(f"show_updates={total_show_updates}")
    print(f"logos_mirrored={total_logos_mirrored}")
    print(f"failures={len(failures)}")

    if failures:
        for failure in failures[:10]:
            print(f"- {failure}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
