#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from scripts._sync_common import (
    add_show_filter_args,
    extract_most_recent_episode,
    filter_show_rows_for_sync,
    load_env_and_db,
)
from supabase import Client
from trr_backend.integrations.tmdb.client import TmdbClientError, fetch_tv_watch_providers, resolve_api_key
from trr_backend.media.s3_mirror import mirror_tmdb_logo_row
from trr_backend.repositories.sync_state import (
    assert_core_sync_state_table_exists,
    mark_sync_state_failed,
    mark_sync_state_in_progress,
    mark_sync_state_success,
)
from trr_backend.utils.env import load_env

PROVIDER_FIELDS = (
    "provider_id,provider_name,display_priority,tmdb_logo_path,logo_path,hosted_logo_key,hosted_logo_url,hosted_logo_sha256,"
    "hosted_logo_content_type,hosted_logo_bytes,hosted_logo_etag,hosted_logo_at"
)


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="sync_tmdb_watch_providers",
        description="Sync TMDb watch providers into core tables and update show/provider associations.",
    )
    add_show_filter_args(parser)
    parser.add_argument("--skip-s3", action="store_true", help="Skip S3 logo mirroring.")
    return parser.parse_args(argv)


def _require_supabase_db_url() -> None:
    if not (os.getenv("SUPABASE_DB_URL") or "").strip():
        raise RuntimeError("SUPABASE_DB_URL must be set for TMDb watch provider sync scripts.")


def _now_utc_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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
    fields = "id,name,tmdb_id,most_recent_episode"
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


def _parse_watch_providers_payload(
    payload: Mapping[str, Any],
    *,
    show_id: str,
    fetched_at: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[tuple[str, str], set[int]]]:
    results = payload.get("results")
    if not isinstance(results, Mapping):
        return [], [], {}

    provider_rows_by_id: dict[int, dict[str, Any]] = {}
    show_provider_rows: list[dict[str, Any]] = []
    ids_by_group: dict[tuple[str, str], set[int]] = {}

    for region, region_block in results.items():
        if not isinstance(region, str) or not isinstance(region_block, Mapping):
            continue
        link = region_block.get("link") if isinstance(region_block.get("link"), str) else None
        for offer_type, providers in region_block.items():
            if offer_type == "link":
                continue
            if not isinstance(offer_type, str) or not isinstance(providers, list):
                continue
            for provider in providers:
                if not isinstance(provider, Mapping):
                    continue
                provider_id = provider.get("provider_id")
                provider_name = provider.get("provider_name")
                if not isinstance(provider_id, int) or not isinstance(provider_name, str):
                    continue
                provider_rows_by_id[provider_id] = {
                    "provider_id": provider_id,
                    "provider_name": provider_name.strip(),
                    "display_priority": provider.get("display_priority"),
                    "tmdb_logo_path": provider.get("logo_path"),
                    "tmdb_meta": dict(provider),
                    "tmdb_fetched_at": fetched_at,
                }
                show_provider_rows.append(
                    {
                        "show_id": show_id,
                        "region": region,
                        "offer_type": offer_type,
                        "provider_id": provider_id,
                        "display_priority": provider.get("display_priority"),
                        "link": link,
                        "fetched_at": fetched_at,
                    }
                )
                ids_by_group.setdefault((region, offer_type), set()).add(provider_id)

    return list(provider_rows_by_id.values()), show_provider_rows, ids_by_group


def _compute_stale_provider_ids(existing_ids: set[int], current_ids: set[int]) -> list[int]:
    return sorted(existing_ids - current_ids)


def _upsert_rows(db: Client, *, table: str, rows: list[dict[str, Any]], on_conflict: str) -> None:
    if not rows:
        return
    response = db.schema("core").table(table).upsert(rows, on_conflict=on_conflict).execute()
    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Supabase error upserting {table}: {response.error}")


def _fetch_existing_provider_ids(
    db: Client,
    *,
    show_id: str,
    region: str,
    offer_type: str,
) -> set[int]:
    response = (
        db.schema("core")
        .table("show_watch_providers")
        .select("provider_id")
        .eq("show_id", show_id)
        .eq("region", region)
        .eq("offer_type", offer_type)
        .execute()
    )
    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Supabase error fetching watch providers: {response.error}")
    data = response.data or []
    if not isinstance(data, list):
        return set()
    return {row.get("provider_id") for row in data if isinstance(row.get("provider_id"), int)}


def _prune_stale_watch_providers(
    db: Client,
    *,
    show_id: str,
    region: str,
    offer_type: str,
    current_ids: set[int],
) -> int:
    existing_ids = _fetch_existing_provider_ids(db, show_id=show_id, region=region, offer_type=offer_type)
    stale_ids = _compute_stale_provider_ids(existing_ids, current_ids)
    if not stale_ids:
        return 0
    response = (
        db.schema("core")
        .table("show_watch_providers")
        .delete()
        .eq("show_id", show_id)
        .eq("region", region)
        .eq("offer_type", offer_type)
        .in_("provider_id", stale_ids)
        .execute()
    )
    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Supabase error pruning watch providers: {response.error}")
    return len(stale_ids)


def _fetch_providers_for_logo(db: Client, *, ids: list[int]) -> list[dict[str, Any]]:
    if not ids:
        return []
    response = db.schema("core").table("watch_providers").select(PROVIDER_FIELDS).in_("provider_id", ids).execute()
    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Supabase error fetching watch providers: {response.error}")
    data = response.data or []
    return data if isinstance(data, list) else []


def _mirror_provider_logos(
    db: Client,
    *,
    ids: list[int],
    processed: set[int],
    dry_run: bool,
    force: bool,
    s3_client,
) -> int:
    to_process = [provider_id for provider_id in ids if provider_id not in processed]
    if not to_process:
        return 0
    processed.update(to_process)
    rows = _fetch_providers_for_logo(db, ids=to_process)
    mirrored = 0
    for row in rows:
        patch = mirror_tmdb_logo_row(
            row,
            kind="watch-providers",
            id_field="provider_id",
            logo_path_field="tmdb_logo_path",
            force=force,
            s3_client=s3_client,
        )
        if not patch:
            continue
        mirrored += 1
        if dry_run:
            continue
        response = (
            db.schema("core").table("watch_providers").update(patch).eq("provider_id", row.get("provider_id")).execute()
        )
        if hasattr(response, "error") and response.error:
            raise RuntimeError(f"Supabase error updating watch provider logo: {response.error}")
    return mirrored


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    load_env()
    _require_supabase_db_url()

    api_key = resolve_api_key() or None
    if not api_key:
        raise RuntimeError("TMDB_API_KEY is required for TMDb watch provider sync.")

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
        table_name="tmdb_watch_providers",
        incremental=bool(args.incremental),
        resume=bool(args.resume),
        force=bool(args.force),
        since=args.since,
        check_total_seasons=False,
        verbose=bool(args.verbose),
    )
    show_rows = filter_result.selected
    if not show_rows:
        print("No shows need TMDb watch provider sync.")
        return 0

    processed_provider_ids: set[int] = set()
    s3_client = None
    if not args.skip_s3 and not args.dry_run:
        from trr_backend.media.s3_mirror import get_s3_client

        s3_client = get_s3_client()

    total_providers_upserted = 0
    total_links_upserted = 0
    total_pruned = 0
    total_logos_mirrored = 0
    failures: list[str] = []

    for show in show_rows:
        show_id = str(show.get("id") or "").strip()
        tmdb_id = show.get("tmdb_id")
        if not show_id or not isinstance(tmdb_id, int):
            if args.verbose:
                print(f"SKIP show id={show.get('id')} (missing tmdb_id)")
            continue

        if not args.dry_run:
            mark_sync_state_in_progress(db, table_name="tmdb_watch_providers", show_id=show_id)

        fetched_at = _now_utc_iso()
        try:
            payload = fetch_tv_watch_providers(tmdb_id, api_key=api_key)
            provider_rows, show_provider_rows, ids_by_group = _parse_watch_providers_payload(
                payload,
                show_id=show_id,
                fetched_at=fetched_at,
            )

            if provider_rows:
                total_providers_upserted += len(provider_rows)
                if not args.dry_run:
                    _upsert_rows(
                        db,
                        table="watch_providers",
                        rows=provider_rows,
                        on_conflict="provider_id",
                    )

            if show_provider_rows:
                total_links_upserted += len(show_provider_rows)
                if not args.dry_run:
                    _upsert_rows(
                        db,
                        table="show_watch_providers",
                        rows=show_provider_rows,
                        on_conflict="show_id,region,offer_type,provider_id",
                    )

            if not args.dry_run:
                for (region, offer_type), provider_ids in ids_by_group.items():
                    total_pruned += _prune_stale_watch_providers(
                        db,
                        show_id=show_id,
                        region=region,
                        offer_type=offer_type,
                        current_ids=provider_ids,
                    )

            if not args.skip_s3 and s3_client is not None:
                provider_ids = [row["provider_id"] for row in provider_rows if isinstance(row.get("provider_id"), int)]
                total_logos_mirrored += _mirror_provider_logos(
                    db,
                    ids=provider_ids,
                    processed=processed_provider_ids,
                    dry_run=args.dry_run,
                    force=bool(args.force),
                    s3_client=s3_client,
                )

            if not args.dry_run:
                mark_sync_state_success(
                    db,
                    table_name="tmdb_watch_providers",
                    show_id=show_id,
                    last_seen_most_recent_episode=extract_most_recent_episode(show),
                )
        except (TmdbClientError, RuntimeError, ValueError) as exc:
            failures.append(f"{tmdb_id}: {exc}")
            if not args.dry_run:
                mark_sync_state_failed(db, table_name="tmdb_watch_providers", show_id=show_id, error=exc)

    print("Summary")
    print(f"shows_processed={len(show_rows)}")
    print(f"providers_upserted={total_providers_upserted}")
    print(f"show_watch_providers_upserted={total_links_upserted}")
    print(f"stale_rows_pruned={total_pruned}")
    print(f"logos_mirrored={total_logos_mirrored}")
    print(f"failures={len(failures)}")

    if failures:
        for failure in failures[:10]:
            print(f"- {failure}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
