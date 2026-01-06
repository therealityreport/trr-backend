#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from typing import Any, Mapping

from trr_backend.integrations.tmdb.client import TmdbClientError, fetch_tv_season_details, resolve_api_key
from trr_backend.media.s3_mirror import (
    get_cdn_base_url,
    get_s3_client,
    mirror_season_image_row,
    prune_orphaned_season_image_objects,
)
from trr_backend.repositories.season_images import (
    assert_core_season_images_table_exists,
    fetch_season_images_missing_hosted,
    update_season_image_hosted_fields,
    upsert_season_images,
)

from scripts._sync_common import add_show_filter_args, fetch_show_rows, load_env_and_db


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="sync_season_episode_images",
        description="Sync season posters (TMDb) and mirror to S3.",
    )
    add_show_filter_args(parser)
    parser.add_argument("--no-s3", action="store_true", help="Skip S3 mirroring.")
    parser.add_argument("--no-prune", action="store_true", help="Skip S3 prune step.")
    parser.add_argument("--force", action="store_true", help="Re-download and re-upload hosted images.")
    parser.add_argument(
        "--mirror-limit",
        type=int,
        default=200,
        help="Max rows to mirror per show (default: 200).",
    )
    return parser.parse_args(argv)


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _fetch_show_seasons(db, show_id: str) -> list[dict[str, Any]]:
    response = (
        db.schema("core")
        .table("seasons")
        .select("id,season_number")
        .eq("show_id", show_id)
        .execute()
    )
    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Supabase error listing seasons: {response.error}")
    data = response.data or []
    return data if isinstance(data, list) else []


def _extract_posters(
    payload: Mapping[str, Any],
    *,
    show_id: str,
    season_id: str,
    season_number: int,
    tmdb_id: int,
    fetched_at: str,
) -> list[dict[str, Any]]:
    images_obj = payload.get("images")
    images_map = images_obj if isinstance(images_obj, Mapping) else {}
    posters = images_map.get("posters")
    poster_rows: list[dict[str, Any]] = []
    if not isinstance(posters, list):
        return poster_rows

    for poster in posters:
        if not isinstance(poster, Mapping):
            continue
        file_path = poster.get("file_path")
        if not isinstance(file_path, str) or not file_path.strip():
            continue
        width = poster.get("width")
        height = poster.get("height")
        if not isinstance(width, int) or not isinstance(height, int):
            continue
        aspect_ratio = poster.get("aspect_ratio")
        if isinstance(aspect_ratio, (int, float)):
            aspect_ratio_val: float = float(aspect_ratio)
        elif height > 0:
            aspect_ratio_val = float(width) / float(height)
        else:
            aspect_ratio_val = 0.0

        poster_rows.append(
            {
                "show_id": show_id,
                "season_id": season_id,
                "tmdb_series_id": int(tmdb_id),
                "season_number": int(season_number),
                "source": "tmdb",
                "kind": "poster",
                "iso_639_1": poster.get("iso_639_1") if isinstance(poster.get("iso_639_1"), str) else None,
                "file_path": file_path,
                "width": int(width),
                "height": int(height),
                "aspect_ratio": aspect_ratio_val,
                "fetched_at": fetched_at,
            }
        )

    return poster_rows


def _episode_images_table_exists(db) -> bool:
    try:
        response = db.schema("core").table("episode_images").select("id").limit(1).execute()
    except Exception:
        return False
    if hasattr(response, "error") and response.error:
        return False
    return True


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    db = load_env_and_db()
    assert_core_season_images_table_exists(db)

    api_key = resolve_api_key() or None
    if not api_key:
        raise RuntimeError("TMDB_API_KEY is required for season image sync.")

    if _episode_images_table_exists(db) and args.verbose:
        print("INFO: core.episode_images detected; episode still sync is not implemented yet.")

    show_rows = fetch_show_rows(db, args)
    if not show_rows:
        print("No shows matched the filters.")
        return 0

    total_posters = 0
    total_mirrored = 0
    total_failed = 0

    s3_client = None
    cdn_base_url = None
    if not args.no_s3 and not args.dry_run:
        s3_client = get_s3_client()
        cdn_base_url = None if args.force else get_cdn_base_url()

    for show in show_rows:
        show_id = str(show.get("id") or "").strip()
        tmdb_id = show.get("tmdb_id")
        imdb_id = str(show.get("imdb_id") or "").strip()
        if not show_id or not isinstance(tmdb_id, int):
            continue

        try:
            seasons = _fetch_show_seasons(db, show_id)
        except Exception as exc:  # noqa: BLE001
            total_failed += 1
            if args.verbose:
                print(f"WARN {show_id}: failed to list seasons: {exc}")
            continue

        for season in seasons:
            season_id = str(season.get("id") or "").strip()
            season_number = season.get("season_number")
            if not season_id or not isinstance(season_number, int):
                continue

            fetched_at = _now_utc_iso()
            try:
                payload = fetch_tv_season_details(
                    tmdb_id,
                    int(season_number),
                    api_key=api_key,
                    append_to_response=["images"],
                    include_image_language="en,null",
                )
                poster_rows = _extract_posters(
                    payload,
                    show_id=show_id,
                    season_id=season_id,
                    season_number=int(season_number),
                    tmdb_id=int(tmdb_id),
                    fetched_at=fetched_at,
                )
            except (TmdbClientError, RuntimeError, ValueError) as exc:
                total_failed += 1
                if args.verbose:
                    print(f"WARN {show_id} season={season_number}: {exc}")
                continue

            if poster_rows:
                total_posters += len(poster_rows)
                if not args.dry_run:
                    upsert_season_images(db, poster_rows)

        if args.no_s3 or args.dry_run or s3_client is None:
            continue

        rows = fetch_season_images_missing_hosted(
            db,
            show_id=show_id,
            limit=int(args.mirror_limit),
            include_hosted=True,
            cdn_base_url=cdn_base_url,
        )
        for row in rows:
            patch = mirror_season_image_row(row, force=bool(args.force), s3_client=s3_client)
            if not patch:
                continue
            update_season_image_hosted_fields(db, str(row.get("id")), patch)
            total_mirrored += 1

        if not args.no_prune and not args.force:
            show_identifier = imdb_id or show_id
            if not show_identifier:
                continue
            prune_orphaned_season_image_objects(
                db,
                show_identifier,
                show_id=show_id,
                dry_run=bool(args.dry_run),
                verbose=bool(args.verbose),
                s3_client=s3_client,
            )

    if args.verbose:
        print(f"posters_upserted={total_posters}")
        print(f"posters_mirrored={total_mirrored}")
        print(f"failed={total_failed}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
