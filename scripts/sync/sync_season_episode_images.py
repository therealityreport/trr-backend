#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from scripts._sync_common import add_show_filter_args, fetch_show_rows, load_env_and_db
from trr_backend.integrations.tmdb.client import (
    TmdbClientError,
    fetch_tv_episode_images,
    fetch_tv_season_images,
    resolve_api_key,
)
from trr_backend.media.s3_mirror import (
    get_cdn_base_url,
    get_s3_client,
    mirror_episode_image_row,
    mirror_season_image_row,
    prune_orphaned_season_image_objects,
)
from trr_backend.repositories.episode_images import (
    assert_core_episode_images_table_exists,
    fetch_episode_images_missing_hosted,
    update_episode_image_hosted_fields,
    upsert_episode_images,
)
from trr_backend.repositories.season_images import (
    assert_core_season_images_table_exists,
    fetch_season_images_missing_hosted,
    update_season_image_hosted_fields,
    upsert_season_images,
)


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="sync_season_episode_images",
        description="Sync season posters (TMDb) and mirror to S3.",
    )
    add_show_filter_args(parser)
    parser.add_argument("--no-s3", action="store_true", help="Skip S3 mirroring.")
    parser.add_argument("--no-prune", action="store_true", help="Skip S3 prune step.")
    # --force is already defined in add_show_filter_args
    parser.add_argument(
        "--mirror-limit",
        type=int,
        default=200,
        help="Max rows to mirror per show (default: 200).",
    )
    return parser.parse_args(argv)


def _now_utc_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _fetch_show_seasons(db, show_id: str) -> list[dict[str, Any]]:
    response = db.schema("core").table("seasons").select("id,season_number").eq("show_id", show_id).execute()
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
    """Extract posters from the dedicated /tv/{id}/season/{n}/images endpoint response."""
    # The dedicated images endpoint returns posters directly at top level
    posters = payload.get("posters")
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
                "url": f"https://image.tmdb.org/t/p/original{file_path}",
                "source_image_id": file_path,
                "width": int(width),
                "height": int(height),
                "aspect_ratio": aspect_ratio_val,
                "fetched_at": fetched_at,
            }
        )

    return poster_rows


def _fetch_season_episodes(db, season_id: str) -> list[dict[str, Any]]:
    """Fetch all episodes for a season from the database."""
    response = (
        db.schema("core")
        .table("episodes")
        .select("id,episode_number,external_ids")
        .eq("season_id", season_id)
        .order("episode_number")
        .execute()
    )
    if hasattr(response, "error") and response.error:
        raise RuntimeError(f"Supabase error listing episodes: {response.error}")
    data = response.data or []
    return data if isinstance(data, list) else []


def _extract_episode_stills(
    payload: Mapping[str, Any],
    *,
    show_id: str,
    season_id: str,
    episode_id: str,
    season_number: int,
    episode_number: int,
    tmdb_id: int,
    fetched_at: str,
) -> list[dict[str, Any]]:
    """Extract stills from the dedicated /tv/{id}/season/{n}/episode/{e}/images endpoint response."""
    stills = payload.get("stills")
    still_rows: list[dict[str, Any]] = []
    if not isinstance(stills, list):
        return still_rows

    for still in stills:
        if not isinstance(still, Mapping):
            continue
        file_path = still.get("file_path")
        if not isinstance(file_path, str) or not file_path.strip():
            continue
        width = still.get("width")
        height = still.get("height")
        if not isinstance(width, int) or not isinstance(height, int):
            continue
        aspect_ratio = still.get("aspect_ratio")
        if isinstance(aspect_ratio, (int, float)):
            aspect_ratio_val: float = float(aspect_ratio)
        elif height > 0:
            aspect_ratio_val = float(width) / float(height)
        else:
            aspect_ratio_val = 0.0

        still_rows.append(
            {
                "show_id": show_id,
                "season_id": season_id,
                "episode_id": episode_id,
                "tmdb_series_id": int(tmdb_id),
                "season_number": int(season_number),
                "episode_number": int(episode_number),
                "source": "tmdb",
                "kind": "still",
                "iso_639_1": still.get("iso_639_1") if isinstance(still.get("iso_639_1"), str) else None,
                "file_path": file_path,
                "url": f"https://image.tmdb.org/t/p/original{file_path}",
                "source_image_id": file_path,
                "width": int(width),
                "height": int(height),
                "aspect_ratio": aspect_ratio_val,
                "fetched_at": fetched_at,
            }
        )

    return still_rows


def _episode_images_table_exists(db) -> bool:
    try:
        assert_core_episode_images_table_exists(db)
        return True
    except Exception:
        return False


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    if args.skip_db:
        print("ERROR: --skip-db is not supported for this script (database access required).", file=sys.stderr)
        return 2
    db = load_env_and_db()
    assert_core_season_images_table_exists(db)

    api_key = resolve_api_key() or None
    if not api_key:
        raise RuntimeError("TMDB_API_KEY is required for season image sync.")

    # Check if episode_images table exists
    episode_images_enabled = _episode_images_table_exists(db)
    if episode_images_enabled and args.verbose:
        print("INFO: core.episode_images detected; syncing episode stills.")

    show_rows = fetch_show_rows(db, args)
    if not show_rows:
        print("No shows matched the filters.")
        return 0

    total_posters = 0
    total_stills = 0
    total_mirrored = 0
    total_episode_mirrored = 0
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

            # Fetch season images using dedicated endpoint
            try:
                season_images_payload = fetch_tv_season_images(
                    tmdb_id,
                    int(season_number),
                    api_key=api_key,
                    include_image_language="en,null",
                )
                poster_rows = _extract_posters(
                    season_images_payload,
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
                poster_rows = []

            if poster_rows:
                total_posters += len(poster_rows)
                if not args.dry_run:
                    upsert_season_images(db, poster_rows)

            # Fetch episode images if table exists
            if episode_images_enabled:
                try:
                    episodes = _fetch_season_episodes(db, season_id)
                except Exception as exc:  # noqa: BLE001
                    if args.verbose:
                        print(f"WARN {show_id} season={season_number}: failed to list episodes: {exc}")
                    episodes = []

                for episode in episodes:
                    episode_id = str(episode.get("id") or "").strip()
                    episode_number = episode.get("episode_number")
                    if not episode_id or not isinstance(episode_number, int):
                        continue

                    try:
                        episode_images_payload = fetch_tv_episode_images(
                            tmdb_id,
                            int(season_number),
                            int(episode_number),
                            api_key=api_key,
                            include_image_language="en,null",
                        )
                        still_rows = _extract_episode_stills(
                            episode_images_payload,
                            show_id=show_id,
                            season_id=season_id,
                            episode_id=episode_id,
                            season_number=int(season_number),
                            episode_number=int(episode_number),
                            tmdb_id=int(tmdb_id),
                            fetched_at=fetched_at,
                        )
                    except (TmdbClientError, RuntimeError, ValueError) as exc:
                        total_failed += 1
                        if args.verbose:
                            print(f"WARN {show_id} S{season_number}E{episode_number}: {exc}")
                        continue

                    if still_rows:
                        total_stills += len(still_rows)
                        if not args.dry_run:
                            upsert_episode_images(db, still_rows)

        if args.no_s3 or args.dry_run or s3_client is None:
            continue

        # Mirror season images to S3
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

        # Mirror episode images to S3
        if episode_images_enabled:
            episode_rows = fetch_episode_images_missing_hosted(
                db,
                show_id=show_id,
                limit=int(args.mirror_limit),
                include_hosted=True,
                cdn_base_url=cdn_base_url,
            )
            for row in episode_rows:
                patch = mirror_episode_image_row(row, force=bool(args.force), s3_client=s3_client)
                if not patch:
                    continue
                update_episode_image_hosted_fields(db, str(row.get("id")), patch)
                total_episode_mirrored += 1

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
        print(f"season_posters_upserted={total_posters}")
        print(f"episode_stills_upserted={total_stills}")
        print(f"season_images_mirrored={total_mirrored}")
        print(f"episode_images_mirrored={total_episode_mirrored}")
        print(f"failed={total_failed}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
