#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
from typing import Any

from dotenv import load_dotenv

from trr_backend.ingestion.show_importer import (
    collect_candidates_from_lists,
    parse_imdb_headers_json_env,
    upsert_candidates_into_supabase,
)
from trr_backend.integrations.imdb.episodic_client import IMDB_JOB_CATEGORY_SELF


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="import_shows_from_lists.py",
        description="Import shows from IMDb and TMDb lists into core.shows.",
    )
    parser.add_argument("--imdb-list", action="append", default=[], help="IMDb list URL (repeatable).")
    parser.add_argument("--tmdb-list", action="append", default=[], help="TMDb list id or URL (repeatable).")
    parser.add_argument("--config", type=str, default=None, help="JSON/YAML config file of list sources.")
    parser.add_argument("--dry-run", action="store_true", help="Print planned changes without writing to Supabase.")
    parser.add_argument(
        "--max-shows",
        type=int,
        default=None,
        help="Optional cap on number of merged candidate shows to process (for quick smoke runs).",
    )
    imdb_graphql = parser.add_mutually_exclusive_group()
    imdb_graphql.add_argument(
        "--imdb-use-graphql",
        dest="imdb_use_graphql",
        action="store_true",
        default=True,
        help="Use IMDb GraphQL TitleListMainPage for list ingestion (default).",
    )
    imdb_graphql.add_argument(
        "--imdb-no-graphql",
        dest="imdb_use_graphql",
        action="store_false",
        help="Disable IMDb GraphQL and use HTML/JSON-LD scraping (debugging only).",
    )
    imdb_episodes = parser.add_mutually_exclusive_group()
    imdb_episodes.add_argument(
        "--imdb-fetch-episodes",
        dest="imdb_fetch_episodes",
        action="store_true",
        default=False,
        help="Fetch IMDb /title/{id}/episodes season pages and persist core.seasons/core.episodes (default: off).",
    )
    imdb_episodes.add_argument(
        "--imdb-no-episodes",
        dest="imdb_fetch_episodes",
        action="store_false",
        help="Skip IMDb episodes ingestion (default).",
    )
    parser.add_argument(
        "--imdb-refresh-episodes",
        action="store_true",
        help="When fetching IMDb episodes, delete existing episode rows for each show_id before upserting.",
    )
    imdb_cast = parser.add_mutually_exclusive_group()
    imdb_cast.add_argument(
        "--imdb-fetch-cast",
        dest="imdb_fetch_cast",
        action="store_true",
        default=False,
        help="Fetch IMDb credits and populate core.people/core.cast_memberships (default: off).",
    )
    imdb_cast.add_argument(
        "--imdb-no-cast",
        dest="imdb_fetch_cast",
        action="store_false",
        help="Skip IMDb cast ingestion (default).",
    )
    parser.add_argument(
        "--imdb-refresh-cast",
        action="store_true",
        help="When fetching IMDb cast, delete existing cast memberships for each show_id before upserting.",
    )
    parser.add_argument(
        "--imdb-cast-overrides-url",
        type=str,
        default=None,
        help="Optional override URL for published ShowInfo OVERRIDE sheet (CSV or pubhtml).",
    )
    tmdb_details = parser.add_mutually_exclusive_group()
    tmdb_details.add_argument(
        "--tmdb-fetch-details",
        dest="tmdb_fetch_details",
        action="store_true",
        default=True,
        help="Fetch TMDb /tv/{id} details during list ingestion (default).",
    )
    tmdb_details.add_argument(
        "--tmdb-no-details",
        dest="tmdb_fetch_details",
        action="store_false",
        help="Skip fetching TMDb /tv/{id} details during list ingestion (faster).",
    )
    tmdb_images = parser.add_mutually_exclusive_group()
    tmdb_images.add_argument(
        "--tmdb-fetch-images",
        dest="tmdb_fetch_images",
        action="store_true",
        default=False,
        help="Fetch TMDb /tv/{id}/images and persist posters/logos/backdrops (default: off).",
    )
    tmdb_images.add_argument(
        "--tmdb-no-images",
        dest="tmdb_fetch_images",
        action="store_false",
        help="Skip TMDb /tv/{id}/images fetch (default).",
    )
    parser.add_argument(
        "--tmdb-refresh-images",
        action="store_true",
        help="When fetching TMDb images, delete existing TMDb image rows for each tmdb_id before upserting.",
    )
    tmdb_seasons = parser.add_mutually_exclusive_group()
    tmdb_seasons.add_argument(
        "--tmdb-fetch-seasons",
        dest="tmdb_fetch_seasons",
        action="store_true",
        default=False,
        help="Fetch TMDb /tv/{id}/season/{n} details+external_ids+images and enrich seasons/episodes (default: off).",
    )
    tmdb_seasons.add_argument(
        "--tmdb-no-seasons",
        dest="tmdb_fetch_seasons",
        action="store_false",
        help="Skip TMDb season enrichment (default).",
    )
    parser.add_argument(
        "--tmdb-refresh-seasons",
        action="store_true",
        help="When fetching TMDb seasons, delete existing season/episode/season_image rows for each tmdb_id before upserting.",
    )
    parser.add_argument(
        "--tmdb-details-max-age-days",
        type=int,
        default=90,
        help="Refetch TMDb /tv/{id} details when missing or older than this many days (0 forces refresh).",
    )
    parser.add_argument(
        "--tmdb-details-refresh",
        action="store_true",
        help="Force refetch TMDb /tv/{id} details (equivalent to --tmdb-details-max-age-days 0).",
    )
    parser.add_argument(
        "--skip-tmdb-external-ids",
        action="store_true",
        help="Skip per-show TMDb /external_ids lookups (faster but no IMDb ids for TMDb list items).",
    )
    parser.add_argument(
        "--annotate-imdb-episodic",
        action="store_true",
        help="Annotate shows with IMDb episodic support flags (and optionally probe seasons if configured).",
    )
    parser.add_argument(
        "--enrich-show-metadata",
        action="store_true",
        help="After upsert, enrich core.shows.external_ids.show_meta using TMDb/IMDb.",
    )
    parser.add_argument(
        "--region",
        type=str,
        default="US",
        help="Region code for TMDb watch providers (default: US).",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=5,
        help="Parallelism for metadata fetch (default: 5).",
    )
    parser.add_argument(
        "--max-enrich",
        type=int,
        default=None,
        help="Optional cap on number of shows to enrich (for quick runs).",
    )
    parser.add_argument(
        "--force-refresh",
        action="store_true",
        help="Ignore existing external_ids.show_meta.fetched_at and refetch.",
    )
    parser.add_argument(
        "--imdb-sleep-ms",
        type=int,
        default=0,
        help="Optional delay in ms between IMDb enrichment requests (default: 0).",
    )
    return parser.parse_args(argv)


def _load_config(path: str) -> dict[str, Any]:
    cfg_path = Path(path)
    raw = cfg_path.read_text(encoding="utf-8")

    if cfg_path.suffix.lower() in {".yml", ".yaml"}:
        try:
            import yaml  # type: ignore
        except ImportError as exc:
            raise RuntimeError("YAML config requires `pyyaml` to be installed.") from exc
        loaded = yaml.safe_load(raw)
    else:
        loaded = json.loads(raw)

    if not isinstance(loaded, dict):
        raise ValueError("Config must be a JSON/YAML object.")
    return loaded


def run_from_cli(args: argparse.Namespace) -> None:
    load_dotenv()

    imdb_lists: list[str] = list(args.imdb_list or [])
    tmdb_lists: list[str] = list(args.tmdb_list or [])

    if args.config:
        cfg = _load_config(args.config)
        imdb_lists.extend([str(u) for u in cfg.get("imdb_lists", [])])
        tmdb_lists.extend([str(u) for u in cfg.get("tmdb_lists", [])])

    imdb_lists = [u for u in imdb_lists if str(u).strip()]
    tmdb_lists = [u for u in tmdb_lists if str(u).strip()]

    if not imdb_lists:
        imdb_default = (os.getenv("IMDB_LIST_URL") or "").strip()
        if imdb_default:
            imdb_lists = [imdb_default]
            print(f"Using IMDB_LIST_URL from .env ({imdb_default})")
    if not tmdb_lists:
        tmdb_default = (os.getenv("TMDB_LIST_ID") or "").strip()
        if tmdb_default:
            tmdb_lists = [tmdb_default]
            print(f"Using TMDB_LIST_ID from .env ({tmdb_default})")

    if not imdb_lists and not tmdb_lists:
        raise SystemExit("No list sources provided. Use --imdb-list/--tmdb-list and/or --config.")

    candidates = collect_candidates_from_lists(
        imdb_list_urls=imdb_lists,
        tmdb_lists=tmdb_lists,
        resolve_tmdb_external_ids=not bool(args.skip_tmdb_external_ids),
        imdb_use_graphql=bool(args.imdb_use_graphql),
    )
    print(f"Collected {len(candidates)} merged candidate shows.")
    if args.skip_tmdb_external_ids and imdb_lists and tmdb_lists:
        print(
            "Note: --skip-tmdb-external-ids disables IMDb id resolution for TMDb list items, "
            "so overlap between lists cannot be merged during candidate collection.",
            file=sys.stderr,
        )

    if args.max_shows is not None:
        limit = max(0, int(args.max_shows))
        if limit and len(candidates) > limit:
            print(f"Limiting run to first {limit} candidate shows (out of {len(candidates)}).", file=sys.stderr)
            candidates = candidates[:limit]

    imdb_probe_name_id = (os.getenv("IMDB_EPISODIC_PROBE_NAME_ID") or "").strip() or None
    imdb_probe_job_category_id = (
        (os.getenv("IMDB_EPISODIC_PROBE_JOB_CATEGORY_ID") or "").strip() or IMDB_JOB_CATEGORY_SELF
    )
    imdb_extra_headers = parse_imdb_headers_json_env()

    result = upsert_candidates_into_supabase(
        candidates,
        dry_run=bool(args.dry_run),
        annotate_imdb_episodic=bool(args.annotate_imdb_episodic),
        tmdb_fetch_details=bool(args.tmdb_fetch_details),
        tmdb_details_max_age_days=0 if bool(args.tmdb_details_refresh) else int(args.tmdb_details_max_age_days or 0),
        tmdb_fetch_images=bool(getattr(args, "tmdb_fetch_images", False)),
        tmdb_refresh_images=bool(getattr(args, "tmdb_refresh_images", False)),
        imdb_fetch_episodes=bool(getattr(args, "imdb_fetch_episodes", False)),
        imdb_refresh_episodes=bool(getattr(args, "imdb_refresh_episodes", False)),
        imdb_fetch_cast=bool(getattr(args, "imdb_fetch_cast", False)),
        imdb_refresh_cast=bool(getattr(args, "imdb_refresh_cast", False)),
        imdb_cast_overrides_url=str(args.imdb_cast_overrides_url or "") or None,
        tmdb_fetch_seasons=bool(getattr(args, "tmdb_fetch_seasons", False)),
        tmdb_refresh_seasons=bool(getattr(args, "tmdb_refresh_seasons", False)),
        enrich_show_metadata=bool(args.enrich_show_metadata),
        enrich_region=str(args.region or "US").upper(),
        enrich_concurrency=int(args.concurrency or 5),
        enrich_max_enrich=int(args.max_enrich) if args.max_enrich is not None else None,
        enrich_force_refresh=bool(args.force_refresh),
        enrich_imdb_sleep_ms=int(args.imdb_sleep_ms or 0),
        imdb_episodic_probe_name_id=imdb_probe_name_id if args.annotate_imdb_episodic else None,
        imdb_episodic_probe_job_category_id=imdb_probe_job_category_id,
        imdb_episodic_extra_headers=imdb_extra_headers,
    )

    print(f"Done. created={result.created} updated={result.updated} skipped={result.skipped}")


def main(argv: list[str]) -> int:
    args = _parse_args(argv)
    run_from_cli(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(list(sys.argv[1:])))
