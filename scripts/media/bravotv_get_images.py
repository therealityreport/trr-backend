#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="bravotv_get_images",
        description="Run the BRAVOTV multi-source image pipeline with cloud-first acquisition.",
    )
    parser.add_argument("--person", type=str, default=None, help="Person name for Person Run mode.")
    parser.add_argument("--show", type=str, default=None, help="Show name for Show Run mode.")
    parser.add_argument("--season", type=int, default=None, help="Optional season number for Show Run mode.")
    parser.add_argument("--episode", type=int, default=None, help="Optional episode number for Show Run mode.")
    parser.add_argument(
        "--output",
        type=str,
        required=True,
        help="Output directory for manifests, raw caches, and reports.",
    )
    parser.add_argument(
        "--sources",
        type=str,
        default="all",
        help="Comma-separated source families. Person mode supports all,getty,imdb,tmdb. Show mode supports all,getty.",
    )
    parser.add_argument("--getty-limit", type=int, default=200, help="Max Getty assets to collect.")
    parser.add_argument("--nbcumv-limit", type=int, default=300, help="Max NBCUMV assets to collect.")
    parser.add_argument("--bravo-limit", type=int, default=300, help="Max Bravo gallery assets to collect.")
    parser.add_argument("--supplemental-limit", type=int, default=100, help="Max IMDb/TMDb person photos per source.")
    parser.add_argument("--imdb-id", type=str, default=None, help="Optional IMDb person ID override (nm...).")
    parser.add_argument("--tmdb-id", type=int, default=None, help="Optional TMDb person ID override.")
    parser.add_argument(
        "--force-all",
        action="store_true",
        help="Refresh all source artifacts instead of reusing caches.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    from trr_backend.media.bravotv import run_get_images_pipeline
    from trr_backend.utils.env import load_env

    args = _parse_args(argv)
    if bool(args.person) == bool(args.show):
        print("Choose exactly one of --person or --show.", file=sys.stderr)
        return 1

    load_env()
    output_dir = Path(args.output).expanduser().resolve()

    def _progress(message: str) -> None:
        print(message, flush=True)

    result = run_get_images_pipeline(
        person_name=args.person,
        show_name=args.show,
        season=args.season,
        episode=args.episode,
        output_dir=output_dir,
        sources=args.sources,
        getty_limit=args.getty_limit,
        nbcumv_limit=args.nbcumv_limit,
        bravo_limit=args.bravo_limit,
        supplemental_limit=args.supplemental_limit,
        imdb_id=args.imdb_id,
        tmdb_id=args.tmdb_id,
        force_all=args.force_all,
        progress_cb=_progress,
    )
    print("")
    print(f"Merged catalog: {result['merged_catalog_path']}")
    print(f"Bridge table: {result['bridge_table_path']}")
    print(f"Reports: {result['reports_path']}")
    if result.get("supplemental_catalog_path"):
        print(f"Supplemental cast photos: {result['supplemental_catalog_path']}")
    print(f"Summary: {result['show_summary_path']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
