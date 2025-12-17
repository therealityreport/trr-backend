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
        "--annotate-imdb-episodic",
        action="store_true",
        help="Annotate shows with IMDb episodic support flags (and optionally probe seasons if configured).",
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

    if not imdb_lists and not tmdb_lists:
        raise SystemExit("No list sources provided. Use --imdb-list/--tmdb-list and/or --config.")

    candidates = collect_candidates_from_lists(imdb_list_urls=imdb_lists, tmdb_lists=tmdb_lists)
    print(f"Collected {len(candidates)} merged candidate shows.")

    imdb_probe_name_id = (os.getenv("IMDB_EPISODIC_PROBE_NAME_ID") or "").strip() or None
    imdb_probe_job_category_id = (
        (os.getenv("IMDB_EPISODIC_PROBE_JOB_CATEGORY_ID") or "").strip() or IMDB_JOB_CATEGORY_SELF
    )
    imdb_extra_headers = parse_imdb_headers_json_env()

    result = upsert_candidates_into_supabase(
        candidates,
        dry_run=bool(args.dry_run),
        annotate_imdb_episodic=bool(args.annotate_imdb_episodic),
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
