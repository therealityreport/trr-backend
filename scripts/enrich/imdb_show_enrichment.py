#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from trr_backend.integrations.imdb.title_page_metadata import fetch_imdb_title_html, parse_imdb_title_html  # noqa: E402


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="imdb_show_enrichment",
        description="Fetch and parse IMDb title pages for show metadata.",
    )
    parser.add_argument("--imdb-id", action="append", default=[], help="IMDb title id (tt...).")
    parser.add_argument(
        "--input",
        type=str,
        default=None,
        help="Optional JSON file containing a list of IMDb ids.",
    )
    parser.add_argument(
        "--out",
        default="out/imdb_show_enrichment.json",
        help="Output JSON path (default: out/imdb_show_enrichment.json).",
    )
    return parser.parse_args(argv)


def _load_ids_from_file(path: str) -> list[str]:
    raw = Path(path).read_text(encoding="utf-8")
    payload = json.loads(raw)
    if isinstance(payload, list):
        return [str(item).strip() for item in payload if str(item).strip()]
    raise ValueError("Input JSON must be a list of IMDb ids.")


def _ensure_output_dir(path: str) -> None:
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)


def _print_summary(rows: list[dict[str, Any]]) -> None:
    header = f"{'title':<28} | {'rating':<6} | {'episodes':<8} | tags"
    print(header)
    print("-" * len(header))
    for row in rows:
        title = (row.get("title") or "")[:28]
        rating = row.get("aggregate_rating_value") or ""
        episodes = row.get("total_episodes") or ""
        tags = ", ".join(row.get("tags") or [])
        print(f"{title:<28} | {rating!s:<6} | {episodes!s:<8} | {tags}")


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    imdb_ids = [str(item).strip() for item in args.imdb_id if str(item).strip()]
    if args.input:
        imdb_ids.extend(_load_ids_from_file(args.input))
    imdb_ids = [item for item in imdb_ids if item]
    if not imdb_ids:
        print("No IMDb ids provided.", file=sys.stderr)
        return 2

    results: list[dict[str, Any]] = []
    for imdb_id in imdb_ids:
        html = fetch_imdb_title_html(imdb_id)
        results.append(parse_imdb_title_html(html, imdb_id=imdb_id))

    _ensure_output_dir(args.out)
    with open(args.out, "w", encoding="utf-8") as handle:
        json.dump(results, handle, ensure_ascii=True, indent=2)

    _print_summary(results)
    print(f"\nWrote {len(results)} rows to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
