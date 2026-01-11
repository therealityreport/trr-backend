#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any
from urllib.parse import quote

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from trr_backend.integrations.fandom import (  # noqa: E402
    FandomInfoboxResult,
    build_real_housewives_wiki_url_from_name,
    fetch_fandom_page,
    fetch_html,
    is_fandom_page_missing,
    parse_fandom_infobox_html,
    search_real_housewives_wiki,
)
from trr_backend.utils.episode_appearances import AggregatedCastMember, aggregate_episode_appearances  # noqa: E402

_DEFAULT_IMDB_ID = "tt11363282"
_DEFAULT_PAGE_SIZE = 1000


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="rhoslc_fandom_enrichment",
        description="Fetch RHOSLC cast from Supabase episode_appearances and enrich with Fandom infobox data.",
    )
    parser.add_argument("--imdb-show-id", default=_DEFAULT_IMDB_ID, help="IMDb show id (tt...).")
    parser.add_argument(
        "--concurrency",
        type=int,
        default=3,
        help="Max concurrent Fandom requests (default: 3).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Optional limit on number of cast members to process.",
    )
    parser.add_argument(
        "--out",
        default="out/rhoslc_fandom_enrichment.json",
        help="Output JSON path (default: out/rhoslc_fandom_enrichment.json).",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=_DEFAULT_PAGE_SIZE,
        help="Supabase page size for episode_appearances fetch (default: 1000).",
    )
    parser.add_argument(
        "--no-search",
        action="store_true",
        help="Disable Fandom search fallback.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Skip Fandom requests; emit placeholder rows with resolved URLs.",
    )
    return parser.parse_args(argv)


def _ensure_output_dir(path: str) -> None:
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)


def _build_error_result(member: AggregatedCastMember, *, url: str, error: str) -> dict[str, Any]:
    payload = member.to_dict()
    payload.update(
        {
            "fandom_page_url": url,
            "birthdate": None,
            "gender": None,
            "full_name": None,
            "resides_in": None,
            "raw_infobox": {},
            "status": "parse_failed",
            "error": error,
        }
    )
    return payload


def _result_from_fandom(
    member: AggregatedCastMember,
    fandom: FandomInfoboxResult,
    *,
    status: str,
    error: str | None,
) -> dict[str, Any]:
    payload = member.to_dict()
    payload.update(
        {
            "fandom_page_url": fandom.url,
            "birthdate": fandom.birth_date,
            "gender": fandom.gender,
            "full_name": fandom.full_name,
            "resides_in": fandom.resides_in,
            "raw_infobox": fandom.infobox,
            "status": status,
            "error": error,
        }
    )
    return payload


def _empty_not_found_result(member: AggregatedCastMember, *, url: str) -> dict[str, Any]:
    payload = member.to_dict()
    payload.update(
        {
            "fandom_page_url": url,
            "birthdate": None,
            "gender": None,
            "full_name": None,
            "resides_in": None,
            "raw_infobox": {},
            "status": "not_found",
            "error": None,
        }
    )
    return payload


def _missing_name_result(member: AggregatedCastMember) -> dict[str, Any]:
    payload = member.to_dict()
    payload.update(
        {
            "fandom_page_url": None,
            "birthdate": None,
            "gender": None,
            "full_name": None,
            "resides_in": None,
            "raw_infobox": {},
            "status": "missing_name",
            "error": "Missing cast_member_name",
        }
    )
    return payload


def _dry_run_result(member: AggregatedCastMember, *, url: str) -> dict[str, Any]:
    payload = member.to_dict()
    payload.update(
        {
            "fandom_page_url": url,
            "birthdate": None,
            "gender": None,
            "full_name": None,
            "resides_in": None,
            "raw_infobox": {},
            "status": "dry_run",
            "error": None,
        }
    )
    return payload


def _enrich_cast_member(
    member: AggregatedCastMember,
    *,
    use_search: bool,
    dry_run: bool,
) -> dict[str, Any]:
    name = (member.cast_member_name or "").strip()
    if not name:
        return _missing_name_result(member)

    url = build_real_housewives_wiki_url_from_name(name)
    if dry_run:
        return _dry_run_result(member, url=url)

    fetch = fetch_fandom_page(url)

    if fetch.error:
        return _build_error_result(member, url=url, error=fetch.error)

    if is_fandom_page_missing(fetch.html, fetch.status_code):
        if use_search:
            search_url = search_real_housewives_wiki(name)
            if search_url:
                url = search_url
                fetch = fetch_fandom_page(url)
                if fetch.error:
                    return _build_error_result(member, url=url, error=fetch.error)

    if is_fandom_page_missing(fetch.html, fetch.status_code):
        return _empty_not_found_result(member, url=url)

    try:
        parsed = parse_fandom_infobox_html(fetch.html or "", url=url)
    except Exception as exc:  # noqa: BLE001
        return _build_error_result(member, url=url, error=str(exc))

    if not parsed.infobox:
        return _result_from_fandom(member, parsed, status="parse_failed", error="No infobox data")

    return _result_from_fandom(member, parsed, status="ok", error=None)


def _print_summary(results: list[dict[str, Any]]) -> None:
    header = f"{'name':<28} | {'found':<5} | {'birthdate':<10} | {'gender':<6} | url"
    print(header)
    print("-" * len(header))
    for row in results:
        name = (row.get("cast_member_name") or "")[:28]
        status = row.get("status")
        if status == "ok":
            found = "yes"
        elif status == "dry_run":
            found = "dry"
        else:
            found = "no"
        birth = row.get("birthdate") or ""
        gender = row.get("gender") or ""
        url = row.get("fandom_page_url") or ""
        print(f"{name:<28} | {found:<5} | {birth:<10} | {gender:<6} | {url}")


def _require_env(name: str) -> str:
    value = (os.getenv(name) or "").strip()
    if not value:
        raise RuntimeError(f"{name} environment variable is not set")
    return value


def _build_supabase_headers(api_key: str) -> dict[str, str]:
    return {
        "apikey": api_key,
        "authorization": f"Bearer {api_key}",
        "accept": "application/json",
        "accept-profile": "core",
    }


def _fetch_episode_appearance_rows(imdb_show_id: str, *, page_size: int) -> list[dict[str, Any]]:
    supabase_url = _require_env("SUPABASE_URL").rstrip("/")
    api_key = _require_env("SUPABASE_SERVICE_ROLE_KEY")
    base_url = f"{supabase_url}/rest/v1/episode_appearances"
    query = f"imdb_show_id=eq.{quote(imdb_show_id)}"
    url = f"{base_url}?{query}"

    headers = _build_supabase_headers(api_key)
    rows: list[dict[str, Any]] = []
    offset = 0
    page_size = max(1, int(page_size))

    while True:
        page_headers = dict(headers)
        page_headers["range"] = f"{offset}-{offset + page_size - 1}"
        status, body, error = fetch_html(url, timeout=30.0, headers=page_headers)
        if error and status is None:
            raise RuntimeError(f"Supabase request failed: {error}")
        if status not in {200, 206}:
            snippet = (body or "")[:200]
            raise RuntimeError(f"Supabase request failed (HTTP {status}): {snippet}")
        if not body:
            break
        try:
            payload = json.loads(body)
        except ValueError as exc:
            raise RuntimeError(f"Supabase response was not valid JSON: {exc}") from exc
        if not isinstance(payload, list) or not payload:
            break
        rows.extend(payload)
        if len(payload) < page_size:
            break
        offset += page_size
    return rows


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    imdb_id = str(args.imdb_show_id or "").strip()
    if not imdb_id:
        print("Missing --imdb-show-id", file=sys.stderr)
        return 2

    concurrency = max(1, min(int(args.concurrency or 1), 4))

    try:
        episode_rows = _fetch_episode_appearance_rows(imdb_id, page_size=args.page_size)
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    cast_members = aggregate_episode_appearances(episode_rows, imdb_show_id=imdb_id)
    if args.limit and args.limit > 0:
        cast_members = cast_members[: args.limit]

    if not cast_members:
        print("No cast rows found for IMDb show id.")
        return 0

    results: list[dict[str, Any]] = [None] * len(cast_members)
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        future_map = {
            executor.submit(
                _enrich_cast_member,
                member,
                use_search=not args.no_search,
                dry_run=bool(args.dry_run),
            ): idx
            for idx, member in enumerate(cast_members)
        }
        for future in as_completed(future_map):
            idx = future_map[future]
            try:
                results[idx] = future.result()
            except Exception as exc:  # noqa: BLE001
                member = cast_members[idx]
                results[idx] = _build_error_result(member, url="", error=str(exc))

    _ensure_output_dir(args.out)
    with open(args.out, "w", encoding="utf-8") as handle:
        json.dump(results, handle, ensure_ascii=True, indent=2)

    _print_summary(results)
    print(f"\nWrote {len(results)} rows to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
