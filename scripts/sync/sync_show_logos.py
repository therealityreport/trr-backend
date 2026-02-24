#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.parse import quote, urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from trr_backend.db.admin import create_supabase_admin_client
from trr_backend.media.s3_mirror import (
    build_hosted_url,
    build_show_image_s3_key,
    download_image,
    get_s3_bucket,
    get_s3_client,
    guess_ext_from_content_type,
    upload_bytes_to_s3,
)
from trr_backend.repositories.media_assets import update_asset_with_mirror_result
from trr_backend.repositories.web_scrape_images import (
    create_media_asset_from_scrape,
    create_media_link_for_entity,
    find_asset_by_sha256,
)
from trr_backend.utils.env import load_env

_WIKIDATA_ENTITY_URL = "https://www.wikidata.org/wiki/Special:EntityData/{item_id}.json"
_REQUEST_HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "accept-language": "en-US,en;q=0.9",
    "user-agent": "TRR-Backend/1.0",
}
_SKIP_IMAGE_RE = re.compile(r"favicon|sprite|icon|avatar|badge|blank", re.IGNORECASE)
_LOGO_HINT_RE = re.compile(r"logo|wordmark|brand|title", re.IGNORECASE)


@dataclass
class SyncSummary:
    show_logos_discovered: int = 0
    show_logos_imported: int = 0
    show_logos_skipped: int = 0
    show_logo_failures: int = 0
    failures: int = 0


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="sync_show_logos",
        description="Harvest show logo candidates from show homepage and Wikipedia URLs.",
    )
    parser.add_argument("--all", action="store_true", help="Accepted for parity; script processes used shows by default.")
    parser.add_argument("--force", action="store_true", help="Re-link/import even when logo links already exist.")
    parser.add_argument("--dry-run", action="store_true", help="Collect and score candidates without writing.")
    parser.add_argument("--skip-s3", action="store_true", help="Skip downloading + mirroring candidate logos.")
    parser.add_argument("--limit", type=int, default=None, help="Maximum shows to process.")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output.")
    return parser.parse_args(argv)


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _iter_rows_paged(query, *, page_size: int = 1000):
    start = 0
    while True:
        response = query.range(start, start + page_size - 1).execute()
        if hasattr(response, "error") and response.error:
            raise RuntimeError(f"Supabase paging error: {response.error}")
        rows = response.data or []
        if not isinstance(rows, list) or not rows:
            break
        for row in rows:
            if isinstance(row, dict):
                yield row
        if len(rows) < page_size:
            break
        start += page_size


def _extract_enwiki_url(entity: dict[str, Any]) -> str | None:
    sitelinks = entity.get("sitelinks")
    if not isinstance(sitelinks, dict):
        return None
    enwiki = sitelinks.get("enwiki")
    if not isinstance(enwiki, dict):
        return None
    title = _normalize_text(enwiki.get("title"))
    if not title:
        return None
    return f"https://en.wikipedia.org/wiki/{quote(title.replace(' ', '_'))}"


def _resolve_show_wikipedia_url(wikidata_id: str | None) -> str | None:
    item_id = _normalize_text(wikidata_id).upper()
    if not item_id:
        return None
    try:
        response = requests.get(
            _WIKIDATA_ENTITY_URL.format(item_id=item_id),
            headers={"accept": "application/json", "user-agent": "TRR-Backend/1.0"},
            timeout=20,
        )
        response.raise_for_status()
        payload = response.json()
    except requests.RequestException:
        return None
    entities = payload.get("entities")
    if not isinstance(entities, dict):
        return None
    entity = entities.get(item_id)
    if not isinstance(entity, dict):
        return None
    return _extract_enwiki_url(entity)


def _score_candidate_url(*, url: str, attrs_text: str, is_meta: bool) -> int:
    score = 0
    lowered = url.casefold()
    if _LOGO_HINT_RE.search(lowered):
        score += 50
    if _LOGO_HINT_RE.search(attrs_text):
        score += 30
    if "wikipedia.org" in lowered:
        score += 10
    if is_meta:
        score += 5
    return score


def _extract_logo_candidates_from_html(*, html: str, page_url: str, limit: int) -> list[str]:
    soup = BeautifulSoup(html or "", "html.parser")
    scored: list[tuple[int, str]] = []

    def add_candidate(raw_url: str | None, attrs_text: str, *, is_meta: bool) -> None:
        url = _normalize_text(raw_url)
        if not url:
            return
        absolute = urljoin(page_url, url)
        if not absolute.startswith("http://") and not absolute.startswith("https://"):
            return
        if _SKIP_IMAGE_RE.search(absolute):
            return
        score = _score_candidate_url(url=absolute, attrs_text=attrs_text, is_meta=is_meta)
        scored.append((score, absolute))

    for selector in (
        ("meta", {"property": "og:image"}),
        ("meta", {"name": "og:image"}),
        ("meta", {"name": "twitter:image"}),
    ):
        tag = soup.find(selector[0], attrs=selector[1])
        if tag is None:
            continue
        add_candidate(tag.get("content"), "meta", is_meta=True)

    for image in soup.find_all("img"):
        src = image.get("src") or image.get("data-src") or image.get("data-original")
        attrs = " ".join(
            [
                _normalize_text(image.get("alt")),
                _normalize_text(image.get("class")),
                _normalize_text(image.get("id")),
                _normalize_text(image.get("title")),
            ]
        ).casefold()
        add_candidate(src, attrs, is_meta=False)

    scored.sort(key=lambda item: (-item[0], item[1]))
    out: list[str] = []
    seen: set[str] = set()
    for _score, url in scored:
        if url in seen:
            continue
        seen.add(url)
        out.append(url)
        if len(out) >= limit:
            break
    return out


def _collect_source_urls(show_row: dict[str, Any]) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()

    tmdb_meta = show_row.get("tmdb_meta")
    if isinstance(tmdb_meta, dict):
        homepage = _normalize_text(tmdb_meta.get("homepage"))
        if homepage.startswith("http") and homepage not in seen:
            seen.add(homepage)
            urls.append(homepage)

    wikipedia_url = _resolve_show_wikipedia_url(_normalize_text(show_row.get("wikidata_id")) or None)
    if wikipedia_url and wikipedia_url not in seen:
        seen.add(wikipedia_url)
        urls.append(wikipedia_url)

    return urls[:3]


def _load_existing_show_logo_asset_ids(db, *, show_id: str) -> set[str]:
    out: set[str] = set()
    query = (
        db.schema("core")
        .table("media_links")
        .select("media_asset_id")
        .eq("entity_type", "show")
        .eq("entity_id", show_id)
        .eq("kind", "logo")
    )
    for row in _iter_rows_paged(query):
        asset_id = _normalize_text(row.get("media_asset_id"))
        if asset_id:
            out.add(asset_id)
    return out


def _mirror_show_logo_asset(
    db,
    *,
    show_row: dict[str, Any],
    source_page_url: str,
    candidate_url: str,
    existing_asset_ids: set[str],
    force: bool,
    dry_run: bool,
    skip_s3: bool,
    s3_client,
) -> str:
    show_id = _normalize_text(show_row.get("id"))
    if not show_id:
        return "failed"

    if skip_s3:
        return "skipped"

    data, content_type = download_image(candidate_url, source="show_logo_sync", referer=source_page_url)
    sha256 = hashlib.sha256(data).hexdigest()

    existing_asset = find_asset_by_sha256(db, sha256)
    if existing_asset:
        asset_id = _normalize_text(existing_asset.get("id"))
        if not asset_id:
            return "failed"
        if not force and asset_id in existing_asset_ids:
            return "skipped"

        if not dry_run and not _normalize_text(existing_asset.get("hosted_url")):
            ext = guess_ext_from_content_type(content_type)
            show_identifier = _normalize_text(show_row.get("imdb_id")) or show_id
            s3_key = build_show_image_s3_key(
                show_identifier=show_identifier,
                kind="logo",
                source="show-logo-sync",
                sha256=sha256,
                ext=ext,
            )
            bucket = get_s3_bucket()
            etag, file_size = upload_bytes_to_s3(
                s3_client,
                bucket=bucket,
                key=s3_key,
                data=data,
                content_type=content_type or "application/octet-stream",
            )
            update_asset_with_mirror_result(
                db,
                asset_id=asset_id,
                sha256=sha256,
                hosted_bucket=bucket,
                hosted_key=s3_key,
                hosted_url=build_hosted_url(s3_key),
                hosted_bytes=file_size,
                hosted_content_type=content_type,
                hosted_etag=etag,
                completed_at=datetime.now(UTC).isoformat(),
            )

        if not dry_run:
            create_media_link_for_entity(
                db,
                entity_type="show",
                entity_id=show_id,
                media_asset_id=asset_id,
                kind="logo",
                position=0,
                context={
                    "source_url": candidate_url,
                    "source_page_url": source_page_url,
                    "source": "show_logo_harvest",
                },
            )
        existing_asset_ids.add(asset_id)
        return "imported"

    if dry_run:
        return "imported"

    ext = guess_ext_from_content_type(content_type)
    show_identifier = _normalize_text(show_row.get("imdb_id")) or show_id
    s3_key = build_show_image_s3_key(
        show_identifier=show_identifier,
        kind="logo",
        source="show-logo-sync",
        sha256=sha256,
        ext=ext,
    )
    bucket = get_s3_bucket()
    etag, file_size = upload_bytes_to_s3(
        s3_client,
        bucket=bucket,
        key=s3_key,
        data=data,
        content_type=content_type or "application/octet-stream",
    )

    asset = create_media_asset_from_scrape(
        db,
        source="show_logo_harvest",
        source_url=candidate_url,
        sha256=sha256,
        hosted_bucket=bucket,
        hosted_key=s3_key,
        hosted_url=build_hosted_url(s3_key),
        hosted_bytes=file_size,
        hosted_etag=etag,
        content_type=content_type or "application/octet-stream",
        width=None,
        height=None,
        caption=None,
        metadata={
            "page_url": source_page_url,
            "source_page_url": source_page_url,
            "source_type": "show_logo_harvest",
            "kind": "logo",
        },
    )
    asset_id = _normalize_text(asset.get("id"))
    if not asset_id:
        return "failed"

    create_media_link_for_entity(
        db,
        entity_type="show",
        entity_id=show_id,
        media_asset_id=asset_id,
        kind="logo",
        position=0,
        context={
            "source_url": candidate_url,
            "source_page_url": source_page_url,
            "source": "show_logo_harvest",
        },
    )
    existing_asset_ids.add(asset_id)
    return "imported"


def run_sync(args: argparse.Namespace) -> SyncSummary:
    load_env()
    db = create_supabase_admin_client()
    summary = SyncSummary()

    s3_client = None if args.skip_s3 or args.dry_run else get_s3_client()

    show_rows = []
    query = db.schema("core").table("shows").select("id,name,imdb_id,tmdb_meta,wikidata_id").order("id")
    for row in _iter_rows_paged(query):
        show_rows.append(row)
        if isinstance(args.limit, int) and args.limit > 0 and len(show_rows) >= args.limit:
            break

    for show_row in show_rows:
        show_name = _normalize_text(show_row.get("name")) or _normalize_text(show_row.get("id"))
        source_urls = _collect_source_urls(show_row)
        if not source_urls:
            continue

        existing_asset_ids = _load_existing_show_logo_asset_ids(db, show_id=_normalize_text(show_row.get("id")))
        imported_for_show = 0
        seen_candidate_urls: set[str] = set()

        for source_url in source_urls:
            if imported_for_show >= 12:
                break

            try:
                response = requests.get(source_url, headers=_REQUEST_HEADERS, timeout=20)
                response.raise_for_status()
            except requests.RequestException as exc:
                summary.show_logo_failures += 1
                summary.failures += 1
                if args.verbose:
                    print(f"WARN show={show_name} source={source_url} fetch_failed={exc}", file=sys.stderr)
                continue

            content_type = _normalize_text(response.headers.get("content-type")).lower()
            if content_type.startswith("image/"):
                candidates = [source_url]
            else:
                candidates = _extract_logo_candidates_from_html(
                    html=response.text,
                    page_url=source_url,
                    limit=8,
                )

            if not candidates:
                continue

            for candidate_url in candidates:
                if imported_for_show >= 12:
                    break
                if candidate_url in seen_candidate_urls:
                    continue
                seen_candidate_urls.add(candidate_url)
                summary.show_logos_discovered += 1

                try:
                    outcome = _mirror_show_logo_asset(
                        db,
                        show_row=show_row,
                        source_page_url=source_url,
                        candidate_url=candidate_url,
                        existing_asset_ids=existing_asset_ids,
                        force=bool(args.force),
                        dry_run=bool(args.dry_run),
                        skip_s3=bool(args.skip_s3),
                        s3_client=s3_client,
                    )
                except Exception as exc:  # noqa: BLE001
                    summary.show_logo_failures += 1
                    summary.failures += 1
                    if args.verbose:
                        print(
                            f"WARN show={show_name} source={source_url} candidate={candidate_url} import_failed={exc}",
                            file=sys.stderr,
                        )
                    continue

                if outcome == "imported":
                    summary.show_logos_imported += 1
                    imported_for_show += 1
                else:
                    summary.show_logos_skipped += 1

    return summary


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    summary = run_sync(args)
    summary_dict = summary.__dict__

    print("Summary")
    for key in (
        "show_logos_discovered",
        "show_logos_imported",
        "show_logos_skipped",
        "show_logo_failures",
        "failures",
    ):
        print(f"{key}={summary_dict[key]}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
