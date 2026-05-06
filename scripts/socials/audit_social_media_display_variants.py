#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import Any

import requests

from trr_backend.db import pg
from trr_backend.repositories import social_season_analytics as social_repo
from trr_backend.utils.env import load_env


@dataclass(slots=True)
class VariantAuditCounters:
    scanned: int = 0
    missing_originals: int = 0
    missing_variants: int = 0
    missing_display_thumbnail: int = 0
    cdn_inaccessible_variants: int = 0
    partial_failed_variants: int = 0
    status_counts: Counter[str] = field(default_factory=Counter)
    sample_post_ids: dict[str, list[str]] = field(default_factory=lambda: defaultdict(list))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Dry-run audit for social media original covers/media and display thumbnail variants."
    )
    parser.add_argument(
        "--platforms",
        default="instagram,tiktok,youtube,twitter,facebook,threads",
        help="Comma-separated platforms to inspect.",
    )
    parser.add_argument("--limit-per-platform", type=int, default=5000)
    parser.add_argument("--sample-size", type=int, default=25)
    parser.add_argument(
        "--cdn-smoke",
        action="store_true",
        help="HEAD a small sample of display_thumbnail_url values and variant URLs.",
    )
    parser.add_argument("--cdn-timeout", type=float, default=8.0)
    return parser.parse_args()


def _platforms(value: str) -> list[str]:
    requested = [item.strip().lower() for item in str(value or "").split(",") if item.strip()]
    return [platform for platform in requested if platform in social_repo.PLATFORM_POST_TABLES]


def _record_sample(counters: VariantAuditCounters, key: str, post_id: str, *, sample_size: int) -> None:
    bucket = counters.sample_post_ids.setdefault(key, [])
    if len(bucket) < sample_size:
        bucket.append(post_id)


def _is_cdn_accessible(url: str, *, timeout: float) -> bool:
    try:
        response = requests.head(url, allow_redirects=True, timeout=timeout)
        if response.status_code == 405:
            response = requests.get(url, stream=True, timeout=timeout)
        content_type = str(response.headers.get("content-type") or "").split(";", 1)[0].strip().lower()
        return response.status_code < 400 and content_type.startswith("image/")
    except Exception:  # noqa: BLE001
        return False


def _load_rows(platform: str, *, limit: int) -> list[dict[str, Any]]:
    table = social_repo.PLATFORM_POST_TABLES.get(platform)
    source_id_column = social_repo.PLATFORM_SOURCE_ID_COLUMN.get(platform)
    posted_at_column = social_repo.PLATFORM_POSTED_AT_COLUMN.get(platform)
    if not table or not source_id_column or not posted_at_column:
        return []
    media_urls_expr = (
        "p.media_urls"
        if social_repo._platform_posts_has_column(platform, "media_urls")  # noqa: SLF001
        else "'[]'::jsonb"
    )
    thumbnail_expr = (
        "coalesce(nullif(p.media_urls ->> 0, ''), '')"
        if platform == "twitter"
        else "coalesce(nullif(p.thumbnail_url, ''), '')"
    )
    return pg.fetch_all(
        f"""
        select
          p.id::text as id,
          p.{source_id_column} as source_id,
          {thumbnail_expr} as thumbnail_url,
          {media_urls_expr} as media_urls,
          coalesce(to_jsonb(p) ->> 'hosted_thumbnail_url', '') as hosted_thumbnail_url,
          coalesce(to_jsonb(p) -> 'hosted_media_urls', '[]'::jsonb) as hosted_media_urls,
          coalesce(to_jsonb(p) -> 'asset_manifest', '{{}}'::jsonb) as asset_manifest,
          coalesce(to_jsonb(p) -> 'raw_data', '{{}}'::jsonb) as raw_data,
          coalesce(to_jsonb(p) ->> 'media_mirror_status', '') as media_mirror_status,
          p.{posted_at_column} as posted_at
        from social.{table} p
        order by p.{posted_at_column} desc nulls last
        limit %s
        """,
        [max(1, int(limit))],
    )


def main() -> int:
    load_env()
    args = _parse_args()
    platforms = _platforms(args.platforms)
    if not platforms:
        raise SystemExit("No valid platforms requested.")

    by_platform: dict[str, dict[str, Any]] = {}
    totals = VariantAuditCounters()
    for platform in platforms:
        counters = VariantAuditCounters()
        rows = _load_rows(platform, limit=args.limit_per_platform)
        for row in rows:
            counters.scanned += 1
            post_id = str(row.get("id") or row.get("source_id") or "").strip()
            source_thumbnail_url, source_media_urls = social_repo._platform_post_source_urls(platform, row)  # noqa: SLF001
            hosted_thumbnail_url = str(row.get("hosted_thumbnail_url") or "").strip()
            hosted_media_urls = social_repo._as_text_list(row.get("hosted_media_urls"))  # noqa: SLF001
            manifest = social_repo._extract_platform_post_asset_manifest(platform, row)  # noqa: SLF001
            variants = social_repo._display_thumbnail_variants_from_manifest(manifest)  # noqa: SLF001
            display_payload = social_repo._build_display_thumbnail_payload(  # noqa: SLF001
                asset_manifest=manifest,
                hosted_thumbnail_url=hosted_thumbnail_url,
                source_thumbnail_url=source_thumbnail_url,
                hosted_media_urls=hosted_media_urls,
                source_media_urls=source_media_urls,
            )
            status = str(manifest.get("display_variants_status") or display_payload.get("display_thumbnail_status") or "unknown")
            counters.status_counts[status] += 1

            originals = manifest.get("originals") if isinstance(manifest, dict) else None
            if not isinstance(originals, dict) or (not originals.get("cover") and not originals.get("media")):
                counters.missing_originals += 1
                _record_sample(counters, "missing_originals", post_id, sample_size=args.sample_size)
            if hosted_thumbnail_url and not variants and status != "unsupported":
                counters.missing_variants += 1
                _record_sample(counters, "missing_variants", post_id, sample_size=args.sample_size)
            if not display_payload.get("display_thumbnail_url") and (hosted_thumbnail_url or source_thumbnail_url):
                counters.missing_display_thumbnail += 1
                _record_sample(counters, "missing_display_thumbnail", post_id, sample_size=args.sample_size)
            if status == "failed":
                counters.partial_failed_variants += 1
                _record_sample(counters, "partial_failed_variants", post_id, sample_size=args.sample_size)
            if args.cdn_smoke:
                urls = [str(display_payload.get("display_thumbnail_url") or "").strip()]
                urls.extend(str(entry.get("url") or "").strip() for entry in variants.values() if isinstance(entry, dict))
                for url in {url for url in urls if url}:
                    if not _is_cdn_accessible(url, timeout=float(args.cdn_timeout)):
                        counters.cdn_inaccessible_variants += 1
                        _record_sample(counters, "cdn_inaccessible_variants", post_id, sample_size=args.sample_size)
                        break

        totals.scanned += counters.scanned
        totals.missing_originals += counters.missing_originals
        totals.missing_variants += counters.missing_variants
        totals.missing_display_thumbnail += counters.missing_display_thumbnail
        totals.cdn_inaccessible_variants += counters.cdn_inaccessible_variants
        totals.partial_failed_variants += counters.partial_failed_variants
        totals.status_counts.update(counters.status_counts)
        by_platform[platform] = {
            "scanned": counters.scanned,
            "missing_originals": counters.missing_originals,
            "missing_variants": counters.missing_variants,
            "missing_display_thumbnail": counters.missing_display_thumbnail,
            "cdn_inaccessible_variants": counters.cdn_inaccessible_variants,
            "partial_failed_variants": counters.partial_failed_variants,
            "status_counts": dict(counters.status_counts),
            "sample_post_ids": dict(counters.sample_post_ids),
        }

    print(
        json.dumps(
            {
                "dry_run": True,
                "cdn_smoke": bool(args.cdn_smoke),
                "platforms": platforms,
                "totals": {
                    "scanned": totals.scanned,
                    "missing_originals": totals.missing_originals,
                    "missing_variants": totals.missing_variants,
                    "missing_display_thumbnail": totals.missing_display_thumbnail,
                    "cdn_inaccessible_variants": totals.cdn_inaccessible_variants,
                    "partial_failed_variants": totals.partial_failed_variants,
                    "status_counts": dict(totals.status_counts),
                },
                "by_platform": by_platform,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
