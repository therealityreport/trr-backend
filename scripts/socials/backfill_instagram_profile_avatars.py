#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any

try:
    from trr_backend.db import pg
    from trr_backend.socials import social_season_analytics_impl as social_repo
    from trr_backend.socials.instagram import InstagramScraper
    from trr_backend.utils.env import load_env
except ModuleNotFoundError:  # pragma: no cover - script execution convenience
    repo_root = Path(__file__).resolve().parents[2]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    from trr_backend.db import pg
    from trr_backend.socials import social_season_analytics_impl as social_repo
    from trr_backend.socials.instagram import InstagramScraper
    from trr_backend.utils.env import load_env


@dataclass(slots=True)
class BackfillStats:
    scanned: int = 0
    eligible: int = 0
    enriched: int = 0
    mirrored: int = 0
    partial: int = 0
    failed: int = 0
    skipped: int = 0


class _AvatarBackfillPost:
    def __init__(self, row: dict[str, Any]) -> None:
        self.shortcode = str(row.get("shortcode") or "").strip()
        self.pk = row.get("media_id")
        self.username = str(row.get("username") or row.get("source_account") or "").strip()
        self.caption = str(row.get("caption") or "").strip()
        self.post_type = str(row.get("media_type") or "post").strip() or "post"
        self.media_urls = social_repo._as_text_list(row.get("media_urls"))  # noqa: SLF001
        self.thumbnail_url = str(row.get("thumbnail_url") or "").strip() or None
        self.likes = int(row.get("likes") or 0)
        self.comments = int(row.get("comments_count") or 0)
        self.video_views = int(row.get("views") or 0)
        self.taken_at = row.get("posted_at")
        self.profile_tags = social_repo._as_text_list(row.get("profile_tags"))  # noqa: SLF001
        self.collaborators = social_repo._as_text_list(row.get("collaborators"))  # noqa: SLF001
        self.hashtags = social_repo._as_text_list(row.get("hashtags"), strip_prefix="#")  # noqa: SLF001
        self.mentions = social_repo._as_text_list(  # noqa: SLF001
            row.get("mentions"),
            prefix="@",
            strip_prefix="@",
        )
        self.duration_seconds = row.get("duration_seconds")
        self.post_format = row.get("post_format")
        self.metadata_source = row.get("metadata_source")
        self.metadata_scraped_at = row.get("metadata_scraped_at")
        self.metadata_error = row.get("metadata_error")
        self.owner_profile_pic_url = str(row.get("owner_profile_pic_url") or "").strip() or None
        self.owner_detail: Any = None
        owner_detail_raw = social_repo._as_json_object((row.get("raw_data") or {}).get("owner_detail"))  # noqa: SLF001
        if owner_detail_raw:
            self.owner_detail = SimpleNamespace(**owner_detail_raw)
            if not getattr(self.owner_detail, "profile_pic_url", None) and self.owner_profile_pic_url:
                self.owner_detail.profile_pic_url = self.owner_profile_pic_url
        self.tagged_users_detail: list[Any] = [
            SimpleNamespace(**item)
            for item in social_repo._as_json_object_list(row.get("tagged_users_detail"))  # noqa: SLF001
            if isinstance(item, dict)
        ]
        self.collaborators_detail: list[Any] = [
            SimpleNamespace(**item)
            for item in social_repo._as_json_object_list(row.get("collaborators_detail"))  # noqa: SLF001
            if isinstance(item, dict)
        ]
        self.hosted_owner_profile_pic_url = str(row.get("hosted_owner_profile_pic_url") or "").strip() or None
        self.hosted_tagged_profile_pics = social_repo._normalize_hosted_tagged_profile_pics(  # noqa: SLF001
            row.get("hosted_tagged_profile_pics")
        )
        self.profile_pic_mirror_status = str(row.get("profile_pic_mirror_status") or "").strip() or None
        self.profile_pic_mirror_error = str(row.get("profile_pic_mirror_error") or "").strip() or None
        self._raw_data = row.get("raw_data") if isinstance(row.get("raw_data"), dict) else {}

    def to_dict(self) -> dict[str, Any]:
        payload = dict(self._raw_data or {})
        if self.owner_detail is not None:
            payload["owner_detail"] = (
                self.owner_detail.to_dict()
                if hasattr(self.owner_detail, "to_dict")
                else dict(self.owner_detail.__dict__)
                if hasattr(self.owner_detail, "__dict__")
                else {}
            )
        payload["tagged_users_detail"] = [
            item.to_dict() if hasattr(item, "to_dict") else dict(item.__dict__) if hasattr(item, "__dict__") else {}
            for item in (self.tagged_users_detail or [])
        ]
        payload["collaborators_detail"] = [
            item.to_dict() if hasattr(item, "to_dict") else dict(item.__dict__) if hasattr(item, "__dict__") else {}
            for item in (self.collaborators_detail or [])
        ]
        payload["mentions"] = social_repo._as_text_list(self.mentions, prefix="@", strip_prefix="@")  # noqa: SLF001
        return payload


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="backfill_instagram_profile_avatars",
        description="Backfill Instagram hosted profile avatar fields for posts that already exist.",
    )
    parser.add_argument("--weeks", type=int, default=8, help="Lookback window in weeks (default: 8).")
    parser.add_argument("--all-history", action="store_true", help="Ignore the lookback window.")
    parser.add_argument("--season-id", action="append", default=[], help="Optional season UUID filter.")
    parser.add_argument("--show-id", action="append", default=[], help="Optional show UUID filter.")
    parser.add_argument("--post-id", action="append", default=[], help="Optional post row UUID filter.")
    parser.add_argument("--source-id", action="append", default=[], help="Optional shortcode filter.")
    parser.add_argument("--account", action="append", default=[], help="Optional Instagram handle filter.")
    parser.add_argument("--limit", type=int, default=1000, help="Maximum candidate rows to scan (default: 1000).")
    parser.add_argument(
        "--source-scope",
        default="bravo",
        choices=["bravo", "creator", "community"],
        help="Week window scope for deterministic S3 routing.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview the backfill without mutating rows.")
    parser.add_argument("--apply", action="store_true", help="Apply the backfill.")
    parser.set_defaults(dry_run=True)
    return parser.parse_args(argv)


def _normalize_filters(values: list[str] | tuple[str, ...] | None) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for raw in values or []:
        value = str(raw or "").strip()
        if not value or value in seen:
            continue
        seen.add(value)
        normalized.append(value)
    return normalized


def _load_candidate_rows(
    *,
    cutoff: datetime | None,
    season_ids: list[str],
    show_ids: list[str],
    post_ids: list[str],
    source_ids: list[str],
    accounts: list[str],
    limit: int,
) -> list[dict[str, Any]]:
    filters = ["coalesce(nullif(p.shortcode, ''), '') <> ''"]
    params: list[Any] = []
    if cutoff is not None:
        filters.append("coalesce(p.posted_at, p.scraped_at) >= %s")
        params.append(cutoff)
    if season_ids:
        filters.append("p.season_id::text = any(%s)")
        params.append(season_ids)
    if show_ids:
        filters.append("p.show_id::text = any(%s)")
        params.append(show_ids)
    if post_ids:
        filters.append("p.id::text = any(%s)")
        params.append(post_ids)
    if source_ids:
        filters.append("p.shortcode = any(%s)")
        params.append(source_ids)
    if accounts:
        filters.append(
            "ltrim(lower(coalesce(nullif(p.source_account, ''), nullif(p.username, ''), '')), '@') = any(%s)"
        )
        params.append([str(account).strip().lstrip("@").lower() for account in accounts])
    filters.append(
        "("
        "coalesce(nullif(p.hosted_owner_profile_pic_url, ''), '') = '' "
        "or coalesce(nullif(p.profile_pic_mirror_status, ''), '') in ('', 'pending', 'partial', 'failed') "
        "or jsonb_array_length(coalesce(to_jsonb(p) -> 'tagged_users_detail', '[]'::jsonb)) > 0 "
        "or jsonb_array_length(coalesce(to_jsonb(p) -> 'collaborators_detail', '[]'::jsonb)) > 0 "
        "or jsonb_array_length(coalesce(to_jsonb(p) -> 'mentions', '[]'::jsonb)) > 0"
        ")"
    )
    params.append(max(1, int(limit)))
    return pg.fetch_all(
        f"""
        select
          p.id::text as id,
          p.shortcode,
          p.media_id,
          p.username,
          p.caption,
          p.media_type,
          p.media_urls,
          p.thumbnail_url,
          p.likes,
          p.comments_count,
          p.views,
          p.posted_at,
          p.raw_data,
          p.source_account,
          p.show_id::text as show_id,
          p.season_id::text as season_id,
          p.post_format,
          p.profile_tags,
          p.collaborators,
          p.hashtags,
          p.mentions,
          p.duration_seconds,
          p.metadata_source,
          p.metadata_scraped_at,
          p.metadata_error,
          p.owner_profile_pic_url,
          p.tagged_users_detail,
          p.collaborators_detail,
          p.hosted_owner_profile_pic_url,
          p.hosted_tagged_profile_pics,
          p.profile_pic_mirror_status,
          p.profile_pic_mirror_error
        from social.instagram_posts p
        where {" and ".join(filters)}
        order by coalesce(p.posted_at, p.scraped_at) desc, p.id desc
        limit %s
        """,
        params,
    )


def _needs_avatar_backfill(row: dict[str, Any]) -> bool:
    hosted_owner = str(row.get("hosted_owner_profile_pic_url") or "").strip()
    hosted_tagged = social_repo._as_json_string_map(row.get("hosted_tagged_profile_pics"))  # noqa: SLF001
    status = str(row.get("profile_pic_mirror_status") or "").strip().lower()
    owner_source = str(row.get("owner_profile_pic_url") or "").strip()
    tagged_details = social_repo._as_json_object_list(row.get("tagged_users_detail"))  # noqa: SLF001
    collaborators_detail = social_repo._as_json_object_list(row.get("collaborators_detail"))  # noqa: SLF001
    mention_handles = {
        str(item or "").strip().lstrip("@").lower()
        for item in social_repo._as_text_list(row.get("mentions"), prefix="@", strip_prefix="@")  # noqa: SLF001
        if str(item or "").strip()
    }
    detail_handles = {
        str(item.get("username") or "").strip().lstrip("@").lower()
        for item in [*tagged_details, *collaborators_detail]
        if isinstance(item, dict) and str(item.get("username") or "").strip()
    }
    target_handles = {handle for handle in [*mention_handles, *detail_handles] if handle}
    if owner_source and not hosted_owner:
        return True
    if target_handles and any(handle not in hosted_tagged for handle in target_handles):
        return True
    return status in {"", "pending", "partial", "failed"} or not hosted_owner


def _populate_avatar_details_from_instagram(
    *,
    post: _AvatarBackfillPost,
    scraper: InstagramScraper,
    now_utc: datetime,
) -> bool:
    enriched = False
    social_repo._enrich_instagram_post_from_permalink(post=post, scraper=scraper, now_utc=now_utc)  # noqa: SLF001
    payload = scraper.fetch_post_info(post.shortcode, delay=0.0) if post.shortcode else None
    items = payload.get("items") if isinstance(payload, dict) else None
    node = items[0] if isinstance(items, list) and items and isinstance(items[0], dict) else None
    if isinstance(node, dict):
        owner_detail = scraper._extract_owner_detail(node)  # noqa: SLF001
        if owner_detail is not None:
            post.owner_detail = owner_detail
            if getattr(owner_detail, "profile_pic_url", None):
                post.owner_profile_pic_url = str(owner_detail.profile_pic_url).strip() or post.owner_profile_pic_url
            enriched = True
        tagged_details = scraper._extract_tagged_users_detail(node)  # noqa: SLF001
        if tagged_details:
            post.tagged_users_detail = tagged_details
            enriched = True
        collaborator_details = scraper._extract_collaborators_detail(node)  # noqa: SLF001
        if collaborator_details:
            post.collaborators_detail = collaborator_details
            enriched = True
    return enriched


def main(argv: list[str] | None = None) -> int:
    load_env()
    args = _parse_args(argv or sys.argv[1:])
    dry_run = bool(args.dry_run and not args.apply)
    if not dry_run:
        try:
            social_repo.ensure_media_mirror_s3_ready()
        except Exception as exc:  # noqa: BLE001
            raise SystemExit(f"Instagram profile avatar mirror S3 preflight failed: {exc}") from exc

    cutoff = None if bool(args.all_history) else datetime.now(tz=UTC) - timedelta(weeks=max(1, int(args.weeks)))
    rows = _load_candidate_rows(
        cutoff=cutoff,
        season_ids=_normalize_filters(args.season_id),
        show_ids=_normalize_filters(args.show_id),
        post_ids=_normalize_filters(args.post_id),
        source_ids=_normalize_filters(args.source_id),
        accounts=_normalize_filters(args.account),
        limit=args.limit,
    )

    try:
        cookies = social_repo._load_instagram_cookies()  # noqa: SLF001
    except Exception:
        cookies = {}
    scraper = InstagramScraper(cookies=cookies)
    stats = BackfillStats()
    season_context_cache: dict[str, social_repo.SeasonContext] = {}

    for row in rows:
        stats.scanned += 1
        if not _needs_avatar_backfill(row):
            stats.skipped += 1
            continue
        stats.eligible += 1
        post = _AvatarBackfillPost(row)
        season_id = str(row.get("season_id") or "").strip()
        if not season_id:
            stats.failed += 1
            continue

        enriched = _populate_avatar_details_from_instagram(
            post=post,
            scraper=scraper,
            now_utc=datetime.now(tz=UTC),
        )
        if enriched:
            stats.enriched += 1

        mirror_result = social_repo._mirror_instagram_profile_pics_for_post(row, post=post)  # noqa: SLF001
        post.hosted_owner_profile_pic_url = mirror_result.get("hosted_owner_profile_pic_url")
        post.hosted_tagged_profile_pics = mirror_result.get("hosted_tagged_profile_pics") or {}
        post.profile_pic_mirror_status = str(mirror_result.get("profile_pic_mirror_status") or "").strip() or None
        post.profile_pic_mirror_error = str(mirror_result.get("profile_pic_mirror_error") or "").strip() or None

        mirror_status = str(post.profile_pic_mirror_status or "").strip().lower()
        if mirror_status == "mirrored":
            stats.mirrored += 1
        elif mirror_status == "partial":
            stats.partial += 1
        else:
            stats.failed += 1

        if dry_run:
            continue

        context = season_context_cache.get(season_id)
        if context is None:
            try:
                context = social_repo.get_season_context(season_id)
            except Exception:
                stats.failed += 1
                continue
            season_context_cache[season_id] = context

        social_repo._upsert_instagram_post(  # noqa: SLF001
            context,
            job_id=None,
            account=str(row.get("source_account") or row.get("username") or "").strip(),
            post=post,
            conn=None,
        )

    payload = {
        "cutoff": cutoff.isoformat() if cutoff is not None else None,
        "all_history": bool(args.all_history),
        "season_ids": _normalize_filters(args.season_id),
        "show_ids": _normalize_filters(args.show_id),
        "post_ids": _normalize_filters(args.post_id),
        "source_ids": _normalize_filters(args.source_id),
        "accounts": _normalize_filters(args.account),
        "dry_run": dry_run,
        "totals": {
            "scanned": stats.scanned,
            "eligible": stats.eligible,
            "enriched": stats.enriched,
            "mirrored": stats.mirrored,
            "partial": stats.partial,
            "failed": stats.failed,
            "skipped": stats.skipped,
        },
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
