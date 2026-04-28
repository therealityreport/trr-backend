#!/usr/bin/env python3
"""Backfill queryable Instagram fields from existing raw payload families.

This script is intentionally bounded. Existing post rows can be upgraded only
when social.instagram_posts.raw_data has enough source fields. Profile/about
payloads and following rows require explicit JSONL inputs unless prior full
payload captures already exist.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from trr_backend.db import pg  # noqa: E402
from trr_backend.repositories import social_season_analytics as repo  # noqa: E402
from trr_backend.socials.instagram.post_normalizer import normalize_instagram_post  # noqa: E402


@dataclass
class BackfillPostDTO:
    shortcode: str
    post_type: str
    date_time: str
    taken_at: int
    caption: str
    profile_tags: list[str]
    sponsored: bool
    likes: int
    comments: int
    video_views: int
    url: str
    pk: str
    username: str
    media_urls: list[str] = field(default_factory=list)
    thumbnail_url: str | None = None
    hashtags: list[str] = field(default_factory=list)
    mentions: list[str] = field(default_factory=list)
    collaborators: list[str] = field(default_factory=list)
    tagged_users_detail: list[Any] = field(default_factory=list)
    collaborators_detail: list[Any] = field(default_factory=list)
    owner_detail: Any = None
    input_url: str | None = None
    source_post_id: str | None = None
    caption_id: str | None = None
    caption_is_edited: bool | None = None
    caption_has_translation: bool | None = None
    owner_user_id: str | None = None
    owner_username: str | None = None
    owner_profile_pic_url_hd: str | None = None
    location_id: str | None = None
    location_name: str | None = None
    location_raw: dict[str, Any] | None = None
    original_width: int | None = None
    original_height: int | None = None
    product_type: str | None = None
    video_play_count: int | None = None
    width: int | None = None
    height: int | None = None
    comments_disabled: bool | None = None
    like_and_view_counts_disabled: bool | None = None
    commenting_disabled_for_viewer: bool | None = None
    media_repost_count: int | None = None
    is_paid_partnership: bool | None = None
    is_advertisement: bool | None = None
    can_viewer_reshare: bool | None = None
    has_audio: bool | None = None
    music_info: dict[str, Any] | None = None
    audio_url: str | None = None
    video_duration: float | None = None
    child_posts_data: list[dict[str, Any]] = field(default_factory=list)
    _raw_data: dict[str, Any] = field(default_factory=dict, repr=False)

    def to_dict(self) -> dict[str, Any]:
        return dict(self._raw_data)


def _jsonl_rows(path: Path | None) -> list[dict[str, Any]]:
    if path is None:
        return []
    rows: list[dict[str, Any]] = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        stripped = line.strip()
        if not stripped:
            continue
        value = json.loads(stripped)
        if not isinstance(value, dict):
            raise ValueError(f"{path}:{line_number} is not a JSON object")
        rows.append(value)
    return rows


def _coerce_epoch(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _coerce_bool_or_none(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "t", "1", "yes", "y", "on"}:
            return True
        if normalized in {"false", "f", "0", "no", "n", "off"}:
            return False
    return None


def _coerce_int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _post_dto_from_row(row: dict[str, Any]) -> BackfillPostDTO | None:
    raw_data = row.get("raw_data") if isinstance(row.get("raw_data"), dict) else {}
    source = dict(raw_data or {})
    source.setdefault("shortcode", row.get("shortcode"))
    source.setdefault("code", row.get("shortcode"))
    source.setdefault("caption", row.get("caption"))
    source.setdefault("media_type", row.get("media_type"))
    source.setdefault("like_count", row.get("likes"))
    source.setdefault("comment_count", row.get("comments_count"))
    source.setdefault("view_count", row.get("views"))
    source.setdefault("taken_at", row.get("posted_at"))
    source.setdefault("user", {"username": row.get("username"), "id": row.get("user_id")})
    normalized = normalize_instagram_post(
        source, account_handle=str(row.get("source_account") or row.get("username") or "")
    )
    shortcode = normalized.shortcode or str(row.get("shortcode") or "").strip()
    if not shortcode:
        return None
    taken_at = _coerce_epoch(source.get("taken_at"))
    posted_at = row.get("posted_at")
    if taken_at <= 0 and isinstance(posted_at, datetime):
        taken_at = int(posted_at.replace(tzinfo=posted_at.tzinfo or UTC).timestamp())
    flags = normalized.flags
    owner = normalized.owner
    location = normalized.location
    location_raw = source.get("location") if isinstance(source.get("location"), dict) else None
    return BackfillPostDTO(
        shortcode=shortcode,
        post_type=normalized.media_type or str(row.get("media_type") or ""),
        date_time=posted_at.isoformat() if isinstance(posted_at, datetime) else "",
        taken_at=taken_at,
        caption=normalized.caption.text or str(row.get("caption") or ""),
        profile_tags=[user.username for user in normalized.tagged_users if user.username],
        sponsored=bool(source.get("is_paid_partnership") or source.get("sponsored") or False),
        likes=int(row.get("likes") or 0),
        comments=int(row.get("comments_count") or 0),
        video_views=int(row.get("views") or 0),
        url=normalized.permalink or f"https://www.instagram.com/p/{shortcode}/",
        pk=normalized.source_id or str(row.get("media_id") or ""),
        username=(normalized.owner.username if normalized.owner and normalized.owner.username else row.get("username"))
        or "",
        media_urls=normalized.media_urls,
        thumbnail_url=normalized.thumbnail_url,
        hashtags=normalized.hashtags,
        mentions=normalized.mentions,
        collaborators=[user.username for user in normalized.collaborators if user.username],
        tagged_users_detail=normalized.tagged_users,
        collaborators_detail=normalized.collaborators,
        owner_detail=owner,
        input_url=str(source.get("inputUrl") or "").strip() or None,
        source_post_id=normalized.source_id,
        caption_id=normalized.caption.caption_id,
        caption_is_edited=normalized.caption.is_edited,
        caption_has_translation=normalized.caption.has_translation,
        owner_user_id=owner.user_id if owner else None,
        owner_username=owner.username if owner else None,
        owner_profile_pic_url_hd=owner.profile_pic_url_hd if owner else None,
        location_id=location.location_id if location else None,
        location_name=location.name if location else None,
        location_raw=location_raw,
        original_width=normalized.width,
        original_height=normalized.height,
        product_type=str(source.get("product_type") or "") or None,
        video_play_count=normalized.video_play_count,
        width=normalized.width,
        height=normalized.height,
        comments_disabled=flags.get("comments_disabled"),
        like_and_view_counts_disabled=flags.get("like_count_disabled"),
        commenting_disabled_for_viewer=_coerce_bool_or_none(source.get("commenting_disabled_for_viewer")),
        media_repost_count=_coerce_int_or_none(source.get("media_repost_count")),
        is_paid_partnership=flags.get("paid_partnership"),
        is_advertisement=flags.get("advertisement"),
        can_viewer_reshare=_coerce_bool_or_none(source.get("can_viewer_reshare")),
        has_audio=_coerce_bool_or_none(source.get("has_audio")),
        music_info=normalized.music_info,
        audio_url=normalized.audio_url,
        video_duration=normalized.video_duration,
        child_posts_data=[asdict(child) for child in normalized.child_posts],
        _raw_data=raw_data or source,
    )


def _backfill_posts(*, account: str | None, limit: int, dry_run: bool) -> dict[str, int]:
    where = ["coalesce(raw_data, '{}'::jsonb) <> '{}'::jsonb"]
    params: list[Any] = []
    if account:
        where.append("lower(coalesce(source_account, username, '')) = %s")
        params.append(account.lower())
    params.append(limit)
    rows = pg.fetch_all(
        f"""
        select *
        from social.instagram_posts
        where {" and ".join(where)}
        order by scraped_at desc nulls last, posted_at desc nulls last, id desc
        limit %s
        """,
        params,
    )
    attempted = 0
    upgraded = 0
    skipped = 0
    with pg.db_connection() as conn:
        for row in rows:
            dto = _post_dto_from_row(row)
            if dto is None:
                skipped += 1
                continue
            attempted += 1
            if dry_run:
                continue
            account_name = str(row.get("source_account") or row.get("username") or account or "").strip()
            updated = repo._upsert_instagram_post(
                None, job_id=row.get("job_id"), account=account_name, post=dto, conn=conn
            )
            upgraded += 1 if updated else 0
    return {"selected": len(rows), "attempted": attempted, "upgraded": upgraded, "skipped": skipped}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--account", help="Optional Instagram account handle filter")
    parser.add_argument("--source-scope", default="bravo")
    parser.add_argument("--limit", type=int, default=250)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--profiles-jsonl", type=Path)
    parser.add_argument("--following-jsonl", type=Path)
    args = parser.parse_args()

    summary: dict[str, Any] = {
        "dry_run": bool(args.dry_run),
        "posts": _backfill_posts(account=args.account, limit=max(1, args.limit), dry_run=bool(args.dry_run)),
        "profiles": {"selected": 0, "upserted": 0},
        "following": {"selected": 0, "upserted": 0, "mismatches": 0},
    }
    if args.profiles_jsonl:
        profile_rows = _jsonl_rows(args.profiles_jsonl)
        summary["profiles"]["selected"] = len(profile_rows)
        if not args.dry_run:
            with pg.db_connection() as conn:
                for payload in profile_rows:
                    repo.persist_instagram_profile_snapshot(
                        payload,
                        source_scope=args.source_scope,
                        source_account=args.account or payload.get("username") or payload.get("ownerUsername"),
                        conn=conn,
                    )
                    summary["profiles"]["upserted"] += 1
    if args.following_jsonl:
        if not args.account:
            raise ValueError("--account is required when --following-jsonl is provided")
        following_rows = _jsonl_rows(args.following_jsonl)
        summary["following"]["selected"] = len(following_rows)
        if not args.dry_run:
            with pg.db_connection() as conn:
                result = repo.persist_instagram_profile_relationships(
                    following_rows,
                    owner_username=args.account,
                    source_scope=args.source_scope,
                    intended_relationship_type="following",
                    conn=conn,
                )
            summary["following"]["upserted"] = result["rows_upserted"]
            summary["following"]["mismatches"] = len(result["mismatches"])
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
