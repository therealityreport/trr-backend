"""Season-scoped social analytics + ingest helpers."""

from __future__ import annotations

import csv
import io
import json
import logging
import re
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from trr_backend.db import pg

logger = logging.getLogger(__name__)

SUPPORTED_PLATFORMS = ("instagram", "tiktok", "twitter", "youtube")
SUPPORTED_SCOPES = ("bravo", "creator", "community")

POSITIVE_WORDS = {
    "amazing",
    "awesome",
    "best",
    "binge",
    "brilliant",
    "classic",
    "excellent",
    "excited",
    "favorite",
    "fun",
    "funny",
    "good",
    "great",
    "happy",
    "hilarious",
    "iconic",
    "impressed",
    "incredible",
    "interesting",
    "love",
    "loved",
    "loving",
    "obsessed",
    "perfect",
    "solid",
    "strong",
    "stunning",
    "wow",
}

NEGATIVE_WORDS = {
    "annoying",
    "awful",
    "bad",
    "boring",
    "chaotic",
    "confusing",
    "cringe",
    "disappointing",
    "dull",
    "fake",
    "hate",
    "hated",
    "horrible",
    "lazy",
    "mess",
    "mid",
    "negative",
    "problem",
    "rough",
    "sad",
    "slow",
    "terrible",
    "toxic",
    "trash",
    "weak",
    "worse",
    "worst",
}

STOPWORDS = {
    "about",
    "after",
    "again",
    "also",
    "because",
    "before",
    "being",
    "bravo",
    "from",
    "have",
    "just",
    "like",
    "more",
    "only",
    "over",
    "really",
    "season",
    "show",
    "that",
    "their",
    "there",
    "these",
    "they",
    "this",
    "those",
    "very",
    "what",
    "with",
    "would",
    "your",
}

TOKEN_RE = re.compile(r"[a-zA-Z][a-zA-Z0-9']+")
TRAILER_MARKER_RE = re.compile(
    r"\b(first look|sneak peek|trailer|season announcement|official trailer)\b",
    re.IGNORECASE,
)


@dataclass(slots=True)
class SeasonContext:
    season_id: str
    show_id: str
    show_name: str | None
    season_number: int
    anchor_date: date


@dataclass(slots=True)
class WeekWindow:
    week_index: int
    start_local: datetime
    end_local: datetime


@dataclass(slots=True)
class IngestResult:
    platform: str
    source_scope: str
    account: str
    job_id: str
    status: str
    posts: int
    comments: int
    error: str | None = None


@dataclass(slots=True)
class IngestOptions:
    platforms: set[str] | None
    source_scope: str
    max_posts_per_target: int
    max_comments_per_post: int
    fetch_replies: bool
    date_start: datetime | None
    date_end: datetime | None


# ---------------------------------------------------------------------------
# General helpers
# ---------------------------------------------------------------------------


def _now_utc() -> datetime:
    return datetime.now(tz=UTC)


def _coerce_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, date):
        return datetime.combine(value, time.min, tzinfo=UTC)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
        except ValueError:
            return None
    return None


def _iso(dt: datetime | None) -> str | None:
    if not dt:
        return None
    return dt.astimezone(UTC).isoformat()


def _to_et_date(dt: datetime | None, tz_name: str) -> date | None:
    if dt is None:
        return None
    zone = ZoneInfo(tz_name)
    return dt.astimezone(zone).date()


def _slug_words(value: str) -> list[str]:
    cleaned = re.sub(r"[^a-zA-Z0-9 ]+", " ", value).strip()
    return [token for token in cleaned.split() if token]


def _normalize_unique_terms(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for raw in values:
        value = str(raw or "").strip()
        if not value:
            continue
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(value)
    return out


def _derive_show_terms(show_name: str | None) -> tuple[list[str], list[str]]:
    if not show_name:
        return [], []

    words = _slug_words(show_name)
    if not words:
        return [], []

    lower_name = show_name.lower()
    hashtags: list[str] = ["".join(words)]
    keywords: list[str] = [show_name]

    # Canonical RHOSLC aliases requested by product requirements.
    if "salt lake city" in lower_name:
        keywords.extend(["Salt Lake City", "RHOSLC"])
        hashtags.append("RHOSLC")

    # Generic Real Housewives acronym fallback (e.g., RHOBH, RHOP).
    franchise_match = re.search(r"real housewives of (.+)", lower_name)
    if franchise_match:
        suffix_words = [
            token
            for token in _slug_words(franchise_match.group(1))
            if token.lower() not in {"the", "of", "and"}
        ]
        if suffix_words:
            acronym = f"rho{''.join(token[0].lower() for token in suffix_words)}"
            keywords.append(acronym.upper())
            hashtags.append(acronym)

    return _normalize_unique_terms(hashtags), _normalize_unique_terms(keywords)


def _text_contains_any_term(*, text: str | None, hashtags: list[str], keywords: list[str]) -> bool:
    term_hashtags = [str(tag).strip().lstrip("#").lower() for tag in hashtags if str(tag).strip()]
    term_keywords = [str(kw).strip().lower() for kw in keywords if str(kw).strip()]
    if not term_hashtags and not term_keywords:
        return True

    haystack = (text or "").lower()
    if not haystack:
        return False

    for tag in term_hashtags:
        if f"#{tag}" in haystack or re.search(rf"\b{re.escape(tag)}\b", haystack):
            return True
    for keyword in term_keywords:
        if keyword in haystack:
            return True
    return False


def _build_twitter_or_query(*, hashtags: list[str], keywords: list[str]) -> str:
    terms: list[str] = []
    for keyword in keywords:
        value = str(keyword or "").strip()
        if not value:
            continue
        if " " in value:
            terms.append(f'"{value}"')
        else:
            terms.append(value)
    for hashtag in hashtags:
        value = str(hashtag or "").strip().lstrip("#")
        if not value:
            continue
        terms.append(f"#{value}")

    unique_terms = _normalize_unique_terms(terms)
    if not unique_terms:
        return ""
    if len(unique_terms) == 1:
        return unique_terms[0]
    return f"({' OR '.join(unique_terms)})"


# ---------------------------------------------------------------------------
# Context + targets
# ---------------------------------------------------------------------------


def get_season_context(season_id: str) -> SeasonContext:
    season = pg.fetch_one(
        """
        select
          s.id::text as season_id,
          s.show_id::text as show_id,
          s.season_number,
          s.air_date,
          sh.name as show_name
        from core.seasons s
        join core.shows sh on sh.id = s.show_id
        where s.id = %s
        """,
        [season_id],
    )
    if not season:
        raise ValueError(f"Season {season_id} not found")

    episode = pg.fetch_one(
        """
        select e.air_date
        from core.episodes e
        where e.season_id = %s
          and e.air_date is not null
        order by e.air_date asc
        limit 1
        """,
        [season_id],
    )

    anchor: date
    if episode and isinstance(episode.get("air_date"), date):
        anchor = episode["air_date"]
    elif isinstance(season.get("air_date"), date):
        anchor = season["air_date"]
    else:
        anchor = datetime.now(tz=ZoneInfo("America/New_York")).date()

    return SeasonContext(
        season_id=str(season["season_id"]),
        show_id=str(season["show_id"]),
        show_name=season.get("show_name"),
        season_number=int(season.get("season_number") or 0),
        anchor_date=anchor,
    )


def _default_targets(context: SeasonContext) -> list[dict[str, Any]]:
    default_hashtags, default_keywords = _derive_show_terms(context.show_name)
    keywords = _normalize_unique_terms([*default_keywords, f"season {context.season_number}"])
    hashtags = _normalize_unique_terms(default_hashtags)

    defaults = [
        {
            "platform": "instagram",
            "source_scope": "bravo",
            "timezone": "America/New_York",
            "accounts": ["bravotv"],
            "hashtags": hashtags,
            "keywords": keywords,
            "is_active": True,
            "config": {"include_comments": True},
        },
        {
            "platform": "tiktok",
            "source_scope": "bravo",
            "timezone": "America/New_York",
            "accounts": ["bravotv"],
            "hashtags": hashtags,
            "keywords": keywords,
            "is_active": True,
            "config": {"include_comments": True},
        },
        {
            "platform": "twitter",
            "source_scope": "bravo",
            "timezone": "America/New_York",
            "accounts": ["BravoTV"],
            "hashtags": hashtags,
            "keywords": keywords,
            "is_active": True,
            "config": {"include_replies": True},
        },
        {
            "platform": "youtube",
            "source_scope": "bravo",
            "timezone": "America/New_York",
            "accounts": ["bravo"],
            "hashtags": [],
            "keywords": keywords,
            "is_active": True,
            "config": {"include_comments": True},
        },
    ]
    return defaults


def get_targets(season_id: str, *, source_scope: str = "bravo") -> dict[str, Any]:
    context = get_season_context(season_id)
    rows = pg.fetch_all(
        """
        select
          season_id::text,
          show_id::text,
          platform,
          source_scope,
          timezone,
          accounts,
          hashtags,
          keywords,
          is_active,
          config,
          updated_by,
          updated_at,
          created_at
        from social.season_targets
        where season_id = %s
          and source_scope = %s
        order by platform asc
        """,
        [season_id, source_scope],
    )
    if not rows:
        rows = _default_targets(context)

    return {
        "season_id": context.season_id,
        "show_id": context.show_id,
        "season_number": context.season_number,
        "show_name": context.show_name,
        "source_scope": source_scope,
        "targets": rows,
        "using_defaults": len(rows) > 0 and "created_at" not in rows[0],
    }


def put_targets(
    season_id: str,
    *,
    source_scope: str,
    targets: list[dict[str, Any]],
    updated_by: str | None,
) -> dict[str, Any]:
    context = get_season_context(season_id)

    if source_scope not in SUPPORTED_SCOPES:
        raise ValueError(f"Unsupported source scope: {source_scope}")

    upserted: list[dict[str, Any]] = []
    for target in targets:
        platform = str(target.get("platform") or "").strip().lower()
        if platform not in {"instagram", "tiktok", "twitter", "youtube", "reddit"}:
            raise ValueError(f"Unsupported platform: {platform}")

        accounts = target.get("accounts") or []
        hashtags = target.get("hashtags") or []
        keywords = target.get("keywords") or []
        timezone = str(target.get("timezone") or "America/New_York")
        is_active = bool(target.get("is_active", True))
        config = target.get("config") or {}

        row = pg.fetch_one(
            """
            insert into social.season_targets (
              season_id,
              show_id,
              platform,
              source_scope,
              timezone,
              accounts,
              hashtags,
              keywords,
              is_active,
              config,
              updated_by,
              updated_at
            )
            values (
              %s,
              %s,
              %s,
              %s,
              %s,
              %s::jsonb,
              %s::jsonb,
              %s::jsonb,
              %s,
              %s::jsonb,
              %s,
              now()
            )
            on conflict (season_id, platform, source_scope)
            do update set
              show_id = excluded.show_id,
              timezone = excluded.timezone,
              accounts = excluded.accounts,
              hashtags = excluded.hashtags,
              keywords = excluded.keywords,
              is_active = excluded.is_active,
              config = excluded.config,
              updated_by = excluded.updated_by,
              updated_at = now()
            returning
              season_id::text,
              show_id::text,
              platform,
              source_scope,
              timezone,
              accounts,
              hashtags,
              keywords,
              is_active,
              config,
              updated_by,
              updated_at,
              created_at
            """,
            [
                season_id,
                context.show_id,
                platform,
                source_scope,
                timezone,
                json.dumps(accounts),
                json.dumps(hashtags),
                json.dumps(keywords),
                is_active,
                json.dumps(config),
                updated_by,
            ],
        )
        if row:
            upserted.append(row)

    return {
        "season_id": context.season_id,
        "show_id": context.show_id,
        "season_number": context.season_number,
        "show_name": context.show_name,
        "source_scope": source_scope,
        "targets": sorted(upserted, key=lambda item: str(item.get("platform") or "")),
    }


# ---------------------------------------------------------------------------
# Ingest helpers
# ---------------------------------------------------------------------------


def _create_job(
    context: SeasonContext,
    *,
    platform: str,
    source_scope: str,
    job_type: str,
    config: dict[str, Any],
    initiated_by: str | None,
) -> str:
    row = pg.fetch_one(
        """
        insert into social.scrape_jobs (
          platform,
          job_type,
          config,
          status,
          started_at,
          show_id,
          season_id,
          source_scope,
          initiated_by,
          metadata
        )
        values (
          %s,
          %s,
          %s::jsonb,
          'running',
          now(),
          %s,
          %s,
          %s,
          %s,
          '{}'::jsonb
        )
        returning id::text
        """,
        [platform, job_type, json.dumps(config), context.show_id, context.season_id, source_scope, initiated_by],
    )
    if not row:
        raise RuntimeError("Failed to create scrape job")
    return str(row["id"])


def _finish_job(
    job_id: str,
    *,
    status: str,
    items_found: int,
    error_message: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> None:
    pg.fetch_one(
        """
        update social.scrape_jobs
        set
          status = %s,
          items_found = %s,
          error_message = %s,
          completed_at = now(),
          metadata = coalesce(%s::jsonb, '{}'::jsonb)
        where id = %s
        returning id::text
        """,
        [status, items_found, error_message, json.dumps(metadata or {}), job_id],
    )


def _parse_instagram_time(ts: Any) -> datetime | None:
    if isinstance(ts, int):
        return datetime.fromtimestamp(ts, tz=UTC)
    if isinstance(ts, float):
        return datetime.fromtimestamp(int(ts), tz=UTC)
    return _coerce_dt(ts)


def _parse_tiktok_time(ts: Any) -> datetime | None:
    return _parse_instagram_time(ts)


def _upsert_instagram_post(
    db: Any,
    context: SeasonContext,
    *,
    job_id: str,
    account: str,
    post: Any,
) -> dict[str, Any] | None:
    posted_at = _parse_instagram_time(getattr(post, "taken_at", None))
    payload = {
        "shortcode": getattr(post, "shortcode", ""),
        "media_id": getattr(post, "pk", None),
        "username": getattr(post, "username", account),
        "user_id": None,
        "caption": getattr(post, "caption", None),
        "media_type": getattr(post, "post_type", None),
        "media_urls": getattr(post, "media_urls", []) or [],
        "likes": int(getattr(post, "likes", 0) or 0),
        "comments_count": int(getattr(post, "comments", 0) or 0),
        "views": int(getattr(post, "video_views", 0) or 0),
        "posted_at": posted_at,
        "scraped_at": _now_utc(),
        "raw_data": post.to_dict() if hasattr(post, "to_dict") else {},
        "show_id": context.show_id,
        "season_id": context.season_id,
        "job_id": job_id,
        "source_account": account,
    }
    resp = db.schema("social").table("instagram_posts").upsert(payload, on_conflict="shortcode").execute()
    if hasattr(resp, "error") and resp.error:
        raise RuntimeError(f"instagram_posts upsert failed: {resp.error}")
    rows = resp.data or []
    return rows[0] if rows else None


def _upsert_instagram_comment_tree(
    db: Any,
    context: SeasonContext,
    *,
    job_id: str,
    account: str,
    post_id: str,
    comment: Any,
    parent_comment_db_id: str | None = None,
) -> int:
    created_at = _parse_instagram_time(getattr(comment, "created_at", None))
    payload = {
        "comment_id": getattr(comment, "comment_id", ""),
        "post_id": post_id,
        "parent_comment_id": parent_comment_db_id,
        "username": getattr(comment, "username", ""),
        "user_id": getattr(comment, "user_id", None),
        "text": getattr(comment, "text", ""),
        "likes": int(getattr(comment, "likes", 0) or 0),
        "is_reply": bool(getattr(comment, "is_reply", False)),
        "reply_count": int(getattr(comment, "reply_count", 0) or 0),
        "created_at": created_at,
        "scraped_at": _now_utc(),
        "raw_data": comment.to_dict() if hasattr(comment, "to_dict") else {},
        "season_id": context.season_id,
        "job_id": job_id,
        "source_account": account,
    }
    resp = db.schema("social").table("instagram_comments").upsert(payload, on_conflict="comment_id").execute()
    if hasattr(resp, "error") and resp.error:
        raise RuntimeError(f"instagram_comments upsert failed: {resp.error}")
    row = (resp.data or [{}])[0]
    comment_db_id = row.get("id")

    total = 1
    for reply in getattr(comment, "replies", []) or []:
        total += _upsert_instagram_comment_tree(
            db,
            context,
            job_id=job_id,
            account=account,
            post_id=post_id,
            comment=reply,
            parent_comment_db_id=comment_db_id,
        )
    return total


def _ingest_instagram(
    db: Any,
    context: SeasonContext,
    *,
    account: str,
    hashtags: list[str],
    keywords: list[str],
    opts: IngestOptions,
    job_id: str,
) -> tuple[int, int]:
    from trr_backend.socials.instagram import InstagramScraper, ScrapeConfig

    scraper = InstagramScraper(cookies={})
    config = ScrapeConfig(
        username=account,
        hashtags=[],
        date_start=opts.date_start,
        date_end=opts.date_end,
        delay_seconds=1.0,
        max_pages=None,
        show_id=context.show_id,
        season_number=context.season_number,
    )

    posts = scraper.scrape(config)
    post_count = 0
    comment_count = 0

    for post in posts:
        caption = str(getattr(post, "caption", "") or "")
        if not _text_contains_any_term(text=caption, hashtags=hashtags, keywords=keywords):
            continue
        if post_count >= opts.max_posts_per_target:
            break

        upserted = _upsert_instagram_post(db, context, job_id=job_id, account=account, post=post)
        if not upserted:
            continue
        post_count += 1

        comments = scraper.fetch_comments(
            getattr(post, "shortcode", ""),
            max_comments=opts.max_comments_per_post,
            fetch_replies=opts.fetch_replies,
            delay=0.5,
        )
        for comment in comments:
            comment_count += _upsert_instagram_comment_tree(
                db,
                context,
                job_id=job_id,
                account=account,
                post_id=str(upserted["id"]),
                comment=comment,
            )

    return post_count, comment_count


def _upsert_tiktok_post(
    db: Any, context: SeasonContext, *, job_id: str, account: str, post: Any
) -> dict[str, Any] | None:
    posted_at = _parse_tiktok_time(getattr(post, "create_time", None))
    payload = {
        "video_id": getattr(post, "video_id", ""),
        "aweme_id": getattr(post, "video_id", ""),
        "username": getattr(post, "username", account),
        "user_id": None,
        "nickname": getattr(post, "author_nickname", None),
        "description": getattr(post, "description", None),
        "hashtags": getattr(post, "hashtags", []) or [],
        "music_info": {
            "title": getattr(post, "music_title", None),
            "author": getattr(post, "music_author", None),
        },
        "likes": int(getattr(post, "likes", 0) or 0),
        "comments_count": int(getattr(post, "comments", 0) or 0),
        "shares": int(getattr(post, "shares", 0) or 0),
        "views": int(getattr(post, "views", 0) or 0),
        "duration_seconds": int(getattr(post, "duration", 0) or 0),
        "posted_at": posted_at,
        "scraped_at": _now_utc(),
        "raw_data": post.to_dict() if hasattr(post, "to_dict") else {},
        "show_id": context.show_id,
        "season_id": context.season_id,
        "job_id": job_id,
        "source_account": account,
    }
    resp = db.schema("social").table("tiktok_posts").upsert(payload, on_conflict="video_id").execute()
    if hasattr(resp, "error") and resp.error:
        raise RuntimeError(f"tiktok_posts upsert failed: {resp.error}")
    rows = resp.data or []
    return rows[0] if rows else None


def _upsert_tiktok_comment_tree(
    db: Any,
    context: SeasonContext,
    *,
    job_id: str,
    account: str,
    post_id: str,
    comment: Any,
    parent_comment_db_id: str | None = None,
) -> int:
    created_at = _parse_tiktok_time(getattr(comment, "created_at", None))
    payload = {
        "comment_id": getattr(comment, "comment_id", ""),
        "post_id": post_id,
        "parent_comment_id": parent_comment_db_id,
        "username": getattr(comment, "username", ""),
        "user_id": getattr(comment, "user_id", None),
        "nickname": getattr(comment, "nickname", None),
        "text": getattr(comment, "text", ""),
        "likes": int(getattr(comment, "likes", 0) or 0),
        "is_reply": bool(getattr(comment, "is_reply", False)),
        "reply_count": int(getattr(comment, "reply_count", 0) or 0),
        "created_at": created_at,
        "scraped_at": _now_utc(),
        "raw_data": comment.to_dict() if hasattr(comment, "to_dict") else {},
        "season_id": context.season_id,
        "job_id": job_id,
        "source_account": account,
    }
    resp = db.schema("social").table("tiktok_comments").upsert(payload, on_conflict="comment_id").execute()
    if hasattr(resp, "error") and resp.error:
        raise RuntimeError(f"tiktok_comments upsert failed: {resp.error}")
    row = (resp.data or [{}])[0]
    comment_db_id = row.get("id")

    total = 1
    for reply in getattr(comment, "replies", []) or []:
        total += _upsert_tiktok_comment_tree(
            db,
            context,
            job_id=job_id,
            account=account,
            post_id=post_id,
            comment=reply,
            parent_comment_db_id=comment_db_id,
        )
    return total


def _ingest_tiktok(
    db: Any,
    context: SeasonContext,
    *,
    account: str,
    hashtags: list[str],
    keywords: list[str],
    opts: IngestOptions,
    job_id: str,
) -> tuple[int, int]:
    from trr_backend.socials.tiktok import TikTokScrapeConfig, TikTokScraper

    scraper = TikTokScraper()
    config = TikTokScrapeConfig(
        username=account,
        hashtags=[],
        date_start=opts.date_start,
        date_end=opts.date_end,
        delay_seconds=1.0,
        max_pages=None,
        show_id=context.show_id,
        season_number=context.season_number,
    )

    posts = scraper.scrape(config)
    post_count = 0
    comment_count = 0

    for post in posts:
        description = str(getattr(post, "description", "") or "")
        if not _text_contains_any_term(text=description, hashtags=hashtags, keywords=keywords):
            continue
        if post_count >= opts.max_posts_per_target:
            break

        upserted = _upsert_tiktok_post(db, context, job_id=job_id, account=account, post=post)
        if not upserted:
            continue
        post_count += 1

        comments = scraper.fetch_comments(
            getattr(post, "video_id", ""),
            username=account,
            max_comments=opts.max_comments_per_post,
            fetch_replies=opts.fetch_replies,
            delay=0.5,
        )
        for comment in comments:
            comment_count += _upsert_tiktok_comment_tree(
                db,
                context,
                job_id=job_id,
                account=account,
                post_id=str(upserted["id"]),
                comment=comment,
            )

    return post_count, comment_count


def _upsert_youtube_video(
    db: Any,
    context: SeasonContext,
    *,
    job_id: str,
    account: str,
    video: Any,
) -> dict[str, Any] | None:
    published_at = _parse_instagram_time(getattr(video, "published_at", None))
    payload = {
        "video_id": getattr(video, "video_id", ""),
        "channel_id": getattr(video, "channel_id", None),
        "channel_title": getattr(video, "channel_title", None),
        "title": getattr(video, "title", ""),
        "description": getattr(video, "description", None),
        "duration": getattr(video, "duration", None),
        "duration_seconds": int(getattr(video, "duration_seconds", 0) or 0),
        "views": int(getattr(video, "views", 0) or 0),
        "likes": int(getattr(video, "likes", 0) or 0),
        "comments_count": int(getattr(video, "comments", 0) or 0),
        "thumbnail_url": getattr(video, "thumbnail_url", None),
        "published_at": published_at,
        "scraped_at": _now_utc(),
        "raw_data": video.to_dict() if hasattr(video, "to_dict") else {},
        "show_id": context.show_id,
        "season_id": context.season_id,
        "job_id": job_id,
        "source_account": account,
    }
    resp = db.schema("social").table("youtube_videos").upsert(payload, on_conflict="video_id").execute()
    if hasattr(resp, "error") and resp.error:
        raise RuntimeError(f"youtube_videos upsert failed: {resp.error}")
    rows = resp.data or []
    return rows[0] if rows else None


def _upsert_youtube_comment_tree(
    db: Any,
    context: SeasonContext,
    *,
    job_id: str,
    account: str,
    video_db_id: str,
    comment: Any,
    parent_comment_db_id: str | None = None,
) -> int:
    created_at = _parse_instagram_time(getattr(comment, "created_at", None))
    payload = {
        "comment_id": getattr(comment, "comment_id", ""),
        "video_id": video_db_id,
        "parent_comment_id": parent_comment_db_id,
        "author": getattr(comment, "author", ""),
        "author_channel_id": getattr(comment, "author_channel_id", None),
        "text": getattr(comment, "text", ""),
        "likes": int(getattr(comment, "likes", 0) or 0),
        "is_reply": bool(getattr(comment, "is_reply", False)),
        "reply_count": int(getattr(comment, "reply_count", 0) or 0),
        "created_at": created_at,
        "scraped_at": _now_utc(),
        "raw_data": comment.to_dict() if hasattr(comment, "to_dict") else {},
        "season_id": context.season_id,
        "job_id": job_id,
        "source_account": account,
    }
    resp = db.schema("social").table("youtube_comments").upsert(payload, on_conflict="comment_id").execute()
    if hasattr(resp, "error") and resp.error:
        raise RuntimeError(f"youtube_comments upsert failed: {resp.error}")
    row = (resp.data or [{}])[0]
    comment_db_id = row.get("id")

    total = 1
    for reply in getattr(comment, "replies", []) or []:
        total += _upsert_youtube_comment_tree(
            db,
            context,
            job_id=job_id,
            account=account,
            video_db_id=video_db_id,
            comment=reply,
            parent_comment_db_id=comment_db_id,
        )
    return total


def _ingest_youtube(
    db: Any,
    context: SeasonContext,
    *,
    account: str,
    hashtags: list[str],
    keywords: list[str],
    opts: IngestOptions,
    job_id: str,
) -> tuple[int, int]:
    from trr_backend.socials.youtube import YouTubeScrapeConfig, YouTubeScraper

    scraper = YouTubeScraper()
    config = YouTubeScrapeConfig(
        channel_handle=account,
        keywords=keywords,
        date_start=opts.date_start,
        date_end=opts.date_end,
        delay_seconds=1.0,
        max_results=max(1, opts.max_posts_per_target),
        show_id=context.show_id,
        season_number=context.season_number,
    )

    videos = scraper.scrape(config)
    video_count = 0
    comment_count = 0

    for video in videos:
        combined_text = f"{getattr(video, 'title', '')} {getattr(video, 'description', '')}"
        if not _text_contains_any_term(text=combined_text, hashtags=hashtags, keywords=keywords):
            continue
        if video_count >= opts.max_posts_per_target:
            break

        upserted = _upsert_youtube_video(db, context, job_id=job_id, account=account, video=video)
        if not upserted:
            continue
        video_count += 1

        comments = scraper.fetch_comments(
            getattr(video, "video_id", ""),
            max_comments=opts.max_comments_per_post,
            fetch_replies=opts.fetch_replies,
            delay=0.5,
        )
        for comment in comments:
            comment_count += _upsert_youtube_comment_tree(
                db,
                context,
                job_id=job_id,
                account=account,
                video_db_id=str(upserted["id"]),
                comment=comment,
            )

    return video_count, comment_count


def _upsert_tweet(db: Any, context: SeasonContext, *, job_id: str, account: str, tweet: Any) -> dict[str, Any] | None:
    created_at = _parse_instagram_time(getattr(tweet, "created_at", None))
    payload = {
        "tweet_id": getattr(tweet, "tweet_id", ""),
        "username": getattr(tweet, "username", ""),
        "display_name": getattr(tweet, "display_name", None),
        "user_verified": bool(getattr(tweet, "user_verified", False)),
        "text": getattr(tweet, "text", ""),
        "hashtags": getattr(tweet, "hashtags", []) or [],
        "mentions": getattr(tweet, "mentions", []) or [],
        "media_urls": getattr(tweet, "media_urls", []) or [],
        "likes": int(getattr(tweet, "likes", 0) or 0),
        "retweets": int(getattr(tweet, "retweets", 0) or 0),
        "replies_count": int(getattr(tweet, "replies", 0) or 0),
        "quotes": int(getattr(tweet, "quotes", 0) or 0),
        "views": int(getattr(tweet, "views", 0) or 0),
        "is_reply": bool(getattr(tweet, "is_reply", False)),
        "is_retweet": bool(getattr(tweet, "is_retweet", False)),
        "is_quote": bool(getattr(tweet, "is_quote", False)),
        "reply_to_tweet_id": getattr(tweet, "reply_to_tweet_id", None),
        "quoted_tweet_id": getattr(tweet, "quoted_tweet_id", None),
        "created_at": created_at,
        "scraped_at": _now_utc(),
        "raw_data": tweet.to_dict() if hasattr(tweet, "to_dict") else {},
        "show_id": context.show_id,
        "season_id": context.season_id,
        "job_id": job_id,
        "source_account": account,
    }
    resp = db.schema("social").table("twitter_tweets").upsert(payload, on_conflict="tweet_id").execute()
    if hasattr(resp, "error") and resp.error:
        raise RuntimeError(f"twitter_tweets upsert failed: {resp.error}")
    rows = resp.data or []
    return rows[0] if rows else None


def _ingest_twitter(
    db: Any,
    context: SeasonContext,
    *,
    account: str,
    hashtags: list[str],
    keywords: list[str],
    opts: IngestOptions,
    job_id: str,
) -> tuple[int, int]:
    from trr_backend.socials.twitter import TwitterScrapeConfig, TwitterScraper

    date_start = opts.date_start or datetime.combine(context.anchor_date, time.min, tzinfo=UTC)
    date_end = opts.date_end or _now_utc()
    keyword_list = [kw for kw in keywords if isinstance(kw, str) and kw.strip()]
    hashtag_list = [tag for tag in hashtags if isinstance(tag, str) and tag.strip()]
    term_query = _build_twitter_or_query(hashtags=hashtag_list, keywords=keyword_list)
    query = f"from:{account} {term_query}".strip() if term_query else f"from:{account}"

    config = TwitterScrapeConfig(
        query=query or account,
        date_start=date_start,
        date_end=date_end,
        include_replies=True,
        include_links=True,
        delay_seconds=1.0,
        max_pages=10,
        show_id=context.show_id,
        season_number=context.season_number,
    )
    scraper = TwitterScraper()
    tweets = scraper.scrape(config)[: opts.max_posts_per_target]

    post_count = 0
    reply_count = 0
    for tweet in tweets:
        if not _text_contains_any_term(
            text=str(getattr(tweet, "text", "") or ""),
            hashtags=hashtag_list,
            keywords=keyword_list,
        ):
            continue
        upserted = _upsert_tweet(db, context, job_id=job_id, account=account, tweet=tweet)
        if not upserted:
            continue
        if bool(getattr(tweet, "is_reply", False)):
            reply_count += 1
        else:
            post_count += 1
    return post_count, reply_count


def ingest_season(
    db: Any,
    season_id: str,
    *,
    platforms: list[str] | None,
    source_scope: str,
    max_posts_per_target: int,
    max_comments_per_post: int,
    fetch_replies: bool,
    date_start: datetime | None,
    date_end: datetime | None,
    initiated_by: str | None,
) -> dict[str, Any]:
    context = get_season_context(season_id)

    if source_scope not in SUPPORTED_SCOPES:
        raise ValueError(f"Unsupported source scope: {source_scope}")

    platform_filter = {p.strip().lower() for p in platforms or [] if isinstance(p, str) and p.strip()}
    if platform_filter:
        unsupported = platform_filter - set(SUPPORTED_PLATFORMS)
        if unsupported:
            raise ValueError(f"Unsupported platforms requested: {', '.join(sorted(unsupported))}")

    targets_payload = get_targets(season_id, source_scope=source_scope)
    targets = [
        target
        for target in targets_payload.get("targets", [])
        if target.get("is_active", True)
        and (not platform_filter or str(target.get("platform") or "").lower() in platform_filter)
    ]

    if not targets:
        return {
            "season_id": context.season_id,
            "show_id": context.show_id,
            "season_number": context.season_number,
            "source_scope": source_scope,
            "results": [],
            "message": "No active targets configured for selected platforms",
        }

    opts = IngestOptions(
        platforms=platform_filter or None,
        source_scope=source_scope,
        max_posts_per_target=max(1, max_posts_per_target),
        max_comments_per_post=max(1, max_comments_per_post),
        fetch_replies=fetch_replies,
        date_start=date_start,
        date_end=date_end,
    )

    results: list[IngestResult] = []

    for target in targets:
        platform = str(target.get("platform") or "").lower()
        accounts = [str(item).strip() for item in (target.get("accounts") or []) if str(item).strip()]
        target_hashtags = [
            str(item).strip().lstrip("#") for item in (target.get("hashtags") or []) if str(item).strip()
        ]
        target_keywords = [str(item).strip() for item in (target.get("keywords") or []) if str(item).strip()]
        fallback_hashtags, fallback_keywords = _derive_show_terms(context.show_name)
        hashtags = _normalize_unique_terms([*target_hashtags, *fallback_hashtags])
        keywords = _normalize_unique_terms([*target_keywords, *fallback_keywords])

        if not accounts:
            continue

        for account in accounts:
            job_config = {
                "season_id": context.season_id,
                "show_id": context.show_id,
                "platform": platform,
                "account": account,
                "hashtags": hashtags,
                "keywords": keywords,
                "date_start": _iso(opts.date_start),
                "date_end": _iso(opts.date_end),
                "max_posts_per_target": opts.max_posts_per_target,
                "max_comments_per_post": opts.max_comments_per_post,
                "fetch_replies": opts.fetch_replies,
            }
            job_id = _create_job(
                context,
                platform=platform,
                source_scope=source_scope,
                job_type="comments",
                config=job_config,
                initiated_by=initiated_by,
            )

            try:
                if platform == "instagram":
                    posts_count, comments_count = _ingest_instagram(
                        db,
                        context,
                        account=account,
                        hashtags=hashtags,
                        keywords=keywords,
                        opts=opts,
                        job_id=job_id,
                    )
                elif platform == "tiktok":
                    posts_count, comments_count = _ingest_tiktok(
                        db,
                        context,
                        account=account,
                        hashtags=hashtags,
                        keywords=keywords,
                        opts=opts,
                        job_id=job_id,
                    )
                elif platform == "youtube":
                    posts_count, comments_count = _ingest_youtube(
                        db,
                        context,
                        account=account,
                        hashtags=hashtags,
                        keywords=keywords,
                        opts=opts,
                        job_id=job_id,
                    )
                elif platform == "twitter":
                    posts_count, comments_count = _ingest_twitter(
                        db,
                        context,
                        account=account,
                        hashtags=hashtags,
                        keywords=keywords,
                        opts=opts,
                        job_id=job_id,
                    )
                else:
                    raise RuntimeError(f"Platform {platform} ingest is not supported in V1")

                _finish_job(
                    job_id,
                    status="completed",
                    items_found=posts_count + comments_count,
                    metadata={
                        "posts": posts_count,
                        "comments": comments_count,
                        "account": account,
                    },
                )
                results.append(
                    IngestResult(
                        platform=platform,
                        source_scope=source_scope,
                        account=account,
                        job_id=job_id,
                        status="completed",
                        posts=posts_count,
                        comments=comments_count,
                    )
                )
            except Exception as exc:  # noqa: BLE001
                logger.exception("Social ingest failed: season=%s platform=%s account=%s", season_id, platform, account)
                _finish_job(
                    job_id,
                    status="failed",
                    items_found=0,
                    error_message=str(exc),
                    metadata={"account": account},
                )
                results.append(
                    IngestResult(
                        platform=platform,
                        source_scope=source_scope,
                        account=account,
                        job_id=job_id,
                        status="failed",
                        posts=0,
                        comments=0,
                        error=str(exc),
                    )
                )

    return {
        "season_id": context.season_id,
        "show_id": context.show_id,
        "season_number": context.season_number,
        "source_scope": source_scope,
        "results": [
            {
                "platform": result.platform,
                "source_scope": result.source_scope,
                "account": result.account,
                "job_id": result.job_id,
                "status": result.status,
                "posts": result.posts,
                "comments": result.comments,
                "error": result.error,
            }
            for result in results
        ],
    }


def list_jobs(season_id: str, *, limit: int = 50) -> list[dict[str, Any]]:
    safe_limit = max(1, min(int(limit), 250))
    return pg.fetch_all(
        """
        select
          id::text,
          platform,
          job_type,
          status,
          items_found,
          error_message,
          started_at,
          completed_at,
          created_at,
          config,
          metadata,
          source_scope,
          initiated_by
        from social.scrape_jobs
        where season_id = %s
        order by created_at desc
        limit %s
        """,
        [season_id, safe_limit],
    )


# ---------------------------------------------------------------------------
# Analytics + exports
# ---------------------------------------------------------------------------


def sentiment_for_text(text: str | None) -> tuple[str, int]:
    if not text:
        return "neutral", 0

    tokens = [token.lower() for token in TOKEN_RE.findall(text)]
    positive = sum(1 for token in tokens if token in POSITIVE_WORDS)
    negative = sum(1 for token in tokens if token in NEGATIVE_WORDS)
    score = positive - negative
    if score > 0:
        return "positive", score
    if score < 0:
        return "negative", score
    return "neutral", 0


def _text_is_trailer_marker(value: str | None) -> bool:
    if not value:
        return False
    return bool(TRAILER_MARKER_RE.search(value))


def _video_matches_season(video: dict[str, Any], season_number: int) -> bool:
    raw = video.get("season_number")
    if isinstance(raw, int):
        return raw == season_number
    if isinstance(raw, str):
        stripped = raw.strip()
        if stripped.isdigit():
            return int(stripped) == season_number

    searchable = f"{video.get('title') or ''} {video.get('kicker') or ''}".lower()
    return f"season {season_number}" in searchable or f"s{season_number}" in searchable


def _load_bravo_snapshot_videos(show_id: str, season_number: int) -> list[dict[str, Any]]:
    row = pg.fetch_one(
        """
        select payload
        from core.show_source_latest
        where show_id = %s
          and source_id = 'bravo'
          and variant = 'default'
        limit 1
        """,
        [show_id],
    )
    if not row:
        return []

    payload = row.get("payload")
    if not isinstance(payload, dict):
        return []

    normalized = payload.get("normalized")
    if not isinstance(normalized, dict):
        return []

    videos_raw = normalized.get("videos_show")
    if not isinstance(videos_raw, list):
        return []

    videos: list[dict[str, Any]] = []
    for item in videos_raw:
        if not isinstance(item, dict):
            continue
        if not _video_matches_season(item, season_number):
            continue
        videos.append(item)
    return videos


def _find_week_zero_start_from_snapshot(
    *,
    context: SeasonContext,
    premiere_local: datetime,
    timezone: str,
) -> datetime | None:
    zone = ZoneInfo(timezone)
    candidates: list[tuple[datetime, bool]] = []

    for video in _load_bravo_snapshot_videos(context.show_id, context.season_number):
        published_at = _coerce_dt(video.get("published_at"))
        if not published_at:
            continue
        published_local = published_at.astimezone(zone)
        if published_local >= premiere_local:
            continue
        marker_hit = _text_is_trailer_marker(
            f"{video.get('title') or ''} {video.get('kicker') or ''}"
        )
        candidates.append((published_local, marker_hit))

    if not candidates:
        return None

    marker_only = [value for value, marker in candidates if marker]
    if marker_only:
        return min(marker_only)
    return min(value for value, _ in candidates)


def _find_week_zero_start_from_social_rows(
    *,
    season_id: str,
    season_number: int,
    premiere_utc: datetime,
    timezone: str,
) -> datetime | None:
    lookback_utc = premiere_utc - timedelta(days=180)
    rows = pg.fetch_all(
        """
        with source_rows as (
          select posted_at as ts, caption as text from social.instagram_posts where season_id = %s
          union all
          select posted_at as ts, description as text from social.tiktok_posts where season_id = %s
          union all
          select created_at as ts, text as text from social.twitter_tweets where season_id = %s
          union all
          select published_at as ts, coalesce(title, '') || ' ' || coalesce(description, '') as text
          from social.youtube_videos
          where season_id = %s
        )
        select ts, text
        from source_rows
        where ts is not null
          and ts >= %s
          and ts < %s
        order by ts asc
        """,
        [season_id, season_id, season_id, season_id, lookback_utc, premiere_utc],
    )
    if not rows:
        return None

    zone = ZoneInfo(timezone)
    trailer_like_season_specific: list[datetime] = []
    trailer_like_generic: list[datetime] = []
    season_marker_variants = (
        f"season {season_number}",
        f"s{season_number}",
    )
    for row in rows:
        ts = _coerce_dt(row.get("ts"))
        if ts is None:
            continue
        text = str(row.get("text") or "")
        if not _text_is_trailer_marker(text):
            continue
        text_lower = text.lower()
        ts_local = ts.astimezone(zone)
        if any(marker in text_lower for marker in season_marker_variants):
            trailer_like_season_specific.append(ts_local)
        else:
            trailer_like_generic.append(ts_local)

    if trailer_like_season_specific:
        return min(trailer_like_season_specific)
    if trailer_like_generic:
        return min(trailer_like_generic)
    return _coerce_dt(rows[0].get("ts")).astimezone(zone) if rows[0].get("ts") else None


def _parse_local_override_datetime(value: Any, timezone: str) -> datetime | None:
    zone = ZoneInfo(timezone)
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo:
            return value.astimezone(zone)
        return value.replace(tzinfo=zone)
    if isinstance(value, date):
        return datetime.combine(value, time.min, tzinfo=zone)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw):
            parsed_date = datetime.fromisoformat(raw).date()
            return datetime.combine(parsed_date, time.min, tzinfo=zone)
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo:
            return parsed.astimezone(zone)
        return parsed.replace(tzinfo=zone)
    return None


def _find_week_zero_start_override(
    *,
    season_id: str,
    source_scope: str,
    timezone: str,
    premiere_local: datetime,
) -> datetime | None:
    rows = pg.fetch_all(
        """
        select config
        from social.season_targets
        where season_id = %s
          and source_scope = %s
          and is_active = true
        """,
        [season_id, source_scope],
    )
    if not rows:
        return None

    candidates: list[datetime] = []
    for row in rows:
        config = row.get("config")
        if not isinstance(config, dict):
            continue
        for key in ("preseason_start", "preseason_start_at", "week_zero_start"):
            parsed = _parse_local_override_datetime(config.get(key), timezone)
            if parsed is None:
                continue
            if parsed >= premiere_local:
                continue
            candidates.append(parsed)
            break

    if not candidates:
        return None
    return min(candidates)


def _resolve_week_windows(
    context: SeasonContext,
    *,
    timezone: str,
    source_scope: str,
    now_utc: datetime,
) -> tuple[list[WeekWindow], datetime]:
    zone = ZoneInfo(timezone)
    now_local = now_utc.astimezone(zone)
    episode_rows = pg.fetch_all(
        """
        select episode_number, air_date
        from core.episodes
        where season_id = %s
          and air_date is not null
        order by episode_number asc, air_date asc
        """,
        [context.season_id],
    )

    episode_starts: list[tuple[int, datetime]] = []
    seen_numbers: set[int] = set()
    for row in episode_rows:
        raw_num = row.get("episode_number")
        air_date = row.get("air_date")
        if not isinstance(raw_num, int) or raw_num < 1 or not isinstance(air_date, date):
            continue
        if raw_num in seen_numbers:
            continue
        seen_numbers.add(raw_num)
        episode_starts.append((raw_num, datetime.combine(air_date, time(20, 0), tzinfo=zone)))

    if not episode_starts:
        anchor_local = datetime.combine(context.anchor_date, time.min, tzinfo=zone)
        return [WeekWindow(1, anchor_local, now_local)], anchor_local

    premiere_local = episode_starts[0][1]
    trailer_start_local = _find_week_zero_start_override(
        season_id=context.season_id,
        source_scope=source_scope,
        timezone=timezone,
        premiere_local=premiere_local,
    )
    if trailer_start_local is None:
        trailer_start_local = _find_week_zero_start_from_snapshot(
        context=context,
        premiere_local=premiere_local,
        timezone=timezone,
        )
    if trailer_start_local is None:
        trailer_start_local = _find_week_zero_start_from_social_rows(
            season_id=context.season_id,
            season_number=context.season_number,
            premiere_utc=premiere_local.astimezone(UTC),
            timezone=timezone,
        )
    if trailer_start_local is None:
        trailer_start_local = premiere_local - timedelta(days=7)
    if trailer_start_local >= premiere_local:
        trailer_start_local = premiere_local - timedelta(days=1)

    windows: list[WeekWindow] = [WeekWindow(0, trailer_start_local, premiere_local)]

    for idx, (episode_number, episode_start) in enumerate(episode_starts):
        if idx + 1 < len(episode_starts):
            episode_end = episode_starts[idx + 1][1]
        else:
            episode_end = now_local
        if episode_end <= episode_start:
            continue
        windows.append(WeekWindow(episode_number, episode_start, episode_end))

    return windows, trailer_start_local


def _week_for_timestamp(ts: datetime, *, windows: list[WeekWindow], timezone: str) -> WeekWindow | None:
    local_dt = ts.astimezone(ZoneInfo(timezone))
    for window in windows:
        if window.start_local <= local_dt < window.end_local:
            return window
    return None


def _rows_for_platform(
    season_id: str,
    *,
    platform: str,
    start_dt: datetime,
    end_dt: datetime,
    source_scope: str,
) -> list[dict[str, Any]]:
    bravo_scope = source_scope == "bravo"

    if platform == "instagram":
        account_filter_posts = (
            "and lower(coalesce(p.username, p.source_account, '')) in ('bravotv', 'bravo')"
            if bravo_scope
            else ""
        )
        account_filter_comments = (
            "and lower(coalesce(p.username, p.source_account, '')) in ('bravotv', 'bravo')"
            if bravo_scope
            else ""
        )
        return pg.fetch_all(
            f"""
            with posts as (
              select
                'instagram'::text as platform,
                'post'::text as kind,
                p.shortcode as source_id,
                p.caption as text,
                greatest(
                  0,
                  coalesce(p.likes, 0) + coalesce(p.comments_count, 0) + coalesce(p.views, 0)
                )::bigint as engagement,
                p.posted_at as ts,
                ('https://www.instagram.com/p/' || p.shortcode || '/')::text as url,
                p.username as author
              from social.instagram_posts p
              where p.season_id = %s
                and p.posted_at >= %s
                and p.posted_at <= %s
                {account_filter_posts}
            ), comments as (
              select
                'instagram'::text as platform,
                'comment'::text as kind,
                c.comment_id as source_id,
                c.text as text,
                greatest(0, coalesce(c.likes, 0))::bigint as engagement,
                c.created_at as ts,
                ('https://www.instagram.com/p/' || p.shortcode || '/')::text as url,
                c.username as author
              from social.instagram_comments c
              join social.instagram_posts p on p.id = c.post_id
              where c.season_id = %s
                and c.created_at >= %s
                and c.created_at <= %s
                {account_filter_comments}
            )
            select * from posts
            union all
            select * from comments
            """,
            [season_id, start_dt, end_dt, season_id, start_dt, end_dt],
        )

    if platform == "tiktok":
        account_filter_posts = (
            "and lower(coalesce(p.username, p.source_account, '')) in ('bravotv', 'bravo')"
            if bravo_scope
            else ""
        )
        account_filter_comments = (
            "and lower(coalesce(p.username, p.source_account, '')) in ('bravotv', 'bravo')"
            if bravo_scope
            else ""
        )
        return pg.fetch_all(
            f"""
            with posts as (
              select
                'tiktok'::text as platform,
                'post'::text as kind,
                p.video_id as source_id,
                p.description as text,
                greatest(
                  0,
                  coalesce(p.likes, 0)
                  + coalesce(p.comments_count, 0)
                  + coalesce(p.shares, 0)
                  + coalesce(p.views, 0)
                )::bigint as engagement,
                p.posted_at as ts,
                ('https://www.tiktok.com/@' || p.username || '/video/' || p.video_id)::text as url,
                p.username as author
              from social.tiktok_posts p
              where p.season_id = %s
                and p.posted_at >= %s
                and p.posted_at <= %s
                {account_filter_posts}
            ), comments as (
              select
                'tiktok'::text as platform,
                'comment'::text as kind,
                c.comment_id as source_id,
                c.text as text,
                greatest(0, coalesce(c.likes, 0))::bigint as engagement,
                c.created_at as ts,
                ('https://www.tiktok.com/@' || p.username || '/video/' || p.video_id)::text as url,
                c.username as author
              from social.tiktok_comments c
              join social.tiktok_posts p on p.id = c.post_id
              where c.season_id = %s
                and c.created_at >= %s
                and c.created_at <= %s
                {account_filter_comments}
            )
            select * from posts
            union all
            select * from comments
            """,
            [season_id, start_dt, end_dt, season_id, start_dt, end_dt],
        )

    if platform == "youtube":
        account_filter_videos = (
            "and lower(coalesce(v.channel_title, v.source_account, '')) in ('bravo', 'bravotv')"
            if bravo_scope
            else ""
        )
        account_filter_comments = (
            "and lower(coalesce(v.channel_title, v.source_account, '')) in ('bravo', 'bravotv')"
            if bravo_scope
            else ""
        )
        return pg.fetch_all(
            f"""
            with videos as (
              select
                'youtube'::text as platform,
                'post'::text as kind,
                v.video_id as source_id,
                v.title as text,
                greatest(
                  0,
                  coalesce(v.views, 0) + coalesce(v.likes, 0) + coalesce(v.comments_count, 0)
                )::bigint as engagement,
                v.published_at as ts,
                ('https://www.youtube.com/watch?v=' || v.video_id)::text as url,
                v.channel_title as author
              from social.youtube_videos v
              where v.season_id = %s
                and v.published_at >= %s
                and v.published_at <= %s
                {account_filter_videos}
            ), comments as (
              select
                'youtube'::text as platform,
                'comment'::text as kind,
                c.comment_id as source_id,
                c.text as text,
                greatest(0, coalesce(c.likes, 0))::bigint as engagement,
                c.created_at as ts,
                ('https://www.youtube.com/watch?v=' || v.video_id)::text as url,
                c.author as author
              from social.youtube_comments c
              join social.youtube_videos v on v.id = c.video_id
              where c.season_id = %s
                and c.created_at >= %s
                and c.created_at <= %s
                {account_filter_comments}
            )
            select * from videos
            union all
            select * from comments
            """,
            [season_id, start_dt, end_dt, season_id, start_dt, end_dt],
        )

    if platform == "twitter":
        account_filter = (
            "and lower(coalesce(t.username, t.source_account, '')) in ('bravotv', 'bravo')"
            if bravo_scope
            else ""
        )
        return pg.fetch_all(
            f"""
            select
              'twitter'::text as platform,
              case when t.is_reply then 'comment' else 'post' end as kind,
              t.tweet_id as source_id,
              t.text as text,
              greatest(
                0,
                coalesce(t.likes, 0)
                + coalesce(t.retweets, 0)
                + coalesce(t.replies_count, 0)
                + coalesce(t.quotes, 0)
                + coalesce(t.views, 0)
              )::bigint as engagement,
              t.created_at as ts,
              ('https://x.com/' || t.username || '/status/' || t.tweet_id)::text as url,
              t.username as author
            from social.twitter_tweets t
            where t.season_id = %s
              and t.created_at >= %s
              and t.created_at <= %s
              {account_filter}
            """,
            [season_id, start_dt, end_dt],
        )

    return []


def _build_rows(
    season_id: str,
    *,
    platforms: list[str],
    start_dt: datetime,
    end_dt: datetime,
    source_scope: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for platform in platforms:
        rows.extend(
            _rows_for_platform(
                season_id,
                platform=platform,
                start_dt=start_dt,
                end_dt=end_dt,
                source_scope=source_scope,
            )
        )

    normalized: list[dict[str, Any]] = []
    for row in rows:
        ts = _coerce_dt(row.get("ts"))
        if ts is None:
            continue
        kind = str(row.get("kind") or "").lower()
        text_value = row.get("text")
        text = str(text_value) if text_value is not None else ""
        sentiment, score = sentiment_for_text(text)
        normalized.append(
            {
                "platform": str(row.get("platform") or ""),
                "kind": kind,
                "source_id": str(row.get("source_id") or ""),
                "text": text,
                "engagement": int(row.get("engagement") or 0),
                "ts": ts,
                "url": str(row.get("url") or ""),
                "author": str(row.get("author") or ""),
                "sentiment": sentiment,
                "sentiment_score": score,
            }
        )

    normalized.sort(key=lambda item: item["ts"], reverse=True)
    return normalized


def _build_drivers(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    token_counts: Counter[str] = Counter()
    token_scores: defaultdict[str, int] = defaultdict(int)

    for row in rows:
        if row["kind"] != "comment":
            continue
        text = row.get("text") or ""
        sentiment_score = int(row.get("sentiment_score") or 0)
        tokens = [token.lower() for token in TOKEN_RE.findall(text)]
        for token in tokens:
            if len(token) < 4 or token in STOPWORDS:
                continue
            token_counts[token] += 1
            token_scores[token] += sentiment_score

    positive = [
        {
            "term": term,
            "count": token_counts[term],
            "score": token_scores[term],
        }
        for term in token_counts
        if token_counts[term] >= 2 and token_scores[term] > 0
    ]
    negative = [
        {
            "term": term,
            "count": token_counts[term],
            "score": token_scores[term],
        }
        for term in token_counts
        if token_counts[term] >= 2 and token_scores[term] < 0
    ]

    positive.sort(key=lambda item: (-item["score"], -item["count"], item["term"]))
    negative.sort(key=lambda item: (item["score"], -item["count"], item["term"]))

    return {
        "positive": positive[:10],
        "negative": negative[:10],
    }


def get_analytics(
    season_id: str,
    *,
    platforms: list[str] | None,
    timezone: str,
    week: int | None,
    source_scope: str,
    include_rows: bool = False,
) -> dict[str, Any]:
    context = get_season_context(season_id)
    available_platforms = [
        platform for platform in (platforms or list(SUPPORTED_PLATFORMS)) if platform in SUPPORTED_PLATFORMS
    ]
    if not available_platforms:
        available_platforms = list(SUPPORTED_PLATFORMS)

    now = _now_utc()
    week_windows, week_zero_start_local = _resolve_week_windows(
        context,
        timezone=timezone,
        source_scope=source_scope,
        now_utc=now,
    )
    windows_by_index = {item.week_index: item for item in week_windows}

    selected_window = windows_by_index.get(week) if week is not None else None
    if week is not None and selected_window is None:
        raise ValueError(f"Week {week} is not available for this season")

    if selected_window:
        start_dt = selected_window.start_local.astimezone(UTC)
        end_dt = (selected_window.end_local - timedelta(microseconds=1)).astimezone(UTC)
    else:
        start_dt = week_zero_start_local.astimezone(UTC)
        end_dt = now
    if end_dt < start_dt:
        end_dt = start_dt

    rows = _build_rows(
        season_id,
        platforms=available_platforms,
        start_dt=start_dt,
        end_dt=end_dt,
        source_scope=source_scope,
    )

    posts = [row for row in rows if row["kind"] == "post"]
    comments = [row for row in rows if row["kind"] == "comment"]

    sentiment_counts = {
        "positive": sum(1 for row in comments if row["sentiment"] == "positive"),
        "neutral": sum(1 for row in comments if row["sentiment"] == "neutral"),
        "negative": sum(1 for row in comments if row["sentiment"] == "negative"),
    }

    total_comments = max(1, len(comments))
    sentiment_mix = {
        "positive": round(sentiment_counts["positive"] / total_comments, 4),
        "neutral": round(sentiment_counts["neutral"] / total_comments, 4),
        "negative": round(sentiment_counts["negative"] / total_comments, 4),
        "counts": sentiment_counts,
    }

    visible_windows = [selected_window] if selected_window else week_windows
    weekly_map: dict[int, dict[str, Any]] = {
        item.week_index: {
            "post_volume": 0,
            "comment_volume": 0,
            "engagement": 0,
            "sentiment": {"positive": 0, "neutral": 0, "negative": 0},
            "week_start": item.start_local,
            "week_end": item.end_local,
        }
        for item in visible_windows
    }

    for row in rows:
        week_window = _week_for_timestamp(row["ts"], windows=visible_windows, timezone=timezone)
        if not week_window:
            continue
        entry = weekly_map[week_window.week_index]
        entry["engagement"] += int(row["engagement"])
        if row["kind"] == "post":
            entry["post_volume"] += 1
        else:
            entry["comment_volume"] += 1
            entry["sentiment"][row["sentiment"]] += 1

    weekly = []
    for week_index in sorted(weekly_map):
        entry = weekly_map[week_index]
        week_end_inclusive = entry["week_end"] - timedelta(microseconds=1)
        weekly.append(
            {
                "week_index": week_index,
                "label": "Pre-Season" if week_index == 0 else f"Week {week_index}",
                "start": _iso(entry["week_start"].astimezone(UTC)),
                "end": _iso(week_end_inclusive.astimezone(UTC)),
                "post_volume": entry["post_volume"],
                "comment_volume": entry["comment_volume"],
                "engagement": entry["engagement"],
                "sentiment": entry["sentiment"],
            }
        )

    weekly_platform_posts_map: dict[int, dict[str, int]] = {
        item.week_index: dict.fromkeys(SUPPORTED_PLATFORMS, 0)
        for item in visible_windows
    }
    for row in rows:
        if row["kind"] != "post":
            continue
        week_window = _week_for_timestamp(row["ts"], windows=visible_windows, timezone=timezone)
        if not week_window:
            continue
        platform = row["platform"]
        if platform in weekly_platform_posts_map[week_window.week_index]:
            weekly_platform_posts_map[week_window.week_index][platform] += 1

    weekly_platform_posts: list[dict[str, Any]] = []
    for week_entry in weekly:
        week_index = int(week_entry["week_index"])
        platform_posts = weekly_platform_posts_map.get(
            week_index,
            dict.fromkeys(SUPPORTED_PLATFORMS, 0),
        )
        weekly_platform_posts.append(
            {
                "week_index": week_index,
                "label": week_entry["label"],
                "start": week_entry["start"],
                "end": week_entry["end"],
                "posts": {
                    "instagram": int(platform_posts.get("instagram", 0)),
                    "youtube": int(platform_posts.get("youtube", 0)),
                    "tiktok": int(platform_posts.get("tiktok", 0)),
                    "twitter": int(platform_posts.get("twitter", 0)),
                },
                "total_posts": int(sum(platform_posts.values())),
            }
        )

    platform_breakdown = []
    for platform in available_platforms:
        platform_rows = [row for row in rows if row["platform"] == platform]
        platform_comments = [row for row in platform_rows if row["kind"] == "comment"]
        platform_breakdown.append(
            {
                "platform": platform,
                "posts": sum(1 for row in platform_rows if row["kind"] == "post"),
                "comments": len(platform_comments),
                "engagement": sum(int(row["engagement"]) for row in platform_rows),
                "sentiment": {
                    "positive": sum(1 for row in platform_comments if row["sentiment"] == "positive"),
                    "neutral": sum(1 for row in platform_comments if row["sentiment"] == "neutral"),
                    "negative": sum(1 for row in platform_comments if row["sentiment"] == "negative"),
                },
            }
        )

    leaderboards = {
        "bravo_content": [
            {
                "platform": row["platform"],
                "source_id": row["source_id"],
                "text": row["text"][:240],
                "engagement": row["engagement"],
                "url": row["url"],
                "timestamp": _iso(row["ts"]),
            }
            for row in sorted(posts, key=lambda item: item["engagement"], reverse=True)[:15]
        ],
        "viewer_discussion": [
            {
                "platform": row["platform"],
                "source_id": row["source_id"],
                "text": row["text"][:240],
                "engagement": row["engagement"],
                "url": row["url"],
                "timestamp": _iso(row["ts"]),
                "sentiment": row["sentiment"],
            }
            for row in sorted(comments, key=lambda item: item["engagement"], reverse=True)[:20]
        ],
    }

    jobs = list_jobs(season_id, limit=25)

    response: dict[str, Any] = {
        "window": {
            "start": _iso(start_dt),
            "end": _iso(end_dt),
            "timezone": timezone,
            "week_anchor": str(context.anchor_date),
            "week_zero_start": _iso(week_zero_start_local.astimezone(UTC)),
            "week": week,
            "source_scope": source_scope,
        },
        "summary": {
            "show_id": context.show_id,
            "season_id": context.season_id,
            "season_number": context.season_number,
            "show_name": context.show_name,
            "total_posts": len(posts),
            "total_comments": len(comments),
            "total_engagement": sum(int(row["engagement"]) for row in rows),
            "sentiment_mix": sentiment_mix,
            "deltas": {
                "posts": None,
                "comments": None,
                "engagement": None,
            },
        },
        "weekly": weekly,
        "weekly_platform_posts": weekly_platform_posts,
        "platform_breakdown": platform_breakdown,
        "themes": _build_drivers(comments),
        "leaderboards": leaderboards,
        "jobs": jobs,
    }

    if include_rows:
        lookup_windows = visible_windows
        rows_payload: list[dict[str, Any]] = []
        for row in rows:
            bucket = _week_for_timestamp(row["ts"], windows=lookup_windows, timezone=timezone)
            rows_payload.append(
                {
                    "week_index": bucket.week_index if bucket else None,
                    "platform": row["platform"],
                    "kind": row["kind"],
                    "source_id": row["source_id"],
                    "timestamp": _iso(row["ts"]),
                    "author": row["author"],
                    "url": row["url"],
                    "engagement": row["engagement"],
                    "sentiment": row["sentiment"],
                    "text": row["text"],
                }
            )
        response["rows"] = rows_payload

    return response


def build_csv(snapshot: dict[str, Any]) -> str:
    rows = snapshot.get("rows") or []
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(
        [
            "week_index",
            "platform",
            "kind",
            "source_id",
            "timestamp",
            "author",
            "url",
            "engagement",
            "sentiment",
            "text",
        ]
    )
    for row in rows:
        writer.writerow(
            [
                row.get("week_index"),
                row.get("platform"),
                row.get("kind"),
                row.get("source_id"),
                row.get("timestamp"),
                row.get("author"),
                row.get("url"),
                row.get("engagement"),
                row.get("sentiment"),
                row.get("text"),
            ]
        )
    return output.getvalue()


def build_pdf(snapshot: dict[str, Any]) -> bytes:
    try:
        from reportlab.lib import colors
        from reportlab.lib.pagesizes import LETTER
        from reportlab.lib.styles import getSampleStyleSheet
        from reportlab.lib.units import inch
        from reportlab.platypus import PageBreak, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError("PDF export requires reportlab") from exc

    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=LETTER,
        leftMargin=0.6 * inch,
        rightMargin=0.6 * inch,
        topMargin=0.6 * inch,
        bottomMargin=0.6 * inch,
    )
    styles = getSampleStyleSheet()

    summary = snapshot.get("summary") or {}
    window = snapshot.get("window") or {}
    weekly = snapshot.get("weekly") or []
    platform_breakdown = snapshot.get("platform_breakdown") or []
    themes = snapshot.get("themes") or {}
    discussions = (snapshot.get("leaderboards") or {}).get("viewer_discussion") or []

    story = []
    story.append(Paragraph("Season Social Analytics Report", styles["Title"]))
    story.append(
        Paragraph(
            (
                f"Show: {summary.get('show_name') or 'Unknown'} | "
                f"Season: {summary.get('season_number') or 'N/A'} | "
                f"Window: {window.get('start')} to {window.get('end')} | "
                f"Timezone: {window.get('timezone') or 'America/New_York'}"
            ),
            styles["Normal"],
        )
    )
    story.append(Spacer(1, 0.2 * inch))

    summary_table = Table(
        [
            ["Metric", "Value"],
            ["Total Posts", str(summary.get("total_posts") or 0)],
            ["Total Comments", str(summary.get("total_comments") or 0)],
            ["Total Engagement", str(summary.get("total_engagement") or 0)],
            [
                "Sentiment Mix",
                (
                    f"P {((summary.get('sentiment_mix') or {}).get('positive') or 0):.1%} | "
                    f"N {((summary.get('sentiment_mix') or {}).get('neutral') or 0):.1%} | "
                    f"Neg {((summary.get('sentiment_mix') or {}).get('negative') or 0):.1%}"
                ),
            ],
        ],
        hAlign="LEFT",
        colWidths=[2.1 * inch, 4.6 * inch],
    )
    summary_table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#111827")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#d4d4d8")),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("ALIGN", (0, 0), (-1, -1), "LEFT"),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ]
        )
    )
    story.append(summary_table)
    story.append(Spacer(1, 0.2 * inch))

    if weekly:
        story.append(Paragraph("Weekly Trend", styles["Heading2"]))
        weekly_table_data = [["Week", "Posts", "Comments", "Engagement", "Positive", "Neutral", "Negative"]]
        for row in weekly[:16]:
            sentiment = row.get("sentiment") or {}
            weekly_table_data.append(
                [
                    row.get("label"),
                    row.get("post_volume"),
                    row.get("comment_volume"),
                    row.get("engagement"),
                    sentiment.get("positive", 0),
                    sentiment.get("neutral", 0),
                    sentiment.get("negative", 0),
                ]
            )
        weekly_table = Table(weekly_table_data, hAlign="LEFT")
        weekly_table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#334155")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                    ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#e4e4e7")),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ]
            )
        )
        story.append(weekly_table)
        story.append(Spacer(1, 0.2 * inch))

    if platform_breakdown:
        story.append(Paragraph("Platform Breakdown", styles["Heading2"]))
        platform_table_data = [["Platform", "Posts", "Comments", "Engagement", "Positive", "Neutral", "Negative"]]
        for row in platform_breakdown:
            sentiment = row.get("sentiment") or {}
            platform_table_data.append(
                [
                    row.get("platform"),
                    row.get("posts"),
                    row.get("comments"),
                    row.get("engagement"),
                    sentiment.get("positive", 0),
                    sentiment.get("neutral", 0),
                    sentiment.get("negative", 0),
                ]
            )
        platform_table = Table(platform_table_data, hAlign="LEFT")
        platform_table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1d4ed8")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                    ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#e4e4e7")),
                    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ]
            )
        )
        story.append(platform_table)

    story.append(PageBreak())
    story.append(Paragraph("Appendix", styles["Title"]))

    positive_themes = themes.get("positive") or []
    negative_themes = themes.get("negative") or []

    story.append(Paragraph("Top Positive Drivers", styles["Heading2"]))
    if positive_themes:
        for theme in positive_themes[:10]:
            story.append(
                Paragraph(
                    f"{theme.get('term')} (count {theme.get('count')}, score {theme.get('score')})",
                    styles["Normal"],
                )
            )
    else:
        story.append(Paragraph("No positive drivers identified.", styles["Normal"]))

    story.append(Spacer(1, 0.15 * inch))
    story.append(Paragraph("Top Negative Drivers", styles["Heading2"]))
    if negative_themes:
        for theme in negative_themes[:10]:
            story.append(
                Paragraph(
                    f"{theme.get('term')} (count {theme.get('count')}, score {theme.get('score')})",
                    styles["Normal"],
                )
            )
    else:
        story.append(Paragraph("No negative drivers identified.", styles["Normal"]))

    story.append(Spacer(1, 0.2 * inch))
    story.append(Paragraph("Viewer Discussion Highlights", styles["Heading2"]))
    discussion_table_data = [["Platform", "Sentiment", "Engagement", "Comment Excerpt"]]
    for row in discussions[:20]:
        text = str(row.get("text") or "").strip()
        excerpt = text if len(text) <= 140 else f"{text[:137]}..."
        discussion_table_data.append(
            [
                row.get("platform"),
                row.get("sentiment"),
                row.get("engagement"),
                excerpt,
            ]
        )

    if len(discussion_table_data) == 1:
        discussion_table_data.append(["-", "-", "-", "No comments available in this filter window."])

    discussion_table = Table(
        discussion_table_data,
        hAlign="LEFT",
        colWidths=[0.9 * inch, 0.9 * inch, 0.9 * inch, 4.9 * inch],
    )
    discussion_table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#0f766e")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#e4e4e7")),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ]
        )
    )
    story.append(discussion_table)

    doc.build(story)
    return buffer.getvalue()


def pdf_filename(show_id: str, season_number: int, generated_at: datetime | None = None) -> str:
    ts = (generated_at or _now_utc()).strftime("%Y%m%d")
    return f"social_report_{show_id}_s{season_number}_{ts}.pdf"
