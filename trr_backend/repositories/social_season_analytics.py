"""Season-scoped social analytics + ingest helpers."""

from __future__ import annotations

import csv
import io
import json
import logging
import os
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from trr_backend.db import pg

logger = logging.getLogger(__name__)

SUPPORTED_PLATFORMS = ("instagram", "tiktok", "twitter", "youtube")
SUPPORTED_SCOPES = ("bravo", "creator", "community")
SUPPORTED_INGEST_MODES = ("posts_only", "posts_and_comments")
SUPPORTED_DEPTH_PRESETS = ("quick", "balanced", "deep")
JOB_PROGRESS_UPDATE_EVERY = 25

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
    max_replies_per_post: int
    fetch_replies: bool
    ingest_mode: str
    depth_preset: str
    date_start: datetime | None
    date_end: datetime | None


# ---------------------------------------------------------------------------
# General helpers
# ---------------------------------------------------------------------------


def _now_utc() -> datetime:
    return datetime.now(tz=UTC)


def _env_truthy(name: str, *, default: bool = False) -> bool:
    raw = (os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def is_queue_enabled() -> bool:
    return _env_truthy("SOCIAL_QUEUE_ENABLED", default=False)


def _resolve_depth_defaults(
    *,
    depth_preset: str,
    max_posts_per_target: int,
    max_comments_per_post: int,
    max_replies_per_post: int,
    fetch_replies: bool,
) -> tuple[int, int, int, bool]:
    normalized = (depth_preset or "deep").strip().lower()
    if normalized not in SUPPORTED_DEPTH_PRESETS:
        normalized = "deep"

    if normalized == "quick":
        return (
            max(1, min(max_posts_per_target, 400)),
            max(0, min(max_comments_per_post, 20)),
            max(0, min(max_replies_per_post, 20)),
            False if max_comments_per_post > 0 else fetch_replies,
        )
    if normalized == "deep":
        return (
            max(1, max_posts_per_target),
            max(0, max(max_comments_per_post, 200)),
            max(0, max(max_replies_per_post, 100)),
            fetch_replies,
        )
    # balanced
    return (
        max(1, max_posts_per_target),
        max(0, max_comments_per_post),
        max(0, max_replies_per_post),
        fetch_replies,
    )


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


def _relation_exists(qualified_name: str) -> bool:
    row = pg.fetch_one("select to_regclass(%s) is not null as exists", [qualified_name]) or {}
    return bool(row.get("exists"))


def _column_exists(schema: str, table: str, column: str) -> bool:
    row = pg.fetch_one(
        """
        select exists (
          select 1
          from information_schema.columns
          where table_schema = %s
            and table_name = %s
            and column_name = %s
        ) as exists
        """,
        [schema, table, column],
    ) or {}
    return bool(row.get("exists"))


def _assert_social_queue_schema_ready() -> None:
    missing: list[str] = []
    if not _relation_exists("social.scrape_runs"):
        missing.append("social.scrape_runs table (migration 0121)")
    for col, migration in (
        ("run_id", "0122"),
        ("available_at", "0122"),
        ("priority", "0122"),
        ("attempt_count", "0122"),
    ):
        if not _column_exists("social", "scrape_jobs", col):
            missing.append(f"social.scrape_jobs.{col} column (migration {migration})")

    if missing:
        details = "; ".join(missing)
        raise ValueError(
            "Social ingest queue schema is not migrated. Apply migrations 0121, 0122, 0123. "
            f"Missing: {details}"
        )


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


def _load_instagram_cookies() -> dict[str, str]:
    """
    Resolve Instagram auth cookies for season ingest.

    Resolution order:
    1) SOCIAL_INSTAGRAM_COOKIES_JSON / INSTAGRAM_COOKIES_JSON (inline JSON object)
    2) SOCIAL_INSTAGRAM_COOKIES_FILE / INSTAGRAM_COOKIES_FILE (path to JSON file)
    3) scripts/socials/instagram/instagram_cookies.json (repo-local default)
    """
    from trr_backend.socials.instagram import load_cookies_from_file

    raw_json = (
        (os.getenv("SOCIAL_INSTAGRAM_COOKIES_JSON") or "").strip()
        or (os.getenv("INSTAGRAM_COOKIES_JSON") or "").strip()
    )
    if raw_json:
        try:
            parsed = json.loads(raw_json)
        except json.JSONDecodeError:
            logger.warning("Invalid Instagram cookies JSON from env; falling back to file-based cookies")
        else:
            if isinstance(parsed, dict):
                cookies = {str(k): str(v) for k, v in parsed.items() if v is not None and not str(k).startswith("_")}
                if cookies:
                    return cookies
            logger.warning("Instagram cookies JSON env value is not an object; falling back to file-based cookies")

    file_candidates: list[str] = []
    file_candidates.extend(
        [
            (os.getenv("SOCIAL_INSTAGRAM_COOKIES_FILE") or "").strip(),
            (os.getenv("INSTAGRAM_COOKIES_FILE") or "").strip(),
        ]
    )
    default_path = Path(__file__).resolve().parents[2] / "scripts" / "socials" / "instagram" / "instagram_cookies.json"
    file_candidates.append(str(default_path))

    for raw_path in file_candidates:
        if not raw_path:
            continue
        path = Path(raw_path).expanduser()
        if not path.is_file():
            continue
        try:
            cookies = load_cookies_from_file(str(path))
        except Exception as exc:
            logger.warning("Failed to load Instagram cookies from %s: %s", path, exc)
            continue
        if cookies:
            return cookies

    return {}


def _load_twitter_auth() -> tuple[dict[str, str], str | None]:
    """
    Resolve Twitter auth for season ingest.

    Returns (cookies_dict, bearer_token | None).
    Resolution:
    1) SOCIAL_TWITTER_COOKIES_JSON / TWITTER_COOKIES_JSON (inline JSON)
    2) SOCIAL_TWITTER_COOKIES_FILE / TWITTER_COOKIES_FILE (path to JSON file)
    3) SOCIAL_TWITTER_BEARER_TOKEN / TWITTER_BEARER_TOKEN (bearer token)
    """
    cookies: dict[str, str] = {}
    raw_json = (
        (os.getenv("SOCIAL_TWITTER_COOKIES_JSON") or "").strip()
        or (os.getenv("TWITTER_COOKIES_JSON") or "").strip()
    )
    if raw_json:
        try:
            parsed = json.loads(raw_json)
            if isinstance(parsed, dict):
                cookies = {str(k): str(v) for k, v in parsed.items() if v is not None and not str(k).startswith("_")}
        except json.JSONDecodeError:
            logger.warning("Invalid Twitter cookies JSON from env")

    if not cookies:
        file_path = (
            (os.getenv("SOCIAL_TWITTER_COOKIES_FILE") or "").strip()
            or (os.getenv("TWITTER_COOKIES_FILE") or "").strip()
        )
        if file_path:
            p = Path(file_path).expanduser()
            if p.is_file():
                try:
                    with open(p) as f:
                        parsed = json.load(f)
                    if isinstance(parsed, dict):
                        cookies = {str(k): str(v) for k, v in parsed.items() if v is not None}
                except Exception as exc:
                    logger.warning("Failed to load Twitter cookies from %s: %s", p, exc)

    bearer_token = (
        (os.getenv("SOCIAL_TWITTER_BEARER_TOKEN") or "").strip()
        or (os.getenv("TWITTER_BEARER_TOKEN") or "").strip()
        or None
    )

    return cookies, bearer_token


def _load_twikit_credentials() -> dict[str, str] | None:
    """
    Load twikit auth from env vars or cookie file.

    Resolution order (first match wins):
    1) TWIKIT_COOKIES_FILE – JSON file with auth_token + ct0 keys
    2) TWIKIT_AUTH_TOKEN + TWIKIT_CT0 – inline cookie values
    3) TWIKIT_USERNAME + TWIKIT_PASSWORD (+ TWIKIT_EMAIL) – login creds
    """
    # 1) Cookie file
    cookie_file = (os.getenv("TWIKIT_COOKIES_FILE") or "").strip()
    if cookie_file:
        p = Path(cookie_file).expanduser()
        if p.is_file():
            try:
                with open(p) as f:
                    parsed = json.load(f)
                if isinstance(parsed, dict) and parsed.get("auth_token") and parsed.get("ct0"):
                    return {"auth_token": str(parsed["auth_token"]), "ct0": str(parsed["ct0"])}
            except Exception as exc:
                logger.warning("Failed to load twikit cookies from %s: %s", p, exc)

    # 2) Inline cookie env vars
    auth_token = (os.getenv("TWIKIT_AUTH_TOKEN") or "").strip()
    ct0 = (os.getenv("TWIKIT_CT0") or "").strip()
    if auth_token and ct0:
        return {"auth_token": auth_token, "ct0": ct0}

    # 3) Login credentials
    username = (os.getenv("TWIKIT_USERNAME") or "").strip()
    password = (os.getenv("TWIKIT_PASSWORD") or "").strip()
    email = (os.getenv("TWIKIT_EMAIL") or "").strip()
    if username and password:
        return {"username": username, "email": email or username, "password": password}

    return None


def _load_tiktok_cookies() -> dict[str, str]:
    """
    Resolve TikTok cookies for season ingest.

    Resolution:
    1) SOCIAL_TIKTOK_COOKIES_JSON / TIKTOK_COOKIES_JSON (inline JSON)
    2) SOCIAL_TIKTOK_COOKIES_FILE / TIKTOK_COOKIES_FILE (path to JSON file)
    """
    raw_json = (
        (os.getenv("SOCIAL_TIKTOK_COOKIES_JSON") or "").strip()
        or (os.getenv("TIKTOK_COOKIES_JSON") or "").strip()
    )
    if raw_json:
        try:
            parsed = json.loads(raw_json)
            if isinstance(parsed, dict):
                cookies = {str(k): str(v) for k, v in parsed.items() if v is not None and not str(k).startswith("_")}
                if cookies:
                    return cookies
        except json.JSONDecodeError:
            logger.warning("Invalid TikTok cookies JSON from env")

    file_path = (
        (os.getenv("SOCIAL_TIKTOK_COOKIES_FILE") or "").strip()
        or (os.getenv("TIKTOK_COOKIES_FILE") or "").strip()
    )
    if file_path:
        p = Path(file_path).expanduser()
        if p.is_file():
            try:
                with open(p) as f:
                    parsed = json.load(f)
                if isinstance(parsed, dict):
                    return {str(k): str(v) for k, v in parsed.items() if v is not None}
            except Exception as exc:
                logger.warning("Failed to load TikTok cookies from %s: %s", p, exc)

    return {}


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
    run_id: str | None,
    platform: str,
    source_scope: str,
    job_type: str,
    stage: str,
    config: dict[str, Any],
    initiated_by: str | None,
    status: str,
    priority: int = 100,
) -> str:
    row = pg.fetch_one(
        """
        insert into social.scrape_jobs (
          run_id,
          platform,
          job_type,
          config,
          status,
          available_at,
          priority,
          show_id,
          season_id,
          source_scope,
          initiated_by,
          metadata
        )
        values (
          %s,
          %s,
          %s,
          %s::jsonb,
          %s,
          now(),
          %s,
          %s,
          %s,
          %s,
          %s,
          %s::jsonb
        )
        returning id::text
        """,
        [
            run_id,
            platform,
            job_type,
            json.dumps(config),
            status,
            priority,
            context.show_id,
            context.season_id,
            source_scope,
            initiated_by,
            json.dumps({"stage": stage}),
        ],
    )
    if not row:
        raise RuntimeError("Failed to create scrape job")
    return str(row["id"])


def _set_job_running(job_id: str, *, worker_id: str | None = None) -> None:
    pg.fetch_one(
        """
        update social.scrape_jobs
        set
          status = 'running',
          started_at = coalesce(started_at, now()),
          claimed_at = now(),
          heartbeat_at = now(),
          worker_id = coalesce(%s, worker_id),
          attempt_count = attempt_count + 1
        where id = %s
        returning id::text
        """,
        [worker_id, job_id],
    )


def _touch_job_heartbeat(job_id: str, *, worker_id: str | None = None) -> None:
    pg.fetch_one(
        """
        update social.scrape_jobs
        set
          heartbeat_at = now(),
          worker_id = coalesce(%s, worker_id)
        where id = %s
        returning id::text
        """,
        [worker_id, job_id],
    )


def _update_job_progress(
    job_id: str,
    *,
    items_found: int,
    metadata: dict[str, Any] | None = None,
) -> None:
    pg.fetch_one(
        """
        update social.scrape_jobs
        set
          items_found = %s,
          metadata = coalesce(metadata, '{}'::jsonb) || %s::jsonb,
          heartbeat_at = now()
        where id = %s
          and status in ('queued', 'pending', 'retrying', 'running')
        returning id::text
        """,
        [items_found, json.dumps(metadata or {}), job_id],
    )


def _finish_job(
    job_id: str,
    *,
    status: str,
    items_found: int,
    error_message: str | None = None,
    metadata: dict[str, Any] | None = None,
    last_error_code: str | None = None,
    last_error_class: str | None = None,
    next_available_at: datetime | None = None,
) -> None:
    is_terminal = status in {"completed", "failed", "cancelled"}
    completed_expr = "now()" if is_terminal else "completed_at"
    pg.fetch_one(
        """
        update social.scrape_jobs
        set
          status = %s,
          items_found = %s,
          error_message = %s,
          completed_at = """
        + completed_expr
        + """,
          metadata = coalesce(%s::jsonb, '{}'::jsonb),
          heartbeat_at = now(),
          last_error_code = %s,
          last_error_class = %s,
          available_at = coalesce(%s, available_at)
        where id = %s
        returning id::text
        """,
        [
            status,
            items_found,
            error_message,
            json.dumps(metadata or {}),
            last_error_code,
            last_error_class,
            next_available_at,
            job_id,
        ],
    )


def _create_run(
    context: SeasonContext,
    *,
    source_scope: str,
    initiated_by: str | None,
    config: dict[str, Any],
    status: str,
) -> str:
    row = pg.fetch_one(
        """
        insert into social.scrape_runs (
          season_id,
          show_id,
          source_scope,
          status,
          initiated_by,
          config,
          started_at
        )
        values (
          %s,
          %s,
          %s,
          %s,
          %s,
          %s::jsonb,
          case when %s = 'running' then now() else null end
        )
        returning id::text
        """,
        [
            context.season_id,
            context.show_id,
            source_scope,
            status,
            initiated_by,
            json.dumps(config),
            status,
        ],
    )
    if not row:
        raise RuntimeError("Failed to create social scrape run")
    return str(row["id"])


def _set_run_status(run_id: str, status: str) -> None:
    pg.fetch_one(
        """
        update social.scrape_runs
        set
          status = %s,
          started_at = case
            when %s = 'running' then coalesce(started_at, now())
            else started_at
          end,
          completed_at = case
            when %s in ('completed', 'failed', 'cancelled') then coalesce(completed_at, now())
            else completed_at
          end,
          cancelled_at = case
            when %s = 'cancelled' then coalesce(cancelled_at, now())
            else cancelled_at
          end
        where id = %s
        returning id::text
        """,
        [status, status, status, status, run_id],
    )


def _update_run_summary(run_id: str) -> dict[str, Any]:
    summary_row = pg.fetch_one(
        """
        with stats as (
          select
            count(*)::int as total_jobs,
            count(*) filter (where status = 'completed')::int as completed_jobs,
            count(*) filter (where status = 'failed')::int as failed_jobs,
            count(*) filter (where status in ('queued', 'pending', 'retrying', 'running'))::int as active_jobs,
            coalesce(sum(items_found), 0)::int as items_found_total
          from social.scrape_jobs
          where run_id = %s
        ),
        stage_stats as (
          select
            coalesce(config->>'stage', 'unknown') as stage,
            count(*)::int as total,
            count(*) filter (where status = 'completed')::int as completed,
            count(*) filter (where status = 'failed')::int as failed,
            count(*) filter (where status in ('queued', 'pending', 'retrying', 'running'))::int as active
          from social.scrape_jobs
          where run_id = %s
          group by coalesce(config->>'stage', 'unknown')
        )
        select
          (select row_to_json(stats) from stats) as stats,
          coalesce((select jsonb_object_agg(stage, jsonb_build_object(
            'total', total,
            'completed', completed,
            'failed', failed,
            'active', active
          )) from stage_stats), '{}'::jsonb) as stage_counts
        """,
        [run_id, run_id],
    ) or {}

    stats = summary_row.get("stats") or {}
    stage_counts = summary_row.get("stage_counts") or {}
    summary = {
        "total_jobs": int(stats.get("total_jobs") or 0),
        "completed_jobs": int(stats.get("completed_jobs") or 0),
        "failed_jobs": int(stats.get("failed_jobs") or 0),
        "active_jobs": int(stats.get("active_jobs") or 0),
        "items_found_total": int(stats.get("items_found_total") or 0),
        "stage_counts": stage_counts,
    }
    pg.fetch_one(
        """
        update social.scrape_runs
        set summary = %s::jsonb
        where id = %s
        returning id::text
        """,
        [json.dumps(summary), run_id],
    )
    return summary


def _finalize_run_status(run_id: str) -> dict[str, Any]:
    summary = _update_run_summary(run_id)
    active_jobs = int(summary.get("active_jobs") or 0)
    failed_jobs = int(summary.get("failed_jobs") or 0)
    current = pg.fetch_one("select status from social.scrape_runs where id = %s", [run_id]) or {}
    if str(current.get("status")) == "cancelled":
        return summary
    if active_jobs > 0:
        _set_run_status(run_id, "running")
    elif failed_jobs > 0:
        _set_run_status(run_id, "failed")
    else:
        _set_run_status(run_id, "completed")
    return summary


def _parse_instagram_time(ts: Any) -> datetime | None:
    if isinstance(ts, int):
        return datetime.fromtimestamp(ts, tz=UTC)
    if isinstance(ts, float):
        return datetime.fromtimestamp(int(ts), tz=UTC)
    return _coerce_dt(ts)


def _parse_tiktok_time(ts: Any) -> datetime | None:
    return _parse_instagram_time(ts)


def _pg_upsert(
    table: str,
    payload: dict[str, Any],
    *,
    conflict_col: str,
    conn: Any | None = None,
) -> dict[str, Any] | None:
    """Upsert a row into social.{table} using direct SQL (psycopg2).

    This avoids Supabase PostgREST schema-cache (PGRST002) errors that
    occur intermittently with the ``social`` schema.
    """
    from psycopg2.extras import Json as PgJson

    adapted: dict[str, Any] = {}
    for key, value in payload.items():
        if isinstance(value, (dict, list)):
            adapted[key] = PgJson(value)
        else:
            adapted[key] = value

    cols = list(adapted.keys())
    col_list = ", ".join(cols)
    placeholders = ", ".join(["%s"] * len(cols))
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c != conflict_col)

    sql = f"""
        INSERT INTO social.{table} ({col_list})
        VALUES ({placeholders})
        ON CONFLICT ({conflict_col}) DO UPDATE SET {updates}
        RETURNING *
    """
    with pg.db_cursor(conn=conn) as cur:
        return pg.fetch_one_with_cursor(cur, sql, list(adapted.values()))


def _upsert_instagram_post(
    context: SeasonContext,
    *,
    job_id: str,
    account: str,
    post: Any,
    conn: Any | None = None,
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
    return _pg_upsert("instagram_posts", payload, conflict_col="shortcode", conn=conn)


def _upsert_instagram_comment_tree(
    context: SeasonContext,
    *,
    job_id: str,
    account: str,
    post_id: str,
    comment: Any,
    parent_comment_db_id: str | None = None,
    conn: Any | None = None,
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
    row = _pg_upsert("instagram_comments", payload, conflict_col="comment_id", conn=conn)
    comment_db_id = (row or {}).get("id")

    total = 1
    for reply in getattr(comment, "replies", []) or []:
        total += _upsert_instagram_comment_tree(
            context,
            job_id=job_id,
            account=account,
            post_id=post_id,
            comment=reply,
            parent_comment_db_id=comment_db_id,
            conn=conn,
        )
    return total


def _ingest_instagram(
    context: SeasonContext,
    *,
    account: str,
    hashtags: list[str],
    keywords: list[str],
    opts: IngestOptions,
    job_id: str,
    stage: str = "posts",
) -> tuple[int, int, dict[str, Any]]:
    from trr_backend.socials.instagram import InstagramScraper, ScrapeConfig

    try:
        post_delay_seconds = float((os.getenv("SOCIAL_INSTAGRAM_DELAY_SEC") or "").strip() or "0.15")
    except ValueError:
        post_delay_seconds = 0.15
    try:
        comment_delay_seconds = float((os.getenv("SOCIAL_INSTAGRAM_COMMENT_DELAY_SEC") or "").strip() or "0.25")
    except ValueError:
        comment_delay_seconds = 0.25

    cookies = _load_instagram_cookies()
    if not cookies.get("sessionid"):
        logger.warning("Instagram ingest running without sessionid cookie; results may be limited to ~12 recent posts")
    scraper = InstagramScraper(cookies=cookies)
    config = ScrapeConfig(
        username=account,
        hashtags=[],
        date_start=opts.date_start,
        date_end=opts.date_end,
        delay_seconds=post_delay_seconds,
        max_pages=None,
        show_id=context.show_id,
        season_number=context.season_number,
    )

    logger.info("[instagram] Scraping account=%s date_range=%s..%s", account, opts.date_start, opts.date_end)
    posts = scraper.scrape(config)
    retrieval_meta = dict(getattr(scraper, "last_retrieval_meta", {}) or {})
    post_count = 0
    comment_count = 0
    skipped_keyword = 0
    total_scraped = 0
    last_progress_total = 0

    def _report_progress(*, force: bool = False) -> None:
        nonlocal last_progress_total
        total_items = post_count + comment_count
        if not force:
            if total_items == 0:
                return
            if (total_items - last_progress_total) < JOB_PROGRESS_UPDATE_EVERY:
                return
        last_progress_total = total_items
        _update_job_progress(
            job_id,
            items_found=total_items,
            metadata={
                "stage": stage,
                "platform": "instagram",
                "account": account,
                "stage_counters": {"posts": post_count, "comments": comment_count},
            },
        )

    _update_job_progress(
        job_id,
        items_found=0,
        metadata={
            "stage": stage,
            "platform": "instagram",
            "account": account,
            "stage_counters": {"posts": 0, "comments": 0},
        },
    )

    with pg.db_connection() as conn:
        for post in posts:
            total_scraped += 1
            caption = str(getattr(post, "caption", "") or "")
            if not _text_contains_any_term(text=caption, hashtags=hashtags, keywords=keywords):
                skipped_keyword += 1
                continue
            if post_count >= opts.max_posts_per_target:
                break

            upserted = _upsert_instagram_post(context, job_id=job_id, account=account, post=post, conn=conn)
            if not upserted:
                continue
            post_count += 1
            _report_progress()

            if opts.max_comments_per_post > 0:
                comments = scraper.fetch_comments(
                    getattr(post, "shortcode", ""),
                    max_comments=opts.max_comments_per_post,
                    fetch_replies=opts.fetch_replies,
                    delay=comment_delay_seconds,
                )
                for comment in comments:
                    comment_count += _upsert_instagram_comment_tree(
                        context,
                        job_id=job_id,
                        account=account,
                        post_id=str(upserted["id"]),
                        comment=comment,
                        conn=conn,
                    )
                    _report_progress()

    _report_progress(force=True)

    logger.info(
        "[instagram] Done: scraped=%d matched=%d skipped_keyword=%d comments=%d",
        total_scraped,
        post_count,
        skipped_keyword,
        comment_count,
    )
    return post_count, comment_count, retrieval_meta


def _upsert_tiktok_post(
    context: SeasonContext, *, job_id: str, account: str, post: Any, conn: Any | None = None
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
    return _pg_upsert("tiktok_posts", payload, conflict_col="video_id", conn=conn)


def _upsert_tiktok_comment_tree(
    context: SeasonContext,
    *,
    job_id: str,
    account: str,
    post_id: str,
    comment: Any,
    parent_comment_db_id: str | None = None,
    conn: Any | None = None,
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
    row = _pg_upsert("tiktok_comments", payload, conflict_col="comment_id", conn=conn)
    comment_db_id = (row or {}).get("id")

    total = 1
    for reply in getattr(comment, "replies", []) or []:
        total += _upsert_tiktok_comment_tree(
            context,
            job_id=job_id,
            account=account,
            post_id=post_id,
            comment=reply,
            parent_comment_db_id=comment_db_id,
            conn=conn,
        )
    return total


def _ingest_tiktok(
    context: SeasonContext,
    *,
    account: str,
    hashtags: list[str],
    keywords: list[str],
    opts: IngestOptions,
    job_id: str,
    stage: str = "posts",
) -> tuple[int, int, dict[str, Any]]:
    from trr_backend.socials.tiktok import TikTokScrapeConfig, TikTokScraper

    tiktok_cookies = _load_tiktok_cookies()
    scraper = TikTokScraper(cookies=tiktok_cookies)
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

    logger.info("[tiktok] Scraping account=%s date_range=%s..%s", account, opts.date_start, opts.date_end)
    posts = scraper.scrape(config)
    retrieval_meta = dict(getattr(scraper, "last_retrieval_meta", {}) or {})
    post_count = 0
    comment_count = 0
    skipped_keyword = 0
    total_scraped = 0
    last_progress_total = 0

    def _report_progress(*, force: bool = False) -> None:
        nonlocal last_progress_total
        total_items = post_count + comment_count
        if not force:
            if total_items == 0:
                return
            if (total_items - last_progress_total) < JOB_PROGRESS_UPDATE_EVERY:
                return
        last_progress_total = total_items
        _update_job_progress(
            job_id,
            items_found=total_items,
            metadata={
                "stage": stage,
                "platform": "tiktok",
                "account": account,
                "stage_counters": {"posts": post_count, "comments": comment_count},
            },
        )

    _update_job_progress(
        job_id,
        items_found=0,
        metadata={
            "stage": stage,
            "platform": "tiktok",
            "account": account,
            "stage_counters": {"posts": 0, "comments": 0},
        },
    )

    with pg.db_connection() as conn:
        for post in posts:
            total_scraped += 1
            description = str(getattr(post, "description", "") or "")
            if not _text_contains_any_term(text=description, hashtags=hashtags, keywords=keywords):
                skipped_keyword += 1
                continue
            if post_count >= opts.max_posts_per_target:
                break

            upserted = _upsert_tiktok_post(context, job_id=job_id, account=account, post=post, conn=conn)
            if not upserted:
                continue
            post_count += 1
            _report_progress()

            if opts.max_comments_per_post > 0:
                comments = scraper.fetch_comments(
                    getattr(post, "video_id", ""),
                    username=account,
                    max_comments=opts.max_comments_per_post,
                    fetch_replies=opts.fetch_replies,
                    delay=0.5,
                )
                for comment in comments:
                    comment_count += _upsert_tiktok_comment_tree(
                        context,
                        job_id=job_id,
                        account=account,
                        post_id=str(upserted["id"]),
                        comment=comment,
                        conn=conn,
                    )
                    _report_progress()

    _report_progress(force=True)

    logger.info(
        "[tiktok] Done: scraped=%d matched=%d skipped_keyword=%d comments=%d",
        total_scraped,
        post_count,
        skipped_keyword,
        comment_count,
    )
    return post_count, comment_count, retrieval_meta


def _upsert_youtube_video(
    context: SeasonContext,
    *,
    job_id: str,
    account: str,
    video: Any,
    conn: Any | None = None,
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
    return _pg_upsert("youtube_videos", payload, conflict_col="video_id", conn=conn)


def _upsert_youtube_comment_tree(
    context: SeasonContext,
    *,
    job_id: str,
    account: str,
    video_db_id: str,
    comment: Any,
    parent_comment_db_id: str | None = None,
    conn: Any | None = None,
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
    row = _pg_upsert("youtube_comments", payload, conflict_col="comment_id", conn=conn)
    comment_db_id = (row or {}).get("id")

    total = 1
    for reply in getattr(comment, "replies", []) or []:
        total += _upsert_youtube_comment_tree(
            context,
            job_id=job_id,
            account=account,
            video_db_id=video_db_id,
            comment=reply,
            parent_comment_db_id=comment_db_id,
            conn=conn,
        )
    return total


def _ingest_youtube(
    context: SeasonContext,
    *,
    account: str,
    hashtags: list[str],
    keywords: list[str],
    opts: IngestOptions,
    job_id: str,
    stage: str = "posts",
) -> tuple[int, int, dict[str, Any]]:
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

    logger.info(
        "[youtube] Scraping channel=%s keywords=%s date_range=%s..%s",
        account,
        keywords,
        opts.date_start,
        opts.date_end,
    )
    videos = scraper.scrape(config)
    retrieval_meta = dict(getattr(scraper, "last_retrieval_meta", {}) or {})
    video_count = 0
    comment_count = 0
    skipped_keyword = 0
    total_scraped = 0
    last_progress_total = 0

    def _report_progress(*, force: bool = False) -> None:
        nonlocal last_progress_total
        total_items = video_count + comment_count
        if not force:
            if total_items == 0:
                return
            if (total_items - last_progress_total) < JOB_PROGRESS_UPDATE_EVERY:
                return
        last_progress_total = total_items
        _update_job_progress(
            job_id,
            items_found=total_items,
            metadata={
                "stage": stage,
                "platform": "youtube",
                "account": account,
                "stage_counters": {"posts": video_count, "comments": comment_count},
            },
        )

    _update_job_progress(
        job_id,
        items_found=0,
        metadata={
            "stage": stage,
            "platform": "youtube",
            "account": account,
            "stage_counters": {"posts": 0, "comments": 0},
        },
    )

    with pg.db_connection() as conn:
        for video in videos:
            total_scraped += 1
            combined_text = f"{getattr(video, 'title', '')} {getattr(video, 'description', '')}"
            if not _text_contains_any_term(text=combined_text, hashtags=hashtags, keywords=keywords):
                skipped_keyword += 1
                continue
            if video_count >= opts.max_posts_per_target:
                break

            upserted = _upsert_youtube_video(context, job_id=job_id, account=account, video=video, conn=conn)
            if not upserted:
                continue
            video_count += 1
            _report_progress()

            if opts.max_comments_per_post > 0:
                comments = scraper.fetch_comments(
                    getattr(video, "video_id", ""),
                    max_comments=opts.max_comments_per_post,
                    fetch_replies=opts.fetch_replies,
                    delay=0.5,
                )
                for comment in comments:
                    comment_count += _upsert_youtube_comment_tree(
                        context,
                        job_id=job_id,
                        account=account,
                        video_db_id=str(upserted["id"]),
                        comment=comment,
                        conn=conn,
                    )
                    _report_progress()

    _report_progress(force=True)

    logger.info(
        "[youtube] Done: scraped=%d matched=%d skipped_keyword=%d comments=%d",
        total_scraped,
        video_count,
        skipped_keyword,
        comment_count,
    )
    return video_count, comment_count, retrieval_meta


def _upsert_tweet(
    context: SeasonContext,
    *,
    job_id: str,
    account: str,
    tweet: Any,
    conn: Any | None = None,
) -> dict[str, Any] | None:
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
    return _pg_upsert("twitter_tweets", payload, conflict_col="tweet_id", conn=conn)


def _ingest_twitter(
    context: SeasonContext,
    *,
    account: str,
    hashtags: list[str],
    keywords: list[str],
    opts: IngestOptions,
    job_id: str,
    include_reply_records: bool = True,
    hydrate_audience_replies: bool = False,
    stage: str = "posts",
) -> tuple[int, int, dict[str, Any]]:
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
    logger.info("[twitter] Searching query=%r date_range=%s..%s", config.query, date_start, date_end)
    twitter_cookies, twitter_bearer = _load_twitter_auth()
    twikit_creds = _load_twikit_credentials()
    scraper = TwitterScraper(cookies=twitter_cookies, bearer_token=twitter_bearer, twikit_credentials=twikit_creds)
    tweets = scraper.scrape(config)[: opts.max_posts_per_target]
    retrieval_meta = dict(getattr(scraper, "last_retrieval_meta", {}) or {})
    logger.info("[twitter] Scraper returned %d tweets", len(tweets))

    post_count = 0
    reply_count = 0
    skipped_keyword = 0
    anchor_posts: list[Any] = []
    hydrated_replies = 0
    last_progress_total = 0

    def _report_progress(*, force: bool = False) -> None:
        nonlocal last_progress_total
        total_items = post_count + reply_count
        if not force:
            if total_items == 0:
                return
            if (total_items - last_progress_total) < JOB_PROGRESS_UPDATE_EVERY:
                return
        last_progress_total = total_items
        _update_job_progress(
            job_id,
            items_found=total_items,
            metadata={
                "stage": stage,
                "platform": "twitter",
                "account": account,
                "stage_counters": {"posts": post_count, "comments": reply_count},
            },
        )

    _update_job_progress(
        job_id,
        items_found=0,
        metadata={
            "stage": stage,
            "platform": "twitter",
            "account": account,
            "stage_counters": {"posts": 0, "comments": 0},
        },
    )

    with pg.db_connection() as conn:
        for tweet in tweets:
            text = str(getattr(tweet, "text", "") or "")
            if not _text_contains_any_term(text=text, hashtags=hashtag_list, keywords=keyword_list):
                skipped_keyword += 1
                continue
            is_reply = bool(getattr(tweet, "is_reply", False))
            if not is_reply:
                anchor_posts.append(tweet)
            if is_reply and not include_reply_records:
                continue
            upserted = _upsert_tweet(context, job_id=job_id, account=account, tweet=tweet, conn=conn)
            if not upserted:
                continue
            if is_reply:
                reply_count += 1
            else:
                post_count += 1
            _report_progress()

        if hydrate_audience_replies and opts.max_comments_per_post > 0:
            per_post_limit = max(0, opts.max_replies_per_post or opts.max_comments_per_post)
            if per_post_limit > 0:
                for tweet in anchor_posts[: opts.max_posts_per_target]:
                    tweet_id = str(getattr(tweet, "tweet_id", "") or "")
                    if not tweet_id:
                        continue
                    replies = scraper.fetch_tweet_replies(tweet_id, delay=0.5)[:per_post_limit]
                    for reply in replies:
                        # Normalize reply linkage for downstream analytics.
                        if not getattr(reply, "reply_to_tweet_id", None):
                            reply.reply_to_tweet_id = tweet_id
                        reply.is_reply = True
                        if _upsert_tweet(context, job_id=job_id, account=account, tweet=reply, conn=conn):
                            hydrated_replies += 1
                            _report_progress()
            reply_count += hydrated_replies

    _report_progress(force=True)

    logger.info(
        "[twitter] Done: total=%d posts=%d replies=%d hydrated_replies=%d skipped_keyword=%d",
        len(tweets),
        post_count,
        reply_count,
        hydrated_replies,
        skipped_keyword,
    )
    retrieval_meta["hydrated_replies"] = hydrated_replies
    return post_count, reply_count, retrieval_meta


def _retry_backoff_seconds(attempt_count: int) -> int:
    return max(5, min(300, 5 * (2 ** max(0, attempt_count - 1))))


def _classify_job_error(exc: Exception) -> tuple[str, str, bool]:
    message = str(exc).lower()
    error_class = exc.__class__.__name__
    transient_markers = ("timeout", "temporar", "connection", "network", "429", "502", "503", "504", "rate limit")
    if any(marker in message for marker in transient_markers):
        return "transient_error", error_class, True
    return "fatal_error", error_class, False


def _run_platform_stage(
    *,
    context: SeasonContext,
    platform: str,
    stage: str,
    account: str,
    hashtags: list[str],
    keywords: list[str],
    opts: IngestOptions,
    job_id: str,
) -> tuple[int, int, dict[str, Any]]:
    if stage not in {"posts", "comments"}:
        raise ValueError(f"Unsupported ingest stage: {stage}")

    if stage == "posts":
        stage_opts = replace(opts, max_comments_per_post=0, fetch_replies=False)
        if platform == "instagram":
            return _ingest_instagram(
                context,
                account=account,
                hashtags=hashtags,
                keywords=keywords,
                opts=stage_opts,
                job_id=job_id,
            )
        if platform == "tiktok":
            return _ingest_tiktok(
                context,
                account=account,
                hashtags=hashtags,
                keywords=keywords,
                opts=stage_opts,
                job_id=job_id,
            )
        if platform == "youtube":
            return _ingest_youtube(
                context,
                account=account,
                hashtags=hashtags,
                keywords=keywords,
                opts=stage_opts,
                job_id=job_id,
            )
        if platform == "twitter":
            return _ingest_twitter(
                context,
                account=account,
                hashtags=hashtags,
                keywords=keywords,
                opts=stage_opts,
                job_id=job_id,
                include_reply_records=False,
                hydrate_audience_replies=False,
            )
    else:
        if opts.max_comments_per_post <= 0:
            return 0, 0, {}
        if platform == "instagram":
            _, comments, meta = _ingest_instagram(
                context, account=account, hashtags=hashtags, keywords=keywords, opts=opts, job_id=job_id
            )
            return 0, comments, meta
        if platform == "tiktok":
            _, comments, meta = _ingest_tiktok(
                context, account=account, hashtags=hashtags, keywords=keywords, opts=opts, job_id=job_id
            )
            return 0, comments, meta
        if platform == "youtube":
            _, comments, meta = _ingest_youtube(
                context, account=account, hashtags=hashtags, keywords=keywords, opts=opts, job_id=job_id
            )
            return 0, comments, meta
        if platform == "twitter":
            _, comments, meta = _ingest_twitter(
                context,
                account=account,
                hashtags=hashtags,
                keywords=keywords,
                opts=opts,
                job_id=job_id,
                include_reply_records=False,
                hydrate_audience_replies=True,
            )
            return 0, comments, meta

    raise RuntimeError(f"Platform {platform} ingest is not supported")


def _claim_next_job(*, worker_id: str | None = None, run_id: str | None = None) -> dict[str, Any] | None:
    return pg.fetch_one(
        """
        with candidate as (
          select id
          from social.scrape_jobs
          where status in ('queued', 'pending', 'retrying')
            and available_at <= now()
            and (%s::uuid is null or run_id = %s::uuid)
          order by priority asc, created_at asc
          for update skip locked
          limit 1
        )
        update social.scrape_jobs as j
        set
          status = 'running',
          started_at = coalesce(j.started_at, now()),
          claimed_at = now(),
          heartbeat_at = now(),
          worker_id = coalesce(%s, j.worker_id),
          attempt_count = j.attempt_count + 1
        from candidate
        where j.id = candidate.id
        returning
          j.id::text,
          j.run_id::text as run_id,
          j.platform,
          j.job_type,
          j.status,
          j.config,
          j.metadata,
          j.attempt_count,
          j.max_attempts,
          j.source_scope,
          j.season_id::text as season_id
        """,
        [run_id, run_id, worker_id],
    )


def _execute_claimed_job(job: dict[str, Any], *, worker_id: str | None = None) -> dict[str, Any]:
    job_id = str(job.get("id") or "")
    run_id = str(job.get("run_id") or "")
    platform = str(job.get("platform") or "")
    config = dict(job.get("config") or {})
    stage = str(config.get("stage") or ((job.get("metadata") or {}).get("stage")) or "posts")
    context = get_season_context(str(config.get("season_id") or job.get("season_id") or ""))
    opts = IngestOptions(
        platforms=None,
        source_scope=str(config.get("source_scope") or job.get("source_scope") or "bravo"),
        max_posts_per_target=max(1, int(config.get("max_posts_per_target") or 1000)),
        max_comments_per_post=max(0, int(config.get("max_comments_per_post") or 0)),
        max_replies_per_post=max(0, int(config.get("max_replies_per_post") or 0)),
        fetch_replies=bool(config.get("fetch_replies", True)),
        ingest_mode=str(config.get("ingest_mode") or "posts_and_comments"),
        depth_preset=str(config.get("depth_preset") or "balanced"),
        date_start=_coerce_dt(config.get("date_start")),
        date_end=_coerce_dt(config.get("date_end")),
    )
    account = str(config.get("account") or "")
    hashtags = [str(item).strip().lstrip("#") for item in (config.get("hashtags") or []) if str(item).strip()]
    keywords = [str(item).strip() for item in (config.get("keywords") or []) if str(item).strip()]

    try:
        _touch_job_heartbeat(job_id, worker_id=worker_id)
        posts_count, comments_count, retrieval_meta = _run_platform_stage(
            context=context,
            platform=platform,
            stage=stage,
            account=account,
            hashtags=hashtags,
            keywords=keywords,
            opts=opts,
            job_id=job_id,
        )
        _finish_job(
            job_id,
            status="completed",
            items_found=posts_count + comments_count,
            metadata={
                "stage": stage,
                "stage_counters": {"posts": posts_count, "comments": comments_count},
                "platform": platform,
                "account": account,
                "retrieval_meta": retrieval_meta,
            },
        )
    except Exception as exc:  # noqa: BLE001
        error_code, error_class, transient = _classify_job_error(exc)
        attempt_count = int(job.get("attempt_count") or 1)
        max_attempts = int(job.get("max_attempts") or 1)
        can_retry = transient and attempt_count < max_attempts
        next_available_at = _now_utc() + timedelta(seconds=_retry_backoff_seconds(attempt_count)) if can_retry else None
        _finish_job(
            job_id,
            status="retrying" if can_retry else "failed",
            items_found=0,
            error_message=str(exc),
            metadata={
                "stage": stage,
                "platform": platform,
                "account": account,
                "error": str(exc),
            },
            last_error_code=error_code,
            last_error_class=error_class,
            next_available_at=next_available_at,
        )
        logger.exception("Social ingest job failed: job_id=%s stage=%s platform=%s", job_id, stage, platform)
    finally:
        if run_id:
            _finalize_run_status(run_id)

    return pg.fetch_one(
        """
        select
          id::text,
          run_id::text as run_id,
          platform,
          job_type,
          status,
          items_found,
          error_message,
          metadata
        from social.scrape_jobs
        where id = %s
        """,
        [job_id],
    ) or {}


def execute_run(run_id: str, *, worker_id: str | None = None) -> dict[str, Any]:
    _set_run_status(run_id, "running")
    while True:
        job = _claim_next_job(worker_id=worker_id, run_id=run_id)
        if not job:
            break
        run_state = pg.fetch_one("select status from social.scrape_runs where id = %s", [run_id]) or {}
        if str(run_state.get("status")) == "cancelled":
            _finish_job(str(job.get("id")), status="cancelled", items_found=0, metadata={"stage": "cancelled"})
            continue
        _execute_claimed_job(job, worker_id=worker_id)

    _finalize_run_status(run_id)
    return pg.fetch_one(
        """
        select
          id::text,
          season_id::text as season_id,
          show_id::text as show_id,
          source_scope,
          status,
          config,
          summary,
          created_at,
          started_at,
          completed_at,
          cancelled_at
        from social.scrape_runs
        where id = %s
        """,
        [run_id],
    ) or {}


def process_next_queued_job(*, worker_id: str) -> dict[str, Any] | None:
    job = _claim_next_job(worker_id=worker_id, run_id=None)
    if not job:
        return None
    return _execute_claimed_job(job, worker_id=worker_id)


def ingest_season(
    season_id: str,
    *,
    platforms: list[str] | None,
    source_scope: str,
    max_posts_per_target: int,
    max_comments_per_post: int,
    max_replies_per_post: int = 100,
    fetch_replies: bool,
    ingest_mode: str = "posts_and_comments",
    depth_preset: str = "balanced",
    date_start: datetime | None,
    date_end: datetime | None,
    initiated_by: str | None,
) -> dict[str, Any]:
    _assert_social_queue_schema_ready()
    context = get_season_context(season_id)
    if source_scope not in SUPPORTED_SCOPES:
        raise ValueError(f"Unsupported source scope: {source_scope}")

    normalized_mode = (ingest_mode or "posts_and_comments").strip().lower()
    if normalized_mode not in SUPPORTED_INGEST_MODES:
        raise ValueError(f"Unsupported ingest mode: {ingest_mode}")

    normalized_depth = (depth_preset or "balanced").strip().lower()
    if normalized_depth not in SUPPORTED_DEPTH_PRESETS:
        raise ValueError(f"Unsupported depth preset: {depth_preset}")

    platform_filter = {p.strip().lower() for p in platforms or [] if isinstance(p, str) and p.strip()}
    if platform_filter:
        unsupported = platform_filter - set(SUPPORTED_PLATFORMS)
        if unsupported:
            raise ValueError(f"Unsupported platforms requested: {', '.join(sorted(unsupported))}")

    resolved_posts, resolved_comments, resolved_replies, resolved_fetch_replies = _resolve_depth_defaults(
        depth_preset=normalized_depth,
        max_posts_per_target=max_posts_per_target,
        max_comments_per_post=max_comments_per_post,
        max_replies_per_post=max_replies_per_post,
        fetch_replies=fetch_replies,
    )
    if normalized_mode == "posts_only":
        resolved_comments = 0
        resolved_replies = 0
        resolved_fetch_replies = False

    opts = IngestOptions(
        platforms=platform_filter or None,
        source_scope=source_scope,
        max_posts_per_target=resolved_posts,
        max_comments_per_post=resolved_comments,
        max_replies_per_post=resolved_replies,
        fetch_replies=resolved_fetch_replies,
        ingest_mode=normalized_mode,
        depth_preset=normalized_depth,
        date_start=date_start,
        date_end=date_end,
    )

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
            "run_id": None,
            "stages": ["posts"] if normalized_mode == "posts_only" else ["posts", "comments"],
            "queued_or_started_jobs": 0,
            "message": "No active targets configured for selected platforms",
        }

    run_config = {
        "season_id": context.season_id,
        "show_id": context.show_id,
        "source_scope": source_scope,
        "date_start": _iso(opts.date_start),
        "date_end": _iso(opts.date_end),
        "max_posts_per_target": opts.max_posts_per_target,
        "max_comments_per_post": opts.max_comments_per_post,
        "max_replies_per_post": opts.max_replies_per_post,
        "fetch_replies": opts.fetch_replies,
        "ingest_mode": opts.ingest_mode,
        "depth_preset": opts.depth_preset,
    }
    run_id = _create_run(
        context,
        source_scope=source_scope,
        initiated_by=initiated_by,
        config=run_config,
        status="queued",
    )

    queue_enabled = is_queue_enabled()
    initial_job_status = "queued" if queue_enabled else "pending"
    job_ids: list[str] = []

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
        for account in accounts:
            base_config = {
                "run_id": run_id,
                "season_id": context.season_id,
                "show_id": context.show_id,
                "platform": platform,
                "source_scope": source_scope,
                "account": account,
                "hashtags": hashtags,
                "keywords": keywords,
                "date_start": _iso(opts.date_start),
                "date_end": _iso(opts.date_end),
                "max_posts_per_target": opts.max_posts_per_target,
                "max_comments_per_post": opts.max_comments_per_post,
                "max_replies_per_post": opts.max_replies_per_post,
                "fetch_replies": opts.fetch_replies,
                "ingest_mode": opts.ingest_mode,
                "depth_preset": opts.depth_preset,
            }
            job_ids.append(
                _create_job(
                    context,
                    run_id=run_id,
                    platform=platform,
                    source_scope=source_scope,
                    job_type="posts",
                    stage="posts",
                    config={**base_config, "stage": "posts"},
                    initiated_by=initiated_by,
                    status=initial_job_status,
                    priority=100,
                )
            )
            if normalized_mode == "posts_and_comments" and opts.max_comments_per_post > 0:
                job_ids.append(
                    _create_job(
                        context,
                        run_id=run_id,
                        platform=platform,
                        source_scope=source_scope,
                        job_type="comments",
                        stage="comments",
                        config={**base_config, "stage": "comments"},
                        initiated_by=initiated_by,
                        status=initial_job_status,
                        priority=200,
                    )
                )

    summary = _update_run_summary(run_id)
    return {
        "season_id": context.season_id,
        "show_id": context.show_id,
        "season_number": context.season_number,
        "source_scope": source_scope,
        "run_id": run_id,
        "status": "queued" if queue_enabled else "pending",
        "stages": ["posts"] if normalized_mode == "posts_only" else ["posts", "comments"],
        "queued_or_started_jobs": len(job_ids),
        "summary": summary,
    }


def cancel_run(season_id: str, run_id: str, *, cancelled_by: str | None = None) -> dict[str, Any]:
    _assert_social_queue_schema_ready()
    run_row = pg.fetch_one(
        """
        select id::text, season_id::text, status
        from social.scrape_runs
        where id = %s and season_id = %s
        """,
        [run_id, season_id],
    )
    if not run_row:
        raise ValueError("Run not found")

    pg.fetch_one(
        """
        update social.scrape_runs
        set
          status = 'cancelled',
          cancelled_at = now(),
          completed_at = now(),
          summary = coalesce(summary, '{}'::jsonb) || jsonb_build_object('cancelled_by', %s)
        where id = %s
        returning id::text
        """,
        [cancelled_by, run_id],
    )
    cancelled_jobs = pg.execute_returning(
        """
        update social.scrape_jobs
        set
          status = 'cancelled',
          completed_at = now(),
          error_message = coalesce(error_message, 'Cancelled by user request')
        where run_id = %s
          and status in ('queued', 'pending', 'retrying', 'running')
        returning id::text
        """,
        [run_id],
    )
    summary = _update_run_summary(run_id)
    return {
        "run_id": run_id,
        "season_id": season_id,
        "status": "cancelled",
        "cancelled_jobs": len(cancelled_jobs),
        "summary": summary,
    }


def list_jobs(
    season_id: str,
    *,
    limit: int = 50,
    run_id: str | None = None,
    status: str | None = None,
    platform: str | None = None,
) -> list[dict[str, Any]]:
    safe_limit = max(1, min(int(limit), 250))
    has_run_id = _column_exists("social", "scrape_jobs", "run_id")
    has_queue_fields = all(
        [
            _column_exists("social", "scrape_jobs", "attempt_count"),
            _column_exists("social", "scrape_jobs", "max_attempts"),
            _column_exists("social", "scrape_jobs", "priority"),
            _column_exists("social", "scrape_jobs", "available_at"),
            _column_exists("social", "scrape_jobs", "claimed_at"),
            _column_exists("social", "scrape_jobs", "heartbeat_at"),
            _column_exists("social", "scrape_jobs", "worker_id"),
            _column_exists("social", "scrape_jobs", "last_error_code"),
            _column_exists("social", "scrape_jobs", "last_error_class"),
        ]
    )

    select_run_id = "run_id::text as run_id" if has_run_id else "null::text as run_id"
    select_attempt_count = "attempt_count" if has_queue_fields else "0::int as attempt_count"
    select_max_attempts = "max_attempts" if has_queue_fields else "0::int as max_attempts"
    select_priority = "priority" if has_queue_fields else "0::int as priority"
    select_available_at = "available_at" if has_queue_fields else "null::timestamptz as available_at"
    select_claimed_at = "claimed_at" if has_queue_fields else "null::timestamptz as claimed_at"
    select_heartbeat_at = "heartbeat_at" if has_queue_fields else "null::timestamptz as heartbeat_at"
    select_worker_id = "worker_id" if has_queue_fields else "null::text as worker_id"
    select_last_error_code = "last_error_code" if has_queue_fields else "null::text as last_error_code"
    select_last_error_class = "last_error_class" if has_queue_fields else "null::text as last_error_class"

    sql = """
        select
          id::text,
          """
    sql += select_run_id
    sql += """,
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
          initiated_by,
          """
    sql += select_attempt_count
    sql += """,
          """
    sql += select_max_attempts
    sql += """,
          """
    sql += select_priority
    sql += """,
          """
    sql += select_available_at
    sql += """,
          """
    sql += select_claimed_at
    sql += """,
          """
    sql += select_heartbeat_at
    sql += """,
          """
    sql += select_worker_id
    sql += """,
          """
    sql += select_last_error_code
    sql += """,
          """
    sql += select_last_error_class
    sql += """
        from social.scrape_jobs
        where season_id = %s
    """
    params: list[Any] = [season_id]
    if run_id and has_run_id:
        sql += " and run_id = %s"
        params.append(run_id)
    if status:
        sql += " and status = %s"
        params.append(status)
    if platform:
        sql += " and platform = %s"
        params.append(platform)
    sql += " order by created_at desc limit %s"
    params.append(safe_limit)
    return pg.fetch_all(sql, params)


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
            "and lower(coalesce(nullif(p.username, ''), p.source_account, '')) in ('bravotv', 'bravo')"
            if bravo_scope
            else ""
        )
        account_filter_comments = (
            "and lower(coalesce(nullif(p.username, ''), p.source_account, '')) in ('bravotv', 'bravo')"
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
            "and lower(coalesce(nullif(p.username, ''), p.source_account, '')) in ('bravotv', 'bravo')"
            if bravo_scope
            else ""
        )
        account_filter_comments = (
            "and lower(coalesce(nullif(p.username, ''), p.source_account, '')) in ('bravotv', 'bravo')"
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
            "and lower(coalesce(nullif(v.channel_title, ''), v.source_account, '')) in ('bravo', 'bravotv')"
            if bravo_scope
            else ""
        )
        account_filter_comments = (
            "and lower(coalesce(nullif(v.channel_title, ''), v.source_account, '')) in ('bravo', 'bravotv')"
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
            "and lower(coalesce(nullif(t.username, ''), t.source_account, '')) in ('bravotv', 'bravo')"
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
