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
SUPPORTED_INGEST_MODES = ("posts_only", "posts_and_comments", "comments_only")
SUPPORTED_SYNC_STRATEGIES = ("incremental", "full_refresh")
JOB_PROGRESS_UPDATE_EVERY = 25
COMMENT_STALE_RECHECK_INTERVAL = timedelta(hours=24)
QUIET_POST_FORCE_RECHECK_AGE = timedelta(days=14)

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
MENTION_TOKEN_RE = re.compile(r"@([A-Za-z0-9_.]+)")
TRAILER_MARKER_RE = re.compile(
    r"\b(first look|sneak peek|trailer|season announcement|official trailer)\b",
    re.IGNORECASE,
)
YOUTUBE_GENERIC_SEASON_TERM_RE = re.compile(r"^(season\s+\d+|s\d+)$", re.IGNORECASE)

NEGATION_WORDS = {
    "aint",
    "aren't",
    "cant",
    "can't",
    "didnt",
    "didn't",
    "doesnt",
    "doesn't",
    "dont",
    "don't",
    "hardly",
    "isnt",
    "isn't",
    "never",
    "no",
    "not",
    "nothing",
    "wasnt",
    "wasn't",
    "without",
    "wont",
    "won't",
}

INTENSIFIER_WEIGHTS = {
    "absolutely": 1.5,
    "crazy": 1.2,
    "extremely": 1.6,
    "incredibly": 1.5,
    "really": 1.25,
    "so": 1.2,
    "super": 1.35,
    "totally": 1.35,
    "very": 1.2,
}

DIMINISHER_WEIGHTS = {
    "barely": 0.7,
    "kind": 0.8,
    "kinda": 0.8,
    "slightly": 0.7,
    "somewhat": 0.8,
    "sort": 0.8,
}

CONTRAST_MARKERS = {"but", "however", "though", "although", "yet"}
DEFAULT_GEMINI_SENTIMENT_MODEL = "gemini-2.5-flash"
SENTIMENT_GEMINI_MAX_COMMENTS_CAP = 120
SENTIMENT_GEMINI_BATCH_SIZE_CAP = 25


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
    sync_strategy: str
    max_posts_per_target: int
    max_comments_per_post: int
    max_replies_per_post: int
    fetch_replies: bool
    ingest_mode: str
    date_start: datetime | None
    date_end: datetime | None


@dataclass(slots=True)
class CommentLifecycleSnapshot:
    active_count: int
    total_count: int
    latest_comment_created_at: datetime | None
    last_seen_at: datetime | None
    last_checked_at: datetime | None


@dataclass(slots=True)
class CommentRefreshDecision:
    should_refresh: bool
    reason: str


@dataclass(slots=True)
class EpisodeSentimentContext:
    summary: str
    terms: set[str]


@dataclass(slots=True)
class SentimentAnalyzerContext:
    cast_terms: set[str]
    cast_phrases: set[str]
    episode_terms: set[str]
    episode_summary: str


@dataclass(slots=True)
class SentimentRuleResult:
    label: str
    score: float
    confidence: float
    ambiguous: bool


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
    max_posts_per_target: int,
    max_comments_per_post: int,
    max_replies_per_post: int,
    fetch_replies: bool,
) -> tuple[int, int, int, bool]:
    return (
        max(1, max_posts_per_target),
        max(0, max(max_comments_per_post, 200)),
        max(0, max(max_replies_per_post, 100)),
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


_column_exists_cache: dict[tuple[str, str, str], bool] = {}


def _column_exists(schema: str, table: str, column: str) -> bool:
    key = (schema, table, column)
    cached = _column_exists_cache.get(key)
    if cached is not None:
        return cached
    row = (
        pg.fetch_one(
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
        )
        or {}
    )
    result = bool(row.get("exists"))
    _column_exists_cache[key] = result
    return result


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
            f"Social ingest queue schema is not migrated. Apply migrations 0121, 0122, 0123. Missing: {details}"
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
            token for token in _slug_words(franchise_match.group(1)) if token.lower() not in {"the", "of", "and"}
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


def _youtube_title_is_cross_show_excluded(title: str | None) -> bool:
    text = str(title or "").lower()
    if not text:
        return False
    return "wife swap" in text and "real housewives edition" in text


def _youtube_video_matches_show_terms(
    *,
    title: str | None,
    description: str | None,
    hashtags: list[str],
    keywords: list[str],
) -> bool:
    """Apply strict show matching for YouTube ingestion.

    Rules:
    - Match if show terms appear in title.
    - Match if configured show hashtags appear in description with ``#`` prefix.
    - Exclude known cross-show promos by title pattern.
    """
    if _youtube_title_is_cross_show_excluded(title):
        return False

    show_hashtags = [
        str(tag).strip().lstrip("#").lower()
        for tag in hashtags
        if str(tag).strip() and not YOUTUBE_GENERIC_SEASON_TERM_RE.fullmatch(str(tag).strip().lstrip("#"))
    ]
    show_keywords = [
        str(keyword).strip()
        for keyword in keywords
        if str(keyword).strip() and not YOUTUBE_GENERIC_SEASON_TERM_RE.fullmatch(str(keyword).strip())
    ]

    if not show_hashtags and not show_keywords:
        return _text_contains_any_term(text=f"{title or ''} {description or ''}", hashtags=hashtags, keywords=keywords)

    title_match = _text_contains_any_term(text=title, hashtags=show_hashtags, keywords=show_keywords)
    if title_match:
        return True

    description_lower = str(description or "").lower()
    if not description_lower:
        return False

    return any(f"#{tag}" in description_lower for tag in show_hashtags)


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

    raw_json = (os.getenv("SOCIAL_INSTAGRAM_COOKIES_JSON") or "").strip() or (
        os.getenv("INSTAGRAM_COOKIES_JSON") or ""
    ).strip()
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
    raw_json = (os.getenv("SOCIAL_TWITTER_COOKIES_JSON") or "").strip() or (
        os.getenv("TWITTER_COOKIES_JSON") or ""
    ).strip()
    if raw_json:
        try:
            parsed = json.loads(raw_json)
            if isinstance(parsed, dict):
                cookies = {str(k): str(v) for k, v in parsed.items() if v is not None and not str(k).startswith("_")}
        except json.JSONDecodeError:
            logger.warning("Invalid Twitter cookies JSON from env")

    if not cookies:
        file_path = (os.getenv("SOCIAL_TWITTER_COOKIES_FILE") or "").strip() or (
            os.getenv("TWITTER_COOKIES_FILE") or ""
        ).strip()
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
    raw_json = (os.getenv("SOCIAL_TIKTOK_COOKIES_JSON") or "").strip() or (
        os.getenv("TIKTOK_COOKIES_JSON") or ""
    ).strip()
    if raw_json:
        try:
            parsed = json.loads(raw_json)
            if isinstance(parsed, dict):
                cookies = {str(k): str(v) for k, v in parsed.items() if v is not None and not str(k).startswith("_")}
                if cookies:
                    return cookies
        except json.JSONDecodeError:
            logger.warning("Invalid TikTok cookies JSON from env")

    file_path = (os.getenv("SOCIAL_TIKTOK_COOKIES_FILE") or "").strip() or (
        os.getenv("TIKTOK_COOKIES_FILE") or ""
    ).strip()
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


def _default_targets(context: SeasonContext, *, source_scope: str = "bravo") -> list[dict[str, Any]]:
    if source_scope != "bravo":
        return []

    default_hashtags, default_keywords = _derive_show_terms(context.show_name)
    keywords = _normalize_unique_terms([*default_keywords, f"season {context.season_number}"])
    hashtags = _normalize_unique_terms(default_hashtags)

    defaults = [
        {
            "platform": "instagram",
            "source_scope": source_scope,
            "timezone": "America/New_York",
            "accounts": ["bravotv", "bravodailydish", "bravowwhl"],
            "hashtags": hashtags,
            "keywords": keywords,
            "is_active": True,
            "config": {"include_comments": True},
        },
        {
            "platform": "tiktok",
            "source_scope": source_scope,
            "timezone": "America/New_York",
            "accounts": ["bravotv", "bravowwhl"],
            "hashtags": hashtags,
            "keywords": keywords,
            "is_active": True,
            "config": {"include_comments": True},
        },
        {
            "platform": "twitter",
            "source_scope": source_scope,
            "timezone": "America/New_York",
            "accounts": ["BravoTV", "BravoWWHL"],
            "hashtags": hashtags,
            "keywords": keywords,
            "is_active": True,
            "config": {"include_replies": True},
        },
        {
            "platform": "youtube",
            "source_scope": source_scope,
            "timezone": "America/New_York",
            "accounts": ["bravo", "wwhl"],
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
        rows = _default_targets(context, source_scope=source_scope)

    return {
        "season_id": context.season_id,
        "show_id": context.show_id,
        "season_number": context.season_number,
        "show_name": context.show_name,
        "source_scope": source_scope,
        "targets": rows,
        "using_defaults": len(rows) > 0 and "created_at" not in rows[0],
    }


def _normalize_account_handle(value: Any) -> str:
    return str(value or "").strip().lstrip("@").lower()


def _target_accounts_by_platform(season_id: str, *, source_scope: str) -> dict[str, set[str]]:
    targets_payload = get_targets(season_id, source_scope=source_scope)
    accounts_by_platform: dict[str, set[str]] = {platform: set() for platform in SUPPORTED_PLATFORMS}

    for target in targets_payload.get("targets", []):
        if not bool(target.get("is_active", True)):
            continue
        platform = str(target.get("platform") or "").strip().lower()
        if platform not in accounts_by_platform:
            continue
        for account in target.get("accounts") or []:
            normalized = _normalize_account_handle(account)
            if normalized:
                accounts_by_platform[platform].add(normalized)

    return accounts_by_platform


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
    summary_row = (
        pg.fetch_one(
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
        )
        or {}
    )

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


def _parse_platform_time(ts: Any) -> datetime | None:
    """Parse a platform timestamp (unix int/float or ISO string) to UTC datetime."""
    if isinstance(ts, int):
        return datetime.fromtimestamp(ts, tz=UTC)
    if isinstance(ts, float):
        return datetime.fromtimestamp(int(ts), tz=UTC)
    return _coerce_dt(ts)


# Backwards-compatible aliases
_parse_instagram_time = _parse_platform_time
_parse_tiktok_time = _parse_platform_time


def _count_stored_comments(post_ids: list[str], platform: str) -> dict[str, int]:
    """Return {post_db_id: stored_comment_count} for a batch of posts."""
    if not post_ids:
        return {}
    comment_table_map = {
        "instagram": ("instagram_comments", "post_id"),
        "tiktok": ("tiktok_comments", "post_id"),
        "youtube": ("youtube_comments", "video_id"),
    }
    entry = comment_table_map.get(platform)
    if not entry:
        return {}
    table, fk_col = entry
    placeholders = ",".join(["%s"] * len(post_ids))
    rows = pg.fetch_all(
        f"SELECT {fk_col}::text AS pid, count(*)::int AS cnt "
        f"FROM social.{table} WHERE {fk_col} IN ({placeholders}) GROUP BY {fk_col}",
        post_ids,
    )
    return {str(r["pid"]): int(r["cnt"]) for r in rows}


def _count_stored_replies(tweet_ids: list[str]) -> dict[str, int]:
    """Return {tweet_id: stored_reply_count} for Twitter anchor tweets."""
    if not tweet_ids:
        return {}
    placeholders = ",".join(["%s"] * len(tweet_ids))
    rows = pg.fetch_all(
        f"SELECT reply_to_tweet_id AS tid, count(*)::int AS cnt "
        f"FROM social.twitter_tweets "
        f"WHERE reply_to_tweet_id IN ({placeholders}) AND is_reply = true "
        f"GROUP BY reply_to_tweet_id",
        tweet_ids,
    )
    return {str(r["tid"]): int(r["cnt"]) for r in rows}


def _comment_lifecycle_supported(table: str) -> bool:
    return all(
        _column_exists("social", table, column)
        for column in ("is_missing", "missing_at", "first_seen_at", "last_seen_at", "last_seen_run_id")
    )


def _build_comment_snapshot_map(rows: list[dict[str, Any]]) -> dict[str, CommentLifecycleSnapshot]:
    snapshots: dict[str, CommentLifecycleSnapshot] = {}
    for row in rows:
        anchor_id = str(row.get("anchor_id") or "")
        if not anchor_id:
            continue
        snapshots[anchor_id] = CommentLifecycleSnapshot(
            active_count=int(row.get("active_count") or 0),
            total_count=int(row.get("total_count") or 0),
            latest_comment_created_at=_coerce_dt(row.get("latest_comment_created_at")),
            last_seen_at=_coerce_dt(row.get("last_seen_at")),
            last_checked_at=_coerce_dt(row.get("last_checked_at")),
        )
    return snapshots


def _load_comment_lifecycle_snapshots(anchor_ids: list[str], *, platform: str) -> dict[str, CommentLifecycleSnapshot]:
    if not anchor_ids:
        return {}

    placeholders = ",".join(["%s"] * len(anchor_ids))
    normalized_platform = (platform or "").strip().lower()

    if normalized_platform == "twitter":
        lifecycle_supported = _comment_lifecycle_supported("twitter_tweets")
        if lifecycle_supported:
            rows = pg.fetch_all(
                f"""
                select
                  reply_to_tweet_id as anchor_id,
                  count(*) filter (where is_missing = false)::int as active_count,
                  count(*)::int as total_count,
                  max(created_at) filter (where is_missing = false) as latest_comment_created_at,
                  max(last_seen_at) filter (where is_missing = false) as last_seen_at,
                  coalesce(max(last_seen_at), max(scraped_at)) as last_checked_at
                from social.twitter_tweets
                where is_reply = true
                  and reply_to_tweet_id in ({placeholders})
                group by reply_to_tweet_id
                """,
                anchor_ids,
            )
        else:
            rows = pg.fetch_all(
                f"""
                select
                  reply_to_tweet_id as anchor_id,
                  count(*)::int as active_count,
                  count(*)::int as total_count,
                  max(created_at) as latest_comment_created_at,
                  null::timestamptz as last_seen_at,
                  max(scraped_at) as last_checked_at
                from social.twitter_tweets
                where is_reply = true
                  and reply_to_tweet_id in ({placeholders})
                group by reply_to_tweet_id
                """,
                anchor_ids,
            )
        return _build_comment_snapshot_map(rows)

    table_map = {
        "instagram": ("instagram_comments", "post_id", "comment_id"),
        "tiktok": ("tiktok_comments", "post_id", "comment_id"),
        "youtube": ("youtube_comments", "video_id", "comment_id"),
    }
    entry = table_map.get(normalized_platform)
    if not entry:
        return {}

    table, anchor_col, _external_id_col = entry
    lifecycle_supported = _comment_lifecycle_supported(table)
    if lifecycle_supported:
        rows = pg.fetch_all(
            f"""
            select
              {anchor_col}::text as anchor_id,
              count(*) filter (where is_missing = false)::int as active_count,
              count(*)::int as total_count,
              max(created_at) filter (where is_missing = false) as latest_comment_created_at,
              max(last_seen_at) filter (where is_missing = false) as last_seen_at,
              coalesce(max(last_seen_at), max(scraped_at)) as last_checked_at
            from social.{table}
            where {anchor_col} in ({placeholders})
            group by {anchor_col}
            """,
            anchor_ids,
        )
    else:
        rows = pg.fetch_all(
            f"""
            select
              {anchor_col}::text as anchor_id,
              count(*)::int as active_count,
              count(*)::int as total_count,
              max(created_at) as latest_comment_created_at,
              null::timestamptz as last_seen_at,
              max(scraped_at) as last_checked_at
            from social.{table}
            where {anchor_col} in ({placeholders})
            group by {anchor_col}
            """,
            anchor_ids,
        )
    return _build_comment_snapshot_map(rows)


def _decide_comment_refresh(
    *,
    sync_strategy: str,
    expected_count: int,
    snapshot: CommentLifecycleSnapshot | None,
    post_published_at: datetime | None,
    now: datetime | None = None,
) -> CommentRefreshDecision:
    if sync_strategy == "full_refresh":
        return CommentRefreshDecision(should_refresh=True, reason="full_refresh")

    now_utc = now or _now_utc()
    if snapshot is None:
        return CommentRefreshDecision(should_refresh=True, reason="never_checked")

    active_stored = int(snapshot.active_count) if snapshot else 0
    if expected_count > active_stored:
        return CommentRefreshDecision(should_refresh=True, reason="count_gap")
    if expected_count < active_stored:
        return CommentRefreshDecision(should_refresh=True, reason="count_drop")

    quiet_anchor = snapshot.latest_comment_created_at or post_published_at
    if quiet_anchor:
        quiet_age = now_utc - (quiet_anchor if quiet_anchor.tzinfo else quiet_anchor.replace(tzinfo=UTC))
        if quiet_age >= QUIET_POST_FORCE_RECHECK_AGE:
            return CommentRefreshDecision(should_refresh=True, reason="quiet_post_force_recheck")

    if not snapshot.last_checked_at:
        return CommentRefreshDecision(should_refresh=True, reason="never_checked")

    checked_at = snapshot.last_checked_at
    if not checked_at.tzinfo:
        checked_at = checked_at.replace(tzinfo=UTC)
    if (now_utc - checked_at) >= COMMENT_STALE_RECHECK_INTERVAL:
        return CommentRefreshDecision(should_refresh=True, reason="stale_recheck")

    return CommentRefreshDecision(should_refresh=False, reason="up_to_date")


def _is_comment_fetch_complete(
    *,
    fetch_failed: bool,
    fail_reason: str | None,
    auth_failed: bool = False,
    fetched_count: int,
    max_comments_per_post: int,
) -> bool:
    if fetch_failed:
        return False
    if auth_failed:
        return False
    if fail_reason:
        return False
    # Conservative guard: if we hit local cap, we cannot guarantee full coverage.
    if max_comments_per_post > 0 and fetched_count >= max_comments_per_post:
        return False
    return True


def _mark_missing_comments_for_anchor(
    *,
    platform: str,
    anchor_id: str,
    observed_comment_ids: set[str],
    conn: Any | None = None,
) -> int:
    normalized_platform = (platform or "").strip().lower()
    if normalized_platform == "twitter":
        if not _comment_lifecycle_supported("twitter_tweets"):
            return 0
        base_sql = """
            update social.twitter_tweets
            set
              is_missing = true,
              missing_at = coalesce(missing_at, now())
            where is_reply = true
              and reply_to_tweet_id = %s
              and is_missing = false
        """
        params: list[Any] = [anchor_id]
        if observed_comment_ids:
            placeholders = ",".join(["%s"] * len(observed_comment_ids))
            base_sql += f" and tweet_id not in ({placeholders})"
            params.extend(sorted(observed_comment_ids))
        base_sql += " returning id::text"
        with pg.db_cursor(conn=conn) as cur:
            rows = pg.fetch_all_with_cursor(cur, base_sql, params)
        return len(rows)

    table_map = {
        "instagram": ("instagram_comments", "post_id", "comment_id"),
        "tiktok": ("tiktok_comments", "post_id", "comment_id"),
        "youtube": ("youtube_comments", "video_id", "comment_id"),
    }
    entry = table_map.get(normalized_platform)
    if not entry:
        return 0

    table, anchor_col, external_id_col = entry
    if not _comment_lifecycle_supported(table):
        return 0

    sql = f"""
        update social.{table}
        set
          is_missing = true,
          missing_at = coalesce(missing_at, now())
        where {anchor_col} = %s
          and is_missing = false
    """
    params = [anchor_id]
    if observed_comment_ids:
        placeholders = ",".join(["%s"] * len(observed_comment_ids))
        sql += f" and {external_id_col} not in ({placeholders})"
        params.extend(sorted(observed_comment_ids))
    sql += " returning id::text"
    with pg.db_cursor(conn=conn) as cur:
        rows = pg.fetch_all_with_cursor(cur, sql, params)
    return len(rows)


def _load_existing_posts(
    platform: str,
    context: SeasonContext,
    account: str,
    date_start: datetime | None,
    date_end: datetime | None,
) -> list[dict[str, Any]]:
    """Load existing posts from the DB for the comment-only stage."""
    table_map = {
        "instagram": ("instagram_posts", "posted_at"),
        "tiktok": ("tiktok_posts", "posted_at"),
        "youtube": ("youtube_videos", "published_at"),
        "twitter": ("twitter_tweets", "created_at"),
    }
    entry = table_map.get(platform)
    if not entry:
        return []
    table, ts_col = entry

    conditions = ["season_id = %s", "source_account = %s"]
    params: list[Any] = [context.season_id, account]
    if date_start:
        conditions.append(f"{ts_col} >= %s")
        params.append(date_start)
    if date_end:
        conditions.append(f"{ts_col} <= %s")
        params.append(date_end)

    where = " AND ".join(conditions)
    sql = f"SELECT * FROM social.{table} WHERE {where} ORDER BY {ts_col} ASC"
    return pg.fetch_all(sql, params)


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
    media_urls = [str(url).strip() for url in (getattr(post, "media_urls", []) or []) if str(url).strip()]
    thumbnail_url = str(getattr(post, "thumbnail_url", "") or "").strip() or (media_urls[0] if media_urls else None)
    payload = {
        "shortcode": getattr(post, "shortcode", ""),
        "media_id": getattr(post, "pk", None),
        "username": getattr(post, "username", account),
        "user_id": None,
        "caption": getattr(post, "caption", None),
        "media_type": getattr(post, "post_type", None),
        "media_urls": media_urls,
        "thumbnail_url": thumbnail_url,
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
    job_id: str | None,
    run_id: str | None,
    account: str,
    post_id: str,
    comment: Any,
    parent_comment_db_id: str | None = None,
    observed_comment_ids: set[str] | None = None,
    conn: Any | None = None,
) -> int:
    created_at = _parse_instagram_time(getattr(comment, "created_at", None))
    comment_external_id = str(getattr(comment, "comment_id", "") or "")
    if observed_comment_ids is not None and comment_external_id:
        observed_comment_ids.add(comment_external_id)
    payload = {
        "comment_id": comment_external_id,
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
        "source_account": account,
    }
    if job_id:
        payload["job_id"] = job_id
    if _comment_lifecycle_supported("instagram_comments"):
        payload["is_missing"] = False
        payload["missing_at"] = None
        payload["last_seen_at"] = _now_utc()
        payload["last_seen_run_id"] = run_id
    row = _pg_upsert("instagram_comments", payload, conflict_col="comment_id", conn=conn)
    comment_db_id = (row or {}).get("id")

    total = 1
    for reply in getattr(comment, "replies", []) or []:
        total += _upsert_instagram_comment_tree(
            context,
            job_id=job_id,
            run_id=run_id,
            account=account,
            post_id=post_id,
            comment=reply,
            parent_comment_db_id=comment_db_id,
            observed_comment_ids=observed_comment_ids,
            conn=conn,
        )
    return total


def _ingest_instagram(
    context: SeasonContext,
    *,
    run_id: str | None,
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
    instagram_authenticated = bool(cookies.get("sessionid"))
    if not instagram_authenticated:
        logger.warning("Instagram ingest running without sessionid cookie; results may be limited to ~12 recent posts")
    scraper = InstagramScraper(cookies=cookies)

    retrieval_meta: dict[str, Any] = {"instagram_authenticated": instagram_authenticated}
    post_count = 0
    comment_count = 0
    comment_errors = 0
    comment_fail_reasons: set[str] = set()
    comment_refresh_reasons: Counter[str] = Counter()
    missing_marked = 0
    incomplete_comment_fetches = 0
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

    if stage == "comments":
        # Comment-only stage: read existing posts from DB instead of re-scraping
        existing_posts = _load_existing_posts("instagram", context, account, opts.date_start, opts.date_end)
        logger.info(
            "[instagram] Comments stage: %d existing posts from DB for account=%s",
            len(existing_posts),
            account,
        )
        retrieval_meta["source"] = "db"
        snapshots = _load_comment_lifecycle_snapshots(
            [str(r["id"]) for r in existing_posts if r.get("id")],
            platform="instagram",
        )
        skipped_synced = 0
        with pg.db_connection() as conn:
            for row in existing_posts:
                shortcode = str(row.get("shortcode") or "")
                post_db_id = str(row.get("id") or "")
                if not shortcode:
                    continue
                expected = int(row.get("comments_count") or 0)
                decision = _decide_comment_refresh(
                    sync_strategy=opts.sync_strategy,
                    expected_count=expected,
                    snapshot=snapshots.get(post_db_id),
                    post_published_at=_coerce_dt(row.get("posted_at")),
                )
                comment_refresh_reasons[decision.reason] += 1
                if not decision.should_refresh:
                    skipped_synced += 1
                    continue
                post_count += 1
                if post_count % 10 == 0:
                    _touch_job_heartbeat(job_id)
                fetch_failed = False
                comments: list[Any] = []
                fail_reason = ""
                observed_comment_ids: set[str] = set()
                try:
                    comments = scraper.fetch_comments(
                        shortcode,
                        max_comments=opts.max_comments_per_post,
                        fetch_replies=opts.fetch_replies,
                        delay=comment_delay_seconds,
                    )
                    fail_reason = str(getattr(scraper, "last_comment_fetch_reason", "") or "")
                    if fail_reason:
                        comment_fail_reasons.add(fail_reason)
                        if not comments:
                            comment_errors += 1
                    for comment in comments:
                        comment_count += _upsert_instagram_comment_tree(
                            context,
                            job_id=job_id,
                            run_id=run_id,
                            account=account,
                            post_id=post_db_id,
                            comment=comment,
                            observed_comment_ids=observed_comment_ids,
                            conn=conn,
                        )
                        _report_progress()
                except Exception:
                    fetch_failed = True
                    comment_errors += 1
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    logger.exception(
                        "[instagram] Failed to fetch comments for post %s (shortcode=%s)",
                        post_db_id,
                        shortcode,
                    )
                is_complete = _is_comment_fetch_complete(
                    fetch_failed=fetch_failed,
                    fail_reason=fail_reason,
                    auth_failed=bool(getattr(scraper, "comments_auth_failed", False)),
                    fetched_count=len(comments),
                    max_comments_per_post=opts.max_comments_per_post,
                )
                if is_complete:
                    missing_marked += _mark_missing_comments_for_anchor(
                        platform="instagram",
                        anchor_id=post_db_id,
                        observed_comment_ids=observed_comment_ids,
                        conn=conn,
                    )
                else:
                    incomplete_comment_fetches += 1
        if skipped_synced:
            logger.info("[instagram] Skipped %d posts whose comments are already synced", skipped_synced)
    else:
        # Posts stage (or posts+comments): scrape from live API
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
        retrieval_meta.update(dict(getattr(scraper, "last_retrieval_meta", {}) or {}))

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
                    try:
                        comments = scraper.fetch_comments(
                            getattr(post, "shortcode", ""),
                            max_comments=opts.max_comments_per_post,
                            fetch_replies=opts.fetch_replies,
                            delay=comment_delay_seconds,
                        )
                        fail_reason = str(getattr(scraper, "last_comment_fetch_reason", "") or "")
                        if fail_reason:
                            comment_fail_reasons.add(fail_reason)
                            if not comments:
                                comment_errors += 1
                        for comment in comments:
                            comment_count += _upsert_instagram_comment_tree(
                                context,
                                job_id=job_id,
                                run_id=run_id,
                                account=account,
                                post_id=str(upserted["id"]),
                                comment=comment,
                                conn=conn,
                            )
                            _report_progress()
                    except Exception:
                        comment_errors += 1
                        try:
                            conn.rollback()
                        except Exception:
                            pass
                        logger.exception(
                            "[instagram] Failed to fetch comments for post %s (shortcode=%s)",
                            upserted.get("id"),
                            getattr(post, "shortcode", "?"),
                        )

    _report_progress(force=True)

    if comment_errors:
        retrieval_meta["comment_errors"] = comment_errors
    if comment_fail_reasons:
        retrieval_meta["comment_fail_reasons"] = sorted(comment_fail_reasons)
    if comment_refresh_reasons:
        retrieval_meta["comment_refresh_decisions"] = dict(comment_refresh_reasons)
    if missing_marked:
        retrieval_meta["comments_marked_missing"] = missing_marked
    if incomplete_comment_fetches:
        retrieval_meta["incomplete_comment_fetches"] = incomplete_comment_fetches
    if scraper.comments_auth_failed:
        retrieval_meta["comments_auth_failed"] = True
    logger.info(
        "[instagram] Done: scraped=%d matched=%d skipped_keyword=%d comments=%d comment_errors=%d auth_failed=%s",
        total_scraped,
        post_count,
        skipped_keyword,
        comment_count,
        comment_errors,
        scraper.comments_auth_failed,
    )
    return post_count, comment_count, retrieval_meta


def _upsert_tiktok_post(
    context: SeasonContext, *, job_id: str, account: str, post: Any, conn: Any | None = None
) -> dict[str, Any] | None:
    posted_at = _parse_tiktok_time(getattr(post, "create_time", None))
    media_urls = [str(url).strip() for url in (getattr(post, "media_urls", []) or []) if str(url).strip()]
    thumbnail_url = str(getattr(post, "thumbnail_url", "") or "").strip() or (media_urls[0] if media_urls else None)
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
        "thumbnail_url": thumbnail_url,
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
    job_id: str | None,
    run_id: str | None,
    account: str,
    post_id: str,
    comment: Any,
    parent_comment_db_id: str | None = None,
    observed_comment_ids: set[str] | None = None,
    conn: Any | None = None,
) -> int:
    created_at = _parse_tiktok_time(getattr(comment, "created_at", None))
    comment_external_id = str(getattr(comment, "comment_id", "") or "")
    if observed_comment_ids is not None and comment_external_id:
        observed_comment_ids.add(comment_external_id)
    payload = {
        "comment_id": comment_external_id,
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
        "source_account": account,
    }
    if job_id:
        payload["job_id"] = job_id
    if _comment_lifecycle_supported("tiktok_comments"):
        payload["is_missing"] = False
        payload["missing_at"] = None
        payload["last_seen_at"] = _now_utc()
        payload["last_seen_run_id"] = run_id
    row = _pg_upsert("tiktok_comments", payload, conflict_col="comment_id", conn=conn)
    comment_db_id = (row or {}).get("id")

    total = 1
    for reply in getattr(comment, "replies", []) or []:
        total += _upsert_tiktok_comment_tree(
            context,
            job_id=job_id,
            run_id=run_id,
            account=account,
            post_id=post_id,
            comment=reply,
            parent_comment_db_id=comment_db_id,
            observed_comment_ids=observed_comment_ids,
            conn=conn,
        )
    return total


def _ingest_tiktok(
    context: SeasonContext,
    *,
    run_id: str | None,
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

    retrieval_meta: dict[str, Any] = {}
    post_count = 0
    comment_count = 0
    comment_errors = 0
    comment_fail_reasons: set[str] = set()
    comment_refresh_reasons: Counter[str] = Counter()
    missing_marked = 0
    incomplete_comment_fetches = 0
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

    if stage == "comments":
        existing_posts = _load_existing_posts("tiktok", context, account, opts.date_start, opts.date_end)
        logger.info("[tiktok] Comments stage: %d existing posts from DB for account=%s", len(existing_posts), account)
        retrieval_meta["source"] = "db"
        snapshots = _load_comment_lifecycle_snapshots(
            [str(r["id"]) for r in existing_posts if r.get("id")],
            platform="tiktok",
        )
        skipped_synced = 0
        with pg.db_connection() as conn:
            for row in existing_posts:
                video_id = str(row.get("video_id") or "")
                post_db_id = str(row.get("id") or "")
                if not video_id:
                    continue
                expected = int(row.get("comments_count") or 0)
                decision = _decide_comment_refresh(
                    sync_strategy=opts.sync_strategy,
                    expected_count=expected,
                    snapshot=snapshots.get(post_db_id),
                    post_published_at=_coerce_dt(row.get("posted_at")),
                )
                comment_refresh_reasons[decision.reason] += 1
                if not decision.should_refresh:
                    skipped_synced += 1
                    continue
                post_count += 1
                if post_count % 10 == 0:
                    _touch_job_heartbeat(job_id)
                fetch_failed = False
                comments: list[Any] = []
                fail_reason = ""
                observed_comment_ids: set[str] = set()
                try:
                    comments = scraper.fetch_comments(
                        video_id,
                        username=account,
                        max_comments=opts.max_comments_per_post,
                        fetch_replies=opts.fetch_replies,
                        delay=0.5,
                    )
                    fail_reason = str(getattr(scraper, "_last_api_fail_reason", "") or "")
                    if fail_reason:
                        comment_fail_reasons.add(fail_reason)
                        if not comments:
                            comment_errors += 1
                    for comment in comments:
                        comment_count += _upsert_tiktok_comment_tree(
                            context,
                            job_id=job_id,
                            run_id=run_id,
                            account=account,
                            post_id=post_db_id,
                            comment=comment,
                            observed_comment_ids=observed_comment_ids,
                            conn=conn,
                        )
                        _report_progress()
                except Exception:
                    fetch_failed = True
                    comment_errors += 1
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    logger.exception(
                        "[tiktok] Failed to fetch comments for post %s (video_id=%s)",
                        post_db_id,
                        video_id,
                    )
                is_complete = _is_comment_fetch_complete(
                    fetch_failed=fetch_failed,
                    fail_reason=fail_reason,
                    fetched_count=len(comments),
                    max_comments_per_post=opts.max_comments_per_post,
                )
                if is_complete:
                    missing_marked += _mark_missing_comments_for_anchor(
                        platform="tiktok",
                        anchor_id=post_db_id,
                        observed_comment_ids=observed_comment_ids,
                        conn=conn,
                    )
                else:
                    incomplete_comment_fetches += 1
        if skipped_synced:
            logger.info("[tiktok] Skipped %d posts whose comments are already synced", skipped_synced)
    else:
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
        retrieval_meta.update(dict(getattr(scraper, "last_retrieval_meta", {}) or {}))

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
                    try:
                        comments = scraper.fetch_comments(
                            getattr(post, "video_id", ""),
                            username=account,
                            max_comments=opts.max_comments_per_post,
                            fetch_replies=opts.fetch_replies,
                            delay=0.5,
                        )
                        fail_reason = str(getattr(scraper, "_last_api_fail_reason", "") or "")
                        if fail_reason:
                            comment_fail_reasons.add(fail_reason)
                            if not comments:
                                comment_errors += 1
                        for comment in comments:
                            comment_count += _upsert_tiktok_comment_tree(
                                context,
                                job_id=job_id,
                                run_id=run_id,
                                account=account,
                                post_id=str(upserted["id"]),
                                comment=comment,
                                conn=conn,
                            )
                            _report_progress()
                    except Exception:
                        comment_errors += 1
                        try:
                            conn.rollback()
                        except Exception:
                            pass
                        logger.exception(
                            "[tiktok] Failed to fetch comments for post %s (video_id=%s)",
                            upserted.get("id"),
                            getattr(post, "video_id", "?"),
                        )

    _report_progress(force=True)

    if comment_errors:
        retrieval_meta["comment_errors"] = comment_errors
    if comment_fail_reasons:
        retrieval_meta["comment_fail_reasons"] = sorted(comment_fail_reasons)
    if comment_refresh_reasons:
        retrieval_meta["comment_refresh_decisions"] = dict(comment_refresh_reasons)
    if missing_marked:
        retrieval_meta["comments_marked_missing"] = missing_marked
    if incomplete_comment_fetches:
        retrieval_meta["incomplete_comment_fetches"] = incomplete_comment_fetches
    logger.info(
        "[tiktok] Done: scraped=%d matched=%d skipped_keyword=%d comments=%d comment_errors=%d",
        total_scraped,
        post_count,
        skipped_keyword,
        comment_count,
        comment_errors,
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
    job_id: str | None,
    run_id: str | None,
    account: str,
    video_db_id: str,
    comment: Any,
    parent_comment_db_id: str | None = None,
    observed_comment_ids: set[str] | None = None,
    conn: Any | None = None,
) -> int:
    created_at = _parse_instagram_time(getattr(comment, "created_at", None))
    comment_external_id = str(getattr(comment, "comment_id", "") or "")
    if observed_comment_ids is not None and comment_external_id:
        observed_comment_ids.add(comment_external_id)
    payload = {
        "comment_id": comment_external_id,
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
        "source_account": account,
    }
    if job_id:
        payload["job_id"] = job_id
    if _comment_lifecycle_supported("youtube_comments"):
        payload["is_missing"] = False
        payload["missing_at"] = None
        payload["last_seen_at"] = _now_utc()
        payload["last_seen_run_id"] = run_id
    row = _pg_upsert("youtube_comments", payload, conflict_col="comment_id", conn=conn)
    comment_db_id = (row or {}).get("id")

    total = 1
    for reply in getattr(comment, "replies", []) or []:
        total += _upsert_youtube_comment_tree(
            context,
            job_id=job_id,
            run_id=run_id,
            account=account,
            video_db_id=video_db_id,
            comment=reply,
            parent_comment_db_id=comment_db_id,
            observed_comment_ids=observed_comment_ids,
            conn=conn,
        )
    return total


def _ingest_youtube(
    context: SeasonContext,
    *,
    run_id: str | None,
    account: str,
    hashtags: list[str],
    keywords: list[str],
    opts: IngestOptions,
    job_id: str,
    stage: str = "posts",
) -> tuple[int, int, dict[str, Any]]:
    from trr_backend.socials.youtube import YouTubeScrapeConfig, YouTubeScraper

    scraper = YouTubeScraper()

    retrieval_meta: dict[str, Any] = {}
    video_count = 0
    comment_count = 0
    comment_errors = 0
    comment_refresh_reasons: Counter[str] = Counter()
    missing_marked = 0
    incomplete_comment_fetches = 0
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

    if stage == "comments":
        existing_posts = _load_existing_posts("youtube", context, account, opts.date_start, opts.date_end)
        logger.info("[youtube] Comments stage: %d existing videos from DB for account=%s", len(existing_posts), account)
        retrieval_meta["source"] = "db"
        snapshots = _load_comment_lifecycle_snapshots(
            [str(r["id"]) for r in existing_posts if r.get("id")],
            platform="youtube",
        )
        skipped_synced = 0
        with pg.db_connection() as conn:
            for row in existing_posts:
                vid_id = str(row.get("video_id") or "")
                post_db_id = str(row.get("id") or "")
                if not vid_id:
                    continue
                expected = int(row.get("comments_count") or 0)
                decision = _decide_comment_refresh(
                    sync_strategy=opts.sync_strategy,
                    expected_count=expected,
                    snapshot=snapshots.get(post_db_id),
                    post_published_at=_coerce_dt(row.get("published_at")),
                )
                comment_refresh_reasons[decision.reason] += 1
                if not decision.should_refresh:
                    skipped_synced += 1
                    continue
                video_count += 1
                if video_count % 10 == 0:
                    _touch_job_heartbeat(job_id)
                fetch_failed = False
                comments: list[Any] = []
                observed_comment_ids: set[str] = set()
                try:
                    comments = scraper.fetch_comments(
                        vid_id,
                        max_comments=opts.max_comments_per_post,
                        fetch_replies=opts.fetch_replies,
                        delay=0.5,
                    )
                    for comment in comments:
                        comment_count += _upsert_youtube_comment_tree(
                            context,
                            job_id=job_id,
                            run_id=run_id,
                            account=account,
                            video_db_id=post_db_id,
                            comment=comment,
                            observed_comment_ids=observed_comment_ids,
                            conn=conn,
                        )
                        _report_progress()
                except Exception:
                    fetch_failed = True
                    comment_errors += 1
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    logger.exception(
                        "[youtube] Failed to fetch comments for video %s (video_id=%s)",
                        post_db_id,
                        vid_id,
                    )
                is_complete = _is_comment_fetch_complete(
                    fetch_failed=fetch_failed,
                    fail_reason=None,
                    fetched_count=len(comments),
                    max_comments_per_post=opts.max_comments_per_post,
                )
                if is_complete:
                    missing_marked += _mark_missing_comments_for_anchor(
                        platform="youtube",
                        anchor_id=post_db_id,
                        observed_comment_ids=observed_comment_ids,
                        conn=conn,
                    )
                else:
                    incomplete_comment_fetches += 1
        if skipped_synced:
            logger.info("[youtube] Skipped %d videos whose comments are already synced", skipped_synced)
    else:
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
        retrieval_meta.update(dict(getattr(scraper, "last_retrieval_meta", {}) or {}))

        with pg.db_connection() as conn:
            for video in videos:
                total_scraped += 1
                if not _youtube_video_matches_show_terms(
                    title=getattr(video, "title", ""),
                    description=getattr(video, "description", ""),
                    hashtags=hashtags,
                    keywords=keywords,
                ):
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
                    try:
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
                                run_id=run_id,
                                account=account,
                                video_db_id=str(upserted["id"]),
                                comment=comment,
                                conn=conn,
                            )
                            _report_progress()
                    except Exception:
                        comment_errors += 1
                        try:
                            conn.rollback()
                        except Exception:
                            pass
                        logger.exception(
                            "[youtube] Failed to fetch comments for video %s (video_id=%s)",
                            upserted.get("id"),
                            getattr(video, "video_id", "?"),
                        )

    _report_progress(force=True)

    if comment_errors:
        retrieval_meta["comment_errors"] = comment_errors
    if comment_refresh_reasons:
        retrieval_meta["comment_refresh_decisions"] = dict(comment_refresh_reasons)
    if missing_marked:
        retrieval_meta["comments_marked_missing"] = missing_marked
    if incomplete_comment_fetches:
        retrieval_meta["incomplete_comment_fetches"] = incomplete_comment_fetches
    logger.info(
        "[youtube] Done: scraped=%d matched=%d skipped_keyword=%d comments=%d comment_errors=%d",
        total_scraped,
        video_count,
        skipped_keyword,
        comment_count,
        comment_errors,
    )
    return video_count, comment_count, retrieval_meta


def _upsert_tweet(
    context: SeasonContext,
    *,
    job_id: str | None,
    run_id: str | None,
    account: str,
    tweet: Any,
    conn: Any | None = None,
) -> dict[str, Any] | None:
    created_at = _parse_platform_time(getattr(tweet, "created_at", None))
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
        "source_account": account,
    }
    if job_id:
        payload["job_id"] = job_id
    if _comment_lifecycle_supported("twitter_tweets"):
        payload["is_missing"] = False
        payload["missing_at"] = None
        payload["last_seen_at"] = _now_utc()
        payload["last_seen_run_id"] = run_id
    return _pg_upsert("twitter_tweets", payload, conflict_col="tweet_id", conn=conn)


def _ingest_twitter(
    context: SeasonContext,
    *,
    run_id: str | None,
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

    twitter_cookies, twitter_bearer = _load_twitter_auth()
    twikit_creds = _load_twikit_credentials()
    # If no explicit Twitter cookies but twikit has auth_token/ct0, use them
    # so GraphQL endpoints (including TweetDetail for replies) get authenticated access.
    if not twitter_cookies.get("ct0") and twikit_creds:
        if twikit_creds.get("auth_token") and twikit_creds.get("ct0"):
            twitter_cookies = {**twitter_cookies, "auth_token": twikit_creds["auth_token"], "ct0": twikit_creds["ct0"]}
            logger.info("[twitter] Using twikit auth_token/ct0 cookies for GraphQL requests")
    scraper = TwitterScraper(cookies=twitter_cookies, bearer_token=twitter_bearer, twikit_credentials=twikit_creds)

    retrieval_meta: dict[str, Any] = {}
    post_count = 0
    reply_count = 0
    skipped_keyword = 0
    hydrated_replies = 0
    comment_errors = 0
    comment_refresh_reasons: Counter[str] = Counter()
    missing_marked = 0
    incomplete_comment_fetches = 0
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

    if stage == "comments":
        # Comment-only stage: read existing tweets from DB and hydrate replies
        existing_posts = _load_existing_posts("twitter", context, account, date_start, date_end)
        # Filter to non-reply tweets only (anchor posts)
        anchor_rows = [r for r in existing_posts if not r.get("is_reply")]
        logger.info("[twitter] Comments stage: %d anchor tweets from DB for account=%s", len(anchor_rows), account)
        retrieval_meta["source"] = "db"
        per_post_limit = max(0, opts.max_replies_per_post or opts.max_comments_per_post)
        snapshots = _load_comment_lifecycle_snapshots(
            [str(r["tweet_id"]) for r in anchor_rows if r.get("tweet_id")],
            platform="twitter",
        )
        skipped_synced = 0
        with pg.db_connection() as conn:
            if per_post_limit > 0:
                for row in anchor_rows[: opts.max_posts_per_target]:
                    tweet_id = str(row.get("tweet_id") or "")
                    if not tweet_id:
                        continue
                    expected = int(row.get("replies_count") or 0)
                    decision = _decide_comment_refresh(
                        sync_strategy=opts.sync_strategy,
                        expected_count=expected,
                        snapshot=snapshots.get(tweet_id),
                        post_published_at=_coerce_dt(row.get("created_at")),
                    )
                    comment_refresh_reasons[decision.reason] += 1
                    if not decision.should_refresh:
                        skipped_synced += 1
                        continue
                    post_count += 1
                    if post_count % 10 == 0:
                        _touch_job_heartbeat(job_id)
                    fetch_failed = False
                    replies: list[Any] = []
                    observed_reply_ids: set[str] = set()
                    try:
                        replies = scraper.fetch_tweet_replies(tweet_id, delay=0.5)[:per_post_limit]
                        for reply in replies:
                            if not getattr(reply, "reply_to_tweet_id", None):
                                reply.reply_to_tweet_id = tweet_id
                            reply.is_reply = True
                            reply_id = str(getattr(reply, "tweet_id", "") or "")
                            if reply_id:
                                observed_reply_ids.add(reply_id)
                            if _upsert_tweet(
                                context,
                                job_id=job_id,
                                run_id=run_id,
                                account=account,
                                tweet=reply,
                                conn=conn,
                            ):
                                hydrated_replies += 1
                                _report_progress()
                    except Exception:
                        fetch_failed = True
                        comment_errors += 1
                        logger.exception("[twitter] Failed to fetch replies for tweet %s", tweet_id)
                    is_complete = _is_comment_fetch_complete(
                        fetch_failed=fetch_failed,
                        fail_reason=None,
                        fetched_count=len(replies),
                        max_comments_per_post=per_post_limit,
                    )
                    if is_complete:
                        missing_marked += _mark_missing_comments_for_anchor(
                            platform="twitter",
                            anchor_id=tweet_id,
                            observed_comment_ids=observed_reply_ids,
                            conn=conn,
                        )
                    else:
                        incomplete_comment_fetches += 1
            reply_count += hydrated_replies
        if skipped_synced:
            logger.info("[twitter] Skipped %d tweets whose replies are already synced", skipped_synced)
    else:
        # Posts stage: scrape from live API
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
        tweets = scraper.scrape(config)[: opts.max_posts_per_target]
        retrieval_meta.update(dict(getattr(scraper, "last_retrieval_meta", {}) or {}))
        logger.info("[twitter] Scraper returned %d tweets", len(tweets))

        anchor_posts: list[Any] = []
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
                upserted = _upsert_tweet(context, job_id=job_id, run_id=run_id, account=account, tweet=tweet, conn=conn)
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
                        try:
                            replies = scraper.fetch_tweet_replies(tweet_id, delay=0.5)[:per_post_limit]
                            for reply in replies:
                                if not getattr(reply, "reply_to_tweet_id", None):
                                    reply.reply_to_tweet_id = tweet_id
                                reply.is_reply = True
                                if _upsert_tweet(
                                    context,
                                    job_id=job_id,
                                    run_id=run_id,
                                    account=account,
                                    tweet=reply,
                                    conn=conn,
                                ):
                                    hydrated_replies += 1
                                    _report_progress()
                        except Exception:
                            comment_errors += 1
                            logger.exception("[twitter] Failed to fetch replies for tweet %s", tweet_id)
                reply_count += hydrated_replies

    if comment_errors:
        retrieval_meta["comment_errors"] = comment_errors
    if comment_refresh_reasons:
        retrieval_meta["comment_refresh_decisions"] = dict(comment_refresh_reasons)
    if missing_marked:
        retrieval_meta["comments_marked_missing"] = missing_marked
    if incomplete_comment_fetches:
        retrieval_meta["incomplete_comment_fetches"] = incomplete_comment_fetches

    _report_progress(force=True)

    logger.info(
        "[twitter] Done: posts=%d replies=%d hydrated_replies=%d skipped_keyword=%d comment_errors=%d",
        post_count,
        reply_count,
        hydrated_replies,
        skipped_keyword,
        comment_errors,
    )
    retrieval_meta["hydrated_replies"] = hydrated_replies
    return post_count, reply_count, retrieval_meta


def _retry_backoff_seconds(attempt_count: int) -> int:
    return max(5, min(300, 5 * (2 ** max(0, attempt_count - 1))))


def _classify_job_error(exc: Exception) -> tuple[str, str, bool]:
    message = str(exc).lower()
    error_class = exc.__class__.__name__
    transient_markers = (
        "timeout",
        "temporar",
        "connection",
        "network",
        "429",
        "502",
        "503",
        "504",
        "rate limit",
        "json",
        "decode",
        "unexpected",
        "login_required",
    )
    if any(marker in message for marker in transient_markers):
        return "transient_error", error_class, True
    return "fatal_error", error_class, False


def _run_platform_stage(
    *,
    context: SeasonContext,
    run_id: str | None,
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
                run_id=run_id,
                account=account,
                hashtags=hashtags,
                keywords=keywords,
                opts=stage_opts,
                job_id=job_id,
                stage=stage,
            )
        if platform == "tiktok":
            return _ingest_tiktok(
                context,
                run_id=run_id,
                account=account,
                hashtags=hashtags,
                keywords=keywords,
                opts=stage_opts,
                job_id=job_id,
                stage=stage,
            )
        if platform == "youtube":
            return _ingest_youtube(
                context,
                run_id=run_id,
                account=account,
                hashtags=hashtags,
                keywords=keywords,
                opts=stage_opts,
                job_id=job_id,
                stage=stage,
            )
        if platform == "twitter":
            return _ingest_twitter(
                context,
                run_id=run_id,
                account=account,
                hashtags=hashtags,
                keywords=keywords,
                opts=stage_opts,
                job_id=job_id,
                include_reply_records=False,
                hydrate_audience_replies=False,
                stage=stage,
            )
    else:
        if opts.max_comments_per_post <= 0:
            return 0, 0, {}
        if platform == "instagram":
            _, comments, meta = _ingest_instagram(
                context,
                run_id=run_id,
                account=account,
                hashtags=hashtags,
                keywords=keywords,
                opts=opts,
                job_id=job_id,
                stage=stage,
            )
            return 0, comments, meta
        if platform == "tiktok":
            _, comments, meta = _ingest_tiktok(
                context,
                run_id=run_id,
                account=account,
                hashtags=hashtags,
                keywords=keywords,
                opts=opts,
                job_id=job_id,
                stage=stage,
            )
            return 0, comments, meta
        if platform == "youtube":
            _, comments, meta = _ingest_youtube(
                context,
                run_id=run_id,
                account=account,
                hashtags=hashtags,
                keywords=keywords,
                opts=opts,
                job_id=job_id,
                stage=stage,
            )
            return 0, comments, meta
        if platform == "twitter":
            _, comments, meta = _ingest_twitter(
                context,
                run_id=run_id,
                account=account,
                hashtags=hashtags,
                keywords=keywords,
                opts=opts,
                job_id=job_id,
                include_reply_records=False,
                hydrate_audience_replies=True,
                stage=stage,
            )
            return 0, comments, meta

    raise RuntimeError(f"Platform {platform} ingest is not supported")


def _claim_next_job(
    *,
    worker_id: str | None = None,
    run_id: str | None = None,
    stage: str | None = None,
) -> dict[str, Any] | None:
    return pg.fetch_one(
        """
        with candidate as (
          select id
          from social.scrape_jobs
          where status in ('queued', 'pending', 'retrying')
            and available_at <= now()
            and (%s::uuid is null or run_id = %s::uuid)
            and (
              %s::text is null
              or coalesce(config->>'stage', metadata->>'stage', job_type) = %s::text
            )
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
        [run_id, run_id, stage, stage, worker_id],
    )


def _execute_claimed_job(job: dict[str, Any], *, worker_id: str | None = None) -> dict[str, Any]:
    job_id = str(job.get("id") or "")
    run_id = str(job.get("run_id") or "")
    platform = str(job.get("platform") or "")
    config = dict(job.get("config") or {})
    stage = str(config.get("stage") or ((job.get("metadata") or {}).get("stage")) or "posts")
    context = get_season_context(str(config.get("season_id") or job.get("season_id") or ""))
    sync_strategy = str(config.get("sync_strategy") or "incremental").strip().lower()
    if sync_strategy not in SUPPORTED_SYNC_STRATEGIES:
        sync_strategy = "incremental"
    opts = IngestOptions(
        platforms=None,
        source_scope=str(config.get("source_scope") or job.get("source_scope") or "bravo"),
        sync_strategy=sync_strategy,
        max_posts_per_target=max(1, int(config.get("max_posts_per_target") or 1000)),
        max_comments_per_post=max(0, int(config.get("max_comments_per_post") or 0)),
        max_replies_per_post=max(0, int(config.get("max_replies_per_post") or 0)),
        fetch_replies=bool(config.get("fetch_replies", True)),
        ingest_mode=str(config.get("ingest_mode") or "posts_and_comments"),
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
            run_id=run_id or None,
            platform=platform,
            stage=stage,
            account=account,
            hashtags=hashtags,
            keywords=keywords,
            opts=opts,
            job_id=job_id,
        )
        run_is_cancelled = False
        if run_id:
            run_state = pg.fetch_one("select status from social.scrape_runs where id = %s", [run_id]) or {}
            run_is_cancelled = str(run_state.get("status") or "") == "cancelled"
        if run_is_cancelled:
            _finish_job(
                job_id,
                status="cancelled",
                items_found=posts_count + comments_count,
                metadata={
                    "stage": stage,
                    "stage_counters": {"posts": posts_count, "comments": comments_count},
                    "platform": platform,
                    "account": account,
                    "retrieval_meta": retrieval_meta,
                },
            )
            return (
                pg.fetch_one(
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
                )
                or {}
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
        run_is_cancelled = False
        if run_id:
            run_state = pg.fetch_one("select status from social.scrape_runs where id = %s", [run_id]) or {}
            run_is_cancelled = str(run_state.get("status") or "") == "cancelled"
        if run_is_cancelled:
            _finish_job(
                job_id,
                status="cancelled",
                items_found=0,
                error_message="Cancelled by user request",
                metadata={
                    "stage": stage,
                    "platform": platform,
                    "account": account,
                },
            )
            if run_id:
                _finalize_run_status(run_id)
            return (
                pg.fetch_one(
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
                )
                or {}
            )
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

    return (
        pg.fetch_one(
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
        )
        or {}
    )


def execute_run(
    run_id: str,
    *,
    worker_id: str | None = None,
    stage: str | None = None,
) -> dict[str, Any]:
    _set_run_status(run_id, "running")
    while True:
        job = _claim_next_job(worker_id=worker_id, run_id=run_id, stage=stage)
        if not job:
            break
        run_state = pg.fetch_one("select status from social.scrape_runs where id = %s", [run_id]) or {}
        if str(run_state.get("status")) == "cancelled":
            _finish_job(str(job.get("id")), status="cancelled", items_found=0, metadata={"stage": "cancelled"})
            continue
        _execute_claimed_job(job, worker_id=worker_id)

    _finalize_run_status(run_id)
    return (
        pg.fetch_one(
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
        )
        or {}
    )


def process_next_queued_job(*, worker_id: str, stage: str | None = None) -> dict[str, Any] | None:
    job = _claim_next_job(worker_id=worker_id, run_id=None, stage=stage)
    if not job:
        return None
    return _execute_claimed_job(job, worker_id=worker_id)


def ingest_season(
    season_id: str,
    *,
    platforms: list[str] | None,
    source_scope: str,
    sync_strategy: str = "incremental",
    max_posts_per_target: int,
    max_comments_per_post: int,
    max_replies_per_post: int = 100,
    fetch_replies: bool,
    ingest_mode: str = "posts_and_comments",
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
    normalized_sync_strategy = (sync_strategy or "incremental").strip().lower()
    if normalized_sync_strategy not in SUPPORTED_SYNC_STRATEGIES:
        raise ValueError(f"Unsupported sync strategy: {sync_strategy}")

    platform_filter = {p.strip().lower() for p in platforms or [] if isinstance(p, str) and p.strip()}
    if platform_filter:
        unsupported = platform_filter - set(SUPPORTED_PLATFORMS)
        if unsupported:
            raise ValueError(f"Unsupported platforms requested: {', '.join(sorted(unsupported))}")

    resolved_posts, resolved_comments, resolved_replies, resolved_fetch_replies = _resolve_depth_defaults(
        max_posts_per_target=max_posts_per_target,
        max_comments_per_post=max_comments_per_post,
        max_replies_per_post=max_replies_per_post,
        fetch_replies=fetch_replies,
    )
    if normalized_mode == "posts_only":
        resolved_comments = 0
        resolved_replies = 0
        resolved_fetch_replies = False
    elif normalized_mode == "comments_only":
        resolved_posts = 0

    stage_plan = ["posts"] if normalized_mode == "posts_only" else ["comments"] if normalized_mode == "comments_only" else ["posts", "comments"]

    opts = IngestOptions(
        platforms=platform_filter or None,
        source_scope=source_scope,
        sync_strategy=normalized_sync_strategy,
        max_posts_per_target=resolved_posts,
        max_comments_per_post=resolved_comments,
        max_replies_per_post=resolved_replies,
        fetch_replies=resolved_fetch_replies,
        ingest_mode=normalized_mode,
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
            "stages": stage_plan,
            "queued_or_started_jobs": 0,
            "message": "No active targets configured for selected platforms",
        }

    run_config = {
        "season_id": context.season_id,
        "show_id": context.show_id,
        "source_scope": source_scope,
        "platforms": sorted(platform_filter) if platform_filter else "all",
        "date_start": _iso(opts.date_start),
        "date_end": _iso(opts.date_end),
        "sync_strategy": opts.sync_strategy,
        "max_posts_per_target": opts.max_posts_per_target,
        "max_comments_per_post": opts.max_comments_per_post,
        "max_replies_per_post": opts.max_replies_per_post,
        "fetch_replies": opts.fetch_replies,
        "ingest_mode": opts.ingest_mode,
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
                "sync_strategy": opts.sync_strategy,
                "max_posts_per_target": opts.max_posts_per_target,
                "max_comments_per_post": opts.max_comments_per_post,
                "max_replies_per_post": opts.max_replies_per_post,
                "fetch_replies": opts.fetch_replies,
                "ingest_mode": opts.ingest_mode,
            }
            if normalized_mode != "comments_only":
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
            if normalized_mode in {"posts_and_comments", "comments_only"} and opts.max_comments_per_post > 0:
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
        "stages": stage_plan,
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


def list_runs(
    season_id: str,
    *,
    limit: int = 50,
    status: str | None = None,
    source_scope: str | None = None,
) -> list[dict[str, Any]]:
    safe_limit = max(1, min(int(limit), 250))
    sql = """
        select
          id::text,
          season_id::text as season_id,
          show_id::text as show_id,
          source_scope,
          status,
          config,
          summary,
          initiated_by,
          created_at,
          started_at,
          completed_at,
          cancelled_at
        from social.scrape_runs
        where season_id = %s
    """
    params: list[Any] = [season_id]
    if status:
        sql += " and status = %s"
        params.append(status)
    if source_scope:
        sql += " and source_scope = %s"
        params.append(source_scope)
    sql += " order by created_at desc limit %s"
    params.append(safe_limit)
    return pg.fetch_all(sql, params)


# ---------------------------------------------------------------------------
# Analytics + exports
# ---------------------------------------------------------------------------


def _default_sentiment_context() -> SentimentAnalyzerContext:
    return SentimentAnalyzerContext(cast_terms=set(), cast_phrases=set(), episode_terms=set(), episode_summary="")


def _normalize_sentiment_text_key(text: str | None) -> str:
    raw = (text or "").strip().lower()
    if not raw:
        return ""
    return re.sub(r"\s+", " ", raw)


def _tokenize_handle_terms(value: str | None) -> set[str]:
    if not value:
        return set()
    return {token.lower() for token in re.split(r"[^A-Za-z0-9]+", value) if token and len(token) >= 3}


def _build_cast_entity_terms(names: list[str]) -> tuple[set[str], set[str]]:
    cast_terms: set[str] = set()
    cast_phrases: set[str] = set()
    for raw_name in names:
        cleaned = str(raw_name or "").strip()
        if not cleaned:
            continue
        lower_phrase = cleaned.lower()
        cast_phrases.add(lower_phrase)
        for token in TOKEN_RE.findall(cleaned):
            lower = token.lower()
            if len(lower) >= 3:
                cast_terms.add(lower)
    return cast_terms, cast_phrases


def _load_cast_names_for_show(show_id: str) -> list[str]:
    try:
        rows = pg.fetch_all(
            """
            select distinct p.full_name
            from core.credits c
            join core.people p on p.id = c.person_id
            where c.show_id = %s
              and lower(coalesce(c.credit_category, '')) = 'self'
              and nullif(trim(coalesce(p.full_name, '')), '') is not null
            order by p.full_name asc
            """,
            [show_id],
        )
        names = [str(row.get("full_name") or "").strip() for row in rows]
        names = [name for name in names if name]
        if names:
            return names
    except Exception:  # noqa: BLE001
        logger.warning("Failed to load cast names from core.credits for show %s", show_id, exc_info=True)

    try:
        fallback_rows = pg.fetch_all(
            """
            select distinct cast_member_name
            from core.v_show_cast
            where show_id = %s
              and nullif(trim(coalesce(cast_member_name, '')), '') is not null
            order by cast_member_name asc
            """,
            [show_id],
        )
        return [
            str(row.get("cast_member_name") or "").strip()
            for row in fallback_rows
            if str(row.get("cast_member_name") or "").strip()
        ]
    except Exception:  # noqa: BLE001
        logger.warning("Failed to load cast names from core.v_show_cast for show %s", show_id, exc_info=True)
        return []


def _load_episode_sentiment_context(season_id: str) -> EpisodeSentimentContext:
    try:
        rows = pg.fetch_all(
            """
            select episode_number, title, coalesce(nullif(overview, ''), nullif(synopsis, ''), '') as summary
            from core.episodes
            where season_id = %s
            order by episode_number asc
            """,
            [season_id],
        )
    except Exception:  # noqa: BLE001
        logger.warning("Failed to load episode sentiment context for season %s", season_id, exc_info=True)
        return EpisodeSentimentContext(summary="", terms=set())

    terms: set[str] = set()
    snippets: list[str] = []
    for row in rows:
        title = str(row.get("title") or "").strip()
        summary = str(row.get("summary") or "").strip()
        episode_number = row.get("episode_number")
        for token in TOKEN_RE.findall(f"{title} {summary}"):
            lower = token.lower()
            if len(lower) >= 4 and lower not in STOPWORDS:
                terms.add(lower)
        if title or summary:
            short_summary = re.sub(r"\s+", " ", summary)
            if len(short_summary) > 90:
                short_summary = f"{short_summary[:87].rstrip()}..."
            episode_label = f"E{episode_number}" if isinstance(episode_number, int) else "Episode"
            if short_summary:
                snippets.append(f"{episode_label}: {title or 'Untitled'} — {short_summary}")
            else:
                snippets.append(f"{episode_label}: {title or 'Untitled'}")

    return EpisodeSentimentContext(summary="; ".join(snippets[:10]), terms=terms)


def _build_sentiment_context(context: SeasonContext) -> SentimentAnalyzerContext:
    cast_names = _load_cast_names_for_show(context.show_id)
    cast_terms, cast_phrases = _build_cast_entity_terms(cast_names)
    episode_context = _load_episode_sentiment_context(context.season_id)
    return SentimentAnalyzerContext(
        cast_terms=cast_terms,
        cast_phrases=cast_phrases,
        episode_terms=episode_context.terms,
        episode_summary=episode_context.summary,
    )


def _has_negation(tokens: list[str], index: int) -> bool:
    start = max(0, index - 3)
    return any(token in NEGATION_WORDS for token in tokens[start:index])


def _modifier_weight(tokens: list[str], index: int) -> float:
    if index <= 0:
        return 1.0
    weight = 1.0
    prev = tokens[index - 1]
    weight *= INTENSIFIER_WEIGHTS.get(prev, 1.0)
    weight *= DIMINISHER_WEIGHTS.get(prev, 1.0)
    if index >= 2 and tokens[index - 1] == "of":
        weight *= DIMINISHER_WEIGHTS.get(tokens[index - 2], 1.0)
    return weight


def _score_with_contrast(tokens: list[str], contributions: list[float]) -> float:
    total = sum(contributions)
    markers = [idx for idx, token in enumerate(tokens) if token in CONTRAST_MARKERS]
    if not markers:
        return total

    pivot = markers[-1]
    before = sum(contributions[:pivot])
    after = sum(contributions[pivot + 1 :])
    if before == 0 or after == 0:
        return total
    return (before * 0.6) + (after * 1.4)


def _rule_based_sentiment_for_text(
    text: str | None,
    *,
    analyzer_context: SentimentAnalyzerContext,
) -> SentimentRuleResult:
    raw_text = str(text or "").strip()
    if not raw_text:
        return SentimentRuleResult(label="neutral", score=0.0, confidence=1.0, ambiguous=False)

    lowered = raw_text.lower()
    tokens = [token.lower() for token in TOKEN_RE.findall(raw_text)]
    if not tokens:
        return SentimentRuleResult(label="neutral", score=0.0, confidence=0.95, ambiguous=False)

    mention_terms = {
        token.lower() for mention in MENTION_TOKEN_RE.findall(raw_text) for token in _tokenize_handle_terms(mention)
    }
    blocked_entity_terms = set(analyzer_context.cast_terms)
    blocked_entity_terms.update(mention_terms)

    contributions: list[float] = [0.0 for _ in tokens]
    sentiment_hits = 0
    for index, token in enumerate(tokens):
        if token in blocked_entity_terms:
            continue
        base = 0.0
        if token in POSITIVE_WORDS:
            base = 1.0
        elif token in NEGATIVE_WORDS:
            base = -1.0
        if base == 0.0:
            continue

        sentiment_hits += 1
        if _has_negation(tokens, index):
            base *= -1.0
        contributions[index] = base * _modifier_weight(tokens, index)

    if sentiment_hits == 0:
        has_context_only_signal = any(token in analyzer_context.cast_terms for token in tokens) or any(
            token in analyzer_context.episode_terms for token in tokens
        )
        confidence = 0.94 if has_context_only_signal else 0.85
        return SentimentRuleResult(label="neutral", score=0.0, confidence=confidence, ambiguous=False)

    score = _score_with_contrast(tokens, contributions)
    exclamation_count = lowered.count("!")
    if exclamation_count > 0:
        score *= min(1.35, 1.0 + (0.08 * exclamation_count))

    if score >= 0.55:
        label = "positive"
    elif score <= -0.55:
        label = "negative"
    else:
        label = "neutral"

    confidence = min(0.99, abs(score) / max(1.0, sentiment_hits * 0.9))
    if label == "neutral":
        confidence = min(confidence, 0.45)
    confidence = max(0.15, confidence)

    ambiguous = label == "neutral" or confidence < 0.5 or abs(score) < 0.9
    return SentimentRuleResult(label=label, score=score, confidence=confidence, ambiguous=ambiguous)


def _score_from_label(label: str) -> int:
    if label == "positive":
        return 1
    if label == "negative":
        return -1
    return 0


def _resolve_sentiment_gemini_model_selection() -> tuple[str, str, str | None]:
    custom = (os.getenv("SOCIAL_SENTIMENT_GEMINI_MODEL") or "").strip()
    if custom:
        return custom, "SOCIAL_SENTIMENT_GEMINI_MODEL", None

    pro = (os.getenv("GEMINI_MODEL_PRO") or "").strip()
    if pro:
        return pro, "GEMINI_MODEL_PRO", "SOCIAL_SENTIMENT_GEMINI_MODEL->GEMINI_MODEL_PRO"

    google_alias = (os.getenv("GOOGLE_GEMINI_MODEL") or "").strip()
    if google_alias:
        return (
            google_alias,
            "GOOGLE_GEMINI_MODEL",
            "SOCIAL_SENTIMENT_GEMINI_MODEL->GEMINI_MODEL_PRO->GOOGLE_GEMINI_MODEL",
        )

    canonical = (os.getenv("GEMINI_MODEL") or "").strip()
    if canonical:
        return (
            canonical,
            "GEMINI_MODEL",
            "SOCIAL_SENTIMENT_GEMINI_MODEL->GEMINI_MODEL_PRO->GOOGLE_GEMINI_MODEL->GEMINI_MODEL",
        )

    fast = (os.getenv("GEMINI_MODEL_FAST") or "").strip()
    if fast:
        return (
            fast,
            "GEMINI_MODEL_FAST",
            "SOCIAL_SENTIMENT_GEMINI_MODEL->GEMINI_MODEL_PRO->GOOGLE_GEMINI_MODEL->GEMINI_MODEL->GEMINI_MODEL_FAST",
        )

    return (
        DEFAULT_GEMINI_SENTIMENT_MODEL,
        "default",
        "SOCIAL_SENTIMENT_GEMINI_MODEL->GEMINI_MODEL_PRO->GOOGLE_GEMINI_MODEL->GEMINI_MODEL->GEMINI_MODEL_FAST->default",
    )


def _resolve_sentiment_gemini_model() -> str:
    model_name, _model_source, _fallback_path = _resolve_sentiment_gemini_model_selection()
    return model_name


def _resolve_sentiment_gemini_max_comments() -> int:
    raw = (os.getenv("SOCIAL_SENTIMENT_GEMINI_MAX_COMMENTS") or "").strip()
    if raw.isdigit():
        return max(1, min(int(raw), SENTIMENT_GEMINI_MAX_COMMENTS_CAP))
    return min(40, SENTIMENT_GEMINI_MAX_COMMENTS_CAP)


def _resolve_sentiment_gemini_batch_size() -> int:
    raw = (os.getenv("SOCIAL_SENTIMENT_GEMINI_BATCH_SIZE") or "").strip()
    if raw.isdigit():
        return max(1, min(int(raw), SENTIMENT_GEMINI_BATCH_SIZE_CAP))
    return min(10, SENTIMENT_GEMINI_BATCH_SIZE_CAP)


def _extract_gemini_text(response: Any) -> str:
    text = getattr(response, "text", None)
    if isinstance(text, str) and text.strip():
        return text
    candidates = getattr(response, "candidates", None) or []
    for candidate in candidates:
        content = getattr(candidate, "content", None)
        parts = getattr(content, "parts", None) or []
        for part in parts:
            candidate_text = getattr(part, "text", None)
            if isinstance(candidate_text, str) and candidate_text.strip():
                return candidate_text
    return ""


def _build_gemini_text_generator(*, api_key: str, model_name: str):
    try:
        from google import genai as google_genai  # type: ignore

        client = google_genai.Client(api_key=api_key)

        def _generate(prompt: str) -> Any:
            return client.models.generate_content(model=model_name, contents=prompt)

        return _generate, "google-genai"
    except Exception:  # noqa: BLE001
        return None, None


def _parse_gemini_sentiment_payload(raw: str) -> dict[int, tuple[str, float]]:
    if not raw:
        return {}
    candidate = raw.strip()
    candidate = re.sub(r"^```(?:json)?\s*", "", candidate, flags=re.IGNORECASE)
    candidate = re.sub(r"\s*```$", "", candidate)

    payload: Any
    try:
        payload = json.loads(candidate)
    except json.JSONDecodeError:
        match = re.search(r"(\[[\s\S]*\]|\{[\s\S]*\})", candidate)
        if not match:
            return {}
        try:
            payload = json.loads(match.group(1))
        except json.JSONDecodeError:
            return {}

    items: list[dict[str, Any]]
    if isinstance(payload, list):
        items = [item for item in payload if isinstance(item, dict)]
    elif isinstance(payload, dict):
        raw_items = payload.get("items")
        if isinstance(raw_items, list):
            items = [item for item in raw_items if isinstance(item, dict)]
        else:
            return {}
    else:
        return {}

    parsed: dict[int, tuple[str, float]] = {}
    for item in items:
        index_raw = item.get("index")
        sentiment_raw = str(item.get("sentiment") or "").strip().lower()
        confidence_raw = item.get("confidence")
        if sentiment_raw not in {"positive", "neutral", "negative"}:
            continue
        if not isinstance(index_raw, int):
            continue
        confidence = 0.5
        if isinstance(confidence_raw, (int, float)):
            confidence = max(0.0, min(float(confidence_raw), 1.0))
        parsed[index_raw] = (sentiment_raw, confidence)
    return parsed


def _classify_ambiguous_sentiments_with_gemini(
    entries: list[tuple[str, str]],
    *,
    context: SeasonContext,
    analyzer_context: SentimentAnalyzerContext,
) -> dict[str, tuple[str, float]]:
    if not entries:
        return {}
    if not _env_truthy("SOCIAL_SENTIMENT_GEMINI_ENABLED", default=False):
        return {}

    api_key = (os.getenv("GEMINI_API_KEY") or "").strip() or (os.getenv("GOOGLE_GEMINI_API_KEY") or "").strip()
    if not api_key:
        logger.info("Gemini sentiment disambiguation enabled but API key is missing; falling back to rules")
        return {}

    model_name, model_source, fallback_path = _resolve_sentiment_gemini_model_selection()
    batch_size = _resolve_sentiment_gemini_batch_size()
    max_comments = _resolve_sentiment_gemini_max_comments()
    limited_entries = entries[:max_comments]

    generate_content, sdk_name = _build_gemini_text_generator(api_key=api_key, model_name=model_name)
    if generate_content is None:
        logger.warning("Gemini sentiment disambiguation unavailable: no supported SDK import found")
        return {}

    logger.info(
        "Gemini sentiment route=pro model=%s sdk=%s source=%s fallback_path=%s",
        model_name,
        sdk_name,
        model_source,
        fallback_path or "none",
    )

    cast_preview = ", ".join(sorted(analyzer_context.cast_phrases)[:20]) or "None"
    episode_summary = analyzer_context.episode_summary or "Unavailable"
    overrides: dict[str, tuple[str, float]] = {}

    for offset in range(0, len(limited_entries), batch_size):
        batch = limited_entries[offset : offset + batch_size]
        batch_comments = [{"index": index, "text": text} for index, (_, text) in enumerate(batch)]
        prompt = (
            "Classify each social comment sentiment for a Bravo reality TV season.\n"
            f"Show: {context.show_name or context.show_id}\n"
            f"Season: {context.season_number}\n"
            f"Episode context: {episode_summary}\n"
            f"Cast/entity names (treat as entities, not inherently sentiment): {cast_preview}\n"
            "Rules: label each comment as positive, neutral, or negative based on intent/context.\n"
            "Return only valid JSON as an array of objects: "
            '[{"index":0,"sentiment":"positive|neutral|negative","confidence":0.0}].\n'
            f"Comments: {json.dumps(batch_comments, ensure_ascii=True)}"
        )

        try:
            response = generate_content(prompt)
            parsed = _parse_gemini_sentiment_payload(_extract_gemini_text(response))
        except Exception:  # noqa: BLE001
            logger.warning("Gemini sentiment request failed; falling back to rule-based sentiment", exc_info=True)
            continue

        for index, (sentiment, confidence) in parsed.items():
            if index < 0 or index >= len(batch):
                continue
            key = batch[index][0]
            overrides[key] = (sentiment, confidence)

    return overrides


def _apply_optional_gemini_sentiment(
    rows: list[dict[str, Any]],
    *,
    context: SeasonContext,
    analyzer_context: SentimentAnalyzerContext,
) -> None:
    if not rows or not _env_truthy("SOCIAL_SENTIMENT_GEMINI_ENABLED", default=False):
        return

    deduped: dict[str, str] = {}
    for row in rows:
        if row.get("kind") != "comment":
            continue
        if not bool(row.get("_sentiment_ambiguous")):
            continue
        text_key = str(row.get("_sentiment_key") or "")
        if not text_key or text_key in deduped:
            continue
        deduped[text_key] = str(row.get("text") or "")

    if not deduped:
        return

    entries = list(deduped.items())
    overrides = _classify_ambiguous_sentiments_with_gemini(entries, context=context, analyzer_context=analyzer_context)
    if not overrides:
        return

    for row in rows:
        if row.get("kind") != "comment":
            continue
        text_key = str(row.get("_sentiment_key") or "")
        override = overrides.get(text_key)
        if not override:
            continue
        sentiment, confidence = override
        row["sentiment"] = sentiment
        row["sentiment_score"] = _score_from_label(sentiment)
        row["_sentiment_confidence"] = confidence
        row["_sentiment_ambiguous"] = False


def sentiment_for_text(text: str | None) -> tuple[str, int]:
    result = _rule_based_sentiment_for_text(text, analyzer_context=_default_sentiment_context())
    if result.label == "neutral":
        return "neutral", 0
    rounded = int(round(result.score))
    if rounded == 0:
        rounded = _score_from_label(result.label)
    return result.label, rounded


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
        marker_hit = _text_is_trailer_marker(f"{video.get('title') or ''} {video.get('kicker') or ''}")
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
    target_accounts_by_platform: dict[str, set[str]] | None = None,
) -> list[dict[str, Any]]:
    account_map = (
        target_accounts_by_platform
        if target_accounts_by_platform is not None
        else _target_accounts_by_platform(season_id, source_scope=source_scope)
    )
    platform_accounts = set(account_map.get(platform, set()))
    requires_target_accounts = source_scope in {"bravo", "creator"}
    if requires_target_accounts and not platform_accounts:
        return []
    apply_account_filter = source_scope != "community" and bool(platform_accounts)
    platform_accounts_list = sorted(platform_accounts)

    if platform == "instagram":
        account_filter_posts = (
            "and lower(coalesce(nullif(p.username, ''), p.source_account, '')) = any(%s)"
            if apply_account_filter
            else ""
        )
        account_filter_comments = (
            "and lower(coalesce(nullif(p.username, ''), p.source_account, '')) = any(%s)"
            if apply_account_filter
            else ""
        )
        params: list[Any] = [season_id, start_dt, end_dt]
        if apply_account_filter:
            params.append(platform_accounts_list)
        params.extend([season_id, start_dt, end_dt])
        if apply_account_filter:
            params.append(platform_accounts_list)
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
            params,
        )

    if platform == "tiktok":
        account_filter_posts = (
            "and lower(coalesce(nullif(p.username, ''), p.source_account, '')) = any(%s)"
            if apply_account_filter
            else ""
        )
        account_filter_comments = (
            "and lower(coalesce(nullif(p.username, ''), p.source_account, '')) = any(%s)"
            if apply_account_filter
            else ""
        )
        params = [season_id, start_dt, end_dt]
        if apply_account_filter:
            params.append(platform_accounts_list)
        params.extend([season_id, start_dt, end_dt])
        if apply_account_filter:
            params.append(platform_accounts_list)
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
            params,
        )

    if platform == "youtube":
        account_filter_videos = (
            "and lower(coalesce(nullif(v.channel_title, ''), v.source_account, '')) = any(%s)"
            if apply_account_filter
            else ""
        )
        account_filter_comments = (
            "and lower(coalesce(nullif(v.channel_title, ''), v.source_account, '')) = any(%s)"
            if apply_account_filter
            else ""
        )
        params = [season_id, start_dt, end_dt]
        if apply_account_filter:
            params.append(platform_accounts_list)
        params.extend([season_id, start_dt, end_dt])
        if apply_account_filter:
            params.append(platform_accounts_list)
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
            params,
        )

    if platform == "twitter":
        if apply_account_filter:
            return pg.fetch_all(
                """
                with recursive in_scope_posts as (
                  select p.tweet_id
                  from social.twitter_tweets p
                  where p.season_id = %s
                    and p.is_reply = false
                    and p.created_at >= %s
                    and p.created_at <= %s
                    and lower(coalesce(nullif(p.username, ''), p.source_account, '')) = any(%s)
                ), legacy_thread_replies as (
                  select r.tweet_id
                  from social.twitter_tweets r
                  where r.season_id = %s
                    and r.is_reply = true
                    and (r.source_account is null or btrim(r.source_account) = '')
                    and r.reply_to_tweet_id in (select tweet_id from in_scope_posts)
                  union
                  select child.tweet_id
                  from social.twitter_tweets child
                  join legacy_thread_replies parent on child.reply_to_tweet_id = parent.tweet_id
                  where child.season_id = %s
                    and child.is_reply = true
                    and (child.source_account is null or btrim(child.source_account) = '')
                ), posts as (
                  select
                    'twitter'::text as platform,
                    'post'::text as kind,
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
                    (
                      'https://x.com/'
                      || coalesce(nullif(t.username, ''), nullif(t.source_account, ''), 'i')
                      || '/status/'
                      || t.tweet_id
                    )::text as url,
                    coalesce(nullif(t.username, ''), nullif(t.source_account, ''), '') as author
                  from social.twitter_tweets t
                  where t.season_id = %s
                    and t.created_at >= %s
                    and t.created_at <= %s
                    and t.is_reply = false
                    and lower(coalesce(nullif(t.username, ''), t.source_account, '')) = any(%s)
                ), comments as (
                  select
                    'twitter'::text as platform,
                    'comment'::text as kind,
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
                    (
                      'https://x.com/'
                      || coalesce(nullif(t.username, ''), nullif(t.source_account, ''), 'i')
                      || '/status/'
                      || t.tweet_id
                    )::text as url,
                    coalesce(nullif(t.username, ''), nullif(t.source_account, ''), '') as author
                  from social.twitter_tweets t
                  where t.season_id = %s
                    and t.created_at >= %s
                    and t.created_at <= %s
                    and t.is_reply = true
                    and (
                      lower(coalesce(nullif(t.source_account, ''), nullif(t.username, ''), '')) = any(%s)
                      or t.tweet_id in (select tweet_id from legacy_thread_replies)
                    )
                )
                select * from posts
                union all
                select * from comments
                """,
                [
                    season_id,
                    start_dt,
                    end_dt,
                    platform_accounts_list,
                    season_id,
                    season_id,
                    season_id,
                    start_dt,
                    end_dt,
                    platform_accounts_list,
                    season_id,
                    start_dt,
                    end_dt,
                    platform_accounts_list,
                ],
            )

        return pg.fetch_all(
            """
            with posts as (
              select
                'twitter'::text as platform,
                'post'::text as kind,
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
                (
                  'https://x.com/'
                  || coalesce(nullif(t.username, ''), nullif(t.source_account, ''), 'i')
                  || '/status/'
                  || t.tweet_id
                )::text as url,
                coalesce(nullif(t.username, ''), nullif(t.source_account, ''), '') as author
              from social.twitter_tweets t
              where t.season_id = %s
                and t.created_at >= %s
                and t.created_at <= %s
                and t.is_reply = false
            ), comments as (
              select
                'twitter'::text as platform,
                'comment'::text as kind,
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
                (
                  'https://x.com/'
                  || coalesce(nullif(t.username, ''), nullif(t.source_account, ''), 'i')
                  || '/status/'
                  || t.tweet_id
                )::text as url,
                coalesce(nullif(t.username, ''), nullif(t.source_account, ''), '') as author
              from social.twitter_tweets t
              where t.season_id = %s
                and t.created_at >= %s
                and t.created_at <= %s
                and t.is_reply = true
            )
            select * from posts
            union all
            select * from comments
            """,
            [season_id, start_dt, end_dt, season_id, start_dt, end_dt],
        )

    return []


def _build_rows(
    season_id: str,
    *,
    platforms: list[str],
    start_dt: datetime,
    end_dt: datetime,
    source_scope: str,
    season_context: SeasonContext,
    analyzer_context: SentimentAnalyzerContext,
    target_accounts_by_platform: dict[str, set[str]],
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
                target_accounts_by_platform=target_accounts_by_platform,
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
        if kind == "comment":
            rule_result = _rule_based_sentiment_for_text(text, analyzer_context=analyzer_context)
            sentiment = rule_result.label
            if sentiment == "neutral":
                score = 0
            else:
                rounded = int(round(rule_result.score))
                score = rounded if rounded != 0 else _score_from_label(sentiment)
            confidence = rule_result.confidence
            ambiguous = rule_result.ambiguous
            sentiment_key = _normalize_sentiment_text_key(text)
        else:
            sentiment = "neutral"
            score = 0
            confidence = 1.0
            ambiguous = False
            sentiment_key = ""
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
                "_sentiment_confidence": confidence,
                "_sentiment_ambiguous": ambiguous,
                "_sentiment_key": sentiment_key,
            }
        )

    _apply_optional_gemini_sentiment(normalized, context=season_context, analyzer_context=analyzer_context)
    normalized.sort(key=lambda item: item["ts"], reverse=True)
    return normalized


def _build_drivers(
    rows: list[dict[str, Any]],
    *,
    analyzer_context: SentimentAnalyzerContext | None = None,
) -> dict[str, list[dict[str, Any]]]:
    effective_context = analyzer_context or _default_sentiment_context()
    token_counts: Counter[str] = Counter()
    token_scores: defaultdict[str, int] = defaultdict(int)
    blocked_terms = set(STOPWORDS)
    blocked_terms.update(effective_context.cast_terms)

    account_terms: set[str] = set()
    for row in rows:
        account_terms.update(_tokenize_handle_terms(str(row.get("author") or "")))
        for mention in MENTION_TOKEN_RE.findall(str(row.get("text") or "")):
            account_terms.update(_tokenize_handle_terms(mention))
    blocked_terms.update(account_terms)

    for row in rows:
        if row["kind"] != "comment":
            continue
        text = row.get("text") or ""
        sentiment = str(row.get("sentiment") or "neutral").lower()
        sentiment_score = _score_from_label(sentiment)
        if sentiment_score == 0:
            continue
        row_mentions = {
            token.lower()
            for mention in MENTION_TOKEN_RE.findall(str(text))
            for token in _tokenize_handle_terms(mention)
        }
        tokens = [token.lower() for token in TOKEN_RE.findall(text)]
        for token in tokens:
            if len(token) < 4 or token in blocked_terms or token in row_mentions:
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

    sentiment_context = _build_sentiment_context(context)
    target_accounts_by_platform = _target_accounts_by_platform(season_id, source_scope=source_scope)
    rows = _build_rows(
        season_id,
        platforms=available_platforms,
        start_dt=start_dt,
        end_dt=end_dt,
        source_scope=source_scope,
        season_context=context,
        analyzer_context=sentiment_context,
        target_accounts_by_platform=target_accounts_by_platform,
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
        item.week_index: dict.fromkeys(SUPPORTED_PLATFORMS, 0) for item in visible_windows
    }
    weekly_platform_comments_map: dict[int, dict[str, int]] = {
        item.week_index: dict.fromkeys(SUPPORTED_PLATFORMS, 0) for item in visible_windows
    }
    weekly_platform_engagement_map: dict[int, dict[str, int]] = {
        item.week_index: dict.fromkeys(SUPPORTED_PLATFORMS, 0) for item in visible_windows
    }
    for row in rows:
        week_window = _week_for_timestamp(row["ts"], windows=visible_windows, timezone=timezone)
        if not week_window:
            continue
        platform = row["platform"]
        if platform in weekly_platform_engagement_map[week_window.week_index]:
            weekly_platform_engagement_map[week_window.week_index][platform] += int(row["engagement"] or 0)
        if row["kind"] == "post" and platform in weekly_platform_posts_map[week_window.week_index]:
            weekly_platform_posts_map[week_window.week_index][platform] += 1
        elif row["kind"] == "comment" and platform in weekly_platform_comments_map[week_window.week_index]:
            weekly_platform_comments_map[week_window.week_index][platform] += 1

    weekly_platform_posts: list[dict[str, Any]] = []
    for week_entry in weekly:
        week_index = int(week_entry["week_index"])
        platform_posts = weekly_platform_posts_map.get(
            week_index,
            dict.fromkeys(SUPPORTED_PLATFORMS, 0),
        )
        platform_comments = weekly_platform_comments_map.get(
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
                "comments": {
                    "instagram": int(platform_comments.get("instagram", 0)),
                    "youtube": int(platform_comments.get("youtube", 0)),
                    "tiktok": int(platform_comments.get("tiktok", 0)),
                    "twitter": int(platform_comments.get("twitter", 0)),
                },
                "total_posts": int(sum(platform_posts.values())),
                "total_comments": int(sum(platform_comments.values())),
            }
        )

    weekly_platform_engagement: list[dict[str, Any]] = []
    for week_entry in weekly:
        week_index = int(week_entry["week_index"])
        platform_engagement = weekly_platform_engagement_map.get(
            week_index,
            dict.fromkeys(SUPPORTED_PLATFORMS, 0),
        )
        engagement_payload = {
            "instagram": int(platform_engagement.get("instagram", 0)),
            "youtube": int(platform_engagement.get("youtube", 0)),
            "tiktok": int(platform_engagement.get("tiktok", 0)),
            "twitter": int(platform_engagement.get("twitter", 0)),
        }
        total_engagement = int(sum(engagement_payload.values()))
        weekly_platform_engagement.append(
            {
                "week_index": week_index,
                "label": week_entry["label"],
                "start": week_entry["start"],
                "end": week_entry["end"],
                "engagement": engagement_payload,
                "total_engagement": total_engagement,
                "has_data": total_engagement > 0,
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
        "weekly_platform_engagement": weekly_platform_engagement,
        "platform_breakdown": platform_breakdown,
        "themes": _build_drivers(comments, analyzer_context=sentiment_context),
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


# ---------------------------------------------------------------------------
# Week detail (drill-down into individual posts + comments for one week)
# ---------------------------------------------------------------------------

_MENTION_RE = re.compile(r"@([\w.]+)")


def _parse_mentions(text: str | None) -> list[str]:
    """Extract @mentions from text."""
    if not text:
        return []
    return [f"@{m}" for m in _MENTION_RE.findall(text)]


def _week_detail_instagram(
    season_id: str,
    *,
    start_dt: datetime,
    end_dt: datetime,
    account_handles: set[str],
    max_comments: int,
) -> dict[str, Any]:
    account_handles_list = sorted(account_handles)
    account_filter = (
        "and lower(coalesce(nullif(p.username, ''), p.source_account, '')) = any(%s)" if account_handles_list else ""
    )
    params = [season_id, start_dt, end_dt]
    if account_handles_list:
        params.append(account_handles_list)
    posts = pg.fetch_all(
        f"""
        select
          p.id,
          p.shortcode as source_id,
          p.username as author,
          p.caption as text,
          coalesce(p.likes, 0) as likes,
          coalesce(p.comments_count, 0) as comments_count,
          coalesce(p.views, 0) as views,
          p.media_type,
          p.media_urls,
          p.thumbnail_url,
          p.posted_at as ts
        from social.instagram_posts p
        where p.season_id = %s
          and p.posted_at >= %s
          and p.posted_at <= %s
          {account_filter}
        order by (coalesce(p.likes, 0) + coalesce(p.comments_count, 0) + coalesce(p.views, 0)) desc
        """,
        params,
    )

    post_ids = [p["id"] for p in posts]
    comments_by_post: dict[Any, list[dict]] = defaultdict(list)
    comment_counts_by_post: dict[Any, int] = defaultdict(int)

    if post_ids:
        # Get total comment counts per post
        count_rows = pg.fetch_all(
            """
            select c.post_id, count(*)::int as cnt
            from social.instagram_comments c
            where c.post_id = any(%s::uuid[])
            group by c.post_id
            """,
            [post_ids],
        )
        for row in count_rows:
            comment_counts_by_post[row["post_id"]] = row["cnt"]

        # Get top N comments per post using lateral join (0 = no cap)
        limit_clause = f"limit {max_comments}" if max_comments > 0 else "limit all"
        comment_rows = pg.fetch_all(
            f"""
            select sub.*
            from unnest(%s::uuid[]) as pid(id)
            cross join lateral (
              select
                c.comment_id,
                c.post_id,
                c.username as author,
                c.text,
                coalesce(c.likes, 0) as likes,
                coalesce(c.is_reply, false) as is_reply,
                coalesce(c.reply_count, 0) as reply_count,
                c.created_at
              from social.instagram_comments c
              where c.post_id = pid.id
              order by c.likes desc nulls last, c.created_at asc
              {limit_clause}
            ) sub
            """,
            [post_ids],
        )
        for row in comment_rows:
            comments_by_post[row["post_id"]].append(row)

    result_posts = []
    total_engagement = 0
    total_comments_count = 0
    for p in posts:
        engagement = p["likes"] + p["comments_count"] + p["views"]
        total_engagement += engagement
        mentions = _parse_mentions(p.get("text"))
        db_comment_count = comment_counts_by_post.get(p["id"], 0)
        total_comments_count += p["comments_count"]
        post_comments = comments_by_post.get(p["id"], [])

        media_urls = p.get("media_urls")
        if isinstance(media_urls, str):
            try:
                media_urls = json.loads(media_urls)
            except (json.JSONDecodeError, TypeError):
                media_urls = None

        result_posts.append(
            {
                "source_id": p["source_id"],
                "author": p["author"] or "",
                "text": p.get("text") or "",
                "url": f"https://www.instagram.com/p/{p['source_id']}/" if p["source_id"] else "",
                "posted_at": _iso(p["ts"]),
                "likes": p["likes"],
                "comments_count": p["comments_count"],
                "views": p["views"],
                "media_type": p.get("media_type"),
                "media_urls": media_urls,
                "thumbnail_url": p.get("thumbnail_url"),
                "mentions": mentions,
                "engagement": engagement,
                "total_comments_available": db_comment_count,
                "comments": [
                    {
                        "comment_id": c["comment_id"],
                        "author": c["author"] or "",
                        "text": c["text"] or "",
                        "likes": c["likes"],
                        "is_reply": c["is_reply"],
                        "reply_count": c["reply_count"],
                        "created_at": _iso(c["created_at"]),
                    }
                    for c in post_comments
                ],
            }
        )

    return {
        "posts": result_posts,
        "totals": {
            "posts": len(result_posts),
            "total_comments": total_comments_count,
            "total_engagement": total_engagement,
        },
    }


def _week_detail_tiktok(
    season_id: str,
    *,
    start_dt: datetime,
    end_dt: datetime,
    account_handles: set[str],
    max_comments: int,
) -> dict[str, Any]:
    account_handles_list = sorted(account_handles)
    account_filter = (
        "and lower(coalesce(nullif(p.username, ''), p.source_account, '')) = any(%s)" if account_handles_list else ""
    )
    params = [season_id, start_dt, end_dt]
    if account_handles_list:
        params.append(account_handles_list)
    posts = pg.fetch_all(
        f"""
        select
          p.id,
          p.video_id as source_id,
          p.username as author,
          p.nickname,
          p.description as text,
          coalesce(p.likes, 0) as likes,
          coalesce(p.comments_count, 0) as comments_count,
          coalesce(p.shares, 0) as shares,
          coalesce(p.views, 0) as views,
          p.hashtags,
          p.thumbnail_url,
          p.duration_seconds,
          p.posted_at as ts
        from social.tiktok_posts p
        where p.season_id = %s
          and p.posted_at >= %s
          and p.posted_at <= %s
          {account_filter}
        order by (
          coalesce(p.likes, 0) + coalesce(p.comments_count, 0)
          + coalesce(p.shares, 0) + coalesce(p.views, 0)
        ) desc
        """,
        params,
    )

    post_ids = [p["id"] for p in posts]
    comments_by_post: dict[Any, list[dict]] = defaultdict(list)
    comment_counts_by_post: dict[Any, int] = defaultdict(int)

    if post_ids:
        count_rows = pg.fetch_all(
            """
            select c.post_id, count(*)::int as cnt
            from social.tiktok_comments c
            where c.post_id = any(%s::uuid[])
            group by c.post_id
            """,
            [post_ids],
        )
        for row in count_rows:
            comment_counts_by_post[row["post_id"]] = row["cnt"]

        limit_clause = f"limit {max_comments}" if max_comments > 0 else "limit all"
        comment_rows = pg.fetch_all(
            f"""
            select sub.*
            from unnest(%s::uuid[]) as pid(id)
            cross join lateral (
              select
                c.comment_id,
                c.post_id,
                c.username as author,
                c.text,
                coalesce(c.likes, 0) as likes,
                coalesce(c.is_reply, false) as is_reply,
                coalesce(c.reply_count, 0) as reply_count,
                c.created_at
              from social.tiktok_comments c
              where c.post_id = pid.id
              order by c.likes desc nulls last, c.created_at asc
              {limit_clause}
            ) sub
            """,
            [post_ids],
        )
        for row in comment_rows:
            comments_by_post[row["post_id"]].append(row)

    result_posts = []
    total_engagement = 0
    total_comments_count = 0
    for p in posts:
        engagement = p["likes"] + p["comments_count"] + p["shares"] + p["views"]
        total_engagement += engagement
        total_comments_count += p["comments_count"]
        mentions = _parse_mentions(p.get("text"))
        db_comment_count = comment_counts_by_post.get(p["id"], 0)
        post_comments = comments_by_post.get(p["id"], [])

        hashtags = p.get("hashtags")
        if isinstance(hashtags, str):
            try:
                hashtags = json.loads(hashtags)
            except (json.JSONDecodeError, TypeError):
                hashtags = None

        author = p["author"] or ""
        result_posts.append(
            {
                "source_id": p["source_id"],
                "author": author,
                "nickname": p.get("nickname") or "",
                "text": p.get("text") or "",
                "url": f"https://www.tiktok.com/@{author}/video/{p['source_id']}" if p["source_id"] and author else "",
                "posted_at": _iso(p["ts"]),
                "likes": p["likes"],
                "comments_count": p["comments_count"],
                "shares": p["shares"],
                "views": p["views"],
                "hashtags": hashtags or [],
                "thumbnail_url": p.get("thumbnail_url"),
                "duration_seconds": p.get("duration_seconds"),
                "mentions": mentions,
                "engagement": engagement,
                "total_comments_available": db_comment_count,
                "comments": [
                    {
                        "comment_id": c["comment_id"],
                        "author": c["author"] or "",
                        "text": c["text"] or "",
                        "likes": c["likes"],
                        "is_reply": c["is_reply"],
                        "reply_count": c["reply_count"],
                        "created_at": _iso(c["created_at"]),
                    }
                    for c in post_comments
                ],
            }
        )

    return {
        "posts": result_posts,
        "totals": {
            "posts": len(result_posts),
            "total_comments": total_comments_count,
            "total_engagement": total_engagement,
        },
    }


def _week_detail_youtube(
    season_id: str,
    *,
    start_dt: datetime,
    end_dt: datetime,
    account_handles: set[str],
    max_comments: int,
) -> dict[str, Any]:
    account_handles_list = sorted(account_handles)
    account_filter = (
        "and lower(coalesce(nullif(v.channel_title, ''), v.source_account, '')) = any(%s)"
        if account_handles_list
        else ""
    )
    params = [season_id, start_dt, end_dt]
    if account_handles_list:
        params.append(account_handles_list)
    posts = pg.fetch_all(
        f"""
        select
          v.id,
          v.video_id as source_id,
          v.channel_title as author,
          v.title,
          v.description as text,
          coalesce(v.views, 0) as views,
          coalesce(v.likes, 0) as likes,
          coalesce(v.comments_count, 0) as comments_count,
          v.thumbnail_url,
          v.duration_seconds,
          v.published_at as ts
        from social.youtube_videos v
        where v.season_id = %s
          and v.published_at >= %s
          and v.published_at <= %s
          {account_filter}
        order by (coalesce(v.views, 0) + coalesce(v.likes, 0) + coalesce(v.comments_count, 0)) desc
        """,
        params,
    )

    post_ids = [p["id"] for p in posts]
    comments_by_post: dict[Any, list[dict]] = defaultdict(list)
    comment_counts_by_post: dict[Any, int] = defaultdict(int)

    if post_ids:
        count_rows = pg.fetch_all(
            """
            select c.video_id, count(*)::int as cnt
            from social.youtube_comments c
            where c.video_id = any(%s::uuid[])
            group by c.video_id
            """,
            [post_ids],
        )
        for row in count_rows:
            comment_counts_by_post[row["video_id"]] = row["cnt"]

        limit_clause = f"limit {max_comments}" if max_comments > 0 else "limit all"
        comment_rows = pg.fetch_all(
            f"""
            select sub.*
            from unnest(%s::uuid[]) as pid(id)
            cross join lateral (
              select
                c.comment_id,
                c.video_id,
                c.author,
                c.text,
                coalesce(c.likes, 0) as likes,
                coalesce(c.is_reply, false) as is_reply,
                coalesce(c.reply_count, 0) as reply_count,
                c.created_at
              from social.youtube_comments c
              where c.video_id = pid.id
              order by c.likes desc nulls last, c.created_at asc
              {limit_clause}
            ) sub
            """,
            [post_ids],
        )
        for row in comment_rows:
            comments_by_post[row["video_id"]].append(row)

    result_posts = []
    total_engagement = 0
    total_comments_count = 0
    for p in posts:
        engagement = p["views"] + p["likes"] + p["comments_count"]
        total_engagement += engagement
        total_comments_count += p["comments_count"]
        db_comment_count = comment_counts_by_post.get(p["id"], 0)
        post_comments = comments_by_post.get(p["id"], [])

        result_posts.append(
            {
                "source_id": p["source_id"],
                "author": p["author"] or "",
                "title": p.get("title") or "",
                "text": p.get("text") or "",
                "url": f"https://www.youtube.com/watch?v={p['source_id']}" if p["source_id"] else "",
                "posted_at": _iso(p["ts"]),
                "views": p["views"],
                "likes": p["likes"],
                "comments_count": p["comments_count"],
                "thumbnail_url": p.get("thumbnail_url"),
                "duration_seconds": p.get("duration_seconds"),
                "engagement": engagement,
                "total_comments_available": db_comment_count,
                "comments": [
                    {
                        "comment_id": c["comment_id"],
                        "author": c["author"] or "",
                        "text": c["text"] or "",
                        "likes": c["likes"],
                        "is_reply": c["is_reply"],
                        "reply_count": c["reply_count"],
                        "created_at": _iso(c["created_at"]),
                    }
                    for c in post_comments
                ],
            }
        )

    return {
        "posts": result_posts,
        "totals": {
            "posts": len(result_posts),
            "total_comments": total_comments_count,
            "total_engagement": total_engagement,
        },
    }


def _week_detail_twitter(
    season_id: str,
    *,
    start_dt: datetime,
    end_dt: datetime,
    account_handles: set[str],
    max_comments: int,
) -> dict[str, Any]:
    """Twitter week detail with optional recursive reply chains for scoped accounts."""

    account_handles_list = sorted(account_handles)
    account_filter = (
        "and lower(coalesce(nullif(t.username, ''), t.source_account, '')) = any(%s)" if account_handles_list else ""
    )
    posts_params = [season_id, start_dt, end_dt]
    if account_handles_list:
        posts_params.append(account_handles_list)

    posts = pg.fetch_all(
        f"""
        select
          t.tweet_id as source_id,
          coalesce(nullif(t.username, ''), nullif(t.source_account, ''), '') as author,
          t.display_name,
          t.text,
          coalesce(t.likes, 0) as likes,
          coalesce(t.retweets, 0) as retweets,
          coalesce(t.replies_count, 0) as replies_count,
          coalesce(t.quotes, 0) as quotes,
          coalesce(t.views, 0) as views,
          t.hashtags,
          t.mentions,
          t.media_urls,
          t.created_at as ts
        from social.twitter_tweets t
        where t.season_id = %s
          and t.is_reply = false
          and t.created_at >= %s
          and t.created_at <= %s
          {account_filter}
        order by (
          coalesce(t.likes, 0) + coalesce(t.retweets, 0) + coalesce(t.replies_count, 0)
          + coalesce(t.quotes, 0) + coalesce(t.views, 0)
        ) desc
        """,
        posts_params,
    )

    # For account-scoped runs: get full recursive reply chains for each post.
    # For unscoped runs: get direct replies only.
    post_tweet_ids = [p["source_id"] for p in posts if p["source_id"]]
    comments_by_root: dict[str, list[dict]] = defaultdict(list)
    comment_counts_by_root: dict[str, int] = defaultdict(int)

    if post_tweet_ids:
        if account_handles_list:
            # Full recursive reply chain (matches existing analytics CTE pattern)
            reply_rows = pg.fetch_all(
                """
                with recursive root_posts as (
                  select tweet_id from unnest(%s::text[]) as t(tweet_id)
                ), thread_replies as (
                  select r.tweet_id, r.reply_to_tweet_id,
                         r.reply_to_tweet_id as root_tweet_id
                  from social.twitter_tweets r
                  where r.season_id = %s
                    and r.is_reply = true
                    and r.reply_to_tweet_id in (select tweet_id from root_posts)
                  union
                  select child.tweet_id, child.reply_to_tweet_id,
                         parent.root_tweet_id
                  from social.twitter_tweets child
                  join thread_replies parent on child.reply_to_tweet_id = parent.tweet_id
                  where child.season_id = %s
                    and child.is_reply = true
                )
                select
                  tr.root_tweet_id,
                  t.tweet_id as comment_id,
                  coalesce(nullif(t.username, ''), nullif(t.source_account, ''), '') as author,
                  t.text,
                  coalesce(t.likes, 0) as likes,
                  true as is_reply,
                  coalesce(t.replies_count, 0) as reply_count,
                  t.created_at
                from thread_replies tr
                join social.twitter_tweets t on t.tweet_id = tr.tweet_id
                where t.season_id = %s
                order by tr.root_tweet_id, t.likes desc nulls last, t.created_at asc
                """,
                [post_tweet_ids, season_id, season_id, season_id],
            )
        else:
            # Non-bravo: direct replies only
            reply_rows = pg.fetch_all(
                """
                select
                  t.reply_to_tweet_id as root_tweet_id,
                  t.tweet_id as comment_id,
                  coalesce(nullif(t.username, ''), nullif(t.source_account, ''), '') as author,
                  t.text,
                  coalesce(t.likes, 0) as likes,
                  true as is_reply,
                  coalesce(t.replies_count, 0) as reply_count,
                  t.created_at
                from social.twitter_tweets t
                where t.season_id = %s
                  and t.is_reply = true
                  and t.reply_to_tweet_id = any(%s)
                order by t.reply_to_tweet_id, t.likes desc nulls last, t.created_at asc
                """,
                [season_id, post_tweet_ids],
            )

        # Group and count (max_comments=0 means no cap)
        for row in reply_rows:
            root_id = row["root_tweet_id"]
            comment_counts_by_root[root_id] = comment_counts_by_root.get(root_id, 0) + 1
            if max_comments == 0 or len(comments_by_root[root_id]) < max_comments:
                comments_by_root[root_id].append(row)

    result_posts = []
    total_engagement = 0
    total_comments_count = 0
    for p in posts:
        engagement = p["likes"] + p["retweets"] + p["replies_count"] + p["quotes"] + p["views"]
        total_engagement += engagement
        total_comments_count += comment_counts_by_root.get(p["source_id"], 0)

        hashtags = p.get("hashtags")
        if isinstance(hashtags, str):
            try:
                hashtags = json.loads(hashtags)
            except (json.JSONDecodeError, TypeError):
                hashtags = None

        mentions = p.get("mentions")
        if isinstance(mentions, str):
            try:
                mentions = json.loads(mentions)
            except (json.JSONDecodeError, TypeError):
                mentions = None
        if isinstance(mentions, list):
            mentions = [f"@{m}" if not str(m).startswith("@") else str(m) for m in mentions]

        media_urls = p.get("media_urls")
        if isinstance(media_urls, str):
            try:
                media_urls = json.loads(media_urls)
            except (json.JSONDecodeError, TypeError):
                media_urls = None

        author = p["author"] or "i"
        post_comments = comments_by_root.get(p["source_id"], [])

        result_posts.append(
            {
                "source_id": p["source_id"],
                "author": p["author"] or "",
                "display_name": p.get("display_name") or "",
                "text": p.get("text") or "",
                "url": f"https://x.com/{author}/status/{p['source_id']}" if p["source_id"] else "",
                "posted_at": _iso(p["ts"]),
                "likes": p["likes"],
                "retweets": p["retweets"],
                "replies_count": p["replies_count"],
                "quotes": p["quotes"],
                "views": p["views"],
                "hashtags": hashtags or [],
                "mentions": mentions or [],
                "media_urls": media_urls,
                "engagement": engagement,
                "total_comments_available": comment_counts_by_root.get(p["source_id"], 0),
                "comments": [
                    {
                        "comment_id": c["comment_id"],
                        "author": c["author"] or "",
                        "text": c["text"] or "",
                        "likes": c["likes"],
                        "is_reply": c["is_reply"],
                        "reply_count": c["reply_count"],
                        "created_at": _iso(c["created_at"]),
                    }
                    for c in post_comments
                ],
            }
        )

    return {
        "posts": result_posts,
        "totals": {
            "posts": len(result_posts),
            "total_comments": total_comments_count,
            "total_engagement": total_engagement,
        },
    }


_WEEK_DETAIL_HANDLERS: dict[str, Any] = {
    "instagram": _week_detail_instagram,
    "tiktok": _week_detail_tiktok,
    "youtube": _week_detail_youtube,
    "twitter": _week_detail_twitter,
}


def get_week_detail(
    season_id: str,
    *,
    week_index: int,
    platforms: list[str] | None,
    timezone: str,
    source_scope: str,
    max_comments_per_post: int = 50,
) -> dict[str, Any]:
    """Return detailed post-level data for a single week of a season."""
    context = get_season_context(season_id)
    available_platforms = [p for p in (platforms or list(SUPPORTED_PLATFORMS)) if p in SUPPORTED_PLATFORMS]
    if not available_platforms:
        available_platforms = list(SUPPORTED_PLATFORMS)

    now = _now_utc()
    week_windows, _week_zero_start_local = _resolve_week_windows(
        context,
        timezone=timezone,
        source_scope=source_scope,
        now_utc=now,
    )
    windows_by_index = {w.week_index: w for w in week_windows}
    window = windows_by_index.get(week_index)
    if window is None:
        raise ValueError(f"Week {week_index} is not available for this season")

    start_dt = window.start_local.astimezone(UTC)
    end_dt = (window.end_local - timedelta(microseconds=1)).astimezone(UTC)
    if end_dt < start_dt:
        end_dt = start_dt

    target_accounts_by_platform = _target_accounts_by_platform(season_id, source_scope=source_scope)
    requires_target_accounts = source_scope in {"bravo", "creator"}

    platform_results: dict[str, Any] = {}
    grand_posts = 0
    grand_comments = 0
    grand_engagement = 0

    for platform in available_platforms:
        handler = _WEEK_DETAIL_HANDLERS.get(platform)
        if not handler:
            continue
        account_handles = set(target_accounts_by_platform.get(platform, set()))
        if requires_target_accounts and not account_handles:
            result = {
                "posts": [],
                "totals": {
                    "posts": 0,
                    "total_comments": 0,
                    "total_engagement": 0,
                },
            }
            platform_results[platform] = result
            continue
        result = handler(
            season_id,
            start_dt=start_dt,
            end_dt=end_dt,
            account_handles=account_handles,
            max_comments=max_comments_per_post,
        )
        platform_results[platform] = result
        totals = result.get("totals", {})
        grand_posts += totals.get("posts", 0)
        grand_comments += totals.get("total_comments", 0)
        grand_engagement += totals.get("total_engagement", 0)

    week_end_inclusive = window.end_local - timedelta(microseconds=1)
    return {
        "week": {
            "week_index": week_index,
            "label": "Pre-Season" if week_index == 0 else f"Week {week_index}",
            "start": _iso(window.start_local.astimezone(UTC)),
            "end": _iso(week_end_inclusive.astimezone(UTC)),
        },
        "season": {
            "season_id": context.season_id,
            "show_id": context.show_id,
            "show_name": context.show_name,
            "season_number": context.season_number,
        },
        "source_scope": source_scope,
        "platforms": platform_results,
        "totals": {
            "posts": grand_posts,
            "total_comments": grand_comments,
            "total_engagement": grand_engagement,
        },
    }


# ---------------------------------------------------------------------------
# Post detail — all comments for a single post, threaded
# ---------------------------------------------------------------------------


def _thread_comments(flat: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Group flat comment list into threaded tree: top-level first, replies nested."""
    by_id: dict[Any, dict[str, Any]] = {}
    roots: list[dict[str, Any]] = []
    for c in flat:
        node = {**c, "replies": []}
        by_id[c["id"]] = node

    for c in flat:
        node = by_id[c["id"]]
        parent = c.get("parent_comment_id")
        if parent and parent in by_id:
            by_id[parent]["replies"].append(node)
        else:
            roots.append(node)

    return roots


def _serialize_comment_tree(node: dict[str, Any]) -> dict[str, Any]:
    return {
        "comment_id": node.get("comment_id") or "",
        "author": node.get("author") or "",
        "text": node.get("text") or "",
        "likes": node.get("likes", 0),
        "is_reply": node.get("is_reply", False),
        "reply_count": node.get("reply_count", 0),
        "created_at": _iso(node.get("created_at")),
        "replies": [_serialize_comment_tree(r) for r in node.get("replies", [])],
    }


def get_post_comments(
    season_id: str,
    *,
    platform: str,
    source_id: str,
) -> dict[str, Any]:
    """Return ALL comments for a single post, threaded by parent_comment_id."""
    if platform not in SUPPORTED_PLATFORMS:
        raise ValueError(f"Unsupported platform: {platform}")

    if platform == "instagram":
        post = pg.fetch_one(
            """
            select p.id, p.shortcode as source_id, p.username as author, p.caption as text,
                   coalesce(p.likes, 0) as likes, coalesce(p.comments_count, 0) as comments_count,
                   coalesce(p.views, 0) as views, p.thumbnail_url, p.posted_at as ts
            from social.instagram_posts p
            where p.season_id = %s and p.shortcode = %s
            """,
            [season_id, source_id],
        )
        if not post:
            raise ValueError("Post not found")

        comments = pg.fetch_all(
            """
            select c.id, c.comment_id, c.parent_comment_id,
                   c.username as author, c.text,
                   coalesce(c.likes, 0) as likes,
                   coalesce(c.is_reply, false) as is_reply,
                   coalesce(c.reply_count, 0) as reply_count,
                   c.created_at
            from social.instagram_comments c
            where c.post_id = %s
            order by c.likes desc nulls last, c.created_at asc
            """,
            [post["id"]],
        )

        engagement = post["likes"] + post["comments_count"] + post["views"]
        return {
            "platform": "instagram",
            "source_id": source_id,
            "author": post["author"] or "",
            "text": post.get("text") or "",
            "url": f"https://www.instagram.com/p/{source_id}/",
            "posted_at": _iso(post["ts"]),
            "thumbnail_url": post.get("thumbnail_url"),
            "stats": {
                "likes": post["likes"],
                "comments_count": post["comments_count"],
                "views": post["views"],
                "engagement": engagement,
            },
            "total_comments_in_db": len(comments),
            "comments": [_serialize_comment_tree(node) for node in _thread_comments(comments)],
        }

    if platform == "tiktok":
        post = pg.fetch_one(
            """
            select p.id, p.video_id as source_id, p.username as author, p.description as text,
                   coalesce(p.likes, 0) as likes, coalesce(p.comments_count, 0) as comments_count,
                   coalesce(p.shares, 0) as shares, coalesce(p.views, 0) as views,
                   p.thumbnail_url, p.posted_at as ts
            from social.tiktok_posts p
            where p.season_id = %s and p.video_id = %s
            """,
            [season_id, source_id],
        )
        if not post:
            raise ValueError("Post not found")

        comments = pg.fetch_all(
            """
            select c.id, c.comment_id, c.parent_comment_id,
                   c.username as author, c.text,
                   coalesce(c.likes, 0) as likes,
                   coalesce(c.is_reply, false) as is_reply,
                   coalesce(c.reply_count, 0) as reply_count,
                   c.created_at
            from social.tiktok_comments c
            where c.post_id = %s
            order by c.likes desc nulls last, c.created_at asc
            """,
            [post["id"]],
        )

        engagement = post["likes"] + post["comments_count"] + post["shares"] + post["views"]
        return {
            "platform": "tiktok",
            "source_id": source_id,
            "author": post["author"] or "",
            "text": post.get("text") or "",
            "url": f"https://www.tiktok.com/@{post['author'] or ''}/video/{source_id}",
            "posted_at": _iso(post["ts"]),
            "thumbnail_url": post.get("thumbnail_url"),
            "stats": {
                "likes": post["likes"],
                "comments_count": post["comments_count"],
                "shares": post["shares"],
                "views": post["views"],
                "engagement": engagement,
            },
            "total_comments_in_db": len(comments),
            "comments": [_serialize_comment_tree(node) for node in _thread_comments(comments)],
        }

    if platform == "youtube":
        post = pg.fetch_one(
            """
            select v.id, v.video_id as source_id, v.channel_title as author,
                   v.title, v.description as text,
                   coalesce(v.views, 0) as views, coalesce(v.likes, 0) as likes,
                   coalesce(v.comments_count, 0) as comments_count,
                   v.thumbnail_url, v.published_at as ts
            from social.youtube_videos v
            where v.season_id = %s and v.video_id = %s
            """,
            [season_id, source_id],
        )
        if not post:
            raise ValueError("Post not found")

        comments = pg.fetch_all(
            """
            select c.id, c.comment_id, c.parent_comment_id,
                   c.author, c.text,
                   coalesce(c.likes, 0) as likes,
                   coalesce(c.is_reply, false) as is_reply,
                   coalesce(c.reply_count, 0) as reply_count,
                   c.created_at
            from social.youtube_comments c
            where c.video_id = %s
            order by c.likes desc nulls last, c.created_at asc
            """,
            [post["id"]],
        )

        engagement = post["views"] + post["likes"] + post["comments_count"]
        return {
            "platform": "youtube",
            "source_id": source_id,
            "author": post["author"] or "",
            "title": post.get("title") or "",
            "text": post.get("text") or "",
            "url": f"https://www.youtube.com/watch?v={source_id}",
            "posted_at": _iso(post["ts"]),
            "thumbnail_url": post.get("thumbnail_url"),
            "stats": {
                "views": post["views"],
                "likes": post["likes"],
                "comments_count": post["comments_count"],
                "engagement": engagement,
            },
            "total_comments_in_db": len(comments),
            "comments": [_serialize_comment_tree(node) for node in _thread_comments(comments)],
        }

    if platform == "twitter":
        post = pg.fetch_one(
            """
            select t.tweet_id as source_id,
                   coalesce(nullif(t.username, ''), nullif(t.source_account, ''), '') as author,
                   t.display_name, t.text,
                   coalesce(t.likes, 0) as likes,
                   coalesce(t.retweets, 0) as retweets,
                   coalesce(t.replies_count, 0) as replies_count,
                   coalesce(t.quotes, 0) as quotes,
                   coalesce(t.views, 0) as views,
                   t.created_at as ts
            from social.twitter_tweets t
            where t.season_id = %s and t.tweet_id = %s and t.is_reply = false
            """,
            [season_id, source_id],
        )
        if not post:
            raise ValueError("Post not found")

        # Full recursive reply chain
        reply_rows = pg.fetch_all(
            """
            with recursive thread_replies as (
              select r.tweet_id, r.reply_to_tweet_id,
                     r.reply_to_tweet_id as parent_id
              from social.twitter_tweets r
              where r.season_id = %s
                and r.is_reply = true
                and r.reply_to_tweet_id = %s
              union
              select child.tweet_id, child.reply_to_tweet_id,
                     child.reply_to_tweet_id as parent_id
              from social.twitter_tweets child
              join thread_replies parent on child.reply_to_tweet_id = parent.tweet_id
              where child.season_id = %s
                and child.is_reply = true
            )
            select
              t.tweet_id as id,
              t.tweet_id as comment_id,
              t.reply_to_tweet_id as parent_comment_id,
              coalesce(nullif(t.username, ''), nullif(t.source_account, ''), '') as author,
              t.text,
              coalesce(t.likes, 0) as likes,
              true as is_reply,
              coalesce(t.replies_count, 0) as reply_count,
              t.created_at
            from thread_replies tr
            join social.twitter_tweets t on t.tweet_id = tr.tweet_id
            where t.season_id = %s
            order by t.likes desc nulls last, t.created_at asc
            """,
            [season_id, source_id, season_id, season_id],
        )

        # Thread replies: direct replies to root go under root, deeper replies nest
        threaded: list[dict[str, Any]] = []
        by_id: dict[str, dict[str, Any]] = {}
        for r in reply_rows:
            node = {**r, "replies": []}
            by_id[r["id"]] = node
        for r in reply_rows:
            node = by_id[r["id"]]
            parent_id = r.get("parent_comment_id")
            if parent_id == source_id or parent_id not in by_id:
                threaded.append(node)
            else:
                by_id[parent_id]["replies"].append(node)

        engagement = post["likes"] + post["retweets"] + post["replies_count"] + post["quotes"] + post["views"]
        author = post["author"] or "i"
        return {
            "platform": "twitter",
            "source_id": source_id,
            "author": post["author"] or "",
            "display_name": post.get("display_name") or "",
            "text": post.get("text") or "",
            "url": f"https://x.com/{author}/status/{source_id}",
            "posted_at": _iso(post["ts"]),
            "stats": {
                "likes": post["likes"],
                "retweets": post["retweets"],
                "replies_count": post["replies_count"],
                "quotes": post["quotes"],
                "views": post["views"],
                "engagement": engagement,
            },
            "total_comments_in_db": len(reply_rows),
            "comments": [_serialize_comment_tree(node) for node in threaded],
        }

    raise ValueError(f"Unsupported platform: {platform}")


def refresh_post_comments(
    season_id: str,
    *,
    platform: str,
    source_id: str,
    max_comments_per_post: int = 100000,
    fetch_replies: bool = True,
) -> dict[str, Any]:
    """Re-sync comments for a single post/video/reply thread."""
    normalized_platform = (platform or "").strip().lower()
    if normalized_platform not in SUPPORTED_PLATFORMS:
        raise ValueError(f"Unsupported platform: {platform}")

    context = get_season_context(season_id)
    try:
        max_comments = max(0, int(max_comments_per_post))
    except (TypeError, ValueError):
        max_comments = 0

    if normalized_platform == "instagram":
        row = pg.fetch_one(
            """
            select p.id::text as id,
                   coalesce(nullif(p.source_account, ''), nullif(p.username, ''), '') as account
            from social.instagram_posts p
            where p.season_id = %s and p.shortcode = %s
            """,
            [season_id, source_id],
        )
        if not row:
            raise ValueError("Post not found")

        from trr_backend.socials.instagram import InstagramScraper

        account = str(row.get("account") or "")
        scraper = InstagramScraper(cookies=_load_instagram_cookies())
        comments = (
            scraper.fetch_comments(
                source_id,
                max_comments=max_comments,
                fetch_replies=fetch_replies,
                delay=0.25,
            )
            if max_comments > 0
            else []
        )
        upserted = 0
        with pg.db_connection() as conn:
            for comment in comments:
                upserted += _upsert_instagram_comment_tree(
                    context,
                    job_id=None,
                    run_id=None,
                    account=account,
                    post_id=str(row["id"]),
                    comment=comment,
                    conn=conn,
                )
        total_comments = _count_stored_comments([str(row["id"])], "instagram").get(str(row["id"]), 0)
        return {
            "platform": normalized_platform,
            "source_id": source_id,
            "comments_fetched": len(comments),
            "comments_upserted": upserted,
            "total_comments_in_db": total_comments,
            "fetch_replies": fetch_replies,
            "max_comments_per_post": max_comments,
            "comment_fetch_reason": str(getattr(scraper, "last_comment_fetch_reason", "") or ""),
            "comments_auth_failed": bool(getattr(scraper, "comments_auth_failed", False)),
        }

    if normalized_platform == "tiktok":
        row = pg.fetch_one(
            """
            select p.id::text as id,
                   coalesce(nullif(p.source_account, ''), nullif(p.username, ''), '') as account
            from social.tiktok_posts p
            where p.season_id = %s and p.video_id = %s
            """,
            [season_id, source_id],
        )
        if not row:
            raise ValueError("Post not found")

        from trr_backend.socials.tiktok import TikTokScraper

        account = str(row.get("account") or "")
        scraper = TikTokScraper(cookies=_load_tiktok_cookies())
        comments = (
            scraper.fetch_comments(
                source_id,
                username=account,
                max_comments=max_comments,
                fetch_replies=fetch_replies,
                delay=0.5,
            )
            if max_comments > 0
            else []
        )
        upserted = 0
        with pg.db_connection() as conn:
            for comment in comments:
                upserted += _upsert_tiktok_comment_tree(
                    context,
                    job_id=None,
                    run_id=None,
                    account=account,
                    post_id=str(row["id"]),
                    comment=comment,
                    conn=conn,
                )
        total_comments = _count_stored_comments([str(row["id"])], "tiktok").get(str(row["id"]), 0)
        return {
            "platform": normalized_platform,
            "source_id": source_id,
            "comments_fetched": len(comments),
            "comments_upserted": upserted,
            "total_comments_in_db": total_comments,
            "fetch_replies": fetch_replies,
            "max_comments_per_post": max_comments,
            "comment_fetch_reason": str(getattr(scraper, "_last_api_fail_reason", "") or ""),
        }

    if normalized_platform == "youtube":
        row = pg.fetch_one(
            """
            select v.id::text as id,
                   coalesce(nullif(v.source_account, ''), nullif(v.channel_title, ''), '') as account
            from social.youtube_videos v
            where v.season_id = %s and v.video_id = %s
            """,
            [season_id, source_id],
        )
        if not row:
            raise ValueError("Post not found")

        from trr_backend.socials.youtube import YouTubeScraper

        account = str(row.get("account") or "")
        scraper = YouTubeScraper()
        comments = (
            scraper.fetch_comments(
                source_id,
                max_comments=max_comments,
                fetch_replies=fetch_replies,
                delay=0.5,
            )
            if max_comments > 0
            else []
        )
        upserted = 0
        with pg.db_connection() as conn:
            for comment in comments:
                upserted += _upsert_youtube_comment_tree(
                    context,
                    job_id=None,
                    run_id=None,
                    account=account,
                    video_db_id=str(row["id"]),
                    comment=comment,
                    conn=conn,
                )
        total_comments = _count_stored_comments([str(row["id"])], "youtube").get(str(row["id"]), 0)
        return {
            "platform": normalized_platform,
            "source_id": source_id,
            "comments_fetched": len(comments),
            "comments_upserted": upserted,
            "total_comments_in_db": total_comments,
            "fetch_replies": fetch_replies,
            "max_comments_per_post": max_comments,
        }

    row = pg.fetch_one(
        """
        select t.tweet_id,
               coalesce(nullif(t.source_account, ''), nullif(t.username, ''), '') as account
        from social.twitter_tweets t
        where t.season_id = %s and t.tweet_id = %s and t.is_reply = false
        """,
        [season_id, source_id],
    )
    if not row:
        raise ValueError("Post not found")

    from trr_backend.socials.twitter import TwitterScraper

    account = str(row.get("account") or "")
    twitter_cookies, twitter_bearer = _load_twitter_auth()
    twikit_creds = _load_twikit_credentials()
    if not twitter_cookies.get("ct0") and twikit_creds:
        if twikit_creds.get("auth_token") and twikit_creds.get("ct0"):
            twitter_cookies = {**twitter_cookies, "auth_token": twikit_creds["auth_token"], "ct0": twikit_creds["ct0"]}

    scraper = TwitterScraper(cookies=twitter_cookies, bearer_token=twitter_bearer, twikit_credentials=twikit_creds)
    replies = (
        scraper.fetch_tweet_replies(source_id, delay=0.5)[:max_comments]
        if fetch_replies and max_comments > 0
        else []
    )
    upserted = 0
    with pg.db_connection() as conn:
        for reply in replies:
            if not getattr(reply, "reply_to_tweet_id", None):
                reply.reply_to_tweet_id = source_id
            reply.is_reply = True
            if _upsert_tweet(
                context,
                job_id=None,
                run_id=None,
                account=account,
                tweet=reply,
                conn=conn,
            ):
                upserted += 1
    total_comments = _count_stored_replies([source_id]).get(source_id, 0)
    return {
        "platform": normalized_platform,
        "source_id": source_id,
        "comments_fetched": len(replies),
        "comments_upserted": upserted,
        "total_comments_in_db": total_comments,
        "fetch_replies": fetch_replies,
        "max_comments_per_post": max_comments,
    }


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
