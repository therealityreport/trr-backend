"""Repository for SocialBlade growth data (pipeline.socialblade_growth_data)."""

from __future__ import annotations

import logging
import re
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse
from uuid import UUID

from psycopg2.extras import Json

from trr_backend.db import pg

logger = logging.getLogger(__name__)
_PLATFORM_RE = re.compile(r"[^a-z]")
_HANDLE_RE = re.compile(r"[^a-zA-Z0-9._-]")
_KNOWN_URL_HOST_PREFIXES = (
    "www.",
    "socialblade.com/",
    "instagram.com/",
    "threads.net/",
    "tiktok.com/",
    "youtube.com/",
    "youtu.be/",
    "facebook.com/",
    "fb.com/",
)
_SOCIALBLADE_ROUTE_SEGMENTS = {"user", "handle", "channel", "c"}
_REFRESH_METADATA_KEYS = (
    "history_source",
    "profile_stats_labels",
    "chart_metric_label",
    "socialblade_url",
    "fallback_chain",
    "runtime_metadata",
)
_FAILED_ATTEMPT_METADATA_KEYS = (
    "last_attempt_at",
    "last_attempt_stats_refreshed",
    "last_attempt_history_source",
    "last_attempt_error",
    "last_attempt_runtime_metadata",
)


def _build_previous_run_snapshot(data: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(data, dict):
        return None
    profile_stats = data.get("profile_stats")
    if not isinstance(profile_stats, dict) or not profile_stats:
        return None

    snapshot: dict[str, Any] = {
        "profile_stats": profile_stats,
        "scraped_at": data.get("scraped_at"),
    }

    rankings = data.get("rankings")
    if isinstance(rankings, dict) and rankings:
        snapshot["rankings"] = rankings

    profile_stats_labels = data.get("profile_stats_labels")
    if isinstance(profile_stats_labels, dict) and profile_stats_labels:
        snapshot["profile_stats_labels"] = profile_stats_labels

    return snapshot


def normalize_socialblade_platform(platform: str | None) -> str:
    normalized = _PLATFORM_RE.sub("", str(platform or "").strip().lower())
    return normalized or "instagram"


def normalize_socialblade_person_id(person_id: str | None, *, field_name: str = "personId") -> str | None:
    rendered = str(person_id or "").strip()
    if not rendered:
        return None
    try:
        return str(UUID(rendered))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be a valid UUID") from exc


def _is_youtube_channel_id(value: str, *, platform: str | None) -> bool:
    rendered = str(value or "").strip()
    if not rendered:
        return False
    normalized_platform = normalize_socialblade_platform(platform) if platform else ""
    if normalized_platform == "youtube":
        return rendered.upper().startswith("UC")
    return rendered.startswith("UC")


def _clean_socialblade_account_handle(value: str, *, platform: str | None = None) -> str:
    rendered = str(value or "").strip().lstrip("@")
    if not rendered:
        return ""
    if _is_youtube_channel_id(rendered, platform=platform):
        return _HANDLE_RE.sub("", rendered)
    return _HANDLE_RE.sub("", rendered.lower())


def _extract_handle_from_url(value: str) -> str:
    rendered = value.strip()
    lowered = rendered.lower()
    if "://" not in rendered and lowered.startswith(_KNOWN_URL_HOST_PREFIXES):
        rendered = f"https://{rendered}"

    parsed = urlparse(rendered)
    if not parsed.netloc:
        return value

    host = parsed.netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    segments = [unquote(segment).strip() for segment in parsed.path.split("/") if segment.strip()]
    if not segments:
        return ""

    if host.endswith("socialblade.com"):
        if len(segments) >= 3 and segments[1].lower() in _SOCIALBLADE_ROUTE_SEGMENTS:
            return segments[2]
        return segments[-1]

    if host.endswith(("instagram.com", "threads.net")):
        if segments[0].lower() in {"accounts", "explore", "p", "reel", "stories"}:
            return ""
        return segments[0]

    if host.endswith("tiktok.com"):
        return next((segment for segment in segments if segment.startswith("@")), segments[0])

    if host.endswith(("youtube.com", "youtu.be")):
        first = segments[0].lower()
        if first in {"channel", "user", "c", "handle"} and len(segments) >= 2:
            return segments[1]
        return segments[0]

    if host.endswith(("facebook.com", "fb.com")):
        if segments[0].lower() == "profile.php":
            profile_id = str((parse_qs(parsed.query).get("id") or [""])[0]).strip()
            return profile_id
        if segments[0].lower() in {"pages", "watch", "reel"}:
            return segments[-1]
        return segments[0]

    return segments[-1]


def normalize_socialblade_account_handle(handle: str | None, *, platform: str | None = None) -> str:
    rendered = str(handle or "").strip()
    if not rendered:
        return ""
    rendered = _extract_handle_from_url(rendered)
    return _clean_socialblade_account_handle(rendered, platform=platform)


def _failed_attempt_metadata(fresh: dict[str, Any]) -> dict[str, Any]:
    attempted_at = fresh.get("last_attempt_at") or fresh.get("scraped_at") or datetime.now(tz=UTC).isoformat()
    metadata: dict[str, Any] = {
        "last_attempt_at": attempted_at,
        "last_attempt_stats_refreshed": bool(fresh.get("stats_refreshed", False)),
    }
    if "history_source" in fresh:
        metadata["last_attempt_history_source"] = fresh.get("history_source")
    if "error" in fresh:
        metadata["last_attempt_error"] = fresh.get("error")
    if "runtime_metadata" in fresh:
        metadata["last_attempt_runtime_metadata"] = fresh.get("runtime_metadata")
    return metadata


def _metrics_row_count(metrics: Any) -> int:
    if not isinstance(metrics, dict):
        return 0
    try:
        return int(metrics.get("row_count") or 0)
    except (TypeError, ValueError):
        return 0


def _metric_row_date(row: Any) -> str:
    if not isinstance(row, dict):
        return ""
    rendered = str(row.get("Date") or row.get("date") or "").strip()
    match = re.search(r"\d{4}-\d{2}-\d{2}", rendered)
    return match.group(0) if match else rendered[:10]


def _merge_daily_channel_metrics(existing: Any, fresh: Any) -> dict[str, Any]:
    if not isinstance(fresh, dict):
        return existing if isinstance(existing, dict) else {}
    if not isinstance(existing, dict):
        return fresh

    fresh_count = _metrics_row_count(fresh)
    existing_count = _metrics_row_count(existing)
    if fresh_count >= existing_count:
        return fresh

    existing_rows = existing.get("data")
    fresh_rows = fresh.get("data")
    if not isinstance(existing_rows, list) or not isinstance(fresh_rows, list) or not existing_rows or not fresh_rows:
        return fresh

    rows_by_date: dict[str, dict[str, Any]] = {}
    for row in existing_rows:
        date = _metric_row_date(row)
        if date and isinstance(row, dict):
            rows_by_date[date] = dict(row)
    for row in fresh_rows:
        date = _metric_row_date(row)
        if date and isinstance(row, dict):
            rows_by_date[date] = dict(row)

    merged_rows = [rows_by_date[date] for date in sorted(rows_by_date)]
    if len(merged_rows) <= fresh_count:
        return fresh

    headers = fresh.get("headers") or existing.get("headers") or []
    return {
        **existing,
        **fresh,
        "headers": headers,
        "data": merged_rows,
        "row_count": len(merged_rows),
        "period": f"Last {len(merged_rows)} Days",
    }


def socialblade_growth_table_exists() -> bool:
    """Return whether the SocialBlade growth table exists in the current database."""
    row = pg.fetch_one("SELECT to_regclass('pipeline.socialblade_growth_data') AS relation_name")
    return bool(row and row.get("relation_name"))


def socialblade_growth_snapshots_table_exists() -> bool:
    """Return whether the immutable SocialBlade snapshot table exists."""
    row = pg.fetch_one("SELECT to_regclass('pipeline.socialblade_growth_snapshots') AS relation_name")
    return bool(row and row.get("relation_name"))


def get_growth_data(person_id: str | None, handle: str, *, platform: str = "instagram") -> dict[str, Any] | None:
    """Fetch stored SocialBlade data for a handle, preferring the linked person row when present."""
    normalized_person_id = normalize_socialblade_person_id(person_id)
    normalized_platform = normalize_socialblade_platform(platform)
    normalized_handle = normalize_socialblade_account_handle(handle, platform=normalized_platform)
    if not normalized_handle:
        return None
    if normalized_person_id:
        row = pg.fetch_one(
            """
            SELECT *
            FROM pipeline.socialblade_growth_data
            WHERE platform = %s
              AND account_handle = %s
              AND (person_id = %s OR person_id IS NULL)
            ORDER BY
              CASE
                WHEN person_id = %s THEN 0
                WHEN person_id IS NULL THEN 1
                ELSE 2
              END,
              updated_at DESC NULLS LAST,
              created_at DESC NULLS LAST
            LIMIT 1
            """,
            [normalized_platform, normalized_handle, normalized_person_id, normalized_person_id],
        )
    else:
        row = pg.fetch_one(
            """
            SELECT *
            FROM pipeline.socialblade_growth_data
            WHERE platform = %s
              AND account_handle = %s
            ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST
            LIMIT 1
            """,
            [normalized_platform, normalized_handle],
        )
    if not row:
        return None
    return _row_to_response(row)


def upsert_growth_data(
    person_id: str | None,
    handle: str,
    data: dict[str, Any],
    *,
    platform: str = "instagram",
) -> dict[str, Any]:
    """Upsert merged SocialBlade data. Returns the stored row."""
    normalized_person_id = normalize_socialblade_person_id(person_id)
    normalized_platform = normalize_socialblade_platform(platform)
    normalized_handle = normalize_socialblade_account_handle(handle, platform=normalized_platform)
    rows = pg.execute_returning(
        "INSERT INTO pipeline.socialblade_growth_data "
        "(person_id, platform, account_handle, instagram_handle, scraped_at, stats_refreshed, "
        " profile_stats, rankings, daily_channel_metrics_60day, "
        " daily_total_followers_chart, raw_response, updated_at) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now()) "
        "ON CONFLICT (platform, account_handle) DO UPDATE SET "
        " person_id = COALESCE(pipeline.socialblade_growth_data.person_id, EXCLUDED.person_id), "
        " instagram_handle = EXCLUDED.instagram_handle, "
        " scraped_at = EXCLUDED.scraped_at, "
        " stats_refreshed = EXCLUDED.stats_refreshed, "
        " profile_stats = EXCLUDED.profile_stats, "
        " rankings = EXCLUDED.rankings, "
        " daily_channel_metrics_60day = EXCLUDED.daily_channel_metrics_60day, "
        " daily_total_followers_chart = EXCLUDED.daily_total_followers_chart, "
        " raw_response = EXCLUDED.raw_response, "
        " updated_at = now() "
        "RETURNING *",
        [
            normalized_person_id,
            normalized_platform,
            normalized_handle,
            normalized_handle,
            data.get("scraped_at", datetime.now(tz=UTC).isoformat()),
            bool(data.get("stats_refreshed", False)),
            Json(data.get("profile_stats", {})),
            Json(data.get("rankings", {})),
            Json(data.get("daily_channel_metrics_60day", {})),
            Json(data.get("daily_total_followers_chart")),
            Json(data),  # store full response as raw_response
        ],
    )
    if not rows:
        raise RuntimeError("Upsert returned no rows")
    return _row_to_response(rows[0])


def insert_growth_snapshot(
    person_id: str | None,
    handle: str,
    data: dict[str, Any],
    *,
    platform: str = "instagram",
    growth_data_id: str | None = None,
    source: str | None = None,
    force: bool = False,
) -> dict[str, Any]:
    """Insert an immutable SocialBlade scrape snapshot."""
    normalized_person_id = normalize_socialblade_person_id(person_id)
    normalized_platform = normalize_socialblade_platform(platform)
    normalized_handle = normalize_socialblade_account_handle(handle, platform=normalized_platform)
    if not normalized_handle:
        raise ValueError("SocialBlade snapshot requires a valid account handle.")
    rows = pg.execute_returning(
        """
        insert into pipeline.socialblade_growth_snapshots (
          growth_data_id,
          person_id,
          platform,
          account_handle,
          instagram_handle,
          scraped_at,
          stats_refreshed,
          profile_stats,
          rankings,
          daily_channel_metrics_60day,
          daily_total_followers_chart,
          raw_response,
          snapshot_source,
          refresh_source,
          refresh_forced
        )
        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        returning
          id::text as id,
          growth_data_id::text as growth_data_id,
          platform,
          account_handle,
          scraped_at
        """,
        [
            growth_data_id,
            normalized_person_id,
            normalized_platform,
            normalized_handle,
            normalized_handle if normalized_platform == "instagram" else None,
            data.get("scraped_at", datetime.now(tz=UTC).isoformat()),
            bool(data.get("stats_refreshed", False)),
            Json(data.get("profile_stats", {})),
            Json(data.get("rankings", {})),
            Json(data.get("daily_channel_metrics_60day", {})),
            Json(data.get("daily_total_followers_chart")),
            Json(data),
            source,
            source,
            bool(force),
        ],
    )
    if not rows:
        raise RuntimeError("SocialBlade snapshot insert returned no rows")
    row = dict(rows[0])
    scraped_at_raw = row.get("scraped_at")
    row["scraped_at"] = scraped_at_raw.isoformat() if hasattr(scraped_at_raw, "isoformat") else scraped_at_raw
    return row


def merge_chart_data(existing: dict[str, Any] | None, fresh: dict[str, Any]) -> dict[str, Any]:
    """Merge fresh scrape data with existing data.

    Key rule: never change follower counts for dates more than 1 day old.
    Always update: profile_stats, rankings, scraped_at, 60-day table.
    """
    stats_refreshed = bool(fresh.get("stats_refreshed", False))
    if not existing:
        if stats_refreshed:
            return fresh
        return {**fresh, **_failed_attempt_metadata(fresh)}

    now = datetime.now(tz=UTC)
    cutoff_date = (now - timedelta(days=1)).strftime("%Y-%m-%d")

    merged: dict[str, Any] = {
        **existing,
        "scraped_at": (
            fresh.get("scraped_at", existing.get("scraped_at")) if stats_refreshed else existing.get("scraped_at")
        ),
        "stats_refreshed": stats_refreshed if stats_refreshed else bool(existing.get("stats_refreshed", False)),
        "profile_stats": existing.get("profile_stats", {}),
        "rankings": existing.get("rankings", {}),
        "daily_channel_metrics_60day": existing.get("daily_channel_metrics_60day", {}),
    }
    existing_previous_run = existing.get("previous_run")
    if isinstance(existing_previous_run, dict) and existing_previous_run:
        merged["previous_run"] = existing_previous_run
    if "instagram_following_scrape" in fresh:
        merged["instagram_following_scrape"] = fresh.get("instagram_following_scrape")

    if stats_refreshed:
        for key in _FAILED_ATTEMPT_METADATA_KEYS:
            merged.pop(key, None)
        previous_run = _build_previous_run_snapshot(existing)
        if previous_run:
            merged["previous_run"] = previous_run
        else:
            merged.pop("previous_run", None)
        merged["profile_stats"] = fresh.get("profile_stats", existing.get("profile_stats", {}))
        merged["rankings"] = fresh.get("rankings", existing.get("rankings", {}))
        merged["daily_channel_metrics_60day"] = _merge_daily_channel_metrics(
            existing.get("daily_channel_metrics_60day"),
            fresh.get("daily_channel_metrics_60day"),
        )
        for key in _REFRESH_METADATA_KEYS:
            if key in fresh:
                merged[key] = fresh.get(key)
    else:
        merged.update(_failed_attempt_metadata(fresh))
        return merged

    fresh_chart = fresh.get("daily_total_followers_chart")
    existing_chart = existing.get("daily_total_followers_chart")

    if fresh_chart and existing_chart:
        existing_by_date: dict[str, int] = {}
        for pt in existing_chart.get("data", []):
            existing_by_date[pt["date"]] = pt["followers"]

        fresh_by_date: dict[str, int] = {}
        for pt in fresh_chart.get("data", []):
            fresh_by_date[pt["date"]] = pt["followers"]

        # Start with all existing data
        merged_points: dict[str, int] = dict(existing_by_date)

        for date, followers in fresh_by_date.items():
            if date >= cutoff_date:
                # Recent data: use fresh value
                merged_points[date] = followers
            elif date not in merged_points:
                # Historical gap: fill from fresh
                merged_points[date] = followers
            # Historical data already present: keep existing (don't overwrite)

        sorted_data = sorted(merged_points.items(), key=lambda x: x[0])
        data_list = [{"date": d, "followers": f} for d, f in sorted_data]

        merged["daily_total_followers_chart"] = {
            "frequency": "daily",
            "metric": "total_followers",
            "total_data_points": len(data_list),
            "date_range": {
                "from": data_list[0]["date"] if data_list else "",
                "to": data_list[-1]["date"] if data_list else "",
            },
            "data": data_list,
        }
    elif fresh_chart:
        merged["daily_total_followers_chart"] = fresh_chart

    return merged


def _row_to_response(row: dict[str, Any]) -> dict[str, Any]:
    """Convert a database row to the JSON response shape the frontend expects."""
    platform = normalize_socialblade_platform(row.get("platform"))
    raw_response = (row.get("raw_response") or {}) if isinstance(row.get("raw_response"), dict) else {}
    previous_run = _build_previous_run_snapshot(raw_response.get("previous_run"))
    account_handle = str(row.get("account_handle") or row.get("instagram_handle") or "").strip()
    scraped_at_raw = row.get("scraped_at")
    scraped_at_value = scraped_at_raw.isoformat() if hasattr(scraped_at_raw, "isoformat") else str(scraped_at_raw or "")
    freshness_status = "missing"
    is_stale = True
    age_hours: float | None = None
    if scraped_at_value:
        try:
            parsed = datetime.fromisoformat(scraped_at_value.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=UTC)
            age_hours = max(0.0, (datetime.now(tz=UTC) - parsed.astimezone(UTC)).total_seconds() / 3600.0)
            is_stale = age_hours > 24.0
            freshness_status = "stale" if is_stale else "fresh"
        except ValueError:
            freshness_status = "unknown"

    return {
        "row_id": str(row.get("id") or "").strip() or None,
        "username": account_handle,
        "account_handle": account_handle,
        "platform": platform,
        "scraped_at": scraped_at_value,
        "stats_refreshed": bool(row.get("stats_refreshed", False)),
        "history_source": raw_response.get("history_source"),
        "profile_stats": row.get("profile_stats", {}),
        "profile_stats_labels": raw_response.get("profile_stats_labels") or {},
        "rankings": row.get("rankings", {}),
        "daily_channel_metrics_60day": row.get("daily_channel_metrics_60day", {}),
        "daily_total_followers_chart": row.get("daily_total_followers_chart"),
        "chart_metric_label": raw_response.get("chart_metric_label"),
        "socialblade_url": raw_response.get("socialblade_url"),
        "instagram_following_scrape": raw_response.get("instagram_following_scrape"),
        "previous_run": previous_run,
        "last_attempt_at": raw_response.get("last_attempt_at"),
        "last_attempt_stats_refreshed": raw_response.get("last_attempt_stats_refreshed"),
        "last_attempt_history_source": raw_response.get("last_attempt_history_source"),
        "last_attempt_error": raw_response.get("last_attempt_error"),
        "freshness_status": freshness_status,
        "is_stale": is_stale,
        "age_hours": age_hours,
    }
