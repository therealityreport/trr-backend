"""Repository for SocialBlade growth data (pipeline.socialblade_growth_data)."""

from __future__ import annotations

import logging
import re
from datetime import UTC, datetime, timedelta
from typing import Any

from psycopg2.extras import Json

from trr_backend.db import pg

logger = logging.getLogger(__name__)
_PLATFORM_RE = re.compile(r"[^a-z]")
_HANDLE_RE = re.compile(r"[^a-zA-Z0-9._-]")


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


def normalize_socialblade_account_handle(handle: str | None) -> str:
    return _HANDLE_RE.sub("", str(handle or "").strip().lstrip("@").lower())


def socialblade_growth_table_exists() -> bool:
    """Return whether the SocialBlade growth table exists in the current database."""
    row = pg.fetch_one("SELECT to_regclass('pipeline.socialblade_growth_data') AS relation_name")
    return bool(row and row.get("relation_name"))


def get_growth_data(person_id: str | None, handle: str, *, platform: str = "instagram") -> dict[str, Any] | None:
    """Fetch stored SocialBlade data for a handle, preferring the linked person row when present."""
    normalized_platform = normalize_socialblade_platform(platform)
    normalized_handle = normalize_socialblade_account_handle(handle)
    if not normalized_handle:
        return None
    if person_id:
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
            [normalized_platform, normalized_handle, person_id, person_id],
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
    normalized_platform = normalize_socialblade_platform(platform)
    normalized_handle = normalize_socialblade_account_handle(handle)
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
            person_id,
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


def merge_chart_data(existing: dict[str, Any] | None, fresh: dict[str, Any]) -> dict[str, Any]:
    """Merge fresh scrape data with existing data.

    Key rule: never change follower counts for dates more than 1 day old.
    Always update: profile_stats, rankings, scraped_at, 60-day table.
    """
    if not existing:
        return fresh

    now = datetime.now(tz=UTC)
    cutoff_date = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    stats_refreshed = bool(fresh.get("stats_refreshed", False))

    merged: dict[str, Any] = {
        **existing,
        "scraped_at": fresh.get("scraped_at", existing.get("scraped_at")),
        "stats_refreshed": stats_refreshed,
        "profile_stats": existing.get("profile_stats", {}),
        "rankings": existing.get("rankings", {}),
        "daily_channel_metrics_60day": existing.get("daily_channel_metrics_60day", {}),
    }
    existing_previous_run = existing.get("previous_run")
    if isinstance(existing_previous_run, dict) and existing_previous_run:
        merged["previous_run"] = existing_previous_run

    if stats_refreshed:
        previous_run = _build_previous_run_snapshot(existing)
        if previous_run:
            merged["previous_run"] = previous_run
        else:
            merged.pop("previous_run", None)
        merged["profile_stats"] = fresh.get("profile_stats", existing.get("profile_stats", {}))
        merged["rankings"] = fresh.get("rankings", existing.get("rankings", {}))
        merged["daily_channel_metrics_60day"] = fresh.get(
            "daily_channel_metrics_60day",
            existing.get("daily_channel_metrics_60day", {}),
        )

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
        "previous_run": previous_run,
        "freshness_status": freshness_status,
        "is_stale": is_stale,
        "age_hours": age_hours,
    }
