"""Repository for SocialBlade growth data (pipeline.socialblade_growth_data)."""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import Any

from psycopg2.extras import Json

from trr_backend.db import pg

logger = logging.getLogger(__name__)


def socialblade_growth_table_exists() -> bool:
    """Return whether the SocialBlade growth table exists in the current database."""
    row = pg.fetch_one("SELECT to_regclass('pipeline.socialblade_growth_data') AS relation_name")
    return bool(row and row.get("relation_name"))


def get_growth_data(person_id: str, handle: str) -> dict[str, Any] | None:
    """Fetch stored SocialBlade data for a person+handle pair."""
    row = pg.fetch_one(
        "SELECT * FROM pipeline.socialblade_growth_data WHERE person_id = %s AND instagram_handle = %s",
        [person_id, handle],
    )
    if not row:
        return None
    return _row_to_response(row)


def upsert_growth_data(person_id: str, handle: str, data: dict[str, Any]) -> dict[str, Any]:
    """Upsert merged SocialBlade data. Returns the stored row."""
    rows = pg.execute_returning(
        "INSERT INTO pipeline.socialblade_growth_data "
        "(person_id, instagram_handle, scraped_at, stats_refreshed, "
        " profile_stats, rankings, daily_channel_metrics_60day, "
        " daily_total_followers_chart, raw_response, updated_at) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, now()) "
        "ON CONFLICT (person_id, instagram_handle) DO UPDATE SET "
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
            handle,
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

    if stats_refreshed:
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
        "username": row.get("instagram_handle", ""),
        "platform": "instagram",
        "scraped_at": scraped_at_value,
        "stats_refreshed": bool(row.get("stats_refreshed", False)),
        "history_source": ((row.get("raw_response") or {}) if isinstance(row.get("raw_response"), dict) else {}).get(
            "history_source"
        ),
        "profile_stats": row.get("profile_stats", {}),
        "rankings": row.get("rankings", {}),
        "daily_channel_metrics_60day": row.get("daily_channel_metrics_60day", {}),
        "daily_total_followers_chart": row.get("daily_total_followers_chart"),
        "freshness_status": freshness_status,
        "is_stale": is_stale,
        "age_hours": age_hours,
    }
