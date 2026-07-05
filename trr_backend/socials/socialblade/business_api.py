"""Opt-in SocialBlade Business API adapter."""

from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import Any

import httpx

from trr_backend.socials.socialblade.service import sanitize_socialblade_handle, sanitize_socialblade_platform

_DEFAULT_BASE_URL = "https://matrix.sbapis.com/b"
_DEFAULT_TIMEOUT_SECONDS = 30.0


class SocialBladeBusinessApiUnavailable(RuntimeError):  # noqa: N818
    """Raised when the opt-in SocialBlade Business API path is not configured."""


def _env_truthy(name: str, *, default: bool = False) -> bool:
    raw = str(os.getenv(name) or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def socialblade_business_api_enabled() -> bool:
    return (
        _env_truthy("SOCIALBLADE_API_ENABLED")
        and bool(str(os.getenv("SOCIALBLADE_API_CLIENT_ID") or "").strip())
        and bool(str(os.getenv("SOCIALBLADE_API_TOKEN") or "").strip())
    )


def _first_number(*values: Any) -> float:
    for value in values:
        if value is None:
            continue
        if isinstance(value, (int, float)):
            return float(value)
        rendered = str(value).strip().replace(",", "")
        if not rendered:
            continue
        try:
            return float(rendered)
        except ValueError:
            continue
    return 0.0


def _first_string(*values: Any) -> str:
    for value in values:
        rendered = str(value or "").strip()
        if rendered:
            return rendered
    return ""


def _dict_at(source: dict[str, Any], *keys: str) -> dict[str, Any]:
    current: Any = source
    for key in keys:
        if not isinstance(current, dict):
            return {}
        current = current.get(key)
    return current if isinstance(current, dict) else {}


def _value_at(source: dict[str, Any], *keys: str) -> Any:
    current: Any = source
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _profile_payload(payload: dict[str, Any]) -> dict[str, Any]:
    for key in ("data", "result", "results", "profile", "account"):
        value = payload.get(key)
        if isinstance(value, dict):
            return value
    return payload


def _daily_points(profile: dict[str, Any], *, followers_key: str) -> list[dict[str, Any]]:
    candidates = [
        profile.get("daily"),
        profile.get("history"),
        _value_at(profile, "statistics", "daily"),
        _value_at(profile, "statistics", "history"),
    ]
    rows = next((value for value in candidates if isinstance(value, list)), [])
    points: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        date = _first_string(row.get("date"), row.get("day"), row.get("created_at"))
        followers = _first_number(row.get(followers_key), row.get("followers"), row.get("subscribers"))
        if not date:
            continue
        points.append({"date": date[:10], "followers": int(followers)})
    return points


def _build_payload(handle: str, platform: str, raw_payload: dict[str, Any]) -> dict[str, Any]:
    profile = _profile_payload(raw_payload)
    totals = _dict_at(profile, "statistics", "total") or _dict_at(profile, "total") or profile
    ranks = _dict_at(profile, "ranks") or _dict_at(profile, "rankings")
    followers_key = "followers" if platform != "youtube" else "subscribers"
    followers = int(_first_number(totals.get(followers_key), totals.get("followers"), profile.get(followers_key)))
    following = int(_first_number(totals.get("following"), profile.get("following")))
    media_count = int(
        _first_number(totals.get("media"), totals.get("uploads"), totals.get("posts"), profile.get("media"))
    )
    average_likes = _first_number(totals.get("average_likes"), profile.get("average_likes"))
    average_comments = _first_number(totals.get("average_comments"), profile.get("average_comments"))
    engagement_rate = _first_number(totals.get("engagement_rate"), profile.get("engagement_rate"))
    points = _daily_points(profile, followers_key=followers_key)
    now = datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")
    chart = None
    if points:
        chart = {
            "frequency": "daily",
            "metric": "total_followers",
            "total_data_points": len(points),
            "date_range": {"from": points[0]["date"], "to": points[-1]["date"]},
            "data": points,
        }
    return {
        "username": _first_string(profile.get("username"), profile.get("handle"), handle).lstrip("@") or handle,
        "platform": platform,
        "scraped_at": now,
        "stats_refreshed": True,
        "history_source": "business_api",
        "profile_stats": {
            "followers": followers,
            "following": following,
            "media_count": media_count,
            "engagement_rate": f"{engagement_rate:.2f}%",
            "average_likes": average_likes,
            "average_comments": average_comments,
        },
        "rankings": {
            "grade": _first_string(profile.get("grade"), ranks.get("grade")),
            "sb_rank": _first_string(ranks.get("sb"), ranks.get("socialblade"), "0"),
            "followers_rank": _first_string(ranks.get("followers"), ranks.get(followers_key), "0"),
            "engagement_rate_rank": _first_string(ranks.get("engagement_rate"), "0"),
        },
        "daily_channel_metrics_60day": {
            "row_count": len(points),
            "period": "official_api",
            "source": "business_api",
        },
        "daily_total_followers_chart": chart,
        "runtime_metadata": {
            "source": "socialblade_business_api",
            "business_api_enabled": True,
            "business_api_base_url": str(os.getenv("SOCIALBLADE_API_BASE_URL") or _DEFAULT_BASE_URL).rstrip("/"),
        },
    }


def scrape_socialblade_business_api(handle: str, *, platform: str = "instagram") -> dict[str, Any]:
    normalized_platform = sanitize_socialblade_platform(platform)
    safe_handle = sanitize_socialblade_handle(handle, platform=normalized_platform)
    if not safe_handle:
        raise ValueError("Invalid SocialBlade handle")
    if normalized_platform != "instagram":
        raise SocialBladeBusinessApiUnavailable(f"SocialBlade Business API fallback is not mapped for {platform}")
    if not socialblade_business_api_enabled():
        raise SocialBladeBusinessApiUnavailable("SocialBlade Business API fallback is not configured")

    base_url = str(os.getenv("SOCIALBLADE_API_BASE_URL") or _DEFAULT_BASE_URL).rstrip("/")
    raw_timeout = str(os.getenv("SOCIALBLADE_API_TIMEOUT_SECONDS") or "").strip()
    try:
        timeout_seconds = float(raw_timeout or _DEFAULT_TIMEOUT_SECONDS)
    except ValueError:
        timeout_seconds = _DEFAULT_TIMEOUT_SECONDS
    headers = {
        "clientid": str(os.getenv("SOCIALBLADE_API_CLIENT_ID") or "").strip(),
        "token": str(os.getenv("SOCIALBLADE_API_TOKEN") or "").strip(),
    }
    with httpx.Client(timeout=max(5.0, timeout_seconds)) as client:
        response = client.get(
            f"{base_url}/{normalized_platform}/statistics", headers=headers, params={"query": safe_handle}
        )
        response.raise_for_status()
        payload = response.json()
    if not isinstance(payload, dict):
        raise RuntimeError("SocialBlade Business API returned a non-object payload")
    return _build_payload(safe_handle, normalized_platform, payload)


def scrape_socialblade_business_api_if_configured(handle: str, *, platform: str = "instagram") -> dict[str, Any] | None:
    if not socialblade_business_api_enabled():
        return None
    return scrape_socialblade_business_api(handle, platform=platform)
