"""SocialBlade scraper entrypoints and legacy browser fallbacks."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import sys
from collections import OrderedDict
from datetime import UTC, datetime
from typing import Any
from urllib.parse import parse_qs, quote, unquote, urlparse

logger = logging.getLogger(__name__)


class SocialBladeEndpointError(RuntimeError):
    """Raised when a SocialBlade endpoint returns a non-200 response."""

    def __init__(self, endpoint: str, status: int):
        self.endpoint = endpoint
        self.status = status
        super().__init__(f"SocialBlade endpoint {endpoint} returned HTTP {status}")


def _log(msg: str) -> None:
    logger.info(msg)
    print(msg, file=sys.stderr)


_JS_EXTRACT_TABLE = """(() => {
    const datePattern = /^(Mon|Tue|Wed|Thu|Fri|Sat|Sun)?\\s*\\d{4}-\\d{2}-\\d{2}$/;
    const titleCase = value => value.replace(/\\b\\w/g, letter => letter.toUpperCase());
    for (const table of [...document.querySelectorAll("table")]) {
        const rows = [...table.querySelectorAll("tr")];
        if (rows.length === 0) continue;
        const headers = [...rows[0].querySelectorAll("th,td")]
            .map(cell => cell.textContent.trim())
            .filter(Boolean);
        if (headers.length === 0) continue;
        const normalizedHeaders = headers.map(header => header.toLowerCase());
        const isMetricSummaryTable =
            normalizedHeaders.length === 4 &&
            normalizedHeaders[0] === "date" &&
            normalizedHeaders.includes("following") &&
            (
                normalizedHeaders.includes("followers") ||
                normalizedHeaders.includes("subscribers") ||
                normalizedHeaders.includes("likes")
            );
        const expandedHeaders = isMetricSummaryTable
            ? [
                "Date",
                `${titleCase(headers[1])} Delta`,
                `${titleCase(headers[1])} Total`,
                `${titleCase(headers[2])} Delta`,
                `${titleCase(headers[2])} Total`,
                `${titleCase(headers[3])} Delta`,
                `${titleCase(headers[3])} Total`,
            ]
            : headers;
        if ((expandedHeaders[0] || "").toLowerCase() !== "date") continue;
        const data = rows.slice(1)
            .map(row => [...row.querySelectorAll("td")].map(td => td.textContent.trim()))
            .filter(
                cells =>
                    cells.length >= expandedHeaders.length &&
                    datePattern.test((cells[0] || "").replace(/^\\s+|\\s+$/g, ""))
            )
            .map(cells => Object.fromEntries(expandedHeaders.map((header, index) => [header, cells[index] || ""])));
        if (data.length > 0) return { headers: expandedHeaders, data };
    }
    return null;
})()"""


_ACCESS_DENIED_PATTERNS = (
    "access denied",
    "error reference number: 1020",
    "social blade access denied",
)
_DATE_PREFIX_PATTERN = re.compile(r"^(?:(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s*)?(\d{4}-\d{2}-\d{2})$")
_TRPC_FETCH_JS = """async ({ endpoint }) => {
    const response = await fetch(endpoint, {
        headers: { accept: "application/json, text/plain, */*" },
    });
    return {
        status: response.status,
        text: await response.text(),
    };
}"""
_SOCIALBLADE_RANGE_OPTIONS = (
    "Last 14 Days",
    "Last 30 Days",
    "Last 60 Days",
    "Last 180 Days",
    "Last 365 Days",
    "Last 3 Years",
)
_SOCIALBLADE_CHART_INTERVAL_OPTIONS = ("Daily", "Weekly", "Monthly")
_SOCIALBLADE_CHART_METRIC_OPTIONS = ("Gained", "Total", "Averages")
_SOCIALBLADE_AUTHENTICATED_HISTORY_LIMIT = 60
_SOCIALBLADE_AUTHENTICATED_HISTORY_SOURCES = frozenset(
    {"authenticated_api", "page_trpc_capture", "page_trpc_capture_short"}
)
_SOCIALBLADE_DAILY_TOTAL_CHART_LIMIT = 1096
_PLATFORM_ROUTE_SEGMENTS = {
    "instagram": "user",
    "facebook": "user",
    "tiktok": "user",
    "youtube": "handle",
}
_KNOWN_PROFILE_URL_HOST_PREFIXES = (
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
_PROFILE_STAT_LABELS = {
    "instagram": {
        "followers": ("Followers",),
        "following": ("Following",),
        "media_count": ("Media Count", "Posts"),
        "engagement_rate": ("Engagement Rate",),
        "average_likes": ("Average Likes",),
        "average_comments": ("Average Comments",),
        "chart_metric_label": "Followers",
    },
    "facebook": {
        "followers": ("Likes", "Followers"),
        "following": ("Talking About", "People Talking About This"),
        "media_count": ("Posts", "Uploads"),
        "engagement_rate": ("Engagement Rate",),
        "average_likes": ("Average Reactions", "Average Likes"),
        "average_comments": ("Average Comments",),
        "chart_metric_label": "Likes",
    },
    "youtube": {
        "followers": ("Subscribers",),
        "following": ("Video Views", "Views"),
        "media_count": ("Uploads", "Videos"),
        "engagement_rate": ("Engagement Rate",),
        "average_likes": ("Average Views", "Average Likes"),
        "average_comments": ("Average Comments",),
        "chart_metric_label": "Subscribers",
    },
    "tiktok": {
        "followers": ("Followers",),
        "following": ("Following",),
        "media_count": ("Likes",),
        "engagement_rate": ("Engagement Rate",),
        "average_likes": ("Average Likes",),
        "average_comments": ("Average Comments",),
        "chart_metric_label": "Followers",
    },
}
_HISTORY_THIRD_METRIC_FIELDS = {
    "instagram": ("media_count", "posts", "uploads"),
    "facebook": ("media_count", "posts", "uploads"),
    "youtube": ("media_count", "uploads", "videos"),
    "tiktok": ("likes", "like_count", "heart_count", "digg_count", "media_count"),
}


def _parse_int(value: str) -> int:
    return int(re.sub(r"[^0-9-]", "", value) or "0")


def _parse_float(value: str) -> float:
    return float(re.sub(r"[^0-9.\-]", "", value) or "0")


def _parse_metric_number(value: str) -> int:
    rendered = str(value or "").strip().replace(",", "")
    if not rendered:
        return 0
    match = re.search(r"(-?\d+(?:\.\d+)?)\s*([kmb])\b", rendered, re.IGNORECASE)
    if not match:
        match = re.search(r"(-?\d+(?:\.\d+)?)", rendered)
    if not match:
        return 0
    amount = float(match.group(1))
    suffix = str(match.group(2) if len(match.groups()) >= 2 and match.group(2) else "").lower()
    multiplier = {"k": 1_000, "m": 1_000_000, "b": 1_000_000_000}.get(suffix, 1)
    return int(round(amount * multiplier))


def _first_present_metric_value(source: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        value = source.get(key)
        if value is None or value == "":
            continue
        return value
    return 0


def _default_profile_stat_labels(platform: str) -> dict[str, str]:
    config = _PROFILE_STAT_LABELS.get(platform, _PROFILE_STAT_LABELS["instagram"])
    return {
        "followers": config["followers"][0],
        "following": config["following"][0],
        "media_count": config["media_count"][0],
        "engagement_rate": config["engagement_rate"][0],
        "average_likes": config["average_likes"][0],
        "average_comments": config["average_comments"][0],
        "chart_metric_label": str(config["chart_metric_label"]),
    }


def _history_third_metric_label(platform: str) -> str:
    return _default_profile_stat_labels(platform)["media_count"]


def _history_third_metric_keys(platform: str) -> tuple[str, ...]:
    normalized_platform = str(platform or "instagram").strip().lower()
    return _HISTORY_THIRD_METRIC_FIELDS.get(normalized_platform, _HISTORY_THIRD_METRIC_FIELDS["instagram"])


def _normalize_body_lines(body_text: str) -> list[str]:
    return [line.strip() for line in body_text.splitlines() if line.strip()]


def _find_line_after(lines: list[str], label: str) -> str:
    normalized = label.strip().lower()
    for index, line in enumerate(lines):
        if line.strip().lower() != normalized:
            continue
        if index + 1 < len(lines):
            return lines[index + 1].strip()
        break
    return ""


def _find_line_before(lines: list[str], label: str) -> str:
    normalized = label.strip().lower()
    for index, line in enumerate(lines):
        if line.strip().lower() != normalized:
            continue
        if index > 0:
            return lines[index - 1].strip()
        break
    return ""


def _extract_body_text(page: Any) -> str:
    try:
        return page.locator("body").inner_text(timeout=2_000)
    except Exception:  # noqa: BLE001
        return ""


def _page_access_denied(body_text: str) -> bool:
    normalized = body_text.lower()
    return any(marker in normalized for marker in _ACCESS_DENIED_PATTERNS)


def _socialblade_profile_url(platform: str, handle: str) -> str:
    normalized_platform = str(platform or "instagram").strip().lower()
    normalized_handle = _normalize_socialblade_profile_handle(normalized_platform, handle)
    if normalized_platform == "youtube" and normalized_handle.startswith(("channel/", "user/", "c/", "handle/")):
        return f"https://socialblade.com/youtube/{normalized_handle}"
    if normalized_platform == "youtube" and normalized_handle.upper().startswith("UC"):
        return f"https://socialblade.com/youtube/channel/{normalized_handle}"
    if normalized_platform == "youtube" and normalized_handle.startswith(("user/", "c/")):
        return f"https://socialblade.com/youtube/{normalized_handle}"
    route_segment = _PLATFORM_ROUTE_SEGMENTS.get(normalized_platform, "user")
    return f"https://socialblade.com/{normalized_platform}/{route_segment}/{normalized_handle}"


def _normalize_socialblade_profile_handle(platform: str, handle: str) -> str:
    rendered = str(handle or "").strip()
    lowered = rendered.lower()
    if "://" not in rendered and lowered.startswith(_KNOWN_PROFILE_URL_HOST_PREFIXES):
        rendered = f"https://{rendered}"

    parsed = urlparse(rendered)
    if not parsed.netloc:
        return _clean_socialblade_handle(platform, rendered)

    host = parsed.netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    segments = [unquote(segment).strip() for segment in parsed.path.split("/") if segment.strip()]
    if not segments:
        return ""

    if host.endswith("socialblade.com"):
        if platform == "youtube" and len(segments) >= 3 and segments[1].lower() in {"channel", "user", "c", "handle"}:
            return f"{segments[1].lower()}/{_clean_socialblade_handle(platform, segments[2])}"
        if len(segments) >= 3 and segments[1].lower() in {"user", "handle", "channel", "c"}:
            return _clean_socialblade_handle(platform, segments[2])
        return _clean_socialblade_handle(platform, segments[-1])

    if host.endswith(("instagram.com", "threads.net")):
        return _clean_socialblade_handle(platform, segments[0])

    if host.endswith("tiktok.com"):
        tiktok_handle = next((segment for segment in segments if segment.startswith("@")), segments[0])
        return _clean_socialblade_handle(platform, tiktok_handle)

    if host.endswith(("youtube.com", "youtu.be")):
        first = segments[0].lower()
        if platform == "youtube" and first in {"channel", "user", "c", "handle"} and len(segments) >= 2:
            return f"{first}/{_clean_socialblade_handle(platform, segments[1])}"
        return _clean_socialblade_handle(platform, segments[0])

    if host.endswith(("facebook.com", "fb.com")):
        if segments[0].lower() == "profile.php":
            facebook_handle = str((parse_qs(parsed.query).get("id") or [""])[0]).strip()
        else:
            facebook_handle = segments[0]
        return _clean_socialblade_handle(platform, facebook_handle)

    return _clean_socialblade_handle(platform, segments[-1])


def _clean_socialblade_handle(platform: str, value: str) -> str:
    rendered = str(value or "").strip().lstrip("@")
    if platform == "youtube" and rendered.upper().startswith("UC"):
        return re.sub(r"[^a-zA-Z0-9._-]", "", rendered)
    return re.sub(r"[^a-zA-Z0-9._-]", "", rendered.lower())


def _label_value_after(lines: list[str], labels: tuple[str, ...]) -> tuple[str, str]:
    for label in labels:
        value = _find_line_after(lines, label)
        if value:
            return label, value
    return labels[0], ""


def _extract_profile_stats_from_body_text(
    body_text: str,
    platform: str,
) -> tuple[dict[str, Any], dict[str, str], dict[str, str]]:
    lines = _normalize_body_lines(body_text)
    config = _PROFILE_STAT_LABELS.get(platform, _PROFILE_STAT_LABELS["instagram"])
    followers_label, followers_value = _label_value_after(lines, config["followers"])
    following_label, following_value = _label_value_after(lines, config["following"])
    media_count_label, media_count_value = _label_value_after(lines, config["media_count"])
    engagement_label, engagement_value = _label_value_after(lines, config["engagement_rate"])
    average_likes_label, average_likes_value = _label_value_after(lines, config["average_likes"])
    average_comments_label, average_comments_value = _label_value_after(lines, config["average_comments"])
    stats: dict[str, Any] = {
        "followers": _parse_metric_number(followers_value),
        "following": _parse_metric_number(following_value),
        "media_count": _parse_metric_number(media_count_value),
        "engagement_rate": engagement_value or "0%",
        "average_likes": _parse_float(average_likes_value),
        "average_comments": _parse_float(average_comments_value),
    }
    rankings: dict[str, str] = {
        "grade": "",
        "sb_rank": _find_line_before(lines, "SB Rank") or _find_line_after(lines, "SB Rank"),
        "followers_rank": _find_line_before(lines, "Followers Rank") or _find_line_after(lines, "Followers Rank"),
        "engagement_rate_rank": (
            _find_line_before(lines, "Engagement Rate Rank") or _find_line_after(lines, "Engagement Rate Rank")
        ),
    }

    grade_value = _find_line_before(lines, "Grade")
    if re.fullmatch(r"[A-F][+-]?", grade_value):
        rankings["grade"] = grade_value

    labels = _default_profile_stat_labels(platform)
    labels.update(
        {
            "followers": followers_label,
            "following": following_label,
            "media_count": media_count_label,
            "engagement_rate": engagement_label,
            "average_likes": average_likes_label,
            "average_comments": average_comments_label,
            "chart_metric_label": str(config["chart_metric_label"]),
        }
    )
    return stats, rankings, labels


def _normalize_table_data(table_data: dict[str, Any] | None, body_text: str) -> dict[str, Any]:
    headers = [str(item).strip() for item in list((table_data or {}).get("headers") or []) if str(item).strip()]
    rows = list((table_data or {}).get("data") or [])
    lines = _normalize_body_lines(body_text)
    period = _find_line_after(lines, "Daily Channel Metrics") or "Last 14 Days"
    return {
        "period": period,
        "row_count": len(rows),
        "headers": headers,
        "data": rows,
    }


def _followers_chart_from_table(metrics: dict[str, Any], *, metric_label: str) -> dict[str, Any] | None:
    total_column_candidates = [
        f"{metric_label} Total",
        metric_label,
    ]
    headers = [str(header).strip() for header in list(metrics.get("headers") or [])]
    total_column = next((candidate for candidate in total_column_candidates if candidate in headers), None)
    if not total_column:
        return None
    chart_points: list[dict[str, Any]] = []
    for row in metrics.get("data") or []:
        raw_date = str(row.get("Date") or "").strip()
        raw_total = str(row.get(total_column) or "").strip()
        if not raw_date or not raw_total:
            continue
        match = _DATE_PREFIX_PATTERN.match(raw_date)
        if not match:
            continue
        chart_points.append(
            {
                "date": match.group(1),
                "followers": _parse_metric_number(raw_total),
            }
        )
    if not chart_points:
        return None
    return {
        "frequency": "daily",
        "metric": "total_followers",
        "total_data_points": len(chart_points),
        "date_range": {
            "from": chart_points[0]["date"],
            "to": chart_points[-1]["date"],
        },
        "data": chart_points,
    }


def _followers_chart_from_points(points_by_date: dict[str, int]) -> dict[str, Any] | None:
    sorted_points = [
        {"date": date, "followers": followers}
        for date, followers in sorted(points_by_date.items(), key=lambda item: item[0])
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", date)
    ]
    if not sorted_points:
        return None
    return {
        "frequency": "daily",
        "metric": "total_followers",
        "total_data_points": len(sorted_points),
        "date_range": {
            "from": sorted_points[0]["date"],
            "to": sorted_points[-1]["date"],
        },
        "data": sorted_points,
    }


def _merge_followers_charts(*charts: dict[str, Any] | None) -> dict[str, Any] | None:
    """Merge follower charts, letting later sources correct or extend earlier ones."""
    merged_points: dict[str, int] = {}
    for chart in charts:
        if not isinstance(chart, dict):
            continue
        for point in chart.get("data") or []:
            if not isinstance(point, dict):
                continue
            date = str(point.get("date") or "")[:10]
            if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date):
                continue
            merged_points[date] = _parse_metric_number(str(point.get("followers") or "0"))
    return _followers_chart_from_points(merged_points)


def _build_total_followers_chart_from_total_rows(
    total_rows: list[dict[str, Any]],
    *,
    metric_key: str = "followers",
) -> dict[str, Any] | None:
    """Build a daily total-followers chart from SocialBlade's Total chart rows."""
    points_by_date: dict[str, int] = {}
    for row in sorted(total_rows, key=lambda item: str(item.get("date") or "")):
        if not isinstance(row, dict):
            continue
        date = str(row.get("date") or "")[:10]
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date):
            continue
        points_by_date[date] = _parse_metric_number(str(row.get(metric_key) or "0"))
    return _followers_chart_from_points(points_by_date)


def _set_socialblade_listbox_option(page: Any, current_options: tuple[str, ...], target_option: str) -> bool:
    buttons = page.locator('button[id*="headlessui-listbox-button"]')
    button_count = buttons.count()
    for index in range(button_count):
        button = buttons.nth(index)
        label = button.inner_text(timeout=1_000).strip()
        if label not in current_options:
            continue
        if label == target_option:
            return False
        button.click()
        option = page.get_by_role("option", name=target_option, exact=True)
        try:
            if option.get_attribute("aria-disabled", timeout=1_000) == "true":
                page.keyboard.press("Escape")
                return False
            if option.get_attribute("data-disabled", timeout=1_000) is not None:
                page.keyboard.press("Escape")
                return False
        except Exception:  # noqa: BLE001
            pass
        option.click()
        page.wait_for_timeout(1_000)
        return True
    raise RuntimeError(f"Could not find SocialBlade control for {target_option}")


def _configure_socialblade_page_fallback_state(page: Any) -> None:
    """Force the page fallback into the expected UI state.

    SocialBlade defaults to a 14-day metrics table and Monthly/Gained charts.
    When we fall back to page scraping, explicitly switch to the richer view
    so the table and visible chart state match the stored dataset shape.
    """

    updates: list[str] = []
    if _set_socialblade_listbox_option(page, _SOCIALBLADE_RANGE_OPTIONS, "Last 60 Days"):
        updates.append("range=Last 60 Days")
    if _set_socialblade_listbox_option(page, _SOCIALBLADE_CHART_INTERVAL_OPTIONS, "Daily"):
        updates.append("chart_interval=Daily")
    if _set_socialblade_listbox_option(page, _SOCIALBLADE_CHART_METRIC_OPTIONS, "Total"):
        updates.append("chart_metric=Total")
    if updates:
        _log(f"Configured SocialBlade fallback controls: {', '.join(updates)}")


def _format_ordinal_rank(value: int) -> str:
    if value <= 0:
        return ""
    suffix = "th"
    if value % 100 not in {11, 12, 13}:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(value % 10, "th")
    return f"{value:,}{suffix}"


def _coerce_trpc_json(raw_payload: str, *, endpoint: str) -> Any:
    try:
        payload = json.loads(raw_payload)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"SocialBlade returned non-JSON data for {endpoint}") from exc
    return payload


def _unwrap_trpc_result(payload: Any, *, endpoint: str, index: int | None = None) -> Any:
    item = payload
    if isinstance(payload, list):
        if index is None:
            index = 0
        if index >= len(payload):
            raise RuntimeError(f"SocialBlade tRPC payload missing index {index} for {endpoint}")
        item = payload[index]
    if not isinstance(item, dict):
        raise RuntimeError(f"Unexpected SocialBlade tRPC payload for {endpoint}")
    error = item.get("error")
    if error:
        message = (
            error.get("json", {}).get("message") or error.get("message") or f"SocialBlade tRPC error for {endpoint}"
        )
        raise RuntimeError(str(message))
    result = item.get("result", {}).get("data", {}).get("json")
    if result is None:
        raise RuntimeError(f"SocialBlade tRPC response missing result data for {endpoint}")
    return result


def _fetch_trpc_result(page: Any, endpoint: str, *, index: int | None = None) -> Any:
    response = page.evaluate(_TRPC_FETCH_JS, {"endpoint": endpoint})
    status = int(response.get("status") or 0)
    payload = _coerce_trpc_json(str(response.get("text") or ""), endpoint=endpoint)
    if status != 200:
        raise SocialBladeEndpointError(endpoint, status)
    return _unwrap_trpc_result(payload, endpoint=endpoint, index=index)


def _search_socialblade_profile(page: Any, handle: str, *, platform: str = "instagram") -> dict[str, Any]:
    normalized_platform = str(platform or "instagram").strip().lower() or "instagram"
    endpoint = f"/api/trpc/{normalized_platform}.search?input=" + quote(
        json.dumps({"json": {"query": handle}}, separators=(",", ":"))
    )
    result = _fetch_trpc_result(page, endpoint)
    profile = result.get("platformResult") if isinstance(result, dict) else None
    if not isinstance(profile, dict) or not str(profile.get("id") or "").strip():
        raise RuntimeError(f"SocialBlade could not resolve {normalized_platform}/@{handle}")
    return profile


def _fetch_socialblade_user(page: Any, creator_id: str, *, platform: str = "instagram") -> dict[str, Any]:
    normalized_platform = str(platform or "instagram").strip().lower() or "instagram"
    endpoint = f"/api/trpc/{normalized_platform}.user?input=" + quote(
        json.dumps({"json": {"id": creator_id}}, separators=(",", ":"))
    )
    result = _fetch_trpc_result(page, endpoint)
    if not isinstance(result, dict):
        raise RuntimeError(f"SocialBlade {normalized_platform} user endpoint returned an unexpected payload")
    return result


def _fetch_socialblade_history(
    page: Any,
    creator_id: str,
    *,
    limit: int,
    platform: str = "instagram",
) -> list[dict[str, Any]]:
    normalized_platform = str(platform or "instagram").strip().lower() or "instagram"
    endpoint = f"/api/trpc/{normalized_platform}.user,{normalized_platform}.history?batch=1&input=" + quote(
        json.dumps(
            {
                "0": {"json": {"id": creator_id}},
                "1": {"json": {"id": creator_id, "limit": limit}},
            },
            separators=(",", ":"),
        )
    )
    result = _fetch_trpc_result(page, endpoint, index=1)
    if not isinstance(result, list):
        raise RuntimeError(f"SocialBlade {normalized_platform} history endpoint returned an unexpected payload")
    return result


def _fetch_socialblade_period_deltas(
    page: Any,
    creator_id: str,
    *,
    period: str,
    platform: str = "instagram",
) -> list[dict[str, Any]]:
    normalized_platform = str(platform or "instagram").strip().lower() or "instagram"
    endpoint = f"/api/trpc/{normalized_platform}.monthly?batch=1&input=" + quote(
        json.dumps(
            {"0": {"json": {"id": creator_id, "period": period}}},
            separators=(",", ":"),
        )
    )
    result = _fetch_trpc_result(page, endpoint, index=0)
    if not isinstance(result, list):
        raise RuntimeError(f"SocialBlade {normalized_platform} {period} endpoint returned an unexpected payload")
    return result


def _fetch_socialblade_daily_total_rows(
    page: Any,
    creator_id: str,
    *,
    platform: str = "instagram",
    limit: int = _SOCIALBLADE_DAILY_TOTAL_CHART_LIMIT,
) -> list[dict[str, Any]]:
    normalized_platform = str(platform or "instagram").strip().lower() or "instagram"
    endpoint = f"/api/trpc/{normalized_platform}.monthly?batch=1&input=" + quote(
        json.dumps(
            {
                "0": {
                    "json": {
                        "id": creator_id,
                        "limit": limit,
                        "period": "daily",
                        "type": "total",
                    }
                }
            },
            separators=(",", ":"),
        )
    )
    result = _fetch_trpc_result(page, endpoint, index=0)
    if not isinstance(result, list):
        raise RuntimeError(f"SocialBlade {normalized_platform} daily total endpoint returned an unexpected payload")
    return result


def _scrape_authenticated_api(
    page: Any,
    handle: str,
    *,
    platform: str = "instagram",
) -> tuple[dict[str, Any], dict[str, str], dict[str, Any], dict[str, Any] | None]:
    profile = _search_socialblade_profile(page, handle, platform=platform)
    creator_id = str(profile.get("id") or "").strip()
    user_payload = _fetch_socialblade_user(page, creator_id, platform=platform)
    stats, rankings = _build_profile_stats_from_user_payload(user_payload, platform=platform)
    history_rows = _fetch_socialblade_history(page, creator_id, limit=60, platform=platform)
    metrics = _history_rows_to_metrics(history_rows, limit=60, platform=platform)
    daily_total_rows = _fetch_socialblade_daily_total_rows(page, creator_id, platform=platform)
    chart_data = _merge_followers_charts(
        _build_total_followers_chart_from_total_rows(daily_total_rows),
        _followers_chart_from_table(metrics, metric_label=_default_profile_stat_labels(platform)["chart_metric_label"]),
    )
    return stats, rankings, metrics, chart_data


def _has_socialblade_login_credentials() -> bool:
    return bool((os.getenv("SOCIALBLADE_EMAIL") or "").strip() and (os.getenv("SOCIALBLADE_PASSWORD") or "").strip())


def _should_retry_in_visible_shared_browser(error: Exception | None) -> bool:
    if isinstance(error, SocialBladeEndpointError):
        if error.status in {401, 403}:
            return True
        if error.status == 412 and ".monthly" in error.endpoint:
            return True
        if error.status == 403 and "instagram.search" in error.endpoint:
            return True
    rendered = str(error or "").lower()
    if any(marker in rendered for marker in _ACCESS_DENIED_PATTERNS):
        return True
    if "access-denied" in rendered or "cloudflare" in rendered and "1020" in rendered:
        return True
    if re.search(r"\b(?:http\s*)?(?:401|403)\b", rendered) and "socialblade" in rendered:
        return True
    if ".monthly" in rendered and "http 412" in rendered:
        return True
    if "incomplete profile stats or daily metrics data" in rendered:
        return True
    return "returned non-json data for /api/trpc/instagram.search" in rendered or (
        "returned non-json data for /api/trpc/tiktok.search" in rendered
    )


def _format_scrape_failure_message(error: Exception | None) -> str:
    if _should_retry_in_visible_shared_browser(error):
        return (
            "SocialBlade scrape failed: SocialBlade challenged the authenticated API "
            "before profile history could be fetched"
        )

    rendered = str(error or "").strip()
    if "turnstile" in rendered.lower():
        return "SocialBlade scrape failed: visible browser session could not complete challenge"
    if rendered.startswith("SocialBlade scrape failed:"):
        return rendered
    if rendered:
        return f"SocialBlade scrape failed: {rendered}"
    return "SocialBlade scrape failed: incomplete profile stats or daily metrics data"


def _socialblade_payload_needs_login_retry(payload: dict[str, Any] | None) -> bool:
    if not isinstance(payload, dict):
        return False
    metrics = payload.get("daily_channel_metrics_60day")
    if not isinstance(metrics, dict):
        return True
    try:
        row_count = int(metrics.get("row_count") or 0)
    except (TypeError, ValueError):
        row_count = 0
    chart = payload.get("daily_total_followers_chart")
    try:
        chart_points = int(chart.get("total_data_points") or 0) if isinstance(chart, dict) else 0
    except (TypeError, ValueError):
        chart_points = 0
    history_source = str(payload.get("history_source") or "").strip()
    period = str(metrics.get("period") or "").strip()
    if history_source not in _SOCIALBLADE_AUTHENTICATED_HISTORY_SOURCES and re.search(
        r"\b(?:14|30|31)\s+days\b", period, re.IGNORECASE
    ):
        return True
    if chart_points > row_count:
        return False
    runtime_metadata = payload.get("runtime_metadata")
    control_updates = runtime_metadata.get("capture_control_updates") if isinstance(runtime_metadata, dict) else None
    selected_expected_chart_controls = (
        isinstance(control_updates, dict)
        and str(control_updates.get("last60Days") or "") in {"selected", "already_selected"}
        and str(control_updates.get("daily") or "") in {"selected", "already_selected"}
        and str(control_updates.get("total") or "") in {"selected", "already_selected"}
    )
    if row_count > 0 and chart_points > 0 and selected_expected_chart_controls:
        return False
    if 0 < row_count < _SOCIALBLADE_AUTHENTICATED_HISTORY_LIMIT:
        return True
    if row_count >= _SOCIALBLADE_AUTHENTICATED_HISTORY_LIMIT:
        return False
    return False


def _socialblade_payload_has_complete_page_capture(payload: dict[str, Any] | None) -> bool:
    if not isinstance(payload, dict):
        return False
    if str(payload.get("history_source") or "").strip() != "page_trpc_capture":
        return False
    metrics = payload.get("daily_channel_metrics_60day")
    if not isinstance(metrics, dict):
        return False
    try:
        return int(metrics.get("row_count") or 0) >= _SOCIALBLADE_AUTHENTICATED_HISTORY_LIMIT
    except (TypeError, ValueError):
        return False


def _socialblade_payload_has_authenticated_seed_session(payload: dict[str, Any] | None) -> bool:
    if not isinstance(payload, dict):
        return False
    runtime_metadata = payload.get("runtime_metadata")
    return bool(isinstance(runtime_metadata, dict) and runtime_metadata.get("seed_has_socialblade_session"))


def _mark_payload_as_degraded_attempt(payload: dict[str, Any], error: Exception) -> dict[str, Any]:
    """Keep partial scrape evidence when the authenticated retry cannot complete."""
    degraded = dict(payload)
    if _socialblade_payload_has_complete_page_capture(degraded):
        runtime_metadata = (
            degraded.get("runtime_metadata") if isinstance(degraded.get("runtime_metadata"), dict) else {}
        )
        degraded["runtime_metadata"] = {
            **runtime_metadata,
            "login_retry_failed": True,
            "login_retry_error": str(error),
        }
        return degraded
    metrics = degraded.get("daily_channel_metrics_60day")
    row_count = 0
    if isinstance(metrics, dict):
        try:
            row_count = int(metrics.get("row_count") or 0)
        except (TypeError, ValueError):
            row_count = 0
    if str(degraded.get("history_source") or "").strip() == "page_trpc_capture" and row_count < (
        _SOCIALBLADE_AUTHENTICATED_HISTORY_LIMIT
    ):
        degraded["history_source"] = "page_trpc_capture_short"
    degraded["stats_refreshed"] = False
    degraded["error"] = _format_scrape_failure_message(error)
    return degraded


def _build_profile_stats_from_user_payload(
    user: dict[str, Any],
    *,
    platform: str = "instagram",
) -> tuple[dict[str, Any], dict[str, str]]:
    followers = _parse_metric_number(str(user.get("followers") or "0"))
    following = _parse_metric_number(str(user.get("following") or "0"))
    media_count = _parse_metric_number(str(_first_present_metric_value(user, _history_third_metric_keys(platform))))
    engagement_rate_value = float(user.get("engagement_rate") or 0)
    average_likes = float(user.get("average_likes") or 0)
    average_comments = float(user.get("average_comments") or 0)
    ranks = user.get("ranks") or {}
    return (
        {
            "followers": followers,
            "following": following,
            "media_count": media_count,
            "engagement_rate": f"{engagement_rate_value:.2f}%",
            "average_likes": average_likes,
            "average_comments": average_comments,
        },
        {
            "grade": str(user.get("grade") or "").strip(),
            "sb_rank": _format_ordinal_rank(int(ranks.get("sb") or 0)),
            "followers_rank": _format_ordinal_rank(int(ranks.get("followers") or 0)),
            "engagement_rate_rank": _format_ordinal_rank(int(ranks.get("engagement_rate") or 0)),
        },
    )


def _history_rows_to_metrics(
    history_rows: list[dict[str, Any]],
    *,
    limit: int,
    platform: str = "instagram",
) -> dict[str, Any]:
    ordered_totals: OrderedDict[str, dict[str, int]] = OrderedDict()
    third_metric_keys = _history_third_metric_keys(platform)
    third_metric_label = _history_third_metric_label(platform)
    for row in sorted(history_rows, key=lambda item: str(item.get("date") or "")):
        date = str(row.get("date") or "")[:10]
        if not date:
            continue
        ordered_totals[date] = {
            "followers": _parse_int(str(row.get("followers") or "0")),
            "following": _parse_int(str(row.get("following") or "0")),
            "third_metric": _parse_int(str(_first_present_metric_value(row, third_metric_keys))),
        }

    previous: dict[str, int] | None = None
    rendered_rows: list[dict[str, str]] = []
    for date, totals in ordered_totals.items():
        rendered_rows.append(
            {
                "Date": date,
                "Followers Delta": str(totals["followers"] - (previous["followers"] if previous else 0)),
                "Followers Total": f"{totals['followers']:,}",
                "Following Delta": str(totals["following"] - (previous["following"] if previous else 0)),
                "Following Total": f"{totals['following']:,}",
                f"{third_metric_label} Delta": str(
                    totals["third_metric"] - (previous["third_metric"] if previous else 0)
                ),
                f"{third_metric_label} Total": f"{totals['third_metric']:,}",
            }
        )
        previous = totals

    return {
        "period": f"Last {min(limit, len(rendered_rows))} Days",
        "row_count": len(rendered_rows),
        "headers": [
            "Date",
            "Followers Delta",
            "Followers Total",
            "Following Delta",
            "Following Total",
            f"{third_metric_label} Delta",
            f"{third_metric_label} Total",
        ],
        "data": rendered_rows,
    }


def _build_total_followers_chart_from_daily_deltas(
    current_followers: int,
    daily_rows: list[dict[str, Any]],
) -> dict[str, Any] | None:
    if current_followers <= 0 or not daily_rows:
        return None

    deltas_by_date: OrderedDict[str, int] = OrderedDict()
    for row in sorted(daily_rows, key=lambda item: str(item.get("date") or "")):
        date = str(row.get("date") or "")[:10]
        if not date:
            continue
        deltas_by_date[date] = _parse_int(str(row.get("followers") or "0"))

    if not deltas_by_date:
        return None

    running_total = current_followers
    rendered_rows: list[dict[str, Any]] = []
    for date, delta in reversed(deltas_by_date.items()):
        rendered_rows.append({"date": date, "followers": running_total})
        running_total -= delta
    rendered_rows.reverse()

    return {
        "frequency": "daily",
        "metric": "total_followers",
        "total_data_points": len(rendered_rows),
        "date_range": {
            "from": rendered_rows[0]["date"],
            "to": rendered_rows[-1]["date"],
        },
        "data": rendered_rows,
    }


# ---------------------------------------------------------------------------
# Main scrape function
# ---------------------------------------------------------------------------


def _scrape_socialblade_in_context(
    context: Any,
    handle: str,
    *,
    platform: str,
    playwright: Any | None,
    cookies: list[dict[str, Any]] | None,
    allow_login_fallback: bool,
    allow_visible_browser_retry: bool,
) -> dict[str, Any]:
    from trr_backend.socials.socialblade.auth import (
        SOCIALBLADE_STEALTH_INIT_SCRIPT,
        normalize_socialblade_cookies,
    )

    sb_url = _socialblade_profile_url(platform, handle)
    context.add_init_script(SOCIALBLADE_STEALTH_INIT_SCRIPT)

    normalized_cookies = normalize_socialblade_cookies(cookies or [])
    if normalized_cookies:
        context.add_cookies(normalized_cookies)
        _log(f"Injected {len(normalized_cookies)} cookies")

    page = context.new_page()
    try:
        page.goto(sb_url, wait_until="domcontentloaded", timeout=30_000)
        page.wait_for_timeout(4_000)
        _log(f"Navigated to {sb_url}")

        body_text = _extract_body_text(page)
        if _page_access_denied(body_text):
            if allow_visible_browser_retry:
                _log("SocialBlade page was blocked by Cloudflare; retrying through visible shared Chrome session...")
                return scrape_socialblade_with_shared_browser_session(
                    handle,
                    platform=platform,
                    playwright=playwright,
                )
            raise RuntimeError("SocialBlade blocked by Cloudflare (1020 access denied)")

        chart_data = None
        metrics = None
        stats = None
        rankings = None
        history_source = "unavailable"

        authenticated_api_error: Exception | None = None
        profile_labels = _default_profile_stat_labels(platform)
        if platform in {"instagram", "tiktok"}:
            try:
                if platform == "instagram":
                    stats, rankings, metrics, chart_data = _scrape_authenticated_api(page, handle)
                else:
                    stats, rankings, metrics, chart_data = _scrape_authenticated_api(
                        page,
                        handle,
                        platform=platform,
                    )
                history_source = "authenticated_api"
                _log(
                    f"Authenticated API scrape: {stats['followers']} followers, "
                    f"{chart_data['total_data_points'] if chart_data else 0} daily total points"
                )
            except Exception as exc:  # noqa: BLE001
                authenticated_api_error = exc
                _log(f"Authenticated API scrape unavailable: {exc}")
                if allow_visible_browser_retry and _should_retry_in_visible_shared_browser(exc):
                    _log("Retrying SocialBlade scrape through visible shared Chrome session...")
                    return scrape_socialblade_with_shared_browser_session(
                        handle,
                        platform=platform,
                        playwright=playwright,
                    )

            if (not chart_data or not metrics) and allow_login_fallback and _has_socialblade_login_credentials():
                try:
                    _log("Attempting SocialBlade login fallback for authenticated API access...")
                    _do_login(page, context)
                    page.goto(sb_url, wait_until="domcontentloaded", timeout=30_000)
                    page.wait_for_timeout(4_000)
                    body_text = _extract_body_text(page)
                    if _page_access_denied(body_text):
                        raise RuntimeError("SocialBlade blocked by Cloudflare after login")
                    if platform == "instagram":
                        stats, rankings, metrics, chart_data = _scrape_authenticated_api(page, handle)
                    else:
                        stats, rankings, metrics, chart_data = _scrape_authenticated_api(
                            page,
                            handle,
                            platform=platform,
                        )
                    history_source = "authenticated_api"
                    _log(
                        f"Authenticated API scrape after login: {stats['followers']} followers, "
                        f"{chart_data['total_data_points'] if chart_data else 0} daily total points"
                    )
                except Exception as exc:  # noqa: BLE001
                    authenticated_api_error = exc
                    _log(f"Authenticated API scrape still unavailable after login: {exc}")

        if not chart_data or not metrics:
            try:
                _configure_socialblade_page_fallback_state(page)
                body_text = _extract_body_text(page)
            except Exception as exc:  # noqa: BLE001
                _log(f"Could not configure SocialBlade fallback controls: {exc}")

        if not stats or not rankings:
            _log("Extracting profile stats and rankings from page text...")
            stats, rankings, profile_labels = _extract_profile_stats_from_body_text(body_text, platform)
            _log(f"Stats: {stats['followers']} followers, SB Rank: {rankings['sb_rank']}")

        if not metrics:
            raw_table_data = page.evaluate(_JS_EXTRACT_TABLE)
            metrics = _normalize_table_data(raw_table_data, body_text)
            _log(f"Table: {metrics['row_count']} rows ({metrics['period']})")
            if metrics.get("row_count") and history_source == "unavailable":
                history_source = "table_fallback"

        if not chart_data:
            chart_data = _followers_chart_from_table(metrics, metric_label=str(profile_labels["chart_metric_label"]))
            if chart_data:
                history_source = "table_fallback"
                _log(
                    f"Follower history fallback: {chart_data['total_data_points']} points, "
                    f"{chart_data['date_range']['from']} -> {chart_data['date_range']['to']}"
                )
            else:
                if authenticated_api_error is not None:
                    _log(f"WARNING: Falling back after authenticated API failure: {authenticated_api_error}")
                _log("WARNING: Could not derive follower history from authenticated API or daily metrics table")

        stats_refreshed = bool(stats["followers"] > 0 and metrics["row_count"] > 0)
        if not stats_refreshed:
            raise RuntimeError(_format_scrape_failure_message(authenticated_api_error))

        result: dict[str, Any] = {
            "username": handle,
            "account_handle": handle,
            "platform": platform,
            "scraped_at": datetime.now(tz=UTC).isoformat(),
            "stats_refreshed": stats_refreshed,
            "history_source": history_source,
            "profile_stats": stats,
            "profile_stats_labels": profile_labels,
            "rankings": rankings,
            "daily_channel_metrics_60day": metrics
            or {
                "period": "Last 14 Days",
                "row_count": 0,
                "headers": [],
                "data": [],
            },
            "daily_total_followers_chart": chart_data,
            "chart_metric_label": profile_labels["chart_metric_label"],
            "socialblade_url": sb_url,
        }

        _log("Scrape complete")
        return result
    finally:
        page.close()


def scrape_socialblade_with_shared_browser_session(
    handle: str,
    *,
    platform: str = "instagram",
    playwright: Any | None = None,
) -> dict[str, Any]:
    """Scrape SocialBlade via the visible shared Chrome session."""
    from trr_backend.socials.socialblade.auth import (
        _chrome_cdp_endpoint_reachable,
        _socialblade_visible_chrome_cdp_url,
        preflight_socialblade_chrome_profile,
    )

    if playwright is None:
        from playwright.sync_api import sync_playwright

        with sync_playwright() as sync_playwright_context:
            return scrape_socialblade_with_shared_browser_session(
                handle,
                platform=platform,
                playwright=sync_playwright_context,
            )

    cdp_url = _socialblade_visible_chrome_cdp_url()
    if not _chrome_cdp_endpoint_reachable(cdp_url):
        raise RuntimeError(
            "Visible shared Chrome session is not running on port 9222; "
            "start the manual browser session before retrying SocialBlade"
        )
    preflight_socialblade_chrome_profile(require_visible_managed=True)

    browser = playwright.chromium.connect_over_cdp(cdp_url)
    try:
        if not browser.contexts:
            raise RuntimeError("Visible shared Chrome session is not available for SocialBlade scraping")
        return _scrape_socialblade_in_context(
            browser.contexts[0],
            handle,
            platform=platform,
            playwright=playwright,
            cookies=None,
            allow_login_fallback=False,
            allow_visible_browser_retry=False,
        )
    finally:
        browser.close()


def scrape_socialblade(
    handle: str,
    cookies: Any,
    *,
    platform: str = "instagram",
    allow_login_fallback: bool = True,
    allow_visible_browser_retry: bool = False,
) -> dict[str, Any]:
    """Scrape SocialBlade account data using the default Scrapling flow.

    The visible shared-browser and credential-login Playwright paths remain as
    recovery fallbacks when the authenticated SocialBlade endpoints challenge
    the default fetch path.
    """
    from trr_backend.socials.socialblade.auth import preflight_socialblade_chrome_profile

    preflight_socialblade_chrome_profile()
    _log(f"Scraping SocialBlade for {platform} @{handle}")
    attempted_login_fallback = False
    first_payload: dict[str, Any] | None = None
    try:
        payload = _run_scrapling_socialblade_fetch(handle, cookies, platform=platform)
        first_payload = payload
        if (
            platform in {"instagram", "tiktok"}
            and allow_login_fallback
            and _has_socialblade_login_credentials()
            and _socialblade_payload_needs_login_retry(payload)
        ):
            metrics = payload.get("daily_channel_metrics_60day") if isinstance(payload, dict) else {}
            row_count = metrics.get("row_count") if isinstance(metrics, dict) else None
            _log(
                "SocialBlade scrape returned short history "
                f"({row_count or 0} rows); logging in and retrying authenticated scrape..."
            )
            attempted_login_fallback = True
            try:
                refreshed_cookies = _refresh_socialblade_cookies_via_login()
                payload = _run_scrapling_socialblade_fetch(handle, refreshed_cookies, platform=platform)
            except Exception as login_exc:  # noqa: BLE001
                return _mark_payload_as_degraded_attempt(payload, login_exc)
        return payload
    except Exception as exc:  # noqa: BLE001
        candidate_error = exc.__cause__ if getattr(exc, "__cause__", None) is not None else exc
        if allow_visible_browser_retry and _should_retry_in_visible_shared_browser(candidate_error):
            _log("Retrying SocialBlade scrape through visible shared Chrome session...")
            return scrape_socialblade_with_shared_browser_session(
                handle,
                platform=platform,
            )

        if (
            platform in {"instagram", "tiktok"}
            and allow_login_fallback
            and not attempted_login_fallback
            and _has_socialblade_login_credentials()
        ):
            try:
                _log("Attempting SocialBlade login fallback before retrying Scrapling fetch...")
                attempted_login_fallback = True
                refreshed_cookies = _refresh_socialblade_cookies_via_login()
                return _run_scrapling_socialblade_fetch(handle, refreshed_cookies, platform=platform)
            except Exception as login_exc:  # noqa: BLE001
                candidate_login_error = (
                    login_exc.__cause__ if getattr(login_exc, "__cause__", None) is not None else login_exc
                )
                if allow_visible_browser_retry and _should_retry_in_visible_shared_browser(candidate_login_error):
                    _log("Login fallback still challenged; retrying in visible shared Chrome session...")
                    return scrape_socialblade_with_shared_browser_session(
                        handle,
                        platform=platform,
                    )
                raise login_exc from exc

        if attempted_login_fallback and first_payload:
            return _mark_payload_as_degraded_attempt(first_payload, exc)

        raise


def _run_scrapling_socialblade_fetch(
    handle: str,
    cookies: Any,
    *,
    platform: str,
) -> dict[str, Any]:
    from trr_backend.socials.socialblade.fetcher import SocialBladeScraplingFetcher
    from trr_backend.socials.socialblade.proxy import select_socialblade_proxy
    from trr_backend.socials.socialblade.session import resolve_socialblade_scrapling_session

    session = resolve_socialblade_scrapling_session(cookies)
    proxy_config = select_socialblade_proxy(session_key=f"{platform}:{handle}")

    async def _run() -> dict[str, Any]:
        fetcher = SocialBladeScraplingFetcher(
            cookies=session.cookies,
            raw_cookies=session.raw_cookies,
            platform=platform,
            proxy_config=proxy_config,
        )
        try:
            return await fetcher.scrape(handle)
        finally:
            await fetcher.aclose()

    return asyncio.run(_run())


# ---------------------------------------------------------------------------
# Login fallback
# ---------------------------------------------------------------------------


def _socialblade_context_has_session_cookie(context: Any) -> bool:
    try:
        cookies = context.cookies()
    except Exception:  # noqa: BLE001
        return False
    for cookie in cookies or []:
        if not isinstance(cookie, dict):
            continue
        name = str(cookie.get("name") or "").strip().lower()
        value = str(cookie.get("value") or "").strip()
        domain = str(cookie.get("domain") or "").strip().lower()
        if name == "session" and value and "socialblade.com" in domain:
            return True
    return False


def _socialblade_page_is_logged_in(page: Any, context: Any | None = None) -> bool:
    try:
        if page.locator('a[href="/logout"], a[href*="/logout"]').count():
            return True
    except Exception:  # noqa: BLE001
        pass
    try:
        body_text = _extract_body_text(page).lower()
    except Exception:  # noqa: BLE001
        body_text = ""
    if "logout" in body_text or "log out" in body_text:
        return True
    return bool(context is not None and _socialblade_context_has_session_cookie(context))


def _modal_runtime_disallows_visible_socialblade_login() -> bool:
    if os.getenv("MODAL_TASK_ID") or os.getenv("MODAL_ENVIRONMENT"):
        return True
    job_plane = str(os.getenv("TRR_JOB_PLANE_MODE") or "").strip().lower()
    remote_executor = str(os.getenv("TRR_REMOTE_EXECUTOR") or "").strip().lower()
    return job_plane == "remote" and remote_executor == "modal"


def _do_login(page: Any, context: Any) -> None:
    """Attempt SocialBlade login using credentials from environment."""
    email = os.environ.get("SOCIALBLADE_EMAIL", "")
    password = os.environ.get("SOCIALBLADE_PASSWORD", "")
    if not email or not password:
        raise RuntimeError("SocialBlade login required but SOCIALBLADE_EMAIL / SOCIALBLADE_PASSWORD not set")

    _log("Navigating to SocialBlade login page...")
    page.goto("https://socialblade.com/login", wait_until="domcontentloaded", timeout=30_000)
    page.wait_for_timeout(2000)
    if _socialblade_page_is_logged_in(page, context):
        _log("SocialBlade session is already logged in")
        return

    # Fill email
    email_input = page.locator('input[type="email"], input[name="email"], input[placeholder*="mail"]').first
    email_input.fill(email)

    # Fill password
    pw_input = page.locator('input[type="password"]').first
    pw_input.fill(password)

    turnstile_input = page.locator('input[name="cf-turnstile-response"]').first
    if turnstile_input.count():
        for _ in range(12):
            if turnstile_input.input_value().strip():
                break
            page.wait_for_timeout(5_000)
        else:
            raise RuntimeError(
                "SocialBlade login blocked: Cloudflare Turnstile did not complete in the headless browser"
            )

    # Submit
    submit = page.locator('button[type="submit"], input[type="submit"]').first
    submit.click()

    # Wait for login to complete
    page.wait_for_timeout(5000)

    # Verify
    if not _socialblade_page_is_logged_in(page, context):
        raise RuntimeError("SocialBlade login failed: no active session found after submit")

    _log("Login successful")

    # Extract and store cookies for future use
    cookies = context.cookies()
    sb_cookies = [c for c in cookies if "socialblade" in c.get("domain", "")]
    if sb_cookies:
        _log(f"Captured {len(sb_cookies)} SocialBlade cookies after login")


def _persist_socialblade_context_cookies(context: Any) -> dict[str, str]:
    from trr_backend.socials.browser_cookie_refresh import cookie_payload, write_cookie_file
    from trr_backend.socials.socialblade.auth import (
        SOCIALBLADE_COOKIE_DOMAINS,
        require_socialblade_authenticated_cookies,
        socialblade_cookie_file_path,
    )

    refreshed = cookie_payload(context.cookies(), domains=SOCIALBLADE_COOKIE_DOMAINS)
    if not refreshed:
        raise RuntimeError("SocialBlade login fallback completed without any SocialBlade cookies")
    require_socialblade_authenticated_cookies(refreshed, source="SocialBlade login fallback")
    write_cookie_file(socialblade_cookie_file_path(), refreshed)
    return refreshed


def _refresh_socialblade_cookies_via_visible_login() -> dict[str, str]:
    return asyncio.run(_refresh_socialblade_cookies_via_visible_login_async())


async def _refresh_socialblade_cookies_via_visible_login_async() -> dict[str, str]:
    import websockets

    from trr_backend.socials.socialblade.auth import (
        _cdp_http_json,
        _cdp_send_command,
        _ensure_visible_managed_chrome_available,
        _socialblade_visible_chrome_cdp_url,
        preflight_socialblade_chrome_profile,
    )

    cdp_url = _socialblade_visible_chrome_cdp_url()
    preflight_socialblade_chrome_profile()
    _ensure_visible_managed_chrome_available(cdp_url)
    target = _cdp_http_json(cdp_url, "/json/new?https://socialblade.com/login", method="PUT")
    target_id = str(target.get("id") or "").strip()
    websocket_url = str(target.get("webSocketDebuggerUrl") or "").strip()
    if not websocket_url:
        raise RuntimeError("Visible shared Chrome did not expose a SocialBlade login websocket")

    email = os.environ.get("SOCIALBLADE_EMAIL", "")
    password = os.environ.get("SOCIALBLADE_PASSWORD", "")
    if not email or not password:
        raise RuntimeError("SocialBlade login required but SOCIALBLADE_EMAIL / SOCIALBLADE_PASSWORD not set")
    command_id = 1
    login_completed = False
    try:
        async with websockets.connect(websocket_url, open_timeout=10, close_timeout=2) as websocket:
            await _cdp_send_command(websocket, command_id, "Page.enable")
            command_id += 1
            await _cdp_send_command(websocket, command_id, "Network.enable")
            command_id += 1
            await asyncio.sleep(3)

            def build_logged_in_check() -> str:
                return """(() => {
                    const text = document.body ? document.body.innerText : "";
                    return Boolean(document.querySelector('a[href*="/logout"]'))
                        || /\\blog\\s*out\\b|\\blogout\\b/i.test(text);
                })()"""

            async def has_session_cookie() -> bool:
                nonlocal command_id
                cookie_result = await _cdp_send_command(
                    websocket,
                    command_id,
                    "Network.getCookies",
                    {"urls": ["https://socialblade.com/"]},
                )
                command_id += 1
                return any(
                    str(cookie.get("name") or "").strip().lower() == "session"
                    and str(cookie.get("value") or "").strip()
                    for cookie in cookie_result.get("cookies") or []
                    if isinstance(cookie, dict)
                )

            async def is_logged_in() -> bool:
                nonlocal command_id
                logged_in = await _cdp_send_command(
                    websocket,
                    command_id,
                    "Runtime.evaluate",
                    {"expression": build_logged_in_check(), "returnByValue": True},
                )
                command_id += 1
                if bool(((logged_in or {}).get("result") or {}).get("value")):
                    return True
                return await has_session_cookie()

            if await is_logged_in():
                login_completed = True
            else:
                fill_expression = f"""(() => {{
                    const email = {json.dumps(email)};
                    const password = {json.dumps(password)};
                    const pick = selectors => selectors.map(selector => document.querySelector(selector)).find(Boolean);
                    const setValue = (element, value) => {{
                        element.focus();
                        element.value = value;
                        element.dispatchEvent(new Event("input", {{ bubbles: true }}));
                        element.dispatchEvent(new Event("change", {{ bubbles: true }}));
                    }};
                    const emailInput = pick([
                        'input[type="email"]',
                        'input[name="email"]',
                        'input[placeholder*="mail"]'
                    ]);
                    const passwordInput = pick(['input[type="password"]']);
                    if (!emailInput || !passwordInput) {{
                        return {{
                            ok: false,
                            reason: "login_fields_missing",
                            text: (document.body?.innerText || "").slice(0, 300)
                        }};
                    }}
                    setValue(emailInput, email);
                    setValue(passwordInput, password);
                    return {{
                        ok: true,
                        hasTurnstile: Boolean(document.querySelector('input[name="cf-turnstile-response"]'))
                    }};
                }})()"""
                fill_result = await _cdp_send_command(
                    websocket,
                    command_id,
                    "Runtime.evaluate",
                    {"expression": fill_expression, "returnByValue": True},
                )
                command_id += 1
                fill_value = ((fill_result or {}).get("result") or {}).get("value")
                if not isinstance(fill_value, dict) or not fill_value.get("ok"):
                    raise RuntimeError(f"Visible SocialBlade login could not fill credentials: {fill_value}")

                turnstile_ready = False
                for _ in range(24):
                    state_result = await _cdp_send_command(
                        websocket,
                        command_id,
                        "Runtime.evaluate",
                        {
                            "expression": """(() => {
                                const input = document.querySelector('input[name="cf-turnstile-response"]');
                                const text = document.body ? document.body.innerText : "";
                                return {
                                    loggedIn: Boolean(document.querySelector('a[href*="/logout"]'))
                                        || /\\blog\\s*out\\b|\\blogout\\b/i.test(text),
                                    present: Boolean(input),
                                    solved: Boolean(input && input.value && input.value.trim())
                                };
                            })()""",
                            "returnByValue": True,
                        },
                    )
                    command_id += 1
                    state = ((state_result or {}).get("result") or {}).get("value") or {}
                    if state.get("loggedIn"):
                        turnstile_ready = True
                        break
                    if await has_session_cookie():
                        turnstile_ready = True
                        login_completed = True
                        break
                    if not state.get("present") or state.get("solved"):
                        turnstile_ready = True
                        break
                    await asyncio.sleep(5)
                if not turnstile_ready:
                    raise RuntimeError(
                        "Visible SocialBlade login is waiting on Cloudflare Turnstile; complete it in Chrome and retry"
                    )

                await _cdp_send_command(
                    websocket,
                    command_id,
                    "Runtime.evaluate",
                    {
                        "expression": """(() => {
                            const submit = document.querySelector('button[type="submit"], input[type="submit"]');
                            if (!submit) return false;
                            submit.click();
                            return true;
                        })()""",
                        "returnByValue": True,
                    },
                )
                command_id += 1
                await asyncio.sleep(6)

            for _ in range(12):
                if await is_logged_in():
                    login_completed = True
                    break
                await asyncio.sleep(3)
            if not login_completed:
                raise RuntimeError("Visible SocialBlade login failed: no active session found after submit")

            cookie_result = await _cdp_send_command(
                websocket,
                command_id,
                "Network.getCookies",
                {"urls": ["https://socialblade.com/"]},
            )
            from trr_backend.socials.browser_cookie_refresh import cookie_payload, write_cookie_file
            from trr_backend.socials.socialblade.auth import (
                SOCIALBLADE_COOKIE_DOMAINS,
                require_socialblade_authenticated_cookies,
                socialblade_cookie_file_path,
            )

            refreshed = cookie_payload(cookie_result.get("cookies") or [], domains=SOCIALBLADE_COOKIE_DOMAINS)
            if not refreshed:
                raise RuntimeError("Visible SocialBlade login completed without any SocialBlade cookies")
            require_socialblade_authenticated_cookies(refreshed, source="Visible SocialBlade login")
            write_cookie_file(socialblade_cookie_file_path(), refreshed)
            return refreshed
    finally:
        if target_id and login_completed:
            try:
                _cdp_http_json(cdp_url, f"/json/close/{target_id}")
            except Exception:
                pass


def _refresh_socialblade_cookies_via_login(*, headless: bool | None = None) -> dict[str, str]:
    from playwright.sync_api import sync_playwright

    from trr_backend.socials.browser_cookie_refresh import open_cookie_refresh_context
    from trr_backend.socials.socialblade.auth import (
        SOCIALBLADE_STEALTH_INIT_SCRIPT,
        SOCIALBLADE_STEALTH_USER_AGENT,
        preflight_socialblade_chrome_profile,
    )

    preflight_socialblade_chrome_profile()
    if headless is None:
        headless_raw = str(os.getenv("SOCIALBLADE_LOGIN_HEADLESS") or "true").strip().lower()
        headless = headless_raw not in {"0", "false", "off", "no"}
    if _modal_runtime_disallows_visible_socialblade_login():
        headless = True

    try:
        with sync_playwright() as pw:
            session = open_cookie_refresh_context(
                pw,
                platform="socialblade",
                headless=bool(headless),
                viewport={"width": 1440, "height": 1600},
                user_agent=SOCIALBLADE_STEALTH_USER_AGENT,
                locale="en-US",
                timezone_id="America/New_York",
                require_profile=not _modal_runtime_disallows_visible_socialblade_login(),
            )
            try:
                context = session.context
                context.add_init_script(SOCIALBLADE_STEALTH_INIT_SCRIPT)
                page = context.new_page()
                try:
                    _do_login(page, context)
                    return _persist_socialblade_context_cookies(context)
                finally:
                    page.close()
            finally:
                session.close()
    except Exception as exc:
        if not headless:
            raise
        detail = str(exc).lower()
        if "turnstile" not in detail and "cloudflare" not in detail:
            raise
        if _modal_runtime_disallows_visible_socialblade_login():
            raise RuntimeError(
                "Headless SocialBlade login was challenged in Modal. "
                "Refresh SOCIALBLADE_COOKIES_JSON from a visible logged-in browser and update the Modal social secret."
            ) from exc
        _log("Headless SocialBlade login was challenged; retrying through visible managed Chrome...")
        return _refresh_socialblade_cookies_via_visible_login()


def _refresh_socialblade_cookies_via_headless_login_legacy() -> dict[str, str]:
    from playwright.sync_api import sync_playwright

    from trr_backend.socials.browser_cookie_refresh import open_cookie_refresh_context
    from trr_backend.socials.socialblade.auth import (
        SOCIALBLADE_STEALTH_INIT_SCRIPT,
        SOCIALBLADE_STEALTH_USER_AGENT,
    )

    with sync_playwright() as pw:
        session = open_cookie_refresh_context(
            pw,
            platform="socialblade",
            headless=True,
            viewport={"width": 1440, "height": 1600},
            user_agent=SOCIALBLADE_STEALTH_USER_AGENT,
            locale="en-US",
            timezone_id="America/New_York",
        )
        try:
            context = session.context
            context.add_init_script(SOCIALBLADE_STEALTH_INIT_SCRIPT)
            page = context.new_page()
            try:
                _do_login(page, context)
                return _persist_socialblade_context_cookies(context)
            finally:
                page.close()
        finally:
            session.close()


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python -m trr_backend.socials.socialblade.scraper <handle>", file=sys.stderr)
        sys.exit(1)

    target_handle = sys.argv[1]
    from trr_backend.socials.socialblade.auth import load_socialblade_cookies_from_sources

    cookie_list: list[dict[str, Any]] | dict[str, str]
    cookies_json = os.environ.get("SOCIALBLADE_COOKIES_JSON", "[]")
    try:
        cookie_list = json.loads(cookies_json)
        if not cookie_list:
            cookie_list = load_socialblade_cookies_from_sources()
    except json.JSONDecodeError:
        cookie_list = load_socialblade_cookies_from_sources()

    result = scrape_socialblade(target_handle, cookie_list)
    print(json.dumps(result))
