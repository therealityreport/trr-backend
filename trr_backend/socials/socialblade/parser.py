"""Pure SocialBlade parsing and payload-shaping helpers."""

from __future__ import annotations

import json
import re
from collections import OrderedDict
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

SOCIALBLADE_STEALTH_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/146.0.0.0 Safari/537.36"
)
SOCIALBLADE_STEALTH_INIT_SCRIPT = """
Object.defineProperty(navigator, "webdriver", { get: () => undefined });
Object.defineProperty(navigator, "platform", { get: () => "MacIntel" });
Object.defineProperty(navigator, "languages", { get: () => ["en-US", "en"] });
window.chrome = window.chrome || { runtime: {} };
"""


class SocialBladeEndpointError(RuntimeError):
    """Raised when a SocialBlade endpoint returns a non-200 response."""

    def __init__(self, endpoint: str, status: int):
        self.endpoint = endpoint
        self.status = status
        super().__init__(f"SocialBlade endpoint {endpoint} returned HTTP {status}")


_ACCESS_DENIED_PATTERNS = (
    "access denied",
    "error reference number: 1020",
    "social blade access denied",
)
_DATE_PREFIX_PATTERN = re.compile(r"^(?:(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\s*)?(\d{4}-\d{2}-\d{2})$")
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
    points_by_date: dict[str, int] = {}
    for row in sorted(total_rows, key=lambda item: str(item.get("date") or "")):
        if not isinstance(row, dict):
            continue
        date = str(row.get("date") or "")[:10]
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date):
            continue
        points_by_date[date] = _parse_metric_number(str(row.get(metric_key) or "0"))
    return _followers_chart_from_points(points_by_date)


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


def _format_scrape_failure_message(error: Exception | None) -> str:
    rendered = str(error or "").strip()
    if "turnstile" in rendered.lower():
        return "SocialBlade scrape failed: visible browser session could not complete challenge"
    if rendered.startswith("SocialBlade scrape failed:"):
        return rendered
    if rendered:
        return f"SocialBlade scrape failed: {rendered}"
    return "SocialBlade scrape failed: incomplete profile stats or daily metrics data"


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
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date):
            continue
        ordered_totals[date] = {
            "followers": _parse_metric_number(str(row.get("followers") or "0")),
            "following": _parse_metric_number(str(row.get("following") or "0")),
            third_metric_label.lower().replace(" ", "_"): _parse_metric_number(
                str(_first_present_metric_value(row, third_metric_keys))
            ),
        }

    dates = list(ordered_totals.keys())
    rendered_rows: list[dict[str, str]] = []
    previous_totals = {"followers": 0, "following": 0, third_metric_label.lower().replace(" ", "_"): 0}
    for index, date in enumerate(dates):
        current = ordered_totals[date]
        followers_delta = current["followers"] - previous_totals["followers"] if index else current["followers"]
        following_delta = current["following"] - previous_totals["following"] if index else current["following"]
        third_metric_key = third_metric_label.lower().replace(" ", "_")
        third_metric_delta = (
            current[third_metric_key] - previous_totals[third_metric_key] if index else current[third_metric_key]
        )
        rendered_rows.append(
            {
                "Date": date,
                "Followers Delta": str(followers_delta),
                "Followers Total": f"{current['followers']:,}",
                "Following Delta": str(following_delta),
                "Following Total": f"{current['following']:,}",
                f"{third_metric_label} Delta": str(third_metric_delta),
                f"{third_metric_label} Total": f"{current[third_metric_key]:,}",
            }
        )
        previous_totals = current

    return {
        "period": f"Last {min(limit, len(rendered_rows))} Days" if rendered_rows else "Last 14 Days",
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
    current_total: int,
    daily_deltas: list[dict[str, Any]],
) -> dict[str, Any] | None:
    dated_deltas = []
    for row in sorted(daily_deltas, key=lambda item: str(item.get("date") or "")):
        date = str(row.get("date") or "")[:10]
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date):
            continue
        dated_deltas.append((date, _parse_metric_number(str(row.get("followers") or "0"))))
    if not dated_deltas:
        return None

    total = int(current_total)
    points: list[dict[str, Any]] = []
    for date, delta in reversed(dated_deltas):
        points.append({"date": date, "followers": total})
        total -= delta
    points.reverse()
    return {
        "frequency": "daily",
        "metric": "total_followers",
        "total_data_points": len(points),
        "date_range": {"from": points[0]["date"], "to": points[-1]["date"]},
        "data": points,
    }
