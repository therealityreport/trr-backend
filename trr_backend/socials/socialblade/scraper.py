"""SocialBlade Instagram scraper using Playwright."""

from __future__ import annotations

import json
import logging
import os
import re
import sys
from collections import OrderedDict
from datetime import UTC, datetime
from typing import Any
from urllib.parse import quote

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
    const table = document.querySelector("table");
    if (!table) return null;
    const rows = [...table.querySelectorAll("tr")];
    const datePattern = /^(Mon|Tue|Wed|Thu|Fri|Sat|Sun)\\d{4}-\\d{2}-\\d{2}$/;
    const headers = [
        "Date",
        "Followers Delta",
        "Followers Total",
        "Following Delta",
        "Following Total",
        "Media Count Delta",
        "Media Count Total",
    ];
    const data = rows.slice(1)
        .map(row => [...row.querySelectorAll("td")].map(td => td.textContent.trim()))
        .filter(cells => cells.length >= 7 && datePattern.test(cells[0]))
        .map(cells => ({
            "Date": cells[0],
            "Followers Delta": cells[1],
            "Followers Total": cells[2],
            "Following Delta": cells[3],
            "Following Total": cells[4],
            "Media Count Delta": cells[5],
            "Media Count Total": cells[6],
        }));
    return { headers, data };
})()"""


_ACCESS_DENIED_PATTERNS = (
    "access denied",
    "error reference number: 1020",
    "social blade access denied",
)
_DATE_PREFIX_PATTERN = re.compile(r"^(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)(\d{4}-\d{2}-\d{2})$")
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


def _parse_int(value: str) -> int:
    return int(re.sub(r"[^0-9-]", "", value) or "0")


def _parse_float(value: str) -> float:
    return float(re.sub(r"[^0-9.\-]", "", value) or "0")


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


def _extract_profile_stats_from_body_text(body_text: str) -> tuple[dict[str, Any], dict[str, str]]:
    lines = _normalize_body_lines(body_text)
    stats: dict[str, Any] = {
        "followers": _parse_int(_find_line_after(lines, "Followers")),
        "following": _parse_int(_find_line_after(lines, "Following")),
        "media_count": _parse_int(_find_line_after(lines, "Media Count")),
        "engagement_rate": _find_line_after(lines, "Engagement Rate") or "0%",
        "average_likes": _parse_float(_find_line_after(lines, "Average Likes")),
        "average_comments": _parse_float(_find_line_after(lines, "Average Comments")),
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

    return stats, rankings


def _normalize_table_data(table_data: dict[str, Any] | None, body_text: str) -> dict[str, Any]:
    headers = [
        "Date",
        "Followers Delta",
        "Followers Total",
        "Following Delta",
        "Following Total",
        "Media Count Delta",
        "Media Count Total",
    ]
    rows = list((table_data or {}).get("data") or [])
    lines = _normalize_body_lines(body_text)
    period = _find_line_after(lines, "Daily Channel Metrics") or "Last 14 Days"
    return {
        "period": period,
        "row_count": len(rows),
        "headers": headers,
        "data": rows,
    }


def _followers_chart_from_table(metrics: dict[str, Any]) -> dict[str, Any] | None:
    chart_points: list[dict[str, Any]] = []
    for row in metrics.get("data") or []:
        raw_date = str(row.get("Date") or "").strip()
        raw_total = str(row.get("Followers Total") or "").strip()
        if not raw_date or not raw_total:
            continue
        match = _DATE_PREFIX_PATTERN.match(raw_date)
        if not match:
            continue
        chart_points.append(
            {
                "date": match.group(1),
                "followers": _parse_int(raw_total),
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
        page.get_by_role("option", name=target_option, exact=True).click()
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


def _search_socialblade_profile(page: Any, handle: str) -> dict[str, Any]:
    endpoint = "/api/trpc/instagram.search?input=" + quote(
        json.dumps({"json": {"query": handle}}, separators=(",", ":"))
    )
    result = _fetch_trpc_result(page, endpoint)
    profile = result.get("platformResult") if isinstance(result, dict) else None
    if not isinstance(profile, dict) or not str(profile.get("id") or "").strip():
        raise RuntimeError(f"SocialBlade could not resolve @{handle}")
    return profile


def _fetch_socialblade_user(page: Any, creator_id: str) -> dict[str, Any]:
    endpoint = "/api/trpc/instagram.user?input=" + quote(
        json.dumps({"json": {"id": creator_id}}, separators=(",", ":"))
    )
    result = _fetch_trpc_result(page, endpoint)
    if not isinstance(result, dict):
        raise RuntimeError("SocialBlade user endpoint returned an unexpected payload")
    return result


def _fetch_socialblade_history(page: Any, creator_id: str, *, limit: int) -> list[dict[str, Any]]:
    endpoint = "/api/trpc/instagram.user,instagram.history?batch=1&input=" + quote(
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
        raise RuntimeError("SocialBlade history endpoint returned an unexpected payload")
    return result


def _fetch_socialblade_period_deltas(page: Any, creator_id: str, *, period: str) -> list[dict[str, Any]]:
    endpoint = "/api/trpc/instagram.monthly?batch=1&input=" + quote(
        json.dumps(
            {"0": {"json": {"id": creator_id, "period": period}}},
            separators=(",", ":"),
        )
    )
    result = _fetch_trpc_result(page, endpoint, index=0)
    if not isinstance(result, list):
        raise RuntimeError(f"SocialBlade {period} endpoint returned an unexpected payload")
    return result


def _scrape_authenticated_api(
    page: Any,
    handle: str,
) -> tuple[dict[str, Any], dict[str, str], dict[str, Any], dict[str, Any] | None]:
    profile = _search_socialblade_profile(page, handle)
    creator_id = str(profile.get("id") or "").strip()
    user_payload = _fetch_socialblade_user(page, creator_id)
    stats, rankings = _build_profile_stats_from_user_payload(user_payload)
    history_rows = _fetch_socialblade_history(page, creator_id, limit=60)
    metrics = _history_rows_to_metrics(history_rows, limit=60)
    daily_deltas = _fetch_socialblade_period_deltas(page, creator_id, period="daily")
    chart_data = _build_total_followers_chart_from_daily_deltas(stats["followers"], daily_deltas)
    return stats, rankings, metrics, chart_data


def _has_socialblade_login_credentials() -> bool:
    return bool((os.getenv("SOCIALBLADE_EMAIL") or "").strip() and (os.getenv("SOCIALBLADE_PASSWORD") or "").strip())


def _should_retry_in_visible_shared_browser(error: Exception | None) -> bool:
    if isinstance(error, SocialBladeEndpointError):
        if error.status == 412 and "instagram.monthly" in error.endpoint:
            return True
        if error.status == 403 and "instagram.search" in error.endpoint:
            return True
    rendered = str(error or "").lower()
    if "instagram.monthly" in rendered and "http 412" in rendered:
        return True
    return "returned non-json data for /api/trpc/instagram.search" in rendered


def _format_scrape_failure_message(error: Exception | None) -> str:
    if _should_retry_in_visible_shared_browser(error):
        return "SocialBlade scrape failed: SocialBlade challenged the authenticated API before profile history could be fetched"

    rendered = str(error or "").strip()
    if "turnstile" in rendered.lower():
        return "SocialBlade scrape failed: visible browser session could not complete challenge"
    if rendered.startswith("SocialBlade scrape failed:"):
        return rendered
    if rendered:
        return f"SocialBlade scrape failed: {rendered}"
    return "SocialBlade scrape failed: incomplete profile stats or daily metrics data"


def _build_profile_stats_from_user_payload(user: dict[str, Any]) -> tuple[dict[str, Any], dict[str, str]]:
    followers = _parse_int(str(user.get("followers") or "0"))
    following = _parse_int(str(user.get("following") or "0"))
    media_count = _parse_int(str(user.get("media_count") or "0"))
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


def _history_rows_to_metrics(history_rows: list[dict[str, Any]], *, limit: int) -> dict[str, Any]:
    ordered_totals: OrderedDict[str, dict[str, int]] = OrderedDict()
    for row in sorted(history_rows, key=lambda item: str(item.get("date") or "")):
        date = str(row.get("date") or "")[:10]
        if not date:
            continue
        ordered_totals[date] = {
            "followers": _parse_int(str(row.get("followers") or "0")),
            "following": _parse_int(str(row.get("following") or "0")),
            "media_count": _parse_int(str(row.get("media_count") or "0")),
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
                "Media Count Delta": str(totals["media_count"] - (previous["media_count"] if previous else 0)),
                "Media Count Total": f"{totals['media_count']:,}",
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
            "Media Count Delta",
            "Media Count Total",
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
    playwright: Any | None,
    cookies: list[dict[str, Any]] | None,
    allow_login_fallback: bool,
    allow_visible_browser_retry: bool,
) -> dict[str, Any]:
    from trr_backend.socials.socialblade.auth import (
        SOCIALBLADE_STEALTH_INIT_SCRIPT,
        normalize_socialblade_cookies,
    )

    sb_url = f"https://socialblade.com/instagram/user/{handle}"
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
            raise RuntimeError("SocialBlade blocked by Cloudflare (1020 access denied)")

        chart_data = None
        metrics = None
        stats = None
        rankings = None
        history_source = "unavailable"

        authenticated_api_error: Exception | None = None
        try:
            stats, rankings, metrics, chart_data = _scrape_authenticated_api(page, handle)
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
                return scrape_socialblade_with_shared_browser_session(handle, playwright=playwright)

        if (not chart_data or not metrics) and allow_login_fallback and _has_socialblade_login_credentials():
            try:
                _log("Attempting SocialBlade login fallback for authenticated API access...")
                _do_login(page, context)
                page.goto(sb_url, wait_until="domcontentloaded", timeout=30_000)
                page.wait_for_timeout(4_000)
                body_text = _extract_body_text(page)
                if _page_access_denied(body_text):
                    raise RuntimeError("SocialBlade blocked by Cloudflare after login")
                stats, rankings, metrics, chart_data = _scrape_authenticated_api(page, handle)
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
            stats, rankings = _extract_profile_stats_from_body_text(body_text)
            _log(f"Stats: {stats['followers']} followers, SB Rank: {rankings['sb_rank']}")

        if not metrics:
            raw_table_data = page.evaluate(_JS_EXTRACT_TABLE)
            metrics = _normalize_table_data(raw_table_data, body_text)
            _log(f"Table: {metrics['row_count']} rows ({metrics['period']})")

        if not chart_data:
            chart_data = _followers_chart_from_table(metrics)
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

        stats_refreshed = bool(stats["followers"] > 0 and stats["following"] >= 0 and metrics["row_count"] > 0)
        if not stats_refreshed:
            raise RuntimeError(_format_scrape_failure_message(authenticated_api_error))

        result: dict[str, Any] = {
            "username": handle,
            "platform": "instagram",
            "scraped_at": datetime.now(tz=UTC).isoformat(),
            "stats_refreshed": stats_refreshed,
            "history_source": history_source,
            "profile_stats": stats,
            "rankings": rankings,
            "daily_channel_metrics_60day": metrics
            or {
                "period": "Last 14 Days",
                "row_count": 0,
                "headers": [],
                "data": [],
            },
            "daily_total_followers_chart": chart_data,
        }

        _log("Scrape complete")
        return result
    finally:
        page.close()


def scrape_socialblade_with_shared_browser_session(
    handle: str,
    *,
    playwright: Any | None = None,
) -> dict[str, Any]:
    """Scrape SocialBlade via the visible shared Chrome session."""
    from trr_backend.socials.socialblade.auth import _chrome_cdp_endpoint_reachable, _socialblade_visible_chrome_cdp_url

    if playwright is None:
        from playwright.sync_api import sync_playwright

        with sync_playwright() as sync_playwright_context:
            return scrape_socialblade_with_shared_browser_session(
                handle,
                playwright=sync_playwright_context,
            )

    cdp_url = _socialblade_visible_chrome_cdp_url()
    if not _chrome_cdp_endpoint_reachable(cdp_url):
        raise RuntimeError(
            "Visible shared Chrome session is not running on port 9222; start the manual browser session before retrying SocialBlade"
        )

    browser = playwright.chromium.connect_over_cdp(cdp_url)
    try:
        if not browser.contexts:
            raise RuntimeError("Visible shared Chrome session is not available for SocialBlade scraping")
        return _scrape_socialblade_in_context(
            browser.contexts[0],
            handle,
            playwright=playwright,
            cookies=None,
            allow_login_fallback=False,
            allow_visible_browser_retry=False,
        )
    finally:
        browser.close()


def scrape_socialblade(
    handle: str,
    cookies: list[dict[str, Any]],
    *,
    allow_login_fallback: bool = True,
    allow_visible_browser_retry: bool = False,
) -> dict[str, Any]:
    """Scrape SocialBlade Instagram data using Playwright.

    Args:
        handle: Instagram username (e.g. "lisabarlow14").
        cookies: List of cookie dicts to inject (Playwright format).
                 Each should have at least ``name``, ``value``, ``domain``.

    Returns:
        Combined scrape result dict matching the existing JSON schema.
    """
    from playwright.sync_api import sync_playwright

    from trr_backend.socials.browser_cookie_refresh import launch_browser
    from trr_backend.socials.socialblade.auth import (
        SOCIALBLADE_STEALTH_USER_AGENT,
    )
    _log(f"Scraping SocialBlade for @{handle}")

    with sync_playwright() as pw:
        browser = launch_browser(pw, headless=True)
        try:
            context = browser.new_context(
                viewport={"width": 1440, "height": 1600},
                user_agent=SOCIALBLADE_STEALTH_USER_AGENT,
                locale="en-US",
                timezone_id="America/New_York",
            )
            return _scrape_socialblade_in_context(
                context,
                handle,
                playwright=pw,
                cookies=cookies,
                allow_login_fallback=allow_login_fallback,
                allow_visible_browser_retry=allow_visible_browser_retry,
            )

        finally:
            browser.close()


# ---------------------------------------------------------------------------
# Login fallback
# ---------------------------------------------------------------------------


def _do_login(page: Any, context: Any) -> None:
    """Attempt SocialBlade login using credentials from environment."""
    email = os.environ.get("SOCIALBLADE_EMAIL", "")
    password = os.environ.get("SOCIALBLADE_PASSWORD", "")
    if not email or not password:
        raise RuntimeError("SocialBlade login required but SOCIALBLADE_EMAIL / SOCIALBLADE_PASSWORD not set")

    _log("Navigating to SocialBlade login page...")
    page.goto("https://socialblade.com/login", wait_until="domcontentloaded", timeout=30_000)
    page.wait_for_timeout(2000)

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
    is_logged_in = page.evaluate("""(() => {
        const logoutLink = document.querySelector('a[href="/logout"]');
        return !!logoutLink;
    })()""")
    if not is_logged_in:
        raise RuntimeError("SocialBlade login failed — no logout link found after submit")

    _log("Login successful")

    # Extract and store cookies for future use
    cookies = context.cookies()
    sb_cookies = [c for c in cookies if "socialblade" in c.get("domain", "")]
    if sb_cookies:
        _log(f"Captured {len(sb_cookies)} SocialBlade cookies after login")


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
