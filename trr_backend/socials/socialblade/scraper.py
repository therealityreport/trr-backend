"""SocialBlade Instagram scraper using Playwright."""

from __future__ import annotations

import json
import logging
import os
import re
import sys
from datetime import UTC, datetime
from typing import Any

logger = logging.getLogger(__name__)


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
            _find_line_before(lines, "Engagement Rate Rank")
            or _find_line_after(lines, "Engagement Rate Rank")
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


# ---------------------------------------------------------------------------
# Main scrape function
# ---------------------------------------------------------------------------

def scrape_socialblade(handle: str, cookies: list[dict[str, Any]]) -> dict[str, Any]:
    """Scrape SocialBlade Instagram data using headless Playwright.

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
        SOCIALBLADE_STEALTH_INIT_SCRIPT,
        SOCIALBLADE_STEALTH_USER_AGENT,
        normalize_socialblade_cookies,
    )

    sb_url = f"https://socialblade.com/instagram/user/{handle}"
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
            context.add_init_script(SOCIALBLADE_STEALTH_INIT_SCRIPT)

            # Inject SocialBlade cookies if provided
            normalized_cookies = normalize_socialblade_cookies(cookies)
            if normalized_cookies:
                context.add_cookies(normalized_cookies)
                _log(f"Injected {len(normalized_cookies)} cookies")

            page = context.new_page()

            page.goto(sb_url, wait_until="domcontentloaded", timeout=30_000)
            page.wait_for_timeout(4_000)
            _log(f"Navigated to {sb_url}")

            body_text = _extract_body_text(page)
            if _page_access_denied(body_text):
                raise RuntimeError("SocialBlade blocked by Cloudflare (1020 access denied)")

            _log("Extracting profile stats and rankings...")
            stats, rankings = _extract_profile_stats_from_body_text(body_text)
            _log(f"Stats: {stats['followers']} followers, SB Rank: {rankings['sb_rank']}")

            raw_table_data = page.evaluate(_JS_EXTRACT_TABLE)
            metrics = _normalize_table_data(raw_table_data, body_text)
            _log(f"Table: {metrics['row_count']} rows ({metrics['period']})")

            chart_data = _followers_chart_from_table(metrics)
            if chart_data:
                _log(
                    f"Follower history: {chart_data['total_data_points']} points, "
                    f"{chart_data['date_range']['from']} → {chart_data['date_range']['to']}"
                )
            else:
                _log("WARNING: Could not derive follower history from daily metrics table")

            stats_refreshed = bool(
                stats["followers"] > 0
                and stats["following"] >= 0
                and metrics["row_count"] > 0
            )
            if not stats_refreshed:
                raise RuntimeError("SocialBlade scrape returned incomplete profile stats or table data")

            result: dict[str, Any] = {
                "username": handle,
                "platform": "instagram",
                "scraped_at": datetime.now(tz=UTC).isoformat(),
                "stats_refreshed": stats_refreshed,
                "profile_stats": stats,
                "rankings": rankings,
                "daily_channel_metrics_60day": metrics or {
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
            browser.close()


# ---------------------------------------------------------------------------
# Login fallback
# ---------------------------------------------------------------------------

def _do_login(page: Any, context: Any) -> None:
    """Attempt SocialBlade login using credentials from environment."""
    email = os.environ.get("SOCIALBLADE_EMAIL", "")
    password = os.environ.get("SOCIALBLADE_PASSWORD", "")
    if not email or not password:
        raise RuntimeError(
            "SocialBlade login required but SOCIALBLADE_EMAIL / SOCIALBLADE_PASSWORD not set"
        )

    _log("Navigating to SocialBlade login page...")
    page.goto("https://socialblade.com/login", wait_until="domcontentloaded", timeout=30_000)
    page.wait_for_timeout(2000)

    # Fill email
    email_input = page.locator('input[type="email"], input[name="email"], input[placeholder*="mail"]').first
    email_input.fill(email)

    # Fill password
    pw_input = page.locator('input[type="password"]').first
    pw_input.fill(password)

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
    except json.JSONDecodeError:
        cookie_list = load_socialblade_cookies_from_sources()

    result = scrape_socialblade(target_handle, cookie_list)
    print(json.dumps(result))
