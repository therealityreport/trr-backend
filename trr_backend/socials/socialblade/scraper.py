"""SocialBlade Instagram scraper using Playwright.

Usage (standalone):
    python -m trr_backend.socials.socialblade.scraper <handle>

Outputs JSON to stdout. Requires ``playwright install chromium`` to have been run.

Steps:
1. Navigate to the SocialBlade user page
2. Click "Refresh Stats"
3. Extract profile stats and rankings from the DOM
4. Set chart dropdowns to Daily / Total (HeadlessUI listboxes)
5. Extract ECharts follower data via React fiber traversal
6. Switch table to 60 days and extract table data
7. Return combined dict
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from datetime import UTC, datetime
from typing import Any

logger = logging.getLogger(__name__)


def _log(msg: str) -> None:
    logger.info(msg)
    print(msg, file=sys.stderr)


# ---------------------------------------------------------------------------
# HeadlessUI dropdown helper
# ---------------------------------------------------------------------------

def _select_headlessui_option(page: Any, button_id: str, option_text: str) -> bool:
    """Open a HeadlessUI listbox button and click the target option.

    HeadlessUI listboxes don't respond to JS-dispatched events — we must use
    real mouse clicks at the element's screen coordinates.
    """
    # Scroll button into view
    btn_info = page.evaluate(f"""(() => {{
        const btn = document.getElementById("{button_id}");
        if (!btn) return null;
        btn.scrollIntoView({{ block: "center" }});
        return {{ text: btn.textContent.trim() }};
    }})()""")
    if not btn_info:
        return False

    page.wait_for_timeout(300)

    # Click button to open dropdown
    pos = page.evaluate(f"""(() => {{
        const btn = document.getElementById("{button_id}");
        const r = btn.getBoundingClientRect();
        return {{ x: r.x + r.width / 2, y: r.y + r.height / 2 }};
    }})()""")
    page.mouse.click(pos["x"], pos["y"])
    page.wait_for_timeout(500)

    # Verify expanded
    expanded = page.evaluate(
        f'document.getElementById("{button_id}")?.getAttribute("aria-expanded")'
    )
    if expanded != "true":
        _log(f"  Dropdown {button_id} did not open")
        return False

    # Find the correct listbox via aria-controls
    controls_id = page.evaluate(
        f'document.getElementById("{button_id}")?.getAttribute("aria-controls")'
    )

    listbox_selector = f'document.getElementById("{controls_id}") || ' if controls_id else ""
    option_pos = page.evaluate(f"""(() => {{
        const listbox = {listbox_selector}document.querySelector("[role=listbox]");
        if (!listbox) return null;
        const opts = [...listbox.querySelectorAll("[role=option]")];
        const target = opts.find(o => o.textContent.trim() === "{option_text}");
        if (!target) return null;
        const r = target.getBoundingClientRect();
        return {{ x: r.x + r.width / 2, y: r.y + r.height / 2 }};
    }})()""")

    if not option_pos:
        _log(f'  Option "{option_text}" not found')
        return False

    page.mouse.click(option_pos["x"], option_pos["y"])
    page.wait_for_timeout(500)
    _log(f"  Set {button_id} → {option_text}")
    return True


# ---------------------------------------------------------------------------
# JS extraction expressions (ported verbatim from scrape-socialblade.mjs)
# ---------------------------------------------------------------------------

_JS_EXTRACT_PROFILE_STATS = """(() => {
    const h2s = [...document.querySelectorAll("h2")];
    const h4s = [...document.querySelectorAll("h4")];
    const pairs = [];
    for (const h2 of h2s) {
        const rect = h2.getBoundingClientRect();
        const nearbyH4 = h4s.find(h4 => {
            const h4r = h4.getBoundingClientRect();
            return Math.abs(h4r.x - rect.x) < 50 && h4r.y > rect.y && h4r.y - rect.y < 80;
        });
        if (nearbyH4) {
            pairs.push({ value: h2.textContent.trim(), label: nearbyH4.textContent.trim(), y: rect.y });
        }
    }
    return pairs;
})()"""

_JS_EXTRACT_GRADE = """(() => {
    const els = [...document.querySelectorAll("span, div")];
    const gradeEl = els.find(e =>
        /^[A-F][+-]?$/.test(e.textContent.trim()) &&
        e.getBoundingClientRect().height > 30 &&
        e.getBoundingClientRect().height < 80
    );
    return gradeEl ? gradeEl.textContent.trim() : "";
})()"""

_JS_SCROLL_TO_CHARTS = """(() => {
    const spans = [...document.querySelectorAll("span")];
    const header = spans.find(s => s.textContent.trim() === "Detailed Charts");
    if (header) header.scrollIntoView({ block: "start" });
    return !!header;
})()"""

_JS_FIND_DROPDOWNS = """(() => {
    const buttons = [...document.querySelectorAll('button[id^="headlessui-listbox-button"]')];
    return buttons.map(b => ({ id: b.id, text: b.textContent.trim() }));
})()"""

_JS_EXTRACT_CHART_DATA = """(() => {
    const charts = document.querySelectorAll("[_echarts_instance_]");
    let bestChart = null;
    let bestCount = 0;
    for (const chart of charts) {
        const key = Object.keys(chart).find(k => k.startsWith("__reactFiber"));
        if (!key) continue;
        let fiber = chart[key];
        for (let i = 0; i < 20 && fiber; i++) {
            const props = fiber.memoizedProps;
            if (props?.option?.series?.[0]?.data) {
                const data = props.option.series[0].data;
                if (data.length > bestCount) {
                    bestCount = data.length;
                    bestChart = {
                        data,
                        categories: props.option.xAxis?.[0]?.data || props.option.xAxis?.data,
                    };
                }
                break;
            }
            fiber = fiber.return;
        }
    }
    if (!bestChart) return null;
    return {
        total_data_points: bestChart.data.length,
        date_range: {
            from: bestChart.categories?.[0] || null,
            to: bestChart.categories?.[bestChart.categories.length - 1] || null,
        },
        data: bestChart.categories
            ? bestChart.categories.map((date, i) => ({ date, followers: bestChart.data[i] }))
            : bestChart.data.map((v, i) => ({ date: String(i), followers: v })),
    };
})()"""

_JS_EXTRACT_TABLE = """(() => {
    const table = document.querySelector("table");
    if (!table) return null;
    const rows = [...table.querySelectorAll("tr")];
    const headers = [...rows[0].querySelectorAll("th")].map(th => th.textContent.trim());
    const datePattern = /^(Mon|Tue|Wed|Thu|Fri|Sat|Sun)\\d{4}-\\d{2}-\\d{2}$/;
    const data = rows.slice(1)
        .map(row => {
            const cells = [...row.querySelectorAll("td")];
            const obj = {};
            headers.forEach((h, i) => { obj[h] = cells[i]?.textContent?.trim() || ""; });
            return obj;
        })
        .filter(row => datePattern.test(row[headers[0]] || ""));
    return { period: "Last 60 Days", row_count: data.length, headers, data };
})()"""


# ---------------------------------------------------------------------------
# Profile stats parser
# ---------------------------------------------------------------------------

def _parse_profile_stats(
    pairs: list[dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, str]]:
    """Parse H2/H4 label-value pairs into stats and rankings dicts."""
    stats: dict[str, Any] = {
        "followers": 0,
        "following": 0,
        "media_count": 0,
        "engagement_rate": "0%",
        "average_likes": 0,
        "average_comments": 0,
    }
    rankings: dict[str, str] = {
        "sb_rank": "",
        "followers_rank": "",
        "engagement_rate_rank": "",
        "grade": "",
    }

    import re

    for pair in pairs:
        label = pair.get("label", "").lower()
        val = pair.get("value", "")

        if "sb rank" in label:
            rankings["sb_rank"] = val
        elif "followers rank" in label:
            rankings["followers_rank"] = val
        elif "engagement" in label and "rank" in label:
            rankings["engagement_rate_rank"] = val
        elif label == "followers":
            stats["followers"] = int(re.sub(r"[^0-9]", "", val) or "0")
        elif label == "following":
            stats["following"] = int(re.sub(r"[^0-9]", "", val) or "0")
        elif "media" in label or "posts" in label:
            stats["media_count"] = int(re.sub(r"[^0-9]", "", val) or "0")
        elif "engagement" in label:
            stats["engagement_rate"] = val
        elif "avg" in label and "like" in label:
            stats["average_likes"] = float(re.sub(r"[^0-9.]", "", val) or "0")
        elif "avg" in label and "comment" in label:
            stats["average_comments"] = float(re.sub(r"[^0-9.]", "", val) or "0")

    return stats, rankings


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

    sb_url = f"https://socialblade.com/instagram/user/{handle}"
    _log(f"Scraping SocialBlade for @{handle}")

    with sync_playwright() as pw:
        browser = launch_browser(pw, headless=True)
        try:
            context = browser.new_context(viewport={"width": 1440, "height": 1200})

            # Inject SocialBlade cookies if provided
            if cookies:
                context.add_cookies(cookies)
                _log(f"Injected {len(cookies)} cookies")

            page = context.new_page()

            # 1. Navigate to SocialBlade user page
            page.goto(sb_url, wait_until="domcontentloaded", timeout=30_000)
            page.wait_for_timeout(3000)
            _log(f"Navigated to {sb_url}")

            # Check if we need to login
            is_logged_in = page.evaluate("""(() => {
                const logoutLink = document.querySelector('a[href="/logout"]');
                const loginLink = document.querySelector('a[href="/login"]');
                return !!logoutLink || !loginLink;
            })()""")

            if not is_logged_in:
                _log("Not logged in — attempting login")
                _do_login(page, context)
                # Navigate back to user page after login
                page.goto(sb_url, wait_until="domcontentloaded", timeout=30_000)
                page.wait_for_timeout(3000)

            # 2. Click "Refresh Stats"
            _log("Clicking Refresh Stats...")
            refresh_clicked = page.evaluate("""(() => {
                const btns = [...document.querySelectorAll("button")];
                const btn = btns.find(b => b.textContent.trim().toLowerCase().includes("refresh"));
                if (!btn) return false;
                btn.click();
                return true;
            })()""")
            if refresh_clicked:
                _log("Refresh Stats clicked, waiting for update...")
                page.wait_for_timeout(5000)
            else:
                _log("Refresh Stats button not found (may have been recently refreshed)")

            # 3. Extract profile stats
            _log("Extracting profile stats and rankings...")
            profile_pairs = page.evaluate(_JS_EXTRACT_PROFILE_STATS)
            stats, rankings = _parse_profile_stats(profile_pairs or [])

            grade = page.evaluate(_JS_EXTRACT_GRADE)
            if grade:
                rankings["grade"] = grade

            _log(f"Stats: {stats['followers']} followers, SB Rank: {rankings['sb_rank']}")

            # 4. Set chart dropdowns to Daily / Total
            _log("Setting chart dropdowns to Daily / Total...")
            page.evaluate(_JS_SCROLL_TO_CHARTS)
            page.wait_for_timeout(500)

            dropdown_ids = page.evaluate(_JS_FIND_DROPDOWNS)
            _log(f"Found {len(dropdown_ids or [])} HeadlessUI listbox buttons")

            if dropdown_ids and len(dropdown_ids) >= 1:
                if "Daily" not in dropdown_ids[0].get("text", ""):
                    _log(f"Setting frequency: {dropdown_ids[0]['text']} → Daily")
                    _select_headlessui_option(page, dropdown_ids[0]["id"], "Daily")
                    page.wait_for_timeout(2000)
                else:
                    _log("Frequency already set to Daily")

            if dropdown_ids and len(dropdown_ids) >= 2:
                if "Total" not in dropdown_ids[1].get("text", ""):
                    _log(f"Setting metric: {dropdown_ids[1]['text']} → Total")
                    _select_headlessui_option(page, dropdown_ids[1]["id"], "Total")
                    page.wait_for_timeout(3000)
                else:
                    _log("Metric already set to Total")

            # 5. Extract ECharts chart data via React fiber
            _log("Extracting chart data via React fiber...")
            chart_data = page.evaluate(_JS_EXTRACT_CHART_DATA)

            if chart_data:
                _log(
                    f"Chart data: {chart_data['total_data_points']} points, "
                    f"{chart_data['date_range']['from']} → {chart_data['date_range']['to']}"
                )
            else:
                _log("WARNING: Could not extract chart data")

            # 6. Extract 60-day table
            _log("Switching table to 60 days...")
            table_btn = page.evaluate("""(() => {
                const btns = [...document.querySelectorAll("button")];
                const btn = btns.find(b => b.textContent.trim().startsWith("Last "));
                if (!btn) return null;
                btn.scrollIntoView({ block: "center" });
                return { text: btn.textContent.trim() };
            })()""")
            page.wait_for_timeout(300)

            if table_btn and "60" not in table_btn.get("text", ""):
                btn_pos = page.evaluate("""(() => {
                    const btns = [...document.querySelectorAll("button")];
                    const btn = btns.find(b => b.textContent.trim().startsWith("Last "));
                    const r = btn.getBoundingClientRect();
                    return { x: r.x + r.width / 2, y: r.y + r.height / 2 };
                })()""")
                page.mouse.click(btn_pos["x"], btn_pos["y"])
                page.wait_for_timeout(500)

                opt_pos = page.evaluate("""(() => {
                    const menus = document.querySelectorAll("div.absolute");
                    for (const menu of menus) {
                        const r = menu.getBoundingClientRect();
                        if (r.height > 100 && r.width > 100 && r.width < 300) {
                            const items = [...menu.children];
                            const target = items.find(i => i.textContent.trim() === "Last 60 Days");
                            if (target) {
                                const tr = target.getBoundingClientRect();
                                return { x: tr.x + tr.width / 2, y: tr.y + tr.height / 2 };
                            }
                        }
                    }
                    return null;
                })()""")
                if opt_pos:
                    page.mouse.click(opt_pos["x"], opt_pos["y"])
                    page.wait_for_timeout(3000)

            table_data = page.evaluate(_JS_EXTRACT_TABLE)
            _log(f"Table: {table_data.get('row_count', 0) if table_data else 0} rows")

            # 7. Build result
            result: dict[str, Any] = {
                "username": handle,
                "platform": "instagram",
                "scraped_at": datetime.now(tz=UTC).isoformat(),
                "stats_refreshed": bool(refresh_clicked),
                "profile_stats": stats,
                "rankings": rankings,
                "daily_channel_metrics_60day": table_data or {
                    "period": "Last 60 Days",
                    "row_count": 0,
                    "headers": [],
                    "data": [],
                },
                "daily_total_followers_chart": (
                    {
                        "frequency": "daily",
                        "metric": "total_followers",
                        **chart_data,
                    }
                    if chart_data
                    else None
                ),
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
    cookies_json = os.environ.get("SOCIALBLADE_COOKIES_JSON", "[]")
    try:
        cookie_list = json.loads(cookies_json)
    except json.JSONDecodeError:
        cookie_list = []

    result = scrape_socialblade(target_handle, cookie_list)
    print(json.dumps(result))
