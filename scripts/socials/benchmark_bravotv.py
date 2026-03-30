#!/usr/bin/env python3
"""Benchmark all TRR scrapers against BravoTV handles.

Usage:
    .venv/bin/python scripts/socials/benchmark_bravotv.py [--platform PLATFORM]
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import traceback
from datetime import datetime, timedelta, timezone

# ── bootstrap ──────────────────────────────────────────────────────────
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from dotenv import load_dotenv

load_dotenv()

# ── BravoTV handles per platform ──────────────────────────────────────
BRAVO_HANDLES = {
    "facebook": "Bravo",
    "youtube": "bravo",
    "instagram": "bravotv",
    "tiktok": "bravotv",
    "twitter": "BravoTV",
    "threads": "bravotv",
}

# Date window: last 7 days
NOW = datetime.now(timezone.utc)
DATE_END = NOW
DATE_START = NOW - timedelta(days=7)


def _load_cookies(platform: str) -> dict | None:
    """Load cookies from JSON env var or file."""
    env_json = os.environ.get(f"SOCIAL_{platform.upper()}_COOKIES_JSON", "")
    if env_json and env_json.strip() not in ("", "{}", "[]"):
        return json.loads(env_json)
    env_file = os.environ.get(f"SOCIAL_{platform.upper()}_COOKIES_FILE", "")
    if env_file and os.path.exists(env_file):
        with open(env_file) as f:
            return json.load(f)
    return None


def _fmt_duration(seconds: float) -> str:
    if seconds < 1:
        return f"{seconds * 1000:.0f}ms"
    return f"{seconds:.1f}s"


# ── Platform benchmarks ──────────────────────────────────────────────

def benchmark_facebook():
    from trr_backend.socials.facebook import FacebookScraper, FacebookScrapeConfig

    cookies = _load_cookies("facebook")

    scraper = FacebookScraper(cookies=cookies or {})
    config = FacebookScrapeConfig(
        page_handle=BRAVO_HANDLES["facebook"],
        date_start=DATE_START,
        date_end=DATE_END,
        max_pages=2,
        include_feed=True,
        include_reels=True,
        include_photos=False,
        fast_mode=True,
        max_scrape_seconds=300,  # 5 min cap for benchmarks
    )

    t0 = time.monotonic()
    try:
        posts = scraper.scrape(config)
        duration = time.monotonic() - t0
        return {
            "status": "ok",
            "posts": len(posts),
            "duration": _fmt_duration(duration),
            "sample": [
                {
                    "id": p.post_id,
                    "type": p.post_type,
                    "caption": (p.caption or "")[:80],
                    "likes": p.likes,
                    "views": p.views,
                    "posted_at": p.posted_at,
                    "url": p.url,
                }
                for p in posts[:3]
            ],
            "meta": scraper.last_retrieval_meta,
        }
    except Exception as e:
        duration = time.monotonic() - t0
        return {
            "status": "error",
            "error": str(e),
            "traceback": traceback.format_exc(),
            "duration": _fmt_duration(duration),
            "meta": getattr(scraper, "last_retrieval_meta", None),
        }


def benchmark_youtube():
    from trr_backend.socials.youtube import YouTubeScraper, YouTubeScrapeConfig

    scraper = YouTubeScraper()
    config = YouTubeScrapeConfig(
        channel_handle=BRAVO_HANDLES["youtube"],
        keywords=[],  # no keyword filter — get all recent
        date_start=DATE_START,
        date_end=DATE_END,
        max_pages=2,
        enforce_keyword_filter=False,
        fast_mode=True,
    )

    t0 = time.monotonic()
    try:
        videos = scraper.scrape(config)
        duration = time.monotonic() - t0
        return {
            "status": "ok",
            "videos": len(videos),
            "duration": _fmt_duration(duration),
            "sample": [
                {
                    "id": v.video_id,
                    "title": v.title[:80] if v.title else "",
                    "views": v.views,
                    "likes": v.likes,
                    "is_short": v.is_short,
                    "published_at": v.published_at,
                    "url": v.url,
                }
                for v in videos[:3]
            ],
            "meta": scraper.last_retrieval_meta,
        }
    except Exception as e:
        duration = time.monotonic() - t0
        return {
            "status": "error",
            "error": str(e),
            "traceback": traceback.format_exc(),
            "duration": _fmt_duration(duration),
            "meta": getattr(scraper, "last_retrieval_meta", None),
        }


def benchmark_instagram():
    from trr_backend.socials.instagram import InstagramScraper, ScrapeConfig

    cookies = _load_cookies("instagram")
    scraper = InstagramScraper(cookies=cookies or {})
    config = ScrapeConfig(
        username=BRAVO_HANDLES["instagram"],
        hashtags=[],
        date_start=DATE_START,
        date_end=DATE_END,
        max_pages=3,
        fast_mode=True,
        scrape_mode="auto",  # graphql → browser_intercept fallback
        require_auth=True,  # auto-refresh via Playwright if cookies missing/expired
        max_scrape_seconds=300,  # 5 min cap for benchmarks
    )

    t0 = time.monotonic()
    try:
        posts = scraper.scrape(config)
        duration = time.monotonic() - t0
        return {
            "status": "ok",
            "posts": len(posts),
            "duration": _fmt_duration(duration),
            "sample": [
                {
                    "shortcode": p.shortcode,
                    "type": p.post_type,
                    "caption": (p.caption or "")[:80],
                    "likes": p.likes,
                    "views": p.video_views,
                    "url": p.url,
                }
                for p in posts[:3]
            ],
            "meta": scraper.last_retrieval_meta,
        }
    except Exception as e:
        duration = time.monotonic() - t0
        return {
            "status": "error",
            "error": str(e),
            "traceback": traceback.format_exc(),
            "duration": _fmt_duration(duration),
            "meta": getattr(scraper, "last_retrieval_meta", None),
        }


def benchmark_tiktok():
    from trr_backend.socials.tiktok import TikTokScraper, TikTokScrapeConfig

    cookies = _load_cookies("tiktok")
    scraper = TikTokScraper(cookies=cookies)
    config = TikTokScrapeConfig(
        username=BRAVO_HANDLES["tiktok"],
        hashtags=[],
        date_start=DATE_START,
        date_end=DATE_END,
        max_pages=1,
        fast_mode=True,
    )

    t0 = time.monotonic()
    try:
        posts = scraper.scrape(config)
        duration = time.monotonic() - t0
        return {
            "status": "ok",
            "posts": len(posts),
            "duration": _fmt_duration(duration),
            "sample": [
                {
                    "id": p.video_id,
                    "desc": (p.description or "")[:80],
                    "likes": p.likes,
                    "views": p.views,
                    "url": p.url,
                }
                for p in posts[:3]
            ],
            "meta": scraper.last_retrieval_meta,
        }
    except Exception as e:
        duration = time.monotonic() - t0
        return {
            "status": "error",
            "error": str(e),
            "traceback": traceback.format_exc(),
            "duration": _fmt_duration(duration),
            "meta": getattr(scraper, "last_retrieval_meta", None),
        }


def benchmark_twitter():
    from trr_backend.socials.twitter import TwitterScraper, TwitterScrapeConfig

    cookies = _load_cookies("twitter")
    bearer = os.environ.get("SOCIAL_TWITTER_BEARER_TOKEN", "")

    scraper = TwitterScraper(cookies=cookies, bearer_token=bearer or None)
    config = TwitterScrapeConfig(
        query=f"from:{BRAVO_HANDLES['twitter']}",
        date_start=DATE_START,
        date_end=DATE_END,
        max_pages=1,
        fast_mode=True,
    )

    t0 = time.monotonic()
    try:
        tweets = scraper.scrape(config)
        duration = time.monotonic() - t0
        return {
            "status": "ok",
            "tweets": len(tweets),
            "duration": _fmt_duration(duration),
            "sample": [
                {
                    "id": t.tweet_id,
                    "text": (t.text or "")[:80],
                    "likes": t.likes,
                    "views": t.views,
                    "url": t.url,
                }
                for t in tweets[:3]
            ],
            "meta": scraper.last_retrieval_meta,
        }
    except Exception as e:
        duration = time.monotonic() - t0
        return {
            "status": "error",
            "error": str(e),
            "traceback": traceback.format_exc(),
            "duration": _fmt_duration(duration),
            "meta": getattr(scraper, "last_retrieval_meta", None),
        }


def benchmark_threads():
    from trr_backend.socials.threads import ThreadsScraper, ThreadsScrapeConfig

    cookies = _load_cookies("threads")
    scraper = ThreadsScraper(cookies=cookies or {})
    config = ThreadsScrapeConfig(
        username=BRAVO_HANDLES["threads"],
        date_start=DATE_START,
        date_end=DATE_END,
        max_pages=1,
        fast_mode=True,
    )

    t0 = time.monotonic()
    try:
        posts = scraper.scrape(config)
        duration = time.monotonic() - t0
        return {
            "status": "ok",
            "posts": len(posts),
            "duration": _fmt_duration(duration),
            "sample": [
                {
                    "id": p.post_id,
                    "text": (p.text or "")[:80],
                    "likes": p.likes,
                    "views": p.views,
                    "url": p.url,
                }
                for p in posts[:3]
            ],
            "meta": scraper.last_retrieval_meta,
        }
    except Exception as e:
        duration = time.monotonic() - t0
        return {
            "status": "error",
            "error": str(e),
            "traceback": traceback.format_exc(),
            "duration": _fmt_duration(duration),
            "meta": getattr(scraper, "last_retrieval_meta", None),
        }


BENCHMARKS = {
    "facebook": benchmark_facebook,
    "youtube": benchmark_youtube,
    "instagram": benchmark_instagram,
    "tiktok": benchmark_tiktok,
    "twitter": benchmark_twitter,
    "threads": benchmark_threads,
}


def main():
    parser = argparse.ArgumentParser(description="Benchmark TRR scrapers with BravoTV handles")
    parser.add_argument("--platform", choices=list(BENCHMARKS) + ["all"], default="all")
    parser.add_argument("--output", type=str, default=None, help="JSON output file path")
    args = parser.parse_args()

    platforms = list(BENCHMARKS) if args.platform == "all" else [args.platform]

    print(f"═══ TRR Scraper Benchmark — BravoTV ═══")
    print(f"Date window: {DATE_START.strftime('%Y-%m-%d %H:%M')} → {DATE_END.strftime('%Y-%m-%d %H:%M')}")
    print()

    results = {}
    for platform in platforms:
        handle = BRAVO_HANDLES[platform]
        print(f"── {platform.upper()} (@{handle}) ──")
        result = BENCHMARKS[platform]()
        results[platform] = result

        status = result["status"]
        if status == "skip":
            print(f"  SKIPPED: {result['reason']}")
        elif status == "ok":
            count_key = next(
                (k for k in ("posts", "videos", "tweets") if k in result), "items"
            )
            count = result.get(count_key, 0)
            print(f"  OK: {count} {count_key} in {result['duration']}")
            for s in result.get("sample", []):
                print(f"    • {json.dumps(s, default=str)}")
        else:
            print(f"  ERROR in {result['duration']}: {result['error']}")
            if result.get("traceback"):
                for line in result["traceback"].strip().split("\n")[-5:]:
                    print(f"    {line}")

        if result.get("meta") and isinstance(result["meta"], dict):
            # Print all scalar meta values (skip large nested objects like raw payloads)
            meta_summary = {
                k: v for k, v in result["meta"].items()
                if isinstance(v, (str, int, float, bool, type(None)))
            }
            if meta_summary:
                print(f"  meta: {json.dumps(meta_summary, default=str)}")

        print()

    # Summary table
    print("═══ Summary ═══")
    for p in platforms:
        r = results[p]
        s = r["status"]
        count_key = next(
            (k for k in ("posts", "videos", "tweets") if k in r), "items"
        )
        count = r.get(count_key, "-")
        dur = r.get("duration", "-")
        err = r.get("error", "")[:60] if s == "error" else ""
        reason = r.get("reason", "") if s == "skip" else ""
        print(f"  {p:12s}  {s:6s}  {str(count):>4s} items  {dur:>8s}  {err}{reason}")

    # JSON output
    output_path = args.output
    if output_path is None:
        output_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "docs", "ai", "benchmarks")
        os.makedirs(output_dir, exist_ok=True)
        ts = NOW.strftime("%Y%m%dT%H%M%SZ")
        output_path = os.path.join(output_dir, f"bravotv_benchmark_{ts}.json")
    payload = {
        "generated_at": NOW.isoformat(),
        "date_start": DATE_START.isoformat(),
        "date_end": DATE_END.isoformat(),
        "handles": BRAVO_HANDLES,
        "platforms": {p: _sanitize_result(results[p]) for p in platforms},
    }
    with open(output_path, "w") as f:
        json.dump(payload, f, indent=2, default=str)
    print(f"\nResults written to: {output_path}")


def _sanitize_result(result: dict) -> dict:
    """Remove traceback for JSON output, keep everything else."""
    clean = {k: v for k, v in result.items() if k != "traceback"}
    return clean


if __name__ == "__main__":
    main()
