# TikTok vs. Instagram Scrapers — Comparison After Bug Fixes

**Date:** 2026-04-14
**Baseline:** `main` at c0ca8e2 plus bug fixes #1–#10 + Phase B runtime
scaffolding (see `.claude/plans/fancy-beaming-dijkstra.md`).

## Handoff Snapshot
```yaml
handoff:
  include: true
  state: recent
  last_updated: 2026-04-14
  current_phase: "social scraper comparison"
  next_action: "Use this comparison as continuity context for follow-up Instagram runtime hardening"
  detail: self
```

---

## 1. Architectural Summary

| Dimension             | TikTok                                    | Instagram                                                  |
|-----------------------|-------------------------------------------|------------------------------------------------------------|
| Primary path          | `yt-dlp` for posts (production default)   | Crawlee/Playwright → GraphQL / `i/api/v1` JSON             |
| Secondary path        | Direct HTTP via `_TikTokHttpClientBase`   | Apify actor (`apify_scraper.py`) for bulk backfill         |
| Tertiary path         | Playwright interception (experimental)    | Permalink HTML (via `permalink_metadata.py`)               |
| Impersonation         | Optional `curl_cffi` with Chrome profile  | None at transport layer — relies on cookie freshness       |
| Identity management   | Single session; proxy URL optional        | `InstagramIdentityPool` w/ rotation, strikes, generations  |
| Cookie freshness      | `cookie_refresh.py` passive               | `auth_resolver.py` active GraphQL validation + cache       |
| Hot-path auth failure | 401 retried (transient)                   | 401/403 **now non-retryable** post-bug-#5/#6               |
| Concurrency model     | Thread pool inside scraper                | Single-threaded + async-ready runtimes via Protocol        |
| LoC (scraper.py)      | ~2,200 lines                              | ~4,100 lines (largest surface)                             |

## 2. What the Bug Fixes Changed

| Bug | Scraper | Observable improvement                                                                      |
|-----|---------|----------------------------------------------------------------------------------------------|
| #1  | TikTok  | True exponential backoff w/ jitter — TikTok rate-limit bucket recovery no longer starved    |
| #2  | TikTok  | Return-type annotation matches reality; mypy now catches misuse                              |
| #3  | TikTok  | Malformed media payloads (missing `id`) no longer slip through as false matches              |
| #4  | TikTok  | Playwright `context.close()` runs before `browser.close()`; FDs released under concurrency   |
| #5  | IG      | 401 terminates fast; identity rotation triggered instead of retry storm                      |
| #6  | IG      | 403 terminates fast; identity is retired rather than hammering a shadowbanned account        |
| #7  | IG      | `require_validation=False` is a real skip — no unnecessary cookie refresh loops              |
| #8  | IG      | `_is_expired_for_age` is pure; caller explicitly retires aged identities                     |
| #9  | IG      | Fresh `Set-Cookie` values merged into session — auth rotations persist across requests       |
| #10 | IG      | Apify timestamp-format drift is now visible in logs (DEBUG with `exc_info`)                  |

## 3. Reliability Failure Modes

### TikTok (before vs. after)

| Failure mode                     | Before                        | After                                              |
|----------------------------------|-------------------------------|----------------------------------------------------|
| TikTok 429 rate limit            | linear backoff starves recovery | exponential + jitter; recovers within 3–4 retries |
| Malformed `itemStruct`           | returns empty-id item (garbage)| returns `None`; caller falls through               |
| Playwright crash mid-scrape      | `browser` closed, `context` leaked | both closed with per-op try/except             |
| `build_tiktok_http_client` abuse | `requests.Session` interface typed but wrong object | correct Protocol surface |

### Instagram (before vs. after)

| Failure mode                     | Before                        | After                                              |
|----------------------------------|-------------------------------|----------------------------------------------------|
| 401 on aged cookies              | retry loop burns pool         | fail-fast + retire identity                        |
| 403 (shadowban)                  | retry accelerates suppression | fail-fast + retire identity                        |
| Skip-validation call             | cookies flagged invalid → refresh loop | cookies treated as valid                  |
| Aged identity during `acquire()` | state mutated inside filter predicate | pure check, explicit retirement              |
| Rotated cookies from IG response | silently discarded            | merged into session jar                            |
| Apify schema drift               | silent `posted_at=None`       | DEBUG log includes raw value + exception           |

## 4. Remaining Gaps — by scraper

**TikTok**
- `cookie_refresh.py` still has no TTL check on refreshed cookies (Quality #5, deferred).
- Bare `except Exception` with `noqa: BLE001` at ~6 sites (Quality #4, deferred).
- `_rate_limit` in `scraper.py` duplicates the new shared retry logic; migration to
  `trr_backend.socials._retry` is the natural follow-up.

**Instagram**
- 21+ `except Exception` sites in `scraper.py` alone (Quality #4, deferred).
- `InstagramErrorCode` enum is defined in `constants.py` but call sites still use
  bare strings (partial Quality #2 — enum added, migration pending).
- `cookie_refresh.py` writes refreshed cookies with no `issued_at` stamp
  (Quality #5, deferred).
- Crawlee adapter has blocking `requests.*` calls in async context (Quality #7,
  deferred — needs `asyncio.to_thread(...)` or `httpx` migration).

## 5. Phase B (fallback runtimes) — status

The `trr_backend/socials/instagram/runtimes/` package is scaffolded:
- **Protocol + DTOs** — live (`protocol.py`)
- **Dispatcher** — live (`dispatcher.py`) with test coverage (5 tests pass)
- **CrawleeRuntime** — live wrapper around existing scraper
  (posts + profile wired; `fetch_post_detail` deferred)
- **ScraplingRuntime** — scaffold, healthcheck only; implementation raises
  `NotImplementedError` with explicit TODO pointing at
  https://github.com/D4Vinci/Scrapling
- **Crawl4aiRuntime** — scaffold, healthcheck only; TODO points at
  https://docs.crawl4ai.com/api/arun/
- **BrowserUseRuntime** — scaffold, auth-recovery-only surface; TODO points at
  https://docs.browser-use.com/quickstart

The stub `NotImplementedError` is intentional: the underlying libraries have
shifted signatures multiple times across minor versions. Implementing from
memory would ship broken code. Dedicated follow-up sessions should pin each
version (`pip show ...`), verify signatures against live docs, then implement.

## 6. Recommended Next Actions (in priority order)

1. **Commit the bug fixes + Phase B scaffolding** as one atomic PR — all 314
   socials tests pass.
2. **Implement ScraplingRuntime first** — highest ROI (JSON path, no
   browser, no LLM cost). Gate behind `INSTAGRAM_RUNTIME_ORDER` env var
   so default production traffic still uses Crawlee until it's proven.
3. **Migrate `InstagramErrorCode` enum across raise sites** — pure
   search-and-replace, but with unit tests guarding each branch.
4. **Quality #5 (cookie TTL)** — single-file surface in `cookie_refresh.py`,
   prevents a class of silent-auth-failure reports.
5. **Quality #4 (narrow `except Exception`)** — do as opportunistic part of
   any future scraper touch; full-file sweep is low ROI.

## 7. Verification Commands

```bash
cd TRR-Backend
python -m pytest tests/socials/ -q \
  --ignore=tests/socials/test_facebook_engagement.py \
  --ignore=tests/socials/test_threads_scraper.py \
  --ignore=tests/socials/test_facebook_threads_recon_gate.py
# expect: 314 passed

python -m pytest \
  tests/socials/tiktok/test_bug_fixes.py \
  tests/socials/test_instagram_bug_fixes.py \
  tests/socials/test_retry.py \
  tests/socials/instagram/runtimes/test_dispatcher.py -v
# expect: 29 passed (regression suite for this change)
```
