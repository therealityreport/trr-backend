# TikTok Posts Scrapling — Signature Gap Decision

**Status:** Open. Requires decision before committing more engineering to the TikTok Scrapling lane.

## Problem

The TikTok posts Scrapling fetcher uses the hybrid pattern (Patchright warmup + httpx for API calls). Phase C smoke test revealed that `https://www.tiktok.com/api/user/detail/?uniqueId={handle}` returns non-JSON HTML even after a successful browser warmup. Root cause: TikTok requires three query params computed by JavaScript in `webmssdk.js`:

- `X-Bogus` — hash derived from URL + user-agent + request params
- `_signature` — separate cryptographic signature
- `msToken` — session token (appears in cookies, but also expected as a query param)

The Patchright browser computes these when loading the profile page, but the httpx client does not. Result: `statusCode != 0` and body contains HTML → classified as `challenge_or_blocked`.

## Observed evidence

- `scripts/socials/tiktok/smoke_posts_scrapling.py --account bravotv --max-pages 1` fails with `error_message: TikTok user detail failed: non_json_response`
- Warmup request (`https://www.tiktok.com/@bravotv`) returns 200 — confirms cookies are valid and the browser is accepted
- The subsequent `/api/user/detail/` call from httpx returns HTML, not JSON
- Existing codebase has zero helpers for generating X-Bogus (confirmed via `grep -r "X-Bogus\|_signature" trr_backend/`)

## Options

### A — Port X-Bogus signature generator to Python

**Approach:** Reverse-engineer `webmssdk.js` and translate the hash functions into Python. Open-source implementations exist (e.g., GitHub search for "tiktok xbogus python").

**Pros:** Pure-Python after warmup. Preserves async/concurrent httpx transport. Lowest ongoing cost per request.

**Cons:** TikTok rotates signature logic; breaks silently when the algorithm changes. Medium risk of being detected as a script.

**Cost:** ~3 days to port + ~1 day/quarter to chase algorithm changes.

### B — Browser-driven API calls (keep Patchright after warmup)

**Approach:** Instead of handing off to httpx after warmup, use `page.evaluate('fetch(...)')` inside Patchright to make every API call. Browser computes signatures automatically.

**Pros:** Robust to signature rotation. Zero custom crypto.

**Cons:** Slow (~2-5s per API call vs ~100ms for httpx). Defeats the async-httpx speed advantage. Higher memory footprint.

**Cost:** ~1 day to wire up. Higher per-job runtime + infrastructure.

### C — Use Apify's managed TikTok scraper

**Approach:** Replace the posts Scrapling lane for TikTok with an Apify actor call. Already using Apify for Instagram fallback.

**Pros:** Known-working, maintained by third party. Zero fingerprinting surface.

**Cons:** $0.30–$3.00 per 1000 videos depending on actor. Vendor lock-in. No control over internals.

**Cost:** ~1 day to wire. Ongoing $ cost per scrape.

### D — Accept the current gap; lean on yt-dlp for TikTok posts

**Approach:** Retire the TikTok Scrapling lane entirely. yt-dlp (already the TikTok production default) continues to serve all post scraping; Scrapling stays Instagram-only.

**Pros:** Zero ongoing cost. Simplest.

**Cons:** TikTok comments (if we ever want them) still blocked. Loses the proxy rotation benefit of DECODO.

**Cost:** ~30 min to remove the lane wiring and dispatch branch. Could keep the code in-repo marked "archived" for future reuse.

## Decision criteria

Before choosing, resolve:

1. **How often does TikTok rotate its signature algorithm?** If >1x/quarter, A is a maintenance burden.
2. **What's our $ budget for TikTok data?** If tight, A or D. If loose, C is simplest.
3. **Do we need real-time TikTok post data or is nightly batched OK?** If nightly, B is viable despite slowness.
4. **Will we ever want TikTok comments?** If yes, D is insufficient.

## Recommendation

**Short-term (next 2 weeks):** Option D — mark the TikTok Scrapling lane as archived, delete the dispatch branch, keep the package source for reference. Don't pay ongoing maintenance costs for a lane that doesn't work without more work.

**If/when TikTok real-time posts become strategically important:** Revisit with Option B (browser-driven) for a quick unblock, then migrate to Option A if volume justifies.

## Tracking

- Phase E task: make final TikTok signature-gap decision and act on it
- Related: if we stay with Option A, prioritize a `test_signature_generation_matches_browser` integration test that cross-checks our Python hash against a live Patchright page.eval response

## Other Phase E follow-ups (unrelated to TikTok signature gap)

These items surfaced during Phase D cross-task review but were out of scope. Tracked here so a Phase E starter can find them in one place:

- **Migrate comments_scrapling fetcher to use `trr_backend/socials/_scrapling_http_utils.py`.** Phase D Task 1 extracted 5 shared helpers (`env_truthy`, `response_text`, `status_code`, `safe_location`, `extract_response_cookies`) and migrated both posts_scrapling fetchers, but the comments_scrapling fetcher still inlines its own copies at `trr_backend/socials/instagram/comments_scrapling/fetcher.py` lines 45, 57, 67, 76, 93. The migration is mechanical (same aliased-import pattern Task 1 used) but should land with a regression run of the comments retry suite.

- **Scrub raw cookie values from `comments_scrapling.fetcher.runtime_metadata`.** Phase D Task 2 fixed the cookie-value leak in both posts_scrapling fetchers (now exposes only `warmup_cookie_names` + `warmup_cookie_count`), but the comments_scrapling fetcher at `trr_backend/socials/instagram/comments_scrapling/fetcher.py` line 183 still serializes the full `warmup_cookie_delta: dict[str, str]` into `social.scrape_jobs.metadata.fetcher_runtime`. This persists session cookies (sessionid, csrftoken, etc.) in plaintext where any operator with read access to the metadata column can see them. Apply the same fix as Task 2.

- **Orphaned-run cleanup pattern.** Phase D `start_instagram_posts_scrapling_scrape` and `start_tiktok_posts_scrapling_scrape` (and the comments-lane equivalent they were modeled on) leave a `scrape_runs` row in status `queued` if `_create_job` fails after `_create_run` succeeds. The advisory lock prevents double-starts, but the orphan blocks future enqueues for that account until manually cleared. Either wrap both calls in a single transaction or add a finally-block cleanup.
