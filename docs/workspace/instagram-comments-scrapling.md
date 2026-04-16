# Instagram Comments Scrapling Lane — Operator Runbook

Last reviewed: 2026-04-15

This document covers the standalone Instagram comments scraper built on
[Scrapling](https://github.com/D4Vinci/Scrapling) v0.4+. It runs as a
dedicated worker lane (`instagram_comments_scrapling`) alongside the existing
Instagram posts pipeline and writes to the shared `social.instagram_comments`
table. No schema forks, no second scheduler.

---

## Architecture at a glance

```
UI (/social/:platform/:handle/comments)
  └─ POST /api/v1/admin/socials/profiles/:platform/:account_handle/comments/scrape
       └─ enqueues job with config.required_worker_lane="instagram_comments_scrapling"
            └─ comments worker (wrapper over shared scripts/socials/worker.py)
                 └─ InstagramCommentsScraplingFetcher → StealthyFetcher (Patchright)
                      └─ persists into social.instagram_comments (shared table)
```

Key identities:

| Thing                 | Value                                   |
|-----------------------|-----------------------------------------|
| Worker lane           | `instagram_comments_scrapling`          |
| Job stage             | `comments_scrapling`                    |
| Platform filter       | `instagram` (lane does not accept others) |
| Default proxy provider | Decodo/Smartproxy (env-configurable)   |
| Canonical UI URL      | `/social/:platform/:handle/comments`    |

The lane runs a separate OS process from the posts worker. A crash in the
Scrapling / Patchright stack cannot take down the posts pipeline, and a
stale Instagram cookie manifests as a single failed job rather than a
cross-lane outage.

---

## Initial setup (local dev)

```bash
cd TRR-Backend
./scripts/setup_scrapling.sh
```

That script does three things, in order:
1. `pip install -r requirements.lock.txt` — pulls in `scrapling==0.4.6`.
2. `scrapling install` — downloads the Patchright/Playwright browser
   binaries that `StealthyFetcher` needs. Skipping this is the classic
   reason the lane crashes on first run with "browser binary not found".
3. Smoke import — proves `from scrapling.fetchers import StealthyFetcher`
   works in the venv.

Idempotent — safe to re-run after pulling upgrades.

---

## Environment variables

Required only for **production** runs; local dev works with cookies alone.

```bash
# Proxy (Decodo default; swap provider by changing _PROVIDER + _URLS)
SOCIAL_INSTAGRAM_COMMENTS_PROXY_PROVIDER=decodo
SOCIAL_INSTAGRAM_COMMENTS_PROXY_URLS=            # comma-separated URLs, overrides provider
SOCIAL_INSTAGRAM_COMMENTS_USE_STICKY_PROXY=false
SOCIAL_INSTAGRAM_COMMENTS_PROXY_SESSION_TTL_SECONDS=600

# Run caps (defense against runaway jobs)
SOCIAL_INSTAGRAM_COMMENTS_MAX_POSTS_PER_RUN=50
SOCIAL_INSTAGRAM_COMMENTS_MAX_COMMENTS_PER_POST=200

# Browser mode (false for visual debugging)
SOCIAL_INSTAGRAM_COMMENTS_HEADLESS=true

# Decodo-specific (when PROVIDER=decodo and no explicit _URLS)
DECODO_USERNAME=...
DECODO_PASSWORD=...
DECODO_GATEWAY=gate.decodo.com:7000

# Worker lane scaling
TRR_SOCIAL_INGEST_WORKER_ENABLED=1
TRR_SOCIAL_INGEST_WORKER_COMMENTS_SCRAPLING=1    # default 0 — opt in explicitly
```

---

## Starting workers

### Local dev

```bash
# Single-worker, manually
SOCIAL_WORKER_LANE=instagram_comments_scrapling \
  python -m scripts.socials.instagram.comments_worker --parallel 1 --interval 3

# Multi-worker via launcher (matches production shape)
TRR_SOCIAL_INGEST_WORKER_ENABLED=1 \
TRR_SOCIAL_INGEST_WORKER_COMMENTS_SCRAPLING=1 \
  ./scripts/start_remote_job_workers.sh
```

### Production (Modal)

Modal deploy is driven by `trr_backend/modal_jobs.py`. The social browser
image already installs Scrapling + browser binaries (see the
`_SOCIAL_BROWSER_SETUP_COMMANDS` constant which now includes both
`playwright install chromium` and `scrapling install`). Deploy via the
standard modal pipeline; no lane-specific Modal config is required beyond
ensuring the comments worker process is launched with the env vars above.

---

## One-shot debugging

When the lane misbehaves in production, reproduce locally in non-headless
mode to watch the browser:

```bash
SOCIAL_INSTAGRAM_COMMENTS_HEADLESS=false \
  python -m scripts.socials.instagram.comments_scrape_cli \
    --shortcode=<SHORTCODE> \
    --max-comments=20
```

This bypasses the queue and runs the fetch inline so you can see the
Patchright window, the cookie state, and the requests being made.

---

## Known failure modes and remediation

### 1. `SOCIAL_WORKER_UNAVAILABLE` from the comments-scrape route

**Symptom.** Comments tab shows "No Instagram comments worker is online..."

**Cause.** Nothing is heartbeating the `instagram_comments_scrapling` lane.

**Fix.** Start the worker. For local dev:
```bash
TRR_SOCIAL_INGEST_WORKER_ENABLED=1 \
TRR_SOCIAL_INGEST_WORKER_COMMENTS_SCRAPLING=1 \
  ./scripts/start_remote_job_workers.sh
```
For production: ensure the Modal deployment manifest sets
`TRR_SOCIAL_INGEST_WORKER_COMMENTS_SCRAPLING>=1`. Never work around this by
dropping the lane-enforcement check — the check is what prevents the main
posts worker from silently stealing Scrapling jobs it can't run.

### 2. `instagram_comments_auth_failed`

**Symptom.** Job fails with "Instagram auth failed while fetching comments..."

**Cause.** Cookies expired or IG pushed a login checkpoint.

**Fix.** Refresh cookies via `scripts/socials/cookie_refresh_worker.py`
(same refresh flow the posts pipeline uses). Once a fresh `sessionid` is
saved, the next queued job auto-uses it.

### 3. Browser binary not found at worker startup

**Symptom.** Worker logs include something like
`Executable doesn't exist at /root/.cache/ms-playwright/chromium-*`.

**Cause.** `scrapling install` / `playwright install chromium` wasn't run
in the image.

**Fix.** For local: rerun `./scripts/setup_scrapling.sh`. For Modal:
confirm `_SOCIAL_BROWSER_SETUP_COMMANDS` in `trr_backend/modal_jobs.py`
still contains `"scrapling install"`, and redeploy the browser image.

### 4. IG rotates the GraphQL `doc_id` / HTML structure

**Symptom.** Jobs start returning empty `comments=[]` for posts you know
have comments; `fetch_reason` is `api_status_fail` or `non_json_response`.

**Cause.** Instagram periodically rotates its private GraphQL `doc_id`
params (roughly every 2–4 weeks) or changes the shape of the JSON response
around `has_more_comments` / `next_min_id`.

**Fix.**
1. Reproduce via the one-shot CLI in non-headless mode (§ One-shot debugging).
2. Open the DevTools network tab in the Patchright window and capture the
   actual request Instagram makes to `/api/v1/media/:id/comments/`. Update
   `trr_backend/socials/instagram/constants.py::COMMENTS_URL` and any
   `params` in `fetcher.py::fetch_comments_for_shortcode`.
3. Update the fixture the parser tests load from
   `tests/fixtures/instagram/comments/` and rerun pytest.
4. Bump the relevant Scrapling version pin if the response shape change
   coincides with a Scrapling release; run the P1-4 contract test first.

### 5. Proxy bandwidth exhausted mid-run

**Symptom.** 407 / auth-fail responses from the proxy after a burst of
successful fetches.

**Cause.** Decodo (or whichever provider) balance ran out.

**Fix.** Top up the proxy account, OR add a new provider URL via
`SOCIAL_INSTAGRAM_COMMENTS_PROXY_URLS=...` (highest precedence, overrides
provider-specific builder). The fetcher's retry/backoff (P1-5) will
re-attempt transient failures up to three times with exponential backoff
before surfacing `retryable=true` so the queue requeues.

### 6. Transient 429 or 5xx from Instagram

**Symptom.** Job metadata shows `error_code=http_429` or `http_5xx` but
job status is `retrying` (not `failed`).

**Cause.** Expected. The fetcher retries up to 3 times with exponential
backoff, honoring `Retry-After`. After that the job is marked retryable
and the queue requeues on its own.

**Fix.** No action unless retries persistently fail — in that case the
proxy pool is likely flagged; swap to a fresh IP set.

---

## Implementation note: "separate daemon" vs shared `worker.py`

The plan called for a "separate daemon". The implementation satisfies
that requirement via **a separate OS process** running the thin wrapper
at `scripts/socials/instagram/comments_worker.py`, which imports and
invokes `scripts.socials.worker.main()` with a prepended
`--stage comments_scrapling --platform instagram` and
`SOCIAL_WORKER_LANE=instagram_comments_scrapling`. Process isolation is
preserved (a Scrapling crash cannot take down the posts worker). Code
sharing via the shared worker module means the lane inherits heartbeat,
claim-loop, and recovery behavior for free — exactly what's desired.

---

## Tests to run before shipping changes to this lane

```bash
cd TRR-Backend
.venv/bin/ruff check trr_backend/socials/instagram/comments_scrapling \
  scripts/socials/instagram/ api/routers/socials.py
.venv/bin/ruff format --check trr_backend/socials/instagram/comments_scrapling \
  scripts/socials/instagram/
.venv/bin/pytest -q \
  tests/socials/test_instagram_comments_scrapling*.py \
  tests/scripts/test_instagram_comments_worker.py

cd ../TRR-APP
pnpm -C apps/web run lint
pnpm -C apps/web exec next build --webpack
pnpm -C apps/web exec vitest run social-account-profile-page.runtime show-admin-routes
```

All three sections must pass before merge.

---

## Out of scope for this lane

- instagrapi / private mobile API fallback.
- Replacing the existing `requests` + GraphQL posts scraper.
- Broad migration of other admin URL tabs from `/admin/social/` to
  `/social/` (only the comments tab was migrated; see the Locked
  Decisions in the fix plan).
- Second scheduler / second queue table.
