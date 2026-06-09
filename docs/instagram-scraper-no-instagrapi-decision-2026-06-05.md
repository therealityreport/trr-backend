# Instagram posts scraper — keep web-GraphQL + browser warmup; do not adopt instagrapi

Date: 2026-06-05
Status: Accepted

## Context

- Instagram "Backfill Posts" runs every job through the Scrapling web-GraphQL cursor path: `trr_backend/socials/instagram/posts_scrapling/fetcher.py` (`InstagramPostsScraplingFetcher`), driven by `posts_scrapling/job_runner.py`. It does a browser warmup (Patchright/StealthyFetcher) to extract runtime tokens (`lsd`, `bloks_version`, `__spin_*`, `hsi`), bridges cookies into an httpx client, then paginates the private web GraphQL endpoint with `doc_id` rotation.
- The live failure mode (see workspace `instagram-backfill-debug-report.md`) is 401/403 on cursor pagination after 0–660 posts — an auth/rate-limit problem, addressed separately via Decodo residential proxy + sticky-IP-per-identity + identity rotation + cross-process 401/403 cooldown.
- We evaluated replacing the hand-rolled fetcher with `instagrapi` (a maintained Python Instagram private-API client).

## Decision

Do NOT replace the web+browser path with instagrapi.

## Reasons (against replacement)

1. instagrapi targets the **private mobile API** (`/api/v1/`), which is more heavily fingerprinted and rate-limited than the web GraphQL endpoint, and is actively pattern-blocked. Instagram's 2024–2025 defenses block raw `requests`/`httpx` by TLS fingerprinting — TRR already mitigates this with `curl_cffi` + a real browser warmup, which instagrapi's default transport does not replicate.
2. Switching discards the browser-warmup camouflage and would require re-implementing the ~17 responsibilities the current fetcher owns: browser warmup + token extraction, cookie bridging, `doc_id` rotation, per-page/identity proxy binding, cross-process advisory-lock pacing, transient backoff, cooldown recording, auth-failure classification, warmup pooling, bidirectional probe, pagination-state persistence, and runtime metadata.
3. instagrapi's own best practices (keep one stable IP per account, persist sessions, don't rotate proxy mid-challenge, use realistic delays) describe higher ban risk and reinforce a conservative approach — they do not argue for a swap.

## What we reuse from instagrapi (without depending on it)

- Its error taxonomy as a model for our 401/403 classification: `ChallengeRequired`, `LoginRequired`, `PleaseWaitFewMinutes` (a.k.a. `feedback_required`), and `FeedbackRequired`. These map onto our cooldown / stop-rule logic (`trr_backend/socials/instagram/auth_cooldown.py`).
- Its best practices (sticky IP per identity, session persistence, `delay_range`) which already back our Decodo sticky-session + identity-rotation design.

## Consequences / future option

We may later add instagrapi as an isolated *fallback runtime* under `trr_backend/socials/instagram/runtimes/` (alongside the existing crawlee/scrapling/crawl4ai/browser_use runtimes), marked `RuntimeUnsupported` until verified — but it is NOT wired now. `runtimes/protocol.py` already defines the `RuntimeUnsupported` sentinel and `runtimes/__init__.py` documents the "route per-endpoint across multiple runtimes rather than replace" philosophy that such a fallback would follow.
