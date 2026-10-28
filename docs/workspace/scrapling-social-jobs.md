# Scrapling Social Jobs

Last reviewed: 2026-10-19

This note covers shared Scrapling practices for TRR social scraping jobs. It is
platform-neutral and applies to Instagram, TikTok, Threads, SocialBlade, and any
future Scrapling-backed social lane unless a lane runbook narrows the behavior.

## Source Rules

- Official Scrapling docs define API behavior, supported arguments, and upgrade
  expectations.
- Scrapling_dev public posts are field notes: tips, ideas, examples, and use
  cases that can inspire better defaults or diagnostics after local validation.
- TRR source code and tests decide whether an idea belongs in these lanes.
- No live scrape is part of a routine readiness check. Live requests still need
  the lane-specific operator gate.

## Scrapling 0.5 compatibility

Runtime and Modal browser inputs require `scrapling[fetchers]>=0.5,<0.6`;
the separate operator environment pins `scrapling[fetchers]==0.5`. Compile the
three lockfiles with their recorded Python 3.11/Linux header commands. The
upgrade requires curl-cffi 0.16.1, Playwright 1.62.0, Patchright 1.62.1,
cssselect 1.5.0, and apify-fingerprint-datapoints 0.15.0 or newer.

Browser fetches use `StealthyFetcher`; network capture uses its async session
counterpart, `AsyncStealthySession(record_requests=True)`. SocialBlade records
TRPC responses, and TikTok records API paths only when
`SOCIAL_TIKTOK_POSTS_CAPTURE_XHR=1`. Each capture session retains at most 100
completed requests; `session.network.search(url_pattern=..., limit=None)` reads
all retained matching responses before the async context closes. This replaces
removed `capture_xhr` and `response.captured_xhr` APIs. Paths in runtime metadata
omit query strings. Response bodies and request details stay private.

The Modal image continues to run `scrapling install --force`. Upstream 0.5 still
supports this command and refreshes Chromium, system dependencies, and TLD data.
Source checks do not prove browser assets or deployed runtime readiness.

## Runtime Defaults

The shared helper keeps current behavior unchanged when no tuning env vars are
set. Optional browser tuning is opt-in and limited to arguments that do not
replace lane-owned timeout, proxy, cookie, retry, or navigation policy.

Supported env prefixes:

| Lane | Prefix |
|---|---|
| Instagram posts | `SOCIAL_INSTAGRAM_POSTS_SCRAPLING` |
| TikTok posts | `SOCIAL_TIKTOK_POSTS_SCRAPLING` |
| Threads posts | `SOCIAL_THREADS_POSTS_SCRAPLING` |
| SocialBlade | `SOCIALBLADE_SCRAPLING` |

Set options either as individual env vars or as one JSON bundle:

```bash
SOCIAL_INSTAGRAM_POSTS_SCRAPLING_BLOCK_ADS=1
SOCIAL_INSTAGRAM_POSTS_SCRAPLING_SCRAPLING_FETCHER_OPTIONS='{"block_ads": true, "block_webrtc": true}'
```

Currently allowed tuning keys for the active posts and SocialBlade lanes:

```text
additional_args
ai_targeted
allow_webgl
block_ads
block_webrtc
blocked_domains
dns_over_https
google_search
hide_canvas
init_script
real_chrome
selector_config
solve_cloudflare
useragent
wait_selector
wait_selector_state
```

Invalid values are ignored and recorded in metadata. This keeps production
defaults stable while making experiments visible.

The Instagram comments lane records shared Scrapling fetcher metadata, but it
does not yet accept optional browser tuning env vars because that worker already
has a larger active retry/proxy surface.

## Metadata

Scrapling jobs should include these redaction-safe fields in runtime metadata:

| Field | Meaning |
|---|---|
| `scrapling_fetcher_class` | Fetcher boundary used by the lane |
| `scrapling_browser_tuning` | Configured and invalid opt-in tuning keys |
| `scrapling_observed_proxy_labels` | Host-level proxy labels without secrets |
| `scrapling_observed_proxy_count` | Number of distinct observed proxy labels |

Metadata must not include cookies, raw proxy URLs with credentials, browser
profile paths, session ids, request bodies, or account secrets.

## Process

When translating Scrapling docs or field notes into TRR changes:

1. Prefer one shared helper when the behavior applies across platforms.
2. Keep lane defaults unchanged unless tests and runbooks explicitly document
   the new default.
3. Add or update a unit test for parsing, redaction, and fail-closed behavior.
4. Verify at least one non-Instagram lane when a shared helper changes.
5. Keep platform-specific fixes in the lane when they depend on that site's
   auth, pagination, or response shape.
