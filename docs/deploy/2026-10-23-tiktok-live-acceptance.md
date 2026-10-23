# TikTok live acceptance — 2026-10-23

## Scope

Continuation of chat `01a16977-e279-7a40-a0b8-849e3d407dcd` (Review TikTok scraper status).
Verify database capacity, two bounded persisted runs, deployed catalog retry and
coverage, TRR-profile admin controls, and three target video mirrors. Comments
remain disabled. No new providers or proxy purchases were made.

## Capacity and source repair

- Verified Supabase project `vwxfvzutyufrkhfgoeaa` through the scoped MCP, local
  link, and runtime connection binding.
- Increased the existing Supavisor default pool from the effective 15-session
  limit to 25. Readback confirmed 25; API health changed from 503
  `session_pool_capacity` to 200 with database connected. No compute-tier change.
- Subsequent concurrent database I/O caused intermittent admin query timeouts;
  restoring the connection limit is not proof that all database latency is fixed.
- Fixed `_shared_account_posts_stage_for_platform`: TikTok catalog jobs must use
  `shared_account_posts`, while explicit posts-only runs retain their dedicated
  Scrapling stage. The old mapping bypassed resumable public catalog persistence.
- Source commit: `86edb5e1`. Regression reproduced before the fix; four selected
  repository tests and 21 public catalog/profile tests passed afterward.
- App follow-up repairs are recorded below. No production app build or deployment was performed.

## Deployment provenance

Target: `admin-56995 / main / trr-backend-jobs`, app
`ap-DkLTRoSvqhbkGO7fHyHxrD`.

The standard `make dev-hybrid` preflight automatically reconciled runtime as v134
from the current working tree. This included existing unrelated dirty ingestion
work. The intended TikTok repair then deployed as v135, tag
`tiktok-catalog-routing-01a16a88`, with explicit-main readiness and API cold-start
checks passing. The deployment recorded `70ff95e*`; the routing fix was committed
subsequently as `86edb5e1`. A concurrent Instagram task later deployed v136,
`ig-missing-data-77cac4944569`, recording `86edb5e*`. Existing unrelated edits and
newer deployment were preserved. Do not roll back the whole app merely to revert
this two-file source change; reconcile concurrent work first.

## Bounded persistence

| Account | Run | Job | Result |
| --- | --- | --- | --- |
| bravotv | `0f2c0d54-e42f-4a82-96de-02be2398f4e2` | `471e727b-9bd9-4c19-8f13-8f794cb6b563` | completed, 150 canonical posts |
| bravowwhl | `8e2b02c2-25d3-4b7f-87f3-958a20a32e58` | `73b5c2c6-f602-4abf-b2f3-a4aa041fc770` | completed, 150 canonical posts |

Both ran through durable deployed `run_social_posts_job` calls with `max_pages=5`
and posts-only ingestion. Both used the existing yt-dlp fallback, reported
`pages_scanned=0`, and stopped at the 150-post cap. These are persisted bounded
runs, **not proof of five native pages**. No comments were persisted.

## Catalog and admin checks

- Corrected full-history bravotv run: `9326b8c0-bbdb-4199-8a4b-5f1c2d7edc3e`;
  job `c1d64218-d5f1-420b-b9da-d422ccf34fcc`.
- It saved 19 posts from one anonymous native page and retained its version-2
  checkpoint. Expected total remained 12,461. It did not reach full history.
- A bounded deployed-browser diagnostic observed a valid 19-item first response
  with `hasMore=true`, followed by an unreadable response. The document scrolled;
  no evidence supports calling this native exhaustion. The stalled task-owned
  run was cancelled through the repository control plane to release capacity.
- Opened the friendly TRR Chrome profile and local hybrid admin at
  `https://admin.trr.localhost`; its workers are deployed Modal workers.
- Backfill Posts launched WWHL run `589dec0d-dda8-4817-a88e-6d80f39aea04`, job
  `3f1d683e-aa4d-48a6-9328-e5cb4f7e99e2`. The UI showed queued, then running;
  TikTok's one-worker platform cap explained the initial wait.
- Set that queued test job's per-attempt native page budget to one, preserving
  full-history scope and expected total, to exercise incomplete/retry behavior.
- The first Cancel Run confirmation stalled browser control and did not submit
  a cancellation request. A fresh task tab recovered the admin view. Do not
  count the first click as verified cancellation.

## Three target videos

| Video ID | Post UUID | Verified MP4 bytes |
| --- | --- | ---: |
| `7699534476111547662` | `7d5f5951-fc40-4fc8-9a5e-22a9d8aec5b7` | 5,110,252 |
| `7699538168709762318` | `3826eb35-5ec9-4a0c-8cfd-e7464b35c00b` | 23,286,886 |
| `7699553575608798478` | `b0045bcb-08c3-4527-8d0e-9c8963524f7c` | 14,081,421 |

Deployed media workers populated one hosted video URL per row. Authenticated R2
HEAD/range readbacks verified object bytes, `video/mp4`, and MP4 signatures.
The existing managed bucket hostname also returned HTTP 206 and matching byte
ranges for all three. The canonical row identities were preserved.

The configured `media.thereality.report` hostname returns NXDOMAIN. Cloudflare
reports its R2 custom domain enabled, but authoritative nameservers are WordPress.
Read-only WordPress domain inventory did not expose the domain; the current
Cloudflare token returns 403 for zone/DNS access. No WordPress or DNS changes were
made. Public reads through the configured custom hostname remain unverified;
the managed hostname is diagnostic evidence, not a production URL migration.

## Evidence

Private local receipts and logs:
`/Users/thomashulihan/.codex/tmp/trr-tiktok-acceptance-01a16a88/`.
Includes deployment/readiness logs, run and call IDs, database snapshots, media
readbacks, native response diagnostics, and admin progress screenshot.

## Final catalog and UI evidence

WWHL job `3f1d683e-aa4d-48a6-9328-e5cb4f7e99e2` persisted 16 posts,
reported `catalog_incomplete` with `retryable=true`, retained a version-2 checkpoint
containing all 16 committed IDs, and was claimed for attempt 2. Its native receipt
reported one page, `native_exhausted=false`, one response failure, and unchanged
expected total 3,628. The runtime-supersession guard then cancelled the original
run and created replacement `8f5faa4e-78b4-4e10-ab47-6d9e8d15f2ea`.

The replacement reached 32 persisted posts in the live UI. This proves further
catalog persistence, not complete history. The user-requested cancellation test
used its visible Cancel Run button and native Chrome confirmation in the verified
TRR profile. The UI displayed “Cancelled run 8f5faa4e”; an independent database
read confirmed `status=cancelled`. The stale predecessor summary remains visible after cancellation and can still
show the original run as queued even though both runs are cancelled in the database.
This remaining summary/cache inconsistency is not resolved; no new backfill was
started afterward.

The live check found and repaired two app issues:

- Cancelled/failed runs were incorrectly described as scrape-complete.
  App commit `7a10893d` uses the actual terminal status.
- A runtime replacement lost its selection while loading, and stale predecessor
  progress could overwrite replacement progress. The repair retains provisional
  replacement state and rejects progress from its superseded predecessor.

Validation: three terminal-status cases and four replacement/selection cases
passed; the required quick validation passed 79 tests and generated-artifact
checks. Browser verification confirmed the active replacement remained selected,
showed 32/3,628 progress, and exposed its Cancel Run control. These app changes
were exercised on the local hybrid admin, not a deployed production frontend.

The target video repeat-enqueue probe requested auxiliary repair jobs even with
hosted video URLs present. Those three task-owned probe jobs were cancelled,
and their three rows were conditionally restored to the verified completed media
job IDs and `mirrored` state. Hosted arrays and MP4 objects were unchanged.
Do not claim the broader auxiliary-media enqueue decision is a no-op.

All requested full-history completeness gates remain open. Native five-page
proof is also open because the bounded posts-only runs used yt-dlp. No expected
totals were lowered, no comments were collected, and no WordPress/DNS writes or
provider purchases were performed.
