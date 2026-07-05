# AUDIT.md

## Repo Fit

The PRD fits the existing TRR backend architecture. The current repo already has concrete Instagram posts/comments lanes, benchmark helpers, queue status, backfill health, media mirror guards, comment persistence, Modal readiness tooling, and operator progress scripts. The plan should extend those seams rather than introduce a replacement scraper or new third-party scraping provider.

## Current-Reality Findings

- Posts benchmark helper currently emits a bounded dry-run payload and placeholder metrics. It needs to become the account/date-window benchmark report seam.
- Comments benchmark helper already has fixture and guarded live modes, p95 timing, retryable gaps, terminal unavailable counts, and media comment totals.
- Backfill health already aggregates run progress, auth failures, queue depth, worker/auth health, auth cooldowns, and proxy bandwidth.
- Queue status tests already cover stale jobs, dispatch blocks, running jobs, recent failures, and media stale claims.
- Comments persistence already handles author profile image URL, author verification, reply topology, comment media mirror status, and comment media mirror enqueue counters.
- Media queue guard already blocks media-safe startup when stale media claims exist.
- Existing docs explicitly say Apify is reference-only and the posts lane should not use managed scraper actors as a shortcut.

## Blockers

No planning blockers remain. Implementation must still verify live schema and runtime state before writing migrations, changing worker defaults, or launching live Instagram runs.

## Readiness Risks

- The exact Supabase pressure thresholds are not proven and must start as conservative config defaults.
- Live benchmark date windows and mega-post samples must be chosen at execution time from current data to avoid stale examples.
- Runtime changes touch Modal-deployed scraper/job code and therefore require Modal follow-through.
- The worktree is dirty with many unrelated backend changes; implementation agents must preserve unrelated state.

## Test Seams

- Highest seam: account/date-window benchmark and gap report.
- Supporting seams: queue/backfill health, retry/completeness records, comments persistence, media completion, control-plane budget decision, lane enforcement, Modal readiness.
