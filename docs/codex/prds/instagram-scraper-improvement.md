# PRD: Instagram Scraper Complete Snapshot, Speed, and Safety Improvements

## Problem Statement

TRR needs Instagram scraping to capture a Complete Instagram Post Snapshot for Bravo accounts in an acceptable amount of time, comparable to managed third-party scrapers, without damaging Supabase health or increasing the risk that Instagram flags the scraping identities. Today the workflow has useful lanes for posts, comments, media, Modal execution, and progress reporting, but the system still needs a unified completeness contract, reliable gap tracking, adaptive speed control, benchmark evidence, and safe recovery for slow or incomplete posts.

## Solution

Build an Instagram scraper improvement program around a measurable account/date-window benchmark, explicit completeness states, lane-specific retry queues, and a shared Adaptive Scrape Control Plane.

The main user-visible outcome is that a Bravo account backfill can be launched, monitored, and completed with trustworthy status: every required post, media, comment, reply, comment media, avatar, and metadata component is either captured or marked unavailable with Instagram Source-Unavailable Evidence. The system should finish as fast as possible when Supabase, proxy, and Instagram-health signals are good, then slow or pause only the stressed lane when pressure rises.

The first benchmark target is `bravotv` over a recent bounded date window. A second benchmark must cover one high-comment account or post set to expose Instagram Mega-Post Shard behavior. Success is measured primarily at the account/date-window level, with p95 post detail/comment timing, Supabase pressure, Instagram flag risk, retries, and completeness gaps as guardrails.

## User Stories

1. As a TRR operator, I want an Instagram account/date-window backfill to produce a Complete Instagram Post Snapshot, so that the admin surface can trust the data it shows.
2. As a TRR operator, I want the scraper to capture stable public post fields, so that post lists and detail pages are complete.
3. As a TRR operator, I want the scraper to capture owner fields, so that post authors are queryable and displayable.
4. As a TRR operator, I want the scraper to capture captions, so that posts are searchable and reviewable.
5. As a TRR operator, I want the scraper to capture engagement counts, so that post performance can be compared.
6. As a TRR operator, I want the scraper to capture media variants and carousel children, so that post media can be rendered correctly.
7. As a TRR operator, I want the scraper to capture tagged users, so that people and accounts mentioned in post media are available.
8. As a TRR operator, I want the scraper to capture collaborators, so that coauthored posts are represented correctly.
9. As a TRR operator, I want the scraper to capture location, music, and ad flags where Instagram exposes them, so that enriched post context is not lost.
10. As a TRR operator, I want hosted media mirrors to be part of completion, so that TRR is not dependent on expiring Instagram source URLs.
11. As a TRR operator, I want full comments and replies to be captured, so that comment analysis is not based on partial embedded samples.
12. As a TRR operator, I want comment author metadata captured, so that comments show username, avatar, verification status, likes, and reply counts.
13. As a TRR operator, I want comment media such as GIFs, stickers, or attached media captured where available, so that rich comments are not flattened into text-only records.
14. As a TRR operator, I want account-specific viewer state excluded, so that scraped data does not depend on which Instagram account was used to scrape.
15. As a TRR operator, I want `has liked`, `has saved`, mutual-follow, and viewer permission fields ignored for product data, so that TRR stores account-independent facts.
16. As a TRR operator, I want the scraper to save valid data immediately, so that a later failure does not throw away completed work.
17. As a TRR operator, I want missing snapshot parts recorded explicitly, so that post detail, media, comments, replies, comment media, and avatar gaps are visible.
18. As a TRR operator, I want retry jobs targeted to missing pieces, so that the system does not redo completed work.
19. As a TRR operator, I want retry jobs to use saved cursors and checkpoints, so that large comment threads resume instead of restarting.
20. As a TRR operator, I want an account/date-window marked complete only when required parts are captured or source-unavailable, so that completion status is trustworthy.
21. As a TRR operator, I want source-unavailable states to require stable Instagram evidence, so that temporary failures are not recorded as permanent facts.
22. As a TRR operator, I want timeouts, 429s, proxy failures, and auth failures to remain retryable, so that infrastructure issues do not become false data truth.
23. As a TRR operator, I want the first benchmark to use `bravotv`, so that the test exercises the real Bravo shared-account workflow.
24. As a TRR operator, I want a high-comment benchmark sample, so that mega-post behavior is measured separately from ordinary posts.
25. As a TRR operator, I want benchmark reports to show account-level runtime, so that the real workflow speed is visible.
26. As a TRR operator, I want benchmark reports to show p95 per-post detail and comment timing, so that slow posts cannot hide inside averages.
27. As a TRR operator, I want benchmark reports to show Supabase pressure, so that speed gains do not harm database health.
28. As a TRR operator, I want benchmark reports to show Instagram and proxy risk, so that speed gains do not increase flag or checkpoint risk.
29. As a TRR operator, I want a shared Adaptive Scrape Control Plane, so that pressure decisions are consistent across scraper lanes.
30. As a TRR operator, I want each lane to enforce budgets in its own way, so that comments, posts, media mirror, and database writes can respond appropriately.
31. As a TRR operator, I want Instagram Lane Budgets to include normal, reduced, paused, and identity-blocked states, so that backoff behavior is simple to inspect.
32. As a TRR operator, I want blocked identities to take precedence over other budgets, so that challenged or unsafe Instagram sessions stop immediately.
33. As a TRR operator, I want proxy cooldowns to take precedence over account-level speed, so that repeated proxy failures do not keep firing.
34. As a TRR operator, I want account-lane pauses to affect only the stressed lane, so that unrelated healthy work can continue.
35. As a TRR operator, I want Supabase pressure to reduce write batch size and write concurrency, so that the database is protected without stopping all fetching.
36. As a TRR operator, I want Instagram flag risk to slow posts and comments requests, so that the scraper does not chase speed into checkpoints.
37. As a TRR operator, I want comment retry spikes to slow the comments lane, so that post discovery or safe media work can continue.
38. As a TRR operator, I want media mirror pressure to reduce download and upload concurrency, so that hosted completion remains safe.
39. As a TRR operator, I want benchmark runs to adjust budgets only inside bounded runs, so that experiments do not silently change production defaults.
40. As a TRR operator, I want permanent default changes to require completeness, speed, Supabase, and Instagram-health evidence, so that tuning is evidence-backed.
41. As a TRR operator, I want high-comment posts split into Instagram Mega-Post Shards, so that one large post does not stall an account backfill.
42. As a TRR operator, I want mega-post shards to carry their own cursors and retry state, so that recovery is precise.
43. As a TRR operator, I want the UI and progress tools to distinguish partial success from completion, so that operators do not mistake gaps for done work.
44. As a TRR operator, I want Modal runs to expose the same completion and budget evidence as local runs, so that production validation is comparable.
45. As a TRR developer, I want one shared control-plane model, so that future platform lanes do not copy divergent pressure logic.
46. As a TRR developer, I want tests at the account/date-window seam, so that behavior is verified from the operator workflow rather than internal helper details.
47. As a TRR developer, I want lane-level tests only where lane enforcement differs, so that tests stay focused on externally visible behavior.
48. As a TRR developer, I want schema changes to be additive and backfillable, so that existing scraper data is not broken.
49. As a TRR developer, I want rollout to go through Modal when scraper runtime code changes, so that local success is not mistaken for deployed success.
50. As a TRR developer, I want the benchmark and gap report to be the first implementation slice, so that later optimization starts from measured truth.

## Implementation Decisions

- Use the existing TRR-native Instagram source families as the source of truth. Apify-style field names are reference aliases only.
- Define the target output as a Complete Instagram Post Snapshot, not a raw Instagram dump.
- Exclude logged-in viewer state from product persistence, including `has liked`, `has saved`, mutual-follow data, and viewer-specific permission fields.
- Treat aggregate public metrics such as likes, comments, plays, views, and stable save/share counts as eligible only when the source can be verified as account-independent.
- Make account/date-window runtime the primary performance target.
- Track p95 per-post detail and comment timing as a secondary guardrail.
- Use `bravotv` over a recent bounded date window as the first account-level benchmark.
- Add one high-comment account or post set as the second benchmark target.
- Build or extend benchmark reporting to include account runtime, p95 post timing, completeness gaps, retry volume, Supabase pressure, Instagram flag risk, proxy health, and current lane budgets.
- Use partial success with explicit retry queues. Valid data is saved immediately, and missing pieces become targeted retry work.
- Track missing snapshot parts separately for post detail, hosted media, comments, replies, comment media, author avatars, and other required enrichment.
- Require Instagram Source-Unavailable Evidence before marking a required part permanently unavailable.
- Keep timeouts, 429s, proxy failures, retry exhaustion, and auth failures retryable.
- Treat hosted media and avatar mirrors as part of Hosted Snapshot Completion. Source URLs alone are partial.
- Queue comment media mirroring after text and reply capture so comment content completes before richer media laggards.
- Create Instagram Mega-Post Shards automatically when expected comment volume or runtime crosses a threshold.
- Preserve cursors and checkpoints for mega-post retries.
- Introduce a shared Adaptive Scrape Control Plane for pressure decisions.
- Let individual lanes enforce budgets in lane-specific ways.
- Publish Instagram Lane Budgets as normal, reduced, paused, and identity-blocked.
- Enforce budget precedence in this order: blocked identity, proxy cooldown, account-lane pause, global lane budget, default.
- Include Supabase signals in pressure decisions: write latency, failed upserts, connection pressure, active jobs, queue age, stale running jobs, and advisory-lock contention.
- Include Instagram and proxy risk signals in pressure decisions: 429s, checkpoint/challenge/login redirects, repeated cursor failures, sudden empty responses, proxy blocks, auth probe failures, and rising retry rate per identity or proxy.
- Persist durable budget state and evidence, while allowing in-memory caches to avoid excessive reads before every request.
- Allow benchmark runs to change budgets only for the bounded run.
- Require benchmark evidence before changing permanent defaults.
- Keep Modal as the production follow-through path for scraper runtime changes.

## Testing Decisions

- The highest-value test seam is the account/date-window backfill benchmark and completion contract. Tests should verify the externally visible output: runtime report shape, completeness state, retry targets, source-unavailable reasons, and budget decisions.
- Existing posts and comments lane tests should be extended only where lane behavior changes under budgets.
- Existing benchmark helper tests should be extended to require Supabase pressure, Instagram/proxy risk, retry volume, p95 timing, completeness gaps, and lane-budget fields.
- Existing control-plane and queue-status tests should cover budget precedence, durable budget persistence, in-memory budget cache behavior, and stale-pressure handling.
- Existing comments tests should cover retryable versus source-unavailable gaps, saved cursors, reply checkpoint behavior, comment media gaps, and mega-post sharding.
- Existing media mirror tests should cover Hosted Snapshot Completion, source URL partial states, avatar mirror completion, comment media mirror completion, and unavailable media evidence.
- Existing Modal readiness and deployed smoke tests should cover the presence of scraper control-plane signals and the ability to validate Instagram auth/proxy readiness without launching unsafe unbounded runs.
- Tests should avoid asserting private helper calls. They should assert behavior at queue, benchmark, completion, retry, persistence, and API/reporting boundaries.
- Fixture-backed tests should be used for deterministic cases such as 429s, checkpoint signals, disabled comments, deleted media, exhausted pagination, proxy blocks, and write failures.
- Live tests should remain bounded, explicit, and operator-triggered. They should never become default unit or CI behavior.

## Out of Scope

- Replacing TRR's Instagram scraper with Apify or another managed scraper.
- Persisting logged-in viewer state as product data.
- Treating retry exhaustion as proof that Instagram data is permanently unavailable.
- Unbounded full-history scraping without a date window, account target, or operator-approved run contract.
- Making permanent speed/default changes without benchmark evidence.
- UI redesign beyond showing trustworthy status fields required by the scraper contract.
- Changing non-Instagram platform behavior except where shared control-plane interfaces must remain compatible.
- Running production Modal deployments as part of this PRD document itself.

## Further Notes

- The PRD follows the TRR backend glossary and the accepted Adaptive Instagram Scrape Control Plane ADR.
- The first implementation issue should create the baseline `bravotv` benchmark and gap report before changing scraper behavior.
- The second implementation issue should define and persist retry/completeness records if existing structures cannot represent all required snapshot parts.
- The control-plane work should be wired incrementally so each lane can adopt budgets without a single large risky rewrite.
- Any backend, worker, scraper, job, runtime, or Modal secret-preparation code changes must include Modal follow-through unless explicitly scoped local-only.
