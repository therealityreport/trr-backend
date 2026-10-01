# Plan Grader Audit: Threads, Twitter/X, and Facebook Architecture Parity

Source plan: `docs/codex/plans/2026-05-05-threads-twitter-facebook-architecture-parity.md`
Audit date: 2026-05-05
Scope: `/Users/thomashulihan/Projects/TRR/TRR-Backend`

## Verdict

APPROVE_AFTER_REVISION

The source plan is current-state aware, file-specific, and valuable. It correctly separates Threads claimed-job hardening from Twitter/Facebook shared catalog extraction, and it names the operator-facing remote auth bug now affecting all three platforms.

The revised plan should be the execution source because it tightens three things that matter in this dirty repo:

- exact Phase 0 test node IDs and expected pre-fix failures,
- strict no-new-lane boundaries for Twitter/X and Facebook,
- disjoint subagent ownership around `trr_backend/socials/social_season_analytics_impl.py`.

Recommended next execution skill: `orchestrate-subagents`.

## Gate Checks

- 30-second triage: PASS. The plan has a clear goal, concrete files, phased execution, and verification commands.
- Hard-fail conditions: PASS. It does not propose schema changes, route contract changes, destructive rewrites, or unsupported production actions.
- Wrong-thing guardrail: PASS. The target is the right outcome: platform-local Modules plus operator readiness parity, not broad scraper reinvention.
- Automatic downgrade caps: none applied. The plan is execution-ready after minor revision.

## Current-State Fit

Strong.

The plan matches current repo evidence:

- `docs/architecture/social-platform-module-checklist.md` is the right architecture contract for platform-local Modules.
- `trr_backend/socials/threads/posts_scrapling/job_runner.py` already has a real lane but lacks cancellation and degraded final-read/finalization handling.
- `trr_backend/socials/social_season_analytics_impl.py` lists Twitter/X, Facebook, and Threads in `REMOTE_AUTH_REQUIRED_PLATFORMS`, but `probe_remote_auth_health()` is still only complete for Instagram and TikTok.
- Existing Twitter/X, Facebook, and Threads cookie loaders and validators make remote-auth readiness a localized fix.
- `_scrape_shared_twitter_posts()`, `_scrape_shared_facebook_posts()`, and `_scrape_shared_threads_posts()` are the correct compatibility-wrapper surfaces for posts catalog extraction.

## Main Findings

1. Phase 0 needs exact test node IDs before implementation begins.
   The source plan says to add tests first, but leaves several names broad. In this repo, broad social analytics tests are expensive and easy to overfit. The revised plan gives exact node names for cancellation, degraded DB handling, remote auth structure flags, CLI parsing, and wrapper delegation.

2. Twitter/X and Facebook must remain catalog extraction work, not new job-lane work.
   The source plan states this as an assumption. The revised plan repeats it as a stop rule and subagent ownership boundary so a worker does not add new `jobs.py`, stages, routes, or worker-lane behavior.

3. Remote auth should extend the existing cookie registry shape instead of adding more per-platform special cases.
   The source plan names the right loader/validator functions. The revised plan makes safe structure flags explicit and requires no raw token/cookie values in probe payloads, metadata fixtures, or docs.

4. Threads has two different seams that should not be merged.
   `threads/posts_scrapling` is the claimed-job lifecycle lane; `threads/posts_catalog` should be shared-account catalog orchestration. The revised plan assigns these to different workers and keeps wrapper integration in the main session.

5. Subagent orchestration needs an integration owner.
   Independent workstreams are useful here, but the compatibility module and readiness CLI are shared edit surfaces. The revised plan gives workers disjoint write scopes and keeps wrapper wiring, import-cycle checks, docs, and final validation in the main session.

## Benefit Score

High.

This work reduces future bug-fix cost in a part of the repo that already has repeated platform parity fixes. The benefit is not just code cleanliness: it makes operator readiness truthful for three platforms, hardens a real Threads worker lane, and narrows future Twitter/Facebook catalog bugs to platform-local Modules.

## Residual Risks

- The worktree is dirty and overlaps target files. Executors must inspect current diffs before editing and must not revert unrelated changes.
- `tests/repositories/test_social_season_analytics.py` is large. Use targeted node IDs wherever possible and broaden only after the focused tests pass.
- Twitter/X retrieval paths are not interchangeable. The catalog Module should not invent a generic adapter abstraction until tests prove it.
- Facebook extraction can become a shallow forwarder if the new Module just re-exports the monolith. Apply the deletion test: callers should know less after extraction.
- Batch upsert remains unsafe unless optional-column, conflict-target, assignment, `job_id`, and return-shape equivalence are proven.

## Decision

Use `REVISED_PLAN.md` as the execution source. The source plan is good enough to preserve, but the revised plan is safer for parallel implementation and better aligned with the current repo gates.
