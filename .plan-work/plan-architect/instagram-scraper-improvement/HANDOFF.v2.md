# HANDOFF.v2.md

## Target Skill

Use `orchestrate-subagents`.

## Execution Mode

Serialized multi-scope execution. The scopes are separable, but several share queue, completion, and reporting contracts, so keep contract-defining scopes ordered.

## Source Plan

Use `REVISED_PLAN.v2.md` as the execution source of truth.

## Ownership Scopes

1. Benchmark Scope
   - Extend posts benchmark into account/date-window report output.
   - Combine comments benchmark, backfill progress, queue status, and backfill health.
   - Keep fixture default and explicit live read-only guardrails.

2. Completion Scope
   - Define snapshot parts and completion states.
   - Add additive nullable persistence only if existing metadata cannot represent the contract.
   - Preserve valid data and create retry targets only for missing parts.

3. Control-Plane Scope
   - Add budget decision module using backfill health, queue status, cooldowns, recent failures, write failures, and benchmark-scoped overrides.
   - Persist budget state/evidence and use short in-memory TTL caching.

4. Lane Enforcement Scope
   - Wire comments, posts, media mirror, and DB writes to consume budgets.
   - Keep lane-specific enforcement out of the shared pressure policy.

5. Mega-Post Scope
   - Detect large/high-runtime posts.
   - Split into one-post jobs with saved cursors/checkpoints.
   - Ensure retries resume instead of restarting ordinary work.

6. Media Completion Scope
   - Treat source URLs as partial.
   - Complete or mark hosted media, avatar, and comment media as source-unavailable with evidence.
   - Keep media retry targets separate from comment text/reply targets.

7. Operations Scope
   - Expose completion, retry target, source-unavailable, and budget state through progress/backfill-health/API surfaces.
   - Update runbooks.
   - Run targeted validation.
   - Deploy/update Modal and run readiness/auth probes for scraper/job/runtime/secret-prep changes.

## Required Coordination

- Backend-first. Only touch TRR-APP if backend API/reporting changes require UI follow-through.
- Preserve unrelated dirty-tree changes.
- Do not adopt generic third-party packages from Tool Finder.
- If SQL changes are needed, use additive nullable schema changes and repo-local DB helper readback.
- If GitHub issue tracking is used, verify the live issue before citing it as coordination proof.

## Completion Checklist

- Backend/API validation status reported.
- SQL ledger/readback status reported when SQL ownership changes.
- TRR-APP build status reported only if app validation/build is relevant.
- Modal follow-through status reported for scraper/job/runtime/Modal secret-prep changes.
- Browser target reported only if browser verification is used.
- Temporary benchmark outputs cleaned only when they are not evidence.
