# AUDIT.v2.md

## Repo Fit

The plan fits the current TRR backend architecture. Current files confirm the plan should extend existing Instagram posts/comments benchmark helpers, queue status, backfill health, comment persistence, media guard, and Modal readiness tooling rather than adding a replacement scraper or third-party Instagram client.

## Current-Reality Findings

- `scripts/socials/instagram/benchmark_posts_backfill.py` is still a side-effect-free payload helper with `live_scrape_executed: False`, zero run metrics, and placeholder timing fields. Phase 1 correctly extends this seam before optimization.
- `scripts/socials/instagram/benchmark_comments_shards.py` already has fixture-default behavior, read-only live mode, explicit live confirmation, active-job preflight, retryable gap counts, terminal unavailable counts, media comment counts, and p95 timing.
- `trr_backend/socials/control_plane/backfill_health.py` aggregates run progress, auth cooldowns, worker/auth health, queue depth, and proxy bandwidth/cost signals.
- `tests/repositories/test_social_queue_status.py` proves queue status surfaces stale jobs, dispatch-blocked jobs, runs summary, silent-drop alerts, and running jobs.
- `trr_backend/socials/instagram/comments_scrapling/persistence.py` preserves author metadata, writes comment/reply fields, writes media URL state, and conditionally writes hosted/comment-media fields.
- `scripts/socials/media_queue_guard.py` blocks media-safe startup when stale media mirror/comment-media mirror jobs exist.
- `scripts/modal/verify_modal_readiness.py` supports JSON readiness output and deployed Instagram posts/comments auth probes.
- `CONTEXT.md`, the PRD, and ADR define the target domain language and the shared Adaptive Instagram Scrape Control Plane.

## Findings

| ID | Severity | Finding | Evidence | Required Fix |
| --- | --- | --- | --- | --- |
| BUG-001 | Medium | GitHub issue traceability is named as verified, but this rerun did not verify the live issue. | Supplied plan references `therealityreport/trr-backend#149`; no current GitHub proof was gathered. | Treat GitHub issue linkage as optional traceability unless execution verifies it live. |
| BUG-002 | Medium | Tool Finder returned irrelevant external package candidates that must not become implementation tools. | `TOOLS.v2.md` ranks PyPI/Homebrew candidates such as `instagram`, `scraper`, `complete`, `snapshot`, `pcl`, `abcl`, and `abpoa`. | Reject these candidates and keep repo-native implementation seams. |
| BUG-003 | Low | Previous run metadata used `idea_to_plan`, but the current user supplied a concrete plan path through the compatibility alias. | Plan Architect routing says compatibility alias plus supplied plan is `existing_plan_review`. | Correct `result.json` trigger metadata for this rerun. |

## Readiness Risks

- Live threshold values are still intentionally unproven. This is acceptable because the plan uses conservative defaults and benchmark-scoped ramping.
- The worktree remains dirty with many unrelated backend changes. The handoff must continue to preserve unrelated changes.
- SQL ownership is conditional. Any execution slice that adds schema must use additive nullable migrations and repo-local SQL readback.

## No Blockers

No planning blocker remains after the narrow v2 changes. The plan is ready for handoff at readiness `98`.
