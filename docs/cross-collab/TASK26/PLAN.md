# Shared-Profile Social Rollout Guardrails — Task 26 Plan

Repo: TRR-Backend
Last updated: 2026-04-04

## Goal
Finish the shared-profile social rollout by proving the Modal worker plane is live, then executing the authenticated shared-profile canary sequence before replaying the affected backfill.

## Status Snapshot
Backend implementation is complete in code and targeted tests. Modal readiness was re-verified on 2026-04-04, and the initial bounded `Sync Recent` canary completed on Modal. The remaining work is no longer generic rollout monitoring: the live `bravotv` resume-tail and `bravodailydish` replay exposed an Instagram shared-account frontier/auth failure (`instagram_graphql_cursor_request_failed`, `auth_reason=checkpoint_required`) plus mixed Modal runtime-image evidence that must be resolved before rerunning the canary/replay sequence.

## Remaining Work Scope

### Phase 3: Diagnose And Fix The Live Instagram Frontier/Auth Failure
Use the live run evidence from Task 26 to fix the shared-account Instagram frontier path before attempting another replay.

Artifacts to update:
- `docs/cross-collab/TASK26/STATUS.md` — record command output, run ids, and operator result
- `docs/cross-collab/TASK26/VERIFICATION.md` — acceptance evidence and any live rollout blockers
- targeted code and test evidence for the frontier/auth fix

Primary backend touchpoints:
- `trr_backend/repositories/social_season_analytics.py`
  - `_run_shared_account_frontier_posts_stage(...)`
  - `_fetch_shared_instagram_graphql_posts_page(...)`
  - `_validate_instagram_cookie_health(...)`
  - `_build_catalog_run_progress_alerts(...)`
  - `_resolve_run_progress_runtime_versions(...)`
  - `resume_tail_social_account_catalog(...)`
- `tests/repositories/test_social_season_analytics.py`
  - Instagram cookie-health and checkpoint cases
  - shared-account frontier progress / catalog-progress alert coverage
  - resume-tail and shared-account backfill orchestration coverage

Execution steps:
1. Treat the current live data-plane evidence as the first source of truth:
   - `bravotv` resume-tail `dc67440d-6daa-432a-a6ae-c972bbb7bee7`
   - `bravodailydish` replay `360380f8-34ef-428b-96d7-0a507e480565`
   - both frontiers now show `instagram_graphql_cursor_request_failed`
   - both frontiers now show `auth_allowed=false` and `auth_reason=checkpoint_required`
2. Audit the shared-account Instagram frontier code path for:
   - checkpoint/challenge handling after the initial bootstrap succeeds
   - retry behavior when GraphQL cursor requests fail mid-frontier
   - auth-preflight truthfulness versus actual runtime session viability
   - any runtime-image drift or incompatible worker image split between bootstrap and retry stages
3. Make the fix in the frontier/auth path:
   - preserve the concrete auth failure reason on frontier rows and job metadata
   - ensure checkpoint/challenge signals fail closed instead of being treated as a generic retry-only cursor error
   - verify resume-tail/frontier retries do not leave the run in a misleading aggregate state
   - if runtime-version drift is real, make the progress surface expose it deterministically from job metadata
4. Add or update targeted regression tests for:
   - checkpoint-required frontier failure propagation
   - catalog-progress alerting when frontier auth fails
   - runtime-version drift detection from mixed job metadata
   - aggregate run-state derivation when run rows lag active frontier/job state
5. Re-run the smallest backend validation slice that proves the new fix:
   - `ruff check api/routers/socials.py trr_backend/repositories/social_season_analytics.py tests/api/routers/test_socials_season_analytics.py tests/repositories/test_social_season_analytics.py`
   - `ruff format --check api/routers/socials.py trr_backend/repositories/social_season_analytics.py tests/api/routers/test_socials_season_analytics.py tests/repositories/test_social_season_analytics.py`
   - targeted `pytest -q tests/repositories/test_social_season_analytics.py -k 'checkpoint_required or runtime_version_drift or classify_backlog_after_scrape or frontier_lease_stale or dispatch_blocked or resume_frontier_cursor'`
   - targeted router slice if any response shape changes

### Phase 4: Rerun The Authenticated Canary And Replay From An Allowlisted Environment
Once the frontier/auth fix is in place, rerun the live rollout sequence from an allowlisted admin environment and capture direct control-plane evidence.

Artifacts to update:
- `docs/cross-collab/TASK26/STATUS.md`
- `docs/cross-collab/TASK26/VERIFICATION.md`
- operator evidence artifacts captured from the admin control plane, if generated

Execution steps:
1. Re-run `scripts/modal/verify_modal_readiness.py --json` from the backend virtualenv and capture the current Modal API web URL.
2. Verify `GET /api/v1/admin/socials/ingest/worker-health` reports:
   - `dispatcher_readiness.resolved = true`
   - `dispatcher_heartbeat_fresh = true`
   - `remote_auth_capabilities.instagram.ready = true`
   - `shared_account_backfill_readiness.ready = true`
   - no blocking `alerts`
3. From the admin control plane, run the bounded shared-profile Instagram canary in this order:
   - `Sync Recent`
   - `Resume Tail` if a resumable frontier exists
4. Inspect the resulting catalog-progress payload for:
   - no `runtime_version_drift`
   - no `frontier_lease_stale`
   - no `dispatch_blocked`
   - no `classify_backlog_after_scrape` that fails to drain
5. Only after the bounded canary is green, replay the affected shared-profile `Backfill Posts` run.
6. Record the final run ids, readiness evidence, and any follow-up remediation in `STATUS.md`.

Allowlisted rerun checklist:
- run from an environment that is on the admin allowlist for `https://admin-56995--trr-backend-api.modal.run`
- capture:
  - worker-health JSON
  - canary run id for `Sync Recent`
  - canary run id for `Resume Tail` if present
  - replay run id for `Backfill Posts`
  - final `catalog/progress` payloads for each run
- if any run surfaces `runtime_version_drift`, `frontier_lease_stale`, `dispatch_blocked`, `retry_loop_detected`, or `classify_backlog_after_scrape`, stop closure and record the alert payload verbatim in `VERIFICATION.md`

Runtime-drift confirmation rules:
- treat mixed image labels in storage (`im-P9...` vs `im-E7...`) as a suspected drift signal, not proof
- confirm real drift only if the allowlisted `catalog/progress` or direct job metadata after the fix still shows more than one runtime label for the same replay family
- if drift disappears after redeploy/rerun, record it as rollout residue rather than an open runtime bug

### Phase 5: Closeout Or Escalation
Close Task 26 only if the live canary and replay both complete without rollout-blocking alerts. Otherwise, preserve the failure mode and stop with a concrete blocker.

Artifacts to update:
- `docs/cross-collab/TASK26/STATUS.md`
- `docs/cross-collab/TASK26/VERIFICATION.md`

Closeout rules:
1. If the canary and replay are both green, mark the frontier/auth fix complete and set the task to ready for closure.
2. If readiness is green but authenticated canary access is blocked, leave the task active with `current_phase: "authenticated canary pending"`.
3. If readiness or alert review fails, do not declare rollout complete; capture the blocking alert or auth state and treat it as the next fix target.
4. Before closure, make sure `STATUS.md` and `VERIFICATION.md` include:
   - exact run ids
   - exact worker-health readiness evidence
   - exact progress alert outcomes
   - whether runtime drift was confirmed or ruled out

## Out of Scope
- screenalytics changes unless a downstream backend contract consumer breaks
- Dedicated Bravo editorial/media systems unrelated to shared-profile ingest
- Vercel app deployment mechanics

## Locked Contracts
### Social Admin Routes
Keep `/api/v1/admin/socials/profiles/{platform}/{account_handle}/...` stable and additive.

### Shared Profile Identity
`account_handle` remains the lookup key; `network_name` is operator-facing metadata only.

## Acceptance Criteria
1. Worker-health and catalog-progress surfaces emit structured alert codes for rollout decisions.
2. Shared-profile payloads expose `network_name` without changing route shapes.
3. Operator docs and templates no longer default to `bravotv` as the generic shared-profile example.
4. Targeted backend verification passes, live Modal readiness is confirmed, and rollout state is recorded in Task 26 docs.
