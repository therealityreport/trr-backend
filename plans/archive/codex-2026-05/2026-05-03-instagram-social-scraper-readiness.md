# Instagram Social Scraper Readiness Plan

## Summary

No additional backend refactor code is required for Instagram to work based on the current repository evidence: the Instagram-first scraper separation, comments auth repair guard, persistence move, handler registry, router package split, ops thinning, and targeted validations are already complete.

One final readiness pass should be added before treating Instagram as operationally proven: run live smoke checks against the local/admin flow and record whether the current Instagram session can access the comments endpoint. This is validation and small corrective follow-up only, not another architecture phase.

## Project Context

Current evidence:
- `docs/ai/local-status/social-scraper-separation-final-20260502.md` says all planned phases are completed.
- `tests/repositories/test_social_season_analytics.py` passed with `745 passed`.
- Social API, control-plane, comments/auth, platform lane, script, compile, Ruff, and diff checks are recorded as passing.
- Remaining documented next action is a future compatibility-wrapper deletion pass, which is cleanup and not required for Instagram runtime correctness.
- Older handoff notes still mention Instagram session repair, so live auth state must be rechecked rather than assumed fixed.

## Assumptions

- Goal is “Instagram scraper works from the admin/operator path,” not “remove every legacy compatibility wrapper.”
- Existing dirty social-scraper changes are still assigned to this refactor.
- No schema or API response change should be added unless a live smoke check proves a concrete break.
- Compatibility wrapper deletion is deferred because it is not necessary for Instagram to work.

## Implementation Changes

- Add a final Instagram smoke/readiness checklist artifact under `docs/ai/local-status/` after validation.
- Run the existing backend test gates already used for the completed refactor if the worktree changes again.
- Run live/local checks for:
  - Instagram catalog backfill launch from the admin route.
  - Instagram `Sync Comments` / `Incomplete Fill` launch path.
  - Comments endpoint probe behavior and returned `auth_repair_*` metadata.
  - Deferred comments follow-up after catalog completion.
  - Worker health and queue status for Instagram lanes.
- If the comments endpoint probe is auth-blocked, do not add generic scraper changes. Use the existing Repair Instagram Auth flow and record the result.
- Only patch code if the live check finds one of these concrete failures:
  - launch queues comments jobs after a failed probe
  - repair metadata is missing from the route response
  - worker health blocks a valid lane
  - deferred comments bypass the guard
  - the UI cannot display repair progress/failure from the existing response fields

## Validation

Run, at minimum:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
pytest -q tests/repositories/test_social_season_analytics.py
pytest -q tests/api/routers/test_socials_route_shape.py tests/api/routers/test_socials_season_analytics.py
pytest -q tests/socials/test_instagram_comments_scrapling_retry.py tests/socials/test_cookie_refresh_flows.py tests/socials/test_instagram_auth_resolver.py
ruff check api/routers/socials trr_backend/socials/instagram trr_backend/socials/pipelines/comments trr_backend/socials/control_plane
python -m compileall -q api/routers/socials trr_backend/socials scripts/socials
```

Expected result: all pass, with any live DB-only skip documented.

For live/admin validation, record:
- requested action
- run id or failure code
- `auth_repair_attempted`
- `auth_repair_status`
- `auth_repair_reason`
- comments endpoint probe result
- whether any job was queued after failed repair

## Acceptance Criteria

- Instagram catalog and comments launch paths either queue valid work or fail before queueing with a clear auth repair reason.
- Comments endpoint auth repair remains scoped to explicit comments/backfill actions only.
- No page-load, polling, cookie-health, or ordinary post-list read triggers auth repair.
- Deferred comments follow-up uses the same guard as direct comments launches.
- Worker health and queue status show a usable Instagram lane or a clear blocker.
- A final local-status note states whether Instagram is operationally ready or blocked only by live auth/session repair.

## Risks / Open Questions

- Live Instagram comments access may still be blocked even when profile GraphQL health passes; this should be treated as session/proxy/auth state, not a reason to redesign the scraper.
- The compatibility-wrapper deletion pass is useful cleanup but should not be bundled into Instagram readiness.
- Current dirty worktree is broad; avoid unrelated cleanup during smoke validation.

## Recommended Handoff

Use `orchestrate-plan-execution` for the readiness pass because the work is sequential: verify backend state, run live/admin checks, then make only targeted fixes if a check fails.

## Ready For Execution

Yes. The plan is ready for execution as a validation/final-readiness pass. It should not start another refactor unless a live check exposes a specific failing contract.
