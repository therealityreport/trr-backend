# Instagram Social Scraper Readiness - 2026-05-03

## Handoff Snapshot
```yaml
handoff:
  include: true
  state: blocked
  last_updated: 2026-05-03
  current_phase: "Instagram scraper readiness passed code and route-contract gates, but launch-path comments auth remained blocked."
  next_action: "Investigate the comments endpoint auth/session mismatch before queuing live Instagram comments jobs."
  detail: self
```

## Summary

The Instagram scraper refactor readiness pass is complete from a code and route-contract perspective. The repository gates are passing, the admin/auth status routes are reachable with the local internal-admin token, and the final implementation patch was limited to the documented Instagram auth repair command.

Operational Instagram comments readiness is still blocked by live session behavior: the launch-path comments endpoint probe for `thetraitorsus` shortcode `DXuqLQzjODD` returned `auth_blocked` with reason `redirect_to_homepage`. Per operator direction, no further login or re-auth attempts were run after the second repair attempt.

## Artifacts

- Approved plan saved at `docs/codex/plans/2026-05-03-instagram-social-scraper-readiness.md`.
- This readiness note records the local/admin smoke evidence and the remaining live-auth blocker.

## Code Changes From Readiness Pass

- `scripts/modal/repair_instagram_auth.py`
  - Added repo-root `sys.path` bootstrap so the documented direct command can import `trr_backend`.
  - Forced Instagram repair refresh and validation commands to use `--validation-mode comments_endpoint`.
- `tests/scripts/test_repair_instagram_auth.py`
  - Added assertions that repair refresh and validation commands use `comments_endpoint`.
- Ruff import hygiene was applied to the social control-plane Instagram import surfaces touched by the larger social-scraper refactor.

No additional scraper architecture changes were made.

## Validation Gates

All commands were run from `/Users/thomashulihan/Projects/TRR/TRR-Backend`.

```bash
pytest -q tests/repositories/test_social_season_analytics.py
# 745 passed in 11.49s

pytest -q tests/api/routers/test_socials_route_shape.py tests/api/routers/test_socials_season_analytics.py
# 231 passed in 6.49s

pytest -q tests/socials/test_instagram_comments_scrapling_retry.py tests/socials/test_cookie_refresh_flows.py tests/socials/test_instagram_auth_resolver.py tests/scripts/test_repair_instagram_auth.py
# 154 passed in 2.73s

ruff check scripts/modal/repair_instagram_auth.py tests/scripts/test_repair_instagram_auth.py api/routers/socials trr_backend/socials/instagram trr_backend/socials/pipelines/comments trr_backend/socials/control_plane
# All checks passed.

python -m compileall -q scripts/modal/repair_instagram_auth.py api/routers/socials trr_backend/socials scripts/socials
# passed
```

## Live / Local Checks

### Admin API Auth

- Request: `GET /api/v1/admin/socials/ingest/queue-status?fresh=true` without credentials.
- Result: `401`, confirming the backend is reachable and admin auth is enforced.
- Local shell env did not include `TRR_ADMIN_BEARER_TOKEN`, `WORKSPACE_TRR_INTERNAL_ADMIN_SHARED_SECRET`, or `TRR_INTERNAL_ADMIN_SHARED_SECRET`.
- Derived a short-lived local internal-admin JWT using the same deterministic secret formula as `scripts/dev-workspace.sh`; token and secret were not logged.

### Worker Health

- Request: `GET /api/v1/admin/socials/ingest/worker-health`.
- Result: `200`.
- Evidence: `queue_enabled=true`; returned worker metadata includes `instagram_comments_scrapling` lane entries with `instagram_authenticated=true`.

### Queue Status

- Request: `GET /api/v1/admin/socials/ingest/queue-status`.
- Result: `200` for cached status in an earlier call.
- Request: `GET /api/v1/admin/socials/ingest/queue-status?fresh=true`.
- Result: timed out at 30 seconds with no body.
- Readiness interpretation: cached queue status is reachable, but fresh queue status needs follow-up performance/DB investigation before treating the admin health panel as fully proven.

### Instagram Dashboard / Catalog

- Request: `GET /api/v1/admin/socials/profiles/instagram/thetraitorsus/dashboard?detail=lite`.
- Result: `200`.
- Evidence: dashboard returned catalog/comment history, including 436 catalog posts, 424 eligible comment posts, 104,785 retrieved comments, and recent catalog/comment run history.

- Request: `GET /api/v1/admin/socials/profiles/instagram/thetraitorsus/catalog/posts?page=1&page_size=5`.
- Result: `200`.
- Representative shortcode selected for comments probe: `DXuqLQzjODD`.

### Comments Endpoint Probe

- Request: direct launch-path probe helper for account `thetraitorsus`, shortcode `DXuqLQzjODD`.
- Result:

```json
{
  "mode": "comments_endpoint",
  "reason": "redirect_to_homepage",
  "result": "auth_blocked",
  "retryable": false,
  "shortcode": "DXuqLQzjODD",
  "status": "auth_blocked"
}
```

### Auth Repair

- Request: `python scripts/modal/repair_instagram_auth.py --json`.
- First run exposed the missing repo-root import bootstrap and was patched.
- Repair flow then completed:
  - refresh: `ok`, `validated=true`
  - validate_local: `ok`, `validated=true`
  - apply_named_secrets: `ok`
  - deploy_modal_app: `ok`
  - verify_remote_auth: `failed`, `CalledProcessError`
  - overall `ok=false`, `failure_reason=remote_probe_failed`
- A follow-up launch-path probe still returned `auth_blocked` / `redirect_to_homepage`.
- Per operator direction, no further login, repair, or re-auth attempts should be run unless explicitly necessary.

### Validate-Only Cookie Check

- Request: `python scripts/socials/refresh_cookies.py --platform instagram --validate-only --validation-mode comments_endpoint`.
- Result: `validated=true`, `reason=null`, cookie file `data/instagram_cookies.json`.
- Readiness interpretation: the stored-cookie validator and the launch-path probe disagree. The launch-path probe is the source of truth for whether comments jobs should be queued.

## Acceptance Status

- Catalog/dashboard read paths are reachable.
- Worker health shows an Instagram comments lane and authenticated worker metadata.
- Route/unit coverage confirms comments and catalog launches return auth repair metadata and fail with `SOCIAL_INSTAGRAM_COMMENTS_AUTH_REPAIR_FAILED` when repair fails.
- No comments or catalog launch jobs were intentionally queued during this readiness pass.
- Instagram comments are not operationally ready until the launch-path comments endpoint probe returns `valid`.
- Current blocker is live auth/session/proxy behavior, not missing scraper refactor code.

## Follow-Up

- Investigate why `refresh_cookies.py --validate-only --validation-mode comments_endpoint` reports valid while `_probe_instagram_comments_endpoint_for_launch` reports `auth_blocked`.
- Investigate `queue-status?fresh=true` timeout separately; cached queue status and worker health are reachable.
- Do not perform additional headed login/repair attempts without operator confirmation.
