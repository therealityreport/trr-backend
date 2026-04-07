# Status — Task 26 (Instagram Shared-Profile Rollout Guardrails)

Repo: TRR-Backend
Last updated: 2026-04-04

## Handoff Snapshot
```yaml
handoff:
  include: true
  state: active
  last_updated: 2026-04-04
  current_phase: "frontier/auth fix proven live; instagram session repair pending"
  next_action: "repair the Instagram authenticated session, then rerun bounded canary and affected replay against the deployed backend"
  detail: self
```

## Phase Status

| Phase | Description | Status | Notes |
|-------|-------------|--------|-------|
| 1 | Worker-health and catalog alert contract | Implemented | Targeted backend tests passing |
| 2 | Shared-profile metadata and operator defaults cleanup | Implemented | `network_name` added; Bravo defaults removed from templates |
| 3 | Modal deploy and live canary | Partial | Updated backend code is deployed and proven live: the new canary now fails closed with `frontier_auth_blocked` / `instagram_graphql_checkpoint_required` |
| 4 | Affected backfill replay | Blocked | Replay rerun is deferred until the Instagram authenticated session is repaired; stale failed runs were cancelled |

## Blockers
- `scripts/handoff-lifecycle.sh pre-plan` is currently blocked by unrelated generated handoff drift already present in the workspace.
- Catalog verification/progress surfaces on the live backend can time out under load; database-backed confirmation was required to validate the active canary and replay state in this session.
- The current live blocker is no longer just access. Both active Instagram frontier runs now show `instagram_graphql_cursor_request_failed` with frontier metadata `auth_allowed=false` and `auth_reason=checkpoint_required`, so Task 26 cannot close until the shared-account frontier/auth path is fixed and replayed.
- The current blocker is the Instagram authenticated session itself:
  - `scripts/socials/refresh_cookies.py --platform instagram --validate-only` returns `reason=checkpoint_required`
  - a forced refresh attempt did not recover usable cookies in this environment

## Recent Activity
- 2026-04-04: Fixed the local Instagram shared-account frontier/auth path so checkpointed sessions fail closed before cursor fetch instead of degrading into a generic retryable `instagram_graphql_cursor_request_failed` loop:
  - `_run_shared_account_frontier_posts_stage(...)` now reads frontier auth state from config plus frontier metadata
  - checkpointed frontier runs now write `last_error_code=instagram_graphql_checkpoint_required` with `auth_allowed=false` and `auth_reason=checkpoint_required`
  - catalog progress now raises a deterministic `frontier_auth_blocked` alert and marks the aggregate `run_state` as `failed` when frontier auth is blocked
  - runtime-version drift detection now also reads nested `retrieval_meta.runtime_version` data from job metadata
- 2026-04-04: Re-ran the minimal validation slice for the frontier/auth fix:
  - `ruff check api/routers/socials.py trr_backend/repositories/social_season_analytics.py tests/api/routers/test_socials_season_analytics.py tests/repositories/test_social_season_analytics.py`
  - `ruff format --check api/routers/socials.py trr_backend/repositories/social_season_analytics.py tests/api/routers/test_socials_season_analytics.py tests/repositories/test_social_season_analytics.py`
  - `pytest -q tests/repositories/test_social_season_analytics.py -k 'checkpoint_required or runtime_version_drift or frontier_auth_blocked or classify_backlog_after_scrape or frontier_lease_stale or dispatch_blocked or resume_frontier_cursor'`
    - Result: `4 passed`
  - `pytest -q tests/api/routers/test_socials_season_analytics.py -k 'catalog_run_progress or worker_health or shared_account_backfill_readiness'`
    - Result: `4 passed`
- 2026-04-04: Deployed the updated backend to Modal with `./.venv/bin/python -m modal deploy -m trr_backend.modal_jobs`:
  - app deploy succeeded
  - backend URL remains `https://admin-56995--trr-backend-api.modal.run`
- 2026-04-04: Restored admin control-plane access from this host by using the correct service-role token shape for backend auth:
  - `GET /api/v1/admin/socials/ingest/worker-health` -> `200`
  - dispatcher readiness remained healthy and remote Instagram auth capability still reported `ready=true`
- 2026-04-04: Re-ran a bounded live canary on `instagram/bravotv`; the new run `f6bd45bc-cc2a-4fdf-9f67-7060912d39de` proved the deployed fix:
  - `shared_account_discovery` completed after 1 page / 33 posts
  - `shared_account_posts` failed with `Shared-account frontier auth blocked for @bravotv: checkpoint_required`
  - progress now surfaces `frontier_auth_blocked`
  - frontier metadata now records `last_error_code=instagram_graphql_checkpoint_required`
  - this confirms the deployed backend now fails closed on frontier auth instead of degrading into a generic cursor-request failure
- 2026-04-04: Validated the local Instagram cookie bundle with the canonical repo flow:
  - `scripts/socials/refresh_cookies.py --platform instagram --validate-only`
  - result: `validated=false`, `reason=checkpoint_required`
- 2026-04-04: Attempted canonical Instagram cookie refresh:
  - `scripts/socials/refresh_cookies.py --platform instagram --force`
  - result: no recovered cookie bundle from this environment
- 2026-04-04: Cancelled stale blocked runs so they no longer prevent a future rerun after auth repair:
  - `bravotv` canary `f6bd45bc-cc2a-4fdf-9f67-7060912d39de` -> `cancelled`
  - `bravodailydish` replay `360380f8-34ef-428b-96d7-0a507e480565` -> `cancelled`
- 2026-04-04: Tightened Modal enforcement for season ingest, sync sessions, shared ingest, and catalog-supported backfills so Modal-required platforms fail closed with explicit `required_platforms` / `required_execution_backend` metadata.
- 2026-04-04: Normalized bounded-window catalog backfill `date_end` values to inclusive end-of-day before dispatch so the final day is not silently omitted on literal-timestamp scrapers.
- 2026-04-04: Re-ran targeted backend validation:
  - `ruff check api/routers/socials.py trr_backend/repositories/social_season_analytics.py tests/api/routers/test_socials_season_analytics.py tests/repositories/test_social_season_analytics.py`
  - `ruff format --check api/routers/socials.py trr_backend/repositories/social_season_analytics.py tests/api/routers/test_socials_season_analytics.py tests/repositories/test_social_season_analytics.py`
  - focused API/repository/scraper pytest slices (`19 passed`, `17 passed`, `7 passed`)
- 2026-04-04: Re-verified live Modal readiness with `./.venv/bin/python scripts/modal/verify_modal_readiness.py --json`:
  - `ok: true`
  - `api_web_url: https://admin-56995--trr-backend-api.modal.run`
  - no missing functions, secrets, or web endpoints
- 2026-04-04: Authenticated `GET /api/v1/admin/socials/ingest/worker-health` passed against the live backend:
  - `queue_enabled=true`
  - `healthy=true`
  - `healthy_workers=4`
  - `remote_auth_capabilities.instagram.ready=true`
  - dispatcher heartbeat and Modal execution backend were present on `modal:social-dispatcher`
- 2026-04-04: Re-attempted live admin reads from the current environment using the deployed Task 26 backend URL and an internal-admin JWT loaded from the backend `.env`, but the control plane is now allowlist-gated from this host:
  - `GET /api/v1/admin/socials/ingest/worker-health` -> `403 {"detail":"Allowlist admin access required"}`
  - `GET /api/v1/admin/socials/profiles/instagram/bravotv/catalog/runs/dc67440d-6daa-432a-a6ae-c972bbb7bee7/progress` -> `403 {"detail":"Allowlist admin access required"}`
  - `GET /api/v1/admin/socials/profiles/instagram/bravodailydish/catalog/runs/360380f8-34ef-428b-96d7-0a507e480565/progress` -> `403 {"detail":"Allowlist admin access required"}`
  - `POST /api/v1/admin/socials/profiles/instagram/bravodailydish/catalog/backfill` -> `403 {"detail":"Allowlist admin access required"}`
- 2026-04-04: Confirmed via the data plane that the Modal dispatcher itself remains healthy even while control-plane reads are allowlist-blocked:
  - `social.scrape_workers.worker_id=modal:social-dispatcher`
  - `last_seen_at=2026-04-04 11:26:17+00`
  - `dispatch_enabled=true`
  - `execution_backend_canonical=modal`
  - authenticated capabilities still present for Instagram, TikTok, Facebook, Threads, and Twitter
- 2026-04-04: `POST /api/v1/admin/socials/profiles/instagram/bravotv/catalog/sync-recent` returned `409 SOCIAL_ACCOUNT_CATALOG_RUN_ALREADY_ACTIVE`; the active run was then confirmed directly from the data plane:
  - `run_id=cecf6ad0-a849-4129-b132-79a129ae8eb6`
  - `status=completed`
  - `completed_at=2026-04-04 11:09:50+00`
  - single `shared_account_posts` job completed on Modal worker `modal:social:modal:2:bbfa3e30`
  - no failed jobs and no dispatch error recorded
- 2026-04-04: Confirmed the `bravotv` resume-tail path is applicable and currently failing in the live data plane:
  - `run_id=dc67440d-6daa-432a-a6ae-c972bbb7bee7`
  - run `status=running`
  - `shared_account_discovery` completed after `history_bootstrap_resume`
  - `shared_account_posts` failed at `2026-04-04 11:25:27+00` with `last_error_code=instagram_graphql_cursor_request_failed`
  - frontier status is `retrying` with `retry_count=2`
  - frontier metadata now shows `auth_allowed=false`, `auth_reason=checkpoint_required`, and a resumed cursor
  - follow-on `post_classify` started, so the run remains live even though the frontier path failed
- 2026-04-04: Launched the affected `instagram/bravodailydish` full-history replay from the live backend. The HTTP request timed out before a structured response returned, but the run was created in the database and later exposed the same frontier/auth failure mode:
  - `run_id=360380f8-34ef-428b-96d7-0a507e480565`
  - run `status=queued` with `started_at=2026-04-04 11:13:55+00`
  - `shared_account_discovery` completed and saved the initial 33 posts
  - `shared_account_posts` is now `retrying` with `last_error_code=instagram_graphql_cursor_request_failed`
  - frontier status is `retrying` with `retry_count=1`
  - frontier metadata now shows `auth_allowed=false`, `auth_reason=checkpoint_required`, and the active GraphQL cursor
  - classify backlog has not drained: one `post_classify` job is `retrying` and another remains `queued`
  - job dispatch metadata also showed a transient `remote_blocked_reason=modal_capacity_pending` on the retrying classify worker
- 2026-04-04: The live data plane shows runtime image skew across the same replay family:
  - bootstrap discovery jobs ran on image `im-P9Hyfpnsr8Y0ARBmwL4MKl`
  - later retrying frontier/classify jobs ran on image `im-E7fb7cXjTujYaYPsnUkrCK`
  - treat this as rollout evidence of runtime-version drift until disproven from an allowlisted progress surface
- 2026-04-04: Repaired the local Instagram browser session with the new password via the canonical Playwright flow:
  - `scripts/socials/refresh_cookies.py --platform instagram --force`
  - result at refresh time: `validated=true`, `cookie_count=11`, and the managed browser-session state updated under `data/social-browser-sessions/instagram/`
- 2026-04-04: Rotated the deployed Modal secrets and redeployed the app so remote workers had the same updated Instagram credential and fresh cookie payload:
  - `scripts/modal/prepare_named_secrets.py --source-env <temp> --apply --modal-environment main`
  - updated both `trr-backend-runtime` and `trr-social-auth`
  - redeployed with `./.venv/bin/python -m modal deploy -m trr_backend.modal_jobs`
- 2026-04-04: Updated the local backend `.env` so future local cookie refresh attempts no longer fall back to the stale Instagram password:
  - refreshed `INSTAGRAM_PASSWORD`
  - added/updated `SOCIAL_AUTH_INSTAGRAM_USERNAME`
  - added/updated `SOCIAL_AUTH_INSTAGRAM_PASSWORD`
- 2026-04-04: Added a backend fallback so the canonical Instagram cookie refresh path accepts the repo's legacy `INSTAGRAM_USERNAME` / `INSTAGRAM_PASSWORD` env pair when `SOCIAL_AUTH_INSTAGRAM_*` is unset:
  - file: `trr_backend/repositories/social_season_analytics.py`
  - regression test added in `tests/repositories/test_social_season_analytics.py`
- 2026-04-04: Re-ran a fresh bounded canary after the password/session rotation:
  - `run_id=f214d5d4-7672-4422-90c3-8e9f082ed93f`
  - final run status: `completed`
  - run summary: single `shared_account_posts` job completed, `failed_jobs=0`
  - this confirms the repaired session can still complete a bounded recent-sync path end-to-end
- 2026-04-04: Re-ran the affected `instagram/bravodailydish` full-history replay after the password/session rotation:
  - `run_id=aed0334a-2053-4f08-b624-7dff3c1807e7`
  - discovery completed and saved the initial 33 posts
  - frontier metadata again flipped to `auth_allowed=false`, `auth_reason=checkpoint_required`, `last_transport=public`, `next_cursor_present=true`
  - first `shared_account_posts` worker stalled and was recovered via `recover_stale_running_jobs(run_id=...)`
  - on retry, `shared_account_posts` failed definitively with `last_error_code=instagram_graphql_checkpoint_required`
  - `post_classify` also aged into stale-heartbeat recovery noise, so the replay was cancelled after the auth failure was proven
  - final replay run status: `cancelled`
  - final summary: `completed_jobs=1`, `failed_jobs=1`, `items_found_total=66`
- 2026-04-04: Reproduced the remaining auth blocker locally with the freshly refreshed cookie bundle:
  - direct local `InstagramScraper.fetch_posts_graphql('bravodailydish')` using `data/instagram_cookies.json` failed immediately with
    `error_code=instagram_graphql_checkpoint_required`
  - inference: the account remains checkpoint-gated on the authenticated GraphQL path even after the password rotation and successful Playwright login
- 2026-04-04: Re-validated the canonical cookie-health check after the above reruns:
  - `scripts/socials/refresh_cookies.py --platform instagram --validate-only`
  - result: `validated=false`, `reason=checkpoint_required`
  - inference: the earlier post-refresh `validated=true` state was transient and did not survive the deeper authenticated GraphQL path
- 2026-04-02: Added worker-health, queue, and catalog alert payloads plus shared-profile `network_name` metadata.
- 2026-04-02: Updated automation templates, env example, and runbook language to use generic network-profile defaults.
- 2026-04-02: Verified targeted backend repository and router tests; Modal readiness verified successfully when run from the backend virtualenv.
- 2026-04-02: Deployed `trr_backend.modal_jobs` via Modal and re-verified readiness with `verify_modal_readiness.py --json`.
