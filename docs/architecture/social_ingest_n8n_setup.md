# Social Ingest n8n Setup

This is the recommended control-plane shape for TRR social scraping when `n8n` is used to trigger work.

## Current audit status

- Repo-owned `n8n` coverage exists only as checked-in starter templates under
  `docs/automation/`.
- Those templates are classified as `launches-backfill` control-plane workflows:
  they launch catalog runs and poll backend progress until terminal state.
- No live external `n8n` instance, workflow id, or schedule ownership is stored
  in this repo. That means the templates are reviewed, but any actual external
  `n8n` environment still requires a separate operational audit before it can be
  called ready.

## What TRR now guarantees

- Instagram browser state is isolated per account.
- Shared Instagram execution paths acquire an account-level backend lock before running.
- If `n8n` is misconfigured and submits overlapping work for the same account, the backend now serializes that execution instead of allowing session bleed.

## What n8n should do

Use `n8n` as the trigger and polling layer, not as the browser runtime.
Ready-made starter templates live in `docs/automation/`, including credential-based variants that use n8n `HTTP Bearer Auth` credentials instead of embedding the token in workflow config.
Those credentials must hold an internal-admin bearer JWT for the backend admin
routes. Do not send the raw `TRR_INTERNAL_ADMIN_SHARED_SECRET` as a header value.

Recommended flow:

1. Trigger a TRR admin API route for the account:
   - `POST /api/v1/admin/socials/profiles/{platform}/{account_handle}/catalog/backfill`
   - `POST /api/v1/admin/socials/profiles/{platform}/{account_handle}/catalog/sync-recent`
   - `POST /api/v1/admin/socials/profiles/{platform}/{account_handle}/catalog/sync-newer`
   - `POST /api/v1/admin/socials/profiles/{platform}/{account_handle}/catalog/resume-tail`
2. Read the returned `run_id`.
3. Poll:
   - `GET /api/v1/admin/socials/profiles/{platform}/{account_handle}/catalog/runs/{run_id}/progress`
4. Stop polling when the run reaches a terminal state.
5. Optionally call:
   - `POST /api/v1/admin/socials/profiles/{platform}/{account_handle}/catalog/runs/{run_id}/cancel`

Before scheduling full-history Instagram backfills from `n8n`, require this
backend readiness contract:

- `scripts/modal/verify_modal_readiness.py --json` returns `ok: true`
- `GET /api/v1/admin/socials/ingest/worker-health` reports:
  - `dispatcher_readiness.resolved = true`
  - `dispatcher_heartbeat_fresh = true`
  - `remote_auth_capabilities.instagram.ready = true`
  - `shared_account_backfill_readiness.ready = true`
- a manual `Sync Recent` canary succeeds first

And before claiming a live `n8n` environment is ready, confirm all of the
following for the external workflow instance:

- workflow name/id
- trigger or schedule ownership
- whether it launches, retries, monitors, or only notifies
- exact backend endpoint it calls
- auth mode and credential owner
- timeout/retry behavior
- preservation of backend failure details for dispatch/auth/worker-plane errors

## Workflow rules

- Keep one active workflow lane per account handle.
- Do not let the same Instagram account overlap across workflows.
- Different accounts may run concurrently.
- Keep Playwright session ownership in TRR workers, not in `n8n`.
- If launch fails with `SOCIAL_MODAL_DISPATCH_UNAVAILABLE`,
  `SOCIAL_MODAL_EXECUTOR_REQUIRED`, `SOCIAL_WORKER_UNAVAILABLE`, or auth
  preflight errors, surface the backend detail directly and pause retries until
  the worker plane is healthy again.

## Zapier

Zapier is acceptable for notifications or operator approvals after run completion.
Do not use Zapier as the primary scraper queue or session scheduler.

## Notte

Notte is optional and should only be introduced if login/session refresh becomes the bottleneck.
The primary scraping path should stay on TRR's Playwright-based backend runtime.
