# Social Ingest n8n Setup

This is the recommended control-plane shape for TRR social scraping when `n8n` is used to trigger work.

## What TRR now guarantees

- Instagram browser state is isolated per account.
- Shared Instagram execution paths acquire an account-level backend lock before running.
- If `n8n` is misconfigured and submits overlapping work for the same account, the backend now serializes that execution instead of allowing session bleed.

## What n8n should do

Use `n8n` as the trigger and polling layer, not as the browser runtime.
Ready-made starter templates live in `docs/automation/`, including credential-based variants that use n8n `HTTP Bearer Auth` credentials instead of embedding the token in workflow config.

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

## Workflow rules

- Keep one active workflow lane per account handle.
- Do not let the same Instagram account overlap across workflows.
- Different accounts may run concurrently.
- Keep Playwright session ownership in TRR workers, not in `n8n`.

## Zapier

Zapier is acceptable for notifications or operator approvals after run completion.
Do not use Zapier as the primary scraper queue or session scheduler.

## Notte

Notte is optional and should only be introduced if login/session refresh becomes the bottleneck.
The primary scraping path should stay on TRR's Playwright-based backend runtime.
