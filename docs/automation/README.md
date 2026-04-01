# n8n Automation Templates

## Files

- `n8n_trr_instagram_catalog_sync_recent.json`
- `n8n_trr_instagram_catalog_sync_recent_credential.json`
- `n8n_trr_instagram_catalog_backfill.json`
- `n8n_trr_instagram_catalog_backfill_credential.json`

## How to use

1. Import the JSON into `n8n`.
2. Open the `Set Config` node and set:
   - `baseUrl`
   - `accountHandle`
   - `sourceScope`
   - use the canonical backend admin base, not the public app host
3. If you imported a non-credential workflow:
   - set `bearerToken`
   - this must be a bearer JWT for the backend internal-admin path, not the raw `TRR_INTERNAL_ADMIN_SHARED_SECRET`
4. If you imported a credential workflow:
   - open each HTTP Request node
   - create or select an `HTTP Bearer Auth` credential
   - store the same internal-admin bearer JWT in that credential
5. For `sync-recent`, also set:
   - `lookbackDays`
6. For `backfill`, also set:
   - `backfillScope`
   - optionally `dateStartIso`
   - optionally `dateEndIso`
7. Run it manually first.
8. If it behaves correctly, replace `Manual Trigger` with a schedule trigger.
9. Use `Sync Recent` as the first canary after Modal deploy. Use `Resume Tail` or `Backfill Posts` only after the worker-health surface shows remote Instagram auth and shared-account backfill readiness as green.

## Recommended choice

- Use the `_credential.json` variants in real n8n environments.
- Keep the non-credential variants for quick local setup or debugging.

## Current Template Inventory

These checked-in workflows are starter templates, not proof that a live external
`n8n` instance has been configured correctly. Current repo-owned inventory:

| Template | Classification | Trigger | Launch endpoint | Poll endpoint | Auth | Poll cadence | Terminal states |
|---|---|---|---|---|---|---|---|
| `n8n_trr_instagram_catalog_sync_recent_credential.json` | `launches-backfill` | `Manual Trigger` by default; replace with schedule after canary | `POST /api/v1/admin/socials/profiles/{platform}/{account_handle}/catalog/sync-recent` | `GET /api/v1/admin/socials/profiles/{platform}/{account_handle}/catalog/runs/{run_id}/progress` | `HTTP Bearer Auth` credential carrying an internal-admin bearer JWT | `pollSeconds` (default `20`) | `completed`, `failed`, `cancelled` |
| `n8n_trr_instagram_catalog_sync_recent.json` | `launches-backfill` | `Manual Trigger` by default; replace with schedule after canary | same as above | same as above | inline bearer JWT value in workflow config; debug/local-only | `pollSeconds` (default `20`) | `completed`, `failed`, `cancelled` |
| `n8n_trr_instagram_catalog_backfill_credential.json` | `launches-backfill` | `Manual Trigger` by default; replace with schedule after canary | `POST /api/v1/admin/socials/profiles/{platform}/{account_handle}/catalog/backfill` | `GET /api/v1/admin/socials/profiles/{platform}/{account_handle}/catalog/runs/{run_id}/progress` | `HTTP Bearer Auth` credential carrying an internal-admin bearer JWT | `pollSeconds` (default `20`) | `completed`, `failed`, `cancelled` |
| `n8n_trr_instagram_catalog_backfill.json` | `launches-backfill` | `Manual Trigger` by default; replace with schedule after canary | same as above | same as above | inline bearer JWT value in workflow config; debug/local-only | `pollSeconds` (default `20`) | `completed`, `failed`, `cancelled` |

No live workflow id is tracked in-repo. If an external `n8n` deployment is used
in production, that deployment still needs a separate owner review before anyone
claims "`n8n` is ready" as an environment, not just as a template set.

## Notes

- The `sync-recent` workflows start a `catalog/sync-recent` run and poll the TRR progress route until the run reaches a terminal state.
- The `backfill` workflows start a `catalog/backfill` run and poll the same progress route until the run reaches a terminal state.
- Credential workflows use n8n's `HTTP Bearer Auth` credential mode instead of storing the token in the workflow payload.
- If the backend returns `SOCIAL_MODAL_DISPATCH_UNAVAILABLE`, `SOCIAL_MODAL_EXECUTOR_REQUIRED`, `SOCIAL_WORKER_UNAVAILABLE`, or an auth-preflight failure, treat that as a control-plane/runtime problem and stop scheduling more runs until Modal and worker health are green.
- For full-history Instagram backfills, confirm backend worker health reports:
  - `dispatcher_readiness.resolved = true`
  - `dispatcher_heartbeat_fresh = true`
  - `remote_auth_capabilities.instagram.ready = true`
  - `shared_account_backfill_readiness.ready = true`
- A checked-in template passing review does not mean a live external `n8n`
  instance is ready. Live readiness additionally requires confirming the active
  workflow id, schedule/trigger owner, credential source, retry policy, and
  whether the workflow preserves backend response details instead of collapsing
  them into generic `n8n` errors.
- This workflow is intentionally account-specific. Duplicate it per account if you want simple `n8n` scheduling with clear ownership.
