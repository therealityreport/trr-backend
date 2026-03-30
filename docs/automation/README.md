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
3. If you imported a non-credential workflow:
   - set `bearerToken`
4. If you imported a credential workflow:
   - open each HTTP Request node
   - create or select an `HTTP Bearer Auth` credential
5. For `sync-recent`, also set:
   - `lookbackDays`
6. For `backfill`, also set:
   - `backfillScope`
   - optionally `dateStartIso`
   - optionally `dateEndIso`
7. Run it manually first.
8. If it behaves correctly, replace `Manual Trigger` with a schedule trigger.

## Recommended choice

- Use the `_credential.json` variants in real n8n environments.
- Keep the non-credential variants for quick local setup or debugging.

## Notes

- The `sync-recent` workflows start a `catalog/sync-recent` run and poll the TRR progress route until the run reaches a terminal state.
- The `backfill` workflows start a `catalog/backfill` run and poll the same progress route until the run reaches a terminal state.
- Credential workflows use n8n's `HTTP Bearer Auth` credential mode instead of storing the token in the workflow payload.
- This workflow is intentionally account-specific. Duplicate it per account if you want simple `n8n` scheduling with clear ownership.
