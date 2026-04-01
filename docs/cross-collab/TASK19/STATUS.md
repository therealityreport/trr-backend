# Status — Task 19 (Env contract migration and local startup recovery)

Repo: TRR-Backend
Last updated: 2026-03-30

## Phase Status

| Phase | Description | Status | Notes |
|---|---|---|---|
| 1 | Local startup recovery | Complete | Workspace launcher now exports `TRR_LOCAL_DEV=1`; backend local startup no longer requires deployed-only secrets. |
| 2 | Runtime contract normalization | Complete | Backend runtime and scripts prefer `TRR_DB_URL` / `TRR_DB_FALLBACK_URL`; legacy names are compatibility-only. |
| 3 | Validation | Complete | Focused pytest coverage and workspace smoke checks passed. |
| 4 | Live env inventory | Complete | Render inventory captured; Modal secret names inventoried; no unsafe backend live deletions performed. |

## Blockers
- No backend-local blocker remains.
- Survey cutover remains intentionally blocked until the full cross-repo env-contract migration is closed out.

## Recent Activity
- 2026-03-30: Added backend startup regression coverage proving `TRR_LOCAL_DEV=1` bypasses deployed-only secret requirements only for local workspace runs.
- 2026-03-30: Added `scripts/_db_url.py` and migrated backend scripts toward canonical DB precedence (`TRR_DB_URL`, optional `TRR_DB_FALLBACK_URL`).
- 2026-03-30: Updated repository/runtime recovery hints to stop advertising `SUPABASE_DB_URL` as the normal runtime contract.
- 2026-03-30: Focused backend validation passed: `pytest -q tests/test_startup_config.py tests/db/test_connection_resolution.py tests/scripts/test_prepare_named_secrets.py`.
- 2026-03-30: Render inventory confirmed canonical `TRR_DB_URL` and `TRR_API_URL`; Modal inventory was limited to secret names, so value-level mutation was intentionally deferred.
