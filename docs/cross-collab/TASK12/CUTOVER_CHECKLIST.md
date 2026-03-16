# Cast Screen-Time Cutover Checklist

Repo: TRR-Backend  
Last updated: 2026-03-16

## Enablement Guard
- `TRR_CAST_SCREENTIME_ADMIN_ENABLED`
  - `true`: new `TRR-APP` admin surface and proxy remain available
  - `false`: new `TRR-APP` admin surface and proxy are hidden/404 for rollback

## Go / No-Go Inputs
- Reuse matrix completed and reviewed
- Golden Dataset validator reports `pass` on curated cases
- Deployed smoke run completed for:
  - episode upload
  - promo upload
  - YouTube promo import
  - episode approve + publish
- Stale-run recovery drill completed
- Managed Chrome operator walkthrough completed
- Promo assets verified as independent reports only
- Show/season rollups verified against published episode assets only

## Enablement Order
1. Confirm `TRR_CAST_SCREENTIME_ADMIN_ENABLED=true` in the target app environment.
2. Run the deployed smoke script against the target backend.
3. Run the managed operator walkthrough in `TRR-APP`.
4. Record evidence into the three acceptance reports.
5. Announce cutover only after all reports show no blocking `PENDING` items.

## Rollback
1. Set `TRR_CAST_SCREENTIME_ADMIN_ENABLED=false` in the app environment.
2. Confirm `/admin/cast-screentime` returns `404` and the admin proxy route returns `404`.
3. Keep backend schema and worker code in place; no migration rollback is required.
4. Fall back to the legacy/reference operator path only if necessary.

## Stop Conditions
- Golden Dataset fail
- stale-run replay duplicates persisted state
- publish flow affects promo assets or canonical totals incorrectly
- operator walkthrough finds a blocking admin workflow defect
