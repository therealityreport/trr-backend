# Modal Instagram Auth Repair

Last updated: 2026-04-06

## Status
- Backend and Modal worker repair complete.
- Remote Instagram auth probe is passing in the deployed Modal worker plane.
- Social dispatcher heartbeat is fresh and reporting `instagram_authenticated = true`.

## What changed
- Split Instagram worker auth validation into structural cookie checks plus a single GraphQL canary with stable reason buckets.
- Added additive `detail` fields to worker auth and shared-account readiness payloads.
- Added the deployed Modal function `probe_social_remote_auth`.
- Extended `scripts/modal/verify_modal_readiness.py` with `--probe-remote-auth instagram`.
- Added `scripts/modal/repair_instagram_auth.py` to run refresh, local validation, secret apply, deploy, and remote probe as one fail-closed operator workflow.
- Tightened scraper-side cookie validation and disabled browser fallback during validator-only checks.

## Live validation
- Passed: `./.venv/bin/python scripts/modal/verify_modal_readiness.py --probe-remote-auth instagram --json`
- Passed: `./.venv/bin/python - <<'PY' ... heartbeat_remote_executors.remote() ... PY`
- Confirmed in `social.scrape_workers`:
  - `modal:social-dispatcher`
  - `status = idle`
  - fresh heartbeat
  - `metadata.auth_capabilities.instagram_authenticated = true`
- Confirmed queue tables are clear of non-terminal work:
  - `social.scrape_runs`: only `completed`, `cancelled`, `failed`
  - `social.scrape_jobs`: only `completed`, `cancelled`, `failed`

## Operator note
- If the admin UI still shows the old remote-worker error, refresh the page so it reloads worker health from the updated dispatcher heartbeat.

## Handoff Snapshot
```yaml
handoff:
  include: true
  state: recent
  last_updated: 2026-04-06
  current_phase: "modal instagram auth repair"
  next_action: "Refresh the admin social profile page and run Backfill Posts."
  detail: self
```
