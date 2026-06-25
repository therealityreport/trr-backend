# Modal serve_backend_api Crash Loop: v439 to v440

Date: 2026-05-28
Workspace: `admin-56995`
App: `trr-backend-jobs`
Function: `trr_backend.modal_jobs.serve_backend_api`

## Summary

Modal emailed that `serve_backend_api` containers were repeatedly failing to
start after deploy `v439`. The API image could resolve the deployed web
endpoint, but cold-started containers failed while importing `api.main`.

## Timeline

- `2026-05-28 10:37:31 EDT`: deploy `v439` completed in workspace
  `admin-56995`.
- `2026-05-28 10:37:37 EDT`: first `serve_backend_api` import failure logged.
- `2026-05-28 10:42:18 EDT`: Modal emitted crash-looping signal for
  `serve_backend_api`.
- `2026-05-28 11:20 EDT`: readiness still resolved the app and web endpoint,
  but logs showed repeated cold-start failures.
- `2026-05-28 11:23:23 EDT`: deploy `v440` completed through the pinned
  `admin-56995` wrapper.
- `2026-05-28 11:23:41 EDT`: `/health` returned HTTP 200 with database
  connected.

## Cause

`serve_backend_api` imports `api.main`, and `api.main` imports routers including
`api.routers.admin_show_sync`. That router imported `scripts.sync.*` modules at
module import time. The lean API Modal image mounted `api` and `trr_backend`,
but did not mount the top-level `scripts` package, so container startup failed:

```text
ModuleNotFoundError("No module named 'scripts'")
```

## Fix

- `api.routers.admin_show_sync` now lazy-loads `scripts.sync.*` modules only
  when script-backed endpoints execute.
- The lean Modal image mounts the minimal script payload required by API/admin
  sync paths:
  - `scripts/__init__.py`
  - `scripts/_sync_common.py`
  - `scripts/sync`
- Modal image payload validation now checks required local mount paths for each
  image family before image construction.
- The deploy wrapper now runs readiness and an API `/health` cold-start canary
  after successful deploys.

## Verification

- Deploy `v440` URL:
  `https://modal.com/apps/admin-56995/main/deployed/trr-backend-jobs`
- Readiness:
  - `ok = true`
  - `api_web_url` resolved for `serve_backend_api`
  - `missing_functions = []`
  - `missing_web_endpoints = []`
- `/health` returned HTTP 200 three times after `v440`.
- Logs after `2026-05-28 11:23:20 EDT` showed API startup and health probes,
  with no new `ModuleNotFoundError` or crash-looping entry.

## Follow-up Hardening

- Keep the deploy wrapper as the only deploy path for Modal backend changes.
- Treat readiness-only success as insufficient for API deploys; the cold-start
  canary must pass.
- Keep script-backed admin endpoints lazy so API startup does not depend on
  optional script payloads.

## v442 Guard Regression And Replacement

Deploy `v442` briefly exposed a second startup failure after the image payload
guard was added. The guard correctly knew about browser-family script mounts,
but it ran inside deployed Modal containers where those source paths are not
present. That made the API function import `trr_backend.modal_jobs` and fail
before serving `/health`.

The fix was to keep the guard strict for local deploy/image construction while
skipping local-path validation inside remote runtime containers. Deploy `v443`
replaced `v442`; readiness returned `ok = true`, the API canary returned HTTP
200 on attempt 1, and recent logs no longer showed `Runner failed` or
crash-looping entries.

<!-- modal-deploy-history:start -->
## Deploy History Stamp

- Last stamped: `2026-06-25T16:36:37-04:00`
- Workspace: `admin-56995`
- Profile: `admin-56995`
- Canary: `https://admin-56995--trr-backend-api.modal.run/health` HTTP `200` on attempt `1`

| Version | Deployed At | Deployed By | Commit | Client |
| --- | --- | --- | --- | --- |
| v94 | 2026-06-25 16:36:11-04:00 | admin-56995 | 0d1a1b0* | 1.4.0 |
| v93 | 2026-06-25 13:28:00-04:00 | admin-56995 | 0d1a1b0* | 1.4.0 |
| v92 | 2026-06-25 13:18:08-04:00 | admin-56995 | 0d1a1b0* | 1.4.0 |
| v91 | 2026-06-25 13:08:52-04:00 | admin-56995 | 0d1a1b0* | 1.4.0 |
| v90 | 2026-06-25 12:54:44-04:00 | admin-56995 | 0d1a1b0* | 1.4.0 |

<!-- modal-deploy-history:end -->
