# Status — Task 11 (Tab-Isolated Admin Operations + Resumable Streams)

Repo: TRR-Backend
Last updated: March 5, 2026

## March 7 live rollout follow-up — admin image analysis is now on Modal in staging

- Modal app `trr-backend-jobs` was redeployed with the dedicated vision function:
  - `run_admin_vision`
- Staging artifact and runtime:
  - artifact:
    - `s3://trr-backend/artifacts/trr-backend/20260307-194422/trr_backend_modal_vision_cutover_20260307-194422.tar.gz`
  - API host:
    - `i-01a7b672f5946d19a`
  - added staging SSM runtime parameters:
    - `TRR_MODAL_VISION_FUNCTION=run_admin_vision`
    - `TRR_ADMIN_IMAGE_EXECUTION_BACKEND=modal`
    - `TRR_BACKEND_ARTIFACT_S3_URI=s3://trr-backend/artifacts/trr-backend/20260307-194422/trr_backend_modal_vision_cutover_20260307-194422.tar.gz`
- Live host verification:
  - artifact/env overlay SSM command: `2cbccd6c-0b34-4390-ba59-0f36df203009`
  - command output confirmed:
    - `{"status":"healthy"}`
    - `systemctl is-active trr-api` -> `active`
    - `modal_function_ok=run_admin_vision`
    - `/etc/trr-api.env` contains `TRR_MODAL_VISION_FUNCTION=run_admin_vision`
    - `/etc/trr-api.env` contains `TRR_ADMIN_IMAGE_EXECUTION_BACKEND=modal`
- Runtime outcome:
  - covered admin image-analysis routes are now Modal-backed on staging and no longer require the Screenalytics HTTP service to execute
  - `SCREENALYTICS_API_URL` remains present only for non-admin surfaces outside this cutover
  - `trr-worker-asg` remains absent; only `trr-api-asg` is visible in this AWS account
- Validation:
  - `python scripts/modal/verify_modal_readiness.py --json` -> `ok=true`, seven functions resolved, no missing secrets
  - `aws elbv2 describe-target-health ... trr-api-tg ...` -> `i-01a7b672f5946d19a` healthy

## March 7 Live Rollout — Staging moved to remote+Modal, production target absent in current AWS account

- Executed the staging runtime rollout for the canonical remote executor contract:
  - named Modal secrets created:
    - `trr-backend-runtime`
    - `trr-social-auth`
  - Modal app `trr-backend-jobs` redeployed and now resolves all six functions:
    - `run_admin_operation`
    - `run_google_news_sync`
    - `run_reddit_refresh`
    - `run_social_job`
    - `sweep_social_dispatch_queue`
    - `heartbeat_remote_executors`
- Staging artifact and runtime:
  - artifact:
    - `s3://trr-backend/artifacts/trr-backend/20260307-191044/trr_backend_modal_remote_cutover_20260307-191044.tar.gz`
  - API host:
    - `i-01a7b672f5946d19a`
  - SSM runtime parameters now target:
    - `TRR_JOB_PLANE_MODE=remote`
    - `TRR_LONG_JOB_ENFORCE_REMOTE=1`
    - `TRR_REMOTE_EXECUTOR=modal`
    - `TRR_MODAL_ENABLED=1`
    - `TRR_MODAL_APP_NAME=trr-backend-jobs`
    - `TRR_MODAL_ADMIN_OPERATION_FUNCTION=run_admin_operation`
    - `TRR_MODAL_GOOGLE_NEWS_FUNCTION=run_google_news_sync`
    - `TRR_MODAL_REDDIT_REFRESH_FUNCTION=run_reddit_refresh`
    - `TRR_MODAL_SOCIAL_JOB_FUNCTION=run_social_job`
    - `TRR_MODAL_SOCIAL_RECOVERY_FUNCTION=sweep_social_dispatch_queue`
    - `TRR_MODAL_RUNTIME_SECRET_NAME=trr-backend-runtime`
    - `TRR_MODAL_SOCIAL_SECRET_NAME=trr-social-auth`
    - `SOCIAL_QUEUE_ENABLED=true`
    - `TRR_BACKEND_ARTIFACT_S3_URI=s3://trr-backend/artifacts/trr-backend/20260307-191044/trr_backend_modal_remote_cutover_20260307-191044.tar.gz`
- Host auth fix:
  - rollout initially landed code and env successfully but host-side Modal auth was missing
  - added SecureString SSM params:
    - `/trr/staging/MODAL_TOKEN_ID`
    - `/trr/staging/MODAL_TOKEN_SECRET`
  - reconciled `/etc/trr-api.env` to include `MODAL_TOKEN_ID` and `MODAL_TOKEN_SECRET`
  - restarted `trr-api`; host can now hydrate all six Modal functions directly
- Verification:
  - artifact/env overlay SSM command: `41f65990-8dcb-41e1-b65c-8b2419fe6dc5`
  - Modal token reconcile SSM command: `669c2b17-9688-452a-b11d-ca2604a41aea`
  - worker-health proof SSM command: `b15f3b57-08b5-407f-b79d-c4e466694e35`
  - ALB target health:
    - `trr-api-tg` -> `i-01a7b672f5946d19a` healthy
  - social worker-health now reports fresh Modal dispatcher rows:
    - `modal:social-dispatcher`
    - `modal:reddit-dispatcher`
    - `modal:google-news-dispatcher`
    - `modal:admin-dispatcher`
    with `execution_backend_canonical=modal`
- Production status:
  - production rollout was not executable in this AWS account context
  - `/trr/production/*` SSM namespace is empty
  - only one ASG is visible: `trr-api-asg`

## March 7 Final Follow-Up — Linked Supabase schema applied, local Modal proof captured, and lint baseline cleared

- Completed the remaining non-mutating Modal cutover prep items without touching staging or production:
  - local workspace launcher/profile now start the backend with the canonical `remote + modal` env contract by default
  - named-secret enforcement remains production-only; local/dev still uses the safe fallback path
  - rollout helper scripts now cover secret rendering, cutover command generation, and readiness verification
- Applied `0179_shared_social_account_ingest.sql` to the linked Supabase project so the shared-ingest admin surfaces can run against the real schema.
- Fixed a backend regression in social worker-health payload building:
  - Modal-enabled `get_worker_health(...)` now always returns the Modal-enriched executor payload instead of only touching dispatcher heartbeats
  - this unblocked the admin system-health modal from showing `Execution backend: Modal`
- Full validation now includes:
  - repo-wide backend `ruff check .` passing
  - focused backend Modal/social regression tests passing
  - local managed-Chrome verification against the workspace after restarting in `remote + modal`
- Browser evidence captured:
  - `/Users/thomashulihan/Projects/TRR/.tmp/system-health-modal-after-0179.png`
  - `/Users/thomashulihan/Projects/TRR/.tmp/social-media-shared-ingest-after-0179.png`
  - `/Users/thomashulihan/Projects/TRR/.tmp/rhoslc-social-after-0179.png`
- Supabase proof:
  - `information_schema.tables` now returns:
    - `shared_account_sources`
    - `shared_post_matches`
    - `shared_post_review_queue`
- Result:
  - `/admin/social-media` now renders shared source inventory and review queue without schema errors
  - the executor-health modal still verifies cleanly under local `remote + modal`

## March 7 Late Follow-Up — Modal readiness verification and EC2-retirement guardrails added

- Added non-mutating operator support for the remaining Modal cutover checks:
  - `scripts/modal/verify_modal_readiness.py` verifies the named secrets, deployed `trr-backend-jobs` app, and all six required Modal functions
  - `scripts/modal/render_cutover_commands.py` now includes the readiness check in the rollout checklist
  - `scripts/modal/prepare_named_secrets.py --apply` now deletes rendered secret env files by default after publishing, unless `--keep-rendered-files` is passed
- Clarified the cutover boundary in runbooks:
  - social worker cutover readiness is not the same thing as full EC2 retirement
  - EC2 retirement remains incomplete while documented admin image-analysis jobs still depend on `SCREENALYTICS_API_URL`
- Validation:
  - targeted pytest for the new readiness script

## March 7 Follow-Up — API moved to local+Modal ownership and EC2 worker fleet drained

- Goal:
  - stop paying for always-on EC2 workers and keep long jobs on demand only
- Backend/runtime changes now live on the API host:
  - admin operation kickoff can dispatch supported job types to Modal even with `TRR_JOB_PLANE_MODE=local`
  - Google News async sync can dispatch to Modal, otherwise falls back to API background execution
  - Reddit refresh kickoff can dispatch to Modal, otherwise falls back to API background execution
  - social ingest endpoints now run inline/background on the API when queue mode is disabled, instead of requiring the EC2 social worker plane
- Modal deployment:
  - app: `trr-backend-jobs`
  - deployed app id: `ap-zCguzD3kS4CL6v7tLJElyv`
  - functions:
    - `run_admin_operation`
    - `run_google_news_sync`
    - `run_reddit_refresh`
- Live rollout:
  - artifact: `s3://trr-backend/artifacts/trr-backend/20260307-161237/trr_backend_modal_worker_cutover_20260307-161237.tar.gz`
  - API rollout SSM command: `ddbd3ce1-0a2e-43d6-97a3-336134308343`
    - install/update work completed; command only failed its first immediate localhost health probe after restart
  - API verification SSM command: `2cf72270-1df6-4130-a8e7-70e3ba065d57`
    - `trr-api` active
    - `modal` installed in `/opt/trr-backend/.venv`
    - Modal app lookup from the host succeeded
    - localhost `/health` returned healthy
- Boot/runtime configuration changed:
  - `/etc/trr-api.env`
    - `TRR_JOB_PLANE_MODE=local`
    - `TRR_LONG_JOB_ENFORCE_REMOTE=0`
    - `TRR_MODAL_ENABLED=1`
    - `TRR_MODAL_APP_NAME=trr-backend-jobs`
    - `TRR_MODAL_ADMIN_OPERATION_FUNCTION=run_admin_operation`
    - `TRR_MODAL_GOOGLE_NEWS_FUNCTION=run_google_news_sync`
    - `TRR_MODAL_REDDIT_REFRESH_FUNCTION=run_reddit_refresh`
    - `SOCIAL_QUEUE_ENABLED=false`
  - SSM parameters updated:
    - `/trr/staging/TRR_JOB_PLANE_MODE=local`
    - `/trr/staging/TRR_LONG_JOB_ENFORCE_REMOTE=0`
    - `/trr/staging/TRR_BACKEND_ARTIFACT_S3_URI=s3://trr-backend/artifacts/trr-backend/20260307-161237/trr_backend_modal_worker_cutover_20260307-161237.tar.gz`
- Worker fleet shutdown:
  - `aws autoscaling update-auto-scaling-group --region us-east-1 --auto-scaling-group-name trr-worker-asg --min-size 0 --desired-capacity 0`
  - observed state:
    - worker instances `i-048f605583d2a11dd` and `i-0c4934e80de1dd89d` reached `terminated`
    - worker instance `i-02e0d952e67e302a0` was still `shutting-down` / `Terminating` at capture time
    - `trr-worker-asg` now reports `MinSize=0`, `DesiredCapacity=0`
- Verification:
  - local targeted tests: `153 passed`
  - touched runtime file lint: pass
  - ALB target health for `trr-api-tg`: `healthy`
  - pre-scale DB snapshot showed no active queued/running admin, Google News, or social scrape jobs
- Remaining follow-up:
  - `trr-worker-asg` still exists with `MaxSize=8`; only the running capacity was drained in this pass
  - historical backlog rows still exist:
    - `reddit_refresh_runs={partial: 38}`
    - `social_scrape_runs={queued: 2}`

## March 6 Late Follow-Up — Season social run scoping and bounded single-platform scheduling deployed

- Root cause behind the oversized one-platform season syncs:
  - the frontend could request a single platform/day/week run, but the backend run list and schedule creation paths were not fully scoped to that exact run window
  - bounded one-platform season/date-window syncs were still allowed to keep caller-provided dual-runner, fine-grained shard settings, which produced huge job counts for a simple single-platform sync
- Backend changes:
  - `api/routers/socials.py`
    - ingest routes now resolve canonical week windows from `week_index`
    - run list and run summary endpoints now accept and return `client_workflow_id`, `platforms`, `week_index`, `date_start`, `date_end`
  - `trr_backend/repositories/social_season_analytics.py`
    - run listing now filters by scope-bearing config fields
    - bounded one-platform syncs are coerced to `single_runner`, `runner_count=1`, coarse shard windows
    - exact run-scope metadata is preserved on created runs so the app can recover only the relevant runs for the current page/tab
- Validation:
  - local targeted checks passed:
    - `py_compile`
    - repository test for scoped run filtering + creator scheduling coercion
    - router test for ingest-runs filter passthrough
- Live rollout:
  - payload: `s3://trr-backend/artifacts/trr-backend/20260306-192954/trr_backend_social_scope_fix.tar.gz`
  - API rollout SSM command: `4545406d-cfda-454a-acc4-4b4f64de41d9`
  - worker rollout SSM command: `549b720d-a51d-43cc-897c-4e44ac0b461e`
  - verification SSM command: `c2ac9e3f-2769-44f6-96d3-ca6774c128c2`
  - restarted `trr-api` and `trr-social-worker-pool.service`
- Live verification run:
  - created run `09765051-9627-4e3a-8977-60087fce671c` using the same RHOSLC Twitter preseason date window that previously produced 408 jobs
  - saved run config after coercion:
    - `runner_strategy=single_runner`
    - `runner_count=1`
    - `window_shard_hours=24`
  - final run summary:
    - `total_jobs=2`
    - `completed_jobs=2`
  - resulting jobs were only:
    - `twitter/posts @bravotv`
    - `twitter/posts @bravowwhl`
  - this confirms the Supabase-backed queue no longer explodes bounded one-platform season runs into hundreds of shard jobs


## March 6 Follow-Up — Queue worker cancel probe hardened and active-worker counts narrowed

- Review-driven follow-up on the social worker plane found one remaining fragile edge:
  - the worker was running a pre-process cancel probe before `process_claimed_job(...)`
  - that probe was outside the existing processing failure guard, so a transient DB failure there could still kill the queue loop before repository cancellation/failure handling ran
- Backend changes:
  - added public repository helper `cancel_claimed_job_before_processing(...)` in `trr_backend/repositories/social_season_analytics.py`
  - helper reuses repository cancellation semantics and supports both job-level and run-level cancellation before execution starts
  - `scripts/socials/worker.py` now calls that helper and fails open on probe errors, logging the probe failure instead of crashing the worker loop
  - `get_run_progress_snapshot(...)` now counts only `running` rows toward `active_workers_now`
- Validation:
  - targeted `py_compile` passed
  - targeted pytest for worker-cancel and run-progress paths passed
  - `ruff check scripts/socials/worker.py tests/scripts/test_social_worker.py` passed
- Known lint constraint:
  - full `ruff check trr_backend/repositories/social_season_analytics.py` is still blocked by large pre-existing baseline issues outside this follow-up

## March 5 Late-Night Follow-Up — Social stuck-job root cause and hotfix rollout

- Investigated six live `running_stale_heartbeat` social jobs on the fresh AWS season restart.
- Root cause:
  - `_claim_next_jobs(...)` marked rows `running` and set the initial claim heartbeat.
  - `_execute_claimed_job(...)` still performed context/config/runtime preflight before entering its `try/except`.
  - If that preflight crashed before `_touch_job_heartbeat(...)`, the worker loop survived and kept processing later jobs, but the original job row never reached `_finish_job(...)`.
- Evidence:
  - leaked rows had `started_at == claimed_at == heartbeat_at` with no later heartbeat movement and no `error_message`
  - the same `worker_id` values were already attached to newer jobs in `social.scrape_workers`
  - manual `recover_stale_running_jobs(stage='posts', limit=20)` recovered all six immediately
- Fix:
  - moved preflight setup inside `_execute_claimed_job(...)`'s guarded failure path
  - added regression `test_execute_claimed_job_finalizes_preflight_crashes`
- Validation:
  - targeted pytest and py_compile passed locally
  - queue status after recovery and rollout: `stuck_jobs_total=0`, `stale_claims.total=0`
- Live rollout:
  - payload: `s3://trr-backend/artifacts/trr-backend/20260306-042032/trr_backend_stuck_jobs_fix.tar.gz`
  - SSM rollout command: `37f10d90-faf9-402e-9d40-1c188f08c062`
  - restarted `trr-api` on the API host and `trr-social-worker-pool.service` on all worker hosts

## March 5 Late Refresh — Social worker plane reset and AWS-only restart validation

- Investigated mixed worker visibility in admin health and confirmed the `thomass-MacBook-Pro.local` rows were live local workers, not only historical heartbeats.
- Local cleanup:
  - terminated detached launcher shell `3411`
  - found and terminated orphaned local worker children that had been re-parented to PID `1`
  - removed all remaining `.local` rows from `social.scrape_workers`
- Queue reset:
  - cancelled all active social ingest jobs; active-state query now returns only `cancelled`
  - restarted `trr-social-worker-pool.service` on all three worker instances in `trr-worker-asg`
  - purged stale worker heartbeat rows so the fresh worker list is AWS-only again
- Replacement run:
  - queued fresh RHOSLC S6 full-season/all-platform orchestration with workflow id `reset-rhoslc-s6-season-all-20260306-033558`
  - settings match the lightweight dry-run path:
    - `posts_only`
    - `max_posts_per_target=1`
    - no comments/replies
    - `resume_existing=false`
  - validated that current `working` workers are all EC2-hosted and attached to the new workflow id
- Observed progress during handoff snapshot:
  - child runs: `28 completed`, `9 running`, `37 queued`
  - child jobs: `63 completed`, `15 running`, `46 queued`

## March 4 Refresh — Remaining Phase 1 Coverage Completed

- Completed operation-backed migration for remaining targeted stream routes:
  - `POST /api/v1/admin/shows/{show_id}/refresh-photos/stream`
  - `POST /api/v1/admin/person/{person_id}/refresh-images/stream`
  - `POST /api/v1/admin/person/{person_id}/reprocess-images/stream`
- Added remote producer mappings and builders so remote worker mode can execute all migrated operation types without API-local ownership.
- Contract remains additive-only; no path/verb removals.

### Supported `operation_type` matrix (remote dispatcher)

| Operation type | Producer builder | Status |
|---|---|---|
| `admin_asset_batch_jobs` | `build_batch_jobs_operation_producer` | Ready |
| `admin_scrape_import_images` | `build_scrape_import_operation_producer` | Ready |
| `admin_show_links_discover` | `build_show_links_discovery_operation_producer` | Ready |
| `admin_show_bravo_preview` | `build_bravo_preview_operation_producer` | Ready |
| `admin_show_refresh` | `build_show_refresh_operation_producer` | Ready |
| `admin_show_refresh_photos` | `build_show_refresh_photos_operation_producer` | Ready |
| `admin_person_refresh_images` | `build_person_refresh_images_operation_producer` | Ready |
| `admin_person_reprocess_images` | `build_person_reprocess_images_operation_producer` | Ready |

### Validation refresh (March 4)

- Pass:
  - `pytest -q tests/api/routers/test_admin_show_sync.py` (`24 passed`)
  - `pytest -q tests/api/routers/test_admin_show_links.py` (`104 passed`)
  - `pytest -q tests/api/routers/test_admin_show_bravo.py` (`37 passed`)
  - `pytest -q tests/api/routers/test_admin_show_sync.py tests/api/routers/test_admin_show_links.py tests/api/routers/test_admin_show_bravo.py tests/api/routers/test_admin_operations.py tests/repositories/test_admin_operations.py tests/api/routers/test_admin_scrape_contracts.py tests/api/routers/test_admin_asset_batch_jobs.py tests/api/routers/test_admin_show_news.py tests/api/routers/test_socials_reddit_refresh_routes.py tests/api/routers/test_admin_person_images.py::test_refresh_stream_resizing_heartbeat_includes_operation_progress tests/api/routers/test_admin_person_images.py::test_reprocess_stream_emits_terminal_error_for_unhandled_exception tests/api/routers/test_admin_person_images.py::test_refresh_stream_emits_terminal_error_for_unhandled_exception` (`233 passed`)
  - `pytest -q tests/api/routers/test_admin_operations.py tests/repositories/test_admin_operations.py tests/api/routers/test_admin_show_bravo.py tests/api/routers/test_admin_scrape_contracts.py tests/api/routers/test_admin_asset_batch_jobs.py tests/api/routers/test_admin_show_news.py tests/api/routers/test_socials_reddit_refresh_routes.py` (`102 passed`)
- Known baseline blocker (unrelated to operation-stream work in this pass):
  - `pytest -q tests/api/routers/test_admin_person_images.py` fails on 3 pre-existing metadata enrichment assertions:
    - `test_enrich_cast_photos_with_episode_metadata_falls_back_to_imdb_title_metadata`
    - `test_enrich_cast_photos_with_episode_metadata_marks_unresolved_imdb_episode_show_as_null`
    - `test_enrich_cast_photos_with_episode_metadata_uses_wwhl_credit_episode_ids_when_fallback_missing`


## Plan A Contract Freeze Checkpoint

### Freeze criteria

| Criterion | Status | Evidence |
|---|---|---|
| Operation API tests pass (`GET status`, `GET stream`, `POST cancel`) | Pass | `pytest -q tests/api/routers/test_admin_operations.py` |
| Operation repository lifecycle/replay tests pass | Pass | `pytest -q tests/repositories/test_admin_operations.py` |
| Earliest `operation` envelope replay assertion present and passing | Pass | `test_start_operation_emits_operation_envelope_as_first_replayed_event` |
| Remote mode does not execute reddit/google async in API process | Pass | `test_start_reddit_refresh_run_remote_mode_does_not_start_in_api`, `test_google_news_sync_async_remote_mode_does_not_start_in_api` |
| Worker-claim schema fields added for admin/google-news/reddit job tables | Pass | migration `0173_remote_job_plane_claims.sql` |
| Migrated SSE routes assert additive `operation_id` + monotonic `event_seq` | Pass (targeted) | Updated route tests + targeted run command (see Validation section) |
| `POST /api/v1/admin/scrape/import/stream` uses operation contract path | Pass | Route now uses `start_operation_for_stream` + `operation_stream_response`; contract test passing |
| Structured guardrail logs (create vs attach, replay usage, cancel audit) | Pass | `trr_backend/pipeline/admin_operations.py`, `api/routers/admin_operations.py` |
| Workspace default mode locked to no-reload reuse | Pass | `WORKSPACE_BROWSER_TAB_SYNC_MODE=reuse_no_reload` surfaced in `scripts/status-workspace.sh` |

### Freeze gate decision

- **Contract freeze for Plan B consumers: Ready with caveat on broad baseline test noise.**
- Additive contracts are stable and validated with targeted suites required for operation/replay semantics.
- Broad repo lint/full-suite failures remain pre-existing baseline noise outside Task 11 scope and are documented below.

## Plan B Integration Addendum (Additive)

- Added `GET /api/v1/admin/socials/reddit/runs` to support explicit manual-attach run selection from TRR-APP reddit window/details pages.
- Endpoint is additive and filterable (`community_id`, `season_id`, `period_key`, `status`, `limit`).
- No breaking changes to existing `/reddit/runs` start or `/reddit/runs/{run_id}` status APIs.

## Validation

### Passing targeted commands

- `ruff check` on changed Plan A files:
  - `tests/api/routers/conftest.py`
  - `tests/api/routers/test_admin_operations.py`
  - `tests/repositories/test_admin_operations.py`
  - `tests/api/routers/test_admin_show_sync.py`
  - `tests/api/routers/test_admin_show_links.py`
  - `tests/api/routers/test_admin_show_bravo.py`
  - `tests/api/routers/test_admin_person_images.py`
  - `tests/api/routers/test_admin_scrape_contracts.py`
  - `tests/api/routers/test_admin_asset_batch_jobs.py`
  - `trr_backend/pipeline/admin_operations.py`
  - `api/routers/admin_operations.py`
- `ruff format --check` on same changed Plan A files (pass)
- `pytest -q tests/api/routers/test_admin_operations.py tests/repositories/test_admin_operations.py tests/api/routers/test_admin_scrape_contracts.py` (pass; `15 passed`)
- `pytest -q tests/api/routers/test_admin_show_sync.py::TestRefreshShow::test_refresh_stream_emits_complete_event tests/api/routers/test_admin_show_links.py::test_discover_show_links_stream_emits_progress_events_before_complete tests/api/routers/test_admin_show_bravo.py::test_preview_bravo_import_stream_emits_start_progress_and_complete tests/api/routers/test_admin_person_images.py::test_refresh_stream_resizing_heartbeat_includes_operation_progress tests/api/routers/test_admin_asset_batch_jobs.py::TestAssetBatchJobsStream::test_skips_unsupported_origin` (pass; `5 passed`)
- `pytest -q tests/api/routers/test_socials_reddit_refresh_routes.py` (pass; `19 passed`)
- `pytest tests/api/routers/test_admin_operations.py tests/api/routers/test_socials_reddit_refresh_routes.py tests/api/routers/test_admin_show_news.py tests/api/routers/test_admin_asset_batch_jobs.py tests/repositories/test_admin_operations.py` (pass; `60 passed`)
- `pytest -q tests/api/routers/test_admin_scrape_contracts.py::test_import_images_stream_includes_operation_contract_fields tests/api/routers/test_admin_show_bravo.py::test_preview_bravo_import_stream_emits_start_progress_and_complete tests/api/routers/test_admin_person_images.py::test_refresh_stream_resizing_heartbeat_includes_operation_progress` (pass; `3 passed`)
- Workspace mode check: `WORKSPACE_BROWSER_TAB_SYNC_MODE=reuse_no_reload bash ./scripts/status-workspace.sh` prints `WORKSPACE_BROWSER_TAB_SYNC_MODE: reuse_no_reload`

### Baseline blockers (unrelated to Task 11 contract logic)

- `ruff check .` fails on pre-existing unrelated files (social analytics, scripts, unrelated tests).
- `ruff format --check .` reports broad pre-existing formatting drift across many unrelated files.
- Full combined router file run from spec (`256` tests) was long-running in local environment and did not complete within practical session bounds; targeted updated-route contract tests pass.

## Phase 2 AWS runtime status (external/manual)

- Not executed from this code session (requires AWS console/infra access and deploy permissions).
- Pending manual completion items remain:
  - worker launch template + worker ASG rollout
  - worker systemd service enablement validation on instances
  - SG split hardening (`trr-alb-sg` vs `trr-api-sg`)
  - queue-depth/failure alarms and staging canary evidence capture

## Accepted Stream Payload Envelopes (Frozen)

### `operation`

```json
{
  "operation_id": "<uuid>",
  "status": "pending|running|...",
  "attached": false,
  "execution_owner": "local_api|remote_worker",
  "execution_mode_canonical": "local|remote",
  "request_id": "<optional>",
  "event_seq": 1
}
```

### `progress`

```json
{
  "operation_id": "<uuid>",
  "event_seq": 2,
  "stage": "...",
  "message": "..."
}
```

### `complete`

```json
{
  "operation_id": "<uuid>",
  "event_seq": 3,
  "stage": "complete",
  "...existing_route_fields": "preserved"
}
```

### `error`

```json
{
  "operation_id": "<uuid>",
  "event_seq": 4,
  "stage": "operation|stream|...",
  "error": "...",
  "detail": "..."
}
```

## Policy lock

- Auth remains shared across tabs.
- Workflow/run state is tab-scoped only (`client_session_id` + `client_workflow_id` semantics).
- All API changes remain additive; no endpoint removals.

## March 5, 2026 — Phase 7/8/9 Validation Pack Execution (Agent A)

Evidence root:
- `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/ai/evidence/aws-worker-plane/20260304-191411`

### Phase 7 result: Completed

- Added and executed backend ops validation pack under `/Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/ops/aws_worker_plane/`.
- Context discovery, SSM worker unit checks, API remote env checks, CloudWatch log discovery checks, alarm inventory/planning/apply all executed.
- Final 7-alarm target is satisfied:
  - `trr-api-target-5xx`
  - `trr-worker-zero-inservice`
  - `trr-worker-status-check-failed`
  - `trr-queue-depth-high`
  - `trr-stale-leases-high`
  - `trr-long-job-failures-high`
  - `trr-worker-service-failure-signal`
- Alarm/log evidence artifacts:
  - `alarm_inventory.json`, `alarm_post_apply_inventory.json`, `missing_alarms.json`
  - `cloudwatch_log_presence.json`, `cloudwatch_links.md`

### Phase 8 result: Partial (3 blocked, 1 completed)

Completed:
- Screenalytics outage simulation + rollback script completed:
  - `scenario_screenalytics_outage.log`
  - `ssm_outputs/screenalytics_outage_apply.json`
  - `ssm_outputs/screenalytics_restore.json`

Blocked scenarios:
- API recycle scenario blocked.
- Worker recycle scenario blocked.
- SSE replay scenario blocked.

Blocker details:
- Both public ALB kickoff and API-instance-local (SSM) kickoff failed to emit operation stream payloads with `operation_id`.
- Public kickoff symptom: HTTP 504 in transcripts (example `scenario_api_recycle_stream.txt`).
- SSM local kickoff symptom: curl timed out after 180s with zero bytes (examples in `ssm_outputs/*kickoff_fallback.json`).
- Because no `operation_id` or `event_seq` could be captured, replay/resume assertions are blocked.

### Phase 9 result: Completed (docs append-only)

- This status section appended without replacing existing infra narrative.
- HANDOFF updated with runtime/alarm/resilience matrices and blocker evidence pointers.
- TRR-APP Task11 status updated with backend readiness/checkpoint note and blocker caveat.

### Follow-up unblock required before canary sign-off

1. Investigate why kickoff stream endpoints are not emitting initial operation envelope in staging runtime (ALB and local API host).
2. Re-run:
   - `30_resilience_api_recycle.sh`
   - `31_resilience_worker_recycle.sh`
   - `32_resilience_sse_replay.sh`
3. Attach successful `operation_ids.txt`, `request_ids.txt`, and replay transcript evidence before production canary gate.

## March 5, 2026 — Task 11 Unblock Plan Execution (Phase 0-5)

Evidence root:
- `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/ai/evidence/aws-worker-plane/20260304-195705-task11-unblock`

### Root cause statement used for this run

- Staging `DATABASE_URL` pointed to Supabase pooler (`aws-1-us-east-1.pooler.supabase.com:6543`), while API/worker SG egress previously lacked `tcp/6543`.

### Phase 0 baseline capture (complete before mutation)

- Captured baseline context, ASG/instance IDs, SG rules, alarm state, and bootstrap/log inventory in `baseline/`.
- Captured `/trr/staging/DATABASE_URL` and confirmed `/trr/staging/SUPABASE_DB_URL` was missing pre-change.

### Phase 1 runtime/network correction (applied)

- Added SG egress rules:
  - `trr-api-sg (sg-09ad087d9a6b689dd)`: `tcp/6543 -> 0.0.0.0/0` (`Supabase pooler Postgres`)
  - `trr-worker-sg (sg-09b039c604dd9d077)`: `tcp/6543 -> 0.0.0.0/0` (`Supabase pooler Postgres`)
- Preserved pre-existing `443/6379/5432` egress rules in this pass.
- Added/overwrote SSM param:
  - `/trr/staging/SUPABASE_DB_URL` = same value as `/trr/staging/DATABASE_URL`
- DB reachability gate outcome:
  - Connectivity to pooler host is confirmed from API instance.
  - Query gate `select 1 from core.admin_operations` fails because relation is absent in runtime DB (`phase4_db_table_presence_check.txt` shows both `core.admin_operations` and `core.admin_operation_events` absent).

### Phase 2 runtime refresh + smoke

- Recycled API ASG instance:
  - before: `i-05d7f68c379dc05db`
  - after: `i-09b52a7ad5f88c0a1`
- `/health` recovered successfully after replacement.
- Smoke kickoff still failed to emit envelope:
  - stream response: `Internal Server Error`
  - no `operation_id`, no `event_seq`

### Phase 3 required reruns (`30/31/32`)

- Executed reruns against the same evidence root.
- Final rerun exits (`phase3_rerun_t180_summary.txt`):
  - `30_resilience_api_recycle.sh`: `1`
  - `31_resilience_worker_recycle.sh`: `1`
  - `32_resilience_sse_replay.sh`: `1`
- Blocking signature after SG fix is unchanged:
  - public ALB kickoff: no operation envelope
  - SSM local fallback kickoff: `Internal Server Error`
  - fallback outputs:
    - `ssm_outputs/api_recycle_kickoff_fallback.json`
    - `ssm_outputs/worker_recycle_kickoff_fallback.json`
    - `ssm_outputs/sse_replay_kickoff_fallback.json`

### Phase 4 evidence integrity status

- Present/non-empty:
  - `request_ids.txt`
  - `scenario_api_recycle_stream.txt`
  - `scenario_worker_recycle_stream.txt`
  - `scenario_sse_initial_stream.txt`
  - `cloudwatch_links.md`
- Missing or empty due blocker:
  - `operation_ids.txt` (missing)
  - `scenario_sse_replay_stream.txt` (empty)
- Alarm sanity:
  - required 7 alarms present exactly once, no duplicates (`phase4_validation_summary.json`).

### Canary gate status

- **Not satisfied.**
- SG/runtime mismatch was corrected, but operation envelope emission remains blocked in staging and Phase 8 resilience proofs cannot be completed until operation tables/runtime path are restored.

## March 5, 2026 — DB parity fix + rerun continuation

Evidence root (same as prior unblock run):
- `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/ai/evidence/aws-worker-plane/20260304-195705-task11-unblock`

### DB runtime parity fix (applied)

- Applied missing staging migrations on the API DB endpoint:
  - `0172_admin_operations_and_events.sql`
  - `0173_remote_job_plane_claims.sql`
- Recorded migration history rows in `supabase_migrations.schema_migrations` for versions `0172` and `0173`.
- Verified table parity:
  - `core.admin_operations` exists
  - `core.admin_operation_events` exists
- Verification artifacts:
  - `phase_fix_check_admin_operations.out`
  - `phase_fix_check_admin_operation_events.out`
  - `phase_fix_check_migration_rows.out`
  - `ssm_outputs/phase_fix_db_runtime_gate_v2.json`

### Rerun results (`30/31/32` only)

- `30_resilience_api_recycle.sh`:
  - kickoff now emits envelope and captures operation ID (`0ee165ab-f5e4-4d84-b92c-1f4c85849937`)
  - scenario still exits `1` because operation remained `pending` until timeout
- `31_resilience_worker_recycle.sh` (postfix rerun):
  - kickoff now emits envelope and captures operation ID (`5f6154c1-0801-4283-9570-fea1591be91d`)
  - scenario still exits `1` because operation remained `pending` until timeout
- `32_resilience_sse_replay.sh` (postfix rerun):
  - kickoff emits envelope and captures operation ID (`a08be9d5-8962-4973-a35e-18c2f7d3514f`)
  - scenario exits `1` because replay request used malformed `after_seq` (`"event_seq": 1`) and replay stream had no `event_seq`
- Exit summary:
  - `phase_postfix2_rerun_summary.txt` (`run30_postfix_exit=1`, `run31_postfix2_exit=1`, `run32_postfix2_exit=1`)

### Evidence integrity after parity fix

- Now non-empty:
  - `operation_ids.txt`
  - `request_ids.txt`
  - `scenario_sse_initial_stream.txt`
- Still incomplete:
  - `scenario_sse_replay_stream.txt` remains empty
- DB/API snapshots for captured operation IDs:
  - `db_snapshots/postfix_admin_operations_snapshot.txt`
  - `db_snapshots/postfix_admin_operation_events_snapshot.txt`
  - `postfix_api_operation_statuses.txt`

### Canary gate status (current)

- **Still not satisfied.**
- DB parity restoration removed the original missing-table blocker and restored envelope emission, but:
  - operations remain stuck in `pending` (latest_event_seq remains `1`),
  - SSE replay scenario remains red in current script run.

## March 5, 2026 — Script fix pass (`32` replay seq extraction) + rerun

Evidence root (same):
- `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/ai/evidence/aws-worker-plane/20260304-195705-task11-unblock`

Change applied:
- Patched `scripts/ops/aws_worker_plane/32_resilience_sse_replay.sh` to parse numeric `event_seq` using POSIX/BSD-safe sed spacing (`[[:space:]]`) instead of `\s` pattern in sed replacement.
- This removed malformed replay URL generation seen previously (`after_seq="event_seq": 1`).

Rerun executed:
- `32_resilience_sse_replay.sh` only (same evidence root)
- Result: `run32_postfix3_exit=1` (`phase_postfix3_rerun32_summary.txt`)

Observed behavior after script fix:
- Kickoff stream now parses correctly:
  - `after_seq=1`
  - `operation_id=8de0dc8a-def6-4078-8601-5bd1ea9dd1b6`
- Replay stream still empty:
  - `scenario_sse_replay_stream.txt` remains `0` bytes
  - log shows transport/runtime failure (`HTTP/2 INTERNAL_ERROR`) and no replay `event_seq`
- This confirms remaining replay failure is no longer caused by the prior `after_seq` parsing bug.

## March 5, 2026 — AWS worker plane verification + social season dry-run matrix

### Live worker-plane verification result: Completed

- Direct AWS verification is no longer IAM-blocked.
- Confirmed remote worker plane inventory:
  - API instance: `i-01a7b672f5946d19a`
  - worker instance: `i-02e0d952e67e302a0`
- Health observations:
  - API target group health: healthy
  - worker instance health: healthy
  - worker SSM status: online
  - CloudWatch alarms/log groups readable; worker-plane alarms observed `OK`
- Runtime contract on the API host:
  - `TRR_JOB_PLANE_MODE=remote`
  - `TRR_LONG_JOB_ENFORCE_REMOTE=1`
- Remote worker services observed active:
  - `trr-admin-operations-worker.service`
  - `trr-reddit-refresh-worker.service`
  - `trr-google-news-worker.service`
  - `trr-social-worker-pool.service`

### Running-instance rollout result: Completed with bootstrap drift caveat

- Current backend runtime fixes were pushed to the running API/worker hosts via S3 payload + SSM rollout:
  - payload: `s3://trr-backend/artifacts/trr-backend/20260305-190623/trr_backend_rollout_payload.tar.gz`
- API and worker services restarted cleanly after rollout.
- On-host verification confirmed the runtime now includes:
  - season orchestration code in `api/routers/socials.py`
  - remote-only Google News async enforcement + corrected image payload behavior in `api/routers/admin_show_news.py`
  - updated orchestration metadata in `trr_backend/repositories/social_season_analytics.py`
  - social worker child-process isolation/heartbeat fixes in `scripts/socials/worker.py`
- Remaining infra caveat:
  - This caveat is now closed.
  - SSM bootstrap parameter `/trr/staging/TRR_BACKEND_ARTIFACT_S3_URI` now points at canonical full artifact `s3://trr-backend/artifacts/trr-backend/20260305-202656/trr-backend.tar.gz`
  - Future ASG replacement/scale-out should now boot the refreshed runtime tree instead of the prior March 4 artifact.

## March 6 Late-Night Follow-Up — YouTube crawl cap + media re-mirror skip deployed

- Root cause addressed:
  - bounded YouTube date-window crawls were still too permissive for incremental runs with `max_posts_per_target=0`
  - post refresh and direct media mirror could still touch already-hosted YouTube media
- Local code changes:
  - `trr_backend/socials/youtube/scraper.py`
  - `trr_backend/repositories/social_season_analytics.py`
- Rollout artifact:
  - `s3://trr-backend/artifacts/trr-backend/20260306-235206/trr_backend_youtube_sync_fix.tar.gz`
- SSM rollout commands:
  - API: `dd607685-954c-439b-9e38-fcf2110278e6`
  - workers: `520d9c51-6569-449f-8223-fa53d4983562`
  - verification: `e78e28d0-a67b-4f07-a554-6b489ccc5dd9`
- Applied to:
  - API: `i-01a7b672f5946d19a`
  - workers: `i-02e0d952e67e302a0`, `i-048f605583d2a11dd`, `i-0c4934e80de1dd89d`
- Service restarts:
  - `trr-api`
  - `trr-social-worker-pool.service`
- Remote verification:
  - all four hosts now contain `INITIAL_DATE_WINDOW_NO_HIT_PAGE_CAP = 8` in `youtube/scraper.py`
  - all four hosts now contain `incremental_existing_window_page_cap` logic in `social_season_analytics.py`
- Runtime note:
  - the in-flight YouTube run `e015bcd3-20c1-45ed-bda7-b9c955047b30` ended `cancelled` during the rollout window, so a fresh rerun is required to measure the improved crawl behavior remotely.

### Reddit OAuth status: Configured on live hosts

- Installed Reddit OAuth app credentials on:
  - `/etc/trr-api.env`
  - `/etc/trr-worker.env`
- Restarted:
  - `trr-api`
  - `trr-reddit-refresh-worker.service`
- Verified the worker host can exchange the configured app credentials for a bearer token:
  - Reddit token endpoint returned HTTP `200`
  - `access_token` present in response
- Result:
  - remote Reddit refresh jobs are no longer forced to operate in anonymous mode
  - the earlier `REDDIT_CLIENT_ID not set` condition is resolved for the live worker host

### Live social season dry-run matrix: Executed against remote plane

Season used:
- RHOSLC season `e9161955-6ee4-4985-865e-3386a0f670fb`

Workflows launched:
- one week / one platform:
  - `dryrun-rhoslc-s6-week1-instagram-20260305`
- one week / all platforms:
  - `dryrun-rhoslc-s6-week1-all-20260305`
- full season / one platform:
  - `dryrun-rhoslc-s6-season-instagram-20260305`
- full season / all platforms:
  - `dryrun-rhoslc-s6-season-all-20260305`

Dry-run settings:
- `ingest_mode=posts_only`
- `max_posts_per_target=1`
- `max_comments_per_post=0`
- `max_replies_per_post=0`
- `fetch_replies=false`
- `resume_existing=true`

Observed behavior:
- Week 1 / Instagram: drained immediately and entered active processing.
- Week 1 / all platforms: multiple child runs moved from queued to running during the observation window.
- Full season / Instagram: fanout progressed and accumulated active/completed jobs, but remained backlog-heavy.
- Full season / all platforms: `144` child runs were created and remained queued during the observation window.
- Queue status reported:
  - remote plane contract intact
  - stale claims `0`
  - large pre-existing `posts` backlog
  - active worker processing on Instagram pages

Interpretation:
- The worker plane is healthy and consuming work.
- The dominant limiter for the largest season-wide fanout is queue depth / worker capacity, not stuck claims or dead workers.
- Capacity tuning has now been applied:
  - `trr-worker-asg` has target-tracking CPU scaling with scale-in enabled, so `DesiredCapacity` alone was immediately pulled back to `1`
  - pinned `MinSize=3` and `DesiredCapacity=3`
  - verified three `c7i.xlarge` workers in service:
    - `i-02e0d952e67e302a0`
    - `i-048f605583d2a11dd`
    - `i-0c4934e80de1dd89d`
- Remaining Reddit-specific blocker is no longer credentials on the live hosts. Any further Reddit throughput limits should now be treated as application- or platform-level rate-limit behavior rather than missing OAuth configuration.

Update 2026-03-06 00:55 ET:
- Root cause for the recurring “cancelled but still working” social runs was confirmed: already-claimed jobs could continue into full scrape execution after the run had been cancelled.
- Backend repo fix landed locally:
  - `social_season_analytics._execute_claimed_job(...)` now aborts cancelled jobs/runs before stage execution.
  - `scripts/socials/worker.py` now discards already-claimed cancelled runs before handing them to `process_claimed_job(...)`.
- Live worker-plane mitigation was applied immediately:
  - patched `worker.py` onto the three worker hosts via S3 + SSM
  - hard-reset `trr-social-worker-pool.service`
  - purged stale worker heartbeats in Supabase
- Old RHOSLC preseason Twitter run `02df9a2c-9458-4163-9c2f-920bb1fda706` was cancelled.
- Fresh replacement remote-worker run created:
  - run id: `be9e2637-c738-46bb-8b21-683c794358fe`
  - scope: Twitter preseason only
  - shape: `4` jobs total
  - observed state after start: `3` completed, `1` running, `0` stuck claims
- Important environment note:
  - no local database migration was needed during this pass; local backend/dev execution was already pointed at Supabase via `SUPABASE_DB_URL`.

Update 2026-03-07 19:30 ET:
- Covered admin image-analysis routes are now off the Screenalytics HTTP execution path in code:
  - `trr_backend.clients.screenalytics.count_people(...)` and `count_people_batch(...)` dispatch into the backend-owned vision runtime instead of requiring `SCREENALYTICS_API_URL` for those admin flows
  - dedicated Modal vision entrypoint added: `run_admin_vision`
  - readiness/cutover tooling now expects the vision function as part of the Modal app contract
- Verified locally:
  - targeted backend pytest passed for the adapter, readiness script, and covered admin image-count/person-image routes
  - `ruff check` passed on the touched vision/runtime files
- What remains before TASK11 can be called complete:
  - deploy the updated Modal app with `run_admin_vision`
  - roll out host env so `TRR_ADMIN_IMAGE_EXECUTION_BACKEND=modal` and `TRR_MODAL_VISION_FUNCTION=run_admin_vision` are active in the live happy path
  - decommission the EC2 worker ASG so it cannot scale back up outside rollback
  - complete the social job cutover so the admin docs no longer truthfully show `EC2` as the current runtime for social flows

Update 2026-03-07 20:15 ET:
- Full Modal backend hosting prep has now been executed for the TRR backend itself:
  - `serve_backend_api` deployed inside `trr-backend-jobs`
  - named Modal secrets `trr-backend-runtime` and `trr-social-auth` refreshed
  - readiness verifier now resolves the API web endpoint in addition to the job functions
- Live Modal API endpoint:
  - `https://admin-56995--trr-backend-api.modal.run`
- Direct API verification against the deployed Modal URL now reports the canonical backend contract:
  - `execution_mode_canonical=remote`
  - `execution_owner=remote_worker`
  - `execution_backend_canonical=modal`
  - `remote_job_plane_enforced=true`
- This removes the remaining backend-runtime mismatch where the hosted API previously still advertised `local_api`.
- Current scope/result:
  - backend code path: cut over to Modal-hostable API + Modal job plane
  - local admin UI: verified against the Modal backend URL
  - staging/production frontend runtime base URL cutover: not executed in this pass
  - EC2 retirement: still pending environment-level cutover and rollback-window planning

Update 2026-03-07 20:30 ET:
- Follow-up backend validation debt from the Modal API cutover is resolved.
- The remaining failing pytest cases were test-environment assumptions, not product/runtime defects:
  - ingest-planning tests now stub immediate post-create dispatch
  - legacy worker-health tests explicitly select the non-Modal branch
  - reddit route tests now pin local vs legacy-worker vs modal env contracts
- Current backend validation state:
  - targeted Modal/social/admin/reddit pytest slice: `498 passed`
  - backend Ruff: clean
- TASK11 remains blocked only on environment-level cutover and retirement work:
  - point deployed app traffic at the Modal backend URL
  - complete smoke validation on the deployed app/backend pair
  - retire the legacy AWS backend runtime after the rollback window
