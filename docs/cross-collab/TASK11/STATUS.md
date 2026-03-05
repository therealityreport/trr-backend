# Status — Task 11 (Tab-Isolated Admin Operations + Resumable Streams)

Repo: TRR-Backend
Last updated: March 5, 2026

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
