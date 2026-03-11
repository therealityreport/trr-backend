# Other Projects — Task 11 (Plan A Contract Freeze)

Repo: TRR-Backend
Last updated: March 4, 2026

## March 7, 2026 admin-vision rollout follow-up

- TRR-Backend staging now covers the remaining admin image-analysis cutover item:
  - `run_admin_vision` is deployed in Modal
  - staging API host env now points admin image execution at Modal
- TRR-APP docs should treat the covered image-analysis and social entries as `Modal` current runtime after this rollout.
- screenalytics remains outside the execution path for documented TRR admin jobs in staging, but may still be retained for non-admin surfaces outside this task scope.

## March 7, 2026 rollout checkpoint

- Staging backend runtime is now live on the canonical `remote + modal` contract.
- Modal named secrets and host-side Modal credentials were provisioned during the staging rollout.
- Social/admin/google-news/reddit dispatchers now surface as Modal-backed executor rows on staging worker-health.
- Production remains unaddressable in the current AWS account context:
  - no `/trr/production/*` SSM namespace
  - no separate production ASG visible
- TRR-APP remains contract-compatible; no additional consumer wire-shape changes were required for this live backend cutover.

## Cross-Repo Snapshot

- `TRR-Backend`: Plan A contract surface implemented and freeze checkpoint published; additive reddit run-list endpoint shipped for Plan B manual-attach parity.
- `TRR-APP`: Consumer alignment required to use frozen operation APIs and replay envelopes.
- `screenalytics`: No contract/code changes required for Plan A.

## Consumer Contract for TRR-APP

TRR-APP should consume these frozen guarantees:

1. Operation APIs available at:
   - `GET /api/v1/admin/operations/{operation_id}`
   - `GET /api/v1/admin/operations/{operation_id}/stream?after_seq={n}`
   - `POST /api/v1/admin/operations/{operation_id}/cancel`
2. Existing SSE routes remain, now with additive `operation_id` + `event_seq` fields.
3. Kickoff metadata accepted additively: `client_session_id`, `client_workflow_id`.
4. Header metadata accepted additively: `x-trr-tab-session-id`, `x-trr-flow-key`.
5. Auth is still shared across tabs; only workflow/run state is tab-scoped.
6. Additive reddit run-list API available for manual attach selectors:
   - `GET /api/v1/admin/socials/reddit/runs`
7. Additive kickoff ownership fields available on async/long-job starts:
   - `execution_owner` (`local_api|remote_worker`)
   - `execution_mode_canonical` (`local|remote`)
8. Worker plane mode toggles are backend-runtime only and do not change auth:
   - `TRR_JOB_PLANE_MODE`
   - `TRR_LONG_JOB_ENFORCE_REMOTE`

## Plan B Start Condition

Plan B UI migration is unblocked once TRR-APP acknowledges this checkpoint in `TRR-APP/docs/cross-collab/TASK11/STATUS.md` and uses the payload envelopes documented in backend `STATUS.md`.

March 4 note:
- Backend remote operation dispatcher matrix now includes:
  - `admin_show_refresh_photos`
  - `admin_person_refresh_images`
  - `admin_person_reprocess_images`
- Plan B consumers can assume replay contract support for those stream families via:
  - `GET /api/v1/admin/operations/{operation_id}`
  - `GET /api/v1/admin/operations/{operation_id}/stream?after_seq={n}`

## Workspace Coordination

Workspace startup scripts are locked to no-reload tab reuse by default (`reuse_no_reload`) with explicit legacy fallback (`reload_all`).

## March 5, 2026 cross-repo checkpoint

- Unblock execution evidence root:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/ai/evidence/aws-worker-plane/20260304-195705-task11-unblock`
- Backend staging infra/runtime corrections were applied (`tcp/6543` egress on API/worker SG + explicit `/trr/staging/SUPABASE_DB_URL`).
- Required resilience reruns (`30/31/32`) were executed and still blocked due missing kickoff envelope (`operation_id`/`event_seq`).
- TRR-APP should keep additive contract consumption unchanged and treat canary replay confidence as pending until backend evidence set is completed.

March 5 continuation:
- Backend DB parity is now restored on staging runtime DB endpoint (`0172`/`0173` applied; operation tables present).
- `30/31/32` reruns now capture operation envelopes and populate `operation_ids.txt`, but canary is still blocked by operations stuck in `pending` and replay transcript remaining empty.
