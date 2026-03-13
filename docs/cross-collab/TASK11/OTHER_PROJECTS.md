# Other Projects — Task 11 (Plan A Contract Freeze)

Repo: TRR-Backend
Last updated: March 13, 2026

## March 13, 2026 final verification is complete

- `TRR-Backend` is fully closed for Task 11:
  - Render is the live public backend
  - Modal is the live async plane
  - R2 is the live object-storage backend
  - Better Stack ingestion is verified green in live tail
  - AWS migration-scope resources are gone
- `TRR-APP` does not need any additional consumer follow-up for this task.
- `screenalytics` is aligned to the shared R2 contract and no longer has any Task 11 dependency on AWS storage.

## March 13, 2026 final closeout reconciliation

- `TRR-Backend` is fully cut over:
  - public traffic is on Render
  - async execution is on Modal
  - runtime object storage is on R2
  - Better Stack ingestion is live
  - the legacy AWS API/network shell and migration-scope observability/storage tail are deleted
- `TRR-APP` does not need additional consumer contract changes for Task 11 closeout.
- `screenalytics` is aligned to the shared object-storage contract and no longer blocks Task 11.
- Remaining non-task artifacts are intentionally retained reference items only:
  - staging SSM parameters
  - ACM certificate `api.thereality.report`

## March 13, 2026 final exit pass now leaves only observability cleanup plus two draining S3 buckets

- `TRR-Backend` completed the live shell retirement and storage cutover steps:
  - Render + Modal now use the R2 object-storage contract live
  - the public media host is temporarily the managed Cloudflare `r2.dev` domain because `media.thereality.report` is not yet attached to the bucket
  - the AWS API/network shell and manual RDS snapshot are deleted
- `screenalytics` is now operationally aligned to the same object-storage contract:
  - local runtime `.env` points at `screenalytics-artifacts-prod`
  - repo startup defaults no longer imply `AWS_S3_BUCKET=screenalytics`
- `TRR-APP` still does not need consumer route-shape changes for this finish pass.
- The remaining cross-project blocker is Better Stack, not storage or hosting:
  - existing Better Stack source token returns `401`
  - CloudWatch removal remains deferred until Better Stack ingestion is verified
- Remaining AWS storage cleanup is operational only:
  - `trr-backend` and `ltsr-data-bucket` are gone
  - versioned bucket `screenalytics` is still draining historical versions before final deletion

## March 12, 2026 Better Stack + R2 follow-up now touches screenalytics too

- `TRR-Backend` now has the repo-side implementation for the post-cutover finish pass:
  - Better Stack env passthrough remains the Render/Modal logging mechanism
  - provider-neutral object storage envs (`OBJECT_STORAGE_*`) are now supported
  - bucket migration scripts now exist for AWS S3 -> Cloudflare R2
- `screenalytics` is no longer fully unaffected by the remaining storage work:
  - it still has direct S3-compatible storage reads/writes
  - it now accepts the shared `OBJECT_STORAGE_*` env aliases so it can follow the same Cloudflare R2 cutover
- `TRR-APP` still does not need consumer contract changes for this finish pass; asset URL hosts may change, but route shapes do not.
- The remaining blocker is external credentials, not repo code:
  - Better Stack source token/host
  - Cloudflare R2 endpoint + keys + public base URL

## March 12, 2026 Better Stack deferral note

- `TRR-Backend` can defer Better Stack setup until the Render cutover observation window ends on March 13, 2026 at 16:09 EDT.
- When that follow-up starts, the default is to use Better Stack free first instead of a paid plan.
- The backend repo now also contains a dedicated AWS teardown operator script and runbook for the post-window cleanup:
  - `scripts/ops/aws_teardown_pass.py`
  - `docs/deploy/aws_teardown.md`
- `TRR-APP` does not need any consumer or dashboard code change for this follow-up; the Dev Dashboard now picks it up from the backend task plan status snapshot.
- screenalytics remains unaffected by this logging follow-up.

## March 12, 2026 Render readiness hardening

- `TRR-Backend` hardened the final public-hosting artifacts for the `Render API + Modal jobs` steady state:
  - Render service naming is now normalized everywhere to `trr-backend-api`
  - the Render sync path can pass through Better Stack logging env and explicit `CORS_ALLOW_ORIGINS`
  - backend and Modal jobs now support Better Stack HTTP log ingestion when those env vars are present
- `TRR-APP` does not need consumer contract changes for this step.
- The deployed app path is still on the Modal API URL until Render billing is enabled and a live Render service exists.

## March 12, 2026 Vercel + AWS retirement checkpoint

- `TRR-APP` has now executed the deployed runtime cutover:
  - Vercel Preview `TRR_API_URL` -> `https://admin-56995--trr-backend-api.modal.run`
  - Vercel Production `TRR_API_URL` -> `https://admin-56995--trr-backend-api.modal.run`
  - final ready deployments:
    - Preview `dpl_7mCRQqEiWPmuruGriqTTjfLxNgSZ`
    - Production `dpl_C6JooMoQh4gD1jQpNRRS5qF41Lt6`
- `TRR-Backend` is now the live Modal-hosted backend API and async plane for the deployed app path.
- The legacy AWS API runtime has been retired from active capacity in the current account:
  - `trr-api-asg` scaled to `0/0/0`
  - no worker ASG is present
  - remaining target is terminating/draining only

## March 12, 2026 Render API-hosting checkpoint

- TRR-Backend now has repo-side Render deployment artifacts:
  - `render.yaml`
  - `scripts/render/sync_render_service_from_aws.py`
- No consumer contract changes have landed in TRR-APP yet because the live Render service was blocked by missing billing in the Render workspace.
- TRR-APP should continue targeting the current Modal-backed API host until a Render service exists and a frontend base-URL cutover is executed.
- screenalytics remains unchanged in this checkpoint; only the future public API host is being re-homed.

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

## March 13, 2026 hosted-media runtime follow-up

- `TRR-Backend`, `screenalytics`, and `TRR-APP` are now aligned on the hosted-media/object-storage contract for runtime media selection.
- Legacy CloudFront-hosted asset URLs are canonicalized to the live R2 public host at runtime in `TRR-APP`, while backend/storage helpers now treat `OBJECT_STORAGE_*` as canonical and preserve `AWS_*` aliases only for compatibility.
- Historical hosted-media cleanup now has a canonical backend remediation entrypoint:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/socials/backfill_social_media_mirror_jobs.py`
  - It now supports `--all-history`, season/post/source targeting, dry-run audits, and reason-filtered cleanup for legacy-host, page-wrapper, missing-hosted-media, and Twitter video-thumbnail repair cases.
- Historical image unification cleanup now also has a canonical backend remediation entrypoint:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/backfill/backfill_media_assets.py`
  - It now backfills unified `media_assets` / `media_links` coverage for `show_images`, `season_images`, `episode_images`, `person_images`, and `cast_photos`, and can generate the `thumb/card/detail` variant metadata the app consumes.
- Rows that still lack a hosted variant or unified media row will continue to fall back to their original external source URLs only until those backfills are executed for the affected historical scope.
