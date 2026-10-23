# Instagram missing-data repair and commit readiness

Date: 2026-10-23. Scope: repair future retrieval/persistence behavior and prepare the pending Instagram feature for commit/push. Full January completeness is not an acceptance requirement. This report records validation before publication; see Git history for the subsequent feature commit.

## Root causes and repairs

1. New GIF-only comments can legitimately have empty text; unavailable authors can have no username. Canonical merging removed empty values even though the database requires both columns. This could reject a complete page transaction. New rows now receive empty strings, while known saved values survive sparse refreshes.
2. Canonical upserts excluded created_at from every update, preventing correction of null/epoch dates even when native observations supplied valid dates. Only missing/epoch dates are now repaired, and only from native dates between the post date and observation time. Valid dates, first-seen metadata, and reply links remain unchanged. Historical evidence identified 47 epoch rows; the original cause of those dates is unproved. No bulk data repair was performed.
3. Authenticated bootstrap projection could discard a hasNext marker before validation. Frames containing either hasNext value are now rejected as unsupported streams, preventing a false-complete projection. This does not enable authenticated fallback for the public January run.

These defects do not establish that every missing upstream comment is recoverable. Retained evidence also shows anonymous source omissions despite pagination exhaustion. Expected totals were not reduced. Full-history remains disabled. Media work is intentionally ordered after comments; its pending state during the comments phase is not proof of media loss.

## Validation

- 119 affected tests passed across canonical persistence, bootstrap, relay, and attachment inventory/media integration. Regression cases use isolated PostgreSQL and cover GIF-only inserts, missing authors, sparse refreshes, date repair bounds, and preserved parent/reply links.
- One pre-existing clean-interpreter import-boundary test fails because provider_registry is loaded through the Instagram package initializer. The same behavior was reproduced in the unchanged prior installed release. It is not a new regression; this is not a claim that the full suite passes.
- Thirteen initial fixture setup errors came from an overly narrow pytest confcutdir. Rerunning those attachment suites with the normal root fixtures passed all 13.
- Scoped Ruff and git diff --check passed. Independent review covered the pending ingestion batch; fix-only review found no remaining defects.
- Review evidence was delivered by the native child. Jev Router reconciliation returned HOST_ERROR (thread not loaded); its assignment telemetry remains pending. No successful Router acceptance is claimed.

## Runtime delivery

- Target: Modal workspace admin-56995, environment main, app trr-backend-jobs (ap-DkLTRoSvqhbkGO7fHyHxrD).
- Deployed v136, tag ig-missing-data-77cac4944569, at 17:03:42 EDT. Source: 86edb5e127c7bd143cedbab5ee12f2fbcc5eb025 plus the reviewed uncommitted changes. The concurrent TikTok commit and cloud changes were preserved; 537 unchanged production Python files matched the pre-deploy cloud baseline.
- Explicit-environment readiness, health canary, loaded-code hashes, streamed-frame rejection, and closed full-history gate passed. Smoke made no database writes or Instagram requests.
- Local worker was switched through a recorded drain and single bootstrap to the same release. PID 58705 and working directory were verified. Run 2aef72ed-110c-4af9-b2b3-4a45da170a32 retains generation 1, intent hash, public transport, and policy revision 7 with six concurrent posts. No attempt, reservation, or policy reset was made.
- Post-start observation confirmed one coordinator lease, running state, unchanged policy, and no blocked unresolved comment targets. At the bounded follow-up, the latest page still predated the handover (41,977 pages); fresh post-handover page persistence has not been established. Existing reservations were allowed to expire normally. Source regression checks and deployment identity are verified; full-run progress/completeness is not claimed.
- Prior local v131 release and original module/plist/binding copies remain available for recovery. Superseded v130 was moved to actual Trash after checking process, open-file, and LaunchAgent references.
- No app/UI, API contract, SQL migration, or database backfill was changed by this repair. No app build or new scheduled task was required.

Private operational receipts and rollback material: ~/.codex/tmp/trr-instagram-ready-01a16a89 (deploy.log, cloud-smoke.json, local-manifest.json, activation.json). These are local evidence, not files to commit.

## Commit scope

The existing checkout and index were preserved. The feature has pending earlier attachment/authenticated-transport work as well as these repairs. Review and stage the following ingestion paths together; do not stage unrelated repository-hygiene, TikTok, media-mirror, API-test, or app changes. Include this readiness report and review the existing Instagram validation/contract documentation separately for any unrelated edits.

- `scripts/ingestion/launch_authenticated_local.py`
- `tests/ingestion/test_attachment_activity.py`
- `tests/ingestion/test_attachment_inventory_guards.py`
- `tests/ingestion/test_attachment_media_integration.py`
- `tests/ingestion/test_authenticated_comment_transport.py`
- `tests/ingestion/test_comment_attachment_refresh.py`
- `tests/ingestion/test_launch_authenticated_local.py`
- `trr_backend/ingestion/attachment_activity.py`
- `trr_backend/ingestion/authenticated_comment_transport.py`
- `trr_backend/ingestion/authenticated_relay_contract.py`
- `trr_backend/ingestion/comment_attachment_refresh.py`
- `scripts/ingestion/local_worker.py`
- `scripts/ingestion/validate_fixed_posts.py`
- `tests/ingestion/test_authenticated_bootstrap.py`
- `tests/ingestion/test_authenticated_relay.py`
- `tests/ingestion/test_canonical.py`
- `tests/ingestion/test_fixed_validation.py`
- `tests/ingestion/test_local_worker.py`
- `tests/ingestion/test_public_composition.py`
- `trr_backend/ingestion/authenticated_bootstrap.py`
- `trr_backend/ingestion/authenticated_relay.py`
- `trr_backend/ingestion/canonical.py`
- `trr_backend/ingestion/command_service.py`
- `trr_backend/ingestion/comments.py`
- `trr_backend/ingestion/composition.py`
- `trr_backend/ingestion/domain.py`
- `trr_backend/ingestion/evidence.py`
- `trr_backend/ingestion/fixed_validation.py`
- `trr_backend/ingestion/media.py`
- `trr_backend/ingestion/media_snapshot.py`
- `trr_backend/ingestion/replay.py`
