# Reddit capture v2 rollout — 2026-10-21

## Verified runtime

Reddit v2 is enabled for future runs in Modal workspace `admin-56995`, environment
`main`, app `trr-backend-jobs` (`ap-DkLTRoSvqhbkGO7fHyHxrD`). The enabled release is
`v94`, tag `reddit-v2-enabled-22ae7a68`. Compatible readers first shipped as `v92`.
The deploy candidate used backend revision `22ae7a68a7f57e9bbd73e8b18d20f69aa064243b`
plus the exact files in the [source manifest](2026-10-21-reddit-capture-v2-manifest.json).
A later concurrent commit `3f6d13ed` changed only Instagram deployment documentation.
The candidate excluded unrelated dirty source. No branch switch was made.

Both additive migrations, `20261021210000_reddit_capture_v2` and
`20261021210100_reddit_workflows`, were applied to Supabase project
`vwxfvzutyufrkhfgoeaa`. Before revoking public operational reads, source and database
reader inventories found no dependent public views. Readback confirmed migration
versions and that `anon` no longer has SELECT access to refresh runs. Existing
content APIs remain available through their authenticated adapters.

The deployment procedure ran `verify_modal_readiness.py --env main` and the API
cold-start canary successfully. The deployed Reddit OAuth probe was configured,
authenticated and reachable, with no active cooldown or block. The explicit
Reddit dispatcher smoke returned enabled with zero unresolved retries, overdue
reconciliation items, or invalid completed runs.

The workspace permits five scheduled functions. Reddit therefore uses the existing
minute heartbeat to spawn its separate dispatcher. Failure to wake Reddit is logged
and does not stop the other heartbeat work. The disabled capability does not wake it.

## Bounded acceptance evidence

The [runtime receipt](2026-10-21-reddit-capture-v2-receipt.json) stores identifiers
and capture evidence without comment bodies or credentials.

| Check | Result |
| --- | --- |
| Discovery: one page, r/summerhousebravo, new 24-hour window | Three posts saved; chronological lower boundary proven; two targets satisfied, zero unresolved |
| Comments/media: one selected newly discovered post | Six comments saved against six reported; two detected media assets persisted; four targets satisfied, zero unresolved |
| Future-run creation and automatic dispatch | Contract v2 selected by the normal creation path; minute dispatcher picked up the run; existing comment/media evidence reused |
| Deterministic analysis | Automatic worker completed a one-post/six-comment job with source window, capture timestamps, versions and coverage metadata |
| Rollout exclusions | Schedule, daily analysis and AI settings disabled; zero manual legacy enrollments; only three newly captured posts enrolled |

No historical season repair, manual legacy cleanup, schedule activation, or paid AI
call was performed. Synthetic AI fixtures passed four reviewed cases and detected
four injected regressions. This tests the harness and output guards; live-model
classification quality is unmeasured and is not certified by this rollout.

## Source and app validation

Focused backend tests cover transport, comments and media retries, checkpoint
resume, coverage, controls, API authorization, lifecycle, schedule timing, analysis
budgets, and output validation. Isolated PostgreSQL tests cover deduplication races,
claim fencing, transaction rollback, constraints, permissions, and engine recovery.
Independent implementation review findings were fixed and checked in focused batches.
The Modal image-ordering and heartbeat wake changes passed affected tests.

App quick validation, focused route/component tests, generated contract checks and
TypeScript validation passed during implementation. The explicitly approved full
production build passed. Later panel-only changes received focused tests and lint;
the final typecheck passed. Existing
passing suites and the production build were not repeated on unchanged inputs.

Browser validation used `make dev-hybrid`, friendly Chrome profile `TRR`, and
`https://admin.trr.localhost/summer-house/s10/social/reddit`. Verified: authenticated
coverage loading, committed versus unknown post capture, run selection, committed
work targets, and disabled schedule/AI settings. A local auth option mismatch was
fixed and browser-rechecked. Expanded boundary/target evidence received a focused
component regression test; the browser connection dropped during a subsequent
network change. No production app deployment was performed.

## Rollback and retained limits

Stop new v2 dispatch using `REDDIT_PIPELINE_V2_ENABLED=0`, pause/drain or checkpoint
active runs, then deploy the compatible source with the flag off. The prior `v91`
release is retained by Modal. Keep all additive tables, manifests, tombstones and
saved content; do not restore deleted content or delete evidence to roll back.
See the [operator procedure](../reddit-capture-v2-operations.md#rollback-flag-off-drain-preserve).

Shared legacy storage objects remain visibly unresolved until all-platform
references are checked; v2 assets use the Reddit namespace. Uncertain provider
usage retains its AI budget reservation. General workflow jobs do not expose the
capture-run pause/cancel controls. These limits are documented in the operator guide.
