# Reddit capture v2 operations

This runbook describes the implemented source contract. It does not certify that a
migration, deployment, flag, credential, or provider connection is active. Record
separate evidence for the candidate source and the deployed runtime.

The scope is future runs. Reads and migrations do not repair historical captures.
Untouched legacy posts remain outside automatic lifecycle work. Legacy capture
quality is **unknown**, even when an old run says `completed`.

Implementation owners: [capture](../trr_backend/repositories/reddit_capture.py),
[execution](../trr_backend/repositories/reddit_execution.py),
[workflows](../trr_backend/repositories/reddit_workflows.py),
[live adapters](../trr_backend/repositories/reddit_runtime.py), and
[admin routes](../api/routers/socials/reddit.py). See the
[shared contract](../../docs/workspace/reddit-capture-contract.md).

## Before an operator change

1. Confirm the community, season, requested post selection, and intended effect.
2. Verify the live Supabase project is `vwxfvzutyufrkhfgoeaa` (`trr-core`).
3. Verify the Modal workspace is `admin-56995` and the app is
   `trr-backend-jobs`. The workspace's expected environment is `main`; verify the
   actual target and pass the same explicit environment to deployment and checks.
4. Confirm both capture migrations and their service-role permissions are present.
   Source files are [core migration](../supabase/migrations/20261021210000_reddit_capture_v2.sql)
   and [workflow migration](../supabase/migrations/20261021210100_reddit_workflows.sql).
5. Record the source revision, relevant uncommitted changes, deployed identity,
   environment, current flags, and rollback candidate in the task record.

Production workers require configured Reddit OAuth credentials. The transport
uses `REDDIT_CLIENT_ID`, `REDDIT_CLIENT_SECRET`, and `REDDIT_USER_AGENT`. An AI job
also requires `OPENAI_API_KEY`. Use supported secret mechanisms. Do not put secret
values in requests, logs, SQL notes, or this runbook.

Deployment validation remains a separate operator action. From `TRR-Backend`, use
`python3.11 scripts/modal/verify_modal_readiness.py --env <resolved-env>` with the
verified target, then a targeted health or worker smoke. Replace the environment
placeholder. A readiness pass alone does not establish capture coverage.

## Defaults and dispatch

| Control | Default | Effect |
| --- | --- | --- |
| `REDDIT_PIPELINE_V2_ENABLED` | Disabled | Gates the v2 capture path and Reddit dispatcher wake-up. |
| `schedule_enabled` | `false` per community/season | Enables episode capture scheduling only. |
| `ai_enabled` | `false` per community/season | Permits explicit AI jobs after model and budgets are configured. |
| `scheduled_analysis_enabled` | `false` per community/season | Permits daily AI jobs only while `ai_enabled` is also true. |

The existing Modal minute heartbeat can wake `dispatch_reddit_capture_work` while
the rollout flag is enabled. Postgres owns pending work, retries, leases, and
outbox delivery. Modal executes `run_reddit_refresh`, `run_reddit_workflow`, and
`run_reddit_episode_discovery`. A queue acknowledgement is not completion.

Workflow delivery orders lifecycle work before manual analysis, capture schedules,
and scheduled analysis. Duplicate deliveries are fenced by claims. Capture and
episode jobs have their own durable ledgers. General workflow jobs use
`queued`, `running`, `completed`, or `failed`. A failed workflow does not have a
public generic retry endpoint; investigate it before creating another manual job.

## Admin endpoints

Backend base: `/api/v1/admin/socials/reddit`. The app proxies through
`/api/admin/reddit`. Use the authenticated admin surface. The paths below are
relative to the backend base.

| Method and path | Purpose |
| --- | --- |
| `POST /runs` | Create or reuse a capture with normalized server-side configuration. |
| `GET /runs`, `GET /runs/{run_id}` | Read saved run state and evidence. |
| `GET /runs/{run_id}/targets?limit=100&offset=0` | Inspect target states, retries, and the v2 run contract. |
| `POST /runs/{run_id}/control` | `pause`, `resume`, `cancel`, or `retry`. |
| `GET /runs/{run_id}/compare?baseline_id=...` | Compare two saved runs in the same community/season. |
| `GET /communities/{community_id}/coverage?season_id=...` | Read run and per-post capture evidence. |
| `GET` or `PATCH /communities/{community_id}/settings?season_id=...` | Read or change schedule and analysis settings. |
| `GET /communities/{community_id}/legacy-inventory?season_id=...` | Read legacy inventory and scoped unresolved object deletions. |
| `POST /communities/{community_id}/lifecycle` | Queue a selected lifecycle check; legacy enrollment requires `legacy:true`. |
| `POST /communities/{community_id}/analysis` | Queue `kind:deterministic` or explicitly enabled `kind:ai`. |
| `POST /communities/{community_id}/reclassify` | Preview or apply classification from stored data. |
| `POST /communities/{community_id}/episode-discussions/discover` | Queue durable episode-discussion discovery while rollout is active. |
| `GET /jobs/{job_id}` | Read workflow or episode-job state and results. |

Settings, coverage, inventory, comparison, and job reads do not enqueue work.
Lifecycle, analysis, and reclassification bodies include `season_id`. Omitted or
empty `post_ids` means the stored community/season scope. Supply specific IDs for
a narrow manual operation.

## Capture controls and evidence

`POST /runs` accepts `refresh_policy:missing|stale|force`, optional
`selected_post_ids`, and `requested_phases:discovery|comments|media`.
`stale_after_seconds` controls freshness. `force_rescrape:true` is the force alias;
it must not conflict with an explicit policy. The server computes the configuration
hash. Similar requests with materially different inputs remain distinct.

The missing policy reuses satisfied capture evidence. The stale policy rechecks
old evidence. Force requests capture work again while still respecting lifecycle
tombstones. A post that lacks v2 evidence is eligible; the system does not infer
success from old timestamps or flags.

For a control action, send a body such as `{"action":"pause"}`:

- **Pause:** stop at ownership checks and durable checkpoints. Wait for the owner
  to release before resuming.
- **Resume:** verify Reddit health and authentication, then requeue the paused run.
  A provider block or cooldown can prevent resume.
- **Cancel:** revoke capture ownership and cancel unresolved targets. Committed
  data remains saved.
- **Retry:** create a linked new run after the parent is terminal. Satisfied
  evidence and checkpoints are retained; unresolved work gets a new attempt budget.

Legacy runs cannot use these controls. An explicit new refresh is required.

Targets are `pending`, `running`, `retry_wait`, `committed`, `source_unavailable`,
`failed`, or `cancelled`. Only `committed` and `source_unavailable` satisfy a target.
The latter requires positive evidence. It is not an assertion that content was
captured. Failed and cancelled required targets remain unresolved.

A completed v2 run requires zero unresolved required targets and no known coverage
gap. Search indexing limits, ranked samples, page/query budgets, cursor repetition,
and malformed source responses can leave coverage partial. Chronological listing
boundary evidence can establish coverage even when search has separate limits.
Inspect the manifest and targets before interpreting a terminal status.

Comment capture follows both explicit `morechildren` IDs and no-ID "continue this
thread" markers. Continuations fetch the focal parent with `context=0` and merge
its subtree into the saved parent graph. Both frontiers share the request budget
and durable version-1 checkpoints. Missing parents, malformed responses and
repeated markers remain unresolved; a zero marker count is not exhaustion proof.
Completion requires both frontiers to be empty. Report saved comment counts beside
Reddit's reported counts, which can include content that the source does not return.

If `morechildren` omits an explicit child, a guarded `/api/info` lookup can resolve
an exact post/parent/comment match whose body is `[deleted]` or `[removed]`.
These source stubs retain their parent graph and per-comment unavailable reason;
they do not recover removed text. The lookup shares the expansion request budget.
Absent, ordinary, malformed or mismatched results remain pending. Unavailable
reason evidence is derived from saved markers and survives checkpoint resumption.

Reddit media downloads have a finite 100 MiB per-file limit. Tenor GIF share URLs
are resolved through HTTPS provider metadata before the binary download; their
original URL remains the asset and target identity. The mirror still validates
the actual content type and bytes. Files above the limit or invalid/unavailable
responses remain visible as unresolved targets, rather than being recorded as saved.

When replacing the worker release during a capture, checkpoint and pause affected
runs before retiring their verified worker containers. A warm container can retain
previously imported code after deployment. Resume through normal run controls and
verify changed behavior on new workers. Preserve checkpoints and attempt history;
use a linked retry for exhausted work after the parent run reaches a terminal state.
Target setup can return large checkpoints without changing `updated_at`, so an
unchanged progress timestamp alone does not establish a stalled worker.

Media incidence reads include both legacy mirrors and v2 asset associations.
The mirrored count requires a successful mirror in either ledger. A post is counted
once even with multiple assets or both ledger types. Media incidence differs from
capture completion: a media phase can finish successfully with zero source assets.

## Configure schedules without accidental analysis

Scheduling requires an IANA `timezone`, exact timezone-aware `episode_airtimes`,
and a valid capture template. Each airtime offset must match its timezone at that
instant, including daylight-saving rules. An example airtime is
`2026-10-28T20:00:00-04:00` with `America/New_York`.

Keep all enable fields false while reviewing configuration. At the settings
endpoint, the server can resolve the current community/show capture template when
scheduling is explicitly enabled and no template exists. A supplied template must
match the settings' community and season and contain `period_key` and `subreddit`.
The canonical template uses `period_key:scheduled-season`.

The capture cadence is:

| Time relative to airtime | Cadence | Freshness limit |
| --- | --- | --- |
| One hour before through six hours after | 15 minutes | 900 seconds |
| After six hours through 48 hours | Hourly | 3,600 seconds |
| After 48 hours through seven days | Daily | 86,400 seconds |

Overlapping active episode windows coalesce into one durable slot at the fastest
applicable cadence. Scheduled captures use priority `20`, `refresh_policy:stale`,
and `force_rescrape:false`. The window starts one hour before the earliest active
airtime, clipped to a seven-day lookback, and ends at execution time. Stale slots
are rejected; the dispatcher does not replay every missed historical slot.

Changing or disabling settings invalidates queued schedule assumptions. Capture
scheduling does not enable analysis. Daily AI analysis needs both analysis enable
fields. Its idempotency key is `daily-ai:YYYY-MM-DD` in **UTC**, independent of the
airtime timezone. Expired UTC-day analysis slots are rejected.

## Analysis and interpretation

Manual deterministic analysis uses current canonical cast names and stored aliases
unless explicit `cast_terms`/`cast_aliases` override them. Ambiguous aliases do not
assign a comment to multiple cast members. Topics use the versioned default
vocabulary or explicit `topic_terms`.

Comments are sampled deterministically, up to 1,000, across post, per-post time
quartile, depth, and score. The result discloses the sampling method. Cast/topic
counts are counts over selected source rows, not population-level sentiment.

Both analysis kinds include source links, source windows, capture timestamps,
reported and saved comment counts, conservative coverage, and explicit
analysis/prompt/schema/topic/alias versions. Deterministic results add summaries
and adjacent-period comparisons using stored period assignments. Unequal windows,
unknown evidence, unavailable sources, and saved/report gaps prevent a clean
comparability claim. Descriptive deltas can still be present.

AI requires all of the following before `ai_enabled:true` is accepted:

- An explicitly selected `model`.
- A positive integer `per_run_token_cap`.
- A positive integer `daily_token_budget` at least as large as the per-run cap.
- A configured provider key in the worker environment.

Provider configuration is checked during execution; settings validation does not
prove credential or model availability. No default model is selected. A small cap
can be valid configuration yet fail to fit input plus output.

Each AI request reserves its per-run cap transactionally against the community/
season **UTC-day** budget before the provider call. Actual usage reconciles the
reservation. Missing or uncertain provider usage retains reserved capacity. Do not
release it or issue a replacement request merely because a worker failed. Preserve
the job/reservation records and reconcile provider evidence first. There is no
public operator endpoint for reservation reconciliation in this release.

AI requests use no tools. Output must pass the schema, source-reference, sentiment,
and configured cast/topic checks. Sentiment is a model interpretation. The prompt
requires uncertainty abstention and mixed sentiment where appropriate. Valid links
and a valid schema do not prove every interpretation is correct.

Valid cached analysis can be reused before any new provider request. The cache key
includes stable source inputs, settings, model, vocabulary, and versions. Capture,
settings, classification, and lifecycle changes invalidate affected outputs. A job
whose analysis has been invalidated returns `result:null` and
`analysis_invalidated:true`.

The fixed synthetic evaluation cases in
[test_reddit_workflows.py](../tests/repositories/test_reddit_workflows.py) cover
sarcasm, mixed sentiment, cast ambiguity, and unsupported claims.
`evaluate_ai_outputs` evaluates supplied offline outputs and fixed labels; it does
not call a provider. Fixture pass rates establish harness behavior only. They do
not measure the configured live model. Keep AI disabled until the operator explicitly configures the model and budgets
and authorizes activation. Live model quality requires separate evidence.

## Manual legacy checks and cleanup

1. Read the legacy inventory for the selected community and season. Its evidence
   remains `unknown`; inventory alone does not authorize a repair.
2. Select the post IDs and confirm the intended manual scope.
3. Queue lifecycle with `{"season_id":"<season UUID>","legacy":true,"post_ids":["<post ID>"]}`.
4. Poll `GET /jobs/{job_id}`. Inspect checked IDs, explicit redactions, unresolved
   object deletions, and errors.
5. Verify canonical/admin reads and derived-analysis invalidation before closing
   the operation. A queued or failed job is not cleanup proof.

A lifecycle request with `legacy:false` checks already enrolled posts only. An
explicit legacy request enrolls its selected IDs. Future refreshed captures can
be enrolled, including an existing post refreshed by an authorized future run.
Untouched legacy captures are excluded from automatic enrollment and author sweeps.

Lifecycle checks use explicit Reddit deletion, removal, or account-deletion
signals. Missing IDs, private content, an empty listing, HTTP failures, and rate
limits do not authorize deletion. Account deletion removes stored author identity
from enrolled content while preserving surviving title/body content.

Content deletion clears canonical/raw content, diagnostic snapshots, affected
analysis, and relevant episode-job snapshots. Episode-job invalidation revokes
its claim and lease. Comment tombstones prevent reimport; retained raw comment
rows discard nested `replies` copies. Verify the visible admin reads after cleanup.

Storage deletion has a separate durable outbox. It takes the shared
`reddit-object:<key>` lock and checks remaining references before deleting an
object. Future v2 Reddit objects use the `reddit/` namespace. Existing shared
legacy keys remain unresolved with `SharedLegacyObjectError` until all-platform
reference verification is available. Do not remove the prefix guard or delete
shared objects manually to make a job look complete.

Lifecycle results expose `unresolved_object_deletions` and `object_deletion_errors`.
Legacy inventory includes scoped `object_deletions`. Canonical cleanup can succeed
while shared-object removal remains unresolved. Preserve that distinction.

## Reclassification and run comparison

Preview reclassification with `apply:false`. Inspect the preview ID, expiration,
source/settings revisions, transition evidence, and `counts` for added, removed,
moved, and unchanged classifications. Applying requires the same preview ID and
unchanged inputs. Expired or stale previews require a new preview.

Added/removed describes the transition in show-or-flair inclusion. Period
assignments are preserved, so `moved` is empty. Manual threads and reviewed matches
are preserved. This operation reads stored source data and does not fetch Reddit.

Run comparison requires the same community and season. Numeric observations show
new posts, metric changes, repaired capture gaps, and newly unavailable phases.
`comparable_coverage` explains differences in normalized boundaries, requested
phases, coverage, or missing evidence. Legacy observations remain unknown. The
comparison does not retain or return deleted body/text snapshots.

## Monitoring and alerts

New capture enrollment schedules its first lifecycle check for 24 hours later.
The dispatcher checks enrolled due work; lifecycle jobs are deduplicated by UTC
hour. Confirmed checked posts receive another 24-hour check time. Missing provider
observations remain due rather than being treated as deletion.

The dispatcher returns these monitor counters:

| Counter or signal | Operator response |
| --- | --- |
| `invalid_completed_runs > 0` | Stop accepting completion claims. Inspect required targets and the source/runtime revision. |
| `reconciliation_late > 0` | Investigate enrolled checks more than one hour overdue. Check dispatcher delivery, worker errors, OAuth, and provider cooldown. |
| `unresolved_retry_targets` growing | Inspect retry times, attempt counts, coverage limits, and provider errors. Distinguish pending retry from exhausted work. |
| `Reddit invariant/reconciliation alert` | Inspect the returned counters and affected ledger rows; do not clear evidence to silence the alert. |
| Episode `error:lifecycle_invalidated` | The old result/claim was revoked. Request new discovery only after reviewing the lifecycle scope. |
| Workflow `failed` | Read the job and provider/worker evidence. Generic workflow errors can expose only a safe exception category. |
| AI reservation `uncertain` | Hold replacement inference and reconcile usage. Preserve reserved capacity. |
| Object error `SharedLegacyObjectError` | Preserve the unresolved outbox row. Arrange shared-reference verification; do not force deletion. |

Operational ledgers are service-role/admin data. Read the relevant rows from
`reddit_refresh_runs`, `reddit_work_targets`, `reddit_dispatch_outbox`,
`reddit_workflow_jobs`, `reddit_episode_discovery_jobs`,
`reddit_lifecycle_enrollment`, `reddit_ai_reservations`, and
`reddit_object_deletion_outbox`. Do not expose source bodies, raw payloads, or
credentials while collecting incident evidence.

The rollout flag also gates dispatcher monitoring and reconciliation. During a
flag-off incident, inspect the durable ledgers manually and record deferred
lifecycle work. There is no separate notification service or universal workflow
pause/cancel endpoint in this implementation.

## Rollback: flag off, drain, preserve

1. Stop new operator capture, discovery, lifecycle, and analysis submissions for
   the affected scope. Flag-off can restore the compatibility capture path, so it
   does not by itself prevent every new legacy request.
2. Set the affected settings to `schedule_enabled:false`,
   `scheduled_analysis_enabled:false`, and `ai_enabled:false`. Record the previous
   settings. Disabling settings does not undo a request already accepted by a provider.
3. Set `REDDIT_PIPELINE_V2_ENABLED=0` in the explicitly selected API/worker runtime
   configuration and send that configuration through the supported deployment
   procedure. Verify the loaded flag on the target; a local shell change is not
   runtime rollback proof.
4. Pause or cancel active v2 capture runs through their control endpoint as needed.
   Let already running workflow/episode jobs finish or use the supported provider
   interruption procedure. General workflow jobs have no public pause/cancel API.
   Preserve incomplete or expired claims for diagnosis; do not fabricate completion.
5. Inspect running jobs, claim leases, queued runs, pending dispatch/object outbox
   rows, retry times, and AI reservations. Record what drained and what remains
   intentionally queued. Flag-off stops new dispatcher work; it does not kill an
   in-flight worker or automatically empty the ledgers.
6. Roll back the runtime to the recorded compatible revision if required. Preserve
   both additive migrations and all manifests, targets, checkpoints, tombstones,
   numeric observations, budget/reservation records, and unresolved object evidence.
   Do not reverse lifecycle redaction or restore deleted content from old snapshots.
7. Verify authenticated reads, disabled settings, target-specific health, and the
   absence of new unintended work. Record deferred lifecycle checks and the exact
   conditions for controlled recovery.

Recovery resumes the durable ledger after the source/provider cause is fixed.
Review active settings before re-enabling the rollout flag. Recheck the exact
project/environment, deployment identity, readiness, and one targeted worker path.
Do not convert recovery into an automatic historical backfill or paid AI rollout.
