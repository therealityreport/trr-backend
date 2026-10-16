# Instagram ingestion rebuild

## Authority and scope

The user approved implementation in the current Codex conversation on 2026-10-16.
The earlier plan-only sentence in the copied plan describes the planning turn;
the subsequent explicit implementation request authorizes this work. Preserve
existing branches and unrelated changes. No new paid services are authorized.

Instagram first: listing, details, comments/replies and hosted media. Keep current
authentication, source scopes, date semantics, selected tasks, lane pressure,
identity blocks, retry budgets and completion rules. Keep existing run identities
and canonical data. Other platforms and active legacy runs stay on their owner.

## Accepted design

- Explicit domain/application/workflows/providers/persistence/queries modules.
- No social monolith, runtime namespace publication or wildcard imports in the new path.
- Compare real DBOS and Temporal locally before choosing. Engine history schedules;
  Postgres business receipts establish completion. No implicit engine migration.
- Transactional run intent/outbox, deterministic execution identities, fenced
  remote reservations and idempotent canonical writes.
- Freeze launch policy; persist listing discovery; seal downstream manifests when
  discovery completes. Unknown totals are not complete.
- Detail accounting: total = committed + cached_satisfied + source_unavailable +
  unresolved. Requests/attempts are separate. Preserve three-attempt detail cap.
- Fetch, sanitize/retain, normalize, validate, commit and receipt. Private Postgres
  evidence retained 30 days; unresolved runs pin needed payloads. Keep provenance.
  Replay defaults to dry-run and cannot replace newer observations with old data.
- Strict v2 actions and read-only progress, generated openapi-typescript /
  openapi-fetch / openapi-react-query clients, TanStack Query ingestion panels.
- Existing caller compatibility, additive schema and disabled initial rollout.

## Gates and execution ledger

| Gate | Evidence required | Status |
|---|---|---|
| A | Current behavior matrix and sanitized fixture baselines | Legacy callable map and retained native fixtures established; unassigned media and avatar parity added |
| B | Matched real-engine failure harness; hosting/recovery/version feasibility | DBOS selected for implementation after local comparison, bounded-executor and 50-target partition tests; production topology verification remains a release gate |
| C | Domain/provider/replay parity and import boundaries | Canonical, listing, comments, media, runtime integration and hosted-result replay extension implemented and reviewed; local checks pass |
| D | Real persistence crash, ownership, cancellation and recovery tests | Local PostgreSQL/DBOS failure matrix passes. Live cancellation preserved run/targets/budgets; live resume and partial recovery remain pending |
| E | Backend schema/client parity and browser operator flow | API/client and fixture-backed browser journeys pass. App 964e9ce remote production build and authenticated checks pass; normal TRR browser shows the run, cancellation and persistence across reload. Live resume remains pending |
| F | Deployment identity/readiness, bounded canary, 10 runs and 24h observation | Target identity/readiness and bounded idle lifecycle pass. First canary cancelled for a pre-request control-pool defect; reviewed repair deployment next. Successful runs, aggregate resource acceptance and 24h observation remain pending |

Engine selection: DBOS 3.3.0 is the implementation candidate. Real local engine
comparison, version-aware recovery, bounded single-executor replacement and
50-target partition continuation evidence support this selection. Independent
review found and closed a cancellation-at-continuation defect and clarified
statement-counter exclusions. Temporal's local correctness evidence does not
provide an existing authorized production Temporal service. No new service is
provisioned. Separate bounded/partition probes do not establish aggregate
production connection limits. Modal lifecycle, session-preserving Supabase
connections, restricted role grants and resource acceptance remain mandatory
before deployment/canary acceptance.

Candidates must pass recovery before/after dispatch and commit,
duplicate/stale owner, durable cooldown/cancel/resume, version pinning and bounded
history/resource checks. Prefer existing authorized hosting, then fewer services,
then measured coordination/recovery overhead, then DBOS. Neither qualifying means
hold engine adoption and continue independent modules/contracts, not fake success.

Runtime target record: Modal admin-56995 / trr-backend-jobs / main; Supabase
vwxfvzutyufrkhfgoeaa. Reverify before mutation and resolve recorded checkpoints.
Preserve current hard capacity caps. Reject unexplained >10% regressions in matched
DB sessions, coordination queries per committed target or ingestion elapsed time.
Full app build requires explicit current-chat approval after quick validation.
Browser acceptance uses dev-hybrid, TRR profile and canonical Portless URLs.

Canary selected by the user on 2026-10-16: Instagram `bravotv`, all calendar
2026. Freeze the UTC interval `[2026-01-01T00:00:00Z, 2027-01-01T00:00:00Z)`.
Run one at a time after release prerequisites pass. This selection does not
bypass parity, build, security, deployment, resource or observation gates.

Rollback disables new routing; pinned runs drain/pause on compatible workers. No
destructive schema rollback, in-flight engine transfers or duplicate live shadows.

## Ownership and starting state

- Parent: integration, domain contracts, persistence and rollout acceptance.
- Engine evaluation helper: scripts/ingestion_engine_eval,
  tests/ingestion_engine_eval, docs/architecture/ingestion-engine-evaluation.md.
- Baseline helper: read-only callable/behavior map.
- Backend starts at 59ccb5f4fcd63ca804aca7deea3958c4cc3c9816 on
  chore/repo-hygiene-2026-09-25 with unrelated dirty docs/tests.
- App starts at d11cd2813df09cf05e0c4712a5c5aae31826f780 on that same branch
  name with unrelated dirty docs and three components/pages.
- Hindsight initiative capture returned HTTP 402, balance zero. No memory write
  succeeded; this document is the execution record.

## Validation discipline

Use focused existing pytest/Vitest/Playwright checks, retaining passing evidence
on unchanged inputs. Test repeat submit/resume, read-only GET, worker death at all
side-effect boundaries, stale ownership, 429/auth/database pressure, unavailable
source evidence, partial comments/media, replay expiry and timestamp preservation,
refresh/multi-tab/network loss and UI actions. Independently review the complete
failure harness and rollout batch before production acceptance.

## Local evidence, 2026-10-16

- Engine report: `ingestion-engine-evaluation.md`; real DBOS/Temporal matrix,
  native Temporal pinning, 1000-target finite workload, partial cancellation,
  DBOS bounded executor replacement and explicit engine cancelled-state checks.
  None is live Modal/Supabase acceptance or a current-ingestion performance baseline.
- Foundation: 9 domain/evidence tests; 10 isolated PostgreSQL persistence cases;
  3 strict contract/read-only query cases. Use `--confcutdir=tests/ingestion` to
  isolate them from production fixture initialization. No live requests.
- Independent review found four defects, all repaired and confirmed in a fixes-only
  follow-up: discovery page replay/cursor ordering, UTC elapsed retention,
  evidence-to-target provenance and exact UTF-8 payload-size limits.
- Queries/schema work added after that review is not covered by its acceptance.
- The additive migrations are tested only on private PostgreSQL and are not applied
  remotely. API and Modal entry points now bind the feature behind explicit flags;
  launch is additionally limited to the configured account, date window and run budget.
- Explicit workflow state commands now cover retry checkpoint/cooldown retention,
  exhausted-attempt blocking, all-stage completion and outbox crash redelivery.
  Fourteen persistence/workflow cases pass against isolated PostgreSQL.
- Offline replay tests verify original capture times, account scoping, no source
  requests or writes, explicit unavailable-parser behavior and sanitized rich
  GraphQL/comment-media fields. Twelve replay/domain cases pass.
- Post activity tests prove evidence retention before validation, no database lock
  held during fetch, unresolved partial source and capture-time binding at receipt.
  Fourteen affected application/persistence cases pass.
- Canonical post/comment extraction used disjoint helper ownership; integration
  and release acceptance remain with the parent.

## Integration evidence and remaining release work

- The user approved the full app production build in this chat. It passed once;
  unchanged build inputs have not been rebuilt. Generated contracts, focused UI
  checks and direct-SQL inventory verification passed. The app adds HTTP calls,
  not direct SQL, so no SQL exception or migration-ledger entry is required.
- The connected local PostgreSQL test uses two classified posts, two listing
  pages and four native comment pages. It proves eight business receipts, twelve
  evidence records, four comment-page receipts, sealed completion in all four
  stages, lost-ack recovery without refetch and cancel/resume into generation 3.
  Source HTTP and object storage are controlled fixtures; this is not live proof.
- Independent fixes-only review closed all eight findings from the core and
  provider batches. The final integration batch found two additional defects:
  stale cached avatars could ignore newer registry observations, and the finite
  coordinator delayed legacy recovery. Both are fixed and confirmed by a
  fixes-only review. Seven affected media checks and six new plus five existing
  sweep checks passed; unchanged checks were reused.
- Existing physical Modal functions and their caps are retained. New admission
  counts legacy work, and the legacy budget now counts new leases and ambiguous
  sends after schema readiness. These are capacity snapshots, not a cross-system
  atomic reservation. The DBOS coordinator is protected by a session singleton.
- Narrow equal-output canonical persistence measured 633 versus 603 statements
  for 30 posts, a 4.9751% increase. Actual details-phase orchestration measured
  1,334 versus 1,184 execution statements for 12 targets, a 12.6689% increase,
  with peak database sessions of two for both. All 150 added statements are
  attributed in the engine report. The candidate refreshes the catalog as well
  as post/canonical models; that legacy details path does not. Therefore this
  broader probe proves common post/request accounting, not equal coverage in
  all three models. Timing is descriptive. Listing/comments/media and DBOS
  metadata overhead remain outside this matched phase measurement.
- Replay supports detail, comment, listing and hosted-media application with
  original-time and newer-observation guards. Successful media work retains
  both source inventory and a strict hosted-result envelope. Replay restores
  canonical metadata from those historical outputs without remote-object
  verification; it retains only hashes actually produced by the mirror. Old
  inventory-only captures remain preview-only. The bounded extension passed
  64 affected checks, two capture-boundary checks, ten domain/evidence checks,
  a timestamp round-trip check and independent review without findings.
- Default-off runtime flags, an explicit restricted-role DBOS bootstrap CLI,
  and exact target/account/window canary configuration are implemented. Live
  Supabase inspection confirmed the intended project and no ingestion or DBOS
  schema yet. No remote migration, secret change, deployment or canary has run.
- Next gates: apply the additive schema and
  restricted DBOS bootstrap on the verified target, deploy disabled, verify
  readiness and the affected worker path, then enable only the approved canary.
  Ten successful runs and 24 hours of observation remain unfulfilled.

## Shared local router incident

During browser preparation, a helper invoked `portless proxy stop --help` as a
help query. Portless 0.15.6 ignored the help flag and stopped the shared managed
router. This was an agent error. THB's route registrations and application
processes remained intact. The original Project Manager manifest already uses
wildcard routing; the TRR startup guard did not recognize its managed process.
A source-only compatibility repair and tests now address that recognition.
Its 29 focused checks and fixes-only review passed. Managed ownership is checked
independently from readiness, and a restoration between checks cannot trigger a
replacement proxy. The workspace repair is committed locally as `c83903d`.

The exact restoration is:

```sh
sudo launchctl kickstart -k system/local.thomashulihan.projectmanager.portless-router
```

The non-interactive attempt was blocked by `sudo: a password is required`.
The user was asked to run this command in their own terminal, without sharing
credentials.
The user then authorized Computer Use to run the command. That tool explicitly
blocked Terminal access for safety, so no Terminal command ran. The exact
terminal command was then completed by the user.
The installed Project Manager's native Router Setup was also inspected. Its
Start or update router control entered Checking while reporting a listener
ownership conflict. The original PID and stale ready receipt remained, and the
proxy markers were still absent; this did not establish recovery. After the user
completed the exact restart, the managed-router verifier passed and trusted HTTPS
returned 200 from `thb.localhost` and 307 from `admin.thb.localhost`. Cancel was
requested for the unused native update preview; its UI confirmation remains
pending. Authorize & Apply was never clicked. No configuration,
route or application-server rollback was needed. TRR browser acceptance remains
separate from this successful shared-router restoration.

## Local delivery

- Workspace startup compatibility: `c83903d` on `main`.
- App controls and generated contracts: `36fd62c` on
  `chore/repo-hygiene-2026-09-25`. The 19 committed paths exclude all preexisting
  dirty app work. Semantic OpenAPI review confirms nine added routes/operations
  and 18 added schemas, with no existing API contract changes.
- Backend candidate: `ac4f95be` on `chore/repo-hygiene-2026-09-25`.
- These are local source commits. No push, remote migration, deployment, enabled
  routing, or production canary is implied by them.

## Predeployment browser environment

`make dev-hybrid` with automatic database apply and Modal deploy disabled passed
live Modal readiness but correctly stopped on source fingerprint drift. The
candidate has not been deployed. The supported `dev-local` target is selected
for local-only Gate E browser acceptance before the controlled rollout. Modal
workers, dispatch, coordinator, schema-ready, routing and replay-apply flags stay
off. Hybrid and production evidence remain separate later gates.

Startup repairs align the doctor with the current Modal CLI/SDK and stop hybrid
startup after a failed preflight. Five focused checks passed; commit `8f6b1c9`
preserves the unrelated Makefile interpreter change. The current policy checker
also needed its retired MCP-matrix expectation replaced by the current tool
policy section and inventory reference; six focused checks and the live policy
check passed. Preflight now records a failed legacy Chrome MCP diagnostic as
unverified browser attention rather than requiring that specific transport.
Active tool/profile verification remains mandatory. Native Chrome inspection
confirmed the friendly TRR profile and its `admin@thereality.report` identity.

All three canonical TRR names resolve to loopback and return trusted HTTPS 404
through the managed router before services start. Startup uses the existing
`PORTLESS_SYNC_HOSTS=0` option because hosts resolution is already ready; no
administrator hosts edit is needed. The separate Project Manager preview still
shows disabled cancel/close controls; no update was authorized or applied.

## Browser acceptance and rollout preparation

Seven browser journeys passed against the guarded local server, with API calls
fulfilled by browser-only fixtures. Four saved screenshots were inspected: rollout
disabled, service absent, balanced incomplete progress, and expired replay evidence.
The three focused transport checks, incremental TypeScript, and scoped ESLint also
passed. App follow-up commit: `c10889a`. No production build was repeated after
the narrow existing-local-auth-helper change; the earlier build remains evidence
for its original inputs. These checks do not prove live ingestion or production
authentication.

Native Chrome in the verified TRR profile displayed the real account page with
the new feature disabled. The 2026 summary read returned 1,994 posts. Separate
legacy snapshot/catalog-total queries hit statement timeouts; this is a retained
legacy-read limitation, not successful live ingestion acceptance. Database
inspection found 13 sessions against a 60-connection maximum, without saturation.

Additional workspace startup commits: `8f6b1c9`, `3623bfc`, and `5db5639`. Existing
unrelated dirty changes remain outside the task commits.

Read-only predeployment verification confirmed Modal workspace `admin-56995`,
environment `main`, app `trr-backend-jobs`, app ID `ap-DkLTRoSvqhbkGO7fHyHxrD`,
and current version `v55` with tag `plan055-20261016-59ccb5f4-rotated`. Supabase
dry-run listed exactly the four additive ingestion migrations. No remote schema,
credential, secret, or deployment changes have yet been made by this task.

Before canary enablement, a separately reviewed bounded probe must exercise the
actual deployed coordinator helper with the restricted DBOS role. The temporary
probe keeps published routing/coordinator/replay flags off, requires empty new
business and engine state, checks singleton contention and connection demand,
and confirms owned-process/session cleanup. It does not establish active-run
recovery or full-workload resource acceptance. The prospective runtime-role
connection cap is four, based on the observed three-connection topology plus one
contender; live measurement is still required.

## Disabled rollout actions, 2026-10-16

The exact target was reverified immediately before mutation: Modal
`admin-56995/main/trr-backend-jobs` and Supabase `vwxfvzutyufrkhfgoeaa`. All four
additive ingestion migrations applied successfully and are recorded in migration
history. All 13 new business tables have RLS enabled; `anon` and `authenticated`
have no SELECT privilege. No canonical data was changed by these migrations.

The DBOS bootstrap completed at migration version 123 using DBOS 3.3.0. Restricted
role `trr_ingestion_runtime` passed runtime login and grants checks with connection
limit four, no schema/database creation privilege, no application-table access,
and `run_migrations=false`. Its credential is retained in the ignored local
`.env.ingestion-runtime` file with mode 0600; no secret values are in this ledger.

Modal's supported `Secret.update` API merged 14 ingestion/target keys into
`trr-backend-runtime`, environment `main`, preserving other keys. Schema readiness
is enabled; launch, coordinator, and replay apply remain explicitly disabled.
The canary configuration is restricted to `bravotv`, calendar 2026, maximum ten
runs, with existing one-at-a-time admission. No canary has been launched.

Disabled deployment started through `scripts/modal/deploy_backend.py --env main`
with tag `instagram-ingestion-20261016-ac4f95be-disabled`. Backend runtime source
is commit `ac4f95be`; pending task changes at launch are this ledger and the
standalone probe/test, plus the previously recorded unrelated dirty files.
Deployment/readiness subsequently passed as recorded below. The bounded runtime
probe remains pending. Rollback
retains additive tables and evidence, keeps new routing disabled, and preserves
compatible workers. The prior deployment identity is v55.

Disabled backend deployment completed as Modal `v56`, tag
`instagram-ingestion-20261016-ac4f95be-disabled`. Its guarded deploy procedure
ran explicit `--env main` readiness successfully and API health returned 200 on
the first attempt. Live provider metadata confirms the API and coordinator
maintenance function both use lean image `im-zwO0lXq7zwaQR9ngYZefjQ`. In an
active container of that image, all 42 recorded runtime-source hashes match the
accepted local candidate. Installed dependencies include DBOS 3.3.0, psycopg
3.3.6, SQLAlchemy 2.1.4 and psycopg2-binary 2.9.12. Published gates were observed
as schema-ready 1 and launch/coordinator/replay-apply 0.

The independent temporary-probe review found four issues: unvalidated database
fallbacks, a blocking stdout read beyond the watchdog deadline, unbound import
origins, and weak singleton-holder assertions. The complete batch is being
repaired and reviewed before any lifecycle probe. This is a probe acceptance
hold; no production run has started.

The app's production backend setting was classified internally as the exact
expected TRR Modal target, without recording the value. The canonical Vercel
alias currently targets `dpl_GTftK96AymQheX2juaiQqXJPZ2ax`, source `d11cd281`.
The intervening `cdb32011` commit changes only Firebase CI publishing. A clean
archive of app commit `c10889a` contains the 21 ingestion-owned files over that
current baseline and excludes unrelated local changes. Its SHA-256 is
`62c935a038b28fd02fb26a31a65cdfe13f57013fa537dac18cfb0f6c2607cfbc`.
The guarded Vercel production-environment deployment completed with
`--skip-domain`: `dpl_EGsPZm4DnjQeFMdXSRDRfsJJreA8`, immutable host
`trr-c035opoo6-the-reality-reports-projects.vercel.app`, is READY. Remote production
build passed compilation, TypeScript and generation of 104 static pages. Pinned
release evidence verified the TRR project/team and source commit. Canonical alias
promotion remains held for the authenticated disabled-capability smoke.


## Bounded deployed-runtime diagnosis

The reviewed temporary probe keeps published gates off. The first attempt stopped
before coordinator A because the Supabase session pooler did not preserve libpq
startup options. The observer now explicitly establishes and verifies read-only
transactions, a five-second statement timeout and row-security fail-closed behavior;
TLS is checked on the actual client connection. Five affected cases and the narrow
independent review passed.

The second probe returned `HELPER_NOT_COMPLETED`. Owned-process cleanup and
temporary-source removal were confirmed. A separate read-only database check
confirmed zero runtime-role sessions, zero advisory locks, zero ingestion business
rows and zero DBOS workflows. It is not a successful lifecycle result.

Safe failure diagnostics were added to retain fixed helper status/exit code, bounded
elapsed time and connection/singleton observations. Eight affected cases, Ruff,
compilation and independent review passed. The third diagnostic attempt uses this
reviewed probe; the deployed runtime source is unchanged. The source timer ordering
may consume the shutdown margin during startup; a runtime fix remains contingent
on actual diagnostic evidence. No canary has started.

Production Vercel metadata confirms the shared signing secret is present and marked
sensitive/write-only. An initial smoke-harness inference that it was missing was
incorrect and has been withdrawn. Authenticated capability smoke is still pending;
no secret replacement or provider configuration change was made for that inference.


The third diagnostic probe recorded helper `bounded_timeout`, exit 124, after
102.03 seconds including probe/startup overhead. Singleton contention passed and
peak runtime connections were three; owned-process cleanup and temporary-source
removal passed. This proves the hard timer fired, without isolating SDK startup,
control SQL or shutdown duration. Source inspection identified a refreshed loop
budget after startup. The repair uses one monotonic start for both the hard
90-second lifetime and the 85-second work cutoff. Three affected cases passed
(two fake slow-startup regressions and the existing real isolated DBOS/PostgreSQL
matrix), with Ruff, format, compilation and independent review passing.

The immutable app deployment passed a service-authenticated capability GET:
HTTP 200 JSON, ingestion disabled, replay apply disabled. The actual Firebase
admin record, verified email, enabled state, allowlist UID and production-project
match were checked using the existing local SDK credential. Existing matching
local signing credentials and the existing Vercel bypass token were used only in
memory. This does not prove a human browser login or Vercel Firebase ADC readiness.

The guarded alias promotion succeeded. Read-only provider verification confirms
`trr-app.vercel.app` targets `dpl_EGsPZm4DnjQeFMdXSRDRfsJJreA8`, source `c10889a`,
in the intended TRR project/team. A fresh canonical-host-bound capability GET
also passed HTTP 200 with both flags disabled. The prior deployment is retained
for rollback. The nonsecret receipt is at workspace
`output/releases/instagram-ingestion-app-c10889a/authenticated-smoke.json`.


## Deployed lifecycle acceptance and canary enablement

Backend revision `0f540896` deployed disabled as Modal v57 and passed explicit
`main` readiness plus API health. All 42 expected runtime hashes matched an active
container. The actual helper completed with exit 0; singleton contention passed
and peak restricted-role connections were four, within its enforced cap.

The standalone v4 probe remains `not_passed`: its immediate zero-backend-session
cleanup condition failed. Supplementary evidence resolves that condition: all
original backend PIDs disappeared, no advisory locks remained, all four role slots
accepted simultaneous fresh TLS clients, and the same singleton key was acquired
and released. Every fresh client was explicitly closed; a later observation
confirmed zero role sessions and locks. Business and DBOS workflow state remained
empty. Owned-process and temporary-source cleanup passed. Independent review
supported composed acceptance, recorded separately in
`.artifacts/ingestion-rollout/bounded-lifecycle-acceptance.json`. No provider
retention timeout or same-backend reuse is claimed. The idle lifecycle result does
not replace active-run, scraping, resource or 24-hour observation evidence.

Only the existing launch/coordinator flags were enabled for the configured
`bravotv` calendar-2026 canary. Replay apply remains off. The hard account/window,
one-active-or-resumable-run and ten-total-run limits remain enforced. The first
enabled deployment command was rejected before deployment because its tag exceeded
Modal's 50-character limit; the corrected shorter tag is
`ig-ingestion-20261016-0f540896-bravotv`. Deployment verification is pending.


The corrected enabled deployment passed readiness and API health. Direct live
capability reads returned `enabled=true` for `bravotv`, `enabled=false` for an
out-of-canary account, and replay apply false for both. The canonical app also
passed its authenticated enabled-capability check.

The first launch attempt used stable key
`instagram-ingestion-20261016-bravotv-2026-canary-01` and the approved UTC window
`[2026-01-01,2027-01-01)` with all three downstream tasks. It returned HTTP 503;
read-only verification confirmed zero new runs. Admission counted 21,792 terminal
legacy jobs, all older than one day with no recent heartbeat, because historical
remote status/reservation metadata remained nonterminal. Existing legacy capacity
code limits such terminal drain ambiguity to two hours, matching the reused
physical functions' 7,200-second timeout and zero automatic retries. A narrow
admission repair is in progress to preserve that window and current caps, while
retaining recent/nonterminal work, missing-time ambiguity, and explicit live leases.
No historical jobs have been modified. The canary has not started.


The admission correction passed 40 isolated PostgreSQL cases, formatting, lint,
compilation and independent review. A read-only check against the actual TRR
database using the corrected query found no occupied legacy capacity and admitted
the listing slot (`blocked=false`, `fits_listing=true`), without modifying jobs.

Native Chrome's window title verified the TRR profile. The normal production
account page displayed the new ingestion panel and the saved 1,994-post 2026
summary using its existing session, without credentials being extracted or a
login flow. No run was selected. Before a browser launch, source review found
that the existing month picker emits an inclusive final millisecond while the
new API uses an exclusive end. An app-only new-client request conversion is being
prepared; legacy route payloads remain unchanged.


Backend revision `938b37b9` deployed to the explicit TRR `main` target and passed
readiness, HTTP 200 health, and all 42 expected deployed source hashes. Retrying
the unchanged first-launch payload and idempotency key returned HTTP 202 at
2026-10-16T10:50:53Z. Run `31fc35b1-654e-4306-8d71-ca02c8bb770a` advanced
from queued to running without browser polling. The normal production TRR Chrome
session then displayed that same run and stage accounting.

Listing reached two attempts with zero recorded source requests, no evidence and
no committed results. This is under investigation and is not a successful canary.
The parent used the production browser's Cancel Run and Confirm cancel controls;
the UI confirmed cancelled, generation 2, Work remains, with Resume Run available.
The run, intent, target and attempt counts were preserved. Resume remains held
while the worker path is diagnosed. No broader account routing was enabled.

The app month-boundary correction passed seven focused runtime cases, incremental
TypeScript and independent review. Existing ESLint configuration excludes the
large page; ESLint exited zero and checked the test file. The new request DTO
converts only an exact inclusive UTC month end to the following exclusive month
start. Legacy payloads remain inclusive. Its production release is in progress.


Both accepted posts calls returned the safe operational code
`identity_policy_unavailable` before contacting Instagram. The verified deployed
`social_control` pool has min/max one. The identity gate borrowed that connection
then called `Store.check_owner`, which attempted a second borrow and failed. The
listing lane insert rolled back. The same nested pattern was found in the shared
detail transport; the batch audit covered listing, comments, details and avatars.

The reviewed correction uses an explicit supplied transaction for in-lock ownership
checks, preserving lane → run → target lock order and closing the transaction before
source work. Pool limits remain unchanged. Safe fixed failure codes now persist in
the target checkpoint and appear through the existing optional diagnostics field;
invalid text becomes `activity_failed`. No schema change is needed. The 108 passing
affected checks were reused; one cursor-only checkpoint assertion was updated and
its single affected case passed. Real one-connection, lane-lock cancellation, detail
reuse, diagnostic sanitation and retained-budget cases pass. Independent review
reported no findings. The candidate has not yet been verified in production.

App `964e9ce` is deployed as `dpl_5tR5bTQi3ueMA3mayt4h4dg9RKwW` and promoted
to `trr-app.vercel.app`. Provider project/team/source/READY checks and immutable plus
canonical authenticated GETs passed. Compilation, TypeScript and 104 generated
pages passed. Normal TRR Chrome reload retained the cancelled generation-2 run and
two listing attempts. The previous app deployment remains READY for rollback.

The next backend deployment will keep coordinator/workers enabled but disable new
launch routing, to verify rollback semantics through resume of this same pinned run.
No accounting or successful-ingestion claim follows from the accepted launch.
