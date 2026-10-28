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
| D | Real persistence crash, ownership, cancellation and recovery tests | Local PostgreSQL/DBOS failure matrix passes. Live cancel/resume preserved run, targets and budgets; successful partial recovery remains pending |
| E | Backend schema/client parity and browser operator flow | API/client and fixture-backed browser journeys pass. App 773eab6 is promoted and authenticated checks pass; normal TRR browser verifies local placement controls and current durable counts. Earlier cancel/reload/resume evidence remains valid for unchanged modal controls. |
| F | Deployment identity/readiness, bounded canary, 10 runs and 24h observation | 5b140c72 (v75) is deployed and verified. The public local v2 canary has 99 post identities and is cancelled at generation 6 with all manifests unsealed. Public replies are rate-limited; full commenter-profile metadata remains unverified. Old authenticated run remains held generation 9. Complete live replies/media/profile verification remains pending. Full pipeline, representative-run and 24-hour gates remain open. |

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

Backend `17f978d1` subsequently deployed to verified workspace `admin-56995`,
environment `main`, app `trr-backend-jobs`, with new launches disabled and the
coordinator enabled. Readiness, HTTP 200 health and all 43 expected deployed source
hashes passed. The production UI displayed the disabled-launch message and retained
Resume Run for the cancelled run. Resuming through that normal browser control
preserved the intent and counts and advanced the same run to generation 3.

The next listing attempt reached the source boundary: three cumulative attempts,
one recorded request, then `blocked` with safe code `unauthorized`. No evidence or
canonical result was committed, and no lease remained. This verifies the repaired
connection path and retained-run recovery routing, but does not establish successful
ingestion. New launches remain disabled; the blocked run is preserved.

The existing TRR Chrome session displayed a healthy Instagram feed. Validation of
that profile's extracted cookies through the repository's GraphQL validator failed
with `instagram_graphql_initial_request_failed`. Cookie values were kept in memory;
no cookie file or Modal secret was updated. Browser success does not establish API
session validity. The failure phase and response classification remain under
investigation before another source attempt or credential transfer.

The complete matched four-stage resource comparison is in progress against frozen
backend `17f978d1`, using actual legacy orchestration and DBOS paths with external
I/O fixtures. Runtime inspection established effective default pool min/max 1/2,
named control min/max 1/1 and close-after-return enabled. Local direct PostgreSQL
results will be labelled separately from deployed session-pooler behavior. No full
resource acceptance, ten-run gate or healthy 24-hour observation is claimed yet.

The safe local warmup probe reproduced a classification false positive at
2026-10-16T11:23:27Z: HTTP 200, actual final route `profile`, but classified as
`checkpoint` because auth words occurred in script/style content. There were no
matching text nodes outside scripts/styles and no username/password input pair.
The probe used a frozen diagnostic-only fetcher, made one browser warmup operation,
and did not run GraphQL, write credentials or touch the database. The first two
local attempts stopped before a response because the required Patchright browser
executable was missing; installing its matching Chromium resolved that separate
local prerequisite. These local facts do not prove the deployed cookie's validity.

Full-stage resource inspection also found an exact-URL reuse difference: when the
source thumbnail and media URL are identical, the candidate stored two original
objects while legacy reused the cover object. The media array itself still had
one entry; the duplicate object was the separately hosted thumbnail. A narrow
per-snapshot reuse fix passed 11 affected tests and independent review. It keeps
separate role metadata and preserves conflicting existing storage keys. Original
frozen measurements are retained; current-candidate measurements remain pending.

The warmup repair passed 53 focused tests and independent review. It requires
structured authentication evidence while retaining HTTP 401/403 and terminal
route guards. A second local source check at 2026-10-16T11:27:38Z returned HTTP 200
at the profile route and passed warmup with the repaired classifier, despite the
same incidental script words. It made one browser operation, no GraphQL call and
no credential/database write. Deployed current-credential verification remains
required before any correction of the retained identity/target blocks or resume.


Backend `595fda4b` deployed to the verified TRR `main` target with launch routing
still disabled, coordinator enabled and replay apply disabled. Readiness and HTTP
health passed. Fresh API containers matched all 44 expected source files. A guarded
worker bootstrap rejected an empty reservation before database/source activity;
the actual social worker image then matched the same 44 files and had Patchright.

A bounded deployed-current-credential probe at 2026-10-16T11:32:28Z returned HTTP
200 at the home route, which the repaired warmup classifier rejected. It made one
warmup operation and no GraphQL request, database write or credential change.
Local TRR Chrome cookies passed repaired profile warmup, but the bounded GraphQL
request returned HTTP 200 with a guarded JSON error envelope: numeric error
1357001 and a login-required signal. The normal TRR Chrome session still displayed
the signed-in Bravo profile and current posts. These observations establish a
request-path discrepancy, not its cause. Structured GraphQL envelope handling is
being corrected and reviewed before any further source check.

An in-memory comparison established that local TRR Chrome and the deployed
credential resolver use different sessions and different Instagram accounts.
No account identifiers, credentials or hashes were persisted. No secret was
updated. Substituting those cookies would change the scraping identity and is
outside a routine same-account refresh. Identity and target blocks remain intact;
the retained run has not resumed again.

The full-stage resource harness required reassessment after two independent
fixture schema gaps. Those gaps were corrected, but every post, comment and avatar
asset still used identical PNG bytes. That stress case favors legacy global hash
reuse and is not representative of a distinct-asset workload. Its measurements
are retained without acceptance: legacy 3,975 adjusted SQL statements, peak two
sessions and 13.91 seconds to stage completion; candidate 15,814 statements at
completion, peak five sessions and 55.66 seconds. Full candidate lifecycle reached
18,386 adjusted statements and 86.83 seconds. A complete fixture-contract and
counter audit is required before another matched pair. The resource gate, healthy
canary completion, ten-run gate and 24-hour observation remain open.


The guarded-JSON/error-classification repair passed 45 focused checks, three final
HTTP/HTML guard checks and 44 affected checks after the review correction. The
independent reviewer closed the malformed-connection finding. Decoded mappings
reach the source observer before classification. Error 1357001 is explicitly
rejected; generic errors, malformed nodes and missing pagination proof remain
unresolved. Explicit valid empty terminal pages still work in both directions.
No request or retry budget is reset by the repair.

Passive native TRR Chrome DevTools inspection then observed a successful timeline
pagination response at `/graphql/query`, HTTP 200, with the expected native
connection and edges. It used `PolarisProfilePostsTabContentQuery_connection`,
public document ID `39229011733413759`, and `__user=0`; none match the maintained
query name/document fallback combination or its `__user=viewer_id` shape. Native
form fields also included `__hsi`, `fb_dtsg` and `jazoest`. Only allowlisted shape
facts were saved; private request/response values were discarded. The browser's
save-login prompt was dismissed with Not now, and DevTools was closed afterward.
These differences establish contract drift, not which difference causes rejection.

A bounded local probe at 2026-10-16T11:51:11Z changed only `__user` to zero, keeping
the reviewed fetcher and other request fields. Warmup passed; the single GraphQL
response returned guarded numeric error 1357004 instead of 1357001, without a
native connection or login signal. The source-operation guard prevented a second
GraphQL request. This is diagnostic, not successful ingestion or causal proof;
credential, identity and launch state remain unchanged.

The resource review consolidated seven fixture/counter corrections: native worker
entrypoints and matching subprocess boundaries, distinct asset bytes, native worker
registry schema/preflight, complete field/asset assertions, persisted storage
operations, observer-failure propagation, and frozen-source enforcement. They are
being implemented as one batch before one reviewed representative pair. Resolved
production pool facts remain default min/max 1/2 and control 1/1: local-development
mode bypasses the Modal session-pooler ceiling. No pool setting was changed.


Backend `6f38e07d` deployed to the verified `admin-56995` / `trr-backend-jobs`
`main` target with new launches disabled, coordinator enabled and replay apply
disabled. HTTP health and target readiness passed. Four inspected fresh API
containers matched all 44 source hashes; one short-lived container was unavailable
for inspection and is not counted as verified. The guarded social-worker bootstrap
rejected an incomplete reservation before database or source activity. Its actual
worker container `ta-01M529Y27R99FEZNXDNHHR1G3R` matched all 44 hashes and included
Patchright. Offline deployed-worker checks passed guarded decoding, explicit auth
classification, malformed-connection rejection and valid empty-terminal handling.
They made zero database or source calls.

Local token-context diagnostics stopped before GraphQL when the context was
ambiguous or unavailable. The one-warmup structural audit at
2026-10-16T12:11:09Z returned HTTP 200 at the profile route and found two named DTSG
definitions in separate script scopes. Main-world `require` was absent while
`requireLazy` and `__d` existed. Both CurrentUser definitions used zero; this alone
is not evidence of an invalid Instagram identity. No token was selected by position,
no secret was updated, and no identity/target block was changed. The audit retained
only bounded structural metadata, not HTML, tokens or account identifiers.

The seven resource-harness corrections passed independent fixes-only review with
no findings. The parent accepted one representative pair using the frozen corrected
harness and existing legacy/candidate exports. Full output parity and resource
acceptance remain pending that pair and attribution. Later guarded-error changes
are outside its valid-fixture/SQL inputs and do not invalidate their frozen lineage.


A fresh native TRR document/request comparison identified two distinct DTSG
candidates. The successful same-document initial timeline request used the first
candidate only. Its public operation was `PolarisProfilePostsQuery`, document ID
`28834890909485062`; the earlier pagination request used a different operation.
All private values stayed in memory and were cleared; no native token was copied
into another transport. This narrows document-context selection, but does not prove
that the copied-cookie browser context is equivalent or that context explains the
remaining source rejection.

The corrected representative resource pair passed common data and asset assertions:
12 posts, 24 comments/replies, three avatars, and 147 physical media objects per arm.
Both arms had zero SQL errors, no final workload sessions, and stopped their private
Postgres instances. Adjusted statements through business completion were 6,089
legacy versus 15,872 candidate; full lifecycle counts were 6,102 versus 18,222.
Peak sessions were two versus five. Single-trial completion times were 69.644 and
57.775 seconds; full lifecycles were 70.043 and 86.804 seconds. These are descriptive
measurements, not accepted performance claims. Attribution of SQL, session and
lifecycle increases remains required; no additional pair was launched.


Parent local resource decision: accept the completed matched comparison for the
bounded canary under the plan's rule against *unexplained* regressions above 10%.
Independent evidence review found no findings. The additional calls are assigned
to auditable ownership/request accounting, transaction setup, journal/evidence,
SDK, storage and global coordinator availability mechanisms; their equations
reconcile. This is explicit acceptance of measured overhead, not proof that every
call is necessary or efficient. Repeated avatar downloads and conditional-storage
HEADs are known implementation costs. No pool cap was raised. Do not claim resource
improvement or production-wide capacity from this one local pair. Live session
capacity, active-run behavior and the ten-run/24-hour observation remain required
before expansion. The reviewed report retains all numeric increases and limits.

At 2026-10-16T12:25:19Z, a bounded local request using its own uniquely selected
direct document context returned a timeline connection with ten edges. The probe
used the maintained first document/query/variables, `__user=0`, its own DTSG,
DTSG-derived jazoest and same-warmup runtime fields. Two operations occurred: one
warmup and one GraphQL request. Normal pacing remained enabled; zero configured
session database candidates and no cached session pool meant the maintained file
fallback applied. No database or credential writes occurred. Its envelope check
recorded error-key presence, not nonempty errors, so normal parser acceptance is
still pending. The accepted production-fix design preserves optional-context
behavior, existing request/retry/auth policy, query IDs, and fresh-token handoffs.


### Current Scrapling query contract and response evidence (2026-10-16)

The maintained posts transport remains Scrapling/Patchright profile warmup followed
by HTTP GraphQL using that warmup's cookies and runtime context. The durable
`ScraplingListingSource` already uses this path. The separate legacy browser
fallback is not a drop-in replacement: it persists account storage, contains an
older initial-query contract, and does not expose the per-operation ownership and
source-evidence hooks required by the durable adapter. Its mocked fallback-success
tests are not current live pagination evidence. This does not invalidate historical
successes through that strategy.

Native TRR browser inspection and bounded local diagnostics identified the complete
current connection operation `PolarisProfilePostsTabContentQuery_connection`,
document `39229011733413759`. Its variables include the existing pagination fields,
`include_multi_captions`, and four provider flags: MultiCaption and ShortDrama true,
ThreadsStyleComments and ReelsRecoDebug false. A diagnostic missing the final two
flags failed; the complete observed contract with a null initial cursor returned
12 edges and valid pagination at 2026-10-16T12:49:32Z. This is current-contract
compatibility evidence, not a proof of arbitrary future document stability.

The initial native operation and complete connection diagnostic each contained
one error at an edge's `node.location.profile_pic_url`, with severity `ERROR`.
That field is not used by the canonical post/location normalizer. A candidate
shared response policy is being implemented for live fetching, listing acceptance,
and retained-evidence replay. Only that exact optional path may be accepted after
connection, node, index, pagination and operational-error checks. Unknown, mixed,
pathless, authentication and rate-limit errors remain rejected. Retained provenance
will contain bounded typed classifications and paths, never arbitrary error text,
request tokens or stack traces. The affected fetcher, response-policy and evidence checks passed; existing
top-level authentication classification and stable failure reasons were preserved.

The signed-in TRR Chrome session differs from the deployed Instagram account.
No credentials, identity blocks, target blocks or launch flags were changed during
these diagnostics. Replacing the shared deployed cookie environment would affect
other Instagram consumers and requires an explicit identity/scope decision. The
current source changes do not establish deployed authentication recovery or a
healthy canary; normal completion, partial recovery and ten runs/24 hours remain
open.


At 2026-10-16T13:06:13Z, normal candidate transport validation passed with one
Scrapling warmup and two unmodified `fetch_posts_page` calls. Each page returned
33 posts using the existing configured page size, the next cursor advanced, and
both responses passed evidence capture/read, strict listing parsing and offline
listing replay with 33 posts each. All three source operations returned HTTP 200.
The probe used five frozen source hashes and made no database or credential writes.
This proves two-page behavior in one fetcher context and replay of those pages;
independent worker contexts, production authentication and canonical commits remain
separate live-canary gates. Independent combined review and disabled deployment
remain pending this entry.


The combined review found one blocker: explicit permission/auth/rate primitive
flags on an error or its extensions could bypass the ancillary-field policy.
The shared primitive classifier now runs before optional-field acceptance at all
three levels. Actual boolean true is required for boolean identity verdicts;
malformed markers reject generically. New negative cases preserve rejection through
sanitization, evidence read, listing parsing and replay. The repair passed 33 new
policy/provenance cases and 13 fetcher callback/top-level compatibility cases.
Fixes-only independent review found no remaining findings. Previous passing checks
were reused for unchanged behavior.

At 2026-10-16T13:15:16Z, one warmup and one normal GraphQL page passed with the
final policy hash `64b48a660a5475f3d709560721e7d9a937e2c89435646f36e5bfc7c9d5935b7a`.
The response contained 33 posts and valid next-page metadata, with no fetch/auth
failure. This bounded final-policy check made no database or credential writes.
The earlier two-page/replay result remains the pagination evidence. Disabled
deployment and actual worker source verification are the next gates; the shared
production authentication identity decision remains unresolved.


### Disabled deployment acceptance (2026-10-16T13:20Z)

Commit `79f5dc8d` is deployed as Modal `v63`, tag
`ig-ingestion-20261016-79f5dc8d-rollback`, in workspace `admin-56995`,
environment `main`, app `trr-backend-jobs` (`ap-DkLTRoSvqhbkGO7fHyHxrD`).
The provider reports `79f5dc8*` because unrelated pre-existing dirty files were
preserved. The scoped 45-file deployed source manifest has no mismatches.
The guarded deployment's cold-start API health check returned 200. Readiness
passed with no blocking/advisory failures; all required functions and secrets
resolved. Dispatch configuration resolves the same named social functions.

Three live lean containers passed the source manifest and five pure response,
evidence and replay checks. A fresh posts-worker bootstrap rejected an empty
reservation before source/database work; its newly observed container
`ta-01M52DWF6ZWZ83YTVVG9GF68SR` passed the same 45 hashes and all five checks,
with Patchright available. The initial brief worker probe exited before inspection;
its failed inspection remains in the evidence rather than counted as acceptance.
The successful worker proof is
`.artifacts/ingestion-rollout/deployed-worker-source-proof-79f5dc8d.json`.
All inspected containers report launch=0, coordinator=1, replay-apply=0. These
checks do not establish production authentication or successful canary completion.

Rollback material remains available: provider `v62`/tag
`ig-ingestion-20261016-6f38e07d-rollback`, committed source history and prior
protected evidence. No credential, identity/target block or launch setting was
changed by this deployment. The user explicitly prohibits all use of the
`thereality.report` Instagram account's sessions/cookies, including local diagnostics,
validation, cookie refresh and deployment. Never use that identity. Earlier local
probe results are historical evidence only; they do not authorize future use.
No session/cookies from that account were transferred to Modal or persisted by these
probes. All 16 task-owned browser-cookie probe scripts have been disabled before
credential extraction. The proposed shared-cookie replacement option is withdrawn.

To continue, identify and verify a separately authorized Instagram account and use
only its session. The shared `SOCIAL_INSTAGRAM_COOKIES_JSON` key in `trr-social-auth`
affects other Instagram consumers, not just bravotv. Preserve the current deployed
identity unless a different permitted identity is explicitly authorized. Any
approved refresh must preserve protected rollback, merge only the selected key,
verify a fresh worker's selected identity and normal source path, then perform
guarded target recovery. Normal completion, partial recovery and the ten-run/24-hour
gate remain unfinished.


### Same-account session recovery (2026-10-16)

The user directed recovery of the existing Modal ingestion identity and explicitly
authorized the `<ingestion-account>` login in Chrome's `codex` profile. The prohibition on
`thereality.report` sessions/cookies remains absolute. The active browser tool
verified profile `codex` and the account's own `<ingestion-account>` edit controls. A new
helper is pinned only to that verified profile; the 16 previous browser probes
remain disabled.

A salted in-memory comparison verified that Modal's selected `env_json` credential
and the authorized browser login have the same account ID but different session
tokens. The resolver's `require_validation=False` result established required
fields, not live login validity. Browser login does not update the Modal secret.
The fresh authorized session passed the maintained normal fetcher against
`bravotv` locally. Two old-session worker attempts failed before a response with
`instagram_posts_warmup_transport_failed`; browser startup itself passed. These
failures do not distinguish expired/revoked credentials from a transport event.
Earlier deployed-session evidence also recorded an authentication rejection.

Only `SOCIAL_INSTAGRAM_COOKIES_JSON` in `trr-social-auth`, environment `main`, was
refreshed at 2026-10-16T13:30:21Z via the supported merge-only Secret.update API.
A protected, ignored 0600 rollback copy of the old key is retained inside a 0700
private directory. No other key or launch/control setting changed. Deployment
`v64`, tag `ig-session-20261016-79f5dc8d-livestn`, loaded that same-account refresh;
the 45 runtime source hashes remained unchanged. Guarded API health passed.

At 2026-10-16T13:32:57Z a fresh posts-worker image verified the exact authorized
account and refreshed session, selected from env_json, and all 45 source hashes.
One profile warmup and one GraphQL page returned HTTP 200, 33 bravotv posts and
has_next_page=true without fetch/auth failure. This is an authentication check,
not a full backfill or a 33-post cap. SourceRequestGate, existing retry budgets and
per-request pacing remain authoritative for actual ingestion.

After that proof, one guarded transaction on verified trr-core cleared ONLY
blocked_reason on the exact unresolved listing/page:root target of run
31fc35b1-654e-4306-8d71-ca02c8bb770a. It required the blocked generation-3 run,
original intent hash/engine/version, attempts=3 of12, requests=1, original checkpoint,
no owner/lease, no other target and no receipts/evidence. All those counters and
checkpoint fields were retained; shared identity lanes were not changed. Supported
authenticated resume returned HTTP202, queued generation4 with original engine and
workflow version. New launches remain0; coordinator1 and replay-apply0. Actual
cross-page commits and full canary completion are pending observation.


Post-refresh `verify_modal_readiness.py --env main` passed with no missing functions,
secrets, or blocking/advisory probe failures. The supported resume was accepted at
13:35:10Z. At 13:36:29Z generation4 remained queued with no new source request or
commit yet. The existing dispatch sweep runs on its two-minute schedule; no duplicate
scheduler or manual legacy sweep was started for this observation.

### Bounded-window parity repair (2026-10-16)

The resumed canary committed its first page at 13:38Z: 33 selected bravotv posts,
one retained evidence page and receipt, and a durable next cursor. Each selected
post has detail, comment and media targets. This proves the first page committed;
it does not establish full-window discovery or downstream completion.

A source comparison then found a bounded-window parity gap. The maintained catalog
scraper stops after a consecutive older-post/date-page boundary. The new listing
activity filtered dates but always followed provider cursors to total profile
exhaustion, delaying downstream manifest sealing while scanning older history.
The supported cancel command paused the canary at generation5 at 13:40:59Z. A
read-only check found no active owners; its receipt, next cursor and all targets
remain preserved. No retry budget, identity lane or credential was reset.

The approved repair retains the native transport and uses a stricter version of
the existing date-boundary heuristic: validate the entire page, require at least
three candidate posts all older than date_start in nonincreasing timestamp order,
and exclude explicitly pinned posts. Missing pin flags retain legacy treatment;
malformed or conflicting explicit flags prevent heuristic termination. Any candidate
at or after date_start prevents a boundary stop, including posts after date_end.
Current retained source evidence has 33 nodes and no pin-named fields, so an
explicit-is_pinned-false requirement would not repair this live path. No maximum
number of profile pins is assumed.

This remains an ordering heuristic, not a provider guarantee that unseen pages
contain no newer post. Atomic cursor state and receipts distinguish the bounded
heuristic from actual provider exhaustion. Full-history runs retain explicit
provider pagination termination. The stricter whole-page rule avoids the legacy
within-page early break before later in-window posts. Focused persistence tests,
independent review, deployment and resume remain required before acceptance.

The repair's isolated PostgreSQL listing checks passed: 31 tests in 4.50 seconds,
plus Ruff, compilation and scoped diff checks. The predicate applies to every frozen
date_start, including recent_window/head_gap; full_history normalization clears
bounds. Tests cover duplicate identities, whole-page date validation, pin-state
ambiguity, ordering, lower/upper bounds, no cross-page streak, cursor loops, atomic
rollback, duplicate receipt delivery and cancel/resume fences and counters. The
frozen listing source SHA256 is
864cc112816f8afa708c2cfa324758586a1f0673e07a972728daf17e7fbdcaba;
the test file SHA256 is
45fd244dc81c65ab0f5fbb22d2a308db0ae17519e8468bfb24bf6011a9442da3.

Independent review of that frozen two-file batch found no actionable issues. It
verified hashes, full-page guards, fenced atomic metadata/receipt settlement and
full-history compatibility, and reused the passing focused checks. Parent accepts
this as the maintained heuristic with the explicitly recorded unseen-ordering
limit; live canary acceptance remains separate. Live provider verification again
confirmed admin-56995 / main / trr-backend-jobs before deployment.

### Bounded-window deployment and resume (2026-10-16T13:50Z)

Commit 7cb698c8 is deployed as Modal v65, tag ig-window-20261016-7cb698c8,
admin-56995 / main / trr-backend-jobs. The provider reports 7cb698c* because
unrelated dirty work remains preserved. Guarded cold-start API health returned 200
on attempt 1. Readiness passed with no missing functions/secrets or blocking/advisory
failures. A fresh posts-worker container verified all 45 runtime hashes and the
changed date-boundary helper; no source or database operations were made by that
smoke. Launch 0, coordinator 1, replay-apply 0 remain unchanged. v64 and its source
history remain available for rollback; the authorized `<ingestion-account>` credential did
not change during this deployment.

Supported resume with a new stable idempotency key returned 202, generation 6,
original DBOS engine/workflow version. At 13:50:49Z the run was running: the original
page receipt and 33 downstream target sets remained preserved, and the saved next
listing cursor had one active owner on its first attempt. No new launch or manual
scheduler wakeup was used. Receipt/evidence paths are
.artifacts/ingestion-rollout/{deployment-7cb698c8,readiness-7cb698c8,
deployed-worker-source-proof-7cb698c8}.json. Full-window and downstream completion
remain unproven at this observation.

At 13:54:29Z page two committed. The run retained 66 unique post targets per
selected downstream stage, two listing receipts and a durable third-page cursor.
A read-only lock trace during that commit found one progressing canonical writer
and run-row waiters, without a blocking root or observed cycle. The transaction
held the run lock across hundreds of synchronous canonical statements. No writer
was terminated and no receipt or counter was reset.

### Worker locality candidate — awaiting cost decision

Live metadata showed the fresh posts worker in Modal region ap (Azure), while the
TRR database pooler is aws-1-us-east-1. A lean worker reported ap-northeast; the API
container reported us-east (GCP). Five read-only SELECT 1 roundtrips on the same
database at 13:56:55–57Z measured median 199.47 ms from the posts container and
11.87 ms from the API container. This small cross-cloud sample is descriptive;
it is not a matched full-pipeline benchmark or a total-cost measurement. Protected
receipt: .artifacts/ingestion-rollout/region-latency-before.json.

The prepared source candidate adds only region="us-east" to the existing
run_social_posts_job, run_social_comments_job, run_social_media_job and
sweep_social_dispatch_queue decorators. AST comparison proved those four keywords
are the only code changes; Ruff/diff checks and independent review passed.
It keeps shared container caps, dispatch names, images, secrets, retry policies,
timeouts and schedules. It affects legacy placement on the shared functions too.
Dynamic Function.with_options was rejected because it creates an independently
scaled pool and the existing admission snapshot does not replace the shared cap.

The provider documents a 1.75x compute-rate multiplier for narrow regions such as
us-east, versus 1.15x for broad us. The candidate has NOT been committed/deployed;
a direct user cost decision is pending. No assumption is made that faster wall time
will reduce total cost. Current v65 canary execution continues under automatic
placement. If approved, use explicit rolling deployment: the provider documents
that active inputs finish on old containers while new containers take new work.
Preserve existing calls and reservations; do not redispatch them.

References: https://modal.com/docs/guide/region-selection and
https://modal.com/docs/guide/managing-deployments (checked 2026-10-16).

At 13:59:55Z the canary had three committed listing pages and 99 target identities
per selected downstream stage. Page four was queued, with no new block. The shared
region candidate remains undeployed pending the user's explicit rate-premium
choice. A thread heartbeat named "Instagram ingestion canary" (automation ID
instagram-ingestion-canary) now checks every 15 minutes. It observes the existing
run only, stays quiet for routine page increments, preserves launch 0, and reports
meaningful stage/terminal changes or failures. It cannot deploy the priced candidate
or treat silence as approval. The original full rollout acceptance remains open.

### US-East trial authorized (2026-10-16)

The user explicitly approved the US-East trial and its 75% compute-rate premium,
requested comparison of actual cost and speed, and directed continued work until
the complete ingestion pipeline is finished and optimized. This supersedes the
pending-cost-decision boundary above. The reviewed four-keyword candidate hash
still matches 2bbc1f3dbfff0f464c6392927044528699aa91e4032088f4627d3c1136649789.
Use a rolling deployment to preserve active inputs. Record actual provider usage
and billing separately from rate-based estimates, measure stage throughput and
canonical commit latency, and preserve the single-run admission/capacity policies.
The `<ingestion-account>`-only identity authorization and thereality.report session/cookie
prohibition remain unchanged.


### US-East deployed trial and initial measurements (2026-10-16T14:23Z)

Commit 77cba23c is deployed as Modal v66, tag ig-east-20261016-77cba23c,
using explicit rolling strategy in admin-56995 / main / trr-backend-jobs
(ap-DkLTRoSvqhbkGO7fHyHxrD). Deployment health returned 200 on attempt one;
target-specific readiness passed. A fresh posts worker in us-east/GCP and a
fresh lean worker in us-east/AWS matched all 45 runtime hashes. Five read-only
SELECT 1 calls measured medians of 11.53 ms and 1.68 ms respectively. The
previous posts worker measured 199.47 ms in AP/Azure. This is a small cross-cloud
sample, not a matched pipeline benchmark. Unchanged app functions may still run
elsewhere; no claim is made that every app container is in US East.

The unchanged generation-6 canary advanced to seven committed listing pages and
231 downstream targets per selected stage at 14:20:48Z. The earlier 17.5-minute
stall was abnormal. Its DBOS partition completed all 50 decisions and created the
next partition after the region change; replay/startup overhead is a plausible
cause, not a proven attribution. No manual wakeup, duplicate dispatch, reset or
new launch was used.

Two pagination targets before the move recorded 189.328 and 225.678 seconds from
source observation to receipt timestamp. The first four after the move recorded
13.410, 12.696, 10.489 and 11.737 seconds. These are different pages within the
same run; receipt timestamps are written inside the transaction, so these are
recorded processing intervals rather than exact transaction-commit wall times.
Details, comments, media and end-to-end completion remain unverified. Receipt:
.artifacts/ingestion-rollout/listing-timing-region-trial.json.

The supported Modal billing export succeeded for the exact app and environment.
The 12:00–13:00 UTC bucket reports USD 0.02664886; 13:00–14:00 UTC reports
USD 0.02987936 (CPU 0.02032690, memory 0.00470786, egress 0.00484460 in the
latter). These are actual app-hour metered charges before credits/allowances,
including unrelated functions and probes; they are not exact canary charges or
final invoice amounts. The 14:00–15:00 UTC mixed deployment hour is not yet closed.
The installed supported export cannot allocate cost per function or invocation.
Protected evidence: modal-billing-baseline-12-14utc.json,
deployed-east-source-latency-proof.json, deployment-77cba23c.json and
readiness-77cba23c.json under .artifacts/ingestion-rollout/.

The heartbeat now reflects the explicit premium approval and continued full
pipeline work. Expansion still requires the original acceptance gates, including
ten representative runs and 24 hours without accounting, ownership or completion
violations. Current launch 0, coordinator 1 and replay-apply 0 remain unchanged.


### Pipeline recovery and efficiency candidate (in review preparation)

The next candidate keeps current admission, ownership, lease, retry and stage
ordering policies. It batches downstream listing targets into one parameterized
statement per page, changes coordinator outbox polling to one second and engine
error reconciliation to five seconds, and leaves the 85/90-second deadlines and
50-decision partition limit unchanged. Thirty-three isolated PostgreSQL listing
cases and two fake-clock lifecycle cases passed. Real database blocking can still
delay polling; these intervals are not hard latency guarantees.

Comments recovery reconstructs only the current successful scan from retained
native evidence/page receipts, drains unfinished earlier-parent replies before
continuing the top cursor, and preserves prior observation timestamps. A
versioned native REST-list envelope requires explicit pagination proof; bare
lists remain unresolved. The opt-in maintained-fetcher seed leaves existing
callers unchanged. Focused comments/adapter/resume checks passed 86 cases, two
additional root-restart/coverage regressions passed, and six maintained-fetcher
cursor/reply compatibility cases passed.

The initial new mock-HTTP harness was invalid: six cases reached Instagram with
synthetic or empty fixture cookies because REST rebuilt httpx.AsyncClient outside
the patched factory. No real account credentials were resolved or extracted;
request count was not instrumented and remains unknown. That run is failed
harness evidence, not acceptance. Corrected direct-constructor, HTTP transport
and browser guards now fail closed outside explicit mock transport; corrected
checks passed.

Media inventory now reconciles matching hosted avatar proof without weakening
newer conflicting-observation guards. Streaming checks the local deadline on
every chunk and durable ownership at most one second apart, with forced checks
before requests, uploads and commit. A 50 MiB / 800-chunk test used nine durable
checks plus one counted request check, detected cancellation within one second,
and rejected a new generation after the final upload. Physical summaries count
unique object keys while keeping role metadata distinct. Forty affected media
cases passed, reusing 35 initial passes and rerunning only four repaired
integration failures plus the new optional snapshot-binding case. Replay accepts
independently recomputed physical totals or the exact historical v1 role totals;
it rejects conflicting duplicate-key metadata and hybrid totals. Sixteen focused
replay summary cases passed. Partial-hosting cache expansion remains deferred;
no incomplete target is declared complete from remote object existence.

A per-target expired-link refresh adapter and strict secret-backed credential
snapshot are still being integrated. The latter removes ambient resolver/file/
browser/identity-pool candidates from this rebuilt path. Deployment must first
pin the existing verified `<ingestion-account>` native user ID in the existing social
secret without changing cookies, then verify fresh runtime readback. No
thereality.report session or cookie is allowed. Current live v66 remains the
region-only baseline until this complete candidate passes consolidated review.


Credential deployment contract: `SOCIAL_INSTAGRAM_COOKIES_JSON`,
`TRR_INSTAGRAM_INGESTION_AUTH_USER_ID`, and
`TRR_INSTAGRAM_INGESTION_AUTH_USERNAME` belong together in `trr-social-auth`.
The pins name the already verified native user ID and authorized `<ingestion-account>`
username; missing, malformed or mismatched snapshots fail closed. The source
environment example contains no native ID or cookie. Secret preparation routes
both pins to the social secret; its affected split-routing regression passed.
This deployment uses a targeted merge of the two pins after a same-identity
preflight, not regeneration of social credentials. Old code ignores the additive
pins, so rollback preserves the verified cookie value.

The connected two-post local pipeline passed after reconciling stale assertions
with previously implemented exact-URL cover/original reuse: 27 physical objects,
separate cover/media manifest roles, three actual registry avatar objects and
matching canonical avatar references. It completed all eight selected targets
with eight receipts and preserved lost-ack/cancel/resume behavior. This is local
connected proof; actual canary downstream execution is still pending.


### Consolidated review repair hold (2026-10-16T14:38Z)

Independent review found three concrete defects: maintained fetchers could replace
the supplied identity through pools/warmup snapshots/response jars; native REST
list metadata could discard an explicit failure and permit false completion;
and an explicitly pending reply lane with an unsupported REST cursor could skip
requests when counts already matched. Two prerequisites also remain: preserve
the maintained static detail headers, and validate the actual deployed cookie
mapping with the exact hash-bound new validator before installing identity pins.
All five are being repaired as one batch; subsequent review checks those fixes
and any defects introduced by them, without repeating passing unrelated checks.

The existing canary was intentionally paused through the supported cancel command
at 14:37:35Z, generation 7, before downstream source execution. Read-only state at
14:37:53Z confirms 33 committed listing pages, 1,089 target identities per selected
downstream stage, 33 receipts/evidence records, one saved next-page target and no
active owners. The three downstream stages have zero attempts and requests.
Resume this SAME run only after the identity/completion repair batch is accepted,
deployed and runtime-verified; use a new stable resume idempotency key. Do not
reset cursors, completed outcomes, counters or budgets. This intentional hold is
not normal completion or a new source failure. The region-only trial remains v66;
its first-hour billing bucket will include this hold and other app activity.

At 14:44:04Z the reviewed pin installer verified the exact candidate validator
against the current v66 runtime cookie mapping and the already verified
`<ingestion-account>` identity, then added only the two identity-pin keys to
`admin-56995/main/trr-social-auth`. Cookie values were not changed. The receipt
records successful same-account and exact-validator preflights; fresh deployed
runtime readback remains pending. The first invocation lacked repository
PYTHONPATH and exited before provider code; the corrected invocation succeeded.
The canary stays paused until the source repair batch passes review and the new
runtime is verified. Both operational scripts passed independent source review;
post-deploy proof will cover all 45 prior paths plus five additions and check
loaded module origins against those hashes.

The repair batch now pins identity through the maintained scraper, posts and
comments transports, raw state, response cookies, outgoing jars and browser
requests. Legacy defaults remain unchanged; strict scraper mode disables only
internal adapter retries/redirects that bypass the caller's identity/request
checks. Outer counted retry, pacing and cooldown policies remain in place.
The initial API batch passed 77 cases and found a default-referrer slash mismatch;
its repair and affected/new checks passed 24 cases. The comments batch passed
105 cases after narrowing strict JSON auth classification to request metadata,
so ordinary comment text cannot become an auth failure. Classified auth/rate
metadata still reaches failure/cooldown handling. The connected pipeline passed
one test in 4.88 seconds after those changes.

Fix-only review closed native-list failure, pending-reply and static-header
findings, and identified one remaining edge of the original browser identity
finding: root-URL cookie inspection missed API-path cookies and could mask a
missing actual browser pair. The bounded repair now queries the exact request
URL, rejects conflicting duplicate entries and requires both actual browser
identity cookies before evaluation. Six affected browser cases passed in 0.76
seconds. No unrelated suite or connected pipeline was repeated for this isolated
browser path. Final inspection is limited to that remaining finding; deployment
and canary resume still require acceptance and fresh runtime proof.

Final limited review confirms all five findings closed at source level, including
the six browser-identity regressions. Parent accepts this candidate for a scoped
commit and one updated resource measurement. Live completion and post-deploy
credential/source proof remain separate gates; the canary is still paused.

### Updated fixed-fixture measurement and bounded rollout decision

Exact commit `9e2349e2786db583c6811903663437af9e7fec47` passed one updated
candidate measurement with the reviewed compatibility harness. All 38 activities
committed; common fields, role bytes/dimensions, 147 physical objects, three
avatar references and no-orphan assertions passed. Adjusted SQL was 12,462
through business completion and 12,798 through full lifecycle, versus the prior
candidate's 15,872 and 18,222. Peak sampled sessions stayed five; all 40 pool
receipts preserved the same caps. SQL errors and final workload sessions were
zero, and private Postgres stopped. The unchanged legacy evidence remains
6,089 / 6,102 statements with peak two sessions.

Business completion was 74.5535 seconds and full lifecycle 89.1523 seconds,
versus prior candidate 57.7754 / 86.8038 and legacy 69.6436 / 70.0430. New business
time is about 7.05% above the frozen legacy baseline; full lifecycle is about
27.28% above legacy and 2.71% above the previously accepted candidate. Against the
prior candidate, 13.9871 of the 16.7781 added completion seconds were inside
native dispatches: fixture/import +8.2041, activity +3.7104 and wrapper/subprocess
+2.0726. Measured child-start gaps added only 1.6287 seconds. These historical
single trials cannot separate host load from code effects. The main loop changed
from 0.1 to 1.0 seconds; the separate lease monitor remains 0.25 seconds. No claim
of faster local completion or causal attribution to that poll change is made.

Statement deltas reconcile by category with no new unexplained query/session
increase. Completion reduction of 3,410 comprises transaction/session -2,657,
owner/accounting -154, coordinator polls -826, registry +12 and known SDK shapes
+215. Full reduction of 5,424 comprises -3,978, -154, -1,266, +12 and -38 in the
same categories. Network counter remains 427: 40 Instagram, 60 CDN, 147 PUT and
180 HEAD. This fixture demonstrates no avatar network reduction; no such saving
is claimed. Registry proof freshness remains strict.

Parent accepts the new measurement for the existing bounded canary under the
frozen-baseline tolerance and previously accepted durable/session/lifecycle
overhead, with timing uncertainty retained. This is not wider rollout or a
production speed/cost claim. US East deployment keeps launch disabled and must
pass fresh 50-file source, credential and worker checks before generation-7
canary resume. Actual provider billing, live all-stage completion and the ten-run/
24-hour expansion gate remain pending. Evidence and category equations are in
`.artifacts/ingestion-engine-eval-full-17f978d/optimized-candidate-report.md`.

### v67 deployment and generation-8 resume

Modal `admin-56995/main/trr-backend-jobs` deployed v67 at 14:54:07Z with tag
`ig-recovery-20261016-9e2349e2`; provider revision is `9e2349e*` because unrelated
working-tree changes remain present. The exact 50-file ingestion manifest matches
fresh containers. Deployment finished in 13.445 seconds; the cold-start API health
check returned 200 on attempt one. Targeted readiness reports core ready and ready,
with no missing secret/function and a ready control-plane provider probe.

Actual posts worker `ta-01M52KC482SMZPWV1D11EGCX7R` was started with an incomplete
reservation and rejected it at the input boundary before source/database work.
Its subsequent read-only proof verified all 50 hashes, loaded-module origins,
Patchright availability, US East placement, valid `<ingestion-account>` snapshot/identity
guard, and flags launch=0, coordinator=1, replay-apply=0. Other unconstrained app
functions can still run in other regions; this is not an all-app placement claim.
The readback helper gained only an optional container-ID filter to avoid repeating
unchanged proofs. This smoke proves startup/input rejection and configuration,
not live Instagram authentication or successful downstream completion.

The supported resume API returned HTTP 202 for the SAME canary
`31fc35b1-654e-4306-8d71-ca02c8bb770a`, generation 8, queued, DBOS
`instagram-ingestion-v1`, using new stable idempotency key
`instagram-ingestion-20261016-pinned-identity-repair-resume-01`.
The generation-7 review hold is now resolved; normal coordinator recovery may
continue this run. Preserve all saved listing pages, targets, cursors and budgets.
New-run routing stays disabled. The previous v66 code and unchanged cookie value
remain rollback material; never remove additive schema or transfer this run's
engine/version. Deployment, readiness, source/identity proofs and command receipt
are preserved in `.artifacts/ingestion-rollout/`.

### Public-first routing regression hold — supersedes generation-8 continuation

The user's follow-up recalled the earlier no-login implementation. Current source
and Git history confirm that the legacy posts runner defaults to public_first,
the comments runner defaults to public_first/public_relay, and public_probe.py
uses empty cookies for listing, permalink details and comments with replies.
The initial durable composition in ac4f95be bypassed those mode branches and
required validated cookies; 9e2349e2 tightened that requirement to an explicit
identity. This is a rebuild routing regression, not evidence that Instagram
universally requires login. Prior sampled public progress does not prove all-stage,
full-account completeness; retain truthful stage-specific completion checks.

The supported cancel action returned 202 at 15:03:06Z, preserving this SAME run
as cancelled generation 9. Read-only state at 15:03:17Z shows 66 committed listing
pages, 2,178 targets for each downstream stage, 66 receipts/evidence records,
one saved unresolved next page, zero active owners, and zero downstream attempts
or requests. DO NOT resume the authenticated canary automatically. Public-mode
restoration design is in progress; preserve authenticated cursors and budgets,
and establish adapter/version compatibility before any source-mode transition.
No public source probe, cookie/proxy action, or mode-switch code change has yet
been performed for this correction. Helpers are design/read-only only.

Parent recommendation in response to 'which one should we use?': native public-first
is the default, durable orchestration remains, and authenticated mode is a separate
explicit option for a demonstrated gap. Do not silently fall back from public mode
to account cookies. Apify's own public documentation describes logged-out data and
comments/replies; it does not establish complete TRR coverage or authorize a paid
provider integration. Await the settled restoration contract before implementation.

### Public restoration implementation — authenticated run remains paused

The parent settled the repair contract and released implementation within the
original ingestion-rebuild scope. New launches freeze a source profile containing
access mode, exact listing/detail/comment families, contract version and adapter
version. API composition resolves the maintained public-first default once at
launch. Workers use the frozen profile. Unknown configuration fails closed.
Historical untagged runs retain authenticated interpretation and byte-equivalent
intent serialization/digests; their cursors cannot become public cursors.

The parent implemented the immutable intent/command seam. Its 43 focused domain,
command and new profile cases passed using isolated PostgreSQL. This includes
idempotent launch replay after policy changes, invalid/mixed profile rejection,
and preserved historical payload/digest behavior. Transport integration is still
in progress: API owner handles public listing/details/media refresh, comments owner
handles durable Relay parent/child resume, and workflow owner handles public-access
cooldown without creating an authenticated-account block. No live source requests,
new canary, deployment, credential change, or authenticated-run resume occurred.

Public requests share the existing anonymous pressure lane and unchanged caps.
Every explicit source operation must be gated and observed. Public adapters start
without credentials and reject authenticated cookies before subsequent sends;
attempt-local anonymous state remains ephemeral. Server Retry-After is preserved,
including fail-closed overflow behavior. Previously blocked anonymous rows remain
blocked pending explicit reconciliation; this repair does not reset them.

Actual Modal usage for the exact app during 14:00–15:00 UTC was $0.06690604 before
credits/allowances: CPU $0.03424182, memory $0.00650723, network egress $0.02615699.
The 13:00–14:00 baseline was $0.02987936. These are whole-app hourly measurements,
not isolated canary costs: the 14:00 hour spans the prior region, US East rollout,
pauses, repair deployment and other app activity. They do not establish a causal
region cost increase or end-to-end performance result. Original provider response
is retained in .artifacts/ingestion-rollout/modal-billing-trial-14-15utc.json.

The public gate batch passed 53 isolated PostgreSQL cases. It preserves the
shared 180-request/660-second cap, immediate public-access cooldown, existing
blocks, cancellation and probe fencing. A later shorter public failure cannot
shorten an earlier server Retry-After. Authenticated defaults remain unchanged.

A single provider billing refresh at 15:12:20 UTC confirmed both completed hours.
The provider's reported totals are $0.02987935 (13 UTC) and $0.06690603 (14 UTC),
each $0.00000001 below the separately summed displayed resources due to rounding.
The exact provider total difference is $0.03702668; no per-canary allocation or
causal region comparison is claimed. Explicit admin-56995 profile and live
Workspace.name were checked; the unrelated global active profile was unchanged.
Evidence: .artifacts/ingestion-rollout/modal-billing-complete-hours-20261016T151220Z.json.

Read-only live preparation at 15:12:46 UTC verified the bound trr-core project,
found no existing anonymous request-lane row, and confirmed the preserved
historical run is cancelled at generation 9. No reconciliation, source request,
or state mutation was performed. Evidence is retained in
.artifacts/ingestion-rollout/public-lane-readiness-151246.json.

Prepared .artifacts/ingestion-rollout/verify-public-runtime.py for the later
source/runtime gate. It verifies deployed source hashes and the public frozen
contract without reading a credential snapshot. Only local compilation has run;
this is preparation, not deployed-runtime proof.

### Connected public pipeline and rollout hold prerequisite

The connected isolated-PostgreSQL public case passed through
composition.build_worker with maintained listing, permalink, Relay and profile
parsers, real durable/canonical/media processing, mocked HTTP sends and fake
object storage. It committed and sealed all four stages with zero unresolved
items, four target receipts, seven evidence rows, a parent/reply pair, three
avatars and fourteen stored objects. Seven source HTTP operations and four media
downloads were counted. Frozen public routing held despite an authenticated
process setting. This is fixture integration evidence, not live completeness.

That case found a strict nested API-intent mismatch. CommandService now validates
the full internal intent and projects only source_profile out of progress/recent
responses. Stored intent, hashes and internal worker/query routing are unchanged;
the existing API schema and generated client remain valid. Two focused public/auth
projection cases and the connected case passed after the repair.

The existing one-canary guard correctly counts the cancelled authenticated run as
resumable, so it still blocks a separate public launch. A separate additive hold
batch is now being implemented: a nullable execution hold on an already-cancelled
run, a database constraint preventing held runs from becoming runnable, explicit
resume/retry rejection, no executable progress actions, and exclusion of held
cancelled rows only from the active count. Held rows still consume total budget.
The exact existing run, generation, intent, attempts, cursors and evidence remain
preserved. No hold has been applied and no new run has been launched.

After a public run exists, rollback must disable new routing and safely pause
work while retaining profile-compatible API/workers. The old v67 source cannot
parse new public intent; do not claim it is a complete code rollback for those
runs. The hold database constraint independently prevents older code from
reactivating the preserved authenticated run. Keep additive schema and hold data.

The durable-hold batch passed independent review with no findings. Its 23 focused
PostgreSQL cases establish state preservation, idempotence, typed progress,
one-run counting and the old-SQL constraint boundary. The additive migration
20261016070400_instagram_ingestion_run_holds.sql (SHA256
7c0a1f8220cfb56189b1fe796c5c88ef5122a633eacb01d305a0022474dbf32a)
was applied through the verified repo-scoped Supabase MCP to trr-core. The provider
recorded version 20261016153330, name instagram_ingestion_run_holds; this generated
provider version differs from the local source filename and is recorded explicitly.
Post-apply inspection confirmed the nullable text column and held-implies-cancelled
constraint, zero held runs, and the existing canary still cancelled at generation
9 with a null hold. No run hold or new launch has occurred. Receipt:
.artifacts/ingestion-rollout/run-hold-migration-receipt.json.

The new local migration was then renamed to the provider's exact recorded version:
20261016153330_instagram_ingestion_run_holds.sql. The isolated fixture path was
updated accordingly. SQL bytes and the reviewed SHA256 are unchanged; no migration
was reapplied. This keeps future migration discovery aligned with provider history.

### Public repair review closeout

The first consolidated review found four transport gaps: broad HTML login-word
classification, an incompatible environment-selected Relay client, dropped server
Retry-After values on public-access failures, and an untyped Relay cookie-identity
rejection that delayed shared backoff. The owners repaired the complete batch.
Targeted evidence passed: 82 public response/listing/detail/avatar cases, and 54
public Relay cases including nine real-gate boundary cases. The fixes-only review
closed those four findings.

That review found one new parser edge: negative numeric Retry-After was clamped
to zero. The parent reassessed the conversion contract rather than starting a
third broad review cycle. Numeric delays must be nonnegative; negative numbers
and negative numeric overflow now return invalid/None, preserving the gate's
900-second default. A valid expired HTTP date remains zero. Twenty targeted
parser and real parser-to-gate cases passed, including a 403 negative-header
response that enters 900-second backoff and prevents the next request. Ruff
passed after correcting one new test import-order finding. This closes the sole
remaining source-review issue; unchanged earlier evidence was reused.

The existing deployed API returned HTTP 200 after the additive hold migration,
with the original run still cancelled at generation 9, 66 committed listing
pages, 2,178 unresolved targets in each downstream stage and zero downstream
attempts/requests. Its intent hash and last-update time remain unchanged.
No public run or actual hold has yet been applied. Source acceptance is separate
from the pending deployed-source proof, actual hold and new public canary.

### Public deployment and preserved-run hold — 2026-10-16 15:52 UTC

Source commit 1b648130 was deployed to admin-56995/main/trr-backend-jobs as
v68, tag ig-public-20261016-1b648130. Deployment readiness and health passed.
All 53 expected source hashes and loaded module origins matched; the actual
posts worker also matched, with US East placement and Patchright available.
Unrelated dirty source was preserved and is identified by the deployment's dirty
revision marker. No push was performed.

The reviewed transactional hold was applied to authenticated run
31fc35b1-654e-4306-8d71-ca02c8bb770a at 15:47:03 UTC with reason
public_transport_rebuild_preserved. State remains cancelled at generation 9.
All other run fields, intent hash, target/discovery/receipt/evidence counts and
state digests were unchanged. API progress returned no executable actions.
This run must remain held; its budgets and cursors are not transferred.

The runtime secret enabled new-run routing only; the existing bravotv/2026
scope, total limit of ten and single-active-run guard remain. Configuration-only
deployment ig-public-canary-20261016-1b648130 passed readiness and health. The
first API preflight still reported routing disabled, so no launch was sent.
At 15:52 UTC, both current inspected containers reported flags enabled=1,
coordinator=1, replay_apply=0, public launch profile and all 53 matching hashes.
API preflight is being rechecked before the idempotent public launch.
Evidence: deployed-public-post-enable-proof.json, authenticated-run-hold-receipt.json,
public-canary-enable-flags.json, and deployment-public-canary-1b648130.json in
.artifacts/ingestion-rollout/. Source checks and deployment do not establish
pipeline completion; downstream live stages and sustained acceptance remain.

### Public canary started — 2026-10-16 15:54 UTC

Fresh API preflight returned enabled=true for bravotv, enabled=false for nbc and
replay_apply_enabled=false. The public launch returned HTTP 202 at 15:53:59 UTC,
run bcc7411d-085d-4efc-be77-7d8e87303eb6, generation 1. Read-only database
inspection verified the frozen public profile and unchanged held authenticated
run. The public run began automatically. Its first listing attempt made exactly
one source request, received public_access_backoff, saved no source evidence and
released ownership. Its next attempt is 16:09:08 UTC; do not reset the cooldown.
No downstream targets or receipts exist yet. The existing 15-minute follow-up
now tracks this public run and explicitly prohibits resuming the held run.

Evidence: public-canary-launch-request.json, public-canary-launch-result.json and
public-canary-first-state.json. The transient disabled API response during the
rolling transition resolved without another secret change or deployment.

The first source failure retained only the normalized backoff code, which does
not distinguish HTTP rejection from a login redirect. A narrow diagnostic adds
fixed-field warnings for status, controlled error code, method and endpoint
category. It logs no URL, account, query, body, header, cookie or exception text.
Four focused synthetic cases passed, including sentinel exclusion and logging
handler failure without changed accounting. Parent reviewed the complete two-file
diff; transport, retry, credential and cooldown behavior are unchanged. This
diagnostic will observe the next normally scheduled attempt, not an extra probe.

Diagnostic commit 049f4648 was rolled out to the same verified Modal main target
with tag ig-public-diag-20261016-049f4648. Deployment readiness and health passed.
At 15:59 UTC, all six inspected containers matched the 53-file manifest, loaded
module origins, public profile and enabled/coordinator/replay flags 1/1/0. This
is deployed-source proof, not evidence of the next source attempt or a successful
worker scrape. Current evidence is deployed-public-diag-proof-049f4648.json,
deployment-public-diag-049f4648.json and deploy-public-diag-049f4648.log.
Next action: observe the scheduled attempt after 16:09:08 UTC and inspect only
its controlled instagram_public_request_failed diagnostic if it fails. Preserve
the cooldown and budgets. Retrieve the completed 15:00–16:00 UTC billing hour
once available, reporting missing provider data as unavailable rather than zero.

Observation at 16:01:48 UTC: public run remains generation 1 with one attempt,
one request, no owner and the unchanged 16:09:08 UTC cooldown. The old run is
still held at cancelled generation 9. No source request was made by this check.
The completed 15:00–16:00 UTC Modal app-hour report was available: total
$0.04138597, CPU $0.02613144, memory $0.00616577, egress $0.00908876. This is
metered whole-app cost before credits, covering deployments and mixed work;
it is not per-canary cost or a matched region comparison. Evidence:
public-canary-observation-1601.json and modal-billing-15-16utc.json. No stage or
acceptance gate changed; the next scheduled source attempt remains pending.

### Public response diagnostic channel repair — 2026-10-16 16:20 UTC

The second public attempt at 16:09 UTC made one request and again recorded
public_access_backoff. Totals are two attempts/two requests, no evidence or
completed listing page; next attempt is 16:24:17 UTC. The old run remains held.
The expected diagnostic was absent because run_ingestion_activity deliberately
starts worker.main with stderr=DEVNULL. The first logging-only change therefore
did not establish deployed observability; its passing logger tests missed that
subprocess boundary. No additional source requests were made to diagnose this.

The repair passes one fixed-field diagnostic through the existing bounded JSON
result, validates it independently at worker and runtime boundaries, and keeps
raw stderr discarded. Source requests, cooldowns, ownership and persistence are
unchanged. A source-only comparison found no missing maintained bootstrap
headers, anonymous cookie handling or GraphQL context. Fresh per-activity
sessions differ from legacy pagination reuse but do not explain the first
bootstrap rejection without response evidence. No protocol workaround is justified
by the two normalized failures alone.

Fourteen affected cases passed in 1.79 seconds, including a real subprocess path
from synthetic HTTP 403 through ObservedPublicSession, worker.main and runtime.
They verify no-diagnostic output parity, rejection of malicious child fields,
fixed-type allowlists, first-event bounds and reset/copy isolation. Ruff, AST and
diff checks passed; parent reviewed the complete six-file change with no findings.
Existing unchanged transport evidence was reused. Runtime rollout is next.

### Confirmed public login redirect and decision hold — 2026-10-16 16:26 UTC

Diagnostic channel c0d167c5 deployed as Modal v71 at 16:22:36 UTC with tag
ig-public-channel-20261016-c0d167c5. Readiness and API health passed; all six
inspected containers matched 54 source hashes and loaded origins, public profile
and flags 1/1/0. No extra source request was used for deployment verification.

The third scheduled public attempt returned a validated diagnostic through the
actual worker result: HTTP 302, redirect_login, GET, profile_bootstrap. It made
one request, taking totals to three attempts/three requests. There are no listing
pages, source evidence or downstream targets. This confirms that this cloud
worker's public profile request was redirected to login; it does not establish
that all public access is impossible or that authenticated access is required
for every Instagram dataset.

The supported cancel command returned HTTP 202 at 16:25:54 UTC. Database readback
confirmed public run bcc7411d-085d-4efc-be77-7d8e87303eb6 cancelled generation 2,
three attempts/requests preserved, no owner, and its 16:39:26 UTC cooldown retained.
The old authenticated run remains held at cancelled generation 9. Do not resume
either automatically or transfer state between them.

The user was asked to choose between a separate authenticated run using the
already-authorized `<ingestion-account>` identity and keeping public-only ingestion while
assessing an alternative provider without paid usage. This is pending; silence
is not approval. The existing follow-up was updated to honor this decision hold.
No provider enrollment, paid call, authentication fallback or source workaround
was performed. Full pipeline completion remains blocked on source access.
Evidence: public-canary-attempt3.json, public-login-pause.json,
public-login-pause-verified.json, deployed-public-channel-proof-c0d167c5.json and
deploy-public-channel-c0d167c5.log in .artifacts/ingestion-rollout/.


### Public-only user direction and local source comparison — 2026-10-16 17:10 UTC

The user explicitly requested fixing public access without login and providing
results. This supersedes the earlier pending access-path choice. Do not resume
the authenticated run or add an authenticated fallback.

Bounded local diagnostics used no credentials or proxy and did not write run,
evidence, receipt or canonical tables. Source requests after the initial one-GET
comparison used the existing shared anonymous request/cooldown gate:

- Local profile bootstrap returned HTTP 200 with public runtime context.
- The maintained public listing adapter returned 33 posts in two source requests.
  Twelve observed sample rows were saved; pagination remained open, so this is
  neither all 2026 posts nor a completed ingestion run.
- The permalink returned HTTP 200 but no native media payload. A shape-only
  inspection found 33 structured payloads and no detail/root-query media data.
- The maintained standalone shortcode query returned HTTP 403. The shared public
  lane recorded `public_access_backoff` and cooldown through 17:18:10 UTC. Do not
  clear or shorten it. No further source request was sent during the cooldown.

Evidence is in `.artifacts/ingestion-rollout/public-local-listing-probe-result.json`,
`public-local-detail-result.json`, `public-local-detail-shape.json`, and
`public-local-graphql-detail-result.json`. The latter records a callback wrapper
class; the controlled request diagnostic identifies the underlying HTTP 403.
The result sample CSV is `public-instagram-observed-posts.csv` (12 rows).

The source repair adds the existing detail completeness validator to listing
materialization, retaining original listing observation time and provenance.
It does not relax freshness, hidden-metric, force or carousel checks. Eight
focused offline tests pass. The 33 existing listing/atomicity tests also pass
against their isolated local PostgreSQL fixture (explicit test marker required
by the repository connection guard). Deployment and live acceptance are pending.

A separate coupled change is implementing durable run-specific local placement
and a bounded serial local driver. Placement must remain separate from frozen
source intent, forbid cloud fallback after a local crash, and preserve shared
admission, ownership, counters, receipts and public pacing. No placement/schema
change has been applied to the live database. Local execution is outside Modal's
physical function cap; shared logical admission remains required.

Both existing runs remain paused. Do not resume the public run until the local
placement guard and changed path are reviewed, deployed and verified. Comments,
replies, media, full pipeline, ten representative runs and the 24-hour observation
remain unverified. The earlier cloud-only failure is not proof that every public
access path is unavailable.

### Public results and bounded recovery candidate — 2026-10-16 17:33 UTC

Further local anonymous checks used the shared source gate, with no login, proxy,
canonical writes or run resume:

- All 33 returned listing rows satisfy the existing detail completeness validator.
  `public-instagram-observed-posts-33.csv` contains the observed captions, dates,
  metrics and media types. Listing pagination remains open.
- The three-request comment diagnostic retrieved 26 top-level comments and ended
  the top-level lane. Native parent reply counts were null; no child request was
  sent. This is not a fair full-coverage comparison with an older production run.
  The maintained older public Relay path also skips these unknown-count parents
  by default. Older live coverage has not been established by this comparison.
- Media delivery failed because evidence sanitization removed four native CDN
  delivery parameters. The bounded fix preserves `_nc_ad`, `_nc_cid`, `_nc_ss`
  and `ig_cache_key` only on approved Instagram/Facebook CDN hosts. Authentication
  fields remain excluded. Eleven focused tests passed. A fresh anonymous check
  confirmed the sanitized URL equals the original and downloads HTTP 200:
  350844-byte JPEG, 1216 by 1504, SHA-256
  `7b33ba6752d16ad9a23c3f5b12e0b5f256c2b1862eaf13af37493427759c245c`.

The unknown-reply candidate now probes null-count parents while preserving null
in observation state. Completion requires native query-bound child-root through
terminal-page proof; explicit leaf proof is separate from count coercion. Twelve
new and twelve affected legacy tests passed; unchanged earlier checks are reused.
Independent review precedes a 40-request/300-second anonymous diagnostic.

The placement review found two recovery defects: old sending reservations could
block admission after safe recovery, and a stalled lease query could bypass abort.
The implementer is repairing both in one batch, with real admission and stalled
query/child-reaping fixtures. No live placement migration or local resume has been
performed. Deployment must precede the additive placement column; old cloud code
must not be permitted to execute a locally placed run.

Evidence is under `.artifacts/ingestion-rollout/`: public-local-comments-result.json,
public-local-listing-validation-result.json, public-local-original-media-result.json,
public-local-sanitized-media-result.json, and their sanitized source evidence.
Full pipeline completion and the original representative-run/observation gates
remain open. Both existing runs are still paused.

### Complete comment-record scope and live reply attempt — 2026-10-16 17:38 UTC

The user reaffirmed that completion includes comments, replies, comment media,
usernames, comment likes and commenter profiles. Embedded author fields and full
public profile enrichment must be assessed separately; do not call either
complete solely because comment pagination completed. No login remains required.

The reviewed repair's bounded anonymous probe admitted five sends, retrieved
42 top-level comments over three terminally exhausted top-level pages, then
received `rate_limited` on the first reply attempt. The shared lane retained its
cooldown through 17:49:38.518628 UTC. No retry was sent during that cooldown.
This is partial observation, not completed reply coverage or canonical ingestion.

The 42-row export includes usernames, user IDs and avatar URLs for all rows,
439 total observed comment likes, and one comment with source media URLs.
It does not prove hosted comment media, hosted avatars or full profile metadata.
Evidence: `public-local-repaired-comments-result.json`, the three sanitized
`public-local-repaired-comment-page-*.json` files, and
`public-instagram-observed-comments-42.csv` under `.artifacts/ingestion-rollout/`.

Both review batches are now resolved. Placement fixes passed 12 affected checks
and fixes-only review. Comment fixes passed 10 affected cases and fixes-only
review: any positive conflicting reply-count alias remains unsupported, and
strict public absent/null counts are not synthesized as zero. Existing unchanged
checks were reused. App contract/control alignment is in progress.

### Guarded placement deployed — 2026-10-16 17:40 UTC

Source `2ce08802` deployed to admin-56995/main/trr-backend-jobs as Modal v72,
tag `ig-public-local-20261016-2ce08802`. Health and explicit-main readiness passed.
All six inspected current containers matched 55 relevant source hashes, loaded
origins and the configured public launch profile. No source request was used by
those deployment checks. Unrelated dirty checkout changes remain preserved;
provider commit metadata consequently carries its dirty marker.

After that verification, the additive placement migration applied to trr-core
(vwxfvzutyufrkhfgoeaa) as provider version `20261016173915`. Local migration filename
and its test reference were aligned with that recorded version without changing
SQL bytes (SHA256 c3e3497a862bb09ee115289c65f07a13779e79a8d4d783f5d308cf5854498ad1).
Both existing runs still read cancelled with original generation/intent hashes
and default placement modal. No run was resumed by this migration.

The app placement controls and matching generated contract are committed as
`773eab6`: local resume/retry controls are hidden, cancellation remains available.
Sixteen focused app tests, source and fixture type checks, lint and deterministic
contract checks passed. Production app delivery/live UI proof remains pending.
A separate newly found GIF-only DTO omission is being repaired; full public
commenter-profile enrichment is under design. These are not claimed deployed.

The GIF-only transport repair is source-verified: explicit supported GIF, image,
video and sticker attachments now keep otherwise empty-text comments in the DTO
and fresh reply hydration. Empty placeholders, author-only URLs and unsafe URL
schemes remain rejected. Twelve focused cases passed, including native observer
through DTO and interrupted reply pagination. Parent diff review found no issue.
Comment media still requires separate hosted-file verification after cooldown.

### Public local execution and app acceptance — 2026-10-16 17:55 UTC

Backend d24b16fb (GIF-only attachments and canonical comment permalink fallback)
was deployed with tag ig-comment-media-20261016-d24b16fb. Health, explicit-main
readiness and 55 relevant hashes in five current containers passed. Provider
version number has not yet been independently recorded for that tag.

App 773eab6 is now promoted to trr-app.vercel.app, deployment
 dpl_3xUuchVT3jtTRYe9b6xKX7SiSpLW. Exact TRR project/team/source and READY state
passed. Authenticated capability and progress reads returned 200. Native Chrome
in the verified TRR profile displayed the durable run, committed listing count,
33 pending targets per downstream stage, cancelled generation 4, and the local
resume/retry instruction with no cloud resume/retry controls. The unrelated
legacy account-summary panel reported temporarily unavailable; durable progress
loaded correctly. Receipt: workspace output/releases/instagram-ingestion-app-773eab6.
Rollback remains the previous READY 964e9ce deployment.

The first immutable local-driver attempt returned Conflict while the shared
coordinator lease was held; the run remained cancelled generation 2/modal. After
waiting for lease release, the same reviewed release acquired the lease and
committed one public listing page. No cookie, login or alternate provider was
used. It created 33 post-details, 33 comments and 33 media targets and preserved
the next listing cursor. The bounded finally-cancel left public run bcc7411d
cancelled generation 4, placement local, workflow instagram-ingestion-v1, original
intent hash unchanged, all manifests unsealed and no target owners. Listing
cumulative accounting is four attempts/five requests including earlier failures;
these are not counts for only the successful local attempt.
Evidence: local-public-driver-second-result.json and
local-public-after-first-page.json under .artifacts/ingestion-rollout.

Do not resume the old v1 run automatically while profile enrichment is being
integrated. Parent will verify/deploy v2, then explicitly adopt this exact cancelled
local run through the guarded version-adoption operation. Do not rewrite intent,
generation or historical evidence. Both existing runs remain preserved. The old
authenticated run is still held and must not be resumed.

The authorized profile feature is in progress: public native-identity-bound
profiles, deduplicated media-stage profile targets from observed comment pages,
source-time-safe persistence, and a generation-fenced media discovery finalizer.
No relationship graph or profile timeline collection is included. Optional public
fields remain unknown when absent. Avatar version-cache batch passed 97 tests,
plus three affected checks after the final later-slot adjustment; it is source-only
and awaits the integrated review/deployment. Full replies, hosted comment media,
full commenter profiles, representative-run acceptance and 24-hour observation
remain open. One listing page does not establish full pipeline completion.

Provider history read at 17:57:57 UTC confirms the d24b16fb deployment is v73,
tag ig-comment-media-20261016-d24b16fb, deployed 17:44:52 UTC. Provider CLI revision
is d24b16f*; runtime file hashes supply the separate source-content proof.
Latest complete-hour whole-app metered cost before credits: 15–16 UTC $0.04138597
(unchanged after refresh), 16–17 UTC $0.02860791: CPU $0.02088748, memory
$0.00542487, network egress $0.00229556. The total for those two hours is
$0.06999388. This includes all trr-backend-jobs work and cannot establish the
public canary cost or a matched US East cost/speed improvement. The in-progress
17–18 hour was excluded. Evidence: modal-billing-complete-hours-20261016T175757Z.json
and modal-deployment-history-d24b16fb-20261016T175757Z.json in the rollout artifacts.

### Public child-reply diagnostic — 2026-10-16 18:00 UTC

A reviewed immutable-d24 diagnostic reconstructed the 42 retained parent lanes
with top pagination exhausted and no top refetch. All native reply counts are
unknown; the selected first pending parent was not claimed to have replies.
The public permalink bootstrap returned HTTP 200. The sole child GraphQL POST
also returned HTTP 200 but the maintained native failure classifier reported
rate_limited. Exactly two sends were admitted, no native child page was produced,
and zero replies were retrieved. The shared public gate now retains cooldown
until 18:14:57.189139 UTC. Do not repeat this unchanged request after two native
child-rate-limit observations; diagnose source classification/request shape first.
No cookies, proxies, source-control bypasses, run/canonical writes or new provider
were used. Existing public run remains deliberately cancelled/local generation4.
Artifact public-local-child-only-d24b16fb-20261016T175954Z-result.json records bounded
response metadata; checkpoint timestamps reconstructed from old scan start are
explicitly diagnostic-only and must not be committed canonically.

The canonical legacy profile snapshot independently returned HTTP504 after60.23s
with UPSTREAM_TIMEOUT; a single bounded request confirmed the timeout. Its null
summary fallback displays zero counters, not evidence of deleted data. That
legacy app route/page is unchanged from964e9ce to773eab6; the durable v2 progress
request and new local controls loaded correctly. Narrow read-only backend cause
diagnosis continues without repeating the expensive GET. Receipt is workspace
output/releases/instagram-ingestion-app-773eab6/canonical-legacy-summary-diagnostic.json.

### Profile-v2 candidate checks — 2026-10-16 18:12 UTC

The connected public-v2 fixture completed all four stages through production
composition/scheduler/activities and real isolated PostgreSQL. Native parent/reply
and two profile responses were simulated at HTTP; object storage was simulated.
The MEDIA stage settled three targets, two profiles preserved unknown coverage
and original source timestamps, and avatar reuse held downloads to four. This is
source/integration proof only, not live Instagram profile or reply coverage.

Independent review accepted the frozen avatar-cache and version-adoption batches
without findings. Reviewed checks were reused: 97 media/snapshot cases plus three
final affected cases; 25 adoption cases plus one retained-history case. No live
adoption has occurred. The remaining profile/inventory/shared integration batch
still requires final focused checks and review. Adoption is forward-only to v2;
before applying it, retain a verified immutable v2 executor archive. Reverting the
cloud deployment to d24 alone would not resume a v2-pinned run. Preserve paused
run state and use compatible code or a reviewed forward repair for recovery.

A bounded comparison of the two slow legacy summary predicates found zero match
differences on 128 rows per table when repeated whole-row JSON serialization was
replaced with explicit identity columns/raw_data. Sample times improved from
144.24 to49.53ms (catalog) and269.92 to53.67ms (posts), with73 and80 matches.
These sample results do not establish full-endpoint latency. The scoped source
repair is authorized for only catalog_totals and lite_header_stats; other
predicate callers remain unchanged. No schema/index addition or app build is
needed. Source checks/review and one post-deployment endpoint check remain.

The frozen profile/integration batch has34 distinct passing focused cases:
17profile,7inventory,1connectedv2pipeline,1version-familybudget,1legacypublicpipeline,
and7affectedlisting/workflow/local checks. Static checks passed. Independent
review found one P2: malformed non-object receipt/page canonical_identity or
inventory state can escape as AttributeError instead of the fixed unresolved
proof diagnostic. The complete repair batch is limited to strict object guards
and malformed-proof fixtures; fixes-only review must close it before deployment.
All other reviewed profile/cache/version/budget behavior had no findings.

Parent independently reviewed the three-file legacy projection batch and verified
all frozen hashes; no findings. The complete diff changes only the optional
native-column helper and two Instagram summary call sites, preserving default
SQL and other platforms. Reused eight focused repository tests and two isolated
PostgreSQL tests covering192synthetic semantic comparisons; no production query
was needed for those tests. Full deployed latency remains unverified.

### Profile-v2 release acceptance — 2026-10-16

The sole P2 malformed-proof finding is closed. Strict object guards, including
savepoint-contained SQL validation, passed 22 new malformed-proof cases and
three affected existing cases. Fixes-only independent review found no remaining
issues. All profile, avatar, adoption, and summary projection batches are accepted
for release. Prior passing evidence is reused for unchanged inputs. Deployment,
immutable v2 recovery archive, guarded adoption, and live coverage remain separate
gates; the public run remains cancelled at generation 4 until those prerequisites
are verified. No successful live reply/profile coverage or ten-run/24-hour gate
is claimed by source acceptance.

### Profile-v2 deployment and adoption — 2026-10-16 18:26 UTC

Feature commit `24cb811c` and summary fix `be5de077` were deployed to
`admin-56995/main/trr-backend-jobs` with tag `ig-profile-v2-20261016-be5de077`.
Provider history confirms v74, deployed at 18:24:24 UTC. Explicit-main readiness
and cold-start health HTTP 200 passed. Six new containers matched all 59 expected
source hashes, loaded module origins, v2 profile projection, and public launch
configuration. One older container was still present during the rolling update;
it was left untouched and was not claimed as matching v2. The provider's dirty
revision marker is separate from the verified source manifest.

The verified Git archive of `be5de077` is retained with SHA-256
`e29cbbf697e090ba55fe8a04ea78575108f11b617e348b7207c1e81cacebe42c`.
After the shared coordinator lease became available, guarded adoption changed
the public run from v1 to v2 at cancelled/local generation 4. Before-and-after
hashes prove that every other run field, four manifests, 101 targets, and one
receipt were preserved. Adoption made no source requests.

One authenticated request to the canonical legacy lite snapshot returned HTTP
200 in 37.761 seconds. The response contained a fresh, non-null summary with
cache age zero: 18,487 catalog posts, 18,487 stored posts, 1,142,609 saved comments,
and 16,354 of 20,134 media files saved. This proves one successful request after
the earlier timeout. It is not a general latency guarantee, and these legacy
totals were not produced by the new run. The safe receipt is in workspace
`output/releases/instagram-ingestion-app-773eab6/canonical-legacy-summary-after-be5de077.json`.

Native Chrome in the verified TRR profile reloaded the canonical catalog. Saved
coverage totals were visible again, freshness was `fresh`, and durable progress
showed v2 generation 4 with the local-driver instruction. The UI reported a
4.4-second snapshot on that load. This is a second descriptive observation, not
a controlled speed comparison. No additional app build was needed.

The existing follow-up automation was found paused. Its prompt was updated for
v2 and its paused status was preserved.

### Bounded v2 continuation and diagnostic release — 2026-10-16 18:32 UTC

The local v2 continuation ran for 122.744 seconds, including setup and
cancellation. It committed two more listing pages with 66 new post identities,
then reached its deadline during a third page. The run now has three committed
listing pages and 99 targets in each downstream stage. The interrupted page
remains unresolved; no active target owners remain. The run is cancelled/local
at generation 6 with the same immutable public intent. The interruption is not
a successful target, and this measurement is not full-pipeline throughput.
Future continuation bounds should allow enough time for the selected page count.

The parent independently reviewed and accepted the fixed-token comment failure
diagnostic. All 30 focused tests passed: 22 new cases and eight existing cases.
Request construction, failure decisions, retries, and gates are unchanged. The
diagnostic retains fixed enums and booleans, never raw messages or credentials.

Commit `5b140c72` was deployed to the same explicit main target with tag
`ig-public-basis-20261016-5b140c72`. Provider history confirms v75, deployed at
18:30:05 UTC. Health HTTP 200 and readiness passed. Seven new containers matched
all 59 expected source files and the public-v2 runtime checks. An older draining
container was recorded separately and left untouched. A verified compatible
archive is retained for recovery.

### Measured cost — 2026-10-16 18:31 UTC

Modal reported $0.03341586 for the whole app during the complete 17:00–18:00 UTC
hour: CPU $0.02504706, memory $0.00620015, and network egress $0.00216865. The
report was retrieved at 18:31:56 UTC. The three complete hours from 15:00 through
18:00 total $0.10340974. These metered amounts precede credits, reservations,
and allowances, include concurrent app activity, and exclude local-machine
execution cost. They do not isolate the canary or prove a matched region saving.
Evidence: `.artifacts/ingestion-rollout/modal-billing-17-18utc.json`.

### Public commenter-profile source constraint — 2026-10-16 18:34 UTC

One anonymous web-profile-info request was admitted at 18:31:41.764743 UTC.
The shared lane then recorded `public_access_backoff`, failure generation 8,
and a cooldown until 18:46:42.946819 UTC. The local diagnostic lost its returned
result when `Path.open` rejected an unsupported `opener` argument. The offline
repair uses `builtins.open` and retains safe partial results even if cleanup or
writing fails. No profile-endpoint retry was made. The lane readback proves
rejection and accounting; it does not establish the HTTP status or successful
profile parsing.

The normal public profile page is a separate ordinary interface that can be
checked after cooldown. No native profile metadata shape has been proven there.
Existing code extracts runtime/page context and recognizes GraphQL `data.user`
and `data.fetch__XDTUserDict`, but has no complete HTML metadata parser bound to
the commenter's native ID. No fields will be inferred from Open Graph text,
tokens, a matching handle alone, or HTTP 200. Missing fields remain unknown.
The reviewed child-failure diagnostic waits for the same shared cooldown.
Full live replies, hosted media, commenter profiles, ten representative runs,
and the 24-hour observation gate remain open.

### Public source checks after cooldown — 2026-10-16 18:47 UTC

The one-request ordinary profile-page diagnostic returned HTTP 200 with 589,638
bytes, 35 valid embedded JSON scripts, and 1,190 inspected structural nodes in
2.816 seconds. It found no recognized native profile envelope. This proves that
the page was publicly retrievable, not that full profile metadata was available.
No HTML, tokens, biography, or other profile strings were retained. The bounded
scan's supported native paths limit this conclusion. Its unproved shape did not
clear source health or create a new rejection; cleanup released only its probe.
Evidence: `public-commenter-profile-html-be5de077-result.json`.

The improved child diagnostic admitted exactly two requests: a permalink GET
and one child GraphQL POST. Both returned HTTP 200. The native response contained
one CRITICAL error with a rate-message marker and the legacy-recognized rate-code
signal. It contained no child connection, and the bounded diagnostic scan was
not truncated. The source therefore remains classified `rate_limited`; this
result is more specific than the earlier sanitized-only error. Zero replies and
zero accepted child pages were retrieved. No canonical or run writes occurred.
The shared gate retained cooldown until 19:02:10.760011 UTC. Do not repeat this
unchanged child request. The native response is not evidence of zero replies.
Evidence: `public-local-child-only-5b140c72-20261016T184705Z-result.json`.

The public canary remains cancelled/local v2 at generation 6 with its saved
listing pages and 99 downstream targets per stage preserved. The final maintained
native profile-content GraphQL assessment is recorded below.

### Final bounded public profile assessment — 2026-10-16 19:03 UTC

After the recorded cooldown, the reviewed diagnostic used the immutable
`5b140c72` release and verified all 59 source hashes. It admitted exactly two
requests in 22.037 seconds: one ordinary commenter-profile GET and one maintained
profile-content GraphQL POST. Both returned HTTP 200. The POST contained one
native error, no recognized user object, and no native success status. Strict
profile parsing did not pass. The classifier recorded `parse_error` with the
fixed basis `native_error_or_status_rejection`; this result does not independently
prove a rate limit or a request-format defect. No raw response, profile strings,
credentials, canonical data, or run changes were written by the diagnostic.
Evidence: `.artifacts/ingestion-rollout/public-commenter-profile-graphql-5b140c72-result.json`.

The read-only check at 19:02:47 UTC confirms cancelled/local v2 generation 6,
three listing receipts, one unresolved listing continuation, and 99 unresolved
targets in each of post details, comments, and media. All four manifests remain
unsealed, no active target owners remain, and the diagnostic released its probe.
The public lane retains cooldown until 19:17:43.261431 UTC. Evidence:
`.artifacts/ingestion-rollout/public-v2-observation-190247.json`.

This concludes the bounded public diagnostic sequence. Do not repeat unchanged
child or profile requests merely because a cooldown expires. Public top-level
comments were observed separately (42 comments, 439 likes, and one GIF), but
those diagnostic observations are not canonical completion receipts for this
run. Full replies, full commenter profiles, hosted comment-media completion,
and an end-to-end successful canary remain unverified. The ten representative
runs, 24-hour observation, and matched full-pipeline cost/speed comparison remain
open. The paused follow-up automation remains paused. Further rollout requires
new source evidence or a separately authorized change to the public-only access
constraint; no login, proxy, or paid-provider fallback was used.


### Public-only follow-up: verified placeholder and comment UI fixes — 2026-10-17

The cancelled local v2 canary `bcc7411d-085d-4efc-be77-7d8e87303eb6`
remains at generation 6. The 00:29 UTC readback confirms 99 unresolved targets
in each downstream stage, three listing receipts, four evidence records, no
active owners, and unchanged run state. Diagnostic samples are not canonical
completion receipts. No broad backfill or observation automation was resumed.

The current Instaloader PR 2722 candidate (head
`83ddd40d40925b98571e18dbd2c582ff0b905e1e`) uses the anonymous feed-by-username
endpoint. One admitted request for a commenter whose native ID is present in
retained comment evidence returned HTTP 401 (119 bytes). The shared public gate
recorded `public_access_backoff`, failure generation 11, with cooldown through
2026-10-17 00:35:01 UTC. No credentials, proxies, redirects, retries, raw profile
payload retention, or business writes were used. The candidate was not integrated.
Safe evidence: `.artifacts/ingestion-rollout/public-feed-profile-20261017T002002Z.json`.

The retained GIF-only comment has a `giphy_media_info` attachment referencing
Instagram's static `xg_5YoVlvjp.gif`. A real CDN download returned a valid image/gif,
444 bytes, 160 by 101 pixels, one frame, SHA256
`05e81bfed1fda5743fea1da04586e635c4f82cd2258c2716265b538508efd5a9`.
Visual inspection shows **Content unavailable**, not the requested comment media.
The previous downloader would have hosted this placeholder and reported success.
Backend commit `7f35ce5b` rejects these exact observed bytes with
`media_source_placeholder`; the target stays unresolved and no success receipt
is issued. It does not classify arbitrary small images or assume native deletion.
The real-byte regression failed before the fix and passed after it; eight affected
media cases pass, and Ruff check/format pass. Independent read-only review found
no blockers. Other placeholder variants are not claimed to be recognized.

App commit `7ca68c3e` displays all selected comment attachments, adds HD-avatar
fallbacks, and shows verification only when explicitly true. The existing modal
runtime regression now verifies both attachments, HD-only avatar, badge, and hover
preview. That regression, ESLint, generated checks, and all 80 quick-validation
cases pass. No production build was needed or run. Full profile biography/counts
remain stored separately in `social.instagram_profiles`; the comment read model
currently does not project that enrichment. It must not be described as visible
in the comment UI. No new API/schema contract was introduced by this fix.

Deployment verified `admin-56995 / main / trr-backend-jobs`
(`ap-DkLTRoSvqhbkGO7fHyHxrD`). Tagged deployment v76 used
`ig-placeholder-20261017-7f35ce5b`; the default hybrid-startup reconciler then
redeployed the same revision as v77. v77 is the latest observed deployment.
Readiness and health passed (HTTP 200). A live v77 container matched all 59 checked
ingestion source hashes. A targeted in-container smoke imported the deployed
media module and rejected the real placeholder fixture without source requests,
storage writes, or database writes. v75 and its source archive remain available
for rollback; no rollback was performed. Dirty unrelated checkout files remain
preserved. The local canary still points at its prior frozen recovery archive;
no source adoption or canary resume is claimed.

`make dev-hybrid` started successfully with `PORTLESS_SYNC_HOSTS=0` after all three
canonical TRR hosts were verified to resolve to loopback. The initial attempt
waited for privileged hosts sync and was stopped; the retry used the supported
skip because no host change was needed. The retry also set
`WORKSPACE_RUNTIME_MODAL_AUTO_DEPLOY=0` to avoid another redundant deployment.
The app and backend health checks pass and canonical Portless routes are ready.
The browser tool inventory exposes no connected Chrome profile. The Chrome
DevTools doctor supports an isolated browser but has no verified same-instance
TRR profile receipt. Browser acceptance remains pending the required TRR profile
connection; an isolated browser is not evidence of that account context.

The bounded real reply + genuine attachment + full commenter profile sample,
canonical-to-hosted-to-API-to-browser verification, ten representative runs and
24-hour observation remain open. These fixes improve correctness but do not
establish successful anonymous end-to-end ingestion.

### Verified anonymous profile source; public replies still blocked — 2026-10-17 00:58 UTC

Backend commit `690e08c1` adds a separate, finite public GraphQL profile source.
Current publicly delivered `PolarisProfilePageContentQuery` definition
`28649589871350527` was observed in static interface code. The adapter makes one
ordinary profile-page GET and one native-ID-bound GraphQL POST, with no document
fallback, login, proxy, ambient credentials, redirects, or hidden retries. All six
Relay provider flags come from the same GET's public bootstrap. Response bytes
and request time are bounded. Bootstrap tokens stay transient.

The source-only live check passed through the maintained adapter and existing
public admission gate at 00:47 UTC: two admitted requests, matching native ID and
handle, and a validated metadata projection. Name, biography, follower/following
counts, verification, privacy and avatar were observed; post count and HD avatar
were absent and remain unknown. No commenter posts were retained or paginated.
Safe evidence: `.artifacts/ingestion-rollout/public-maintained-profile-query-20261017T004750Z.json`.

The separate parser `instagram-public-commenter-profile-graphql-v1` accepts an
error-free native GraphQL user root without inventing REST's `status: ok`.
Projection version 2 carries the exact query and six boolean provider flags.
Old REST evidence retains its original parser/version. Cache settlement compares
canonical proof with retained evidence under that evidence's parser. Native ID
matching, per-user targets, field validation, ownership fencing, partial metadata
persistence and exact avatar reuse remain enforced. Missing fields do not inherit
stale canonical values. There is no schema or app/API contract change.

Focused validation passed: ten GraphQL source/parser cases, nineteen profile
cases including real disposable PostgreSQL persistence/cache/cooldown, the
composed public-v2 pipeline fixture, and fifteen public composition cases. Source
and storage in the composed fixture are controlled fakes; this is not a live
full-pipeline result. The existing database-backed profile modules now carry the
required `live` marker so the shared offline test guard does not block their
owned local database. Ruff and diff checks pass. Independent review found one
HTTP-200 native-error Retry-After gap; the fix and focused regression preserve a
7200-second server delay in the shared lane with two requests and zero canonical
profile/receipt rows. Repair review found no remaining blockers.

A bounded current-page inspection retained static Bootloader structure and the
same-page resource map. It linked the comment-input component to a 30-resource
deferred group. The 30 saved JS bundles total 1,897,603 bytes and contain no
current child GraphQL query definition. They do contain `PolarisAPIGetChildComments`,
whose native path is `/api/v1/media/{media_id}/comments/{parent_comment_id}/child_comments/`.
No module/resource positional pairing or query ID was guessed. The second static
batch saved its files but hit a diagnostic-only signal-handler variable collision
while finalizing its status output. The finalizer was repaired; the saved inventory
was reconstructed offline without repeating source requests. Evidence:
`.artifacts/ingestion-rollout/public-lazy-inventory-recovered.json`.

One admitted request using that current native child definition and a retained
parent ID returned HTTP 302 to login at 00:54 UTC. The gate recorded
`public_access_backoff` / `redirect_login`, failure generation 12, with cooldown
through `2026-10-17T01:09:40.673662+00:00`. No redirect was followed and no next
endpoint was tried. Evidence: the timestamped `public-native-child-*.json` artifact.
This does not prove every possible anonymous approach impossible; it establishes
that no tested, supported anonymous reply path is available for the required
sample. Authenticated access requires the separate user decision in the approved
plan.

Deployment v78 is `admin-56995 / main / trr-backend-jobs`, app
`ap-DkLTRoSvqhbkGO7fHyHxrD`, tag `ig-public-profile-20261017-690e08c1`, at
00:53:20 UTC. Provider history records `690e08c*`: unrelated dirty checkout files
were preserved. Explicit-main readiness and health pass. Five new deployment
containers matched all 60 checked source files and passed the GraphQL identity,
projection and unknown-field smoke without source/database IO. One older container
still had the v77 profile files; it was not treated as new-revision proof or stopped.
The deployed media worker also rejected an intentionally incomplete reservation
before source/database IO (call `fc-01M53NSRA4NXHMXHPMYEP03DE5`). Its new media
worker container matched all 60 source hashes and passed the same GraphQL smoke.
The initial local
smoke helper imported the SDK before pinning its profile and failed lookup without
dispatch; pinning the process environment before import corrected that helper.
Previous v77/v76 and source commit `7f35ce5b` remain rollback references.

The 00:56 UTC readback confirms the original canary remains cancelled, local v2,
generation 6: 99 unresolved identities in each downstream stage, three listing
receipts, four evidence records, unsealed manifests, no active target owners, and
no held public probe. No broad resume, source adoption or observation automation
occurred. Anonymous profile source success is not a canonical live profile receipt.
The real reply + genuine attachment + profile sample, hosted/API/browser acceptance,
ten representative runs and 24-hour observation remain open. The required TRR
browser-profile connection is still pending. The earlier app quick checks and
`make dev-hybrid` result stand; this backend profile change needs no app build.

### Account run policy, network lanes and the Modal page limit (2026-10-19)

Commit `2186d546` adds an opt-in replacement for the bravotv canary lock. Codex
commits `e7bc474c`, `3c77cc2a` and `fc90431a` sit on top of it. None is deployed;
live remains v80 (`60cc3117`).

Runtime settings (Modal secret `trr-backend-runtime`; all absent on 2026-10-19,
so v2 is fully off in production):

| Setting | Value to enable | Meaning |
| --- | --- | --- |
| `TRR_INSTAGRAM_DBOS_DATABASE_URL` | session DSN, port 5432, user `trr_ingestion_runtime.<ref>` | DBOS refuses any other role, owner, superuser or port 6543 |
| `TRR_INSTAGRAM_DBOS_RUNTIME_ROLE` | `trr_ingestion_runtime` | Restricted role; exists in `trr-core` with the `dbos` schema |
| `TRR_INSTAGRAM_INGESTION_SCHEMA_READY` | `1` | API builds the v2 command service |
| `TRR_INSTAGRAM_INGESTION_ENABLED` | `1` | New-run routing |
| `TRR_INSTAGRAM_INGESTION_COORDINATOR_ENABLED` | `1` | Coordinator runs inside `sweep_social_dispatch_queue` (2-minute cron) |
| `TRR_INSTAGRAM_INGESTION_POLICY` | `accounts` | Default `canary` keeps the old bravotv calendar-2026 lock; unknown values fail closed |
| `TRR_INSTAGRAM_INGESTION_MAX_RUNS_PER_DAY` | optional, default `12` (1–48) | Per-account daily launch cap |

Account policy rules (`AccountRunPolicy`, `composition.py`):

- Accounts are active Instagram rows in `social.shared_account_sources`; an
  unreadable list admits nobody.
- Actions: `backfill` (`full_history` or `bounded_window`), `sync_newer`
  (`head_gap`, requires an initial backfill), `sync_recent` (`recent_window`),
  `resume_tail`.
- One `queued`/`running`/`blocked` run per account. Cancelled and failed runs stay
  resumable but no longer block a new launch.
- While `ENABLED=1` and `POLICY=accounts`, the legacy `/catalog/{backfill,
  sync-recent,sync-newer,resume-tail}` routes return 409
  `instagram_v2_owns_launches` for Instagram, and v2 rejects a launch while a
  legacy `social.scrape_jobs` Instagram job for that account is non-terminal.
- With v2 enabled, the account page's existing Backfill / Sync buttons already
  launch v2 runs (`SocialAccountProfilePage.tsx`, `ingestionLaunchFromCatalog`).

Logged-out request lanes (`identity_policy.py`):

- Public listing, detail and comment gates use `public:modal` or `public:local`
  from the run's execution placement, so the Mac and Modal never share a budget.
  Since 2026-10-21, public avatar, commenter-profile and media-refresh gates use
  the same placement lane, so a Mac media run honours a Mac listing/comment
  backoff. Authenticated gates keep their credential identity. The legacy shared
  `public` row is no longer selected by v2 public runs.
- Since 2026-10-22, a profile/avatar lookup that returns HTTP 404 or a complete
  response with `user: null` is lane-neutral on a network lane: the target still
  fails, but the shared failure count, `last_error_code`, cooldown and backoff
  ladder are unchanged, and a held probe lease is released. Other failures keep
  the 3-identical-failure circuit. Credential lanes keep their accounting.
- Network lanes follow the June public backoff without Retry-After: rejection
  1 h → 4 h → 12 h → 24 h; 429 waits 2 h; a server Retry-After always wins. One
  success steps escalation down one level instead of resetting it.

Measured logged-out behaviour (2026-10-19, bounded probes, no DB writes):

- Mac (residential): 10 listing pages, 330 posts, 82 s, no rejection.
- Modal (datacenter): page 1 (33 posts), then HTTP 401 on page 2, both before and
  after a full one-hour cooldown. Logged-out Modal runs can therefore serve only
  the newest page; full history must run with `local` placement.
- Listing fills caption, time, likes, comment count, all media URLs, tags,
  collaborators and location. It does not fill video view count or duration.
  Logged-out permalink details return an empty shell (`metadata_empty` in the
  probe; `public_detail_unresolved` in v2), so public runs should select only
  `comments` and `media`.
- Replies: the earlier entry above found no supported anonymous reply path; the
  2026-10-19 child-reply request returned GraphQL rate-limit error `1675004` on
  its first call. Treat replies as unavailable logged out. No login fallback is
  configured.

Deferred: video duration (the browser image has no FFmpeg), skipping unchanged
comment targets (rollups hold counts, not coverage), and view counts.

### Mac worker and "Run on: This Mac" (2026-10-19)

- Launch: `InstagramIngestionLaunchV2.execution_placement` (`modal` default,
  `local`). A local launch sets the placement inside the creating transaction, so
  the Modal coordinator never sees the run as Modal-placed. Local launches require
  the public source profile. Default Modal launches keep their historical command
  fingerprint (the default field is excluded from the hash).
- Worker: `scripts/ingestion/local_worker.py` serves every queued/running `local`
  run until stopped. It holds its own session advisory lease
  (`composition.LOCAL_WORKER_LOCK_KEY`, namespace 48112, key 20261017), so it never
  blocks the Modal coordinator (key 20261016). Partitions run inline with the real
  Scheduler; a stop mid-partition leaves the partition unacknowledged and the next
  start replays it without resending settled targets. Shutdown never cancels runs.
- Status: capabilities report `local_worker_online` from `pg_locks` while that lease
  is held; the app disables "This Mac" when it is false.
- API resume/retry of local runs stays rejected by the existing placement fence;
  cancel works from the app.
- Install (user-level LaunchAgent): create git-ignored `TRR-Backend/.env.local-worker`
  with `TRR_INSTAGRAM_DBOS_DATABASE_URL`, `TRR_INSTAGRAM_DBOS_RUNTIME_ROLE` and
  `TRR_INSTAGRAM_INGESTION_COORDINATOR_ENABLED=1`, copy
  `scripts/ingestion/launchd/com.trr.instagram-local-worker.plist` to
  `~/Library/LaunchAgents/`, then `launchctl bootstrap gui/$(id -u) <plist>`.
  Logs: `~/Library/Logs/trr-instagram-local-worker.log` (no DSN is printed).
- App: logged-out v2 backfills default to `comments, media`; the Backfill dialog
  shows "Run on: Modal / This Mac" only while v2 is enabled.

Validation: 9 worker tests plus the existing local-execution suite pass on an
isolated PostgreSQL; the ingestion and API suites pass (2632) except the pre-existing
real-DBOS timing test in `test_workflows.py`, which fails identically on `032e86f7`.
App: ingestion and account-page tests (231), `make app-validate-quick` and
`pnpm run typecheck` pass.
