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
| E | Backend schema/client parity and browser operator flow | API/client and fixture-backed browser journeys pass. App 964e9ce remote production build and authenticated checks pass; normal TRR browser verifies run display, cancel, reload and resume while new launches are disabled |
| F | Deployment identity/readiness, bounded canary, 10 runs and 24h observation | US East v66 identity/readiness passes. Canary generation 7 is intentionally paused with 33 listing pages and 1,089 targets per downstream stage preserved while the reviewed identity/comments repair batch is completed. The matched local resource comparison is accepted with explained overhead; revised measurements, full live completion, actual trial billing, ten representative runs and 24h observation remain pending |

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
authorized the `livestnation` login in Chrome's `codex` profile. The prohibition on
`thereality.report` sessions/cookies remains absolute. The active browser tool
verified profile `codex` and the account's own `livestnation` edit controls. A new
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
history remain available for rollback; the authorized livestnation credential did
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
The livestnation-only identity authorization and thereality.report session/cookie
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
pin the existing verified livestnation native user ID in the existing social
secret without changing cookies, then verify fresh runtime readback. No
thereality.report session or cookie is allowed. Current live v66 remains the
region-only baseline until this complete candidate passes consolidated review.


Credential deployment contract: `SOCIAL_INSTAGRAM_COOKIES_JSON`,
`TRR_INSTAGRAM_INGESTION_AUTH_USER_ID`, and
`TRR_INSTAGRAM_INGESTION_AUTH_USERNAME` belong together in `trr-social-auth`.
The pins name the already verified native user ID and authorized livestnation
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
livestnation identity, then added only the two identity-pin keys to
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
