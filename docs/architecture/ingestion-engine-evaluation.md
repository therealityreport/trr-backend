# Instagram ingestion engine evaluation

Evaluation date: 2026-10-16. Owner: isolated engine-evaluation assignment
`6f437318-f439-48d7-917c-146c8a1d7f0e`. Parent owns engine selection,
integration and acceptance. **No production engine is selected by this report.**

The real local recovery checks pass for both candidates. Production hosting,
database-role readiness, current-ingestion performance parity, and live Modal
dispatch remain unverified. Passing this harness does not pass adoption Gate B
by itself. See the parent [implementation record](instagram-ingestion-rebuild.md).

## Reproduce

From `TRR-Backend`, use a separate Python 3.11 environment. Do not install the
evaluation dependencies in the application environment. The saved freeze
records all installed transitive versions; the small requirements file records
the direct dependencies. The harness imports no production modules.

```sh
python3.11 -m venv /tmp/trr-engine-eval-venv
/tmp/trr-engine-eval-venv/bin/pip install -r scripts/ingestion_engine_eval/evidence/2026-10-16/environment.freeze.txt
/tmp/trr-engine-eval-venv/bin/python scripts/ingestion_engine_eval/run.py --runtime-dir /tmp/trr-engine-eval-matched --targets 120
/tmp/trr-engine-eval-venv/bin/python scripts/ingestion_engine_eval/run.py --runtime-dir /tmp/trr-engine-eval-large --targets 1000 --bounded-only
/tmp/trr-engine-eval-venv/bin/python scripts/ingestion_engine_eval/run.py --runtime-dir /tmp/trr-engine-eval-partial --partial-cancel-only
/tmp/trr-engine-eval-venv/bin/python scripts/ingestion_engine_eval/temporal_pinning.py --runtime-dir /tmp/trr-engine-eval-pinning
/tmp/trr-engine-eval-venv/bin/python -m pytest -q -o addopts='' --confcutdir=tests/ingestion_engine_eval tests/ingestion_engine_eval
ruff check scripts/ingestion_engine_eval tests/ingestion_engine_eval
ruff format --check scripts/ingestion_engine_eval tests/ingestion_engine_eval
```

Runtime directories must be new and empty; the pinning directory must not exist.
`--pg-bin` selects a different installed PostgreSQL binary directory. Default is
Homebrew PostgreSQL 17. Target count is explicitly bounded to 1–1000. The SDK
downloads the full Temporal dev server on first use; this is not the time-skipping
test-server implementation. Its exact downloaded version is recorded per run.

Measured versions: Python **3.11.15**, DBOS **3.3.0**, Temporal Python **1.34.0**,
psycopg **3.3.6**, SQLAlchemy **2.1.4**, PostgreSQL **17.10**, Temporal CLI
**1.9.1**, Temporal Server **1.32.0**. The local server's SQLite history is on disk.
DBOS system history and the shared fake-provider tables use private UTF-8
Postgres. Nothing uses TRR credentials, Supabase or production tables.

## Workload and evidence boundary

Both workers use the same `store.py` run, target, dispatch and receipt tables.
Each target has a deterministic dispatch key. SQL uniqueness deduplicates fake
remote acceptance and receipts. A locked run row checks cancellation and owner
epoch both before acceptance and before receipt commit. This SQL boundary models
a fenced Modal adapter; **it does not prove live Modal idempotency or delivery**.

Each crash case commits a visible checkpoint and blocks its real worker process.
The coordinator sends SIGKILL, then starts a replacement process against the same
persisted engine history. Injection covers before dispatch, after accepted
dispatch and after committed receipt but before the step/activity acknowledges.
The replacement replays engine history. Reusing the workflow ID must leave
accepted dispatches, receipts and attempt counts unchanged after completion.

DBOS uses a fixed local executor ID for restart recovery. Temporal uses a
version-specific queue for the matched workload. A separate **native** Temporal
deployment-version probe uses the same queue for v1 and v2, sets PINNED behavior,
promotes v2, signals a waiting v1 workflow and verifies it stays running until v1
returns. Its provider activity then reports v1; server description confirms the
pinned deployment version. Queue separation alone is not native pinning proof.

## Observed matrix

| Check | DBOS | Temporal | Meaning and limits |
|---|---|---|---|
| SIGKILL before dispatch | PASS | PASS | Three unique targets recovered; one receipt each |
| SIGKILL after dispatch | PASS | PASS | Acceptance deduplicated by application SQL |
| SIGKILL after receipt commit, before ack | PASS | PASS | Re-executed adapter call cannot duplicate receipt |
| Duplicate completed workflow ID | PASS | PASS | Temporal explicitly sets REJECT_DUPLICATE reuse policy |
| Stale owner dispatch/completion | PASS | PASS | SQL adapter fence; independent late-completion tests also pass |
| Durable cooldown after worker death | PASS | PASS | No acceptance before persisted deadline |
| Cancel and resume during cooldown | PASS | PASS via history reset | DBOS native resume; Temporal new execution under same workflow ID |
| Existing receipt/attempt expenditure across cancel/resume | PASS | PASS via history reset | Receipt timestamp unchanged, target 0 attempts advance from 2 to 3 |
| Application version recovery | PASS | PASS | DBOS application_version; matched Temporal queue isolation |
| Native deployment pinning | Application version above | PASS separately | Same-queue Temporal v1/v2 deployment probe |
| 120 and 1000 target fixtures | PASS | PASS | Serial workload, small history payloads, bounded adapter threads |
| Bounded workflow partitions | PASS separately | UNVERIFIED | DBOS 20 deterministic partitions of 50 targets, continuation crash recovery |
| Continue-as-new and global history retention | UNVERIFIED | UNVERIFIED | Partition proof does not establish global storage bounds |
| Temporal service stop/start, completed history query | N/A | PASS | Same on-disk history after full dev-server restart |
| Production role/schema bootstrap | UNVERIFIED | UNVERIFIED | Local bootstrap role is trusted and owns its database |
| Modal executor death/replacement and cross-container recovery | UNVERIFIED | UNVERIFIED | No live provider call made |
| Three-attempt cap enforcement after repeated resumes | UNVERIFIED | UNVERIFIED | Attempt expenditure is retained; toy store does not implement policy rejection |
| Existing ingestion regression thresholds | UNVERIFIED | UNVERIFIED | No matched current-production baseline |

Four separate real-Postgres adapter tests pass: 16 concurrent duplicate calls
produce one accepted dispatch and receipt, ownership change between acceptance
and commit rejects the old completion, cancellation at that boundary rejects
completion, and a cancelled run rejects dispatch. These are application SQL
tests, not additional evidence of engine scheduling semantics.

## Measurements

Single runs on the same host, no Instagram I/O. Elapsed time includes worker
startup/shutdown and engine coordination. Statement calls use
`pg_stat_statements`, including application, DBOS metadata and test checkpoint,
snapshot and outbox observer calls. The matched-run counter excludes the
`pg_stat_activity` session-sampler and counter-aggregation SELECTs; it does not
exclude all test observer traffic. Session counts are sampled every 100 ms and exclude the
sampler connection; short-lived peaks can be missed.

| Fixture | Engine | Seconds | Postgres statement calls | Sampled peak sessions | Engine history |
|---|---|---:|---:|---:|---|
| 120 targets | DBOS | 2.404 | 2521 | 6 | 121 recorded steps |
| 120 targets | Temporal | 5.704 | 1457 | 1 | 737 events / 87,438 protobuf bytes |
| 1000 targets | DBOS | 16.641 | 20,727 | 6 | 1001 recorded steps |
| 1000 targets | Temporal | 58.502 | 12,017 | 1 | 6017 events / 719,447 protobuf bytes |

The PostgreSQL costs are **not equal-topology production cost comparisons**:
DBOS stores its engine history in that Postgres database; Temporal stores its
engine history in SQLite in this experiment. Temporal's production persistence,
visibility, frontend/matching/history resources are absent from those SQL counts.
These observations cannot establish a <=10% regression against current TRR.
History grows with the fixture. DBOS partitioning is tested in the separate
supplement below; continue-as-new, retention and workloads larger than 1000
targets remain unverified.

## Existing-host feasibility and missing gates

DBOS can embed in a Python executor without a separate DBOS server. The actual
local worker recovery proves process restart with a stable executor identity,
matching application version and persisted Postgres history. On Modal, an
inference is that an existing function could host bounded polling/execution and
an existing scheduled function could wake replacement executors. It still needs
an explicit restart owner, stable logical executor identity, mutual exclusion
while replacing it, compatible versions kept available, and a proven wakeup path
after function timeout/scale-to-zero. Default background SDK threads alone do
not supply that lifecycle. Distributed executors with random IDs do not
automatically recover each other's pending workflows without a recovery owner.
[DBOS recovery documentation](https://docs.dbos.dev/production/workflow-recovery)
supports these recovery distinctions; **Modal hosting is not yet proven**.

DBOS defaults to a 20-connection system pool and LISTEN/NOTIFY. Production must
set bounded pool/thread counts in addition to existing TRR psycopg2 pools. Its
bootstrap needs schema/table/index/function/trigger DDL; subsequent runtime can
use an already migrated schema with `run_migrations=False` and the required
schema USAGE and table/sequence/function privileges. Exact least-privilege
grants, schema version and Supabase-role readiness are unverified. A migration
owner should bootstrap the designated schema out of band; do not grant DDL to
ordinary API requests. [DBOS configuration](https://docs.dbos.dev/python/reference/configuration)
describes these settings. Installed 3.3.0 `_migration.py` also creates schemas and
notification functions/triggers; it was inspected directly.

The workspace environment contract routes remote workers to the session pooler
on 5432 and gates the transaction pooler on 6543. The evaluated notification
mode requires session-scoped connection behavior. Do not silently use transaction
pooling. DBOS offers polling mode, selected when the system schema is created;
its transaction-pool compatibility and polling overhead were not tested here.
Verify the real `TRR_DB_SESSION_URL` lane, connection budget, SSL and role grants
without printing credentials before adoption. No live connection was inspected
in this assignment.

Temporal workers need an available Temporal service, in addition to application
Postgres. The full dev server with SQLite is local evidence and is not a
production-hosting design. Render publishes a blueprint with multiple Temporal
services and persistence. Existing TRR Render capacity, private gRPC routing,
TLS/auth, production schemas/roles, visibility persistence and incremental cost
are unverified. No hosted service was provisioned.
[Render deployment guide](https://render.com/docs/deploy-temporal),
[Temporal production deployment](https://docs.temporal.io/production-deployment).

Neither topology is accepted for production by this report. DBOS has a plausible
path using existing Python/Modal/Postgres without another paid service, but
bounded worker lifecycle, distributed recovery ownership and database readiness
still require connected evidence. Temporal has passing local semantics but
requires a production service topology not established by existing capacity.

## Evidence, errors and cleanup

Machine-readable results, exact versions and environment freeze are saved
under `scripts/ingestion_engine_eval/evidence/2026-10-16/`. Full private runtime
directories for this run are `/tmp/trr-engine-eval-evidence-utf8-6f437318`,
`/tmp/trr-engine-eval-large-6f437318`,
`/tmp/trr-engine-eval-partial-6f437318` and
`/tmp/trr-engine-eval-native-pinning-6f437318`. They retain local databases,
downloaded server binaries and logs for inspection. They contain synthetic data.

The initial `/tmp/trr-engine-eval-evidence-6f437318` probe failed with SQL_ASCII:
SQLAlchemy received byte-string server metadata and Temporal converted a byte
version to a numeric list. The replacement explicitly initializes UTF-8.
The first standalone adapter fixture also hit macOS's 103-byte Unix-socket path
limit; TCP-only sockets fix it. The repository's parent test fixture imports
production dependencies; `--confcutdir` intentionally isolates this harness.
Those failures are preserved as setup failures, not engine-semantic failures.

All completed runs call worker/server/Postgres cleanup and record `cleanup.json`.
No production modules, requirements, migrations or deployed apps changed in this
assignment. TRR-APP build and Modal deployment are not applicable to this local
evaluation. Parent independent review and final acceptance remain outstanding.

## Supplement: bounded existing-service DBOS topology

This separate follow-up leaves the earlier comparison evidence unchanged.
Run `dbos_existing_service.py --runtime-dir <new-private-directory>` with the
same evaluation Python environment. Evidence suffix is
`dbos-existing-service`; the completed runtime is
`/tmp/trr-dbos-existing-service-bootstrapped-6f437318`.

**Local topology result: PASS, credible implementation candidate using existing
Modal/Python/Postgres service boundaries. Live adoption remains unverified.**
No DBOS Conductor or other orchestration service was used.

The worker acquires a dedicated session advisory lock before `DBOS.launch`.
Only the winner launches SDK threads. It uses stable logical executor identity
`instagram-coordinator-slot-0`, application version `frozen-v1`, system pool size
1, `max_overflow=0`, polling concurrency 1, and executor-thread limit 2. The
installed executor reports the actual thread limit as 2. A process lifetime
timer exits with code 124 to model a bounded platform timeout. Its replacement
uses the same identity/version and automatically recovers the pending workflow.
The probe calls public `DBOS.destroy()` before releasing the coordinator lock.
That call does **not** prove active workflow threads have joined: installed
DBOS 3.3.0 calls executor `shutdown(wait=False, cancel_futures=True)`. The forced
process-exit replacement cases remain valid, but this probe's normal-shutdown
ordering is not sufficient evidence for releasing a production lease.

Two representative cases pass:

| Boundary | Before timeout | After replacement | Rival wakeup | Elapsed |
|---|---|---|---|---:|
| Durable cooldown | 0 dispatches, 0 receipts | 3 dispatches, 3 receipts, 3 attempts | Busy; SDK not launched | 8.985 s |
| Receipt committed before ack | 1 dispatch, 1 receipt | 3 dispatches, 3 receipts, 4 attempts | Busy; SDK not launched | 6.896 s |

At 20 ms sampling, the maximum granted coordinator advisory locks is one.
The sampled total Postgres peak, excluding the observer, is **4 sessions**:
one advisory-lock session, two DBOS sessions and one adapter session. DBOS's
Postgres listener detaches its connection from the system pool, so **pool size
one does not mean one total DBOS connection**. The observed DBOS peak is two:
one pooled metadata connection and one detached LISTEN connection. Idle SDK
background threads also exist outside the two-thread workflow executor; the
executor limit is not a global Python-thread limit.

Bootstrap is deliberately separate. The first probe tried `run_migrations=True`
with pool size one and timed out before workflow startup. Installed DBOS 3.3.0
holds a connection for the migration advisory lock while schema migration asks
for another connection. The corrected probe uses supported `DBOS.migrate`,
whose bootstrap pool size is two, before starting any coordinator. Its
`application_role` argument grants access to an independently created local
runtime role. That role is not a superuser or schema owner, has schema USAGE,
has no schema CREATE permission, and runs DBOS with `run_migrations=False`.
These local role checks pass; they establish no Supabase privilege claim.

The lease monitor exits on a failed lease query instead of reconnecting and
continuing under a released lock. Arbitrary network partitions, blackholed
sessions and live Supavisor advisory-lock behavior were not injected. Business
owner-generation checks remain necessary at every dispatch/commit boundary;
the coordinator lock is not a replacement for those fences.

### Exact wakeup integration available in current source

Read-only inspection found an existing wakeup owner, rather than requiring a
new scheduler service:

- `trr_backend/modal_jobs.py:1793` decorates `sweep_social_dispatch_queue` with
  timeout 15 minutes, `max_containers=_SOCIAL_RECOVERY_CONCURRENCY_LIMIT` and
  cron `*/2 * * * *` when always-on schedules are enabled. The concurrency
  setting defaults to 1 at line 191.
- `trr_backend/modal_jobs.py:1801` validates the maintenance owner, recovers and
  dispatches social jobs, then recovers stale pending launches before finishing
  and closing legacy DB pools. This is the concrete hook for waking a separate
  bounded ingestion coordinator function **inside the same Modal app**. Do not
  run an unbounded SDK background thread after the sweep returns.
- `api/main.py:224` sets fallback social-recovery cadence to 120 seconds by
  default. `_run_modal_social_recovery_once` at line 247 calls the same Modal
  maintenance function. `_run_modal_runtime_scheduler_loop` at line 281 owns
  this fallback scheduling path.
- `trr_backend/modal_jobs.py:633` rejects zero or duplicate maintenance owners
  when the owner guard is required. Preserve the deliberate choice between
  Modal cron and API runtime scheduler; do not enable both.

The concrete candidate integration is: keep that existing sweep/fallback as
the wakeup owner; wake one bounded coordinator function in `trr-backend-jobs`;
acquire the session lock before launch; drain/recover only the new Instagram
owner's workflows in a dedicated subprocess; keep its advisory lease until that
process exits normally or the parent terminates it at the bounded deadline.
Never release the lease while workflow threads can survive. Public
`DBOS.destroy()` alone does not establish this boundary. Keep that coordinator's container cap at one and its internal
pool/thread limits explicit. A budget shorter than the 120-second sweep cadence
leaves room for cleanup; choose it against actual Modal timeout/queue behavior
and do not increase existing hard capacity caps implicitly. The next sweep
recovers timeout-interrupted work. This hook is specified, not implemented or
deployed by the evaluation assignment.

Remaining production checks are specific: verify the deployed maintenance owner
and cadence in `admin-56995 / trr-backend-jobs / main`; prove the wakeup hook and
replacement after actual Modal timeout/scale-to-zero; verify a session-preserving
Supabase connection to `vwxfvzutyufrkhfgoeaa`, role grants/schema version, SSL and
aggregate connection budget; retain compatible worker code for pinned pending
runs; enforce durable retry budgets and business-generation fencing; test
provider dispatch/receipt behavior and measured current-ingestion regression
thresholds. No new paid service is required by the demonstrated local topology,
but existing provider capacity and operating cost have not been accepted.

## Supplement: cancellation terminal-state verification

Independent review identified that accepting any first-worker exit after a
cancel request did not prove engine cancellation. The strengthened probe now
requires DBOS `get_workflow_status` to report `CANCELLED`, or Temporal's actual
execution description to report `CANCELED`, before it clears the business cancel
flag or resumes/resets the workflow. Those statuses are included in the result
evidence. An unrelated worker crash alone cannot satisfy the assertion.

The affected four normal/partial cancel cases are rerun separately with:

```sh
/tmp/trr-engine-eval-venv/bin/python scripts/ingestion_engine_eval/run.py --runtime-dir /tmp/trr-engine-eval-cancel-terminal --cancel-only
```

Earlier measurements and logs remain preserved. This correction changes no
workflow scheduling, business adapter behavior or unrelated acceptance claim.
All four affected cases pass: normal/partial DBOS report `CANCELLED` before
resume; normal/partial Temporal report `CANCELED` before reset. Completed receipt
timestamps and recorded attempts remain preserved. The new evidence suffix is
`cancel-terminal`. At that review boundary, history partitioning and workflow
batching were still unverified. The later DBOS supplement below tests bounded
partitions. Native continue-as-new and global retention remain **UNVERIFIED**.

## Supplement: DBOS bounded partitions and atomic continuation

Run the independent finite fixture with:

```sh
/tmp/trr-engine-eval-venv/bin/python scripts/ingestion_engine_eval/dbos_partitioned.py --runtime-dir /tmp/trr-dbos-partitioned-1000
```

**PASS:** the same business run and owner epoch process 1000 targets in twenty
deterministically named workflows, each limited to fifty targets. The pinned
application version is `partition-v1`. Every workflow has fifty adapter steps
and one continuation-commit step: the observed maximum is **51 recorded steps
per workflow**, and the retained total is **1020 steps** across all workflows.

The continuation step locks the business run, verifies the owner and committed
receipts for its target range, and commits three facts in one Postgres
transaction: the advanced business cursor, the completed partition intent and
the next partition's outbox intent. A deterministic workflow ID is stored in
that intent before any next-partition engine submission. A non-workflow
dispatcher reads those durable intents and submits using the stored identity.
There is no growing parent orchestration workflow or process-memory next link.

The first process is killed after partition zero commits cursor 50 and the next
intent, but before DBOS acknowledges its final step. At the crash boundary:
fifty receipts exist, partition zero's intent is complete and partition one's
intent is pending. The replacement starts against the same DBOS metadata and
version, recovers the interrupted workflow, and deterministically submits the
pending partition. All twenty intents finish, the cursor reaches 1000, and
there are exactly **1000 accepted dispatches, 1000 receipts and 1000 recorded
adapter attempts**. Repeating a completed partition's deterministic submission
changes none of those business counts. Thus acknowledged target steps were not
refetched when the continuation step recovered.

Observed measurements, including initial startup, forced crash and replacement:

| Quantity | Observed |
|---|---:|
| Elapsed | 17.767 seconds |
| PostgreSQL statement calls, session-sampler SELECTs excluded | 20,817 |
| Sampled peak sessions, observer excluded | 4 |
| Retained DBOS table/index/TOAST bytes | 974,848 |
| Retained business table/index/TOAST bytes | 630,784 |
| Whole database bytes, including PostgreSQL system objects | 9,828,019 |

The evidence suffix is `dbos-partitioned-1000`; the actual completed runtime is
`/tmp/trr-dbos-partitioned-1000-6f437318`. Raw histories, continuation-boundary
state, source hash, worker logs and cleanup evidence are retained separately.
The probe uses system pool two and executor-thread limit two; the previous
pool-one existing-service proof is separate and is not silently combined with
these measurements.

The 20,817-call counter excludes only `pg_stat_activity` session-sampler SELECTs.
It includes checkpoint polling, snapshots, outbox scans, counter reads and other
test observer traffic. It is a measured harness-total count with that one
exclusion, not an isolated engine-query count. The saved measurement-scope
sidecar states this explicitly; the archived numeric result is unchanged.

This closes the local finite **per-workflow history partitioning** criterion for
DBOS. Total retained database history still grows with all processed targets;
this is not an infinite-workload, retention, native continue-as-new, Temporal
partitioning, production-lifecycle or live-provider proof. Production must
implement the approved atomic cursor/outbox continuation and the same version
and ownership guards, then pass its connected persistence/runtime gates.

### Continuation cancellation repair

Independent review found that the original partition continuation checked the
owner epoch but did not check the business cancel flag under the same run lock.
It could therefore advance the cursor and create a successor after cancellation
between the final target receipt and continuation. The dispatcher also lacked a
cancel filter. Both checks are now fixed in the evaluation source.

The focused real-DBOS probe pauses inside the continuation step after the first
fifty receipts exist, cancels the business run, then releases that boundary. The
continuation checks owner and cancellation while holding `FOR UPDATE`; it returns
without advancing the cursor, completing the old intent or creating a successor.
The dispatch query joins the run and excludes cancelled runs. The test proves:
fifty receipts and attempts retained, cursor zero, only partition zero's pending
intent retained, no successor, and no dispatchable intent for the cancelled run.
This focused probe passes in **1.603 seconds**.

```sh
/tmp/trr-engine-eval-venv/bin/python scripts/ingestion_engine_eval/dbos_partitioned.py --runtime-dir /tmp/trr-dbos-partition-cancel --cancel-continuation-only
```

The saved suffix is `dbos-partition-cancel`; its manifest hashes the changed
source and references the earlier 1000-target source hash. The earlier passing
1000-target run remains evidence for its unchanged non-cancelled behavior and
is not represented as a rerun against this repair. Its numeric measurements and
logs are preserved. The focused cancellation check covers the changed branch;
it is not an additional claim about cancellation resume generations or live
providers.

## Supplement: legacy canonical persistence baseline captured now

The baseline was captured **now from the unchanged legacy path**, after candidate
implementation. It is not historical before-implementation timing. The real
legacy calls are `catalog_ingest._upsert_instagram_post`,
`_upsert_shared_catalog_instagram_post`, and the maintained canonical catalog
sync. The candidate calls `ingestion.canonical.write_post` and its independent
normalizer. Both write the same retained sanitized
`tests/fixtures/ingestion/canonical_capture.json` post into private Postgres using
the retained canonical schema, with a caller-owned transaction per target and
the same explicit observation clock. No persistence algorithms were replaced.
The legacy scraper DTO boundary is supplied by a fixture-backed object whose
`to_dict()` returns that capture; this is not a test of transport parsing.

The retained schema omits the maintained partial observation index required by
the actual legacy `ON CONFLICT` statement. The harness adds that exact index to
its private database only. It also creates a valid private legacy run identity
for both paths' foreign keys. All implicit/configured database entrypoints and
non-loopback sockets fail closed. No production connection, transport, object
storage, worker, API, or engine execution is used in this comparison.

```sh
PYTHONPATH=. .venv/bin/python scripts/ingestion_engine_eval/legacy_resource_baseline.py --runtime-dir /tmp/trr-legacy-baseline --targets 30
```

Three alternating trials persisted thirty new post identities per path. Checked
field parity passes for post, account-catalog and canonical identity/content,
media URLs, basic metrics and publication time. Counts match for observations,
legacy references, entities and media assets. Entity/media payload contents,
all raw aliases, target outcome accounting and source-unavailability behavior
are not exhaustively compared by this narrow fixture. A persisted target here
means the canonical transaction committed; it is not a durable run receipt.

| Narrow persistence measurement | Legacy | Candidate |
|---|---:|---:|
| Statement calls / 30 persisted targets, warmed schema cache | 603 | 783 |
| Calls per persisted target, including fixed counter overhead | 20.1 | 26.1 |
| Sampled peak worker sessions | 1 | 1 |
| Median elapsed across three trials | 1.126139 s | 1.149369 s |
| Individual elapsed trials | 1.753799 / 0.674970 / 1.126139 s | 1.149369 / 2.586783 / 0.520810 s |

**The initial candidate had a 29.85% observed statement-count regression at this
seam; it was not accepted or waived.** The repair and affected measurement follow
below. The initial median elapsed difference was about +2.06%,
but the large timing spread does not support a strong latency conclusion.
The first legacy trial made 68 additional cold schema-column probes (671 total
calls); subsequent legacy trials used its maintained column cache (603). The
counter includes BEGIN/COMMIT, one reset call and fixed counter-transaction
overhead, while excluding only the session-sampler SELECT. Post-measurement
parity reads and query-attribution reads are outside that measured interval.
Session sampling is every 10 ms; caller-owned serial connections constrain both
paths to one active worker session. This is not a deployed pool-capacity test.

A single extra paired trial captured normalized query shapes for attribution:

| Per-target statement category | Legacy warmed | Candidate | Difference |
|---|---:|---:|---:|
| Initial/preservation row reads, including media | 2 | 5 | +3 |
| Canonical identity advisory lock | 0 | 1 | +1 |
| Entity write statements | 2 grouped | 5 individual | +3 |
| Canonical social_posts writes | 2 | 1 | -1 |
| Other writes and transaction commands | Same | Same | 0 |
| Net | | | **+6** |

The candidate's five reads are three independent existing post/catalog/canonical
rows and two media-preservation rows. Safe optimization candidates for parent
review are a single locked media-range read, one bulk entity upsert with the same
composite conflict/update semantics, and a single SQL statement with separately
locked CTEs for the three existing model reads. Preserve the advisory identity
lock, including its protection for absent rows, all row locks, timestamp/metric
monotonicity and atomic writes. None of those production optimizations is made
by this assignment.

Evaluated `canonical.py` SHA-256 is
`8a48ce84c3366b4d90267e47d27fa86dba8ff3cf5119ec551f20a24595618988`.
The current source still matched that hash at attribution time, so this baseline
includes the parent's monotonic-views repair at that revision. Legacy source
files had no working-copy diff. Full fixture and source hashes, raw row parity,
individual timings and normalized SQL query shapes are retained under the
`legacy-canonical-baseline` evidence suffix. Source/log evidence for the two
private harness setup repairs is retained in the temporary directories; those
were a dictionary-cursor count lookup and a missing private fixture run row.

**Full listing/detail ingestion resource acceptance remains UNVERIFIED.**
`PostActivity.execute` additionally owns acquisition, cancellation/request checks,
source evidence, parser/coverage validation and receipt commit. Legacy
`run_durable_instagram_details` and listing orchestration also bind job claims,
target accounting, account/runtime policy, source cursors, transport and deferred
flush behavior through the legacy provider namespace. The matched sink seam
does not execute those orchestration paths, and substituting hand-written
accounting would produce misleading full-pipeline numbers. A connected fixture
adapter for those policy/claim/transport seams is still needed to compare their
sessions, coordination queries and end-to-end ingestion time. The current
canonical query regression must be explained/repaired and the connected
resource gate must pass; these narrower figures cannot establish full-pipeline
acceptance. The following repair addresses the isolated sink-query regression.

### Candidate batching follow-up, retained legacy baseline

The parent changed only candidate canonical batching: five entity upserts become
one bulk upsert, and two media preservation reads become one locked range read.
The identity advisory lock, existing-row locks and merge/monotonicity semantics
remain. This assignment reran only the changed candidate, with the same retained
fixture and three trials, and reused the unchanged legacy baseline. Fixture,
schema and both legacy source hashes are checked before using retained rows.

```sh
PYTHONPATH=. .venv/bin/python scripts/ingestion_engine_eval/legacy_resource_baseline.py --runtime-dir /tmp/trr-candidate-batched --targets 30 --baseline-dir scripts/ingestion_engine_eval/evidence/2026-10-16
```

| Affected narrow measurement | Retained legacy | Batched candidate |
|---|---:|---:|
| Statement calls / 30 persisted targets | 603 warmed | 633 in each trial |
| Statement calls per persisted target | 20.1 | 21.1 |
| Sampled peak worker sessions | 1 | 1 |
| Median elapsed | 1.126139 s | 0.593248 s |
| Candidate individual times | N/A | 0.677174 / 0.593248 / 0.501478 s |

The remaining difference is **one statement per target**, or **+4.98%** in the
observed total. The six-statement difference falls by five after batching. This
is within 10% at the narrow canonical persistence seam; it does not waive or
pass the full listing/detail ingestion resource gate. Elapsed times use the
retained earlier baseline, have wide host variance and do not establish a
causal speedup. Checked field and count parity passes against retained rows.
Entity/media payload completeness remains outside this benchmark's parity check.

The measured candidate source SHA-256 is
`3dc4d313424316be7c234411741070d4a58e9f88c11755e1750a860140cb66b9`.
Results, individual rows, query shapes, cleanup and source metadata use the
`candidate-canonical-batched` suffix. The initial canonical measurements remain
unchanged at their earlier hash. A retained-row comparison initially treated
archived timestamp strings and current Python datetimes as different types;
the harness now compares both through the same JSON representation. Reanalysis
of the saved actual rows confirms parity without another database run, and the
original comparison diagnostic is preserved.

### Actual details-phase coordination resource supplement

The connected details slice now runs both maintained execution paths against
private PostgreSQL 17.10 with fake HTTP and the same retained capture. The
baseline was captured now from unchanged legacy code, not before implementation.
`details_phase_resource.py` calls the actual legacy
`run_durable_instagram_details`, including target claims, ownership/cancellation
checks, heartbeat, request accounting, DetailTransport and batched persistence.
The candidate calls actual `Scheduler.tick`, `Worker`, `DetailSource` and
`PostActivity`, including persisted dispatch/decision state, source evidence and
business receipts. Both use the legacy native parser and DetailTransport with
an injected HTTP response derived from the same capture. The fixture projects
one image per post; this does not test carousel completeness.

The measurement starts with catalog rows, frozen target manifests and outer job
ownership already prepared. It ends after committed detail targets and candidate
partition completion. It excludes listing discovery, comments, media downloads
and storage, outer command launch/job claim/dispatch, outer rollup, Modal API and
runtime, DBOS SDK history/metadata, live source latency and production database
pool/network behavior. Admission succeeds locally and dispatch invokes Worker
synchronously. The optional legacy `scrape_workers` registry is absent; actual
feature detection skips that registry emission, while job heartbeat updates and
ownership checks execute. Thus this is a connected details-phase comparison,
not full pipeline or live acceptance.

```sh
PYTHONPATH=. .venv/bin/python scripts/ingestion_engine_eval/details_phase_resource.py --runtime-dir /tmp/trr-details-phase-sealed-6f437318 --targets 12 --repetitions 3
```

Three alternating trials per path each committed 12 targets with 12 source
requests. Both produced 12 Instagram post rows with aggregate 144 likes and
48 comments. Source requests assert that no database connection is checked out.
The exact checked-out and 10 ms sampled peak was two sessions on both paths.
All task-owned PostgreSQL processes stopped after validation.

| Measured details-phase resource | Legacy | Candidate |
|---|---:|---:|
| Raw SQL calls, warmed schema cache | 1,187 | 1,341 |
| Execution SQL calls, explicit observer/assertion exclusions below | 1,184 | 1,334 |
| Execution calls / committed target | 98.6667 | 111.1667 |
| Execution database connections opened | 339 | 218 |
| Peak checked-out / sampled worker sessions | 2 / 2 | 2 / 2 |
| Raw elapsed trials, seconds | 1.808582 / 1.718209 / 1.634625 | 1.615934 / 1.455836 / 1.380866 |
| Raw median elapsed, seconds | 1.718209 | 1.455836 |

The execution SQL difference is **150 statements, +12.6689%**. This is not
silently waived or reduced by another benchmark run. PostgreSQL
`pg_stat_statements` supplies counts, including implicit BEGIN/COMMIT. Only the
session sampler SELECT is filtered from raw counts. Each path includes three
fixed measurement calls: reset SELECT, COMMIT of the reset transaction and BEGIN
of the counter-snapshot transaction. Candidate raw counts additionally include
two harness verification SELECTs and their BEGIN/COMMIT; these four assertion
calls are explicitly excluded from execution counts. The raw candidate timer
also includes that small assertion transaction; no execution-only latency is
claimed. First legacy cold-cache trial made 1,302 raw calls, while both later
legacy trials made 1,187; candidate made 1,341 in every trial. Raw trial data is
retained unchanged. Local fresh connections, instrumentation and host timing
variation prevent causal speedup or deployed pool-efficiency conclusions.

Normalized SQL shapes account for every execution call in a warmed trial:

| Execution statement category / 12 targets | Legacy | Candidate | Difference |
|---|---:|---:|---:|
| BEGIN/COMMIT | 678 | 436 | -242 |
| Legacy job/run/heartbeat, deadline and target lifecycle; candidate owner/target/phase | 374 | 412 | +38 |
| Shared transport request gate | 60 | 60 | 0 |
| Canonical models and preservation | 72 | 216 | +144 |
| Persisted dispatch lifecycle | 0 | 96 | +96 |
| Scheduler decisions and partition journal | 0 | 65 | +65 |
| Immutable source evidence, insert and capture-time lookup | 0 | 24 | +24 |
| Business receipt lookup and insert | 0 | 24 | +24 |
| Final scrape-run status update | 0 | 1 | +1 |
| Total | **1,184** | **1,334** | **+150** |

Dispatch/journal and evidence/receipt statements implement required durability;
their presence is distinct from unexplained SQL overhead. The table attributes
query categories, not a proof that each call is irreducible or optimally batched.
The parent owns the judgment about justified overhead and the acceptance rule
for an unexplained regression above 10%; this supplement does not choose an
engine, optimize production code or declare that gate passed.

**Output scope review flag:** all three model families do not have equal field
coverage in this actual details-path comparison. Legacy details batches
`instagram_posts`, canonical `social_posts` and associated payload/ref/observation
writes, but leaves the preseeded shared catalog unchanged. Candidate
`canonical.write_post` also refreshes catalog fields/payload and writes the
second legacy ref and observation. The +144 canonical statements therefore
include additional catalog coherence, per-target rather than legacy buffered
writes, preservation and identity locking. These are not 144 statements proven
to be unnecessary. Common post metric/accounting checks pass, but this harness
does not assert all-three-model field parity or full entity/media payload
parity. The earlier narrow sink benchmark deliberately invoked both legacy
catalog and post sinks and their canonical sync to compare checked fields in all
three models; its +4.9751% result applies only at that equalized seam.

Saved source hashes match current files when this supplement was archived:
canonical `3dc4d313424316be7c234411741070d4a58e9f88c11755e1750a860140cb66b9`,
Worker `e1d74d36f7b6c577de3e941b1389378e60e3fcea585802054f17c655d71ffbcd`,
WorkflowStore `a752376c2927d26b7f3224078a6f6181d52b19d1695d39434eadf5ff97e4ec8d`
and scheduler workflows
`97800439daa185b8972ebf8b45ebb28bf754a384c6086f16b021f94acc6434ea`.
Legacy catalog hash remains
`0496172d32f82ba38084741f294c237d28ccc2f7528130968adca4784cc2b445`.
The artifact records all eight source hashes, fixture input, harness hash,
command, individual results, raw and adjusted accounting, classified normalized
queries, all six query snapshots and cleanup. Hashes were captured at completion
and rechecked while archiving; this harness did not independently guard source
identity at both trial start and end during concurrent work. Evidence uses the
separate `details-phase-resource` suffix. Earlier engine and sink evidence is
unchanged. Remaining all-stage, equal-output and live resource release gates
remain unverified.
