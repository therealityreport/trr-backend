# Representative local resource comparison, v3

Status: both arms and common-output assertions passed. **Resource acceptance remains parent-owned.** The candidate has a large measured SQL and network cost increase. Every SQL call is assigned to an auditable category; this does not prove that each extra call is necessary, efficient or justified. No threshold pass is claimed.

## Inputs and comparable boundary

Exactly one serial pair, using unchanged legacy source `17f978d101a26634c96ac62f6f352f8ae9bc7487` and candidate `595fda4b238f04b9daad382386c00162d6eab594`. This is a legacy baseline captured now, not a before-implementation timing. Full 2,468-file source manifests, schema, fixture, harness and package versions were sealed before timing and checked afterward. Later `6f38e07d` guarded-error classification is outside measured lineage; valid mocked responses do not exercise that error-only delta.

Frozen harness SHA256:

- `full_pipeline_resource.py`: `404885a093d65684f43359a613b3bed5722ca04c73a205d6b54a56c43df70534`
- `full_pipeline_sources.py`: `31b1e06f610feff549b48c3330d661fd279c0f47d2e7a1f7c54b87b2540ddfdf`
- Pre-run workload contract: `3b6383126ef06667dfa95f0289d15a66bd844a9a9b8f3ffe1980b58895330510`

The same 12 posts, two listing pages, 12 parent pages, 12 reply pages, three avatar identities and 27 distinct PNG source bodies drive both arms. Post thumbnail/media share an exact URL. Real PostgreSQL, native persistence, real variants and DBOS 3.3.0 execute; external source/browser/CDN/storage and provider dispatch are fixtures. Unknown external requests fail closed.

Legacy uses 27 actual `claim_and_process_social_job` children. One explicit driver owns the job plan; native automatic media followups and second provider dispatch are disabled through maintained flags. Native claim, progress, registry, heartbeat, finish and pool cleanup remain. Candidate uses real CommandService/Admission, DBOS/lease, scheduler and 38 native `run_ingestion_activity` children, retaining reservation validation, bounded output, deadline and Worker.main. Thus both use local per-job child processes, while native granularity differs. No metric divides by different job counts: **per committed post means the same 12 post identities for this complete workload**.

Excluded: bootstrap/migrations/seed; observer-role SQL; post-run parity snapshots; legacy HTTP launch/planner/periodic sweep; candidate account-specific canary policy/HTTP wrapper; live Modal/container/network/Supavisor behavior. Included: all workload-role SQL, transaction/session setup, capability checks, native driver queries, admission, locks, request checks, durability, SDK and idle polling. Raw totals include one reset SELECT per arm; adjusted totals remove only that one. No prior benchmark numbers are added to this pair.

## Observed result

| Metric | Legacy | Candidate |
|---|---:|---:|
| SQL at observed business completion, raw | 6,090 | 15,873 |
| SQL at completion, adjusted | **6,089** | **15,872** |
| SQL through bounded process exit, raw | 6,103 | 18,223 |
| SQL through exit, adjusted | **6,102** | **18,222** |
| SQL per committed post, completion | 507.4167 | 1,322.6667 |
| SQL per committed post, through exit | 508.5 | 1,518.5 |
| Observed business completion, seconds | 69.6436 | 57.7754 |
| Full bounded lifecycle, seconds | 70.0430 | 86.8038 |
| Completion-to-exit tail, seconds | 0.3994 | 29.0284 |
| SQL in that tail | 13 | 2,350 |
| Sampled aggregate session peak | 2 | 5 |
| Final workload sessions / SQL errors | 0 / 0 | 0 / 0 |
| Native child processes | 27 | 38 |
| Unique physical hosted objects | 147 | 147 |

Completion increase: `(15,872 − 6,089) / 6,089 = 160.6668%`, or **+9,783 calls**. Full lifecycle increase: `(18,222 − 6,102) / 6,102 = 198.6234%`, or **+12,120 calls**. Full delta equals completion delta plus tail delta: `9,783 + (2,350 − 13) = 12,120`.

One instrumented serial trial with fake I/O and real image processing does not establish a statistical or causal speed improvement. Timer values come from `measurement.json`; `result.json.seconds` also includes later bookkeeping and is not the workload timer. Completion is the observer's first persisted business completed-state observation, not an engine-quiescence timestamp; an in-flight transaction can cross that observation.

## Complete SQL attribution

Counts include session/transaction statements. Categories are deterministic first-match SQL-shape/role classifications in `representative-analysis.py`; all normalized statements and server execution times remain in the JSON. Multi-table statements appear once under that rule. Zero unclassified calls means arithmetic coverage, **not zero unjustified cost**.

| SQL category | Legacy completion | Candidate completion | Legacy full | Candidate full |
|---|---:|---:|---:|---:|
| Avatar registry and hosted-avatar linking | 75 | 75 | 75 | 75 |
| Business advisory/identity locks | 107 | 98 | 108 | 98 |
| Business dispatch, decision and partition journal | 0 | 466 | 0 | 466 |
| Business evidence, receipts and comment-page durability | 0 | 276 | 0 | 276 |
| Business run/target ownership, request accounting and manifest state | 0 | 3,558 | 0 | 3,558 |
| Business schema capability probes | 875 | 0 | 875 | 0 |
| Business transaction/session setup | 2,122 | 7,762 | 2,125 | 9,164 |
| Candidate capacity admission and fresh-session probes | 0 | 78 | 0 | 78 |
| Comment models, audit and inventory | 184 | 132 | 184 | 132 |
| Coordinator pending-outbox/partition polling and error scans | 0 | 980 | 0 | 1,447 |
| DBOS SDK, lease, listener and SDK session SQL | 0 | 1,577 | 0 | 2,058 |
| Legacy durable detail targets/receipts | 45 | 0 | 46 | 0 |
| Legacy job/run lifecycle and progress | 1,712 | 2 | 1,718 | 2 |
| Legacy worker registry/health | 448 | 0 | 449 | 0 |
| Native core/storage context and source status | 101 | 0 | 101 | 0 |
| Post/catalog/canonical models, preservation and media inventory | 312 | 540 | 313 | 540 |
| Shared request pressure, pacing and auth cooldown | 108 | 328 | 108 | 328 |
| **Total adjusted** | **6,089** | **15,872** | **6,102** | **18,222** |

At completion, the largest added categories are transaction/session **+5,640**, run/target owner/accounting/manifest **+3,558**, SDK/lease/listener **+1,577**, coordinator polling **+980**, dispatch/partition journal **+466**, and evidence/receipt/comment-page durability **+276**. Legacy job/registry costs fall by 2,158 and schema probes by 875; the remaining smaller deltas are in the table. Evidence and receipts alone do not explain most of the increase.

Measured transaction shapes: legacy completion has 659 BEGIN, 659 COMMIT, 659 search_path SET, 89 set_config and 56 timeout SET calls =2,122. Candidate has 2,565 BEGIN, 2,564 COMMIT, 2,565 search_path SET, 56 timeout SET and 12 repeatable-read SET =7,762. The completion snapshot catches one candidate transaction in flight. Full counts are legacy 660 of each basic statement plus145 other setup=2,125; candidate 3,032 of each plus68 other setup=9,164.

Three candidate full SQL shapes contribute 3,126 calls: 1,535 run-row `FOR UPDATE` reads, 1,164 owned-target `FOR UPDATE` reads, and 427 target request increments. Frozen `persistence.py:376` `check_owner` performs the run/generation/state/expiry/owner fence; callbacks before source and storage operations connect these shapes to ownership and request accounting. **Shape counts are measured; this source-level causal mapping is an inference, not per-call stack tracing.** No optimization, fence removal or assumption that all repeated checks are irreducible was made.

The full coordinator poll category is exactly 684 pending-outbox queries +684 pending-partition queries +79 engine-error scans =1,447. Admission is39 native capacity snapshots plus39 fresh-session SELECT probes=78 (one launch and38 targets). The maintained native probes were retained; the harness does not inject fresh connections to hide a pool1 issue.

## Why the coordinator tail exists

The candidate tail's 2,350 calls are **1,402 business transaction/session statements +467 outbox/partition/error polls +481 SDK/lease/listener statements**. Its remaining 29.0284 seconds are measured rather than added as a constant. Legacy's13 tail calls are three transaction statements, one advisory lock, one detail-target statement, six job/run statements, one worker-registry statement and one post-model statement.

Frozen `workflows.py:238` `DBOSAdapter.run_bounded` acquires the global session advisory lease, starts the SDK, then at lines270–297 polls until `lifetime_seconds −5` regardless of a particular run's completion. The harness uses90 seconds; initialization and cleanup share that bounded lifecycle. Outbox/partition queries are account-independent and filter active generations, so this process remains available for other eligible work. This is intended **global coordinator availability**, not a requirement that a completed run spend29 extra seconds finishing its own targets. `workflow_store.py:451` completes the partition and persists business state; the observer measures `instagram_ingestion_runs.state='completed'`. DBOS terminal status, business completed state and bounded coordinator exit are separate facts; no immediate engine/run-terminal synchronization is claimed.

At shutdown `workflows.py:300` calls DBOS.destroy and then terminates the dedicated process while retaining the lease. DBOS3.3 destroy does not prove active executor threads joined. `runtime.py:48` owns the outer115-second process bound/reaping in production; this fixture executes the real bounded adapter in its task-owned child and measures its exit. The outer provider maintenance wakeup and live shared-account traffic are excluded. A shared busy coordinator could amortize this tail across runs; this single-run pair measures the whole cost and does not assume such amortization.

## Sessions, configured caps and cleanup

| Resource | Expected local setting/topology | Observed evidence |
|---|---|---|
| Native default pool per process | min1/max2 | All28 legacy and40 candidate resolved receipts match |
| Native control/progress/profile per process | min1/max1 each; close-after-return=true | Same receipts; caps not raised |
| Runtime markers | Modal=true, local_dev=true | Receipts match; local-dev bypass of deployed Modal max1 ceiling preserved |
| SDK | pool max1/overflow0; two executor threads | Frozen RuntimeConfig/workflows.py:127; SDK system metadata is real |
| Additional SDK-role sessions | dedicated LISTEN +session lease | Peak role/application pattern consistent with topology |
| Aggregate business sessions at sampled candidate peak | coordinator +one serial native worker can each own control1 | Two `trr-backend:social_control` active sessions |
| Aggregate SDK-role sessions at same sample | pool/listener/lease three sessions | Two `trr-instagram-dbos` idle +one blank-app idle |
| Local aggregate peak | No asserted global cap of5 | Legacy2/candidate5 at10ms sampling; lower bound |
| Exit | No owned child/session/helper remains | 27/38 child records reaped; cleanup killed/skipped empty; final sessions empty; both PostgreSQL stopped |

Peak sample at13.306303 seconds contains the five sessions above. The sampler records role/application/state totals, **not backend PID**. Mapping those sessions to coordinator/worker/SDK/listener/lease is inferred from frozen process topology and pool receipts, not directly observed PID ownership. Independent per-process pools explain why two control connections do not violate an individual max1 setting. Receipts prove resolved configuration; sampled aggregate counts do not establish every transient per-process maximum. All38 candidate targets had one attempt. No local parallel provider expansion or pool-cap workaround occurred.

The parent independently supplied the deployed effective default1/2 and control1/1, session route and close=true, with local_dev=true bypassing the Modal ceiling. This benchmark preserves those settings but uses direct loopback sessions. It **does not prove live Supavisor session preservation, aggregate connection limits, grants or Modal concurrency**. The earlier all-one diagnostic also changed close-after-return and is not a pool-size-only comparison.

## Semantic and network differences

Assertions passed for exact12 common source identities and catalog/post/canonical metadata;12 parents+12 replies with correct text/likes/topology/nonmissing state;3 avatar identities; original/thumbnail/comment/avatar source-role bytes and dimensions; and all10 maintained JPEG/WebP variants per post. Both arms contain147 physical objects. Twelve literal comment-media URLs differ because independently generated comment-row UUIDs enter native object keys; role/source/body parity is asserted without rewriting keys. Raw snapshots retain those differences.

Candidate additionally fills canonical asset hosted URL/mirror status where legacy leaves null. This is additional model-coherence work, not exact-all-field parity. The post/catalog/canonical category also includes preservation and media inventory; its +228 completion calls are not asserted to be exclusively those canonical writes. Stable detail values do not cover catalog-on-edit behavior; empty entity arrays do not establish nonempty entity parity. Videos, carousels, assignment inference and live rate limits are outside this fixture.

| Operations | Legacy | Candidate |
|---|---:|---:|
| Logical listing/details/parent/reply requests | 38 | 38 |
| Additional browser warmups/profile-info | 3+1 | 2+0 |
| CDN downloads | 27 | 60 |
| Physical storage writes | 147 | 147 |
| HEAD misses / hits | 0 / 0 | 147 / 33 |
| Client close events, not network calls | 0 | 12 |
| Total source/CDN/storage network operations | 216 | 427 |

`427 −216 =180 HEAD +33 CDN −2 Instagram =211` additional operations. Each of three stable avatar sources is downloaded once by legacy versus12 times by candidate, explaining all33 additional CDN calls. Legacy reuses its mirrored avatar-registry entry before downloading; candidate media host downloads/hashes and then checks idempotent storage in each snapshot. HEAD operations support the content-fenced conditional storage path. These are measured policy/implementation costs, **not a declaration that every extra download or HEAD is required**. The native candidate's target.requests427 includes40 Instagram requests (including two warmups),60 CDN and327 storage requests; target requests are not source-only calls. SDK event totals147 versus339 also include the12 candidate client closes; object count alone is not an operation count.

## Preserved evidence and final limits

- Pair directories: `representative-legacy-v3-01/` and `representative-candidate-595fda4b-v3-01/`, adjacent to this report.
- `measurement.json`: raw queries/server times, completion/full snapshots,10ms samples, observer health and final sessions.
- `sealed-inputs.json`, `verified-inputs-after.json`, schema dump/inventory and27 fixture assets: enforced lineage and prerequisites.
- `snapshot.json`, `objects.json`, `output-assertions.json`, `matched-output-comparison.json`: actual fields/roles/bytes and literal differences.
- Transport/storage journals and operation-counts; dispatch timings; child process journal, pool receipts and cleanup/error receipts.
- `representative-attribution.json` and `representative-analysis.py`: deterministic posthoc category/equation analysis; no database access or workload rerun.

No further pair, optimization or unrelated checks were run. The earlier all-identical image pair remains a separate stress case. This pair closes the missing bounded matched measurement, **not the parent acceptance decision or live release gate**. The large increase is localized to explicit mechanisms and work differences; whether its topology and cost satisfy the reject-unexplained-over10% requirement remains a parent judgment.
