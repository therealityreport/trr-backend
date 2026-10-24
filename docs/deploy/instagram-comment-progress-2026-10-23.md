# Instagram comment progress and deployment — October 23, 2026

## Scope and ownership

Continue the January bravotv run from chat `01a16af5-f057-71b3-b515-3cd3395539e6`, improving speed without dropping comments or replies. Implementation owner: continuation chat `01a16b47-029d-70a0-8503-992e012a2b64`. Existing backend checkout and branch `chore/repo-hygiene-2026-09-25` retained. Unrelated hygiene, media and test changes were excluded from this batch.

Run: `2aef72ed-110c-4af9-b2b3-4a45da170a32`, generation 1, January 1–February 1 UTC. Public, anonymous access only. Full-history remains disabled; no schedules were created. Supabase target: `vwxfvzutyufrkhfgoeaa`.

## Implemented changes

- Strict native traversal deduplicates by Instagram identity, preserving distinct comments and replies with identical author/text. Legacy content-deduplication defaults remain compatible.
- Fresh Relay parsing uses the largest valid native reply-count alias, matching retained evidence and resume parsing. A zero alias cannot hide a positive count.
- Each observed page copies only changed checkpoint records, avoiding a full JSON copy of all saved comments before every reply-page commit.
- One SQL statement updates the target checkpoint and inserts its page receipt atomically. It transmits the serialized checkpoint once. Existing ownership, run, lease and expected-checkpoint fences remain in force; a receipt failure rolls back the update.
- Reply responses now contribute to request/byte telemetry.

## Validation and measurement

273 ingestion/comment/resume tests and 35 focused legacy-fetcher tests passed (308 total). Scoped Ruff, formatting and diff checks passed. Independent review found no blocking issue. Tests include identical-content distinct IDs, cross-page repeated IDs, conflicting count aliases, one checkpoint transmission and rollback after a duplicate receipt.

Eight stale assertions were reproduced against unchanged HEAD production modules before correction: four reply-count aliases expected zero despite positive native counts; four source-gap cases expected saved comments to be marked missing. Corrected assertions preserve the existing completeness safeguards.

A synthetic same-input, same-output benchmark measured checkpoint preparation plus final serialization:

| Fixture | Before | After | Reduction |
| --- | ---: | ---: | ---: |
| 4,743 comments / 1,325 lanes | 16.69 ms/page | 6.17 ms/page | 63.0% |
| 15,000 comments / 14,000 lanes | 150.68 ms/page | 49.41 ms/page | 67.2% |

These figures exclude network and database latency. Live windows differ in workload and do not establish an overall throughput multiplier.

## Deployment and rollback

Deployed Modal **v139**, tag `ig-comment-speed-ad314c5ba6e2`, at October 23, 2026 20:33:37 Eastern, to **admin-56995 / main / trr-backend-jobs** (`ap-DkLTRoSvqhbkGO7fHyHxrD`). Source was `e8d731b4436219e15d2ae3a157d11ede495093c2` plus this reviewed source batch. Runtime manifest SHA-256: `ad314c5ba6e24df9997c94564bf28221e0f501e2b5e23a08ce9b61ed5b816407`.

The maintained deployment wrapper passed readiness verification for `main` and a cold HTTP 200 health canary. A fresh-container smoke verified all 652 packaged Python files, the two changed module hashes, strict identity/count behavior and the closed full-history gate. It made no Instagram requests or database connections. All 1,013 unchanged runtime files from the newer v138 Reddit deployment were preserved. Cloud rollback is v138, not v137.

The local worker uses installed release `ig-comment-speed-ad314c5ba6e2`. Handover drained the old coordinator before starting the new release; binding and launch configuration were preserved for rollback. Current local rollback release is `ig-admission-a62275980c30` (v137). Unused v136 `ig-missing-data-77cac4944569` was checked for process, launch configuration and open-file references, then moved to actual Trash. No active release was removed.

## Run state and attempt authorization

User instruction: “Extend to whatever.” Routine productive-attempt budget extensions within this January run no longer require another question. Error, backoff, identity and completeness safeguards still apply.

- `DTiwjjpAYzG`: budget 12 → 128; all counters/checkpoints preserved. Subsequently resumed to attempt 14 with 2,028 requests and 4,760 observed comments at 20:58:59 Eastern.
- `DTgXh94kXyo`: budget 24 → 128 at 20:59:50 Eastern, preserving attempt 21, all row fields other than the budget, and 2,189 retained response pages.

Both operations are recorded in the ingestion command journal. They do not alter the frozen run intent or reset progress.

At 20:58:59 Eastern: 286/286 post details satisfied; 170 comment targets committed and 116 unresolved; all 286 media targets unresolved. The new worker had saved 2,415 response pages across 10 posts, including 2,316 reply pages. Six comment targets had active ownership. Policy revision 7 still requests six workers. An initial diagnostic showing a four-worker cap used shell defaults; the installed launch command explicitly raises those caps to eight. No cap change was required.

A strict saved-evidence audit at 20:36:58 Eastern verified 10 targets, found 156 source-count gaps and left 120 targets pending. That earlier audit is separate from the later progress snapshot. A committed native traversal is not a final completeness certificate. Final acceptance remains false.

## Remaining known issues and next actions

| Issue | Next action / constraint |
| --- | --- |
| Native traversal can end below Instagram's reported total | Retain exact gaps and raw evidence; compare IDs/counts across authorized native observations. Never lower expected totals or mark unseen saved comments missing to manufacture success. Some source omissions may remain unresolvable through anonymous access. |
| Reply counts are often absent | Exhaust each unknown reply branch. Skip only branches with valid native leaf/zero evidence; empty responses still provide required proof. |
| Large posts need many timed attempts | Continue from saved cursors with extended budgets. Separate productive continuation from repeated failures; preserve retry/backoff guards. |
| Full checkpoint snapshots still grow with observed IDs/lanes | Current copy and transmission costs are reduced. A future incremental checkpoint/receipt design needs explicit replay, crash and ownership equivalence tests before replacing full snapshots. |
| Per-page source and persistence latency remains | Measure comparable live workloads before further concurrency or batching changes. Keep six-worker policy; higher concurrency previously failed database-capacity qualification. |
| Fetch errors or unsupported nested topology may prevent settlement | Preserve failure evidence, honor backoff, and fix only demonstrated parser/transport defects; do not silently truncate branches. Latest progress snapshot has one unresolved `comment_fetch_failed` checkpoint. |
| Earlier committed targets were collected before these fixes | Reconcile saved native evidence and count gaps after the pending traversal finishes; this deployment does not automatically certify or refetch earlier committed targets. |
| Media and hosted-byte verification are unfinished | Complete the comment prerequisite and use the existing media snapshot gate, then mirror and verify bytes. Do not bypass the whole-stage gate to label the run complete. |
| Final acceptance is blocked | Re-run the saved-data certifier after required work completes; listing/detail, comment identity/topology/count, media and hosted-byte evidence must all pass. Keep full-history disabled. |

Backend/API: targeted tests and deployed worker verification passed; public API shape unchanged. TRR-APP build: not applicable (backend scraper/persistence changes only). No schema migration or SQL ownership change; existing ingestion persistence and fenced operational budget updates only.

Private detailed logs, manifests, test output, benchmark and operation receipts are retained under `~/.codex/tmp/trr-instagram-speed-01a16b47/`. This report records a completed implementation batch, not completion of the January dataset.
