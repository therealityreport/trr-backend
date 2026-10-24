# January recovery and media completion — October 24, 2026

Implementation owner: continuation chat `01a16b47-029d-70a0-8503-992e012a2b64`.
Existing backend branch and unrelated dirty work were preserved. Source batch
adds target-scoped public recovery, per-post media readiness and inventory
fingerprints, a drained-batch media turn, bounded recovery through the existing
worker lifecycle, and a maintained strict final certifier. Public API shape and
schema are unchanged. See workspace contract
`docs/workspace/instagram-january-completion.md` in the workspace repository.

## Evidence before rollout

- Original source/test batch: 163 affected public transport, replay, recovery and
  local-worker checks passed. Subsequent nullable-total/recovery-worker changes:
  26 passed; new January completion-replay SQL regression: 1 passed. Final worker
  abort/timeout/group-reaping and callback-order regressions: 3 passed. These
  totals overlap; they must not be summed as unique tests.
- Media/snapshot/replay/attachment batch: 141 passed initially, then 12 targeted
  checks closed attachment exceptions. Own target/generation fence: 5 passed.
  Backoff/source-block preservation: both affected checks passed. Passing
  unchanged evidence was reused.
- Certification: 85 original collected tests and 48 retained subtests have passing
  evidence; maintained relative attachment import adds one further regression.
  Lint, formatting, compilation and diff checks passed on owned files.
- Independent review found four blockers across the recovery/media and
  certification batches: unknown totals becoming zero; media invalidation
  clearing server backoff; an old standalone attachment-audit import; and
  unowned certifier/decoder children after worker abort. All four were repaired
  and their affected checks passed; fix-only review found no remaining blocker.
- An unrelated real-DBOS SDK lifecycle fixture remains timing-sensitive. Its
  five-second cooldown was shorter than its six-second first child lifetime.
  Role allowlisting was repaired and the fixture delay made twelve seconds.
  A replacement wake assertion then failed. Repeating against unchanged HEAD
  WorkflowStore passed that assertion but failed a later terminal-state timing
  assertion. It does not establish a production regression in this batch. The
  local worker uses its separately tested SDK-free partition path. No claim that
  the full DBOS lifecycle suite passed is made.

A live read-only certifier invocation inventoried all 1,103 current stage targets
and found 202 known source-gap posts totaling 10,539 at its snapshot. It returned
`final_acceptance=false`, zero retained-page scans and zero storage requests.
The first operator invocation lacked an explicit session candidate after choosing
its transaction read lane; the second supplied both verified TRR lanes and
succeeded. No credential/configuration bypass was used.

## Verified storage

Existing R2 account `73204b3e632bd7137a1bd2c867dc8ae8`, bucket `trr-media-prod`.
Task canary `verification/january-01a16b47/storage-binding-v1.txt` was uploaded
conditionally, read through the configured S3 endpoint, then read through
`https://pub-a3c452f3df0d40319f7c585253a4776c.r2.dev`. All 67 bytes and SHA-256
`46d7c1e08df66083818a61c0a4c4e66af2d15f74d8f946db2a12b7a192d4d015`
matched. `media.thereality.report` returned a connection failure. The January
local worker uses the proved existing endpoint; no bucket migration, new domain,
provider account switch or recurring schedule is needed. The connector exposes
another Cloudflare account and cannot mutate the TRR account; those resources
were left untouched.

Private manifests, binding proof, test/operator receipts, deployment and handover
records: `~/.codex/tmp/trr-instagram-speed-01a16b47/january-completion/`.
Deployment and current progress are appended after provider/local verification.

TRR-APP build: not applicable (backend-only/internal worker contracts). No schema
migration or new SQL ownership boundary; existing ingestion control/persistence
owners implement the new command. Full-history remains disabled.

## Live selection correction

First rollout v141 passed deployment/readiness and 654 packaged Python hashes,
then local handover at 00:38:33 Eastern preserved the six-worker policy. A
read-only live per-post eligibility check exposed a 15-second statement timeout:
full comment equality/metadata scans were correlated before LIMIT. EXPLAIN
confirmed those scans. The correction selects at most eight cheap candidates,
then applies the same exact proofs. A persisted administrative keyset cursor
advances even past ineligible posts and wraps, preserving source cursor state and
seals. Six affected checks passed. Live read-only candidate selection returned
`DS-H2Lhj9BN` in 1.052 seconds with the same 15-second limit, no writes and no
source requests.

The completion hook also handles a separately blocked stage: bounded recovery
can reopen a completed comment gap while preserving the other block. Cached
completion/blocked decisions cannot finish over newly eligible recovered work.
The certifier runs after terminal completed or blocked state and produces an
explicit acceptance or limitation report. Empty output left by interruption is
archived, not mistaken for a complete report; worker startup resumes missing
certification under its own coordinator lease. Group abort/timeout/order and
artifact preservation tests passed. The local worker uses the verified R2 public
base; the shared cloud storage secret was not changed for unrelated consumers.

Blocked phases also perform the bounded media scan without waiting for six new
comment submissions. A versioned full-cycle origin/wrapped marker covers a saved
middle cursor, so newly ready earlier posts cannot be skipped at termination.
The eight-proof cap remains; later work is journaled as a wait. Seven blocked-scan
checks and five full-cycle affected checks passed (overlapping evidence).
Fix-only independent review closed the cursor-starvation finding. A read-only
full snapshot for the live candidate also loaded successfully with two comment
attachments and one selected avatar. The worker was drained during query repair;
its saved run state and source progress were preserved.

## Retained evidence and productive attempt closure

The v142 correction (`ig-january-complete-c906c4c7f3c7`) deployed to
`admin-56995 / main / trr-backend-jobs` at 00:51:30 Eastern. Maintained readiness
and the fresh-container smoke passed; 654 packaged Python hashes matched.
Local PID 58020 acquired the sole coordinator lease after the recorded drain.
At 00:58:37 Eastern it had resumed fresh source traffic, preserving six comment
workers and policy revision 7. The later 01:00:33 snapshot had 218 settled comment
targets and 68 unresolved. No media attempt had yet started at that snapshot.
The superseded v141 local release was moved to actual Trash only after checking
for zero live references; the healthy v139 rollback release remains.

The next source batch adds offline-first reparsing through the existing fenced
ReplayCommands path. It streams retained public payloads in batches of 128,
without loading page checkpoints, and reads one canonical inventory per target.
It audits earlier roots too; historical observations never advance the current
checkpoint or settlement, and cannot undo newer/unknown missing markers or
explicit deletions. A durable summary binds checkpoint, parser and audit source
fingerprints. Every settled target is audited, including count-parity targets and
two-pass exhausted targets. Summary lookups are batched. An incomplete offline
stream raises a control conflict and cannot authorize a fresh source request.

A live offline audit of DS-H2Lhj9BN, before the historical expansion, matched 35
retained pages and 34 native IDs in 1.551 seconds, with zero replay writes or
source requests. Existing evidence and canonical data stayed intact.

Productive exhausted attempts receive an audited twelve-attempt extension only
when their latest owner generation committed new native IDs or more exhausted
lanes than its prior generation (or has the first valid page). Repeated identical
root proof does not qualify. The journal preserves exact bindings and progress
scalars. Ownership, checkpoints, rows, counters, retry deadlines, failure codes
and the two-pass no-improvement limit remain protected.

Retained/recovery SQL checks: 26 passed across one 24-test batch and two focused
checks. Productive-budget SQL checks: 15 passed. Parent completion integration:
1 passed. Counts overlap with earlier evidence and must not be summed as a full
suite. The independent review initially found tombstone resurrection, historical
root exclusion and count-parity audit omission; this batch addresses those
findings together. Candidate source is distinct from installed/runtime status;
rollout evidence is appended after verification.
