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
