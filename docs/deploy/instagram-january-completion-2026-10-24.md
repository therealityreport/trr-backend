# January recovery and media completion — October 24, 2026

## Parallel reply rollout: v147 active; media repairs pending

Implementation/deployment/coordinator owner is chat
`01a16af5-f057-71b3-b515-3cd3395539e6`. Continuation chat
`01a16b47-029d-70a0-8503-992e012a2b64` completed independent review and supplied
read-only DTg baseline evidence. Backend HEAD remains
`6b92330f4f9ee9084df13ef2e6d38787f5434a93`; the reviewed candidate preserves the
concurrent Reddit work and unrelated working-tree changes.

The transport defaults to concurrency one; two applies only to strict public
reply traversal. Two slots include all in-flight, waiting and committing pages.
Independent parent branches can overlap; each branch's pagination and all owner
writes remain serialized. Private cookie jars reset from the owner-approved
snapshot. Disk spooling, bounded gzip/deflate expansion and streamed JSON parsing
preserve complete large values. Original clocks, permits, owner generation,
page size and cursors remain bound to responses. No dependency, API or schema
change was required.

Affected correctness evidence includes 124 maintained public transport tests,
40 scheduler/integration/canary checks and all 51 streamed HTTP cases. Later
permit, cooldown, cancellation, deadline and diagnostic changes have focused
passing checks. These overlapping batches must not be summed as unique tests.
The legacy retry batch passed 148 checks before three fake-repository failures;
those three also fail with unchanged HEAD and are not claimed as passing.

The final predefined benchmark used the full 14,542-ID baseline, a 2,308,288-byte
checkpoint (94.49% of measured live compact size), 64 pending parents and 65
requests. Page size 12, 250 ms pacing, controlled latency and real isolated
PostgreSQL persistence were identical. All ten arms matched complete native-ID
inventories, parent links, attachments, expected totals, lane states and receipt
bindings. Five-pair median elapsed time fell from 53.1248 s to 29.3780 s:
**44.70% lower**. Sample standard deviations were 0.0911 s and 0.2412 s;
variances 0.00829 and 0.05819 s². Median network sums were 40.7359/41.1770 s,
pacing 0.2794/4.1234 s, queue 0.000422/0.000532 s, and persistence
10.9604/10.7447 s. Overlapping categories are not additive wall time.

Measured source hashes differ from the final candidate only by retained reviewed
deadline/error-path deltas, with focused passing regression evidence. Passing
successful-path benchmark evidence was reused under the user verification rule;
there is no claim that the benchmark executed the final source hash.

The parent now transmits its absolute subprocess deadline before startup. Startup
and resume consume the same existing 590-second budget; source work stops twenty
seconds before that deadline. Actual subprocess tests prove received-peer commit,
HTTP cancellation, pool/spool cleanup and normal child exit with headroom. Fifteen
new deadline checks and nine compatibility checks passed. The earlier v146 local
canary was withdrawn before sending any requests after this timing mismatch was
found; its rollback and evidence remain retained.

Cloud v147 `ig-parallel-replies-2c13d7d1022c` deployed at 07:03:09Z on October 24
to `admin-56995 / main / trr-backend-jobs`. Maintained readiness, API health,
fresh-container verification of 659 Python module hashes and targeted scheduler/
deadline smoke passed. Local v144 was drained and reaped before v147 PID 52164
started at 07:07:52Z. One coordinator lease and policy revision 7 with six post
workers were verified. The live canary is restricted to existing January run
`2aef72ed-110c-4af9-b2b3-4a45da170a32`, generation 1, DTgXh94kXyo ownership
generation 57. All other attempts remained concurrency one during the canary. The accepted
configuration was subsequently enabled broadly, as recorded below. Serial
configuration and the v144 release are retained
for rollback. No scheduled task was created; full-history remains disabled.

At 07:08:33Z, 256 comment targets were committed and 30 unresolved; media remained
3 committed and 283 unresolved. These are intermediate traversal outcomes, not a
January completeness certificate. Private evidence is retained under
`~/.codex/tmp/trr-instagram-parallel-01a16af5/`; current rollout, canary and final
certificate paths are in its `deadline-rollout/` directory.

### Live canary acceptance and remaining media repairs

The generation-57 canary ended normally at 07:17:55Z with `target_deadline`.
It admitted 222 reply requests, committed 221 pages and added 221 exhausted
parent-lane proofs. One request remained uncommitted at the normal deadline;
its cursor did not advance. All prior canonical native IDs, parent links and
attachments were preserved. Canonical inventory grew from 14,638 to 14,642;
checkpoint inventory added five IDs. The reported post total remained 27,488.
Peak outstanding reply pages was two, with zero rate-limit, authentication or
native reply-access rejection counters. The retained acceptance report passed.

Two concurrent serial receipts admitted and committed 237 requests with the same
zero rejection counters. Canary persistence averaged 1.5795 seconds per committed
page versus 0.9447 for those different serial targets; these are different live
checkpoint workloads and do not establish equivalent per-page persistence cost.
The canary's live throughput is not the controlled five-pair performance claim.
The old v144 attempts lack a total-request denominator, so no historical failure
rate is inferred. The canary ended with one coordinator lease and the unchanged
run intent/generation and six-worker policy.

At 07:18:52Z, comments were 259 committed/27 unresolved and media 3/283. Three
technical media issues are being repaired before the next combined handover:
immutable-key conflicts after CDN rendition changes; partial-batch/all-worker
barriers starving ready media; and missing public avatar discovery for required
handles. The proposed fixes preserve prior object bytes/evidence, cap combined
active activities at six, and validate public profile native identity plus exact
handle before registry writes. These repairs are not yet deployed. Broad reply activation was subsequently
completed independently, as recorded below.

### Broad reply activation

The accepted v147 reply configuration was activated broadly at 07:27:37Z while
media repairs remained under review. The old local process was drained/reaped;
new coordinator PID 68376 acquired one lease. Six activity subprocesses were
verified. The exact run, generation, intent, policy revision 7 and six-worker
limit remained unchanged. There were 98 fresh page commits by 07:28:39Z. The
unexpired old media reservation seen in the database was not a seventh process.
The worker continues with concurrency two; default source configuration remains
one and the serial rollback configuration is retained.

Independent review completed one frozen media/worker/avatar batch and found
four repair groups: ephemeral signed-avatar delivery including pending retries;
absolute media budgets and bounded blocking I/O; technical control failures
being misclassified as source rejection; and auxiliary evidence with old-envelope
expiry. Those repairs are in progress. No additional scheduling/capacity blocker
was found. First-snapshot empty-key conflict was verified to roundtrip successfully
and is regression coverage, not a decoder failure.

### Media release closure in progress

At 07:53:27Z, the unchanged v147 worker had 268 committed comment targets and
18 unresolved targets; media remained 3 committed and 283 unresolved. One
coordinator lease and six active comment owners were observed. These counts do
not constitute completeness or establish public-source omissions.

The first combined media candidate was withdrawn before deployment. Independent
review closed the fairness, signed-avatar delivery, technical-error attribution,
and evidence-envelope repairs. The R2 boundary now runs each real SDK operation
in an owned subprocess with an absolute deadline, parent-liveness watchdog,
private stdin configuration and exact-child cancellation/reaping. Twelve distinct
focused SDK cases and thirteen affected media cases passed; independent review
accepted that boundary. Conditional writes, HTTP 412 handling, old object bytes,
and lost-upload-response retry behavior are preserved. The observed server-delay handoff on abort paths also passed independent review: 26 prior cases and eight focused closure cases have passing evidence, including original-error preservation and exact received-request permit binding.

### Reviewed media release v148

Combined candidate `ig-january-media-5d7741a64f2f` received independent acceptance
and deployed as Modal v148 at 08:02:04Z. Target remained
`admin-56995 / main / trr-backend-jobs`. Maintained readiness passed, API health
returned 200, and a fresh container matched all 660 Python module hashes and
passed targeted smoke checks. The local package verified 2,710 files. Ten reply
canary dependencies are byte-identical to v147; its accepted canary evidence is
reused. Current HEAD `6b92330f4f9ee9084df13ef2e6d38787f5434a93` and unrelated
Reddit/shared work were preserved. Seven runtime files differ from v147.

The local v147 coordinator and children were drained and reaped before v148
coordinator PID 86846 started. One coordinator lease, unchanged intent/generation,
policy revision 7 and six-worker limit were verified. Concurrency two remains
active, full-history remains disabled, and no scheduled task was created.
Prior release and serial configuration remain available for rollback. Normal
media work acquired DT1VE-flKq3 at 08:02:53Z. By 08:09:23Z, 111 fresh reply pages had committed with six activity processes
and one coordinator lease. Two media targets committed under v148, bringing
media to 5 of 286. At 08:12Z, one media activity overlapped five comment activities with exactly
six activity processes and one coordinator lease. Media scheduling is therefore
verified in the live run. Terminal certification remains pending; deployment
alone is not final acceptance.
Artifacts are in `~/.codex/tmp/trr-instagram-parallel-01a16af5/media-rollout/`.

### Bounded final-certifier local package

Independent terminal-path review found that the maintained deep audit retained
all full historical page checkpoints and payloads in memory. Local package
`ig-january-certifier-d7064b727d91` replaces that path with a same-transaction,
one-row named cursor and compact versioned full-row multiset fingerprints.
Every historical row remains covered, including rows before the current scan
root; current-root replay keeps all 52 prior validation expressions. SQL,
arguments, duplicate multiplicity and row count remain fingerprinted under
`january-query-row-multiset-v2`. Stable before/after evidence is still required.

Independent review accepted the four operator-file changes. Existing 100 tests
plus 48 subtests and 17 streaming tests passed; the lazy >90 MiB fixture used
less than 8 MiB traced peak memory. This is fixture evidence, not a whole-run
memory measurement. Local package verification checked 2,711 files. Backend/API,
scraper and local-worker implementation are byte-identical to v148. Modal does
not ship these audit scripts, so cloud deployment remains verified v148.

The prior local worker was drained/reaped before coordinator PID 92601 started
on this package with one lease and unchanged run/policy. Its final certificate
output is `~/.codex/tmp/trr-instagram-parallel-01a16af5/certifier-rollout/final-certificate.json`.
Before this handover, v148 had committed three additional media targets and
2,114 fresh reply pages in the retained 20-minute watch. Six finished reply
attempts reported 1,202 requests / 1,198 committed pages, zero rate/auth/native
reply rejections and maximum two outstanding pages. Active attempts were
excluded from those receipt totals. January is still running and uncertified.

### Public profile failure diagnostics

DTeS9vEFtl0 failed at 08:27:14Z with `parse_error` during the best-supported
first uncached owner-avatar refresh. The native error response was not retained,
so its exact cause and the query namespace of two retained owner IDs remain
unproven. No ID fallback or parser relaxation was authorized by that evidence.
DT3vAwmEoP8's separately observed content conflict actually occurred at 07:58:09Z,
before v148; its later dispatch timestamp was reconciliation, not a new failure.

Candidate `ig-profile-diagnostics-0ba3a13336d1` adds only bounded auxiliary
`instagram-public-profile-failure-v1` evidence: at most two response facts and
eight numeric codes, original observation clocks, phase/status, structural facts,
request attribution and complete/partial body hashes and sizes. It excludes raw
bodies, messages, headers, URLs, credentials and comment text. Parser-specific
validation leaves ordinary evidence projection unchanged. Same-owner fencing,
original errors, request limits and maximum cooldowns remain enforced.

Independent review accepted 18 new passing tests, 75 affected passing tests and
Ruff evidence. Package verification checked 2,713 files and 661 Python modules.
The ordinary sanitizer body is AST-identical to d7064; prior reply canary proof
is reused with this explicit non-diagnostic path qualification. Modal v149
deployed at 08:52:30Z to the same `admin-56995 / main / trr-backend-jobs` target.
Readiness passed, health returned 200, and fresh-container source verification
and diagnostic roundtrip smoke passed. Local handover waited until the active media attempt ended at 08:54:26Z,
then drained/reaped the prior worker and started coordinator PID 2868 with one
lease and unchanged run/policy. DTi0JLej9iW committed; the run reached 281 comment
and nine media targets. Final output is now under
`profile-diagnostics-rollout/final-certificate.json`. This diagnostic repair does not establish the
native cause or close the unresolved avatar source failure.

### Saved native owner hint coverage v150

The first retained v149 diagnostic proved a complete legacy public-profile
HTTP 401 for `bravodailydish` on DTIvhYcj-Dj at 08:56:20Z, one request. Indexed
saved native post data already contained matching IDs for `bravodailydish`,
`peacock` and `nbc`, but the old two-row lookup examined only `raw_data.user`.
Older rows could lack that branch while retaining explicit canonical owner and
native `owner_detail` identity fields.

`ig-profile-hints-307dd6cb8fc4` changes only `_public_avatar_lookup_hint` to
recognize those explicit owner bindings in the same owner-fenced indexed
`LIMIT 2` lookup. Exact associated handles and validated IDs are required;
conflicting eligible IDs return no hint. Native ID precedence, fresh profile
validation, request bounds and absence of fallback retries remain unchanged.
DTeS9vEFtl0's separate ID-namespace question is not resolved by this change.

Independent review accepted the source and 37 passing focused tests. The local
package verified 2,714 files; all other composition functions and all prior reply
canary dependencies remain unchanged. Modal v150 deployed at 09:03:21Z to the
same explicit target, with readiness, health 200, fresh-container 661-module
verification and targeted smoke passing. Local handover waited for media idle
at 09:05:57Z, drained/reaped v149, and started PID 6841 with one lease and the
same run, policy and saved checkpoints. Normal live profile-path verification
and terminal certification remain pending. Final output is now
`profile-hint-rollout/final-certificate.json` under the private task directory.

Historical v144 implementation owner: continuation chat `01a16b47-029d-70a0-8503-992e012a2b64`.
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

## Verified v143 rollout and first per-post media commit

Committed source `8aa7859b237d9c9028bfad9820dfe030aac448d2` deployed as v143
`ig-january-complete-a49b6547901f` to the same verified Modal target at 01:05:13
Eastern. The candidate tracked-source snapshot is bound by runtime SHA-256
`a49b6547901f1191999cd16de32fbc85eeb4284ad193cf6ce763070e8d44074b`.
Provider Git metadata is null for this isolated candidate path; the explicit
source manifest and all 656 packaged Python hashes matched in fresh container
`ta-01M5P4RZY4DAQ4ZATCDYCCP0FR`. Maintained readiness and first API canary passed.
Independent fix-only review found no remaining actionable blocker. The five
existing completion-hook checks also passed after the budget entry integration.

The expanded live offline audit again matched 35 pages / 34 native IDs for
DS-H2Lhj9BN. It wrote only idempotent audit summaries/page-audit journal records,
with zero replay writes, no source requests and no checkpoint/coverage changes.

While 65 comment targets remained unresolved, the v142 local worker processed
DS-H2Lhj9BN media and committed receipt
`b24a1e12-781a-48d0-a36a-02c5c073306d` at 01:10:44.916 Eastern. It binds
`instagram-media-inventory-v1` SHA-256
`0c35faf0572a7f90b73fdf2bc3874755e0cfd8eb6eefecfbe563a798dc425451`.
Its retained comment settlement still reports 34 observed / 35 expected / gap 1.
This proves a per-post media turn without certifying source parity. All comment
children were drained before it ran, and six comment children resumed afterward.

Recorded v143 local handover reaped old PID 58020 and verified zero old release
references before one bootstrap. New PID 65054 and its release cwd matched; the
subsequent read verified the sole coordinator lease, unchanged intent/generation,
policy revision 7 / six workers, and preserved source owners/checkpoints. Total
immutable pages rose from 74,857 before stop to 74,858 before activation. The
01:12:29 snapshot had 221 settled comment targets / 65 unresolved and one media
commit / 285 unresolved. The v142 release remains the healthy rollback; older
v139 was moved to actual Trash only after a zero-reference check.

No final certificate or public-source limitation is asserted yet. The run is
still traversing, and first media success does not establish whole-run bytes.
The last source correction separates terminal known-media/byte certification
from count parity; rollout of that correction is recorded below when verified.

## Terminal certification gate correction

Active unfinished runs retain the cheap known-gap preflight. Completed or blocked
runs now enter the complete audits even when a known source gap remains. An
explicit `complete_observed_inventory_v1` media proof requires the entire January
key set, passing independent post fingerprints, all verified observed native IDs,
parents, pages, clocks, attachments and media bindings, and known integer
settlements. Only positive public-native-exhaustion count parity diagnostics are
permitted; unknown counts or any other inventory discrepancy still block the
media proof and its byte plan. The original comments report is unchanged.

Hosted verification then reads every required observed role, checks all full
bodies/hashes and decodes them; source count parity and aggregate comments remain
blocked. Final acceptance remains the conjunction of all gates. Exact parity now
requires integer expected_total == observed_total; a clamped zero count_gap does
not make an over-count acceptable. Worker settlement and traversal are unchanged.

Fourteen new focused cases have passing evidence, including actual media audit
through physical planning and full object read/hash/decode for every required
fixture role, leaving final acceptance false for the count-gap case. Refusals
cover unknown counts, parent/reply errors, absent source proof, extra inventory,
failed independent post proof, changed bindings/rows and count over-shoot. Earlier
passing unaffected checks were reused. Ruff passed on all four changed paths.

## Final reviewed runtime and continuing execution

The terminal-certification correction is committed as
`0d1b50cdead6a1f6e54200e653247ae2e6a7a82f`; independent review found no actionable
blocker across the four-file batch and exact-parity amendment. v144
`ig-january-complete-84739870b8b4` deployed to the unchanged verified
`admin-56995 / main / trr-backend-jobs` target at 01:22:55 Eastern. Runtime SHA-256
`84739870b8b4a7753100a0435572668055d021c76ff5664946469f3c91f9c205`
binds the isolated tracked-source candidate. Maintained readiness/API-200 passed.
Fresh container `ta-01M5P5SPK4T9HYJX7X19RQS8PR` matched all 656 packaged Python
hashes and the new certification scope. Smoke made no native requests or database
connections. Provider Git metadata is null for this candidate path; exact source
revision/hashes and fresh-container verification establish deployed identity.

Local handover reaped PID 65054 and verified no old release references before one
bootstrap. PID 68010 and the new cwd matched. Follow-up verified one coordinator
lease, unchanged run generation/intent and policy revision 7 / six workers. Prior
source owners and deadlines were preserved; no checkpoint or owner was forced.
The v143 release remains the rollback. Older local releases were moved to actual
Trash after zero-reference checks; no rollback/source evidence was deleted.

At 01:24:05 Eastern: all 286 detail targets were settled (285 cached, one newly
committed), 223 comment targets were settled and 63 unresolved; media had one
commit and 285 unresolved. Fresh public pages were verified on the preceding
release at 01:21:47; its unchanged comment/media execution code is reused in the
final candidate, rather than repeating passing source-path checks.

The installed maintained certifier ran read-only against the exact live binding
at 01:24:07. It inventoried all 1,103 stage targets and returned
`final_acceptance=false`: 212 currently reported count-gap posts, total 10,778.
This is an interim running-run snapshot, not a terminal public-source limitation.
No native requests, database mutations or storage writes occurred. The other
whole-run gates remain pending until traversal/media and bounded recovery settle.
The existing local worker will execute those steps and the terminal full audit
through its configured lifecycle; no recurring automation was created.

Source, local, cloud, read-only certification and rollback receipts for this
final batch are under the private `certification-rollout/` evidence directory.
TRR-APP build is not applicable; public API shape and schema are unchanged.
Full-history remains disabled. The January run and whole-run verification remain
in progress; a first media commit or working public endpoint is not final
acceptance.

Final-release follow-up at 01:25:12 Eastern verified its sole coordinator lease
and fresh native page commit at 01:25:11.161, after the final handover. The 223/63
comment and 1/285 media split remained unchanged at that snapshot. Execution is
continuing under the installed v144 candidate.

## v150 live saved-ID and hosted-avatar proof

At 09:16:21Z, normal owned media work for `DTObcKuDUpn` retained a public
GraphQL profile response for `peacock` / native ID `19846578012`. Its original
hash and strict native-ID/handle parser passed. The target subsequently committed
its ordinary media receipt. At 09:23:12Z the maintained hosted-object verifier
confirmed the receipt's required Peacock mention-avatar binding and original
observation clock, read all 2,836 bytes from R2 and the configured public CDN,
matched the retained SHA-256, proved byte equality, and fully decoded the
150x150 JPEG. This is scoped avatar acceptance, not whole-target or whole-run
acceptance. Private proof: `profile-hint-rollout/peacock-hosted-avatar-proof.json`.

The 09:22:30Z run snapshot had 285/286 committed comment targets and 16/286
committed media targets. DTgXh94kXyo remained in reply traversal. One coordinator
lease, generation 1 and the original intent remained intact. The separate
checkpoint CAS experiment remains offline and unqualified; v150 is unchanged.

## Rejected offline checkpoint CAS experiment

The five-pair inbound-checkpoint experiment failed its fixed 20% whole-elapsed
gate: baseline median 29.6235 seconds versus candidate 29.7732 seconds (0.51%
slower), despite 89.22% fewer database response bytes. All 650 historical
checkpoint comparisons and cross-arm inventory, parent, attachment, count,
lane, clock and receipt checks passed. Correctness alone did not qualify it.
The exact v150 comments.py bytes were restored after checking candidate hashes;
the candidate and test evidence remain private, and only its two new test files
were moved to actual Trash. Production was never changed. Evidence:
`checkpoint-cas-assessment/qualification-final.json` and
`checkpoint-cas-assessment/rejected-restoration.json`.

## Reviewed avatar delivery relocation release v151

Retained evidence for DTTlAy3Dm1I showed the same complete bravowwhl avatar
path moving between regional Instagram CDN hosts. The avatar-only repair
requires private validated same-handle/native-ID provenance, strict HTTPS
regional hosts, default ports and identical full paths. It keeps canonical
references, storage identities, source clocks, saved formats and post/comment
matching unchanged. Prebound aliases map back to their original avatar roles
for bounded refresh, with pin and collision checks. The first candidate was
withdrawn before deployment after independent review found that retry seam.
The corrected workflow passed the reviewer's original end-to-end failing case.

Focused validation retained 56 affected public checks, three strict provenance
checks and 11 legacy refresh/cancellation checks; the fix added 16 passing
new/affected cases. Ruff passed. The accepted candidate
`ig-avatar-relocation-711323a94e9b` deployed as Modal v151 at 09:45:54Z to
`admin-56995 / main / trr-backend-jobs`. Readiness and API HTTP 200 passed; a
fresh container matched all 661 packaged Python hashes and passed the targeted
network-free relocation/retry smoke. Local package verification covered 2,715
files. The ten reply-canary dependency hashes remain identical to v150.

At this entry, local handover is waiting for the current media activity to
finish; v150 remains the local worker. Historical cached-original alias reuse
is excluded from this release. Its separate investigation found the old
belowdeckbravo object intact but missed by host-exact cache lookup. No current
changed avatar is permitted to replace that preserved asset.

The v151 local handover observed media idle at 09:50:05Z, drained/reaped
PID 6841, and started PID 30236 from the verified 711323 package at 09:50:10Z.
The 09:50:44Z follow-up verified one coordinator lease, unchanged run generation
and intent, and the six-worker policy. Final output is now
`avatar-relocation-rollout/final-certificate.json`. Initial follow-up showed no
fresh activity/pages yet; resumed processing remains a separate gate.

## Expired side-lane media reconciliation v152

The v151 handover exposed an existing race: DUB7jhqjoOU entered sending at
09:50:07Z but never acquired ownership. Admission correctly retained that
ambiguous send, while per-post media selection lacked the main phase's expired
send reconciliation. The isolated fix changes only workflow_store.py: after
existing intent/manifest/selection guards, select one expired current-token,
current-generation unresolved send and return the normal reconcile decision.
No admission cap, early expiry, direct reset or dispatch readiness gate changed.
Fifteen focused database-backed/affected checks and Ruff passed; independent
review accepted the complete batch. The fixture-only test package excluded
concurrent unpublished cached-avatar work.

`ig-media-reconcile-06f8e5fdad9f` deployed as v152 at 10:03:43Z. Readiness,
API HTTP 200, all 661 fresh-container Python hashes and the targeted smoke
passed. Local verification covered 2,716 files. Handover at 10:04:28Z drained
and reaped PID 30236 and started PID 42630. Final output is now
`media-reconcile-rollout/final-certificate.json`.

The 10:05:36Z read-only live qualification proved partition 121 ordinal 3
journaled the exact DUB reservation reconciliation and its dispatch became
expired at 10:04:41Z. Attempts/requests stayed zero and owner_started_at stayed
null. Normal media admission then acquired DTeS9vEFtl0 at 10:04:51Z for its
second attempt. One coordinator lease, original run generation/intent and
policy revision 7/six workers remained intact. This is runtime acceptance of
reconciliation, not January completion. Evidence: D10/live-reconciliation-proof.json.

The deployed relocation repair subsequently passed its live target check.
DTTlAy3Dm1I committed on attempt 2 under v152. Its bravowwhl owner avatar
retained the exact original LAX canonical reference and original source clock
2026-10-23T03:11:52.455343Z. The fresh strict profile retained native ID
1063480066 and the same full asset path on a different regional host, with its
new clock separate. The maintained verifier read all 6,883 bytes from both R2
and the configured CDN, matched the retained SHA, proved full byte equality,
and decoded the 150x150 JPEG. Receipt fence and ordinary replay passed.
This is scoped repaired-avatar acceptance, not whole-target/run acceptance.
Proof: `media-reconcile-rollout/dtt-relocation-live-proof.json`.


### v153 cached original avatars and provider flags

The reviewed candidate `ig-cached-avatar-a00b02bbdf2a` adds an owner-fenced,
per-post bridge from a complete same-handle regional registry cohort to the
exact original avatar reference. It preserves source and byte clocks, verifies
complete R2/CDN bytes and decode, retains a separate immutable proof before
promotion, and keeps ordinary source seals, receipt checks and final audit.
First-attempt sealing, retry/inventory/role reauthorization and historical versus
current authorization selection were repaired before shipment. Two unpublished
candidate packages remain preserved as withdrawn evidence. The second review
finding triggered an explicit invariant reassessment before the narrow auditor
repair. No historical proof corruption is excluded by candidate selection.

The separate provider-flags repair compares only the six required strict
booleans. Unrelated keys and metadata cannot cause false ambiguity; missing,
malformed or conflicting required values still reject before a profile POST.
The exact flag behind the retained DTI failure was not observable, so this is a
reproduced parser repair rather than a claimed diagnosis of that particular body.

Validation reused unchanged evidence and added 60 bridge cases, two affected
media checks, 11 focused cohort/history checks, and the provider-flags affected
batch (51 checks; overlapping earlier 38 not added). Ordinary seal AST, payload,
hash and clock invariance passed. Independent review accepted the combined six
runtime-file delta; 1,036 runtime manifest entries and 2,719 packaged files
matched. The ten reply dependencies remain unchanged, preserving the qualified
reply benchmark and bounded live canary evidence.

Modal v153 deployed to admin-56995/main/trr-backend-jobs at 10:33:47Z.
Readiness and API HTTP 200 passed. A fresh container verified 662 Python
modules and targeted network-free smoke checks. The local handover at
10:40:58Z drained/reaped PID 42630 and started PID 61235. One coordinator
lease, the original run/generation/intent, policy revision 7 and six-worker
limit were verified. Full-history remains disabled; no scheduled task was
created. Current final output is `cached-avatar-rollout/final-certificate.json`.

At the first new-worker snapshot, 285 comment targets and 30 media targets
were committed. New cached-avatar live acceptance and final certification
remain pending. A newly diagnosed usmagazine/usweekly saved-owner storage-key
lineage gap is separate unfinished work; it is not established public-source
loss. Evidence lives in cached-avatar-rollout (D9), profile-flags-rollout (D11),
and media-reconcile-rollout/old-v152-media-failure-batch-summary.json.


Startup qualification found two pre-handover reservations occupying admission:
DTgXh94kXyo until 10:50:46.653466Z, and unacquired DUEPCeVjun7 until
10:50:53.442502Z. The latter was reserved seconds before bootout and retains
zero attempts/requests. The new coordinator advances partitions with one lease;
there is no source-lane hold. No early expiry/reset was applied. Fresh
persistence must be verified after normal reconciliation; startup health alone
is not progress. Exact tokens and diagnosis remain private in
`cached-avatar-rollout/v153-startup-wait-diagnosis.json`.


v153 progress qualified at 10:53:56Z: the exact DUEPCeVjun7 token reconciled
at partition 123 ordinal 41 and expired at 10:50:55.688654Z, after its original
deadline, preserving zero attempts/requests and no acquisition. DTg acquired a
new owner at 10:51:16Z and saved 47 fresh durable pages by 10:53:54Z. DT1Ep1JArL-
started ordinary media attempt 2. Run, generation, intent, local placement and
one lease remained correct. Proof: `cached-avatar-rollout/live-reconciliation-and-progress.json`.


### v154 urgent ordinary-cache query repair

The v153 cached-original owner proof for DTe passed, but its third media attempt
failed on an unrelated ordinary cached bravotv role: the bridge queried 2,049
same-path rows before reaching its existing no-auxiliary cache skip. This was a
technical ordering bug, not source absence. Several other targets hit the same
failure. PID 61235 and all children were stopped/reaped at 11:02:56Z to prevent
further attempt consumption. All evidence, promoted original bytes and counters
were preserved; no owner deadline was forced early.

The isolated fix moves that existing skip before the cohort query, after the
same owner check. All auxiliary validation and uncached/derived cohort limits
remain. Four new database cases passed; seven checks against immutable v153
dependencies proved the mixed promotion/cache normal receipt and replay path.
Independent review accepted the exact two-added/two-removed-line delta.

The package also adds an explicit one-shot media repair-budget operator. It
permits only 3/3 to 3/4 after exact installed review/process/target/diagnostic/
eligibility qualification, and changes only max_attempts plus the atomic command
journal. It defaults to a read-only plan and preserves all waits, source blocks,
counters and data. The semantic binding-path review finding was fixed; the
accepted final allowlist adds the diagnosed inventory-limit error but continues
to reject rate-limit, public-backoff and source-absence grants. No live grant has
been made at this record point.

`ig-cache-fastpath-81cc53c11f45` is immutable v153 plus exactly three runtime
paths; unfinished saved-owner-asset work is excluded. Review verified 1,038
runtime manifest entries and 2,724 packaged files. Modal v154 deployed at
11:08:09Z; readiness, API HTTP 200 and fresh-container 663-module smoke passed.
The already-drained local service started PID 75233. Exact cwd/configuration,
public-only mode, reply concurrency 2, one lease, original run generation and
intent were qualified. Final output is now
`cached-avatar-fastpath-rollout/final-certificate.json`.

Read-only eligibility records for DTeS9vEFtl0, DTIvhYcj-Dj and DTtwe3rEsXL
capture exact exhausted states and validated retained proofs. They establish
applicability of the ordering fix, not that all other media work will succeed.
Parent approval and an explicit journaled grant remain separate gates. The
ordinary pipeline and complete-byte/final-audit gates remain required.


Three exact repair grants were subsequently qualified and applied: DTIvhYcj-Dj
first, then DTeS9vEFtl0 and DTtwe3rEsXL after uncached-avatar preflight found no
known cache-contract obstruction. Each is a single 3/3 to 3/4 grant under the
reviewed v154 runtime. The read-only plans and atomic journal readbacks prove
that requests, owner generation, checkpoints, retry times and blocks stayed
unchanged. Remaining no-archive avatars still require ordinary public/CDN
verification; the grants are not source-completeness acceptance. Readback at
11:18:03Z confirmed all three journals and max_attempts=4, with attempts still3.
Private receipts: `cached-avatar-fastpath-rollout/repair-grants/`.

### v154 live cached-avatar acceptance, 11:24 UTC

DULebyVjAKi committed through its normal second media attempt at 11:21:27 UTC. Installed-v154 verification checked receipt `1a07e2bd-e03b-4cdf-947d-2e9b4b24c71c`, ordinary result replay, both retained auxiliary proofs, original/current source seals and current cohort. Tagged and collaborator roles bind one Carl Radke avatar object. Full R2 and configured CDN reads matched 5,299 bytes and SHA-256 `1e4a5007fe901a6ed04d2a24daa19ee18f36fdce954409eca761a79fc8efab90`; complete JPEG decode passed at 150 by 150 pixels. The original reference, October 23 source clock and July 11 inherited byte clock remain intact. Evidence: `cached-avatar-fastpath-rollout/duleby-cached-avatar-live-proof.json`. This accepts the scoped derived-avatar path; whole-post and final January acceptance remain pending.

### Saved-owner candidate and count-gap preflight, 11:39 UTC

Candidate `ig-saved-owner-c150441405db` is frozen and packaged but not deployed. It contains the six approved runtime deltas for per-post historical owner-asset provenance and retains v154 cache/budget repairs. Local zero-I/O smoke verified 664 modules and 2,726 package files; full-history is closed. A first smoke harness failure (missing fixture validate method) was corrected without source changes; the initial log is retained. Independent review is pending. Existing reply benchmark/canary evidence is retained through ten identical dependency hashes and ordinary-path invariance.

The shallow preflight at 11:30:58 UTC found 269 committed comment targets with positive count gaps: expected 117,682, observed 105,158, difference 12,524. Their recorded public-native-exhaustion basis does not yet establish final source omissions; public recovery pass metadata was absent. Installed before-finalize recovery audits retained evidence and performs bounded positive-gap recovery; final certification still rejects nonzero or unknown parity gaps. `current-preflight-gaps.json` and `current-preflight-recovery-addendum.json` contain the exact inventory and source-backed trigger mapping. An initially flagged 3/3 media target was actually actively using attempt 3 with a stale generation-2 error; it subsequently committed. No extra grant was made for that alert.

### Qualified fourth-attempt result, 11:43 UTC

DTeS9vEFtl0 committed its normal fourth attempt at 11:39:53 UTC. Independent installed-v154 verification passed ordinary receipt/replay and all four derived cached-avatar proofs. Four roles/four objects passed complete configured R2 and CDN reads, SHA equality and JPEG decode: owner 4,668 bytes, tagged avatars 6,145/7,807/4,060 bytes. Original references/source clock and inherited byte clocks remain intact. The original owner proof remains applicable without reauthorization. Evidence: `cached-avatar-fastpath-rollout/dte-v154-media-qualification.json`. This is scoped avatar acceptance, not whole-post or final certification.

### v155 deployment and handover, 11:47 UTC

Independent review accepted `ig-saved-owner-c150441405db` with exact six-runtime-path scope, complete package hash verification and preserved ordinary-path checks. Cloud v155 deployed at 11:44:02 UTC to admin-56995/main/trr-backend-jobs. Readiness passed without blocking failures; API health returned 200 first attempt; fresh container `ta-01M5PVKKY1FZ70D7DAPK3W969R` verified 664 modules and targeted zero-I/O smoke.

At the media-idle boundary, old coordinator 75233 and descendants were drained/reaped with zero remaining lease or open release references before one bootstrap. New coordinator 3144 uses the exact package; one lease, unchanged run generation/intent and policy revision 7/six workers were verified. Reply concurrency remains 2/public-only/full-history closed. Fresh v155 persistence and the saved-owner live target remain pending. Rollback v154 and concurrency-1 configuration remain available.

All three one-time fourth attempts committed under v154. DTI independent receipt/derived-owner/full-R2-CDN check passed (4,847-byte JPEG with original clocks). DTt committed before handover and its scoped check is in progress. Media is 41/286 committed; this is not final acceptance.

### v155 startup and final fourth-attempt qualification, 11:48 UTC

Read-only consistent snapshot at 11:47:56 UTC confirms expected inherited DTg reservation (generation 82, never acquired) until 11:56:35.797410 UTC; no early expiry or reset. Media DUBZMmTDeSJ acquired its normal third attempt on v155 at 11:47:46.778353 UTC. DUDfG9Vkwu8 remains released at 2/3 with one normal attempt available. One lease and unchanged policy/run binding remain verified.

DTtwe3rEsXL committed under v154 at 11:46:32 UTC and was independently verified using v155 with the byte-identical cached-avatar helper. Three derived avatar roles/objects passed receipt/replay/proof/current-cohort and complete configured R2/CDN hash/equality/decode checks (6,215/6,527/6,511 bytes). Original references and source/inherited-byte clocks remain intact. All three one-time fourth attempts now have scoped avatar acceptance; January final acceptance remains pending.

### Saved-owner live qualification passed, 11:58 UTC

DUDfG9Vkwu8 committed its normal third attempt at 11:53:01.658 UTC on v155, receipt `4699b6f3…`, result `9460f121…`. Installed maintained independent saved-owner audit returned one identity and no failures. Input/auth evidence, original ordinary source seal, explicit same-post usweekly branches, current usmagazine owner slot, historical cohort (26 rows) and expected one-row current projection all validated. Original 645… asset reference and October 23 source clock remain intact; inherited June 30 byte clock remains intact. Canonical hosted owner URL changed only through the normal receipt transaction to the content-addressed usmagazine destination.

Both historical usweekly and current usmagazine objects passed complete configured R2/CDN reads, equal 3,307-byte SHA-256 `1a0159cd66c28b94919fee2a985a63189fd2061f90748e653ef77643d4ba510f`, and full 150 by 150 JPEG decoding. No global handle or native-ID equivalence is claimed. Evidence: `saved-owner-asset-rollout/dud-saved-owner-live-qualification.json`. This qualifies the scoped new path; full-run parity, media roles/bytes and final stable-evidence certification remain pending.

### Post-handover reply persistence, 12:01 UTC

Normal reply acquisition resumed after the inherited reservation expired. The bounded monitor observed 30 fresh pages by 11:59 and 72 by 12:01, with one coordinator lease and simultaneous media activity. The installed v155 process qualification now includes fresh media and reply persistence. No ownership reset, early expiry, pacing reduction, or new scheduled task was used.

### Media-readiness and scheduling contention diagnosis, 12:28 UTC

All earlier unresolved cache-limit failures cleared by 12:07, leaving 48 media committed and 238 unresolved. Eight bounded unattempted candidates then failed maintained canonical-inventory/receipt guards. DT_FCpNDY41 has one precise historical extra native ID `17985564182779954`, first observed June 12 and last scraped June 16 under legacy coauthor Relay provenance. The October receipt proves 236 IDs against expected 243; the saved canonical inventory has 237. Neither current nor checked predecessor pages supplied that historical ID. It is preserved, not relabeled or deleted.

A maintained offline audit was attempted once and safely rolled back with `LockNotAvailable`, zero pages/journal entries, and no source requests. A separate read-only snapshot directly observed a shared run-fence waiter blocked by normal coordinator readiness selection holding the exact current run row exclusively for more than two seconds. D15 proposes decision-only shared run fencing plus conditional media-manifest row locking for cursor serialization. All other entrypoints remain exclusive. Eleven new real PostgreSQL cases and 110 other affected checks passed; one bounded DBOS replacement-child check failed identically against candidate and captured installed-v155 baseline and remains an explicitly unclassified validation limitation.

Five paired controlled contention trials measured median run-fence-call elapsed 265.38 to 0.85 ms, complete writer 276.38 to 9.35 ms, and whole fixed-hold scenario 285.75 to 275.54 ms. This is synthetic lock-overlap evidence, not a production throughput percentage. Candidate `ig-scheduling-fence-75e97b271c1c` is packaged (one runtime-file delta), with local zero-I/O smoke passing 664 modules/2,727 files; independent review and deployment remain pending.

A task-local guarded exact-root operation is independently reviewed but uninvoked. It pins completed offline audit/checkpoint/source, preserves the command journal idempotency key, locks command/run/comments/media in order, checks both targets idle, and uses the existing recovery helper on the same transaction. Atomic private artifact publication and same-journal output recovery passed seven isolated filesystem checks. It must not run before a successful post-fix offline audit.

### v156 rollout and first guarded early recovery, 12:35 UTC

Independent review accepted the one-file scheduling change with the baseline DBOS fixture limitation disclosed. v156 `ig-scheduling-fence-75e97b271c1c` deployed at 12:32:57 UTC, exact admin-56995/main target; readiness passed, health 200 first attempt, and fresh container `ta-01M5PYD1BEQY34JH16AXS3VZPR` verified 664 modules and targeted fencing smoke. Old local coordinator 3144 was drained/reaped before one bootstrap of PID 20784. One lease, same run/intent/generation, policy revision 7/six workers and reply concurrency 2/public-only were verified.

The previously lock-blocked maintained offline audit for DT_FCpNDY41 now completed: 145 pages, 236 native IDs, matched, zero replay writes and zero source requests, unchanged current traversal coverage. The reviewed task-local helper (SHA `f6c2532bc0420d27b3f9d3a566d77e0621ba055c1a4b323911e09320a84dfc9f`) then journaled exactly one public root recovery, pass 1. Both targets were atomically fenced idle; the comment attempt count remained 1, saved checkpoint contents and receipt history were preserved, and the operator issued no source requests. Execution remains with the original coordinator. Postcommit/source progress qualification and final acceptance remain pending.


### v157 scan-yield rollout and continued recovery, 12:51 UTC

DT_FCpNDY41 first recovery completed at 12:41:33 with the same 236 native IDs (133 roots, 103 replies), expected 243, gap 7. IDs, parents and attachments were preserved. This is one complete no-improvement pass. Its second retained audit matched 290 pages without source requests or replay writes, and a separately journaled second root pass started normally at 12:46:42. At 12:49:47 it remained incomplete with 138 partial observed IDs; no second-pass completion is claimed.

Independent review accepted v157 `ig-media-yield-5f04ad7d97aa`: exactly three runtime changes let an eligible comment proceed after an unfinished bounded media scan, carrying the pending-media flag through Scheduler and LocalDispatch. Ready media/reconciliation retain priority; eight-proof scan batches, source guards, admission, shared run fence, six-worker maximum and pacing remain unchanged. All 12 new cases have passing evidence across focused runs; 37 affected checks passed, with the previously baseline-reproduced DBOS replacement fixture explicitly unqualified. All 2,728 packaged files matched; local smoke exercised the actual worker path. Cloud readiness and health passed and fresh container `ta-01M5PZBCF66MYH4QJ4WA513JSR` verified 664 modules. The cloud image does not ship local_worker.py; its proof is the separate local hash and smoke.

Controlled local handover at 12:50:19 reaped coordinator 20784 and descendants before starting PID 29266. This stopped incomplete comment attempts; their committed checkpoints and existing leases are retained for normal resume, not counted as complete passes. Same run/generation/intent and policy revision 7/six workers were preserved, with 97,587 saved pages before and after handover. Fresh v157 persistence and coordinator lease qualification are pending. v156 and concurrency-1 rollback remain available.

Four further retained audits matched (DT_MoEeknGJ: 43 pages/52 IDs; DT_NceFgRLd: 51/79; DT_PveAlR7F: 159/198; DT_WjSNgbCN: 80/128), with zero source calls/replays. All parser/audit dependencies were verified byte-identical across v156/v157. Four exact-target guarded root controls were accepted after handover; only fixed target and qualified release literals differ from the reviewed operator. No competing coordinator, new schedule, full-history activation, deletion or coverage relabeling was introduced. Media remains 48 committed/238 unresolved; final acceptance remains false/pending.


### v157 connected live yield qualification, 12:55 UTC

Independent read-only verification bound all four new root dispatch tokens to partition 137, consecutive ordinals 31–34, each with media_scan_pending=true and no intervening scan-wait decision. Accepted-to-dispatch delays were 28.788, 33.614, 39.637 and 44.950 seconds; all four acquired normally and had 156 fresh committed pages at the 12:52:49 snapshot. These unlike-workload live timings establish connected yielding, not a controlled performance percentage. One coordinator lease, unchanged six-worker policy, effective reply concurrency 2, public mode and full-history disabled were verified. The subsequent bounded monitor reached 269 fresh pages and three of the four recovered targets settled by 12:54:31. Their source-gap outcomes still require maintained per-target classification.

Inherited DT_F generation 3 and DTg generation 88 deadlines (12:56:34.510648 and 12:59:06.293855 UTC) remain untouched. Evidence: media-scan-yield-rollout/live-yield-qualification-summary.json, which binds the first metadata snapshot and corrected exact-owner journal query. No exact per-decision historical cursor value is claimed because that value is not stored.


### First two-pass terminal gap and bounded batch preparation, 13:05 UTC

DT_FCpNDY41 completed recovery pass 2 at 12:58:24.321 UTC, owner generation 4/attempt 4. Maintained unchanged_passes returns 2: expected 243, observed 236, gap 7; no new/lost IDs, all lanes exhausted and no repeated cursors. Historical saved ID 17985564182779954 remains absent. It will receive no third root. The selected checkpoint comparison proves parent equality only: observed nodes contain no attachment fields, as explicitly corrected in the classification addendum. Final retained-payload/attachment audit and media/source-gap reporting remain separate. Its preserved canonical extra still blocks current media completeness.

All four further first recoveries settled. Second retained audits matched DT_MoEeknGJ (86 pages/52 IDs), DT_NceFgRLd (102/79), DT_PveAlR7F (318/198), and DT_WjSNgbCN (160/128), with no source requests or replay repairs. Their second roots were accepted through literal-specialized reviewed helpers, preserving attempts; DT_Wj waited for a recovery slot. Three more previously diagnosed drift targets were audited: DT_x_Q2gXoF (135/230), DTa2iInAYpw (237/348), DTAia41ky8t (57/64). Each first root was then accepted as capacity became available. Manual controls are now stopped before the proposed batch-driver handoff.

A task-only finite batch driver and a separate narrow committed-media invalidation wrapper are in implementation/testing, not executed. The initial driver retains the exact unattempted-media wrapper, max four outstanding recovery roots, one 120-second observer, pinned current release/intent/cohort, offline-first per-checkpoint audits, immutable request/journal recovery, and maintained two-complete-no-improvement classification. No new runtime deployment, native source client, coordinator, schedule, full-history mode, or public API/schema change is introduced. Settled media remains outside the first tranche and requires its own reviewed normal-invalidation preimage/receipt guards.

A bounded DTg snapshot at 13:00:38 showed 14,694 retained native IDs and 4,827 unknown-count branches without exhaustion proof, down from the earlier 6,405 at 09:50. It is actively continuing its original traversal; this is not current source-count parity or a completed recovery pass. The preserved canonical expected total remains 27,488, distinct from the null checkpoint expected field.


### Additional two-pass gaps and source protocol qualification, 13:18 UTC

Four further selected checkpoints/current owner-bound receipts were classified at 13:10:05 UTC. Maintained unchanged_passes returns 2 for DT_MoEeknGJ (64 expected/52 observed/gap 12), DT_NceFgRLd (80/79/1), DT_PveAlR7F (212/198/14), and DT_WjSNgbCN (131/128/3). They retain 12/1/5/2 historical canonical extras respectively. No new/lost IDs or parent changes occurred; no nonexhausted/repeated/unsupported lanes remained. Exact native IDs/source/date provenance and the separate attachment-proof limitation are in media-scan-yield-rollout/four-second-pass-classification.json. None gets a third root.

While the finite driver remains unapproved, the parent continues only reviewed manual operators. DT_x_Q2gXoF, DTa2iInAYpw and DTAia41ky8t had positive current gaps, fresh matched retained audits and their second root accepted. DTlf9zjkmRx had an existing raw-page diagnosis; a fresh audit preceded its first root, with only target/release and accurate count-gap reason literals changed in the reviewed helper. These targets are already in the proposed driver allowlist. Manual controls must stop before actual driver activation; no competing driver has run.

DTlf's expected 3,249 is independently qualified through its cached details receipt's cache_source link to listing evidence 37deca00-bc20-444d-b39b-8dd5380589e9. Strict retained hash, shortcode/native ID and maintained parser checks pass; native comment_count=3249 was captured 2026-10-23 02:58:01.031653 UTC and is independent of likes. A prior observation that details had no directly linked evidence was superseded by this explicit listing-evidence chain, not left as an unqualified count.

The old DTlf scan exhausted after three root pages (15/15/14 nodes), but the bounded current snapshot includes 29 additional root pages with the same document 25516980651312394 and recent order. No ranked/filter/cap implementation bug was demonstrated; the old exhaustion is not stable across recovery. The alternate public document is an error fallback, not an independently sampled successful-exhaustion surface. This limitation does not itself justify a repair or bypassing the two-pass guard. The DTg retained sample explicitly has null/absent supported counts; no discarded native zero/leaf proof was found. These results are scoped to retained allowlisted projections, not unseen HTTP fields. Full evidence: public-relay-protocol-investigation.json and largest-gap-count-protocol-proof.json.

The task-only batch v1 has 24 passing isolated checks and an actual-schema shape check, but parent review found normal settled owner tokens and accepted comment dispatches wrongly block eligibility (236 captured settled candidates, zero v1 cheap eligibility). It remains unexecuted pending a combined independent-review repair batch, including deadline-boundary review. No passing-test claim is used as production acceptance.


### Progress and diagnostic correction, 13:39 UTC

The exact maintained DTg resume planner at 13:24:43 found 4,156 pending root reply branches, not the earlier naive 4,827 count. The current naive count was 4,483 and included 327 already-proved native reply leaves. The old checkpoint was not retained, so no reconstructed older exact comparison or request-count ETA is asserted. The addendum preserves the old evidence and correction. DTlf progressed from 49 to 1,887 native IDs at 13:21:35 while preserving expected 3,249; its traversal remained incomplete.

Three more two-pass source gaps were qualified at 13:37:42 against current receipts and maintained settlement: DT_x_Q2gXoF expected 238/observed 230/gap 8; DTa2iInAYpw 432/348/84; DTAia41ky8t 72/64/8. All have unchanged_passes=2, zero new/lost IDs or parent changes, exhausted non-repeated lanes, and preserved historical extras 3/19/1. No third roots. These are protocol/time-scoped public-source gaps, not permanent disappearance claims. DS96C34gfkl, DS99eoSjbnQ and DT-7HykimKC each had a fresh offline audit and reviewed first-root control accepted and remain unresolved. No batch driver has executed.

Coordinator 29266 exited 125 and existing LaunchAgent restarted 42058 at 13:22:20; that process also exited 125 and current 45054 began at 13:30:30. The status lease_lost is ambiguous: heartbeat exceptions, watchdog deadline, control LostOwnership and cleanup abort paths share the flag. PostgreSQL showed incomplete-client-message events at 13:22:16.593 and 13:30:23.887 on non-qualified backends; Supavisor showed a session client socket closed while busy at 13:22:16.027169. These do not establish the first cause. A narrow first-cause diagnostic is in implementation with the two-second watchdog/five-second server expiry unchanged. The old observer child count was stale after restart; progress-v2 now follows and verifies current release cwd/PID. At 13:36:16 there was one coordinator lease, 2,542 fresh v157 pages, 281 committed/5 unresolved comment targets and 48 committed/238 unresolved media targets. New roots remain paused during this diagnosis.

Batch v2 fixes all five initial review findings and has 32 affected passing checks; independent fixes-only review is pending and current-process manifest qualification remains required. The committed-media wrapper full review found only its unpinned external verifier bootstrap; that narrow fix is in progress. Both remain unexecuted. Neither source candidate nor scoped media proof constitutes final January acceptance.


### First-pass follow-up and diagnostic candidate, 13:44 UTC

The three latest first roots settled by the 13:43:11 snapshot: DS96C34gfkl expected 183/observed 167/gap16, DS99eoSjbnQ 153/147/6, DT-7HykimKC 157/152/5. Each maintained unchanged_passes=1 with no new/lost IDs or parent changes; preserved historical extras1/3/3 remain. These are eligible for a second fresh-audited pass only after recovery-control handoff is qualified, not terminal two-pass results. At 13:42:24 the coordinator remained45054 with one lease, two activity children,284 settled/2 unresolved comments,48/238 media,2951 freshv157 pages.

Diagnostic candidate ig-abort-diagnostic-62aa86e1f18c is packaged but not deployed. Exactly scripts/ingestion/local_driver.py and local_worker.py changed againstv157; new test_coordinator_abort_diagnostics.py. Twelve focused checks and nine retained cleanup/reaper checks pass. Packaged local smoke verifies2729files,664runtime module hashes and zero-I/O actual diagnostic watchdog/first-cause behavior; full-history remains closed. Independent review pending. Reply benchmark/canary are reused only after exact unchanged dependency hashes. No claim that a diagnostic alone repairs the restart cause.

Batch v2 fixes-only source review accepted all five findings; source_fixes_approved=true but activation_approved=false. Current-process/lease/release manifest must be refreshed after the diagnostic candidate review/rollout. Settled-media wrapper v2 addresses its sole P2 bootstrap finding with a pinned verifier bytebuffer before executable imports; five affected new tests pass and29unchanged cases reused, final fixes-only review pending. Both operators remain unexecuted.


### Diagnostic review and retained-data progress, 13:52 UTC

D17 candidate62aa is unaccepted and unshipped. Complete independent review found two issues: optional lease metadata/SQLSTATE access happens on the producer path before abort/reaping (bounded LocalDispatch.close probe proves both delayed), and connection.info.backend_pid is not qualified as the server pg_backend_pid namespace. One narrow combined repair is assigned: primitive first-cause capture/abort without optional work on the shutdown path; optional enrichment only off-path; capture server PID in existing acquisition SQL or correctly label protocol PID. Preserve2s/5s/15s thresholds. No claim these candidate findings caused prior installedv157 exits.

DTlf at13:50:09 reached2737 native IDs (2563roots,174replies), rootexhausted=true,1945pendingreplybranches, expected3249 still preserved; initial49/prior1887. DTg13:44:34 actualplannerpending4066 vs4156 at13:24,14723IDs; no root restart. Both current traversals remain incomplete. Offline-only first settled-media candidate DS-H2Lhj9BN audit matched35pages/34IDs, zero replay/sourcecalls, unchangedcheckpoint. Its wrapper/control remains unexecuted.

The parent prepared a D17-only driver release mapping as frozenv3; since diagnostic review requires changes, this exact62aa pin will need one final explicit identity update after the diagnostic repair freezes. Frozenv2 acceptance, frozenv3 and all candidate packages remain preserved; no permissive dynamic release selection or productionmanifest activation.


### v158 reviewed diagnostic rollout and fresh qualification, 14:01 UTC

Combined fixes-only review accepted ig-abort-diagnostic-4888620b4113. Producer abort now performs only nonblocking fixed-cause/reference capture and Event signaling; minimum cause is emitted by an independent daemon before optional enrichment. Actualpg_backend_pid is read in existing acquisitionSQL, and watchdogcomparisonage is captured while latecontext is labeledreport_time. Fifteenfocused finaltests plus8affected acquisition/cleanupchecks pass; reviewed remainingv1checks reused. All1039runtime and2729local files independentlyhashmatch, exactly2runtime-script changesvs v157. Rejected62aa package/manifests remain in package-v1-rejected and originalreleasepath.

Modal main/admin-56995/trr-backend-jobs deployedv158 at13:57:25UTC, readinessallpass/APIhealth200healthy. Freshcloudtask ta-01M5Q37QR56WA5PFX06PQ14S6R verified664 packagedruntime modules/zeroIOsmoke. Cloud doesnotship localscripts: newdiagnosticbehavior is proved by actualpackagedlocal smoke, not falselyattributedtocloud. ReplyHTTP/parser/persistence/deadline dependenciesmatch priorqualifiedcanary/benchmark exactly.

Controlledlocalhandover13:58:24 reaped45054/oldchildren and verifiedzerooldlease before onebootstrap54108. Savedcommentpages101200before/after; policyrev7/six and runidentity unchanged. Existingtargetleases/checkpoints retained. Originalobserver45478 was explicitlystopped; firstcase-sensitiveexecutable-nameprobe missedit, correctedexacttaskpathprobe stopped/reaped it, correctionevidence preserved. OneD17finiteobserver now follows currentrelease/process and projects only safeabortdiagnostic fields.

Installedqualification14:00:54 passed all2729filehashes, process54108/startdescriptionhash, actualserverlease258096, effectivepublic/reply2/R2binding/full-historyclosed, policy6 and26freshcommittedpages. This establishes newruntimepersistence, not the cause of earlierambiguousv157exits or finalJanuarycompletion. No new scheduledtasks.

Settled-media wrapperv2 and batchdriverv4 sourcefix/pinreviews accepted. DS-H2Lhj9BN retainedaudit35pages/34IDs matched withzero replay/sourcecalls; exactplan cdfbf0478a0eb5a23823aa68d4a7a7b8ef29aa35e50b3fc4ae05d219a0a67022 prepare/dryrun passed then apply accepted. Oldmediareceipt b24a1e12-781a-48d0-a36a-02c5c073306d and savedrows/bytesreferences/history preserved bytransactionalpostconditions; normalrootcontrol invalidatesmedia settlement pending freshproof. This one target is outside237batchallowlist; batchglobaloutstanding-rootcount will still includeit. No furthermanualcontrolswhilebatchruns. Finalbatchmanifest preparation remains pending, with immutableall-requesthistory and exactcurrentjournal adoption.


### Reviewed batch activation and actual persistence, 14:17 UTC

The final v4 manifest retains the exact 237-target cohort and all 20 prior command identities across 12 adopted targets. Independent launch review accepted manifest 5de6e54d8904b9713e0183928e9c85d213320731ed050db08cf568359195c414 and driver 018debcf6b0e1590be2b251d8051cb8ddf80eb676aad68a7b5a25bc5638761f3. Observer54205 was stopped and reaped before the sole finite controller59119 launched. Coordinator54108/serverlease258096 and deployedv158 remain pinned; four outstanding recovery roots maximum, existing six-worker policy, public-only/reply2/full-historydisabled unchanged. No scheduled task was created.

The first batch turn accepted DT-SI_RlmQR, DT-YwRFDKMT and DT-f0t2k8T_ alongside continuing DTlf9zjkmRx. A separate read-only qualification at14:17:01 proves30/21/16 fresh native pages committed respectively; control acceptance alone was not used as persistence proof. Second controller turn retains four outstanding roots and issues no further command. Evidence: batch-recovery/first-batch-live-persistence.json and events.jsonl under the task evidence base.

The earlier DS-H2Lhj9BN settled-media canary finished its first recovery at expected35/observed34/gap1 with maintained unchanged_passes1, all34 prior canonical IDs retained, and fresh native/canonical equality. Its invalidated media subsequently committed attempt2 by14:07:07. Old receipts/history remain preserved. This is scoped recovery/media proof, not the final full hosted-byte certificate.

The remaining settled-media tranche is not activated. Source review identified a reachable later finalizer admission path that could authorize a fifth outstanding root when all existing roots are blocked/exhausted; current productive original traversals prevent that condition. Common admission must be resolved before successor activation. Preserved historical canonical extras also need a valid historical-evidence coverage contract before exact media/certificate gates can pass; no saved rows, counts, or requirements are removed. These are technical work, separate from already qualified two-pass public-source gaps.


### Historical lineage investigation and next admission repair, 14:25 UTC

First-live persistence review accepted the67new savedpages within its limited scope (batch-recovery/independent-review-first-live-persistence.json). DT-YwRFDKMT subsequently settled its first recovery; controller admitted DT-oW-GDV1E while retaining four outstanding roots. No statement of parity or final media acceptance follows from this.

A bounded exact-job/run read-only investigation found no qualified immutable native payload or supported archive reference for preserved DT_FCpNDY41 historical comment17985564182779954. The old canonical DTO and June16clocks remain; exactlegacy job/run metadata, one aggregateaudit, legacyrun evidence and current/legacy page identity lookups provided no originalnative body. This is a historical native-lineage hold, separate from the maintained two-pass current public expected243/observed236/gap7. It does not prove publicdeletion or absence from all externalarchives. No unioninventory certificate or equality relaxation is implemented. Evidence: historical-coverage-investigation/consolidated-evidence-and-design.json, SHAd34b9e8299033cbcd15be06147f430c16df43dceff116e047998748993b57715.

Common recovery admission is in source implementation only: nonblocking run/account advisory admission after acceptedjournalreplay, four outstanding unresolved recoveryroots acrossauthorizers, and typed capacity/busy deferral that keeps finalizer waiting. Nonblocking acquisition is required because wrappers alreadyholdtargetlocks. Current installedv158 and activebatch remain unchanged pending tests/review/controlledhandover.


## October 25 repair continuation — owner 01a171e0

The user authorized repair and live follow-through without waiting for domain
access. Keep the exact frozen January run, all saved data/native evidence,
public-only access and full-history-disabled gate. Lead owner is chat
`01a171e0-cb87-7423-940b-53be471e81bb`; previous controller remains paused and is
not restarted with stale process/lease pins. Existing checkout/current branches
are retained; Backend HEAD at adoption is `6b92330f4f9ee9084df13ef2e6d38787f5434a93`.
Unrelated dirty files and index state are preserved.

Checkable continuation:

1. Qualify shared nonblocking command/root admission and abort-aware finalizer.
2. Fix generic public reply-branch starvation with durable technical failure
   metadata, fair continuation and strict native completion guards.
3. Keep the verified TRR R2 binding; identify hosted HTTP clients explicitly and
   report bounded safe403 diagnostics. The observed403 is client-specific1010;
   current requests/curl and descriptive TRR user agents return verified bytes.
4. Independently review the complete candidate, deploy to verified
   `admin-56995 / main / trr-backend-jobs`, verify readiness/source and smoke,
   then perform one recorded local worker handover with rollback retained.
5. Reconcile January recovery journals, issue only reviewed exact-target
   continuations, and validate fresh page preservation/media progress. Final
   acceptance requires all unchanged-evidence/count/media/hosted-byte gates;
   historical DTOs without native evidence stay preserved and unresolved.

Source ownership: recovery helper owns comment_recovery.py/local_worker.py and
its admission tests; branch helper owns comments.py/comment_transport.py/
composition.py/reply failure scheduling/fetcher and targeted tests; lead owns
hosted reader/client tests, integration, release/control and this record.
Evidence base: `~/.codex/tmp/trr-instagram-20261025-01a171e0/`. Investigation
records are sibling `trr-instagram-20261025-r2` and `trr-instagram-20261025-stall`.

Passing source evidence so far:60 recovery/worker cases plus2 strengthened
journal cases;100 existing certifier cases and48 subtests plus4 new client
identity/denial cases. A new test initially missed its fixture context; it was
corrected locally and only those4 cases rerun. No source validation is a claim
of deployed or final January acceptance.


### October 25 v159 rollout and live preservation

Implementation commit `98ccb8ed` captures the accepted prior January runtime
prerequisites plus this repair; unrelated dirty work stays outside that commit.
Workspace contract commit `eca0623` aligns recovery and storage semantics.

Frozen release `ig-public-recovery-c4a7910aa922` (runtime SHA-256
`c4a7910aa92239564f0e1ede53a827f56178c6c06d172670da208030cca81d96`)
received independent source/procedure acceptance. All 2,733 package files and
1,040 runtime entries matched. Modal v159 deployed at03:39:38 EDT to the live
verified `admin-56995 / main / trr-backend-jobs` app
`ap-DkLTRoSvqhbkGO7fHyHxrD`. Maintained readiness and API cold-start health passed;
a fresh container verified 665 deployed Python modules and the state-free targeted
worker smoke. The initial probe wrongly included an auxiliary script outside the
maintained image; its failed output remains preserved, and the corrected probe
uses the actual package mounts. Local coordinator/certifier scripts are verified
in the separate installed package rather than claimed present in that image.

The existing local worker was drained before one bootstrap of the new release.
Run/intent/generation, frozen 286 membership, policy revision 7/six workers,
public access and full-history-disabled state remained unchanged. The preserved
two-second heartbeat watchdog caused one restart at07:46:16 UTC after a 2.48s
heartbeat delay; no timeout was relaxed. The current process 47444 and one actual
server lease 344161 were qualified at07:56:40 UTC with 41 fresh committed pages.

The R2 403 was a library-default urllib client-signature denial (Cloudflare 1010),
not missing bytes or a disabled bucket. The frozen hosted reader sent
`TRR-January-Media-Audit/1.0`, read 4,847 bytes matching the retained SHA-256 through
S3 and the exact managed public URL, and decoded the complete 150x150 JPEG.
No domain, bucket binding, object, proxy, login, paid-provider or route change was
needed. The existing requests reader also passed before this identification fix.

A focused independent review corrected a circular qualification prerequisite:
separate installed identity and preservation proof precede one diagnostic grant;
fresh-page acceptance remains required afterward. Exact DTg checkpoint
`10bd91404c9ab6d6ffe129ffd13d7621f22d468279fa92482fe5494ade824506` was preserved
under a journaled 128→129 maximum-attempt grant, with no root restart, counter
reset, backoff removal or weakened productive extension rule. Owner generation 132
now saves fresh pages while the original failed parent remains explicit technical
metadata outside native page proof. The 07:59:46 preservation snapshot proves all
14,733 prior native IDs/parents retained and all 14,797 original canonical rows and
missing-status values unchanged; four new IDs grew canonical inventory to 14,801.

Latest stage snapshot before attempt rollover: comments 285 committed/1 unresolved;
media 48 committed/238 unresolved. Stage settlement does not prove count parity.
January final acceptance remains false/pending because traversal, source-count,
historical native-lineage and complete media/hosted-byte gates remain open.
Full-history remains disabled. No recurring schedule or competing batch controller
was created. The previous batch controller remains paused; the maintained worker
owns productive continuations, subsequent guarded recovery and certification.

The two retained runnable copies are current v159 and rollback v158. Twenty-one
superseded inactive runnable packages were moved to actual Trash with exact source
verification and recovery mappings; original task evidence and saved data remain.
Evidence base: `~/.codex/tmp/trr-instagram-20261025-01a171e0/`, including manifests,
review acceptance, deployment/readiness/cloud smoke, activation preimages,
installed qualification, exact diagnostic journal and preservation receipts.
Backend/source checks and the live repaired path pass. One existing media-cache
fixture fails identically on v158; that unchanged unrelated failure remains
recorded. TRR-APP build: not applicable (no app/API contract change). SQL: existing
journal/target fields only; no schema migration or ownership change.


At 08:05:37 UTC, the finite read-only diagnostic watcher observed the maintained
productive budget rule extend DTg from 129 to 141 maximum attempts after the
successful generation 132 continuation ended normally with target_deadline. This
is a journaled productive extension under the unchanged rule, not another manual
grant or root reset. The finite watcher exited; no monitoring process or schedule
was left behind. See diagnostic-successor.json and diagnostic-watch.jsonl in the
same evidence base. All five routed helper assignments were reconciled and
criterion-assessed; native exact plaintext instruction matching remained
unverified because host input was encrypted. Later read-only follow-up evidence
is retained separately with the R2 and focused ordering receipts.

### October 25 certification continuation, 08:27 UTC

The user explicitly requested continued repair until January passes certification,
with parallel helpers. Current v159 remains the source/runtime baseline. This
phase has one coordinated editing owner (lead) in the existing checkout; helpers
prepare private operational candidates and independent evidence. No branch,
app-build, full-history or public-only setting change is authorized by this phase.

Checkable next work is: reconcile and independently review a finite v159 batch
controller adopting every prior root journal identity; activate at most four
outstanding roots under the existing six-worker policy; obtain exact safe native
error diagnostics for the remaining DTg parent in a serialized local stream;
resolve historical native coverage before ordinary media admission; then rerun
only applicable certification gates against stable evidence. Saved canonical
extras cannot serve as their own independent native witness.

At 08:27:29 UTC the exact 286-target cohort remains unchanged: comments285
committed/1unresolved, media48/238. DTg has14,745 native IDs and14,808 active saved
canonical rows, with current owner generation135/attempt132/max141. Its productive
continuation remains active. Current preflight returns final_acceptance=false:
269 known count-gap posts/9,463 total gap. The 237 unattempted media targets held
on persisted_comment_inventory_changed preserve4,352 canonical extras absent
from their current receipts, with zero receipt IDs missing from saved rows.
These are separate lineage and current source-count holds. No inventory union,
count reduction, saved-row deletion or guard relaxation is implemented.

Private continuation evidence lives under trr-instagram-20261025-01a171e0,
trr-instagram-20261025-batch-continuation, and the existing helper evidence
siblings. Initial preflight performs no storage fetches or deep retained-page
scan while the active count-gap guard applies. The earlier paused controller is
not restarted with obsolete pins. This record will retain actual activation,
persistence and final certification outcomes separately.

At 08:38:43 UTC, an independent current-planner snapshot proves339 completed DTg
branches since the exact07:45:36 v159 activation. Comparable unresolved branch
counts4156→4066→3592→3253 show actual progress. Current3253 comprise3251 healthy
pending branches and2 retry-eligible technical failures, with no deferred/held
or repeated native lanes. Current native inventory14747 retains the original
14733. Owner136 was productive within one second of the snapshot. Healthy-tail
arithmetic is several hours and does not establish a certification ETA.

The finite v159 batch source/procedure candidate received independent acceptance
(0aec3860 candidate /651ddc75 driver); preparation and activation remain pending
fresh runtime proof and generated-manifest review. The private four-send public
diagnostic/handover review found one pre-execution defect: POST JSON/cookie
processing could mask a response Retry-After. The narrow header-first repair
passes21 affected cases; fixes-only acceptance remains pending. No diagnostic
or handover has yet executed at this record point.

Heartbeat finish-january-instagram-certification is ACTIVE in this same chat,
15-minute cadence bounded to48 checks. Saved configuration verifies the exact
chat01a171e0 and prompt hash488598cbd4bce5bf590b098862cd993d3b3c70b61a4e78976b13954bea903a86.
It continues authorized repair and certification work, remains quiet for ordinary
increments/unchanged waits, and preserves public-only/full-history-disabled/data
and journal constraints. All four earlier Instagram heartbeat automations remain
paused; none was resumed or modified. The worker and future controller remain
application-owned; the heartbeat must not create a competing diagnostic stream.

### Controlled diagnostic attempt and productive worker restoration, 09:08 UTC

Final private diagnostic review closed the header-before-parser Retry-After
repair. A recorded handover unloaded the sole worker at08:54:52 UTC. The private
handover guard then incorrectly counted all910 historical accepted dispatches
as active. The exact task wrapper was stopped and its failed finally/restore
outcome retained. No diagnostic capture or HTTP send occurred. A separate narrow
controller repair preserves the originals and uses the already-reviewed exact
current-owner/unresolved/active-or-ambiguous predicate. Actual isolated SQL
regression and independent fixes-only review passed. At08:59:40 live metadata
showed zero leases/activeowners/currentunresolveddispatches;909 accepted history
rows comprised45superseded-token and864settled-target rows. None were cleared.

The repaired existing-drain resume passed at09:03:36 without another bootout.
Capture then hit a five-second QueryCanceled before pins/started/source requests.
The outer finally bootstrapped the same v159 once at09:03:46; startup exec raced
the initial process proof. Safe same-instance reconciliation at09:05:44 verified
PID34489, sole backend lease354626, exact intended plist with only log paths
changed, linked task log inode529864435, unchanged timers/runtime/binding.
Fresh current-process qualification at09:05:34 proves10new committedpages.
No wrapper/drain/capture/apply retry or saved-business-state reset occurred.
All failed outcomes, original candidates, preimages and new recovery artifacts
remain in diagnostic-handover; restored.json records actual successful runtime.

A single unchanged-timeout read-only timing pass found canonical full-row
fingerprinting dominant at3.178s versus0.194s for target/checkpoint hashes; all
queries passed in that sample. The exact failed statement is not proved. A
separate private versioned tuple-version guard candidate is in preparation to
avoid repeated large canonical-row serialization. It is not deployed or executed.
The productive worker remains running during that independent work.

Frozen batch preparation succeeded with237target allowlist,286membership,
26priorjournalidentities across18adoptedtargets andzerooutstandingroots.
Eight exact two-complete-unchanged targets are ineligible for another root,
including DT_FCpNDY41; these ledgers are preserved. Prepared manifest SHA
2d2f797fb0ee9ddba7d0bd5d66d6a3c1c9ee38b0be792d3b30d3c43c996cc0f2
awaits generated-manifest launch review. Source review remains accepted; the
current process qualification is pinned separately. No new batch is launched
at this record point. Current paused old controllers must not be restarted.
January remains uncertified and full-history remains disabled.

### First-cause recovery reassessment and source candidate, 09:32 UTC

The new finite batch did launch once, but paused before any audit/root command
at09:11:13 with technical_ProcessLookupError because pinned coordinator34489
had exited. Preserve its events and manifest; do not restart it or overwrite its
pause. The linked log captures heartbeat_deadline events at09:08:18/09:09:49/
09:12:28. Captured watchdog ages were4.759/3.947/4.123s. SELECT durations formerly
shown alongside them are report-time values and may refer to later queries;
they do not prove database execution latency. The sole worker continues through
LaunchAgent restarts; all original budgets/fences remain intact. One bounded
paired exact-session sample was healthy and cannot explain those earlier stalls.

Source-backed repair candidate ig-heartbeat-readiness-904e30d697f1 is frozen at
heartbeat-rollout. It has2734package files/1040runtime entries and exactly3runtime
deltas fromv159: local_driver.py,local_worker.py,workflow_store.py. Readiness
loads only the post/comment fields its unchanged exact proof consumes, avoiding
unused rawJSON. The ordinary full media snapshot is unchanged. Coherent immutable
heartbeat samples, query/completion sequences, watchdogwakegap and fixedforeground
phase separate firstcause timing from later report_time_lease context.2s/5s/15s
guards and heartbeat SQLcount remain unchanged. Neither change is claimed a
verified rootcause fix. Affected projection/PGguard and actualthread/worker/lease
checks pass; complete review found only an activation shell-vs-Python identity
proof issue. That narrow operator fix now pins actualkernel executable plus
exactworkerargs/CWD beforeandafter a bounded stabilitywait. Source/runtimebytes
remain unchanged; fixes-only acceptance and deployment are pending.

Private probe_v2 source delta is independently accepted (activationfalse). Its
explicit canonical tuple-version fingerprint avoids repeated rawrowserialization,
with registered-PG proofs for arbitraryTOAST updates/inserts/deletes/status and
unchangedrows; all originalproof/timeouts remain. It has NOT captured pins, started,
sentHTTP or changeddata. Defer its livewindow while productive traversal and
source/runtime recovery continue; no old failedwrapper/capture may be blindly
rerun. Media preparation investigation found no existing legitimate custody-only
newdownload path; preserve exact ordinary nativeinventory/equality admission.
No new byteaudit of alreadyverifiedobject was repeated. January certification
remains pending; public-only/full-history-disabled rules remain mandatory.

### v160 deployment and diagnostic-window outcome, 10:06 UTC

The reviewed candidate ig-heartbeat-readiness-904e30d697f1 is deployed as Modal
v160 in admin-56995/main/trr-backend-jobs (ap-DkLTRoSvqhbkGO7fHyHxrD), deployed
09:36:16 UTC. Target-specific readiness reports ok/core_ok with no missing
functions, secrets or blockers. Fresh deployed task ta-01M5S6P0AQXH9YFHREYV5E841R
verified 665 actual cloud Python modules and state-free smoke; full-history stayed
closed and no native request or database connection was made by that smoke.
Separate local-package validation verified 2734 files/1040 runtime entries.
The local handover installed the same candidate; timers and source policy stayed
unchanged. Deployment, source and private operator receipts are retained under
trr-instagram-20261025-01a171e0/heartbeat-rollout.

The coherent 09:41:53 watchdog sample shows the next heartbeat query started
2.451584s after the previous completion; it had been in flight only 0.074125s
at abort comparison. A later 10:02:07 sample has a 4.568301s watchdog wake gap.
These establish delayed scheduling before heartbeat execution; they do not
prove database-query latency or distinguish the underlying GIL/OS cause. The
2s heartbeat deadline, 5s server expiry and 15s child-exit fence remain intact.

The accepted v160 diagnostic wrapper drained the sole worker and waited for
natural owner expiry. Fresh capture completed at 09:55. The local parent-approval
window expired during context handoff, before any probe HTTP request. Its
finally restored the same v160 once at 09:57:10, verifying actual Python PID76152,
sole server lease358650, exact source/plist and linked log inode529928013.
The worker later restarted on the recorded heartbeat deadline. Preserve the
expired window, captured pins and all earlier failed outcomes; do not rerun those
frozen directories. A fresh bounded diagnostic candidate and same-source
qualification-epoch controller are being prepared independently. Neither has
executed at this record point.

The 10:05:42 read-only snapshot retains the exact 286-target cohort: comments
285 committed/1 unresolved and media48 committed/238 unresolved. DTg has14760
native IDs,14822 active saved rows,107768 total run pages and owner143, with
attempts140/max141. Productive budget extensions use the maintained journaled
progress guard, never a root reset. January is not certified. All saved data,
public-only operation, native proof requirements and the full-history hard stop
remain unchanged.

### Standard scheduling trial and actual public diagnostic, 10:19 UTC

The sole service was unloaded at10:13 for one combined diagnostic/scheduling
window. Its first drain guard rejected mapped files opened by the operator's own
installed virtualenv interpreter. The original failed outcome and failed finally
remain immutable. A zero-connection reproduction identified15 self-owned mapped
files inside the release; the maintained Backend virtualenv opened none there.
Both use the same pinned Python3.11.17 kernel. An independently reviewed resume
uses that maintained operator interpreter, preserves every original guard and
preimage, and resumes the existing bootout without another service stop. No
open-file exception, owner reset or lease bypass was introduced.

Natural drain completed10:18:25 with no active owners, dispatches or lease. Fresh
exact pins were approved at10:18. The diagnostic sent one anonymous request to
the historical exact-comment permalink and received HTTP302. endpoint_redirect
stopped the stream before any DTg bootstrap/child request. It retained
preserved_preimage=true; saved rows, targets and checkpoints were unchanged.
That response supplies no comment-membership or absence proof. No redirect was
followed and no second diagnostic stream was started. Normal-path typed failure
retention is the next source repair for future automatic child failures.

Finally restored v160 at10:19:03 with actual PID98866, sole server lease360211
and linked log inode529980103. The exact requested plist delta is ProcessType
Background→Standard plus two unique log paths; all other fields are identical.
Observed launch classification changed background(5)→daemon(3). The 2s/5s/15s
guards, source/public policy, concurrency and frozen cohort remain unchanged.
This is a persistent launch policy trial, bounded to20minutes or the first miss
for observation, followed by an explicit keep-or-rollback decision. It does not
yet prove the underlying heartbeat issue repaired. New-owner persistence still
requires separate current-process qualification.

Heartbeat finish-january-instagram-certification is temporarily PAUSED during
active parent recovery because its10:14 trigger interrupted ongoing work. Resume
it after the sole runtime/controller is qualified; preserve the existing cadence,
finite count and quiet notification policy. This pause holds the follow-up timer,
not the user's repair objective or native worker. The source candidate is retained
in Backend commit f960a4be; unrelated dirty work is preserved.

### Local diagnostics, retry-hold repair and productive continuation, 11:02 UTC

The user explicitly selected local execution. Modal v161 had already deployed
the reviewed diagnostics candidate ig-reply-diagnostics-8e3350210aa9 at10:36:52
UTC in the same authorized main target, with readiness and API cold-start health
200. No further cloud-container smoke or cloud deployment was started after
the local-only instruction. The local handover started actual PID16296 at10:38:41,
retaining all108265 saved run pages, exact run/policy and Standard launch class.
The four diagnostics modules plus launch template have85 distinct targeted
passing checks and independent source/package/operator acceptance. Failed native
reply observations now retain bounded numeric diagnostics and honestly labelled
decoded-response hashes; legacy branch identities and holds remain intact.

Actual normal-owner requests exposed primary doc36239935742272683 error1675030
and fallback doc34884685271179117 error1675004. The latter's retained diagnostic
classification is rate_limited; numeric codes alone do not establish a broader
source diagnosis. The saved reply_access set contained1985 pending parents,
including all four technically held parents. Its early resume branch bypassed
their existing holds/backoff. Multiple owners147–156 repeated those parents;
their failure counts rose to11. This was not an increase within one owner, and
no empty fallback or deletion conclusion is supported by the retained envelopes.

The narrow repair changes only PublicRelayResume.from_checkpoint: a rejected
lane now composes technical deferral with the saved server wait. Either defers
the parent, preserving counters, cursors, classification, restart precedence,
permits and native completion proof. The regression failed before the repair;
14 affected checks pass afterward, with the85 unchanged checks reused. The
reviewed local package ig-local-reply-holds-056cfe0023bf has2737files/1041runtime
entries, exactly one runtime delta from v161 and one new focused test. Actual
local PID33810 started10:55:07. Its binding explicitly records local_only scope
and retains the real cloud v161 identity; no cloud deployment record is invented.

After installed-source qualification against the real saved checkpoint proved
all four held/access-overlap parents deferred, a separately reviewed idempotent
transaction granted exactly one continuation, max153→154 at10:59:38. Checkpoint
ce7a621bdc659e364f4da8c96252012bd713483b81681b5f5afc83bb312beb26,
attempt153, owner156, all four holds/counts11, waits and saved data were preserved.
No root reset or repeated unchanged grant is permitted. The existing productive
budget extension remains available only after its ordinary committed-page guard.

The11:01:53 read-only snapshot proves42 newly committed pages (108307 total)
under new owner157, started10:59:51. All four previous held counters remain11;
their evidence clocks remain10:45, proving they were not re-requested. One newly
encountered technical branch has count1 and qualified diagnostics; it does not
erase the productive healthy traversal. January is still uncertified. Source
and operational receipts are under normal-diagnostics-rollout/local-hold-repair;
the strict local-source recovery controller awaits actual process/page
qualification and generated-manifest review. Public-only, reply2, six activities,
all saved data and the full-history hard stop remain mandatory.

Backend validation:85diagnostic/priority checks plus14affected guard checks and
actual packaged import/gate smoke pass; live continuation now commits pages.
TRR-APP build: not applicable (backend/local scraper only, no app/API contract
change). SQL uses existing target/journal fields; no migration or SQL ownership
change. Modal remains v161 by the user's explicit local-only selection.

### Parallel local recovery activated and observed, 11:17 UTC

The local-only controller source, exact installed-source qualification and
generated manifest received independent acceptance. Manifest
1c4ef2f8ca4e8bc7e9f5239229d9c3739f712f9dafff24d67b74f0bdd475b3f9
retains237allowed targets,286members,all26prior journal identities/18adoptions,
eight two-complete-unchanged exclusions and no initial outstanding recovery roots.
Local authority is established by the reviewed source/package/install/binding;
the cloud v161 identity is retained separately. No cloud deployment is required
or fabricated. The epoch module retains the accepted lease/helper/restart fixes
and all finite bounds. Maintained final certification already supports the local
binding; source-only inspection found no cloud-tag equivalence requirement.

One finite controller launched at11:13 in the private batch-controller directory.
It accepted four guarded roots, with no fifth while those were outstanding:
DT0XyMHjuCe,DT0aEMqDZz9,DT0fHDwFSjb,DT0hPUfCSMH. The11:17:04 read-only proof
records77,48,60,49 fresh pages respectively:234actual new pages, each matching
the new reservation generation and captured after root acceptance. This is
actual persistence, not command-acceptance-only evidence. The original DTg
continuation remains independent and productive. One coordinator lease remains.
The temporary stage ledger is comments281committed/5unresolved because four
settled targets reopened for fresh evidence; no saved data was removed. Media
remains48committed/238unresolved at that snapshot and still requires its ordinary
exact native/comment-inventory and byte proofs. January is not certified.

Current local code is preserved in Backend commit f2da8ec1, following f960a4be.
Only task-owned paths were staged; all unrelated dirty work and the empty prior
index were preserved. No branch switch, push or pull request occurred. The
current local package and v161 rollback are retained. Obsolete v158,v159,v160
runnable directories were moved whole to actual Trash with source hash checks
before and after, inactive-handle checks and recovery mappings. All task evidence,
native receipts, saved data and object storage remain intact.

Application-owned local execution continues through the finite controller and
existing LaunchAgent. The existing follow-up is to resume with this current local
scope after active handover work ends; it must not start a competing controller,
deploy new cloud code, reset held branches, grant another unchanged continuation,
or waive any certification gate. Ordinary increments remain quiet. Reassess at
the controller's bounded exit or a genuine new failure, then continue authorized
repair from preserved evidence. Full history remains disabled even after any
future January certificate.

The same follow-up is now ACTIVE again with its unchanged15minute/48check bound
and quiet notification intent. Saved prompt SHA
58cac29b58d1bbf431fd73902e2467a9e30c55d97107a878b7519e5b2e7ba335
pins the current local source/controller, honest retained cloud identity, no
further cloud deployment, and the existing data/public/full-history guards.
The five original routed helper assignments remain reconciled and accepted;
follow-up source/operations reviews are retained with the current evidence.

### Local controller recovery, 12:44 UTC

The first finite local controller paused at11:55:40 with
`helper_failed_acceptance_uncertain` for DT3Wh4Hkh3o. Its process exited2 and
its helper was reaped. The original failure cause remains UNKNOWN because
that driver discarded helper stderr/stdout. Its frozen directory contains no
audit-input, report or root request; those markers precede retained business
audit/replay. The authoritative target and journal reconciliation found no new
root command for that selection. A later separate read-only exact preflight
passed all phases in8.16s with the same checkpoint, zero native requests and
zero business writes. This does not establish the original error's cause.

The replacement is a NEW local controller candidate under
`normal-diagnostics-rollout/local-hold-repair/controller-recovery-1201/candidate`.
The old paused controller, failure directory, events and diagnostic windows
remain frozen. Typed helper outcomes now distinguish named pre-business
deferrals from unknown or post-boundary failures. A valid deferral stops further
helpers for the turn and waits120s within the original budget. An immutable
business-start marker, full binding checks and mandatory journal reconciliation
retain the uncertainty fence; missing, corrupt or unknown outcomes still stop.
No arbitrary exception text, SQL, stdout or credentials enters these outcomes.
The root operator and qualification module retain their accepted bytes.

The focused candidate has17distinct regression cases, staged affected passes,
lint and7production AST checks. Passing unchanged local/epoch/capacity checks
were reused. Independent source acceptance and generated-manifest launch review
found no blockers. The generated manifest SHA is
1229a66bacc7392752e33d8533e8787f9f76f2a3f2ab3eb254ba17e06d55d4fb;
driver SHA9f7f63dd30dfc6d53ff37504b6dd2e95f71376553ae660ff357776cea34c6228.
It carries all39prior request identities/31adoptions, exact237/286 scope, eight
two-pass exclusions, and the original13roots/14audits/19charged turns. Original
11:08:15→23:08:15UTC clock,360turns/12h/800roots/1000audits and8replacement
candidates remain; no new lifetime or attempt grant was introduced.

Actual local source ig-local-reply-holds-056cfe0023bf, PID33810, Standard plist,
environment and sole lease360211/backend_start10:18:26.397808 were qualified.
Initial proof includes fresh page5e6944ba-4ac0-4e7f-b959-8689b0cdb522 under
new owner166 and its dispatch. At12:39:59 the parent observed no competing
private controller/observer, an available global controller lock and the sole
application-owned worker. One continuation launched in tool session46303.
The native worker was not restarted, and no cloud deployment occurred.

The12:44:02 bounded read-only proof `candidate/live-progress-1245.json`
(SHAd4a1d71eca8255447d931401958a00776911d7f28ebbb1fa6e6bb8f2c114ed50)
records42,40,24,30new persisted pages for DT3Wh4Hkh3o,DT3Z4UCkeaq,
DT3fgUpjrw3,DT3hSwqDfOK:136actual pages after their accepted roots and under
new reservation generations. Thus the formerly failed selection progressed
through a fresh retained audit, guarded root and real persistence. Global
four-root capacity remains authoritative; the new driver adopted the previous
outstanding root rather than duplicating it.

At that snapshot comments were282committed/4unresolved and media48committed/
238unresolved. Reopened targets retain their saved rows and receipts. DTg was
166/166, owner_generation169, without an active owner. The maintained ordinary
productive guard already extended154→166 earlier, as recorded in worker.log;
the controller's ceiling event is advisory, not evidence of a guard refusal.
That guard runs at a drained scheduler boundary and requires actual committed
page improvement. Leave its decision to the sole worker; never repeat the
manual153→154 grant, reset the target or bypass its held branches.

January remains uncertified: historical native gaps, reply evidence and media
acceptance still require their existing proofs. Do not run the final certifier
for unchanged known gaps. Saved extras, evidence clocks, counts, native parent
identities and object bytes remain preserved. The R2 managed-public403 repair
still stands; domain access is not a prerequisite. Full history remains disabled
even after any future acceptance. Backend/local control validation is recorded
above; no app/API contract or SQL migration changed. Cloud remains honestly
v161/ig-reply-diagnostics-8e3350210aa9 by the user's local-only instruction.

The follow-up timer was temporarily paused during this parent-owned recovery
to prevent a competing handover. The same finite quiet follow-up is ACTIVE again
with its original cadence/check bound and thread binding. Its saved prompt SHA
86161bb466c7745774590db0e2c40e899ed147d061f556b4a6dcaa33235a004a
points to the new candidate manifest/controller and retains the original clock,
counts and all scope/proof guards. At12:45:32 the cumulative controller ledger
was turn21/19root commands/20audits with four outstanding roots. Do not restart
either old paused controller or create an extra source/observer stream.

### One-shot local controller ownership, 16:26 UTC

The 1201 continuation recorded `technical_KeyboardInterrupt` at 15:45:42,
54.197 seconds after completed turn 98. Its process was absent at the 16:00
check. The driver converts SIGTERM to KeyboardInterrupt, while SIGINT can also
produce that exception; the signal sender and cause remain UNKNOWN. Its expired
tool session could not supply a direct exit-status observation. All 67 new
audits, root starts and accepted roots matched, and all 79 charged turns had
completed before the interruption. No helper or uncertain command remained.
The native worker stayed at PID 33810; its source and server lease were unchanged.

The old directory remains frozen. A NEW candidate under
`local-hold-repair/controller-recovery-1600/candidate` carries 80 root commands,
81 audits and 98 turns from the full predecessor chain. It preserves the
original 11:08:15.797480 to 23:08:15.797480 UTC clock and all finite bounds.
Preparation reconciled 106 control request identities, 98 adoptions and 107
global journal identities. DS-H2Lhj9BN is global evidence within the 286-member
cohort, outside the 237-target control allowlist; it gained no new authorization.
Eight two-pass exclusions and the actual unused eight-replacement budget remain.
The first helper failure stays UNKNOWN in the nested provenance. No counts,
checkpoints, journals, receipts or evidence clocks were reset.

The narrow source change supports this proved clean interruption and compounded
continuation. Runtime/Driver class ASTs and the five operational modules remain
unchanged. Thirteen focused regressions, lint, formatting and seven AST checks
pass; unchanged validation was reused. Independent source, generated-manifest
and actual launch-envelope reviews found no blockers. Manifest SHA is
e757566dba8d91bcc266e76b01b13e66e9130cfc0f31089f83435aedf9553caf;
driver SHA is
6127cc993c69161ca5fd6cc42fe4d7277e67e39debc6538389ef72ef5b2daa3d.
Fresh maintained qualification passed without altering its page-proof rules.

At 16:20:45, the parent bootstrapped the reviewed one-shot job
`gui/501/com.trr.instagram-january-recovery-e757566dba8d` once. Its task-owned
plist lives outside automatic LaunchAgents loading. It uses RunAtLoad, Standard
process class, private logs, KeepAlive=false and no schedule, retry or kickstart.
Default process-group cleanup covers controller helpers. Actual controller
PID 65874 has one run and a separate process group from native worker PID 33810.
Both the global exclusive controller lock and existing source guards remain.
Paused or nonzero exits stay stopped; preserve their evidence and remove only
the exact inactive job registration after completion. GUI logout or reboot can
still stop this job. No native-worker restart or cloud deployment occurred.

At 16:24:47, `controller-recovery-1600/live-progress-1625.json` (SHA
9e6f75cde6dac75a06f2c47f80ea35c5afb750658bbf8942e6cba615a0802062)
proved 29, 10 and 3 new persisted pages for DTQywKmCpwy, DTRI9mBCKiW and
DTRLxC2AcMf, under new owner generations after root acceptance. The fourth
root, DTRMBjREl_v, had been accepted about three seconds before that snapshot
and had no page yet. This is 42 actual saved pages, not four-post completion.
The completed turn 99 ledger carried 84 roots and 85 audits with four
outstanding roots. At 16:26:47 the registered job was still running once, with
the native worker unchanged and no pause event.

The 16:15 preparation snapshot was comments 285 committed/1 unresolved and
media 49 committed/237 unresolved. After four controlled reopenings, the
16:24 snapshot was comments 281/5 and media 49/237. All saved data remains.
The same bounded read-only check resolved the productive-budget uncertainty:
DTg is 166/166 at owner generation 169, with NO committed native page for that
generation. Its latest earlier page belongs to generation 166 at 12:36:40.
Worker logs also prove the drained recovery path invoked the guard without
granting an extension. The current-generation page prerequisite therefore
fails as designed; earlier productive pages do not authorize another grant.
Preserve the ceiling and held branches. Never repeat the manual 153-to-154
continuation or reset the target.

January remains uncertified. Reply access, native evidence/count gaps and media
acceptance retain their full requirements; no final certifier was rerun for
unchanged known gaps. Public-only access, six activities, reply concurrency two,
the all-authorizer four-root cap and disabled full-history remain mandatory.
R2's managed-public 403 repair still stands without domain access. Backend
source remains f2da8ec1 and local package ig-local-reply-holds-056cfe0023bf;
cloud remains honestly v161/ig-reply-diagnostics-8e3350210aa9. No app/API
contract or SQL migration changed. Only this local controller ownership and
continuation changed; all evidence and rollback material are retained.

The same quiet finite follow-up was paused only during this parent-owned
handover. It is ACTIVE again against the accepted one-shot job and manifest.
Saved prompt SHA is
b254f5d96bc46cc5e2826da36d68e596d9a8953a08c41d7b124d76ed1f6abad3;
the original creation time, 15-minute/48-check bound and thread remain intact.
Do not bootstrap it twice, kickstart it, or restart either frozen predecessor.

### Approved manual continuation repair, 21:00 UTC

The user deleted `finish-january-instagram-certification`; that automation is
deleted, overriding earlier ACTIVE timer descriptions above. The user then
approved the manual local recovery/January verification plan. No new automation,
schedule, watcher, login loader or automatic controller restart is authorized.
The original 23:08:15.797480 UTC deadline and all cumulative budgets remain.

The lead retains the existing Backend checkout on
`chore/repo-hygiene-2026-09-25` at f6c7f1c9, preserving unrelated dirty files and
the index. The lead owns this record, runtime handover and guarded database work.
A routed implementer owns only the new private
`local-hold-repair/controller-recovery-2100/candidate`; a separate investigator
owns private protocol evidence. No predecessor directory may be restarted.

The 1600 controller stopped after completed turn131:137 roots and138 audits.
All163 prior control request identities reconcile with authoritative results;
164 global identities are retained without expanding the237 control allowlist
or286-member January cohort. The worker restarted after the host shutdown as822
and then as20423 after a recorded SIGKILL whose cause is unknown. Current source
remains ig-local-reply-holds-056cfe0023bf. PID20423 has no new committed source
page and the same403104 server lease observed with822; current ownership remains
unqualified. Both observations and original clocks are preserved. A graceful
handover, if required, must prove old processes and lease are gone before one
restart, and must end with the existing local worker running.

New read-only readiness evidence records236
`persisted_comment_inventory_changed` media targets and one
`complete_comment_inventory_proof_required`, all237 unattempted. These are
inventory proof failures, not new R2 download failures. No saved row or expected
count may be deleted or lowered to admit media. The latest DTiw scan parses all84
IDs in its six retained top pages; older5203-ID evidence remains preserved and
cannot be substituted into the latest receipt. DTg remains166/166 with all six
failed reply branches and four technical holds unchanged. January is not yet
certified. Public-only access and disabled full-history remain mandatory.


### Manual repair installation and public-root comparison, 21:54 UTC

The maintained worker changes are committed locally as `8bafd6d8`. Focused
source checks and independent review passed. The installed local release is
`ig-local-public-parent-fa955e20db59`; the packaged manifest records its exact
reviewed working-tree bytes from pre-commit HEAD f6c7f1c9. No cloud deployment,
app build, SQL migration or API/status-contract change occurred. The retained
cloud identity remains v161/ig-reply-diagnostics-8e3350210aa9.

The earlier C3 qualification failed before business commands: startup opened a
separate connection for each of164 historical journal reads and exhausted the
original900second source3 window. Those journals reconciled; no helper/root
was sent by that qualification. The connection repair uses one fresh read-only
repeatable-read connection for the complete reconciliation pass. It preserves
each result/hash check and closes the connection on every exit.

One guarded parent bootstrap was applied to DTiwjjpAYzG at21:10:35 with its
existing audit/attempt/capacity gates. Together with the failed qualification,
the current compounded ledger is133charged turns/138roots/139audits,
164control identities and165global identities. The237control allowlist,
286January members, original11:08:15.797480--23:08:15.797480UTC lifetime and all
finite caps remain. No second bootstrap allowance exists.

At21:45:02 the old71900 coordinator and descendant86976 were absent and its
lease had released. The reviewed package was installed through the existing
worker job once. Actual newPID88100 acquired backend403104 at21:45:04. The
empty-lease boundary and old-process absence are retained, so pooler reuse is
proved rather than inferred. Source4 first observation is21:45:02.763235 and
its deadline remains22:00:02.763235. By21:51:08 the actual process had35fresh
saved pages; a retained strong observation identifies a new reservation41,
dispatch21:49:50, owner21:49:54 and committed native page21:50:38. Package
provenance is verified; full two-observation controller qualification remains
separate. No automation, watcher, schedule or controller restart was added.

The bounded comparison ran inside that owned DTiw activity, charged all four
wire sends to existing permits, and completed at21:50:05. Independent root
queries25516980651312394 and26113520058347588 both returned the same ordered
15native IDs, matching the latest retained root. Each has a next cursor and
no native count. This proves no query-selection/parser defect and gives no
basis to substitute15 for the expected5498 or combine unsupported inventories.
The older5203-ID evidence and every saved extra remain preserved. Ordinary
pagination continues from the maintained checkpoint. DTg's six failed reply
branches, four holds and166/166ceiling remain unchanged.

The historical controller check also exposed an old intended-plist reference
pointing at the live LaunchAgents path. The current controller repair resolves
only that exact old pin through retained byte-identical backup evidence during
historical validation; active new-plist verification stays strict. Fifteen
focused regressions, actual nested historical validation and independent review
passed. The original pre-worker readiness remains pinned, with separate review
and lineage for the later controller-only repair. No source clock is reset.
All paused controller directories, raw events and original manifests remain
frozen. January is not certified; saved scrape passes are not verified complete.


### Source4 qualification first cause and bounded reassessment, 22:07 UTC

The exact reviewed C4 manifest4421aea8351944de59ec16ca3fba9c42955b111a2fd018a76564b0799629794f
was launched manually once as91597 at21:56UTC, with no scheduler/automation.
All164 requests reconciled using one read-only connection. Two waiting source
observations at21:56:45 and21:58:46 contain the same88100/403104 process and
lease, real new-owner pages and valid handover proof. The pure lease/page
qualification predicate passes. No qualified epoch was published.

The first cause is `epoch_old_descendant_not_reaped`. Old qualified worker33810
recorded descendant86533 with identityd6bf6... at17:32:20UTC in frozen1600 epoch43.
After the actual19:19:53UTC boot, macOS reused86533 for unrelated
`mscamerad-xpc`, started21:36:23UTC with distinct identityfa0be999.... That
service was not terminated. Its boot/start/identity evidence proves it cannot
be the earlier child; the old guard rejected numeric PID reuse conservatively.
This is separate from the previously resolved server-lease ambiguity.

Source4's22:00:02.763235 qualification deadline expired. The parent stopped
only identified controller91597 with SIGTERM at22:00:26, recording its deliberate
`technical_KeyboardInterrupt` pause. It was reaped, with no audit/root/helper
business events. Compounded accounting is136turns/138roots/139audits,
164control/165global identities. Worker88100 stays running. C4 and its events,
164journal receipts,165authoritative results and two waiting epochs are frozen;
source4 must remain failed/unqualified and cannot be retroactively promoted.

The reassessed C5 repair is restricted to identity reuse with pinned historical
observation, actual reboot boundary, distinct current identity and post-boot
start. Matching or uncertain identities, same-boot reuse without separate proof,
old coordinator identities and lease/page/deadline checks remain strict. Current
source4 is charged as failed slot4; a fifth source may start only after the
whole actual predecessor/package/reaping input preflight and independent review.
Original23:08:15.797480UTC expiry and all8replacement/900second limits stay.
No controller directory may be restarted and no automation may be restored.

Independent native inventory reconciliation found no parser duplication in the
three overshoots. DTLbQv6EdE1 is563expected/571observed (470parents101replies),
DTvYo3MkcvI300/317 (272/45), and DTysU1Nj5LQ349/382 (242/140).
Their exact current retained audits match complete checkpoint, receipt and
canonical inventories with zero discrepancies/replay. Earlier native counters
are supported by retained permalink evidence through `cache_source`; a null
detail-receipt evidence FK is normal cache behavior, not missing counter proof.
The counters precede the later comment observations. Preserve both native
clocks and totals; the API's existing count_parity label does not establish
exact equality. No guarded refresh exists for these settled detail targets;
reopening directly or fitting a counter to saved rows is not authorized.

DTiw's current durable inventory is4778unique IDs at22:03:51 against5498expected.
Its latest new pages verify distinct empty reply branches, explaining low ID
growth despite new pages. The bounded sample establishes no parent pagination
loop or request-construction defect. No parser/query change, unsupported union,
row deletion or expected-count reduction was made. January remains uncertified.


### C5 actual-input preflight and same-package handover, 22:23 UTC

C5's 48 focused pure checks pass, including the actual frozen C4/C2/boot
chain, package proof, strict failed-source accounting, lease/page rules and
final deadline checks. Independent source review approved candidate
4dbe81a82fa87e5a7f7a30807946f948483e548516c9befd54e5d720821c705c
and driver407fe3812f1c8b8774a381c2ae6316259dd64ab0385b0a58a376400b5778259f.
The complete actual-input preflight then passed before any fifth-source start.
Receipt8de9ccc6a0e7d9fd59a3989c52ac3b8abfce8e8036e0356dd6f8ee02beaa3c4c
contains no database/process/source observations and grants no activation.

Independent handover review closed the one-shot race and final expiry-check
findings. The parent procedure now claims a permanent exclusive attempt under
the global lock, checks expiry inside each bounded lifecycle send, closes every
read-only connection explicitly and binds the exact pre-stop lease. Four
focused guard cases passed; unchanged checks were reused. Previous source and
review evidence are retained. The reviewed handover envelope has SHA
e38508fd5954c719f389664fc8d0a6c337dc5aa2779d2ab145c183d4f894ef21.

At22:20:36 the parent stopped only existing worker88100 through its supported
job. Worker88100 and actual descendant97177 exited, and the lease was observed
absent before one bootstrap of the SAME package and plist. No unrelated process
was terminated. New worker97849 was actually observed at22:20:38.564443UTC;
its unchanged900second qualification deadline is22:35:38.564443UTC. It acquired
lease403104 at22:20:39.558997. Raw five-event handover/result/procedure evidence
is retained in controller-recovery-c5/operator; result SHA
be130d867f4a523b56918e56d6a6831b656e29f6ea1e6dc1532be5061e430edb.
Source5 is not qualified merely because its process and lease are running.

Actual final source-input SHA
a2fb099c7310fc2877ceef9c59014a69cfbe71ac77eba422aa8f26eedf9695a7
preserves136turns/138roots/139audits,164control/165global identities and all
four failed-source clocks. Independent input review passed. Normal preparation
then published manifest7a01582f2a3276901090269112052f24b091823026c4f2d3674307c3cc715ecd,
with the same237-target control allowlist and286-post January cohort.
The P/installed-qualification.json35-page receipt and top-level legacy process
fields remain HISTORICAL PACKAGE PROVENANCE. Current authority can come only
from a new source5 Epochs proof after two stable observations, a real post-
bootstrap owner/dispatch/page, process/reboot/lease checks and both final clocks.
The controller may own those bounded waits before any business helper; no new
page or qualification receipt is fabricated. Final launch review remains pending
at this record point. No automation, watcher, schedule or automatic restart was
created. Full-history remains disabled; January remains uncertified.


### Manual recovery outcome and final bounded verification, 23:05 UTC

January is NOT certified. The maintained certifier returned blocked / false
at the current-target known-gap preflight: 268 posts have count deficits,
with a summed gap of 25,137 comments/replies. Native membership, traversal,
parentage, media lineage, hosted bytes and unchanged-evidence deep gates remain
pending; the preflight made one read-only snapshot, zero source/storage sends
and zero mutations. The complete exact shortcode/count list is retained in
controller-recovery-c6/operator/final-january-certificate.json, SHA
a1bd653d8c3f2564d60c3e8b6827c0f1f6b72f7dcbfd31d888584d13891480fd.
This result does not certify saved scrape passes or any January subset.

The reviewed C5 controller was launched manually once and actually qualified
source5 (PID97849) at22:29:23.402152UTC using two stable observations and a real
new DTiw reservation/dispatch/page. It accepted two additional recovery roots,
DTjkLS2Dn4Q and DTkiCbllTpR. The third target DTkoytpiUe0 had an exact retained
252-page audit with354IDs and zero replay, but its next-root pressure check
stopped the controller before any DTkoy root command. C5 was fully reaped.
All prior167 global journal identities were reconciled;166 are control keys.
Cumulative counts remain141turns/140roots/142audits. The original237-target
control allowlist,286-post cohort,131-turn predecessor chain and original
11:08:15.797480-to23:08:15.797480UTC lifetime were retained without resets.

C6 source preparation fixes received focused checks and independent review:
qualified-business-prefix accounting, complete replacement history and strict
reuse of the exact completed DTkoy audit. Final driver SHA is
411c796dc2a9cf34e6ddb7cdebad1ea88e5f4664bd4120efb4375cab947d117b.
The final route intentionally accepts only the pinned C5 predecessor; it does
not claim future C6 self-reuse. All-source/runtime inputs were reviewed before
normal preparation. Actual live preparation then refused activation with
technical_EpochLost. C6 has no published manifest and was never launched.
A source-reviewed candidate is not an installed or active controller.

The concrete new runtime failure occurred at22:45:13.776450UTC: worker97849
aborted on lease_heartbeat_deadline during scheduler_tick and exited125.
Its recorded heartbeat age was2.178886seconds, with SELECT inflight1.921233s
and watchdog wake gap1.855519s; the previous completed SELECT took0.034194s.
The underlying reason for that late heartbeat is not established by these
measurements. Existing native LaunchAgent KeepAlive restarted the same package
as PID4633 at22:45:15, logging lease acquisition/backend403104 at22:45:16.466226.
This is the same backend identity as the prior qualified owner, so current
server ownership remains unqualified under the retained-lease ambiguity guard.
No database session was killed, no force-unlock occurred, and no new controller
business command was sent after PID loss.

Source6 is charged as replacement slot6 of8. The earliest native observation
22:45:16.466226 sets a conservative900second expiry of23:00:16.466226UTC;
late parent observation at23:00:34 cannot grant a fresh window. The earlier
source6-first-observation file is preserved. An append-only clock correction,
SHA70144a6145ddf9cd41e3dd2af06381e47c3199befc1c4b14ff18a7160461968e,
records the distinct clocks, unchanged cumulative budgets and no qualification.
No further handover or replacement window was invented before original expiry.

At23:04:42 the actual installed worker4633/public environment/source package
were verified, with523 real retained pages since its start and a fresh DTiw
owner48 dispatch/reservation/page. These pages prove activity, not server lease
qualification or January completeness. DTiw's current inventory was4857unique
IDs against5498expected; older84-ID/5203-ID observations remain distinct and
preserved. DTg is unchanged at166/166,owner169,14818observed IDs with all six
failed reply branches and four technical holds retained. No repeated unchanged
1675030/1675004 requests, attempt grant, parser/query switch or unsupported
inventory union was introduced. The two already-supported independent public
parent queries returned identical15-ID roots in the reviewed bounded probe,
which did not demonstrate a query-selection or request-construction defect.

The23:05:05 read-only progress snapshot shows284 saved comment/reply passes,
2 unresolved posts (DTgXh94kXyo and DTiwjjpAYzG),49 saved media passes and237
unresolved media targets. Saved passes do not establish verified completeness.
The exact237-target bounded media-readiness scan reports235
persisted_comment_inventory_changed and2complete_comment_inventory_proof_required
(the two unresolved comment targets), with no eligible target. Receipt SHA
7a3df48528ffef680007a2a5a274968e6eed66a5e6892173398176d1253b7122.
No newly saved media target was established in this repair interval. R2's
previous managed-public403 repair remains reused; domain access is not a gate.
No unchanged canary or hosted-byte check was repeated and pending hosted-byte
proof is not claimed as passed.

The three overshoots remain separate unresolved native-counter comparisons:
DTLbQv6EdE1 expected563/observed571, DTvYo3MkcvI300/317 and
DTysU1Nj5LQ349/382. Their retained inventory audits are correct and their earlier
native counters have provenance. No saved extras were deleted or expectations
lowered. A guarded count-only witness refresh for settled detail targets is not
implemented, so these are not represented as exhausted external-only blockers.

Backend source commit8bafd6d8 contains the reviewed local public-parent diagnostic
and lease-acquisition provenance changes. The actually installed package remains
ig-local-public-parent-fa955e20db59; no C6 source runtime installation is claimed.
Focused diagnostic53checks plus reviewed continuation tests passed; unchanged
checks were reused. All saved rows, native identities, checkpoints, receipts,
attachments, objects, evidence clocks and unrelated dirty work were preserved.
No app build, SQL migration, API/status contract change or cloud deployment was
performed. Retained cloud identity remains v161/ig-reply-diagnostics-8e3350210aa9.

The native worker is intentionally left running. All manual controllers stay
stopped; the deleted automation remains deleted. No schedule, watcher, login
loader or automatic controller restart was created. Original7:08p.m.NewYork
controller expiry is not extended. Public-only/logged-out access and the hard
full-history/resume-tail block remain in force, including after any future
certification. Remaining work is heartbeat/lease replacement qualification,
public reply completion, exact latest-scan/native counts and inventory proof,
three overshoot witnesses, media admission/lineage/bytes and final286-post
certification. The task is not represented as complete.


### Reliability audit and repair candidate — October 25, 2026, 9 p.m. New York

Chat `01a17583-5ddd-7dc0-b32e-3cf2b8886292` owns this coordinated repair.
Baseline backend HEAD was `4712d847`; the running local package was
`ig-local-public-parent-fa955e20db59`, worker 4633, grant backend 403104.
Live provider history confirmed cloud v161 / `ig-reply-diagnostics-8e3350210aa9`
in `admin-56995 / main / trr-backend-jobs`. Existing unrelated dirty work and
branches were preserved. No old controller authorization was revived.

The prioritized findings were: reentrant pooled lease admission and connectivity-only
heartbeats; unbounded startup and a reproduced daemon stdout shutdown crash;
overshoot mislabelled as parity; stale retained-audit reuse after canonical changes;
legacy comments mutations bypassing v2 admission; hidden holds and capability
fallbacks; and incomplete process supervision around auxiliary count witnesses.
The source repairs reject old grants before serving, check the actual grant on
heartbeats, bound startup/shutdown and manually supervised lifetimes, require exact
parity, bind retained audits to full canonical-row fingerprints, fence legacy
mutations, preserve actionable UI state, and add a bounded auxiliary public count
operator. Media inventory equality and final certification were not weakened.

The new worker/witness interruption checks cover timeout, supervisor loss, exact
child cleanup and preserved work. Source checks and independent review were
completed in bounded batches; after two review rounds exposed a common supervision
weakness, the design was simplified to worker-owned deadlines and parent-loss
fencing and verified with focused subprocess regressions. Router runtime-binding
bookkeeping returned HOST_ERROR/thread-not-loaded despite native child results;
no exact model/instruction telemetry claim is made.

Fresh preflight certificate `before-certificate.json` reports a known count gap of
25,382 and final_acceptance=false, using one read-only snapshot and zero source or
storage sends. Deep gates remain pending. Current run counts at baseline were
285 saved comment passes / one unresolved, and 49 saved media passes / 237
unresolved. These are not completeness counts. The three overshoot targets pass
the new operator's read-only binding preview after correcting terminal owner-token
handling. Live witness observations and candidate activation are pending.

App lightweight validation, focused UI regressions and typecheck passed. The full
production build, explicitly approved in this chat, passed with the existing
require-in-the-middle bundling warning. No app deployment is implied.
Shared behavior and optional process settings are documented in
`../../../docs/workspace/instagram-ingestion-reliability.md`. There is no SQL
migration or change of SQL ownership. Runtime follow-through is recorded below.


### Verified reliability release v162 and bounded local run in progress

Backend commit `47249752` contains the reliability repair; app commit `51d9cf98`
contains bounded launch/status behavior and generated contract updates. Workspace
commit `9a71c20` records the shared contract. Maintenance CLI follow-up `49a3bcf9`
repairs Darwin child reaping; that script is run from the maintained checkout and
is outside Modal's mounted ingestion worker surface. No pushes were performed.

Local package `ig-reliability-a40acced8dfb` freezes commit `47249752` with 956
Python file hashes. Old worker 4633 had no live descendants at handover, exited
through its supported LaunchAgent bootout, and two observations showed its grant
absent before replacement. At 01:10:24 UTC on October 26 (9:10 p.m. New York on
October 25), new worker 37673 acquired backend 429802. Its process-local fence
is the existing January run, maximum six activities, two reply pages, an absolute
one-hour controller deadline including preparation, and parent-loss shutdown.
It is manually supervised once; no automatic controller restart was created.
Fresh retained-page audits have committed under the new full-row cache binding.

The first auxiliary count witness completed for DTLbQv6EdE1: native count 563,
one source send, capture 01:09:49 UTC, private artifact
`count-witness-4c71957470f744d79562300ff87a9bd1.json`. It confirms that the
563/571 source/saved comparison remains unresolved. The operation reached its
90-second limit before completing the remaining witnesses. Its independent child
watchdog exited and a subsequent process inventory found no witness child. The
original CLI cleanup reported EPERM/reaped=false. An offline real-Darwin probe
reproduced group-signal EPERM on an exited, unreaped same-UID child; the kernel
reason remains unknown. `49a3bcf9` now attempts both waits despite signal errors
and reports reaping separately from group-signalling uncertainty. Existing
artifacts were preserved; uncertain source attempts were not repeated.

Cloud v162 deployed at 01:14:14 UTC to the verified
`admin-56995 / main / trr-backend-jobs`, tag
`ig-reliability-a40acced8dfb`. Maintained explicit-main readiness and API
health/cold-start canary passed (HTTP 200). Fresh container
`ta-01M5TWDJRMP8G58CYYXTN5K1MR` matched all 668 backend/API Python hashes and
passed overshoot/hold-schema smoke checks without source or database I/O. The
actual deployed comments worker rejected an incomplete reservation before
business I/O in call `fc-01M5TWG92FEM0FVQN5JQTFR3QS`. Its short-lived container
ended before a separate hash read, so no worker-container hash attestation is
claimed. Existing deployment v161 and local package
`ig-local-public-parent-fa955e20db59` remain rollback references. Superseded
local v161 and local-reply-holds copies were moved to actual Trash after
process/dependency checks; current plus rollback are retained.

No full-history/resume-tail release, authenticated fallback, scheduled task, SQL
migration, app production deployment, or hosted-byte completeness claim occurred.
Final bounded-run outcome, certification and native-service restoration follow
below. Private evidence: `~/.codex/tmp/ig-reliability-01a17583/`.


### Consolidated audit findings and repair evidence (current task)

| Priority | Demonstrated behavior and evidence | Repair and validation |
| --- | --- | --- |
| P1 | A pooled backend could already hold the same advisory key; a connectivity-only heartbeat did not prove continued ownership. The recorded heartbeat exit's underlying cause remains unproven. | Reject reentrant admission before acquiring; qualify the actual server PID and exact grant on every heartbeat. Isolated PostgreSQL acquisition/loss/replacement tests and focused local-execution checks pass. Heartbeat deadlines were not increased. |
| P1 | Lease startup could block before normal work, and a real full stdout pipe reproduced an unsafe daemon shutdown. Supervisor loss also exposed unbounded lifetimes. | Bound startup; use a raw diagnostic write with bounded join; enforce independent worker/child deadlines and actual parent identity. Real subprocess shutdown, blocked-output, cancellation and parent-loss regressions pass. |
| P1 | Saved overshoot could be labelled parity, and historical committed receipts could remain candidate-complete. | Require equality and report overshoot separately. Settlement and legacy-receipt regressions pass; previous data and expectations stay intact. |
| P1 | Retained-audit cache keys did not change for later canonical mutations. | Bind complete canonical rows, including tombstones and multiplicity, using server-side hashes. Isolated reconciliation, parent lineage, attachment preservation, transaction rollback and canonical-mutation invalidation tests pass. Current live audits do not manufacture new coverage. |
| P1 | Legacy HTTP mutations and internal catalog/comments launch helpers could bypass configured v2 ownership. Direct caller inventory included cursor CLI, deferred catalog and streaming child launch paths. | Guard both HTTP admission and all three new-run helpers. Nine internal ownership/configuration cases pass; eight prior HTTP cases remain valid. Fresh local and deployed configured-provider smoke rejects before probes or persistence. Execution of already accepted jobs is preserved. |
| P1 | Capability fallback and lifecycle copy could present a saved/resolved pass as complete or permit a legacy new launch; holds were hidden. | Keep capability failures visible, use bounded v2 launches, expose optional execution_hold_reason, prioritize unresolved diagnostics, and describe unavailable outcomes as resolved. Backend/API checks, affected app/hook/proxy checks, contracts, typecheck, quick validation and approved production build pass. |
| P2 | Settled posts had no safe bounded count-refresh operator. Live witness supervision exposed Darwin group-signal EPERM after child exit. | Add auxiliary public-only witnesses with snapshot binding and budgets; retain observations without changing settlements. Thirty-six core cases plus focused deadline/cleanup/process cases pass. Actual reaping now proceeds even when group signalling fails; no uncertain source attempt is automatically repeated. |
| Data gate | Earlier evidence recorded 235 inventory rejections, two incomplete inventories and three count overshoots; those historical counts are not reasserted as current measurements. | Strict media receipt/canonical equality stays enabled. Current certification and final read-only diagnostics below own remaining gap counts. No union of unsupported inventories or forced parity is permitted. |

Final caller-boundary repair is backend commit `62d504ef`. Frozen package
`ig-reliability-62d504ef4744` contains 956 Python hashes. Modal v163 deployed
at 01:50:33 UTC on October 26 to `admin-56995 / main / trr-backend-jobs`.
Maintained explicit-main readiness and the health/cold-start canary passed.
Container `ta-01M5TYEF9QMPN6631V5PHXSKCR` matched all 668 API/backend Python
hashes and all three internal launch guards rejected configured v2 ownership
before business I/O. The smoke changed configuration only inside its short-lived
exec process; provider flags, secrets and January execution placement were not
changed. v162 remains available for provider rollback.


### Final bounded-window result and restoration

The original controller stopped new dispatch at 02:08:48 UTC on October 26
(10:08 p.m. New York on October 25), one hour after its start. Worker 37673
exited with code 125 after the existing 15-second cleanup allowance; total
controller elapsed including cleanup was 3,615.09 seconds. The final abort
reported `signal_requested`, foreground `before_finalize`, and an 84 ms heartbeat
age. This was the authorized deadline stop, not a newly observed heartbeat
failure. The previous worker and descendants were absent and its coordinator
grant was absent before any replacement dispatch. There was no new controller
or automatic extension.

The run completed 32 retained audits over 3,908 pages: 31 matched; DTojfyxDeaq
requires fresh source evidence after 86 retained pages. No native comment IDs
were recovered. Ingestion-target request accounting did not increase during the
window. Saved-pass counts remain 285 committed comments / one unresolved
(DTgXh94kXyo), and 49 committed media / 237 unresolved. Post details remain
285 cached-satisfied plus one committed; listing remains 245 committed pages.
These lifecycle counts do not certify source or hosted-media completeness.

The maintained certifier ran after drain and before the stop-generation change.
`final-certificate.json` certifies the examined generation 1 as **blocked**, with
final_acceptance=false: 269 known deficit posts total 25,382 comments. It used
one read-only snapshot; deep traversal, listing, posts, media, hosted-byte and
unchanged-evidence gates remain pending because of the known source gap.
`remaining-comment-gaps.csv` gives each shortcode, expected/saved counts and gap;
`final-target-diagnostics.json` preserves all target status/count evidence.
Both are under the private evidence directory recorded above. The three legacy
overshoots remain 563/571 (DTLbQv6EdE1), 300/317 (DTvYo3MkcvI) and 349/382
(DTysU1Nj5LQ). Only the first obtained a fresh witness in this window. The other
two witnesses and the five-post live acceptance sequence did not finish; the
window was largely consumed validating invalidated retained-audit caches. No
fresh-source success or media acceptance is inferred from those audits.

To enforce the end of the authorization, the existing fenced cancellation and
hold commands stopped this exact local run at generation 2 with
`bounded_recovery_window_expired`. This is an execution fence, not deletion of
saved work. Its fingerprint of all 1,103 target rows excluding cleared owner
fields stayed identical, including all 156,612 accounted target requests,
checkpoints, outcomes, attempts and waits. All 1,033 immutable receipts remained.
The focused isolated PostgreSQL hold-preservation/idempotency test passed. The
certificate remains explicitly bound to generation 1 before this lifecycle
change. An ordinary worker cannot extend this held run.

The existing native LaunchAgent was restored once at 02:09:33 UTC using package
`ig-reliability-62d504ef4744` / backend `62d504ef4744`, PID 77264. Its unique
grant backend is 429802. The pooler reused the previous numerical backend PID;
that number alone was not qualification. Two absent-grant observations preceded
startup, the new process passed pre-acquisition/reentrant-grant checks and actual
server acquisition, and two subsequent live process/grant observations passed.
The final state check still shows generation 2 cancelled/held, unchanged target
fingerprint and receipts, and the unique live native lease. Native service
RunAtLoad/KeepAlive/Standard/throttle settings are preserved. January root
callbacks and manual process-local recovery limits were removed from normal
service startup; the durable run hold prevents resumed January work.

Current and rollback copies are now exactly two: local current
`ig-reliability-62d504ef4744`, rollback `ig-reliability-a40acced8dfb` (v162).
The superseded `ig-local-public-parent-fa955e20db59` was moved to actual Trash
only after process, cwd, LaunchAgent and symlink dependency checks. Mapping is
`final-rollback-retirement.json`. Provider v162 remains the cloud rollback.

Acceptance: focused backend/API, isolated ownership/reconciliation, witness and
real-process interruption checks passed; 74 API/command checks and 19 affected
retained/cache checks passed, with passing unchanged evidence reused. Initial
combined-run failures were repaired and their affected checks passed; the old
failure log is retained rather than overwritten. Nine final helper-boundary
configuration cases plus the historical overshoot regression passed. App quick
validation (contracts and 79 smoke cases), affected UI/hook/proxy regressions,
typecheck and the explicitly approved full production build passed. No app
production deployment or authenticated browser acceptance was performed; the
available browser profile was not verified as TRR. No schema migration, paid
service, account expansion, login fallback, recurring automation, full-history
or resume-tail release occurred. Relevant software fixes are accepted and
backend-deployed; January data certification remains blocked as stated above.
