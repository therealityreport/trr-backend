# Instagram server waits and January recovery — October 22, 2026

Additional repair candidate: source changes and focused checks are complete for bounded local-worker shutdown, residual operational retry dates, and preservation of unseen comments during public count gaps. This candidate is not deployed or installed. The parent owns final acceptance and rollout. Its maintained baseline includes the newer reviewed TikTok commit `7c4df283`; Modal v120 preservation is recorded separately by the parent. The dated v119 observations below remain historical evidence.

Status at 11:13 UTC: the permanent removal of local Instagram penalty cooldowns remains deployed and installed. Modal v119 and the matching Mac release now use the reviewed anonymous ordered-profile adapter. The normal worker is online and the January successor has committed four listing pages and receipts, with 59 counted requests and a fifth page active. There is no blocked reason or retry deadline. January is **not complete**: traversal is still in October and January downstream targets have not been created. Full-history ingestion remains disabled. The earlier HTTP 401 and zero-page state below are preserved historical evidence.

## Policy and source

Backend commit `fa8c4f74c6fe7322d07aa9f0b7bd26d934dd5327` contains the scoped implementation and tests. Workspace policy commit `87e63c620fc4b5eeb03a08ec6f344b518a98bf53` documents server-only waits and removes the two obsolete public cooldown environment settings. Existing authenticated Relay integration and unrelated dirty work were preserved outside these commits.

- Source gate and detail transport ignore legacy mixed-origin `cooldown_until` values. Only actual source Retry-After creates or extends `server_retry_after_until`; later failures cannot shorten an active server deadline.
- Reply-access rejection retains failure counts, lanes, evidence, and finite attempts without a synthesized five-minute/exponential delay. Only a checkpoint marked `retry_source=server_retry_after` can defer replies.
- Legacy posts/comments fetchers use separate server-only deadline files. Comments SQL pacing clamps old inflated reservations while retaining the actual server deadline and ordinary request spacing.
- Selected-account checkpoint and exact identity blockers remain enforced. Instagram auth failure records create no timed penalty; other platforms retain their existing policy.
- The local worker rechecks live scheduling state at most five seconds after a journaled wait. It does not rewrite immutable decisions or bypass future target/source deadlines.
- Request accounting, authenticated rolling request caps, finite attempts, deadlines, ownership, placement, cancellation, and the full-history gate remain intact. The public rolling-window exemption was already present in v112 and was not introduced here. This report does not claim a separate fixed per-run public request-count cap.

## Database

Migration `20261022120000_instagram_server_retry_after.sql` was applied at 07:30:20 UTC and registered in `supabase_migrations.schema_migrations` for verified project `vwxfvzutyufrkhfgoeaa` (`trr-core`). SHA-256: `0137ed5058c1c1816d7c9b6dac832c52f538feba7e404d940dd31921db935ab6`.

It adds nullable `server_retry_after_until` to `social.instagram_detail_request_lanes` and the existing `social.ig_comment_rate_pace`. Old mixed deadlines were not backfilled as server instructions. Both deployed columns were verified. There are no app direct-SQL ownership changes.

## Runtime and API recovery

The combined candidate preserved the latest TikTok and header/catalog releases. Modal v116 deployed the cooldown/GIF change, but its final API canary timed out. Live logs showed active long-lived status streams and the provider reported eight running inputs plus queued requests. The API had no input-concurrency decorator. The bounded repair adds `@modal.concurrent(max_inputs=16, target_inputs=8)` only to the ASGI API function, preserving the eight-container ceiling, worker concurrency, timeout, and database pool limits. This is not a claim that all stream-query latency is fixed.

Modal v117 (`ig-no-local-cooldown-api-e6c04618e3a7`) passed the required `main` readiness command and API canary on its first attempt. The later v118 (`reddit-periods-674862fe`, deployed 07:42:43 UTC) preserved every v117 file outside its five disjoint reviewed Reddit period files. No repair was lost. All 38 function bindings and five cron schedules remained identical to the pre-rollout provider snapshot.

| Verification | Result |
| --- | --- |
| Live backend modules | 59 source hashes matched; public default and full-history gate closed |
| Normal GIF evidence | Exact signed native GIF URL survives selection; credentials and lookalike delivery hosts excluded; zero Instagram requests |
| API repair module | Live `modal_jobs.py` SHA-256 `593d7b4950eb677c54afd29562b33fe3c2f7451523c2c7418519482f0e75f91f` |
| `/health/live` | HTTP 200, 0.218 seconds |
| `/health` | HTTP 200, DB connected, 0.277 seconds |
| Unknown `/readyz` route | Expected HTTP 404, 0.396 seconds |
| `/health/db-pressure` | HTTP 200, `pool_pressure_ok`, 0.170 seconds |
| Mac installation | All 2,645 files matched the adopted v118 manifest; 41 virtual-environment entrypoints relocated |
| Runnable copies | Current `ig-no-local-cooldown-api-e60990617c12`, with v112 `ig-public-lane-097e66e1-ab4f2feec7ba` retained as rollback; obsolete temporary copy moved to actual Trash |

The frozen manifest, rather than a clean shared checkout revision, identifies the complete deployed source. Backend commit fa8c4f74 identifies only this task's owned changes. No TRR-APP build was needed: this change modifies backend source policy and runtime admission, with no app response-contract change.

## Historical January execution through 07:48 UTC

Existing run: `c5880efe-25cf-4aeb-999a-efc36a0b9701`, generation 5, immutable intent hash `12148e35aeae0182696be90e610584e95d0cf67532c24c4320c458bb6cb2afb7`. Scope remains January 1 inclusive through February 1 exclusive, 2026 UTC; anonymous public local execution; details/comments/media selected; one details worker, one comments worker; media followups false.

At 07:45:07 UTC, one fenced reconciliation cleared only `listing/page:root.next_attempt_at` from `2026-10-22T12:16:13.002224+00:00` to null. All other target fields, run fields, counters, checkpoints, and immutable ordinal0 wait decision remained equal. The partition was already due. The old identity lane and failure history were left intact.

The preserved investigation attributes that deadline to HTTP 401 and the old third-step twelve-hour local ladder. Raw response headers were not retained; this is not a claim that their absence was verified. There was no recorded explicit server deadline and the new server-only column remained null. Future explicit server instructions must still be obeyed.

The execution owner independently verified preservation, installed the matching candidate, and ran exactly one production activity under the normal coordinator lease. That activity made two counted requests and returned unresolved `public_access_backoff` with allowlisted diagnostic HTTP 401, method POST, GraphQL endpoint. The owner stopped at the activity boundary. At 07:48:19 UTC:

- Run state remains `running`, complete false, generation 5.
- Listing has one unresolved target, attempts 2, requests 2, retry_at null.
- There are zero discovery pages, comment pages, and receipts; later stages have not run.
- Worker is offline with zero live target owners and zero uncertain dispatches.
- Full-history gate remains closed.

No January coverage or canonical completion is established by the catalog setup, the already verified original GIF repair, or deployment health. Source qualification and January result verification remain open. The same quiet heartbeat follows owner-controlled recovery; it does not blindly retry the rejected request or create a replacement controller.

## Validation and private receipts

Reused unchanged passing evidence: 127 focused core checks; 302 checks from the parent policy/fetcher batch; 233 integration checks. After old-policy assertions were updated, 35 affected integration cases passed; the affected admission SQL batch passed all 44 tests, including active and expired real server waits beside an inflated legacy reservation. The durable wait recovery regression passed two cases. The API concurrency/schedule batch passed three cases. Final scoped Ruff and diff checks passed. Independent reviews closed both original findings and found no remaining issue in the wait-recheck or API recovery batches.

The 37 unrelated job-runner/auto-public test failures reproduced with the unchanged baseline fetcher. Their exact failing-node sets matched; this report does not describe that suite as passing. No broad suite was repeated against unchanged inputs.

Private operational receipts are under `/Users/thomashulihan/.codex/tmp/trr-instagram-no-local-cooldown-01a16023/`: `migration-applied.json`, `january-timer-reconciled.json`, `live-core.json`, `live-gif.json`, `release-api/deploy.log`, `release-api/api-closure.json`, `release-current/manifest.json`, `release-current/preservation.json`, and `release-current/live-modal-delta.json`. The matching adopted source is `/Users/thomashulihan/.codex/tmp/trr-reddit-periods-01a16033/source`.

Worker-owner receipts are under `/Users/thomashulihan/.codex/tmp/trr-instagram-settlement-01a16023/headless-public/`: `integrated-control-before.json`, `first-january-activity-result.json`, and `after-first-january-activity.json`. The first activity result, raw provider rejection, and zero-page outcome remain failure evidence, not successful ingestion.

## Ordered public rollout and verified continuation

The recovery replaced the obsolete listing contract with an explicit new root. It did not reinterpret an old cursor or change the public-mode default. The new frozen family is `instagram_profile_ordered_public`, source contract 2 / adapter 1; details and comments retain their existing public families. This family requires an explicit bounded date window.

Native grid cards establish membership. Separate retained native permalink evidence establishes dates and actual owners; the adapter does not add fabricated timestamps to grid responses. Partial hydration survives a retry without advancing the cursor. Cache reuse requires original retained evidence, matching identities, valid freshness and force policy, and original capture clocks. Out-of-window captures do not write canonical posts or downstream targets.

The traversal is capped at 1,024 committed native pages. The accepted window boundary requires observed January membership followed by three fully dated, monotone pages entirely before January 1, excluding known pins. Missing pin facts remain unknown. Its evidence label is `ordered_window_crossing_heuristic_v1`; it is not certified chronology or provider exhaustion. An access boundary, missing proof, repeated cursor or page cap remains unresolved.

### Source, review and deployment

The maintained-source candidate is `ig-ordered-public-9a093967b23d`, with 2,656 files and file-map SHA-256 `9a093967b23d8597c5586336f429ab157d167ea1a348dac897b121258dc95230`. It records HEAD `d65d1f9f1e8f4b05549e6b4c2b68bd115d967d04`, owned patch `a198846217e039c83cc0bc1e76af883e865ce3238fda06363395d6f7e9dc7a47`, and the complete preserved dirty scope. The full manifest identifies the deployed combined source; a scoped local commit alone does not identify all previously accepted authentication and attachment work.

The candidate preserves all 2,645 prior release paths, with 21 classified changes and 11 declared additions. Four newer maintained runtime files include six reviewed repairs: due sync retry, cookie domain boundaries, Threads session resolution off the event loop, Twitter search error propagation, comment-mirror conflict/savepoint handling, and per-anchor Twitter/YouTube transactions. Eighteen focused checks and Ruff passed. Reversing those six patches from private copies exactly reproduced the prior four runtime files. One read-only metadata query against verified `trr-core` confirmed the existing comment-mirror unique index is valid, ready, and matches the corrected expression and predicate. No schema or data mutation was needed for this adapter rollout.

Independent review found one native-owner proof gap. The fix reads the actual unwrapped native detail node without a request-account fallback, rejects missing or mismatched required owner identities, and preserves actual collaborator owners. Fifteen affected checks passed, including wrapped and missing-owner cases and affected PostgreSQL/cache/worker paths. The fix-only review and narrow recovery follow-up found no remaining issue. Passing checks on unchanged inputs were reused.

Modal **v119**, tag `ig-ordered-public-9a093967b23d`, deployed at 10:44:24 UTC to `admin-56995/main/trr-backend-jobs`, app `ap-DkLTRoSvqhbkGO7fHyHxrD`. The required deployment wrapper completed `verify_modal_readiness.py --json --env main` successfully and the health canary returned 200 on its first attempt. All 38 function bindings and five schedules match the prior provider snapshot; the accepted deployment flags are unchanged.

Actual deployed API image `im-6BznD2aBD4LLE0uSdQSZV8` matched 68 expected module hashes and passed the new source-family, native date/owner, page-cap, server-wait and full-history guards. A fresh API container on the same image passed GIF selection, signed delivery URL retention, credential exclusion and lookalike-host checks. These runtime smokes made zero Instagram requests and zero database writes. Container start times, app membership, API-function health logs and actual image/source hashes bind this proof to v119; no locally built `Image.run` result is claimed as deployed proof.

### Installed worker and successor

The matching Mac release has all 2,656 source files verified and 41 virtual-environment entrypoints relocated. The normal launchd worker started at 11:05:02 UTC, PID 71400, with the exact release as its working directory. The macOS process image resolves to Homebrew `Python.app`; the installed script path, working directory and source hashes establish the release identity. No restart was made for the earlier absolute-argv false negative.

The predecessor `c5880efe-25cf-4aeb-999a-efc36a0b9701` is cancelled at generation 6 and held with `superseded_public_ordered_contract`. Its original intent and non-lease target history remain preserved. The clean successor is `2aef72ed-110c-4af9-b2b3-4a45da170a32`, generation 1, intent hash `10746e1197ea8b5f7272a90c662a2d1ebfd6e36a8a01cda4be1c73be428b9fc4`. Only the source profile differs from the original January choices: network scope, January 1 inclusive to February 1 exclusive UTC, force false, details/comments/media selected, one detail and one comment worker, comment media follow-ups false, and attempt budgets 12/12/3.

The first normal installed-worker activity committed a real listing page with 13 counted requests, one receipt, one discovery page and 12 native detail dates. It stopped at the planned boundary. A committed target retains its owner token as receipt provenance; `local_dispatch_status` treats an exact receipt or settled outcome as settled before dispatch. The normal daemon replayed the journal without refetching the root. No manual clearing or retry was needed.

| Observed time | Verified result |
| --- | --- |
| 11:09:30 UTC | Root unchanged at one attempt / 13 requests; second page committed at one attempt / 14 requests; third page active at one attempt / 12 requests. All target blocked reasons and retry deadlines null. Two committed pages contain 24 native IDs; the newest committed page added 12 with zero overlap. |
| 11:11 UTC | Three committed pages and 36 retained per-card dates were read through `Evidence.read` and the native parser. Dates span October 5 at 17:59:32 UTC through October 22 at 02:00:01 UTC. Total requests 47, with the fourth page active. |
| 11:13:13 UTC | Worker online; four committed listing pages, discovery pages and receipts; 59 requests; fifth target active. No blocked or retry fields. Full-history gate closed. |

The observer's earlier `errors` count grouped all unresolved targets, including healthy active work. It was not an error count: blocked reason, last error code and retry deadline were null. The private read-only observer now separates unresolved activity from settled receipt history; no business code or stored run state was changed for that correction.

January membership has not yet been observed. Details, comments and media targets for January remain zero while listing traverses October. The coverage remains unsealed, `provider_exhausted=false`, and `chronology_certified=false`. These pages prove resumed production execution, not January completion.

### Recovery and remaining verification

The compatible recovery archive and complete manifest are verified. They preserve the four newer runtime repairs and retain the identical reviewed additive evidence sanitizer. Actual `Queries.evidence` and `Evidence.read` checks against synthetic stored SSR/API roots, pin facts and native detail dates passed. The parent must hold and drain the successor before applying recovery. Run rows, frozen intents, cursors and evidence remain preserved; new-family execution and semantic replay/apply stay disabled until the accepted candidate is restored. Raw v118 alone is historical recovery material, not a direct operational rollback.

The obsolete v112 worker was checked for process, working-directory and plist references, then moved to actual Trash. The new installed release and prior e609 release are the only two runnable copies. The verified compatible recovery material remains available separately.

The January owner remains the sole runtime controller while active. The existing quiet 15-minute heartbeat follows the exact successor, coordinates authorized diagnosis and recovery, and continues until actual January canonical posts, comments/replies and selected media are verified. It must not restart the predecessor, duplicate native requests, invent cooldown timers or enable full-history ingestion.

Backend/API rollout verification passed. TRR-APP build: not applicable; the internal source family does not change the app API projection. No direct-SQL ownership changed. No browser was used for this rollout verification.

Current release receipts: `/Users/thomashulihan/.codex/tmp/trr-instagram-ordered-01a16023/release/manifest.json`, `deployment-receipt.json`, `preservation.json`, and `recovery-manifest.json`. Source review, focused check logs, index metadata proof and recovery inspection proof are in the parent task directory. Current runtime receipts are under `/Users/thomashulihan/.codex/tmp/trr-instagram-settlement-01a16023/headless-public/`: `january-ordered-successor.json`, `january-ordered-daemon-start.json`, `after-daemon-start.json`, `daemon-continuation-proof.json`, `verified-date-traversal.json`, `latest-runtime-progress.json`, and `integrated-rollout-status.json`.

## Additional operational repair candidate

The preserved 16:12 UTC native sample places the old worker's main thread in a synchronous business PostgreSQL call (`psycopg2` / `PQexec` / `PQgetResult` / `poll`). It does not identify the exact SQL statement or the original connection failure. The lease uses a separate `psycopg` connection. Its watchdog can set the abort flag while a foreground business query remains blocked. The earlier one-time worker restart recovered that incident without resetting the run or cursor.

The candidate adds an independent local-worker guard before lease entry and keeps it armed through lease and pool cleanup. An actual abort starts a 15-second graceful window; a worker still stuck after that window exits with code 125 so launchd can recover it. Existing synchronous activity abort polling and private process-group reaping remain the primary child cleanup. This is a shutdown bound, not a source cooldown. It does not claim to repair the original network cause or bound a separate business-connection hang while the lease stays healthy.

Operational deadline, missing-policy, generic activity, incomplete-observation, profile, avatar, and legacy-detail failures now retain their error code without inventing a future retry date. A lost probe can wait only until an actual competing probe's future expiry. Media retry parsing accepts actual source delay seconds or a valid future HTTP date, treats zero as immediate, and creates no fallback wait for absent, invalid, or expired values. A valid source delay beyond the timestamp range remains closed at the maximum UTC timestamp. Explicit typed server deadlines and authentication blocks remain intact.

Public native comment exhaustion can retain a positive expected-versus-observed count gap. That observation now preserves unseen saved comments instead of marking them missing. Observed comments still upsert, and the receipt retains `public_native_exhaustion`, expected and observed totals, and the exact count gap. Only zero-gap settlement may reconcile missing rows, under the existing full-observation, topology and original scan-cutoff guards. A scoped receipt with a positive gap does not prove complete January comment coverage.

Focused checks passed:

- Five source-free subprocess cases exercise the actual local-worker entrypoint with fake lease/store dependencies: stuck control work, active private activity reaped before forced exit, blocked pool cleanup, healthy activity longer than 15 seconds, and graceful SIGTERM. Forced-exit cases assert the 15-second bound with a two-second tolerance; normal cleanup disarms the guard only after the pool closes.
- Forty pure-policy and disposable-PostgreSQL cases verify real retry propagation, no fabricated future dates, finite attempt exhaustion, positive-gap preservation of complete saved parent/reply/media/history rows, and unchanged zero-gap and scan-cutoff behavior.
- The targeted existing restart-replay check confirms that a committed target is not dispatched again. Ruff passed for the eight production paths and four affected test files.

Request counts, authenticated request admission and the public exemption, genuine source Retry-After, competing probe and owner expirations, source spacing, finite attempts, cancellation, checkpoints and full-history fences remain unchanged. The diagnostic fixed-validation tranche budget and its existing hint are outside this candidate. No schema, environment or app API contract changed. TRR-APP build: not applicable to this operational backend change.

Candidate baseline, task patch, preservation hashes and check logs are under `/Users/thomashulihan/.codex/tmp/trr-instagram-operational-recovery-01a16023/`. The parent-approved newer v120 preservation record is `/Users/thomashulihan/.codex/tmp/trr-instagram-ordered-01a16023/newer-v120-preservation.json`; the next rollout must preserve that newer maintained release as well as the accepted Instagram repairs. No live source requests, run controls, worker restart, installation, deployment or production database writes were performed to prepare this candidate. January completion and the rollout remain with the parent; full-history stays disabled.
