# Instagram server waits and January recovery — October 22, 2026

Status at 07:48 UTC: the permanent removal of local Instagram penalty cooldowns is deployed and installed. January ingestion resumed past the obsolete wait but its first anonymous GraphQL POST returned HTTP 401. January is **not complete**. Full-history ingestion remains disabled. The worker/control owner is investigating the anonymous listing contract with the separate offline contract reconstruction owner; no repeated source request is authorized by this report.

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

## January execution and preservation

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
