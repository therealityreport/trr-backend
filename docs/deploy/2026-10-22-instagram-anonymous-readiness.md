# Anonymous Instagram readiness — October 22, 2026

Status as of 03:05 Eastern: profile-absence handling repaired and deployed; complete anonymous scraping remains unverified and full-history execution remains gated.

## Delivered repair

Maintained change `42c8daa0` prevents a clean missing-profile response (404 or explicit null user) from penalizing the shared anonymous request lane. Independent review found that explicit source failures could also be classified as missing profiles. Repair `097e66e1ba3e593afac78968b4f07b44822e6984` preserves rate-limit/login/error classifications and applies server-requested retry delays even on the first failure.

The release uses the verified v111 deployment manifest plus the reviewed maintained profile modules and this repair. It preserves unrelated deployed behavior and excludes unrelated pending checkout edits. Exact source identity: `ig-public-lane-097e66e1-ab4f2feec7ba`.

## Verification and runtime

- Focused network-lane, identity, and profile checks: 116 passed. Previously passing avatar/media checks were reused because their relevant inputs did not change. Ruff passed.
- Modal: workspace `admin-56995`, environment `main`, app `trr-backend-jobs`, app ID `ap-DkLTRoSvqhbkGO7fHyHxrD`, deployment v112 at 02:58:44 Eastern. Supported deployment wrapper completed its explicit-environment readiness check; API cold-start canary returned HTTP 200 on attempt 1.
- Deployed-container smoke verified ten module hashes, anonymous default, clean-absence/error distinction, and closed full-history gate. It made no Instagram requests or database writes.
- Provider comparison confirmed all 38 function identities/URLs/timeouts and five schedules unchanged.
- Mac LaunchAgent `com.trr.instagram-local-worker` runs the same new release, PID 11220 at verification, with one granted worker advisory lock. A transient bootstrap failure during asynchronous launchd teardown was resolved after service removal completed.
- Read-only checks against Supabase `vwxfvzutyufrkhfgoeaa` confirmed run `c5880efe-25cf-4aeb-999a-efc36a0b9701` remains queued, public/local, generation 5, with the same January 2026 intent, zero listing requests, and unchanged checkpoint. The public:local lane remains at failure generation 14, `public_access_backoff`, until October 22 at 08:16:13 Eastern (12:16:13 UTC).
- No app changes/build or SQL/schema changes were needed. No new scrape or cooldown reset was performed.

## Remaining readiness tasks, in order

1. After the existing 08:16 Eastern cooldown, verify the already queued bounded January listing can retrieve older posts. Previous public listing received 401; successful current-profile access alone does not establish history access. Respect any new source wait.
2. Resolve or explicitly retain the missing-parent-comment limitation on `DesoVBhD027`: comment `18281777497294705` from `christy.mattis` was visible signed in but absent from the anonymous response. The supplied investigation reported four page sizes returning the same 43 IDs and natural exhaustion; a later check again returned the same IDs. This is unresolved source coverage, not proof of a parser defect or completeness.
3. Complete fixed-post parent/reply validation by exact IDs, pagination exhaustion, and unresolved-gap reporting. Counts alone are insufficient.
4. Verify anonymous post details and media end to end, including carousel/video variants, comment attachments/GIFs, hosted assets, and idempotent persistence. Previously verified attachments from separately authorized authenticated work do not satisfy this anonymous gate.
5. Reconcile the bounded January run across listing, details, comments, and media. Keep partial/blocked states truthful; only then consider releasing the full-history gate based on the required acceptance evidence.

## Recovery and evidence

Modal v111 remains the rollback deployment. The previous verified Mac release `f06b63c89b9b-overlay-4bfc6762afa1` and original LaunchAgent plist remain available. The older superseded rollback `f06b63c89b9b-overlay-7867fc631421` was moved to actual Trash after checking process/open-file dependencies, leaving two runnable copies.

Private evidence directory: `/Users/thomashulihan/.codex/tmp/trr-instagram-readiness-01a1625a` contains the source manifest, test receipt, deployment log, deployed smoke, before/after provider state, before/after queued-run state, local-install receipt, original plist, and retirement receipt. No secrets are included in this report.
