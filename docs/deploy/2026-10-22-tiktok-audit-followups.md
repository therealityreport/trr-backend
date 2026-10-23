# TikTok audit follow-ups — 2026-10-22

All five audit findings and the remaining bounded posts-ingestion blocker are
repaired and deployed. Modal v120 completed a two-page canary and upserted 34
unique canonical posts with zero comment rows, reusing the 18 posts from its
previous one-page canary. Earlier receipts below are retained as historical
evidence.

## Initial audit rollout: v113–v116

### Scope and source

- Branch: `chore/repo-hygiene-2026-09-25`; original continuation baseline:
  `097e66e1ba3e593afac78968b4f07b44822e6984`.
- Preserved Claude's TikTok extraction, 76 legacy wrappers, scraper patch seams,
  and the earlier route-threadpool, HTTP-error, page-cap, and parked-comments fixes.
- Instagram catalog room names now precede provider adoption. Local batch builders
  retain preservation-row routing; deliberate legacy patches remain visible.
- Shared yt-dlp auth construction serves profile listing, scraper metadata/bulk
  paths, and media resolution. Existing Netscape files retain precedence;
  canonical cookies and the opt-in proxy retain their settings. Helper-created
  files use `0600` and are removed in `finally`.
- yt-dlp timestamps use UTC; sound requests use the canonical user agent.
- Corrected the cached-total and public-scraper test fixtures at their current
  contracts, preserving production strategy and public-session safeguards.
- Shared provider registry, schema, SQL ownership, and app contracts are unchanged
  by this task. TRR-APP build: not applicable (backend-only repairs).

The clean candidate used the previous live v112 archive plus 47 scoped owner
paths, with maintained-source reconciliation. Its manifest contains SHA-256
hashes for all 2,636 files. Manifest SHA-256:
`c7ab88d134557f7c0dfa372959f7d7fbe7a180217f0edc4352373784a3a77a38`.
Unrelated unfinished ingestion code was excluded from this deployment.

The scoped source commit retains the TikTok extraction, repairs, fixtures, and
completion ledgers. All 46 non-monolith owner files and the 77 task-owned
monolith functions were checked against the reviewed candidate. Unrelated
Instagram cooldown edits remain outside this task's staged source.

## Verification

| Evidence | Result |
| --- | --- |
| Scraper/resolver, reproduced Instagram failures, provider compatibility, import boundary | 47 passed |
| New audit regression batch | 26 passed; one fixture setup failure corrected, then both batch branches passed |
| Affected TikTok routes, cookies, HTTP, direct scrape, worker/start/lane and control-plane contracts | 213 passed |
| TikTok ingest and sound compatibility selection | 7 passed; unrelated season tests deselected |
| Fresh imports, import boundary and batch checks in the actual release archive | 11 passed |
| Scoped Ruff and formatting | Passed; 55 files passed the formatting check |
| Independent complete candidate review and subsequent header integration review | No blocking findings |

Passing checks were reused on unchanged inputs. The full season-analytics suite
was not repeated. Mocked resolver checks are independent of public DNS while
retaining URL-safety coverage. Live database receipt reads verified the TRR
project binding `vwxfvzutyufrkhfgoeaa` and used read-only transactions.

## Modal and API

- Workspace/environment/app: `admin-56995 / main / trr-backend-jobs`.
- App ID: `ap-DkLTRoSvqhbkGO7fHyHxrD`.
- Deployed through `scripts/modal/deploy_backend.py --env main` as v113,
  tag `tiktok-audit-20261022-62b2cab5`, at 03:21:45 Eastern.
- Other active tasks subsequently deployed v114/v115 header work and v116
  Instagram cooldown work. The canary ran against the combined deployed source.
  All 46 other owner-file hashes and the exact text of all 77 task-owned
  monolith functions matched the reviewed candidate. Monolith SHA-256 observed:
  `971a7309a5d867cbda6ebb370b3326edac956d4a1d180467c3d266db58d5196b`.
- Final explicit-main readiness passed, including deployed control-plane provider
  initialization. API `/health` returned 200.
- All 38 function IDs, dispatch URLs/timeouts, and five schedules matched the
  pre-deployment layout. Repeated post-deployment logs recorded seven completed
  recovery sweeps and thirteen completed executor-heartbeat calls, with no failed
  executions in the captured evidence.

## Initial failed canary: historical

The browser-enabled worker used existing canonical credentials. The posts job
used `max_pages=1`, `ingest_mode=posts_only`; its canonical fallback enforced a
30-post cap. No comments lane or bulk backfill was started.

- Run: `32633f87-a7c4-4586-9567-57a0abbc0cb0`.
- Job: `498c665b-3a02-442c-b8f7-aac0ea31e4a3`.
- Result: `failed`, `non_json_response`, zero pages/posts and zero post/catalog
  upserts. Warmup returned 200. The fetcher records the existing limitation that
  TikTok may require JavaScript-generated API parameters. The canonical yt-dlp
  profile fallback used canonical cookies and the 30-video cap, but exited
  nonzero. The temporary canary worker is confirmed `stopped` in the database.
- An initial canary selected a lean deployed image without Scrapling fetchers;
  that attempt failed closed and its worker stopped. Acceptance was then rerun
  in the verified browser image; the result above is the relevant transport test.
- Media resolution for existing post `7654404977770663182` exercised the changed
  deployed path. yt-dlp used canonical cookies and found one URL; its CDN probe
  returned 403. Watch-page extraction also found a URL whose probe returned 403.
  The existing unofficial-API fallback returned one usable media asset.

At this initial checkpoint, successful posts persistence was unverified because
both listing paths failed. The v120 repair and persisted acceptance below
supersede that limit. The original failed job and zero-write receipt remain
preserved.

## Rollback and evidence

Provider v112 remains the pre-task rollback artifact; the exact v113 candidate
archive and manifest are retained. Provider history also retains v114–v116.
Rolling back the whole app to v112 would remove the later tasks' changes, so a
rollback must select its intended combined release. No rollback was indicated by
the source/readiness/media checks; the posts limitation did not establish a new
regression in the repaired paths.

Durable local evidence and the verified source archive:
`/Users/thomashulihan/.codex/tmp/trr-tiktok-continuation-01a16268/`.
This contains safe runtime receipts, test logs, provider layout, source manifest,
and the exact reviewed v113 source. Raw cookie values, proxy credentials, and
sensitive subprocess output are excluded from the delivered evidence.

## Posts-ingestion repair and acceptance: v120

The live comparison isolated a session and transport boundary. The existing
cookie-seeded browser produced non-JSON listing responses. An anonymous native
browser received real post-list JSON. A read-only trial of current stable yt-dlp
2026.10.21 still failed profile extraction, so the production dependency pin was
retained.

A bounded public browser fallback now runs after empty yt-dlp results, only when
the job has an explicit page budget. It attaches no saved cookies, proxy or
persistent profile, registers response capture before navigation, blocks comment
API requests, and accepts items only when the native author matches the requested
account. It ignores prefetched responses beyond the budget, deduplicates cursors
and video IDs, enforces the post cap, and closes the browser after collection.
No custom signer, paid provider, proxy purchase, login or cookie refresh was added.
Unbounded fallback jobs retain their previous behavior.

The worker persists through the existing canonical adapter. Its progress records
actual native pages and partial coverage when the requested cap is reached.
Required catalog-persistence and cancellation checks now apply consistently to
early fallback exits as well as normal pagination. The independent review's two
findings—an early-return persistence guard and early post-cap retention—were fixed
in one batch. Fix-only review found no remaining blocker.

Validation reused 24 passing affected cases plus two corrected retry-budget
regression cases, for 26 affected scraper/worker cases. Five worker and import
boundary checks passed separately; scoped Ruff and four-file formatting passed.
No full suite or TRR-APP build was needed. No schema or SQL ownership changed.

Release source: `tiktok-public-posts-3d8d96c5e674`, file-map SHA-256
`3d8d96c5e6748fb1703abdba483f1e6b52595fd83926ee0786fe7e66395fd035`.
The exact manifest is authoritative for the complete digest. The archive is based
on verified Modal v119, `ig-ordered-public-9a093967b23d`, and retains all 2,656
base files, including the accepted ordered Instagram adapter. Only four declared
TikTok source/test paths differ or are added; all 2,658 candidate hashes were
verified independently. Other unfinished checkout work remains preserved.

Modal v120 deployed at 2026-10-22 14:07:27 Eastern to
`admin-56995 / main / trr-backend-jobs`, app
`ap-DkLTRoSvqhbkGO7fHyHxrD`. The supported deploy wrapper completed explicit-main
readiness/provider checks and the API health canary returned 200 on its first
attempt. All 38 function bindings, dispatch URLs/timeouts and five schedules
match the pre-deploy snapshot. The deployed canary matched both repaired runtime
module hashes and 12 preserved v119 module hashes before creating its job.

### Successful persisted one-page canary

- Public account: `bravotv`; `max_pages=1`, `ingest_mode=posts_only`.
- Run: `4ea52164-f6eb-492d-874d-fc55cafdae64` — completed.
- Job: `6fa12f04-f419-4531-b218-4547a2da4954` — completed.
- Result: one native listing page, 18 posts found, 18 canonical posts upserted,
  zero skipped posts, and a 30-post cap.
- An independent read-only database connection confirmed 18
  `social.tiktok_posts` rows tied to that exact job, all owned by `bravotv`, all
  with native post times. It found zero `social.tiktok_comments` rows for the job.
- Runtime metadata: `retrieval_mode=public_browser`,
  `auth_mode=public_anonymous`, `pages_scanned=1`, `post_cap=30`,
  `owner_rejections=0`, `stop_reason=max_pages`. The page budget intentionally
  leaves profile coverage partial; job completion does not mean full history.
- The temporary deployed worker processed the real claimed job and is confirmed
  stopped in the database. No permanent worker or schedule configuration changed.

Provider v119 remains the immediate pre-repair rollback artifact. No rollback was
needed. Exact source, manifest, independent review, test logs, source-preservation
proof and safe deployed/database receipts are retained at
`/Users/thomashulihan/.codex/tmp/trr-tiktok-posts-repair-01a16268/`.

## Two-page pagination and deduplication canary

The follow-up canary used the unchanged v120 deployed application in
`admin-56995 / main / trr-backend-jobs`. Fourteen deployed module hashes matched
the accepted release before any job creation. It used `bravotv`, `max_pages=2`,
`ingest_mode=posts_only`, and a 60-post cap.

- Run: `d6f9d1e6-1db3-40eb-8132-dff2896b4c84` — completed.
- Job: `e24b20d7-805e-438c-8716-73d830d69775` — completed.
- Native page 1 returned 18 posts; page 2 returned 16 posts. The first response's
  next-cursor hash exactly matched the second request's cursor hash, and the two
  request cursors differed. Both responses indicated more history; the worker
  stopped at the requested two-page boundary and retained partial coverage.
- The raw pages contained 34 distinct IDs with no overlap. All 34 accepted IDs
  exactly matched the 34 canonical rows read back for the job. No duplicate
  persisted video IDs were found.
- All 18 posts from the earlier one-page canary retained their original canonical
  row UUIDs. They were upserted in place; the second page contributed 16 additional
  IDs to this canary's result. This verifies repeated-run identity reuse. The live
  pages did not contain cross-page duplicates; the existing focused duplicate-ID
  and duplicate-cursor checks remain the evidence for that input case.
- An independent read-only database connection confirmed 34 canonical rows,
  native author `bravotv` and post times on every row, zero comment rows, completed
  job/run status, and the temporary worker stopped.

A passive Python return observer recorded native callback IDs and cursor hashes.
It changed no fetcher, response, or deployed application function. Cursor query
values, cookies, credentials, and response bodies are excluded from the delivered
receipt. No paid provider, proxy purchase, source repair, application deployment,
permanent worker, or schedule change was needed. TRR-APP build and SQL/schema
changes were not applicable. Unchanged passing source checks were reused.

Safe native-page and independent database receipts:
`/Users/thomashulihan/.codex/tmp/trr-tiktok-two-page-01a16268/` —
`deployed-canary-receipt.json` and `final-persistence-receipt.json`.


## October 23 continuation — incomplete acceptance

Current live provider history identified v131, `ig-stall-scale-0422708b505d`,
in `admin-56995 / main / trr-backend-jobs`. Both accepted TikTok runtime module
hashes still matched in the browser container. No application source or deployment
was changed during this continuation; the newer Instagram changes were preserved.

- Local anonymous native-browser five-page checks returned 81 unique `bravotv`
  posts and 80 unique `bravowwhl` posts. Each stopped at `max_pages`, rejected zero
  account owners, and reported one unsuccessful response. These are retrieval
  checks, not database-persistence acceptance.
- A subsequent 25-page `bravotv` check returned zero accepted pages, two failed
  responses, and `public_listing_incomplete`, despite profile HTTP 200. Full-history
  acceptance remains open; a successful profile load is not listing success.
- The remote persisted canary first failed to initialize the database pool.
  A subsequent direct connection succeeded. A retry with a process-local smaller
  pool created run `665a571b-8a72-4615-8099-6023379c32d3` but ended without a job or
  terminal receipt. Readback found zero jobs for that run. The task-owned queued
  run was cancelled and its idle worker marked stopped. Neither remote attempt
  establishes persistence success. The exact cause of interrupted container exec
  remains unverified.
- Media validation for `7699538168709762318` and `7699553575608798478` each found
  one usable asset through the existing unofficial API. Both yt-dlp and watch-page
  URLs failed CDN validation with 403. Hosted-media persistence was not tested.
- Source inspection found that the admin TikTok full-history action uses the
  shared catalog path, whereas the accepted canary uses the posts-only Scrapling
  lane. Admin defaults still contain comments. No admin backfill was launched;
  comments stayed disabled in every executed listing check.

Remaining: repair and verify persisted execution on the current runtime; establish
reliable full-history continuation through the actual catalog entrypoint; exclude
comments from the requested admin workflow and verify launch/progress/cancellation;
verify hosted media persistence; reconcile the older signature-gap recommendation
with the anonymous browser fallback. The older signature-gap file already has
unrelated edits and was preserved. This record does not mark those tasks complete.

Evidence: `/Users/thomashulihan/.codex/tmp/trr-tiktok-finish-01a16977/`, including
`deeper-public.json`, `media-probes.json`, and private remote diagnostic/cleanup
receipts. No paid provider or purchased proxy was added. No schema changes were
made. TRR-APP build was not applicable to this diagnostic continuation.


## October 23 implementation — deployed, live acceptance blocked

Implemented anonymous native-browser listing in the actual shared TikTok catalog path, replacing legacy partition selection for new TikTok catalog jobs. Each page commits catalog/materialized posts before a claim-fenced version-2 checkpoint. Retries preserve new IDs around shifted page boundaries, skip only committed IDs, keep native and saved counts separate, and bind checkpoints to the same run, task settings, account, and dates. Cancellation is checked per post; periodic progress maintains the worker heartbeat. Browser errors, malformed items, owner mismatches, missing frontiers, and expected-total shortfalls remain incomplete.

Comments are disabled in backend catalog execution, API omitted-task defaults, both launchers, and app defaults. Explicit selection returns `TIKTOK_COMMENTS_DISABLED`. Queued catalog configurations cannot re-enable fetching. No paid provider or proxy was added, and no schema changed.

Independent review found four defects in the initial candidate; all were fixed and the reviewer accepted the fixes. Passing checks cover 21 transport/catalog cases, 12 selected repository cases, one API route case, five unchanged posts-lane launcher cases, and eight TikTok app cases. Passing results were reused when relevant source was unchanged. Ruff and diff whitespace checks passed on task changes. No full app production build or frontend deployment was performed.

The reviewed release overlays seven backend files onto the verified v131 archive (`ig-stall-scale-0422708b505d`), preserving every other deployed file, including Instagram. Final deployment is **v133**, tag `tiktok-catalog-e94d19d07ec9`, environment `main`, workspace `admin-56995`, app `trr-backend-jobs`. Source identity is HEAD `fa58bede4cebc0fc925bf4619cb92e2efa3e2538` plus the seven-file overlay in the private manifest. The prior archive/tag is retained for rollback. Intermediate v132 passed API health; final v133 passed Modal function/provider readiness but returned HTTP 503 on health with `reason=session_pool_capacity`.

Live limitations and reconciliation:

- Two local queue launch attempts hit Supabase `EMAXCONNSESSION`: all 15 session slots were occupied. Direct-lane resolution was attempted; that connection failed and its session fallback encountered the same cap. The error occurred before run creation. Independent Supabase readback found **zero runs** with initiator `tiktok_durable_canary_20261023`, so no orphan cleanup is required.
- The existing deployed API returned saved TikTok catalog posts with HTTP 200 before the final cold-start health failure. This proves a read path only, not new-run persistence or full-history coverage. No full-history launch was submitted after health became degraded.
- Supabase read-only inspection confirmed the three selected posts (`7699534476111547662`, `7699538168709762318`, `7699553575608798478`) exist but have empty hosted-media arrays and no media-mirror job. Hosted-media acceptance remains open.
- Connected browser sessions did not identify the required TRR friendly profile. Browser policy blocked the profile-inspection URL. No alternate-profile account operation or security-policy workaround was attempted. Live admin rendering/launch/cancel verification remains open; the eight app behavior tests passed.
- Shared sessions, Instagram processes, pool limits, and provider subscriptions were not changed to manufacture acceptance. The database capacity condition predated the TikTok launches and also affects API health. Runtime acceptance must be repeated after capacity is safely restored.

Remaining order: (1) restore usable database capacity within shared-runtime ownership, then persisted five-page `bravotv` and `bravowwhl` canaries; (2) actual catalog retry/full-history coverage; (3) live admin launch/progress/cancel with the TRR browser identity; (4) mirror and verify the three persisted videos, including repeat-run identity reuse. Source implementation and deployment are delivered; these live acceptance items are not complete.

Evidence directory: `/Users/thomashulihan/.codex/tmp/trr-tiktok-finish-01a16977/`. The release manifest contains exact hashes and rollback identity. Private deployment, readiness, API, and database failure logs are retained there; they are not publication artifacts.
