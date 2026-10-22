# TikTok audit follow-ups — 2026-10-22

All five source findings are repaired and deployed. Live posts ingestion remains
limited by TikTok responses: the bounded worker canary failed closed with no
persisted posts. This is not a successful posts-ingestion acceptance result.

## Scope and source

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

## Live canary and remaining limit

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

Successful posts persistence remains unverified because both public listing
paths failed. No new signer, login flow, paid provider, proxy purchase, or
historical repair was added. A later posts canary should run after the existing
public transport works; changing its signing/provider strategy is separate work.

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
