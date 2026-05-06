# Social Platform Module Checklist

Purpose: keep platform social work local, testable, and compatible with existing
route, job, worker, and persistence contracts.

Use this checklist for TikTok, Instagram, Threads, Twitter/X, Facebook, and
future platform Modules. It describes expected shape only; it does not authorize
schema, route, payload, stage, worker-lane, or persisted-row changes.

## Checklist

- Constants: stage names, worker lanes, env var names, page sizes, retry budgets, and timeout defaults live in the platform Module or an obvious canonical owner. Reuse existing values such as `tiktok_posts_scrapling`, `instagram_posts_scrapling`, and comments lane names instead of inventing aliases.
- Auth/session: cookie and session resolution has one public entrypoint per lane. TikTok posts currently resolves cookies through `posts_scrapling/session.py`; Instagram posts/comments and Threads posts use their lane session Modules.
- Proxy: proxy selection is lane-local and logs safe fingerprints only. Examples include `posts_scrapling/proxy.py` for TikTok, Instagram, and Threads.
- Fetcher: network and pagination behavior lives in a fetcher Module, returns typed/simple result objects, records fetch reasons, and does not persist rows directly.
- Persistence: persistence Modules adapt platform DTOs into existing repository helpers or canonical persistence owners. They must not change table shape or identity rules inside a platform refactor.
- Job handlers: platform `jobs.py` registers claimed-job handlers by platform and stage, and job runners stay focused on orchestration: session, proxy, fetch, persist, progress, finish.
- Lifecycle use: job runners should use the social lifecycle/control-plane Interface for heartbeat, progress, retry backoff, job finish, and run finalization once that Interface is available. Until moved, calls into legacy repository helpers are compatibility debt and should be tracked in the wrapper ledger.
- Scripts: `scripts/socials/<platform>/` scripts stay thin. Shared scrape, smoke, auth, or persistence behavior belongs in importable platform Modules.
- Tests: add or update start, worker-lane, route preview/scrape, fetcher, persistence, and job-runner tests for the touched platform. Extend import guards when a platform stops depending on legacy compatibility paths.
- Comments Contract: do not add or wire a new comments ingestion lane until live evidence proves comment identity, pagination model, persistence target, and operator launch contract. For TikTok, the existing CLI can fetch comments experimentally, but backend persisted comments ingestion remains out of scope until those four items are documented.

## TikTok Posts Current Review Points

- `trr_backend/socials/tiktok/jobs.py` owns the `tiktok_posts_scrapling` claimed-job handler registration.
- `trr_backend/socials/tiktok/posts_scrapling/session.py`, `proxy.py`, `fetcher.py`, `persistence.py`, and `job_runner.py` already form the expected posts lane shape.
- `scripts/socials/tiktok/smoke_posts_scrapling.py` is the operator smoke script; it still imports legacy repository helpers to seed runs/jobs.
- `scripts/socials/tiktok/scrape.py` includes experimental comments export paths. Do not treat that CLI as a backend comments persistence contract.
- Tests named in the approved plan for TikTok posts are `tests/repositories/test_tiktok_posts_scrapling_start.py`, `tests/repositories/test_tiktok_posts_scrapling_worker_lane.py`, `tests/api/routers/test_socials_tiktok_preview.py`, and `tests/api/routers/test_socials_tiktok_scrape.py`.

## TikTok Comments Contract Status

Current status: blocked for ingestion work. The repo has enough code to fetch or export comments experimentally, but it does not yet have a backend persisted TikTok comments lane.

Current evidence:

- `scripts/socials/tiktok/scrape.py` has comments-oriented CLI/export helpers.
- `trr_backend/socials/tiktok/scraper.py` has direct comment API code, but that path is parked behind scraper/CLI behavior rather than a social queue stage.
- `trr_backend/socials/tiktok/posts_scrapling/persistence.py` persists TikTok post rows and the post `comments` count, not individual TikTok comment rows.
- No `tiktok_comments_*` scrape stage, worker lane, route launch contract, or persistence module is registered alongside `tiktok_posts_scrapling`.

Required evidence before implementation:

- Comment identity: stable external comment id, author identity, parent/reply identity, and post association.
- Pagination model: cursor shape, termination reason, retryability, and partial-run semantics.
- Persistence target: table, unique key, upsert behavior, media fields, and how deletes/edits are represented.
- Operator launch contract: route path, payload, worker lane, run status/stage names, progress counters, and cancel behavior.

## TikTok/YouTube Batch-Upsert Decision

Current status for this docs/scripts follow-through pass: deferred, pending main
integration equivalence proof.

TikTok and YouTube post persistence still treats `_upsert_tiktok_post()` and
`_upsert_youtube_video()` as the canonical row-shape contracts. Do not switch
either platform to `_pg_upsert_many` until tests prove equivalence for:

- conflict targets (`video_id` for TikTok posts and YouTube videos)
- optional-column gates from `_platform_posts_has_column(...)`
- assignment payload application from `_apply_assignment_payload(...)`
- `job_id` behavior
- raw-data serialization and datetime coercion
- returned row shape consumed by callers and progress metadata

This note intentionally does not claim batch upsert implementation. Future work
may add platform-local batch adapters only after those parity checks pass.

## Threads, Twitter/X, And Facebook Current Review Points

- Threads has two separate Modules: `trr_backend/socials/threads/posts_scrapling/`
  for the claimed-job lifecycle lane and `trr_backend/socials/threads/posts_catalog/`
  for shared-account catalog orchestration. Do not merge their Interfaces.
- Twitter/X shared-account catalog behavior should live behind
  `trr_backend/socials/twitter/posts_catalog/`. Do not add a Twitter/X worker lane
  unless a future plan proves a route, stage, queue, and persistence contract.
- Facebook shared-account catalog behavior should live behind
  `trr_backend/socials/facebook/posts_catalog/`. Do not add a Facebook worker lane
  unless a future plan proves a route, stage, queue, and persistence contract.
- Remote-auth readiness covers Twitter/X, Facebook, and Threads through
  `scripts/modal/verify_modal_readiness.py --probe-remote-auth <platform>`.
  Probe output must contain safe structure booleans only.
- Catalog metadata golden fixtures live under
  `tests/fixtures/socials/run_metadata/*_catalog_metadata_golden.json`; update
  them when intentionally changing stable retrieval metadata field names.

## Twitter/Facebook/Threads Batch-Upsert Checklist

Current status: deferred for all three platforms.

Do not switch Twitter/X, Facebook, or Threads catalog persistence to batch upsert
until tests prove equivalence for:

- source row payload shape and raw-data serialization
- conflict target and uniqueness behavior
- optional-column gates
- assignment payload application
- `job_id` behavior in legacy non-shared mode
- returned row shape consumed by callers
- progress and retrieval metadata counters

## Review Checklist

- Did this change add a worker lane for Twitter/X or Facebook? If yes, stop and
  require a separate plan with route, stage, queue, and persistence evidence.
- Did this change emit raw cookie or token values? If yes, replace them with
  safe boolean structure flags before merging.
- Did this change leave a compatibility wrapper? If yes, update
  `docs/architecture/social-compatibility-wrapper-ledger.md` with owner Module,
  callers, deletion condition, and validation command.

## Stop Rules

- Stop and re-plan if implementation needs a new table, route payload, scrape stage, worker lane, run status, or metadata key.
- Stop and update the wrapper ledger if a legacy import cannot be removed.
- Stop TikTok comments work unless the Comments Contract evidence is present.
