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

- Direct admin scrape/preview ownership lives in
  `trr_backend/socials/tiktok/direct_scrape.py`. That Module is distinct from
  `posts_scrapling/` and must not import `trr_backend.repositories.social_season_analytics`.
- `trr_backend/socials/tiktok/jobs.py` owns the `tiktok_posts_scrapling` claimed-job handler registration.
- `trr_backend/socials/tiktok/posts_scrapling/session.py`, `proxy.py`, `fetcher.py`, `persistence.py`, and `job_runner.py` already form the expected posts lane shape.
- `scripts/socials/tiktok/smoke_posts_scrapling.py` is the operator smoke script; it still imports legacy repository helpers to seed runs/jobs.
- `scripts/socials/tiktok/scrape.py` includes experimental comments export paths. Do not treat that CLI as a backend comments persistence contract.
- Tests named in the approved plan for TikTok posts are `tests/repositories/test_tiktok_posts_scrapling_start.py`, `tests/repositories/test_tiktok_posts_scrapling_worker_lane.py`, `tests/api/routers/test_socials_tiktok_preview.py`, and `tests/api/routers/test_socials_tiktok_scrape.py`.

## TikTok Module Layout

Current status: the TikTok-specific code that used to live in
`trr_backend/socials/social_season_analytics_impl.py` (76 functions) and the
3,100-line `trr_backend/socials/tiktok/scraper.py` class now lives in owned
Modules under `trr_backend/socials/tiktok/`. This was a pure move: every
monolith function keeps a same-named delegating wrapper, and `TikTokScraper`
keeps its class name, method names, and signatures.

| Module | Owns |
| --- | --- |
| `constants.py` | Stage and worker-lane names, env var names, API URLs, user agent, page sizes, retry and timeout defaults (also re-exported from `http_client.py`; `jobs.py`, `posts_scrapling/fetcher.py`, and the legacy repository import from here). |
| `models.py`, `errors.py` | `TikTokPost`, `TikTokComment`, `TikTokScrapeConfig`; `TikTokHttpRequestError` and the two posts-Scrapling lane errors (re-exported by `scraper.py`, `http_client.py`, and `posts_scrapling/job_runner.py`). |
| `parsing/posts.py`, `parsing/comments.py`, `parsing/profile.py`, `parsing/ytdlp.py`, `parsing/sounds.py` | Mixins for post items, comments, profile snapshots, and yt-dlp metadata; `parser.py` re-exports `extract_profile_preview_sections`; `media_resolver.py` imports the shared yt-dlp cookie finder and payload parser; sounds holds the repository sound helpers. |
| `transports/ytdlp.py`, `transports/web_api.py`, `transports/browser_intercept.py` | The yt-dlp CLI path (including canonical-cookie and proxy arguments), the direct web API path (`_scrape_api`, comments, replies, profile HTML, oembed), and the Playwright intercept path. |
| `scraper.py` | Facade: composes the transport and parsing mixins into `TikTokScraper`, owns `scrape()` routing, HTTP client composition, retrieval metadata, and path health (under 450 lines). |
| `cookies.py` | Cookie resolution, validation, refresh, and the canonical cookie-loader adapter; the three cookie state globals stay in the repository and are read through the provider. |
| `posts/persistence.py`, `posts/ingest.py` | Canonical post upsert, enrichment, anomaly events, sound snapshot persistence, and the `_ingest_tiktok` season stage. |
| `comments/persistence.py`, `comments/coverage.py`, `comments/media_mirror.py` | Comment-tree upsert and enrichment, comment coverage and gap priority reads, comment media mirror enqueue and stage. |
| `shared_catalog/context.py`, `config.py`, `discovery.py`, `scrape.py`, `persistence.py` | Shared-account catalog bootstrap, configuration, cursor-partition discovery, hydration and partitioned scrape, and catalog post upsert. |
| `analytics/week.py`, `filters.py`, `header_stats.py`, `reads.py` | Week detail/summary rows, SQL filter helpers, profile header stats, and the nine `get_tiktok_*` season read models (re-exported by `trr_backend.socials.analytics.read_models`). |
| `alerts.py`, `lane_start.py` | Single-path topology alerts; `start_tiktok_posts_scrapling_scrape` and the synchronous post-detail refresh. |

Rules for this layout:

- Extracted repository rooms copy `trr_backend/socials/instagram/catalog_ingest.py`: a `LateNamespaceProvider` published by the legacy repository's tail loop, `_sync_core_overrides()` at room entry, `_room_callable` for sibling rooms that tests monkeypatch, and `_core.<name>` for repository-owned state. Room names are declared before the provider so a late `adopt_published` still captures the repository wrappers. `parsing/profile.py` is imported with the package, so its two repository rooms use the lighter `instagram/media_mirror.py`-style provider instead.
- The repository keeps only delegating wrappers for TikTok, plus the state and types that cannot move yet: `_tiktok_cookie_*` state, `_SharedTikTokCommentFetchTimeoutError` (subclasses the repository YouTube timeout), `_TIKTOK_SINGLE_PATH_TOPOLOGY_DEFAULT`, `FIELD_UNSET`, `TIKTOK_PARTITION_RETRY_DELAYS_SECONDS`, the two media-mirror job-type constants, `_parse_tiktok_time`, and `_resolve_tiktok_media_for_video_id`.
- `tests/scripts/test_tiktok_import_boundary.py` guards that no module under `trr_backend.socials.tiktok` imports either legacy repository path, in source or transitively at import time, and that every provider-backed leaf exposes `_LOCAL_ROOM_FUNCTIONS`.

## TikTok Audit Follow-ups

- `transports/ytdlp_auth.py` owns cookie/proxy argument construction for the scraper and media resolver. Supplied Netscape files win over canonical cookie maps. Canonical-cookie and proxy settings keep their existing defaults.
- The media resolver accepts an optional keyword-only cookie map. The backend supplies available cookies without starting a login or refresh flow. Temporary files belong to the creating call and use mode `0600`.
- yt-dlp dates use UTC. Sound snapshots use `TIKTOK_USER_AGENT`. Subprocess diagnostics retain reason codes without raw command output.
- Instagram catalog room names must exist before provider adoption. Local builders keep preservation-row batching, and intentional legacy patches retain their compatible call shape.

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

- Direct admin scrape/preview/search ownership lives in platform
  `direct_scrape.py` Modules:
  - `trr_backend/socials/twitter/direct_scrape.py` for Twitter/X search,
    replies, quotes, media mirror flags, retrieval metadata, and standalone
    persistence summary shaping.
  - `trr_backend/socials/facebook/direct_scrape.py` for Facebook page scrape,
    search-posts, preview, scrape-post, comments/shares flags, engagement DTO
    shaping, and auth readiness boundaries.
  - `trr_backend/socials/threads/direct_scrape.py` for Threads direct scrape
    and preview.
- Direct scrape Modules are route-facing platform Interfaces. They may accept
  auth loader callbacks from the router, but they must not import
  `trr_backend.repositories.social_season_analytics`.
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

## YouTube Current Review Points

- Direct admin scrape ownership lives in
  `trr_backend/socials/youtube/direct_scrape.py`.
- Keep `trr_backend/socials/youtube/api_client.py`,
  `trr_backend/socials/youtube/crawlee_adapter.py`, and
  `trr_backend/socials/youtube/media_resolver.py` as distinct platform
  adapters. Do not force a Scrapling-shaped abstraction onto YouTube.
- YouTube comment behavior is preservation-only where already route-visible or
  script-visible. Do not add a new route, payload, scrape stage, worker lane, or
  persistence contract as part of this cleanup.

## Current Direct Scrape Dependency Map

The main integrator owns `api/routers/socials/__init__.py`, this checklist, and
`social-compatibility-wrapper-ledger.md`. Platform workers own only their
platform Module and focused tests.

| Platform | Direct route surface | Platform owner | Separate lanes/wrappers | Focused tests |
| --- | --- | --- | --- | --- |
| TikTok | `/tiktok/scrape`, `/tiktok/preview/{username}` | `trr_backend/socials/tiktok/direct_scrape.py` | `posts_scrapling/` remains the claimed-job posts lane; comments ingestion remains blocked. | `tests/api/routers/test_socials_tiktok_preview.py`, `tests/api/routers/test_socials_tiktok_scrape.py`, `tests/socials/tiktok/` |
| Twitter/X | `/twitter/search`, `/twitter/replies`, `/twitter/quotes` | `trr_backend/socials/twitter/direct_scrape.py` | `posts_catalog/` owns shared-account catalog; no Twitter/X worker lane in this cleanup. | `tests/api/routers/test_socials_twitter_admin_routes.py`, `tests/api/routers/test_twitter_persist_endpoint.py`, `tests/socials/test_twitter_*`, `tests/socials/twitter/` |
| Facebook | `/facebook/scrape`, `/facebook/search-posts`, `/facebook/preview/{page_handle}`, `/facebook/scrape-post` | `trr_backend/socials/facebook/direct_scrape.py` | `posts_catalog/` owns shared-account catalog; no Facebook worker lane in this cleanup. | `tests/api/routers/test_socials_facebook.py`, `tests/socials/test_facebook_*`, `tests/socials/facebook/` |
| Threads | `/threads/scrape`, `/threads/preview/{username}` | `trr_backend/socials/threads/direct_scrape.py` | `posts_catalog/` and `posts_scrapling/` stay separate from direct scrape. | `tests/socials/test_threads_scraper.py`, `tests/socials/threads/` |
| YouTube | `/youtube/scrape` | `trr_backend/socials/youtube/direct_scrape.py` | `posts_catalog/` owns shared-account catalog diagnostics; API/Crawlee/media adapters stay distinct. | `tests/socials/youtube/`, `tests/repositories/test_youtube_catalog_backfill_diagnostics.py` |

## Operator Script Adapter Boundaries

Current status: reusable script behavior has a platform-local owner when the
current call graph proves reuse.

- TikTok reusable diagnostics and posts-Scrapling smoke execution live in
  `trr_backend/socials/tiktok/ops.py`. `scripts/socials/tiktok/scrape.py` and
  `scripts/socials/tiktok/smoke_posts_scrapling.py` remain CLI/output adapters.
- Twitter/X CLI search persistence handoff lives in
  `trr_backend/socials/twitter/ops.py`. The script still owns argument parsing,
  auth loading, preview printing, and CSV/JSON output.
- YouTube ad hoc download directory resolution and `yt-dlp` download execution
  live in `trr_backend/socials/youtube/ops.py`; the scrape script keeps only
  route-visible scrape/comment CLI behavior and output formatting.
- `scripts/socials/run_rhoslc_threads_full_refresh.py` remains a one-off RHOSLC
  remediation workflow. Current `rg` evidence shows no tests or second operator
  path reusing it, so no permanent `threads/ops.py` API was created.
- `scripts/socials/cleanup_youtube_false_positives.py` remains a remediation
  workflow. It was not absorbed into `youtube/ops.py` because current reuse is
  limited to the script itself.

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
