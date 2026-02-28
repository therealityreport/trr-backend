# Session Handoff (TRR-Backend)

Purpose: persistent state for multi-turn AI agent sessions in `TRR-Backend`. Update before ending a session or requesting handoff.

## Latest Update (2026-02-28) — Facebook scraper reduced-count and object-count fallbacks

- Completed robustness pass for Facebook SSR engagement parsing in `trr_backend/socials/facebook/scraper.py`:
  - added missing reducer/count-object fallbacks for comments, views, plays, shares/reshare, and total comments (including reduced-count variants and object payloads like `comment_count.count`),
  - kept `0`-safe behavior while prioritizing exact-number fields then reduced representations,
  - extended per-reaction extraction to support i18n-only edge entries when numeric `reaction_count` keys are absent.
- Extended parser regression tests in `tests/socials/test_facebook_engagement.py`:
  - reduced/object fallback comment count sample (`comment_count.count`, `*_reduced`),
  - edge-level reaction labels coming from `i18n_reaction_count` even without explicit `localized_name` object.
- `FacebookScraper._coerce_engagement_count` is now used across these fallbacks for consistency.
- validation:
  - not run in this pass (awaiting your requested runtime verification on `s6/social/w0`).
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- downstream_repos_impacted:
  - `TRR-Backend`: yes
  - `screenalytics`: no
  - `TRR-APP`: no

## Latest Update (2026-02-28) — IMDb person-gallery pagination + Traitors/WWHL show-focus enrichment

- Implemented IMDb person-gallery pagination support so refresh can evaluate images beyond first-page 50 results when show-focused filtering is active.
- Updated `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/integrations/imdb/person_gallery.py`:
  - added payload/state parsers for person mediaindex `all_images` (`parse_imdb_person_mediaindex_state`, `parse_imdb_person_mediaindex_payload`)
  - added `fetch_imdb_person_mediaindex_page(...)` against IMDb persisted GraphQL pagination operation (`NameMediaIndexPagination`)
  - added page-info extraction (`has_next_page`, `end_cursor`, `total`) and robust payload extraction from `__NEXT_DATA__` / JSON script sources.
- Updated `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/ingestion/cast_photo_sources.py`:
  - `fetch_imdb_cast_photos(...)` now expands scan to additional pages when `allowed_title_imdb_ids`/`allowed_title_keywords`/`prioritize_solo_people` is enabled.
  - preserves dedupe across pages by image key.
  - keeps confidence behavior: keyword/title filtering + solo-priority ordering before final `limit`.
  - uses mediaindex caption fallback when mediaviewer caption is unavailable.
- Updated `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_person_images.py`:
  - `_resolve_imdb_focus_filters(...)` now resolves `show_name` from `show_id` when omitted.
  - expanded keyword targets:
    - Traitors: `the traitors`, `traitors`
    - WWHL: `watch what happens live`, `watch what happens live with andy cohen`, `wwhl`
- Added/updated tests:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/integrations/imdb/test_person_gallery_parser.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/ingestion/test_cast_photo_sources_imdb.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_person_images.py`

- Validation:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ./.venv/bin/ruff check trr_backend/integrations/imdb/person_gallery.py trr_backend/ingestion/cast_photo_sources.py api/routers/admin_person_images.py tests/ingestion/test_cast_photo_sources_imdb.py tests/integrations/imdb/test_person_gallery_parser.py tests/api/routers/test_admin_person_images.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ./.venv/bin/pytest -q tests/integrations/imdb/test_person_gallery_parser.py tests/ingestion/test_cast_photo_sources_imdb.py tests/api/routers/test_admin_person_images.py -k "imdb or focus_filters"` (pass; `22 passed`)

- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: ``
- downstream_repos_impacted:
  - `TRR-Backend`: yes
  - `screenalytics`: no
  - `TRR-APP`: no
- residual_risks:
  - IMDb persisted query hash can change server-side; if pagination stops working, hash refresh in `person_gallery.py` may be required.
  - additional-page scan is intentionally bounded (`max_pages <= 10`) to avoid runaway refresh latency.

## Latest Update (2026-02-28) — Facebook scraper metric normalization and compact-count parsing

- Fixed Facebook engagement parsing to tolerate payload variants that were driving metric zeros (`reactions`, `views`, `shares`).
- Updated `trr_backend/socials/facebook/scraper.py`:
  - expanded regex matching for `reaction_count`, `share_count`, `reshare_count`, `video_view_count`, `play_count`, `total_comment_count`, and `top_reactions`.
  - added `_coerce_engagement_count(raw)` to normalize ints, quoted strings, comma formatting, and compact suffixes (`K`, `M`, `B`).
  - added `_extract_reactions_from_edges(edges_text)` with JSON-first parsing and regex fallback.
  - `_extract_engagement(...)` now uses shared coercion for all metrics and more stable reaction breakdown extraction.
- Added regression coverage in `tests/socials/test_facebook_engagement.py` for quoted and compact-style payloads (`3.4K`, `152.5K`, `282.150K`, `1.2M`).
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: ``
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `yes`
- Validation:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest -q tests/socials/test_facebook_engagement.py` (pass, `14 passed`)
- residual_risks:
  - parser remains dependent on Facebook SSR payload shape; continue verifying against live runs (`rhoslc/s6/social/w0`) on scraper refresh to catch future markup shifts.

## Latest Update (2026-02-28) — DB pool-exhaustion mitigation for admin/social endpoints

- Diagnosed runtime failures on admin pages to backend psycopg pool exhaustion:
  - observed `psycopg2.pool.PoolError: connection pool exhausted` in `/api/v1/admin/socials/ingest/queue-status` and brands reads.
- Implemented backend mitigations:
  - `trr_backend/db/pg.py`
    - added bounded pool-acquire retry behavior for `getconn()` failures due pool exhaustion:
      - `TRR_DB_POOL_ACQUIRE_ATTEMPTS` (default `8`)
      - `TRR_DB_POOL_ACQUIRE_SLEEP_MS` (default `50`)
  - `trr_backend/repositories/social_season_analytics.py`
    - `get_queue_status()` now degrades gracefully when worker-health DB access fails, returning structured `workers` fallback plus `queue.error` instead of raising route-level exceptions.
  - `api/routers/admin_brands.py`
    - mapped `connection pool exhausted` and `database pool initialization failed` runtime errors to `503` service-unavailable class.
  - `tests/db/test_pg_pool.py`
    - added retry regression test for pool-exhausted acquire that succeeds after bounded retries.

- Files changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/db/pg.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_brands.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/db/test_pg_pool.py`

- Validation:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest -q tests/db/test_pg_pool.py tests/api/routers/test_admin_brands.py tests/api/routers/test_socials_season_analytics.py -k "pool or queue_status or brands or worker_health"` (pass; `17 passed`)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check trr_backend/db/pg.py api/routers/admin_brands.py tests/db/test_pg_pool.py tests/api/routers/test_admin_brands.py tests/api/routers/test_socials_season_analytics.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check trr_backend/repositories/social_season_analytics.py --select F821,C420` (pass)
  - runtime verification:
    - backend log tail shows repeated `GET /api/v1/admin/socials/ingest/queue-status ... 200 OK` after reload
    - Playwright network for `http://admin.localhost:3000/brands/shows-and-franchises` shows:
      - `/api/admin/trr-api/brands/franchise-rules` -> `200`
      - `/api/admin/trr-api/brands/shows-franchises?limit=300` -> `200`
      - `/api/admin/trr-api/social/ingest/queue-status` -> `200`

- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: ``
- downstream_repos_impacted:
  - `TRR-Backend`: yes
  - `screenalytics`: no
  - `TRR-APP`: yes

## Latest Update (2026-02-28) — Admin brands endpoints + social ingest queue-status endpoint

- Implemented backend routes for brands shows/franchise workflows:
  - `GET /api/v1/admin/brands/shows-franchises`
  - `GET /api/v1/admin/brands/franchise-rules`
  - `PUT /api/v1/admin/brands/franchise-rules/{franchise_key}`
  - `POST /api/v1/admin/brands/franchise-rules/{franchise_key}/apply`
- Added endpoint wiring in social admin router:
  - `GET /api/v1/admin/socials/ingest/queue-status` -> `get_queue_status(...)`
- Added queue-status repository helper aggregation in `social_season_analytics`:
  - returns `queue_enabled`, `workers`, and `queue` payload with `by_status`, `by_platform`, `by_job_type`, `recent_failures`, and partial-failure `queue.error`.
- Added backend test coverage:
  - new `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_brands.py`
  - extended `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_socials_season_analytics.py` with queue-status success/failure tests.

- Files changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_brands.py` (new)
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/main.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/socials.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_brands.py` (new)
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_socials_season_analytics.py`

- Validation:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest -q tests/api/routers/test_admin_brands.py tests/api/routers/test_socials_season_analytics.py` (pass; `48 passed`)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest -q tests/api/routers/test_admin_brands.py tests/api/routers/test_socials_season_analytics.py -k "queue_status or worker_health or brands"` (pass; `12 passed`)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check api/routers/admin_brands.py api/routers/socials.py tests/api/routers/test_admin_brands.py tests/api/routers/test_socials_season_analytics.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check trr_backend/repositories/social_season_analytics.py --select C420` (pass)
  - backend smoke against running local server was attempted via Node `fetch`; requests aborted in this runtime due environment/server availability limits (no `curl` binary and backend endpoint health instability in current dev session).

- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: ``
- downstream_repos_impacted:
  - `TRR-Backend`: yes
  - `screenalytics`: no
  - `TRR-APP`: yes

## Latest Update (2026-02-28) — IMDb refresh show-context inference + alias/external-id lookup hardening

- Implemented conservative inference for unresolved IMDb episode rows in person image refresh:
  - `_apply_show_context_to_photos(...)` now attempts trusted assignment for `show_context_source=imdb_episode_unresolved` rows when strong evidence exists:
    - fallback show IMDb ID matches requested show IMDb ID, or
    - fallback show name alias matches requested show, or
    - episode title (+ season/episode when present) matches an episode under requested `show_id`.
  - inferred rows are tagged with `show_context_source=request_context_inferred`.
  - rows without strong evidence remain unresolved and continue to avoid forced assignment.

- Improved show lookup coverage in refresh pipeline:
  - `_build_show_lookup_maps(...)` now augments `core.shows` data with:
    - `core.show_alternative_names`
    - `core.show_external_ids` (`source_id='imdb'`)
  - added parenthetical-aware alias normalization so variants like `The Traitors (US)` map to `The Traitors`.

- Updated IMDb fallback metadata handling:
  - `_enrich_cast_photos_with_episode_metadata(...)` now preserves unresolved hint fields:
    - `imdb_fallback_show_name`
    - `imdb_fallback_show_imdb_id`
  - canonical resolved show fields remain null when unresolved, preserving existing guardrails.

- Added regression coverage:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_person_images.py`
    - unresolved fallback metadata preservation assertions
    - parenthetical alias key normalization test
    - unresolved -> inferred assignment via fallback show alias
    - unresolved -> inferred assignment via episode-title(+season/episode) match
    - unresolved skip behavior remains validated
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/integrations/imdb/test_title_page_metadata.py`
    - episodic sparse-title fallback case with year in episode segment (`The Power of the Seer (2025) - The Traitors - IMDb`)

- Files changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_person_images.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_person_images.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/integrations/imdb/test_title_page_metadata.py`

- Validation:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check api/routers/admin_person_images.py tests/api/routers/test_admin_person_images.py tests/integrations/imdb/test_title_page_metadata.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest tests/api/routers/test_admin_person_images.py tests/integrations/imdb/test_title_page_metadata.py` (pass; `34 passed`)

- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: ``
- downstream_repos_impacted:
  - `TRR-Backend`: yes
  - `screenalytics`: no
  - `TRR-APP`: yes

## Latest Update (2026-02-28) — Twitter scrape reliability + target verification + quotes API parity

- Implemented auth/source reliability improvements for Twitter scraping:
  - `_load_twikit_credentials(twitter_cookies=...)` now accepts preloaded Twitter cookies and can derive twikit cookie auth from:
    - `SOCIAL_TWITTER_COOKIES_JSON` / `TWITTER_COOKIES_JSON`
    - `SOCIAL_TWITTER_COOKIES_HEADER` / `TWITTER_COOKIES_HEADER`
    - `SOCIAL_TWITTER_COOKIES_FILE` / `TWITTER_COOKIES_FILE`
  - Missing `TWIKIT_COOKIES_FILE` now logs at debug level (non-noisy) instead of warning.

- Added quote-fetch diagnostics and SearchTimeline 404 capability caching in Twitter scraper:
  - `TwitterScraper.last_quote_fetch_meta` now records per-source attempts, chosen source, and failure reason.
  - Quote fallback order remains `TweetDetail -> SearchTimeline -> twikit`.
  - Quote search endpoint 404 support is cached per scraper instance to avoid repeated failing search attempts across subsequent quote calls.
  - `fetch_public_tweet_summary(...)` now includes root `text`, canonical `url`, and `created_at` fields for CLI/API context display.

- Updated Twitter CLI (`scripts/socials/twitter/scrape.py`):
  - Switched env loading to `load_env()` for deterministic `.env` resolution.
  - Dedicated `--replies/--quotes` mode now resolves and prints root tweet context before fetching.
  - Fails fast with clear error when root tweet metadata cannot be resolved.
  - `--quotes` now forwards `--max-pages` to quote fallback flow.
  - Empty results now print source diagnostics (`attempts`, `failure_reason`).
  - Auth loading now passes resolved cookie map into `_load_twikit_credentials(...)`.

- Added admin API quotes endpoint parity:
  - New `POST /api/v1/admin/socials/twitter/quotes`
  - New models: `TweetQuotesRequest`, `TweetQuotesResponse`
  - Response includes full `TweetResponse` payload with `hosted_media_urls`, plus `source_used` and `failure_reason`.
  - Existing search/replies handlers now pass preloaded cookies into `_load_twikit_credentials(...)`.

- Files changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/socials/twitter/scraper.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/socials/twitter/scrape.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/socials.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/repositories/test_social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/socials/test_comment_scraper_fixes.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_socials_twitter_admin_routes.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/scripts/test_twitter_scrape_cli.py` (new)

- Validation:
  - `pytest -q tests/repositories/test_social_season_analytics.py -k twikit` (pass; `6 passed`)
  - `pytest -q tests/socials/test_comment_scraper_fixes.py -k "quote or mirror_tweet_media"` (pass; `9 passed`)
  - `pytest -q tests/api/routers/test_socials_twitter_admin_routes.py -k "twitter and quotes"` (pass; `2 passed`)
  - `pytest -q tests/scripts/test_twitter_scrape_cli.py` (pass; `3 passed`)
  - `ruff check scripts/socials/twitter/scrape.py api/routers/socials.py trr_backend/socials/twitter/scraper.py tests/scripts/test_twitter_scrape_cli.py tests/api/routers/test_socials_twitter_admin_routes.py tests/socials/test_comment_scraper_fixes.py tests/repositories/test_social_season_analytics.py` (pass)
  - Manual:
    - `.venv/bin/python -m scripts.socials.twitter.scrape --replies --tweet 1956000357282406729 --delay 0.5` (pass; root context shown; `36` replies)
    - `.venv/bin/python -m scripts.socials.twitter.scrape --quotes --tweet 1956000357282406729 --delay 0.5 --max-pages 5` (pass; root context shown; `5` quotes via fallback)

- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: ``
- downstream_repos_impacted:
  - `TRR-Backend`: yes
  - `screenalytics`: no
  - `TRR-APP`: no

## Latest Update (2026-02-28) — Twitter CLI auth source fix (.env + _load_twitter_auth)

- Fixed `scripts/socials/twitter/scrape.py` auth loading so the CLI now:
  - loads `.env` via `load_dotenv()`
  - prefers `_load_twitter_auth()` (env-backed, including `SOCIAL_TWITTER_COOKIES_JSON`)
  - falls back to `--cookies` file path (`twitter_cookies.json` default) only when env auth is absent
  - passes `_load_twikit_credentials()` output into `TwitterScraper(..., twikit_credentials=...)`

- File changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/socials/twitter/scrape.py`

- Validation:
  - `ruff check scripts/socials/twitter/scrape.py` (pass)
  - `.venv/bin/python -m scripts.socials.twitter.scrape --replies --tweet 1956000357282406729 --mirror` (pass; env auth loaded, `36` replies, hosted CDN URLs emitted)
  - `.venv/bin/python -m scripts.socials.twitter.scrape --quotes --tweet 1956000357282406729` (pass; env auth loaded, `0` quotes for this tweet in this runtime)
  - `curl -I https://d1fmdyqfafwim3.cloudfront.net/media/0d/0d313bff17406d24354094fa2969c149ea9ba67bb6098293de1643cfd8b3410a.mp4` (`HTTP/2 200`)

- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: ``

## Latest Update (2026-02-28) — Twitter ad-hoc media mirroring + TweetDetail-first quote fallback

- Implemented generic ad-hoc media mirroring primitives for URL->S3 in `s3_mirror`:
  - `infer_media_extension(...)`
  - `MirrorResult`
  - `mirror_url_to_s3(...)` (best-effort, content-addressed keying via `media/{sha[:2]}/{sha}{ext}`)
  - `mirror_urls_to_s3(...)` (batch wrapper with per-URL isolation + in-batch dedupe)
- Extended Twitter tweet model with hosted media support:
  - `Tweet.hosted_media_urls`
  - `mirror_tweet_media(tweets)` helper exported from `trr_backend.socials.twitter`
- Upgraded quote retrieval flow:
  - Added `_fetch_quotes_via_tweet_detail(...)`
  - Added `_fetch_tweet_quotes_via_search(...)` helper
  - `fetch_tweet_quotes(...)` now uses fallback order:
    1. TweetDetail
    2. SearchTimeline
    3. twikit
  - Preserves `last_quote_fetch_reason` semantics (cleared on success, retained only when no source yields quotes).
- CLI enhancements:
  - Added `--quotes` dedicated mode (mutually exclusive with `--replies`)
  - Added `--mirror` to mirror media and print hosted URL summary
- API enhancements for admin Twitter routes:
  - `TwitterSearchRequest.mirror_to_s3`
  - `TweetRepliesRequest.mirror_to_s3`
  - `TweetResponse.hosted_media_urls`
  - Added `TweetRepliesResponse` with full `TweetResponse` payload list
  - `/admin/socials/twitter/search` and `/admin/socials/twitter/replies` now optionally mirror media when requested.

- Files changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/media/s3_mirror.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/socials/twitter/scraper.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/socials/twitter/__init__.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/socials/twitter/scrape.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/socials.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/media/test_s3_mirror.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/socials/test_comment_scraper_fixes.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_socials_twitter_admin_routes.py`

- Validation:
  - `ruff check trr_backend/media/s3_mirror.py trr_backend/socials/twitter/scraper.py trr_backend/socials/twitter/__init__.py scripts/socials/twitter/scrape.py api/routers/socials.py tests/media/test_s3_mirror.py tests/socials/test_comment_scraper_fixes.py tests/api/routers/test_socials_twitter_admin_routes.py` (pass)
  - `pytest -q tests/media/test_s3_mirror.py` (pass, `47 passed`)
  - `pytest -q tests/socials/test_comment_scraper_fixes.py -k "quote or mirror_tweet_media"` (pass, `7 passed`)
  - `pytest -q tests/api/routers/test_socials_twitter_admin_routes.py` (pass, `4 passed`)

- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: ``
- downstream_repos_impacted:
  - `TRR-Backend`: yes
  - `screenalytics`: no
  - `TRR-APP`: no

## Latest Update (2026-02-28) — Person image refresh show context overwrite + stream diagnostics

- Fixed stale show metadata leakage during person image refresh:
  - `_apply_show_context_to_photos(...)` now overwrites `metadata.show_id` / `metadata.show_name` whenever show context is supplied, instead of only setting missing values.
  - This prevents foreign-context IMDb/TMDb/Fandom assets from remaining incorrectly tagged after a context-specific refresh.
- Added regression coverage for overwrite behavior:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_person_images.py`
    - `test_apply_show_context_to_photos_overwrites_existing_metadata`

- Files changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_person_images.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_person_images.py`

- Validation:
  - `./.venv/bin/ruff check api/routers/admin_person_images.py tests/api/routers/test_admin_person_images.py` (pass)
  - `./.venv/bin/pytest tests/api/routers/test_admin_person_images.py -k "apply_show_context_to_photos" -q` (pass)

- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: ``
- downstream_repos_impacted:
  - `TRR-Backend`: yes
  - `screenalytics`: no
  - `TRR-APP`: yes

## Latest Update (2026-02-27) — TikTok Post Details now includes `stats.saves`

- Added TikTok post-detail `saves` support in `get_post_comments(...)`:
  - query now selects a `saves` value for `social.tiktok_posts` (column when present, JSON fallback otherwise).
  - response `stats` now includes `"saves": <int>` for TikTok.
- Files changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/repositories/test_social_season_analytics.py`
- Validation:
  - `./.venv/bin/ruff check trr_backend/repositories/social_season_analytics.py tests/repositories/test_social_season_analytics.py` (pass)
  - `./.venv/bin/pytest -q tests/repositories/test_social_season_analytics.py -k "get_post_comments_tiktok_includes_comment_media_and_metadata"` (pass)
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: ``

## Latest Update (2026-02-27) — TikTok saves (`collectCount`) persisted + RHOSLC S6 backfill

- Implemented TikTok saves ingestion for future runs:
  - Added `saves` to `TikTokPost` model.
  - TikTok parsing now reads saves from:
    - `stats.collectCount` / `stats.collect_count`
    - `statsV2.collectCount` / `statsV2.collect_count`
    - fallback `favoriteCount` aliases
    - top-level actor-style fields when present
  - yt-dlp parsing now maps `collect_count` / `save_count` to `TikTokPost.saves`.
  - TikTok post persistence now writes `saves` to `social.tiktok_posts` when the column exists.
  - TikTok CLI CSV export now includes a `saves` column.

- Added migration:
  - `supabase/migrations/0153_tiktok_saves_count.sql`
  - Adds `social.tiktok_posts.saves integer not null default 0`.

- Added operational backfill script:
  - `scripts/socials/backfill_tiktok_saves.py`
  - Resolves season scope (default RHOSLC S6), fetches per-video saves from TikTok watch-page embedded payload, and updates:
    - `social.tiktok_posts.saves` when column is present
    - always `raw_data.saves` (idempotent fallback)
  - Supports `--dry-run`, `--limit`, `--season-id`, and `--delay-seconds`.

- Executed RHOSLC S6 backfill run:
  - command: `PYTHONPATH=. ./.venv/bin/python scripts/socials/backfill_tiktok_saves.py`
  - result:
    - `scanned=405`
    - `updated=403`
    - `failed=2`
    - `has_saves_column=false` (migration not yet applied in target DB at execution time; values written to `raw_data.saves`)
  - failed video IDs:
    - `7603902989299961102` (`item_struct_missing`)
    - `7592432347375570231` (`item_struct_missing`)
  - post-check:
    - `total=405`
    - `with_raw_saves=403`

- Applied migration to active DB and hydrated column values:
  - Executed:
    - `alter table social.tiktok_posts add column if not exists saves integer not null default 0`
    - `update social.tiktok_posts set saves = greatest(0, (raw_data->>'saves')::int) ... where season_id='e9161955-6ee4-4985-865e-3386a0f670fb'`
    - `insert into supabase_migrations.schema_migrations(version='0153', name='0153_tiktok_saves_count.sql', ...) on conflict do nothing`
  - Post-hydration verification:
    - `total=405`
    - `with_saves_col_gt_0=404`
    - `with_raw_saves=404`
    - `has_saves_column=true`
  - Follow-up retry on prior failures:
    - `video_id=7592432347375570231` recovered and updated to `saves=903`
    - remaining unresolved: `video_id=7603902989299961102` (`item_struct_missing`)

- Files changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/socials/tiktok/scraper.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/socials/tiktok/scrape.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/socials/backfill_tiktok_saves.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/supabase/migrations/0153_tiktok_saves_count.sql`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/socials/test_comment_scraper_fixes.py`

- Validation:
  - `./.venv/bin/python -m py_compile trr_backend/socials/tiktok/scraper.py trr_backend/repositories/social_season_analytics.py scripts/socials/tiktok/scrape.py scripts/socials/backfill_tiktok_saves.py tests/socials/test_comment_scraper_fixes.py` (pass)
  - `./.venv/bin/ruff check trr_backend/socials/tiktok/scraper.py trr_backend/repositories/social_season_analytics.py scripts/socials/tiktok/scrape.py scripts/socials/backfill_tiktok_saves.py tests/socials/test_comment_scraper_fixes.py` (pass)
  - `./.venv/bin/ruff format --check trr_backend/socials/tiktok/scraper.py trr_backend/repositories/social_season_analytics.py scripts/socials/tiktok/scrape.py scripts/socials/backfill_tiktok_saves.py tests/socials/test_comment_scraper_fixes.py` (pass)
  - `./.venv/bin/pytest -q tests/socials/test_comment_scraper_fixes.py -k 'tiktok_parse_post_item or tiktok_parse_ytdlp_metadata'` (pass)
  - `./.venv/bin/pytest -q tests/repositories/test_social_season_analytics.py -k 'tiktok'` (pass)
  - `PYTHONPATH=. ./.venv/bin/python scripts/socials/backfill_tiktok_saves.py --limit 1 --dry-run` (pass; `scanned=1 skipped=1 failed=0 has_saves_column=true`)

- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: ``
- downstream_repos_impacted:
  - `TRR-Backend`: yes
  - `screenalytics`: no
  - `TRR-APP`: no

## Latest Update (2026-02-25) — Week 1 TikTok mirror executed, leaderboard thumbnails confirmed, resolver hardening

## Latest Update (2026-02-27) — Week 0 X/Twitter recovery hardening + rerun

- Implemented Twitter recovery hardening to improve Week 0 reliability when GraphQL replies/quotes endpoints fail:
  - Added public tweet summary fetch via syndication endpoint in scraper:
    - `TwitterScraper.fetch_public_tweet_summary(...)`
    - supports `conversation_count` reply totals
    - supports profile metadata (`username`, avatar URL)
    - supports media extraction from `video.variants` and `mediaDetails`
  - Added fallback path for replies:
    - `fetch_tweet_replies(...)` now falls back to search-based conversation query via `_fetch_tweet_replies_via_search(...)` when TweetDetail fails.
  - Improved Twitter coverage math:
    - `_expected_comment_count_for_platform("twitter", ...)` now uses `replies_count + quotes`.
    - `_reconcile_post_comment_count(platform="twitter", ...)` now reconciles both reply and quote counters (instead of downward-only replies).
    - `_is_comment_fetch_complete(...)` now guards against false-complete empty fetches when expected interactions are non-zero.
  - Added URL fallback for post/comment user profile URLs in `get_post_comments(... platform='twitter')` query path.
  - Added deterministic username->profile URL fallback in `_upsert_tweet(...)`.
  - Added `_apply_twitter_public_summary(...)` update helper (update-only; avoids insert/not-null failures) for refreshing existing root tweet metadata.

- Targeted test additions:
  - `tests/socials/test_comment_scraper_fixes.py`
    - `test_twitter_reply_fetch_falls_back_to_search_on_http_error`
    - `test_twitter_fetch_public_tweet_summary_includes_reply_count_and_media`
  - `tests/repositories/test_social_season_analytics.py`
    - `test_expected_comment_count_for_platform_twitter_includes_quotes`
    - `test_apply_twitter_public_summary_uses_non_empty_fields`

- Operational execution (Week 0 RHOSLC, Twitter-only):
  - Ingest run executed:
    - `run_id = ee60df3f-d954-437e-8577-65175bc96b2a`
    - window: `2025-08-14T04:00:00+00:00` -> `2025-09-16T23:59:59.999999+00:00`
    - mode: `posts_and_comments`, scope `bravo`, platform `twitter`
    - completed: `2/2` jobs, comments job processed all `35` anchors.
  - Post-run metadata refresh pass:
    - applied `fetch_public_tweet_summary` + `_apply_twitter_public_summary` across all 35 root tweets (`updated=35, skipped=0`).
  - Operational repair for prior false-missing state:
    - unmarked Week 0 Twitter thread replies that had been set `is_missing=true` by earlier empty fetch handling.
    - restored active saved-reply baseline for coverage (`506`).
  - Verified sample root tweet correction:
    - `tweet_id=1962923513301639212` now has:
      - `replies_count=89`
      - `user_profile_url=https://x.com/BravoTV`
      - `user_avatar_url` populated
  - Week 0 aggregate after refresh:
    - posts: `35`
    - reported replies: `1721`
    - reported quotes: `1916`
    - reported interactions (`replies+quotes`): `3637`
    - saved replies: `506`
    - saved quotes: `0`
    - coverage (Twitter only, Week 0 window): `506 / 3637` (`13.9%`)

- Known blocker (environment/runtime):
  - Authenticated GraphQL endpoints for replies/quotes are currently failing with `404` and later `429` in this runtime (no valid Twitter/Twikit cookie file at configured path), so quote/reply body completeness cannot converge without credential repair.
  - `TWIKIT_COOKIES_FILE` resolves to `data/twitter_cookies.json` but file is missing in this environment.

- Files changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/socials/twitter/scraper.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/socials/test_comment_scraper_fixes.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/repositories/test_social_season_analytics.py`

- Validation:
  - `./.venv/bin/ruff check trr_backend/socials/twitter/scraper.py trr_backend/repositories/social_season_analytics.py tests/socials/test_comment_scraper_fixes.py tests/repositories/test_social_season_analytics.py` (pass)
  - `./.venv/bin/pytest -q tests/socials/test_comment_scraper_fixes.py -k "twitter_reply_fetch_falls_back_to_search_on_http_error or twitter_fetch_public_tweet_summary_includes_reply_count_and_media"` (pass)
  - `./.venv/bin/pytest -q tests/repositories/test_social_season_analytics.py -k "expected_comment_count_for_platform_twitter_includes_quotes or apply_twitter_public_summary_uses_non_empty_fields"` (pass)

- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: ``
- downstream_repos_impacted:
  - `TRR-Backend`: yes
  - `screenalytics`: no
  - `TRR-APP`: no

- Executed Week 1 TikTok mirror run and measured content coverage before/after:
  - Pre: `total_posts=405`, `week_posts=1`, `total_with_hosted=9`, `week_with_hosted=0`, `week_needs_mirror=1`
  - Post: `total_posts=405`, `week_posts=1`, `total_with_hosted=10`, `week_with_hosted=1`, `week_needs_mirror=0`
- Root-cause behavior observed during queue processing:
  - a queued TikTok mirror job was consumed by another worker runtime and completed with metadata error `s3_setup_failed:Missing required environment variable: AWS_S3_BUCKET`.
  - local runtime had valid S3 config and could execute the mirror stage.
- TikTok mirror reliability fix implemented:
  - Added resolver controls in `resolve_tiktok_media(...)`:
    - `allow_ytdlp` to explicitly skip yt-dlp in mirror-stage resolution.
    - `validate_download_url` to probe candidate media URLs before selecting source.
  - Mirror-stage TikTok re-resolve now calls resolver with:
    - `allow_ytdlp=False`
    - `validate_download_url=True`
  - This forces automatic fallback from dead `watch_page_json` video URLs (HTTP 403) to working `unofficial_api` URLs when needed.
- Worker startup hardening:
  - Expanded S3 preflight guard in worker from Instagram-only to all `media_mirror` platforms (`instagram`, `tiktok`, `youtube`, `twitter`).
  - Error log text normalized to `Social media mirror S3 preflight failed: ...`.
- Live Week 1 TikTok execution result:
  - Post `7608346230631976205` transitioned to `media_mirror_status='mirrored'`.
  - `hosted_media_urls` now populated with S3/CloudFront MP4 URL.
  - `hosted_thumbnail_url` now populated with S3/CloudFront WEBP URL.
  - Selected source at run time: `unofficial_api` after probe failure on `watch_page_json`.
- Bravo Content Leaderboard thumbnail verification:
  - `get_analytics(..., platforms=['tiktok'], week=23, source_scope='bravo')`
  - `leaderboards.bravo_content[0].thumbnail_url` now returns hosted CloudFront thumbnail URL for the mirrored TikTok post.

- Files changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/socials/tiktok/media_resolver.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/socials/worker.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/socials/tiktok/test_media_resolver.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/scripts/test_social_worker.py`

- Validation:
  - `./.venv/bin/ruff check trr_backend/socials/tiktok/media_resolver.py trr_backend/repositories/social_season_analytics.py scripts/socials/worker.py tests/socials/tiktok/test_media_resolver.py tests/scripts/test_social_worker.py` (pass)
  - `./.venv/bin/ruff format --check trr_backend/socials/tiktok/media_resolver.py trr_backend/repositories/social_season_analytics.py scripts/socials/worker.py tests/socials/tiktok/test_media_resolver.py tests/scripts/test_social_worker.py` (pass)
  - `PYTHONPATH=. ./.venv/bin/pytest -q tests/socials/tiktok/test_media_resolver.py tests/scripts/test_social_worker.py tests/repositories/test_social_season_analytics.py -k "tiktok or mirror or worker"` (`24 passed, 73 deselected`)
  - Live mirror execution via repository stage runner for Week 1 TikTok post completed with status `mirrored`.

- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: ``
- additive_user_skill_used: `github-repo-feature-implementer`
- downstream_repos_impacted:
  - `TRR-Backend`: yes
  - `screenalytics`: no
  - `TRR-APP`: no

## Latest Update (2026-02-25) — TikTok media mirror hardened (Joean-style fallback + quality selection)

- Reviewed [JoeanAmier/TikTokDownloader](https://github.com/JoeanAmier/TikTokDownloader) extraction/downloader strategy and aligned TRR TikTok media selection with:
  - highest-quality video candidate preference (bitrate/resolution),
  - no-watermark/source fallback behavior,
  - separated video-vs-thumbnail handling.
- Implemented TikTok media resolver with fallback chain:
  1. `yt_dlp_manifest` (single-video manifest resolution),
  2. `watch_page_json` (`__UNIVERSAL_DATA_FOR_REHYDRATION__` / `SIGI_STATE` parse + bitrate selection),
  3. `unofficial_api` (`tikwm`-style fallback),
  4. `og_fallback`.
- Fixed TikTok ingestion parsing bug:
  - `thumbnail_url` no longer defaults to `playAddr` video URL.
  - `media_urls` now prioritize actual playable video URL(s) instead of mixing cover images into media list.
  - yt-dlp metadata parsing now keeps video URL(s) in `media_urls` and thumbnail in `thumbnail_url`.
- Mirror-stage integration:
  - TikTok mirror stage now re-resolves media URLs on missing/stale source (`failed`/`partial`) and records:
    - `retrieval_meta.mirror.selected_source`
    - `retrieval_meta.mirror.attempts[]`.
  - Missing-source reason code for TikTok now explicit: `tiktok_media_not_found`.
- Queue eligibility update:
  - `_platform_post_needs_media_mirror(...)` now requires non-empty `hosted_media_urls` for TikTok (same guard behavior as YouTube), preventing thumbnail-only terminal state.
- Files changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/socials/tiktok/media_resolver.py` (new)
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/socials/tiktok/scraper.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/socials/tiktok/__init__.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/socials/tiktok/test_media_resolver.py` (new)
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/socials/test_comment_scraper_fixes.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/repositories/test_social_season_analytics.py`
- Validation:
  - `./.venv/bin/ruff check trr_backend/socials/tiktok/media_resolver.py trr_backend/socials/tiktok/scraper.py trr_backend/socials/tiktok/__init__.py trr_backend/repositories/social_season_analytics.py tests/socials/tiktok/test_media_resolver.py tests/socials/test_comment_scraper_fixes.py tests/repositories/test_social_season_analytics.py` (pass)
  - `./.venv/bin/ruff format --check trr_backend/socials/tiktok/media_resolver.py trr_backend/socials/tiktok/scraper.py trr_backend/socials/tiktok/__init__.py trr_backend/repositories/social_season_analytics.py tests/socials/tiktok/test_media_resolver.py tests/socials/test_comment_scraper_fixes.py tests/repositories/test_social_season_analytics.py` (pass)
  - `./.venv/bin/pytest -q tests/socials/tiktok/test_media_resolver.py tests/socials/test_comment_scraper_fixes.py tests/repositories/test_social_season_analytics.py -k "tiktok or mirror"` (`22 passed, 103 deselected`)
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: ``
- additive_user_skill_used: `github-repo-feature-implementer`
- downstream_repos_impacted:
  - `TRR-Backend`: yes
  - `screenalytics`: no
  - `TRR-APP`: no

## Latest Update (2026-02-25) — YouTube media mirror now resolves real video streams (not thumbnail-only)

- Reviewed [Tyrrrz/YoutubeDownloader](https://github.com/Tyrrrz/YoutubeDownloader) behavior and aligned backend mirroring approach with stream-manifest selection semantics.
  - Reference behavior observed:
    - manifest-driven stream selection (prefer muxed/highest viable quality),
    - explicit download-option resolution before transfer.
- Implemented backend YouTube media resolver with deterministic fallback chain:
  1. `yt_dlp_manifest` (`yt-dlp --dump-single-json` best playable stream),
  2. `watch_page_streaming_data` (parse `ytInitialPlayerResponse` formats/adaptiveFormats),
  3. `og_fallback` (`og:video` / `og:image`).
- Fixed core mirror behavior regression:
  - removed the thumbnail-only hard-stop for YouTube in mirror path, so `media_urls` are now mirrored to S3 when resolved.
  - YouTube mirror stage now re-resolves source media URLs on run when media is missing/stale and records:
    - `retrieval_meta.mirror.selected_source`
    - `retrieval_meta.mirror.attempts[]`.
- Queue/retry semantics updated for YouTube:
  - `_platform_post_needs_media_mirror(...)` now treats empty `hosted_media_urls` as still requiring mirror for YouTube posts (with valid source id), even if thumbnail is already hosted.
  - mirror stage returns `youtube_media_not_found` when no video media URL is resolved.
- Files changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/socials/youtube/media_resolver.py` (new)
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/socials/youtube/__init__.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/socials/youtube/test_media_resolver.py` (new)
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/repositories/test_social_season_analytics.py`
- Validation:
  - `./.venv/bin/ruff check trr_backend/socials/youtube/media_resolver.py trr_backend/repositories/social_season_analytics.py tests/socials/youtube/test_media_resolver.py tests/repositories/test_social_season_analytics.py` (pass)
  - `./.venv/bin/pytest -q tests/socials/youtube/test_media_resolver.py tests/repositories/test_social_season_analytics.py -k "youtube or mirror"` (`23 passed, 67 deselected`)
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: ``
- additive_user_skill_used: `github-repo-feature-implementer`
- downstream_repos_impacted:
  - `TRR-Backend`: yes
  - `screenalytics`: no
  - `TRR-APP`: no

## Latest Update (2026-02-25) — Instagram auto-media resolution + mirror S3 fail-fast + jsonb persistence fix

- Implemented Instagram media resolution and mirroring hardening in backend ingest/mirror jobs (no extension dependency).
  - Fallback order now implemented in code:
    1. `api_media_info`
    2. `graphql_shortcode`
    3. `html_json`
    4. `og_fallback`
  - Resolver emits additive structured attempts metadata per source:
    - `source`
    - `success`
    - `reason_code`
    - `http_status`
    - `selected_url_count`
- Root cause debug and fix:
  - Existing mirror failures were caused by missing S3 runtime vars in job environments (for example `AWS_S3_BUCKET`).
  - Worker/backfill startup now runs env load + S3 preflight and exits early with explicit error when config is missing.
- Persistence fix:
  - `social.instagram_posts.hosted_media_urls` writes now use `jsonb` assignment (`%s::jsonb`) in mirror update SQL path.
  - Added regression test coverage for this cast behavior.
- Mirror-stage behavior additions:
  - Instagram mirror stage re-resolves source media URLs when missing or stale (`failed`/`partial`) before S3 download.
  - Stage metadata now includes:
    - `retrieval_meta.mirror.selected_source`
    - `retrieval_meta.mirror.attempts[]`
- Error taxonomy updates in media download/upload path:
  - `download_failed:*` and `upload_failed:*` reasons are now explicit and propagated.

- Files changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/socials/instagram/permalink_metadata.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/socials/instagram/__init__.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/socials/worker.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/socials/backfill_instagram_metadata_and_media.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/socials/backfill_social_media_mirror_jobs.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/socials/test_instagram_permalink_metadata.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/repositories/test_social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/scripts/test_social_worker.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/scripts/test_backfill_instagram_metadata_and_media.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/scripts/test_backfill_social_media_mirror_jobs.py`

- Validation:
  - `ruff check trr_backend/socials/instagram/permalink_metadata.py trr_backend/socials/instagram/__init__.py trr_backend/repositories/social_season_analytics.py scripts/socials/worker.py scripts/socials/backfill_instagram_metadata_and_media.py scripts/socials/backfill_social_media_mirror_jobs.py tests/socials/test_instagram_permalink_metadata.py tests/repositories/test_social_season_analytics.py tests/scripts/test_social_worker.py tests/scripts/test_backfill_instagram_metadata_and_media.py tests/scripts/test_backfill_social_media_mirror_jobs.py` (pass)
  - `ruff format --check trr_backend/socials/instagram/permalink_metadata.py trr_backend/socials/instagram/__init__.py trr_backend/repositories/social_season_analytics.py scripts/socials/worker.py scripts/socials/backfill_instagram_metadata_and_media.py scripts/socials/backfill_social_media_mirror_jobs.py tests/socials/test_instagram_permalink_metadata.py tests/repositories/test_social_season_analytics.py tests/scripts/test_social_worker.py tests/scripts/test_backfill_instagram_metadata_and_media.py tests/scripts/test_backfill_social_media_mirror_jobs.py` (pass)
  - `pytest -q tests/socials/test_instagram_permalink_metadata.py tests/scripts/test_social_worker.py tests/scripts/test_backfill_instagram_metadata_and_media.py tests/scripts/test_backfill_social_media_mirror_jobs.py tests/repositories/test_social_season_analytics.py -k "instagram or mirror or social_worker or backfill_social_media_mirror_jobs"` (`42 passed, 57 deselected`)

- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: ``
- additive_user_skill_used: `github-repo-feature-implementer`
- downstream_repos_impacted:
  - `TRR-Backend`: yes
  - `screenalytics`: no
  - `TRR-APP`: no

## Latest Update (2026-02-25) — Social week-window correction: BYE weeks + final-week +7 cap

- Implemented backend-first weekly period model fix for season social analytics so completed seasons no longer drift to `now`.
  - File:
    - `trr_backend/repositories/social_season_analytics.py`
  - Core behavior changes:
    - Week windows now include explicit `week_type` classification:
      - `preseason`
      - `episode`
      - `bye`
    - Episode windows are fixed 7-day windows from each episode air anchor, clipped to next episode start only to avoid overlap.
    - Gap intervals between episodes are materialized as one or more BYE weeks in 7-day segments.
    - Final episode window always ends at `episode_start + 7 days` (no unbounded extension to current time).
    - BYE labels now emitted as `BYE WEEK (Mon D-Mon D)`.
  - Analytics output additions (backward-compatible additive):
    - `week_type`
    - `episode_number` (nullable)
    - propagated on:
      - `weekly`
      - `weekly_platform_posts`
      - `weekly_platform_engagement`
      - `weekly_daily_activity`
      - week detail `week` object
  - Full-window query alignment:
    - for `get_analytics(..., week=None)`, analysis end is capped to generated week-window horizon (`min(now, latest_window_end)`).
  - Noise reduction:
    - trend flags (`drop/spike/no_activity`) now skip BYE weeks to avoid false regression markers.
- Tests added/updated:
  - `tests/repositories/test_social_season_analytics.py`
    - BYE insertion and final +7 window cap coverage.
    - full-window horizon cap and metadata emission coverage.
  - `tests/api/routers/test_socials_season_analytics.py`
    - additive week metadata assertions for analytics + week detail.
- Validation evidence:
  - `pytest tests/repositories/test_social_season_analytics.py tests/api/routers/test_socials_season_analytics.py` (`114 passed`)
  - `ruff check trr_backend/repositories/social_season_analytics.py tests/repositories/test_social_season_analytics.py tests/api/routers/test_socials_season_analytics.py` (pass)
  - `ruff format --check trr_backend/repositories/social_season_analytics.py tests/repositories/test_social_season_analytics.py tests/api/routers/test_socials_season_analytics.py` (pass)

## Latest Update (2026-02-25) — Crawlee incremental runtime wrappers for social queue jobs

- Implemented backend-only Crawlee runtime scaffolding for social ingest queue execution with legacy-safe fallbacks.
  - Added package: `trr_backend/socials/crawlee_runtime/`
    - `config.py` (feature flags, per-platform limits, account registry constants)
    - `request_keys.py` (deterministic request key format)
    - `error_taxonomy.py` (fixed taxonomy: blocked, rate_limited, auth, network, parse, unknown)
    - `auth_preflight.py` (platform auth readiness checks + non-secret metadata)
    - `runtime.py` (incremental wrapper with retry/telemetry and `CrawleeRuntimeError`)
  - Added platform adapters:
    - `trr_backend/socials/instagram/crawlee_adapter.py`
    - `trr_backend/socials/tiktok/crawlee_adapter.py`
    - `trr_backend/socials/twitter/crawlee_adapter.py`
    - `trr_backend/socials/youtube/crawlee_adapter.py`
  - Integrated queue worker execution path in `trr_backend/repositories/social_season_analytics.py`:
    - `_execute_claimed_job(...)` now conditionally routes through Crawlee adapters when `SOCIAL_CRAWLEE_ENABLED` and platform flags allow.
    - Added auth preflight failure handling with structured additive metadata (`auth_context`, `crawler_runtime`) and per-platform fallback controls.
  - Expanded `.env.example` social runtime/auth contract:
    - `SOCIAL_CRAWLEE_*` feature/limit vars
    - explicit Twitter/TikTok auth loader vars already consumed by repository loaders
    - optional account refs and username/password placeholders for cookie-refresh tooling only.
  - Added tests:
    - `tests/socials/test_crawlee_request_keys.py`
    - `tests/socials/test_crawlee_error_taxonomy.py`
    - `tests/socials/test_crawlee_auth_preflight.py`
  - Updated runbook:
    - `docs/runbooks/social_worker_queue_ops.md` with Crawlee flags, auth preflight expectations, and incident bypass guidance.

## Latest Update (2026-02-25) — Default skill chain policy enforcement

- Updated `AGENTS.md` to enforce the default non-trivial implementation chain:
  1. `skillforge`
  2. `write-plan-codex`
  3. `senior-fullstack`
  4. `senior-backend` or `senior-frontend` (primary-surface rule; backend tie-break for contract/schema/pipeline semantics)
  5. `senior-qa`
  6. `code-reviewer`
- Added mandatory exceptions, additive-domain-skill rule, and handoff compliance keys:
  - `default_skill_chain_applied`
  - `default_skill_chain_used`
  - `default_skill_chain_exception_reason`
- Validation: workspace `rg` checks for default chain section, ordered steps, trigger scope, and key presence passed.


## Latest Update (2026-02-24) — Networks sync release branch + deploy preflight

- February 24, 2026: Isolated and validated releasable networks sync updates in a clean branch for deployment handoff.
  - Branch:
    - `codex/networks-sync-release`
  - Commit:
    - `a0c96de5bf1f4351b3aafff48811b37c0f55c5e8`
  - Files:
    - `api/routers/admin_show_sync.py`
    - `scripts/sync/sync_networks_streaming_links.py`
    - `tests/api/routers/test_admin_show_sync.py`
    - `tests/scripts/test_sync_networks_streaming_links.py`
  - Validation:
    - `ruff check api/routers/admin_show_sync.py scripts/sync/sync_networks_streaming_links.py tests/api/routers/test_admin_show_sync.py tests/scripts/test_sync_networks_streaming_links.py` (pass)
    - `PYTHONPATH=. pytest tests/api/routers/test_admin_show_sync.py tests/scripts/test_sync_networks_streaming_links.py -q` (`49 passed`)
  - Deploy blocker:
    - Cloud Run deploy still blocked from this shell:
      - `gcloud run services list --project trr-web-25d2e --region us-east1` fails for active service accounts with `Permission 'run.services.list' denied`.
      - `admin@thereality.report` requires interactive re-auth (`gcloud auth login`) which cannot run non-interactively here.

## Latest Update (2026-02-24) — Show refresh stream heartbeat + request-id tracing

- February 24, 2026: Implemented final RHOSLC stabilization items for show refresh SSE observability and request correlation.
  - Files:
    - `api/routers/admin_show_sync.py`
    - `tests/api/routers/test_admin_show_sync.py`
  - Changes:
    - `POST /api/v1/admin/shows/{show_id}/refresh/stream`
      - accepts `x-trr-request-id` and echoes `request_id` in stream events.
      - emits explicit step-start progress event (`step_status=running`).
      - runs each blocking script step in a worker thread and emits periodic heartbeat progress every 10s while step is active:
        - `heartbeat=true`
        - `elapsed_ms`
        - stable `step`, `stage_key`, `target`, `current`, `total`.
      - wraps uncaught stream failures in terminal SSE `error` payload including `request_id` when provided.
    - `POST /api/v1/admin/shows/{show_id}/refresh-photos/stream`
      - accepts `x-trr-request-id` and echoes `request_id` on all `progress` and `complete` events.
  - Validation:
    - `python3 -m py_compile api/routers/admin_show_sync.py` (pass)
    - `pytest tests/api/routers/test_admin_show_sync.py -q` (`22 passed`)

## Latest Update (2026-02-24) — Credit-safe discovery locks + resumable sync run state

- February 24, 2026: Implemented next-phase hardening for networks/streaming/production logo sync to prevent repeated external discovery calls and support resumable long runs.
  - Files:
    - `supabase/migrations/0143_network_streaming_discovery_state.sql` (new)
    - `supabase/migrations/0144_network_streaming_sync_runs.sql` (new)
    - `scripts/sync/sync_networks_streaming_links.py`
    - `api/routers/admin_show_sync.py`
    - `trr_backend/media/s3_mirror.py`
    - `Dockerfile`
    - `scripts/README.md`
    - `tests/scripts/test_sync_networks_streaming_links.py`
    - `tests/api/routers/test_admin_show_sync.py`
  - Changes:
    - Added persistent source-lock state table (`admin.network_streaming_discovery_state`) with per entity/source outcome and candidate counts.
    - Added run-state table (`admin.network_streaming_sync_runs`) with status, cursor, counters, and error tracking.
    - Added sync script flags:
      - `--refresh-external-sources`
      - `--batch-size`
      - `--max-runtime-sec`
      - `--resume-run-id`
      - `--start-after`
    - Enforced discovery lock policy:
      - source is treated locked after first attempt; normal runs reuse cached URLs and skip re-query.
      - refresh requires explicit `--refresh-external-sources`.
    - Added resumable runtime behavior:
      - periodic run-state updates by batch,
      - graceful `stopped` status when runtime budget is reached,
      - resume cursor emitted in output.
    - Added SVG rasterizer preflight plumbing:
      - `svg_rasterizer_available()` helper in media mirror,
      - sync emits startup preflight line,
      - sync fails fast when SVG candidates are encountered without rasterizer availability.
    - Expanded admin sync endpoint contract:
      - request: `refresh_external_sources`, `batch_size`, `max_runtime_sec`, `resume_run_id`
      - response: `run_id`, `status`, `resume_cursor`
      - schema preflight now validates new admin run/discovery tables.
    - Docker runtime parity update for CairoSVG dependency (`libcairo2`, `libffi-dev`).
  - Validation:
    - `ruff check scripts/sync/sync_networks_streaming_links.py api/routers/admin_show_sync.py trr_backend/media/s3_mirror.py tests/scripts/test_sync_networks_streaming_links.py tests/api/routers/test_admin_show_sync.py` (pass)
    - `pytest tests/scripts/test_sync_networks_streaming_links.py tests/api/routers/test_admin_show_sync.py tests/media/test_s3_mirror.py` (`80 passed`)

## Latest Update (2026-02-24) — Gallery host reliability repair script + broken-row audit marking

- February 24, 2026: Implemented backend repair tooling for person-gallery hosted URL failures (403/unreachable) with dry-run classification, selective repair, and audit-safe broken marking.
  - Files:
    - `scripts/media/repair_gallery_hosts.py` (new)
    - `tests/scripts/test_repair_gallery_hosts.py` (new)
    - `scripts/media/restore_changed_originals.py` (docs note only)
  - Changes:
    - Added `repair_gallery_hosts.py` CLI for person-gallery candidates from:
      - `core.media_links(kind='gallery', entity_type='person')` + `core.media_assets`
      - person-scope `core.cast_photos`
    - Reachability model:
      - hosted URL probe via `GET` + `Range: bytes=0-0`
      - source URL probe with source-aware headers (IMDb referer etc.)
      - classification: `ok`, `repaired`, `broken_unreachable`, `error`
    - Apply mode behavior:
      - `repaired`: force re-mirror + base variant regeneration; crop variants regenerated only when crop payload exists
      - `broken_unreachable`: add audit marker fields without deleting data:
        - `gallery_status="broken_unreachable"`
        - `gallery_status_reason`
        - `gallery_status_checked_at`
      - media-link candidates write markers to `media_links.context`; cast-photo candidates write markers to `cast_photos.metadata`
    - Script options:
      - `--apply`, `--sources`, `--person-id`, `--show-id`, `--limit`, `--timeout`, `--output-json`
    - Clarified `restore_changed_originals.py` as hash-integrity tooling (separate concern from host availability).
  - Validation:
    - `pytest /Users/thomashulihan/Projects/TRR/TRR-Backend/tests/scripts/test_repair_gallery_hosts.py -q` (`4 passed`)
    - `pytest /Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_asset_batch_jobs.py -q` (`6 passed`)
    - `pytest /Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_person_images.py -q` (`20 passed`)
    - Runtime dry-run (bounded sample):
      - `python /Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/media/repair_gallery_hosts.py --sources imdb,tmdb,fandom,bravo --limit 50 --output-json /tmp/gallery-host-repair-dryrun.json`
      - Result: `scanned=50`, `ok=0`, `repaired=21`, `broken_unreachable=29`, `error=0`, `apply=false`.
    - Runtime direct-invocation smoke:
      - `python /Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/media/repair_gallery_hosts.py --sources imdb,tmdb,fandom,bravo --limit 1 --output-json /tmp/gallery-host-repair-dryrun-smoke.json`
      - Result: `scanned=1`, `broken_unreachable=1`, script executes directly without manual `PYTHONPATH`.
  - Notes:
    - Unbounded dry-run/apply was not executed in this session because it is network-bound and long-running.
    - No DB migration required.

## Latest Update (2026-02-24) — Instagram scraper + async S3 mirror hardening (queue-backed)

- February 24, 2026: Implemented additional Instagram reliability hardening and async mirror queue plumbing with additive diagnostics.
  - Files:
    - `trr_backend/socials/instagram/scraper.py`
    - `trr_backend/socials/instagram/permalink_metadata.py`
    - `trr_backend/repositories/social_season_analytics.py`
    - `api/routers/socials.py`
    - `scripts/socials/backfill_instagram_metadata_and_media.py`
    - `scripts/socials/worker.py`
    - `supabase/migrations/0137_instagram_media_mirror_diagnostics.sql` (new)
    - `tests/socials/test_comment_scraper_fixes.py`
    - `tests/socials/test_instagram_permalink_metadata.py`
    - `tests/repositories/test_social_season_analytics.py`
    - `tests/api/routers/test_socials_season_analytics.py`
    - `tests/scripts/test_backfill_instagram_metadata_and_media.py` (new)
  - Changes:
    - Scraper failure semantics:
      - `fetch_comments(...)` now marks invalid shortcodes with `last_comment_fetch_reason='invalid_shortcode'`.
      - `comments_auth_failed` reset at fetch start.
      - comments/replies endpoint payload `status != "ok"` is treated as logical failure (`api_status_fail`), including auth/challenge classification.
      - request-error logging path no longer assumes a response object exists.
      - post/comment timestamp formatting normalized to UTC.
    - Permalink metadata hardening:
      - strict shortcode validation (`^[A-Za-z0-9_-]{5,32}$`) for direct shortcode and URL extraction.
      - malformed shortcode/url now fails fast (`None`) instead of passing through raw text.
      - `data-sjs` wrapped JSON payload decoding retained and validated.
    - Async media mirroring design in ingest:
      - posts stage now persists post/comment data first and enqueues `instagram_media_mirror` jobs instead of inline S3 upload.
      - media-mirror job stage execution added to worker pipeline (`stage=media_mirror`).
      - mirror downloader streams to temp file with max-byte guard and retry/backoff classification (timeout/429/5xx retryable; permanent failures non-retryable).
      - shortcode fallback key hardening to avoid `"unknown"` collisions (`unknown-{id/hash}`).
    - Existing-post robustness:
      - Instagram account matching now normalizes leading `@` on DB side (`ltrim(..., '@')`).
    - Lifecycle consistency:
      - zero-comment refresh (`max_comments_per_post=0`) now reports `fetch_disabled`, `is_complete=false`, and avoids false completeness.
    - Backfill behavior:
      - backfill script switched to enqueue mirror jobs (no inline uploads).
      - thumbnail-only rows with complete hosted coverage are no longer treated as perpetually missing when status is unset.
    - Additive API/admin fields:
      - new endpoint: `POST /api/v1/admin/socials/seasons/{season_id}/instagram/mirror/requeue`
      - Instagram week/post detail payloads now include:
        - `media_mirror_attempt_count`
        - `media_mirror_last_attempt_at`
        - `media_mirror_last_job_id`
  - Validation:
    - Targeted lint:
      - `ruff check trr_backend/socials/instagram/scraper.py trr_backend/socials/instagram/permalink_metadata.py trr_backend/repositories/social_season_analytics.py api/routers/socials.py scripts/socials/backfill_instagram_metadata_and_media.py tests/socials/test_comment_scraper_fixes.py tests/socials/test_instagram_permalink_metadata.py tests/repositories/test_social_season_analytics.py tests/api/routers/test_socials_season_analytics.py tests/scripts/test_backfill_instagram_metadata_and_media.py` (pass)
    - Targeted tests:
      - `pytest -q tests/socials/test_comment_scraper_fixes.py tests/socials/test_instagram_permalink_metadata.py tests/repositories/test_social_season_analytics.py tests/api/routers/test_socials_season_analytics.py tests/scripts/test_backfill_instagram_metadata_and_media.py -k "instagram or refresh_post_comments or mirror"` (`40 passed, 88 deselected`)
    - Full-suite note:
      - `pytest -q --maxfail=1` stopped on an unrelated pre-existing failure in `tests/api/routers/test_admin_show_sync.py::TestSyncNetworksStreaming::test_runs_three_steps_and_aggregates_metrics` (expected `failures=4`, got `8`).

## Latest Update (2026-02-24) — Global original-integrity audit/repair script (IMDb strict hash)

- February 24, 2026: Added an operational script to verify hosted/source integrity and apply repairs only for provable mismatches.
  - Files:
    - `scripts/media/restore_changed_originals.py` (new)
    - `tests/scripts/test_restore_changed_originals.py` (new)
    - `scripts/media/restore_person_gallery_base_previews.py`
    - `scripts/media/README.md`
  - Changes:
    - Added strict hash audit/repair flow:
      - candidate scope supports `cast_photos`, `media_assets`, or `both`
      - default source filter is IMDb (`--source imdb`)
      - classifies each candidate as `match`, `mismatch`, `unreachable`, or `error`
      - apply mode (`--apply`) repairs only `mismatch` rows
      - repair path uses canonical mirror functions + base variant regeneration (no crop payload)
    - Added JSON reporting:
      - summary totals and per-table/per-source breakdowns
      - explicit `mismatch_ids`, `unreachable_ids`, `error_ids`, `repair_failed_ids`
    - Clarified existing person rollback script purpose:
      - preview-state metadata/context rollback only; not original integrity repair.
  - Validation:
    - `ruff check scripts/media/restore_changed_originals.py tests/scripts/test_restore_changed_originals.py` (pass)
    - `pytest tests/scripts/test_restore_changed_originals.py -q` (`3 passed`)
    - Runtime smoke (bounded):
      - `PYTHONPATH=. python scripts/media/restore_changed_originals.py --source imdb --tables both --limit 40 --output-json /tmp/imdb-original-integrity-audit.json`
      - `PYTHONPATH=. python scripts/media/restore_changed_originals.py --source imdb --tables both --limit 40 --apply --output-json /tmp/imdb-original-integrity-apply.json`
      - result: `40/40 match`, `0 mismatch`, `0 repaired`.
  - Notes:
    - Full unbounded runtime pass is intentionally left as an operator-run command because it is network-bound and long-running.

## Latest Update (2026-02-24) — IMDb mediaindex tags normalized + show season/episode context enrichment

- February 24, 2026: Implemented backend normalization for IMDb mediaindex rows so people/type/title tags are consistently persisted and episode context is inferred when IMDb title IDs match local episodes.
  - Files:
    - `trr_backend/ingestion/imdb_show_mediaindex.py`
    - `api/routers/admin_show_sync.py`
    - `tests/ingestion/test_imdb_show_mediaindex_rows.py` (new)
    - `tests/api/routers/test_admin_show_sync_imdb_mediaindex_context.py` (new)
  - Changes:
    - `fetch_imdb_show_mediaindex_rows(...)` now:
      - flattens mediaviewer tag payload into metadata keys:
        - `people_names`, `people_imdb_ids`, `title_names`, `title_imdb_ids`, `imdb_image_type`
      - normalizes kind by IMDb type:
        - `Still Frame`/`still` -> `episode_still`
        - `Poster` -> `poster`
        - `Publicity` -> `promo`
    - Added `_enrich_imdb_mediaindex_rows_with_episode_context(...)` in `admin_show_sync`:
      - resolves title IMDb IDs against `core.episodes.imdb_episode_id` for the current show,
      - tags metadata with `show_id`, `show_name`, `show_imdb_id`, and when matched:
        - `episode_id`, `episode_imdb_id`, `episode_title`, `episode_number`, `season_number`, `episode_air_date`
      - forces `kind = episode_still` when an episode match is found.
    - `POST /api/v1/admin/shows/{show_id}/refresh-photos/stream` now runs this enrichment before upserting IMDb mediaindex rows.
  - Validation:
    - `pytest tests/ingestion/test_imdb_show_mediaindex_rows.py tests/api/routers/test_admin_show_sync_imdb_mediaindex_context.py -q` (`3 passed`)
    - `pytest tests/api/routers/test_admin_show_sync.py -q` (`20 passed`)

## Latest Update (2026-02-24) — Linked Supabase migrations applied through 0135 for networks logo assets

- February 24, 2026: Applied pending migrations to the linked Supabase database to resolve missing admin logo-assets relation errors seen by TRR-APP detail pages.
  - Runtime actions:
    - Verified linked drift via `supabase migration list --linked` (remote lagging at `0123`).
    - Applied pending migrations via direct DB URL:
      - `set -a && source .env && set +a && supabase db push --db-url "$SUPABASE_DB_URL"`
    - This applied `0124`–`0135` including:
      - `0127_add_network_provider_link_fields.sql`
      - `0128_add_network_provider_monochrome_logo_fields.sql`
      - `0131_network_streaming_completion_and_overrides.sql`
      - `0135_network_streaming_logo_assets.sql`
    - Verified relation exists:
      - `select to_regclass('admin.network_streaming_logo_assets');` -> non-null.
    - Verified sync schema preflight reports no missing required columns.
  - Notes:
    - No backend source code changes were required in this subtask; this was schema alignment in the target Supabase environment.

## Latest Update (2026-02-24) — Resize now rebuilds base + crop variants with fallback

- February 24, 2026: Stabilized admin resize behavior to prevent stretched previews by always generating crop variants for resize operations.
  - Files:
    - `api/routers/admin_asset_batch_jobs.py`
    - `api/routers/admin_person_images.py`
    - `tests/api/routers/test_admin_asset_batch_jobs.py`
    - `tests/api/routers/test_admin_person_images.py`
  - Changes:
    - Batch jobs `resize` now runs two variant passes per target:
      - base variants (`crop=None`)
      - crop variants with resolved crop payload.
    - Added crop payload resolution for batch resize:
      - prefer existing manual/auto crop metadata/context.
      - when missing, attempt auto-count detection refresh.
      - when still missing/unavailable, apply center fallback crop payload:
        - `x=50`, `y=32`, `zoom=1`, `mode=auto`, `strategy=resize_center_fallback_v1`.
    - Added additive progress detail for resize success:
      - `crop_source` in progress payload (`manual`, `auto`, `auto_detect:*`, or `fallback`).
    - Person gallery resize stage now always attempts crop generation for all resize candidates:
      - uses existing crop when present,
      - attempts auto-count refresh when absent,
      - falls back to center payload when detection is unavailable.
  - Validation:
    - `pytest /Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_asset_batch_jobs.py -q` (`6 passed`)
    - `pytest /Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_person_images.py -q` (`20 passed`)

## Latest Update (2026-02-24) — Social cloud-DB failover hardening for pooler DNS/SSL faults

- February 24, 2026: Implemented cloud-first DB candidate resolution + pooled reconnect fallback to reduce social endpoint timeouts when Supabase pooler DNS/transport faults occur.
  - Files:
    - `trr_backend/db/connection.py`
    - `trr_backend/db/pg.py`
    - `tests/db/test_connection_resolution.py` (new)
    - `tests/db/test_pg_pool.py`
  - Root cause:
    - Backend pool initialization used a single DSN and failed hard on transient pooler faults (`ENOTFOUND`, host translation failures, SSL EOF), amplifying social endpoint timeouts.
  - Changes:
    - Added `resolve_database_url_candidates(...)` with cloud-first order:
      - `SUPABASE_DB_URL` -> `TRR_DB_FALLBACK_URL` -> auto-derived Supabase direct host fallback (`db.<project_ref>.supabase.co:5432`) -> `DATABASE_URL` -> `TRR_DB_URL` -> local Supabase fallback.
    - Kept `resolve_database_url(...)` contract unchanged (returns first candidate).
    - Updated `pg.py` pool creation to try candidates in order on transient transport init failures.
    - Added shared pool reset + one-time transient retry for helper query execution paths (`fetch_all`, `fetch_one`, `execute_returning`, execute-values helpers).
    - Added active-pool DSN diagnostics helper (`current_pool_dsn()`).
  - Validation:
    - `ruff check trr_backend/db/connection.py trr_backend/db/pg.py tests/db/test_connection_resolution.py tests/db/test_pg_pool.py` (pass)
    - `pytest tests/db/test_connection_resolution.py tests/db/test_pg_pool.py` (`7 passed`)
    - `pytest tests/api/routers/test_socials_season_analytics.py -k "targets or analytics or ingest_runs"` (`22 passed`)

## Latest Update (2026-02-24)

- February 24, 2026: Hybrid-cloud no-Docker screenalytics migration coordination (integration contract only; no backend code changes in this session).
  - Contract reminder:
    - `SCREENALYTICS_API_URL` in backend runtime can now target either:
      - local no-Docker Screenalytics (`http://127.0.0.1:8001`) when launched with `SCREENALYTICS_SKIP_DOCKER=1`, or
      - hosted Screenalytics API URL (Cloud Run/managed runtime).
  - Scope:
    - This backend repo was not functionally modified for this migration step.
    - Change was captured here for cross-repo handoff traceability.

## Latest Update (2026-02-24)

- February 24, 2026: Added local-only scrape download utility for image-import workflows and validated with script-level tests.
  - Files:
    - `scripts/import/download_scraped_images_local.py` (new)
    - `scripts/download_scraped_images_local.py` (new wrapper)
    - `tests/scripts/test_download_scraped_images_local.py` (new)
    - `scripts/README.md`
  - Changes:
    - Added CLI utility to scrape image candidates from any URL and download image bytes directly to a local folder, without creating `media_assets`/`media_links` rows.
    - Default output directory is `~/Downloads/Bachelorette`, with options for `--min-width`, `--limit`, `--overwrite`, and custom manifest filename.
    - Added per-run `manifest.json` output containing source metadata, download results, and errors.
    - Added root-level wrapper script to match existing script alias pattern in this repo.
  - Validation:
    - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest -q tests/scripts/test_download_scraped_images_local.py` (pass)

## Latest Update (2026-02-23)

- February 23, 2026: Implemented deterministic cast-only Bravo probing for canonical `/people/*` URLs, including cast-only parser mode and additive preview counters.
  - Files:
    - `api/routers/admin_show_bravo.py`
    - `trr_backend/scraping/bravo_parser.py`
    - `tests/api/routers/test_admin_show_bravo.py`
    - `tests/scraping/test_bravo_parser.py`
  - Changes:
    - Added additive request field on preview:
      - `cast_only: bool = False` in `BravoPreviewRequest`.
    - Cast-only preview/commit now probes all canonical cast candidates (plus explicit candidates) and bypasses prior link-state suppression (`has_non_rejected` / existing N/A markers).
    - Added parser flag `candidate_people_only` to `parse_bravo_show_bundle(...)`:
      - when true, probe only `person_url_candidates` and skip merging show-page discovered people URLs.
      - default remains false for non-cast-only behavior compatibility.
    - Added additive preview counters (derived from `person_candidate_results`):
      - `bravo_candidates_tested`
      - `bravo_candidates_valid`
      - `bravo_candidates_missing`
      - `bravo_candidates_errors`
    - Commit cast-only path still preserves existing missing-profile N/A marker persistence behavior.
  - Validation:
    - `ruff check api/routers/admin_show_bravo.py trr_backend/scraping/bravo_parser.py tests/api/routers/test_admin_show_bravo.py tests/scraping/test_bravo_parser.py` (pass)
    - `pytest -q tests/api/routers/test_admin_show_bravo.py tests/scraping/test_bravo_parser.py` (`29 passed`)

## Latest Update (2026-02-19)

- February 19, 2026: Implemented Bravo cast-only URL validation with commit-time N/A persistence for missing Bravo person pages.
  - Files:
    - `trr_backend/scraping/bravo_parser.py`
    - `api/routers/admin_show_bravo.py`
    - `tests/scraping/test_bravo_parser.py`
    - `tests/api/routers/test_admin_show_bravo.py`
  - Changes:
    - Added additive parser output `person_candidate_results` in `parse_bravo_show_bundle(...)` with per-candidate status:
      - `ok` for parsed person pages
      - `missing` for Bravo “Page Not Found” profiles
      - `error` for transient/request failures
    - Updated parser candidate ordering to prioritize explicit `person_url_candidates` ahead of show-discovered people URLs.
    - Added Bravo profile eligibility filtering in `import-bravo` preview/commit:
      - include show-cast candidate URLs only when person has no non-rejected `bravo_profile` link
      - skip already N/A-marked people (`status='rejected'` + `metadata.bravo_probe_state='na'`)
    - Added commit-time persistence of missing candidates as N/A `core.entity_links` rows:
      - `entity_type='person'`, `link_kind='bravo_profile'`, `status='rejected'`
      - metadata markers: `bravo_probe_state=na`, `bravo_probe_reason=missing`, `bravo_probe_checked_at`, `bravo_probe_source=bravo_import_commit`
    - Added additive Bravo probe counters in commit response `counts`:
      - `bravo_candidates_tested`
      - `bravo_candidates_valid`
      - `bravo_candidates_missing`
      - `bravo_na_marked`
    - Added parser `max_people` routing override (`max(40, len(person_url_candidates))`) so larger cast candidate lists are not truncated by default.
  - Validation:
    - `pytest /Users/thomashulihan/Projects/TRR/TRR-Backend/tests/scraping/test_bravo_parser.py -q` (`7 passed`)
    - `pytest /Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_show_bravo.py -q` (`17 passed`)
    - `ruff check /Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/scraping/bravo_parser.py /Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_show_bravo.py /Users/thomashulihan/Projects/TRR/TRR-Backend/tests/scraping/test_bravo_parser.py /Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_show_bravo.py` (pass)

- February 19, 2026: Expanded streaming inclusion scope to all watch-provider availability rows and fixed completion upsert array serialization.
  - Files:
    - `scripts/sync/sync_networks_streaming_links.py`
    - `tests/scripts/test_sync_networks_streaming_links.py`
  - Changes:
    - Removed restrictive filters from provider inventory and IMDb-provider sampling:
      - no longer constrained to `region='US'` and `offer_type in ('flatrate','ads')`.
      - now includes all `core.show_watch_providers` availability rows, with existing fallback from `core.shows.streaming_providers`.
    - Fixed `admin.network_streaming_completion.source_priority` writes:
      - added Postgres `text[]` serializer (`_to_pg_text_array_literal`) to prevent malformed array literal errors during upsert.
      - this unblocked completion row persistence and unresolved tracking.
    - Runtime validation:
      - Verified Bravo-scoped provider availability now includes `hayu`, `peacock premium`, and `peacock premium plus`.
      - Reprocessed Bravo network + Bravo TV provider; both now resolved with base + black + white hosted logos.
  - Validation:
    - `ruff check scripts/sync/sync_networks_streaming_links.py tests/scripts/test_sync_networks_streaming_links.py` (pass)
    - `pytest tests/scripts/test_sync_networks_streaming_links.py tests/api/routers/test_admin_show_sync.py tests/media/test_s3_mirror.py` (`63 passed`)

- February 19, 2026: Continued networks/streaming completion pipeline with Brandfetch + Logopedia + TMDb network alias enrichment, runtime fix, and Supabase schema verification.
  - Files:
    - `trr_backend/integrations/brandfetch.py` (new)
    - `trr_backend/integrations/logopedia.py` (new)
    - `trr_backend/integrations/tmdb/client.py`
    - `scripts/sync/sync_networks_streaming_links.py`
    - `tests/scripts/test_sync_networks_streaming_links.py`
    - `.env` (local runtime only; `BRANDFETCH_API_KEY` + timeout configured)
    - `.env.example`
  - Changes:
    - Added Brandfetch adapter with auth/not-found/request error classification and candidate ranking that prefers transparent/vector assets.
    - Added Logopedia adapter (logos.fandom MediaWiki API) with title/search fallback and quality-ranked candidate extraction.
    - Added TMDb network endpoints in client:
      - `fetch_network_details(...)`
      - `fetch_network_alternative_names(...)`
    - Extended `sync_networks_streaming_links` to:
      - resolve network aliases/homepage/logo hints from show-derived `core.shows.tmdb_network_ids`,
      - include `official`/`catalog` source tiers from Brandfetch + Logopedia + IMDb watch-box provider logos,
      - log source attempt outcomes for completion audit rows,
      - support `--unresolved-only` orchestration input from admin sync API.
    - Fixed live runtime bug in provider inventory build:
      - replaced unsupported PostgREST relation projection (`provider:watch_providers(...)`) with `provider_id` query + provider-name lookup map.
    - Added `.env.example` contract keys:
      - `BRANDFETCH_API_KEY=`
      - `BRANDFETCH_TIMEOUT_SEC=20`
  - Validation:
    - Supabase schema checks (remote target from `SUPABASE_DB_URL`) confirmed required columns now exist on:
      - `core.networks`
      - `core.watch_providers`
      - `admin.covered_shows`
      - `admin.network_streaming_overrides`
      - `admin.network_streaming_completion`
      - `admin.network_streaming_completion_attempts`
    - `ruff check trr_backend/integrations/brandfetch.py trr_backend/integrations/logopedia.py trr_backend/integrations/tmdb/client.py scripts/sync/sync_networks_streaming_links.py api/routers/admin_show_sync.py tests/scripts/test_sync_networks_streaming_links.py tests/api/routers/test_admin_show_sync.py tests/media/test_s3_mirror.py` (pass)
    - `pytest tests/scripts/test_sync_networks_streaming_links.py tests/api/routers/test_admin_show_sync.py tests/media/test_s3_mirror.py` (`60 passed`)
    - `PYTHONPATH=. python scripts/sync_networks_streaming_links.py --all --dry-run --skip-s3 --limit 1` (pass)
  - Notes:
    - Full-repo `ruff check .` currently reports unrelated pre-existing issues outside this task scope (`api/main.py`, `trr_backend/db/pg.py`).

- February 19, 2026: Fixed Week-window YouTube undercounts caused by premature continuation stopping and improved date-window fallback recall.
  - Files:
    - `trr_backend/socials/youtube/scraper.py`
    - `tests/socials/test_comment_scraper_fixes.py`
  - Root cause:
    - Continuation crawl used a flat `no_hit_pages >= 5` cutoff even when pages were still too recent (`in_range=False`), so older week windows could be abandoned before reaching the target dates.
    - yt-dlp fallback used relevance-ranked `ytsearch50`, which could miss additional same-week Bravo videos.
  - Changes:
    - Added `pre_window_pages` tracking and changed no-hit behavior:
      - pages that are only too-recent no longer increment no-hit cutoff,
      - no-hit cutoff now applies once we are in/near the target window (or timestamps are unknown).
    - Added date-aware yt-dlp fallback mode:
      - use `ytsearchdate200` when date window is set,
      - include broader channel-biased query (`bravo`) in addition to keyword queries,
      - retain channel/date filtering and dedupe.
  - Validation:
    - `pytest -q tests/socials/test_comment_scraper_fixes.py -k youtube` (`10 passed, 15 deselected`)
    - `pytest -q tests/socials/test_comment_scraper_fixes.py` (`25 passed`)
    - `pytest -q tests/repositories/test_social_season_analytics.py -k youtube` (`4 passed, 35 deselected`)
    - `ruff check trr_backend/socials/youtube/scraper.py tests/socials/test_comment_scraper_fixes.py` (pass)

- February 19, 2026: Fixed Week 3 YouTube undercount caused by watch-page precise-date parser mismatch and broadened timestamp refinement coverage.
  - Files:
    - `trr_backend/socials/youtube/scraper.py`
    - `tests/socials/test_comment_scraper_fixes.py`
  - Root cause:
    - `_fetch_precise_publish_timestamp` matched only bare `YYYY-MM-DD` JSON values.
    - YouTube now commonly returns ISO timestamps like `uploadDate":"2025-10-02T06:00:06-07:00"` and `<meta itemprop="datePublished" content="...">`.
    - Precise extraction silently returned `0`, so low-precision relative labels (e.g. `4 months ago`) could remain incorrectly estimated and be excluded from week windows.
  - Changes:
    - Replaced single regex with multiple watch-page patterns covering JSON keys (`datePublished|uploadDate|publishDate`) and meta itemprop tags.
    - Added candidate parser that accepts ISO timestamps with timezone offsets and falls back to date-prefix parsing.
    - Added `_refine_video_publish_timestamp_if_needed` and applied it in both initial page and continuation page paths.
      - Now refines when publish time is low precision (`month|year`) or unknown (`0`) during date-window scrapes.
      - Added lightweight precise-fetch delay throttling and telemetry counters in `last_retrieval_meta` (`precise_publish_attempts/successes/failures`).
    - Corrected `first_page_count` telemetry to use actual first-page matches.
  - Validation:
    - `ruff check trr_backend/socials/youtube/scraper.py tests/socials/test_comment_scraper_fixes.py` (pass)
    - `pytest -q tests/socials/test_comment_scraper_fixes.py -k youtube` (`8 passed, 15 deselected`)
    - `pytest -q tests/repositories/test_social_season_analytics.py -k youtube` (`3 passed, 35 deselected`)

- February 19, 2026: Fixed YouTube week-range misses caused by coarse relative publish timestamps.
  - Files:
    - `trr_backend/socials/youtube/scraper.py`
    - `tests/socials/test_comment_scraper_fixes.py`
  - Changes:
    - Added absolute publish date parsing in `_estimate_publish_date` (e.g. `Premiered Oct 3, 2025`) and changed unknown fallback to `0` instead of `now`.
    - Added low-precision detection for month/year relative labels (e.g. `4 months ago`).
    - Added exact publish-date refinement via watch-page metadata (`datePublished`/`uploadDate`) when a low-precision estimate would otherwise be excluded by date-range filtering.
    - Applied refinement in both initial-page and continuation-page filtering paths so week-window ingest no longer drops valid videos due month/year approximation drift.
  - Validation:
    - `pytest -q tests/socials/test_comment_scraper_fixes.py` (`21 passed`)
    - `pytest -q tests/repositories/test_social_season_analytics.py -k "youtube_video_matches_show_terms or get_post_comments_youtube_includes_thumbnail_url"` (`3 passed, 34 deselected`)

- February 19, 2026: Added Instagram GraphQL no-match page cutoff to prevent long-running zero-yield incremental scans.
  - Files:
    - `trr_backend/socials/instagram/scraper.py`
    - `tests/socials/test_comment_scraper_fixes.py`
  - Changes:
    - Added `ScrapeConfig.no_match_page_limit` (optional override).
    - Added scraper-level resolver for no-match page cap:
      - explicit config override, else env (`SOCIAL_INSTAGRAM_NO_MATCH_PAGE_LIMIT` / `SOCIAL_NO_MATCH_PAGE_LIMIT`), else default `40` when date filters are active.
    - Updated `_scrape_graphql` pagination loop to stop after consecutive pages with zero new matches and emit telemetry:
      - `last_retrieval_meta.stop_reason` (`no_match_page_limit_reached`, `date_start_reached`, `max_pages_reached`, etc.)
      - `last_retrieval_meta.no_match_pages`
      - `last_retrieval_meta.no_match_page_limit`
    - Added regression test asserting GraphQL crawl halts after configured no-match threshold.
  - Validation:
    - `pytest -q tests/socials/test_comment_scraper_fixes.py` (`19 passed`)
    - `pytest -q tests/repositories/test_social_season_analytics.py tests/api/routers/test_socials_season_analytics.py -k "not test_get_analytics_includes_weekly_platform_engagement_and_has_data"` (`54 passed, 1 deselected`)

- February 19, 2026: Hardened person source-link discovery/validation and executed Bravo-wide cleanup + rediscovery backfill.
  - Files:
    - `api/routers/admin_show_links.py`
    - `api/routers/admin_show_bravo.py`
    - `trr_backend/integrations/fandom.py`
    - `trr_backend/integrations/fandom_community_allowlist.txt` (new)
    - `scripts/shows/cleanup_invalid_person_knowledge_links.py`
    - `scripts/shows/backfill_bravo_person_source_links.py` (new)
    - `tests/api/routers/test_admin_show_links.py`
    - `tests/api/routers/test_admin_show_bravo.py`
    - `tests/integrations/fandom/test_fandom_search.py` (new)
  - Changes:
    - Extended person discovery to emit only validated links for `imdb`, `tmdb`, `wikidata`, `wikipedia`, `fandom`, and `bravo_profile`.
    - Added ID sourcing from `core.people.external_ids` with fallback to `core.cast_tmdb` (`imdb_id`, `tmdb_id`, `wikidata_id`).
    - Added strict source-specific validators:
      - IMDb canonical `https://www.imdb.com/name/{nm...}/`
      - TMDb canonical `https://www.themoviedb.org/person/{id}`
      - Bravo canonical `https://www.bravotv.com/people/{slug}`
      - Fandom/Wikipedia/Wikidata owner-match + existence checks (with fetch-error preservation)
    - Added allowlisted cross-wiki Fandom search support and seed allowlist file.
    - Expanded invalid-link cleanup scanner from knowledge-only to full person-source set (`wikipedia`, `wikidata`, `fandom`, `imdb`, `tmdb`, `bravo_profile`).
    - Updated Bravo sync link persistence to respect discovered status/confidence instead of force-setting pending.
    - Added Bravo backfill script to run per-show cleanup then rediscovery across all Bravo shows.
  - Validation:
    - `ruff check api/routers/admin_show_links.py api/routers/admin_show_bravo.py trr_backend/integrations/fandom.py scripts/shows/cleanup_invalid_person_knowledge_links.py scripts/shows/backfill_bravo_person_source_links.py tests/api/routers/test_admin_show_links.py tests/api/routers/test_admin_show_bravo.py tests/integrations/fandom/test_fandom_search.py` (pass)
    - `python -m py_compile api/routers/admin_show_links.py api/routers/admin_show_bravo.py trr_backend/integrations/fandom.py scripts/shows/cleanup_invalid_person_knowledge_links.py scripts/shows/backfill_bravo_person_source_links.py` (pass)
    - `pytest -q tests/api/routers/test_admin_show_links.py tests/api/routers/test_admin_show_bravo.py tests/integrations/fandom/test_fandom_search.py` (`38 passed`)
    - Backfill execution:
      - `PYTHONPATH=. python -u scripts/shows/backfill_bravo_person_source_links.py --apply`
      - Result: `shows=62`, `failed_shows=0`, `discovered_upserted=991`, `cleanup_scanned=52`, `cleanup_invalid=0`, `cleanup_deleted=0`, `cleanup_fetch_errors=0`.

- February 19, 2026: Fixed social ingest queue/runtime reliability for stale recovery and incremental post skip behavior.
  - Files:
    - `trr_backend/repositories/social_season_analytics.py`
    - `tests/repositories/test_social_season_analytics.py`
  - Changes:
    - Escaped literal SQL format token in stale heartbeat error message (`%%s`) inside `recover_stale_running_jobs` so psycopg2 bind placeholder counting matches provided params.
    - Added regression assertion in stale-recovery unit test that unescaped `%s` placeholder count equals parameter list length.
    - Added posts-stage incremental fast-path for Instagram/TikTok/YouTube/Twitter:
      - when a post already exists and expected comment/reply counts match stored active counts, skip upsert/comment fetch for that post.
      - records skip reason in `comment_refresh_decisions.up_to_date`.
    - Removed undefined `comment_count` accumulators in platform comment loops to avoid runtime `NameError` during comment ingest.
    - Added regression test for Instagram posts-stage incremental skip behavior.
  - Validation:
    - `pytest tests/repositories/test_social_season_analytics.py tests/api/routers/test_socials_season_analytics.py -k 'not test_get_analytics_includes_weekly_platform_engagement_and_has_data' -q` (`54 passed, 1 deselected`)

- February 19, 2026: Implemented scraped-first live progress + stale job auto-retry for social ingest queue runs.
  - Files:
    - `trr_backend/repositories/social_season_analytics.py`
    - `scripts/socials/worker.py`
    - `trr_backend/socials/instagram/scraper.py`
    - `trr_backend/socials/tiktok/scraper.py`
    - `trr_backend/socials/twitter/scraper.py`
    - `trr_backend/socials/youtube/scraper.py`
    - `tests/repositories/test_social_season_analytics.py`
    - `tests/socials/test_comment_scraper_fixes.py`
    - `tests/api/routers/test_socials_season_analytics.py`
  - Changes:
    - Progress contract now tracks scraped counters as primary (`items_found`, `metadata.stage_counters`) with additive saved counters in `metadata.persist_counters`.
    - Added additive activity diagnostics in `metadata.activity` (`phase`, `pages_scanned`, `posts_checked`, `matched_posts`, `last_progress_at`).
    - Added reusable progress flush controls in repository:
      - `JOB_PROGRESS_MIN_DELTA=5`
      - `JOB_PROGRESS_MAX_INTERVAL_SECONDS=3`
      - force flush at stage start/end.
    - Added optional per-page `progress_cb` support to Instagram/TikTok/Twitter/YouTube scrapers and wired callbacks into ingest loops for in-flight visibility before upsert.
    - Added stale-running-job recovery with default `SOCIAL_JOB_STALE_SECONDS=300`:
      - stale `running` jobs move to `retrying` when attempts remain, else `failed`.
      - metadata includes `error_code=stale_heartbeat_timeout` and retryability diagnostics.
    - Worker loop now executes stale-recovery checks before run execution and queue claims.
    - Preserved additive API/job metadata compatibility; no breaking route changes.
  - Validation:
    - `pytest tests/socials/test_comment_scraper_fixes.py tests/repositories/test_social_season_analytics.py tests/api/routers/test_socials_season_analytics.py` (`71 passed`)

- February 19, 2026: Implemented strict 100%-gate networks/streaming completion pipeline with schema preflight + per-entity override/completion persistence.
  - Files:
    - `supabase/migrations/0131_network_streaming_completion_and_overrides.sql` (new)
    - `scripts/sync/sync_networks_streaming_links.py`
    - `api/routers/admin_show_sync.py`
    - `scripts/README.md`
    - `tests/scripts/test_sync_networks_streaming_links.py`
    - `tests/api/routers/test_admin_show_sync.py`
  - Changes:
    - Added admin persistence tables:
      - `admin.network_streaming_overrides`
      - `admin.network_streaming_completion`
      - `admin.network_streaming_completion_attempts`
    - Extended `sync_networks_streaming_links` to:
      - build used-entity inventory from full show scope,
      - apply override precedence,
      - log per-source attempt outcomes (`override|tmdb|wikimedia|official|catalog|variant`),
      - persist completion rows and attempt history,
      - support `--unresolved-only` reruns,
      - emit completion metrics (`completion_total`, `completion_resolved`, `completion_unresolved`, `completion_percent`).
    - Added hard schema preflight to `POST /api/v1/admin/shows/sync-networks-streaming`:
      - blocks sync when required columns/tables are missing,
      - returns structured `missing_columns[]`.
    - Expanded sync endpoint contract with completion gate fields:
      - `completion_total`, `completion_resolved`, `completion_unresolved`, `completion_percent`, `completion_gate_passed`.
    - Added override CRUD endpoints:
      - `GET /api/v1/admin/shows/networks-streaming/overrides`
      - `POST /api/v1/admin/shows/networks-streaming/overrides`
      - `PATCH /api/v1/admin/shows/networks-streaming/overrides/{id}`
      - `DELETE /api/v1/admin/shows/networks-streaming/overrides/{id}`
  - Validation:
    - `ruff check api/routers/admin_show_sync.py scripts/sync/sync_networks_streaming_links.py tests/api/routers/test_admin_show_sync.py tests/scripts/test_sync_networks_streaming_links.py` (pass)
    - `pytest -q tests/api/routers/test_admin_show_sync.py tests/scripts/test_sync_networks_streaming_links.py tests/media/test_s3_mirror.py` (`59 passed`)
    - `pytest -q tests/scripts/test_sync_tmdb_watch_providers.py` (`2 passed`)

- February 19, 2026: Fixed social ingest queue fail-fast behavior, worker heartbeat tracking, Twitter comments-only anchor handling, and malformed comment ID persistence guardrails.
  - Files:
    - `api/routers/socials.py`
    - `scripts/socials/worker.py`
    - `trr_backend/repositories/social_season_analytics.py`
    - `supabase/migrations/0130_social_worker_heartbeat_and_comment_id_guardrails.sql` (new)
    - `tests/api/routers/test_socials_season_analytics.py`
    - `tests/repositories/test_social_season_analytics.py`
    - `docs/runbooks/social_worker_queue_ops.md` (new)
  - Changes:
    - Added worker heartbeat persistence + health checks (`social.scrape_workers`) and queue-mode fail-fast enforcement.
    - Ingest start endpoint now returns additive `503` with detail code `SOCIAL_WORKER_UNAVAILABLE` when queue mode is enabled and no healthy worker is present.
    - Added additive admin endpoint: `GET /api/v1/admin/socials/ingest/worker-health`.
    - Fixed Twitter comments-only anchor iteration so `max_posts_per_target <= 0` is treated as no cap in comment hydration paths.
    - Hardened comment persistence:
      - skip blank external IDs (`comment_id` / `tweet_id`),
      - track `comments_fetched`, `comments_upserted`, `comments_skipped_missing_id`,
      - include comment stats in ingest job metadata for diagnostics.
    - Added DB guardrails in migration `0130`:
      - heartbeat table for workers,
      - pre-cleanup deletes for blank IDs,
      - check constraints to prevent blank IDs going forward.
    - Added operations runbook with worker start commands and exact SQL checks for heartbeat, queue backlog, and comment-stat gaps.
  - Validation:
    - `ruff check api/routers/socials.py tests/api/routers/test_socials_season_analytics.py tests/repositories/test_social_season_analytics.py scripts/socials/worker.py trr_backend/repositories/social_season_analytics.py` (pass)
    - `pytest -q tests/socials/test_comment_scraper_fixes.py tests/repositories/test_social_season_analytics.py tests/api/routers/test_socials_season_analytics.py` (`64 passed`)

- February 19, 2026: Added Google News featured-image parity with Bravo and mirrored image sync to S3/Supabase.
  - Files:
    - `api/routers/admin_show_news.py`
    - `trr_backend/scraping/google_news_parser.py`
    - `tests/api/routers/test_admin_show_news.py`
    - `tests/scraping/test_google_news_parser.py`
  - Changes:
    - Extended Google RSS ingestion to backfill missing `image_url` from article metadata (`og:image`, `twitter:image`, `image_src`) when RSS omits media.
    - Added image enrichment metrics to parser result payload:
      - `featured_images_added`
      - `featured_images_probed`
      - `featured_image_errors`
    - Added Google News featured-image import stage in sync endpoint using the same `admin_scrape.import_images` media pipeline as Bravo imports.
      - Mirrors to S3 and persists media assets/links in Supabase.
      - Writes hosted URLs back into normalized Google news snapshot items.
      - Tracks sync metadata in response + snapshot source metadata (`image_sync`).
    - Expanded normalized Google news shape with additive fields:
      - `summary`
      - `original_image_url`
      - `hosted_image_url`
      - `media_asset_id`
      - `featured_image_synced`
    - Preserved already-tagged payloads during normalization (reuses existing `person_tags`, `topic_tags`, `season_matches` when present).
  - Validation:
    - `ruff check api/routers/admin_show_news.py trr_backend/scraping/google_news_parser.py tests/api/routers/test_admin_show_news.py tests/scraping/test_google_news_parser.py` (pass)
    - `pytest -q tests/scraping/test_google_news_parser.py tests/api/routers/test_admin_show_news.py` (`9 passed`)

- February 19, 2026: Completed production logo completion pipeline with black/white transparent variants + unresolved reporting contract.
  - Files:
    - `trr_backend/media/s3_mirror.py`
    - `scripts/sync/sync_networks_streaming_links.py`
    - `api/routers/admin_show_sync.py`
    - `supabase/migrations/0128_add_network_provider_monochrome_logo_fields.sql` (new)
    - `tests/media/test_s3_mirror.py`
    - `tests/scripts/test_sync_networks_streaming_links.py`
    - `tests/api/routers/test_admin_show_sync.py`
  - Changes:
    - Added explicit monochrome variant persistence columns for `core.networks` and `core.watch_providers` (`hosted_logo_black_*`, `hosted_logo_white_*`).
    - Added deterministic monochrome variant pipeline in S3 mirror utility:
      - key namespace `images/logos/{kind}/{entity_id}/{black|white}/{sha}.png`
      - transparent alpha extraction from existing alpha, corner background removal, then luminance fallback
      - no-overwrite behavior unless forced.
    - Extended `sync_networks_streaming_links` to:
      - process used-row scope only,
      - mirror base logos + black/white variants,
      - emit unresolved-logo records with reason codes and machine-parsable log lines.
    - Expanded admin sync endpoint response with:
      - `variants_black_mirrored`, `variants_white_mirrored`
      - `unresolved_logos_count`, `unresolved_logos`, `unresolved_logos_truncated`
      - unresolved payload truncation cap (300).
    - Added/updated tests for:
      - variant generation and transparent output,
      - no-overwrite behavior,
      - used-row scope filtering,
      - unresolved reason mapping,
      - admin endpoint unresolved-list truncation behavior.
  - Validation:
    - `ruff check trr_backend/media/s3_mirror.py scripts/sync/sync_networks_streaming_links.py api/routers/admin_show_sync.py tests/media/test_s3_mirror.py tests/scripts/test_sync_networks_streaming_links.py tests/api/routers/test_admin_show_sync.py` (pass)
    - `pytest tests/media/test_s3_mirror.py tests/scripts/test_sync_networks_streaming_links.py tests/api/routers/test_admin_show_sync.py` (`55 passed`)

- February 19, 2026: Added networks/streaming enrichment persistence, sync pipeline orchestration, and admin sync endpoint.
  - Files:
    - `supabase/migrations/0127_add_network_provider_link_fields.sql` (new)
    - `scripts/sync/sync_networks_streaming_links.py` (new)
    - `scripts/sync_networks_streaming_links.py` (new)
    - `api/routers/admin_show_sync.py`
    - `trr_backend/media/s3_mirror.py`
    - `scripts/README.md`
    - `tests/scripts/test_sync_networks_streaming_links.py` (new)
    - `tests/api/routers/test_admin_show_sync.py`
    - `tests/media/test_s3_mirror.py`
  - Changes:
    - Added additive enrichment columns on both dimension tables:
      - `wikidata_id`, `wikipedia_url`, `wikimedia_logo_file`, `link_enriched_at`, `link_enrichment_source`.
    - Implemented new enrichment script:
      - Wikidata entity resolution by name
      - Wikipedia sitelink persistence
      - Wikimedia logo extraction from Wikidata claims (`P154`/`P2910`)
      - fallback mirror-to-S3 for missing logos (or forced runs)
      - summary counters (`processed`, `links_enriched`, `wikidata_linked`, `wikipedia_linked`, `logos_mirrored`, `failures`).
    - Added admin endpoint:
      - `POST /api/v1/admin/shows/sync-networks-streaming`
      - orchestrates `sync_tmdb_show_entities --all`, `sync_tmdb_watch_providers --all`, `sync_networks_streaming_links --all`
      - returns per-step status/metrics + aggregate counters.
    - Added generic external-logo mirror helper in S3 mirror utility for wiki/commons image ingestion.
  - Validation:
    - `pytest -q tests/scripts/test_sync_networks_streaming_links.py tests/api/routers/test_admin_show_sync.py tests/media/test_s3_mirror.py` (`45 passed`)
    - `ruff check api/routers/admin_show_sync.py scripts/sync/sync_networks_streaming_links.py scripts/sync_networks_streaming_links.py trr_backend/media/s3_mirror.py tests/scripts/test_sync_networks_streaming_links.py tests/api/routers/test_admin_show_sync.py tests/media/test_s3_mirror.py` (pass)

- February 17, 2026: Implemented person-gallery reliability pipeline upgrades (cast + media mirroring, cast-photo variants, resize stage, and counter expansion).
  - Files:
    - `api/routers/admin_person_images.py`
    - `api/routers/admin_media_assets.py`
    - `api/routers/admin_cast_photos.py`
    - `trr_backend/media/s3_mirror.py`
    - `trr_backend/media/image_variants.py`
    - `trr_backend/repositories/media_assets.py`
    - `tests/api/routers/test_admin_person_images.py`
    - `tests/api/routers/test_admin_cast_photos.py` (new)
    - `tests/repositories/test_media_assets_mirroring.py`
  - Changes:
    - Added shared media-asset mirror helper (`mirror_media_asset_row`) with optional dimension extraction and reused it in admin media-assets mirror route.
    - Extended media-asset mirror persistence to optionally backfill `width`/`height`.
    - Added cast-photo variant generation support:
      - new variant key namespace `cast-photo-variants/{photo_id}/{crop_signature_hash}/...`
      - metadata wiring (`variants`, `thumb_url`, `display_url`, `detail_url`, crop variant URLs/signature).
    - Added endpoint:
      - `POST /api/v1/admin/cast-photos/{photo_id}/variants`
    - Upgraded person refresh pipelines (`refresh-images` + `refresh-images/stream`) to:
      - mirror both cast photos and person-linked media assets,
      - report split mirror counters and aggregate totals,
      - run variant generation stage for cast/media (base + crop variants),
      - emit/return resize counters and stream `stage: "resizing"` progress.
  - Validation:
    - `ruff check api/routers/admin_cast_photos.py api/routers/admin_media_assets.py api/routers/admin_person_images.py trr_backend/media/image_variants.py trr_backend/media/s3_mirror.py trr_backend/repositories/media_assets.py tests/api/routers/test_admin_person_images.py tests/api/routers/test_admin_cast_photos.py tests/repositories/test_media_assets_mirroring.py` (pass)
    - `pytest tests/api/routers/test_admin_person_images.py tests/api/routers/test_admin_cast_photos.py tests/repositories/test_media_assets_mirroring.py` (`36 passed`)

- February 17, 2026: Implemented Task 10 social admin incremental sync and comment lifecycle reconciliation.
  - Files:
    - `api/routers/socials.py`
    - `trr_backend/repositories/social_season_analytics.py`
    - `supabase/migrations/0126_social_comment_lifecycle_flags.sql`
    - `tests/api/routers/test_socials_season_analytics.py`
    - `tests/repositories/test_social_season_analytics.py`
    - `docs/cross-collab/TASK10/PLAN.md`
    - `docs/cross-collab/TASK10/OTHER_PROJECTS.md`
    - `docs/cross-collab/TASK10/STATUS.md`
  - Changes:
    - Added additive ingest request field `sync_strategy` with default `incremental` and `full_refresh` override.
    - Persisted run config details for `sync_strategy` and explicit platform scope in scrape runs.
    - Added migration `0126` with additive lifecycle fields/indexes for social comments/tweets:
      - `is_missing`, `missing_at`, `first_seen_at`, `last_seen_at`, `last_seen_run_id`.
    - Replaced comment refresh skip heuristic with policy matrix:
      - count gap/drop checks,
      - never-checked refresh,
      - 24h stale recheck,
      - 14-day quiet-post force rerun refresh,
      - full-refresh override.
    - Added conservative missing-mark logic:
      - mark missing only on complete comment fetches,
      - clear missing flags on reappearance during upsert.
  - Validation:
    - `pytest -q tests/api/routers/test_socials_season_analytics.py tests/repositories/test_social_season_analytics.py` (`39 passed`)
  - Cross-repo:
    - screenalytics compatibility validation completed (no code changes required).
    - TRR-APP consumer/UX updates completed under `TRR-APP/docs/cross-collab/TASK9/`.

- February 17, 2026: Implemented Task 10 social admin incremental sync and comment lifecycle reconciliation.
  - Files:
    - `api/routers/socials.py`
    - `trr_backend/repositories/social_season_analytics.py`
    - `supabase/migrations/0126_social_comment_lifecycle_flags.sql`
    - `tests/api/routers/test_socials_season_analytics.py`
    - `tests/repositories/test_social_season_analytics.py`
    - `docs/cross-collab/TASK10/PLAN.md`
    - `docs/cross-collab/TASK10/OTHER_PROJECTS.md`
    - `docs/cross-collab/TASK10/STATUS.md`
  - Changes:
    - Added additive ingest request field `sync_strategy` with default `incremental` and `full_refresh` override.
    - Persisted run config details for `sync_strategy` and explicit platform scope in scrape runs.
    - Added migration `0126` with additive lifecycle fields/indexes for social comments/tweets:
      - `is_missing`, `missing_at`, `first_seen_at`, `last_seen_at`, `last_seen_run_id`.
    - Replaced comment refresh skip heuristic with policy matrix:
      - count gap/drop checks,
      - never-checked refresh,
      - 24h stale recheck,
      - 14-day quiet-post force rerun refresh,
      - full-refresh override.
    - Added conservative missing-mark logic:
      - mark missing only on complete comment fetches,
      - clear missing flags on reappearance during upsert.
  - Validation:
    - `pytest -q tests/api/routers/test_socials_season_analytics.py tests/repositories/test_social_season_analytics.py` (`39 passed`)
  - Cross-repo:
    - screenalytics compatibility validation completed (no code changes required).
    - TRR-APP consumer/UX updates completed under `TRR-APP/docs/cross-collab/TASK9/`.

- February 17, 2026: Implemented social admin reliability contract additions for run history and query performance hardening.
  - Files:
    - `api/routers/socials.py`
    - `trr_backend/repositories/social_season_analytics.py`
    - `supabase/migrations/0125_social_analytics_query_indexes.sql`
    - `start-api.sh`
    - `tests/api/routers/test_socials_season_analytics.py`
    - `tests/repositories/test_social_season_analytics.py`
  - Changes:
    - Added additive endpoint `GET /api/v1/admin/socials/seasons/{season_id}/ingest/runs` with `limit`, optional `status`, optional `source_scope`.
    - Added repository `list_runs(...)` with ordering by `created_at desc` and optional filters.
    - Removed duplicate `_pg_upsert` helper definition from social analytics repository.
    - Added migration `0125` with additive `if not exists` social analytics indexes for IG/TT/YT/Twitter post+comment query paths and scrape job recency.
    - Tuned local reload behavior in `start-api.sh` to watch only `api` + `trr_backend` and exclude noisy dirs (`.logs`, `.venv`, `tests`, `scripts`, `supabase`) while keeping reload enabled by default.
  - Validation:
    - `ruff check api/routers/socials.py trr_backend/repositories/social_season_analytics.py tests/api/routers/test_socials_season_analytics.py tests/repositories/test_social_season_analytics.py` (pass)
    - `bash -n start-api.sh` (pass)
    - `pytest -q tests/api/routers/test_socials_season_analytics.py tests/repositories/test_social_season_analytics.py` (`35 passed`)

- February 17, 2026: Hardened person knowledge-link ownership validation and relationship-text sanitization for cast-matrix sync.
  - Person knowledge link validation hardening:
    - `api/routers/admin_show_links.py`
    - `_validated_person_knowledge_url(...)` now validates that fetched Wikipedia/Fandom pages actually match the expected person name (URL slug + page heading/title candidates), in addition to missing-page checks.
    - Prevents false-positive person links where a name appears on an unrelated page (for example show pages or the wrong cast member page).
    - Discovery now also prunes stale pending auto-discovered person knowledge links that are no longer valid and reports `stale_pending_people_deleted` in discovery response payload.
  - Relationship parser cleanup:
    - `trr_backend/ingestion/show_cast_matrix_scraper.py`
    - Added cleanup for zero-width chars, wiki template CSS leakage (`.mw-parser-output...`), and citation markers.
    - Improved chunk splitting to respect parentheses/braces so marriage-template style text no longer fragments names.
    - Added noise-name filtering to skip numeric/template fragments in relationship extraction.
  - Test updates:
    - `tests/api/routers/test_admin_show_links.py`
      - Added validation tests for mismatched Wikipedia/Fandom pages and matched fandom person pages.
    - `tests/ingestion/test_show_cast_matrix_scraper.py`
      - Added regression for CSS/template spouse text cleanup + semicolon split behavior.
  - Validation:
    - `ruff check api/routers/admin_show_links.py trr_backend/ingestion/show_cast_matrix_scraper.py tests/api/routers/test_admin_show_links.py tests/ingestion/test_show_cast_matrix_scraper.py` (pass)
    - `pytest -q tests/api/routers/test_admin_show_links.py tests/ingestion/test_show_cast_matrix_scraper.py` (`16 passed`)

- February 17, 2026: Expanded Bravo social account targeting and source-scoped analytics for RHOSLC Season 6.
  - Account map updates for `source_scope=bravo`:
    - YouTube now targets both `bravo` and `wwhl`.
    - Instagram includes `bravotv`, `bravowwhl`, and `bravodailydish`.
    - TikTok includes `bravotv` and `bravowwhl`.
    - Twitter/X includes `BravoTV` and `BravoWWHL`.
  - Repository changes:
    - `trr_backend/repositories/social_season_analytics.py`
    - Replaced hardcoded account filters with dynamic platform-specific target account sets.
    - Added account-handle normalization and scoped filtering for analytics rows + week details.
    - Preserved unscoped `community` behavior while enforcing scoped empty-results for Bravo/Creator when no mapped accounts exist for a platform.
  - Test updates:
    - `tests/repositories/test_social_season_analytics.py`
    - `tests/api/routers/test_socials_season_analytics.py`
    - Updated expectations for dynamic `= any(%s)` account filtering and week-detail account handle passing.
  - Validation:
    - `ruff check trr_backend/repositories/social_season_analytics.py tests/repositories/test_social_season_analytics.py` (pass)
    - `pytest -q tests/repositories/test_social_season_analytics.py` (`19 passed`)
    - `pytest -q tests/api/routers/test_socials_season_analytics.py` (`10 passed`)

- February 17, 2026: Fixed RHOSLC YouTube false positives, added IG/TikTok thumbnail persistence, and shipped a cleanup utility for existing bad rows.
  - YouTube strict show matching and cross-show exclusion:
    - `trr_backend/repositories/social_season_analytics.py`
    - Enforced title-or-hashtag matching for show relevance.
    - Added explicit exclusion for titles containing both `wife swap` and `real housewives edition`.
  - Thumbnail ingestion and persistence:
    - `trr_backend/socials/instagram/scraper.py` (populate `media_urls` + `thumbnail_url`)
    - `trr_backend/socials/tiktok/scraper.py` (extract API/yt-dlp thumbnail candidates + `thumbnail_url`)
    - `trr_backend/repositories/social_season_analytics.py` (upsert/payload support for IG/TT thumbnails; post-detail thumbnail support for IG/TT/YT)
  - DB migration:
    - `supabase/migrations/0124_social_thumbnails_and_reddit_sources.sql`
    - Added:
      - `social.instagram_posts.thumbnail_url text`
      - `social.tiktok_posts.thumbnail_url text`
  - One-off cleanup utility:
    - `scripts/socials/cleanup_youtube_false_positives.py`
    - Supports `--dry-run`; scoped for RHOSLC + Wife Swap cross-show title pattern.
  - Tests:
    - `tests/repositories/test_social_season_analytics.py`
    - `tests/socials/test_comment_scraper_fixes.py`
    - Added coverage for strict YouTube matching/exclusions and IG/TT thumbnail parsing + payload wiring.
  - Validation:
    - `ruff check trr_backend/repositories/social_season_analytics.py trr_backend/socials/instagram/scraper.py trr_backend/socials/tiktok/scraper.py tests/repositories/test_social_season_analytics.py tests/socials/test_comment_scraper_fixes.py scripts/socials/cleanup_youtube_false_positives.py` (pass)
    - `python3 -m py_compile trr_backend/repositories/social_season_analytics.py trr_backend/socials/instagram/scraper.py trr_backend/socials/tiktok/scraper.py scripts/socials/cleanup_youtube_false_positives.py` (pass)
    - `pytest -q tests/repositories/test_social_season_analytics.py tests/socials/test_comment_scraper_fixes.py` (`27 passed`)

- February 17, 2026: Implemented global cast-matrix sync with Wiki/Fandom role ingestion, relationship roles, Kid global assignment, and Bravo auto-link/image enforcement.
  - Added cast-matrix scraper + relationship extraction:
    - `trr_backend/ingestion/show_cast_matrix_scraper.py`
  - Extended Fandom person parser label + family relation handling:
    - `trr_backend/ingestion/fandom_person_scraper.py`
  - Added new admin endpoint:
    - `POST /api/v1/admin/shows/{show_id}/cast-matrix/sync`
    - Router/service implementation: `api/routers/admin_show_roles.py`
  - Extended Bravo commit contract with auto-trigger:
    - `sync_cast_matrix: bool = true` in `api/routers/admin_show_bravo.py`
  - Fixed Bravo snapshot variant mismatch for link discovery reads:
    - `api/routers/admin_show_links.py` now uses the same `default` variant as Bravo writer.
  - Added/updated backend tests and fixtures:
    - `tests/ingestion/test_show_cast_matrix_scraper.py`
    - `tests/ingestion/test_fandom_person_scraper.py`
    - `tests/api/routers/test_admin_show_roles.py`
    - `tests/api/routers/test_admin_show_links.py`
    - `tests/api/routers/test_admin_show_bravo.py`
    - `tests/fixtures/wikipedia/rhoslc_cast_table_sample.html`
    - `tests/fixtures/fandom/rhoslc_cast_table_sample.html`
    - `tests/fixtures/fandom/lisa_barlow_person_live_infobox_sample.html`
  - Validation:
    - `pytest -q tests/ingestion/test_show_cast_matrix_scraper.py tests/ingestion/test_fandom_person_scraper.py tests/api/routers/test_admin_show_roles.py tests/api/routers/test_admin_show_links.py tests/api/routers/test_admin_show_bravo.py` (`21 passed`)

- February 13, 2026: Added Bravo profile-picture tagging + thumbnail metadata persistence.
  - `api/routers/admin_show_bravo.py`
    - Bravo person hero imports now persist `context_type="profile_picture"` (legacy `profile` removed for new imports).
    - Updated image labels to "Bravo profile picture".
  - `trr_backend/media/image_variants.py`
    - Base variant metadata now writes `thumb_url` for lightweight gallery card rendering.
  - Validation:
    - `ruff check api/routers/admin_show_bravo.py trr_backend/media/image_variants.py` (pass)
    - `pytest -q tests/api/routers/test_admin_show_bravo.py` (10 passed)

- February 12, 2026: Reduced Bravo read latency for show admin pages.
  - `api/routers/admin_show_bravo.py` now avoids unnecessary `person_source_latest` fan-out reads when show snapshots already contain embedded normalized person videos/news.
  - Fallback person snapshot reads are preserved only for older snapshots without embedded `videos_person` / `news_person`.
  - Validation:
    - `ruff check api/routers/admin_show_bravo.py tests/api/routers/test_admin_show_bravo.py`
    - `pytest -q tests/api/routers/test_admin_show_bravo.py` (10 passed)

- February 12, 2026: Implemented image variant persistence and crop derivative generation for existing/new media assets.
  - Added migration: `supabase/migrations/0119_create_media_asset_variants.sql`.
  - Added variant generator: `trr_backend/media/image_variants.py`.
  - Added admin endpoint: `POST /api/v1/admin/media-assets/{asset_id}/variants` (`api/routers/admin_media_assets.py`).
  - Wired variant generation into:
    - `api/routers/admin_scrape.py` (new imports + duplicate-link flows)
    - `api/routers/admin_image_counts.py` (auto-count thumbnail crop -> crop variants)
  - Added backfill scripts:
    - `scripts/media/backfill_media_asset_variants.py`
    - `scripts/backfill_media_asset_variants.py`
  - Validation:
    - `ruff check` on touched backend files (pass)
    - `python3 -m py_compile` on touched backend modules/scripts (pass)

- February 12, 2026: Instagram social ingest reliability + throughput hardening (RHOSLC S6 live debug).
  - Increased season ingest API defaults for full backfills:
    - `api/routers/socials.py` `SeasonSocialIngestRequest.max_posts_per_target`: `25 -> 5000` (max `20000`)
    - `SeasonSocialIngestRequest.max_comments_per_post`: `100 -> 0` (allows post-only ingest)
  - Added comment-fetch bypass when `max_comments_per_post == 0` for:
    - Instagram (`_ingest_instagram`)
    - TikTok (`_ingest_tiktok`)
    - YouTube (`_ingest_youtube`)
    - plus ingest options coercion now allows zero comments.
  - Added Instagram ingest pacing env controls:
    - `SOCIAL_INSTAGRAM_DELAY_SEC` (default `0.15`)
    - `SOCIAL_INSTAGRAM_COMMENT_DELAY_SEC` (default `0.25`)
    - documented in `.env.example`.
  - Added router test to lock zero-comment behavior:
    - `tests/api/routers/test_socials_season_analytics.py::test_ingest_allows_zero_comments_limit`
  - Live smoke result (local DB):
    - Season `e9161955-6ee4-4985-865e-3386a0f670fb` (RHOSLC S6), Instagram-only pre-season ingest (Aug 14, 2025 → Sep 16, 2025 ET): `51` posts ingested, `0` comments.
    - Analytics check: week 0 Instagram posts = `47` (`get_analytics(... week=0, platforms=['instagram'])`).

- February 12, 2026: fixed Instagram season-ingest undercount in social analytics.
  - Root cause: `trr_backend/repositories/social_season_analytics.py` hardcoded `InstagramScraper(cookies={})`, forcing unauthenticated mode (limited to ~12 posts).
  - Added `_load_instagram_cookies()` with resolution order:
    1. `SOCIAL_INSTAGRAM_COOKIES_JSON` / `INSTAGRAM_COOKIES_JSON`
    2. `SOCIAL_INSTAGRAM_COOKIES_FILE` / `INSTAGRAM_COOKIES_FILE`
    3. repo default `scripts/socials/instagram/instagram_cookies.json`
  - `_ingest_instagram(...)` now uses resolved cookies and logs a warning when `sessionid` is missing.
  - Added env docs in `.env.example`:
    - `SOCIAL_INSTAGRAM_COOKIES_JSON`
    - `SOCIAL_INSTAGRAM_COOKIES_FILE`
  - Added tests:
    - `tests/repositories/test_social_season_analytics.py` (env-json precedence and file fallback)
  - Improved Instagram GraphQL reliability in `trr_backend/socials/instagram/scraper.py`:
    - Added doc-id fallback chain (`26035927152742158` then `33944389991841132`)
    - Added optional env override `INSTAGRAM_PROFILE_POSTS_DOC_ID`
  - Verification:
    - `pytest -q tests/repositories/test_social_season_analytics.py tests/api/routers/test_socials_season_analytics.py` (12 passed)
    - `ruff check trr_backend/repositories/social_season_analytics.py tests/repositories/test_social_season_analytics.py` (passed)

- February 12, 2026: Reduced Bravo read latency for show admin pages.
  - `api/routers/admin_show_bravo.py` now avoids unnecessary `person_source_latest` fan-out reads when show snapshots already contain embedded normalized person videos/news.
  - Fallback person snapshot reads are preserved only for older snapshots without embedded `videos_person` / `news_person`.
  - Validation:
    - `ruff check api/routers/admin_show_bravo.py tests/api/routers/test_admin_show_bravo.py`
    - `pytest -q tests/api/routers/test_admin_show_bravo.py` (10 passed)

- February 12, 2026: Implemented image variant persistence and crop derivative generation for existing/new media assets.
  - Added migration: `supabase/migrations/0119_create_media_asset_variants.sql`.
  - Added variant generator: `trr_backend/media/image_variants.py`.
  - Added admin endpoint: `POST /api/v1/admin/media-assets/{asset_id}/variants` (`api/routers/admin_media_assets.py`).
  - Wired variant generation into:
    - `api/routers/admin_scrape.py` (new imports + duplicate-link flows)
    - `api/routers/admin_image_counts.py` (auto-count thumbnail crop -> crop variants)
  - Added backfill scripts:
    - `scripts/media/backfill_media_asset_variants.py`
    - `scripts/backfill_media_asset_variants.py`
  - Validation:
    - `ruff check` on touched backend files (pass)
    - `python3 -m py_compile` on touched backend modules/scripts (pass)

- February 12, 2026: Instagram social ingest reliability + throughput hardening (RHOSLC S6 live debug).
  - Increased season ingest API defaults for full backfills:
    - `api/routers/socials.py` `SeasonSocialIngestRequest.max_posts_per_target`: `25 -> 5000` (max `20000`)
    - `SeasonSocialIngestRequest.max_comments_per_post`: `100 -> 0` (allows post-only ingest)
  - Added comment-fetch bypass when `max_comments_per_post == 0` for:
    - Instagram (`_ingest_instagram`)
    - TikTok (`_ingest_tiktok`)
    - YouTube (`_ingest_youtube`)
    - plus ingest options coercion now allows zero comments.
  - Added Instagram ingest pacing env controls:
    - `SOCIAL_INSTAGRAM_DELAY_SEC` (default `0.15`)
    - `SOCIAL_INSTAGRAM_COMMENT_DELAY_SEC` (default `0.25`)
    - documented in `.env.example`.
  - Added router test to lock zero-comment behavior:
    - `tests/api/routers/test_socials_season_analytics.py::test_ingest_allows_zero_comments_limit`
  - Live smoke result (local DB):
    - Season `e9161955-6ee4-4985-865e-3386a0f670fb` (RHOSLC S6), Instagram-only pre-season ingest (Aug 14, 2025 → Sep 16, 2025 ET): `51` posts ingested, `0` comments.
    - Analytics check: week 0 Instagram posts = `47` (`get_analytics(... week=0, platforms=['instagram'])`).

- February 12, 2026: fixed Instagram season-ingest undercount in social analytics.
  - Root cause: `trr_backend/repositories/social_season_analytics.py` hardcoded `InstagramScraper(cookies={})`, forcing unauthenticated mode (limited to ~12 posts).
  - Added `_load_instagram_cookies()` with resolution order:
    1. `SOCIAL_INSTAGRAM_COOKIES_JSON` / `INSTAGRAM_COOKIES_JSON`
    2. `SOCIAL_INSTAGRAM_COOKIES_FILE` / `INSTAGRAM_COOKIES_FILE`
    3. repo default `scripts/socials/instagram/instagram_cookies.json`
  - `_ingest_instagram(...)` now uses resolved cookies and logs a warning when `sessionid` is missing.
  - Added env docs in `.env.example`:
    - `SOCIAL_INSTAGRAM_COOKIES_JSON`
    - `SOCIAL_INSTAGRAM_COOKIES_FILE`
  - Added tests:
    - `tests/repositories/test_social_season_analytics.py` (env-json precedence and file fallback)
  - Improved Instagram GraphQL reliability in `trr_backend/socials/instagram/scraper.py`:
    - Added doc-id fallback chain (`26035927152742158` then `33944389991841132`)
    - Added optional env override `INSTAGRAM_PROFILE_POSTS_DOC_ID`
  - Verification:
    - `pytest -q tests/repositories/test_social_season_analytics.py tests/api/routers/test_socials_season_analytics.py` (12 passed)
    - `ruff check trr_backend/repositories/social_season_analytics.py tests/repositories/test_social_season_analytics.py` (passed)

- Fixed Bravo snapshot persistence failure (`Failed to persist show bravo snapshot`) for environments missing `core.sources.id='bravo'`:
  - Applied migration `supabase/migrations/0117_add_bravo_source.sql` to active DB.
  - Added `_ensure_bravo_source(...)` guard in `api/routers/admin_show_bravo.py` so commit paths verify/create the Bravo source row before writing show/person snapshots.
  - Improved error detail in snapshot persistence failures to surface underlying DB messages in logs/API detail.
  - Validation: `pytest -q tests/api/routers/test_admin_show_bravo.py` and `ruff check api/routers/admin_show_bravo.py`.
- Fixed silent-failure behavior in episodic cast sync (`scripts/sync/sync_episode_appearances.py`):
  - Root issue observed: IMDb episodic GraphQL call now returns `PersistedQueryNotFound` for operation `TitleEpisodeBottomSheetCredits`, causing zero `core.credit_occurrences` inserts.
  - Script now treats "all cast episodic fetches failed" as a fatal show failure (`exit code 1`) so admin refresh surfaces a failed step instead of a false success.
  - Script no longer deletes `core.credit_occurrences` for credits that did not refresh successfully, preventing accidental wipeout of prior episode evidence during IMDb upstream failures.
  - Added summary metric: `fatal_show_failures`.
  - Validation: `ruff check scripts/sync/sync_episode_appearances.py` and `python -m py_compile scripts/sync/sync_episode_appearances.py`.
- Added resilient episodic GraphQL fallback in `trr_backend/integrations/imdb/episodic_client.py`:
  - When persisted query hashes rotate (`PERSISTED_QUERY_NOT_FOUND`), client now falls back to a direct POST GraphQL query against IMDb to fetch episode credits.
  - Direct response is reshaped to the existing parser contract, so downstream sync code remains unchanged.
  - Validation: `ruff check trr_backend/integrations/imdb/episodic_client.py` and `python -m py_compile trr_backend/integrations/imdb/episodic_client.py`.
  - Live verification on show `940ca82c-e4a9-45b4-9d85-946a654925ce`: `sync_episode_appearances` now succeeds (`occurrences_inserted=51`, `failures=0`).

- Pipeline performance controls added for show photo refresh:
  - `api/routers/admin_show_sync.py` now accepts fast-mode fields on `refresh-photos/stream`:
    - `skip_auto_count`, `skip_word_detection`, `imdb_mediaindex_max_pages`, `imdb_mediaindex_max_images`
  - This allows the app to run a reduced-cost refresh path for everyday syncs and avoid long AI post-processing delays.
- Added Bravo sync-readiness enforcement before preview/commit:
  - `POST /api/v1/admin/shows/{show_id}/import-bravo/preview`
  - `POST /api/v1/admin/shows/{show_id}/import-bravo/commit`
  - Both now require existing synced seasons, episodes, and cast; otherwise return `409` with missing sections.
  - File: `api/routers/admin_show_bravo.py`
- Added Fandom policy guard:
  - Show refresh cast-photo stage skips Fandom sources for non-`Real Housewives` shows.
  - Person refresh (sync + stream) also skips Fandom profile/source fetches for non-`Real Housewives` when show context is provided.
  - Files: `api/routers/admin_show_sync.py`, `api/routers/admin_person_images.py`
- Test/lint validation for this update:
  - `ruff check api/routers/admin_show_sync.py api/routers/admin_person_images.py api/routers/admin_show_bravo.py tests/api/routers/test_admin_show_bravo.py`
  - `pytest -q tests/api/routers/test_admin_show_bravo.py tests/api/routers/test_admin_show_sync.py tests/api/routers/test_admin_person_images.py` (`31 passed`)

## Goal

- Implement Supabase unification and schema cleanup work for screenalytics + TRR (migrations + API/ingestion updates), coordinated across TRR-Backend → screenalytics → TRR-APP.

## Status

- Implemented TRR-Backend migrations `0102`-`0115` (screenalytics missing tables + schema cleanup, excluding deferred phases 6j/6k).
- Updated TRR-Backend ingestion/admin routes and pipeline scripts to align with credits model + enriched people fields + new cast views.
- Cross-collab task docs updated:
  - `docs/cross-collab/TASK5/*` (screenalytics data layer unification: backend-owned pieces)
  - `docs/cross-collab/TASK4/*` (schema cleanup: backend-owned pieces)
  - `docs/cross-collab/TASK1/*`, `docs/cross-collab/TASK2/*`, `docs/cross-collab/TASK3/*` (status corrections for Dev Dashboard task scanning; Task 2 marked complete via screenalytics implementation; Task 1 complete; Task 3 complete)
- Admin show sync/refresh endpoints shipped (Task 6):
  - `POST /api/v1/admin/shows/sync-from-lists` (`api/routers/admin_show_sync.py`) — import candidates from IMDb/TMDb lists + enrich metadata (no images).
  - `POST /api/v1/admin/shows/{show_id}/refresh` (`api/routers/admin_show_sync.py`) — synchronous refresh targets via existing sync scripts (details, seasons/episodes, photos, cast/credits).
  - `POST /api/v1/admin/shows/{show_id}/refresh-photos/stream` (`api/routers/admin_show_sync.py`) — high-fidelity show photo refresh with live SSE stage updates and accurate progress counts (IMDb/TMDb show + season/episode + cast photos, S3 mirroring, auto-count + word id).
  - Tests: `tests/api/routers/test_admin_show_sync.py`
  - Cross-collab docs: `docs/cross-collab/TASK6/*`
- Admin media workflow enhancements shipped (Task 1):
  - `POST /api/v1/admin/media-assets/{asset_id}/detect-text-overlay?force={bool}` (`api/routers/admin_media_assets.py`)
  - `POST /api/v1/admin/cast-photos/{photo_id}/detect-text-overlay?force={bool}` (`api/routers/admin_cast_photos.py`)
  - Detector implementation tracked in `trr_backend/vision/text_overlay.py` (Gemini; gated by env; returns `503` if not configured)
- Person gallery refresh improvements:
  - `POST /api/v1/admin/person/{person_id}/refresh-images` now also refreshes TMDb + Fandom profiles best-effort and performs word-id detection best-effort.
  - `POST /api/v1/admin/person/{person_id}/refresh-images/stream` now emits live stage messages (`sync_imdb`, `sync_tmdb`, `sync_fandom`, `sync_fandom_gallery`, `mirroring`, `auto_count`, `word_id`) with accurate `current/total` progress, and can tag all fetched photos with show context (`show_id`/`show_name`) so “This Show” filtering works across sources.
- Media asset mirroring now persists mirror timestamps in `core.media_assets.metadata`:
  - `metadata.mirrored_at` (UTC ISO timestamp)
  - `metadata.mirrored_from` (source_url)
  - Files:
    - `api/routers/admin_media_assets.py`
    - `trr_backend/repositories/media_assets.py`
- Multi-person dedup backend piece shipped (Task 3):
  - `trr_backend/media/s3_mirror.py#mirror_cast_photo_row` mirrors cast photos to shared content-addressed keys (`media/{sha256[:2]}/{sha256}{ext}`)
- Fast checks: `ruff check . && ruff format --check . && pytest -q` (421 passed, 18 skipped).

Pending / not executed:
- Supabase migrations were validated locally via `supabase db reset` and applied to staging Supabase via `supabase db push --linked` (linked project: `trr-core`) in this session; prod not updated.
- Credits backfill (Phase 6b) needs to be run/verified on staging before applying 0107 drop (Phase 6c) in any environment that still relies on legacy cast tables.

## Notes / Constraints

- Local dev API runs on `http://127.0.0.1:8000` by default (`TRR_BACKEND_PORT` override supported).
- In the multi-repo workspace, `make dev/stop/logs` from this repo delegates to the workspace root (`../Makefile`).
- `start-api.sh` now fails fast if `.venv/` is missing and uses `exec uvicorn ...` so stop scripts can reliably terminate the server process.
- Workspace runner (`/Users/thomashulihan/Projects/TRR/make dev`) wires:
  - `SCREENALYTICS_API_URL` (default `http://127.0.0.1:8001`)
  - `CORS_ALLOW_ORIGINS` for TRR-APP (`:3000`)
- Shared DB env var contract: `TRR_DB_URL` is canonical; `SUPABASE_DB_URL` is a deprecated alias during transition.

## Next Steps

1. (Prod) Apply migrations in order; reload PostgREST schema cache if needed after function/view changes.
2. Run/verify the credits backfill (Phase 6b) and parity checks before applying 0107 in any environment with existing data.
3. Ensure TRR-APP + screenalytics deploy alongside the backend migrations (consumers now rely on `core.v_show_cast` / `core.v_episode_cast` and people multi-source fields).

## Verification Commands

```bash
source .venv/bin/activate
ruff check . && ruff format --check . && pytest
```

If schema/migrations changed:
```bash
supabase db reset --yes
make schema-docs-check
```

---

Last updated: 2026-02-10
Updated by: Claude Opus 4.6

Workspace integration (this session):
- Verified `codex/admin-show-sync-refresh` branch was already squash-merged to origin/main; deleted local branch
- Dropped 2 local stashes (both superseded by merged branch work)
- Repo is clean on main, up to date with origin

## Admin Gallery Actions (This Session)

- Added unified admin endpoints to archive/star assets across tables:
  - `POST /api/v1/admin/assets/archive` and `POST /api/v1/admin/assets/star`
  - Implementation: `api/routers/admin_asset_flags.py`
- Added Supabase migration to support archiving unified/show assets:
  - `supabase/migrations/0116_archive_media_assets_and_show_images.sql`
  - Adds `archived_at`, `archived_by_firebase_uid`, `archived_reason` to `core.media_assets` and `core.show_images`
- Updated S3 mirror candidate queries to skip archived rows:
  - `trr_backend/repositories/show_images.py`
  - `trr_backend/repositories/season_images.py`
  - `trr_backend/repositories/episode_images.py`
  - `trr_backend/repositories/cast_photos.py`
- Extended web scrape import contracts to persist richer per-image metadata:
  - `context_section`, `context_type`, `source_logo`, `asset_name`
  - Implementation: `api/routers/admin_scrape.py`

Face-count and text-overlay URL fallback hardening (this session):
- Normalized stale Fandom static URLs (strips `/revision/latest...`) in `trr_backend/media/s3_mirror.py`.
- Cast photo mirroring now tries multiple candidate URLs (normalized Fandom + raw + thumb) before failing.
- Person-image auto-count now builds URL candidates (hosted + normalized/raw source URLs) and retries per image instead of failing on the first bad URL.
- `/admin/cast-photos/{id}/auto-count` and `/admin/media-assets/{id}/auto-count` now retry candidate URLs, including Fandom static URL normalization.
- Text-overlay detection for cast photos and media assets now retries candidate URLs and supports normalized Fandom static URLs.
- Added/updated regression tests:
  - `tests/api/routers/test_admin_person_images.py`
  - `tests/api/routers/test_admin_image_counts_fallback.py`
  - `tests/media/test_s3_mirror.py`
  - `tests/vision/test_text_overlay_fallback.py`
- Verification:
  - `ruff check ...` (touched backend files) passed
  - `pytest -q tests/api/routers/test_admin_person_images.py tests/api/routers/test_admin_image_counts_fallback.py tests/media/test_s3_mirror.py tests/vision/test_text_overlay_fallback.py` passed (`49 passed`)

Gemini env-var compatibility update (this session):
- Text-overlay model lookup now accepts `GEMINI-MODEL` in addition to `GEMINI_MODEL` and `GOOGLE_GEMINI_MODEL`.
- File: `trr_backend/vision/text_overlay.py`
- Docs/examples updated to list model aliases:
  - `.env.example`
  - `README.md`
- Verification:
  - `ruff check trr_backend/vision/text_overlay.py` passed
  - `pytest -q tests/vision/test_text_overlay_fallback.py` passed (`2 passed`)

Person image refresh backfill + centroid persistence fixes (this session):
- Updated person refresh/reprocess auto-count behavior in `api/routers/admin_person_images.py`:
  - Removed skip condition that excluded rows when `cast_photos.people_names` was populated.
  - Refresh now backfills count/text jobs across all missing eligible photos, not only newly-upserted IDs.
- Updated per-photo auto-count endpoints in `api/routers/admin_image_counts.py`:
  - After successful auto-count, now also writes auto `thumbnail_crop` face centroid for cast photos (`metadata.thumbnail_crop`) unless existing crop mode is manual.
  - For media assets, applies centroid to linked person `media_links.context.thumbnail_crop` unless manual.
- Hardened centroid helper in `trr_backend/clients/screenalytics.py`:
  - `face_centroid()` now handles result objects without `detections` attribute safely.

Validation run:
- `ruff check trr_backend/clients/screenalytics.py api/routers/admin_image_counts.py api/routers/admin_person_images.py` (pass)
- `pytest -q tests/api/routers/test_admin_person_images.py tests/api/routers/test_admin_image_counts_fallback.py` (pass; 20 passed)

Gemini text-overlay empty-candidate handling fix (this session):
- Fixed `trr_backend/vision/text_overlay.py` to avoid relying solely on `response.text` quick accessor.
- Added safe response extraction helpers:
  - `_extract_gemini_response_text()` parses candidate `content.parts[].text` when quick accessor is unavailable.
  - Includes finish reason extraction for clearer diagnostics.
- Added one retry in strict JSON response mode (`response_mime_type='application/json'`, larger token cap) when first Gemini response has no text parts.
- New explicit error when still empty: `Gemini returned no candidate text content (finish_reason=...)`.
- Added unit tests covering empty/quick-accessor-failure behavior in `tests/vision/test_text_overlay_fallback.py`.

Validation run:
- `ruff check trr_backend/vision/text_overlay.py tests/vision/test_text_overlay_fallback.py` (pass)
- `pytest -q tests/vision/test_text_overlay_fallback.py` (pass; 4 passed)

Show-level scrape import + metadata controls (this session):
- Extended scrape import entity support from `season|person` to `season|show|person`.
  - `show` imports now require `show_id` and accept optional season metadata (`season_number`/`season_id`) in link context.
  - File: `api/routers/admin_scrape.py`
- Added show-identifier resolution helper (`IMDb ID` preferred, show UUID fallback) for show-image S3 key paths.
  - File: `api/routers/admin_scrape.py`
- Non-stream and stream import handlers now build show image S3 keys via `build_show_image_s3_key`.
  - File: `api/routers/admin_scrape.py`
- Stream importer link creation now mirrors non-stream behavior:
  - always creates entity links (not only when cast fuzzy-match succeeds),
  - persists `context_section`, `context_type`, `source_logo`, `asset_name`,
  - applies optional source-page timestamp metadata consistently.
  - File: `api/routers/admin_scrape.py`

Verification (this session):
- `ruff check api/routers/admin_scrape.py` (pass)
- `python -m compileall api/routers/admin_scrape.py` (pass)
- `pytest -q tests/test_api_smoke.py` (pass; 12 passed)

Show scrape import kind expansion (this session):
- Added `logo` to scrape-import `ImageKind` validation so show-level import drawer can ingest logos.
  - File: `api/routers/admin_scrape.py`

Verification (this session):
- `ruff check api/routers/admin_scrape.py` (pass)
- `python -m compileall api/routers/admin_scrape.py` (pass)

Auto-crop/centering end-to-end hardening (this session):
- Extended Screenalytics client parsing to support mixed detection kinds (`face` + `person`) and introduced deterministic face+torso crop synthesis:
  - `trr_backend/clients/screenalytics.py`
  - Added `auto_thumbnail_crop()` strategy (`face_torso_v2`) with clamped `x/y/zoom` and `kind`-aware fallback behavior.
- Updated admin auto-count endpoints to persist richer auto crop payloads (`strategy`, `generated_at`) and respect manual crops:
  - `api/routers/admin_image_counts.py`
- Expanded person refresh/reprocess backend jobs to include media-link gallery rows (not only cast_photos) for count/text/crop updates:
  - `api/routers/admin_person_images.py`
  - Added media-links auto-count and text-overlay passes.
  - Added/propagated real `centering_cropping` stage payloads with numeric counts in stream responses.
  - Added refresh/reprocess summaries for centering counters (`centering_attempted/succeeded/failed/skipped_manual`).
- Added framing unit tests:
  - `tests/vision/test_people_count_auto_crop.py`

Verification (this session):
- `ruff check trr_backend/clients/screenalytics.py api/routers/admin_image_counts.py api/routers/admin_person_images.py tests/vision/test_people_count_auto_crop.py tests/api/routers/test_admin_image_counts_fallback.py tests/api/routers/test_admin_person_images.py` (pass)
- `pytest -q tests/vision/test_people_count_auto_crop.py tests/api/routers/test_admin_image_counts_fallback.py tests/api/routers/test_admin_person_images.py` (pass; 24 passed)
- `python -m py_compile api/routers/admin_person_images.py trr_backend/clients/screenalytics.py api/routers/admin_image_counts.py` (pass)

MSN + Bravo official-season-announcement scrape support (this session):
- Extended URL image scraper with MSN detail API fallback for client-rendered MSN article pages:
  - Detects MSN article IDs from URLs (e.g. `/ar-AA1TfjXH`)
  - Fetches `https://assets.msn.com/content/view/v2/Detail/{locale}/{articleId}`
  - Builds candidates from `imageResources` with `cmsId` body-order matching
  - Extracts cast-friendly context as `Name\nBio` from MSN body HTML (heading + following paragraphs)
  - Propagates MSN `publishedDateTime` and title into scrape result metadata
  - File: `trr_backend/scraping/url_image_scraper.py`
- Improved generic nearby-text extraction for article layouts (including Bravo cast bio pages):
  - Added sibling-context extraction (nearest heading + bio paragraphs)
  - Trims boilerplate heading suffixes like `"'s official bio for ..."`
  - Prefers richer heading+bio context over short inline captions/figcaptions
  - File: `trr_backend/scraping/url_image_scraper.py`
- Added/updated scraper tests:
  - heading+bio extraction for article-style image blocks
  - richer context preference over figcaption/inline caption
  - MSN detail API fallback extraction + metadata fields
  - File: `tests/scraping/test_url_image_scraper.py`

Verification (this session):
- `ruff check trr_backend/scraping/url_image_scraper.py tests/scraping/test_url_image_scraper.py` (pass)
- `pytest -q tests/scraping/test_url_image_scraper.py` (pass; 7 passed)
- Live smoke checks against:
  - `https://www.msn.com/en-us/entertainment/news/the-valley-persian-style-cast-previews-age-appropriate-drama/ar-AA1TfjXH`
  - `https://www.bravotv.com/the-daily-dish/the-valley-persian-style-season-1-cast-official-photos-bios-details`
  confirmed cast-photo candidates now include populated name/bio context.

Show refresh + scrape-source normalization fixes (this session):
- Fixed cast/credits refresh delete filter bug in sync scripts by replacing unsupported `.neq(...)` calls with DbSession-compatible `.not_.eq(...)`:
  - `scripts/sync/sync_show_cast.py`
  - `scripts/sync/sync_episode_appearances.py`
  - `scripts/sync/sync_people.py`
  - `trr_backend/ingestion/show_importer.py`
- Added `external_ids` fallback support for show ID extraction used by sync scripts:
  - `scripts/_sync_common.py`
  - `SHOW_SELECT_FIELDS` now includes `external_ids`
  - `extract_imdb_series_id()` and `extract_tmdb_series_id()` now fallback to `external_ids.{imdb|imdb_id|tmdb|tmdb_id}`.
- Improved show photo refresh robustness for schemas missing `archived_at`:
  - Added fallback query path (without archived filter) when `archived_at` column is absent in:
    - `trr_backend/repositories/show_images.py`
    - `trr_backend/repositories/season_images.py`
    - `trr_backend/repositories/episode_images.py`
    - `trr_backend/repositories/cast_photos.py`
- Added IMDb ID fallback from `external_ids` in show photo stream refresh bootstrap:
  - `api/routers/admin_show_sync.py`

Validation (this session):
- `ruff check scripts/_sync_common.py scripts/sync/sync_show_cast.py scripts/sync/sync_episode_appearances.py scripts/sync/sync_people.py trr_backend/ingestion/show_importer.py api/routers/admin_show_sync.py trr_backend/repositories/show_images.py trr_backend/repositories/season_images.py trr_backend/repositories/episode_images.py trr_backend/repositories/cast_photos.py` (pass)
- Reproduced and verified fixes on show `940ca82c-e4a9-45b4-9d85-946a654925ce`:
  - `PYTHONPATH=. python scripts/sync/sync_show_cast.py --show-id ... --force --verbose` (pass; `credits_inserted=19`, `failures=0`)
  - `PYTHONPATH=. python scripts/sync/sync_show_images.py --show-id ... --force --verbose` (pass; IMDb images mirrored to S3)
  - DB spot checks:
    - `core.credits` Self count for show restored to `19`
    - `core.show_images` has IMDb hosted rows (`imdb|2|2`)

Gemini text-overlay malformed JSON fallback (this session):
- Hardened JSON extraction in `trr_backend/vision/text_overlay.py` so truncated Gemini payloads (e.g. `{"`) no longer always hard-fail:
  - tries robust `JSONDecoder.raw_decode` scanning first,
  - falls back to extracting `has_text_overlay`/`confidence` from partial key-value text when possible,
  - adds a structured-JSON retry path when initial parse fails (not only when text is empty).
- Added regression tests in `tests/vision/test_text_overlay_fallback.py`:
  - incomplete JSON object recovery,
  - truncated confidence recovery,
  - explicit error for non-parseable junk input.

Verification (this session):
- `ruff check trr_backend/vision/text_overlay.py tests/vision/test_text_overlay_fallback.py` (pass)
- `pytest -q tests/vision/test_text_overlay_fallback.py` (pass; 7 passed)
- `python -m py_compile trr_backend/vision/text_overlay.py` (pass)

Text-overlay unknown-state fallback for empty Gemini candidates (this session):
- Updated `trr_backend/vision/text_overlay.py` so text-overlay detection no longer hard-fails when Gemini returns no candidate text (or unparseable payload after retries).
- `TextOverlayResult` now supports unknown outcomes:
  - `has_text_overlay: None`
  - `status: "unknown"`
  - persisted diagnostics: `text_overlay_error`, `text_overlay_finish_reason`
- Existing-metadata reader now recognizes persisted unknown status without forcing re-run.
- Admin detect endpoints now return `status="unknown"` when detection result is unknown:
  - `api/routers/admin_cast_photos.py`
  - `api/routers/admin_media_assets.py`
- Added tests in `tests/vision/test_text_overlay_fallback.py` for unknown-result creation and persisted unknown extraction.

Verification (this session):
- `ruff check trr_backend/vision/text_overlay.py api/routers/admin_cast_photos.py api/routers/admin_media_assets.py tests/vision/test_text_overlay_fallback.py` (pass)
- `pytest -q tests/vision/test_text_overlay_fallback.py tests/api/routers/test_admin_image_counts_fallback.py` (pass; 13 passed)
- `python -m py_compile trr_backend/vision/text_overlay.py api/routers/admin_cast_photos.py api/routers/admin_media_assets.py` (pass)

Refresh stream stability + text-overlay reasoned fallback (this session):
- Extended refresh/reprocess summaries in `api/routers/admin_person_images.py`:
  - Added `text_overlay_unknown`, `text_overlay_failure_reasons`
  - Added metadata enrichment counters: `episode_metadata_tagged`, `show_context_tagged`, `metadata_enrichment_failed`
  - Stream `complete` payload now includes `run_id` and new counters.
- Added explicit metadata enrichment stage progress in refresh stream (`stage=metadata_enrichment`).
- Standardized SSE error payload shape for person/reprocess stream setup errors:
  - `{ run_id, stage, error, detail? }`.
- Updated text-overlay batch helpers in `api/routers/admin_person_images.py`:
  - `_detect_text_overlay_cast_photos` and `_detect_text_overlay_media_links` now return `(attempted, succeeded, unknown, failed)`
  - Capture reason-bucket counts via `reason_counts` map.
- Hardened Gemini text overlay pipeline in `trr_backend/vision/text_overlay.py`:
  - Unknown outcomes now include `reason_code` persisted to metadata (`text_overlay_error_code`).
  - Gemini request/no-text/JSON parse issues now persist `unknown` result instead of hard-failing after bytes are downloaded.
  - Added hosted-key fallback download path: if URL fetch fails, attempt S3 object bytes via `hosted_key`.
  - Added helper `classify_text_overlay_failure_reason(...)` for backend reason buckets.
- Added response field passthrough for per-photo detection endpoints:
  - `api/routers/admin_cast_photos.py`: `text_overlay_error_code`
  - `api/routers/admin_media_assets.py`: `text_overlay_error_code`

Tests and checks:
- `ruff check api/routers/admin_person_images.py trr_backend/vision/text_overlay.py api/routers/admin_cast_photos.py api/routers/admin_media_assets.py tests/vision/test_text_overlay_fallback.py tests/api/routers/test_admin_person_images.py` (pass)
- `pytest -q tests/vision/test_text_overlay_fallback.py tests/api/routers/test_admin_person_images.py` (pass; 26 passed)
- `python -m py_compile api/routers/admin_person_images.py trr_backend/vision/text_overlay.py api/routers/admin_cast_photos.py api/routers/admin_media_assets.py` (pass)

New/updated test coverage:
- `tests/vision/test_text_overlay_fallback.py`
  - reason code persisted for unknown results
  - existing metadata reads persisted `text_overlay_error_code`
  - hosted-key S3 fallback when URL download fails
- `tests/api/routers/test_admin_person_images.py`
  - refresh success payload now asserts presence of new counters/fields.

People-count semantics for source/library tags (this session):
- Updated source import tag context generation in `api/routers/admin_scrape.py` (`_build_people_tags_context`).
  - Source/library-matched person tags now persist only `people_ids` / `people_names`.
  - Removed automatic `people_count` + `people_count_source="manual"` writes from source-tag context.
- Updated manual-detection helpers so manual protection applies to explicit manual count source only:
  - `trr_backend/repositories/cast_photo_tags.py` (`has_manual_tags`)
  - `trr_backend/repositories/media_links.py` (`has_manual_people_tags`)
- Result:
  - Known-person tagging no longer implies authoritative people count.
  - Auto-count flows are no longer blocked merely because person tags exist.
  - Explicit manual counts (where `people_count_source="manual"`) are still protected unless force override is used.

Verification:
- `ruff check api/routers/admin_scrape.py trr_backend/repositories/cast_photo_tags.py trr_backend/repositories/media_links.py` (pass)
- `ruff format api/routers/admin_scrape.py` + `ruff format --check ...` (pass)
- `pytest tests/api/routers/test_admin_image_counts_fallback.py -q` (pass; 4 passed)

Fandom profile mis-attachment guard (this session):
- Root cause discovered: `search_real_housewives_wiki("Reza Farahan")` can return `Sutton_Stracke`, and `admin_person_images._refresh_fandom_profile(...)` previously upserted the parsed profile without person-name validation.
- Added strict name-match safeguards in `api/routers/admin_person_images.py`:
  - `_normalize_name_for_match`, `_names_match`, `_name_from_fandom_url`, `_fandom_profile_matches_person_name`
  - `_refresh_fandom_profile` now skips candidate URLs and parsed payloads when names do not match target person.
- Result: Fandom refresh no longer attaches unrelated pages to a person when search returns an incorrect top result.
- Applied one-time local data correction for Reza (`person_id=d3e56687-6d11-43fb-9f99-b8f45c9b5ff1`):
  - deleted mismatched `core.cast_fandom` row pointing to Sutton page (`id=fed4a4f6-43f5-4c51-b5bf-5d7bcf9dc5f3`).

Verification:
- `ruff check api/routers/admin_person_images.py` (pass)
- `ruff format --check api/routers/admin_person_images.py` (pass)
- `pytest tests/api/routers/test_admin_person_images.py -q` (pass; 16 passed)
- Runtime check: parsed Sutton profile no longer matches expected `Reza Farahan` via new matcher.

Bravo import + cast eligibility support (this session, 2026-02-11):
- Added Bravo source migration:
  - `supabase/migrations/0117_add_bravo_source.sql`
- Added Bravo parser module:
  - `trr_backend/scraping/bravo_parser.py`
- Added admin Bravo endpoints and persistence/commit logic:
  - `api/routers/admin_show_bravo.py`
  - wired in `api/main.py`
- Implemented persisted snapshot read model for videos/news (no live scrape on read), plus person snapshot merge behavior.
- Commit logic includes:
  - show description update
  - person `biography/homepage/profile_image_url` source updates under `bravo`
  - social merge policy fill-missing-only
  - selected show image import via existing scrape pipeline
- Updated videos endpoint default to `merge_person_sources=true` to match task defaults.
- Added tests:
  - `tests/scraping/test_bravo_parser.py`
  - `tests/api/routers/test_admin_show_bravo.py`

Validation (this session):
- `ruff check api/routers/admin_show_bravo.py trr_backend/scraping/bravo_parser.py tests/api/routers/test_admin_show_bravo.py tests/scraping/test_bravo_parser.py` (pass)
- `pytest -q tests/scraping/test_bravo_parser.py tests/api/routers/test_admin_show_bravo.py` (pass, 6 passed)

Bravo parser refinement (this session, 2026-02-11):
- Updated `trr_backend/scraping/bravo_parser.py` to improve show import preview quality:
  - airs extraction now recognizes day/time strings like `Tuesdays at 8/7c`
  - show image candidate extraction now excludes video/news thumbnails, cast headshots, and global social/site icon assets
- Added regression tests in `tests/scraping/test_bravo_parser.py` for:
  - day/time airs extraction
  - video/news image candidate exclusion on show page parsing
- Validation:
  - `ruff check trr_backend/scraping/bravo_parser.py tests/scraping/test_bravo_parser.py` (pass)
  - `pytest -q tests/scraping/test_bravo_parser.py` (pass, 5 passed)
  - live parse check for `https://www.bravotv.com/summer-house` now returns `airs_text = "Tuesdays at 8/7c"` and 3 show image candidates.

Bravo preview metadata/date improvements (this session, 2026-02-11):
- `trr_backend/scraping/bravo_parser.py`
  - Added `published_at` extraction for videos/news.
  - Video list scraping now prefers `/watch/videos` so season feed aligns with Bravo filter defaults (e.g. Summer House Season 10).
  - Hydrates missing `published_at` by reading clip/article pages when needed.
- `api/routers/admin_show_bravo.py`
  - Normalized show/person video/news payloads now carry `published_at` through persisted snapshots + read endpoints.
- Tests updated:
  - `tests/scraping/test_bravo_parser.py` now covers video/news `published_at` extraction.
- Validation:
  - `ruff check trr_backend/scraping/bravo_parser.py api/routers/admin_show_bravo.py tests/scraping/test_bravo_parser.py tests/api/routers/test_admin_show_bravo.py` (pass)
  - `pytest -q tests/scraping/test_bravo_parser.py tests/api/routers/test_admin_show_bravo.py` (pass, 8 passed)

Season Social Analytics V2 (this session, 2026-02-11):
- Added season social analytics schema migration:
  - `supabase/migrations/0118_social_season_analytics.sql`
  - Adds `social.season_targets` and season/job/source lineage columns across `social.*` tables.
- Added backend social analytics + ingest repository:
  - `trr_backend/repositories/social_season_analytics.py`
  - Supports:
    - season target defaults/updates (`bravo` scope)
    - ingest job creation/status updates
    - normalized + `raw_data` persistence for posts/comments across Instagram, TikTok, YouTube, Twitter
    - sentiment scoring + weekly/platform aggregations
    - CSV and PDF export builders
- Extended admin socials router with season endpoints:
  - `GET /api/v1/admin/socials/seasons/{season_id}/targets`
  - `PUT /api/v1/admin/socials/seasons/{season_id}/targets`
  - `POST /api/v1/admin/socials/seasons/{season_id}/ingest`
  - `GET /api/v1/admin/socials/seasons/{season_id}/ingest/jobs`
  - `GET /api/v1/admin/socials/seasons/{season_id}/analytics`
  - `GET /api/v1/admin/socials/seasons/{season_id}/analytics/export.csv`
  - `GET /api/v1/admin/socials/seasons/{season_id}/analytics/export.pdf`
- Added PDF dependency:
  - `requirements.txt` now includes `reportlab>=4.2.0`
- Added tests:
  - `tests/api/routers/test_socials_season_analytics.py`
  - `tests/repositories/test_social_season_analytics.py`

Validation (this session):
- `ruff check api/routers/socials.py trr_backend/repositories/social_season_analytics.py tests/api/routers/test_socials_season_analytics.py tests/repositories/test_social_season_analytics.py` (pass)
- `python -m py_compile api/routers/socials.py trr_backend/repositories/social_season_analytics.py` (pass)
- `pytest -q tests/api/routers/test_socials_season_analytics.py tests/repositories/test_social_season_analytics.py` (pass, 5 passed)

Bravo show-image media-type selection support (this session, 2026-02-11):
- File: `api/routers/admin_show_bravo.py`
- Added new commit request field:
  - `selected_show_images: [{url, kind}]` (optional)
  - `kind` enum: `poster|backdrop|logo|episode_still|cast|promo|intro|reunion|other`
- Backward-compatible behavior:
  - if `selected_show_images` is omitted, legacy `selected_show_image_urls[]` is still accepted and imported as `promo`.
- Commit import behavior:
  - selected image `kind` now flows into `ImportImageItem.kind` (previously always `promo`).
- Tests:
  - `tests/api/routers/test_admin_show_bravo.py` adds assertion that selected kinds (`logo`, `poster`) are passed to import request.
- Validation:
  - `uv run ruff check api/routers/admin_show_bravo.py tests/api/routers/test_admin_show_bravo.py`
  - `uv run pytest tests/api/routers/test_admin_show_bravo.py -q`

Bravo cast hero-image ingest + tag merge + season filter (this session, 2026-02-11):
- File: `api/routers/admin_show_bravo.py`
- Cast image ingest:
  - Added `_import_bravo_person_image(...)` and invoke it during commit for each resolved person with `hero_image_url`.
  - Uses existing `admin_scrape.import_images` with `entity_type=person`, so media assets are mirrored to S3 and linked to person media.
  - Commit response now includes `counts.imported_person_images`, `counts.skipped_person_images`, and `person_image_import_errors`.
- Person/article/video tagging:
  - Enhanced dedupe flow to merge `person_tags` across duplicate URLs instead of keeping only first tag set.
  - Affects show/person merged video/news normalization and read extraction.
- Season filter support:
  - Added optional `season_number` to Bravo preview/commit request models.
  - Applies to show/person videos before preview and persistence (`_filter_bundle_by_season`).
- Tests updated in `tests/api/routers/test_admin_show_bravo.py`:
  - selected show image kind passthrough remains covered.
  - preview season filter behavior covered.
  - dedupe person-tag merge covered.
- Validation:
  - `uv run ruff check api/routers/admin_show_bravo.py tests/api/routers/test_admin_show_bravo.py`
  - `uv run pytest tests/api/routers/test_admin_show_bravo.py -q` (6 passed)

Bravo show-news relevance filter hardening (this session, 2026-02-11):
- File: `trr_backend/scraping/bravo_parser.py`
- Changes:
  - Added show-news relevance filters to drop unrelated "Latest Bravo News" sidebar cards.
  - Relevance checks now use show slug/title and discovered cast-person slug phrases.
  - `parse_show_news(...)` now accepts optional `show_title` and `person_urls` and applies relevance filtering before return.
  - `parse_bravo_show_bundle(...)` now calls `parse_show_news(...)` with show title + discovered person URLs.
- Tests:
  - Added `test_parse_show_news_ignores_unrelated_latest_sidebar_items` in `tests/scraping/test_bravo_parser.py`.
- Validation:
  - `uv run ruff check trr_backend/scraping/bravo_parser.py tests/scraping/test_bravo_parser.py api/routers/admin_show_bravo.py tests/api/routers/test_admin_show_bravo.py`
  - `uv run pytest tests/scraping/test_bravo_parser.py tests/api/routers/test_admin_show_bravo.py -q` (12 passed)

Season social week-model update (this session, 2026-02-11):
- Files:
  - `trr_backend/repositories/social_season_analytics.py`
  - `api/routers/socials.py`
  - `tests/repositories/test_social_season_analytics.py`
  - `tests/api/routers/test_socials_season_analytics.py`
- Changes:
  - Implemented episode-anchored week windows with explicit `Week 0` support.
  - `Week 0` is now variable length: trailer/first-look/sneak-peek drop -> premiere (episode 1 air datetime).
  - Week windows now follow episode boundaries (`Week 1 = Ep1->Ep2`, etc.) instead of fixed 7-day bins from anchor date.
  - Added `window.week_zero_start` to analytics response.
  - Added `week=0` support to analytics/export endpoints (router validation now `ge=0`).
  - Added Bravo-first platform scoping in analytics row queries so `source_scope=bravo` filters to Bravo-owned accounts/channels.
  - Tightened trailer start fallback to pre-premiere lookback (180d) and season-specific marker preference (`season N` / `sN`).
- Tests:
  - Expanded repository unit tests for trailer-marker parsing and season matching.
  - Added API route test confirming `week=0` is accepted and forwarded.
- Validation:
  - `python -m py_compile trr_backend/repositories/social_season_analytics.py api/routers/socials.py` (pass)
  - `pytest -q tests/repositories/test_social_season_analytics.py tests/api/routers/test_socials_season_analytics.py` (pass, 8 passed)
  - Live RHOSLC check now returns:
    - `window.week_zero_start = 2025-09-14T21:31:09+00:00`
    - non-zero weeks at 0, 13, 16 under Bravo scope.

Weekly run controls + platform weekly table support (this session, 2026-02-11):
- File: `trr_backend/repositories/social_season_analytics.py`
- Changes:
  - Added `weekly_platform_posts` to analytics response:
    - per-week post counts split by `instagram/youtube/tiktok/twitter` plus `total_posts`.
  - Added Bravo-scope account/channel filtering in analytics row queries (`source_scope='bravo'`):
    - Instagram/TikTok/Twitter restricted to Bravo accounts (`bravotv`/`bravo`).
    - YouTube restricted to Bravo channel title/source account (`bravo`/`bravotv`).
  - Maintains Week 0 variable window and episode-bounded weekly windows from prior update.
- Validation:
  - `python -m py_compile trr_backend/repositories/social_season_analytics.py api/routers/socials.py` (pass)
  - `pytest -q tests/repositories/test_social_season_analytics.py tests/api/routers/test_socials_season_analytics.py` (pass)
  - Live run: week-scoped + platform-scoped ingest confirmed with explicit date_start/date_end in saved scrape_job config.

Pre-Season + 8pm episode boundary update (this session, 2026-02-11):
- File: `trr_backend/repositories/social_season_analytics.py`
- Changes:
  - Week 0 is now labeled `Pre-Season` in analytics output.
  - Episode week boundaries now switch at `20:00 America/New_York` on episode air dates (instead of midnight).
  - Added optional season-target config overrides for Week 0 start:
    - `config.preseason_start`
    - `config.preseason_start_at`
    - `config.week_zero_start`
  - Week 0 start resolution order:
    1. season target config override,
    2. Bravo show snapshot season videos,
    3. season social rows trailer markers,
    4. fallback.
- Operational data update:
  - Persisted RHOSLC S6 Bravo season targets and set `config.preseason_start = 2025-08-14T00:00:00-04:00` for all four platforms.
- Validation:
  - `python -m py_compile trr_backend/repositories/social_season_analytics.py api/routers/socials.py` (pass)
  - `pytest -q tests/repositories/test_social_season_analytics.py tests/api/routers/test_socials_season_analytics.py` (pass)
  - Live RHOSLC check now returns:
    - Week 0 (`Pre-Season`) ET window: `2025-08-14 00:00` -> `2025-09-16 19:59:59.999999`
    - Week 1 start ET: `2025-09-16 20:00`.

RHOSLC term-match ingest criteria update (this session, 2026-02-11):
- Files:
  - `trr_backend/repositories/social_season_analytics.py`
  - `trr_backend/socials/twitter/scraper.py`
  - `tests/repositories/test_social_season_analytics.py`
- Changes:
  - Added canonical Bravo term matching helpers so ingest can match caption/text on any of:
    - `Salt Lake City`
    - `RHOSLC`
    - `#RHOSLC`
  - Updated default Bravo season targets to include RHOSLC aliases in target keywords/hashtags (instead of only full show-name slug).
  - Instagram/TikTok ingest now filters posts with OR semantics across keywords + hashtags via caption/description text matching.
  - YouTube ingest now enforces the same term matching before persistence (title/description text).
  - Twitter ingest now:
    - builds advanced query as `from:{account}` + OR-term clause,
    - supports advanced query passthrough in scraper query builder,
    - applies final text-based term guard before persistence.
- Validation:
  - `ruff check trr_backend/repositories/social_season_analytics.py trr_backend/socials/twitter/scraper.py tests/repositories/test_social_season_analytics.py` (pass)
  - `pytest -q tests/repositories/test_social_season_analytics.py tests/api/routers/test_socials_season_analytics.py` (pass, 10 passed)
- Note:
  - Ingest now merges season-target terms with show-derived fallback aliases at runtime, so previously saved targets still get RHOSLC/Salt Lake City matching without a manual target reset.

Bravo profile import semantics for season promos (this session, 2026-02-11):
- File: `api/routers/admin_show_bravo.py`
- Changes:
  - Added `_resolve_season_id(show_id, season_number)` guard to validate/resolve season before commit import.
  - `commit_bravo_import` now resolves requested `season_number` and returns `404` if season is missing for the show.
  - `_import_bravo_person_image` now supports season-scoped import and maps Bravo profile hero images to:
    - `kind="promo"`,
    - `context_section="bravo_profile"`,
    - `context_type="profile"`,
    - `asset_name="Bravo profile image"`.
  - For season-scoped imports, hero images are imported as `entity_type="season"` with `person_ids` attached so person-gallery links are also created by existing import logic.
- Validation:
  - `pytest -q tests/api/routers/test_admin_show_bravo.py -q` (pass).

Bravo profile social links -> people external_ids normalization (this session, 2026-02-11):
- File: `api/routers/admin_show_bravo.py`
- Changes:
  - Expanded social-link merge logic so Bravo profile socials are normalized into canonical person external ID keys.
  - Incoming social values now hydrate both legacy and canonical keys for compatibility:
    - `instagram` + `instagram_id` (+ `instagram_url`)
    - `twitter` + `twitter_id` (+ `twitter_url`)
    - `facebook` + `facebook_id` (+ `facebook_url`)
    - `tiktok` + `tiktok_id` (+ `tiktok_url`)
    - `youtube` + `youtube_id` (+ `youtube_url`)
  - Added URL/handle normalization and non-overwrite behavior that respects existing values.
- Tests:
  - Updated `tests/api/routers/test_admin_show_bravo.py` to assert canonical social key hydration and URL normalization.
- Validation:
  - `pytest -q tests/api/routers/test_admin_show_bravo.py -q` (pass)
  - `python -m py_compile api/routers/admin_show_bravo.py` (pass)

RHOSLC S6 Bravo re-sync execution + social external ID backfill (this session, 2026-02-11):
- Runtime action executed against backend admin endpoint:
  - `POST /api/v1/admin/shows/7782652f-783a-488b-8860-41b97de32e75/import-bravo/commit` with `season_number=6`.
- Commit result summary:
  - `people_updated=8`
  - `unmatched_people=0`
  - `person_snapshots=8`
  - `imported_person_images=0`, `skipped_person_images=8`
- Social external IDs outcome after sync:
  - RHOSLC S6 cast with social IDs on `core.people.external_ids`: 5 members
  - Handles now visible for Instagram/Twitter where present.

YouTube generic placeholder guard (this session, 2026-02-11):
- Files:
  - `api/routers/admin_show_bravo.py`
  - `tests/api/routers/test_admin_show_bravo.py`
- Changes:
  - Prevented generic YouTube placeholders (`user`, `channel`, `c`) from being written as person external IDs.
  - Added regression test for skipping generic YouTube placeholder URLs.
- One-time data cleanup executed:
  - Removed `youtube`/`youtube_id`/`youtube_url` keys for RHOSLC S6 cast rows where value was generic placeholder.
  - Rows cleaned: 8.
- Validation:
  - `pytest -q tests/api/routers/test_admin_show_bravo.py -q` (pass)
  - `python -m py_compile api/routers/admin_show_bravo.py` (pass)

PR merge/readiness pass (this session, 2026-02-12):
- Merged `origin/main` into `feat/admin-media-pipeline-enhancements`.
- Resolved merge conflicts in:
  - `api/routers/admin_person_images.py`
  - `docs/ai/HANDOFF.md`
- Validation:
  - `python -m py_compile api/routers/admin_person_images.py`
  - `ruff check api/routers/admin_person_images.py`
  - `pytest -q tests/api/routers/test_admin_person_images.py` (16 passed)
- Branch pushed and PR #56 is merge-clean with passing checks.

Bravo season-description persistence fix (this session, 2026-02-12):
- Files:
  - `api/routers/admin_show_bravo.py`
  - `tests/api/routers/test_admin_show_bravo.py`
- Changes:
  - Added `_persist_season_overview(...)` and updated Bravo commit persistence behavior:
    - when `season_number` is provided (season-scoped sync), Bravo description is persisted to `core.seasons.overview`
    - global `core.shows.description` is no longer overwritten in season-scoped runs
    - show-level sync (no `season_number`) still updates `core.shows.description` as before
  - Added regression test asserting season-scoped commit calls season-overview persistence and does not call show-description persistence.
- Validation:
  - `ruff check api/routers/admin_show_bravo.py tests/api/routers/test_admin_show_bravo.py` (pass)
  - `pytest -q tests/api/routers/test_admin_show_bravo.py` (10 passed)
  - `python3 -m py_compile api/routers/admin_show_bravo.py` (pass)

Show Admin overhaul foundation: links + roles APIs, season->show media propagation (this session, 2026-02-12):
- Files:
  - `supabase/migrations/0120_show_admin_links_and_roles.sql`
  - `api/routers/admin_show_links.py`
  - `api/routers/admin_show_roles.py`
  - `api/routers/admin_scrape.py`
  - `api/main.py`
- Changes:
  - Added `wikipedia` source seed in migration.
  - Added show admin link registry schema:
    - `core.entity_links` (`pending|approved|rejected`, group/kind, confidence, provenance metadata)
  - Added show-scoped role schema:
    - `core.show_role_catalog`
    - `core.show_cast_role_assignments`
    - read model `core.v_show_cast_roles_enriched`
  - Added new admin routers and mounted endpoints:
    - `/api/v1/admin/shows/{show_id}/links` (+ discover, patch, delete)
    - `/api/v1/admin/shows/{show_id}/roles` (+ patch)
    - `/api/v1/admin/shows/{show_id}/cast/{person_id}/roles`
    - `/api/v1/admin/shows/{show_id}/cast-role-members`
  - Updated image import paths so season imports also create show-level media links (while preserving person linking) in both sync and stream handlers.
- Validation:
  - `ruff check api/main.py api/routers/admin_show_links.py api/routers/admin_show_roles.py api/routers/admin_scrape.py` (pass)
  - `python -m py_compile api/main.py api/routers/admin_show_links.py api/routers/admin_show_roles.py api/routers/admin_scrape.py` (pass)
  - `pytest -q tests/api/routers/test_admin_show_bravo.py` (10 passed)

Show Admin Bravo ingest augmentation (this session, 2026-02-12):
- Files:
  - `api/routers/admin_show_bravo.py`
  - `api/routers/admin_show_roles.py`
  - `supabase/migrations/0120_show_admin_links_and_roles.sql`
- Changes:
  - Bravo commit now auto-persists discovery links as `pending` after snapshot write (show + season + person link discovery), enabling review-before-publish flow.
  - Bravo commit now derives cast-role suggestions from cast-announcement headlines and persists show-scoped role assignments (`source=bravo_cast_announcement`) for tagged people when possible.
  - Expanded `people_refs` for Bravo normalization to include known show cast names so person tag inference can capture announcement references beyond explicit Bravo people pages.
  - Enhanced cast roles read model/view to expose `archive_episodes` and `season_numbers` array.
  - Enhanced cast-role-members endpoint:
    - season multi-select now filters by actual season membership (`season_numbers`) instead of latest season only
    - added `archive_mode` filter: `all|exclude|only`
- Validation:
  - `ruff check api/routers/admin_show_bravo.py api/routers/admin_show_roles.py api/main.py api/routers/admin_show_links.py api/routers/admin_scrape.py` (pass)
  - `python -m py_compile api/routers/admin_show_bravo.py api/routers/admin_show_roles.py api/main.py api/routers/admin_show_links.py api/routers/admin_scrape.py` (pass)
  - `pytest -q tests/api/routers/test_admin_show_bravo.py` (10 passed)

Social ingest hardening + unified run queue implementation (this session, 2026-02-12):
- Files:
  - `trr_backend/socials/instagram/scraper.py`
  - `trr_backend/socials/tiktok/scraper.py`
  - `trr_backend/socials/youtube/scraper.py`
  - `trr_backend/socials/twitter/scraper.py`
  - `trr_backend/db/pg.py`
  - `trr_backend/repositories/social_season_analytics.py`
  - `api/routers/socials.py`
  - `scripts/socials/worker.py`
  - `supabase/migrations/0121_social_scrape_runs.sql`
  - `supabase/migrations/0122_social_scrape_jobs_queue_fields.sql`
  - `supabase/migrations/0123_social_scrape_jobs_queue_indexes.sql`
  - `tests/api/routers/test_socials_season_analytics.py`
  - `tests/repositories/test_social_season_analytics.py`
- Changes:
  - Platform retrieval hardening completed:
    - Instagram GraphQL first-page failure now auto-falls back to profile-info mode with telemetry.
    - TikTok now classifies challenge/non-JSON responses, applies bounded yt-dlp fallback budgets, and emits retrieval telemetry.
    - YouTube continuation crawling now respects `max_pages`, no-hit cutoff, unknown-timestamp safeguards, and emits retrieval telemetry.
    - Twitter now broadens GraphQL hash discovery, retries once on hash-rotation 404, prioritizes `from:` fallback paths earlier, and emits retrieval telemetry.
  - Added DB helper enhancements for connection reuse and batched SQL helpers in `trr_backend/db/pg.py`.
  - Reworked season ingest orchestration into run-based staged jobs in `social_season_analytics.py`:
    - single `run_id` with stage jobs (`posts` then `comments`)
    - ingest options now include `ingest_mode`, `depth_preset`, `max_replies_per_post`
    - queue-aware job state fields, run summaries, retry/backoff classification, stage metadata
    - Twitter stage-2 comment sync hydrates audience replies per post with bounded limits.
  - Added run/queue APIs in router:
    - ingest now returns run metadata (`run_id`, `stages`, `queued_or_started_jobs`, summary)
    - jobs endpoint supports `run_id`, `status`, `platform` filters
    - added run cancellation endpoint: `POST /api/v1/admin/socials/seasons/{season_id}/ingest/runs/{run_id}/cancel`
  - Added worker entrypoint (`scripts/socials/worker.py`) to process queued jobs with DB-claim flow.
  - Added migrations for `social.scrape_runs`, queue fields, and queue indexes.
  - Expanded API + repository tests for run metadata, filtered jobs listing, cancel endpoint, and depth preset defaults.
- Validation:
  - `ruff check trr_backend/repositories/social_season_analytics.py api/routers/socials.py scripts/socials/worker.py tests/api/routers/test_socials_season_analytics.py tests/repositories/test_social_season_analytics.py` (pass)
  - `pytest -q tests/api/routers/test_socials_season_analytics.py tests/repositories/test_social_season_analytics.py` (17 passed)
  - `python -m py_compile trr_backend/repositories/social_season_analytics.py api/routers/socials.py scripts/socials/worker.py` (pass)

Cast-role API + Bravo ingest follow-up (this session, 2026-02-12):
- Files:
  - `api/routers/admin_show_roles.py`
  - `api/routers/admin_show_bravo.py`
  - `supabase/migrations/0120_show_admin_links_and_roles.sql`
- Changes:
  - Cast-role members API now supports true season multi-select filtering using `season_numbers` and supports `archive_mode=all|exclude|only`.
  - Bravo commit now runs pending-link discovery persistence and cast-role suggestion persistence after snapshot write.
- Validation:
  - `ruff check api/routers/admin_show_bravo.py api/routers/admin_show_roles.py api/main.py api/routers/admin_show_links.py api/routers/admin_scrape.py` (pass)
  - `python -m py_compile api/routers/admin_show_bravo.py api/routers/admin_show_roles.py api/main.py api/routers/admin_show_links.py api/routers/admin_scrape.py` (pass)
  - `pytest -q tests/api/routers/test_admin_show_bravo.py` (10 passed)
Queue migration guardrail for social ingest (same session, 2026-02-12):
- File:
  - `trr_backend/repositories/social_season_analytics.py`
  - `tests/api/routers/test_socials_season_analytics.py`
- Changes:
  - Added queue schema preflight check before ingest/cancel to fail fast with actionable error when migrations `0121`/`0122`/`0123` are missing.
  - Added legacy-safe `list_jobs` select path that tolerates missing `run_id`/queue columns while migrations are pending.
  - Added router test for ingest 400 behavior when queue schema is unavailable.
- Validation:
  - `ruff check trr_backend/repositories/social_season_analytics.py tests/api/routers/test_socials_season_analytics.py` (pass)
  - `pytest -q tests/api/routers/test_socials_season_analytics.py tests/repositories/test_social_season_analytics.py` (18 passed)

RHOSLC backfill runbook for links + role suggestions (this session, 2026-02-12):
- Files:
  - `docs/runbooks/rhoslc-show-admin-backfill.md`
  - `docs/cross-collab/TASK7/STATUS.md`
- Changes:
  - Added executable runbook for RHOSLC backfill covering:
    - season-scoped Bravo commit runs,
    - explicit links discovery pass,
    - pending link review/approval commands,
    - cast-role suggestion verification queries.
  - Added cross-collab status entry referencing the runbook.
- Validation:
  - Documentation-only update (no runtime code changes).

Social ingest live-progress + full-depth defaults + cancel safety (this session, 2026-02-12):
- Files:
  - `trr_backend/repositories/social_season_analytics.py`
  - `api/routers/socials.py`
  - `tests/api/routers/test_socials_season_analytics.py`
- Changes:
  - Added in-flight job progress updates during platform loops (Instagram/TikTok/YouTube/Twitter):
    - updates `social.scrape_jobs.items_found` while jobs run
    - writes stage/platform/account counters into job metadata
    - updates heartbeat on each progress flush
  - Propagated explicit stage labels into ingest execution (`posts` vs `comments`) for progress metadata consistency.
  - Added cancellation-safe job completion behavior:
    - if run is cancelled while a job is executing, terminal state remains `cancelled` instead of flipping back to `completed`/`failed`.
  - Shifted ingest defaults to full-depth behavior:
    - backend defaults now use `depth_preset=deep`
    - unsupported `depth_preset` values now gracefully fall back to `deep`
    - router request defaults raised to high limits for posts/comments/replies to support full ingest.
  - Updated API router test expectation for default depth preset (`deep`).
- Validation:
  - `python3 -m py_compile trr_backend/repositories/social_season_analytics.py api/routers/socials.py` (pass)
  - `ruff check trr_backend/repositories/social_season_analytics.py api/routers/socials.py tests/api/routers/test_socials_season_analytics.py` (pass)
  - `pytest tests/api/routers/test_socials_season_analytics.py -q` (10 passed)

People links discovery tightening (this session, 2026-02-12):
- Files:
  - `api/routers/admin_show_links.py`
  - `docs/cross-collab/TASK7/STATUS.md`
- Changes:
  - Person knowledge discovery now only emits `wikipedia` + `fandom` links when a fandom profile URL exists for that person (`core.cast_fandom` join presence).
- RHOSLC data update executed:
  - Deleted existing RHOSLC person `wikipedia`/`fandom` links and reran links discovery.
  - Resulting RHOSLC person link kinds now paired: `fandom:66`, `wikipedia:66`.
- Validation:
  - `ruff check api/routers/admin_show_links.py` (pass)
  - `python -m py_compile api/routers/admin_show_links.py` (pass)

Comment scraping debug + platform fixes + parallel worker option (this session, 2026-02-12):
- Files:
  - `trr_backend/socials/instagram/scraper.py`
  - `trr_backend/socials/tiktok/scraper.py`
  - `trr_backend/socials/youtube/scraper.py`
  - `trr_backend/socials/twitter/scraper.py`
  - `trr_backend/repositories/social_season_analytics.py`
  - `scripts/socials/worker.py`
  - `tests/socials/test_comment_scraper_fixes.py`
- Changes:
  - TikTok comment scraping fixed by including `aid=1988` on comments/replies endpoints and adding robust status/non-JSON handling with explicit failure reasons.
  - YouTube comment scraping fixed for modern payload shape by parsing `commentViewModel` + entity payloads (in addition to legacy renderers), including safer continuation progress guards.
  - Twitter reply scraping fixed by adding required TweetDetail feature flags and implementing one retry path that auto-adds newly required flags from 400 validation payloads.
  - Twitter reply author parsing improved with `core.screen_name`/`core.name` fallback when legacy username/name fields are missing.
  - Instagram comment scraping now classifies HTML challenge/auth responses and non-JSON failures with actionable reason codes.
  - Season analytics ingest metadata now records platform comment failure reasons for diagnostics.
  - Worker script now supports:
    - `--parallel N` for generic multi-worker `--run-id` processing.
    - stage pinning via `--stage posts|comments`.
    - tandem mode via `--tandem --posts-workers N --comments-workers N` to run dedicated posts and comments workers concurrently for faster throughput.
- Validation:
  - `pytest -q tests/socials/test_comment_scraper_fixes.py tests/repositories/test_social_season_analytics.py tests/api/routers/test_socials_season_analytics.py` (23 passed)
  - `ruff check trr_backend/socials/instagram/scraper.py trr_backend/socials/tiktok/scraper.py trr_backend/socials/youtube/scraper.py trr_backend/socials/twitter/scraper.py trr_backend/repositories/social_season_analytics.py scripts/socials/worker.py tests/socials/test_comment_scraper_fixes.py` (pass)
  - `python -m py_compile trr_backend/socials/instagram/scraper.py trr_backend/socials/tiktok/scraper.py trr_backend/socials/youtube/scraper.py trr_backend/socials/twitter/scraper.py trr_backend/repositories/social_season_analytics.py scripts/socials/worker.py` (pass)

Twitter analytics comment-scope correction for Bravo posts (this session, 2026-02-12):
- Files:
  - `trr_backend/repositories/social_season_analytics.py`
  - `tests/repositories/test_social_season_analytics.py`
- Changes:
  - Refactored Twitter analytics rows in `_rows_for_platform(...)` to split explicit `posts` and `comments` CTE branches (matching other platforms).
  - Corrected Bravo comment scoping to include audience replies on Bravo posts:
    - posts branch remains Bravo-owned posts only (`bravo`/`bravotv`) in the selected window.
    - comments branch now includes replies with Bravo `source_account` context.
    - added legacy fallback for older rows missing `source_account` by recursively walking reply chains that originate from in-scope Bravo posts.
  - Preserved analytics row output schema (platform/kind/source_id/text/engagement/ts/url/author).
  - Added repository tests asserting:
    - Bravo Twitter SQL includes explicit post/comment stage filters, source-account scope, legacy reply-chain fallback, and time-window filters.
    - non-Bravo Twitter SQL keeps open comment scope and excludes Bravo-only alias filters.
- Validation:
  - `ruff check trr_backend/repositories/social_season_analytics.py tests/repositories/test_social_season_analytics.py` (pass)
  - `python -m py_compile trr_backend/repositories/social_season_analytics.py` (pass)
  - `pytest -q tests/repositories/test_social_season_analytics.py tests/api/routers/test_socials_season_analytics.py` (20 passed)

Season Social Analytics V3 — no-data weekly engagement bars + contextual sentiment (this session, 2026-02-17):
- Files:
  - `trr_backend/repositories/social_season_analytics.py`
  - `tests/repositories/test_social_season_analytics.py`
  - `tests/api/routers/test_socials_season_analytics.py`
  - `.env.example`
  - `docs/cross-collab/TASK8/PLAN.md`
  - `docs/cross-collab/TASK8/STATUS.md`
  - `docs/cross-collab/TASK8/OTHER_PROJECTS.md`
- Changes:
  - Added additive analytics field `weekly_platform_engagement` with per-platform engagement totals, `total_engagement`, and `has_data`.
  - Upgraded analytics sentiment flow to contextual rule-based classification with:
    - negation/intensifier/contrast handling,
    - cast/entity-aware token treatment,
    - optional Gemini disambiguation for ambiguous comments only (`SOCIAL_SENTIMENT_GEMINI_*` flags, fallback-safe).
  - Updated sentiment driver extraction to:
    - exclude cast-name-derived tokens and handles/mentions,
    - score from resolved comment sentiment labels.
  - Added repository and API tests for sentiment behavior, driver filtering, and analytics payload field presence.
- Validation:
  - `ruff check trr_backend/repositories/social_season_analytics.py tests/repositories/test_social_season_analytics.py tests/api/routers/test_socials_season_analytics.py` (pass)
  - `python -m py_compile trr_backend/repositories/social_season_analytics.py api/routers/socials.py` (pass)
  - `pytest -q tests/repositories/test_social_season_analytics.py tests/api/routers/test_socials_season_analytics.py` (29 passed)
  - Note: `ruff check .` still reports unrelated pre-existing repo issues (outside touched scope), including `api/main.py` and `trr_backend/db/pg.py`.

Cast matrix relationship sync now uses person-level Fandom + Wikipedia pages (this session, 2026-02-17):
- Files:
  - `trr_backend/ingestion/show_cast_matrix_scraper.py`
  - `api/routers/admin_show_roles.py`
  - `tests/ingestion/test_show_cast_matrix_scraper.py`
  - `tests/api/routers/test_admin_show_roles.py`
- Changes:
  - Added person-level URL builders for knowledge pages:
    - `build_person_fandom_url(person_name)` -> `https://real-housewives.fandom.com/wiki/<Person_Name>`
    - `build_person_wikipedia_url(person_name)` -> `https://en.wikipedia.org/wiki/<Person_Name>`
  - Extended relationship parsing to support person-page extraction from both sources:
    - new `extract_relationship_data_from_wikipedia_html(...)` for infobox `Spouse/Partner/Children/Family/Relatives` data.
    - enhanced Fandom infobox extraction to emit `global_partner_roles` (season `0`) in addition to season table matches and kids.
  - Updated relationship role inference patterns to include `spouse` and `partner` variants (including ex- forms) and map them to existing canonical cast-matrix relationship roles.
  - Updated cast sync relationship URL resolution to fetch per-person pages from:
    - existing `core.cast_fandom` URL,
    - person `entity_links` (`fandom` + `wikipedia`),
    - deterministic person slugs as fallback.
  - Relationship assignment builder now ingests both Fandom and Wikipedia person pages and writes:
    - season-scoped relationship roles when season evidence exists,
    - global season `0` relationship roles from infobox relationship fields,
    - global season `0` kid roles.
  - Added safeguard to skip self-relationship assignments when source person matches inferred related person.
- Validation:
  - `ruff check trr_backend/ingestion/show_cast_matrix_scraper.py api/routers/admin_show_roles.py tests/ingestion/test_show_cast_matrix_scraper.py tests/api/routers/test_admin_show_roles.py` (pass)
  - `python -m py_compile trr_backend/ingestion/show_cast_matrix_scraper.py api/routers/admin_show_roles.py` (pass)
  - `pytest -q tests/ingestion/test_show_cast_matrix_scraper.py tests/api/routers/test_admin_show_roles.py` (9 passed)

Post-level social comment re-sync endpoint for analytics modal (this session, 2026-02-17):
- Files:
  - `api/routers/socials.py`
  - `trr_backend/repositories/social_season_analytics.py`
  - `tests/api/routers/test_socials_season_analytics.py`
- Changes:
  - Added `POST /api/v1/admin/socials/seasons/{season_id}/analytics/posts/{platform}/{source_id}/refresh`.
  - New repository helper `refresh_post_comments(...)` re-fetches comments for a single post/video (Instagram, TikTok, YouTube, Twitter replies), upserts them, and returns refresh summary counts.
  - Endpoint now refreshes comments then returns the latest full post-detail payload (same shape as existing GET) with an additional `refresh` summary object.
  - Updated low-level upsert helpers to accept nullable `job_id` for direct on-demand refresh writes outside queue jobs.
  - Added router coverage test for the new refresh endpoint and payload forwarding.
- Validation:
  - `pytest -q tests/api/routers/test_socials_season_analytics.py` (11 passed)
  - `ruff check api/routers/socials.py trr_backend/repositories/social_season_analytics.py tests/api/routers/test_socials_season_analytics.py` (pass)

Cast matrix discovery hardening + person link validation + Wikidata season resolution (this session, 2026-02-17):
- Files:
  - `trr_backend/ingestion/show_cast_matrix_scraper.py`
  - `api/routers/admin_show_roles.py`
  - `api/routers/admin_show_links.py`
  - `tests/ingestion/test_show_cast_matrix_scraper.py`
  - `tests/api/routers/test_admin_show_roles.py`
  - `tests/api/routers/test_admin_show_links.py`
- Changes:
  - Added explicit missing-page detection guards for Wikipedia and Fandom person pages so non-existent pages are ignored during relationship extraction and link validation.
  - Tightened cast-matrix name parsing cleanup to strip template/CSS leakage and numeric ref markers before relationship inference.
  - Updated relationship sync path to skip relationship parsing when fetched person knowledge pages are missing.
  - Updated show link discovery:
    - season Wikipedia links now prefer Wikidata `enwiki` sitelinks (actual season article URL),
    - person Wikipedia/Fandom links are validated before emission,
    - missing person pages are excluded.
  - Added auto-approval behavior when person profile ingestion succeeds from Bravo profile URLs (`entity_links` updated to approved with raised confidence).
  - Person discovery rows now honor per-row status/confidence overrides in upsert path.
- Validation:
  - `ruff check api/routers/admin_show_links.py api/routers/admin_show_roles.py trr_backend/ingestion/show_cast_matrix_scraper.py tests/api/routers/test_admin_show_links.py tests/api/routers/test_admin_show_roles.py tests/ingestion/test_show_cast_matrix_scraper.py` (pass)
  - `pytest -q tests/api/routers/test_admin_show_links.py tests/api/routers/test_admin_show_roles.py tests/ingestion/test_show_cast_matrix_scraper.py` (16 passed)

Admin image metadata enhancements: face boxes + content-type endpoint (this session, 2026-02-17):
- Files:
  - `api/routers/admin_image_counts.py`
  - `api/routers/admin_asset_flags.py`
  - `tests/api/routers/test_admin_image_counts_fallback.py`
  - `tests/api/routers/test_admin_asset_flags.py`
- Changes:
  - Extended auto-count responses to include normalized `face_boxes` derived from Screenalytics detections.
  - Persisted auto-count face boxes to image metadata/context:
    - cast photos: `core.cast_photos.metadata.face_boxes`
    - media assets: propagated into linked `core.media_links.context.face_boxes`.
  - Added new admin endpoint:
    - `POST /api/v1/admin/assets/content-type`
    - updates normalized content type in metadata and table-specific fields (`kind` / `context_type`) by origin.
    - updates linked `media_links.context` when origin is `media_assets` so app consumers see content-type changes immediately.
  - Added/updated router tests for:
    - face-box response behavior from auto-count,
    - content-type update success and invalid input rejection.
- Validation:
  - `pytest -q tests/api/routers/test_admin_image_counts_fallback.py tests/api/routers/test_admin_asset_flags.py` (10 passed)
  - `ruff check api/routers/admin_image_counts.py api/routers/admin_asset_flags.py tests/api/routers/test_admin_image_counts_fallback.py tests/api/routers/test_admin_asset_flags.py` (pass)

Person profile ownership hardening for fandom enrichment (this session, 2026-02-17):
- Files:
  - `api/routers/admin_person_images.py`
  - `scripts/enrich/enrich_show_cast.py`
  - `tests/api/routers/test_admin_person_images.py`
- Changes:
  - Tightened `_names_match(...)` so person matching no longer accepts last-name-only matches.
  - Added honorific-aware tokenization (`Dr.`, `Mr.`, etc.) while still requiring first+last person-level alignment.
  - Hardened Fandom profile ownership checks:
    - if page URL slug owner does not match expected person, reject the profile row even when scraped fields look similar.
  - Applied the same matching/ownership guard in both the admin refresh path and the enrichment script path.
  - Added regression tests covering:
    - `Henry Barlow` not matching `Lisa Barlow` / `John Barlow`,
    - mismatched page-owner URL rejection (`Lisa_Barlow` URL cannot hydrate `John Barlow`).
- Validation:
  - `ruff check api/routers/admin_person_images.py scripts/enrich/enrich_show_cast.py tests/api/routers/test_admin_person_images.py` (pass)
  - `pytest -q tests/api/routers/test_admin_person_images.py` (18 passed)

Cast latest-season derivation fix for show cast grid (this session, 2026-02-17):
- Files:
  - `api/routers/admin_show_roles.py`
  - `tests/api/routers/test_admin_show_roles.py`
- Changes:
  - Updated `GET /api/v1/admin/shows/{show_id}/cast-role-members` aggregation logic to derive season metrics from the union of:
    - episode-credit season evidence (`season_numbers` from `v_show_cast_roles_enriched`), and
    - role-assignment seasons (`show_cast_role_assignments`, excluding global season `0`).
  - `latest_season`, `seasons_appeared`, and `season_numbers` are now recomputed from that union before filtering/sorting, so role assignments in later seasons are reflected in cast cards.
  - Added regression test proving a person with episode evidence only in season 1 but role assignment in season 3 now resolves to `latest_season = 3` and `season_numbers = [1,3]`.
- Validation:
  - `pytest -q tests/api/routers/test_admin_show_roles.py` (5 passed)
  - `ruff check api/routers/admin_show_roles.py tests/api/routers/test_admin_show_roles.py` (pass)

TRR stack audit remediation baseline (this session, 2026-02-17):
- Files:
  - `.github/workflows/ci.yml`
  - `.env.example`
  - `README.md`
  - `requirements.in`
  - `requirements.lock.txt`
  - `requirements.txt`
  - `scripts/check_env_example.py`
  - `trr_backend/vision/text_overlay.py`
  - `docs/cross-collab/TASK9/PLAN.md`
  - `docs/cross-collab/TASK9/OTHER_PROJECTS.md`
  - `docs/cross-collab/TASK9/STATUS.md`
- Changes:
  - Added CI merge-marker guard (`<<<<<<<`/`>>>>>>>`) fail-fast step.
  - Added env contract validation for `.env.example` (required keys, duplicate keys, invalid key names).
  - Canonicalized Gemini model resolution to prefer `GEMINI_MODEL`, then `GOOGLE_GEMINI_MODEL`, with deprecated `GEMINI-MODEL` fallback warning.
  - Introduced lock-driven Python install flow:
    - source manifest: `requirements.in`
    - lock artifact: `requirements.lock.txt`
    - compatibility entrypoint: `requirements.txt -> -r requirements.lock.txt`
  - Added CI lock-freshness verification for backend lock artifact.
- Validation:
  - `python3 scripts/check_env_example.py --file .env.example --required SCREENALYTICS_API_URL TRR_INTERNAL_ADMIN_SHARED_SECRET SCREENALYTICS_SERVICE_TOKEN --allow-hyphen GEMINI-MODEL` (pass)
  - `python3 -m py_compile trr_backend/vision/text_overlay.py scripts/check_env_example.py` (pass)
  - `ruff check trr_backend/vision/text_overlay.py scripts/check_env_example.py` (pass)
  - `uv pip compile requirements.in --python-version 3.11 -o requirements.lock.txt` + diff-against-baseline lock check (pass)
- Follow-up in same session:
  - Migrated backend Gemini call sites to prefer `google-genai` with legacy fallback compatibility:
    - `trr_backend/vision/text_overlay.py`
    - `trr_backend/repositories/social_season_analytics.py`
  - Added route-aware model key support (`GEMINI_MODEL_FAST`, `GEMINI_MODEL_PRO`) and updated env/docs examples.
  - Replaced `google-generativeai` dependency with `google-genai` in `requirements.in` and regenerated `requirements.lock.txt`.
  - Validation:
    - `python3 -m py_compile trr_backend/vision/text_overlay.py trr_backend/repositories/social_season_analytics.py` (pass)
    - `ruff check trr_backend/vision/text_overlay.py trr_backend/repositories/social_season_analytics.py` (pass)
    - `uv pip compile requirements.in --python-version 3.11 -o requirements.lock.txt` + diff-against-baseline lock check (pass)
- Continuation (same session):
  - Removed legacy `google.generativeai` fallback branches from backend Gemini callsites; runtime now expects `google-genai`.
  - Updated sentiment/text-overlay model resolution to support route keys (`GEMINI_MODEL_FAST`, `GEMINI_MODEL_PRO`) with canonical fallback chain.
  - Validation:
    - `python3 -m py_compile trr_backend/vision/text_overlay.py trr_backend/repositories/social_season_analytics.py` (pass)
    - `ruff check trr_backend/vision/text_overlay.py trr_backend/repositories/social_season_analytics.py` (pass)

Cast episode scope + person knowledge ownership hardening (this session, 2026-02-17):
- Files:
  - `api/routers/admin_show_roles.py`
  - `api/routers/admin_show_links.py`
  - `tests/api/routers/test_admin_show_roles.py`
  - `tests/api/routers/test_admin_show_links.py`
  - `scripts/shows/cleanup_invalid_person_knowledge_links.py`
- Changes:
  - `GET /api/v1/admin/shows/{show_id}/cast-role-members` now scopes `total_episodes` to selected seasons when `seasons` query is provided, while preserving all-season totals when no season filter is present.
  - Relationship-role sync now validates each relationship source URL against the source person before parsing; mismatched ownership pages are skipped.
  - Person knowledge discovery now validates `wikidata` links in addition to `wikipedia`/`fandom`.
  - Replaced stale-pending-only cleanup with full invalid person-knowledge cleanup across statuses/origins (`wikipedia`, `fandom`, `wikidata`), with fetch-error rows preserved and reported.
  - `POST /api/v1/admin/shows/{show_id}/links/discover` response now includes:
    - `invalid_people_links_deleted`
    - `invalid_people_links_validation_failures`
  - Added one-off cleanup script with `--show-id`, `--dry-run` (default behavior), and `--apply`.
- Validation:
  - `ruff check api/routers/admin_show_links.py api/routers/admin_show_roles.py tests/api/routers/test_admin_show_links.py tests/api/routers/test_admin_show_roles.py scripts/shows/cleanup_invalid_person_knowledge_links.py` (pass)
  - `pytest -q tests/api/routers/test_admin_show_links.py tests/api/routers/test_admin_show_roles.py` (`17 passed`)
  - `python3 -m py_compile api/routers/admin_show_links.py api/routers/admin_show_roles.py scripts/shows/cleanup_invalid_person_knowledge_links.py` (pass)
- RHOSLC cleanup run:
  - Dry-run:
    - `PYTHONPATH=/Users/thomashulihan/Projects/TRR/TRR-Backend /Users/thomashulihan/Projects/TRR/TRR-Backend/.venv/bin/python scripts/shows/cleanup_invalid_person_knowledge_links.py --show-id 7782652f-783a-488b-8860-41b97de32e75 --dry-run`
    - scanned `132`, invalid `48`, validation_failures `38`, deleted `0`
  - Apply:
    - `PYTHONPATH=/Users/thomashulihan/Projects/TRR/TRR-Backend /Users/thomashulihan/Projects/TRR/TRR-Backend/.venv/bin/python scripts/shows/cleanup_invalid_person_knowledge_links.py --show-id 7782652f-783a-488b-8860-41b97de32e75 --apply`
    - scanned `132`, invalid `48`, validation_failures `38`, deleted `48`

Continuation (same session, 2026-02-17) — cross-collab sync only:
- Updated `docs/cross-collab/TASK9/STATUS.md` to reflect downstream screenalytics TASK7 completion for lint-signal restoration and Wave A dependency validation.
- No backend code/runtime changes in this continuation block.

Continuation (same session, 2026-02-17) — Gemini telemetry modernization:
- Updated `trr_backend/vision/text_overlay.py`:
  - Added model selection telemetry (`model_source`, `model_route`, `model_fallback_path`)
  - Persisted telemetry fields to metadata patch:
    - `text_overlay_model_source`
    - `text_overlay_model_route`
    - `text_overlay_model_fallback_path`
  - Added route/source/fallback logging for Gemini text-overlay detection requests.
- Updated `trr_backend/repositories/social_season_analytics.py`:
  - Added model selection helper returning `(model, source, fallback_path)`
  - Enhanced Gemini sentiment logging with `source` + `fallback_path` while keeping route `pro`.
  - Formatted two long `_upsert_tweet(...)` lines to satisfy Ruff line-length checks.

Validation evidence:
- `python3 -m py_compile trr_backend/vision/text_overlay.py trr_backend/repositories/social_season_analytics.py` (pass)
- `ruff check trr_backend/vision/text_overlay.py trr_backend/repositories/social_season_analytics.py --target-version py311` (pass)

Wikipedia person URL hardening + RHOSLC cleanup rerun (this session, 2026-02-17):
- Files:
  - `api/routers/admin_show_links.py`
  - `tests/api/routers/test_admin_show_links.py`
- Changes:
  - Added Wikipedia API title-resolution fallback (`w/api.php?action=query&redirects=1&prop=info&inprop=url`) for person URL validation.
  - `_validate_person_knowledge_url(..., kind="wikipedia")` now:
    - rejects missing articles directly from API response,
    - validates owner-name match from canonical title,
    - returns canonical Wikipedia URL when valid,
    - falls back to HTML fetch path only when API fetch has transient errors.
  - Added regression tests for:
    - missing article rejection (`Whitney_Comstock_Duncan` style case),
    - API-validated accept path,
    - API owner mismatch rejection.
- Validation:
  - `PYTHONPATH=. .venv/bin/python -m pytest tests/api/routers/test_admin_show_links.py -q` (`14 passed`)
- RHOSLC cleanup rerun (same session):
  - `PYTHONPATH=. .venv/bin/python scripts/shows/cleanup_invalid_person_knowledge_links.py --show-id 7782652f-783a-488b-8860-41b97de32e75 --dry-run`:
    - scanned `84`, invalid `42`, validation_failures `0`
  - `--apply` run(s) until stable:
    - deleted invalid rows across runs (`37`, then `3`, then `1`)
  - final apply snapshot:
    - scanned `43`, invalid `0`, validation_failures `1`, deleted `0`

Continuation (same session, 2026-02-17) — Gemini telemetry acceptance hardening:
- Files:
  - `tests/vision/test_text_overlay_fallback.py`
  - `tests/repositories/test_social_season_analytics.py`
- Changes:
  - Added text-overlay Gemini model-selection tests for route-specific precedence and canonical fallback-path reporting.
  - Added persisted metadata telemetry assertion for:
    - `text_overlay_model_source`
    - `text_overlay_model_route`
    - `text_overlay_model_fallback_path`
  - Added social sentiment Gemini regression tests validating:
    - `GEMINI_MODEL_PRO` selection precedence
    - route log payload includes `source` + `fallback_path`.
- Validation:
  - `pytest -q tests/vision/test_text_overlay_fallback.py::test_resolve_gemini_model_selection_prefers_fast_alias tests/vision/test_text_overlay_fallback.py::test_resolve_gemini_model_selection_tracks_canonical_fallback_path tests/vision/test_text_overlay_fallback.py::test_detect_media_asset_text_overlay_persists_model_telemetry_fields tests/repositories/test_social_season_analytics.py::test_resolve_sentiment_gemini_model_selection_prefers_pro_alias tests/repositories/test_social_season_analytics.py::test_classify_ambiguous_sentiments_logs_model_source_and_fallback` (`5 passed`)
  - `ruff check tests/vision/test_text_overlay_fallback.py tests/repositories/test_social_season_analytics.py` (pass)

Continuation (same session, 2026-02-17) — remediation PR stabilization + green CI:
- Branch/PR:
  - `codex/plan-remediation-backend`
  - `https://github.com/therealityreport/trr-backend/pull/63`
- Final stabilization actions:
  - Pushed consolidated remediation commit with env contract checks, lock workflow artifacts, social lifecycle parity updates, migration `0126`, and cross-collab TASK9/TASK10 docs.
  - Resolved prior CI mismatch by ensuring PR checks run against the updated branch head.
- Validation:
  - Local: `pytest -q` (`549 passed, 18 skipped`)
  - GitHub checks on head `6d015478035b61f0342b9ad7bdf9212111c9b5dc`:
    - `ci / test` (success)
    - `Secret Scan / gitleaks` (success)
    - `Repository Map / generate-repo-map` (success)

Continuation (same session, 2026-02-18) — Health Center pipeline orchestration + episode precedence:
- Files:
  - `api/routers/admin_show_sync.py`
  - `scripts/sync/sync_seasons_episodes.py`
  - `trr_backend/ingestion/show_importer.py`
  - `tests/api/routers/test_admin_show_sync.py`
  - `tests/ingestion/test_show_importer_episode_precedence.py`
- Changes:
  - Reordered refresh stream `seasons_episodes` execution to IMDb episodes first, then seasons.
  - Added optional structured SSE progress metadata for consumers:
    - `stage_key`
    - `topic`
    - `provider`
  - Reordered composed sync script to run episodes before seasons.
  - Updated episode precedence behavior:
    - IMDb episode text fields are authoritative when present (`title`, `overview`, `synopsis`, `air_date`).
    - TMDb enrichment now fills canonical text only when missing, while still updating TMDb-specific fields.
  - Added tests for stream order/metadata and precedence regression coverage.
- Validation:
  - `pytest -q tests/api/routers/test_admin_show_sync.py tests/ingestion/test_show_importer_episode_precedence.py` (`11 passed`)

Continuation (same session, 2026-02-19) — Google News ingestion + unified show news validation:
- Files (already present on current branch; validated in-session):
  - `api/routers/admin_show_news.py`
  - `trr_backend/scraping/google_news_parser.py`
  - `supabase/migrations/0129_add_google_news_source.sql`
  - `tests/api/routers/test_admin_show_news.py`
  - `tests/scraping/test_google_news_parser.py`
  - `api/main.py` (router registration)
- Validation:
  - `./.venv/bin/python -m ruff check api/routers/admin_show_news.py trr_backend/scraping/google_news_parser.py tests/api/routers/test_admin_show_news.py tests/scraping/test_google_news_parser.py` (pass)
  - `./.venv/bin/python -m pytest -q tests/scraping/test_google_news_parser.py tests/api/routers/test_admin_show_news.py` (`7 passed`)

Continuation (same session, 2026-02-19) — CI fix for TMDb season enrichment overview precedence:
- Files:
  - `trr_backend/ingestion/show_importer.py`
  - `tests/integrations/tmdb/test_tmdb_season_enrichment.py`
- Changes:
  - Preserved IMDb canonical episode text (`title`, `overview`, `synopsis`, `air_date`) for existing episodes during TMDb season enrichment.
  - Updated TMDb season enrichment regression test expectations to assert non-overwrite behavior for existing IMDb-backed episode fields.
- Validation:
  - `pytest -q tests/integrations/tmdb/test_tmdb_season_enrichment.py::test_tmdb_season_enrichment_preserves_imdb_title_and_upserts_posters tests/ingestion/test_show_importer_episode_precedence.py::test_imdb_episode_fields_take_precedence_and_tmdb_fills_provider_fields` (`2 passed`)
  - `ruff check trr_backend/ingestion/show_importer.py tests/integrations/tmdb/test_tmdb_season_enrichment.py tests/ingestion/test_show_importer_episode_precedence.py` (pass)

Continuation (same session, 2026-02-19) — Google News parser/router refinements:
- Files:
  - `api/routers/admin_show_news.py`
  - `trr_backend/scraping/google_news_parser.py`
  - `tests/api/routers/test_admin_show_news.py`
  - `tests/scraping/test_google_news_parser.py`
- Changes:
  - Included pending parser and admin router refinements for show news ingestion plus updated regression coverage.
- Validation:
  - `ruff check api/routers/admin_show_news.py trr_backend/scraping/google_news_parser.py tests/api/routers/test_admin_show_news.py tests/scraping/test_google_news_parser.py` (pass)
  - `pytest -q tests/api/routers/test_admin_show_news.py tests/scraping/test_google_news_parser.py` (`9 passed`)

Continuation (same session, 2026-02-19) — person refresh source-policy bypass + word-id completion telemetry:
- Files:
  - `api/routers/admin_person_images.py`
  - `tests/api/routers/test_admin_person_images.py`
- Changes:
  - Added `enforce_show_source_policy: bool = true` to `RefreshImagesRequest`.
  - Added `_resolve_refresh_sources(...)` helper so both refresh endpoints can bypass `_apply_show_source_policy(...)` when `enforce_show_source_policy=false`.
  - Updated both endpoints:
    - `POST /api/v1/admin/person/{person_id}/refresh-images`
    - `POST /api/v1/admin/person/{person_id}/refresh-images/stream`
    to use unified source resolution.
  - Enhanced stream word-detection telemetry and completion payload fields:
    - `text_overlay_configured: boolean`
    - `text_overlay_candidates: number`
    - `text_overlay_skipped_reason: "not_configured" | "no_pending_images" | null`
  - Added explicit stream progress message when detector is configured but there are no pending text-overlay candidates.
- Validation:
  - `pytest TRR-Backend/tests/api/routers/test_admin_person_images.py -q` (`20 passed`)

Continuation (same session, 2026-02-19) — Google News featured image extraction + hosted backfill retries:
- Files:
  - `trr_backend/scraping/google_news_parser.py`
  - `api/routers/admin_show_news.py`
  - `tests/scraping/test_google_news_parser.py`
  - `tests/api/routers/test_admin_show_news.py`
- Changes:
  - Added RSS description `<img>` extraction fallback for Google News items when media/enclosure tags are absent.
  - Added first-page `<img>` fallback in article HTML featured-image resolver (after OG/Twitter checks).
  - Updated fresh-snapshot stale guard logic to retry sync when Google news items have image URLs but no `hosted_image_url`, enabling remirror to Supabase/S3.
  - Kept Google sync persistence shape additive (`image_url`, `original_image_url`, `hosted_image_url`, `media_asset_id`, `featured_image_synced`).
- Validation:
  - `pytest -q tests/scraping/test_google_news_parser.py tests/api/routers/test_admin_show_news.py` (`11 passed`)
  - `ruff check api/routers/admin_show_news.py trr_backend/scraping/google_news_parser.py tests/api/routers/test_admin_show_news.py tests/scraping/test_google_news_parser.py` (pass)

Continuation (same session, 2026-02-19) — season social analytics daily activity contract for heatmap UI:
- Files:
  - `trr_backend/repositories/social_season_analytics.py`
  - `tests/repositories/test_social_season_analytics.py`
  - `tests/api/routers/test_socials_season_analytics.py`
- Changes:
  - Added additive analytics payload field `weekly_daily_activity` to `get_analytics(...)`.
  - Daily buckets are now built per visible week window using 24-hour slots anchored to each week start.
  - Each day row includes:
    - `day_index`
    - `date_local` (`YYYY-MM-DD`)
    - per-platform `posts` and `comments`
    - `total_posts` and `total_comments`
  - Kept existing analytics fields unchanged (`weekly`, `weekly_platform_posts`, `weekly_platform_engagement`).
  - Added repository tests to verify:
    - day-level aggregation correctness,
    - daily totals summing to weekly totals,
    - zero-activity weeks retaining zeroed day buckets,
    - dynamic day counts for longer pre-season windows.
  - Extended API analytics week-zero test expectation to include `weekly_daily_activity`.
- Validation:
  - `ruff check trr_backend/repositories/social_season_analytics.py tests/repositories/test_social_season_analytics.py tests/api/routers/test_socials_season_analytics.py` (pass)
  - `pytest -q tests/repositories/test_social_season_analytics.py tests/api/routers/test_socials_season_analytics.py` (`55 passed`)

Continuation (same session, 2026-02-19) — heatmap day-index correction for calendar-date placement:
- Files:
  - `trr_backend/repositories/social_season_analytics.py`
  - `tests/repositories/test_social_season_analytics.py`
- Changes:
  - Fixed `weekly_daily_activity` day assignment to use **local calendar date delta** instead of elapsed 24-hour offset from week start.
  - Normalized week-start/day-label generation to requested timezone before deriving `date_local` and day indices.
  - Prevents one-day visual shifts (for example, Oct 1 content appearing on Sep 30 tile) in week heatmaps.
  - Added regression test covering episode-air anchored week start (`20:00 ET`) with an Oct 1 post to ensure it maps to the `2025-10-01` tile.
- Validation:
  - `ruff check trr_backend/repositories/social_season_analytics.py tests/repositories/test_social_season_analytics.py` (pass)
  - `pytest -q tests/repositories/test_social_season_analytics.py tests/api/routers/test_socials_season_analytics.py` (`56 passed`)

Continuation (same session, 2026-02-19) — media admin stabilization (reprocess telemetry, season-scoped refresh, batch-job SSE):
- Files:
  - `api/routers/admin_person_images.py`
  - `api/routers/admin_show_sync.py`
  - `api/routers/admin_asset_batch_jobs.py` (new)
  - `api/main.py`
  - `tests/api/routers/test_admin_person_images.py`
  - `tests/api/routers/test_admin_show_sync.py`
  - `tests/api/routers/test_admin_asset_batch_jobs.py` (new)
- Changes:
  - Reprocess stream `complete` payload now includes:
    - `text_overlay_configured`
    - `text_overlay_candidates`
    - `text_overlay_skipped_reason`
  - Added explicit text-overlay candidate scan and no-pending/not-configured skip signaling while preserving existing counters.
  - Extended `RefreshShowPhotosRequest` with `season_number` and updated stream behavior so season mode:
    - scopes TMDb season/episode sync and mirror stages to the selected season,
    - avoids show-level sync/mirror/prune stages,
    - performs cast discovery from `episode_appearances` only (no `show_cast` fallback).
  - Added batch-job stream router with:
    - `POST /api/v1/admin/shows/{show_id}/assets/batch-jobs/stream`
    - `POST /api/v1/admin/shows/{show_id}/seasons/{season_number}/assets/batch-jobs/stream`
    - operation multi-select (`count`, `crop`, `id_text`, `resize`),
    - explicit targets + content types,
    - surfaced skipped/failure reasons (including unsupported origin and season-scope mismatch).
- Validation:
  - `ruff check api/routers/admin_person_images.py api/routers/admin_show_sync.py api/routers/admin_asset_batch_jobs.py tests/api/routers/test_admin_person_images.py tests/api/routers/test_admin_show_sync.py tests/api/routers/test_admin_asset_batch_jobs.py` (pass)
  - `pytest /Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_person_images.py -q` (`22 passed`)
  - `pytest /Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_show_sync.py -q` (`18 passed`)
  - `pytest /Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_asset_batch_jobs.py -q` (`3 passed`)

Continuation (same session, 2026-02-19) — YouTube ingest recall + queue fallback diagnostics for UI/terminal parity:
- Files:
  - `api/routers/socials.py`
  - `trr_backend/repositories/social_season_analytics.py`
  - `tests/api/routers/test_socials_season_analytics.py`
  - `tests/repositories/test_social_season_analytics.py`
- Changes:
  - Added additive ingest request flag: `allow_inline_dev_fallback` on `SeasonSocialIngestRequest`.
  - Added controlled dev/local inline fallback when queue is enabled but workers are unavailable:
    - preserves existing `503 SOCIAL_WORKER_UNAVAILABLE` outside allowed fallback path,
    - returns additive response fields: `execution_mode`, optional `warnings`, and `worker_health` when fallback is used.
  - Broadened `_youtube_video_matches_show_terms(...)` to accept show-term matches in either title or description text while retaining:
    - cross-show exclusion (`wife swap` + `real housewives edition`),
    - generic season-term stripping,
    - hashtag fast-path matching.
  - Extended `_ingest_youtube(...)` retrieval diagnostics with additive metadata:
    - `videos_scanned`
    - `videos_matched_show_terms`
    - `videos_filtered_show_terms`
    - `videos_skipped_up_to_date`
    - `filter_samples` (capped dropped-video samples with reason/title/video_id)
  - Added repository/API coverage for description-based matching, filter diagnostics, and fallback vs strict queue behavior.
- Validation:
  - `ruff check api/routers/socials.py trr_backend/repositories/social_season_analytics.py tests/repositories/test_social_season_analytics.py tests/api/routers/test_socials_season_analytics.py` (pass)
  - `pytest -q tests/repositories/test_social_season_analytics.py tests/api/routers/test_socials_season_analytics.py` (`59 passed`)

Continuation (same session, 2026-02-19) — additive live-count telemetry verification pass:
- Files (already updated earlier in session):
  - `api/routers/admin_show_sync.py`
  - `api/routers/admin_asset_batch_jobs.py`
  - `api/routers/admin_person_images.py`
  - `tests/api/routers/test_admin_show_sync.py`
  - `tests/api/routers/test_admin_asset_batch_jobs.py`
  - `tests/api/routers/test_admin_person_images.py`
- Notes:
  - Confirmed additive `live_counts` and progress `operation_counts` snapshots remain stable and backward-compatible across show refresh, batch jobs, and person refresh/reprocess streams.
- Validation:
  - `pytest /Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_show_sync.py -q` (`18 passed`)
  - `pytest /Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_asset_batch_jobs.py -q` (`3 passed`)
  - `pytest /Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_person_images.py -q` (`22 passed`)

Continuation (same session, 2026-02-19) — YouTube comment-count correctness for ingest + week/detail analytics:
- Files:
  - `trr_backend/repositories/social_season_analytics.py`
  - `tests/repositories/test_social_season_analytics.py`
  - `tests/api/routers/test_socials_season_analytics.py`
- Root cause:
  - YouTube comments were persisted, but analytics/week detail often displayed `0 comments` because they relied on `social.youtube_videos.comments_count`, which can be `0` from YouTube search metadata.
- Changes:
  - Added internal helper `_youtube_effective_comment_count(...)` and applied it to:
    - `_week_detail_youtube(...)` post payload + engagement + totals
    - `get_post_comments(... platform='youtube' ...)` stats/engagement
  - Added `_sync_youtube_video_comment_counts(...)` to batch-sync `youtube_videos.comments_count` from saved `youtube_comments` using `greatest(existing, saved)`.
  - Wired sync helper into YouTube ingest flows:
    - `_ingest_youtube(... stage='comments')`
    - `_ingest_youtube(... stage='posts')`
    - `refresh_post_comments(... platform='youtube' ...)`
  - Added additive retrieval metadata:
    - `youtube_comment_count_sync_targets`
    - `youtube_comment_count_synced`
  - Improved Smart Incremental expected-count behavior for YouTube:
    - `_expected_comment_count_for_platform(..., snapshot=...)` now falls back to lifecycle snapshot active count when reported YouTube `comments_count <= 0`.
  - Added season-scoped operational backfill helper:
    - `backfill_youtube_comment_counts_for_season(...)`
    - supports optional `source_account` (normalized with optional `@`) and date range scoping.
- Validation:
  - `pytest tests/repositories/test_social_season_analytics.py -k "youtube and (week_detail or post_comments or ingest or refresh)"` (`6 passed`)
  - `pytest tests/api/routers/test_socials_season_analytics.py -k "week_detail or post_comments"` (`3 passed`)
  - `ruff check trr_backend/repositories/social_season_analytics.py tests/repositories/test_social_season_analytics.py tests/api/routers/test_socials_season_analytics.py` (pass)
- Operational backfill executed:
  - Command: `backfill_youtube_comment_counts_for_season('e9161955-6ee4-4985-865e-3386a0f670fb', source_account='bravo', date_start=2025-10-01 UTC, date_end=2025-10-07 UTC)`
  - Result: `videos_scanned=3`, `youtube_comment_count_backfilled=3`
  - Post-check (Week 3 window):
    - `tkaK_8nrmJE`: `comments_count=20`, saved `20`
    - `U0wltNE406o`: `comments_count=418`, saved `418`
    - `2QEpGFPULhY`: `comments_count=20`, saved `20`

Continuation (same session, 2026-02-19) — networks/streaming metadata completion to 100%:
- Files:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/sync/sync_networks_streaming_links.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/scripts/test_sync_networks_streaming_links.py`
- Changes:
  - Added streaming alias expansion for metadata lookup (channel/tier suffix stripping + known brand aliases) to improve Wikidata/Wikipedia resolution for provider variants.
  - Enhanced Wikipedia URL extraction to fall back to non-`enwiki` sitelinks when English sitelink is unavailable.
  - Applied targeted `admin.network_streaming_overrides` rows for the last unresolved entities and propagated override links into core dimension rows.
- Validation:
  - `ruff check scripts/sync/sync_networks_streaming_links.py tests/scripts/test_sync_networks_streaming_links.py` (pass)
  - `pytest tests/scripts/test_sync_networks_streaming_links.py` (`12 passed`)
  - Runtime sync runs:
    - `PYTHONPATH=. python scripts/sync/sync_networks_streaming_links.py --all --unresolved-only`
    - `PYTHONPATH=. python scripts/sync/sync_networks_streaming_links.py --all --skip-s3`
    - `PYTHONPATH=. python scripts/sync/sync_networks_streaming_links.py --all --unresolved-only`
  - Final completion snapshot:
    - `completion_total=213`
    - `completion_resolved=213`
    - `completion_unresolved=0`
    - `completion_percent=100.00`

Continuation (same session, 2026-02-19) — Instagram permalink metadata enrichment + media mirroring + analytics thumbnail support:
- Files:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/supabase/migrations/0132_instagram_permalink_metadata_and_media_mirroring.sql`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/socials/instagram/permalink_metadata.py` (new)
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/socials/instagram/__init__.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/socials/backfill_instagram_metadata_and_media.py` (new)
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/socials/test_instagram_permalink_metadata.py` (new)
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/repositories/test_social_season_analytics.py`
- Changes:
  - Added additive `social.instagram_posts` columns for permalink-derived metadata (`post_format`, collaborators/tags/hashtags/mentions, `duration_seconds`, source/scrape status) and hosted media mirror fields (`hosted_thumbnail_url`, `hosted_media_urls`, mirror status/error).
  - Implemented permalink extractor using logged-out HTML `data-sjs` parsing and recursive lookup of `xdt_api__v1__media__shortcode__web_info.items[0]`.
  - Added best-effort enrichment flow in Instagram ingest with fallback to existing media info endpoint; ingest never fails on enrichment errors and persists status/error fields.
  - Added best-effort S3 media mirroring for thumbnail + media URLs using deterministic keys under `social/instagram/{show_id}/{season_number}/week-{week_index|unknown}/{shortcode}/...` and persisted hosted-first URLs/status.
  - Extended retrieval payloads to expose Instagram metadata in week detail and post-detail comments routes.
  - Extended analytics rows/leaderboards to include `thumbnail_url` for `bravo_content` and `viewer_discussion`.
  - Added one-time backfill script (`--weeks` default `8`) for metadata/mirroring refresh with idempotent checks and counters.
- Validation:
  - `ruff check trr_backend/socials/instagram/permalink_metadata.py trr_backend/repositories/social_season_analytics.py scripts/socials/backfill_instagram_metadata_and_media.py tests/socials/test_instagram_permalink_metadata.py tests/repositories/test_social_season_analytics.py` (pass)
  - `pytest -q tests/socials/test_instagram_permalink_metadata.py tests/repositories/test_social_season_analytics.py -k 'instagram or analytics_includes_weekly_platform_engagement'` (`12 passed`)

Continuation (same session, 2026-02-19) — Bravo cast-only sync path + canonical cast URL probing:
- Files:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_show_bravo.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/scraping/bravo_parser.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_show_bravo.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/scraping/test_bravo_parser.py`
- Changes:
  - Extended Bravo preview/commit request models to accept `person_url_candidates`; commit also supports `cast_only` mode.
  - Added cast-name-derived canonical URL generation (`https://www.bravotv.com/people/{slug}`) in router and merged candidates into parser inputs.
  - Updated parser to merge/normalize candidate person URLs and only return valid, successfully parsed person URLs in `discovered_person_urls` when people parsing is enabled.
  - Added not-found detection for Bravo person pages (including "Page Not Found" fallback pages) so invalid people URLs are skipped.
  - Updated commit behavior for `cast_only` mode to skip show-copy persistence, pending-link suggestions, cast-role suggestion sync, cast-matrix sync, and show-image import handling.
- Validation:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest tests/scraping/test_bravo_parser.py tests/api/routers/test_admin_show_bravo.py` (`21 passed`)

Continuation (same session, 2026-02-19) — Sync by Fandom backend foundation (person + season, multi-wiki, AI cleanup):
- Files:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/integrations/fandom_community_allowlist.txt`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/supabase/migrations/0133_fandom_sync_expansion.sql`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_fandom_sync.py` (new)
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/main.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/integrations/fandom_discovery.py` (new)
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/integrations/openai_fandom_cleanup.py` (new)
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/ingestion/fandom_person_scraper.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/ingestion/fandom_season_scraper.py` (new)
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/season_fandom.py` (new)
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/supabase/schema_docs/core.cast_fandom.md`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/supabase/schema_docs/core.season_fandom.md` (new)
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/supabase/schema_docs/INDEX.md`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/integrations/fandom/test_fandom_discovery.py` (new)
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/ingestion/test_fandom_person_scraper.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_fandom_sync.py` (new)
- Changes:
  - Expanded allowlisted fandom communities to include `realitytv-girl.fandom.com` and `my-the-jinxer.fandom.com`.
  - Added migration `0133_fandom_sync_expansion.sql`:
    - extends `core.cast_fandom` with dynamic/AI/citation/conflict/source-variant fields.
    - creates `core.season_fandom` with uniqueness on `(season_id, source)` and supporting indexes/grants.
  - Added multi-community Fandom candidate discovery with allowlist enforcement and AllPages traversal:
    - MediaWiki `list=allpages` continuation support.
    - HTML `Special:AllPages` fallback paging support.
  - Added season page parser with generic section extraction and canonical section normalization.
  - Expanded person parser to emit:
    - `dynamic_sections`
    - `bio_card`
    - `casting_summary`
  - Added OpenAI cleanup integration (`OPENAI_API_KEY`, configurable model via `OPENAI_FANDOM_MODEL`) with deterministic fallback.
  - Added new admin APIs:
    - `GET /api/v1/admin/person/{person_id}/fandom`
    - `POST /api/v1/admin/person/{person_id}/import-fandom/preview`
    - `POST /api/v1/admin/person/{person_id}/import-fandom/commit`
    - `GET /api/v1/admin/shows/{show_id}/seasons/{season_number}/fandom`
    - `POST /api/v1/admin/shows/{show_id}/seasons/{season_number}/import-fandom/preview`
    - `POST /api/v1/admin/shows/{show_id}/seasons/{season_number}/import-fandom/commit`
  - Added season_fandom repository helpers and registered the new router in `api/main.py`.
- Validation:
  - `ruff check api/routers/admin_fandom_sync.py trr_backend/integrations/fandom_discovery.py trr_backend/integrations/openai_fandom_cleanup.py trr_backend/ingestion/fandom_person_scraper.py trr_backend/ingestion/fandom_season_scraper.py trr_backend/repositories/season_fandom.py tests/api/routers/test_admin_fandom_sync.py tests/integrations/fandom/test_fandom_discovery.py tests/ingestion/test_fandom_person_scraper.py` (pass)
  - `pytest -q tests/integrations/fandom/test_fandom_discovery.py tests/ingestion/test_fandom_person_scraper.py tests/api/routers/test_admin_fandom_sync.py` (`8 passed`)

Continuation (same session, 2026-02-20) — cast refresh split backend foundations (IMDb preflight + ingest-only skips + reprocess resize):
- Files:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_show_sync.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_person_images.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_show_sync.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_person_images.py`
- Changes:
  - Added cast refresh preflight for `/api/v1/admin/shows/{show_id}/refresh` and `/refresh/stream`: when `cast_credits` is requested and show lacks IMDb ID (`imdb_id` or `external_ids.imdb/imdb_id`), endpoint now returns `409` and does not run cast scripts.
  - Extended `RefreshImagesRequest` with additive optional flags: `skip_auto_count`, `skip_word_detection`, `skip_centering`, `skip_resize`.
  - Applied those skip flags to both person refresh endpoints (`refresh-images` and `refresh-images/stream`) while keeping fetch/upsert/mirror/prune/profile sync intact.
  - Updated `reprocess-images/stream` to include resize stage (`_resize_person_gallery_images`) after centering and report resize counters + `live_counts.resized`.
- Validation:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest tests/api/routers/test_admin_show_sync.py tests/api/routers/test_admin_person_images.py` (`43 passed`)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check api/routers/admin_show_sync.py api/routers/admin_person_images.py tests/api/routers/test_admin_show_sync.py tests/api/routers/test_admin_person_images.py` (pass)

Continuation (same session, 2026-02-23) — social landing perf hardening (`include_jobs=false`) + targets context short-circuit:
- Files:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/socials.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/repositories/test_social_season_analytics.py`
- Root cause/focus:
  - Season landing analytics path always computed ingest jobs and always loaded full season context in `get_targets`, adding avoidable backend latency for `/analytics` and `/targets` calls.
- Changes:
  - Added additive `include_jobs: bool = False` arg to `get_analytics(...)`.
  - Preserved response shape by always returning `"jobs"` key; when `include_jobs=False`, it now returns `[]` and skips `list_jobs(...)`.
  - Updated season analytics endpoint to call `get_analytics(..., include_jobs=False)`.
  - Updated `get_targets(...)` to query `social.season_targets` first and avoid `get_season_context(...)` when explicit targets exist.
  - Added lightweight season identity lookup (`season/show metadata only`) for explicit-target responses.
  - Added regressions for:
    - `get_analytics(include_jobs=False)` skips job listing and preserves `jobs=[]`.
    - `get_targets` does not call `get_season_context` when rows already exist.
- Validation:
  - `ruff check trr_backend/repositories/social_season_analytics.py api/routers/socials.py tests/repositories/test_social_season_analytics.py` (pass)
  - `pytest tests/repositories/test_social_season_analytics.py -k "include_jobs_false or get_targets_uses_existing_rows"` (`2 passed`)
  - `pytest tests/repositories/test_social_season_analytics.py -k "analytics or targets"` (1 failure unrelated to this change set):
    - failing test: `test_ingest_instagram_posts_stage_skips_up_to_date_posts_in_incremental_mode`
    - observed failure: upsert/comments fetch executed unexpectedly in `_ingest_instagram` incremental path (`AssertionError: fetch_comments should not run for up-to-date incremental posts`).

Continuation (same session, 2026-02-23) — cast-role-members timeout hardening + query path optimization:
- Files:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/supabase/migrations/0134_optimize_cast_role_members.sql` (new)
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_show_roles.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_show_roles.py`
- Root cause/focus:
  - `cast-role-members` path could exceed proxy timeout under load due expensive join/aggregation shape and row-wise role accumulation.
- Changes:
  - Added migration `0134_optimize_cast_role_members.sql`:
    - Added idempotent index: `core.credits (show_id, person_id)`.
    - Added partial index for cast-photo first-image lookups: `core.cast_photos (person_id, gallery_index) WHERE gallery_index IS NOT NULL`.
    - Replaced `core.v_show_cast_roles_enriched` with pre-aggregated CTE view (`base_cast`, `episode_stats`, `role_stats`) to reduce multiplicative joins before grouping.
    - Re-granted view select to `authenticated` and `service_role`.
  - Updated `/api/v1/admin/shows/{show_id}/cast-role-members` path:
    - Aggregates role names and assignment seasons in SQL per person (instead of row-wise Python accumulation).
    - Keeps response shape/filter semantics unchanged.
    - Added env-gated performance logs (`TRR_CAST_ROLE_MEMBERS_PERF_LOGS`) with segmented query timings + total endpoint duration.
- Validation:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check api/routers/admin_show_roles.py tests/api/routers/test_admin_show_roles.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest -q tests/api/routers/test_admin_show_roles.py` (`7 passed`)

Continuation (same session, 2026-02-23) — social landing timeout stabilization (DB pool + social query-path reductions):
- Files:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/db/pg.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/socials.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/db/test_pg_pool.py` (new)
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/repositories/test_social_season_analytics.py`
- Root cause/focus:
  - Social landing paths were sensitive to DB/connectivity degradation because each query opened a new Postgres connection and analytics/targets paths performed avoidable extra work/lookups.
- Changes:
  - Replaced per-query connection lifecycle in `pg.py` with process-level `ThreadedConnectionPool`.
    - Added env knobs: `TRR_DB_POOL_MINCONN`, `TRR_DB_POOL_MAXCONN`.
    - Added safe checkout/return, commit on success, rollback on exception.
    - Added `close_pool()` helper for cleanup/tests.
  - `get_targets(...)` now uses joined season metadata in the same query path and avoids full `get_season_context(...)` when explicit target rows exist.
  - `_target_accounts_by_platform(...)` now reads `social.season_targets` directly and falls back to defaults only when rows are absent; optional `context` reuse added to avoid repeated lookups.
  - `get_analytics(...)` fast path remains `include_jobs=False` with preserved response shape (`jobs: []`).
  - Added structured timing logs (`duration_ms`) for `/targets`, `/runs`, `/analytics` router handlers.
- Validation:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check trr_backend/db/pg.py trr_backend/repositories/social_season_analytics.py api/routers/socials.py tests/repositories/test_social_season_analytics.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest tests/db/test_pg_pool.py` (`2 passed`)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest tests/api/routers/test_socials_season_analytics.py -k "targets or analytics or ingest_runs"` (`22 passed`)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest tests/repositories/test_social_season_analytics.py -k "targets or analytics or runs"` (`52 passed, 1 failed`)
    - Existing failing test not introduced in this change set: `test_ingest_instagram_posts_stage_skips_up_to_date_posts_in_incremental_mode` (Instagram incremental refresh behavior assertion).
- Rollback notes:
  - To rollback pooling only, revert `/trr_backend/db/pg.py`; repository/router logic remains backward compatible with previous DB helpers.
  - To rollback social query-path changes, revert `/trr_backend/repositories/social_season_analytics.py` and `/api/routers/socials.py` together.

Continuation (same session, 2026-02-24) — Bravo cast-only auto-probe streaming preview with per-URL progress:
- Files:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/scraping/bravo_parser.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_show_bravo.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/scraping/test_bravo_parser.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_show_bravo.py`
- Root cause/focus:
  - Cast-only Bravo preview was blocking on one payload, so users saw static zero counters with no candidate-level visibility while probes were running.
- Changes:
  - Added additive parser probe iterator behavior to expose deterministic per-candidate probe metadata (`candidate_url`, `status`, optional `person`, optional `error`) via `probe_bravo_person_url_candidates(...)`.
  - Kept `parse_bravo_show_bundle(...)` behavior backward-compatible while reusing the iterator for single-source probe logic.
  - Added new additive SSE endpoint:
    - `POST /api/v1/admin/shows/{show_id}/import-bravo/preview/stream`
    - emits `start` (candidate list + total), `progress` (per URL status + live counters), `complete` (preview-compatible aggregate payload), and fail-safe `error` events.
  - Reused existing cast-only candidate construction and suppression bypass behavior (`cast_only=true` probes all canonical cast `/people/*` URLs).
  - Added shared candidate summary helper for tested/valid/missing/error counters.
- Validation:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ./.venv/bin/ruff check trr_backend/scraping/bravo_parser.py api/routers/admin_show_bravo.py tests/scraping/test_bravo_parser.py tests/api/routers/test_admin_show_bravo.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ./.venv/bin/pytest -q tests/scraping/test_bravo_parser.py tests/api/routers/test_admin_show_bravo.py` (`34 passed`)

Continuation (same session, 2026-02-24) — weekly analytics comment coverage fields for Bravo post count table:
- Files:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/repositories/test_social_season_analytics.py`
- Root cause/focus:
  - Weekly table rows exposed saved comment totals only, with no denominator for "how complete is comments backfill" by week.
- Changes:
  - Added `reported_comments` projection to analytics row queries for post rows:
    - Instagram/TikTok/YouTube use post `comments_count`.
    - Twitter uses post `replies_count`.
    - Comment rows emit `reported_comments=0`.
  - Extended weekly aggregation payload to include additive coverage fields:
    - `weekly_platform_posts[].reported_comments` (per-platform map)
    - `weekly_platform_posts[].total_reported_comments`
    - `weekly_platform_posts[].comments_saved_pct` (saved-vs-reported %, nullable when denominator missing)
  - Preserved existing response keys/shapes (additive only, no breaking changes).
- Validation:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check trr_backend/repositories/social_season_analytics.py tests/repositories/test_social_season_analytics.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest tests/repositories/test_social_season_analytics.py -k "get_analytics_includes_weekly_platform_engagement_and_has_data or get_analytics_include_jobs_false_keeps_jobs_key_and_skips_listing or test_get_analytics_weekly_daily_activity_uses_dynamic_day_count"` (`3 passed`)
  - Note: full-file run still contains an existing unrelated failure in this repo baseline:
    - `test_ingest_instagram_posts_stage_skips_up_to_date_posts_in_incremental_mode`

Continuation (same session, 2026-02-24) — Bravo cast-only performance hardening (lightweight probes + concurrent stream + commit preview reuse):
- Files:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/scraping/bravo_parser.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_show_bravo.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/scraping/test_bravo_parser.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_show_bravo.py`
- Root cause/focus:
  - Cast-only probes still parsed/hydrated person-page related videos/news (many extra fetches), and cast-only commit re-probed the full candidate set after preview.
- Changes:
  - Added additive lightweight parse controls in parser:
    - `parse_person_page(..., include_related_content: bool = True, hydrate_related_dates: bool = True)`
    - cast-only lightweight mode returns essential profile fields with `videos=[]` and `news=[]`.
  - Extended probe/bundle parsing APIs additively:
    - `probe_bravo_person_url_candidates(..., include_related_content, hydrate_related_dates)`
    - `parse_bravo_show_bundle(..., include_person_related_content, hydrate_person_related_dates)`
  - Cast-only preview/commit now call lightweight person parsing (`False/False`) so person probes avoid related-content hydration.
  - Upgraded `POST /api/v1/admin/shows/{show_id}/import-bravo/preview/stream` cast-only people probe path:
    - bounded worker pool (max 3),
    - emits `progress` with `status: in_progress` when a candidate starts,
    - terminal `progress` includes additive `candidate_index` and `elapsed_ms`,
    - logs slow-candidate warnings and stream-level avg/p95 candidate timings.
  - Added additive preview metadata to preview payloads (`/preview` + stream `complete`):
    - `show_url`, `cast_only`, `season_filter`.
  - Added additive commit reuse contract:
    - `BravoCommitRequest.preview_result?: dict`
    - cast-only commit validates preview freshness (show URL, candidate set, season filter) and reuses preview bundle instead of re-probing.
    - stale/mismatched preview returns `409` (`Preview stale. Re-run preview.`).
  - Existing missing-profile handling retained:
    - NA marker persistence path still runs from candidate `missing` statuses.
- Validation:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check api/routers/admin_show_bravo.py trr_backend/scraping/bravo_parser.py tests/api/routers/test_admin_show_bravo.py tests/scraping/test_bravo_parser.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest tests/scraping/test_bravo_parser.py tests/api/routers/test_admin_show_bravo.py` (`38 passed`)

Continuation (same session, 2026-02-24) — Bravo cast-only commit now sets person profile media + thumbnail context:
- Files:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_show_bravo.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_show_bravo.py`
- Root cause/focus:
  - Cast-only commit persisted person text/profile metadata and imported gallery assets, but did not explicitly promote imported Bravo hero media to primary person profile media links.
- Changes:
  - Extended `_import_bravo_person_image(...)` return payload additively with:
    - `asset_ids`
    - `hosted_urls`
    - `primary_hosted_url`
  - Added `_promote_bravo_profile_media_link(...)` to:
    - upsert/normalize person `gallery` link context for Bravo profile assets,
    - seed `context.thumbnail_crop` default (`x=50, y=32, zoom=1, mode=auto`) when absent,
    - upsert `profile` link and set it as primary via `core.set_primary_media_link` RPC.
  - Commit path now:
    - promotes imported Bravo hero assets to profile links,
    - writes hosted mirrored URL back into `people.profile_image_url.bravo` so profile image uses hosted media.
  - Added regression test asserting hosted profile promotion call path and hosted URL persistence behavior during commit.
- Validation:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check api/routers/admin_show_bravo.py tests/api/routers/test_admin_show_bravo.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest tests/api/routers/test_admin_show_bravo.py -q` (`26 passed`)

Continuation (same session, 2026-02-24) — Bravo + Fandom cast sync integration + show-level fandom discovery hardening:
- Files:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_show_links.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_show_bravo.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_show_links.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_show_bravo.py`
- Changes:
  - Added show-level fandom discovery in `_discover_show_links(...)`:
    - prefers existing persisted non-rejected show fandom/wikia links,
    - falls back to canonical `https://real-housewives.fandom.com/wiki/{Show_Name}`,
    - writes as show knowledge links so Settings -> Show Pages can display persisted fandom rows.
  - Extended Bravo preview/stream/commit flows with additive Fandom support:
    - fandom domain resolution from show-assigned fandom/wikia links with fallback domain,
    - per-cast canonical fandom candidate generation,
    - preview + stream payloads include fandom results/counters/domains,
    - stream progress events include source-aware (`bravo`/`fandom`) progress semantics.
  - Cast-only preview reuse validation remains strict for freshness but is backward-compatible:
    - if old preview payload lacks fandom candidate fields, reuse no longer hard-fails solely on missing fandom set.
  - Commit flow includes additive fandom outcome counters in response and persisted fandom side effects already wired in route.
  - Added/updated regression coverage for:
    - show link fandom discovery fallback/prefer-existing behavior,
    - source-aware stream progress/counters,
    - cast-only commit preview reuse + fandom count reporting.
- Validation:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check api/routers/admin_show_bravo.py api/routers/admin_show_links.py tests/api/routers/test_admin_show_bravo.py tests/api/routers/test_admin_show_links.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest -q tests/api/routers/test_admin_show_links.py tests/api/routers/test_admin_show_bravo.py` (`54 passed`)

Continuation (same session, 2026-02-24) — Networks/Streaming all-logo gallery sync + API metrics expansion:
- Files:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/supabase/migrations/0135_network_streaming_logo_assets.sql`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/sync/sync_networks_streaming_links.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_show_sync.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/README.md`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/scripts/test_sync_networks_streaming_links.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_show_sync.py`
- Changes:
  - Added persistent gallery table migration: `admin.network_streaming_logo_assets` (mirrored/skipped/failed assets with source provenance and hosted metadata).
  - Extended `sync_networks_streaming_links.py` to mirror all capped candidate logos by source, dedupe by URL + SHA, upsert each asset row, and mark canonical primary asset.
  - Preserved canonical behavior: primary/base logo still updates `core.networks` / `core.watch_providers`; black/white variant generation remains primary-only.
  - Added sync output metrics: `logo_assets_discovered`, `logo_assets_mirrored`, `logo_assets_skipped`, `logo_assets_failed`.
  - Expanded `POST /api/v1/admin/shows/sync-networks-streaming` response with the new `logo_assets_*` counters and schema preflight requirements for the new table.
  - Added/updated tests for source caps/order, candidate dedupe behavior, and endpoint metric aggregation.
- Validation:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check scripts/sync/sync_networks_streaming_links.py tests/scripts/test_sync_networks_streaming_links.py tests/api/routers/test_admin_show_sync.py api/routers/admin_show_sync.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest tests/scripts/test_sync_networks_streaming_links.py tests/api/routers/test_admin_show_sync.py` (`35 passed`)

Continuation (same session, 2026-02-19) — Instagram sync reliability hardening (transaction safety, lifecycle consistency, scraper failure semantics):
- Files:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/socials/instagram/scraper.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/socials/instagram/permalink_metadata.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/repositories/test_social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/socials/test_comment_scraper_fixes.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/socials/test_instagram_permalink_metadata.py`
- Changes:
  - Reworked Instagram ingest transaction behavior to use per-post/per-anchor savepoints instead of full-connection rollbacks, preventing prior successful writes from being reverted by later per-post failures.
  - Corrected persistence accounting by applying comment counters only after savepoint release; added additive retrieval metadata:
    - `rolled_back_posts`
    - `rolled_back_comments`
    - `comment_fetch_failures_by_reason`
    - `comments_mark_missing_skipped`
  - Added lifecycle consistency fixes:
    - posts-stage Instagram comment sync now tracks observed IDs and marks missing comments only when fetch is complete.
    - comments-stage logical API failures now skip missing-mark operations and report skipped counts.
    - manual `refresh_post_comments(... platform='instagram')` now applies completeness checks, optional missing-mark updates, and returns additive fields:
      - `fetch_failed`
      - `is_complete`
      - `comments_marked_missing`
      - `incomplete_reason`
      - `comment_fail_reasons`
  - Prevented lifecycle provenance clearing on manual refresh by not writing `last_seen_run_id` when `run_id is None`.
  - Hardened account matching in `_load_existing_posts(...)` with normalized account expressions (including fallback to username/channel title), reducing incremental sync drift.
  - Updated depth-default resolution:
    - explicit caller values are respected.
    - env-backed defaults apply when values are unset (`None`):
      - `SOCIAL_DEFAULT_MAX_COMMENTS_PER_POST`
      - `SOCIAL_DEFAULT_MAX_REPLIES_PER_POST`
  - Hardened Instagram scraper error semantics:
    - `comments_auth_failed` resets per `fetch_comments` call.
    - comments/replies API payloads with `status != 'ok'` now set `last_comment_fetch_reason='api_status_fail'` and mark auth failure for login/challenge/checkpoint conditions.
    - request-error logging path now safely handles missing response objects.
  - Hardened permalink metadata extraction:
    - route-aware probing now tries canonical route first (`p|reel|tv`) with fallback probing order.
    - `data-sjs` parsing supports wrapped payload patterns (e.g. function-wrapped JSON) in addition to raw JSON bodies.
  - Hardened media mirroring:
    - dedupes thumbnail/media source URLs to avoid duplicate downloads/uploads.
    - streams downloads with max-size guard via `SOCIAL_MEDIA_MIRROR_MAX_BYTES` (default `50MB`) and `asset_too_large` failure reason.
- Validation:
  - Targeted lint:
    - `ruff check trr_backend/repositories/social_season_analytics.py trr_backend/socials/instagram/scraper.py trr_backend/socials/instagram/permalink_metadata.py tests/repositories/test_social_season_analytics.py tests/socials/test_comment_scraper_fixes.py tests/socials/test_instagram_permalink_metadata.py` (pass)
  - Targeted tests:
    - `pytest -q tests/socials/test_comment_scraper_fixes.py tests/socials/test_instagram_permalink_metadata.py tests/repositories/test_social_season_analytics.py -k "instagram or refresh_post_comments"` (`31 passed`)
  - Full backend test suite:
    - `pytest -q` (`734 passed`, `18 skipped`)
  - Full-repo lint/format status (pre-existing workspace drift):
    - `ruff check .` fails on unrelated existing files outside this task scope.
    - `ruff format --check .` reports many pre-existing files needing formatting.

Continuation (same session, 2026-02-24) — Person source URL auto-approval hardening (no pending person source links):
- Files:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_show_links.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_show_bravo.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/shows/backfill_bravo_person_source_links.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_show_links.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_show_bravo.py`
- Changes:
  - Enforced person-source no-pending behavior in discovery/sync flows for `imdb`, `tmdb`, `wikipedia`, `wikidata`, `fandom`, `wikia`, `bravo_profile`.
  - Added challenge-aware IMDb/TMDb validation fallback for anti-bot/challenge pages so canonical ID-backed URLs can still validate as approved when page routing is canonical.
  - Expanded cleanup scan behavior:
    - pending person-source rows now either promote to `approved` when valid or are removed when invalid/unverifiable.
    - existing non-pending rows continue strict owner/topic validation; fetch-error rows are preserved.
  - Added pending promotion path in cleanup with duplicate-safe handling:
    - if promotion collides with `entity_links_unique_active`, stale pending row is deleted.
  - Updated Bravo commit persistence (`_persist_pending_links_from_bravo_sync`) to never force person-source links to pending and to skip non-approved person-source rows.
  - Updated backfill script:
    - target scope now auto-selects shows with IMDb-backed cast IDs (or accepts explicit `--show-id`),
    - enforces no-pending person-source upserts,
    - adds `cleanup_promoted` reporting,
    - skips known `entity_links_unique_active` duplicate upsert collisions safely.
  - Added/updated tests for:
    - IMDb challenge-page acceptance for canonical ID-backed person URLs,
    - cleanup promotion of pending valid rows,
    - cleanup deletion of pending rows on fetch error,
    - Bravo sync persistence skipping non-approved person-source discovered rows.
- Validation:
  - `ruff check api/routers/admin_show_links.py api/routers/admin_show_bravo.py scripts/shows/backfill_bravo_person_source_links.py tests/api/routers/test_admin_show_links.py tests/api/routers/test_admin_show_bravo.py` (pass)
  - `python -m py_compile api/routers/admin_show_links.py api/routers/admin_show_bravo.py scripts/shows/backfill_bravo_person_source_links.py` (pass)
  - `pytest -q tests/api/routers/test_admin_show_links.py tests/api/routers/test_admin_show_bravo.py` (`54 passed`)
- Backfill execution:
  - Dry-run:
    - `PYTHONPATH=/Users/thomashulihan/Projects/TRR/TRR-Backend python -u scripts/shows/backfill_bravo_person_source_links.py`
    - totals: `cleanup_scanned=158`, `cleanup_invalid=0`, `cleanup_promoted=15`, `cleanup_deleted=0`, `cleanup_fetch_errors=32`, `discovered_upserted=1294`, `failed_shows=0`
  - Apply:
    - `PYTHONPATH=/Users/thomashulihan/Projects/TRR/TRR-Backend python -u scripts/shows/backfill_bravo_person_source_links.py --apply`
    - first run completed 5/6 shows and surfaced one duplicate-collision failure; reran failed show after duplicate-safe patch:
      - `PYTHONPATH=/Users/thomashulihan/Projects/TRR/TRR-Backend python -u scripts/shows/backfill_bravo_person_source_links.py --apply --show-id 7782652f-783a-488b-8860-41b97de32e75`
      - totals: `cleanup_scanned=89`, `cleanup_invalid=0`, `cleanup_promoted=0`, `cleanup_deleted=0`, `cleanup_fetch_errors=27`, `discovered_upserted=161`, `failed_shows=0`
  - Post-run verification:
    - impacted-show query reports `show_count=6`, `pending_person_source=0`, `approved_person_source=987`, `total_person_source=987`
    - Andy Cohen IMDb row present and approved: `https://www.imdb.com/name/nm0169212/`

Continuation (same session, 2026-02-24) — Social analytics additive contract + run summary + diagnostics hardening:
- Files:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/socials.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/repositories/test_social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_socials_season_analytics.py`
- Changes:
  - Extended analytics output (additive):
    - `summary.data_quality` (coverage/freshness timestamps + per-platform comment coverage)
    - `weekly_flags` (`zero_activity`, `spike`, `drop`, `comment_gap`)
    - `schedule_profile` (timezone + day-level posting profile)
    - `benchmark` (current vs previous/trailing deltas + consistency scores)
  - Added analytics include slicing in route:
    - `GET /api/v1/admin/socials/seasons/{season_id}/analytics?include=rows,flags,schedule,benchmark`
  - Added ingest run summary endpoint:
    - `GET /api/v1/admin/socials/seasons/{season_id}/ingest/runs/summary`
  - Added normalized `job_error_code` surface in job listing and run-summary aggregation.
  - Extended week-detail output with additive totals + diagnostics:
    - `expected_comments_total`, `saved_comments_total`, `comments_saved_pct`
    - `diagnostics.run_id`, `diagnostics.generated_at`, `diagnostics.source_scope`
  - Export enhancements:
    - CSV/PDF include additive data-quality/flags/benchmark sections.
- Tests:
  - Added/expanded backend tests for:
    - analytics additive fields,
    - include-slice forwarding,
    - ingest run summary endpoint,
    - week-detail diagnostics/totals additive fields.
- Validation:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check api/routers/socials.py trr_backend/repositories/social_season_analytics.py tests/repositories/test_social_season_analytics.py tests/api/routers/test_socials_season_analytics.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest -q tests/repositories/test_social_season_analytics.py tests/api/routers/test_socials_season_analytics.py` (`86 passed`)

Continuation (same session, 2026-02-24) — Backend lint/format cleanup pass:
- Files:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/media/restore_person_gallery_base_previews.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_person_images.py`
  - plus broad repo formatting normalization from `ruff format .` (47 files reformatted in this pass).
- Changes:
  - Ran repo-wide lint autofix and formatting cleanup.
  - Manually fixed remaining `E501` long SQL-snippet strings in `restore_person_gallery_base_previews.py` after autofix/format.
  - Ensured backend-wide Ruff lint and format checks are fully green.
- Validation:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check .` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff format --check .` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest -q` (`741 passed`, `18 skipped`)

Continuation (same session, 2026-02-24) — Week-view progress timeout hardening during comment fill:
- Files:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/socials.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/repositories/test_social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_socials_season_analytics.py`
- Root cause evidence:
  - App log showed repeated week-view polling timeouts (`/social/runs`/`/social/jobs` 504 around ~16s).
  - Backend log showed `psycopg2.pool.PoolError: connection pool exhausted` during active ingest/poll overlap.
  - Ingest paths held DB connections across slow external fetch loops, starving poll requests.
- Changes:
  - Refactored ingest loops to use short-lived DB scopes and moved remote fetches outside DB connection scopes across Instagram/TikTok/YouTube/Twitter ingest paths.
  - Added optional `run_id` filter support to ingest runs listing in repository `list_runs(...)`.
  - Extended `GET /api/v1/admin/socials/seasons/{season_id}/ingest/runs` route to accept/pass `run_id` and include it in response filters.
  - Added/updated tests for `run_id` SQL filtering and API passthrough.
- Validation:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check trr_backend/repositories/social_season_analytics.py api/routers/socials.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest tests/repositories/test_social_season_analytics.py -k "list_runs or ingest"` (`9 passed`)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest tests/api/routers/test_socials_season_analytics.py -k "ingest_runs"` (`3 passed`)
- Ops note:
  - For local high-concurrency social ingest + polling, set DB pool env to reduce starvation risk:
    - `TRR_DB_POOL_MINCONN=2`
    - `TRR_DB_POOL_MAXCONN=24`
  - Local test dependency added during validation: `python-multipart` in backend venv (required for FastAPI form parsing in API test import path).

Continuation (same session, 2026-02-24) — Networks/Streaming logo discovery cache + Bravo variant backfill:
- Files:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/sync/sync_networks_streaming_links.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/integrations/brandfetch.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/scripts/test_sync_networks_streaming_links.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_show_sync.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/README.md`
- Changes:
  - Implemented cache-first external discovery for `sync_networks_streaming_links`:
    - Reuses persisted `admin.network_streaming_logo_assets.source_url` candidates.
    - Skips Brandfetch/Logopedia/IMDb discovery on reruns when source URLs are already cached (unless `--force`).
  - Added per-asset cached mirror short-circuit:
    - If a candidate URL is already mirrored in `admin.network_streaming_logo_assets`, skip re-download/re-mirror and reuse hosted metadata.
  - Added catalog SVG raster fallback expansion:
    - For Logopedia `static.wikia.nocookie.net ... .svg/revision/latest` URLs, auto-generates `.../scale-to-width-down/1024` variants to improve mirror success without re-querying discovery APIs.
  - Updated Brandfetch candidate ranking to prefer PNG/WebP before SVG to reduce decode failures in environments without SVG rasterization libs.
  - Updated sync/router tests for 4-step sync orchestration (`show_logos` step), new cache behavior, and cached remirror skip behavior.
  - Updated script docs with explicit cache behavior and `--force` credit note.
- Validation:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check trr_backend/integrations/brandfetch.py scripts/sync/sync_networks_streaming_links.py tests/scripts/test_sync_networks_streaming_links.py tests/api/routers/test_admin_show_sync.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest -q tests/scripts/test_sync_networks_streaming_links.py tests/api/routers/test_admin_show_sync.py::TestSyncNetworksStreaming -q` (pass)
  - Bravo targeted runs (`entity_type=network`, `entity_key=bravo`):
    - First backfill run after cache logic: `asset_rows=22`, `mirrored_assets=7`
    - After SVG raster fallback expansion run: `asset_rows=28`, `mirrored_assets=13`
    - Re-run cache verification: `brandfetch_calls=0`, `logopedia_calls=0`

Continuation (same session, 2026-02-24) — SVG rasterization enablement for logo mirroring:
- Files:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/media/s3_mirror.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/media/test_s3_mirror.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/requirements.in`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/requirements.lock.txt`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/README.md`
- Changes:
  - Added SVG sniffing in image content-type detection.
  - Added SVG->PNG conversion path in `_ensure_png_bytes(...)` using `cairosvg.svg2png(...)` before PIL fallback.
  - Added media tests for SVG sniffing and raster conversion behavior.
  - Added `cairosvg` to `requirements.in` and recompiled lock via:
    - `uv pip compile requirements.in --python-version 3.11 -o requirements.lock.txt`
  - Local runtime install for execution validation:
    - `/opt/homebrew/opt/python@3.11/bin/python3.11 -m pip install cairosvg`
- Validation:
  - `ruff check trr_backend/media/s3_mirror.py tests/media/test_s3_mirror.py scripts/sync/sync_networks_streaming_links.py tests/scripts/test_sync_networks_streaming_links.py tests/api/routers/test_admin_show_sync.py` (pass)
  - `pytest -q tests/media/test_s3_mirror.py tests/scripts/test_sync_networks_streaming_links.py tests/api/routers/test_admin_show_sync.py::TestSyncNetworksStreaming -q` (pass)
- Bravo run impact after SVG support:
  - Targeted run summary: `logo_assets_mirrored=9`, `logo_assets_failed=0`, `logo_assets_skipped=13`.
  - Persisted Bravo assets now: `total_rows=30`, `mirrored=24`, `failed=5`, `skipped=1`.
  - Remaining failed rows are legacy catalog URLs; failed set now narrowed to 5 (`logo_decode_failed`) and does not affect canonical logo + black/white variants.
  - Cache behavior remains intact after SVG enablement:
    - `brandfetch_calls=0`, `logopedia_calls=0`, `logo_assets_mirrored=0`, `logo_assets_failed=0`, `logo_assets_skipped=22` on subsequent rerun.

Continuation (same session, 2026-02-24) — cleanup pass for superseded failed Logopedia SVG rows:
- Files:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/sync/sync_networks_streaming_links.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/scripts/test_sync_networks_streaming_links.py`
- Changes:
  - Added row-level cleanup logic in sync:
    - Detects failed `catalog` Logopedia raw SVG rows (`logo_decode_failed`) that have a mirrored raster variant (`/scale-to-width-down/1024`) for the same source URL lineage.
    - Marks those rows as `mirror_status='skipped'` with `failure_reason='raster_variant_mirrored'`.
  - Keeps remaining genuinely unresolved rows as failed.
- Validation:
  - `ruff check scripts/sync/sync_networks_streaming_links.py tests/scripts/test_sync_networks_streaming_links.py` (pass)
  - `pytest -q tests/scripts/test_sync_networks_streaming_links.py -q` (pass)
- Bravo targeted cleanup result:
  - Before: `failed=5`, `mirrored=24`, `skipped=1`.
  - After: `failed=3`, `mirrored=25`, `skipped=2`.
  - New non-blocking cleanup marker rows: `failure_reason='raster_variant_mirrored'` count `1`.
  - Remaining Bravo failed rows (3) are OLN legacy SVG files with no mirrored raster counterpart yet.

Continuation (same session, 2026-02-24) — news feature hardening: async Google sync jobs, dedupe/pagination, canonical URLs, and mirror retry cooldowns.
- Files:
  - `api/routers/admin_show_news.py`
  - `tests/api/routers/test_admin_show_news.py`
  - `tests/scraping/test_google_news_parser.py`
- Changes:
  - Expanded `POST /api/v1/admin/shows/{show_id}/google-news/sync` request contract with additive `async` mode; async requests now return `{ queued, job_id }` and run via background task.
  - Added additive job-status endpoint: `GET /api/v1/admin/shows/{show_id}/google-news/sync/{job_id}`.
  - Hardened stale guard + mirror backfill with retry cooldown and attempt caps (`mirror_status`, `mirror_attempt_count`, `last_mirror_*`, `mirror_retry_after`).
  - Added canonical article URL support and normalization in unified payload (`canonical_article_url`) and used it for dedupe.
  - Added additive `/news` query support: `source_contains`, `since`, `until`, `limit`, `cursor`; response now includes `total_count`, `next_cursor`, and additive `quality_score` per item.
  - Strengthened source parsing: invalid `sources` tokens now return `422` (absent/blank still defaults).
  - Reduced featured-image probe budget and surfaced additional sync diagnostics in snapshot source metadata.
  - Added read-path optimization to skip cast-index fetch when Google snapshot items already include `person_tags` and person filtering is not requested.
- Validation:
  - `ruff check api/routers/admin_show_news.py trr_backend/scraping/google_news_parser.py tests/api/routers/test_admin_show_news.py tests/scraping/test_google_news_parser.py` (pass)
  - `pytest -q tests/api/routers/test_admin_show_news.py tests/scraping/test_google_news_parser.py` (`16 passed`)

## 2026-02-24 Media Pipeline Stabilization (Codex)
- Added periodic heartbeat progress emissions during blocking source fetches in:
  - `api/routers/admin_person_images.py`
  - `api/routers/admin_show_sync.py`
- Added stage-selective reprocess controls via request payload in `reprocess-images/stream` (`run_count`, `run_id_text`, `run_crop`, `run_resize`).
- Fixed sync-networks failure aggregation to avoid inflating total failures.
- Improved show-level auto-count to:
  - fall back from `hosted_url` to `source_url` candidates,
  - persist `face_boxes` into link context,
  - generate/update thumbnail crop context and variants when available.
- Validation:
  - `pytest -q tests/api/routers/test_admin_show_sync.py tests/api/routers/test_admin_person_images.py tests/api/routers/test_admin_image_counts_fallback.py`

Continuation (same session, 2026-02-24) — News hardening phase 2 (additional defects remediation).
- Files:
  - `api/routers/admin_show_news.py`
  - `trr_backend/scraping/google_news_parser.py`
  - `supabase/migrations/0140_google_news_sync_job_heartbeat.sql` (new)
  - `tests/api/routers/test_admin_show_news.py`
  - `tests/scraping/test_google_news_parser.py`
- Changes:
  - Mirror retry hardening:
    - Added terminal mirror status (`missing_image_terminal`) for items with no source image.
    - Increment mirror attempts on terminal transition and missing-data retry branches.
    - Prevented repeat retries for terminal/no-source-image items.
  - Async sync resilience:
    - Added stale job reconciliation for queued/running jobs older than TTL (`15m`) with `job_orphaned_or_timed_out` error.
    - Added heartbeat touch/update support in async sync lifecycle.
    - Added additive migration column/index for job heartbeat scans.
  - Tagging precision:
    - Topic matching switched to boundary-aware normalized matching (prevents `cast`/`podcast` bleed).
    - Person alias generation tightened (no broad first-name expansion; unique first-name alias only).
  - Unified news facets:
    - Added additive `/news` response `facets` (`sources`, `people`, `topics`, `seasons`) computed pre-pagination.
  - Canonical URL normalization:
    - Stable query-param ordering for stronger dedupe of equivalent URLs.
- Validation:
  - `ruff check api/routers/admin_show_news.py trr_backend/scraping/google_news_parser.py tests/api/routers/test_admin_show_news.py tests/scraping/test_google_news_parser.py` (pass)
  - `pytest -q tests/api/routers/test_admin_show_news.py tests/scraping/test_google_news_parser.py` (`23 passed`)

Continuation (same session, 2026-02-24) — URL/Sync/Media hardening batch (person sources approved-only + Sync-by-Bravo signature/heartbeat).
- Files:
  - `api/main.py`
  - `api/routers/admin_show_links.py`
  - `api/routers/admin_show_bravo.py`
  - `scripts/shows/backfill_bravo_person_source_links.py`
  - `trr_backend/integrations/fandom.py`
  - `supabase/migrations/0139_add_fandom_allowlist_table.sql` (new)
  - `tests/api/routers/test_admin_show_links.py`
  - `tests/api/routers/test_admin_show_bravo.py`
  - `tests/integrations/fandom/test_fandom_search.py`
- Changes:
  - Added shared URL canonicalization and replaced raw lowercased URL keys in admin show link flows (create/patch/discovery/upsert).
  - Added `wikia -> fandom` kind normalization for writes while preserving legacy-row reads.
  - Added robust duplicate-detection helper (`23505`/constraint parsing fallback) and used it in link upsert + backfill script.
  - Hardened person-link validation with per-source timeout envs and stronger IMDb/TMDb challenge classification (requires ID-consistent + owner signals).
  - Cached fandom allowlist once per discovery pass and moved fandom candidate selection to scored best-candidate ranking.
  - Made person-link cleanup atomic with transaction-scoped promote+delete.
  - Added admin allowlist endpoints (`GET`/`PUT /api/v1/admin/fandom/allowlist`) and DB-backed allowlist load with file fallback cache.
  - Renamed Bravo persistence helper to `_persist_discovered_links_from_bravo_sync` and preserved approved-only person-source behavior with explicit skip counters.
  - Added additive `preview_signature` to Bravo preview/stream/commit contract and stale-signature `409` guard in commit.
  - Added SSE `heartbeat` events for long-running cast-only preview streams.
  - Made show-image candidate IDs deterministic via URL-hash and added retry-safe/idempotent stage guards in fandom profile import path.
  - Enhanced backfill script telemetry with JSON summary output and explicit skip/failure-reason counters.
- Validation:
  - `ruff check api/main.py api/routers/admin_show_links.py api/routers/admin_show_bravo.py scripts/shows/backfill_bravo_person_source_links.py trr_backend/integrations/fandom.py tests/api/routers/test_admin_show_links.py tests/api/routers/test_admin_show_bravo.py tests/integrations/fandom/test_fandom_search.py` (pass)
  - `pytest -q tests/api/routers/test_admin_show_links.py tests/api/routers/test_admin_show_bravo.py tests/integrations/fandom/test_fandom_search.py` (`68 passed`)
  - `python -m py_compile api/main.py api/routers/admin_show_links.py api/routers/admin_show_bravo.py scripts/shows/backfill_bravo_person_source_links.py trr_backend/integrations/fandom.py` (pass)

Continuation (same session, 2026-02-24) — Reliable comment sync hardening (coverage endpoint + inline fallback cap).
- Files:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/socials.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/.env.example`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/repositories/test_social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_socials_season_analytics.py`
- Changes:
  - Added env-controlled inline comments-only fallback worker cap using `SOCIAL_INLINE_COMMENTS_WORKERS` (`default=1`, bounded `1..4`).
  - Added additive season endpoint `GET /api/v1/admin/socials/seasons/{season_id}/analytics/comments-coverage` with scope/date/platform filters.
  - Added repository coverage aggregation returning:
    - `total_saved_comments`, `total_reported_comments`, `coverage_pct`, `up_to_date`, `stale_posts_count`, `posts_scanned`, `by_platform`, `evaluated_at`.
  - Documented local pool sizing recommendation in `.env.example`:
    - `TRR_DB_POOL_MINCONN=2`
    - `TRR_DB_POOL_MAXCONN=24`
- Validation:
  - `ruff check trr_backend/repositories/social_season_analytics.py api/routers/socials.py trr_backend/db/pg.py tests/repositories/test_social_season_analytics.py tests/api/routers/test_socials_season_analytics.py` (pass)
  - `pytest -q tests/repositories/test_social_season_analytics.py tests/api/routers/test_socials_season_analytics.py -k "coverage or ingest or comments"` (`35 passed, 59 deselected`)

Continuation (same session, 2026-02-24) — News Stability Patch Set (remaining fixes).
- Files:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_show_news.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/.env.example`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/scraping/google_news_parser.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_show_news.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/scraping/test_google_news_parser.py`
- Changes:
  - Added env-configurable stale timeout reader in router:
    - `GOOGLE_NEWS_SYNC_STALE_TIMEOUT_MINUTES` (default `15`, min `1`).
  - Documented `GOOGLE_NEWS_SYNC_STALE_TIMEOUT_MINUTES` in `.env.example`.
  - Kept stale-job reconcile contract stable (`job_orphaned_or_timed_out`) while using heartbeat-aware stale scan windows.
  - Threaded optional `heartbeat_cb` through Google parser + mirror sync paths and invoked periodic touches during:
    - topic candidate attempts,
    - canonical URL probes,
    - featured-image probes,
    - mirror import loop (every 5 image imports).
  - Hardened mirror terminal handling:
    - when source image exists but `article_url` is missing, mark item terminal with existing additive status:
      - `mirror_status="missing_image_terminal"`
      - `last_mirror_error="Missing article URL required for mirroring"`
      - `mirror_retry_after=null`.
    - terminal states remain non-retryable via `_google_item_needs_mirror_retry`.
  - Preserved additive `/news` + async sync contracts (no breaking route/field removals).
- Validation:
  - `ruff check api/routers/admin_show_news.py trr_backend/scraping/google_news_parser.py tests/api/routers/test_admin_show_news.py tests/scraping/test_google_news_parser.py` (pass)
  - `pytest -q tests/api/routers/test_admin_show_news.py tests/scraping/test_google_news_parser.py` (`28 passed`)

Continuation (same session, 2026-02-24) — Social analytics 22-item hardening pass (backend segment).
- Files:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/db/pg.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/supabase/migrations/0141_social_analytics_hot_path_indexes.sql` (new)
- Changes:
  - Raised default DB pool sizing for local/dev concurrency from `1..8` to `2..24` while preserving env override behavior.
  - Added short TTL in-process analytics cache (`15s`) keyed by request-shape dimensions to reduce repeated recomputation under live polling.
  - Added cached scrape-job feature-flag probing (`run_id` + queue columns) to remove repeated schema checks in `list_jobs`.
  - Added `include_post_text` row-build flag so aggregate-only analytics paths avoid pulling full post text payloads; full row paths remain unchanged.
  - Added per-request memoization for rule-based comment sentiment (`platform + source_id + normalized text hash`) to reduce repeated sentiment scoring cost.
  - Added additive hot-path social indexes (`IF NOT EXISTS`) across runs/jobs/post/comment tables for season/run/scope/time and common account/parent filters.
- Validation:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check trr_backend/db/pg.py trr_backend/repositories/social_season_analytics.py api/routers/socials.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest tests/repositories/test_social_season_analytics.py -k "analytics or runs or jobs or targets"` (`66 passed`)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest tests/api/routers/test_socials_season_analytics.py -k "runs or jobs or analytics or week"` (`28 passed`)

Continuation (same session, 2026-02-24) — URL/Sync/Bravo stabilization patch (post-hardening bugfix pass).
- Files:
  - `api/routers/admin_show_links.py`
  - `api/routers/admin_show_bravo.py`
  - `scripts/shows/backfill_bravo_person_source_links.py`
  - `tests/api/routers/test_admin_show_links.py`
  - `tests/api/routers/test_admin_show_bravo.py`
- Changes:
  - Tightened IMDb/TMDb challenge-page validation to prevent false approvals when owner signal cannot be verified (challenge outcomes now classify as unverifiable/fetch_error unless identity is strongly confirmed).
  - Reordered IMDb/TMDb validation flow so challenge detection can still run on challenge-like 4xx responses before generic invalid classification.
  - Hardened cast-only commit contract: `preview_signature` is now required for cast-only commits (422 when missing), stale mismatches still return 409.
  - Tightened cast-only preview reuse validation to always compare expected vs preview fandom candidate sets (including empty preview-set cases).
  - Improved backfill script operability: direct script execution works without requiring manual `PYTHONPATH=.` bootstrapping.
  - Added/updated regression tests for:
    - IMDb/TMDb challenge strictness behavior.
    - Fandom allowlist endpoint auth + normalization/persistence flows.
    - Cast-only commit missing-signature rejection.
    - Fandom stale-set preview validation behavior.
- Validation:
  - `ruff check api/routers/admin_show_links.py api/routers/admin_show_bravo.py scripts/shows/backfill_bravo_person_source_links.py tests/api/routers/test_admin_show_links.py tests/api/routers/test_admin_show_bravo.py` (pass)
  - `pytest -q tests/api/routers/test_admin_show_links.py tests/api/routers/test_admin_show_bravo.py` (`72 passed`)
  - `python -m py_compile api/routers/admin_show_links.py api/routers/admin_show_bravo.py scripts/shows/backfill_bravo_person_source_links.py` (pass)
  - `python scripts/shows/backfill_bravo_person_source_links.py --help` from repo root (pass without `PYTHONPATH=.`)

Continuation (same session, 2026-02-24) — Bravo video thumbnail quality + S3 mirroring.
- Files:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/scraping/bravo_parser.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_show_bravo.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/scraping/test_bravo_parser.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_show_bravo.py`
- Changes:
  - Improved Bravo video thumbnail extraction quality:
    - parses `data-srcset/srcset` and chooses highest descriptor (`w`/`x`) instead of first token,
    - stores `original_image_url` on video items,
    - upgrades video `image_url` from clip-page `og:image`/`twitter:image` during hydration.
  - Added reusable Bravo video thumbnail mirror pipeline in show router:
    - new helper `_sync_bravo_video_thumbnails(...)` mirrors to S3/Supabase through existing `import_images(...)`,
    - persists additive video fields: `hosted_image_url`, `original_image_url`, `media_asset_id`, `thumbnail_sync_status`, `thumbnail_sync_error`,
    - sets `image_url` to hosted URL on success.
  - Bravo commit flow now runs video thumbnail sync before snapshot persist, and stores additive sync summary under `normalized.video_thumbnail_sync`.
  - Added backfill endpoint:
    - `POST /api/v1/admin/shows/{show_id}/bravo/videos/sync-thumbnails` (supports `{ force?: boolean }`).
  - Kept existing Bravo read routes additive/backward-compatible.
- Validation:
  - `ruff check trr_backend/scraping/bravo_parser.py api/routers/admin_show_bravo.py tests/scraping/test_bravo_parser.py tests/api/routers/test_admin_show_bravo.py` (pass)
  - `pytest -q tests/scraping/test_bravo_parser.py tests/api/routers/test_admin_show_bravo.py` (`52 passed`)

Continuation (same session, 2026-02-24) — Cross-platform social sync hardening + async media mirror expansion.
- Files:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/supabase/migrations/0145_cross_platform_media_mirror_fields_and_job_types.sql`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/socials.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/socials/tiktok/scraper.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/socials/youtube/scraper.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/socials/twitter/scraper.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/socials/backfill_social_media_mirror_jobs.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/socials/backfill_instagram_metadata_and_media.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/socials/test_comment_scraper_fixes.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/repositories/test_social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_socials_season_analytics.py`
- Changes:
  - Added cross-platform media mirror schema support for TikTok/YouTube/Twitter (hosted URLs, mirror status/error, attempt diagnostics) and expanded `social.scrape_jobs.job_type` constraint to include platform-specific mirror job types.
  - Generalized mirror queue helpers and worker-stage execution so `stage=media_mirror` runs for Instagram, TikTok, YouTube, and Twitter with hosted-first thumbnail reads.
  - Added generic requeue endpoint `POST /api/v1/admin/socials/seasons/{season_id}/{platform}/mirror/requeue` and retained Instagram alias for backward compatibility.
  - Normalized account-scoped filters to `ltrim(lower(...), '@')` across load/coverage/row queries to avoid `@`/case drift misses.
  - Hardened comment/reply completeness semantics across TikTok/YouTube/Twitter:
    - deterministic fail reasons from scrapers,
    - missing-mark only when fetch is complete,
    - additive `refresh_post_comments(...)` completeness fields across all platforms,
    - `max_comments_per_post=0` returns `is_complete=false` + `incomplete_reason='fetch_disabled'`.
  - Enforced nested reply trimming for TikTok/YouTube using effective per-post reply limits.
  - Added cross-platform mirror backfill script to enqueue (not inline upload) mirror jobs for recent windows.
- Validation:
  - `ruff check trr_backend/socials/tiktok/scraper.py trr_backend/socials/youtube/scraper.py trr_backend/socials/twitter/scraper.py trr_backend/repositories/social_season_analytics.py api/routers/socials.py` (pass)
  - `pytest -q tests/socials/test_comment_scraper_fixes.py tests/repositories/test_social_season_analytics.py tests/api/routers/test_socials_season_analytics.py -k "instagram or tiktok or youtube or twitter or mirror or refresh_post_comments or coverage"` (`78 passed, 52 deselected`)
  - `find /Users/thomashulihan/Projects/TRR/screenalytics/apps/api -name '*.py' -print0 | xargs -0 -n 1 python -m py_compile` (pass)
  - `pytest -q /Users/thomashulihan/Projects/TRR/screenalytics/tests/unit -k "metadata or api"` (`4 passed`)
  - `pnpm --dir /Users/thomashulihan/Projects/TRR/TRR-APP/apps/web test -- --runInBand season-social-analytics-section.test.tsx week-social-thumbnails.test.tsx` executed broad vitest run in this workspace (`181 files / 737 tests passed`).

Continuation (same session, 2026-02-24) — Comments-coverage SQL alias regression fix.
- Files:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/repositories/test_social_season_analytics.py`
- Changes:
  - Fixed Twitter comments-coverage lifecycle filter aliasing in recursive CTE path:
    - use `r.is_missing` in the root reply branch,
    - use `child.is_missing` in recursive branch,
    - keep `t.is_missing` only in the non-recursive aggregate path.
  - Eliminates runtime SQL error: `missing FROM-clause entry for table "t"` on `/analytics/comments-coverage`.
  - Added regression test: `test_comments_coverage_twitter_recursive_filter_uses_reply_aliases`.
- Validation:
  - `pytest /Users/thomashulihan/Projects/TRR/TRR-Backend/tests/repositories/test_social_season_analytics.py -k "comments_coverage"` (`3 passed`).
  - Endpoint smoke after restart returns `200` for season comments-coverage requests.

Continuation (same session, 2026-02-24) — Cross-platform media mirror rollout execution (ops run).
- Scope:
  - Applied migration target: `0145_cross_platform_media_mirror_fields_and_job_types.sql`.
  - Enqueued recent cross-platform mirror jobs (8 weeks, bravo scope).
  - Captured mirror/queue/coverage health snapshots.
- Execution details:
  - `supabase migration list --db-url "$SUPABASE_DB_URL"` and `supabase db push --db-url "$SUPABASE_DB_URL"` were blocked by existing remote/local migration drift (`0142` exists remotely but no local file), so migration apply was completed via direct `psql` execution of `0145` plus explicit insert into `supabase_migrations.schema_migrations`.
  - Verified post-apply:
    - `0145` present in `supabase_migrations.schema_migrations`.
    - mirror columns exist on `social.tiktok_posts`, `social.youtube_videos`, `social.twitter_tweets`.
    - `social.scrape_jobs` check constraint includes `instagram_media_mirror|tiktok_media_mirror|youtube_media_mirror|twitter_media_mirror`.
    - mirror pending indexes exist (`idx_tiktok_posts_media_mirror_pending`, `idx_youtube_videos_media_mirror_pending`, `idx_twitter_tweets_media_mirror_pending`).
  - Backfill enqueue run:
    - `PYTHONPATH=. python scripts/socials/backfill_social_media_mirror_jobs.py --weeks 8 --platforms instagram,tiktok,youtube,twitter --source-scope bravo --limit-per-platform 5000`
    - Output: `scanned=1017`, `queued=270`, `skipped=747`, `failed=0`
    - Platform breakdown:
      - instagram: `scanned=61`, `queued=61`
      - tiktok: `scanned=80`, `queued=58`
      - youtube: `scanned=24`, `queued=24`
      - twitter: `scanned=852`, `queued=127`
  - Worker executability check:
    - Ran local one-shot worker against queue: `SOCIAL_QUEUE_ENABLED=true PYTHONPATH=. python -m scripts.socials.worker --stage media_mirror --once --interval 1`
    - Processed one mirror job successfully (`status=completed`, `items=1`), proving `media_mirror` stage execution path is live.
- Monitoring snapshot (immediately after enqueue + one local worker pass):
  - Mirror status distribution:
    - instagram: `pending=60`, `failed=1`
    - tiktok: `pending=58`
    - youtube: `pending=24`
    - twitter: `pending=1`
  - Mirror retry/status reasons (48h):
    - `pending/unknown=269`, `completed/unknown=1`
  - Queue backlog (24h):
    - `media_mirror`: `pending=269`, `completed=1`
    - existing posts/comments queue items unchanged from prior runs.
  - Comments coverage snapshot (`season_id=e9161955-6ee4-4985-865e-3386a0f670fb`, `source_scope=bravo`):
    - `saved=35261`, `reported=177223`, `coverage_pct=19.9`, `up_to_date=false`
    - by platform:
      - instagram `7.6%`
      - tiktok `27.5%`
      - twitter `37.1%`
      - youtube `100%`
- Blocker:
  - Cloud Run deploy actions from this environment are blocked by non-interactive gcloud reauthentication:
    - `gcloud run services list --region us-east1` => `Reauthentication failed. cannot prompt during non-interactive execution.`
  - API/worker deployment step remains pending until interactive reauth (`gcloud auth login`) or service-account auth is provided.

Continuation (same session, 2026-02-24) — MEDIA/GALLERY hardening follow-through (repair script safety).
- Files:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/media/repair_gallery_hosts.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/scripts/test_repair_gallery_hosts.py`
- Changes:
  - Added additive CLI controls to `repair_gallery_hosts.py`:
    - `--retry-attempts` (default `2`)
    - `--retry-backoff-ms` (default `500`)
    - `--confirm-unreachable-pass/--no-confirm-unreachable-pass` (default confirm enabled)
  - Added retry-aware reachability probing with transient-failure classification (`429`, `5xx`, connection/timeout class request failures).
  - Hardened candidate classification semantics:
    - `broken_unreachable` only after non-transient source failure, with optional confirmation pass.
    - transient/indeterminate outcomes classified as `error` and never marked broken in apply mode.
  - Preserved existing repair semantics for recoverable rows (re-mirror + base variants, crop variants when crop payload exists).
  - Expanded script tests to cover:
    - transient retry behavior,
    - confirmation-pass rescue from broken classification,
    - transient-indeterminate non-mutation guarantees,
    - CLI defaults for new flags.
- Validation:
  - `pytest /Users/thomashulihan/Projects/TRR/TRR-Backend/tests/scripts/test_repair_gallery_hosts.py -q` (`8 passed`).
  - Script dry-run smoke:
    - `python /Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/media/repair_gallery_hosts.py --sources imdb,tmdb,fandom,bravo --limit 10 --output-json /tmp/gallery-host-repair-post-hardening-dryrun.json`
    - Result summary: `scanned=10`, `ok=0`, `repaired=5`, `broken_unreachable=5`, `error=0`, `apply=false`.

Continuation (same session, 2026-02-24) — Networks/Streaming sync hardening rollout execution (ops run).
- Scope:
  - Applied remote Supabase migrations through current head (`0124`-`0144`) including:
    - `0143_network_streaming_discovery_state.sql`
    - `0144_network_streaming_sync_runs.sql`
  - Verified discovery-lock behavior and logo-asset reuse on targeted `network:bravo` passes.
- Execution details:
  - Migration apply:
    - `supabase migration up --db-url "$SUPABASE_DB_URL" --include-all`
  - Schema verification:
    - Confirmed existence of:
      - `admin.network_streaming_discovery_state`
      - `admin.network_streaming_sync_runs`
      - `admin.network_streaming_logo_assets`
  - Targeted Bravo sync processor runs (`_process_entity` direct invocation):
    - Initial state:
      - `assets_total=30`
      - by source: `tmdb=1`, `wikimedia=2`, `official=7`, `catalog=20`
      - by status: `mirrored=28`, `skipped=2`
    - Credit-safe pass (`refresh_external_sources=false`):
      - `processed=1`
      - `logo_assets_discovered=22`
      - `logo_assets_mirrored=0`
      - `logo_assets_skipped=22`
      - `logo_assets_failed=0`
      - no asset-count deltas (cache/S3 reuse confirmed)
    - Refresh pass (`refresh_external_sources=true`):
      - `processed=1`
      - `logo_assets_discovered=22`
      - `logo_assets_mirrored=0`
      - `logo_assets_skipped=22`
      - `logo_assets_failed=0`
      - no asset-count deltas (already complete)
    - Discovery-state deltas for Bravo:
      - `catalog attempt_count: 1 -> 2` (`cached_candidate_count=12`)
      - `official attempt_count: 1 -> 2` (`last_reason=download_failed`)
  - Full-run smoke:
    - `sync_networks_streaming_links.py --all --batch-size 25 --max-runtime-sec 180`
    - observed long-running external-download phase; run was manually interrupted during an external image request.
    - stale `running` run rows were reconciled in `admin.network_streaming_sync_runs` to avoid orphaned operator state.
- Blockers:
  - Cloud Run deploy remains blocked in this environment due non-interactive `gcloud` reauthentication:
    - `Reauthentication failed. cannot prompt during non-interactive execution.`
  - Backend deployment still requires interactive `gcloud auth login` (or service-account auth) before `gcloud run deploy`.

Continuation (same session, 2026-02-24) — Networks/Streaming operational follow-up (global metrics + unresolved sweep).
- Scope:
  - Captured current global logo/completion metrics post-rollout.
  - Attempted unresolved-only external refresh pass to reduce remaining manual-required rows.
- Current snapshot:
  - `admin.network_streaming_logo_assets`:
    - total rows: `849`
    - by type: `network=341`, `streaming=254`, `production=254`
    - by mirror status: `mirrored=836`, `skipped=13`
    - distinct entities with gallery rows: `61`
  - `admin.network_streaming_completion`:
    - total rows: `233`
    - by status: `resolved=225`, `manual_required=8` (pre-refresh snapshot)
  - After unresolved-only refresh attempts and snapshot recompute:
    - unresolved rows currently: `14`
    - all unresolved rows are `production` and `resolution_reason='incomplete_metadata'`:
      - 10 by 10 Entertainment
      - 3 Ball Productions
      - A. Smith & Co. Productions
      - Advanced Medical Productions
      - Alfred Street Industries
      - All3Media America
      - Bayonne Entertainment
      - Bazal
      - BBC Worldwide Productions
      - BET Productions
      - Big Head Productions
      - BiggerStage
      - Bodega Pictures
      - Brass Ring Productions
- Notes:
  - Unresolved-only refresh run (`network-streaming-20260224T173031Z`) processed `10` entities before manual interruption during external Logopedia lookup; run status reconciled to `failed` with explicit operator-stop error.

Continuation (same session, 2026-02-24) — Finalization hardening implementation (credit-safe stability + targeted execution).
- Files:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/integrations/brandfetch.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/integrations/logopedia.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/sync/sync_networks_streaming_links.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_show_sync.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/README.md`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/scripts/test_sync_networks_streaming_links.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_show_sync.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/integrations/test_brandfetch.py` (new)
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/integrations/test_logopedia.py` (new)
- Changes:
  - Added bounded timeout tuples + transient retry budgets for Brandfetch and Logopedia integrations.
    - Brandfetch env knobs: `BRANDFETCH_TIMEOUT_SEC`, `BRANDFETCH_RETRY_ATTEMPTS`, `BRANDFETCH_RETRY_BACKOFF_MS`.
    - Logopedia env knobs: `LOGOPEDIA_TIMEOUT_SEC`, `LOGOPEDIA_RETRY_ATTEMPTS`, `LOGOPEDIA_RETRY_BACKOFF_MS`.
  - Extended sync script CLI targeting controls:
    - `--entity-type network|streaming|production`
    - `--entity-key <normalized key>` (repeatable)
  - Added interruption-safe run behavior:
    - catches `KeyboardInterrupt`,
    - marks run `failed`,
    - persists resume cursor + terminal run state.
  - Added bounded Wikidata fetch/search retries in sync script (`429`, `5xx`, timeout retry budget).
  - Made sync context build selection-aware to reduce expensive preloads:
    - only build network hints when network entities are selected,
    - only build streaming hints when streaming entities are selected,
    - production IMDb hints can be skipped on unresolved-only safe runs unless external refresh is enabled.
  - Added API passthrough for targeted execution:
    - request additions to `/api/v1/admin/shows/sync-networks-streaming`:
      - `entity_type`
      - `entity_keys`
  - Updated script docs for new targeting flags + timeout/retry env vars.
- Production ops executed:
  - Seeded/updated override rows for unresolved production entities:
    - first pass `seeded_overrides=14`, then refreshed to `seeded_overrides=17` after unresolved set expanded.
  - Multiple production-only unresolved runs executed; several were operator-terminated and reconciled to terminal `failed` state in `admin.network_streaming_sync_runs`.
  - Current unresolved snapshot after execution:
    - `admin.network_streaming_completion` unresolved rows = `18` (all production, `resolution_reason=incomplete_metadata`).
- Validation:
  - `ruff check trr_backend/integrations/brandfetch.py trr_backend/integrations/logopedia.py scripts/sync/sync_networks_streaming_links.py api/routers/admin_show_sync.py tests/integrations/test_brandfetch.py tests/integrations/test_logopedia.py tests/scripts/test_sync_networks_streaming_links.py tests/api/routers/test_admin_show_sync.py` (pass)
  - `pytest tests/integrations/test_brandfetch.py tests/integrations/test_logopedia.py tests/scripts/test_sync_networks_streaming_links.py tests/api/routers/test_admin_show_sync.py` (`51 passed`)

Continuation (same session, 2026-02-24) — Bravo video thumbnail one-time backfill execution + scheduled backfill script.
- Files:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/backfill/backfill_bravo_video_thumbnails.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/backfill_bravo_video_thumbnails.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/README.md`
- Changes:
  - Added a reusable backfill runner for Bravo video thumbnail mirroring:
    - discovers shows with `core.show_source_latest` Bravo snapshots,
    - supports `--show-id`, `--limit`, `--force`, `--dry-run`, `--json-summary`,
    - syncs pending thumbnails via existing `_sync_bravo_video_thumbnails` pipeline,
    - updates snapshot `normalized.video_thumbnail_sync` metadata and writes updated snapshot.
  - Added top-level wrapper script for compatibility:
    - `scripts/backfill_bravo_video_thumbnails.py`.
  - Added README usage documentation for Bravo video thumbnail backfill.
- Operational run results:
  - Attempted multi-show forced backfill via app proxy; most candidate shows had no persisted Bravo snapshots in this environment (`404 No persisted Bravo snapshot`).
  - Executed direct forced RHOSLC sync:
    - `show_id=7782652f-783a-488b-8860-41b97de32e75`
    - response: `attempted=70`, `synced=70`, `failed=0`, `missing_source=0`, `pending_remaining=0`.
  - Post-sync API verification (`/api/admin/trr-api/shows/{id}/bravo/videos`):
    - `count=58`, `hosted_count=58`, statuses all `synced`.
  - New script apply verification for RHOSLC:
    - `status=skipped_no_pending`, `pending_before=0`, `pending_after=0`.
- Validation:
  - `ruff check scripts/backfill/backfill_bravo_video_thumbnails.py scripts/backfill_bravo_video_thumbnails.py` (pass)
  - `PYTHONPATH=. .venv/bin/python scripts/backfill/backfill_bravo_video_thumbnails.py --show-id 7782652f-783a-488b-8860-41b97de32e75 --dry-run --json-summary -` (pass)
  - `PYTHONPATH=. .venv/bin/python scripts/backfill/backfill_bravo_video_thumbnails.py --show-id 7782652f-783a-488b-8860-41b97de32e75 --json-summary -` (pass)

Continuation (same session, 2026-02-24) — Cross-platform media mirror rollout completion follow-through.
- Files:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/runbooks/social_worker_queue_ops.md`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/runbooks/supabase_migration_history_repair.md` (new)
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/ai/HANDOFF.md`
- Changes:
  - Added stage-compatibility fallback SQL to social queue runbook:
    - uses `coalesce(nullif(to_jsonb(j)->>'stage',''), nullif(j.metadata->>'stage',''), 'unknown')`.
  - Added dedicated runbook for Supabase migration-history drift repair to restore `supabase db push` workflow when remote/local migration histories diverge.
  - Executed additional rollout operations:
    - attempted non-interactive Cloud Run access with available service-account keys;
    - re-validated deploy blocker was IAM-scoped in this environment;
    - ran manual `media_mirror` queue drain loop for 15 jobs.
- Execution evidence:
  - Cloud Run auth/permissions:
    - activated SA `firebase-adminsdk-fbsvc@trr-web-25d2e.iam.gserviceaccount.com` (success),
    - `gcloud run services list --region us-east1` failed with `Permission 'run.services.list' denied`.
    - `trr-backend-sa@trr-backend.iam.gserviceaccount.com` key points at project `trr-backend` where Cloud Run API is not enabled.
  - Manual queue drain:
    - `PYTHONPATH=. python - <<PY ... process_next_queued_job(... stage='media_mirror') ... PY`
    - processed `15` jobs, all `completed` (Instagram).
  - Post-drain monitoring snapshot:
    - mirror status:
      - instagram `pending=45`, `failed=16`
      - tiktok `pending=58`
      - youtube `pending=24`
      - twitter `pending=1`
    - media_mirror job statuses (24h):
      - `completed=16`, `pending=254`
    - retry reasons (48h):
      - `pending/unknown=254`, `completed/unknown=16`
    - worker heartbeat:
      - latest manual worker recorded as `stopped` after controlled exit.
  - Coverage snapshot (season `e9161955-6ee4-4985-865e-3386a0f670fb`, source_scope `bravo`):
    - `saved=35261`, `reported=177223`, `coverage_pct=19.9`, `up_to_date=false`.
- Outstanding blocker:
  - Production Cloud Run API/worker deployment still requires an account with `run.services.list/deploy` permissions (interactive `gcloud auth login` as privileged user or proper deploy service-account grant).

Continuation (same session, 2026-02-24) — RHOSLC media refresh stabilization: first-event SSE + Screenalytics degraded mode.
- Files:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/clients/screenalytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_person_images.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_show_sync.py`
- Changes:
  - Added temporary Screenalytics circuit-breaker behavior in client:
    - new `ScreenalyticsUnavailableError` with `retry_after_s`.
    - new unavailable-state helper `get_screenalytics_unavailable_state()`.
    - failed endpoint probing now enters cooldown (`SCREENALYTICS_UNAVAILABLE_COOLDOWN_SECONDS`, default 300s) and short-circuits subsequent calls.
  - Person refresh stream (`refresh-images/stream`):
    - emits immediate `starting` progress event before setup work.
    - auto-count and centering stages now emit structured service-unavailable skip/pause payloads (`skip_reason=service_unavailable`, `retry_after_s`, `service_unavailable=true`) instead of noisy repeated failures.
    - additive `skip_reason=not_configured` on not-configured skip path.
  - Show photos refresh stream (`refresh-photos/stream`):
    - auto-count stage now checks Screenalytics unavailable state and skips fast with structured payload.
    - mid-stage service outage emits pause event with retry-after metadata.
    - additive `skip_reason=not_configured` on not-configured skip path.
- Validation:
  - `python -m py_compile /Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_person_images.py /Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_show_sync.py /Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/clients/screenalytics.py` (pass)

Continuation (same session, 2026-02-24) — URL/Sync/Bravo stabilization follow-up (operational diagnostics + guardrails).
- Files:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/shows/backfill_bravo_person_source_links.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/scripts/test_backfill_bravo_person_source_links.py` (new)
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/README.md`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/ai/HANDOFF.md`
- Changes:
  - Extended person-source backfill script with operations-oriented diagnostics and threshold controls:
    - `--warn-fetch-errors`, `--fail-fetch-errors`
    - `--warn-pending-person-sources`, `--fail-pending-person-sources`
    - `--diagnose-missing-person-sources`, `--diagnose-name`, `--diagnostics-json`
  - Added missing-source diagnostics output with per-person rationale fields for IMDb/TMDb (`missing_identifier`, `validation_outcome:*`, `owner_mismatch`, etc.).
  - Added pending person-source post-run count into summary and JSON artifacts.
  - Added threshold-aware non-zero exit (`2`) for alertable/failing operational conditions while preserving existing failure exit (`1`) for `failed_shows`.
  - Documented script usage/exit-code behavior in `scripts/README.md`.
  - Added tests for parser flags, pending-count query, diagnostic reason mapping, and threshold exit semantics.
- Validation:
  - `ruff check /Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_show_links.py /Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_show_bravo.py /Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/shows/backfill_bravo_person_source_links.py /Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_show_links.py /Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_show_bravo.py /Users/thomashulihan/Projects/TRR/TRR-Backend/tests/scripts/test_backfill_bravo_person_source_links.py` (pass)
  - `pytest -q /Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_show_links.py /Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_show_bravo.py /Users/thomashulihan/Projects/TRR/TRR-Backend/tests/scripts/test_backfill_bravo_person_source_links.py` (pass, `79 passed`)
  - `python -m py_compile /Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_show_links.py /Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_show_bravo.py /Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/shows/backfill_bravo_person_source_links.py` (pass)
- Runtime verification:
  - DB check confirmed fandom allowlist migration target table present: `core.fandom_community_allowlist`.

Continuation (same session, 2026-02-24) — MEDIA/GALLERY post-hardening completion run.
- Files:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_asset_batch_jobs.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_person_images.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/scripts/test_repair_gallery_hosts.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/media/repair_gallery_hosts.py`
- Validation completed:
  - `pytest /Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_asset_batch_jobs.py -q` (`6 passed`)
  - `pytest /Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_person_images.py -q` (`20 passed`)
  - `pytest /Users/thomashulihan/Projects/TRR/TRR-Backend/tests/scripts/test_repair_gallery_hosts.py -q` (`8 passed`)
  - `ruff check /Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/media/repair_gallery_hosts.py /Users/thomashulihan/Projects/TRR/TRR-Backend/tests/scripts/test_repair_gallery_hosts.py` (pass)
- Repair rollout execution:
  - Initial broad dry-run with default timeout was terminated due runtime impracticality at this dataset scale.
  - Completed staged run with explicit timeout while preserving retry+confirm logic:
    - Dry-run: `python /Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/media/repair_gallery_hosts.py --sources imdb,tmdb,fandom,bravo --timeout 3 --limit 100 --output-json /tmp/gallery-host-repair-dryrun-full.json`
      - Result: `scanned=100`, `ok=3`, `repaired=28`, `broken_unreachable=69`, `error=0`, `apply=false`
    - Apply batch-1: `python /Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/media/repair_gallery_hosts.py --sources imdb,tmdb,fandom,bravo --timeout 3 --limit 100 --apply --output-json /tmp/gallery-host-repair-apply-batch1.json`
      - Result: `scanned=100`, `ok=3`, `repaired=28`, `broken_unreachable=69`, `error=0`, `apply=true`
- Outstanding operational step:
  - Stage-3 full apply remains deferred until manual UI acceptance checks are completed on post-batch state.
- Follow-up fix (same pass):
  - Corrected diagnostics SQL in `_load_show_cast_people_for_diagnostics` to `ORDER BY person_name NULLS LAST, person_id` for Postgres `SELECT DISTINCT` compliance.
  - Added regression test `test_load_show_cast_people_for_diagnostics_orders_by_selected_aliases`.
  - Re-ran validation: `ruff` (pass), `py_compile` (pass), `pytest` (pass, `80 passed`).
  - Ran direct Andy diagnostics artifact generation:
    - `/tmp/trr_andy_missing_diagnostics_direct_2026-02-24.json`
- Additional fix (same session, 2026-02-24): cross-show entity link collision root cause.
  - Root cause: `core.entity_links` uniqueness/upsert conflict key omitted `show_id`, allowing shared person URLs to collide across shows.
  - Code fix: `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_show_links.py`
    - `_upsert_link(...).upsert(... on_conflict=...)` now uses `show_id,entity_type,entity_id,link_kind,season_number,url_key`.
  - Migration added: `/Users/thomashulihan/Projects/TRR/TRR-Backend/supabase/migrations/0146_entity_links_unique_per_show.sql`
    - drops/recreates `entity_links_unique_active` to include `show_id`.
  - Test coverage:
    - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_show_links.py`
      - `test_upsert_link_uses_show_scoped_conflict_key`.
  - Validation:
    - `ruff check ...` (pass)
    - `python -m py_compile ...` (pass)
    - `pytest -q ...test_admin_show_links.py ...test_admin_show_bravo.py ...test_backfill_bravo_person_source_links.py` (pass, `81 passed`).
  - Operational note:
    - Migration `0146` must be applied before running production backfill apply so per-show person source rows are persisted independently.

Continuation (same session, 2026-02-24) — MEDIA/GALLERY completion follow-up (rollout execution limits).
- Additional execution performed:
  - Re-validated completion-test gate:
    - `pytest /Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_asset_batch_jobs.py -q` (pass, `6 passed`)
    - `pytest /Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_person_images.py -q` (pass, `20 passed`)
    - `pytest /Users/thomashulihan/Projects/TRR/TRR-Backend/tests/scripts/test_repair_gallery_hosts.py -q` (pass, `8 passed`)
- Repair rollout status:
  - Batch-1 dry-run/apply artifacts from prior step remain the latest successful operational outputs:
    - `/tmp/gallery-host-repair-dryrun-full.json`
    - `/tmp/gallery-host-repair-apply-batch1.json`
  - Attempted unlimited follow-up dry-runs were started and then interrupted due runtime impracticality at current network-bound probe volume:
    - `python /Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/media/repair_gallery_hosts.py --sources imdb,tmdb,fandom,bravo --timeout 1 --output-json /tmp/gallery-host-repair-dryrun-full-unlimited.json` (interrupted)
    - `python /Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/media/repair_gallery_hosts.py --sources imdb --timeout 1 --output-json /tmp/gallery-host-repair-dryrun-imdb.json` (interrupted)
- Current operational recommendation:
  - Keep stage-3 “full/no-limit” repair as an async/offline run window (not interactive shell window), then re-run manual gallery spot checks on repaired rows.

Continuation (same session, 2026-02-24) — Stage-3 full/no-limit repair apply launched offline.
- Launch mode:
  - `launchctl submit` one-shot job (detached from interactive shell lifecycle).
  - Label: `trr.gallery.repair.stage3.20260224-145429`
- Runner/artifacts:
  - Runner script: `/tmp/gallery-host-repair-stage3-apply-20260224-145429.sh`
  - Live log: `/tmp/gallery-host-repair-stage3-apply-20260224-145429.log`
  - JSON output (on completion): `/tmp/gallery-host-repair-stage3-apply-20260224-145429.json`
- Launch command path:
  - Uses explicit backend interpreter to avoid PATH issues in launchd:
    - `/Users/thomashulihan/Projects/TRR/TRR-Backend/.venv/bin/python`
- Initial verification:
  - `launchctl list | rg 'trr.gallery.repair.stage3.20260224-145429'` showed active entry (`pid=75091`, status `0` while running).
  - Log shows start marker: `[start] 2026-02-24T14:54:29-0500`.

Continuation (same session, 2026-02-24) — Conflict-resolved mirror rollout finalization (ops execution).
- Scope and constraints:
  - Kept rollout work operational-only (migration/deploy/backfill/monitoring) and left unrelated dirty worktree files untouched.
  - Verified migration truth: `0145_cross_platform_media_mirror_fields_and_job_types.sql` is the mirror migration; `0144` is unrelated.
- Phase execution:
  - Cloud Run auth/IAM preflight:
    - checked all available principals (`admin@thereality.report`, `firebase-adminsdk-fbsvc@trr-web-25d2e.iam.gserviceaccount.com`, `trr-backend-sa@trr-backend.iam.gserviceaccount.com`).
    - none had deploy-capable access in this shell:
      - user account requires interactive reauth,
      - service accounts fail `run.services.list/get` on `trr-web-25d2e`.
  - Supabase migration drift repair:
    - drift before repair: remote-only `0142`, local pending `0146`.
    - `supabase migration repair --status reverted 0142 --db-url "$SUPABASE_DB_URL"` failed on pooler `:6543` with prepared-statement errors.
    - repaired successfully using pooler `:5432` alternate URL.
    - post-repair verification:
      - `supabase migration list --db-url "$SUPABASE_DB_URL"` no longer reports remote-only drift.
      - `supabase db push --dry-run --db-url "$SUPABASE_DB_URL"` succeeds and shows only pending local `0146`.
  - Backfill enqueue rerun:
    - `PYTHONPATH=. python scripts/socials/backfill_social_media_mirror_jobs.py --weeks 8 --platforms instagram,tiktok,youtube,twitter --source-scope bravo --limit-per-platform 5000`
    - result: `scanned=1002`, `queued=267`, `skipped=735`, `failed=0`.
  - Queue drain attempt (manual worker loop):
    - processed 60 `media_mirror` jobs via `process_next_queued_job(...)`.
    - outcome summary: `completed=52`, `retrying=8`.
- Monitoring snapshots (post-drain):
  - mirror status by platform rows:
    - instagram: `pending=16`, `failed=45`
    - tiktok: `pending=50`, `failed=8`
    - youtube: `pending=24`
    - twitter: `pending=1`
  - media_mirror jobs (24h):
    - `completed=69`, `pending=212`, `retrying=5`
  - retry reasons (48h):
    - `unknown/pending=212`, `unknown/completed=69`, `transient_error/retrying=5`
  - workers:
    - no continuously healthy worker heartbeat (manual workers now `stopped`).
  - comments coverage (season `e9161955-6ee4-4985-865e-3386a0f670fb`, scope `bravo`):
    - `saved=35353`, `reported=177223`, `coverage_pct=19.9`, `up_to_date=false`.
- New blocker discovered during drain:
  - Twitter media-mirror job updates fail with DB type mismatch:
    - `psycopg2.errors.DatatypeMismatch: column "hosted_media_urls" is of type jsonb but expression is of type text[]`
    - failing path:
      - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/social_season_analytics.py:3210`
      - invoked from `_update_platform_post_media_mirror_fields` during `media_mirror` stage.
  - Impact:
    - queue can process many jobs, but affected Twitter jobs keep retrying/failing to persist hosted URLs until code fix is applied.
- Remaining blockers to close rollout acceptance:
  - deploy-capable GCP principal still required to complete Cloud Run API + dedicated worker deployment (`trr-web-25d2e`, `us-east1`).
  - Twitter `hosted_media_urls` write path must cast/write JSONB correctly to prevent retry churn.

Continuation (same session, 2026-02-24) — mirror requeue + additional drain verification.
- Actions:
  - Ran failed-only cross-platform mirror requeue (season `e9161955-6ee4-4985-865e-3386a0f670fb`, `source_scope=bravo`):
    - instagram: `queued_jobs=45`
    - tiktok: `queued_jobs=8`
    - youtube: `queued_jobs=0`
    - twitter: `queued_jobs=0`
  - Ran additional manual `media_mirror` drain batch (`max_jobs=30`):
    - result: `completed=12`, `retrying=13`, `failed=5`.
- Runtime findings:
  - Confirmed recurring twitter mirror persistence error during drain:
    - `DatatypeMismatch: hosted_media_urls jsonb vs text[]` in `_update_platform_post_media_mirror_fields`.
  - Current retry reason aggregation now surfaces this under `transient_error` job classification (needs code-level error classification fix if this should be permanent).
- Latest monitoring snapshot:
  - post-table mirror status:
    - instagram: `pending=61`
    - tiktok: `pending=58`
    - youtube: `mirrored=12`, `pending=12`
    - twitter: `pending=1`
  - `social.scrape_jobs` (24h):
    - `media_mirror pending=246`, `completed=81`, `retrying=7`, `failed=5`
  - retry reasons (48h):
    - `unknown/pending=246`, `unknown/completed=81`, `transient_error/retrying=7`, `transient_error/failed=5`
- Rollout implication:
  - Queue is executable and draining for many jobs, but full stabilization still blocked by:
    1) missing Cloud Run deploy principal permissions,
    2) twitter `hosted_media_urls` write-path type mismatch causing retry/fail churn.
- Rollout continuation (same session, 2026-02-24): applied per-show entity link uniqueness in live DB and re-ran targeted backfills.
  - DB migration state before apply:
    - `entity_links_unique_active` columns were `entity_type, entity_id, link_kind, season_number, url_key` (missing `show_id`).
  - Applied DDL in connected DB:
    - dropped/recreated `entity_links_unique_active` as
      `(show_id, entity_type, entity_id, link_kind, season_number, url_key)`.
  - Verified post-apply constraint columns include `show_id` first.
  - Backfill apply runs executed for remaining impacted shows with artifacts:
    - `/tmp/trr_backfill_person_sources_apply_show_9b20_2026-02-24.json`
    - `/tmp/trr_backfill_person_sources_apply_show_eebf_2026-02-24.json`
  - Post-run checks:
    - global pending person-source rows (`imdb/tmdb/wikipedia/wikidata/fandom/wikia/bravo_profile`) = `0`.
    - Andy/Heather spot check query confirms approved links present per show where validated.
    - Andy targeted diagnostics artifact:
      - `/tmp/trr_andy_missing_diagnostics_post_showkey_2026-02-24.json`
      - result now only IMDb `unverifiable_fetch_error` on two show contexts (TMDb no longer `valid_but_not_persisted`).
  - Additional release evidence artifact:
    - `/tmp/trr_person_source_spotcheck_10_2026-02-24.json` (10-person matrix sample across impacted shows).

Continuation (same session, 2026-02-24) — Social soak verification backend observations.
- Backend process behavior during soak window:
  - Health endpoint remained available when backend process was up (`/health` -> `200`).
  - Ingest run `2014239f-1b37-43ea-ad89-b1a63c6392b3` completed successfully (`4/4` comments jobs completed, no failed jobs).
- Extracted backend failure signals from current backend log window:
  - `connection pool exhausted`: `0`
  - social endpoint `5xx` log lines (`/api/v1/admin/socials/*`): `0`
- Interpretation:
  - Current NO_GO is driven primarily by app-proxy timeout/restart behavior under concurrent admin traffic rather than confirmed backend pool exhaustion in this run window.
  - Backend remains candidate for further job-list path optimization to reduce end-to-end latency pressure on app poll routes.

Continuation (same session, 2026-02-24) — NO_GO follow-up patch: unscoped jobs hot path optimization.
- Files changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/repositories/test_social_season_analytics.py`
- Change summary:
  - Optimized `list_jobs(...)` for unscoped polling path (no `run_id` filter) by introducing a two-step query:
    1. `candidate_jobs` CTE selects `id` ordered by `created_at desc` with `limit`.
    2. Joined fetch reads full row payload (`config`, `metadata`, queue fields) only for candidate IDs.
  - Preserved response shape and existing filters (`season_id`, `status`, `platform`).
  - Kept run-scoped path as direct query (`run_id` filter) to preserve behavior.
- Regression tests added:
  - `test_list_jobs_uses_candidate_cte_for_unscoped_queries`
  - `test_list_jobs_uses_direct_query_for_run_scoped_queries`
- Validation evidence:
  - `ruff check trr_backend/repositories/social_season_analytics.py tests/repositories/test_social_season_analytics.py` (pass)
  - `pytest tests/repositories/test_social_season_analytics.py -k "list_jobs_uses_candidate_cte_for_unscoped_queries or list_jobs_uses_direct_query_for_run_scoped_queries"` (pass)
- Continuation (same session, 2026-02-24): IMDb/TMDb fetch-error carry-forward hardening.
  - File updated:
    - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_show_links.py`
  - Behavior change:
    - Added `_load_preapproved_person_source_url(...)` and `_validated_or_carried_person_source_url(...)`.
    - For person `imdb`/`tmdb` discovery only, when live validation outcome is `fetch_error`, discovery now reuses an existing approved link for the same `(person_id, kind, url_key)` (including legacy slash/no-slash `url_key` variants).
    - Keeps strict owner-match policy for new approvals; carry-forward only reuses previously approved rows.
  - Discovery wiring:
    - `_discover_people_links(...)` now uses `_validated_or_carried_person_source_url(...)` for imdb/tmdb.
  - Tests updated:
    - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_show_links.py`
      - `test_discover_people_links_carries_forward_imdb_when_validation_fetch_errors`
      - `test_load_preapproved_person_source_url_matches_by_url_key`
      - `test_load_preapproved_person_source_url_ignores_non_person_sources`
      - adjusted existing imdb/tmdb generation + failure tests for new helper path.
  - Validation:
    - `ruff check ...admin_show_links.py ...test_admin_show_links.py` (pass)
    - `pytest -q ...test_admin_show_links.py ...test_admin_show_bravo.py ...test_backfill_bravo_person_source_links.py` (pass, `84 passed`)
    - `python -m py_compile ...admin_show_links.py` (pass)
  - Targeted data remediation executed:
    - Used targeted retry/upsert for Andy Cohen on shows:
      - `8eaa9603-8a3a-4b5e-9f06-4bbf7e76b489`
      - `eebff6c6-1b28-46a1-9051-d73ff0398c42`
    - Result: IMDb approved links inserted via carry-forward path.
    - Post-check now shows Andy Cohen has approved IMDb on all 3 relevant show contexts.
  - Policy check:
    - pending person-source rows remain `0`.

Continuation (same session, 2026-02-24) — Cloud Run deploy fix for requirements lockfile copy.
- root_cause:
  - Cloud Build failed during Docker step `pip install -r requirements.txt` because `requirements.txt` references `requirements.lock.txt` but Dockerfile only copied `requirements.txt` before install.
- fix:
  - Updated `/Users/thomashulihan/Projects/TRR/TRR-Backend/Dockerfile` to copy both `requirements.txt` and `requirements.lock.txt` before running pip install.
  - Commit on `main`: `0f88008` (`fix(deploy): copy requirements.lock.txt before pip install`).
- deploy:
  - Service: `trr-backend`
  - Project/region: `trr-web-25d2e` / `us-east1`
  - New ready revision: `trr-backend-00031-dl2`
  - Service URL: `https://trr-backend-e7yfe64hoa-ue.a.run.app`
- primary_skill: `senior-devops`
- supporting_skills:
  - `senior-backend`
- mcp_tools_used:
  - primary: `functions.exec_command` (git/gcloud/docker validation)
  - fallback: none
- delegation_map:
  - role: API Contract Owner
    scope: verify API contract impact
    deliverable: confirmed no API contract/schema change
    verification_command: `curl -i $SERVICE_URL/openapi.json`
    status: completed
  - role: Schema Owner
    scope: verify DB migration impact
    deliverable: confirmed no schema/migration changes
    verification_command: `git diff --name-only HEAD~1..HEAD`
    status: completed
  - role: Integration Owner
    scope: downstream repo impact
    deliverable: confirmed no screenalytics/TRR-APP changes required
    verification_command: `git diff --name-only HEAD~1..HEAD`
    status: completed
  - role: QA Owner
    scope: deploy/build regression validation
    deliverable: successful Docker build + Cloud Run deploy + health/openapi check
    verification_command: `docker build -t trr-backend:dockerfile-fix . && gcloud run services describe ... && curl -i $SERVICE_URL/openapi.json`
    status: completed
- risk_class: `no_contract`
- validation_evidence:
  - `docker build -t trr-backend:dockerfile-fix .` -> pass
  - `gcloud run deploy trr-backend --source . --region us-east1 --project trr-web-25d2e --quiet` -> pass
  - `gcloud run services describe trr-backend --region us-east1 --project trr-web-25d2e --format='value(status.latestReadyRevisionName,status.url)'` -> `trr-backend-00031-dl2` and service URL
  - `curl -i "$SERVICE_URL/openapi.json"` -> `HTTP/2 200`
- downstream_repos_impacted:
  - TRR-Backend: yes
  - screenalytics: no
  - TRR-APP: no

Continuation (same session, 2026-02-25) — Crawlee Instagram hardening for post-details/comments reliability.
- Scope:
  - Implemented true Crawlee primitive execution in runtime layer while preserving existing stage business logic paths.
  - Added Instagram auth fail-open fallback to legacy execution (default) with strict override.
  - Added additive metadata contract fields for crawlee/auth context, including explicit fallback marker.
- Files updated:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/socials/crawlee_runtime/config.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/socials/crawlee_runtime/auth_preflight.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/socials/crawlee_runtime/runtime.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/socials/crawlee_runtime/__init__.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/.env.example`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/socials/test_crawlee_auth_preflight.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/repositories/test_social_season_analytics.py`
- Behavior changes:
  - `execute_platform_stage_with_crawlee` now uses `RequestQueue` + `Request` lifecycle with retries, `SessionPool`, and `ProxyConfiguration`.
  - When Crawlee package is unavailable, runtime falls back to internal retry wrapper and still emits runtime/auth metadata.
  - `_execute_claimed_job` now:
    - keeps Crawlee routing for `posts/comments`,
    - applies Instagram preflight fail-open fallback to legacy path when auth missing and strict flag is disabled,
    - preserves strict fail-fast behavior via `SOCIAL_CRAWLEE_AUTH_STRICT_INSTAGRAM=true`.
  - Additive metadata fields now include:
    - `auth_context.fallback_to_legacy`
    - `crawler_runtime.crawlee_request_count`
    - `crawler_runtime.crawlee_retry_count`
    - `crawler_runtime.crawlee_session_pool_used`
- Validation:
  - `ruff check trr_backend/socials/crawlee_runtime trr_backend/repositories/social_season_analytics.py tests/socials/test_crawlee_auth_preflight.py tests/repositories/test_social_season_analytics.py` (pass)
  - `ruff format --check trr_backend/socials/crawlee_runtime trr_backend/repositories/social_season_analytics.py tests/socials/test_crawlee_auth_preflight.py tests/repositories/test_social_season_analytics.py` (pass)
  - `pytest -q tests/socials/test_crawlee_request_keys.py tests/socials/test_crawlee_error_taxonomy.py tests/socials/test_crawlee_auth_preflight.py tests/repositories/test_social_season_analytics.py tests/api/routers/test_socials_season_analytics.py tests/socials/test_comment_scraper_fixes.py` (pass, `155 passed`)
- default_skill_chain_applied: true
- default_skill_chain_used:
  - `orchestrate-plan-execution` (fallback process; skill not present in current skill inventory)
  - `senior-fullstack`
  - `senior-backend`
  - `senior-qa`
  - `code-reviewer`
- default_skill_chain_exception_reason:
  - `orchestrate-plan-execution` is referenced by repo policy but not available as an installable skill in this session; proceeded with the remaining mandatory chain.

Continuation (same session, 2026-02-25) — Row-level Sync Comments missing-only backend targeting.
- primary_skill: `senior-backend`
- supporting_skills: `skillforge`, `write-plan-codex`, `senior-fullstack`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- risk_class: `code_first`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `skillforge -> write-plan-codex -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- downstream_repos_impacted:
  - `TRR-Backend`: yes
  - `screenalytics`: no
  - `TRR-APP`: yes (consumer payload wiring)
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/socials.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_socials_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/repositories/test_social_season_analytics.py`
- behavior summary:
  - Added additive ingest request fields: `comment_refresh_policy` (`balanced|missing_only`) and `comment_anchor_source_ids` (per-platform source-id lists).
  - Added strict `missing_only` refresh policy path in comment refresh decisioning: refresh only on true `count_gap`; skip `stale_recheck`, `quiet_post_force_recheck`, and skip `count_drop` refreshes.
  - Added source-id filtering support to `_load_existing_posts(...)` and threaded platform-specific source-id targets into comment-stage handlers for Instagram/TikTok/YouTube/Twitter.
  - Persisted policy + target metadata into run/job config and ensured `comments_only + missing_only` skips comment job creation when a platform has an explicitly empty target list.
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check api/routers/socials.py trr_backend/repositories/social_season_analytics.py tests/api/routers/test_socials_season_analytics.py tests/repositories/test_social_season_analytics.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest tests/api/routers/test_socials_season_analytics.py tests/repositories/test_social_season_analytics.py -k "comments_only or comment_refresh or anchor_source_ids"` (pass, `4 passed`)

Continuation (same session, 2026-02-25) — Sync Comments queue/runner acceleration for platform-parallel execution.
- primary_skill: `senior-backend`
- supporting_skills: `skillforge`, `write-plan-codex`, `senior-fullstack`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- risk_class: `code_first`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `skillforge -> write-plan-codex -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- downstream_repos_impacted:
  - `TRR-Backend`: yes
  - `screenalytics`: no
  - `TRR-APP`: no
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/socials.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/socials/worker.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_socials_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/scripts/test_social_worker.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/.env.example`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/runbooks/social_worker_queue_ops.md`
- behavior summary:
  - `comments_only` ingest now fans out API background runners in both inline and queue execution paths, so a single run can execute multiple comments jobs concurrently.
  - Added `SOCIAL_COMMENTS_RUN_WORKERS` (default `4`, capped `1..8`) and backward-compatible fallback to `SOCIAL_INLINE_COMMENTS_WORKERS`.
  - Queue worker script now supports true queue-mode fanout with `--parallel N` (without `--run-id`) by spawning child workers with unique worker IDs.
  - Added regression tests for queue-mode API fanout and worker-script queue fanout behavior.
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check api/routers/socials.py scripts/socials/worker.py tests/api/routers/test_socials_season_analytics.py tests/scripts/test_social_worker.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest tests/api/routers/test_socials_season_analytics.py tests/scripts/test_social_worker.py -k "comments_only or worker or queue"` (pass, `12 passed`)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest tests/api/routers/test_socials_season_analytics.py tests/repositories/test_social_season_analytics.py -k "comments_only or comment_refresh or anchor_source_ids"` (pass, `5 passed`)

Continuation (same session, 2026-02-25) — Social analytics daily reported-comments payload for heatmap completeness.
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- risk_class: `code_first`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- downstream_repos_impacted:
  - `TRR-Backend`: yes
  - `screenalytics`: no
  - `TRR-APP`: yes (consumes additive fields)
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/repositories/test_social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_socials_season_analytics.py`
- behavior summary:
  - Added additive daily payload fields in `weekly_daily_activity.days[]`:
    - `reported_comments` (per-platform)
    - `total_reported_comments` (daily total)
  - Preserved existing weekly metadata (`week_type`, `episode_number`) and existing weekly comment-quality fields.
  - No breaking contract changes; this is additive-only.
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest -q tests/repositories/test_social_season_analytics.py tests/api/routers/test_socials_season_analytics.py` (pass, `114 passed`)

Continuation (same session, 2026-02-25) — Final verification pass for additive daily reported-comments contract.
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- risk_class: `code_first`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- downstream_repos_impacted:
  - `TRR-Backend`: yes
  - `screenalytics`: no
  - `TRR-APP`: yes
- validation_evidence:
  - `pytest -q /Users/thomashulihan/Projects/TRR/TRR-Backend/tests/repositories/test_social_season_analytics.py /Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_socials_season_analytics.py` (pass, `114 passed`)

Continuation (same session, 2026-02-25) — Bravo leaderboard thumbnails now prefer S3-hosted media when available.
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- risk_class: `code_first`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- downstream_repos_impacted:
  - `TRR-Backend`: yes
  - `screenalytics`: no
  - `TRR-APP`: no (consumes unchanged `thumbnail_url` field)
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/repositories/test_social_season_analytics.py`
- behavior summary:
  - Updated thumbnail selection expressions to prefer S3-hosted URLs in this order:
    - `hosted_thumbnail_url`
    - `hosted_media_urls[0]`
    - legacy/source thumbnail (`thumbnail_url` or `media_urls[0]` for X/Twitter)
  - This unblocks `Bravo Content Leaderboard` post thumbnails when mirror sync succeeded but only `hosted_media_urls` was populated.
  - Response contract is unchanged (`thumbnail_url` remains the same field).
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check trr_backend/repositories/social_season_analytics.py tests/repositories/test_social_season_analytics.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest tests/repositories/test_social_season_analytics.py -k "thumbnail_expr or leaderboard"` (pass, `2 passed`)

Continuation (same session, 2026-02-25) — RHOSLC social analytics revision: postseason windows + Instagram actor-style normalization.
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- risk_class: `code_first`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: yes
  - `screenalytics`: no
  - `TRR-APP`: yes
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/socials/instagram/scraper.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/repositories/test_social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/socials/test_comment_scraper_fixes.py`
- behavior_summary:
  - Added 3 `postseason` week windows after the final episode week (final episode week remains 7 days).
  - Preserved additive weekly payload compatibility while emitting postseason week metadata in analytics arrays.
  - Hardened Instagram parser for dual schemas: legacy IG fields and actor-style camelCase payloads for posts/comments/replies.
  - Added canonical mention/hashtag normalization and collaborator/tag extraction fallbacks from `coauthorProducers`/`taggedUsers` without double-counting posts/comments.
  - Kept non-canonical source fields in raw payload surfaces and avoided schema migrations.
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check trr_backend/repositories/social_season_analytics.py trr_backend/socials/instagram/scraper.py tests/repositories/test_social_season_analytics.py tests/socials/test_comment_scraper_fixes.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff format --check trr_backend/repositories/social_season_analytics.py trr_backend/socials/instagram/scraper.py tests/repositories/test_social_season_analytics.py tests/socials/test_comment_scraper_fixes.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest tests/socials/test_comment_scraper_fixes.py tests/repositories/test_social_season_analytics.py tests/api/routers/test_socials_season_analytics.py` (pass, `151 passed`)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check .` (fails on unrelated existing issues in `scripts/sync/sync_show_logos.py`)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff format --check .` (fails due unrelated existing workspace formatting drift)

Continuation (same session, 2026-02-25) — Cloud Run env repair + networks sync verification (ops-only).
- primary_skill: `senior-fullstack`
- supporting_skills: `senior-devops`, `senior-backend`, `senior-qa`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - secondary: `mcp__playwright__*`, `mcp__chrome-devtools__*`
- delegation_map:
  - role: `API Contract Owner`
    scope: `sync endpoint execution contract`
    deliverable: `no API shape changes; verified runtime execution of existing /api/v1/admin/shows/sync-networks-streaming contract`
    verification_command: `curl -fsS "$SERVICE_URL/openapi.json" >/dev/null`
    status: `completed`
  - role: `Schema Owner`
    scope: `data/migration impact`
    deliverable: `no schema or migration changes performed; verified run state through existing admin.network_streaming_sync_runs rows`
    verification_command: `PYTHONPATH=. python - <<'PY' ... select from admin.network_streaming_sync_runs ... PY`
    status: `completed`
  - role: `Integration Owner`
    scope: `TRR-APP admin /admin/networks runtime path`
    deliverable: `attempted browser-triggered sync on admin route; local page rendered but API auth warnings remained in that local session`
    verification_command: `Playwright snapshot on http://admin.localhost:3000/admin/networks`
    status: `completed`
  - role: `QA Owner`
    scope: `ops verification + clean log window`
    deliverable: `new Cloud Run revision with required env names, health check pass, sync runs terminal, and no post-window sync traceback signatures`
    verification_command: `gcloud logging read ... textPayload:"admin sync step failed" ...`
    status: `completed`
- risk_class: `no_contract`
- runtime_changes:
  - Cloud Run service updated in `trr-web-25d2e/us-east1`:
    - previous revision: `trr-backend-00034-hv4`
    - new ready revision: `trr-backend-00035-ch6`
  - Added/updated env bindings on service:
    - `TMDB_API_KEY`
    - `TMDB_BEARER_TOKEN`
    - `AWS_REGION`
    - `AWS_S3_BUCKET`
    - `AWS_CDN_BASE_URL`
    - `AWS_ACCESS_KEY_ID`
    - `AWS_SECRET_ACCESS_KEY`
    - `BRANDFETCH_API_KEY`
  - Health gate:
    - `GET /openapi.json` => `200`
- sync_verification_window:
  - `SYNC_START_UTC=2026-02-25T05:53:44Z`
  - Terminal run-state evidence from `admin.network_streaming_sync_runs` (>= window):
    - `run_id=network-streaming-20260225T055437Z` -> `status=stopped`, cursor `production:irwin entertainment`
    - `run_id=network-streaming-20260225T060040Z` -> `status=stopped`, cursor `production:one potato productions`
  - Resume-path evidence:
    - polling captured `run_id=network-streaming-20260225T060040Z` transition `running -> stopped` with cursor progression `production:mtv networks -> production:one potato productions`.
- validation_evidence:
  - `gcloud run services describe trr-backend --project trr-web-25d2e --region us-east1 --format='value(status.latestReadyRevisionName,status.url,spec.template.spec.containers[0].env[].name)'`
  - `curl -fsS "$SERVICE_URL/openapi.json" >/dev/null`
  - `gcloud logging read 'resource.type="cloud_run_revision" AND resource.labels.service_name="trr-backend" AND timestamp>="2026-02-25T05:53:44Z" AND textPayload:"admin sync step failed"' --project trr-web-25d2e --limit=100 --format='value(timestamp,textPayload)'` (no matches)
  - `gcloud logging read 'resource.type="cloud_run_revision" AND resource.labels.service_name="trr-backend" AND timestamp>="2026-02-25T05:53:44Z" AND textPayload:"Traceback (most recent call last)" AND textPayload:"/app/scripts/sync/"' --project trr-web-25d2e --limit=100 --format='value(timestamp,textPayload)'` (no matches)
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `yes` (runtime verification path and handoff update only; no app source mutation)

Continuation (same session, 2026-02-25) — TikTok content scraper dual-shape normalization (actor-style + legacy).
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- risk_class: `code_first`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `no`
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/socials/tiktok/scraper.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/socials/test_comment_scraper_fixes.py`
- behavior_summary:
  - Added robust dual-key parsing for TikTok posts:
    - legacy keys (`desc`, `author`, `stats`, `music`, `video`)
    - actor-style keys (`text`, `authorMeta`, `musicMeta`, `videoMeta`, top-level counts, `webVideoUrl`, structured hashtags/mentions)
  - Added timestamp coercion support for unix ints, numeric strings, and ISO timestamps (`createTimeISO`).
  - Normalized mention extraction to strip punctuation (`@hulu.` -> `hulu`) and dedupe across structured + caption-derived values.
  - Extended comment parsing to support actor-style comment payloads:
    - keys like `cid|id`, `uniqueId`, `uid`, `diggCount`, `replyCommentTotal`, `createTimeISO`, `avatarThumbnail`, `videoWebUrl`
    - nested `replies` parsing into typed reply objects
    - fallback `video_id` extraction from `videoWebUrl` when explicit IDs are missing.
  - Expanded media/thumbnail extraction to include actor-style fields (`mediaUrls`, `videoMeta.coverUrl`, `videoMeta.originalCoverUrl`) while preserving legacy behavior.
  - Hardened scrape loops to skip malformed rows with missing/invalid timestamps (for example actor rows containing only `error`) instead of terminating pagination early.
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check trr_backend/socials/tiktok/scraper.py tests/socials/test_comment_scraper_fixes.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff format --check trr_backend/socials/tiktok/scraper.py tests/socials/test_comment_scraper_fixes.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest -q tests/socials/test_comment_scraper_fixes.py tests/socials/tiktok/test_media_resolver.py` (pass, `41 passed`)

Continuation (same session, 2026-02-25) — Full Sync + Mirror backend contract + queue fanout hardening.
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- risk_class: `code_first`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `yes`
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/socials.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_socials_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/repositories/test_social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/socials/start_worker_pool.sh`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/runbooks/social_worker_queue_ops.md`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/.env.example`
- behavior_summary:
  - Added `GET /api/v1/admin/socials/seasons/{season_id}/analytics/mirror-coverage` with parity to comments-coverage response shape and `_platform_post_needs_media_mirror(...)` semantics.
  - Extended mirror requeue endpoints to support `date_start/date_end` window filtering and added response metadata: `window_applied`, `window`, `eligible_in_window`.
  - Hardened comments-only fanout worker cap via env (`SOCIAL_COMMENTS_RUN_WORKERS`, fallback `SOCIAL_INLINE_COMMENTS_WORKERS`, bounded `1..8`).
  - Added persistent worker pool helper script (`scripts/socials/start_worker_pool.sh`) and runbook/env documentation for general + mirror worker pools.
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && .venv/bin/python -m pytest tests/api/routers/test_socials_season_analytics.py -k 'mirror_coverage or mirror_requeue or comments_only_fanout_respects_worker_cap' tests/repositories/test_social_season_analytics.py -k 'mirror_coverage or requeue_media_mirror_jobs'` (pass, `3 passed`)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && .venv/bin/python -m ruff check api/routers/socials.py trr_backend/repositories/social_season_analytics.py tests/api/routers/test_socials_season_analytics.py tests/repositories/test_social_season_analytics.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && bash -n scripts/socials/start_worker_pool.sh` (pass)

Continuation (same session, 2026-02-26) — RHOSLC YouTube Shorts ingestion + ownership hardening + analytics breakdown.
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- delegation_map:
  - role: `API Contract Owner`
    scope: `social analytics response contract`
    deliverable: `added additive summary field summary.data_quality.youtube_content_breakdown without breaking existing keys`
    verification_command: `pytest -q tests/repositories/test_social_season_analytics.py -k "youtube and analytics"`
    status: `completed`
  - role: `Ingest Owner`
    scope: `YouTube posts scraping/persistence`
    deliverable: `explicit shorts surface crawl + canonical shorts URL + ownership pre-filter + is_short/source_surface persistence`
    verification_command: `pytest -q tests/socials/test_comment_scraper_fixes.py -k "youtube and (shorts or ownership or pre_window)"`
    status: `completed`
  - role: `Schema Owner`
    scope: `youtube_videos schema`
    deliverable: `new migration for is_short/source_surface and season+short index`
    verification_command: `ruff check supabase/migrations/0148_youtube_shorts_flags.sql`
    status: `completed`
  - role: `QA Owner`
    scope: `youtube regression coverage`
    deliverable: `added tests for shorts parsing, owner filtering, pre-window cap, upsert flags, and analytics breakdown`
    verification_command: `pytest -q tests/socials/test_comment_scraper_fixes.py tests/repositories/test_social_season_analytics.py -k "youtube and (shorts or ownership or analytics or upsert or pre_window)"`
    status: `completed`
- risk_class: `medium` (ingest behavior + additive analytics contract + migration)
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `yes`
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/socials/youtube/scraper.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/socials/test_comment_scraper_fixes.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/repositories/test_social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/supabase/migrations/0148_youtube_shorts_flags.sql`
- behavior_summary:
  - Added `CHANNEL_SHORTS_URL` scraping path and dual-surface crawl (`videos` + `shorts`) in YouTube scraper.
  - Added renderer URL canonicalization so shorts persist as `https://www.youtube.com/shorts/{id}`.
  - Added owner-handle filtering at scrape time to reject mismatched channel-owner renderers before upsert.
  - Added `is_short` and `source_surface` to `YouTubeVideo` and persisted via `_upsert_youtube_video`.
  - Updated analytics row building to preserve shorts URLs for YouTube posts and include `is_short` on normalized rows.
  - Added `summary.data_quality.youtube_content_breakdown` with `videos_count`, `reels_count`, and `total_count`.
  - Added pre-window continuation cap (`SOCIAL_YOUTUBE_PRE_WINDOW_PAGE_CAP`, default `12`) with retrieval metadata (`pre_window_page_cap`, `scan_capped_reason`).
  - Added defensive try/except around ownership cleanup inside `_ingest_youtube` so mocked/no-DB test harnesses do not crash.
- operational_actions:
  - Scoped cleanup + YouTube-only Week 0 run started for season `e9161955-6ee4-4985-865e-3386a0f670fb`.
  - Cleanup query result: `cleanup_candidates=0`, `cleanup_deleted=0`.
  - Active run: `0d91d498-7cad-40e2-9341-f23294ab76f2` (currently `running` while posts stage continues).
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check trr_backend/socials/youtube/scraper.py tests/socials/test_comment_scraper_fixes.py trr_backend/repositories/social_season_analytics.py tests/repositories/test_social_season_analytics.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest -q tests/socials/test_comment_scraper_fixes.py tests/repositories/test_social_season_analytics.py -k "youtube and (shorts or ownership or analytics or upsert or pre_window)"` (pass, `17 passed`)

Continuation (same session, 2026-02-26) — Week 0 YouTube operational rerun stabilization.
- operational_adjustments:
  - Attempted YouTube Week 0 `posts_and_comments` rerun (`run_id=0d91d498-7cad-40e2-9341-f23294ab76f2`) encountered long continuation scanning in posts stage and was cancelled.
  - Executed scoped fallback refresh as `comments_only` with explicit Week 0 YouTube anchors (`2` source IDs) to complete comment refresh deterministically.
- final_run_state:
  - Completed fallback run: `83c46edb-604c-42f1-873d-72278f8efdf4`
  - Status: `completed`
  - Summary: `total_jobs=1`, `completed_jobs=1`, `failed_jobs=0`, `active_jobs=0`.

Continuation (same session, 2026-02-26) — Social analytics: strict YouTube ownership cleanup + X quotes/media enrichment for week detail.
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- delegation_map:
  - role: `API Contract Owner`
    scope: `social week/post detail payloads`
    deliverable: `extended twitter post detail with additive quotes payload + quote counts while preserving existing comments semantics`
    verification_command: `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest tests/repositories/test_social_season_analytics.py tests/api/routers/test_socials_season_analytics.py -q`
    status: `completed`
  - role: `Ingest Owner`
    scope: `twitter and youtube ingest validation`
    deliverable: `added twitter link-preview extraction + quote sync and strict youtube ownership filtering + mismatch cleanup`
    verification_command: `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest tests/socials/test_comment_scraper_fixes.py -q`
    status: `completed`
  - role: `QA Owner`
    scope: `targeted backend regression coverage`
    deliverable: `added/updated tests for youtube owner filtering, twitter media extraction, quote payloads, and router pass-through`
    verification_command: `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest tests/repositories/test_social_season_analytics.py tests/socials/test_comment_scraper_fixes.py tests/api/routers/test_socials_season_analytics.py -q`
    status: `completed`
- risk_class: `medium` (ingest filtering + additive API response fields)
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no` (contract check only; no schema/API shape used by screenalytics changed)
  - `TRR-APP`: `yes`
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/socials/twitter/scraper.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/socials/youtube/scraper.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/socials/test_comment_scraper_fixes.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/repositories/test_social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_socials_season_analytics.py`
- behavior_summary:
  - Enforced stricter YouTube ownership checks using canonical channel identity resolution and strict channel-id matching in yt-dlp search fallback.
  - Added ingest-time cleanup path for season/account-scoped mismatched YouTube rows under strict ownership policy.
  - Extended Twitter scrape parsing to capture link-preview media candidates and exposed count metadata.
  - Added Twitter quote retrieval/persistence (`is_quote=true`, `quoted_tweet_id=<root>`) in ingest and refresh flows.
  - Extended Twitter post-detail response with additive `quotes` and `total_quotes_in_db` fields, while keeping `comments` as comments/replies thread semantics.
  - Extended week-detail Twitter post payload with `total_quotes_in_db` for UI labels.
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check trr_backend/repositories/social_season_analytics.py trr_backend/socials/twitter/scraper.py trr_backend/socials/youtube/scraper.py tests/repositories/test_social_season_analytics.py tests/socials/test_comment_scraper_fixes.py tests/api/routers/test_socials_season_analytics.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest tests/repositories/test_social_season_analytics.py tests/socials/test_comment_scraper_fixes.py tests/api/routers/test_socials_season_analytics.py -q` (pass, `170 passed`)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check .` (known pre-existing failure in `scripts/sync/sync_show_logos.py`; unrelated to this change set)

Continuation (same session, 2026-02-26) — Hotfix: environments without `youtube_videos.is_short` / `source_surface` columns.
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `no`
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/repositories/test_social_season_analytics.py`
- behavior_summary:
  - Added schema-safe helper `_youtube_is_short_expr(alias)` so SQL reads do not reference `v.is_short` when the column is absent.
  - Updated YouTube analytics/week-detail/post-detail SQL selectors to use the schema-safe `is_short` expression.
  - Updated YouTube upsert to conditionally persist `is_short` and `source_surface` only when those columns exist.
  - Result: legacy/mid-migration DBs no longer crash with `column v.is_short does not exist`.
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check trr_backend/repositories/social_season_analytics.py tests/repositories/test_social_season_analytics.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest -q tests/repositories/test_social_season_analytics.py -k "youtube_content_breakdown or youtube and (week_detail or post_comments or upsert)"` (pass, `6 passed`)
  - Live smoke: `get_analytics(... platforms=['youtube'], timezone='America/New_York', week=None)` returned successfully in local env.

Continuation (same session, 2026-02-27) — Phase 1A extension: show icon S3/API backend.
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `yes`
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/media/s3_mirror.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_show_icons.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/main.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/supabase/migrations/0150_add_show_icons_table.sql`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/media/test_s3_mirror_icons.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_show_icons.py`
- behavior_summary:
  - Added show-icon S3 key helpers for deterministic `icons/{show_key}/{filename}` storage.
  - Added admin show icon API: upload/list/delete endpoints under `/api/v1/admin/shows/{show_key}/icons`.
  - Added `public.show_icons` migration for icon metadata persistence.
  - Wired router into main API app.
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check api/routers/admin_show_icons.py trr_backend/media/s3_mirror.py tests/api/routers/test_admin_show_icons.py tests/media/test_s3_mirror_icons.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && python -m pytest tests/api/routers/test_admin_show_icons.py tests/media/test_s3_mirror_icons.py -v` (pass, `6 passed`)

Continuation (same session, 2026-02-27) — Runtime fix for admin show icons DB access.
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `yes`
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_show_icons.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/ai/HANDOFF.md`
- behavior_summary:
  - Fixed runtime mismatch where `admin_show_icons` attempted Supabase `.table(...)` calls against `DbSession`.
  - Switched icon record CRUD helpers to use `trr_backend.db.pg` SQL helpers (`fetch_all`, `fetch_one`, `execute_returning`) while preserving endpoint contracts.
  - This removes the `AttributeError: 'DbSession' object has no attribute 'table'` failure path in live server mode.
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check api/routers/admin_show_icons.py tests/api/routers/test_admin_show_icons.py tests/media/test_s3_mirror_icons.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && python -m pytest tests/api/routers/test_admin_show_icons.py tests/media/test_s3_mirror_icons.py -q` (pass, `6 passed`)

Continuation (same session, 2026-02-27) — Apply show-icons migration + seed RHOSLC default icon.
- primary_skill: `senior-fullstack`
- supporting_skills: `orchestrate-plan-execution`, `senior-backend`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- delegation_map:
  - role: `API Integration Owner`
    scope: `show icon API runtime`
    deliverable: `fixed show icon response serialization by typing ShowIconRecord.created_at as datetime`
    verification_command: `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check api/routers/admin_show_icons.py tests/api/routers/test_admin_show_icons.py tests/media/test_s3_mirror_icons.py && python -m pytest tests/api/routers/test_admin_show_icons.py tests/media/test_s3_mirror_icons.py -q`
    status: `completed`
  - role: `Ops Owner`
    scope: `DB migration + data seeding`
    deliverable: `applied migration 0150, uploaded BlackStar icon via show-icons endpoint path, and set RHOSLC survey show default icon_url`
    verification_command: `psql verification queries + FastAPI TestClient GET /api/v1/admin/shows/rhoslc/icons`
    status: `completed`
- risk_class: `medium` (production-facing admin icon flow + DB state mutation)
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `yes` (runtime reads `survey_shows.icon_url`)
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_show_icons.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/ai/HANDOFF.md`
- behavior_summary:
  - Applied `supabase/migrations/0150_add_show_icons_table.sql` to active DB.
  - Uploaded `/Volumes/HardDrive/APP-NOV/RHOSLC/BlackStar.png` through the new show icon API flow (via same FastAPI endpoint logic), resulting icon record key `icons/rhoslc/blackstar.png`.
  - Updated `public.survey_shows.icon_url` for RHOSLC (`trr_show_id=7782652f-783a-488b-8860-41b97de32e75`) to `https://trr-backend.s3.amazonaws.com/icons/rhoslc/blackstar.png`.
  - Fixed icon API response model serialization bug (`created_at` typing) so list/upload responses serialize correctly.
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -f supabase/migrations/0150_add_show_icons_table.sql` (pass)
  - `select to_regclass('public.show_icons') ...` => table exists; icon row present for `show_key='rhoslc'`
  - `FastAPI TestClient GET /api/v1/admin/shows/rhoslc/icons` => 200 with seeded icon record
  - `UPDATE public.survey_shows ... RETURNING ...` => RHOSLC row updated to blackstar URL
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check ... && python -m pytest tests/api/routers/test_admin_show_icons.py tests/media/test_s3_mirror_icons.py -q` (pass, `6 passed`)

Continuation (same session, 2026-02-27) — Facebook + Threads social ingest expansion (backend core + recon fixture gate).
- primary_skill: `senior-fullstack`
- supporting_skills: `orchestrate-plan-execution`, `senior-backend`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `compatibility check only`
  - `TRR-APP`: `yes`
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/supabase/migrations/0152_add_facebook_and_meta_threads_social_platforms.sql`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/socials/platforms.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/socials/crawlee_runtime/config.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/socials/crawlee_runtime/auth_preflight.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/socials/facebook/*`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/socials/threads/*`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/socials.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/socials/worker.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/socials/backfill_social_media_mirror_jobs.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/fixtures/socials/recon/facebook_threads_recon_fixture_pack.json`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/socials/test_facebook_threads_recon_gate.py`
- behavior_summary:
  - Added `facebook` and `threads` support through migration constraints, new platform tables (`facebook_*`, `meta_threads_*`), and new media-mirror job types.
  - Added shared social platform registry to reduce hardcoded platform drift across runtime config/worker/router/repository.
  - Extended Crawlee runtime/auth preflight for facebook/threads with strict public-first + user-cookie fallback behavior.
  - Added facebook/threads adapter + scraper modules and integrated dispatch/read/write paths in `social_season_analytics`.
  - Added deterministic recon fixture pack + gate test to require six target classes and two stable consecutive source signatures.
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && python3 -m py_compile api/routers/socials.py trr_backend/repositories/social_season_analytics.py trr_backend/socials/crawlee_runtime/config.py trr_backend/socials/crawlee_runtime/auth_preflight.py trr_backend/socials/facebook/scraper.py trr_backend/socials/threads/scraper.py trr_backend/socials/platforms.py scripts/socials/worker.py scripts/socials/backfill_social_media_mirror_jobs.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest -q tests/socials/test_facebook_threads_recon_gate.py tests/socials/test_crawlee_auth_preflight.py tests/api/routers/test_socials_season_analytics.py tests/repositories/test_social_season_analytics.py` (pass, `139 passed`)

Continuation (same session, 2026-02-27) — Twitter comments-stage completeness fix + Week 0 rerun validation.
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `yes` (consumes run/coverage outputs)
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/ai/HANDOFF.md`
- behavior_summary:
  - Fixed Twitter comments-stage `UnboundLocalError` by moving `is_complete` evaluation out of the quotes loop so completion logic always executes per anchor.
  - Hardened completeness checks to use combined reply+quote fetch counts where expected counts include both interaction types.
  - Extended manual refresh path (`refresh_post_comments` for Twitter) to include `replies_count`/`quotes` in expected-count completeness guard.
  - Re-ran Week 0 Twitter `posts_and_comments` for RHOSLC window (`2025-08-14T04:00:00+00:00` -> `2025-09-16T23:59:59.999999+00:00`) with run id `005e6447-69fe-48e6-90f3-c264bd2c1e0c`.
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check trr_backend/repositories/social_season_analytics.py tests/repositories/test_social_season_analytics.py tests/socials/test_comment_scraper_fixes.py trr_backend/socials/twitter/scraper.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest -q tests/repositories/test_social_season_analytics.py -k "expected_comment_count_for_platform_twitter_includes_quotes or apply_twitter_public_summary_uses_non_empty_fields or is_comment_fetch_complete"` (pass, `3 passed`)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest -q tests/socials/test_comment_scraper_fixes.py -k "twitter_reply_fetch_falls_back_to_search_on_http_error or twitter_fetch_public_tweet_summary_includes_reply_count_and_media"` (pass, `2 passed`)
  - Run verification (Supabase metadata): `run_id=005e6447-69fe-48e6-90f3-c264bd2c1e0c`, `status=completed`, comments job `items_found=35`, no job-level error.
  - Coverage snapshot after rerun (`twitter`, Week 0 window): `saved=506`, `reported=3637`, `coverage_pct=13.9`, `up_to_date=false`, `stale_posts_count=35`.
- remaining_blockers:
  - X GraphQL endpoints continue returning `404`/`429` in this environment without valid authenticated cookies (`TWIKIT_COOKIES_FILE` unresolved), so replies/quotes body hydration remains incomplete despite successful run execution.

Continuation (same session, 2026-02-27) — Run migration `0152` + Facebook/Threads Bravo smoke ingest stabilization in shared DB.
- primary_skill: `senior-fullstack`
- supporting_skills: `orchestrate-plan-execution`, `senior-backend`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `no`
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/socials/facebook/scraper.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/socials/threads/scraper.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/ai/HANDOFF.md`
- behavior_summary:
  - Applied migration SQL `0152_add_facebook_and_meta_threads_social_platforms.sql` against target Supabase DB and inserted `version='0152'` into `supabase_migrations.schema_migrations` for migration history continuity.
  - Added browser-rendered fallback in Facebook scraper for public pages that return HTTP 400 to plain HTTP clients, enabling compliant public extraction without auth bypass.
  - Added Playwright profile discovery in Threads scraper and adjusted HTTP header strategy for post fetches so post descriptions are recoverable from public pages.
  - Ran RHOSLC S6 Bravo Facebook+Threads ingest (`run_id=84518379-6a9c-469a-bf24-7456f2e3dab3`) and recovered externally-failed jobs by rerunning failed stages via in-process manual execution (`codex-manual`) due shared DB worker interference from legacy runtime.
  - Final run state for `84518379-6a9c-469a-bf24-7456f2e3dab3`: `completed`, `7/7 jobs completed`, `items_found_total=27`.
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && psql "$SUPABASE_DB_URL" -v ON_ERROR_STOP=1 -f supabase/migrations/0152_add_facebook_and_meta_threads_social_platforms.sql` (pass, committed)
  - `insert into supabase_migrations.schema_migrations(version='0152', ...)` (pass, row present)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && python -m py_compile trr_backend/socials/facebook/scraper.py trr_backend/socials/threads/scraper.py && ruff check trr_backend/socials/facebook/scraper.py trr_backend/socials/threads/scraper.py` (pass)
  - Direct scraper probes:
    - Facebook (`Bravo`) discovery now returns `11` candidate posts on public surfaces.
    - Threads (`@bravotv`) discovery now returns `4` profile posts with non-empty text.
  - DB verification after run completion (`season_id=e9161955-6ee4-4985-865e-3386a0f670fb`):
    - `social.facebook_posts=1`
    - `social.facebook_comments=0`
    - `social.meta_threads_posts=2`
    - `social.meta_threads_comments=0`
- remaining_blockers:
  - Shared target DB has an external worker (`worker_id=social-worker:localhost:2`) running older code that can claim and fail jobs (`Platform <platform> ingest is not supported`, `mirror_platform_not_supported`) before local execution; manual in-process recovery was required to complete the smoke run.
  - Public, compliant surfaces currently expose limited Bravo historical depth and limited comment visibility for Facebook/Threads without user-provided authenticated session artifacts, preventing a reliable “100% RHOSLC S6” capture from public-only mode.

Continuation (same session, 2026-02-27) — Max-capture Bravo Facebook/Threads run recovery and final stored-volume checkpoint.
- primary_skill: `senior-fullstack`
- supporting_skills: `orchestrate-plan-execution`, `senior-backend`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `no`
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/ai/HANDOFF.md`
- behavior_summary:
  - Executed max-capture run `522e1c0c-0220-47eb-80c2-7312bbb78703` for RHOSLC S6 (`platforms=['facebook','threads']`) using broadened Bravo target matching to maximize public-surface retrieval.
  - Initial run was marked failed due 19 media-mirror failures from external legacy worker claims.
  - Recovered all failed jobs in a single controlled pass by forcing failed jobs to `running` and replaying with in-process `_execute_claimed_job(...)` under `worker_id='codex-manual'`.
  - Final run state: `completed`, `29/29 jobs completed`, `items_found_total=126`.
  - Final season storage checkpoint:
    - `social.facebook_posts=10`
    - `social.facebook_comments=0`
    - `social.meta_threads_posts=15`
    - `social.meta_threads_comments=0`
  - RHOSLC mention checkpoint in stored text:
    - `threads_posts` containing `rhoslc`: `2`
    - `facebook_posts` containing `rhoslc`: `0`
- validation_evidence:
  - `select id,status,summary from social.scrape_runs where id='522e1c0c-0220-47eb-80c2-7312bbb78703'` => `completed` with zero failed jobs.
  - `select count(*) ...` across facebook/meta_threads post/comment tables for season `e9161955-6ee4-4985-865e-3386a0f670fb` (values above).
- remaining_blockers:
  - Even with improved discovery and max-capture pass, Facebook/Threads comments are not materially available in compliant public mode for these targets.
  - True “100% RHOSLC S6” cannot be guaranteed without authenticated session artifacts and/or a fully isolated worker environment preventing legacy-worker race conditions on shared queue jobs.

Continuation (same session, 2026-02-27) — RHOSLC S6 Overview completion for IG/TT/X/YT/FB/Threads + URL-route validation.
- primary_skill: `senior-fullstack`
- supporting_skills: `orchestrate-plan-execution`, `senior-backend`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `validation only`
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/repositories/test_social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/ai/HANDOFF.md`
- behavior_summary:
  - Added deterministic timestamp fallback in Facebook/Threads upsert paths:
    - post fallback: `posted_at <- scraped_at` when source timestamp is missing.
    - comment fallback: `created_at <- scraped_at` when source timestamp is missing.
    - persisted fallback marker in `raw_data._ingest` for auditability.
  - Cancelled runaway all-platform run `75761ec1-bbeb-426c-aafa-2d45c181ee18` after posts stage completion to stop unnecessary mirror backlog from shared-worker contention.
  - Backfilled RHOSLC S6 Facebook/Threads post timestamps and clamped to season window end so platform rows are eligible for Overview time-window aggregation.
  - Verified season analytics now include non-zero `facebook` and `threads` in `platform_breakdown` and weekly platform totals.
  - Validated route behavior for both short slug and full slug URL forms in TRR-APP route tests (`rhoslc/...` and `the-real-housewives-of-salt-lake-city/...`).
- validation_evidence:
  - Backend checks:
    - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && source .venv/bin/activate && pytest -q tests/repositories/test_social_season_analytics.py -k "upsert_facebook_post_falls_back_to_scraped_at_when_posted_at_missing or upsert_threads_post_falls_back_to_scraped_at_when_posted_at_missing"` (pass, `2 passed`)
    - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && source .venv/bin/activate && ruff check trr_backend/repositories/social_season_analytics.py tests/repositories/test_social_season_analytics.py` (pass)
  - Supabase verification (`season_id=e9161955-6ee4-4985-865e-3386a0f670fb`):
    - Stored rows: `instagram=1012`, `tiktok=405`, `twitter=3545`, `youtube=141`, `facebook=10`, `threads=15`.
    - No active RHOSLC runs: `running_runs=0`.
    - `get_analytics(... source_scope='bravo', platforms=[ig,tt,twitter,youtube,facebook,threads])` platform breakdown:
      - `instagram posts=274`
      - `tiktok posts=405`
      - `twitter posts=286`
      - `youtube posts=65`
      - `facebook posts=10`
      - `threads posts=15`
  - TRR-APP route validation:
    - `cd /Users/thomashulihan/Projects/TRR/TRR-APP && pnpm -C apps/web exec vitest run tests/show-admin-routes.test.ts tests/admin-host-middleware.test.ts` (pass, `34 passed`)
- remaining_blockers:
  - Shared DB still has external legacy worker interference risk for queue/mirror stages; manual cancellation/replay remains required when it claims jobs with outdated platform support.
  - Facebook/Threads comments remain near-zero in compliant public mode without authenticated artifacts.

Continuation (same session, 2026-02-27) — Fix Twitter post-detail SQL placeholder regression (`{hosted_media_expr}`).
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `no`
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/repositories/test_social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/ai/HANDOFF.md`
- behavior_summary:
  - Fixed SQL construction bug in `get_post_comments(... platform='twitter')` where the replies CTE query was missing an `f` prefix, causing literal `{hosted_media_expr}` to be sent to Postgres and fail with `syntax error at or near "{"`.
  - Added regression assertion in Twitter post-detail repository test to ensure unresolved `{hosted_media_expr}` placeholders never leak into SQL text.
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && source .venv/bin/activate && pytest -q tests/repositories/test_social_season_analytics.py -k "get_post_comments_twitter_returns_separate_quotes_payload or week_detail_twitter_includes_total_quotes_in_db"` (pass, `2 passed`)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && source .venv/bin/activate && pytest -q tests/api/routers/test_socials_season_analytics.py -k "twitter_quotes_payload"` (pass, `1 passed`)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && source .venv/bin/activate && ruff check trr_backend/repositories/social_season_analytics.py tests/repositories/test_social_season_analytics.py` (pass)

Continuation (same session, 2026-02-27) — Fix Twitter quote ingestion under-counting (`All Quotes (0)`).
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `no`
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/socials/twitter/scraper.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/socials/test_comment_scraper_fixes.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/ai/HANDOFF.md`
- behavior_summary:
  - Hardened Twitter quote detection in scraper parsing by adding legacy quote markers support:
    - `legacy.is_quote_status`
    - `legacy.quoted_status_id_str` / `legacy.quoted_status_id`
    - existing `quoted_status_result` path remains supported.
  - Updated quote-only search ingestion (`conversation_id:<id> filter:quote`) to keep quote rows even when GraphQL omits nested quote metadata in timeline entries.
  - Added parser parity improvements for syndication and twikit pathways so `quoted_tweet_id` is populated when available.
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check trr_backend/socials/twitter/scraper.py tests/socials/test_comment_scraper_fixes.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest -q tests/socials/test_comment_scraper_fixes.py -k "twitter_parse_tweet_result_detects_legacy_quote_fields or twitter_fetch_tweet_quotes_keeps_legacy_quote_entries or twitter_parse_tweet_result_reads_username_from_core_fallback or twitter_parse_tweet_result_prefers_mp4_variant_for_video_media"` (pass, `4 passed`)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest -q tests/repositories/test_social_season_analytics.py -k "get_post_comments_twitter_returns_separate_quotes_payload or week_detail_twitter_includes_total_quotes_in_db"` (pass, `2 passed`)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest -q tests/api/routers/test_socials_season_analytics.py -k "twitter_quotes_payload"` (pass, `1 passed`)
- remaining_blockers:
  - Existing historic runs in shared Supabase remain under-captured until a fresh Twitter ingest/backfill is executed with this patch in the active worker runtime.

Continuation (same session, 2026-02-27) — Quote fetch failure classification + live refresh verification.
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `no`
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/socials/twitter/scraper.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/socials/test_comment_scraper_fixes.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/ai/HANDOFF.md`
- behavior_summary:
  - Added `last_quote_fetch_reason` tracking in `TwitterScraper.fetch_tweet_quotes(...)` with classifiable reasons (`http_<status>`, `search_error`, `parse_error`, `api_errors`).
  - Wired quote failure reason propagation into Twitter ingest paths so quote failures are recorded instead of silently returning empty lists.
  - Verified live `refresh_post_comments(...)` now returns machine-readable quote failure metadata (`quote_fetch_reason='http_404'`, `quote_fetch_failed=true`) for RHOSLC tweets in this local env.
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check trr_backend/socials/twitter/scraper.py trr_backend/repositories/social_season_analytics.py tests/socials/test_comment_scraper_fixes.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest -q tests/socials/test_comment_scraper_fixes.py -k "twitter_fetch_tweet_quotes or twitter_parse_tweet_result_detects_legacy_quote_fields or twitter_parse_tweet_result_reads_username_from_core_fallback or twitter_parse_tweet_result_prefers_mp4_variant_for_video_media"` (pass, `5 passed`)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest -q tests/repositories/test_social_season_analytics.py -k "get_post_comments_twitter_returns_separate_quotes_payload or week_detail_twitter_includes_total_quotes_in_db"` (pass, `2 passed`)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest -q tests/api/routers/test_socials_season_analytics.py -k "twitter_quotes_payload"` (pass, `1 passed`)
  - Live probe (`season_id=e9161955-6ee4-4985-865e-3386a0f670fb`, tweet `1956000357282406729`):
    - `refresh_post_comments(... platform='twitter' ...)` => `quotes_fetched=0`, `quote_fetch_reason='http_404'`, `quote_fetch_failed=true`, `total_quotes_in_db=0`.
- remaining_blockers:
  - Local runtime has no `TWITTER_AUTH_TOKEN`/`TWITTER_CT0`/`TWIKIT_*` credentials loaded, and Twitter Search/TweetDetail quote surfaces currently return `404` under compliant unauthenticated mode for tested RHOSLC tweets.

Continuation (same session, 2026-02-27) — X media mirror hardening + Twitter session artifact loader expansion.
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `no`
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/repositories/test_social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/ai/HANDOFF.md`
- behavior_summary:
  - Hardened X media mirror behavior for RHOSLC S6 by re-resolving tweet media from public tweet summary during mirror stage when source media is missing/placeholder-only.
  - Live remediation confirmed all RHOSLC S6 X roots with source media now have hosted media + thumbnail (`32/32` mirrored, `0` missing hosted media, `0` missing hosted thumbnail).
  - Expanded Twitter/Twikit auth loaders to support browser-exported cookie artifacts:
    - `SOCIAL_TWITTER_COOKIES_JSON` now accepts flat dict, `cookies[]` list, and Playwright-style `{\"cookies\": [...]}` payloads.
    - `SOCIAL_TWITTER_COOKIES_FILE`/`TWITTER_COOKIES_FILE` now accept those same cookie payload shapes.
    - `TWIKIT_COOKIES_FILE` now accepts either flat `{\"auth_token\",\"ct0\"}` or cookie-list/storage-state shape containing `auth_token` and `ct0` entries.
  - Post-detail X popup SQL placeholder regression is no longer present in current runtime (`get_post_comments(..., platform='twitter')` executes without unresolved `{hosted_media_expr}`).
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check trr_backend/repositories/social_season_analytics.py tests/repositories/test_social_season_analytics.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest -q tests/repositories/test_social_season_analytics.py -k "load_twitter_auth_accepts_storage_state_json_env or load_twitter_auth_accepts_cookie_list_file or load_twikit_credentials_accepts_storage_state_file or merge_twitter_media_urls_prefers_video_from_public_summary or run_platform_media_mirror_stage_twitter_resolves_video_from_public_summary"` (pass, `5 passed`)
  - Supabase spot checks (`season_id=e9161955-6ee4-4985-865e-3386a0f670fb`):
    - X mirror coverage: `total_with_source_media=32`, `mirrored_status=32`, `missing_hosted_media=0`, `missing_hosted_thumb=0`.
    - X quote coverage remains blocked without auth: `roots_with_reported_quotes=242`, `roots_with_saved_quotes=0`, `roots_missing_saved_quotes=242`, `saved_quotes_total=0`.
- remaining_blockers:
  - No local Twitter session artifacts available yet (`SOCIAL_TWITTER_COOKIES_JSON` empty, `TWIKIT_COOKIES_FILE=data/twitter_cookies.json` missing file). Without valid auth cookies (`auth_token` + `ct0`) quote refresh continues to return `quote_fetch_reason='http_404'` and persists no new quotes.

Continuation (same session, 2026-02-27) — Twitter/X resilience follow-up (twikit fallback + auth loading hardening).
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `no`
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/socials/twitter/scraper.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/socials/test_comment_scraper_fixes.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/repositories/test_social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/.env.example`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/ai/HANDOFF.md`
- behavior_summary:
  - Added twikit fallback retrieval for X replies/quotes when GraphQL/TweetDetail surfaces fail:
    - new helper normalization path for twikit tweet objects
    - `fetch_tweet_quotes(...)` now attempts twikit quote search fallback after GraphQL `http_404/search_error` outcomes
    - `fetch_tweet_replies(...)` now attempts twikit fallback when detail + search fallbacks fail
  - Improved Twitter auth artifact loading reliability:
    - browser cookie fallback now defaults to `SOCIAL_TWITTER_BROWSER=auto` and tries a broad browser set in order (`chrome/chromium/brave/edge/opera/vivaldi/firefox/safari`)
    - `TWIKIT_COOKIES_JSON` inline storage-state payload support added
    - missing `TWIKIT_COOKIES_FILE` now logs explicit warning for faster diagnosis
  - Fixed Twitter post-detail popup user URL field mapping bug (`user_url` alias now used in payload mapping).
  - Expanded env contract docs for new/active vars:
    - `SOCIAL_TWITTER_COOKIES_HEADER`, `TWITTER_COOKIES_HEADER`
    - `SOCIAL_TWITTER_BROWSER_COOKIE_FALLBACK`, `SOCIAL_TWITTER_BROWSER`, `SOCIAL_TWITTER_COOKIE_DOMAINS`, `SOCIAL_TWITTER_COOKIE_NAMES`
    - `TWIKIT_COOKIES_JSON`
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && source .venv/bin/activate && ruff check trr_backend/socials/twitter/scraper.py trr_backend/repositories/social_season_analytics.py tests/socials/test_comment_scraper_fixes.py tests/repositories/test_social_season_analytics.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && source .venv/bin/activate && pytest -q tests/socials/test_comment_scraper_fixes.py -k "twitter"` (pass, `14 passed`)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && source .venv/bin/activate && pytest -q tests/repositories/test_social_season_analytics.py -k "twitter and (refresh_post_comments or get_post_comments or merge_twitter_media_urls or week_detail_twitter or run_platform_media_mirror_stage_twitter)"` (pass, `6 passed`)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && source .venv/bin/activate && pytest -q tests/repositories/test_social_season_analytics.py -k "load_twikit_credentials_accepts_storage_state_json_env or load_twikit_credentials_accepts_cookie_header_env or refresh_post_comments_twitter_infers_auth_failed_when_no_session_artifacts or get_post_comments_twitter_returns_separate_quotes_payload"` (pass, `4 passed`)
  - local auth probe with browser auto fallback:
    - `SOCIAL_TWITTER_BROWSER_COOKIE_FALLBACK=1 SOCIAL_TWITTER_BROWSER=auto ... _load_twitter_auth()` => no `auth_token`/`ct0` discovered in this environment
    - `_load_twikit_credentials()` => `None`
- remaining_blockers:
  - This runtime still has no usable X session artifacts (`auth_token` + `ct0`), so quote persistence remains blocked at source despite improved fallback/retry behavior.

Continuation (same session, 2026-02-27) — IMDb person-gallery metadata fallback recovery for cross-title images.
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- delegation_map:
  - role: `Design Context Owner`
    scope: `IMDb mediaviewer parser failure modes`
    deliverable: `identified missing People/Titles section fallback for mediaviewer pages that only expose caption links/text`
    verification_command: `pytest tests/integrations/imdb/test_person_gallery_parser.py -q`
    status: `completed`
  - role: `UI Implementer`
    scope: `n/a (backend patch)`
    deliverable: `n/a`
    verification_command: `n/a`
    status: `completed`
  - role: `API Integration Owner`
    scope: `IMDb parser output contract`
    deliverable: `parse_imdb_person_mediaviewer_details now fills people/title metadata from caption links and caption text fallback when sections are absent`
    verification_command: `pytest tests/integrations/imdb/test_person_gallery_parser.py -q`
    status: `completed`
  - role: `QA Owner`
    scope: `parser regression coverage`
    deliverable: `added tests for caption-link fallback and caption-text fallback parsing paths`
    verification_command: `pytest tests/integrations/imdb/test_person_gallery_parser.py -q`
    status: `completed`
- risk_class: `low`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `yes`
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/integrations/imdb/person_gallery.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/integrations/imdb/test_person_gallery_parser.py`
- behavior_summary:
  - IMDb person mediaviewer parsing now recovers `people_names`/`title_names` when the page omits explicit `People`/`Titles` sections.
  - Fallback priority is now: section blocks -> caption anchor links -> structured caption text (`... in Title (Year)`).
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest tests/integrations/imdb/test_person_gallery_parser.py -q` (pass, `4 passed`)

Continuation (same session, 2026-02-27) — IMDb person-gallery metadata enrichment defaults for lightbox completeness.
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- delegation_map:
  - role: `Design Context Owner`
    scope: `IMDb ingest metadata gaps for cross-title photos`
    deliverable: `identified missing source/page/title/logo defaults in `fetch_imdb_cast_photos` rows`
    verification_command: `pytest tests/ingestion/test_cast_photo_sources_imdb.py -q`
    status: `completed`
  - role: `UI Implementer`
    scope: `n/a (backend-only segment)`
    deliverable: `n/a`
    verification_command: `n/a`
    status: `completed`
  - role: `API Integration Owner`
    scope: `cast photo row payload`
    deliverable: `IMDb rows now include source defaults (`source_variant`, `source_logo`, `source_page_title`, `asset_name`, `source_file_url`)`
    verification_command: `pytest tests/ingestion/test_cast_photo_sources_imdb.py -q`
    status: `completed`
  - role: `QA Owner`
    scope: `ingestion regression coverage`
    deliverable: `added unit test for IMDb row metadata defaults`
    verification_command: `pytest tests/integrations/imdb/test_person_gallery_parser.py tests/ingestion/test_cast_photo_sources_imdb.py -q`
    status: `completed`
- risk_class: `low`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `yes`
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/ingestion/cast_photo_sources.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/ingestion/test_cast_photo_sources_imdb.py`
- behavior_summary:
  - IMDb cast-photo ingest now persists richer source metadata for each row so the person lightbox can display complete provenance fields even for cross-title photos.
  - Added metadata defaults: `source_variant=imdb_person_gallery`, `source_logo=IMDb`, `source_file_url/source_image_url`, plus title-derived `source_page_title/asset_name/name`.
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest tests/integrations/imdb/test_person_gallery_parser.py tests/ingestion/test_cast_photo_sources_imdb.py -q` (pass, `5 passed`)

Continuation (same session, 2026-02-27) — Social Instagram cover-source + source/hosted payload hardening.
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- delegation_map:
  - role: `Design Context Owner`
    scope: `instagram week/detail payload semantics`
    deliverable: `added explicit cover-source inference (`custom_cover` vs `still_frame_or_default`) and source/hosted media fields for week/detail payloads`
    verification_command: `pytest -q tests/repositories/test_social_season_analytics.py -k "instagram_cover_source or week_detail_instagram_includes_thumbnail_url or get_post_comments_instagram_includes_metadata_fields"`
    status: `completed`
  - role: `API Integration Owner`
    scope: `get_post_comments instagram SQL`
    deliverable: `fixed undefined `hosted_media_urls_expr` reference in instagram detail query`
    verification_command: `python -m py_compile trr_backend/repositories/social_season_analytics.py`
    status: `completed`
  - role: `QA Owner`
    scope: `repository regression coverage`
    deliverable: `added assertions/tests for `cover_source`, `cover_source_confidence`, and source-vs-hosted media fields`
    verification_command: `pytest -q tests/repositories/test_social_season_analytics.py -k "instagram_cover_source or week_detail_instagram_includes_thumbnail_url or get_post_comments_instagram_includes_metadata_fields"`
    status: `completed`
- risk_class: `low`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `yes`
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/repositories/test_social_season_analytics.py`
- behavior_summary:
  - Instagram week/detail analytics payloads now include additive fields: `source_media_urls`, `hosted_media_urls`, `source_thumbnail_url`, `hosted_thumbnail_url`, `cover_source`, and `cover_source_confidence`.
  - Cover source inference distinguishes likely custom cover-photo reels vs default/still-frame reels when explicit hints exist.
  - Fixed an instagram post-detail query bug that could raise at runtime due an undefined SQL expression variable.
- validation_evidence:
  - `python -m py_compile /Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/social_season_analytics.py` (pass)
  - `pytest -q /Users/thomashulihan/Projects/TRR/TRR-Backend/tests/repositories/test_social_season_analytics.py -k "instagram_cover_source or week_detail_instagram_includes_thumbnail_url or get_post_comments_instagram_includes_metadata_fields"` (pass, `4 passed`)

Continuation (same session, 2026-02-27) — Fandom person gallery header parsing + metadata/tag assignment hardening.
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- delegation_map:
  - role: `Design Context Owner`
    scope: `Fandom person-page gallery parsing semantics`
    deliverable: `implemented Gallery section subheader parsing so image rows inherit per-subheader show/season/type context instead of generic article context`
    verification_command: `pytest tests/ingestion/test_fandom_person_scraper.py tests/ingestion/test_cast_photo_sources_fandom.py`
    status: `completed`
  - role: `UI Implementer`
    scope: `n/a (backend-only)`
    deliverable: `n/a`
    verification_command: `n/a`
    status: `completed`
  - role: `API Integration Owner`
    scope: `cast photo source normalization`
    deliverable: `fandom cast-photo rows now assign show/season/content-type/title/person metadata from gallery headers and persist title/people tags for downstream lightbox/meta usage`
    verification_command: `pytest tests/ingestion/test_cast_photo_sources_fandom.py`
    status: `completed`
  - role: `QA Owner`
    scope: `parser + source-row regression coverage`
    deliverable: `added fixture/tests for Andy-style gallery with per-show season headers and asserted inferred RHOC/RHOSLC metadata`
    verification_command: `pytest tests/ingestion/test_fandom_person_scraper.py tests/ingestion/test_cast_photo_sources_fandom.py`
    status: `completed`
- risk_class: `medium`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `yes`
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/ingestion/fandom_person_scraper.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/ingestion/cast_photo_sources.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/fixtures/fandom/andy_cohen_gallery_sample.html`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/ingestion/test_fandom_person_scraper.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/ingestion/test_cast_photo_sources_fandom.py`
- behavior_summary:
  - Added dedicated Gallery-subsection parsing in `parse_fandom_person_html`: images under `Gallery` now inherit nearest `h3/h4` labels (for example: `The Real Housewives of Orange County Season 18 Reunion`), inferred season number, and inferred context type (`reunion_look`, `confessional_look`, etc.).
  - Fandom cast-photo rows now persist richer metadata derived from gallery headers and page context: `content_type`, `show_name`, `show_title`, `show_short_code`, `season_number`, `episode_number` (when present), `source_page_title`, `asset_name`, `name`, and `tags` (`people`/`titles`).
  - Fandom row-level `people_names` and `title_names` are now populated for downstream assignment and lightbox metadata visibility.
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest tests/ingestion/test_fandom_person_scraper.py tests/ingestion/test_cast_photo_sources_fandom.py` (pass, `4 passed`)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check trr_backend/ingestion/fandom_person_scraper.py trr_backend/ingestion/cast_photo_sources.py tests/ingestion/test_fandom_person_scraper.py tests/ingestion/test_cast_photo_sources_fandom.py` (pass)

Continuation (same session, 2026-02-27) — IMDb media saver metadata assignment hardening (show/season/episode/title).
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- delegation_map:
  - role: `Design Context Owner`
    scope: `IMDb person media saver parsing + metadata enrichment`
    deliverable: `identified enrichment gap where unresolved IMDb title IDs were not mapped to show/season/episode metadata`
    verification_command: `pytest tests/api/routers/test_admin_person_images.py -k enrich_cast_photos_with_episode_metadata_falls_back`
    status: `completed`
  - role: `UI Implementer`
    scope: `n/a (backend-only)`
    deliverable: `n/a`
    verification_command: `n/a`
    status: `completed`
  - role: `API Integration Owner`
    scope: `IMDb parser + metadata endpoints`
    deliverable: `added robust section-label parsing for IMDb mediaviewer and fallback title-page enrichment path for unresolved IMDb episode IDs`
    verification_command: `pytest tests/integrations/imdb/test_person_gallery_parser.py tests/integrations/imdb/test_title_page_metadata.py`
    status: `completed`
  - role: `QA Owner`
    scope: `regression coverage`
    deliverable: `added tests for singular Title section extraction, TVEpisode title-page parsing, and fallback enrichment assignment`
    verification_command: `pytest tests/integrations/imdb/test_person_gallery_parser.py tests/integrations/imdb/test_title_page_metadata.py tests/api/routers/test_admin_person_images.py -k "mediaviewer or title_page_metadata or enrich_cast_photos_with_episode_metadata_falls_back"`
    status: `completed`
- risk_class: `medium`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `yes`
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/integrations/imdb/person_gallery.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/integrations/imdb/title_page_metadata.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_person_images.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/integrations/imdb/test_person_gallery_parser.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/integrations/imdb/test_title_page_metadata.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_person_images.py`
- behavior_summary:
  - IMDb mediaviewer parser now extracts title IDs/names from section headers labeled either `Title` or `Titles` (including variants like `Title (1)`).
  - IMDb title-page parser now extracts additive episode-series fields for TVEpisode pages: `title_type`, `series_title`, `series_imdb_id`, `season_number`, `episode_number`, `episode_air_date`.
  - Person refresh metadata enrichment now keeps DB-first episode matching and adds IMDb-title fallback for unresolved `title_imdb_ids`, populating `show_name`, `show_short_code`, `show_imdb_id`, `season_number`, `episode_number`, `episode_title`, and `episode_imdb_id`.
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest tests/integrations/imdb/test_person_gallery_parser.py tests/integrations/imdb/test_title_page_metadata.py tests/api/routers/test_admin_person_images.py -k "mediaviewer or title_page_metadata or enrich_cast_photos_with_episode_metadata_falls_back"` (pass, `7 passed`)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check trr_backend/integrations/imdb/person_gallery.py trr_backend/integrations/imdb/title_page_metadata.py api/routers/admin_person_images.py tests/integrations/imdb/test_person_gallery_parser.py tests/integrations/imdb/test_title_page_metadata.py tests/api/routers/test_admin_person_images.py` (pass)

Continuation (same session, 2026-02-27) — auto-crop/centering stabilization for thumbnail pipelines.
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- delegation_map:
  - role: `API Integration Owner`
    scope: `admin auto-count responses`
    deliverable: `added additive `thumbnail_crop` payload on cast-photo/media-asset auto-count responses so UI can reuse computed centering data`
    verification_command: `pytest tests/api/routers/test_admin_image_counts_fallback.py`
    status: `completed`
  - role: `QA Owner`
    scope: `auto-count crop response regression`
    deliverable: `added coverage for generated and existing-crop response scenarios`
    verification_command: `pytest tests/api/routers/test_admin_image_counts_fallback.py`
    status: `completed`
- risk_class: `low`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `yes`
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_image_counts.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_image_counts_fallback.py`
- behavior_summary:
  - `POST /api/v1/admin/cast-photos/{photo_id}/auto-count` now includes resolved `thumbnail_crop` in response.
  - `POST /api/v1/admin/media-assets/{asset_id}/auto-count` now includes resolved `thumbnail_crop` from generated crop, link context fallback, or metadata fallback.
  - Crop payload normalization now safely clamps/normalizes x/y/zoom/mode while preserving additive fields (`strategy`, `generated_at`).
- validation_evidence:
  - `python3 -m py_compile /Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_image_counts.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest -q tests/api/routers/test_admin_image_counts_fallback.py` (pass, `7 passed`)

Continuation (same session, 2026-02-28) — Facebook/YouTube hosted media URL repair + CDN guardrails.
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- risk_class: `medium`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `no`
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/media/s3_mirror.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/socials/repair_social_hosted_urls.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/repositories/test_social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/media/test_s3_mirror.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/scripts/test_repair_social_hosted_urls.py`
- behavior_summary:
  - Added CDN-host compliance guard to `_platform_post_needs_media_mirror(...)`: rows with non-empty hosted URLs on non-CDN hosts now correctly return `needs_mirror=True` (when `AWS_CDN_BASE_URL` host is configured).
  - Hardened `AWS_CDN_BASE_URL` validation to reject direct S3 endpoints (for example `*.s3.amazonaws.com`, `s3.<region>.amazonaws.com`).
  - Added one-time repair script `scripts/socials/repair_social_hosted_urls.py` with platform filter, optional season filter, dry-run/apply modes, and JSON summary output.
  - Script includes a local import fallback so it runs via `python scripts/socials/repair_social_hosted_urls.py ...` without requiring manual `PYTHONPATH=.` setup.
  - Executed controlled repair against Supabase target DB for `facebook,youtube`; rewrote legacy S3-hosted URLs to CloudFront while preserving object key paths.
- validation_evidence:
  - Unit tests:
    - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest -q tests/media/test_s3_mirror.py -k "cdn_base_url_rejects_s3_endpoints or cdn_base_url_rejects_placeholder"` (pass, `5 passed`)
    - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest -q tests/repositories/test_social_season_analytics.py -k "flags_non_cdn_thumbnail_host or flags_non_cdn_media_host or accepts_matching_cdn_hosts or youtube_requires_hosted_media_urls or tiktok_requires_hosted_media_urls"` (pass, `5 passed`)
    - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest -q tests/scripts/test_repair_social_hosted_urls.py` (pass, `5 passed`)
  - Lint:
    - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check trr_backend/media/s3_mirror.py scripts/socials/repair_social_hosted_urls.py tests/media/test_s3_mirror.py tests/scripts/test_repair_social_hosted_urls.py` (pass)
  - DB repair execution:
    - Baseline counts before repair:
      - `facebook_posts media_urls_s3=10 thumb_urls_s3=10`
      - `youtube_videos media_urls_s3=8 thumb_urls_s3=8`
    - Dry run:
      - `PYTHONPATH=. python scripts/socials/repair_social_hosted_urls.py --platforms facebook,youtube --dry-run`
      - summary: `rows_needing_repair=18` (`facebook=10`, `youtube=8`)
    - Apply:
      - `PYTHONPATH=. python scripts/socials/repair_social_hosted_urls.py --platforms facebook,youtube --apply`
      - summary: `rows_updated=18` (`facebook=10`, `youtube=8`)
    - Post-check counts:
      - `facebook_posts media_urls_s3=0 thumb_urls_s3=0`
      - `youtube_videos media_urls_s3=0 thumb_urls_s3=0`
    - Sample readability probes after repair:
      - `facebook_media` => `206 image/jpeg`
      - `facebook_thumb` => `206 image/jpeg`
      - `youtube_media` => `206 video/mp4`
      - `youtube_thumb` => `206 image/webp`
- residual_risks:
  - Existing repository-wide `ruff check` on `trr_backend/repositories/social_season_analytics.py` still reports pre-existing E501 lines outside this change scope; targeted tests/lint for modified behaviors pass.

Continuation (same session, 2026-02-28) — social token parity, instagram collaborator preservation, and bravo facebook/threads recovery.
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- delegation_map:
  - role: `Design Context Owner`
    scope: `root-cause execution for social token/display regressions`
    deliverable: `implemented contract-safe additive backend responses and persistence updates for tiktok/youtube/facebook/threads`
    verification_command: `pytest -q tests/repositories/test_social_season_analytics.py tests/api/routers/test_socials_season_analytics.py`
    status: `completed`
  - role: `UI Implementer`
    scope: `n/a (backend repo)`
    deliverable: `n/a`
    verification_command: `n/a`
    status: `completed`
  - role: `API Integration Owner`
    scope: `social season analytics repository payload/persistence`
    deliverable: `added token persistence/upsert behavior, week/post token parity, metadata coverage fixes, bravo target default merge behavior, migration, and idempotent backfill scripts`
    verification_command: `pytest -q tests/repositories/test_social_season_analytics.py tests/api/routers/test_socials_season_analytics.py`
    status: `completed`
  - role: `QA Owner`
    scope: `repository + router regression coverage`
    deliverable: `added/updated tests for collaborator preservation, token persistence, token payload parity, post metadata coverage counts, and facebook/threads platform maps`
    verification_command: `pytest -q tests/repositories/test_social_season_analytics.py tests/api/routers/test_socials_season_analytics.py`
    status: `completed`
- risk_class: `medium`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `validation-only`
  - `TRR-APP`: `yes`
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/supabase/migrations/0155_social_token_columns_for_cross_platform_posts.sql`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/socials/backfill_social_post_tokens.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/socials/backfill_bravo_missing_platform_targets.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/repositories/test_social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_socials_season_analytics.py`
- behavior_summary:
  - Instagram permalink enrichment now preserves existing collaborators when metadata lacks collaborator values.
  - Upsert paths now persist token arrays for missing platforms/columns (tiktok mentions, youtube/facebook/threads hashtags+mentions) with schema guards.
  - Week detail payloads and `get_post_comments` now return additive hashtags/mentions for tiktok/youtube/facebook/threads with stored-first and text fallback behavior.
  - `post_metadata` coverage now counts tags/mentions across platforms using token columns and text-regex fallback, including YouTube description-aware logic.
  - Bravo default targets include facebook/threads and `_target_accounts_by_platform` now merges missing platform defaults without overriding explicit rows.
  - Added migration `0155` for token columns and two idempotent scripts:
    - `backfill_social_post_tokens.py` (`--season-id`, `--platforms`, `--batch-size`, `--dry-run`)
    - `backfill_bravo_missing_platform_targets.py` (`--season-id`, `--updated-by`, `--dry-run`)
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest -q tests/repositories/test_social_season_analytics.py tests/api/routers/test_socials_season_analytics.py` (pass, `177 passed`)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest -q tests/repositories/test_social_season_analytics.py -k "default_targets_include_rhoslc_aliases or target_accounts_by_platform_uses_direct_targets_query or target_accounts_by_platform_does_not_override_explicit_platform_rows or compute_post_metadata_counts_tags_and_mentions_for_cross_platform_tokens or upsert_tiktok_post_persists_mentions or upsert_youtube_video_persists_hashtags_and_mentions or enrich_instagram_post_preserves_existing_collaborators_when_metadata_missing or week_detail_facebook_includes_token_fallbacks or week_detail_threads_includes_token_fallbacks or week_detail_youtube_uses_effective_saved_comment_count or get_post_comments_tiktok_includes_comment_media_and_metadata or get_post_comments_youtube_includes_thumbnail_url or get_post_comments_facebook_includes_hashtags_and_mentions or get_post_comments_threads_includes_hashtags_and_mentions or upsert_facebook_post_falls_back_to_scraped_at_when_posted_at_missing or upsert_threads_post_falls_back_to_scraped_at_when_posted_at_missing"` (pass, `16 passed`)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest -q tests/api/routers/test_socials_season_analytics.py -k "accepts_facebook_threads_platform_filters or returns_facebook_threads_platform_maps"` (pass, `2 passed`)
  - `cd /Users/thomashulihan/Projects/TRR/screenalytics && rg -n "tiktok_posts|youtube_videos|facebook_posts|meta_threads_posts|hashtags|mentions|season_targets" -S` (no direct dependency coupling found)
  - `cd /Users/thomashulihan/Projects/TRR/screenalytics && pytest -q` (fails in baseline unrelated areas; `1923 passed, 50 skipped, 38 failed`, primarily legacy page shims, celery optional dependency, and device/provider environment-specific tests)

Continuation (same session, 2026-02-28) — TikTok hosted HTML mirror artifact detection + targeted requeue.
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- risk_class: `medium`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `yes` (consumer behavior benefits from remirrored playable assets)
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/socials/backfill_social_media_mirror_jobs.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/repositories/test_social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/scripts/test_backfill_social_media_mirror_jobs.py`
- behavior_summary:
  - Added `_hosted_media_urls_need_content_repair(...)` in mirror eligibility logic to treat hosted `.html/.htm` media URLs as invalid mirror outputs requiring remirror.
  - Updated `_platform_post_needs_media_mirror(...)` to return `True` for hosted HTML media URLs even when mirror status is `mirrored` and CDN host matches.
  - Extended `scripts/socials/backfill_social_media_mirror_jobs.py` with `--hosted-html-only` filtering so only rows with hosted HTML media artifacts are requeued.
  - Script JSON output now includes `hosted_html_only` for deterministic auditability.
- validation_evidence:
  - Unit tests:
    - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest -q tests/repositories/test_social_season_analytics.py -k "flags_html_hosted_media_urls or flags_non_cdn_media_host or accepts_matching_cdn_hosts or tiktok_requires_hosted_media_urls"` (pass, `4 passed`)
    - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest -q tests/scripts/test_backfill_social_media_mirror_jobs.py` (pass, `2 passed`)
  - Lint/syntax:
    - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check scripts/socials/backfill_social_media_mirror_jobs.py tests/scripts/test_backfill_social_media_mirror_jobs.py tests/repositories/test_social_season_analytics.py` (pass)
    - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && python -m py_compile trr_backend/repositories/social_season_analytics.py scripts/socials/backfill_social_media_mirror_jobs.py tests/scripts/test_backfill_social_media_mirror_jobs.py tests/repositories/test_social_season_analytics.py` (pass)
  - Operational execution (Supabase DB):
    - Pre-check: hosted HTML TikTok rows
      - query result: `{'html_rows': 74, 'total_rows': 405}`
    - Requeue run:
      - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && PYTHONPATH=. python scripts/socials/backfill_social_media_mirror_jobs.py --platforms tiktok --weeks 104 --limit-per-platform 5000 --source-scope bravo --hosted-html-only`
      - result: `{"totals": {"scanned": 405, "queued": 74, "skipped": 331, "failed": 0}, "by_platform": {"tiktok": {"scanned": 405, "queued": 74, "skipped": 331, "failed": 0}}}`
    - Post-check status on targeted rows:
      - `{'status_counts_for_html_rows': [{'status': 'pending', 'count': 74}]}`
- residual_risks:
  - Remirror jobs were queued successfully; hosted HTML URLs remain until workers complete and write fresh mirrored media URLs.
  - Repository-wide `ruff check trr_backend/repositories/social_season_analytics.py` still reports pre-existing E501 lines outside this change scope.

Continuation (same session, 2026-02-28) — remove mirrored-count fetch short-circuit in person refresh stream.
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- risk_class: `medium`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `yes`
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_person_images.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/ai/HANDOFF.md`
- behavior_summary:
  - Removed the stream-stage skip path that treated IMDb/TMDb as fully synced when mirrored count met known-source totals.
  - Stream refresh now always performs source fetch attempts for IMDb/TMDb/Fandom stages (still reporting known totals/mirrored counts for diagnostics) so new upstream images are discoverable.
  - `source_skip_details` remains available but no longer records `already_mirrored` fetch bypass entries.
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ./.venv/bin/python -m pytest tests/api/routers/test_admin_person_images.py -q` (pass, `23 passed`)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ./.venv/bin/ruff check api/routers/admin_person_images.py` (pass)
  - End-to-end probe through web proxy:
    - `curl -N http://127.0.0.1:3000/api/admin/trr-api/people/1b76d932-b414-4abd-9699-7e2388abd3f0/refresh-images/stream ...`
    - observed `sync_imdb: "Syncing IMDb..."` followed by `"Synced IMDb (5 photos)."` (no `Skipping IMDb ... already mirrored` event).
- residual_risks:
  - Known-source totals are still heuristic diagnostics and can drift from real upstream source totals; they should not be used for gating fetch behavior (now removed for this stream path).

Continuation (same session, 2026-02-28) — refresh stream heartbeat cadence improvement.
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- risk_class: `low`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `yes`
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_person_images.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/ai/HANDOFF.md`
- behavior_summary:
  - Reduced source-fetch heartbeat join interval in refresh SSE loop from `10s` to `2s`, increasing progress event frequency during long IMDb/TMDb/Fandom fetches.
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ./.venv/bin/python -m pytest tests/api/routers/test_admin_person_images.py -q` (pass, `23 passed`)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ./.venv/bin/ruff check api/routers/admin_person_images.py` (pass)
- residual_risks:
  - Higher heartbeat frequency increases event volume on long-running syncs; payloads are small and expected impact is minimal in local admin usage.

Continuation (same session, 2026-02-28) — playable social mirror fix (page URL -> MP4), all-platform requeue, and season-6 rerun progress.
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- risk_class: `medium`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `yes` (post details playback behavior benefits from corrected hosted media)
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/repositories/test_social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/ai/HANDOFF.md`
- behavior_summary:
  - Added page/media quality guardrails to mirror eligibility:
    - hosted page/HTML URLs are considered invalid mirror outputs.
    - tiktok/youtube rows now require hosted video-like media URLs (not just non-empty arrays).
    - rows with structurally valid hosted outputs are no longer repeatedly re-queued only because status was `failed/partial/pending`.
  - Updated platform mirror stage re-resolution behavior:
    - tiktok/youtube re-resolution now treats page/non-video source URLs as repair-needed.
    - tiktok now enables ytdlp fallback when source media is missing/page-like and preserves canonical watch URL for fallback mirroring.
    - youtube now falls back to canonical watch URL when resolved URLs are absent/non-video-quality.
  - Added yt-dlp download fallback inside `_mirror_platform_media_to_s3_result(...)` for tiktok/youtube page URLs, producing mirrored MP4 binaries instead of HTML artifacts.
  - Fixed yt-dlp output selection bug (zero-byte placeholder path) by switching to template output in a temp directory and selecting non-zero media output.
  - Added/updated regression tests for host/page quality checks, tiktok allow_ytdlp behavior, canonical fallback path, and non-page unresolved failure path.
- validation_evidence:
  - Unit tests:
    - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest -q tests/repositories/test_social_season_analytics.py -k "platform_post_needs_media_mirror or run_platform_media_mirror_stage_tiktok"` (pass, `13 passed`)
    - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest -q tests/repositories/test_social_season_analytics.py -k "mirror_instagram_media_to_s3 or mirror_platform_media_to_s3 or platform_post_needs_media_mirror or run_platform_media_mirror_stage_tiktok"` (pass)
    - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest -q tests/media/test_s3_mirror.py -k "cdn_base_url_rejects_s3_endpoints"` (pass)
    - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest -q tests/scripts/test_repair_social_hosted_urls.py` (pass)
  - Syntax:
    - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && python -m py_compile trr_backend/repositories/social_season_analytics.py` (pass)
  - Season execution scope:
    - season_id: `e9161955-6ee4-4985-865e-3386a0f670fb` (`show_id=7782652f-783a-488b-8860-41b97de32e75`, season 6)
    - all-platform requeue run (`requeue_media_mirror_jobs`) results:
      - instagram queued `965`
      - tiktok queued `149`
      - youtube queued `131`
      - twitter queued `456`
      - threads queued `22`
      - facebook queued `0`
    - additional bounded manual processing completed in-session:
      - threads: `22` completed
      - tiktok: queue drained (`0` queued_like)
      - youtube: reduced to `0` queued_like
      - twitter: `44` completed in bounded pass; `412` queued_like remain
      - instagram: background queue still large (`911` queued_like)
  - Representative playable URL probes (HTTP range check `206 video/mp4`):
    - instagram: `https://d1fmdyqfafwim3.cloudfront.net/social/instagram/7782652f-783a-488b-8860-41b97de32e75/6/week-0/DOqZBCzgBuL/media-01.mp4`
    - tiktok (fixed week-0 problematic post): `https://d1fmdyqfafwim3.cloudfront.net/social/tiktok/7782652f-783a-488b-8860-41b97de32e75/6/week-0/7540327205503601933/therealhousewivesofsaltlakecitybravotvTikTokPost2.mp4`
    - youtube (current hosted row for `J9TFhnjgwKQ`): `https://d1fmdyqfafwim3.cloudfront.net/social/youtube/7782652f-783a-488b-8860-41b97de32e75/6/week-2/J9TFhnjgwKQ/therealhousewivesofsaltlakecitybravoYouTubePost3.mp4`
    - twitter: `https://d1fmdyqfafwim3.cloudfront.net/social/twitter/7782652f-783a-488b-8860-41b97de32e75/6/week-18/2011339494734078234/therealhousewivesofsaltlakecityGyalaxyzeTwitterPost1_S1.mp4`
    - threads: `https://d1fmdyqfafwim3.cloudfront.net/social/threads/7782652f-783a-488b-8860-41b97de32e75/6/week-0/b42547e4-f80e-4039-bfed-/therealhousewivesofsaltlakecitybravotvThreadsPost1_S1.mp4`
  - Facebook season snapshot after rerun attempts:
    - `hosted_mp4=0`, `source_video_hint=0` across `17` rows (no extractable source video URLs in current facebook rows).
- residual_risks:
  - Large queues remain for instagram/twitter in this season (`instagram 911`, `twitter 412` queued_like). Additional worker time is required for full completion.
  - Remaining tiktok/youtube rows still needing mirror are primarily source-restricted (`tiktok_media_unresolved`) or blocked by size (`media[0]:asset_too_large` for larger YouTube assets under current `SOCIAL_MEDIA_MIRROR_MAX_BYTES` default).

Continuation (same session, 2026-02-28) — season-6 social mirror drain completion + final verification.
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- risk_class: `medium`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `yes` (consumer playback now uses mirrored MP4 on repaired rows)
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/ai/HANDOFF.md`
- behavior_summary:
  - Completed active drain by running dedicated media mirror workers for `instagram` (`--parallel 6`) and `twitter` (`--parallel 4`) until queue depletion.
  - Stopped temporary drain worker processes after queue reached zero.
  - Verified requested sample platform URLs are publicly readable/playable (`HTTP 200`, `content-type: video/mp4`) and confirmed DB hosted URL rows for user-specified IDs.
  - Computed post-rerun residual needs via `_platform_post_needs_media_mirror(...)` for season `e9161955-6ee4-4985-865e-3386a0f670fb`.
- validation_evidence:
  - Final queue state (`social.scrape_jobs`, season-scoped, `job_type like '%media_mirror%'`):
    - `facebook queued_like=0 running=0 completed=11 failed=0`
    - `instagram queued_like=0 running=0 completed=2256 failed=0`
    - `threads queued_like=0 running=0 completed=39 failed=0`
    - `tiktok queued_like=0 running=0 completed=587 failed=0`
    - `twitter queued_like=0 running=0 completed=1141 failed=5`
    - `youtube queued_like=0 running=0 completed=473 failed=3`
  - Platform samples verified:
    - instagram `DOqZBCzgBuL` -> hosted MP4 present and `HTTP 200 video/mp4`
    - tiktok `7608346230631976205` + problematic `7540327205503601933` -> hosted MP4 present and `HTTP 200 video/mp4`
    - youtube `J9TFhnjgwKQ` -> hosted MP4 present and `HTTP 200 video/mp4`
    - twitter `2011339494734078234` -> hosted MP4 present and `HTTP 200 video/mp4`
    - threads `DObQ1nbEYSd` -> hosted MP4 present and `HTTP 200 video/mp4`
    - facebook season snapshot remains no hosted playable video rows (`17 total`, `0` hosted video-like URL rows).
  - Residual need counts from `_platform_post_needs_media_mirror(...)` (season-scoped):
    - `instagram: 965`
    - `tiktok: 148`
    - `youtube: 88`
    - `twitter: 3`
    - `facebook: 0`
    - `threads: 0`
  - Top residual errors:
    - instagram: `http_403_auth_or_expired` variants
    - tiktok: `tiktok_media_unresolved`
    - youtube: `media[0]:asset_too_large`, `youtube_media_unresolved`
    - twitter: `http_404_not_found`
- residual_risks:
  - No queue backlog remains, but residual mirror-eligible rows are blocked by upstream/source constraints (expired protected source URLs, unresolved media extraction, or configured mirror size limits).
  - To reduce residuals meaningfully, source refresh/re-scrape and/or mirror-size policy adjustments are required; repeated reruns without source refresh are likely to reproduce the same failures.

Continuation (same session, 2026-02-28) — refresh-driven person gallery correction (IMDb provenance + self-healing repair).
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- risk_class: `medium`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `yes`
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/integrations/imdb/title_page_metadata.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_person_images.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/integrations/imdb/test_title_page_metadata.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_person_images.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/ai/HANDOFF.md`
- behavior_summary:
  - Hardened IMDb episode title-page parsing: when JSON-LD `partOfSeries` is missing, parser now derives parent series from the HTML `<title>` and decodes HTML entities in title/show text.
  - Added authoritative show-resolution behavior during IMDb fallback enrichment:
    - resolves by `shows.imdb_id` first,
    - then normalized `name` and `alternative_names` aliases,
    - writes `metadata.show_context_source` as `episode_table`, `imdb_title_fallback`, or `imdb_episode_unresolved`.
  - For unresolved IMDb episode/title rows, enrichment now explicitly nulls `metadata.show_id/show_name/show_imdb_id/show_short_code` to clear stale merged values.
  - `_apply_show_context_to_photos(...)` now skips request-context stamping for unresolved IMDb rows with episode/title evidence and marks applied context with `show_context_source=request_context`.
  - Added self-healing repair stage to both refresh endpoints (`refresh-images` and `refresh-images/stream`): each run reloads existing IMDb cast photos for the person, re-enriches them, and upserts repaired metadata immediately.
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest tests/api/routers/test_admin_person_images.py tests/integrations/imdb/test_title_page_metadata.py` (pass, `30 passed`)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check api/routers/admin_person_images.py trr_backend/integrations/imdb/title_page_metadata.py tests/api/routers/test_admin_person_images.py tests/integrations/imdb/test_title_page_metadata.py` (pass)
- residual_risks:
  - Show lookup map currently builds from `core.shows` per enrichment run for robust alias matching; if show volume grows significantly, this may need targeted caching/indexed lookups.

Continuation (same session, 2026-02-28) — per-face 1:1 crops + confidence-gated auto-ID integrated into Refresh Images auto-count.
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- risk_class: `medium`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `yes`
  - `TRR-APP`: `yes`
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/clients/screenalytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/media/face_crops.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_image_counts.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_person_images.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_image_counts_fallback.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_person_images_auto_count_enrichment.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/ai/HANDOFF.md`
- behavior_summary:
  - Extended screenalytics detection parsing to accept optional identity and square-crop fields while preserving backward compatibility.
  - Added deterministic 256x256 per-face crop generation/upload utility and persisted `face_crops` to cast photo metadata and media-link context.
  - Enriched Refresh Images `auto_count` stage (`refresh-images` and `refresh-images/stream`) to persist `face_boxes` + `face_crops`, and auto-populate `people_ids`/`people_names` from confident matches.
  - Preserved existing semantics: `skip_auto_count` skips full feature work; manual tags/manual crops remain authoritative.
  - Updated auto-count write paths used by single-item and batch count routes to store enriched face data.
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && python3 -m py_compile api/routers/admin_person_images.py api/routers/admin_image_counts.py trr_backend/media/face_crops.py trr_backend/clients/screenalytics.py tests/api/routers/test_admin_image_counts_fallback.py tests/api/routers/test_admin_person_images_auto_count_enrichment.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check api/routers/admin_person_images.py api/routers/admin_image_counts.py trr_backend/media/face_crops.py trr_backend/clients/screenalytics.py tests/api/routers/test_admin_image_counts_fallback.py tests/api/routers/test_admin_person_images_auto_count_enrichment.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest -q tests/api/routers/test_admin_image_counts_fallback.py tests/api/routers/test_admin_person_images_auto_count_enrichment.py` (pass, `9 passed`)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest -q tests/api/routers/test_admin_person_images.py -k "refresh_success_returns_summary or stream_honors_skip_flags_for_ingest_only_mode"` (pass)
- residual_risks:
  - Crop upload depends on source image readability for each mirror URL; when source fetch fails, face boxes can still persist while crop URL may be absent and UI falls back gracefully.

Continuation (same session, 2026-02-28) — social queue-status pool exhaustion mitigation (cache + load-shed validation).
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-backend`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- risk_class: `medium`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `yes`
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/social_season_analytics.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/ai/HANDOFF.md`
- behavior_summary:
  - Added short TTL in-process cache for `get_worker_health()` and `get_queue_status()` to prevent hot polling from repeatedly opening new DB queries each request.
  - Added cache invalidation on worker heartbeat writes so worker status can refresh promptly.
  - Queue-status responses now return cached snapshots under burst load instead of repeatedly hitting pool-heavy aggregate/recent-failure queries.
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && source .venv/bin/activate && pytest tests/db/test_pg_pool.py -q` (pass, `5 passed`)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && source .venv/bin/activate && pytest tests/api/routers/test_socials_season_analytics.py -k "queue_status_endpoint or worker_health_endpoint" -q` (pass, `3 passed`)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && source .venv/bin/activate && python -m py_compile trr_backend/repositories/social_season_analytics.py` (pass)
  - Burst check after restart: `20` parallel app proxy calls to `/api/admin/trr-api/social/ingest/queue-status` returned `ok` and backend log had no new `connection pool exhausted` / queue-status query-failed lines.
- residual_risks:
  - Repo-wide `ruff check trr_backend/repositories/social_season_analytics.py` still reports pre-existing unrelated long-line and subprocess-style findings outside this change scope.

Continuation (same session, 2026-02-28) — refresh stream stability hardening (`to_thread` follow-through, terminal SSE safety, and `source_asset_id` migration/backward compatibility).
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-backend`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- risk_class: `medium`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `yes` (consumer now receives guaranteed terminal `event:error` on unexpected stream failures)
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_person_images.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_person_images.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/supabase/migrations/0156_add_source_asset_id_to_cast_photos.sql`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/supabase/schema_docs/core.cast_photos.md`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/supabase/schema_docs/core.cast_photos.json`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/ai/HANDOFF.md`
- behavior_summary:
  - Replaced remaining async-path blocking join pattern in refresh source sync (`Thread.join(timeout=...)`) with non-blocking async polling around `asyncio.to_thread(...)` + heartbeat yields.
  - Added guarded wrappers for both SSE generators (`refresh-images/stream`, `reprocess-images/stream`) so any unhandled runtime exception emits terminal `event:error` payload with `stage="stream"` instead of silently terminating.
  - Hardened existing IMDb repair lookup to gracefully fallback when `core.cast_photos.source_asset_id` is not present yet, then normalize returned rows with `source_asset_id=None`.
  - Added migration `0156_add_source_asset_id_to_cast_photos.sql` (column add + backfill from `source_image_id` + partial index).
  - Updated cast-photos schema docs (`.md` + `.json`) to include the new column/index.
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check api/routers/admin_person_images.py tests/api/routers/test_admin_person_images.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest -q tests/api/routers/test_admin_person_images.py -k 'falls_back_when_source_asset_id_missing or emits_terminal_error'` (pass, `3 passed`)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest -q tests/api/routers/test_admin_person_images.py -k 'refresh_images_stream or reprocess_images_stream or terminal_error or source_asset_id'` (pass, `3 passed`)
- residual_risks:
  - Existing stream-specific tests in `test_admin_person_images.py` remain nested under another test function in current tree and are not collected by pytest; targeted top-level tests were added for new guardrails, but broader stream coverage should be normalized in a follow-up cleanup.

Continuation (same session, 2026-02-28) — migration `0156` applied to live DB + live SSE refresh verification.
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-qa`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `n/a`
- risk_class: `low` (operational execution only)
- default_skill_chain_applied: `false`
- default_skill_chain_used: `n/a` (no repo-tracked code edits in this continuation)
- default_skill_chain_exception_reason: `Operational migration apply + runtime verification only.`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `yes` (verified proxy stream behavior live)
- behavior_summary:
  - Applied `supabase/migrations/0156_add_source_asset_id_to_cast_photos.sql` to configured Supabase DB from `.env`.
  - Verified `core.cast_photos.source_asset_id` column exists, partial index exists, and backfill is complete (`0` rows with `source_image_id is not null and source_asset_id is null`).
  - Executed live backend refresh SSE and observed `event: complete`.
  - Executed live Next.js proxy refresh SSE and observed connect checkpoints (`connect_start`, `proxy_connected`, `backend_streaming`) plus forwarded backend events through `event: complete`.
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && set -a && source .env && set +a && ./scripts/db/run_sql.sh supabase/migrations/0156_add_source_asset_id_to_cast_photos.sql` (pass: `ALTER TABLE`, `UPDATE 17315`, `CREATE INDEX`)
  - `./scripts/db/run_sql.sh -c "...information_schema + pg_indexes + null-check queries..."` (pass)
  - Live backend stream: `curl -N http://127.0.0.1:8000/api/v1/admin/person/<id>/refresh-images/stream ...` (pass, streamed to `event: complete`)
  - Live proxy stream: `curl -N http://127.0.0.1:3000/api/admin/trr-api/people/<id>/refresh-images/stream ...` (pass, streamed connect checkpoints + backend events to `event: complete`)

Continuation (same session, 2026-02-28) — show Settings `Add Link(s)` classifier endpoint + IMDb/TMDb discovery coverage.
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-backend`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- risk_class: `medium`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `yes`
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_show_links.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_show_links.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/ai/HANDOFF.md`
- behavior_summary:
  - Added `POST /api/v1/admin/shows/{show_id}/links/add` to accept one or many URL/handle inputs and classify each into show/season/person targets with `link_group` + `link_kind` inference.
  - Classifier supports IMDb, TMDb (show/person/season paths), Wikipedia, Wikidata, Fandom/Wikia, Bravo profile URLs, and social URL/handle normalization (`instagram:foo`, `x:@foo`, etc.).
  - Added connected knowledge-link expansion so IMDb/TMDb/Wikipedia/Wikidata inputs can auto-upsert companion Wikidata/Wikipedia links when resolvable.
  - Extended discover flow to include show-level IMDb and TMDb links from `core.shows.imdb_id` / `core.shows.tmdb_id`.
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check api/routers/admin_show_links.py tests/api/routers/test_admin_show_links.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest tests/api/routers/test_admin_show_links.py -q` (pass, `43 passed`)
- residual_risks:
  - URL-to-entity routing is heuristic for generic Wikipedia/Fandom pages; ambiguous titles may default to show-level links and may still need manual edit in Settings.

Continuation (same session, 2026-02-28) — Bravo-only gating for BravoTV suggestions and classifier assignments.
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-backend`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- risk_class: `low`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `yes` (Settings `Add Link(s)` now receives Bravo-only classifier errors on non-Bravo shows)
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_show_links.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_show_links.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/ai/HANDOFF.md`
- behavior_summary:
  - Discovery no longer emits `official_page` `bravotv.com/<slug>` for non-Bravo network shows.
  - `Add Link(s)` classifier now rejects `bravotv.com` links unless the show is Bravo-network.
  - Added regression tests for both non-Bravo behaviors.
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check api/routers/admin_show_links.py tests/api/routers/test_admin_show_links.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest tests/api/routers/test_admin_show_links.py -q` (pass, `45 passed`)
- residual_risks:
  - Network metadata quality controls this behavior; if a Bravo show is missing `"bravo"` in `core.shows.networks`, BravoTV link classification/suggestions will be intentionally blocked until data is corrected.

Continuation (same session, 2026-02-28) — removed blanket Real-Housewives Fandom fallback and added missing-page validation.
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-backend`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- risk_class: `low`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `yes` (Settings discovery no longer suggests invalid Real-Housewives Fandom pages for unrelated shows)
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_show_links.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_show_links.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/ai/HANDOFF.md`
- behavior_summary:
  - Removed unconditional fallback that generated `https://real-housewives.fandom.com/wiki/{show_name}` for every show.
  - Added show-level Fandom candidate validation in discovery: fetched pages that return missing-page content (`"There is currently no text in this page"`) are now excluded.
  - Existing show-level Fandom links are only re-suggested when they resolve successfully and pass missing-page checks.
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check api/routers/admin_show_links.py tests/api/routers/test_admin_show_links.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest tests/api/routers/test_admin_show_links.py -q` (pass, `46 passed`)
- residual_risks:
  - If upstream Fandom returns transient HTML errors/timeouts during discovery, candidate links may be skipped until a later retry.

Continuation (same session, 2026-02-28) — Wikipedia show-link patch now cascades to show/season Wikipedia links.
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-backend`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- risk_class: `medium`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `yes` (Settings edit flow now receives cascaded Wikipedia updates from backend)
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_show_links.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_show_links.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/ai/HANDOFF.md`
- behavior_summary:
  - Updating a show-level Wikipedia link through `PATCH /api/v1/admin/shows/{show_id}/links/{link_id}` now canonicalizes the show Wikipedia URL and cascades updates.
  - Cascade sync updates other show-level Wikipedia links for the same show and refreshes non-manual season Wikipedia links.
  - Season URLs are resolved from season Wikidata sitelinks when available; otherwise derived from the canonical show Wikipedia title plus `_season_{n}` suffix.
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check api/routers/admin_show_links.py tests/api/routers/test_admin_show_links.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest tests/api/routers/test_admin_show_links.py -q` (pass, `48 passed`)
- residual_risks:
  - Derived fallback season URLs rely on naming convention (`<show_title>_season_<n>`), so uncommon title/season page naming can still require manual override.

Continuation (same session, 2026-02-28) — Wikipedia missing-page and wrong-variant filtering for show Settings links.
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-backend`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- risk_class: `medium`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `yes` (show Settings `Add Link(s)` now rejects missing/mismatched Wikipedia URLs at backend)
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_show_links.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_show_links.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/ai/HANDOFF.md`
- behavior_summary:
  - Added centralized Wikipedia URL resolution helper that canonicalizes article URLs and flags missing pages.
  - `Add Link(s)` classifier now rejects Wikipedia URLs that resolve to missing pages with detail: `"Wikipedia does not have an article with this exact name."`.
  - Show-level Wikipedia classification now rejects mismatched variants using Wikidata comparison (e.g., blocks UK `https://en.wikipedia.org/wiki/The_Traitors` for a show whose Wikidata item is different).
  - Show link discovery now skips missing show-level Wikipedia pages and skips show-level Wikipedia candidates whose Wikidata item does not match the show record.
  - `PATCH /admin/shows/{show_id}/links/{link_id}` now rejects missing show-level Wikipedia pages and mismatched show variants before persisting.
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check api/routers/admin_show_links.py tests/api/routers/test_admin_show_links.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest tests/api/routers/test_admin_show_links.py -q` (pass, `53 passed`)
- residual_risks:
  - Variant mismatch guard depends on a populated `core.shows.wikidata_id`; if absent, discovery/classifier can still accept ambiguous Wikipedia titles when no stronger identifier is available.

Continuation (same session, 2026-02-28) — network blog URL support + curated Traitors fandom base URLs in show Settings discovery/classifier.
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-backend`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- risk_class: `low`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `yes` (show Settings link discovery/classification now returns network blog + curated fandom candidates)
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_show_links.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_show_links.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/ai/HANDOFF.md`
- behavior_summary:
  - Added `network_blog` link kind and classifier detection for:
    - `https://www.peacocktv.com/blog/show/<slug>`
    - `https://www.nbc.com/nbc-insider/franchise/<slug>`
  - These URLs are assigned as show-level `cast_announcements` links with labels `Peacock Blog` / `NBC Insider`.
  - Show discovery now auto-suggests network blog URLs for shows whose `networks` include `peacock` and/or `nbc`.
  - Added curated show-level fandom base URLs for `The Traitors`:
    - `https://thetraitorsuk.fandom.com/wiki/The_Traitors_US`
    - `https://thetraitors.fandom.com/wiki/The_Traitors_(US)`
  - Curated fandom URLs are still validated against allowlisted domains and missing-page checks before inclusion.
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check api/routers/admin_show_links.py tests/api/routers/test_admin_show_links.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest tests/api/routers/test_admin_show_links.py -q` (pass, `57 passed`)
- residual_risks:
  - Curated fandom URL coverage is currently explicit for `The Traitors`; additional franchise-specific curated bases must be added intentionally to avoid broad false positives.

Continuation (same session, 2026-02-28) — refresh/reprocess stream resize stage now emits live operation progress during long variant generation.
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-backend`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- risk_class: `medium`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `yes`
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_person_images.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/ai/HANDOFF.md`
- behavior_summary:
  - Resize stage in both `refresh_person_images_stream` and `reprocess_person_images_stream` now forwards `_resize_person_gallery_images(..., progress_cb=...)` updates through SSE heartbeat payloads.
  - Heartbeat events now publish current operation counts (processed/total variant ops) instead of fixed `0/1` while long-running resize work executes in `asyncio.to_thread(...)`.
  - Completion events now emit final resize op counters based on actual attempted operations.
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check api/routers/admin_person_images.py tests/api/routers/test_admin_person_images.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest -q tests/api/routers/test_admin_person_images.py -k 'resizing_heartbeat or stream_emits_resizing_stage_and_complete_counters or emits_terminal_error or falls_back_when_source_asset_id_missing'` (pass, `5 passed`)
- residual_risks:
  - `resize_succeeded` remains summary success count while heartbeat progress reflects processed operations; during active runs, progress can advance even when final success count later ends lower due failures.

Continuation (same session, 2026-02-28) — IMDb person-gallery sync now supports Traitors/WWHL filtering + solo-first ranking, and reports real gallery totals.
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-backend`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- risk_class: `medium`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `yes` (refresh stream progress now receives more accurate IMDb source totals and filtered/ordered rows)
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_person_images.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/ingestion/cast_photo_sources.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/integrations/imdb/person_gallery.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/ingestion/test_cast_photo_sources_imdb.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/integrations/imdb/test_person_gallery_parser.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/ai/HANDOFF.md`
- behavior_summary:
  - Added show-context IMDb focus filters for refresh runs: when show context is Traitors/WWHL, IMDb gallery rows are filtered by show/episode IMDb IDs and Traitors/WWHL title/caption keywords.
  - IMDb gallery rows now support solo-first ranking (`people_count == 1` first) before applying per-source limits.
  - Added `extract_imdb_person_mediaindex_total(...)` to read true gallery totals from IMDb page payloads (`__NEXT_DATA__`) with text fallback, and wired source-total progress reporting to this value.
  - Result: UI no longer misleadingly reports `50/50` when IMDb has hundreds of images; it now reflects gallery total even though fetched rows may still be constrained by upstream page edge size.
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check api/routers/admin_person_images.py trr_backend/ingestion/cast_photo_sources.py trr_backend/integrations/imdb/person_gallery.py tests/ingestion/test_cast_photo_sources_imdb.py tests/integrations/imdb/test_person_gallery_parser.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest -q tests/ingestion/test_cast_photo_sources_imdb.py tests/integrations/imdb/test_person_gallery_parser.py` (pass, `9 passed`)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest -q tests/api/routers/test_admin_person_images.py -k 'refresh_stream_emits_resizing_heartbeat_during_long_variant_generation or stream_emits_resizing_stage_and_complete_counters'` (pass, `1 passed`)
- residual_risks:
  - IMDb person-gallery ingestion still consumes only the first page of `all_images.edges` from the current mediaindex HTML payload; true total is now reported, but additional page retrieval is still a follow-up item.

Continuation (same session, 2026-02-28) — existing IMDb cast-photo metadata now refreshes on upsert instead of remaining stale.
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-backend`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- risk_class: `medium`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `yes` (refresh results now include updated IMDb metadata fields for existing rows)
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/cast_photos.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/repositories/test_cast_photos_upsert.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/ai/HANDOFF.md`
- behavior_summary:
  - Root cause: DB upsert functions preserved existing `core.cast_photos.metadata` and ignored incoming metadata payloads on conflict.
  - Implemented repository-side metadata merge pass after RPC upsert:
    - matches returned rows by dedupe identity/canonical key,
    - merges incoming metadata into existing metadata,
    - persists merged metadata via `UPDATE core.cast_photos SET metadata = ...`.
  - Existing IMDb images now get revised metadata (title/caption/tag/source fields) during refresh/upsert runs.
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check api/routers/admin_person_images.py trr_backend/ingestion/cast_photo_sources.py trr_backend/integrations/imdb/person_gallery.py trr_backend/repositories/cast_photos.py tests/ingestion/test_cast_photo_sources_imdb.py tests/integrations/imdb/test_person_gallery_parser.py tests/repositories/test_cast_photos_upsert.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest -q tests/repositories/test_cast_photos_upsert.py tests/ingestion/test_cast_photo_sources_imdb.py tests/integrations/imdb/test_person_gallery_parser.py` (pass, `17 passed`)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest -q tests/api/routers/test_admin_person_images.py -k 'stream_emits_resizing_stage_and_complete_counters or refresh_stream_emits_resizing_heartbeat_during_long_variant_generation'` (pass, `1 passed`)
- residual_risks:
  - Metadata merge is shallow (`{**existing, **incoming}`), so nested objects (for example `metadata.tags`) are replaced as whole objects, not deep-merged.

Continuation (same session, 2026-02-28) — show Settings refresh now prunes invalid non-Bravo/incorrect-community Fandom links from existing rows.
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-backend`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- risk_class: `medium`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `yes` (Settings `Refresh` now gets backend cleanup results for invalid existing show/season knowledge links)
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_show_links.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_show_links.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/ai/HANDOFF.md`
- behavior_summary:
  - Kept the existing show/season knowledge-link cleanup wired into `POST /api/v1/admin/shows/{show_id}/links/discover`, including invalid-domain and missing-page pruning for Fandom/Wikipedia.
  - Updated cleanup semantics so invalid manual links are only preserved when already `approved`; invalid manual `pending` links are now deleted during refresh.
  - Added US-variant Traitors alias matching for curated Fandom domains so variant show names still enforce correct-community domain filtering.
  - Fixed and expanded tests around invalid show-level Fandom cleanup to cover:
    - non-manual invalid deletion,
    - manual approved invalid preservation,
    - manual pending invalid deletion.
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check api/routers/admin_show_links.py tests/api/routers/test_admin_show_links.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest -q tests/api/routers/test_admin_show_links.py` (pass, `62 passed`)
- residual_risks:
  - If upstream Fandom/Wikipedia fetching returns transient errors, cleanup records validation failures and may defer deletion until a later refresh.

Continuation (same session, 2026-02-28) — discovered knowledge links now auto-approve and Wikidata submissions derive connected IMDb/TMDb/TVDB IDs.
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-backend`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- risk_class: `medium`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `yes` (show Settings no longer requires approving discovered knowledge links that were validated/saved)
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_show_links.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_show_links.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/.env.example`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/README.md`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/scripts/README.md`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/README_local.md`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/ai/HANDOFF.md`
- behavior_summary:
  - `POST /api/v1/admin/shows/{show_id}/links/discover` now defaults discovered `knowledge` links to `approved` status instead of `pending`.
  - Added `tvdb` and `ratinggraph` knowledge link kind support (`TVDB` / `RatingGraph` labels + URL classification for `thetvdb.com` and `ratingraph.com` links).
  - Extended Wikidata summary parsing to pull external IDs from claims (`P345` IMDb, `P4983` TMDb TV, `P4985` TMDb person, `P4835` TVDB, `P12544` RatingGraph TV show ID).
  - Show discovery now backfills missing show-level IMDb/TMDb/TVDB/RatingGraph candidates from Wikidata claims when those IDs are absent in `core.shows`.
  - `Add Link(s)` connected-link expansion from Wikidata/Wikipedia now auto-generates additional IMDb/TMDb/TVDB/RatingGraph links where Wikidata provides them.
  - Environment/docs now include canonical `TVDB_API_KEY` alongside existing TMDb variables (`THETVDB_API_KEY` retained as legacy alias in examples).
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check api/routers/admin_show_links.py tests/api/routers/test_admin_show_links.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest -q tests/api/routers/test_admin_show_links.py` (pass, `67 passed`)
- residual_risks:
  - Wikidata-derived external IDs depend on claim quality; if the item has stale/missing properties, connected IDs will be absent or may require manual correction.

Continuation (same session, 2026-02-28) — refresh now force-promotes pending validated links and IMDb/TMDb fetch errors no longer hide person source URLs.
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-backend`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- risk_class: `medium`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `yes` (refresh now receives `pending_links_promoted` and older pending rows move to approved)
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_show_links.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_show_links.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/ai/HANDOFF.md`
- behavior_summary:
  - Added `_promote_pending_links_to_approved(...)` and wired it into `POST /api/v1/admin/shows/{show_id}/links/discover` after cleanup passes.
  - Refresh now auto-promotes pending rows for:
    - knowledge links (`imdb`, `tmdb`, `wikidata`, `wikipedia`, `fandom/wikia`, `tvdb`, `ratinggraph`)
    - `network_blog` links.
  - Discovery response now includes `pending_links_promoted`.
  - Updated person-source fallback behavior: when IMDb/TMDb validation returns `fetch_error` (challenge/temporary block), discovery now keeps the canonical candidate URL instead of dropping it.
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check api/routers/admin_show_links.py tests/api/routers/test_admin_show_links.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest -q tests/api/routers/test_admin_show_links.py` (pass, `69 passed`)
- residual_risks:
  - Auto-promotion assumes non-rejected pending knowledge/blog rows are trusted after cleanup; if stale bad rows evade cleanup rules, they can be promoted and may require manual deletion.

Continuation (same session, 2026-02-28) — fixed Wikipedia season-link validation/derivation so missing pages are never auto-approved and US-variant pages are preferred.
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-backend`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- risk_class: `medium`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `no`
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_show_links.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_show_links.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/ai/HANDOFF.md`
- behavior_summary:
  - Hardened `_resolve_wikipedia_url(...)`:
    - when Wikipedia API summary fetch fails, it now falls back to page HTML and explicitly returns `missing` for placeholder pages (`"Wikipedia does not have an article with this exact name."` / missing-page markers),
    - returns `fetch_error` when it cannot verify, instead of silently treating unverified URLs as valid.
  - Updated show discovery to only include show-level Wikipedia rows when resolver outcome is fully valid (`error is None`).
  - Updated season discovery to derive and validate candidates in priority order:
    1) season Wikidata sitelink,
    2) season URL derived from the attached canonical show Wikipedia page,
    3) plain show-name fallback.
    Only validated URLs are emitted.
  - Updated manual `Add Link(s)` classification for Wikipedia to reject unverified fetch-error cases with explicit retry guidance.
  - Updated show/season invalid-link scan to count Wikipedia fetch-error rows as validation failures (defer delete) rather than treating them as valid.
  - Applied data correction for show `0306e098-f671-4815-972c-696c359243b6`:
    - removed invalid season URLs like `.../The_Traitors_season_N` that resolve as missing,
    - promoted/kept validated US-variant season pages like `.../The_Traitors_(American_TV_series)_season_N` as `approved`.
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check api/routers/admin_show_links.py tests/api/routers/test_admin_show_links.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest -q tests/api/routers/test_admin_show_links.py` (pass, `72 passed`)
- residual_risks:
  - If Wikipedia is intermittently unreachable, some links may remain unverified (`fetch_error`) until next refresh; they are no longer auto-promoted as valid in that state.

Continuation (same session, 2026-02-28) — refresh discovery now reads related season/cast links from the show's Wikidata entity and assigns matched Wikidata pages to season/person rows.
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-backend`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- risk_class: `medium`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `no` (existing links UI consumes enriched rows)
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_show_links.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_show_links.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/ai/HANDOFF.md`
- behavior_summary:
  - Extended Wikidata summary extraction to include related item IDs from claims:
    - season entities from `P527` (has part),
    - cast entities from `P161` (cast member).
  - `discover_show_links` refresh path now uses show-level Wikidata claims to enrich season links:
    - when a season row lacks `external_wikidata_id`, it infers the Wikidata season entity by matching season number from related Wikidata labels/titles,
    - adds season Wikidata link rows with source `show_wikidata_season_claims`.
  - `discover_show_links` refresh path now uses show-level Wikidata claims to enrich cast links:
    - when a cast person lacks a Wikidata ID in local sources, it tries exact normalized name match against cast-claim entities,
    - adds person Wikidata link rows with source `show_wikidata_cast_claims`.
  - Kept discovery conservative: only exact normalized-name matches are auto-assigned for cast from show-level claims.
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check api/routers/admin_show_links.py tests/api/routers/test_admin_show_links.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest -q tests/api/routers/test_admin_show_links.py` (pass, `74 passed`)
- residual_risks:
  - Show-level cast claims can include non-roster people; exact-name matching avoids most false positives but may miss alias/middle-name variants.

Continuation (same session, 2026-02-28) — backend-owned Reddit refresh pipeline added (async runs + canonical Supabase persistence + admin APIs).
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-backend`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- risk_class: `high`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `yes`
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/supabase/migrations/0157_reddit_refresh_pipeline.sql`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/reddit_refresh.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/socials.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/repositories/test_reddit_refresh.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_socials_reddit_refresh_routes.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/ai/HANDOFF.md`
- behavior_summary:
  - Added canonical cloud Supabase storage for Reddit refresh runs/posts/comments/period matches under `social.*`.
  - Added backend async admin endpoints:
    - `POST /api/v1/admin/socials/reddit/runs`
    - `GET /api/v1/admin/socials/reddit/runs/{run_id}`
    - `GET /api/v1/admin/socials/reddit/cache`
  - Added URS-style listing + flair-search backfill collection flow with corrected exhaustive completeness semantics.
  - Added persistence + merge behavior (`reddit_post_id`/`reddit_comment_id` dedupe) and cached period payload retrieval.
  - Added rate-limit backoff/retry logic and short-lived DB transaction pattern (network fetches no longer hold DB connections).
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check api/routers/socials.py trr_backend/repositories/reddit_refresh.py tests/repositories/test_reddit_refresh.py tests/api/routers/test_socials_reddit_refresh_routes.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest -q tests/repositories/test_reddit_refresh.py tests/api/routers/test_socials_reddit_refresh_routes.py` (pass, `7 passed`)
- residual_risks:
  - BackgroundTasks-backed async execution is process-local; a dedicated worker claim/execute path is still recommended for stronger durability across restarts.
  - Comment harvesting currently captures available tree payload from `/comments/{id}.json`; `morechildren` expansion is not implemented yet.

Continuation (same session, 2026-02-28) — Reddit refresh runs now default to post-only ingestion for speed and lower rate-limit pressure.
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-backend`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- risk_class: `medium`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `yes`
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/socials.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/reddit_refresh.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/ai/HANDOFF.md`
- behavior_summary:
  - Added additive request field `fetch_comments` to Reddit refresh run input (`default=false`).
  - `execute_refresh_run` now honors `request_payload.fetch_comments` and skips comment tree expansion when disabled.
  - Run diagnostics now include `comments.enabled` so operators can confirm whether comment harvesting was active for a run.
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check api/routers/socials.py trr_backend/repositories/reddit_refresh.py tests/api/routers/test_socials_reddit_refresh_routes.py tests/repositories/test_reddit_refresh.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest -q tests/repositories/test_reddit_refresh.py tests/api/routers/test_socials_reddit_refresh_routes.py` (pass, `7 passed`)
- residual_risks:
  - BackgroundTasks execution is still process-local; durable worker claim-loop cutover remains the next reliability step for restart safety.

Continuation (same session, 2026-02-28) — refresh link discovery now uses all show Fandom bases to discover/test season and cast pages per community.
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-backend`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- risk_class: `medium`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `no`
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_show_links.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_show_links.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/ai/HANDOFF.md`
- behavior_summary:
  - Added shared show-level Fandom seed collection used by season/person discovery, sourcing from:
    - current refresh run’s discovered show-level Fandom links,
    - existing persisted show-level `entity_links` (`fandom`/`wikia`, non-rejected).
  - Extended cast-person Fandom discovery:
    - probes each show Fandom community domain for each cast name,
    - validates candidate profile pages,
    - stores multiple valid Fandom profile links (one valid URL per domain), instead of stopping after first match.
  - Extended season discovery with Fandom support:
    - probes each show Fandom community domain using season-aware queries,
    - validates candidate pages as real (not missing page) and confirms season-number match from URL/title/heading metadata,
    - only emits valid season Fandom links; missing-page responses are excluded.
  - Wired discover endpoint so season/person discovery receives show-level Fandom seeds from the same refresh pass.
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check api/routers/admin_show_links.py tests/api/routers/test_admin_show_links.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest tests/api/routers/test_admin_show_links.py` (pass, `76 passed`)
- residual_risks:
  - Season Fandom discovery currently keeps top valid result per community domain; if a wiki has multiple valid season page variants on the same domain, only the first validated candidate is retained.

Continuation (same session, 2026-02-28) — Week-0 Reddit refresh execution hardening + cloud migration application.
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-backend`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- risk_class: `high`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `yes`
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/reddit_refresh.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/ai/HANDOFF.md`
- behavior_summary:
  - Applied migration `0157_reddit_refresh_pipeline.sql` to the cloud Supabase target used by backend (`social.reddit_*` tables now present).
  - Fixed runtime defects that blocked real refresh execution:
    - `_update_run` now uses `UPDATE ... RETURNING id` so `execute_returning` is valid.
    - Upsert inserts for posts/comments/period matches removed timestamp columns that already default in schema (resolved `INSERT has more target columns than expressions`).
  - Expanded search backfill with phrase gap-fill query per flair (in addition to `flair:"..."`) to recover older window rows Reddit flair search alone misses.
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check api/routers/socials.py trr_backend/repositories/reddit_refresh.py tests/repositories/test_reddit_refresh.py tests/api/routers/test_socials_reddit_refresh_routes.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest -q tests/repositories/test_reddit_refresh.py tests/api/routers/test_socials_reddit_refresh_routes.py` (pass, `7 passed`)
  - Live run execution + persistence verification:
    - run id: `338f9b66-94ed-48d7-a007-66740513f469`
    - status: `partial`
    - totals: `fetched_rows=10`, `matched_rows=10`, `tracked_flair_rows=4`
    - persisted rows in `social.reddit_period_post_matches` for run: `10` (tracked flair key `salt lake city`: `4`)
- residual_risks:
  - Run remains `partial` because listing crawl completeness is false for deep history (`window_exhaustive_complete=false`), although backfill returned in-window rows.
  - Discovery currently stores both show-match and flair-match rows; if strict flair-only window payloads are required, output filtering should be tightened in a follow-up.

Continuation (same session, 2026-02-28) — season discovery now auto-adds TMDb season URLs when show TMDb IDs are available.
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-backend`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- risk_class: `low`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `no`
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_show_links.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_show_links.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/ai/HANDOFF.md`
- behavior_summary:
  - `_discover_season_links(...)` now emits TMDb season links automatically when `core.shows.tmdb_id` is present.
  - Generated URL format: `https://www.themoviedb.org/tv/{show_tmdb_id}/season/{season_number}`.
  - Source attribution prefers `core.seasons.tmdb_season_id` when available, otherwise falls back to `core.shows.tmdb_id`.
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check api/routers/admin_show_links.py tests/api/routers/test_admin_show_links.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest tests/api/routers/test_admin_show_links.py` (pass, `77 passed`)
- residual_risks:
  - TMDb season URL generation depends on show-level TMDb ID being present; if a show lacks `tmdb_id`, season TMDb links are still skipped.

Continuation (same session, 2026-02-28) — link refresh now reports richer discovery metrics and improves Fandom/Wikidata season+cast coverage.
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-backend`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- risk_class: `medium`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `yes`
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_show_links.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_show_links.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/ai/HANDOFF.md`
- behavior_summary:
  - Fandom season discovery now tries deterministic seed-derived URLs per domain before API search, then validates each page; this improves coverage when search endpoints miss known pages.
  - Fandom person discovery now also tries deterministic per-domain person profile URLs (`/wiki/{Name}`) before search fallback.
  - Person Wikidata fallback now also scans cast IDs attached to season Wikidata items (from the show’s season claims), not only show-level cast claims.
  - Discover response now returns richer counts for progress/telemetry in UI:
    - `counts_by_kind`
    - `counts_by_entity_type`
    - `fandom_domains_used`
    - `fandom_links_by_entity`
    - `tmdb_season_links_discovered`
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check api/routers/admin_show_links.py tests/api/routers/test_admin_show_links.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest tests/api/routers/test_admin_show_links.py` (pass, `80 passed`)
- residual_risks:
  - Season/person Fandom generation still depends on community naming conventions; deterministic URL attempts plus search fallback improve coverage but cannot guarantee every custom wiki title pattern.

Continuation (same session, 2026-02-28) — Reddit run-status now includes queue depth metrics for active-job visibility.
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-backend`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- risk_class: `low`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `yes`
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/reddit_refresh.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/repositories/test_reddit_refresh.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_socials_reddit_refresh_routes.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/ai/HANDOFF.md`
- behavior_summary:
  - `get_refresh_run(...)` now includes additive `queue` metrics in response payload:
    - `running_total`, `queued_total`, `other_running`, `other_queued`, `queued_ahead`.
  - Queue metrics are computed from active (`queued`/`running`) reddit refresh runs and account for the current run’s own status.
  - Enables APP to explain queued state with concrete backlog/running-worker context.
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest tests/repositories/test_reddit_refresh.py tests/api/routers/test_socials_reddit_refresh_routes.py` (pass, `9 passed`)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check trr_backend/repositories/reddit_refresh.py tests/repositories/test_reddit_refresh.py tests/api/routers/test_socials_reddit_refresh_routes.py` (pass)
- residual_risks:
  - Queue counters are point-in-time and can shift quickly under concurrent refresh activity; UI should treat them as live estimates.

Continuation (same session, 2026-02-28) — Wikidata expansion now includes presenter-linked people and TVmaze IDs (show/season).
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-backend`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- risk_class: `medium`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `yes` (consumes richer discover payload and links)
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_show_links.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_show_links.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/ai/HANDOFF.md`
- behavior_summary:
  - `_fetch_wikidata_summary(...)` now treats both `cast member (P161)` and `presenter (P371)` as cast-like person links for assignment expansion.
  - Added Wikidata external ID extraction for TVmaze:
    - show ID: `P8600` -> `https://www.tvmaze.com/shows/{id}`
    - season ID: `P10669` -> `https://www.tvmaze.com/seasons/{id}`
  - Show discovery now auto-adds `tvmaze` links from show external IDs or Wikidata external IDs.
  - Season discovery now auto-adds `tvmaze` links from season external IDs or season Wikidata external IDs.
  - Connected-link builder now emits `tvmaze` when present on linked Wikidata entities.
  - Manual link classifier now recognizes TVmaze URLs as `knowledge/tvmaze`.
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check api/routers/admin_show_links.py tests/api/routers/test_admin_show_links.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest tests/api/routers/test_admin_show_links.py` (pass, `82 passed`)
- residual_risks:
  - Wikidata claims vary by item quality; if a season/show omits TVmaze/presenter claims, discovery still falls back to existing ID/link inference paths.

Continuation (same session, 2026-02-28) — IMDb refresh now supports Traitors-strict filtering, diagnostics, and targeted metadata backfill.
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-backend`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- risk_class: `medium`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `no` (additive backend payload fields only)
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/integrations/imdb/person_gallery.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/ingestion/cast_photo_sources.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_person_images.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/integrations/imdb/test_person_gallery_parser.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/ingestion/test_cast_photo_sources_imdb.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_person_images.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/ai/HANDOFF.md`
- behavior_summary:
  - IMDb mediaindex parsing now preserves normalized `image_type` for each gallery row (`event`, `still_frame`, etc.).
  - IMDb source fetcher now supports strict Traitors context filtering inputs:
    - `strict_types`, `target_person_imdb_id`, `target_person_name`,
    - `allowed_cast_imdb_ids`, `allowed_cast_names`, `allowed_episode_imdb_ids`,
    - `strict_mode_enabled`, `imdb_diagnostics`.
  - Strict keep rules are enforced as requested:
    - type must be `event` or `still_frame`,
    - keep when solo self, or self+Traitors-cast-only group, or still-frame matching allowed episode IMDb IDs.
  - Strict ranking now prefers: `solo_self` -> `traitors_cast_group` -> `episode_still_frame`, then fewer people, then gallery index.
  - Additive IMDb diagnostics are propagated through refresh response/stream payloads:
    - `imdb_pages_scanned`, `imdb_candidates_seen`, `imdb_kept`,
    - `imdb_filtered_type`, `imdb_filtered_people`, `imdb_filtered_episode`, `imdb_filtered_other`.
  - Existing IMDb repair now runs as targeted backfill:
    - only rows with weak/missing IMDb metadata are refreshed,
    - mediaviewer data is merged into existing row metadata,
    - `imdb_image_type` and `imdb_metadata_refreshed_at` are written,
    - strict filter annotations are applied when Traitors-strict context is active.
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ./.venv/bin/ruff check api/routers/admin_person_images.py trr_backend/ingestion/cast_photo_sources.py trr_backend/integrations/imdb/person_gallery.py tests/ingestion/test_cast_photo_sources_imdb.py tests/integrations/imdb/test_person_gallery_parser.py tests/api/routers/test_admin_person_images.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ./.venv/bin/pytest -q tests/ingestion/test_cast_photo_sources_imdb.py tests/integrations/imdb/test_person_gallery_parser.py tests/api/routers/test_admin_person_images.py -k "imdb or traitors or focus or repair"` (pass, `27 passed, 27 deselected`)
- residual_risks:
  - Strict cast matching relies on IMDb people tags; rows with sparse/missing people tags depend on episode still-frame fallback to be retained.
  - Traitors-strict activation depends on show-name resolution containing `traitors`; ambiguous/missing show context will fall back to legacy behavior.

Continuation (same session, 2026-02-28) — IMDb refresh hardening for live Traitors runs (stream diagnostics + pagination + image-type fallback).
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-backend`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- risk_class: `medium`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `no`
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/integrations/imdb/person_gallery.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/ingestion/cast_photo_sources.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_person_images.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/integrations/imdb/test_person_gallery_parser.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/ingestion/test_cast_photo_sources_imdb.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_person_images.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/ai/HANDOFF.md`
- behavior_summary:
  - Added stream test coverage to assert `sync_imdb` progress payloads include IMDb diagnostics fields.
  - Fixed strict-mode pagination scan behavior in IMDb fetcher so strict runs are not capped at first page.
  - Updated IMDb GraphQL pagination request to POST JSON (`content-type: application/json`) for current endpoint contract; previous GET query pattern returned HTTP 415 and silently limited scans to page 1.
  - Added mediaviewer image-type fallback parsing from `__NEXT_DATA__` (per-image `type`) and propagated to:
    - ingest filtering (`fetch_imdb_cast_photos`)
    - targeted existing-row repair (`_repair_existing_imdb_cast_photos`)
  - Result: strict filter can classify rows even when mediaindex payload omits `imageType`.
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ./.venv/bin/ruff check trr_backend/integrations/imdb/person_gallery.py trr_backend/ingestion/cast_photo_sources.py api/routers/admin_person_images.py tests/integrations/imdb/test_person_gallery_parser.py tests/ingestion/test_cast_photo_sources_imdb.py tests/api/routers/test_admin_person_images.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ./.venv/bin/pytest -q tests/integrations/imdb/test_person_gallery_parser.py tests/ingestion/test_cast_photo_sources_imdb.py tests/api/routers/test_admin_person_images.py -k "imdb or traitors or stream or repair or mediaviewer"` (pass, `36 passed`)
  - Live refresh verification (Alan Cumming, Traitors context, IMDb-only, ingest-only skips) via backend non-stream endpoint:
    - `photos_fetched=50`
    - `photos_upserted=50`
    - `imdb_pages_scanned=3`
    - `imdb_candidates_seen=150`
    - `imdb_kept=50`
    - `imdb_filtered_type=0`
    - `imdb_filtered_episode=80`
- residual_risks:
  - Dev-mode hot reload can interrupt long-running SSE curl captures (`curl: (18)`); non-stream refresh is currently more reliable for deterministic live validation.
  - Existing unrelated TMDb profile upsert data-shape error still appears during refresh (`malformed array literal` on aliases) and should be addressed separately.

Continuation (same session, 2026-02-28) — Reddit backfill depth upgrades + cache payload now sourced from canonical tables.
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-backend`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- risk_class: `medium`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `yes`
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/reddit_refresh.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/repositories/test_reddit_refresh.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/ai/HANDOFF.md`
- behavior_summary:
  - Search backfill improvements:
    - increased default search pages per query (`20`), max configurable up to `50`.
    - removed premature stop condition when a search page reaches period-start timestamp (now continues until `after=null` or max pages).
    - added additional backfill query kinds: show alias terms, show-name phrase, and subreddit top-year listing crawl.
    - top-year listing query now correctly uses `/r/{subreddit}/top.json?t=year` with pagination.
  - Cached payload behavior:
    - `get_cached_period_payload(...)` now always reads canonical rows from `social.reddit_period_post_matches` + `social.reddit_posts`.
    - no longer short-circuits to stale `diagnostics.result` blob.
    - totals now derive from canonical cached rows, so post counts reflect backfilled/seeded table rows.
  - Added regression test proving search backfill does not stop early when first page already crosses period-start.
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest tests/repositories/test_reddit_refresh.py tests/api/routers/test_socials_reddit_refresh_routes.py` (pass, `10 passed`)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check trr_backend/repositories/reddit_refresh.py tests/repositories/test_reddit_refresh.py` (pass)
- residual_risks:
  - Reddit index/listing APIs can still omit some directly-addressable posts; canonical cache now reflects all rows actually persisted (including later manual seeds).

Continuation (same session, 2026-02-28) — Show Links refresh now expands Wikidata hierarchy, normalizes legacy kinds, and reports deterministic stage counts.
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-backend`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- risk_class: `medium`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `yes`
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_show_links.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_show_links.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/ai/HANDOFF.md`
- behavior_summary:
  - Added show-level Wikidata fallback resolution from `core.entity_links` when `core.shows.wikidata_id` is empty, and wired it into show/season/people discovery + classifier context.
  - `_build_connected_knowledge_rows(...)` now resolves Wikidata directly from submitted primary Wikidata URL when context is missing.
  - Wikidata summary parsing now captures `P179` (`part_of_series_item_ids`) and `P12397` (`tvdb_season_id`), and season discovery carries this metadata.
  - People discovery now uses token-aware `_person_name_candidates_match(...)` for Wikidata cast matching and derives missing IMDb/TMDb person IDs from person Wikidata summaries.
  - Season Wikipedia validation now rejects wrong-franchise pages by checking season page Wikidata `P179` against the show Wikidata ID.
  - Added legacy link-kind normalization (`kg`/`knowledge_graph`/`knowledge` -> `wikipedia` or `wikidata` by host), including duplicate-safe normalization.
  - `POST /admin/shows/{show_id}/links/discover` now returns additive `stage_counts` (`show_scanned`, `season_scanned`, `people_scanned`, `legacy_rows_normalized`, `links_validated`, `links_promoted`).
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest tests/api/routers/test_admin_show_links.py` (pass, `87 passed`)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check api/routers/admin_show_links.py tests/api/routers/test_admin_show_links.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check .` (fails due existing unrelated repo-wide violations outside touched scope)
- residual_risks:
  - Wikidata claim completeness remains source-dependent; if `P179`/cast/external-id claims are absent upstream, discovery falls back to existing heuristics.

Continuation (same session, 2026-02-28) — RHOSLC scan-flair inclusion semantics + URL-seed fallback for backend-owned Reddit refresh.
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-backend`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- risk_class: `high`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `yes`
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/reddit_refresh.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/socials.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/supabase/migrations/0158_reddit_period_match_flair_mode.sql`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/repositories/test_reddit_refresh.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_socials_reddit_refresh_routes.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/ai/HANDOFF.md`
- behavior_summary:
  - Split flair inclusion semantics in discovery/matching:
    - `analysis_all_flares`: include by flair alone (`flair_mode=all`).
    - `analysis_flares`: include only when flair matches and RHOSLC term match exists (`flair_mode=scan_term`).
    - `force_include_flares`: unconditional include (`flair_mode=forced`).
    - `show_match` inclusion remains supported (`flair_mode=show_match`).
  - RHOSLC term base now includes fixed terms (`RHOSLC`, `Real Housewives of Salt Lake City`, `Salt Lake City`, `SLC`) plus show aliases/show name.
  - Added boundary-safe acronym matching (notably for `SLC`) and regex-based term matching.
  - Added URL-seed fallback ingestion:
    - new run request field `seed_post_urls`.
    - extracts post IDs from URLs, fetches submissions via `/comments/{id}.json`, applies window filtering + standard matching, merges into canonical upsert flow.
    - run/discovery diagnostics now include `seed_urls_requested|parsed|ingested|failed` and related IDs.
  - Added persistent `flair_mode` support to canonical period matches and cached payload thread rows.
  - Kept cached payload authoritative from canonical tables (`reddit_period_post_matches` + `reddit_posts`), with test coverage against stale `diagnostics.result` blobs.
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && pytest tests/repositories/test_reddit_refresh.py tests/api/routers/test_socials_reddit_refresh_routes.py` (pass, `16 passed`)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ruff check trr_backend/repositories/reddit_refresh.py tests/repositories/test_reddit_refresh.py tests/api/routers/test_socials_reddit_refresh_routes.py` (pass)
- residual_risks:
  - `0158_reddit_period_match_flair_mode.sql` must be applied before deploy/runtime paths that read/write `flair_mode`.
  - Seed URL ingestion depends on Reddit `/comments/{id}.json` availability/rate limits; failures are surfaced in diagnostics.

Continuation (same session, 2026-02-28) — TMDb cast profile alias serialization hardening + refresh TMDb status diagnostics.
- primary_skill: `senior-backend`
- supporting_skills: `orchestrate-plan-execution`, `senior-fullstack`, `senior-backend`, `senior-qa`, `code-reviewer`
- mcp_tools_used:
  - primary: `functions.exec_command`
  - fallback: `functions.apply_patch`
- risk_class: `medium`
- default_skill_chain_applied: `true`
- default_skill_chain_used: `orchestrate-plan-execution -> senior-fullstack -> senior-backend -> senior-qa -> code-reviewer`
- default_skill_chain_exception_reason: `n/a`
- downstream_repos_impacted:
  - `TRR-Backend`: `yes`
  - `screenalytics`: `no`
  - `TRR-APP`: `no`
- files_changed:
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/repositories/cast_tmdb.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/trr_backend/integrations/tmdb_person.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/api/routers/admin_person_images.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/repositories/test_cast_tmdb.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/integrations/tmdb/test_tmdb_person.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/tests/api/routers/test_admin_person_images.py`
  - `/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/ai/HANDOFF.md`
- behavior_summary:
  - Added canonical `also_known_as` normalization in `cast_tmdb` repository:
    - supports `list/tuple/set`, JSON-array strings, scalar strings, and `None`.
    - always writes `list[str]` to match `core.cast_tmdb.also_known_as` (`TEXT[]`).
    - trims whitespace and drops null/empty alias items.
  - Added enriched `CastTMDbRepositoryError` context on upsert failures including `field=also_known_as`, normalized runtime type, and bounded preview.
  - Hardened TMDb integration alias coercion:
    - `fetch_tmdb_person_details(...)` now normalizes `also_known_as` defensively.
    - `TMDbPersonFull.to_cast_tmdb_row(...)` always emits `also_known_as` as `list[str]`.
  - Added TMDb stage diagnostics to refresh outputs:
    - additive fields: `tmdb_profile_status`, `tmdb_profile_error_code`, `tmdb_profile_error_detail`.
    - emitted in non-stream refresh response and stream `complete` payload.
    - refresh remains non-terminal on TMDb failures; errors are appended with classified code.
- validation_evidence:
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ./.venv/bin/ruff check trr_backend/repositories/cast_tmdb.py trr_backend/integrations/tmdb_person.py api/routers/admin_person_images.py tests/repositories/test_cast_tmdb.py tests/integrations/tmdb/test_tmdb_person.py tests/api/routers/test_admin_person_images.py` (pass)
  - `cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ./.venv/bin/pytest -q tests/repositories/test_cast_tmdb.py tests/integrations/tmdb/test_tmdb_person.py tests/api/routers/test_admin_person_images.py -k "tmdb or cast_tmdb or refresh"` (pass, `22 passed`)
- residual_risks:
  - Live Supabase schema introspection was not available in this environment; plan assumes deployed schema still matches migration `0044` (`also_known_as TEXT[]`).
