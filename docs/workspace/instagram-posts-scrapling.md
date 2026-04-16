# Instagram Posts Scrapling Runbook

**Status:** Experimental / additive. Old `scraper.py` handles production traffic. This lane is opt-in via stage-based dispatch. Last reviewed: 2026-04-16.

## What this lane does

- Uses Scrapling/Patchright to warm up an Instagram profile page (`https://www.instagram.com/{username}/`), solving challenges and extracting runtime tokens (`lsd`, `bloks_version`, `__spin_*`).
- Bridges the browser cookies into an `httpx.AsyncClient` and calls `https://www.instagram.com/graphql/query` with the profile timeline `doc_id`, paginating via `after` + `end_cursor`.
- Handles both the legacy `Graph*` response shape and the newer `XDTMediaDict` shape returned by the current profile timeline connection.
- Persists each raw GraphQL edge node through the canonical `_upsert_instagram_post()` repo helper, preserving view monotonicity, optional columns, and mirror metadata.

## Routing

- `job_type = "posts"` (already in `scrape_jobs` check constraint)
- `config.stage = INSTAGRAM_POSTS_SCRAPLING_STAGE` ("posts_scrapling")
- `config.required_worker_lane = INSTAGRAM_POSTS_SCRAPLING_WORKER_LANE` ("instagram_posts_scrapling") when using queue-safe enqueue
- Dispatch branch: `social_season_analytics.py:_execute_claimed_job` routes platform=instagram + stage=posts_scrapling → `run_instagram_posts_scrapling_job`

## Required env vars

| Var | Purpose | Notes |
|---|---|---|
| `SOCIAL_INSTAGRAM_COOKIES_JSON` or `SOCIAL_INSTAGRAM_COOKIES_FILE` | Session cookies | `auth_resolver` accepts either |
| `DECODO_USERNAME` / `DECODO_PASSWORD` / `DECODO_GATEWAY` | Proxy creds | Optional; no-proxy in local dev |
| `SOCIAL_INSTAGRAM_POSTS_PROXY_URLS` | Explicit proxy list | Takes precedence over DECODO |
| `SOCIAL_INSTAGRAM_POSTS_PROXY_PROVIDER` | Override provider | Default: `decodo` |
| `SOCIAL_INSTAGRAM_POSTS_HEADLESS` | Browser headless toggle | Default: `true` |
| `TRR_DB_URL` or `SUPABASE_DB_URL` | Postgres URL | Required |

## Invocation

### Manual smoke (creates real run+job rows)

```bash
python scripts/socials/instagram/smoke_posts_scrapling.py --account bravotv --max-pages 1 --fast
```

### Queue-safe enqueue

```python
from trr_backend.repositories.social_season_analytics import start_instagram_posts_scrapling_scrape

result = start_instagram_posts_scrapling_scrape(
    account_handle="bravotv",
    max_pages=5,
    fast_mode=True,
    initiated_by="operator@example.com",
)
# result = {"run_id": ..., "job_id": ..., "status": "queued",
#           "platform": "instagram", "account_handle": "bravotv",
#           "required_worker_lane": "instagram_posts_scrapling"}
```

The helper acquires a per-account advisory lock so only one posts_scrapling run can be active per account at a time. When `is_queue_enabled()` is True, it also asserts a worker with the `instagram_posts_scrapling` lane is available before enqueuing — preventing dead-letter jobs.

## Failure modes and recovery

| Symptom | Likely cause | Action |
|---|---|---|
| `instagram_posts_auth_failed` in scrape_jobs.error_message | Cookies invalid/expired or challenge required | Refresh cookies via `SOCIAL_INSTAGRAM_COOKIE_AUTO_REFRESH=1` or re-save from browser |
| `http_429` with `retryable: true` | Rate limiting | Decrease concurrency; wait for backoff retry |
| `html_challenge_or_auth_required` | Session triggered Instagram challenge | Refresh cookies and re-run |
| `graphql_empty_connection` on all doc_ids | Instagram rotated doc_ids | Update `PROFILE_POSTS_DOC_IDS` in `constants.py` — check a fresh profile page manually |
| No posts upserted but `items_found > 0` | Response shape changed again | Inspect `raw_node` — check if fields match XDTMediaDict or a newer shape; add a new branch to `_graph_node_to_post_dto` |
| `SOCIAL_POSTS_SCRAPLING_RUN_ALREADY_ACTIVE` from `start_instagram_posts_scrapling_scrape` | Another run is already queued/running for this account | Wait for the existing run to complete, or cancel it |
| `SocialWorkerUnavailableError` from `start_*` | Queue is enabled but no worker is heartbeating with the `instagram_posts_scrapling` lane | Start a worker with `--worker-lane instagram_posts_scrapling` or disable queue mode |

## Observability

- Structured logs under logger `socials.instagram.posts_scrapling.fetcher` with `extra.event` field. Currently emitted:
  - `warmup_success` — fires after browser warmup completes (browser nav → cookie merge → httpx client rebuild). Includes `account`, `cookie_count`, `page_tokens_count`, `proxy_fingerprint`. Tail with `grep '"event": "warmup_success"' /var/log/...`.
- Job metadata (`scrape_jobs.metadata`):
  - `stage_counters.posts` — raw posts fetched from API
  - `persist_counters.posts_upserted` — posts successfully upserted (difference indicates silent drops)
  - `fetcher_runtime.warmup_cookie_count` — number of new cookies from warmup (cookie *names* available via `warmup_cookie_names` — values are scrubbed for security)
  - `fetcher_runtime.selected_proxy_fingerprint` — safe proxy identity (host:port:provider, no credentials)
  - `fetcher_runtime.page_tokens_found` — which runtime tokens extracted (empty list means regex didn't match — IG doesn't always require them)
- `social.instagram_posts.metadata_source = "posts_scrapling"` marks rows from this lane for shadow comparison queries.

## Known limitations

- No identity pool — DECODO handles rotation at provider level. If a specific IP gets blocked, rotate DECODO session ID.
- Not yet plumbed into the shared-account dispatcher. For bulk scrapes, use the existing `shared_account_posts` stage on the legacy lane.
- Shadow comparison framework not built — compare ad-hoc via SQL:
  ```sql
  SELECT shortcode, username, likes, comments_count, views, metadata_source, scraped_at
  FROM social.instagram_posts
  WHERE username = 'bravotv'
    AND scraped_at > now() - interval '1 day'
  ORDER BY scraped_at DESC LIMIT 20;
  ```
- Orphaned-run risk: if `_create_job` fails after `_create_run` succeeds (e.g., schema validation error), the resulting orphaned `scrape_runs` row in status `queued` blocks future enqueues for this account until manually cleared. This pattern is inherited from the comments lane and tracked for Phase E cleanup.
