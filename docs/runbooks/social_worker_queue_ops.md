# Social Worker Queue Ops Runbook

## Scope

This runbook covers social ingest queue operations, with Modal as the canonical
production remote executor and the legacy worker scripts retained for local/dev
and rollback only. The final public hosting target is Render; Modal remains the
job plane:

- Render-hosted backend API readiness
- Modal dispatcher readiness (`serve_backend_api` and worker functions)
- Modal dispatcher heartbeat availability (`social.scrape_workers`)
- Queue backlog and stuck jobs (`social.scrape_jobs`, `social.scrape_runs`)
- Comment persistence diagnostics (`comment_stats` metadata + ID guardrails)

## Required Runtime Configuration

- Canonical production/staging target:
  - `TRR_JOB_PLANE_MODE=remote`
  - `TRR_LONG_JOB_ENFORCE_REMOTE=1`
  - `TRR_REMOTE_EXECUTOR=modal`
  - `TRR_MODAL_ENABLED=1`
  - `TRR_MODAL_APP_NAME=trr-backend-jobs`
  - `TRR_MODAL_API_FUNCTION=serve_backend_api`
  - `TRR_MODAL_API_LABEL=trr-backend-api`
  - `TRR_MODAL_ADMIN_OPERATION_FUNCTION=run_admin_operation`
  - `TRR_MODAL_GOOGLE_NEWS_FUNCTION=run_google_news_sync`
  - `TRR_MODAL_REDDIT_REFRESH_FUNCTION=run_reddit_refresh`
  - `TRR_MODAL_SOCIAL_JOB_FUNCTION=run_social_job`
  - `TRR_MODAL_SOCIAL_RECOVERY_FUNCTION=sweep_social_dispatch_queue`
  - `TRR_MODAL_RUNTIME_SECRET_NAME=trr-backend-runtime`
  - `TRR_MODAL_SOCIAL_SECRET_NAME=trr-social-auth`
  - `SOCIAL_QUEUE_ENABLED=true`
- Named Modal secrets are mandatory outside local/dev:
  - `trr-backend-runtime`
  - `trr-social-auth`
- Legacy worker-family env flags remain rollback/local-only:
  - `TRR_ADMIN_OPERATION_WORKER_ENABLED`
  - `TRR_REDDIT_REFRESH_WORKER_ENABLED`
  - `TRR_GOOGLE_NEWS_WORKER_ENABLED`
- `SOCIAL_WORKER_HEARTBEAT_STALE_SECONDS` (optional, default `180`)
- `SOCIAL_WORKER_HEARTBEAT_INTERVAL_SEC` (optional, default `15`, minimum `5`)
- `SOCIAL_COMMENTS_RUN_WORKERS` (optional, default `4`, min `1`, max `8`; API-assisted comments-only fanout)
- `SOCIAL_WORKER_MIN_STAGE_RUNNERS` (optional, default `6`; floor for enabled stage worker pools)
- `SOCIAL_WORKER_ALLOW_STAGE_DISABLE` (optional, default `0`; set `1` to allow explicit `0` workers per stage)
- `SOCIAL_WORKER_POOL_POSTS` (optional, default `8`; legacy/local-only persistent posts-stage workers)
- `SOCIAL_WORKER_POOL_COMMENTS` (optional, default `8`; legacy/local-only persistent comments-stage workers)
- `SOCIAL_WORKER_POOL_MEDIA_MIRROR` (optional, default `6`; persistent post media mirror workers)
- `SOCIAL_WORKER_POOL_COMMENT_MEDIA_MIRROR` (optional, default `6`; persistent comment media mirror workers)
- `SOCIAL_WORKER_POOL_SHARED_ACCOUNT_POSTS` (optional, default `6`; legacy/local-only shared-account scrape workers)
- `SOCIAL_WORKER_POOL_POST_CLASSIFY` (optional, default `10`; internal classification concurrency target)
- `SOCIAL_WORKER_POOL_SEASON_MATERIALIZE` (optional, default `10`; internal materialization concurrency target)
- `SOCIAL_WORKER_POOL_ANALYTICS_REFRESH` (optional, default `4`; internal analytics refresh concurrency target)
- `SOCIAL_MODAL_DISPATCH_LIMIT` (optional, default `25`; maximum jobs dispatched per Modal sweep)
- `TRR_MODAL_SOCIAL_JOB_CONCURRENCY_LIMIT` (optional, default `64`; Modal `run_social_job` container cap)
- `SOCIAL_WORKER_POOL_INTERVAL_SEC` (optional, default `2`; worker idle sleep interval)
- `SOCIAL_STALE_RECOVERY_INTERVAL_SEC` (optional, default `30`; stale job recovery cadence)
- `SOCIAL_RUN_SUMMARY_RECONCILE_INTERVAL_SEC` (optional, default `60`; run-summary reconciliation cadence)
- `SOCIAL_JOB_CLAIM_BATCH_SIZE` (optional, default `posts=10`, other stages=`5`, max `25`; queue claim batch size per worker)
- `SOCIAL_RUN_IN_FLIGHT_CAP` (optional, default `24`; per-run fairness cap)
- `SOCIAL_JOB_STALE_SECONDS` (optional, default `300`; global stale timeout fallback)
- `SOCIAL_JOB_STALE_SECONDS_YOUTUBE_POSTS` (optional, default `900`)
- `SOCIAL_JOB_STALE_SECONDS_YOUTUBE_COMMENTS` (optional, default `600`)
- `SOCIAL_DB_UPSERT_BATCH_SIZE_COMMENTS` (optional, default `300`; comment/reply bulk upsert chunk size)
- `SOCIAL_DB_UPSERT_BATCH_SIZE_POSTS` (optional, default `100`; post bulk upsert chunk size)
- `SOCIAL_TWITTER_DELAY_SEC`, `SOCIAL_TIKTOK_DELAY_SEC`, `SOCIAL_YOUTUBE_DELAY_SEC` (optional, default `0.35`)
- `SOCIAL_CRAWLEE_ENABLED` (optional, default `true` in non-local deployments)
- `SOCIAL_CRAWLEE_PLATFORMS` (optional, default `instagram,tiktok,twitter,youtube`)
- `SOCIAL_CRAWLEE_FORCE_LEGACY_PLATFORMS` (optional emergency bypass)
- `SOCIAL_CRAWLEE_MAX_CONCURRENCY_*` and `SOCIAL_CRAWLEE_MAX_RETRIES_*` (optional per-platform limits)

When Crawlee runtime is enabled for a platform, auth preflight checks run before execution:

- Instagram: requires `SOCIAL_INSTAGRAM_COOKIES_JSON` or `SOCIAL_INSTAGRAM_COOKIES_FILE`
- TikTok: requires `SOCIAL_TIKTOK_COOKIES_JSON` / `SOCIAL_TIKTOK_COOKIES_FILE` (or legacy `TIKTOK_COOKIES_*`)
- Twitter/X: requires one of cookie auth (`SOCIAL_TWITTER_COOKIES_*`), bearer auth (`SOCIAL_TWITTER_BEARER_TOKEN`), or `TWIKIT_*` credentials
- YouTube: public mode supported (no auth required)

In workspace dev, `WORKSPACE_TRR_REMOTE_SOCIAL_WORKERS` remains a lane enable flag only.
Throughput tuning comes from `WORKSPACE_TRR_REMOTE_SOCIAL_DISPATCH_LIMIT`,
`WORKSPACE_TRR_MODAL_SOCIAL_JOB_CONCURRENCY_LIMIT`, and the remote social stage
caps (`WORKSPACE_TRR_REMOTE_SOCIAL_POSTS`, `...COMMENTS`, `...MEDIA_MIRROR`,
`...COMMENT_MEDIA_MIRROR`).

## Modal Secret Prep And Cutover Commands

Render the named secret env files and exact `modal secret create` commands
without mutating Modal:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
python3.11 scripts/modal/prepare_named_secrets.py
```

Render the non-mutating cutover command checklist for SSM parameters and host
env reconciliation:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
python3.11 scripts/modal/render_cutover_commands.py --parameter-prefix /trr/staging
```

Verify the named secrets, deployed app, and all eight required Modal functions
plus the API web URL before rollout:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
python3.11 scripts/modal/verify_modal_readiness.py
```

Deploy the Modal app after named secrets are provisioned:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
./.venv/bin/python -m modal deploy -m trr_backend.modal_jobs
```

After deploy, capture the public backend URL from the readiness output and point
the deployed TRR-APP runtime at the Render service URL before running admin
smoke checks. Keep the Modal API URL available only as the rollback target
during the observation window. In this phase, staging means Vercel Preview and
production means Vercel Production. `TRR_API_URL` is owned by Vercel env
configuration, not repo-local `.env` files.

Full legacy backend retirement is not complete until:

- the Render backend API URL is the canonical `TRR_API_URL` for both Vercel Preview and Vercel Production
- covered admin image-analysis jobs no longer depend on the retired legacy host
- social and non-social background work run only through Modal
- Better Stack is receiving backend and Modal job logs so centralized log access stays intact

## Legacy Worker Start Commands

The commands below are rollback/local-dev only after Modal cutover.

Start a dedicated queue worker:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
SOCIAL_QUEUE_ENABLED=true python -m scripts.socials.worker --interval 2
```

Start stage-specific workers (optional):

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
SOCIAL_QUEUE_ENABLED=true python -m scripts.socials.worker --stage posts --interval 2
SOCIAL_QUEUE_ENABLED=true python -m scripts.socials.worker --stage comments --interval 2
```

Run one specific run id:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
SOCIAL_QUEUE_ENABLED=true python -m scripts.socials.worker --run-id <run_uuid>
```

Run with multiple workers to process queued jobs in parallel:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
SOCIAL_QUEUE_ENABLED=true python -m scripts.socials.worker --parallel 4 --interval 2
```

Start a persistent full-sync worker pool (posts + comments + mirrors):

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
SOCIAL_QUEUE_ENABLED=true \
SOCIAL_WORKER_MIN_STAGE_RUNNERS=6 \
SOCIAL_WORKER_POOL_POSTS=6 \
SOCIAL_WORKER_POOL_COMMENTS=6 \
SOCIAL_WORKER_POOL_MEDIA_MIRROR=6 \
SOCIAL_WORKER_POOL_COMMENT_MEDIA_MIRROR=6 \
SOCIAL_WORKER_POOL_SHARED_ACCOUNT_POSTS=6 \
SOCIAL_WORKER_POOL_POST_CLASSIFY=6 \
SOCIAL_WORKER_POOL_SEASON_MATERIALIZE=6 \
SOCIAL_WORKER_POOL_ANALYTICS_REFRESH=2 \
SOCIAL_DB_UPSERT_BATCH_SIZE_COMMENTS=200 \
SOCIAL_DB_UPSERT_BATCH_SIZE_POSTS=50 \
./scripts/socials/start_worker_pool.sh
```

## Post Author Avatar Mirroring Guarantee

- Social post-author avatars are mirrored to S3 for non-Instagram platforms.
- Hosted avatar URLs are preferred in payloads and UI where available.
- YouTube avatar fallback uses channel-header image sources (`yt3.googleusercontent.com`) and normalizes size to `s1024` before mirror persistence.

## Remote Job Plane Workers (Admin + Reddit + Google News)

When `TRR_JOB_PLANE_MODE=remote` (or `TRR_LONG_JOB_ENFORCE_REMOTE=1`), API kickoff routes enqueue jobs and worker loops own execution:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
TRR_JOB_PLANE_MODE=remote \
TRR_LONG_JOB_ENFORCE_REMOTE=1 \
TRR_REMOTE_WORKER_POLL_SECONDS=2 \
TRR_ADMIN_OPERATION_WORKER_ENABLED=1 \
TRR_ADMIN_OPERATION_WORKER_COUNT=2 \
TRR_REDDIT_REFRESH_WORKER_ENABLED=1 \
TRR_REDDIT_REFRESH_WORKER_COUNT=2 \
TRR_GOOGLE_NEWS_WORKER_ENABLED=1 \
TRR_GOOGLE_NEWS_WORKER_COUNT=1 \
TRR_GOOGLE_NEWS_WORKER_LEASE_SECONDS=300 \
./scripts/start_remote_job_workers.sh
```

Worker-family knobs:

- `TRR_ADMIN_OPERATION_WORKER_COUNT` (default `1`)
- `TRR_REDDIT_REFRESH_WORKER_COUNT` (default `1`)
- `TRR_GOOGLE_NEWS_WORKER_COUNT` (default `1`)
- `TRR_GOOGLE_NEWS_WORKER_LEASE_SECONDS` (default `300`)
- `TRR_REMOTE_WORKER_POLL_SECONDS` (default `2`)

Individual worker entrypoints:

```bash
python -m scripts.workers.admin_operations_worker
python -m scripts.workers.reddit_refresh_worker
python -m scripts.workers.google_news_worker
```

## Controlled Benchmark Harness

Run the synthetic benchmark harness to compare baseline vs optimized queue/write-path settings with parity checks across:

- single-platform comments-heavy
- sync-all (twitter+tiktok+youtube)
- concurrent multi-run backlog

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
.venv/bin/python scripts/socials/benchmark_sync_jobs.py \
  --output docs/ai/benchmarks/social_sync_benchmark_latest.json
```

The script fails if scenario counters diverge between baseline and optimized profiles.
Use the generated JSON for before/after handoff evidence.

## API Behavior Guardrail

When queue mode is enabled and no healthy worker heartbeat exists, ingest start now fails fast:

- Endpoint: `POST /api/v1/admin/socials/seasons/{season_id}/ingest`
- Status: `503`
- Error detail code: `SOCIAL_WORKER_UNAVAILABLE`

This prevents silent enqueueing when no worker is available.

## SQL Health Checks

### 1) Worker heartbeat status

```sql
select
  worker_id,
  status,
  stage,
  run_id,
  current_job_id,
  last_seen_at,
  now() - last_seen_at as heartbeat_age
from social.scrape_workers
order by last_seen_at desc
limit 25;
```

### 2) Healthy worker count (3-minute window)

```sql
select
  count(*) filter (
    where status in ('starting', 'idle', 'working')
      and last_seen_at >= now() - interval '3 minutes'
  ) as healthy_workers,
  count(*) filter (where status <> 'stopped') as active_workers,
  count(*) as total_workers
from social.scrape_workers;
```

### 3) Queue distribution

```sql
select status, count(*) as jobs
from social.scrape_jobs
group by status
order by status;
```

### 3b) Queue distribution with stage compatibility fallback

Some environments store stage both as a physical column and in `metadata.stage`.
Use this query to avoid SQL drift across deployments:

```sql
select
  coalesce(nullif(to_jsonb(j)->>'stage',''), nullif(j.metadata->>'stage',''), 'unknown') as stage_name,
  j.status,
  count(*) as jobs
from social.scrape_jobs j
group by 1,2
order by 1,2;
```

### 4) Jobs stuck queued beyond threshold

```sql
select
  id,
  run_id,
  platform,
  stage,
  status,
  created_at,
  now() - created_at as queued_age
from social.scrape_jobs
where status = 'queued'
  and created_at < now() - interval '10 minutes'
order by created_at asc
limit 100;
```

### 5) Run status backlog view

```sql
select
  id,
  season_id,
  status,
  created_at,
  started_at,
  completed_at
from social.scrape_runs
where created_at >= now() - interval '24 hours'
order by created_at desc
limit 100;
```

### 6) Comment persistence counters by recent jobs

```sql
select
  id,
  run_id,
  platform,
  stage,
  status,
  metadata -> 'comment_stats' ->> 'comments_fetched' as comments_fetched,
  metadata -> 'comment_stats' ->> 'comments_upserted' as comments_upserted,
  metadata -> 'comment_stats' ->> 'comments_skipped_missing_id' as comments_skipped_missing_id,
  metadata ->> 'error_code' as error_code
from social.scrape_jobs
where created_at >= now() - interval '24 hours'
order by created_at desc
limit 200;
```

### 7) Guardrail sanity checks for malformed external IDs

```sql
select count(*) as blank_instagram_comment_ids
from social.instagram_comments
where btrim(comment_id) = '';

select count(*) as blank_tiktok_comment_ids
from social.tiktok_comments
where btrim(comment_id) = '';

select count(*) as blank_youtube_comment_ids
from social.youtube_comments
where btrim(comment_id) = '';

select count(*) as blank_twitter_tweet_ids
from social.twitter_tweets
where btrim(tweet_id) = '';
```

Expected: all zero after migration `0130_social_worker_heartbeat_and_comment_id_guardrails.sql`.

## Triage Playbook

1. If ingest endpoint returns `SOCIAL_WORKER_UNAVAILABLE`, verify worker process + heartbeat rows first.
2. If healthy worker count is zero:
   - start/restart worker process,
   - verify `SOCIAL_QUEUE_ENABLED=true` in API and worker runtime,
   - re-run worker heartbeat SQL checks.
3. If jobs are growing in `queued`:
   - check worker logs for recurring failures,
   - check `error_code` and `comment_stats` metadata on recent jobs.
4. If `comments_fetched` is high but `comments_upserted` is low:
   - inspect `comments_skipped_missing_id`,
   - review scraper payload quality for missing IDs by platform.
5. If jobs fail with `crawlee_auth_preflight_failed:*`:
   - verify platform auth env vars are populated,
   - verify secrets are available to worker runtime (not only API runtime),
   - use `SOCIAL_CRAWLEE_FORCE_LEGACY_PLATFORMS=<platform>` for temporary bypass.
