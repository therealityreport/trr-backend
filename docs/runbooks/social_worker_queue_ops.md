# Social Worker Queue Ops Runbook

## Scope

This runbook covers production operations for social ingest queue mode:

- Worker heartbeat availability (`social.scrape_workers`)
- Queue backlog and stuck jobs (`social.scrape_jobs`, `social.scrape_runs`)
- Comment persistence diagnostics (`comment_stats` metadata + ID guardrails)

## Required Runtime Configuration

- `SOCIAL_QUEUE_ENABLED=true` only when at least one worker process is running.
- `SOCIAL_WORKER_HEARTBEAT_STALE_SECONDS` (optional, default `180`)
- `SOCIAL_WORKER_HEARTBEAT_INTERVAL_SEC` (optional, default `15`, minimum `5`)
- `SOCIAL_COMMENTS_RUN_WORKERS` (optional, default `4`, min `1`, max `8`; API-assisted comments-only fanout)
- `SOCIAL_WORKER_POOL_GENERAL` (optional, default `4`; persistent general queue fanout workers)
- `SOCIAL_WORKER_POOL_MEDIA_MIRROR` (optional, default `2`; persistent mirror-stage workers)
- `SOCIAL_WORKER_POOL_INTERVAL_SEC` (optional, default `2`; worker idle sleep interval)
- `SOCIAL_CRAWLEE_ENABLED` (optional, default `false`)
- `SOCIAL_CRAWLEE_PLATFORMS` (optional, default `instagram,tiktok,twitter,youtube`)
- `SOCIAL_CRAWLEE_FORCE_LEGACY_PLATFORMS` (optional emergency bypass)
- `SOCIAL_CRAWLEE_MAX_CONCURRENCY_*` and `SOCIAL_CRAWLEE_MAX_RETRIES_*` (optional per-platform limits)
- `SOCIAL_YOUTUBE_PRE_WINDOW_PAGE_CAP` (optional, default `12`; continuation-page cap before yt-dlp fallback when scans stay pre-window)
- `SOCIAL_YOUTUBE_YTDLP_TIMEOUT_SECONDS` (optional, default `120`; per-query yt-dlp timeout)

When Crawlee runtime is enabled for a platform, auth preflight checks run before execution:

- Instagram: requires `SOCIAL_INSTAGRAM_COOKIES_JSON` or `SOCIAL_INSTAGRAM_COOKIES_FILE`
- TikTok: requires `SOCIAL_TIKTOK_COOKIES_JSON` / `SOCIAL_TIKTOK_COOKIES_FILE` (or legacy `TIKTOK_COOKIES_*`)
- Twitter/X: requires one of cookie auth (`SOCIAL_TWITTER_COOKIES_*`), bearer auth (`SOCIAL_TWITTER_BEARER_TOKEN`), or `TWIKIT_*` credentials
- YouTube: public mode supported (no auth required)

## Worker Start Commands

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

Start a persistent full-sync worker pool (general + media mirror):

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
SOCIAL_QUEUE_ENABLED=true \
SOCIAL_WORKER_POOL_GENERAL=4 \
SOCIAL_WORKER_POOL_MEDIA_MIRROR=2 \
./scripts/socials/start_worker_pool.sh
```

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

## YouTube Cross-Show Cleanup (Season Scoped)

Use this only when older excluded YouTube rows were saved before the current guardrails and still appear in RHOSLC analytics/detail views.

1. Inspect candidate rows:

```sql
select id, video_id, source_account, channel_title, title, published_at
from social.youtube_videos
where season_id = 'e9161955-6ee4-4985-865e-3386a0f670fb'
  and ltrim(lower(coalesce(nullif(source_account, ''), nullif(channel_title, ''), '')), '@') = 'bravo'
  and published_at >= timestamptz '2025-08-14T04:00:00+00:00'
  and published_at <= timestamptz '2025-09-16T23:59:59.999999+00:00'
  and coalesce(lower(title), '') like '%wife swap%'
  and coalesce(lower(title), '') like '%real housewives edition%';
```

2. Delete only scoped excluded rows (comments cascade automatically via FK):

```sql
delete from social.youtube_videos v
where v.season_id = 'e9161955-6ee4-4985-865e-3386a0f670fb'
  and ltrim(lower(coalesce(nullif(v.source_account, ''), nullif(v.channel_title, ''), '')), '@') = 'bravo'
  and v.published_at >= timestamptz '2025-08-14T04:00:00+00:00'
  and v.published_at <= timestamptz '2025-09-16T23:59:59.999999+00:00'
  and coalesce(lower(v.title), '') like '%wife swap%'
  and coalesce(lower(v.title), '') like '%real housewives edition%'
returning v.id, v.video_id, v.title;
```

3. Re-run YouTube posts+comments for that exact window:

```bash
curl -X POST "http://127.0.0.1:8000/api/v1/admin/socials/seasons/e9161955-6ee4-4985-865e-3386a0f670fb/ingest" \
  -H "Content-Type: application/json" \
  -d '{
    "platforms": ["youtube"],
    "source_scope": "bravo",
    "ingest_mode": "posts_and_comments",
    "sync_strategy": "incremental",
    "date_start": "2025-08-14T04:00:00+00:00",
    "date_end": "2025-09-16T23:59:59.999999+00:00"
  }'
```
