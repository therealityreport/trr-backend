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
