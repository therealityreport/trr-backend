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
  - `TRR_MODAL_SOCIAL_AUTH_PROBE_FUNCTION=probe_social_remote_auth`
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
- `SOCIAL_MODAL_DISPATCH_LIMIT` (optional, default `8`; maximum jobs dispatched per Modal sweep)
- `TRR_MODAL_SOCIAL_JOB_CONCURRENCY_LIMIT` (optional, default `8`; Modal `run_social_job` container cap)
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

## Modal Maintenance Ownership

Modal maintenance must have exactly one active owner for recovery, heartbeat,
and cleanup:

- Default owner: Modal worker-plane singleton maintenance functions, enabled by
  `TRR_MODAL_ALWAYS_ON_SCHEDULES_ENABLED=1`.
- API fallback owner: backend runtime scheduler, enabled only when
  `TRR_MODAL_RUNTIME_SCHEDULER_ENABLED=1`.
- Default API behavior: `TRR_MODAL_RUNTIME_SCHEDULER_ENABLED` unset or `0`;
  the API must not independently schedule heartbeat, social recovery, or stale
  worker cleanup.
- Deployed Modal runtime secrets set `TRR_MODAL_MAINTENANCE_OWNER_REQUIRED=1`;
  startup fails when neither owner is enabled or when both owners are enabled.
- Do not enable the API fallback while the Modal singleton owner is healthy.
  Use it only for a bounded recovery window, then set it back to `0`.
- The API fallback uses `TRR_MODAL_RUNTIME_HEARTBEAT_INTERVAL_SECONDS`,
  `TRR_MODAL_RUNTIME_SOCIAL_RECOVERY_INTERVAL_SECONDS`, and
  `TRR_MODAL_RUNTIME_STALE_WORKER_CLEANUP_INTERVAL_SECONDS` only after the
  opt-in flag is enabled.
- Always-on Modal cron schedules and warm containers are billing-sensitive.
  `scripts/modal-billing-guardrail.sh` blocks them from the same
  `TRR_MODAL_SOURCE_ENV` file used by named-secret rendering and from the
  process environment unless `WORKSPACE_ALLOW_MODAL_ALWAYS_ON_BILLING=1` is set
  for a short, intentional operator window.

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

Verify the named secrets, deployed app, and all required Modal functions
plus the API web URL before rollout:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
.venv/bin/python scripts/modal/verify_modal_readiness.py
```

Verify the deployed worker image can actually read and validate the remote Instagram cookie bundle:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
.venv/bin/python scripts/modal/verify_modal_readiness.py --probe-remote-auth instagram --json
```

Deploy the Modal app after named secrets are provisioned:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
./.venv/bin/python -m modal deploy -m trr_backend.modal_jobs
```

Use the staged manual Instagram auth pipeline when `remote_auth_capabilities.instagram.reason` is not ready or local and browser auth disagree.

First, complete the manual handoff in Chrome Profile 13:

- open Chrome Profile 13 for `codex@thereality.report`
- open Instagram and Gmail in that same profile
- let the operator complete password entry, reCAPTCHA, checkpoints, and verification codes
- stop Codex at any CAPTCHA, security prompt, checkpoint, or verification-code prompt

Validate the browser session without writing cookie files or touching Modal:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
python3.11 scripts/modal/refresh_instagram_cookies_from_chrome.py --validate-chrome-only --json
```

Expected safe validation output includes `ok: true`, `wrote_cookie_files: false`,
`pushed_to_modal: false`, `deployed_modal: false`, and `verified_remote: false`.

Sync validated Chrome cookies into local cookie files only after explicit
operator approval:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
python3.11 scripts/modal/refresh_instagram_cookies_from_chrome.py --sync-local --json --confirm-instagram-refresh "I UNDERSTAND INSTAGRAM AUTH RISK"
```

Validate local cookies without refresh, Modal secret apply, deploy, remote
verification, or cooldown writes:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
python3.11 scripts/modal/repair_instagram_auth.py --validate-local-only --json
```

Push Modal secrets and deploy are separate operator-approved stages. Do not run
either during validation:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
python3.11 scripts/modal/refresh_instagram_cookies_from_chrome.py --push-to-modal --json --confirm-instagram-refresh "I UNDERSTAND INSTAGRAM AUTH RISK"
python3.11 scripts/modal/refresh_instagram_cookies_from_chrome.py --deploy --json --confirm-instagram-refresh "I UNDERSTAND INSTAGRAM AUTH RISK"
python3.11 scripts/modal/refresh_instagram_cookies_from_chrome.py --verify-remote --json --confirm-instagram-refresh "I UNDERSTAND INSTAGRAM AUTH RISK"
```

Legacy full repair remains available for compatibility, but it is not the first
validation step:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
python3.11 scripts/modal/repair_instagram_auth.py --json
```

Repeated failed approved repair attempts can create a local cooldown lock.
Validation-only checks must not create the cooldown. Clear the lock only after
the manual checkpoint has been resolved and you are ready to validate again:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
python3.11 scripts/modal/repair_instagram_auth.py --json --clear-auth-repair-cooldown
```

Legacy lower-level auth flags such as `SOCIAL_INSTAGRAM_COOKIE_AUTO_REFRESH` and
`SOCIAL_INSTAGRAM_INTERACTIVE_LOGIN` are also blocked unless
`SOCIAL_INSTAGRAM_AUTH_REPAIR_CONFIRMATION` exactly matches
`I UNDERSTAND INSTAGRAM AUTH RISK`. Do not set that confirmation globally; use
the explicit staged commands above for one operator-controlled run.

Run the proactive local-only Instagram repair worker when you want a single command that checks cookie age, recent auth failures, and then invokes the full repair pipeline only when needed:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
python3.11 scripts/socials/cookie_refresh_worker.py --json
```

Schedule the worker on a trusted local workstation only. Do not schedule it on Modal; Playwright refresh still requires a browser-capable local environment. A simple daily cron is sufficient:

```cron
0 6 * * * cd /Users/thomashulihan/Projects/TRR/TRR-Backend && ./.venv/bin/python scripts/socials/cookie_refresh_worker.py --json >> .logs/instagram-cookie-refresh.log 2>&1
```

Worker trigger policy:

- skip when the local Instagram cookie bundle validates and `_cookie_refreshed_at` is still inside the max-age threshold
- run the validation and Modal-secret/deploy flow when the cookie age threshold is exceeded and validation is still safe
- stop for manual action when recent checkpoint, login prompt, browser-session-invalidated, or verification failures are detected
- fail loudly when validation, secret apply, deploy, or remote auth probe fails; do not silently loop on auth or verification failures

Do not treat local cookie files as proof that remote Instagram backfills are
ready. Full shared-account Instagram backfill is only considered ready when all
of the following are true:

- `scripts/modal/verify_modal_readiness.py --json` returns `ok: true` for core Modal readiness
- `scripts/modal/verify_modal_readiness.py --probe-remote-auth instagram --json` returns:
  - `remote_auth_probe.platform = "instagram"`
  - `remote_auth_probe.ready = true`
- `GET /api/v1/admin/socials/ingest/worker-health` reports:
  - `dispatcher_readiness.resolved = true`
  - `dispatcher_heartbeat_fresh = true`
  - `remote_auth_capabilities.instagram.ready = true`
  - `shared_account_backfill_readiness.ready = true`
- a bounded canary (`Sync Recent`) succeeds before a full-history `Backfill Posts`
  run is launched

`Sync Recent`, `Sync Newer`, and `Backfill Posts` are all variants of the
same shared-profile catalog backfill pipeline. The operator choice changes the seed scope,
not the pipeline family. `Backfill Posts` automatically resumes a saved older
frontier when one exists, so `Resume Tail` is no longer a separate operator
action.

The shared-account catalog route responses now expose additive execution diagnostics:

- `queue_enabled`
- `used_inline_fallback`
- `requires_modal_executor`

Normal production/shared-account behavior should keep `allow_inline_dev_fallback=false`.
If a route response comes back with `status="started"` and `used_inline_fallback=true`,
the request ran inline on the API host instead of entering the queue/Modal path.

Preferred shared Instagram canary order:

1. `Sync Recent`
2. `Sync Newer` if diagnostics show a head gap
3. `Backfill Posts` only after the first checks are green; it resumes any saved
   older frontier automatically before continuing full-history fetches

Use `Sync Newer` as the same-pipeline bounded head-gap repair when diagnostics show the newest stored post lags the live profile. It is not a separate worker path.

## Instagram Account Health Repair

Use the account-health repair command when a shared Instagram account has a
catalog gap, a post-classification backlog, or media mirror work that should be
drained after a missing-post repair. The command is report-only by default.

Preview the current gap without launching jobs:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
./.venv/bin/python scripts/socials/repair_instagram_account_health.py --account thetraitorsus --pretty
```

Apply the recommended missing-post repair and drain the follow-up
`media_mirror` and `post_classify` stages:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
./.venv/bin/python scripts/socials/repair_instagram_account_health.py --account thetraitorsus --apply --pretty
```

If the preview reports `recommended_action: wait_for_active_run`, do not launch
a second repair. Pass the active run id back to the same command so it drains
the follow-up stages for the run that already exists:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
./.venv/bin/python scripts/socials/repair_instagram_account_health.py --account thetraitorsus --run-id <run-id> --apply --pretty
```

Result fields:

- `gap_before` and `gap_after`: account catalog health before and after repair.
- `steps`: executed or planned actions, including `fill_missing_posts`,
  `drain_media_mirror`, and `drain_post_classify`.
- `repaired`: true only when the final gap report recommends no further action.

## Social Media Mirror Covers And Display Variants

Post media mirror jobs now preserve full-quality originals and derive smaller
cover variants for catalog/gallery display. The canonical full-resolution cover
is stored in `hosted_thumbnail_url`, playable videos and original carousel media
are stored in `hosted_media_urls`, and `asset_manifest` carries:

- `originals.cover` for the full-quality cover/still image
- `originals.media` for full-quality original media, including playable video
- `display_variants` for `thumb`, `card`, `detail`, and optional poster-crop
  WebP/JPEG variants
- `display_variants_status` and `display_variants_reason`
- `storage_summary` with original bytes, variant bytes, object count, variant
  count, and CDN URL count

Expected platform caveats:

- Instagram reels usually expose a still/cover and a video URL. If the cover URL
  is expired, the mirror stage tries to re-resolve it before marking variants
  failed.
- TikTok and YouTube may expose page/watch URLs for video. The mirror stage uses
  yt-dlp for playable media when supported; if no cover image is resolvable, the
  video can still mirror while display variants are marked `unsupported`.
- Twitter/X media can include video URLs plus `pbs.twimg.com` stills. Repair
  reason `twitter_video_thumbnail` means a prior run used a video URL where a
  still image candidate exists.
- Facebook and Threads can have rows with media but no independent cover source.
  These rows should keep original media and report `display_variants_status:
  unsupported` rather than inventing a still.

Run a dry-run freshness audit before broad repairs:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
python3.11 scripts/socials/audit_social_media_display_variants.py --limit-per-platform 5000
```

Add a bounded CDN smoke check when public serving is the concern:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
python3.11 scripts/socials/audit_social_media_display_variants.py --cdn-smoke --limit-per-platform 100
```

Queue variant repair without touching complete original-only rows by filtering
on the display variant reason:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
python3.11 scripts/socials/backfill_social_media_mirror_jobs.py --repair-reasons missing_display_variants --dry-run
```

Rollback gates before allowing a full shared-profile backfill:

- `scripts/modal/verify_modal_readiness.py` must return `ok: true`
- worker-health must show remote Instagram auth and shared-account backfill readiness as green
- no canary run may show runtime-version drift, stale frontier ownership, dispatch-blocked alerts, or classify backlog that never drains after scrape completion

If `shared_account_backfill_readiness.ready` is false, read the reason in this
order:

- `dispatcher_readiness.reason` or `last_dispatch_blocked_reason`
- `dispatcher_heartbeat_fresh = false`
- `remote_auth_capabilities.instagram.reason`

Use those fields to distinguish:

- Modal app/function deployment missing
- dispatcher heartbeat missing or stale
- remote Instagram auth missing in the worker plane
- queue backlog or stale-running recovery issues after dispatch has already succeeded

For remote Instagram auth failures, use these reason buckets:

- `checkpoint_required`: the shared Instagram session itself needs operator attention; refresh locally in headed mode and expect Instagram to challenge or reject the account.
- `cookie_schema_invalid`: the Modal social secret contains a partial or malformed Instagram cookie bundle; regenerate and re-apply named secrets before retrying.
- `request_error`: the worker image had structurally valid cookies but the GraphQL validation canary failed at transport/runtime level; inspect Modal runtime health, network, and deployment drift.
- `unexpected_exception:*`: the validation path itself raised inside the worker image; treat this as a runtime or image issue first, not as proof that the Instagram account is invalid.

## TikTok And YouTube Posts Smoke Checks

Use these checks when validating the TikTok posts Scrapling lane or the YouTube
shared-account posts path. They are operator smoke checks, not schema or route
changes.

First confirm Modal can see the TikTok auth bundle in the worker plane:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
python scripts/modal/verify_modal_readiness.py --probe-remote-auth tiktok --json
```

The TikTok probe is ready only when `remote_auth_probe.ready=true`. The safe
structure keys are `has_sessionid`, `has_sid_tt`, and `has_ms_token`; they show
cookie presence only and must not contain raw cookie values.

Use the local wrapper to list the approved post-path smoke commands without
requiring credentials:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
python scripts/socials/smoke_tiktok_youtube_posts.py --dry-run
```

Run the commands only from an environment with DB access and the required social
runtime secrets:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
python scripts/socials/smoke_tiktok_youtube_posts.py --run
```

The wrapper currently covers:

- `remote-auth-tiktok`: `scripts/modal/verify_modal_readiness.py --probe-remote-auth tiktok --json`
- `tiktok-posts`: `python -m scripts.socials.tiktok.smoke_posts_scrapling --account bravotv --max-pages 1`
- `youtube-posts`: `python -m scripts.socials.youtube.scrape --channel bravo --keywords Bravo --start <window-start> --end <window-end> --max-results 5`

## Threads, Twitter/X, And Facebook Remote Auth Smoke Checks

Use these checks when validating remote auth readiness for the remaining
auth-required shared-account platforms. They do not add worker lanes for
Twitter/X or Facebook; those platforms still use the shared-account catalog
path unless a separate plan adds a claimed-job lane.

List the commands without requiring Modal credentials:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
python scripts/socials/smoke_threads_twitter_facebook_auth.py --dry-run
```

Run the probes from an environment with Modal access and named social secrets:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
python scripts/socials/smoke_threads_twitter_facebook_auth.py --run
```

Equivalent single-platform checks:

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
python scripts/modal/verify_modal_readiness.py --probe-remote-auth twitter --json
python scripts/modal/verify_modal_readiness.py --probe-remote-auth facebook --json
python scripts/modal/verify_modal_readiness.py --probe-remote-auth threads --json
```

Safe structure keys only indicate credential shape. They must never contain raw
cookie or token values:

- Twitter/X: `has_auth_token`, `has_ct0`, `has_bearer_token`, `has_twikit_credentials`
- Facebook: `has_c_user`, `has_xs`
- Threads: `has_sessionid`, `has_csrftoken`

Copyable recent-failure query:

```sql
select
  platform,
  stage,
  status,
  last_error_code,
  last_error_class,
  updated_at,
  left(coalesce(error_message, ''), 240) as error_message
from social.scrape_jobs
where platform in ('twitter', 'facebook', 'threads')
  and last_error_code in (
    'twitter_remote_auth_not_ready',
    'facebook_remote_auth_not_ready',
    'threads_remote_auth_not_ready',
    'threads_posts_scrapling_cancelled',
    'threads_posts_scrapling_final_read_degraded'
  )
order by updated_at desc
limit 50;
```

Key TikTok job metadata fields:

- `stage`, `platform`, and `account`: confirm the job is the expected
  `tiktok_posts_scrapling` lane and account.
- `stage_counters.posts` and `stage_counters.pages`: fetched post/page counts.
- `persist_counters.posts_upserted`: rows persisted through the current per-row
  post helper.
- `activity.phase` and `activity.last_progress_at`: terminal phase and last
  progress timestamp.
- `fetcher_runtime`: safe fetcher diagnostics such as proxy fingerprint,
  request count, warmup cookie names/count, and `sec_uid_resolved`.
- `runtime_metadata`: failure-specific fetch reason when the job fails or
  retries.

Key YouTube metadata fields:

- `retrieval_meta.error_code`, `retryable`, and `error_class`: terminal
  diagnostic for the posts scrape.
- `retrieval_meta.first_page_counts`: first-page videos/shorts counts used to
  identify empty-channel pages.
- `retrieval_meta.continuation_pages`,
  `retrieval_meta.continuation_failure_reason`, and
  `retrieval_meta.continuation_failure_count`: pagination coverage and failure
  signal.
- `retrieval_meta.scan_capped_reason`: bounded crawl stop reason such as
  `pre_window_cap`.
- `retrieval_meta.profile_snapshot` and `retrieval_meta.total_posts`: profile
  and total-post evidence carried into shared catalog progress.
- `persist_counters.posts_upserted`: persisted post count when present on the
  job/run metadata.

Error-code triage:

- `tiktok_posts_account_missing`: job config is invalid; do not retry until the
  launch payload includes an account.
- `tiktok_posts_auth_failed`: TikTok auth failed; run the remote auth probe and
  refresh/redeploy the TikTok cookie secret before retrying.
- `challenge_or_blocked`: TikTok challenged or blocked the fetch path; inspect
  cookie freshness, proxy posture, and fallback-chain metadata.
- `http_429`: TikTok rate limit; retry with lower concurrency or a longer delay.
- `tiktok_posts_fetch_failed` or `tiktok_posts_scrapling_failed`: inspect
  `runtime_metadata.fetch_reason` and `fetcher_runtime` before retrying.
- `tiktok_posts_scrapling_cancelled`: operator or control-plane cancellation;
  check `cancel_scope`, `job_status`, and `run_status` when those keys are
  present.
- `youtube_empty_channel_page`: YouTube returned no first-page videos or shorts
  with no lower-level error; retry is allowed, but inspect channel identity and
  parser/continuation behavior first.
- `youtube_continuation_fetch_failed`: continuation pagination failed; inspect
  `continuation_failure_reason` and avoid treating partial coverage as complete.

Batch upsert is not a live TikTok/YouTube contract in this docs/scripts pass.
Keep interpreting TikTok and YouTube post persistence through the existing
per-row helper semantics until the platform payloads have equivalence tests for
conflict columns, optional columns, assignment payloads, and returned row shape.

30-day failure audit:

```sql
select
  j.id::text as job_id,
  j.run_id::text as run_id,
  coalesce(nullif(j.platform, ''), nullif(j.metadata->>'platform', ''), r.config->>'platform') as platform,
  coalesce(nullif(to_jsonb(j)->>'stage', ''), nullif(j.metadata->>'stage', ''), j.job_type, 'unknown') as stage,
  j.status,
  coalesce(
    nullif(j.last_error_code, ''),
    nullif(j.metadata->>'error_code', ''),
    nullif(j.metadata->'retrieval_meta'->>'error_code', '')
  ) as error_code,
  coalesce(
    nullif(j.last_error_class, ''),
    nullif(j.metadata->>'error_class', ''),
    nullif(j.metadata->'retrieval_meta'->>'error_class', '')
  ) as error_class,
  j.metadata->'runtime_metadata' as runtime_metadata,
  j.metadata->'fetcher_runtime' as fetcher_runtime,
  j.metadata->'retrieval_meta' as retrieval_meta,
  j.metadata->'persist_counters' as persist_counters,
  j.error_message,
  j.created_at,
  j.updated_at
from social.scrape_jobs j
left join social.scrape_runs r on r.id = j.run_id
where coalesce(nullif(j.platform, ''), nullif(j.metadata->>'platform', ''), r.config->>'platform') in ('tiktok', 'youtube')
  and j.created_at >= now() - interval '30 days'
  and (
    j.status in ('failed', 'retrying', 'cancelled')
    or coalesce(
      nullif(j.last_error_code, ''),
      nullif(j.metadata->>'error_code', ''),
      nullif(j.metadata->'retrieval_meta'->>'error_code', '')
    ) is not null
  )
order by j.created_at desc
limit 200;
```

## Alert Contract

The social worker-health and catalog-progress payloads now expose additive
`alerts` arrays. Preserve them in logs and automation outputs; they are the
operator-facing reason codes for whether a run should continue.

Worker-health alert codes to watch:

- `dispatcher_not_ready`
- `dispatcher_heartbeat_stale`
- `auth_preflight_not_ready`
- `stale_running_jobs`
- `queue_wait_exceeded`
- `dispatch_blocked_jobs`
- `retry_loop_detected`

Catalog-progress alert codes to watch:

- `runtime_version_drift`
- `runtime_superseded`
- `no_eligible_worker_for_required_runtime`
- `failed_recovery_no_partitions_discovered`
- `frontier_lease_stale`
- `modal_capacity_wait_exceeded`
- `dispatch_blocked`
- `retry_loop_detected`
- `classify_backlog_after_scrape`

Interpretation:

- `runtime_version_drift`: one run observed more than one semantically distinct runtime identity. Equivalent labels that resolve to the same `modal_image` or `commit_sha` do not count as drift.
- `runtime_superseded`: a stale run was automatically replaced by a fresh run on the current worker runtime. Follow the `replacement_run_id` rather than the cancelled run.
- `no_eligible_worker_for_required_runtime`: no healthy worker currently matches the stale run's pinned runtime. This can appear while work is still queued, or after discovery completed and the recovery handoff still could not advance into catalog fetch. Manual remediation stays available in either state until a replacement run is clearly active.
- `failed_recovery_no_partitions_discovered`: history discovery completed, but the run still failed with `completion_gap_reason=fetch_incomplete` before any recovery fetch work materialized. Treat this as a recovery dead-end first, not as a generic incomplete fetch.
- `frontier_lease_stale`: a frontier lease heartbeat has expired or was not reclaimed correctly; do not assume the next retry is safe until ownership is reconciled.
- `modal_capacity_wait_exceeded`: jobs are dispatched but stuck pending in Modal longer than the configured threshold; treat this as capacity pressure, not a duplicate backfill.
- `classify_backlog_after_scrape`: scrape stages are complete but classify fanout has not drained in time; pause follow-up backfills until the backlog clears or the cause is understood.
- `dispatch_blocked` / `dispatch_blocked_jobs`: the control plane could not hand work to Modal; do not keep scheduling new runs.
- `retry_loop_detected`: the same frontier or job type is retrying repeatedly; investigate before the next launch.

Runtime authority rules:

- Modal-required catalog runs use fresh healthy Modal worker or dispatcher heartbeats as the authoritative runtime source.
- If no fresh healthy Modal heartbeat publishes `metadata.runtime_version`, the authoritative remote runtime is `unknown`, not the API process build.
- The terminal Instagram failure shape `history discovery complete -> no_partitions_discovered -> fetch_incomplete` should now end in one of two operator-visible outcomes: a semantically eligible worker claims the recovery/catalog-fetch work, or the stale run is superseded exactly once into a replacement run on the current runtime.
- Manual `Cancel + Requeue clean run` is fallback-only. The UI hides it only after auto-requeue has already produced a `replacement_run_id` with `auto_requeue_status=queued|running`.

One catalog run can legitimately fan out into multiple classify jobs. Multiple
queued classify jobs are downstream work from the same run, not proof that the
operator clicked the backfill button more than once.

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

## n8n Control Plane

`n8n` may trigger and poll catalog runs, but it is not the browser/runtime
owner. Keep Playwright session ownership inside TRR workers and use `n8n` only
for launch, polling, retry, and notification control.

Before calling the backend from `n8n`:

- use the credential workflow variants in `docs/automation/`
- authenticate with a bearer token minted from `TRR_INTERNAL_ADMIN_SHARED_SECRET`
  rather than sending the raw secret itself
- point `baseUrl` at the canonical backend admin host
- preserve backend response details for `SOCIAL_MODAL_DISPATCH_UNAVAILABLE`,
  `SOCIAL_MODAL_EXECUTOR_REQUIRED`, `SOCIAL_WORKER_UNAVAILABLE`, and auth
  preflight failures instead of masking them as generic workflow errors

Current repo-owned `n8n` status:

- checked-in templates exist and are reviewed as `launches-backfill`
  control-plane workflows
- they are not the browser/runtime owner and do not change Modal worker-plane
  readiness
- no live external `n8n` workflow instance is tracked in-repo, so a real `n8n`
  environment still needs a separate operational audit before it can be called
  ready

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
