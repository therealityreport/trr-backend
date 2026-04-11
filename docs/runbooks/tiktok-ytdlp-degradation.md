# TikTok yt-dlp Degradation Runbook

## What to watch

- `GET /api/v1/admin/socials/ingest/health-dot`
- `GET /api/v1/admin/socials/live-status`
- Operational visibility note: this alert is currently confirmed only on the admin endpoint surfaces above; there is no verified Slack/PagerDuty-style escalation path wired yet, so schedule that separately if humans need push-based notification.
- Queue-status `alerts` with:
  - `tiktok_single_path_risk`
  - `tiktok_single_path_degraded`
- `queue.recent_failures` rows where `platform == "tiktok"` and the error code or message mentions `yt-dlp`
- Retry loops or repeated `failed` / `retrying` TikTok jobs in the ingest queue

## What the alerts mean

- `tiktok_single_path_risk` is a warning that TikTok posts currently have one proven live path: yt-dlp.
- `tiktok_single_path_degraded` is a critical signal that recent TikTok queue failures point at the primary yt-dlp path.

## What breaks when yt-dlp degrades

- TikTok post ingestion becomes unreliable or stops advancing.
- Shared-account backfills that depend on the TikTok post path can stall behind retries.
- Health-dot and live-status continue to show the issue, but there is no alternate proven live browser-intercept fallback in this alert model.

## Operator response

1. Confirm the alert code and inspect the most recent TikTok failures.
2. Verify whether the failure is yt-dlp-specific rather than a broader queue or auth problem.
3. If the queue is still making progress, keep the incident in observation and watch for recovery.
4. If failures continue, treat TikTok ingestion as degraded until the primary path stabilizes.

## Non-goals

- Do not reopen request signing here.
- Do not reopen `curl_cffi` plus proxy work here.
- Do not expand this runbook into a new TikTok transport subsystem.
