# Media Mirror Alerts

Use this when Instagram media-mirror failures start accumulating faster than the retry worker can clear them.

## What To Watch

- Active `media_mirror` jobs stuck in `queued`, `retrying`, or `running`
- Failed Instagram `media_mirror` jobs whose normalized reason is retryable:
  - `request_timeout`
  - `connection_error`
  - `request_error`
  - `http_429`
  - `http_500`
  - `http_502`
  - `http_503`
  - `http_504`
- Failed Instagram `media_mirror` jobs whose normalized reason is non-retryable:
  - `http_403_auth_or_expired`
  - `http_404_not_found`
  - `invalid_source_url`
  - `asset_too_large`
  - `asset_wrong_content_type`

## Operator Flow

1. Run the duplicate-job dry runs first:
   - `python -m scripts.socials.retire_duplicate_instagram_media_mirror_jobs --dry-run`
   - `python -m scripts.socials.retire_duplicate_instagram_comment_media_mirror_jobs --dry-run`
2. If the failed backlog is dominated by permanent Instagram media errors, preview retirement:
   - `python -m scripts.socials.retire_stale_instagram_media_mirror_failures --dry-run`
3. Review the preview job IDs and confirm the failures are truly permanent.
4. Apply retirement only after review:
   - `python -m scripts.socials.retire_stale_instagram_media_mirror_failures --apply`
5. Re-run the dry run to confirm the stale non-retryable backlog is gone.

## Guardrails

- These scripts retire obsolete queue rows by marking them `cancelled`; they do not delete history.
- Run cleanup previews before adding or validating active-job uniqueness indexes.
- If you see signed CDN URLs in logs or operator output, stop and fix redaction before sharing artifacts.
