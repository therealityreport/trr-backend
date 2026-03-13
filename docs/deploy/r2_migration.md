# Cloudflare R2 Migration

For bucket naming, token choices, public/private access, and the canonical
runtime env contract, start with
[`R2-setup.md`](/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/deploy/R2-setup.md).

Use the S3-compatible migration scripts after the R2 buckets and public/custom
domain are already configured.

## Required Inputs

```bash
export R2_ENDPOINT_URL=https://<accountid>.r2.cloudflarestorage.com
export R2_ACCESS_KEY_ID=...
export R2_SECRET_ACCESS_KEY=...
export R2_PUBLIC_BASE_URL=https://media.thereality.report
```

## Sync A Bucket

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
python3.11 scripts/storage/sync_bucket_to_r2.py \
  --source-bucket trr-backend \
  --destination-bucket trr-backend \
  --destination-endpoint-url "$R2_ENDPOINT_URL" \
  --destination-access-key-id "$R2_ACCESS_KEY_ID" \
  --destination-secret-access-key "$R2_SECRET_ACCESS_KEY" \
  --skip-existing \
  --json
```

## Verify Counts And Bytes

```bash
cd /Users/thomashulihan/Projects/TRR/TRR-Backend
python3.11 scripts/storage/verify_bucket_sync.py \
  --source-bucket trr-backend \
  --destination-bucket trr-backend \
  --destination-endpoint-url "$R2_ENDPOINT_URL" \
  --destination-access-key-id "$R2_ACCESS_KEY_ID" \
  --destination-secret-access-key "$R2_SECRET_ACCESS_KEY" \
  --json
```

Repeat for:

- `screenalytics`
- `ltsr-data-bucket`

## Cutover Order

1. Sync the AWS bucket contents into the matching R2 bucket.
2. Verify object counts and total bytes match.
3. Set `OBJECT_STORAGE_*` on Render and in the Modal runtime secret.
4. Validate one hosted asset read, one upload, one mirror path, and one manifest write.
5. Keep AWS S3 read-only during the rollback window.
6. Delete the AWS buckets only after rollback completes cleanly.
