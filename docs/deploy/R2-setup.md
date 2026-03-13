# Cloudflare R2 Setup

Use this document as the canonical operator reference for the TRR R2 setup.
Keep AWS S3 live during migration. Do not delete AWS buckets until R2 is
verified, writes have cut over, and the rollback window has elapsed.

## Recommended Bucket Layout

Create these buckets in Cloudflare R2:

- `trr-media-prod`
  - Public bucket for application media and hosted assets.
  - Intended custom domain: `media.thereality.report`
  - Use for the current `trr-backend` AWS bucket migration target.
- `screenalytics-artifacts-prod`
  - Private bucket for screenalytics artifacts, uploads, and generated files.
  - No public domain.
- `ltsr-archive-prod`
  - Private archival bucket for the current `ltsr-data-bucket` contents.
  - No public domain.

Bucket names must remain lowercase and use only letters, numbers, and hyphens.

## Migration Policy

- Keep existing AWS S3 and CloudFront URLs working during migration.
- Sync AWS bucket contents into the matching R2 bucket first.
- Verify object counts and bytes match before changing app env.
- Cut new writes to R2 before rewriting old hosted URLs.
- Keep AWS buckets read-only during the rollback window.
- Delete AWS buckets only after verification and rollback expiration.

## Public And Private Access

Use this access model:

- `trr-media-prod`
  - Public reads through a custom domain.
  - Do not rely on `r2.dev` in production.
  - Presigned uploads should still use the R2 S3-compatible endpoint, not the
    public custom domain.
- `screenalytics-artifacts-prod`
  - Private only.
  - Accessed by signed requests or direct backend credentials.
- `ltsr-archive-prod`
  - Private only.
  - Treat as archive storage, not an application read path.

## API Token Strategy

Prefer the R2-native token flow:

1. In Cloudflare, open `R2 Object Storage`.
2. Select `Manage API tokens`.
3. Choose `Create Account API token`.

Use an account token, not a user token, for the shared runtime credentials used
by Render and Modal.

Important:

- The generic Cloudflare bearer token created from `My Profile > API Tokens >
  Create Custom Token` is fine for management API calls such as bucket
  creation.
- It is not the preferred runtime credential for the app's S3-compatible boto3
  access.
- Render and Modal should use the Access Key ID and Secret Access Key created
  from the R2-native `Manage API tokens` flow.

### Runtime Token

Create one long-lived runtime token with:

- Name: `trr-r2-runtime`
- Permission: `Object Read & Write`
- Scope: `Apply to specific buckets only`
- Buckets:
  - `trr-media-prod`
  - `screenalytics-artifacts-prod`
  - add `ltsr-archive-prod` only if the live app needs write access there
- Client IP filtering: leave blank

Use this token for:

- Render API runtime
- Modal job runtime
- presigned upload generation
- object reads/writes through the S3-compatible API

### Migration/Admin Token

Create a second short-lived token only if operator automation must create,
delete, or reconfigure buckets:

- Name: `trr-r2-migration-admin`
- Permission: `Admin Read & Write`
- Scope: specific buckets if possible, otherwise account-level for the shortest
  possible time window
- Revoke it after migration is complete

Do not use the admin token as the day-to-day runtime credential.

## If You Are On The Generic Cloudflare Token Screen

If you create a token from `My Profile > API Tokens > Create Custom Token`,
choose:

- First dropdown: `Account`
- Permission group: `Workers R2 Storage`
- Permission level: `Edit`

Then continue to scope the token to specific buckets if the UI exposes that
option. If you do not see bucket scoping there, back out and use the R2-native
`Manage API tokens` flow instead. That flow is the preferred one for R2.

## Required App Environment Variables

Use these canonical runtime envs after cutover:

```bash
OBJECT_STORAGE_PROVIDER=r2
OBJECT_STORAGE_BUCKET=trr-media-prod
OBJECT_STORAGE_REGION=auto
OBJECT_STORAGE_ENDPOINT_URL=https://<ACCOUNT_ID>.r2.cloudflarestorage.com
OBJECT_STORAGE_ACCESS_KEY_ID=<Access Key ID>
OBJECT_STORAGE_SECRET_ACCESS_KEY=<Secret Access Key>
OBJECT_STORAGE_PUBLIC_BASE_URL=https://media.thereality.report
```

Optional:

```bash
OBJECT_STORAGE_PREFIX=
```

Compatibility aliases already supported in code:

- `AWS_S3_BUCKET` -> `OBJECT_STORAGE_BUCKET`
- `AWS_CDN_BASE_URL` -> `OBJECT_STORAGE_PUBLIC_BASE_URL`

## Custom Domain

For `trr-media-prod`:

- Attach `media.thereality.report` as the public/custom domain.
- Use `https://media.thereality.report` as
  `OBJECT_STORAGE_PUBLIC_BASE_URL`.
- Keep object keys unchanged during migration.

## Endpoint Reference

Use the account endpoint for S3-compatible clients:

```text
https://<ACCOUNT_ID>.r2.cloudflarestorage.com
```

If jurisdictional buckets are used later, switch to the jurisdiction-specific
endpoint for those buckets only.

## Operational Checklist

1. Create the three R2 buckets.
2. Create the `trr-r2-runtime` account token with `Object Read & Write`.
3. Record:
   - `Access Key ID`
   - `Secret Access Key`
   - `ACCOUNT_ID`
   - public custom domain
4. Configure `media.thereality.report` for `trr-media-prod`.
5. Sync AWS buckets to R2 with the existing migration scripts.
6. Verify counts and bytes match.
7. Set `OBJECT_STORAGE_*` in Render and Modal.
8. Validate one hosted asset read, one upload, one mirror path, and one
   manifest write.
9. Freeze AWS buckets to read-only during rollback.
10. Delete AWS buckets only after rollback completes.

## Related Docs

- [r2_migration.md](/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/deploy/r2_migration.md)
- [render.md](/Users/thomashulihan/Projects/TRR/TRR-Backend/docs/deploy/render.md)
