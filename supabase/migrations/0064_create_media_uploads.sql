begin;

-- ---------------------------------------------------------------------------
-- core.media_uploads - Upload session table for safe presigned URL flow
-- ---------------------------------------------------------------------------
-- This table tracks upload sessions for user-uploaded media. It enables:
-- - Safe presigned upload permissions
-- - Deterministic finalization
-- - Expiration/cancellation handling
-- - Audit trail (who uploaded what)
--
-- Security: Admin/service_role only for v1. RLS enabled with explicit deny-all
-- policy to prevent accidental access via authenticated key.
-- ---------------------------------------------------------------------------

create table if not exists core.media_uploads (
  id uuid primary key default gen_random_uuid(),

  -- Who requested the upload (nullable for service-role initiated)
  uploader_user_id uuid null,

  -- Target entity
  entity_type text not null,  -- 'show'|'season'|'episode'|'person'
  entity_id uuid not null,
  kind text not null,         -- 'poster'|'backdrop'|'logo'|'profile'|'still'|'gallery'

  -- Upload intent / metadata
  original_filename text null,
  content_type text not null,
  expected_bytes bigint null,
  caption text null,
  alt_text text null,

  -- Whether to make this the primary image for entity+kind
  make_primary boolean not null default false,

  -- Lifecycle
  status text not null default 'initiated'
    check (status in ('initiated', 'uploaded', 'finalized', 'failed', 'expired', 'canceled')),
  error text null,
  expires_at timestamptz not null default (now() + interval '1 hour'),

  -- Temp S3 location
  s3_bucket text not null,
  s3_temp_key text not null,

  -- Result links (populated after finalize)
  media_asset_id uuid null references core.media_assets(id) on delete set null,
  media_link_id uuid null references core.media_links(id) on delete set null,

  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

-- Index for finding uploads by entity
create index if not exists media_uploads_entity_idx
  on core.media_uploads (entity_type, entity_id, kind);

-- Index for cleanup queries (finding expired sessions)
create index if not exists media_uploads_status_idx
  on core.media_uploads (status, expires_at);

-- Index for finding uploads by uploader
create index if not exists media_uploads_uploader_idx
  on core.media_uploads (uploader_user_id)
  where uploader_user_id is not null;

-- Auto-update updated_at on changes
create trigger core_media_uploads_set_updated_at
before update on core.media_uploads
for each row execute function core.set_updated_at();

-- ---------------------------------------------------------------------------
-- Security: admin/service_role only for v1
-- ---------------------------------------------------------------------------
-- DO NOT grant to anon or authenticated - uploads are admin-only
-- Enable RLS with explicit deny-all policy for non-service-role
-- (service_role bypasses RLS by default)
-- ---------------------------------------------------------------------------

alter table core.media_uploads enable row level security;

-- Explicit deny-all for non-service-role
-- This prevents accidental access if someone uses authenticated key by mistake
create policy "deny_all_non_service_role" on core.media_uploads
  for all using (false);

-- ---------------------------------------------------------------------------
-- For v2 (authenticated user uploads), replace with policies like:
-- ---------------------------------------------------------------------------
-- drop policy "deny_all_non_service_role" on core.media_uploads;
-- create policy "users_view_own_uploads" on core.media_uploads
--   for select using (uploader_user_id = auth.uid());
-- create policy "users_insert_own_uploads" on core.media_uploads
--   for insert with check (uploader_user_id = auth.uid());
-- ---------------------------------------------------------------------------

commit;
