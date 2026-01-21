begin;

-- ---------------------------------------------------------------------------
-- core.media_assets - Unified media store (images, videos, etc.)
-- ---------------------------------------------------------------------------

create table if not exists core.media_assets (
  id uuid primary key default gen_random_uuid(),

  -- Core identity
  media_type text not null default 'image',
  source text not null,
  source_asset_id text null,
  source_url text null,

  -- Content deduplication
  sha256 text null,

  -- Basic metadata
  content_type text null,
  bytes bigint null,
  width integer null,
  height integer null,

  -- Descriptive metadata
  caption text null,
  alt_text text null,

  -- S3 hosting
  hosted_bucket text null,
  hosted_key text null,
  hosted_url text null,
  hosted_etag text null,
  hosted_at timestamptz null,
  hosted_sha256 text null,
  hosted_content_type text null,
  hosted_bytes bigint null,

  -- Source-specific metadata
  metadata jsonb not null default '{}'::jsonb,

  -- Timestamps
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  fetched_at timestamptz null
);

-- Deduplication constraints
create unique index if not exists media_assets_source_asset_id_unique
  on core.media_assets (source, source_asset_id)
  where source_asset_id is not null;

create unique index if not exists media_assets_source_url_unique
  on core.media_assets (source, source_url)
  where source_url is not null;

create unique index if not exists media_assets_sha256_unique
  on core.media_assets (sha256)
  where sha256 is not null;

-- Indexes
create index if not exists media_assets_source_idx
  on core.media_assets (source);

create index if not exists media_assets_sha256_idx
  on core.media_assets (sha256)
  where sha256 is not null;

create index if not exists media_assets_hosted_url_idx
  on core.media_assets (hosted_url)
  where hosted_url is not null;

create index if not exists media_assets_metadata_idx
  on core.media_assets using gin (metadata);

-- updated_at trigger
create trigger core_media_assets_set_updated_at
before update on core.media_assets
for each row
execute function core.set_updated_at();

commit;
