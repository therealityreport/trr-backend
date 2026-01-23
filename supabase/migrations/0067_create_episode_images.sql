-- Episode images table for storing TMDb episode stills
-- Follows the pattern established by season_images (0013, 0051, 0052)

begin;

-- Drop table if exists for clean re-run (CASCADE handles dependent objects)
drop table if exists core.episode_images cascade;

create table core.episode_images (
  id uuid primary key default gen_random_uuid(),
  show_id uuid not null references core.shows(id) on delete cascade,
  season_id uuid not null references core.seasons(id) on delete cascade,
  episode_id uuid not null references core.episodes(id) on delete cascade,
  tmdb_series_id integer not null,
  season_number integer not null,
  episode_number integer not null,
  source text not null default 'tmdb',
  kind text not null default 'still',
  iso_639_1 text,
  file_path text not null,
  url text not null,
  url_original text generated always as ('https://image.tmdb.org/t/p/original' || file_path) stored,
  source_image_id text not null,
  width integer not null,
  height integer not null,
  aspect_ratio numeric not null,
  caption text,
  position integer,
  metadata jsonb not null default '{}'::jsonb,
  fetch_method text,
  fetched_from_url text,
  fetched_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  -- Hosted fields for S3 mirroring
  hosted_bucket text,
  hosted_key text,
  hosted_url text,
  hosted_sha256 text,
  hosted_content_type text,
  hosted_bytes bigint,
  hosted_etag text,
  hosted_at timestamptz
);

-- Unique constraint for deduplication (episode + source + source_image_id)
create unique index if not exists episode_images_episode_source_image_unique
  on core.episode_images (episode_id, source, source_image_id);

-- Alternative unique constraint matching season_images pattern
create unique index if not exists episode_images_tmdb_episode_source_file_unique
  on core.episode_images (tmdb_series_id, season_number, episode_number, source, file_path);

-- Indexes for efficient queries
create index if not exists episode_images_episode_id_idx
  on core.episode_images (episode_id);

create index if not exists episode_images_show_id_idx
  on core.episode_images (show_id);

create index if not exists episode_images_season_id_idx
  on core.episode_images (season_id);

create index if not exists episode_images_tmdb_series_season_episode_idx
  on core.episode_images (tmdb_series_id, season_number, episode_number);

create index if not exists episode_images_source_image_id_idx
  on core.episode_images (source, source_image_id)
  where source_image_id is not null;

-- Index for metadata queries (tags, people)
create index if not exists episode_images_metadata_idx
  on core.episode_images using gin (metadata);

-- Index for fetch_method filtering
create index if not exists episode_images_fetch_method_idx
  on core.episode_images (fetch_method);

-- Partial index for S3 backfill queries (images missing hosted_url)
create index if not exists episode_images_missing_hosted_idx
  on core.episode_images (id)
  where hosted_url is null;

-- Partial index for hosted_at not null (already mirrored)
create index if not exists episode_images_hosted_at_idx
  on core.episode_images (hosted_at)
  where hosted_at is not null;

-- Partial index for hosted_sha256 (dedup checks)
create index if not exists episode_images_hosted_sha256_idx
  on core.episode_images (hosted_sha256)
  where hosted_sha256 is not null;

-- Grants for API access
grant select on table core.episode_images to anon, authenticated;
grant all privileges on table core.episode_images to service_role;

-- Enable RLS
alter table core.episode_images enable row level security;

-- Public read policy
create policy core_episode_images_public_read on core.episode_images
  for select to anon, authenticated using (true);

-- Updated_at trigger
create trigger core_episode_images_set_updated_at
  before update on core.episode_images
  for each row
  execute function core.set_updated_at();

commit;
