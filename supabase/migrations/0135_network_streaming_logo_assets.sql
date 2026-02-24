begin;

create schema if not exists admin;

create table if not exists admin.network_streaming_logo_assets (
  id uuid primary key default gen_random_uuid(),
  entity_type text not null check (entity_type in ('network', 'streaming')),
  entity_key text not null,
  entity_id text,
  display_name text not null,
  source text not null check (source in ('override', 'tmdb', 'wikimedia', 'official', 'catalog', 'imdb')),
  source_url text not null,
  source_rank int not null default 0,
  run_id text,
  hosted_logo_key text,
  hosted_logo_url text,
  hosted_logo_sha256 text,
  hosted_logo_content_type text,
  hosted_logo_bytes bigint,
  hosted_logo_etag text,
  base_logo_format text not null default 'unknown'
    check (base_logo_format in ('png', 'svg', 'webp', 'jpg', 'unknown')),
  pixel_width int,
  pixel_height int,
  mirror_status text not null check (mirror_status in ('mirrored', 'skipped', 'failed')),
  failure_reason text,
  is_primary boolean not null default false,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (entity_type, entity_key, source, source_url)
);

create index if not exists network_streaming_logo_assets_entity_idx
  on admin.network_streaming_logo_assets (entity_type, entity_key, is_primary desc, source_rank asc);

create index if not exists network_streaming_logo_assets_sha_idx
  on admin.network_streaming_logo_assets (hosted_logo_sha256);

create index if not exists network_streaming_logo_assets_run_idx
  on admin.network_streaming_logo_assets (run_id);

grant usage on schema admin to service_role;
grant all privileges on table admin.network_streaming_logo_assets to service_role;

commit;
