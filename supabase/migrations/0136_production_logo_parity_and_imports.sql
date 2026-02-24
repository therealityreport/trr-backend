begin;

create schema if not exists admin;

alter table if exists core.production_companies
  add column if not exists wikidata_id text,
  add column if not exists wikipedia_url text,
  add column if not exists wikimedia_logo_file text,
  add column if not exists link_enriched_at timestamptz,
  add column if not exists link_enrichment_source text,
  add column if not exists hosted_logo_black_key text,
  add column if not exists hosted_logo_black_url text,
  add column if not exists hosted_logo_black_sha256 text,
  add column if not exists hosted_logo_black_content_type text,
  add column if not exists hosted_logo_black_bytes bigint,
  add column if not exists hosted_logo_black_etag text,
  add column if not exists hosted_logo_black_at timestamptz,
  add column if not exists hosted_logo_white_key text,
  add column if not exists hosted_logo_white_url text,
  add column if not exists hosted_logo_white_sha256 text,
  add column if not exists hosted_logo_white_content_type text,
  add column if not exists hosted_logo_white_bytes bigint,
  add column if not exists hosted_logo_white_etag text,
  add column if not exists hosted_logo_white_at timestamptz;

alter table if exists admin.network_streaming_overrides
  drop constraint if exists network_streaming_overrides_entity_type_check;
alter table if exists admin.network_streaming_overrides
  add constraint network_streaming_overrides_entity_type_check
  check (entity_type in ('network', 'streaming', 'production'));

alter table if exists admin.network_streaming_completion
  drop constraint if exists network_streaming_completion_entity_type_check;
alter table if exists admin.network_streaming_completion
  add constraint network_streaming_completion_entity_type_check
  check (entity_type in ('network', 'streaming', 'production'));

alter table if exists admin.network_streaming_completion_attempts
  drop constraint if exists network_streaming_completion_attempts_entity_type_check;
alter table if exists admin.network_streaming_completion_attempts
  add constraint network_streaming_completion_attempts_entity_type_check
  check (entity_type in ('network', 'streaming', 'production'));

alter table if exists admin.network_streaming_logo_assets
  drop constraint if exists network_streaming_logo_assets_entity_type_check;
alter table if exists admin.network_streaming_logo_assets
  add constraint network_streaming_logo_assets_entity_type_check
  check (entity_type in ('network', 'streaming', 'production'));

create table if not exists admin.entity_logo_imports (
  id uuid primary key default gen_random_uuid(),
  target_type text not null check (target_type in ('show', 'network', 'streaming', 'production')),
  target_id text not null,
  target_key text,
  source_type text not null check (source_type in ('url', 'file')),
  source_url text,
  uploaded_filename text,
  hosted_logo_url text,
  hosted_logo_sha256 text,
  status text not null check (status in ('imported', 'skipped', 'failed')),
  failure_reason text,
  created_by text,
  created_at timestamptz not null default now()
);

create index if not exists entity_logo_imports_target_idx
  on admin.entity_logo_imports (target_type, target_id, created_at desc);

grant usage on schema admin to service_role;
grant all privileges on table admin.entity_logo_imports to service_role;

commit;
