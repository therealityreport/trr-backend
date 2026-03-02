begin;

create schema if not exists admin;

alter table if exists admin.entity_logo_imports
  drop constraint if exists entity_logo_imports_target_type_check;
alter table if exists admin.entity_logo_imports
  add constraint entity_logo_imports_target_type_check
  check (
    target_type in (
      'show',
      'network',
      'streaming',
      'production',
      'franchise',
      'publication',
      'social',
      'other'
    )
  );

create table if not exists admin.brand_logo_assets (
  id uuid primary key default gen_random_uuid(),
  target_type text not null
    check (target_type in ('franchise', 'publication', 'social', 'other')),
  target_key text not null,
  target_label text not null,
  source_url text not null,
  source_page_url text,
  source_domain text,
  source_rank int not null default 0,
  run_id text,
  hosted_logo_key text,
  hosted_logo_url text,
  hosted_logo_sha256 text,
  hosted_logo_content_type text,
  hosted_logo_bytes bigint,
  hosted_logo_etag text,
  hosted_logo_at timestamptz,
  hosted_logo_black_key text,
  hosted_logo_black_url text,
  hosted_logo_black_sha256 text,
  hosted_logo_black_content_type text,
  hosted_logo_black_bytes bigint,
  hosted_logo_black_etag text,
  hosted_logo_black_at timestamptz,
  hosted_logo_white_key text,
  hosted_logo_white_url text,
  hosted_logo_white_sha256 text,
  hosted_logo_white_content_type text,
  hosted_logo_white_bytes bigint,
  hosted_logo_white_etag text,
  hosted_logo_white_at timestamptz,
  base_logo_format text not null default 'unknown'
    check (base_logo_format in ('png', 'svg', 'webp', 'jpg', 'unknown')),
  mirror_status text not null check (mirror_status in ('mirrored', 'skipped', 'failed')),
  failure_reason text,
  is_primary boolean not null default false,
  metadata jsonb,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (target_type, target_key, source_url)
);

create index if not exists brand_logo_assets_target_idx
  on admin.brand_logo_assets (target_type, target_key, is_primary desc, updated_at desc);

create index if not exists brand_logo_assets_sha_idx
  on admin.brand_logo_assets (hosted_logo_sha256);

grant usage on schema admin to service_role;
grant all privileges on table admin.brand_logo_assets to service_role;

grant all privileges on table admin.entity_logo_imports to service_role;

commit;
