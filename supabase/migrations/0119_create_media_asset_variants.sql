-- Persist generated display/crop image variants for media assets.

create table if not exists core.media_asset_variants (
  id uuid primary key default gen_random_uuid(),
  media_asset_id uuid not null references core.media_assets(id) on delete cascade,
  variant_key text not null,
  format text not null,
  width integer,
  height integer,
  bytes bigint,
  hosted_bucket text not null,
  hosted_key text not null,
  hosted_url text not null,
  crop_mode text,
  crop_x numeric,
  crop_y numeric,
  crop_zoom numeric,
  crop_signature text not null default 'base',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create unique index if not exists media_asset_variants_asset_variant_format_crop_uq
  on core.media_asset_variants (media_asset_id, variant_key, format, crop_signature);

create unique index if not exists media_asset_variants_hosted_key_uq
  on core.media_asset_variants (hosted_key);

create index if not exists media_asset_variants_asset_idx
  on core.media_asset_variants (media_asset_id);

create index if not exists media_asset_variants_crop_signature_idx
  on core.media_asset_variants (crop_signature);

-- Keep updated_at current on upserts/updates.
create or replace function core.touch_media_asset_variants_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

drop trigger if exists trg_touch_media_asset_variants_updated_at on core.media_asset_variants;
create trigger trg_touch_media_asset_variants_updated_at
before update on core.media_asset_variants
for each row execute function core.touch_media_asset_variants_updated_at();
