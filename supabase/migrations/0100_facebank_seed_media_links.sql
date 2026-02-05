begin;

-- Add facebank_seed flag to media_links (person gallery images)
alter table core.media_links
  add column if not exists facebank_seed boolean not null default false;

-- Optional index to speed up filtered reads
create index if not exists media_links_facebank_seed_idx
  on core.media_links (entity_type, entity_id, kind, facebank_seed);

-- Update Screenalytics view to expose facebank_seed
create or replace view core.v_person_images as
select
    ml.id,
    ml.entity_id as person_id,
    ma.id as media_asset_id,
    ma.source,
    ma.source_asset_id,
    ml.kind,
    ma.width,
    ma.height,
    ma.hosted_key,
    ma.hosted_url,
    ml.is_primary,
    ml.position,
    coalesce(ma.hosted_url, ma.source_url) as served_url,
    ma.created_at,
    ma.updated_at,
    ml.facebank_seed
from core.media_links ml
join core.media_assets ma on ml.media_asset_id = ma.id
where ml.entity_type = 'person';

comment on view core.v_person_images is
'Person images for Screenalytics facebank seeding.
Includes width, height, hosted_key, is_primary, facebank_seed.
Joins media_links (entity_type=person) to media_assets.';

-- service_role only (Screenalytics backend access)
grant select on core.v_person_images to service_role;

-- Update served media view for app usage
create or replace view core.v_person_images_served_media_v2 as
select
  ml.id,
  ml.entity_id as person_id,
  ma.source,
  ma.source_asset_id as source_image_id,
  ml.kind,
  ma.width,
  ma.height,
  ma.caption,
  ml.is_primary,
  ma.source_url as url,
  coalesce(ma.source_url, ma.hosted_url) as url_original,
  ma.hosted_url,
  ml.position,
  ma.fetched_at,
  ma.created_at,
  ma.updated_at,
  coalesce(ma.hosted_url, ma.source_url) as served_url,
  ml.facebank_seed
from core.media_links ml
join core.media_assets ma on ml.media_asset_id = ma.id
where ml.entity_type = 'person';

grant select on core.v_person_images_served_media_v2 to anon, authenticated, service_role;

commit;
