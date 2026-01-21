begin;

-- ---------------------------------------------------------------------------
-- Compatibility views backed by core.media_assets + core.media_links
-- ---------------------------------------------------------------------------

create or replace view core.v_show_images_served_media as
select
  ml.id,
  ml.entity_id as show_id,
  ma.source,
  ma.source_asset_id as source_image_id,
  ml.kind,
  (ml.context->>'iso_639_1')::text as iso_639_1,
  (ml.context->>'file_path')::text as file_path,
  ma.width,
  ma.height,
  (ma.width::numeric / nullif(ma.height, 0)) as aspect_ratio,
  (ma.metadata->>'vote_average')::numeric as vote_average,
  (ma.metadata->>'vote_count')::integer as vote_count,
  ma.caption,
  ma.source_url as url,
  ma.hosted_url,
  ml.position,
  ma.created_at,
  ma.updated_at,
  coalesce(ma.hosted_url, ma.source_url) as served_url
from core.media_links ml
join core.media_assets ma on ml.media_asset_id = ma.id
where ml.entity_type = 'show';

grant select on core.v_show_images_served_media to anon, authenticated, service_role;

create or replace view core.v_person_images_served_media as
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
  ma.hosted_url,
  ml.position,
  ma.created_at,
  ma.updated_at,
  coalesce(ma.hosted_url, ma.source_url) as served_url
from core.media_links ml
join core.media_assets ma on ml.media_asset_id = ma.id
where ml.entity_type = 'person';

grant select on core.v_person_images_served_media to anon, authenticated, service_role;

commit;
