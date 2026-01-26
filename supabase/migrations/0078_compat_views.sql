begin;

create or replace view core.v_show_images_served_media_v2 as
select
  ml.id,
  ml.entity_id as show_id,
  coalesce((ml.context->>'tmdb_id')::int, s.tmdb_id) as tmdb_id,
  s.name as show_name,
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
  coalesce(ma.source_url, ma.hosted_url) as url_original,
  ma.hosted_url,
  ml.position,
  ma.fetched_at,
  ma.created_at,
  ma.updated_at,
  coalesce(ma.hosted_url, ma.source_url) as served_url
from core.media_links ml
join core.media_assets ma on ml.media_asset_id = ma.id
left join core.shows s on s.id = ml.entity_id
where ml.entity_type = 'show';

grant select on core.v_show_images_served_media_v2 to anon, authenticated, service_role;

create or replace view core.v_season_images_served_media_v2 as
select
  ml.id,
  s.show_id,
  ml.entity_id as season_id,
  (ml.context->>'tmdb_series_id')::int as tmdb_series_id,
  (ml.context->>'season_number')::int as season_number,
  ma.source,
  ma.source_asset_id as source_image_id,
  ml.kind,
  (ml.context->>'iso_639_1')::text as iso_639_1,
  (ml.context->>'file_path')::text as file_path,
  ma.width,
  ma.height,
  (ma.width::numeric / nullif(ma.height, 0)) as aspect_ratio,
  ma.caption,
  ma.source_url as url,
  coalesce(ma.source_url, ma.hosted_url) as url_original,
  ma.hosted_url,
  ml.position,
  ma.fetched_at,
  ma.created_at,
  ma.updated_at,
  coalesce(ma.hosted_url, ma.source_url) as served_url
from core.media_links ml
join core.media_assets ma on ml.media_asset_id = ma.id
left join core.seasons s on s.id = ml.entity_id
where ml.entity_type = 'season';

grant select on core.v_season_images_served_media_v2 to anon, authenticated, service_role;

create or replace view core.v_episode_images_served_media_v2 as
select
  ml.id,
  e.show_id,
  e.season_id,
  ml.entity_id as episode_id,
  (ml.context->>'tmdb_series_id')::int as tmdb_series_id,
  (ml.context->>'season_number')::int as season_number,
  (ml.context->>'episode_number')::int as episode_number,
  ma.source,
  ma.source_asset_id as source_image_id,
  ml.kind,
  (ml.context->>'iso_639_1')::text as iso_639_1,
  (ml.context->>'file_path')::text as file_path,
  ma.width,
  ma.height,
  (ma.width::numeric / nullif(ma.height, 0)) as aspect_ratio,
  ma.caption,
  ma.source_url as url,
  coalesce(ma.source_url, ma.hosted_url) as url_original,
  ma.hosted_url,
  ml.position,
  ma.fetched_at,
  ma.created_at,
  ma.updated_at,
  coalesce(ma.hosted_url, ma.source_url) as served_url
from core.media_links ml
join core.media_assets ma on ml.media_asset_id = ma.id
left join core.episodes e on e.id = ml.entity_id
where ml.entity_type = 'episode';

grant select on core.v_episode_images_served_media_v2 to anon, authenticated, service_role;

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
  coalesce(ma.hosted_url, ma.source_url) as served_url
from core.media_links ml
join core.media_assets ma on ml.media_asset_id = ma.id
where ml.entity_type = 'person';

grant select on core.v_person_images_served_media_v2 to anon, authenticated, service_role;

commit;
