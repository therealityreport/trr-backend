begin;

-- ---------------------------------------------------------------------------
-- show_images -> media_assets
-- ---------------------------------------------------------------------------
insert into core.media_assets (
  media_type, source, source_asset_id, source_url,
  content_type, bytes, width, height, caption,
  hosted_bucket, hosted_key, hosted_url, hosted_etag, hosted_at, hosted_sha256,
  hosted_content_type, hosted_bytes,
  metadata, fetched_at,
  created_at, updated_at
)
select 'image', source, source_image_id, coalesce(url_original, url),
       hosted_content_type, hosted_bytes, width, height, caption,
       hosted_bucket, hosted_key, hosted_url, hosted_etag, hosted_at, hosted_sha256,
       hosted_content_type, hosted_bytes,
       coalesce(metadata, '{}'::jsonb), fetched_at,
       created_at, updated_at
from core.show_images
on conflict do nothing;

insert into core.media_links (entity_type, entity_id, media_asset_id, kind, position, context, created_at, updated_at)
select 'show', s.show_id, a.id, s.kind, s.position,
       jsonb_build_object(
         'legacy_table','show_images',
         'legacy_id', s.id,
         'file_path', s.file_path,
         'url_path', s.url_path,
         'iso_639_1', s.iso_639_1,
         'tmdb_id', s.tmdb_id,
         'source_image_id', s.source_image_id,
         'image_type', s.image_type,
         'fetch_method', s.fetch_method,
         'fetched_from_url', s.fetched_from_url
       ),
       s.created_at,
       s.updated_at
from core.show_images s
join lateral (
  select a.*
  from core.media_assets a
  where a.source = s.source
    and (
      (a.hosted_sha256 is not null and a.hosted_sha256 = s.hosted_sha256)
      or (a.source_asset_id is not null and a.source_asset_id = s.source_image_id)
      or (a.source_url is not null and a.source_url = coalesce(s.url_original, s.url))
    )
  order by
    (a.hosted_sha256 is not null and a.hosted_sha256 = s.hosted_sha256) desc,
    (a.source_asset_id is not null and a.source_asset_id = s.source_image_id) desc,
    (a.source_url is not null and a.source_url = coalesce(s.url_original, s.url)) desc,
    a.id
  limit 1
) a on true
on conflict (entity_type, entity_id, kind, media_asset_id) do update
set
  context = excluded.context,
  position = coalesce(excluded.position, core.media_links.position),
  updated_at = now();

-- ---------------------------------------------------------------------------
-- season_images -> media_assets
-- ---------------------------------------------------------------------------
insert into core.media_assets (
  media_type, source, source_asset_id, source_url,
  content_type, bytes, width, height, caption,
  hosted_bucket, hosted_key, hosted_url, hosted_etag, hosted_at, hosted_sha256,
  hosted_content_type, hosted_bytes,
  metadata, fetched_at,
  created_at, updated_at
)
select 'image', source, source_image_id, coalesce(url_original, url),
       hosted_content_type, hosted_bytes, width, height, caption,
       hosted_bucket, hosted_key, hosted_url, hosted_etag, hosted_at, hosted_sha256,
       hosted_content_type, hosted_bytes,
       coalesce(metadata, '{}'::jsonb), fetched_at,
       created_at, updated_at
from core.season_images
on conflict do nothing;

insert into core.media_links (entity_type, entity_id, media_asset_id, kind, position, context, created_at, updated_at)
select 'season', s.season_id, a.id, s.kind, s.position,
       jsonb_build_object(
         'legacy_table','season_images',
         'legacy_id', s.id,
         'file_path', s.file_path,
         'url_path', s.url_path,
         'iso_639_1', s.iso_639_1,
         'tmdb_series_id', s.tmdb_series_id,
         'season_number', s.season_number,
         'source_image_id', s.source_image_id,
         'image_type', s.image_type,
         'fetch_method', s.fetch_method,
         'fetched_from_url', s.fetched_from_url
       ),
       s.created_at,
       s.updated_at
from core.season_images s
join lateral (
  select a.*
  from core.media_assets a
  where a.source = s.source
    and (
      (a.hosted_sha256 is not null and a.hosted_sha256 = s.hosted_sha256)
      or (a.source_asset_id is not null and a.source_asset_id = s.source_image_id)
      or (a.source_url is not null and a.source_url = coalesce(s.url_original, s.url))
    )
  order by
    (a.hosted_sha256 is not null and a.hosted_sha256 = s.hosted_sha256) desc,
    (a.source_asset_id is not null and a.source_asset_id = s.source_image_id) desc,
    (a.source_url is not null and a.source_url = coalesce(s.url_original, s.url)) desc,
    a.id
  limit 1
) a on true
on conflict (entity_type, entity_id, kind, media_asset_id) do update
set
  context = excluded.context,
  position = coalesce(excluded.position, core.media_links.position),
  updated_at = now();

-- ---------------------------------------------------------------------------
-- episode_images -> media_assets
-- ---------------------------------------------------------------------------
insert into core.media_assets (
  media_type, source, source_asset_id, source_url,
  content_type, bytes, width, height, caption,
  hosted_bucket, hosted_key, hosted_url, hosted_etag, hosted_at, hosted_sha256,
  hosted_content_type, hosted_bytes,
  metadata, fetched_at,
  created_at, updated_at
)
select 'image', source, source_image_id, coalesce(url_original, url),
       hosted_content_type, hosted_bytes, width, height, caption,
       hosted_bucket, hosted_key, hosted_url, hosted_etag, hosted_at, hosted_sha256,
       hosted_content_type, hosted_bytes,
       coalesce(metadata, '{}'::jsonb), fetched_at,
       created_at, updated_at
from core.episode_images
on conflict do nothing;

insert into core.media_links (entity_type, entity_id, media_asset_id, kind, position, context, created_at, updated_at)
select 'episode', e.episode_id, a.id, e.kind, e.position,
       jsonb_build_object(
         'legacy_table','episode_images',
         'legacy_id', e.id,
         'file_path', e.file_path,
         'url_path', null,
         'iso_639_1', e.iso_639_1,
         'tmdb_series_id', e.tmdb_series_id,
         'season_number', e.season_number,
         'episode_number', e.episode_number,
         'source_image_id', e.source_image_id,
         'image_type', null,
         'fetch_method', e.fetch_method,
         'fetched_from_url', e.fetched_from_url
       ),
       e.created_at,
       e.updated_at
from core.episode_images e
join lateral (
  select a.*
  from core.media_assets a
  where a.source = e.source
    and (
      (a.hosted_sha256 is not null and a.hosted_sha256 = e.hosted_sha256)
      or (a.source_asset_id is not null and a.source_asset_id = e.source_image_id)
      or (a.source_url is not null and a.source_url = coalesce(e.url_original, e.url))
    )
  order by
    (a.hosted_sha256 is not null and a.hosted_sha256 = e.hosted_sha256) desc,
    (a.source_asset_id is not null and a.source_asset_id = e.source_image_id) desc,
    (a.source_url is not null and a.source_url = coalesce(e.url_original, e.url)) desc,
    a.id
  limit 1
) a on true
on conflict (entity_type, entity_id, kind, media_asset_id) do update
set
  context = excluded.context,
  position = coalesce(excluded.position, core.media_links.position),
  updated_at = now();

-- ---------------------------------------------------------------------------
-- person_images -> media_assets
-- ---------------------------------------------------------------------------
insert into core.media_assets (
  media_type, source, source_asset_id, source_url,
  content_type, bytes, width, height, caption,
  metadata, fetched_at,
  created_at, updated_at
)
select 'image', source, null, url,
       null, null, width, height, caption,
       '{}'::jsonb, null,
       created_at, updated_at
from core.person_images
on conflict do nothing;

insert into core.media_links (
  entity_type, entity_id, media_asset_id, kind, position, is_primary, context, created_at, updated_at
)
select 'person', p.person_id, a.id, 'profile', null, false,
       jsonb_build_object(
         'legacy_table','person_images',
         'legacy_id', p.id
       ),
       p.created_at,
       p.updated_at
from core.person_images p
join core.media_assets a
  on a.source = p.source
 and a.source_url = p.url
on conflict (entity_type, entity_id, kind, media_asset_id) do update
set
  context = excluded.context,
  is_primary = excluded.is_primary,
  updated_at = now();

-- ---------------------------------------------------------------------------
-- cast_photos -> media_assets
-- ---------------------------------------------------------------------------
insert into core.media_assets (
  media_type, source, source_asset_id, source_url,
  content_type, bytes, width, height, caption,
  hosted_bucket, hosted_key, hosted_url, hosted_etag, hosted_at, hosted_sha256,
  hosted_content_type, hosted_bytes,
  metadata, fetched_at,
  created_at, updated_at
)
select 'image', source, source_image_id,
       coalesce(image_url_canonical, image_url, url, thumb_url),
       hosted_content_type, hosted_bytes, width, height, caption,
       hosted_bucket, hosted_key, hosted_url, hosted_etag, hosted_at, hosted_sha256,
       hosted_content_type, hosted_bytes,
       coalesce(metadata, '{}'::jsonb), fetched_at,
       coalesce(fetched_at, updated_at, now()),
       coalesce(updated_at, fetched_at, now())
from core.cast_photos
on conflict do nothing;

insert into core.media_links (entity_type, entity_id, media_asset_id, kind, position, context, created_at, updated_at)
select 'person', c.person_id, a.id, 'gallery', c.gallery_index,
       jsonb_build_object(
         'legacy_table','cast_photos',
         'legacy_id', c.id,
         'source_image_id', c.source_image_id,
         'viewer_id', c.viewer_id,
         'mediaindex_url_path', c.mediaindex_url_path,
         'mediaviewer_url_path', c.mediaviewer_url_path,
         'image_url_canonical', c.image_url_canonical,
         'gallery_index', c.gallery_index,
         'gallery_total', c.gallery_total
       ),
       coalesce(c.fetched_at, c.updated_at, now()),
       coalesce(c.updated_at, c.fetched_at, now())
from core.cast_photos c
join lateral (
  select a.*
  from core.media_assets a
  where a.source = c.source
    and (
      (a.hosted_sha256 is not null and a.hosted_sha256 = c.hosted_sha256)
      or (a.source_asset_id is not null and a.source_asset_id = c.source_image_id)
      or (a.source_url is not null and a.source_url = coalesce(c.image_url_canonical, c.image_url, c.url, c.thumb_url))
    )
  order by
    (a.hosted_sha256 is not null and a.hosted_sha256 = c.hosted_sha256) desc,
    (a.source_asset_id is not null and a.source_asset_id = c.source_image_id) desc,
    (a.source_url is not null and a.source_url = coalesce(c.image_url_canonical, c.image_url, c.url, c.thumb_url)) desc,
    a.id
  limit 1
) a on true
on conflict (entity_type, entity_id, kind, media_asset_id) do update
set
  context = excluded.context,
  position = coalesce(excluded.position, core.media_links.position),
  updated_at = now();

commit;
