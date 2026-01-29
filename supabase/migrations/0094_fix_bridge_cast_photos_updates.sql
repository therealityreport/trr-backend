-- Fix cast_photos bridge: avoid unique conflicts on source_url/hosted_sha256 updates
create or replace function core.bridge_cast_photos_to_media()
returns trigger
language plpgsql
as $$
declare
  v_source_url text;
  v_media_asset_id uuid;
  v_hosted_sha text;
begin
  if tg_op = 'DELETE' then
    delete from core.media_links
    where entity_type = 'person'
      and (context->>'legacy_table') = 'cast_photos'
      and (context->>'legacy_id')::uuid = old.id;
    return old;
  end if;

  -- Skip media_assets linking on UPDATE to avoid duplicate insert/update conflicts.
  -- Hosted fields are stored on cast_photos and do not need re-linking.
  if tg_op = 'UPDATE' then
    return new;
  end if;

  v_source_url := coalesce(new.image_url_canonical, new.image_url, new.url, new.thumb_url);
  v_hosted_sha := new.hosted_sha256;

  v_media_asset_id := null;

  select id into v_media_asset_id
  from core.media_assets
  where source = new.source
    and (
      (new.hosted_sha256 is not null and hosted_sha256 = new.hosted_sha256)
      or (source_asset_id is not null and source_asset_id = new.source_image_id)
      or (source_url is not null and source_url = v_source_url)
    )
  order by created_at asc
  limit 1;

  -- Re-check by direct keys before any insert to avoid unique conflicts
  if v_media_asset_id is null and v_source_url is not null then
    select id into v_media_asset_id
    from core.media_assets
    where source = new.source
      and source_url = v_source_url
    order by created_at asc
    limit 1;
  end if;

  if v_media_asset_id is null and v_hosted_sha is not null then
    select id into v_media_asset_id
    from core.media_assets
    where source = new.source
      and hosted_sha256 = v_hosted_sha
    order by created_at asc
    limit 1;
  end if;

  -- If we're about to insert, avoid source_url/hosted_sha collisions up front
  if v_media_asset_id is null and v_source_url is not null then
    if exists (
      select 1 from core.media_assets
      where source = new.source
        and source_url = v_source_url
    ) then
      v_source_url := null;
    end if;
  end if;

  if v_media_asset_id is null and v_hosted_sha is not null then
    if exists (
      select 1 from core.media_assets
      where source = new.source
        and hosted_sha256 = v_hosted_sha
    ) then
      v_hosted_sha := null;
    end if;
  end if;

  -- On UPDATE, never attempt to modify unique columns on media_assets
  if tg_op = 'UPDATE' then
    v_source_url := null;
    v_hosted_sha := null;
  end if;

  if v_media_asset_id is not null then
    if v_source_url is not null then
      if exists (
        select 1 from core.media_assets
        where source = new.source
          and source_url = v_source_url
          and id <> v_media_asset_id
      ) then
        v_source_url := null;
      end if;
    end if;

    if v_hosted_sha is not null then
      if exists (
        select 1 from core.media_assets
        where source = new.source
          and hosted_sha256 = v_hosted_sha
          and id <> v_media_asset_id
      ) then
        v_hosted_sha := null;
      end if;
    end if;

    update core.media_assets
    set
      source_asset_id = coalesce(core.media_assets.source_asset_id, new.source_image_id),
      source_url = coalesce(core.media_assets.source_url, v_source_url),
      content_type = coalesce(new.hosted_content_type, core.media_assets.content_type),
      bytes = coalesce(new.hosted_bytes, core.media_assets.bytes),
      width = greatest(coalesce(new.width, 0), coalesce(core.media_assets.width, 0)),
      height = greatest(coalesce(new.height, 0), coalesce(core.media_assets.height, 0)),
      caption = coalesce(new.caption, core.media_assets.caption),
      hosted_bucket = coalesce(new.hosted_bucket, core.media_assets.hosted_bucket),
      hosted_key = coalesce(new.hosted_key, core.media_assets.hosted_key),
      hosted_url = coalesce(new.hosted_url, core.media_assets.hosted_url),
      hosted_etag = coalesce(new.hosted_etag, core.media_assets.hosted_etag),
      hosted_at = coalesce(new.hosted_at, core.media_assets.hosted_at),
      hosted_sha256 = coalesce(v_hosted_sha, core.media_assets.hosted_sha256),
      hosted_content_type = coalesce(new.hosted_content_type, core.media_assets.hosted_content_type),
      hosted_bytes = coalesce(new.hosted_bytes, core.media_assets.hosted_bytes),
      metadata = coalesce(new.metadata, core.media_assets.metadata),
      fetched_at = coalesce(new.fetched_at, core.media_assets.fetched_at),
      updated_at = now()
    where id = v_media_asset_id;
  end if;

  if v_media_asset_id is null then
    if tg_op = 'UPDATE' then
      return new;
    end if;
  end if;

  if v_media_asset_id is null and v_hosted_sha is not null then
    insert into core.media_assets (
      media_type, source, source_asset_id, source_url,
      content_type, bytes, width, height, caption,
      hosted_bucket, hosted_key, hosted_url, hosted_etag, hosted_at, hosted_sha256,
      hosted_content_type, hosted_bytes,
      metadata, fetched_at,
      created_at, updated_at
    ) values (
      'image', new.source, new.source_image_id, v_source_url,
      new.hosted_content_type, new.hosted_bytes, new.width, new.height, new.caption,
      new.hosted_bucket, new.hosted_key, new.hosted_url, new.hosted_etag, new.hosted_at, v_hosted_sha,
      new.hosted_content_type, new.hosted_bytes,
      coalesce(new.metadata, '{}'::jsonb), new.fetched_at,
      coalesce(new.fetched_at, new.updated_at, now()),
      coalesce(new.updated_at, new.fetched_at, now())
    )
    on conflict do nothing
    returning id into v_media_asset_id;
  else
    if v_media_asset_id is null and new.source_image_id is not null then
      insert into core.media_assets (
        media_type, source, source_asset_id, source_url,
        content_type, bytes, width, height, caption,
        hosted_bucket, hosted_key, hosted_url, hosted_etag, hosted_at, hosted_sha256,
        hosted_content_type, hosted_bytes,
        metadata, fetched_at,
        created_at, updated_at
      ) values (
        'image', new.source, new.source_image_id, v_source_url,
        new.hosted_content_type, new.hosted_bytes, new.width, new.height, new.caption,
        new.hosted_bucket, new.hosted_key, new.hosted_url, new.hosted_etag, new.hosted_at, v_hosted_sha,
        new.hosted_content_type, new.hosted_bytes,
        coalesce(new.metadata, '{}'::jsonb), new.fetched_at,
        coalesce(new.fetched_at, new.updated_at, now()),
        coalesce(new.updated_at, new.fetched_at, now())
      )
      on conflict (source, source_asset_id) where source_asset_id is not null do update
      set
        source_url = coalesce(core.media_assets.source_url, excluded.source_url),
        content_type = coalesce(excluded.content_type, core.media_assets.content_type),
        bytes = coalesce(excluded.bytes, core.media_assets.bytes),
        width = greatest(coalesce(excluded.width, 0), coalesce(core.media_assets.width, 0)),
        height = greatest(coalesce(excluded.height, 0), coalesce(core.media_assets.height, 0)),
        caption = coalesce(excluded.caption, core.media_assets.caption),
        hosted_bucket = coalesce(excluded.hosted_bucket, core.media_assets.hosted_bucket),
        hosted_key = coalesce(excluded.hosted_key, core.media_assets.hosted_key),
        hosted_url = coalesce(excluded.hosted_url, core.media_assets.hosted_url),
        hosted_etag = coalesce(excluded.hosted_etag, core.media_assets.hosted_etag),
        hosted_at = coalesce(excluded.hosted_at, core.media_assets.hosted_at),
        hosted_sha256 = coalesce(core.media_assets.hosted_sha256, excluded.hosted_sha256),
        hosted_content_type = coalesce(excluded.hosted_content_type, core.media_assets.hosted_content_type),
        hosted_bytes = coalesce(excluded.hosted_bytes, core.media_assets.hosted_bytes),
        metadata = coalesce(excluded.metadata, core.media_assets.metadata),
        fetched_at = coalesce(excluded.fetched_at, core.media_assets.fetched_at),
        updated_at = now()
      returning id into v_media_asset_id;
    end if;

    if v_media_asset_id is null and v_source_url is not null then
      insert into core.media_assets (
        media_type, source, source_asset_id, source_url,
        content_type, bytes, width, height, caption,
        hosted_bucket, hosted_key, hosted_url, hosted_etag, hosted_at, hosted_sha256,
        hosted_content_type, hosted_bytes,
        metadata, fetched_at,
        created_at, updated_at
      ) values (
        'image', new.source, new.source_image_id, v_source_url,
        new.hosted_content_type, new.hosted_bytes, new.width, new.height, new.caption,
        new.hosted_bucket, new.hosted_key, new.hosted_url, new.hosted_etag, new.hosted_at, v_hosted_sha,
        new.hosted_content_type, new.hosted_bytes,
        coalesce(new.metadata, '{}'::jsonb), new.fetched_at,
        coalesce(new.fetched_at, new.updated_at, now()),
        coalesce(new.updated_at, new.fetched_at, now())
      )
      on conflict (source, source_url) where source_url is not null do update
      set
        source_asset_id = coalesce(core.media_assets.source_asset_id, excluded.source_asset_id),
        content_type = coalesce(excluded.content_type, core.media_assets.content_type),
        bytes = coalesce(excluded.bytes, core.media_assets.bytes),
        width = greatest(coalesce(excluded.width, 0), coalesce(core.media_assets.width, 0)),
        height = greatest(coalesce(excluded.height, 0), coalesce(core.media_assets.height, 0)),
        caption = coalesce(excluded.caption, core.media_assets.caption),
        hosted_bucket = coalesce(excluded.hosted_bucket, core.media_assets.hosted_bucket),
        hosted_key = coalesce(excluded.hosted_key, core.media_assets.hosted_key),
        hosted_url = coalesce(excluded.hosted_url, core.media_assets.hosted_url),
        hosted_etag = coalesce(excluded.hosted_etag, core.media_assets.hosted_etag),
        hosted_at = coalesce(excluded.hosted_at, core.media_assets.hosted_at),
        hosted_sha256 = coalesce(core.media_assets.hosted_sha256, excluded.hosted_sha256),
        hosted_content_type = coalesce(excluded.hosted_content_type, core.media_assets.hosted_content_type),
        hosted_bytes = coalesce(excluded.hosted_bytes, core.media_assets.hosted_bytes),
        metadata = coalesce(excluded.metadata, core.media_assets.metadata),
        fetched_at = coalesce(excluded.fetched_at, core.media_assets.fetched_at),
        updated_at = now()
      returning id into v_media_asset_id;
    end if;
  end if;

  if v_media_asset_id is null then
    select id into v_media_asset_id
    from core.media_assets
    where source = new.source
      and (
        (hosted_sha256 is not null and hosted_sha256 = v_hosted_sha)
        or (source_asset_id is not null and source_asset_id = new.source_image_id)
        or (source_url is not null and source_url = v_source_url)
      )
    order by created_at asc
    limit 1;
  end if;

  if v_media_asset_id is null then
    return new;
  end if;

  insert into core.media_links (
    entity_type, entity_id, media_asset_id, kind, position, is_primary, context, created_at, updated_at
  ) values (
    'person', new.person_id, v_media_asset_id, 'gallery', new.gallery_index, false,
    jsonb_build_object(
      'legacy_table','cast_photos',
      'legacy_id', new.id,
      'source_image_id', new.source_image_id,
      'viewer_id', new.viewer_id,
      'mediaindex_url_path', new.mediaindex_url_path,
      'mediaviewer_url_path', new.mediaviewer_url_path,
      'image_url_canonical', new.image_url_canonical,
      'gallery_index', new.gallery_index,
      'gallery_total', new.gallery_total
    ),
    coalesce(new.fetched_at, new.updated_at, now()),
    coalesce(new.updated_at, new.fetched_at, now())
  )
  on conflict (entity_type, entity_id, kind, media_asset_id) do update
  set
    context = excluded.context,
    position = coalesce(excluded.position, core.media_links.position),
    updated_at = now();

  return new;
end $$;
