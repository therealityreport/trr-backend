begin;

create schema if not exists core;

-- ---------------------------------------------------------------------------
-- show_images bridge
-- ---------------------------------------------------------------------------
create or replace function core.bridge_show_images_to_media()
returns trigger
language plpgsql
as $$
declare
  v_source_url text;
  v_media_asset_id uuid;
  v_media_link_id uuid;
begin
  if tg_op = 'DELETE' then
    delete from core.media_links
    where entity_type = 'show'
      and entity_id = old.show_id
      and (context->>'legacy_table') = 'show_images'
      and (context->>'legacy_id')::uuid = old.id;
    return old;
  end if;

  v_source_url := coalesce(new.url_original, new.url);

  v_media_asset_id := null;

  if tg_op = 'UPDATE' then
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

    if v_media_asset_id is not null then
      update core.media_assets
      set
        source_asset_id = coalesce(core.media_assets.source_asset_id, new.source_image_id),
        source_url = coalesce(v_source_url, core.media_assets.source_url),
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
        hosted_sha256 = coalesce(new.hosted_sha256, core.media_assets.hosted_sha256),
        hosted_content_type = coalesce(new.hosted_content_type, core.media_assets.hosted_content_type),
        hosted_bytes = coalesce(new.hosted_bytes, core.media_assets.hosted_bytes),
        metadata = coalesce(new.metadata, core.media_assets.metadata),
        fetched_at = coalesce(new.fetched_at, core.media_assets.fetched_at),
        updated_at = now()
      where id = v_media_asset_id;
    end if;
  end if;

  if v_media_asset_id is null then
    if tg_op = 'UPDATE' then
      return new;
    end if;

    if new.hosted_sha256 is not null then
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
        new.hosted_bucket, new.hosted_key, new.hosted_url, new.hosted_etag, new.hosted_at, new.hosted_sha256,
        new.hosted_content_type, new.hosted_bytes,
        coalesce(new.metadata, '{}'::jsonb), new.fetched_at,
        new.created_at, new.updated_at
      )
      on conflict (source, hosted_sha256) where hosted_sha256 is not null do update
      set
        source_asset_id = coalesce(core.media_assets.source_asset_id, excluded.source_asset_id),
        source_url = coalesce(excluded.source_url, core.media_assets.source_url),
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
        hosted_sha256 = coalesce(excluded.hosted_sha256, core.media_assets.hosted_sha256),
        hosted_content_type = coalesce(excluded.hosted_content_type, core.media_assets.hosted_content_type),
        hosted_bytes = coalesce(excluded.hosted_bytes, core.media_assets.hosted_bytes),
        metadata = coalesce(excluded.metadata, core.media_assets.metadata),
        fetched_at = coalesce(excluded.fetched_at, core.media_assets.fetched_at),
        updated_at = now()
      returning id into v_media_asset_id;
    else
      if new.source_image_id is not null then
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
          new.hosted_bucket, new.hosted_key, new.hosted_url, new.hosted_etag, new.hosted_at, new.hosted_sha256,
          new.hosted_content_type, new.hosted_bytes,
          coalesce(new.metadata, '{}'::jsonb), new.fetched_at,
          new.created_at, new.updated_at
        )
        on conflict (source, source_asset_id) where source_asset_id is not null do update
        set
          source_url = coalesce(excluded.source_url, core.media_assets.source_url),
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
          hosted_sha256 = coalesce(excluded.hosted_sha256, core.media_assets.hosted_sha256),
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
          new.hosted_bucket, new.hosted_key, new.hosted_url, new.hosted_etag, new.hosted_at, new.hosted_sha256,
          new.hosted_content_type, new.hosted_bytes,
          coalesce(new.metadata, '{}'::jsonb), new.fetched_at,
          new.created_at, new.updated_at
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
          hosted_sha256 = coalesce(excluded.hosted_sha256, core.media_assets.hosted_sha256),
          hosted_content_type = coalesce(excluded.hosted_content_type, core.media_assets.hosted_content_type),
          hosted_bytes = coalesce(excluded.hosted_bytes, core.media_assets.hosted_bytes),
          metadata = coalesce(excluded.metadata, core.media_assets.metadata),
          fetched_at = coalesce(excluded.fetched_at, core.media_assets.fetched_at),
          updated_at = now()
        returning id into v_media_asset_id;
      end if;
    end if;
  end if;

  if v_media_asset_id is null then
    select id into v_media_asset_id
    from core.media_assets
    where source = new.source
      and (
        (hosted_sha256 is not null and hosted_sha256 = new.hosted_sha256)
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
    'show', new.show_id, v_media_asset_id, new.kind, new.position, false,
    jsonb_build_object(
      'legacy_table','show_images',
      'legacy_id', new.id,
      'file_path', new.file_path,
      'url_path', new.url_path,
      'iso_639_1', new.iso_639_1,
      'tmdb_id', new.tmdb_id,
      'source_image_id', new.source_image_id,
      'image_type', new.image_type,
      'fetch_method', new.fetch_method,
      'fetched_from_url', new.fetched_from_url
    ),
    new.created_at, new.updated_at
  )
  on conflict (entity_type, entity_id, kind, media_asset_id) do update
  set
    context = excluded.context,
    position = coalesce(excluded.position, core.media_links.position),
    updated_at = now()
  returning id into v_media_link_id;

  if v_media_link_id is not null
    and new.kind in ('poster', 'backdrop', 'logo', 'still', 'profile') then
    perform core.set_primary_media_link('show', new.show_id, new.kind, v_media_link_id);
  end if;

  return new;
end $$;

-- Trigger guard

do $$
begin
  if exists (select 1 from pg_trigger where tgname = 'bridge_show_images_to_media') then
    drop trigger bridge_show_images_to_media on core.show_images;
  end if;
  create trigger bridge_show_images_to_media
  after insert or update or delete on core.show_images
  for each row execute function core.bridge_show_images_to_media();
end $$;

-- ---------------------------------------------------------------------------
-- season_images bridge
-- ---------------------------------------------------------------------------
create or replace function core.bridge_season_images_to_media()
returns trigger
language plpgsql
as $$
declare
  v_source_url text;
  v_media_asset_id uuid;
  v_media_link_id uuid;
begin
  if tg_op = 'DELETE' then
    delete from core.media_links
    where entity_type = 'season'
      and entity_id = old.season_id
      and (context->>'legacy_table') = 'season_images'
      and (context->>'legacy_id')::uuid = old.id;
    return old;
  end if;

  v_source_url := coalesce(new.url_original, new.url);

  v_media_asset_id := null;

  if tg_op = 'UPDATE' then
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

    if v_media_asset_id is not null then
      update core.media_assets
      set
        source_asset_id = coalesce(core.media_assets.source_asset_id, new.source_image_id),
        source_url = coalesce(v_source_url, core.media_assets.source_url),
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
        hosted_sha256 = coalesce(new.hosted_sha256, core.media_assets.hosted_sha256),
        hosted_content_type = coalesce(new.hosted_content_type, core.media_assets.hosted_content_type),
        hosted_bytes = coalesce(new.hosted_bytes, core.media_assets.hosted_bytes),
        metadata = coalesce(new.metadata, core.media_assets.metadata),
        fetched_at = coalesce(new.fetched_at, core.media_assets.fetched_at),
        updated_at = now()
      where id = v_media_asset_id;
    end if;
  end if;

  if v_media_asset_id is null then
    if tg_op = 'UPDATE' then
      return new;
    end if;

    if new.hosted_sha256 is not null then
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
        new.hosted_bucket, new.hosted_key, new.hosted_url, new.hosted_etag, new.hosted_at, new.hosted_sha256,
        new.hosted_content_type, new.hosted_bytes,
        coalesce(new.metadata, '{}'::jsonb), new.fetched_at,
        new.created_at, new.updated_at
      )
      on conflict (source, hosted_sha256) where hosted_sha256 is not null do update
      set
        source_asset_id = coalesce(core.media_assets.source_asset_id, excluded.source_asset_id),
        source_url = coalesce(excluded.source_url, core.media_assets.source_url),
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
        hosted_sha256 = coalesce(excluded.hosted_sha256, core.media_assets.hosted_sha256),
        hosted_content_type = coalesce(excluded.hosted_content_type, core.media_assets.hosted_content_type),
        hosted_bytes = coalesce(excluded.hosted_bytes, core.media_assets.hosted_bytes),
        metadata = coalesce(excluded.metadata, core.media_assets.metadata),
        fetched_at = coalesce(excluded.fetched_at, core.media_assets.fetched_at),
        updated_at = now()
      returning id into v_media_asset_id;
    else
      if new.source_image_id is not null then
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
          new.hosted_bucket, new.hosted_key, new.hosted_url, new.hosted_etag, new.hosted_at, new.hosted_sha256,
          new.hosted_content_type, new.hosted_bytes,
          coalesce(new.metadata, '{}'::jsonb), new.fetched_at,
          new.created_at, new.updated_at
        )
        on conflict (source, source_asset_id) where source_asset_id is not null do update
        set
          source_url = coalesce(excluded.source_url, core.media_assets.source_url),
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
          hosted_sha256 = coalesce(excluded.hosted_sha256, core.media_assets.hosted_sha256),
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
          new.hosted_bucket, new.hosted_key, new.hosted_url, new.hosted_etag, new.hosted_at, new.hosted_sha256,
          new.hosted_content_type, new.hosted_bytes,
          coalesce(new.metadata, '{}'::jsonb), new.fetched_at,
          new.created_at, new.updated_at
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
          hosted_sha256 = coalesce(excluded.hosted_sha256, core.media_assets.hosted_sha256),
          hosted_content_type = coalesce(excluded.hosted_content_type, core.media_assets.hosted_content_type),
          hosted_bytes = coalesce(excluded.hosted_bytes, core.media_assets.hosted_bytes),
          metadata = coalesce(excluded.metadata, core.media_assets.metadata),
          fetched_at = coalesce(excluded.fetched_at, core.media_assets.fetched_at),
          updated_at = now()
        returning id into v_media_asset_id;
      end if;
    end if;
  end if;

  if v_media_asset_id is null then
    select id into v_media_asset_id
    from core.media_assets
    where source = new.source
      and (
        (hosted_sha256 is not null and hosted_sha256 = new.hosted_sha256)
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
    'season', new.season_id, v_media_asset_id, new.kind, new.position, false,
    jsonb_build_object(
      'legacy_table','season_images',
      'legacy_id', new.id,
      'file_path', new.file_path,
      'url_path', new.url_path,
      'iso_639_1', new.iso_639_1,
      'tmdb_series_id', new.tmdb_series_id,
      'season_number', new.season_number,
      'source_image_id', new.source_image_id,
      'image_type', new.image_type,
      'fetch_method', new.fetch_method,
      'fetched_from_url', new.fetched_from_url
    ),
    new.created_at, new.updated_at
  )
  on conflict (entity_type, entity_id, kind, media_asset_id) do update
  set
    context = excluded.context,
    position = coalesce(excluded.position, core.media_links.position),
    updated_at = now()
  returning id into v_media_link_id;

  if v_media_link_id is not null
    and new.kind in ('poster', 'backdrop', 'logo', 'still', 'profile') then
    perform core.set_primary_media_link('season', new.season_id, new.kind, v_media_link_id);
  end if;

  return new;
end $$;

-- Trigger guard

do $$
begin
  if exists (select 1 from pg_trigger where tgname = 'bridge_season_images_to_media') then
    drop trigger bridge_season_images_to_media on core.season_images;
  end if;
  create trigger bridge_season_images_to_media
  after insert or update or delete on core.season_images
  for each row execute function core.bridge_season_images_to_media();
end $$;

-- ---------------------------------------------------------------------------
-- episode_images bridge
-- ---------------------------------------------------------------------------
create or replace function core.bridge_episode_images_to_media()
returns trigger
language plpgsql
as $$
declare
  v_source_url text;
  v_media_asset_id uuid;
  v_media_link_id uuid;
begin
  if tg_op = 'DELETE' then
    delete from core.media_links
    where entity_type = 'episode'
      and entity_id = old.episode_id
      and (context->>'legacy_table') = 'episode_images'
      and (context->>'legacy_id')::uuid = old.id;
    return old;
  end if;

  v_source_url := coalesce(new.url_original, new.url);

  v_media_asset_id := null;

  if tg_op = 'UPDATE' then
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

    if v_media_asset_id is not null then
      update core.media_assets
      set
        source_asset_id = coalesce(core.media_assets.source_asset_id, new.source_image_id),
        source_url = coalesce(v_source_url, core.media_assets.source_url),
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
        hosted_sha256 = coalesce(new.hosted_sha256, core.media_assets.hosted_sha256),
        hosted_content_type = coalesce(new.hosted_content_type, core.media_assets.hosted_content_type),
        hosted_bytes = coalesce(new.hosted_bytes, core.media_assets.hosted_bytes),
        metadata = coalesce(new.metadata, core.media_assets.metadata),
        fetched_at = coalesce(new.fetched_at, core.media_assets.fetched_at),
        updated_at = now()
      where id = v_media_asset_id;
    end if;
  end if;

  if v_media_asset_id is null then
    if tg_op = 'UPDATE' then
      return new;
    end if;

    if new.hosted_sha256 is not null then
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
        new.hosted_bucket, new.hosted_key, new.hosted_url, new.hosted_etag, new.hosted_at, new.hosted_sha256,
        new.hosted_content_type, new.hosted_bytes,
        coalesce(new.metadata, '{}'::jsonb), new.fetched_at,
        new.created_at, new.updated_at
      )
      on conflict (source, hosted_sha256) where hosted_sha256 is not null do update
      set
        source_asset_id = coalesce(core.media_assets.source_asset_id, excluded.source_asset_id),
        source_url = coalesce(excluded.source_url, core.media_assets.source_url),
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
        hosted_sha256 = coalesce(excluded.hosted_sha256, core.media_assets.hosted_sha256),
        hosted_content_type = coalesce(excluded.hosted_content_type, core.media_assets.hosted_content_type),
        hosted_bytes = coalesce(excluded.hosted_bytes, core.media_assets.hosted_bytes),
        metadata = coalesce(excluded.metadata, core.media_assets.metadata),
        fetched_at = coalesce(excluded.fetched_at, core.media_assets.fetched_at),
        updated_at = now()
      returning id into v_media_asset_id;
    else
      if new.source_image_id is not null then
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
          new.hosted_bucket, new.hosted_key, new.hosted_url, new.hosted_etag, new.hosted_at, new.hosted_sha256,
          new.hosted_content_type, new.hosted_bytes,
          coalesce(new.metadata, '{}'::jsonb), new.fetched_at,
          new.created_at, new.updated_at
        )
        on conflict (source, source_asset_id) where source_asset_id is not null do update
        set
          source_url = coalesce(excluded.source_url, core.media_assets.source_url),
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
          hosted_sha256 = coalesce(excluded.hosted_sha256, core.media_assets.hosted_sha256),
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
          new.hosted_bucket, new.hosted_key, new.hosted_url, new.hosted_etag, new.hosted_at, new.hosted_sha256,
          new.hosted_content_type, new.hosted_bytes,
          coalesce(new.metadata, '{}'::jsonb), new.fetched_at,
          new.created_at, new.updated_at
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
          hosted_sha256 = coalesce(excluded.hosted_sha256, core.media_assets.hosted_sha256),
          hosted_content_type = coalesce(excluded.hosted_content_type, core.media_assets.hosted_content_type),
          hosted_bytes = coalesce(excluded.hosted_bytes, core.media_assets.hosted_bytes),
          metadata = coalesce(excluded.metadata, core.media_assets.metadata),
          fetched_at = coalesce(excluded.fetched_at, core.media_assets.fetched_at),
          updated_at = now()
        returning id into v_media_asset_id;
      end if;
    end if;
  end if;

  if v_media_asset_id is null then
    select id into v_media_asset_id
    from core.media_assets
    where source = new.source
      and (
        (hosted_sha256 is not null and hosted_sha256 = new.hosted_sha256)
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
    'episode', new.episode_id, v_media_asset_id, new.kind, new.position, false,
    jsonb_build_object(
      'legacy_table','episode_images',
      'legacy_id', new.id,
      'file_path', new.file_path,
      'url_path', null,
      'iso_639_1', new.iso_639_1,
      'tmdb_series_id', new.tmdb_series_id,
      'season_number', new.season_number,
      'episode_number', new.episode_number,
      'source_image_id', new.source_image_id,
      'image_type', null,
      'fetch_method', new.fetch_method,
      'fetched_from_url', new.fetched_from_url
    ),
    new.created_at, new.updated_at
  )
  on conflict (entity_type, entity_id, kind, media_asset_id) do update
  set
    context = excluded.context,
    position = coalesce(excluded.position, core.media_links.position),
    updated_at = now()
  returning id into v_media_link_id;

  if v_media_link_id is not null
    and new.kind in ('poster', 'backdrop', 'logo', 'still', 'profile') then
    perform core.set_primary_media_link('episode', new.episode_id, new.kind, v_media_link_id);
  end if;

  return new;
end $$;

-- Trigger guard

do $$
begin
  if exists (select 1 from pg_trigger where tgname = 'bridge_episode_images_to_media') then
    drop trigger bridge_episode_images_to_media on core.episode_images;
  end if;
  create trigger bridge_episode_images_to_media
  after insert or update or delete on core.episode_images
  for each row execute function core.bridge_episode_images_to_media();
end $$;

-- ---------------------------------------------------------------------------
-- person_images bridge
-- ---------------------------------------------------------------------------
create or replace function core.bridge_person_images_to_media()
returns trigger
language plpgsql
as $$
declare
  v_media_asset_id uuid;
  v_media_link_id uuid;
begin
  if tg_op = 'DELETE' then
    delete from core.media_links
    where entity_type = 'person'
      and (context->>'legacy_table') = 'person_images'
      and (context->>'legacy_id')::uuid = old.id;
    return old;
  end if;

  insert into core.media_assets (
    media_type, source, source_asset_id, source_url,
    content_type, bytes, width, height, caption,
    metadata, fetched_at,
    created_at, updated_at
  ) values (
    'image', new.source, null, new.url,
    null, null, new.width, new.height, new.caption,
    '{}'::jsonb, null,
    new.created_at, new.updated_at
  )
  on conflict (source, source_url) where source_url is not null do update
  set
    width = greatest(coalesce(excluded.width, 0), coalesce(core.media_assets.width, 0)),
    height = greatest(coalesce(excluded.height, 0), coalesce(core.media_assets.height, 0)),
    caption = coalesce(excluded.caption, core.media_assets.caption),
    updated_at = now();

  select id into v_media_asset_id
  from core.media_assets
  where source = new.source
    and source_url = new.url
  order by created_at asc
  limit 1;

  if v_media_asset_id is null then
    return new;
  end if;

  insert into core.media_links (
    entity_type, entity_id, media_asset_id, kind, position, is_primary, context, created_at, updated_at
  ) values (
    'person', new.person_id, v_media_asset_id, 'profile', null, false,
    jsonb_build_object(
      'legacy_table','person_images',
      'legacy_id', new.id
    ),
    new.created_at, new.updated_at
  )
  on conflict (entity_type, entity_id, kind, media_asset_id) do update
  set
    context = excluded.context,
    is_primary = excluded.is_primary,
    updated_at = now()
  returning id into v_media_link_id;

  if v_media_link_id is not null then
    perform core.set_primary_media_link('person', new.person_id, 'profile', v_media_link_id);
  end if;

  return new;
end $$;

-- Trigger guard

do $$
begin
  if exists (select 1 from pg_trigger where tgname = 'bridge_person_images_to_media') then
    drop trigger bridge_person_images_to_media on core.person_images;
  end if;
  create trigger bridge_person_images_to_media
  after insert or update or delete on core.person_images
  for each row execute function core.bridge_person_images_to_media();
end $$;

-- ---------------------------------------------------------------------------
-- cast_photos bridge
-- ---------------------------------------------------------------------------
create or replace function core.bridge_cast_photos_to_media()
returns trigger
language plpgsql
as $$
declare
  v_source_url text;
  v_media_asset_id uuid;
begin
  if tg_op = 'DELETE' then
    delete from core.media_links
    where entity_type = 'person'
      and (context->>'legacy_table') = 'cast_photos'
      and (context->>'legacy_id')::uuid = old.id;
    return old;
  end if;

  v_source_url := coalesce(new.image_url_canonical, new.image_url, new.url, new.thumb_url);

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

  if v_media_asset_id is not null then
    update core.media_assets
    set
      source_asset_id = coalesce(core.media_assets.source_asset_id, new.source_image_id),
      source_url = coalesce(v_source_url, core.media_assets.source_url),
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
      hosted_sha256 = coalesce(new.hosted_sha256, core.media_assets.hosted_sha256),
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

  if v_media_asset_id is null and new.hosted_sha256 is not null then
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
      new.hosted_bucket, new.hosted_key, new.hosted_url, new.hosted_etag, new.hosted_at, new.hosted_sha256,
      new.hosted_content_type, new.hosted_bytes,
      coalesce(new.metadata, '{}'::jsonb), new.fetched_at,
      coalesce(new.fetched_at, new.updated_at, now()),
      coalesce(new.updated_at, new.fetched_at, now())
    )
    on conflict (source, hosted_sha256) where hosted_sha256 is not null do update
    set
      source_asset_id = coalesce(core.media_assets.source_asset_id, excluded.source_asset_id),
      source_url = coalesce(excluded.source_url, core.media_assets.source_url),
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
      hosted_sha256 = coalesce(excluded.hosted_sha256, core.media_assets.hosted_sha256),
      hosted_content_type = coalesce(excluded.hosted_content_type, core.media_assets.hosted_content_type),
      hosted_bytes = coalesce(excluded.hosted_bytes, core.media_assets.hosted_bytes),
      metadata = coalesce(excluded.metadata, core.media_assets.metadata),
      fetched_at = coalesce(excluded.fetched_at, core.media_assets.fetched_at),
      updated_at = now()
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
        new.hosted_bucket, new.hosted_key, new.hosted_url, new.hosted_etag, new.hosted_at, new.hosted_sha256,
        new.hosted_content_type, new.hosted_bytes,
        coalesce(new.metadata, '{}'::jsonb), new.fetched_at,
        coalesce(new.fetched_at, new.updated_at, now()),
        coalesce(new.updated_at, new.fetched_at, now())
      )
      on conflict (source, source_asset_id) where source_asset_id is not null do update
      set
        source_url = coalesce(excluded.source_url, core.media_assets.source_url),
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
        hosted_sha256 = coalesce(excluded.hosted_sha256, core.media_assets.hosted_sha256),
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
        new.hosted_bucket, new.hosted_key, new.hosted_url, new.hosted_etag, new.hosted_at, new.hosted_sha256,
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
        hosted_sha256 = coalesce(excluded.hosted_sha256, core.media_assets.hosted_sha256),
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
        (hosted_sha256 is not null and hosted_sha256 = new.hosted_sha256)
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

-- Trigger guard

do $$
begin
  if exists (select 1 from pg_trigger where tgname = 'bridge_cast_photos_to_media') then
    drop trigger bridge_cast_photos_to_media on core.cast_photos;
  end if;
  create trigger bridge_cast_photos_to_media
  after insert or update or delete on core.cast_photos
  for each row execute function core.bridge_cast_photos_to_media();
end $$;

commit;
