begin;

-- =============================================================================
-- Migration 0035: RPC helpers for deterministic show_images upserts
-- =============================================================================

create or replace function core._show_images_best_width(current_width int, incoming_width int)
returns int
language sql
immutable
as $$
  select
    case
      when current_width is null and incoming_width is null then null
      when current_width is null then incoming_width
      when incoming_width is null then current_width
      else greatest(current_width, incoming_width)
    end
$$;

create or replace function core._show_images_pick_url(
  current_url text,
  current_width int,
  incoming_url text,
  incoming_width int
) returns text
language sql
immutable
as $$
  select
    case
      when incoming_url is null then current_url
      when current_url is null then incoming_url
      when incoming_width is null then current_url
      when current_width is null then incoming_url
      when incoming_width >= current_width then incoming_url
      else current_url
    end
$$;

create or replace function core.upsert_show_images_by_identity(rows jsonb)
returns setof core.show_images
language plpgsql
security definer
as $$
declare
  rec jsonb;
  row_data core.show_images%rowtype;
  inserted core.show_images%rowtype;
begin
  if rows is null then
    return;
  end if;

  for rec in select * from jsonb_array_elements(rows)
  loop
    row_data := jsonb_populate_record(null::core.show_images, rec);

    insert into core.show_images (
      show_id,
      tmdb_id,
      source,
      source_image_id,
      kind,
      iso_639_1,
      file_path,
      width,
      height,
      aspect_ratio,
      vote_average,
      vote_count,
      fetched_at,
      url,
      url_path,
      caption,
      position,
      image_type,
      metadata,
      created_at,
      updated_at,
      fetch_method,
      fetched_from_url
    ) values (
      row_data.show_id,
      row_data.tmdb_id,
      row_data.source,
      row_data.source_image_id,
      row_data.kind,
      row_data.iso_639_1,
      row_data.file_path,
      row_data.width,
      row_data.height,
      row_data.aspect_ratio,
      row_data.vote_average,
      row_data.vote_count,
      coalesce(row_data.fetched_at, now()),
      row_data.url,
      row_data.url_path,
      row_data.caption,
      row_data.position,
      row_data.image_type,
      coalesce(row_data.metadata, '{}'::jsonb),
      coalesce(row_data.created_at, now()),
      coalesce(row_data.updated_at, now()),
      row_data.fetch_method,
      row_data.fetched_from_url
    )
    on conflict on constraint show_images_show_source_source_image_id_key
    do update set
      tmdb_id = coalesce(excluded.tmdb_id, core.show_images.tmdb_id),
      url = core._show_images_pick_url(core.show_images.url, core.show_images.width, excluded.url, excluded.width),
      url_path = coalesce(excluded.url_path, core.show_images.url_path),
      caption = coalesce(excluded.caption, core.show_images.caption),
      position = coalesce(excluded.position, core.show_images.position),
      image_type = coalesce(excluded.image_type, core.show_images.image_type),
      metadata = coalesce(excluded.metadata, core.show_images.metadata),
      kind = coalesce(excluded.kind, core.show_images.kind),
      width = core._show_images_best_width(core.show_images.width, excluded.width),
      height = case
        when excluded.height is null then core.show_images.height
        when core.show_images.width is null or excluded.width is null then excluded.height
        when excluded.width >= core.show_images.width then excluded.height
        else core.show_images.height
      end,
      aspect_ratio = coalesce(excluded.aspect_ratio, core.show_images.aspect_ratio),
      vote_average = coalesce(excluded.vote_average, core.show_images.vote_average),
      vote_count = coalesce(excluded.vote_count, core.show_images.vote_count),
      fetched_at = coalesce(excluded.fetched_at, core.show_images.fetched_at),
      updated_at = coalesce(excluded.updated_at, now()),
      fetch_method = coalesce(excluded.fetch_method, core.show_images.fetch_method),
      fetched_from_url = coalesce(excluded.fetched_from_url, core.show_images.fetched_from_url)
    returning * into inserted;

    return next inserted;
  end loop;

  return;
end $$;

create or replace function core.upsert_tmdb_show_images_by_identity(rows jsonb)
returns setof core.show_images
language plpgsql
security definer
as $$
declare
  rec jsonb;
  row_data core.show_images%rowtype;
  inserted core.show_images%rowtype;
begin
  if rows is null then
    return;
  end if;

  for rec in select * from jsonb_array_elements(rows)
  loop
    row_data := jsonb_populate_record(null::core.show_images, rec);

    insert into core.show_images (
      show_id,
      tmdb_id,
      source,
      source_image_id,
      kind,
      iso_639_1,
      file_path,
      width,
      height,
      aspect_ratio,
      vote_average,
      vote_count,
      fetched_at,
      url,
      url_path,
      caption,
      position,
      image_type,
      metadata,
      created_at,
      updated_at,
      fetch_method,
      fetched_from_url
    ) values (
      row_data.show_id,
      row_data.tmdb_id,
      row_data.source,
      row_data.source_image_id,
      row_data.kind,
      row_data.iso_639_1,
      row_data.file_path,
      row_data.width,
      row_data.height,
      row_data.aspect_ratio,
      row_data.vote_average,
      row_data.vote_count,
      coalesce(row_data.fetched_at, now()),
      row_data.url,
      row_data.url_path,
      row_data.caption,
      row_data.position,
      row_data.image_type,
      coalesce(row_data.metadata, '{}'::jsonb),
      coalesce(row_data.created_at, now()),
      coalesce(row_data.updated_at, now()),
      row_data.fetch_method,
      row_data.fetched_from_url
    )
    on conflict on constraint show_images_tmdb_source_kind_file_path_key
    do update set
      show_id = excluded.show_id,
      source_image_id = coalesce(excluded.source_image_id, core.show_images.source_image_id),
      url = core._show_images_pick_url(core.show_images.url, core.show_images.width, excluded.url, excluded.width),
      url_path = coalesce(excluded.url_path, core.show_images.url_path),
      caption = coalesce(excluded.caption, core.show_images.caption),
      position = coalesce(excluded.position, core.show_images.position),
      image_type = coalesce(excluded.image_type, core.show_images.image_type),
      metadata = coalesce(excluded.metadata, core.show_images.metadata),
      width = core._show_images_best_width(core.show_images.width, excluded.width),
      height = case
        when excluded.height is null then core.show_images.height
        when core.show_images.width is null or excluded.width is null then excluded.height
        when excluded.width >= core.show_images.width then excluded.height
        else core.show_images.height
      end,
      aspect_ratio = coalesce(excluded.aspect_ratio, core.show_images.aspect_ratio),
      vote_average = coalesce(excluded.vote_average, core.show_images.vote_average),
      vote_count = coalesce(excluded.vote_count, core.show_images.vote_count),
      fetched_at = coalesce(excluded.fetched_at, core.show_images.fetched_at),
      updated_at = coalesce(excluded.updated_at, now()),
      fetch_method = coalesce(excluded.fetch_method, core.show_images.fetch_method),
      fetched_from_url = coalesce(excluded.fetched_from_url, core.show_images.fetched_from_url)
    returning * into inserted;

    return next inserted;
  end loop;

  return;
end $$;

grant execute on function core.upsert_show_images_by_identity(jsonb) to service_role;
grant execute on function core.upsert_tmdb_show_images_by_identity(jsonb) to service_role;

commit;
