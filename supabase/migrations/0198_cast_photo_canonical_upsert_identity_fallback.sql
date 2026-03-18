begin;

create or replace function core._normalize_cast_photo_canonical_url(raw_url text)
returns text
language sql
immutable
as $$
  select
    case
      when raw_url is null then null
      when btrim(raw_url) = '' then null
      else lower(split_part(btrim(raw_url), '?', 1))
    end
$$;

create or replace function core.upsert_cast_photos_by_canonical(rows jsonb)
returns setof core.cast_photos
language plpgsql
security definer
as $$
declare
  rec jsonb;
  row_data core.cast_photos%rowtype;
  inserted core.cast_photos%rowtype;
  matched_id uuid;
begin
  if rows is null then
    return;
  end if;

  for rec in select * from jsonb_array_elements(rows)
  loop
    row_data := jsonb_populate_record(null::core.cast_photos, rec);
    row_data.source := coalesce(row_data.source, 'imdb');
    row_data.image_url_canonical := core._normalize_cast_photo_canonical_url(
      coalesce(row_data.image_url_canonical, row_data.image_url, row_data.url)
    );
    matched_id := null;

    if row_data.source_image_id is not null then
      select cp.id
      into matched_id
      from core.cast_photos cp
      where cp.person_id = row_data.person_id
        and cp.source = row_data.source
        and cp.source_image_id = row_data.source_image_id
      limit 1;
    end if;

    if matched_id is null and row_data.image_url_canonical is not null then
      select cp.id
      into matched_id
      from core.cast_photos cp
      where cp.person_id = row_data.person_id
        and cp.source = row_data.source
        and (
          cp.image_url_canonical = row_data.image_url_canonical
          or core._normalize_cast_photo_canonical_url(cp.image_url_canonical) = row_data.image_url_canonical
        )
      order by
        case when cp.image_url_canonical = row_data.image_url_canonical then 0 else 1 end,
        cp.updated_at desc
      limit 1;
    end if;

    if matched_id is not null then
      update core.cast_photos
      set
        imdb_person_id = coalesce(row_data.imdb_person_id, core.cast_photos.imdb_person_id),
        viewer_id = coalesce(row_data.viewer_id, core.cast_photos.viewer_id),
        mediaindex_url_path = coalesce(row_data.mediaindex_url_path, core.cast_photos.mediaindex_url_path),
        mediaviewer_url_path = coalesce(row_data.mediaviewer_url_path, core.cast_photos.mediaviewer_url_path),
        url = core._cast_photos_pick_url(core.cast_photos.url, core.cast_photos.width, row_data.url, row_data.width),
        url_path = core._cast_photos_pick_url_path(
          core.cast_photos.url_path,
          core.cast_photos.width,
          row_data.url_path,
          row_data.width
        ),
        image_url = core._cast_photos_pick_url(
          core.cast_photos.image_url,
          core.cast_photos.width,
          row_data.image_url,
          row_data.width
        ),
        width = core._cast_photos_best_width(core.cast_photos.width, row_data.width),
        height = core._cast_photos_pick_height(
          core.cast_photos.height,
          core.cast_photos.width,
          row_data.height,
          row_data.width
        ),
        caption = coalesce(row_data.caption, core.cast_photos.caption),
        gallery_index = coalesce(row_data.gallery_index, core.cast_photos.gallery_index),
        gallery_total = coalesce(row_data.gallery_total, core.cast_photos.gallery_total),
        people_imdb_ids = coalesce(row_data.people_imdb_ids, core.cast_photos.people_imdb_ids),
        people_names = coalesce(row_data.people_names, core.cast_photos.people_names),
        title_imdb_ids = coalesce(row_data.title_imdb_ids, core.cast_photos.title_imdb_ids),
        title_names = coalesce(row_data.title_names, core.cast_photos.title_names),
        fetched_at = coalesce(row_data.fetched_at, core.cast_photos.fetched_at),
        updated_at = coalesce(row_data.updated_at, now()),
        metadata = coalesce(core.cast_photos.metadata, '{}'::jsonb)
          || coalesce(row_data.metadata, '{}'::jsonb),
        source_page_url = coalesce(row_data.source_page_url, core.cast_photos.source_page_url),
        thumb_url = coalesce(row_data.thumb_url, core.cast_photos.thumb_url),
        file_name = coalesce(row_data.file_name, core.cast_photos.file_name),
        alt_text = coalesce(row_data.alt_text, core.cast_photos.alt_text),
        context_section = coalesce(row_data.context_section, core.cast_photos.context_section),
        context_type = coalesce(row_data.context_type, core.cast_photos.context_type),
        season = coalesce(row_data.season, core.cast_photos.season),
        position = coalesce(row_data.position, core.cast_photos.position),
        image_url_canonical = coalesce(core.cast_photos.image_url_canonical, row_data.image_url_canonical)
      where core.cast_photos.id = matched_id
      returning * into inserted;

      return next inserted;
      continue;
    end if;

    insert into core.cast_photos (
      person_id,
      imdb_person_id,
      source,
      source_image_id,
      viewer_id,
      mediaindex_url_path,
      mediaviewer_url_path,
      url,
      url_path,
      width,
      height,
      caption,
      gallery_index,
      gallery_total,
      people_imdb_ids,
      people_names,
      title_imdb_ids,
      title_names,
      fetched_at,
      updated_at,
      metadata,
      source_page_url,
      image_url,
      thumb_url,
      file_name,
      alt_text,
      context_section,
      context_type,
      season,
      position,
      image_url_canonical
    ) values (
      row_data.person_id,
      row_data.imdb_person_id,
      row_data.source,
      row_data.source_image_id,
      row_data.viewer_id,
      row_data.mediaindex_url_path,
      row_data.mediaviewer_url_path,
      row_data.url,
      row_data.url_path,
      row_data.width,
      row_data.height,
      row_data.caption,
      row_data.gallery_index,
      row_data.gallery_total,
      row_data.people_imdb_ids,
      row_data.people_names,
      row_data.title_imdb_ids,
      row_data.title_names,
      coalesce(row_data.fetched_at, now()),
      coalesce(row_data.updated_at, now()),
      coalesce(row_data.metadata, '{}'::jsonb),
      row_data.source_page_url,
      row_data.image_url,
      row_data.thumb_url,
      row_data.file_name,
      row_data.alt_text,
      row_data.context_section,
      row_data.context_type,
      row_data.season,
      row_data.position,
      row_data.image_url_canonical
    )
    on conflict on constraint cast_photos_person_source_image_url_canonical_key
    do update set
      imdb_person_id = coalesce(excluded.imdb_person_id, core.cast_photos.imdb_person_id),
      viewer_id = coalesce(excluded.viewer_id, core.cast_photos.viewer_id),
      mediaindex_url_path = coalesce(excluded.mediaindex_url_path, core.cast_photos.mediaindex_url_path),
      mediaviewer_url_path = coalesce(excluded.mediaviewer_url_path, core.cast_photos.mediaviewer_url_path),
      url = core._cast_photos_pick_url(core.cast_photos.url, core.cast_photos.width, excluded.url, excluded.width),
      url_path = core._cast_photos_pick_url_path(
        core.cast_photos.url_path,
        core.cast_photos.width,
        excluded.url_path,
        excluded.width
      ),
      image_url = core._cast_photos_pick_url(
        core.cast_photos.image_url,
        core.cast_photos.width,
        excluded.image_url,
        excluded.width
      ),
      width = core._cast_photos_best_width(core.cast_photos.width, excluded.width),
      height = core._cast_photos_pick_height(
        core.cast_photos.height,
        core.cast_photos.width,
        excluded.height,
        excluded.width
      ),
      caption = coalesce(excluded.caption, core.cast_photos.caption),
      gallery_index = coalesce(excluded.gallery_index, core.cast_photos.gallery_index),
      gallery_total = coalesce(excluded.gallery_total, core.cast_photos.gallery_total),
      people_imdb_ids = coalesce(excluded.people_imdb_ids, core.cast_photos.people_imdb_ids),
      people_names = coalesce(excluded.people_names, core.cast_photos.people_names),
      title_imdb_ids = coalesce(excluded.title_imdb_ids, core.cast_photos.title_imdb_ids),
      title_names = coalesce(excluded.title_names, core.cast_photos.title_names),
      fetched_at = coalesce(excluded.fetched_at, core.cast_photos.fetched_at),
      updated_at = coalesce(excluded.updated_at, now()),
      metadata = coalesce(core.cast_photos.metadata, '{}'::jsonb)
        || coalesce(excluded.metadata, '{}'::jsonb),
      source_page_url = coalesce(excluded.source_page_url, core.cast_photos.source_page_url),
      thumb_url = coalesce(excluded.thumb_url, core.cast_photos.thumb_url),
      file_name = coalesce(excluded.file_name, core.cast_photos.file_name),
      alt_text = coalesce(excluded.alt_text, core.cast_photos.alt_text),
      context_section = coalesce(excluded.context_section, core.cast_photos.context_section),
      context_type = coalesce(excluded.context_type, core.cast_photos.context_type),
      season = coalesce(excluded.season, core.cast_photos.season),
      position = coalesce(excluded.position, core.cast_photos.position),
      image_url_canonical = coalesce(excluded.image_url_canonical, core.cast_photos.image_url_canonical)
    returning * into inserted;

    return next inserted;
  end loop;

  return;
end $$;

grant execute on function core.upsert_cast_photos_by_canonical(jsonb) to service_role;

commit;
