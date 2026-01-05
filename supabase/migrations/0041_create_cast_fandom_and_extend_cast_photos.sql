begin;

-- ---------------------------------------------------------------------------
-- core.cast_fandom
-- ---------------------------------------------------------------------------

create table if not exists core.cast_fandom (
  id uuid primary key default gen_random_uuid(),
  person_id uuid not null references core.people (id) on delete cascade,
  source text not null,
  source_url text not null,
  page_title text null,
  page_revision_id bigint null,
  scraped_at timestamptz not null default now(),
  full_name text null,
  birthdate date null,
  birthdate_display text null,
  gender text null,
  resides_in text null,
  hair_color text null,
  eye_color text null,
  height_display text null,
  weight_display text null,
  romances text[] null,
  family jsonb null,
  friends jsonb null,
  enemies jsonb null,
  installment text null,
  installment_url text null,
  main_seasons_display text null,
  summary text null,
  taglines jsonb null,
  reunion_seating jsonb null,
  trivia jsonb null,
  infobox_raw jsonb null,
  raw_html_sha256 text null
);

alter table core.cast_fandom
  add constraint cast_fandom_person_source_key unique (person_id, source);

create index if not exists core_cast_fandom_person_id_idx
  on core.cast_fandom (person_id);

create index if not exists core_cast_fandom_source_idx
  on core.cast_fandom (source);

grant select on table core.cast_fandom to anon, authenticated;
grant all privileges on table core.cast_fandom to service_role;

-- ---------------------------------------------------------------------------
-- Extend core.cast_photos for fandom ingestion
-- ---------------------------------------------------------------------------

alter table core.cast_photos add column if not exists source_page_url text;
alter table core.cast_photos add column if not exists image_url text;
alter table core.cast_photos add column if not exists thumb_url text;
alter table core.cast_photos add column if not exists file_name text;
alter table core.cast_photos add column if not exists alt_text text;
alter table core.cast_photos add column if not exists context_section text;
alter table core.cast_photos add column if not exists context_type text;
alter table core.cast_photos add column if not exists season integer;
alter table core.cast_photos add column if not exists position integer;
alter table core.cast_photos add column if not exists image_url_canonical text;

alter table core.cast_photos
  add constraint cast_photos_person_source_image_url_canonical_key
  unique (person_id, source, image_url_canonical);

-- ---------------------------------------------------------------------------
-- Upsert helper for fandom/canonical URLs
-- ---------------------------------------------------------------------------

create or replace function core.upsert_cast_photos_by_canonical(rows jsonb)
returns setof core.cast_photos
language plpgsql
security definer
as $$
declare
  rec jsonb;
  row_data core.cast_photos%rowtype;
  inserted core.cast_photos%rowtype;
begin
  if rows is null then
    return;
  end if;

  for rec in select * from jsonb_array_elements(rows)
  loop
    row_data := jsonb_populate_record(null::core.cast_photos, rec);

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
      coalesce(row_data.source, 'imdb'),
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
