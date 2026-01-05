begin;

-- ---------------------------------------------------------------------------
-- core.cast_photos
-- ---------------------------------------------------------------------------

-- Ensure updated_at helper exists (shared across core tables).
create or replace function core.set_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;

create table if not exists core.cast_photos (
  id uuid primary key default gen_random_uuid(),
  person_id uuid not null references core.people (id) on delete cascade,
  imdb_person_id text not null,
  source text not null default 'imdb',
  source_image_id text not null,
  viewer_id text null,
  mediaindex_url_path text null,
  mediaviewer_url_path text null,
  url text not null,
  url_path text not null,
  width integer null,
  height integer null,
  caption text null,
  gallery_index integer null,
  gallery_total integer null,
  people_imdb_ids text[] null,
  people_names text[] null,
  title_imdb_ids text[] null,
  title_names text[] null,
  fetched_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  metadata jsonb null
);

alter table core.cast_photos
  add constraint cast_photos_person_source_source_image_id_key
  unique (person_id, source, source_image_id);

create index if not exists core_cast_photos_imdb_person_id_idx
  on core.cast_photos (imdb_person_id);

create index if not exists core_cast_photos_source_source_image_id_idx
  on core.cast_photos (source, source_image_id);

create index if not exists core_cast_photos_person_id_idx
  on core.cast_photos (person_id);

alter table core.cast_photos
  add constraint cast_photos_source_image_id_chk
  check (source <> 'imdb' or source_image_id is not null);

-- Maintain updated_at automatically on updates.
drop trigger if exists core_cast_photos_set_updated_at on core.cast_photos;
create trigger core_cast_photos_set_updated_at
before update on core.cast_photos
for each row
execute function core.set_updated_at();

-- ---------------------------------------------------------------------------
-- Upsert helpers (do not downgrade image quality)
-- ---------------------------------------------------------------------------

create or replace function core._cast_photos_best_width(current_width int, incoming_width int)
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

create or replace function core._cast_photos_pick_url(
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

create or replace function core._cast_photos_pick_url_path(
  current_url_path text,
  current_width int,
  incoming_url_path text,
  incoming_width int
) returns text
language sql
immutable
as $$
  select
    case
      when incoming_url_path is null then current_url_path
      when current_url_path is null then incoming_url_path
      when incoming_width is null then current_url_path
      when current_width is null then incoming_url_path
      when incoming_width >= current_width then incoming_url_path
      else current_url_path
    end
$$;

create or replace function core._cast_photos_pick_height(
  current_height int,
  current_width int,
  incoming_height int,
  incoming_width int
) returns int
language sql
immutable
as $$
  select
    case
      when incoming_height is null then current_height
      when current_height is null then incoming_height
      when current_width is null or incoming_width is null then incoming_height
      when incoming_width >= current_width then incoming_height
      else current_height
    end
$$;

create or replace function core.upsert_cast_photos_by_identity(rows jsonb)
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
      metadata
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
      coalesce(row_data.metadata, '{}'::jsonb)
    )
    on conflict on constraint cast_photos_person_source_source_image_id_key
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
        || coalesce(excluded.metadata, '{}'::jsonb)
    returning * into inserted;

    return next inserted;
  end loop;

  return;
end $$;

grant select on table core.cast_photos to anon, authenticated;
grant all privileges on table core.cast_photos to service_role;
grant execute on function core.upsert_cast_photos_by_identity(jsonb) to service_role;

commit;
