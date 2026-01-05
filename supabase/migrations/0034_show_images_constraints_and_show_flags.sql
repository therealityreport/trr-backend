begin;

-- =============================================================================
-- Migration 0034: Add show_images constraints + show flags
-- =============================================================================

-- Flag TMDb-only rows pending IMDb resolution.
alter table core.shows
  add column if not exists needs_imdb_resolution boolean not null default false;

-- Backfill needs_imdb_resolution for existing rows.
update core.shows
set needs_imdb_resolution = case
  when tmdb_id is not null and (imdb_id is null or btrim(imdb_id) = '') then true
  else false
end
where needs_imdb_resolution is false;

-- Ensure no duplicates before adding unique constraints.
do $$
begin
  if exists (
    select 1
    from core.show_images
    group by show_id, source, source_image_id
    having count(*) > 1
  ) then
    raise exception
      'Duplicate core.show_images rows for (show_id, source, source_image_id). Resolve before applying 0034.';
  end if;
end $$;

-- Add named unique constraint for primary identity (show_id/source/source_image_id).
do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'show_images_show_source_source_image_id_key'
      and conrelid = 'core.show_images'::regclass
  ) then
    if to_regclass('core.core_show_images_source_unique') is not null then
      alter table core.show_images
        add constraint show_images_show_source_source_image_id_key
        unique using index core_show_images_source_unique;
    else
      alter table core.show_images
        add constraint show_images_show_source_source_image_id_key
        unique (show_id, source, source_image_id);
    end if;
  end if;
end $$;

-- Ensure no duplicates before adding TMDb identity constraint.
do $$
begin
  if exists (
    select 1
    from core.show_images
    where tmdb_id is not null and file_path is not null
    group by tmdb_id, source, kind, file_path
    having count(*) > 1
  ) then
    raise exception
      'Duplicate core.show_images rows for (tmdb_id, source, kind, file_path). Resolve before applying 0034.';
  end if;
end $$;

-- Add named unique constraint for TMDb identity (tmdb_id/source/kind/file_path).
do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'show_images_tmdb_source_kind_file_path_key'
      and conrelid = 'core.show_images'::regclass
  ) then
    alter table core.show_images
      add constraint show_images_tmdb_source_kind_file_path_key
      unique (tmdb_id, source, kind, file_path);
  end if;
end $$;

-- Enforce source_image_id for IMDb rows.
do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'show_images_imdb_requires_source_image_id_ck'
      and conrelid = 'core.show_images'::regclass
  ) then
    alter table core.show_images
      add constraint show_images_imdb_requires_source_image_id_ck
      check (source <> 'imdb' or source_image_id is not null);
  end if;
end $$;

commit;
