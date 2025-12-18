begin;

-- Re-key show images on TMDb series id for stable linkage.
-- Adds `tmdb_id` + `url_original` and updates uniqueness/indexes accordingly.

create schema if not exists core;

alter table core.show_images add column if not exists tmdb_id integer;

-- Backfill tmdb_id from core.shows when possible.
update core.show_images si
set tmdb_id = s.tmdb_id
from core.shows s
where si.tmdb_id is null
  and si.show_id = s.id
  and s.tmdb_id is not null;

-- TMDb rows must have a tmdb_id (non-TMDb sources may remain null).
do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'core_show_images_tmdb_id_required'
      and conrelid = 'core.show_images'::regclass
  ) then
    alter table core.show_images
    add constraint core_show_images_tmdb_id_required
    check ((source <> 'tmdb') or (tmdb_id is not null));
  end if;
end $$;

alter table core.show_images add column if not exists url_original text;

update core.show_images
set url_original = 'https://image.tmdb.org/t/p/original' || file_path
where url_original is null
  and coalesce(source, 'tmdb') = 'tmdb';

-- Replace uniqueness: dedupe by (tmdb_id, source, kind, file_path).
drop index if exists core.core_show_images_unique;

do $$
begin
  if exists (
    select 1
    from core.show_images
    where tmdb_id is not null
    group by tmdb_id, source, kind, file_path
    having count(*) > 1
  ) then
    raise exception
      'Duplicate values found in core.show_images (tmdb_id,source,kind,file_path). Resolve duplicates before applying unique index core_show_images_tmdb_unique.';
  end if;
end $$;

create unique index if not exists core_show_images_tmdb_unique
on core.show_images (tmdb_id, source, kind, file_path)
where tmdb_id is not null;

create index if not exists core_show_images_tmdb_kind_idx
on core.show_images (tmdb_id, kind)
where tmdb_id is not null;

create index if not exists core_show_images_tmdb_kind_lang_idx
on core.show_images (tmdb_id, kind, iso_639_1)
where tmdb_id is not null;

commit;
