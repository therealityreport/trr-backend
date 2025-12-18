begin;

-- Update `core.show_images` to a strict per-image-per-row TMDb schema:
-- - require `tmdb_id`
-- - require width/height/aspect_ratio
-- - generate `url_original`
-- - drop vote fields (do not store or expose)

create schema if not exists core;

-- Drop dependent view so column changes (drops/re-adds) succeed.
drop view if exists core.v_show_images;

-- Ensure required columns exist (older deployments may be missing some).
alter table core.show_images add column if not exists tmdb_id integer;
alter table core.show_images add column if not exists width integer;
alter table core.show_images add column if not exists height integer;
alter table core.show_images add column if not exists aspect_ratio numeric;

-- Backfill tmdb_id from core.shows when possible.
update core.show_images si
set tmdb_id = s.tmdb_id
from core.shows s
where si.tmdb_id is null
  and si.show_id = s.id
  and s.tmdb_id is not null;

-- Backfill required dimensions/ratio to avoid NOT NULL failures.
update core.show_images
set width = coalesce(width, 0),
    height = coalesce(height, 0)
where width is null
   or height is null;

update core.show_images
set aspect_ratio = case
    when coalesce(width, 0) > 0 and coalesce(height, 0) > 0 then (width::numeric / nullif(height, 0))
    else 0
  end
where aspect_ratio is null;

-- Drop vote fields (explicitly not stored).
alter table core.show_images drop column if exists vote_average;
alter table core.show_images drop column if exists vote_count;

-- Replace url_original with a generated stored column.
alter table core.show_images drop column if exists url_original;
alter table core.show_images
  add column if not exists url_original text generated always as ('https://image.tmdb.org/t/p/original' || file_path) stored;

-- Legacy constraint (tmdb_id required only for tmdb source) is superseded by tmdb_id NOT NULL.
alter table core.show_images drop constraint if exists core_show_images_tmdb_id_required;

-- Enforce tmdb_id NOT NULL (fail fast with a helpful message if legacy rows remain).
do $$
begin
  if exists (select 1 from core.show_images where tmdb_id is null) then
    raise exception
      'NULL values found in core.show_images.tmdb_id. Ensure core.shows.tmdb_id is populated (see supabase/migrations/0007_core_shows_tmdb_id.sql) and re-run, or delete invalid show_images rows.';
  end if;
end $$;

alter table core.show_images alter column tmdb_id set not null;
alter table core.show_images alter column width set not null;
alter table core.show_images alter column height set not null;
alter table core.show_images alter column aspect_ratio set not null;

-- Replace uniqueness: dedupe by (tmdb_id, source, kind, file_path).
drop index if exists core.core_show_images_tmdb_unique;

do $$
begin
  if exists (
    select 1
    from core.show_images
    group by tmdb_id, source, kind, file_path
    having count(*) > 1
  ) then
    raise exception
      'Duplicate values found in core.show_images (tmdb_id,source,kind,file_path). Resolve duplicates before applying unique index core_show_images_tmdb_unique.';
  end if;
end $$;

create unique index if not exists core_show_images_tmdb_unique
on core.show_images (tmdb_id, source, kind, file_path);

-- Indexes for common access patterns.
drop index if exists core.core_show_images_tmdb_kind_idx;
create index if not exists core_show_images_tmdb_kind_idx
on core.show_images (tmdb_id, kind);

create index if not exists core_show_images_show_kind_idx
on core.show_images (show_id, kind);

commit;

