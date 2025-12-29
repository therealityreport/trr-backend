begin;

-- Extend core.show_images to support multiple sources (TMDb + IMDb) with a shared schema.

create schema if not exists core;

alter table core.show_images add column if not exists source_image_id text;
alter table core.show_images add column if not exists url text;
alter table core.show_images add column if not exists url_path text;
alter table core.show_images add column if not exists caption text;
alter table core.show_images add column if not exists position integer;
alter table core.show_images add column if not exists metadata jsonb;
alter table core.show_images add column if not exists image_type text;
alter table core.show_images add column if not exists created_at timestamptz;
alter table core.show_images add column if not exists updated_at timestamptz;

update core.show_images
set metadata = '{}'::jsonb
where metadata is null;

update core.show_images
set source_image_id = coalesce(source_image_id, file_path),
    url_path = coalesce(url_path, file_path),
    url = coalesce(url, url_original, case when file_path is not null then 'https://image.tmdb.org/t/p/original' || file_path end),
    image_type = coalesce(image_type, kind)
where source = 'tmdb' or source is null;

update core.show_images
set created_at = coalesce(created_at, fetched_at, now()),
    updated_at = coalesce(updated_at, fetched_at, now())
where created_at is null or updated_at is null;

alter table core.show_images alter column tmdb_id drop not null;
alter table core.show_images alter column width drop not null;
alter table core.show_images alter column height drop not null;
alter table core.show_images alter column aspect_ratio drop not null;
alter table core.show_images alter column file_path drop not null;

alter table core.show_images drop constraint if exists core_show_images_kind_check;

alter table core.show_images alter column metadata set not null;
alter table core.show_images alter column metadata set default '{}'::jsonb;
alter table core.show_images alter column created_at set not null;
alter table core.show_images alter column created_at set default now();
alter table core.show_images alter column updated_at set not null;
alter table core.show_images alter column updated_at set default now();
alter table core.show_images alter column source_image_id set not null;
alter table core.show_images alter column url set not null;

drop index if exists core.core_show_images_unique;
drop index if exists core.core_show_images_tmdb_unique;

do $$
begin
  if exists (
    select 1
    from core.show_images
    group by show_id, source, source_image_id
    having count(*) > 1
  ) then
    raise exception
      'Duplicate values found in core.show_images (show_id,source,source_image_id). Resolve duplicates before applying unique index core_show_images_source_unique.';
  end if;
end $$;

create unique index if not exists core_show_images_source_unique
on core.show_images (show_id, source, source_image_id);

create index if not exists core_show_images_show_id_idx
on core.show_images (show_id);

create index if not exists core_show_images_source_show_idx
on core.show_images (source, show_id);

commit;
