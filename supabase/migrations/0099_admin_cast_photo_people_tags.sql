-- Admin schema: manual/auto people tags and counts for cast photos
begin;

create schema if not exists admin;

create table if not exists admin.cast_photo_people_tags (
  cast_photo_id uuid primary key references core.cast_photos(id) on delete cascade,
  people_names text[] null,
  people_ids text[] null,
  people_count int null,
  people_count_source text null check (people_count_source in ('manual', 'auto')),
  detector text null,
  created_at timestamptz default now(),
  updated_at timestamptz default now(),
  created_by_firebase_uid text,
  updated_by_firebase_uid text
);

create index if not exists cast_photo_people_tags_people_ids_idx
  on admin.cast_photo_people_tags using gin (people_ids);

create index if not exists cast_photo_people_tags_people_names_idx
  on admin.cast_photo_people_tags using gin (people_names);

-- Grants: admin table only accessible by service_role
grant usage on schema admin to service_role;
grant all privileges on table admin.cast_photo_people_tags to service_role;

commit;
