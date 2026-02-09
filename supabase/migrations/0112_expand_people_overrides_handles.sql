-- Migration 0112: Expand core.people_overrides social handles

begin;

-- Some environments may have schema drift where core.people_overrides is
-- missing even though earlier migrations were recorded. Ensure it exists.
create table if not exists core.people_overrides (
  id uuid primary key default gen_random_uuid(),
  person_id uuid not null references core.people (id) on delete cascade,
  full_name_override text,
  instagram_handle text,
  external_ids_override jsonb not null default '{}'::jsonb,
  notes text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (person_id)
);

create index if not exists idx_people_overrides_person_id
  on core.people_overrides (person_id);

drop trigger if exists people_overrides_set_updated_at on core.people_overrides;
create trigger people_overrides_set_updated_at
before update on core.people_overrides
for each row execute function core.set_updated_at();

grant select on core.people_overrides to service_role;
grant insert, update, delete on core.people_overrides to service_role;

alter table core.people_overrides
  add column if not exists tiktok_handle text,
  add column if not exists twitter_handle text,
  add column if not exists youtube_handle text;

commit;
