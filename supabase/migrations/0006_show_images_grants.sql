begin;

-- Grants + RLS policies for `core.show_images`.
-- Required because grants in 0001_init.sql do not automatically apply to tables created later.

create schema if not exists core;

-- Ensure Supabase API roles can use the schema.
grant usage on schema core to anon, authenticated, service_role;

-- Public read (matches `core.shows` pattern).
grant select on table core.show_images to anon, authenticated;

-- Service role can manage everything (scripts/admin jobs).
grant all privileges on table core.show_images to service_role;

-- RLS (consistent with other `core.*` tables).
alter table core.show_images enable row level security;

drop policy if exists core_show_images_public_read on core.show_images;
create policy core_show_images_public_read on core.show_images
for select to anon, authenticated
using (true);

commit;

