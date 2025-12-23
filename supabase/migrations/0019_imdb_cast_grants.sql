begin;

grant usage on schema core to anon, authenticated, service_role;

grant select on table
  core.show_cast,
  core.episode_appearances
to anon, authenticated;

grant all privileges on table
  core.show_cast,
  core.episode_appearances
to service_role;

alter table core.show_cast enable row level security;
alter table core.episode_appearances enable row level security;

drop policy if exists core_show_cast_public_read on core.show_cast;
create policy core_show_cast_public_read on core.show_cast
for select to anon, authenticated
using (true);

drop policy if exists core_episode_appearances_public_read on core.episode_appearances;
create policy core_episode_appearances_public_read on core.episode_appearances
for select to anon, authenticated
using (true);

commit;
