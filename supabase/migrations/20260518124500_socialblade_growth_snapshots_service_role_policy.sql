begin;

grant usage on schema pipeline to service_role;

grant select, insert, update, delete
  on table pipeline.socialblade_growth_snapshots
  to service_role;

revoke all on table pipeline.socialblade_growth_snapshots
  from public, anon, authenticated;

alter table pipeline.socialblade_growth_snapshots enable row level security;

drop policy if exists socialblade_growth_snapshots_service_role_all
  on pipeline.socialblade_growth_snapshots;

create policy socialblade_growth_snapshots_service_role_all
  on pipeline.socialblade_growth_snapshots
  as permissive
  for all
  to service_role
  using (true)
  with check (true);

commit;
