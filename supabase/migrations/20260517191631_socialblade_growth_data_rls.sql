begin;

grant usage on schema pipeline to service_role;

grant select, insert, update, delete
  on table pipeline.socialblade_growth_data
  to service_role;

revoke all on table pipeline.socialblade_growth_data
  from public, anon, authenticated;

alter table pipeline.socialblade_growth_data enable row level security;

drop policy if exists socialblade_growth_data_service_role_all
  on pipeline.socialblade_growth_data;

create policy socialblade_growth_data_service_role_all
  on pipeline.socialblade_growth_data
  as permissive
  for all
  to service_role
  using (true)
  with check (true);

commit;
