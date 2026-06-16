create table if not exists core.admin_runtime_settings (
  key text primary key,
  value jsonb not null default '{}'::jsonb,
  updated_at timestamptz not null default now(),
  updated_by text null
);

alter table core.admin_runtime_settings enable row level security;

drop policy if exists admin_runtime_settings_service_role_all on core.admin_runtime_settings;
create policy admin_runtime_settings_service_role_all
on core.admin_runtime_settings
for all
to service_role
using (true)
with check (true);
