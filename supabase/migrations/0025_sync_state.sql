begin;

create schema if not exists core;

create table if not exists core.sync_state (
  table_name text not null,
  show_id uuid not null references core.shows (id) on delete cascade,
  status text not null default 'in_progress'
    check (status in ('in_progress', 'success', 'failed')),
  last_success_at timestamptz,
  last_seen_most_recent_episode text,
  last_error text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique (table_name, show_id)
);

create index if not exists core_sync_state_show_id_idx
on core.sync_state (show_id);

create index if not exists core_sync_state_table_name_idx
on core.sync_state (table_name);

create index if not exists core_sync_state_status_idx
on core.sync_state (status);

drop trigger if exists core_sync_state_set_updated_at on core.sync_state;
create trigger core_sync_state_set_updated_at
before update on core.sync_state
for each row
execute function core.set_updated_at();

grant usage on schema core to anon, authenticated, service_role;
grant select on table core.sync_state to anon, authenticated;
grant all privileges on table core.sync_state to service_role;

alter table core.sync_state enable row level security;

drop policy if exists core_sync_state_public_read on core.sync_state;
create policy core_sync_state_public_read on core.sync_state
for select to anon, authenticated
using (true);

commit;
