-- Additive admin operation tracking + SSE replay log for resumable admin workflows.

create table if not exists core.admin_operations (
  id uuid primary key default gen_random_uuid(),
  operation_type text not null,
  status text not null check (status in ('pending', 'running', 'completed', 'failed', 'cancelled', 'cancelling')),
  initiated_by text,
  request_id text,
  client_session_id text,
  client_workflow_id text,
  request_payload jsonb not null default '{}'::jsonb,
  progress_payload jsonb not null default '{}'::jsonb,
  result_payload jsonb,
  error_payload jsonb,
  cancel_requested_at timestamptz,
  started_at timestamptz,
  completed_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists idx_admin_operations_status_created_at
  on core.admin_operations(status, created_at desc);

create index if not exists idx_admin_operations_operation_type_created_at
  on core.admin_operations(operation_type, created_at desc);

create index if not exists idx_admin_operations_client_session_created_at
  on core.admin_operations(client_session_id, created_at desc)
  where client_session_id is not null;

create index if not exists idx_admin_operations_client_workflow_status
  on core.admin_operations(client_session_id, client_workflow_id, status, created_at desc)
  where client_session_id is not null and client_workflow_id is not null;

create table if not exists core.admin_operation_events (
  id bigint generated always as identity primary key,
  operation_id uuid not null references core.admin_operations(id) on delete cascade,
  event_seq bigint,
  event_type text not null,
  event_payload jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  constraint admin_operation_events_op_seq_unique unique (operation_id, event_seq)
);

create or replace function core.set_admin_operation_event_seq()
returns trigger
language plpgsql
as $$
begin
  if new.event_seq is null then
    select coalesce(max(e.event_seq), 0) + 1
      into new.event_seq
    from core.admin_operation_events e
    where e.operation_id = new.operation_id;
  end if;
  return new;
end;
$$;

drop trigger if exists core_admin_operation_events_set_seq on core.admin_operation_events;
create trigger core_admin_operation_events_set_seq
before insert on core.admin_operation_events
for each row
execute function core.set_admin_operation_event_seq();

create index if not exists idx_admin_operation_events_operation_seq
  on core.admin_operation_events(operation_id, event_seq);

create index if not exists idx_admin_operation_events_created_at
  on core.admin_operation_events(created_at);

drop trigger if exists core_admin_operations_set_updated_at on core.admin_operations;
create trigger core_admin_operations_set_updated_at
before update on core.admin_operations
for each row
execute function core.set_updated_at();

create or replace function core.purge_admin_operations(retention interval default interval '14 days')
returns integer
language plpgsql
as $$
declare
  deleted_count integer := 0;
begin
  with deleted_rows as (
    delete from core.admin_operations
    where created_at < now() - retention
      and status in ('completed', 'failed', 'cancelled')
    returning id
  )
  select count(*) into deleted_count from deleted_rows;

  return deleted_count;
end;
$$;
