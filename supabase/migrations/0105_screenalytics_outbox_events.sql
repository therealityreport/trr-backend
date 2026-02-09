-- Migration: Screenalytics outbox pattern table for reliable event delivery

begin;

create schema if not exists screenalytics;

create table screenalytics.outbox_events (
  event_id uuid primary key default gen_random_uuid(),
  event_type text not null,
  aggregate_id text not null,
  payload_json jsonb not null,
  created_at timestamptz not null default now(),
  delivered_at timestamptz,
  delivery_attempts integer not null default 0,
  last_error text
);

create index idx_sa_outbox_events_undelivered
  on screenalytics.outbox_events (created_at)
  where delivered_at is null;

-- Grants (service_role only)
grant usage on schema screenalytics to service_role;
grant all privileges on table screenalytics.outbox_events to service_role;

-- RLS with explicit service_role policies
alter table screenalytics.outbox_events enable row level security;

create policy "service_role_all_outbox_events"
on screenalytics.outbox_events for all to service_role
using (true) with check (true);

commit;

