-- Migration: Screenalytics v1 operational tables (run/job persistence + smart suggestions)
--
-- NOTE: v1 uses screenalytics-internal identifiers (ep_id tokens, cast_id strings).
-- These are not core.episodes.id / core.people.id.

begin;

create schema if not exists screenalytics;

create table screenalytics.runs (
  run_id text primary key,
  ep_id text not null, -- screenalytics internal episode token (not core.episodes.id)
  created_at timestamptz not null default now(),
  label text,
  stage_state_json jsonb,
  config_json jsonb
);

create index idx_sa_runs_ep_id on screenalytics.runs (ep_id);

create table screenalytics.job_runs (
  job_run_id uuid primary key default gen_random_uuid(),
  run_id text not null references screenalytics.runs (run_id) on delete cascade,
  ep_id text not null,
  job_name text not null,
  request_json jsonb not null default '{}'::jsonb,
  status text not null,
  started_at timestamptz,
  finished_at timestamptz,
  error_text text,
  artifact_index_json jsonb,
  metrics_json jsonb
);

create index idx_sa_job_runs_run_id on screenalytics.job_runs (run_id);
create index idx_sa_job_runs_ep_id on screenalytics.job_runs (ep_id);
create index idx_sa_job_runs_job_name on screenalytics.job_runs (job_name);

create table screenalytics.identity_locks (
  ep_id text not null,
  run_id text not null references screenalytics.runs (run_id) on delete cascade,
  identity_id text not null,
  locked boolean not null default true,
  locked_at timestamptz,
  locked_by text,
  reason text,
  primary key (ep_id, run_id, identity_id)
);

create index idx_sa_identity_locks_ep_run on screenalytics.identity_locks (ep_id, run_id);

create table screenalytics.suggestion_batches (
  batch_id uuid primary key default gen_random_uuid(),
  ep_id text not null,
  run_id text not null references screenalytics.runs (run_id) on delete cascade,
  created_at timestamptz not null default now(),
  generator_version text not null,
  generator_config_json jsonb not null default '{}'::jsonb,
  summary_json jsonb
);

create index idx_sa_suggestion_batches_ep_run on screenalytics.suggestion_batches (ep_id, run_id);

create table screenalytics.suggestions (
  suggestion_id uuid primary key default gen_random_uuid(),
  batch_id uuid not null references screenalytics.suggestion_batches (batch_id) on delete cascade,
  ep_id text not null,
  run_id text not null,
  type text not null,
  target_identity_id text,
  suggested_person_id text not null,
  confidence double precision not null default 0,
  evidence_json jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default now(),
  dismissed boolean not null default false,
  dismissed_at timestamptz
);

create index idx_sa_suggestions_batch_id on screenalytics.suggestions (batch_id);
create index idx_sa_suggestions_ep_run on screenalytics.suggestions (ep_id, run_id);

create table screenalytics.suggestion_applies (
  apply_id uuid primary key default gen_random_uuid(),
  batch_id uuid not null references screenalytics.suggestion_batches (batch_id) on delete cascade,
  suggestion_id uuid references screenalytics.suggestions (suggestion_id) on delete set null,
  ep_id text not null,
  run_id text not null,
  applied_at timestamptz not null default now(),
  applied_by text,
  changes_json jsonb not null default '{}'::jsonb
);

create index idx_sa_suggestion_applies_batch_id on screenalytics.suggestion_applies (batch_id);
create index idx_sa_suggestion_applies_ep_run on screenalytics.suggestion_applies (ep_id, run_id);

-- Grants (service_role only)
grant usage on schema screenalytics to service_role;
grant all privileges on table screenalytics.runs to service_role;
grant all privileges on table screenalytics.job_runs to service_role;
grant all privileges on table screenalytics.identity_locks to service_role;
grant all privileges on table screenalytics.suggestion_batches to service_role;
grant all privileges on table screenalytics.suggestions to service_role;
grant all privileges on table screenalytics.suggestion_applies to service_role;

-- RLS with explicit service_role policies
alter table screenalytics.runs enable row level security;
alter table screenalytics.job_runs enable row level security;
alter table screenalytics.identity_locks enable row level security;
alter table screenalytics.suggestion_batches enable row level security;
alter table screenalytics.suggestions enable row level security;
alter table screenalytics.suggestion_applies enable row level security;

create policy "service_role_all_runs"
on screenalytics.runs for all to service_role
using (true) with check (true);

create policy "service_role_all_job_runs"
on screenalytics.job_runs for all to service_role
using (true) with check (true);

create policy "service_role_all_identity_locks"
on screenalytics.identity_locks for all to service_role
using (true) with check (true);

create policy "service_role_all_suggestion_batches"
on screenalytics.suggestion_batches for all to service_role
using (true) with check (true);

create policy "service_role_all_suggestions"
on screenalytics.suggestions for all to service_role
using (true) with check (true);

create policy "service_role_all_suggestion_applies"
on screenalytics.suggestion_applies for all to service_role
using (true) with check (true);

commit;

