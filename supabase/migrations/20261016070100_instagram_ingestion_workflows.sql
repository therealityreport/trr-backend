begin;

-- Engine-independent continuation/outbox. A generation never changes engine/version.
create table social.instagram_ingestion_partitions (
  execution_id text primary key,
  run_id uuid not null references social.instagram_ingestion_runs(run_id),
  generation bigint not null,
  partition_number bigint not null check (partition_number >= 0),
  engine text not null check (engine = 'dbos'),
  workflow_version text not null,
  available_at timestamptz not null default clock_timestamp(),
  acknowledged_at timestamptz,
  completed_at timestamptz,
  next_execution_id text,
  unique(run_id,generation,partition_number)
);
create index instagram_ingestion_partitions_pending_idx
  on social.instagram_ingestion_partitions(available_at) where acknowledged_at is null;

create table social.instagram_ingestion_decisions (
  execution_id text not null references social.instagram_ingestion_partitions(execution_id),
  ordinal integer not null check (ordinal >= 0 and ordinal < 50),
  decision jsonb not null,
  primary key(execution_id,ordinal)
);

-- Persisted before remote submission. An ambiguous send is reconciled, never
-- retried on an engine exception. The target owner token is the remote identity.
create table social.instagram_ingestion_dispatches (
  owner_token uuid primary key,
  run_id uuid not null,
  generation bigint not null,
  stage text not null,
  target_key text not null,
  state text not null default 'reserved' check(state in ('reserved','sending','accepted','expired')),
  remote_identity text,
  created_at timestamptz not null default clock_timestamp(),
  updated_at timestamptz not null default clock_timestamp(),
  foreign key(run_id,stage,target_key)
    references social.instagram_ingestion_targets(run_id,stage,target_key)
);
create index instagram_ingestion_dispatches_target_idx
  on social.instagram_ingestion_dispatches(run_id,stage,target_key,created_at desc);

do $$
declare name text;
begin
  foreach name in array array['instagram_ingestion_partitions','instagram_ingestion_decisions',
    'instagram_ingestion_dispatches'] loop
    execute format('alter table social.%I enable row level security',name);
    execute format('revoke all on social.%I from public,anon,authenticated',name);
    execute format('grant all on social.%I to service_role',name);
  end loop;
end $$;
commit;
