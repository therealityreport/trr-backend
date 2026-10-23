begin;

-- Operator-owned execution settings do not modify immutable source intent or
-- generation, targets, attempts, leases, receipts, or canonical business rows.
create table social.instagram_ingestion_execution_policies (
  run_id uuid primary key references social.instagram_ingestion_runs(run_id) on delete restrict,
  account text not null check (account ~ '^[a-z0-9._]{1,30}$'),
  generation bigint not null check (generation >= 1),
  intent_hash text not null check (intent_hash ~ '^[a-f0-9]{64}$'),
  execution_placement text not null check (execution_placement = 'local'),
  revision bigint not null check (revision >= 1),
  comments_parallelism integer not null check (comments_parallelism in (1,2,4)),
  updated_by text not null check (length(btrim(updated_by)) between 1 and 200),
  updated_at timestamptz not null default clock_timestamp()
);

comment on table social.instagram_ingestion_execution_policies is
  'Revision-CAS local execution limits bound to the exact run/account/generation/intent; ingestion_commands owns command idempotency and audit.';

alter table social.instagram_ingestion_execution_policies enable row level security;
revoke all on social.instagram_ingestion_execution_policies from public, anon, authenticated;
grant all on social.instagram_ingestion_execution_policies to service_role;

commit;
