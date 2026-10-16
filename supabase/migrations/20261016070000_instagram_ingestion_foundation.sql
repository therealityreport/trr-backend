begin;

-- Additive, disabled-until-rollout foundation. Existing runs retain their owner.
create table social.instagram_ingestion_runs (
  run_id uuid primary key references social.scrape_runs(id) on delete restrict,
  account text not null,
  intent jsonb not null,
  intent_hash text not null,
  engine text not null check (engine in ('dbos', 'temporal')),
  workflow_version text not null,
  state text not null default 'queued'
    check (state in ('queued', 'running', 'blocked', 'cancelled', 'completed', 'failed')),
  generation bigint not null default 1,
  execution_selection jsonb,
  last_committed_at timestamptz,
  created_at timestamptz not null default clock_timestamp(),
  updated_at timestamptz not null default clock_timestamp()
);
create index instagram_ingestion_runs_account_idx
  on social.instagram_ingestion_runs(account, created_at desc);

create table social.instagram_ingestion_commands (
  account text not null,
  idempotency_key text not null,
  command_hash text not null,
  run_id uuid not null references social.instagram_ingestion_runs(run_id),
  result jsonb not null,
  created_at timestamptz not null default clock_timestamp(),
  primary key (account, idempotency_key)
);

create table social.instagram_ingestion_outbox (
  execution_id text primary key,
  run_id uuid not null references social.instagram_ingestion_runs(run_id),
  generation bigint not null,
  available_at timestamptz not null default clock_timestamp(),
  acknowledged_at timestamptz,
  attempts integer not null default 0 check (attempts >= 0),
  unique (run_id, generation)
);
create index instagram_ingestion_outbox_pending_idx
  on social.instagram_ingestion_outbox(available_at) where acknowledged_at is null;

create table social.instagram_ingestion_manifests (
  run_id uuid not null references social.instagram_ingestion_runs(run_id),
  stage text not null check (stage in ('listing', 'post_details', 'comments', 'media')),
  discovery_complete boolean not null default false,
  cursor_state jsonb not null default '{}'::jsonb,
  sealed_at timestamptz,
  primary key (run_id, stage),
  check (discovery_complete = (sealed_at is not null))
);

create table social.instagram_ingestion_targets (
  run_id uuid not null,
  stage text not null,
  target_key text not null,
  outcome text not null default 'unresolved'
    check (outcome in ('unresolved', 'committed', 'cached_satisfied', 'source_unavailable')),
  attempts integer not null default 0 check (attempts >= 0),
  requests integer not null default 0 check (requests >= 0),
  max_attempts integer not null check (max_attempts > 0),
  next_attempt_at timestamptz,
  blocked_reason text,
  owner_token uuid,
  owner_generation bigint not null default 0,
  owner_expires_at timestamptz,
  owner_started_at timestamptz,
  checkpoint jsonb not null default '{}'::jsonb,
  observed_at timestamptz,
  committed_at timestamptz,
  primary key (run_id, stage, target_key),
  foreign key (run_id, stage) references social.instagram_ingestion_manifests(run_id, stage),
  check (stage <> 'post_details' or max_attempts = 3),
  check (outcome <> 'committed' or (observed_at is not null and committed_at is not null))
);
create index instagram_ingestion_targets_ready_idx
  on social.instagram_ingestion_targets(run_id, stage, next_attempt_at)
  where outcome = 'unresolved';
create index instagram_ingestion_targets_ordered_idx
  on social.instagram_ingestion_targets(run_id, stage, target_key)
  where outcome = 'unresolved';

create table social.instagram_ingestion_discovery_pages (
  run_id uuid not null,
  stage text not null,
  page_id text not null,
  request_hash text not null,
  committed_at timestamptz not null default clock_timestamp(),
  primary key (run_id,stage,page_id),
  foreign key (run_id,stage) references social.instagram_ingestion_manifests(run_id,stage)
);

create table social.instagram_ingestion_evidence (
  evidence_id uuid primary key,
  run_id uuid not null,
  stage text not null,
  target_key text not null,
  source_identity text not null,
  captured_at timestamptz not null,
  expires_at timestamptz not null,
  content_hash text not null,
  parser_version text not null,
  schema_version integer not null,
  -- Canonical compact JSON text: same UTF-8 bytes at capture and storage.
  payload text,
  foreign key (run_id, stage, target_key)
    references social.instagram_ingestion_targets(run_id, stage, target_key),
  unique(evidence_id,run_id,stage,target_key),
  check (expires_at = captured_at + interval '720 hours'),
  check (payload is null or (octet_length(payload) <= 4194304 and jsonb_typeof(payload::jsonb)='object'))
);
create index instagram_ingestion_evidence_expiry_idx
  on social.instagram_ingestion_evidence(expires_at) where payload is not null;

create table social.instagram_ingestion_receipts (
  receipt_id uuid primary key,
  run_id uuid not null,
  stage text not null,
  target_key text not null,
  owner_token uuid not null,
  owner_generation bigint not null,
  evidence_id uuid,
  observed_at timestamptz not null,
  committed_at timestamptz not null default clock_timestamp(),
  outcome text not null check (outcome in ('committed', 'cached_satisfied', 'source_unavailable')),
  canonical_identity jsonb not null,
  foreign key (run_id, stage, target_key)
    references social.instagram_ingestion_targets(run_id, stage, target_key),
  foreign key (evidence_id,run_id,stage,target_key)
    references social.instagram_ingestion_evidence(evidence_id,run_id,stage,target_key),
  unique (run_id, stage, target_key, owner_generation)
);

-- No public/API-role visibility, including source evidence. Backend role only.
do $$
declare name text;
begin
  foreach name in array array[
    'instagram_ingestion_runs', 'instagram_ingestion_commands', 'instagram_ingestion_outbox',
    'instagram_ingestion_manifests', 'instagram_ingestion_targets',
    'instagram_ingestion_evidence', 'instagram_ingestion_receipts', 'instagram_ingestion_discovery_pages'
  ] loop
    execute format('alter table social.%I enable row level security', name);
    execute format('revoke all on social.%I from public, anon, authenticated', name);
    execute format('grant all on social.%I to service_role', name);
  end loop;
end $$;

commit;
