begin;

-- Apply proof is independent of worker leases, generations and completion.
-- Dry runs persist only their idempotency response in the command ledger.
create table social.instagram_ingestion_replay_receipts (
  receipt_id uuid primary key,
  account text not null,
  idempotency_key text not null,
  run_id uuid not null,
  evidence_id uuid not null,
  stage text not null check (stage in ('listing','post_details','comments','media')),
  target_key text not null,
  captured_at timestamptz not null,
  content_hash text not null,
  capture_parser_version text not null,
  parser_version text not null,
  schema_version integer not null check (schema_version > 0),
  canonical_identity jsonb not null,
  writes integer not null check (writes >= 0),
  initiated_by text not null,
  committed_at timestamptz not null default clock_timestamp(),
  unique(account,idempotency_key),
  foreign key(evidence_id,run_id,stage,target_key)
    references social.instagram_ingestion_evidence(evidence_id,run_id,stage,target_key),
  foreign key(account,idempotency_key)
    references social.instagram_ingestion_commands(account,idempotency_key)
    deferrable initially deferred
);
create index instagram_ingestion_replay_receipts_run_idx
  on social.instagram_ingestion_replay_receipts(run_id,committed_at desc);
alter table social.instagram_ingestion_replay_receipts enable row level security;
revoke all on social.instagram_ingestion_replay_receipts from public,anon,authenticated;
grant all on social.instagram_ingestion_replay_receipts to service_role;

commit;
