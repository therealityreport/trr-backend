alter table if exists ml.screentime_runs
  add column if not exists result_contract_version text null,
  add column if not exists status_reason text null,
  add column if not exists summary_counts jsonb not null default '{}'::jsonb,
  add column if not exists result_ingest_status text null,
  add column if not exists result_ingested_at timestamptz null,
  add column if not exists result_ingest_error text null;

create index if not exists ml_screentime_runs_result_ingest_status_idx
  on ml.screentime_runs (result_ingest_status, completed_at desc);
