begin;

create schema if not exists admin;

create table if not exists admin.network_streaming_sync_runs (
  run_id text primary key,
  status text not null check (status in ('running', 'completed', 'failed', 'stopped')),
  started_at timestamptz not null,
  finished_at timestamptz,
  cursor_entity_type text,
  cursor_entity_key text,
  processed int not null default 0,
  links_enriched int not null default 0,
  wikidata_linked int not null default 0,
  wikipedia_linked int not null default 0,
  logos_mirrored int not null default 0,
  variants_black_mirrored int not null default 0,
  variants_white_mirrored int not null default 0,
  logo_assets_discovered int not null default 0,
  logo_assets_mirrored int not null default 0,
  logo_assets_skipped int not null default 0,
  logo_assets_failed int not null default 0,
  show_logos_discovered int not null default 0,
  show_logos_imported int not null default 0,
  show_logos_skipped int not null default 0,
  show_logo_failures int not null default 0,
  completion_total int not null default 0,
  completion_resolved int not null default 0,
  completion_unresolved int not null default 0,
  completion_percent numeric(6,2) not null default 0,
  failures int not null default 0,
  error_message text
);

create index if not exists network_streaming_sync_runs_status_started_idx
  on admin.network_streaming_sync_runs (status, started_at desc);

create index if not exists network_streaming_sync_runs_finished_idx
  on admin.network_streaming_sync_runs (finished_at desc);

grant usage on schema admin to service_role;
grant all privileges on table admin.network_streaming_sync_runs to service_role;

commit;
