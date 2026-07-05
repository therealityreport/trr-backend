-- Codify social.instagram_comments_public_proxy_budget_ledger.
--
-- The table already exists in production (created ad-hoc during prior planning,
-- 0 rows) but had no tracked migration. This migration is idempotent
-- (create table / index / policy IF NOT EXISTS) so it is a no-op against the
-- live project while giving fresh environments and CI the canonical schema.
--
-- Purpose: per-run bandwidth ledger + kill-switch for the budgeted
-- public-comments Decodo proxy fan-out (Phase 3 of the throughput plan). Counts
-- proxied JSON response bytes, derives estimated USD from a configured $/GB, and
-- records whether a run tripped its RUN budget (falling back to direct egress).
-- Enforcement is run-scoped; there is no daily/cross-run cap.

create schema if not exists social;

create table if not exists social.instagram_comments_public_proxy_budget_ledger (
  id bigint generated always as identity primary key,
  run_id uuid,
  job_id uuid,
  account_handle text not null,
  comments_load_strategy text not null default 'public_relay',
  proxy_state text not null default 'budgeted_public_proxy',
  proxy_provider text,
  proxy_fingerprint text,
  proxy_session_mode text,
  http_client text,
  rate_scope text,
  request_count integer not null default 0,
  proxy_bytes_total bigint not null default 0,
  proxy_cdn_bytes_leak bigint not null default 0,
  proxy_bytes_by_host jsonb not null default '{}'::jsonb,
  proxy_cdn_bytes_leak_by_host jsonb not null default '{}'::jsonb,
  usd_per_gb numeric,
  estimated_usd numeric,
  budget_usd numeric,
  budget_exhausted boolean not null default false,
  metadata jsonb not null default '{}'::jsonb,
  recorded_at timestamptz not null default now()
);

-- Daily-spend rollups filter by account + recorded_at; run-scoped reads by run_id.
create index if not exists ig_comments_proxy_budget_ledger_run_idx
  on social.instagram_comments_public_proxy_budget_ledger (run_id);
create index if not exists ig_comments_proxy_budget_ledger_recorded_idx
  on social.instagram_comments_public_proxy_budget_ledger (recorded_at);
create index if not exists ig_comments_proxy_budget_ledger_account_recorded_idx
  on social.instagram_comments_public_proxy_budget_ledger (account_handle, recorded_at);

alter table social.instagram_comments_public_proxy_budget_ledger enable row level security;

grant all privileges on table social.instagram_comments_public_proxy_budget_ledger to service_role;
revoke all on table social.instagram_comments_public_proxy_budget_ledger from anon, authenticated;

drop policy if exists ig_comments_proxy_budget_ledger_service_role_all
  on social.instagram_comments_public_proxy_budget_ledger;
create policy ig_comments_proxy_budget_ledger_service_role_all
on social.instagram_comments_public_proxy_budget_ledger
for all
to service_role
using (true)
with check (true);
