begin;

-- Account-scoped auth cooldown for the social posts-backfill reliability layer.
--
-- Partition runners execute in SEPARATE Modal containers, so an in-process
-- cooldown cannot be shared across them. This table is the cross-process source
-- of truth: when a posts/comments lane classifies a hard 401/403 (or a
-- checkpoint/challenge), it records an escalating cooldown keyed on
-- (platform, account_handle). Readers (the fetcher pacing path, the job runner,
-- and the dispatch guard) consult cooldown_until before issuing further
-- authenticated requests for that account, and clear the row on a clean fetch.
create table if not exists social.account_auth_cooldown (
  platform text not null,
  account_handle text not null,
  cooldown_until timestamptz,
  consecutive_auth_failures integer not null default 0 check (consecutive_auth_failures >= 0),
  last_error_code text,
  -- checkpoint/challenge blocks must NOT auto-rotate-retry; flag them so the
  -- dispatch guard and job runner can surface them as a hard blocker that needs
  -- operator intervention rather than a transient backoff.
  blocker_kind text not null default 'auth' check (blocker_kind in ('auth', 'checkpoint')),
  updated_at timestamptz not null default now(),
  primary key (platform, account_handle)
);

-- Dispatch guard / recovery sweep lookup: "which accounts are still cooling
-- down, soonest first?" Partial so cleared rows do not bloat the index.
create index if not exists account_auth_cooldown_active_idx
  on social.account_auth_cooldown (platform, cooldown_until)
  where cooldown_until is not null;

comment on table social.account_auth_cooldown is
  'Cross-process (Modal-container-safe) account auth cooldown for the social posts/comments backfill reliability layer. Keyed on (platform, account_handle).';

comment on column social.account_auth_cooldown.cooldown_until is
  'When set and in the future, no authenticated request should be issued for this account; the job soft-stops and requeues with available_at = cooldown_until.';

comment on column social.account_auth_cooldown.consecutive_auth_failures is
  'Drives escalating exponential backoff (exponential_backoff_delay) for cooldown_until; reset to 0 on a clean page fetch.';

comment on column social.account_auth_cooldown.blocker_kind is
  'auth = transient 401/403 (rotate + bounded retry allowed); checkpoint = challenge/checkpoint/login_required (non-clearing, no auto-rotate-retry, needs operator action).';

-- Internal scrape-state table: enable RLS and deny API roles explicitly while
-- privileged backend/service-role access continues. Mirrors
-- 20260518005750_social_internal_tables_enable_rls.sql so the Supabase security
-- advisor default-deny posture stays green.
alter table social.account_auth_cooldown enable row level security;

drop policy if exists deny_api_access_account_auth_cooldown
  on social.account_auth_cooldown;
create policy deny_api_access_account_auth_cooldown
  on social.account_auth_cooldown
  as restrictive
  for all
  to public
  using (false)
  with check (false);

commit;
